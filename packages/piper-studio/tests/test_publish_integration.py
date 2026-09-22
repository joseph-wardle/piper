import contextlib
import getpass
import multiprocessing
import os
import shutil
import textwrap
import threading
import uuid
from collections.abc import Iterator
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path, PurePosixPath
from typing import Any, cast

import pytest
import shotgun_api3
from pxr import Ar, Sdf, Usd, Vt

from piper.errors import PiperError, RegistryError
from piper.registry import Registry
from piper.tracker import Asset
from piper_shotgrid.registry import ShotGridRegistry
from piper_shotgrid.tracker import ShotGridTracker
from piper_studio import publish as publish_module
from piper_studio.create import create_asset
from piper_studio.layout import asset_root, product_root
from piper_studio.publish import ProductVersion, UnregisteredVersionError, publish_product

pytestmark = pytest.mark.integration

needs_shotgrid = pytest.mark.skipif(
    not os.environ.get("PIPER_SHOTGRID_KEY"), reason="PIPER_SHOTGRID_KEY is not set"
)

SITE = "https://byuanimation.shotgunstudio.com"
SCRIPT = "sandwich_pipeline"
# An inactive copy of the production, kept for writes. The live one is never written.
WRITE_PROJECT = 782
# Scratch space on the same NFS mount as production storage
ROOT = Path("/groups/sandwich/04_temp")
TYPE = "Set Piece"

GEO = """
    #usda 1.0
    (
        defaultPrim = "pan"
    )

    def Xform "pan"
    {
        def Mesh "body"
        {
        }
    }
"""


class Unregistered:
    """A registry for tests of storage alone: it records nothing."""

    def register(self, asset: Asset, *, product: str, version: int, path: PurePosixPath) -> str:
        return "unregistered"


@pytest.fixture
def run_id() -> str:
    """Unique to the run, so two runs cannot collide."""
    return uuid.uuid4().hex[:8]


@pytest.fixture
def export(run_id: str) -> Iterator[Path]:
    export = ROOT / f"piper_test_{run_id}_export"
    export.mkdir()
    try:
        yield export
    finally:
        shutil.rmtree(export, ignore_errors=True)


@pytest.fixture
def asset(run_id: str) -> Iterator[Asset]:
    """An asset directory in scratch storage, with no tracker entity behind it."""
    folder = f"piper_test_{run_id}"
    asset = Asset(
        id="0",
        name=f"Piper Test {run_id}",
        type=TYPE,
        folder=folder,
        pipe_name=f"piper_test_{run_id}",
    )
    Path(asset_root(PurePosixPath(ROOT), folder, asset.pipe_name or "")).mkdir(parents=True)
    try:
        yield asset
    finally:
        remove_directories(folder)


def write(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(textwrap.dedent(text).lstrip(), encoding="utf-8")
    return path


def publish_to_scratch(
    registry: Registry, asset: Asset, product: str, layer: Path
) -> ProductVersion:
    return publish_product(
        registry, root=PurePosixPath(ROOT), asset=asset, product=product, layer=PurePosixPath(layer)
    )


def products(asset: Asset, product: str) -> Path:
    pan = asset_root(PurePosixPath(ROOT), asset.folder or "", asset.pipe_name or "")
    return Path(product_root(pan, product))


def publish_when_all_are_ready(barrier: threading.Barrier, asset: Asset, layer: Path) -> int:
    """Runs in its own process, as a second artist's publish would."""
    barrier.wait()
    return publish_to_scratch(Unregistered(), asset, "geo", layer).version


def test_concurrent_publishes_of_one_product_install_distinct_versions(
    asset: Asset, export: Path
) -> None:
    workers = 4
    layers = [
        write(
            export / str(worker) / "geo.usda",
            f"""
            #usda 1.0
            (
                customLayerData = {{
                    int worker = {worker}
                }}
                defaultPrim = "pan"
            )

            def Xform "pan"
            {{
            }}
            """,
        )
        for worker in range(workers)
    ]

    context = multiprocessing.get_context("spawn")
    with context.Manager() as manager, ProcessPoolExecutor(workers, mp_context=context) as pool:
        barrier = manager.Barrier(workers)
        futures = [
            pool.submit(publish_when_all_are_ready, barrier, asset, layer) for layer in layers
        ]
        versions = [future.result() for future in futures]

    assert sorted(versions) == [1, 2, 3, 4]
    for worker, version in enumerate(versions):
        installed = products(asset, "geo") / f"v{version:03d}" / "geo.usda"
        assert Sdf.Layer.OpenAsAnonymous(str(installed)).customLayerData["worker"] == worker
    assert sorted(path.name for path in products(asset, "geo").iterdir()) == [
        "v001",
        "v002",
        "v003",
        "v004",
    ]


def test_a_version_filled_during_install_is_skipped_on_nfs(
    asset: Asset, export: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # NFS must refuse a rename onto a filled version with an error that means "taken".
    layer = write(export / "geo.usda", GEO)
    first = publish_to_scratch(Unregistered(), asset, "geo", layer)
    monkeypatch.setattr(publish_module, "_next_version", lambda _products: 1)

    second = publish_to_scratch(Unregistered(), asset, "geo", layer)

    assert (first.version, second.version) == (1, 2)


def test_a_refused_crate_layer_leaves_no_version_and_no_staging_on_nfs(
    asset: Asset, export: Path
) -> None:
    # NFS keeps a removed file that is still open, so a staged crate still held
    # by USD would stop its staging directory from being removed.
    exported = Sdf.Layer.CreateNew(str(export / "geo.usdc"))
    prim = Sdf.CreatePrimInLayer(exported, "/pan")
    prim.specifier = Sdf.SpecifierDef
    Sdf.AttributeSpec(prim, "points", Sdf.ValueTypeNames.Point3fArray).default = Vt.Vec3fArray(
        400_000
    )
    exported.Save()
    del prim, exported

    with pytest.raises(PiperError, match=r"geo\.usdc sets no defaultPrim"):
        publish_to_scratch(Unregistered(), asset, "geo", export / "geo.usdc")

    assert list(products(asset, "geo").iterdir()) == []


def material(color: str) -> str:
    return f"""
        #usda 1.0
        (
            defaultPrim = "pan"
        )

        over "pan"
        {{
            def Material "wood"
            {{
                color3f inputs:diffuseColor = ({color})
            }}
        }}
    """


def entry(pins: str, mtl: str) -> str:
    return f"""
        #usda 1.0
        (
            defaultPrim = "pan"
            subLayers = [
                @{pins}/mtl/{mtl}/mtl.usda@,
                @{pins}/geo/v001/geo.usda@
            ]
        )
    """


def composed_color(published: ProductVersion) -> tuple[float, ...]:
    context = Ar.DefaultResolverContext([str(ROOT)])
    stage = Usd.Stage.Open(str(published.path), context, Usd.Stage.LoadAll)
    assert stage.GetPrimAtPath("/pan/body")
    return tuple(stage.GetPrimAtPath("/pan/wood").GetAttribute("inputs:diffuseColor").Get())


@needs_shotgrid
def test_pinned_components_publish_compose_and_register_in_the_write_project(
    run_id: str, export: Path
) -> None:
    key = os.environ["PIPER_SHOTGRID_KEY"]
    tracker = ShotGridTracker(site=SITE, script=SCRIPT, key=key, project=WRITE_PROJECT)
    registry = ShotGridRegistry(site=SITE, script=SCRIPT, key=key, project=WRITE_PROJECT)
    name, folder = f"Piper Test {run_id}", f"piper_test_{run_id}"

    def publish_layer(registry: Registry, product: str, text: str) -> ProductVersion:
        layer = write(export / product / f"{product}.usda", text)
        return publish_to_scratch(registry, asset, product, layer)

    try:
        asset = create_asset(
            tracker,
            root=PurePosixPath(ROOT),
            types=(TYPE,),
            name=name,
            type=TYPE,
            folder=folder,
            new_folder=True,
        ).asset
        pins = f"asset/{folder}/piper_test_{run_id}/publish"

        geo = publish_layer(registry, "geo", GEO)
        mtl_1 = publish_layer(registry, "mtl", material("0.8, 0.5, 0.2"))
        entry_1 = publish_layer(registry, "entry", entry(pins, "v001"))
        mtl_2 = publish_layer(registry, "mtl", material("0.2, 0.3, 0.9"))
        entry_2 = publish_layer(registry, "entry", entry(pins, "v002"))

        published = (geo, mtl_1, entry_1, mtl_2, entry_2)
        assert [result.version for result in published] == [1, 1, 1, 2, 2]
        assert composed_color(entry_1) == pytest.approx((0.8, 0.5, 0.2))
        assert composed_color(entry_2) == pytest.approx((0.2, 0.3, 0.9))

        found = shotgrid().find(
            "PublishedFile",
            [["entity", "is", {"type": "Asset", "id": int(asset.id)}]],
            ["project", "code", "name", "version_number", "path", "description"],
        )
        records = {str(record["id"]): record for record in cast("list[dict[str, Any]]", found)}
        assert len(records) == len(published)
        for result in published:
            record = records[str(result.record_id)]
            assert record["project"]["id"] == WRITE_PROJECT
            assert (record["name"], record["version_number"]) == (result.product, result.version)
            assert record["code"] == f"{result.product} v{result.version:03d}"
            assert record["path"]["url"] == Path(result.path).as_uri()
            assert record["description"] == f"published by {getpass.getuser()}"

        with pytest.raises(RegistryError, match="already registered"):
            registry.register(asset, product="entry", version=2, path=entry_2.path)

        refusing = ShotGridRegistry(
            site=SITE, script=SCRIPT, key="not-a-real-key", project=WRITE_PROJECT
        )
        with pytest.raises(UnregisteredVersionError) as raised:
            publish_layer(refusing, "geo", GEO)
        unregistered = raised.value.version
        assert (unregistered.version, unregistered.record_id) == (2, None)
        assert Path(unregistered.path).is_file()
        assert str(unregistered.path) in str(raised.value)
    finally:
        remove_from_shotgrid(name)
        remove_directories(folder)


def shotgrid() -> shotgun_api3.Shotgun:
    return shotgun_api3.Shotgun(SITE, script_name=SCRIPT, api_key=os.environ["PIPER_SHOTGRID_KEY"])


def remove_from_shotgrid(name: str) -> None:
    """Retire the asset and its published files.

    ShotGrid's API can only retire, so each run leaves retired records in the
    write project, invisible to every find.
    """
    site = shotgrid()
    project = {"type": "Project", "id": WRITE_PROJECT}
    for asset in site.find("Asset", [["project", "is", project], ["code", "is", name]]):
        for record in site.find("PublishedFile", [["entity", "is", asset]]):
            site.delete("PublishedFile", record["id"])
        site.delete("Asset", asset["id"])


def remove_directories(folder: str) -> None:
    """Remove a test folder and the versions installed in it."""
    shutil.rmtree(ROOT / "asset" / folder, ignore_errors=True)
    with contextlib.suppress(OSError):
        (ROOT / "asset").rmdir()
