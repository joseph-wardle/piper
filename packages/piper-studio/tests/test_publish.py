import errno
import os
import re
import shutil
import stat
import textwrap
from pathlib import Path, PurePosixPath

import pytest
from pxr import Ar, Sdf, Usd, Vt

from piper.errors import PiperError, RegistryError
from piper.registry import Registry
from piper.tracker import Asset
from piper_studio import publish as publish_module
from piper_studio.publish import PartialPublishError, PublishResult, publish

Registrations = list[tuple[Asset, str, int, PurePosixPath]]

PAN = Asset(id="7701", name="Frying Pan", type="Prop", folder="kitchen")

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

# Pins geo v001 the way every pin is spelled: from the production root.
ENTRY = """
    #usda 1.0
    (
        defaultPrim = "pan"
        subLayers = [
            @asset/kitchen/frying_pan/publish/geo/v001/geo.usda@
        ]
    )
"""


@pytest.fixture
def root(tmp_path: Path) -> Path:
    """A production holding the frying pan's directory."""
    root = tmp_path / "production"
    (root / "asset" / "kitchen" / "frying_pan").mkdir(parents=True)
    return root


@pytest.fixture
def export(tmp_path: Path) -> Path:
    """Where an artist exports layers, outside the production."""
    return tmp_path / "export"


def write(path: Path, text: str = "") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(textwrap.dedent(text).lstrip(), encoding="utf-8")
    return path


def run(
    registry: Registry, root: Path, layer: Path, *, product: str = "geo", asset: Asset = PAN
) -> PublishResult:
    return publish(
        registry,
        root=PurePosixPath(root),
        asset=asset,
        product=product,
        layer=PurePosixPath(layer),
    )


def products(root: Path, product: str = "geo") -> Path:
    return root / "asset" / "kitchen" / "frying_pan" / "publish" / product


def mode(path: Path) -> int:
    return stat.S_IMODE(path.stat().st_mode)


def test_installs_the_layer_and_what_it_depends_on_as_a_locked_first_version(
    registry: Registry, registrations: Registrations, root: Path, export: Path, tmp_path: Path
) -> None:
    write(
        export / "geo.usda",
        """
        #usda 1.0
        (
            defaultPrim = "pan"
            subLayers = [
                @./looks/wood.usda@
            ]
        )

        def Xform "pan"
        {
        }
        """,
    )
    write(
        export / "looks" / "wood.usda",
        """
        #usda 1.0

        over "pan"
        {
            asset texture = @../tex/wood.<UDIM>.png@
        }
        """,
    )
    write(export / "tex" / "wood.1001.png")
    # A link into a mutable library must install as the file it names.
    (export / "tex" / "wood.1002.png").symlink_to(write(tmp_path / "library" / "wood.png"))
    write(export / "notes.txt")

    result = run(registry, root, export / "geo.usda")

    version = products(root) / "v001"
    assert (result.version, result.path) == (1, PurePosixPath(version / "geo.usda"))
    installed = {str(path.relative_to(version)) for path in version.rglob("*") if path.is_file()}
    assert installed == {"geo.usda", "looks/wood.usda", "tex/wood.1001.png", "tex/wood.1002.png"}
    assert not any(path.is_symlink() for path in version.rglob("*"))
    assert {mode(path) for path in version.rglob("*") if path.is_file()} == {0o444}
    assert {mode(path) for path in (version, *version.rglob("*")) if path.is_dir()} == {0o555}
    assert registrations == [(PAN, "geo", 1, result.path)]
    assert result.record_id is not None
    assert [path.name for path in products(root).iterdir()] == ["v001"]


def test_the_next_version_follows_the_highest_numbered_version_directory(
    registry: Registry, root: Path, export: Path
) -> None:
    write(export / "geo.usda", GEO)
    run(registry, root, export / "geo.usda")
    # v999 sorts after v1000 as text; empty directories still take their numbers.
    for name in ("v999", "v1000", "v12", "v1000a", ".tmp_abandoned"):
        (products(root) / name).mkdir()

    result = run(registry, root, export / "geo.usda")

    assert (result.version, result.path.parent.name) == (1001, "v1001")


def test_a_pinned_version_composes_from_a_copy_of_the_production(
    registry: Registry, root: Path, export: Path, tmp_path: Path
) -> None:
    write(export / "geo.usda", GEO)
    run(registry, root, export / "geo.usda")
    write(export / "entry.usda", ENTRY)
    run(registry, root, export / "entry.usda", product="entry")

    copy = tmp_path / "copy"
    shutil.copytree(root, copy)

    entry = products(copy, "entry") / "v001" / "entry.usda"
    context = Ar.DefaultResolverContext([str(copy)])
    stage = Usd.Stage.Open(str(entry), context, Usd.Stage.LoadAll)
    assert stage.GetPrimAtPath("/pan/body")
    used = [layer.realPath for layer in stage.GetUsedLayers() if layer.realPath]
    assert len(used) == 2
    assert all(Path(path).is_relative_to(copy) for path in used)


@pytest.mark.parametrize(
    ("asset", "product", "name", "refusal"),
    [
        pytest.param(PAN, "Geo", "geo.usda", "such as 'geo'", id="product-not-a-slug"),
        pytest.param(PAN, "geo", "geo.abc", "not a .usd, .usda, or .usdc layer", id="not-usd"),
        pytest.param(PAN, "geo", "missing.usda", "does not exist", id="no-layer"),
        pytest.param(
            Asset(id="7702", name="Pan Lid", type=None, folder=None),
            "geo",
            "geo.usda",
            "it needs a name and a folder",
            id="asset-without-folder",
        ),
        pytest.param(
            Asset(id="7704", name="Toaster", type="Prop", folder="kitchen"),
            "geo",
            "geo.usda",
            "has no directory at",
            id="asset-without-directory",
        ),
    ],
)
def test_a_request_that_cannot_be_published_changes_nothing(
    registry: Registry,
    registrations: Registrations,
    root: Path,
    export: Path,
    asset: Asset,
    product: str,
    name: str,
    refusal: str,
) -> None:
    write(export / "geo.usda", GEO)
    write(export / "geo.abc")

    with pytest.raises(PiperError, match=re.escape(refusal)):
        run(registry, root, export / name, product=product, asset=asset)

    assert not (root / "asset" / "kitchen" / "frying_pan" / "publish").exists()
    assert registrations == []


@pytest.mark.parametrize(
    ("reference", "refusal"),
    [
        pytest.param("./missing.usda", "missing.usda does not resolve", id="unresolved"),
        pytest.param(
            "../elsewhere/part.usda",
            "elsewhere/part.usda is outside",
            id="outside-the-export",
        ),
        pytest.param(
            "./src/part.usda", "src/part.usda is under src/, which a version reserves", id="src"
        ),
    ],
)
def test_a_dependency_that_cannot_be_installed_is_refused_before_copying(
    registry: Registry, root: Path, export: Path, tmp_path: Path, reference: str, refusal: str
) -> None:
    part = """
        #usda 1.0

        def Scope "part"
        {
        }
    """
    write(tmp_path / "elsewhere" / "part.usda", part)
    write(export / "src" / "part.usda", part)
    write(
        export / "geo.usda",
        f"""
        #usda 1.0
        (
            defaultPrim = "pan"
        )

        def Xform "pan" (
            references = @{reference}@</part>
        )
        {{
        }}
        """,
    )

    with pytest.raises(PiperError, match=re.escape(refusal)):
        run(registry, root, export / "geo.usda")

    assert not products(root).exists()


def test_an_export_inside_a_publish_directory_is_refused(
    registry: Registry, root: Path, tmp_path: Path
) -> None:
    layer = write(tmp_path / "publish" / "geo.usda", GEO)

    with pytest.raises(PiperError, match="inside a publish directory"):
        run(registry, root, layer)


@pytest.mark.parametrize(
    ("version", "file_mode", "directory_mode"),
    [
        pytest.param("v001/src", 0o444, 0o555, id="into-src"),
        pytest.param("v001", 0o644, 0o555, id="writable-file"),
        pytest.param("v001", 0o444, 0o755, id="writable-version"),
        pytest.param(".tmp_1a2b3c4d", 0o444, 0o555, id="into-staging"),
    ],
)
def test_a_pin_must_be_a_locked_file_of_an_installed_version(
    registry: Registry,
    root: Path,
    export: Path,
    version: str,
    file_mode: int,
    directory_mode: int,
) -> None:
    pinned = write(products(root) / version / "geo.usda", GEO)
    pinned.chmod(file_mode)
    for directory in (pinned.parent, products(root) / version.split("/")[0]):
        directory.chmod(directory_mode)
    pin = pinned.relative_to(root)
    write(
        export / "entry.usda",
        f"""
        #usda 1.0
        (
            defaultPrim = "pan"
            subLayers = [
                @{pin}@
            ]
        )
        """,
    )

    with pytest.raises(PiperError, match=f"{re.escape(str(pinned))} is neither inside"):
        run(registry, root, export / "entry.usda", product="entry")

    assert not products(root, "entry").exists()


def test_an_absolute_path_is_refused_from_the_staged_copy_and_staging_is_removed(
    registry: Registry, registrations: Registrations, root: Path, export: Path
) -> None:
    write(export / "geo.usda", GEO)
    geo = run(registry, root, export / "geo.usda").path
    write(
        export / "entry.usda",
        ENTRY.replace("asset/kitchen/frying_pan/publish/geo", str(geo.parent.parent)),
    )

    with pytest.raises(
        PiperError, match=f"entry.usda spells @{re.escape(str(geo))}@ as an absolute"
    ):
        run(registry, root, export / "entry.usda", product="entry")

    assert list(products(root, "entry").iterdir()) == []
    assert len(registrations) == 1


def crate_without_default_prim(path: Path) -> None:
    layer = Sdf.Layer.CreateNew(str(path))
    prim = Sdf.CreatePrimInLayer(layer, "/pan")
    prim.specifier = Sdf.SpecifierDef
    points = Sdf.AttributeSpec(prim, "points", Sdf.ValueTypeNames.Point3fArray)
    points.default = Vt.Vec3fArray(100_000)
    layer.Save()


@pytest.mark.parametrize(
    ("name", "text", "refusal"),
    [
        pytest.param(
            "geo.usda",
            'def Xform "pan"\n{\n}',
            "geo.usda sets no defaultPrim",
            id="no-default-prim",
        ),
        pytest.param(
            "geo.usda",
            '(\n    defaultPrim = "lid"\n)\n\ndef Xform "pan"\n{\n}',
            "geo.usda names defaultPrim 'lid', but has no such prim",
            id="default-prim-names-nothing",
        ),
        pytest.param(
            "geo.usda",
            '(\n    defaultPrim = "pan"\n)\n\n'
            'def Xform "pan" (\n    references = @./part.usda@</nope>\n)\n{\n}',
            "</nope>",
            id="composition-error",
        ),
        pytest.param("geo.usdc", None, "geo.usdc sets no defaultPrim", id="crate"),
    ],
)
def test_a_staged_copy_that_does_not_compose_is_refused_and_removed(
    registry: Registry, root: Path, export: Path, name: str, text: str | None, refusal: str
) -> None:
    write(export / "part.usda", '#usda 1.0\n\ndef Scope "part"\n{\n}\n')
    if text is None:
        export.mkdir(exist_ok=True)
        crate_without_default_prim(export / name)
    else:
        write(export / name, "#usda 1.0\n" + text + "\n")

    with pytest.raises(PiperError, match=re.escape(refusal)):
        run(registry, root, export / name)

    assert list(products(root).iterdir()) == []


def test_a_dependency_that_resolves_only_from_the_export_is_refused_from_the_staged_copy(
    registry: Registry, root: Path, export: Path
) -> None:
    write(export / "tex" / "wood.png")
    write(
        export / "geo.usda",
        """
        #usda 1.0
        (
            defaultPrim = "pan"
        )

        def Xform "pan"
        {
            asset texture = @../export/tex/wood.png@
        }
        """,
    )

    with pytest.raises(PiperError) as raised:
        run(registry, root, export / "geo.usda")

    assert f"{products(root) / 'export' / 'tex' / 'wood.png'} does not resolve" in str(raised.value)
    assert list(products(root).iterdir()) == []


def test_a_layer_with_unsaved_edits_in_this_session_is_refused(
    registry: Registry, root: Path, export: Path
) -> None:
    exported = write(export / "geo.usda", GEO)
    edited = Sdf.Layer.FindOrOpen(str(exported))
    Sdf.CreatePrimInLayer(edited, "/pan/lid")

    with pytest.raises(PiperError, match=re.escape("geo.usda has unsaved edits in this session")):
        run(registry, root, exported)

    assert not products(root).exists()


def test_a_pin_found_only_in_the_working_directory_is_refused_naming_it(
    registry: Registry, root: Path, export: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    elsewhere = tmp_path / "elsewhere"
    write(
        elsewhere / "asset" / "kitchen" / "frying_pan" / "publish" / "geo" / "v001" / "geo.usda",
        GEO,
    )
    write(export / "entry.usda", ENTRY)
    monkeypatch.chdir(elsewhere)

    with pytest.raises(PiperError, match=f"working directory, {re.escape(str(elsewhere))}, first"):
        run(registry, root, export / "entry.usda", product="entry")


def test_a_registration_failure_reports_the_installed_version(
    registry: Registry, root: Path, export: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def refuse(asset: Asset, *, product: str, version: int, path: PurePosixPath) -> str:
        raise RegistryError("shotgrid: refused to register geo v001")

    monkeypatch.setattr(registry, "register", refuse)
    write(export / "geo.usda", GEO)

    with pytest.raises(PartialPublishError) as raised:
        run(registry, root, export / "geo.usda")

    result = raised.value.result
    assert (result.version, result.record_id) == (1, None)
    assert Path(result.path).is_file()
    assert f"installed {result.path}, but could not register it" in str(raised.value)


def test_a_version_filled_by_another_publish_during_install_is_skipped_not_replaced(
    registry: Registry, root: Path, export: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    write(export / "geo.usda", GEO)
    first = run(registry, root, export / "geo.usda")
    write(export / "geo.usda", GEO.replace('"body"', '"lid"'))
    # As though another publish installed v001 after this one counted the versions.
    monkeypatch.setattr(publish_module, "_next_version", lambda _products: 1)

    second = run(registry, root, export / "geo.usda")

    assert second.version == 2
    assert '"body"' in Path(first.path).read_text(encoding="utf-8")
    assert '"lid"' in Path(second.path).read_text(encoding="utf-8")


def test_a_publish_that_finds_every_version_taken_is_refused_and_removes_staging(
    registry: Registry, root: Path, export: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    write(export / "geo.usda", GEO)
    for _ in range(5):
        run(registry, root, export / "geo.usda")
    monkeypatch.setattr(publish_module, "_next_version", lambda _products: 1)

    with pytest.raises(PiperError, match="other publishes took every version up to v005 first"):
        run(registry, root, export / "geo.usda")

    assert sorted(path.name for path in products(root).iterdir()) == [
        "v001",
        "v002",
        "v003",
        "v004",
        "v005",
    ]


def test_a_storage_failure_before_the_rename_is_refused_and_removes_staging(
    registry: Registry,
    registrations: Registrations,
    root: Path,
    export: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unreadable(_product_root: Path) -> int:
        raise OSError(errno.EIO, os.strerror(errno.EIO))

    monkeypatch.setattr(publish_module, "_next_version", unreadable)
    write(export / "geo.usda", GEO)

    with pytest.raises(PiperError, match=re.escape("could not be installed (Input/output error)")):
        run(registry, root, export / "geo.usda")

    assert list(products(root).iterdir()) == []
    assert registrations == []


# A soft NFS mount can time out a rename the server then performs; a resent
# rename finds its source gone, or its target taken by itself.
@pytest.mark.parametrize("error", [errno.EIO, errno.ENOENT, errno.EEXIST])
def test_a_rename_reported_as_failed_after_it_happened_is_installed(
    registry: Registry,
    registrations: Registrations,
    root: Path,
    export: Path,
    monkeypatch: pytest.MonkeyPatch,
    error: int,
) -> None:
    rename = os.rename

    def rename_then_fail(source: Path, target: Path) -> None:
        rename(source, target)
        raise OSError(error, os.strerror(error))

    monkeypatch.setattr(os, "rename", rename_then_fail)
    write(export / "geo.usda", GEO)

    result = run(registry, root, export / "geo.usda")

    assert result.version == 1
    assert Path(result.path).is_file()
    assert len(registrations) == 1


def test_a_rename_that_failed_without_moving_staging_leaves_both_and_names_them(
    registry: Registry,
    registrations: Registrations,
    root: Path,
    export: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail(source: Path, target: Path) -> None:
        raise OSError(errno.EIO, os.strerror(errno.EIO))

    monkeypatch.setattr(os, "rename", fail)
    write(export / "geo.usda", GEO)

    with pytest.raises(PiperError, match="install outcome unknown") as raised:
        run(registry, root, export / "geo.usda")

    (staging,) = products(root).iterdir()
    assert staging.name.startswith(".tmp_")
    assert str(staging) in str(raised.value)
    assert registrations == []
