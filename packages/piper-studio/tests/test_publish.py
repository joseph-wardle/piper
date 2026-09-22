import errno
import os
import re
import shutil
import textwrap
from pathlib import Path, PurePosixPath

import pytest
from pxr import Ar, Sdf, Usd, Vt

from piper.errors import PiperError, RegistryError
from piper.registry import Registry
from piper.tracker import Asset
from piper_studio import publish as publish_module
from piper_studio.current import current, make_current
from piper_studio.publish import (
    PartialPublishError,
    ProductVersion,
    PublishResult,
    UnregisteredVersionError,
    composition,
    current_line,
    other_versions,
    publish,
    publish_product,
)

Registrations = list[tuple[Asset, str, int, PurePosixPath]]

PAN = Asset(id="7701", name="Frying Pan", type="Prop", folder="kitchen", pipe_name="frying_pan")

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

# What a material publish installs: overs on the model, a definition inside it.
MTL = """
    #usda 1.0
    (
        defaultPrim = "pan"
    )

    over "pan"
    {
        def Material "wood"
        {
            color3f inputs:diffuseColor = (0.8, 0.5, 0.2)
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
    registry: Registry,
    root: Path,
    layer: Path,
    *,
    product: str = "geo",
    asset: Asset = PAN,
    source: Path | None = None,
) -> ProductVersion:
    return publish_product(
        registry,
        root=PurePosixPath(root),
        asset=asset,
        product=product,
        layer=PurePosixPath(layer),
        source=PurePosixPath(source) if source else None,
    )


def products(root: Path, product: str = "geo") -> Path:
    return root / "asset" / "kitchen" / "frying_pan" / "publish" / product


def test_installs_the_layer_and_what_it_depends_on_as_the_first_version(
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
            Asset(id="7704", name="Toaster", type="Prop", folder="kitchen", pipe_name="toaster"),
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
    "version",
    [
        pytest.param("v001/src", id="into-src"),
        pytest.param(".tmp_1a2b3c4d", id="into-staging"),
    ],
)
def test_a_pin_must_be_a_published_file_of_an_installed_version(
    registry: Registry, root: Path, export: Path, version: str
) -> None:
    pinned = write(products(root) / version / "geo.usda", GEO)
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


def test_the_work_file_a_layer_came_from_is_kept_in_the_versions_src(
    registry: Registry, root: Path, export: Path, tmp_path: Path
) -> None:
    write(export / "geo.usda", GEO)
    work = write(tmp_path / "work" / "frying_pan.mb", "the scene")

    result = run(registry, root, export / "geo.usda", source=work)

    version = Path(result.path).parent
    installed = {str(path.relative_to(version)) for path in version.rglob("*") if path.is_file()}
    assert installed == {"geo.usda", "src/frying_pan.mb"}
    assert (version / "src" / "frying_pan.mb").read_bytes() == work.read_bytes()


def test_a_work_file_that_cannot_be_copied_is_refused_and_removes_staging(
    registry: Registry, registrations: Registrations, root: Path, export: Path, tmp_path: Path
) -> None:
    write(export / "geo.usda", GEO)
    missing = tmp_path / "work" / "frying_pan.mb"

    with pytest.raises(PiperError, match=re.escape(f"{missing} could not be copied")):
        run(registry, root, export / "geo.usda", source=missing)

    assert list(products(root).iterdir()) == []
    assert registrations == []


def test_a_registration_failure_reports_the_installed_version(
    registry: Registry, root: Path, export: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def refuse(asset: Asset, *, product: str, version: int, path: PurePosixPath) -> str:
        raise RegistryError("shotgrid: refused to register geo v001")

    monkeypatch.setattr(registry, "register", refuse)
    write(export / "geo.usda", GEO)

    with pytest.raises(UnregisteredVersionError) as raised:
        run(registry, root, export / "geo.usda")

    result = raised.value.version
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


# ---------------------------------------------------------------------------
# `publish`: the component, the asset version pinning it, and `current`.


def compose_run(
    registry: Registry,
    root: Path,
    layer: Path,
    *,
    product: str = "geo",
    with_versions: dict[str, int] | None = None,
) -> PublishResult:
    return publish(
        registry,
        root=PurePosixPath(root),
        asset=PAN,
        product=product,
        layer=PurePosixPath(layer),
        with_versions=with_versions or {},
    )


def composed(root: Path, version: int) -> Usd.Stage:
    entry = products(root, "asset") / f"v{version:03d}" / "frying_pan.usda"
    return Usd.Stage.Open(str(entry), Ar.DefaultResolverContext([str(root)]), Usd.Stage.LoadAll)


def test_the_first_publish_builds_an_asset_version_pinning_it_alone_and_makes_it_current(
    registry: Registry, registrations: Registrations, root: Path, export: Path
) -> None:
    write(export / "geo.usda", GEO)

    result = compose_run(registry, root, export / "geo.usda")

    assert result.component.version == 1 and result.asset_version is not None
    assert (result.asset_version.version, dict(result.pins), result.current) == (
        1,
        {"geo": 1},
        True,
    )
    assert [(product, version) for _, product, version, _ in registrations] == [
        ("geo", 1),
        ("asset", 1),
    ]
    assert current(PurePosixPath(root), PAN) == 1
    assert composition(result) == "asset v001 pins geo v001, and is current"
    stage = composed(root, 1)
    assert stage.GetCompositionErrors() == [] and stage.GetPrimAtPath("/frying_pan/body")


def test_each_publish_replaces_its_own_component_in_what_is_current(
    registry: Registry, root: Path, export: Path
) -> None:
    write(export / "geo.usda", GEO)
    write(export / "mtl.usda", MTL)
    compose_run(registry, root, export / "geo.usda")

    with_material = compose_run(registry, root, export / "mtl.usda", product="mtl")
    new_geometry = compose_run(registry, root, export / "geo.usda")

    assert dict(with_material.pins) == {"geo": 1, "mtl": 1}
    assert dict(new_geometry.pins) == {"geo": 2, "mtl": 1}
    assert current(PurePosixPath(root), PAN) == 3
    assert composition(with_material) == "asset v002 pins geo v001 and mtl v001, and is current"
    assert (
        composed(root, 2)
        .GetPrimAtPath("/frying_pan/wood")
        .GetAttribute("inputs:diffuseColor")
        .Get()
    )


def test_an_older_composition_made_current_is_the_base_of_the_next_publish(
    registry: Registry, root: Path, export: Path
) -> None:
    write(export / "geo.usda", GEO)
    write(export / "mtl.usda", MTL)
    compose_run(registry, root, export / "geo.usda")
    compose_run(registry, root, export / "mtl.usda", product="mtl")
    compose_run(registry, root, export / "geo.usda")
    make_current(PurePosixPath(root), PAN, 2)

    result = compose_run(registry, root, export / "mtl.usda", product="mtl")

    assert dict(result.pins) == {"geo": 1, "mtl": 2}
    assert result.asset_version is not None and result.asset_version.version == 4
    assert current(PurePosixPath(root), PAN) == 4


def test_a_dialog_says_what_is_current_and_offers_the_other_components_versions(
    registry: Registry, root: Path, export: Path
) -> None:
    production = PurePosixPath(root)
    assert current_line(None, {}) == "Nothing is current."
    assert other_versions(production, PAN, {}, "geo") == {}

    write(export / "geo.usda", GEO)
    write(export / "mtl.usda", MTL)
    compose_run(registry, root, export / "geo.usda")
    compose_run(registry, root, export / "mtl.usda", product="mtl")
    compose_run(registry, root, export / "geo.usda")
    make_current(production, PAN, 2)
    pins = {"geo": 1, "mtl": 1}

    assert current_line(2, pins) == "Current v002 pins geo v001, mtl v001."
    assert other_versions(production, PAN, pins, "geo") == {"mtl": ([1], 1)}
    assert other_versions(production, PAN, pins, "mtl") == {"geo": ([1, 2], 1)}


def test_a_named_version_replaces_the_current_pin_for_that_publish_only(
    registry: Registry, root: Path, export: Path
) -> None:
    write(export / "geo.usda", GEO)
    write(export / "mtl.usda", MTL)
    compose_run(registry, root, export / "geo.usda")
    compose_run(registry, root, export / "geo.usda")
    make_current(PurePosixPath(root), PAN, 1)

    result = compose_run(
        registry, root, export / "mtl.usda", product="mtl", with_versions={"geo": 2}
    )

    assert dict(result.pins) == {"geo": 2, "mtl": 1}


@pytest.mark.parametrize(
    ("product", "name", "with_versions", "refusal"),
    [
        pytest.param(
            "asset", "asset.usda", {}, "an asset version is built by publishing", id="asset"
        ),
        pytest.param("mtl", "geo.usda", {}, "named for its product, mtl.usd, mtl.usda", id="name"),
        pytest.param(
            "geo", "geo.usda", {"geo": 1}, "cannot pin geo while publishing geo", id="self"
        ),
        pytest.param(
            "geo", "geo.usda", {"mtl": 4}, r"has no mtl v004 \(installed: none\)", id="missing"
        ),
    ],
)
def test_a_composition_that_cannot_be_built_is_refused_before_anything_is_published(
    registry: Registry,
    registrations: Registrations,
    root: Path,
    export: Path,
    product: str,
    name: str,
    with_versions: dict[str, int],
    refusal: str,
) -> None:
    write(export / "geo.usda", GEO)
    write(export / "asset.usda", GEO)

    with pytest.raises(PiperError, match=refusal):
        compose_run(registry, root, export / name, product=product, with_versions=with_versions)

    assert registrations == []
    assert not (root / "asset" / "kitchen" / "frying_pan" / "publish").exists()


def refusing(registry: Registry, product: str, monkeypatch: pytest.MonkeyPatch) -> None:
    register = registry.register

    def refuse(asset: Asset, *, product: str, version: int, path: PurePosixPath) -> str:
        if product == refused:
            raise RegistryError(f"shotgrid: refused to register {product} v{version:03d}")
        return register(asset, product=product, version=version, path=path)

    refused = product
    monkeypatch.setattr(registry, "register", refuse)


def test_a_component_that_did_not_register_builds_no_asset_version(
    registry: Registry, root: Path, export: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    write(export / "geo.usda", GEO)
    refusing(registry, "geo", monkeypatch)

    with pytest.raises(PartialPublishError) as raised:
        compose_run(registry, root, export / "geo.usda")

    result = raised.value.result
    assert (result.component.version, result.component.record_id) == (1, None)
    assert (result.asset_version, dict(result.pins), result.current) == (None, {"geo": 1}, False)
    assert str(raised.value).endswith(
        "publishing again installs another version; no asset version was built"
    )
    assert not products(root, "asset").exists()
    assert current(PurePosixPath(root), PAN) is None


def test_an_asset_version_that_did_not_register_is_installed_and_not_current(
    registry: Registry, root: Path, export: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    write(export / "geo.usda", GEO)
    compose_run(registry, root, export / "geo.usda")
    refusing(registry, "asset", monkeypatch)

    with pytest.raises(PartialPublishError) as raised:
        compose_run(registry, root, export / "geo.usda")

    result = raised.value.result
    assert result.component.record_id is not None and result.asset_version is not None
    assert (result.asset_version.version, result.asset_version.record_id, result.current) == (
        2,
        None,
        False,
    )
    assert "published geo v002; installed" in str(raised.value)
    assert str(raised.value).endswith("`piper current` makes it current")
    assert current(PurePosixPath(root), PAN) == 1
    assert composition(result) == "asset v002 pins geo v002, and is not current"


def test_an_asset_version_that_cannot_be_installed_leaves_the_component_published(
    registry: Registry, registrations: Registrations, root: Path, export: Path
) -> None:
    write(export / "geo.usda", GEO)
    products(root, "asset").mkdir(parents=True)
    products(root, "asset").chmod(0o500)
    try:
        with pytest.raises(PartialPublishError) as raised:
            compose_run(registry, root, export / "geo.usda")
    finally:
        products(root, "asset").chmod(0o700)

    result = raised.value.result
    assert result.component.record_id == "6601"
    assert (result.asset_version, result.current) == (None, False)
    assert "published geo v001, but could not build the asset version: cannot publish" in str(
        raised.value
    )
    assert [product for _, product, _, _ in registrations] == ["geo"]
    assert current(PurePosixPath(root), PAN) is None


def test_a_version_made_current_during_the_publish_stays_current_and_is_named(
    registry: Registry, root: Path, export: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    write(export / "geo.usda", GEO)
    write(export / "mtl.usda", MTL)
    compose_run(registry, root, export / "geo.usda")
    compose_run(registry, root, export / "mtl.usda", product="mtl")
    make_current(PurePosixPath(root), PAN, 1)
    register = registry.register

    def move_meanwhile(asset: Asset, *, product: str, version: int, path: PurePosixPath) -> str:
        if product == "asset":
            make_current(PurePosixPath(root), PAN, 2)
        return register(asset, product=product, version=version, path=path)

    monkeypatch.setattr(registry, "register", move_meanwhile)

    with pytest.raises(PartialPublishError) as raised:
        compose_run(registry, root, export / "geo.usda")

    result = raised.value.result
    assert result.asset_version is not None and result.asset_version.version == 3
    assert (dict(result.pins), result.current) == ({"geo": 2}, False)
    assert (
        "published geo v002 and asset v003, but asset v002 became current while you were "
        "publishing, pinning geo v001, mtl v001; asset v003 is not current; publish again"
    ) in str(raised.value)
    assert current(PurePosixPath(root), PAN) == 2


def test_a_race_on_the_same_component_is_settled_by_making_the_newer_version_current(
    registry: Registry, root: Path, export: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    write(export / "geo.usda", GEO)
    write(export / "mtl.usda", MTL)
    compose_run(registry, root, export / "geo.usda")
    compose_run(registry, root, export / "mtl.usda", product="mtl")
    register = registry.register

    def publish_meanwhile(asset: Asset, *, product: str, version: int, path: PurePosixPath) -> str:
        if product == "asset" and version == 3:
            monkeypatch.setattr(registry, "register", register)
            compose_run(registry, root, export / "geo.usda")
        return register(asset, product=product, version=version, path=path)

    monkeypatch.setattr(registry, "register", publish_meanwhile)

    with pytest.raises(PartialPublishError) as raised:
        compose_run(registry, root, export / "geo.usda")

    assert raised.value.result.asset_version is not None
    assert str(raised.value).endswith(
        "asset v004 became current while you were publishing, pinning geo v003; "
        "`piper current` makes asset v003 current instead"
    )
    assert current(PurePosixPath(root), PAN) == 4


def test_a_current_layer_that_cannot_be_written_leaves_both_versions_and_names_the_remedy(
    registry: Registry, root: Path, export: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    write(export / "geo.usda", GEO)
    compose_run(registry, root, export / "geo.usda")
    register = registry.register

    def lock_meanwhile(asset: Asset, *, product: str, version: int, path: PurePosixPath) -> str:
        if product == "asset":
            products(root, "asset").chmod(0o500)
        return register(asset, product=product, version=version, path=path)

    monkeypatch.setattr(registry, "register", lock_meanwhile)
    try:
        with pytest.raises(PartialPublishError) as raised:
            compose_run(registry, root, export / "geo.usda")
    finally:
        products(root, "asset").chmod(0o700)

    result = raised.value.result
    assert result.asset_version is not None and result.asset_version.record_id is not None
    assert result.current is False
    assert (
        "published geo v002 and asset v002, but could not make it current: could not write"
        in str(raised.value)
    )
    assert current(PurePosixPath(root), PAN) == 1
