import re
import shutil
import textwrap
from pathlib import Path, PurePosixPath

import pytest
from pxr import Ar, Sdf, Usd, UsdShade

from piper.errors import PiperError, RegistryError
from piper.registry import Registry
from piper.tracker import Asset
from piper_studio.compose import current, current_pins
from piper_studio.publish import (
    PartialPublishTexturesError,
    PublishTexturesResult,
    publish,
    publish_textures,
)

Registrations = list[tuple[Asset, str, int, PurePosixPath]]

PAN = Asset(id="7701", name="Frying Pan", type="Prop", folder="kitchen", pipe_name="frying_pan")
TEX = "asset/kitchen/frying_pan/publish/tex"

# What Maya's publish installs: the model, and an empty slot per shading group under mtl.
GEO = """
    #usda 1.0
    (
        defaultPrim = "frying_pan"
    )

    def Xform "frying_pan"
    {
        def Scope "mtl"
        {
            def Material "body"
            {
            }
        }
    }
"""

# A material reading the textures of tex v001, as the HDA spells them: from the root.
MTL = f"""
    #usda 1.0
    (
        defaultPrim = "frying_pan"
    )

    over "frying_pan"
    {{
        over "mtl"
        {{
            over "body"
            {{
                def Shader "rman"
                {{
                    uniform token info:id = "PxrTexture"
                    asset inputs:filename = @{TEX}/v001/body_BaseColor.<UDIM>.tex@
                }}

                def Shader "normal"
                {{
                    uniform token info:id = "PxrTexture"
                    asset inputs:filename = @{TEX}/v001/body_Normal.<UDIM>.tex@
                }}

                def Shader "preview"
                {{
                    uniform token info:id = "UsdUVTexture"
                    asset inputs:file = @{TEX}/v001/body_BaseColor.<UDIM>.jpg@
                }}
            }}
        }}
    }}
"""

PLAIN_MTL = """
    #usda 1.0
    (
        defaultPrim = "frying_pan"
    )

    over "frying_pan"
    {
        over "mtl"
        {
            over "body"
            {
                color3f inputs:diffuseColor = (0.8, 0.5, 0.2)
            }
        }
    }
"""


@pytest.fixture
def root(tmp_path: Path) -> Path:
    """A production holding the frying pan's directory."""
    root = tmp_path / "production"
    (root / "asset" / "kitchen" / "frying_pan").mkdir(parents=True)
    return root


@pytest.fixture
def lookdev(registry: Registry, root: Path, renderman: Path, tmp_path: Path) -> None:
    """The pan after lookdev: geo v001, tex v001 of two BaseColor tiles and one Normal, mtl v001."""
    publish_layer(registry, root, write(tmp_path / "geo" / "geo.usda", GEO))
    run(
        registry,
        root,
        renderman,
        export(
            tmp_path / "first", "body_BaseColor.1001", "body_BaseColor.1002", "body_Normal.1001"
        ),
    )
    publish_layer(registry, root, write(tmp_path / "mtl" / "mtl.usda", MTL))


def write(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(textwrap.dedent(text).lstrip(), encoding="utf-8")
    return path


def export(directory: Path, *names: str) -> Path:
    """A Painter export: the PNG and the preview of each tile named ``<slot>_<map>.<udim>``."""
    directory.mkdir(parents=True)
    for name in names:
        (directory / f"{name}.png").touch()
        (directory / f"{name}.jpg").touch()
    return directory


def publish_layer(registry: Registry, root: Path, layer: Path) -> None:
    publish(
        registry,
        root=PurePosixPath(root),
        asset=PAN,
        product=layer.stem,
        layer=PurePosixPath(layer),
    )


def run(registry: Registry, root: Path, renderman: Path, exported: Path) -> PublishTexturesResult:
    return publish_textures(
        registry,
        root=PurePosixPath(root),
        asset=PAN,
        export=PurePosixPath(exported),
        renderman=renderman,
    )


def texture_paths(root: Path, version: int) -> dict[str, str]:
    """Each texture shader's authored path in the composed asset version, by shader name."""
    entry = root / "asset" / "kitchen" / "frying_pan" / "publish" / "asset"
    stage = Usd.Stage.Open(
        str(entry / f"v{version:03d}" / "frying_pan.usda"),
        Ar.DefaultResolverContext([str(root)]),
        Usd.Stage.LoadAll,
    )
    assert stage.GetCompositionErrors() == []
    paths: dict[str, str] = {}
    for prim in stage.Traverse():
        if prim.IsA(UsdShade.Shader):
            for name in ("inputs:filename", "inputs:file"):
                if prim.HasAttribute(name):
                    paths[prim.GetName()] = prim.GetAttribute(name).Get().path
    return paths


def test_a_first_publish_installs_the_export_freshly_converted_and_names_lookdev(
    registry: Registry, registrations: Registrations, root: Path, renderman: Path, tmp_path: Path
) -> None:
    exported = export(tmp_path / "export", "body_BaseColor.1001", "body_Normal.1001")
    # Left by an earlier `piper convert`, or older than its PNG: never trusted.
    (exported / "body_BaseColor.1001.tex").write_text("stale", encoding="utf-8")

    result = run(registry, root, renderman, exported)

    installed = Path(result.textures.path)
    assert installed == root / TEX / "v001"
    assert sorted(path.name for path in installed.iterdir()) == [
        "body_BaseColor.1001.jpg",
        "body_BaseColor.1001.png",
        "body_BaseColor.1001.tex",
        "body_Normal.1001.jpg",
        "body_Normal.1001.png",
        "body_Normal.1001.tex",
    ]
    assert (installed / "body_BaseColor.1001.tex").read_text(encoding="utf-8") == ""
    assert sorted(path.name for path in exported.iterdir()) == [
        "body_BaseColor.1001.jpg",
        "body_BaseColor.1001.png",
        "body_BaseColor.1001.tex",
        "body_Normal.1001.jpg",
        "body_Normal.1001.png",
    ]
    assert (exported / "body_BaseColor.1001.tex").read_text(encoding="utf-8") == "stale"
    assert (result.material, result.derived_from) == (None, None)
    assert result.warnings == (
        "no material uses textures yet; `piper open 'Frying Pan' lookdev` builds one",
    )
    assert registrations == [(PAN, "tex", 1, PurePosixPath(installed))]
    assert current(PurePosixPath(root), PAN) is None


@pytest.mark.usefixtures("lookdev")
def test_the_material_is_derived_reading_the_new_version_and_keeping_what_it_lacks(
    registry: Registry, registrations: Registrations, root: Path, renderman: Path, tmp_path: Path
) -> None:
    second = export(
        tmp_path / "second",
        "body_BaseColor.1001",
        "body_BaseColor.1002",
        "body_BaseColor.1003",
        "body_Metallic.1001",
        "handle_BaseColor.1001",
    )

    result = run(registry, root, renderman, second)

    assert result.material is not None and result.material.asset_version is not None
    assert (result.derived_from, result.material.component.version) == (1, 2)
    assert (dict(result.material.pins), result.material.current) == ({"geo": 1, "mtl": 2}, True)
    assert result.warnings == (
        "body_Normal.<UDIM>.tex is not in tex v002, so the material keeps reading it from tex v001",
        "tex v002 holds body_Metallic, which mtl v001 does not read",
        "tex v002 holds handle_BaseColor, which mtl v001 does not read",
        "handle is a texture set of tex v002, but geo v001 has no slot named handle (slots: body)",
        "body_BaseColor has tiles 1001, 1002, 1003 in tex v002 and 1001, 1002 in tex v001",
    )
    material = Sdf.Layer.OpenAsAnonymous(str(root / result.material.component.path))
    assert material.documentation == "derived from mtl v001 with tex v002"
    assert texture_paths(root, 3) == {
        "rman": f"{TEX}/v002/body_BaseColor.<UDIM>.tex",
        "normal": f"{TEX}/v001/body_Normal.<UDIM>.tex",
        "preview": f"{TEX}/v002/body_BaseColor.<UDIM>.jpg",
    }
    assert [(product, version) for _, product, version, _ in registrations[-3:]] == [
        ("tex", 2),
        ("mtl", 2),
        ("asset", 3),
    ]


@pytest.mark.usefixtures("lookdev")
def test_a_version_the_material_reads_nothing_of_is_installed_and_derives_nothing(
    registry: Registry, root: Path, renderman: Path, tmp_path: Path
) -> None:
    unread = run(registry, root, renderman, export(tmp_path / "unread", "body_Metallic.1001"))

    assert (unread.material, unread.derived_from) == (None, None)
    assert unread.warnings == (
        "body_BaseColor.<UDIM>.jpg is not in tex v002, so the material keeps reading it from "
        "tex v001",
        "body_BaseColor.<UDIM>.tex is not in tex v002, so the material keeps reading it from "
        "tex v001",
        "body_Normal.<UDIM>.tex is not in tex v002, so the material keeps reading it from tex v001",
        "nothing mtl v001 reads is in tex v002, so no material was derived; point it at tex v002 "
        "in Houdini",
    )
    assert current_pins(PurePosixPath(root), PAN) == (2, {"geo": 1, "mtl": 1})

    publish_layer(registry, root, write(tmp_path / "plain" / "mtl.usda", PLAIN_MTL))
    plain = run(registry, root, renderman, export(tmp_path / "plain_export", "body_BaseColor.1001"))

    assert (plain.material, plain.derived_from) == (None, None)
    assert plain.warnings == (
        "mtl v002 reads no published textures, so no material was derived; point it at tex v003 "
        "in Houdini",
    )
    assert current_pins(PurePosixPath(root), PAN) == (3, {"geo": 1, "mtl": 2})


def test_an_export_that_cannot_be_converted_leaves_nothing(
    registry: Registry, registrations: Registrations, root: Path, renderman: Path, tmp_path: Path
) -> None:
    exported = export(tmp_path / "export", "body_BaseColor.1001", "body_broken_Normal.1001")

    with pytest.raises(PiperError) as refusal:
        run(registry, root, renderman, exported)

    assert str(refusal.value) == (
        f"cannot publish {exported}:\n  cannot convert {exported / 'body_broken_Normal.1001.png'}: "
        "rmanoiiotool failed\nlibpng error: IDAT: Read error: hit end of file"
    )
    assert list((root / TEX).iterdir()) == []
    assert registrations == []

    (exported / "notes").mkdir()
    with pytest.raises(PiperError, match="notes is not a file, and a Painter export is flat"):
        run(registry, root, renderman, exported)
    with pytest.raises(PiperError, match="is not a directory"):
        run(registry, root, renderman, exported / "body_BaseColor.1001.png")
    (root / TEX / "v001").mkdir()
    with pytest.raises(PiperError, match="inside a publish directory"):
        run(registry, root, renderman, root / TEX / "v001")


def refuse(registry: Registry, product: str, monkeypatch: pytest.MonkeyPatch) -> None:
    register = registry.register

    def refusing(asset: Asset, *, product: str, version: int, path: PurePosixPath) -> str:
        if product == refused:
            raise RegistryError(f"shotgrid: refused to register {product} v{version:03d}")
        return register(asset, product=product, version=version, path=path)

    refused = product
    monkeypatch.setattr(registry, "register", refusing)


def test_textures_that_did_not_register_derive_no_material(
    registry: Registry, root: Path, renderman: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    refuse(registry, "tex", monkeypatch)

    with pytest.raises(PartialPublishTexturesError) as raised:
        run(registry, root, renderman, export(tmp_path / "export", "body_BaseColor.1001"))

    result = raised.value.result
    assert (result.textures.version, result.textures.record_id, result.material) == (1, None, None)
    assert str(raised.value) == (
        f"installed {root / TEX / 'v001'}, but could not register it: shotgrid: refused to "
        "register tex v001; publishing again installs another version; no material was derived"
    )
    assert (root / TEX / "v001" / "body_BaseColor.1001.tex").is_file()


@pytest.mark.usefixtures("lookdev")
def test_a_material_that_could_not_be_registered_or_derived_leaves_the_textures(
    registry: Registry, root: Path, renderman: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    second = export(tmp_path / "second", "body_BaseColor.1001")

    refuse(registry, "mtl", monkeypatch)
    with pytest.raises(PartialPublishTexturesError) as raised:
        run(registry, root, renderman, second)

    result = raised.value.result
    assert result.material is not None and result.material.asset_version is None
    assert (result.textures.version, result.derived_from) == (2, 1)
    assert result.material.component.record_id is None
    assert re.fullmatch(
        r"published tex v002; installed .*/mtl/v002/mtl\.usda, but could not register it: .*; "
        r"publishing again installs another version; no asset version was built",
        str(raised.value),
    )

    shutil.rmtree(root / "asset" / "kitchen" / "frying_pan" / "publish" / "mtl" / "v001")
    with pytest.raises(PartialPublishTexturesError) as raised:
        run(registry, root, renderman, second)

    result = raised.value.result
    assert (result.textures.version, result.material, result.derived_from) == (3, None, None)
    assert str(raised.value) == (
        "published tex v003, but could not derive the material: Frying Pan has no mtl v001 "
        "(installed: v002); publishing again installs another version"
    )
    assert current_pins(PurePosixPath(root), PAN) == (2, {"geo": 1, "mtl": 1})
