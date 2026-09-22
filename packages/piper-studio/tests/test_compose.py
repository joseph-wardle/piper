"""Building an asset version and reading its pins, against hand-written component layers."""

import textwrap
from pathlib import Path, PurePosixPath

import pytest
from pxr import Ar, Sdf, Usd, UsdGeom, UsdShade

from piper.errors import PiperError
from piper.tracker import Asset
from piper_studio import compose

PAN = Asset(id="7701", name="Frying Pan", type="Prop", folder="kitchen", pipe_name="frying_pan")

GEO = """
    #usda 1.0
    (
        defaultPrim = "frying_pan"
        metersPerUnit = 1
        upAxis = "Y"
    )

    def Xform "frying_pan" (
        kind = "component"
    )
    {
        def Scope "geo"
        {
            def Xform "render"
            {
                uniform token purpose = "render"

                def Mesh "body" (
                    prepend apiSchemas = ["MaterialBindingAPI"]
                )
                {
                    float3[] extent = [(-1, -1, -1), (1, 1, 1)]
                    rel material:binding = </frying_pan/mtl/ironSG>
                }
            }
        }

        def Scope "mtl"
        {
            def Material "ironSG"
            {
            }
        }
    }
"""

# What Houdini writes above a Layer Break: overs on the slot, definitions inside it.
MTL = """
    #usda 1.0
    (
        defaultPrim = "frying_pan"
        metersPerUnit = 1
        upAxis = "Y"
    )

    over "frying_pan"
    {
        over "mtl"
        {
            over "ironSG"
            {
                token outputs:ri:surface.connect = </frying_pan/mtl/ironSG/rman.outputs:out>

                def Shader "rman"
                {
                    uniform token info:id = "PxrSurface"
                    token outputs:out
                }
            }
        }
    }
"""


@pytest.fixture
def root(tmp_path: Path) -> PurePosixPath:
    root = tmp_path / "production"
    (root / "asset" / "kitchen" / "frying_pan").mkdir(parents=True)
    return PurePosixPath(root)


def install(root: Path | PurePosixPath, product: str, version: int, text: str) -> Path:
    """A component version as `publish_product` leaves it, without going through it."""
    layer = Path(
        root, "asset/kitchen/frying_pan/publish", product, f"v{version:03d}", f"{product}.usda"
    )
    layer.parent.mkdir(parents=True)
    layer.write_text(textwrap.dedent(text).lstrip(), encoding="utf-8")
    return layer


def build(root: PurePosixPath, tmp_path: Path, pins: dict[str, int], version: int = 1) -> Path:
    """Write an asset version and install it where `publish_product` would."""
    written = compose.write_asset_version(
        tmp_path / f"staged{version}", root=root, asset=PAN, pins=pins
    )
    installed = compose.entry_path(root, PAN, version)
    installed.parent.mkdir(parents=True)
    for file in written.parent.iterdir():
        (installed.parent / file.name).write_bytes(file.read_bytes())
    return installed


def test_the_entry_composes_the_pinned_geometry_and_material(
    root: PurePosixPath, tmp_path: Path
) -> None:
    install(root, "geo", 1, GEO)
    install(root, "mtl", 1, MTL)
    (tmp_path / "staged1").mkdir()

    entry = build(root, tmp_path, {"geo": 1, "mtl": 1})

    stage = Usd.Stage.Open(str(entry), Ar.DefaultResolverContext([str(root)]), Usd.Stage.LoadAll)
    model = stage.GetPrimAtPath("/frying_pan")
    body = stage.GetPrimAtPath("/frying_pan/geo/render/body")
    surface = UsdShade.Material(stage.GetPrimAtPath("/frying_pan/mtl/ironSG")).ComputeSurfaceSource(
        "ri"
    )
    assert stage.GetCompositionErrors() == []
    assert stage.GetDefaultPrim() == model
    assert (Usd.ModelAPI(model).GetKind(), model.GetAssetInfo()) == (
        "component",
        {"name": "frying_pan"},
    )
    assert model.GetInherits().GetAllDirectInherits() == [Sdf.Path("/__class__/frying_pan")]
    assert UsdGeom.Imageable(body).ComputePurpose() == UsdGeom.Tokens.render
    assert UsdShade.MaterialBindingAPI(body).ComputeBoundMaterial()[0].GetPath().name == "ironSG"
    assert surface[0].GetPrim().GetPath() == Sdf.Path("/frying_pan/mtl/ironSG/rman")
    assert (UsdGeom.GetStageUpAxis(stage), UsdGeom.GetStageMetersPerUnit(stage)) == ("Y", 1.0)
    assert sorted(path.name for path in entry.parent.iterdir()) == [
        "frying_pan.usda",
        "payload.usda",
    ]


def test_the_payload_is_readable_as_pins_and_spelled_from_the_root(
    root: PurePosixPath, tmp_path: Path
) -> None:
    install(root, "geo", 2, GEO)
    install(root, "mtl", 1, MTL)
    (tmp_path / "staged1").mkdir()

    entry = build(root, tmp_path, {"mtl": 1, "geo": 2})

    payload = (entry.parent / "payload.usda").read_text(encoding="utf-8")
    assert "@asset/kitchen/frying_pan/publish/geo/v002/geo.usda@" in payload
    assert "@asset/kitchen/frying_pan/publish/mtl/v001/mtl.usda@" in payload
    assert compose.pins(root, PAN, 1) == {"geo": 2, "mtl": 1}


def test_a_version_with_one_component_pins_that_alone(root: PurePosixPath, tmp_path: Path) -> None:
    install(root, "geo", 1, GEO)
    (tmp_path / "staged1").mkdir()

    entry = build(root, tmp_path, {"geo": 1})

    stage = Usd.Stage.Open(str(entry), Ar.DefaultResolverContext([str(root)]), Usd.Stage.LoadAll)
    assert stage.GetCompositionErrors() == []
    assert stage.GetPrimAtPath("/frying_pan/geo/render/body")
    assert compose.pins(root, PAN, 1) == {"geo": 1}


def test_a_pin_names_a_version_that_is_installed(root: PurePosixPath, tmp_path: Path) -> None:
    install(root, "geo", 1, GEO)
    install(root, "geo", 3, GEO)

    with pytest.raises(PiperError, match=r"has no geo v002 \(installed: v001, v003\)"):
        compose.write_asset_version(tmp_path, root=root, asset=PAN, pins={"geo": 2})
    with pytest.raises(PiperError, match=r"has no mtl v001 \(installed: none\)"):
        compose.write_asset_version(tmp_path, root=root, asset=PAN, pins={"mtl": 1})
    with pytest.raises(PiperError, match="has no readable asset v001"):
        compose.pins(root, PAN, 1)
    assert compose.versions(root, PAN, "geo") == [1, 3]
    assert compose.versions(root, PAN, "asset") == []
