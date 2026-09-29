from pathlib import Path
from typing import Any

from piper_painter import export
from piper_studio import textures


def maps(config: dict[str, object]) -> list[dict[str, Any]]:
    """The one preset's maps."""
    presets = config["exportPresets"]
    assert isinstance(presets, list) and len(presets) == 1
    return presets[0]["maps"]


def test_every_map_of_the_table_is_exported_from_its_painter_channel() -> None:
    config = export.config(Path("/scratch/export"), ["paint", "metal"], preview=False)

    assert config["exportPath"] == str(Path("/scratch/export"))
    assert config["exportList"] == [{"rootPath": "paint"}, {"rootPath": "metal"}]
    assert [m["fileName"] for m in maps(config)] == [
        f"$textureSet_{name}(.$udim)" for name in textures.MAPS
    ]
    by_name = {m["fileName"].split("_")[1].split("(")[0]: m for m in maps(config)}
    assert by_name["BaseColor"]["channels"][0] == {
        "destChannel": "R",
        "srcChannel": "R",
        "srcMapType": "documentMap",
        "srcMapName": "baseColor",
    }
    assert [c["destChannel"] for c in by_name["Metallic"]["channels"]] == ["L"]
    assert by_name["Normal"]["channels"][0]["srcMapName"] == "Normal_OpenGL"
    assert by_name["BaseColor"]["parameters"] == {"fileFormat": "png", "bitDepth": "16"}
    assert by_name["SpecularRoughness"]["parameters"] == {"fileFormat": "png", "bitDepth": "8"}
    assert by_name["Normal"]["parameters"]["bitDepth"] == "16"
    # No colour space in a name: the extension says what the file is.
    assert "colorSpace" not in str(config)


def test_previews_are_the_same_maps_as_1k_jpegs() -> None:
    config = export.config(Path("/scratch/export"), ["paint"], preview=True)

    assert len(maps(config)) == len(textures.MAPS)
    assert {str(m["parameters"]) for m in maps(config)} == {
        str({"fileFormat": "jpeg", "bitDepth": "8", "sizeLog2": 10})
    }


def test_a_map_a_set_has_no_png_of_is_named_with_the_sets(tmp_path: Path) -> None:
    for name in textures.MAPS:
        (tmp_path / f"paint_{name}.1001.png").touch()
        if name not in ("Emissive", "Presence"):
            (tmp_path / f"metal_{name}.1002.png").touch()
    (tmp_path / "metal_Emissive.1002.jpeg").touch()

    assert export.missing_maps(tmp_path, ["paint", "metal"]) == {
        "Emissive": ["metal"],
        "Presence": ["metal"],
    }
