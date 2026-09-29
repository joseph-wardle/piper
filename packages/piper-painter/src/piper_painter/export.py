"""What Painter exports for Piper: every map of the table, named as a tex version names them."""

from pathlib import Path

from piper_studio import textures

PRESET = "piper"
PREVIEW_SIZE_LOG2 = 10

# The Painter channel each map of the table is exported from: a
# document channel as painted, or a map Painter computes, such as the normal
# with height folded in. Painter's channels are named in its own case.
_SOURCES = {
    "BaseColor": ("documentMap", "baseColor", "RGB"),
    "Metallic": ("documentMap", "metallic", "L"),
    "SpecularRoughness": ("documentMap", "roughness", "L"),
    "Normal": ("virtualMap", "Normal_OpenGL", "RGB"),
    "Emissive": ("documentMap", "emissive", "RGB"),
    "Presence": ("documentMap", "opacity", "L"),
    "Displacement": ("documentMap", "height", "L"),
}
# Exported at 16 bits: converted to half or kept signed, where 8 bits would band.
_DEEP = ("BaseColor", "Emissive", "Normal", "Displacement")


def config(directory: Path, sets: list[str], *, preview: bool) -> dict[str, object]:
    """Painter's export configuration for ``sets`` into ``directory``."""
    return {
        "exportPath": str(directory),
        "exportShaderParams": False,
        "defaultExportPreset": PRESET,
        "exportPresets": [
            {"name": PRESET, "maps": [_map(name, preview) for name in textures.MAPS]}
        ],
        "exportList": [{"rootPath": name} for name in sets],
        "exportParameters": [{"parameters": {"paddingAlgorithm": "infinite", "dithering": False}}],
    }


def _map(name: str, preview: bool) -> dict[str, object]:
    kind, source, channels = _SOURCES[name]
    if preview:
        parameters: dict[str, object] = {
            "fileFormat": "jpeg",
            "bitDepth": "8",
            "sizeLog2": PREVIEW_SIZE_LOG2,
        }
    else:
        parameters = {"fileFormat": "png", "bitDepth": "16" if name in _DEEP else "8"}
    return {
        "fileName": f"$textureSet_{name}(.$udim)",
        "channels": [
            {
                "destChannel": channel,
                "srcChannel": channel,
                "srcMapType": kind,
                "srcMapName": source,
            }
            for channel in channels
        ],
        "parameters": parameters,
    }


def missing_maps(directory: Path, sets: list[str]) -> dict[str, list[str]]:
    """Each map of the table ``directory`` has no PNG of, with the sets lacking it."""
    exported = {
        (named["slot"], named["map"])
        for png in directory.glob("*.png")
        if (named := textures.NAMED.fullmatch(png.name))
    }
    lacking = {name: [s for s in sets if (s, name) not in exported] for name in textures.MAPS}
    return {name: lacked for name, lacked in lacking.items() if lacked}
