"""The maps a Painter export holds, and their conversion into RenderMan textures."""

import os
import re
import subprocess
from pathlib import Path

from piper.errors import PiperError

RENDERMAN_VERSION = "27.3"

# Painted in sRGB;
COLOUR_MAPS = ("BaseColor", "Emissive")
DATA_MAPS = ("Metallic", "SpecularRoughness", "Normal", "Displacement", "Presence")
MAPS = (*COLOUR_MAPS, *DATA_MAPS)

_EXPORTED = re.compile(r"(?P<slot>.+)_(?P<map>[A-Za-z]+)\.(?P<udim>\d{4})\.png")


def convert(directory: Path, *, renderman: Path) -> list[Path]:
    """Write a RenderMan texture beside every exported PNG in ``directory``."""
    if not directory.is_dir():
        raise PiperError(f"cannot convert {directory}: it is not a directory")
    exported = sorted(directory.glob("*.png"))
    if not exported:
        raise PiperError(f"cannot convert {directory}: it holds no PNG")
    maps = {png: _map_name(png) for png in exported}
    oiiotool = renderman / "bin" / "rmanoiiotool"
    if not os.access(oiiotool, os.X_OK):
        raise PiperError(
            f"cannot convert {directory}: RenderMan {RENDERMAN_VERSION} is not installed at "
            f"{renderman}; set RMANTREE to where it is"
        )
    for png in exported:
        png.with_suffix(".tex").unlink(missing_ok=True)

    textures: list[Path] = []
    for png, name in maps.items():
        texture = png.with_suffix(".tex")
        command = _command(png, texture, colour=name in COLOUR_MAPS, renderman=renderman)
        ran = subprocess.run(command, capture_output=True, text=True, check=False)
        # A truncated PNG makes oiiotool complain on stderr, write a texture, and exit 0.
        if ran.returncode != 0 or ran.stderr:
            texture.unlink(missing_ok=True)
            raise PiperError(f"cannot convert {png}: rmanoiiotool failed\n{ran.stderr.strip()}")
        textures.append(texture)
    return textures


def renderman_install() -> Path:
    """Where RenderMan is installed on this machine: ``RMANTREE``, or its usual place."""
    located = os.environ.get("RMANTREE")
    return Path(located) if located else Path(f"/opt/pixar/RenderManProServer-{RENDERMAN_VERSION}")


def _map_name(png: Path) -> str:
    matched = _EXPORTED.fullmatch(png.name)
    if matched is None:
        raise PiperError(
            f"cannot convert {png}: a texture is named <slot>_<map>.<udim>.png, "
            "as Painter exports it"
        )
    name = matched.group("map")
    if name not in MAPS:
        known = ", ".join(MAPS)
        raise PiperError(f"cannot convert {png}: {name!r} is not a map Piper knows ({known})")
    return name


def _command(png: Path, texture: Path, *, colour: bool, renderman: Path) -> list[str]:
    """The oiiotool call that converts ``png`` into ``texture``."""
    call = [str(renderman / "bin" / "rmanoiiotool")]
    if colour:
        config = renderman / "lib" / "ocio" / "ACES-1.3" / "config.ocio"
        call += ["--colorconfig", str(config), str(png)]
        call += ["--colorconvert", "sRGB - Texture", "ACEScg", "-d", "half"]
        container = "exr"
    else:
        call += [str(png)]
        container = "tiff"
    call += ["--compression", "zip", "--planarconfig", "separate"]
    call += [f"-otex:fileformatname={container}:wrap=clamp:resize=1:prman_options=1", str(texture)]
    return call
