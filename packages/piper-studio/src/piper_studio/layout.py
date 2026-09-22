"""Where this studio keeps production entities on disk."""

import re
from pathlib import PurePosixPath

from piper_studio.context import Context

_APOSTROPHES = re.compile(r"['\u2019]")
_SEPARATORS = re.compile(r"[^a-z0-9]+")
_VERSION = re.compile(r"v([0-9]{3,})")
LAYER_SUFFIXES = (".usd", ".usda", ".usdc")


def slug(name: str) -> str:
    """Spell a readable name for a path: ``"Mr. Yoon's Shop"`` becomes ``mr_yoons_shop``.

    Empty when the name has no letters or digits to keep.
    """
    lowered = _APOSTROPHES.sub("", name.lower())
    return _SEPARATORS.sub("_", lowered).strip("_")


def usable_pipe_name(pipe_name: str) -> bool:
    """Whether a pipe name can name both an asset's directory and its USD root prim."""
    return slug(pipe_name) == pipe_name and pipe_name[:1].isalpha()


def asset_root(root: PurePosixPath, folder: str, pipe_name: str) -> PurePosixPath:
    """The directory that holds everything belonging to one asset."""
    return root / "asset" / slug(folder) / slug(pipe_name)


def product_root(asset_root: PurePosixPath, product: str) -> PurePosixPath:
    """The directory that holds every version of one of an asset's products."""
    return asset_root / "publish" / product


def work_file(asset_root: PurePosixPath, context: Context) -> PurePosixPath:
    """The one file an asset's work in ``context`` is kept in, named for the asset."""
    return asset_root / "work" / context.name / f"{asset_root.name}.{context.extension}"


def version_name(number: int) -> str:
    """Spell a version number as its directory name: 4 becomes ``v004``."""
    return f"v{number:03d}"


def version_number(name: str) -> int | None:
    """Read a version directory's number: ``v004`` and ``v1000`` are 4 and 1000.

    None for a name that is not a version.
    """
    matched = _VERSION.fullmatch(name)
    return int(matched[1]) if matched else None


def version_directory(root: PurePosixPath, path: PurePosixPath) -> PurePosixPath | None:
    """The product version directory that ``path`` lies inside, if it lies inside one."""
    if not path.is_relative_to(root):
        return None
    parts = path.relative_to(root).parts
    # asset/<folder>/<name>/publish/<product>/<version>/<file>
    if len(parts) < 7 or parts[0] != "asset" or parts[3] != "publish":
        return None
    if version_number(parts[5]) is None:
        return None
    return root.joinpath(*parts[:6])
