"""Where this studio keeps production entities on disk."""

import re
from pathlib import PurePosixPath

_APOSTROPHES = re.compile(r"['\u2019]")
_SEPARATORS = re.compile(r"[^a-z0-9]+")


def slug(name: str) -> str:
    """Spell a readable name for a path: ``"Mr. Yoon's Shop"`` becomes ``mr_yoons_shop``.

    Empty when the name has no letters or digits to keep.
    """
    lowered = _APOSTROPHES.sub("", name.lower())
    return _SEPARATORS.sub("_", lowered).strip("_")


def asset_root(root: PurePosixPath, folder: str, name: str) -> PurePosixPath:
    """The directory that holds everything belonging to one asset."""
    return root / "asset" / slug(folder) / slug(name)
