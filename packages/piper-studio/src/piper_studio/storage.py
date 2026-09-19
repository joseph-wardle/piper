"""What every operation reads from and changes in production storage the same way."""

import os
import stat
from pathlib import Path, PurePosixPath

from piper.errors import PiperError
from piper.tracker import Asset
from piper_studio.layout import asset_root, slug


def asset_directory(root: PurePosixPath, asset: Asset) -> Path:
    """The directory creating ``asset`` made, refusing an asset that has none."""
    folder, pipe_name = asset.folder or "", asset.pipe_name or ""
    if not slug(folder):
        raise PiperError(
            f"asset {asset.name!r} has no folder, and so no directory; "
            "give it a folder in the tracker"
        )
    if not pipe_name:
        raise PiperError(
            f"asset {asset.name!r} has no pipe name, and so no directory; "
            "`piper create asset` gives it one"
        )
    if slug(pipe_name) != pipe_name:
        raise PiperError(unusable_pipe_name(asset))
    directory = Path(asset_root(root, folder, pipe_name))
    if not directory.is_dir():
        raise PiperError(
            f"asset {asset.name!r} has no directory at {directory}; `piper create asset` makes it"
        )
    return directory


def unusable_pipe_name(asset: Asset) -> str:
    """The refusal of a pipe name somebody typed into the tracker that cannot name a directory."""
    return (
        f"asset {asset.name!r} has the pipe name {asset.pipe_name!r}, which cannot name its "
        "directory: a pipe name is lowercase letters, digits, and underscores; "
        "correct it in the tracker"
    )


def make_directories(directory: Path) -> None:
    """Make ``directory`` and its missing parents, copying the nearest directory that exists."""
    missing: list[Path] = []
    existing = directory
    while not existing.is_dir():
        missing.append(existing)
        existing = existing.parent
    template = existing.stat()
    for path in reversed(missing):
        try:
            path.mkdir()
        except FileExistsError:
            if path == directory:
                raise
            continue
        os.chown(path, -1, template.st_gid)
        path.chmod(stat.S_IMODE(template.st_mode))
