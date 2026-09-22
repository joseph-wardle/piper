"""What every operation reads from production storage the same way."""

from pathlib import Path, PurePosixPath

from piper.errors import PiperError
from piper.tracker import Asset
from piper_studio.layout import asset_root, slug, usable_pipe_name


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
    if not usable_pipe_name(pipe_name):
        raise PiperError(pipe_name_refusal(asset))
    directory = Path(asset_root(root, folder, pipe_name))
    if not directory.is_dir():
        raise PiperError(
            f"asset {asset.name!r} has no directory at {directory}; `piper create asset` makes it"
        )
    return directory


def pipe_name_refusal(asset: Asset) -> str:
    """The refusal of a pipe name somebody typed into the tracker that cannot name its paths."""
    return (
        f"asset {asset.name!r} has the pipe name {asset.pipe_name!r}, which cannot name its "
        "directory or its USD prim: a pipe name is lowercase letters, digits, and "
        "underscores, and starts with a letter; correct it in the tracker"
    )
