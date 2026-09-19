"""An asset's work area: the mutable files a context is authored in."""

from pathlib import Path, PurePosixPath

from piper.errors import PiperError
from piper.tracker import Asset
from piper_studio import layout
from piper_studio.context import Context
from piper_studio.storage import asset_directory


def prepare_work(*, root: PurePosixPath, asset: Asset, context: Context) -> Path:
    """Make the directory ``asset``'s work in ``context`` is kept in, and name its work file.

    The file itself is the host's to create. Safe to repeat.
    """
    if context.subject != "asset":
        raise PiperError(
            f"{context.name} is {context.subject} work, and {asset.name!r} is an asset"
        )
    file = Path(layout.work_file(PurePosixPath(asset_directory(root, asset)), context))
    try:
        file.parent.mkdir(parents=True)
    except OSError as exc:
        # Another artist opening the same work may have made it first.
        if not file.parent.is_dir():
            raise PiperError(
                f"cannot open {context.name} work on {asset.name!r}: {file.parent} could not "
                f"be created ({exc.strerror})"
            ) from exc
    return file
