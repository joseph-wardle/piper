"""An asset's work area: the mutable files a context is authored in, and the stamp saying whose."""

from collections.abc import Mapping
from pathlib import Path, PurePosixPath

from piper.errors import PiperError
from piper.tracker import Asset, Tracker
from piper_studio import layout
from piper_studio.context import Context, context_named
from piper_studio.production import Production
from piper_studio.storage import asset_directory

PRODUCTION_KEY = "piper_production"
ASSET_ID_KEY = "piper_asset_id"
CONTEXT_KEY = "piper_context"
STAMP_KEYS = (PRODUCTION_KEY, ASSET_ID_KEY, CONTEXT_KEY)


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


def stamp(production: Production, asset: Asset, context: Context) -> dict[str, str]:
    """What a work file carries, in its host's own metadata, to say whose work it is."""
    return {PRODUCTION_KEY: production.name, ASSET_ID_KEY: asset.id, CONTEXT_KEY: context.name}


def stamped_asset(
    tracker: Tracker,
    production: Production,
    carried: Mapping[str, str | None],
    scene: Path,
    remedy: str,
) -> Asset:
    """The asset the open scene is work on, refusing a scene that is not that work's own file.

    ``carried`` is the scene's stamp, a key it lacks None; ``scene`` is where the
    host has it; ``remedy`` tells the artist what to do with any other scene.
    """
    asset_id = carried[ASSET_ID_KEY]
    # A scene the host has never saved carries no stamp, so this refuses it too.
    if carried[PRODUCTION_KEY] != production.name or not asset_id:
        raise PiperError(f"this scene is not {production.name} work; {remedy}")
    asset = tracker.asset(asset_id)
    if asset is None:
        raise PiperError(
            f"this scene is work on an asset {production.name} does not have (id {asset_id})"
        )
    context = context_named(carried[CONTEXT_KEY] or "", subject="asset")
    directory = PurePosixPath(asset_directory(production.root, asset))
    expected = Path(layout.work_file(directory, context))
    if scene.resolve() != expected.resolve():
        raise PiperError(
            f"this scene is {scene}, not {asset.name}'s {context.name} work at {expected}; {remedy}"
        )
    return asset


def restamp_notice(
    tracker: Tracker,
    production: Production,
    carried: Mapping[str, str | None],
    asset: Asset,
    context: Context,
) -> str | None:
    """What to tell an artist whose file is now ``asset``'s work in ``context``.

    ``carried`` is the stamp the file had. None for a file that carried none,
    which is new work and needs no telling.
    """
    if not any(carried.values()):
        return None
    # An id means something only to the production whose tracker gave it.
    known = carried[PRODUCTION_KEY] == production.name and carried[ASSET_ID_KEY]
    was = tracker.asset(known) if known else None
    name = was.name if was is not None else "an asset this production does not have"
    return (
        f"This file was {carried[CONTEXT_KEY] or 'unnamed'} work on {name}, in "
        f"{carried[PRODUCTION_KEY] or 'no production'}.\n\nIt is now {context.name} work on "
        f"{asset.name}, and has been saved that way."
    )
