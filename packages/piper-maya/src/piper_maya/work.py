"""Opening an asset's work in Maya: its scene, its project, and the stamp saying whose it is."""

from pathlib import Path

from maya import cmds, mel

from piper.errors import PiperError
from piper.tracker import Asset, Tracker
from piper_studio.context import Context
from piper_studio.production import Production
from piper_studio.work import prepare_work

PRODUCTION_KEY = "piper_production"
ASSET_ID_KEY = "piper_asset_id"
CONTEXT_KEY = "piper_context"

_FILE_TYPES = {"mb": "mayaBinary", "ma": "mayaAscii"}


def open_work(
    tracker: Tracker, production: Production, asset: Asset, context: Context
) -> Path | None:
    """Open ``asset``'s work in ``context``, starting it when there is none yet."""
    file = prepare_work(root=production.root, asset=asset, context=context)
    if not mel.eval('saveChanges("")'):
        return None

    project = cmds.workspace(query=True, rootDirectory=True)
    cmds.workspace(str(file.parent), openWorkspace=True)
    try:
        if file.is_file():
            cmds.file(str(file), open=True, force=True)
        else:
            cmds.file(new=True, force=True)
            cmds.file(rename=str(file))
    except RuntimeError as exc:
        # Maya keeps the scene it had when it cannot read a file, so that scene keeps its project.
        cmds.workspace(project, openWorkspace=True)
        raise PiperError(f"Maya could not open {file} ({str(exc).strip()})") from exc

    stamp = {PRODUCTION_KEY: production.name, ASSET_ID_KEY: asset.id, CONTEXT_KEY: context.name}
    previous = scene_stamp()
    if previous == stamp:
        return file
    was = _described(tracker, production, previous) if any(previous.values()) else None
    for key, value in stamp.items():
        cmds.fileInfo(key, value)
    # Writing fileInfo does not mark the scene modified, so nothing later would save it.
    try:
        cmds.file(save=True, force=True, type=_FILE_TYPES[context.extension])
    except RuntimeError as exc:
        raise PiperError(
            f"{file} is open, but Maya could not save it ({str(exc).strip()})"
        ) from exc
    if was is not None:
        cmds.confirmDialog(
            title="Piper",
            message=f"This file was {was}.\n\nIt is now {context.name} work on {asset.name}, "
            "and has been saved that way.",
            button=["OK"],
        )
    return file


def scene_stamp() -> dict[str, str | None]:
    """What the open scene says it is. A key the scene does not carry is None."""
    keys = (PRODUCTION_KEY, ASSET_ID_KEY, CONTEXT_KEY)
    return {key: next(iter(cmds.fileInfo(key, query=True)), None) for key in keys}


def _described(tracker: Tracker, production: Production, stamp: dict[str, str | None]) -> str:
    # An id means something only to the production whose tracker gave it.
    known = stamp[PRODUCTION_KEY] == production.name and stamp[ASSET_ID_KEY]
    asset = tracker.asset(known) if known else None
    name = asset.name if asset is not None else "an asset this production does not have"
    context = stamp[CONTEXT_KEY] or "unnamed"
    return f"{context} work on {name}, in {stamp[PRODUCTION_KEY] or 'no production'}"
