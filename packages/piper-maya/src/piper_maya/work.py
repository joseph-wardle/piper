"""Opening an asset's work in Maya: its scene, its project, and the stamp saying whose it is."""

from pathlib import Path

from maya import cmds, mel

from piper.errors import PiperError
from piper.tracker import Asset, Tracker
from piper_studio.context import Context
from piper_studio.production import Production
from piper_studio.work import STAMP_KEYS, prepare_work, restamp_notice, stamp

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

    stamped = stamp(production, asset, context)
    previous = scene_stamp()
    if previous == stamped:
        return file
    notice = restamp_notice(tracker, production, previous, asset, context)
    for key, value in stamped.items():
        cmds.fileInfo(key, value)
    # Writing fileInfo does not mark the scene modified, so nothing later would save it.
    try:
        cmds.file(save=True, force=True, type=_FILE_TYPES[context.extension])
    except RuntimeError as exc:
        raise PiperError(
            f"{file} is open, but Maya could not save it ({str(exc).strip()})"
        ) from exc
    if notice is not None:
        cmds.confirmDialog(title="Piper", message=notice, button=["OK"])
    return file


def scene_stamp() -> dict[str, str | None]:
    """What the open scene says it is. A key the scene does not carry is None."""
    return {key: next(iter(cmds.fileInfo(key, query=True)), None) for key in STAMP_KEYS}
