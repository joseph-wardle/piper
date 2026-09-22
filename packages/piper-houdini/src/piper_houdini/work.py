"""Opening an asset's work in Houdini: its file, its starter network, and the stamp saying whose."""

from pathlib import Path, PurePosixPath

import hou

from piper.errors import PiperError
from piper.tracker import Asset, Tracker
from piper_studio import compose
from piper_studio.context import Context
from piper_studio.production import Production
from piper_studio.storage import asset_directory
from piper_studio.work import STAMP_KEYS, prepare_work, restamp_notice, stamp

STAGE = "/stage"
PRODUCT = "mtl"
OUTPUT_NAME = f"OUT_{PRODUCT}"
OUTPUT = f"{STAGE}/{OUTPUT_NAME}"


def open_work(
    tracker: Tracker, production: Production, asset: Asset, context: Context
) -> Path | None:
    """Open ``asset``'s work in ``context``, starting it on the current asset version when new."""
    file = prepare_work(root=production.root, asset=asset, context=context)
    warned = None
    try:
        if file.is_file():
            try:
                hou.hipFile.load(str(file))
            except hou.LoadWarning as warning:
                # Raised once the file is open.
                warned = str(warning).strip()
        else:
            pinned = compose.current(production.root, asset)
            if pinned is None:
                raise PiperError(
                    f"nothing is current for {asset.name}, so there is no asset to look at "
                    "yet; publish its geo first"
                )
            hou.hipFile.clear()
            hou.hipFile.setName(str(file))
            build_starter(production.root, asset, pinned)
    except hou.OperationInterrupted:
        # The artist kept the scene they had, at Houdini's own prompt to save it.
        return None
    except hou.OperationFailed as exc:
        raise PiperError(
            f"Houdini could not open {file} ({exc.instanceMessage().strip()})"
        ) from exc

    stamped = stamp(production, asset, context)
    previous = scene_stamp()
    if previous != stamped:
        notice = restamp_notice(tracker, production, previous, asset, context)
        for key, value in stamped.items():
            hou.node("/").setUserData(key, value)
        try:
            hou.hipFile.save()
        except hou.OperationFailed as exc:
            raise PiperError(
                f"{file} is open, but Houdini could not save it ({exc.instanceMessage().strip()})"
            ) from exc
        if notice is not None:
            hou.ui.displayMessage(notice, title="Piper")
    if warned is not None:
        raise PiperError(f"{file} is open, with Houdini's warnings: {warned}")
    return file


def build_starter(root: PurePosixPath, asset: Asset, version: int) -> None:
    """The network lookdev starts from."""
    pipe_name = asset_directory(root, asset).name
    entry = PurePosixPath(compose.entry_path(root, asset, version)).relative_to(root)
    stage = hou.node(STAGE)
    loaded = stage.createNode("sublayer", compose.ASSET)
    loaded.parm("filepath1").set(str(entry))
    layer_break = stage.createNode("layerbreak", f"break_{PRODUCT}")
    layer_break.setInput(0, loaded)
    materials = stage.createNode("materiallibrary", "materials")
    materials.setInput(0, layer_break)
    materials.parm("matpathprefix").set(f"/{pipe_name}/{PRODUCT}/")
    materials.parm("matnode1").set("*")
    materials.parm("matflag1").set(1)
    materials.parm("assign1").set(0)
    output = stage.createNode("null", OUTPUT_NAME)
    output.setInput(0, materials)
    dome = stage.createNode("domelight::3.0", "view_dome")
    dome.setInput(0, output)
    camera = stage.createNode("camera", "thumbnail_cam")
    camera.setInput(0, dome)
    karma = stage.createNode("karmarenderproperties", "view_karma")
    karma.setInput(0, camera)
    karma.setDisplayFlag(True)
    stage.layoutChildren()


def scene_stamp() -> dict[str, str | None]:
    """What the open scene says it is. A key the scene does not carry is None."""
    return {key: hou.node("/").userData(key) for key in STAMP_KEYS}
