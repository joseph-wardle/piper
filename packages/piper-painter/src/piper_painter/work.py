"""Opening an asset's work in Painter: its project, from the pinned geo when new, and its stamp."""

from collections.abc import Callable
from pathlib import Path

from substance_painter import event, project
from substance_painter.exception import ProjectError

from piper.errors import PiperError
from piper.tracker import Asset, Tracker
from piper_painter import command
from piper_studio.context import Context
from piper_studio.layout import version_name
from piper_studio.production import Production
from piper_studio.storage import layer_file
from piper_studio.work import STAMP_KEYS, prepare_work, restamp_notice, stamp

METADATA = "piper"
GEOMETRY = "geo"
GEO_KEY = "piper_geo"


def open_work(
    tracker: Tracker,
    production: Production,
    asset: Asset,
    context: Context,
    shown: Callable[[str], None],
) -> Path:
    """Open ``asset``'s work in ``context``, making its project from the pinned geo when new.

    Painter goes on loading after the call returns, so the project is stamped,
    saved, and ``shown`` what to know once it can be edited.
    """
    file = prepare_work(root=production.local_root, asset=asset, context=context)
    pins = current_pins(asset)
    geo = pins.get(GEOMETRY)
    new = not file.is_file()
    if new and geo is None:
        raise PiperError(
            f"nothing current for {asset.name} pins a {GEOMETRY}, so there is no model to "
            "paint yet; publish its geo first"
        )
    mesh = layer_file(production.local_root, asset, GEOMETRY, geo) if geo is not None else None
    if new and mesh is None:
        raise PiperError(
            f"{asset.name}'s current asset version pins {GEOMETRY} {version_name(geo or 0)}, "
            "which is not installed"
        )
    try:
        if project.is_open():
            project.close()
        if new:
            settings = project.Settings(
                normal_map_format=project.NormalMapFormat.OpenGL,
                project_workflow=project.ProjectWorkflow.UVTile,
            )
            project.create(mesh_file_path=str(mesh), settings=settings)
        else:
            project.open(str(file))
    except ProjectError as exc:
        raise PiperError(f"Painter could not open {file} ({exc})") from exc

    def finish() -> None:
        # Painter may have opened another project meanwhile, or never opened this one.
        if not _opened_as_asked(file, mesh if new else None):
            return
        try:
            lines = _stamp_and_save(tracker, production, asset, context, file, geo, new)
            if not new and geo is not None:
                lines += _older_geo_line(geo, mesh)
        except PiperError as refusal:
            lines = [str(refusal)]
        if lines:
            shown("\n\n".join(lines))

    when_editable(finish)
    return file


def current_pins(asset: Asset) -> dict[str, int]:
    """What the current asset version pins, read through the command line."""
    payload = command.query("current", asset.name)
    pins = payload.get("pins")
    return dict(pins) if isinstance(pins, dict) else {}


def scene_stamp() -> dict[str, str | None]:
    """What the open project says it is. A key the project does not carry is None."""
    metadata = project.Metadata(METADATA)
    carried = metadata.list()
    return {key: str(metadata.get(key)) if key in carried else None for key in STAMP_KEYS}


def open_file() -> Path | None:
    """The open project's file; None for a project never saved."""
    try:
        path = project.file_path()
    except ProjectError:
        return None
    return Path(path) if path else None


def _opened_as_asked(file: Path, mesh: Path | None) -> bool:
    """Whether Painter has the project asked for open: one made from ``mesh``, or ``file``."""
    if mesh is not None:
        return Path(project.last_imported_mesh_path()) == mesh
    return open_file() == file


def when_editable(callback: Callable[[], None]) -> None:
    """Run ``callback`` once the project is open, loaded, and idle."""
    if not project.is_open() or not project.is_in_edition_state():
        _once_edition_entered(lambda: when_editable(callback))
    elif project.is_busy():
        project.execute_when_not_busy(lambda: when_editable(callback))
    else:
        callback()


def _once_edition_entered(callback: Callable[[], None]) -> None:
    def entered(_event: event.Event) -> None:
        event.DISPATCHER.disconnect(event.ProjectEditionEntered, entered)
        callback()

    event.DISPATCHER.connect_strong(event.ProjectEditionEntered, entered)


def _stamp_and_save(
    tracker: Tracker,
    production: Production,
    asset: Asset,
    context: Context,
    file: Path,
    geo: int | None,
    new: bool,
) -> list[str]:
    """Stamp the project as ``asset``'s work and save it; what to tell the artist about that."""
    stamped = stamp(production, asset, context)
    previous = scene_stamp()
    if previous == stamped and not new:
        return []
    notice = restamp_notice(tracker, production, previous, asset, context)
    metadata = project.Metadata(METADATA)
    for key, value in stamped.items():
        metadata.set(key, value)
    if new and geo is not None:
        metadata.set(GEO_KEY, geo)
    try:
        if new:
            project.save_as(str(file), project.ProjectSaveMode.Full)
        else:
            project.save()
    except ProjectError as exc:
        raise PiperError(f"{file} is open, but Painter could not save it ({exc})") from exc
    return [notice] if notice is not None else []


def _older_geo_line(geo: int, mesh: Path | None) -> list[str]:
    """A notice that current pins a newer geo than the project was made from."""
    metadata = project.Metadata(METADATA)
    made = metadata.get(GEO_KEY) if GEO_KEY in metadata.list() else None
    if made is None or int(made) == geo:
        return []
    return [
        f"Current pins {GEOMETRY} {version_name(geo)}; this project was made from "
        f"{GEOMETRY} {version_name(int(made))}. Reload the mesh from {mesh} in Painter, "
        "keeping strokes, to paint on the newer model."
    ]
