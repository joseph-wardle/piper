"""What Piper adds to Painter's interface: its menu items, and the work they do."""

import contextlib
import functools
import os
import tempfile
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import ParamSpec

from PySide6 import QtCore, QtGui, QtWidgets
from substance_painter import project
from substance_painter import ui as painter_ui
from substance_painter.exception import ProjectError

from piper.errors import PiperError
from piper.tracker import Tracker
from piper_painter.publish import (
    export_textures,
    publish_work,
    scene_asset,
    scene_file,
    texture_sets,
)
from piper_painter.work import open_file, open_work
from piper_studio import profile, textures
from piper_studio.context import CONTEXTS, context_named
from piper_studio.launch import OPEN_ENV
from piper_studio.production import Production
from piper_studio.tracker import tracker_for

_P = ParamSpec("_P")
_actions: list[QtGui.QAction] = []


def refusals_shown(action: Callable[_P, None]) -> Callable[_P, None]:
    """Show an artist what Piper refused, where a script error would go unseen."""

    @functools.wraps(action)
    def shown(*args: _P.args, **kwargs: _P.kwargs) -> None:
        try:
            action(*args, **kwargs)
        except PiperError as refusal:
            QtWidgets.QMessageBox.warning(painter_ui.get_main_window(), "Piper", str(refusal))

    return shown


def start() -> None:
    """Add Piper's items to the File menu, then open the work the launch named, if it named any."""
    for label, act in (
        ("Piper: Open Work…", show_open_work),
        ("Piper: Publish…", show_publish),
        ("Piper: Export Textures…", show_export),
    ):
        action = QtGui.QAction(label, painter_ui.get_main_window())
        action.triggered.connect(functools.partial(_run, act))
        painter_ui.add_action(painter_ui.ApplicationMenu.File, action)
        _actions.append(action)
    work = os.environ.pop(OPEN_ENV, "")
    if work:
        # Once Painter's own startup has had the event loop.
        QtCore.QTimer.singleShot(0, functools.partial(open_launched_work, *work.split()))


def stop() -> None:
    for action in _actions:
        painter_ui.delete_ui_element(action)
    _actions.clear()


def _run(act: Callable[[], None], *_: object) -> None:
    act()


@refusals_shown
def open_launched_work(asset_id: str, context_name: str) -> None:
    """Open the work ``piper open`` named."""
    production, tracker = _production_and_tracker()
    asset = tracker.asset(asset_id)
    if asset is None:
        raise PiperError(f"{production.name} has no asset with the id {asset_id}")
    context = context_named(context_name, subject="asset")
    if _leaving_open_project():
        open_work(tracker, production, asset, context, _show)


@refusals_shown
def show_open_work() -> None:
    """Show the list an artist picks an asset from."""
    production, tracker = _production_and_tracker()
    [context] = [c for c in CONTEXTS if (c.subject, c.host) == ("asset", "painter")]
    found = tracker.find_assets("")
    rows = [f"{asset.name}    ({asset.folder or 'no folder'})" for asset in found]
    chosen, ok = QtWidgets.QInputDialog.getItem(
        painter_ui.get_main_window(),
        f"Open Work — {production.name}",
        f"Open {context.name} work on",
        rows,
        0,
        False,
    )
    if ok and chosen in rows and _leaving_open_project():
        open_work(tracker, production, found[rows.index(chosen)], context, _show)


@refusals_shown
def show_publish() -> None:
    """Say what the open project would publish, and publish it when the artist agrees."""
    production, tracker = _production_and_tracker()
    asset = scene_asset(tracker, production)
    sets = texture_sets()
    lines = [
        f"Publish to {asset.name}, {textures.PRODUCT}?",
        "",
        f"Texture sets: {', '.join(sets) or 'none'}",
        "",
        "The project is saved first. Then every map of every set is exported, converted, and "
        "installed, and the current material is derived to read them. This takes minutes, "
        "and Painter waits.",
    ]
    buttons = QtWidgets.QMessageBox.StandardButton
    answer = QtWidgets.QMessageBox.question(
        painter_ui.get_main_window(), "Piper", "\n".join(lines), buttons.Ok | buttons.Cancel
    )
    if answer != buttons.Ok:
        return
    try:
        project.save()
    except ProjectError as exc:
        raise PiperError(
            f"Painter could not save the project ({exc}), so nothing was published"
        ) from exc
    scene = scene_file()
    with (
        _waiting(),
        tempfile.TemporaryDirectory(
            prefix="piper_export_", ignore_cleanup_errors=True
        ) as directory,
    ):
        warnings = export_textures(Path(directory), sets)
        said = publish_work(asset, Path(directory), scene)
    _show("\n\n".join([said, *warnings]))


@refusals_shown
def show_export() -> None:
    """Export every map of every set into a folder the artist picks, with RenderMan textures.

    Needs no production: a class project exports as a production's publish does.
    """
    if not project.is_open():
        raise PiperError("no project is open")
    saved = open_file()
    chosen = QtWidgets.QFileDialog.getExistingDirectory(
        painter_ui.get_main_window(), "Export Textures", str(saved.parent) if saved else ""
    )
    if not chosen:
        return
    directory = Path(chosen)
    with _waiting():
        warnings = export_textures(directory, texture_sets())
        try:
            converted = textures.convert(directory, renderman=textures.renderman_install())
        except PiperError as refusal:
            # The PNGs are written by now; alone, the refusal would read as if nothing was.
            said = f"The textures were exported into {directory} but not converted: {refusal}"
            raise PiperError("\n\n".join([said, *warnings])) from refusal
    said = f"Exported and converted {len(converted)} textures into {directory}."
    _show("\n\n".join([said, *warnings]))


@contextlib.contextmanager
def _waiting() -> Iterator[None]:
    """Painter's wait cursor, for work that holds Painter for minutes."""
    QtWidgets.QApplication.setOverrideCursor(QtCore.Qt.CursorShape.WaitCursor)
    try:
        yield
    finally:
        QtWidgets.QApplication.restoreOverrideCursor()


def _leaving_open_project() -> bool:
    """Whether the open project, if any, may be left: saved, or let go, at the artist's word."""
    if not project.is_open() or not project.needs_saving():
        return True
    buttons = QtWidgets.QMessageBox.StandardButton
    answer = QtWidgets.QMessageBox.question(
        painter_ui.get_main_window(),
        "Piper",
        "The open project has unsaved changes. Save it before opening other work?",
        buttons.Save | buttons.Discard | buttons.Cancel,
    )
    if answer == buttons.Save:
        try:
            project.save()
        except ProjectError as exc:
            raise PiperError(f"Painter could not save the project ({exc})") from exc
    return answer != buttons.Cancel


def _show(message: str) -> None:
    QtWidgets.QMessageBox.information(painter_ui.get_main_window(), "Piper", message)


def _production_and_tracker() -> tuple[Production, Tracker]:
    production = profile.active().production
    if production is None:
        known = ", ".join(sorted(profile.PRODUCTIONS))
        raise PiperError(
            "this Painter is not working in a production; "
            f"run `piper configure` with one of: {known}, then `piper launch painter` again"
        )
    return production, tracker_for(production)
