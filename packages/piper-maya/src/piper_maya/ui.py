"""What Piper adds to Maya's interface: its menu, and what opens, previews, and publishes work."""

import functools
import os
import shutil
import subprocess
import tempfile
import threading
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import ParamSpec

from maya import cmds, utils

from piper.errors import PiperError
from piper.tracker import Tracker
from piper_maya.work import open_work
from piper_studio import launch, profile
from piper_studio.context import CONTEXTS, context_named
from piper_studio.layout import version_name
from piper_studio.production import Production
from piper_studio.profile import Profile
from piper_studio.registry import registry_for
from piper_studio.tracker import tracker_for

_P = ParamSpec("_P")

_MENU = "piper_menu"
_OPEN_WORK_WINDOW = "piper_open_work"
_NAMES_LISTED = 8


def refusals_shown(action: Callable[_P, None]) -> Callable[_P, None]:
    """Show an artist what Piper refused, where a script error would go unseen."""

    @functools.wraps(action)
    def shown(*args: _P.args, **kwargs: _P.kwargs) -> None:
        try:
            action(*args, **kwargs)
        except PiperError as refusal:
            show_refusal(str(refusal))

    return shown


def show_refusal(message: str) -> None:
    cmds.confirmDialog(title="Piper", message=message, button=["OK"], icon="warning")


def start(asset_id: str | None = None, context_name: str | None = None) -> None:
    """Add Piper's menu, then open the work the launch named, if it named any.

    Maya's startup command calls this once the main window exists.
    """
    cmds.menu(_MENU, label="Piper", parent="MayaWindow")
    cmds.menuItem(label="Open Work…", command=lambda *_: show_open_work())
    cmds.menuItem(label="Publish…", command=lambda *_: show_publish())
    cmds.menuItem(label="Preview…", command=lambda *_: show_preview())
    if asset_id is not None and context_name is not None:
        _open_launched_work(asset_id, context_name)


@refusals_shown
def _open_launched_work(asset_id: str, context_name: str) -> None:
    production, tracker = _production_and_tracker(profile.active())
    asset = tracker.asset(asset_id)
    if asset is None:
        raise PiperError(f"{production.name} has no asset with the id {asset_id}")
    open_work(tracker, production, asset, context_named(context_name, subject="asset"))


@refusals_shown
def show_open_work() -> None:
    """Show the dialog an artist picks an asset and a context in."""
    production, tracker = _production_and_tracker(profile.active())
    found = list(tracker.find_assets(""))
    contexts = [c.name for c in CONTEXTS if (c.subject, c.host) == ("asset", "maya")]

    if cmds.window(_OPEN_WORK_WINDOW, exists=True):
        cmds.deleteUI(_OPEN_WORK_WINDOW)
    cmds.window(_OPEN_WORK_WINDOW, title=f"Open Work — {production.name}", widthHeight=(380, 440))
    cmds.columnLayout(adjustableColumn=True, rowSpacing=6, columnAttach=("both", 8))
    search = cmds.textField(placeholderText="Part of an asset's name, then Enter")
    listing = cmds.textScrollList(allowMultiSelection=False, height=320)
    context = cmds.optionMenu(label="Context")
    for name in contexts:
        cmds.menuItem(label=name)

    def list_found() -> None:
        rows = [f"{asset.name}    ({asset.folder or 'no folder'})" for asset in found]
        cmds.textScrollList(listing, edit=True, removeAll=True)
        cmds.textScrollList(listing, edit=True, append=rows)

    # One tracker request per Enter, never per keystroke.
    @refusals_shown
    def search_tracker(text: str) -> None:
        found[:] = tracker.find_assets(text.strip())
        list_found()

    @refusals_shown
    def open_selected(*_: object) -> None:
        selected = cmds.textScrollList(listing, query=True, selectIndexedItem=True)
        if not selected:
            raise PiperError("choose an asset to open")
        chosen = context_named(cmds.optionMenu(context, query=True, value=True), subject="asset")
        if open_work(tracker, production, found[selected[0] - 1], chosen) is not None:
            cmds.deleteUI(_OPEN_WORK_WINDOW)

    cmds.textField(search, edit=True, enterCommand=search_tracker)
    cmds.textScrollList(listing, edit=True, doubleClickCommand=open_selected)
    cmds.button(label="Open", command=open_selected)
    list_found()
    cmds.showWindow(_OPEN_WORK_WINDOW)


@refusals_shown
def show_publish() -> None:
    """Show what the open scene would publish, and publish it when the artist agrees."""
    # Imported here: loading USD takes most of a second, and Maya's startup does not wait for it.
    from piper_maya.publish import PRODUCT, materials, publish_work, scene_asset, selection
    from piper_studio import compose
    from piper_studio.current import current
    from piper_studio.publish import composition, current_line, other_versions

    production, tracker = _production_and_tracker(profile.active())
    root = production.root
    asset = scene_asset(tracker, production)
    now = current(root, asset)
    pins = compose.pins(root, asset, now) if now is not None else {}
    lines = [
        f"Publish to {asset.name}, {PRODUCT}?",
        "",
        f"Selected: {_listed(selection())}",
        f"Materials: {_listed(materials())}",
        "",
        current_line(now, pins),
    ]
    unsaved = cmds.file(query=True, modified=True)
    buttons = ["Save and Publish", "Publish Without Saving"] if unsaved else ["Publish"]
    answer = publish_window(lines, other_versions(root, asset, pins, PRODUCT), buttons)
    if answer is None:
        return
    chosen, with_versions = answer
    if chosen == "Save and Publish":
        try:
            cmds.file(save=True)
        except RuntimeError as exc:
            raise PiperError(
                f"Maya could not save the scene ({str(exc).strip()}), so nothing was published; "
                "Publish Without Saving publishes it as it is"
            ) from exc

    cmds.waitCursor(state=True)
    try:
        result = publish_work(registry_for(production), production, asset, with_versions)
    finally:
        cmds.waitCursor(state=False)
    cmds.confirmDialog(
        title="Piper",
        message=f"Published {result.component.product} {version_name(result.component.version)} "
        f"of {asset.name}\n\n{result.component.path}\n\n{composition(result)}",
        button=["OK"],
    )


@refusals_shown
def show_preview() -> None:
    """Open the selection in usdview, composed as a publish would compose it, installing nothing."""
    from piper_maya.publish import preview_work, scene_asset

    active = profile.active()
    production, tracker = _production_and_tracker(active)
    asset = scene_asset(tracker, production)
    directory = Path(tempfile.mkdtemp(prefix="piper_preview_"))
    try:
        entry = preview_work(production, asset, directory)
        viewer = subprocess.Popen(
            launch.usdview_command(active, entry),
            env=launch.compose(os.environ, launch.usdview_variables(active)),
            cwd=launch.working_directory(active),
            stderr=subprocess.PIPE,
        )
    except BaseException:
        shutil.rmtree(directory, ignore_errors=True)
        raise
    threading.Thread(target=_remove_after_viewing, args=(viewer, directory), daemon=True).start()


def _remove_after_viewing(viewer: subprocess.Popen[bytes], directory: Path) -> None:
    _, said = viewer.communicate()
    shutil.rmtree(directory, ignore_errors=True)
    if viewer.returncode != 0:
        # Maya's interface is the main thread's; this runs there when Maya is next idle.
        utils.executeDeferred(
            show_refusal,
            f"usdview closed with status {viewer.returncode}\n\n"
            f"{said.decode(errors='replace').strip()[-800:]}",
        )


def publish_window(
    lines: list[str], offered: Mapping[str, tuple[list[int], int]], buttons: list[str]
) -> tuple[str, dict[str, int]] | None:
    """Show ``lines``, a version menu per product ``offered``, and ``buttons``."""
    menus: dict[str, str] = {}
    chosen: dict[str, int] = {}

    def press(label: str, *_: object) -> None:
        # Read before the dialog is dismissed: its controls are gone once it returns.
        for product, (versions, _start) in offered.items():
            picked = cmds.optionMenu(menus[product], query=True, select=True)
            chosen[product] = versions[picked - 1]
        cmds.layoutDialog(dismiss=label)

    def build() -> None:
        # layoutDialog makes its form layout the current parent while this runs.
        form = cmds.setParent(query=True)
        column = cmds.columnLayout(adjustableColumn=True, rowSpacing=6, columnAttach=("both", 8))
        cmds.text(label="\n".join(lines), align="left")
        for product, (versions, start) in offered.items():
            menus[product] = cmds.optionMenu(label=product)
            for number in versions:
                cmds.menuItem(label=version_name(number))
            cmds.optionMenu(menus[product], edit=True, value=version_name(start))
        cmds.rowLayout(numberOfColumns=len(buttons) + 1)
        for label in buttons:
            cmds.button(label=label, command=functools.partial(press, label))
        cmds.button(label="Cancel", command=lambda *_: cmds.layoutDialog(dismiss="Cancel"))
        cmds.formLayout(
            form, edit=True, attachForm=[(column, side, 0) for side in ("top", "left", "right")]
        )

    answer = cmds.layoutDialog(ui=build, title="Piper")
    if answer not in buttons:
        return None
    return answer, chosen


def _listed(names: list[str]) -> str:
    listed = ", ".join(names[:_NAMES_LISTED]) or "none"
    more = len(names) - _NAMES_LISTED
    return f"{listed}, and {more} more" if more > 0 else listed


def _production_and_tracker(active: Profile) -> tuple[Production, Tracker]:
    production = active.production
    if production is None:
        raise PiperError(
            "this Maya is not working in a production; "
            "run `piper configure`, then start Maya with `piper launch maya`"
        )
    return production, tracker_for(production)
