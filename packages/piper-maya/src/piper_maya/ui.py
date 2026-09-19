"""What Piper adds to Maya's interface: its menu, and the dialogs that open and publish work."""

import functools
from collections.abc import Callable
from typing import ParamSpec

from maya import cmds

from piper.errors import PiperError
from piper.tracker import Tracker
from piper_maya.work import open_work
from piper_studio import profile
from piper_studio.context import CONTEXTS, context_named
from piper_studio.layout import version_name
from piper_studio.production import Production
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
            cmds.confirmDialog(title="Piper", message=str(refusal), button=["OK"], icon="warning")

    return shown


def start(asset_id: str | None = None, context_name: str | None = None) -> None:
    """Add Piper's menu, then open the work the launch named, if it named any.

    Maya's startup command calls this once the main window exists.
    """
    cmds.menu(_MENU, label="Piper", parent="MayaWindow")
    cmds.menuItem(label="Open Work…", command=lambda *_: show_open_work())
    cmds.menuItem(label="Publish…", command=lambda *_: show_publish())
    if asset_id is not None and context_name is not None:
        _open_launched_work(asset_id, context_name)


@refusals_shown
def _open_launched_work(asset_id: str, context_name: str) -> None:
    production, tracker = _production_and_tracker()
    asset = tracker.asset(asset_id)
    if asset is None:
        raise PiperError(f"{production.name} has no asset with the id {asset_id}")
    open_work(tracker, production, asset, context_named(context_name, subject="asset"))


@refusals_shown
def show_open_work() -> None:
    """Show the dialog an artist picks an asset and a context in."""
    production, tracker = _production_and_tracker()
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

    production, tracker = _production_and_tracker()
    asset = scene_asset(tracker, production)
    message = (
        f"Publish to {asset.name}, {PRODUCT}?\n\n"
        f"Selected: {_listed(selection())}\n"
        f"Materials: {_listed(materials())}"
    )
    unsaved = cmds.file(query=True, modified=True)
    buttons = ["Save and Publish", "Publish Without Saving"] if unsaved else ["Publish"]
    chosen = cmds.confirmDialog(
        title="Piper",
        message=message,
        button=[*buttons, "Cancel"],
        defaultButton=buttons[0],
        cancelButton="Cancel",
        dismissString="Cancel",
    )
    if chosen not in buttons:
        return
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
        result = publish_work(registry_for(production), production, asset)
    finally:
        cmds.waitCursor(state=False)
    cmds.confirmDialog(
        title="Piper",
        message=f"Published {result.product} {version_name(result.version)} of {asset.name}\n\n"
        f"{result.path}",
        button=["OK"],
    )


def _listed(names: list[str]) -> str:
    listed = ", ".join(names[:_NAMES_LISTED]) or "none"
    more = len(names) - _NAMES_LISTED
    return f"{listed}, and {more} more" if more > 0 else listed


def _production_and_tracker() -> tuple[Production, Tracker]:
    production = profile.active().production
    if production is None:
        raise PiperError(
            "this Maya is not working in a production; "
            "run `piper configure`, then start Maya with `piper launch maya`"
        )
    return production, tracker_for(production)
