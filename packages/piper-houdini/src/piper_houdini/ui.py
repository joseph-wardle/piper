"""What Piper adds to Houdini's interface: the dialogs behind its menu."""

import functools
import tempfile
from collections.abc import Callable, Mapping
from pathlib import Path, PurePosixPath
from typing import ParamSpec

import hou

from piper.errors import PiperError
from piper.tracker import Tracker
from piper_houdini.publish import copy_scene, pin, save_layer, scene_asset, slots
from piper_houdini.work import PRODUCT, open_work
from piper_studio import compose, profile
from piper_studio.context import CONTEXTS, context_named
from piper_studio.current import current
from piper_studio.layout import version_name
from piper_studio.production import Production
from piper_studio.publish import composition, current_line, other_versions, publish
from piper_studio.registry import registry_for
from piper_studio.tracker import tracker_for

_P = ParamSpec("_P")


def refusals_shown(action: Callable[_P, None]) -> Callable[_P, None]:
    """Show an artist what Piper refused, where a script error would go unseen."""

    @functools.wraps(action)
    def shown(*args: _P.args, **kwargs: _P.kwargs) -> None:
        try:
            action(*args, **kwargs)
        except PiperError as refusal:
            hou.ui.displayMessage(str(refusal), title="Piper", severity=hou.severityType.Warning)

    return shown


@refusals_shown
def open_launched_work(asset_id: str, context_name: str) -> None:
    """Open the work ``piper open`` named, once Houdini's interface is up."""
    production, tracker = _production_and_tracker()
    asset = tracker.asset(asset_id)
    if asset is None:
        raise PiperError(f"{production.name} has no asset with the id {asset_id}")
    open_work(tracker, production, asset, context_named(context_name, subject="asset"))


@refusals_shown
def show_open_work() -> None:
    """Show the list an artist picks an asset from."""
    production, tracker = _production_and_tracker()
    [context] = [c for c in CONTEXTS if (c.subject, c.host) == ("asset", "houdini")]
    found = tracker.find_assets("")
    rows = [f"{asset.name}    ({asset.folder or 'no folder'})" for asset in found]
    chosen = hou.ui.selectFromList(
        rows,
        exclusive=True,
        title=f"Open Work — {production.name}",
        message=f"Open {context.name} work on",
        column_header="Asset",
        width=380,
        height=440,
    )
    if chosen:
        open_work(tracker, production, found[chosen[0]], context)


@refusals_shown
def show_publish() -> None:
    """Show what the scene would publish, and publish it when the artist agrees."""
    production, tracker = _production_and_tracker()
    root = production.root
    asset = scene_asset(tracker, production)
    loaded, parm = pin(root, asset)
    now = current(root, asset)
    pins = compose.pins(root, asset, now) if now is not None else {}
    # Read before the layer is saved: the ROP that saves it comes and goes as a change.
    unsaved = hou.hipFile.hasUnsavedChanges()
    buttons = ["Save and Publish", "Publish Without Saving"] if unsaved else ["Publish"]
    with tempfile.TemporaryDirectory(
        prefix="piper_publish_", ignore_cleanup_errors=True
    ) as directory:
        layer = save_layer(Path(directory), asset)
        lines = [
            f"Publish to {asset.name}, {PRODUCT}?",
            "",
            _loads_line(loaded, parm, now),
            current_line(now, pins),
            "",
            *(
                f"{slot}: {', '.join(given) or 'nothing'}"
                for slot, given in slots(layer, asset).items()
            ),
        ]
        answer = publish_window(lines, other_versions(root, asset, pins, PRODUCT), buttons)
        if answer is None:
            return
        chosen, with_versions = answer
        if chosen == "Save and Publish":
            try:
                hou.hipFile.save()
            except hou.OperationFailed as exc:
                raise PiperError(
                    f"Houdini could not save the scene ({exc.instanceMessage().strip()}), so "
                    "nothing was published; Publish Without Saving publishes it as it is"
                ) from exc
        source = copy_scene(Path(directory)) if unsaved else Path(hou.hipFile.path())
        result = publish(
            registry_for(production),
            root=root,
            asset=asset,
            product=PRODUCT,
            layer=PurePosixPath(layer),
            source=PurePosixPath(source),
            with_versions=with_versions,
        )
    hou.ui.displayMessage(
        f"Published {result.component.product} {version_name(result.component.version)} "
        f"of {asset.name}\n\n{result.component.path}\n\n{composition(result)}",
        title="Piper",
    )


def publish_window(
    lines: list[str], offered: Mapping[str, tuple[list[int], int]], buttons: list[str]
) -> tuple[str, dict[str, int]] | None:
    """Show ``lines``, a version menu per product ``offered``, and ``buttons``.

    ``offered`` maps a product to its installed versions and the one to start
    on. Returns the button pressed and the versions chosen, or None for Cancel.
    """
    from PySide6 import QtWidgets

    window = QtWidgets.QDialog(hou.qt.mainWindow())
    window.setWindowTitle("Piper")
    column = QtWidgets.QVBoxLayout(window)
    column.addWidget(QtWidgets.QLabel("\n".join(lines)))
    form = QtWidgets.QFormLayout()
    menus: dict[str, QtWidgets.QComboBox] = {}
    for product, (versions, start) in offered.items():
        menu = QtWidgets.QComboBox()
        menu.addItems([version_name(number) for number in versions])
        menu.setCurrentText(version_name(start))
        form.addRow(f"{product}:", menu)
        menus[product] = menu
    column.addLayout(form)
    row = QtWidgets.QHBoxLayout()
    pressed: list[str] = []

    def press(label: str) -> None:
        pressed.append(label)
        window.accept()

    for label in buttons:
        button = QtWidgets.QPushButton(label)
        button.clicked.connect(functools.partial(press, label))
        row.addWidget(button)
    cancel = QtWidgets.QPushButton("Cancel")
    cancel.clicked.connect(window.reject)
    row.addWidget(cancel)
    column.addLayout(row)
    if window.exec() != QtWidgets.QDialog.DialogCode.Accepted:
        return None
    chosen = {
        product: versions[menus[product].currentIndex()]
        for product, (versions, _) in offered.items()
    }
    return pressed[0], chosen


def _loads_line(loaded: int, parm: str, now: int | None) -> str:
    """Which asset version the scene loads, and how that stands against current."""
    loads = f"This scene loads {compose.ASSET} {version_name(loaded)} ({parm})"
    if now is None:
        return f"{loads}; nothing is current."
    if now == loaded:
        return f"{loads}, which is current."
    return f"{loads}; current is {version_name(now)}."


def _production_and_tracker() -> tuple[Production, Tracker]:
    production = profile.active().production
    if production is None:
        raise PiperError(
            "this Houdini is not working in a production; "
            "run `piper configure`, then start Houdini with `piper launch houdini`"
        )
    return production, tracker_for(production)
