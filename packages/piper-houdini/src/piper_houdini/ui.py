"""What Piper adds to Houdini's interface: the dialogs behind its menu."""

import functools
import tempfile
from collections.abc import Callable, Mapping
from pathlib import Path, PurePosixPath
from typing import ParamSpec

import hou

from piper.errors import PiperError
from piper.tracker import Asset, Tracker
from piper_houdini import material
from piper_houdini.publish import publish_work, save_layer, scene_asset, scene_pin, surfaces
from piper_houdini.work import PRODUCT, open_work
from piper_studio import compose, profile, textures
from piper_studio.context import CONTEXTS, context_named
from piper_studio.layout import version_name
from piper_studio.production import Production
from piper_studio.publish import composition_line, other_versions, published_line
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
    context = context_named(context_name, subject="asset")
    if open_work(tracker, production, asset, context) is not None:
        _warn_stale(production.root, asset)


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
    if chosen and open_work(tracker, production, found[chosen[0]], context) is not None:
        _warn_stale(production.root, found[chosen[0]])


@refusals_shown
def show_use_textures() -> None:
    """Point the scene's Piper Material at a tex version the artist picks."""
    production, tracker = _production_and_tracker()
    root = production.root
    asset = scene_asset(tracker, production)
    generator = _generator()
    versions = compose.versions(root, asset, textures.PRODUCT)
    if not versions:
        raise PiperError(
            f"no {textures.PRODUCT} is published for {asset.name}; "
            f"`piper publish {asset.name!r} {textures.PRODUCT} <export>` installs one"
        )
    rows = [f"{version_name(n)}{'    (newest)' if n == versions[-1] else ''}" for n in versions]
    chosen = hou.ui.selectFromList(
        rows,
        default_choices=[len(versions) - 1],
        exclusive=True,
        title=f"Use Textures — {asset.name}",
        message=f"Point {generator.path()} at",
        column_header=textures.PRODUCT,
        width=380,
        height=440,
    )
    if not chosen:
        return
    version = versions[chosen[0]]
    generator.parm("textures").set(str(material.textures_path(root, asset, version)))
    lines = [
        f"{generator.path()} reads {textures.PRODUCT} {version_name(version)}.",
        *material.missing_lines(material.missing_textures(generator)),
        "Add Materials adds a material for a texture set that has none.",
    ]
    hou.ui.displayMessage("\n".join(lines), title="Piper")


@refusals_shown
def add_materials(generator: hou.Node) -> None:
    """Add Materials from the node's button, saying what it added and what is not there."""
    hou.ui.displayMessage(material.added_line(material.add_materials(generator)), title="Piper")


@refusals_shown
def show_publish() -> None:
    """Show what the scene would publish, and publish it when the artist agrees."""
    production, tracker = _production_and_tracker()
    root = production.root
    asset = scene_asset(tracker, production)
    loaded, parm = scene_pin(root, asset)
    now, pins = compose.current_pins(root, asset)
    named = compose.slots(root, asset, pins)
    # Read before the temporary ROP, in case Houdini counts it as a change.
    unsaved = hou.hipFile.hasUnsavedChanges()
    buttons = ["Save and Publish", "Publish Without Saving"] if unsaved else ["Publish"]
    with tempfile.TemporaryDirectory(
        prefix="piper_publish_", ignore_cleanup_errors=True
    ) as directory:
        layer = save_layer(Path(directory), asset, named)
        lines = [
            f"Publish to {asset.name}, {PRODUCT}?",
            "",
            _loads_line(loaded, compose.pins(root, asset, loaded), parm, now),
            compose.current_line(now, pins),
            "",
            *(
                f"{slot}: {', '.join(given) or 'nothing'}"
                for slot, given in surfaces(layer, asset, named).items()
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
        result = publish_work(
            registry_for(production),
            production,
            asset,
            layer,
            saved=chosen != "Publish Without Saving",
            with_versions=with_versions,
        )
    hou.ui.displayMessage(
        f"{published_line(result.component)}\n\n{result.component.path}\n\n{composition_line(result)}",
        title="Piper",
    )


def publish_window(
    lines: list[str], offered: Mapping[str, tuple[list[int], int]], buttons: list[str]
) -> tuple[str, dict[str, int]] | None:
    """Show ``lines``, a version menu per product ``offered``, and ``buttons``."""
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


def _warn_stale(root: PurePosixPath, asset: Asset) -> None:
    """Say what the open scene loads and reads when newer versions exist."""
    try:
        loaded, parm = scene_pin(root, asset)
        pinned = compose.pins(root, asset, loaded)
    except PiperError:
        return
    now, pins = compose.current_pins(root, asset)
    lines = []
    if now != loaded:
        lines += [_loads_line(loaded, pinned, parm, now), compose.current_line(now, pins)]
    installed = compose.versions(root, asset, textures.PRODUCT)
    for reading, version in material.scene_textures(root, asset):
        if installed and version < installed[-1]:
            lines.append(
                f"This scene reads {textures.PRODUCT} {version_name(version)} ({reading}); "
                f"the newest is {textures.PRODUCT} {version_name(installed[-1])}."
            )
    if lines:
        hou.ui.displayMessage("\n".join(lines), title="Piper")


def _generator() -> hou.Node:
    """The one Piper Material node of the open scene."""
    found = material.instances()
    if len(found) == 1:
        return found[0]
    if found:
        listed = ", ".join(node.path() for node in found)
        raise PiperError(f"this scene has {len(found)} Piper Material nodes ({listed}); keep one")
    raise PiperError(
        "this scene has no Piper Material node; Open Work… starts a network with one, "
        "or add one inside the Material Library"
    )


def _loads_line(loaded: int, pinned: Mapping[str, int], parm: str, now: int | None) -> str:
    listed = ", ".join(f"{p} {version_name(n)}" for p, n in sorted(pinned.items()))
    loads = f"This scene loads {compose.ASSET} {version_name(loaded)} ({parm}), pinning {listed}"
    return f"{loads}, which is current." if now == loaded else f"{loads}."


def _production_and_tracker() -> tuple[Production, Tracker]:
    production = profile.active().production
    if production is None:
        known = ", ".join(sorted(profile.PRODUCTIONS))
        raise PiperError(
            "this Houdini is not working in a production; "
            f"run `piper configure` with one of: {known}, then `piper launch houdini` again"
        )
    return production, tracker_for(production)
