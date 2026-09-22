"""Publishing lookdev work from Houdini: the facts a publish needs, and the layer it installs."""

import shutil
from pathlib import Path, PurePosixPath

import hou
from pxr import Sdf

from piper.errors import PiperError
from piper.tracker import Asset, Tracker
from piper_houdini.work import OUTPUT, PRODUCT, scene_stamp
from piper_studio import compose, layout
from piper_studio.production import Production
from piper_studio.storage import asset_directory
from piper_studio.work import stamped_asset

_REMEDY = "open the asset's work with Piper > Open Work…, then merge this file into it"
_SURFACES = {
    "outputs:ri:surface": "ri",
    "outputs:mtlx:surface": "mtlx",
    "outputs:surface": "preview",
}


def scene_asset(tracker: Tracker, production: Production) -> Asset:
    """The asset the open scene is work on, refusing a scene that is not that work's own file."""
    return stamped_asset(tracker, production, scene_stamp(), Path(hou.hipFile.path()), _REMEDY)


def pin(root: PurePosixPath, asset: Asset) -> tuple[int, str]:
    """The asset version the scene loads, and the parm loading it.

    Refused for a scene that loads none, or several.
    """
    directory = layout.product_root(PurePosixPath(asset_directory(root, asset)), compose.ASSET)
    loads: list[tuple[str, int]] = []
    for parm, path in hou.fileReferences():
        version = layout.version_directory(root, root / hou.text.expandString(path))
        number = layout.version_number(version.name) if version is not None else None
        if parm is not None and version is not None and version.parent == directory and number:
            loads.append((parm.path(), number))
    if len(loads) != 1:
        listed = ", ".join(f"{parm} ({layout.version_name(n)})" for parm, n in sorted(loads))
        raise PiperError(
            f"this scene must load one asset version of {asset.name}, the one its {PRODUCT} is "
            f"made against; it loads {len(loads) or 'none'}{': ' + listed if loads else ''}"
        )
    [(parm, number)] = loads
    return number, parm


def save_layer(directory: Path, asset: Asset) -> Path:
    """Save the layer ``OUT_mtl`` holds into ``directory``.

    Refused when it sublayers anything, which a missing Layer Break does, or
    authors anything but the asset's slots.
    """
    output = hou.node(OUTPUT)
    if output is None:
        raise PiperError(
            f"this scene has no {OUTPUT} to publish from; Open Work… starts a network with one"
        )
    layer = directory / f"{PRODUCT}.usda"
    rop = hou.node("/out").createNode("usd", "piper_publish")
    try:
        rop.parm("loppath").set(OUTPUT)
        rop.parm("lopoutput").set(str(layer))
        rop.render()
    except hou.OperationFailed as exc:
        raise PiperError(
            f"Houdini could not save {OUTPUT} ({exc.instanceMessage().strip()})"
        ) from exc
    finally:
        rop.destroy()
    _check_layer(layer, asset)
    return layer


def slots(layer: Path, asset: Asset) -> dict[str, list[str]]:
    """What ``layer`` gives each of the asset's material slots: ri, mtlx, and preview surfaces."""
    materials = f"/{asset.pipe_name}/{PRODUCT}"
    # The composed asset names the slots; the saved layer alone says what each was given.
    given = {
        prim.GetName(): []
        for prim in hou.node(OUTPUT).stage().GetPrimAtPath(materials).GetChildren()
    }
    # A spec outlives no layer, so the layer is held while its specs are read.
    saved = Sdf.Layer.OpenAsAnonymous(str(layer))
    for spec in _children(saved, materials):
        authored = {prop.name for prop in spec.properties}
        given[spec.name] = [name for attr, name in _SURFACES.items() if attr in authored]
    return given


def copy_scene(directory: Path) -> Path:
    """A copy of the scene as it is, unsaved changes included, in ``directory``.

    Houdini writes it as a backup, in ``backup/`` beside the work file, and
    the copy is moved from there; the work file itself is never changed.
    """
    scene = Path(hou.hipFile.path())
    try:
        backup = Path(hou.hipFile.saveAsBackup())
    except hou.OperationFailed as exc:
        raise PiperError(
            f"Houdini could not save a copy of the scene ({exc.instanceMessage().strip()})"
        ) from exc
    copied = directory / scene.name
    shutil.move(backup, copied)
    return copied


def _check_layer(layer: Path, asset: Asset) -> None:
    saved = Sdf.Layer.OpenAsAnonymous(str(layer))
    if saved.subLayerPaths:
        listed = ", ".join(saved.subLayerPaths)
        raise PiperError(
            f"{OUTPUT} sublayers {listed}, so the asset would be published as its own {PRODUCT}; "
            "keep a Layer Break between the asset and the materials"
        )
    root = f"/{asset.pipe_name}"
    materials = f"{root}/{PRODUCT}"
    outside = [str(prim.path) for prim in saved.rootPrims if prim.path != Sdf.Path(root)]
    outside += [str(prim.path) for prim in _children(saved, root) if prim.name != PRODUCT]
    if outside:
        raise PiperError(
            f"{OUTPUT} authors {', '.join(outside)}, outside {materials}; "
            f"{PRODUCT} fills the slots there and nothing else"
        )
    # A slot is a Material the asset already has, so the library writes an `over` on it;
    # a `def` is a Material named for no slot, which nothing is bound to.
    defined = [
        str(prim.path) for prim in _children(saved, materials) if prim.specifier == Sdf.SpecifierDef
    ]
    if defined:
        slots = [
            prim.GetName()
            for prim in hou.node(OUTPUT).stage().GetPrimAtPath(materials).GetChildren()
            if str(prim.GetPath()) not in defined
        ]
        raise PiperError(
            f"{OUTPUT} defines {', '.join(defined)}, which is not a slot of {asset.name}; "
            f"name each material for the slot it fills ({', '.join(slots) or 'none'})"
        )


def _children(layer: Sdf.Layer, path: str) -> list[Sdf.PrimSpec]:
    spec = layer.GetPrimAtPath(path)
    return list(spec.nameChildren) if spec else []
