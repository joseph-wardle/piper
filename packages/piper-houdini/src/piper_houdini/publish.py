"""Publishing lookdev work from Houdini: the facts a publish needs, and the layer it installs."""

import shutil
from collections.abc import Mapping
from pathlib import Path, PurePosixPath
from types import MappingProxyType

import hou
from pxr import Sdf

from piper.errors import PiperError
from piper.registry import Registry
from piper.tracker import Asset, Tracker
from piper_houdini.work import OUTPUT, PRODUCT, scene_stamp
from piper_studio import compose, layout
from piper_studio.production import Production
from piper_studio.publish import PublishResult, publish
from piper_studio.storage import asset_directory
from piper_studio.work import stamped_asset

_GEOMETRY = "geo"
_REMEDY = "open the asset's work with Piper > Open Work…, then merge this file into it"
_SURFACES = {
    "outputs:ri:surface": "ri",
    "outputs:mtlx:surface": "mtlx",
    "outputs:surface": "preview",
}


def scene_asset(tracker: Tracker, production: Production) -> Asset:
    """The asset the open scene is work on, refusing a scene that is not that work's own file."""
    return stamped_asset(tracker, production, scene_stamp(), Path(hou.hipFile.path()), _REMEDY)


def scene_pin(root: PurePosixPath, asset: Asset) -> tuple[int, str]:
    """The asset version the scene loads, and the parm loading it."""
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


def save_layer(directory: Path, asset: Asset, slot_names: list[str]) -> Path:
    """Save the layer ``OUT_mtl`` holds into ``directory``, refusing one filling other slots."""
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
    _check_layer(layer, asset, slot_names)
    return layer


def surfaces(layer: Path, asset: Asset, slot_names: list[str]) -> dict[str, list[str]]:
    """What ``layer`` gives each slot: ri, mtlx, and preview surfaces."""
    given: dict[str, list[str]] = {slot: [] for slot in slot_names}
    # Held: a spec dies with its layer.
    saved = Sdf.Layer.OpenAsAnonymous(str(layer))
    for spec in _children(saved, f"/{asset.pipe_name}/{PRODUCT}"):
        authored = {prop.name for prop in spec.properties}
        given[spec.name] = [name for attr, name in _SURFACES.items() if attr in authored]
    return given


def publish_work(
    registry: Registry,
    production: Production,
    asset: Asset,
    layer: Path,
    *,
    saved: bool,
    with_versions: Mapping[str, int] = MappingProxyType({}),
) -> PublishResult:
    """Publish ``layer``, which ``save_layer`` wrote, as ``asset``'s materials.

    The source is the work file when ``saved``, and otherwise a copy of the scene as it is.
    """
    source = Path(hou.hipFile.path()) if saved else _copy_scene(layer.parent)
    return publish(
        registry,
        root=production.root,
        asset=asset,
        product=PRODUCT,
        layer=PurePosixPath(layer),
        source=PurePosixPath(source),
        with_versions=with_versions,
    )


def _copy_scene(directory: Path) -> Path:
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


def _check_layer(layer: Path, asset: Asset, slot_names: list[str]) -> None:
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
            f"{OUTPUT} authors {', '.join(outside)}, outside {materials}; {PRODUCT} fills the "
            f"slots there and nothing else: move anything else below {OUTPUT}, or remove it"
        )
    # The library writes over on a slot the asset has and def on any other name.
    defined = [
        prim.name for prim in _children(saved, materials) if prim.specifier == Sdf.SpecifierDef
    ]
    if defined:
        unfilled = ", ".join(slot for slot in slot_names if slot not in defined)
        raise PiperError(
            f"{OUTPUT} defines {', '.join(defined)} under {materials}, which is not a slot of "
            f"the {_GEOMETRY} this publish pins; name each material for the slot it fills "
            f"({unfilled or 'none'})"
        )


def _children(layer: Sdf.Layer, path: str) -> list[Sdf.PrimSpec]:
    spec = layer.GetPrimAtPath(path)
    return list(spec.nameChildren) if spec else []
