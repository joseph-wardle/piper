"""Publishing modeling work from Maya, and looking at it composed before publishing."""

import tempfile
from collections.abc import Mapping
from pathlib import Path, PurePosixPath
from types import MappingProxyType

from maya import cmds
from pxr import Sdf, Usd, UsdShade

from piper.errors import PiperError
from piper.registry import Registry
from piper.tracker import Asset, Tracker
from piper_maya.work import scene_stamp
from piper_studio import compose
from piper_studio.production import Production
from piper_studio.publish import PublishResult, publish
from piper_studio.storage import asset_directory
from piper_studio.work import stamped_asset

PRODUCT = "geo"
_GEOMETRY = "geo"
_RENDER = "render"
_MATERIALS = "mtl"
_REMEDY = "open the asset's work with Piper > Open Work…, then import this scene into it"


def scene_asset(tracker: Tracker, production: Production) -> Asset:
    """The asset the open scene is work on, refusing a scene that is not that work's own file."""
    scene = Path(cmds.file(query=True, sceneName=True))
    return stamped_asset(tracker, production, scene_stamp(), scene, _REMEDY)


def selection() -> list[str]:
    """What is selected, refusing a selection with no geometry in it."""
    # With a root prim, exporting nothing still writes a layer a publish would accept.
    if not cmds.ls(selection=True, dagObjects=True, type="mesh", noIntermediate=True):
        raise PiperError("select the geometry to publish; nothing selected holds a mesh")
    return cmds.ls(selection=True)


def materials() -> list[str]:
    """The shading groups the selected geometry is in. Each publishes as a slot."""
    shapes = cmds.ls(selection=True, dagObjects=True, shapes=True, noIntermediate=True)

    return sorted(
        {group for shape in shapes for group in cmds.listSets(object=shape, type=1) or []}
    )


def publish_work(
    registry: Registry,
    production: Production,
    asset: Asset,
    with_versions: Mapping[str, int] = MappingProxyType({}),
) -> PublishResult:
    """Publish the selection as ``asset``'s geometry, with the scene as its source."""
    scene = Path(cmds.file(query=True, sceneName=True))
    # A cleanup that fails must not replace the result of a publish that happened.
    with tempfile.TemporaryDirectory(
        prefix="piper_publish_", ignore_cleanup_errors=True
    ) as directory:
        layer = export_selection(Path(directory), asset_directory(production.root, asset).name)
        source = scene
        if cmds.file(query=True, modified=True):
            source = Path(directory) / scene.name
            try:
                cmds.file(
                    str(source),
                    exportAll=True,
                    preserveReferences=True,
                    type=cmds.file(query=True, type=True)[0],
                )
            except RuntimeError as exc:
                raise PiperError(
                    f"Maya could not write a copy of the scene ({str(exc).strip()})"
                ) from exc
        return publish(
            registry,
            root=production.root,
            asset=asset,
            product=PRODUCT,
            layer=PurePosixPath(layer),
            source=PurePosixPath(source),
            with_versions=with_versions,
        )


def preview_work(production: Production, asset: Asset, directory: Path) -> Path:
    """Write into ``directory`` what publishing the selection would compose."""
    root = production.root
    layer = export_selection(directory, asset_directory(root, asset).name)
    _, pins = compose.current_pins(root, asset)
    return compose.write_preview(directory, root=root, asset=asset, pins=pins, layer=layer)


def export_selection(directory: Path, pipe_name: str) -> Path:
    """Export the selection into ``directory`` as the geometry of the asset named ``pipe_name``."""
    selection()
    layer = directory / f"{PRODUCT}.usd"
    try:
        cmds.loadPlugin("mayaUsdPlugin", quiet=True)
        cmds.mayaUSDExport(
            file=str(layer),
            selection=True,
            rootPrim=pipe_name,
            rootPrimType="xform",
            exportComponentTags=False,
            metersPerUnit=1.0,
            exportDistanceUnit=True,
        )
    except RuntimeError as exc:
        raise PiperError(f"Maya could not export the selection ({str(exc).strip()})") from exc
    _empty_materials(layer)
    _move_under_render(layer, pipe_name)
    return layer


def _empty_materials(path: Path) -> None:
    """Keep each material as a named slot geometry is bound to, and drop its shading.

    MayaUSD has no option that writes the bindings without the shaders.
    """
    layer = Sdf.Layer.FindOrOpen(str(path))
    stage = Usd.Stage.Open(layer)
    material_paths = [prim.GetPath() for prim in stage.Traverse() if prim.IsA(UsdShade.Material)]
    for material in material_paths:
        spec = layer.GetPrimAtPath(material)
        for child in list(spec.nameChildren):
            del spec.nameChildren[child.name]
        for prop in list(spec.properties):
            spec.RemoveProperty(prop)
    layer.Save()


def _move_under_render(path: Path, pipe_name: str) -> None:
    """Move the exported model under ``/geo/render`` with purpose ``render``; ``/mtl`` stays."""
    layer = Sdf.Layer.FindOrOpen(str(path))
    root = layer.GetPrimAtPath(f"/{pipe_name}")
    exported = [child.path for child in root.nameChildren if child.name != _MATERIALS]
    # Made under a name the export cannot hold, then renamed once the model is inside.
    geo = Sdf.PrimSpec(root, f"piper_{_GEOMETRY}", Sdf.SpecifierDef, "Scope")
    render = Sdf.PrimSpec(geo, _RENDER, Sdf.SpecifierDef, "Xform")
    purpose = Sdf.AttributeSpec(render, "purpose", Sdf.ValueTypeNames.Token, Sdf.VariabilityUniform)
    purpose.default = _RENDER
    edit = Sdf.BatchNamespaceEdit()
    for child in exported:
        edit.Add(Sdf.NamespaceEdit.Reparent(child, render.path, -1))
    edit.Add(Sdf.NamespaceEdit.Rename(geo.path, _GEOMETRY))
    if not layer.Apply(edit):
        raise PiperError(
            f"could not move the exported model under /{pipe_name}/{_GEOMETRY}/{_RENDER}"
        )
    layer.Save()
