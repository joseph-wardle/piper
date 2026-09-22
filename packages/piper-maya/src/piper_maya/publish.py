"""Publishing modeling work from Maya."""

import tempfile
from pathlib import Path, PurePosixPath

from maya import cmds
from pxr import Sdf, Usd, UsdShade

from piper.errors import PiperError
from piper.registry import Registry
from piper.tracker import Asset, Tracker
from piper_maya.work import ASSET_ID_KEY, CONTEXT_KEY, PRODUCTION_KEY, scene_stamp
from piper_studio.context import context_named
from piper_studio.layout import work_file
from piper_studio.production import Production
from piper_studio.publish import PublishResult, publish
from piper_studio.storage import asset_directory

PRODUCT = "geo"
_GEOMETRY = "geo"
_RENDER = "render"
_MATERIALS = "mtl"
_REMEDY = "open the asset's work with Piper > Open Work…, then import this scene into it"


def scene_asset(tracker: Tracker, production: Production) -> Asset:
    """The asset the open scene is work on, refusing a scene that is not that work's own file."""
    stamp = scene_stamp()
    asset_id = stamp[ASSET_ID_KEY]
    # A scene Maya has never saved carries no stamp, so this refuses it too.
    if stamp[PRODUCTION_KEY] != production.name or not asset_id:
        raise PiperError(f"this scene is not {production.name} work; {_REMEDY}")
    asset = tracker.asset(asset_id)
    if asset is None:
        raise PiperError(
            f"this scene is work on an asset {production.name} does not have (id {asset_id})"
        )
    context = context_named(stamp[CONTEXT_KEY] or "", subject="asset")
    expected = Path(work_file(PurePosixPath(asset_directory(production.root, asset)), context))
    scene = Path(cmds.file(query=True, sceneName=True))
    if scene.resolve() != expected.resolve():
        raise PiperError(
            f"this scene is {scene}, not {asset.name}'s {context.name} work at {expected}; "
            f"{_REMEDY}"
        )
    return asset


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


def publish_work(registry: Registry, production: Production, asset: Asset) -> PublishResult:
    """Publish the selection as ``asset``'s geometry, with the scene as its source.

    ``asset`` is what ``scene_asset`` returned. The source is the work file when
    the scene has no unsaved changes, and otherwise the scene as a save would
    have written it; the work file is never saved or changed here.
    """
    selection()
    # Usable as a prim name: `scene_asset` refused the scene otherwise.
    pipe_name = asset.pipe_name or ""
    scene = Path(cmds.file(query=True, sceneName=True))
    # A cleanup that fails must not replace the result of a publish that happened.
    with tempfile.TemporaryDirectory(
        prefix="piper_publish_", ignore_cleanup_errors=True
    ) as directory:
        layer = Path(directory) / f"{PRODUCT}.usd"
        source = scene
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
            if cmds.file(query=True, modified=True):
                source = Path(directory) / scene.name
                cmds.file(
                    str(source),
                    exportAll=True,
                    preserveReferences=True,
                    type=cmds.file(query=True, type=True)[0],
                )
        except RuntimeError as exc:
            raise PiperError(f"Maya could not export the scene ({str(exc).strip()})") from exc
        _empty_materials(layer)
        _under_render_purpose(layer, pipe_name)
        return publish(
            registry,
            root=production.root,
            asset=asset,
            product=PRODUCT,
            layer=PurePosixPath(layer),
            source=PurePosixPath(source),
        )


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


def _under_render_purpose(path: Path, pipe_name: str) -> None:
    """Move the exported model under ``/geo/render`` with purpose ``render``; ``/mtl`` stays."""
    layer = Sdf.Layer.FindOrOpen(str(path))
    root = layer.GetPrimAtPath(f"/{pipe_name}")
    exported = [child for child in root.nameChildren if child.name != _MATERIALS]
    taken = sorted(child.name for child in exported if child.name in (_GEOMETRY, _MATERIALS))
    if taken:
        raise PiperError(
            f"rename the top-level node {', '.join(taken)}: Piper puts the model under "
            f"/{pipe_name}/{_GEOMETRY}/{_RENDER} and materials under /{pipe_name}/{_MATERIALS}"
        )
    geo = Sdf.PrimSpec(root, _GEOMETRY, Sdf.SpecifierDef, "Scope")
    render = Sdf.PrimSpec(geo, _RENDER, Sdf.SpecifierDef, "Xform")
    purpose = Sdf.AttributeSpec(render, "purpose", Sdf.ValueTypeNames.Token, Sdf.VariabilityUniform)
    purpose.default = _RENDER
    edit = Sdf.BatchNamespaceEdit()
    for child in exported:
        edit.Add(Sdf.NamespaceEdit.Reparent(child.path, render.path, -1))
    if not layer.Apply(edit):
        raise PiperError(
            f"could not move the exported model under /{pipe_name}/{_GEOMETRY}/{_RENDER}"
        )
    layer.Save()
