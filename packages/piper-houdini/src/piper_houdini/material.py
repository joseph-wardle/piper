"""The Piper Material node: a material per texture set, reading the textures in one directory."""

from dataclasses import dataclass
from pathlib import Path, PurePosixPath

import hou
from pxr import Ar

from piper.errors import PiperError
from piper.tracker import Asset
from piper_studio import layout, textures
from piper_studio.storage import asset_directory

TYPE = "piper::material"
NAME = "piper_material"
_SURFACE = "pxrsurface::3.0"

# Where each RenderMan texture goes: the node, its input, and the texture's output.
# Normal goes through a PxrNormalMap instead, and Displacement has no place yet.
_RENDERMAN = {
    "BaseColor": ("workflow", "baseColor", "resultRGB"),
    "Metallic": ("workflow", "metallic", "resultR"),
    "SpecularRoughness": ("surface", "specularRoughness", "resultR"),
    "Emissive": ("surface", "glowColor", "resultRGB"),
    "Presence": ("surface", "presence", "resultR"),
}
# Where each preview goes: the UsdPreviewSurface input, the texture's output, and its colour space.
_PREVIEW = {
    "BaseColor": ("diffuseColor", "rgb", "sRGB"),
    "Emissive": ("emissiveColor", "rgb", "sRGB"),
    "Metallic": ("metallic", "r", "raw"),
    "SpecularRoughness": ("roughness", "r", "raw"),
    "Normal": ("normal", "rgb", "raw"),
    "Presence": ("opacity", "r", "raw"),
}
_WIRED = (*_RENDERMAN, "Normal")


@dataclass(frozen=True, slots=True)
class AddMaterialsResult:
    """The materials added, the maps no material reads, and what is read but not there.

    ``missing`` pairs a material's texture node with the file it reads and the
    directory lacks: what pointing the node at another version left behind.
    """

    added: tuple[str, ...]
    unwired: tuple[str, ...]
    missing: tuple[tuple[str, str], ...]


def add_materials(node: hou.Node) -> AddMaterialsResult:
    """Add a material for each texture set in ``node``'s directory that has none.

    A material that exists, generated or not, is never touched: deleting one is
    the artist's act. Every texture path is an expression off the node's
    ``textures`` parm, so pointing the scene at another version is one parm.
    """
    directory = _directory(node)
    sets = _texture_sets(directory)
    if not sets:
        raise PiperError(
            f"{directory} holds no texture named <slot>_<map>.<udim>{textures.TEXTURE} or "
            f"{textures.PREVIEW}, as a tex version and `piper convert` name them"
        )
    if hou.nodeType(hou.vopNodeTypeCategory(), _SURFACE) is None:
        raise PiperError("RenderMan for Houdini is not loaded, so no PxrSurface can be made")
    library = node.parent()
    added = []
    for slot, maps in sets.items():
        if library.node(slot) is None:
            _build(library, node, slot, maps)
            added.append(slot)
    if added:
        library.layoutChildren(items=[library.node(slot) for slot in added])
    unwired = {name for maps in sets.values() for name in maps if name not in _WIRED}
    return AddMaterialsResult(tuple(added), tuple(sorted(unwired)), missing_textures(node))


def added_line(result: AddMaterialsResult) -> str:
    """What Add Materials added, what it could not wire, and what is read but not there."""
    lines = [
        f"Added {', '.join(result.added)}." if result.added else "Every texture set has a material."
    ]
    if result.unwired:
        lines.append(f"No material reads {', '.join(result.unwired)}: wire it by hand.")
    return " ".join(lines + missing_lines(result.missing))


def missing_textures(node: hou.Node) -> tuple[tuple[str, str], ...]:
    """Each texture read from ``node``'s directory with no file there: the reader, and the file."""
    spelled = PurePosixPath(node.parm("textures").eval())
    directory = _directory(node)
    library = node.parent()
    missing = []
    for parm, _ in hou.fileReferences():
        if parm is None or not parm.node().path().startswith(f"{library.path()}/"):
            continue
        read = PurePosixPath(parm.eval())
        if read.parent == spelled and not textures.tiles(directory, read.name):
            missing.append((library.relativePathTo(parm.node()), read.name))
    return tuple(sorted(missing))


def missing_lines(missing: tuple[tuple[str, str], ...]) -> list[str]:
    """Each missing texture as a sentence naming its reader."""
    return [
        f"{reader} reads {name}, which the directory does not hold." for reader, name in missing
    ]


def instances() -> tuple[hou.Node, ...]:
    """Every Piper Material node in the open scene."""
    node_type = hou.nodeType(hou.vopNodeTypeCategory(), TYPE)
    return node_type.instances() if node_type is not None else ()


def textures_path(root: PurePosixPath, asset: Asset, version: int) -> PurePosixPath:
    """An installed tex version's directory, spelled from the root."""
    product = layout.product_root(PurePosixPath(asset_directory(root, asset)), textures.PRODUCT)
    return (product / layout.version_name(version)).relative_to(root)


def scene_textures(root: PurePosixPath, asset: Asset) -> list[tuple[str, int]]:
    """Each tex version of ``asset`` a Piper Material node reads, and the parm reading it."""
    directory = layout.product_root(PurePosixPath(asset_directory(root, asset)), textures.PRODUCT)
    read = []
    for node in instances():
        parm = node.parm("textures")
        version = root / parm.eval()
        number = layout.version_number(version.name)
        if version.parent == directory and number:
            read.append((parm.path(), number))
    return sorted(read)


def _directory(node: hou.Node) -> Path:
    """The directory ``node``'s parm names, resolved as the scene's paths are."""
    spelled = node.parm("textures").eval()
    if not spelled:
        raise PiperError(f"{node.path()} names no textures directory")
    resolved = str(Ar.GetResolver().Resolve(spelled))
    if not resolved or not Path(resolved).is_dir():
        raise PiperError(
            f"{node.path()} reads {spelled}, which is not a directory Houdini can find; a tex "
            "version is spelled from the production root, as asset/kitchen/pan/publish/tex/v003"
        )
    return Path(resolved)


def _texture_sets(directory: Path) -> dict[str, dict[str, set[str]]]:
    """Each texture set in ``directory``, its maps, and the suffixes each map has: .tex, .jpg."""
    sets: dict[str, dict[str, set[str]]] = {}
    for path in sorted(directory.iterdir()):
        named = textures.NAMED.fullmatch(path.name)
        if named is None or path.suffix not in (textures.TEXTURE, textures.PREVIEW):
            continue
        if not named["slot"].isidentifier():
            raise PiperError(
                f"{path.name} cannot name a material: name the texture set in Painter with "
                "letters, digits, and underscores"
            )
        sets.setdefault(named["slot"], {}).setdefault(named["map"], set()).add(path.suffix)
    return sets


def _build(library: hou.Node, node: hou.Node, slot: str, maps: dict[str, set[str]]) -> None:
    """One material named for the slot: a PxrSurface and, with previews, a UsdPreviewSurface."""
    material = library.createNode("subnet", slot)
    for child in material.children():
        child.destroy()
    material.setMaterialFlag(True)
    folder = f'chs("../{material.relativePathTo(node)}/textures")'

    def reader(kind: str, name: str, parm: str, file: str) -> hou.Node:
        """A texture node named ``name``, whose ``parm`` reads ``file`` from the folder."""
        made = material.createNode(kind, name)
        made.parm(parm).setExpression(f'{folder} + "/{file}"', hou.exprLanguage.Hscript)
        return made

    rendered = [n for n, suffixes in maps.items() if textures.TEXTURE in suffixes and n in _WIRED]
    surface = material.createNode(_SURFACE, "rman")
    targets = {"surface": surface}
    if "BaseColor" in rendered or "Metallic" in rendered:
        workflow = material.createNode("pxrmetallicworkflow::3.0", "workflow")
        # Its own default is RenderMan's placeholder blue.
        workflow.parmTuple("baseColor").set(surface.parmTuple("diffuseColor").eval())
        surface.setNamedInput("diffuseColor", workflow, "resultDiffuseRGB")
        surface.setNamedInput("specularFaceColor", workflow, "resultSpecularFaceRGB")
        surface.setNamedInput("specularEdgeColor", workflow, "resultSpecularEdgeRGB")
        targets["workflow"] = workflow
    for name in rendered:
        file = f"{slot}_{name}.{textures.UDIM}{textures.TEXTURE}"
        colorspace = "rendering" if name in textures.COLOUR_MAPS else "data"
        if name == "Normal":
            normal = reader("pxrnormalmap::3.0", name, "filename", file)
            normal.parm("filename_colorspace").set(colorspace)
            # Painter exports OpenGL normals; the node's own default is Custom.
            normal.parm("orientation").set(0)
            surface.setNamedInput("bumpNormal", normal, "resultN")
            continue
        target, input_name, output = _RENDERMAN[name]
        made = reader("pxrtexture::3.0", name, "filename", file)
        made.parm("filename_colorspace").set(colorspace)
        targets[target].setNamedInput(input_name, made, output)
        if name == "Emissive":
            surface.parm("glowGain").set(1)

    collect = material.createNode("collect", "OUT")
    collect.setInput(0, surface, 0)
    previews = [n for n, suffixes in maps.items() if textures.PREVIEW in suffixes and n in _PREVIEW]
    if previews:
        preview = material.createNode("usdpreviewsurface", "preview")
        st = material.createNode("usdprimvarreader", "st")
        st.parm("signature").set("float2")
        st.parm("varname").set("st")
        for name in previews:
            input_name, output, colorspace = _PREVIEW[name]
            file = f"{slot}_{name}.{textures.UDIM}{textures.PREVIEW}"
            made = reader("usduvtexture::2.0", f"preview_{name}", "file", file)
            made.parm("sourceColorSpace").set(colorspace)
            made.setNamedInput("st", st, "result")
            if name == "Normal":
                # Stored 0 to 1; the surface wants tangent space, -1 to 1.
                made.parmTuple("scale").set((2, 2, 2, 1))
                made.parmTuple("bias").set((-1, -1, -1, 0))
            preview.setNamedInput(input_name, made, output)
        collect.setInput(1, preview, 0)
    material.layoutChildren()
