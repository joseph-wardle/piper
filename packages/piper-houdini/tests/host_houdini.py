"""Checks Piper's composed environment, and opening and publishing work, inside a real Houdini."""

import os
import subprocess
import sys
import tempfile
import textwrap
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from collections.abc import Callable

    from piper.tracker import Asset, Tracker
    from piper_studio.production import Production

PACKAGE = "piper_check"
"""A Houdini package the production supplies, and Piper never parses."""

_PRODUCTION = """
name = "host_check"
root = "{root}"
types = ["Prop"]

[shotgrid]
site = "https://example.invalid"
script = "piper"
project = 0
"""

# What Maya's publish writes: the model under geo/render, bound to empty slots under mtl.
_GEO = """
    #usda 1.0
    (
        defaultPrim = "{name}"
        metersPerUnit = 1
        upAxis = "Y"
    )

    def Xform "{name}" (
        kind = "component"
    )
    {{
        def Scope "geo"
        {{
            def Xform "render"
            {{
                uniform token purpose = "render"

                def Mesh "body" (
                    prepend apiSchemas = ["MaterialBindingAPI"]
                )
                {{
                    float3[] extent = [(-1, -1, -1), (1, 1, 1)]
                    int[] faceVertexCounts = [4]
                    int[] faceVertexIndices = [0, 1, 2, 3]
                    point3f[] points = [(-1, 0, -1), (1, 0, -1), (1, 0, 1), (-1, 0, 1)]
                    rel material:binding = </{name}/mtl/woodSG>
                }}
            }}
        }}

        def Scope "mtl"
        {{
            def Material "woodSG"
            {{
            }}

            def Material "metalSG"
            {{
            }}
        }}
    }}
"""


def check_host() -> int:
    """Assert, inside Houdini, what the composed environment promised."""
    failures = []

    def check(name: str, ok: bool, detail: str = "") -> None:
        print(f"{'pass' if ok else 'FAIL'}  {name}{('  ' + detail) if detail else ''}")
        if not ok:
            failures.append(name)

    import piper
    import piper_studio.publish  # noqa: F401

    check("piper and piper_studio.publish import", True, piper.__file__)

    from piper_studio.launch import houdini_executable, release_root
    from piper_studio.profile import active

    profile = active()
    production = profile.production
    check(
        "Houdini works in the production that launched it",
        production is not None and production.name == "host_check",
        profile.name,
    )
    check(
        "Houdini starts at the production's root",
        production is not None and Path.cwd() == Path(str(production.root)),
        str(Path.cwd()),
    )
    # Houdini sets HFS from the install it was started as, so this is the install
    # answering, not Piper's own resolution repeated back to itself.
    started_from = os.environ.get("HFS", "")
    resolved = houdini_executable(profile).parent.parent
    check(
        "Houdini runs from the install Piper resolved",
        bool(started_from) and Path(started_from).resolve() == resolved.resolve(),
        started_from or "unset",
    )

    from pxr import Usd

    check(
        "pxr is Houdini's own USD",
        Usd.GetVersion() == (0, 25, 5)
        and Path(Usd.__file__).resolve().is_relative_to(Path(started_from).resolve()),
        f"{Usd.GetVersion()} {Usd.__file__}",
    )

    import hou

    piper_houdini = release_root() / "packages" / "piper-houdini"
    check(
        "Piper's menu and startup hook are on Houdini's path",
        str(piper_houdini) in hou.getenv("HOUDINI_PATH").split(os.pathsep)
        and str(piper_houdini / "python3.11libs") in sys.path
        and (piper_houdini / "MainMenuCommon.xml").is_file(),
        hou.getenv("HOUDINI_PATH"),
    )
    check(f"the production's Houdini package {PACKAGE} loaded", hou.getenv("PIPER_CHECK") == "1")

    for name in ("cyclopts", "rich"):
        try:
            __import__(name)
        except ImportError:
            check(f"{name} is absent", True)
        else:
            check(f"{name} is absent", False, "imported: the command line's environment leaked in")

    import PySide6
    import shotgun_api3

    import piper_houdini as package

    check(
        "shotgun_api3 comes from piper-houdini's own environment",
        str(Path(package.__file__).parents[2] / ".venv") in shotgun_api3.__file__,
        shotgun_api3.__file__,
    )
    check(
        "that environment changed nothing Houdini brings",
        Path(sys.prefix).resolve().is_relative_to(Path(started_from).resolve())
        and PySide6.__file__.startswith(sys.prefix),
        f"{sys.prefix} {PySide6.__file__}",
    )

    if production is not None:
        check_open_work(production, check)
        check_publish_work(production, check)
        check_material(production, check)

    print(f"\n{len(failures)} failed" if failures else "\nall passed")
    return 1 if failures else 0


class Records:
    """A registry that records nothing and refuses nothing."""

    def register(self, asset: "Asset", *, product: str, version: int, path: object) -> str:
        return "1"


class Shown:
    """Stands in for ``hou.ui``, which hython does not have: keeps every message, answers a list."""

    def __init__(self) -> None:
        self.messages: list[str] = []
        self.asked: list[tuple[list[str], dict[str, object]]] = []
        self.choice: list[int] = []

    def displayMessage(self, message: str, **_: object) -> int:  # noqa: N802
        self.messages.append(message)
        return 0

    def selectFromList(self, rows: list[str], **options: object) -> list[int]:  # noqa: N802
        self.asked.append((rows, options))
        return self.choice


def publish_geo(root: Path, asset: "Asset", version: int) -> None:
    """Publish the model as its next geo version, and so an asset version pinning it."""
    from piper.registry import Registry
    from piper_studio.publish import publish

    export = root / "export" / f"{asset.pipe_name}_{version}"
    export.mkdir(parents=True)
    layer = export / "geo.usda"
    layer.write_text(textwrap.dedent(_GEO.format(name=asset.pipe_name)).lstrip(), encoding="utf-8")
    publish(
        cast("Registry", Records()),
        root=PurePosixPath(root),
        asset=asset,
        product="geo",
        layer=PurePosixPath(layer),
    )


def check_open_work(production: "Production", check: "Callable[..., None]") -> None:
    """Open work the way the menu and the startup hook do, on assets of this run's own."""
    import shutil
    from unittest import mock

    import hou

    from piper.errors import PiperError
    from piper.tracker import Asset
    from piper_houdini.publish import scene_pin
    from piper_houdini.work import OUTPUT, open_work, scene_stamp
    from piper_studio.context import context_named

    pan = Asset(id="101", name="Frying Pan", type="Prop", folder="kitchen", pipe_name="frying_pan")
    pot = Asset(id="202", name="Sauce Pot", type="Prop", folder="kitchen", pipe_name="sauce_pot")

    class Assets:
        def asset(self, id: str) -> Asset | None:
            return next((asset for asset in (pan, pot) if asset.id == id), None)

    tracker = cast("Tracker", Assets())
    lookdev = context_named("lookdev", subject="asset")
    root = Path(str(production.root))
    for asset in (pan, pot):
        (root / "asset" / "kitchen" / (asset.pipe_name or "")).mkdir(parents=True)
    shown = Shown()

    def stamp_of(asset: Asset) -> dict[str, str | None]:
        return {
            "piper_production": production.name,
            "piper_asset_id": asset.id,
            "piper_context": "lookdev",
        }

    def refusal(action: "Callable[[], object]") -> str:
        try:
            action()
        except PiperError as refused:
            return str(refused)
        return ""

    expected = root / "asset" / "kitchen" / "frying_pan" / "work" / "lookdev" / "frying_pan.hipnc"
    with mock.patch.object(hou, "ui", shown, create=True):
        check(
            "work on an asset nothing is current for is refused, and no file is made",
            "publish its geo first" in refusal(lambda: open_work(tracker, production, pan, lookdev))
            and not expected.exists(),
        )
        publish_geo(root, pan, 1)

        file = open_work(tracker, production, pan, lookdev)
        check(
            "new work is saved at its path before the artist does anything",
            file == expected and expected.is_file() and hou.hipFile.path() == str(expected),
            hou.hipFile.path(),
        )
        check("and is stamped with what it is", scene_stamp() == stamp_of(pan), str(scene_stamp()))
        nodes = {node.name(): node for node in hou.node("/stage").children()}
        check(
            "and holds the starter network, from the asset to the output a publish saves",
            {
                "asset",
                "break_mtl",
                "materials",
                "OUT_mtl",
                "view_dome",
                "thumbnail_cam",
                "view_karma",
            }
            <= set(nodes)
            and nodes["asset"].parm("filepath1").eval()
            == "asset/kitchen/frying_pan/publish/asset/v001/frying_pan.usda"
            and nodes["materials"].parm("matpathprefix").eval() == "/frying_pan/mtl/"
            and nodes["materials"].node("piper_material").parm("textures").eval() == ""
            and nodes["view_karma"].isDisplayFlagSet(),
            ", ".join(sorted(nodes)),
        )
        slots = hou.node(OUTPUT).stage().GetPrimAtPath("/frying_pan/mtl").GetChildren()
        check(
            "the asset, spelled from the production root, composes with its slots",
            [prim.GetName() for prim in slots] == ["woodSG", "metalSG"]
            and not nodes["asset"].errors(),
            str(nodes["asset"].errors()),
        )
        check(
            "the scene's pin is the version the sublayer loads, and the parm loading it",
            scene_pin(production.root, pan) == (1, "/stage/asset/filepath1"),
            str(scene_pin(production.root, pan)),
        )

        hou.node("/stage").createNode("null", "kept")
        hou.hipFile.save()
        saved = expected.stat().st_mtime_ns
        hou.hipFile.clear()
        open_work(tracker, production, pan, lookdev)
        check(
            "existing work is opened, not rewritten",
            expected.stat().st_mtime_ns == saved and hou.node("/stage/kept") is not None,
        )

        copied = root / "asset" / "kitchen" / "sauce_pot" / "work" / "lookdev" / "sauce_pot.hipnc"
        copied.parent.mkdir(parents=True)
        shutil.copyfile(expected, copied)
        open_work(tracker, production, pot, lookdev)
        hou.hipFile.clear()
        hou.hipFile.load(str(copied))
        check(
            "a file copied from another asset is restamped and saved as this asset's work",
            scene_stamp() == stamp_of(pot)
            and hou.node("/stage/kept") is not None
            and shown.messages[-1:]
            == [
                "This file was lookdev work on Frying Pan, in host_check.\n\n"
                "It is now lookdev work on Sauce Pot, and has been saved that way."
            ],
            str(scene_stamp()),
        )

        expected.write_bytes(b"not a Houdini scene")
        refused = refusal(lambda: open_work(tracker, production, pan, lookdev))
        check("a file Houdini cannot read is refused in words", "Houdini could not open" in refused)

        shutil.copyfile(copied, expected)
        expected.chmod(0o444)
        check(
            "a copied file Houdini cannot save is refused in words",
            "could not save it" in refusal(lambda: open_work(tracker, production, pan, lookdev)),
        )
        expected.chmod(0o644)


def check_publish_work(production: "Production", check: "Callable[..., None]") -> None:
    """Publish the output's layer the way the menu does, into this run's own production."""
    import shutil
    from unittest import mock

    import hou
    from pxr import Ar, Sdf, Usd, UsdShade

    from piper.errors import PiperError
    from piper.registry import Registry
    from piper.tracker import Asset
    from piper_houdini import ui
    from piper_houdini.publish import (
        publish_work,
        save_layer,
        scene_asset,
        scene_pin,
        surfaces,
    )
    from piper_houdini.work import OUTPUT, open_work
    from piper_studio import compose
    from piper_studio.context import context_named

    kettle = Asset(id="303", name="Kettle", type="Prop", folder="kitchen", pipe_name="kettle")

    class Assets:
        def asset(self, id: str) -> Asset | None:
            return kettle if id == kettle.id else None

    tracker = cast("Tracker", Assets())
    registry = cast("Registry", Records())
    root = Path(str(production.root))
    (root / "asset" / "kitchen" / "kettle").mkdir(parents=True)
    publish_geo(root, kettle, 1)
    shown = Shown()

    def refusal(action: "Callable[[], object]") -> str:
        try:
            action()
        except PiperError as refused:
            return str(refused)
        return ""

    def versions(product: str) -> list[int]:
        return compose.versions(production.root, kettle, product)

    with mock.patch.object(hou, "ui", shown, create=True):
        work = open_work(tracker, production, kettle, context_named("lookdev", subject="asset"))
    assert work is not None
    stage = hou.node("/stage")
    materials = hou.node("/stage/materials")
    wood = materials.createNode("subnet", "woodSG")
    for child in wood.children():
        child.destroy()
    rman = wood.createNode("pxrsurface", "rman")
    mtlx = wood.createNode("mtlxstandard_surface", "mtlx")
    collect = wood.createNode("collect", "OUT")
    collect.setInput(0, rman, 0)
    collect.setInput(1, mtlx, 0)
    wood.setMaterialFlag(True)

    check(
        "a saved scene at its work path names its asset", scene_asset(tracker, production) == kettle
    )
    named = compose.slots(production.root, kettle, compose.current_pins(production.root, kettle)[1])
    check("the slots are the pinned geo's, in its order", named == ["woodSG", "metalSG"])
    with tempfile.TemporaryDirectory() as directory:
        layer = save_layer(Path(directory), kettle, named)
        saved = Sdf.Layer.OpenAsAnonymous(str(layer))
        check(
            "the output saves the materials alone, as overs on the slots",
            layer.name == "mtl.usda"
            and saved.defaultPrim == "kettle"
            and not saved.subLayerPaths
            and [prim.path for prim in saved.rootPrims] == [Sdf.Path("/kettle")]
            and saved.GetPrimAtPath("/kettle/mtl/woodSG").specifier == Sdf.SpecifierOver
            and saved.GetPrimAtPath("/kettle/mtl/woodSG/rman").specifier == Sdf.SpecifierDef,
            saved.ExportToString()[:200].replace("\n", " | "),
        )
        check(
            "and each slot is reported with what it was given",
            surfaces(layer, kettle, named) == {"woodSG": ["ri", "mtlx", "preview"], "metalSG": []},
            str(surfaces(layer, kettle, named)),
        )
        before = work.stat().st_mtime_ns, work.read_bytes()
        result = publish_work(registry, production, kettle, layer, saved=False)
    published = Path(str(result.component.path)).parent
    check(
        "publishing without saving makes mtl v001 and asset v002, pinning both, current, "
        "with a copy of the unsaved scene as the source and the work file left alone",
        (result.component.product, result.component.version) == ("mtl", 1)
        and result.asset_version is not None
        and result.asset_version.version == 2
        and dict(result.pins) == {"geo": 1, "mtl": 1}
        and result.current
        and (published / "src" / "kettle.hipnc").is_file()
        and (work.stat().st_mtime_ns, work.read_bytes()) == before
        and hou.hipFile.path() == str(work),
        str(result),
    )
    entry = compose.entry_path(production.root, kettle, 2)
    composed = Usd.Stage.Open(str(entry), Ar.DefaultResolverContext([str(root)]), Usd.Stage.LoadAll)
    body = composed.GetPrimAtPath("/kettle/geo/render/body")
    bound = UsdShade.MaterialBindingAPI(body).ComputeBoundMaterial()[0]
    surface = bound.ComputeSurfaceSource("ri")
    check(
        "the asset version composes Maya's geometry with Houdini's RenderMan surface",
        composed.GetCompositionErrors() == []
        and bound.GetPath() == Sdf.Path("/kettle/mtl/woodSG")
        and surface[0].GetPrim().GetPath() == Sdf.Path("/kettle/mtl/woodSG/rman")
        and surface[0].GetShaderId() == "PxrSurface",
        str(surface[0].GetPrim().GetPath()) if surface[0] else "no ri surface",
    )
    asset_node = hou.node("/stage/asset")
    layer_break = hou.node("/stage/break_mtl")
    layer_break.destroy()
    materials.setInput(0, asset_node)
    with tempfile.TemporaryDirectory() as directory:
        refused = refusal(lambda: save_layer(Path(directory), kettle, named))
        check(
            "without the Layer Break, the asset would go out as mtl, so it is refused by name",
            "keep a Layer Break" in refused,
            refused[:90],
        )
        layer_break = stage.createNode("layerbreak", "break_mtl")
        layer_break.setInput(0, asset_node)
        materials.setInput(0, layer_break)

        junk = stage.createNode("primitive", "junk")
        junk.parm("primpath").set("/kettle/geo/junk")
        junk.setInput(0, materials)
        output = hou.node(OUTPUT)
        output.setInput(0, junk)
        refused = refusal(lambda: save_layer(Path(directory), kettle, named))
        check(
            "a prim outside the materials is refused by path",
            "authors /kettle/geo, outside /kettle/mtl" in refused,
            refused[:90],
        )
        output.setInput(0, materials)
        junk.destroy()

        stray = materials.createNode("subnet", "wood")
        stray.setMaterialFlag(True)
        refused = refusal(lambda: save_layer(Path(directory), kettle, named))
        check(
            "a material named for no slot is refused, and the slots are named",
            "defines wood under /kettle/mtl, which is not a slot of the geo" in refused
            and refused.endswith("(woodSG, metalSG)"),
            refused[-90:],
        )
        stray.destroy()

        output.setName("elsewhere")
        check(
            "a scene with no output to publish from is refused",
            f"has no {OUTPUT}" in refusal(lambda: save_layer(Path(directory), kettle, named)),
        )
        output.setName("OUT_mtl")

    second = stage.createNode("sublayer", "again")
    second.parm("filepath1").set(str(entry))
    check(
        "a scene loading two asset versions is refused, naming both",
        "it loads 2: /stage/again/filepath1 (v002), /stage/asset/filepath1 (v001)"
        in refusal(lambda: scene_pin(production.root, kettle)),
        refusal(lambda: scene_pin(production.root, kettle)),
    )
    second.destroy()

    publish_geo(root, kettle, 2)

    asked: list[tuple[list[str], dict[str, tuple[list[int], int]], list[str]]] = []
    answers: list[tuple[str, dict[str, int]] | None] = []

    def answer_window(
        lines: list[str], offered: dict[str, tuple[list[int], int]], buttons: list[str]
    ) -> tuple[str, dict[str, int]] | None:
        asked.append((lines, offered, buttons))
        return answers.pop(0)

    with (
        mock.patch.object(ui, "tracker_for", lambda _: tracker),
        mock.patch.object(ui, "registry_for", lambda _: registry),
        mock.patch.object(ui, "publish_window", answer_window),
        mock.patch.object(hou, "ui", shown, create=True),
    ):
        answers[:] = [None]
        ui.show_publish()
        lines, offered, buttons = asked[0]
        check(
            "Publish… says what the scene loads, what is current, and what each slot gets",
            lines[0] == "Publish to Kettle, mtl?"
            and "This scene loads asset v001 (/stage/asset/filepath1), pinning geo v001." in lines
            and "Current is asset v003, pinning geo v002, mtl v001." in lines
            and lines[-2:] == ["woodSG: ri, mtlx, preview", "metalSG: nothing"],
            " | ".join(lines),
        )
        check(
            "offers the other component's versions, starting on current's pin",
            offered == {"geo": ([1, 2], 2)}
            and buttons == ["Save and Publish", "Publish Without Saving"]
            and versions("mtl") == [1],
            f"{offered} {buttons}",
        )
        saved = work.stat().st_mtime_ns
        shutil.rmtree(work.parent / "backup", ignore_errors=True)
        answers[:] = [("Save and Publish", {"geo": 2})]
        ui.show_publish()
        second = Path(str(result.component.path)).parent.with_name("v002") / "src" / "kettle.hipnc"
        check(
            "Save and Publish saves the work file, publishes it as the source, and says what it "
            "made and pinned",
            work.stat().st_mtime_ns != saved
            and versions("mtl") == [1, 2]
            and second.read_bytes() == work.read_bytes()
            and not (work.parent / "backup").exists()
            and compose.pins(production.root, kettle, 4) == {"geo": 2, "mtl": 2}
            and "Published mtl v002 of 'Kettle'" in shown.messages[-1]
            and "asset v004 pins geo v002 and mtl v002, and is current" in shown.messages[-1],
            shown.messages[-1].replace("\n", " | "),
        )
        saved = work.stat().st_mtime_ns, work.read_bytes()
        answers[:] = [("Publish Without Saving", {"geo": 1})]
        ui.show_publish()
        check(
            "Publish Without Saving pins the version chosen, and leaves the work file as it was",
            (work.stat().st_mtime_ns, work.read_bytes()) == saved
            and compose.pins(production.root, kettle, 5) == {"geo": 1, "mtl": 3}
            and "asset v005 pins geo v001 and mtl v003, and is current" in shown.messages[-1],
            shown.messages[-1].replace("\n", " | "),
        )
        work.chmod(0o444)
        answers[:] = [("Save and Publish", {"geo": 2})]
        ui.show_publish()
        work.chmod(0o644)
        check(
            "a save Houdini refuses is shown in Houdini's words, and publishes nothing",
            "Houdini could not save the scene (" in shown.messages[-1]
            and versions("mtl") == [1, 2, 3],
            shown.messages[-1][:90],
        )
        hou.node(OUTPUT).setName("elsewhere")
        ui.show_publish()
        check(
            "a refusal is shown to the artist, and nothing is asked",
            f"has no {OUTPUT}" in shown.messages[-1]
            and len(asked) == 4
            and versions("mtl") == [1, 2, 3],
            shown.messages[-1][:80],
        )

    hou.hipFile.load(str(published / "src" / "kettle.hipnc"))
    check(
        "its source reopens holding the materials",
        hou.node("/stage/materials/woodSG/rman") is not None,
    )
    check(
        "a stamped scene that is not at its work path is refused",
        f"not Kettle's lookdev work at {work}" in refusal(lambda: scene_asset(tracker, production)),
        refusal(lambda: scene_asset(tracker, production))[:70],
    )
    hou.hipFile.clear()
    check(
        "a scene with no stamp is refused",
        "is not host_check work" in refusal(lambda: scene_asset(tracker, production)),
    )
    with (
        mock.patch.object(ui, "tracker_for", lambda _: tracker),
        mock.patch.object(hou, "ui", shown, create=True),
    ):
        ui.open_launched_work(kettle.id, "lookdev")
    check(
        "opening work that loads an older asset version than current says so",
        hou.hipFile.path() == str(work)
        and shown.messages[-1]
        == "This scene loads asset v001 (/stage/asset/filepath1), pinning geo v001.\n"
        "Current is asset v005, pinning geo v001, mtl v003.",
        shown.messages[-1].replace("\n", " | "),
    )
    check(
        "a publish left nothing in Houdini's output network",
        hou.node("/out").children() == (),
        ", ".join(node.name() for node in hou.node("/out").children()),
    )


def check_material(production: "Production", check: "Callable[..., None]") -> None:
    """Generate materials from textures, as the node's button, Open Work…, and Use Textures… do."""
    from unittest import mock

    import hou
    from pxr import Sdf, UsdShade

    from piper.errors import PiperError
    from piper.registry import Registry
    from piper.tracker import Asset
    from piper_houdini import material, ui
    from piper_houdini.publish import publish_work, save_layer
    from piper_houdini.work import OUTPUT, open_work
    from piper_studio import compose
    from piper_studio.context import context_named
    from piper_studio.launch import release_root
    from piper_studio.publish import publish_textures

    root = Path(str(production.root))
    shown = Shown()

    def refusal(action: "Callable[[], object]") -> str:
        try:
            action()
        except PiperError as refused:
            return str(refused)
        return ""

    def source(node: hou.Node, input_name: str) -> tuple[str, str] | None:
        """Which node's output feeds ``input_name``: Houdini names them from the wire's view."""
        for connection in node.inputConnections():
            if connection.outputName() == input_name:
                return connection.inputNode().name(), connection.inputName()
        return None

    node_type = hou.nodeType(hou.vopNodeTypeCategory(), material.TYPE)
    library_file = Path(node_type.definition().libraryFilePath()) if node_type else None
    check(
        "the Piper Material node loads from the repository's text HDA",
        library_file
        == release_root() / "packages" / "piper-houdini" / "otls" / "piper_material.hda",
        str(library_file),
    )

    # A class project's directory: converted by `piper convert`, with previews beside.
    exported = root / "class_textures"
    exported.mkdir()
    for udim in ("1001", "1002"):
        for extension in ("tex", "jpg", "png"):
            (exported / f"woodSG_BaseColor.{udim}.{extension}").touch()
    for name in ("Metallic", "SpecularRoughness", "Normal", "Emissive", "Presence"):
        for extension in ("tex", "jpg", "png"):
            (exported / f"woodSG_{name}.1001.{extension}").touch()
    for name in ("woodSG_Displacement.1001.tex", "metalSG_BaseColor.1001.tex", "Thumbs.db"):
        (exported / name).touch()
    (exported / "src").mkdir()

    hou.hipFile.clear()
    library = hou.node("/stage").createNode("materiallibrary", "materials")
    library.parm("matpathprefix").set("/kettle/mtl/")
    library.parm("matnode1").set("*")
    library.parm("matflag1").set(1)
    library.parm("assign1").set(0)
    generator = library.createNode(material.TYPE, material.NAME)
    generator.parm("textures").set(str(exported))
    result = material.add_materials(generator)
    check(
        "Add Materials adds a material per texture set, and says which map it cannot wire",
        result == material.AddMaterialsResult(("metalSG", "woodSG"), ("Displacement",), ()),
        str(result),
    )
    wood = library.node("woodSG")
    rman = wood.node("rman")
    workflow = wood.node("workflow")
    check(
        "woodSG's PxrSurface reads colour and metallic through the metallic workflow, roughness, "
        "the normal map as OpenGL, emissive with glow on, and presence, from the .tex files",
        wood.isMaterialFlagSet()
        and source(rman, "diffuseColor") == ("workflow", "resultDiffuseRGB")
        and source(rman, "specularFaceColor") == ("workflow", "resultSpecularFaceRGB")
        and source(rman, "specularEdgeColor") == ("workflow", "resultSpecularEdgeRGB")
        and source(workflow, "baseColor") == ("BaseColor", "resultRGB")
        and source(workflow, "metallic") == ("Metallic", "resultR")
        and source(rman, "specularRoughness") == ("SpecularRoughness", "resultR")
        and source(rman, "bumpNormal") == ("Normal", "resultN")
        and wood.node("Normal").parm("orientation").evalAsString() == "0"
        and source(rman, "glowColor") == ("Emissive", "resultRGB")
        and rman.parm("glowGain").eval() == 1
        and source(rman, "presence") == ("Presence", "resultR")
        and wood.node("BaseColor").parm("filename").eval()
        == f"{exported}/woodSG_BaseColor.<UDIM>.tex"
        and wood.node("BaseColor").parm("filename_colorspace").eval() == "rendering"
        and wood.node("Normal").parm("filename_colorspace").eval() == "data"
        and wood.node("Displacement") is None,
        ", ".join(sorted(child.name() for child in wood.children())),
    )
    preview = wood.node("preview")
    normal = wood.node("preview_Normal")
    check(
        "and its UsdPreviewSurface reads the jpegs, colour as sRGB, data raw, the normal rescaled",
        source(preview, "diffuseColor") == ("preview_BaseColor", "rgb")
        and source(preview, "metallic") == ("preview_Metallic", "r")
        and source(preview, "roughness") == ("preview_SpecularRoughness", "r")
        and source(preview, "emissiveColor") == ("preview_Emissive", "rgb")
        and source(preview, "opacity") == ("preview_Presence", "r")
        and source(preview, "normal") == ("preview_Normal", "rgb")
        and source(normal, "st") == ("st", "result")
        and wood.node("preview_BaseColor").parm("file").eval()
        == f"{exported}/woodSG_BaseColor.<UDIM>.jpg"
        and wood.node("preview_BaseColor").parm("sourceColorSpace").eval() == "sRGB"
        and normal.parm("sourceColorSpace").eval() == "raw"
        and normal.parmTuple("scale").eval() == (2, 2, 2, 1)
        and normal.parmTuple("bias").eval() == (-1, -1, -1, 0),
        ", ".join(sorted(child.name() for child in wood.children())),
    )
    metal = library.node("metalSG")
    grey = tuple(round(value, 2) for value in metal.node("workflow").parmTuple("baseColor").eval())
    check(
        "metalSG, with no jpeg, gets RenderMan alone, its workflow starting from the surface's "
        "grey",
        metal.node("preview") is None
        and len(metal.node("OUT").inputConnections()) == 1
        and grey == (0.18, 0.18, 0.18)
        and metal.node("rman").parm("glowGain").eval() == 0,
        ", ".join(sorted(child.name() for child in metal.children())),
    )
    with tempfile.TemporaryDirectory() as directory:
        layer = Path(directory) / "mtl.usda"
        rop = hou.node("/out").createNode("usd", "piper_check")
        rop.parm("loppath").set(library.path())
        rop.parm("lopoutput").set(str(layer))
        rop.render()
        rop.destroy()
        saved = Sdf.Layer.OpenAsAnonymous(str(layer))

        def surface_of(slot: str, output: str = "outputs:surface") -> str:
            spec = saved.GetPrimAtPath(f"/kettle/mtl/{slot}")
            return str(spec.attributes[output].connectionPathList.explicitItems[0])

        colour = saved.GetPrimAtPath("/kettle/mtl/woodSG/BaseColor").attributes["inputs:filename"]
        jpeg = saved.GetPrimAtPath("/kettle/mtl/woodSG/preview_BaseColor").attributes["inputs:file"]
        # The ROP spells an absolute path from the layer; a root-relative one it leaves alone.
        check(
            "the library saves each as a Material with a RenderMan surface and the preview "
            "surface reading jpegs, or Houdini's own where there are none, with <UDIM> kept",
            surface_of("woodSG", "outputs:ri:surface") == "/kettle/mtl/woodSG/rman.outputs:bxdf_out"
            and surface_of("woodSG") == "/kettle/mtl/woodSG/preview.outputs:surface"
            and surface_of("metalSG") == "/kettle/mtl/metalSG/rman_preview.outputs:surface"
            and colour.default.path.endswith("/class_textures/woodSG_BaseColor.<UDIM>.tex")
            and jpeg.default.path.endswith("/class_textures/woodSG_BaseColor.<UDIM>.jpg"),
            f"{surface_of('woodSG')} {surface_of('metalSG')} {colour.default}",
        )

    rman.parm("diffuseGain").set(0.5)
    before = sorted(child.name() for child in wood.children())
    again = material.add_materials(generator)
    metal.destroy()
    returned = material.add_materials(generator)
    check(
        "Add Materials again adds nothing and keeps an edit; a deleted material alone comes back",
        again.added == ()
        and rman.parm("diffuseGain").eval() == 0.5
        and sorted(child.name() for child in wood.children()) == before
        and returned.added == ("metalSG",),
        f"{again} | {returned}",
    )

    fewer = root / "fewer_textures"
    fewer.mkdir()
    (fewer / "woodSG_BaseColor.1001.tex").touch()
    generator.parm("textures").set(str(fewer))
    missing = material.add_materials(generator).missing
    check(
        "pointed at a directory lacking maps, the materials read what is not there, and it is said",
        len(missing) == 12
        and ("woodSG/Emissive", "woodSG_Emissive.<UDIM>.tex") in missing
        and ("woodSG/preview_BaseColor", "woodSG_BaseColor.<UDIM>.jpg") in missing
        and ("metalSG/BaseColor", "metalSG_BaseColor.<UDIM>.tex") in missing
        and not any(reader == "woodSG/BaseColor" for reader, _ in missing)
        and material.added_line(material.AddMaterialsResult((), (), missing[:1]))
        == "Every texture set has a material. metalSG/BaseColor reads metalSG_BaseColor.<UDIM>.tex,"
        " which the directory does not hold.",
        str(missing),
    )

    unnameable = root / "unnameable"
    unnameable.mkdir()
    (unnameable / "Clothes Mat_BaseColor.1001.tex").touch()
    refused = {}
    for spelled in (str(root / "nowhere"), str(root), "", str(unnameable)):
        generator.parm("textures").set(spelled)
        refused[spelled] = refusal(lambda: material.add_materials(generator))
    check(
        "a directory Houdini cannot find, one with no textures, none, and a texture set that "
        "cannot name a material are each refused in words, before anything is made",
        "is not a directory Houdini can find" in refused[str(root / "nowhere")]
        and "publish/tex/v003" in refused[str(root / "nowhere")]
        and "holds no texture named" in refused[str(root)]
        and "names no textures directory" in refused[""]
        and "cannot name a material" in refused[str(unnameable)]
        and library.node("Clothes Mat") is None,
        " | ".join(text[:60] for text in refused.values()),
    )

    generator.parm("textures").set(str(exported))
    with mock.patch.object(hou, "ui", shown, create=True):
        generator.parm("add_materials").pressButton()
        pressed = shown.messages[-1]
        generator.parm("textures").set("")
        generator.parm("add_materials").pressButton()
    check(
        "the node's button adds materials and says what it added, or what it refused",
        pressed
        == "Every texture set has a material. No material reads Displacement: wire it by hand."
        and "names no textures directory" in shown.messages[-1],
        f"{pressed} | {shown.messages[-1]}",
    )

    teapot = Asset(id="404", name="Teapot", type="Prop", folder="kitchen", pipe_name="teapot")

    class Assets:
        def asset(self, id: str) -> Asset | None:
            return teapot if id == teapot.id else None

    tracker = cast("Tracker", Assets())
    registry = cast("Registry", Records())
    (root / "asset" / "kitchen" / "teapot").mkdir(parents=True)
    publish_geo(root, teapot, 1)
    renderman = root / "renderman"
    (renderman / "bin").mkdir(parents=True)
    oiiotool = renderman / "bin" / "rmanoiiotool"
    oiiotool.write_text('#!/bin/bash\ntouch "${!#}"\n', encoding="utf-8")
    oiiotool.chmod(0o755)
    export = root / "export" / "teapot_textures"
    export.mkdir(parents=True)
    for name in ("BaseColor", "Normal"):
        for extension in ("png", "jpg"):
            (export / f"woodSG_{name}.1001.{extension}").touch()
    first = publish_textures(
        registry,
        root=production.root,
        asset=teapot,
        export=PurePosixPath(export),
        renderman=renderman,
    )
    assert first.textures.version == 1 and first.material is None, first
    with mock.patch.object(hou, "ui", shown, create=True):
        work = open_work(tracker, production, teapot, context_named("lookdev", subject="asset"))
    assert work is not None
    reading = "/stage/materials/piper_material/textures"
    filename = hou.node("/stage/materials/woodSG/BaseColor").parm("filename").eval()
    check(
        "Open Work… starts the network with a Piper Material reading the newest tex, spelled from "
        "the root, with its materials added once",
        hou.node("/stage/materials/piper_material").type().name() == material.TYPE
        and hou.parm(reading).eval() == "asset/kitchen/teapot/publish/tex/v001"
        and filename == "asset/kitchen/teapot/publish/tex/v001/woodSG_BaseColor.<UDIM>.tex"
        and material.scene_textures(production.root, teapot) == [(reading, 1)]
        and hou.node("/stage/materials/metalSG") is None,
        filename,
    )
    composed = hou.node(OUTPUT).stage()
    wood_material = UsdShade.Material(composed.GetPrimAtPath("/teapot/mtl/woodSG"))
    check(
        "and the output composes it onto the slot, resolving the version through the root",
        wood_material.GetSurfaceOutput("ri").HasConnectedSource()
        and wood_material.GetSurfaceOutput().HasConnectedSource()
        and not hou.node("/stage/materials").errors(),
        str(hou.node("/stage/materials").errors()),
    )
    named = compose.slots(production.root, teapot, compose.current_pins(production.root, teapot)[1])
    with tempfile.TemporaryDirectory() as directory:
        layer = save_layer(Path(directory), teapot, named)
        published = publish_work(registry, production, teapot, layer, saved=False)
    check(
        "Publish… publishes the generated material as mtl v001, reading tex v001",
        (published.component.product, published.component.version) == ("mtl", 1)
        and published.current
        and dict(published.pins) == {"geo": 1, "mtl": 1}
        and "tex/v001/woodSG_BaseColor.<UDIM>.tex"
        in Path(str(published.component.path)).read_text(),
        str(published),
    )
    second = publish_textures(
        registry,
        root=production.root,
        asset=teapot,
        export=PurePosixPath(export),
        renderman=renderman,
    )
    check(
        "a second tex publish derives mtl v002 from the material Houdini made",
        second.material is not None
        and (second.material.component.version, second.derived_from) == (2, 1)
        and second.material.current
        and second.warnings == (),
        str(second),
    )
    with (
        mock.patch.object(ui, "tracker_for", lambda _: tracker),
        mock.patch.object(hou, "ui", shown, create=True),
    ):
        ui.open_launched_work(teapot.id, "lookdev")
    check(
        "Open Work… says the scene loads an older asset version and reads older textures",
        shown.messages[-1]
        == "This scene loads asset v001 (/stage/asset/filepath1), pinning geo v001.\n"
        "Current is asset v003, pinning geo v001, mtl v002.\n"
        f"This scene reads tex v001 ({reading}); the newest is tex v002.",
        shown.messages[-1].replace("\n", " | "),
    )

    shown.choice = [1]
    with (
        mock.patch.object(ui, "tracker_for", lambda _: tracker),
        mock.patch.object(hou, "ui", shown, create=True),
    ):
        ui.show_use_textures()
    check(
        "Use Textures… offers the installed tex versions, newest marked, and points the node at "
        "the chosen one",
        shown.asked[-1][0] == ["v001", "v002    (newest)"]
        and shown.asked[-1][1]["default_choices"] == [1]
        and hou.parm(reading).eval() == "asset/kitchen/teapot/publish/tex/v002"
        and shown.messages[-1]
        == "/stage/materials/piper_material reads tex v002.\nAdd Materials adds a material for a "
        "texture set that has none.",
        f"{shown.asked[-1][0]} | {shown.messages[-1]}",
    )
    extra = hou.node("/stage/materials").createNode(material.TYPE)
    with (
        mock.patch.object(ui, "tracker_for", lambda _: tracker),
        mock.patch.object(hou, "ui", shown, create=True),
    ):
        ui.show_use_textures()
        two = shown.messages[-1]
        extra.destroy()
        hou.node("/stage/materials/piper_material").destroy()
        ui.show_use_textures()
    check(
        "a scene with two Piper Material nodes, or none, is refused",
        "has 2 Piper Material nodes" in two and "has no Piper Material node" in shown.messages[-1],
        f"{two[:80]} | {shown.messages[-1][:80]}",
    )


def run_in_host() -> int:
    """Compose Houdini's environment for a production and run this file under ``hython`` in it."""
    from piper.errors import PiperError
    from piper_studio.launch import (
        environment,
        houdini_executable,
        houdini_variables,
        working_directory,
    )
    from piper_studio.production import load_production
    from piper_studio.profile import Profile

    with tempfile.TemporaryDirectory() as directory:
        path = write_production(Path(directory))
        profile = Profile(name="host_check", production=load_production(path), path=path)
        try:
            hython = houdini_executable(profile).parent / "hython"
        except PiperError as refusal:
            print(f"piper: {refusal}", file=sys.stderr)
            return 1
        if not os.access(hython, os.X_OK):
            print(f"piper: {hython.name} is not installed at {hython}", file=sys.stderr)
            return 1
        return subprocess.call(
            [str(hython), str(Path(__file__).resolve())],
            env=environment(os.environ, houdini_variables(profile)),
            cwd=working_directory(profile),
        )


def write_production(root: Path) -> Path:
    """A production of this run's own, holding one Houdini package."""
    tools = root / "tools" / "houdini"
    tools.mkdir(parents=True)
    (tools / f"{PACKAGE}.json").write_text('{"env": [{"PIPER_CHECK": "1"}]}\n', encoding="utf-8")
    path = root / "production.toml"
    path.write_text(_PRODUCTION.format(root=root), encoding="utf-8")
    return path


if __name__ == "__main__":
    try:
        import hou  # noqa: F401
    except ImportError:
        sys.exit(run_in_host())
    sys.exit(check_host())
