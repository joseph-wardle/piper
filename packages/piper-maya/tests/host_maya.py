"""Checks Piper's composed environment, and opening and publishing work, inside a real Maya."""

import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from collections.abc import Callable

    from piper.tracker import Tracker
    from piper_studio.production import Production

TOOL = "piper_check"
"""A Maya tool the production supplies, and Piper never parses."""

_PRODUCTION = """
name = "host_check"
root = "{root}"
types = ["Prop"]

[shotgrid]
site = "https://example.invalid"
script = "piper"
project = 0
"""


def check_host() -> int:
    """Assert, inside Maya, what the composed environment promised."""
    import maya.standalone

    maya.standalone.initialize(name="python")

    failures = []

    def check(name: str, ok: bool, detail: str = "") -> None:
        print(f"{'pass' if ok else 'FAIL'}  {name}{('  ' + detail) if detail else ''}")
        if not ok:
            failures.append(name)

    import piper
    import piper_studio.publish  # noqa: F401

    check("piper and piper_studio.publish import", True, piper.__file__)

    from piper_studio.launch import maya_executable
    from piper_studio.profile import active

    profile = active()
    production = profile.production
    check(
        "Maya works in the production that launched it",
        production is not None and production.name == "host_check",
        profile.name,
    )
    check(
        "Maya starts at the production's root",
        production is not None and Path.cwd() == Path(str(production.root)),
        str(Path.cwd()),
    )
    # Maya sets MAYA_LOCATION from the executable it was started as, so this is the
    # install answering, not Piper's own resolution repeated back to itself.
    started_from = os.environ.get("MAYA_LOCATION", "")
    resolved = maya_executable(profile).parent.parent
    check(
        "Maya runs from the install Piper resolved",
        bool(started_from) and Path(started_from).resolve() == resolved.resolve(),
        started_from or "unset",
    )

    from pxr import Usd

    check(
        "pxr is Maya's own USD, at the version the profile selected",
        Usd.GetVersion() == (0, 25, 5) and "mayausd" in Usd.__file__,
        f"{Usd.GetVersion()} {Usd.__file__}",
    )

    from maya import cmds

    modules = set(cmds.moduleInfo(listModules=True))
    check(f"the production's Maya tool {TOOL} loaded", TOOL in modules)
    # Piper assigns MAYA_MODULE_PATH. Maya appends its own locations to it, so an
    # artist entering a production keeps every tool they had outside one.
    others = sorted(modules - {TOOL})
    check(
        "Maya still finds the modules Piper never named",
        bool(others),
        f"{len(others)}: {', '.join(others[:6])}…" if others else "none",
    )

    for name in ("cyclopts", "rich"):
        try:
            __import__(name)
        except ImportError:
            check(f"{name} is absent", True)
        else:
            check(f"{name} is absent", False, "imported: the command line's environment leaked in")

    import PySide6
    import shotgun_api3

    import piper_maya

    check(
        "shotgun_api3 comes from piper-maya's own environment",
        str(Path(piper_maya.__file__).parents[2] / ".venv") in shotgun_api3.__file__,
        shotgun_api3.__file__,
    )
    check(
        "that environment changed nothing Maya brings",
        sys.prefix == started_from and started_from in PySide6.__file__,
        f"{sys.prefix} {PySide6.__file__}",
    )

    if production is not None:
        check_open_work(production, check)
        check_publish_work(production, check)

    print(f"\n{len(failures)} failed" if failures else "\nall passed")
    return 1 if failures else 0


def check_open_work(production: "Production", check: "Callable[..., None]") -> None:
    """Open work the way the menu and the startup command do, on assets of this run's own."""
    import shutil

    from maya import cmds

    from piper.errors import PiperError
    from piper.tracker import Asset
    from piper_maya.work import open_work, scene_stamp
    from piper_studio.context import context_named

    pan = Asset(id="101", name="Frying Pan", type="Prop", folder="kitchen", pipe_name="frying_pan")
    pot = Asset(id="202", name="Sauce Pot", type="Prop", folder="kitchen", pipe_name="sauce_pot")

    class Assets:
        def asset(self, id: str) -> Asset | None:
            return next((asset for asset in (pan, pot) if asset.id == id), None)

    # Opening work asks a tracker for nothing but an asset by its id.
    tracker = cast("Tracker", Assets())
    modeling = context_named("modeling", subject="asset")
    root = Path(str(production.root))
    for asset in (pan, pot):
        (root / "asset" / "kitchen" / (asset.pipe_name or "")).mkdir(parents=True)

    def stamp_of(asset: Asset) -> dict[str, str | None]:
        return {
            "piper_production": production.name,
            "piper_asset_id": asset.id,
            "piper_context": "modeling",
        }

    file = open_work(tracker, production, pan, modeling)
    expected = root / "asset" / "kitchen" / "frying_pan" / "work" / "modeling" / "frying_pan.mb"
    check(
        "new work is saved at its path before the artist does anything",
        file == expected and expected.is_file(),
    )
    check("and is the open scene", cmds.file(query=True, sceneName=True) == str(expected))
    check("and is stamped with what it is", scene_stamp() == stamp_of(pan), str(scene_stamp()))
    check(
        "Maya's project is the work's directory",
        Path(cmds.workspace(query=True, rootDirectory=True)) == expected.parent,
        cmds.workspace(query=True, rootDirectory=True),
    )

    cmds.polyCube()
    cmds.file(save=True)
    saved = expected.stat().st_mtime_ns
    cmds.file(new=True, force=True)
    open_work(tracker, production, pan, modeling)
    check(
        "existing work is opened, not rewritten",
        expected.stat().st_mtime_ns == saved and bool(cmds.ls("pCube1")),
    )

    copied = root / "asset" / "kitchen" / "sauce_pot" / "work" / "modeling" / "sauce_pot.mb"
    copied.parent.mkdir(parents=True)
    shutil.copyfile(expected, copied)
    open_work(tracker, production, pot, modeling)
    cmds.file(new=True, force=True)
    cmds.file(str(copied), open=True, force=True)
    check(
        "a file copied from another asset is restamped and saved as this asset's work",
        scene_stamp() == stamp_of(pot) and bool(cmds.ls("pCube1")),
        str(scene_stamp()),
    )

    expected.write_bytes(b"not a Maya scene")
    try:
        open_work(tracker, production, pan, modeling)
    except PiperError as refusal:
        check("a file Maya cannot read is refused in words", True, str(refusal)[:80])
    else:
        check("a file Maya cannot read is refused in words", False, "opened")
    check(
        "and the artist keeps the scene and the project they had",
        cmds.file(query=True, sceneName=True) == str(copied)
        and Path(cmds.workspace(query=True, rootDirectory=True)) == copied.parent,
        cmds.workspace(query=True, rootDirectory=True),
    )

    shutil.copyfile(copied, expected)
    expected.chmod(0o444)
    try:
        open_work(tracker, production, pan, modeling)
    except PiperError as refusal:
        check("a copied file Maya cannot save is refused in words", True, str(refusal)[-60:])
    else:
        check("a copied file Maya cannot save is refused in words", False, "saved")


def check_publish_work(production: "Production", check: "Callable[..., None]") -> None:
    """Publish a scene's selection the way the menu does, into this run's own production."""
    import dataclasses
    from unittest import mock

    from maya import cmds
    from pxr import Sdf, Usd, UsdGeom, UsdShade, UsdUtils

    from piper.errors import PiperError
    from piper.registry import Registry
    from piper.tracker import Asset
    from piper_maya.publish import publish_work, scene_asset
    from piper_maya.work import open_work
    from piper_studio.context import context_named

    kettle = Asset(id="303", name="Kettle", type="Prop", folder="kitchen", pipe_name="kettle")

    class Assets:
        def asset(self, id: str) -> Asset | None:
            return kettle if id == kettle.id else None

    class Records:
        def register(self, asset: Asset, *, product: str, version: int, path: object) -> str:
            return "1"

    tracker = cast("Tracker", Assets())
    registry = cast("Registry", Records())
    root = Path(str(production.root))
    (root / "asset" / "kitchen" / "kettle").mkdir(parents=True)
    spout = root / "spout.mb"
    cmds.file(new=True, force=True)
    cmds.polyCone(name="spout")
    cmds.file(rename=str(spout))
    cmds.file(save=True, type="mayaBinary")

    def refusal(action: "Callable[[], object]") -> str:
        try:
            action()
        except PiperError as refused:
            return str(refused)
        return ""

    work = open_work(tracker, production, kettle, context_named("modeling", subject="asset"))
    assert work is not None
    body = cmds.polyCube(name="body")[0]
    cmds.group(body, cmds.polyCylinder(name="handle")[0], name="kettle_grp")
    cmds.move(0, 2, 0, "kettle_grp")
    cmds.polySphere(name="lid")
    cmds.polyPlane(name="scratch")
    for name, faces in (("steel", body + ".f[0:2]"), ("wood", body + ".f[3:]")):
        shader = cmds.shadingNode("standardSurface", asShader=True, name=name + "_mtl")
        group = cmds.sets(renderable=True, noSurfaceShader=True, empty=True, name=name + "SG")
        cmds.connectAttr(shader + ".outColor", group + ".surfaceShader")
        cmds.sets(faces, edit=True, forceElement=group)
    texture = cmds.shadingNode("file", asTexture=True, name="wood_file")
    cmds.setAttr(texture + ".fileTextureName", str(root / "wood.png"), type="string")
    cmds.connectAttr(texture + ".outColor", "wood_mtl.baseColor")
    cmds.file(str(spout), reference=True, namespace="spout")
    cmds.file(save=True)

    cmds.select(clear=True)
    check(
        "publishing with nothing selected is refused",
        "select the geometry" in refusal(lambda: publish_work(registry, production, kettle)),
    )

    cmds.select("kettle_grp", "lid")
    numbered = cast(
        "Tracker", mock.Mock(asset=lambda id: dataclasses.replace(kettle, pipe_name="3d_kettle"))
    )
    check(
        "a scene whose asset's pipe name cannot name a root prim is refused before any export",
        "starts with a letter" in refusal(lambda: scene_asset(numbered, production)),
    )
    check(
        "a saved scene at its work path names its asset", scene_asset(tracker, production) == kettle
    )
    result = publish_work(registry, production, kettle)
    version = Path(str(result.component.path)).parent
    check(
        "a saved scene publishes as geo v001",
        (result.component.product, result.component.version, result.component.path.name)
        == ("geo", 1, "geo.usd"),
        str(result.component.path),
    )
    stage = Usd.Stage.Open(str(result.component.path))
    prims = list(stage.Traverse())
    paths = {str(prim.GetPath()) for prim in prims}
    model = "/kettle/geo/render"
    body = stage.GetPrimAtPath(f"{model}/kettle_grp/body")
    body_in_world = UsdGeom.Xformable(body).ComputeLocalToWorldTransform(Usd.TimeCode.Default())
    check(
        "the selection, and nothing else, is under the root's geo/render, where Maya had it",
        stage.GetDefaultPrim().GetPath() == Sdf.Path("/kettle")
        and {f"{model}/kettle_grp/body", f"{model}/kettle_grp/handle", f"{model}/lid"} <= paths
        and not any("scratch" in path or "spout" in path for path in paths)
        and stage.GetPrimAtPath("/kettle/geo").GetTypeName() == "Scope"
        and UsdGeom.Imageable(body).ComputePurpose() == UsdGeom.Tokens.render,
        f"kind={Usd.ModelAPI(stage.GetDefaultPrim()).GetKind()!r}",
    )
    half = max(abs(value) for point in UsdGeom.Mesh(body).GetPointsAttr().Get() for value in point)
    check(
        "in metres: a cube of one centimetre two centimetres up",
        UsdGeom.GetStageMetersPerUnit(stage) == 1.0
        and tuple(body_in_world.ExtractTranslation()) == (0, 0.02, 0)
        and abs(half - 0.005) < 1e-9,
        f"{tuple(body_in_world.ExtractTranslation())} half={half}",
    )
    materials = [prim for prim in prims if prim.IsA(UsdShade.Material)]
    families = {
        UsdGeom.Subset(prim).GetFamilyNameAttr().Get() for prim in prims if prim.IsA(UsdGeom.Subset)
    }
    bound = UsdShade.MaterialBindingAPI(stage.GetPrimAtPath(f"{model}/kettle_grp/body/steelSG"))
    check(
        "materials are named slots under /mtl that the moved geometry is still bound to",
        {"steelSG", "woodSG"} <= {prim.GetName() for prim in materials}
        and all(prim.GetPath().GetParentPath() == Sdf.Path("/kettle/mtl") for prim in materials)
        and families == {"materialBind"}
        and bound.GetDirectBinding().GetMaterialPath() == Sdf.Path("/kettle/mtl/steelSG"),
        f"{sorted(prim.GetName() for prim in materials)} {sorted(families)}",
    )
    spelled: list[str] = []
    UsdUtils.ModifyAssetPaths(
        Sdf.Layer.OpenAsAnonymous(str(result.component.path)),
        lambda path: (spelled.append(path), path)[1],
    )
    check(
        "and hold no shading and no texture paths",
        not any(prim.GetChildren() or prim.GetAuthoredProperties() for prim in materials)
        and not any(prim.IsA(UsdShade.Shader) for prim in prims)
        and not spelled,
        str(spelled),
    )
    check(
        "the version's source is the work file, byte for byte, and the scene is still unmodified",
        (version / "src" / "kettle.mb").read_bytes() == work.read_bytes()
        and not cmds.file(query=True, modified=True),
    )

    cmds.polyTorus(name="later_edit")
    cmds.select("kettle_grp", "lid", "later_edit")
    saved = work.stat().st_mtime_ns, work.read_bytes()
    result = publish_work(registry, production, kettle)
    source = Path(str(result.component.path)).parent / "src" / "kettle.mb"
    check(
        "publishing without saving leaves the work file, and the scene, as they were",
        (work.stat().st_mtime_ns, work.read_bytes()) == saved
        and cmds.file(query=True, modified=True)
        and cmds.file(query=True, sceneName=True) == str(work),
    )
    # A prim outlives no stage, so the stage is held while the prim is asked for.
    edited = Usd.Stage.Open(str(result.component.path))
    check(
        "and publishes the unsaved edit",
        result.component.version == 2 and bool(edited.GetPrimAtPath(f"{model}/later_edit")),
    )

    cmds.group(cmds.polyCube(name="inside")[0], name="geo")
    cmds.select("geo")
    check(
        "a top-level node named for the namespace is refused by name",
        "rename the top-level node geo"
        in refusal(lambda: publish_work(registry, production, kettle)),
    )
    cmds.delete("geo")

    # The menu's handler, with the artist's answers scripted: no dialog can be shown here.

    from piper_maya import ui

    shown: list[tuple[str, list[str]]] = []
    answers: list[str] = []

    def answer_dialog(*, message: str, button: list[str], **_: object) -> str:
        shown.append((message, button))
        return answers.pop(0) if answers else "OK"

    def versions() -> list[str]:
        return sorted(path.name for path in version.parent.iterdir())

    with (
        mock.patch.object(ui, "tracker_for", lambda _: tracker),
        mock.patch.object(ui, "registry_for", lambda _: registry),
        mock.patch.object(cmds, "confirmDialog", answer_dialog),
    ):
        # `body` was the default material's before its faces were assigned, and Maya
        # leaves that connection behind. Naming it would promise a slot the export does not write.
        cmds.select("body")
        answers[:] = ["Cancel"]
        ui.show_publish()
        check(
            "Publish… says what an unsaved scene would publish, and Cancel publishes nothing",
            shown[0][1] == ["Save and Publish", "Publish Without Saving", "Cancel"]
            and "Publish to Kettle, geo?" in shown[0][0]
            and "Selected: body\n" in shown[0][0]
            and shown[0][0].endswith("Materials: steelSG, woodSG")
            and len(shown) == 1
            and versions() == ["v001", "v002"],
            shown[0][0].replace("\n", " | "),
        )
        cmds.select("kettle_grp", "lid", "later_edit")
        answers[:] = ["Save and Publish"]
        ui.show_publish()
        published = Usd.Stage.Open(str(version.parent / "v003" / "geo.usd"))
        slots = sorted(p.GetName() for p in published.Traverse() if p.IsA(UsdShade.Material))
        check(
            "Save and Publish saves the work file, publishes it, and says which version it made",
            not cmds.file(query=True, modified=True)
            and (version.parent / "v003" / "src" / "kettle.mb").read_bytes() == work.read_bytes()
            and "Published geo v003 of Kettle" in shown[-1][0],
            shown[-1][0].replace("\n", " | "),
        )
        check(
            "the materials it named are the slots it published",
            len(slots) == 3 and shown[-2][0].endswith(f"Materials: {', '.join(slots)}"),
            str(slots),
        )
        answers[:] = ["Cancel"]
        ui.show_publish()
        check(
            "a saved scene is asked nothing about saving: Publish or Cancel",
            shown[-1][1] == ["Publish", "Cancel"] and versions()[-1] == "v003",
        )
        cmds.setAttr("lid.translateY", 1)
        work.chmod(0o444)
        saved = work.read_bytes()
        answers[:] = ["Save and Publish"]
        ui.show_publish()
        check(
            "a save Maya refuses is shown in Maya's words, and publishes nothing",
            "Maya could not save the scene (" in shown[-1][0] and versions()[-1] == "v003",
            shown[-1][0][:90],
        )
        answers[:] = ["Publish Without Saving"]
        ui.show_publish()
        work.chmod(0o644)
        check(
            "and Publish Without Saving then publishes, leaving the work file as it was",
            "Published geo v004 of Kettle" in shown[-1][0]
            and work.read_bytes() == saved
            and cmds.file(query=True, modified=True),
        )
        cmds.select(clear=True)
        ui.show_publish()
        check(
            "a refusal is shown to the artist, and nothing is asked",
            "select the geometry" in shown[-1][0] and shown[-1][1] == ["OK"],
        )

    cmds.file(str(source), open=True, force=True)
    check(
        "its source reopens holding the edit, with the reference still a reference",
        bool(cmds.ls("later_edit"))
        and cmds.referenceQuery("spout:spout", isNodeReferenced=True)
        and [Path(path) for path in cmds.file(query=True, reference=True)] == [spout],
        str(cmds.file(query=True, reference=True)),
    )
    # A published source carries its work's stamp, and is not that work.
    check(
        "a stamped scene that is not at its work path is refused",
        f"not Kettle's modeling work at {work}"
        in refusal(lambda: scene_asset(tracker, production)),
        refusal(lambda: scene_asset(tracker, production))[:70],
    )
    cmds.file(new=True, force=True)
    check(
        "a scene with no stamp is refused",
        "is not host_check work" in refusal(lambda: scene_asset(tracker, production)),
    )


def run_in_host() -> int:
    """Compose Maya's environment for a production and run this file under ``mayapy`` in it."""
    from piper.errors import PiperError
    from piper_studio.launch import compose, maya_executable, maya_variables, working_directory
    from piper_studio.production import load_production
    from piper_studio.profile import Profile

    with tempfile.TemporaryDirectory() as directory:
        path = write_production(Path(directory))
        profile = Profile(name="host_check", production=load_production(path), path=path)
        try:
            mayapy = maya_executable(profile).parent / "mayapy"
        except PiperError as refusal:
            print(f"piper: {refusal}", file=sys.stderr)
            return 1
        if not os.access(mayapy, os.X_OK):
            print(f"piper: {mayapy.name} is not installed at {mayapy}", file=sys.stderr)
            return 1
        return subprocess.call(
            [str(mayapy), str(Path(__file__).resolve())],
            env=compose(os.environ, maya_variables(profile)),
            cwd=working_directory(profile),
        )


def write_production(root: Path) -> Path:
    """A production of this run's own, holding one Maya tool."""
    tools = root / "tools" / "maya"
    (tools / TOOL).mkdir(parents=True)
    (tools / f"{TOOL}.mod").write_text(f"+ {TOOL} 1.0 {tools / TOOL}\n", encoding="utf-8")
    path = root / "production.toml"
    path.write_text(_PRODUCTION.format(root=root), encoding="utf-8")
    return path


if __name__ == "__main__":
    try:
        import maya  # noqa: F401
    except ImportError:
        sys.exit(run_in_host())
    sys.exit(check_host())
