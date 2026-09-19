"""Checks Piper's composed environment, and opening work, inside a real Maya."""

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
