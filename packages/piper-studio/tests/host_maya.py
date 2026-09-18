"""Checks Piper's composed environment inside a real Maya."""

import os
import subprocess
import sys
import tempfile
from pathlib import Path

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

    print(f"\n{len(failures)} failed" if failures else "\nall passed")
    return 1 if failures else 0


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
