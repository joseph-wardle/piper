"""Composes a host application's environment and launch."""

import os
import re
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import NoReturn

import piper_studio
from piper.errors import PiperError
from piper.tracker import Asset
from piper_studio.context import Context
from piper_studio.production import PRODUCTION_ENV
from piper_studio.profile import Profile

MAYA_VERSION = "2026"
MAYA_USD_VERSION = "0.25.5"


def maya(profile: Profile, work: tuple[Asset, Context] | None = None) -> NoReturn:
    """Launch Maya, working in ``profile``, with Piper's code and menu loaded.

    ``work`` is an asset and the context of its work for Maya to open once it is up.
    """
    _exec(
        maya_executable(profile),
        ["-command", maya_startup_command(work)],
        maya_variables(profile),
        directory=working_directory(profile),
    )


def maya_startup_command(work: tuple[Asset, Context] | None) -> str:
    """The MEL Maya runs once its interface is up: Piper's menu, then ``work`` if any."""
    if work is None:
        return 'python("import piper_maya; piper_maya.start()")'
    asset, context = work
    for token in (asset.id, context.name):
        # Spliced into MEL and Python source, where a quote or backslash would be code.
        if not re.fullmatch(r"[A-Za-z0-9_]+", token):
            raise PiperError(
                f"cannot hand {token!r} to Maya: only letters, digits, and underscores "
                "pass through its command line"
            )
    started = f'piper_maya.start(\\"{asset.id}\\", \\"{context.name}\\")'
    return f'python("import piper_maya; {started}")'


def maya_executable(profile: Profile) -> Path:
    """The Maya binary ``profile`` wants, wherever this machine keeps it."""
    version = _maya_version(profile)
    located = os.environ.get("MAYA_LOCATION")
    install = Path(located) if located else Path(f"/usr/autodesk/maya{version}")
    executable = install / "bin" / f"maya{version}"
    if not os.access(executable, os.X_OK):
        hint = (
            f"point MAYA_LOCATION at a Maya {version} instead"
            if located
            else "set MAYA_LOCATION if Maya lives elsewhere on this machine"
        )
        raise PiperError(
            f"{profile.name} needs Maya {version}, which is not installed at {install}; {hint}"
        )
    return executable


def _maya_version(profile: Profile) -> str:
    production = profile.production
    if production is None or production.software.maya is None:
        return MAYA_VERSION
    return production.software.maya


def maya_variables(profile: Profile) -> dict[str, str | None]:
    """Every variable Maya's environment must set or unset. ``None`` unsets."""
    root = _root(profile)
    tools = root / "tools" / "maya" if root is not None else None
    return {
        "PYTHONPATH": python_path("maya"),
        "MULTI_USD_VERSION": MAYA_USD_VERSION,
        "QT_PLUGIN_PATH": None,
        PRODUCTION_ENV: str(profile.path) if profile.path is not None else None,
        "PXR_AR_DEFAULT_SEARCH_PATH": str(root) if root is not None else None,
        "MAYA_MODULE_PATH": str(tools) if tools is not None and tools.is_dir() else None,
    }


def working_directory(profile: Profile) -> Path | None:
    """Where the host starts, or ``None`` to leave it where the artist was."""
    root = _root(profile)
    if root is None:
        return None
    if not root.is_dir():
        raise PiperError(f"{profile.name} is stored at {root}, which is not a directory")
    return root


def _root(profile: Profile) -> Path | None:
    production = profile.production
    return Path(str(production.root)) if production is not None else None


def compose(inherited: Mapping[str, str], variables: Mapping[str, str | None]) -> dict[str, str]:
    """Apply ``variables`` to a copy of ``inherited``. A ``None`` removes the variable."""
    environment = dict(inherited)
    for name, value in variables.items():
        if value is None:
            environment.pop(name, None)
        else:
            environment[name] = value
    return environment


def python_path(host: str) -> str:
    """Where ``host`` imports Piper from: package sources, then the packages its integration chose.

    The integration's environment holds only what its own manifest names, so
    nothing there can shadow a package the host brings.
    """
    packages = release_root() / "packages"
    names = ("piper-core", "piper-shotgrid", "piper-studio", f"piper-{host}")
    sources = [packages / name / "src" for name in names]
    environment = packages / f"piper-{host}" / ".venv"
    chosen = next(environment.glob("lib/python*/site-packages"), None)
    if chosen is None:
        raise PiperError(
            f"Piper's {host} environment is not built at {environment}; "
            f"run `just sync` in {packages.parent}"
        )
    return os.pathsep.join(str(path) for path in (*sources, chosen))


def release_root() -> Path:
    """The directory Piper's own code and tools are found beneath."""
    source = Path(piper_studio.__file__).resolve()
    for root in source.parents:
        if (root / "packages" / "piper-core" / "src" / "piper").is_dir():
            return root
    raise PiperError(
        f"piper runs from {source.parent}, which is not a release directory; "
        "launching a host needs Piper's package sources, because an install "
        "directory holds packages the host must not import"
    )


def _exec(
    executable: Path,
    arguments: list[str],
    variables: Mapping[str, str | None],
    *,
    directory: Path | None,
) -> NoReturn:
    """Replace this process with ``executable``."""
    environment = compose(os.environ, variables)
    if directory is not None:
        os.chdir(directory)
    # Whatever Piper printed is still buffered when stdout is a pipe, and would be lost.
    sys.stdout.flush()
    os.execve(executable, [str(executable), *arguments], environment)
