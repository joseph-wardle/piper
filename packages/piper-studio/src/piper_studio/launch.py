"""Composes a host application's environment and launch."""

import os
from collections.abc import Mapping
from pathlib import Path
from typing import NoReturn

import piper_studio
from piper.errors import PiperError
from piper_studio.production import PRODUCTION_ENV
from piper_studio.profile import Profile

MAYA_VERSION = "2026"
MAYA_USD_VERSION = "0.25.5"


def maya(profile: Profile) -> NoReturn:
    """Launch Maya, working in ``profile``, with Piper's code loaded."""
    _exec(maya_executable(profile), maya_variables(profile), directory=working_directory(profile))


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
        "PYTHONPATH": python_path(),
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


def python_path() -> str:
    """The package source directories a host imports Piper from."""
    root = release_root()
    packages = ("piper-core", "piper-studio")
    return os.pathsep.join(str(root / "packages" / name / "src") for name in packages)


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
    executable: Path, variables: Mapping[str, str | None], *, directory: Path | None
) -> NoReturn:
    """Replace this process with ``executable``."""
    environment = compose(os.environ, variables)
    if directory is not None:
        os.chdir(directory)
    os.execve(executable, [str(executable)], environment)
