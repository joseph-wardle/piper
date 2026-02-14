from __future__ import annotations

from pathlib import Path

from piper.errors import ScriptResolutionError
from piper.models import ShowConfig


def validate_script_name(script_name: str) -> None:
    if not script_name:
        raise ScriptResolutionError("script name is required")

    if script_name in {".", ".."}:
        raise ScriptResolutionError("script name cannot be '.' or '..'")

    if "/" in script_name or "\\" in script_name:
        raise ScriptResolutionError("script name must not include path separators")


def resolve_script_path(show: ShowConfig, script_name: str) -> Path:
    validate_script_name(script_name)

    for script_dir in show.script_dirs:
        candidates = (script_dir / script_name, script_dir / f"{script_name}.py")
        for candidate in candidates:
            if candidate.is_file():
                return candidate

    searched = ", ".join(str(path) for path in show.script_dirs) or "<none>"
    raise ScriptResolutionError(f"script '{script_name}' not found in: {searched}")


def list_available_scripts(show: ShowConfig) -> list[str]:
    names: set[str] = set()

    for script_dir in show.script_dirs:
        if not script_dir.is_dir():
            continue

        for child in script_dir.iterdir():
            if not child.is_file() or child.name.startswith("."):
                continue

            if child.suffix == ".py":
                if child.stem:
                    names.add(child.stem)
                continue

            if child.suffix:
                continue

            names.add(child.name)

    return sorted(names)
