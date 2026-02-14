from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ShowConfig:
    """Resolved show-level runtime configuration for a command invocation."""

    name: str
    root: Path
    goto_templates: dict[str, tuple[str, ...]]
    script_dirs: tuple[Path, ...]
    user_config_path: Path
    show_config_path: Path


@dataclass(frozen=True)
class ResolvedContext:
    """Context shared across commands that require an active show."""

    show: ShowConfig
    cwd: Path
