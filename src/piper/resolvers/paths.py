from __future__ import annotations

from pathlib import Path

from piper.errors import PathResolutionError, PiperError
from piper.models import ShowConfig


class UnknownKindError(PiperError):
    """Raised when an unknown path kind is requested."""


def build_path_candidates(show: ShowConfig, kind: str, identifier: str) -> list[Path]:
    templates = show.goto_templates.get(kind)
    if not templates:
        known_kinds = ", ".join(sorted(show.goto_templates))
        raise UnknownKindError(f"unknown kind '{kind}'. Known kinds: {known_kinds}")

    candidates: list[Path] = []
    for template in templates:
        rendered = template.format(root=str(show.root), id=identifier)
        candidate = Path(rendered).expanduser()
        if not candidate.is_absolute():
            candidate = show.root / candidate
        candidates.append(candidate)

    return candidates


def resolve_existing_path(show: ShowConfig, kind: str, identifier: str) -> Path:
    candidates = build_path_candidates(show, kind, identifier)

    for candidate in candidates:
        if candidate.exists():
            return candidate

    raise PathResolutionError(kind, identifier, candidates)
