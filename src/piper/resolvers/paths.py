from __future__ import annotations

import glob
from pathlib import Path

from piper.errors import PathResolutionError, PiperError
from piper.models import ShowConfig


class UnknownKindError(PiperError):
    """Raised when an unknown path kind is requested."""


def _expand_template_paths(
    template: str, *, show_root: Path, identifier: str
) -> list[Path]:
    rendered = template.format(root=str(show_root), id=identifier)
    rendered_path = Path(rendered).expanduser()
    if not rendered_path.is_absolute():
        rendered_path = show_root / rendered_path

    rendered_text = str(rendered_path)
    if not glob.has_magic(rendered_text):
        return [rendered_path]

    # Expand wildcard candidates in deterministic order so resolution is stable.
    matches = [Path(match).resolve() for match in sorted(glob.glob(rendered_text))]
    if matches:
        return matches

    # Keep the unresolved pattern for meaningful error reporting.
    return [rendered_path]


def build_path_candidates(show: ShowConfig, kind: str, identifier: str) -> list[Path]:
    templates = show.goto_templates.get(kind)
    if not templates:
        known_kinds = ", ".join(sorted(show.goto_templates))
        raise UnknownKindError(f"unknown kind '{kind}'. Known kinds: {known_kinds}")

    candidates: list[Path] = []
    seen: set[Path] = set()
    for template in templates:
        for candidate in _expand_template_paths(
            template,
            show_root=show.root,
            identifier=identifier,
        ):
            if candidate in seen:
                continue
            seen.add(candidate)
            candidates.append(candidate)

    return candidates


def resolve_existing_path(show: ShowConfig, kind: str, identifier: str) -> Path:
    candidates = build_path_candidates(show, kind, identifier)

    for candidate in candidates:
        if candidate.exists():
            return candidate

    raise PathResolutionError(kind, identifier, candidates)
