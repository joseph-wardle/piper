from __future__ import annotations

from pathlib import Path


class PiperError(Exception):
    """Base exception for user-facing piper failures."""


class ConfigError(PiperError):
    """Raised for malformed configuration."""


class ShowResolutionError(PiperError):
    """Raised when no show can be resolved for the current invocation."""


class PathResolutionError(PiperError):
    """Raised when a goto/path target cannot be resolved."""

    def __init__(self, kind: str, identifier: str, candidates: list[Path]) -> None:
        attempted = ", ".join(str(path) for path in candidates)
        super().__init__(
            f"unable to resolve '{kind} {identifier}'. Tried: {attempted or '<none>'}"
        )
        self.kind = kind
        self.identifier = identifier
        self.candidates = tuple(candidates)


class ScriptResolutionError(PiperError):
    """Raised when a run script cannot be validated or found."""
