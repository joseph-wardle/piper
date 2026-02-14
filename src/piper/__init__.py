from __future__ import annotations

from piper.cli import main as _main

__all__ = ["main"]


def main() -> None:
    raise SystemExit(_main())
