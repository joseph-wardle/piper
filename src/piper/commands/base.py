from __future__ import annotations

from argparse import ArgumentParser, Namespace
from pathlib import Path
from typing import Mapping, Protocol

from piper.models import ResolvedContext


class Command(Protocol):
    name: str
    help: str
    requires_context: bool

    def configure(self, parser: ArgumentParser) -> None:
        ...

    def run(
        self,
        args: Namespace,
        *,
        context: ResolvedContext | None,
        environ: Mapping[str, str],
        cwd: Path,
    ) -> int:
        ...
