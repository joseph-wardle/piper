from __future__ import annotations

from argparse import ArgumentParser, Namespace
from pathlib import Path
from typing import Mapping

from piper.errors import PiperError
from piper.models import ResolvedContext
from piper.resolvers import resolve_existing_path


class PathCommand:
    name = "path"
    help = "Resolve and print a path for a kind/id pair."
    requires_context = True

    def configure(self, parser: ArgumentParser) -> None:
        parser.add_argument("kind", help="Configured path kind, e.g. shot, asset, environment")
        parser.add_argument("id", help="Identifier value for the selected kind")

    def run(
        self,
        args: Namespace,
        *,
        context: ResolvedContext | None,
        environ: Mapping[str, str],
        cwd: Path,
    ) -> int:
        del environ, cwd

        if context is None:
            raise PiperError("path requires a resolved show context")

        path = resolve_existing_path(context.show, args.kind, args.id)
        print(path)
        return 0
