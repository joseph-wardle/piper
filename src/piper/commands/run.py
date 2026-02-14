from __future__ import annotations

import argparse
import subprocess
import sys
from argparse import ArgumentParser, Namespace
from pathlib import Path
from typing import Mapping

from piper.errors import PiperError
from piper.models import ResolvedContext
from piper.resolvers import list_available_scripts, resolve_script_path


class RunCommand:
    name = "run"
    help = "Run configured scripts in the current working directory."
    requires_context = True

    def configure(self, parser: ArgumentParser) -> None:
        parser.add_argument(
            "--list",
            action="store_true",
            dest="list_scripts",
            help="List discovered scripts and exit",
        )
        parser.add_argument("script", nargs="?", help="Script logical name")
        parser.add_argument(
            "script_args",
            nargs=argparse.REMAINDER,
            help="Arguments forwarded to the target script",
        )

    def run(
        self,
        args: Namespace,
        *,
        context: ResolvedContext | None,
        environ: Mapping[str, str],
        cwd: Path,
    ) -> int:
        del environ

        if context is None:
            raise PiperError("run requires a resolved show context")

        if args.list_scripts:
            if args.script:
                raise PiperError("--list cannot be used with a script name")

            for name in list_available_scripts(context.show):
                print(name)
            return 0

        if not args.script:
            raise PiperError("script name is required unless --list is provided")

        forwarded_args = list(args.script_args)
        if forwarded_args and forwarded_args[0] == "--":
            forwarded_args = forwarded_args[1:]

        script_path = resolve_script_path(context.show, args.script)

        process = subprocess.run(
            [sys.executable, str(script_path), *forwarded_args],
            cwd=str(cwd),
            check=False,
        )
        return int(process.returncode)
