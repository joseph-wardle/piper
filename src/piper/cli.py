from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Mapping, Sequence

from piper.commands import Command, built_in_commands
from piper.config import resolve_context
from piper.errors import PiperError
from piper.models import ResolvedContext


def build_parser(commands: Sequence[Command]) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="piper")
    parser.add_argument("--show", help="Override show name")

    subparsers = parser.add_subparsers(dest="command")

    for command in commands:
        subparser = subparsers.add_parser(command.name, help=command.help)
        command.configure(subparser)
        subparser.set_defaults(command_obj=command)

    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    environ: Mapping[str, str] | None = None,
    cwd: Path | None = None,
) -> int:
    commands = built_in_commands()
    parser = build_parser(commands)

    args_input = list(sys.argv[1:] if argv is None else argv)

    try:
        parsed = parser.parse_args(args_input)
    except SystemExit as exc:
        if isinstance(exc.code, int):
            return exc.code
        if exc.code is None:
            return 0
        return 1

    command: Command | None = getattr(parsed, "command_obj", None)
    if command is None:
        parser.print_help()
        return 0

    runtime_env = os.environ if environ is None else environ
    runtime_cwd = Path.cwd() if cwd is None else cwd

    try:
        resolved_context: ResolvedContext | None = None
        if command.requires_context:
            resolved_context = resolve_context(
                parsed.show,
                cwd=runtime_cwd,
                environ=runtime_env,
            )

        return int(
            command.run(
                parsed,
                context=resolved_context,
                environ=runtime_env,
                cwd=runtime_cwd,
            )
        )
    except PiperError as exc:
        print(f"piper: {exc}", file=sys.stderr)
        return 1
