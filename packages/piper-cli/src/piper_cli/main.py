"""Entry point for the ``piper`` command."""

import argparse

import piper


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="piper", description=piper.__doc__)
    parser.add_argument(
        "--version",
        action="version",
        version=f"piper {piper.__version__}",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    parser.parse_args(argv)
    parser.print_help()
    return 0
