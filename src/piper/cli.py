"""CLI root — subcommands are added progressively across commits.

Entry point: `uv run piper` or `python -m piper`.
"""

import typer

from piper import __version__

app = typer.Typer(
    name="piper",
    help="Diagnostics dashboard for the sandwich USD production pipeline.",
    no_args_is_help=True,
)


def _version_callback(value: bool) -> None:
    if value:
        typer.echo(f"piper {__version__}")
        raise typer.Exit()


@app.callback()
def _main(
    version: bool = typer.Option(
        False,
        "--version",
        "-V",
        callback=_version_callback,
        is_eager=True,
        help="Print version and exit.",
    ),
) -> None:
    """Diagnostics dashboard for the sandwich USD production pipeline."""
