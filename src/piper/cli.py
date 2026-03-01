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
    # Eager options (--version) raise typer.Exit() before this body runs,
    # so configure_logging() is only called for real subcommands.
    from piper.logging import configure_logging

    configure_logging()


# ---------------------------------------------------------------------------
# config subcommands
# ---------------------------------------------------------------------------

_config_app = typer.Typer(help="Inspect resolved configuration.")
app.add_typer(_config_app, name="config")


@_config_app.command("show")
def config_show() -> None:
    """Print the fully-resolved configuration and exit.

    Shows which config file was loaded and the final value of every setting
    after environment-variable overrides are applied.  Useful for confirming
    that PIPER_* overrides are being picked up correctly.
    """
    from piper.config import _config_file, get_settings

    settings = get_settings()

    typer.echo(f"\n  config : {_config_file()}\n")

    for section_name, section in settings.model_dump().items():
        typer.echo(f"  [{section_name}]")
        width = max(len(k) for k in section)
        for key, val in section.items():
            typer.echo(f"  {key.ljust(width)} = {val}")
        typer.echo()
