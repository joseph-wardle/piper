"""CLI root — entry point for all piper subcommands.

Entry points:
  uv run piper          (recommended)
  python -m piper

Command surface:
  piper init            initialise warehouse and directory layout
  piper ingest          discover and ingest new telemetry files
  piper backfill        re-ingest a historical date range
  piper materialize     rebuild silver and gold SQL models
  piper doctor          check data freshness and quality
  piper config show     print resolved configuration
"""

import typer

from piper import __version__
from piper.logging import get_logger

app = typer.Typer(
    name="piper",
    help="Diagnostics dashboard for the sandwich USD production pipeline.",
    no_args_is_help=True,
)

_log = get_logger(__name__)


# ---------------------------------------------------------------------------
# Global callback — runs before every subcommand
# ---------------------------------------------------------------------------


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
# init
# ---------------------------------------------------------------------------


@app.command("init")
def init() -> None:
    """Initialise the warehouse and output directory layout.

    Creates all managed output directories (warehouse, silver, state,
    quarantine, run_logs) and runs the DuckDB schema migration to the
    latest version.  Safe to re-run: migration is idempotent.
    """
    _log.info("init started")


# ---------------------------------------------------------------------------
# ingest
# ---------------------------------------------------------------------------


@app.command("ingest")
def ingest(
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Discover and validate files but do not write anything.",
    ),
    limit: int = typer.Option(
        0,
        "--limit",
        help="Maximum number of files to process per run.  0 = no limit.",
    ),
) -> None:
    """Discover and ingest new telemetry JSONL files.

    Scans raw_root for JSONL files that are stable (mtime older than
    settle_seconds), skips files already tracked in the ingest manifest,
    parses and validates each line, and upserts new events into
    silver_events.  Quarantines malformed or contract-invalid lines.
    """
    _log.info("ingest started", dry_run=dry_run, limit=limit or "unlimited")


# ---------------------------------------------------------------------------
# backfill
# ---------------------------------------------------------------------------


@app.command("backfill")
def backfill(
    start: str = typer.Option(
        ...,
        "--start",
        "-s",
        help="Inclusive start date (YYYY-MM-DD).",
    ),
    end: str = typer.Option(
        ...,
        "--end",
        "-e",
        help="Inclusive end date (YYYY-MM-DD).",
    ),
    force: bool = typer.Option(
        False,
        "--force",
        help="Re-ingest files already recorded in the ingest manifest.",
    ),
) -> None:
    """Re-ingest telemetry for a specific date range.

    Forces re-ingestion of all raw JSONL files whose mtime falls within
    the given window, ignoring the ingest manifest.  Use this after
    recovering from a pipeline gap or correcting a data quality issue.
    """
    _log.info("backfill started", start=start, end=end, force=force)


# ---------------------------------------------------------------------------
# materialize
# ---------------------------------------------------------------------------


@app.command("materialize")
def materialize(
    model: str = typer.Option(
        "",
        "--model",
        "-m",
        help="Rebuild only this named silver or gold model.  Empty = all.",
    ),
) -> None:
    """Rebuild silver and gold SQL models from silver_events.

    Executes silver domain SQL models first (publish, tool, farm, render,
    storage), then gold KPI models in dependency order.  All models are
    CREATE OR REPLACE, so this is safe to run at any time.
    """
    _log.info("materialize started", model=model or "all")


# ---------------------------------------------------------------------------
# doctor
# ---------------------------------------------------------------------------


@app.command("doctor")
def doctor(
    check: str = typer.Option(
        "",
        "--check",
        "-c",
        help="Run only this named check.  Empty = run all checks.",
    ),
) -> None:
    """Check data freshness, quality, and pipeline health.

    Runs assertions against the warehouse and prints a pass / warn / fail
    status for each.  Checks include: event-volume floor, per-domain
    freshness windows, invalid-line rate, and clock-skew anomalies.

    Exit codes: 0 = all pass, 1 = one or more warnings, 2 = one or more failures.
    """
    _log.info("doctor started", check=check or "all")


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
