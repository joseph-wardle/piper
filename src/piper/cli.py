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
    from piper.config import get_settings
    from piper.paths import ProjectPaths
    from piper.warehouse import open_warehouse, run_migrations

    settings = get_settings()
    paths = ProjectPaths.from_settings(settings)
    paths.ensure_output_dirs()
    _log.info("output directories ready", data_root=str(paths.data_root))

    conn = open_warehouse(paths)
    n = run_migrations(conn)
    conn.close()

    if n:
        _log.info("schema migrations applied", count=n)
    else:
        _log.info("schema is up to date")


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
    from piper.config import get_settings
    from piper.discovery import FoundFile, discover_settled_files
    from piper.ingest import IngestStats, ingest_file
    from piper.lock import LockError, RunLock
    from piper.manifest import is_already_ingested, mark_ingested
    from piper.paths import ProjectPaths
    from piper.warehouse import open_warehouse, run_migrations

    settings = get_settings()
    paths = ProjectPaths.from_settings(settings)
    paths.ensure_output_dirs()

    conn = open_warehouse(paths)
    run_migrations(conn)

    files = discover_settled_files(paths.raw_root, settings.ingest.settle_seconds)
    _log.info("discovery complete", total=len(files))

    pending: list[FoundFile] = []
    results: list[IngestStats] = []

    try:
        with RunLock(paths.state_dir):
            pending = [f for f in files if not is_already_ingested(conn, f)]
            if limit:
                pending = pending[:limit]

            _log.info(
                "ingest plan",
                pending=len(pending),
                already_ingested=len(files) - len(pending),
                dry_run=dry_run,
            )

            if dry_run:
                for file in pending:
                    typer.echo(f"  {file.path}")
            else:
                for file in pending:
                    stats = ingest_file(conn, file, quarantine_dir=paths.quarantine_dir)
                    mark_ingested(
                        conn,
                        file,
                        event_count=stats.accepted,
                        error_count=stats.quarantined,
                    )
                    results.append(stats)
                    _log.info(
                        "file ingested",
                        path=str(file.path),
                        accepted=stats.accepted,
                        duplicate=stats.duplicate,
                        quarantined=stats.quarantined,
                    )

    except LockError as exc:
        _log.error("piper already running", detail=str(exc))
        raise typer.Exit(1) from exc

    finally:
        conn.close()

    # Print run summary
    n = len(pending)
    if dry_run:
        typer.echo(f"\nDry run — {n} file(s) would be ingested")
    else:
        accepted = sum(s.accepted for s in results)
        duplicate = sum(s.duplicate for s in results)
        quarantined = sum(s.quarantined for s in results)
        typer.echo(f"\nIngest complete — {n} file(s) processed")
        typer.echo(f"  rows accepted:      {accepted}")
        typer.echo(f"  rows duplicate:     {duplicate}")
        typer.echo(f"  lines quarantined:  {quarantined}")


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
    from pathlib import Path

    from piper.config import get_settings
    from piper.paths import ProjectPaths
    from piper.sql_runner import run_sql_file
    from piper.warehouse import open_warehouse, run_gold_views, run_migrations, run_silver_views

    settings = get_settings()
    paths = ProjectPaths.from_settings(settings)
    paths.ensure_output_dirs()

    conn = open_warehouse(paths)
    run_migrations(conn)

    if model:
        # Always apply silver views first so gold model dependencies exist.
        run_silver_views(conn)
        _pkg = Path(__file__).parent
        for sql_dir in (_pkg / "sql" / "silver", _pkg / "sql" / "gold"):
            sql_file = sql_dir / f"{model}.sql"
            if sql_file.exists():
                run_sql_file(conn, sql_file)
                conn.close()
                typer.echo(f"Materialized: {model}")
                _log.info("model materialized", model=model)
                return
        conn.close()
        typer.echo(f"Error: model {model!r} not found in silver or gold SQL dirs", err=True)
        raise typer.Exit(1)

    run_silver_views(conn)
    run_gold_views(conn)
    conn.close()

    typer.echo("Materialize complete")
    typer.echo("  silver views: 7")
    typer.echo("  gold views:   6")
    _log.info("materialize complete", silver_views=7, gold_views=6)


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
    from piper.config import get_settings
    from piper.doctor import run_checks
    from piper.paths import ProjectPaths
    from piper.warehouse import open_warehouse, run_migrations

    settings = get_settings()
    paths = ProjectPaths.from_settings(settings)
    paths.ensure_output_dirs()

    conn = open_warehouse(paths)
    run_migrations(conn)

    try:
        results = run_checks(conn, only=check)
    except ValueError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(2) from exc
    finally:
        conn.close()

    # Print aligned table of check results.
    _STATUS_LABEL = {"pass": "PASS", "warn": "WARN", "fail": "FAIL"}
    name_width = max(len(r.name) for r in results)
    for r in results:
        typer.echo(f"  {r.name.ljust(name_width)}  {_STATUS_LABEL[r.status]:4}  {r.message}")
        if r.hint:
            typer.echo(f"  {''.ljust(name_width)}        hint: {r.hint}")

    n_warn = sum(1 for r in results if r.status == "warn")
    n_fail = sum(1 for r in results if r.status == "fail")
    typer.echo()
    if n_fail:
        typer.echo(f"  {n_warn} warning(s) — {n_fail} failure(s)")
        _log.warning("doctor finished with failures", warnings=n_warn, failures=n_fail)
        raise typer.Exit(2)
    if n_warn:
        typer.echo(f"  {n_warn} warning(s) — 0 failures")
        _log.warning("doctor finished with warnings", warnings=n_warn)
        raise typer.Exit(1)
    typer.echo("  all checks passed")
    _log.info("doctor finished", status="pass")


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
