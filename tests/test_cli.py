"""Tests for the CLI command surface."""

import os
import shutil
from pathlib import Path

import pytest
import structlog
from typer.testing import CliRunner

from piper.cli import app
from piper.lock import LOCK_FILE

runner = CliRunner()

_FIXTURE_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture(autouse=True)
def _reset_structlog():
    """Isolate each test from structlog state and settings cache."""
    from piper.config import get_settings

    get_settings.cache_clear()
    structlog.reset_defaults()
    structlog.contextvars.clear_contextvars()
    yield
    get_settings.cache_clear()
    structlog.reset_defaults()
    structlog.contextvars.clear_contextvars()


def _ingest_env(tmp_path: Path, settle_seconds: int = 0) -> dict[str, str]:
    """Return env vars that point piper at temp dirs with no raw files."""
    return {
        "PIPER_PATHS__RAW_ROOT": str(tmp_path / "raw"),
        "PIPER_PATHS__DATA_ROOT": str(tmp_path / "data"),
        "PIPER_INGEST__SETTLE_SECONDS": str(settle_seconds),
    }


def _setup_raw_root(tmp_path: Path) -> Path:
    """Copy fixture JSONL files to raw_root with epoch mtimes (always settled)."""
    raw = tmp_path / "raw"
    raw.mkdir(parents=True, exist_ok=True)
    for src in sorted(_FIXTURE_DIR.glob("*.jsonl")):
        dst = raw / src.name
        shutil.copy(src, dst)
        os.utime(dst, (0.0, 0.0))  # mtime = epoch → always settled
    return raw


# ---------------------------------------------------------------------------
# Help and version
# ---------------------------------------------------------------------------


class TestHelp:
    def test_root_help_exits_zero(self):
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0

    def test_root_help_lists_all_commands(self):
        result = runner.invoke(app, ["--help"])
        output = result.output
        for cmd in ("init", "ingest", "backfill", "materialize", "doctor", "config"):
            assert cmd in output, f"command {cmd!r} missing from --help"

    def test_version_flag(self):
        result = runner.invoke(app, ["--version"])
        assert result.exit_code == 0
        assert "piper" in result.output

    @pytest.mark.parametrize(
        "cmd",
        ["init", "ingest", "materialize", "doctor"],
    )
    def test_each_command_has_help(self, cmd):
        result = runner.invoke(app, [cmd, "--help"])
        assert result.exit_code == 0
        assert len(result.output) > 0


# ---------------------------------------------------------------------------
# Commands exit zero (smoke tests with valid temp paths)
# ---------------------------------------------------------------------------


class TestCommandsExitZero:
    def test_init(self, tmp_path):
        result = runner.invoke(
            app,
            ["init"],
            env={
                "PIPER_PATHS__DATA_ROOT": str(tmp_path),
                "PIPER_PATHS__RAW_ROOT": str(tmp_path / "raw"),
            },
        )
        assert result.exit_code == 0, result.output

    def test_ingest_defaults(self, tmp_path):
        result = runner.invoke(app, ["ingest"], env=_ingest_env(tmp_path))
        assert result.exit_code == 0, result.output

    def test_ingest_dry_run(self, tmp_path):
        result = runner.invoke(app, ["ingest", "--dry-run"], env=_ingest_env(tmp_path))
        assert result.exit_code == 0, result.output

    def test_ingest_with_limit(self, tmp_path):
        result = runner.invoke(app, ["ingest", "--limit", "10"], env=_ingest_env(tmp_path))
        assert result.exit_code == 0, result.output

    def test_backfill_with_dates(self):
        result = runner.invoke(app, ["backfill", "--start", "2026-01-01", "--end", "2026-03-01"])
        assert result.exit_code == 0, result.output

    def test_backfill_with_force(self):
        result = runner.invoke(
            app, ["backfill", "--start", "2026-01-01", "--end", "2026-03-01", "--force"]
        )
        assert result.exit_code == 0, result.output

    def test_materialize_defaults(self, tmp_path):
        result = runner.invoke(
            app, ["materialize"],
            env={
                "PIPER_PATHS__DATA_ROOT": str(tmp_path),
                "PIPER_PATHS__RAW_ROOT": str(tmp_path / "raw"),
            },
        )
        assert result.exit_code == 0, result.output

    def test_materialize_named_model(self, tmp_path):
        result = runner.invoke(
            app,
            ["materialize", "--model", "gold_publish_health_daily"],
            env={
                "PIPER_PATHS__DATA_ROOT": str(tmp_path),
                "PIPER_PATHS__RAW_ROOT": str(tmp_path / "raw"),
            },
        )
        assert result.exit_code == 0, result.output

    def test_doctor_defaults(self, tmp_path):
        # An empty warehouse will fail freshness/volume checks (exit 2 is correct).
        result = runner.invoke(
            app, ["doctor"],
            env={
                "PIPER_PATHS__DATA_ROOT": str(tmp_path),
                "PIPER_PATHS__RAW_ROOT": str(tmp_path / "raw"),
            },
        )
        assert result.exit_code in (0, 1, 2), result.output

    def test_doctor_named_check(self, tmp_path):
        result = runner.invoke(
            app, ["doctor", "--check", "freshness"],
            env={
                "PIPER_PATHS__DATA_ROOT": str(tmp_path),
                "PIPER_PATHS__RAW_ROOT": str(tmp_path / "raw"),
            },
        )
        assert result.exit_code in (0, 1, 2), result.output

    def test_config_show(self):
        result = runner.invoke(app, ["config", "show"])
        assert result.exit_code == 0, result.output


class TestBackfillRequiredOptions:
    def test_backfill_missing_start_fails(self):
        result = runner.invoke(app, ["backfill", "--end", "2026-03-01"])
        assert result.exit_code != 0

    def test_backfill_missing_end_fails(self):
        result = runner.invoke(app, ["backfill", "--start", "2026-01-01"])
        assert result.exit_code != 0

    def test_backfill_missing_both_fails(self):
        result = runner.invoke(app, ["backfill"])
        assert result.exit_code != 0


# ---------------------------------------------------------------------------
# Ingest command — end-to-end behaviour
# ---------------------------------------------------------------------------


class TestIngestCommand:
    def test_full_run_on_fixture_data_prints_row_counts(self, tmp_path):
        """Full ingest run on fixture data completes and prints row counts."""
        _setup_raw_root(tmp_path)
        result = runner.invoke(app, ["ingest"], env=_ingest_env(tmp_path))
        assert result.exit_code == 0, result.output
        # 5 fixture files with 54 total events, all unique event IDs
        assert "54" in result.output
        assert "rows accepted" in result.output

    def test_full_run_shows_ingest_complete(self, tmp_path):
        _setup_raw_root(tmp_path)
        result = runner.invoke(app, ["ingest"], env=_ingest_env(tmp_path))
        assert "Ingest complete" in result.output

    def test_full_run_shows_five_files_processed(self, tmp_path):
        _setup_raw_root(tmp_path)
        result = runner.invoke(app, ["ingest"], env=_ingest_env(tmp_path))
        assert "5 file(s)" in result.output

    def test_empty_raw_root_exits_zero(self, tmp_path):
        result = runner.invoke(app, ["ingest"], env=_ingest_env(tmp_path))
        assert result.exit_code == 0, result.output
        assert "0 file(s)" in result.output

    def test_raw_root_missing_exits_zero(self, tmp_path):
        """Nonexistent raw_root → discover returns [] → graceful empty run."""
        env = _ingest_env(tmp_path)
        env["PIPER_PATHS__RAW_ROOT"] = str(tmp_path / "nonexistent")
        result = runner.invoke(app, ["ingest"], env=env)
        assert result.exit_code == 0, result.output

    def test_second_run_skips_already_ingested(self, tmp_path):
        """A file ingested in run 1 is skipped in run 2."""
        _setup_raw_root(tmp_path)
        env = _ingest_env(tmp_path)
        runner.invoke(app, ["ingest"], env=env)  # run 1

        result = runner.invoke(app, ["ingest"], env=env)  # run 2
        assert result.exit_code == 0, result.output
        assert "0 file(s)" in result.output

    def test_dry_run_prints_would_be_ingested(self, tmp_path):
        _setup_raw_root(tmp_path)
        result = runner.invoke(app, ["ingest", "--dry-run"], env=_ingest_env(tmp_path))
        assert result.exit_code == 0, result.output
        assert "Dry run" in result.output
        assert "5 file(s)" in result.output

    def test_dry_run_does_not_write_to_db(self, tmp_path):
        """Dry run must leave silver_events empty."""
        import duckdb

        from piper.sql_runner import apply_pending_migrations

        _setup_raw_root(tmp_path)
        runner.invoke(app, ["ingest", "--dry-run"], env=_ingest_env(tmp_path))

        db = tmp_path / "data" / "warehouse" / "telemetry.duckdb"
        conn = duckdb.connect(str(db))
        apply_pending_migrations(
            conn, Path(__file__).parent.parent / "src" / "piper" / "sql" / "schema"
        )
        count = conn.execute("SELECT COUNT(*) FROM silver_events").fetchone()
        conn.close()
        assert count is not None
        assert count[0] == 0

    def test_limit_restricts_files_processed(self, tmp_path):
        """--limit 2 processes at most 2 of the 5 fixture files."""
        _setup_raw_root(tmp_path)
        result = runner.invoke(app, ["ingest", "--limit", "2"], env=_ingest_env(tmp_path))
        assert result.exit_code == 0, result.output
        assert "2 file(s)" in result.output

    def test_already_running_exits_nonzero(self, tmp_path):
        """A live lock file causes exit code 1."""
        env = _ingest_env(tmp_path)
        # Bootstrap dirs so state_dir exists
        runner.invoke(
            app,
            ["init"],
            env={
                "PIPER_PATHS__DATA_ROOT": str(tmp_path / "data"),
                "PIPER_PATHS__RAW_ROOT": str(tmp_path / "raw"),
            },
        )
        # Write lock file with the current (live) PID
        state_dir = tmp_path / "data" / "state"
        (state_dir / LOCK_FILE).write_text(str(os.getpid()))

        result = runner.invoke(app, ["ingest"], env=env)
        assert result.exit_code == 1, result.output
