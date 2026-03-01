"""Tests for the CLI command surface."""

import pytest
import structlog
from typer.testing import CliRunner

from piper.cli import app

runner = CliRunner()


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


class TestCommandsExitZero:
    """Each stub must run without error and exit 0."""

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

    def test_ingest_defaults(self):
        result = runner.invoke(app, ["ingest"])
        assert result.exit_code == 0, result.output

    def test_ingest_dry_run(self):
        result = runner.invoke(app, ["ingest", "--dry-run"])
        assert result.exit_code == 0, result.output

    def test_ingest_with_limit(self):
        result = runner.invoke(app, ["ingest", "--limit", "10"])
        assert result.exit_code == 0, result.output

    def test_backfill_with_dates(self):
        result = runner.invoke(app, ["backfill", "--start", "2026-01-01", "--end", "2026-03-01"])
        assert result.exit_code == 0, result.output

    def test_backfill_with_force(self):
        result = runner.invoke(
            app, ["backfill", "--start", "2026-01-01", "--end", "2026-03-01", "--force"]
        )
        assert result.exit_code == 0, result.output

    def test_materialize_defaults(self):
        result = runner.invoke(app, ["materialize"])
        assert result.exit_code == 0, result.output

    def test_materialize_named_model(self):
        result = runner.invoke(app, ["materialize", "--model", "gold_publish_health_daily"])
        assert result.exit_code == 0, result.output

    def test_doctor_defaults(self):
        result = runner.invoke(app, ["doctor"])
        assert result.exit_code == 0, result.output

    def test_doctor_named_check(self):
        result = runner.invoke(app, ["doctor", "--check", "freshness"])
        assert result.exit_code == 0, result.output

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
