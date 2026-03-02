"""Integration tests — raw JSONL files to populated silver_events.

These tests run the full ingest pipeline against real temp-dir paths using
the CLI, then query the resulting DuckDB warehouse to verify data integrity.
They complement the unit tests in tests/test_cli.py (which check exit codes
and stdout) by asserting that the *right data ended up in the right place*.

Fixture setup (module-scoped, runs once):
  1. Copy all tests/fixtures/*.jsonl into a temp raw_root with epoch mtimes.
  2. ``piper ingest`` via the CLI runner.
  3. Open the resulting warehouse for read-only queries.
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path

import duckdb
import pytest
import structlog
from typer.testing import CliRunner

from piper.cli import app
from piper.config import get_settings

_FIXTURE_DIR = Path(__file__).parent.parent / "fixtures"
_SCHEMA_DIR = Path(__file__).parent.parent.parent / "src" / "piper" / "sql" / "schema"

_runner = CliRunner()

# All 18 event types in the fixture files.
_ALL_EVENT_TYPES = {
    "publish.asset.usd",
    "publish.anim.usd",
    "publish.camera.usd",
    "publish.customanim.usd",
    "publish.previs_asset.usd",
    "dcc.launch",
    "file.open",
    "file.create",
    "shot.setup",
    "playblast.create",
    "build.houdini.component",
    "texture.export.substance",
    "texture.convert.tex",
    "tractor.job.spool",
    "tractor.farm.snapshot",
    "render.stats.summary",
    "storage.scan.summary",
    "storage.scan.bucket",
}


# ---------------------------------------------------------------------------
# Module-scoped pipeline fixture
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def ingest_env(tmp_path_factory):
    """Run piper ingest once against all fixture files; yield the env vars."""
    root = tmp_path_factory.mktemp("ingest_e2e")
    raw = root / "raw"
    raw.mkdir()

    # Copy fixtures with epoch mtime so they are always settled.
    for src in sorted(_FIXTURE_DIR.glob("*.jsonl")):
        dst = raw / src.name
        shutil.copy(src, dst)
        os.utime(dst, (0.0, 0.0))

    env = {
        "PIPER_PATHS__RAW_ROOT": str(raw),
        "PIPER_PATHS__DATA_ROOT": str(root / "data"),
        "PIPER_INGEST__SETTLE_SECONDS": "0",
    }

    get_settings.cache_clear()
    structlog.reset_defaults()
    result = _runner.invoke(app, ["ingest"], env=env)
    assert result.exit_code == 0, f"ingest failed:\n{result.output}"

    yield env, root

    get_settings.cache_clear()


@pytest.fixture(scope="module")
def warehouse(ingest_env):
    """Open the warehouse written by the ingest fixture for read-only queries."""
    env, root = ingest_env
    db_path = root / "data" / "warehouse" / "telemetry.duckdb"
    conn = duckdb.connect(str(db_path))
    yield conn
    conn.close()


# ---------------------------------------------------------------------------
# silver_events content
# ---------------------------------------------------------------------------


class TestSilverEventsContent:
    def test_all_54_events_ingested(self, warehouse):
        row = warehouse.execute("SELECT COUNT(*) FROM silver_events").fetchone()
        assert row is not None
        assert row[0] == 54

    def test_all_18_event_types_present(self, warehouse):
        rows = warehouse.execute("SELECT DISTINCT event_type FROM silver_events").fetchall()
        found = {r[0] for r in rows}
        assert found == _ALL_EVENT_TYPES

    def test_no_null_event_ids(self, warehouse):
        row = warehouse.execute(
            "SELECT COUNT(*) FROM silver_events WHERE event_id IS NULL"
        ).fetchone()
        assert row is not None
        assert row[0] == 0

    def test_no_null_occurred_at(self, warehouse):
        row = warehouse.execute(
            "SELECT COUNT(*) FROM silver_events WHERE occurred_at_utc IS NULL"
        ).fetchone()
        assert row is not None
        assert row[0] == 0

    def test_all_event_ids_are_unique(self, warehouse):
        total = warehouse.execute("SELECT COUNT(*) FROM silver_events").fetchone()
        unique = warehouse.execute("SELECT COUNT(DISTINCT event_id) FROM silver_events").fetchone()
        assert total is not None and unique is not None
        assert total[0] == unique[0]



# ---------------------------------------------------------------------------
# Ingest manifest
# ---------------------------------------------------------------------------


class TestIngestManifest:
    def test_five_files_recorded(self, warehouse):
        row = warehouse.execute("SELECT COUNT(*) FROM ingest_manifest").fetchone()
        assert row is not None
        assert row[0] == 5

    def test_total_events_matches_silver(self, warehouse):
        manifest_total = warehouse.execute(
            "SELECT SUM(event_count) FROM ingest_manifest"
        ).fetchone()
        silver_total = warehouse.execute("SELECT COUNT(*) FROM silver_events").fetchone()
        assert manifest_total is not None and silver_total is not None
        assert manifest_total[0] == silver_total[0]

    def test_no_manifest_errors_for_valid_fixtures(self, warehouse):
        """All fixture events are valid; error_count should be 0 for every file."""
        row = warehouse.execute("SELECT SUM(error_count) FROM ingest_manifest").fetchone()
        assert row is not None
        assert row[0] == 0


# ---------------------------------------------------------------------------
# Quarantine
# ---------------------------------------------------------------------------


class TestQuarantine:
    def test_no_quarantine_files_for_valid_fixtures(self, ingest_env):
        """All fixture JSONL lines are valid; nothing should be quarantined."""
        _, root = ingest_env
        quarantine_dir = root / "data" / "quarantine"
        quarantined = list(quarantine_dir.rglob("*.jsonl")) if quarantine_dir.exists() else []
        assert quarantined == [], f"unexpected quarantine files: {quarantined}"


# ---------------------------------------------------------------------------
# Second-run idempotency
# ---------------------------------------------------------------------------


class TestSecondRunIdempotency:
    def test_second_ingest_skips_all_files(self, ingest_env):
        """Re-running ingest on an already-ingested raw_root processes 0 files."""
        env, _ = ingest_env
        get_settings.cache_clear()
        structlog.reset_defaults()
        result = _runner.invoke(app, ["ingest"], env=env)
        assert result.exit_code == 0, result.output
        assert "0 file(s)" in result.output

    def test_second_ingest_leaves_row_count_unchanged(self, ingest_env, warehouse):
        env, _ = ingest_env
        get_settings.cache_clear()
        structlog.reset_defaults()
        _runner.invoke(app, ["ingest"], env=env)
        row = warehouse.execute("SELECT COUNT(*) FROM silver_events").fetchone()
        assert row is not None
        assert row[0] == 54
