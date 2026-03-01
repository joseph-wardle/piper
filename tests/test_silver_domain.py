"""Tests for silver domain SQL views: silver_publish_usd and silver_tool_events."""

from pathlib import Path

import duckdb
import pytest

from piper.discovery import FoundFile
from piper.ingest import ingest_file
from piper.sql_runner import apply_pending_migrations, apply_views

_SQL_SCHEMA_DIR = Path(__file__).parent.parent / "src" / "piper" / "sql" / "schema"
_SQL_SILVER_DIR = Path(__file__).parent.parent / "src" / "piper" / "sql" / "silver"
_FIXTURE_DIR = Path(__file__).parent / "fixtures"


# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------


@pytest.fixture()
def conn(tmp_path):
    c = duckdb.connect(":memory:")
    apply_pending_migrations(c, _SQL_SCHEMA_DIR)
    for fixture in sorted(_FIXTURE_DIR.glob("*.jsonl")):
        ff = FoundFile(
            path=fixture,
            size=fixture.stat().st_size,
            mtime=fixture.stat().st_mtime,
        )
        ingest_file(c, ff, quarantine_dir=tmp_path / "quarantine")
    apply_views(c, _SQL_SILVER_DIR)
    yield c
    c.close()


def _count(conn: duckdb.DuckDBPyConnection, view: str, where: str = "") -> int:
    sql = f"SELECT COUNT(*) FROM {view}"
    if where:
        sql += f" WHERE {where}"
    row = conn.execute(sql).fetchone()
    assert row is not None
    return int(row[0])


# ---------------------------------------------------------------------------
# silver_publish_usd
# ---------------------------------------------------------------------------


class TestSilverPublishUsd:
    _PUBLISH_TYPES = frozenset(
        {
            "publish.asset.usd",
            "publish.anim.usd",
            "publish.camera.usd",
            "publish.customanim.usd",
            "publish.previs_asset.usd",
        }
    )

    def test_row_count_matches_publish_fixture(self, conn):
        """publish.jsonl has 15 events (5 types × 3)."""
        assert _count(conn, "silver_publish_usd") == 15

    def test_all_five_publish_types_present(self, conn):
        rows = conn.execute(
            "SELECT DISTINCT event_type FROM silver_publish_usd ORDER BY event_type"
        ).fetchall()
        assert {r[0] for r in rows} == self._PUBLISH_TYPES

    def test_no_tool_events_included(self, conn):
        n = _count(
            conn,
            "silver_publish_usd",
            "event_type NOT IN ('publish.asset.usd', 'publish.anim.usd', "
            "'publish.camera.usd', 'publish.customanim.usd', 'publish.previs_asset.usd')",
        )
        assert n == 0

    def test_output_path_always_populated(self, conn):
        assert _count(conn, "silver_publish_usd", "output_path IS NULL") == 0

    def test_duration_ms_populated_for_success(self, conn):
        assert _count(conn, "silver_publish_usd", "status = 'success' AND duration_ms IS NULL") == 0

    def test_output_size_bytes_populated_for_success(self, conn):
        assert (
            _count(conn, "silver_publish_usd", "status = 'success' AND output_size_bytes IS NULL")
            == 0
        )

    def test_duration_ms_null_for_error(self, conn):
        assert (
            _count(conn, "silver_publish_usd", "status = 'error' AND duration_ms IS NOT NULL") == 0
        )

    def test_output_size_bytes_null_for_error(self, conn):
        assert (
            _count(conn, "silver_publish_usd", "status = 'error' AND output_size_bytes IS NOT NULL")
            == 0
        )

    def test_event_date_derived_from_occurred_at_utc(self, conn):
        """event_date must be a DATE — fetched as a string like 'YYYY-MM-DD'."""
        row = conn.execute("SELECT event_date FROM silver_publish_usd LIMIT 1").fetchone()
        assert row is not None
        assert row[0] is not None

    def test_hostname_column_populated(self, conn):
        assert _count(conn, "silver_publish_usd", "hostname IS NULL") == 0

    def test_username_column_populated(self, conn):
        assert _count(conn, "silver_publish_usd", "username IS NULL") == 0


# ---------------------------------------------------------------------------
# silver_tool_events
# ---------------------------------------------------------------------------


class TestSilverToolEvents:
    _TOOL_TYPES = frozenset(
        {
            "dcc.launch",
            "file.open",
            "file.create",
            "shot.setup",
            "playblast.create",
            "build.houdini.component",
            "texture.export.substance",
            "texture.convert.tex",
        }
    )

    def test_row_count_matches_tool_fixture(self, conn):
        """tool.jsonl has 24 events (8 types × 3)."""
        assert _count(conn, "silver_tool_events") == 24

    def test_all_eight_tool_types_present(self, conn):
        rows = conn.execute(
            "SELECT DISTINCT event_type FROM silver_tool_events ORDER BY event_type"
        ).fetchall()
        assert {r[0] for r in rows} == self._TOOL_TYPES

    def test_no_publish_events_included(self, conn):
        assert _count(conn, "silver_tool_events", "event_type LIKE 'publish.%'") == 0

    def test_duration_ms_populated_for_dcc_launch(self, conn):
        """dcc.launch stores launch_duration_ms; the view must coalesce it for success rows."""
        assert (
            _count(
                conn,
                "silver_tool_events",
                "event_type = 'dcc.launch' AND status = 'success' AND duration_ms IS NULL",
            )
            == 0
        )

    def test_duration_ms_populated_for_other_tools(self, conn):
        assert (
            _count(
                conn,
                "silver_tool_events",
                "event_type != 'dcc.launch' AND status = 'success' AND duration_ms IS NULL",
            )
            == 0
        )

    def test_event_date_derived_from_occurred_at_utc(self, conn):
        row = conn.execute("SELECT event_date FROM silver_tool_events LIMIT 1").fetchone()
        assert row is not None
        assert row[0] is not None

    def test_hostname_column_populated(self, conn):
        assert _count(conn, "silver_tool_events", "hostname IS NULL") == 0

    def test_username_column_populated(self, conn):
        assert _count(conn, "silver_tool_events", "username IS NULL") == 0
