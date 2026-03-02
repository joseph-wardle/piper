"""Tests for gold metric SQL views.

Each gold model is a CREATE OR REPLACE VIEW over the silver domain views.
The fixture conn applies all silver + gold views over the full set of
fixture data so every model can be exercised in a single setup pass.
"""

from pathlib import Path

import duckdb
import pytest

from piper.discovery import FoundFile
from piper.ingest import ingest_file
from piper.sql_runner import apply_pending_migrations, apply_views

_SQL_SCHEMA_DIR = Path(__file__).parent.parent / "src" / "piper" / "sql" / "schema"
_SQL_SILVER_DIR = Path(__file__).parent.parent / "src" / "piper" / "sql" / "silver"
_SQL_GOLD_DIR = Path(__file__).parent.parent / "src" / "piper" / "sql" / "gold"
_FIXTURE_DIR = Path(__file__).parent / "fixtures"


# ---------------------------------------------------------------------------
# Shared fixture
# ---------------------------------------------------------------------------


@pytest.fixture()
def conn(tmp_path):
    """In-memory DuckDB with all fixture data ingested and all views applied."""
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
    apply_views(c, _SQL_GOLD_DIR)
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
# gold_publish_health_daily
# ---------------------------------------------------------------------------


class TestGoldPublishHealthDaily:
    def test_has_rows(self, conn):
        assert _count(conn, "gold_publish_health_daily") > 0

    def test_has_five_event_types(self, conn):
        row = conn.execute(
            "SELECT COUNT(DISTINCT event_type) FROM gold_publish_health_daily"
        ).fetchone()
        assert row is not None
        assert row[0] == 5

    def test_required_columns_not_null(self, conn):
        assert (
            _count(
                conn,
                "gold_publish_health_daily",
                "event_date IS NULL OR total_publishes IS NULL "
                "OR success_count IS NULL OR error_count IS NULL "
                "OR success_rate_pct IS NULL",
            )
            == 0
        )

    def test_success_rate_in_valid_range(self, conn):
        assert (
            _count(
                conn, "gold_publish_health_daily", "success_rate_pct < 0 OR success_rate_pct > 100"
            )
            == 0
        )

    def test_counts_add_up(self, conn):
        assert (
            _count(
                conn, "gold_publish_health_daily", "success_count + error_count != total_publishes"
            )
            == 0
        )


# ---------------------------------------------------------------------------
# gold_render_health_daily
# ---------------------------------------------------------------------------


class TestGoldRenderHealthDaily:
    def test_has_rows(self, conn):
        assert _count(conn, "gold_render_health_daily") > 0

    def test_required_columns_not_null(self, conn):
        assert (
            _count(
                conn,
                "gold_render_health_daily",
                "event_date IS NULL OR total_jobs IS NULL "
                "OR success_rate_pct IS NULL OR total_frames IS NULL",
            )
            == 0
        )

    def test_success_rate_in_valid_range(self, conn):
        assert (
            _count(
                conn, "gold_render_health_daily", "success_rate_pct < 0 OR success_rate_pct > 100"
            )
            == 0
        )

    def test_total_frames_non_negative(self, conn):
        assert _count(conn, "gold_render_health_daily", "total_frames < 0") == 0


# ---------------------------------------------------------------------------
# gold_farm_pressure_daily
# ---------------------------------------------------------------------------


class TestGoldFarmPressureDaily:
    def test_has_rows(self, conn):
        """Only success snapshots; fixture has 2 success rows."""
        assert _count(conn, "gold_farm_pressure_daily") > 0

    def test_required_columns_not_null(self, conn):
        assert (
            _count(
                conn,
                "gold_farm_pressure_daily",
                "event_date IS NULL OR snapshot_count IS NULL "
                "OR avg_active_jobs IS NULL OR peak_active_blades IS NULL",
            )
            == 0
        )

    def test_peak_ge_avg(self, conn):
        assert _count(conn, "gold_farm_pressure_daily", "peak_active_jobs < avg_active_jobs") == 0
        assert (
            _count(conn, "gold_farm_pressure_daily", "peak_active_blades < avg_active_blades") == 0
        )


# ---------------------------------------------------------------------------
# gold_tool_reliability_daily
# ---------------------------------------------------------------------------


class TestGoldToolReliabilityDaily:
    def test_has_rows(self, conn):
        assert _count(conn, "gold_tool_reliability_daily") > 0

    def test_has_eight_event_types(self, conn):
        row = conn.execute(
            "SELECT COUNT(DISTINCT event_type) FROM gold_tool_reliability_daily"
        ).fetchone()
        assert row is not None
        assert row[0] == 8

    def test_required_columns_not_null(self, conn):
        assert (
            _count(
                conn,
                "gold_tool_reliability_daily",
                "event_date IS NULL OR total_invocations IS NULL OR success_rate_pct IS NULL",
            )
            == 0
        )

    def test_success_rate_in_valid_range(self, conn):
        assert (
            _count(
                conn,
                "gold_tool_reliability_daily",
                "success_rate_pct < 0 OR success_rate_pct > 100",
            )
            == 0
        )

    def test_counts_add_up(self, conn):
        assert (
            _count(
                conn,
                "gold_tool_reliability_daily",
                "success_count + error_count != total_invocations",
            )
            == 0
        )


# ---------------------------------------------------------------------------
# gold_storage_growth_weekly
# ---------------------------------------------------------------------------


class TestGoldStorageGrowthWeekly:
    def test_has_rows(self, conn):
        """Only success scan_bucket rows; fixture has 2 success rows."""
        assert _count(conn, "gold_storage_growth_weekly") > 0

    def test_required_columns_not_null(self, conn):
        assert (
            _count(
                conn,
                "gold_storage_growth_weekly",
                "week_start IS NULL OR bucket IS NULL "
                "OR latest_size_bytes IS NULL OR scan_count IS NULL",
            )
            == 0
        )

    def test_latest_size_bytes_positive(self, conn):
        assert _count(conn, "gold_storage_growth_weekly", "latest_size_bytes <= 0") == 0


# ---------------------------------------------------------------------------
# gold_data_quality_daily
# ---------------------------------------------------------------------------


class TestGoldDataQualityDaily:
    def test_has_rows(self, conn):
        assert _count(conn, "gold_data_quality_daily") > 0

    def test_required_columns_not_null(self, conn):
        assert (
            _count(
                conn,
                "gold_data_quality_daily",
                "event_date IS NULL OR total_events IS NULL "
                "OR error_rate_pct IS NULL OR active_users IS NULL",
            )
            == 0
        )

    def test_error_rate_in_valid_range(self, conn):
        assert (
            _count(conn, "gold_data_quality_daily", "error_rate_pct < 0 OR error_rate_pct > 100")
            == 0
        )

    def test_total_events_covers_all_fixtures(self, conn):
        """Sum of total_events across all days must equal all 54 fixture events."""
        row = conn.execute("SELECT SUM(total_events) FROM gold_data_quality_daily").fetchone()
        assert row is not None
        assert int(row[0]) == 54

    def test_active_users_positive(self, conn):
        assert _count(conn, "gold_data_quality_daily", "active_users <= 0") == 0
