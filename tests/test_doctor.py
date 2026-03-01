"""Unit tests for piper.doctor — pipeline health checks.

Each check function is tested against a minimal in-memory DuckDB so the
tests are time-stable: events are inserted relative to NOW() rather than
using hard-coded timestamps.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import pytest

from piper.doctor import (
    CheckResult,
    check_clock_skew,
    check_freshness,
    check_invalid_rate,
    check_volume,
    run_checks,
)
from piper.sql_runner import apply_pending_migrations

_SQL_DIR = Path(__file__).parent.parent / "src" / "piper" / "sql" / "schema"


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


@pytest.fixture()
def conn():
    c = duckdb.connect(":memory:")
    apply_pending_migrations(c, _SQL_DIR)
    yield c
    c.close()


def _insert_event(
    conn: duckdb.DuckDBPyConnection,
    *,
    event_id: str = "evt-001",
    hours_ago: float = 1.0,
    status: str = "success",
) -> None:
    """Insert a minimal silver_events row with occurred_at_utc set relative to now."""
    ts = datetime.now(UTC) - timedelta(hours=hours_ago)
    conn.execute(
        "INSERT INTO silver_events "
        "(event_id, schema_version, event_type, occurred_at_utc, status, "
        "pipeline_name, host_hostname, host_user, session_id, "
        "payload, metrics, source_file, source_line) "
        "VALUES (?, '1.0', 'file.open', ?, ?, "
        "'sandwich-pipeline', 'host01', 'user01', 'sess-001', "
        "'{}', '{}', 'test.jsonl', 1)",
        [event_id, ts, status],
    )


def _insert_manifest_row(
    conn: duckdb.DuckDBPyConnection,
    *,
    file_path: str = "raw/test.jsonl",
    event_count: int = 10,
    error_count: int = 0,
) -> None:
    conn.execute(
        "INSERT INTO ingest_manifest "
        "(file_path, file_mtime, file_size, event_count, error_count) "
        "VALUES (?, 0.0, 100, ?, ?)",
        [file_path, event_count, error_count],
    )


# ---------------------------------------------------------------------------
# check_freshness
# ---------------------------------------------------------------------------


class TestCheckFreshness:
    def test_empty_db_fails(self, conn):
        r = check_freshness(conn)
        assert r.status == "fail"
        assert "no events" in r.message
        assert r.hint

    def test_recent_event_passes(self, conn):
        _insert_event(conn, hours_ago=1)
        r = check_freshness(conn)
        assert r.status == "pass"
        assert "h ago" in r.message

    def test_48h_boundary_passes(self, conn):
        _insert_event(conn, hours_ago=47)
        r = check_freshness(conn)
        assert r.status == "pass"

    def test_60h_event_warns(self, conn):
        _insert_event(conn, hours_ago=60)
        r = check_freshness(conn)
        assert r.status == "warn"
        assert r.hint

    def test_100h_event_fails(self, conn):
        _insert_event(conn, hours_ago=100)
        r = check_freshness(conn)
        assert r.status == "fail"
        assert r.hint

    def test_result_name(self, conn):
        r = check_freshness(conn)
        assert r.name == "freshness"


# ---------------------------------------------------------------------------
# check_volume
# ---------------------------------------------------------------------------


class TestCheckVolume:
    def test_empty_db_fails(self, conn):
        r = check_volume(conn)
        assert r.status == "fail"
        assert "0 events" in r.message

    def test_ten_recent_events_passes(self, conn):
        for i in range(10):
            _insert_event(conn, event_id=f"evt-{i:03}", hours_ago=1)
        r = check_volume(conn)
        assert r.status == "pass"
        assert "10 events" in r.message

    def test_five_recent_events_warns(self, conn):
        for i in range(5):
            _insert_event(conn, event_id=f"evt-{i:03}", hours_ago=1)
        r = check_volume(conn)
        assert r.status == "warn"
        assert r.hint

    def test_old_events_not_counted(self, conn):
        # Event older than 7 days should not satisfy the volume floor.
        _insert_event(conn, event_id="old-001", hours_ago=200)
        r = check_volume(conn)
        assert r.status == "fail"

    def test_result_name(self, conn):
        r = check_volume(conn)
        assert r.name == "volume"


# ---------------------------------------------------------------------------
# check_invalid_rate
# ---------------------------------------------------------------------------


class TestCheckInvalidRate:
    def test_no_manifest_rows_passes(self, conn):
        r = check_invalid_rate(conn)
        assert r.status == "pass"
        assert "skipping" in r.message

    def test_zero_errors_passes(self, conn):
        _insert_manifest_row(conn, event_count=100, error_count=0)
        r = check_invalid_rate(conn)
        assert r.status == "pass"
        assert "0.0%" in r.message

    def test_low_error_rate_passes(self, conn):
        _insert_manifest_row(conn, event_count=100, error_count=1)
        r = check_invalid_rate(conn)
        assert r.status == "pass"

    def test_moderate_error_rate_warns(self, conn):
        # 3 / 100 = 3% → warn
        _insert_manifest_row(conn, event_count=97, error_count=3)
        r = check_invalid_rate(conn)
        assert r.status == "warn"
        assert r.hint

    def test_high_error_rate_fails(self, conn):
        # 15 / 100 = 15% → fail
        _insert_manifest_row(conn, event_count=85, error_count=15)
        r = check_invalid_rate(conn)
        assert r.status == "fail"
        assert r.hint

    def test_result_name(self, conn):
        r = check_invalid_rate(conn)
        assert r.name == "invalid_rate"


# ---------------------------------------------------------------------------
# check_clock_skew
# ---------------------------------------------------------------------------


class TestCheckClockSkew:
    def test_empty_db_passes(self, conn):
        r = check_clock_skew(conn)
        assert r.status == "pass"

    def test_recent_event_passes(self, conn):
        _insert_event(conn, hours_ago=1)
        r = check_clock_skew(conn)
        assert r.status == "pass"

    def test_2day_skew_warns(self, conn):
        # occurred_at 48 h ago, ingested_at ≈ now → 48 h skew → warn (> 1 day)
        _insert_event(conn, hours_ago=48)
        r = check_clock_skew(conn)
        assert r.status == "warn"
        assert r.hint

    def test_8day_skew_fails(self, conn):
        # occurred_at 200 h ago, ingested_at ≈ now → 200 h skew → fail (> 7 days)
        _insert_event(conn, hours_ago=200)
        r = check_clock_skew(conn)
        assert r.status == "fail"
        assert r.hint

    def test_result_name(self, conn):
        r = check_clock_skew(conn)
        assert r.name == "clock_skew"


# ---------------------------------------------------------------------------
# run_checks — registry
# ---------------------------------------------------------------------------


class TestRunChecks:
    def test_all_checks_returned(self, conn):
        results = run_checks(conn)
        assert len(results) == 4
        names = {r.name for r in results}
        assert names == {"freshness", "volume", "invalid_rate", "clock_skew"}

    def test_single_check_by_name(self, conn):
        results = run_checks(conn, only="freshness")
        assert len(results) == 1
        assert results[0].name == "freshness"

    def test_unknown_check_raises(self, conn):
        with pytest.raises(ValueError, match="unknown check"):
            run_checks(conn, only="nonexistent")

    def test_all_results_are_check_results(self, conn):
        for r in run_checks(conn):
            assert isinstance(r, CheckResult)
            assert r.status in ("pass", "warn", "fail")
