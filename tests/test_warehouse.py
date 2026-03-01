"""Tests for DuckDB schema migration and warehouse connection.

Acceptance criteria (from commit spec):
  - piper init creates telemetry.duckdb with all tables
  - Re-running is idempotent
"""

from pathlib import Path

import duckdb
import pytest

from piper.config import Settings
from piper.paths import ProjectPaths
from piper.sql_runner import apply_pending_migrations
from piper.warehouse import WAREHOUSE_FILE, open_warehouse, run_migrations

# SQL directory bundled with the package.
_SQL_DIR = Path(__file__).parent.parent / "src" / "piper" / "sql" / "schema"


def _paths(tmp_path: Path) -> ProjectPaths:
    s = Settings(paths={"raw_root": str(tmp_path / "raw"), "data_root": str(tmp_path)})
    p = ProjectPaths.from_settings(s)
    p.ensure_output_dirs()
    return p


# ---------------------------------------------------------------------------
# apply_pending_migrations (unit tests against in-memory DuckDB)
# ---------------------------------------------------------------------------


class TestApplyPendingMigrations:
    @pytest.fixture()
    def mem_conn(self):
        conn = duckdb.connect(":memory:")
        yield conn
        conn.close()

    def test_first_run_applies_migrations(self, mem_conn):
        n = apply_pending_migrations(mem_conn, _SQL_DIR)
        assert n == 1  # 001_init.sql

    def test_second_run_returns_zero(self, mem_conn):
        apply_pending_migrations(mem_conn, _SQL_DIR)
        n = apply_pending_migrations(mem_conn, _SQL_DIR)
        assert n == 0

    def test_schema_migrations_table_created(self, mem_conn):
        apply_pending_migrations(mem_conn, _SQL_DIR)
        rows = mem_conn.execute("SELECT version FROM schema_migrations").fetchall()
        assert rows == [("001_init",)]

    def test_silver_events_table_created(self, mem_conn):
        apply_pending_migrations(mem_conn, _SQL_DIR)
        # SELECT * FROM information_schema.tables works in DuckDB.
        rows = mem_conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_name = 'silver_events'"
        ).fetchall()
        assert rows == [("silver_events",)]

    def test_ingest_manifest_table_created(self, mem_conn):
        apply_pending_migrations(mem_conn, _SQL_DIR)
        rows = mem_conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_name = 'ingest_manifest'"
        ).fetchall()
        assert rows == [("ingest_manifest",)]


# ---------------------------------------------------------------------------
# silver_events column schema
# ---------------------------------------------------------------------------


class TestSilverEventsSchema:
    @pytest.fixture(scope="class")
    def conn(self):
        c = duckdb.connect(":memory:")
        apply_pending_migrations(c, _SQL_DIR)
        yield c
        c.close()

    def _columns(self, conn) -> set[str]:
        rows = conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'silver_events'"
        ).fetchall()
        return {row[0] for row in rows}

    def test_identity_columns_present(self, conn):
        cols = self._columns(conn)
        for col in (
            "event_id",
            "schema_version",
            "event_type",
            "occurred_at_utc",
            "status",
            "ingested_at_utc",
        ):
            assert col in cols, f"missing column: {col}"

    def test_context_columns_present(self, conn):
        cols = self._columns(conn)
        for col in (
            "pipeline_name",
            "pipeline_dcc",
            "host_hostname",
            "host_user",
            "host_os",
            "session_id",
            "action_id",
        ):
            assert col in cols, f"missing column: {col}"

    def test_scope_columns_present(self, conn):
        cols = self._columns(conn)
        for col in (
            "scope_show",
            "scope_sequence",
            "scope_shot",
            "scope_asset",
            "scope_department",
            "scope_task",
        ):
            assert col in cols, f"missing column: {col}"

    def test_payload_and_metrics_columns_present(self, conn):
        cols = self._columns(conn)
        assert "payload" in cols
        assert "metrics" in cols

    def test_source_lineage_columns_present(self, conn):
        cols = self._columns(conn)
        assert "source_file" in cols
        assert "source_line" in cols


# ---------------------------------------------------------------------------
# open_warehouse + run_migrations (integration with real file on disk)
# ---------------------------------------------------------------------------


class TestOpenWarehouse:
    def test_creates_database_file(self, tmp_path):
        paths = _paths(tmp_path)
        conn = open_warehouse(paths)
        conn.close()
        assert (paths.warehouse_dir / WAREHOUSE_FILE).is_file()

    def test_run_migrations_creates_all_tables(self, tmp_path):
        paths = _paths(tmp_path)
        conn = open_warehouse(paths)
        run_migrations(conn)
        tables = {
            row[0]
            for row in conn.execute("SELECT table_name FROM information_schema.tables").fetchall()
        }
        conn.close()
        assert "silver_events" in tables
        assert "ingest_manifest" in tables
        assert "schema_migrations" in tables

    def test_run_migrations_is_idempotent(self, tmp_path):
        paths = _paths(tmp_path)
        conn = open_warehouse(paths)
        run_migrations(conn)
        run_migrations(conn)  # second call must not raise
        conn.close()

    def test_run_migrations_returns_count(self, tmp_path):
        paths = _paths(tmp_path)
        conn = open_warehouse(paths)
        n_first = run_migrations(conn)
        n_second = run_migrations(conn)
        conn.close()
        assert n_first == 1  # 001_init.sql applied
        assert n_second == 0  # nothing new
