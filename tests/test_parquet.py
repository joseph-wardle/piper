"""Tests for the silver_events and silver domain Parquet exports."""

from pathlib import Path

import duckdb
import pytest

from piper.discovery import FoundFile
from piper.ingest import ingest_file
from piper.parquet import export_silver_domain, export_silver_events
from piper.sql_runner import apply_pending_migrations, apply_views

_SQL_DIR = Path(__file__).parent.parent / "src" / "piper" / "sql" / "schema"
_SQL_SILVER_DIR = Path(__file__).parent.parent / "src" / "piper" / "sql" / "silver"
_FIXTURE_DIR = Path(__file__).parent / "fixtures"


# ---------------------------------------------------------------------------
# Fixtures and helpers
# ---------------------------------------------------------------------------


@pytest.fixture()
def conn():
    c = duckdb.connect(":memory:")
    apply_pending_migrations(c, _SQL_DIR)
    yield c
    c.close()


def _insert(conn: duckdb.DuckDBPyConnection, event_id: str, event_type: str, date: str) -> None:
    """Insert one minimal row into silver_events."""
    ts = f"{date}T10:00:00+00:00"
    conn.execute(
        """
        INSERT INTO silver_events (
            event_id, schema_version, event_type, occurred_at_utc, status,
            pipeline_name, host_hostname, host_user, session_id,
            payload, metrics, source_file, source_line
        ) VALUES (?, ?, ?, ?::TIMESTAMPTZ, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            event_id,
            "1.0",
            event_type,
            ts,
            "success",
            "sandwich-pipeline",
            "samus.cs.byu.edu",
            "rees23",
            "sess-001",
            "{}",
            "{}",
            "/raw/test.jsonl",
            1,
        ],
    )


# ---------------------------------------------------------------------------
# Core export behaviour
# ---------------------------------------------------------------------------


class TestExportSilverEvents:
    def test_empty_table_returns_zero(self, conn, tmp_path):
        n = export_silver_events(conn, tmp_path / "silver")
        assert n == 0

    def test_empty_table_creates_output_directory(self, conn, tmp_path):
        export_silver_events(conn, tmp_path / "silver")
        assert (tmp_path / "silver" / "silver_events").is_dir()

    def test_returns_row_count(self, conn, tmp_path):
        _insert(conn, "id-1", "dcc.launch", "2026-02-15")
        _insert(conn, "id-2", "dcc.launch", "2026-02-15")
        n = export_silver_events(conn, tmp_path / "silver")
        assert n == 2

    def test_parquet_files_created(self, conn, tmp_path):
        _insert(conn, "id-1", "dcc.launch", "2026-02-15")
        export_silver_events(conn, tmp_path / "silver")
        parquet_files = list((tmp_path / "silver" / "silver_events").rglob("*.parquet"))
        assert len(parquet_files) > 0

    def test_parquet_readable_by_duckdb(self, conn, tmp_path):
        _insert(conn, "id-1", "dcc.launch", "2026-02-15")
        _insert(conn, "id-2", "file.open", "2026-02-15")
        export_silver_events(conn, tmp_path / "silver")

        out_dir = tmp_path / "silver" / "silver_events"
        row = conn.execute(
            f"SELECT COUNT(*) FROM read_parquet('{out_dir}/**/*.parquet')"
        ).fetchone()
        assert row is not None
        assert row[0] == 2


# ---------------------------------------------------------------------------
# export_silver_domain — shared fixture
# ---------------------------------------------------------------------------


@pytest.fixture()
def domain_conn(tmp_path):
    """In-memory DuckDB with all fixture data ingested and silver views applied."""
    c = duckdb.connect(":memory:")
    apply_pending_migrations(c, _SQL_DIR)
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


# ---------------------------------------------------------------------------
# export_silver_domain — core behaviour
# ---------------------------------------------------------------------------


class TestExportSilverDomain:
    _VIEWS = [
        "silver_publish_usd",
        "silver_tool_events",
        "silver_tractor_job_spool",
        "silver_tractor_farm_snapshot",
        "silver_render_stats_summary",
        "silver_storage_scan_summary",
        "silver_storage_scan_bucket",
    ]

    def test_returns_dict_with_all_seven_views(self, domain_conn, tmp_path):
        result = export_silver_domain(domain_conn, tmp_path / "silver")
        assert set(result.keys()) == set(self._VIEWS)

    def test_row_counts_are_positive(self, domain_conn, tmp_path):
        result = export_silver_domain(domain_conn, tmp_path / "silver")
        assert all(n > 0 for n in result.values())

    def test_output_directories_created(self, domain_conn, tmp_path):
        export_silver_domain(domain_conn, tmp_path / "silver")
        for view in self._VIEWS:
            assert (tmp_path / "silver" / view).is_dir()

    def test_parquet_files_exist_in_each_dataset(self, domain_conn, tmp_path):
        export_silver_domain(domain_conn, tmp_path / "silver")
        for view in self._VIEWS:
            files = list((tmp_path / "silver" / view).rglob("*.parquet"))
            assert len(files) > 0, f"no Parquet files for {view}"

    def test_partitioned_by_event_date(self, domain_conn, tmp_path):
        export_silver_domain(domain_conn, tmp_path / "silver")
        publish_dir = tmp_path / "silver" / "silver_publish_usd"
        date_dirs = [d.name for d in publish_dir.iterdir() if d.is_dir()]
        assert any(d.startswith("event_date=") for d in date_dirs)

    def test_parquet_readable_by_duckdb(self, domain_conn, tmp_path):
        result = export_silver_domain(domain_conn, tmp_path / "silver")
        for view, expected_n in result.items():
            out_dir = tmp_path / "silver" / view
            row = domain_conn.execute(
                f"SELECT COUNT(*) FROM read_parquet('{out_dir}/**/*.parquet')"
            ).fetchone()
            assert row is not None
            assert row[0] == expected_n, f"{view}: expected {expected_n} rows in Parquet"

    def test_empty_view_creates_directory_no_files(self, tmp_path):
        """An empty domain view produces a directory but no Parquet files."""
        c = duckdb.connect(":memory:")
        apply_pending_migrations(c, _SQL_DIR)
        apply_views(c, _SQL_SILVER_DIR)
        result = export_silver_domain(c, tmp_path / "silver")
        c.close()
        assert all(n == 0 for n in result.values())
        for view in self._VIEWS:
            assert (tmp_path / "silver" / view).is_dir()

    def test_idempotent_reexport(self, domain_conn, tmp_path):
        silver_dir = tmp_path / "silver"
        export_silver_domain(domain_conn, silver_dir)
        files_1 = {str(f.relative_to(silver_dir)) for f in silver_dir.rglob("*.parquet")}
        export_silver_domain(domain_conn, silver_dir)
        files_2 = {str(f.relative_to(silver_dir)) for f in silver_dir.rglob("*.parquet")}
        assert files_1 == files_2


# ---------------------------------------------------------------------------
# Partition layout
# ---------------------------------------------------------------------------


class TestPartitionLayout:
    def test_hive_date_partition_directory_exists(self, conn, tmp_path):
        """Partition directory named event_date=YYYY-MM-DD must be created."""
        _insert(conn, "id-1", "dcc.launch", "2026-02-15")
        export_silver_events(conn, tmp_path / "silver")

        date_dir = tmp_path / "silver" / "silver_events" / "event_date=2026-02-15"
        assert date_dir.is_dir()

    def test_hive_event_type_partition_directory_exists(self, conn, tmp_path):
        """Sub-partition event_type=<type> must exist under the date partition."""
        _insert(conn, "id-1", "dcc.launch", "2026-02-15")
        export_silver_events(conn, tmp_path / "silver")

        type_dir = (
            tmp_path
            / "silver"
            / "silver_events"
            / "event_date=2026-02-15"
            / "event_type=dcc.launch"
        )
        assert type_dir.is_dir()

    def test_parquet_file_inside_partition(self, conn, tmp_path):
        _insert(conn, "id-1", "dcc.launch", "2026-02-15")
        export_silver_events(conn, tmp_path / "silver")

        type_dir = (
            tmp_path
            / "silver"
            / "silver_events"
            / "event_date=2026-02-15"
            / "event_type=dcc.launch"
        )
        assert any(type_dir.glob("*.parquet"))

    def test_multiple_event_types_separate_subdirectories(self, conn, tmp_path):
        """Two event types on the same day → two separate partition directories."""
        _insert(conn, "id-1", "dcc.launch", "2026-02-15")
        _insert(conn, "id-2", "file.open", "2026-02-15")
        export_silver_events(conn, tmp_path / "silver")

        date_dir = tmp_path / "silver" / "silver_events" / "event_date=2026-02-15"
        subdirs = [d.name for d in date_dir.iterdir() if d.is_dir()]
        assert "event_type=dcc.launch" in subdirs
        assert "event_type=file.open" in subdirs

    def test_multiple_dates_separate_date_partitions(self, conn, tmp_path):
        """Events on different days → separate date partition directories."""
        _insert(conn, "id-1", "dcc.launch", "2026-02-14")
        _insert(conn, "id-2", "dcc.launch", "2026-02-15")
        export_silver_events(conn, tmp_path / "silver")

        root = tmp_path / "silver" / "silver_events"
        date_dirs = [d.name for d in root.iterdir() if d.is_dir()]
        assert "event_date=2026-02-14" in date_dirs
        assert "event_date=2026-02-15" in date_dirs

    def test_dot_in_event_type_creates_valid_directory(self, conn, tmp_path):
        """Event types with dots (e.g. tractor.job.spool) are valid dir names."""
        _insert(conn, "id-1", "tractor.job.spool", "2026-02-15")
        export_silver_events(conn, tmp_path / "silver")

        type_dir = (
            tmp_path
            / "silver"
            / "silver_events"
            / "event_date=2026-02-15"
            / "event_type=tractor.job.spool"
        )
        assert type_dir.is_dir()


# ---------------------------------------------------------------------------
# Idempotency
# ---------------------------------------------------------------------------


class TestIdempotency:
    def test_reexport_same_data_produces_same_files(self, conn, tmp_path):
        _insert(conn, "id-1", "dcc.launch", "2026-02-15")
        _insert(conn, "id-2", "file.open", "2026-02-16")
        silver_dir = tmp_path / "silver"

        export_silver_events(conn, silver_dir)
        files_1 = {str(f.relative_to(silver_dir)) for f in silver_dir.rglob("*.parquet")}

        export_silver_events(conn, silver_dir)
        files_2 = {str(f.relative_to(silver_dir)) for f in silver_dir.rglob("*.parquet")}

        assert files_1 == files_2

    def test_reexport_returns_same_row_count(self, conn, tmp_path):
        _insert(conn, "id-1", "dcc.launch", "2026-02-15")
        silver_dir = tmp_path / "silver"
        n1 = export_silver_events(conn, silver_dir)
        n2 = export_silver_events(conn, silver_dir)
        assert n1 == n2 == 1

    def test_reexport_does_not_raise(self, conn, tmp_path):
        _insert(conn, "id-1", "dcc.launch", "2026-02-15")
        silver_dir = tmp_path / "silver"
        export_silver_events(conn, silver_dir)
        export_silver_events(conn, silver_dir)  # must not raise

    def test_reexport_after_new_data_reflects_changes(self, conn, tmp_path):
        """After inserting more rows, re-export includes the new data."""
        _insert(conn, "id-1", "dcc.launch", "2026-02-15")
        silver_dir = tmp_path / "silver"

        export_silver_events(conn, silver_dir)

        _insert(conn, "id-2", "file.open", "2026-02-16")
        n = export_silver_events(conn, silver_dir)
        assert n == 2

        out_dir = silver_dir / "silver_events"
        row = conn.execute(
            f"SELECT COUNT(*) FROM read_parquet('{out_dir}/**/*.parquet')"
        ).fetchone()
        assert row is not None
        assert row[0] == 2
