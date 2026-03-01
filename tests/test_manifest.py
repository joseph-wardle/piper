"""Tests for ingest manifest fingerprint tracking."""

from pathlib import Path

import duckdb
import pytest

from piper.discovery import FoundFile
from piper.manifest import is_already_ingested, mark_ingested
from piper.sql_runner import apply_pending_migrations

_SQL_DIR = Path(__file__).parent.parent / "src" / "piper" / "sql" / "schema"

# A representative FoundFile used across tests.
_FILE = FoundFile(
    path=Path("/raw/host1/user1/2026-02-15.jsonl"),
    size=4096,
    mtime=1_740_000_000.0,
)


@pytest.fixture()
def conn():
    c = duckdb.connect(":memory:")
    apply_pending_migrations(c, _SQL_DIR)
    yield c
    c.close()


# ---------------------------------------------------------------------------
# is_already_ingested
# ---------------------------------------------------------------------------


class TestIsAlreadyIngested:
    def test_untracked_file_returns_false(self, conn):
        assert not is_already_ingested(conn, _FILE)

    def test_returns_true_after_mark_ingested(self, conn):
        mark_ingested(conn, _FILE, event_count=10, error_count=0)
        assert is_already_ingested(conn, _FILE)

    def test_different_mtime_returns_false(self, conn):
        """Same path and size but newer mtime = re-ingestion required."""
        mark_ingested(conn, _FILE, event_count=10, error_count=0)
        changed = FoundFile(path=_FILE.path, size=_FILE.size, mtime=_FILE.mtime + 1.0)
        assert not is_already_ingested(conn, changed)

    def test_different_size_returns_false(self, conn):
        """Same path and mtime but different size = re-ingestion required."""
        mark_ingested(conn, _FILE, event_count=10, error_count=0)
        changed = FoundFile(path=_FILE.path, size=_FILE.size + 1, mtime=_FILE.mtime)
        assert not is_already_ingested(conn, changed)

    def test_different_path_returns_false(self, conn):
        mark_ingested(conn, _FILE, event_count=10, error_count=0)
        other = FoundFile(
            path=Path("/raw/host2/user2/other.jsonl"),
            size=_FILE.size,
            mtime=_FILE.mtime,
        )
        assert not is_already_ingested(conn, other)


# ---------------------------------------------------------------------------
# mark_ingested
# ---------------------------------------------------------------------------


class TestMarkIngested:
    def test_stores_event_count(self, conn):
        mark_ingested(conn, _FILE, event_count=42, error_count=0)
        row = conn.execute(
            "SELECT event_count FROM ingest_manifest WHERE file_path = ?",
            [str(_FILE.path)],
        ).fetchone()
        assert row == (42,)

    def test_stores_error_count(self, conn):
        mark_ingested(conn, _FILE, event_count=42, error_count=3)
        row = conn.execute(
            "SELECT error_count FROM ingest_manifest WHERE file_path = ?",
            [str(_FILE.path)],
        ).fetchone()
        assert row == (3,)

    def test_stores_fingerprint(self, conn):
        mark_ingested(conn, _FILE, event_count=1, error_count=0)
        row = conn.execute(
            "SELECT file_mtime, file_size FROM ingest_manifest WHERE file_path = ?",
            [str(_FILE.path)],
        ).fetchone()
        assert row == (_FILE.mtime, _FILE.size)

    def test_upsert_updates_changed_fingerprint(self, conn):
        """Re-ingesting a corrected file (same path, new mtime) updates the row."""
        mark_ingested(conn, _FILE, event_count=10, error_count=0)
        updated = FoundFile(path=_FILE.path, size=8192, mtime=_FILE.mtime + 60.0)
        mark_ingested(conn, updated, event_count=20, error_count=1)

        row = conn.execute(
            "SELECT file_size, event_count, error_count FROM ingest_manifest "
            "WHERE file_path = ?",
            [str(_FILE.path)],
        ).fetchone()
        assert row == (8192, 20, 1)

    def test_one_row_per_path_after_upsert(self, conn):
        mark_ingested(conn, _FILE, event_count=10, error_count=0)
        mark_ingested(conn, _FILE, event_count=10, error_count=0)
        count = conn.execute(
            "SELECT COUNT(*) FROM ingest_manifest WHERE file_path = ?",
            [str(_FILE.path)],
        ).fetchone()[0]
        assert count == 1

    def test_multiple_paths_tracked_independently(self, conn):
        file_b = FoundFile(
            path=Path("/raw/host2/user2/2026-02-16.jsonl"),
            size=2048,
            mtime=_FILE.mtime + 3600,
        )
        mark_ingested(conn, _FILE, event_count=5, error_count=0)
        mark_ingested(conn, file_b, event_count=8, error_count=1)

        assert is_already_ingested(conn, _FILE)
        assert is_already_ingested(conn, file_b)
        assert conn.execute("SELECT COUNT(*) FROM ingest_manifest").fetchone()[0] == 2
