"""Tests for the canonical silver_events load (ingest_file)."""

from __future__ import annotations

import json
import uuid
from datetime import date
from pathlib import Path

import duckdb
import pytest

from piper.discovery import FoundFile
from piper.ingest import IngestStats, ingest_file
from piper.models.envelope import Envelope
from piper.models.row import SilverRow
from piper.sql_runner import apply_pending_migrations

_SQL_DIR = Path(__file__).parent.parent / "src" / "piper" / "sql" / "schema"
_TODAY = date(2026, 3, 1)

# A valid base event; all tests that need a complete event start here.
_BASE_EVENT = {
    "schema_version": "1.0",
    "event_type": "dcc.launch",
    "occurred_at_utc": "2020-01-01T00:00:00Z",
    "status": "success",
    "pipeline": {"name": "sandwich-pipeline", "dcc": "maya"},
    "host": {"hostname": "samus.cs.byu.edu", "user": "rees23"},
    "session": {"session_id": "sess-001"},
}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def conn():
    c = duckdb.connect(":memory:")
    apply_pending_migrations(c, _SQL_DIR)
    yield c
    c.close()


def _make_event(event_id: str | None = None, **overrides) -> dict:
    """Return a minimal valid event dict with a fresh UUID if not provided."""
    event = {**_BASE_EVENT, "event_id": event_id or str(uuid.uuid4())}
    event.update(overrides)
    return event


def _write_jsonl(path: Path, events: list[dict]) -> FoundFile:
    """Write *events* to *path* as JSONL and return a FoundFile for it."""
    path.write_text("\n".join(json.dumps(e) for e in events) + "\n", encoding="utf-8")
    st = path.stat()
    return FoundFile(path=path, size=st.st_size, mtime=st.st_mtime)


# ---------------------------------------------------------------------------
# The headline spec test
# ---------------------------------------------------------------------------


class TestDeduplication:
    def test_100_events_20_duplicates_yields_80_rows(self, conn, tmp_path):
        """100 ingested events with 20 duplicates yields exactly 80 rows."""
        unique = [_make_event() for _ in range(80)]
        duplicates = unique[:20]  # repeat the first 20
        file = _write_jsonl(tmp_path / "events.jsonl", unique + duplicates)

        ingest_file(conn, file, quarantine_dir=tmp_path / "q", today=_TODAY)

        count = conn.execute("SELECT COUNT(*) FROM silver_events").fetchone()[0]
        assert count == 80

    def test_stats_accepted_and_duplicate(self, conn, tmp_path):
        unique = [_make_event() for _ in range(80)]
        duplicates = unique[:20]
        file = _write_jsonl(tmp_path / "events.jsonl", unique + duplicates)

        stats = ingest_file(conn, file, quarantine_dir=tmp_path / "q", today=_TODAY)

        assert stats.accepted == 80
        assert stats.duplicate == 20

    def test_second_ingest_of_same_file_adds_no_rows(self, conn, tmp_path):
        events = [_make_event() for _ in range(10)]
        file = _write_jsonl(tmp_path / "events.jsonl", events)

        ingest_file(conn, file, quarantine_dir=tmp_path / "q", today=_TODAY)
        stats2 = ingest_file(conn, file, quarantine_dir=tmp_path / "q", today=_TODAY)

        assert stats2.accepted == 0
        assert stats2.duplicate == 10

    def test_cross_file_dedup_by_event_id(self, conn, tmp_path):
        """The same event_id in two different files counts as one row."""
        shared_id = str(uuid.uuid4())
        file1 = _write_jsonl(tmp_path / "a.jsonl", [_make_event(shared_id)])
        file2 = _write_jsonl(tmp_path / "b.jsonl", [_make_event(shared_id)])

        ingest_file(conn, file1, quarantine_dir=tmp_path / "q", today=_TODAY)
        stats2 = ingest_file(conn, file2, quarantine_dir=tmp_path / "q", today=_TODAY)

        assert conn.execute("SELECT COUNT(*) FROM silver_events").fetchone()[0] == 1
        assert stats2.duplicate == 1


# ---------------------------------------------------------------------------
# IngestStats counts
# ---------------------------------------------------------------------------


class TestIngestStats:
    def test_empty_file_returns_zero_stats(self, conn, tmp_path):
        p = tmp_path / "empty.jsonl"
        p.write_text("", encoding="utf-8")
        file = FoundFile(path=p, size=0, mtime=p.stat().st_mtime)

        stats = ingest_file(conn, file, quarantine_dir=tmp_path / "q", today=_TODAY)

        assert stats == IngestStats(total=0, accepted=0, duplicate=0, quarantined=0)

    def test_total_counts_nonblank_lines(self, conn, tmp_path):
        events = [_make_event() for _ in range(5)]
        lines = ["", json.dumps(events[0]), "  ", json.dumps(events[1])]
        p = tmp_path / "events.jsonl"
        p.write_text("\n".join(lines), encoding="utf-8")
        file = FoundFile(path=p, size=p.stat().st_size, mtime=p.stat().st_mtime)

        stats = ingest_file(conn, file, quarantine_dir=tmp_path / "q", today=_TODAY)

        assert stats.total == 2  # 2 non-blank lines; blank lines don't count

    def test_bad_json_counted_in_quarantined(self, conn, tmp_path):
        p = tmp_path / "events.jsonl"
        p.write_text("{bad json}\n", encoding="utf-8")
        file = FoundFile(path=p, size=p.stat().st_size, mtime=p.stat().st_mtime)

        stats = ingest_file(conn, file, quarantine_dir=tmp_path / "q", today=_TODAY)

        assert stats.quarantined == 1
        assert stats.accepted == 0

    def test_invalid_envelope_counted_in_quarantined(self, conn, tmp_path):
        # Missing required 'host' field
        bad_event = {k: v for k, v in _make_event().items() if k != "host"}
        file = _write_jsonl(tmp_path / "events.jsonl", [bad_event])

        stats = ingest_file(conn, file, quarantine_dir=tmp_path / "q", today=_TODAY)

        assert stats.quarantined == 1
        assert stats.accepted == 0

    def test_mix_of_good_bad_and_duplicate(self, conn, tmp_path):
        good = _make_event()
        duplicate = dict(good)  # same event_id
        bad = "{not json}"
        p = tmp_path / "events.jsonl"
        p.write_text(
            json.dumps(good) + "\n" + json.dumps(duplicate) + "\n" + bad + "\n",
            encoding="utf-8",
        )
        file = FoundFile(path=p, size=p.stat().st_size, mtime=p.stat().st_mtime)

        stats = ingest_file(conn, file, quarantine_dir=tmp_path / "q", today=_TODAY)

        assert stats.total == 3
        assert stats.accepted == 1
        assert stats.duplicate == 1
        assert stats.quarantined == 1


# ---------------------------------------------------------------------------
# Quarantine output
# ---------------------------------------------------------------------------


class TestQuarantineOutput:
    def test_bad_json_written_to_quarantine(self, conn, tmp_path):
        p = tmp_path / "events.jsonl"
        p.write_text("{bad}\n", encoding="utf-8")
        file = FoundFile(path=p, size=p.stat().st_size, mtime=p.stat().st_mtime)
        q = tmp_path / "quarantine"

        ingest_file(conn, file, quarantine_dir=q, today=_TODAY)

        day_dir = q / "invalid_jsonl" / "2026-03-01"
        assert (day_dir / p.name).is_file()

    def test_invalid_envelope_written_to_quarantine(self, conn, tmp_path):
        bad_event = {k: v for k, v in _make_event().items() if k != "pipeline"}
        file = _write_jsonl(tmp_path / "events.jsonl", [bad_event])
        q = tmp_path / "quarantine"

        ingest_file(conn, file, quarantine_dir=q, today=_TODAY)

        day_dir = q / "invalid_jsonl" / "2026-03-01"
        assert (day_dir / file.path.name).is_file()


# ---------------------------------------------------------------------------
# silver_events field mapping
# ---------------------------------------------------------------------------


class TestFieldMapping:
    def _ingest_one(self, conn, tmp_path, event: dict) -> None:
        file = _write_jsonl(tmp_path / "events.jsonl", [event])
        ingest_file(conn, file, quarantine_dir=tmp_path / "q", today=_TODAY)

    def _exists(self, conn, event_id: str) -> bool:
        row = conn.execute(
            "SELECT event_id FROM silver_events WHERE event_id = ?", [event_id]
        ).fetchone()
        return row is not None

    def _fetch(self, conn, event_id: str, columns: str):
        return conn.execute(
            f"SELECT {columns} FROM silver_events WHERE event_id = ?", [event_id]
        ).fetchone()

    def test_event_id_stored(self, conn, tmp_path):
        eid = str(uuid.uuid4())
        event = _make_event(eid)
        self._ingest_one(conn, tmp_path, event)
        assert self._exists(conn, eid)

    def test_event_type_stored(self, conn, tmp_path):
        event = _make_event(event_type="playblast.create")
        self._ingest_one(conn, tmp_path, event)
        row = self._fetch(conn, event["event_id"], "event_type")
        assert row[0] == "playblast.create"

    def test_pipeline_dcc_null_for_background_collector(self, conn, tmp_path):
        event = _make_event()
        event["pipeline"] = {"name": "sandwich-pipeline"}  # no dcc
        event["event_type"] = "tractor.farm.snapshot"
        self._ingest_one(conn, tmp_path, event)
        row = self._fetch(conn, event["event_id"], "pipeline_dcc")
        assert row[0] is None

    def test_scope_fields_stored(self, conn, tmp_path):
        event = _make_event()
        event["scope"] = {"show": "shw", "shot": "0010_0010", "asset": "char_hero"}
        self._ingest_one(conn, tmp_path, event)
        row = self._fetch(conn, event["event_id"], "scope_show, scope_shot, scope_asset")
        assert row == ("shw", "0010_0010", "char_hero")

    def test_missing_scope_fields_are_null(self, conn, tmp_path):
        event = _make_event()  # no scope
        self._ingest_one(conn, tmp_path, event)
        row = self._fetch(conn, event["event_id"], "scope_show, scope_sequence, scope_shot")
        assert row == (None, None, None)

    def test_error_fields_populated(self, conn, tmp_path):
        event = _make_event(status="error")
        event["error"] = {"code": "E_IO", "message": "disk full"}
        self._ingest_one(conn, tmp_path, event)
        row = self._fetch(conn, event["event_id"], "error_code, error_message")
        assert row == ("E_IO", "disk full")

    def test_error_fields_null_for_success(self, conn, tmp_path):
        event = _make_event(status="success")
        self._ingest_one(conn, tmp_path, event)
        row = self._fetch(conn, event["event_id"], "error_code, error_message")
        assert row == (None, None)

    def test_payload_stored_as_json(self, conn, tmp_path):
        event = _make_event()
        event["payload"] = {"preset": "web", "frames": 100}
        self._ingest_one(conn, tmp_path, event)
        raw = self._fetch(conn, event["event_id"], "payload")[0]
        assert json.loads(raw) == {"preset": "web", "frames": 100}

    def test_metrics_stored_as_json(self, conn, tmp_path):
        event = _make_event()
        event["metrics"] = {"duration_ms": 1500}
        self._ingest_one(conn, tmp_path, event)
        raw = self._fetch(conn, event["event_id"], "metrics")[0]
        assert json.loads(raw) == {"duration_ms": 1500}

    def test_source_lineage_recorded(self, conn, tmp_path):
        event = _make_event()
        file = _write_jsonl(tmp_path / "lineage_test.jsonl", [event])
        ingest_file(conn, file, quarantine_dir=tmp_path / "q", today=_TODAY)
        row = self._fetch(conn, event["event_id"], "source_file, source_line")
        assert row[0] == str(file.path)
        assert row[1] == 1  # first non-blank line


# ---------------------------------------------------------------------------
# SilverRow — unit tests independent of DB
# ---------------------------------------------------------------------------


class TestSilverRow:
    def _envelope(self, **overrides) -> Envelope:
        from piper.validate import validate_envelope

        event = {**_BASE_EVENT, "event_id": str(uuid.uuid4()), **overrides}
        return validate_envelope(event)

    def test_payload_serialized_to_json_string(self):
        env = self._envelope()
        row = SilverRow.from_envelope(env, source_file=Path("/a.jsonl"), source_line=1)
        json.loads(row.payload)  # must not raise

    def test_empty_payload_serialized_as_empty_object(self):
        env = self._envelope()
        row = SilverRow.from_envelope(env, source_file=Path("/a.jsonl"), source_line=1)
        assert row.payload == "{}"
