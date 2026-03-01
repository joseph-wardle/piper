"""Tests for the quarantine writer."""

import json
from datetime import date
from pathlib import Path

from piper.parser import BadLine
from piper.quarantine import quarantine_line

_TODAY = date(2026, 3, 1)
_SOURCE = Path("/raw/host1/user1/2026-02-15.jsonl")
_BAD = BadLine(line_number=7, raw_text="{broken", reason="invalid JSON: ... (col 8)")


# ---------------------------------------------------------------------------
# File layout
# ---------------------------------------------------------------------------


class TestFileLayout:
    def test_creates_day_subdirectory(self, tmp_path):
        quarantine_line(tmp_path, _SOURCE, _BAD, today=_TODAY)
        day_dir = tmp_path / "invalid_jsonl" / "2026-03-01"
        assert day_dir.is_dir()

    def test_quarantine_file_named_after_source(self, tmp_path):
        quarantine_line(tmp_path, _SOURCE, _BAD, today=_TODAY)
        out = tmp_path / "invalid_jsonl" / "2026-03-01" / _SOURCE.name
        assert out.is_file()

    def test_creates_parent_dirs(self, tmp_path):
        deep = tmp_path / "a" / "b" / "c"
        quarantine_line(deep, _SOURCE, _BAD, today=_TODAY)
        assert (deep / "invalid_jsonl" / "2026-03-01" / _SOURCE.name).is_file()

    def test_different_date_different_directory(self, tmp_path):
        quarantine_line(tmp_path, _SOURCE, _BAD, today=date(2026, 3, 1))
        quarantine_line(tmp_path, _SOURCE, _BAD, today=date(2026, 3, 2))
        assert (tmp_path / "invalid_jsonl" / "2026-03-01").is_dir()
        assert (tmp_path / "invalid_jsonl" / "2026-03-02").is_dir()

    def test_today_defaults_to_wall_clock(self, tmp_path):
        """Omitting today= must not raise."""
        quarantine_line(tmp_path, _SOURCE, _BAD)
        assert any((tmp_path / "invalid_jsonl").iterdir())


# ---------------------------------------------------------------------------
# Record content
# ---------------------------------------------------------------------------


class TestRecordContent:
    def _read_records(self, tmp_path: Path) -> list[dict]:
        out = tmp_path / "invalid_jsonl" / "2026-03-01" / _SOURCE.name
        return [json.loads(line) for line in out.read_text().splitlines() if line.strip()]

    def test_record_is_valid_json(self, tmp_path):
        quarantine_line(tmp_path, _SOURCE, _BAD, today=_TODAY)
        records = self._read_records(tmp_path)
        assert len(records) == 1

    def test_record_has_source_file(self, tmp_path):
        quarantine_line(tmp_path, _SOURCE, _BAD, today=_TODAY)
        r = self._read_records(tmp_path)[0]
        assert r["source_file"] == str(_SOURCE)

    def test_record_has_line_number(self, tmp_path):
        quarantine_line(tmp_path, _SOURCE, _BAD, today=_TODAY)
        r = self._read_records(tmp_path)[0]
        assert r["line_number"] == _BAD.line_number

    def test_record_has_reason(self, tmp_path):
        quarantine_line(tmp_path, _SOURCE, _BAD, today=_TODAY)
        r = self._read_records(tmp_path)[0]
        assert r["reason"] == _BAD.reason

    def test_record_has_raw_text(self, tmp_path):
        quarantine_line(tmp_path, _SOURCE, _BAD, today=_TODAY)
        r = self._read_records(tmp_path)[0]
        assert r["raw_text"] == _BAD.raw_text

    def test_record_has_quarantined_at_utc(self, tmp_path):
        quarantine_line(tmp_path, _SOURCE, _BAD, today=_TODAY)
        r = self._read_records(tmp_path)[0]
        assert "quarantined_at_utc" in r

    def test_quarantined_at_utc_is_iso8601(self, tmp_path):
        from datetime import datetime

        quarantine_line(tmp_path, _SOURCE, _BAD, today=_TODAY)
        r = self._read_records(tmp_path)[0]
        # Must parse without error
        datetime.fromisoformat(r["quarantined_at_utc"])


# ---------------------------------------------------------------------------
# Append behaviour
# ---------------------------------------------------------------------------


class TestAppendBehaviour:
    def test_multiple_calls_append_multiple_records(self, tmp_path):
        bad2 = BadLine(line_number=12, raw_text="[1,2]", reason="expected JSON object, got list")
        quarantine_line(tmp_path, _SOURCE, _BAD, today=_TODAY)
        quarantine_line(tmp_path, _SOURCE, bad2, today=_TODAY)
        out = tmp_path / "invalid_jsonl" / "2026-03-01" / _SOURCE.name
        records = [json.loads(ln) for ln in out.read_text().splitlines() if ln.strip()]
        assert len(records) == 2
        assert records[0]["line_number"] == 7
        assert records[1]["line_number"] == 12

    def test_different_source_files_separate_quarantine_files(self, tmp_path):
        source2 = Path("/raw/host2/user2/2026-02-16.jsonl")
        quarantine_line(tmp_path, _SOURCE, _BAD, today=_TODAY)
        quarantine_line(tmp_path, source2, _BAD, today=_TODAY)
        day_dir = tmp_path / "invalid_jsonl" / "2026-03-01"
        assert (day_dir / _SOURCE.name).is_file()
        assert (day_dir / source2.name).is_file()

    def test_quarantine_file_is_valid_jsonl(self, tmp_path):
        """Each line in the quarantine file must be valid JSON."""
        for i in range(5):
            bad = BadLine(line_number=i, raw_text=f"bad{i}", reason="test")
            quarantine_line(tmp_path, _SOURCE, bad, today=_TODAY)
        out = tmp_path / "invalid_jsonl" / "2026-03-01" / _SOURCE.name
        lines = [ln for ln in out.read_text().splitlines() if ln.strip()]
        assert len(lines) == 5
        for line in lines:
            json.loads(line)  # must not raise
