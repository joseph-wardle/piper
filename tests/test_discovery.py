"""Tests for JSONL file discovery and stability filtering."""

import os
from pathlib import Path

from piper.discovery import discover_settled_files

_SETTLE = 120  # seconds — matches default settings.ingest.settle_seconds
_NOW = 1_000_000.0  # arbitrary fixed "now"; large enough that _NOW - _SETTLE > 0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _touch(path: Path, mtime: float) -> None:
    """Create *path* (and its parents) then set its mtime to *mtime*."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch()
    os.utime(path, (mtime, mtime))


# ---------------------------------------------------------------------------
# discover_settled_files — settled vs. unsettled classification
# ---------------------------------------------------------------------------


class TestDiscoverSettledFiles:
    def test_missing_raw_root_returns_empty(self, tmp_path):
        result = discover_settled_files(tmp_path / "nonexistent", _SETTLE, now=_NOW)
        assert result == []

    def test_empty_directory_returns_empty(self, tmp_path):
        result = discover_settled_files(tmp_path, _SETTLE, now=_NOW)
        assert result == []

    def test_settled_file_is_returned(self, tmp_path):
        p = tmp_path / "host1" / "user1" / "events.jsonl"
        _touch(p, mtime=_NOW - _SETTLE - 1)  # 1 s beyond settle window
        result = discover_settled_files(tmp_path, _SETTLE, now=_NOW)
        assert len(result) == 1
        assert result[0].path == p

    def test_unsettled_file_is_skipped(self, tmp_path):
        p = tmp_path / "host1" / "user1" / "events.jsonl"
        _touch(p, mtime=_NOW - _SETTLE + 1)  # 1 s inside settle window
        result = discover_settled_files(tmp_path, _SETTLE, now=_NOW)
        assert result == []

    def test_file_exactly_at_cutoff_is_settled(self, tmp_path):
        """mtime == now - settle_seconds is on the settled side of the boundary."""
        p = tmp_path / "events.jsonl"
        _touch(p, mtime=_NOW - _SETTLE)
        result = discover_settled_files(tmp_path, _SETTLE, now=_NOW)
        assert len(result) == 1

    def test_mix_of_settled_and_unsettled(self, tmp_path):
        _touch(tmp_path / "old.jsonl", mtime=_NOW - _SETTLE - 60)
        _touch(tmp_path / "new.jsonl", mtime=_NOW - 10)  # very recent
        result = discover_settled_files(tmp_path, _SETTLE, now=_NOW)
        assert len(result) == 1
        assert result[0].path.name == "old.jsonl"

    def test_only_jsonl_extension_included(self, tmp_path):
        _touch(tmp_path / "data.jsonl", mtime=_NOW - _SETTLE - 1)
        _touch(tmp_path / "data.json", mtime=_NOW - _SETTLE - 1)
        _touch(tmp_path / "data.txt", mtime=_NOW - _SETTLE - 1)
        result = discover_settled_files(tmp_path, _SETTLE, now=_NOW)
        assert len(result) == 1
        assert result[0].path.suffix == ".jsonl"

    def test_nested_subdirectories_discovered(self, tmp_path):
        for host in ("host1", "host2"):
            for user in ("userA", "userB"):
                _touch(tmp_path / host / user / "events.jsonl", mtime=_NOW - _SETTLE - 1)
        result = discover_settled_files(tmp_path, _SETTLE, now=_NOW)
        assert len(result) == 4

    def test_real_pipeline_layout(self, tmp_path):
        """Files under <raw_root>/<host>/<user>/*.jsonl are discovered."""
        _touch(
            tmp_path / "samus.cs.byu.edu" / "rees23" / "2026-02-15.jsonl", mtime=_NOW - _SETTLE - 1
        )
        _touch(
            tmp_path / "link.cs.byu.edu" / "wards49" / "2026-02-15.jsonl", mtime=_NOW - _SETTLE - 1
        )
        result = discover_settled_files(tmp_path, _SETTLE, now=_NOW)
        assert len(result) == 2

    def test_now_defaults_to_wall_clock(self, tmp_path):
        """Omitting now= uses time.time() — just confirm it doesn't raise."""
        _touch(tmp_path / "ancient.jsonl", mtime=1_000.0)  # always settled
        result = discover_settled_files(tmp_path, _SETTLE)  # no now=
        assert len(result) == 1


# ---------------------------------------------------------------------------
# discover_settled_files — sort order
# ---------------------------------------------------------------------------


class TestSortOrder:
    def test_sorted_oldest_mtime_first(self, tmp_path):
        _touch(tmp_path / "c.jsonl", mtime=_NOW - _SETTLE - 10)  # oldest
        _touch(tmp_path / "a.jsonl", mtime=_NOW - _SETTLE - 5)
        _touch(tmp_path / "b.jsonl", mtime=_NOW - _SETTLE - 1)  # newest-settled
        result = discover_settled_files(tmp_path, _SETTLE, now=_NOW)
        assert [f.path.name for f in result] == ["c.jsonl", "a.jsonl", "b.jsonl"]

    def test_equal_mtime_sorted_by_path(self, tmp_path):
        mtime = _NOW - _SETTLE - 1
        for name in ("z.jsonl", "a.jsonl", "m.jsonl"):
            _touch(tmp_path / name, mtime=mtime)
        result = discover_settled_files(tmp_path, _SETTLE, now=_NOW)
        assert [f.path.name for f in result] == ["a.jsonl", "m.jsonl", "z.jsonl"]


# ---------------------------------------------------------------------------
# FoundFile — fingerprint structure
# ---------------------------------------------------------------------------


class TestFoundFile:
    def test_fingerprint_captures_size(self, tmp_path):
        p = tmp_path / "events.jsonl"
        p.write_text('{"event": true}\n')
        expected_size = p.stat().st_size
        _touch(p, mtime=_NOW - _SETTLE - 1)  # touch preserves content
        result = discover_settled_files(tmp_path, _SETTLE, now=_NOW)
        assert result[0].size == expected_size

    def test_fingerprint_captures_mtime(self, tmp_path):
        p = tmp_path / "events.jsonl"
        expected_mtime = _NOW - _SETTLE - 1
        _touch(p, mtime=expected_mtime)
        result = discover_settled_files(tmp_path, _SETTLE, now=_NOW)
        assert result[0].mtime == expected_mtime

