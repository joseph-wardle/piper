"""Tests for the line-by-line JSONL parser."""

import json
from pathlib import Path

from piper.parser import parse_jsonl_file


def _write(tmp_path: Path, lines: list[str]) -> Path:
    p = tmp_path / "test.jsonl"
    p.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return p


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


class TestParseJsonlFileHappyPath:
    def test_single_valid_object(self, tmp_path):
        p = _write(tmp_path, ['{"event": "test", "v": 1}'])
        good, bad = parse_jsonl_file(p)
        assert len(good) == 1 and len(bad) == 0
        assert good[0].data == {"event": "test", "v": 1}

    def test_multiple_valid_objects(self, tmp_path):
        p = _write(tmp_path, [json.dumps({"n": i}) for i in range(5)])
        good, bad = parse_jsonl_file(p)
        assert len(good) == 5 and len(bad) == 0

    def test_empty_file_returns_empty_lists(self, tmp_path):
        p = tmp_path / "empty.jsonl"
        p.write_text("", encoding="utf-8")
        assert parse_jsonl_file(p) == ([], [])

    def test_whitespace_only_lines_skipped(self, tmp_path):
        p = _write(tmp_path, ["   ", "\t", '{"ok": true}', "  "])
        good, bad = parse_jsonl_file(p)
        assert len(good) == 1 and len(bad) == 0

    def test_blank_lines_count_toward_line_numbers(self, tmp_path):
        """Blank lines are skipped but their position is still counted in line_number."""
        p = _write(tmp_path, ["", '{"x": 1}'])
        good, _ = parse_jsonl_file(p)
        assert good[0].line_number == 2


# ---------------------------------------------------------------------------
# Bad lines
# ---------------------------------------------------------------------------


class TestParseJsonlFileBadLines:
    def test_malformed_json_goes_to_bad(self, tmp_path):
        p = _write(tmp_path, ["{not valid json}"])
        good, bad = parse_jsonl_file(p)
        assert len(good) == 0 and len(bad) == 1

    def test_bad_line_captures_metadata(self, tmp_path):
        """BadLine records line number, raw text, and a reason mentioning 'invalid JSON'."""
        raw = "{not valid json}"
        p = _write(tmp_path, ['{"ok": 1}', raw])
        _, bad = parse_jsonl_file(p)
        assert bad[0].line_number == 2
        assert bad[0].raw_text == raw
        assert "invalid JSON" in bad[0].reason

    def test_non_dict_json_goes_to_bad(self, tmp_path):
        """Arrays, strings, numbers, and null are all rejected; reason names the type."""
        p = _write(tmp_path, ["[1, 2, 3]"])
        good, bad = parse_jsonl_file(p)
        assert len(good) == 0 and len(bad) == 1
        assert "list" in bad[0].reason and "expected JSON object" in bad[0].reason

    def test_mix_of_good_and_bad(self, tmp_path):
        p = _write(tmp_path, ['{"ok": 1}', "bad", '{"ok": 2}', "[1]", '{"ok": 3}'])
        good, bad = parse_jsonl_file(p)
        assert len(good) == 3 and len(bad) == 2

    def test_good_and_bad_line_numbers_are_consistent(self, tmp_path):
        p = _write(tmp_path, ['{"a": 1}', "bad", '{"b": 2}'])
        good, bad = parse_jsonl_file(p)
        assert good[0].line_number == 1
        assert bad[0].line_number == 2
        assert good[1].line_number == 3

    def test_truncated_json_goes_to_bad(self, tmp_path):
        p = _write(tmp_path, ['{"key": '])
        _, bad = parse_jsonl_file(p)
        assert len(bad) == 1


# ---------------------------------------------------------------------------
# Integration: real fixture files
# ---------------------------------------------------------------------------


class TestParseJsonlFileWithFixtures:
    def test_publish_fixture_parses_cleanly(self):
        fixture = Path(__file__).parent / "fixtures" / "publish.jsonl"
        good, bad = parse_jsonl_file(fixture)
        assert len(bad) == 0
        assert len(good) == 15  # 5 event types × 3 events each
