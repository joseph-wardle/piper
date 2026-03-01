"""Tests for the line-by-line JSONL parser."""

import json
from pathlib import Path

import pytest

from piper.parser import BadLine, ParsedLine, parse_jsonl_file

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write(tmp_path: Path, lines: list[str]) -> Path:
    """Write *lines* to a temp JSONL file and return its path."""
    p = tmp_path / "test.jsonl"
    p.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return p


# ---------------------------------------------------------------------------
# parse_jsonl_file — happy path
# ---------------------------------------------------------------------------


class TestParseJsonlFileHappyPath:
    def test_single_valid_object(self, tmp_path):
        p = _write(tmp_path, ['{"event": "test", "v": 1}'])
        good, bad = parse_jsonl_file(p)
        assert len(good) == 1
        assert len(bad) == 0
        assert good[0].data == {"event": "test", "v": 1}

    def test_line_number_is_one_based(self, tmp_path):
        p = _write(tmp_path, ['{"a": 1}'])
        good, bad = parse_jsonl_file(p)
        assert good[0].line_number == 1

    def test_multiple_valid_objects(self, tmp_path):
        lines = [json.dumps({"n": i}) for i in range(5)]
        p = _write(tmp_path, lines)
        good, bad = parse_jsonl_file(p)
        assert len(good) == 5
        assert len(bad) == 0

    def test_returns_parsed_line_instances(self, tmp_path):
        p = _write(tmp_path, ['{"x": 1}'])
        good, _ = parse_jsonl_file(p)
        assert isinstance(good[0], ParsedLine)

    def test_preserves_nested_structure(self, tmp_path):
        obj = {"a": {"b": [1, 2, 3]}, "c": None}
        p = _write(tmp_path, [json.dumps(obj)])
        good, _ = parse_jsonl_file(p)
        assert good[0].data == obj

    def test_empty_file_returns_empty_lists(self, tmp_path):
        p = tmp_path / "empty.jsonl"
        p.write_text("", encoding="utf-8")
        good, bad = parse_jsonl_file(p)
        assert good == []
        assert bad == []

    def test_whitespace_only_lines_skipped(self, tmp_path):
        p = _write(tmp_path, ["   ", "\t", '{"ok": true}', "  "])
        good, bad = parse_jsonl_file(p)
        assert len(good) == 1
        assert len(bad) == 0

    def test_blank_lines_skipped(self, tmp_path):
        p = _write(tmp_path, ['{"a": 1}', "", '{"b": 2}'])
        good, bad = parse_jsonl_file(p)
        assert len(good) == 2
        assert [g.data["a"] if "a" in g.data else g.data["b"] for g in good] == [1, 2]

    def test_line_numbers_skip_blank_lines(self, tmp_path):
        """Blank lines count toward line numbering but are not returned."""
        p = _write(tmp_path, ["", '{"x": 1}'])
        good, _ = parse_jsonl_file(p)
        assert good[0].line_number == 2


# ---------------------------------------------------------------------------
# parse_jsonl_file — bad lines
# ---------------------------------------------------------------------------


class TestParseJsonlFileBadLines:
    def test_malformed_json_goes_to_bad(self, tmp_path):
        p = _write(tmp_path, ["{not valid json}"])
        good, bad = parse_jsonl_file(p)
        assert len(good) == 0
        assert len(bad) == 1

    def test_bad_line_has_correct_line_number(self, tmp_path):
        p = _write(tmp_path, ['{"ok": 1}', "bad json"])
        _, bad = parse_jsonl_file(p)
        assert bad[0].line_number == 2

    def test_bad_line_captures_raw_text(self, tmp_path):
        raw = "{not valid json}"
        p = _write(tmp_path, [raw])
        _, bad = parse_jsonl_file(p)
        assert bad[0].raw_text == raw

    def test_bad_line_reason_mentions_invalid_json(self, tmp_path):
        p = _write(tmp_path, ["{not valid json}"])
        _, bad = parse_jsonl_file(p)
        assert "invalid JSON" in bad[0].reason

    def test_bad_line_reason_includes_column(self, tmp_path):
        p = _write(tmp_path, ["{not valid json}"])
        _, bad = parse_jsonl_file(p)
        assert "col" in bad[0].reason

    def test_returns_bad_line_instances(self, tmp_path):
        p = _write(tmp_path, ["not json"])
        _, bad = parse_jsonl_file(p)
        assert isinstance(bad[0], BadLine)

    def test_json_array_goes_to_bad(self, tmp_path):
        p = _write(tmp_path, ["[1, 2, 3]"])
        good, bad = parse_jsonl_file(p)
        assert len(good) == 0
        assert len(bad) == 1

    def test_json_string_goes_to_bad(self, tmp_path):
        p = _write(tmp_path, ['"just a string"'])
        _, bad = parse_jsonl_file(p)
        assert len(bad) == 1

    def test_json_number_goes_to_bad(self, tmp_path):
        p = _write(tmp_path, ["42"])
        _, bad = parse_jsonl_file(p)
        assert len(bad) == 1

    def test_json_null_goes_to_bad(self, tmp_path):
        p = _write(tmp_path, ["null"])
        _, bad = parse_jsonl_file(p)
        assert len(bad) == 1

    def test_non_object_reason_mentions_type(self, tmp_path):
        p = _write(tmp_path, ["[1, 2]"])
        _, bad = parse_jsonl_file(p)
        assert "list" in bad[0].reason

    def test_non_object_reason_says_expected_object(self, tmp_path):
        p = _write(tmp_path, ["[1, 2]"])
        _, bad = parse_jsonl_file(p)
        assert "expected JSON object" in bad[0].reason

    def test_mix_of_good_and_bad(self, tmp_path):
        p = _write(tmp_path, ['{"ok": 1}', "bad", '{"ok": 2}', "[1]", '{"ok": 3}'])
        good, bad = parse_jsonl_file(p)
        assert len(good) == 3
        assert len(bad) == 2

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
# parse_jsonl_file — integration with real fixture files
# ---------------------------------------------------------------------------


class TestParseJsonlFileWithFixtures:
    def test_publish_fixture_all_good(self, tmp_path):
        fixture = Path(__file__).parent / "fixtures" / "publish.jsonl"
        good, bad = parse_jsonl_file(fixture)
        assert len(bad) == 0
        assert len(good) == 15  # 5 event types × 3 events each

    def test_tool_fixture_all_good(self, tmp_path):
        fixture = Path(__file__).parent / "fixtures" / "tool.jsonl"
        good, bad = parse_jsonl_file(fixture)
        assert len(bad) == 0
        assert len(good) == 24  # 8 event types × 3 events each

    def test_parsed_lines_are_dicts(self, tmp_path):
        fixture = Path(__file__).parent / "fixtures" / "farm.jsonl"
        good, _ = parse_jsonl_file(fixture)
        assert all(isinstance(g.data, dict) for g in good)


# ---------------------------------------------------------------------------
# ParsedLine and BadLine — dataclass properties
# ---------------------------------------------------------------------------


class TestDataclassProperties:
    def test_parsed_line_is_frozen(self):
        pl = ParsedLine(line_number=1, data={"x": 1})
        with pytest.raises((AttributeError, TypeError)):
            pl.line_number = 2  # type: ignore[misc]

    def test_bad_line_is_frozen(self):
        bl = BadLine(line_number=1, raw_text="x", reason="bad")
        with pytest.raises((AttributeError, TypeError)):
            bl.line_number = 2  # type: ignore[misc]

    def test_parsed_line_fields(self):
        pl = ParsedLine(line_number=5, data={"k": "v"})
        assert pl.line_number == 5
        assert pl.data == {"k": "v"}

    def test_bad_line_fields(self):
        bl = BadLine(line_number=3, raw_text="oops", reason="invalid JSON: ...")
        assert bl.line_number == 3
        assert bl.raw_text == "oops"
        assert "invalid JSON" in bl.reason
