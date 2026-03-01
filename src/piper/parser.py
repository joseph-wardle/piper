"""Line-by-line JSONL parser with bad-line capture.

``parse_jsonl_file(path)`` reads a JSONL file and returns two lists:
- ``good``: :class:`ParsedLine` for every line that is valid JSON *and* a
  JSON object (``{...}``).
- ``bad``:  :class:`BadLine` for every line that is malformed JSON or a
  non-object JSON value.

Empty and whitespace-only lines are skipped silently and do not appear in
either list.  The caller is responsible for deciding what to do with
``bad`` lines — typically passing them to :mod:`piper.quarantine`.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class ParsedLine:
    """A successfully parsed JSONL line."""

    line_number: int
    data: dict[str, Any]


@dataclass(frozen=True)
class BadLine:
    """A line that could not be parsed into a JSON object."""

    line_number: int
    raw_text: str
    reason: str


def parse_jsonl_file(path: Path) -> tuple[list[ParsedLine], list[BadLine]]:
    """Parse *path* line-by-line and return ``(good, bad)``.

    Args:
        path: Path to a ``.jsonl`` file (UTF-8 encoded).

    Returns:
        A 2-tuple ``(good, bad)`` where *good* is a list of
        :class:`ParsedLine` and *bad* is a list of :class:`BadLine`.
        Lists are in line-number order.  Empty lines are skipped and
        appear in neither list.
    """
    good: list[ParsedLine] = []
    bad: list[BadLine] = []

    for line_number, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        raw = raw.strip()
        if not raw:
            continue

        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as exc:
            bad.append(
                BadLine(
                    line_number=line_number,
                    raw_text=raw,
                    reason=f"invalid JSON: {exc.msg} (col {exc.colno})",
                )
            )
            continue

        if not isinstance(parsed, dict):
            bad.append(
                BadLine(
                    line_number=line_number,
                    raw_text=raw,
                    reason=f"expected JSON object, got {type(parsed).__name__}",
                )
            )
            continue

        good.append(ParsedLine(line_number=line_number, data=parsed))

    return good, bad
