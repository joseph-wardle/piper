"""Quarantine writer for bad JSONL lines.

``quarantine_line(quarantine_dir, source_file, bad)`` appends a single
JSON record to::

    <quarantine_dir>/invalid_jsonl/<YYYY-MM-DD>/<source_file.name>

Each record captures enough context to investigate the problem later:
- ``quarantined_at_utc`` — ISO-8601 timestamp of when the line was quarantined
- ``source_file``        — absolute path of the originating JSONL file
- ``line_number``        — 1-based line number within the source file
- ``reason``             — human-readable description of the parse failure
- ``raw_text``           — the offending line verbatim

Bad lines are *appended*, so quarantine files are themselves valid JSONL
and can be replayed or audited later without data loss.
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

from piper.parser import BadLine

_SUBDIR = "invalid_jsonl"


def quarantine_line(
    quarantine_dir: Path,
    source_file: Path,
    bad: BadLine,
    *,
    today: date | None = None,
) -> None:
    """Append *bad* to the quarantine JSONL for *source_file*.

    Args:
        quarantine_dir: Root quarantine directory (``paths.quarantine_dir``).
        source_file:    The JSONL file the bad line came from.
        bad:            The :class:`~piper.parser.BadLine` to record.
        today:          Override the partition date (for testing).  Defaults
                        to :func:`datetime.date.today`.
    """
    partition = today if today is not None else date.today()
    day_dir = quarantine_dir / _SUBDIR / partition.strftime("%Y-%m-%d")
    day_dir.mkdir(parents=True, exist_ok=True)

    record = {
        "quarantined_at_utc": datetime.now(UTC).isoformat(timespec="seconds"),
        "source_file": str(source_file),
        "line_number": bad.line_number,
        "reason": bad.reason,
        "raw_text": bad.raw_text,
    }
    with (day_dir / source_file.name).open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record) + "\n")
