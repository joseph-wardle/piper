"""Canonical silver_events load: validate → normalize → upsert.

``ingest_file(conn, file, quarantine_dir=...)`` is the single entry point.
It orchestrates the full pipeline for one settled JSONL file:

1. Parse every line with :func:`~piper.parser.parse_jsonl_file`.
2. Quarantine unparseable lines (bad JSON, non-object values).
3. Validate parseable lines with :func:`~piper.validate.validate_envelope`.
4. Quarantine lines whose envelope fails validation.
5. Normalize accepted envelopes to :class:`~piper.models.row.SilverRow`.
6. Bulk-upsert into ``silver_events`` with ``ON CONFLICT (event_id) DO NOTHING``
   so duplicate event IDs are silently skipped.

Returns an :class:`IngestStats` summary of the run.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import duckdb

from piper.discovery import FoundFile
from piper.models.row import SilverRow
from piper.parser import BadLine, parse_jsonl_file
from piper.quarantine import quarantine_line
from piper.validate import EnvelopeError, validate_envelope

# Column list must match SilverRow.as_params() order exactly.
_INSERT_SQL = """
INSERT INTO silver_events (
    event_id, schema_version, event_type, occurred_at_utc, status,
    pipeline_name, pipeline_dcc,
    host_hostname, host_user, host_os,
    session_id, action_id,
    scope_show, scope_sequence, scope_shot, scope_asset, scope_department, scope_task,
    error_code, error_message,
    payload, metrics,
    source_file, source_line
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (event_id) DO NOTHING
"""


def _silver_count(conn: duckdb.DuckDBPyConnection) -> int:
    row = conn.execute("SELECT COUNT(*) FROM silver_events").fetchone()
    assert row is not None  # COUNT(*) always returns exactly one row
    return int(row[0])


@dataclass(frozen=True)
class IngestStats:
    """Per-file ingest statistics returned by :func:`ingest_file`.

    Attributes:
        total:       Non-blank lines found in the source file.
        accepted:    Rows newly inserted into ``silver_events``.
        duplicate:   Valid rows skipped because ``event_id`` already existed.
        quarantined: Lines written to the quarantine directory (bad JSON +
                     envelope validation failures).
    """

    total: int
    accepted: int
    duplicate: int
    quarantined: int


def ingest_file(
    conn: duckdb.DuckDBPyConnection,
    file: FoundFile,
    *,
    quarantine_dir: Path,
    today: date | None = None,
) -> IngestStats:
    """Parse, validate, normalize, and upsert one JSONL file.

    Args:
        conn:          Open DuckDB connection (migrations already applied).
        file:          Settled :class:`~piper.discovery.FoundFile` to ingest.
        quarantine_dir: Root directory for quarantine output.
        today:         Override today's date for quarantine partitioning
                       (pass in tests to avoid wall-clock dependence).

    Returns:
        :class:`IngestStats` describing what happened.
    """
    good_lines, bad_lines = parse_jsonl_file(file.path)

    # Step 1 — quarantine unparseable lines
    for bad in bad_lines:
        quarantine_line(quarantine_dir, file.path, bad, today=today)

    # Step 2 — validate and normalize parseable lines
    rows: list[SilverRow] = []
    validation_errors = 0

    for parsed in good_lines:
        try:
            envelope = validate_envelope(parsed.data)
        except EnvelopeError as exc:
            quarantine_line(
                quarantine_dir,
                file.path,
                BadLine(
                    line_number=parsed.line_number,
                    raw_text=json.dumps(parsed.data, separators=(",", ":")),
                    reason=str(exc),
                ),
                today=today,
            )
            validation_errors += 1
            continue
        rows.append(
            SilverRow.from_envelope(
                envelope,
                source_file=file.path,
                source_line=parsed.line_number,
            )
        )

    # Step 3 — bulk upsert; count actually-inserted rows via pre/post diff
    if rows:
        pre = _silver_count(conn)
        conn.executemany(_INSERT_SQL, [r.as_params() for r in rows])
        post = _silver_count(conn)
        accepted = post - pre
    else:
        accepted = 0

    return IngestStats(
        total=len(good_lines) + len(bad_lines),
        accepted=accepted,
        duplicate=len(rows) - accepted,
        quarantined=len(bad_lines) + validation_errors,
    )
