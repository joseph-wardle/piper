"""Ingest manifest: tracks which files have already been processed.

``is_already_ingested(conn, file)`` checks whether a FoundFile's exact
fingerprint (path + mtime + size) is recorded in ``ingest_manifest``.
``mark_ingested(conn, file, *, event_count, error_count)`` upserts the
file record after successful ingestion so that the next run skips it.

The fingerprint includes both mtime and size so that a corrected file
(same path, different content) is detected and re-ingested automatically.
"""

from __future__ import annotations

import duckdb

from piper.discovery import FoundFile


def is_already_ingested(conn: duckdb.DuckDBPyConnection, file: FoundFile) -> bool:
    """Return True if *file*'s exact fingerprint is already in the manifest.

    Matches on path, mtime, and size.  A file at the same path with a
    different mtime or size is treated as a new version and returns False.
    """
    row = conn.execute(
        """
        SELECT 1 FROM ingest_manifest
        WHERE file_path = ? AND file_mtime = ? AND file_size = ?
        """,
        [str(file.path), file.mtime, file.size],
    ).fetchone()
    return row is not None


def mark_ingested(
    conn: duckdb.DuckDBPyConnection,
    file: FoundFile,
    *,
    event_count: int,
    error_count: int,
) -> None:
    """Record *file* as fully ingested (upsert by file_path).

    If the same path was previously recorded with a different fingerprint
    (file was corrected and re-ingested), the row is updated in place.
    """
    conn.execute(
        """
        INSERT INTO ingest_manifest
            (file_path, file_mtime, file_size, event_count, error_count)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT (file_path) DO UPDATE SET
            file_mtime      = excluded.file_mtime,
            file_size       = excluded.file_size,
            ingested_at_utc = now(),
            event_count     = excluded.event_count,
            error_count     = excluded.error_count
        """,
        [str(file.path), file.mtime, file.size, event_count, error_count],
    )
