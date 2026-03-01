"""Silver Parquet export: silver_events → partitioned Parquet dataset.

``export_silver_events(conn, silver_dir)`` writes the entire ``silver_events``
table to Hive-partitioned Parquet under ``silver_dir/silver_events/``.

Partition layout::

    silver_dir/
      silver_events/
        event_date=2026-02-15/
          event_type=dcc.launch/
            data_0.parquet
          event_type=playblast.create/
            data_0.parquet
        event_date=2026-02-16/
          ...

The partition key ``event_date`` is derived from ``occurred_at_utc::DATE``
so downstream consumers can prune reads by date without scanning the full
dataset.

The output directory is always rebuilt from scratch, so re-export is
idempotent: running twice with identical data produces identical files.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import duckdb

# Name of the Parquet dataset directory written under silver_dir.
_DATASET = "silver_events"


def export_silver_events(
    conn: duckdb.DuckDBPyConnection,
    silver_dir: Path,
) -> int:
    """Export all ``silver_events`` rows to Hive-partitioned Parquet.

    Partitions first by ``event_date`` (``occurred_at_utc::DATE``) then by
    ``event_type``.  The destination directory is always rebuilt wholesale,
    so the function is safe to call on every pipeline run.

    Args:
        conn:       Open DuckDB connection with ``silver_events`` populated.
        silver_dir: Root directory for Parquet output (``paths.silver_dir``).

    Returns:
        Number of rows exported.  Returns 0 if ``silver_events`` is empty
        (the output directory is still created but contains no Parquet files).
    """
    row = conn.execute("SELECT COUNT(*) FROM silver_events").fetchone()
    assert row is not None  # COUNT(*) always returns one row
    n_rows = int(row[0])

    out_dir = silver_dir / _DATASET
    if out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True)

    if n_rows > 0:
        conn.execute(f"""
            COPY (
                SELECT *, occurred_at_utc::DATE AS event_date
                FROM silver_events
                ORDER BY event_date, event_type, event_id
            ) TO '{out_dir}'
            (FORMAT PARQUET, PARTITION_BY (event_date, event_type))
        """)

    return n_rows
