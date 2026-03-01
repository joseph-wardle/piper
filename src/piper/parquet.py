"""Silver Parquet exports: silver_events and all 7 domain views.

Two public functions:

``export_silver_events(conn, silver_dir)``
    Writes ``silver_events`` to Hive-partitioned Parquet at
    ``silver_dir/silver_events/``, partitioned by ``event_date`` then
    ``event_type``.

``export_silver_domain(conn, silver_dir)``
    Writes each of the 7 silver domain views to its own Parquet dataset at
    ``silver_dir/<view_name>/``, partitioned by ``event_date`` only.

Both functions always rebuild the target directory from scratch so
re-export is idempotent: running twice with identical data produces
identical files.

Layout example::

    silver_dir/
      silver_events/
        event_date=2026-02-15/
          event_type=dcc.launch/
            data_0.parquet
      silver_publish_usd/
        event_date=2026-02-15/
          data_0.parquet
      silver_tool_events/
        event_date=2026-02-15/
          data_0.parquet
      ...
"""

from __future__ import annotations

import shutil
from pathlib import Path

import duckdb

# Ordered list of domain view names exported by export_silver_domain().
_DOMAIN_VIEWS = [
    "silver_publish_usd",
    "silver_tool_events",
    "silver_tractor_job_spool",
    "silver_tractor_farm_snapshot",
    "silver_render_stats_summary",
    "silver_storage_scan_summary",
    "silver_storage_scan_bucket",
]


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

    out_dir = silver_dir / "silver_events"
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


def export_silver_domain(
    conn: duckdb.DuckDBPyConnection,
    silver_dir: Path,
) -> dict[str, int]:
    """Export all 7 silver domain views to Parquet, partitioned by ``event_date``.

    Each view is written to ``silver_dir/<view_name>/``.  All destination
    directories are rebuilt from scratch so the export is idempotent.

    Args:
        conn:       Open DuckDB connection with silver domain views applied.
        silver_dir: Root directory for Parquet output (``paths.silver_dir``).

    Returns:
        Mapping of ``{view_name: row_count}`` for every domain view exported.
    """
    return {view: _export_view(conn, view, silver_dir) for view in _DOMAIN_VIEWS}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _export_view(
    conn: duckdb.DuckDBPyConnection,
    view: str,
    silver_dir: Path,
) -> int:
    """Export one view to Parquet under silver_dir, partitioned by event_date."""
    row = conn.execute(f"SELECT COUNT(*) FROM {view}").fetchone()
    assert row is not None
    n_rows = int(row[0])

    out_dir = silver_dir / view
    if out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True)

    if n_rows > 0:
        conn.execute(f"""
            COPY (SELECT * FROM {view} ORDER BY event_date)
            TO '{out_dir}'
            (FORMAT PARQUET, PARTITION_BY (event_date))
        """)

    return n_rows
