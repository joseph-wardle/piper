"""SQL migration runner for the piper warehouse.

``apply_pending_migrations(conn, sql_dir)`` is the single entry point.

It bootstraps a ``schema_migrations`` tracking table on first run, then
applies every ``*.sql`` file in ``sql_dir`` that has not already been
recorded, in lexicographic filename order.  The file stem (e.g. ``001_init``)
is stored as the version key so the runner is safe to call on every startup.
"""

from __future__ import annotations

from pathlib import Path

import duckdb

# DDL to bootstrap the version-tracking table.
# This is executed before any migration files are read so it is never managed
# by the migration system itself.
_BOOTSTRAP = """
    CREATE TABLE IF NOT EXISTS schema_migrations (
        version        TEXT PRIMARY KEY,
        applied_at_utc TIMESTAMPTZ NOT NULL DEFAULT current_timestamp
    )
"""


def apply_pending_migrations(
    conn: duckdb.DuckDBPyConnection,
    sql_dir: Path,
) -> int:
    """Apply pending migrations from ``sql_dir`` and return the count applied.

    Migration files are ``*.sql`` files with a sortable numeric prefix
    (``001_init.sql``, ``002_add_index.sql``, …).  The file stem is used as
    the unique version key stored in ``schema_migrations``.

    Args:
        conn:    Open, writable DuckDB connection.
        sql_dir: Directory containing ordered ``*.sql`` migration files.

    Returns:
        Number of newly applied migrations (0 if schema is already current).
    """
    conn.execute(_BOOTSTRAP)
    applied = _applied_versions(conn)
    pending = sorted(p for p in sql_dir.glob("*.sql") if p.stem not in applied)

    for sql_file in pending:
        _execute_sql_file(conn, sql_file.read_text())
        conn.execute(
            "INSERT INTO schema_migrations (version) VALUES (?)", [sql_file.stem]
        )

    return len(pending)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _applied_versions(conn: duckdb.DuckDBPyConnection) -> set[str]:
    return {row[0] for row in conn.execute("SELECT version FROM schema_migrations").fetchall()}


def _execute_sql_file(conn: duckdb.DuckDBPyConnection, sql: str) -> None:
    """Execute a SQL file containing one or more semicolon-terminated statements."""
    for stmt in sql.split(";"):
        stmt = stmt.strip()
        if stmt:
            conn.execute(stmt)
