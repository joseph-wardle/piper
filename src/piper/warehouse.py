"""DuckDB warehouse connection and schema migration.

``open_warehouse(paths)`` opens or creates ``telemetry.duckdb``.
``run_migrations(conn)``  brings the schema up to date.

Typical usage in ``piper init``:

    paths = ProjectPaths.from_settings(settings)
    paths.ensure_output_dirs()

    conn = open_warehouse(paths)
    n = run_migrations(conn)
    conn.close()
"""

from __future__ import annotations

from pathlib import Path

import duckdb

from piper.paths import ProjectPaths

# Filename of the warehouse database inside paths.warehouse_dir.
WAREHOUSE_FILE = "telemetry.duckdb"

# SQL directories bundled with this package.
_SQL_DIR = Path(__file__).parent / "sql" / "schema"
_SQL_SILVER_DIR = Path(__file__).parent / "sql" / "silver"
_SQL_GOLD_DIR = Path(__file__).parent / "sql" / "gold"


def open_warehouse(paths: ProjectPaths) -> duckdb.DuckDBPyConnection:
    """Open (or create) the warehouse database and return a connection.

    The warehouse directory must already exist — call
    ``paths.ensure_output_dirs()`` before this function.  The caller is
    responsible for closing the connection when done.
    """
    db_path = paths.warehouse_dir / WAREHOUSE_FILE
    return duckdb.connect(str(db_path))


def run_migrations(conn: duckdb.DuckDBPyConnection) -> int:
    """Apply any pending schema migrations and return the count applied.

    Safe to call on every startup: already-applied migrations are skipped.
    """
    from piper.sql_runner import apply_pending_migrations

    return apply_pending_migrations(conn, _SQL_DIR)


def run_silver_views(conn: duckdb.DuckDBPyConnection) -> None:
    """(Re-)apply all silver domain view definitions.

    Safe to call at any time: uses ``CREATE OR REPLACE VIEW``.
    """
    from piper.sql_runner import apply_views

    apply_views(conn, _SQL_SILVER_DIR)


def run_gold_views(conn: duckdb.DuckDBPyConnection) -> None:
    """(Re-)apply all gold metric view definitions.

    Safe to call at any time: uses ``CREATE OR REPLACE VIEW``.
    """
    from piper.sql_runner import apply_views

    apply_views(conn, _SQL_GOLD_DIR)
