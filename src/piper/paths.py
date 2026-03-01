"""Filesystem path derivations — all piper-managed locations in one place.

Every path that piper reads from or writes to is derived here.
Nothing writes to an arbitrary location; all paths flow through ProjectPaths.

Layout under data_root:
  warehouse/                     ← DuckDB files (telemetry.duckdb)
  silver/                        ← partitioned Parquet datasets
  state/                         ← ingest manifest DB
  quarantine/invalid_jsonl/      ← rejected JSONL lines, organised by date
  run_logs/                      ← per-run structured logs, organised by date
"""

from dataclasses import dataclass
from pathlib import Path

from piper.config import Settings


@dataclass(frozen=True)
class ProjectPaths:
    """All filesystem paths used by piper, derived from settings.

    Construct with ``ProjectPaths.from_settings(settings)`` rather than
    directly, so that the derivation logic stays in one place.
    """

    # Source data — pipeline-written, treated as read-only by piper.
    raw_root: Path

    # Managed output root — everything piper writes lives here.
    data_root: Path

    # Derived from data_root.
    warehouse_dir: Path  # DuckDB database files
    silver_dir: Path  # partitioned Parquet datasets
    state_dir: Path  # ingest manifest + lock files
    quarantine_dir: Path  # invalid_jsonl/<YYYY-MM-DD>/ subdirs created at runtime
    run_logs_dir: Path  # <YYYY-MM-DD>/ subdirs created at runtime

    @classmethod
    def from_settings(cls, settings: Settings) -> "ProjectPaths":
        """Derive all paths from the resolved settings."""
        data = settings.paths.data_root
        return cls(
            raw_root=settings.paths.raw_root,
            data_root=data,
            warehouse_dir=data / "warehouse",
            silver_dir=data / "silver",
            state_dir=data / "state",
            quarantine_dir=data / "quarantine" / "invalid_jsonl",
            run_logs_dir=data / "run_logs",
        )

    def ensure_output_dirs(self) -> None:
        """Create all piper-managed output directories (idempotent).

        Called by ``piper init``.  Does not touch raw_root — that directory
        is owned by the pipeline, not piper.
        """
        for path in (
            self.warehouse_dir,
            self.silver_dir,
            self.state_dir,
            self.quarantine_dir,
            self.run_logs_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)
