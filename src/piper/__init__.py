"""piper — diagnostics dashboard for the sandwich USD production pipeline.

Ingests telemetry JSONL from the shared production spool, normalises events
through a Raw → Silver → Gold lakehouse, and serves Grafana-ready metrics
from DuckDB.

Pipeline contract: schema_version=1.0, 18 stable event types.
"""

__version__ = "0.1.0"
