# Changelog

## [1.0.0] — 2026-03-01

Initial production release of piper, the diagnostics dashboard for the
sandwich USD production pipeline.

### Added

**Ingestion pipeline**
- JSONL discovery with configurable settle-window to avoid reading files mid-write
- Event validation against the `schema_version=1.0` contract (18 event types)
- Canonical `silver_events` load into DuckDB with upsert deduplication
- Ingest manifest tracking and PID-file lock to prevent concurrent runs
- Per-day quarantine writer for malformed or schema-invalid lines
- `piper ingest` CLI command with `--dry-run` and `--limit` options
- `piper backfill` CLI command for forced re-ingestion of historical date ranges

**Silver domain models** (7 SQL views over silver_events)
- `silver_publish_usd` — all USD publish event types (5 types)
- `silver_tool_events` — DCC tool invocations (8 types)
- `silver_tractor_job_spool` — Tractor job submissions
- `silver_tractor_farm_snapshot` — farm health polls
- `silver_render_stats_summary` — per-job render metrics
- `silver_storage_scan_summary` — storage root totals
- `silver_storage_scan_bucket` — per-bucket storage scans
- Partitioned Parquet export for each domain view

**Gold metric models** (6 SQL views)
- `gold_publish_health_daily` — success rate, duration, output bytes
- `gold_render_health_daily` — success rate, CPU hours, failed frames
- `gold_farm_pressure_daily` — peak active blades and errored jobs
- `gold_tool_reliability_daily` — success rate and duration by tool type
- `gold_storage_growth_weekly` — latest size and file count by bucket
- `gold_data_quality_daily` — cross-domain error rate and active users
- `piper materialize` CLI command with `--model` flag for targeted rebuilds

**Pipeline health checks**
- `piper doctor` command with four checks: freshness, volume, invalid_rate, clock_skew
- Aligned PASS / WARN / FAIL table output
- Exit codes: 0 = all pass, 1 = warnings, 2 = failures

**Metrics catalog**
- `conf/metrics_catalog.yml` — 14 metrics across all 6 gold models
- `piper catalog list` command with `--model` filter

**Grafana provisioning**
- `docker-compose.yml` — single-container Grafana stack with DuckDB plugin
- Six dashboard stubs (one per gold model) provisioned from JSON files
- DuckDB datasource provisioned from `grafana/provisioning/datasources/duckdb.yaml`

**Operations**
- `scripts/run_ingest.sh` — cron-ready ingest wrapper
- `scripts/run_materialize.sh` — cron-ready materialize wrapper
- `scripts/run_backfill.sh` — backfill helper with pass-through arguments
- `docs/runbook.md` — complete on-call operations guide

**Test suite** — 426 tests
- Unit tests for every module
- Integration tests covering the full ingest → materialize → gold pipeline
- Time-stable doctor tests using relative timestamps
