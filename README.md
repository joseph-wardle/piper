# piper

Diagnostics dashboard for the **sandwich** USD production pipeline.

Reads telemetry JSONL from the shared production spool, normalises events
through a Raw → Silver → Gold lakehouse, and serves operational metrics
to Grafana via DuckDB.

---

## Architecture

```
Pipeline JSONL  (raw spool — written by 50+ artists across many machines)
     │
     ▼  piper ingest
Silver Events   (canonical DuckDB table + partitioned Parquet, dedupe by event_id)
     │
     ├──▶ silver/publish_usd            publish.*.usd events
     ├──▶ silver/tool_events            dcc.launch, file.*, shot.setup, playblast, build, texture
     ├──▶ silver/tractor_job_spool      tractor.job.spool events
     ├──▶ silver/tractor_farm_snapshot  farm pressure time-series
     ├──▶ silver/render_stats_summary   per-job render diagnostics
     ├──▶ silver/storage_scan_summary   storage audit summary
     └──▶ silver/storage_scan_bucket    per-bucket storage detail
                │
                ▼  piper materialize
        Gold KPI models   (DuckDB SQL views queried directly by Grafana)
                │
                ▼
            Grafana         (6 operational dashboards)
```

All state lives under a single `data_root` directory alongside the raw spool —
no external services required beyond DuckDB and Grafana.

---

## Quick Start

```bash
# Install dependencies
uv sync

# Initialise the warehouse (creates telemetry.duckdb + ingest manifest DB)
uv run piper init

# Run a single ingest pass
uv run piper ingest

# Rebuild all silver and gold models
uv run piper materialize

# Check data freshness and quality
uv run piper doctor
```

---

## Configuration

Copy `.env.example` to `.env` and adjust for your environment.  Every key
has a default declared in `conf/settings.toml`; the `.env.example` file
documents each override.

---

## Development

```bash
uv sync                  # install all deps (including dev group)
uv run pytest            # run test suite
uv run ruff check .      # lint
uv run ruff format .     # format
uv run ty check src      # type check
```

---

## Storage Layout

```
<data_root>/
  raw/<host>/<user>/*.jsonl                              ← pipeline writes here
  silver/events/event_date=YYYY-MM-DD/event_type=*/     ← canonical Parquet
  silver/publish_usd/event_date=YYYY-MM-DD/
  silver/tool_events/event_date=YYYY-MM-DD/
  silver/tractor_job_spool/event_date=YYYY-MM-DD/
  silver/tractor_farm_snapshot/event_date=YYYY-MM-DD/
  silver/render_stats_summary/event_date=YYYY-MM-DD/
  silver/storage_scan_summary/event_date=YYYY-MM-DD/
  silver/storage_scan_bucket/event_date=YYYY-MM-DD/
  warehouse/telemetry.duckdb                            ← gold models live here
  state/ingest_manifest.duckdb                          ← per-file ingest state
  quarantine/invalid_jsonl/YYYY-MM-DD/                  ← rejected lines
  run_logs/YYYY-MM-DD/                                  ← per-run structured logs
```

---

## Pipeline Contract

Source: `schema_version=1.0` telemetry from `/groups/sandwich/.pipeline`.

| Group   | Event Types |
|---------|-------------|
| Publish | `publish.asset.usd`, `publish.anim.usd`, `publish.camera.usd`, `publish.customanim.usd`, `publish.previs_asset.usd` |
| Tool    | `dcc.launch`, `file.open`, `file.create`, `shot.setup`, `playblast.create`, `build.houdini.component`, `texture.export.substance`, `texture.convert.tex` |
| Farm    | `tractor.job.spool`, `tractor.farm.snapshot` |
| Render  | `render.stats.summary` |
| Storage | `storage.scan.summary`, `storage.scan.bucket` |
