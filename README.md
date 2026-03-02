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
        Gold KPI views  (DuckDB SQL, queried directly by Grafana)
          gold_publish_health_daily
          gold_render_health_daily
          gold_farm_pressure_daily
          gold_tool_reliability_daily
          gold_storage_growth_weekly
          gold_data_quality_daily
                │
                ▼
            Grafana  (6 operational dashboards, served via Docker Compose)
```

All state lives under a single `data_root` directory — no external services
required beyond DuckDB and Grafana.

---

## Quick Start

```bash
uv sync                          # install dependencies

uv run piper init                # create telemetry.duckdb + manifest DB
uv run piper ingest              # ingest new JSONL from the raw spool
uv run piper materialize         # rebuild all silver and gold models
uv run piper doctor              # check freshness, volume, and data quality
```

Backfill a date range (forces re-ingest, ignoring the manifest):

```bash
uv run piper backfill --start 2026-01-01 --end 2026-01-31
```

---

## Grafana

Start the dashboard stack with Docker Compose:

```bash
docker compose up -d
```

Grafana is available at <http://localhost:3000> (user: `admin`, pass: `piper`).
Anonymous read-only access is enabled by default.

The DuckDB datasource and all six dashboards are provisioned automatically
from `grafana/provisioning/`. The warehouse is mounted read-only at
`/var/piper/telemetry.duckdb` inside the container.

To point at a non-default warehouse location:

```bash
PIPER_DATA_ROOT=/path/to/.telemetry docker compose up -d
```

---

## Configuration

Defaults are declared in `conf/settings.toml`. Override any value with an
environment variable — use `PIPER_<SECTION>__<KEY>` (double underscore):

```bash
PIPER_PATHS__RAW_ROOT=/custom/spool  uv run piper ingest
PIPER_LOGGING__LEVEL=DEBUG           uv run piper ingest
PIPER_PRIVACY__MASK_USERS=true       uv run piper materialize
```

Copy `.env.example` to `.env` for a persistent local override file.
The example file documents every available key.

---

## Scheduling (cron)

`scripts/` contains cron-ready wrappers with sensible defaults:

```
scripts/run_ingest.sh       # run every 5 minutes
scripts/run_materialize.sh  # run every hour
scripts/run_backfill.sh     # ad-hoc backfill by date range
```

Example crontab:

```cron
*/5 * * * * /path/to/piper/scripts/run_ingest.sh
0   * * * * /path/to/piper/scripts/run_materialize.sh
```

---

## Development

```bash
uv sync              # install all deps including dev group
uv run pytest        # test suite (346 tests)
uv run ruff check    # lint
uv run ruff format   # format
uv run ty check src  # type check
```

CI runs the same four steps on every push and PR to `main`
(see `.github/workflows/ci.yml`).

---

## Storage Layout

```
<raw_root>/                                            ← pipeline writes here
  <host>/<user>/*.jsonl

<data_root>/
  silver/events/event_date=YYYY-MM-DD/event_type=*/   ← canonical Parquet
  silver/publish_usd/event_date=YYYY-MM-DD/
  silver/tool_events/event_date=YYYY-MM-DD/
  silver/tractor_job_spool/event_date=YYYY-MM-DD/
  silver/tractor_farm_snapshot/event_date=YYYY-MM-DD/
  silver/render_stats_summary/event_date=YYYY-MM-DD/
  silver/storage_scan_summary/event_date=YYYY-MM-DD/
  silver/storage_scan_bucket/event_date=YYYY-MM-DD/
  warehouse/telemetry.duckdb                           ← gold views live here
  state/ingest_manifest.duckdb                         ← per-file ingest state
  quarantine/invalid_jsonl/YYYY-MM-DD/                 ← rejected lines
  run_logs/YYYY-MM-DD/                                 ← per-run structured logs
```

`raw_root` and `data_root` are independently configurable. By default both
live under `/groups/sandwich/05_production/.telemetry/`.

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
