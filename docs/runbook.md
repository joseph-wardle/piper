# Piper Operations Runbook

Piper is the diagnostics dashboard for the sandwich USD production pipeline.
This runbook covers day-to-day operations, scheduled maintenance, and incident
recovery.  An on-call engineer with read access to the production telemetry
spool and the piper repository should be able to execute every procedure
described here.

---

## Table of Contents

1. [Architecture overview](#architecture-overview)
2. [Scheduled jobs](#scheduled-jobs)
3. [Daily health check](#daily-health-check)
4. [Starting Grafana](#starting-grafana)
5. [Ingest failures and recovery](#ingest-failures-and-recovery)
6. [Backfilling a date range](#backfilling-a-date-range)
7. [Quarantine review](#quarantine-review)
8. [Warehouse maintenance](#warehouse-maintenance)
9. [Grafana datasource issues](#grafana-datasource-issues)
10. [Environment variables reference](#environment-variables-reference)

---

## Architecture overview

```
Pipeline hosts → JSONL spool → piper ingest → silver_events (DuckDB)
                                    ↓
                             piper materialize → gold views (DuckDB)
                                    ↓
                                 Grafana (Docker) → dashboards
```

| Layer   | Location                                                        | Managed by       |
|---------|-----------------------------------------------------------------|------------------|
| Raw     | `/groups/sandwich/05_production/.telemetry/raw/<host>/<user>/` | pipeline (r/o)   |
| Silver  | `$DATA_ROOT/warehouse/telemetry.duckdb` (silver_events table)   | piper ingest     |
| Gold    | same DuckDB file — views over silver_events                     | piper materialize|
| Grafana | Docker container on the analytics workstation                    | docker compose   |

---

## Scheduled jobs

Two cron jobs keep the warehouse current.  Both scripts live in `scripts/` and
must be installed on the machine that mounts the production NFS share.

```cron
# Ingest new telemetry every 15 minutes
*/15 * * * *  /path/to/piper/scripts/run_ingest.sh  >> /var/log/piper/ingest.log 2>&1

# Rebuild gold views every hour (after ingest has run)
5  * * * *    /path/to/piper/scripts/run_materialize.sh >> /var/log/piper/materialize.log 2>&1
```

Edit `scripts/run_ingest.sh` and `scripts/run_materialize.sh` to set the
correct `PIPER_DATA_ROOT` and `PIPER_PATHS__RAW_ROOT` before installing.

---

## Daily health check

Run this first thing each morning to verify the pipeline is healthy:

```bash
piper doctor
```

Expected output when everything is fine:

```
  freshness    PASS  last event 0.4 h ago
  volume       PASS  1 203 events in the last 7 days
  invalid_rate PASS  invalid-line rate 0.0%
  clock_skew   PASS  max clock skew 0.1 h

  all checks passed
```

Exit codes:

| Code | Meaning                         |
|------|---------------------------------|
|  0   | All checks passed               |
|  1   | One or more warnings            |
|  2   | One or more failures — act now  |

### Interpreting warnings and failures

| Check        | Warn                      | Fail                        | Common cause                     |
|--------------|---------------------------|-----------------------------|----------------------------------|
| freshness    | last event > 48 h ago     | last event > 96 h ago       | ingest cron stopped / NFS down   |
| volume       | < 7 events in 7 days      | 0 events in 7 days          | spool dir empty / wrong raw_root |
| invalid_rate | error rate > 2 %          | error rate > 10 %           | schema change in pipeline code   |
| clock_skew   | max skew > 1 day          | max skew > 7 days           | host clock drift / wrong TZ      |

---

## Starting Grafana

From the repository root:

```bash
# Start (or restart) the Grafana container
docker compose up -d

# Confirm it is running
docker compose ps

# Tail logs for startup errors
docker compose logs -f grafana
```

Grafana is available at **http://localhost:3000**.  Default credentials: `admin / piper`.

The DuckDB datasource and all six dashboards are provisioned automatically
from `grafana/provisioning/`.  If they do not appear within 30 seconds,
check the Grafana logs for plugin errors.

### Override the data root

If the warehouse is not at the default path, set `PIPER_DATA_ROOT` before
starting the container:

```bash
PIPER_DATA_ROOT=/custom/path docker compose up -d
```

---

## Ingest failures and recovery

### Symptoms

- `piper doctor` reports freshness FAIL
- Cron log shows errors from `run_ingest.sh`
- New events are not appearing in Grafana

### Procedure

1. Check NFS mount:

   ```bash
   ls "$PIPER_PATHS__RAW_ROOT"
   ```

2. Run ingest manually with verbose output:

   ```bash
   PIPER_LOGGING__FORMAT=text PIPER_LOGGING__LEVEL=DEBUG piper ingest
   ```

3. If the lock file is stale (piper crashed mid-run):

   ```bash
   # Find and remove the stale lock
   ls "$PIPER_DATA_ROOT/state/"
   rm "$PIPER_DATA_ROOT/state/piper.lock"
   piper ingest
   ```

4. If events are being quarantined at an unexpected rate, see
   [Quarantine review](#quarantine-review).

---

## Backfilling a date range

Use `piper backfill` to re-ingest raw JSONL files whose mtime falls within a
given window.  This is useful after recovering from a pipeline gap or after
fixing a data quality issue.

```bash
# Re-ingest all files touched in the given window
piper backfill --start 2026-01-15 --end 2026-01-20

# Force re-ingest even if already in the manifest
piper backfill --start 2026-01-15 --end 2026-01-20 --force
```

After backfilling, rebuild the gold views:

```bash
piper materialize
```

---

## Quarantine review

Malformed or schema-invalid JSONL lines are written to:

```
$PIPER_DATA_ROOT/quarantine/<YYYY-MM-DD>/<original_filename>.jsonl
```

To inspect the most recent quarantine files:

```bash
ls -lt "$PIPER_DATA_ROOT/quarantine/" | head
cat "$PIPER_DATA_ROOT/quarantine/2026-03-01/some_host.jsonl"
```

Each quarantined line is the raw JSON that failed validation, making it easy
to identify the offending field.  If many lines are quarantined from the same
host, check whether the pipeline on that host was recently updated.

---

## Warehouse maintenance

### Check warehouse size

```bash
du -sh "$PIPER_DATA_ROOT/warehouse/telemetry.duckdb"
```

### Re-run schema migrations (after a piper upgrade)

```bash
piper init
```

`piper init` is idempotent — it applies only pending migrations and is safe to
re-run at any time.

### Rebuild all views after an upgrade

```bash
piper materialize
```

---

## Grafana datasource issues

### DuckDB plugin not installed

If the Grafana UI shows "Plugin not found", the container may have started
before the plugin finished installing.  Restart it:

```bash
docker compose restart grafana
docker compose logs -f grafana   # wait for "HTTP Server Listen"
```

### Datasource returns no data

1. Confirm the warehouse file is mounted correctly:

   ```bash
   docker compose exec grafana ls -lh /var/piper/telemetry.duckdb
   ```

2. Confirm gold views exist (they are rebuilt by `piper materialize`):

   ```bash
   piper materialize
   ```

3. If the file is missing, run the full pipeline:

   ```bash
   piper init
   piper ingest
   piper materialize
   ```

---

## Environment variables reference

All variables follow the pattern `PIPER_<SECTION>__<KEY>` (double underscore
for nested fields).

| Variable                         | Default                                                   | Description                              |
|----------------------------------|-----------------------------------------------------------|------------------------------------------|
| `PIPER_PATHS__RAW_ROOT`          | `/groups/sandwich/05_production/.telemetry/raw`           | Root of the pipeline telemetry spool     |
| `PIPER_PATHS__DATA_ROOT`         | `/groups/sandwich/05_production/.telemetry`               | Root of all piper-managed output         |
| `PIPER_INGEST__SETTLE_SECONDS`   | `120`                                                     | Files newer than this are skipped        |
| `PIPER_INGEST__QUARANTINE_MAX_PER_DAY` | `1000`                                              | Max quarantine files per day             |
| `PIPER_LOGGING__LEVEL`           | `INFO`                                                    | Log level (`DEBUG`, `INFO`, `WARNING`, …)|
| `PIPER_LOGGING__FORMAT`          | `json`                                                    | Log format (`json` or `text`)            |
| `PIPER_PRIVACY__MASK_USERS`      | `false`                                                   | Hash `host_user` in gold views           |
| `PIPER_CONFIG_FILE`              | `conf/settings.toml` (repo-relative)                      | Override config file path                |
| `PIPER_DATA_ROOT`                | (see `PIPER_PATHS__DATA_ROOT`)                            | Shorthand used by Docker Compose only    |

---

*Maintained by the pipeline team.  File issues at the piper repository.*
