# Piper — convenience commands
# Run `just` (no arguments) to list all available recipes.

set shell := ["bash", "-euo", "pipefail", "-c"]

grafana_url := "http://localhost:3000"

# List available recipes
default:
    @just --list

# ── Data pipeline ─────────────────────────────────────────────────────────────

# Ingest new JSONL files from the telemetry spool
ingest:
    uv run piper ingest

# Rebuild silver and gold SQL views
materialize:
    uv run piper materialize

# Check data freshness and quality
doctor:
    uv run piper doctor

# Ingest then materialize — bring data fully up to date
update: ingest materialize

# ── Grafana ───────────────────────────────────────────────────────────────────

# Download the DuckDB Grafana plugin (first-time setup only)
plugin:
    scripts/setup_grafana_plugin.sh

# Start the Grafana container
up:
    podman compose up -d

# Stop the Grafana container
down:
    podman compose down

# Tail Grafana container logs
logs:
    podman compose logs -f

# Open the dashboard in the default browser
open:
    xdg-open {{grafana_url}}

# ── Combined ──────────────────────────────────────────────────────────────────

# Update data, start Grafana, and open the dashboard
launch: plugin update up
    @echo "Waiting for Grafana to start..."
    @sleep 3
    xdg-open {{grafana_url}}
