#!/usr/bin/env bash
# run_materialize.sh — rebuild all silver and gold SQL views.
#
# Install as a cron job to run once per hour, a few minutes after ingest:
#   5 * * * *  /path/to/piper/scripts/run_materialize.sh >> /var/log/piper/materialize.log 2>&1
#
# Running materialize after every ingest keeps gold views current.
# All models use CREATE OR REPLACE VIEW, so this is safe to run at any time.
#
# Environment variables:
#   PIPER_PATHS__DATA_ROOT  — parent directory for the warehouse (must already exist)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

export PIPER_PATHS__DATA_ROOT="${PIPER_PATHS__DATA_ROOT:-/groups/sandwich/05_production/.telemetry}"
export PIPER_PATHS__RAW_ROOT="${PIPER_PATHS__RAW_ROOT:-/groups/sandwich/05_production/.telemetry/raw}"

cd "$REPO_ROOT"
exec uv run piper materialize
