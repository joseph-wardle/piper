#!/usr/bin/env bash
# run_ingest.sh — discover and ingest new telemetry JSONL files.
#
# Install as a cron job to run every 15 minutes:
#   */15 * * * *  /path/to/piper/scripts/run_ingest.sh >> /var/log/piper/ingest.log 2>&1
#
# Environment variables (override defaults in conf/settings.toml):
#   PIPER_PATHS__DATA_ROOT    — parent directory for warehouse, silver, state, quarantine
#   PIPER_PATHS__RAW_ROOT     — root of the pipeline telemetry spool (read-only)
#   PIPER_INGEST__SETTLE_SECONDS — files newer than this are skipped (default: 120)
#
# The script exits non-zero if piper itself exits non-zero, so cron will
# record the failure in the mail log (or log file if redirected).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Default paths — override via environment variables if needed.
export PIPER_PATHS__DATA_ROOT="${PIPER_PATHS__DATA_ROOT:-/groups/sandwich/05_production/.telemetry}"
export PIPER_PATHS__RAW_ROOT="${PIPER_PATHS__RAW_ROOT:-/groups/sandwich/05_production/.telemetry/raw}"

cd "$REPO_ROOT"
exec uv run piper ingest
