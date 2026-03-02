#!/usr/bin/env bash
# run_backfill.sh — re-ingest telemetry for a historical date range.
#
# Usage:
#   scripts/run_backfill.sh --start 2026-01-15 --end 2026-01-20
#   scripts/run_backfill.sh --start 2026-01-15 --end 2026-01-20 --force
#
# Options:
#   --start DATE   Inclusive start date (YYYY-MM-DD)   [required]
#   --end   DATE   Inclusive end date   (YYYY-MM-DD)   [required]
#   --force        Re-ingest files already in the manifest
#
# After backfilling, run run_materialize.sh to rebuild gold views.
#
# Environment variables:
#   PIPER_PATHS__DATA_ROOT  — parent directory for the warehouse
#   PIPER_PATHS__RAW_ROOT   — root of the pipeline telemetry spool

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

export PIPER_PATHS__DATA_ROOT="${PIPER_PATHS__DATA_ROOT:-/groups/sandwich/05_production/.telemetry}"
export PIPER_PATHS__RAW_ROOT="${PIPER_PATHS__RAW_ROOT:-/groups/sandwich/05_production/.telemetry/raw}"

cd "$REPO_ROOT"
exec uv run piper backfill "$@"
