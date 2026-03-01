"""Pipeline health checks for ``piper doctor``.

Each check is a function that takes an open DuckDB connection and returns a
``CheckResult``.  All checks are safe to run on an empty warehouse (they
return FAIL or WARN with a meaningful message rather than raising).

Exit-code contract (enforced by the CLI):
  0 — all checks passed
  1 — one or more warnings, no failures
  2 — one or more failures
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import duckdb

Status = Literal["pass", "warn", "fail"]


@dataclass(frozen=True)
class CheckResult:
    name: str
    status: Status
    message: str
    hint: str = ""


# ---------------------------------------------------------------------------
# Individual checks
# ---------------------------------------------------------------------------


def check_freshness(conn: duckdb.DuckDBPyConnection) -> CheckResult:
    """Most recent event must have arrived within the last 48 hours.

    PASS  — most recent event ≤ 48 h ago
    WARN  — 48 h – 96 h ago
    FAIL  — older than 96 h, or no data at all
    """
    count_row = conn.execute("SELECT COUNT(*) FROM silver_events").fetchone()
    assert count_row is not None
    if int(count_row[0]) == 0:
        return CheckResult(
            name="freshness",
            status="fail",
            message="no events in silver_events",
            hint="run `piper ingest` to pull the latest telemetry",
        )

    # Compute age as a plain DOUBLE (hours) entirely in SQL to avoid returning
    # a TIMESTAMPTZ to Python, which requires the optional pytz package.
    age_row = conn.execute(
        "SELECT epoch(NOW() - MAX(occurred_at_utc)) / 3600.0 FROM silver_events"
    ).fetchone()
    assert age_row is not None
    age_hours = float(age_row[0])

    if age_hours <= 48:
        return CheckResult(
            name="freshness",
            status="pass",
            message=f"most recent event {age_hours:.1f} h ago",
        )
    if age_hours <= 96:
        return CheckResult(
            name="freshness",
            status="warn",
            message=f"most recent event {age_hours:.1f} h ago (threshold: 48 h)",
            hint="run `piper ingest` — pipeline may have missed recent files",
        )
    return CheckResult(
        name="freshness",
        status="fail",
        message=f"most recent event {age_hours:.1f} h ago (threshold: 96 h)",
        hint="check raw_root and run `piper ingest`",
    )


def check_volume(conn: duckdb.DuckDBPyConnection) -> CheckResult:
    """At least 10 events must have been ingested in the last 7 days.

    PASS  — ≥ 10 events
    WARN  — 1 – 9 events
    FAIL  — 0 events
    """
    row = conn.execute(
        "SELECT COUNT(*) FROM silver_events WHERE occurred_at_utc >= NOW() - INTERVAL '7 days'"
    ).fetchone()
    assert row is not None
    n = int(row[0])

    if n >= 10:
        return CheckResult(
            name="volume",
            status="pass",
            message=f"{n} events in the last 7 days",
        )
    if n > 0:
        return CheckResult(
            name="volume",
            status="warn",
            message=f"only {n} events in the last 7 days (floor: 10)",
            hint="confirm the pipeline is running and raw_root is correct",
        )
    return CheckResult(
        name="volume",
        status="fail",
        message="0 events in the last 7 days",
        hint="confirm the pipeline is running and run `piper ingest`",
    )


def check_invalid_rate(conn: duckdb.DuckDBPyConnection) -> CheckResult:
    """Quarantined-line rate from ingest_manifest must be ≤ 10 %.

    PASS  — ≤ 2 %
    WARN  — 2 % – 10 %
    FAIL  — > 10 % (or manifest indicates all lines were errors)
    """
    row = conn.execute("SELECT SUM(event_count), SUM(error_count) FROM ingest_manifest").fetchone()
    if row is None or (row[0] is None and row[1] is None):
        return CheckResult(
            name="invalid_rate",
            status="pass",
            message="no files ingested yet — skipping",
        )

    events = int(row[0] or 0)
    errors = int(row[1] or 0)
    total = events + errors
    if total == 0:
        return CheckResult(
            name="invalid_rate",
            status="pass",
            message="no lines processed yet — skipping",
        )

    rate = 100.0 * errors / total
    if rate <= 2.0:
        return CheckResult(
            name="invalid_rate",
            status="pass",
            message=f"{rate:.1f}% of lines quarantined ({errors}/{total})",
        )
    if rate <= 10.0:
        return CheckResult(
            name="invalid_rate",
            status="warn",
            message=f"{rate:.1f}% of lines quarantined (warn threshold: 2%)",
            hint="inspect quarantine/invalid_jsonl for malformed records",
        )
    return CheckResult(
        name="invalid_rate",
        status="fail",
        message=f"{rate:.1f}% of lines quarantined (fail threshold: 10%)",
        hint="investigate the upstream producer emitting malformed JSONL",
    )


def check_clock_skew(conn: duckdb.DuckDBPyConnection) -> CheckResult:
    """Events whose occurred_at_utc differs from ingested_at_utc by > 7 days.

    PASS  — no skewed events
    WARN  — 1 + events with skew 1 – 7 days
    FAIL  — 1 + events with skew > 7 days
    """
    fail_row = conn.execute(
        "SELECT COUNT(*) FROM silver_events "
        "WHERE ABS(epoch(ingested_at_utc) - epoch(occurred_at_utc)) > 7 * 86400"
    ).fetchone()
    assert fail_row is not None
    n_fail = int(fail_row[0])

    if n_fail > 0:
        return CheckResult(
            name="clock_skew",
            status="fail",
            message=f"{n_fail} event(s) with clock skew > 7 days",
            hint="check host clock synchronisation on field machines",
        )

    warn_row = conn.execute(
        "SELECT COUNT(*) FROM silver_events "
        "WHERE ABS(epoch(ingested_at_utc) - epoch(occurred_at_utc)) > 86400"
    ).fetchone()
    assert warn_row is not None
    n_warn = int(warn_row[0])

    if n_warn > 0:
        return CheckResult(
            name="clock_skew",
            status="warn",
            message=f"{n_warn} event(s) with clock skew > 1 day",
            hint="check host clock synchronisation on field machines",
        )

    return CheckResult(
        name="clock_skew",
        status="pass",
        message="no clock-skew anomalies detected",
    )


# ---------------------------------------------------------------------------
# Registry and runner
# ---------------------------------------------------------------------------

_ALL_CHECKS: dict[str, object] = {
    "freshness": check_freshness,
    "volume": check_volume,
    "invalid_rate": check_invalid_rate,
    "clock_skew": check_clock_skew,
}


def run_checks(
    conn: duckdb.DuckDBPyConnection,
    *,
    only: str = "",
) -> list[CheckResult]:
    """Run all registered checks (or just ``only`` if named) and return results.

    Args:
        conn: Open DuckDB connection.
        only: If non-empty, run only the check with this name.

    Raises:
        ValueError: If ``only`` names a check that does not exist.
    """
    if only:
        if only not in _ALL_CHECKS:
            raise ValueError(f"unknown check: {only!r}.  Known: {sorted(_ALL_CHECKS)}")
        return [_ALL_CHECKS[only](conn)]  # type: ignore[operator]
    return [fn(conn) for fn in _ALL_CHECKS.values()]  # type: ignore[misc]
