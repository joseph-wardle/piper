"""Pydantic model for the raw telemetry event envelope (schema v1.0).

Mirrors the nested JSON structure emitted by the sandwich pipeline exactly.
No field is flattened or renamed here — that transformation happens at the
silver layer when events are written to DuckDB and Parquet.

Real event sample (playblast.create, 2026-03-01):

    {
        "schema_version": "1.0",
        "event_id": "bfc41fdd-...",
        "event_type": "playblast.create",
        "occurred_at_utc": "2026-03-01T02:41:55Z",
        "status": "success",
        "pipeline": {"name": "sandwich-pipeline", "dcc": "maya"},
        "host": {"hostname": "samus.cs.byu.edu", "os": "Linux",
                 "os_release": "5.14.0-...", "pid": 648681, "user": "rees23"},
        "session": {"session_id": "53fe7b...", "action_id": "5e440a..."},
        "payload": {"preset": "web", "frame_start": 1001, ...},
        "metrics": {"duration_ms": 1294, "output_size_bytes": 57584654},
        "scope": {"shot": "custom"}
    }
"""

from typing import Any, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict


class PipelineInfo(BaseModel):
    model_config = ConfigDict(extra="ignore")

    name: str
    # dcc is absent for background collectors (tractor_poll, storage_scan, etc.)
    dcc: str | None = None


class HostInfo(BaseModel):
    model_config = ConfigDict(extra="ignore")

    hostname: str
    user: str
    os: str | None = None
    os_release: str | None = None
    pid: int | None = None


class SessionInfo(BaseModel):
    model_config = ConfigDict(extra="ignore")

    session_id: str
    action_id: str | None = None


class ScopeInfo(BaseModel):
    """Optional project context attached to events; all fields are sparse.

    Not every event carries scope — background collectors (storage_scan,
    tractor_poll) typically have no scope at all.
    """

    model_config = ConfigDict(extra="ignore")

    show: str | None = None
    sequence: str | None = None
    shot: str | None = None
    asset: str | None = None
    department: str | None = None
    task: str | None = None


class ErrorInfo(BaseModel):
    """Present only when status='error'.  Both fields are technically optional
    because the pipeline emits them on a best-effort basis."""

    model_config = ConfigDict(extra="ignore")

    code: str | None = None
    message: str | None = None


class Envelope(BaseModel):
    """Complete parsed representation of one telemetry event.

    All sub-models use ``extra="ignore"`` so that fields added by future
    pipeline versions are silently dropped rather than causing parse errors.
    """

    model_config = ConfigDict(extra="ignore")

    # ── Identity ─────────────────────────────────────────────────────────────
    schema_version: str
    event_id: str
    event_type: str
    occurred_at_utc: AwareDatetime  # must be timezone-aware; Z / +00:00 required
    status: Literal["success", "error", "warning", "info"]

    # ── Context (always present) ──────────────────────────────────────────────
    pipeline: PipelineInfo
    host: HostInfo
    session: SessionInfo

    # ── Variable content (optional, event-type specific) ─────────────────────
    payload: dict[str, Any] = {}  # event-specific structured data
    metrics: dict[str, Any] = {}  # optional numeric measurements
    scope: ScopeInfo = ScopeInfo()  # project context (show/sequence/shot/asset…)
    error: ErrorInfo | None = None  # populated only when status='error'
