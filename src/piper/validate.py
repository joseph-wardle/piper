"""Telemetry envelope validation and normalization.

``validate_envelope(raw)`` is the single entry point.  It converts a raw dict
(parsed from one JSONL line) into a typed ``Envelope`` or raises a typed error.

Error hierarchy (all inherit from ValueError for easy catch-all handling):

    EnvelopeError
    ├── MissingFieldError   — a required field is absent
    ├── InvalidFieldError   — a field is present but fails type/constraint checks
    └── ClockSkewError      — timestamp is implausibly far in the future

Unknown event_type is intentionally NOT an error here.  Unknown types are
accepted by the validator, stored in silver_events, and surfaced as a data
quality signal by ``piper doctor``.  This means piper keeps working when the
pipeline adds new event types before piper is updated.
"""

from datetime import UTC, datetime, timedelta
from typing import Any

from pydantic import ValidationError as _PydanticError

from piper.models.envelope import Envelope

# ---------------------------------------------------------------------------
# Known event types (schema v1.0, 18 total)
# ---------------------------------------------------------------------------

KNOWN_EVENT_TYPES: frozenset[str] = frozenset(
    {
        # Publish
        "publish.asset.usd",
        "publish.anim.usd",
        "publish.camera.usd",
        "publish.customanim.usd",
        "publish.previs_asset.usd",
        # Tool
        "dcc.launch",
        "file.open",
        "file.create",
        "shot.setup",
        "playblast.create",
        "build.houdini.component",
        "texture.export.substance",
        "texture.convert.tex",
        # Farm
        "tractor.job.spool",
        "tractor.farm.snapshot",
        # Render
        "render.stats.summary",
        # Storage
        "storage.scan.summary",
        "storage.scan.bucket",
    }
)

# Events timestamped more than this far in the future are rejected.
# One hour tolerates NTP drift and minor clock-sync lag on artist machines.
CLOCK_SKEW_TOLERANCE: timedelta = timedelta(hours=1)


# ---------------------------------------------------------------------------
# Typed error classes
# ---------------------------------------------------------------------------


class EnvelopeError(ValueError):
    """Base class for all telemetry envelope validation errors."""


class MissingFieldError(EnvelopeError):
    """A required field is absent from the event envelope."""


class InvalidFieldError(EnvelopeError):
    """A field value is present but fails type or constraint checks."""


class ClockSkewError(EnvelopeError):
    """The event timestamp is implausibly far in the future.

    Indicates a clock-sync failure on the source host.  The event is
    quarantined rather than ingested to avoid polluting time-series metrics.
    """


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def validate_envelope(
    raw: dict[str, Any],
    *,
    now: datetime | None = None,
) -> Envelope:
    """Parse and validate a raw event dict into a typed ``Envelope``.

    Args:
        raw:  Parsed dict from one JSONL line.
        now:  Override the current UTC time used for clock-skew checks.
              Defaults to ``datetime.now(UTC)``.  Pass an explicit value
              in tests to avoid depending on wall-clock time.

    Returns:
        A fully validated ``Envelope`` instance.

    Raises:
        MissingFieldError: A required field is absent.
        InvalidFieldError: A field value is invalid (wrong type, bad format,
                           unknown Literal value such as an unseen status).
        ClockSkewError:    ``occurred_at_utc`` is more than
                           ``CLOCK_SKEW_TOLERANCE`` in the future.
    """
    try:
        envelope = Envelope.model_validate(raw)
    except _PydanticError as exc:
        raise _convert_pydantic_error(exc) from exc

    _check_clock_skew(envelope.occurred_at_utc, now=now)

    return envelope


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _check_clock_skew(ts: datetime, *, now: datetime | None = None) -> None:
    wall = now if now is not None else datetime.now(UTC)
    if ts > wall + CLOCK_SKEW_TOLERANCE:
        delta = ts - wall
        raise ClockSkewError(
            f"Event timestamp is {delta} ahead of wall clock "
            f"(tolerance: {CLOCK_SKEW_TOLERANCE}).  "
            f"Check NTP sync on the source host."
        )


def _convert_pydantic_error(exc: _PydanticError) -> EnvelopeError:
    """Map the first Pydantic validation error to one of our typed errors.

    We report only the first error to keep quarantine messages concise.
    The full Pydantic error is preserved as ``__cause__`` for debugging.
    """
    first = exc.errors(include_url=False)[0]
    field = " → ".join(str(loc) for loc in first.get("loc", ()))
    msg = first.get("msg", str(exc))

    if first.get("type") == "missing":
        return MissingFieldError(f"Required field missing: {field!r}")
    return InvalidFieldError(f"Invalid value for {field!r}: {msg}")
