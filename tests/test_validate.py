"""Tests for telemetry envelope validation.

Covers:
  - Happy path for a typical real-world event
  - All required-field missing cases (MissingFieldError)
  - Invalid field values (InvalidFieldError)
  - Clock-skew rejection (ClockSkewError)
  - Known vs unknown event_type handling
  - extra="ignore" forward-compatibility
  - Sub-model optional fields
"""

from datetime import UTC, datetime, timedelta

import pytest

from piper.validate import (
    ClockSkewError,
    InvalidFieldError,
    MissingFieldError,
    validate_envelope,
)

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

_NOW = datetime(2026, 3, 1, 12, 0, 0, tzinfo=UTC)


def _base() -> dict:
    """Minimal valid envelope dict (mirrors real playblast.create shape)."""
    return {
        "schema_version": "1.0",
        "event_id": "bfc41fdd-0000-0000-0000-000000000000",
        "event_type": "playblast.create",
        "occurred_at_utc": "2026-03-01T10:00:00Z",
        "status": "success",
        "pipeline": {"name": "sandwich-pipeline", "dcc": "maya"},
        "host": {"hostname": "samus.cs.byu.edu", "user": "rees23"},
        "session": {"session_id": "53fe7b00"},
    }


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


class TestHappyPath:
    def test_returns_envelope(self):
        env = validate_envelope(_base(), now=_NOW)
        assert env.event_type == "playblast.create"
        assert env.status == "success"

    def test_occurred_at_utc_is_aware(self):
        env = validate_envelope(_base(), now=_NOW)
        assert env.occurred_at_utc.tzinfo is not None

    def test_payload_defaults_to_empty_dict(self):
        env = validate_envelope(_base(), now=_NOW)
        assert env.payload == {}

    def test_metrics_defaults_to_empty_dict(self):
        env = validate_envelope(_base(), now=_NOW)
        assert env.metrics == {}

    def test_scope_defaults_to_empty_scope_info(self):
        env = validate_envelope(_base(), now=_NOW)
        assert env.scope.shot is None

    def test_scope_fields_parsed(self):
        raw = {**_base(), "scope": {"show": "skwondo", "shot": "0010_0020"}}
        env = validate_envelope(raw, now=_NOW)
        assert env.scope.show == "skwondo"
        assert env.scope.shot == "0010_0020"

    def test_error_field_absent_by_default(self):
        env = validate_envelope(_base(), now=_NOW)
        assert env.error is None

    def test_error_field_parsed_when_present(self):
        raw = {**_base(), "status": "error", "error": {"code": "E42", "message": "boom"}}
        env = validate_envelope(raw, now=_NOW)
        assert env.error is not None
        assert env.error.code == "E42"

    def test_pipeline_dcc_optional(self):
        """Background collectors (storage_scan) omit dcc."""
        raw = _base()
        raw["pipeline"] = {"name": "sandwich-pipeline"}
        env = validate_envelope(raw, now=_NOW)
        assert env.pipeline.dcc is None

    def test_extra_fields_ignored_top_level(self):
        raw = {**_base(), "future_field": "ignored"}
        env = validate_envelope(raw, now=_NOW)
        assert not hasattr(env, "future_field")

    def test_extra_fields_ignored_in_sub_model(self):
        raw = _base()
        raw["host"] = {**raw["host"], "gpu": "RTX 4090"}
        env = validate_envelope(raw, now=_NOW)
        assert not hasattr(env.host, "gpu")

    def test_payload_and_metrics_passed_through(self):
        raw = {**_base(), "payload": {"preset": "web"}, "metrics": {"duration_ms": 1294}}
        env = validate_envelope(raw, now=_NOW)
        assert env.payload["preset"] == "web"
        assert env.metrics["duration_ms"] == 1294


# ---------------------------------------------------------------------------
# Unknown event_type is NOT an error (forward-compat design)
# ---------------------------------------------------------------------------


class TestUnknownEventType:
    def test_unknown_type_accepted(self):
        raw = {**_base(), "event_type": "future.unknown.event"}
        env = validate_envelope(raw, now=_NOW)
        assert env.event_type == "future.unknown.event"

    def test_known_types_all_accepted(self):
        from piper.validate import KNOWN_EVENT_TYPES

        for event_type in KNOWN_EVENT_TYPES:
            raw = {**_base(), "event_type": event_type}
            env = validate_envelope(raw, now=_NOW)
            assert env.event_type == event_type


# ---------------------------------------------------------------------------
# Missing required fields → MissingFieldError
# ---------------------------------------------------------------------------


class TestMissingFields:
    @pytest.mark.parametrize(
        "field",
        [
            "schema_version",
            "event_id",
            "event_type",
            "occurred_at_utc",
            "status",
            "pipeline",
            "host",
            "session",
        ],
    )
    def test_missing_top_level_field(self, field):
        raw = _base()
        del raw[field]
        with pytest.raises(MissingFieldError):
            validate_envelope(raw, now=_NOW)

    def test_missing_pipeline_name(self):
        raw = _base()
        raw["pipeline"] = {}
        with pytest.raises(MissingFieldError):
            validate_envelope(raw, now=_NOW)

    def test_missing_host_hostname(self):
        raw = _base()
        raw["host"] = {"user": "rees23"}
        with pytest.raises(MissingFieldError):
            validate_envelope(raw, now=_NOW)

    def test_missing_host_user(self):
        raw = _base()
        raw["host"] = {"hostname": "samus.cs.byu.edu"}
        with pytest.raises(MissingFieldError):
            validate_envelope(raw, now=_NOW)

    def test_missing_session_id(self):
        raw = _base()
        raw["session"] = {}
        with pytest.raises(MissingFieldError):
            validate_envelope(raw, now=_NOW)

    def test_missing_field_error_is_value_error(self):
        raw = _base()
        del raw["event_id"]
        with pytest.raises(ValueError):
            validate_envelope(raw, now=_NOW)


# ---------------------------------------------------------------------------
# Invalid field values → InvalidFieldError
# ---------------------------------------------------------------------------


class TestInvalidFields:
    def test_bad_status_value(self):
        raw = {**_base(), "status": "unknown"}
        with pytest.raises(InvalidFieldError):
            validate_envelope(raw, now=_NOW)

    def test_non_string_event_id(self):
        raw = {**_base(), "event_id": 12345}
        # Pydantic coerces int → str in lax mode; if it does, that's OK.
        # If it raises, it must be InvalidFieldError.
        try:
            env = validate_envelope(raw, now=_NOW)
            assert env.event_id == "12345"
        except InvalidFieldError:
            pass  # also acceptable

    def test_naive_datetime_rejected(self):
        """AwareDatetime requires a UTC offset; naive timestamps are invalid."""
        raw = {**_base(), "occurred_at_utc": "2026-03-01T10:00:00"}
        with pytest.raises(InvalidFieldError):
            validate_envelope(raw, now=_NOW)

    def test_garbage_datetime_rejected(self):
        raw = {**_base(), "occurred_at_utc": "not-a-date"}
        with pytest.raises(InvalidFieldError):
            validate_envelope(raw, now=_NOW)

    def test_invalid_field_error_is_value_error(self):
        raw = {**_base(), "status": "bad"}
        with pytest.raises(ValueError):
            validate_envelope(raw, now=_NOW)


# ---------------------------------------------------------------------------
# Clock-skew checks
# ---------------------------------------------------------------------------


class TestClockSkew:
    def test_event_just_within_tolerance_accepted(self):
        """59 minutes into the future — inside the 1-hour window."""
        ts = _NOW + timedelta(minutes=59)
        raw = {**_base(), "occurred_at_utc": ts.isoformat()}
        env = validate_envelope(raw, now=_NOW)
        assert env.occurred_at_utc == ts

    def test_event_exactly_at_tolerance_accepted(self):
        """Exactly 1 hour ahead — boundary is exclusive (ts > wall + tolerance)."""
        ts = _NOW + timedelta(hours=1)
        raw = {**_base(), "occurred_at_utc": ts.isoformat()}
        env = validate_envelope(raw, now=_NOW)
        assert env.occurred_at_utc == ts

    def test_event_over_tolerance_rejected(self):
        """61 minutes into the future — exceeds 1-hour tolerance."""
        ts = _NOW + timedelta(minutes=61)
        raw = {**_base(), "occurred_at_utc": ts.isoformat()}
        with pytest.raises(ClockSkewError):
            validate_envelope(raw, now=_NOW)

    def test_clock_skew_message_is_informative(self):
        ts = _NOW + timedelta(hours=2)
        raw = {**_base(), "occurred_at_utc": ts.isoformat()}
        with pytest.raises(ClockSkewError, match="ahead of wall clock"):
            validate_envelope(raw, now=_NOW)

    def test_old_event_never_rejected(self):
        """Events from the past, no matter how old, are always valid."""
        ts = _NOW - timedelta(days=365)
        raw = {**_base(), "occurred_at_utc": ts.isoformat()}
        env = validate_envelope(raw, now=_NOW)
        assert env.occurred_at_utc == ts

    def test_clock_skew_error_is_value_error(self):
        ts = _NOW + timedelta(hours=2)
        raw = {**_base(), "occurred_at_utc": ts.isoformat()}
        with pytest.raises(ValueError):
            validate_envelope(raw, now=_NOW)

    def test_now_defaults_to_wall_clock(self):
        """Omitting ``now`` uses datetime.now(UTC) — just check it doesn't raise."""
        # Use a timestamp well in the past so wall-clock skew never triggers.
        raw = {**_base(), "occurred_at_utc": "2020-01-01T00:00:00Z"}
        validate_envelope(raw)  # no now= argument
