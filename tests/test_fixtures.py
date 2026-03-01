"""Verify that all fixture events are well-formed and the fixture helpers work.

Acceptance criteria (from commit spec):
  - Fixtures load without error
  - Fixture factory generates deterministic event_ids
  - All 18 known event types are represented
  - Each type has at least one success and one error variant
"""

import uuid

import pytest

from piper.validate import KNOWN_EVENT_TYPES, validate_envelope
from tests.conftest import FIXTURE_DIR, load_fixture, make_event_id

# ---------------------------------------------------------------------------
# Fixture factory
# ---------------------------------------------------------------------------


class TestMakeEventId:
    def test_is_deterministic(self):
        assert make_event_id("publish.asset.usd", 0) == make_event_id("publish.asset.usd", 0)

    def test_differs_by_event_type(self):
        assert make_event_id("publish.asset.usd", 0) != make_event_id("publish.anim.usd", 0)

    def test_differs_by_index(self):
        assert make_event_id("dcc.launch", 0) != make_event_id("dcc.launch", 1)

    def test_returns_valid_uuid(self):
        uid = make_event_id("playblast.create", 0)
        parsed = uuid.UUID(uid)  # raises ValueError if malformed
        assert str(parsed) == uid

    def test_default_index_is_zero(self):
        assert make_event_id("dcc.launch") == make_event_id("dcc.launch", 0)


# ---------------------------------------------------------------------------
# Fixture file loading
# ---------------------------------------------------------------------------


class TestLoadFixture:
    @pytest.mark.parametrize("name", ["farm", "publish", "render", "storage", "tool"])
    def test_fixture_file_exists(self, name):
        assert (FIXTURE_DIR / f"{name}.jsonl").is_file()

    @pytest.mark.parametrize("name", ["farm", "publish", "render", "storage", "tool"])
    def test_fixture_loads_as_list_of_dicts(self, name):
        events = load_fixture(name)
        assert isinstance(events, list)
        assert len(events) > 0
        assert all(isinstance(ev, dict) for ev in events)

    def test_all_fixture_events_session_fixture(self, all_fixture_events):
        assert len(all_fixture_events) >= 50  # ~54 in the current fixture set

    def test_all_events_have_required_keys(self, all_fixture_events):
        required = {
            "schema_version",
            "event_id",
            "event_type",
            "occurred_at_utc",
            "status",
            "pipeline",
            "host",
            "session",
        }
        for ev in all_fixture_events:
            missing = required - ev.keys()
            assert not missing, f"Event {ev.get('event_id')} missing fields: {missing}"


# ---------------------------------------------------------------------------
# Coverage of all 18 known event types
# ---------------------------------------------------------------------------


class TestEventTypeCoverage:
    def test_all_known_event_types_present(self, fixture_events_by_type):
        for event_type in KNOWN_EVENT_TYPES:
            assert event_type in fixture_events_by_type, f"No fixtures for {event_type!r}"

    def test_each_type_has_success_variant(self, fixture_events_by_type):
        for event_type, events in fixture_events_by_type.items():
            statuses = {ev["status"] for ev in events}
            assert "success" in statuses, f"{event_type!r} has no success event"

    def test_each_type_has_error_variant(self, fixture_events_by_type):
        for event_type, events in fixture_events_by_type.items():
            statuses = {ev["status"] for ev in events}
            assert "error" in statuses, f"{event_type!r} has no error event"

    def test_all_event_ids_are_unique(self, all_fixture_events):
        ids = [ev["event_id"] for ev in all_fixture_events]
        assert len(ids) == len(set(ids)), "Duplicate event_id found in fixture set"


# ---------------------------------------------------------------------------
# Event IDs match the factory
# ---------------------------------------------------------------------------


class TestEventIdMatchesFactory:
    """IDs in JSONL files must equal make_event_id(event_type, n) for each n."""

    def test_event_ids_match_factory(self, fixture_events_by_type):
        for event_type, events in fixture_events_by_type.items():
            for n, ev in enumerate(events):
                expected = make_event_id(event_type, n)
                assert ev["event_id"] == expected, (
                    f"{event_type}[{n}]: expected {expected!r}, got {ev['event_id']!r}"
                )


# ---------------------------------------------------------------------------
# All fixtures pass envelope validation
# ---------------------------------------------------------------------------


class TestFixturesValidate:
    def test_all_events_pass_validation(self, all_fixture_events):
        """Every fixture event must survive validate_envelope without raising."""
        from datetime import UTC, datetime

        # Fixed "now" well after all fixture timestamps so no clock-skew fires.
        now = datetime(2026, 12, 31, 23, 59, 59, tzinfo=UTC)
        for ev in all_fixture_events:
            validate_envelope(ev, now=now)

    def test_error_events_have_error_field(self, fixture_events_by_type):
        """Fixture error events should carry an 'error' dict."""
        for event_type, events in fixture_events_by_type.items():
            for ev in events:
                if ev["status"] == "error":
                    assert "error" in ev, (
                        f"{event_type} error event {ev['event_id']!r} missing 'error' field"
                    )

    def test_background_collectors_have_no_dcc(self, fixture_events_by_type):
        """Farm, render, and storage collectors should omit pipeline.dcc."""
        background_types = {
            "tractor.farm.snapshot",
            "tractor.job.spool",
            "render.stats.summary",
            "storage.scan.summary",
            "storage.scan.bucket",
        }
        for event_type in background_types:
            for ev in fixture_events_by_type.get(event_type, []):
                assert "dcc" not in ev["pipeline"], (
                    f"Background collector {event_type!r} should not have pipeline.dcc"
                )
