"""Shared pytest helpers and fixtures for the piper test suite.

load_fixture(name)          — parse tests/fixtures/<name>.jsonl into event dicts
make_event_id(type, n)      — deterministic UUID for synthetic fixture events
all_fixture_events          — session fixture: every event from tests/fixtures/
fixture_events_by_type      — session fixture: event_type → [raw dict, …]
"""

import json
import uuid
from pathlib import Path

import pytest

FIXTURE_DIR = Path(__file__).parent / "fixtures"

# Stable namespace for fixture event IDs — never change this value.
# All IDs in the JSONL files were generated with this namespace.
_FIXTURE_NS = uuid.UUID("e4a3b2c1-d5e6-7890-abcd-ef0123456789")


def make_event_id(event_type: str, n: int = 0) -> str:
    """Return a deterministic UUID string for synthetic fixture event *n* of *event_type*.

    Stable as long as ``_FIXTURE_NS`` and the ``"<event_type>:<n>"`` key are unchanged.
    Use this in tests instead of hard-coding UUIDs::

        assert ev["event_id"] == make_event_id("dcc.launch", 0)
    """
    return str(uuid.uuid5(_FIXTURE_NS, f"{event_type}:{n}"))


def load_fixture(name: str) -> list[dict]:
    """Parse ``tests/fixtures/<name>.jsonl`` and return a list of event dicts."""
    return [
        json.loads(line)
        for line in (FIXTURE_DIR / f"{name}.jsonl").read_text().splitlines()
        if line.strip()
    ]


@pytest.fixture(scope="session")
def all_fixture_events() -> list[dict]:
    """All synthetic events from ``tests/fixtures/``, in filename-sorted order."""
    events = []
    for path in sorted(FIXTURE_DIR.glob("*.jsonl")):
        events.extend(json.loads(line) for line in path.read_text().splitlines() if line.strip())
    return events


@pytest.fixture(scope="session")
def fixture_events_by_type(all_fixture_events: list[dict]) -> dict[str, list[dict]]:
    """Map of event_type → list of events, preserving insertion order within each type."""
    by_type: dict[str, list[dict]] = {}
    for ev in all_fixture_events:
        by_type.setdefault(ev["event_type"], []).append(ev)
    return by_type
