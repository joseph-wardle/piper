from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True, slots=True)
class Asset:
    """An asset in the production tracker.

    ``id`` is opaque and belongs to the tracker;
    ``name`` is what artists see. ``kind`` is the tracker's own classification
    and may be absent.
    """

    id: str
    name: str
    kind: str | None


@dataclass(frozen=True, slots=True)
class Shot:
    """A shot in the production tracker."""

    id: str
    name: str
    sequence: str | None


class Tracker(Protocol):
    """Reads one production's entities from the tracker that owns them."""

    def find_assets(self, name_contains: str) -> tuple[Asset, ...]: ...

    def find_shots(self, name_contains: str) -> tuple[Shot, ...]: ...
