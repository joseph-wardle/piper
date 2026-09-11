from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True, slots=True)
class Asset:
    """An asset in the production tracker.

    ``id`` is opaque and belongs to the tracker; ``name`` is what artists see.
    ``type`` is the tracker's classification of what the asset is, and
    ``folder`` is where artists browse for it. Either may be absent from an
    asset Piper did not create.
    """

    id: str
    name: str
    type: str | None
    folder: str | None


@dataclass(frozen=True, slots=True)
class Shot:
    """A shot in the production tracker."""

    id: str
    name: str
    sequence: str | None


class Tracker(Protocol):
    """One production's entities, in the tracker that owns them."""

    def find_assets(self, name_contains: str) -> tuple[Asset, ...]: ...

    def find_shots(self, name_contains: str) -> tuple[Shot, ...]: ...

    def create_asset(self, name: str, *, type: str, folder: str) -> Asset: ...
