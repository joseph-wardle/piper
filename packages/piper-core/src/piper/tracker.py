from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True, slots=True)
class Asset:
    """An asset in the production tracker.

    ``id`` is opaque and belongs to the tracker.
    ``name`` is what artists see and may change.
    ``type`` is the tracker's classification of what the asset is.
    ``folder`` is where artists browse for it.
    ``pipe_name`` is what the asset's paths are built from: given once, unique
        in the production, and never changed by a rename. Any of the three may
        be absent from an asset Piper did not create.
    """

    id: str
    name: str
    type: str | None
    folder: str | None
    pipe_name: str | None


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

    def asset(self, id: str) -> Asset | None:
        """The asset the tracker knows by ``id``, or None when it knows no such asset."""
        ...

    def create_asset(self, name: str, *, type: str, folder: str, pipe_name: str) -> Asset: ...

    def set_pipe_name(self, asset: Asset, pipe_name: str) -> Asset:
        """Give an asset that has no pipe name its pipe name, and return it as it now is."""
        ...
