"""Trackers that stand in for a real one, shared by every package's tests."""

from dataclasses import dataclass, field

import pytest

from piper.errors import TrackerError
from piper.tracker import Asset, Shot


@dataclass
class FakeTracker:
    """A tracker whose contents the test states outright.

    It matches names the way the contract describes and keeps what it is asked
    to create. It cannot prove ordering, because it is the same code asserting
    and answering; only a real tracker can.
    """

    assets: list[Asset] = field(default_factory=list)
    shots: list[Shot] = field(default_factory=list)

    def find_assets(self, name_contains: str) -> tuple[Asset, ...]:
        return tuple(asset for asset in self.assets if _matches(asset.name, name_contains))

    def find_shots(self, name_contains: str) -> tuple[Shot, ...]:
        return tuple(shot for shot in self.shots if _matches(shot.name, name_contains))

    def create_asset(self, name: str, *, type: str, folder: str) -> Asset:
        asset = Asset(id=str(9000 + len(self.assets)), name=name, type=type, folder=folder)
        self.assets.append(asset)
        return asset


@dataclass(frozen=True)
class UnreachableTracker:
    """A tracker that fails the way a real one does when it cannot answer."""

    # As long as a real one: a failure message is not allowed to wrap, and a
    # short fake would not prove it.
    message: str = (
        "shotgrid: cannot read assets of project 716 from https://example.invalid "
        "(Can't authenticate script 'sandwich_pipeline')"
    )

    def find_assets(self, name_contains: str) -> tuple[Asset, ...]:
        raise TrackerError(self.message)

    def find_shots(self, name_contains: str) -> tuple[Shot, ...]:
        raise TrackerError(self.message)

    def create_asset(self, name: str, *, type: str, folder: str) -> Asset:
        raise TrackerError(self.message)


def _matches(name: str, name_contains: str) -> bool:
    return name_contains.casefold() in name.casefold()


@pytest.fixture
def tracker() -> FakeTracker:
    """A small production with both kinds, mixed case, and absent details."""
    return FakeTracker(
        assets=[
            Asset(id="7701", name="Frying Pan", type="Prop", folder="kitchen"),
            Asset(id="7702", name="Pan Lid", type=None, folder=None),
            Asset(id="7703", name="Kitchen Counter", type="Set Piece", folder="kitchen"),
        ],
        shots=[
            Shot(id="8801", name="SQ010_SH0020", sequence="SQ010"),
            Shot(id="8802", name="pan_test", sequence=None),
        ],
    )


@pytest.fixture
def unreachable_tracker() -> UnreachableTracker:
    return UnreachableTracker()
