"""Trackers and registries that stand in for real ones, shared by every package's tests."""

from dataclasses import dataclass, field
from pathlib import PurePosixPath

import pytest

from piper.errors import RegistryError, TrackerError
from piper.tracker import Asset, Shot


def pytest_report_header() -> str:
    # Hosts bring their own USD, so a run must say which one it tested.
    from pxr import Usd

    return "usd: " + ".".join(str(part) for part in Usd.GetVersion())


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


@dataclass
class FakeRegistry:
    """A registry that keeps what it is asked to record, and refuses a repeat as a real one does."""

    registrations: list[tuple[Asset, str, int, PurePosixPath]] = field(default_factory=list)

    def register(self, asset: Asset, *, product: str, version: int, path: PurePosixPath) -> str:
        if any(kept[:3] == (asset, product, version) for kept in self.registrations):
            raise RegistryError(f"version {version} of {product} is already registered")
        self.registrations.append((asset, product, version, path))
        return str(6600 + len(self.registrations))


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


@pytest.fixture
def registry() -> FakeRegistry:
    return FakeRegistry()


@pytest.fixture
def registrations(registry: FakeRegistry) -> list[tuple[Asset, str, int, PurePosixPath]]:
    """What the ``registry`` fixture was asked to record, in order."""
    return registry.registrations
