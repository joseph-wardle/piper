"""Finding tracked entities by name."""

from dataclasses import dataclass

from piper.tracker import Asset, Shot, Tracker


@dataclass(frozen=True, slots=True)
class Matches:
    """Everything a query matched, kept separate by kind."""

    assets: tuple[Asset, ...]
    shots: tuple[Shot, ...]


def find(tracker: Tracker, name_contains: str = "") -> Matches:
    """Find every asset and shot whose name contains ``name_contains``.

    An empty ``name_contains`` matches the whole production.
    """
    return Matches(
        assets=tracker.find_assets(name_contains),
        shots=tracker.find_shots(name_contains),
    )
