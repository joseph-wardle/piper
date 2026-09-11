import pytest

from piper.errors import TrackerError
from piper_shotgrid.tracker import ShotGridTracker


def build(site: str) -> ShotGridTracker:
    return ShotGridTracker(site=site, script="sandwich_pipeline", key="not-a-real-key", project=716)


def test_a_malformed_site_address_fails_as_a_piper_error() -> None:
    with pytest.raises(TrackerError, match="not a usable site address"):
        build("byuanimation.shotgunstudio.com")


def test_construction_contacts_nothing() -> None:
    # A site that cannot resolve. Construction that reached the network would
    # fail here instead of at the first query, and `piper find` would pay for a
    # connection it may never use.
    build("https://sandwich.invalid")
