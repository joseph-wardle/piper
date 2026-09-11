import os

import pytest

from piper.errors import TrackerError
from piper.tracker import Asset, Shot
from piper_shotgrid.tracker import ShotGridTracker

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.environ.get("PIPER_SHOTGRID_KEY"),
        reason="PIPER_SHOTGRID_KEY is not set",
    ),
]

SITE = "https://byuanimation.shotgunstudio.com"
SCRIPT = "sandwich_pipeline"
PROJECT = 716


@pytest.fixture(scope="module")
def tracker() -> ShotGridTracker:
    return ShotGridTracker(
        site=SITE,
        script=SCRIPT,
        key=os.environ["PIPER_SHOTGRID_KEY"],
        project=PROJECT,
    )


@pytest.fixture(scope="module")
def every_asset(tracker: ShotGridTracker) -> tuple[Asset, ...]:
    return tracker.find_assets("")


@pytest.fixture(scope="module")
def every_shot(tracker: ShotGridTracker) -> tuple[Shot, ...]:
    return tracker.find_shots("")


def test_the_whole_production_comes_back_unfiltered(
    every_asset: tuple[Asset, ...], every_shot: tuple[Shot, ...]
) -> None:
    assert len(every_asset) > 50
    assert len(every_shot) > 50


def test_assets_are_sorted_by_name_case_insensitively(every_asset: tuple[Asset, ...]) -> None:
    names = [asset.name for asset in every_asset]

    assert names == sorted(names, key=str.casefold)


def test_shots_are_sorted_by_name_case_insensitively(every_shot: tuple[Shot, ...]) -> None:
    names = [shot.name for shot in every_shot]

    assert names == sorted(names, key=str.casefold)


def test_a_lowercase_query_matches_a_mixed_case_name(
    tracker: ShotGridTracker, every_asset: tuple[Asset, ...]
) -> None:
    mixed_case = next(asset for asset in every_asset if asset.name != asset.name.lower())

    matched = tracker.find_assets(mixed_case.name[:4].lower())

    assert mixed_case.id in {asset.id for asset in matched}


def test_shots_carry_a_readable_sequence_name(every_shot: tuple[Shot, ...]) -> None:
    sequences = {shot.sequence for shot in every_shot if shot.sequence}

    assert sequences
    assert all(isinstance(sequence, str) for sequence in sequences)


def test_assets_carry_the_tracker_classification(every_asset: tuple[Asset, ...]) -> None:
    kinds = {asset.kind for asset in every_asset if asset.kind}

    assert len(kinds) > 1


def test_nothing_matches_a_nonsense_query(tracker: ShotGridTracker) -> None:
    assert tracker.find_assets("zzz-no-such-asset-zzz") == ()
    assert tracker.find_shots("zzz-no-such-shot-zzz") == ()


def test_a_rejected_credential_stays_a_piper_error() -> None:
    tracker = ShotGridTracker(site=SITE, script=SCRIPT, key="not-a-real-key", project=PROJECT)

    with pytest.raises(TrackerError, match="cannot read assets"):
        tracker.find_assets("pan")


def test_an_unreachable_site_stays_a_piper_error() -> None:
    tracker = ShotGridTracker(
        site="https://sandwich.invalid", script=SCRIPT, key="not-a-real-key", project=PROJECT
    )

    with pytest.raises(TrackerError, match="cannot read assets"):
        tracker.find_assets("pan")
