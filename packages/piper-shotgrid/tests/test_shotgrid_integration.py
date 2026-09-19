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
# An inactive copy of the production, kept for writes. The live one is never written.
# This is for development purposes. v1.0.0 should not have this.
WRITE_PROJECT = 782


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


def test_assets_carry_their_type_and_folder(every_asset: tuple[Asset, ...]) -> None:
    types = {asset.type for asset in every_asset if asset.type}
    folders = {asset.folder for asset in every_asset if asset.folder}

    assert len(types) > 1
    assert len(folders) > 1


def test_an_asset_is_read_back_by_its_id_within_its_own_project_only(
    tracker: ShotGridTracker, every_asset: tuple[Asset, ...]
) -> None:
    known = every_asset[0]
    elsewhere = ShotGridTracker(
        site=SITE, script=SCRIPT, key=os.environ["PIPER_SHOTGRID_KEY"], project=WRITE_PROJECT
    )

    assert tracker.asset(known.id) == known
    assert elsewhere.asset(known.id) is None
    assert tracker.asset("not-an-id") is None


def test_nothing_matches_a_nonsense_query(tracker: ShotGridTracker) -> None:
    assert tracker.find_assets("zzz-no-such-asset-zzz") == ()
    assert tracker.find_shots("zzz-no-such-shot-zzz") == ()


def test_a_rejected_credential_stays_a_piper_error() -> None:
    tracker = ShotGridTracker(site=SITE, script=SCRIPT, key="not-a-real-key", project=PROJECT)

    with pytest.raises(TrackerError, match="cannot read assets"):
        tracker.find_assets("pan")


def test_the_site_refuses_a_type_it_does_not_offer_and_creates_nothing() -> None:
    # Piper checks a type only against the production's configured list and
    # trusts the site to refuse the rest. This proves the site does.
    tracker = ShotGridTracker(
        site=SITE, script=SCRIPT, key=os.environ["PIPER_SHOTGRID_KEY"], project=WRITE_PROJECT
    )

    with pytest.raises(TrackerError, match=r"cannot create asset .* not a valid list value"):
        tracker.create_asset(
            "Piper Rejected Type",
            type="Not A Type",
            folder="piper_test",
            pipe_name="piper_rejected_type",
        )

    assert tracker.find_assets("Piper Rejected Type") == ()


def test_an_unreachable_site_stays_a_piper_error() -> None:
    tracker = ShotGridTracker(
        site="https://sandwich.invalid", script=SCRIPT, key="not-a-real-key", project=PROJECT
    )

    with pytest.raises(TrackerError, match="cannot read assets"):
        tracker.find_assets("pan")
