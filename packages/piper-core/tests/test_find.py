import pytest

from piper.errors import TrackerError
from piper.find import Matches, find
from piper.tracker import Tracker


def test_reports_both_kinds_separately(tracker: Tracker) -> None:
    matches = find(tracker, "pan")

    assert [asset.name for asset in matches.assets] == ["Frying Pan", "Pan Lid"]
    assert [shot.name for shot in matches.shots] == ["pan_test"]


def test_an_empty_query_matches_the_whole_production(tracker: Tracker) -> None:
    matches = find(tracker)

    assert len(matches.assets) == 3
    assert len(matches.shots) == 2


def test_matching_one_kind_leaves_the_other_empty(tracker: Tracker) -> None:
    matches = find(tracker, "counter")

    assert [asset.name for asset in matches.assets] == ["Kitchen Counter"]
    assert matches.shots == ()


def test_matching_nothing_is_not_a_failure(tracker: Tracker) -> None:
    assert find(tracker, "no such thing") == Matches(assets=(), shots=())


def test_a_tracker_failure_reaches_the_caller(unreachable_tracker: Tracker) -> None:
    with pytest.raises(TrackerError):
        find(unreachable_tracker, "pan")
