from collections.abc import Callable
from pathlib import Path

import pytest

from piper.tracker import Asset, Tracker
from piper_studio import launch
from piper_studio.context import Context
from piper_studio.profile import Profile

Run = Callable[..., int]
Launches = list[tuple[Asset, Context]]


@pytest.fixture
def launches(root: Path, monkeypatch: pytest.MonkeyPatch) -> Launches:
    """The work Maya was launched on, in a production holding the frying pan's directory."""
    (root / "asset" / "kitchen" / "frying_pan").mkdir(parents=True)
    launched: Launches = []

    def maya(_profile: Profile, work: tuple[Asset, Context]) -> None:
        launched.append(work)

    monkeypatch.setattr(launch, "maya", maya)
    return launched


def test_an_asset_is_opened_by_a_part_of_its_name_only_it_has(
    run: Run, tracker: Tracker, launches: Launches, capsys: pytest.CaptureFixture[str]
) -> None:
    assert run(tracker, "open", "fry", "modeling") == 0

    assert capsys.readouterr().out == "Opening modeling work on 'Frying Pan'\n"
    assert [(asset.id, context.name) for asset, context in launches] == [("7701", "modeling")]


def test_a_whole_name_in_any_case_wins_over_longer_names_containing_it(
    run: Run, tracker: Tracker, launches: Launches
) -> None:
    tracker.create_asset(
        "Frying Pan Lid", type="Prop", folder="kitchen", pipe_name="frying_pan_lid"
    )

    assert run(tracker, "open", "frying pan", "modeling") == 0

    assert [asset.name for asset, _context in launches] == ["Frying Pan"]


def test_a_part_several_names_have_opens_nothing_and_lists_them(
    run: Run, tracker: Tracker, launches: Launches, capsys: pytest.CaptureFixture[str]
) -> None:
    assert run(tracker, "open", "pan", "modeling") == 1

    assert capsys.readouterr().err == (
        "piper: 2 assets have names containing 'pan': 'Frying Pan', 'Pan Lid'\n"
    )
    assert launches == []


def test_an_unknown_context_names_the_contexts_there_are(
    run: Run, tracker: Tracker, launches: Launches, capsys: pytest.CaptureFixture[str]
) -> None:
    assert run(tracker, "open", "Frying Pan", "rigging") == 1

    assert capsys.readouterr().err == (
        "piper: no asset context is named 'rigging' (contexts: modeling)\n"
    )
    assert launches == []


def test_an_asset_create_never_finished_is_refused_before_maya_starts(
    run: Run, tracker: Tracker, launches: Launches, capsys: pytest.CaptureFixture[str]
) -> None:
    assert run(tracker, "open", "Kitchen Counter", "modeling") == 1

    assert "`piper create asset` gives it one" in capsys.readouterr().err
    assert launches == []
