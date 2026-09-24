from collections.abc import Callable
from pathlib import Path

import pytest

from piper.tracker import Asset, Tracker
from piper_studio import launch
from piper_studio.context import Context
from piper_studio.profile import Profile

Run = Callable[..., int]
Launches = list[tuple[str, Asset, Context]]


@pytest.fixture
def launches(root: Path, monkeypatch: pytest.MonkeyPatch) -> Launches:
    """Which host was launched on which work, in a production holding the frying pan's directory."""
    (root / "asset" / "kitchen" / "frying_pan").mkdir(parents=True)
    launched: Launches = []

    def maya(_profile: Profile, work: tuple[Asset, Context]) -> None:
        launched.append(("maya", *work))

    def houdini(_profile: Profile, work: tuple[Asset, Context]) -> None:
        launched.append(("houdini", *work))

    monkeypatch.setattr(launch, "maya", maya)
    monkeypatch.setattr(launch, "houdini", houdini)
    return launched


def test_an_asset_is_opened_by_a_part_of_its_name_only_it_has(
    run: Run, tracker: Tracker, launches: Launches, capsys: pytest.CaptureFixture[str]
) -> None:
    assert run(tracker, "open", "fry", "modeling") == 0

    assert capsys.readouterr().out == "Opening modeling work on 'Frying Pan'\n"
    assert [(host, asset.id, context.name) for host, asset, context in launches] == [
        ("maya", "7701", "modeling")
    ]


def test_lookdev_work_opens_in_houdini(
    run: Run, tracker: Tracker, launches: Launches, root: Path
) -> None:
    assert run(tracker, "open", "fry", "lookdev") == 0

    assert [(host, context.name) for host, _asset, context in launches] == [("houdini", "lookdev")]
    assert (root / "asset" / "kitchen" / "frying_pan" / "work" / "lookdev").is_dir()


def test_a_whole_name_in_any_case_wins_over_longer_names_containing_it(
    run: Run, tracker: Tracker, launches: Launches
) -> None:
    tracker.create_asset(
        "Frying Pan Lid", type="Prop", folder="kitchen", pipe_name="frying_pan_lid"
    )

    assert run(tracker, "open", "frying pan", "modeling") == 0

    assert [asset.name for _host, asset, _context in launches] == ["Frying Pan"]


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
        "piper: no asset context is named 'rigging' (contexts: modeling, lookdev, texturing)\n"
    )
    assert launches == []


def test_texturing_work_is_refused_until_painter_can_be_launched(
    run: Run, tracker: Tracker, launches: Launches, capsys: pytest.CaptureFixture[str]
) -> None:
    assert run(tracker, "open", "Frying Pan", "texturing") == 1

    assert capsys.readouterr().err == (
        "piper: texturing work is done in painter, which piper cannot launch yet\n"
    )
    assert launches == []


def test_an_asset_create_never_finished_is_refused_before_maya_starts(
    run: Run, tracker: Tracker, launches: Launches, capsys: pytest.CaptureFixture[str]
) -> None:
    assert run(tracker, "open", "Kitchen Counter", "modeling") == 1

    assert "`piper create asset` gives it one" in capsys.readouterr().err
    assert launches == []
