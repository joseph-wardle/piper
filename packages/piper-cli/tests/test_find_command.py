import json
from collections.abc import Callable

import pytest

from piper.tracker import Tracker
from piper_studio.production import PRODUCTION_ENV

Run = Callable[..., int]


def test_shows_names_and_details_but_never_ids(
    run: Run, tracker: Tracker, capsys: pytest.CaptureFixture[str]
) -> None:
    assert run(tracker, "find", "pan") == 0

    out = capsys.readouterr().out
    assert "Frying Pan" in out
    assert "Prop" in out
    assert "kitchen" in out
    assert "pan_test" in out
    assert "Kitchen Counter" not in out
    assert "7701" not in out


def test_shows_a_placeholder_where_the_tracker_knows_nothing(
    run: Run, tracker: Tracker, capsys: pytest.CaptureFixture[str]
) -> None:
    assert run(tracker, "find", "pan") == 0

    assert "—" in capsys.readouterr().out


def test_omitting_the_query_lists_the_whole_production(
    run: Run, tracker: Tracker, capsys: pytest.CaptureFixture[str]
) -> None:
    assert run(tracker, "find") == 0

    assert "Kitchen Counter" in capsys.readouterr().out


def test_json_carries_the_record_fields_exactly(
    run: Run, tracker: Tracker, capsys: pytest.CaptureFixture[str]
) -> None:
    assert run(tracker, "find", "pan", "--json") == 0

    assert json.loads(capsys.readouterr().out) == {
        "assets": [
            {"id": "7701", "name": "Frying Pan", "type": "Prop", "folder": "kitchen"},
            {"id": "7702", "name": "Pan Lid", "type": None, "folder": None},
        ],
        "shots": [{"id": "8802", "name": "pan_test", "sequence": None}],
    }


def test_json_of_nothing_is_still_json(
    run: Run, tracker: Tracker, capsys: pytest.CaptureFixture[str]
) -> None:
    assert run(tracker, "find", "no such thing", "--json") == 0

    assert json.loads(capsys.readouterr().out) == {"assets": [], "shots": []}


def test_finding_nothing_says_so_and_succeeds(
    run: Run, tracker: Tracker, capsys: pytest.CaptureFixture[str]
) -> None:
    assert run(tracker, "find", "no such thing") == 0

    captured = capsys.readouterr()
    assert "no such thing" in captured.out
    assert captured.err == ""


def test_a_tracker_failure_is_one_readable_line_on_stderr(
    run: Run, unreachable_tracker: Tracker, capsys: pytest.CaptureFixture[str]
) -> None:
    assert run(unreachable_tracker, "find", "pan") == 1

    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err.startswith("piper: shotgrid: cannot read assets")
    assert "Traceback" not in captured.err
    assert captured.err.splitlines() == [captured.err.rstrip("\n")]


def test_a_command_that_needs_a_production_says_how_to_select_one(
    run: Run,
    tracker: Tracker,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.delenv(PRODUCTION_ENV)

    assert run(tracker, "find", "pan") == 1

    assert "piper configure" in capsys.readouterr().err
