import json
from collections.abc import Callable
from pathlib import Path

import pytest

from piper.tracker import Tracker

Run = Callable[..., int]


def published(run: Run, tracker: Tracker, layer: Path) -> None:
    """Two geo publishes, so asset v001 and v002 exist and v002 is current."""
    for _ in range(2):
        assert run(tracker, "publish", "Frying Pan", "geo", str(layer)) == 0


def test_reports_nothing_then_what_publishes_made_current(
    run: Run, tracker: Tracker, layer: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert run(tracker, "current", "Frying Pan") == 0
    assert capsys.readouterr().out == "'Frying Pan': Nothing is current.\n"

    published(run, tracker, layer)
    capsys.readouterr()

    assert run(tracker, "current", "Frying Pan") == 0
    assert capsys.readouterr().out == "'Frying Pan': Current is asset v002, pinning geo v002.\n"


def test_a_version_given_becomes_current_and_is_reported(
    run: Run, tracker: Tracker, layer: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    published(run, tracker, layer)
    capsys.readouterr()

    assert run(tracker, "current", "Frying Pan", "1", "--json") == 0
    assert run(tracker, "current", "Frying Pan", "3") == 1

    captured = capsys.readouterr()
    assert json.loads(captured.out)["version"] == 1
    assert json.loads(captured.out)["pins"] == {"geo": 1}
    assert captured.err == "piper: Frying Pan has no asset v003 (installed: v001, v002)\n"
