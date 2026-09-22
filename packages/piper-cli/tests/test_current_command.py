import json
from collections.abc import Callable
from pathlib import Path

import pytest

from piper.tracker import Tracker

Run = Callable[..., int]


@pytest.fixture
def published(run: Run, tracker: Tracker, root: Path, tmp_path: Path) -> Path:
    """Two geo publishes, so asset v001 and v002 exist and v002 is current."""
    (root / "asset" / "kitchen" / "frying_pan").mkdir(parents=True)
    layer = tmp_path / "export" / "geo.usda"
    layer.parent.mkdir()
    layer.write_text(
        '#usda 1.0\n(\n    defaultPrim = "pan"\n)\n\ndef Xform "pan"\n{\n}\n', encoding="utf-8"
    )
    assert run(tracker, "publish", "Frying Pan", "geo", str(layer)) == 0
    assert run(tracker, "publish", "Frying Pan", "geo", str(layer)) == 0
    return root


def test_reports_nothing_current_before_any_publish(
    run: Run, tracker: Tracker, root: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    (root / "asset" / "kitchen" / "frying_pan").mkdir(parents=True)

    assert run(tracker, "current", "Frying Pan") == 0

    assert capsys.readouterr().out == "Nothing is current for 'Frying Pan'\n"


def test_reports_the_current_version_and_its_pins(
    run: Run, tracker: Tracker, published: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    capsys.readouterr()

    assert run(tracker, "current", "Frying Pan") == 0

    assert capsys.readouterr().out == "Current for 'Frying Pan': asset v002 pins geo v002\n"


def test_a_version_given_becomes_current_and_is_reported(
    run: Run, tracker: Tracker, published: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    capsys.readouterr()

    assert run(tracker, "current", "Frying Pan", "1", "--json") == 0
    assert run(tracker, "current", "Frying Pan", "3") == 1

    captured = capsys.readouterr()
    assert json.loads(captured.out)["version"] == 1
    assert json.loads(captured.out)["pins"] == {"geo": 1}
    assert captured.err == "piper: Frying Pan has no asset v003 (installed: v001, v002)\n"
