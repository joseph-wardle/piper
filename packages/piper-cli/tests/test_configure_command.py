from pathlib import Path

import pytest

from piper.tracker import Tracker
from piper_cli import main as cli
from piper_studio import profile
from piper_studio.production import PRODUCTION_ENV, Production

_PRODUCTION = """
name = "sandwich"
root = "{root}"
types = ["Prop"]

[shotgrid]
site = "https://byuanimation.shotgunstudio.com"
script = "sandwich_pipeline"
project = 716
"""


@pytest.fixture
def sandwich(root: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A production Piper knows by name."""
    monkeypatch.delenv(PRODUCTION_ENV, raising=False)
    path = tmp_path / "sandwich.toml"
    path.write_text(_PRODUCTION.format(root=root), encoding="utf-8")
    monkeypatch.setattr(profile, "PRODUCTIONS", {"sandwich": path})
    return path


def test_general_is_where_piper_starts_and_where_it_returns_to(
    sandwich: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert cli.main(("configure",)) == 0
    assert capsys.readouterr().out == "general\n"

    cli.main(("configure", "sandwich"))
    capsys.readouterr()

    assert cli.main(("configure", "general")) == 0
    assert capsys.readouterr().out == "general\n"


def test_a_selection_outlives_the_command_that_made_it(
    sandwich: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert cli.main(("configure", "sandwich")) == 0
    capsys.readouterr()

    assert cli.main(("configure",)) == 0
    assert capsys.readouterr().out == f"sandwich  {sandwich}\n"


def test_a_later_command_works_in_the_selected_production(
    sandwich: Path, root: Path, tracker: Tracker, monkeypatch: pytest.MonkeyPatch
) -> None:
    worked_in: list[Production] = []

    def tracker_for(production: Production) -> Tracker:
        worked_in.append(production)
        return tracker

    monkeypatch.setattr(cli, "tracker_for", tracker_for)
    assert cli.main(("configure", "sandwich")) == 0

    assert cli.main(("find", "pan")) == 0

    assert [(one.shotgrid.project, str(one.root)) for one in worked_in] == [(716, str(root))]


def test_an_override_is_named_rather_than_applied_silently(
    sandwich: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    cli.main(("configure", "general"))
    capsys.readouterr()
    monkeypatch.setenv(PRODUCTION_ENV, str(sandwich))

    assert cli.main(("configure",)) == 0

    out = capsys.readouterr().out
    assert out.startswith(f"sandwich  {sandwich}\n")
    assert f"{PRODUCTION_ENV} overrides the configured profile 'general'" in out


def test_an_override_of_the_same_name_is_still_named(
    sandwich: Path,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The inactive copy of a production shares its name, and only its path tells them apart."""
    cli.main(("configure", "sandwich"))
    capsys.readouterr()
    copy = tmp_path / "copy.toml"
    copy.write_text(
        sandwich.read_text().replace("project = 716", "project = 782"), encoding="utf-8"
    )
    monkeypatch.setenv(PRODUCTION_ENV, str(copy))

    assert cli.main(("configure",)) == 0

    out = capsys.readouterr().out
    assert out.startswith(f"sandwich  {copy}\n")
    assert f"{PRODUCTION_ENV} overrides the configured profile 'sandwich'" in out


def test_a_profile_piper_cannot_name_is_reported(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv(PRODUCTION_ENV, raising=False)

    assert cli.main(("configure", "bobo")) == 1

    assert "no profile is named 'bobo'" in capsys.readouterr().err
