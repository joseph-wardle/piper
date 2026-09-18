from pathlib import Path

import pytest

from piper.errors import ConfigError
from piper_studio import profile
from piper_studio.production import PRODUCTION_ENV

_PRODUCTION = """
name = "sandwich"
root = "/groups/sandwich/05_production"
types = ["Prop"]

[shotgrid]
site = "https://byuanimation.shotgunstudio.com"
script = "sandwich_pipeline"
project = 716
"""


@pytest.fixture
def config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """An empty configuration directory of this test's own."""
    monkeypatch.delenv(PRODUCTION_ENV, raising=False)
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "config"))
    return profile.config_path()


@pytest.fixture
def sandwich(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A production Piper knows by name, configured in a file this test wrote."""
    path = tmp_path / "production.toml"
    path.write_text(_PRODUCTION, encoding="utf-8")
    monkeypatch.setattr(profile, "PRODUCTIONS", {"sandwich": path})
    return path


def test_the_environment_outranks_the_selection_without_changing_it(
    config: Path, sandwich: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    profile.select("sandwich")
    elsewhere = tmp_path / "elsewhere.toml"
    elsewhere.write_text(_PRODUCTION.replace("project = 716", "project = 782"), encoding="utf-8")
    monkeypatch.setenv(PRODUCTION_ENV, str(elsewhere))

    active = profile.active()

    assert active.path == elsewhere
    assert active.production is not None
    assert active.production.shotgrid.project == 782
    assert profile.selected() == "sandwich"


def test_a_production_that_cannot_be_read_is_refused_before_it_is_selected(
    config: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(profile, "PRODUCTIONS", {"sandwich": tmp_path / "gone.toml"})

    with pytest.raises(ConfigError):
        profile.select("sandwich")

    assert not config.exists()


def test_a_configuration_file_nobody_can_parse_says_to_delete_it(config: Path) -> None:
    config.parent.mkdir(parents=True)
    config.write_text("profile = ", encoding="utf-8")

    with pytest.raises(ConfigError, match="delete it"):
        profile.selected()
