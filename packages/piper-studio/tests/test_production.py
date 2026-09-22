from pathlib import Path, PurePosixPath

import pytest

from piper.errors import ConfigError
from piper_studio.production import PRODUCTION_ENV, load_production

_COMPLETE = """
name = "sandwich"
root = "/groups/sandwich/05_production"
types = ["Character", "Environment", "Set Piece", "Vehicle"]

[shotgrid]
site = "https://byuanimation.shotgunstudio.com"
script = "sandwich_pipeline"
project = 716
"""


def write_production(tmp_path: Path, text: str) -> Path:
    path = tmp_path / "production.toml"
    path.write_text(text, encoding="utf-8")
    return path


def test_reads_the_production_its_storage_and_its_shotgrid_project(tmp_path: Path) -> None:
    production = load_production(write_production(tmp_path, _COMPLETE))

    assert production.name == "sandwich"
    assert production.root == PurePosixPath("/groups/sandwich/05_production")
    assert production.types == ("Character", "Environment", "Set Piece", "Vehicle")
    assert production.shotgrid.site == "https://byuanimation.shotgunstudio.com"
    assert production.shotgrid.script == "sandwich_pipeline"
    assert production.shotgrid.project == 716


def test_the_releases_a_production_is_made_in_are_optional(tmp_path: Path) -> None:
    assert load_production(write_production(tmp_path, _COMPLETE)).software.maya is None

    named = _COMPLETE + '\n[software]\nmaya = "2026"\nhoudini = "21.0"\n'

    software = load_production(write_production(tmp_path, named)).software
    assert (software.maya, software.houdini) == ("2026", "21.0")


def test_the_environment_names_the_configuration(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = write_production(tmp_path, _COMPLETE)
    monkeypatch.setenv(PRODUCTION_ENV, str(path))

    assert load_production().name == "sandwich"


def test_an_unset_environment_variable_says_which_one(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(PRODUCTION_ENV, raising=False)

    with pytest.raises(ConfigError, match=PRODUCTION_ENV):
        load_production()


def test_a_missing_file_names_the_path(tmp_path: Path) -> None:
    missing = tmp_path / "absent.toml"

    with pytest.raises(ConfigError, match=str(missing)):
        load_production(missing)


def test_malformed_toml_says_so(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="not valid TOML"):
        load_production(write_production(tmp_path, "name = "))


def test_a_missing_section_names_the_key(tmp_path: Path) -> None:
    without_shotgrid = _COMPLETE.split("[shotgrid]")[0]

    with pytest.raises(ConfigError, match="shotgrid"):
        load_production(write_production(tmp_path, without_shotgrid))


def test_a_missing_value_names_the_key(tmp_path: Path) -> None:
    without_project = _COMPLETE.replace("project = 716\n", "")

    with pytest.raises(ConfigError, match=r"shotgrid\.project"):
        load_production(write_production(tmp_path, without_project))


def test_a_value_of_the_wrong_type_says_what_was_expected(tmp_path: Path) -> None:
    quoted_project = _COMPLETE.replace("project = 716", 'project = "716"')

    with pytest.raises(ConfigError, match="must be int, not str"):
        load_production(write_production(tmp_path, quoted_project))
