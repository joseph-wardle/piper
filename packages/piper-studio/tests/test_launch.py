import os
from pathlib import Path, PurePosixPath

import pytest

import piper_studio
from piper.errors import PiperError
from piper_studio import launch
from piper_studio.production import PRODUCTION_ENV, Production, ShotGridConfig, Software
from piper_studio.profile import GENERAL, Profile

RELEASE = Path(__file__).resolve().parents[3]
GENERAL_PROFILE = Profile(name=GENERAL, production=None, path=None)


def production_at(root: Path, maya: str | None = None) -> Profile:
    """A profile whose production is stored under ``root``."""
    return Profile(
        name="sandwich",
        production=Production(
            name="sandwich",
            root=PurePosixPath(root),
            types=("Prop",),
            shotgrid=ShotGridConfig(site="https://example.invalid", script="piper", project=782),
            software=Software(maya=maya),
        ),
        path=root.parent / "production.toml",
    )


def test_piper_becomes_maya_with_its_own_variables_and_none_of_the_shells(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    executable = tmp_path / "bin" / f"maya{launch.MAYA_VERSION}"
    executable.parent.mkdir()
    executable.touch(mode=0o755)
    monkeypatch.setenv("MAYA_LOCATION", str(tmp_path))
    monkeypatch.setenv("PYTHONPATH", "/venv/site-packages")
    monkeypatch.setenv("QT_PLUGIN_PATH", "/venv/plugins")
    monkeypatch.setenv("HOME", "/users/nobody")
    monkeypatch.delenv(PRODUCTION_ENV, raising=False)
    became: list[str] = []
    handed: dict[str, str] = {}

    def execve(path: Path, _argv: list[str], environment: dict[str, str]) -> None:
        became.append(str(path))
        handed.update(environment)

    monkeypatch.setattr(os, "execve", execve)

    launch.maya(GENERAL_PROFILE)

    assert became == [str(executable)]
    assert handed["PYTHONPATH"] == launch.python_path()
    assert handed["MULTI_USD_VERSION"] == launch.MAYA_USD_VERSION
    assert "QT_PLUGIN_PATH" not in handed
    assert handed["HOME"] == "/users/nobody"


def test_a_production_is_made_in_the_maya_it_names(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("MAYA_LOCATION", raising=False)
    profile = production_at(tmp_path, maya="2027")

    with pytest.raises(PiperError) as refusal:
        launch.maya_executable(profile)

    refused = str(refusal.value)
    assert "sandwich needs Maya 2027" in refused
    assert "/usr/autodesk/maya2027" in refused
    assert "set MAYA_LOCATION" in refused


def test_a_maya_of_another_release_is_refused_even_where_maya_location_names_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    installed = tmp_path / "bin" / "maya2026"
    installed.parent.mkdir()
    installed.touch(mode=0o755)
    monkeypatch.setenv("MAYA_LOCATION", str(tmp_path))
    profile = production_at(tmp_path, maya="2027")

    with pytest.raises(PiperError, match="point MAYA_LOCATION at a Maya 2027"):
        launch.maya_executable(profile)


def test_a_host_imports_piper_from_its_sources_and_nowhere_else() -> None:
    assert launch.python_path().split(os.pathsep) == [
        str(RELEASE / "packages" / "piper-core" / "src"),
        str(RELEASE / "packages" / "piper-studio" / "src"),
    ]


def test_an_installed_piper_is_not_a_release(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    installed = tmp_path / "site-packages" / "piper_studio" / "__init__.py"
    installed.parent.mkdir(parents=True)
    installed.touch()
    monkeypatch.setattr(piper_studio, "__file__", str(installed))

    with pytest.raises(PiperError, match="not a release directory"):
        launch.release_root()


def test_general_takes_the_host_out_of_every_production() -> None:
    variables = launch.maya_variables(GENERAL_PROFILE)

    assert variables[PRODUCTION_ENV] is None
    assert variables["PXR_AR_DEFAULT_SEARCH_PATH"] is None
    assert variables["MAYA_MODULE_PATH"] is None
    assert launch.working_directory(GENERAL_PROFILE) is None


def test_a_production_pins_itself_into_the_session(tmp_path: Path) -> None:
    profile = production_at(tmp_path)

    variables = launch.maya_variables(profile)

    assert variables[PRODUCTION_ENV] == str(profile.path)
    assert variables["PXR_AR_DEFAULT_SEARCH_PATH"] == str(tmp_path)
    assert launch.working_directory(profile) == tmp_path


def test_a_production_adds_maya_tools_by_making_the_directory(tmp_path: Path) -> None:
    profile = production_at(tmp_path)
    assert launch.maya_variables(profile)["MAYA_MODULE_PATH"] is None

    tools = tmp_path / "tools" / "maya"
    tools.mkdir(parents=True)

    assert launch.maya_variables(profile)["MAYA_MODULE_PATH"] == str(tools)


def test_a_production_whose_storage_is_not_mounted_is_refused(tmp_path: Path) -> None:
    profile = production_at(tmp_path / "missing")

    with pytest.raises(PiperError, match="not a directory"):
        launch.working_directory(profile)
