import os
from pathlib import Path, PurePosixPath

import pytest

import piper_studio
from piper.errors import PiperError
from piper.tracker import Asset
from piper_studio import launch
from piper_studio.context import Context
from piper_studio.production import PRODUCTION_ENV, Production, ShotGridConfig, Software
from piper_studio.profile import GENERAL, Profile

GENERAL_PROFILE = Profile(name=GENERAL, production=None, path=None)


PAN = Asset(id="7701", name="Frying Pan", type="Prop", folder="kitchen", pipe_name="frying_pan")
MODELING = Context(name="modeling", subject="asset", host="maya", extension="mb")
LOOKDEV = Context(name="lookdev", subject="asset", host="houdini", extension="hipnc")


@pytest.fixture
def release(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A release directory Piper runs from, with each host's environment built."""
    release = tmp_path / "release"
    packages = release / "packages"
    (packages / "piper-core" / "src" / "piper").mkdir(parents=True)
    for host in ("maya", "houdini"):
        environment = packages / f"piper-{host}" / ".venv" / "lib" / "python3.11" / "site-packages"
        environment.mkdir(parents=True)
    installed = packages / "piper-studio" / "src" / "piper_studio" / "__init__.py"
    installed.parent.mkdir(parents=True)
    installed.touch()
    monkeypatch.setattr(piper_studio, "__file__", str(installed))
    return release


def production_at(root: Path, maya: str | None = None, houdini: str | None = None) -> Profile:
    """A profile whose production is stored under ``root``."""
    return Profile(
        name="sandwich",
        production=Production(
            name="sandwich",
            root=PurePosixPath(root),
            types=("Prop",),
            shotgrid=ShotGridConfig(site="https://example.invalid", script="piper", project=782),
            software=Software(maya=maya, houdini=houdini),
        ),
        path=root.parent / "production.toml",
    )


def test_piper_becomes_maya_with_its_own_variables_and_none_of_the_shells(
    tmp_path: Path, release: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    executable = tmp_path / "bin" / f"maya{launch.MAYA_VERSION}"
    executable.parent.mkdir()
    executable.touch(mode=0o755)
    monkeypatch.setenv("MAYA_LOCATION", str(tmp_path))
    monkeypatch.setenv("PYTHONPATH", "/venv/site-packages")
    monkeypatch.setenv("QT_PLUGIN_PATH", "/venv/plugins")
    monkeypatch.setenv("HOME", "/users/nobody")
    monkeypatch.delenv(PRODUCTION_ENV, raising=False)
    became: list[list[str]] = []
    handed: dict[str, str] = {}

    def execve(path: Path, argv: list[str], environment: dict[str, str]) -> None:
        became.append([str(path), *argv])
        handed.update(environment)

    monkeypatch.setattr(os, "execve", execve)

    launch.maya(GENERAL_PROFILE)

    command = 'python("import piper_maya; piper_maya.start()")'
    assert became == [[str(executable), str(executable), "-command", command]]
    assert handed["PYTHONPATH"] == launch.python_path("maya")
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


def test_maya_is_told_which_work_to_open_once_it_is_up() -> None:
    command = launch.maya_startup_command((PAN, MODELING))

    assert command == r'python("import piper_maya; piper_maya.start(\"7701\", \"modeling\")")'
    # Maya's launcher script drops every single quote an argument holds.
    assert "'" not in command


def test_piper_becomes_houdini_in_the_foreground_with_its_menu_on_the_path(
    tmp_path: Path, release: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    executable = tmp_path / "bin" / "houdini"
    executable.parent.mkdir()
    executable.touch(mode=0o755)
    monkeypatch.setenv("HFS", str(tmp_path))
    monkeypatch.setenv("PYTHONPATH", "/venv/site-packages")
    monkeypatch.setenv("QT_PLUGIN_PATH", "/venv/plugins")
    monkeypatch.setenv(launch.WORK_ENV, "7701 lookdev")
    monkeypatch.delenv(PRODUCTION_ENV, raising=False)
    became: list[list[str]] = []
    handed: dict[str, str] = {}

    def execve(path: Path, argv: list[str], environment: dict[str, str]) -> None:
        became.append([str(path), *argv])
        handed.update(environment)

    monkeypatch.setattr(os, "execve", execve)

    launch.houdini(GENERAL_PROFILE)

    assert became == [[str(executable), str(executable), "-foreground"]]
    assert handed["PYTHONPATH"] == launch.python_path("houdini")
    assert handed["HOUDINI_PATH"] == f"{release / 'packages' / 'piper-houdini'}{os.pathsep}&"
    assert "QT_PLUGIN_PATH" not in handed
    assert launch.WORK_ENV not in handed


def test_a_production_is_made_in_the_houdini_it_names(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("HFS", raising=False)
    profile = production_at(tmp_path, houdini="21.5")

    with pytest.raises(PiperError) as refusal:
        launch.houdini_executable(profile)

    refused = str(refusal.value)
    assert "sandwich needs Houdini 21.5" in refused
    assert "/opt/hfs21.5" in refused
    assert "set HFS" in refused


def test_houdini_is_told_which_work_to_open_once_it_is_up(release: Path) -> None:
    variables = launch.houdini_variables(GENERAL_PROFILE, work=(PAN, LOOKDEV))

    assert variables[launch.WORK_ENV] == "7701 lookdev"


def test_a_production_adds_houdini_packages_by_making_the_directory(
    tmp_path: Path, release: Path
) -> None:
    profile = production_at(tmp_path)
    assert launch.houdini_variables(profile)["HOUDINI_PACKAGE_DIR"] is None
    assert launch.houdini_variables(profile)["PXR_AR_DEFAULT_SEARCH_PATH"] == str(tmp_path)

    tools = tmp_path / "tools" / "houdini"
    tools.mkdir(parents=True)

    assert launch.houdini_variables(profile)["HOUDINI_PACKAGE_DIR"] == str(tools)


def test_a_host_imports_piper_from_its_sources_and_the_packages_its_integration_chose(
    release: Path,
) -> None:
    packages = release / "packages"

    assert launch.python_path("maya").split(os.pathsep) == [
        str(packages / "piper-core" / "src"),
        str(packages / "piper-shotgrid" / "src"),
        str(packages / "piper-studio" / "src"),
        str(packages / "piper-maya" / "src"),
        str(packages / "piper-maya" / ".venv" / "lib" / "python3.11" / "site-packages"),
    ]


def test_a_host_environment_that_was_never_built_is_refused_with_its_remedy(
    release: Path,
) -> None:
    (release / "packages" / "piper-maya" / ".venv" / "lib").rename(release / "elsewhere")

    with pytest.raises(PiperError, match=f"run `just sync` in {release}"):
        launch.python_path("maya")


def test_an_installed_piper_is_not_a_release(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    installed = tmp_path / "site-packages" / "piper_studio" / "__init__.py"
    installed.parent.mkdir(parents=True)
    installed.touch()
    monkeypatch.setattr(piper_studio, "__file__", str(installed))

    with pytest.raises(PiperError, match="not a release directory"):
        launch.release_root()


def test_general_takes_the_host_out_of_every_production(release: Path) -> None:
    variables = launch.maya_variables(GENERAL_PROFILE)

    assert variables[PRODUCTION_ENV] is None
    assert variables["PXR_AR_DEFAULT_SEARCH_PATH"] is None
    assert variables["MAYA_MODULE_PATH"] is None
    assert launch.working_directory(GENERAL_PROFILE) is None


def test_a_production_pins_itself_into_the_session(tmp_path: Path, release: Path) -> None:
    profile = production_at(tmp_path)

    variables = launch.maya_variables(profile)

    assert variables[PRODUCTION_ENV] == str(profile.path)
    assert variables["PXR_AR_DEFAULT_SEARCH_PATH"] == str(tmp_path)
    assert launch.working_directory(profile) == tmp_path


def test_a_production_adds_maya_tools_by_making_the_directory(
    tmp_path: Path, release: Path
) -> None:
    profile = production_at(tmp_path)
    assert launch.maya_variables(profile)["MAYA_MODULE_PATH"] is None

    tools = tmp_path / "tools" / "maya"
    tools.mkdir(parents=True)

    assert launch.maya_variables(profile)["MAYA_MODULE_PATH"] == str(tools)


def test_a_production_whose_storage_is_not_mounted_is_refused(tmp_path: Path) -> None:
    profile = production_at(tmp_path / "missing")

    with pytest.raises(PiperError, match="not a directory"):
        launch.working_directory(profile)


def test_a_preview_is_opened_by_the_production_houdinis_usdview_without_mayas_libraries(
    tmp_path: Path, release: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    houdini = tmp_path / "hfs" / "bin"
    houdini.mkdir(parents=True)
    (houdini / "houdini").touch(mode=0o755)
    monkeypatch.setenv("HFS", str(houdini.parent))
    profile = production_at(tmp_path / "production")
    (tmp_path / "production").mkdir()

    command = launch.usdview_command(profile, tmp_path / "preview" / "frying_pan.usda")
    handed = launch.compose(
        {"LD_LIBRARY_PATH": "/usr/autodesk/maya2026/lib", "HOME": "/home/artist"},
        launch.usdview_variables(profile),
    )

    assert command == [
        str(houdini / "hython"),
        str(houdini / "usdview"),
        str(tmp_path / "preview" / "frying_pan.usda"),
    ]
    assert "LD_LIBRARY_PATH" not in handed
    assert handed["HOME"] == "/home/artist"
    assert handed["PXR_AR_DEFAULT_SEARCH_PATH"] == str(tmp_path / "production")
    assert handed["PYTHONPATH"] == launch.python_path("houdini")
    assert launch.working_directory(profile) == tmp_path / "production"
