import os
from pathlib import Path
from typing import NoReturn

import pytest

from piper_cli import main as cli
from piper_studio import launch


def test_a_maya_this_machine_does_not_have_is_reported_and_nothing_launches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    def execve(path: Path, _argv: list[str], _environment: dict[str, str]) -> NoReturn:
        raise AssertionError(f"piper became {path}")

    monkeypatch.setattr(os, "execve", execve)
    monkeypatch.setenv("MAYA_LOCATION", str(tmp_path))

    assert cli.main(("launch", "maya")) == 1

    assert capsys.readouterr().err == (
        f"piper: general needs Maya {launch.MAYA_VERSION}, which is not installed at "
        f"{tmp_path}; point MAYA_LOCATION at a Maya {launch.MAYA_VERSION} instead\n"
    )
