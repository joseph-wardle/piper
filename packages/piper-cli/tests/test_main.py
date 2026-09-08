import pytest

import piper
from piper_cli.main import main


def test_without_arguments_prints_help(capsys: pytest.CaptureFixture[str]) -> None:
    assert main([]) == 0
    assert "usage: piper" in capsys.readouterr().out


def test_version_flag_reports_the_core_version(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exit_info:
        main(["--version"])

    assert exit_info.value.code == 0
    assert piper.__version__ in capsys.readouterr().out
