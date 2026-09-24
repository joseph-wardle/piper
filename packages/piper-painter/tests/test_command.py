import subprocess
from typing import Any

import pytest

from piper.errors import PiperError
from piper_painter import command
from piper_studio.launch import PYTHON_ENV


@pytest.fixture
def ran(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    """Every command run, with the interpreter the launch handed over and a plugin path set."""
    monkeypatch.setenv(PYTHON_ENV, "C:/piper-venv/Scripts/python.exe")
    monkeypatch.setenv("PYTHONPATH", "H:/piper/packages/piper-painter/.venv/lib/site-packages")
    calls: list[dict[str, Any]] = []

    def run(argv: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append({"argv": argv, **kwargs})
        environment = kwargs["env"]
        assert isinstance(environment, dict)
        answer = environment.get("ANSWER", "")
        status = int(environment.get("STATUS", "0"))
        return subprocess.CompletedProcess(argv, status, stdout=answer, stderr="piper: no\n")

    monkeypatch.setattr(subprocess, "run", run)
    return calls


def test_piper_runs_in_the_interpreter_the_launch_named_without_painters_path(
    ran: list[dict[str, Any]], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("ANSWER", "Published tex v001 of 'Frying Pan'\n")

    said = command.run("publish", "Frying Pan", "tex", "C:/export")

    assert said == "Published tex v001 of 'Frying Pan'"
    [call] = ran
    assert call["argv"] == [
        "C:/piper-venv/Scripts/python.exe",
        "-m",
        "piper_cli",
        "publish",
        "Frying Pan",
        "tex",
        "C:/export",
    ]
    assert "PYTHONPATH" not in call["env"] and call["env"][PYTHON_ENV]


def test_a_refusal_is_pipers_own_words(
    ran: list[dict[str, Any]], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("STATUS", "1")

    with pytest.raises(PiperError, match=r"^no$"):
        command.run("publish", "Frying Pan", "tex", "C:/export")


def test_a_query_is_the_json_form(
    ran: list[dict[str, Any]], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("ANSWER", '{"version": 3, "pins": {"geo": 2}}')

    assert command.query("current", "Frying Pan") == {"version": 3, "pins": {"geo": 2}}
    assert ran[0]["argv"][-1] == "--json"


def test_painter_started_without_the_launch_is_told_how(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(PYTHON_ENV, raising=False)

    with pytest.raises(PiperError, match="piper launch painter"):
        command.run("current", "Frying Pan")
