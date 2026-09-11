from collections.abc import Callable
from pathlib import Path

import pytest

from piper.tracker import Tracker
from piper_studio.production import PRODUCTION_ENV

_PRODUCTION = """
name = "sandwich"

[shotgrid]
site = "https://byuanimation.shotgunstudio.com"
script = "sandwich_pipeline"
project = 716
"""


@pytest.fixture
def production(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A real configuration file the command loads for itself."""
    path = tmp_path / "production.toml"
    path.write_text(_PRODUCTION, encoding="utf-8")
    monkeypatch.setenv(PRODUCTION_ENV, str(path))
    return path


@pytest.fixture
def run(production: Path, monkeypatch: pytest.MonkeyPatch) -> Callable[..., int]:
    """Runs ``piper`` against a tracker the test supplies."""
    from piper_cli import main as cli

    def run_against(answers: Tracker, *tokens: str) -> int:
        monkeypatch.setattr(cli, "tracker_for", lambda _production: answers)
        return cli.main(tokens)

    return run_against
