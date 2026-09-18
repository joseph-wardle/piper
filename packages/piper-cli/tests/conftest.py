from collections.abc import Callable
from pathlib import Path

import pytest

from piper.registry import Registry
from piper.tracker import Tracker
from piper_studio.production import PRODUCTION_ENV

_PRODUCTION = """
name = "sandwich"
root = "{root}"
types = ["Prop", "Set Piece"]

[shotgrid]
site = "https://byuanimation.shotgunstudio.com"
script = "sandwich_pipeline"
project = 716
"""


@pytest.fixture(autouse=True)
def config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A configuration directory of the test's own, so no selection of mine leaks in."""
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "config"))
    return tmp_path / "config"


@pytest.fixture
def root(tmp_path: Path) -> Path:
    """The production's storage root."""
    root = tmp_path / "production"
    root.mkdir()
    return root


@pytest.fixture
def production(root: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A real configuration file the command loads for itself."""
    path = tmp_path / "production.toml"
    path.write_text(_PRODUCTION.format(root=root), encoding="utf-8")
    monkeypatch.setenv(PRODUCTION_ENV, str(path))
    return path


@pytest.fixture
def run(
    production: Path, registry: Registry, monkeypatch: pytest.MonkeyPatch
) -> Callable[..., int]:
    """Runs ``piper`` against a tracker the test supplies, and the ``registry`` fixture."""
    from piper_cli import main as cli

    def run_against(answers: Tracker, *tokens: str) -> int:
        monkeypatch.setattr(cli, "tracker_for", lambda _production: answers)
        monkeypatch.setattr(cli, "registry_for", lambda _production: registry)
        return cli.main(tokens)

    return run_against
