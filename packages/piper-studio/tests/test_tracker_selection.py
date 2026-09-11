from pathlib import PurePosixPath

import pytest

from piper.errors import ConfigError
from piper_shotgrid.tracker import ShotGridTracker
from piper_studio.production import Production, ShotGridConfig
from piper_studio.tracker import SHOTGRID_KEY_ENV, tracker_for

_PRODUCTION = Production(
    name="sandwich",
    root=PurePosixPath("/groups/sandwich/05_production"),
    types=("Set Piece",),
    shotgrid=ShotGridConfig(
        site="https://byuanimation.shotgunstudio.com",
        script="sandwich_pipeline",
        project=716,
    ),
)


def test_a_shotgrid_production_selects_the_shotgrid_tracker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(SHOTGRID_KEY_ENV, "not-a-real-key")

    assert isinstance(tracker_for(_PRODUCTION), ShotGridTracker)


def test_a_missing_credential_names_the_variable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(SHOTGRID_KEY_ENV, raising=False)

    with pytest.raises(ConfigError, match=SHOTGRID_KEY_ENV):
        tracker_for(_PRODUCTION)
