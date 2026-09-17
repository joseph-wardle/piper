"""Constructs the tracker a production's configuration selects."""

import os

from piper.errors import ConfigError
from piper.tracker import Tracker
from piper_shotgrid.tracker import ShotGridTracker
from piper_studio.production import Production

SHOTGRID_KEY_ENV = "PIPER_SHOTGRID_KEY"


def tracker_for(production: Production) -> Tracker:
    """Construct the production's tracker. Talks to nothing yet."""
    return ShotGridTracker(
        site=production.shotgrid.site,
        script=production.shotgrid.script,
        key=shotgrid_key(),
        project=production.shotgrid.project,
    )


def shotgrid_key() -> str:
    """The ShotGrid script key, which stays out of production configuration."""
    key = os.environ.get(SHOTGRID_KEY_ENV)
    if not key:
        raise ConfigError(f"shotgrid credentials: {SHOTGRID_KEY_ENV} is not set")
    return key
