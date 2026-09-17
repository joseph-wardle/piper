"""Constructs the registry a production's configuration selects."""

from piper.registry import Registry
from piper_shotgrid.registry import ShotGridRegistry
from piper_studio.production import Production
from piper_studio.tracker import shotgrid_key


def registry_for(production: Production) -> Registry:
    """Construct the production's registry. Talks to nothing yet."""
    return ShotGridRegistry(
        site=production.shotgrid.site,
        script=production.shotgrid.script,
        key=shotgrid_key(),
        project=production.shotgrid.project,
    )
