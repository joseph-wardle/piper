"""Entry point for the ``piper`` command."""

import sys
from collections.abc import Sequence
from pathlib import Path, PurePosixPath
from typing import Annotated

from cyclopts import App, Parameter

import piper
from piper.errors import PiperError
from piper.find import find
from piper.tracker import Asset, Tracker
from piper_cli import render
from piper_studio import launch, profile
from piper_studio.create import PartialCreateAssetError, UnknownFolderError, create_asset
from piper_studio.production import Production
from piper_studio.registry import registry_for
from piper_studio.tracker import tracker_for

# `result_action` off: Cyclopts otherwise calls `sys.exit` for the command, and
# `main` owns Piper's exit codes. `negative` off: every flag is off unless given,
# so a `--no-` form would only clutter the help.
app = App(
    name="piper",
    help=piper.__doc__,
    version=piper.__version__,
    result_action="return_none",
    default_parameter=Parameter(negative=()),
)

create_app = App(name="create", help="Create production entities.")
app.command(create_app)

launch_app = App(name="launch", help="Launch an application with Piper's code loaded.")
app.command(launch_app)


@app.command(name="configure")
def configure_command(name: str = "", /) -> None:
    """Select the production later commands work in, or report the current selection.

    Parameters
    ----------
    name
        A production's name, or general to work outside every production.
    """
    if name:
        profile.select(name)
    overridden = profile.selected() if profile.override() is not None else None
    render.profile_as_text(profile.active(), overridden)


@launch_app.command(name="maya")
def launch_maya_command() -> None:
    """Become Maya, working in the active profile, with Piper's code loaded."""
    launch.maya(profile.active())


@app.command(name="find")
def find_command(
    query: str = "",
    /,
    *,
    as_json: Annotated[bool, Parameter(name="--json")] = False,
) -> None:
    """Find assets and shots by name."""
    production = _active_production()
    matches = find(tracker_for(production), query)
    if as_json:
        render.as_json(matches)
    else:
        render.as_tables(matches, query)


@create_app.command(name="asset")
def create_asset_command(
    name: str,
    /,
    *,
    type: str,
    folder: str,
    new_folder: bool = False,
    as_json: Annotated[bool, Parameter(name="--json")] = False,
) -> None:
    """Create an asset in the tracker and its directory in storage.

    Running it again finishes whichever half is missing.

    Parameters
    ----------
    name
        The asset's name, as artists will read it.
    type
        What the asset is, from the production's asset types.
    folder
        Where artists browse for the asset, such as kitchen.
    new_folder
        Start a folder no asset is in yet.
    as_json
        Write the result as JSON for another program.
    """
    production = _active_production()
    try:
        result = create_asset(
            tracker_for(production),
            root=production.root,
            types=production.types,
            name=name,
            type=type,
            folder=folder,
            new_folder=new_folder,
        )
    except UnknownFolderError as exc:
        raise PiperError(f"{exc}; pass --new-folder to start it") from exc
    except PartialCreateAssetError as exc:
        if as_json:
            render.create_asset_result_as_json(exc.result, error=str(exc))
        raise
    if as_json:
        render.create_asset_result_as_json(result)
    else:
        render.create_asset_result_as_text(result)


@app.command(name="publish")
def publish_command(
    asset: str,
    product: str,
    layer: Path,
    /,
    *,
    as_json: Annotated[bool, Parameter(name="--json")] = False,
) -> None:
    """Install an exported USD layer as the next version of an asset's product, and register it.

    Parameters
    ----------
    asset
        The asset's name, spelled exactly as the tracker spells it.
    product
        What the layer holds, such as geo or mtl.
    layer
        The exported USD layer.
    as_json
        Write the result as JSON for another program.
    """
    # Imported here: loading USD takes most of a second, and no other command needs it.
    from piper_studio.publish import PartialPublishError, publish

    production = _active_production()
    try:
        result = publish(
            registry_for(production),
            root=production.root,
            asset=_asset_named(tracker_for(production), asset),
            product=product,
            layer=PurePosixPath(layer),
        )
    except PartialPublishError as exc:
        if as_json:
            render.publish_result_as_json(exc.result, error=str(exc))
        raise
    if as_json:
        render.publish_result_as_json(result)
    else:
        render.publish_result_as_text(result)


def _active_production() -> Production:
    """The active profile's production, for a command that cannot work without one."""
    active = profile.active()
    if active.production is None:
        known = ", ".join(sorted(profile.PRODUCTIONS))
        raise PiperError(
            f"{active.name} has no production; run `piper configure` with one of: {known}"
        )
    return active.production


def _asset_named(tracker: Tracker, name: str) -> Asset:
    found = tracker.find_assets(name)
    named = [asset for asset in found if asset.name == name]
    if len(named) == 1:
        return named[0]
    if named:
        raise PiperError(f"{len(named)} assets are named {name!r}; rename all but one")
    similar = ", ".join(repr(asset.name) for asset in found)
    raise PiperError(
        f"no asset is named {name!r}" + (f" (names containing it: {similar})" if similar else "")
    )


def main(tokens: Sequence[str] | None = None) -> int:
    try:
        app(tokens)
    except PiperError as exc:
        print(f"piper: {exc}", file=sys.stderr)
        return 1
    return 0
