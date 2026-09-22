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
from piper_studio.context import context_named
from piper_studio.create import PartialCreateAssetError, UnknownFolderError, create_asset
from piper_studio.production import Production
from piper_studio.registry import registry_for
from piper_studio.tracker import tracker_for
from piper_studio.work import prepare_work

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


@launch_app.command(name="houdini")
def launch_houdini_command() -> None:
    """Become Houdini, working in the active profile, with Piper's code loaded."""
    launch.houdini(profile.active())


@app.command(name="open")
def open_command(asset: str, context: str, /) -> None:
    """Become the context's host, with an asset's work in that context open.

    Parameters
    ----------
    asset
        The asset's name, or a part of it no other asset's name has.
    context
        The kind of work, such as modeling or lookdev.
    """
    production = _active_production()
    chosen = context_named(context, subject="asset")
    found = _asset_matching(tracker_for(production), asset)
    prepare_work(root=production.root, asset=found, context=chosen)
    print(f"Opening {chosen.name} work on {found.name!r}")
    become = launch.maya if chosen.host == "maya" else launch.houdini
    become(profile.active(), work=(found, chosen))


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
    pinned: Annotated[tuple[str, ...], Parameter(name="--with")] = (),
    as_json: Annotated[bool, Parameter(name="--json")] = False,
) -> None:
    """Publish a component: install the layer, build the asset version pinning it, make it current.

    The asset version pins what the current one pins, with this component
    replaced. A component nothing pins is left out.

    Parameters
    ----------
    asset
        The asset's name, spelled exactly as the tracker spells it.
    product
        What the layer holds, such as geo or mtl. The layer is named for it.
    layer
        The exported USD layer.
    pinned
        Another component's version to pin instead of the current one's, as geo=5.
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
            with_versions=_versions_pinned(pinned),
        )
    except PartialPublishError as exc:
        if as_json:
            render.publish_result_as_json(exc.result, error=str(exc))
        raise
    if as_json:
        render.publish_result_as_json(result)
    else:
        render.publish_result_as_text(result)


@app.command(name="current")
def current_command(
    asset: str,
    version: int | None = None,
    /,
    *,
    as_json: Annotated[bool, Parameter(name="--json")] = False,
) -> None:
    """Report which asset version consumers get by default, or make one the version they get.

    Parameters
    ----------
    asset
        The asset's name, spelled exactly as the tracker spells it.
    version
        The asset version to make current, such as 12. Without it, nothing moves.
    as_json
        Write the result as JSON for another program.
    """
    from piper_studio import compose
    from piper_studio.current import current, make_current

    production = _active_production()
    found = _asset_named(tracker_for(production), asset)
    if version is not None:
        make_current(production.root, found, version)
    number = current(production.root, found)
    pins = compose.pins(production.root, found, number) if number is not None else {}
    if as_json:
        render.current_as_json(found, number, pins)
    else:
        render.current_as_text(found, number, pins)


def _versions_pinned(pinned: tuple[str, ...]) -> dict[str, int]:
    """``--with geo=5`` as ``{"geo": 5}``."""
    versions: dict[str, int] = {}
    for token in pinned:
        product, _, number = token.partition("=")
        if not product or not number.isdigit():
            raise PiperError(f"spell --with as product=version, such as geo=5, not {token!r}")
        versions[product] = int(number)
    return versions


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


def _asset_matching(tracker: Tracker, name: str) -> Asset:
    """The asset ``name`` names whatever its case, or the only asset whose name contains it."""
    found = tracker.find_assets(name)
    named = [asset for asset in found if asset.name.casefold() == name.casefold()]
    if len(named) == 1:
        return named[0]
    if named:
        raise PiperError(f"{len(named)} assets are named {name!r}; rename all but one")
    if len(found) == 1:
        return found[0]
    if found:
        names = ", ".join(repr(asset.name) for asset in found)
        raise PiperError(f"{len(found)} assets have names containing {name!r}: {names}")
    raise PiperError(f"no asset has a name containing {name!r}")


def main(tokens: Sequence[str] | None = None) -> int:
    try:
        app(tokens)
    except PiperError as exc:
        print(f"piper: {exc}", file=sys.stderr)
        return 1
    return 0
