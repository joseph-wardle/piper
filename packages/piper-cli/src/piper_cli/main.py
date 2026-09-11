"""Entry point for the ``piper`` command."""

import sys
from collections.abc import Sequence
from typing import Annotated

from cyclopts import App, Parameter

import piper
from piper.errors import PiperError
from piper.find import find
from piper_cli import render
from piper_studio.create import PartialCreateAssetError, UnknownFolderError, create_asset
from piper_studio.production import load_production
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


@app.command(name="find")
def find_command(
    query: str = "",
    /,
    *,
    as_json: Annotated[bool, Parameter(name="--json")] = False,
) -> None:
    """Find assets and shots by name."""
    production = load_production()
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
    production = load_production()
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


def main(tokens: Sequence[str] | None = None) -> int:
    try:
        app(tokens)
    except PiperError as exc:
        print(f"piper: {exc}", file=sys.stderr)
        return 1
    return 0
