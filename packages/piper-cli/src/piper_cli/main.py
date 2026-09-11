"""Entry point for the ``piper`` command."""

import sys
from collections.abc import Sequence
from typing import Annotated

from cyclopts import App, Parameter

import piper
from piper.errors import PiperError
from piper.find import find
from piper_cli import render
from piper_studio.production import load_production
from piper_studio.tracker import tracker_for

# `result_action` off: Cyclopts otherwise calls `sys.exit` for the command, and
# `main` owns Piper's exit codes.
app = App(
    name="piper",
    help=piper.__doc__,
    version=piper.__version__,
    result_action="return_none",
)


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


def main(tokens: Sequence[str] | None = None) -> int:
    try:
        app(tokens)
    except PiperError as exc:
        print(f"piper: {exc}", file=sys.stderr)
        return 1
    return 0
