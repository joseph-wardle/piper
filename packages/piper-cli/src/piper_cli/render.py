"""Presents find results."""

import json

from rich import box
from rich.console import Console
from rich.table import Table
from rich.text import Text

from piper.find import Matches

_MISSING = "—"


def as_json(matches: Matches) -> None:
    """Write the result to stdout as one JSON object."""
    payload = {
        "assets": [
            {"id": asset.id, "name": asset.name, "kind": asset.kind} for asset in matches.assets
        ],
        "shots": [
            {"id": shot.id, "name": shot.name, "sequence": shot.sequence} for shot in matches.shots
        ],
    }
    print(json.dumps(payload))


def as_tables(matches: Matches, query: str) -> None:
    """Write the result to stdout as tables a person can read."""
    console = Console()
    if not matches.assets and not matches.shots:
        console.print(_nothing_found(query), markup=False, highlight=False)
        return
    if matches.assets:
        rows = [(asset.name, asset.kind) for asset in matches.assets]
        console.print(_table("Asset", "Kind", rows))
    if matches.shots:
        if matches.assets:
            console.print()
        rows = [(shot.name, shot.sequence) for shot in matches.shots]
        console.print(_table("Shot", "Sequence", rows))


def _nothing_found(query: str) -> str:
    if query:
        return f"No assets or shots matching {query!r}."
    return "No assets or shots in this production."


def _table(name_header: str, detail_header: str, rows: list[tuple[str, str | None]]) -> Table:
    table = Table(box=box.SIMPLE_HEAD, show_edge=False, pad_edge=False)
    table.add_column(name_header)
    table.add_column(detail_header)
    for name, detail in rows:
        # Names come from the tracker and may contain Rich's markup brackets.
        table.add_row(Text(name), Text(detail if detail else _MISSING))
    return table
