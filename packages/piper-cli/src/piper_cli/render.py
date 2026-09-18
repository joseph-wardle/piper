"""Presents operation results."""

import json
from collections.abc import Sequence
from typing import TYPE_CHECKING

from rich import box
from rich.console import Console
from rich.table import Table
from rich.text import Text

from piper.find import Matches
from piper.tracker import Asset
from piper_studio.create import CreateAssetResult
from piper_studio.layout import version_name
from piper_studio.production import PRODUCTION_ENV
from piper_studio.profile import Profile

if TYPE_CHECKING:
    # For its type only: importing the module loads USD.
    from piper_studio.publish import PublishResult

_MISSING = "—"


def as_json(matches: Matches) -> None:
    """Write the result to stdout as one JSON object."""
    payload = {
        "assets": [_asset_json(asset) for asset in matches.assets],
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
        rows = [(asset.name, asset.type, asset.folder) for asset in matches.assets]
        console.print(_table(("Asset", "Type", "Folder"), rows))
    if matches.shots:
        if matches.assets:
            console.print()
        rows = [(shot.name, shot.sequence) for shot in matches.shots]
        console.print(_table(("Shot", "Sequence"), rows))


def create_asset_result_as_json(result: CreateAssetResult, error: str | None = None) -> None:
    """Write what a create left behind to stdout as one JSON object.

    ``error`` accompanies a create that left the asset without its directory.
    """
    payload: dict[str, object] = {
        "asset": _asset_json(result.asset),
        "directory": str(result.directory),
        "created": {"tracker": result.asset_created, "directory": result.directory_created},
    }
    if error is not None:
        payload["error"] = error
    print(json.dumps(payload))


def create_asset_result_as_text(result: CreateAssetResult) -> None:
    """Write what a create made, and what it found already there."""
    asset = result.asset
    verb = "Created" if result.asset_created else "Found"
    print(f"{verb} asset {asset.name!r} ({asset.type}, in {asset.folder})")
    verb = "Created" if result.directory_created else "Found"
    print(f"{verb} {result.directory}")


def publish_result_as_json(result: "PublishResult", error: str | None = None) -> None:
    """Write the version a publish installed to stdout as one JSON object."""
    payload: dict[str, object] = {
        "asset": _asset_json(result.asset),
        "product": result.product,
        "version": result.version,
        "path": str(result.path),
        "record_id": result.record_id,
    }
    if error is not None:
        payload["error"] = error
    print(json.dumps(payload))


def publish_result_as_text(result: "PublishResult") -> None:
    """Write which version a publish installed, and its root layer."""
    version = version_name(result.version)
    print(f"Published {result.product} {version} of {result.asset.name!r}")
    print(result.path)


def profile_as_text(profile: Profile, overridden: str | None) -> None:
    """Write which production later commands work in.

    ``overridden`` names the configured profile that ``PIPER_PRODUCTION`` outranks.
    """
    print(profile.name if profile.path is None else f"{profile.name}  {profile.path}")
    if overridden is not None:
        print(f"{PRODUCTION_ENV} overrides the configured profile {overridden!r}")


def _asset_json(asset: Asset) -> dict[str, str | None]:
    return {"id": asset.id, "name": asset.name, "type": asset.type, "folder": asset.folder}


def _nothing_found(query: str) -> str:
    if query:
        return f"No assets or shots matching {query!r}."
    return "No assets or shots in this production."


def _table(headers: tuple[str, ...], rows: Sequence[tuple[str | None, ...]]) -> Table:
    table = Table(box=box.SIMPLE_HEAD, show_edge=False, pad_edge=False)
    for header in headers:
        table.add_column(header)
    for row in rows:
        # Names come from the tracker and may contain Rich's markup brackets.
        table.add_row(*(Text(cell or _MISSING) for cell in row))
    return table
