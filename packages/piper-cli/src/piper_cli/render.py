"""Presents operation results."""

import json
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING

from rich import box
from rich.console import Console
from rich.table import Table
from rich.text import Text

from piper.find import Matches
from piper.tracker import Asset
from piper_studio.create import CreateAssetResult
from piper_studio.production import PRODUCTION_ENV
from piper_studio.profile import Profile

if TYPE_CHECKING:
    from piper_studio.publish import ProductVersion, PublishResult, PublishTexturesResult

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
        "created": {
            "tracker": result.asset_created,
            "pipe_name": result.pipe_name_given,
            "directory": result.directory_created,
        },
    }
    if error is not None:
        payload["error"] = error
    print(json.dumps(payload))


def create_asset_result_as_text(result: CreateAssetResult) -> None:
    """Write what a create made, and what it found already there."""
    asset = result.asset
    verb = "Created" if result.asset_created else "Found"
    print(f"{verb} asset {asset.name!r} ({asset.type}, in {asset.folder})")
    if result.pipe_name_given and not result.asset_created:
        print(f"Named its paths {asset.pipe_name!r}")
    verb = "Created" if result.directory_created else "Found"
    print(f"{verb} {result.directory}")


def publish_result_as_json(result: "PublishResult", error: str | None = None) -> None:
    """Write what a publish left to stdout as one JSON object."""
    payload: dict[str, object] = {"asset": _asset_json(result.component.asset)}
    payload.update(_composition_json(result))
    if error is not None:
        payload["error"] = error
    print(json.dumps(payload))


def publish_result_as_text(result: "PublishResult") -> None:
    """Write which versions a publish installed, their root layers, and what is current."""
    from piper_studio.publish import composition_line, published_line

    print(published_line(result.component))
    print(result.component.path)
    print(composition_line(result))
    if result.asset_version is not None:
        print(result.asset_version.path)


def publish_textures_result_as_json(
    result: "PublishTexturesResult", error: str | None = None
) -> None:
    """Write what a texture publish left to stdout as one JSON object."""
    payload: dict[str, object] = {
        "asset": _asset_json(result.textures.asset),
        "textures": _version_json(result.textures),
        "material": _composition_json(result.material) if result.material else None,
        "derived_from": result.derived_from,
        "warnings": list(result.warnings),
    }
    if error is not None:
        payload["error"] = error
    print(json.dumps(payload))


def publish_textures_result_as_text(result: "PublishTexturesResult") -> None:
    """Write which textures were installed, which material now reads them, and what to know."""
    from piper_studio.publish import composition_line, derived_line, published_line

    print(published_line(result.textures))
    print(result.textures.path)
    if result.material is not None and result.derived_from is not None:
        print(derived_line(result.material.component, result.derived_from))
        print(composition_line(result.material))
        if result.material.asset_version is not None:
            print(result.material.asset_version.path)
    for warning in result.warnings:
        print(warning)


def _composition_json(result: "PublishResult") -> dict[str, object]:
    return {
        "component": _version_json(result.component),
        "asset_version": _version_json(result.asset_version) if result.asset_version else None,
        "pins": dict(result.pins),
        "current": result.current,
    }


def current_as_json(asset: Asset, version: int | None, pins: Mapping[str, int]) -> None:
    """Write which asset version is current, and its pins, as one JSON object."""
    print(json.dumps({"asset": _asset_json(asset), "version": version, "pins": dict(pins)}))


def current_as_text(asset: Asset, version: int | None, pins: Mapping[str, int]) -> None:
    """Write which asset version consumers of ``asset`` get by default."""
    from piper_studio.compose import current_line

    print(f"{asset.name!r}: {current_line(version, pins)}")


def _version_json(version: "ProductVersion") -> dict[str, object]:
    return {
        "product": version.product,
        "version": version.version,
        "path": str(version.path),
        "record_id": version.record_id,
    }


def profile_as_text(profile: Profile, overridden: str | None) -> None:
    """Write which production later commands work in.

    ``overridden`` names the configured profile that ``PIPER_PRODUCTION`` outranks.
    """
    print(profile.name if profile.path is None else f"{profile.name}  {profile.path}")
    if overridden is not None:
        print(f"{PRODUCTION_ENV} overrides the configured profile {overridden!r}")


def _asset_json(asset: Asset) -> dict[str, str | None]:
    return {
        "id": asset.id,
        "name": asset.name,
        "type": asset.type,
        "folder": asset.folder,
        "pipe_name": asset.pipe_name,
    }


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
