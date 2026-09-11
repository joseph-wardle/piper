"""Reads a ShotGrid project through Piper's tracker contract.

ShotGrid field names, filter syntax, entity dictionaries, and exceptions do not
leave this module.
"""

import http.client
from typing import Any, cast

import shotgun_api3
from shotgun_api3.lib import httplib2

from piper.errors import TrackerError
from piper.tracker import Asset, Shot

_ASSET_FIELDS = ["id", "code", "sg_asset_type"]
_SHOT_FIELDS = ["id", "code", "sg_sequence"]

# `shotgun_api3` re-raises whatever its transport raised, so `ShotgunError`
# alone leaks. An HTTP status of 300 or worse arrives as `shotgun_api3.Error`
# (`xmlrpc.client.Error`), and an unresolvable host as
# `httplib2.ServerNotFoundError`, neither of which derives from it.
_REQUEST_FAILURES = (
    shotgun_api3.ShotgunError,
    shotgun_api3.Error,
    httplib2.HttpLib2Error,
    http.client.HTTPException,
    OSError,
)


class ShotGridTracker:
    """Reads one ShotGrid project."""

    def __init__(self, *, site: str, script: str, key: str, project: int) -> None:
        self._site = site
        self._project = project
        try:
            self._shotgrid = shotgun_api3.Shotgun(
                site, script_name=script, api_key=key, connect=False
            )
        except ValueError as exc:
            raise TrackerError(f"shotgrid: {site} is not a usable site address ({exc})") from exc

    def find_assets(self, name_contains: str) -> tuple[Asset, ...]:
        entities = self._find("Asset", _ASSET_FIELDS, name_contains)
        assets = (
            Asset(
                id=str(entity["id"]),
                name=entity["code"] or "",
                kind=entity["sg_asset_type"] or None,
            )
            for entity in entities
        )
        return tuple(sorted(assets, key=_by_name))

    def find_shots(self, name_contains: str) -> tuple[Shot, ...]:
        entities = self._find("Shot", _SHOT_FIELDS, name_contains)
        shots = (
            Shot(
                id=str(entity["id"]),
                name=entity["code"] or "",
                sequence=_linked_name(entity["sg_sequence"]),
            )
            for entity in entities
        )
        return tuple(sorted(shots, key=_by_name))

    def _find(
        self, entity_type: str, fields: list[str], name_contains: str
    ) -> list[dict[str, Any]]:
        filters: list[list[Any]] = [
            ["project", "is", {"type": "Project", "id": self._project}],
        ]
        if name_contains:
            filters.append(["code", "contains", name_contains])
        try:
            found = self._shotgrid.find(entity_type, filters, fields)
        except _REQUEST_FAILURES as exc:
            raise TrackerError(
                f"shotgrid: cannot read {entity_type.lower()}s of project "
                f"{self._project} from {self._site} ({exc})"
            ) from exc
        # `shotgun_api3` types its results as a TypedDict declaring only `id`
        # and `type`, but returns every field asked for.
        return cast("list[dict[str, Any]]", found)


def _by_name(entity: Asset | Shot) -> str:
    return entity.name.casefold()


def _linked_name(link: dict[str, Any] | None) -> str | None:
    return link["name"] if link else None
