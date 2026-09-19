"""Reads and creates entities in a ShotGrid project through Piper's tracker contract.

ShotGrid field names, filter syntax, entity dictionaries, and exceptions do not
leave this module.
"""

import http.client
from dataclasses import replace
from typing import Any, cast

import shotgun_api3
from shotgun_api3.lib import httplib2

from piper.errors import TrackerError
from piper.tracker import Asset, Shot

_ASSET_FIELDS = ["id", "code", "sg_asset_type", "sg_subdirectory", "sg_pipe_name"]
_SHOT_FIELDS = ["id", "code", "sg_sequence"]

# `shotgun_api3` re-raises whatever its transport raised, so `ShotgunError`
# alone leaks. An HTTP status of 300 or worse arrives as `shotgun_api3.Error`
# (`xmlrpc.client.Error`), and an unresolvable host as
# `httplib2.ServerNotFoundError`, neither of which derives from it.
REQUEST_FAILURES = (
    shotgun_api3.ShotgunError,
    shotgun_api3.Error,
    httplib2.HttpLib2Error,
    http.client.HTTPException,
    OSError,
)


class ShotGridTracker:
    """One ShotGrid project, and nothing else on its site."""

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
        return tuple(sorted((_asset(entity) for entity in entities), key=_by_name))

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

    def asset(self, id: str) -> Asset | None:
        if not id.isdecimal():
            return None
        filters = [["project", "is", self._project_link()], ["id", "is", int(id)]]
        try:
            entity = self._shotgrid.find_one("Asset", filters, _ASSET_FIELDS)
        except REQUEST_FAILURES as exc:
            raise TrackerError(
                f"shotgrid: cannot read asset {id} of project {self._project} "
                f"from {self._site} ({exc})"
            ) from exc
        return _asset(cast("dict[str, Any]", entity)) if entity else None

    def create_asset(self, name: str, *, type: str, folder: str, pipe_name: str) -> Asset:
        data = {
            "project": self._project_link(),
            "code": name,
            "sg_asset_type": type,
            "sg_subdirectory": folder,
            "sg_pipe_name": pipe_name,
        }
        try:
            entity = self._shotgrid.create("Asset", data, _ASSET_FIELDS)
        except REQUEST_FAILURES as exc:
            raise TrackerError(
                f"shotgrid: cannot create asset {name!r} in project {self._project} "
                f"on {self._site} ({exc})"
            ) from exc
        return _asset(entity)

    def set_pipe_name(self, asset: Asset, pipe_name: str) -> Asset:
        try:
            self._shotgrid.update("Asset", int(asset.id), {"sg_pipe_name": pipe_name})
        except REQUEST_FAILURES as exc:
            raise TrackerError(
                f"shotgrid: cannot give asset {asset.name!r} the pipe name {pipe_name!r} "
                f"in project {self._project} on {self._site} ({exc})"
            ) from exc
        return replace(asset, pipe_name=pipe_name)

    def _find(
        self, entity_type: str, fields: list[str], name_contains: str
    ) -> list[dict[str, Any]]:
        filters: list[list[Any]] = [["project", "is", self._project_link()]]
        if name_contains:
            filters.append(["code", "contains", name_contains])
        try:
            found = self._shotgrid.find(entity_type, filters, fields)
        except REQUEST_FAILURES as exc:
            raise TrackerError(
                f"shotgrid: cannot read {entity_type.lower()}s of project "
                f"{self._project} from {self._site} ({exc})"
            ) from exc
        # `shotgun_api3` types its results as a TypedDict declaring only `id`
        # and `type`, but returns every field asked for.
        return cast("list[dict[str, Any]]", found)

    def _project_link(self) -> dict[str, Any]:
        return {"type": "Project", "id": self._project}


def _asset(entity: dict[str, Any]) -> Asset:
    return Asset(
        id=str(entity["id"]),
        name=entity["code"] or "",
        type=entity["sg_asset_type"] or None,
        folder=entity["sg_subdirectory"] or None,
        pipe_name=entity["sg_pipe_name"] or None,
    )


def _by_name(entity: Asset | Shot) -> str:
    return entity.name.casefold()


def _linked_name(link: dict[str, Any] | None) -> str | None:
    return link["name"] if link else None
