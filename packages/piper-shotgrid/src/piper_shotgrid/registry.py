"""Records installed product versions as ShotGrid PublishedFiles through Piper's registry contract.

ShotGrid field names, filter syntax, entity dictionaries, and exceptions do not
leave this module.
"""

import getpass
from pathlib import Path, PurePosixPath
from typing import Any

import shotgun_api3

from piper.errors import RegistryError
from piper.tracker import Asset
from piper_shotgrid.tracker import REQUEST_FAILURES

_TIMEOUT_SECONDS = 30


class ShotGridRegistry:
    """PublishedFiles in one ShotGrid project, and nothing else on its site."""

    def __init__(self, *, site: str, script: str, key: str, project: int) -> None:
        self._site = site
        self._project = project
        try:
            self._shotgrid = shotgun_api3.Shotgun(
                site, script_name=script, api_key=key, connect=False
            )
        except ValueError as exc:
            raise RegistryError(f"shotgrid: {site} is not a usable site address ({exc})") from exc
        self._shotgrid.config.timeout_secs = _TIMEOUT_SECONDS

    def register(self, asset: Asset, *, product: str, version: int, path: PurePosixPath) -> str:
        code = f"{product} v{version:03d}"
        described = f"{code} of {asset.name!r} in project {self._project} on {self._site}"
        project = {"type": "Project", "id": self._project}
        entity = {"type": "Asset", "id": int(asset.id)}
        filters: list[list[Any]] = [
            ["project", "is", project],
            ["entity", "is", entity],
            ["name", "is", product],
            ["version_number", "is", version],
        ]
        try:
            existing = self._shotgrid.find_one("PublishedFile", filters, ["id"])
        except REQUEST_FAILURES as exc:
            raise RegistryError(
                f"shotgrid: cannot check whether {described} is already registered ({exc})"
            ) from exc
        if existing:
            raise RegistryError(
                f"shotgrid: {described} is already registered as PublishedFile "
                f"{existing['id']}, so its version number was used before"
            )

        data = {
            "project": project,
            "entity": entity,
            "name": product,
            "version_number": version,
            "code": code,
            "path": {"url": Path(path).as_uri(), "name": path.name},
            # `created_by` is the script for every publish, so the OS login is recorded here.
            "description": f"published by {getpass.getuser()}",
        }
        try:
            created = self._shotgrid.create("PublishedFile", data, ["id"])
        except shotgun_api3.Fault as exc:
            raise RegistryError(f"shotgrid: refused to register {described} ({exc})") from exc
        except REQUEST_FAILURES as exc:
            # The request may have reached the site, and `shotgun_api3` resends a
            # create after some failures, so the record may exist, even twice.
            raise RegistryError(
                f"shotgrid: registering {described} may or may not have succeeded ({exc}); "
                f"check ShotGrid for {code!r} on {asset.name!r}"
            ) from exc
        return str(created["id"])
