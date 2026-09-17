"""Creating an asset: its tracker entity first, then its directory."""

import os
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from piper.errors import PiperError
from piper.tracker import Asset, Tracker
from piper_studio.layout import asset_root, slug
from piper_studio.storage import make_directories


@dataclass(frozen=True, slots=True)
class CreateAssetResult:
    """An asset and its directory, and which of the two this create made."""

    asset: Asset
    directory: PurePosixPath
    asset_created: bool
    directory_created: bool


class UnknownFolderError(PiperError):
    """No asset is in the folder yet, and starting it was not asked for."""


class PartialCreateAssetError(PiperError):
    """The asset is in the tracker, but its directory could not be made."""

    def __init__(self, message: str, result: CreateAssetResult) -> None:
        super().__init__(message)
        self.result = result


def create_asset(
    tracker: Tracker,
    *,
    root: PurePosixPath,
    types: tuple[str, ...],
    name: str,
    type: str,
    folder: str,
    new_folder: bool = False,
) -> CreateAssetResult:
    """Create an asset in the tracker, then its directory beneath ``root``."""
    if not slug(name):
        raise PiperError(f"cannot name an asset {name!r}: a name needs a letter or digit")
    if "/" in folder:
        raise PiperError(f"folder {folder!r} contains '/': a folder is one name, not a path")
    if not slug(folder):
        raise PiperError(f"cannot name a folder {folder!r}: a name needs a letter or digit")
    if type not in types:
        raise PiperError(
            f"{type!r} is not an asset type in this production (types: {', '.join(types)})"
        )

    if not (Path(root).is_dir() and os.access(root, os.W_OK | os.X_OK)):
        raise PiperError(f"production root {root} is not a directory you can write to")

    folder = slug(folder)
    directory = asset_root(root, folder, name)
    directory_exists = Path(directory).is_dir()
    assets = tracker.find_assets("")
    existing = next((asset for asset in assets if slug(asset.name) == slug(name)), None)

    if existing is None:
        folders = sorted({slug(known.folder) for known in assets if known.folder})
        if folder not in folders and not new_folder:
            in_use = ", ".join(folders) or "none"
            raise UnknownFolderError(f"no asset is in folder {folder!r} (folders in use: {in_use})")
        asset = tracker.create_asset(name, type=type, folder=folder)
    else:
        if existing.type != type or slug(existing.folder or "") != folder:
            raise PiperError(
                f"asset {existing.name!r} already exists (type: {existing.type or 'none'}, "
                f"folder: {existing.folder or 'none'}); a create cannot move or reclassify it"
            )
        if directory_exists:
            raise PiperError(f"asset {existing.name!r} already exists at {directory}")
        asset = existing

    if not directory_exists:
        try:
            make_directories(Path(directory))
        except OSError as exc:
            unfinished = CreateAssetResult(
                asset, directory, asset_created=existing is None, directory_created=False
            )
            raise PartialCreateAssetError(
                f"asset {asset.name!r} is in the tracker, but {directory} could not be created "
                f"({exc.strerror}); create it again to finish",
                unfinished,
            ) from exc

    return CreateAssetResult(
        asset,
        directory,
        asset_created=existing is None,
        directory_created=not directory_exists,
    )
