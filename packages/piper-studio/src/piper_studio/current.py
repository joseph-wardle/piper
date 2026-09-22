"""What consumers get by default: one layer per asset, sublayering the current asset version."""

import secrets
from pathlib import Path, PurePosixPath

# `.usda` is a format of the `usd` plugin. The usd-core wheel cannot load that plugin
# on demand; importing `Usd` loads it.
from pxr import Sdf, Tf, Usd  # noqa: F401

from piper.errors import PiperError
from piper.tracker import Asset
from piper_studio import compose, layout
from piper_studio.storage import asset_directory


def current(root: PurePosixPath, asset: Asset) -> int | None:
    """The asset version consumers get by default, or None when none is current."""
    path = current_path(root, asset)
    if not path.is_file():
        return None
    layer = Sdf.Layer.OpenAsAnonymous(str(path))
    sublayers = list(layer.subLayerPaths) if layer is not None else []
    number = layout.version_number(PurePosixPath(sublayers[0]).parent.name) if sublayers else None
    if number is None or len(sublayers) != 1:
        raise PiperError(f"{path} is not a current layer Piper wrote; `piper current` rewrites it")
    return number


def make_current(root: PurePosixPath, asset: Asset, version: int) -> None:
    """Make an installed asset version what consumers get by default."""
    entry = compose.entry_path(root, asset, version)
    if not entry.is_file():
        installed = ", ".join(
            layout.version_name(n) for n in compose.versions(root, asset, compose.ASSET)
        )
        raise PiperError(
            f"{asset.name} has no {compose.ASSET} {layout.version_name(version)} "
            f"(installed: {installed or 'none'})"
        )
    path = current_path(root, asset)
    # Written whole beside its place, then renamed over it: a reader sees the old
    # layer or the new one, never a partial file.
    temporary = path.with_name(f".tmp_{secrets.token_hex(4)}_{path.name}")
    try:
        layer = Sdf.Layer.CreateNew(str(temporary))
        layer.subLayerPaths = [f"./{entry.parent.name}/{entry.name}"]
        installed_entry = Sdf.Layer.OpenAsAnonymous(str(entry))
        for name in ("defaultPrim", "upAxis", "metersPerUnit"):
            if installed_entry.pseudoRoot.HasInfo(name):
                layer.pseudoRoot.SetInfo(name, installed_entry.pseudoRoot.GetInfo(name))
        layer.Save()
        temporary.replace(path)
    except Tf.ErrorException as exc:
        raise PiperError(f"could not write {temporary}: {_usd_error(exc)}") from exc
    except OSError as exc:
        raise PiperError(f"could not replace {path} with {temporary} ({exc.strerror})") from exc


def _usd_error(exc: Tf.ErrorException) -> str:
    return "; ".join(error.commentary.strip() for error in exc.args)


def current_path(root: PurePosixPath, asset: Asset) -> Path:
    directory = asset_directory(root, asset)
    return Path(
        layout.product_root(PurePosixPath(directory), compose.ASSET), f"{directory.name}.usda"
    )
