"""An asset version: two layers pinning one version of each component, and which one is current."""

import secrets
from collections.abc import Mapping
from pathlib import Path, PurePosixPath

# Unused, but importing Usd loads the plugin that reads .usda, which usd-core cannot load on demand.
from pxr import Sdf, Tf, Usd  # noqa: F401

from piper.errors import PiperError
from piper.tracker import Asset
from piper_studio import layout
from piper_studio.storage import asset_directory

ASSET = "asset"
GEOMETRY = "geo"
MATERIAL = "mtl"
_PAYLOAD = "payload.usda"
_STAGE_METADATA = ("upAxis", "metersPerUnit")


def write_asset_version(
    directory: Path, *, root: PurePosixPath, asset: Asset, pins: Mapping[str, int]
) -> Path:
    """Write the entry and payload pinning ``pins`` into ``directory``."""
    references = []
    for product, version in sorted(pins.items()):
        spelled = layer_path(root, asset, product, version)
        references.append((str(spelled), Path(root / spelled)))
    return _write_entry(directory, root, asset, references)


def write_preview(
    directory: Path, *, root: PurePosixPath, asset: Asset, pins: Mapping[str, int], layer: Path
) -> Path:
    """Write the entry a publish of ``layer``, exported into ``directory``, would build."""
    references = {layer.stem: (f"./{layer.relative_to(directory)}", layer)}
    for product, version in pins.items():
        if product != layer.stem:
            spelled = layer_path(root, asset, product, version)
            references[product] = (str(spelled), Path(root / spelled))
    return _write_entry(directory, root, asset, [references[p] for p in sorted(references)])


def _write_entry(
    directory: Path, root: PurePosixPath, asset: Asset, references: list[tuple[str, Path]]
) -> Path:
    pipe_name = asset_directory(root, asset).name
    stage = _stage_metadata([path for _, path in references])

    payload = Sdf.Layer.CreateNew(str(directory / _PAYLOAD))
    pinning = Sdf.CreatePrimInLayer(payload, f"/{pipe_name}")
    pinning.specifier = Sdf.SpecifierDef
    pinning.referenceList.prependedItems = [Sdf.Reference(spelling) for spelling, _ in references]
    payload.defaultPrim = pipe_name
    _set_stage_metadata(payload, stage)
    payload.Save()

    entry = Sdf.Layer.CreateNew(str(directory / f"{pipe_name}.usda"))
    model = Sdf.CreatePrimInLayer(entry, f"/{pipe_name}")
    model.specifier = Sdf.SpecifierDef
    model.typeName = "Xform"
    model.kind = "component"
    model.assetInfo = {"name": pipe_name}
    model.inheritPathList.prependedItems = [Sdf.Path(f"/__class__/{pipe_name}")]
    model.payloadList.prependedItems = [Sdf.Payload(f"./{_PAYLOAD}")]
    # Every instance inherits this, so a shot can override them all at once.
    Sdf.CreatePrimInLayer(entry, f"/__class__/{pipe_name}").specifier = Sdf.SpecifierClass
    entry.GetPrimAtPath("/__class__").specifier = Sdf.SpecifierClass
    entry.defaultPrim = pipe_name
    _set_stage_metadata(entry, stage)
    entry.Save()
    return Path(entry.realPath)


def pins(root: PurePosixPath, asset: Asset, version: int) -> dict[str, int]:
    """Which version of each component an installed asset version pins."""
    payload = entry_path(root, asset, version).with_name(_PAYLOAD)
    layer = Sdf.Layer.OpenAsAnonymous(str(payload)) if payload.is_file() else None
    if layer is None:
        raise PiperError(
            f"{asset.name} has no readable {ASSET} {layout.version_name(version)} at {payload}"
        )
    pinned: dict[str, int] = {}
    for reference in layer.GetPrimAtPath(f"/{layer.defaultPrim}").referenceList.prependedItems:
        directory = layout.version_directory(root, root / reference.assetPath)
        number = layout.version_number(directory.name) if directory is not None else None
        if directory is None or number is None:
            raise PiperError(f"{payload} pins {reference.assetPath}, which is not a version")
        pinned[directory.parent.name] = number
    return pinned


def versions(root: PurePosixPath, asset: Asset, product: str) -> list[int]:
    """The installed version numbers of a product, lowest first."""
    directory = Path(layout.product_root(PurePosixPath(asset_directory(root, asset)), product))
    if not directory.is_dir():
        return []
    numbers = (layout.version_number(path.name) for path in directory.iterdir())
    return sorted(number for number in numbers if number is not None)


def entry_path(root: PurePosixPath, asset: Asset, version: int) -> Path:
    """Where the entry of an asset version is, whether or not it is installed."""
    directory = asset_directory(root, asset)
    product = layout.product_root(PurePosixPath(directory), ASSET)
    return Path(product, layout.version_name(version), f"{directory.name}.usda")


def layer_path(root: PurePosixPath, asset: Asset, product: str, version: int) -> PurePosixPath:
    """An installed component version's root layer, named for its product, spelled from the root."""
    directory = Path(layout.product_root(PurePosixPath(asset_directory(root, asset)), product))
    candidates = [
        path
        for path in (directory / layout.version_name(version)).glob(f"{product}.*")
        if path.suffix in layout.LAYER_SUFFIXES
    ]
    if not candidates:
        raise _no_version(root, asset, product, version)
    return PurePosixPath(candidates[0].relative_to(Path(root)))


def slots(root: PurePosixPath, asset: Asset, pins: Mapping[str, int]) -> list[str]:
    """The material slots of the geo version ``pins`` names, in the order the geo declares them."""
    if GEOMETRY not in pins:
        return []
    geo = root / layer_path(root, asset, GEOMETRY, pins[GEOMETRY])
    # The layer is held until the names are copied out: its specs die with it.
    layer = Sdf.Layer.OpenAsAnonymous(str(geo))
    materials = layer.GetPrimAtPath(f"/{asset.pipe_name}/{MATERIAL}") if layer else None
    return [spec.name for spec in materials.nameChildren] if materials else []


def current(root: PurePosixPath, asset: Asset) -> int | None:
    """The asset version consumers get by default, or None when none is current."""
    path = current_path(root, asset)
    if not path.is_file():
        return None
    layer = Sdf.Layer.OpenAsAnonymous(str(path))
    sublayers = list(layer.subLayerPaths) if layer is not None else []
    number = layout.version_number(PurePosixPath(sublayers[0]).parent.name) if sublayers else None
    if number is None or len(sublayers) != 1:
        raise PiperError(
            f"{path} is not a current layer Piper wrote; "
            f"`piper current {asset.name!r} <version>` rewrites it"
        )
    return number


def current_pins(root: PurePosixPath, asset: Asset) -> tuple[int | None, dict[str, int]]:
    """The current asset version and what it pins; None and nothing when none is current."""
    version = current(root, asset)
    return version, pins(root, asset, version) if version is not None else {}


def current_line(version: int | None, pinned: Mapping[str, int]) -> str:
    """Which asset version is current and what it pins, as one sentence."""
    if version is None:
        return "Nothing is current."
    listed = ", ".join(f"{p} {layout.version_name(n)}" for p, n in sorted(pinned.items()))
    return f"Current is {ASSET} {layout.version_name(version)}, pinning {listed}."


def make_current(root: PurePosixPath, asset: Asset, version: int) -> None:
    """Make an installed asset version what consumers get by default."""
    entry = entry_path(root, asset, version)
    if not entry.is_file():
        raise _no_version(root, asset, ASSET, version)
    path = current_path(root, asset)
    # Renamed into place, so a reader sees the old layer or the new one.
    temporary = path.with_name(f".tmp_{secrets.token_hex(4)}_{path.name}")
    try:
        layer = Sdf.Layer.CreateNew(str(temporary))
        layer.subLayerPaths = [f"./{entry.parent.name}/{entry.name}"]
        installed = Sdf.Layer.OpenAsAnonymous(str(entry))
        for name in ("defaultPrim", *_STAGE_METADATA):
            if installed.pseudoRoot.HasInfo(name):
                layer.pseudoRoot.SetInfo(name, installed.pseudoRoot.GetInfo(name))
        layer.Save()
        temporary.replace(path)
    except Tf.ErrorException as exc:
        raise PiperError(f"could not write {temporary}: {usd_error(exc)}") from exc
    except OSError as exc:
        raise PiperError(f"could not replace {path} with {temporary} ({exc.strerror})") from exc


def current_path(root: PurePosixPath, asset: Asset) -> Path:
    directory = asset_directory(root, asset)
    return Path(layout.product_root(PurePosixPath(directory), ASSET), f"{directory.name}.usda")


def usd_error(exc: Tf.ErrorException) -> str:
    return "; ".join(error.commentary.strip() for error in exc.args)


def _no_version(root: PurePosixPath, asset: Asset, product: str, version: int) -> PiperError:
    installed = ", ".join(layout.version_name(n) for n in versions(root, asset, product))
    return PiperError(
        f"{asset.name} has no {product} {layout.version_name(version)} "
        f"(installed: {installed or 'none'})"
    )


def _stage_metadata(layers: list[Path]) -> dict[str, object]:
    """Copied onto the entry: a stage takes these from its root layer, never from a reference."""
    for path in layers:
        layer = Sdf.Layer.OpenAsAnonymous(str(path))
        if layer is None:
            continue
        found = {
            name: layer.pseudoRoot.GetInfo(name)
            for name in _STAGE_METADATA
            if layer.pseudoRoot.HasInfo(name)
        }
        if found:
            return found
    return {}


def _set_stage_metadata(layer: Sdf.Layer, metadata: Mapping[str, object]) -> None:
    for name, value in metadata.items():
        layer.pseudoRoot.SetInfo(name, value)
