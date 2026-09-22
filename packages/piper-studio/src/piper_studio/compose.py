"""An asset version: two small layers that pin one version of each component.

``publish/asset/vNNN/<pipe_name>.usda`` is the entry consumers reference. Its
payload, ``payload.usda``, references the pinned component layers, spelled from
the production root. Only Piper writes them, and it writes them before they are
installed like any product, so nothing here is ever edited in place.
"""

from collections.abc import Mapping
from pathlib import Path, PurePosixPath

from pxr import Sdf

from piper.errors import PiperError
from piper.tracker import Asset
from piper_studio import layout
from piper_studio.storage import asset_directory

ASSET = "asset"
"""The product whose versions are asset versions."""
_PAYLOAD = "payload.usda"
_STAGE_METADATA = ("upAxis", "metersPerUnit")


def write_asset_version(
    directory: Path, *, root: PurePosixPath, asset: Asset, pins: Mapping[str, int]
) -> Path:
    """Write the entry and payload pinning ``pins`` into ``directory``; return the entry."""
    pipe_name = asset_directory(root, asset).name
    references = [
        layer_path(root, asset, product, version) for product, version in sorted(pins.items())
    ]
    stage = _stage_metadata(root, references)

    payload = Sdf.Layer.CreateNew(str(directory / _PAYLOAD))
    pinning = Sdf.CreatePrimInLayer(payload, f"/{pipe_name}")
    pinning.specifier = Sdf.SpecifierDef
    pinning.referenceList.prependedItems = [Sdf.Reference(str(path)) for path in references]
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
    # The class every instance inherits, so a shot can override all of them at once.
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
    """The root layer of an installed component version, spelled from the production root.

    A component's root layer is named for its product, which `publish` requires.
    """
    directory = Path(layout.product_root(PurePosixPath(asset_directory(root, asset)), product))
    candidates = [
        path
        for path in (directory / layout.version_name(version)).glob(f"{product}.*")
        if path.suffix in layout.LAYER_SUFFIXES
    ]
    if not candidates:
        installed = ", ".join(
            layout.version_name(number) for number in versions(root, asset, product)
        )
        raise PiperError(
            f"{asset.name} has no {product} {layout.version_name(version)} "
            f"(installed: {installed or 'none'})"
        )
    return PurePosixPath(candidates[0].relative_to(Path(root)))


def _stage_metadata(root: PurePosixPath, references: list[PurePosixPath]) -> dict[str, object]:
    """``upAxis`` and ``metersPerUnit`` as the first pinned layer that states them does."""
    for reference in references:
        layer = Sdf.Layer.OpenAsAnonymous(str(root / reference))
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
