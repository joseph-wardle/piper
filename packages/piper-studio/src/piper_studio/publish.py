"""Publishing: a component becomes an immutable version, pinned by a new asset version."""

import errno
import os
import secrets
import shutil
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from types import MappingProxyType

from pxr import Ar, Sdf, Tf, Usd, UsdUtils

from piper.errors import PiperError, RegistryError
from piper.registry import Registry
from piper.tracker import Asset
from piper_studio import compose, layout, textures
from piper_studio.storage import asset_directory

_SOURCE = "src"
_INSTALL_ATTEMPTS = 5
_NO_VERSIONS: Mapping[str, int] = MappingProxyType({})


@dataclass(frozen=True, slots=True)
class ProductVersion:
    """An installed version of an asset's product, and its registry record."""

    asset: Asset
    product: str
    version: int
    path: PurePosixPath
    record_id: str | None


class UnregisteredVersionError(PiperError):
    """The version is installed, but the registry did not record it."""

    def __init__(self, message: str, version: ProductVersion) -> None:
        super().__init__(message)
        self.version = version


@dataclass(frozen=True, slots=True)
class PublishResult:
    """The component, the asset version pinning it, and whether that version is current."""

    component: ProductVersion
    asset_version: ProductVersion | None
    pins: Mapping[str, int]
    current: bool


class PartialPublishError(PiperError):
    """The component is installed, but a later step failed; ``result`` says what exists."""

    def __init__(self, message: str, result: PublishResult) -> None:
        super().__init__(message)
        self.result = result


@dataclass(frozen=True, slots=True)
class PublishTexturesResult:
    """The textures installed, and the material publish that reads them, when one was derived.

    ``material`` is None when nothing was derived, and ``warnings`` say why;
    ``derived_from`` is the mtl version the material came from, or None with it.
    """

    textures: ProductVersion
    material: PublishResult | None
    derived_from: int | None
    warnings: tuple[str, ...]


class PartialPublishTexturesError(PiperError):
    """The textures are installed, but a later step failed; ``result`` says what exists."""

    def __init__(self, message: str, result: PublishTexturesResult) -> None:
        super().__init__(message)
        self.result = result


@dataclass(frozen=True, slots=True)
class UseTexturesResult:
    """The next material's layer and the mtl version it came from; or neither, and why not."""

    layer: Path | None
    derived_from: int | None
    warnings: tuple[str, ...]


def publish(
    registry: Registry,
    *,
    root: PurePosixPath,
    asset: Asset,
    product: str,
    layer: PurePosixPath,
    source: PurePosixPath | None = None,
    with_versions: Mapping[str, int] = _NO_VERSIONS,
) -> PublishResult:
    """Publish a component, build the asset version pinning it, and make that current."""
    if product == compose.ASSET:
        raise PiperError(
            f"cannot publish {compose.ASSET}: an asset version is built by publishing a "
            "component, such as geo or mtl"
        )
    if Path(layer).stem != product:
        raise PiperError(
            f"cannot publish {layer} as {product}: a component's root layer is named for its "
            f"product, {product}.usd, {product}.usda, or {product}.usdc"
        )
    if product in with_versions:
        raise PiperError(f"cannot pin {product} while publishing {product}")
    before, pins = compose.current_pins(root, asset)
    for named, version in with_versions.items():
        compose.layer_path(root, asset, named, version)
        pins[named] = version

    try:
        component = publish_product(
            registry, root=root, asset=asset, product=product, layer=layer, source=source
        )
    except UnregisteredVersionError as exc:
        installed = exc.version
        raise PartialPublishError(
            f"{exc}; no asset version was built",
            PublishResult(installed, None, {**pins, product: installed.version}, current=False),
        ) from exc
    pins[product] = component.version
    published = f"published {_named(component)}"

    with tempfile.TemporaryDirectory(
        prefix="piper_asset_", ignore_cleanup_errors=True
    ) as directory:
        try:
            entry = compose.write_asset_version(Path(directory), root=root, asset=asset, pins=pins)
            asset_version = publish_product(
                registry, root=root, asset=asset, product=compose.ASSET, layer=PurePosixPath(entry)
            )
        except UnregisteredVersionError as exc:
            raise PartialPublishError(
                f"{published}; {exc}; "
                f"{_current_command(asset, exc.version.version)} makes it current",
                PublishResult(component, exc.version, pins, current=False),
            ) from exc
        except PiperError as exc:
            raise PartialPublishError(
                f"{published}, but could not build the asset version: {exc}",
                PublishResult(component, None, pins, current=False),
            ) from exc
    published = f"{published} and {_named(asset_version)}"
    built = PublishResult(component, asset_version, pins, current=False)

    try:
        after = compose.current(root, asset)
        if after == before:
            compose.make_current(root, asset, asset_version.version)
            return PublishResult(component, asset_version, pins, current=True)
        why = _moved_meanwhile(root, asset, after, product, asset_version, pins)
    except PiperError as exc:
        raise PartialPublishError(
            f"{published}, but could not make it current: {exc}", built
        ) from exc
    raise PartialPublishError(f"{published}, but {why}", built)


def published_line(component: ProductVersion) -> str:
    """Which component version a publish installed, and of which asset, as one sentence."""
    return f"Published {_named(component)} of {component.asset.name!r}"


def derived_line(material: ProductVersion, derived_from: int) -> str:
    """Which material a texture publish derived, and from which, as one sentence."""
    return f"Derived {_named(material)} from {_named_number(material.product, derived_from)}"


def composition_line(result: PublishResult) -> str:
    """What the asset version pins and whether it is current, as one sentence."""
    if result.asset_version is None:
        return "no asset version was built"
    pinned = [f"{product} {layout.version_name(n)}" for product, n in sorted(result.pins.items())]
    listed = " and ".join(pinned) if len(pinned) <= 2 else ", ".join(pinned)
    state = "is current" if result.current else "is not current"
    return f"{_named(result.asset_version)} pins {listed}, and {state}"


def other_versions(
    root: PurePosixPath, asset: Asset, pins: Mapping[str, int], product: str
) -> dict[str, tuple[list[int], int]]:
    """Each other pinned component's installed versions, and the one ``pins`` holds."""
    return {
        named: (compose.versions(root, asset, named), pinned)
        for named, pinned in sorted(pins.items())
        if named != product
    }


def publish_textures(
    registry: Registry,
    *,
    root: PurePosixPath,
    asset: Asset,
    export: PurePosixPath,
    renderman: Path,
    source: PurePosixPath | None = None,
) -> PublishTexturesResult:
    """Publish a Painter export as the next tex version, then the material derived to read it."""
    exported = Path(export).resolve()
    files = _exported_files(exported)
    product_root = _product_root(root, asset, textures.PRODUCT)
    _, pins = compose.current_pins(root, asset)

    staging = _staging(exported, product_root)
    try:
        textures.convert(exported, renderman=renderman, into=staging)
    except PiperError as exc:
        raise _refusal(exported, [str(exc)], _discard(staging)) from exc
    try:
        for file in files:
            shutil.copyfile(file, staging / file.name)
        if source is not None:
            (staging / _SOURCE).mkdir()
            shutil.copyfile(source, staging / _SOURCE / source.name)
    except OSError as exc:
        problem = f"{exc.filename} could not be copied into {staging} ({exc.strerror})"
        raise _refusal(exported, [problem], _discard(staging)) from exc
    version = _install(exported, product_root, staging)
    path = PurePosixPath(product_root, layout.version_name(version))
    try:
        installed = _register(registry, asset, textures.PRODUCT, version, path)
    except UnregisteredVersionError as exc:
        raise PartialPublishTexturesError(
            f"{exc}; no material was derived", PublishTexturesResult(exc.version, None, None, ())
        ) from exc
    published = f"published {_named(installed)}"

    with tempfile.TemporaryDirectory(prefix="piper_mtl_", ignore_cleanup_errors=True) as directory:
        try:
            derived = use_textures(
                Path(directory), root=root, asset=asset, pins=pins, version=version
            )
        except PiperError as exc:
            raise PartialPublishTexturesError(
                f"{published}, but could not derive the material: {exc}; "
                "publishing again installs another version",
                PublishTexturesResult(installed, None, None, ()),
            ) from exc
        if derived.layer is None:
            return PublishTexturesResult(installed, None, None, derived.warnings)
        try:
            material = publish(
                registry,
                root=root,
                asset=asset,
                product=compose.MATERIAL,
                layer=PurePosixPath(derived.layer),
            )
        except PartialPublishError as exc:
            raise PartialPublishTexturesError(
                f"{published}; {exc}",
                PublishTexturesResult(
                    installed, exc.result, derived.derived_from, derived.warnings
                ),
            ) from exc
        except PiperError as exc:
            raise PartialPublishTexturesError(
                f"{published}, but could not publish the derived material: {exc}; "
                "publishing again installs another version",
                PublishTexturesResult(installed, None, None, derived.warnings),
            ) from exc
    return PublishTexturesResult(installed, material, derived.derived_from, derived.warnings)


def _exported_files(exported: Path) -> list[Path]:
    """The files of a Painter export to install: all of them but textures, which are converted."""
    if not exported.is_dir():
        raise PiperError(f"cannot publish {exported}: it is not a directory")
    if "publish" in exported.parts:
        raise PiperError(
            f"cannot publish {exported}: it is inside a publish directory; "
            "publish the export it came from"
        )
    files = sorted(exported.iterdir())
    for file in files:
        if not file.is_file():
            raise PiperError(
                f"cannot publish {exported}: {file.name} is not a file, and a Painter export "
                "is flat"
            )
    return [file for file in files if file.suffix != textures.TEXTURE]


def use_textures(
    directory: Path, *, root: PurePosixPath, asset: Asset, pins: Mapping[str, int], version: int
) -> UseTexturesResult:
    """Write into ``directory`` the mtl ``pins`` names, reading its textures from tex ``version``.

    A texture path is rewritten when it lies in a tex version and its file, or
    a tile of it, is in ``version``; otherwise it is kept, still resolving in
    the older version, and said. Said too: what ``version`` holds that the
    material does not read, a texture set that is no slot of the pinned geo,
    and a map whose tiles changed. With nothing rewritten, there is no layer.
    """
    if compose.MATERIAL not in pins:
        remedy = f"`piper open {asset.name!r} lookdev` builds one"
        return UseTexturesResult(None, None, (f"no material uses textures yet; {remedy}",))
    source = pins[compose.MATERIAL]
    source_named = _named_number(compose.MATERIAL, source)
    tex_root = layout.product_root(PurePosixPath(asset_directory(root, asset)), textures.PRODUCT)
    new = Path(tex_root, layout.version_name(version))
    new_named = _named_number(textures.PRODUCT, version)
    layer_path = root / compose.layer_path(root, asset, compose.MATERIAL, source)
    layer = Sdf.Layer.OpenAsAnonymous(str(layer_path))
    if layer is None:
        raise PiperError(f"{source_named} at {layer_path} cannot be read")

    # Each name the material reads from a tex version, and the version it read it from.
    rewritten: dict[str, Path] = {}
    kept: dict[str, Path] = {}

    def retarget(spelling: str) -> str:
        older = layout.version_directory(root, root / spelling)
        if older is None or older.parent != tex_root:
            return spelling
        name = PurePosixPath(spelling).name
        if not textures.tiles(new, name):
            kept[name] = Path(older)
            return spelling
        rewritten[name] = Path(older)
        return str(PurePosixPath(new, name).relative_to(root))

    UsdUtils.ModifyAssetPaths(layer, retarget)
    warnings = [
        f"{name} is not in {new_named}, so the material keeps reading it from "
        f"{_named_directory(older)}"
        for name, older in sorted(kept.items())
    ]
    if not rewritten:
        why = (
            f"nothing {source_named} reads is in {new_named}"
            if kept
            else f"{source_named} reads no published textures"
        )
        warnings.append(f"{why}, so no material was derived; point it at {new_named} in Houdini")
        return UseTexturesResult(None, None, tuple(warnings))

    held = {map for file in new.iterdir() if (map := _map_of(file.name))}
    read = {map for name in rewritten if (map := _map_of(name))}
    warnings += [
        f"{new_named} holds {map}, which {source_named} does not read"
        for map in sorted(held - read)
    ]
    if compose.GEOMETRY in pins:
        slots = compose.slots(root, asset, pins)
        geo = _named_number(compose.GEOMETRY, pins[compose.GEOMETRY])
        listed = ", ".join(slots) or "none"
        sets = {map.rpartition("_")[0] for map in held}
        warnings += [
            f"{slot} is a texture set of {new_named}, but {geo} has no slot named {slot} "
            f"(slots: {listed})"
            for slot in sorted(sets - set(slots))
        ]
    compared = {map: older for name, older in rewritten.items() if (map := _map_of(name))}
    for map, older in sorted(compared.items()):
        before, after = _udims(older, map), _udims(new, map)
        if before != after:
            warnings.append(
                f"{map} has tiles {', '.join(after)} in {new_named} and {', '.join(before)} "
                f"in {_named_directory(older)}"
            )

    layer.documentation = f"derived from {source_named} with {new_named}"
    written = directory / f"{compose.MATERIAL}.usda"
    if not layer.Export(str(written)):
        raise PiperError(f"could not write {written}")
    return UseTexturesResult(written, source, tuple(warnings))


def _named_directory(version: Path) -> str:
    """``tex v003``, from an installed version's directory."""
    return f"{version.parent.name} {version.name}"


def _map_of(name: str) -> str | None:
    """``body_BaseColor`` from a file or template named as Painter names them; else None."""
    matched = textures.NAMED.fullmatch(name)
    return f"{matched.group('slot')}_{matched.group('map')}" if matched else None


def _udims(directory: Path, map: str) -> list[str]:
    """The tiles a map's PNGs hold in a tex version, as Painter numbered them."""
    tiles = (textures.NAMED.fullmatch(png.name) for png in directory.glob(f"{map}.*.png"))
    return sorted(tile.group("udim") for tile in tiles if tile)


def _named(version: ProductVersion) -> str:
    return _named_number(version.product, version.version)


def _named_number(product: str, number: int) -> str:
    return f"{product} {layout.version_name(number)}"


def _current_command(asset: Asset, version: int) -> str:
    return f"`piper current {asset.name!r} {version}`"


def _moved_meanwhile(
    root: PurePosixPath,
    asset: Asset,
    moved: int | None,
    product: str,
    asset_version: ProductVersion,
    pins: Mapping[str, int],
) -> str:
    """Why ``asset_version``, pinning ``pins``, is not current, and the remedy."""
    command = _current_command(asset, asset_version.version)
    if moved is None:
        return f"nothing is current any more; {command} makes {_named(asset_version)} current"
    differing = {
        name: number
        for name, number in compose.pins(root, asset, moved).items()
        if pins.get(name) != number
    }
    listed = ", ".join(f"{name} {layout.version_name(n)}" for name, n in sorted(differing.items()))
    became = (
        f"{compose.ASSET} {layout.version_name(moved)} became current while you were "
        f"publishing, pinning {listed}"
    )
    if set(differing) <= {product}:
        return f"{became}; {command} makes {_named(asset_version)} current instead"
    return f"{became}; {_named(asset_version)} is not current; publish again to compose with it"


@dataclass(frozen=True, slots=True)
class _Dependencies:
    """What a layer depends on, as paths, so that no USD object outlives the walk."""

    layers: tuple[str, ...]
    assets: tuple[str, ...]
    unresolved: tuple[str, ...]
    dirty: tuple[str, ...]


def publish_product(
    registry: Registry,
    *,
    root: PurePosixPath,
    asset: Asset,
    product: str,
    layer: PurePosixPath,
    source: PurePosixPath | None = None,
) -> ProductVersion:
    """Install ``layer`` and the files it depends on as the product's next version; register it."""
    exported = Path(layer).resolve()
    product_root = _product_root(root, asset, product)
    _check_exported(exported)
    copies = _installable_files(root, exported)

    staging = _staging(exported, product_root)
    try:
        _copy(exported.parent, copies, staging)
        if source is not None:
            (staging / _SOURCE).mkdir()
            shutil.copyfile(source, staging / _SOURCE / source.name)
    except OSError as exc:
        problem = f"{exc.filename} could not be copied into {staging} ({exc.strerror})"
        raise _refusal(exported, [problem], _discard(staging)) from exc
    problems = _staged_problems(root, staging, staging / exported.name)
    if problems:
        raise _refusal(exported, problems, _discard(staging))

    version = _install(exported, product_root, staging)
    path = PurePosixPath(product_root, layout.version_name(version), exported.name)
    return _register(registry, asset, product, version, path)


def _product_root(root: PurePosixPath, asset: Asset, product: str) -> Path:
    directory = asset_directory(root, asset)
    if layout.slug(product) != product:
        raise PiperError(
            f"cannot name a product {product!r}: a product is named in lowercase letters, "
            f"digits, and underscores, such as {layout.slug(product) or 'geo'!r}"
        )
    return Path(layout.product_root(PurePosixPath(directory), product))


def _check_exported(exported: Path) -> None:
    if exported.suffix not in layout.LAYER_SUFFIXES:
        raise PiperError(f"cannot publish {exported}: it is not a .usd, .usda, or .usdc layer")
    if not exported.is_file():
        raise PiperError(f"cannot publish {exported}: it does not exist")
    if "publish" in exported.parent.parts:
        raise PiperError(
            f"cannot publish {exported}: it is inside a publish directory; "
            "publish the export it came from"
        )


def _installable_files(root: PurePosixPath, exported: Path) -> list[PurePosixPath]:
    """The files to copy into the version, relative to the export's directory."""
    export = exported.parent
    try:
        dependencies = _dependencies(root, exported)
    except Tf.ErrorException as exc:
        raise _refusal(exported, [compose.usd_error(exc)]) from exc

    problems = _unresolved_or_dirty(dependencies)
    copies: list[PurePosixPath] = []
    for file in (*dependencies.layers, *dependencies.assets):
        path = Path(file)
        if not path.is_relative_to(export):
            if not _is_pinnable(root, file):
                problems.append(_outside_problem(root, export, file))
            continue
        relative = PurePosixPath(path.relative_to(export))
        if relative.parts[0] == _SOURCE:
            problems.append(f"{file} is under {_SOURCE}/, which a version reserves for work files")
        elif not path.is_file():
            problems.append(f"{file} is not a file")
        else:
            copies.append(relative)
    if problems:
        raise _refusal(exported, problems)
    return copies


def _staged_problems(root: PurePosixPath, staging: Path, layer: Path) -> list[str]:
    """What stops the staged copy from installing, checked where it will be installed from."""
    try:
        dependencies = _dependencies(root, layer)
        problems = _unresolved_or_dirty(dependencies)
        for file in (*dependencies.layers, *dependencies.assets):
            if not (Path(file).is_relative_to(staging) or _is_pinnable(root, file)):
                problems.append(f"{file} is neither in the staged copy nor pinnable")
        for staged in dependencies.layers:
            if Path(staged).is_relative_to(staging):
                problems += _absolute_spellings(staging, Path(staged))
        problems += _composition_problems(root, layer)
    except Tf.ErrorException as exc:
        return [compose.usd_error(exc)]
    return problems


def _dependencies(root: PurePosixPath, layer: Path) -> _Dependencies:
    with Ar.ResolverContextBinder(_resolver_context(root)):
        layers, assets, unresolved = UsdUtils.ComputeAllDependencies(Sdf.AssetPath(str(layer)))
    return _Dependencies(
        layers=tuple(found.realPath for found in layers),
        assets=tuple(assets),
        unresolved=tuple(unresolved),
        dirty=tuple(found.realPath for found in layers if found.dirty),
    )


def _resolver_context(root: PurePosixPath) -> Ar.DefaultResolverContext:
    # Passed to each walk and stage rather than set as the default search path,
    # which belongs to whichever application hosts the publish.
    return Ar.DefaultResolverContext([str(root)])


def _unresolved_or_dirty(dependencies: _Dependencies) -> list[str]:
    problems = [f"{path} does not resolve" for path in dependencies.unresolved]
    problems += [f"{path} has unsaved edits in this session" for path in dependencies.dirty]
    return problems


def _is_pinnable(root: PurePosixPath, file: str) -> bool:
    """Whether ``file`` is a file of an installed version, which a layer may pin."""
    real = PurePosixPath(os.path.realpath(file))
    version = layout.version_directory(PurePosixPath(os.path.realpath(root)), real)
    return version is not None and real.relative_to(version).parts[0] != _SOURCE


def _outside_problem(root: PurePosixPath, export: Path, file: str) -> str:
    if Path(file).is_relative_to(root):
        return (
            f"{file} is neither inside {export} nor pinnable: a pin names a file outside "
            f"{_SOURCE}/ in an installed version"
        )
    # ArDefaultResolver looks for a path spelled from the production root in the
    # working directory before it looks in the production.
    return (
        f"{file} is outside {export} and the production; a path spelled from the production "
        f"root is looked for in the working directory, {Path.cwd()}, first"
    )


def _absolute_spellings(staging: Path, layer: Path) -> list[str]:
    # The walk reports where each path resolved, not how it was written.
    spellings: list[str] = []

    def keep_absolute(spelling: str) -> str:
        if spelling.startswith("/"):
            spellings.append(spelling)
        return spelling

    UsdUtils.ModifyAssetPaths(Sdf.Layer.OpenAsAnonymous(str(layer)), keep_absolute)
    name = layer.relative_to(staging)
    return [
        f"{name} spells @{spelling}@ as an absolute path; spell a pin from the production root"
        for spelling in spellings
    ]


def _composition_problems(root: PurePosixPath, layer: Path) -> list[str]:
    stage = Usd.Stage.Open(str(layer), _resolver_context(root), Usd.Stage.LoadAll)
    problems = [str(error) for error in stage.GetCompositionErrors()]
    default_prim = stage.GetRootLayer().defaultPrim
    if not default_prim:
        problems.append(f"{layer.name} sets no defaultPrim")
    elif not stage.GetDefaultPrim():
        problems.append(f"{layer.name} names defaultPrim {default_prim!r}, but has no such prim")
    return problems


def _copy(export: Path, files: list[PurePosixPath], staging: Path) -> None:
    """Copy each file to the same relative path in staging, following symbolic links."""
    for relative in files:
        target = staging / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(export / relative, target)


def _staging(exported: Path, product_root: Path) -> Path:
    """Make the directory a version is assembled in, beside the versions."""
    staging = product_root / f".tmp_{secrets.token_hex(4)}"
    try:
        staging.mkdir(parents=True)
    except OSError as exc:
        raise _refusal(exported, [f"{staging} could not be created ({exc.strerror})"]) from exc
    return staging


def _install(exported: Path, product_root: Path, staging: Path) -> int:
    """Rename staging onto the next free version number, and return the number."""
    try:
        return _rename_onto_next_version(exported, product_root, staging)
    except OSError as exc:
        # Returned whenever the rename happened, so staging is not a version.
        problem = f"{staging} could not be installed ({exc.strerror})"
        raise _refusal(exported, [problem], _discard(staging)) from exc


def _rename_onto_next_version(exported: Path, product_root: Path, staging: Path) -> int:
    staged_inode = staging.stat().st_ino
    attempted = 0
    for _ in range(_INSTALL_ATTEMPTS):
        version = max(_next_version(product_root), attempted + 1)
        target = product_root / layout.version_name(version)
        try:
            staging.rename(target)
        except OSError as exc:
            # An NFS client can report a failure for a rename the server performed,
            # so the target's inode, not the error, decides whether staging is there.
            if _inode(target) == staged_inode:
                return version
            if exc.errno not in (errno.EEXIST, errno.ENOTEMPTY):
                raise PiperError(
                    f"install outcome unknown: renaming {staging} to {target} failed "
                    f"({exc.strerror}); check both before publishing again"
                ) from exc
            attempted = version
            continue
        return version
    raise _refusal(
        exported,
        [f"other publishes took every version up to {layout.version_name(attempted)} first"],
        _discard(staging),
    )


def _register(
    registry: Registry, asset: Asset, product: str, version: int, path: PurePosixPath
) -> ProductVersion:
    try:
        record_id = registry.register(asset, product=product, version=version, path=path)
    except RegistryError as exc:
        raise UnregisteredVersionError(
            f"installed {path}, but could not register it: {exc}; "
            "publishing again installs another version",
            ProductVersion(asset, product, version, path, record_id=None),
        ) from exc
    return ProductVersion(asset, product, version, path, record_id)


def _next_version(product_root: Path) -> int:
    # Every version name counts, even an empty directory's: a rename onto an
    # empty directory replaces it instead of failing.
    numbers = (layout.version_number(path.name) for path in product_root.iterdir())
    return max((number for number in numbers if number is not None), default=0) + 1


def _inode(path: Path) -> int | None:
    try:
        return path.stat().st_ino
    except OSError:
        return None


def _discard(staging: Path) -> str:
    """Remove staging; when it cannot be removed, a note naming where it was left."""
    try:
        shutil.rmtree(staging)
    except OSError as exc:
        return f"\nstaging left at {staging} ({exc.strerror})"
    return ""


def _refusal(exported: Path, problems: list[str], note: str = "") -> PiperError:
    listed = "".join(f"\n  {problem}" for problem in problems)
    return PiperError(f"cannot publish {exported}:{listed}{note}")
