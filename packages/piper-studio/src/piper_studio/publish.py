"""Publishing a product: an exported USD layer becomes an immutable, registered version.

A version directory is the authority for what was published and which numbers
are taken. The registry indexes a version only once it is installed.
"""

import errno
import os
import secrets
import shutil
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from pxr import Ar, Sdf, Tf, Usd, UsdUtils

from piper.errors import PiperError, RegistryError
from piper.registry import Registry
from piper.tracker import Asset
from piper_studio import layout
from piper_studio.storage import asset_directory, make_directories

_LAYER_SUFFIXES = (".usd", ".usda", ".usdc")
# Reserved inside a version for the work files a DCC publish will capture.
_SOURCE = "src"
_INSTALL_ATTEMPTS = 5


@dataclass(frozen=True, slots=True)
class PublishResult:
    """An installed version of an asset's product, and its registry record.

    ``path`` is the version's root layer. ``record_id`` is None when the
    version installed but did not register.
    """

    asset: Asset
    product: str
    version: int
    path: PurePosixPath
    record_id: str | None


class PartialPublishError(PiperError):
    """The version is installed, but the registry did not record it."""

    def __init__(self, message: str, result: PublishResult) -> None:
        super().__init__(message)
        self.result = result


@dataclass(frozen=True, slots=True)
class _Dependencies:
    """What a layer depends on, as paths, so that no USD object outlives the walk."""

    layers: tuple[str, ...]
    assets: tuple[str, ...]
    unresolved: tuple[str, ...]
    dirty: tuple[str, ...]


def publish(
    registry: Registry,
    *,
    root: PurePosixPath,
    asset: Asset,
    product: str,
    layer: PurePosixPath,
) -> PublishResult:
    """Install ``layer`` and the files it depends on as the product's next version; register it."""
    exported = Path(layer).resolve()
    product_root = _product_root(root, asset, product)
    _check_exported(exported)
    copies = _installable_files(root, exported)

    staging = product_root / f".tmp_{secrets.token_hex(4)}"
    try:
        make_directories(staging)
    except OSError as exc:
        raise _refusal(exported, [f"{staging} could not be created ({exc.strerror})"]) from exc
    try:
        _copy(exported.parent, copies, staging)
    except OSError as exc:
        problem = f"{exc.filename} could not be copied into {staging} ({exc.strerror})"
        raise _refusal(exported, [problem], _discard(staging)) from exc
    problems = _staged_problems(root, staging, staging / exported.name)
    if problems:
        raise _refusal(exported, problems, _discard(staging))

    try:
        _lock(staging)
        version = _install(exported, product_root, staging)
    except OSError as exc:
        # `_install` returns whenever the rename happened, so staging is not a version.
        problem = f"{staging} could not be installed ({exc.strerror})"
        raise _refusal(exported, [problem], _discard(staging)) from exc
    path = PurePosixPath(product_root, layout.version_name(version), exported.name)

    try:
        record_id = registry.register(asset, product=product, version=version, path=path)
    except RegistryError as exc:
        raise PartialPublishError(
            f"installed {path}, but could not register it: {exc}; "
            "publishing again installs another version",
            PublishResult(asset, product, version, path, record_id=None),
        ) from exc
    return PublishResult(asset, product, version, path, record_id)


def _product_root(root: PurePosixPath, asset: Asset, product: str) -> Path:
    directory = asset_directory(root, asset)
    if layout.slug(product) != product:
        raise PiperError(
            f"cannot name a product {product!r}: a product is named in lowercase letters, "
            f"digits, and underscores, such as {layout.slug(product) or 'geo'!r}"
        )
    return Path(layout.product_root(PurePosixPath(directory), product))


def _check_exported(exported: Path) -> None:
    if exported.suffix not in _LAYER_SUFFIXES:
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
        raise _refusal(exported, [_usd_error(exc)]) from exc

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
        return [_usd_error(exc)]
    return problems


def _dependencies(root: PurePosixPath, layer: Path) -> _Dependencies:
    with Ar.ResolverContextBinder(_context(root)):
        layers, assets, unresolved = UsdUtils.ComputeAllDependencies(Sdf.AssetPath(str(layer)))
    return _Dependencies(
        layers=tuple(found.realPath for found in layers),
        assets=tuple(assets),
        unresolved=tuple(unresolved),
        dirty=tuple(found.realPath for found in layers if found.dirty),
    )


def _context(root: PurePosixPath) -> Ar.DefaultResolverContext:
    # Passed to each walk and stage rather than set as the default search path,
    # which belongs to whichever application hosts the publish.
    return Ar.DefaultResolverContext([str(root)])


def _unresolved_or_dirty(dependencies: _Dependencies) -> list[str]:
    problems = [f"{path} does not resolve" for path in dependencies.unresolved]
    problems += [f"{path} has unsaved edits in this session" for path in dependencies.dirty]
    return problems


def _is_pinnable(root: PurePosixPath, file: str) -> bool:
    """Whether ``file`` is a locked file of an installed version, which a layer may pin."""
    real = PurePosixPath(os.path.realpath(file))
    version = layout.version_directory(PurePosixPath(os.path.realpath(root)), real)
    if version is None or real.relative_to(version).parts[0] == _SOURCE:
        return False
    return not any(Path(path).stat().st_mode & 0o222 for path in (real, version))


def _outside_problem(root: PurePosixPath, export: Path, file: str) -> str:
    if Path(file).is_relative_to(root):
        return (
            f"{file} is neither inside {export} nor pinnable: a pin names a file outside "
            f"{_SOURCE}/ in an installed version, and both must be read-only"
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
    stage = Usd.Stage.Open(str(layer), _context(root), Usd.Stage.LoadAll)
    problems = [str(error) for error in stage.GetCompositionErrors()]
    default_prim = stage.GetRootLayer().defaultPrim
    if not default_prim:
        problems.append(f"{layer.name} sets no defaultPrim")
    elif not stage.GetDefaultPrim():
        problems.append(f"{layer.name} names defaultPrim {default_prim!r}, but has no such prim")
    return problems


def _usd_error(exc: Tf.ErrorException) -> str:
    return "; ".join(error.commentary.strip() for error in exc.args)


def _copy(export: Path, files: list[PurePosixPath], staging: Path) -> None:
    """Copy each file to the same relative path in staging, following symbolic links."""
    for relative in files:
        target = staging / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(export / relative, target)


def _lock(directory: Path) -> None:
    """Make every file and directory read-only, deepest first."""
    for parent, directories, files in os.walk(directory, topdown=False):
        for name in files:
            Path(parent, name).chmod(0o444)
        for name in directories:
            Path(parent, name).chmod(0o555)
    directory.chmod(0o555)


def _install(exported: Path, product_root: Path, staging: Path) -> int:
    """Rename staging onto the next free version number, and return the number."""
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
        staging.chmod(0o700)
        for parent, directories, _ in os.walk(staging):
            for name in directories:
                Path(parent, name).chmod(0o700)
        shutil.rmtree(staging)
    except OSError as exc:
        return f"\nstaging left at {staging} ({exc.strerror})"
    return ""


def _refusal(exported: Path, problems: list[str], note: str = "") -> PiperError:
    listed = "".join(f"\n  {problem}" for problem in problems)
    return PiperError(f"cannot publish {exported}:{listed}{note}")
