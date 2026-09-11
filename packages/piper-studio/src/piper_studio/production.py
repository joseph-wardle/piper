"""Reads the configuration that describes one production."""

import os
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import TypeVar

from piper.errors import ConfigError

PRODUCTION_ENV = "PIPER_PRODUCTION"

_T = TypeVar("_T")


@dataclass(frozen=True, slots=True)
class ShotGridConfig:
    """Which ShotGrid site and project hold this production."""

    site: str
    script: str
    project: int


@dataclass(frozen=True, slots=True)
class Production:
    """One production, where it is stored, and the systems that hold it.

    ``types`` are the asset types artists may create, chosen from those the
    tracker offers.
    """

    name: str
    root: PurePosixPath
    types: tuple[str, ...]
    shotgrid: ShotGridConfig


def load_production(path: Path | None = None) -> Production:
    """Read the production configuration, by default the one ``PIPER_PRODUCTION`` names."""
    path = path if path is not None else _configured_path()
    try:
        document = tomllib.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ConfigError(f"production configuration: cannot read {path} ({exc.strerror})") from exc
    except (tomllib.TOMLDecodeError, UnicodeDecodeError) as exc:
        raise ConfigError(f"production configuration: {path} is not valid TOML ({exc})") from exc

    shotgrid = _required(document, "shotgrid", dict, path)
    return Production(
        name=_required(document, "name", str, path),
        root=_root(document, path),
        types=_types(document, path),
        shotgrid=ShotGridConfig(
            site=_required(shotgrid, "site", str, path, prefix="shotgrid."),
            script=_required(shotgrid, "script", str, path, prefix="shotgrid."),
            project=_required(shotgrid, "project", int, path, prefix="shotgrid."),
        ),
    )


def _configured_path() -> Path:
    configured = os.environ.get(PRODUCTION_ENV)
    if not configured:
        raise ConfigError(
            f"production configuration: {PRODUCTION_ENV} is not set to a configuration file"
        )
    return Path(configured)


def _root(document: Mapping[str, object], path: Path) -> PurePosixPath:
    root = PurePosixPath(_required(document, "root", str, path))
    if not root.is_absolute():
        raise ConfigError(f"production configuration: root in {path} must be absolute, not {root}")
    return root


def _types(document: Mapping[str, object], path: Path) -> tuple[str, ...]:
    listed = _required(document, "types", list, path)
    types = tuple(name for name in listed if isinstance(name, str))
    if not types or len(types) != len(listed):
        raise ConfigError(f"production configuration: types in {path} must list asset type names")
    return types


def _required(
    table: Mapping[str, object], key: str, expected: type[_T], path: Path, prefix: str = ""
) -> _T:
    value = table.get(key)
    if value is None:
        raise ConfigError(f"production configuration: {path} does not set {prefix}{key}")
    if not isinstance(value, expected):
        raise ConfigError(
            f"production configuration: {prefix}{key} in {path} must be "
            f"{expected.__name__}, not {type(value).__name__}"
        )
    return value
