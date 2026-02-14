from __future__ import annotations

import os
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .errors import ConfigError, ShowResolutionError
from .models import ResolvedContext, ShowConfig

DEFAULT_GOTO_TEMPLATES: dict[str, tuple[str, ...]] = {
    "shot": ("{root}/production/shot/{id}",),
    "asset": ("{root}/production/asset/{id}",),
    "environment": ("{root}/production/set/{id}",),
}
DEFAULT_SCRIPT_DIR_NAME = "scripts"


@dataclass(frozen=True)
class UserConfig:
    path: Path
    default_show: str | None
    show_roots: dict[str, Path]
    goto_templates: dict[str, tuple[str, ...]]
    script_dirs: tuple[Path, ...]


@dataclass(frozen=True)
class ShowOverrides:
    path: Path
    goto_templates: dict[str, tuple[str, ...]]
    script_dirs: tuple[Path, ...]


def get_user_config_path(environ: Mapping[str, str] | None = None) -> Path:
    env = os.environ if environ is None else environ

    override = env.get("PIPER_CONFIG_PATH")
    if override:
        return Path(override).expanduser()

    xdg_config_home = env.get("XDG_CONFIG_HOME")
    if xdg_config_home:
        return Path(xdg_config_home).expanduser() / "piper" / "config.toml"

    return Path.home() / ".config" / "piper" / "config.toml"


def _read_toml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}

    try:
        raw = path.read_text(encoding="utf-8")
        parsed = tomllib.loads(raw)
    except tomllib.TOMLDecodeError as exc:
        raise ConfigError(f"invalid TOML in {path}: {exc}") from exc

    if not isinstance(parsed, dict):
        raise ConfigError(f"invalid top-level structure in {path}")

    return parsed


def _normalize_path(value: str, *, base_dir: Path) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = base_dir / path
    return path


def _find_project_root() -> Path:
    current = Path(__file__).resolve()
    for candidate in (current, *current.parents):
        if (candidate / "pyproject.toml").is_file():
            return candidate
    return Path.cwd()


def _default_script_dirs() -> tuple[Path, ...]:
    return (_find_project_root() / DEFAULT_SCRIPT_DIR_NAME,)


def _parse_templates(
    raw_value: object, *, config_path: Path, key: str
) -> tuple[str, ...]:
    if isinstance(raw_value, str):
        return (raw_value,)

    if isinstance(raw_value, list):
        values: list[str] = []
        for item in raw_value:
            if not isinstance(item, str):
                raise ConfigError(
                    f"invalid '{key}' templates in {config_path}: all values must be strings"
                )
            values.append(item)

        if values:
            return tuple(values)

    raise ConfigError(
        f"invalid '{key}' templates in {config_path}: expected string or non-empty list"
    )


def _parse_goto_table(
    raw_value: object, *, config_path: Path
) -> dict[str, tuple[str, ...]]:
    if raw_value is None:
        return {}

    if not isinstance(raw_value, dict):
        raise ConfigError(f"invalid [goto] section in {config_path}: expected a table")

    templates: dict[str, tuple[str, ...]] = {}
    for key, value in raw_value.items():
        if not isinstance(key, str):
            raise ConfigError(
                f"invalid [goto] key in {config_path}: expected string keys"
            )
        templates[key] = _parse_templates(value, config_path=config_path, key=key)

    return templates


def _parse_script_dirs(
    raw_value: object,
    *,
    config_path: Path,
    base_dir: Path,
) -> tuple[Path, ...]:
    if raw_value is None:
        return ()

    if not isinstance(raw_value, dict):
        raise ConfigError(
            f"invalid [scripts] section in {config_path}: expected a table"
        )

    scripts_table: dict[str, object] = {}
    for key, value in raw_value.items():
        if not isinstance(key, str):
            raise ConfigError(
                f"invalid [scripts] key in {config_path}: expected string keys"
            )
        scripts_table[key] = value

    values: list[str] = []

    single_dir = scripts_table.get("dir")
    if single_dir is not None:
        if not isinstance(single_dir, str):
            raise ConfigError(f"invalid scripts.dir in {config_path}: expected string")
        values.append(single_dir)

    many_dirs = scripts_table.get("dirs")
    if many_dirs is not None:
        if not isinstance(many_dirs, list):
            raise ConfigError(
                f"invalid scripts.dirs in {config_path}: expected list of strings"
            )
        for item in many_dirs:
            if not isinstance(item, str):
                raise ConfigError(
                    f"invalid scripts.dirs in {config_path}: expected list of strings"
                )
            values.append(item)

    deduped: list[Path] = []
    seen: set[Path] = set()
    for value in values:
        normalized = _normalize_path(value, base_dir=base_dir)
        if normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(normalized)

    return tuple(deduped)


def load_user_config(path: Path) -> UserConfig:
    data = _read_toml(path)

    default_show = data.get("default_show")
    if default_show is not None and not isinstance(default_show, str):
        raise ConfigError(f"invalid default_show in {path}: expected string")

    raw_shows = data.get("shows", {})
    if not isinstance(raw_shows, dict):
        raise ConfigError(f"invalid [shows] section in {path}: expected a table")

    show_roots: dict[str, Path] = {}
    for key, value in raw_shows.items():
        if not isinstance(key, str) or not isinstance(value, str):
            raise ConfigError(
                f"invalid [shows] entry in {path}: expected string key/value pairs"
            )
        show_roots[key] = _normalize_path(value, base_dir=path.parent)

    goto_templates = _parse_goto_table(data.get("goto"), config_path=path)
    script_dirs = _parse_script_dirs(
        data.get("scripts"),
        config_path=path,
        base_dir=path.parent,
    )

    return UserConfig(
        path=path,
        default_show=default_show,
        show_roots=show_roots,
        goto_templates=goto_templates,
        script_dirs=script_dirs,
    )


def get_show_config_path(show_root: Path) -> Path:
    return show_root / "pipeline" / "piper.toml"


def load_show_overrides(show_root: Path) -> ShowOverrides:
    path = get_show_config_path(show_root)
    data = _read_toml(path)

    goto_templates = _parse_goto_table(data.get("goto"), config_path=path)
    script_dirs = _parse_script_dirs(
        data.get("scripts"),
        config_path=path,
        base_dir=path.parent,
    )

    return ShowOverrides(
        path=path, goto_templates=goto_templates, script_dirs=script_dirs
    )


def _is_subpath(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def _infer_show_name(cwd: Path, show_roots: Mapping[str, Path]) -> str | None:
    candidates = [
        (name, root) for name, root in show_roots.items() if _is_subpath(cwd, root)
    ]
    if not candidates:
        return None

    # Prefer the most specific root if nested roots ever exist.
    candidates.sort(key=lambda item: len(item[1].parts), reverse=True)
    return candidates[0][0]


def _resolve_show_name(
    show_arg: str | None,
    *,
    cwd: Path,
    environ: Mapping[str, str],
    user_config: UserConfig,
) -> str:
    requested_show = show_arg or environ.get("PIPER_SHOW")
    if requested_show:
        if requested_show not in user_config.show_roots:
            known = ", ".join(sorted(user_config.show_roots)) or "<none>"
            raise ShowResolutionError(
                f"unknown show '{requested_show}'. Known shows: {known}"
            )
        return requested_show

    inferred = _infer_show_name(cwd, user_config.show_roots)
    if inferred is not None:
        return inferred

    if user_config.default_show is not None:
        if user_config.default_show not in user_config.show_roots:
            raise ShowResolutionError(
                f"default_show '{user_config.default_show}' is not defined in [shows]"
            )
        return user_config.default_show

    if not user_config.show_roots:
        raise ShowResolutionError(
            f"no shows configured in {user_config.path}. Add a [shows] table."
        )

    raise ShowResolutionError(
        "unable to resolve show. Use --show, set PIPER_SHOW, or configure default_show."
    )


def resolve_context(
    show_arg: str | None,
    *,
    cwd: Path | None = None,
    environ: Mapping[str, str] | None = None,
    user_config_path: Path | None = None,
) -> ResolvedContext:
    env = os.environ if environ is None else environ
    runtime_cwd = Path.cwd() if cwd is None else cwd

    config_path = (
        get_user_config_path(env) if user_config_path is None else user_config_path
    )
    user_config = load_user_config(config_path)

    show_name = _resolve_show_name(
        show_arg,
        cwd=runtime_cwd,
        environ=env,
        user_config=user_config,
    )
    show_root = user_config.show_roots[show_name]
    if not show_root.exists():
        raise ShowResolutionError(f"show root does not exist: {show_root}")

    show_overrides = load_show_overrides(show_root)

    goto_templates = dict(DEFAULT_GOTO_TEMPLATES)
    goto_templates.update(user_config.goto_templates)
    goto_templates.update(show_overrides.goto_templates)

    if show_overrides.script_dirs:
        script_dirs = show_overrides.script_dirs
    elif user_config.script_dirs:
        script_dirs = user_config.script_dirs
    else:
        script_dirs = _default_script_dirs()

    show_config = ShowConfig(
        name=show_name,
        root=show_root,
        goto_templates=goto_templates,
        script_dirs=script_dirs,
        user_config_path=config_path,
        show_config_path=show_overrides.path,
    )

    return ResolvedContext(show=show_config, cwd=runtime_cwd)
