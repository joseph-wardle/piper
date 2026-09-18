"""Which production Piper works in: a persisted selection, or an override from the environment."""

import os
import tomllib
from dataclasses import dataclass
from pathlib import Path

from piper.errors import ConfigError
from piper_studio.production import PRODUCTION_ENV, Production, load_production

GENERAL = "general"
PRODUCTIONS = {"sandwich": Path("/groups/sandwich/production.toml")}


@dataclass(frozen=True, slots=True)
class Profile:
    """Which production later commands work in, and where it was read from.

    ``general`` is the profile with no production.
    """

    name: str
    production: Production | None
    path: Path | None


def active() -> Profile:
    """The profile commands follow: the override where one is set, otherwise the selection."""
    path = override()
    if path is not None:
        production = load_production(path)
        return Profile(name=production.name, production=production, path=path)
    return named(selected())


def override() -> Path | None:
    """The configuration file ``PIPER_PRODUCTION`` names, which outranks the selection."""
    configured = os.environ.get(PRODUCTION_ENV)
    return Path(configured) if configured else None


def named(name: str) -> Profile:
    """The profile called ``name``, with its production loaded."""
    if name == GENERAL:
        return Profile(name=GENERAL, production=None, path=None)
    path = PRODUCTIONS.get(name)
    if path is None:
        known = ", ".join(sorted([GENERAL, *PRODUCTIONS]))
        raise ConfigError(f"no profile is named {name!r}; Piper knows {known}")
    return Profile(name=name, production=load_production(path), path=path)


def selected() -> str:
    """The persisted selection's name, or ``general`` where nothing is persisted."""
    path = config_path()
    try:
        document = tomllib.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return GENERAL
    except OSError as exc:
        raise ConfigError(f"profile: cannot read {path} ({exc.strerror})") from exc
    except (tomllib.TOMLDecodeError, UnicodeDecodeError) as exc:
        raise ConfigError(f"profile: {path} is not valid TOML ({exc}); delete it") from exc
    name = document.get("profile")
    if not isinstance(name, str):
        raise ConfigError(f"profile: {path} does not name a profile; delete it")
    return name


def select(name: str) -> None:
    """Persist ``name`` as the selection, once its production is known to load."""
    named(name)
    path = config_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    # One key, written as a literal: the standard library reads TOML but writes none.
    path.write_text(f'profile = "{name}"\n', encoding="utf-8")


def config_path() -> Path:
    """The selection's file, in the artist's own configuration directory."""
    config = os.environ.get("XDG_CONFIG_HOME")
    home = Path(config) if config else Path.home() / ".config"
    return home / "piper" / "config.toml"
