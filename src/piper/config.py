"""Typed configuration — single source of truth for all piper runtime settings.

Loading priority (highest to lowest):
  1. Explicit init kwargs (programmatic overrides, tests)
  2. Environment variables: PIPER_<SECTION>__<KEY>  (double-underscore separator)
  3. Config file: PIPER_CONFIG_FILE env var, or conf/settings.toml at project root
  4. Model field defaults

Example env overrides:
  PIPER_PATHS__RAW_ROOT=/custom/raw
  PIPER_INGEST__SETTLE_SECONDS=60
  PIPER_LOGGING__LEVEL=DEBUG
"""

import os
from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, field_validator
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    TomlConfigSettingsSource,
)

# Derive project root from this file's location: src/piper/config.py → ../../..
_PROJECT_ROOT = Path(__file__).parent.parent.parent
_DEFAULT_CONFIG = _PROJECT_ROOT / "conf" / "settings.toml"


def _config_file() -> Path:
    """Resolve the config file path.

    Returns PIPER_CONFIG_FILE if set (raises FileNotFoundError if missing),
    otherwise returns the bundled default at conf/settings.toml.
    """
    if env_val := os.environ.get("PIPER_CONFIG_FILE"):
        p = Path(env_val)
        if not p.is_file():
            raise FileNotFoundError(f"PIPER_CONFIG_FILE not found: {p}")
        return p
    return _DEFAULT_CONFIG


# ---------------------------------------------------------------------------
# Section models — each maps to a [section] in conf/settings.toml
# ---------------------------------------------------------------------------


class PathsSettings(BaseModel):
    """Filesystem roots for piper's source data and managed output."""

    raw_root: Path = Path("/groups/sandwich/05_production/.telemetry/raw")
    data_root: Path = Path("/groups/sandwich/05_production/.telemetry")


class IngestSettings(BaseModel):
    """Controls ingestion behaviour and safety limits."""

    # Files modified within this window are skipped to avoid reading mid-write.
    settle_seconds: int = 120
    # Prevents unbounded quarantine growth from a badly misconfigured producer.
    quarantine_max_per_day: int = 1000


class LoggingSettings(BaseModel):
    """Logging verbosity and output format."""

    level: str = "INFO"
    format: Literal["json", "text"] = "json"

    @field_validator("level", mode="before")
    @classmethod
    def _normalise_level(cls, v: str) -> str:
        valid = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if v.upper() not in valid:
            raise ValueError(f"level must be one of {sorted(valid)}, got {v!r}")
        return v.upper()


class PrivacySettings(BaseModel):
    """Privacy controls for dashboard output."""

    # Replace host_user with a stable SHA-256 prefix in gold SQL models.
    mask_users: bool = False


# ---------------------------------------------------------------------------
# Root settings class
# ---------------------------------------------------------------------------


class Settings(BaseSettings):
    """All piper runtime settings, fully resolved and validated."""

    paths: PathsSettings = PathsSettings()
    ingest: IngestSettings = IngestSettings()
    logging: LoggingSettings = LoggingSettings()
    privacy: PrivacySettings = PrivacySettings()

    model_config = SettingsConfigDict(
        env_prefix="PIPER_",
        env_nested_delimiter="__",  # PIPER_PATHS__RAW_ROOT → paths.raw_root
    )

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        # Exclude dotenv and file-secret sources; piper uses TOML + env only.
        return (
            init_settings,
            env_settings,
            TomlConfigSettingsSource(settings_cls, toml_file=_config_file()),
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the process-wide Settings instance (loaded once, cached thereafter).

    Tests should call ``get_settings.cache_clear()`` before each test that
    patches environment variables or the config file.
    """
    return Settings()
