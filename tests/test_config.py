"""Tests for the typed configuration system."""

import pytest
from pydantic import ValidationError

from piper.config import Settings, get_settings


@pytest.fixture(autouse=True)
def _clear_settings_cache():
    """Ensure each test starts with a fresh Settings instance."""
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


class TestDefaults:
    def test_loads_without_env_overrides(self):
        s = Settings()
        assert s.ingest.settle_seconds == 120
        assert s.ingest.quarantine_max_per_day == 1000
        assert s.logging.level == "INFO"
        assert s.logging.format == "json"
        assert s.privacy.mask_users is False

    def test_paths_are_path_objects(self):
        from pathlib import Path

        s = Settings()
        assert isinstance(s.paths.raw_root, Path)
        assert isinstance(s.paths.data_root, Path)


class TestEnvOverrides:
    def test_ingest_settle_seconds(self, monkeypatch):
        monkeypatch.setenv("PIPER_INGEST__SETTLE_SECONDS", "60")
        assert Settings().ingest.settle_seconds == 60

    def test_logging_level_uppercase_normalisation(self, monkeypatch):
        monkeypatch.setenv("PIPER_LOGGING__LEVEL", "debug")
        assert Settings().logging.level == "DEBUG"

    def test_privacy_mask_users(self, monkeypatch):
        monkeypatch.setenv("PIPER_PRIVACY__MASK_USERS", "true")
        assert Settings().privacy.mask_users is True

    def test_paths_raw_root(self, monkeypatch, tmp_path):
        monkeypatch.setenv("PIPER_PATHS__RAW_ROOT", str(tmp_path))
        assert Settings().paths.raw_root == tmp_path


class TestValidation:
    def test_invalid_log_level_raises(self):
        with pytest.raises(ValidationError, match="level must be one of"):
            Settings(logging={"level": "NONSENSE"})

    def test_invalid_log_format_raises(self):
        with pytest.raises(ValidationError):
            Settings(logging={"format": "xml"})

    def test_valid_log_formats(self):
        assert Settings(logging={"format": "json"}).logging.format == "json"
        assert Settings(logging={"format": "text"}).logging.format == "text"


class TestCaching:
    def test_get_settings_returns_same_instance(self):
        s1 = get_settings()
        s2 = get_settings()
        assert s1 is s2

    def test_cache_clear_returns_new_instance(self):
        s1 = get_settings()
        get_settings.cache_clear()
        s2 = get_settings()
        # Different instances after cache clear.
        assert s1 is not s2
