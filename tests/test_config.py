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
