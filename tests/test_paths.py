"""Tests for filesystem path derivations."""

from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

from piper.config import Settings
from piper.paths import ProjectPaths


def _settings_with_data_root(tmp_path: Path) -> Settings:
    return Settings(paths={"raw_root": str(tmp_path / "raw"), "data_root": str(tmp_path)})


class TestFromSettings:
    def test_all_paths_derived_from_data_root(self, tmp_path):
        s = _settings_with_data_root(tmp_path)
        p = ProjectPaths.from_settings(s)

        assert p.data_root == tmp_path
        assert p.raw_root == tmp_path / "raw"
        assert p.warehouse_dir == tmp_path / "warehouse"
        assert p.silver_dir == tmp_path / "silver"
        assert p.state_dir == tmp_path / "state"
        assert p.quarantine_dir == tmp_path / "quarantine" / "invalid_jsonl"
        assert p.run_logs_dir == tmp_path / "run_logs"

    def test_is_frozen(self, tmp_path):
        """ProjectPaths must be immutable — paths should not change mid-run."""
        s = _settings_with_data_root(tmp_path)
        p = ProjectPaths.from_settings(s)
        with pytest.raises(FrozenInstanceError):
            p.warehouse_dir = tmp_path / "other"  # type: ignore[misc]


class TestEnsureOutputDirs:
    def test_creates_all_managed_dirs(self, tmp_path):
        s = _settings_with_data_root(tmp_path)
        p = ProjectPaths.from_settings(s)
        p.ensure_output_dirs()

        assert p.warehouse_dir.is_dir()
        assert p.silver_dir.is_dir()
        assert p.state_dir.is_dir()
        assert p.quarantine_dir.is_dir()
        assert p.run_logs_dir.is_dir()

    def test_does_not_create_raw_root(self, tmp_path):
        """raw_root is pipeline-owned; piper must not create it."""
        s = _settings_with_data_root(tmp_path)
        p = ProjectPaths.from_settings(s)
        p.ensure_output_dirs()

        assert not p.raw_root.exists()

    def test_idempotent(self, tmp_path):
        s = _settings_with_data_root(tmp_path)
        p = ProjectPaths.from_settings(s)
        p.ensure_output_dirs()
        p.ensure_output_dirs()  # second call must not raise
