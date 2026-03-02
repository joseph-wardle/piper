"""Tests for the metrics catalog loader and piper catalog list command."""

from pathlib import Path

import yaml
from typer.testing import CliRunner

from piper.catalog import CatalogEntry, get_catalog, load_catalog
from piper.cli import app

_GOLD_MODELS = {
    "gold_publish_health_daily",
    "gold_render_health_daily",
    "gold_farm_pressure_daily",
    "gold_tool_reliability_daily",
    "gold_storage_growth_weekly",
    "gold_data_quality_daily",
}

runner = CliRunner()


# ---------------------------------------------------------------------------
# load_catalog / get_catalog
# ---------------------------------------------------------------------------


class TestLoadCatalog:
    def test_bundled_catalog_loads_without_error(self):
        entries = get_catalog()
        assert len(entries) > 0

    def test_every_gold_model_has_at_least_one_entry(self):
        entries = get_catalog()
        models_in_catalog = {e.model for e in entries}
        assert models_in_catalog >= _GOLD_MODELS

    def test_all_entries_are_catalog_entry_instances(self):
        for entry in get_catalog():
            assert isinstance(entry, CatalogEntry)

    def test_required_fields_non_empty(self):
        for entry in get_catalog():
            assert entry.name.strip()
            assert entry.owner.strip()
            assert entry.model.strip()
            assert entry.column.strip()
            assert entry.description.strip()
            assert entry.refresh.strip()

    def test_refresh_values_are_known_cadences(self):
        valid = {"daily", "weekly", "hourly"}
        for entry in get_catalog():
            assert entry.refresh in valid, f"{entry.name}: unexpected refresh {entry.refresh!r}"

    def test_names_are_unique(self):
        entries = get_catalog()
        names = [e.name for e in entries]
        assert len(names) == len(set(names)), "duplicate metric names in catalog"

    def test_load_catalog_from_custom_path(self, tmp_path):
        """load_catalog works with any path, not just the bundled file."""
        content = {
            "metrics": [
                {
                    "name": "test_metric",
                    "owner": "test-team",
                    "model": "gold_data_quality_daily",
                    "column": "error_rate_pct",
                    "description": "A test metric.",
                    "refresh": "daily",
                }
            ]
        }
        catalog_file = tmp_path / "catalog.yml"
        catalog_file.write_text(yaml.dump(content))
        entries = load_catalog(catalog_file)
        assert len(entries) == 1
        assert entries[0].name == "test_metric"

    def test_model_names_reference_known_gold_models(self):
        """Every model in the catalog must match an existing gold SQL file."""
        sql_gold_dir = Path(__file__).parent.parent / "src" / "piper" / "sql" / "gold"
        existing_models = {f.stem for f in sql_gold_dir.glob("*.sql")}
        for entry in get_catalog():
            assert entry.model in existing_models, (
                f"Catalog entry {entry.name!r} references unknown model {entry.model!r}"
            )


# ---------------------------------------------------------------------------
# piper catalog list (CLI)
# ---------------------------------------------------------------------------


class TestCatalogListCommand:
    def test_exits_zero(self):
        result = runner.invoke(app, ["catalog", "list"])
        assert result.exit_code == 0, result.output

    def test_output_contains_all_gold_models(self):
        result = runner.invoke(app, ["catalog", "list"])
        for model in _GOLD_MODELS:
            assert model in result.output

    def test_output_has_header_row(self):
        result = runner.invoke(app, ["catalog", "list"])
        assert "name" in result.output
        assert "model" in result.output
        assert "refresh" in result.output

    def test_filter_by_model(self):
        result = runner.invoke(app, ["catalog", "list", "--model", "gold_farm_pressure_daily"])
        assert result.exit_code == 0, result.output
        assert "gold_farm_pressure_daily" in result.output
        assert "gold_publish_health_daily" not in result.output

    def test_unknown_model_returns_no_metrics_message(self):
        result = runner.invoke(app, ["catalog", "list", "--model", "gold_nonexistent"])
        assert result.exit_code == 0
        assert "No metrics found" in result.output

    def test_each_entry_shows_refresh_cadence(self):
        result = runner.invoke(app, ["catalog", "list"])
        assert "daily" in result.output
