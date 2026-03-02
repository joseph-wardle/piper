"""Tests for the metrics catalog loader and piper catalog list command."""

from pathlib import Path

from typer.testing import CliRunner

from piper.catalog import get_catalog
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
    def test_every_gold_model_has_at_least_one_entry(self):
        entries = get_catalog()
        assert {e.model for e in entries} >= _GOLD_MODELS

    def test_required_fields_non_empty(self):
        for entry in get_catalog():
            assert entry.name and entry.owner and entry.model
            assert entry.column and entry.description and entry.refresh

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
    def test_output_contains_all_gold_models(self):
        result = runner.invoke(app, ["catalog", "list"])
        assert result.exit_code == 0, result.output
        for model in _GOLD_MODELS:
            assert model in result.output

    def test_filter_by_model(self):
        result = runner.invoke(app, ["catalog", "list", "--model", "gold_farm_pressure_daily"])
        assert result.exit_code == 0, result.output
        assert "gold_farm_pressure_daily" in result.output
        assert "gold_publish_health_daily" not in result.output

    def test_unknown_model_returns_no_metrics_message(self):
        result = runner.invoke(app, ["catalog", "list", "--model", "gold_nonexistent"])
        assert result.exit_code == 0
        assert "No metrics found" in result.output
