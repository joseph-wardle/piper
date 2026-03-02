"""Metrics catalog — structured index of every gold metric.

``get_catalog()`` loads the bundled ``conf/metrics_catalog.yml`` and returns
a list of ``CatalogEntry`` dataclasses.  The catalog is the authoritative
reference for what each Grafana panel measures and who owns it.

Schema
------
Each YAML entry must have:
    name        Unique metric identifier (snake_case).
    owner       Team responsible for this metric's quality.
    model       Gold SQL view that produces it (matches a file in sql/gold/).
    column      Column within that view.
    description Plain-English explanation.
    refresh     How often the cron job refreshes it ("daily" or "weekly").
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml

# Bundled catalog file — lives alongside conf/settings.toml.
_CATALOG_FILE = Path(__file__).parent.parent.parent / "conf" / "metrics_catalog.yml"


@dataclass(frozen=True)
class CatalogEntry:
    """One metric in the catalog."""

    name: str
    owner: str
    model: str
    column: str
    description: str
    refresh: str


def load_catalog(path: Path) -> list[CatalogEntry]:
    """Load and parse a metrics catalog YAML file.

    Args:
        path: Path to a ``metrics_catalog.yml``-format file.

    Returns:
        List of ``CatalogEntry`` objects, one per metric.

    Raises:
        KeyError:   If the file is missing the ``metrics`` top-level key.
        TypeError:  If an entry is missing a required field.
    """
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    return [CatalogEntry(**entry) for entry in raw["metrics"]]


def get_catalog() -> list[CatalogEntry]:
    """Return the bundled metrics catalog (loaded from ``conf/metrics_catalog.yml``)."""
    return load_catalog(_CATALOG_FILE)
