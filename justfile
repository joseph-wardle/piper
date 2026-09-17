default:
    @just --list

sync:
    uv sync

format:
    uv run ruff format .

lint:
    uv run ruff check .
    uv run ruff format --check .

typecheck:
    uv run ty check

test:
    uv run pytest

# Maya's USD. `python -m` runs the overlay's interpreter; the `pytest` script
# would run the locked environment's USD instead.
test-usd-24:
    uv run --with usd-core==24.11 python -m pytest packages/piper-studio

test-integration:
    uv run pytest -m integration
    uv run --with usd-core==24.11 python -m pytest -m integration packages/piper-studio

# `--isolated`: without it, uv layers this environment over the project's
# `.venv`, and every third-party package there would still import.
isolate:
    uv run --isolated --no-project --with ./packages/piper-core \
        python -c "import piper, piper.errors, piper.find, piper.registry, piper.tracker"

build:
    uv build --all-packages

check: lint typecheck test test-usd-24 isolate
