default:
    @just --list

# A host imports from its package's own environment: only what that manifest
# names, so nothing in it can shadow a package the host brings.
sync:
    uv sync
    UV_PROJECT_ENVIRONMENT=packages/piper-maya/.venv uv sync --package piper-maya --no-dev --locked
    UV_PROJECT_ENVIRONMENT=packages/piper-houdini/.venv uv sync --package piper-houdini --no-dev --locked

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

# Piper inside a real Maya and a real Houdini. Kept out of `check`: they need
# the hosts on the machine.
test-host:
    uv run python packages/piper-maya/tests/host_maya.py
    uv run python packages/piper-houdini/tests/host_houdini.py

build:
    uv build --all-packages

check: lint typecheck test test-usd-24 isolate
