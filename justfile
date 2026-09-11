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

test-integration:
    uv run pytest -m integration

isolate:
    uv run --no-project --with ./packages/piper-core \
        python -c "import piper, piper.errors, piper.find, piper.tracker"

build:
    uv build --all-packages

check: lint typecheck test isolate
