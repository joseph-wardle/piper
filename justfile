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

build:
    uv build --all-packages

check: lint typecheck test
