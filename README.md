# Piper

Piper is a WIP production gateway for an animated-film pipeline. It gives
artists and automation one coherent way to invoke production workflows, while
the tracker, filesystem, OpenUSD, scheduler, and review systems keep the 
authority they already have.

This repository currently contains the project skeleton. Right now, it is 
nothing but a sandbox for me to test workflows for my capstone film production

## Layout

| Path | Distribution | Import | Purpose |
| --- | --- | --- | --- |
| `packages/piper-core` | `piper-core` | `piper` | Operations shared by every presentation |
| `packages/piper-cli` | `piper-cli` | `piper_cli` | The `piper` command |

`piper-core` is imported in-process by DCC integrations, so it targets the
2025 VFX Reference Platform python version `3.11.x`. This project will update 
as soon as the software used at BYU target the 2026 target of `3.13.x`.

## Setup

Requires [uv](https://docs.astral.sh/uv/).

```
uv sync
```

## Checks

With [just](https://just.systems/):

```
just check    # lint, typecheck, test
just build
```

Or directly:

```
uv run ruff check .
uv run ruff format --check .
uv run ty check
uv run pytest
uv build --all-packages
```

## License

Apache-2.0. See `LICENSE`.
