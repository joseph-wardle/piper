# Piper

Piper is a WIP production gateway for an animated-film pipeline. It gives
artists and automation one coherent way to invoke production workflows, while
the tracker, filesystem, OpenUSD, scheduler, and review systems keep the 
authority they already have.

Right now it is a sandbox for me to test workflows for my capstone film
production. `piper find` reads assets and shots from the real tracker, and
`piper create asset` creates an asset there and its directory in storage.

## Layout

| Path | Distribution | Import | Purpose |
| --- | --- | --- | --- |
| `packages/piper-core` | `piper-core` | `piper` | Operations shared by every presentation |
| `packages/piper-studio` | `piper-studio` | `piper_studio` | Studio conventions, production config, provider selection |
| `packages/piper-shotgrid` | `piper-shotgrid` | `piper_shotgrid` | ShotGrid behind Piper's contracts |
| `packages/piper-cli` | `piper-cli` | `piper_cli` | The `piper` command |

`piper-core` is imported in-process by DCC integrations, so it targets the
2025 VFX Reference Platform python version `3.11.x`. This project will update 
as soon as the software used at BYU target the 2026 target of `3.13.x`.

## Setup

Requires [uv](https://docs.astral.sh/uv/).

```
uv sync
```

## Use

Point `PIPER_PRODUCTION` at a production configuration and put the tracker
credential in the environment:

```
export PIPER_PRODUCTION=/path/to/production.toml
export PIPER_SHOTGRID_KEY=...
```

```toml
name = "sandwich"
root = "/groups/sandwich/05_production"
types = ["Character", "Environment", "Set Piece", "Vehicle"]

[shotgrid]
site = "https://byuanimation.shotgunstudio.com"
script = "sandwich_pipeline"
project = 716
```

`root` is where the production is stored. `types` are the asset types artists
may create, chosen from those ShotGrid offers.

```
piper find pan          # assets and shots whose name contains "pan"
piper find              # the whole production
piper find pan --json   # the same result, for another program
```

```
piper create asset "Frying Pan" --type "Set Piece" --folder kitchen
piper create asset "Toaster" --type "Set Piece" --folder garage --new-folder
```

The first creates the asset in ShotGrid, then `<root>/asset/kitchen/frying_pan`.
A folder no asset is in yet must be started with `--new-folder`. Running a
create again finishes whichever half is missing.

`create` writes to whichever project and root the configuration names, and the
example above is the live production. Until the next film has its own project,
create against a configuration for the inactive copy instead: `project = 782`
and `root = "/groups/sandwich/04_temp"`.

## Checks

With [just](https://just.systems/):

```
just check              # lint, typecheck, test, core-isolation
just test-integration   # real ShotGrid and storage; needs PIPER_SHOTGRID_KEY
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

`piper-core` must import with no third-party packages installed, which
`just isolate` checks.

## License

Apache-2.0. See `LICENSE`.
