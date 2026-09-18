# Piper

Piper is a WIP production gateway for an animated-film pipeline, and the shared
runtime for the studio's other tooling. It gives artists and automation one
coherent way to invoke production workflows, while the tracker, filesystem,
OpenUSD, scheduler, and review systems keep the authority they already have.

Right now it is a sandbox for me to test workflows for my capstone film
production. `piper find` reads assets and shots from the real tracker,
`piper create asset` creates an asset there and its directory in storage, and
`piper publish` installs an immutable version of an exported USD product.

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

Select the production later commands work in, and put the tracker credential in
the environment:

```
piper configure sandwich   # select a production; the choice outlives the shell
piper configure general    # return to general use, which has no production
piper configure            # report the selection
```

```
export PIPER_SHOTGRID_KEY=...
```

The selection is kept in `~/.config/piper/config.toml`. `PIPER_PRODUCTION` names
a configuration file directly and outranks it, which is how to work in a
production Piper does not list.

A production is one TOML file:

```toml
name = "sandwich"
root = "/groups/sandwich/05_production"
types = ["Character", "Environment", "Set Piece", "Vehicle"]

[shotgrid]
site = "https://byuanimation.shotgunstudio.com"
script = "sandwich_pipeline"
project = 716

[software]
maya = "2026"
```

`root` is where the production is stored. `types` are the asset types artists
may create, chosen from those ShotGrid offers. `[software]` names the release
of each application the production is made in, never where it is installed:
where a release lives differs by machine, and one configuration is read from
every machine. A production naming no release gets the one Piper is developed
against.

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

```
piper publish "Frying Pan" geo ./export/geo.usd
```

`publish` copies the layer and every file it depends on into the product's
next version, `<root>/asset/kitchen/frying_pan/publish/geo/v001/`, makes it
read-only, and registers it in ShotGrid as a PublishedFile. Its dependencies
must be inside the layer's directory, or be pins into installed versions spelled
from the production root, such as `asset/kitchen/frying_pan/publish/geo/v001/geo.usd`.
Publishing again installs another version; nothing is replaced.

`create` and `publish` write to whichever project and root the configuration
names, and the example above is the live production. Until the next film has
its own project, point `PIPER_PRODUCTION` at a configuration for the inactive
copy instead: `project = 782` and `root = "/groups/sandwich/04_temp"`.

```
piper launch maya
```

Maya is the release the production names, found at `/usr/autodesk/maya<release>`;
a machine keeping Maya somewhere else says so in Autodesk's own `MAYA_LOCATION`.
Maya starts with Piper's code importable, this command's environment discarded,
and the production's own Maya tools loaded. A production adds one by dropping a
folder and its `.mod` into `<root>/tools/maya`, which Piper puts on
`MAYA_MODULE_PATH` and never reads itself. Discarding matters: a shell with this
project's virtual environment active must not hand Maya a second `pxr`, which
breaks MayaUSD without an import error. Under a production Maya starts in the
production root, so search-path pins resolve there. Piper becomes Maya, so its
output is this terminal's and its exit code is the command's.

## Checks

With [just](https://just.systems/):

```
just check              # lint, typecheck, test, core-isolation
just test-host          # Piper inside a real Maya; needs Maya installed
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
