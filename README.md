# Piper

Piper is a WIP production gateway for an animated-film pipeline, and the shared
runtime for the studio's other tooling. It gives artists and automation one
coherent way to invoke production workflows, while the tracker, filesystem,
OpenUSD, scheduler, and review systems keep the authority they already have.

Right now it is a sandbox for me to test workflows for my capstone film
production. `piper find` reads assets and shots from the real tracker,
`piper create asset` creates an asset there and its directory in storage,
`piper publish` installs an immutable version of an exported USD product, and
`piper open` starts Maya or Houdini on an asset's work.

## Layout

| Path | Distribution | Import | Purpose |
| --- | --- | --- | --- |
| `packages/piper-core` | `piper-core` | `piper` | Operations shared by every presentation |
| `packages/piper-studio` | `piper-studio` | `piper_studio` | Studio conventions, production config, provider selection |
| `packages/piper-shotgrid` | `piper-shotgrid` | `piper_shotgrid` | ShotGrid behind Piper's contracts |
| `packages/piper-cli` | `piper-cli` | `piper_cli` | The `piper` command |
| `packages/piper-maya` | `piper-maya` | `piper_maya` | Piper inside Maya: its menu, and opening, previewing, and publishing work |
| `packages/piper-houdini` | `piper-houdini` | `piper_houdini` | Piper inside Houdini: its menu, its material node, and opening and publishing work |

`piper-core` is imported in-process by DCC integrations, so it targets the
2025 VFX Reference Platform python version `3.11.x`. This project will update 
as soon as the software used at BYU target the 2026 target of `3.13.x`.

## Setup

Requires [uv](https://docs.astral.sh/uv/) and [just](https://just.systems/).

```
just sync
```

This builds three environments from one lock: the project's own, and one per
host, `packages/piper-maya/.venv` and `packages/piper-houdini/.venv`, each holding
only what its package names and being the one directory of third-party packages
that host imports from.

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
houdini = "21.0"
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
A folder no asset is in yet must be started with `--new-folder`. The directory is
named by the asset's pipe name, the slug of its name, written to ShotGrid once and
never changed: renaming the asset later moves nothing. A create never adopts a
directory that is already there. Running a create again finishes whatever is
missing: the pipe name of an asset Piper did not create, or the directory.

```
piper publish "Frying Pan" geo ./export/geo.usd
piper publish "Frying Pan" mtl ./export/mtl.usda --with geo=1
```

`publish` copies the layer and every file it depends on into the product's
next version, `<root>/asset/kitchen/frying_pan/publish/geo/v001/`, and
registers it in ShotGrid as a PublishedFile. The layer is named for its product.
Its dependencies must be inside the layer's directory, or be pins into installed
versions spelled from the production root, such as
`asset/kitchen/frying_pan/publish/geo/v001/geo.usd`. Publishing again installs
another version; nothing is replaced.

It then builds the asset version, `publish/asset/v003/frying_pan.usda`, which
pins this component beside the versions the current asset version pins, and
makes that version current. `--with` pins another component's version instead
of current's, for that publish only; a component nothing pins is left out. The
hosts' Publish… windows offer the versions of the components current pins;
`--with` can pin any installed version.
Whatever fails after the component is installed is reported with what was left,
and nothing is undone.

```
piper current "Frying Pan"       # which asset version consumers get, and its pins
piper current "Frying Pan" 2     # make an older one current again
```

Current is one layer, `publish/asset/frying_pan.usda`, sublayering the version.
A publish moves it last, and this command is the only other thing that moves it;
every older asset version stays where it is.

```
piper convert ./painter_export
piper publish "Frying Pan" tex ./painter_export
```

`convert` writes a RenderMan `.tex` beside every PNG Painter exported, named
`<slot>_<map>.<udim>.png`: colour maps become ACEScg half EXR, data maps TIFF
at the depth exported. It needs RenderMan, found through `RMANTREE`, and no
production; with the **Piper Material** node pointed at that directory, a class
project has its materials too. Publishing `tex` installs the export as one version,
`publish/tex/v001/`, with a `.tex` converted from each PNG the same way; a
`.tex` already in the export is never installed. Then it derives the current
material to read the version: every texture path in the current `mtl` that
lies in a `tex` version is pointed at the new one, and the result is published
as the next `mtl`, which builds the asset version and makes it current. The
derived layer says what it came from in its own metadata. A material that reads
no published textures is left alone and said; what the export holds that no
material reads, a texture set no slot of the pinned geo has, and a map whose
tiles changed are installed and said.

`create` and `publish` write to whichever project and root the configuration
names, and the example above is the live production. Until the next film has
its own project, point `PIPER_PRODUCTION` at a configuration for the inactive
copy instead: `project = 782` and `root = "/groups/sandwich/04_temp"`.

```
piper open "Frying Pan" modeling
piper open fry modeling            # a part of the name only one asset has
piper open fry lookdev             # in Houdini
```

`open` becomes the context's host on the asset's one work file: for modeling,
Maya on `<root>/asset/kitchen/frying_pan/work/modeling/frying_pan.mb`, creating
and saving it the first time, with Maya's project set to that directory. Inside Maya,
**Piper ▸ Open Work…** does the same from a list. The scene is stamped with its
production, asset, and context, which is how publishing from Maya knows what
it is. A file copied there from another asset becomes this asset's work: Piper
restamps it, saves it, and says so. Anything else in the directory is the
artist's, Maya's `workspace.mel` and incremental saves included.

**Piper ▸ Publish…** publishes the selected geometry as the asset's next `geo`
version, under one root prim named for the asset. Each material is kept as a
named slot the geometry is bound to, without its shading. The window says what is
selected, which asset version is current and what it pins, and offers the other
components' versions to pin, starting on current's. The version keeps the scene
it came from in `src/`. A scene with unsaved changes can be saved first or
published as it is, which leaves the work file untouched. Only an asset's own
work file publishes: import anything else into it first.

**Piper ▸ Preview…** opens the selection in usdview, composed as a publish would
compose it with what current pins, and installs nothing. The viewer is
the production's Houdini's usdview, which carries its render delegates, so the
look can be RenderMan's. What it shows is deleted when it closes, and a viewer
that closes with an error says so.

Lookdev is Houdini's. **Piper ▸ Open Work…** opens the asset's one lookdev file,
`work/lookdev/frying_pan.hipnc`, saying so when the asset version it loads is no
longer current or the textures it reads are older than the newest published, and
starts a new one from the asset version that is current: in `/stage`, a Sublayer
of that version, a Layer Break, a Material Library whose prefix is the asset's
`mtl` scope, the `OUT_mtl` output a publish saves, and below it a dome light, a
camera, and Karma render settings for looking, which are never published. Inside
the library sits a **Piper Material** node pointed at the newest `tex` version,
its materials added once: its Add Materials button adds, for each texture set in
its directory that has no material yet, a PxrSurface reading the `.tex` files and
a UsdPreviewSurface reading the `.jpg` previews, wired by the map table, every path
an expression off the node's one parm. It never touches a material that exists.
**Piper ▸ Use Textures…** points that node at another `tex` version, so every
material reads it, and says what they read that the version lacks. Materials are
named for the slots the geometry left under `mtl`, and a texture set is named for
its slot, so the generated ones fill them. **Piper ▸ Publish…** saves the layer above `OUT_mtl`, says which asset
version the scene loads and which is current, what each slot of the geo being
pinned was given, and offers the other components' versions to pin, then publishes
it as the next `mtl` version. A layer that sublayers the asset, which a deleted Layer Break does,
authors outside `mtl`, or names a material for no slot is refused.

```
piper launch maya
piper launch houdini
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

Houdini is found at `/opt/hfs<release>`, or wherever SideFX's own `HFS` points.
It starts the same way, in the foreground, with `packages/piper-houdini` on
`HOUDINI_PATH` for Piper's menu and startup hook, and the production's Houdini
packages loaded from `<root>/tools/houdini`, which Piper puts on
`HOUDINI_PACKAGE_DIR` and never reads itself.

## Checks

With [just](https://just.systems/):

```
just check              # lint, typecheck, test, core-isolation
just test-host          # Piper inside a real Maya and Houdini; needs them installed
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
