# piper

`piper` is a lightweight CLI utility belt for BYU's film pipeline.

- architecture: `docs/architecture.md`

## Quickstart

1. Install local dev dependencies.

```bash
uv sync --dev
```

2. Verify the CLI is wired.

```bash
uv run piper --help
uv run piper doctor
```

3. Run the quality gate.

```bash
uv run ruff check . && uv run ruff format --check . && uv run ty check && uv run pytest -q
```

## Install CLI on PATH

`piper` must be installed before you call `piper init ...` from shell startup files.

Install from this repo:

```bash
cd /users/animation/joseward/Documents/piper
uv tool install --editable .
```

Ensure uv tool binaries are on PATH:

```bash
uv tool update-shell
```

Verify:

```bash
which piper
piper --help
```

## Shell Setup (`goto`)

`piper goto ...` can only change directory when shell integration is loaded.

Bash (`~/.bashrc`):

```bash
if command -v piper >/dev/null 2>&1; then
  eval "$(piper init bash)"
fi
```

Zsh (`~/.zshrc`):

```zsh
if command -v piper >/dev/null 2>&1; then
  eval "$(piper init zsh)"
fi
```

Reload your shell, then verify:

```bash
type piper
piper goto shot F_160
```

## Usage

```bash
piper path <kind> <id>
piper goto <kind> <id>
piper run <script> [-- args...]
piper run --list
piper doctor
```

Examples:

```bash
piper path shot F_160
piper path asset fence_door
piper run rm_generate_report -- --today
piper run --list
```

## Render Wedge Report Script

Local script name: `rm_wedge_report`

Current scope:

- Parse wedge runs + per-frame RenderMan stats JSON
- Aggregate performance, memory, and hotspot diagnostics
- Produce heuristic recommendations for `dailies` and `final`
- Compare candidate EXRs against ground truth EXRs (RGB) with OIIO `--diff` metrics
- Generate visual spotlight triptychs (ground truth, candidate, absolute diff)
- Generate image-quality charts (scatter + heatmaps)
- Write a clean HTML report and machine-readable CSV/JSON outputs

Run it:

```bash
piper run rm_wedge_report -- \
  --root /groups/bobo/production/shot/C_010/render/tests/2026-02-13_wedge \
  --out /tmp/rm_wedge_report
```

Key options:

- `--attempt-policy latest|first|max-mainloop` (default: `latest`)
- `--include-group <name>` (repeatable)
- `--exclude-group <name>` (repeatable)
- `--images-subdir <name>` (default: `images_dn`)
- `--ground-truth-group <name>` (default: `ground_truth`)
- `--spotlight-limit <int>` (default: `6`)
- `--disable-image-analysis`

Outputs:

- `<out>/report.html`
- `<out>/data/frames.csv`
- `<out>/data/runs.csv`
- `<out>/data/recommendations.json`
- `<out>/data/image_frames.csv` (when image analysis is enabled)
- `<out>/data/image_runs.csv` (when image analysis is enabled)
- `<out>/data/warnings.json`

Image-analysis dependency:

- `oiiotool` must be available on `PATH` for EXR comparisons and visual assets.
- If `oiiotool` is unavailable, the script still writes the core stats report and adds a warning that image analysis was skipped.

Behavior:

- `path` prints only the resolved path to stdout.
- `goto` resolves exactly like `path`.
- `run` executes with current Python and current working directory.
- `run --list` prints machine-clean sorted unique names.

## Configuration

Paths:

- user config: `~/.config/piper/config.toml`
- show config: `/groups/<show>/pipeline/piper.toml`

Templates:

- `examples/config.toml`
- `examples/piper.toml`

Copy templates:

```bash
mkdir -p ~/.config/piper
cp examples/config.toml ~/.config/piper/config.toml
# copy show config template to your show pipeline root as needed
```

### Precedence

Highest to lowest:

1. CLI flags
2. Environment variables
3. Show config
4. User config
5. Built-in defaults

Environment overrides:

- `PIPER_SHOW`
- `PIPER_SCRIPT_DIRS` (`os.pathsep` separated)
- `PIPER_GOTO_<KIND>` (`os.pathsep` separated)

### bobo notes

Typical production roots:

- data: `/groups/bobo/production`
- pipeline: `/groups/bobo/pipeline`

Asset paths in bobo are spread across multiple families (`asset`, `assets`, `character`) and nested categories. Keep `goto.asset` as an ordered candidate list.

## Troubleshooting

### `goto` prints a path but does not `cd`

Shell wrapper is not loaded. Run:

```bash
eval "$(piper init bash)"   # or zsh
```

### `command not found: piper` on shell startup

Install `piper` and ensure uv tool bin is on PATH:

```bash
cd /users/animation/joseward/Documents/piper
uv tool install --editable .
uv tool update-shell
```

Use the guarded shell snippet from the shell setup section so startup does not fail before install.

### `unknown show` or `unable to resolve show`

Check:

- `[shows]` exists in user config
- `default_show` points to a valid key
- `--show` or `PIPER_SHOW` names a configured show
- cwd is under a configured show root when relying on inference

### `unable to resolve '<kind> <id>'`

Check:

- `goto.<kind>` exists
- candidate order reflects real directory layout
- asset kinds include wildcard levels (`*/{id}`, `*/*/{id}`) where required

### `script '<name>' not found`

Check:

- `piper run --list`
- `scripts.dirs` values
- `PIPER_SCRIPT_DIRS` override

## Extension Rules

Use `docs/architecture.md` as the source of truth.

Practical rules:

1. Keep `commands/` thin; push lookup logic into `resolvers/`.
2. Keep precedence behavior centralized in `src/piper/config.py`.
3. Preserve machine-clean stdout behavior for `path` and `run --list`.
4. Add focused tests before expanding command surface area.
