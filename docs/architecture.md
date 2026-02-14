# Architecture

This document defines how `piper` is organized and where to extend it safely.

## Design Principles

- thin command layer
- deterministic resolution
- explicit precedence
- stable machine-friendly output behavior
- low coupling between parsing, config, and resolution

## Project Layout

- `src/piper/cli.py`: argument parsing, command dispatch, top-level error handling
- `src/piper/config.py`: layered config loading and precedence merge
- `src/piper/models.py`: immutable runtime data models
- `src/piper/errors.py`: user-facing domain exceptions
- `src/piper/commands/`: command entry points and orchestration
- `src/piper/resolvers/`: pure-ish path/script resolution logic

## Command Lifecycle

1. `piper.cli.main()` builds parser from registered commands.
2. CLI parses args and chooses command object.
3. If command requires context, `resolve_context()` builds `ResolvedContext`.
4. Command runs and delegates business logic to resolvers.
5. Resolver returns data or raises a domain error.
6. `main()` maps domain errors to stderr + exit code `1`.

## Boundary Rules

### `commands/`

Commands should:

- parse command-specific args
- call config/resolver APIs
- print only the documented command output
- return exit code

Commands should not:

- reimplement precedence logic
- embed path-finding behavior
- parse raw TOML directly

### `resolvers/`

Resolvers should:

- accept explicit inputs (no hidden global state)
- perform deterministic lookup in configured order
- raise typed errors with actionable context

Resolvers should not:

- do CLI parsing
- print output
- read global config files directly

### `config.py`

`config.py` owns:

- config file locations
- TOML parsing and validation
- precedence merge
- show selection rules

All precedence behavior should remain centralized here.

## Current Deterministic Precedence

Highest to lowest:

1. CLI (`--show`)
2. environment (`PIPER_SHOW`, `PIPER_SCRIPT_DIRS`, `PIPER_GOTO_<KIND>`)
3. show config (`/groups/<show>/pipeline/piper.toml`)
4. user config (`~/.config/piper/config.toml`)
5. built-in defaults

## Resolver Contracts

### Path Resolver (`resolvers/paths.py`)

- kinds are dynamic from merged config keys
- templates support `{root}` and `{id}` only
- wildcard candidates (`*`, `?`, `[]`) are expanded in sorted order
- first existing path wins
- failure includes attempted candidate paths

### Script Resolver (`resolvers/scripts.py`)

- script name must be basename only
- lookup is exact name first, then `.py`
- directory order is respected
- listing returns sorted, unique names across dirs

## Extension Points

### Add a new command

1. Create `src/piper/commands/<name>.py` with command class.
2. Implement `configure()` and `run()`.
3. Register command in `src/piper/commands/__init__.py`.
4. Add command-focused tests in `tests/test_cli_commands.py` or a new test module.
5. Update `README.md` and architecture docs if command surface changes.

### Add a new path kind

1. Add `goto.<kind>` templates in show/user config.
2. No code changes required for resolver support.
3. Add tests for candidate order and missing-path behavior.

### Change precedence behavior

1. Update only `src/piper/config.py` merge/select logic.
2. Add/update precedence tests in `tests/test_config.py`.
3. Document behavior changes in README and architecture docs.

### Add future tool families (example: `open`)

Recommended pattern:

1. document expected command behavior and output first
2. keep command module thin
3. add resolver module for lookup logic
4. add focused tests for machine output and failure modes

## Testing Strategy

Keep tests sparse and high signal:

- config precedence (`tests/test_config.py`)
- path resolution and candidate behavior (`tests/test_path_resolver.py`)
- script validation and lookup (`tests/test_script_resolver.py`)
- end-to-end CLI smoke (`tests/test_cli_commands.py`)

Run full local gate before changes are merged:

```bash
uv run ruff check . && uv run ruff format --check . && uv run ty check && uv run pytest -q
```
