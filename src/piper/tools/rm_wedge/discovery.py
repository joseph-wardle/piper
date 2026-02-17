from __future__ import annotations

import re
from pathlib import Path

from .models import DiscoveredRun, DiscoveryResult, RunSettings

_RUN_NAME_RE = re.compile(
    r"^(?P<group>.+?)_resolution_scale_(?P<rs>\d+p\d+)_pixel_variance_(?P<pv>\d+p\d+)_max_samples_(?P<ms>\d+)$"
)
_FRAME_RE = re.compile(r"\.(?P<frame>\d{4})\.json$")


def _token_to_float(token: str) -> float:
    return float(token.replace("p", ".", 1))


def parse_run_settings(run_dir_name: str) -> RunSettings:
    match = _RUN_NAME_RE.match(run_dir_name)
    if match is None:
        raise ValueError(
            f"run name does not match expected wedge pattern: {run_dir_name}"
        )

    return RunSettings(
        group=match.group("group"),
        resolution_scale=_token_to_float(match.group("rs")),
        pixel_variance=_token_to_float(match.group("pv")),
        max_samples=int(match.group("ms")),
        run_name=run_dir_name,
    )


def parse_frame_number(path: Path) -> int:
    match = _FRAME_RE.search(path.name)
    if match is None:
        raise ValueError(f"unable to parse frame id from stats filename: {path.name}")
    return int(match.group("frame"))


def _iter_run_dirs(root: Path) -> list[Path]:
    run_dirs: list[Path] = []

    for group_dir in sorted(root.iterdir()):
        if not group_dir.is_dir():
            continue
        if group_dir.name.startswith("_"):
            continue

        for run_dir in sorted(group_dir.iterdir()):
            if run_dir.is_dir():
                run_dirs.append(run_dir)

    return run_dirs


def discover_runs(root: Path) -> DiscoveryResult:
    if not root.exists():
        raise FileNotFoundError(f"wedge root does not exist: {root}")
    if not root.is_dir():
        raise NotADirectoryError(f"wedge root is not a directory: {root}")

    warnings: list[str] = []
    runs: list[DiscoveredRun] = []

    for run_dir in _iter_run_dirs(root):
        try:
            settings = parse_run_settings(run_dir.name)
        except ValueError:
            warnings.append(f"skipping unrecognized run directory: {run_dir}")
            continue

        stats_dir = run_dir / "stats"
        if not stats_dir.is_dir():
            warnings.append(f"run missing stats directory: {run_dir}")
            continue

        valid_stats_files: list[Path] = []
        for stats_file in sorted(stats_dir.glob("*.json")):
            try:
                parse_frame_number(stats_file)
            except ValueError:
                warnings.append(f"ignoring malformed stats filename: {stats_file}")
                continue
            valid_stats_files.append(stats_file)

        if not valid_stats_files:
            warnings.append(f"run has no valid stats files: {run_dir}")
            continue

        runs.append(
            DiscoveredRun(
                settings=settings,
                run_dir=run_dir,
                stats_files=tuple(valid_stats_files),
                render_usd=run_dir / "render.usd"
                if (run_dir / "render.usd").is_file()
                else None,
                denoise_json=(
                    run_dir / "denoise.json"
                    if (run_dir / "denoise.json").is_file()
                    else None
                ),
            )
        )

    runs.sort(
        key=lambda run: (
            run.settings.group,
            run.settings.resolution_scale,
            run.settings.pixel_variance,
            run.settings.max_samples,
            run.run_dir.as_posix(),
        )
    )

    return DiscoveryResult(runs=tuple(runs), warnings=tuple(warnings))
