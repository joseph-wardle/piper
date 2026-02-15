from __future__ import annotations

import argparse
from collections.abc import Sequence
from pathlib import Path

from .analysis import (
    build_frame_records,
    build_hotspot_overview,
    build_recommendations,
    collect_data_warnings,
    summarize_runs,
)
from .discovery import discover_runs
from .report import write_outputs
from .stats_extract import AttemptPolicy


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Analyze RenderMan wedge stats and generate a clean HTML + CSV report. "
            "Pass 1 focuses on core stats and recommendation heuristics."
        )
    )
    parser.add_argument("--root", type=Path, required=True, help="Wedge root directory")
    parser.add_argument("--out", type=Path, required=True, help="Output directory")
    parser.add_argument(
        "--attempt-policy",
        choices=["latest", "first", "max-mainloop"],
        default="latest",
        help=(
            "Which frame.attempts entry to analyze. "
            "Use latest for checkpoint-style files (default)."
        ),
    )
    parser.add_argument(
        "--include-group",
        action="append",
        default=[],
        help="Include only the named group (repeatable)",
    )
    parser.add_argument(
        "--exclude-group",
        action="append",
        default=[],
        help="Exclude the named group (repeatable)",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress non-error progress logs",
    )

    return parser.parse_args(argv)


def _filter_groups(
    groups: list[str], include_group: list[str], exclude_group: list[str]
) -> list[str]:
    include = {item.lower() for item in include_group}
    exclude = {item.lower() for item in exclude_group}

    filtered: list[str] = []
    for group in groups:
        group_lower = group.lower()
        if include and group_lower not in include:
            continue
        if group_lower in exclude:
            continue
        filtered.append(group)

    return filtered


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)

    discovery = discover_runs(args.root)

    available_groups = sorted({run.settings.group for run in discovery.runs})
    selected_groups = _filter_groups(
        available_groups,
        include_group=list(args.include_group),
        exclude_group=list(args.exclude_group),
    )

    selected_group_set = set(selected_groups)
    selected_runs = [
        run for run in discovery.runs if run.settings.group in selected_group_set
    ]

    if not selected_runs:
        raise SystemExit("No runs selected after group filtering")

    attempt_policy = args.attempt_policy
    typed_policy: AttemptPolicy = attempt_policy

    frame_rows, extraction_warnings = build_frame_records(selected_runs, typed_policy)
    run_rows = summarize_runs(frame_rows)
    recommendations = build_recommendations(run_rows)
    hotspots = build_hotspot_overview(frame_rows)
    all_warnings = collect_data_warnings(
        discovery.warnings,
        extraction_warnings,
        frame_rows,
    )

    write_outputs(
        out_dir=args.out,
        frames=frame_rows,
        runs=run_rows,
        recommendations=recommendations,
        hotspot_overview=hotspots,
        warnings=all_warnings,
    )

    if not args.quiet:
        print(f"Wedge root: {args.root}")
        print(f"Runs analyzed: {len(run_rows)}")
        print(f"Frames analyzed: {len(frame_rows)}")
        print(f"Groups analyzed: {', '.join(selected_groups)}")
        print(f"Attempt policy: {typed_policy}")
        print(f"Report: {(args.out / 'report.html')}")

    return 0
