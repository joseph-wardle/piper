from __future__ import annotations

import dataclasses
import json
import math
import statistics
from collections import defaultdict
from collections.abc import Iterable, Mapping

from .discovery import parse_frame_number
from .models import (
    DiscoveredRun,
    FrameRecord,
    HotspotSummary,
    Recommendation,
    RunSummary,
)
from .stats_extract import AttemptPolicy, choose_attempt, extract_core_metrics


def _median(values: Iterable[float]) -> float | None:
    data = [value for value in values if math.isfinite(value)]
    if not data:
        return None
    return statistics.median(data)


def _safe_div(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator is None:
        return None
    if denominator <= 0.0:
        return None
    return numerator / denominator


def _norm_low_better(value: float | None, lo: float, hi: float) -> float:
    if value is None:
        return 1.0
    if hi <= lo:
        return 0.0
    return (value - lo) / (hi - lo)


def _norm_high_better(value: float, lo: float, hi: float) -> float:
    if hi <= lo:
        return 1.0
    return (value - lo) / (hi - lo)


def _quantile(values: list[float], quantile: float) -> float:
    if not values:
        raise ValueError("cannot compute quantile of an empty sequence")
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]

    position = quantile * (len(ordered) - 1)
    left = math.floor(position)
    right = math.ceil(position)
    if left == right:
        return ordered[left]

    frac = position - left
    return ordered[left] * (1.0 - frac) + ordered[right] * frac


def _pareto_frontier(runs: list[RunSummary]) -> list[RunSummary]:
    frontier: list[RunSummary] = []

    for candidate in runs:
        candidate_time = candidate.median_mainloop_s
        candidate_mem = candidate.median_peak_mem_gib
        candidate_quality = candidate.quality_proxy

        dominated = False
        for other in runs:
            if other is candidate:
                continue

            other_time = other.median_mainloop_s
            other_mem = other.median_peak_mem_gib
            other_quality = other.quality_proxy

            if (
                other_time is not None
                and candidate_time is not None
                and other_time <= candidate_time
                and (
                    other_mem is None
                    or candidate_mem is None
                    or other_mem <= candidate_mem
                )
                and other_quality >= candidate_quality
                and (
                    other_time < candidate_time
                    or (
                        other_mem is not None
                        and candidate_mem is not None
                        and other_mem < candidate_mem
                    )
                    or other_quality > candidate_quality
                )
            ):
                dominated = True
                break

        if not dominated:
            frontier.append(candidate)

    return frontier


def build_frame_records(
    runs: Iterable[DiscoveredRun], attempt_policy: AttemptPolicy
) -> tuple[list[FrameRecord], list[str]]:
    rows: list[FrameRecord] = []
    warnings: list[str] = []

    for run in runs:
        for stats_file in run.stats_files:
            frame_number = parse_frame_number(stats_file)

            try:
                payload = json.loads(stats_file.read_text(encoding="utf-8"))
            except json.JSONDecodeError as exc:
                warnings.append(f"invalid json {stats_file}: {exc}")
                continue

            if not isinstance(payload, Mapping):
                warnings.append(f"stats payload is not a mapping: {stats_file}")
                continue

            try:
                attempt_index, attempt_count, selected_attempt = choose_attempt(
                    payload, attempt_policy
                )
            except ValueError as exc:
                warnings.append(f"{stats_file}: {exc}")
                continue

            metrics = extract_core_metrics(selected_attempt)

            row_warnings: list[str] = []
            if metrics.mainloop_s is None:
                row_warnings.append("missing_mainloop")
            if metrics.proc_mem_peak_committed_b is None:
                row_warnings.append("missing_peak_memory")
            if metrics.cpu_pct_avg is None:
                row_warnings.append("missing_cpu_avg")

            rows.append(
                FrameRecord(
                    settings=run.settings,
                    run_dir=run.run_dir,
                    frame=frame_number,
                    stats_file=stats_file,
                    attempt_index=attempt_index,
                    attempt_count=attempt_count,
                    metrics=metrics,
                    warnings=tuple(row_warnings),
                )
            )

    rows.sort(
        key=lambda row: (
            row.settings.group,
            row.settings.resolution_scale,
            row.settings.pixel_variance,
            row.settings.max_samples,
            row.frame,
        )
    )

    return rows, warnings


def summarize_runs(rows: Iterable[FrameRecord]) -> list[RunSummary]:
    grouped: dict[tuple[str, float, float, int, str], list[FrameRecord]] = defaultdict(
        list
    )

    for row in rows:
        grouped[
            (
                row.settings.group,
                row.settings.resolution_scale,
                row.settings.pixel_variance,
                row.settings.max_samples,
                row.run_dir.as_posix(),
            )
        ].append(row)

    summaries: list[RunSummary] = []

    for group_rows in grouped.values():
        first = group_rows[0]

        frame_count = len(group_rows)
        usable_frames = sum(
            1 for row in group_rows if row.metrics.mainloop_s is not None
        )

        peak_mem_gib = [
            row.metrics.proc_mem_peak_committed_b / (1024.0**3)
            for row in group_rows
            if row.metrics.proc_mem_peak_committed_b is not None
        ]
        rays_per_s = [
            row.metrics.rays_traced / row.metrics.mainloop_s
            for row in group_rows
            if row.metrics.rays_traced is not None
            and row.metrics.mainloop_s is not None
            and row.metrics.mainloop_s > 0.0
        ]

        hotspot_trace = [
            _safe_div(row.metrics.trace_shadows_s, row.metrics.mainloop_s)
            for row in group_rows
        ]
        hotspot_bxdf = [
            _safe_div(row.metrics.shade_hit_bxdf_s, row.metrics.mainloop_s)
            for row in group_rows
        ]
        hotspot_opacity = [
            _safe_div(row.metrics.shade_hit_opacity_s, row.metrics.mainloop_s)
            for row in group_rows
        ]
        hotspot_groups = [
            _safe_div(row.metrics.shade_get_shade_groups_s, row.metrics.mainloop_s)
            for row in group_rows
        ]
        hotspot_displacement = [
            _safe_div(row.metrics.shade_build_displacement_s, row.metrics.mainloop_s)
            for row in group_rows
        ]
        hotspot_geom = [
            _safe_div(row.metrics.create_geom_proto_s, row.metrics.mainloop_s)
            for row in group_rows
        ]

        run_warnings: list[str] = []
        if usable_frames < frame_count:
            run_warnings.append(
                f"usable_frames={usable_frames} of {frame_count}; some stats were incomplete"
            )

        summaries.append(
            RunSummary(
                settings=first.settings,
                run_dir=first.run_dir,
                frame_count=frame_count,
                usable_frames=usable_frames,
                median_mainloop_s=_median(
                    row.metrics.mainloop_s
                    for row in group_rows
                    if row.metrics.mainloop_s is not None
                ),
                median_ttfp_s=_median(
                    row.metrics.ttfp_s
                    for row in group_rows
                    if row.metrics.ttfp_s is not None
                ),
                median_peak_mem_gib=_median(peak_mem_gib),
                median_cpu_avg_pct=_median(
                    row.metrics.cpu_pct_avg
                    for row in group_rows
                    if row.metrics.cpu_pct_avg is not None
                ),
                median_rays_per_s=_median(rays_per_s),
                median_share_trace_shadows=_median(
                    value for value in hotspot_trace if value is not None
                ),
                median_share_shade_bxdf=_median(
                    value for value in hotspot_bxdf if value is not None
                ),
                median_share_shade_opacity=_median(
                    value for value in hotspot_opacity if value is not None
                ),
                median_share_shade_groups=_median(
                    value for value in hotspot_groups if value is not None
                ),
                median_share_shade_displacement=_median(
                    value for value in hotspot_displacement if value is not None
                ),
                median_share_geom_proto=_median(
                    value for value in hotspot_geom if value is not None
                ),
                quality_proxy=0.0,
                time_norm=0.0,
                memory_norm=0.0,
                objective_dailies=0.0,
                objective_final=0.0,
                warnings=tuple(run_warnings),
            )
        )

    return _apply_group_scores(summaries)


def _apply_group_scores(summaries: list[RunSummary]) -> list[RunSummary]:
    by_group: dict[str, list[RunSummary]] = defaultdict(list)
    for summary in summaries:
        by_group[summary.settings.group].append(summary)

    scored: list[RunSummary] = []

    for _group_name, group_runs in by_group.items():
        # Quality proxy components from settings.
        resolutions = [run.settings.resolution_scale for run in group_runs]
        inv_variances = [1.0 / run.settings.pixel_variance for run in group_runs]
        sample_logs = [math.log2(run.settings.max_samples) for run in group_runs]

        res_lo, res_hi = min(resolutions), max(resolutions)
        inv_var_lo, inv_var_hi = min(inv_variances), max(inv_variances)
        smp_lo, smp_hi = min(sample_logs), max(sample_logs)

        time_values = [
            run.median_mainloop_s
            for run in group_runs
            if run.median_mainloop_s is not None
        ]
        mem_values = [
            run.median_peak_mem_gib
            for run in group_runs
            if run.median_peak_mem_gib is not None
        ]

        time_lo = min(time_values) if time_values else 0.0
        time_hi = max(time_values) if time_values else 0.0
        mem_lo = min(mem_values) if mem_values else 0.0
        mem_hi = max(mem_values) if mem_values else 0.0

        for run in group_runs:
            quality_resolution = _norm_high_better(
                run.settings.resolution_scale, res_lo, res_hi
            )
            quality_variance = _norm_high_better(
                1.0 / run.settings.pixel_variance,
                inv_var_lo,
                inv_var_hi,
            )
            quality_samples = _norm_high_better(
                math.log2(run.settings.max_samples),
                smp_lo,
                smp_hi,
            )

            quality_proxy = (
                0.45 * quality_resolution
                + 0.35 * quality_variance
                + 0.20 * quality_samples
            )

            time_norm = _norm_low_better(run.median_mainloop_s, time_lo, time_hi)
            memory_norm = _norm_low_better(run.median_peak_mem_gib, mem_lo, mem_hi)

            objective_dailies = (
                0.70 * time_norm + 0.20 * memory_norm + 0.10 * (1.0 - quality_proxy)
            )
            objective_final = (
                0.55 * (1.0 - quality_proxy) + 0.30 * time_norm + 0.15 * memory_norm
            )

            scored.append(
                dataclasses.replace(
                    run,
                    quality_proxy=quality_proxy,
                    time_norm=time_norm,
                    memory_norm=memory_norm,
                    objective_dailies=objective_dailies,
                    objective_final=objective_final,
                )
            )

    scored.sort(
        key=lambda run: (
            run.settings.group,
            run.settings.resolution_scale,
            run.settings.pixel_variance,
            run.settings.max_samples,
        )
    )
    return scored


def build_recommendations(summaries: Iterable[RunSummary]) -> list[Recommendation]:
    by_group: dict[str, list[RunSummary]] = defaultdict(list)
    for summary in summaries:
        if summary.settings.group.lower() == "ground_truth":
            continue
        by_group[summary.settings.group].append(summary)

    recommendations: list[Recommendation] = []

    for group_name, group_runs in sorted(by_group.items()):
        frontier = _pareto_frontier(group_runs)
        objective_name = "objective_dailies"

        if group_name.lower() == "dailies":
            objective_name = "objective_dailies"
            quality_cut = _quantile([run.quality_proxy for run in frontier], 0.35)
            eligible = [run for run in frontier if run.quality_proxy >= quality_cut]
            if not eligible:
                eligible = frontier

            selected = min(
                eligible,
                key=lambda run: (
                    run.objective_dailies,
                    run.median_mainloop_s
                    if run.median_mainloop_s is not None
                    else float("inf"),
                ),
            )
        else:
            objective_name = "objective_final"
            quality_cut = _quantile([run.quality_proxy for run in frontier], 0.70)
            eligible = [run for run in frontier if run.quality_proxy >= quality_cut]
            if not eligible:
                eligible = frontier

            selected = min(
                eligible,
                key=lambda run: (
                    run.objective_final,
                    -(run.quality_proxy),
                ),
            )

        alternatives = sorted(
            [run for run in group_runs if run is not selected],
            key=lambda run: (
                run.objective_dailies
                if objective_name == "objective_dailies"
                else run.objective_final,
                run.median_mainloop_s
                if run.median_mainloop_s is not None
                else float("inf"),
            ),
        )[:3]

        notes = (
            f"Pareto frontier size: {len(frontier)} of {len(group_runs)} runs.",
            (
                "Quality proxy is derived from resolution_scale, pixel_variance, "
                "and max_samples (no image-diff quality in pass 1)."
            ),
        )

        recommendations.append(
            Recommendation(
                group=group_name,
                objective_name=objective_name,
                selected=selected,
                alternatives=tuple(alternatives),
                pareto_count=len(frontier),
                selection_notes=notes,
            )
        )

    return recommendations


def build_hotspot_overview(rows: Iterable[FrameRecord]) -> list[HotspotSummary]:
    hotspot_values: dict[str, list[float]] = {
        "trace_shadows": [],
        "shade_hit_bxdf": [],
        "shade_hit_opacity": [],
        "shade_get_shade_groups": [],
        "shade_build_displacement": [],
        "create_geometry_prototype": [],
    }

    for row in rows:
        mainloop = row.metrics.mainloop_s
        if mainloop is None or mainloop <= 0.0:
            continue

        ratios = {
            "trace_shadows": _safe_div(row.metrics.trace_shadows_s, mainloop),
            "shade_hit_bxdf": _safe_div(row.metrics.shade_hit_bxdf_s, mainloop),
            "shade_hit_opacity": _safe_div(row.metrics.shade_hit_opacity_s, mainloop),
            "shade_get_shade_groups": _safe_div(
                row.metrics.shade_get_shade_groups_s,
                mainloop,
            ),
            "shade_build_displacement": _safe_div(
                row.metrics.shade_build_displacement_s,
                mainloop,
            ),
            "create_geometry_prototype": _safe_div(
                row.metrics.create_geom_proto_s,
                mainloop,
            ),
        }

        for name, value in ratios.items():
            if value is not None and math.isfinite(value):
                hotspot_values[name].append(value)

    overview: list[HotspotSummary] = []
    for name, values in hotspot_values.items():
        if not values:
            continue
        overview.append(
            HotspotSummary(
                hotspot=name,
                median_share=statistics.median(values),
                p90_share=_quantile(values, 0.90),
            )
        )

    overview.sort(key=lambda item: item.median_share, reverse=True)
    return overview


def collect_data_warnings(
    discovery_warnings: Iterable[str],
    extraction_warnings: Iterable[str],
    rows: Iterable[FrameRecord],
) -> list[str]:
    warnings = list(discovery_warnings)
    warnings.extend(extraction_warnings)

    for row in rows:
        for item in row.warnings:
            warnings.append(
                f"{row.settings.run_name} frame {row.frame:04d}: {item} ({row.stats_file})"
            )

    # Keep stable order while deduplicating.
    deduped: list[str] = []
    seen: set[str] = set()
    for warning in warnings:
        if warning in seen:
            continue
        seen.add(warning)
        deduped.append(warning)

    return deduped
