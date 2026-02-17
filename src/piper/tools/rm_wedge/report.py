from __future__ import annotations

import csv
import html
import json
from pathlib import Path
from typing import Any

from .models import (
    FrameRecord,
    HotspotSummary,
    ImageAnalysisResult,
    ImageFrameMetrics,
    ImageRunSummary,
    Recommendation,
    RunSummary,
    SpotlightComparison,
)


def _fmt_optional_float(value: float | None, digits: int = 3) -> str:
    if value is None:
        return "—"
    return f"{value:.{digits}f}"


def _fmt_optional_percent(value: float | None) -> str:
    if value is None:
        return "—"
    return f"{value * 100.0:.1f}%"


def _fmt_gib(value: float | None) -> str:
    if value is None:
        return "—"
    return f"{value:.2f} GiB"


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return

    fieldnames = sorted({key for row in rows for key in row})
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def frame_rows_to_csv(rows: list[FrameRecord]) -> list[dict[str, Any]]:
    table: list[dict[str, Any]] = []

    for row in rows:
        metrics = row.metrics
        table.append(
            {
                "group": row.settings.group,
                "resolution_scale": row.settings.resolution_scale,
                "pixel_variance": row.settings.pixel_variance,
                "max_samples": row.settings.max_samples,
                "run_name": row.settings.run_name,
                "run_dir": row.run_dir.as_posix(),
                "frame": row.frame,
                "stats_file": row.stats_file.as_posix(),
                "attempt_index": row.attempt_index,
                "attempt_count": row.attempt_count,
                "mainloop_s": metrics.mainloop_s,
                "ttfp_s": metrics.ttfp_s,
                "ttfr_s": metrics.ttfr_s,
                "ttfi_s": metrics.ttfi_s,
                "buckets_rendered": metrics.buckets_rendered,
                "iterations_completed": metrics.iterations_completed,
                "rays_traced": metrics.rays_traced,
                "hits": metrics.hits,
                "tests": metrics.tests,
                "bundles": metrics.bundles,
                "shade_hit_bxdf_s": metrics.shade_hit_bxdf_s,
                "shade_hit_opacity_s": metrics.shade_hit_opacity_s,
                "shade_get_shade_groups_s": metrics.shade_get_shade_groups_s,
                "shade_build_displacement_s": metrics.shade_build_displacement_s,
                "trace_shadows_s": metrics.trace_shadows_s,
                "create_geom_proto_s": metrics.create_geom_proto_s,
                "create_geom_proto_count": metrics.create_geom_proto_count,
                "proc_mem_committed_b": metrics.proc_mem_committed_b,
                "proc_mem_peak_committed_b": metrics.proc_mem_peak_committed_b,
                "mem_tracking_current_b": metrics.mem_tracking_current_b,
                "mem_tracking_peak_b": metrics.mem_tracking_peak_b,
                "mem_dedupe_saved_b": metrics.mem_dedupe_saved_b,
                "proc_user_s": metrics.proc_user_s,
                "proc_sys_s": metrics.proc_sys_s,
                "cpu_pct_now": metrics.cpu_pct_now,
                "cpu_pct_avg": metrics.cpu_pct_avg,
                "num_render_threads": metrics.num_render_threads,
                "integrator": metrics.integrator,
                "warnings": ",".join(row.warnings),
            }
        )

    return table


def run_rows_to_csv(rows: list[RunSummary]) -> list[dict[str, Any]]:
    table: list[dict[str, Any]] = []

    for row in rows:
        table.append(
            {
                "group": row.settings.group,
                "resolution_scale": row.settings.resolution_scale,
                "pixel_variance": row.settings.pixel_variance,
                "max_samples": row.settings.max_samples,
                "run_name": row.settings.run_name,
                "run_dir": row.run_dir.as_posix(),
                "frame_count": row.frame_count,
                "usable_frames": row.usable_frames,
                "median_mainloop_s": row.median_mainloop_s,
                "median_ttfp_s": row.median_ttfp_s,
                "median_peak_mem_gib": row.median_peak_mem_gib,
                "median_cpu_avg_pct": row.median_cpu_avg_pct,
                "median_rays_per_s": row.median_rays_per_s,
                "median_share_trace_shadows": row.median_share_trace_shadows,
                "median_share_shade_bxdf": row.median_share_shade_bxdf,
                "median_share_shade_opacity": row.median_share_shade_opacity,
                "median_share_shade_groups": row.median_share_shade_groups,
                "median_share_shade_displacement": row.median_share_shade_displacement,
                "median_share_geom_proto": row.median_share_geom_proto,
                "quality_proxy": row.quality_proxy,
                "time_norm": row.time_norm,
                "memory_norm": row.memory_norm,
                "objective_dailies": row.objective_dailies,
                "objective_final": row.objective_final,
                "warnings": ",".join(row.warnings),
            }
        )

    return table


def image_frame_rows_to_csv(rows: list[ImageFrameMetrics]) -> list[dict[str, Any]]:
    table: list[dict[str, Any]] = []

    for row in rows:
        table.append(
            {
                "group": row.settings.group,
                "resolution_scale": row.settings.resolution_scale,
                "pixel_variance": row.settings.pixel_variance,
                "max_samples": row.settings.max_samples,
                "run_name": row.settings.run_name,
                "run_dir": row.run_dir.as_posix(),
                "frame": row.frame,
                "candidate_image": row.candidate_image.as_posix(),
                "ground_truth_image": row.ground_truth_image.as_posix(),
                "resized_to_ground_truth": int(row.resized_to_ground_truth),
                "mean_error": row.mean_error,
                "rms_error": row.rms_error,
                "peak_snr": row.peak_snr,
                "max_error": row.max_error,
            }
        )

    return table


def image_run_rows_to_csv(rows: list[ImageRunSummary]) -> list[dict[str, Any]]:
    table: list[dict[str, Any]] = []

    for row in rows:
        table.append(
            {
                "group": row.settings.group,
                "resolution_scale": row.settings.resolution_scale,
                "pixel_variance": row.settings.pixel_variance,
                "max_samples": row.settings.max_samples,
                "run_name": row.settings.run_name,
                "run_dir": row.run_dir.as_posix(),
                "compared_frames": row.compared_frames,
                "median_mean_error": row.median_mean_error,
                "median_rms_error": row.median_rms_error,
                "median_peak_snr": row.median_peak_snr,
                "max_error": row.max_error,
            }
        )

    return table


def recommendations_to_json(
    recommendations: list[Recommendation],
) -> list[dict[str, Any]]:
    data: list[dict[str, Any]] = []

    for recommendation in recommendations:
        data.append(
            {
                "group": recommendation.group,
                "objective_name": recommendation.objective_name,
                "pareto_count": recommendation.pareto_count,
                "selection_notes": list(recommendation.selection_notes),
                "selected": {
                    "run_name": recommendation.selected.settings.run_name,
                    "run_dir": recommendation.selected.run_dir.as_posix(),
                    "label": recommendation.selected.settings.label(),
                    "median_mainloop_s": recommendation.selected.median_mainloop_s,
                    "median_peak_mem_gib": recommendation.selected.median_peak_mem_gib,
                    "quality_proxy": recommendation.selected.quality_proxy,
                    "objective_dailies": recommendation.selected.objective_dailies,
                    "objective_final": recommendation.selected.objective_final,
                },
                "alternatives": [
                    {
                        "run_name": alternative.settings.run_name,
                        "run_dir": alternative.run_dir.as_posix(),
                        "label": alternative.settings.label(),
                        "median_mainloop_s": alternative.median_mainloop_s,
                        "median_peak_mem_gib": alternative.median_peak_mem_gib,
                        "quality_proxy": alternative.quality_proxy,
                        "objective_dailies": alternative.objective_dailies,
                        "objective_final": alternative.objective_final,
                    }
                    for alternative in recommendation.alternatives
                ],
            }
        )

    return data


def write_outputs(
    out_dir: Path,
    frames: list[FrameRecord],
    runs: list[RunSummary],
    recommendations: list[Recommendation],
    hotspot_overview: list[HotspotSummary],
    warnings: list[str],
    image_result: ImageAnalysisResult | None,
) -> None:
    data_dir = out_dir / "data"
    _write_csv(data_dir / "frames.csv", frame_rows_to_csv(frames))
    _write_csv(data_dir / "runs.csv", run_rows_to_csv(runs))
    _write_json(
        data_dir / "recommendations.json", recommendations_to_json(recommendations)
    )

    if image_result is not None:
        _write_csv(
            data_dir / "image_frames.csv",
            image_frame_rows_to_csv(list(image_result.frame_metrics)),
        )
        _write_csv(
            data_dir / "image_runs.csv",
            image_run_rows_to_csv(list(image_result.run_summaries)),
        )

    _write_json(data_dir / "warnings.json", warnings)

    write_html_report(
        out_dir / "report.html",
        frames=frames,
        runs=runs,
        recommendations=recommendations,
        hotspot_overview=hotspot_overview,
        image_result=image_result,
        warnings=warnings,
    )


def _recommendation_card_html(recommendation: Recommendation) -> str:
    selected = recommendation.selected

    alternatives = "".join(
        (
            "<li>"
            f"{html.escape(item.settings.label())} "
            f"(time={_fmt_optional_float(item.median_mainloop_s, 2)} s, "
            f"mem={_fmt_gib(item.median_peak_mem_gib)}, "
            f"quality_proxy={item.quality_proxy:.3f})"
            "</li>"
        )
        for item in recommendation.alternatives
    )

    notes = "".join(
        f"<li>{html.escape(note)}</li>" for note in recommendation.selection_notes
    )

    return (
        '<section class="card">'
        f"<h3>{html.escape(recommendation.group)}</h3>"
        f"<p><strong>Selected:</strong> {html.escape(selected.settings.label())}</p>"
        f"<p><strong>Median mainloop:</strong> {_fmt_optional_float(selected.median_mainloop_s, 2)} s</p>"
        f"<p><strong>Median peak memory:</strong> {_fmt_gib(selected.median_peak_mem_gib)}</p>"
        f"<p><strong>Quality proxy:</strong> {selected.quality_proxy:.3f}</p>"
        f"<p><strong>Pareto candidates:</strong> {recommendation.pareto_count}</p>"
        f"<p><strong>Objective:</strong> {html.escape(recommendation.objective_name)}</p>"
        "<p><strong>Selection notes:</strong></p>"
        f"<ul>{notes}</ul>"
        "<p><strong>Alternatives:</strong></p>"
        f"<ul>{alternatives or '<li>None</li>'}</ul>"
        "</section>"
    )


def _table_html(headers: list[str], rows: list[list[str]]) -> str:
    header_html = "".join(f"<th>{html.escape(header)}</th>" for header in headers)
    body_html = "".join(
        "<tr>" + "".join(f"<td>{cell}</td>" for cell in row) + "</tr>" for row in rows
    )
    return f"<table><thead><tr>{header_html}</tr></thead><tbody>{body_html}</tbody></table>"


def _image_quality_table(
    image_runs: list[ImageRunSummary],
    run_rows: list[RunSummary],
) -> str:
    run_time_map: dict[tuple[str, float, float, int, str], float | None] = {}
    for run in run_rows:
        run_time_map[
            (
                run.settings.group,
                run.settings.resolution_scale,
                run.settings.pixel_variance,
                run.settings.max_samples,
                run.run_dir.as_posix(),
            )
        ] = run.median_mainloop_s

    ordered = sorted(
        [row for row in image_runs if row.median_rms_error is not None],
        key=lambda row: (
            row.median_rms_error if row.median_rms_error is not None else float("inf")
        ),
    )

    rows_html: list[list[str]] = []
    for row in ordered:
        run_time = run_time_map.get(
            (
                row.settings.group,
                row.settings.resolution_scale,
                row.settings.pixel_variance,
                row.settings.max_samples,
                row.run_dir.as_posix(),
            )
        )

        rows_html.append(
            [
                html.escape(row.settings.group),
                html.escape(row.settings.run_name),
                html.escape(str(row.compared_frames)),
                html.escape(_fmt_optional_float(run_time, 2)),
                html.escape(_fmt_optional_float(row.median_mean_error, 5)),
                html.escape(_fmt_optional_float(row.median_rms_error, 5)),
                html.escape(_fmt_optional_float(row.median_peak_snr, 3)),
                html.escape(_fmt_optional_float(row.max_error, 5)),
            ]
        )

    if not rows_html:
        return "<p>No image comparisons available.</p>"

    return _table_html(
        headers=[
            "Group",
            "Run",
            "Compared frames",
            "Median mainloop (s)",
            "Median mean error",
            "Median RMS error",
            "Median Peak SNR",
            "Max error",
        ],
        rows=rows_html,
    )


def _spotlights_html(spotlights: list[SpotlightComparison]) -> str:
    if not spotlights:
        return "<p>No spotlight comparisons available.</p>"

    cards = []
    for item in spotlights:
        cards.append(
            "".join(
                [
                    '<section class="card">',
                    (
                        f'<h3 class="spotlight-title">'
                        f"{html.escape(item.settings.label())} "
                        f"(frame {item.frame:04d})"
                        "</h3>"
                    ),
                    '<p class="muted">Worst compared frame for this run by RMS error.</p>',
                    (
                        '<p class="muted spotlight-run">'
                        f"<code>{html.escape(item.settings.run_name)}</code>"
                        "</p>"
                    ),
                    "<ul>",
                    f"<li>RMS error: {item.rms_error:.5f}</li>",
                    f"<li>Mean error: {item.mean_error:.5f}</li>",
                    f"<li>Peak SNR: {item.peak_snr:.3f}</li>",
                    f"<li>Max error: {item.max_error:.5f}</li>",
                    "</ul>",
                    '<div class="triptych">',
                    "<figure><figcaption>Ground truth</figcaption>"
                    f'<img src="{html.escape(item.gt_png)}" loading="lazy"></figure>',
                    "<figure><figcaption>Candidate</figcaption>"
                    f'<img src="{html.escape(item.candidate_png)}" loading="lazy"></figure>',
                    "<figure><figcaption>|Diff| x8 (clamped)</figcaption>"
                    f'<img src="{html.escape(item.diff_png)}" loading="lazy"></figure>',
                    "</div>",
                    "</section>",
                ]
            )
        )

    return '<div class="spotlight-grid">' + "".join(cards) + "</div>"


def _image_charts_html(image_result: ImageAnalysisResult) -> str:
    chart_nodes: list[str] = []

    scatter = image_result.charts.scatter_time_vs_rms
    if scatter is not None:
        chart_nodes.append(
            '<section class="panel">'
            "<h3>Quality vs Time Scatter</h3>"
            '<p class="muted">Each point is one run. Lower and left is better.</p>'
            f'<img class="chart" src="{html.escape(scatter)}" loading="lazy">'
            "</section>"
        )

    for title, chart_path in image_result.charts.heatmaps:
        chart_nodes.append(
            '<section class="panel">'
            f"<h3>{html.escape(title)}</h3>"
            '<p class="muted">Lower values are better quality matches to ground truth.</p>'
            f'<img class="chart" src="{html.escape(chart_path)}" loading="lazy">'
            "</section>"
        )

    if not chart_nodes:
        return "<p>No image charts available.</p>"

    return "".join(chart_nodes)


def write_html_report(
    out_html: Path,
    *,
    frames: list[FrameRecord],
    runs: list[RunSummary],
    recommendations: list[Recommendation],
    hotspot_overview: list[HotspotSummary],
    image_result: ImageAnalysisResult | None,
    warnings: list[str],
) -> None:
    by_group: dict[str, list[RunSummary]] = {}
    for run in runs:
        by_group.setdefault(run.settings.group, []).append(run)

    for group_runs in by_group.values():
        group_runs.sort(
            key=lambda run: (
                run.median_mainloop_s
                if run.median_mainloop_s is not None
                else float("inf")
            )
        )

    recommendation_html = "".join(
        _recommendation_card_html(item) for item in recommendations
    )

    hotspot_rows = [
        [
            html.escape(item.hotspot),
            html.escape(_fmt_optional_percent(item.median_share)),
            html.escape(_fmt_optional_percent(item.p90_share)),
        ]
        for item in hotspot_overview
    ]

    group_tables: list[str] = []
    for group_name, group_runs in sorted(by_group.items()):
        rows_html = [
            [
                html.escape(run.settings.run_name),
                html.escape(f"{run.settings.resolution_scale:g}"),
                html.escape(f"{run.settings.pixel_variance:g}"),
                html.escape(str(run.settings.max_samples)),
                html.escape(str(run.usable_frames)),
                html.escape(_fmt_optional_float(run.median_mainloop_s, 2)),
                html.escape(_fmt_gib(run.median_peak_mem_gib)),
                html.escape(_fmt_optional_float(run.median_cpu_avg_pct, 1)),
                html.escape(f"{run.quality_proxy:.3f}"),
                html.escape(f"{run.objective_dailies:.3f}"),
                html.escape(f"{run.objective_final:.3f}"),
            ]
            for run in group_runs
        ]

        group_tables.append(
            "<section>"
            f"<h3>{html.escape(group_name)}</h3>"
            + _table_html(
                headers=[
                    "Run",
                    "Res scale",
                    "Pixel variance",
                    "Max samples",
                    "Usable frames",
                    "Median mainloop (s)",
                    "Median peak memory",
                    "Median CPU avg (%)",
                    "Quality proxy",
                    "Objective (dailies)",
                    "Objective (final)",
                ],
                rows=rows_html,
            )
            + "</section>"
        )

    warnings_html = "".join(f"<li>{html.escape(item)}</li>" for item in warnings[:200])

    output_files = [
        "data/frames.csv",
        "data/runs.csv",
        "data/recommendations.json",
        "data/warnings.json",
    ]
    if image_result is not None:
        output_files.extend(["data/image_frames.csv", "data/image_runs.csv"])

    image_summary_lines = ""
    image_sections = ""
    if image_result is not None:
        image_summary_lines = "".join(
            [
                f"<li>Image frame comparisons: {len(image_result.frame_metrics)}</li>",
                f"<li>Image run summaries: {len(image_result.run_summaries)}</li>",
                f"<li>Visual spotlights: {len(image_result.spotlights)}</li>",
            ]
        )

        image_sections = "".join(
            [
                '<section class="panel">',
                "<h2>Image Quality Leaderboard</h2>",
                '<p class="muted">Comparisons are against the configured ground-truth group using EXR RGB channels.</p>',
                _image_quality_table(list(image_result.run_summaries), runs),
                "</section>",
                '<section class="panel">',
                "<h2>Image Charts</h2>",
                _image_charts_html(image_result),
                "</section>",
                '<section class="panel">',
                "<h2>Visual Spotlights</h2>",
                '<p class="muted">Representative run comparisons with candidate, ground truth, and scaled absolute difference.</p>',
                _spotlights_html(list(image_result.spotlights)),
                "</section>",
            ]
        )

    css = """
body { font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial; margin: 24px; color: #111; background: #f8f9fb; }
main { max-width: 1500px; margin: 0 auto; }
h1, h2, h3 { margin: 0.6rem 0 0.4rem; }
small, .muted { color: #5f6773; }
.card-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(340px, 1fr)); gap: 14px; }
.card { border: 1px solid #dbe1ea; border-radius: 12px; background: #fff; padding: 14px; }
.panel { border: 1px solid #dbe1ea; border-radius: 12px; background: #fff; padding: 14px; margin-top: 16px; }
table { width: 100%; border-collapse: collapse; margin-top: 0.5rem; background: #fff; }
th, td { border-bottom: 1px solid #e8edf5; padding: 8px; text-align: left; font-size: 13px; }
th { background: #f1f5fb; position: sticky; top: 0; }
ul { margin-top: 0.3rem; }
code { background: #eef2f7; padding: 0.1rem 0.3rem; border-radius: 4px; }
.chart { width: 100%; height: auto; border: 1px solid #dbe1ea; border-radius: 8px; background: #fff; }
.spotlight-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(860px, 1fr)); gap: 14px; }
.spotlight-title { overflow-wrap: anywhere; margin-bottom: 0.2rem; }
.spotlight-run { overflow-wrap: anywhere; }
.spotlight-run code { white-space: normal; overflow-wrap: anywhere; }
.triptych { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 10px; }
figure { margin: 0; }
figcaption { font-size: 12px; color: #5f6773; margin-bottom: 4px; }
.triptych img { width: 100%; height: auto; border-radius: 8px; border: 1px solid #dbe1ea; background: #fff; }
@media (max-width: 1100px) {
  .spotlight-grid { grid-template-columns: 1fr; }
}
@media (max-width: 900px) {
  .triptych { grid-template-columns: 1fr; }
}
"""

    html_doc = f"""<!doctype html>
<html>
<head>
<meta charset=\"utf-8\">
<title>Render Wedge Stats Report</title>
<style>{css}</style>
</head>
<body>
<main>
  <h1>Render Wedge Stats Report</h1>
  <p class=\"muted\">Core RenderMan stats, recommendation heuristics, and image-based comparisons against ground truth.</p>

  <section class=\"panel\">
    <h2>Dataset Summary</h2>
    <ul>
      <li>Total runs: {len(runs)}</li>
      <li>Total frames analyzed (stats): {len(frames)}</li>
      <li>Groups discovered: {", ".join(html.escape(group) for group in sorted(by_group))}</li>
      {image_summary_lines}
      <li>Output files: {", ".join(f"<code>{html.escape(path)}</code>" for path in output_files)}</li>
    </ul>
  </section>

  <h2>Recommendations</h2>
  <div class=\"card-grid\">{recommendation_html or "<p>No recommendations available.</p>"}</div>

  {image_sections}

  <section class=\"panel\">
    <h2>Global Hotspot Share (relative to mainloop)</h2>
    <p class=\"muted\">Shares can exceed 100% when timers overlap or represent aggregated thread CPU time.</p>
    {_table_html(["Hotspot", "Median share", "P90 share"], hotspot_rows) if hotspot_rows else "<p>No hotspot data available.</p>"}
  </section>

  <section class=\"panel\">
    <h2>Run Leaderboards</h2>
    {"".join(group_tables)}
  </section>

  <section class=\"panel\">
    <h2>Warnings</h2>
    <p class=\"muted\">Showing up to first 200 warnings.</p>
    <ul>{warnings_html or "<li>None</li>"}</ul>
  </section>
</main>
</body>
</html>"""

    out_html.parent.mkdir(parents=True, exist_ok=True)
    out_html.write_text(html_doc, encoding="utf-8")
