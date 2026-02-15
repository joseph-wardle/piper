from __future__ import annotations

import csv
import html
import json
from pathlib import Path
from typing import Any

from .models import FrameRecord, HotspotSummary, Recommendation, RunSummary


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
) -> None:
    data_dir = out_dir / "data"
    _write_csv(data_dir / "frames.csv", frame_rows_to_csv(frames))
    _write_csv(data_dir / "runs.csv", run_rows_to_csv(runs))
    _write_json(
        data_dir / "recommendations.json", recommendations_to_json(recommendations)
    )
    _write_json(data_dir / "warnings.json", warnings)

    write_html_report(
        out_dir / "report.html",
        frames=frames,
        runs=runs,
        recommendations=recommendations,
        hotspot_overview=hotspot_overview,
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


def write_html_report(
    out_html: Path,
    *,
    frames: list[FrameRecord],
    runs: list[RunSummary],
    recommendations: list[Recommendation],
    hotspot_overview: list[HotspotSummary],
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

    css = """
body { font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial; margin: 24px; color: #111; background: #f8f9fb; }
main { max-width: 1400px; margin: 0 auto; }
h1, h2, h3 { margin: 0.6rem 0 0.4rem; }
small, .muted { color: #5f6773; }
.card-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 14px; }
.card { border: 1px solid #dbe1ea; border-radius: 12px; background: #fff; padding: 14px; }
.panel { border: 1px solid #dbe1ea; border-radius: 12px; background: #fff; padding: 14px; margin-top: 16px; }
table { width: 100%; border-collapse: collapse; margin-top: 0.5rem; background: #fff; }
th, td { border-bottom: 1px solid #e8edf5; padding: 8px; text-align: left; font-size: 13px; }
th { background: #f1f5fb; position: sticky; top: 0; }
ul { margin-top: 0.3rem; }
code { background: #eef2f7; padding: 0.1rem 0.3rem; border-radius: 4px; }
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
  <p class=\"muted\">Pass 1: core stats, recommendations, and operational diagnostics (no image-diff quality metrics).</p>

  <section class=\"panel\">
    <h2>Dataset Summary</h2>
    <ul>
      <li>Total runs: {len(runs)}</li>
      <li>Total frames analyzed: {len(frames)}</li>
      <li>Groups discovered: {", ".join(html.escape(group) for group in sorted(by_group))}</li>
      <li>Output files: <code>data/frames.csv</code>, <code>data/runs.csv</code>, <code>data/recommendations.json</code>, <code>data/warnings.json</code></li>
    </ul>
  </section>

  <h2>Recommendations</h2>
  <div class=\"card-grid\">{recommendation_html or "<p>No recommendations available.</p>"}</div>

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
