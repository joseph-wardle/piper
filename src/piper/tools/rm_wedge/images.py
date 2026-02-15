from __future__ import annotations

import html
import math
import re
import shutil
import statistics
import subprocess
from collections import defaultdict
from collections.abc import Callable, Iterable, Sequence
from pathlib import Path

from .discovery import parse_frame_number
from .models import (
    DiscoveredRun,
    FrameRecord,
    ImageAnalysisResult,
    ImageCharts,
    ImageFrameMetrics,
    ImageRunSummary,
    Recommendation,
    RunSummary,
    SpotlightComparison,
)

RunKey = tuple[str, float, float, int, str]
CommandRunner = Callable[[Sequence[str]], tuple[int, str]]

_MEAN_RE = re.compile(r"Mean error = (?P<value>[-+0-9.eE]+)")
_RMS_RE = re.compile(r"RMS error = (?P<value>[-+0-9.eE]+)")
_PSNR_RE = re.compile(r"Peak SNR = (?P<value>[-+0-9.eE]+|inf|INF)")
_MAX_RE = re.compile(r"Max error\s*=\s*(?P<value>[-+0-9.eE]+)")
_RESOLUTION_RE = re.compile(r"^\s*(?P<width>\d+)\s*x\s*(?P<height>\d+),")


def _key_for_run(
    run_dir: Path, settings_group: str, rs: float, pv: float, ms: int
) -> RunKey:
    return (settings_group, rs, pv, ms, run_dir.as_posix())


def _key_for_summary(summary: RunSummary) -> RunKey:
    settings = summary.settings
    return _key_for_run(
        summary.run_dir,
        settings.group,
        settings.resolution_scale,
        settings.pixel_variance,
        settings.max_samples,
    )


def _key_for_frame_metric(metric: ImageFrameMetrics) -> RunKey:
    settings = metric.settings
    return _key_for_run(
        metric.run_dir,
        settings.group,
        settings.resolution_scale,
        settings.pixel_variance,
        settings.max_samples,
    )


def _default_runner(command: Sequence[str]) -> tuple[int, str]:
    result = subprocess.run(
        list(command),
        check=False,
        capture_output=True,
        text=True,
    )
    output = f"{result.stdout}{result.stderr}"
    return result.returncode, output


def _parse_diff_output(output: str) -> tuple[float, float, float, float] | None:
    mean_match = _MEAN_RE.search(output)
    rms_match = _RMS_RE.search(output)
    psnr_match = _PSNR_RE.search(output)
    max_match = _MAX_RE.search(output)

    if not (mean_match and rms_match and psnr_match and max_match):
        return None

    mean_error = float(mean_match.group("value"))
    rms_error = float(rms_match.group("value"))
    peak_snr = float(psnr_match.group("value"))
    max_error = float(max_match.group("value"))
    return (mean_error, rms_error, peak_snr, max_error)


def _parse_resolution(printinfo_output: str) -> tuple[int, int] | None:
    first_line = printinfo_output.splitlines()[0] if printinfo_output else ""
    match = _RESOLUTION_RE.search(first_line)
    if match is None:
        return None
    return (int(match.group("width")), int(match.group("height")))


def _read_image_resolution(
    image_path: Path,
    *,
    oiiotool_bin: str,
    runner: CommandRunner,
    cache: dict[Path, tuple[int, int]],
) -> tuple[int, int] | None:
    cached = cache.get(image_path)
    if cached is not None:
        return cached

    command = [oiiotool_bin, "-i", str(image_path), "--printinfo"]
    return_code, output = runner(command)
    if return_code != 0:
        return None

    resolution = _parse_resolution(output)
    if resolution is None:
        return None

    cache[image_path] = resolution
    return resolution


def _frame_image_path(run_dir: Path, images_subdir: str, frame: int) -> Path:
    return run_dir / images_subdir / f"{frame:04d}.exr"


def _dedupe_stable(values: Iterable[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _build_ground_truth_frame_map(
    runs: Iterable[DiscoveredRun],
    *,
    images_subdir: str,
    ground_truth_group: str,
) -> dict[int, Path]:
    candidates = [
        run for run in runs if run.settings.group.lower() == ground_truth_group.lower()
    ]

    ordered = sorted(
        candidates,
        key=lambda run: (
            run.settings.pixel_variance,
            -run.settings.max_samples,
            abs(run.settings.resolution_scale - 1.0),
            run.settings.run_name,
        ),
    )

    frame_map: dict[int, Path] = {}
    for run in ordered:
        for stats_file in run.stats_files:
            frame = parse_frame_number(stats_file)
            image_path = _frame_image_path(run.run_dir, images_subdir, frame)
            if not image_path.is_file():
                continue
            frame_map.setdefault(frame, image_path)

    return frame_map


def _compare_frame_images(
    *,
    candidate_image: Path,
    ground_truth_image: Path,
    candidate_resolution: tuple[int, int],
    ground_truth_resolution: tuple[int, int],
    oiiotool_bin: str,
    runner: CommandRunner,
) -> tuple[float, float, float, float] | None:
    command: list[str] = [oiiotool_bin, "-i", str(candidate_image), "--ch", "R,G,B"]

    if candidate_resolution != ground_truth_resolution:
        width, height = ground_truth_resolution
        command.extend(["--resize", f"{width}x{height}"])

    command.extend(
        [
            "-i",
            str(ground_truth_image),
            "--ch",
            "R,G,B",
            "--diff",
        ]
    )

    _return_code, output = runner(command)
    return _parse_diff_output(output)


def summarize_image_runs(
    frame_metrics: Iterable[ImageFrameMetrics],
) -> list[ImageRunSummary]:
    grouped: dict[RunKey, list[ImageFrameMetrics]] = defaultdict(list)
    for metric in frame_metrics:
        grouped[_key_for_frame_metric(metric)].append(metric)

    summaries: list[ImageRunSummary] = []
    for metrics in grouped.values():
        first = metrics[0]
        mean_values = [item.mean_error for item in metrics]
        rms_values = [item.rms_error for item in metrics]
        psnr_values = [
            item.peak_snr for item in metrics if math.isfinite(item.peak_snr)
        ]
        max_values = [item.max_error for item in metrics]

        summaries.append(
            ImageRunSummary(
                settings=first.settings,
                run_dir=first.run_dir,
                compared_frames=len(metrics),
                median_mean_error=statistics.median(mean_values)
                if mean_values
                else None,
                median_rms_error=statistics.median(rms_values) if rms_values else None,
                median_peak_snr=statistics.median(psnr_values) if psnr_values else None,
                max_error=max(max_values) if max_values else None,
            )
        )

    summaries.sort(
        key=lambda item: (
            item.settings.group,
            item.settings.resolution_scale,
            item.settings.pixel_variance,
            item.settings.max_samples,
        )
    )
    return summaries


def _group_color(group: str) -> str:
    palette = [
        "#1f77b4",
        "#ff7f0e",
        "#2ca02c",
        "#d62728",
        "#9467bd",
        "#8c564b",
        "#e377c2",
        "#17becf",
    ]
    return palette[abs(hash(group)) % len(palette)]


def _nice_ticks(min_value: float, max_value: float, count: int) -> list[float]:
    if count <= 1:
        return [min_value]
    if max_value <= min_value:
        return [min_value for _ in range(count)]
    return [min_value + (max_value - min_value) * i / (count - 1) for i in range(count)]


def _escape(text: str) -> str:
    return html.escape(text, quote=True)


def _write_scatter_chart(
    *,
    output_path: Path,
    points: list[tuple[float, float, str, str]],
) -> None:
    width = 900
    height = 560
    left = 90
    right = 30
    top = 50
    bottom = 80

    xs = [point[0] for point in points]
    ys = [point[1] for point in points]

    x_min = min(xs)
    x_max = max(xs)
    y_min = min(ys)
    y_max = max(ys)

    if x_max <= x_min:
        x_max = x_min + 1.0
    if y_max <= y_min:
        y_max = y_min + 1.0

    x_pad = (x_max - x_min) * 0.08
    y_pad = (y_max - y_min) * 0.08
    x_min -= x_pad
    x_max += x_pad
    y_min -= y_pad
    y_max += y_pad

    def map_x(value: float) -> float:
        usable = width - left - right
        return left + (value - x_min) / (x_max - x_min) * usable

    def map_y(value: float) -> float:
        usable = height - top - bottom
        return top + (y_max - value) / (y_max - y_min) * usable

    x_ticks = _nice_ticks(x_min, x_max, 6)
    y_ticks = _nice_ticks(y_min, y_max, 6)

    tick_lines: list[str] = []
    tick_labels: list[str] = []

    for tick in x_ticks:
        x = map_x(tick)
        tick_lines.append(
            f'<line x1="{x:.2f}" y1="{top}" x2="{x:.2f}" y2="{height - bottom}" stroke="#e5e7eb" />'
        )
        tick_labels.append(
            f'<text x="{x:.2f}" y="{height - bottom + 22}" text-anchor="middle" '
            f'font-size="12" fill="#4b5563">{tick:.2f}</text>'
        )

    for tick in y_ticks:
        y = map_y(tick)
        tick_lines.append(
            f'<line x1="{left}" y1="{y:.2f}" x2="{width - right}" y2="{y:.2f}" stroke="#e5e7eb" />'
        )
        tick_labels.append(
            f'<text x="{left - 10}" y="{y + 4:.2f}" text-anchor="end" '
            f'font-size="12" fill="#4b5563">{tick:.4f}</text>'
        )

    point_nodes: list[str] = []
    for x_value, y_value, label, group in points:
        x = map_x(x_value)
        y = map_y(y_value)
        color = _group_color(group)
        point_nodes.append(
            f'<circle cx="{x:.2f}" cy="{y:.2f}" r="5" fill="{color}" '
            f'stroke="#111827" stroke-width="0.6">'
            f"<title>{_escape(label)}</title>"
            "</circle>"
        )

    parts = [
        (
            f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" '
            f'height="{height}" viewBox="0 0 900 560">'
        ),
        '<rect width="100%" height="100%" fill="#ffffff" />',
        (
            f'<text x="{width / 2:.1f}" y="26" text-anchor="middle" '
            'font-size="18" font-weight="600" fill="#111827">'
            "Quality vs Render Time (lower RMS is better)"
            "</text>"
        ),
        "".join(tick_lines),
        f'<line x1="{left}" y1="{top}" x2="{left}" y2="{height - bottom}" stroke="#111827" />',
        (
            f'<line x1="{left}" y1="{height - bottom}" '
            f'x2="{width - right}" y2="{height - bottom}" stroke="#111827" />'
        ),
        "".join(tick_labels),
        "".join(point_nodes),
        (
            f'<text x="{width / 2:.1f}" y="{height - 24}" text-anchor="middle" '
            'font-size="13" fill="#374151">Median mainloop time (s)</text>'
        ),
        (
            f'<text x="20" y="{height / 2:.1f}" text-anchor="middle" '
            'font-size="13" fill="#374151" transform="rotate(-90 20 '
            f'{height / 2:.1f})">Median RMS error (vs ground truth)</text>'
        ),
        "</svg>",
    ]
    svg = "".join(parts)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(svg, encoding="utf-8")


def _interpolate_color(low: int, high: int, t: float) -> int:
    return int(round(low + (high - low) * t))


def _heat_color(value: float, lo: float, hi: float) -> str:
    if hi <= lo:
        return "#6ee7b7"
    t = (value - lo) / (hi - lo)
    t = max(0.0, min(1.0, t))

    low = (16, 185, 129)  # teal-ish for lower error
    high = (220, 38, 38)  # red for higher error
    rgb = (
        _interpolate_color(low[0], high[0], t),
        _interpolate_color(low[1], high[1], t),
        _interpolate_color(low[2], high[2], t),
    )
    return f"#{rgb[0]:02x}{rgb[1]:02x}{rgb[2]:02x}"


def _write_heatmap_chart(
    *,
    output_path: Path,
    title: str,
    x_labels: list[str],
    y_labels: list[str],
    values: list[list[float | None]],
) -> None:
    cell_w = 96
    cell_h = 38
    left = 220
    top = 90
    right = 40
    bottom = 80
    width = left + right + len(x_labels) * cell_w
    height = top + bottom + len(y_labels) * cell_h

    numeric = [item for row in values for item in row if item is not None]
    lo = min(numeric) if numeric else 0.0
    hi = max(numeric) if numeric else 1.0

    nodes: list[str] = []
    for row_index, y_label in enumerate(y_labels):
        y = top + row_index * cell_h
        nodes.append(
            f'<text x="{left - 10}" y="{y + cell_h * 0.67:.2f}" text-anchor="end" '
            f'font-size="12" fill="#374151">{_escape(y_label)}</text>'
        )
        for col_index, _x_label in enumerate(x_labels):
            x = left + col_index * cell_w
            value = values[row_index][col_index]
            if value is None:
                color = "#f3f4f6"
                text_value = "—"
            else:
                color = _heat_color(value, lo, hi)
                text_value = f"{value:.4f}"
            nodes.append(
                f'<rect x="{x}" y="{y}" width="{cell_w}" height="{cell_h}" '
                f'fill="{color}" stroke="#e5e7eb" />'
            )
            nodes.append(
                f'<text x="{x + cell_w / 2:.2f}" y="{y + cell_h * 0.64:.2f}" '
                f'text-anchor="middle" font-size="11" fill="#111827">{text_value}</text>'
            )

    for col_index, x_label in enumerate(x_labels):
        x = left + col_index * cell_w + cell_w / 2
        nodes.append(
            f'<text x="{x:.2f}" y="{top - 12}" text-anchor="middle" '
            f'font-size="12" fill="#374151">{_escape(x_label)}</text>'
        )

    parts = [
        (
            f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" '
            f'height="{height}" viewBox="0 0 {width} {height}">'
        ),
        '<rect width="100%" height="100%" fill="#ffffff" />',
        (
            f'<text x="{width / 2:.2f}" y="30" text-anchor="middle" '
            'font-size="18" font-weight="600" fill="#111827">'
            f"{_escape(title)}</text>"
        ),
        (
            f'<text x="{left - 140}" y="{top - 38}" text-anchor="start" '
            'font-size="12" fill="#374151">Rows: pixel variance</text>'
        ),
        (
            f'<text x="{left}" y="{top - 38}" text-anchor="start" '
            'font-size="12" fill="#374151">Columns: max samples</text>'
        ),
        "".join(nodes),
        "</svg>",
    ]
    svg = "".join(parts)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(svg, encoding="utf-8")


def _slugify(text: str) -> str:
    lowered = text.lower()
    slug = re.sub(r"[^a-z0-9._-]+", "_", lowered)
    return slug.strip("_") or "item"


def _write_spotlight_images(
    *,
    out_dir: Path,
    metric: ImageFrameMetrics,
    ground_truth_resolution: tuple[int, int],
    oiiotool_bin: str,
    runner: CommandRunner,
) -> tuple[str, str, str] | None:
    base_name = _slugify(f"{metric.settings.run_name}_{metric.frame:04d}")
    spotlight_dir = out_dir / "assets" / "spotlight"
    spotlight_dir.mkdir(parents=True, exist_ok=True)

    gt_png = spotlight_dir / f"{base_name}_gt.png"
    candidate_png = spotlight_dir / f"{base_name}_candidate.png"
    diff_png = spotlight_dir / f"{base_name}_diff.png"

    width, height = ground_truth_resolution
    resize_args: list[str] = []
    if metric.resized_to_ground_truth:
        resize_args = ["--resize", f"{width}x{height}"]

    commands = [
        [
            oiiotool_bin,
            "-i",
            str(metric.ground_truth_image),
            "--ch",
            "R,G,B",
            "--clamp:min=0:max=1",
            "-d",
            "uint8",
            "-o",
            str(gt_png),
        ],
        [
            oiiotool_bin,
            "-i",
            str(metric.candidate_image),
            "--ch",
            "R,G,B",
            *resize_args,
            "--clamp:min=0:max=1",
            "-d",
            "uint8",
            "-o",
            str(candidate_png),
        ],
        [
            oiiotool_bin,
            "-i",
            str(metric.candidate_image),
            "--ch",
            "R,G,B",
            *resize_args,
            "-i",
            str(metric.ground_truth_image),
            "--ch",
            "R,G,B",
            "--sub",
            "--abs",
            "--mulc",
            "8.0",
            "--clamp:min=0:max=1",
            "-d",
            "uint8",
            "-o",
            str(diff_png),
        ],
    ]

    for command in commands:
        return_code, _output = runner(command)
        if return_code != 0:
            return None

    return (
        gt_png.relative_to(out_dir).as_posix(),
        candidate_png.relative_to(out_dir).as_posix(),
        diff_png.relative_to(out_dir).as_posix(),
    )


def _build_charts(
    *,
    out_dir: Path,
    image_runs: list[ImageRunSummary],
    run_rows: list[RunSummary],
) -> ImageCharts:
    run_time_map = {_key_for_summary(run): run.median_mainloop_s for run in run_rows}

    scatter_points: list[tuple[float, float, str, str]] = []
    for run in image_runs:
        rms = run.median_rms_error
        run_time = run_time_map.get(
            _key_for_run(
                run.run_dir,
                run.settings.group,
                run.settings.resolution_scale,
                run.settings.pixel_variance,
                run.settings.max_samples,
            )
        )
        if rms is None or run_time is None:
            continue
        label = f"{run.settings.run_name} | t={run_time:.2f}s | rms={rms:.4f}"
        scatter_points.append((run_time, rms, label, run.settings.group))

    scatter_relpath: str | None = None
    if len(scatter_points) >= 2:
        scatter_path = out_dir / "charts" / "quality_vs_time.svg"
        _write_scatter_chart(output_path=scatter_path, points=scatter_points)
        scatter_relpath = scatter_path.relative_to(out_dir).as_posix()

    grouped: dict[tuple[str, float], list[ImageRunSummary]] = defaultdict(list)
    for item in image_runs:
        grouped[(item.settings.group, item.settings.resolution_scale)].append(item)

    heatmaps: list[tuple[str, str]] = []
    for (group, resolution_scale), group_items in sorted(grouped.items()):
        pixel_variances = sorted({item.settings.pixel_variance for item in group_items})
        max_samples = sorted({item.settings.max_samples for item in group_items})

        if len(pixel_variances) <= 1 and len(max_samples) <= 1:
            continue

        grid_map: dict[tuple[float, int], float] = {}
        for item in group_items:
            if item.median_rms_error is None:
                continue
            key = (item.settings.pixel_variance, item.settings.max_samples)
            prior = grid_map.get(key)
            if prior is None or item.median_rms_error < prior:
                grid_map[key] = item.median_rms_error

        values: list[list[float | None]] = []
        for pv in pixel_variances:
            row: list[float | None] = []
            for ms in max_samples:
                row.append(grid_map.get((pv, ms)))
            values.append(row)

        title = f"{group}: median RMS heatmap (resolution_scale={resolution_scale:g})"
        file_name = _slugify(
            f"heatmap_{group}_rs_{resolution_scale:g}".replace(".", "p")
        )
        heatmap_path = out_dir / "charts" / f"{file_name}.svg"
        _write_heatmap_chart(
            output_path=heatmap_path,
            title=title,
            x_labels=[str(sample) for sample in max_samples],
            y_labels=[f"{value:g}" for value in pixel_variances],
            values=values,
        )
        heatmaps.append((title, heatmap_path.relative_to(out_dir).as_posix()))

    return ImageCharts(
        scatter_time_vs_rms=scatter_relpath,
        heatmaps=tuple(heatmaps),
    )


def _build_spotlights(
    *,
    out_dir: Path,
    frame_metrics: list[ImageFrameMetrics],
    image_runs: list[ImageRunSummary],
    recommendations: list[Recommendation],
    oiiotool_bin: str,
    runner: CommandRunner,
    spotlight_limit: int,
    resolution_cache: dict[Path, tuple[int, int]],
) -> tuple[list[SpotlightComparison], list[str]]:
    by_run: dict[RunKey, list[ImageFrameMetrics]] = defaultdict(list)
    for metric in frame_metrics:
        by_run[_key_for_frame_metric(metric)].append(metric)

    for metrics in by_run.values():
        metrics.sort(key=lambda item: item.frame)

    ordered_run_keys: list[RunKey] = []
    for recommendation in recommendations:
        selected = recommendation.selected
        key = _key_for_summary(selected)
        if key in by_run:
            ordered_run_keys.append(key)

    best_by_group = sorted(
        [item for item in image_runs if item.median_rms_error is not None],
        key=lambda item: (
            item.settings.group,
            item.median_rms_error
            if item.median_rms_error is not None
            else float("inf"),
        ),
    )
    seen_groups: set[str] = set()
    for summary in best_by_group:
        group_lower = summary.settings.group.lower()
        if group_lower in seen_groups:
            continue
        seen_groups.add(group_lower)
        ordered_run_keys.append(
            _key_for_run(
                summary.run_dir,
                summary.settings.group,
                summary.settings.resolution_scale,
                summary.settings.pixel_variance,
                summary.settings.max_samples,
            )
        )

    all_runs_by_quality = sorted(
        [item for item in image_runs if item.median_rms_error is not None],
        key=lambda item: (
            item.median_rms_error if item.median_rms_error is not None else float("inf")
        ),
    )
    for summary in all_runs_by_quality:
        ordered_run_keys.append(
            _key_for_run(
                summary.run_dir,
                summary.settings.group,
                summary.settings.resolution_scale,
                summary.settings.pixel_variance,
                summary.settings.max_samples,
            )
        )

    selected_run_keys: list[RunKey] = []
    seen_run_keys: set[RunKey] = set()
    for key in ordered_run_keys:
        if key in seen_run_keys:
            continue
        if key not in by_run:
            continue
        seen_run_keys.add(key)
        selected_run_keys.append(key)
        if len(selected_run_keys) >= max(0, spotlight_limit):
            break

    warnings: list[str] = []
    spotlights: list[SpotlightComparison] = []
    for key in selected_run_keys:
        metrics = by_run[key]
        representative = max(metrics, key=lambda item: item.rms_error)

        gt_resolution = _read_image_resolution(
            representative.ground_truth_image,
            oiiotool_bin=oiiotool_bin,
            runner=runner,
            cache=resolution_cache,
        )
        if gt_resolution is None:
            warnings.append(
                f"unable to read ground-truth resolution for spotlight: {representative.ground_truth_image}"
            )
            continue

        image_paths = _write_spotlight_images(
            out_dir=out_dir,
            metric=representative,
            ground_truth_resolution=gt_resolution,
            oiiotool_bin=oiiotool_bin,
            runner=runner,
        )
        if image_paths is None:
            warnings.append(
                f"failed to generate spotlight images for {representative.settings.run_name} frame {representative.frame:04d}"
            )
            continue

        gt_png, candidate_png, diff_png = image_paths
        spotlights.append(
            SpotlightComparison(
                settings=representative.settings,
                run_dir=representative.run_dir,
                frame=representative.frame,
                rms_error=representative.rms_error,
                peak_snr=representative.peak_snr,
                mean_error=representative.mean_error,
                max_error=representative.max_error,
                gt_png=gt_png,
                candidate_png=candidate_png,
                diff_png=diff_png,
            )
        )

    return spotlights, warnings


def analyze_images(
    *,
    runs: list[DiscoveredRun],
    frame_rows: list[FrameRecord],
    run_rows: list[RunSummary],
    recommendations: list[Recommendation],
    out_dir: Path,
    images_subdir: str = "images_dn",
    ground_truth_group: str = "ground_truth",
    oiiotool_bin: str = "oiiotool",
    spotlight_limit: int = 6,
    command_runner: CommandRunner | None = None,
) -> ImageAnalysisResult:
    runner = command_runner or _default_runner
    warnings: list[str] = []

    if shutil.which(oiiotool_bin) is None:
        warnings.append(
            f"image analysis skipped: '{oiiotool_bin}' is not available on PATH"
        )
        return ImageAnalysisResult(
            frame_metrics=(),
            run_summaries=(),
            charts=ImageCharts(scatter_time_vs_rms=None, heatmaps=()),
            spotlights=(),
            warnings=tuple(warnings),
        )

    gt_by_frame = _build_ground_truth_frame_map(
        runs,
        images_subdir=images_subdir,
        ground_truth_group=ground_truth_group,
    )
    if not gt_by_frame:
        warnings.append(
            f"image analysis skipped: no ground-truth images found for group '{ground_truth_group}' in '{images_subdir}'"
        )
        return ImageAnalysisResult(
            frame_metrics=(),
            run_summaries=(),
            charts=ImageCharts(scatter_time_vs_rms=None, heatmaps=()),
            spotlights=(),
            warnings=tuple(warnings),
        )

    resolution_cache: dict[Path, tuple[int, int]] = {}
    frame_metrics: list[ImageFrameMetrics] = []

    for row in frame_rows:
        if row.settings.group.lower() == ground_truth_group.lower():
            continue

        candidate_image = _frame_image_path(row.run_dir, images_subdir, row.frame)
        if not candidate_image.is_file():
            warnings.append(
                f"missing candidate image for {row.settings.run_name} frame {row.frame:04d}: {candidate_image}"
            )
            continue

        ground_truth_image = gt_by_frame.get(row.frame)
        if ground_truth_image is None:
            warnings.append(
                f"missing ground-truth image for frame {row.frame:04d} (group={ground_truth_group})"
            )
            continue

        candidate_resolution = _read_image_resolution(
            candidate_image,
            oiiotool_bin=oiiotool_bin,
            runner=runner,
            cache=resolution_cache,
        )
        if candidate_resolution is None:
            warnings.append(f"unable to read image resolution: {candidate_image}")
            continue

        ground_truth_resolution = _read_image_resolution(
            ground_truth_image,
            oiiotool_bin=oiiotool_bin,
            runner=runner,
            cache=resolution_cache,
        )
        if ground_truth_resolution is None:
            warnings.append(f"unable to read image resolution: {ground_truth_image}")
            continue

        compared = _compare_frame_images(
            candidate_image=candidate_image,
            ground_truth_image=ground_truth_image,
            candidate_resolution=candidate_resolution,
            ground_truth_resolution=ground_truth_resolution,
            oiiotool_bin=oiiotool_bin,
            runner=runner,
        )
        if compared is None:
            warnings.append(
                f"unable to parse diff metrics for {candidate_image} vs {ground_truth_image}"
            )
            continue

        mean_error, rms_error, peak_snr, max_error = compared
        frame_metrics.append(
            ImageFrameMetrics(
                settings=row.settings,
                run_dir=row.run_dir,
                frame=row.frame,
                candidate_image=candidate_image,
                ground_truth_image=ground_truth_image,
                resized_to_ground_truth=(
                    candidate_resolution != ground_truth_resolution
                ),
                mean_error=mean_error,
                rms_error=rms_error,
                peak_snr=peak_snr,
                max_error=max_error,
            )
        )

    frame_metrics.sort(
        key=lambda item: (
            item.settings.group,
            item.settings.resolution_scale,
            item.settings.pixel_variance,
            item.settings.max_samples,
            item.frame,
        )
    )

    run_summaries = summarize_image_runs(frame_metrics)
    charts = _build_charts(
        out_dir=out_dir,
        image_runs=run_summaries,
        run_rows=run_rows,
    )
    spotlights, spotlight_warnings = _build_spotlights(
        out_dir=out_dir,
        frame_metrics=frame_metrics,
        image_runs=run_summaries,
        recommendations=recommendations,
        oiiotool_bin=oiiotool_bin,
        runner=runner,
        spotlight_limit=spotlight_limit,
        resolution_cache=resolution_cache,
    )
    warnings.extend(spotlight_warnings)

    return ImageAnalysisResult(
        frame_metrics=tuple(frame_metrics),
        run_summaries=tuple(run_summaries),
        charts=charts,
        spotlights=tuple(spotlights),
        warnings=tuple(_dedupe_stable(warnings)),
    )
