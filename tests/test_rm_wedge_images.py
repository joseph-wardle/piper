from __future__ import annotations

import tempfile
import unittest
from collections.abc import Sequence
from pathlib import Path
from unittest.mock import patch

from piper.tools.rm_wedge.images import _parse_diff_output, analyze_images
from piper.tools.rm_wedge.models import (
    CoreMetrics,
    DiscoveredRun,
    FrameRecord,
    Recommendation,
    RunSettings,
    RunSummary,
)


def _metrics(mainloop_s: float) -> CoreMetrics:
    return CoreMetrics(
        mainloop_s=mainloop_s,
        ttfp_s=None,
        ttfr_s=None,
        ttfi_s=None,
        buckets_rendered=None,
        iterations_completed=None,
        rays_traced=None,
        hits=None,
        tests=None,
        bundles=None,
        shade_hit_bxdf_s=None,
        shade_hit_opacity_s=None,
        shade_get_shade_groups_s=None,
        shade_build_displacement_s=None,
        trace_shadows_s=None,
        create_geom_proto_s=None,
        create_geom_proto_count=None,
        proc_mem_committed_b=None,
        proc_mem_peak_committed_b=None,
        mem_tracking_current_b=None,
        mem_tracking_peak_b=None,
        mem_dedupe_saved_b=None,
        proc_user_s=None,
        proc_sys_s=None,
        cpu_pct_now=None,
        cpu_pct_avg=None,
        num_render_threads=None,
        integrator=None,
    )


class RenderWedgeImageTests(unittest.TestCase):
    def test_parse_diff_output_extracts_metrics(self) -> None:
        output = """
Computing diff of a vs b
  Mean error = 0.0100858
  RMS error = 0.0192908
  Peak SNR = 56.977
  Max error  = 1.638899803161621 @ (976, 0, R)
FAILURE
"""
        parsed = _parse_diff_output(output)
        assert parsed is not None
        mean_error, rms_error, peak_snr, max_error = parsed

        self.assertAlmostEqual(mean_error, 0.0100858)
        self.assertAlmostEqual(rms_error, 0.0192908)
        self.assertAlmostEqual(peak_snr, 56.977)
        self.assertAlmostEqual(max_error, 1.638899803161621)

    def test_analyze_images_builds_metrics_and_spotlight(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)

            final_run_dir = (
                root
                / "final"
                / "final_resolution_scale_1p00_pixel_variance_0p050_max_samples_256"
            )
            gt_run_dir = (
                root
                / "ground_truth"
                / "ground_truth_resolution_scale_1p00_pixel_variance_0p001_max_samples_4096"
            )

            final_stats = (
                final_run_dir
                / "stats"
                / "final_resolution_scale_1p00_pixel_variance_0p050_max_samples_256.0160.json"
            )
            gt_stats = (
                gt_run_dir
                / "stats"
                / "ground_truth_resolution_scale_1p00_pixel_variance_0p001_max_samples_4096.0160.json"
            )
            final_image = final_run_dir / "images_dn" / "0160.exr"
            gt_image = gt_run_dir / "images_dn" / "0160.exr"

            final_stats.parent.mkdir(parents=True, exist_ok=True)
            gt_stats.parent.mkdir(parents=True, exist_ok=True)
            final_image.parent.mkdir(parents=True, exist_ok=True)
            gt_image.parent.mkdir(parents=True, exist_ok=True)

            final_stats.write_text("{}", encoding="utf-8")
            gt_stats.write_text("{}", encoding="utf-8")
            final_image.write_text("exr", encoding="utf-8")
            gt_image.write_text("exr", encoding="utf-8")

            final_settings = RunSettings(
                group="final",
                resolution_scale=1.0,
                pixel_variance=0.05,
                max_samples=256,
                run_name="final_resolution_scale_1p00_pixel_variance_0p050_max_samples_256",
            )
            gt_settings = RunSettings(
                group="ground_truth",
                resolution_scale=1.0,
                pixel_variance=0.001,
                max_samples=4096,
                run_name="ground_truth_resolution_scale_1p00_pixel_variance_0p001_max_samples_4096",
            )

            final_run = DiscoveredRun(
                settings=final_settings,
                run_dir=final_run_dir,
                stats_files=(final_stats,),
                render_usd=None,
                denoise_json=None,
            )
            gt_run = DiscoveredRun(
                settings=gt_settings,
                run_dir=gt_run_dir,
                stats_files=(gt_stats,),
                render_usd=None,
                denoise_json=None,
            )

            frame_rows = [
                FrameRecord(
                    settings=final_settings,
                    run_dir=final_run_dir,
                    frame=160,
                    stats_file=final_stats,
                    attempt_index=0,
                    attempt_count=1,
                    metrics=_metrics(200.0),
                    warnings=(),
                )
            ]

            final_summary = RunSummary(
                settings=final_settings,
                run_dir=final_run_dir,
                frame_count=1,
                usable_frames=1,
                median_mainloop_s=200.0,
                median_ttfp_s=20.0,
                median_peak_mem_gib=64.0,
                median_cpu_avg_pct=75.0,
                median_rays_per_s=10_000.0,
                median_share_trace_shadows=0.2,
                median_share_shade_bxdf=0.1,
                median_share_shade_opacity=0.03,
                median_share_shade_groups=0.02,
                median_share_shade_displacement=0.01,
                median_share_geom_proto=0.04,
                quality_proxy=0.5,
                time_norm=0.5,
                memory_norm=0.5,
                objective_dailies=0.5,
                objective_final=0.5,
                warnings=(),
            )

            recommendation = Recommendation(
                group="final",
                objective_name="objective_final",
                selected=final_summary,
                alternatives=(),
                pareto_count=1,
                selection_notes=("test",),
            )

            diff_output = """
Computing diff of a vs b
  Mean error = 0.0100
  RMS error = 0.0200
  Peak SNR = 55.000
  Max error  = 1.5000 @ (10, 20, R)
FAILURE
"""

            def fake_runner(command: Sequence[str]) -> tuple[int, str]:
                if "--printinfo" in command:
                    return (0, "1920 x 1080, 3 channel, float\n")
                if "--diff" in command:
                    return (1, diff_output)
                if "-o" in command:
                    out_path = Path(command[command.index("-o") + 1])
                    out_path.parent.mkdir(parents=True, exist_ok=True)
                    out_path.write_text("png", encoding="utf-8")
                    return (0, "")
                return (0, "")

            with patch(
                "piper.tools.rm_wedge.images.shutil.which",
                return_value="/usr/bin/oiiotool",
            ):
                result = analyze_images(
                    runs=[final_run, gt_run],
                    frame_rows=frame_rows,
                    run_rows=[final_summary],
                    recommendations=[recommendation],
                    out_dir=root / "out",
                    spotlight_limit=1,
                    command_runner=fake_runner,
                )

            self.assertEqual(len(result.frame_metrics), 1)
            self.assertEqual(len(result.run_summaries), 1)
            self.assertEqual(len(result.spotlights), 1)
            self.assertEqual(result.charts.scatter_time_vs_rms, None)
            self.assertEqual(result.warnings, ())


if __name__ == "__main__":
    unittest.main()
