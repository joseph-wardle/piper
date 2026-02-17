from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from typing import Any

from piper.tools.rm_wedge.cli import main
from piper.tools.rm_wedge.discovery import parse_run_settings
from piper.tools.rm_wedge.stats_extract import choose_attempt


def _attempt_payload(
    *,
    mainloop_s: float,
    ttfp_s: float,
    peak_mem_b: int,
    cpu_avg_pct: float,
    rays_traced: int,
) -> dict[str, Any]:
    return {
        "metrics": {
            "rman": {
                "renderer": {
                    "mainLoop": {
                        "outer": {
                            "time": {
                                "total": {"payload": [mainloop_s]},
                                "count": {"payload": [1]},
                            }
                        }
                    }
                },
                "timeToFirstPixel": {"payload": [ttfp_s]},
                "timeToFirstRaytrace": {"payload": [ttfp_s * 1.2]},
                "timeToFirstIteration": {"payload": [ttfp_s * 1.3]},
                "totalBucketsRendered": {"payload": [128]},
                "iterationsCompleted": {"payload": [8]},
                "raytracing": {
                    "numRays": {"payload": [rays_traced]},
                    "numHits": {"payload": [rays_traced // 2]},
                    "numTests": {"payload": [rays_traced * 2]},
                    "numBundles": {"payload": [2048]},
                },
                "shading": {
                    "hit": {
                        "bxdf": {"time": {"total": {"payload": [mainloop_s * 0.20]}}},
                        "opacity": {
                            "time": {"total": {"payload": [mainloop_s * 0.05]}}
                        },
                    },
                    "getShadeGroups": {
                        "time": {"total": {"payload": [mainloop_s * 0.03]}}
                    },
                    "build": {
                        "displacement": {
                            "time": {"total": {"payload": [mainloop_s * 0.02]}}
                        }
                    },
                },
                "lighting": {
                    "traceShadows": {
                        "time": {"total": {"payload": [mainloop_s * 0.25]}}
                    }
                },
                "riley": {
                    "createGeometryPrototype": {
                        "time": {
                            "total": {"payload": [mainloop_s * 0.04]},
                            "count": {"payload": [64]},
                        }
                    }
                },
                "numRenderThreads": {"payload": [32]},
                "settings": {"integrator": {"payload": ["PxrPathTracer"]}},
            },
            "system": {
                "processMemory": {
                    "payload": [
                        [
                            peak_mem_b - 2_000_000_000,
                            peak_mem_b - 1_000_000_000,
                            peak_mem_b,
                        ]
                    ]
                },
                "memoryTracking": {
                    "system": {
                        "mem": {
                            "payload": [
                                [
                                    peak_mem_b // 3,
                                    peak_mem_b // 2,
                                    peak_mem_b // 50,
                                ]
                            ]
                        }
                    }
                },
                "processTime": {
                    "payload": [[mainloop_s * 3.0, mainloop_s * 0.6, 80.0, cpu_avg_pct]]
                },
            },
        }
    }


def _write_stats_file(path: Path, attempts: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"frame": {"attempts": attempts}}
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_run(
    root: Path,
    group_dir: str,
    run_name: str,
    *,
    mainloop_s: float,
    ttfp_s: float,
    peak_mem_b: int,
    cpu_avg_pct: float,
    rays_traced: int,
) -> None:
    run_dir = root / group_dir / run_name
    (run_dir / "stats").mkdir(parents=True, exist_ok=True)
    (run_dir / "render.usd").write_text("usd", encoding="utf-8")
    (run_dir / "denoise.json").write_text("{}", encoding="utf-8")

    for frame in (160, 161):
        first_attempt = _attempt_payload(
            mainloop_s=mainloop_s * 0.5,
            ttfp_s=ttfp_s,
            peak_mem_b=peak_mem_b,
            cpu_avg_pct=cpu_avg_pct,
            rays_traced=rays_traced,
        )
        last_attempt = _attempt_payload(
            mainloop_s=mainloop_s,
            ttfp_s=ttfp_s,
            peak_mem_b=peak_mem_b,
            cpu_avg_pct=cpu_avg_pct,
            rays_traced=rays_traced,
        )
        _write_stats_file(
            run_dir / "stats" / f"{run_name}.{frame:04d}.json",
            [first_attempt, last_attempt],
        )


class RenderWedgeToolTests(unittest.TestCase):
    def test_parse_run_settings_supports_underscored_group_names(self) -> None:
        settings = parse_run_settings(
            "ground_truth_resolution_scale_1p00_pixel_variance_0p001_max_samples_4096"
        )
        self.assertEqual(settings.group, "ground_truth")
        self.assertEqual(settings.resolution_scale, 1.0)
        self.assertEqual(settings.pixel_variance, 0.001)
        self.assertEqual(settings.max_samples, 4096)

    def test_choose_attempt_respects_policy(self) -> None:
        frame_json = {
            "frame": {
                "attempts": [
                    _attempt_payload(
                        mainloop_s=100.0,
                        ttfp_s=20.0,
                        peak_mem_b=64_000_000_000,
                        cpu_avg_pct=70.0,
                        rays_traced=1_000_000,
                    ),
                    _attempt_payload(
                        mainloop_s=200.0,
                        ttfp_s=20.0,
                        peak_mem_b=64_000_000_000,
                        cpu_avg_pct=70.0,
                        rays_traced=2_000_000,
                    ),
                ]
            }
        }

        index, count, attempt = choose_attempt(frame_json, "latest")
        self.assertEqual(index, 1)
        self.assertEqual(count, 2)
        self.assertEqual(
            attempt["metrics"]["rman"]["renderer"]["mainLoop"]["outer"]["time"][
                "total"
            ]["payload"][0],
            200.0,
        )

        index, _count, attempt = choose_attempt(frame_json, "first")
        self.assertEqual(index, 0)
        self.assertEqual(
            attempt["metrics"]["rman"]["renderer"]["mainLoop"]["outer"]["time"][
                "total"
            ]["payload"][0],
            100.0,
        )

        index, _count, attempt = choose_attempt(frame_json, "max-mainloop")
        self.assertEqual(index, 1)
        self.assertEqual(
            attempt["metrics"]["rman"]["renderer"]["mainLoop"]["outer"]["time"][
                "total"
            ]["payload"][0],
            200.0,
        )

    def test_end_to_end_report_writes_outputs_and_recommendations(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "wedge"
            out = Path(temp_dir) / "report"

            _write_run(
                root,
                "dailies",
                "dailies_resolution_scale_0p50_pixel_variance_1p000_max_samples_64",
                mainloop_s=120.0,
                ttfp_s=25.0,
                peak_mem_b=48_000_000_000,
                cpu_avg_pct=70.0,
                rays_traced=1_000_000,
            )
            _write_run(
                root,
                "dailies",
                "dailies_resolution_scale_1p00_pixel_variance_0p100_max_samples_256",
                mainloop_s=260.0,
                ttfp_s=45.0,
                peak_mem_b=62_000_000_000,
                cpu_avg_pct=68.0,
                rays_traced=2_200_000,
            )

            _write_run(
                root,
                "final",
                "final_resolution_scale_1p00_pixel_variance_0p050_max_samples_256",
                mainloop_s=320.0,
                ttfp_s=60.0,
                peak_mem_b=66_000_000_000,
                cpu_avg_pct=66.0,
                rays_traced=2_800_000,
            )
            _write_run(
                root,
                "final",
                "final_resolution_scale_1p00_pixel_variance_0p005_max_samples_1024",
                mainloop_s=520.0,
                ttfp_s=90.0,
                peak_mem_b=82_000_000_000,
                cpu_avg_pct=61.0,
                rays_traced=4_400_000,
            )

            _write_run(
                root,
                "ground_truth",
                "ground_truth_resolution_scale_1p00_pixel_variance_0p001_max_samples_4096",
                mainloop_s=900.0,
                ttfp_s=110.0,
                peak_mem_b=92_000_000_000,
                cpu_avg_pct=58.0,
                rays_traced=6_500_000,
            )

            exit_code = main(
                [
                    "--root",
                    str(root),
                    "--out",
                    str(out),
                    "--attempt-policy",
                    "latest",
                    "--quiet",
                ]
            )
            self.assertEqual(exit_code, 0)

            self.assertTrue((out / "report.html").is_file())
            self.assertTrue((out / "data" / "frames.csv").is_file())
            self.assertTrue((out / "data" / "runs.csv").is_file())
            self.assertTrue((out / "data" / "recommendations.json").is_file())

            recommendations = json.loads(
                (out / "data" / "recommendations.json").read_text(encoding="utf-8")
            )
            self.assertEqual(
                {item["group"] for item in recommendations}, {"dailies", "final"}
            )

            final_recommendation = next(
                item for item in recommendations if item["group"] == "final"
            )
            self.assertEqual(
                final_recommendation["selected"]["run_name"],
                "final_resolution_scale_1p00_pixel_variance_0p005_max_samples_1024",
            )


if __name__ == "__main__":
    unittest.main()
