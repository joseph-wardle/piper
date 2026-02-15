from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class RunSettings:
    """Parsed settings encoded in a wedge run directory name."""

    group: str
    resolution_scale: float
    pixel_variance: float
    max_samples: int
    run_name: str

    def key(self) -> tuple[str, float, float, int]:
        return (
            self.group,
            self.resolution_scale,
            self.pixel_variance,
            self.max_samples,
        )

    def label(self) -> str:
        return (
            f"{self.group} rs={self.resolution_scale:g} "
            f"pv={self.pixel_variance:g} ms={self.max_samples}"
        )


@dataclass(frozen=True, slots=True)
class DiscoveredRun:
    """Filesystem entry points for one wedge variant."""

    settings: RunSettings
    run_dir: Path
    stats_files: tuple[Path, ...]
    render_usd: Path | None
    denoise_json: Path | None


@dataclass(frozen=True, slots=True)
class DiscoveryResult:
    """Result of scanning a wedge directory for runs."""

    runs: tuple[DiscoveredRun, ...]
    warnings: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class CoreMetrics:
    """Curated RenderMan/system metrics used by pass-1 analysis."""

    mainloop_s: float | None
    ttfp_s: float | None
    ttfr_s: float | None
    ttfi_s: float | None

    buckets_rendered: int | None
    iterations_completed: int | None

    rays_traced: int | None
    hits: int | None
    tests: int | None
    bundles: int | None

    shade_hit_bxdf_s: float | None
    shade_hit_opacity_s: float | None
    shade_get_shade_groups_s: float | None
    shade_build_displacement_s: float | None
    trace_shadows_s: float | None

    create_geom_proto_s: float | None
    create_geom_proto_count: int | None

    proc_mem_committed_b: int | None
    proc_mem_peak_committed_b: int | None
    mem_tracking_current_b: int | None
    mem_tracking_peak_b: int | None
    mem_dedupe_saved_b: int | None

    proc_user_s: float | None
    proc_sys_s: float | None
    cpu_pct_now: float | None
    cpu_pct_avg: float | None

    num_render_threads: int | None
    integrator: str | None


@dataclass(frozen=True, slots=True)
class FrameRecord:
    """One frame's selected attempt and extracted metrics."""

    settings: RunSettings
    run_dir: Path
    frame: int
    stats_file: Path
    attempt_index: int
    attempt_count: int
    metrics: CoreMetrics
    warnings: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class RunSummary:
    """Per-run statistical summary used in recommendations and report tables."""

    settings: RunSettings
    run_dir: Path

    frame_count: int
    usable_frames: int

    median_mainloop_s: float | None
    median_ttfp_s: float | None
    median_peak_mem_gib: float | None
    median_cpu_avg_pct: float | None
    median_rays_per_s: float | None

    median_share_trace_shadows: float | None
    median_share_shade_bxdf: float | None
    median_share_shade_opacity: float | None
    median_share_shade_groups: float | None
    median_share_shade_displacement: float | None
    median_share_geom_proto: float | None

    quality_proxy: float
    time_norm: float
    memory_norm: float
    objective_dailies: float
    objective_final: float

    warnings: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class Recommendation:
    """Chosen run and nearby alternatives for a workflow target."""

    group: str
    objective_name: str
    selected: RunSummary
    alternatives: tuple[RunSummary, ...]
    pareto_count: int
    selection_notes: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class HotspotSummary:
    """Global hotspot share statistics across analyzed frames."""

    hotspot: str
    median_share: float
    p90_share: float
