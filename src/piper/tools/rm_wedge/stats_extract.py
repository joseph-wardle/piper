from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Literal, cast

from .models import CoreMetrics

AttemptPolicy = Literal["latest", "first", "max-mainloop"]
PathToken = str | int


def _get_path(data: object, path: Sequence[PathToken]) -> object | None:
    current: object = data
    for token in path:
        if isinstance(token, int):
            if not isinstance(current, list):
                return None
            if token < 0 or token >= len(current):
                return None
            current = current[token]
            continue

        if not isinstance(current, dict):
            return None
        current_dict = cast(dict[str, Any], current)
        if token not in current_dict:
            return None
        current = current_dict[token]

    return current


def _payload_scalar(data: object, path: Sequence[PathToken]) -> object | None:
    node = _get_path(data, path)
    if not isinstance(node, dict):
        return None

    node_dict = cast(dict[str, Any], node)
    payload = node_dict.get("payload")
    if not isinstance(payload, list) or not payload:
        return None

    return payload[0]


def _as_float(value: object | None) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _as_int(value: object | None) -> int | None:
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return None


def _as_str(value: object | None) -> str | None:
    if isinstance(value, str):
        return value
    return None


def _triplet_ints(value: object | None) -> tuple[int, int, int] | None:
    if not isinstance(value, list) or len(value) != 3:
        return None
    first, second, third = value
    if not isinstance(first, int):
        return None
    if not isinstance(second, int):
        return None
    if not isinstance(third, int):
        return None
    return (first, second, third)


def _quad_numbers(value: object | None) -> tuple[float, float, float, float] | None:
    if not isinstance(value, list) or len(value) != 4:
        return None
    first, second, third, fourth = value
    if not isinstance(first, (int, float)):
        return None
    if not isinstance(second, (int, float)):
        return None
    if not isinstance(third, (int, float)):
        return None
    if not isinstance(fourth, (int, float)):
        return None
    return (float(first), float(second), float(third), float(fourth))


def _read_mainloop(attempt: Mapping[str, Any]) -> float | None:
    return _as_float(
        _payload_scalar(
            attempt,
            (
                "metrics",
                "rman",
                "renderer",
                "mainLoop",
                "outer",
                "time",
                "total",
            ),
        )
    )


def choose_attempt(
    frame_json: Mapping[str, Any], policy: AttemptPolicy
) -> tuple[int, int, Mapping[str, Any]]:
    attempts_obj = _get_path(frame_json, ("frame", "attempts"))
    if not isinstance(attempts_obj, list) or not attempts_obj:
        raise ValueError("stats file has no frame.attempts entries")

    attempts: list[dict[str, Any]] = []
    for item in attempts_obj:
        if isinstance(item, dict):
            attempts.append(cast(dict[str, Any], item))
    if not attempts:
        raise ValueError("frame.attempts does not contain mapping entries")

    selected_index = len(attempts) - 1

    if policy == "first":
        selected_index = 0
    elif policy == "max-mainloop":
        scored: list[tuple[int, float]] = []
        for index, attempt in enumerate(attempts):
            mainloop = _read_mainloop(attempt)
            if mainloop is None:
                continue
            scored.append((index, mainloop))
        if scored:
            selected_index = max(scored, key=lambda item: item[1])[0]

    selected = attempts[selected_index]
    return selected_index, len(attempts), cast(Mapping[str, Any], selected)


def extract_core_metrics(attempt: Mapping[str, Any]) -> CoreMetrics:
    mainloop_s = _read_mainloop(attempt)

    proc_mem_triplet = _triplet_ints(
        _payload_scalar(attempt, ("metrics", "system", "processMemory"))
    )
    mem_track_triplet = _triplet_ints(
        _payload_scalar(
            attempt,
            ("metrics", "system", "memoryTracking", "system", "mem"),
        )
    )
    proc_time_quad = _quad_numbers(
        _payload_scalar(attempt, ("metrics", "system", "processTime"))
    )

    return CoreMetrics(
        mainloop_s=mainloop_s,
        ttfp_s=_as_float(
            _payload_scalar(attempt, ("metrics", "rman", "timeToFirstPixel"))
        ),
        ttfr_s=_as_float(
            _payload_scalar(attempt, ("metrics", "rman", "timeToFirstRaytrace"))
        ),
        ttfi_s=_as_float(
            _payload_scalar(attempt, ("metrics", "rman", "timeToFirstIteration"))
        ),
        buckets_rendered=_as_int(
            _payload_scalar(attempt, ("metrics", "rman", "totalBucketsRendered"))
        ),
        iterations_completed=_as_int(
            _payload_scalar(attempt, ("metrics", "rman", "iterationsCompleted"))
        ),
        rays_traced=_as_int(
            _payload_scalar(attempt, ("metrics", "rman", "raytracing", "numRays"))
        ),
        hits=_as_int(
            _payload_scalar(attempt, ("metrics", "rman", "raytracing", "numHits"))
        ),
        tests=_as_int(
            _payload_scalar(attempt, ("metrics", "rman", "raytracing", "numTests"))
        ),
        bundles=_as_int(
            _payload_scalar(attempt, ("metrics", "rman", "raytracing", "numBundles"))
        ),
        shade_hit_bxdf_s=_as_float(
            _payload_scalar(
                attempt,
                ("metrics", "rman", "shading", "hit", "bxdf", "time", "total"),
            )
        ),
        shade_hit_opacity_s=_as_float(
            _payload_scalar(
                attempt,
                ("metrics", "rman", "shading", "hit", "opacity", "time", "total"),
            )
        ),
        shade_get_shade_groups_s=_as_float(
            _payload_scalar(
                attempt,
                (
                    "metrics",
                    "rman",
                    "shading",
                    "getShadeGroups",
                    "time",
                    "total",
                ),
            )
        ),
        shade_build_displacement_s=_as_float(
            _payload_scalar(
                attempt,
                (
                    "metrics",
                    "rman",
                    "shading",
                    "build",
                    "displacement",
                    "time",
                    "total",
                ),
            )
        ),
        trace_shadows_s=_as_float(
            _payload_scalar(
                attempt,
                ("metrics", "rman", "lighting", "traceShadows", "time", "total"),
            )
        ),
        create_geom_proto_s=_as_float(
            _payload_scalar(
                attempt,
                (
                    "metrics",
                    "rman",
                    "riley",
                    "createGeometryPrototype",
                    "time",
                    "total",
                ),
            )
        ),
        create_geom_proto_count=_as_int(
            _payload_scalar(
                attempt,
                (
                    "metrics",
                    "rman",
                    "riley",
                    "createGeometryPrototype",
                    "time",
                    "count",
                ),
            )
        ),
        proc_mem_committed_b=(proc_mem_triplet[1] if proc_mem_triplet else None),
        proc_mem_peak_committed_b=(proc_mem_triplet[2] if proc_mem_triplet else None),
        mem_tracking_current_b=(mem_track_triplet[0] if mem_track_triplet else None),
        mem_tracking_peak_b=(mem_track_triplet[1] if mem_track_triplet else None),
        mem_dedupe_saved_b=(mem_track_triplet[2] if mem_track_triplet else None),
        proc_user_s=(proc_time_quad[0] if proc_time_quad else None),
        proc_sys_s=(proc_time_quad[1] if proc_time_quad else None),
        cpu_pct_now=(proc_time_quad[2] if proc_time_quad else None),
        cpu_pct_avg=(proc_time_quad[3] if proc_time_quad else None),
        num_render_threads=_as_int(
            _payload_scalar(attempt, ("metrics", "rman", "numRenderThreads"))
        ),
        integrator=_as_str(
            _payload_scalar(attempt, ("metrics", "rman", "settings", "integrator"))
        ),
    )
