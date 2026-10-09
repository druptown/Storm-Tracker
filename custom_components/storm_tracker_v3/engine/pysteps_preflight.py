"""Observerende, begrensde bewegingspreflight voor een latere pySTEPS-worker."""
from __future__ import annotations

import math
from functools import lru_cache
from typing import Iterable

from .radar_frame import RadarFrame

# Resource policy, geen meteorologische snelheidsgrens. De zoeklus onderzoekt
# maximaal (2 * N + 1)² verschuivingen rond de verplaatsing van het binaire
# neerslagmasker. Daardoor kan het centrum op eender welke rastersprong liggen
# zonder een onbegrensde brute-force zoekactie te starten.
COMPUTE_SEARCH_HALF_WINDOW_CELLS = 12


def _weighted_centroid(frame: RadarFrame) -> tuple[float, float] | None:
    total = sum(intensity for _, _, intensity in frame.cells)
    if total <= 0:
        return None
    return (
        sum(row * intensity for row, _, intensity in frame.cells) / total,
        sum(column * intensity for _, column, intensity in frame.cells) / total,
    )


def _support_centroid(frame: RadarFrame) -> tuple[float, float] | None:
    """Geometrisch centrum van natte cellen, onafhankelijk van intensiteit."""
    if not frame.cells:
        return None
    count = len(frame.cells)
    return (
        sum(row for row, _, _ in frame.cells) / count,
        sum(column for _, column, _ in frame.cells) / count,
    )


def _axis_search_window(center: int, maximum_shift: int) -> tuple[int, int]:
    return (
        max(-maximum_shift, center - COMPUTE_SEARCH_HALF_WINDOW_CELLS),
        min(maximum_shift, center + COMPUTE_SEARCH_HALF_WINDOW_CELLS),
    )


def _compatible(frames: tuple[RadarFrame, ...]) -> bool:
    first = frames[0]
    return all(
        frame.source == first.source
        and frame.bounds == first.bounds
        and frame.height == first.height
        and frame.width == first.width
        and frame.grid_deg == first.grid_deg
        for frame in frames[1:]
    )


def _motion_values(
    frame: RadarFrame,
    row_per_minute: float,
    column_per_minute: float,
) -> tuple[float, float]:
    center_lat = (frame.bounds[1] + frame.bounds[3]) / 2.0
    north_kmh = -row_per_minute * frame.grid_deg * 110.574 * 60.0
    east_kmh = (
        column_per_minute * frame.grid_deg * 111.320
        * max(0.1, abs(math.cos(math.radians(center_lat)))) * 60.0
    )
    speed = math.hypot(east_kmh, north_kmh)
    heading = (
        0.0 if speed < 1e-9
        else (math.degrees(math.atan2(east_kmh, north_kmh)) + 360.0) % 360.0
    )
    return speed, heading


def _centroid_motion(origin: RadarFrame, observed: RadarFrame):
    start, end = _weighted_centroid(origin), _weighted_centroid(observed)
    minutes = (observed.nominal_timestamp - origin.nominal_timestamp) / 60.0
    if start is None or end is None or minutes <= 0:
        return None
    return _motion_values(
        origin,
        (end[0] - start[0]) / minutes,
        (end[1] - start[1]) / minutes,
    )


def _shifted_iou(
    origin: RadarFrame,
    observed: RadarFrame,
    row_shift: int,
    column_shift: int,
    *,
    origin_coordinates: tuple[tuple[int, int], ...] | None = None,
    actual: set[tuple[int, int]] | None = None,
) -> tuple[float, int]:
    """Bereken IoU zonder per kandidaat twee grote tijdelijke sets te bouwen."""
    if origin_coordinates is None:
        origin_coordinates = tuple(
            (row, column) for row, column, _ in origin.cells
        )
    if actual is None:
        actual = {(row, column) for row, column, _ in observed.cells}
    expected_count = 0
    overlap = 0
    for row, column in origin_coordinates:
        shifted_row = row + row_shift
        shifted_column = column + column_shift
        if (
            0 <= shifted_row < origin.height
            and 0 <= shifted_column < origin.width
        ):
            expected_count += 1
            overlap += (shifted_row, shifted_column) in actual
    union_count = expected_count + len(actual) - overlap
    return (overlap / union_count if union_count else 0.0, overlap)


@lru_cache(maxsize=16)
def _match_translation(origin: RadarFrame, observed: RadarFrame) -> dict | None:
    """Zoek rond een onbeperkte centroidverplaatsing met een vaste werkbudget.

    Het venster is in rastercellen uitgedrukt, niet in km/u. Een beste match op
    een kunstmatige vensterrand wordt als gecensureerde kandidaat teruggegeven;
    de caller mag die dus niet als een exact gemeten vector behandelen.
    """
    import numpy as np

    minutes = (observed.nominal_timestamp - origin.nominal_timestamp) / 60.0
    if minutes <= 0 or not origin.cells or not observed.cells:
        return None
    origin_center = _support_centroid(origin)
    observed_center = _support_centroid(observed)
    if origin_center is None or observed_center is None:
        return None
    center_row_shift = round(observed_center[0] - origin_center[0])
    center_column_shift = round(observed_center[1] - origin_center[1])
    max_row_shift = origin.height - 1
    max_column_shift = origin.width - 1
    row_min, row_max = _axis_search_window(
        center_row_shift, max_row_shift
    )
    column_min, column_max = _axis_search_window(
        center_column_shift, max_column_shift
    )
    search_window_exhaustive = (
        row_min == -max_row_shift
        and row_max == max_row_shift
        and column_min == -max_column_shift
        and column_max == max_column_shift
    )
    origin_mask = np.zeros((origin.height, origin.width), dtype=np.bool_)
    observed_mask = np.zeros((observed.height, observed.width), dtype=np.bool_)
    origin_rows, origin_columns = zip(*(
        (row, column) for row, column, _ in origin.cells
    ))
    observed_rows, observed_columns = zip(*(
        (row, column) for row, column, _ in observed.cells
    ))
    origin_mask[origin_rows, origin_columns] = True
    observed_mask[observed_rows, observed_columns] = True
    actual_count = int(np.count_nonzero(observed_mask))
    best = None
    for row_shift in range(row_min, row_max + 1):
        for column_shift in range(column_min, column_max + 1):
            origin_row_start = max(0, -row_shift)
            origin_row_stop = min(origin.height, origin.height - row_shift)
            origin_column_start = max(0, -column_shift)
            origin_column_stop = min(
                origin.width, origin.width - column_shift
            )
            shifted = origin_mask[
                origin_row_start:origin_row_stop,
                origin_column_start:origin_column_stop,
            ]
            actual = observed_mask[
                origin_row_start + row_shift:origin_row_stop + row_shift,
                origin_column_start + column_shift:origin_column_stop + column_shift,
            ]
            expected_count = int(np.count_nonzero(shifted))
            overlap = int(np.count_nonzero(np.logical_and(shifted, actual)))
            union_count = expected_count + actual_count - overlap
            iou = overlap / union_count if union_count else 0.0
            candidate = (
                iou,
                overlap,
                -(
                    (row_shift - center_row_shift) ** 2
                    + (column_shift - center_column_shift) ** 2
                ),
                row_shift, column_shift,
            )
            if best is None or candidate[:3] > best[:3]:
                best = candidate
    if best is None:
        return None
    iou, overlap, _, row_shift, column_shift = best
    speed, heading = _motion_values(
        origin, row_shift / minutes, column_shift / minutes
    )
    artificial_boundary_hit = (
        (row_shift == row_min and row_min > -max_row_shift)
        or (row_shift == row_max and row_max < max_row_shift)
        or (
            column_shift == column_min
            and column_min > -max_column_shift
        )
        or (
            column_shift == column_max
            and column_max < max_column_shift
        )
    )
    translation_censored = artificial_boundary_hit or (
        overlap == 0 and not search_window_exhaustive
    )
    censor_reason = None
    if artificial_boundary_hit:
        censor_reason = "beste_match_op_rand_computationeel_zoekvenster"
    elif overlap == 0 and not search_window_exhaustive:
        censor_reason = "geen_overlap_binnen_computationeel_zoekvenster"
    return {
        "row_shift": row_shift,
        "column_shift": column_shift,
        "row_per_minute": row_shift / minutes,
        "column_per_minute": column_shift / minutes,
        "speed_kmh": speed,
        "heading_deg": heading,
        "match_iou": iou,
        "overlap_cells": overlap,
        "search_center_row_shift": center_row_shift,
        "search_center_column_shift": center_column_shift,
        "search_window_row_min": row_min,
        "search_window_row_max": row_max,
        "search_window_column_min": column_min,
        "search_window_column_max": column_max,
        "search_window_candidates": (
            (row_max - row_min + 1) * (column_max - column_min + 1)
        ),
        "search_window_exhaustive": search_window_exhaustive,
        "translation_censored": translation_censored,
        "censor_reason": censor_reason,
    }


def _angle_difference(first: float, second: float) -> float:
    return abs((second - first + 180.0) % 360.0 - 180.0)


def evaluate_frames(frames: Iterable[RadarFrame]) -> dict:
    """Vergelijk centroid- en rasterbeweging zonder operationele uitvoer."""
    frames = tuple(frames)[-3:]
    base = {
        "status": "insufficient_frames",
        "experimental": True,
        "feeds_detection": False,
        "frames_used": len(frames),
        "method": "bounded_grid_match_v1",
        "search_policy": "centroid_guided_fixed_cell_window_v2",
        "compute_search_half_window_cells": COMPUTE_SEARCH_HALF_WINDOW_CELLS,
        "confidence": "insufficient",
        "translation_km_per_hour": None,
        "heading_deg": None,
        "verification_iou": None,
        "motion_match_iou": None,
        "vector_consistency_deg": None,
        "wet_cell_ratio": None,
        "centroid_translation_km_per_hour": None,
        "centroid_heading_deg": None,
        "search_window_candidates": None,
        "search_window_exhaustive": None,
        "translation_censored": None,
        "translation_uncertainty": None,
        "reason": None,
    }
    if len(frames) < 2:
        return base
    if not _compatible(frames):
        return {
            **base, "status": "incompatible_frames",
            "reason": "bron_of_raster_gewijzigd",
        }
    if any(not frame.cells for frame in frames):
        return {
            **base, "status": "dry_or_empty",
            "reason": "geen_neerslag_in_vergelijkbare_frames",
        }
    if any(
        newer.nominal_timestamp <= older.nominal_timestamp
        for older, newer in zip(frames, frames[1:])
    ):
        return {
            **base, "status": "invalid_timestamps",
            "reason": "geen_oplopende_nominale_minuten",
        }

    latest_match = _match_translation(frames[-2], frames[-1])
    if latest_match is None:
        return {**base, "status": "no_motion_match", "reason": "geen_rastermatch"}
    centroid = _centroid_motion(frames[-2], frames[-1])
    wet_cell_ratio = len(frames[-1].cells) / max(1, len(frames[-2].cells))
    result = {
        **base,
        "status": "warming_up_vectors",
        "translation_km_per_hour": round(latest_match["speed_kmh"], 1),
        "heading_deg": round(latest_match["heading_deg"], 0),
        "motion_match_iou": round(latest_match["match_iou"], 3),
        "wet_cell_ratio": round(wet_cell_ratio, 3),
        "centroid_translation_km_per_hour": (
            round(centroid[0], 1) if centroid else None
        ),
        "centroid_heading_deg": round(centroid[1], 0) if centroid else None,
        "search_window_candidates": latest_match["search_window_candidates"],
        "search_window_exhaustive": latest_match["search_window_exhaustive"],
        "translation_censored": latest_match["translation_censored"],
        "translation_uncertainty": (
            "open_ended_outside_compute_window"
            if latest_match["translation_censored"]
            else "resolved_inside_compute_window"
        ),
        "reason": "tweede_opeenvolgende_vector_nodig",
    }
    if latest_match["translation_censored"]:
        return {
            **result,
            "status": "motion_indeterminate",
            "confidence": "indeterminate",
            "reason": latest_match["censor_reason"],
        }
    if latest_match["overlap_cells"] <= 0:
        return {
            **result,
            "status": "no_motion_match",
            "confidence": "insufficient",
            "reason": "geen_rasteroverlap",
        }
    if len(frames) < 3:
        return result

    previous_match = _match_translation(frames[0], frames[1])
    if previous_match is None:
        return {**result, "status": "no_motion_match", "reason": "geen_vorige_rastermatch"}
    if previous_match["translation_censored"]:
        return {
            **result,
            "status": "motion_indeterminate",
            "confidence": "indeterminate",
            "reason": "vorige_vector_buiten_computationeel_zoekvenster",
        }
    if previous_match["overlap_cells"] <= 0:
        return {
            **result,
            "status": "no_motion_match",
            "confidence": "insufficient",
            "reason": "geen_vorige_rasteroverlap",
        }
    verification_minutes = (
        frames[2].nominal_timestamp - frames[1].nominal_timestamp
    ) / 60.0
    verification, _ = _shifted_iou(
        frames[1], frames[2],
        round(previous_match["row_per_minute"] * verification_minutes),
        round(previous_match["column_per_minute"] * verification_minutes),
    )
    angle_difference = _angle_difference(
        previous_match["heading_deg"], latest_match["heading_deg"]
    )
    max_speed = max(previous_match["speed_kmh"], latest_match["speed_kmh"])
    speed_ratio = (
        1.0 if max_speed <= 5.0
        else min(previous_match["speed_kmh"], latest_match["speed_kmh"])
        / max_speed
    )
    if verification >= 0.35 and angle_difference <= 30 and speed_ratio >= 0.5:
        confidence, status, reason = "high", "ready", None
    elif verification >= 0.15 and angle_difference <= 60 and speed_ratio >= 0.35:
        confidence, status, reason = "medium", "ready", None
    else:
        confidence, status, reason = (
            "low", "unstable_motion", "lage_overlap_of_inconsistente_vector"
        )
    return {
        **result,
        "status": status,
        "confidence": confidence,
        "feeds_detection": status == "ready",
        "verification_iou": round(verification, 3),
        "vector_consistency_deg": round(angle_difference, 1),
        "reason": reason,
    }
