"""Observerende, begrensde bewegingspreflight voor een latere pySTEPS-worker."""
from __future__ import annotations

import math
from typing import Iterable

from .radar_frame import RadarFrame

MAX_TRANSLATION_KMH = 180.0


def _weighted_centroid(frame: RadarFrame) -> tuple[float, float] | None:
    total = sum(intensity for _, _, intensity in frame.cells)
    if total <= 0:
        return None
    return (
        sum(row * intensity for row, _, intensity in frame.cells) / total,
        sum(column * intensity for _, column, intensity in frame.cells) / total,
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
    origin: RadarFrame, observed: RadarFrame, row_shift: int, column_shift: int
) -> tuple[float, int]:
    expected = {
        (row + row_shift, column + column_shift)
        for row, column, _ in origin.cells
        if (
            0 <= row + row_shift < origin.height
            and 0 <= column + column_shift < origin.width
        )
    }
    actual = {(row, column) for row, column, _ in observed.cells}
    union = expected | actual
    overlap = len(expected & actual)
    return (overlap / len(union) if union else 0.0, overlap)


def _match_translation(origin: RadarFrame, observed: RadarFrame) -> dict | None:
    """Zoek de beste gehele rasterverschuiving binnen een fysieke snelheidslimiet."""
    minutes = (observed.nominal_timestamp - origin.nominal_timestamp) / 60.0
    if minutes <= 0 or not origin.cells or not observed.cells:
        return None
    center_lat = (origin.bounds[1] + origin.bounds[3]) / 2.0
    row_cell_km = origin.grid_deg * 110.574
    column_cell_km = (
        origin.grid_deg * 111.320
        * max(0.1, abs(math.cos(math.radians(center_lat))))
    )
    max_distance = MAX_TRANSLATION_KMH * minutes / 60.0
    max_rows = max(1, min(origin.height - 1, math.ceil(max_distance / row_cell_km)))
    max_columns = max(
        1, min(origin.width - 1, math.ceil(max_distance / column_cell_km))
    )
    best = None
    for row_shift in range(-max_rows, max_rows + 1):
        for column_shift in range(-max_columns, max_columns + 1):
            iou, overlap = _shifted_iou(
                origin, observed, row_shift, column_shift
            )
            candidate = (
                iou, overlap, -(row_shift ** 2 + column_shift ** 2),
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
    return {
        "row_shift": row_shift,
        "column_shift": column_shift,
        "row_per_minute": row_shift / minutes,
        "column_per_minute": column_shift / minutes,
        "speed_kmh": speed,
        "heading_deg": heading,
        "match_iou": iou,
        "overlap_cells": overlap,
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
        "confidence": "insufficient",
        "translation_km_per_hour": None,
        "heading_deg": None,
        "verification_iou": None,
        "motion_match_iou": None,
        "vector_consistency_deg": None,
        "wet_cell_ratio": None,
        "centroid_translation_km_per_hour": None,
        "centroid_heading_deg": None,
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
        "reason": "tweede_opeenvolgende_vector_nodig",
    }
    if len(frames) < 3:
        return result

    previous_match = _match_translation(frames[0], frames[1])
    if previous_match is None:
        return {**result, "status": "no_motion_match", "reason": "geen_vorige_rastermatch"}
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
        "verification_iou": round(verification, 3),
        "vector_consistency_deg": round(angle_difference, 1),
        "reason": reason,
    }
