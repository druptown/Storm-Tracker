"""Observerende preflight voor een latere, geïsoleerde pySTEPS-worker.

Dit is bewust geen vervanger van pySTEPS en ook geen bron voor waarschuwingen.
Het meet enkel of de genormaliseerde rasterframes regelmatig genoeg zijn en of
een simpele translatie de eerstvolgende werkelijke frame al enigszins verklaart.
"""
from __future__ import annotations

import math
from typing import Iterable

from .radar_frame import RadarFrame


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


def _shift_skill(
    origin: RadarFrame,
    observed: RadarFrame,
    row_per_minute: float,
    column_per_minute: float,
) -> float | None:
    minutes = (observed.nominal_timestamp - origin.nominal_timestamp) / 60.0
    if minutes <= 0 or not origin.cells or not observed.cells:
        return None
    expected = {
        (
            round(row + row_per_minute * minutes),
            round(column + column_per_minute * minutes),
        )
        for row, column, _ in origin.cells
    }
    actual = {(row, column) for row, column, _ in observed.cells}
    expected = {
        (row, column) for row, column in expected
        if 0 <= row < origin.height and 0 <= column < origin.width
    }
    union = expected | actual
    return len(expected & actual) / len(union) if union else None


def evaluate_frames(frames: Iterable[RadarFrame]) -> dict:
    """Meet rastercontinuïteit en translatievaardigheid, zonder outputraster."""
    frames = tuple(frames)[-3:]
    base = {
        "status": "insufficient_frames",
        "experimental": True,
        "feeds_detection": False,
        "frames_used": len(frames),
        "translation_km_per_hour": None,
        "heading_deg": None,
        "verification_iou": None,
        "reason": None,
    }
    if len(frames) < 2:
        return base
    if not _compatible(frames):
        return {**base, "status": "incompatible_frames", "reason": "bron_of_raster_gewijzigd"}
    first, latest = frames[0], frames[-1]
    start = _weighted_centroid(first)
    end = _weighted_centroid(latest)
    elapsed_minutes = (latest.nominal_timestamp - first.nominal_timestamp) / 60.0
    if start is None or end is None:
        return {**base, "status": "dry_or_empty", "reason": "geen_neerslag_in_vergelijkbare_frames"}
    if elapsed_minutes <= 0:
        return {**base, "status": "invalid_timestamps", "reason": "geen_oplopende_nominale_minuten"}

    # Rijen lopen zuidwaarts, kolommen oostwaarts in het genormaliseerde raster.
    row_per_minute = (end[0] - start[0]) / elapsed_minutes
    column_per_minute = (end[1] - start[1]) / elapsed_minutes
    center_lat = (first.bounds[1] + first.bounds[3]) / 2.0
    north_km_per_hour = -row_per_minute * first.grid_deg * 110.574 * 60.0
    east_km_per_hour = (
        column_per_minute * first.grid_deg * 111.320
        * max(0.1, abs(math.cos(math.radians(center_lat)))) * 60.0
    )
    speed = math.hypot(east_km_per_hour, north_km_per_hour)
    heading = (math.degrees(math.atan2(east_km_per_hour, north_km_per_hour)) + 360.0) % 360.0
    verification = None
    if len(frames) == 3:
        previous_start = _weighted_centroid(frames[0])
        previous_end = _weighted_centroid(frames[1])
        previous_minutes = (frames[1].nominal_timestamp - frames[0].nominal_timestamp) / 60.0
        if previous_start and previous_end and previous_minutes > 0:
            verification = _shift_skill(
                frames[1], frames[2],
                (previous_end[0] - previous_start[0]) / previous_minutes,
                (previous_end[1] - previous_start[1]) / previous_minutes,
            )
    return {
        **base,
        "status": "ready",
        "translation_km_per_hour": round(speed, 1),
        "heading_deg": round(heading, 0),
        "verification_iou": round(verification, 3) if verification is not None else None,
    }
