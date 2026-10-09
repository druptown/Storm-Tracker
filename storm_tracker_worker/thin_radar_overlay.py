"""Compact display-only radar overlays for the thin operational map."""

from __future__ import annotations

from typing import Any

import numpy as np
from pyproj import Transformer

from .thin_provider_adapters import ThinRadarField


RAIN_RATE_LEVELS_MM_H = (0.1, 0.5, 1.0, 2.0, 4.0, 8.0, 16.0, 32.0)


def _intensity_levels(field: ThinRadarField) -> np.ndarray:
    rates = field.rain_rate_mm_h
    eligible = (
        field.valid_mask
        & np.isfinite(rates)
        & (rates >= RAIN_RATE_LEVELS_MM_H[0])
    )
    levels = np.zeros(rates.shape, dtype=np.uint8)
    for intensity, threshold in enumerate(RAIN_RATE_LEVELS_MM_H, start=1):
        levels[eligible & (rates >= threshold)] = intensity
    return levels


def _rectangles_for_levels(
    levels: np.ndarray,
) -> list[tuple[int, int, int, int, int]]:
    """Merge equal horizontal pixel runs vertically into rectangles."""

    active: dict[tuple[int, int, int], int] = {}
    rectangles: list[tuple[int, int, int, int, int]] = []
    previous_row: int | None = None
    for row in range(levels.shape[0]):
        row_runs: set[tuple[int, int, int]] = set()
        column = 0
        while column < levels.shape[1]:
            intensity = int(levels[row, column])
            if intensity == 0:
                column += 1
                continue
            end = column + 1
            while end < levels.shape[1] and int(levels[row, end]) == intensity:
                end += 1
            row_runs.add((intensity, column, end))
            column = end

        if previous_row is not None and row != previous_row + 1:
            rectangles.extend(
                (start_row, previous_row + 1, run[1], run[2], run[0])
                for run, start_row in active.items()
            )
            active.clear()
        for run in tuple(active):
            if run not in row_runs:
                rectangles.append((
                    active.pop(run), row, run[1], run[2], run[0],
                ))
        for run in row_runs:
            active.setdefault(run, row)
        previous_row = row

    if previous_row is not None:
        rectangles.extend(
            (start_row, previous_row + 1, run[1], run[2], run[0])
            for run, start_row in active.items()
        )
    return rectangles


def _max_pool(levels: np.ndarray, factor: int) -> np.ndarray:
    """Coarsen display pixels while retaining every detected rain pixel."""

    rows, columns = levels.shape
    pooled_rows = (rows + factor - 1) // factor
    pooled_columns = (columns + factor - 1) // factor
    padded = np.zeros(
        (pooled_rows * factor, pooled_columns * factor), dtype=levels.dtype,
    )
    padded[:rows, :columns] = levels
    return padded.reshape(
        pooled_rows, factor, pooled_columns, factor,
    ).max(axis=(1, 3))


def build_radar_overlay(
    field: ThinRadarField,
    *,
    maximum_runs: int = 2_000,
) -> dict[str, Any]:
    """Encode visible rain as vertically merged same-intensity pixel runs.

    Rings deliberately use ``[latitude, longitude]`` because that is the
    established ``radar_overlays`` card contract, not GeoJSON geometry.
    """

    if maximum_runs < 1:
        raise ValueError("maximum_runs must be at least 1")

    source_levels = _intensity_levels(field)
    factor = 1
    levels = source_levels
    rectangles = _rectangles_for_levels(levels)
    while len(rectangles) > maximum_runs and (
        levels.shape[0] > 1 or levels.shape[1] > 1
    ):
        factor *= 2
        levels = _max_pool(source_levels, factor)
        rectangles = _rectangles_for_levels(levels)

    inverse = Transformer.from_crs(field.crs, "EPSG:4326", always_xy=True)
    runs = []
    for first_row, last_row, first_column, last_column, intensity in rectangles:
        left = field.origin_x_m + first_column * factor * field.x_resolution_m
        right = field.origin_x_m + min(
            last_column * factor, source_levels.shape[1],
        ) * field.x_resolution_m
        top = field.origin_y_m - first_row * factor * field.y_resolution_m
        bottom = field.origin_y_m - min(
            last_row * factor, source_levels.shape[0],
        ) * field.y_resolution_m
        ring = []
        for x_value, y_value in (
            (left, top),
            (right, top),
            (right, bottom),
            (left, bottom),
            (left, top),
        ):
            longitude, latitude = inverse.transform(x_value, y_value)
            ring.append([float(latitude), float(longitude)])
        runs.append({"intensity": intensity, "ring": ring})

    return {
        "provider_id": field.provider_id,
        "product_id": field.product_id,
        "observed_at": float(field.observed_at),
        "threshold_mm_h": RAIN_RATE_LEVELS_MM_H[0],
        "downsample_factor": factor,
        "source_shape": list(source_levels.shape),
        "runs": runs,
    }


__all__ = ["RAIN_RATE_LEVELS_MM_H", "build_radar_overlay"]
