"""Compact display-only radar overlays for the thin operational map."""

from __future__ import annotations

from dataclasses import replace
import math
from typing import Any, Iterable

import numpy as np
from pyproj import Transformer

from .thin_provider_adapters import ThinRadarField, ThinVisualRadarField


RAIN_RATE_LEVELS_MM_H = (0.1, 0.5, 1.0, 2.0, 4.0, 8.0, 16.0, 32.0)


def _intensity_levels(field: ThinRadarField | ThinVisualRadarField) -> np.ndarray:
    if isinstance(field, ThinVisualRadarField):
        return field.levels
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


def crop_visual_radar_field(field: ThinVisualRadarField, region: Any) -> ThinVisualRadarField | None:
    """Keep source pixels in this region, including Web-Mercator tiles."""
    radius = float(region.radar_analysis_radius_km or region.observation_radius_km)
    latitude = region.center_latitude
    longitude = region.center_longitude
    delta_lat = math.degrees(radius / 6371.0088)
    delta_lon = math.degrees(math.asin(min(
        1.0, math.sin(radius / 6371.0088) / max(0.001, math.cos(math.radians(latitude))),
    )))
    height, width = field.levels.shape
    left, bottom, right, top = longitude-delta_lon, latitude-delta_lat, longitude+delta_lon, latitude+delta_lat
    inverse = None
    if field.crs != "EPSG:4326":
        forward = Transformer.from_crs("EPSG:4326", field.crs, always_xy=True)
        inverse = Transformer.from_crs(field.crs, "EPSG:4326", always_xy=True)
        left, bottom, right, top = forward.transform_bounds(left, bottom, right, top, densify_pts=21)
    row0 = max(0, math.floor((field.latitude_top - top) / field.latitude_scale))
    row1 = min(height, math.ceil((field.latitude_top - bottom) / field.latitude_scale))
    col0 = max(0, math.floor((left - field.longitude_left) / field.longitude_scale))
    col1 = min(width, math.ceil((right - field.longitude_left) / field.longitude_scale))
    if row0 >= row1 or col0 >= col1:
        return None
    rows, columns = np.indices((row1 - row0, col1 - col0))
    ys = field.latitude_top - (rows + row0 + 0.5) * field.latitude_scale
    xs = field.longitude_left + (columns + col0 + 0.5) * field.longitude_scale
    lons, lats = (xs, ys) if inverse is None else inverse.transform(xs, ys)
    latitudes, longitudes = np.radians(lats), np.radians(lons)
    center_latitude = math.radians(latitude)
    haversine = (
        np.sin((latitudes - center_latitude) / 2) ** 2
        + np.cos(latitudes) * math.cos(center_latitude)
        * np.sin((longitudes - math.radians(longitude)) / 2) ** 2
    )
    within = 6371.0088 * 2 * np.arcsin(np.sqrt(np.clip(haversine, 0, 1))) <= radius
    return replace(
        field,
        longitude_left=field.longitude_left + col0 * field.longitude_scale,
        latitude_top=field.latitude_top - row0 * field.latitude_scale,
        levels=np.where(within, field.levels[row0:row1, col0:col1], 0).astype(np.uint8),
    )


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


def _overlay_at_factor(
    field: ThinRadarField | ThinVisualRadarField,
    source_levels: np.ndarray,
    factor: int,
    *,
    rectangles: list[tuple[int, int, int, int, int]] | None = None,
) -> dict[str, Any]:
    levels = source_levels if factor == 1 else _max_pool(source_levels, factor)
    if rectangles is None:
        rectangles = _rectangles_for_levels(levels)

    visual = isinstance(field, ThinVisualRadarField)
    inverse = (None if visual and field.crs == "EPSG:4326"
               else Transformer.from_crs(field.crs, "EPSG:4326", always_xy=True))
    origin_x = field.longitude_left if visual else field.origin_x_m
    origin_y = field.latitude_top if visual else field.origin_y_m
    x_resolution = field.longitude_scale if visual else field.x_resolution_m
    y_resolution = field.latitude_scale if visual else field.y_resolution_m
    runs = []
    for first_row, last_row, first_column, last_column, intensity in rectangles:
        left = origin_x + first_column * factor * x_resolution
        right = origin_x + min(
            last_column * factor, source_levels.shape[1],
        ) * x_resolution
        top = origin_y - first_row * factor * y_resolution
        bottom = origin_y - min(
            last_row * factor, source_levels.shape[0],
        ) * y_resolution
        ring = []
        for x_value, y_value in (
            (left, top),
            (right, top),
            (right, bottom),
            (left, bottom),
            (left, top),
        ):
            longitude, latitude = ((x_value, y_value) if inverse is None
                                   else inverse.transform(x_value, y_value))
            # Six decimals are substantially finer than the 500 m analysis
            # pixels and keep the shared MQTT payload compact.
            ring.append([round(float(latitude), 6), round(float(longitude), 6)])
        runs.append({"intensity": intensity, "ring": ring})

    return {
        "provider_id": field.provider_id,
        "product_id": field.product_id,
        "observed_at": float(field.observed_at),
        **({"quantity": "visual_class", "unit": "level"} if visual
           else {"threshold_mm_h": RAIN_RATE_LEVELS_MM_H[0]}),
        "downsample_factor": factor,
        "source_shape": list(source_levels.shape),
        "runs": runs,
    }


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

    return _overlay_at_factor(
        field,
        source_levels,
        factor,
        rectangles=rectangles,
    )


def build_radar_overlays(
    fields: Iterable[tuple[str, ThinRadarField | ThinVisualRadarField]],
    *,
    maximum_total_runs: int = 4_000,
) -> dict[str, dict[str, Any]]:
    """Encode all regional overlays at one shared, minimally coarse factor.

    The former equal split of the run budget caused a hard quality cliff: one
    slightly more fragmented region was downsampled while its neighbour kept
    native detail.  Treat the budget as a total and only coarsen every region
    together when the combined native representation does not fit.
    """

    if maximum_total_runs < 1:
        raise ValueError("maximum_total_runs must be at least 1")
    prepared = tuple(
        (region_id, field, _intensity_levels(field))
        for region_id, field in fields
    )
    factor = 1
    rectangles_by_region: dict[
        str, list[tuple[int, int, int, int, int]]
    ] = {}
    while True:
        rectangles_by_region = {
            region_id: _rectangles_for_levels(
                levels if factor == 1 else _max_pool(levels, factor)
            )
            for region_id, _field, levels in prepared
        }
        total = sum(len(value) for value in rectangles_by_region.values())
        can_coarsen = any(
            (levels.shape[0] + factor - 1) // factor > 1
            or (levels.shape[1] + factor - 1) // factor > 1
            for _region_id, _field, levels in prepared
        )
        if total <= maximum_total_runs or not can_coarsen:
            break
        factor *= 2

    return {
        region_id: _overlay_at_factor(
            field,
            levels,
            factor,
            rectangles=rectangles_by_region[region_id],
        )
        for region_id, field, levels in prepared
    }


__all__ = [
    "RAIN_RATE_LEVELS_MM_H",
    "build_radar_overlay",
    "build_radar_overlays",
]
