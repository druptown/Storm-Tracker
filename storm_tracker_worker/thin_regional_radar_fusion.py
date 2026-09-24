"""Geographically correct regional radar grids for the thin runtime."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import math
from typing import Iterable

import numpy as np
from pyproj import CRS, Transformer

from .thin_provider_adapters import ThinRadarField
from .thin_region_planner import ThinAcquisitionRegion
from .tracking_core.engine.analysis_grid import AnalysisGrid


MINIMUM_ANALYSIS_RESOLUTION_M = 500.0
MAXIMUM_ANALYSIS_CELLS = 250_000
_AREA_EPSILON_M2 = 1e-6


class RegionalRadarProjectionError(RuntimeError):
    """One source field cannot be projected under the thin metric contract."""


@dataclass(frozen=True, slots=True)
class RegionalRadarLayer:
    provider_id: str
    product_id: str
    observed_at: float
    grid: AnalysisGrid
    rain_rate_mm_h: np.ndarray
    valid_mask: np.ndarray
    support_mask: np.ndarray
    coverage_fraction: np.ndarray
    native_resolution_m: float

    def __post_init__(self) -> None:
        shape = (self.grid.height, self.grid.width)
        arrays = (
            self.rain_rate_mm_h,
            self.valid_mask,
            self.support_mask,
            self.coverage_fraction,
        )
        if any(not isinstance(value, np.ndarray) or value.shape != shape for value in arrays):
            raise ValueError("regional_radar_layer_shape_invalid")
        if not math.isfinite(self.native_resolution_m) or self.native_resolution_m <= 0.0:
            raise ValueError("regional_radar_native_resolution_invalid")


def _canonical_hash(value: object) -> str:
    payload = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _metric_projected_crs(value: str) -> CRS:
    try:
        crs = CRS.from_user_input(value)
    except Exception as error:
        raise RegionalRadarProjectionError("unsupported_metric_crs") from error
    if (
        not crs.is_projected
        or not crs.axis_info
        or not math.isclose(
            float(crs.axis_info[0].unit_conversion_factor),
            1.0,
            rel_tol=0.0,
            abs_tol=1e-12,
        )
    ):
        raise RegionalRadarProjectionError("unsupported_metric_crs")
    return crs


def build_regional_analysis_grid(
    region: ThinAcquisitionRegion,
    fields: Iterable[ThinRadarField],
    *,
    minimum_resolution_m: float = MINIMUM_ANALYSIS_RESOLUTION_M,
    maximum_cells: int = MAXIMUM_ANALYSIS_CELLS,
) -> AnalysisGrid:
    """Build one stable local metric grid covering the complete region."""
    if not isinstance(region, ThinAcquisitionRegion):
        raise TypeError("region_must_be_thin_acquisition_region")
    if not math.isfinite(minimum_resolution_m) or minimum_resolution_m <= 0.0:
        raise ValueError("minimum_resolution_m_invalid")
    if isinstance(maximum_cells, bool) or not isinstance(maximum_cells, int) or maximum_cells < 1:
        raise ValueError("maximum_cells_invalid")
    radius_m = float(region.observation_radius_km) * 1_000.0
    if not math.isfinite(radius_m) or radius_m <= 0.0:
        raise ValueError("region_radius_invalid")

    usable = [
        min(float(field.x_resolution_m), float(field.y_resolution_m))
        for field in fields
        if (
            math.isfinite(float(field.x_resolution_m))
            and math.isfinite(float(field.y_resolution_m))
            and field.x_resolution_m > 0.0
            and field.y_resolution_m > 0.0
        )
    ]
    resolution = max(
        float(minimum_resolution_m),
        min(usable) if usable else float(minimum_resolution_m),
    )
    span_m = radius_m * 2.0
    width = max(1, math.ceil(span_m / resolution))
    height = width
    while width * height > maximum_cells:
        factor = max(2, math.ceil(math.sqrt((width * height) / maximum_cells)))
        resolution *= factor
        width = max(1, math.ceil(span_m / resolution))
        height = width

    covered_span_m = width * resolution
    origin_x_m = -covered_span_m / 2.0
    origin_y_m = covered_span_m / 2.0
    crs = (
        f"+proj=aeqd +lat_0={region.center_latitude:.8f} "
        f"+lon_0={region.center_longitude:.8f} "
        "+datum=WGS84 +units=m +no_defs"
    )
    identity = {
        "region_id": region.region_id,
        "center_latitude": round(float(region.center_latitude), 8),
        "center_longitude": round(float(region.center_longitude), 8),
        "observation_radius_km": round(float(region.observation_radius_km), 6),
        "crs": crs,
        "resolution_m": resolution,
        "width": width,
        "height": height,
    }
    return AnalysisGrid(
        grid_id="regional-grid:" + _canonical_hash(identity),
        crs=crs,
        origin_x_m=origin_x_m,
        origin_y_m=origin_y_m,
        x_resolution_m=resolution,
        y_resolution_m=resolution,
        width=width,
        height=height,
    )


def _clip_axis(
    polygon: list[tuple[float, float]],
    *,
    axis: int,
    boundary: float,
    keep_greater: bool,
) -> list[tuple[float, float]]:
    if not polygon:
        return []

    def inside(point: tuple[float, float]) -> bool:
        return point[axis] >= boundary if keep_greater else point[axis] <= boundary

    def intersection(
        start: tuple[float, float],
        end: tuple[float, float],
    ) -> tuple[float, float]:
        delta = end[axis] - start[axis]
        if abs(delta) <= 1e-15:
            return start
        fraction = (boundary - start[axis]) / delta
        return (
            start[0] + fraction * (end[0] - start[0]),
            start[1] + fraction * (end[1] - start[1]),
        )

    output: list[tuple[float, float]] = []
    previous = polygon[-1]
    previous_inside = inside(previous)
    for current in polygon:
        current_inside = inside(current)
        if current_inside:
            if not previous_inside:
                output.append(intersection(previous, current))
            output.append(current)
        elif previous_inside:
            output.append(intersection(previous, current))
        previous = current
        previous_inside = current_inside
    return output


def _clip_rectangle(
    polygon: list[tuple[float, float]],
    *,
    west: float,
    south: float,
    east: float,
    north: float,
) -> list[tuple[float, float]]:
    clipped = _clip_axis(polygon, axis=0, boundary=west, keep_greater=True)
    clipped = _clip_axis(clipped, axis=0, boundary=east, keep_greater=False)
    clipped = _clip_axis(clipped, axis=1, boundary=south, keep_greater=True)
    return _clip_axis(clipped, axis=1, boundary=north, keep_greater=False)


def _polygon_area(polygon: list[tuple[float, float]]) -> float:
    if len(polygon) < 3:
        return 0.0
    return abs(sum(
        first[0] * second[1] - second[0] * first[1]
        for first, second in zip(polygon, polygon[1:] + polygon[:1])
    )) * 0.5


def _target_boundary_samples(grid: AnalysisGrid) -> tuple[np.ndarray, np.ndarray]:
    west, south, east, north = grid.extent_edges_m
    fractions = np.linspace(0.0, 1.0, 9)
    x_values = np.concatenate((
        west + fractions * (east - west),
        west + fractions * (east - west),
        np.full(fractions.shape, west),
        np.full(fractions.shape, east),
    ))
    y_values = np.concatenate((
        np.full(fractions.shape, north),
        np.full(fractions.shape, south),
        south + fractions * (north - south),
        south + fractions * (north - south),
    ))
    return x_values, y_values


def _source_window(
    field: ThinRadarField,
    grid: AnalysisGrid,
    to_source: Transformer,
) -> tuple[int, int, int, int] | None:
    boundary_x, boundary_y = _target_boundary_samples(grid)
    source_x, source_y = to_source.transform(boundary_x, boundary_y)
    if not np.all(np.isfinite(source_x)) or not np.all(np.isfinite(source_y)):
        raise RegionalRadarProjectionError("coordinate_transform_failed")
    height, width = field.rain_rate_mm_h.shape
    first_column = math.floor(
        (float(np.min(source_x)) - field.origin_x_m) / field.x_resolution_m
    ) - 2
    last_column = math.ceil(
        (float(np.max(source_x)) - field.origin_x_m) / field.x_resolution_m
    ) + 2
    first_row = math.floor(
        (field.origin_y_m - float(np.max(source_y))) / field.y_resolution_m
    ) - 2
    last_row = math.ceil(
        (field.origin_y_m - float(np.min(source_y))) / field.y_resolution_m
    ) + 2
    first_column = max(0, first_column)
    first_row = max(0, first_row)
    last_column = min(width, last_column)
    last_row = min(height, last_row)
    if first_column >= last_column or first_row >= last_row:
        return None
    return first_row, last_row, first_column, last_column


def project_radar_field(
    field: ThinRadarField,
    grid: AnalysisGrid,
) -> RegionalRadarLayer:
    """Conservatively project one native rain-rate field onto one region."""
    source_crs = _metric_projected_crs(field.crs)
    target_crs = _metric_projected_crs(grid.crs)
    to_source = Transformer.from_crs(target_crs, source_crs, always_xy=True)
    to_target = Transformer.from_crs(source_crs, target_crs, always_xy=True)
    shape = (grid.height, grid.width)
    support_area = np.zeros(shape, dtype=np.float64)
    valid_area = np.zeros(shape, dtype=np.float64)
    rain_integral = np.zeros(shape, dtype=np.float64)
    source_window = _source_window(field, grid, to_source)
    if source_window is not None:
        first_row, last_row, first_column, last_column = source_window
        source_x_edges = field.origin_x_m + np.arange(
            first_column, last_column + 1, dtype=float,
        ) * field.x_resolution_m
        source_y_edges = field.origin_y_m - np.arange(
            first_row, last_row + 1, dtype=float,
        ) * field.y_resolution_m
        source_x_mesh, source_y_mesh = np.meshgrid(source_x_edges, source_y_edges)
        target_x_mesh, target_y_mesh = to_target.transform(source_x_mesh, source_y_mesh)
        if not np.all(np.isfinite(target_x_mesh)) or not np.all(np.isfinite(target_y_mesh)):
            raise RegionalRadarProjectionError("coordinate_transform_failed")

        grid_west, grid_south, grid_east, grid_north = grid.extent_edges_m
        for local_row, source_row in enumerate(range(first_row, last_row)):
            for local_column, source_column in enumerate(range(first_column, last_column)):
                polygon = [
                    (float(target_x_mesh[local_row, local_column]), float(target_y_mesh[local_row, local_column])),
                    (float(target_x_mesh[local_row, local_column + 1]), float(target_y_mesh[local_row, local_column + 1])),
                    (float(target_x_mesh[local_row + 1, local_column + 1]), float(target_y_mesh[local_row + 1, local_column + 1])),
                    (float(target_x_mesh[local_row + 1, local_column]), float(target_y_mesh[local_row + 1, local_column])),
                ]
                minimum_x = min(point[0] for point in polygon)
                maximum_x = max(point[0] for point in polygon)
                minimum_y = min(point[1] for point in polygon)
                maximum_y = max(point[1] for point in polygon)
                if (
                    maximum_x <= grid_west
                    or minimum_x >= grid_east
                    or maximum_y <= grid_south
                    or minimum_y >= grid_north
                ):
                    continue
                first_target_column = max(
                    0,
                    math.floor((minimum_x - grid.origin_x_m) / grid.x_resolution_m),
                )
                last_target_column = min(
                    grid.width - 1,
                    math.ceil((maximum_x - grid.origin_x_m) / grid.x_resolution_m) - 1,
                )
                first_target_row = max(
                    0,
                    math.floor((grid.origin_y_m - maximum_y) / grid.y_resolution_m),
                )
                last_target_row = min(
                    grid.height - 1,
                    math.ceil((grid.origin_y_m - minimum_y) / grid.y_resolution_m) - 1,
                )
                for target_row in range(first_target_row, last_target_row + 1):
                    north = grid.origin_y_m - target_row * grid.y_resolution_m
                    south = north - grid.y_resolution_m
                    for target_column in range(first_target_column, last_target_column + 1):
                        west = grid.origin_x_m + target_column * grid.x_resolution_m
                        east = west + grid.x_resolution_m
                        overlap = _polygon_area(_clip_rectangle(
                            polygon,
                            west=west,
                            south=south,
                            east=east,
                            north=north,
                        ))
                        if overlap <= _AREA_EPSILON_M2:
                            continue
                        support_area[target_row, target_column] += overlap
                        if bool(field.valid_mask[source_row, source_column]):
                            valid_area[target_row, target_column] += overlap
                            rain_integral[target_row, target_column] += overlap * float(
                                field.rain_rate_mm_h[source_row, source_column]
                            )

    center_columns = np.arange(grid.width, dtype=float) + 0.5
    center_rows = np.arange(grid.height, dtype=float) + 0.5
    target_center_x, target_center_y = np.meshgrid(
        grid.origin_x_m + center_columns * grid.x_resolution_m,
        grid.origin_y_m - center_rows * grid.y_resolution_m,
    )
    nearest_x, nearest_y = to_source.transform(target_center_x, target_center_y)
    nearest_columns = np.floor(
        (nearest_x - field.origin_x_m) / field.x_resolution_m
    ).astype(np.int64)
    nearest_rows = np.floor(
        (field.origin_y_m - nearest_y) / field.y_resolution_m
    ).astype(np.int64)
    source_height, source_width = field.rain_rate_mm_h.shape
    nearest_in_bounds = (
        (nearest_rows >= 0)
        & (nearest_rows < source_height)
        & (nearest_columns >= 0)
        & (nearest_columns < source_width)
    )
    nearest_valid = np.zeros(shape, dtype=bool)
    nearest_valid[nearest_in_bounds] = field.valid_mask[
        nearest_rows[nearest_in_bounds],
        nearest_columns[nearest_in_bounds],
    ]
    support_mask = support_area > _AREA_EPSILON_M2
    valid_mask = nearest_valid & (valid_area > _AREA_EPSILON_M2)
    rain_rate = np.full(shape, np.nan, dtype=np.float64)
    rain_rate[valid_mask] = (
        rain_integral[valid_mask] / valid_area[valid_mask]
    )
    coverage_fraction = np.clip(valid_area / grid.pixel_area_m2, 0.0, 1.0)
    return RegionalRadarLayer(
        provider_id=field.provider_id,
        product_id=field.product_id,
        observed_at=float(field.observed_at),
        grid=grid,
        rain_rate_mm_h=rain_rate,
        valid_mask=valid_mask,
        support_mask=support_mask,
        coverage_fraction=coverage_fraction,
        native_resolution_m=min(field.x_resolution_m, field.y_resolution_m),
    )


__all__ = [
    "MAXIMUM_ANALYSIS_CELLS",
    "MINIMUM_ANALYSIS_RESOLUTION_M",
    "RegionalRadarLayer",
    "RegionalRadarProjectionError",
    "build_regional_analysis_grid",
    "project_radar_field",
]
