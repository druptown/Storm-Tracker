"""Geographically correct regional radar grids for the thin runtime."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import math
from types import MappingProxyType
from typing import Iterable, Mapping

import numpy as np
from pyproj import CRS, Transformer

from .thin_provider_adapters import ThinRadarField
from .thin_provider_runtime import ProviderResult
from .thin_region_planner import ThinAcquisitionRegion
from .thin_fusion import (
    MAX_FUSION_AGE_SECONDS,
    MAX_FUTURE_SKEW_SECONDS,
    PROVIDER_FUSION_AGE_SECONDS,
)
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


@dataclass(frozen=True, slots=True)
class RegionalFusionSummary:
    """Compact, transport-safe provenance for one regional fusion result."""

    region_id: str
    grid_id: str | None
    status: str
    mode: str
    evaluated_at: float
    field_observed_at: float | None
    contributors: tuple[str, ...]
    excluded: Mapping[str, str]
    standby: Mapping[str, str]
    reason_codes: tuple[str, ...]
    coverage_fraction: float
    mean_confidence: float
    disagreement_fraction: float

    def as_dict(self) -> dict[str, object]:
        return {
            "region_id": self.region_id,
            "grid_id": self.grid_id,
            "status": self.status,
            "mode": self.mode,
            "evaluated_at": self.evaluated_at,
            "field_observed_at": self.field_observed_at,
            "contributors": list(self.contributors),
            "excluded": dict(self.excluded),
            "standby": dict(self.standby),
            "reason_codes": list(self.reason_codes),
            "coverage_fraction": self.coverage_fraction,
            "mean_confidence": self.mean_confidence,
            "disagreement_fraction": self.disagreement_fraction,
        }


@dataclass(frozen=True, slots=True)
class FusedRegionalRadarField:
    region_id: str
    field: ThinRadarField | None
    confidence: np.ndarray
    contributor_mask: np.ndarray
    summary: RegionalFusionSummary


def _empty_fusion(
    region: ThinAcquisitionRegion,
    *,
    evaluated_at: float,
    excluded: Mapping[str, str],
    grid: AnalysisGrid | None = None,
) -> FusedRegionalRadarField:
    shape = (0, 0) if grid is None else (grid.height, grid.width)
    summary = RegionalFusionSummary(
        region_id=region.region_id,
        grid_id=None if grid is None else grid.grid_id,
        status="onvoldoende_data",
        mode="fallback",
        evaluated_at=evaluated_at,
        field_observed_at=None,
        contributors=(),
        excluded=MappingProxyType(dict(sorted(excluded.items()))),
        standby=MappingProxyType({}),
        reason_codes=("no_usable_quantitative_radar",),
        coverage_fraction=0.0,
        mean_confidence=0.0,
        disagreement_fraction=0.0,
    )
    return FusedRegionalRadarField(
        region_id=region.region_id,
        field=None,
        confidence=np.zeros(shape, dtype=np.float64),
        contributor_mask=np.zeros(shape, dtype=np.uint64),
        summary=summary,
    )


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
    analysis_radius_km = (
        region.observation_radius_km
        if region.radar_analysis_radius_km is None
        else region.radar_analysis_radius_km
    )
    radius_m = float(analysis_radius_km) * 1_000.0
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
        "radar_analysis_radius_km": round(float(analysis_radius_km), 6),
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
    """Vector-project one native rain-rate field onto one region.

    Sampling the complete target grid in one pyproj/NumPy pass avoids the
    former source-pixel polygon loop.  The nearest native pixel remains the
    validity authority (so nodata cannot bleed into the fused field), while
    valid neighbours are bilinearly weighted for a stable rain rate.
    """
    source_crs = _metric_projected_crs(field.crs)
    target_crs = _metric_projected_crs(grid.crs)
    to_source = Transformer.from_crs(target_crs, source_crs, always_xy=True)
    shape = (grid.height, grid.width)
    center_columns = np.arange(grid.width, dtype=float) + 0.5
    center_rows = np.arange(grid.height, dtype=float) + 0.5
    target_center_x, target_center_y = np.meshgrid(
        grid.origin_x_m + center_columns * grid.x_resolution_m,
        grid.origin_y_m - center_rows * grid.y_resolution_m,
    )
    nearest_x, nearest_y = to_source.transform(target_center_x, target_center_y)
    if not np.all(np.isfinite(nearest_x)) or not np.all(np.isfinite(nearest_y)):
        raise RegionalRadarProjectionError("coordinate_transform_failed")

    source_columns = (
        (nearest_x - field.origin_x_m) / field.x_resolution_m - 0.5
    )
    source_rows = (
        (field.origin_y_m - nearest_y) / field.y_resolution_m - 0.5
    )
    lower_columns = np.floor(source_columns).astype(np.int64)
    lower_rows = np.floor(source_rows).astype(np.int64)
    column_fraction = np.clip(source_columns - lower_columns, 0.0, 1.0)
    row_fraction = np.clip(source_rows - lower_rows, 0.0, 1.0)

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

    valid_weight = np.zeros(shape, dtype=np.float64)
    rain_weighted = np.zeros(shape, dtype=np.float64)
    for row_offset, column_offset, weights in (
        (0, 0, (1.0 - row_fraction) * (1.0 - column_fraction)),
        (0, 1, (1.0 - row_fraction) * column_fraction),
        (1, 0, row_fraction * (1.0 - column_fraction)),
        (1, 1, row_fraction * column_fraction),
    ):
        rows = lower_rows + row_offset
        columns = lower_columns + column_offset
        in_bounds = (
            (rows >= 0)
            & (rows < source_height)
            & (columns >= 0)
            & (columns < source_width)
        )
        safe_rows = np.clip(rows, 0, source_height - 1)
        safe_columns = np.clip(columns, 0, source_width - 1)
        source_valid = in_bounds & field.valid_mask[safe_rows, safe_columns]
        effective_weight = np.where(source_valid, weights, 0.0)
        valid_weight += effective_weight
        rain_weighted += effective_weight * np.where(
            source_valid,
            field.rain_rate_mm_h[safe_rows, safe_columns],
            0.0,
        )

    support_mask = nearest_in_bounds
    valid_mask = nearest_valid & (valid_weight > _AREA_EPSILON_M2)
    rain_rate = np.full(shape, np.nan, dtype=np.float64)
    rain_rate[valid_mask] = rain_weighted[valid_mask] / valid_weight[valid_mask]
    coverage_fraction = np.clip(valid_weight, 0.0, 1.0)
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


def _weighted_median(values: np.ndarray, weights: np.ndarray) -> float:
    order = np.argsort(values, kind="stable")
    ordered_values = values[order]
    ordered_weights = weights[order]
    threshold = float(np.sum(ordered_weights)) * 0.5
    index = int(np.searchsorted(np.cumsum(ordered_weights), threshold, side="left"))
    return float(ordered_values[min(index, len(ordered_values) - 1)])


def fuse_regional_radar_fields(
    region: ThinAcquisitionRegion,
    provider_results: Mapping[str, ProviderResult],
    *,
    evaluated_at: float,
) -> FusedRegionalRadarField:
    """Fuse at most the three best quantitative radar sources per pixel."""
    evaluated = float(evaluated_at)
    if not math.isfinite(evaluated):
        raise ValueError("evaluated_at_invalid")

    excluded: dict[str, str] = {}
    candidates: list[ThinRadarField] = []
    for mapping_id, result in sorted(provider_results.items()):
        provider_id = str(mapping_id)
        if not isinstance(result, ProviderResult):
            excluded[provider_id] = "invalid_provider_result"
            continue
        provider_id = result.provider_id
        if not result.ready or result.status != "operational":
            excluded[provider_id] = "runtime_not_operational"
            continue
        matching_fields = [
            field for field in result.radar_fields
            if field.provider_id == provider_id
        ]
        if not matching_fields:
            excluded[provider_id] = (
                "provider_mismatch" if result.radar_fields
                else "quantitative_field_unavailable"
            )
            continue
        matching_fields.sort(key=lambda field: field.observed_at, reverse=True)
        field = matching_fields[0]
        age = evaluated - float(field.observed_at)
        maximum_age = float(PROVIDER_FUSION_AGE_SECONDS.get(
            provider_id, MAX_FUSION_AGE_SECONDS,
        ))
        if age > maximum_age:
            excluded[provider_id] = "field_stale"
            continue
        if age < -MAX_FUTURE_SKEW_SECONDS:
            excluded[provider_id] = "field_from_future"
            continue
        candidates.append(field)

    if not candidates:
        return _empty_fusion(
            region,
            evaluated_at=evaluated,
            excluded=excluded,
        )

    grid = build_regional_analysis_grid(region, candidates)
    layers: list[RegionalRadarLayer] = []
    maximum_ages: list[float] = []
    for field in candidates:
        try:
            layer = project_radar_field(field, grid)
        except RegionalRadarProjectionError as error:
            excluded[field.provider_id] = str(error)
            continue
        if not np.any(layer.support_mask):
            excluded[field.provider_id] = "outside_region_coverage"
            continue
        if not np.any(layer.valid_mask):
            excluded[field.provider_id] = "no_valid_region_data"
            continue
        layers.append(layer)
        maximum_ages.append(float(PROVIDER_FUSION_AGE_SECONDS.get(
            field.provider_id, MAX_FUSION_AGE_SECONDS,
        )))

    if not layers:
        return _empty_fusion(
            region,
            evaluated_at=evaluated,
            excluded=excluded,
            grid=grid,
        )
    if len(layers) > 64:
        raise ValueError("regional_fusion_supports_at_most_64_sources")

    ordered_layers = sorted(
        zip(layers, maximum_ages),
        key=lambda item: (-item[0].observed_at, item[0].provider_id),
    )
    layers = [item[0] for item in ordered_layers]
    maximum_ages = [item[1] for item in ordered_layers]

    shape = (grid.height, grid.width)
    source_weights = np.zeros((len(layers), *shape), dtype=np.float64)
    source_values = np.full((len(layers), *shape), np.nan, dtype=np.float64)
    for index, (layer, maximum_age) in enumerate(zip(layers, maximum_ages)):
        age_seconds = max(0.0, evaluated - layer.observed_at)
        age_factor = max(0.10, 1.0 - age_seconds / maximum_age)
        resolution_factor = min(
            1.0,
            grid.x_resolution_m / layer.native_resolution_m,
        )
        weights = (
            age_factor
            * resolution_factor
            * np.clip(layer.coverage_fraction, 0.0, 1.0)
        )
        source_weights[index, layer.valid_mask] = weights[layer.valid_mask]
        source_values[index, layer.valid_mask] = layer.rain_rate_mm_h[layer.valid_mask]

    top_count = min(3, len(layers))
    ranked_indices = np.argsort(
        -source_weights,
        axis=0,
        kind="stable",
    )[:top_count]
    ranked_weights = np.take_along_axis(
        source_weights, ranked_indices, axis=0,
    )
    ranked_values = np.take_along_axis(
        source_values, ranked_indices, axis=0,
    )
    ranked_valid = ranked_weights > 0.0
    total_weight = np.sum(ranked_weights, axis=0)
    fused_valid = total_weight > 0.0

    sortable_values = np.where(ranked_valid, ranked_values, np.inf)
    value_order = np.argsort(sortable_values, axis=0, kind="stable")
    sorted_values = np.take_along_axis(sortable_values, value_order, axis=0)
    sorted_weights = np.take_along_axis(ranked_weights, value_order, axis=0)
    cumulative_weight = np.cumsum(sorted_weights, axis=0)
    median_rank = np.argmax(
        cumulative_weight >= (total_weight[np.newaxis, ...] * 0.5),
        axis=0,
    )
    median = np.take_along_axis(
        sorted_values,
        median_rank[np.newaxis, ...],
        axis=0,
    )[0]
    median = np.where(fused_valid, median, 0.0)
    distance = np.maximum(5.0, 3.0 * np.maximum(1.0, median))
    clipped = np.minimum(
        np.maximum(ranked_values, np.maximum(0.0, median - distance)),
        median + distance,
    )
    disagreement = np.any(
        ranked_valid & (clipped != ranked_values),
        axis=0,
    )
    weighted_sum = np.sum(
        np.where(ranked_valid, clipped * ranked_weights, 0.0),
        axis=0,
    )
    fused_values = np.full(shape, np.nan, dtype=np.float64)
    fused_values[fused_valid] = (
        weighted_sum[fused_valid] / total_weight[fused_valid]
    )
    confidence = np.minimum(1.0, total_weight / 3.0)
    contributor_mask = np.zeros(shape, dtype=np.uint64)
    for rank in range(top_count):
        rank_indices = ranked_indices[rank].astype(np.uint64)
        rank_valid = ranked_valid[rank]
        bits = np.left_shift(np.uint64(1), rank_indices)
        contributor_mask[rank_valid] |= bits[rank_valid]

    selected_provider_ids = {
        layer.provider_id
        for index, layer in enumerate(layers)
        if np.any(ranked_valid & (ranked_indices == index))
    }

    if not np.any(fused_valid):
        return _empty_fusion(
            region,
            evaluated_at=evaluated,
            excluded=excluded,
            grid=grid,
        )

    contributors = tuple(sorted(selected_provider_ids))
    standby = {
        layer.provider_id: "lower_ranked_standby"
        for layer in layers
        if layer.provider_id not in selected_provider_ids
    }
    valid_count = int(np.count_nonzero(fused_valid))
    coverage_fraction = valid_count / fused_valid.size
    disagreement_fraction = (
        float(np.count_nonzero(disagreement & fused_valid)) / valid_count
    )
    reason_codes: list[str] = []
    if coverage_fraction < 0.95:
        reason_codes.append("partial_region_coverage")
    if disagreement_fraction > 0.0:
        reason_codes.append("provider_disagreement")
    if len(contributors) == 1:
        reason_codes.append("single_source")
    status = "current" if not reason_codes else "degraded"
    mode = "fused" if len(contributors) > 1 else "single_source"
    field_observed_at = max(
        layer.observed_at
        for layer in layers
        if layer.provider_id in selected_provider_ids
    )
    field = ThinRadarField(
        provider_id=f"fusion:{region.region_id}",
        product_id="regional-rain-rate",
        observed_at=field_observed_at,
        crs=grid.crs,
        origin_x_m=grid.origin_x_m,
        origin_y_m=grid.origin_y_m,
        x_resolution_m=grid.x_resolution_m,
        y_resolution_m=grid.y_resolution_m,
        rain_rate_mm_h=fused_values,
        valid_mask=fused_valid,
    )
    summary = RegionalFusionSummary(
        region_id=region.region_id,
        grid_id=grid.grid_id,
        status=status,
        mode=mode,
        evaluated_at=evaluated,
        field_observed_at=field_observed_at,
        contributors=contributors,
        excluded=MappingProxyType(dict(sorted(excluded.items()))),
        standby=MappingProxyType(dict(sorted(standby.items()))),
        reason_codes=tuple(reason_codes),
        coverage_fraction=coverage_fraction,
        mean_confidence=float(np.mean(confidence[fused_valid])),
        disagreement_fraction=disagreement_fraction,
    )
    return FusedRegionalRadarField(
        region_id=region.region_id,
        field=field,
        confidence=confidence,
        contributor_mask=contributor_mask,
        summary=summary,
    )


__all__ = [
    "FusedRegionalRadarField",
    "MAXIMUM_ANALYSIS_CELLS",
    "MINIMUM_ANALYSIS_RESOLUTION_M",
    "RegionalFusionSummary",
    "RegionalRadarLayer",
    "RegionalRadarProjectionError",
    "build_regional_analysis_grid",
    "fuse_regional_radar_fields",
    "project_radar_field",
]
