from __future__ import annotations

import math

import numpy as np
import pytest

from storm_tracker_worker.desired_state import Target
from storm_tracker_worker.thin_provider_adapters import ThinRadarField
from storm_tracker_worker.thin_region_planner import ThinAcquisitionRegion
from storm_tracker_worker.thin_regional_radar_fusion import (
    build_regional_analysis_grid,
    project_radar_field,
)
from storm_tracker_worker.tracking_core.engine.analysis_grid import AnalysisGrid


def _region(
    *,
    latitude: float = 0.0,
    longitude: float = 0.0,
    radius_km: float = 2.0,
) -> ThinAcquisitionRegion:
    target = Target(
        "home", "Home", "home", True, latitude, longitude,
        1_000.0, 5.0, radius_km, "BE",
    )
    return ThinAcquisitionRegion(
        region_id="region:home",
        kind="home",
        center_latitude=latitude,
        center_longitude=longitude,
        observation_radius_km=radius_km,
        targets=(target,),
        stale_target_ids=(),
    )


def _field(
    *,
    provider: str = "radar-a",
    crs: str = "EPSG:3857",
    resolution: float = 1_000.0,
    origin: tuple[float, float] = (-2_000.0, 2_000.0),
    values: np.ndarray | None = None,
    valid: np.ndarray | None = None,
) -> ThinRadarField:
    values = (
        np.ones((4, 4), dtype=float)
        if values is None
        else np.asarray(values, dtype=float)
    )
    valid = (
        np.ones(values.shape, dtype=bool)
        if valid is None
        else np.asarray(valid, dtype=bool)
    )
    return ThinRadarField(
        provider, "rain-rate", 1_000.0, crs,
        origin[0], origin[1], resolution, resolution,
        values, valid,
    )


def _grid(
    *,
    width: int,
    height: int,
    west: float,
    north: float,
    resolution: float,
    crs: str = "EPSG:3857",
) -> AnalysisGrid:
    return AnalysisGrid(
        grid_id="test-grid",
        crs=crs,
        origin_x_m=west,
        origin_y_m=north,
        x_resolution_m=resolution,
        y_resolution_m=resolution,
        width=width,
        height=height,
    )


def test_region_grid_is_metric_stable_and_capacity_bounded():
    region = _region(latitude=50.85, longitude=4.35, radius_km=80.0)
    fine = _field(crs="EPSG:3857", resolution=250.0)

    first = build_regional_analysis_grid(region, (fine,))
    second = build_regional_analysis_grid(region, (fine,))

    assert first == second
    assert first.crs.startswith("+proj=aeqd")
    assert first.x_resolution_m >= 500.0
    assert first.width * first.height <= 250_000
    assert first.extent_edges_m[0] <= -80_000.0
    assert first.extent_edges_m[2] >= 80_000.0


@pytest.mark.parametrize(
    "longitude,latitude",
    [(179.95, 10.0), (-179.95, 10.0), (20.0, 75.0)],
)
def test_region_grid_handles_dateline_and_high_latitude(longitude, latitude):
    grid = build_regional_analysis_grid(
        _region(latitude=latitude, longitude=longitude, radius_km=50.0),
        (_field(),),
    )

    assert all(math.isfinite(value) for value in grid.extent_edges_m)
    assert grid.width * grid.height <= 250_000


def test_projection_keeps_zero_nodata_and_outside_distinct():
    field = _field(
        values=np.array([[0.0, 4.0], [8.0, 0.0]]),
        valid=np.array([[True, False], [True, True]]),
        origin=(-1_000.0, 1_000.0),
        resolution=1_000.0,
    )
    grid = _grid(
        width=3, height=2, west=-1_000.0, north=1_000.0,
        resolution=1_000.0,
    )

    layer = project_radar_field(field, grid)

    assert layer.support_mask[0, 0]
    assert layer.valid_mask[0, 0]
    assert layer.rain_rate_mm_h[0, 0] == 0.0
    assert layer.support_mask[0, 1]
    assert not layer.valid_mask[0, 1]
    assert not layer.support_mask[0, 2]


def test_projection_conserves_constant_rate_across_resolution_change():
    layer = project_radar_field(
        _field(
            values=np.full((4, 4), 6.0),
            valid=np.ones((4, 4), bool),
            origin=(0.0, 2_000.0),
            resolution=500.0,
        ),
        _grid(
            width=2, height=2, west=0.0, north=2_000.0,
            resolution=1_000.0,
        ),
    )

    assert np.allclose(layer.rain_rate_mm_h[layer.valid_mask], 6.0)
    assert np.allclose(layer.coverage_fraction[layer.valid_mask], 1.0)


def test_reprojected_object_stays_within_one_analysis_pixel():
    values = np.zeros((4, 4), dtype=float)
    values[1, 1] = 10.0
    source = _field(
        crs="EPSG:3857",
        values=values,
        origin=(-2_000.0, 2_000.0),
        resolution=1_000.0,
    )
    grid = build_regional_analysis_grid(_region(radius_km=2.0), (source,))

    layer = project_radar_field(source, grid)

    row, column = np.unravel_index(
        np.nanargmax(layer.rain_rate_mm_h),
        layer.rain_rate_mm_h.shape,
    )
    x_m, y_m = grid.pixel_center(int(row), int(column))
    assert math.hypot(x_m + 500.0, y_m - 500.0) <= grid.x_resolution_m
