from __future__ import annotations

import math

import numpy as np
import pytest

import storm_tracker_worker.thin_regional_radar_fusion as radar_fusion_module
from storm_tracker_worker.desired_state import DesiredState, RuntimeSettings, Target
from storm_tracker_worker.thin_provider_adapters import RadarFieldIdentity, ThinRadarField
from storm_tracker_worker.thin_provider_runtime import ProviderResult
from storm_tracker_worker.thin_region_planner import (
    ThinAcquisitionRegion,
    ThinRegionPlanner,
)
from storm_tracker_worker.thin_regional_radar_fusion import (
    fuse_regional_radar_fields,
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
    observed_at: float = 1_000.0,
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
        provider, "rain-rate", observed_at, crs,
        origin[0], origin[1], resolution, resolution,
        values, valid,
    )


def test_fusion_preserves_typed_radar_provenance() -> None:
    source = ThinRadarField(
        "radar-a", "rain-rate", 1_000.0, "EPSG:3857",
        -2_000.0, 2_000.0, 1_000.0, 1_000.0,
        np.ones((4, 4), dtype=float), np.ones((4, 4), dtype=bool),
        identity=RadarFieldIdentity(
            quantity="precipitation_rate",
            unit="mm/h",
            quality_kind="source_valid_mask",
            processing_version="source-v3",
            source_composition=("radar-a",),
            canonical_valid_at=1_000.0,
            received_at=1_001.0,
            available_at=1_002.0,
            grid_id="source-grid",
            content_fingerprint="c" * 64,
        ),
    )

    result = fuse_regional_radar_fields(
        _region(), {"radar-a": _provider(source)}, evaluated_at=1_003.0,
    )

    assert result.field is not None
    assert result.field.identity.quantity == "precipitation_rate"
    assert result.field.identity.unit == "mm/h"
    assert result.field.identity.source_composition == ("radar-a",)
    assert result.field.identity.canonical_valid_at == 1_000.0
    assert result.field.identity.received_at == 1_001.0
    assert result.field.identity.available_at == 1_002.0
    assert result.field.identity.grid_id == result.summary.grid_id
    assert len(result.field.identity.content_fingerprint) == 64


def _constant(value: float) -> np.ndarray:
    return np.full((4, 4), value, dtype=float)


def _provider(
    field: ThinRadarField | None = None,
    *,
    provider_id: str | None = None,
    ready: bool = True,
    status: str = "operational",
) -> ProviderResult:
    resolved_id = provider_id or (field.provider_id if field is not None else "provider")
    return ProviderResult(
        provider_id=resolved_id,
        ready=ready,
        status=status,
        reason_code=None,
        observations=(),
        attempted_at=999.0,
        completed_at=1_000.0,
        radar_fields=() if field is None else (field,),
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


def test_region_grid_identity_does_not_change_with_available_provider_resolution():
    """A provider refresh must not invalidate motion history for one region."""

    region = _region(latitude=50.85, longitude=4.35, radius_km=80.0)

    fine_only = build_regional_analysis_grid(
        region,
        (_field(provider="fine", resolution=1_000.0),),
    )
    coarse_only = build_regional_analysis_grid(
        region,
        (_field(provider="coarse", resolution=2_000.0),),
    )

    assert coarse_only == fine_only


def test_planned_region_keeps_250_km_radar_view_beyond_warning_horizon():
    target = Target(
        "test", "Test", "test", True, 39.3, 16.25,
        1_000.0, 5.0, 300.0, "IT",
    )
    state = DesiredState(
        producer_id="ha-nas",
        authority_epoch=1,
        generation=1,
        issued_at=1_000.0,
        previous_state_sha256=None,
        targets=(target,),
        settings=RuntimeSettings(
            ground_radius_km=20.0,
            lightning_source_mode="direct",
            model_guidance_enabled=True,
            region_sharing_distance_km=150.0,
        ),
        canonical_bytes=b"{}",
        state_sha256="0" * 64,
    )
    region = ThinRegionPlanner().plan(state, now=1_000.0).regions[0]

    grid = build_regional_analysis_grid(region, (_field(),))

    assert region.observation_radius_km == 70.0
    assert grid.extent_edges_m[0] <= -250_000.0
    assert grid.extent_edges_m[2] >= 250_000.0
    assert grid.width * grid.height <= 250_000


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


def test_projection_conserves_small_cell_when_downsampling_four_to_one():
    values = np.zeros((4, 4), dtype=float)
    values[0, 0] = 16.0
    layer = project_radar_field(
        _field(
            values=values,
            valid=np.ones((4, 4), bool),
            origin=(0.0, 4_000.0),
            resolution=1_000.0,
        ),
        _grid(
            width=1,
            height=1,
            west=0.0,
            north=4_000.0,
            resolution=4_000.0,
        ),
    )

    assert layer.valid_mask[0, 0]
    assert layer.rain_rate_mm_h[0, 0] == pytest.approx(1.0)


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


def test_projection_uses_vectorized_target_grid_sampling(monkeypatch):
    monkeypatch.setattr(
        radar_fusion_module,
        "_clip_rectangle",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("python_per_pixel_clipping_used")
        ),
    )

    layer = project_radar_field(
        _field(values=_constant(6.0)),
        _grid(
            width=4,
            height=4,
            west=-2_000.0,
            north=2_000.0,
            resolution=1_000.0,
        ),
    )

    assert np.allclose(layer.rain_rate_mm_h[layer.valid_mask], 6.0)


def test_two_sources_fuse_valid_overlap_with_provenance():
    result = fuse_regional_radar_fields(
        _region(),
        {
            "local": _provider(_field(
                provider="local", values=_constant(4.0), observed_at=990.0,
            )),
            "wide": _provider(_field(
                provider="wide", values=_constant(8.0), observed_at=970.0,
            )),
        },
        evaluated_at=1_000.0,
    )

    assert result.field is not None
    valid = result.field.valid_mask
    assert 4.0 < result.field.rain_rate_mm_h[valid].mean() < 8.0
    assert result.summary.contributors == ("local", "wide")
    assert result.summary.mode == "fused"
    assert result.contributor_mask.dtype == np.uint64


def test_only_best_three_sources_contribute_per_pixel():
    providers = {
        name: _provider(_field(
            provider=name,
            values=_constant(value),
            observed_at=stamp,
        ))
        for name, value, stamp in (
            ("a", 1.0, 999.0),
            ("b", 2.0, 998.0),
            ("c", 3.0, 997.0),
            ("d", 100.0, 900.0),
        )
    }

    result = fuse_regional_radar_fields(
        _region(), providers, evaluated_at=1_000.0,
    )

    assert result.summary.contributors == ("a", "b", "c")
    assert result.summary.standby == {"d": "lower_ranked_standby"}
    assert result.field is not None
    assert np.nanmax(result.field.rain_rate_mm_h) < 4.0


def test_disagreement_is_reported_without_silent_source_removal():
    providers = {
        name: _provider(_field(
            provider=name,
            values=_constant(value),
            observed_at=999.0,
        ))
        for name, value in (("a", 0.0), ("b", 1.0), ("c", 100.0))
    }

    result = fuse_regional_radar_fields(
        _region(), providers, evaluated_at=1_000.0,
    )

    assert result.summary.disagreement_fraction > 0.0
    assert "provider_disagreement" in result.summary.reason_codes
    assert len(result.summary.contributors) == 3


def test_fusion_ranking_is_vectorized_not_python_per_pixel(monkeypatch):
    monkeypatch.setattr(
        radar_fusion_module,
        "_weighted_median",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("python_per_pixel_weighted_median_used")
        ),
    )
    providers = {
        name: _provider(_field(
            provider=name,
            values=_constant(value),
            observed_at=999.0,
        ))
        for name, value in (("a", 1.0), ("b", 2.0), ("c", 3.0))
    }

    result = fuse_regional_radar_fields(
        _region(), providers, evaluated_at=1_000.0,
    )

    assert result.field is not None
    assert result.summary.contributors == ("a", "b", "c")


@pytest.mark.parametrize(
    "provider,reason",
    [
        (_provider(_field(provider="stale", observed_at=0.0)), "field_stale"),
        (
            _provider(provider_id="error", ready=True, status="error"),
            "runtime_not_operational",
        ),
        (
            _provider(_field(provider="outside", origin=(1_000_000.0, 1_002_000.0))),
            "outside_region_coverage",
        ),
    ],
)
def test_unusable_provider_is_excluded_with_fixed_reason(provider, reason):
    result = fuse_regional_radar_fields(
        _region(), {provider.provider_id: provider}, evaluated_at=1_000.0,
    )

    assert result.field is None
    assert result.summary.excluded[provider.provider_id] == reason
    assert result.summary.status == "onvoldoende_data"
