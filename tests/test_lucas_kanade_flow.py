"""Tests for the isolated, masked metric Lucas-Kanade baseline."""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest

from storm_tracker_worker.tracking_core.engine.analysis_grid import AnalysisGrid
from storm_tracker_worker.tracking_core.engine.cell_track import CellDetection
from storm_tracker_worker.tracking_core.engine.hierarchical_detection import (
    HierarchicalCellDetection,
    NormalizedRasterFrame,
)
from storm_tracker_worker.tracking_core.engine.lucas_kanade_flow import (
    DetectionFlowAggregationConfig,
    LucasKanadeConfig,
    LucasKanadeFlowCache,
    _array_covariance,
    aggregate_detection_flow_sample,
    estimate_lucas_kanade_flow,
)


def _grid(
    *,
    grid_id: str = "lk-grid-v1",
    width: int = 25,
    height: int = 23,
    x_resolution_m: float = 1_000.0,
    y_resolution_m: float = 2_000.0,
) -> AnalysisGrid:
    return AnalysisGrid(
        grid_id=grid_id,
        crs="EPSG:3035",
        origin_x_m=4_000_000.0,
        origin_y_m=3_100_000.0,
        x_resolution_m=x_resolution_m,
        y_resolution_m=y_resolution_m,
        width=width,
        height=height,
    )


def _quadratic_values(
    height: int,
    width: int,
    *,
    displacement_column: float,
    displacement_row: float,
) -> tuple[tuple[float, ...], ...]:
    rows, columns = np.mgrid[0:height, 0:width]

    def texture(x, y):
        return (
            0.031 * x**2
            + 0.047 * y**2
            + 0.019 * x * y
            + 0.73 * x
            - 0.41 * y
            + 7.0
        )

    values = texture(
        columns - displacement_column,
        rows - displacement_row,
    )
    return tuple(tuple(float(value) for value in row) for row in values)


def _pair(
    *,
    displacement_column: float,
    displacement_row: float,
    elapsed_seconds: float,
    grid: AnalysisGrid | None = None,
    old_values=None,
    new_values=None,
) -> tuple[NormalizedRasterFrame, NormalizedRasterFrame]:
    selected_grid = grid or _grid()
    old = old_values or _quadratic_values(
        selected_grid.height,
        selected_grid.width,
        displacement_column=0.0,
        displacement_row=0.0,
    )
    new = new_values or _quadratic_values(
        selected_grid.height,
        selected_grid.width,
        displacement_column=displacement_column,
        displacement_row=displacement_row,
    )
    return (
        NormalizedRasterFrame(
            frame_id="old-frame",
            valid_at=1_000.0,
            source="normalized-test-radar",
            quantity="rain_rate",
            unit="mm/h",
            grid=selected_grid,
            values=old,
        ),
        NormalizedRasterFrame(
            frame_id="new-frame",
            valid_at=1_000.0 + elapsed_seconds,
            source="normalized-test-radar",
            quantity="rain_rate",
            unit="mm/h",
            grid=selected_grid,
            values=new,
        ),
    )


def _config(
    *,
    process_covariance=((0.25, 0.0), (0.0, 0.5)),
    residual_floor: float = 1e-6,
) -> LucasKanadeConfig:
    return LucasKanadeConfig(
        window_radius_pixels=3,
        minimum_valid_samples=20,
        minimum_structure_eigenvalue=1e-6,
        maximum_condition_number=1e8,
        regularization=0.0,
        residual_variance_floor=residual_floor,
        velocity_process_noise_covariance_m2_s2=process_covariance,
    )


def _aggregation_config(
    *,
    minimum_valid_pixels: int = 1,
    minimum_valid_fraction: float = 1.0,
) -> DetectionFlowAggregationConfig:
    return DetectionFlowAggregationConfig(
        minimum_valid_pixels=minimum_valid_pixels,
        minimum_valid_fraction=minimum_valid_fraction,
        velocity_covariance_floor_m2_s2=((1.0, 0.0), (0.0, 2.0)),
        residual_position_covariance_m2=(
            (10_000.0, 0.0),
            (0.0, 20_000.0),
        ),
        method="conservative_mean",
    )


def _detection(
    frame: NormalizedRasterFrame,
    pixels: tuple[tuple[int, int], ...],
) -> HierarchicalCellDetection:
    centres = tuple(frame.grid.pixel_center(*pixel) for pixel in pixels)
    x_m = sum(item[0] for item in centres) / len(centres)
    y_m = sum(item[1] for item in centres) / len(centres)
    return HierarchicalCellDetection(
        detection=CellDetection(
            detection_id="flow-object",
            valid_at=frame.valid_at,
            analysis_grid_id=frame.grid.grid_id,
            x_m=x_m,
            y_m=y_m,
            position_sigma_m=1_000.0,
            source=frame.source,
        ),
        object_kind="rain_shield",
        parent_detection_id=None,
        footprint_pixels=pixels,
        area_m2=len(pixels) * frame.grid.pixel_area_m2,
        geometric_centroid_x_m=x_m,
        geometric_centroid_y_m=y_m,
        value_weighted_centroid_x_m=x_m,
        value_weighted_centroid_y_m=y_m,
        mean_value=1.0,
        maximum_value=1.0,
        threshold=0.5,
        centroid_method="geometric",
    )


def test_known_subpixel_translation_uses_actual_elapsed_and_metric_axes():
    previous, current = _pair(
        displacement_column=1.25,
        displacement_row=-0.5,
        elapsed_seconds=50.0,
    )

    field = estimate_lucas_kanade_flow(previous, current, _config())
    centre = field.pixel(11, 12)

    assert centre.available is True
    assert centre.vx_m_s == pytest.approx(25.0, abs=1e-9)
    assert centre.vy_m_s == pytest.approx(20.0, abs=1e-9)
    assert centre.residual_rms == pytest.approx(0.0, abs=1e-10)
    assert field.elapsed_seconds == 50.0


def test_whole_pixel_translation_and_stationary_field():
    previous, current = _pair(
        displacement_column=1.0,
        displacement_row=1.0,
        elapsed_seconds=100.0,
    )
    moved = estimate_lucas_kanade_flow(previous, current, _config())
    stationary_previous, stationary_current = _pair(
        displacement_column=0.0,
        displacement_row=0.0,
        elapsed_seconds=100.0,
    )
    stationary = estimate_lucas_kanade_flow(
        stationary_previous,
        stationary_current,
        _config(),
    )

    assert moved.pixel(11, 12).vx_m_s == pytest.approx(10.0, abs=1e-9)
    assert moved.pixel(11, 12).vy_m_s == pytest.approx(-20.0, abs=1e-9)
    assert stationary.pixel(11, 12).vx_m_s == pytest.approx(0.0, abs=1e-12)
    assert stationary.pixel(11, 12).vy_m_s == pytest.approx(0.0, abs=1e-12)


def test_irregular_elapsed_changes_velocity_not_displacement():
    short = _pair(
        displacement_column=2.0,
        displacement_row=0.0,
        elapsed_seconds=37.0,
    )
    long = _pair(
        displacement_column=2.0,
        displacement_row=0.0,
        elapsed_seconds=113.0,
    )

    short_flow = estimate_lucas_kanade_flow(*short, _config()).pixel(11, 12)
    long_flow = estimate_lucas_kanade_flow(*long, _config()).pixel(11, 12)

    assert short_flow.vx_m_s == pytest.approx(2_000.0 / 37.0, abs=1e-9)
    assert long_flow.vx_m_s == pytest.approx(2_000.0 / 113.0, abs=1e-9)


def test_nodata_is_explicit_and_does_not_poison_distant_windows():
    grid = _grid()
    old = [list(row) for row in _quadratic_values(23, 25, displacement_column=0.0, displacement_row=0.0)]
    new = [list(row) for row in _quadratic_values(23, 25, displacement_column=1.0, displacement_row=0.0)]
    old[11][12] = None
    new[11][12] = None
    previous, current = _pair(
        displacement_column=1.0,
        displacement_row=0.0,
        elapsed_seconds=100.0,
        grid=grid,
        old_values=tuple(tuple(row) for row in old),
        new_values=tuple(tuple(row) for row in new),
    )

    field = estimate_lucas_kanade_flow(previous, current, _config())

    assert field.pixel(11, 12).status == "nodata"
    assert field.pixel(5, 5).available is True
    assert field.pixel(5, 5).vx_m_s == pytest.approx(10.0, abs=1e-9)


def test_incompatible_grid_quantity_and_time_fail_closed():
    previous, current = _pair(
        displacement_column=1.0,
        displacement_row=0.0,
        elapsed_seconds=60.0,
    )
    other_grid = _grid(grid_id="different-grid", x_resolution_m=2_000.0)
    incompatible = NormalizedRasterFrame(
        frame_id=current.frame_id,
        valid_at=current.valid_at,
        source=current.source,
        quantity=current.quantity,
        unit=current.unit,
        grid=other_grid,
        values=_quadratic_values(
            other_grid.height,
            other_grid.width,
            displacement_column=1.0,
            displacement_row=0.0,
        ),
    )

    with pytest.raises(ValueError, match="geometrie"):
        estimate_lucas_kanade_flow(previous, incompatible, _config())
    with pytest.raises(ValueError, match="later"):
        estimate_lucas_kanade_flow(current, previous, _config())


def test_aperture_problem_is_ill_conditioned_not_fake_zero_motion():
    grid = _grid()
    _rows, columns = np.mgrid[0 : grid.height, 0 : grid.width]
    old = tuple(tuple(float(value) for value in row) for row in columns)
    new = tuple(tuple(float(value - 2.0) for value in row) for row in columns)
    previous, current = _pair(
        displacement_column=2.0,
        displacement_row=0.0,
        elapsed_seconds=60.0,
        grid=grid,
        old_values=old,
        new_values=new,
    )

    field = estimate_lucas_kanade_flow(previous, current, _config())
    centre = field.pixel(11, 12)
    aggregation = aggregate_detection_flow_sample(
        field,
        _detection(previous, ((11, 12),)),
        _aggregation_config(),
    )

    assert centre.status == "ill_conditioned"
    assert centre.vx_m_s is None
    assert centre.vy_m_s is None
    assert aggregation.status == "unavailable_no_valid_flow"
    assert aggregation.sample is None
    assert aggregation.detection_id == "flow-object"


def test_extreme_vector_is_measured_without_speed_cap():
    previous, current = _pair(
        displacement_column=50.0,
        displacement_row=-20.0,
        elapsed_seconds=1.0,
    )

    pixel = estimate_lucas_kanade_flow(previous, current, _config()).pixel(
        11,
        12,
    )

    assert pixel.available is True
    assert pixel.vx_m_s == pytest.approx(50_000.0, abs=1e-6)
    assert pixel.vy_m_s == pytest.approx(40_000.0, abs=1e-6)


def test_covariance_residual_and_detection_aggregation_are_explicit():
    previous, current = _pair(
        displacement_column=0.75,
        displacement_row=0.25,
        elapsed_seconds=60.0,
    )
    field = estimate_lucas_kanade_flow(
        previous,
        current,
        _config(
            process_covariance=((4.0, 0.5), (0.5, 9.0)),
            residual_floor=0.01,
        ),
    )
    footprint = tuple(
        (row, column)
        for row in range(9, 14)
        for column in range(10, 15)
    )
    aggregation = aggregate_detection_flow_sample(
        field,
        _detection(previous, footprint),
        _aggregation_config(
            minimum_valid_pixels=20,
            minimum_valid_fraction=0.8,
        ),
    )

    assert aggregation.available is True
    assert aggregation.sample is not None
    assert aggregation.sample.vx_m_s == pytest.approx(12.5, abs=1e-9)
    assert aggregation.sample.vy_m_s == pytest.approx(
        -2_000.0 * 0.25 / 60.0,
        abs=1e-9,
    )
    covariance = aggregation.sample.velocity_covariance_m2_s2
    assert covariance[0][0] >= 5.0
    assert covariance[1][1] >= 11.0
    assert aggregation.sample.residual_covariance_m2 == (
        (10_000.0, 0.0),
        (0.0, 20_000.0),
    )
    assert aggregation.mean_quality is not None
    assert aggregation.mean_residual_rms == pytest.approx(0.0, abs=1e-10)


def test_array_covariance_canonicalises_reduction_roundoff():
    covariance = _array_covariance(np.asarray(
        ((4.0, 1.0 + 5e-11), (1.0, 9.0)),
        dtype=np.float64,
    ))

    assert covariance[0][1] == covariance[1][0]
    assert covariance[0][1] == pytest.approx(1.0 + 2.5e-11)


def test_detection_coverage_failure_returns_unavailable_with_diagnostics():
    previous, current = _pair(
        displacement_column=1.0,
        displacement_row=0.0,
        elapsed_seconds=60.0,
    )
    old = [list(row) for row in previous.values]
    new = [list(row) for row in current.values]
    for row, column in ((11, 12), (11, 13), (12, 12)):
        old[row][column] = None
        new[row][column] = None
    previous, current = _pair(
        displacement_column=1.0,
        displacement_row=0.0,
        elapsed_seconds=60.0,
        old_values=tuple(tuple(row) for row in old),
        new_values=tuple(tuple(row) for row in new),
    )
    field = estimate_lucas_kanade_flow(previous, current, _config())
    aggregation = aggregate_detection_flow_sample(
        field,
        _detection(previous, ((11, 12), (11, 13), (12, 12), (10, 10))),
        _aggregation_config(
            minimum_valid_pixels=2,
            minimum_valid_fraction=0.75,
        ),
    )

    assert aggregation.status == "unavailable_insufficient_coverage"
    assert aggregation.valid_pixels == 1
    assert aggregation.valid_fraction == 0.25
    assert aggregation.sample is None


def test_checksum_and_cache_are_deterministic_and_collision_safe():
    previous, current = _pair(
        displacement_column=1.0,
        displacement_row=0.0,
        elapsed_seconds=60.0,
    )
    config = _config()
    cache = LucasKanadeFlowCache(maximum_entries=2)

    first = cache.estimate(previous, current, config)
    second = cache.estimate(previous, current, config)
    direct = estimate_lucas_kanade_flow(previous, current, config)

    assert second is first
    assert first == direct
    assert first.checksum == direct.checksum
    assert cache.hits == 1
    assert cache.misses == 1
    assert cache.size == 1

    changed_values = tuple(
        tuple(value + 1.0 for value in row) for row in current.values
    )
    changed_same_id = NormalizedRasterFrame(
        frame_id=current.frame_id,
        valid_at=current.valid_at,
        source=current.source,
        quantity=current.quantity,
        unit=current.unit,
        grid=current.grid,
        values=changed_values,
    )
    with pytest.raises(ValueError, match="gewijzigde rasterbytes"):
        cache.estimate(previous, changed_same_id, config)


def test_module_is_byte_identical_and_not_live_wired():
    repository = Path(__file__).resolve().parents[1]
    integration = (
        repository
        / "custom_components/storm_tracker_v3/engine/lucas_kanade_flow.py"
    )
    worker = (
        repository
        / "storm_tracker_worker/tracking_core/engine/lucas_kanade_flow.py"
    )

    assert integration.read_bytes() == worker.read_bytes()
    assert "estimate_lucas_kanade_flow" not in (
        repository / "custom_components/storm_tracker_v3/__init__.py"
    ).read_text(encoding="utf-8")
    assert "estimate_lucas_kanade_flow" not in (
        repository / "storm_tracker_worker/service.py"
    ).read_text(encoding="utf-8")
