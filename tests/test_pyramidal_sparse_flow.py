"""Synthetic tests for the bounded pure-NumPy sparse pyramidal challenger."""
from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest

from storm_tracker_worker.tracking_core.engine.analysis_grid import AnalysisGrid
from storm_tracker_worker.tracking_core.engine.cell_track import CellDetection
from storm_tracker_worker.tracking_core.engine.hierarchical_detection import (
    HierarchicalCellDetection,
    NormalizedRasterFrame,
)
from storm_tracker_worker.tracking_core.engine.motion_prediction import (
    OpticalFlowSample,
)
from storm_tracker_worker.tracking_core.engine.pyramidal_sparse_flow import (
    MAX_FEATURES_HARD_LIMIT,
    SparseDetectionAggregationConfig,
    SparsePyramidalConfig,
    SparsePyramidalFlowCache,
    SparseReplayFlowSampleFactory,
    aggregate_sparse_detection_flow,
    estimate_latest_sparse_flow,
    estimate_sparse_pyramidal_motion,
)


def _grid(
    *,
    width: int = 112,
    height: int = 96,
    grid_id: str = "sparse-grid",
) -> AnalysisGrid:
    return AnalysisGrid(
        grid_id=grid_id,
        crs="EPSG:3035",
        origin_x_m=4_000_000.0,
        origin_y_m=3_100_000.0,
        x_resolution_m=1_000.0,
        y_resolution_m=1_500.0,
        width=width,
        height=height,
    )


def _texture(
    height: int,
    width: int,
    *,
    displacement_column: float = 0.0,
    displacement_row: float = 0.0,
    scale: float = 1.0,
    offset: float = 0.0,
) -> tuple[tuple[float, ...], ...]:
    rows, columns = np.mgrid[0:height, 0:width]
    x = columns - displacement_column
    y = rows - displacement_row
    values = scale * (
        0.0017 * x**2
        + 0.0023 * y**2
        + 0.0009 * x * y
        + 1.7 * np.sin(0.19 * x)
        + 1.3 * np.cos(0.17 * y)
        + 0.8 * np.sin(0.11 * x + 0.13 * y)
        + 12.0
    ) + offset
    return tuple(tuple(float(item) for item in row) for row in values)


def _frame(
    frame_id: str,
    valid_at: float,
    values,
    *,
    grid: AnalysisGrid | None = None,
) -> NormalizedRasterFrame:
    selected_grid = grid or _grid(
        width=len(values[0]),
        height=len(values),
    )
    return NormalizedRasterFrame(
        frame_id=frame_id,
        valid_at=valid_at,
        source="synthetic-normalized-radar",
        quantity="rain_rate",
        unit="mm/h",
        grid=selected_grid,
        values=tuple(tuple(item for item in row) for row in values),
    )


def _pair(
    displacement_column: float,
    displacement_row: float,
    *,
    elapsed_seconds: float,
    width: int = 112,
    height: int = 96,
    scale: float = 1.0,
    offset: float = 0.0,
) -> tuple[NormalizedRasterFrame, NormalizedRasterFrame]:
    grid = _grid(width=width, height=height)
    return (
        _frame(
            "sparse-old",
            1_000.0,
            _texture(height, width),
            grid=grid,
        ),
        _frame(
            "sparse-new",
            1_000.0 + elapsed_seconds,
            _texture(
                height,
                width,
                displacement_column=displacement_column,
                displacement_row=displacement_row,
                scale=scale,
                offset=offset,
            ),
            grid=grid,
        ),
    )


def _config(
    *,
    max_features: int = 80,
    levels: int = 5,
    iterations: int = 25,
) -> SparsePyramidalConfig:
    return SparsePyramidalConfig(
        maximum_input_pixels=50_000,
        max_features=max_features,
        minimum_feature_distance_pixels=5.0,
        feature_window_radius_pixels=2,
        minimum_feature_valid_fraction=0.8,
        minimum_feature_eigenvalue=0.01,
        pyramid_levels=levels,
        minimum_pyramid_valid_fraction=0.5,
        tracking_window_radius_pixels=4,
        minimum_tracking_samples=30,
        maximum_iterations=iterations,
        convergence_threshold_pixels=1e-4,
        minimum_tracking_eigenvalue=1e-5,
        maximum_condition_number=1e8,
        regularization=1e-8,
        residual_variance_floor=1e-5,
        velocity_process_noise_covariance_m2_s2=(
            (1.0, 0.0),
            (0.0, 2.0),
        ),
    )


def _aggregation_config(
    *,
    minimum_vectors: int = 1,
    fraction: float = 0.5,
) -> SparseDetectionAggregationConfig:
    return SparseDetectionAggregationConfig(
        minimum_usable_vectors=minimum_vectors,
        minimum_usable_fraction=fraction,
        include_iteration_limit=True,
        residual_weight_floor=1e-4,
        velocity_covariance_floor_m2_s2=((4.0, 0.0), (0.0, 9.0)),
        residual_position_covariance_m2=(
            (100_000.0, 0.0),
            (0.0, 100_000.0),
        ),
        method="inverse_residual_weighted_mean",
    )


def _detection(
    frame: NormalizedRasterFrame,
    pixels: tuple[tuple[int, int], ...],
    *,
    detection_id: str = "synthetic-cell",
) -> HierarchicalCellDetection:
    centres = tuple(frame.grid.pixel_center(*item) for item in pixels)
    x_m = sum(item[0] for item in centres) / len(centres)
    y_m = sum(item[1] for item in centres) / len(centres)
    return HierarchicalCellDetection(
        detection=CellDetection(
            detection_id=detection_id,
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
        mean_value=2.0,
        maximum_value=2.0,
        threshold=1.0,
        centroid_method="geometric",
    )


def _usable(pair):
    return tuple(item for item in pair.vectors if item.usable)


def test_large_32_pixel_translation_is_recovered_without_speed_gate():
    previous, current = _pair(
        32.0,
        -20.0,
        elapsed_seconds=60.0,
    )

    pair = estimate_latest_sparse_flow((previous, current), _config())
    usable = _usable(pair)
    aggregation = aggregate_sparse_detection_flow(
        pair,
        _detection(
            previous,
            tuple((item.start_row, item.start_column) for item in pair.vectors),
        ),
        _aggregation_config(minimum_vectors=2, fraction=0.2),
    )

    assert usable
    assert aggregation.available and aggregation.sample is not None
    assert aggregation.sample.vx_m_s == pytest.approx(
        32_000.0 / 60.0,
        abs=6.0,
    )
    assert aggregation.sample.vy_m_s == pytest.approx(
        30_000.0 / 60.0,
        abs=8.0,
    )
    assert any(
        item.displacement_column_pixels == pytest.approx(32.0, abs=0.35)
        and item.displacement_row_pixels == pytest.approx(-20.0, abs=0.35)
        for item in usable
    )
    assert aggregation.maximum_vector_weight > aggregation.minimum_vector_weight
    assert len(pair.vectors) == pair.feature_audit.selected_count
    assert len(pair.vectors) <= pair.feature_audit.max_features
    best = min(usable, key=lambda item: item.forward_residual_rms)
    assert best.feature_id.startswith("shi-tomasi:")
    assert best.structure_quality is not None
    assert best.velocity_covariance_m2_s2 is not None
    assert pair.previous_frame_id == previous.frame_id
    assert pair.current_frame_id == current.frame_id


def test_subpixel_irregular_dt_stationary_and_three_frame_sequence():
    first, second = _pair(0.75, -0.4, elapsed_seconds=37.0)
    third = _frame(
        "sparse-third",
        1_113.0,
        _texture(
            first.grid.height,
            first.grid.width,
            displacement_column=0.75,
            displacement_row=-0.4,
        ),
        grid=first.grid,
    )
    subpixel = estimate_latest_sparse_flow((first, second), _config(levels=3))
    stationary = estimate_latest_sparse_flow(
        (
            _frame("stationary-a", 2_000.0, _texture(96, 112), grid=first.grid),
            _frame("stationary-b", 2_067.0, _texture(96, 112), grid=first.grid),
        ),
        _config(levels=3),
    )
    sequence = estimate_sparse_pyramidal_motion(
        (first, second, third),
        _config(levels=3),
    )

    assert np.median([item.vx_m_s for item in _usable(subpixel)]) == pytest.approx(
        750.0 / 37.0,
        abs=1.0,
    )
    assert np.median([item.vy_m_s for item in _usable(subpixel)]) == pytest.approx(
        600.0 / 37.0,
        abs=1.0,
    )
    assert np.median([item.vx_m_s for item in _usable(stationary)]) == pytest.approx(
        0.0,
        abs=0.1,
    )
    assert len(sequence.pairs) == 2
    assert sequence.latest.elapsed_seconds == 76.0


def test_two_local_systems_keep_distinct_sparse_vectors_and_cell_samples():
    height, width = 96, 128
    grid = _grid(width=width, height=height)
    old = np.zeros((height, width), dtype=np.float64)
    new = np.zeros((height, width), dtype=np.float64)

    def patch(rows, columns, dx, dy, phase):
        patch_height = rows.stop - rows.start
        patch_width = columns.stop - columns.start
        y, x = np.mgrid[0:patch_height, 0:patch_width]
        texture = (
            5.0
            + np.sin(0.4 * x + phase)
            + np.cos(0.31 * y - phase)
            + 0.6 * np.sin(0.22 * x + 0.27 * y)
        )
        old[rows, columns] = texture
        new[
            slice(rows.start + dy, rows.stop + dy),
            slice(columns.start + dx, columns.stop + dx),
        ] = texture

    patch(slice(18, 48), slice(12, 42), 12, 5, 0.0)
    patch(slice(28, 62), slice(78, 112), -10, -6, 1.2)
    previous = _frame("local-old", 1_000.0, old, grid=grid)
    current = _frame("local-new", 1_060.0, new, grid=grid)
    pair = estimate_latest_sparse_flow(
        (previous, current),
        _config(max_features=180, levels=4),
    )
    first_pixels = tuple(
        (row, column)
        for row in range(18, 48)
        for column in range(12, 42)
    )
    second_pixels = tuple(
        (row, column)
        for row in range(28, 62)
        for column in range(78, 112)
    )
    first = aggregate_sparse_detection_flow(
        pair,
        _detection(previous, first_pixels, detection_id="left-system"),
        _aggregation_config(minimum_vectors=2, fraction=0.3),
    )
    second = aggregate_sparse_detection_flow(
        pair,
        _detection(previous, second_pixels, detection_id="right-system"),
        _aggregation_config(minimum_vectors=2, fraction=0.3),
    )

    assert first.available and first.sample is not None
    assert second.available and second.sample is not None
    assert first.sample.vx_m_s == pytest.approx(200.0, abs=35.0)
    assert first.sample.vy_m_s == pytest.approx(-125.0, abs=45.0)
    assert second.sample.vx_m_s == pytest.approx(-166.67, abs=35.0)
    assert second.sample.vy_m_s == pytest.approx(150.0, abs=45.0)


def test_growth_decay_keeps_every_selected_vector_or_failure():
    baseline_frames = _pair(4.0, -2.0, elapsed_seconds=60.0)
    changed_frames = _pair(
        4.0,
        -2.0,
        elapsed_seconds=60.0,
        scale=1.35,
        offset=2.0,
    )
    baseline = estimate_latest_sparse_flow(baseline_frames, _config(levels=4))
    changed = estimate_latest_sparse_flow(changed_frames, _config(levels=4))

    assert len(changed.vectors) == changed.feature_audit.selected_count
    assert sum(count for _status, count in changed.status_counts) == len(
        changed.vectors
    )
    assert _usable(changed)
    baseline_residual = np.mean(
        [item.forward_residual_rms for item in _usable(baseline)]
    )
    changed_residual = np.mean(
        [item.forward_residual_rms for item in _usable(changed)]
    )
    assert changed_residual > baseline_residual


def test_nodata_masks_are_respected_and_failures_remain_auditable():
    previous, current = _pair(8.0, 3.0, elapsed_seconds=60.0)
    old = [list(row) for row in previous.values]
    new = [list(row) for row in current.values]
    for row in range(30, 60):
        for column in range(40, 75):
            old[row][column] = None
            new[row][column] = None
    previous = _frame("masked-old", previous.valid_at, old, grid=previous.grid)
    current = _frame("masked-new", current.valid_at, new, grid=current.grid)

    pair = estimate_latest_sparse_flow((previous, current), _config(levels=3))

    assert pair.vectors
    assert len(pair.vectors) == pair.feature_audit.selected_count
    assert not any(
        30 <= item.start_row < 60 and 40 <= item.start_column < 75
        for item in pair.vectors
    )
    assert _usable(pair)


def test_aperture_and_no_texture_are_explicit_not_zero_motion():
    height, width = 64, 72
    grid = _grid(width=width, height=height)
    rows, columns = np.mgrid[0:height, 0:width]
    textured = np.sin(columns * 0.3) + np.cos(rows * 0.27)
    constant = np.ones_like(textured)
    lost_texture = estimate_latest_sparse_flow(
        (
            _frame("texture-old", 1_000.0, textured, grid=grid),
            _frame("texture-lost", 1_060.0, constant, grid=grid),
        ),
        _config(max_features=40, levels=3),
    )
    no_texture = estimate_latest_sparse_flow(
        (
            _frame("constant-a", 2_000.0, constant, grid=grid),
            _frame("constant-b", 2_060.0, constant, grid=grid),
        ),
        _config(max_features=40, levels=3),
    )

    assert lost_texture.vectors
    assert all(not item.usable for item in lost_texture.vectors)
    assert all(
        item.status in ("ill_conditioned", "insufficient_support")
        for item in lost_texture.vectors
    )
    assert no_texture.feature_audit.candidate_count == 0
    assert no_texture.feature_audit.selected_count == 0
    assert no_texture.vectors == ()


def test_extreme_uncapped_translation_survives_coarse_to_fine():
    previous, current = _pair(
        80.0,
        -48.0,
        elapsed_seconds=1.0,
        width=192,
        height=176,
    )

    pair = estimate_latest_sparse_flow(
        (previous, current),
        _config(max_features=120, levels=6),
    )
    usable = _usable(pair)
    aggregation = aggregate_sparse_detection_flow(
        pair,
        _detection(
            previous,
            tuple((item.start_row, item.start_column) for item in pair.vectors),
            detection_id="extreme-system",
        ),
        _aggregation_config(minimum_vectors=1, fraction=0.1),
    )

    assert usable
    assert aggregation.available and aggregation.sample is not None
    assert aggregation.sample.vx_m_s == pytest.approx(
        80_000.0,
        abs=1_500.0,
    )
    assert aggregation.sample.vy_m_s == pytest.approx(
        72_000.0,
        abs=2_000.0,
    )


def test_feature_budget_and_input_limit_bound_result_memory():
    previous, current = _pair(2.0, 1.0, elapsed_seconds=60.0)
    pair = estimate_latest_sparse_flow(
        (previous, current),
        _config(max_features=7, levels=4),
    )

    assert pair.feature_audit.selected_count <= 7
    assert len(pair.vectors) <= 7
    assert (
        pair.feature_audit.candidate_count
        == pair.feature_audit.selected_count
        + pair.feature_audit.pyramid_margin_suppressed_count
        + pair.feature_audit.distance_suppressed_count
        + pair.feature_audit.budget_dropped_count
    )
    with pytest.raises(ValueError, match="maximum_input_pixels"):
        estimate_latest_sparse_flow(
            (previous, current),
            replace(
                _config(),
                maximum_input_pixels=100,
            ),
        )
    with pytest.raises(ValueError, match="harde productielimiet"):
        _config(max_features=MAX_FEATURES_HARD_LIMIT + 1)


def test_unavailable_cell_uses_caller_fallback_in_replay_helper():
    previous, current = _pair(2.0, 0.0, elapsed_seconds=60.0)
    sequence = estimate_sparse_pyramidal_motion(
        (previous, current),
        _config(max_features=5, levels=3),
    )
    empty_area = _detection(
        previous,
        ((0, 0),),
        detection_id="featureless-corner",
    )
    captured = {}

    def fallback(detection, current_frame, aggregation):
        captured["status"] = aggregation.status
        return OpticalFlowSample(
            sample_id="caller-broad-zero",
            analysis_grid_id=detection.detection.analysis_grid_id,
            reference_valid_at=detection.detection.valid_at,
            vx_m_s=0.0,
            vy_m_s=0.0,
            velocity_covariance_m2_s2=(
                (1_000_000.0, 0.0),
                (0.0, 1_000_000.0),
            ),
            residual_covariance_m2=(
                (4_000_000.0, 0.0),
                (0.0, 4_000_000.0),
            ),
            method="caller_broad_cold_start",
            support_frame_ids=(previous.frame_id, current.frame_id),
        )

    factory = SparseReplayFlowSampleFactory(
        sequence=sequence,
        aggregation_config=_aggregation_config(),
        unavailable_factory=fallback,
    )
    sample = factory(
        empty_area,
        SimpleNamespace(frame_id=current.frame_id),
    )

    assert sample.sample_id == "caller-broad-zero"
    assert captured["status"] == "unavailable_no_selected_features"


def test_checksum_cache_grid_contract_and_mirror_are_deterministic():
    previous, current = _pair(3.0, -1.0, elapsed_seconds=60.0)
    config = _config(max_features=20, levels=4)
    cache = SparsePyramidalFlowCache(maximum_entries=2)

    first = cache.estimate((previous, current), config)
    second = cache.estimate((previous, current), config)
    direct = estimate_sparse_pyramidal_motion((previous, current), config)

    assert first is second
    assert first == direct
    assert cache.hits == 1
    assert cache.misses == 1
    changed_same_id = _frame(
        current.frame_id,
        current.valid_at,
        tuple(
            tuple(float(item) + 0.5 for item in row)
            for row in current.values
        ),
        grid=current.grid,
    )
    with pytest.raises(ValueError, match="gewijzigde rasterbytes"):
        cache.estimate((previous, changed_same_id), config)
    incompatible_grid = _grid(
        width=previous.grid.width,
        height=previous.grid.height,
        grid_id="other-grid",
    )
    incompatible = _frame(
        "incompatible",
        current.valid_at,
        current.values,
        grid=incompatible_grid,
    )
    with pytest.raises(ValueError, match="geometrie"):
        estimate_sparse_pyramidal_motion(
            (previous, incompatible),
            config,
        )

    repository = Path(__file__).resolve().parents[1]
    integration = (
        repository
        / "custom_components/storm_tracker_v3/engine/pyramidal_sparse_flow.py"
    )
    worker = (
        repository
        / "storm_tracker_worker/tracking_core/engine/pyramidal_sparse_flow.py"
    )
    assert integration.read_bytes() == worker.read_bytes()
    assert "estimate_sparse_pyramidal_motion" not in (
        repository / "custom_components/storm_tracker_v3/__init__.py"
    ).read_text(encoding="utf-8")
    assert "estimate_sparse_pyramidal_motion" not in (
        repository / "storm_tracker_worker/service.py"
    ).read_text(encoding="utf-8")
