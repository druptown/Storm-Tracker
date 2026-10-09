"""Cooperative deadline and lossless capacity gates for cell detection."""
from __future__ import annotations

import math

import pytest

from storm_tracker_worker.tracking_core.engine.analysis_grid import AnalysisGrid
from storm_tracker_worker.tracking_core.engine.hierarchical_detection import (
    HierarchicalDetectionCapacityError,
    HierarchicalDetectionDeadlineError,
    HierarchicalDetectorConfig,
    NormalizedRasterFrame,
    detect_hierarchical_cells,
)


def _frame(values: tuple[tuple[float | None, ...], ...]) -> NormalizedRasterFrame:
    return NormalizedRasterFrame(
        frame_id="resource-bounded-frame",
        valid_at=1_800_000_000.0,
        source="synthetic-quantitative-rate",
        quantity="rainfall_rate",
        unit="mm h-1",
        grid=AnalysisGrid(
            grid_id="resource-bounded-grid-v1",
            crs="LOCAL_METRIC_TEST",
            origin_x_m=0.0,
            origin_y_m=float(len(values) * 1_000),
            x_resolution_m=1_000.0,
            y_resolution_m=1_000.0,
            width=len(values[0]),
            height=len(values),
        ),
        values=values,
    )


def _config() -> HierarchicalDetectorConfig:
    return HierarchicalDetectorConfig(
        shield_threshold=1.0,
        core_threshold=3.0,
        minimum_shield_area_m2=0.0,
        minimum_core_area_m2=0.0,
        shield_position_sigma_m=1_000.0,
        core_position_sigma_m=500.0,
        connectivity=4,
        centroid_method="geometric",
    )


def test_expired_deadline_fails_before_detection() -> None:
    with pytest.raises(HierarchicalDetectionDeadlineError, match="deadline"):
        detect_hierarchical_cells(
            _frame(((2.0,),)),
            _config(),
            deadline_at=5.0,
            monotonic=lambda: 5.0,
        )


def test_deadline_is_checked_inside_large_raster_scan() -> None:
    class AdvancingClock:
        def __init__(self) -> None:
            self.calls = 0

        def __call__(self) -> float:
            self.calls += 1
            return 2.0 if self.calls >= 3 else 0.0

    clock = AdvancingClock()
    values = tuple(tuple(0.0 for _ in range(64)) for _ in range(64))

    with pytest.raises(HierarchicalDetectionDeadlineError, match="deadline"):
        detect_hierarchical_cells(
            _frame(values),
            _config(),
            deadline_at=1.0,
            monotonic=clock,
        )
    assert clock.calls == 3


def test_object_budget_rejects_complete_frame_instead_of_dropping_component() -> None:
    with pytest.raises(HierarchicalDetectionCapacityError, match="objectbudget"):
        detect_hierarchical_cells(
            _frame(((2.0, None, 2.0),)),
            _config(),
            max_detection_objects=1,
        )


def test_object_budget_counts_embedded_core_and_parent_separately() -> None:
    frame = _frame(((4.0, 2.0),))
    with pytest.raises(HierarchicalDetectionCapacityError, match="objectbudget"):
        detect_hierarchical_cells(
            frame,
            _config(),
            max_detection_objects=1,
        )

    complete = detect_hierarchical_cells(
        frame,
        _config(),
        max_detection_objects=2,
    )
    assert len(complete.rain_shields) == 1
    assert len(complete.embedded_cores) == 1


def test_resource_bounds_do_not_change_detection_identity() -> None:
    frame = _frame(((4.0, 2.0, None, 2.0),))
    original = detect_hierarchical_cells(frame, _config())
    bounded = detect_hierarchical_cells(
        frame,
        _config(),
        deadline_at=10.0,
        monotonic=lambda: 0.0,
        max_detection_objects=3,
    )
    assert bounded == original


@pytest.mark.parametrize("value", [0, -1, True, 1.5])
def test_invalid_object_budget_fails_closed(value: object) -> None:
    with pytest.raises(ValueError, match="max_detection_objects"):
        detect_hierarchical_cells(
            _frame(((2.0,),)),
            _config(),
            max_detection_objects=value,  # type: ignore[arg-type]
        )


@pytest.mark.parametrize("value", [math.inf, -math.inf, math.nan, True, "1"])
def test_invalid_deadline_fails_closed(value: object) -> None:
    with pytest.raises(ValueError, match="deadline_at"):
        detect_hierarchical_cells(
            _frame(((2.0,),)),
            _config(),
            deadline_at=value,  # type: ignore[arg-type]
        )


def test_invalid_monotonic_clock_fails_closed() -> None:
    with pytest.raises(ValueError, match="monotonic"):
        detect_hierarchical_cells(
            _frame(((2.0,),)),
            _config(),
            deadline_at=1.0,
            monotonic=lambda: math.nan,
        )

