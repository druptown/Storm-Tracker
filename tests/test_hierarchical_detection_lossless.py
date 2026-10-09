"""Lossless minimum-area semantics for the shadow hierarchical detector."""
from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from storm_tracker_worker.tracking_core.engine.analysis_grid import AnalysisGrid
from storm_tracker_worker.tracking_core.engine.hierarchical_detection import (
    HierarchicalDetectionFrame,
    HierarchicalDetectorConfig,
    NormalizedRasterFrame,
    detect_hierarchical_cells,
)


def _grid(*, width: int, height: int) -> AnalysisGrid:
    return AnalysisGrid(
        grid_id="lossless-detector-grid-v1",
        crs="LOCAL_METRIC_TEST",
        origin_x_m=0.0,
        origin_y_m=float(height * 1_000),
        x_resolution_m=1_000.0,
        y_resolution_m=1_000.0,
        width=width,
        height=height,
    )


def _frame(
    values: tuple[tuple[float | None, ...], ...],
    *,
    frame_id: str = "lossless-frame",
) -> NormalizedRasterFrame:
    return NormalizedRasterFrame(
        frame_id=frame_id,
        valid_at=1_800_000_000.0,
        source="synthetic-quantitative-rate",
        quantity="rainfall_rate",
        unit="mm h-1",
        grid=_grid(width=len(values[0]), height=len(values)),
        values=values,
    )


def _config(
    *,
    minimum_shield_area_m2: float = 2_000_000.0,
    minimum_core_area_m2: float = 2_000_000.0,
) -> HierarchicalDetectorConfig:
    return HierarchicalDetectorConfig(
        shield_threshold=1.0,
        core_threshold=3.0,
        minimum_shield_area_m2=minimum_shield_area_m2,
        minimum_core_area_m2=minimum_core_area_m2,
        shield_position_sigma_m=1_000.0,
        core_position_sigma_m=500.0,
        connectivity=4,
        centroid_method="geometric",
    )


def test_minimum_area_marks_small_shields_but_never_discards_them() -> None:
    detected = detect_hierarchical_cells(
        _frame(((2.0, None, 2.0),)),
        _config(),
    )

    assert len(detected.rain_shields) == 2
    assert {item.footprint_pixels for item in detected.rain_shields} == {
        ((0, 0),),
        ((0, 2),),
    }
    assert detected.low_confidence_fragments == detected.rain_shields
    assert all(item.minimum_area_m2 == 2_000_000.0 for item in detected.rain_shields)


def test_minimum_area_marks_embedded_core_without_removing_parent_or_core() -> None:
    detected = detect_hierarchical_cells(
        _frame(((4.0, 2.0), (2.0, 2.0))),
        _config(minimum_shield_area_m2=4_000_000.0),
    )

    assert len(detected.rain_shields) == 1
    assert len(detected.embedded_cores) == 1
    shield = detected.rain_shields[0]
    core = detected.embedded_cores[0]
    assert shield.low_confidence_fragment is False
    assert core.low_confidence_fragment is True
    assert core.parent_detection_id == shield.detection.detection_id
    assert detected.low_confidence_fragments == (core,)


def test_unrelated_fragment_does_not_renumber_existing_detection_identity() -> None:
    original = detect_hierarchical_cells(
        _frame(((None, None, 2.0, 2.0),)),
        _config(),
    )
    with_fragment = detect_hierarchical_cells(
        _frame(((2.0, None, 2.0, 2.0),)),
        _config(),
    )

    original_large = next(
        item for item in original.rain_shields
        if item.footprint_pixels == ((0, 2), (0, 3))
    )
    current_large = next(
        item for item in with_fragment.rain_shields
        if item.footprint_pixels == ((0, 2), (0, 3))
    )
    assert (
        current_large.detection.detection_id
        == original_large.detection.detection_id
    )


def test_low_confidence_flag_cannot_contradict_area_audit() -> None:
    fragment = detect_hierarchical_cells(
        _frame(((2.0,),)),
        _config(),
    ).rain_shields[0]

    with pytest.raises(ValueError, match="minimumarea-audit"):
        replace(fragment, low_confidence_fragment=False)


def test_detection_frame_rejects_orphan_or_outside_embedded_core() -> None:
    detected = detect_hierarchical_cells(
        _frame(((4.0, 2.0), (2.0, 2.0))),
        _config(minimum_shield_area_m2=4_000_000.0),
    )
    shield = detected.rain_shields[0]
    core = detected.embedded_cores[0]

    with pytest.raises(ValueError, match="frame-shield"):
        replace(detected, detections=(core,))
    outside = replace(core, footprint_pixels=((0, 2),))
    with pytest.raises(ValueError, match="volledig binnen"):
        HierarchicalDetectionFrame(
            frame_id=detected.frame_id,
            valid_at=detected.valid_at,
            analysis_grid_id=detected.analysis_grid_id,
            quantity=detected.quantity,
            unit=detected.unit,
            detections=(shield, outside),
        )


def test_detection_frame_rejects_duplicate_identity_and_overlapping_shields() -> None:
    detected = detect_hierarchical_cells(
        _frame(((2.0, None, 2.0),)),
        _config(),
    )
    first, second = detected.rain_shields

    with pytest.raises(ValueError, match="dubbele detection-ID"):
        replace(detected, detections=(first, first))
    overlapping = replace(
        second,
        footprint_pixels=first.footprint_pixels,
        area_m2=first.area_m2,
    )
    with pytest.raises(ValueError, match="footprints mogen niet overlappen"):
        replace(detected, detections=(first, overlapping))


def test_worker_and_ha_shadow_detector_remain_exact_mirrors() -> None:
    root = Path(__file__).resolve().parents[1]
    worker = root / "storm_tracker_worker/tracking_core/engine/hierarchical_detection.py"
    integration = root / "custom_components/storm_tracker_v3/engine/hierarchical_detection.py"

    assert worker.read_bytes() == integration.read_bytes()
