from __future__ import annotations

import ast
from pathlib import Path

import pytest

from storm_tracker_worker.tracking_core.engine.advected_footprint_evidence import (
    HARD_MAX_ADVECTED_FOOTPRINT_PAIRS,
    AdvectedFootprintCapacityError,
    FootprintEvidenceObject,
    build_advected_footprint_evidence,
    build_complete_advected_footprint_evidence,
)
from storm_tracker_worker.tracking_core.engine.analysis_grid import AnalysisGrid
from storm_tracker_worker.tracking_core.engine.shared_system_translation import (
    BinaryRainShieldFootprintFrame,
    estimate_shared_system_translation,
)


REPO_ROOT = Path(__file__).resolve().parents[1]


def _grid(
    *,
    width: int = 12,
    height: int = 10,
    x_resolution_m: float = 1_000.0,
    y_resolution_m: float = 2_000.0,
    grid_id: str = "advected-footprint-test-grid",
    origin_x_m: float = 100_000.0,
) -> AnalysisGrid:
    return AnalysisGrid(
        grid_id=grid_id,
        crs="EPSG:3035",
        origin_x_m=origin_x_m,
        origin_y_m=900_000.0,
        x_resolution_m=x_resolution_m,
        y_resolution_m=y_resolution_m,
        width=width,
        height=height,
    )


def _frame(
    frame_id: str,
    valid_at: float,
    footprints: tuple[tuple[tuple[int, int], ...], ...],
    *,
    grid: AnalysisGrid,
) -> BinaryRainShieldFootprintFrame:
    return BinaryRainShieldFootprintFrame(
        frame_id=frame_id,
        valid_at=valid_at,
        grid=grid,
        rain_shield_footprints=footprints,
    )


def _object(
    detection_id: str,
    *pixels: tuple[int, int],
) -> FootprintEvidenceObject:
    return FootprintEvidenceObject(
        detection_id=detection_id,
        footprint_pixels=tuple(pixels),
    )


def _resolved_context(
    *,
    grid: AnalysisGrid | None = None,
    row_shift: int = 1,
    column_shift: int = 2,
    elapsed_seconds: float = 300.0,
):
    selected_grid = grid or _grid()
    previous_pixels = ((1, 3), (1, 4), (2, 3))
    current_pixels = tuple(
        (row + row_shift, column + column_shift)
        for row, column in previous_pixels
    )
    previous = _frame("translation-f0", 1_000.0, (previous_pixels,), grid=selected_grid)
    current = _frame(
        "translation-f1",
        1_000.0 + elapsed_seconds,
        (current_pixels,),
        grid=selected_grid,
    )
    translation = estimate_shared_system_translation(previous, current)
    assert translation.unique_peak is True
    assert translation.selected_candidate is not None
    assert (
        translation.selected_candidate.row_shift_pixels,
        translation.selected_candidate.column_shift_pixels,
    ) == (row_shift, column_shift)
    return selected_grid, previous, current, translation


def test_zero_raw_overlap_becomes_exact_advected_iou_one() -> None:
    grid, previous_frame, current_frame, translation = _resolved_context()
    previous = _object("old", (4, 2), (4, 3), (5, 2))
    current = _object("new", (5, 4), (5, 5), (6, 4))

    evidence = build_advected_footprint_evidence(
        grid=grid,
        previous_frame=previous_frame,
        current_frame=current_frame,
        translation=translation,
        previous=previous,
        current=current,
    )

    assert evidence.mode == "advected"
    assert evidence.reason_codes == ()
    assert evidence.raw_iou == 0.0
    assert evidence.raw_intersection_pixels == 0
    assert evidence.iou == 1.0
    assert evidence.intersection_pixels == 3
    assert evidence.union_pixels == 3
    assert evidence.requested_row_shift_pixels == 1
    assert evidence.requested_column_shift_pixels == 2
    assert evidence.applied_row_shift_pixels == 1
    assert evidence.applied_column_shift_pixels == 2
    assert evidence.requested_displacement_x_m == 2_000.0
    assert evidence.requested_displacement_y_m == -2_000.0
    assert evidence.applied_displacement_x_m == 2_000.0
    assert evidence.applied_displacement_y_m == -2_000.0
    assert evidence.quantization_residual_x_m == 0.0
    assert evidence.quantization_residual_y_m == 0.0
    assert evidence.shifted_inside_grid_pixels == 3
    assert evidence.shifted_outside_grid_pixels == 0
    assert evidence.translation_result_sha256 == translation.result_sha256
    assert evidence.translation_status == translation.status
    assert evidence.translation_elapsed_seconds == translation.elapsed_seconds
    assert evidence.grid == grid
    assert len(evidence.evidence_sha256) == 64
    assert evidence.candidate_retained is True
    assert evidence.speed_gate_applied is False
    assert evidence.distance_gate_applied is False
    assert evidence.iou_gate_applied is False


def test_anisotropic_grid_and_negative_y_shift_are_exact() -> None:
    grid = _grid(
        width=12,
        height=10,
        x_resolution_m=750.0,
        y_resolution_m=1_250.0,
    )
    grid, previous_frame, current_frame, translation = _resolved_context(
        grid=grid,
        row_shift=2,
        column_shift=-1,
    )
    previous = _object("old", (2, 5), (2, 6), (3, 5))
    current = _object("new", (4, 4), (4, 5), (5, 4))

    evidence = build_advected_footprint_evidence(
        grid=grid,
        previous_frame=previous_frame,
        current_frame=current_frame,
        translation=translation,
        previous=previous,
        current=current,
    )

    assert evidence.mode == "advected"
    assert evidence.applied_row_shift_pixels == 2
    assert evidence.applied_column_shift_pixels == -1
    assert evidence.applied_displacement_x_m == -750.0
    assert evidence.applied_displacement_y_m == -2_500.0
    assert evidence.quantization_residual_x_m == 0.0
    assert evidence.quantization_residual_y_m == 0.0
    assert evidence.raw_iou == 0.0
    assert evidence.iou == 1.0


def test_shifted_outside_grid_is_retained_in_union_and_accounted() -> None:
    grid = _grid(width=8, height=5, x_resolution_m=1_000.0, y_resolution_m=1_000.0)
    grid, previous_frame, current_frame, translation = _resolved_context(
        grid=grid,
        row_shift=0,
        column_shift=2,
    )
    previous = _object("old", (2, 5), (2, 6))
    current = _object("new", (2, 7))

    evidence = build_advected_footprint_evidence(
        grid=grid,
        previous_frame=previous_frame,
        current_frame=current_frame,
        translation=translation,
        previous=previous,
        current=current,
    )

    assert evidence.mode == "advected"
    assert evidence.shifted_inside_grid_pixels == 1
    assert evidence.shifted_outside_grid_pixels == 1
    assert evidence.intersection_pixels == 1
    assert evidence.union_pixels == 2
    assert evidence.iou == 0.5
    assert evidence.raw_iou == 0.0


def test_extreme_speed_remains_an_advected_candidate_without_any_gate() -> None:
    grid = _grid(
        width=205,
        height=6,
        x_resolution_m=1_000.0,
        y_resolution_m=1_000.0,
    )
    grid, previous_frame, current_frame, translation = _resolved_context(
        grid=grid,
        row_shift=0,
        column_shift=100,
        elapsed_seconds=1.0,
    )
    assert translation.selected_candidate is not None
    assert translation.selected_candidate.interval_speed_kmh == 360_000.0
    previous = _object("old-fast", (3, 1), (3, 2), (4, 1))
    current = _object("new-fast", (3, 101), (3, 102), (4, 101))

    evidence = build_advected_footprint_evidence(
        grid=grid,
        previous_frame=previous_frame,
        current_frame=current_frame,
        translation=translation,
        previous=previous,
        current=current,
    )

    assert evidence.mode == "advected"
    assert evidence.applied_column_shift_pixels == 100
    assert evidence.iou == 1.0
    assert evidence.raw_iou == 0.0
    assert evidence.candidate_retained is True
    assert evidence.speed_gate_applied is False
    assert evidence.distance_gate_applied is False
    assert evidence.iou_gate_applied is False


def test_temporal_and_exact_source_checksum_mismatch_fall_back_to_raw() -> None:
    grid, previous_frame, current_frame, translation = _resolved_context()
    changed_current = _frame(
        current_frame.frame_id,
        current_frame.valid_at + 1.0,
        current_frame.rain_shield_footprints,
        grid=grid,
    )
    previous = _object("old", (4, 2))
    current = _object("new", (5, 4))

    evidence = build_advected_footprint_evidence(
        grid=grid,
        previous_frame=previous_frame,
        current_frame=changed_current,
        translation=translation,
        previous=previous,
        current=current,
    )

    assert evidence.mode == "unadvected_fallback"
    assert set(evidence.reason_codes) == {
        "translation_current_frame_sha_mismatch",
        "translation_elapsed_mismatch",
    }
    assert evidence.requested_row_shift_pixels == 1
    assert evidence.requested_column_shift_pixels == 2
    assert evidence.applied_row_shift_pixels == 0
    assert evidence.applied_column_shift_pixels == 0
    assert evidence.iou == evidence.raw_iou == 0.0
    assert evidence.supplied_current_frame_sha256 == changed_current.fingerprint
    assert (
        evidence.translation_current_frame_sha256
        == translation.current_frame_sha256
    )


def test_exact_grid_mismatch_falls_back_even_when_shape_and_resolution_match() -> None:
    source_grid, previous_frame, current_frame, translation = _resolved_context()
    target_grid = _grid(
        width=source_grid.width,
        height=source_grid.height,
        x_resolution_m=source_grid.x_resolution_m,
        y_resolution_m=source_grid.y_resolution_m,
        grid_id="different-grid-authority",
        origin_x_m=source_grid.origin_x_m + 10_000.0,
    )
    previous = _object("old", (4, 2))
    current = _object("new", (5, 4))

    evidence = build_advected_footprint_evidence(
        grid=target_grid,
        previous_frame=previous_frame,
        current_frame=current_frame,
        translation=translation,
        previous=previous,
        current=current,
    )

    assert evidence.mode == "unadvected_fallback"
    assert evidence.reason_codes == ("translation_grid_mismatch",)
    assert evidence.grid == target_grid
    assert evidence.applied_row_shift_pixels == 0
    assert evidence.applied_column_shift_pixels == 0
    assert evidence.iou == evidence.raw_iou == 0.0


def test_ambiguous_selected_peak_is_never_applied_and_keeps_pair() -> None:
    grid = _grid(width=5, height=5, x_resolution_m=1_000.0, y_resolution_m=1_000.0)
    previous_frame = _frame("tie-0", 0.0, (((2, 2),),), grid=grid)
    current_frame = _frame(
        "tie-1",
        60.0,
        (((2, 1), (2, 3)),),
        grid=grid,
    )
    translation = estimate_shared_system_translation(previous_frame, current_frame)
    assert translation.selected_candidate is not None
    assert translation.unique_peak is False
    previous = _object("old", (1, 1))
    current = _object("new", (1, 2))

    evidence = build_advected_footprint_evidence(
        grid=grid,
        previous_frame=previous_frame,
        current_frame=current_frame,
        translation=translation,
        previous=previous,
        current=current,
    )

    assert evidence.mode == "unadvected_fallback"
    assert evidence.reason_codes == ("translation_peak_not_unique",)
    assert evidence.translation_status == "estimated_with_uncertainty"
    assert evidence.translation_uncertainty_reason_codes == ("peak_not_unique",)
    assert evidence.requested_column_shift_pixels == -1
    assert evidence.applied_column_shift_pixels == 0
    assert evidence.iou == evidence.raw_iou == 0.0
    assert evidence.candidate_retained is True


def test_indeterminate_translation_has_no_requested_shift_and_falls_back() -> None:
    grid = _grid(width=5, height=5, x_resolution_m=1_000.0, y_resolution_m=1_000.0)
    previous_frame = _frame("empty-0", 0.0, (), grid=grid)
    current_frame = _frame("empty-1", 60.0, (), grid=grid)
    translation = estimate_shared_system_translation(previous_frame, current_frame)
    assert translation.status == "indeterminate"
    previous = _object("old", (1, 1))
    current = _object("new", (1, 1))

    evidence = build_advected_footprint_evidence(
        grid=grid,
        previous_frame=previous_frame,
        current_frame=current_frame,
        translation=translation,
        previous=previous,
        current=current,
    )

    assert evidence.mode == "unadvected_fallback"
    assert set(evidence.reason_codes) == {
        "translation_indeterminate",
        "translation_peak_not_unique",
        "translation_search_incomplete",
        "translation_selected_shift_unavailable",
    }
    assert evidence.translation_selected_candidate_id is None
    assert evidence.requested_row_shift_pixels is None
    assert evidence.requested_column_shift_pixels is None
    assert evidence.requested_displacement_x_m is None
    assert evidence.requested_displacement_y_m is None
    assert evidence.quantization_residual_x_m is None
    assert evidence.quantization_residual_y_m is None
    assert evidence.applied_row_shift_pixels == 0
    assert evidence.applied_column_shift_pixels == 0
    assert evidence.iou == evidence.raw_iou == 1.0


def test_unique_uncertain_peak_remains_visible_and_bound_as_advected_evidence() -> None:
    grid = _grid(width=5, height=3, x_resolution_m=1_000.0, y_resolution_m=1_000.0)
    previous_frame = _frame(
        "edge-0",
        0.0,
        (((0, 0), (1, 0), (2, 1)),),
        grid=grid,
    )
    current_frame = _frame(
        "edge-1",
        60.0,
        (((0, 4), (1, 4)),),
        grid=grid,
    )
    translation = estimate_shared_system_translation(previous_frame, current_frame)
    assert translation.status == "estimated_with_uncertainty"
    assert translation.unique_peak is True
    assert translation.selected_candidate is not None
    previous = _object("old", (2, 0))
    current = _object("new", (2, 4))

    evidence = build_advected_footprint_evidence(
        grid=grid,
        previous_frame=previous_frame,
        current_frame=current_frame,
        translation=translation,
        previous=previous,
        current=current,
    )

    assert evidence.mode == "advected"
    assert evidence.reason_codes == ()
    assert evidence.translation_uncertainty_reason_codes == (
        "peak_on_full_search_boundary",
    )
    assert evidence.iou == 1.0


def test_complete_cross_product_retains_every_pair_and_is_canonical() -> None:
    grid, previous_frame, current_frame, translation = _resolved_context()
    previous = (
        _object("old-b", (7, 7)),
        _object("old-a", (4, 2), (4, 3)),
    )
    current = (
        _object("new-c", (0, 0)),
        _object("new-a", (5, 4), (5, 5)),
        _object("new-b", (9, 11)),
    )

    table = build_complete_advected_footprint_evidence(
        grid=grid,
        previous_frame=previous_frame,
        current_frame=current_frame,
        translation=translation,
        previous_objects=previous,
        current_objects=current,
        maximum_pair_count=6,
    )

    assert table.expected_pair_count == 6
    assert len(table.evidence) == 6
    assert table.is_complete is True
    assert {
        (item.previous_detection_id, item.current_detection_id)
        for item in table.evidence
    } == {
        (old.detection_id, new.detection_id)
        for old in previous
        for new in current
    }
    assert all(item.candidate_retained for item in table.evidence)
    assert table.candidate_rejection_by_speed is False
    assert table.candidate_rejection_by_distance is False
    assert table.candidate_rejection_by_iou is False
    assert len(table.table_sha256) == 64


def test_complete_cross_product_capacity_fails_before_partial_table() -> None:
    grid, previous_frame, current_frame, translation = _resolved_context()
    previous = (_object("old-a", (1, 1)), _object("old-b", (2, 2)))
    current = (_object("new-a", (3, 3)), _object("new-b", (4, 4)))

    with pytest.raises(
        AdvectedFootprintCapacityError,
        match="complete advected footprint pair capacity exceeded",
    ):
        build_complete_advected_footprint_evidence(
            grid=grid,
            previous_frame=previous_frame,
            current_frame=current_frame,
            translation=translation,
            previous_objects=previous,
            current_objects=current,
            maximum_pair_count=3,
        )
    with pytest.raises(ValueError):
        build_complete_advected_footprint_evidence(
            grid=grid,
            previous_frame=previous_frame,
            current_frame=current_frame,
            translation=translation,
            previous_objects=previous,
            current_objects=current,
            maximum_pair_count=HARD_MAX_ADVECTED_FOOTPRINT_PAIRS + 1,
        )


def test_object_order_pixel_order_and_replay_have_identical_hashes() -> None:
    grid, previous_frame, current_frame, translation = _resolved_context()
    left_previous = (
        _object("old-b", (7, 7), (7, 8)),
        _object("old-a", (4, 3), (4, 2)),
    )
    right_previous = (
        _object("old-a", (4, 2), (4, 3)),
        _object("old-b", (7, 8), (7, 7)),
    )
    left_current = (
        _object("new-b", (8, 9), (8, 10)),
        _object("new-a", (5, 5), (5, 4)),
    )
    right_current = tuple(reversed(left_current))

    left = build_complete_advected_footprint_evidence(
        grid=grid,
        previous_frame=previous_frame,
        current_frame=current_frame,
        translation=translation,
        previous_objects=left_previous,
        current_objects=left_current,
        maximum_pair_count=4,
    )
    right = build_complete_advected_footprint_evidence(
        grid=grid,
        previous_frame=previous_frame,
        current_frame=current_frame,
        translation=translation,
        previous_objects=right_previous,
        current_objects=right_current,
        maximum_pair_count=4,
    )

    assert left == right
    assert left.table_sha256 == right.table_sha256
    assert left.to_mapping() == right.to_mapping()
    assert [item.evidence_sha256 for item in left.evidence] == [
        item.evidence_sha256 for item in right.evidence
    ]


def test_module_is_worker_only_pure_and_wired_only_to_shadow_tracking() -> None:
    module_path = (
        REPO_ROOT
        / "storm_tracker_worker"
        / "tracking_core"
        / "engine"
        / "advected_footprint_evidence.py"
    )
    source = module_path.read_text(encoding="utf-8")
    tree = ast.parse(source)
    imports = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    } | {
        (node.module or "")
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
    }
    assert imports <= {
        "__future__",
        "analysis_grid",
        "dataclasses",
        "hashlib",
        "json",
        "math",
        "shared_system_translation",
        "typing",
    }
    lowered = source.lower()
    assert all(
        token not in lowered
        for token in (
            "custom_components",
            "homeassistant",
            "mqtt",
            "requests",
            "service.py",
            "socket",
            "urllib",
        )
    )
    unwired = (
        REPO_ROOT / "storm_tracker_worker" / "service.py",
    )
    for path in unwired:
        assert "advected_footprint_evidence" not in path.read_text(
            encoding="utf-8"
        )
    assert "advected_footprint_evidence" in (
        REPO_ROOT
        / "storm_tracker_worker"
        / "tracking_core"
        / "engine"
        / "cell_track_engine.py"
    ).read_text(encoding="utf-8")
    assert "AdvectedFootprintEvidence" in (
        REPO_ROOT
        / "storm_tracker_worker"
        / "tracking_core"
        / "engine"
        / "multi_hypothesis_identity.py"
    ).read_text(encoding="utf-8")
    assert not (
        REPO_ROOT
        / "custom_components"
        / "storm_tracker_v3"
        / "engine"
        / "advected_footprint_evidence.py"
    ).exists()
