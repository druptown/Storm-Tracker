from __future__ import annotations

from pathlib import Path

import pytest

from storm_tracker_worker.tracking_core.engine.association_features import (
    build_association_feature_table,
)
from storm_tracker_worker.tracking_core.engine.cell_track import CellDetection
from storm_tracker_worker.tracking_core.engine.global_assignment import (
    AssignmentEdgeCost,
    GlobalAssignmentProblem,
    solve_global_assignment_with_fallback,
)
from storm_tracker_worker.tracking_core.engine.hierarchical_detection import (
    HierarchicalCellDetection,
)
from storm_tracker_worker.tracking_core.engine.motion_identity_evidence import (
    MotionEvidenceRecord,
    assess_shadow_identity_frame,
    unassessed_motion_evidence,
)
from storm_tracker_worker.tracking_core.engine.motion_prediction import (
    OpticalFlowSample,
    predict_with_optical_flow,
)


def _detection(
    identifier: str,
    valid_at: float,
    x_m: float,
    y_m: float,
    pixel: tuple[int, int],
) -> HierarchicalCellDetection:
    detection = CellDetection(
        detection_id=identifier,
        valid_at=valid_at,
        analysis_grid_id="identity-test-grid",
        x_m=x_m,
        y_m=y_m,
        position_sigma_m=500.0,
        source="synthetic-radar",
    )
    return HierarchicalCellDetection(
        detection=detection,
        object_kind="rain_shield",
        parent_detection_id=None,
        footprint_pixels=(pixel,),
        area_m2=1_000_000.0,
        geometric_centroid_x_m=x_m,
        geometric_centroid_y_m=y_m,
        value_weighted_centroid_x_m=x_m,
        value_weighted_centroid_y_m=y_m,
        mean_value=2.0,
        maximum_value=2.0,
        threshold=1.0,
        centroid_method="geometric",
    )


def _fixture(*, one_best_only: bool = False):
    previous = (
        _detection("old-a", 1_000.0, 0.0, 0.0, (0, 0)),
        _detection("old-b", 1_000.0, 10_000.0, 0.0, (0, 1)),
    )
    current = (
        # The very large displacement is retained deliberately.
        _detection("new-a", 1_060.0, 100_000.0, 0.0, (0, 0)),
        _detection("new-b", 1_060.0, 10_100.0, 0.0, (0, 1)),
    )
    predictions = tuple(
        predict_with_optical_flow(
            item.detection,
            1_060.0,
            OpticalFlowSample(
                sample_id=f"broad:{item.detection.detection_id}",
                analysis_grid_id="identity-test-grid",
                reference_valid_at=1_000.0,
                vx_m_s=0.0,
                vy_m_s=0.0,
                velocity_covariance_m2_s2=(
                    (10_000.0, 0.0),
                    (0.0, 10_000.0),
                ),
                residual_covariance_m2=(
                    (10_000_000_000.0, 0.0),
                    (0.0, 10_000_000_000.0),
                ),
                method="explicit_uncertainty_placeholder",
                support_frame_ids=("frame-old", "frame-new"),
            ),
        )
        for item in previous
    )
    features = build_association_feature_table(
        previous,
        current,
        predictions,
    )
    costs = {
        ("old-a", "new-a"): 0.0,
        ("old-a", "new-b"): 8.0,
        ("old-b", "new-a"): 8.0,
        ("old-b", "new-b"): 0.0,
    }
    edges = tuple(
        AssignmentEdgeCost(
            previous_detection_id=old_id,
            current_detection_id=new_id,
            cost=cost,
            cost_model_id="identity-test-cost-v1",
            components=(("caller_cost", cost),),
        )
        for (old_id, new_id), cost in costs.items()
    )
    problem = GlobalAssignmentProblem(
        previous_detection_ids=features.previous_detection_ids,
        current_detection_ids=features.current_detection_ids,
        edge_costs=edges,
        death_costs=(("old-a", 5.0), ("old-b", 5.0)),
        birth_costs=(("new-a", 5.0), ("new-b", 5.0)),
        policy_id="identity-test-policy-v1",
    )
    result = solve_global_assignment_with_fallback(
        problem,
        solution_limit=3,
        maximum_top_k_current_detections=0 if one_best_only else 4,
    )
    return features, result


def _evidence(status: str) -> tuple[MotionEvidenceRecord, ...]:
    return tuple(
        MotionEvidenceRecord(
            previous_detection_id=identifier,
            target_valid_at=1_060.0,
            status=status,
            reason=(
                "available"
                if status == "available"
                else "unavailable_no_selected_features"
            ),
            method="explicit_uncertainty_placeholder",
            support_frame_ids=("frame-old", "frame-new"),
            selected_vectors=1 if status == "available" else 0,
            usable_vectors=1 if status == "available" else 0,
            usable_fraction=1.0 if status == "available" else 0.0,
        )
        for identifier in ("old-a", "old-b")
    )


def test_unavailable_motion_never_becomes_confirmed_identity():
    features, result = _fixture()

    audit = assess_shadow_identity_frame(
        features,
        result,
        _evidence("unavailable"),
        ambiguity_cost_delta=0.5,
    )

    assert audit.all_candidates_retained is True
    assert len(audit.candidates) == 4
    assert audit.identity_confirmation_claimed is False
    assert audit.indeterminate_match_count == 2
    assert audit.motion_unavailable_match_count == 2
    assert {
        item.status for item in audit.best_match_assessments
    } == {"indeterminate_motion_unavailable"}
    assert all(
        item.confirmed_identity is False
        for item in audit.best_match_assessments
    )


def test_extreme_candidate_and_selected_hypothesis_are_not_speed_rejected():
    features, result = _fixture()

    audit = assess_shadow_identity_frame(
        features,
        result,
        _evidence("unavailable"),
        ambiguity_cost_delta=0.5,
    )

    extreme = next(
        item
        for item in audit.candidates
        if (
            item.previous_detection_id,
            item.current_detection_id,
        ) == ("old-a", "new-a")
    )
    selected = next(
        item
        for item in audit.best_match_assessments
        if item.previous_detection_id == "old-a"
    )
    assert extreme.observed_interval_speed_kmh == pytest.approx(6_000.0)
    assert selected.observed_interval_speed_kmh == pytest.approx(6_000.0)
    assert audit.speed_cap_kmh is None
    assert audit.candidate_rejection_by_speed is False


def test_one_best_capacity_marks_available_motion_indeterminate():
    features, result = _fixture(one_best_only=True)
    assert result.alternatives == ()

    audit = assess_shadow_identity_frame(
        features,
        result,
        _evidence("available"),
        ambiguity_cost_delta=0.5,
    )

    assert audit.alternatives_available is False
    assert {
        item.status for item in audit.best_match_assessments
    } == {"indeterminate_alternatives_not_available"}
    assert all(
        item.appears_in_all_retained_solutions is None
        for item in audit.best_match_assessments
    )


def test_unassessed_motion_is_explicit_and_fail_closed():
    features, result = _fixture()
    evidence = tuple(
        unassessed_motion_evidence(
            previous_detection_id=identifier,
            target_valid_at=1_060.0,
            method="caller-prior-without-quality-assessment",
            support_frame_ids=("frame-old",),
        )
        for identifier in ("old-a", "old-b")
    )

    audit = assess_shadow_identity_frame(
        features,
        result,
        evidence,
        ambiguity_cost_delta=0.5,
    )

    assert {
        item.status for item in audit.best_match_assessments
    } == {"indeterminate_motion_not_assessed"}


def test_candidate_hash_is_order_independent_and_full_payload_retains_solutions():
    features, result = _fixture()
    evidence = _evidence("unavailable")

    first = assess_shadow_identity_frame(
        features,
        result,
        evidence,
        ambiguity_cost_delta=0.5,
    )
    second = assess_shadow_identity_frame(
        features,
        result,
        reversed(evidence),
        ambiguity_cost_delta=0.5,
    )
    payload = first.to_dict()

    assert first.candidate_evidence_sha256 == second.candidate_evidence_sha256
    assert len(payload["candidates"]) == features.expected_feature_count
    assert len(payload["solutions"]) == len(result.solutions)
    assert payload["all_candidates_retained"] is True
    assert payload["identity_confirmation_claimed"] is False


def test_motion_evidence_must_cover_every_previous_detection():
    features, result = _fixture()

    with pytest.raises(ValueError, match="exact alle vorige detecties"):
        assess_shadow_identity_frame(
            features,
            result,
            _evidence("unavailable")[:1],
            ambiguity_cost_delta=0.5,
        )


def test_home_assistant_and_worker_modules_are_exact_mirrors():
    repository = Path(__file__).resolve().parents[1]
    home_assistant = (
        repository
        / "custom_components"
        / "storm_tracker_v3"
        / "engine"
        / "motion_identity_evidence.py"
    )
    worker = (
        repository
        / "storm_tracker_worker"
        / "tracking_core"
        / "engine"
        / "motion_identity_evidence.py"
    )

    assert home_assistant.read_bytes() == worker.read_bytes()
