"""End-to-end proof for the wholly unwired shadow CellTrackEngine."""
from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
import json
from pathlib import Path

import pytest

from storm_tracker_worker.tracking_core.engine.analysis_grid import AnalysisGrid
from storm_tracker_worker.tracking_core.engine.cell_track_engine import (
    CellTrackEnginePolicy,
    CellTrackLineageHypothesis,
    LineageChildIdentity,
    advance_cell_track_engine,
    create_cell_track_engine_state,
)
from storm_tracker_worker.tracking_core.engine.cell_track_lifecycle import (
    CellTrackLifecyclePolicy,
)
from storm_tracker_worker.tracking_core.engine.hierarchical_detection import (
    HierarchicalDetectorConfig,
    NormalizedRasterFrame,
)
from storm_tracker_worker.tracking_core.engine.global_assignment import (
    AssignmentEdgeCost,
    GlobalAssignmentProblem,
    solve_bounded_k_best_global_assignment,
    solve_exact_global_assignment,
)
from storm_tracker_worker.tracking_core.engine.lucas_kanade_flow import (
    DetectionFlowAggregationConfig,
    LucasKanadeConfig,
)
from storm_tracker_worker.tracking_core.engine.motion_prediction import (
    MotionPrediction,
)
from storm_tracker_worker.tracking_core.engine.multi_hypothesis_identity import (
    MultiHypothesisIdentityPolicy,
    MultiScanIdentityPolicy,
)
from storm_tracker_worker.tracking_core.engine.recursive_track_estimator import (
    RecursiveTrackEstimatorPolicy,
)
from storm_tracker_worker.tracking_core.engine.weather_system_grouping import (
    WeatherSystemGroupingPolicy,
)
from storm_tracker_worker.science_shadow_orchestrator import (
    ScienceShadowStoreError,
    _decode_state_value,
    _encode_state_value,
)


ROOT = Path(__file__).resolve().parents[1]
ZERO4 = (
    (0.0, 0.0, 0.0, 0.0),
    (0.0, 0.0, 0.0, 0.0),
    (0.0, 0.0, 0.0, 0.0),
    (0.0, 0.0, 0.0, 0.0),
)
INITIAL4 = (
    (1_000_000.0, 0.0, 0.0, 0.0),
    (0.0, 1_000_000.0, 0.0, 0.0),
    (0.0, 0.0, 400.0, 0.0),
    (0.0, 0.0, 0.0, 400.0),
)


def _grid(
    *,
    grid_id: str = "cell-engine-grid",
    width: int = 40,
    height: int = 9,
    resolution_m: float = 1_000.0,
) -> AnalysisGrid:
    return AnalysisGrid(
        grid_id=grid_id,
        crs="EPSG:3035",
        origin_x_m=4_000_000.0,
        origin_y_m=3_100_000.0,
        x_resolution_m=resolution_m,
        y_resolution_m=resolution_m,
        width=width,
        height=height,
    )


def _policy(**changes) -> CellTrackEnginePolicy:
    policy = CellTrackEnginePolicy(
        policy_id="explicit-cell-engine-test-v1",
        detector=HierarchicalDetectorConfig(
            shield_threshold=1.0,
            core_threshold=5.0,
            minimum_shield_area_m2=1.0,
            minimum_core_area_m2=1.0,
            shield_position_sigma_m=500.0,
            core_position_sigma_m=300.0,
            connectivity=4,
            centroid_method="geometric",
        ),
        lifecycle=CellTrackLifecyclePolicy(
            policy_id="cell-engine-lifecycle-v1",
            confirmation_observations=2,
            maximum_gap_frames=2,
        ),
        estimator=RecursiveTrackEstimatorPolicy(
            policy_id="cell-engine-estimator-v1",
            acceleration_spectral_density_m2_s3=1.0,
        ),
        lucas_kanade=LucasKanadeConfig(
            window_radius_pixels=2,
            minimum_valid_samples=6,
            minimum_structure_eigenvalue=1e-9,
            maximum_condition_number=1e10,
            regularization=0.0,
            residual_variance_floor=1e-6,
            velocity_process_noise_covariance_m2_s2=(
                (1.0, 0.0),
                (0.0, 1.0),
            ),
        ),
        flow_aggregation=DetectionFlowAggregationConfig(
            minimum_valid_pixels=1,
            minimum_valid_fraction=0.1,
            velocity_covariance_floor_m2_s2=((1.0, 0.0), (0.0, 1.0)),
            residual_position_covariance_m2=(
                (100_000.0, 0.0),
                (0.0, 100_000.0),
            ),
            method="conservative_mean",
        ),
        weather_system=WeatherSystemGroupingPolicy(
            policy_id="cell-engine-system-view-v1",
            pixel_connectivity=8,
            maximum_footprint_gap_m=2_000.0,
            maximum_motion_difference_m_s=100_000_000.0,
            maximum_motion_age_s=10_000.0,
            maximum_motion_time_difference_s=10_000.0,
        ),
        initial_state_covariance=INITIAL4,
        prior_process_noise_covariance=ZERO4,
        association_motion_sigma_m_s=100.0,
        motion_nll_weight=1.0,
        object_kind_change_cost=1_000_000.0,
        absolute_log_area_weight=1.0,
        mean_value_difference_weight=0.0,
        maximum_value_difference_weight=0.0,
        footprint_nonoverlap_weight=1.0,
        birth_cost=1e15,
        death_cost=1e15,
        exact_solution_limit=4,
        maximum_exact_current_detections=16,
        maximum_frame_pixels=10_000,
        maximum_detected_objects=200,
        maximum_active_tracks=200,
        maximum_candidate_edges=20_000,
        maximum_total_tracks=1_000,
        maximum_lifecycle_events=5_000,
        maximum_track_history_detections=200,
    )
    return replace(policy, **changes)


def _frame(
    frame_id: str,
    valid_at: float,
    pixels: tuple[tuple[int, int, float], ...],
    *,
    grid: AnalysisGrid | None = None,
    source: str = "synthetic-radar",
) -> NormalizedRasterFrame:
    selected_grid = grid or _grid()
    values = [
        [0.0 for _column in range(selected_grid.width)]
        for _row in range(selected_grid.height)
    ]
    for row, column, value in pixels:
        values[row][column] = value
    return NormalizedRasterFrame(
        frame_id=frame_id,
        valid_at=valid_at,
        source=source,
        quantity="rain_rate",
        unit="mm/h",
        grid=selected_grid,
        values=tuple(tuple(row) for row in values),
    )


def _state(policy: CellTrackEnginePolicy | None = None):
    selected = policy or _policy()
    return create_cell_track_engine_state("shadow-engine-test", selected), selected


def _multi_identity_policy(**changes) -> MultiHypothesisIdentityPolicy:
    policy = MultiHypothesisIdentityPolicy(
        policy_id="bounded-multi-hypothesis-test-v1",
        solution_limit=4,
        maximum_assignment_subproblems=256,
        maximum_retained_local_candidates=20_000,
        maximum_derived_lineage_hypotheses=256,
        maximum_lineage_members=32,
        minimum_system_translation_support_tracks=2,
        conditional_cost_temperature=2.0,
        system_translation_sigma_floor_m=500.0,
        system_translation_sigma_ceiling_m=5_000.0,
        local_deformation_sigma_floor_m=1_000.0,
        local_deformation_sigma_ceiling_m=3_000.0,
        previous_local_deformation_weight=0.5,
        local_deformation_cost_weight=1.0,
    )
    return replace(policy, **changes)


def _multi_tracking_policy(**changes) -> CellTrackEnginePolicy:
    policy = _policy(
        motion_nll_weight=0.0,
        object_kind_change_cost=50.0,
        birth_cost=6.0,
        death_cost=6.0,
        multi_hypothesis_identity=_multi_identity_policy(),
        scalable_one_best_fallback=False,
    )
    return replace(policy, **changes)


def _multi_scan_tracking_policy(**changes) -> CellTrackEnginePolicy:
    identity = _multi_identity_policy(
        multi_scan=MultiScanIdentityPolicy(
            policy_id="bounded-multi-scan-test-v1",
            horizon_frames=3,
            beam_width=8,
            maximum_generated_branches=64,
            maximum_assignment_subproblems_total=8_192,
            maximum_candidate_edges_total=200_000,
            maximum_active_paths_per_branch=200,
            maximum_buffered_detections=2_000,
            maximum_lineage_alternatives=1_000,
            conditional_cost_temperature=2.0,
        )
    )
    policy = _multi_tracking_policy(multi_hypothesis_identity=identity)
    return replace(policy, **changes)


def _active(state):
    return tuple(
        item for item in state.ledger.tracks if item.lifecycle_state != "ended"
    )


def test_bounded_murty_top_k_matches_small_exact_dynamic_program():
    previous = ("old-a", "old-b", "old-c")
    current = ("new-a", "new-b", "new-c", "new-d")
    costs = (
        (1.0, 8.0, 6.0, 7.0),
        (9.0, 1.5, 5.0, 4.0),
        (7.0, 6.0, 1.25, 3.0),
    )
    problem = GlobalAssignmentProblem(
        previous_detection_ids=previous,
        current_detection_ids=current,
        edge_costs=tuple(
            AssignmentEdgeCost(old, new, costs[row][column], "top-k-test")
            for row, old in enumerate(previous)
            for column, new in enumerate(current)
        ),
        death_costs=tuple((item, 4.0) for item in previous),
        birth_costs=tuple((item, 4.0) for item in current),
        policy_id="top-k-test",
    )
    expected = solve_exact_global_assignment(
        problem,
        solution_limit=4,
        maximum_current_detections=4,
    )
    actual = solve_bounded_k_best_global_assignment(
        problem,
        solution_limit=4,
        maximum_subproblems=256,
    )

    assert actual.solver == "bounded_exact_k_best_murty_hungarian"
    assert actual.exact and not actual.capacity_exhausted
    assert actual.explored_subproblems <= actual.maximum_subproblems
    assert tuple(
        (item.matches, item.deaths, item.births) for item in actual.solutions
    ) == tuple(
        (item.matches, item.deaths, item.births) for item in expected.solutions
    )
    assert tuple(item.total_cost for item in actual.solutions) == pytest.approx(
        tuple(item.total_cost for item in expected.solutions)
    )


def test_multi_hypothesis_separates_scene_translation_and_local_deformation():
    policy = _multi_tracking_policy()
    state = create_cell_track_engine_state("shadow-engine-test", policy)
    first = advance_cell_track_engine(
        state,
        _frame("multi-1", 1_000.0, ((4, 2, 2.0), (4, 10, 2.0))),
        policy,
    )
    second = advance_cell_track_engine(
        first.next_state,
        _frame("multi-2", 1_060.0, ((4, 4, 2.0), (4, 13, 2.0))),
        policy,
    )

    snapshot = second.identity_snapshot
    assert second.status == "advanced"
    assert snapshot is second.next_state.identity_snapshot
    assert snapshot is not None
    # Two identical one-pixel systems produce an honestly tied overlap peak.
    # Identity must therefore use explicit unadvected receipts, never the old
    # area-centroid pseudo-vector.
    assert not snapshot.translation.available
    assert snapshot.translation.displacement_x_m is None
    assert all(
        item.advected_footprint_evidence.mode == "unadvected_fallback"
        and "translation_peak_not_unique"
        in item.advected_footprint_evidence.reason_codes
        for item in snapshot.local_candidates
    )
    assert snapshot.candidate_edge_count == snapshot.expected_candidate_edge_count == 4
    assert len(snapshot.local_candidates) == 4
    assert len(snapshot.hypotheses) == 4
    assert sum(item.conditional_weight for item in snapshot.hypotheses) == pytest.approx(1.0)
    assert snapshot.omitted_hypothesis_mass_unknown
    assert len(tuple(item for item in snapshot.local_candidates if item.selected_in_best)) == 2
    assert all(
        any(
            name == "local_deformation_given_system_translation"
            for name, _value in edge.components
        )
        and all(name != "motion_nll" for name, _value in edge.components)
        for edge in second.assignment_problem.edge_costs
    )
    assert second.assignment_result.solver == "bounded_exact_k_best_murty_hungarian"
    assert second.assignment_result.alternatives

    repeated = advance_cell_track_engine(
        first.next_state,
        _frame("multi-2", 1_060.0, ((4, 4, 2.0), (4, 13, 2.0))),
        policy,
    )
    assert repeated == second


def test_multi_hypothesis_retains_unselected_split_and_merge_lineage():
    policy = _multi_tracking_policy()
    state = create_cell_track_engine_state("shadow-engine-test", policy)
    first = advance_cell_track_engine(
        state,
        _frame(
            "morph-1",
            2_000.0,
            ((4, 4, 2.0), (4, 5, 2.0), (4, 6, 2.0)),
        ),
        policy,
    )
    split = advance_cell_track_engine(
        first.next_state,
        _frame("morph-2", 2_060.0, ((4, 4, 2.0), (4, 6, 2.0))),
        policy,
    )
    split_hypotheses = tuple(
        item for item in split.lineage_hypotheses if item.event_type == "split"
    )

    assert len(split_hypotheses) == 1
    assert not split_hypotheses[0].selected
    assert len(split_hypotheses[0].children) == 2
    assert split.applied_lineage_hypothesis_ids == ()
    assert split.identity_snapshot is not None
    assert split.identity_snapshot.derived_lineage_hypothesis_ids == (
        split_hypotheses[0].hypothesis_id,
    )
    split_receipts = {
        item.advected_footprint_evidence.evidence_sha256
        for item in split.identity_snapshot.local_candidates
        if item.current_detection_id in {
            child.detection_id for child in split_hypotheses[0].children
        }
    }
    assert split_receipts <= set(split_hypotheses[0].provenance_ids)

    merged = advance_cell_track_engine(
        split.next_state,
        _frame(
            "morph-3",
            2_120.0,
            ((4, 4, 2.0), (4, 5, 2.0), (4, 6, 2.0)),
        ),
        policy,
    )
    merge_hypotheses = tuple(
        item for item in merged.lineage_hypotheses if item.event_type == "merge"
    )
    assert len(merge_hypotheses) == 1
    assert not merge_hypotheses[0].selected
    assert len(merge_hypotheses[0].parent_track_ids) == 2
    assert merged.applied_lineage_hypothesis_ids == ()
    assert merged.identity_snapshot is not None
    merge_receipts = {
        item.advected_footprint_evidence.evidence_sha256
        for item in merged.identity_snapshot.local_candidates
    }
    assert merge_receipts <= set(merge_hypotheses[0].provenance_ids)


@pytest.mark.parametrize(
    ("multi_policy", "reason"),
    (
        (
            _multi_identity_policy(maximum_assignment_subproblems=1),
            "top_k_assignment_subproblem_capacity_exceeded",
        ),
        (
            _multi_identity_policy(maximum_retained_local_candidates=3),
            "local_deformation_candidate_capacity_exceeded",
        ),
    ),
)
def test_multi_hypothesis_capacity_is_explicit_and_keeps_exact_old_state(
    multi_policy,
    reason,
):
    policy = _multi_tracking_policy(multi_hypothesis_identity=multi_policy)
    state = create_cell_track_engine_state("shadow-engine-test", policy)
    first = advance_cell_track_engine(
        state,
        _frame("multi-cap-1", 3_000.0, ((4, 2, 2.0), (4, 10, 2.0))),
        policy,
    )
    old_state = first.next_state
    result = advance_cell_track_engine(
        old_state,
        _frame("multi-cap-2", 3_060.0, ((4, 4, 2.0), (4, 13, 2.0))),
        policy,
    )

    assert result.status == "indeterminate"
    assert result.reason_codes == (reason,)
    assert result.next_state is old_state
    assert result.candidate_edge_count == 4
    if reason.startswith("top_k"):
        assert result.identity_snapshot is not None
        assert result.assignment_result.capacity_exhausted
    else:
        assert result.identity_snapshot is None


def test_multi_hypothesis_never_speed_gates_even_extreme_translation():
    grid = _grid(width=4, height=3, resolution_m=100_000_000.0)
    policy = _multi_tracking_policy()
    state = create_cell_track_engine_state("shadow-engine-test", policy)
    first = advance_cell_track_engine(
        state,
        _frame("multi-extreme-1", 4_000.0, ((1, 0, 2.0),), grid=grid),
        policy,
    )
    second = advance_cell_track_engine(
        first.next_state,
        _frame("multi-extreme-2", 4_060.0, ((1, 3, 2.0),), grid=grid),
        policy,
    )

    assert second.status == "advanced"
    assert second.association_feature_tables[0].features[0].observed_interval_speed_kmh == (
        18_000_000.0
    )
    assert second.identity_snapshot.translation.interval_speed_kmh == 18_000_000.0
    assert second.assignment_result.best.matches
    evidence = second.identity_snapshot.local_candidates[0].advected_footprint_evidence
    assert evidence.mode == "advected"
    assert evidence.iou == 1.0 and evidence.raw_iou == 0.0
    assert not evidence.speed_gate_applied
    assert not evidence.distance_gate_applied
    assert not evidence.iou_gate_applied


def test_advected_receipt_is_identical_across_main_local_and_multi_scan_layers():
    grid = _grid(width=35, height=15)
    policy = replace(
        _multi_scan_tracking_policy(),
        birth_cost=1e12,
        death_cost=1e12,
    )
    state = create_cell_track_engine_state("two-system-shared-translation", policy)
    first_pixels = (
        (2, 2, 2.0), (2, 3, 2.0), (2, 4, 2.0),
        (2, 25, 2.0), (3, 25, 2.0), (4, 25, 2.0), (5, 25, 2.0),
    )
    second_pixels = (
        (2, 8, 2.0), (2, 9, 2.0), (2, 10, 2.0),
        (9, 25, 2.0), (10, 25, 2.0), (11, 25, 2.0), (12, 25, 2.0),
    )
    first = advance_cell_track_engine(
        state,
        _frame("two-system-1", 20_000.0, first_pixels, grid=grid),
        policy,
    )
    second = advance_cell_track_engine(
        first.next_state,
        _frame("two-system-2", 20_060.0, second_pixels, grid=grid),
        policy,
    )

    assert second.status == "advanced", second.reason_codes
    snapshot = second.identity_snapshot
    posterior = second.multi_scan_posterior
    assert snapshot is not None and posterior is not None
    selected = tuple(item for item in snapshot.local_candidates if item.selected_in_best)
    assert len(selected) == 2
    assert {
        (
            item.advected_footprint_evidence.applied_displacement_x_m,
            item.advected_footprint_evidence.applied_displacement_y_m,
        )
        for item in selected
    } == {(6_000.0, 0.0), (0.0, -7_000.0)}
    assert all(
        item.advected_footprint_evidence.mode == "advected"
        and item.advected_footprint_evidence.raw_iou == 0.0
        and item.advected_footprint_evidence.iou == 1.0
        and item.system_translation_evidence_id
        == item.advected_footprint_evidence.translation_result_sha256
        and item.association_cost_sha256 is not None
        for item in selected
    )
    assert len({item.system_translation_evidence_id for item in selected}) == 2
    edge_by_pair = {
        (item.previous_detection_id, item.current_detection_id): item
        for item in second.assignment_problem.edge_costs
    }
    for item in selected:
        edge = edge_by_pair[(item.previous_detection_id, item.current_detection_id)]
        assert ("advected_footprint_nonoverlap", 0.0) in edge.components
    matched_advected = set(
        posterior.branches[0].steps[-1].matched_advected_footprint_evidence_sha256
    )
    assert matched_advected == {
        item.advected_footprint_evidence.evidence_sha256 for item in selected
    }


def test_gap_pair_forces_zero_shift_and_same_receipt_reaches_multi_scan():
    grid = _grid(width=30, height=12)
    policy = replace(
        _multi_scan_tracking_policy(),
        birth_cost=1e12,
        death_cost=1e12,
    )
    state = create_cell_track_engine_state("advected-gap-fallback", policy)
    frames = (
        _frame(
            "adv-gap-1", 30_000.0,
            (
                (2, 2, 2.0), (2, 3, 2.0), (2, 4, 2.0),
                (2, 20, 2.0), (3, 20, 2.0), (4, 20, 2.0), (5, 20, 2.0),
            ),
            grid=grid,
        ),
        _frame(
            "adv-gap-2", 30_060.0,
            (
                (2, 4, 2.0), (2, 5, 2.0), (2, 6, 2.0),
                (3, 20, 2.0), (4, 20, 2.0), (5, 20, 2.0), (6, 20, 2.0),
            ),
            grid=grid,
        ),
        _frame(
            "adv-gap-3", 30_120.0,
            ((4, 20, 2.0), (5, 20, 2.0), (6, 20, 2.0), (7, 20, 2.0)),
            grid=grid,
        ),
    )
    for frame in frames:
        transition = advance_cell_track_engine(state, frame, policy)
        assert transition.status == "advanced", transition.reason_codes
        state = transition.next_state
    reacquired = advance_cell_track_engine(
        state,
        _frame(
            "adv-gap-4", 30_180.0,
            (
                (2, 10, 2.0), (2, 11, 2.0), (2, 12, 2.0),
                (5, 20, 2.0), (6, 20, 2.0), (7, 20, 2.0), (8, 20, 2.0),
            ),
            grid=grid,
        ),
        policy,
    )

    assert reacquired.status == "advanced", reacquired.reason_codes
    snapshot = reacquired.identity_snapshot
    posterior = reacquired.multi_scan_posterior
    assert snapshot is not None and posterior is not None
    candidate = next(
        item for item in snapshot.local_candidates
        if item.selected_in_best
        and "previous_detection_outside_translation_time_window"
        in item.advected_footprint_evidence.reason_codes
    )
    evidence = candidate.advected_footprint_evidence
    assert evidence.mode == "unadvected_fallback"
    assert "previous_detection_outside_translation_time_window" in evidence.reason_codes
    assert evidence.applied_displacement_x_m == 0.0
    assert evidence.applied_displacement_y_m == 0.0
    # Another live system yields a usable one-frame vector, but this gap pair
    # deliberately records it only as requested evidence and never applies it.
    assert evidence.translation_selected_candidate_id is not None
    assert (
        evidence.requested_displacement_x_m,
        evidence.requested_displacement_y_m,
    ) != (0.0, 0.0)
    assert any(
        evidence.evidence_sha256
        in step.matched_advected_footprint_evidence_sha256
        for branch in posterior.branches
        for step in branch.steps[-1:]
    )


def test_advected_hashes_change_with_feature_and_old_checkpoint_fails_closed():
    policy = _multi_scan_tracking_policy()
    state = create_cell_track_engine_state("advected-hash-drift", policy)
    first = advance_cell_track_engine(
        state,
        _frame("adv-hash-1", 40_000.0, ((4, 3, 2.0), (4, 4, 2.0))),
        policy,
    )
    current = _frame("adv-hash-2", 40_060.0, ((4, 5, 2.0), (4, 6, 2.0)))
    baseline = advance_cell_track_engine(first.next_state, current, policy)
    replay = advance_cell_track_engine(first.next_state, current, policy)
    variant = advance_cell_track_engine(
        first.next_state,
        _frame("adv-hash-2", 40_060.0, ((4, 5, 3.0), (4, 6, 3.0))),
        policy,
    )
    assert replay == baseline
    assert baseline.identity_snapshot is not None
    assert baseline.multi_scan_posterior is not None
    assert variant.identity_snapshot is not None
    assert variant.multi_scan_posterior is not None
    assert (
        baseline.identity_snapshot.local_candidates[0].association_evidence_sha256
        != variant.identity_snapshot.local_candidates[0].association_evidence_sha256
    )
    assert baseline.identity_snapshot.snapshot_id != variant.identity_snapshot.snapshot_id
    assert baseline.multi_scan_posterior.posterior_id != variant.multi_scan_posterior.posterior_id

    encoded = _encode_state_value(baseline.next_state)
    restored = _decode_state_value(encoded)
    assert restored == baseline.next_state
    third = _frame("adv-hash-3", 40_120.0, ((4, 7, 2.0), (4, 8, 2.0)))
    assert advance_cell_track_engine(restored, third, policy) == (
        advance_cell_track_engine(baseline.next_state, third, policy)
    )
    assert encoded["type"] == "CellTrackEngineState"
    encoded["fields"]["algorithm_version"] = "stv3-shadow-cell-track-engine-v3"
    with pytest.raises(ScienceShadowStoreError, match="failed validation"):
        _decode_state_value(encoded)


def test_shared_translation_context_cap_is_fail_closed_before_assignment():
    identity = _multi_identity_policy(maximum_shared_translation_contexts=1)
    policy = _multi_tracking_policy(multi_hypothesis_identity=identity)
    grid = _grid(width=30, height=10)
    state = create_cell_track_engine_state("translation-context-cap", policy)
    first = advance_cell_track_engine(
        state,
        _frame(
            "translation-cap-1",
            50_000.0,
            ((2, 2, 2.0), (2, 3, 2.0), (6, 20, 2.0), (7, 20, 2.0)),
            grid=grid,
        ),
        policy,
    )
    old_state = first.next_state
    result = advance_cell_track_engine(
        old_state,
        _frame(
            "translation-cap-2",
            50_060.0,
            ((2, 4, 2.0), (2, 5, 2.0), (6, 22, 2.0), (7, 22, 2.0)),
            grid=grid,
        ),
        policy,
    )
    assert result.status == "indeterminate"
    assert result.reason_codes == ("shared_translation_context_capacity_exceeded",)
    assert result.next_state is old_state
    assert result.assignment_problem is None


def test_multi_scan_crossing_retains_branches_and_commits_n_scan_ancestor():
    policy = _multi_scan_tracking_policy()
    state = create_cell_track_engine_state("multi-scan-crossing", policy)
    frames = (
        _frame("ms-cross-1", 1_000.0, ((4, 5, 2.0), (4, 15, 2.0))),
        _frame("ms-cross-2", 1_600.0, ((4, 7, 2.0), (4, 13, 2.0))),
        _frame("ms-cross-3", 2_200.0, ((4, 9, 2.0), (4, 11, 2.0))),
        _frame("ms-cross-4", 2_800.0, ((4, 7, 2.0), (4, 13, 2.0))),
        _frame("ms-cross-5", 3_400.0, ((4, 5, 2.0), (4, 15, 2.0))),
    )
    posteriors = []
    for frame in frames:
        transition = advance_cell_track_engine(state, frame, policy)
        assert transition.status == "advanced", transition.reason_codes
        state = transition.next_state
        assert state.multi_scan_posterior is not None
        posteriors.append(state.multi_scan_posterior)

    assert len(posteriors[1].branches) > 1
    assert posteriors[-1].committed_through_revision == 2
    assert posteriors[-1].committed_decision_count == 2
    assert posteriors[-1].newly_committed_step is not None
    assert posteriors[-1].newly_committed_step.revision == 2
    assert all(len(item.steps) <= 3 for item in posteriors[-1].branches)
    assert posteriors[-1].omitted_hypothesis_mass_unknown
    replay = create_cell_track_engine_state("multi-scan-crossing", policy)
    for frame in frames:
        replay = advance_cell_track_engine(replay, frame, policy).next_state
    assert replay == state


def test_multi_scan_separates_shared_translation_from_branch_local_deformation():
    policy = _multi_scan_tracking_policy()
    state = create_cell_track_engine_state("multi-scan-deformation", policy)
    first = advance_cell_track_engine(
        state,
        _frame("ms-def-1", 4_000.0, ((4, 5, 2.0), (4, 15, 2.0))),
        policy,
    )
    second = advance_cell_track_engine(
        first.next_state,
        _frame("ms-def-2", 4_600.0, ((4, 7, 2.0), (4, 18, 2.0))),
        policy,
    )

    assert second.status == "advanced"
    assert second.identity_snapshot is not None
    assert not second.identity_snapshot.translation.available
    posterior = second.multi_scan_posterior
    assert posterior is not None
    selected = posterior.branches[0]
    deformations = sorted(item.local_deformation_x_m for item in selected.paths)
    assert deformations == [2_000.0, 3_000.0]
    assert posterior.conditional_entropy_nats >= 0.0
    assert 1.0 <= posterior.effective_hypothesis_count <= len(posterior.branches)
    assert sum(item.conditional_weight for item in posterior.branches) == pytest.approx(1.0)


def test_multi_scan_missing_detection_keeps_gap_identity_and_reacquires():
    policy = _multi_scan_tracking_policy()
    state = create_cell_track_engine_state("multi-scan-missing", policy)
    frames = (
        _frame("ms-gap-1", 5_000.0, ((4, 5, 2.0), (4, 15, 2.0))),
        _frame("ms-gap-2", 5_600.0, ((4, 6, 2.0), (4, 16, 2.0))),
        _frame("ms-gap-3", 6_200.0, ((4, 7, 2.0),)),
        _frame("ms-gap-4", 6_800.0, ((4, 8, 2.0), (4, 18, 2.0))),
    )
    selected_by_frame = []
    for frame in frames:
        transition = advance_cell_track_engine(state, frame, policy)
        assert transition.status == "advanced", transition.reason_codes
        state = transition.next_state
        posterior = state.multi_scan_posterior
        assert posterior is not None
        selected_by_frame.append(posterior.branches[0])

    gap_paths = tuple(item for item in selected_by_frame[2].paths if item.missed_frames)
    assert len(gap_paths) == 1
    gap_identity = gap_paths[0].identity_id
    reacquired = next(
        item for item in selected_by_frame[3].paths
        if item.identity_id == gap_identity
    )
    assert reacquired.missed_frames == 0
    # Within the uncommitted N-scan horizon the new MAP branch may legitimately
    # come from another retained ancestor with a longer detection history.
    assert len(reacquired.detection_ids) >= 3
    assert selected_by_frame[3].steps[-1].matches


def test_multi_scan_retains_explicit_unselected_split_and_merge_alternatives():
    policy = _multi_scan_tracking_policy()
    state = create_cell_track_engine_state("multi-scan-lineage", policy)
    frames = (
        _frame(
            "ms-lineage-1",
            7_000.0,
            ((4, 5, 2.0), (4, 6, 2.0), (4, 7, 2.0)),
        ),
        _frame("ms-lineage-2", 7_600.0, ((4, 5, 2.0), (4, 7, 2.0))),
        _frame(
            "ms-lineage-3",
            8_200.0,
            ((4, 5, 2.0), (4, 6, 2.0), (4, 7, 2.0)),
        ),
    )
    event_types = []
    for frame in frames:
        transition = advance_cell_track_engine(state, frame, policy)
        assert transition.status == "advanced", transition.reason_codes
        state = transition.next_state
        posterior = state.multi_scan_posterior
        assert posterior is not None
        event_types.extend(item.event_type for item in posterior.lineage_alternatives)
        assert all(not item.selected for item in posterior.lineage_alternatives)
        referenced = {
            alternative_id
            for branch in posterior.branches
            for step in branch.steps
            for alternative_id in step.lineage_alternative_ids
        }
        if posterior.newly_committed_step is not None:
            referenced.update(
                posterior.newly_committed_step.lineage_alternative_ids
            )
        assert referenced == {
            item.alternative_id for item in posterior.lineage_alternatives
        }

    assert "split" in event_types
    assert "merge" in event_types
    assert not {
        item.event_type for item in state.ledger.lineage_events
    }.intersection({"split", "merge"})


@pytest.mark.parametrize(
    ("multi_scan", "reason"),
    (
        (
            MultiScanIdentityPolicy(
                policy_id="multi-scan-generated-cap",
                horizon_frames=3,
                beam_width=1,
                maximum_generated_branches=1,
                maximum_assignment_subproblems_total=8_192,
                maximum_candidate_edges_total=100,
                maximum_active_paths_per_branch=20,
                maximum_buffered_detections=100,
                maximum_lineage_alternatives=100,
                conditional_cost_temperature=2.0,
            ),
            "multi_scan_generated_branch_capacity_exceeded",
        ),
        (
            MultiScanIdentityPolicy(
                policy_id="multi-scan-candidate-cap",
                horizon_frames=3,
                beam_width=1,
                maximum_generated_branches=4,
                maximum_assignment_subproblems_total=8_192,
                maximum_candidate_edges_total=3,
                maximum_active_paths_per_branch=20,
                maximum_buffered_detections=100,
                maximum_lineage_alternatives=100,
                conditional_cost_temperature=2.0,
            ),
            "multi_scan_candidate_edge_capacity_exceeded",
        ),
        (
            MultiScanIdentityPolicy(
                policy_id="multi-scan-subproblem-cap",
                horizon_frames=3,
                beam_width=1,
                maximum_generated_branches=4,
                maximum_assignment_subproblems_total=1,
                maximum_candidate_edges_total=100,
                maximum_active_paths_per_branch=20,
                maximum_buffered_detections=100,
                maximum_lineage_alternatives=100,
                conditional_cost_temperature=2.0,
            ),
            "multi_scan_assignment_subproblem_capacity_exceeded",
        ),
    ),
)
def test_multi_scan_capacity_is_fail_closed_without_partial_head(
    multi_scan,
    reason,
):
    identity = _multi_identity_policy(multi_scan=multi_scan)
    policy = _multi_tracking_policy(multi_hypothesis_identity=identity)
    state = create_cell_track_engine_state("multi-scan-capacity", policy)
    first = advance_cell_track_engine(
        state,
        _frame("ms-cap-1", 9_000.0, ((4, 5, 2.0), (4, 15, 2.0))),
        policy,
    )
    old_state = first.next_state
    second = advance_cell_track_engine(
        old_state,
        _frame("ms-cap-2", 9_600.0, ((4, 7, 2.0), (4, 13, 2.0))),
        policy,
    )
    assert second.status == "indeterminate"
    assert second.reason_codes == (reason,)
    assert second.next_state is old_state


def test_multi_scan_has_no_speed_cutoff_for_extreme_branch_candidate():
    grid = _grid(width=4, height=3, resolution_m=100_000_000.0)
    policy = _multi_scan_tracking_policy()
    state = create_cell_track_engine_state("multi-scan-extreme", policy)
    first = advance_cell_track_engine(
        state,
        _frame("ms-extreme-1", 10_000.0, ((1, 0, 2.0),), grid=grid),
        policy,
    )
    second = advance_cell_track_engine(
        first.next_state,
        _frame("ms-extreme-2", 10_060.0, ((1, 3, 2.0),), grid=grid),
        policy,
    )
    assert second.status == "advanced"
    assert second.multi_scan_posterior is not None
    assert second.multi_scan_posterior.branches[0].steps[-1].matches
    assert second.identity_snapshot.translation.interval_speed_kmh == 18_000_000.0


def test_multi_scan_thousand_frame_posterior_json_size_plateaus():
    multi_scan = MultiScanIdentityPolicy(
        policy_id="multi-scan-thousand-frame-plateau",
        horizon_frames=3,
        beam_width=1,
        maximum_generated_branches=1,
        maximum_assignment_subproblems_total=32,
        maximum_candidate_edges_total=8,
        maximum_active_paths_per_branch=4,
        maximum_buffered_detections=16,
        maximum_lineage_alternatives=8,
        conditional_cost_temperature=2.0,
    )
    identity = _multi_identity_policy(
        solution_limit=1,
        maximum_assignment_subproblems=32,
        multi_scan=multi_scan,
    )
    grid = _grid(width=3, height=3)
    policy = replace(
        _multi_tracking_policy(multi_hypothesis_identity=identity),
        lifecycle=CellTrackLifecyclePolicy(
            policy_id="multi-scan-thousand-frame-lifecycle",
            confirmation_observations=2,
            maximum_gap_frames=0,
        ),
        detector=replace(
            _policy().detector,
            minimum_shield_area_m2=1.0,
            minimum_core_area_m2=1.0,
        ),
        maximum_frame_pixels=9,
        maximum_detected_objects=4,
        maximum_active_tracks=4,
        maximum_candidate_edges=16,
        maximum_total_tracks=8,
        maximum_lifecycle_events=16,
        # The legacy primary ledger has an independent, explicit history cap.
        # Keep it above this proof so the test measures the new posterior's
        # own rolling-state plateau rather than that older fail-closed bound.
        maximum_track_history_detections=2_048,
    )
    state = create_cell_track_engine_state("multi-scan-plateau", policy)
    checkpoint_sizes = {}
    for revision in range(1, 1_025):
        transition = advance_cell_track_engine(
            state,
            _frame(
                f"multi-scan-plateau-{revision:04d}",
                20_000.0 + revision * 60.0,
                ((1, 1 + (revision % 2), 2.0),),
                grid=grid,
            ),
            policy,
        )
        assert transition.status == "advanced", transition.reason_codes
        state = transition.next_state
        posterior = state.multi_scan_posterior
        assert posterior is not None
        assert len(posterior.branches) == 1
        assert len(posterior.frame_buffer) <= 4
        assert len(posterior.branches[0].steps) <= 3
        assert all(
            len(path.detection_ids) <= 4 for path in posterior.branches[0].paths
        )
        if revision in (512, 1_024):
            checkpoint_sizes[revision] = len(json.dumps(
                _encode_state_value(posterior),
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8"))

    assert state.multi_scan_posterior.committed_through_revision == 1_021
    assert checkpoint_sizes[1_024] <= checkpoint_sizes[512] + 8_192
    assert checkpoint_sizes[1_024] < 128 * 1024


def test_two_frame_linear_motion_and_last_two_prior_are_exact_and_uncapped():
    state, policy = _state()
    first = advance_cell_track_engine(
        state,
        _frame("linear-1", 1_000.0, ((3, 2, 2.0),)),
        policy,
    )
    second = advance_cell_track_engine(
        first.next_state,
        _frame("linear-2", 1_060.0, ((3, 3, 2.0),)),
        policy,
    )
    track = _active(second.next_state)[0]
    estimate = second.next_state.estimate(track.track_id)

    assert second.status == "advanced"
    assert track.sample_count == 2
    assert estimate.latest_interval_vx_m_s == pytest.approx(1_000.0 / 60.0)
    assert estimate.latest_interval_vy_m_s == 0.0
    assert estimate.covariance[0][0] > 0.0
    assert second.association_feature_tables[0].is_complete
    assert second.association_graphs[0].is_complete
    edge = second.assignment_problem.edge_costs[0]
    assert edge.components
    assert sum(value for _name, value in edge.components) == pytest.approx(edge.cost)
    assert second.assignment_result.alternatives

    third = advance_cell_track_engine(
        second.next_state,
        _frame("linear-3", 1_120.0, ((3, 4, 2.0),)),
        policy,
    )
    assert third.motion_priors[0].method == "last_two_associated_positions"
    assert third.motion_priors[0].predicted_x_m == pytest.approx(
        third.detection_frame.detections[0].detection.x_m
    )


def test_more_than_twenty_frames_remain_one_individual_track():
    state, policy = _state()
    for index in range(25):
        result = advance_cell_track_engine(
            state,
            _frame(
                f"long-{index:02d}",
                1_000.0 + 60.0 * index,
                ((4, 2 + index, 2.0),),
            ),
            policy,
        )
        assert result.status == "advanced"
        state = result.next_state

    assert len(_active(state)) == 1
    assert _active(state)[0].sample_count == 25
    assert state.estimate(_active(state)[0].track_id).revision == 25


def test_puppies_keep_local_tracks_while_secondary_system_uses_shield_motion():
    state, policy = _state()
    first_pixels = (
        (4, 4, 9.0),
        (4, 5, 2.0),
        (4, 6, 2.0),
        (4, 7, 2.0),
        (4, 8, 9.0),
    )
    second_pixels = (
        (4, 5, 2.0),
        (4, 6, 9.0),
        (4, 7, 2.0),
        (4, 8, 2.0),
        (4, 9, 9.0),
    )
    first = advance_cell_track_engine(
        state,
        _frame("puppy-1", 2_000.0, first_pixels),
        policy,
    )
    second = advance_cell_track_engine(
        first.next_state,
        _frame("puppy-2", 2_060.0, second_pixels),
        policy,
    )

    assert len(_active(second.next_state)) == 3
    assert len(second.next_state.weather_systems.systems) == 1
    system = second.next_state.weather_systems.systems[0]
    assert len(system.member_track_ids) == 3
    assert system.descriptive_motion.method == (
        "componentwise_median_current_rain_shield_motion"
    )
    shield_input = next(
        item for item in system.member_tracks if item.object_kind == "rain_shield"
    )
    puppies = tuple(
        item for item in system.member_tracks if item.object_kind == "embedded_core"
    )
    assert system.descriptive_motion.vx_m_s == shield_input.vx_m_s
    assert len(puppies) == 2
    assert {item.track_id for item in puppies}.isdisjoint({shield_input.track_id})


def test_crossing_cells_follow_last_two_motion_not_sorted_position():
    state, policy = _state()
    frames = (
        _frame("cross-1", 3_000.0, ((4, 1, 2.0), (4, 10, 2.0))),
        _frame("cross-2", 3_060.0, ((4, 4, 2.0), (4, 7, 2.0))),
        _frame("cross-3", 3_120.0, ((4, 3, 2.0), (4, 8, 2.0))),
    )
    for frame in frames[:2]:
        state = advance_cell_track_engine(state, frame, policy).next_state
    left_origin_track = min(_active(state), key=lambda item: item.detections[0].x_m)
    right_origin_track = max(_active(state), key=lambda item: item.detections[0].x_m)
    result = advance_cell_track_engine(state, frames[2], policy)

    assert result.candidate_edge_count == 4
    assert sum(
        len(table.features) for table in result.association_feature_tables
    ) == 4
    assert sum(len(graph.candidates) for graph in result.association_graphs) == 4
    assert result.next_state.track(left_origin_track.track_id).latest_detection.x_m > (
        result.next_state.track(right_origin_track.track_id).latest_detection.x_m
    )
    assert len(result.assignment_result.alternatives) >= 1


def test_explicit_split_and_merge_end_parents_and_create_new_child_ids():
    state, policy = _state()
    born = advance_cell_track_engine(
        state,
        _frame("lineage-1", 4_000.0, ((4, 5, 2.0),)),
        policy,
    )
    parent_id = _active(born.next_state)[0].track_id
    split_frame = _frame(
        "lineage-2",
        4_060.0,
        ((4, 4, 2.0), (4, 6, 2.0)),
    )
    detected_split = advance_cell_track_engine(
        born.next_state,
        split_frame,
        policy,
    ).detection_frame
    child_detection_ids = tuple(
        item.detection.detection_id for item in detected_split.detections
    )
    split_hypothesis = CellTrackLineageHypothesis(
        hypothesis_id="explicit-split-hypothesis",
        event_type="split",
        parent_track_ids=(parent_id,),
        children=tuple(
            sorted(
                (
                    LineageChildIdentity(child_detection_ids[0], "split-child-a"),
                    LineageChildIdentity(child_detection_ids[1], "split-child-b"),
                ),
                key=lambda item: item.detection_id,
            )
        ),
        cost=1.0,
        cost_model_id="explicit-lineage-test-v1",
        cost_components=(("caller_hypothesis", 1.0),),
        provenance_ids=("synthetic-lineage-proof",),
        selected=True,
    )
    split = advance_cell_track_engine(
        born.next_state,
        split_frame,
        policy,
        lineage_hypotheses=(split_hypothesis,),
    )

    assert split.next_state.track(parent_id).termination_reason == "split"
    assert split.next_state.track("split-child-a").parent_track_ids == (parent_id,)
    assert split.next_state.track("split-child-b").parent_track_ids == (parent_id,)
    assert split.applied_lineage_hypothesis_ids == (
        "explicit-split-hypothesis",
    )

    merge_frame = _frame("lineage-3", 4_120.0, ((4, 5, 2.0),))
    merge_detection = advance_cell_track_engine(
        split.next_state,
        merge_frame,
        policy,
    ).detection_frame.detections[0].detection.detection_id
    merge_hypothesis = CellTrackLineageHypothesis(
        hypothesis_id="explicit-merge-hypothesis",
        event_type="merge",
        parent_track_ids=("split-child-a", "split-child-b"),
        children=(LineageChildIdentity(merge_detection, "merged-child"),),
        cost=2.0,
        cost_model_id="explicit-lineage-test-v1",
        cost_components=(("caller_hypothesis", 2.0),),
        provenance_ids=("synthetic-lineage-proof",),
        selected=True,
    )
    merged = advance_cell_track_engine(
        split.next_state,
        merge_frame,
        policy,
        lineage_hypotheses=(merge_hypothesis,),
    )

    assert merged.next_state.track("split-child-a").termination_reason == "merge"
    assert merged.next_state.track("split-child-b").termination_reason == "merge"
    assert merged.next_state.track("merged-child").parent_track_ids == (
        "split-child-a",
        "split-child-b",
    )
    assert tuple(
        event.event_type for event in merged.next_state.ledger.lineage_events
        if event.event_type in ("split", "merge")
    ) == ("split", "merge")


def test_gap_is_retained_and_reacquisition_keeps_track_identity():
    state, policy = _state()
    first = advance_cell_track_engine(
        state,
        _frame("gap-1", 5_000.0, ((4, 4, 2.0),)),
        policy,
    )
    track_id = _active(first.next_state)[0].track_id
    gap = advance_cell_track_engine(
        first.next_state,
        _frame("gap-2", 5_060.0, ()),
        policy,
    )
    assert gap.next_state.track(track_id).lifecycle_state == "coasting"
    reacquired = advance_cell_track_engine(
        gap.next_state,
        _frame("gap-3", 5_120.0, ((4, 5, 2.0),)),
        policy,
    )

    assert reacquired.next_state.track(track_id).sample_count == 2
    assert reacquired.next_state.track(track_id).missed_frames == 0
    assert any(
        event.event_type == "reacquired"
        and event.track_ids == (track_id,)
        for event in reacquired.next_state.ledger.lineage_events
    )


def test_gap_limit_emits_explicit_death_but_never_deletes_track_history():
    lifecycle = CellTrackLifecyclePolicy(
        policy_id="one-gap-then-death-v1",
        confirmation_observations=1,
        maximum_gap_frames=1,
    )
    policy = _policy(lifecycle=lifecycle)
    state = create_cell_track_engine_state("shadow-engine-test", policy)
    first = advance_cell_track_engine(
        state,
        _frame("death-1", 5_500.0, ((4, 4, 2.0),)),
        policy,
    )
    track_id = _active(first.next_state)[0].track_id
    first_gap = advance_cell_track_engine(
        first.next_state,
        _frame("death-2", 5_560.0, ()),
        policy,
    )
    death = advance_cell_track_engine(
        first_gap.next_state,
        _frame("death-3", 5_620.0, ()),
        policy,
    )

    ended = death.next_state.track(track_id)
    assert ended.lifecycle_state == "ended"
    assert ended.termination_reason == "gap_limit_exceeded"
    assert ended.sample_count == 1
    assert any(
        event.event_type == "death" and event.track_ids == (track_id,)
        for event in death.next_state.ledger.lineage_events
    )
    assert len(death.next_state.ledger.tracks) == 1


def test_extreme_speed_candidate_is_retained_and_can_be_assigned():
    grid = _grid(width=4, height=3, resolution_m=100_000_000.0)
    state, policy = _state()
    first = advance_cell_track_engine(
        state,
        _frame("extreme-1", 6_000.0, ((1, 0, 2.0),), grid=grid),
        policy,
    )
    second = advance_cell_track_engine(
        first.next_state,
        _frame("extreme-2", 6_060.0, ((1, 3, 2.0),), grid=grid),
        policy,
    )

    feature = second.association_feature_tables[0].features[0]
    assert feature.observed_interval_speed_kmh == 18_000_000.0
    assert second.assignment_result.best.matches
    assert _active(second.next_state)[0].sample_count == 2


def test_exact_capacity_overflow_is_indeterminate_and_loses_nothing():
    constrained = _policy(maximum_exact_current_detections=1)
    state = create_cell_track_engine_state("shadow-engine-test", constrained)
    first = advance_cell_track_engine(
        state,
        _frame("capacity-1", 7_000.0, ((4, 4, 2.0),)),
        constrained,
    )
    before = first.next_state
    overflow = advance_cell_track_engine(
        before,
        _frame("capacity-2", 7_060.0, ((4, 5, 2.0), (4, 8, 2.0))),
        constrained,
    )

    assert overflow.status == "indeterminate"
    assert overflow.next_state is before
    assert overflow.reason_codes == ("exact_global_assignment_capacity_exceeded",)
    assert len(overflow.detection_frame.detections) == 2
    assert overflow.candidate_edge_count == 2
    assert overflow.association_graphs[0].expected_candidate_count == 2
    assert overflow.association_feature_tables[0].expected_feature_count == 2
    assert len(_active(overflow.next_state)) == 1


def test_explicit_scalable_one_best_fallback_retains_every_candidate():
    scalable = _policy(
        maximum_exact_current_detections=1,
        scalable_one_best_fallback=True,
    )
    state = create_cell_track_engine_state("shadow-engine-test", scalable)
    first = advance_cell_track_engine(
        state,
        _frame("scalable-1", 7_100.0, ((4, 4, 2.0),)),
        scalable,
    )
    advanced = advance_cell_track_engine(
        first.next_state,
        _frame("scalable-2", 7_160.0, ((4, 5, 2.0), (4, 8, 2.0))),
        scalable,
    )

    assert advanced.status == "advanced"
    assert advanced.assignment_result is not None
    assert advanced.assignment_result.solver == "exact_hungarian_one_best"
    assert not advanced.assignment_result.alternatives
    assert advanced.candidate_edge_count == 2
    assert len(advanced.detection_frame.detections) == 2
    assert len(_active(advanced.next_state)) == 2


def test_secondary_group_pair_capacity_is_bounded_before_bootstrap_mutation():
    constrained = _policy(maximum_candidate_edges=1)
    state = create_cell_track_engine_state("shadow-engine-test", constrained)
    overflow = advance_cell_track_engine(
        state,
        _frame(
            "group-capacity-1",
            7_500.0,
            ((4, 2, 2.0), (4, 5, 2.0), (4, 8, 2.0)),
        ),
        constrained,
    )

    assert overflow.status == "indeterminate"
    assert overflow.next_state is state
    assert overflow.reason_codes == ("weather_system_pair_capacity_exceeded",)
    assert len(overflow.detection_frame.detections) == 3
    assert overflow.next_state.ledger.tracks == ()


def _quadratic_frame_pair():
    grid = _grid(width=25, height=23, resolution_m=1_000.0)

    def values(displacement_column: float, displacement_row: float):
        return tuple(
            tuple(
                0.031 * (column - displacement_column) ** 2
                + 0.047 * (row - displacement_row) ** 2
                + 0.019
                * (column - displacement_column)
                * (row - displacement_row)
                + 0.73 * (column - displacement_column)
                - 0.41 * (row - displacement_row)
                + 20.0
                for column in range(grid.width)
            )
            for row in range(grid.height)
        )

    return (
        NormalizedRasterFrame(
            frame_id="flow-1",
            valid_at=8_000.0,
            source="synthetic-radar",
            quantity="rain_rate",
            unit="mm/h",
            grid=grid,
            values=values(0.0, 0.0),
        ),
        NormalizedRasterFrame(
            frame_id="flow-2",
            valid_at=8_060.0,
            source="synthetic-radar",
            quantity="rain_rate",
            unit="mm/h",
            grid=grid,
            values=values(0.75, 0.5),
        ),
    )


def test_cold_start_retains_real_lucas_kanade_prior_when_available():
    detector = HierarchicalDetectorConfig(
        shield_threshold=1.0,
        core_threshold=1_000_000.0,
        minimum_shield_area_m2=1.0,
        minimum_core_area_m2=1.0,
        shield_position_sigma_m=500.0,
        core_position_sigma_m=300.0,
        connectivity=4,
        centroid_method="geometric",
    )
    policy = _policy(detector=detector)
    state = create_cell_track_engine_state("shadow-engine-test", policy)
    previous, current = _quadratic_frame_pair()
    first = advance_cell_track_engine(state, previous, policy)
    second = advance_cell_track_engine(first.next_state, current, policy)

    assert second.flow_field is not None
    assert second.flow_aggregations[0].available
    assert isinstance(second.motion_priors[0], MotionPrediction)
    assert second.motion_priors[0].method == "lucas_kanade_conservative_mean_v1"


def test_replay_is_deterministic_input_is_immutable_and_hypotheses_are_canonical():
    state, policy = _state()
    first = advance_cell_track_engine(
        state,
        _frame("deterministic-1", 9_000.0, ((4, 3, 2.0),)),
        policy,
    )
    frame = _frame("deterministic-2", 9_060.0, ((4, 4, 2.0),))
    unselected_b = CellTrackLineageHypothesis(
        hypothesis_id="alternative-b",
        event_type="split",
        parent_track_ids=(_active(first.next_state)[0].track_id,),
        children=(
            LineageChildIdentity(
                advance_cell_track_engine(first.next_state, frame, policy)
                .detection_frame.detections[0].detection.detection_id,
                "hypothetical-a",
            ),
            LineageChildIdentity("hypothetical-detection", "hypothetical-b"),
        ),
        cost=3.0,
        cost_model_id="alternatives-v1",
        cost_components=(("caller", 3.0),),
        provenance_ids=("proof",),
        selected=False,
    )
    # The alternative above deliberately references an absent detection and
    # must fail closed rather than disappear.
    with pytest.raises(ValueError, match="onbekende detectie"):
        advance_cell_track_engine(
            first.next_state,
            frame,
            policy,
            lineage_hypotheses=(unselected_b,),
        )

    first_replay = advance_cell_track_engine(first.next_state, frame, policy)
    second_replay = advance_cell_track_engine(first.next_state, frame, policy)
    assert first_replay == second_replay
    intensity_variant = advance_cell_track_engine(
        first.next_state,
        _frame("deterministic-2", 9_060.0, ((4, 4, 3.0),)),
        policy,
    )
    assert intensity_variant.detection_frame.detections[0].detection.detection_id == (
        first_replay.detection_frame.detections[0].detection.detection_id
    )
    assert intensity_variant.transition_id != first_replay.transition_id
    assert first.next_state.track(_active(first.next_state)[0].track_id).sample_count == 1
    with pytest.raises(FrozenInstanceError):
        first_replay.next_state.revision = 99  # type: ignore[misc]


def test_valid_lineage_alternative_input_order_cannot_change_replay():
    state, policy = _state()
    first = advance_cell_track_engine(
        state,
        _frame("order-1", 9_500.0, ((4, 4, 2.0),)),
        policy,
    )
    parent = _active(first.next_state)[0].track_id
    current = _frame(
        "order-2",
        9_560.0,
        ((4, 3, 2.0), (4, 5, 2.0)),
    )
    preview = advance_cell_track_engine(first.next_state, current, policy)
    detections = tuple(
        item.detection.detection_id for item in preview.detection_frame.detections
    )

    def alternative(identifier: str):
        return CellTrackLineageHypothesis(
            hypothesis_id=identifier,
            event_type="split",
            parent_track_ids=(parent,),
            children=tuple(
                sorted(
                    (
                        LineageChildIdentity(
                            detections[0],
                            f"{identifier}-child-a",
                        ),
                        LineageChildIdentity(
                            detections[1],
                            f"{identifier}-child-b",
                        ),
                    ),
                    key=lambda item: item.detection_id,
                )
            ),
            cost=4.0,
            cost_model_id="ordered-alternatives-v1",
            cost_components=(("caller", 4.0),),
            provenance_ids=("ordered-proof",),
            selected=False,
        )

    alpha = alternative("alternative-alpha")
    zulu = alternative("alternative-zulu")
    forward = advance_cell_track_engine(
        first.next_state,
        current,
        policy,
        lineage_hypotheses=(alpha, zulu),
    )
    reverse = advance_cell_track_engine(
        first.next_state,
        current,
        policy,
        lineage_hypotheses=(zulu, alpha),
    )

    assert forward == reverse
    assert tuple(item.hypothesis_id for item in forward.lineage_hypotheses) == (
        "alternative-alpha",
        "alternative-zulu",
    )


def test_provider_change_is_explicit_indeterminate_not_cross_handoff_velocity():
    state, policy = _state()
    first = advance_cell_track_engine(
        state,
        _frame("provider-1", 10_000.0, ((4, 4, 2.0),), source="provider-a"),
        policy,
    )
    changed = advance_cell_track_engine(
        first.next_state,
        _frame("provider-2", 10_060.0, ((4, 5, 2.0),), source="provider-b"),
        policy,
    )

    assert changed.status == "indeterminate"
    assert changed.next_state is first.next_state
    assert "provider_handoff_evidence_missing" in changed.reason_codes
    assert "provider_grid_handoff_state_transform_unimplemented" in changed.reason_codes
    assert len(changed.detection_frame.detections) == 1


def test_frame_timestamps_are_strict_and_invalid_input_cannot_mutate_state():
    state, policy = _state()
    first = advance_cell_track_engine(
        state,
        _frame("strict-1", 11_000.0, ((4, 4, 2.0),)),
        policy,
    )
    before = first.next_state

    with pytest.raises(ValueError, match="strikt meteorologisch"):
        advance_cell_track_engine(
            before,
            _frame("strict-same-time", 11_000.0, ((4, 5, 2.0),)),
            policy,
        )
    with pytest.raises(ValueError, match="strikt meteorologisch"):
        advance_cell_track_engine(
            before,
            _frame("strict-older", 10_999.0, ((4, 5, 2.0),)),
            policy,
        )
    assert before is first.next_state
    assert before.revision == 1
    assert _active(before)[0].sample_count == 1


def test_candidate_module_is_not_wired_to_service_ha_or_legacy_engine():
    module = (
        ROOT
        / "storm_tracker_worker/tracking_core/engine/cell_track_engine.py"
    ).read_text(encoding="utf-8")
    assert "from .storm_engine" not in module
    assert "from storm_tracker_worker.service" not in module
    for path in (
        ROOT / "storm_tracker_worker/service.py",
        ROOT / "custom_components/storm_tracker_v3/__init__.py",
        ROOT / "storm_tracker_worker/tracking_core/engine/storm_engine.py",
    ):
        assert "advance_cell_track_engine" not in path.read_text(encoding="utf-8")
