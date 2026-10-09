"""Tests for the isolated metric CellTrack shadow pipeline."""
from __future__ import annotations

from dataclasses import FrozenInstanceError
from pathlib import Path
import random

import pytest

from storm_tracker_worker.tracking_core.engine.analysis_grid import AnalysisGrid
from storm_tracker_worker.tracking_core.engine.association_features import (
    build_association_feature_table,
)
from storm_tracker_worker.tracking_core.engine.cell_track import CellDetection
from storm_tracker_worker.tracking_core.engine.cell_track_lifecycle import (
    CellTrackLedger,
    CellTrackLifecyclePolicy,
    add_track_to_ledger,
    advance_ledger_from_assignment,
    append_cell_detection,
    mark_cell_track_missed,
    record_cell_track_merge,
    record_cell_track_split,
    start_cell_track,
)
from storm_tracker_worker.tracking_core.engine.global_assignment import (
    AssignmentEdgeCost,
    GlobalAssignmentProblem,
    solve_exact_global_assignment,
    solve_global_assignment_one_best,
    solve_global_assignment_with_fallback,
)
from storm_tracker_worker.tracking_core.engine.hierarchical_detection import (
    HierarchicalCellDetection,
    HierarchicalDetectorConfig,
    NormalizedRasterFrame,
    detect_hierarchical_cells,
)
from storm_tracker_worker.tracking_core.engine.motion_prediction import (
    OpticalFlowSample,
    predict_with_optical_flow,
)
from storm_tracker_worker.tracking_core.engine.track_motion_prior import (
    build_cold_start_track_prior,
    build_two_position_track_prior,
)


def _grid(
    *,
    grid_id: str = "belgium-laea-1km-v1",
    width: int = 7,
    height: int = 5,
) -> AnalysisGrid:
    return AnalysisGrid(
        grid_id=grid_id,
        crs="EPSG:3035",
        origin_x_m=4_000_000.0,
        origin_y_m=3_100_000.0,
        x_resolution_m=1_000.0,
        y_resolution_m=1_000.0,
        width=width,
        height=height,
    )


def _cell(
    detection_id: str,
    valid_at: float,
    x_m: float,
    y_m: float,
    *,
    sigma_m: float = 500.0,
    source: str = "test-radar",
    grid_id: str = "metric-grid-v1",
) -> CellDetection:
    return CellDetection(
        detection_id=detection_id,
        valid_at=valid_at,
        analysis_grid_id=grid_id,
        x_m=x_m,
        y_m=y_m,
        position_sigma_m=sigma_m,
        source=source,
    )


def _hierarchical(
    detection_id: str,
    valid_at: float,
    x_m: float,
    y_m: float,
    *,
    kind: str = "rain_shield",
    footprint: tuple[tuple[int, int], ...] = ((0, 0),),
    value: float = 2.0,
    source: str = "test-radar",
    grid_id: str = "metric-grid-v1",
) -> HierarchicalCellDetection:
    detection = _cell(
        detection_id,
        valid_at,
        x_m,
        y_m,
        source=source,
        grid_id=grid_id,
    )
    parent = "parent" if kind == "embedded_core" else None
    return HierarchicalCellDetection(
        detection=detection,
        object_kind=kind,
        parent_detection_id=parent,
        footprint_pixels=footprint,
        area_m2=float(len(footprint) * 1_000_000),
        geometric_centroid_x_m=x_m,
        geometric_centroid_y_m=y_m,
        value_weighted_centroid_x_m=x_m,
        value_weighted_centroid_y_m=y_m,
        mean_value=value,
        maximum_value=value,
        threshold=1.0,
        centroid_method="geometric",
    )


def _flow(
    detection: CellDetection,
    *,
    vx_m_s: float,
    vy_m_s: float,
    velocity_covariance=((4.0, 0.0), (0.0, 9.0)),
    residual_covariance=((2_500.0, 0.0), (0.0, 3_600.0)),
) -> OpticalFlowSample:
    return OpticalFlowSample(
        sample_id=f"flow:{detection.detection_id}",
        analysis_grid_id=detection.analysis_grid_id,
        reference_valid_at=detection.valid_at,
        vx_m_s=vx_m_s,
        vy_m_s=vy_m_s,
        velocity_covariance_m2_s2=velocity_covariance,
        residual_covariance_m2=residual_covariance,
        method="caller-calibrated-optical-flow",
        support_frame_ids=("support-1", "support-2"),
    )


def _edge(old: str, new: str, cost: float) -> AssignmentEdgeCost:
    return AssignmentEdgeCost(
        previous_detection_id=old,
        current_detection_id=new,
        cost=cost,
        cost_model_id="explicit-test-cost-v1",
        components=(("caller_cost", cost),),
    )


def _brute_force_assignment(problem: GlobalAssignmentProblem):
    previous = tuple(sorted(problem.previous_detection_ids))
    current = tuple(sorted(problem.current_detection_ids))
    edges = {
        (item.previous_detection_id, item.current_detection_id): item.cost
        for item in problem.edge_costs
    }
    deaths = dict(problem.death_costs)
    births = dict(problem.birth_costs)
    candidates = []

    def visit(index, used, matches, ended, cost):
        if index == len(previous):
            born = tuple(item for item in current if item not in used)
            total = cost + sum(births[item] for item in born)
            candidates.append((total, tuple(matches), tuple(ended), born))
            return
        old = previous[index]
        visit(
            index + 1,
            used,
            matches,
            ended + [old],
            cost + deaths[old],
        )
        for new in current:
            if new in used:
                continue
            visit(
                index + 1,
                used | {new},
                matches + [(old, new)],
                ended,
                cost + edges[(old, new)],
            )

    visit(0, set(), [], [], 0.0)
    return min(candidates, key=lambda item: (item[0], item[1], item[2], item[3]))


def test_metric_grid_roundtrip_half_open_edges_and_resolution_checks():
    grid = _grid()

    first_x, first_y = grid.pixel_center(0, 0)
    last_x, last_y = grid.pixel_center(4, 6)

    assert (first_x, first_y) == (4_000_500.0, 3_099_500.0)
    assert (last_x, last_y) == (4_006_500.0, 3_095_500.0)
    assert grid.point_to_pixel(first_x, first_y) == (0, 0)
    assert grid.point_to_pixel(last_x, last_y) == (4, 6)
    assert grid.extent_edges_m == (
        4_000_000.0,
        3_095_000.0,
        4_007_000.0,
        3_100_000.0,
    )
    grid.assert_round_trip(tolerance_m=1e-8)
    grid.assert_resolution_at_most(x_m=1_000.0, y_m=1_000.0)
    with pytest.raises(ValueError, match="buiten"):
        grid.point_to_pixel(grid.east_edge_m, first_y)
    with pytest.raises(ValueError, match="grover"):
        grid.assert_resolution_at_most(x_m=999.0, y_m=1_000.0)


def test_grid_rejects_nonmetric_or_shape_ambiguity():
    with pytest.raises(ValueError, match="metrische"):
        AnalysisGrid(
            grid_id="bad",
            crs="EPSG:4326",
            origin_x_m=0.0,
            origin_y_m=0.0,
            x_resolution_m=0.01,
            y_resolution_m=0.01,
            width=2,
            height=2,
            coordinate_unit="degree",
        )

    grid = _grid(width=2, height=2)
    with pytest.raises(ValueError, match="rasterhoogte"):
        grid.validate_raster_shape(((1.0, 2.0),))
    with pytest.raises(ValueError, match="breedte"):
        grid.validate_raster_shape(((1.0,), (2.0, 3.0)))


def test_detector_builds_rain_shield_with_embedded_core_deterministically():
    grid = _grid()
    frame = NormalizedRasterFrame(
        frame_id="frame-001",
        valid_at=1_000.0,
        source="normalized-kmi",
        quantity="rain_rate",
        unit="mm/h",
        grid=grid,
        values=(
            (0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0),
            (0.0, 1.0, 1.0, 0.0, 0.0, 1.0, 0.0),
            (0.0, 1.0, 5.0, 0.0, 0.0, 1.0, 0.0),
            (0.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0),
            (0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0),
        ),
    )
    config = HierarchicalDetectorConfig(
        shield_threshold=1.0,
        core_threshold=4.0,
        minimum_shield_area_m2=3_000_000.0,
        minimum_core_area_m2=1_000_000.0,
        shield_position_sigma_m=800.0,
        core_position_sigma_m=500.0,
        connectivity=4,
        centroid_method="geometric",
    )

    first = detect_hierarchical_cells(frame, config)
    second = detect_hierarchical_cells(frame, config)

    assert first == second
    assert len(first.rain_shields) == 2
    assert len(first.embedded_cores) == 1
    shield = next(item for item in first.rain_shields if item.area_m2 == 6_000_000.0)
    fragment = next(item for item in first.rain_shields if item is not shield)
    core = first.embedded_cores[0]
    assert shield.area_m2 == 6_000_000.0
    assert len(shield.footprint_pixels) == 6
    assert fragment.footprint_pixels == ((1, 5), (2, 5))
    assert fragment.low_confidence_fragment is True
    assert first.low_confidence_fragments == (fragment,)
    assert core.footprint_pixels == ((2, 2),)
    assert core.parent_detection_id == shield.detection.detection_id
    assert shield.detection.position_sigma_m == 800.0
    assert core.detection.position_sigma_m == 500.0
    assert shield.detection.analysis_grid_id == grid.grid_id


def test_detector_requires_all_meteorological_choices_from_caller():
    with pytest.raises(ValueError, match="core_threshold"):
        HierarchicalDetectorConfig(
            shield_threshold=2.0,
            core_threshold=1.0,
            minimum_shield_area_m2=0.0,
            minimum_core_area_m2=0.0,
            shield_position_sigma_m=1.0,
            core_position_sigma_m=1.0,
            connectivity=8,
            centroid_method="geometric",
        )
    with pytest.raises(ValueError, match="connectivity"):
        HierarchicalDetectorConfig(
            shield_threshold=1.0,
            core_threshold=2.0,
            minimum_shield_area_m2=0.0,
            minimum_core_area_m2=0.0,
            shield_position_sigma_m=1.0,
            core_position_sigma_m=1.0,
            connectivity=6,
            centroid_method="geometric",
        )


def test_optical_flow_prediction_propagates_covariance_exactly():
    detection = _cell(
        "old",
        1_000.0,
        20_000.0,
        30_000.0,
        sigma_m=100.0,
    )

    prediction = predict_with_optical_flow(
        detection,
        1_060.0,
        _flow(detection, vx_m_s=10.0, vy_m_s=-5.0),
    )

    assert prediction.predicted_x_m == 20_600.0
    assert prediction.predicted_y_m == 29_700.0
    assert prediction.propagated_flow_covariance_m2 == (
        (14_400.0, 0.0),
        (0.0, 32_400.0),
    )
    assert prediction.predicted_position_covariance_m2 == (
        (26_900.0, 0.0),
        (0.0, 46_000.0),
    )
    assert prediction.interval_speed_kmh == pytest.approx(
        (10.0**2 + 5.0**2) ** 0.5 * 3.6
    )


def test_optical_flow_extreme_vector_is_retained_and_not_capped():
    detection = _cell("old", 1_000.0, 0.0, 0.0)
    prediction = predict_with_optical_flow(
        detection,
        1_060.0,
        _flow(detection, vx_m_s=10_000.0, vy_m_s=0.0),
    )

    assert prediction.predicted_x_m == 600_000.0
    assert prediction.interval_speed_kmh == 36_000.0


def test_invalid_optical_flow_covariance_fails_instead_of_fake_confidence():
    detection = _cell("old", 1_000.0, 0.0, 0.0)
    with pytest.raises(ValueError, match="positief semidefiniet"):
        _flow(
            detection,
            vx_m_s=1.0,
            vy_m_s=1.0,
            velocity_covariance=((1.0, 2.0), (2.0, 1.0)),
        )


def test_association_features_keep_complete_cross_product_and_extremes():
    old_a = _hierarchical(
        "old-a",
        1_000.0,
        0.0,
        0.0,
        footprint=((0, 0), (0, 1)),
    )
    old_b = _hierarchical(
        "old-b",
        1_000.0,
        20_000.0,
        0.0,
        footprint=((0, 3),),
    )
    new_near = _hierarchical(
        "new-near",
        1_060.0,
        600.0,
        0.0,
        footprint=((0, 1), (0, 2)),
        source="handoff-radar",
    )
    new_extreme = _hierarchical(
        "new-extreme",
        1_060.0,
        200_000.0,
        0.0,
        footprint=((4, 6),),
    )
    predictions = tuple(
        predict_with_optical_flow(
            item.detection,
            1_060.0,
            _flow(item.detection, vx_m_s=10.0, vy_m_s=0.0),
        )
        for item in (old_a, old_b)
    )

    table = build_association_feature_table(
        (old_b, old_a),
        (new_extreme, new_near),
        predictions,
    )

    assert table.is_complete is True
    assert table.expected_feature_count == 4
    near = next(
        item
        for item in table.features
        if item.previous_detection_id == "old-a"
        and item.current_detection_id == "new-near"
    )
    extreme = next(
        item
        for item in table.features
        if item.previous_detection_id == "old-a"
        and item.current_detection_id == "new-extreme"
    )
    assert near.residual_x_m == 0.0
    assert near.footprint_iou_unadvected == pytest.approx(1.0 / 3.0)
    assert near.source_changed is True
    assert extreme.observed_interval_speed_kmh > 10_000.0
    assert extreme.mahalanobis_squared > near.mahalanobis_squared


def test_association_features_fail_if_prediction_coverage_is_incomplete():
    old = _hierarchical("old", 1_000.0, 0.0, 0.0)
    new = _hierarchical("new", 1_060.0, 100.0, 0.0)

    with pytest.raises(ValueError, match="exact alle"):
        build_association_feature_table((old,), (new,), ())


def test_global_assignment_finds_joint_optimum_and_returns_alternatives():
    problem = GlobalAssignmentProblem(
        previous_detection_ids=("a", "b"),
        current_detection_ids=("x", "y"),
        edge_costs=(
            _edge("a", "x", 1.0),
            _edge("a", "y", 1_000.0),
            _edge("b", "x", 100.0),
            _edge("b", "y", 1.0),
        ),
        death_costs=(("a", 5.0), ("b", 5.0)),
        birth_costs=(("x", 5.0), ("y", 5.0)),
        policy_id="caller-calibrated-costs-v1",
    )

    result = solve_exact_global_assignment(
        problem,
        solution_limit=4,
        maximum_current_detections=8,
    )

    assert result.exact is True
    assert result.best.matches == (("a", "x"), ("b", "y"))
    assert result.best.total_cost == 2.0
    assert len(result.alternatives) == 3
    assert result.problem.edge_costs == problem.edge_costs
    assert any(item.current_detection_id == "y" and item.cost == 1_000.0 for item in result.problem.edge_costs)


def test_global_assignment_models_birth_and_death_without_candidate_deletion():
    problem = GlobalAssignmentProblem(
        previous_detection_ids=("old",),
        current_detection_ids=("near", "newborn"),
        edge_costs=(
            _edge("old", "near", 1.0),
            _edge("old", "newborn", 100.0),
        ),
        death_costs=(("old", 2.0),),
        birth_costs=(("near", 3.0), ("newborn", 3.0)),
        policy_id="birth-death-test-v1",
    )

    result = solve_exact_global_assignment(
        problem,
        solution_limit=3,
        maximum_current_detections=2,
    )

    assert result.best.matches == (("old", "near"),)
    assert result.best.births == ("newborn",)
    assert result.best.deaths == ()
    assert result.best.total_cost == 4.0
    assert any(
        solution.deaths == ("old",)
        and solution.births == ("near", "newborn")
        for solution in result.solutions
    )
    with pytest.raises(ValueError, match="capaciteit"):
        solve_exact_global_assignment(
            problem,
            solution_limit=1,
            maximum_current_detections=1,
        )


def test_assignment_problem_refuses_incomplete_edge_table():
    with pytest.raises(ValueError, match="volledige"):
        GlobalAssignmentProblem(
            previous_detection_ids=("a",),
            current_detection_ids=("x", "y"),
            edge_costs=(_edge("a", "x", 1.0),),
            death_costs=(("a", 2.0),),
            birth_costs=(("x", 2.0), ("y", 2.0)),
            policy_id="incomplete",
        )


def test_scalable_one_best_matches_independent_bruteforce():
    costs = (
        ("a", "x", 8.0),
        ("a", "y", -2.0),
        ("a", "z", 7.0),
        ("b", "x", 1.0),
        ("b", "y", 6.0),
        ("b", "z", 3.0),
        ("c", "x", 5.0),
        ("c", "y", 4.0),
        ("c", "z", 0.5),
    )
    problem = GlobalAssignmentProblem(
        previous_detection_ids=("c", "a", "b"),
        current_detection_ids=("z", "x", "y"),
        edge_costs=tuple(_edge(old, new, cost) for old, new, cost in costs),
        death_costs=(("a", 2.5), ("b", 2.5), ("c", 2.5)),
        birth_costs=(("x", 2.0), ("y", 2.0), ("z", 2.0)),
        policy_id="bruteforce-parity",
    )

    scalable = solve_global_assignment_one_best(problem).best
    brute = _brute_force_assignment(problem)

    assert scalable.total_cost == pytest.approx(brute[0])
    assert scalable.matches == brute[1]
    assert scalable.deaths == brute[2]
    assert scalable.births == brute[3]


def test_scalable_one_best_bruteforce_parity_matrix_sweep():
    generator = random.Random(20260730)
    for previous_count in range(5):
        for current_count in range(5):
            previous = tuple(f"p{index}" for index in range(previous_count))
            current = tuple(f"c{index}" for index in range(current_count))
            problem = GlobalAssignmentProblem(
                previous_detection_ids=previous,
                current_detection_ids=current,
                edge_costs=tuple(
                    _edge(old, new, generator.uniform(-3.0, 9.0))
                    for old in previous
                    for new in current
                ),
                death_costs=tuple(
                    (item, generator.uniform(0.0, 6.0)) for item in previous
                ),
                birth_costs=tuple(
                    (item, generator.uniform(0.0, 6.0)) for item in current
                ),
                policy_id=f"sweep-{previous_count}-{current_count}",
            )

            scalable = solve_global_assignment_one_best(problem).best
            brute = _brute_force_assignment(problem)

            assert scalable.total_cost == pytest.approx(brute[0])
            assert scalable.matches == brute[1]
            assert scalable.deaths == brute[2]
            assert scalable.births == brute[3]


def test_scalable_fallback_keeps_all_candidates_above_top_k_capacity():
    count = 32
    previous = tuple(f"old-{index:02d}" for index in range(count))
    current = tuple(f"new-{index:02d}" for index in range(count))
    edges = tuple(
        _edge(
            old,
            new,
            0.0 if old_index == new_index else 100.0 + abs(old_index - new_index),
        )
        for old_index, old in enumerate(previous)
        for new_index, new in enumerate(current)
    )
    problem = GlobalAssignmentProblem(
        previous_detection_ids=previous,
        current_detection_ids=current,
        edge_costs=edges,
        death_costs=tuple((item, 1_000.0) for item in previous),
        birth_costs=tuple((item, 1_000.0) for item in current),
        policy_id="large-frame-exact-one-best",
    )

    result = solve_global_assignment_with_fallback(
        problem,
        solution_limit=4,
        maximum_top_k_current_detections=16,
    )

    assert result.exact is True
    assert result.solver == "exact_hungarian_one_best"
    assert len(result.problem.edge_costs) == count * count
    assert len(result.best.matches) == count
    assert result.best.births == ()
    assert result.best.deaths == ()
    assert result.best.total_cost == 0.0
    assert result.best.matches == tuple(zip(previous, current))


def test_scalable_one_best_is_deterministic_with_many_equal_costs():
    count = 24
    previous = tuple(f"tie-old-{index:02d}" for index in range(count))
    current = tuple(f"tie-new-{index:02d}" for index in range(count))
    problem = GlobalAssignmentProblem(
        previous_detection_ids=tuple(reversed(previous)),
        current_detection_ids=tuple(reversed(current)),
        edge_costs=tuple(
            _edge(old, new, float((old_index + new_index) % 2))
            for old_index, old in enumerate(previous)
            for new_index, new in enumerate(current)
        ),
        death_costs=tuple((item, 6.0) for item in previous),
        birth_costs=tuple((item, 6.0) for item in current),
        policy_id="equal-cost-hungarian-regression",
    )

    first = solve_global_assignment_one_best(problem)
    second = solve_global_assignment_one_best(problem)

    assert first.solver == "exact_hungarian_one_best"
    assert first.best == second.best
    assert len(first.best.matches) == count
    assert first.best.deaths == ()
    assert first.best.births == ()
    assert first.best.total_cost == 0.0


def test_immutable_track_confirms_coasts_and_ends_by_explicit_policy():
    policy = CellTrackLifecyclePolicy(
        policy_id="two-hits-one-gap",
        confirmation_observations=2,
        maximum_gap_frames=1,
    )
    first = _cell("d1", 1_000.0, 0.0, 0.0)
    second = _cell("d2", 1_060.0, 1_000.0, 0.0)

    tentative = start_cell_track("track-1", first, "rain_shield", policy)
    confirmed = append_cell_detection(
        tentative,
        second,
        "rain_shield",
        policy,
    )
    coasting = mark_cell_track_missed(confirmed, policy)
    ended = mark_cell_track_missed(coasting, policy)

    assert tentative.lifecycle_state == "tentative"
    assert confirmed.lifecycle_state == "confirmed"
    assert coasting.lifecycle_state == "coasting"
    assert ended.lifecycle_state == "ended"
    assert ended.termination_reason == "gap_limit_exceeded"
    assert tentative.sample_count == 1
    assert ended.sample_count == 2
    with pytest.raises(FrozenInstanceError):
        ended.missed_frames = 0


def test_last_two_positions_form_exact_uncapped_track_motion_prior():
    older = _cell("older", 1_000.0, 0.0, 0.0, sigma_m=100.0)
    latest = _cell("latest", 1_100.0, 1_000.0, 0.0, sigma_m=200.0)
    process_noise = (
        (1_000.0, 0.0, 0.0, 0.0),
        (0.0, 1_000.0, 0.0, 0.0),
        (0.0, 0.0, 1.0, 0.0),
        (0.0, 0.0, 0.0, 1.0),
    )

    prior = build_two_position_track_prior(
        "track-prior",
        older,
        latest,
        target_valid_at=1_200.0,
        process_noise_covariance=process_noise,
    )

    assert prior.method == "last_two_associated_positions"
    assert prior.vx_m_s == 10.0
    assert prior.vy_m_s == 0.0
    assert prior.predicted_x_m == 2_000.0
    assert prior.source_state_covariance[0][0] == 40_000.0
    assert prior.source_state_covariance[0][2] == 400.0
    assert prior.source_state_covariance[2][2] == 5.0
    assert prior.predicted_state_covariance[0][0] == 171_000.0
    assert prior.predicted_state_covariance[2][2] == 6.0

    extreme = build_two_position_track_prior(
        "extreme-prior",
        _cell("extreme-old", 2_000.0, 0.0, 0.0),
        _cell("extreme-new", 2_060.0, 200_000.0, 0.0),
        target_valid_at=2_120.0,
        process_noise_covariance=(
            (0.0, 0.0, 0.0, 0.0),
            (0.0, 0.0, 0.0, 0.0),
            (0.0, 0.0, 0.0, 0.0),
            (0.0, 0.0, 0.0, 0.0),
        ),
    )
    assert extreme.interval_speed_kmh == 12_000.0
    assert extreme.predicted_x_m == 400_000.0


def test_cold_start_prior_requires_explicit_broad_covariance_and_method():
    detection = _cell("cold", 1_000.0, 0.0, 0.0)
    initial = (
        (1_000_000.0, 0.0, 0.0, 0.0),
        (0.0, 1_000_000.0, 0.0, 0.0),
        (0.0, 0.0, 400.0, 0.0),
        (0.0, 0.0, 0.0, 400.0),
    )
    zero = (
        (0.0, 0.0, 0.0, 0.0),
        (0.0, 0.0, 0.0, 0.0),
        (0.0, 0.0, 0.0, 0.0),
        (0.0, 0.0, 0.0, 0.0),
    )

    flow_prior = build_cold_start_track_prior(
        "flow-cold-start",
        detection,
        target_valid_at=1_060.0,
        vx_m_s=20.0,
        vy_m_s=-5.0,
        caller_initial_state_covariance=initial,
        process_noise_covariance=zero,
        method="cold_start_optical_flow",
    )

    assert flow_prior.predicted_x_m == 1_200.0
    assert flow_prior.predicted_y_m == -300.0
    assert flow_prior.predicted_position_covariance_m2[0][0] == 2_440_000.0
    with pytest.raises(ValueError, match="nulvelocity"):
        build_cold_start_track_prior(
            "invalid-zero",
            detection,
            target_valid_at=1_060.0,
            vx_m_s=1.0,
            vy_m_s=0.0,
            caller_initial_state_covariance=initial,
            process_noise_covariance=zero,
            method="cold_start_zero",
        )


def test_association_features_accept_last_two_position_track_prior():
    older = _cell("older", 900.0, -1_000.0, 0.0)
    old = _hierarchical("old", 1_000.0, 0.0, 0.0)
    new = _hierarchical("new", 1_100.0, 1_000.0, 0.0)
    zero = (
        (0.0, 0.0, 0.0, 0.0),
        (0.0, 0.0, 0.0, 0.0),
        (0.0, 0.0, 0.0, 0.0),
        (0.0, 0.0, 0.0, 0.0),
    )
    prior = build_two_position_track_prior(
        "last-two",
        older,
        old.detection,
        target_valid_at=new.detection.valid_at,
        process_noise_covariance=zero,
    )

    table = build_association_feature_table((old,), (new,), (prior,))

    assert table.features[0].residual_distance_m == 0.0
    assert table.features[0].flow_method == "last_two_associated_positions"
    assert table.features[0].flow_sample_id == "last-two"


def test_assignment_advances_matches_births_and_deaths_atomically():
    policy = CellTrackLifecyclePolicy(
        policy_id="assignment-policy",
        confirmation_observations=2,
        maximum_gap_frames=2,
    )
    old = _hierarchical("old", 1_000.0, 0.0, 0.0)
    ledger = add_track_to_ledger(
        CellTrackLedger(),
        start_cell_track(
            "existing-track",
            old.detection,
            old.object_kind,
            policy,
        ),
    )
    near = _hierarchical("near", 1_060.0, 600.0, 0.0)
    newborn = _hierarchical("newborn", 1_060.0, 50_000.0, 0.0)
    problem = GlobalAssignmentProblem(
        previous_detection_ids=("old",),
        current_detection_ids=("near", "newborn"),
        edge_costs=(
            _edge("old", "near", 1.0),
            _edge("old", "newborn", 100.0),
        ),
        death_costs=(("old", 2.0),),
        birth_costs=(("near", 3.0), ("newborn", 3.0)),
        policy_id="assignment-policy",
    )
    solution = solve_exact_global_assignment(
        problem,
        solution_limit=1,
        maximum_current_detections=4,
    ).best

    advanced = advance_ledger_from_assignment(
        ledger,
        solution,
        {"near": near, "newborn": newborn},
        {"newborn": "birth-track"},
        policy,
    )

    assert ledger.track("existing-track").sample_count == 1
    assert advanced.track("existing-track").sample_count == 2
    assert advanced.track("existing-track").lifecycle_state == "confirmed"
    assert advanced.track("birth-track").latest_detection.detection_id == "newborn"


def test_split_and_merge_preserve_ended_parents_and_lineage():
    policy = CellTrackLifecyclePolicy(
        policy_id="lineage-policy",
        confirmation_observations=1,
        maximum_gap_frames=2,
    )
    parent_detection = _hierarchical("parent-d", 1_000.0, 0.0, 0.0)
    parent = start_cell_track(
        "parent-track",
        parent_detection.detection,
        parent_detection.object_kind,
        policy,
    )
    ledger = add_track_to_ledger(CellTrackLedger(), parent)
    child_a = _hierarchical("child-a-d", 1_060.0, -500.0, 0.0)
    child_b = _hierarchical("child-b-d", 1_060.0, 500.0, 0.0)

    split = record_cell_track_split(
        ledger,
        event_id="split-1",
        parent_track_id="parent-track",
        children=(("child-a", child_a), ("child-b", child_b)),
        policy=policy,
    )

    assert split.track("parent-track").lifecycle_state == "ended"
    assert split.track("parent-track").termination_reason == "split"
    assert split.track("child-a").parent_track_ids == ("parent-track",)
    assert split.track("child-b").parent_track_ids == ("parent-track",)
    assert split.lineage_events[0].event_type == "split"

    merged_detection = _hierarchical(
        "merged-d",
        1_120.0,
        0.0,
        0.0,
    )
    merged = record_cell_track_merge(
        split,
        event_id="merge-1",
        parent_track_ids=("child-a", "child-b"),
        child_track_id="merged-track",
        child=merged_detection,
        policy=policy,
    )

    assert merged.track("child-a").termination_reason == "merge"
    assert merged.track("child-b").termination_reason == "merge"
    assert merged.track("merged-track").parent_track_ids == (
        "child-a",
        "child-b",
    )
    assert tuple(item.event_type for item in merged.lineage_events) == (
        "split",
        "merge",
    )


def test_complete_shadow_vertical_slice_from_raster_to_track_revision():
    grid = AnalysisGrid(
        grid_id="vertical-grid",
        crs="EPSG:3035",
        origin_x_m=0.0,
        origin_y_m=3_000.0,
        x_resolution_m=1_000.0,
        y_resolution_m=1_000.0,
        width=3,
        height=3,
    )
    config = HierarchicalDetectorConfig(
        shield_threshold=1.0,
        core_threshold=5.0,
        minimum_shield_area_m2=1_000_000.0,
        minimum_core_area_m2=1_000_000.0,
        shield_position_sigma_m=500.0,
        core_position_sigma_m=300.0,
        connectivity=4,
        centroid_method="geometric",
    )
    old_frame = detect_hierarchical_cells(
        NormalizedRasterFrame(
            frame_id="vertical-old",
            valid_at=1_000.0,
            source="calibration-replay",
            quantity="rain_rate",
            unit="mm/h",
            grid=grid,
            values=(
                (0.0, 0.0, 0.0),
                (2.0, 0.0, 0.0),
                (0.0, 0.0, 0.0),
            ),
        ),
        config,
    )
    new_frame = detect_hierarchical_cells(
        NormalizedRasterFrame(
            frame_id="vertical-new",
            valid_at=1_100.0,
            source="calibration-replay",
            quantity="rain_rate",
            unit="mm/h",
            grid=grid,
            values=(
                (0.0, 0.0, 0.0),
                (0.0, 2.0, 0.0),
                (0.0, 0.0, 0.0),
            ),
        ),
        config,
    )
    old = old_frame.rain_shields[0]
    new = new_frame.rain_shields[0]
    prediction = predict_with_optical_flow(
        old.detection,
        new.detection.valid_at,
        _flow(old.detection, vx_m_s=10.0, vy_m_s=0.0),
    )
    features = build_association_feature_table(
        (old,),
        (new,),
        (prediction,),
    )
    feature = features.features[0]
    edge = AssignmentEdgeCost(
        previous_detection_id=feature.previous_detection_id,
        current_detection_id=feature.current_detection_id,
        cost=feature.gaussian_motion_nll,
        cost_model_id="vertical-explicit-motion-nll-v1",
        components=(("motion_nll", feature.gaussian_motion_nll),),
    )
    problem = GlobalAssignmentProblem(
        previous_detection_ids=features.previous_detection_ids,
        current_detection_ids=features.current_detection_ids,
        edge_costs=(edge,),
        death_costs=((old.detection.detection_id, 100.0),),
        birth_costs=((new.detection.detection_id, 100.0),),
        policy_id="vertical-caller-costs-v1",
    )
    assignment = solve_exact_global_assignment(
        problem,
        solution_limit=2,
        maximum_current_detections=4,
    ).best
    policy = CellTrackLifecyclePolicy(
        policy_id="vertical-lifecycle-v1",
        confirmation_observations=2,
        maximum_gap_frames=1,
    )
    ledger = add_track_to_ledger(
        CellTrackLedger(),
        start_cell_track(
            "vertical-track",
            old.detection,
            old.object_kind,
            policy,
        ),
    )
    advanced = advance_ledger_from_assignment(
        ledger,
        assignment,
        {new.detection.detection_id: new},
        {},
        policy,
    )

    assert feature.residual_distance_m == 0.0
    assert assignment.matches == (
        (old.detection.detection_id, new.detection.detection_id),
    )
    assert advanced.track("vertical-track").sample_count == 2
    assert advanced.track("vertical-track").lifecycle_state == "confirmed"
    assert ledger.track("vertical-track").sample_count == 1


def test_shadow_pipeline_modules_are_exact_mirrors_and_not_live_wired():
    repository = Path(__file__).resolve().parents[1]
    module_names = (
        "analysis_grid.py",
        "hierarchical_detection.py",
        "motion_prediction.py",
        "association_features.py",
        "global_assignment.py",
        "cell_track_lifecycle.py",
        "track_motion_prior.py",
    )
    for name in module_names:
        integration = (
            repository
            / "custom_components"
            / "storm_tracker_v3"
            / "engine"
            / name
        )
        worker = (
            repository
            / "storm_tracker_worker"
            / "tracking_core"
            / "engine"
            / name
        )
        assert integration.read_bytes() == worker.read_bytes()

    integration_runtime = (
        repository / "custom_components/storm_tracker_v3/__init__.py"
    ).read_text(encoding="utf-8")
    worker_service = (
        repository / "storm_tracker_worker/service.py"
    ).read_text(encoding="utf-8")
    for symbol in (
        "detect_hierarchical_cells",
        "predict_with_optical_flow",
        "solve_exact_global_assignment",
        "advance_ledger_from_assignment",
    ):
        assert symbol not in integration_runtime
        assert symbol not in worker_service
