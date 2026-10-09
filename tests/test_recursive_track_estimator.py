"""Scientific contracts for the uncapped multi-frame motion challenger."""
from __future__ import annotations

from dataclasses import FrozenInstanceError
import hashlib
from pathlib import Path

import pytest

from storm_tracker_worker.tracking_core.engine.cell_track import CellDetection
from storm_tracker_worker.tracking_core.engine.recursive_track_estimator import (
    RecursiveTrackEstimatorPolicy,
    assimilate_recursive_detection,
    build_recursive_track_estimate,
    initialize_recursive_track_estimate,
    predict_recursive_track,
)


ROOT = Path(__file__).resolve().parent.parent
INITIAL_COVARIANCE = (
    (1_000_000.0, 0.0, 0.0, 0.0),
    (0.0, 1_000_000.0, 0.0, 0.0),
    (0.0, 0.0, 10_000.0, 0.0),
    (0.0, 0.0, 0.0, 10_000.0),
)
POLICY = RecursiveTrackEstimatorPolicy(
    policy_id="explicit-white-acceleration-test-v1",
    acceleration_spectral_density_m2_s3=2.0,
)


def _detection(
    identifier: str,
    valid_at: float,
    x_m: float,
    y_m: float = 0.0,
    *,
    sigma_m: float = 500.0,
    grid: str = "grid",
) -> CellDetection:
    return CellDetection(
        detection_id=identifier,
        valid_at=valid_at,
        analysis_grid_id=grid,
        x_m=x_m,
        y_m=y_m,
        position_sigma_m=sigma_m,
        source="test-radar",
    )


def _initial(first: CellDetection):
    return initialize_recursive_track_estimate(
        "track-a",
        first,
        initial_vx_m_s=0.0,
        initial_vy_m_s=0.0,
        initial_state_covariance=INITIAL_COVARIANCE,
        policy=POLICY,
    )


def test_exact_last_interval_velocity_remains_uncapped():
    first = _detection("one", 1_000.0, 0.0)
    second = _detection("two", 1_060.0, 120_000.0)

    estimate = assimilate_recursive_detection(_initial(first), second, POLICY)

    assert estimate.latest_interval_speed_kmh == pytest.approx(7_200.0)
    assert estimate.revision == 2
    assert estimate.supporting_detection_ids == ("one", "two")
    assert estimate.normalized_innovation_squared > 0.0


def test_every_associated_outlier_is_assimilated_and_audited():
    detections = (
        _detection("one", 1_000.0, 0.0),
        _detection("two", 1_300.0, 3_000.0),
        _detection("extreme", 1_600.0, 1_000_000.0),
    )

    estimate = build_recursive_track_estimate(
        "track-a",
        detections,
        initial_vx_m_s=0.0,
        initial_vy_m_s=0.0,
        initial_state_covariance=INITIAL_COVARIANCE,
        policy=POLICY,
    )

    assert estimate.supporting_detection_ids[-1] == "extreme"
    assert estimate.latest_interval_speed_kmh > 10_000.0
    assert estimate.normalized_innovation_squared > 1.0
    assert estimate.x_m > 3_000.0


def test_regular_multi_frame_motion_converges_without_rewriting_observations():
    detections = tuple(
        _detection(f"d{index}", 1_000.0 + index * 300.0, index * 3_000.0)
        for index in range(6)
    )

    estimate = build_recursive_track_estimate(
        "track-a",
        detections,
        initial_vx_m_s=0.0,
        initial_vy_m_s=0.0,
        initial_state_covariance=INITIAL_COVARIANCE,
        policy=POLICY,
    )

    assert estimate.last_observed_x_m == 15_000.0
    assert estimate.latest_interval_vx_m_s == pytest.approx(10.0)
    assert estimate.vx_m_s == pytest.approx(10.0, abs=1.0)
    assert estimate.revision == 6


def test_gap_prediction_grows_uncertainty_and_does_not_add_fake_detection():
    first = _detection("one", 1_000.0, 0.0)
    second = _detection("two", 1_300.0, 3_000.0)
    estimate = assimilate_recursive_detection(_initial(first), second, POLICY)

    short = predict_recursive_track(estimate, 1_600.0, POLICY)
    long = predict_recursive_track(estimate, 2_500.0, POLICY)

    assert long.covariance[0][0] > short.covariance[0][0]
    assert long.predicted_x_m > short.predicted_x_m
    assert estimate.supporting_detection_ids == ("one", "two")


def test_policy_grid_time_and_duplicate_contracts_fail_closed():
    first = _detection("one", 1_000.0, 0.0)
    estimate = _initial(first)
    other_policy = RecursiveTrackEstimatorPolicy(
        policy_id="other",
        acceleration_spectral_density_m2_s3=2.0,
    )

    with pytest.raises(ValueError, match="beleid"):
        predict_recursive_track(estimate, 1_300.0, other_policy)
    with pytest.raises(ValueError, match="grid"):
        assimilate_recursive_detection(
            estimate,
            _detection("other", 1_300.0, 1.0, grid="other"),
            POLICY,
        )
    with pytest.raises(ValueError, match="later"):
        assimilate_recursive_detection(
            estimate,
            _detection("old", 1_000.0, 1.0),
            POLICY,
        )


def test_state_is_immutable_and_replay_is_deterministic():
    detections = (
        _detection("one", 1_000.0, 0.0),
        _detection("two", 1_300.0, 3_000.0),
    )
    first = build_recursive_track_estimate(
        "track-a",
        detections,
        initial_vx_m_s=0.0,
        initial_vy_m_s=0.0,
        initial_state_covariance=INITIAL_COVARIANCE,
        policy=POLICY,
    )
    second = build_recursive_track_estimate(
        "track-a",
        detections,
        initial_vx_m_s=0.0,
        initial_vy_m_s=0.0,
        initial_state_covariance=INITIAL_COVARIANCE,
        policy=POLICY,
    )

    assert first == second
    with pytest.raises(FrozenInstanceError):
        first.state = (0.0, 0.0, 0.0, 0.0)


def test_component_and_worker_estimator_are_exact_mirrors():
    component = (
        ROOT
        / "custom_components/storm_tracker_v3/engine/"
        "recursive_track_estimator.py"
    ).read_bytes()
    worker = (
        ROOT
        / "storm_tracker_worker/tracking_core/engine/"
        "recursive_track_estimator.py"
    ).read_bytes()

    assert component == worker
    assert hashlib.sha256(component).hexdigest()
