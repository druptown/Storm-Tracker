"""Tests for the isolated CellTrack-first two-detection shadow primitive."""
from __future__ import annotations

from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

from storm_tracker_worker.tracking_core.engine.cell_track import (
    CellDetection,
    estimate_preliminary_cell_track,
)


def _detection(
    detection_id: str,
    valid_at: float,
    x_m: float,
    y_m: float,
    *,
    sigma_m: float = 2_000.0,
    grid_id: str = "analysis-grid-v1",
) -> CellDetection:
    return CellDetection(
        detection_id=detection_id,
        valid_at=valid_at,
        analysis_grid_id=grid_id,
        x_m=x_m,
        y_m=y_m,
        position_sigma_m=sigma_m,
        source="test-radar",
    )


def test_two_detections_publish_exact_interval_velocity_and_covariance():
    first = _detection("frame-1:cell-4", 1_000.0, 10_000.0, 20_000.0)
    second = _detection("frame-2:cell-7", 1_600.0, 17_000.0, 20_000.0)

    track = estimate_preliminary_cell_track("track-1", first, second)

    assert track.lifecycle_state == "preliminary"
    assert track.sample_count == 2
    assert track.interval_seconds == 600.0
    assert track.analysis_grid_id == "analysis-grid-v1"
    assert track.x_m == 17_000.0
    assert track.y_m == 20_000.0
    assert track.vx_m_s == pytest.approx(7_000.0 / 600.0)
    assert track.vy_m_s == pytest.approx(0.0, abs=1e-12)
    assert track.speed_kmh == pytest.approx(track.vx_m_s * 3.6)
    assert track.heading_deg == pytest.approx(90.0)
    assert len(track.state_covariance) == 4
    assert all(len(row) == 4 for row in track.state_covariance)
    assert track.state_covariance[2][2] > 0.0


def test_extreme_interval_speed_is_measured_not_capped_or_rejected():
    first = _detection("first", 1_000.0, 0.0, 0.0, sigma_m=5_000.0)
    second = _detection(
        "second", 1_060.0, 111_000.0, 0.0, sigma_m=5_000.0
    )

    track = estimate_preliminary_cell_track("extreme", first, second)

    assert track.speed_kmh > 6_000.0
    assert track.lifecycle_state == "preliminary"


def test_shorter_interval_has_larger_velocity_uncertainty():
    first = _detection("first", 1_000.0, 0.0, 0.0, sigma_m=1_000.0)
    short = _detection("short", 1_060.0, 1_000.0, 0.0, sigma_m=1_000.0)
    long = _detection("long", 1_600.0, 1_000.0, 0.0, sigma_m=1_000.0)

    short_track = estimate_preliminary_cell_track("short", first, short)
    long_track = estimate_preliminary_cell_track("long", first, long)

    assert (
        short_track.state_covariance[2][2]
        > long_track.state_covariance[2][2] * 90.0
    )


def test_covariance_matches_independent_two_measurement_model():
    first = _detection("first", 1_000.0, 0.0, 0.0, sigma_m=1_000.0)
    second = _detection("second", 1_100.0, 100.0, 0.0, sigma_m=2_000.0)

    track = estimate_preliminary_cell_track("track", first, second)
    covariance = track.state_covariance

    assert covariance[0][0] == 4_000_000.0
    assert covariance[0][2] == 40_000.0
    assert covariance[2][0] == 40_000.0
    assert covariance[2][2] == 500.0
    assert covariance[1][3] == covariance[3][1] == 40_000.0


def test_detections_on_different_analysis_grids_are_rejected():
    first = _detection("first", 1_000.0, 0.0, 0.0, grid_id="grid-a")
    second = _detection("second", 1_600.0, 1_000.0, 0.0, grid_id="grid-b")

    with pytest.raises(ValueError, match="analysis grid"):
        estimate_preliminary_cell_track("track", first, second)


@pytest.mark.parametrize("second_time", [1_000.0, 999.0])
def test_non_increasing_detection_time_is_rejected(second_time):
    first = _detection("first", 1_000.0, 0.0, 0.0)
    second = _detection("second", second_time, 1_000.0, 0.0)

    with pytest.raises(ValueError, match="later"):
        estimate_preliminary_cell_track("track", first, second)


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("detection_id", ""),
        ("valid_at", float("nan")),
        ("analysis_grid_id", ""),
        ("x_m", float("inf")),
        ("y_m", float("nan")),
        ("position_sigma_m", 0.0),
        ("source", ""),
    ),
)
def test_detection_contract_rejects_invalid_values(field, value):
    values = {
        "detection_id": "valid",
        "valid_at": 1_000.0,
        "analysis_grid_id": "grid",
        "x_m": 1_000.0,
        "y_m": 2_000.0,
        "position_sigma_m": 1_000.0,
        "source": "radar",
    }
    values[field] = value

    with pytest.raises(ValueError):
        CellDetection(**values)


def test_detection_and_snapshot_are_immutable():
    first = _detection("first", 1_000.0, 0.0, 0.0)
    second = _detection("second", 1_600.0, 1_000.0, 0.0)
    track = estimate_preliminary_cell_track("track", first, second)

    with pytest.raises(FrozenInstanceError):
        first.x_m = 5.0
    with pytest.raises(FrozenInstanceError):
        track.vx_m_s = 0.0


def test_celltrack_primitives_do_not_drive_live_runtime_yet():
    root = Path(__file__).resolve().parents[1]
    integration_runtime = (
        root / "custom_components/storm_tracker_v3/__init__.py"
    ).read_text(encoding="utf-8")
    worker_service = (
        root / "storm_tracker_worker/service.py"
    ).read_text(encoding="utf-8")

    assert "estimate_preliminary_cell_track" not in integration_runtime
    assert "build_detection_association_graph" not in integration_runtime
    assert "estimate_preliminary_cell_track" not in worker_service
    assert "build_detection_association_graph" not in worker_service
