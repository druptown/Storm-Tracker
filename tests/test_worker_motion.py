"""Parity and bounds for the external motion preflight."""
from storm_tracker_worker.motion import Frame, deterministic_nowcast, evaluate


def _frame(radar_frame_module, minute: int, column_shift: int):
    return radar_frame_module.RadarFrame(
        source="opera",
        nominal_timestamp=1_700_000_000 + minute * 60,
        bounds=(3.0, 50.0, 6.0, 53.0),
        grid_deg=0.02,
        height=40,
        width=40,
        cells=tuple(
            (row, column + column_shift, 4)
            for row in range(10, 14)
            for column in range(10, 14)
        ),
        input_runs=4,
    )


def test_worker_motion_matches_integration(
    radar_frame_module, pysteps_preflight_module
):
    local_frames = tuple(
        _frame(radar_frame_module, minute, minute)
        for minute in range(3)
    )
    worker_frames = tuple(
        Frame.from_dict(frame.to_dict()) for frame in local_frames
    )
    assert evaluate(worker_frames) == pysteps_preflight_module.evaluate_frames(
        local_frames
    )


def test_worker_motion_rejects_oversized_grid():
    value = {
        "source": "opera",
        "nominal_timestamp": 1_700_000_000,
        "bounds": [3.0, 50.0, 6.0, 53.0],
        "grid_deg": 0.02,
        "height": 801,
        "width": 800,
        "cells": [],
    }
    try:
        Frame.from_dict(value)
    except ValueError as err:
        assert "rastervorm" in str(err)
    else:
        raise AssertionError("oversized raster werd niet geweigerd")


def test_worker_builds_bounded_sixty_minute_nowcast(radar_frame_module):
    frames = tuple(
        Frame.from_dict(_frame(radar_frame_module, minute, minute).to_dict())
        for minute in range(3)
    )
    motion = evaluate(frames)
    result = deterministic_nowcast(frames, motion, [{
        "target_id": "test",
        "latitude": 52.39,
        "longitude": 3.33,
    }])
    assert result["status"] == "ready"
    assert result["horizon_minutes"] == 60
    assert result["step_minutes"] == 5
    assert len(result["steps"]) == 13
    assert result["steps"][0]["lead_minutes"] == 0
    assert result["steps"][-1]["lead_minutes"] == 60


def test_worker_selects_acceleration_only_after_better_hindcast(
    radar_frame_module,
):
    frames = tuple(
        Frame.from_dict(_frame(radar_frame_module, minute, shift).to_dict())
        for minute, shift in ((0, 0), (10, 1), (20, 3), (30, 6))
    )
    result = deterministic_nowcast(frames, evaluate(frames))

    assert result["status"] == "ready"
    assert result["motion_model"] == "constant_acceleration"
    assert result["hindcast_adaptive_iou"] == 1.0
    assert result["hindcast_adaptive_iou"] > result["hindcast_linear_iou"]
    assert result["model_gain"] >= 0.08
    assert 0 < result["acceleration_kmh2"] <= 120


def test_worker_keeps_linear_model_without_four_valid_frames(
    radar_frame_module,
):
    frames = tuple(
        Frame.from_dict(_frame(radar_frame_module, minute, shift).to_dict())
        for minute, shift in ((0, 0), (10, 1), (20, 2))
    )
    result = deterministic_nowcast(frames, evaluate(frames))

    assert result["status"] == "ready"
    assert result["motion_model"] == "linear"
    assert result["acceleration_kmh2"] == 0.0
    assert result["hindcast_adaptive_iou"] is None


def test_worker_does_not_reject_high_speed_or_acceleration(
    radar_frame_module,
):
    frames = tuple(
        Frame.from_dict(_frame(radar_frame_module, minute, shift).to_dict())
        for minute, shift in ((0, 0), (1, 1), (2, 4), (3, 9))
    )
    motion = evaluate(frames)
    result = deterministic_nowcast(frames, motion)

    assert motion["status"] == "ready"
    assert motion["translation_km_per_hour"] > 180.0
    assert result["status"] == "ready"
    assert result["motion_model"] == "constant_acceleration"
    assert result["acceleration_kmh2"] > 120.0
    assert result["projected_speed_kmh"] > 180.0
    assert result["model_selection_basis"] == "hindcast_skill_only"


def test_worker_and_component_match_for_motion_above_legacy_limit(
    radar_frame_module, pysteps_preflight_module,
):
    local_frames = tuple(
        radar_frame_module.RadarFrame(
            source="opera",
            nominal_timestamp=1_700_000_000 + index * 60,
            bounds=(4.0, 50.0, 5.8, 50.4),
            grid_deg=0.02,
            height=20,
            width=90,
            cells=tuple(
                (row, column, 10)
                for row in range(5, 9)
                for column in range(5 + index * 30, 9 + index * 30)
            ),
            input_runs=4,
        )
        for index in range(3)
    )
    worker_frames = tuple(
        Frame.from_dict(frame.to_dict()) for frame in local_frames
    )

    worker_result = evaluate(worker_frames)

    assert worker_result == pysteps_preflight_module.evaluate_frames(
        local_frames
    )
    assert worker_result["status"] == "ready"
    assert worker_result["translation_km_per_hour"] > 180.0
    assert worker_result["translation_censored"] is False


def test_worker_and_component_match_for_censored_search_boundary(
    radar_frame_module, pysteps_preflight_module,
):
    common = {
        "source": "opera",
        "bounds": (4.0, 50.0, 6.0, 50.4),
        "grid_deg": 0.02,
        "height": 20,
        "width": 100,
    }
    origin = radar_frame_module.RadarFrame(
        **common,
        nominal_timestamp=1_200,
        cells=tuple(
            (row, column, 10)
            for row in range(5, 9)
            for column in range(10, 14)
        ),
        input_runs=4,
    )
    observed = radar_frame_module.RadarFrame(
        **common,
        nominal_timestamp=1_260,
        cells=(
            *tuple(
                (row, column, 10)
                for row in range(5, 9)
                for column in range(20, 24)
            ),
            (0, 78, 10), (2, 79, 10), (10, 80, 10),
            (12, 81, 10), (14, 82, 10),
        ),
        input_runs=6,
    )
    local_frames = (origin, observed)
    worker_frames = tuple(
        Frame.from_dict(frame.to_dict()) for frame in local_frames
    )

    worker_result = evaluate(worker_frames)

    assert worker_result == pysteps_preflight_module.evaluate_frames(
        local_frames
    )
    assert worker_result["status"] == "motion_indeterminate"
    assert worker_result["translation_censored"] is True
    nowcast = deterministic_nowcast(worker_frames, worker_result)
    assert nowcast["status"] == "motion_indeterminate"
    assert nowcast["steps"] == []
