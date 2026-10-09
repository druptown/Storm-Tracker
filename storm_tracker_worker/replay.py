"""Isolated deterministic phase-3 replay for dry-weather validation."""
from __future__ import annotations

import json
import time
from dataclasses import dataclass

from .calibration import calibration_operational
from .motion import motion_preflight
from .tracking import _input_digest, _state_digest, tracking_operational
from .tracking_core.engine.association_graph import (
    build_detection_association_graph,
)
from .tracking_core.engine.cell_track import (
    CellDetection,
    estimate_preliminary_cell_track,
)
from .tracking_core.engine.motion_consensus import (
    MotionConsensusResult,
    MotionVectorCandidate,
    select_motion_consensus,
)
from .virtual_probe import virtual_probe_operational


@dataclass(frozen=True)
class _Job:
    payload: dict


_REPLAY_MOTION_COVARIANCE = ((4.0, 0.0), (0.0, 4.0))


def _motion_candidate(
    method: str,
    role: str,
    vx_m_s: float,
    vy_m_s: float,
    *,
    skill: float = 0.8,
) -> MotionVectorCandidate:
    return MotionVectorCandidate(
        method=method,
        role=role,
        vx_m_s=vx_m_s,
        vy_m_s=vy_m_s,
        covariance_m2_s2=_REPLAY_MOTION_COVARIANCE,
        support_count=16,
        support_fraction=0.8,
        history_samples=4,
        hindcast_skill=skill,
    )


def _motion_result_payload(result: MotionConsensusResult) -> dict:
    return {
        "status": result.status,
        "confidence": result.confidence,
        "vx_m_s": result.vx_m_s,
        "vy_m_s": result.vy_m_s,
        "speed_m_s": result.speed_m_s,
        "heading_deg": result.heading_deg,
        "method": result.selected_method,
        "supporting_methods": list(result.supporting_methods),
        "reason_codes": list(result.reason_codes),
    }


def run_hybrid_motion_replay() -> dict:
    """Run bounded deterministic scientific edge cases without publication.

    These fixtures exercise the admission policy itself.  The separate real
    tracking-engine test covers frame-to-track binding and optical-flow
    extraction; this replay concentrates on the meteorological failure modes
    that previously produced implausible red direction lines.
    """

    translation = select_motion_consensus((
        _motion_candidate("flow-translation", "optical_flow", 6.0, 2.0),
        _motion_candidate("track-translation", "object_track", 5.7, 2.1),
    ))
    intensity_only = select_motion_consensus((
        _motion_candidate("flow-intensity-only", "optical_flow", 0.0, 0.0),
        _motion_candidate("track-intensity-only", "object_track", 0.0, 0.0),
    ))
    rotation_only = select_motion_consensus((
        _motion_candidate("flow-rotation-only", "optical_flow", 0.0, 0.0),
        _motion_candidate("track-centroid-drift", "object_track", 5.0, 0.0),
    ))
    system_east = select_motion_consensus((
        _motion_candidate("flow-system-east", "optical_flow", 5.0, 0.0),
        _motion_candidate("track-system-east", "object_track", 5.1, 0.1),
    ))
    system_north = select_motion_consensus((
        _motion_candidate("flow-system-north", "optical_flow", 0.0, 4.0),
        _motion_candidate("track-system-north", "object_track", 0.1, 4.1),
    ))
    checks = {
        "translation_admitted": translation.status == "ready_high",
        "intensity_change_has_no_heading": (
            intensity_only.status == "stationary"
            and intensity_only.heading_deg is None
        ),
        "rotation_centroid_conflict_withheld": (
            rotation_only.status == "indeterminate"
            and rotation_only.vx_m_s is None
            and rotation_only.vy_m_s is None
        ),
        "independent_systems_admitted": (
            system_east.status == "ready_high"
            and system_north.status == "ready_high"
        ),
        "independent_systems_keep_direction": (
            system_east.heading_deg is not None
            and system_north.heading_deg is not None
            and abs(system_east.heading_deg - system_north.heading_deg) > 45.0
        ),
    }
    return {
        "mode": "hybrid_motion_scientific_replay_v1",
        "fixture_kind": "synthetic_known_motion",
        "operational_state_mutated": False,
        "passed": all(checks.values()),
        "checks": checks,
        "scenarios": {
            "translation": _motion_result_payload(translation),
            "intensity_change_without_translation": (
                _motion_result_payload(intensity_only)
            ),
            "rotation_without_bulk_translation": (
                _motion_result_payload(rotation_only)
            ),
            "two_independent_systems": {
                "east": _motion_result_payload(system_east),
                "north": _motion_result_payload(system_north),
            },
        },
    }


def _frame(timestamp: int, shift: int, *, source: str = "replay_radar") -> dict:
    return {
        "source": source,
        "nominal_timestamp": timestamp,
        "bounds": [3.0, 50.0, 6.0, 53.0],
        "grid_deg": 0.02,
        "height": 40,
        "width": 40,
        "cells": [
            [row, column + shift, 120]
            for row in range(12, 16)
            for column in range(10, 14)
        ],
    }


def _observation(timestamp: int, shift: int) -> dict:
    longitude = 4.0 + shift * 0.04
    return {
        "obs_type": "radar",
        "lat": 51.0,
        "lon": longitude,
        "timestamp": timestamp,
        "intensity": 4,
        "max_dbz": 42.0,
        "mean_dbz": 35.0,
        "area_km2": 30.0,
        "quality": 0.95,
        "footprint_points": [
            [50.98, longitude - 0.02],
            [51.02, longitude + 0.02],
        ],
        "radar_cell_id": "replay-cell",
        "parent_system_id": "replay-system",
        "parent_area_km2": 45.0,
        "parent_footprint_points": [
            [50.97, longitude - 0.03],
            [51.03, longitude + 0.03],
        ],
        "rain_mm": None,
        "rain_5min": None,
        "rain_60min": None,
        "station_id": None,
        "rain_capable": False,
        "wind_speed": None,
        "wind_angle": None,
        "gust_speed": None,
        "pressure": None,
        "temperature": None,
        "humidity": None,
        "source": "replay_radar",
    }


def _calibration_frame(source: str, nominal_minute: int, shift: int) -> list:
    points = [
        [row, column + shift, 5.0, 0.95, 1]
        for row in range(12, 16)
        for column in range(10, 14)
    ]
    return [
        "replay-region", source, nominal_minute, nominal_minute * 60.0,
        nominal_minute * 60.0 + 1.0, 0.02, len(points), len(points),
        3.0, 50.0, 6.0, 53.0, 1.0, points,
    ]


def _shadow_cell_detection(
    label: str,
    frame_index: int,
    valid_at: float,
) -> CellDetection:
    if label == "a":
        x_m = frame_index * 1_000.0
        y_m = 0.0
    elif label == "b":
        x_m = 100_000.0
        y_m = frame_index * 500.0
    else:
        raise ValueError(f"onbekende synthetische cel: {label}")
    return CellDetection(
        detection_id=f"frame-{frame_index:02d}:cell-{label}",
        valid_at=valid_at,
        analysis_grid_id="synthetic-metric-grid-v1",
        x_m=x_m,
        y_m=y_m,
        position_sigma_m=1_000.0,
        source="synthetic_contract_fixture",
    )


def run_celltrack_shadow_replay(
    *,
    frame_count: int = 24,
    base_timestamp: int = 1_800_000_000,
) -> dict:
    """Exercise lossless CellTrack primitives over a >20-frame sequence.

    Ground-truth labels in this deterministic fixture provide the external
    association decision.  The replay therefore validates candidate retention,
    ranking and exact interval motion, but deliberately does not claim that a
    global assignment algorithm or real meteorological event is validated.
    """

    if not isinstance(frame_count, int) or frame_count < 20:
        raise ValueError(
            "CellTrack-kwalificatiereplay vereist minstens twintig frames"
        )
    timestamps = [
        float(base_timestamp + frame_index * 300)
        for frame_index in range(frame_count)
    ]
    frames = [
        tuple(
            _shadow_cell_detection(label, frame_index, timestamp)
            for label in ("a", "b")
        )
        for frame_index, timestamp in enumerate(timestamps)
    ]
    candidate_count = 0
    complete_graphs = True
    truth_ranked_first = True
    max_candidate_speed_kmh = 0.0
    speeds_by_track = {"track-a": [], "track-b": []}
    for previous, current in zip(frames, frames[1:]):
        graph = build_detection_association_graph(
            previous,
            current,
            motion_sigma_m_s=20.0,
        )
        complete_graphs = complete_graphs and graph.is_complete
        candidate_count += len(graph.candidates)
        max_candidate_speed_kmh = max(
            max_candidate_speed_kmh,
            *(candidate.interval_speed_kmh for candidate in graph.candidates),
        )
        for label, track_id in (("a", "track-a"), ("b", "track-b")):
            old = next(
                item
                for item in previous
                if item.detection_id.endswith(f"cell-{label}")
            )
            new = next(
                item
                for item in current
                if item.detection_id.endswith(f"cell-{label}")
            )
            truth_candidate = next(
                candidate
                for candidate in graph.candidates
                if candidate.previous_detection_id == old.detection_id
                and candidate.current_detection_id == new.detection_id
            )
            truth_ranked_first = (
                truth_ranked_first
                and truth_candidate.previous_rank == 1
                and truth_candidate.current_rank == 1
            )
            snapshot = estimate_preliminary_cell_track(
                track_id,
                old,
                new,
            )
            speeds_by_track[track_id].append(snapshot.speed_kmh)

    expected_candidates = (frame_count - 1) * 4
    checks = {
        "minimum_twenty_frames": frame_count >= 20,
        "every_graph_complete": complete_graphs,
        "every_pair_retained": candidate_count == expected_candidates,
        "truth_pairs_ranked_first": truth_ranked_first,
        "track_a_exact_speed": all(
            abs(speed - 12.0) < 1e-9
            for speed in speeds_by_track["track-a"]
        ),
        "track_b_exact_speed": all(
            abs(speed - 6.0) < 1e-9
            for speed in speeds_by_track["track-b"]
        ),
        "extreme_cross_candidates_retained": (
            max_candidate_speed_kmh > 1_000.0
        ),
    }
    return {
        "mode": "celltrack_shadow_contract_replay",
        "fixture_kind": "synthetic_known_identity",
        "operational_state_mutated": False,
        "global_assignment_validated": False,
        "real_meteorological_case": False,
        "passed": all(checks.values()),
        "checks": checks,
        "frame_count": frame_count,
        "cell_count_per_frame": 2,
        "candidate_count": candidate_count,
        "expected_candidate_count": expected_candidates,
        "max_candidate_speed_kmh": max_candidate_speed_kmh,
        "track_speeds_kmh": {
            track_id: {
                "minimum": min(speeds),
                "maximum": max(speeds),
                "samples": len(speeds),
            }
            for track_id, speeds in speeds_by_track.items()
        },
    }


def run_phase3_replay(*, base_timestamp: int | None = None) -> dict:
    """Exercise phase 3 without publishing or mutating operational state."""
    base = int(base_timestamp or time.time()) // 300 * 300 - 900
    timestamps = [base + offset for offset in (0, 300, 600)]
    frames = [_frame(timestamp, index) for index, timestamp in enumerate(timestamps)]
    motion = motion_preflight(_Job({
        "region_id": "replay-region",
        "frames": frames,
        "targets": [{
            "target_id": "replay-target",
            "latitude": 51.0,
            "longitude": 4.2,
        }],
    }))["result"]

    tracking_runs = []
    state: list[dict] = []
    for index, timestamp in enumerate(timestamps):
        observations = [_observation(timestamp, index)]
        result = tracking_operational(_Job({
            "tracking_protocol_version": 5,
            "region_id": "replay-region",
            "batch_id": f"{index + 1:032x}",
            "start_revision": index,
            "input_observation_count": len(observations),
            "start_state_count": len(state),
            "input_digest": _input_digest(observations),
            "start_state_digest": _state_digest(state),
            "start_state": state,
            "observations": observations,
        }))
        tracking_runs.append(result)
        state = result["snapshots"]

    probe = virtual_probe_operational(_Job({
        "observed_at": timestamps[-1] + 60,
        "active_probes": [{
            "probe_id": "replay-probe",
            "storm_id": tracking_runs[-1]["active_tracks"][0]["storm_id"],
            "region_id": "replay-region",
            "station_id": "replay-station",
            "latitude": 51.0,
            "longitude": 4.08,
            "created_at": timestamps[0],
            "expected_at": timestamps[-1],
            "eta_minutes": 10.0,
            "passage": "raak",
            "pass_distance_km": 0.0,
            "motion_confidence": "Hoog",
            "radar_source": "replay_radar",
        }],
        "history": {},
        "states_by_region": {},
        "observations_by_region": {
            "replay-region": [{
                "obs_type": "rain",
                "lat": 51.0,
                "lon": 4.08,
                "timestamp": timestamps[-1] + 30,
                "station_id": "replay-station",
                "rain_capable": True,
                "rain_mm": 0.6,
                "rain_60min": 1.4,
                "source": "replay_ground_truth",
            }],
        },
        "radar_sources_by_region": {"replay-region": "replay_radar"},
    }))

    minute = timestamps[-1] // 60
    calibration = calibration_operational(_Job({"frames": [
        _calibration_frame("replay_primary", minute, 2),
        _calibration_frame("replay_reference", minute, 2),
    ]}))
    storm_ids = [
        run["active_tracks"][0]["storm_id"] for run in tracking_runs
    ]
    checks = {
        "motion_ready": motion.get("status") == "ready",
        "nowcast_60_minutes": (
            motion.get("nowcast", {}).get("status") == "ready"
            and motion["nowcast"].get("horizon_minutes") == 60
        ),
        "tracking_continuity": len(set(storm_ids)) == 1,
        "three_tracking_frames": (
            len(tracking_runs[-1]["active_tracks"][0][
                "radar_frame_timestamps"
            ]) == 3
        ),
        "probe_ground_confirmation": (
            probe.get("samples", [{}])[0].get("own_status")
            == "regen_bevestigd"
        ),
        "calibration_parity": (
            len(calibration.get("comparisons", [])) == 1
            and calibration["comparisons"][0][12] == 1.0
        ),
    }
    return {
        "mode": "isolated_replay",
        "operational_state_mutated": False,
        "passed": all(checks.values()),
        "checks": checks,
        "motion": {
            "speed_kmh": motion.get("translation_km_per_hour"),
            "heading_deg": motion.get("heading_deg"),
            "confidence": motion.get("confidence"),
        },
        "tracking": {
            "cycles": len(tracking_runs),
            "stable_storm_id": len(set(storm_ids)) == 1,
            "frame_count": len(tracking_runs[-1]["active_tracks"][0][
                "radar_frame_timestamps"
            ]),
        },
        "probe": {
            "confirmed_count": probe["diagnostics"].get("confirmed_count", 0),
        },
        "calibration": {
            "comparisons": len(calibration.get("comparisons", [])),
        },
    }


def phase3_replay(job) -> dict:
    return run_phase3_replay(
        base_timestamp=job.payload.get("base_timestamp")
    )


if __name__ == "__main__":
    print(json.dumps(run_phase3_replay(), sort_keys=True))
