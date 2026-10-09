"""Independent storm tracking from raw observations and a shared start state."""
from __future__ import annotations

import asyncio
import hashlib
import json

from .tracking_core.engine.observation import Observation, ObservationType
from .tracking_core.engine.storm_engine import StormEngine

MAX_OBSERVATIONS = 8000
MAX_SNAPSHOTS = 2048


def _canonical(value) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _semantic_snapshots(snapshots: list[dict]) -> list[dict]:
    """Strip only processing-clock fields; keep meteorological timestamps."""
    normalized = []
    for snapshot in snapshots:
        item = dict(snapshot)
        item.pop("first_seen", None)
        item.pop("last_update", None)
        normalized.append(item)
    return normalized


def _normalized_state(snapshots: list[dict]) -> list[dict]:
    return sorted(
        snapshots,
        key=lambda item: str(item.get("storm_id", "")),
    )


def _input_digest(observations: list[dict]) -> str:
    return hashlib.sha256(_canonical({
        "schema": "stv3-tracking-observations-v5",
        "observations": observations,
    })).hexdigest()


def _state_digest(snapshots: list[dict]) -> str:
    return hashlib.sha256(_canonical({
        "schema": "stv3-tracking-state-v5",
        "snapshots": _normalized_state(snapshots),
    })).hexdigest()


def _observation(raw: dict) -> Observation:
    values = dict(raw)
    values["obs_type"] = ObservationType(str(values["obs_type"]))
    for key in ("footprint_points", "parent_footprint_points"):
        values[key] = tuple(
            tuple(float(coordinate) for coordinate in point)
            for point in values.get(key, ())
        )
    for key in (
        "footprint_polygons",
        "parent_footprint_polygons",
    ):
        values[key] = tuple(
            tuple(
                tuple(
                    tuple(float(coordinate) for coordinate in point)
                    for point in ring
                )
                for ring in polygon
            )
            for polygon in values.get(key, ())
        )
    return Observation(**values)


def track_observation_batch(payload: dict) -> dict:
    """Run the real tracking engine without consuming HA-computed tracks."""
    region_id = str(payload.get("region_id", ""))
    batch_id = payload.get("batch_id")
    start_revision = payload.get("start_revision")
    raw_observations = payload.get("observations", [])
    start_state = payload.get("start_state", [])
    if payload.get("tracking_protocol_version") != 5:
        raise ValueError("ongeldige tracking_protocol_version")
    if not region_id:
        raise ValueError("region_id ontbreekt")
    if (
        not isinstance(batch_id, str)
        or len(batch_id) != 32
        or any(character not in "0123456789abcdef" for character in batch_id)
    ):
        raise ValueError("ongeldige batch_id")
    if (
        isinstance(start_revision, bool)
        or not isinstance(start_revision, int)
        or start_revision < 0
    ):
        raise ValueError("ongeldige start_revision")
    if (
        not isinstance(raw_observations, list)
        or len(raw_observations) > MAX_OBSERVATIONS
        or payload.get("input_observation_count") != len(raw_observations)
    ):
        raise ValueError("ongeldige observatiebatch")
    if (
        not isinstance(start_state, list)
        or len(start_state) > MAX_SNAPSHOTS
        or payload.get("start_state_count") != len(start_state)
    ):
        raise ValueError("ongeldige trackingbeginsituatie")
    start_ids = [
        snapshot.get("storm_id")
        for snapshot in start_state
        if isinstance(snapshot, dict)
    ]
    if (
        len(start_ids) != len(start_state)
        or any(
            not isinstance(storm_id, str) or not storm_id
            for storm_id in start_ids
        )
        or len(set(start_ids)) != len(start_ids)
    ):
        raise ValueError("trackingbeginsituatie bevat ongeldige storm-ID's")

    start_state = _normalized_state(start_state)
    start_state_digest = _state_digest(start_state)
    input_digest = _input_digest(raw_observations)
    if payload.get("start_state_digest") != start_state_digest:
        raise ValueError("start_state_digest komt niet overeen")
    if payload.get("input_digest") != input_digest:
        raise ValueError("input_digest komt niet overeen")

    engine = StormEngine()
    restored = engine.restore_mcs_history(start_state)
    if restored != len(start_state):
        raise ValueError("trackingbeginsituatie kon niet volledig herstellen")
    observations = [_observation(raw) for raw in raw_observations]
    # Dit is al een geïsoleerde tijdelijke engine. Bij een fout wordt de
    # volledige instantie weggegooid; een tweede deepcopy-checkpoint zou de
    # NUC alleen extra CPU en geheugen kosten.
    asyncio.run(engine.process_batch(observations))
    snapshots = sorted(
        engine.export_tracking_state(),
        key=lambda item: str(item.get("storm_id", "")),
    )
    active = sorted(
        (
            {
                "storm_id": str(storm.storm_id),
                "centroid_lat": round(float(storm.centroid_lat), 5),
                "centroid_lon": round(float(storm.centroid_lon), 5),
                "tracking_status": str(storm.tracking_status),
                "dormant": bool(storm.is_dormant),
                "motion_model": str(storm.motion_model),
                "turn_rate_deg_per_hour": float(
                    storm.turn_rate_deg_per_hour
                ),
                "trajectory_curved": bool(storm.trajectory_curved),
                "curvature_radius_km": storm.curvature_radius_km,
                "acceleration_kmh2": round(
                    (
                        storm.acceleration_east_kmh2 ** 2
                        + storm.acceleration_north_kmh2 ** 2
                    ) ** 0.5,
                    1,
                ),
                "trajectory_outliers_rejected": int(
                    storm.trajectory_outliers_rejected
                ),
                "parent_storm_ids": sorted(storm.parent_storm_ids),
                "merged_storm_ids": sorted(storm.merged_storm_ids),
                "lineage_generation": int(storm.lineage_generation),
                "radar_frame_timestamps": [
                    int(value) for value in storm.radar_frame_timestamps[-12:]
                ],
            }
            for storm in engine.get_active_storms()
        ),
        key=lambda item: item["storm_id"],
    )
    lineage_events = engine.consume_lineage_events()
    return {
        "method": "independent_tracking_batch_v5",
        "tracking_protocol_version": 5,
        "region_id": region_id,
        "batch_id": batch_id,
        "start_revision": start_revision,
        "result_revision": start_revision + 1,
        "input_observation_count": len(observations),
        "start_state_count": len(start_state),
        "input_digest": input_digest,
        "start_state_digest": start_state_digest,
        "snapshot_count": len(snapshots),
        "snapshot_digest": _state_digest(snapshots),
        "semantic_snapshot_digest": hashlib.sha256(
            _canonical({
                "schema": "stv3-tracking-semantic-state-v5",
                "snapshots": _semantic_snapshots(snapshots),
            })
        ).hexdigest(),
        "active_digest": hashlib.sha256(_canonical(active)).hexdigest(),
        "snapshots": snapshots,
        "active_tracks": active,
        "lineage_events": lineage_events,
        "split_count": sum(
            event.get("event") == "split" for event in lineage_events
        ),
        "merge_count": sum(
            event.get("event") == "merge" for event in lineage_events
        ),
    }


def tracking_operational(job) -> dict:
    return track_observation_batch(job.payload)
