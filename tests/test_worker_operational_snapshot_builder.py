from __future__ import annotations

import json

import pytest

from storm_tracker_worker.desired_state import parse_desired_state
from storm_tracker_worker.operational_snapshot_builder import (
    OperationalSnapshotBuildError,
    PENDING_REGION_ID,
    UNKNOWN_SNAPSHOT_TTL_SECONDS,
    build_unknown_operational_snapshot,
)
from storm_tracker_worker.multi_source_operational_cycle import (
    EXPECTED_SOURCE_IDS,
    SOURCE_COUNT,
)
from storm_tracker_worker.tracking_core.operational_snapshot import (
    MAX_CONTROL_ENVELOPE_BYTES,
    MAX_SNAPSHOT_TTL_SECONDS,
    encode_control_envelope,
)


NOW = 1_800_000_000.0


def _desired_state(
    count: int = 2,
    *,
    all_available: bool = False,
    spacing_degrees: float = 0.01,
):
    targets = []
    for index in range(count):
        targets.append({
            "target_id": f"target-{index:02d}",
            "display_name": f"Tracker {index:02d}",
            "kind": "device_tracker" if index else "home",
            "available": all_available or index % 2 == 0,
            "latitude": 51.05 + index * spacing_degrees,
            "longitude": 4.4 + index * spacing_degrees,
            "position_observed_at": (
                NOW - 10 if all_available or index % 2 == 0 else None
            ),
            "location_accuracy_m": (
                12.0 if all_available or index % 2 == 0 else None
            ),
            "observation_radius_km": 250.0,
            "country_code": "BE",
        })
    return parse_desired_state({
        "contract": "stv3-worker-desired-state-v1",
        "producer_id": "ha:boot-a",
        "authority_epoch": 1,
        "generation": 1,
        "issued_at": NOW,
        "previous_state_sha256": None,
        "targets": targets,
        "settings": {
            "ground_radius_km": 20.0,
            "lightning_source_mode": "auto",
            "model_guidance_enabled": True,
            "region_sharing_distance_km": 100.0,
        },
    })


def test_unknown_builder_preserves_targets_but_exposes_no_positions_or_weather():
    desired = _desired_state()
    snapshot = build_unknown_operational_snapshot(
        desired,
        revision=1,
        evaluated_at=NOW + 1,
        worker_state="online",
        queue_depth=3,
        failed_jobs=2,
    )

    assert snapshot["product_valid_at"] is None
    assert snapshot["received_at"] is None
    assert snapshot["valid_until"] == NOW + 1 + UNKNOWN_SNAPSHOT_TTL_SECONDS
    assert snapshot["desired_state_sha256"] == desired.state_sha256
    assert UNKNOWN_SNAPSHOT_TTL_SECONDS == MAX_SNAPSHOT_TTL_SECONDS
    assert snapshot["provenance_complete"] is False
    assert len(snapshot["providers"]["items"]) == SOURCE_COUNT == 28
    assert {
        item["provider_id"] for item in snapshot["providers"]["items"]
    } == EXPECTED_SOURCE_IDS
    assert all(
        item["state"] == "unknown"
        and item["freshness"] == "unknown"
        and item["product_valid_at"] is None
        and item["received_at"] is None
        and item["error"] is None
        and item["reason_codes"] == ["awaiting_meteorological_evidence"]
        for item in snapshot["providers"]["items"]
    )
    assert len(snapshot["regions"]["items"]) == 2
    real_region, pending_region = snapshot["regions"]["items"]
    assert real_region == {
        "region_id": real_region["region_id"],
        "display_name": "Regio 1",
        "state": "unknown",
        "provider_ids": [],
        "target_ids": ["target-00"],
    }
    assert real_region["region_id"].startswith("region:")
    assert real_region["region_id"] != PENDING_REGION_ID
    assert pending_region == {
        "region_id": PENDING_REGION_ID,
        "display_name": "Nog niet meteorologisch toegewezen",
        "state": "unknown",
        "provider_ids": [],
        "target_ids": ["target-01"],
    }
    assert [item["target_id"] for item in snapshot["targets"]["items"]] == [
        "target-00", "target-01"
    ]
    for target in snapshot["targets"]["items"]:
        assert target["current_precipitation"]["state"] == "unknown"
        assert target["next_threat"]["state"] == "unknown"
        assert target["status_text"] == ""
    assert {
        item["target_id"]: item["region_id"]
        for item in snapshot["targets"]["items"]
    } == {
        "target-00": real_region["region_id"],
        "target-01": PENDING_REGION_ID,
    }
    encoded = json.dumps(snapshot, sort_keys=True)
    for forbidden in (
        "latitude", "longitude", "location_accuracy_m", "51.05", "4.4",
        "producer_id", "authority_epoch", "credential", "token",
    ):
        assert forbidden not in encoded


def test_unknown_builder_sorts_pending_region_with_live_dependent_hashes():
    targets = [
        {
            "target_id": "live-003",
            "display_name": "live-003",
            "kind": "device_tracker",
            "available": True,
            "latitude": 51.05,
            "longitude": 4.4,
            "position_observed_at": NOW - 10,
            "location_accuracy_m": 12.0,
            "observation_radius_km": 250.0,
            "country_code": "BE",
        },
        {
            "target_id": "unavailable",
            "display_name": "unavailable",
            "kind": "device_tracker",
            "available": False,
            "latitude": 51.06,
            "longitude": 4.41,
            "position_observed_at": NOW - 10,
            "location_accuracy_m": 12.0,
            "observation_radius_km": 250.0,
            "country_code": "BE",
        },
    ]
    desired = parse_desired_state({
        "contract": "stv3-worker-desired-state-v1",
        "producer_id": "ha:boot-a",
        "authority_epoch": 1,
        "generation": 1,
        "issued_at": NOW,
        "previous_state_sha256": None,
        "targets": targets,
        "settings": {
            "ground_radius_km": 20.0,
            "lightning_source_mode": "auto",
            "model_guidance_enabled": True,
            "region_sharing_distance_km": 100.0,
        },
    })

    snapshot = build_unknown_operational_snapshot(
        desired,
        revision=1,
        evaluated_at=NOW + 1,
        worker_state="online",
        queue_depth=0,
        failed_jobs=0,
    )

    region_ids = [item["region_id"] for item in snapshot["regions"]["items"]]
    assert region_ids == sorted(region_ids)
    assert region_ids[0] == PENDING_REGION_ID
    assert region_ids[1].startswith("region:b")


def test_unknown_builder_exposes_two_real_planned_regions_without_coordinates():
    snapshot = build_unknown_operational_snapshot(
        _desired_state(2, all_available=True, spacing_degrees=4.0),
        revision=1,
        evaluated_at=NOW,
        worker_state="online",
        queue_depth=0,
        failed_jobs=0,
    )

    assert len(snapshot["regions"]["items"]) == 2
    assert all(
        item["region_id"] != PENDING_REGION_ID
        for item in snapshot["regions"]["items"]
    )
    assert sorted(
        len(item["target_ids"])
        for item in snapshot["regions"]["items"]
    ) == [1, 1]
    encoded = json.dumps(snapshot, sort_keys=True)
    for forbidden in ("latitude", "longitude", "51.05", "4.4"):
        assert forbidden not in encoded


@pytest.mark.parametrize("worker_state", ["offline", "unknown", "degraded", "busy"])
def test_all_worker_states_remain_unknown_without_evidence(worker_state):
    snapshot = build_unknown_operational_snapshot(
        _desired_state(1),
        revision=8,
        evaluated_at=NOW,
        worker_state=worker_state,
        queue_depth=0,
        failed_jobs=0,
    )
    assert snapshot["worker"]["state"] == worker_state
    assert snapshot["targets"]["items"][0]["current_precipitation"]["state"] == "unknown"
    assert snapshot["targets"]["items"][0]["next_threat"]["state"] == "unknown"


def test_empty_desired_state_needs_no_synthetic_region():
    snapshot = build_unknown_operational_snapshot(
        _desired_state(0),
        revision=1,
        evaluated_at=NOW,
        worker_state="online",
        queue_depth=0,
        failed_jobs=0,
    )
    assert snapshot["regions"]["items"] == []
    assert snapshot["targets"]["items"] == []


def test_all_64_targets_fit_transport_budget_without_coordinate_leak():
    snapshot = build_unknown_operational_snapshot(
        _desired_state(64),
        revision=1,
        evaluated_at=NOW,
        worker_state="online",
        queue_depth=0,
        failed_jobs=0,
    )
    encoded = encode_control_envelope(snapshot)
    assert len(encoded) <= MAX_CONTROL_ENVELOPE_BYTES
    assert snapshot["providers"] == {
        "complete": False,
        "overflow_count": SOURCE_COUNT,
        "items": [],
    }
    assert b"latitude" not in encoded
    assert b"longitude" not in encoded


@pytest.mark.parametrize(
    "kwargs",
    (
        {"revision": 0},
        {"evaluated_at": float("nan")},
        {"worker_state": "sunny"},
        {"queue_depth": -1},
        {"failed_jobs": True},
        {"reason_code": "invalid reason"},
        {"worker_status_text": " x"},
    ),
)
def test_builder_rejects_invalid_control_material(kwargs):
    values = {
        "revision": 1,
        "evaluated_at": NOW,
        "worker_state": "online",
        "queue_depth": 0,
        "failed_jobs": 0,
    }
    values.update(kwargs)
    with pytest.raises(OperationalSnapshotBuildError):
        build_unknown_operational_snapshot(_desired_state(1), **values)


def test_builder_requires_prevalidated_desired_state():
    with pytest.raises(OperationalSnapshotBuildError, match="strikt"):
        build_unknown_operational_snapshot(
            {},
            revision=1,
            evaluated_at=NOW,
            worker_state="online",
            queue_depth=0,
            failed_jobs=0,
        )
