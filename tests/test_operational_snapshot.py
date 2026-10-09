"""Adversarial tests for the bounded operational snapshot boundary."""
from __future__ import annotations

import ast
import copy
import hashlib
import importlib.util
import inspect
import json
from pathlib import Path
import sys
import textwrap

import pytest

from storm_tracker_worker.tracking_core import operational_snapshot as contract


ROOT = Path(__file__).resolve().parents[1]
HA_MODULE = ROOT / "custom_components/storm_tracker_v3/operational_snapshot.py"
WORKER_MODULE = ROOT / "storm_tracker_worker/tracking_core/operational_snapshot.py"


def _load_ha_mirror():
    name = "storm_tracker_v3_operational_snapshot_test_mirror"
    existing = sys.modules.get(name)
    if existing is not None:
        return existing
    spec = importlib.util.spec_from_file_location(name, HA_MODULE)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _provider(provider_id: str = "rainviewer") -> dict:
    return {
        "provider_id": provider_id,
        "display_name": provider_id.upper(),
        "state": "operational",
        "freshness": "current",
        "product_valid_at": 1_000.0,
        "received_at": 1_002.0,
        "error": None,
        "reason_codes": [],
    }


def _ground_evidence() -> dict:
    return {
        "status": "unknown",
        "freshness": "current",
        "confirmation": None,
        "same_track": None,
        "evidence_valid_at": None,
        "observed_at_min": None,
        "observed_at_max": None,
        "received_at": None,
        "physical_site_count": 0,
        "wet_site_count": 0,
        "source_ids": [],
        "reason_codes": [],
    }


def _current_precipitation(
    state: str = "dry", freshness: str = "current"
) -> dict:
    return {
        "state": state,
        "freshness": freshness,
        "rain_rate_mm_h": (
            0.0 if state == "dry" else 1.2 if state == "raining" else None
        ),
        "product_valid_at": 1_000.0,
        "provider_ids": ["rainviewer"],
        "reason_codes": [],
        "ground_evidence": _ground_evidence(),
    }


def _next_threat(state: str = "available", freshness: str = "current") -> dict:
    available = state == "available" and freshness == "current"
    return {
        "state": state,
        "freshness": freshness,
        "track_id": "storm-1" if available else None,
        "calibrated_probability_percent": 73.5 if available else None,
        "probability_calibration_id": "empirical:threat-v1" if available else None,
        "eta_minutes": 20.0 if available else None,
        "distance_km": 18.5 if available else None,
        "passage_status": "expected" if available else (
            "none" if state == "none" and freshness == "current" else "unknown"
        ),
        "product_valid_at": 1_000.0,
        "provider_ids": ["rainviewer"],
        "reason_codes": [],
    }


def _target(
    target_id: str = "home",
    *,
    region_id: str = "region-home",
    status: str = "dry",
    freshness: str = "current",
) -> dict:
    return {
        "target_id": target_id,
        "display_name": target_id.upper(),
        "region_id": region_id,
        "status_text": "Droog nu; gekalibreerde volgende dreiging over 20 min",
        "current_precipitation": _current_precipitation(status, freshness),
        "next_threat": _next_threat(),
    }


def _region(target_ids=None, provider_ids=None, region_id="region-home") -> dict:
    return {
        "region_id": region_id,
        "display_name": region_id.upper(),
        "state": "current",
        "provider_ids": sorted(provider_ids or ["rainviewer"]),
        "target_ids": sorted(target_ids or ["home"]),
    }


def _storm(storm_id: str = "storm-1", region_id: str = "region-home") -> dict:
    return {
        "storm_id": storm_id,
        "region_id": region_id,
        "display_name": storm_id.upper(),
        "state": "active",
        "diagnostic_quality_percent": 86.0,
        "diagnostic_quality_kind": "motion_fit",
        "motion_speed_kmh": 42.75,
        "motion_heading_deg": 227.5,
        "max_dbz": 51.0,
        "area_km2": 740.0,
        "product_valid_at": 1_000.0,
        "reason_codes": [],
    }


def _snapshot(revision: int = 1) -> dict:
    return {
        "contract": "operational_snapshot_v1",
        "revision": revision,
        "desired_state_sha256": "d" * 64,
        "product_valid_at": 1_000.0,
        "received_at": 1_002.0,
        "evaluated_at": 1_004.0,
        "valid_until": 1_304.0,
        "provenance_complete": True,
        "provenance_source_ids": ["rainviewer"],
        "provenance_reason_codes": [],
        "worker": {
            "state": "online",
            "queue_depth": 0,
            "failed_jobs": 0,
            "status_text": "operationeel",
        },
        "regions": {
            "complete": True,
            "overflow_count": 0,
            "items": [_region()],
        },
        "targets": {
            "complete": True,
            "overflow_count": 0,
            "items": [_target()],
        },
        "providers": {
            "complete": True,
            "overflow_count": 0,
            "items": [_provider()],
        },
        "ui_storms": {
            "complete": True,
            "overflow_count": 0,
            "items": [_storm()],
        },
        "map_artifact": {
            "artifact_handle": "sha256:" + "a" * 64,
            "etag": '"map-etag-a"',
        },
    }


def _many_targets_snapshot(count: int, *, verbose: bool = False) -> dict:
    snapshot = _snapshot()
    targets = []
    for index in range(count):
        target = _target(f"target-{index:02d}")
        if verbose:
            target["status_text"] = "s" * 240
            target["current_precipitation"]["reason_codes"] = [
                f"r{reason:02d}_" + "x" * 76
                for reason in range(4)
            ]
        targets.append(target)
    target_ids = [item["target_id"] for item in targets]
    snapshot["targets"]["items"] = targets
    snapshot["regions"]["items"] = [_region(target_ids=target_ids)]
    return snapshot


def test_worker_and_ha_contract_are_byte_identical_and_encode_identically():
    assert HA_MODULE.read_bytes() == WORKER_MODULE.read_bytes()
    assert hashlib.sha256(HA_MODULE.read_bytes()).hexdigest() == hashlib.sha256(
        WORKER_MODULE.read_bytes()
    ).hexdigest()

    ha = _load_ha_mirror()
    snapshot = _snapshot()
    assert ha.encode_control_envelope(snapshot) == contract.encode_control_envelope(
        snapshot
    )


def test_snapshot_accepts_compact_regional_radar_fusion_collection():
    snapshot = _snapshot()
    snapshot["radar_fusion"] = {
        "complete": True,
        "overflow_count": 0,
        "items": [{
            "region_id": "region-home",
            "grid_id": "grid:abc",
            "status": "current",
            "mode": "fused",
            "contributors": ["dwd_radolan", "knmi_radar"],
            "excluded": {},
            "standby": {},
            "reason_codes": [],
            "coverage_fraction": 0.92,
            "mean_confidence": 0.84,
            "disagreement_fraction": 0.08,
            "evaluated_at": 1_000.0,
            "field_observed_at": 990.0,
        }],
    }

    validated = contract.validate_snapshot(snapshot)

    assert validated["radar_fusion"]["items"][0]["mode"] == "fused"


def test_historical_snapshot_without_radar_fusion_remains_valid():
    assert "radar_fusion" not in contract.validate_snapshot(_snapshot())


def test_canonical_round_trip_has_exact_digest_and_independent_times():
    snapshot = _snapshot()
    # A forecast product can validly refer to a time after reception.  The
    # three clocks stay independent instead of being collapsed or reordered.
    snapshot["product_valid_at"] = 2_000.0
    snapshot["received_at"] = 1_000.0
    snapshot["evaluated_at"] = 1_500.0
    snapshot["valid_until"] = 1_800.0

    encoded = contract.encode_control_envelope(snapshot)
    decoded = contract.decode_control_envelope(encoded)

    assert encoded == contract.canonical_json_bytes(decoded)
    assert len(encoded) < contract.RECOMMENDED_CONTROL_ENVELOPE_BYTES
    assert decoded["revision"] == snapshot["revision"]
    assert decoded["snapshot"]["product_valid_at"] == 2_000.0
    assert decoded["snapshot"]["received_at"] == 1_000.0
    assert decoded["snapshot"]["evaluated_at"] == 1_500.0
    assert decoded["snapshot"]["valid_until"] == 1_800.0
    assert decoded["snapshot_sha256"] == hashlib.sha256(
        contract.canonical_json_bytes(decoded["snapshot"])
    ).hexdigest()


def test_unknown_snapshot_may_omit_product_time_without_inventing_weather_time():
    snapshot = _snapshot()
    snapshot["product_valid_at"] = None
    snapshot["received_at"] = None
    snapshot["provenance_complete"] = False
    snapshot["provenance_source_ids"] = []
    snapshot["regions"] = {"complete": True, "overflow_count": 0, "items": []}
    snapshot["targets"] = {"complete": True, "overflow_count": 0, "items": []}
    snapshot["providers"] = {"complete": True, "overflow_count": 0, "items": []}
    snapshot["ui_storms"] = {"complete": True, "overflow_count": 0, "items": []}
    snapshot["map_artifact"] = {"artifact_handle": None, "etag": None}

    decoded = contract.decode_control_envelope(
        contract.encode_control_envelope(snapshot)
    )
    assert decoded["snapshot"]["product_valid_at"] is None
    assert decoded["snapshot"]["received_at"] is None


def test_snapshot_validity_window_is_strict_and_hard_bounded():
    exact_maximum = _snapshot()
    exact_maximum["valid_until"] = (
        exact_maximum["evaluated_at"] + contract.MAX_SNAPSHOT_TTL_SECONDS
    )
    assert contract.validate_snapshot(exact_maximum)["valid_until"] == 1_304.0

    for invalid in (1_004.0, 1_003.0):
        snapshot = _snapshot()
        snapshot["valid_until"] = invalid
        with pytest.raises(contract.SnapshotValidationError, match="strikt na"):
            contract.validate_snapshot(snapshot)

    too_long = _snapshot()
    too_long["valid_until"] += 0.001
    with pytest.raises(contract.SnapshotValidationError, match="TTL-grens"):
        contract.validate_snapshot(too_long)


@pytest.mark.parametrize(
    "invalid",
    (None, "", "A" * 64, "0" * 63, "g" * 64),
)
def test_desired_state_authority_hash_is_required_and_strict(invalid):
    snapshot = _snapshot()
    snapshot["desired_state_sha256"] = invalid
    with pytest.raises(
        contract.SnapshotValidationError,
        match="desired_state_sha256",
    ):
        contract.validate_snapshot(snapshot)


def test_receiver_authority_mismatch_is_rejected_before_cache_swap():
    cache = contract.OperationalSnapshotCache()
    current = _snapshot(1)
    cache.apply_snapshot(
        current,
        expected_desired_state_sha256="d" * 64,
    )
    original_digest = cache.snapshot_sha256

    obsolete = _snapshot(2)
    obsolete["desired_state_sha256"] = "e" * 64
    with pytest.raises(
        contract.SnapshotRevisionConflict,
        match="desired-state-authoriteit",
    ):
        cache.apply_snapshot(
            obsolete,
            expected_desired_state_sha256="d" * 64,
        )

    assert cache.revision == 1
    assert cache.snapshot_sha256 == original_digest
    assert cache.get_status()["desired_state_sha256"] == "d" * 64


def test_receiver_clock_boundary_caps_authority_and_duplicate_cannot_extend_it():
    cache = contract.OperationalSnapshotCache()
    boundary = _snapshot(1)
    boundary["evaluated_at"] = 1_060.0
    boundary["valid_until"] = 1_360.0

    applied = cache.apply_snapshot(
        boundary,
        receiver_now=1_000.0,
        max_future_skew_seconds=60.0,
    )

    assert applied.action == "applied"
    assert cache.revision == 1
    assert cache.valid_until == 1_300.0
    duplicate = cache.apply_snapshot(
        boundary,
        receiver_now=1_050.0,
        max_future_skew_seconds=60.0,
    )
    assert duplicate.action == "duplicate"
    assert cache.valid_until == 1_300.0


@pytest.mark.parametrize("future_delta", (60.001, 24 * 60 * 60.0))
def test_future_or_clock_backward_snapshot_is_rejected_before_cache_swap(
    future_delta,
):
    cache = contract.OperationalSnapshotCache()
    current = _snapshot(1)
    current["evaluated_at"] = 1_000.0
    current["valid_until"] = 1_300.0
    cache.apply_snapshot(current, receiver_now=1_000.0)
    original_digest = cache.snapshot_sha256

    candidate = _snapshot(2)
    candidate["evaluated_at"] = 1_000.0 + future_delta
    candidate["valid_until"] = candidate["evaluated_at"] + 300.0
    with pytest.raises(contract.SnapshotValidationError, match="klokgrens"):
        cache.apply_snapshot(candidate, receiver_now=1_000.0)

    assert cache.revision == 1
    assert cache.snapshot_sha256 == original_digest
    assert cache.valid_until == 1_300.0

    clock_backward = _snapshot(2)
    clock_backward["evaluated_at"] = 1_001.0
    clock_backward["valid_until"] = 1_301.0
    with pytest.raises(contract.SnapshotValidationError, match="klokgrens"):
        cache.apply_snapshot(clock_backward, receiver_now=900.0)
    assert cache.revision == 1
    assert cache.snapshot_sha256 == original_digest


def test_null_product_time_cannot_hide_current_or_complete_claims():
    current = _snapshot()
    current["product_valid_at"] = None
    with pytest.raises(contract.SnapshotValidationError, match="product- en receipttijd"):
        contract.validate_snapshot(current)

    missing_receipt = _snapshot()
    missing_receipt["received_at"] = None
    with pytest.raises(contract.SnapshotValidationError, match="product- en receipttijd"):
        contract.validate_snapshot(missing_receipt)

    no_source = _snapshot()
    no_source["regions"] = {"complete": True, "overflow_count": 0, "items": []}
    no_source["targets"] = {"complete": True, "overflow_count": 0, "items": []}
    no_source["providers"] = {"complete": True, "overflow_count": 0, "items": []}
    no_source["ui_storms"] = {"complete": True, "overflow_count": 0, "items": []}
    no_source["map_artifact"] = {"artifact_handle": None, "etag": None}
    no_source["provenance_source_ids"] = []
    with pytest.raises(contract.SnapshotValidationError, match="complete provenance"):
        contract.validate_snapshot(no_source)


@pytest.mark.parametrize(
    "mutation",
    (
        lambda value: value.__setitem__("extra", 1),
        lambda value: value.pop("evaluated_at"),
        lambda value: value.pop("valid_until"),
        lambda value: value.pop("desired_state_sha256"),
        lambda value: value["map_artifact"].__setitem__("features", []),
        lambda value: value["targets"]["items"][0].__setitem__(
            "observations", []
        ),
        lambda value: value["providers"]["items"][0].pop("error"),
    ),
)
def test_exact_keys_reject_smuggled_missing_or_bulk_fields(mutation):
    snapshot = _snapshot()
    mutation(snapshot)
    with pytest.raises(contract.SnapshotValidationError):
        contract.validate_snapshot(snapshot)


def test_transport_rejects_whitespace_duplicate_keys_and_digest_tampering():
    encoded = contract.encode_control_envelope(_snapshot())
    with pytest.raises(contract.SnapshotCanonicalEncodingError):
        contract.decode_control_envelope(b" " + encoded)

    duplicate = encoded.replace(
        b'{"contract":',
        b'{"contract":"operational_snapshot_v1","contract":',
        1,
    )
    with pytest.raises(contract.SnapshotValidationError, match="dubbele"):
        contract.decode_control_envelope(duplicate)

    decoded = json.loads(encoded)
    decoded["snapshot_sha256"] = "0" * 64
    tampered = contract.canonical_json_bytes(decoded)
    with pytest.raises(contract.SnapshotValidationError, match="digest"):
        contract.decode_control_envelope(tampered)


def test_hard_transport_bound_is_applied_before_json_parsing():
    oversized = b"{" + b" " * contract.MAX_CONTROL_ENVELOPE_BYTES
    with pytest.raises(contract.SnapshotSizeError, match="64-KiB"):
        contract.decode_control_envelope(oversized)


def test_valid_but_oversized_snapshot_is_rejected_uncompressed():
    snapshot = _many_targets_snapshot(contract.MAX_TARGETS, verbose=True)
    for target in snapshot["targets"]["items"]:
        target["current_precipitation"]["reason_codes"] = [
            f"r{reason:02d}_" + "x" * 150
            for reason in range(contract.MAX_REASON_CODES)
        ]
    with pytest.raises(contract.SnapshotSizeError, match="64-KiB"):
        contract.encode_control_envelope(snapshot)


def test_all_64_minimal_targets_fit_the_hard_control_envelope():
    encoded = contract.encode_control_envelope(
        _many_targets_snapshot(contract.MAX_TARGETS)
    )
    assert len(encoded) <= contract.MAX_CONTROL_ENVELOPE_BYTES


@pytest.mark.parametrize(
    ("collection", "maximum", "factory", "id_field"),
    (
        ("regions", contract.MAX_REGIONS, lambda index: _region([], [], f"region-{index:02d}"), "region_id"),
        ("targets", contract.MAX_TARGETS, lambda index: _target(f"target-{index:02d}"), "target_id"),
        ("providers", contract.MAX_PROVIDERS, lambda index: _provider(f"provider-{index:02d}"), "provider_id"),
        ("ui_storms", contract.MAX_UI_STORM_SUMMARIES, lambda index: _storm(f"storm-{index:02d}"), "storm_id"),
    ),
)
def test_collection_caps_fail_closed_before_cross_reference_work(
    collection, maximum, factory, id_field
):
    snapshot = _snapshot()
    items = [factory(index) for index in range(maximum + 1)]
    items.sort(key=lambda item: item[id_field])
    snapshot[collection]["items"] = items
    with pytest.raises(contract.SnapshotValidationError, match="limiet"):
        contract.validate_snapshot(snapshot)


def test_completeness_and_overflow_are_explicit_and_consistent():
    snapshot = _snapshot()
    snapshot["providers"].update({"complete": False, "overflow_count": 3})
    validated = contract.validate_snapshot(snapshot)
    assert validated["providers"]["complete"] is False
    assert validated["providers"]["overflow_count"] == 3

    snapshot["providers"].update({"complete": True, "overflow_count": 3})
    with pytest.raises(contract.SnapshotValidationError, match="spreken elkaar tegen"):
        contract.validate_snapshot(snapshot)


def test_dry_requires_current_complete_provenance_and_live_worker():
    incomplete = _snapshot()
    incomplete["provenance_complete"] = False
    with pytest.raises(contract.SnapshotValidationError, match="dry"):
        contract.validate_snapshot(incomplete)

    stale = _snapshot()
    stale_current = stale["targets"]["items"][0]["current_precipitation"]
    stale_current["freshness"] = "stale"
    stale_current["ground_evidence"]["freshness"] = "stale"
    with pytest.raises(contract.SnapshotValidationError, match="huidige neerslag"):
        contract.validate_snapshot(stale)

    offline = _snapshot()
    offline["worker"]["state"] = "offline"
    with pytest.raises(contract.SnapshotValidationError, match="offline"):
        contract.validate_snapshot(offline)


def test_offline_unknown_snapshot_is_valid_but_never_dry():
    snapshot = _snapshot()
    snapshot["worker"]["state"] = "offline"
    snapshot["provenance_complete"] = False
    target = snapshot["targets"]["items"][0]
    target["status_text"] = ""
    target["current_precipitation"].update({
        "state": "unknown",
        "freshness": "stale",
        "rain_rate_mm_h": None,
    })
    target["current_precipitation"]["ground_evidence"].update({
        "freshness": "stale",
        "confirmation": None,
        "same_track": None,
    })
    target["next_threat"].update({
        "state": "unknown",
        "freshness": "stale",
        "track_id": None,
        "calibrated_probability_percent": None,
        "probability_calibration_id": None,
        "eta_minutes": None,
        "distance_km": None,
        "passage_status": "unknown",
    })
    snapshot["regions"]["items"][0]["state"] = "stale"
    snapshot["providers"]["items"][0].update({
        "state": "offline",
        "freshness": "stale",
    })
    snapshot["ui_storms"]["items"][0].update({
        "state": "unknown",
        "diagnostic_quality_percent": None,
        "diagnostic_quality_kind": None,
        "motion_speed_kmh": None,
        "motion_heading_deg": None,
        "max_dbz": None,
        "area_km2": None,
    })
    assert contract.validate_snapshot(snapshot)["targets"]["items"][0][
        "current_precipitation"
    ]["state"] == "unknown"


def test_current_precipitation_and_next_threat_never_conflate():
    snapshot = _snapshot()
    target = snapshot["targets"]["items"][0]

    validated = contract.validate_snapshot(snapshot)
    current = validated["targets"]["items"][0]["current_precipitation"]
    threat = validated["targets"]["items"][0]["next_threat"]
    assert current["state"] == "dry"
    assert current["rain_rate_mm_h"] == 0.0
    assert threat["state"] == "available"
    assert threat["track_id"] == "storm-1"
    assert threat["eta_minutes"] == 20.0

    for conflated_state in ("nearby", "approaching", "departing"):
        invalid = copy.deepcopy(snapshot)
        invalid["targets"]["items"][0]["current_precipitation"][
            "state"
        ] = conflated_state
        with pytest.raises(contract.SnapshotValidationError, match="onbekende status"):
            contract.validate_snapshot(invalid)

    assert "precipitation_status" not in target
    assert "storm_id" not in target
    assert "eta_minutes" not in target


def test_probability_is_nullable_or_empirically_calibrated_never_heuristic():
    snapshot = _snapshot()
    threat = snapshot["targets"]["items"][0]["next_threat"]
    validated = contract.validate_snapshot(snapshot)
    assert validated["targets"]["items"][0]["next_threat"][
        "probability_calibration_id"
    ] == "empirical:threat-v1"

    missing_calibration = copy.deepcopy(snapshot)
    missing_calibration["targets"]["items"][0]["next_threat"][
        "probability_calibration_id"
    ] = None
    with pytest.raises(contract.SnapshotValidationError, match="calibration-ID"):
        contract.validate_snapshot(missing_calibration)

    nullable = copy.deepcopy(snapshot)
    nullable_threat = nullable["targets"]["items"][0]["next_threat"]
    nullable_threat["calibrated_probability_percent"] = None
    nullable_threat["probability_calibration_id"] = None
    contract.validate_snapshot(nullable)

    heuristic_id = copy.deepcopy(snapshot)
    heuristic_id["targets"]["items"][0]["next_threat"][
        "probability_calibration_id"
    ] = "heuristic-v1"
    with pytest.raises(contract.SnapshotValidationError, match="empirisch"):
        contract.validate_snapshot(heuristic_id)

    heuristic = copy.deepcopy(snapshot)
    heuristic["targets"]["items"][0]["next_threat"][
        "confidence_percent"
    ] = 80
    with pytest.raises(contract.SnapshotValidationError):
        contract.validate_snapshot(heuristic)
    assert threat["calibrated_probability_percent"] == 73.5


def test_ground_evidence_preserves_time_site_source_and_same_track_truth():
    snapshot = _snapshot()
    target = snapshot["targets"]["items"][0]
    current = target["current_precipitation"]
    current.update({"state": "raining", "rain_rate_mm_h": 0.2})
    ground = current["ground_evidence"]
    ground.update({
        "status": "mixed",
        "freshness": "current",
        "confirmation": True,
        "same_track": True,
        "evidence_valid_at": 1_000.0,
        "observed_at_min": 995.0,
        "observed_at_max": 1_000.0,
        "received_at": 1_002.0,
        "physical_site_count": 3,
        "wet_site_count": 2,
        "source_ids": ["rainviewer"],
    })

    validated_ground = contract.validate_snapshot(snapshot)["targets"][
        "items"
    ][0]["current_precipitation"]["ground_evidence"]
    assert validated_ground["observed_at_min"] == 995.0
    assert validated_ground["observed_at_max"] == 1_000.0
    assert validated_ground["received_at"] == 1_002.0
    assert validated_ground["physical_site_count"] == 3
    assert validated_ground["wet_site_count"] == 2
    assert validated_ground["same_track"] is True
    assert validated_ground["source_ids"] == ["rainviewer"]

    stale_claim = copy.deepcopy(snapshot)
    stale_claim["targets"]["items"][0]["current_precipitation"][
        "ground_evidence"
    ]["freshness"] = "stale"
    with pytest.raises(contract.SnapshotValidationError, match="confirmation"):
        contract.validate_snapshot(stale_claim)

    stale_evidence = copy.deepcopy(stale_claim)
    stale_ground = stale_evidence["targets"]["items"][0][
        "current_precipitation"
    ]["ground_evidence"]
    stale_ground["confirmation"] = None
    stale_ground["same_track"] = None
    contract.validate_snapshot(stale_evidence)


def test_ui_storm_quality_is_diagnostic_and_never_an_untyped_probability():
    snapshot = _snapshot()
    storm = snapshot["ui_storms"]["items"][0]
    assert storm["diagnostic_quality_kind"] == "motion_fit"
    assert "confidence_percent" not in storm

    untyped = copy.deepcopy(snapshot)
    untyped["ui_storms"]["items"][0]["diagnostic_quality_kind"] = None
    with pytest.raises(contract.SnapshotValidationError, match="quality en kind"):
        contract.validate_snapshot(untyped)


def test_target_ids_attach_directly_to_desired_state_without_copying_config():
    snapshot = _snapshot()
    target_id = "@device_tracker/wim@example"
    snapshot["targets"]["items"][0]["target_id"] = target_id
    snapshot["regions"]["items"][0]["target_ids"] = [target_id]

    validated = contract.validate_snapshot(snapshot)
    assert validated["targets"]["items"][0]["target_id"] == target_id
    assert not {
        "kind", "latitude", "longitude", "observation_radius_km", "settings"
    }.intersection(validated["targets"]["items"][0])


def test_map_contract_accepts_only_content_addressed_handle_and_etag():
    mutable = _snapshot()
    mutable["map_artifact"]["artifact_handle"] = "latest-map"
    with pytest.raises(contract.SnapshotValidationError, match="content-addressed"):
        contract.validate_snapshot(mutable)

    inline = _snapshot()
    inline["map_artifact"]["geojson"] = {"type": "FeatureCollection"}
    with pytest.raises(contract.SnapshotValidationError):
        contract.validate_snapshot(inline)


def test_collection_ids_must_be_sorted_unique_and_cross_references_exact():
    duplicate = _snapshot()
    duplicate["targets"]["items"].append(copy.deepcopy(_target()))
    with pytest.raises(contract.SnapshotValidationError, match="uniek"):
        contract.validate_snapshot(duplicate)

    unknown_provider = _snapshot()
    unknown_provider["targets"]["items"][0]["current_precipitation"][
        "provider_ids"
    ] = ["missing"]
    with pytest.raises(contract.SnapshotValidationError, match="onbekende provider"):
        contract.validate_snapshot(unknown_provider)


def test_nonfinite_numbers_and_noncanonical_unicode_fail_closed():
    nan_snapshot = _snapshot()
    nan_snapshot["targets"]["items"][0]["next_threat"][
        "distance_km"
    ] = float("nan")
    with pytest.raises(contract.SnapshotValidationError, match="eindig"):
        contract.validate_snapshot(nan_snapshot)

    unicode_snapshot = _snapshot()
    unicode_snapshot["targets"]["items"][0]["display_name"] = "Cafe\u0301"
    with pytest.raises(contract.SnapshotValidationError, match="NFC"):
        contract.validate_snapshot(unicode_snapshot)

    with pytest.raises(contract.SnapshotValidationError, match="UTF-8"):
        contract.decode_control_envelope("\ud800")

    deeply_nested = b"[" * 1_200 + b"]" * 1_200
    with pytest.raises(contract.SnapshotValidationError):
        contract.decode_control_envelope(deeply_nested)


def test_cache_cas_duplicate_and_revision_conflicts_are_atomic():
    cache = contract.OperationalSnapshotCache()
    snapshot = _snapshot(1)
    encoded = contract.encode_control_envelope(snapshot)

    first = cache.apply_bytes(encoded, expected_revision=0)
    assert first.applied is True
    assert cache.revision == 1

    duplicate = cache.apply_bytes(encoded, expected_revision=1)
    assert duplicate.duplicate is True

    equivocal = _snapshot(1)
    equivocal["evaluated_at"] = 1_005.0
    with pytest.raises(contract.SnapshotRevisionConflict, match="andere"):
        cache.apply_snapshot(equivocal, expected_revision=1)
    assert cache.revision == 1

    second = _snapshot(2)
    second["evaluated_at"] = 1_006.0
    assert cache.apply_snapshot(second, expected_revision=1).applied is True
    assert cache.revision == 2

    with pytest.raises(contract.SnapshotRevisionConflict, match="oudere"):
        cache.apply_bytes(encoded, expected_revision=2)
    with pytest.raises(contract.SnapshotCompareAndSwapError):
        cache.apply_snapshot(_snapshot(3), expected_revision=1)
    assert cache.revision == 2


def test_cache_is_detached_and_lookup_returns_only_small_copies():
    cache = contract.OperationalSnapshotCache()
    snapshot = _snapshot()
    cache.apply_snapshot(snapshot)
    snapshot["targets"]["items"][0]["status_text"] = "mutated upstream"

    first = cache.get_target("home")
    assert first["status_text"] == (
        "Droog nu; gekalibreerde volgende dreiging over 20 min"
    )
    first["status_text"] = "mutated consumer"
    first["current_precipitation"]["provider_ids"].append("evil")

    second = cache.get_target("home")
    assert second["status_text"] == (
        "Droog nu; gekalibreerde volgende dreiging over 20 min"
    )
    assert second["current_precipitation"]["provider_ids"] == ["rainviewer"]
    assert cache.get_target("missing") is None
    assert cache.get_provider("rainviewer")["state"] == "operational"
    assert cache.get_region("region-home")["target_ids"] == ["home"]
    assert cache.get_ui_storm("storm-1")["state"] == "active"
    assert cache.get_map_artifact() == _snapshot()["map_artifact"]


def test_mark_worker_offline_atomically_removes_every_weather_claim():
    cache = contract.OperationalSnapshotCache()
    cache.apply_snapshot(_snapshot())
    original_revision = cache.revision
    original_digest = cache.snapshot_sha256
    original_map = cache.get_map_artifact()

    assert cache.mark_worker_offline() is True
    assert cache.mark_worker_offline() is False
    target = cache.get_target("home")
    current = target["current_precipitation"]
    threat = target["next_threat"]
    ground = current["ground_evidence"]
    assert current["state"] == "unknown"
    assert current["freshness"] == "stale"
    assert current["rain_rate_mm_h"] is None
    assert ground["confirmation"] is None
    assert ground["same_track"] is None
    assert threat["state"] == "unknown"
    assert threat["track_id"] is None
    assert threat["calibrated_probability_percent"] is None
    assert threat["eta_minutes"] is None
    assert "worker_offline" in current["reason_codes"]
    assert "worker_offline" in ground["reason_codes"]
    assert "worker_offline" in threat["reason_codes"]
    provider = cache.get_provider("rainviewer")
    assert provider["state"] == "offline"
    assert provider["freshness"] == "stale"
    assert cache.get_status()["provenance_complete"] is False
    assert cache.get_status()["worker_state"] == "offline"
    assert cache.get_map_artifact() == original_map
    assert cache.revision == original_revision
    assert cache.snapshot_sha256 == original_digest


@pytest.mark.parametrize(
    "status",
    ["candidate", "confirmed", "coasting", "current", "ended", "cancelled", "indeterminate"],
)
def test_scientific_passage_statuses_survive_snapshot_validation(status):
    snapshot = _snapshot()
    snapshot["targets"]["items"][0]["next_threat"]["passage_status"] = status
    assert contract.validate_snapshot(snapshot)["targets"]["items"][0][
        "next_threat"
    ]["passage_status"] == status


def test_monotonic_expiry_compare_and_swap_cannot_touch_another_cache_head():
    cache = contract.OperationalSnapshotCache()
    cache.apply_snapshot(_snapshot(1))
    first_digest = cache.snapshot_sha256
    first_deadline = cache.valid_until

    assert not cache.mark_expired_head(
        snapshot_sha256="f" * 64,
        valid_until=first_deadline,
    )
    assert not cache.mark_expired_head(
        snapshot_sha256=first_digest,
        valid_until=first_deadline + 1,
    )
    assert cache.availability == "current"

    second = _snapshot(2)
    second["evaluated_at"] = 1_006.0
    second["valid_until"] = 1_306.0
    cache.apply_snapshot(second)
    assert not cache.mark_expired_head(
        snapshot_sha256=first_digest,
        valid_until=first_deadline,
    )
    assert cache.revision == 2
    assert cache.availability == "current"

    assert cache.mark_expired_head(
        snapshot_sha256=cache.snapshot_sha256,
        valid_until=cache.valid_until,
    )
    assert cache.availability == "stale"
    assert cache.get_target("home")["current_precipitation"]["state"] == (
        "unknown"
    )
    # A duplicate callback and an old deadline are both idempotent.
    assert not cache.mark_expired_head(
        snapshot_sha256=cache.snapshot_sha256,
        valid_until=cache.valid_until,
    )


def test_offline_reason_survives_a_full_bounded_reason_list():
    snapshot = _snapshot()
    snapshot["targets"]["items"][0]["current_precipitation"][
        "reason_codes"
    ] = [
        f"a{index:02d}" for index in range(contract.MAX_REASON_CODES)
    ]
    cache = contract.OperationalSnapshotCache()
    cache.apply_snapshot(snapshot)
    cache.mark_worker_offline()

    reasons = cache.get_target("home")["current_precipitation"]["reason_codes"]
    assert len(reasons) == contract.MAX_REASON_CODES
    assert "worker_offline" in reasons
    assert reasons == sorted(reasons)


def test_recommended_size_is_reported_without_becoming_a_truth_filter():
    selected = None
    for count in range(1, contract.MAX_TARGETS + 1):
        candidate = _many_targets_snapshot(count, verbose=True)
        try:
            encoded = contract.encode_control_envelope(candidate)
        except contract.SnapshotSizeError:
            break
        if len(encoded) > contract.RECOMMENDED_CONTROL_ENVELOPE_BYTES:
            selected = encoded
            break
    assert selected is not None
    assert len(selected) <= contract.MAX_CONTROL_ENVELOPE_BYTES

    result = contract.OperationalSnapshotCache().apply_bytes(selected)
    assert result.applied is True
    assert result.exceeds_recommended_bytes is True


def test_cache_property_reads_have_no_io_sqlite_or_linear_scan_calls():
    tree = ast.parse(HA_MODULE.read_text(encoding="utf-8"))
    imported_roots = {
        alias.name.split(".")[0]
        for node in ast.walk(tree)
        if isinstance(node, (ast.Import, ast.ImportFrom))
        for alias in node.names
    }
    assert imported_roots.isdisjoint({
        "aiohttp", "base64", "gzip", "homeassistant", "pathlib", "sqlite3"
    })

    for method_name in ("get_target", "get_provider"):
        source = inspect.getsource(getattr(contract.OperationalSnapshotCache, method_name))
        method_tree = ast.parse(textwrap.dedent(source))
        assert not any(
            isinstance(node, (ast.For, ast.While, ast.ListComp, ast.SetComp, ast.DictComp))
            for node in ast.walk(method_tree)
        )
        assert not any(
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id in {"open", "sorted", "sum"}
            for node in ast.walk(method_tree)
        )
