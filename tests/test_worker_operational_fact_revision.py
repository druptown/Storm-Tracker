from __future__ import annotations

from dataclasses import replace

import pytest

from storm_tracker_worker.desired_state import parse_desired_state
from storm_tracker_worker import operational_fact_revision as admission
from storm_tracker_worker.operational_snapshot_builder import (
    build_unknown_operational_snapshot,
)


NOW = 1_800_000_000.0
SOURCE = "1" * 64
CERTIFICATE = "2" * 64
PROCESS = "3" * 64


def _desired(*, available: bool = True, generation: int = 1):
    return parse_desired_state({
        "authority_epoch": 7,
        "contract": "stv3-worker-desired-state-v1",
        "generation": generation,
        "issued_at": NOW - 60.0,
        "previous_state_sha256": None,
        "producer_id": "ha-operational-fact-test",
        "settings": {
            "ground_radius_km": 20.0,
            "lightning_source_mode": "auto",
            "model_guidance_enabled": True,
            "region_sharing_distance_km": 100.0,
        },
        "targets": [{
            "available": available,
            "country_code": "BE",
            "display_name": "Thuis",
            "kind": "home",
            "latitude": 51.04,
            "location_accuracy_m": 10.0 if available else None,
            "longitude": 4.48,
            "observation_radius_km": 250.0,
            "position_observed_at": NOW - 30.0 if available else None,
            "target_id": "zone.home",
        }],
    })


def _unknown_ground():
    return {
        "confirmation": None,
        "evidence_valid_at": None,
        "freshness": "unknown",
        "observed_at_max": None,
        "observed_at_min": None,
        "physical_site_count": 0,
        "reason_codes": ["ground_not_evaluated"],
        "received_at": None,
        "same_track": None,
        "source_ids": [],
        "status": "unknown",
        "wet_site_count": 0,
    }


def _unknown_threat():
    return {
        "calibrated_probability_percent": None,
        "distance_km": None,
        "eta_minutes": None,
        "freshness": "unknown",
        "passage_status": "unknown",
        "probability_calibration_id": None,
        "product_valid_at": None,
        "provider_ids": [],
        "reason_codes": ["forecast_not_admitted"],
        "state": "unknown",
        "track_id": None,
    }


def _raining_snapshot(desired=None):
    desired = desired or _desired()
    return {
        "contract": "operational_snapshot_v1",
        "desired_state_sha256": desired.state_sha256,
        "evaluated_at": NOW,
        "map_artifact": {"artifact_handle": None, "etag": None},
        "product_valid_at": NOW - 60.0,
        "providers": {
            "complete": True,
            "items": [{
                "display_name": "DWD RADOLAN RV",
                "error": None,
                "freshness": "current",
                "product_valid_at": NOW - 60.0,
                "provider_id": "dwd",
                "reason_codes": ["quantitative_observation_admitted"],
                "received_at": NOW - 30.0,
                "state": "operational",
            }],
            "overflow_count": 0,
        },
        "provenance_complete": False,
        "provenance_reason_codes": ["forecast_not_admitted"],
        "provenance_source_ids": ["dwd"],
        "received_at": NOW - 30.0,
        "regions": {
            "complete": True,
            "items": [{
                "display_name": "Regio thuis",
                "provider_ids": ["dwd"],
                "region_id": "region:home",
                "state": "current",
                "target_ids": ["zone.home"],
            }],
            "overflow_count": 0,
        },
        "revision": 1,
        "targets": {
            "complete": True,
            "items": [{
                "current_precipitation": {
                    "freshness": "current",
                    "ground_evidence": _unknown_ground(),
                    "product_valid_at": NOW - 60.0,
                    "provider_ids": ["dwd"],
                    "rain_rate_mm_h": 1.25,
                    "reason_codes": ["quantitative_target_pixel_positive"],
                    "state": "raining",
                },
                "display_name": "Thuis",
                "next_threat": _unknown_threat(),
                "region_id": "region:home",
                "status_text": "Het regent nu",
                "target_id": "zone.home",
            }],
            "overflow_count": 0,
        },
        "ui_storms": {"complete": True, "items": [], "overflow_count": 0},
        "valid_until": NOW + 120.0,
        "worker": {
            "failed_jobs": 0,
            "queue_depth": 0,
            "state": "online",
            "status_text": "",
        },
    }


def _claim_values(snapshot):
    values = {}
    if snapshot["provenance_complete"]:
        values[(
            admission.OperationalFactKind.REVISION_COMPLETE,
            f"snapshot:{snapshot['revision']}",
        )] = admission._meteorological_projection(snapshot)
    for item in snapshot["regions"]["items"]:
        if item["state"] in {"current", "degraded"}:
            values[(admission.OperationalFactKind.REGION_CURRENT, item["region_id"])] = item
    for item in snapshot["providers"]["items"]:
        if item["freshness"] == "current":
            values[(admission.OperationalFactKind.PROVIDER_CURRENT, item["provider_id"])] = item
    for target in snapshot["targets"]["items"]:
        target_id = target["target_id"]
        current = target["current_precipitation"]
        if current["state"] == "raining":
            values[(admission.OperationalFactKind.CURRENT_RAINING, target_id)] = current
        if current["state"] == "dry":
            values[(admission.OperationalFactKind.CURRENT_DRY, target_id)] = current
        ground = current["ground_evidence"]
        if ground["status"] != "unknown":
            values[(admission.OperationalFactKind.GROUND_STATE, target_id)] = ground
        if ground["confirmation"] is not None:
            values[(admission.OperationalFactKind.GROUND_CONFIRMATION, target_id)] = ground
        threat = target["next_threat"]
        if threat["state"] == "available":
            values[(admission.OperationalFactKind.NEXT_THREAT_AVAILABLE, target_id)] = threat
        if threat["state"] == "none":
            values[(admission.OperationalFactKind.NEXT_THREAT_NONE, target_id)] = threat
        if threat["calibrated_probability_percent"] is not None:
            values[(admission.OperationalFactKind.CALIBRATED_PROBABILITY, target_id)] = threat
        if threat["passage_status"] == "expected":
            values[(admission.OperationalFactKind.PASSAGE_EXPECTED, target_id)] = threat
    for item in snapshot["ui_storms"]["items"]:
        if item["state"] != "unknown":
            values[(admission.OperationalFactKind.UI_STORM, item["storm_id"])] = item
    if snapshot["map_artifact"]["artifact_handle"] is not None:
        values[(
            admission.OperationalFactKind.MAP_ARTIFACT,
            f"snapshot:{snapshot['revision']}",
        )] = snapshot["map_artifact"]
    return values


def _receipts(snapshot, desired, *, omit=(), disposition=None):
    omitted = set(omit)
    result = []
    for (kind, scope), value in _claim_values(snapshot).items():
        if kind in omitted:
            continue
        external = kind in admission._EXTERNAL_TRUTH_KINDS
        full_scan = kind in admission._FULL_SCAN_KINDS
        selected_disposition = (
            admission.AdmissionDisposition.ADMITTED
            if disposition is None
            else disposition
        )
        result.append(admission.FactAdmissionReceipt.create(
            fact_kind=kind,
            scope_id=scope,
            fact_value=value,
            desired_state_sha256=desired.state_sha256,
            authority_epoch=desired.authority_epoch,
            source_receipt_sha256s=(SOURCE,),
            certificate_sha256s=(CERTIFICATE,),
            process_receipt_sha256=PROCESS,
            full_scan_complete=full_scan,
            external_truth_validated=external,
            decided_at=NOW - 1.0,
            valid_until=NOW + 120.0,
            disposition=selected_disposition,
            reason_codes=(
                "all_required_evidence_verified"
                if selected_disposition is admission.AdmissionDisposition.ADMITTED
                else "independent_evidence_missing",
            ),
        ))
    return result


def _revision(snapshot, desired, receipts=None):
    return admission.CommittedOperationalFactRevision.create(
        desired_state_sha256=desired.state_sha256,
        authority_epoch=desired.authority_epoch,
        snapshot=snapshot,
        admission_receipts=(
            _receipts(snapshot, desired) if receipts is None else receipts
        ),
    )


def test_exact_fact_receipts_allow_current_rain_without_admitting_forecast():
    desired = _desired()
    snapshot = _raining_snapshot(desired)
    revision = _revision(snapshot, desired)

    result = admission.validate_committed_operational_revision(desired, revision)

    assert result["targets"]["items"][0]["current_precipitation"]["state"] == "raining"
    assert result["targets"]["items"][0]["next_threat"]["state"] == "unknown"
    assert {
        item.fact_kind for item in revision.admission_receipts
    } == {
        admission.OperationalFactKind.CURRENT_RAINING,
        admission.OperationalFactKind.PROVIDER_CURRENT,
        admission.OperationalFactKind.REGION_CURRENT,
    }


@pytest.mark.parametrize(
    "missing",
    (
        admission.OperationalFactKind.CURRENT_RAINING,
        admission.OperationalFactKind.PROVIDER_CURRENT,
        admission.OperationalFactKind.REGION_CURRENT,
    ),
)
def test_every_material_claim_requires_its_own_receipt(missing):
    desired = _desired()
    snapshot = _raining_snapshot(desired)
    revision = _revision(
        snapshot,
        desired,
        _receipts(snapshot, desired, omit=(missing,)),
    )
    with pytest.raises(admission.OperationalFactAdmissionError, match="dekking"):
        admission.validate_committed_operational_revision(desired, revision)


def test_value_change_after_admission_is_not_covered_by_old_receipt():
    desired = _desired()
    original = _raining_snapshot(desired)
    receipts = _receipts(original, desired)
    changed = _raining_snapshot(desired)
    changed["targets"]["items"][0]["current_precipitation"]["rain_rate_mm_h"] = 2.0
    revision = _revision(changed, desired, receipts)
    with pytest.raises(admission.OperationalFactAdmissionError, match="dekking"):
        admission.validate_committed_operational_revision(desired, revision)


def test_withheld_expired_future_and_extra_receipts_fail_closed():
    desired = _desired()
    snapshot = _raining_snapshot(desired)
    withheld = _receipts(
        snapshot,
        desired,
        disposition=admission.AdmissionDisposition.WITHHELD,
    )
    with pytest.raises(admission.OperationalFactAdmissionError, match="withheld"):
        admission.validate_committed_operational_revision(
            desired,
            _revision(snapshot, desired, withheld),
        )

    first = _receipts(snapshot, desired)[0]
    expired = admission.FactAdmissionReceipt.create(
        fact_kind=first.fact_kind,
        scope_id=first.scope_id,
        fact_value=_claim_values(snapshot)[(first.fact_kind, first.scope_id)],
        desired_state_sha256=desired.state_sha256,
        authority_epoch=desired.authority_epoch,
        source_receipt_sha256s=first.source_receipt_sha256s,
        certificate_sha256s=first.certificate_sha256s,
        process_receipt_sha256=first.process_receipt_sha256,
        full_scan_complete=first.full_scan_complete,
        external_truth_validated=first.external_truth_validated,
        decided_at=NOW - 120.0,
        valid_until=NOW + 60.0,
        disposition=first.disposition,
        reason_codes=first.reason_codes,
    )
    receipts = [expired, *_receipts(snapshot, desired)[1:]]
    with pytest.raises(admission.OperationalFactAdmissionError, match="verloopt"):
        admission.validate_committed_operational_revision(
            desired,
            _revision(snapshot, desired, receipts),
        )

    extra = admission.FactAdmissionReceipt.create(
        fact_kind=admission.OperationalFactKind.MAP_ARTIFACT,
        scope_id="snapshot:1",
        fact_value={"artifact_handle": None, "etag": None},
        desired_state_sha256=desired.state_sha256,
        authority_epoch=desired.authority_epoch,
        source_receipt_sha256s=(SOURCE,),
        certificate_sha256s=(CERTIFICATE,),
        process_receipt_sha256=PROCESS,
        full_scan_complete=False,
        external_truth_validated=False,
        decided_at=NOW - 1.0,
        valid_until=NOW + 120.0,
        disposition=admission.AdmissionDisposition.ADMITTED,
        reason_codes=("all_required_evidence_verified",),
    )
    with pytest.raises(admission.OperationalFactAdmissionError, match="extra=1"):
        admission.validate_committed_operational_revision(
            desired,
            _revision(snapshot, desired, [*_receipts(snapshot, desired), extra]),
        )


def test_dry_and_none_require_complete_scan_and_independent_validation():
    desired = _desired()
    snapshot = _raining_snapshot(desired)
    current = snapshot["targets"]["items"][0]["current_precipitation"]
    current.update({
        "rain_rate_mm_h": 0.0,
        "reason_codes": ["complete_coverage_no_precipitation"],
        "state": "dry",
    })
    snapshot["targets"]["items"][0]["next_threat"] = {
        **_unknown_threat(),
        "freshness": "current",
        "passage_status": "none",
        "product_valid_at": NOW - 60.0,
        "provider_ids": ["dwd"],
        "reason_codes": ["complete_candidate_scan_no_threat"],
        "state": "none",
    }
    snapshot["provenance_complete"] = True
    snapshot["provenance_reason_codes"] = ["complete_admitted_revision"]

    revision = _revision(snapshot, desired)
    result = admission.validate_committed_operational_revision(desired, revision)
    kinds = {item.fact_kind for item in revision.admission_receipts}
    assert result["targets"]["items"][0]["current_precipitation"]["state"] == "dry"
    assert admission.OperationalFactKind.REVISION_COMPLETE in kinds
    assert admission.OperationalFactKind.CURRENT_DRY in kinds
    assert admission.OperationalFactKind.NEXT_THREAT_NONE in kinds

    with pytest.raises(admission.OperationalFactAdmissionError, match="volledige scan"):
        admission.FactAdmissionReceipt.create(
            fact_kind=admission.OperationalFactKind.CURRENT_DRY,
            scope_id="zone.home",
            fact_value=current,
            desired_state_sha256=desired.state_sha256,
            authority_epoch=desired.authority_epoch,
            source_receipt_sha256s=(SOURCE,),
            certificate_sha256s=(CERTIFICATE,),
            process_receipt_sha256=PROCESS,
            full_scan_complete=False,
            external_truth_validated=True,
            decided_at=NOW - 1.0,
            valid_until=NOW + 120.0,
            disposition=admission.AdmissionDisposition.ADMITTED,
            reason_codes=("all_required_evidence_verified",),
        )
    with pytest.raises(admission.OperationalFactAdmissionError, match="onafhankelijk"):
        admission.FactAdmissionReceipt.create(
            fact_kind=admission.OperationalFactKind.NEXT_THREAT_NONE,
            scope_id="zone.home",
            fact_value=snapshot["targets"]["items"][0]["next_threat"],
            desired_state_sha256=desired.state_sha256,
            authority_epoch=desired.authority_epoch,
            source_receipt_sha256s=(SOURCE,),
            certificate_sha256s=(CERTIFICATE,),
            process_receipt_sha256=PROCESS,
            full_scan_complete=True,
            external_truth_validated=False,
            decided_at=NOW - 1.0,
            valid_until=NOW + 120.0,
            disposition=admission.AdmissionDisposition.ADMITTED,
            reason_codes=("all_required_evidence_verified",),
        )


def test_probability_and_expected_eta_need_three_separate_external_gates():
    desired = _desired()
    snapshot = _raining_snapshot(desired)
    snapshot["targets"]["items"][0]["next_threat"] = {
        "calibrated_probability_percent": 70.0,
        "distance_km": 12.0,
        "eta_minutes": 18.0,
        "freshness": "current",
        "passage_status": "expected",
        "probability_calibration_id": "empirical:event-held-out-v1",
        "product_valid_at": NOW - 60.0,
        "provider_ids": ["dwd"],
        "reason_codes": ["held_out_calibration_admitted"],
        "state": "available",
        "track_id": "track:42",
    }
    revision = _revision(snapshot, desired)
    kinds = {item.fact_kind for item in revision.admission_receipts}
    assert {
        admission.OperationalFactKind.NEXT_THREAT_AVAILABLE,
        admission.OperationalFactKind.CALIBRATED_PROBABILITY,
        admission.OperationalFactKind.PASSAGE_EXPECTED,
    } <= kinds
    admission.validate_committed_operational_revision(desired, revision)

    receipts = _receipts(
        snapshot,
        desired,
        omit=(admission.OperationalFactKind.CALIBRATED_PROBABILITY,),
    )
    with pytest.raises(admission.OperationalFactAdmissionError, match="ontbreekt=1"):
        admission.validate_committed_operational_revision(
            desired,
            _revision(snapshot, desired, receipts),
        )


def test_wet_same_track_confirmation_is_separate_from_ground_state():
    desired = _desired()
    snapshot = _raining_snapshot(desired)
    snapshot["targets"]["items"][0]["current_precipitation"]["ground_evidence"] = {
        "confirmation": True,
        "evidence_valid_at": NOW - 20.0,
        "freshness": "current",
        "observed_at_max": NOW - 20.0,
        "observed_at_min": NOW - 25.0,
        "physical_site_count": 1,
        "reason_codes": ["same_track_wet_site"],
        "received_at": NOW - 10.0,
        "same_track": True,
        "source_ids": ["metar"],
        "status": "wet",
        "wet_site_count": 1,
    }
    snapshot["providers"]["items"].append({
        "display_name": "METAR",
        "error": None,
        "freshness": "current",
        "product_valid_at": NOW - 20.0,
        "provider_id": "metar",
        "reason_codes": ["ground_semantics_admitted"],
        "received_at": NOW - 10.0,
        "state": "operational",
    })
    snapshot["providers"]["items"].sort(key=lambda item: item["provider_id"])
    revision = _revision(snapshot, desired)
    kinds = {item.fact_kind for item in revision.admission_receipts}
    assert admission.OperationalFactKind.GROUND_STATE in kinds
    assert admission.OperationalFactKind.GROUND_CONFIRMATION in kinds
    admission.validate_committed_operational_revision(desired, revision)


def test_unknown_snapshot_needs_no_meteorological_admission_receipt():
    desired = _desired()
    snapshot = build_unknown_operational_snapshot(
        desired,
        revision=1,
        evaluated_at=NOW,
        worker_state="online",
        queue_depth=0,
        failed_jobs=0,
    )
    revision = _revision(snapshot, desired, [])
    result = admission.validate_committed_operational_revision(desired, revision)
    assert result["targets"]["items"][0]["current_precipitation"]["state"] == "unknown"


def test_desired_authority_target_name_and_availability_are_checked_again():
    desired = _desired()
    snapshot = _raining_snapshot(desired)
    revision = _revision(snapshot, desired)
    changed_desired = _desired(generation=2)
    with pytest.raises(admission.OperationalFactAdmissionError, match="authority"):
        admission.validate_committed_operational_revision(changed_desired, revision)

    renamed = _raining_snapshot(desired)
    renamed["targets"]["items"][0]["display_name"] = "Andere naam"
    renamed_revision = _revision(renamed, desired)
    with pytest.raises(admission.OperationalFactAdmissionError, match="targetnaam"):
        admission.validate_committed_operational_revision(desired, renamed_revision)

    unavailable = _desired(available=False)
    unavailable_snapshot = _raining_snapshot(unavailable)
    unavailable_revision = _revision(unavailable_snapshot, unavailable)
    with pytest.raises(admission.OperationalFactAdmissionError, match="onbeschikbaar"):
        admission.validate_committed_operational_revision(
            unavailable,
            unavailable_revision,
        )


def test_receipt_and_revision_checksums_detect_tamper():
    desired = _desired()
    snapshot = _raining_snapshot(desired)
    receipt = _receipts(snapshot, desired)[0]
    with pytest.raises(admission.OperationalFactAdmissionError, match="receipthash"):
        replace(receipt, receipt_sha256="f" * 64)

    revision = _revision(snapshot, desired)
    with pytest.raises(admission.OperationalFactAdmissionError, match="revision hash"):
        replace(revision, revision_sha256="e" * 64)


def test_mapping_roundtrip_is_exact_and_schema_tamper_fails_closed():
    desired = _desired()
    revision = _revision(_raining_snapshot(desired), desired)
    restored = admission.CommittedOperationalFactRevision.from_mapping(
        revision.to_mapping()
    )
    assert restored == revision
    assert admission.validate_committed_operational_revision(
        desired,
        restored,
    ) == revision.snapshot

    changed = revision.to_mapping()
    changed["unexpected"] = True
    with pytest.raises(admission.OperationalFactAdmissionError, match="schema"):
        admission.CommittedOperationalFactRevision.from_mapping(changed)

    changed = revision.to_mapping()
    changed["admission_receipts"][0]["fact_kind"] = "invented"
    with pytest.raises(admission.OperationalFactAdmissionError, match="enum"):
        admission.CommittedOperationalFactRevision.from_mapping(changed)


def test_nested_snapshot_mutation_after_construction_is_detected_at_use():
    desired = _desired()
    snapshot = _raining_snapshot(desired)
    revision = _revision(snapshot, desired)
    revision.snapshot["targets"]["items"][0]["status_text"] = "Gewijzigd"
    with pytest.raises(admission.OperationalFactAdmissionError, match="na constructie"):
        admission.validate_committed_operational_revision(desired, revision)
