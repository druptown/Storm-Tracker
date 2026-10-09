from __future__ import annotations

import hashlib

import pytest

from storm_tracker_worker.desired_state import parse_desired_state
from storm_tracker_worker.multi_source_operational_cycle import (
    CATALOG_SOURCES,
    CycleStatus,
    CycleTarget,
    EvidenceReference,
    GroundInput,
    LightningInput,
    ModelInput,
    MultiSourceOperationalCycle,
    RadarInput,
    SatelliteInput,
    SourceContribution,
    SourceKind,
    ValidationInput,
    empty_source_input,
    evaluate_target_applicability,
)
from storm_tracker_worker.multi_source_snapshot import (
    MIN_PUBLICATION_LEAD_MS,
    MultiSourceSnapshotError,
    REASON_FRESHNESS_NOT_PUBLICATION_SAFE,
    build_multi_source_status_fact_revision,
    build_multi_source_status_snapshot,
)
from storm_tracker_worker.multi_source_radar_snapshot import (
    MultiSourceRadarSnapshotError,
    PositiveRadarTargetObservation,
    REFLECTIVITY_ECHO_REASON,
    ReflectivityEchoTargetObservation,
    WithheldDryRadarTargetObservation,
    WithheldUndetectRadarTargetObservation,
    UNDETECT_OBSERVATION_REASON,
    ZERO_OBSERVATION_REASON,
    build_positive_radar_fact_revision,
)
from storm_tracker_worker.operational_fact_revision import (
    OperationalFactKind,
    validate_committed_operational_revision,
)
from storm_tracker_worker.multi_source_nowcast_snapshot import (
    IndependentGroundValidation,
    MultiSourceNowcastSnapshotError,
    TargetNowcastThreat,
    build_ground_validated_nowcast_revision,
    threats_from_deterministic_nowcast,
)
from storm_tracker_worker.multi_source_cycle_assembler import (
    CapturedSourceEvidence,
    build_raw_capture_operational_cycle,
)
from storm_tracker_worker.operational_snapshot_builder import PENDING_REGION_ID
from storm_tracker_worker.worker_acquisition_plan import build_worker_acquisition_plan
from storm_tracker_worker.worker_only_cell_tracking import (
    ADMISSION_STATUS,
    SHADOW_ARRIVAL_HORIZON_SECONDS,
    WorkerOnlyShadowArrivalCandidate,
)
from storm_tracker_worker.worker_only_motion_snapshot import (
    MOTION_THREAT_REASON,
    build_radar_motion_nowcast_revision,
)
from storm_tracker_worker.openmeteo_operational_guidance import (
    OpenMeteoTargetGuidance,
    SOURCE_ID as OPENMETEO_SOURCE_ID,
    THREAT_REASON as OPENMETEO_THREAT_REASON,
    enrich_revision_with_openmeteo_guidance,
)


NOW_MS = 1_800_200_000_000


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode("ascii")).hexdigest()


def _desired():
    return parse_desired_state({
        "contract": "stv3-worker-desired-state-v1",
        "producer_id": "ha:test",
        "authority_epoch": 7,
        "generation": 1,
        "issued_at": NOW_MS / 1000,
        "previous_state_sha256": None,
        "targets": [{
            "target_id": "home",
            "display_name": "Thuis",
            "kind": "home",
            "available": True,
            "latitude": 50.8,
            "longitude": 4.2,
            "position_observed_at": NOW_MS / 1000,
            "location_accuracy_m": 10.0,
            "observation_radius_km": 250.0,
            "country_code": "BE",
        }],
        "settings": {
            "ground_radius_km": 20.0,
            "lightning_source_mode": "auto",
            "model_guidance_enabled": True,
            "region_sharing_distance_km": 100.0,
        },
    })


def _desired_two_regions():
    raw = _desired().as_dict()
    raw["targets"].append({
        "target_id": "test.frankfurt",
        "display_name": "Fictieve tracker",
        "kind": "test",
        "available": True,
        "latitude": 50.1109,
        "longitude": 8.6821,
        "position_observed_at": NOW_MS / 1000,
        "location_accuracy_m": 10.0,
        "observation_radius_km": 250.0,
        "country_code": "DE",
    })
    return parse_desired_state(raw)


def _desired_with_unavailable_target():
    raw = _desired().as_dict()
    raw["targets"].append({
        "target_id": "aaa.stale_tracker",
        "display_name": "Tijdelijk onbeschikbaar",
        "kind": "device_tracker",
        "available": False,
        "latitude": 50.8,
        "longitude": 4.2,
        "position_observed_at": None,
        "location_accuracy_m": None,
        "observation_radius_km": 250.0,
        "country_code": "BE",
    })
    return parse_desired_state(raw)


def _opera_echo() -> ReflectivityEchoTargetObservation:
    return ReflectivityEchoTargetObservation(
        source_id="radar.opera.dbzh",
        target_id="home",
        reflectivity_dbz=27.5,
        quality_index=0.8,
        product_valid_at=NOW_MS / 1000,
        received_at=NOW_MS / 1000,
        source_receipt_sha256=_sha("opera-source"),
        certificate_sha256=_sha("opera-certificate"),
        process_receipt_sha256=_sha("opera-process"),
    )


def _evidence(source_id: str):
    return (EvidenceReference(
        role="canonical_input",
        content_sha256=_sha(source_id + ":content"),
        receipt_sha256=_sha(source_id + ":receipt"),
        size_bytes=100,
        schema_id="stv3-test-source-v1",
    ),)


def _payload(source):
    evidence = _evidence(source.source_id)
    coverage = source.coverage_bbox or (-90.0, -180.0, 90.0, 180.0)
    if source.kind is SourceKind.RADAR:
        return RadarInput(NOW_MS, coverage, "precipitation", "mm_h", 1, 1, evidence)
    if source.kind is SourceKind.SATELLITE:
        return SatelliteInput(
            NOW_MS, coverage, source.satellite_id or source.provider_id,
            "precipitation", "mm_h", 1, 1, evidence,
        )
    if source.kind is SourceKind.LIGHTNING:
        return LightningInput(NOW_MS - 60_000, NOW_MS, 1, 0, False, evidence)
    if source.kind is SourceKind.GROUND:
        return GroundInput(
            NOW_MS - 60_000, NOW_MS, 1, 1, ("precipitation",), evidence,
        )
    if source.kind is SourceKind.MODEL:
        return ModelInput(
            NOW_MS - 60_000, NOW_MS, NOW_MS + 3_600_000,
            1, ("precipitation",), evidence,
        )
    return ValidationInput(NOW_MS, 1, source.source_role, evidence)


def _cycle(desired, *, freshness_ms: int = 60_000):
    targets = (CycleTarget.create(
        target_id="home",
        latitude=50.8,
        longitude=4.2,
        country_code="BE",
    ),)
    contributions = []
    for source in CATALOG_SOURCES:
        applicable = any(
            evaluate_target_applicability(source, target).applicable
            for target in targets
        )
        if not applicable:
            status = CycleStatus.NOT_APPLICABLE
            payload = empty_source_input(source.kind)
            reason = "target_outside_catalog_scope"
            fresh_until = None
        elif source.catalog_blocked:
            status = CycleStatus.BLOCKED
            payload = empty_source_input(source.kind)
            reason = source.catalog_blocked_reason
            fresh_until = None
        else:
            status = CycleStatus.CURRENT
            payload = _payload(source)
            reason = None
            fresh_until = NOW_MS + freshness_ms
        contributions.append(SourceContribution.create(
            source_id=source.source_id,
            targets=targets,
            status=status,
            payload=payload,
            reason_code=reason,
            fresh_until_ms=fresh_until,
        ))
    return MultiSourceOperationalCycle.create(
        desired_state_sha256=desired.state_sha256,
        authority_epoch=desired.authority_epoch,
        cycle_sequence=1,
        producer_id="storm_tracker_worker",
        cycle_started_at_ms=NOW_MS - 1_000,
        cycle_completed_at_ms=NOW_MS,
        targets=targets,
        contributions=contributions,
    )


def test_snapshot_exposes_all_twenty_eight_sources_without_fake_weather():
    desired = _desired()
    snapshot = build_multi_source_status_snapshot(
        desired,
        _cycle(desired),
        revision=3,
        evaluated_at=NOW_MS / 1000,
    )
    providers = snapshot["providers"]["items"]
    assert len(providers) == 28
    assert [row["provider_id"] for row in providers] == sorted(
        source.source_id for source in CATALOG_SOURCES
    )
    assert {
        row["provider_id"]: row["state"] for row in providers
    }["radar.kmi.radqpe_native"] == "api_required"
    kmi_radqpe = next(
        row for row in providers
        if row["provider_id"] == "radar.kmi.radqpe_native"
    )
    assert kmi_radqpe["error"] is None
    assert kmi_radqpe["readiness"] == "not_ready"
    assert kmi_radqpe["reason_codes"] == ["api_required"]
    assert {
        row["provider_id"]: row["state"] for row in providers
    }["lightning.blitzortung.event_ingress"] == "offline"
    target = snapshot["targets"]["items"][0]
    assert target["current_precipitation"]["state"] == "unknown"
    assert target["next_threat"]["state"] == "unknown"


def test_live_blitzortung_readiness_overrides_the_stale_catalog_blocker():
    desired = _desired()
    snapshot = build_multi_source_status_snapshot(
        desired,
        _cycle(desired),
        revision=4,
        evaluated_at=NOW_MS / 1000,
        provider_status_overrides={
            "lightning.blitzortung.event_ingress": (
                "operational",
                "regional_qos0_raw_capture_active",
            ),
        },
    )
    blitzortung = next(
        row for row in snapshot["providers"]["items"]
        if row["provider_id"] == "lightning.blitzortung.event_ingress"
    )
    assert blitzortung["state"] == "operational"
    assert blitzortung["readiness"] == "ready"
    assert blitzortung["freshness"] == "unknown"
    assert blitzortung["error"] is None
    assert blitzortung["reason_codes"] == [
        "regional_qos0_raw_capture_active"
    ]
    committed = build_multi_source_status_fact_revision(
        desired,
        _cycle(desired),
        revision=5,
        evaluated_at=NOW_MS / 1000,
        provider_status_overrides={
            "lightning.blitzortung.event_ingress": (
                "operational",
                "regional_qos0_raw_capture_active",
            ),
        },
    )
    validate_committed_operational_revision(desired, committed)


def test_proven_readiness_survives_current_target_outside_provider_scope():
    desired = _desired()
    snapshot = build_multi_source_status_snapshot(
        desired,
        _cycle(desired),
        revision=6,
        evaluated_at=NOW_MS / 1000,
        ready_source_ids=frozenset({"radar.aemet.composite"}),
    )

    aemet = next(
        row for row in snapshot["providers"]["items"]
        if row["provider_id"] == "radar.aemet.composite"
    )
    assert aemet["readiness"] == "ready"
    assert aemet["state"] == "unknown"
    assert aemet["reason_codes"] == ["target_outside_catalog_scope"]


def test_unknown_readiness_source_is_rejected():
    desired = _desired()
    with pytest.raises(MultiSourceSnapshotError, match="onbekende bron"):
        build_multi_source_status_snapshot(
            desired,
            _cycle(desired),
            revision=6,
            evaluated_at=NOW_MS / 1000,
            ready_source_ids=frozenset({"radar.invented.invalid"}),
        )


def test_provider_status_override_is_exactly_scoped_and_fail_closed():
    desired = _desired()
    with pytest.raises(MultiSourceSnapshotError, match="onbekende bron"):
        build_multi_source_status_snapshot(
            desired,
            _cycle(desired),
            revision=4,
            evaluated_at=NOW_MS / 1000,
            provider_status_overrides={
                "radar.dwd.radolan_rv": ("operational", "forged_ready"),
            },
        )


def test_openmeteo_guidance_publishes_positive_current_and_near_term_threat():
    desired = _desired()
    base = build_multi_source_status_fact_revision(
        desired,
        _cycle(desired),
        revision=4,
        evaluated_at=NOW_MS / 1000,
    )
    guidance = OpenMeteoTargetGuidance(
        target_id="home",
        product_valid_at=NOW_MS / 1000,
        received_at=NOW_MS / 1000,
        current_precipitation_mm_15m=0.25,
        current_rate_mm_h=1.0,
        first_wet_at=NOW_MS / 1000 + 30 * 60,
        forecast_total_90m_mm=1.2,
        source_receipt_sha256=_sha("openmeteo-source"),
        certificate_sha256=_sha("openmeteo-certificate"),
        process_receipt_sha256=_sha("openmeteo-process"),
    )

    committed = enrich_revision_with_openmeteo_guidance(
        desired,
        base,
        (guidance,),
    )
    snapshot = validate_committed_operational_revision(desired, committed)
    target = snapshot["targets"]["items"][0]

    assert target["current_precipitation"]["state"] == "raining"
    assert target["current_precipitation"]["rain_rate_mm_h"] == 1.0
    assert target["current_precipitation"]["provider_ids"] == [
        OPENMETEO_SOURCE_ID
    ]
    assert target["next_threat"]["state"] == "available"
    assert target["next_threat"]["eta_minutes"] == 30.0
    assert target["next_threat"]["passage_status"] == "possible"
    assert target["next_threat"]["reason_codes"] == [
        OPENMETEO_THREAT_REASON
    ]
    kinds = {item.fact_kind for item in committed.admission_receipts}
    assert OperationalFactKind.CURRENT_RAINING in kinds
    assert OperationalFactKind.NEXT_THREAT_AVAILABLE in kinds
    assert snapshot["provenance_complete"] is False


def test_snapshot_projects_the_exact_two_region_acquisition_topology():
    desired = _desired_two_regions()
    cycle = build_raw_capture_operational_cycle(
        desired,
        (),
        cycle_sequence=2,
        cycle_started_at_ms=NOW_MS,
        cycle_completed_at_ms=NOW_MS,
    )
    snapshot = build_multi_source_status_snapshot(
        desired,
        cycle,
        revision=4,
        evaluated_at=NOW_MS / 1000,
    )
    plan = build_worker_acquisition_plan(desired)
    expected = {
        region.region_id: list(region.target_ids) for region in plan.regions
    }
    projected = {
        region["region_id"]: region["target_ids"]
        for region in snapshot["regions"]["items"]
    }

    assert len(plan.regions) == 2
    assert projected == expected
    assert PENDING_REGION_ID not in projected
    target_regions = {
        target["target_id"]: target["region_id"]
        for target in snapshot["targets"]["items"]
    }
    assert {
        target_id
        for region in plan.regions
        for target_id in region.target_ids
        if target_regions[target_id] == region.region_id
    } == set(target_regions)


def test_current_cycle_sources_expire_at_snapshot_time():
    desired = _desired()
    snapshot = build_multi_source_status_snapshot(
        desired,
        _cycle(desired, freshness_ms=1_000),
        revision=4,
        evaluated_at=(NOW_MS + 2_000) / 1000,
    )
    states = {
        row["provider_id"]: (row["state"], row["freshness"])
        for row in snapshot["providers"]["items"]
    }
    assert states["radar.dwd.radolan_rv"] == ("stale", "stale")
    assert snapshot["product_valid_at"] is None
    assert snapshot["received_at"] is None
    assert snapshot["provenance_source_ids"] == []


def test_near_deadline_sources_are_withheld_before_publication():
    desired = _desired()
    snapshot = build_multi_source_status_snapshot(
        desired,
        _cycle(desired, freshness_ms=MIN_PUBLICATION_LEAD_MS),
        revision=5,
        evaluated_at=NOW_MS / 1000,
    )
    dwd = next(
        row for row in snapshot["providers"]["items"]
        if row["provider_id"] == "radar.dwd.radolan_rv"
    )

    assert dwd["state"] == "stale"
    assert dwd["freshness"] == "stale"
    assert dwd["reason_codes"] == [
        REASON_FRESHNESS_NOT_PUBLICATION_SAFE
    ]
    assert snapshot["product_valid_at"] is None
    assert snapshot["received_at"] is None
    assert snapshot["provenance_source_ids"] == []


def test_snapshot_rejects_cycle_for_another_desired_state():
    desired = _desired()
    changed = parse_desired_state({
        **desired.as_dict(),
        "generation": 2,
        "previous_state_sha256": desired.state_sha256,
    })
    with pytest.raises(MultiSourceSnapshotError, match="actieve desired state"):
        build_multi_source_status_snapshot(
            changed,
            _cycle(desired),
            revision=1,
            evaluated_at=NOW_MS / 1000,
        )


def test_snapshot_rejects_evaluation_before_cycle_completion():
    desired = _desired()
    with pytest.raises(MultiSourceSnapshotError, match="cycle completion"):
        build_multi_source_status_snapshot(
            desired,
            _cycle(desired),
            revision=1,
            evaluated_at=(NOW_MS - 1) / 1000,
        )


def _positive_observation(
    *, source_id: str = "radar.dwd.radolan_rv", rate: float = 3.2,
    target_id: str = "home",
):
    return PositiveRadarTargetObservation(
        source_id=source_id,
        target_id=target_id,
        rain_rate_mm_h=rate,
        product_valid_at=NOW_MS / 1000,
        received_at=NOW_MS / 1000,
        source_receipt_sha256=_sha(source_id + ":positive-source"),
        certificate_sha256=_sha(source_id + ":positive-certificate"),
        process_receipt_sha256=_sha(source_id + ":positive-process"),
    )


def _withheld_zero_observation(*, target_id: str = "home"):
    source_id = "radar.dwd.radolan_rv"
    return WithheldDryRadarTargetObservation(
        source_id=source_id,
        target_id=target_id,
        rain_rate_mm_h=0.0,
        product_valid_at=NOW_MS / 1000,
        received_at=NOW_MS / 1000,
        source_receipt_sha256=_sha(source_id + ":zero-source"),
        certificate_sha256=_sha(source_id + ":zero-certificate"),
        process_receipt_sha256=_sha(source_id + ":zero-process"),
    )


def _withheld_undetect_observation(*, target_id: str = "home"):
    source_id = "radar.dwd.radolan_rv"
    return WithheldUndetectRadarTargetObservation(
        source_id=source_id,
        target_id=target_id,
        rain_rate_mm_h=None,
        product_valid_at=NOW_MS / 1000,
        received_at=NOW_MS / 1000,
        source_receipt_sha256=_sha(source_id + ":undetect-source"),
        certificate_sha256=_sha(source_id + ":undetect-certificate"),
        process_receipt_sha256=_sha(source_id + ":undetect-process"),
    )


def test_opera_reflectivity_echo_is_visible_without_becoming_a_rain_claim():
    desired = _desired()
    committed = build_positive_radar_fact_revision(
        desired,
        _cycle(desired),
        (_opera_echo(),),
        revision=1,
        evaluated_at=NOW_MS / 1000,
    )

    snapshot = validate_committed_operational_revision(desired, committed)
    target = snapshot["targets"]["items"][0]
    current = target["current_precipitation"]
    assert current["state"] == "unknown"
    assert current["freshness"] == "current"
    assert current["rain_rate_mm_h"] is None
    assert current["provider_ids"] == ["radar.opera.dbzh"]
    assert current["reason_codes"] == [REFLECTIVITY_ECHO_REASON]
    assert target["status_text"].startswith("Radarecho: 27.5 dBZ")
    assert OperationalFactKind.CURRENT_RAINING not in {
        receipt.fact_kind for receipt in committed.admission_receipts
    }


def test_positive_radar_fusion_publishes_real_rain_but_not_fake_dry_or_threat():
    desired = _desired()
    committed = build_positive_radar_fact_revision(
        desired,
        _cycle(desired),
        (
            _positive_observation(rate=2.0),
            _positive_observation(
                source_id="satellite.noaa.err",
                rate=4.0,
            ),
        ),
        revision=5,
        evaluated_at=NOW_MS / 1000,
    )
    snapshot = validate_committed_operational_revision(desired, committed)
    target = snapshot["targets"]["items"][0]
    assert target["current_precipitation"] == {
        "state": "raining",
        "freshness": "current",
        "rain_rate_mm_h": 3.0,
        "product_valid_at": NOW_MS / 1000,
        "provider_ids": [
            "radar.dwd.radolan_rv",
            "satellite.noaa.err",
        ],
        "reason_codes": ["positive_radar_candidates_fused"],
        "ground_evidence": target["current_precipitation"]["ground_evidence"],
    }
    assert target["next_threat"]["state"] == "unknown"
    assert target["next_threat"]["reason_codes"] == [
        "nowcast_motion_not_yet_admitted"
    ]
    assert snapshot["provenance_complete"] is False
    kinds = {receipt.fact_kind for receipt in committed.admission_receipts}
    assert OperationalFactKind.CURRENT_RAINING in kinds
    assert OperationalFactKind.REGION_CURRENT in kinds
    assert OperationalFactKind.CURRENT_DRY not in kinds
    assert OperationalFactKind.NEXT_THREAT_AVAILABLE not in kinds


def test_certified_zero_is_visible_without_becoming_a_dry_claim():
    desired = _desired()
    committed = build_positive_radar_fact_revision(
        desired,
        _cycle(desired),
        (_withheld_zero_observation(),),
        revision=6,
        evaluated_at=NOW_MS / 1000,
    )
    snapshot = validate_committed_operational_revision(desired, committed)
    target = snapshot["targets"]["items"][0]
    current = target["current_precipitation"]

    assert current == {
        "state": "unknown",
        "freshness": "current",
        "rain_rate_mm_h": None,
        "product_valid_at": NOW_MS / 1000,
        "provider_ids": ["radar.dwd.radolan_rv"],
        "reason_codes": [ZERO_OBSERVATION_REASON],
        "ground_evidence": current["ground_evidence"],
    }
    assert "droogte niet onafhankelijk bevestigd" in target["status_text"]
    kinds = {receipt.fact_kind for receipt in committed.admission_receipts}
    assert OperationalFactKind.CURRENT_DRY not in kinds
    assert OperationalFactKind.CURRENT_RAINING not in kinds
    assert OperationalFactKind.REGION_CURRENT in kinds


def test_certified_undetect_is_visible_without_becoming_a_dry_claim():
    desired = _desired()
    committed = build_positive_radar_fact_revision(
        desired,
        _cycle(desired),
        (_withheld_undetect_observation(),),
        revision=7,
        evaluated_at=NOW_MS / 1000,
    )
    snapshot = validate_committed_operational_revision(desired, committed)
    target = snapshot["targets"]["items"][0]
    current = target["current_precipitation"]

    assert current["state"] == "unknown"
    assert current["freshness"] == "current"
    assert current["rain_rate_mm_h"] is None
    assert current["provider_ids"] == ["radar.dwd.radolan_rv"]
    assert current["reason_codes"] == [UNDETECT_OBSERVATION_REASON]
    assert "onder detectiedrempel" in target["status_text"]
    kinds = {receipt.fact_kind for receipt in committed.admission_receipts}
    assert OperationalFactKind.CURRENT_DRY not in kinds
    assert OperationalFactKind.CURRENT_RAINING not in kinds
    assert OperationalFactKind.REGION_CURRENT in kinds


def test_positive_radar_fusion_marks_only_each_observed_region_current():
    desired = _desired_two_regions()
    capture = CapturedSourceEvidence(
        source_id="radar.dwd.radolan_rv",
        product_at_ms=NOW_MS,
        received_at_ms=NOW_MS,
        content_sha256=_sha("two-region-dwd-content"),
        receipt_sha256=_sha("two-region-dwd-receipt"),
        size_bytes=100,
    )
    cycle = build_raw_capture_operational_cycle(
        desired,
        (capture,),
        cycle_sequence=3,
        cycle_started_at_ms=NOW_MS,
        cycle_completed_at_ms=NOW_MS,
    )
    committed = build_positive_radar_fact_revision(
        desired,
        cycle,
        (
            _positive_observation(target_id="home"),
            _positive_observation(target_id="test.frankfurt"),
        ),
        revision=6,
        evaluated_at=NOW_MS / 1000,
    )
    snapshot = validate_committed_operational_revision(desired, committed)
    regions = snapshot["regions"]["items"]

    assert len(regions) == 2
    assert {item["state"] for item in regions} == {"current"}
    assert {tuple(item["provider_ids"]) for item in regions} == {
        ("radar.dwd.radolan_rv",)
    }
    region_receipts = [
        item for item in committed.admission_receipts
        if item.fact_kind is OperationalFactKind.REGION_CURRENT
    ]
    assert len(region_receipts) == 2


def test_dwd_cycle_uses_the_proven_fifteen_minute_freshness_window():
    desired = _desired()
    capture = CapturedSourceEvidence(
        source_id="radar.dwd.radolan_rv",
        product_at_ms=NOW_MS - 12 * 60 * 1000,
        received_at_ms=NOW_MS,
        content_sha256=_sha("recent-dwd-content"),
        receipt_sha256=_sha("recent-dwd-receipt"),
        size_bytes=100,
    )
    current_cycle = build_raw_capture_operational_cycle(
        desired,
        (capture,),
        cycle_sequence=3,
        cycle_started_at_ms=NOW_MS,
        cycle_completed_at_ms=NOW_MS,
    )
    current = next(
        item
        for item in current_cycle.contributions
        if item.source_id == "radar.dwd.radolan_rv"
    )
    assert current.status is CycleStatus.CURRENT
    assert current.fresh_until_ms == NOW_MS + 3 * 60 * 1000

    stale_cycle = build_raw_capture_operational_cycle(
        desired,
        (capture,),
        cycle_sequence=4,
        cycle_started_at_ms=NOW_MS,
        cycle_completed_at_ms=NOW_MS + 3 * 60 * 1000 + 1,
    )
    stale = next(
        item
        for item in stale_cycle.contributions
        if item.source_id == "radar.dwd.radolan_rv"
    )
    assert stale.status is CycleStatus.STALE
    assert stale.reason_code == "freshness_deadline_exceeded"


def test_positive_radar_fusion_rejects_unsupported_source_semantics():
    desired = _desired()
    with pytest.raises(MultiSourceRadarSnapshotError, match="radarsemantiek"):
        build_positive_radar_fact_revision(
            desired,
            _cycle(desired),
            (_positive_observation(source_id="radar.kmi.appv4_visual"),),
            revision=6,
            evaluated_at=NOW_MS / 1000,
        )


def _threat(track_id: str = "nowcast:test-track"):
    return TargetNowcastThreat(
        target_id="home",
        source_id="radar.dwd.radolan_rv",
        track_id=track_id,
        eta_minutes=25.0,
        product_valid_at=NOW_MS / 1000,
        confidence="high",
        peak_intensity=42,
        source_receipt_sha256=_sha("threat-source"),
        certificate_sha256=_sha("threat-certificate"),
        process_receipt_sha256=_sha("threat-process"),
    )


def _ground_validation(track_id: str = "nowcast:test-track", *, wet=True):
    return IndependentGroundValidation(
        track_id=track_id,
        source_id="ground.metar.global",
        observed_at=NOW_MS / 1000 - 60,
        wet=wet,
        source_receipt_sha256=_sha("ground-source"),
        certificate_sha256=_sha("ground-certificate"),
        process_receipt_sha256=_sha("ground-process"),
    )


def test_ground_validated_motion_nowcast_publishes_eta_without_fake_probability():
    desired = _desired()
    committed = build_ground_validated_nowcast_revision(
        desired,
        _cycle(desired),
        (),
        (_threat(),),
        (_ground_validation(),),
        revision=7,
        evaluated_at=NOW_MS / 1000,
    )
    snapshot = validate_committed_operational_revision(desired, committed)
    threat = snapshot["targets"]["items"][0]["next_threat"]
    assert threat["state"] == "available"
    assert threat["eta_minutes"] == 25.0
    assert threat["passage_status"] == "possible"
    assert threat["calibrated_probability_percent"] is None
    receipt = next(
        item for item in committed.admission_receipts
        if item.fact_kind is OperationalFactKind.NEXT_THREAT_AVAILABLE
    )
    assert receipt.external_truth_validated is True


def test_motion_nowcast_is_withheld_without_wet_ground_validation():
    desired = _desired()
    with pytest.raises(MultiSourceNowcastSnapshotError, match="nat grondbewijs"):
        build_ground_validated_nowcast_revision(
            desired,
            _cycle(desired),
            (),
            (_threat(),),
            (_ground_validation(wet=False),),
            revision=8,
            evaluated_at=NOW_MS / 1000,
        )


def _shadow_arrival(*, low_confidence_fragment=False, target_id="home"):
    return WorkerOnlyShadowArrivalCandidate(
        candidate_id="shadow-arrival:test",
        workload_id="radar.dwd.radolan_rv",
        region_lineage_id="tracking-region:test",
        target_id=target_id,
        track_id="track:test",
        detection_id="detection:test",
        source_candidate_id="candidate:test",
        source_candidate_content_sha256=_sha("source-candidate"),
        analysis_grid_id="grid:test",
        reference_valid_at=NOW_MS / 1000,
        horizon_seconds=SHADOW_ARRIVAL_HORIZON_SECONDS,
        entry_seconds=15.0 * 60.0,
        exit_seconds=25.0 * 60.0,
        vx_m_s=8.0,
        vy_m_s=2.0,
        track_sample_count=3,
        footprint_pixel_count=12,
        area_km2=12.0,
        mean_rain_rate_mm_h=3.0,
        maximum_rain_rate_mm_h=8.0,
        target_location_accuracy_m=10.0,
        low_confidence_fragment=low_confidence_fragment,
        passage_state="future_intersection",
        admission_status=ADMISSION_STATUS,
    )


def test_certified_radar_motion_publishes_possible_eta_without_fake_probability():
    desired = _desired()
    committed = build_radar_motion_nowcast_revision(
        desired,
        _cycle(desired),
        (_positive_observation(),),
        (_shadow_arrival(),),
        revision=9,
        evaluated_at=NOW_MS / 1000,
    )
    snapshot = validate_committed_operational_revision(desired, committed)
    threat = snapshot["targets"]["items"][0]["next_threat"]
    assert threat == {
        "state": "available",
        "freshness": "current",
        "track_id": "track:test",
        "calibrated_probability_percent": None,
        "probability_calibration_id": None,
        "eta_minutes": 15.0,
        "distance_km": None,
        "passage_status": "possible",
        "product_valid_at": NOW_MS / 1000,
        "provider_ids": ["radar.dwd.radolan_rv"],
        "reason_codes": [MOTION_THREAT_REASON],
    }
    receipt = next(
        item for item in committed.admission_receipts
        if item.fact_kind is OperationalFactKind.NEXT_THREAT_AVAILABLE
    )
    assert receipt.external_truth_validated is False


def test_stale_unavailable_target_arrival_does_not_erase_home_nowcast():
    desired = _desired_with_unavailable_target()
    cycle = build_raw_capture_operational_cycle(
        desired,
        (CapturedSourceEvidence(
            source_id="radar.dwd.radolan_rv",
            product_at_ms=NOW_MS,
            received_at_ms=NOW_MS,
            content_sha256=_sha("unavailable-target-content"),
            receipt_sha256=_sha("unavailable-target-receipt"),
            size_bytes=100,
        ),),
        cycle_sequence=3,
        cycle_started_at_ms=NOW_MS,
        cycle_completed_at_ms=NOW_MS,
    )
    committed = build_radar_motion_nowcast_revision(
        desired,
        cycle,
        (_positive_observation(),),
        (
            _shadow_arrival(target_id="aaa.stale_tracker"),
            _shadow_arrival(),
        ),
        revision=9,
        evaluated_at=NOW_MS / 1000,
    )

    snapshot = validate_committed_operational_revision(desired, committed)
    targets = {item["target_id"]: item for item in snapshot["targets"]["items"]}
    assert targets["home"]["next_threat"]["state"] == "available"
    assert targets["home"]["next_threat"]["eta_minutes"] == 15.0
    assert len(snapshot["ui_storms"]["items"]) == 1


def test_certified_radar_motion_exposes_relevant_cell_in_ui_storms():
    desired = _desired()
    committed = build_radar_motion_nowcast_revision(
        desired,
        _cycle(desired),
        (_positive_observation(),),
        (_shadow_arrival(),),
        revision=9,
        evaluated_at=NOW_MS / 1000,
    )

    snapshot = validate_committed_operational_revision(desired, committed)
    target = snapshot["targets"]["items"][0]
    storms = snapshot["ui_storms"]

    assert storms["complete"] is True
    assert storms["overflow_count"] == 0
    assert len(storms["items"]) == 1
    storm = storms["items"][0]
    assert storm["region_id"] == target["region_id"]
    assert storm["state"] == "active"
    assert storm["motion_speed_kmh"] == pytest.approx(29.7, abs=0.1)
    assert storm["motion_heading_deg"] == pytest.approx(76.0, abs=0.1)
    assert storm["area_km2"] == 12.0
    assert storm["product_valid_at"] == NOW_MS / 1000
    assert storm["reason_codes"] == [MOTION_THREAT_REASON]


def test_low_confidence_radar_fragment_stays_withheld():
    desired = _desired()
    committed = build_radar_motion_nowcast_revision(
        desired,
        _cycle(desired),
        (_positive_observation(),),
        (_shadow_arrival(low_confidence_fragment=True),),
        revision=10,
        evaluated_at=NOW_MS / 1000,
    )
    snapshot = validate_committed_operational_revision(desired, committed)
    assert snapshot["targets"]["items"][0]["next_threat"]["state"] == "unknown"


def test_deterministic_nowcast_adapter_requires_ready_moderate_motion():
    raw = {
        "method": "adaptive_sparse_advection_v2",
        "status": "ready",
        "source": "radar.dwd.radolan_rv",
        "base_timestamp": int(NOW_MS / 1000),
        "confidence": "medium",
        "targets": {
            "home": {
                "eta_minutes": 15,
                "peak_intensity": 30,
                "first_step_intensity": 12,
            },
        },
    }
    threats = threats_from_deterministic_nowcast(
        raw,
        source_receipt_sha256=_sha("adapter-source"),
        certificate_sha256=_sha("adapter-certificate"),
        process_receipt_sha256=_sha("adapter-process"),
    )
    assert len(threats) == 1
    assert threats[0].target_id == "home"
    assert threats[0].eta_minutes == 15.0
    assert threats_from_deterministic_nowcast(
        {**raw, "confidence": "low"},
        source_receipt_sha256=_sha("adapter-source"),
        certificate_sha256=_sha("adapter-certificate"),
        process_receipt_sha256=_sha("adapter-process"),
    ) == ()


def test_verified_raw_capture_assembler_keeps_all_sources_explicit():
    desired = _desired()
    capture = CapturedSourceEvidence(
        source_id="radar.dwd.radolan_rv",
        product_at_ms=NOW_MS,
        received_at_ms=NOW_MS,
        content_sha256=_sha("raw-dwd-content"),
        receipt_sha256=_sha("raw-dwd-receipt"),
        size_bytes=100,
    )
    cycle = build_raw_capture_operational_cycle(
        desired,
        (capture,),
        source_errors={"ground.metar.global": "upstream_timeout"},
        cycle_sequence=9,
        cycle_started_at_ms=NOW_MS,
        cycle_completed_at_ms=NOW_MS,
    )
    assert len(cycle.contributions) == 28
    statuses = {item.source_id: item.status for item in cycle.contributions}
    assert statuses["radar.dwd.radolan_rv"] is CycleStatus.CURRENT
    assert statuses["ground.metar.global"] is CycleStatus.ERROR
    assert statuses["radar.kmi.radqpe_native"] is CycleStatus.BLOCKED
    assert statuses["lightning.blitzortung.event_ingress"] is CycleStatus.BLOCKED
    assert statuses["satellite.goes18.rrqpe"] is CycleStatus.NOT_APPLICABLE
