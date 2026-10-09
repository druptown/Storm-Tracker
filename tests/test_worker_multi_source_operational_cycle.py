from __future__ import annotations

import copy
from dataclasses import FrozenInstanceError
import hashlib

import pytest

import storm_tracker_worker.multi_source_operational_cycle as subject
from storm_tracker_worker.worker_acquisition_plan import acquisition_catalog_snapshot


STARTED_MS = 1_000_000
COMPLETED_MS = 1_001_000


def _sha(label: str) -> str:
    return hashlib.sha256(label.encode("ascii")).hexdigest()


def _targets() -> tuple[subject.CycleTarget, ...]:
    # The set intentionally intersects every closed catalog domain.  It does
    # not claim that every source is useful for every individual target.
    rows = (
        ("target-at", 47.5, 14.0, "AT"),
        ("target-be", 50.85, 4.35, "BE"),
        ("target-es", 40.0, -3.0, "ES"),
        ("target-fr", 48.8, 2.3, "FR"),
        ("target-gb", 51.5, -0.1, "GB"),
        ("target-it", 42.0, 12.0, "IT"),
        ("target-lu", 49.6, 6.1, "LU"),
        ("target-us", 40.0, -100.0, "US"),
    )
    return tuple(
        subject.CycleTarget.create(
            target_id=target_id,
            latitude=latitude,
            longitude=longitude,
            country_code=country_code,
        )
        for target_id, latitude, longitude, country_code in rows
    )


def _evidence(source_id: str) -> tuple[subject.EvidenceReference, ...]:
    return (
        subject.EvidenceReference(
            role="canonical_input",
            content_sha256=_sha(f"{source_id}:content"),
            receipt_sha256=_sha(f"{source_id}:receipt"),
            size_bytes=100,
            schema_id="stv3-test-source-input-v1",
        ),
    )


def _data_payload(source: subject.CatalogSource) -> subject.SourceInput:
    evidence = _evidence(source.source_id)
    coverage = source.coverage_bbox or (-90.0, -180.0, 90.0, 180.0)
    if source.kind is subject.SourceKind.RADAR:
        return subject.RadarInput(
            observed_at_ms=STARTED_MS,
            coverage_bbox=coverage,
            quantity_id="provider_native_precipitation_candidate",
            unit_id="provider_native_unit",
            sample_count=1,
            valid_sample_count=1,
            evidence=evidence,
        )
    if source.kind is subject.SourceKind.SATELLITE:
        return subject.SatelliteInput(
            observed_at_ms=STARTED_MS,
            coverage_bbox=coverage,
            platform_id=source.satellite_id or source.provider_id,
            quantity_id="provider_native_precipitation_candidate",
            unit_id="provider_native_unit",
            sample_count=1,
            valid_sample_count=1,
            evidence=evidence,
        )
    if source.kind is subject.SourceKind.LIGHTNING:
        return subject.LightningInput(
            window_start_ms=STARTED_MS - 60_000,
            window_end_ms=STARTED_MS,
            event_count=1,
            gap_count=0,
            absence_claimed=False,
            evidence=evidence,
        )
    if source.kind is subject.SourceKind.GROUND:
        return subject.GroundInput(
            window_start_ms=STARTED_MS - 60_000,
            window_end_ms=STARTED_MS,
            station_count=1,
            observation_count=1,
            quantity_ids=("precipitation_candidate",),
            evidence=evidence,
        )
    if source.kind is subject.SourceKind.MODEL:
        return subject.ModelInput(
            run_at_ms=STARTED_MS - 60_000,
            valid_from_ms=STARTED_MS,
            valid_through_ms=STARTED_MS + 3_600_000,
            lead_time_count=1,
            variable_ids=("precipitation_guidance",),
            evidence=evidence,
        )
    return subject.ValidationInput(
        reference_at_ms=STARTED_MS,
        record_count=1,
        validation_role=source.source_role,
        evidence=evidence,
    )


def _contributions(
    targets: tuple[subject.CycleTarget, ...],
) -> tuple[subject.SourceContribution, ...]:
    result: list[subject.SourceContribution] = []
    for source in subject.CATALOG_SOURCES:
        applies = any(
            subject.evaluate_target_applicability(source, target).applicable
            for target in targets
        )
        if not applies:
            status = subject.CycleStatus.NOT_APPLICABLE
            reason = "target_outside_catalog_scope"
            payload = subject.empty_source_input(source.kind)
            fresh_until_ms = None
        elif source.catalog_blocked:
            status = subject.CycleStatus.BLOCKED
            reason = source.catalog_blocked_reason
            payload = subject.empty_source_input(source.kind)
            fresh_until_ms = None
        else:
            status = subject.CycleStatus.CURRENT
            reason = None
            payload = _data_payload(source)
            fresh_until_ms = COMPLETED_MS + 60_000
        result.append(subject.SourceContribution.create(
            source_id=source.source_id,
            targets=targets,
            status=status,
            payload=payload,
            reason_code=reason,
            fresh_until_ms=fresh_until_ms,
        ))
    return tuple(result)


def _cycle(
    *,
    targets: tuple[subject.CycleTarget, ...] | None = None,
    contributions: tuple[subject.SourceContribution, ...] | None = None,
) -> subject.MultiSourceOperationalCycle:
    target_rows = targets or _targets()
    source_rows = contributions or _contributions(target_rows)
    return subject.MultiSourceOperationalCycle.create(
        desired_state_sha256=_sha("desired-state"),
        authority_epoch=4,
        cycle_sequence=12,
        producer_id="storm_tracker_worker",
        cycle_started_at_ms=STARTED_MS,
        cycle_completed_at_ms=COMPLETED_MS,
        targets=target_rows,
        contributions=source_rows,
    )


def test_catalog_is_exactly_twenty_seven_cadence_plus_blitzortung():
    upstream = acquisition_catalog_snapshot()
    assert len(upstream["workloads"]) == 27
    assert sum(
        row["status"] == "contract_ready_unwired"
        for row in upstream["workloads"]
    ) == 26
    assert len(upstream["event_ingresses"]) == 1
    assert subject.CADENCE_SOURCE_COUNT == 27
    assert subject.DISPATCHABLE_CADENCE_SOURCE_COUNT == 26
    assert subject.EVENT_INGRESS_SOURCE_COUNT == 1
    assert subject.SOURCE_COUNT == 28
    assert {item.source_id for item in subject.CATALOG_SOURCES} == (
        subject.EXPECTED_SOURCE_IDS
    )
    assert {item.kind: sum(row.kind is item.kind for row in subject.CATALOG_SOURCES)
            for item in subject.CATALOG_SOURCES} == {
        subject.SourceKind.GROUND: 4,
        subject.SourceKind.LIGHTNING: 4,
        subject.SourceKind.MODEL: 2,
        subject.SourceKind.RADAR: 11,
        subject.SourceKind.SATELLITE: 4,
        subject.SourceKind.VALIDATION: 3,
    }


def test_complete_cycle_has_one_explicit_status_for_every_source_and_target():
    cycle = _cycle()
    assert len(cycle.contributions) == 28
    assert tuple(item.source_id for item in cycle.contributions) == tuple(
        sorted(subject.EXPECTED_SOURCE_IDS)
    )
    assert cycle.coverage.source_count == 28
    assert cycle.coverage.target_count == 8
    assert cycle.coverage.applicability_row_count == 28 * 8
    assert cycle.coverage.applicable_source_count == 28
    assert dict(cycle.coverage.status_counts) == {
        "blocked": 2,
        "current": 26,
        "error": 0,
        "not_applicable": 0,
        "stale": 0,
    }
    assert all(len(item.applicability) == 8 for item in cycle.contributions)
    assert all(item.status.value != "unknown" for item in cycle.contributions)
    assert {
        item.payload.to_mapping()["contract"] for item in cycle.contributions
    } == {
        subject.GROUND_INPUT_CONTRACT,
        subject.LIGHTNING_INPUT_CONTRACT,
        subject.MODEL_INPUT_CONTRACT,
        subject.RADAR_INPUT_CONTRACT,
        subject.SATELLITE_INPUT_CONTRACT,
        subject.VALIDATION_INPUT_CONTRACT,
    }


def test_round_trip_is_content_addressed_immutable_and_exactly_canonical():
    cycle = _cycle()
    encoded = cycle.to_canonical_bytes()
    decoded = subject.parse_operational_cycle(encoded)
    assert decoded == cycle
    assert decoded.cycle_sha256 == hashlib.sha256(
        subject._canonical_json(decoded.payload_mapping())
    ).hexdigest()
    with pytest.raises(FrozenInstanceError):
        decoded.cycle_sequence = 13
    with pytest.raises(subject.MultiSourceCycleError, match="exact canoniek"):
        subject.parse_operational_cycle(b" " + encoded)


def test_country_allowlist_and_bbox_fallback_are_both_explicit():
    france = subject.CATALOG_BY_ID["radar.meteofrance.dpradar"]
    belgian = subject.CycleTarget.create(
        target_id="target-be",
        latitude=50.5,
        longitude=4.5,
        country_code="BE",
    )
    declared = subject.evaluate_target_applicability(france, belgian)
    assert declared.country_assessment == "mismatch"
    assert declared.bbox_assessment == "match"
    assert declared.applicable is False
    assert declared.reason_code == "country_mismatch"

    unknown_country = subject.CycleTarget.create(
        target_id="target-country-unknown",
        latitude=50.5,
        longitude=4.5,
        country_code=None,
    )
    fallback = subject.evaluate_target_applicability(france, unknown_country)
    assert fallback.country_assessment == "unknown"
    assert fallback.bbox_assessment == "match"
    assert fallback.applicable is True
    assert fallback.reason_code == "bbox_match_country_unknown"

    goes18 = subject.CATALOG_BY_ID["satellite.goes18.rrqpe"]
    outside = subject.evaluate_target_applicability(goes18, belgian)
    assert outside.country_assessment == "not_constrained"
    assert outside.bbox_assessment == "mismatch"
    assert outside.applicable is False


def test_not_applicable_is_required_and_cannot_hide_applicable_data():
    belgian = (
        subject.CycleTarget.create(
            target_id="target-be",
            latitude=50.85,
            longitude=4.35,
            country_code="BE",
        ),
    )
    goes18 = subject.CATALOG_BY_ID["satellite.goes18.rrqpe"]
    contribution = subject.SourceContribution.create(
        source_id=goes18.source_id,
        targets=belgian,
        status=subject.CycleStatus.NOT_APPLICABLE,
        payload=subject.empty_source_input(goes18.kind),
        reason_code="target_outside_catalog_scope",
    )
    assert contribution.status is subject.CycleStatus.NOT_APPLICABLE
    with pytest.raises(subject.MultiSourceCycleError, match="not_applicable"):
        dwd = subject.CATALOG_BY_ID["radar.dwd.radolan_rv"]
        subject.SourceContribution.create(
            source_id=dwd.source_id,
            targets=belgian,
            status=subject.CycleStatus.NOT_APPLICABLE,
            payload=subject.empty_source_input(dwd.kind),
            reason_code="target_outside_catalog_scope",
        )
    with pytest.raises(subject.MultiSourceCycleError, match="moet not_applicable"):
        subject.SourceContribution.create(
            source_id=goes18.source_id,
            targets=belgian,
            status=subject.CycleStatus.ERROR,
            payload=subject.empty_source_input(goes18.kind),
            reason_code="upstream_failed",
        )


def test_catalog_blocked_sources_cannot_be_forged_current_or_generic_error():
    targets = _targets()
    for source_id in (
        "radar.kmi.radqpe_native",
        "lightning.blitzortung.event_ingress",
    ):
        source = subject.CATALOG_BY_ID[source_id]
        with pytest.raises(subject.MultiSourceCycleError, match="exact blocked"):
            subject.SourceContribution.create(
                source_id=source_id,
                targets=targets,
                status=subject.CycleStatus.ERROR,
                payload=subject.empty_source_input(source.kind),
                reason_code="upstream_failed",
            )


def test_current_and_stale_freshness_are_checked_at_complete_cycle_boundary():
    targets = _targets()
    baseline = list(_contributions(targets))
    source = subject.CATALOG_BY_ID["radar.dwd.radolan_rv"]
    index = next(
        position for position, row in enumerate(baseline)
        if row.source_id == source.source_id
    )
    baseline[index] = subject.SourceContribution.create(
        source_id=source.source_id,
        targets=targets,
        status=subject.CycleStatus.STALE,
        payload=_data_payload(source),
        reason_code="freshness_deadline_exceeded",
        fresh_until_ms=COMPLETED_MS - 1,
    )
    cycle = _cycle(targets=targets, contributions=tuple(baseline))
    assert dict(cycle.coverage.status_counts)["stale"] == 1

    baseline[index] = subject.SourceContribution.create(
        source_id=source.source_id,
        targets=targets,
        status=subject.CycleStatus.CURRENT,
        payload=_data_payload(source),
        fresh_until_ms=COMPLETED_MS - 1,
    )
    with pytest.raises(subject.MultiSourceCycleError, match="al stale"):
        _cycle(targets=targets, contributions=tuple(baseline))


@pytest.mark.parametrize("mode", ["omission", "duplicate"])
def test_cycle_rejects_source_omission_and_duplicate(mode: str):
    targets = _targets()
    contributions = list(_contributions(targets))
    if mode == "omission":
        contributions.pop()
    else:
        contributions.append(contributions[0])
    with pytest.raises(subject.MultiSourceCycleError, match="mist, dupliceert"):
        _cycle(targets=targets, contributions=tuple(contributions))


def test_cycle_rejects_contribution_cycle_and_catalog_tampering():
    cycle = _cycle()
    contribution_tamper = copy.deepcopy(cycle.to_mapping())
    current = next(
        row for row in contribution_tamper["contributions"]
        if row["status"] == "current" and row["kind"] == "radar"
    )
    current["payload"]["sample_count"] = 2
    with pytest.raises(subject.MultiSourceCycleError, match="contribution_sha256"):
        subject.parse_operational_cycle(contribution_tamper)

    cycle_tamper = copy.deepcopy(cycle.to_mapping())
    cycle_tamper["cycle_sequence"] += 1
    with pytest.raises(subject.MultiSourceCycleError, match="cycle_sha256"):
        subject.parse_operational_cycle(cycle_tamper)

    catalog_tamper = copy.deepcopy(cycle.to_mapping())
    catalog_tamper["acquisition_catalog_sha256"] = "f" * 64
    with pytest.raises(subject.MultiSourceCycleError, match="acquisition catalog"):
        subject.parse_operational_cycle(catalog_tamper)


def test_upstream_catalog_tamper_duplicate_and_omission_fail_closed():
    original = acquisition_catalog_snapshot()
    tampered = copy.deepcopy(original)
    tampered["workloads"][0]["provider_id"] = "forged"
    with pytest.raises(subject.MultiSourceCycleError, match="inhoud en hash"):
        subject.verify_acquisition_catalog(tampered)

    duplicated = copy.deepcopy(original)
    duplicated["workloads"].append(copy.deepcopy(duplicated["workloads"][0]))
    with pytest.raises(subject.MultiSourceCycleError):
        subject.verify_acquisition_catalog(duplicated)

    omitted = copy.deepcopy(original)
    omitted["workloads"].pop()
    with pytest.raises(subject.MultiSourceCycleError):
        subject.verify_acquisition_catalog(omitted)


def test_type_mismatch_partial_payload_and_duplicate_evidence_fail_closed():
    targets = _targets()
    dwd = subject.CATALOG_BY_ID["radar.dwd.radolan_rv"]
    ground_payload = subject.GroundInput(
        window_start_ms=STARTED_MS - 1,
        window_end_ms=STARTED_MS,
        station_count=1,
        observation_count=1,
        quantity_ids=("rain",),
        evidence=_evidence("wrong-kind"),
    )
    with pytest.raises(subject.MultiSourceCycleError, match="payloadtype"):
        subject.SourceContribution.create(
            source_id=dwd.source_id,
            targets=targets,
            status=subject.CycleStatus.CURRENT,
            payload=ground_payload,
            fresh_until_ms=COMPLETED_MS + 1,
        )
    with pytest.raises(subject.MultiSourceCycleError, match="gedeeltelijk"):
        subject.RadarInput(
            observed_at_ms=STARTED_MS,
            coverage_bbox=(49.0, 2.0, 52.0, 7.0),
            quantity_id="rain",
            unit_id="mm/h",
            sample_count=1,
            valid_sample_count=1,
            evidence=(),
        )
    first = _evidence("duplicate-a")[0]
    second = subject.EvidenceReference(
        role=first.role,
        content_sha256=_sha("different-content"),
        receipt_sha256=_sha("different-receipt"),
        size_bytes=10,
        schema_id=first.schema_id,
    )
    with pytest.raises(subject.MultiSourceCycleError, match="dubbele rollen"):
        subject.RadarInput(
            observed_at_ms=STARTED_MS,
            coverage_bbox=(49.0, 2.0, 52.0, 7.0),
            quantity_id="rain",
            unit_id="mm/h",
            sample_count=2,
            valid_sample_count=2,
            evidence=(first, second),
        )


def test_target_evidence_document_and_json_key_overflows_are_bounded():
    with pytest.raises(subject.MultiSourceCycleError, match="te veel targets"):
        targets = tuple(
            subject.CycleTarget.create(
                target_id=f"target-{index:03d}",
                latitude=50.0,
                longitude=4.0,
                country_code="BE",
            )
            for index in range(subject.MAX_TARGETS + 1)
        )
        _cycle(targets=targets, contributions=_contributions(_targets()))

    evidence = tuple(
        subject.EvidenceReference(
            role=f"role-{index:03d}",
            content_sha256=_sha(f"overflow-content-{index}"),
            receipt_sha256=_sha(f"overflow-receipt-{index}"),
            size_bytes=1,
            schema_id="stv3-test-source-input-v1",
        )
        for index in range(subject.MAX_EVIDENCE_REFERENCES_PER_SOURCE + 1)
    )
    with pytest.raises(subject.MultiSourceCycleError, match="evidence overschrijdt"):
        subject.RadarInput(
            observed_at_ms=STARTED_MS,
            coverage_bbox=(49.0, 2.0, 52.0, 7.0),
            quantity_id="rain",
            unit_id="mm/h",
            sample_count=len(evidence),
            valid_sample_count=len(evidence),
            evidence=evidence,
        )
    with pytest.raises(subject.MultiSourceCycleError, match="dubbele sleutel"):
        subject.parse_operational_cycle(b'{"contract":"a","contract":"b"}')
    with pytest.raises(subject.MultiSourceCycleError, match="documentlimiet"):
        subject.parse_operational_cycle(b"x" * (subject.MAX_CYCLE_BYTES + 1))


def test_cycle_schema_rejects_unknown_field_instead_of_ignoring_it():
    mapping = _cycle().to_mapping()
    mapping["future_silent_default"] = True
    with pytest.raises(subject.MultiSourceCycleError, match="schema is niet exact"):
        subject.parse_operational_cycle(mapping)
