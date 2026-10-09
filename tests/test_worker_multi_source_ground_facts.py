from __future__ import annotations

from collections.abc import Iterable
import copy
from dataclasses import replace
import hashlib
import json
import operator

import pytest

import storm_tracker_worker.multi_source_ground_facts as subject
from storm_tracker_worker.canonical_ground_evidence_store import (
    GROUND_EVIDENCE_RECEIPT_SCHEMA,
    GroundCaptureMemberBinding,
    GroundMeasurementValue,
    GroundObservationFact,
    GroundObservationReceipt,
    GroundStationSite,
    ProviderQcFact,
    ground_fact_id,
    ground_fact_sha256,
    ground_observation_key,
    parse_ground_observation,
)
from storm_tracker_worker.multi_source_operational_cycle import (
    CycleStatus,
    CycleTarget,
    GroundInput,
)


EVALUATED_MS = 2_000_000_000_000


def _sha(label: str) -> str:
    return hashlib.sha256(label.encode("ascii")).hexdigest()


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _target(
    target_id: str,
    latitude: float,
    longitude: float,
    country_code: str | None,
    *,
    radius_km: float = 75.0,
) -> subject.GroundTargetScope:
    return subject.GroundTargetScope.create(
        CycleTarget.create(
            target_id=target_id,
            latitude=latitude,
            longitude=longitude,
            country_code=country_code,
        ),
        radius_km=radius_km,
    )


def _receipt(
    source_id: str,
    label: str,
    *,
    latitude: str,
    longitude: str,
    observed_at_ms: int = EVALUATED_MS - 60_000,
    batch: str | None = None,
    variable: str | None = None,
    unit: str | None = None,
    exact_value: str | None = "1.25",
    value_state: str = "observed",
    position_known: bool = True,
) -> GroundObservationReceipt:
    binding = subject.GROUND_DECODER_BINDINGS[source_id]
    assert binding.decoder_available
    batch_id = batch or f"{source_id}:batch"
    capture_id = "capture-" + _sha(batch_id)
    member = GroundCaptureMemberBinding(
        member_id="response",
        role="response",
        receipt_id=f"receipt-{_sha(batch_id)[:24]}",
        content_sha256=_sha(f"{batch_id}:member-content"),
        receipt_sha256=_sha(f"{batch_id}:member-receipt"),
        received_at=observed_at_ms / 1000 + 30.0,
    )
    default_variable = {
        "ground.kmi.aws_10min": "precip_quantity",
        "ground.metar.global": "precipitation_accumulation",
        "ground.netatmo.region": "netatmo_native.rain_live",
        "ground.synoptic.region": "precip_intervals_set_1",
    }[source_id]
    default_unit = {
        "ground.kmi.aws_10min": "mm",
        "ground.metar.global": "in",
        "ground.netatmo.region": "netatmo_native_unit_unspecified",
        "ground.synoptic.region": "mm",
    }[source_id]
    measurement = GroundMeasurementValue(
        variable=variable or default_variable,
        state=value_state,
        representation="decimal" if value_state == "observed" else "none",
        exact_value=exact_value if value_state == "observed" else None,
        unit=unit or default_unit,
        censor_limit=None,
    )
    station = GroundStationSite(
        provider_station_id=f"station-{label}",
        site_id=f"{binding.fact_provider}:{label}",
        network=f"{binding.fact_provider}_test_network",
        station_name=None,
        position_state="known" if position_known else "unknown",
        latitude_decimal=latitude if position_known else None,
        longitude_decimal=longitude if position_known else None,
        elevation_state="unknown",
        elevation_m_decimal=None,
    )
    fact = parse_ground_observation(GroundObservationFact(
        capture_id=capture_id,
        capture_receipt_sha256=_sha(f"{batch_id}:capture-receipt"),
        members=(member,),
        source_member_role=member.role,
        source_member_content_sha256=member.content_sha256,
        source_member_receipt_sha256=member.receipt_sha256,
        source_record_locator=f"record:{label}",
        source_record_sha256=_sha(f"{batch_id}:{label}:record"),
        decoder_identity=binding.decoder_identity,
        decoder_version=binding.decoder_version,
        config_sha256=_sha(f"{source_id}:decoder-config"),
        provider=binding.fact_provider,
        product=binding.fact_product,
        station=station,
        observed_at=observed_at_ms / 1000,
        representative_interval_start=(
            observed_at_ms / 1000
            - (600.0 if source_id == "ground.kmi.aws_10min" else 60.0)
        ),
        representative_interval_end=observed_at_ms / 1000,
        received_at=member.received_at,
        ingested_at=member.received_at + 1.0,
        measurement=measurement,
        provider_qc=(
            ProviderQcFact(
                flag=f"{binding.fact_provider}.provider_qc_preserved",
                value="candidate",
                text="not interpreted as admission",
            ),
        ),
    ).to_mapping())
    fact_id = ground_fact_id(fact)
    observation_key = ground_observation_key(fact)
    fact_sha = ground_fact_sha256(fact)
    committed_at = fact.ingested_at + 1.0
    capture_manifest_sha = _sha(f"{batch_id}:manifest")
    receipt_body = {
        "capture_manifest_sha256": capture_manifest_sha,
        "capture_receipt_sha256": fact.capture_receipt_sha256,
        "committed_at": committed_at,
        "fact_id": fact_id,
        "fact_sha256": fact_sha,
        "observation_key": observation_key,
        "schema": GROUND_EVIDENCE_RECEIPT_SCHEMA,
    }
    return GroundObservationReceipt(
        fact_id=fact_id,
        observation_key=observation_key,
        fact_sha256=fact_sha,
        receipt_sha256=hashlib.sha256(_canonical(receipt_body)).hexdigest(),
        capture_manifest_sha256=capture_manifest_sha,
        committed_at=committed_at,
        fact=fact,
    )


def _targets() -> tuple[subject.GroundTargetScope, ...]:
    return (
        _target("target-be", 50.85, 4.35, "BE"),
        _target("target-us", 40.0, -100.0, "US"),
    )


def _receipts() -> dict[str, tuple[GroundObservationReceipt, ...]]:
    return {
        "ground.kmi.aws_10min": (),
        "ground.metar.global": (
            _receipt(
                "ground.metar.global",
                "be",
                latitude="50.86",
                longitude="4.36",
                batch="metar-shared-batch",
            ),
            _receipt(
                "ground.metar.global",
                "us",
                latitude="40.01",
                longitude="-100.01",
                batch="metar-shared-batch",
            ),
        ),
        "ground.netatmo.region": (
            _receipt(
                "ground.netatmo.region",
                "be",
                latitude="50.84",
                longitude="4.34",
            ),
        ),
        "ground.synoptic.region": (
            _receipt(
                "ground.synoptic.region",
                "us",
                latitude="40.02",
                longitude="-100.02",
            ),
        ),
    }


def _slice(
    *,
    targets: Iterable[subject.GroundTargetScope] | None = None,
    receipts: dict[str, tuple[GroundObservationReceipt, ...]] | None = None,
    errors: dict[str, str] | None = None,
) -> subject.AllGroundContributionSlice:
    return subject.build_all_ground_slice(
        desired_state_sha256=_sha("desired-state"),
        authority_epoch=3,
        evaluated_at_ms=EVALUATED_MS,
        target_scopes=targets or _targets(),
        receipts_by_source=receipts or _receipts(),
        source_errors=errors,
    )


def _workload(
    ground_slice: subject.AllGroundContributionSlice,
    source_id: str,
) -> subject.GroundWorkloadContribution:
    return next(item for item in ground_slice.workloads if item.source_id == source_id)


def _target_row(
    workload: subject.GroundWorkloadContribution,
    target_id: str,
) -> subject.GroundTargetContribution:
    return next(item for item in workload.targets if item.target_id == target_id)


def test_exact_five_ground_workloads_and_real_decoder_maturity():
    assert set(subject.GROUND_DECODER_BINDINGS) == subject.GROUND_SOURCE_IDS
    assert subject.GROUND_SOURCE_IDS == {
        source_id
        for source_id, source in subject.GROUND_CATALOG_BY_ID.items()
        if source.kind.value == "ground"
    }
    assert {
        source_id
        for source_id, binding in subject.GROUND_DECODER_BINDINGS.items()
        if binding.decoder_available
    } == {
        "ground.kmi.aws_10min",
        "ground.metar.global",
        "ground.netatmo.region",
        "ground.synoptic.region",
    }
    assert {
        source_id
        for source_id, binding in subject.GROUND_DECODER_BINDINGS.items()
        if not binding.decoder_available
    } == set()
    with pytest.raises(TypeError):
        operator.setitem(
            subject.GROUND_DECODER_BINDINGS,
            "ground.metar.global",
            subject.GROUND_DECODER_BINDINGS["ground.metar.global"],
        )


def test_all_sources_and_targets_receive_explicit_candidate_or_status():
    ground_slice = _slice()
    assert len(ground_slice.workloads) == 4
    assert all(len(item.targets) == 2 for item in ground_slice.workloads)
    assert ground_slice.input_receipt_count == 4
    assert ground_slice.target_candidate_count == 4

    kmi = _workload(ground_slice, "ground.kmi.aws_10min")
    assert _target_row(kmi, "target-be").status.value == "error"
    assert _target_row(kmi, "target-be").reason_code == (
        "decoder_returned_no_ground_facts"
    )
    assert _target_row(kmi, "target-us").status.value == "not_applicable"

    metar = _workload(ground_slice, "ground.metar.global")
    assert {item.status.value for item in metar.targets} == {"current"}
    netatmo = _workload(ground_slice, "ground.netatmo.region")
    assert _target_row(netatmo, "target-be").status.value == "current"
    assert _target_row(netatmo, "target-us").status.value == "error"
    synoptic = _workload(ground_slice, "ground.synoptic.region")
    assert _target_row(synoptic, "target-be").status.value == "not_applicable"
    assert _target_row(synoptic, "target-us").status.value == "current"


def test_candidates_bind_value_unit_time_qc_freshness_and_receipt_authority():
    ground_slice = _slice()
    netatmo = _target_row(
        _workload(ground_slice, "ground.netatmo.region"),
        "target-be",
    ).candidates[0]
    assert netatmo.exact_value == "1.25"
    assert netatmo.unit == "netatmo_native_unit_unspecified"
    assert netatmo.semantics_profile == "field_level_product_semantics_unverified"
    assert netatmo.observed_at_ms == EVALUATED_MS - 60_000
    assert netatmo.fresh_until_ms == EVALUATED_MS + 540_000
    assert netatmo.freshness == "current"
    assert netatmo.qc_flags == ("netatmo.provider_qc_preserved",)
    assert netatmo.consensus_eligible is False

    synoptic = _target_row(
        _workload(ground_slice, "ground.synoptic.region"),
        "target-us",
    ).candidates[0]
    assert synoptic.unit == "mm"
    assert synoptic.semantics_profile == (
        "between_consecutive_provider_observations"
    )
    reference = _workload(
        ground_slice,
        "ground.synoptic.region",
    ).input_receipts[0]
    assert reference.fact_id == synoptic.fact_id
    assert reference.receipt_sha256 == synoptic.receipt_sha256
    assert reference.capture_manifest_sha256 == synoptic.capture_manifest_sha256


def test_kmi_candidate_uses_official_ten_minute_mm_semantics():
    receipts = _receipts()
    receipts["ground.kmi.aws_10min"] = (
        _receipt(
            "ground.kmi.aws_10min",
            "be-kmi",
            latitude="50.84",
            longitude="4.36",
            exact_value="0.4",
        ),
    )
    candidate = _target_row(
        _workload(_slice(receipts=receipts), "ground.kmi.aws_10min"),
        "target-be",
    ).candidates[0]
    assert candidate.variable == "precip_quantity"
    assert candidate.exact_value == "0.4"
    assert candidate.unit == "mm"
    assert candidate.interval_end_ms - candidate.interval_start_ms == 600_000
    assert candidate.semantics_profile == (
        "preceding_10_minutes_official_kmi"
    )
    assert candidate.consensus_eligible is False


def test_cross_provider_consensus_is_explicitly_withheld_without_certificate():
    ground_slice = _slice()
    assert ground_slice.consensus_status == "withheld"
    assert ground_slice.consensus_candidates == ()
    assert ground_slice.consensus_reason_code == (
        "cross_provider_semantics_certificate_unavailable"
    )
    assert ground_slice.consensus_authority_sha256 == (
        subject.GROUND_CONSENSUS_AUTHORITY_SHA256
    )


def test_slice_converts_to_exact_four_cycle_contributions():
    ground_slice = _slice()
    contributions = subject.build_ground_cycle_contributions(
        ground_slice,
        cycle_completed_at_ms=EVALUATED_MS,
    )
    assert {item.source_id for item in contributions} == subject.GROUND_SOURCE_IDS
    statuses = {item.source_id: item.status for item in contributions}
    assert statuses == {
        "ground.kmi.aws_10min": CycleStatus.ERROR,
        "ground.metar.global": CycleStatus.CURRENT,
        "ground.netatmo.region": CycleStatus.CURRENT,
        "ground.synoptic.region": CycleStatus.CURRENT,
    }
    for contribution in contributions:
        if contribution.status is CycleStatus.CURRENT:
            assert isinstance(contribution.payload, GroundInput)
            assert contribution.payload.observation_count > 0
            evidence = contribution.payload.evidence[0]
            workload = _workload(ground_slice, contribution.source_id)
            assert evidence.content_sha256 == workload.workload_sha256
            assert evidence.receipt_sha256 == workload.workload_receipt_sha256
        else:
            assert contribution.payload.has_data is False


def test_stale_groundfacts_remain_data_but_never_become_current():
    targets = (_target("target-be", 50.85, 4.35, "BE"),)
    receipts = {source_id: () for source_id in subject.GROUND_SOURCE_IDS}
    receipts["ground.metar.global"] = (
        _receipt(
            "ground.metar.global",
            "stale-be",
            latitude="50.85",
            longitude="4.35",
            observed_at_ms=EVALUATED_MS - 3 * 60 * 60 * 1000,
        ),
    )
    ground_slice = _slice(targets=targets, receipts=receipts)
    metar = _target_row(
        _workload(ground_slice, "ground.metar.global"),
        "target-be",
    )
    assert metar.status is subject.GroundTargetStatus.STALE
    cycle_rows = subject.build_ground_cycle_contributions(
        ground_slice,
        cycle_completed_at_ms=EVALUATED_MS,
    )
    cycle_metar = next(
        item for item in cycle_rows if item.source_id == "ground.metar.global"
    )
    assert cycle_metar.status is CycleStatus.STALE
    assert cycle_metar.fresh_until_ms < EVALUATED_MS


def test_empty_supported_decoder_and_explicit_runtime_error_are_not_silent():
    targets = (_target("target-be", 50.85, 4.35, "BE"),)
    receipts = {source_id: () for source_id in subject.GROUND_SOURCE_IDS}
    ground_slice = _slice(targets=targets, receipts=receipts)
    metar = _target_row(
        _workload(ground_slice, "ground.metar.global"),
        "target-be",
    )
    assert metar.status is subject.GroundTargetStatus.ERROR
    assert metar.reason_code == "decoder_returned_no_ground_facts"

    errored = _slice(
        targets=targets,
        receipts=receipts,
        errors={"ground.netatmo.region": "decoder_process_failed"},
    )
    netatmo = _target_row(
        _workload(errored, "ground.netatmo.region"),
        "target-be",
    )
    assert netatmo.status is subject.GroundTargetStatus.ERROR
    assert netatmo.reason_code == "decoder_process_failed"


def test_unknown_position_nonvalue_future_and_outside_radius_are_counted():
    targets = (_target("target-be", 50.85, 4.35, "BE", radius_km=5.0),)
    receipts = {source_id: () for source_id in subject.GROUND_SOURCE_IDS}
    batch = "metar-exclusion-batch"
    receipts["ground.metar.global"] = (
        _receipt(
            "ground.metar.global",
            "future",
            latitude="50.85",
            longitude="4.35",
            observed_at_ms=EVALUATED_MS + 10 * 60 * 1000,
            batch=batch,
        ),
        _receipt(
            "ground.metar.global",
            "missing",
            latitude="50.85",
            longitude="4.35",
            value_state="missing",
            exact_value=None,
            batch=batch,
        ),
        _receipt(
            "ground.metar.global",
            "outside",
            latitude="51.85",
            longitude="4.35",
            batch=batch,
        ),
        _receipt(
            "ground.metar.global",
            "unknown-position",
            latitude="0",
            longitude="0",
            position_known=False,
            batch=batch,
        ),
    )
    row = _target_row(
        _workload(_slice(targets=targets, receipts=receipts), "ground.metar.global"),
        "target-be",
    )
    assert row.status is subject.GroundTargetStatus.ERROR
    assert row.excluded_future_count == 1
    assert row.excluded_non_value_count == 1
    assert row.excluded_outside_radius_count == 1
    assert row.excluded_unknown_position_count == 1
    assert (
        len(row.candidates)
        + row.excluded_future_count
        + row.excluded_non_value_count
        + row.excluded_outside_radius_count
        + row.excluded_unknown_position_count
        == row.input_receipt_count
    )


def test_forged_receipt_decoder_duplicate_and_mixed_batch_fail_closed():
    receipts = _receipts()
    valid = receipts["ground.netatmo.region"][0]
    forged_fact = replace(valid.fact, provider="forged")
    forged = replace(valid, fact=forged_fact)
    broken = dict(receipts)
    broken["ground.netatmo.region"] = (forged,)
    with pytest.raises(subject.MultiSourceGroundError, match="driftte|herbewijs"):
        _slice(receipts=broken)

    duplicate = dict(receipts)
    duplicate["ground.netatmo.region"] = (valid, valid)
    with pytest.raises(subject.MultiSourceGroundError, match="dubbele"):
        _slice(receipts=duplicate)

    mixed = dict(receipts)
    mixed["ground.netatmo.region"] = (
        valid,
        _receipt(
            "ground.netatmo.region",
            "second-batch",
            latitude="50.85",
            longitude="4.35",
            batch="different-netatmo-batch",
        ),
    )
    with pytest.raises(subject.MultiSourceGroundError, match="decodebatches"):
        _slice(receipts=mixed)


def test_source_omission_and_error_plus_data_are_rejected():
    receipts = _receipts()
    omitted = dict(receipts)
    omitted.pop("ground.kmi.aws_10min")
    with pytest.raises(subject.MultiSourceGroundError, match="mist"):
        _slice(receipts=omitted)

    with pytest.raises(subject.MultiSourceGroundError, match="tegelijk error en data"):
        _slice(
            receipts=receipts,
            errors={"ground.metar.global": "decoder_process_failed"},
        )


def test_slice_and_nested_candidate_tampering_are_detected():
    ground_slice = _slice()
    mapping = ground_slice.to_mapping()
    mapping["authority_epoch"] += 1
    with pytest.raises(subject.MultiSourceGroundError, match="slicehash"):
        subject.parse_all_ground_slice(mapping)

    nested = copy.deepcopy(ground_slice.to_mapping())
    metar = next(
        item for item in nested["workloads"]
        if item["source_id"] == "ground.metar.global"
    )
    candidate = next(
        target["candidates"][0]
        for target in metar["targets"]
        if target["candidates"]
    )
    candidate["exact_value"] = "999"
    with pytest.raises(subject.MultiSourceGroundError, match="candidatehash"):
        subject.parse_all_ground_slice(nested)

    consensus = copy.deepcopy(ground_slice.to_mapping())
    consensus["consensus"]["candidates"] = [{"forged": True}]
    with pytest.raises(subject.MultiSourceGroundError, match="consensus"):
        subject.parse_all_ground_slice(consensus)


def test_canonical_roundtrip_duplicate_json_keys_and_bounds():
    ground_slice = _slice()
    encoded = ground_slice.to_canonical_bytes()
    assert subject.parse_all_ground_slice(encoded) == ground_slice
    with pytest.raises(subject.MultiSourceGroundError, match="exact canoniek"):
        subject.parse_all_ground_slice(b" " + encoded)
    with pytest.raises(subject.MultiSourceGroundError, match="dubbele sleutel"):
        subject.parse_all_ground_slice(b'{"contract":"a","contract":"b"}')
    with pytest.raises(subject.MultiSourceGroundError, match="documentlimiet"):
        subject.parse_all_ground_slice(b"x" * (subject.MAX_SLICE_BYTES + 1))

    overflow = {source_id: () for source_id in subject.GROUND_SOURCE_IDS}
    one = _receipts()["ground.metar.global"][0]
    overflow["ground.metar.global"] = tuple(
        one for _ in range(subject.MAX_RECEIPTS_PER_SOURCE + 1)
    )
    with pytest.raises(
        subject.MultiSourceGroundError,
        match="receipts overschrijdt limiet",
    ):
        _slice(receipts=overflow)

    too_many_targets = (
        _target(f"target-{index:03d}", 50.85, 4.35, "BE")
        for index in range(subject.MAX_TARGETS + 1)
    )
    with pytest.raises(subject.MultiSourceGroundError, match="target_scopes.*limiet"):
        _slice(targets=too_many_targets)


def test_cycle_adapter_rejects_different_evaluation_time():
    with pytest.raises(subject.MultiSourceGroundError, match="ander evaluatiemoment"):
        subject.build_ground_cycle_contributions(
            _slice(),
            cycle_completed_at_ms=EVALUATED_MS + 1,
        )
