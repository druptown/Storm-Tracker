from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import replace
import copy
import hashlib
import json
import os
from pathlib import Path
import sqlite3

import pytest

import storm_tracker_worker.completion_output_archive_adapters as subject
import storm_tracker_worker.dwd_rv_completion_consumer as dwd
import storm_tracker_worker.knmi_rtcor_completion_consumer as knmi
import storm_tracker_worker.met_office_rate_completion_consumer as met_office
import storm_tracker_worker.metar_ground_completion_consumer as metar
import storm_tracker_worker.noaa_err_completion_consumer as noaa
import storm_tracker_worker.noaa_err_quantitative as noaa_quantitative
import storm_tracker_worker.raw_manifest_audit_completion_consumer as raw_audit
from storm_tracker_worker.acquisition_capture_recovery import (
    RECOVERY_BINDINGS,
    ROLE_SET_DIGEST_CONTRACT,
    recovery_binding_sha256,
)
from storm_tracker_worker.acquisition_evidence_lifecycle import CompletionLease
from storm_tracker_worker.completion_output_archive import (
    CompletionOutputArchiveConflict,
    CompletionOutputArchiveIntegrityError,
    CompletionOutputArchiveLimits,
    CompletionOutputHotWindowPolicy,
)
from storm_tracker_worker.completion_output_archive_adapters import (
    ALL_ADAPTER_CONTRACT,
    ALL_ARCHIVE_WORKLOAD_IDS,
    RAW_MANIFEST_ARCHIVE_WORKLOAD_IDS,
    RAW_MANIFEST_ROW_REPROOF_CONTRACT,
    build_all_completion_output_archive_adapters,
)
from storm_tracker_worker.terminal_raw_completion_router import (
    TERMINAL_RAW_CONSUMER_CATALOG,
)
import test_worker_completion_output_archive_adapters as stable_helpers


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")


def _digest(value: object) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _sha(label: str) -> str:
    return hashlib.sha256(label.encode("ascii")).hexdigest()


def _event_id(label: str) -> str:
    return f"completion-{_sha(label)}"


def _archive_limits() -> CompletionOutputArchiveLimits:
    return CompletionOutputArchiveLimits(
        max_records=64,
        max_record_bytes=256 * 1024,
        max_total_record_bytes=4 * 1024 * 1024,
        max_database_bytes=16 * 1024 * 1024,
        busy_timeout_ms=5_000,
    )


def _hot_policy() -> CompletionOutputHotWindowPolicy:
    return CompletionOutputHotWindowPolicy(
        hot_window_seconds=3_600,
        safety_records_per_stream=2,
        max_plan_records=64,
    )


def _noaa_output(label: str = "noaa", sequence: int = 15) -> dict[str, object]:
    desired_sha = _sha(f"{label}:desired")
    requested = noaa_quantitative.NoaaErrRegion(50.0, 3.0, 50.1, 3.1)
    window = noaa_quantitative._window_for_region(requested)
    region = noaa.NoaaErrRegionAuthority(
        desired_state_sha256=desired_sha,
        plan_sha256=_sha(f"{label}:plan"),
        acquisition_job_id=f"job-{label}",
        acquisition_job_sha256=_sha(f"{label}:job"),
        policy_sha256=noaa.NoaaErrRegionPolicy().fingerprint,
        region_ids=(f"region-{label}",),
        region_sha256s=(_sha(f"{label}:region"),),
        target_ids=(f"target-{label}",),
        minimum_latitude=requested.minimum_latitude,
        minimum_longitude=requested.minimum_longitude,
        maximum_latitude=requested.maximum_latitude,
        maximum_longitude=requested.maximum_longitude,
        row_start=window.row_start,
        row_stop=window.row_stop,
        column_start=window.column_start,
        column_stop=window.column_stop,
        grid_definition_sha256=window.grid_definition_sha256,
        coverage_definition_sha256=window.coverage_definition_sha256,
    )
    artifact_sha = _sha(f"{label}:artifact")
    return {
        "authority": {
            "desired_authority_sha256": _sha(f"{label}:authority"),
            "desired_state_sha256": desired_sha,
        },
        "claims": {
            "decoded_shadow_candidate": True,
            "fact_staged": False,
            "meteorological_admission_granted": False,
            "operational_snapshot_published": False,
            "raw_refetch_performed": False,
        },
        "consumer_id": noaa.CONSUMER_ID,
        "decoder": {
            "artifact_handle": (
                noaa_quantitative.ARTIFACT_HANDLE_PREFIX + artifact_sha
            ),
            "artifact_sha256": artifact_sha,
            "artifact_size_bytes": 128,
            "attempt_id": f"decoder-attempt-{_sha(f'{label}:attempt')}",
            "commit_id": f"commit-{_sha(f'{label}:commit')}",
            "config_sha256": noaa.DECODER_CONFIG_SHA256,
            "identity": noaa.DECODER_IDENTITY,
            "job_id": f"decoder-job-{_sha(f'{label}:decoder-job')}",
            "receipt_sha256": _sha(f"{label}:decoder-receipt"),
            "result_sha256": _sha(f"{label}:decoder-result"),
            "run_id": f"run-{_sha(f'{label}:run')}",
            "version": noaa.DECODER_VERSION,
        },
        "disposition": noaa.OUTPUT_DISPOSITION,
        "event": {
            "acquisition_job_id": region.acquisition_job_id,
            "event_id": _event_id(label),
            "event_sha256": _sha(f"{label}:event"),
            "producer_receipt_sha256": _sha(f"{label}:producer"),
            "stream_sequence": sequence,
        },
        "fact_revision": None,
        "input": {
            "capture_id": f"capture-{label}",
            "capture_manifest_sha256": _sha(f"{label}:manifest"),
            "capture_receipt_sha256": _sha(f"{label}:capture-receipt"),
            "reference_content_sha256": _sha(f"{label}:reference"),
            "reference_size_bytes": 256,
        },
        "process_isolation": {
            "authority_sha256": _sha(f"{label}:isolation"),
            "effect_contract_sha256": noaa.PROCESS_EFFECT_CONTRACT_SHA256,
            "limits_sha256": _sha(f"{label}:limits"),
            "state": "enforced_before_target_start",
        },
        "region_authority": region.to_mapping(),
        "schema": noaa.OUTPUT_DOCUMENT_SCHEMA,
        "shadow": {
            "admission_status": "withheld",
            "content_sha256": artifact_sha,
            "quantity": "instantaneous_precipitation_rate",
            "unit": "mm/h",
        },
        "stream_id": noaa.STREAM_ID,
    }


def _raw_role_digests(recovery, roles) -> tuple[str, str]:
    ordered = tuple(sorted(roles, key=lambda role: role.role))
    binding_sha = recovery_binding_sha256(recovery)
    content = {
        "binding_sha256": binding_sha,
        "contract": ROLE_SET_DIGEST_CONTRACT,
        "digest_kind": "content_role_set",
        "members": [
            {
                "content_sha256": role.content_sha256,
                "raw_product": recovery.member(role.role).raw_product,
                "role": role.role,
                "size_bytes": role.size_bytes,
            }
            for role in ordered
        ],
    }
    receipts = {
        "binding_sha256": binding_sha,
        "contract": ROLE_SET_DIGEST_CONTRACT,
        "digest_kind": "receipt_role_set",
        "members": [
            {
                "content_sha256": role.content_sha256,
                "receipt_id": role.receipt_id,
                "receipt_sha256": role.receipt_sha256,
                "role": role.role,
                "size_bytes": role.size_bytes,
            }
            for role in ordered
        ],
    }
    return _digest(content), _digest(receipts)


def _raw_output(
    workload_id: str,
    *,
    sequence: int,
    label: str | None = None,
    dynamic_count: int | None = None,
):
    label = label or workload_id.replace(".", "-")
    consumer = raw_audit.AUDIT_CONSUMER_CATALOG[workload_id]
    rule = subject.ACTIVATION_RULES[workload_id]
    recovery = RECOVERY_BINDINGS[str(rule.handler_id)]
    if dynamic_count is None:
        dynamic_count = 1 if recovery.dynamic_member is not None else 0
    dynamic_members = (
        ()
        if recovery.dynamic_member is None
        else tuple(
            recovery.dynamic_member.materialize(
                recovery.dynamic_member.role_for_index(index)
            )
            for index in range(dynamic_count)
        )
    )
    roles = tuple(
        raw_audit.AuditRole(
            role=member.role,
            receipt_id=f"receipt-{label}-{member.role}",
            content_sha256=_sha(f"{label}:{member.role}:content"),
            receipt_sha256=_sha(f"{label}:{member.role}:receipt"),
            size_bytes=100 + index,
            content_type=sorted(member.allowed_content_types)[0],
            provider=recovery.raw_provider,
            product=member.raw_product,
        )
        for index, member in enumerate(
            sorted(
                (*recovery.members, *dynamic_members),
                key=lambda value: value.role,
            )
        )
    )
    content_sha, receipt_sha = _raw_role_digests(recovery, roles)
    binding = raw_audit.RawManifestAuditBinding(
        workload_id=workload_id,
        consumer_id=consumer.consumer_id,
        stream_id=consumer.stream_id,
        acquisition_job_id=f"job-{label}",
        provider_id=rule.provider_id,
        product_id=rule.product_id,
        capture_id=f"capture-{label}",
        capture_manifest_sha256=_sha(f"{label}:manifest"),
        capture_receipt_sha256=_sha(f"{label}:capture-receipt"),
        role_set_content_sha256=content_sha,
        role_set_receipt_sha256=receipt_sha,
        total_bytes=sum(role.size_bytes for role in roles),
        roles=roles,
    )
    lease = CompletionLease(
        event_id=_event_id(label),
        event_sha256=_sha(f"{label}:event"),
        producer_receipt_sha256=_sha(f"{label}:producer"),
        acquisition_job_id=binding.acquisition_job_id,
        consumer_id=consumer.consumer_id,
        stream_id=consumer.stream_id,
        stream_sequence=sequence,
        desired_state_sha256=_sha(f"{label}:desired"),
        authority_sha256=_sha(f"{label}:authority"),
        content_sha256=_sha(f"{label}:reference"),
        size_bytes=2,
        attempt_count=1,
        lease_token=f"lease-{label}",
        lease_expires_at=1_000.0,
        proof={},
        payload=b"{}",
    )
    return raw_audit._make_output(
        lease,
        binding,
        _sha(f"{label}:isolation"),
        _sha(f"{label}:limits"),
    )


@pytest.fixture
def all_sources(tmp_path):
    dwd_store = dwd.DwdRvCompletionOutputStore(
        (tmp_path / "dwd.sqlite3").resolve(), clock=lambda: 100.0
    )
    knmi_store = knmi.KnmiRtcorCompletionOutputStore(
        (tmp_path / "knmi.sqlite3").resolve(), clock=lambda: 101.0
    )
    met_office_store = met_office.MetOfficeRateCompletionOutputStore(
        (tmp_path / "met-office.sqlite3").resolve(), clock=lambda: 102.0
    )
    metar_store = metar.MetarGroundCompletionOutputStore(
        (tmp_path / "metar.sqlite3").resolve(), clock=lambda: 103.0
    )
    noaa_store = noaa.NoaaErrCompletionOutputStore(
        (tmp_path / "noaa.sqlite3").resolve(), clock=lambda: 104.0
    )
    raw_store = raw_audit.RawManifestAuditOutputStore(
        (tmp_path / "raw-audit.sqlite3").resolve(),
        limits=raw_audit.RawManifestAuditOutputLimits(),
        clock=lambda: 105.0,
    )
    receipts = {
        dwd.WORKLOAD_ID: dwd_store.commit(output=stable_helpers._dwd_output()),
        knmi.WORKLOAD_ID: knmi_store.commit(
            stable_helpers._rejected_radar_output(knmi, "knmi", 12)
        ),
        met_office.WORKLOAD_ID: met_office_store.commit(
            stable_helpers._rejected_radar_output(
                met_office, "met-office", 13
            )
        ),
        metar.WORKLOAD_ID: metar_store.commit(stable_helpers._metar_output()),
        noaa.WORKLOAD_ID: noaa_store.commit(_noaa_output()),
    }
    for sequence, workload_id in enumerate(
        sorted(RAW_MANIFEST_ARCHIVE_WORKLOAD_IDS), start=20
    ):
        receipts[workload_id] = raw_store.commit(
            _raw_output(workload_id, sequence=sequence)
        )
    stores = {
        dwd.WORKLOAD_ID: dwd_store,
        knmi.WORKLOAD_ID: knmi_store,
        met_office.WORKLOAD_ID: met_office_store,
        metar.WORKLOAD_ID: metar_store,
        noaa.WORKLOAD_ID: noaa_store,
        **{workload_id: raw_store for workload_id in RAW_MANIFEST_ARCHIVE_WORKLOAD_IDS},
    }
    authority = build_all_completion_output_archive_adapters(
        dwd_store=dwd_store,
        knmi_store=knmi_store,
        met_office_store=met_office_store,
        metar_store=metar_store,
        noaa_store=noaa_store,
        raw_manifest_store=raw_store,
        clock=lambda: 200.0,
    )
    return {
        "adapters": authority,
        "receipts": receipts,
        "stores": stores,
        "raw_store": raw_store,
        "tmp_path": tmp_path,
    }


def _open_archive(env, name: str = "archive.sqlite3", **kwargs):
    return env["adapters"].open_archive_store(
        (env["tmp_path"] / name).resolve(),
        limits=_archive_limits(),
        hot_policy=_hot_policy(),
        clock=kwargs.pop("clock", lambda: 300.0),
        **kwargs,
    )


def test_exact_catalog_profiles_and_shared_raw_physical_identity(all_sources):
    authority = all_sources["adapters"]
    assert set(authority.profiles) == ALL_ARCHIVE_WORKLOAD_IDS
    assert set(authority.profiles) == set(TERMINAL_RAW_CONSUMER_CATALOG)
    raw_identities = {
        authority.descriptors[workload_id].source_store_identity_sha256
        for workload_id in RAW_MANIFEST_ARCHIVE_WORKLOAD_IDS
    }
    raw_native_identities = {
        authority.descriptors[workload_id].native_store_identity_sha256
        for workload_id in RAW_MANIFEST_ARCHIVE_WORKLOAD_IDS
    }
    raw_native_instances = {
        authority.descriptors[workload_id].native_store_instance_sha256
        for workload_id in RAW_MANIFEST_ARCHIVE_WORKLOAD_IDS
    }
    raw_profiles = {
        authority.profiles[workload_id].fingerprint
        for workload_id in RAW_MANIFEST_ARCHIVE_WORKLOAD_IDS
    }
    assert len(raw_identities) == 1
    assert raw_native_identities == {all_sources["raw_store"].identity_sha256}
    assert raw_native_instances == {
        all_sources["raw_store"].store_instance_sha256
    }
    assert len(raw_profiles) == len(RAW_MANIFEST_ARCHIVE_WORKLOAD_IDS)
    for workload_id in RAW_MANIFEST_ARCHIVE_WORKLOAD_IDS:
        profile = authority.profile_for(workload_id)
        binding = TERMINAL_RAW_CONSUMER_CATALOG[workload_id]
        assert profile.consumer_id == binding.consumer_id
        assert profile.stream_id == binding.stream_id
        assert profile.source_store_contract == raw_audit.OUTPUT_STORE_CONTRACT
        assert profile.source_store_schema_version == raw_audit.SCHEMA_VERSION
        assert profile.output_document_schema == raw_audit.OUTPUT_DOCUMENT_SCHEMA
        assert profile.output_receipt_schema == raw_audit.OUTPUT_RECEIPT_SCHEMA
        assert profile.process_receipt_schema == raw_audit.PROCESS_RECEIPT_SCHEMA
        assert profile.source_limits_sha256 == all_sources["raw_store"].limits.fingerprint
        assert profile.effect_contract_sha256 == (
            raw_audit.PROCESS_EFFECT_CONTRACT_SHA256
        )
        assert profile.consumer_binding_sha256 == binding.fingerprint
        assert profile.source_immutability_mode == "append_only_sqlite_triggers"
        assert profile.source_row_reproof_contract.startswith(
            RAW_MANIFEST_ROW_REPROOF_CONTRACT + ":"
        )
    noaa_profile = authority.profile_for(noaa.WORKLOAD_ID)
    noaa_binding = TERMINAL_RAW_CONSUMER_CATALOG[noaa.WORKLOAD_ID]
    assert noaa_profile.source_store_contract == noaa.OUTPUT_STORE_CONTRACT
    assert (
        noaa_profile.source_store_schema_version
        == noaa.OUTPUT_STORE_SCHEMA_VERSION
    )
    assert noaa_profile.output_document_schema == noaa.OUTPUT_DOCUMENT_SCHEMA
    assert noaa_profile.output_receipt_schema == noaa.OUTPUT_RECEIPT_SCHEMA
    assert noaa_profile.process_receipt_schema == noaa.PROCESS_RECEIPT_SCHEMA
    assert noaa_profile.source_limits_sha256 == (
        all_sources["stores"][noaa.WORKLOAD_ID].limits.fingerprint
    )
    assert noaa_profile.effect_contract_sha256 == noaa.PROCESS_EFFECT_CONTRACT_SHA256
    assert noaa_profile.consumer_binding_sha256 == noaa_binding.fingerprint
    assert noaa_profile.source_immutability_mode == "append_only_sqlite_triggers"
    assert noaa_profile.source_row_reproof_contract == subject.TRIGGER_ROW_REPROOF_CONTRACT
    assert authority.diagnostics()["contract"] == ALL_ADAPTER_CONTRACT


_NON_RETENTION_ELIGIBLE_RAW_WORKLOAD_IDS = frozenset({
    "radar.kmi.appv4_visual",
    "validation.geosphere.location",
    "validation.italiameteo.dpc_sri_historical_catalog",
    "model.openmeteo.italiameteo_icon2i.region",
    "validation.meteolux.location",
    "model.openmeteo.region",
})


def test_catalog_retention_evidence_preserves_exact_six_inert_raw_rows(
    all_sources,
):
    authority = all_sources["adapters"]
    false_rows = {
        row["workload_id"]
        for row in subject.acquisition_catalog_snapshot()["workloads"]
        if row["durable_production_retention_eligible"] is False
        and row["workload_id"] in ALL_ARCHIVE_WORKLOAD_IDS
    }
    assert false_rows == _NON_RETENTION_ELIGIBLE_RAW_WORKLOAD_IDS
    for workload_id, descriptor in authority.descriptors.items():
        is_false = workload_id in _NON_RETENTION_ELIGIBLE_RAW_WORKLOAD_IDS
        assert descriptor.durable_production_retention_eligible is (not is_false)
        assert descriptor.production_retention_claimed is False
        assert descriptor.source_payload_materialized is False
        if is_false:
            binding = TERMINAL_RAW_CONSUMER_CATALOG[workload_id]
            assert binding.consumer_kind == "raw_manifest_audit"
            assert binding.uses_reference_payload is True
            assert descriptor.raw_capture_service_only is True


def test_forged_non_raw_audit_false_retention_row_stays_fail_closed(
    monkeypatch,
):
    workload_id = knmi.WORKLOAD_ID
    snapshot = copy.deepcopy(subject.acquisition_catalog_snapshot())
    row = next(
        item for item in snapshot["workloads"]
        if item["workload_id"] == workload_id
    )
    row["durable_production_retention_eligible"] = False
    monkeypatch.setattr(subject, "acquisition_catalog_snapshot", lambda: snapshot)
    rules = dict(subject.ACTIVATION_RULES)
    rules[workload_id] = replace(
        rules[workload_id], raw_capture_service_only=True
    )
    monkeypatch.setattr(subject, "ACTIVATION_RULES", rules)
    assert TERMINAL_RAW_CONSUMER_CATALOG[workload_id].consumer_kind in {
        "decoder",
        "decoder_reference",
    }
    with pytest.raises(CompletionOutputArchiveIntegrityError):
        subject._catalog_row(workload_id, raw_manifest_audit=True)


@pytest.mark.parametrize("workload_id", sorted(ALL_ARCHIVE_WORKLOAD_IDS))
def test_real_eleven_source_rows_seal_load_and_verify_exactly(
    all_sources, workload_id
):
    authority = all_sources["adapters"]
    receipt = all_sources["receipts"][workload_id]
    archive = _open_archive(all_sources, f"archive-{_sha(workload_id)}.sqlite3")
    record = authority.build_record(workload_id, receipt.event_id)
    assert record.output_sha256 == receipt.output_sha256
    assert record.output_receipt_sha256 == receipt.receipt_sha256
    assert record.process_receipt_sha256 == receipt.process_receipt_sha256
    seal = authority.seal(archive, workload_id, receipt.event_id)
    loaded = authority.load_archived(archive, workload_id, receipt.event_id)
    assert loaded is not None and loaded.seal == seal and loaded.record == record
    assert authority.verify_archived_for_ack(
        archive,
        workload_id,
        receipt.event_id,
        output_sha256=receipt.output_sha256,
        output_receipt_sha256=receipt.receipt_sha256,
        process_receipt_sha256=receipt.process_receipt_sha256,
        record_sha256=record.record_sha256,
    ) == loaded


def test_all_catalog_seals_do_not_mutate_or_delete_any_provider_row(all_sources):
    authority = all_sources["adapters"]
    unique_stores = {store.path: store for store in all_sources["stores"].values()}
    before = {path: path.read_bytes() for path in unique_stores}
    archive = _open_archive(all_sources)
    for workload_id, receipt in all_sources["receipts"].items():
        authority.seal(archive, workload_id, receipt.event_id)
    assert {path: path.read_bytes() for path in unique_stores} == before
    with closing(sqlite3.connect(all_sources["raw_store"].path)) as database:
        assert database.execute("SELECT count(*) FROM audit_outputs").fetchone()[0] == len(
            RAW_MANIFEST_ARCHIVE_WORKLOAD_IDS
        )


@pytest.mark.parametrize("dynamic_count", [1, 12])
def test_goes_glm_dynamic_role_minimum_and_maximum_archive_exactly(
    all_sources, dynamic_count
):
    workload_id = "lightning.goes18.glm"
    output = _raw_output(
        workload_id,
        sequence=100 + dynamic_count,
        label=f"glm-dynamic-{dynamic_count}",
        dynamic_count=dynamic_count,
    )
    receipt = all_sources["raw_store"].commit(output)
    record = all_sources["adapters"].build_record(workload_id, receipt.event_id)
    assert record.output_sha256 == receipt.output_sha256
    assert len(output["audit"]["roles"]) == 2 + dynamic_count
    assert "payload" not in output


@pytest.mark.parametrize("mutation", ["minimum", "hole", "extra", "order"])
def test_goes_glm_dynamic_role_drift_fails_archive_closed(
    all_sources, mutation
):
    workload_id = "lightning.goes19.glm"
    count = 0 if mutation == "minimum" else (12 if mutation == "extra" else 2)
    output = copy.deepcopy(_raw_output(
        workload_id,
        sequence={"minimum": 120, "hole": 121, "extra": 122, "order": 123}[
            mutation
        ],
        label=f"glm-invalid-{mutation}",
        dynamic_count=count,
    ))
    roles = output["audit"]["roles"]
    if mutation == "hole":
        role = next(value for value in roles if value["role"] == "netcdf_01")
        role["role"] = "netcdf_02"
    elif mutation == "extra":
        extra = copy.deepcopy(roles[-1])
        extra.update({
            "role": "netcdf_12",
            "receipt_id": "receipt-glm-invalid-extra-netcdf_12",
            "content_sha256": _sha("glm-invalid-extra:netcdf_12:content"),
            "receipt_sha256": _sha("glm-invalid-extra:netcdf_12:receipt"),
        })
        roles.append(extra)
        output["audit"]["total_bytes"] += extra["size_bytes"]
    elif mutation == "order":
        roles.reverse()
    receipt = all_sources["raw_store"].commit(output)
    with pytest.raises(CompletionOutputArchiveIntegrityError):
        all_sources["adapters"].build_record(workload_id, receipt.event_id)


def test_raw_route_cannot_claim_another_route_event(all_sources):
    workloads = sorted(RAW_MANIFEST_ARCHIVE_WORKLOAD_IDS)
    receipt = all_sources["receipts"][workloads[0]]
    with pytest.raises(CompletionOutputArchiveIntegrityError):
        all_sources["adapters"].build_record(workloads[1], receipt.event_id)


def test_forged_raw_role_manifest_is_native_valid_but_archive_rejected(all_sources):
    workload_id = sorted(RAW_MANIFEST_ARCHIVE_WORKLOAD_IDS)[0]
    forged = copy.deepcopy(_raw_output(workload_id, sequence=99, label="forged-role"))
    forged["audit"]["roles"][0]["content_sha256"] = _sha("forged-content")
    receipt = all_sources["raw_store"].commit(forged)
    assert all_sources["raw_store"].load(receipt.event_id) is not None
    with pytest.raises(CompletionOutputArchiveIntegrityError):
        all_sources["adapters"].build_record(workload_id, receipt.event_id)


@pytest.mark.parametrize(
    "field",
    [
        "event_sha256",
        "producer_receipt_sha256",
        "output_sha256",
        "receipt_sha256",
        "process_receipt_sha256",
        "consumer_id",
    ],
)
def test_forged_raw_physical_receipt_projection_fails_closed(
    all_sources, monkeypatch, field
):
    original = raw_audit.RawManifestAuditOutputStore._row

    def forged(store, row):
        receipt = original(store, row)
        value = "forged-consumer-v1" if field == "consumer_id" else _sha(field)
        return replace(receipt, **{field: value})

    monkeypatch.setattr(raw_audit.RawManifestAuditOutputStore, "_row", forged)
    workload_id = sorted(RAW_MANIFEST_ARCHIVE_WORKLOAD_IDS)[0]
    receipt = all_sources["receipts"][workload_id]
    with pytest.raises(CompletionOutputArchiveIntegrityError):
        all_sources["adapters"].build_record(workload_id, receipt.event_id)


@pytest.mark.parametrize(
    "field",
    ["region_authority_sha256", "candidate_content_sha256", "decoder_run_id"],
)
def test_forged_noaa_receipt_projection_fails_closed(
    all_sources, monkeypatch, field
):
    original = noaa.NoaaErrCompletionOutputStore._from_row

    def forged(store, row):
        receipt = original(store, row)
        value = "run-" + _sha("forged-run") if field == "decoder_run_id" else _sha(field)
        return replace(receipt, **{field: value})

    monkeypatch.setattr(noaa.NoaaErrCompletionOutputStore, "_from_row", forged)
    receipt = all_sources["receipts"][noaa.WORKLOAD_ID]
    with pytest.raises(CompletionOutputArchiveIntegrityError):
        all_sources["adapters"].build_record(noaa.WORKLOAD_ID, receipt.event_id)


def test_raw_trigger_tamper_between_plan_and_seal_creates_no_head(all_sources):
    authority = all_sources["adapters"]
    workload_id = sorted(RAW_MANIFEST_ARCHIVE_WORKLOAD_IDS)[0]
    receipt = all_sources["receipts"][workload_id]
    record = authority.build_record(workload_id, receipt.event_id)
    archive = _open_archive(all_sources)
    with closing(sqlite3.connect(all_sources["raw_store"].path)) as database:
        database.execute("DROP TRIGGER audit_outputs_no_update")
        database.commit()
    with pytest.raises(CompletionOutputArchiveIntegrityError):
        archive.seal(record)
    assert authority.load_archived(archive, workload_id, receipt.event_id) is None


def test_raw_metadata_capacity_tamper_fails_before_record(all_sources):
    workload_id = sorted(RAW_MANIFEST_ARCHIVE_WORKLOAD_IDS)[0]
    receipt = all_sources["receipts"][workload_id]
    with closing(sqlite3.connect(all_sources["raw_store"].path)) as database:
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            database.execute("UPDATE audit_metadata SET max_receipts=1")
        database.rollback()
        database.execute("DROP TRIGGER audit_metadata_no_update")
        database.execute("UPDATE audit_metadata SET max_receipts=1")
        database.execute(
            raw_audit.RawManifestAuditOutputStore._trigger_definitions()[
                "audit_metadata_no_update"
            ]
        )
        database.commit()
    with pytest.raises(CompletionOutputArchiveIntegrityError):
        all_sources["adapters"].build_record(workload_id, receipt.event_id)


@pytest.mark.parametrize(
    "workload_id",
    [
        knmi.WORKLOAD_ID,
        noaa.WORKLOAD_ID,
        met_office.WORKLOAD_ID,
        metar.WORKLOAD_ID,
        sorted(RAW_MANIFEST_ARCHIVE_WORKLOAD_IDS)[0],
    ],
)
def test_v2_native_identity_and_live_instance_sentinel_are_exact(
    all_sources, workload_id
):
    authority = all_sources["adapters"]
    store = all_sources["stores"][workload_id]
    receipt = all_sources["receipts"][workload_id]
    descriptor = authority.descriptors[workload_id]
    assert descriptor.native_store_identity_sha256 == store.assert_instance_held()
    assert descriptor.native_store_instance_sha256 == store.store_instance_sha256

    record = authority.build_record(workload_id, receipt.event_id)
    archive = _open_archive(
        all_sources, f"native-instance-{_sha(workload_id)}.sqlite3"
    )
    is_raw = workload_id in RAW_MANIFEST_ARCHIVE_WORKLOAD_IDS
    metadata_table = "audit_metadata" if is_raw else "output_metadata"
    trigger_name = (
        "audit_metadata_no_update" if is_raw else "output_metadata_no_update"
    )
    with closing(sqlite3.connect(store.path)) as database:
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            database.execute(
                f"UPDATE {metadata_table} SET store_instance_sha256=? "
                "WHERE singleton=1",
                (_sha("blocked-native-instance-tamper"),),
            )
        database.rollback()
        database.execute(f"DROP TRIGGER {trigger_name}")
        database.execute(
            f"UPDATE {metadata_table} SET store_instance_sha256=? WHERE singleton=1",
            (_sha(f"forged-native-instance:{workload_id}"),),
        )
        database.execute(type(store)._trigger_definitions()[trigger_name])
        database.commit()

    with pytest.raises(CompletionOutputArchiveIntegrityError):
        archive.seal(record)
    assert archive.diagnostics()["records"] == 0


def test_raw_same_path_valid_store_replacement_fails_existing_and_reopen(
    all_sources
):
    authority = all_sources["adapters"]
    workload_id = sorted(RAW_MANIFEST_ARCHIVE_WORKLOAD_IDS)[0]
    receipt = all_sources["receipts"][workload_id]
    archive = _open_archive(all_sources)
    authority.seal(archive, workload_id, receipt.event_id)
    old_path = all_sources["raw_store"].path
    replacement = raw_audit.RawManifestAuditOutputStore(
        (all_sources["tmp_path"] / "replacement-raw.sqlite3").resolve(),
        limits=all_sources["raw_store"].limits,
    )
    os.replace(replacement.path, old_path)
    with pytest.raises(CompletionOutputArchiveIntegrityError):
        authority.build_record(workload_id, receipt.event_id)
    reopened_raw = raw_audit.RawManifestAuditOutputStore(
        old_path, limits=all_sources["raw_store"].limits
    )
    stores = all_sources["stores"]
    reopened = build_all_completion_output_archive_adapters(
        dwd_store=stores[dwd.WORKLOAD_ID],
        knmi_store=stores[knmi.WORKLOAD_ID],
        met_office_store=stores[met_office.WORKLOAD_ID],
        metar_store=stores[metar.WORKLOAD_ID],
        noaa_store=stores[noaa.WORKLOAD_ID],
        raw_manifest_store=reopened_raw,
    )
    with pytest.raises(CompletionOutputArchiveIntegrityError):
        reopened.open_archive_store(
            archive.path,
            limits=_archive_limits(),
            hot_policy=_hot_policy(),
        )


def test_raw_limits_and_route_catalog_drift_change_or_refuse_authority(
    all_sources, monkeypatch
):
    alternate_raw = raw_audit.RawManifestAuditOutputStore(
        (all_sources["tmp_path"] / "limited-raw.sqlite3").resolve(),
        limits=raw_audit.RawManifestAuditOutputLimits(max_receipts=64),
    )
    stores = all_sources["stores"]
    alternate = build_all_completion_output_archive_adapters(
        dwd_store=stores[dwd.WORKLOAD_ID],
        knmi_store=stores[knmi.WORKLOAD_ID],
        met_office_store=stores[met_office.WORKLOAD_ID],
        metar_store=stores[metar.WORKLOAD_ID],
        noaa_store=stores[noaa.WORKLOAD_ID],
        raw_manifest_store=alternate_raw,
    )
    workload_id = sorted(RAW_MANIFEST_ARCHIVE_WORKLOAD_IDS)[0]
    assert (
        alternate.profile_for(workload_id).fingerprint
        != all_sources["adapters"].profile_for(workload_id).fingerprint
    )
    monkeypatch.setattr(subject, "AUDIT_CONSUMER_CATALOG", {})
    with pytest.raises(CompletionOutputArchiveIntegrityError):
        build_all_completion_output_archive_adapters(
            dwd_store=stores[dwd.WORKLOAD_ID],
            knmi_store=stores[knmi.WORKLOAD_ID],
            met_office_store=stores[met_office.WORKLOAD_ID],
            metar_store=stores[metar.WORKLOAD_ID],
            noaa_store=stores[noaa.WORKLOAD_ID],
            raw_manifest_store=all_sources["raw_store"],
        )


@pytest.mark.parametrize("workload_id", [noaa.WORKLOAD_ID, "satellite.goes19.rrqpe"])
def test_noaa_and_shared_raw_concurrent_archive_cas(all_sources, workload_id):
    receipt = all_sources["receipts"][workload_id]
    first = _open_archive(all_sources, "cas.sqlite3")
    second = _open_archive(all_sources, "cas.sqlite3")
    with ThreadPoolExecutor(max_workers=2) as pool:
        seals = tuple(
            pool.map(
                lambda archive: all_sources["adapters"].seal(
                    archive, workload_id, receipt.event_id
                ),
                (first, second),
            )
        )
    assert seals[0] == seals[1]


class _FaultOnce:
    def __init__(self, point: str) -> None:
        self.point = point
        self.used = False

    def __call__(self, point: str) -> None:
        if point == self.point and not self.used:
            self.used = True
            raise RuntimeError(point)


@pytest.mark.parametrize(
    ("point", "committed"),
    [("archive.before_commit", False), ("archive.after_commit", True)],
)
def test_shared_raw_archive_commit_fault_boundary(all_sources, point, committed):
    workload_id = sorted(RAW_MANIFEST_ARCHIVE_WORKLOAD_IDS)[0]
    receipt = all_sources["receipts"][workload_id]
    archive = _open_archive(
        all_sources,
        f"fault-{point.rsplit('.', 1)[-1]}.sqlite3",
        fault_injector=_FaultOnce(point),
    )
    with pytest.raises(RuntimeError, match=point):
        all_sources["adapters"].seal(archive, workload_id, receipt.event_id)
    loaded = all_sources["adapters"].load_archived(
        archive, workload_id, receipt.event_id
    )
    assert (loaded is not None) is committed


def test_archive_only_replay_and_privacy_for_new_routes(all_sources, monkeypatch):
    authority = all_sources["adapters"]
    archive = _open_archive(all_sources)
    selected = [noaa.WORKLOAD_ID, *sorted(RAW_MANIFEST_ARCHIVE_WORKLOAD_IDS)]
    for workload_id in selected:
        receipt = all_sources["receipts"][workload_id]
        authority.seal(archive, workload_id, receipt.event_id)
    for store_type in (
        noaa.NoaaErrCompletionOutputStore,
        raw_audit.RawManifestAuditOutputStore,
    ):
        monkeypatch.setattr(
            store_type,
            "_connect",
            lambda _self: (_ for _ in ()).throw(AssertionError("source reopened")),
        )
    for workload_id in selected:
        receipt = all_sources["receipts"][workload_id]
        assert authority.load_archived(archive, workload_id, receipt.event_id) is not None
    rendered = _canonical(authority.diagnostics()) + repr(authority).encode("ascii")
    assert str(all_sources["tmp_path"]).encode("utf-8") not in rendered
    assert b"private_" not in rendered.lower()


def test_factory_rejects_wrong_noaa_or_shared_raw_store_type(all_sources):
    stores = all_sources["stores"]
    with pytest.raises(TypeError):
        build_all_completion_output_archive_adapters(
            dwd_store=stores[dwd.WORKLOAD_ID],
            knmi_store=stores[knmi.WORKLOAD_ID],
            met_office_store=stores[met_office.WORKLOAD_ID],
            metar_store=stores[metar.WORKLOAD_ID],
            noaa_store=all_sources["raw_store"],
            raw_manifest_store=all_sources["raw_store"],
        )


def test_all_eleven_adapter_remains_absent_from_runtime_wiring():
    root = Path(__file__).resolve().parents[1]
    for relative in (
        "storm_tracker_worker/service.py",
        "storm_tracker_worker/terminal_raw_completion_router.py",
        "storm_tracker_worker/acquisition_evidence_lifecycle.py",
        "storm_tracker_worker/completion_route_adapters.py",
    ):
        assert "completion_output_archive_adapters" not in (
            root / relative
        ).read_text(encoding="utf-8")
