from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from types import MappingProxyType
import hashlib
import json
import os
import sqlite3

import pytest

import storm_tracker_worker.completion_output_archive_adapters as adapters_module
import storm_tracker_worker.dwd_rv_completion_consumer as dwd
import storm_tracker_worker.dwd_rv_completion_process_isolation as dwd_isolation
import storm_tracker_worker.knmi_rtcor_completion_consumer as knmi
import storm_tracker_worker.met_office_rate_completion_consumer as met_office
import storm_tracker_worker.metar_ground_completion_consumer as metar
from storm_tracker_worker.completion_output_archive import (
    CompletionOutputArchiveConflict,
    CompletionOutputArchiveIntegrityError,
    CompletionOutputArchiveLimits,
    CompletionOutputArchiveSourceReproofAuthority,
    CompletionOutputArchiveSourceReproofBinding,
    CompletionOutputArchiveStore,
    CompletionOutputHotWindowPolicy,
)
from storm_tracker_worker.completion_output_archive_adapters import (
    DWD_ROW_REPROOF_CONTRACT,
    SOURCE_PATH_IDENTITY_CASE_POLICY,
    STABLE_ARCHIVE_WORKLOAD_IDS,
    TRIGGER_ROW_REPROOF_CONTRACT,
    build_stable_completion_output_archive_adapters,
)
from storm_tracker_worker.terminal_raw_completion_router import (
    TERMINAL_RAW_CONSUMER_CATALOG,
)
from storm_tracker_worker.worker_acquisition_plan import (
    CATALOG_SHA256,
    acquisition_catalog_snapshot,
)


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


def _event(label: str, sequence: int) -> dict[str, object]:
    return {
        "event_id": f"completion-{_sha(label)}",
        "event_sha256": _sha(f"{label}:event"),
        "producer_receipt_sha256": _sha(f"{label}:producer"),
        "stream_sequence": sequence,
    }


def _authority(label: str) -> dict[str, str]:
    return {
        "authority_sha256": _sha(f"{label}:authority"),
        "desired_state_sha256": _sha(f"{label}:desired"),
    }


def _dwd_output(label: str = "dwd", sequence: int = 11) -> dict[str, object]:
    event = _event(label, sequence)
    event["acquisition_job_id"] = f"job-{label}"
    return {
        "authority": _authority(label),
        "claims": {
            "forecast_generated": False,
            "hard_realtime_deadline_certified": False,
            "meteorological_admission_granted": False,
            "raw_refetch_performed": False,
            "wet_dry_classification_applied": False,
        },
        "consumer_id": dwd.CONSUMER_ID,
        "decoder": {
            "artifact_handle": f"artifact:{_sha(f'{label}:artifact')}",
            "artifact_sha256": _sha(f"{label}:artifact"),
            "artifact_size_bytes": 1,
            "attempt_id": f"attempt-{label}",
            "commit_id": f"commit-{label}",
            "job_id": f"decoder-job-{label}",
            "receipt_sha256": _sha(f"{label}:evidence-receipt"),
            "result_sha256": _sha(f"{label}:result"),
            "run_id": f"run-{_sha(f'{label}:run')}",
        },
        "disposition": dwd.OUTPUT_DISPOSITION,
        "event": event,
        "fact_revision": None,
        "handoff": {
            "capture_id": f"capture-{label}",
            "capture_manifest_sha256": _sha(f"{label}:manifest"),
            "capture_receipt_sha256": _sha(f"{label}:capture-receipt"),
            "observation_policy": "base_plus000_only",
        },
        "input": {
            "content_sha256": _sha(f"{label}:content"),
            "size_bytes": 1,
        },
        "process_isolation": {
            "authority_sha256": _sha(f"{label}:isolation"),
            "effect_contract_sha256": (
                dwd_isolation.PROCESS_EFFECT_CONTRACT_SHA256
            ),
            "limits_sha256": _sha(f"{label}:limits"),
            "state": "enforced_before_target_start",
        },
        "profile": {
            "detector_config_sha256": _sha(f"{label}:detector"),
            "pipeline_config_sha256": _sha(f"{label}:pipeline"),
            "profile_sha256": _sha(f"{label}:profile"),
            "target_grid_sha256": _sha(f"{label}:grid"),
            "transform_config_sha256": _sha(f"{label}:transform"),
        },
        "schema": dwd.OUTPUT_DOCUMENT_SCHEMA,
        "shadow": {
            "admission_status": dwd.EVIDENCE_TO_CELLS_ADMISSION_STATUS,
            "analysis_field_content_sha256": _sha(f"{label}:analysis"),
            "candidate_id": f"candidate-{label}",
            "content_sha256": _sha(f"{label}:candidate"),
            "detection_objects": 0,
            "operational_admission_blockers": [],
            "target_grid_sha256": _sha(f"{label}:grid"),
            "valid_at": 1.0,
        },
        "stream_id": dwd.STREAM_ID,
    }


def _rejected_radar_output(module, label: str, sequence: int) -> dict[str, object]:
    return {
        "authority": _authority(label),
        "claims": {
            "decoded": False,
            "fact_staged": False,
            "meteorological_admission": False,
            "operational_snapshot_published": False,
        },
        "consumer_id": module.CONSUMER_ID,
        "decoder": {
            "artifact": None,
            "attempt_id": f"decoder-attempt-{_sha(f'{label}:attempt')}",
            "config_sha256": module.DECODER_CONFIG_SHA256,
            "evidence": {
                "capture_manifest_sha256": _sha(f"{label}:manifest"),
                "commit_id": f"commit-{_sha(f'{label}:commit')}",
                "receipt_sha256": _sha(f"{label}:evidence"),
                "result_sha256": _sha(f"{label}:result"),
                "revision_sha256": _sha(f"{label}:revision"),
                "run_id": f"run-{_sha(f'{label}:run')}",
            },
            "identity": module.DECODER_IDENTITY,
            "job_id": f"decoder-job-{_sha(f'{label}:job')}",
            "outcome": "decode_error",
            "reason_code": "decode_error:fixture",
            "version": module.DECODER_VERSION,
        },
        "disposition": "shadow_rejected",
        "event": _event(label, sequence),
        "fact_revision": None,
        "input": {
            "acquisition_job_id": f"job-{label}",
            "capture_id": f"capture-{label}",
            "capture_manifest_sha256": _sha(f"{label}:manifest"),
            "capture_receipt_sha256": _sha(f"{label}:capture-receipt"),
            "reference_content_sha256": _sha(f"{label}:reference"),
            "role_set_content_sha256": _sha(f"{label}:roles"),
            "role_set_receipt_sha256": _sha(f"{label}:role-receipt"),
        },
        "isolation": {
            "authority_sha256": _sha(f"{label}:isolation"),
            "effect_contract_sha256": module.PROCESS_EFFECT_CONTRACT_SHA256,
            "limits_sha256": _sha(f"{label}:limits"),
            "process_boundary": "fresh_spawn_hard_cpu_rss_wall_output_caps",
            "runtime_wiring": "inert",
        },
        "mode": module.OUTPUT_MODE,
        "schema": module.OUTPUT_DOCUMENT_SCHEMA,
        "stream_id": module.STREAM_ID,
        "workload_id": module.WORKLOAD_ID,
    }


def _metar_output(label: str = "metar", sequence: int = 14) -> dict[str, object]:
    factset = {
        "capture_id": f"capture-{label}",
        "capture_manifest_sha256": _sha(f"{label}:manifest"),
        "decoder_config_sha256": metar.DECODER_CONFIG_SHA256,
        "decoder_identity": metar.DECODER_IDENTITY,
        "decoder_version": metar.DECODER_VERSION,
        "fact_count": 0,
        "factset_id": f"ground-factset-{_sha(f'{label}:factset-id')}",
        "factset_sha256": _sha(f"{label}:factset"),
        "schema": metar.FACTSET_RECEIPT_SCHEMA,
    }
    factset["receipt_sha256"] = _digest(factset)
    return {
        "authority": _authority(label),
        "claims": {
            "canonical_ground_evidence_committed": True,
            "ground_confirmation": False,
            "meteorological_admission": False,
            "operational_fact_published": False,
            "operational_snapshot_published": False,
        },
        "consumer_id": metar.CONSUMER_ID,
        "decoder": {
            "attempt_id": f"decoder-attempt-{_sha(f'{label}:attempt')}",
            "config_sha256": metar.DECODER_CONFIG_SHA256,
            "factset": factset,
            "identity": metar.DECODER_IDENTITY,
            "job_id": f"decoder-job-{_sha(f'{label}:job')}",
            "version": metar.DECODER_VERSION,
        },
        "disposition": "shadow_evidence_committed",
        "event": _event(label, sequence),
        "fact_revision": None,
        "input": {
            "acquisition_job_id": f"job-{label}",
            "capture_id": f"capture-{label}",
            "capture_manifest_sha256": _sha(f"{label}:manifest"),
            "capture_receipt_sha256": _sha(f"{label}:capture-receipt"),
            "reference_content_sha256": _sha(f"{label}:reference"),
            "role_set_content_sha256": _sha(f"{label}:roles"),
            "role_set_receipt_sha256": _sha(f"{label}:role-receipt"),
        },
        "isolation": {
            "authority_sha256": _sha(f"{label}:isolation"),
            "effect_contract_sha256": metar.PROCESS_EFFECT_CONTRACT_SHA256,
            "limits_sha256": _sha(f"{label}:limits"),
            "process_boundary": "fresh_spawn_hard_cpu_rss_wall_output_caps",
            "runtime_wiring": "inert",
        },
        "mode": metar.OUTPUT_MODE,
        "schema": metar.OUTPUT_DOCUMENT_SCHEMA,
        "stream_id": metar.STREAM_ID,
        "workload_id": metar.WORKLOAD_ID,
    }


def _archive_limits() -> CompletionOutputArchiveLimits:
    return CompletionOutputArchiveLimits(
        max_records=64,
        max_record_bytes=128 * 1024,
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


@pytest.fixture
def stable_sources(tmp_path):
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
    receipts = {
        dwd.WORKLOAD_ID: dwd_store.commit(output=_dwd_output()),
        knmi.WORKLOAD_ID: knmi_store.commit(
            _rejected_radar_output(knmi, "knmi", 12)
        ),
        met_office.WORKLOAD_ID: met_office_store.commit(
            _rejected_radar_output(met_office, "met-office", 13)
        ),
        metar.WORKLOAD_ID: metar_store.commit(_metar_output()),
    }
    stores = {
        dwd.WORKLOAD_ID: dwd_store,
        knmi.WORKLOAD_ID: knmi_store,
        met_office.WORKLOAD_ID: met_office_store,
        metar.WORKLOAD_ID: metar_store,
    }
    authority = build_stable_completion_output_archive_adapters(
        dwd_store=dwd_store,
        knmi_store=knmi_store,
        met_office_store=met_office_store,
        metar_store=metar_store,
        clock=lambda: 200.0,
    )
    return {
        "adapters": authority,
        "receipts": receipts,
        "stores": stores,
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


def test_profiles_bind_exact_four_store_authorities(stable_sources):
    authority = stable_sources["adapters"]
    assert set(authority.profiles) == STABLE_ARCHIVE_WORKLOAD_IDS
    exact = {
        dwd.WORKLOAD_ID: (
            dwd.RESULT_STORE_CONTRACT,
            dwd.SCHEMA_VERSION,
            dwd.OUTPUT_DOCUMENT_SCHEMA,
            dwd.OUTPUT_RECEIPT_SCHEMA,
            dwd.PROCESS_RECEIPT_SCHEMA,
            dwd_isolation.PROCESS_EFFECT_CONTRACT_SHA256,
            "store_metadata",
        ),
        knmi.WORKLOAD_ID: (
            knmi.OUTPUT_STORE_CONTRACT,
            knmi.SCHEMA_VERSION,
            knmi.OUTPUT_DOCUMENT_SCHEMA,
            knmi.OUTPUT_RECEIPT_SCHEMA,
            knmi.PROCESS_RECEIPT_SCHEMA,
            knmi.PROCESS_EFFECT_CONTRACT_SHA256,
            "output_metadata",
        ),
        met_office.WORKLOAD_ID: (
            met_office.OUTPUT_STORE_CONTRACT,
            met_office.SCHEMA_VERSION,
            met_office.OUTPUT_DOCUMENT_SCHEMA,
            met_office.OUTPUT_RECEIPT_SCHEMA,
            met_office.PROCESS_RECEIPT_SCHEMA,
            met_office.PROCESS_EFFECT_CONTRACT_SHA256,
            "output_metadata",
        ),
        metar.WORKLOAD_ID: (
            metar.OUTPUT_STORE_CONTRACT,
            metar.SCHEMA_VERSION,
            metar.OUTPUT_DOCUMENT_SCHEMA,
            metar.OUTPUT_RECEIPT_SCHEMA,
            metar.PROCESS_RECEIPT_SCHEMA,
            metar.PROCESS_EFFECT_CONTRACT_SHA256,
            "output_metadata",
        ),
    }
    catalog_rows = {
        row["workload_id"]: row for row in acquisition_catalog_snapshot()["workloads"]
    }
    for workload_id, profile in authority.profiles.items():
        binding = TERMINAL_RAW_CONSUMER_CATALOG[workload_id]
        (
            store_contract,
            schema_version,
            output_schema,
            output_receipt_schema,
            process_receipt_schema,
            effect_sha256,
            metadata_table,
        ) = exact[workload_id]
        assert profile.cadence_seconds == catalog_rows[workload_id]["cadence_seconds"]
        assert profile.acquisition_catalog_sha256 == CATALOG_SHA256
        assert profile.consumer_binding_sha256 == binding.fingerprint
        assert profile.consumer_id == binding.consumer_id
        assert profile.stream_id == binding.stream_id
        assert profile.source_store_contract == store_contract
        assert profile.source_store_schema_version == schema_version
        assert profile.output_document_schema == output_schema
        assert profile.output_receipt_schema == output_receipt_schema
        assert profile.process_receipt_schema == process_receipt_schema
        assert profile.effect_contract_sha256 == effect_sha256
        descriptor = authority.descriptors[workload_id]
        assert descriptor.native_store_identity_sha256 == (
            stable_sources["stores"][workload_id].identity_sha256
        )
        assert descriptor.native_store_instance_sha256 == (
            stable_sources["stores"][workload_id].store_instance_sha256
        )
        with closing(
            sqlite3.connect(stable_sources["stores"][workload_id].path)
        ) as database:
            limits_sha256 = database.execute(
                f"SELECT limits_sha256 FROM {metadata_table} WHERE singleton=1"
            ).fetchone()[0]
        assert profile.source_limits_sha256 == limits_sha256
    dwd_profile = authority.profile_for(dwd.WORKLOAD_ID)
    assert dwd_profile.source_immutability_mode == "append_only_sqlite_triggers"
    assert dwd_profile.source_row_reproof_contract == DWD_ROW_REPROOF_CONTRACT
    for workload_id in STABLE_ARCHIVE_WORKLOAD_IDS - {dwd.WORKLOAD_ID}:
        profile = authority.profile_for(workload_id)
        assert profile.source_immutability_mode == "append_only_sqlite_triggers"
        assert profile.source_row_reproof_contract == TRIGGER_ROW_REPROOF_CONTRACT


@pytest.mark.parametrize("workload_id", sorted(STABLE_ARCHIVE_WORKLOAD_IDS))
def test_real_source_row_builds_and_seals_exact_record(stable_sources, workload_id):
    authority = stable_sources["adapters"]
    receipt = stable_sources["receipts"][workload_id]
    record = authority.build_record(workload_id, receipt.event_id)
    output = receipt.output
    assert record.output == output
    assert record.event_id == receipt.event_id
    assert record.event_sha256 == receipt.event_sha256
    assert record.producer_receipt_sha256 == receipt.producer_receipt_sha256
    assert record.output_sha256 == receipt.output_sha256
    assert record.output_receipt_sha256 == receipt.receipt_sha256
    assert record.process_receipt_sha256 == receipt.process_receipt_sha256
    assert record.workload_id == workload_id
    assert record.stream_sequence == output["event"]["stream_sequence"]
    assert record.output["authority"]["desired_state_sha256"] == (
        receipt.desired_state_sha256
    )
    assert 0 < record.source_row_bytes < 16 * 1024**2
    archive = _open_archive(stable_sources)
    seal = authority.seal(archive, workload_id, receipt.event_id)
    entry = authority.verify_archived_for_ack(
        archive,
        workload_id,
        receipt.event_id,
        output_sha256=receipt.output_sha256,
        output_receipt_sha256=receipt.receipt_sha256,
        process_receipt_sha256=receipt.process_receipt_sha256,
        record_sha256=record.record_sha256,
    )
    assert seal.record_sha256 == record.record_sha256
    assert entry.record.canonical_bytes == record.canonical_bytes


def test_archive_only_load_and_verify_never_reopens_sources(
    stable_sources, monkeypatch
):
    authority = stable_sources["adapters"]
    archive = _open_archive(stable_sources)
    for workload_id, receipt in stable_sources["receipts"].items():
        authority.seal(archive, workload_id, receipt.event_id)

    def forbidden_source_open():
        raise AssertionError("archive-only operation reopened a source store")

    for store in stable_sources["stores"].values():
        monkeypatch.setattr(store, "_connect", forbidden_source_open)
    for workload_id, receipt in stable_sources["receipts"].items():
        entry = authority.load_archived(archive, workload_id, receipt.event_id)
        assert entry is not None
        verified = authority.verify_archived_for_ack(
            archive,
            workload_id,
            receipt.event_id,
            output_sha256=receipt.output_sha256,
            output_receipt_sha256=receipt.receipt_sha256,
            process_receipt_sha256=receipt.process_receipt_sha256,
            record_sha256=entry.record.record_sha256,
        )
        assert verified == entry


def test_duplicate_is_idempotent_and_same_event_conflict_is_closed(stable_sources):
    authority = stable_sources["adapters"]
    receipt = stable_sources["receipts"][knmi.WORKLOAD_ID]
    archive = _open_archive(stable_sources)
    first = authority.seal(archive, knmi.WORKLOAD_ID, receipt.event_id)
    second = authority.seal(archive, knmi.WORKLOAD_ID, receipt.event_id)
    assert second == first
    exact = authority.build_record(knmi.WORKLOAD_ID, receipt.event_id)
    forged = replace(exact, stream_sequence=exact.stream_sequence + 1)
    with pytest.raises(CompletionOutputArchiveConflict):
        archive.seal(forged)
    assert archive.diagnostics()["records"] == 1


def _rehash_dwd_desired(store, event_id: str, desired_sha256: str) -> None:
    with closing(sqlite3.connect(store.path)) as database:
        database.row_factory = sqlite3.Row
        row = database.execute(
            "SELECT * FROM completion_outputs WHERE event_id=?", (event_id,)
        ).fetchone()
        assert row is not None
        output = json.loads(bytes(row["output_json"]).decode("utf-8"))
        output["authority"]["desired_state_sha256"] = desired_sha256
        output_bytes = _canonical(output)
        output_sha = hashlib.sha256(output_bytes).hexdigest()
        receipt_sha = _digest(
            {
                "created_at": float(row["created_at"]),
                "output_sha256": output_sha,
                "schema": dwd.OUTPUT_RECEIPT_SCHEMA,
            }
        )
        isolation = output["process_isolation"]
        process_sha = _digest(
            {
                "created_at": float(row["created_at"]),
                "effect_contract_sha256": isolation["effect_contract_sha256"],
                "event_id": event_id,
                "isolation_authority_sha256": isolation["authority_sha256"],
                "limits_sha256": isolation["limits_sha256"],
                "output_receipt_sha256": receipt_sha,
                "output_sha256": output_sha,
                "schema": dwd.PROCESS_RECEIPT_SCHEMA,
            }
        )
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            database.execute(
                "UPDATE completion_outputs SET desired_state_sha256=? "
                "WHERE event_id=?",
                (desired_sha256, event_id),
            )
        database.rollback()
        database.execute("DROP TRIGGER completion_outputs_no_update")
        database.execute(
            "UPDATE completion_outputs SET desired_state_sha256=?,output_sha256=?,"
            "receipt_sha256=?,process_receipt_sha256=?,output_json=?,output_bytes=? "
            "WHERE event_id=?",
            (
                desired_sha256,
                output_sha,
                receipt_sha,
                process_sha,
                output_bytes,
                len(output_bytes),
                event_id,
            ),
        )
        database.execute(
            dwd.DwdRvCompletionOutputStore._trigger_definitions()[
                "completion_outputs_no_update"
            ]
        )
        database.commit()


def test_dwd_rehashed_mutation_between_build_and_seal_fails_before_head(
    stable_sources,
):
    authority = stable_sources["adapters"]
    receipt = stable_sources["receipts"][dwd.WORKLOAD_ID]
    record = authority.build_record(dwd.WORKLOAD_ID, receipt.event_id)
    _rehash_dwd_desired(
        stable_sources["stores"][dwd.WORKLOAD_ID],
        receipt.event_id,
        _sha("changed-desired"),
    )
    archive = _open_archive(stable_sources)
    with pytest.raises(CompletionOutputArchiveIntegrityError):
        archive.seal(record)
    assert archive.diagnostics()["records"] == 0


@pytest.mark.parametrize(
    "workload_id",
    [knmi.WORKLOAD_ID, met_office.WORKLOAD_ID, metar.WORKLOAD_ID],
)
def test_trigger_stores_block_update_and_missing_trigger_fails_reproof(
    stable_sources, workload_id
):
    authority = stable_sources["adapters"]
    store = stable_sources["stores"][workload_id]
    receipt = stable_sources["receipts"][workload_id]
    with closing(sqlite3.connect(store.path)) as database:
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            database.execute(
                "UPDATE completion_outputs SET desired_state_sha256=? WHERE event_id=?",
                (_sha("forged-desired"), receipt.event_id),
            )
        database.rollback()
        database.execute("DROP TRIGGER completion_outputs_no_update")
        database.execute(
            "UPDATE completion_outputs SET desired_state_sha256=? WHERE event_id=?",
            (_sha("forged-desired"), receipt.event_id),
        )
        database.commit()
    with pytest.raises(CompletionOutputArchiveIntegrityError):
        authority.build_record(workload_id, receipt.event_id)


def test_source_path_alias_is_rejected_without_platform_skip(stable_sources):
    store = stable_sources["stores"][knmi.WORKLOAD_ID]
    store.path = store.path.parent / "alias" / ".." / store.path.name
    with pytest.raises(CompletionOutputArchiveIntegrityError, match="aliased"):
        build_stable_completion_output_archive_adapters(
            dwd_store=stable_sources["stores"][dwd.WORKLOAD_ID],
            knmi_store=store,
            met_office_store=stable_sources["stores"][met_office.WORKLOAD_ID],
            metar_store=stable_sources["stores"][metar.WORKLOAD_ID],
        )


def test_archive_path_must_be_disjoint_from_every_source(stable_sources):
    source_path = stable_sources["stores"][dwd.WORKLOAD_ID].path
    with pytest.raises(CompletionOutputArchiveIntegrityError, match="aliases"):
        stable_sources["adapters"].open_archive_store(
            source_path,
            limits=_archive_limits(),
            hot_policy=_hot_policy(),
        )


@pytest.mark.parametrize("relationship", ["same", "nested", "parent", "alias"])
def test_same_nested_parent_and_aliased_archive_paths_fail_closed(
    stable_sources, relationship
):
    source = stable_sources["stores"][knmi.WORKLOAD_ID].path
    if relationship == "same":
        archive_path = source
    elif relationship == "nested":
        archive_path = source / "nested-archive.sqlite3"
    elif relationship == "parent":
        archive_path = source.parent
    else:
        archive_path = source.parent / "alias" / ".." / "archive.sqlite3"
    with pytest.raises(CompletionOutputArchiveIntegrityError):
        stable_sources["adapters"].open_archive_store(
            archive_path,
            limits=_archive_limits(),
            hot_policy=_hot_policy(),
        )


def test_windows_case_policy_is_explicitly_bound(stable_sources):
    expected = (
        "windows-normcase-v1" if os.name == "nt" else "platform-case-sensitive-v1"
    )
    assert SOURCE_PATH_IDENTITY_CASE_POLICY == expected
    first = stable_sources["adapters"].source_reproof_authority.fingerprint
    reopened = build_stable_completion_output_archive_adapters(
        dwd_store=stable_sources["stores"][dwd.WORKLOAD_ID],
        knmi_store=stable_sources["stores"][knmi.WORKLOAD_ID],
        met_office_store=stable_sources["stores"][met_office.WORKLOAD_ID],
        metar_store=stable_sources["stores"][metar.WORKLOAD_ID],
    )
    assert reopened.source_reproof_authority.fingerprint == first


def test_catalog_drift_fails_closed(stable_sources, monkeypatch):
    changed = acquisition_catalog_snapshot()
    changed["catalog_sha256"] = _sha("changed-catalog")
    monkeypatch.setattr(
        adapters_module, "acquisition_catalog_snapshot", lambda: changed
    )
    with pytest.raises(CompletionOutputArchiveIntegrityError, match="catalog"):
        build_stable_completion_output_archive_adapters(
            dwd_store=stable_sources["stores"][dwd.WORKLOAD_ID],
            knmi_store=stable_sources["stores"][knmi.WORKLOAD_ID],
            met_office_store=stable_sources["stores"][met_office.WORKLOAD_ID],
            metar_store=stable_sources["stores"][metar.WORKLOAD_ID],
        )


def test_source_path_drift_changes_reproof_authority_not_source_profile(
    stable_sources,
):
    alternate_knmi = knmi.KnmiRtcorCompletionOutputStore(
        (stable_sources["tmp_path"] / "alternate-knmi.sqlite3").resolve()
    )
    alternate = build_stable_completion_output_archive_adapters(
        dwd_store=stable_sources["stores"][dwd.WORKLOAD_ID],
        knmi_store=alternate_knmi,
        met_office_store=stable_sources["stores"][met_office.WORKLOAD_ID],
        metar_store=stable_sources["stores"][metar.WORKLOAD_ID],
    )
    exact = stable_sources["adapters"]
    assert alternate.profile_for(knmi.WORKLOAD_ID).fingerprint == (
        exact.profile_for(knmi.WORKLOAD_ID).fingerprint
    )
    assert alternate.source_reproof_authority.fingerprint != (
        exact.source_reproof_authority.fingerprint
    )
    archive = _open_archive(stable_sources, name="path-authority.sqlite3")
    with pytest.raises(CompletionOutputArchiveIntegrityError):
        alternate.open_archive_store(
            archive.path,
            limits=_archive_limits(),
            hot_policy=_hot_policy(),
        )


def test_same_path_valid_store_replacement_changes_lineage_and_fails_closed(
    stable_sources,
):
    authority = stable_sources["adapters"]
    archive = _open_archive(stable_sources, name="file-witness.sqlite3")
    old_authority_sha = authority.source_reproof_authority.fingerprint
    old_store = stable_sources["stores"][knmi.WORKLOAD_ID]
    old_receipt = stable_sources["receipts"][knmi.WORKLOAD_ID]
    replacement = knmi.KnmiRtcorCompletionOutputStore(
        (stable_sources["tmp_path"] / "valid-empty-replacement.sqlite3").resolve()
    )
    os.replace(replacement.path, old_store.path)

    with pytest.raises(
        CompletionOutputArchiveIntegrityError, match="file instance changed"
    ):
        authority.build_record(knmi.WORKLOAD_ID, old_receipt.event_id)

    reopened_store = knmi.KnmiRtcorCompletionOutputStore(old_store.path)
    reopened = build_stable_completion_output_archive_adapters(
        dwd_store=stable_sources["stores"][dwd.WORKLOAD_ID],
        knmi_store=reopened_store,
        met_office_store=stable_sources["stores"][met_office.WORKLOAD_ID],
        metar_store=stable_sources["stores"][metar.WORKLOAD_ID],
    )
    assert reopened.source_reproof_authority.fingerprint != old_authority_sha
    with pytest.raises(CompletionOutputArchiveIntegrityError):
        reopened.open_archive_store(
            archive.path,
            limits=_archive_limits(),
            hot_policy=_hot_policy(),
        )


def test_source_limits_drift_changes_profile_and_closed_catalog(stable_sources):
    changed_limits = knmi.KnmiRtcorCompletionOutputLimits(max_receipts=2_048)
    alternate_knmi = knmi.KnmiRtcorCompletionOutputStore(
        (stable_sources["tmp_path"] / "limited-knmi.sqlite3").resolve(),
        limits=changed_limits,
    )
    alternate = build_stable_completion_output_archive_adapters(
        dwd_store=stable_sources["stores"][dwd.WORKLOAD_ID],
        knmi_store=alternate_knmi,
        met_office_store=stable_sources["stores"][met_office.WORKLOAD_ID],
        metar_store=stable_sources["stores"][metar.WORKLOAD_ID],
    )
    exact = stable_sources["adapters"]
    assert alternate.profile_for(knmi.WORKLOAD_ID).source_limits_sha256 == (
        changed_limits.fingerprint
    )
    assert alternate.profile_for(knmi.WORKLOAD_ID).fingerprint != (
        exact.profile_for(knmi.WORKLOAD_ID).fingerprint
    )
    assert alternate.source_catalog.fingerprint != exact.source_catalog.fingerprint


def test_consumer_binding_drift_changes_profile_authority(
    stable_sources, monkeypatch
):
    catalog = dict(TERMINAL_RAW_CONSUMER_CATALOG)
    current = catalog[met_office.WORKLOAD_ID]
    catalog[met_office.WORKLOAD_ID] = replace(
        current, implementation_id=f"{current.implementation_id}-drift"
    )
    monkeypatch.setattr(
        adapters_module,
        "TERMINAL_RAW_CONSUMER_CATALOG",
        MappingProxyType(catalog),
    )
    alternate = build_stable_completion_output_archive_adapters(
        dwd_store=stable_sources["stores"][dwd.WORKLOAD_ID],
        knmi_store=stable_sources["stores"][knmi.WORKLOAD_ID],
        met_office_store=stable_sources["stores"][met_office.WORKLOAD_ID],
        metar_store=stable_sources["stores"][metar.WORKLOAD_ID],
    )
    exact_profile = stable_sources["adapters"].profile_for(
        met_office.WORKLOAD_ID
    )
    drifted_profile = alternate.profile_for(met_office.WORKLOAD_ID)
    assert drifted_profile.consumer_binding_sha256 == (
        catalog[met_office.WORKLOAD_ID].fingerprint
    )
    assert drifted_profile.fingerprint != exact_profile.fingerprint


@pytest.mark.parametrize(
    "mismatch",
    ["event", "producer", "output", "output_receipt", "process_receipt"],
)
def test_seal_reproof_rejects_each_receipt_and_event_mismatch(
    stable_sources, mismatch
):
    authority = stable_sources["adapters"]
    receipt = stable_sources["receipts"][dwd.WORKLOAD_ID]
    record = authority.build_record(dwd.WORKLOAD_ID, receipt.event_id)
    if mismatch == "event":
        forged = replace(record, event_sha256=_sha("forged-event"))
    elif mismatch == "producer":
        forged = replace(
            record, producer_receipt_sha256=_sha("forged-producer")
        )
    elif mismatch == "output_receipt":
        forged = replace(
            record, output_receipt_sha256=_sha("forged-output-receipt")
        )
    elif mismatch == "process_receipt":
        forged = replace(
            record, process_receipt_sha256=_sha("forged-process-receipt")
        )
    else:
        output = record.output
        output["shadow"]["valid_at"] = 2.0
        forged = type(record).from_output(
            source_profile_sha256=record.source_profile_sha256,
            event_id=record.event_id,
            event_sha256=record.event_sha256,
            producer_receipt_sha256=record.producer_receipt_sha256,
            workload_id=record.workload_id,
            consumer_id=record.consumer_id,
            stream_id=record.stream_id,
            stream_sequence=record.stream_sequence,
            created_at=record.created_at,
            output_receipt_sha256=record.output_receipt_sha256,
            process_receipt_sha256=record.process_receipt_sha256,
            source_row_sha256=record.source_row_sha256,
            source_row_bytes=record.source_row_bytes,
            output=output,
        )
    archive = _open_archive(stable_sources, name=f"mismatch-{mismatch}.sqlite3")
    with pytest.raises(CompletionOutputArchiveIntegrityError):
        archive.seal(forged)
    assert archive.diagnostics()["records"] == 0


def _source_rows(stable_sources):
    result = {}
    for workload_id, store in stable_sources["stores"].items():
        with closing(sqlite3.connect(store.path)) as database:
            rows = database.execute(
                "SELECT * FROM completion_outputs ORDER BY event_id"
            ).fetchall()
            result[workload_id] = tuple(tuple(row) for row in rows)
    return result


def test_sealing_never_updates_deletes_or_prunes_provider_rows(stable_sources):
    before = _source_rows(stable_sources)
    authority = stable_sources["adapters"]
    archive = _open_archive(stable_sources, name="no-source-mutation.sqlite3")
    for workload_id, receipt in stable_sources["receipts"].items():
        authority.seal(archive, workload_id, receipt.event_id)
    assert _source_rows(stable_sources) == before
    assert archive.diagnostics()["records"] == 4
    source = Path(adapters_module.__file__).read_text(encoding="utf-8").casefold()
    assert "delete from completion_outputs" not in source
    assert "update completion_outputs" not in source
    assert not hasattr(authority, "prune")
    assert not hasattr(authority, "delete")
    assert not hasattr(authority, "reclaim")


def test_forged_reproof_authority_is_rejected_by_adapter_facade(stable_sources):
    authority = stable_sources["adapters"]
    bindings = list(authority.source_reproof_authority.bindings)
    bindings[0] = replace(bindings[0], resolver_sha256="0" * 64)
    forged_authority = CompletionOutputArchiveSourceReproofAuthority(
        authority_id="forged-source-reproof-authority-v1",
        bindings=tuple(bindings),
    )
    forged_archive = CompletionOutputArchiveStore(
        (stable_sources["tmp_path"] / "forged-archive.sqlite3").resolve(),
        source_catalog=authority.source_catalog,
        source_reproof_authority=forged_authority,
        expected_source_reproof_authority_sha256=forged_authority.fingerprint,
        limits=_archive_limits(),
        hot_policy=_hot_policy(),
        disjoint_paths=tuple(store.path for store in stable_sources["stores"].values()),
    )
    receipt = stable_sources["receipts"][dwd.WORKLOAD_ID]
    with pytest.raises(CompletionOutputArchiveIntegrityError, match="authority"):
        authority.load_archived(forged_archive, dwd.WORKLOAD_ID, receipt.event_id)


class FaultOnce:
    def __init__(self, point: str) -> None:
        self.point = point
        self.fired = False

    def __call__(self, point: str) -> None:
        if point == self.point and not self.fired:
            self.fired = True
            raise RuntimeError(point)


@pytest.mark.parametrize(
    ("point", "expected_records"),
    [("archive.before_commit", 0), ("archive.after_commit", 1)],
)
def test_archive_fault_boundaries_remain_exact(
    stable_sources, point, expected_records
):
    authority = stable_sources["adapters"]
    receipt = stable_sources["receipts"][met_office.WORKLOAD_ID]
    archive = _open_archive(
        stable_sources,
        name=f"fault-{expected_records}.sqlite3",
        fault_injector=FaultOnce(point),
    )
    with pytest.raises(RuntimeError, match=point):
        authority.seal(archive, met_office.WORKLOAD_ID, receipt.event_id)
    assert archive.diagnostics()["records"] == expected_records
    entry = authority.load_archived(
        archive, met_office.WORKLOAD_ID, receipt.event_id
    )
    assert (entry is not None) == bool(expected_records)


def test_two_archive_instances_cas_to_one_seal(stable_sources):
    authority = stable_sources["adapters"]
    receipt = stable_sources["receipts"][metar.WORKLOAD_ID]
    first = _open_archive(stable_sources, name="cas.sqlite3")
    second = _open_archive(stable_sources, name="cas.sqlite3")

    def seal(store):
        return authority.seal(store, metar.WORKLOAD_ID, receipt.event_id)

    with ThreadPoolExecutor(max_workers=2) as executor:
        seals = list(executor.map(seal, (first, second)))
    assert seals[0] == seals[1]
    assert first.diagnostics()["records"] == 1


def test_archive_schema_and_row_tamper_are_detected(stable_sources):
    authority = stable_sources["adapters"]
    receipt = stable_sources["receipts"][knmi.WORKLOAD_ID]
    archive = _open_archive(stable_sources)
    authority.seal(archive, knmi.WORKLOAD_ID, receipt.event_id)
    with closing(sqlite3.connect(archive.path)) as database:
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            database.execute(
                "UPDATE archive_records SET output_sha256=? WHERE event_id=?",
                (_sha("forged-output"), receipt.event_id),
            )
        database.rollback()
        database.execute("DROP TRIGGER archive_records_no_update")
        database.execute(
            "UPDATE archive_records SET output_sha256=? WHERE event_id=?",
            (_sha("forged-output"), receipt.event_id),
        )
        database.commit()
    with pytest.raises(CompletionOutputArchiveIntegrityError):
        authority.load_archived(archive, knmi.WORKLOAD_ID, receipt.event_id)


def test_archive_only_verify_mismatch_fails_closed(stable_sources):
    authority = stable_sources["adapters"]
    receipt = stable_sources["receipts"][dwd.WORKLOAD_ID]
    archive = _open_archive(stable_sources)
    authority.seal(archive, dwd.WORKLOAD_ID, receipt.event_id)
    with pytest.raises(CompletionOutputArchiveConflict):
        authority.verify_archived_for_ack(
            archive,
            dwd.WORKLOAD_ID,
            receipt.event_id,
            output_sha256="0" * 64,
            output_receipt_sha256=receipt.receipt_sha256,
            process_receipt_sha256=receipt.process_receipt_sha256,
        )


def test_diagnostics_and_repr_do_not_disclose_source_paths(stable_sources):
    authority = stable_sources["adapters"]
    rendered = _canonical(authority.diagnostics()) + repr(authority).encode("ascii")
    for store in stable_sources["stores"].values():
        assert str(store.path).encode("utf-8") not in rendered
    assert authority.diagnostics()["runtime_wiring"] == "inert"


def test_unknown_workload_and_wrong_store_type_fail_closed(stable_sources):
    authority = stable_sources["adapters"]
    with pytest.raises(CompletionOutputArchiveIntegrityError):
        authority.profile_for("satellite.noaa.err")
    with pytest.raises(TypeError):
        build_stable_completion_output_archive_adapters(
            dwd_store=stable_sources["stores"][dwd.WORKLOAD_ID],
            knmi_store=stable_sources["stores"][met_office.WORKLOAD_ID],
            met_office_store=stable_sources["stores"][met_office.WORKLOAD_ID],
            metar_store=stable_sources["stores"][metar.WORKLOAD_ID],
        )


def test_module_is_absent_from_runtime_and_keeps_stable_factory_closed():
    root = Path(__file__).resolve().parents[1]
    adapter_source = (
        root / "storm_tracker_worker" / "completion_output_archive_adapters.py"
    ).read_text(encoding="utf-8")
    assert "build_stable_completion_output_archive_adapters" in adapter_source
    assert "build_all_completion_output_archive_adapters" in adapter_source
    for relative in (
        "storm_tracker_worker/service.py",
        "storm_tracker_worker/terminal_raw_completion_router.py",
        "storm_tracker_worker/acquisition_evidence_lifecycle.py",
        "storm_tracker_worker/completion_route_adapters.py",
    ):
        assert "completion_output_archive_adapters" not in (
            root / relative
        ).read_text(encoding="utf-8")
