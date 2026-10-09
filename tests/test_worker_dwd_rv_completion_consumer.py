from __future__ import annotations

import ast
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import asdict, replace
from datetime import datetime, timezone
import hashlib
import io
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tarfile
import threading
import time

import pytest

import storm_tracker_worker.dwd_rv_completion_consumer as dwd_completion
from storm_tracker_worker.acquisition_evidence_lifecycle import (
    AcquisitionEvidenceLifecycleStore,
    DWD_COMPLETION_PROOF_SCHEMA,
    DESIRED_AUTHORITY_SCHEMA,
    desired_authority_proof,
)
from storm_tracker_worker.analysis_grid_transform import AnalysisGridTransformConfig
from storm_tracker_worker.canonical_evidence_store import CanonicalEvidenceStore
from storm_tracker_worker.dwd_rv_capture_handoff import (
    HANDOFF_ADAPTER_ID,
    HANDOFF_ADAPTER_VERSION,
    HANDOFF_CONFIG_SHA256,
    SOURCE_ARCHIVE_PRODUCT,
    SOURCE_CAPTURE_PRODUCT,
    SOURCE_COLLECTOR_VERSION,
    SOURCE_PROVIDER,
    adapt_dwd_rv_raw_capture,
)
from storm_tracker_worker.dwd_rv_completion_consumer import (
    CONSUMER_ID,
    STREAM_ID,
    DwdRvCompletionCancelled,
    DwdRvCompletionCapacityError,
    DwdRvCompletionConflict,
    DwdRvCompletionConsumer,
    DwdRvCompletionDeadlineError,
    DwdRvCompletionIntegrityError,
    DwdRvCompletionOutputLimits,
    DwdRvCompletionOutputStore,
    DwdRvPipelineOutcomeAuthority,
    DwdRvShadowChainProfile,
)
from storm_tracker_worker.completion_route_adapters import (
    DwdCompletionRouteAdapter,
    HeavyChildPermit,
    is_retryable_exception,
)
from storm_tracker_worker.dwd_rv_completion_process_isolation import (
    DwdRvCompletionIsolatedRunner,
    DwdRvCompletionIsolationLimits,
    DwdRvProcessIsolationError,
    PROCESS_EFFECT_CONTRACT,
    PROCESS_EFFECT_CONTRACT_SHA256,
    PROCESS_TARGET,
    _dwd_completion_child_entry,
)
from storm_tracker_worker.dwd_rv_quantitative import DwdRvDerivedArtifactStore
from storm_tracker_worker.desired_state import DesiredStateStore, parse_desired_state
from storm_tracker_worker.evidence_to_cells_shadow import EvidenceToCellsShadowConfig
from storm_tracker_worker.raw_artifacts import (
    NATIVE_CAPTURE_STAGE,
    NATIVE_REPRESENTATION,
    RawArtifactReceipt,
    RawArtifactStore,
)
from storm_tracker_worker.hard_decoder_isolation import (
    DecoderProcessResult,
    hard_boundary_support,
)
from storm_tracker_worker.tracking_core.engine.analysis_grid import AnalysisGrid
from storm_tracker_worker.tracking_core.engine.hierarchical_detection import (
    HierarchicalDetectorConfig,
)
from storm_tracker_worker.worker_first_runtime import PipelineOutcome


REPOSITORY = Path(__file__).resolve().parents[1]
LIVE_ARCHIVE = REPOSITORY / ".analysis" / "dwd-rv-live.tar"
BASE_TIME = datetime(2026, 8, 1, 23, 5, tzinfo=timezone.utc).timestamp()
RECEIVED_AT = BASE_TIME + 60.0
EVALUATED_AT = RECEIVED_AT + 30.0
PROJDEF = (
    "+proj=stere +lat_ts=60 +lat_0=90 +lon_0=10 "
    "+x_0=543196.83521776402 +y_0=3622588.8619310022 "
    "+units=m +a=6378137 +b=6356752.3142451802 +no_defs"
)
SYNC = lambda _path: None


def _canonical(value) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _downgrade_output_store_to_v2(path: Path) -> None:
    """Create an exact legacy-v2 boundary from a valid current store."""

    with closing(sqlite3.connect(path)) as database:
        database.execute("BEGIN IMMEDIATE")
        for trigger in (
            "completion_outputs_no_update",
            "completion_outputs_no_delete",
            "store_metadata_no_update",
            "store_metadata_no_delete",
        ):
            database.execute(f"DROP TRIGGER {trigger}")
        database.execute(
            "UPDATE store_metadata SET contract=?,schema_version=?,"
            "limits_sha256=? WHERE singleton=1",
            (
                dwd_completion.LEGACY_RESULT_STORE_CONTRACT,
                dwd_completion.LEGACY_SCHEMA_VERSION,
                _digest({
                    "max_database_bytes": 64 * 1024 * 1024,
                    "max_receipt_bytes": 128 * 1024,
                    "max_receipts": 4_096,
                    "schema": dwd_completion.LEGACY_OUTPUT_LIMITS_SCHEMA,
                }),
            ),
        )
        database.execute(
            "ALTER TABLE store_metadata DROP COLUMN store_instance_sha256"
        )
        database.commit()


def _digest(value) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


class MutableClock:
    def __init__(self, value: float) -> None:
        self.value = value

    def __call__(self) -> float:
        return self.value


class TokenFactory:
    def __init__(self) -> None:
        self.value = 0

    def __call__(self) -> str:
        self.value += 1
        return f"{self.value:064x}"


class FaultOnce:
    def __init__(self, point: str) -> None:
        self.point = point
        self.hit = False

    def __call__(self, point: str) -> None:
        if point == self.point and not self.hit:
            self.hit = True
            raise RuntimeError(f"fault:{point}")


class DdlAttackAtFinalReproof:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.hit = False

    def __call__(self, point: str) -> None:
        if point != "consumer.after_final_reproof_before_ack":
            return
        self.hit = True
        with closing(sqlite3.connect(
            self.path, timeout=0.01, isolation_level=None
        )) as database:
            database.execute("PRAGMA busy_timeout=1")
            database.execute("DROP TRIGGER completion_outputs_no_delete")


class DdlAttackAtAdapterFinalBarrier:
    """Issue the complete trigger-drop/delete/recreate script under the lease."""

    def __init__(self, store: DwdRvCompletionOutputStore) -> None:
        self.path = store.path
        self.hit = False
        trigger = store._trigger_definitions()["completion_outputs_no_delete"]
        self.script = (
            "DROP TRIGGER completion_outputs_no_delete;"
            "DELETE FROM completion_outputs;"
            f"{trigger};"
        )

    def __call__(self, point: str) -> None:
        if point != "adapter.after_final_output_verify_before_barrier":
            return
        self.hit = True
        with closing(sqlite3.connect(
            self.path, timeout=0.01, isolation_level=None
        )) as database:
            database.execute("PRAGMA busy_timeout=1")
            database.executescript(self.script)


class ExpiringMonotonic:
    def __init__(self) -> None:
        self.calls = 0

    def __call__(self) -> float:
        self.calls += 1
        return 0.0 if self.calls == 1 else 2.0


class InlineProcessExecutor:
    """Deterministic unit harness for the fixed child protocol.

    Hard-boundary behaviour itself is exercised in the dedicated process test;
    these consumer tests keep their focus on durable store/lease composition.
    """

    def execute(self, *, target, request, effect_contract, cancel_event):
        assert target == PROCESS_TARGET
        assert effect_contract == PROCESS_EFFECT_CONTRACT
        if cancel_event.is_set():
            return DecoderProcessResult(
                "cancelled", detail_code="cancelled_before_spawn"
            )
        started = time.monotonic()
        result = _dwd_completion_child_entry(request)
        return DecoderProcessResult(
            "succeeded",
            result=result,
            child_exit_code=0,
            elapsed_seconds=max(0.0, time.monotonic() - started),
        )


class TerminalProcessExecutor:
    def __init__(self, result: DecoderProcessResult) -> None:
        self.result = result

    def execute(self, *, target, request, effect_contract, cancel_event):
        assert target == PROCESS_TARGET
        assert request["schema"].endswith("request-v1")
        assert effect_contract == PROCESS_EFFECT_CONTRACT
        return self.result


class CommitThenCrashExecutor:
    def __init__(self) -> None:
        self.calls = 0

    def execute(self, *, target, request, effect_contract, cancel_event):
        self.calls += 1
        assert target == PROCESS_TARGET
        assert effect_contract == PROCESS_EFFECT_CONTRACT
        envelope = _dwd_completion_child_entry(request)
        assert envelope["status"] == "success"
        return DecoderProcessResult(
            "child_crashed",
            detail_code="simulated_crash_after_commit_before_parent_ipc",
            child_exit_code=-9,
            elapsed_seconds=0.1,
        )


class CaptureRequestExecutor:
    def __init__(self) -> None:
        self.request = None

    def execute(self, *, target, request, effect_contract, cancel_event):
        assert target == PROCESS_TARGET
        assert effect_contract == PROCESS_EFFECT_CONTRACT
        self.request = json.loads(json.dumps(request))
        return DecoderProcessResult(
            "invalid_request",
            detail_code="captured_without_spawn",
            elapsed_seconds=0.0,
        )


def _safe_receipt(receipt: RawArtifactReceipt) -> dict:
    return {
        "content_sha256": receipt.content_sha256,
        "content_type": receipt.content_type,
        "created_at": receipt.created_at,
        "fetch_id": receipt.fetch_id,
        "product": receipt.product,
        "provenance": receipt.provenance,
        "provenance_sha256": receipt.provenance_sha256,
        "provider": receipt.provider,
        "receipt_id": receipt.receipt_id,
        "receipt_sha256": receipt.receipt_sha256,
        "received_at": receipt.received_at,
        "schema": "stv3-worker-raw-receipt-safe-evidence-v1",
        "size_bytes": receipt.size_bytes,
        "upstream_id": receipt.upstream_id,
    }


def _profile(
    *,
    shield_threshold: float = 1.0,
    max_runtime_seconds: float = 60.0,
) -> DwdRvShadowChainProfile:
    return DwdRvShadowChainProfile(
        target_grid=AnalysisGrid(
            grid_id="dwd-rv-completion-test-top-left-2x2",
            crs=PROJDEF,
            origin_x_m=-500.0,
            origin_y_m=500.0,
            x_resolution_m=1000.0,
            y_resolution_m=1000.0,
            width=2,
            height=2,
        ),
        transform_config=AnalysisGridTransformConfig(
            max_source_pixels=1_500_000,
            max_target_pixels=16,
            max_array_bytes=256 * 1024 * 1024,
            max_overlap_contributions=2_000_000,
        ),
        detector_config=HierarchicalDetectorConfig(
            shield_threshold=shield_threshold,
            core_threshold=4.0,
            minimum_shield_area_m2=1_000_000.0,
            minimum_core_area_m2=1_000_000.0,
            shield_position_sigma_m=500.0,
            core_position_sigma_m=350.0,
            connectivity=8,
            centroid_method="geometric",
        ),
        pipeline_config=EvidenceToCellsShadowConfig(
            max_target_pixels=16,
            max_detection_objects=32,
            max_receipt_bytes=4 * 1024 * 1024,
            max_runtime_seconds=max_runtime_seconds,
        ),
    )


def _desired():
    return parse_desired_state({
        "authority_epoch": 1,
        "contract": "stv3-worker-desired-state-v1",
        "generation": 1,
        "issued_at": EVALUATED_AT - 60.0,
        "previous_state_sha256": None,
        "producer_id": "ha-dwd-consumer-test",
        "settings": {
            "ground_radius_km": 20.0,
            "lightning_source_mode": "auto",
            "model_guidance_enabled": True,
            "region_sharing_distance_km": 150.0,
        },
        "targets": [{
            "available": True,
            "country_code": "BE",
            "display_name": "Home",
            "kind": "home",
            "latitude": 51.04,
            "location_accuracy_m": None,
            "longitude": 4.48,
            "observation_radius_km": 250.0,
            "position_observed_at": EVALUATED_AT - 60.0,
            "target_id": "zone.home",
        }],
    })


def _source_and_handoff(
    tmp_path: Path,
    *,
    source_payload: bytes | None = None,
):
    payload = LIVE_ARCHIVE.read_bytes() if source_payload is None else source_payload
    digest = hashlib.sha256(payload).hexdigest()
    source = RawArtifactStore((tmp_path / "source-raw").resolve())
    source.initialize()
    source_receipt = source.store(
        provider=SOURCE_PROVIDER,
        product=SOURCE_ARCHIVE_PRODUCT,
        fetch_id="dwd-consumer-fixture:rv_tar",
        content_type="application/x-tar",
        received_at=RECEIVED_AT,
        upstream_id=f"{SOURCE_ARCHIVE_PRODUCT}|latest|content_sha256={digest}",
        provenance={
            "capture_stage": NATIVE_CAPTURE_STAGE,
            "collector": "dwd_open_data_composite_rv",
            "collector_version": SOURCE_COLLECTOR_VERSION,
            "provenance_scope": "exact_upstream_tar_bytes_only",
            "representation": NATIVE_REPRESENTATION,
            "structure_inspector_version": "dwd-rv-tar-layout-only-v1",
        },
        payload=payload,
    )
    source_capture = source.commit_capture(
        provider=SOURCE_PROVIDER,
        product=SOURCE_CAPTURE_PRODUCT,
        fetch_id="dwd-consumer-fixture:capture",
        upstream_capture_id=(
            f"{SOURCE_CAPTURE_PRODUCT}"
            "|member=composite_rv_20260801_2305_000-hd5"
            f"|content_sha256={digest}"
        ),
        product_timestamp=BASE_TIME,
        members={"rv_tar": source_receipt.receipt_id},
    )
    handoff = RawArtifactStore((tmp_path / "handoff-raw").resolve())
    handoff.initialize()
    adapted = adapt_dwd_rv_raw_capture(
        source_capture_id=source_capture.capture_id,
        source_raw_store=source,
        handoff_raw_store=handoff,
    )
    target_bundle = handoff.load_capture_bundle_bytes(adapted.target_capture_id)
    assert target_bundle is not None
    target_capture, target_receipts, target_payloads = target_bundle
    return (
        payload,
        source_capture,
        source_receipt,
        handoff,
        adapted,
        target_capture,
        target_receipts,
        target_payloads,
    )


def _proof(
    *,
    desired_sha: str,
    authority: dict,
    source_capture,
    source_receipt,
    adapted,
    target_capture,
    target_receipts,
    workload_id: str = STREAM_ID,
) -> dict:
    return {
        "adapter": {
            "adapter_config_sha256": HANDOFF_CONFIG_SHA256,
            "adapter_id": HANDOFF_ADAPTER_ID,
            "adapter_version": HANDOFF_ADAPTER_VERSION,
            "receipt": asdict(adapted),
        },
        "capacity_receipt": {"schema": "synthetic-capacity-proof-v1"},
        "desired_authority": authority,
        "execution_receipts": [],
        "handoff_capture": target_capture.to_metadata(),
        "handoff_receipts": {
            role: _safe_receipt(receipt)
            for role, receipt in target_receipts.items()
        },
        "meteorological_claim": False,
        "observation_policy": "base_plus000_only",
        "raw_refetch_performed": False,
        "scheduler_terminal": {
            "desired_state_sha256": desired_sha,
            "last_error_code": None,
            "last_transition": "completed",
            "state": "completed",
            "terminal_at": EVALUATED_AT,
        },
        "schema": DWD_COMPLETION_PROOF_SCHEMA,
        "source_capture": source_capture.to_metadata(),
        "source_receipts": {"rv_tar": _safe_receipt(source_receipt)},
        "workload": {
            "job_id": "job-dwd-completion-test",
            "product_id": "dwd_radolan_rv_native_tar",
            "provider_id": "dwd",
            "secret_requirement_ids": [],
            "snapshot_sha256": desired_sha,
            "workload_id": workload_id,
        },
    }


def _environment(
    tmp_path: Path,
    *,
    consumer_fault=None,
    monotonic=time.monotonic,
    output_limits=None,
    workload_id: str = STREAM_ID,
    profile: DwdRvShadowChainProfile | None = None,
    process_limits: DwdRvCompletionIsolationLimits | None = None,
    process_executor=None,
    hard_process: bool = False,
    output_fault=None,
    source_payload: bytes | None = None,
):
    (
        payload,
        source_capture,
        source_receipt,
        handoff,
        adapted,
        target_capture,
        target_receipts,
        _target_payloads,
    ) = _source_and_handoff(tmp_path, source_payload=source_payload)
    clock = MutableClock(EVALUATED_AT + 100.0)
    lifecycle = AcquisitionEvidenceLifecycleStore(
        (tmp_path / "lifecycle").resolve(),
        clock=clock,
        token_factory=TokenFactory(),
        directory_fsync=SYNC,
    )
    desired = _desired()
    desired_sha = desired.state_sha256
    authority = desired_authority_proof(desired)
    authority_sha = authority["authority_sha256"]
    producer = lifecycle.publish(
        acquisition_job_id="job-dwd-completion-test",
        consumer_id=CONSUMER_ID,
        stream_id=STREAM_ID,
        desired_state_sha256=desired_sha,
        authority_sha256=authority_sha,
        proof=_proof(
            desired_sha=desired_sha,
            authority=authority,
            source_capture=source_capture,
            source_receipt=source_receipt,
            adapted=adapted,
            target_capture=target_capture,
            target_receipts=target_receipts,
            workload_id=workload_id,
        ),
        payload=payload,
    )
    artifacts = DwdRvDerivedArtifactStore((tmp_path / "derived").resolve())
    evidence = CanonicalEvidenceStore(
        (tmp_path / "canonical.sqlite3").resolve(),
        raw_artifact_reader=handoff,
        clock=clock,
    )
    outputs = DwdRvCompletionOutputStore(
        (tmp_path / "outputs.sqlite3").resolve(),
        limits=output_limits,
        clock=clock,
        fault_injector=output_fault,
    )
    profile = profile or _profile()
    runner = DwdRvCompletionIsolatedRunner(
        handoff_raw_store=handoff,
        artifact_store=artifacts,
        evidence_store=evidence,
        output_store=outputs,
        profile=profile,
        limits=process_limits or DwdRvCompletionIsolationLimits(),
    )
    if process_executor is not None:
        runner._executor = process_executor
    elif not hard_process:
        runner._executor = InlineProcessExecutor()
    consumer = DwdRvCompletionConsumer(
        lifecycle_store=lifecycle,
        handoff_raw_store=handoff,
        artifact_store=artifacts,
        evidence_store=evidence,
        output_store=outputs,
        profile=profile,
        process_runner=runner,
        clock=clock,
        monotonic=monotonic,
        fault_injector=consumer_fault,
    )
    return {
        "artifacts": artifacts,
        "clock": clock,
        "consumer": consumer,
        "desired": desired,
        "evidence": evidence,
        "handoff": handoff,
        "lifecycle": lifecycle,
        "outputs": outputs,
        "producer": producer,
        "profile": profile,
        "runner": runner,
    }


def _reopen_consumer(env, *, profile=None, fault=None, monotonic=time.monotonic):
    lifecycle = AcquisitionEvidenceLifecycleStore(
        env["lifecycle"].root,
        clock=env["clock"],
        token_factory=TokenFactory(),
        directory_fsync=SYNC,
    )
    outputs = DwdRvCompletionOutputStore(
        env["outputs"].path,
        clock=env["clock"],
    )
    reopened_profile = profile or env["profile"]
    runner = DwdRvCompletionIsolatedRunner(
        handoff_raw_store=env["handoff"],
        artifact_store=env["artifacts"],
        evidence_store=env["evidence"],
        output_store=outputs,
        profile=reopened_profile,
        limits=env["runner"].limits,
    )
    runner._executor = InlineProcessExecutor()
    return DwdRvCompletionConsumer(
        lifecycle_store=lifecycle,
        handoff_raw_store=env["handoff"],
        artifact_store=env["artifacts"],
        evidence_store=env["evidence"],
        output_store=outputs,
        profile=reopened_profile,
        process_runner=runner,
        clock=env["clock"],
        monotonic=monotonic,
        fault_injector=fault,
    )


def test_real_completion_reaches_durable_unadmitted_cells_output(tmp_path):
    env = _environment(tmp_path)
    result = env["consumer"].run_once("consumer-owner", env["desired"])

    assert result.disposition == "processed"
    assert result.event_id == env["producer"].event_id
    assert result.output_receipt is not None
    assert result.consumption_receipt is not None
    assert (
        result.consumption_receipt.output_sha256
        == result.output_receipt.output_sha256
    )
    output = result.output_receipt.output
    assert output["handoff"]["observation_policy"] == "base_plus000_only"
    assert output["shadow"]["admission_status"] == "shadow_candidate_unadmitted"
    assert output["shadow"]["operational_admission_blockers"] == [
        "hard_process_isolation_not_wired"
    ]
    assert set(output["claims"].values()) == {False}
    assert env["artifacts"].object_count() == 1
    assert env["evidence"].stats() == {"decoder_runs": 1, "member_links": 1}
    assert env["outputs"].diagnostics()["receipts"] == 1
    assert env["lifecycle"].diagnostics()["outbox_states"]["consumed"] == 1
    text = json.dumps(result.output_receipt.as_dict(), sort_keys=True)
    assert str(tmp_path) not in text
    assert "artifact_path" not in text
    assert "password" not in text.casefold()
    assert len(json.dumps(env["outputs"].diagnostics())) < 4096


def test_process_leased_never_claims_or_acks_and_store_reproves_every_field(
    tmp_path,
):
    env = _environment(tmp_path)
    lease = env["lifecycle"].claim_next(
        CONSUMER_ID,
        STREAM_ID,
        "coordinator-owner",
        lease_seconds=120.0,
        now=env["clock"](),
    )
    assert lease is not None
    outcome = env["consumer"].process_leased(
        lease,
        env["desired"],
        threading.Event(),
    )
    assert isinstance(outcome, PipelineOutcome)
    assert outcome.disposition == "withheld"
    assert outcome.fact_revision_staged is False
    assert env["lifecycle"].diagnostics()["outbox_states"]["leased"] == 1
    assert env["lifecycle"].diagnostics()["outbox_states"]["consumed"] == 0
    durable = env["outputs"].load(lease.event_id)
    assert durable is not None

    verified = env["outputs"].verify_for_completion(
        event_id=lease.event_id,
        event_sha256=lease.event_sha256,
        desired_state_sha256=env["desired"].state_sha256,
        authority_sha256=lease.authority_sha256,
        consumer_id=CONSUMER_ID,
        stream_id=STREAM_ID,
        durable_output_receipt_sha256=durable.receipt_sha256,
        durable_process_receipt_sha256=outcome.process_receipt_sha256,
        process_isolation_authority_sha256=env["runner"].authority_sha256,
        output_sha256=outcome.output_sha256,
        disposition=outcome.disposition,
        staged_fact_revision=None,
    )
    assert verified.output_sha256 == outcome.output_sha256
    assert verified.fact_revision_sha256 is None
    authority = DwdRvPipelineOutcomeAuthority(
        output_store=env["outputs"],
        profile=env["profile"],
        process_isolation_authority_sha256=env["runner"].authority_sha256,
    )
    coordinator_proof = authority.verify(
        lease=lease,
        desired_state=env["desired"],
        outcome=outcome,
    )
    assert coordinator_proof.event_id == lease.event_id
    assert coordinator_proof.event_sha256 == lease.event_sha256
    assert (
        coordinator_proof.producer_receipt_sha256
        == lease.producer_receipt_sha256
    )
    assert coordinator_proof.desired_state_sha256 == env["desired"].state_sha256
    assert coordinator_proof.authority_epoch == env["desired"].authority_epoch
    assert coordinator_proof.authority_sha256 == lease.authority_sha256
    assert coordinator_proof.consumer_id == CONSUMER_ID
    assert coordinator_proof.stream_id == STREAM_ID
    assert coordinator_proof.stream_sequence == lease.stream_sequence
    assert coordinator_proof.output_sha256 == outcome.output_sha256
    assert coordinator_proof.output_receipt_sha256 == durable.receipt_sha256
    assert coordinator_proof.process_receipt_sha256 == outcome.process_receipt_sha256
    assert coordinator_proof.output_receipt_sha256 != outcome.process_receipt_sha256
    assert coordinator_proof.disposition == "withheld"
    assert coordinator_proof.verifier_profile_sha256 == authority.fingerprint
    assert coordinator_proof.fact_binding is None
    with pytest.raises(DwdRvCompletionConflict, match="durable DWD output"):
        DwdRvPipelineOutcomeAuthority(
            output_store=env["outputs"],
            profile=_profile(shield_threshold=2.0),
            process_isolation_authority_sha256=env["runner"].authority_sha256,
        ).verify(
            lease=lease,
            desired_state=env["desired"],
            outcome=outcome,
        )
    with pytest.raises(DwdRvCompletionConflict, match="durable DWD output"):
        authority.verify(
            lease=lease,
            desired_state=env["desired"],
            outcome=PipelineOutcome(
                output_sha256="f" * 64,
                disposition=outcome.disposition,
                fact_revision_staged=False,
                process_receipt_sha256=outcome.process_receipt_sha256,
            ),
        )
    with pytest.raises(DwdRvCompletionConflict, match="completion proof"):
        env["outputs"].verify_for_completion(
            event_id=lease.event_id,
            event_sha256=lease.event_sha256,
            desired_state_sha256=env["desired"].state_sha256,
            authority_sha256="f" * 64,
            consumer_id=CONSUMER_ID,
            stream_id=STREAM_ID,
            durable_output_receipt_sha256=durable.receipt_sha256,
            durable_process_receipt_sha256=outcome.process_receipt_sha256,
            process_isolation_authority_sha256=env["runner"].authority_sha256,
            output_sha256=outcome.output_sha256,
            disposition=outcome.disposition,
            staged_fact_revision=None,
        )
    with pytest.raises(DwdRvCompletionConflict, match="completion proof"):
        env["outputs"].verify_for_completion(
            event_id=lease.event_id,
            event_sha256=lease.event_sha256,
            desired_state_sha256=env["desired"].state_sha256,
            authority_sha256=lease.authority_sha256,
            consumer_id=CONSUMER_ID,
            stream_id=STREAM_ID,
            durable_output_receipt_sha256=durable.receipt_sha256,
            durable_process_receipt_sha256=outcome.process_receipt_sha256,
            process_isolation_authority_sha256=env["runner"].authority_sha256,
            output_sha256=outcome.output_sha256,
            disposition=outcome.disposition,
            staged_fact_revision={
                "pipeline_item_id": f"completion:{lease.event_id}",
                "record_sha256": "a" * 64,
                "revision_sha256": "b" * 64,
            },
        )
    receipt = env["lifecycle"].acknowledge(
        lease.event_id,
        lease.lease_token,
        output_sha256=verified.output_sha256,
        now=env["clock"](),
    )
    assert receipt.output_sha256 == verified.output_sha256


def test_process_leased_honours_coordinator_cancellation_before_decode(tmp_path):
    env = _environment(tmp_path)
    lease = env["lifecycle"].claim_next(
        CONSUMER_ID,
        STREAM_ID,
        "coordinator-owner",
        lease_seconds=120.0,
        now=env["clock"](),
    )
    assert lease is not None
    cancelled = threading.Event()
    cancelled.set()
    with pytest.raises(DwdRvCompletionCancelled):
        env["consumer"].process_leased(lease, env["desired"], cancelled)
    assert env["artifacts"].object_count() == 0
    assert env["outputs"].diagnostics()["receipts"] == 0
    assert env["lifecycle"].diagnostics()["outbox_states"]["leased"] == 1


def test_crash_after_output_commit_recovers_without_second_chain_run(
    tmp_path,
    monkeypatch,
):
    fault = FaultOnce("consumer.after_output_commit")
    env = _environment(tmp_path, consumer_fault=fault)
    with pytest.raises(RuntimeError, match="after_output_commit"):
        env["consumer"].run_once("consumer-owner", env["desired"])

    durable = env["outputs"].load(env["producer"].event_id)
    assert durable is not None
    assert env["lifecycle"].diagnostics()["outbox_states"]["leased"] == 1
    assert env["evidence"].stats()["decoder_runs"] == 1

    def forbidden_chain(**_kwargs):
        raise AssertionError("durable output replay must not rerun the chain")

    monkeypatch.setattr(
        "storm_tracker_worker.dwd_rv_completion_process_isolation._execute_chain",
        forbidden_chain,
    )
    env["clock"].value += 121.0
    recovered = _reopen_consumer(env).run_once(
        "restart-owner", env["desired"]
    )
    assert recovered.disposition == "recovered_output"
    assert recovered.output_receipt == durable
    assert recovered.consumption_receipt is not None
    assert recovered.consumption_receipt.output_sha256 == durable.output_sha256
    assert _reopen_consumer(env).run_once(
        "duplicate-owner", env["desired"]
    ).disposition == "idle"


def test_crash_after_decoder_is_idempotent_on_restart(tmp_path, monkeypatch):
    from storm_tracker_worker import dwd_rv_completion_process_isolation as isolated

    original = isolated._execute_chain
    fault = FaultOnce("consumer.after_decode")

    def faulting_chain(**kwargs):
        kwargs["fault"] = fault
        return original(**kwargs)

    monkeypatch.setattr(isolated, "_execute_chain", faulting_chain)
    env = _environment(tmp_path)
    with pytest.raises(
        RuntimeError, match="child_returned_without_durable_process_receipt"
    ):
        env["consumer"].run_once("consumer-owner", env["desired"])
    assert env["artifacts"].object_count() == 1
    assert env["evidence"].stats()["decoder_runs"] == 1
    assert env["outputs"].diagnostics()["receipts"] == 0

    env["clock"].value += 121.0
    recovered = _reopen_consumer(env).run_once(
        "restart-owner", env["desired"]
    )
    assert recovered.disposition == "processed"
    assert env["artifacts"].object_count() == 1
    assert env["evidence"].stats()["decoder_runs"] == 1
    assert env["outputs"].diagnostics()["receipts"] == 1


def test_expired_deadline_never_decodes_persists_or_acknowledges(tmp_path):
    env = _environment(
        tmp_path,
        profile=_profile(max_runtime_seconds=1.0),
        process_limits=DwdRvCompletionIsolationLimits(
            wall_seconds=1.0,
            cpu_seconds=1.0,
        ),
        process_executor=TerminalProcessExecutor(
            DecoderProcessResult(
                "deadline_exceeded",
                detail_code="hard_wall_deadline",
                child_exit_code=-1,
                elapsed_seconds=1.0,
            )
        ),
    )
    with pytest.raises(DwdRvCompletionDeadlineError):
        env["consumer"].run_once(
            "consumer-owner", env["desired"], lease_seconds=10.0
        )
    assert env["artifacts"].object_count() == 0
    assert env["evidence"].stats()["decoder_runs"] == 0
    assert env["outputs"].diagnostics()["receipts"] == 0
    assert env["lifecycle"].diagnostics()["outbox_states"]["leased"] == 1

    env["clock"].value += 11.0
    replay = env["lifecycle"].claim_next(
        CONSUMER_ID,
        STREAM_ID,
        "retry-owner",
        lease_seconds=10.0,
        now=env["clock"](),
    )
    assert replay is not None and replay.attempt_count == 2


def test_profile_drift_after_durable_output_fails_closed_then_exact_recovers(
    tmp_path,
):
    env = _environment(
        tmp_path,
        consumer_fault=FaultOnce("consumer.after_output_commit"),
    )
    with pytest.raises(RuntimeError):
        env["consumer"].run_once("consumer-owner", env["desired"])
    env["clock"].value += 121.0

    with pytest.raises(DwdRvCompletionConflict, match="durable.*output"):
        _reopen_consumer(
            env,
            profile=_profile(shield_threshold=1.1),
        ).run_once("drifted-owner", env["desired"])
    env["clock"].value += 121.0
    exact = _reopen_consumer(env).run_once("exact-owner", env["desired"])
    assert exact.disposition == "recovered_output"


def test_final_reproof_lease_blocks_trigger_drop_delete_ack_race(tmp_path):
    attack = DdlAttackAtFinalReproof(
        (tmp_path / "outputs.sqlite3").resolve()
    )
    env = _environment(tmp_path, consumer_fault=attack)

    with pytest.raises(sqlite3.OperationalError, match="locked"):
        env["consumer"].run_once("consumer-owner", env["desired"])

    assert attack.hit
    assert env["outputs"].diagnostics()["receipts"] == 1
    assert env["lifecycle"].diagnostics()["outbox_states"]["leased"] == 1
    with closing(sqlite3.connect(env["outputs"].path)) as database:
        assert database.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='trigger' "
            "AND name='completion_outputs_no_delete'"
        ).fetchone()[0] == 1

    env["clock"].value += 121.0
    recovered = _reopen_consumer(env).run_once(
        "recovery-owner", env["desired"]
    )
    assert recovered.disposition == "recovered_output"
    assert env["lifecycle"].diagnostics()["outbox_states"]["consumed"] == 1


def test_real_dwd_route_adapter_fences_ddl_until_exact_recovery_ack(tmp_path):
    env = _environment(tmp_path)
    desired_store = DesiredStateStore(
        (tmp_path / "adapter-desired.sqlite3").resolve()
    )
    desired_store.claim_producer(
        env["desired"].producer_id,
        "dwd-adapter-ddl-test-claim",
        claimed_at=EVALUATED_AT - 120.0,
    )
    desired_store.apply(
        env["desired"].canonical_bytes,
        received_at=EVALUATED_AT,
    )
    lease = env["lifecycle"].claim_next(
        CONSUMER_ID,
        STREAM_ID,
        "serial-dispatcher-owner-one",
        lease_seconds=300.0,
        now=env["clock"](),
    )
    assert lease is not None
    adapter = DwdCompletionRouteAdapter(
        consumer=env["consumer"],
        lifecycle_store=env["lifecycle"],
        desired_state_store=desired_store,
        clock=env["clock"],
    )
    attack = DdlAttackAtAdapterFinalBarrier(env["outputs"])
    env["consumer"]._fault_injector = attack
    nonce = object()
    permit = HeavyChildPermit(
        tick_sha256=_digest("real-dwd-adapter-ddl-tick-one"),
        expected_workload_id=STREAM_ID,
        expected_phase="single",
        nonce=nonce,
    )

    with pytest.raises(sqlite3.OperationalError, match="locked"):
        adapter.begin(
            lease,
            permit=permit,
            permit_nonce=nonce,
            cancel_event=threading.Event(),
        )

    assert attack.hit and permit.consumed
    assert env["outputs"].diagnostics()["receipts"] == 1
    assert env["lifecycle"].diagnostics()["outbox_states"]["leased"] == 1
    with closing(sqlite3.connect(env["outputs"].path)) as database:
        assert database.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='trigger' "
            "AND name='completion_outputs_no_delete'"
        ).fetchone()[0] == 1
        assert database.execute(
            "SELECT count(*) FROM completion_outputs"
        ).fetchone()[0] == 1

    env["consumer"]._fault_injector = None
    env["clock"].value += 301.0
    reclaimed = env["lifecycle"].claim_next(
        CONSUMER_ID,
        STREAM_ID,
        "serial-dispatcher-owner-two",
        lease_seconds=300.0,
        now=env["clock"](),
    )
    assert reclaimed is not None and reclaimed.attempt_count == 2
    recovery_nonce = object()
    recovery_permit = HeavyChildPermit(
        tick_sha256=_digest("real-dwd-adapter-ddl-tick-two"),
        expected_workload_id=STREAM_ID,
        expected_phase="single",
        nonce=recovery_nonce,
    )
    recovered = adapter.begin(
        reclaimed,
        permit=recovery_permit,
        permit_nonce=recovery_nonce,
        cancel_event=threading.Event(),
    )
    assert recovered.status == "acknowledged"
    assert recovery_permit.consumed
    assert env["outputs"].diagnostics()["receipts"] == 1
    assert env["evidence"].stats()["decoder_runs"] == 1
    terminal = env["lifecycle"].terminal_event_identity(STREAM_ID, 1)
    assert terminal is not None
    assert terminal.terminal_kind == "consumed"
    assert terminal.event_sha256 == reclaimed.event_sha256
    with closing(sqlite3.connect(env["lifecycle"].path)) as database:
        assert database.execute(
            "SELECT count(*) FROM consumption_receipts"
        ).fetchone()[0] == 1
    assert env["lifecycle"].claim_next(
        CONSUMER_ID,
        STREAM_ID,
        "serial-dispatcher-owner-three",
        lease_seconds=300.0,
        now=env["clock"](),
    ) is None


@pytest.mark.skipif(
    os.name != "posix",
    reason="POSIX permits rename over an open SQLite database path",
)
def test_real_dwd_adapter_posix_path_replace_never_reports_silent_success(
    tmp_path,
):
    env = _environment(tmp_path)
    desired_store = DesiredStateStore(
        (tmp_path / "posix-adapter-desired.sqlite3").resolve()
    )
    desired_store.claim_producer(
        env["desired"].producer_id,
        "dwd-posix-replace-test-claim",
        claimed_at=EVALUATED_AT - 120.0,
    )
    desired_store.apply(
        env["desired"].canonical_bytes,
        received_at=EVALUATED_AT,
    )
    lease = env["lifecycle"].claim_next(
        CONSUMER_ID,
        STREAM_ID,
        "serial-dispatcher-posix-owner",
        lease_seconds=300.0,
        now=env["clock"](),
    )
    assert lease is not None
    replacement = DwdRvCompletionOutputStore(
        (tmp_path / "posix-replacement-output.sqlite3").resolve(),
        clock=env["clock"],
    )
    replacement_instance = replacement.store_instance_sha256
    replaced = False

    def replace_path(point: str) -> None:
        nonlocal replaced
        if point != "adapter.after_final_output_verify_before_barrier":
            return
        os.replace(replacement.path, env["outputs"].path)
        replaced = True

    env["consumer"]._fault_injector = replace_path
    adapter = DwdCompletionRouteAdapter(
        consumer=env["consumer"],
        lifecycle_store=env["lifecycle"],
        desired_state_store=desired_store,
        clock=env["clock"],
    )
    nonce = object()
    permit = HeavyChildPermit(
        tick_sha256=_digest("real-dwd-posix-replace-tick"),
        expected_workload_id=STREAM_ID,
        expected_phase="single",
        nonce=nonce,
    )

    with pytest.raises(DwdRvCompletionIntegrityError) as captured:
        adapter.begin(
            lease,
            permit=permit,
            permit_nonce=nonce,
            cancel_event=threading.Event(),
        )

    assert replaced and permit.consumed
    assert not is_retryable_exception(captured.value)
    terminal = env["lifecycle"].terminal_event_identity(STREAM_ID, 1)
    assert terminal is None
    assert env["lifecycle"].diagnostics()["outbox_states"]["leased"] == 1
    assert env["lifecycle"].diagnostics()["outbox_states"]["consumed"] == 0
    visible = DwdRvCompletionOutputStore(
        env["outputs"].path,
        clock=env["clock"],
    )
    assert visible.store_instance_sha256 == replacement_instance
    assert visible.load(lease.event_id) is None


def test_wrong_workload_proof_fails_before_decode_or_ack(tmp_path):
    env = _environment(tmp_path, workload_id="radar.dwd.wrong")
    with pytest.raises(DwdRvCompletionIntegrityError, match="workload"):
        env["consumer"].run_once("consumer-owner", env["desired"])
    assert env["artifacts"].object_count() == 0
    assert env["evidence"].stats()["decoder_runs"] == 0
    assert env["outputs"].diagnostics()["receipts"] == 0
    assert env["lifecycle"].diagnostics()["outbox_states"]["leased"] == 1


def test_output_store_tamper_and_capacity_are_explicit(tmp_path):
    env = _environment(tmp_path)
    processed = env["consumer"].run_once("consumer-owner", env["desired"])
    assert processed.output_receipt is not None
    with closing(sqlite3.connect(env["outputs"].path)) as database:
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            database.execute(
                "UPDATE completion_outputs SET output_sha256=? WHERE event_id=?",
                ("f" * 64, processed.event_id),
            )
        database.rollback()
        database.execute("DROP TRIGGER completion_outputs_no_update")
        database.execute(
            "UPDATE completion_outputs SET output_sha256=? WHERE event_id=?",
            ("f" * 64, processed.event_id),
        )
        database.execute(
            env["outputs"]._trigger_definitions()[
                "completion_outputs_no_update"
            ]
        )
        database.commit()
    with pytest.raises(DwdRvCompletionIntegrityError, match="checksum"):
        env["outputs"].load(processed.event_id)

    capped = DwdRvCompletionOutputStore(
        (tmp_path / "capped.sqlite3").resolve(),
        limits=DwdRvCompletionOutputLimits(max_receipts=1),
        clock=env["clock"],
    )
    first_document = processed.output_receipt.output
    capped.commit(output=first_document)
    second_document = json.loads(json.dumps(first_document))
    second_document["event"]["event_id"] = "completion-" + "e" * 64
    second_document["event"]["event_sha256"] = "e" * 64
    with pytest.raises(DwdRvCompletionCapacityError, match="cap"):
        capped.commit(output=second_document)

    tiny = DwdRvCompletionOutputStore(
        (tmp_path / "tiny.sqlite3").resolve(),
        limits=DwdRvCompletionOutputLimits(max_receipt_bytes=1_024),
        clock=env["clock"],
    )
    oversized = json.loads(json.dumps(first_document))
    oversized["shadow"]["operational_admission_blockers"] = ["x" * 2_000]
    with pytest.raises(DwdRvCompletionCapacityError, match="byte cap"):
        tiny.commit(output=oversized)


def test_output_store_v2_migrates_atomically_and_preserves_receipt(tmp_path):
    env = _environment(tmp_path)
    result = env["consumer"].run_once("consumer-owner", env["desired"])
    assert result.output_receipt is not None
    expected_output_sha = result.output_receipt.output_sha256
    path = env["outputs"].path
    _downgrade_output_store_to_v2(path)

    migrated = DwdRvCompletionOutputStore(path, clock=env["clock"])
    loaded = migrated.load(result.event_id)
    assert loaded is not None
    assert loaded.output_sha256 == expected_output_sha
    assert len(migrated.store_instance_sha256) == 64
    identity = migrated.identity_sha256
    with closing(sqlite3.connect(path)) as database:
        metadata = database.execute(
            "SELECT contract,schema_version,store_instance_sha256 "
            "FROM store_metadata WHERE singleton=1"
        ).fetchone()
        triggers = {
            str(row[0]) for row in database.execute(
                "SELECT name FROM sqlite_master WHERE type='trigger'"
            )
        }
    assert metadata == (
        dwd_completion.RESULT_STORE_CONTRACT,
        dwd_completion.SCHEMA_VERSION,
        migrated.store_instance_sha256,
    )
    assert triggers == set(migrated._trigger_definitions())
    assert DwdRvCompletionOutputStore(
        path, clock=env["clock"]
    ).identity_sha256 == identity


def test_output_store_v2_migration_faults_are_old_or_new(tmp_path):
    rollback_path = (tmp_path / "migration-rollback.sqlite3").resolve()
    DwdRvCompletionOutputStore(rollback_path)
    _downgrade_output_store_to_v2(rollback_path)
    with pytest.raises(RuntimeError, match="migration.after_metadata"):
        DwdRvCompletionOutputStore(
            rollback_path,
            fault_injector=FaultOnce("migration.after_metadata"),
        )
    with closing(sqlite3.connect(rollback_path)) as database:
        metadata = database.execute(
            "SELECT contract,schema_version FROM store_metadata WHERE singleton=1"
        ).fetchone()
        columns = {
            str(row[1]) for row in database.execute(
                "PRAGMA table_info(store_metadata)"
            )
        }
    assert metadata == (
        dwd_completion.LEGACY_RESULT_STORE_CONTRACT,
        dwd_completion.LEGACY_SCHEMA_VERSION,
    )
    assert "store_instance_sha256" not in columns
    rolled_forward = DwdRvCompletionOutputStore(rollback_path)
    assert len(rolled_forward.store_instance_sha256) == 64

    committed_path = (tmp_path / "migration-committed.sqlite3").resolve()
    DwdRvCompletionOutputStore(committed_path)
    _downgrade_output_store_to_v2(committed_path)
    with pytest.raises(RuntimeError, match="migration.after_commit"):
        DwdRvCompletionOutputStore(
            committed_path,
            fault_injector=FaultOnce("migration.after_commit"),
        )
    committed = DwdRvCompletionOutputStore(committed_path)
    assert len(committed.store_instance_sha256) == 64


def test_output_store_metadata_and_same_path_replacement_fail_closed(tmp_path):
    path = (tmp_path / "identity.sqlite3").resolve()
    store = DwdRvCompletionOutputStore(path)
    instance_sha = store.store_instance_sha256
    identity_sha = store.identity_sha256
    with closing(sqlite3.connect(path)) as database:
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            database.execute(
                "UPDATE store_metadata SET store_instance_sha256=? "
                "WHERE singleton=1",
                ("f" * 64,),
            )
        database.rollback()
        database.execute("DROP TRIGGER store_metadata_no_update")
        database.execute(
            "UPDATE store_metadata SET store_instance_sha256=? WHERE singleton=1",
            ("f" * 64,),
        )
        database.execute(
            store._trigger_definitions()["store_metadata_no_update"]
        )
        database.commit()
    with pytest.raises(DwdRvCompletionIntegrityError, match="instance authority"):
        store.diagnostics()

    replacement_path = (tmp_path / "replacement.sqlite3").resolve()
    replacement = DwdRvCompletionOutputStore(replacement_path)
    replacement_instance = replacement.store_instance_sha256
    assert replacement_instance != instance_sha
    os.replace(replacement.path, path)
    with pytest.raises(DwdRvCompletionIntegrityError, match="instance changed"):
        _ = store.identity_sha256
    reopened = DwdRvCompletionOutputStore(path)
    assert reopened.store_instance_sha256 == replacement_instance
    assert reopened.identity_sha256 != identity_sha


def test_pipeline_authority_binds_exact_output_store_instance(tmp_path):
    first = DwdRvCompletionOutputStore((tmp_path / "first.sqlite3").resolve())
    second = DwdRvCompletionOutputStore((tmp_path / "second.sqlite3").resolve())
    profile = _profile()
    isolation_sha = _digest("process-isolation-authority")
    first_authority = DwdRvPipelineOutcomeAuthority(
        output_store=first,
        profile=profile,
        process_isolation_authority_sha256=isolation_sha,
    )
    second_authority = DwdRvPipelineOutcomeAuthority(
        output_store=second,
        profile=profile,
        process_isolation_authority_sha256=isolation_sha,
    )
    assert first.store_instance_sha256 != second.store_instance_sha256
    assert first_authority.fingerprint != second_authority.fingerprint


def test_two_runtimes_share_single_lifecycle_lease(tmp_path):
    env = _environment(tmp_path)
    first = env["consumer"]
    second = _reopen_consumer(env)
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = tuple(
            pool.map(
                lambda item: item[0].run_once(item[1], env["desired"]),
                ((first, "owner-one"), (second, "owner-two")),
            )
        )
    assert sorted(item.disposition for item in results) == ["idle", "processed"]
    assert env["artifacts"].object_count() == 1
    assert env["evidence"].stats()["decoder_runs"] == 1
    assert env["outputs"].diagnostics()["receipts"] == 1


def test_child_commit_is_reconciled_before_reported_process_crash_is_retried(
    tmp_path,
):
    executor = CommitThenCrashExecutor()
    env = _environment(tmp_path, process_executor=executor)

    result = env["consumer"].run_once("consumer-owner", env["desired"])

    assert result.disposition == "processed"
    assert executor.calls == 1
    assert result.output_receipt is not None
    assert result.output_receipt.process_receipt_sha256
    assert result.consumption_receipt is not None
    assert env["outputs"].diagnostics()["receipts"] == 1
    assert env["lifecycle"].diagnostics()["outbox_states"]["consumed"] == 1


@pytest.mark.parametrize(
    ("outcome", "expected_error"),
    (
        ("deadline_exceeded", DwdRvCompletionDeadlineError),
        ("cpu_limit_exceeded", DwdRvCompletionCapacityError),
        ("memory_limit_exceeded", DwdRvCompletionCapacityError),
        ("result_too_large", DwdRvCompletionCapacityError),
        ("cancelled", DwdRvCompletionCancelled),
        ("invalid_request", DwdRvCompletionIntegrityError),
        ("unsupported_platform", DwdRvProcessIsolationError),
        ("busy", DwdRvProcessIsolationError),
        ("child_crashed", DwdRvProcessIsolationError),
    ),
)
def test_every_hard_terminal_is_fail_closed_without_output_or_ack(
    tmp_path,
    outcome,
    expected_error,
):
    env = _environment(
        tmp_path,
        process_executor=TerminalProcessExecutor(
            DecoderProcessResult(
                outcome,
                detail_code="bounded_test_terminal",
                child_exit_code=-1,
                elapsed_seconds=0.1,
            )
        ),
    )

    with pytest.raises(expected_error):
        env["consumer"].run_once("consumer-owner", env["desired"])

    assert env["outputs"].diagnostics()["receipts"] == 0
    assert env["artifacts"].object_count() == 0
    assert env["evidence"].stats()["decoder_runs"] == 0
    assert env["lifecycle"].diagnostics()["outbox_states"]["leased"] == 1


def test_process_request_has_no_payload_capability_or_secret_and_tamper_is_closed(
    tmp_path,
):
    capture = CaptureRequestExecutor()
    env = _environment(tmp_path, process_executor=capture)

    with pytest.raises(DwdRvCompletionIntegrityError):
        env["consumer"].run_once("consumer-owner", env["desired"])

    request = capture.request
    assert isinstance(request, dict)
    serialized = json.dumps(request, sort_keys=True).casefold()
    assert "lease_token" not in serialized
    assert "capability-not-forwarded" not in serialized
    for forbidden in (
        "authorization", "api_key", "access_token", "refresh_token",
        "password", "credential", "private_key",
    ):
        assert forbidden not in serialized
    assert len(_canonical(request)) <= env["runner"].limits.max_request_bytes

    tampered = json.loads(json.dumps(request))
    tampered["authority_sha256"] = "f" * 64
    envelope = _dwd_completion_child_entry(tampered)
    assert envelope == {
        "authority_sha256": "0" * 64,
        "code": "authority_request_or_store_invalid",
        "schema": "stv3-dwd-rv-hard-process-child-result-v1",
        "status": "corrupt_contract",
    }
    assert env["outputs"].diagnostics()["receipts"] == 0


def test_hard_process_receipt_tamper_is_detected_independently(tmp_path):
    env = _environment(tmp_path)
    result = env["consumer"].run_once("consumer-owner", env["desired"])
    assert result.output_receipt is not None
    assert (
        result.output_receipt.receipt_sha256
        != result.output_receipt.process_receipt_sha256
    )
    with closing(sqlite3.connect(env["outputs"].path)) as database:
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            database.execute(
                "UPDATE completion_outputs SET process_receipt_sha256=? "
                "WHERE event_id=?",
                ("f" * 64, result.event_id),
            )
        database.rollback()
        database.execute("DROP TRIGGER completion_outputs_no_update")
        database.execute(
            "UPDATE completion_outputs SET process_receipt_sha256=? "
            "WHERE event_id=?",
            ("f" * 64, result.event_id),
        )
        database.execute(
            env["outputs"]._trigger_definitions()[
                "completion_outputs_no_update"
            ]
        )
        database.commit()
    with pytest.raises(
        DwdRvCompletionIntegrityError, match="hard-process receipt checksum"
    ):
        env["outputs"].load(result.event_id)


def test_real_spawn_boundary_commits_the_live_tar_when_supported(tmp_path):
    support = hard_boundary_support()
    if not support.supported:
        pytest.skip(f"hard process boundary unavailable: {support.mechanism}")
    env = _environment(
        tmp_path,
        hard_process=True,
        process_limits=DwdRvCompletionIsolationLimits(
            wall_seconds=90.0,
            cpu_seconds=60.0,
            rss_bytes=1536 * 1024 * 1024,
            max_request_bytes=1024 * 1024,
            max_result_bytes=16 * 1024,
        ),
    )

    result = env["consumer"].run_once("spawn-owner", env["desired"])

    assert result.output_receipt is not None
    isolation = result.output_receipt.output["process_isolation"]
    assert isolation == {
        "authority_sha256": env["runner"].authority_sha256,
        "effect_contract_sha256": PROCESS_EFFECT_CONTRACT_SHA256,
        "limits_sha256": env["runner"].limits.fingerprint,
        "state": "enforced_before_target_start",
    }
    assert PROCESS_EFFECT_CONTRACT.hard_kill_safe is True
    assert PROCESS_EFFECT_CONTRACT.durable_effects == "transactional"
    assert PROCESS_EFFECT_CONTRACT.spawns_descendants is False
    assert result.output_receipt.output["claims"][
        "meteorological_admission_granted"
    ] is False


def test_consumer_is_not_wired_to_live_service_science_or_publication():
    inert_names = {
        "dwd_rv_completion_consumer",
        "dwd_rv_completion_process_isolation",
    }
    for relative in (
        "storm_tracker_worker/service.py",
        "storm_tracker_worker/run.py",
        "storm_tracker_worker/science_shadow_orchestrator.py",
        "storm_tracker_worker/operational_snapshot_runtime.py",
        "custom_components/storm_tracker_v3/__init__.py",
    ):
        path = REPOSITORY / relative
        if path.exists():
            content = path.read_text(encoding="utf-8")
            assert all(name not in content for name in inert_names)
    source = (
        REPOSITORY
        / "storm_tracker_worker"
        / "dwd_rv_completion_consumer.py"
    ).read_text(encoding="utf-8")
    assert "import paho" not in source
    assert "capture_latest_dwd_rv_tar" not in source
    assert "AcquisitionEvidenceLifecycleStore" in source
    assert "claim_next" in source
    isolated_source = (
        REPOSITORY
        / "storm_tracker_worker"
        / "dwd_rv_completion_process_isolation.py"
    ).read_text(encoding="utf-8")
    assert "import paho" not in isolated_source
    assert "claim_next" not in isolated_source
    assert "acknowledge(" not in isolated_source
    assert "PROCESS_TARGET" in isolated_source


def test_profile_rejects_worst_case_detection_capacity():
    profile = _profile()
    with pytest.raises(DwdRvCompletionCapacityError, match="target grid"):
        replace(
            profile,
            pipeline_config=EvidenceToCellsShadowConfig(
                max_target_pixels=16,
                max_detection_objects=4,
                max_receipt_bytes=4 * 1024 * 1024,
                max_runtime_seconds=60.0,
            ),
        )


# The crash inventory below is deliberately literal and independent of the
# production module.  A source-AST assertion fails if production adds, removes
# or duplicates a fault call without extending this matrix.
_DWD_MIGRATION_OS_EXIT73_POINTS = (
    "migration.after_metadata",
    "migration.after_triggers",
    "migration.before_commit",
    "migration.after_commit",
)
_DWD_RUNTIME_OS_EXIT73_EXPECTATIONS = {
    "output.before_commit": (0, 1),
    "output.after_commit": (1, 0),
    "consumer.after_decode": (0, 1),
    "consumer.after_cells": (0, 1),
    "consumer.after_output_commit": (1, 0),
    "consumer.after_claim": (0, 1),
    "consumer.after_final_reproof_before_ack": (1, 0),
    "consumer.after_ack": (1, 0),
}
_DWD_ALL_OS_EXIT73_POINTS = frozenset(
    _DWD_MIGRATION_OS_EXIT73_POINTS
) | frozenset(_DWD_RUNTIME_OS_EXIT73_EXPECTATIONS)
_COMPLETION_CONSUMER_OS_EXIT73_CHILD = (
    REPOSITORY / "tests" / "helpers" / "completion_consumer_os_exit73_child.py"
)


def _append_native_chain_call(path: Path) -> None:
    descriptor = os.open(
        path,
        os.O_WRONLY | os.O_CREAT | os.O_APPEND,
        0o600,
    )
    try:
        os.write(descriptor, b"chain\n")
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _native_chain_call_count(path: Path) -> int:
    return 0 if not path.exists() else len(path.read_bytes().splitlines())


class CountingInlineProcessExecutor(InlineProcessExecutor):
    def __init__(self, call_log: Path) -> None:
        self.calls = 0
        self.call_log = call_log

    def execute(self, **kwargs):
        self.calls += 1
        _append_native_chain_call(self.call_log)
        return super().execute(**kwargs)


def _hermetic_dwd_completion_archive() -> bytes:
    """Build a deterministic, structure-complete DWD tar without `.analysis`."""

    from test_worker_dwd_rv_quantitative import _hdf_bytes

    hdf = _hdf_bytes()
    stream = io.BytesIO()
    with tarfile.open(
        fileobj=stream,
        mode="w",
        format=tarfile.USTAR_FORMAT,
    ) as archive:
        for lead in range(0, 121, 5):
            member = tarfile.TarInfo(
                f"composite_rv_20260801_2305_{lead:03d}-hd5"
            )
            member.size = len(hdf)
            member.mode = 0o644
            member.mtime = 0
            archive.addfile(member, io.BytesIO(hdf))
    return stream.getvalue()


def _dwd_build_chain_document(env, fault, *, call_log: Path):
    lease = env["lifecycle"].claim_next(
        CONSUMER_ID,
        STREAM_ID,
        "dwd-native-chain-crash",
        lease_seconds=120.0,
        now=env["clock"](),
    )
    assert lease is not None
    _append_native_chain_call(call_log)
    binding = dwd_completion._validate_lease(lease, env["handoff"])
    return dwd_completion._execute_chain(
        lease=lease,
        binding=binding,
        raw_store=env["handoff"],
        artifact_store=env["artifacts"],
        evidence_store=env["evidence"],
        profile=env["profile"],
        process_isolation={
            "authority_sha256": env["runner"].authority_sha256,
            "effect_contract_sha256": PROCESS_EFFECT_CONTRACT_SHA256,
            "limits_sha256": env["runner"].limits.fingerprint,
            "state": "enforced_before_target_start",
        },
        deadline_at=time.monotonic() + 60.0,
        monotonic=time.monotonic,
        fault=fault,
    )


def run_dwd_completion_consumer_os_exit73_child(
    scenario_root: Path,
    checkpoint: str,
    crash,
) -> None:
    """Create one closed fixture and reach exactly one production checkpoint."""

    if checkpoint not in _DWD_ALL_OS_EXIT73_POINTS:
        raise RuntimeError("DWD completion crash checkpoint is not allowlisted")
    archive = _hermetic_dwd_completion_archive()
    if checkpoint in _DWD_MIGRATION_OS_EXIT73_POINTS:
        env = _environment(scenario_root, source_payload=archive)
        seeded = env["consumer"].run_once(
            "dwd-migration-seed",
            env["desired"],
            lease_seconds=120.0,
        )
        assert seeded.output_receipt is not None
        path = env["outputs"].path
        _downgrade_output_store_to_v2(path)
        DwdRvCompletionOutputStore(
            path,
            clock=env["clock"],
            fault_injector=crash,
        )
        return

    env = _environment(
        scenario_root,
        consumer_fault=crash,
        output_fault=crash,
        process_executor=CountingInlineProcessExecutor(
            scenario_root / "native-crash-chain-calls.log"
        ),
        source_payload=archive,
    )
    call_log = scenario_root / "native-crash-chain-calls.log"
    if checkpoint in {"consumer.after_decode", "consumer.after_cells"}:
        _dwd_build_chain_document(env, crash, call_log=call_log)
        return
    if checkpoint in {"output.before_commit", "output.after_commit"}:
        document = _dwd_build_chain_document(
            env,
            lambda _point: None,
            call_log=call_log,
        )
        env["outputs"].commit(output=document)
        return
    env["consumer"].run_once(
        "dwd-native-crash",
        env["desired"],
        lease_seconds=120.0,
    )


def _run_dwd_completion_os_exit73(root: Path, checkpoint: str):
    environment = os.environ.copy()
    path_entries = [str(REPOSITORY), str(REPOSITORY / "tests")]
    if environment.get("PYTHONPATH"):
        path_entries.append(environment["PYTHONPATH"])
    environment["PYTHONPATH"] = os.pathsep.join(path_entries)
    return subprocess.run(
        [
            sys.executable,
            str(_COMPLETION_CONSUMER_OS_EXIT73_CHILD),
            "dwd",
            str(root.resolve()),
            checkpoint,
        ],
        cwd=REPOSITORY,
        env=environment,
        capture_output=True,
        text=True,
        timeout=180.0,
        check=False,
    )


def _sqlite_value(value):
    if isinstance(value, bytes):
        return ("bytes", value.hex())
    return value


def _sqlite_table_rows(path: Path, table: str) -> tuple:
    with closing(sqlite3.connect(path)) as database:
        columns = tuple(
            str(row[1])
            for row in database.execute(f"PRAGMA table_info({table})")
        )
        rows = tuple(
            tuple(_sqlite_value(value) for value in row)
            for row in database.execute(
                f"SELECT * FROM {table} ORDER BY 1"
            )
        )
    return columns, rows


def _dwd_migration_shape(path: Path) -> dict:
    with closing(sqlite3.connect(path)) as database:
        metadata = tuple(database.execute(
            "SELECT contract,schema_version FROM store_metadata WHERE singleton=1"
        ).fetchone())
        columns = tuple(
            str(row[1])
            for row in database.execute("PRAGMA table_info(store_metadata)")
        )
        triggers = tuple(sorted(
            str(row[0])
            for row in database.execute(
                "SELECT name FROM sqlite_master WHERE type='trigger'"
            )
        ))
        quick_check = str(database.execute("PRAGMA quick_check").fetchone()[0])
    return {
        "columns": columns,
        "metadata": metadata,
        "quick_check": quick_check,
        "triggers": triggers,
    }


def _assert_dwd_output_hot_journal(path: Path, *, expected: bool) -> None:
    journal = Path(str(path) + "-journal")
    if expected:
        assert journal.is_file() and journal.stat().st_size > 0
    elif journal.exists():
        assert journal.is_file()
        # SQLite DELETE-mode can retain a recovered journal on Windows; a
        # zeroed magic header proves that the journal is no longer hot.
        assert journal.read_bytes()[:8] == b"\0" * 8
    assert not Path(str(path) + "-wal").exists()
    assert not Path(str(path) + "-shm").exists()


def _dwd_runtime_rows(root: Path) -> dict:
    lifecycle_path = root / "lifecycle" / "acquisition_evidence.sqlite3"
    output_path = root / "outputs.sqlite3"
    return {
        "consumption": _sqlite_table_rows(
            lifecycle_path, "consumption_receipts"
        ),
        "outbox": _sqlite_table_rows(lifecycle_path, "completion_outbox"),
        "outputs": _sqlite_table_rows(output_path, "completion_outputs"),
    }


def _non_sqlite_tree(root: Path) -> tuple[tuple[str, int, str], ...]:
    rows = []
    for path in sorted(root.rglob("*")):
        if not path.is_file() or path.is_symlink():
            continue
        name = path.name
        if (
            name.endswith(".sqlite3")
            or name.endswith(".sqlite3-journal")
            or name.endswith(".sqlite3-shm")
            or name.endswith(".sqlite3-wal")
        ):
            continue
        payload = path.read_bytes()
        rows.append((
            path.relative_to(root).as_posix(),
            len(payload),
            hashlib.sha256(payload).hexdigest(),
        ))
    return tuple(rows)


def _reopen_dwd_native_crash_environment(root: Path):
    clock = MutableClock(EVALUATED_AT + 221.0)
    source = RawArtifactStore((root / "source-raw").resolve())
    source.initialize()
    handoff = RawArtifactStore((root / "handoff-raw").resolve())
    handoff.initialize()
    lifecycle = AcquisitionEvidenceLifecycleStore(
        (root / "lifecycle").resolve(),
        clock=clock,
        token_factory=TokenFactory(),
        directory_fsync=SYNC,
    )
    artifacts = DwdRvDerivedArtifactStore((root / "derived").resolve())
    evidence = CanonicalEvidenceStore(
        (root / "canonical.sqlite3").resolve(),
        raw_artifact_reader=handoff,
        clock=clock,
    )
    outputs = DwdRvCompletionOutputStore(
        (root / "outputs.sqlite3").resolve(),
        clock=clock,
    )
    profile = _profile()
    runner = DwdRvCompletionIsolatedRunner(
        handoff_raw_store=handoff,
        artifact_store=artifacts,
        evidence_store=evidence,
        output_store=outputs,
        profile=profile,
        limits=DwdRvCompletionIsolationLimits(),
    )
    executor = CountingInlineProcessExecutor(
        root / "native-crash-chain-calls.log"
    )
    runner._executor = executor
    consumer = DwdRvCompletionConsumer(
        lifecycle_store=lifecycle,
        handoff_raw_store=handoff,
        artifact_store=artifacts,
        evidence_store=evidence,
        output_store=outputs,
        profile=profile,
        process_runner=runner,
        clock=clock,
    )
    return {
        "consumer": consumer,
        "desired": _desired(),
        "evidence": evidence,
        "executor": executor,
        "handoff": handoff,
        "lifecycle": lifecycle,
        "outputs": outputs,
        "source": source,
    }


def test_dwd_completion_production_fault_inventory_is_literal_and_complete():
    source = (
        REPOSITORY / "storm_tracker_worker" / "dwd_rv_completion_consumer.py"
    ).read_text(encoding="utf-8")
    tree = ast.parse(source)
    calls = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or len(node.args) != 1:
            continue
        is_fault = (
            isinstance(node.func, ast.Attribute)
            and node.func.attr == "_fault"
        ) or (isinstance(node.func, ast.Name) and node.func.id == "fault")
        if (
            is_fault
            and isinstance(node.args[0], ast.Constant)
            and isinstance(node.args[0].value, str)
        ):
            calls.append(node.args[0].value)
    assert Counter(calls) == Counter(
        {checkpoint: 1 for checkpoint in _DWD_ALL_OS_EXIT73_POINTS}
    )


@pytest.mark.parametrize(
    "fault_point",
    _DWD_MIGRATION_OS_EXIT73_POINTS,
    ids=lambda value: f"dwd-{value}",
)
def test_native_os_exit73_dwd_output_migration_is_exactly_old_or_new(
    tmp_path,
    fault_point,
):
    crashed = _run_dwd_completion_os_exit73(tmp_path, fault_point)
    assert crashed.returncode == 73, (crashed.stdout, crashed.stderr)
    assert crashed.stdout == ""
    assert crashed.stderr == ""
    path = (tmp_path / "outputs.sqlite3").resolve()
    _assert_dwd_output_hot_journal(
        path,
        expected=fault_point != "migration.after_commit",
    )
    output_before = _sqlite_table_rows(path, "completion_outputs")
    assert len(output_before[1]) == 1
    shape = _dwd_migration_shape(path)
    assert shape["quick_check"] == "ok"
    if fault_point == "migration.after_commit":
        assert shape["metadata"] == (
            dwd_completion.RESULT_STORE_CONTRACT,
            dwd_completion.SCHEMA_VERSION,
        )
        assert "store_instance_sha256" in shape["columns"]
        assert shape["triggers"] == (
            "completion_outputs_no_delete",
            "completion_outputs_no_update",
            "store_metadata_no_delete",
            "store_metadata_no_update",
        )
    else:
        assert shape["metadata"] == (
            dwd_completion.LEGACY_RESULT_STORE_CONTRACT,
            dwd_completion.LEGACY_SCHEMA_VERSION,
        )
        assert "store_instance_sha256" not in shape["columns"]
        assert shape["triggers"] == ()

    recovered = DwdRvCompletionOutputStore(path)
    _assert_dwd_output_hot_journal(path, expected=False)
    identity = recovered.identity_sha256
    assert recovered.diagnostics() == {
        "contract": dwd_completion.RESULT_STORE_CONTRACT,
        "healthy": True,
        "receipt_bytes": output_before[1][0][19],
        "receipts": 1,
    }
    output_after = _sqlite_table_rows(path, "completion_outputs")
    assert output_after == output_before
    current_shape = _dwd_migration_shape(path)
    assert current_shape["metadata"] == (
        dwd_completion.RESULT_STORE_CONTRACT,
        dwd_completion.SCHEMA_VERSION,
    )
    assert current_shape["quick_check"] == "ok"
    tree_after_recovery = _non_sqlite_tree(tmp_path)

    replay = DwdRvCompletionOutputStore(path)
    assert replay.identity_sha256 == identity
    assert replay.diagnostics()["receipts"] == 1
    assert _sqlite_table_rows(path, "completion_outputs") == output_after
    assert _dwd_migration_shape(path) == current_shape
    assert _non_sqlite_tree(tmp_path) == tree_after_recovery


@pytest.mark.parametrize(
    "fault_point",
    tuple(_DWD_RUNTIME_OS_EXIT73_EXPECTATIONS),
    ids=lambda value: f"dwd-{value}",
)
def test_native_os_exit73_dwd_consumer_reopens_recovers_and_replays_once(
    tmp_path,
    fault_point,
):
    expected_outputs_before, expected_executor_calls = (
        _DWD_RUNTIME_OS_EXIT73_EXPECTATIONS[fault_point]
    )
    crashed = _run_dwd_completion_os_exit73(tmp_path, fault_point)
    assert crashed.returncode == 73, (crashed.stdout, crashed.stderr)
    assert crashed.stdout == ""
    assert crashed.stderr == ""

    output_path = (tmp_path / "outputs.sqlite3").resolve()
    call_log = tmp_path / "native-crash-chain-calls.log"
    _assert_dwd_output_hot_journal(
        output_path,
        expected=fault_point == "output.before_commit",
    )
    expected_child_calls = 0 if fault_point == "consumer.after_claim" else 1
    assert _native_chain_call_count(call_log) == expected_child_calls
    rows_before = _dwd_runtime_rows(tmp_path)
    assert len(rows_before["outbox"][1]) == 1
    outbox_columns = rows_before["outbox"][0]
    outbox_before = dict(zip(outbox_columns, rows_before["outbox"][1][0]))
    assert outbox_before["state"] == (
        "consumed" if fault_point == "consumer.after_ack" else "leased"
    )
    assert outbox_before["attempt_count"] == 1
    assert len(rows_before["outputs"][1]) == expected_outputs_before
    assert len(rows_before["consumption"][1]) == (
        1 if fault_point == "consumer.after_ack" else 0
    )
    source_before = _non_sqlite_tree(tmp_path / "source-raw")
    handoff_before = _non_sqlite_tree(tmp_path / "handoff-raw")

    reopened = _reopen_dwd_native_crash_environment(tmp_path)
    _assert_dwd_output_hot_journal(output_path, expected=False)
    recovered = reopened["consumer"].run_once(
        "dwd-native-recovery",
        reopened["desired"],
        lease_seconds=120.0,
    )
    if fault_point == "consumer.after_ack":
        assert recovered.disposition == "idle"
    elif expected_outputs_before:
        assert recovered.disposition == "recovered_output"
    else:
        assert recovered.disposition == "processed"
    assert reopened["executor"].calls == expected_executor_calls
    assert _native_chain_call_count(call_log) == (
        expected_child_calls + expected_executor_calls
    )

    rows_recovered = _dwd_runtime_rows(tmp_path)
    outbox_recovered = dict(zip(
        rows_recovered["outbox"][0], rows_recovered["outbox"][1][0]
    ))
    assert outbox_recovered["state"] == "consumed"
    assert outbox_recovered["attempt_count"] == (
        1 if fault_point == "consumer.after_ack" else 2
    )
    assert len(rows_recovered["outputs"][1]) == 1
    assert len(rows_recovered["consumption"][1]) == 1
    output_event = dict(zip(
        rows_recovered["outputs"][0], rows_recovered["outputs"][1][0]
    ))
    consumption = dict(zip(
        rows_recovered["consumption"][0],
        rows_recovered["consumption"][1][0],
    ))
    assert output_event["event_id"] == outbox_recovered["event_id"]
    assert consumption["event_id"] == outbox_recovered["event_id"]
    assert consumption["output_sha256"] == output_event["output_sha256"]
    assert _non_sqlite_tree(tmp_path / "source-raw") == source_before
    assert _non_sqlite_tree(tmp_path / "handoff-raw") == handoff_before

    diagnostics = reopened["lifecycle"].diagnostics()
    assert diagnostics["outbox_states"] == {
        "consumed": 1,
        "leased": 0,
        "ready": 0,
        "superseded": 0,
    }
    assert reopened["outputs"].diagnostics()["receipts"] == 1
    assert reopened["source"].integrity_check()["healthy"] is True
    assert reopened["handoff"].integrity_check()["healthy"] is True
    assert reopened["evidence"].audit()["healthy"] is True
    stable_files = _non_sqlite_tree(tmp_path)

    replay = reopened["consumer"].run_once(
        "dwd-native-replay",
        reopened["desired"],
        lease_seconds=120.0,
    )
    assert replay.disposition == "idle"
    assert reopened["executor"].calls == expected_executor_calls
    assert _native_chain_call_count(call_log) == (
        expected_child_calls + expected_executor_calls
    )
    assert _dwd_runtime_rows(tmp_path) == rows_recovered
    assert _non_sqlite_tree(tmp_path) == stable_files
    output_json = output_event["output_json"]
    assert output_json[0] == "bytes"
    serialized_output = bytes.fromhex(output_json[1]).decode("utf-8")
    assert str(tmp_path) not in serialized_output
    assert "lease_token" not in serialized_output
