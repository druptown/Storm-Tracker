from __future__ import annotations

from dataclasses import asdict
import hashlib
import json
import multiprocessing
from pathlib import Path
import sqlite3

import pytest

from storm_tracker_worker.acquisition_consumer_registry import (
    AcquisitionConsumerRegistryAuthority,
)
from storm_tracker_worker import autonomous_scheduler, desired_state
from storm_tracker_worker.acquisition_evidence_lifecycle import (
    AcquisitionEvidenceLifecycleStore,
    DWD_COMPLETION_PROOF_SCHEMA,
)
from storm_tracker_worker.acquisition_execution_store import ExecutionReceiptStore
from storm_tracker_worker.acquisition_source_reclamation_authority import (
    PREPARED,
    REGISTRY_VERIFIED_APPLY_BLOCKERS,
    AcquisitionSourceReclamationConflict,
    AcquisitionSourceReclamationCoordinator,
    AcquisitionSourceReclamationIntegrityError,
)
from storm_tracker_worker.acquisition_source_retention import (
    AcquisitionSourceCandidate,
)
from storm_tracker_worker.raw_artifacts import (
    NATIVE_CAPTURE_STAGE,
    NATIVE_REPRESENTATION,
    RawArtifactLimits,
    RawArtifactStore,
)
from storm_tracker_worker.raw_capacity_ledger import (
    ProviderCapacityLimit,
    RawCapacityLedger,
    RawCapacityProfile,
)
from storm_tracker_worker.terminal_raw_completion_router import (
    TERMINAL_RAW_CONSUMER_CATALOG,
)

from test_worker_acquisition_consumer_registry import (
    _archive_authorities,
    _process_open_attempt,
)
from test_worker_acquisition_source_retention import (
    NOW,
    SYNC,
    Authorities,
    _archive,
    _desired_value,
    _safe_raw,
)


WORKLOAD_ID = "radar.dwd.radolan_rv"


def _sha(label: str) -> str:
    return hashlib.sha256(label.encode("ascii")).hexdigest()


def _source_hashes(sources) -> dict[str, str]:
    paths = (
        sources.raw.catalog_path,
        sources.scheduler.path,
        sources.executions.path,
        sources.capacity.path,
        sources.evidence.path,
        *sources.object_paths,
    )
    return {
        str(path): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in paths
    }


def _exact_route_context(
    tmp_path,
    *,
    consumer_id: str | None = None,
    raw_store_kwargs: dict | None = None,
    evidence_store_kwargs: dict | None = None,
):
    source_root = tmp_path / "sources"
    member = TERMINAL_RAW_CONSUMER_CATALOG[WORKLOAD_ID]
    desired = desired_state.DesiredStateStore(
        (source_root / "desired.sqlite3").resolve()
    )
    desired.claim_producer(
        "ha-retention-test",
        "registry-closure-claim",
        claimed_at=NOW - 100.0,
    )
    desired.apply(_desired_value(), received_at=NOW - 59.0)
    scheduler = autonomous_scheduler.AutonomousScheduler(
        (source_root / "scheduler.sqlite3").resolve(),
        desired_state_store=desired,
        plan_builder=lambda _state: (
            autonomous_scheduler.ScheduleIntent(
                intent_id="registry-closure-source",
                job_type="worker_acquisition_slot_v1",
                cadence_seconds=300,
                deadline_seconds=250,
                priority=0,
                payload={"closed_test_marker": "registry-closure"},
            ),
        ),
        clock=lambda: NOW,
        lease_seconds=60.0,
    )
    scheduler.reconcile(now=NOW)
    claim = scheduler.claim_next("registry-closure-worker", now=NOW)
    assert claim is not None
    selected_raw_store_kwargs = dict(raw_store_kwargs or {})
    selected_raw_store_kwargs.setdefault(
        "limits",
        RawArtifactLimits(
            max_object_bytes=2 * 1024 * 1024,
            max_total_bytes=8 * 1024 * 1024,
            max_object_entries=32,
            max_staging_entries=8,
            max_receipt_entries=32,
            max_capture_entries=32,
            max_capture_members=4,
            max_catalog_bytes=4 * 1024 * 1024,
        ),
    )
    raw = RawArtifactStore(
        (source_root / "raw").resolve(),
        **selected_raw_store_kwargs,
    )
    raw.initialize()
    capacity = RawCapacityLedger(
        (source_root / "capacity.sqlite3").resolve(),
        profile=RawCapacityProfile(
            global_max_bytes=8 * 1024 * 1024,
            global_max_entries=32,
            providers=(ProviderCapacityLimit("dwd", 8 * 1024 * 1024, 32),),
            max_reservations=32,
            max_catalog_bytes=4 * 1024 * 1024,
            sqlite_busy_timeout_seconds=2.0,
        ),
        clock=lambda: NOW,
    )
    executions = ExecutionReceiptStore(
        (source_root / "execution.sqlite3").resolve(),
        clock=lambda: NOW + 0.5,
    )
    selected_evidence_store_kwargs = dict(evidence_store_kwargs or {})
    selected_evidence_store_kwargs.setdefault("clock", lambda: NOW + 2.0)
    selected_evidence_store_kwargs.setdefault("directory_fsync", SYNC)
    evidence = AcquisitionEvidenceLifecycleStore(
        (source_root / "evidence").resolve(),
        **selected_evidence_store_kwargs,
    )
    acquisition_job = "registry-closure-acquisition-job"
    payload = b"exact-registry-closure-source:" + b"R" * 128
    content_sha = hashlib.sha256(payload).hexdigest()
    raw_receipt = raw.store(
        provider="dwd",
        product="dwd_open_data_composite_rv_native_tar",
        fetch_id=f"{acquisition_job}:rv_tar",
        content_type="application/x-tar",
        received_at=NOW - 20.0,
        upstream_id=(
            "dwd_open_data_composite_rv_native_tar|latest|"
            f"content_sha256={content_sha}"
        ),
        provenance={
            "capture_stage": NATIVE_CAPTURE_STAGE,
            "collector": "dwd_open_data_composite_rv",
            "collector_version": "registry-closure-test-v1",
            "provenance_scope": "exact_upstream_tar_bytes_only",
            "representation": NATIVE_REPRESENTATION,
            "structure_inspector_version": "registry-closure-layout-v1",
        },
        payload=payload,
    )
    capture = raw.commit_capture(
        provider="dwd",
        product="dwd_open_data_composite_rv_raw_capture",
        fetch_id=f"{acquisition_job}:capture",
        upstream_capture_id=(
            "dwd_open_data_composite_rv_raw_capture|member=test.tar|"
            f"content_sha256={content_sha}"
        ),
        product_timestamp=NOW - 300.0,
        members={"rv_tar": raw_receipt.receipt_id},
    )
    capacity.reserve(
        reservation_id="registry-closure-reservation",
        provider="dwd",
        job_identity=acquisition_job,
        requested_bytes=len(payload),
        requested_entries=1,
    )
    committed = capacity.commit(
        reservation_id="registry-closure-reservation",
        actual_bytes=len(payload),
        actual_entries=1,
        content_sha256=raw_receipt.content_sha256,
        receipt_sha256=raw_receipt.receipt_sha256,
        capture_sha256=capture.capture_receipt_sha256,
    )
    executions.record(
        scheduler_job_id=claim.job_id,
        acquisition_job_id=acquisition_job,
        binding_sha256=_sha("registry-closure-terminal-attempt"),
        attempt_count=claim.attempt_count,
        outcome="completed_shadow_raw",
        reason_code="shadow_raw_capture_complete",
        terminal=True,
        content_sha256=raw_receipt.content_sha256,
        raw_receipt_sha256=raw_receipt.receipt_sha256,
        capture_sha256=capture.capture_receipt_sha256,
        capacity_commit_sha256=committed.commit_sha256,
    )
    scheduler.complete(claim.job_id, claim.lease_token, now=NOW + 1.0)
    scheduler_proof = scheduler.terminal_proof(claim.job_id)
    assert scheduler_proof is not None
    proof = {
        "capacity_receipt": {
            **asdict(committed),
            "schema": "stv3-worker-raw-capacity-receipt-evidence-v1",
        },
        "execution_receipts": [
            item.as_dict()
            for item in executions.load_job_receipts(acquisition_job)
        ],
        "meteorological_claim": False,
        "scheduler_terminal": {
            **scheduler_proof.as_dict(),
            "proof_sha256": scheduler_proof.proof_sha256,
        },
        "schema": DWD_COMPLETION_PROOF_SCHEMA,
        "source_capture": capture.to_metadata(),
        "source_receipts": {"rv_tar": _safe_raw(raw_receipt)},
    }
    producer = evidence.publish(
        acquisition_job_id=acquisition_job,
        consumer_id=consumer_id or member.consumer_id,
        stream_id=member.stream_id,
        desired_state_sha256="1" * 64,
        authority_sha256="2" * 64,
        proof=proof,
        payload=payload,
    )
    lease = evidence.claim_next(
        producer.consumer_id,
        producer.stream_id,
        "registry-closure-test-owner",
        now=NOW + 5.0,
    )
    assert lease is not None
    evidence.acknowledge(
        lease.event_id,
        lease.lease_token,
        output_sha256=_sha("registry-closure-output"),
        now=NOW + 6.0,
    )
    candidate = AcquisitionSourceCandidate(
        acquisition_job_id=acquisition_job,
        scheduler_job_id=claim.job_id,
        raw_capture_id=capture.capture_id,
        evidence_stream_id=member.stream_id,
        evidence_stream_sequence=producer.stream_sequence,
    )
    sources = Authorities(
        raw=raw,
        scheduler=scheduler,
        executions=executions,
        capacity=capacity,
        evidence=evidence,
        candidates=(candidate,),
        object_paths=(Path(raw_receipt.artifact_path),),
    )
    archive = _archive(source_root, sources)
    plan = archive.plan((candidate,), now=NOW + 100.0)
    assert plan.eligible_count == 1
    exported = archive.export(plan)
    assert exported.archived_jobs == 1
    return sources, archive, plan, candidate


def _bound_registry(tmp_path):
    catalog, reproof = _archive_authorities("reclamation-closure")
    authority = AcquisitionConsumerRegistryAuthority(
        (tmp_path / "registry" / "registry.sqlite3").resolve(),
        clock=lambda: NOW + 150.0,
    )
    epoch = authority.ensure_epoch(
        source_catalog=catalog,
        source_reproof_authority=reproof,
    )
    bound = authority.verifier(
        source_catalog=catalog,
        source_reproof_authority=reproof,
    ).bind_current_epoch(
        claimed_epoch_root_sha256=epoch.epoch_root_sha256,
    )
    return authority, bound


def _coordinator(tmp_path, archive, *, bound=None, fault=None):
    return AcquisitionSourceReclamationCoordinator(
        (tmp_path / "reclamation").resolve(),
        archive=archive,
        consumer_registry_verifier=bound,
        clock=lambda: NOW + 200.0,
        fault_injector=fault,
        directory_fsync=lambda _path: None,
    )


def test_bound_prepare_persists_exact_per_job_consumer_closure(tmp_path):
    sources, archive, plan, candidate = _exact_route_context(tmp_path)
    registry, bound = _bound_registry(tmp_path)
    coordinator = _coordinator(tmp_path, archive, bound=bound)
    source_before = _source_hashes(sources)

    outcome = coordinator.prepare(plan)

    assert outcome.state == PREPARED
    assert outcome.blockers == REGISTRY_VERIFIED_APPLY_BLOCKERS
    assert outcome.consumer_closure_root_sha256 is not None
    request = coordinator.load_request(outcome.request_sha256)
    assert request is not None
    assert request.claimed_consumer_registry_root_sha256 == (
        bound.registry_root_sha256
    )
    assert request.claimed_consumer_epoch_root_sha256 == bound.epoch_root_sha256
    closures = coordinator.load_consumer_closure_receipts(
        outcome.request_sha256
    )
    assert len(closures) == 1
    closure = closures[0]
    member = bound.registry_root.by_workload[WORKLOAD_ID]
    assert closure.candidate_sha256 == candidate.candidate_sha256
    assert closure.acquisition_job_id == candidate.acquisition_job_id
    assert closure.workload_id == WORKLOAD_ID
    assert closure.consumer_id == member.consumer_id
    assert closure.stream_id == member.stream_id
    assert closure.registry_member_sha256 == member.fingerprint
    assert closure.registry_epoch_root_sha256 == bound.epoch_root_sha256
    assert closure.registry_store_identity_sha256 == bound.store_identity_sha256
    assert closure.source_archive_anchor_sha256 == (
        request.sealed_archive_anchor_sha256
    )
    assert outcome.consumer_closure_root_sha256 == coordinator._participants.load(
        role="downstream_lifecycle",
        request_sha256=outcome.request_sha256,
    ).child_reference_root_sha256
    encoded = json.dumps(closure.as_dict(), sort_keys=True).casefold()
    for forbidden in (
        "artifact_path",
        "payload",
        "authorization",
        "password",
        "https://",
    ):
        assert forbidden not in encoded
    assert _source_hashes(sources) == source_before
    assert coordinator.audit()["registry_verified_decision_count"] == 1
    registry.close()


def test_unbound_or_forged_registry_claims_refuse_before_prepare_journals(
    tmp_path,
):
    _sources, archive, plan, _candidate = _exact_route_context(tmp_path)
    unbound = _coordinator(tmp_path / "unbound", archive)
    with pytest.raises(
        AcquisitionSourceReclamationConflict,
        match="epoch_root_authority_unavailable",
    ):
        unbound.prepare(
            plan,
            claimed_consumer_registry_root_sha256="a" * 64,
            claimed_consumer_epoch_root_sha256="b" * 64,
        )
    assert unbound.audit()["decision_count"] == 0

    registry, bound = _bound_registry(tmp_path)
    bound_coordinator = _coordinator(tmp_path / "bound", archive, bound=bound)
    with pytest.raises(
        AcquisitionSourceReclamationConflict,
        match="epoch roots differ",
    ):
        bound_coordinator.prepare(
            plan,
            claimed_consumer_registry_root_sha256="a" * 64,
            claimed_consumer_epoch_root_sha256="b" * 64,
        )
    assert bound_coordinator.audit()["decision_count"] == 0
    registry.close()


def test_route_membership_mismatch_refuses_without_prepared_decision(tmp_path):
    _sources, archive, plan, _candidate = _exact_route_context(
        tmp_path,
        consumer_id="forged-dwd-consumer-v1",
    )
    registry, bound = _bound_registry(tmp_path)
    coordinator = _coordinator(tmp_path, archive, bound=bound)

    with pytest.raises(
        AcquisitionSourceReclamationConflict,
        match="epoch_root_authority_unavailable",
    ):
        coordinator.prepare(plan)
    audit = coordinator.audit()
    assert audit["prepared_count"] == 0
    assert audit["participant_count"] == 1
    registry.close()


def test_restart_replays_closures_and_sidecar_tamper_fails_closed(tmp_path):
    _sources, archive, plan, _candidate = _exact_route_context(tmp_path)
    registry, bound = _bound_registry(tmp_path)
    coordinator = _coordinator(tmp_path, archive, bound=bound)
    first = coordinator.prepare(plan)
    first_closures = coordinator.load_consumer_closure_receipts(
        first.request_sha256
    )

    restarted = AcquisitionSourceReclamationCoordinator(
        coordinator.root,
        archive=archive,
        consumer_registry_verifier=bound,
        clock=lambda: NOW + 201.0,
        directory_fsync=lambda _path: None,
    )
    replay = restarted.prepare(plan)
    assert replay.decision_sha256 == first.decision_sha256
    assert restarted.load_consumer_closure_receipts(
        replay.request_sha256
    ) == first_closures

    sidecar = restarted._participants._journals["downstream_lifecycle"].path
    database = sqlite3.connect(sidecar)
    try:
        row = database.execute(
            "SELECT payload_json FROM participant_prepares WHERE request_sha256=?",
            (first.request_sha256,),
        ).fetchone()
        assert row is not None
        payload = json.loads(bytes(row[0]))
        payload["consumer_closure_receipts"][0]["stream_sequence"] += 1
        database.execute(
            "UPDATE participant_prepares SET payload_json=? WHERE request_sha256=?",
            (
                json.dumps(
                    payload,
                    allow_nan=False,
                    ensure_ascii=True,
                    separators=(",", ":"),
                    sort_keys=True,
                ).encode("ascii"),
                first.request_sha256,
            ),
        )
        database.commit()
    finally:
        database.close()
    with pytest.raises(
        AcquisitionSourceReclamationIntegrityError,
        match="consumer closure receipt is invalid",
    ):
        restarted.load_consumer_closure_receipts(first.request_sha256)
    registry.close()


def test_crash_after_durable_closure_sidecar_replays_to_exact_prepared(tmp_path):
    sources, archive, plan, _candidate = _exact_route_context(tmp_path)
    registry, bound = _bound_registry(tmp_path)
    source_before = _source_hashes(sources)
    fired = False

    def fault(point: str) -> None:
        nonlocal fired
        if (
            point == "coordinator.after_downstream_lifecycle_journal_commit"
            and not fired
        ):
            fired = True
            raise RuntimeError(point)

    coordinator = _coordinator(tmp_path, archive, bound=bound, fault=fault)
    with pytest.raises(
        RuntimeError,
        match="after_downstream_lifecycle_journal_commit",
    ):
        coordinator.prepare(plan)
    assert fired
    audit = coordinator.audit()
    assert audit["preparing_count"] == 1
    assert audit["prepared_count"] == 0
    assert audit["registry_verified_decision_count"] == 1
    assert audit["participant_journals"]["downstream_lifecycle"][
        "prepare_count"
    ] == 1

    restarted = AcquisitionSourceReclamationCoordinator(
        coordinator.root,
        archive=archive,
        consumer_registry_verifier=bound,
        clock=lambda: NOW + 201.0,
        directory_fsync=lambda _path: None,
    )
    outcome = restarted.prepare(plan)
    assert outcome.state == PREPARED
    closures = restarted.load_consumer_closure_receipts(
        outcome.request_sha256
    )
    assert len(closures) == 1
    assert closures[0].registry_epoch_root_sha256 == bound.epoch_root_sha256
    assert _source_hashes(sources) == source_before
    registry.close()


def test_cross_process_registry_owner_cannot_replace_bound_prepare_authority(
    tmp_path,
):
    _sources, archive, plan, _candidate = _exact_route_context(tmp_path)
    registry, bound = _bound_registry(tmp_path)
    coordinator = _coordinator(tmp_path, archive, bound=bound)
    context = multiprocessing.get_context("spawn")
    queue = context.Queue()
    process = context.Process(
        target=_process_open_attempt,
        args=(str(registry.path), queue),
    )
    process.start()
    process.join(timeout=20.0)
    try:
        assert process.exitcode == 0
        assert queue.get(timeout=2.0) == "fenced"
    finally:
        if process.is_alive():
            process.terminate()
            process.join(timeout=5.0)
        queue.close()
        queue.join_thread()

    outcome = coordinator.prepare(plan)
    assert outcome.state == PREPARED
    assert len(
        coordinator.load_consumer_closure_receipts(outcome.request_sha256)
    ) == 1
    registry.close()
