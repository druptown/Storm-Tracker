from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, replace
import errno
import hashlib
import json
import os
from pathlib import Path

import pytest

from storm_tracker_worker import (
    acquisition_source_retention,
    autonomous_scheduler,
    desired_state,
)
from storm_tracker_worker.acquisition_evidence_lifecycle import (
    AcquisitionEvidenceConflict,
    AcquisitionEvidenceLifecycleStore,
    DWD_COMPLETION_PROOF_SCHEMA,
    EvidenceRetentionPolicy,
)
from storm_tracker_worker.acquisition_execution_store import ExecutionReceiptStore
from storm_tracker_worker.acquisition_source_retention import (
    CONTRACT,
    AcquisitionSourceCandidate,
    AcquisitionSourceRetentionArchive,
    AcquisitionSourceRetentionCapacityError,
    AcquisitionSourceRetentionConflict,
    AcquisitionSourceRetentionDurabilityError,
    AcquisitionSourceRetentionIntegrityError,
    AcquisitionSourceRetentionLimits,
    AcquisitionSourceRetentionWindowExpired,
)
from storm_tracker_worker.raw_artifacts import (
    NATIVE_CAPTURE_STAGE,
    NATIVE_REPRESENTATION,
    RAW_ARTIFACT_CAPTURE_SCHEMA,
    RawArtifactLimits,
    RawArtifactStore,
)
from storm_tracker_worker.raw_capacity_ledger import (
    ProviderCapacityLimit,
    RawCapacityLedger,
    RawCapacityProfile,
)


NOW = 1_800_000_000.0
SYNC = lambda _path: None


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode("ascii")).hexdigest()


def _desired_value() -> dict:
    return {
        "contract": desired_state.CONTRACT,
        "producer_id": "ha-retention-test",
        "authority_epoch": 1,
        "generation": 1,
        "issued_at": NOW - 60.0,
        "previous_state_sha256": None,
        "settings": {
            "ground_radius_km": 20.0,
            "lightning_source_mode": "auto",
            "model_guidance_enabled": False,
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
            "position_observed_at": NOW - 60.0,
            "target_id": "zone.home",
        }],
    }


def _safe_raw(receipt) -> dict:
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


@dataclass
class Authorities:
    raw: RawArtifactStore
    scheduler: autonomous_scheduler.AutonomousScheduler
    executions: ExecutionReceiptStore
    capacity: RawCapacityLedger
    evidence: AcquisitionEvidenceLifecycleStore
    candidates: tuple[AcquisitionSourceCandidate, ...]
    object_paths: tuple[Path, ...]


def _authorities(
    tmp_path: Path,
    *,
    count: int = 1,
    consume: bool = True,
    reject: bool = False,
    payload_bytes: int = 128,
    consumer_id: str = "retention-decoder-v1",
    stream_id: str = "radar.dwd.retention",
) -> Authorities:
    desired_path = (tmp_path / "desired.sqlite3").resolve()
    desired = desired_state.DesiredStateStore(desired_path)
    desired.claim_producer(
        "ha-retention-test",
        "retention-claim",
        claimed_at=NOW - 100.0,
    )
    desired.apply(_desired_value(), received_at=NOW - 59.0)
    intents = tuple(
        autonomous_scheduler.ScheduleIntent(
            intent_id=f"retention-source-{index}",
            job_type="worker_acquisition_slot_v1",
            cadence_seconds=300,
            deadline_seconds=250,
            priority=index,
            payload={"closed_test_marker": f"source-{index}"},
        )
        for index in range(count)
    )
    scheduler = autonomous_scheduler.AutonomousScheduler(
        (tmp_path / "scheduler.sqlite3").resolve(),
        desired_state_store=desired,
        plan_builder=lambda _state: intents,
        clock=lambda: NOW,
        lease_seconds=60.0,
    )
    scheduler.reconcile(now=NOW)
    raw = RawArtifactStore(
        (tmp_path / "raw").resolve(),
        limits=RawArtifactLimits(
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
    raw.initialize()
    capacity = RawCapacityLedger(
        (tmp_path / "capacity.sqlite3").resolve(),
        profile=RawCapacityProfile(
            global_max_bytes=8 * 1024 * 1024,
            global_max_entries=32,
            providers=(
                ProviderCapacityLimit("dwd", 8 * 1024 * 1024, 32),
            ),
            max_reservations=32,
            max_catalog_bytes=4 * 1024 * 1024,
            sqlite_busy_timeout_seconds=2.0,
        ),
        clock=lambda: NOW,
    )
    executions = ExecutionReceiptStore(
        (tmp_path / "execution.sqlite3").resolve(),
        clock=lambda: NOW + 0.5,
    )
    evidence = AcquisitionEvidenceLifecycleStore(
        (tmp_path / "evidence").resolve(),
        clock=lambda: NOW + 2.0,
        directory_fsync=SYNC,
    )
    candidates: list[AcquisitionSourceCandidate] = []
    paths: list[Path] = []
    for index in range(count):
        claim = scheduler.claim_next("retention-worker", now=NOW)
        assert claim is not None
        acquisition_job = f"acquisition-job-{index}"
        payload = (
            f"exact-source-{index}:".encode("ascii")
            + bytes([65 + index]) * payload_bytes
        )
        content_sha = hashlib.sha256(payload).hexdigest()
        receipt = raw.store(
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
                "collector_version": "retention-test-v1",
                "provenance_scope": "exact_upstream_tar_bytes_only",
                "representation": NATIVE_REPRESENTATION,
                "structure_inspector_version": "retention-test-layout-v1",
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
            product_timestamp=NOW - 300.0 - index * 300.0,
            members={"rv_tar": receipt.receipt_id},
        )
        capacity.reserve(
            reservation_id=f"retention-reservation-{index}",
            provider="dwd",
            job_identity=acquisition_job,
            requested_bytes=len(payload),
            requested_entries=1,
        )
        committed = capacity.commit(
            reservation_id=f"retention-reservation-{index}",
            actual_bytes=len(payload),
            actual_entries=1,
            content_sha256=receipt.content_sha256,
            receipt_sha256=receipt.receipt_sha256,
            capture_sha256=capture.capture_receipt_sha256,
        )
        executions.record(
            scheduler_job_id=claim.job_id,
            acquisition_job_id=acquisition_job,
            binding_sha256=_sha(f"attempt:{index}:retry"),
            attempt_count=0,
            outcome="retryable_failure",
            reason_code="simulated_prior_retry",
            terminal=False,
        )
        executions.record(
            scheduler_job_id=claim.job_id,
            acquisition_job_id=acquisition_job,
            binding_sha256=_sha(f"attempt:{index}:terminal"),
            attempt_count=claim.attempt_count,
            outcome="completed_shadow_raw",
            reason_code="shadow_raw_capture_complete",
            terminal=True,
            content_sha256=receipt.content_sha256,
            raw_receipt_sha256=receipt.receipt_sha256,
            capture_sha256=capture.capture_receipt_sha256,
            capacity_commit_sha256=committed.commit_sha256,
        )
        scheduler.complete(claim.job_id, claim.lease_token, now=NOW + 1.0)
        scheduler_proof = scheduler.terminal_proof(claim.job_id)
        assert scheduler_proof is not None
        history = executions.load_job_receipts(acquisition_job)
        proof = {
            "capacity_receipt": {
                **asdict(committed),
                "schema": "stv3-worker-raw-capacity-receipt-evidence-v1",
            },
            "execution_receipts": [item.as_dict() for item in history],
            "meteorological_claim": False,
            "scheduler_terminal": {
                **scheduler_proof.as_dict(),
                "proof_sha256": scheduler_proof.proof_sha256,
            },
            "schema": DWD_COMPLETION_PROOF_SCHEMA,
            "source_capture": capture.to_metadata(),
            "source_receipts": {"rv_tar": _safe_raw(receipt)},
        }
        producer = evidence.publish(
            acquisition_job_id=acquisition_job,
            consumer_id=consumer_id,
            stream_id=stream_id,
            desired_state_sha256="1" * 64,
            authority_sha256="2" * 64,
            proof=proof,
            payload=payload,
        )
        lease = evidence.claim_next(
            consumer_id,
            stream_id,
            "retention-consumer",
            now=NOW + 3.0 + index,
        )
        assert lease is not None and lease.event_id == producer.event_id
        if reject:
            evidence.reject_leased(
                lease.event_id,
                lease.lease_token,
                expected_event_sha256=lease.event_sha256,
                reason_code="desired_state_changed_before_ack",
                active_desired_state_sha256="3" * 64,
                now=NOW + 4.0 + index,
            )
        elif consume:
            evidence.acknowledge(
                lease.event_id,
                lease.lease_token,
                output_sha256=_sha(f"decoder-output:{index}"),
                now=NOW + 4.0 + index,
            )
        candidates.append(AcquisitionSourceCandidate(
            acquisition_job_id=acquisition_job,
            scheduler_job_id=claim.job_id,
            raw_capture_id=capture.capture_id,
            evidence_stream_id=stream_id,
            evidence_stream_sequence=index + 1,
        ))
        paths.append(Path(receipt.artifact_path))
    return Authorities(
        raw=raw,
        scheduler=scheduler,
        executions=executions,
        capacity=capacity,
        evidence=evidence,
        candidates=tuple(candidates),
        object_paths=tuple(paths),
    )


def _archive(
    tmp_path: Path,
    authorities: Authorities,
    *,
    fault=None,
    limits=None,
    directory_fsync=SYNC,
) -> AcquisitionSourceRetentionArchive:
    return AcquisitionSourceRetentionArchive(
        (tmp_path / "source-archive").resolve(),
        raw_store=authorities.raw,
        scheduler=authorities.scheduler,
        execution_store=authorities.executions,
        capacity_ledger=authorities.capacity,
        evidence_store=authorities.evidence,
        limits=limits or AcquisitionSourceRetentionLimits(
            min_terminal_age_seconds=10.0
        ),
        clock=lambda: NOW + 100.0,
        fault_injector=fault,
        directory_fsync=directory_fsync,
    )


def _file_sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_cross_bound_plan_sealed_export_restart_lookup_and_no_reclaim(tmp_path):
    authorities = _authorities(tmp_path)
    authorities.evidence.rotate_archive(
        policy=EvidenceRetentionPolicy(),
        now=NOW + 10.0,
    )
    before = authorities.raw.stats()
    source_files = (
        authorities.raw.catalog_path,
        authorities.scheduler.path,
        authorities.executions.path,
        authorities.capacity.path,
        authorities.evidence.path,
        *authorities.object_paths,
    )
    source_hashes = {path: _file_sha(path) for path in source_files}
    archive = _archive(tmp_path, authorities)
    plan = archive.plan(authorities.candidates, now=NOW + 100.0)
    assert plan.eligible_count == 1
    assert plan.reclaim_authorized is False
    assert plan.dispositions[0].status == "sealed_export_eligible"
    result = archive.export(plan)
    assert result.status == "archived"
    assert result.archived_jobs == 1
    assert result.source_reclaimed is False
    assert authorities.raw.stats() == before
    assert all(path.exists() for path in authorities.object_paths)
    assert {_path: _file_sha(_path) for _path in source_files} == source_hashes

    replay_plan = archive.plan(authorities.candidates, now=NOW + 101.0)
    replay = archive.export(replay_plan)
    assert replay.status == "idempotent"
    lookup = archive.lookup(authorities.candidates[0].candidate_sha256)
    assert lookup.status == "archived"
    assert lookup.record["source_reclaim_authorized"] is False
    assert len(lookup.record["execution_history"]) == 2
    assert lookup.record["raw_capture"]["schema"] == RAW_ARTIFACT_CAPTURE_SCHEMA
    with pytest.raises(AcquisitionSourceRetentionWindowExpired, match="window_expired"):
        archive.lookup(
            authorities.candidates[0].candidate_sha256,
            include_archive=False,
        )

    reopened = _archive(tmp_path, authorities)
    audit = reopened.audit()
    assert audit["healthy"] is True
    assert audit["archived_job_count"] == 1
    assert audit["reclaim_enabled"] is False
    segment_text = next(
        reopened.segment_directory.glob("*.sealed.json")
    ).read_text(encoding="ascii")
    assert "artifact_path" not in segment_text
    assert str(tmp_path) not in segment_text


@pytest.mark.parametrize("mode", ["ready", "rejected", "unknown"])
def test_non_consumed_downstream_never_becomes_export_eligible(tmp_path, mode):
    authorities = _authorities(
        tmp_path,
        consume=mode != "ready",
        reject=mode == "rejected",
    )
    candidate = authorities.candidates[0]
    if mode == "unknown":
        candidate = replace(candidate, evidence_stream_sequence=2)
    archive = _archive(tmp_path, authorities)
    plan = archive.plan((candidate,), now=NOW + 100.0)
    assert plan.eligible_count == 0
    assert plan.dispositions[0].reason_code == "downstream_terminal_unavailable"
    assert archive.export(plan).status == "no_eligible_records"
    assert authorities.object_paths[0].exists()


def test_hot_window_and_mismatched_cross_binding_fail_closed(tmp_path):
    authorities = _authorities(tmp_path)
    archive = _archive(
        tmp_path,
        authorities,
        limits=AcquisitionSourceRetentionLimits(
            min_terminal_age_seconds=1000.0
        ),
    )
    hot = archive.plan(authorities.candidates, now=NOW + 100.0)
    assert hot.eligible_count == 0
    assert hot.dispositions[0].reason_code == "hot_window_active"

    candidate = replace(
        authorities.candidates[0],
        scheduler_job_id="different-scheduler-job",
    )
    with pytest.raises(
        AcquisitionSourceRetentionIntegrityError,
        match="execution and raw capture",
    ):
        archive.plan((candidate,), now=NOW + 2000.0)


def test_plan_is_canonical_immutable_and_tamper_or_forgery_fails_closed(
    tmp_path,
):
    authorities = _authorities(tmp_path)
    archive = _archive(tmp_path, authorities)
    plan = archive.plan(authorities.candidates, now=NOW + 100.0)
    old_anchor = archive.audit()["archive_anchor_sha256"]
    assert isinstance(plan._record_payloads[0], bytes)

    damaged_payload = bytearray(plan._record_payloads[0])
    damaged_payload[len(damaged_payload) // 2] ^= 1
    damaged = replace(plan, _record_payloads=(bytes(damaged_payload),))
    with pytest.raises(AcquisitionSourceRetentionIntegrityError):
        archive.export(damaged)
    assert archive.audit()["archive_anchor_sha256"] == old_anchor

    forged_record = json.loads(plan._record_payloads[0].decode("ascii"))
    forged_record["reclaim_withheld_reason"] = "forged_authority"
    forged_core = dict(forged_record)
    forged_core.pop("record_sha256")
    forged_record["record_sha256"] = acquisition_source_retention._digest(
        forged_core
    )
    forged_disposition = replace(
        plan.dispositions[0],
        record_sha256=forged_record["record_sha256"],
    )
    forged_payload = acquisition_source_retention._canonical(forged_record)
    forged_core_plan = {
        "created_at": plan.created_at,
        "dispositions": [asdict(forged_disposition)],
        "limits_sha256": archive._limits.fingerprint,
        "record_sha256s": [forged_record["record_sha256"]],
        "schema": acquisition_source_retention.PLAN_SCHEMA,
        "source_binding_sha256": plan.source_binding_sha256,
        "source_reclaim_authorized": False,
        "source_view_sha256": plan.source_view_sha256,
    }
    forged = replace(
        plan,
        plan_sha256=acquisition_source_retention._digest(forged_core_plan),
        dispositions=(forged_disposition,),
        _record_payloads=(forged_payload,),
    )
    with pytest.raises(
        AcquisitionSourceRetentionIntegrityError,
        match="fresh source evidence",
    ):
        archive.export(forged)
    assert archive.audit()["archive_anchor_sha256"] == old_anchor


def test_stale_source_view_fails_before_archive_mutation(tmp_path, monkeypatch):
    authorities = _authorities(tmp_path)
    archive = _archive(tmp_path, authorities)
    plan = archive.plan(authorities.candidates, now=NOW + 100.0)
    old_anchor = archive.audit()["archive_anchor_sha256"]
    source_view = archive._source_view

    def changed_source_view():
        return {**source_view(), "authority_generation": 2}

    monkeypatch.setattr(archive, "_source_view", changed_source_view)
    with pytest.raises(
        AcquisitionSourceRetentionConflict,
        match="source view expired",
    ):
        archive.export(plan)
    assert archive.anchor_path.read_bytes()
    monkeypatch.setattr(archive, "_source_view", source_view)
    assert archive.audit()["archive_anchor_sha256"] == old_anchor


def test_oversized_raw_fails_before_base64_encoding(tmp_path, monkeypatch):
    authorities = _authorities(tmp_path)
    archive = _archive(
        tmp_path,
        authorities,
        limits=AcquisitionSourceRetentionLimits(
            min_terminal_age_seconds=10.0,
            max_segment_bytes=1024,
            max_total_archive_bytes=1024,
        ),
    )

    def forbidden_encode(_payload):
        raise AssertionError("base64 encoding must not run over budget")

    monkeypatch.setattr(
        acquisition_source_retention.base64,
        "b64encode",
        forbidden_encode,
    )
    with pytest.raises(
        AcquisitionSourceRetentionCapacityError,
        match="pre-encoding segment budget",
    ):
        archive.plan(authorities.candidates, now=NOW + 100.0)


class FaultOnce:
    def __init__(self, point: str, *, disk_full: bool = False) -> None:
        self.point = point
        self.disk_full = disk_full
        self.armed = False
        self.hit = False

    def __call__(self, point: str) -> None:
        if self.armed and not self.hit and point == self.point:
            self.hit = True
            if self.disk_full:
                raise OSError(errno.ENOSPC, "simulated full archive")
            raise RuntimeError(f"fault:{point}")


@pytest.mark.parametrize(
    "point",
    [
        "segment.after_stage_fsync",
        "segment.after_publish",
        "segment.after_directory_fsync",
        "manifest.after_stage_fsync",
        "manifest.after_publish",
        "manifest.after_directory_fsync",
        "anchor.before_stage",
        "anchor.after_stage_fsync",
        "anchor.after_publish",
        "anchor.after_directory_fsync",
    ],
)
def test_every_export_boundary_is_old_or_new_and_retryable(tmp_path, point):
    authorities = _authorities(tmp_path)
    fault = FaultOnce(point)
    archive = _archive(tmp_path, authorities, fault=fault)
    plan = archive.plan(authorities.candidates, now=NOW + 100.0)
    old_anchor = archive.audit()["archive_anchor_sha256"]
    fault.armed = True
    with pytest.raises(RuntimeError, match="fault"):
        archive.export(plan)

    reopened = _archive(tmp_path, authorities)
    after_fault = reopened.audit()
    assert after_fault["archive_anchor_sha256"] == old_anchor or (
        after_fault["archived_job_count"] == 1
    )
    recovered = reopened.export(plan)
    assert recovered.status in {"archived", "idempotent"}
    assert reopened.audit()["archived_job_count"] == 1
    assert authorities.object_paths[0].exists()


def test_more_than_one_immediate_orphan_segment_fails_closed(tmp_path):
    authorities = _authorities(tmp_path)
    fault = FaultOnce("segment.after_publish")
    archive = _archive(tmp_path, authorities, fault=fault)
    plan = archive.plan(authorities.candidates, now=NOW + 100.0)
    fault.armed = True
    with pytest.raises(RuntimeError, match="fault"):
        archive.export(plan)

    original_path = next(archive.segment_directory.glob("*.sealed.json"))
    original = json.loads(original_path.read_text(encoding="ascii"))
    body = dict(original)
    body.pop("segment_sha256")
    body["batch_sha256"] = _sha("different-immediate-successor")
    second_payload = acquisition_source_retention._envelope(
        acquisition_source_retention.SEGMENT_SCHEMA,
        "segment_sha256",
        body,
    )
    second = json.loads(second_payload.decode("ascii"))
    (archive.segment_directory / (
        f"{second['segment_sha256']}.sealed.json"
    )).write_bytes(second_payload)

    with pytest.raises(
        AcquisitionSourceRetentionCapacityError,
        match="orphan segment capacity",
    ):
        archive.audit()


def test_disk_full_partial_stage_restart_and_exact_retry(tmp_path):
    authorities = _authorities(tmp_path)
    fault = FaultOnce("segment.after_stage_fsync", disk_full=True)
    archive = _archive(tmp_path, authorities, fault=fault)
    plan = archive.plan(authorities.candidates, now=NOW + 100.0)
    old_anchor = archive.audit()["archive_anchor_sha256"]
    fault.armed = True
    with pytest.raises(
        AcquisitionSourceRetentionCapacityError,
        match="archive_filesystem_full",
    ):
        archive.export(plan)
    assert archive.anchor_path.read_bytes()

    reopened = _archive(tmp_path, authorities)
    assert reopened.audit()["archive_anchor_sha256"] == old_anchor
    assert tuple(reopened.staging_directory.iterdir()) == ()
    assert reopened.export(plan).status == "archived"


@pytest.mark.parametrize("mode", ["tamper", "gap"])
def test_segment_tamper_or_manifest_gap_is_detected(tmp_path, mode):
    authorities = _authorities(tmp_path)
    archive = _archive(tmp_path, authorities)
    plan = archive.plan(authorities.candidates, now=NOW + 100.0)
    archive.export(plan)
    segment = next(archive.segment_directory.glob("*.sealed.json"))
    if mode == "tamper":
        payload = bytearray(segment.read_bytes())
        payload[len(payload) // 2] ^= 1
        segment.write_bytes(payload)
    else:
        segment.unlink()
    with pytest.raises(AcquisitionSourceRetentionIntegrityError):
        archive.audit()


def test_anchor_tamper_and_unknown_staging_entry_fail_closed(tmp_path):
    authorities = _authorities(tmp_path)
    archive = _archive(tmp_path, authorities)
    anchor = bytearray(archive.anchor_path.read_bytes())
    anchor[-3] ^= 1
    archive.anchor_path.write_bytes(anchor)
    with pytest.raises(AcquisitionSourceRetentionIntegrityError):
        archive.audit()

    other = tmp_path / "other"
    authorities = _authorities(other)
    clean = _archive(other, authorities)
    (clean.staging_directory / "unexpected.bin").write_bytes(b"partial")
    with pytest.raises(AcquisitionSourceRetentionIntegrityError):
        clean.audit()


def test_row_segment_and_total_byte_caps_fail_before_anchor(tmp_path):
    authorities = _authorities(tmp_path, count=2, payload_bytes=20_000)
    row_archive = _archive(
        tmp_path,
        authorities,
        limits=AcquisitionSourceRetentionLimits(
            min_terminal_age_seconds=10.0,
            max_archived_jobs=1,
        ),
    )
    row_plan = row_archive.plan(authorities.candidates, now=NOW + 100.0)
    row_anchor = row_archive.audit()["archive_anchor_sha256"]
    with pytest.raises(AcquisitionSourceRetentionCapacityError, match="row"):
        row_archive.export(row_plan)
    assert row_archive.audit()["archive_anchor_sha256"] == row_anchor

    other = tmp_path / "bytes"
    byte_authorities = _authorities(other, payload_bytes=20_000)
    byte_archive = _archive(
        other,
        byte_authorities,
        limits=AcquisitionSourceRetentionLimits(
            min_terminal_age_seconds=10.0,
            max_segment_bytes=12_000,
            max_total_archive_bytes=12_000,
        ),
    )
    with pytest.raises(
        AcquisitionSourceRetentionCapacityError,
        match="pre-encoding segment budget",
    ):
        byte_archive.plan(
            byte_authorities.candidates,
            now=NOW + 100.0,
        )
    assert byte_archive.audit()["archived_job_count"] == 0


def test_concurrent_rotators_serialize_and_anchor_cas_is_explicit(tmp_path):
    authorities = _authorities(tmp_path)
    first = _archive(tmp_path, authorities)
    second = _archive(tmp_path, authorities)
    plan = first.plan(authorities.candidates, now=NOW + 100.0)
    old_anchor = first.audit()["archive_anchor_sha256"]
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = tuple(pool.map(lambda store: store.export(plan), (first, second)))
    assert sorted(item.status for item in results) == ["archived", "idempotent"]
    assert first.audit()["archived_job_count"] == 1
    with pytest.raises(AcquisitionSourceRetentionConflict, match="cas"):
        first.export(plan, expected_anchor_sha256=old_anchor)


def test_source_view_change_and_archive_authority_drift_fail_closed(tmp_path):
    authorities = _authorities(tmp_path)

    class ChangingViewRawStore(RawArtifactStore):
        def __init__(self, original: RawArtifactStore):
            super().__init__(original.root, limits=original.limits)
            self.calls = 0
            self.initialize()

        def stats(self):
            result = super().stats()
            self.calls += 1
            if self.calls >= 4:
                result = {**result, "catalog_bytes": result["catalog_bytes"] + 1}
            return result

    changing = ChangingViewRawStore(authorities.raw)
    archive = AcquisitionSourceRetentionArchive(
        (tmp_path / "changing-archive").resolve(),
        raw_store=changing,
        scheduler=authorities.scheduler,
        execution_store=authorities.executions,
        capacity_ledger=authorities.capacity,
        evidence_store=authorities.evidence,
        limits=AcquisitionSourceRetentionLimits(
            min_terminal_age_seconds=10.0
        ),
        clock=lambda: NOW + 100.0,
        directory_fsync=SYNC,
    )
    with pytest.raises(AcquisitionSourceRetentionConflict, match="source_view"):
        archive.plan(authorities.candidates, now=NOW + 100.0)

    stable = _archive(tmp_path, authorities)
    stable.export(stable.plan(authorities.candidates, now=NOW + 100.0))
    foreign = _authorities(tmp_path / "foreign")
    with pytest.raises(
        AcquisitionSourceRetentionIntegrityError,
        match="source authority",
    ):
        AcquisitionSourceRetentionArchive(
            stable.root,
            raw_store=foreign.raw,
            scheduler=foreign.scheduler,
            execution_store=foreign.executions,
            capacity_ledger=foreign.capacity,
            evidence_store=foreign.evidence,
            limits=AcquisitionSourceRetentionLimits(
                min_terminal_age_seconds=10.0
            ),
            clock=lambda: NOW + 100.0,
            directory_fsync=SYNC,
        )


def test_diagnostics_are_private_and_module_is_inert(tmp_path):
    authorities = _authorities(tmp_path)
    archive = _archive(tmp_path, authorities)
    archive.export(archive.plan(authorities.candidates, now=NOW + 100.0))
    encoded = json.dumps(archive.diagnostics(), sort_keys=True)
    assert len(encoded.encode("utf-8")) < 4096
    for forbidden in (
        "acquisition-job",
        "retention-source",
        "zone.home",
        str(tmp_path),
        "exact-source",
    ):
        assert forbidden not in encoded
    module = (
        Path(__file__).resolve().parents[1]
        / "storm_tracker_worker"
        / "acquisition_source_retention.py"
    ).read_text(encoding="utf-8").lower()
    assert "vacuum" not in module
    assert "service" not in module
    assert "mqtt" not in module
    assert "homeassistant" not in module
    service = (
        Path(__file__).resolve().parents[1]
        / "storm_tracker_worker"
        / "service.py"
    ).read_text(encoding="utf-8")
    assert "acquisition_source_retention" not in service


def test_directory_fsync_failure_is_fail_closed(tmp_path):
    authorities = _authorities(tmp_path)

    def unsupported(_path):
        raise AcquisitionSourceRetentionDurabilityError(
            "directory_fsync_unavailable"
        )

    with pytest.raises(
        AcquisitionSourceRetentionDurabilityError,
        match="directory_fsync_unavailable",
    ):
        _archive(
            tmp_path,
            authorities,
            directory_fsync=unsupported,
        )


def test_terminal_consumption_lookup_is_hot_archive_aware_and_rejects_rejection(
    tmp_path,
):
    authorities = _authorities(tmp_path)
    hot = authorities.evidence.load_terminal_consumption(
        "radar.dwd.retention", 1
    )
    assert hot is not None and hot.status == "hot"
    assert hot.producer_receipt.acquisition_job_id == "acquisition-job-0"
    authorities.evidence.rotate_archive(
        policy=EvidenceRetentionPolicy(), now=NOW + 20.0
    )
    archived = authorities.evidence.load_terminal_consumption(
        "radar.dwd.retention", 1
    )
    assert archived is not None and archived.status == "archived"
    assert archived.consumption_receipt == hot.consumption_receipt

    rejected = _authorities(tmp_path / "rejected", reject=True)
    with pytest.raises(AcquisitionEvidenceConflict, match="normal consumption"):
        rejected.evidence.load_terminal_consumption(
            "radar.dwd.retention", 1
        )
    rejected.evidence.rotate_archive(
        policy=EvidenceRetentionPolicy(), now=NOW + 20.0
    )
    with pytest.raises(AcquisitionEvidenceConflict, match="normal consumption"):
        rejected.evidence.load_terminal_consumption(
            "radar.dwd.retention", 1
        )


def test_contract_and_source_catalog_remain_explicitly_non_reclaiming(tmp_path):
    authorities = _authorities(tmp_path)
    archive = _archive(tmp_path, authorities)
    plan = archive.plan(authorities.candidates, now=NOW + 100.0)
    result = archive.export(plan)
    assert CONTRACT == "stv3-worker-acquisition-source-retention-v1"
    assert result.source_reclaimed is False
    assert authorities.raw.stats()["retention_policy"] == "none"
    assert all(path.exists() for path in authorities.object_paths)
