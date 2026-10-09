from __future__ import annotations

import ast
from collections import Counter
import copy
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import replace
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import threading
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from storm_tracker_worker import acquisition_scheduler_bridge
from storm_tracker_worker import autonomous_scheduler
from storm_tracker_worker import desired_state
from storm_tracker_worker import noaa_err_completion_consumer as subject
from storm_tracker_worker import noaa_err_native as native
from storm_tracker_worker import worker_acquisition_plan
from storm_tracker_worker.acquisition_activation import ACTIVATION_RULES
from storm_tracker_worker.acquisition_capture_recovery import (
    RECOVERY_BINDINGS,
    recovery_evidence_digest,
)
from storm_tracker_worker.acquisition_evidence_lifecycle import (
    AcquisitionEvidenceConflict,
    AcquisitionEvidenceLifecycleStore,
    ConsumptionReceipt,
)
from storm_tracker_worker.acquisition_execution_store import (
    ExecutionReceiptStore,
)
from storm_tracker_worker.canonical_evidence_store import CanonicalEvidenceStore
from storm_tracker_worker.hard_decoder_isolation import (
    DecoderProcessResult,
    HardDecoderExecutor,
    hard_boundary_support,
)
from storm_tracker_worker.noaa_err_completion_consumer import (
    CONSUMER_ID,
    NoaaErrCompletionCancelled,
    NoaaErrCompletionCapacityError,
    NoaaErrCompletionConflict,
    NoaaErrCompletionConsumer,
    NoaaErrCompletionDeadlineError,
    NoaaErrCompletionIntegrityError,
    NoaaErrCompletionIsolatedRunner,
    NoaaErrCompletionIsolationLimits,
    NoaaErrCompletionOutputLimits,
    NoaaErrCompletionOutputStore,
    NoaaErrRegionPolicy,
    OUTPUT_DISPOSITION,
    STREAM_ID,
    WORKLOAD_ID,
    issue_noaa_err_region_authority,
)
from storm_tracker_worker.noaa_err_quantitative import NoaaErrDerivedArtifactStore
from storm_tracker_worker.raw_artifacts import RawArtifactStore
from storm_tracker_worker.raw_capacity_ledger import (
    ProviderCapacityLimit,
    RawCapacityLedger,
    RawCapacityProfile,
)
from storm_tracker_worker.terminal_raw_completion_router import (
    TerminalRawCompletionRouter,
)
from tests.test_worker_noaa_err_quantitative import (
    _hdf_bytes,
    _listing_response,
    _listing_xml,
    _object_response,
)
from tests.test_worker_terminal_raw_completion_router import (
    _raw_limits,
)


NOW = 1_786_000_000.0
REPOSITORY = Path(__file__).resolve().parents[1]
SYNC = lambda _path: None
TEST_LIMITS = NoaaErrCompletionIsolationLimits(
    wall_seconds=15.0,
    cpu_seconds=12.0,
    rss_bytes=1536 * 1024 * 1024,
    max_request_bytes=1024 * 1024,
    max_result_bytes=16 * 1024,
)


class MutableClock:
    def __init__(self, value: float) -> None:
        self.value = value

    def __call__(self) -> float:
        return self.value


class FaultOnce:
    def __init__(self, point: str) -> None:
        self.point = point
        self.hit = False

    def __call__(self, point: str) -> None:
        if point == self.point and not self.hit:
            self.hit = True
            raise RuntimeError(f"fault:{point}")


class DdlDeleteRecreateAtFinalReproof:
    """Attempt the complete out-of-band row deletion under the output fence."""

    def __init__(self, store: NoaaErrCompletionOutputStore) -> None:
        self.path = store.path
        self.hit = False
        trigger = store._trigger_definitions()["completion_outputs_no_delete"]
        self.script = (
            "DROP TRIGGER completion_outputs_no_delete;"
            "DELETE FROM completion_outputs;"
            f"{trigger};"
        )

    def __call__(self, point: str) -> None:
        if point != "consumer.after_final_reproof_before_ack" or self.hit:
            return
        self.hit = True
        with closing(
            sqlite3.connect(self.path, timeout=0.01, isolation_level=None)
        ) as database:
            database.execute("PRAGMA busy_timeout=1")
            database.executescript(self.script)


def _downgrade_output_store_to_v1(path: Path) -> None:
    """Reconstruct the exact pre-sentinel schema from a valid v2 store."""

    with closing(sqlite3.connect(path, isolation_level=None)) as database:
        database.execute("BEGIN IMMEDIATE")
        database.execute("DROP TRIGGER output_metadata_no_update")
        database.execute("DROP TRIGGER output_metadata_no_delete")
        database.execute(
            "UPDATE output_metadata SET contract=?,schema_version=? "
            "WHERE singleton=1",
            (
                subject.LEGACY_OUTPUT_STORE_CONTRACT,
                subject.LEGACY_OUTPUT_STORE_SCHEMA_VERSION,
            ),
        )
        database.execute(
            "ALTER TABLE output_metadata DROP COLUMN store_instance_sha256"
        )
        database.execute("ALTER TABLE output_metadata DROP COLUMN schema_version")
        database.commit()


class InlineExecutor:
    """Exercise the exact child target without a second pytest process."""

    def __init__(self) -> None:
        self.requests: list[dict] = []
        self.results: list[dict] = []

    def execute(self, *, target, request, effect_contract, cancel_event):
        assert target == subject.PROCESS_TARGET
        assert effect_contract is subject.PROCESS_EFFECT_CONTRACT
        self.requests.append(request)
        result = subject._noaa_err_completion_child_entry(request)
        self.results.append(result)
        return DecoderProcessResult("succeeded", result=result, elapsed_seconds=0.01)


class ScriptedExecutor:
    def __init__(self, outcome: str, detail_code: str = "scripted") -> None:
        self.outcome = outcome
        self.detail_code = detail_code
        self.requests: list[dict] = []

    def execute(self, *, target, request, effect_contract, cancel_event):
        self.requests.append(request)
        return DecoderProcessResult(
            self.outcome,
            detail_code=self.detail_code,
            elapsed_seconds=0.01,
        )


class NeverExecutor:
    def __init__(self) -> None:
        self.calls = 0

    def execute(self, **_kwargs):
        self.calls += 1
        raise AssertionError("durable exact retry must not spawn")


def _desired_state() -> desired_state.DesiredState:
    return desired_state.parse_desired_state(
        {
            "authority_epoch": 1,
            "contract": desired_state.CONTRACT,
            "generation": 1,
            "issued_at": NOW - 60.0,
            "previous_state_sha256": None,
            "producer_id": "ha-noaa-err-completion-test",
            "settings": {
                "ground_radius_km": 20.0,
                "lightning_source_mode": "auto",
                "model_guidance_enabled": True,
                "region_sharing_distance_km": 150.0,
            },
            "targets": [
                {
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
                }
            ],
        }
    )


def _event_environment(monkeypatch, tmp_path: Path):
    state = _desired_state()
    plan = worker_acquisition_plan.build_worker_acquisition_plan(state)
    jobs = worker_acquisition_plan.build_worker_acquisition_slot_jobs(plan, as_of=NOW)
    job = next(item for item in jobs if item.workload_id == WORKLOAD_ID)
    schedule = next(
        item
        for item in acquisition_scheduler_bridge.build_acquisition_schedule_intents(plan)
        if item.intent_id == job.intent_id
    )

    desired_store = desired_state.DesiredStateStore((tmp_path / "desired.sqlite3").resolve())
    desired_store.claim_producer(
        state.producer_id, "noaa-completion-test-claim", claimed_at=NOW - 100.0
    )
    desired_store.apply(state.as_dict(), received_at=NOW - 59.0)
    scheduler = autonomous_scheduler.AutonomousScheduler(
        (tmp_path / "scheduler.sqlite3").resolve(),
        desired_state_store=desired_store,
        plan_builder=lambda _state: (schedule,),
        clock=lambda: NOW,
        lease_seconds=60.0,
    )
    scheduler.reconcile(now=NOW)
    claim = scheduler.claim_next("noaa-acquisition-owner", now=NOW)
    assert claim is not None

    rule = ACTIVATION_RULES[WORKLOAD_ID]
    recovery = RECOVERY_BINDINGS[str(rule.handler_id)]
    raw = RawArtifactStore((tmp_path / "source-raw").resolve(), limits=_raw_limits())
    raw.initialize()
    hdf = _hdf_bytes()
    listing = _listing_xml(len(hdf))
    monkeypatch.setattr(native, "_fetch_listing", lambda _prefix: _listing_response(listing))
    monkeypatch.setattr(native, "_fetch_object", lambda _selected: _object_response(hdf))
    captured = native.capture_latest_noaa_err_netcdf4(
        SimpleNamespace(job_id=job.job_id, payload={}),
        raw_store=raw,
        now=datetime(2026, 7, 19, 0, 20, tzinfo=timezone.utc),
    )
    capture_id = captured["raw_provenance"]["raw_capture_id"]
    bundle = raw.load_capture_bundle(capture_id)
    assert bundle is not None
    capture, receipts = bundle
    evidence = recovery_evidence_digest(
        binding=recovery,
        receipts_by_role=receipts,
        capture=capture,
    )

    capacity = RawCapacityLedger(
        (tmp_path / "capacity.sqlite3").resolve(),
        profile=RawCapacityProfile(
            global_max_bytes=512 * 1024 * 1024,
            global_max_entries=512,
            providers=(ProviderCapacityLimit(recovery.ledger_provider, 512 * 1024 * 1024, 512),),
            max_reservations=128,
            max_catalog_bytes=8 * 1024 * 1024,
            sqlite_busy_timeout_seconds=2.0,
        ),
        clock=lambda: NOW,
    )
    reservation_id = "reservation-" + hashlib.sha256(job.job_id.encode("ascii")).hexdigest()
    capacity.reserve(
        reservation_id=reservation_id,
        provider=recovery.ledger_provider,
        job_identity=job.job_id,
        requested_bytes=recovery.maximum_transport_bytes,
        requested_entries=recovery.maximum_artifacts,
    )
    capacity_receipt = capacity.commit(
        reservation_id=reservation_id,
        actual_bytes=evidence.actual_bytes,
        actual_entries=evidence.actual_entries,
        content_sha256=evidence.content_sha256,
        receipt_sha256=evidence.receipt_sha256,
        capture_sha256=evidence.capture_sha256,
    )
    executions = ExecutionReceiptStore(
        (tmp_path / "execution.sqlite3").resolve(), clock=lambda: NOW
    )
    terminal = executions.record(
        scheduler_job_id=claim.job_id,
        acquisition_job_id=job.job_id,
        binding_sha256=hashlib.sha256(b"noaa-execution-binding").hexdigest(),
        attempt_count=claim.attempt_count,
        outcome="completed_shadow_raw",
        reason_code="shadow_raw_capture_complete",
        terminal=True,
        content_sha256=evidence.content_sha256,
        raw_receipt_sha256=evidence.receipt_sha256,
        capture_sha256=evidence.capture_sha256,
        capacity_commit_sha256=capacity_receipt.commit_sha256,
    )
    scheduler.complete(claim.job_id, claim.lease_token, now=NOW + 1.0)

    clock = MutableClock(NOW + 3.0)
    token_counter = iter(range(1, 100))
    lifecycle = AcquisitionEvidenceLifecycleStore(
        (tmp_path / "lifecycle").resolve(),
        clock=clock,
        monotonic=lambda: 1.0,
        token_factory=lambda: f"{next(token_counter):064x}",
        directory_fsync=SYNC,
    )
    router = TerminalRawCompletionRouter(
        lifecycle_store=lifecycle,
        desired_state_store=desired_store,
        scheduler=scheduler,
        execution_store=executions,
        capacity_ledger=capacity,
        source_raw_store=raw,
    )
    routed = router.publish_next()
    assert routed.status == "published"
    assert routed.reason_code == "verified_capture_reference_published"
    return {
        "capture": capture,
        "clock": clock,
        "desired": desired_store,
        "job": job,
        "lifecycle": lifecycle,
        "raw": raw,
        "receipts": receipts,
        "state": state,
        "terminal": terminal,
    }


def _consumer_stack(
    env,
    tmp_path: Path,
    *,
    executor=None,
    consumer_fault=None,
    output_limits: NoaaErrCompletionOutputLimits | None = None,
    limits: NoaaErrCompletionIsolationLimits = TEST_LIMITS,
):
    artifacts = NoaaErrDerivedArtifactStore((tmp_path / "derived").resolve())
    artifacts.initialize()
    evidence = CanonicalEvidenceStore(
        (tmp_path / "canonical.sqlite3").resolve(),
        raw_artifact_reader=env["raw"],
        clock=env["clock"],
    )
    outputs = NoaaErrCompletionOutputStore(
        (tmp_path / "outputs.sqlite3").resolve(),
        limits=output_limits,
        clock=env["clock"],
    )
    runner = NoaaErrCompletionIsolatedRunner(
        raw_store=env["raw"],
        artifact_store=artifacts,
        evidence_store=evidence,
        output_store=outputs,
        limits=limits,
        executor=executor,
    )
    consumer = NoaaErrCompletionConsumer(
        lifecycle_store=env["lifecycle"],
        desired_state_store=env["desired"],
        raw_store=env["raw"],
        artifact_store=artifacts,
        evidence_store=evidence,
        output_store=outputs,
        process_runner=runner,
        clock=env["clock"],
        fault_injector=consumer_fault,
    )
    return consumer, runner, artifacts, evidence, outputs


def _claim(env, owner: str = "noaa-test-owner"):
    lease = env["lifecycle"].claim_next(
        CONSUMER_ID,
        STREAM_ID,
        owner,
        lease_seconds=60.0,
        now=env["clock"](),
    )
    assert lease is not None
    return lease


def _supersede_desired(env, *, issued_at: float = NOW + 5.0):
    active = env["desired"].load_active()
    assert active is not None
    document = active.as_dict()
    document["generation"] += 1
    document["issued_at"] = issued_at
    document["previous_state_sha256"] = active.state_sha256
    document["targets"][0]["position_observed_at"] = issued_at
    state = desired_state.parse_desired_state(document)
    env["desired"].apply(state.as_dict(), received_at=issued_at + 1.0)
    return state


def test_exact_router_event_decodes_once_and_stays_factless(monkeypatch, tmp_path):
    env = _event_environment(monkeypatch, tmp_path / "event")
    executor = InlineExecutor()
    calls = 0
    exact_decoder = subject.decode_noaa_err_region

    def spy(**kwargs):
        nonlocal calls
        calls += 1
        return exact_decoder(**kwargs)

    monkeypatch.setattr(subject, "decode_noaa_err_region", spy)
    consumer, _runner, _artifacts, _evidence, outputs = _consumer_stack(
        env, tmp_path / "consumer", executor=executor
    )
    result = consumer.run_once("noaa-reference-consumer", lease_seconds=60.0)

    assert result.disposition == "processed"
    assert result.consumption_receipt is not None
    assert result.output_receipt is not None
    assert calls == 1
    assert [request["purpose"] for request in executor.requests] == [
        subject.PROCESS_PURPOSE_DECODE,
        subject.PROCESS_PURPOSE_FINAL_REPROOF,
    ]
    assert [envelope["code"] for envelope in executor.results] == [
        "hard_process_output_committed",
        "hard_process_final_authorities_reproved",
    ]
    assert result.output_receipt.output["claims"] == {
        "decoded_shadow_candidate": True,
        "fact_staged": False,
        "meteorological_admission_granted": False,
        "operational_snapshot_published": False,
        "raw_refetch_performed": False,
    }
    assert result.output_receipt.output["disposition"] == OUTPUT_DISPOSITION
    assert result.output_receipt.output["fact_revision"] is None
    authority = subject.NoaaErrRegionAuthority.from_mapping(
        result.output_receipt.output["region_authority"]
    )
    assert authority.acquisition_job_id == env["job"].job_id
    assert authority.pixels <= subject.quantitative.MAX_REGION_PIXELS
    assert result.consumption_receipt.output_sha256 == result.output_receipt.output_sha256

    persisted = json.dumps(result.output_receipt.output, sort_keys=True)
    diagnostic = json.dumps(outputs.diagnostics(), sort_keys=True)
    assert str(tmp_path) not in persisted + diagnostic
    assert "private" not in persisted.casefold()
    request_text = json.dumps(executor.requests, sort_keys=True)
    assert "lease_token" not in request_text
    assert "raw_payload" not in request_text
    assert str(tmp_path) not in persisted + diagnostic
    for forbidden in ("api_key", "password", "authorization", "private_"):
        assert forbidden not in (persisted + diagnostic + request_text).casefold()


def test_crash_after_output_commit_restarts_without_decode_or_duplicate(monkeypatch, tmp_path):
    env = _event_environment(monkeypatch, tmp_path / "event")
    executor = InlineExecutor()
    fault = FaultOnce("consumer.after_output_commit")
    consumer, _runner, artifacts, evidence, outputs = _consumer_stack(
        env, tmp_path / "consumer", executor=executor, consumer_fault=fault
    )
    with pytest.raises(RuntimeError, match="consumer.after_output_commit"):
        consumer.run_once("crashing-owner", lease_seconds=60.0)
    assert len(executor.requests) == 1
    receipt = outputs.load(executor.requests[0]["binding"]["event_id"])
    assert receipt is not None

    env["clock"].value += 61.0
    verifier = InlineExecutor()
    runner = NoaaErrCompletionIsolatedRunner(
        raw_store=env["raw"],
        artifact_store=artifacts,
        evidence_store=evidence,
        output_store=outputs,
        limits=TEST_LIMITS,
        executor=verifier,
    )
    recovered = NoaaErrCompletionConsumer(
        lifecycle_store=env["lifecycle"],
        desired_state_store=env["desired"],
        raw_store=env["raw"],
        artifact_store=artifacts,
        evidence_store=evidence,
        output_store=outputs,
        process_runner=runner,
        clock=env["clock"],
    ).run_once("recovery-owner", lease_seconds=60.0)
    assert recovered.disposition == "recovered_output"
    assert recovered.output_receipt.output_sha256 == receipt.output_sha256
    assert len(verifier.requests) == 1
    assert verifier.requests[0]["purpose"] == subject.PROCESS_PURPOSE_FINAL_REPROOF


def test_tampered_router_reference_and_raw_object_fail_before_spawn(monkeypatch, tmp_path):
    env = _event_environment(monkeypatch, tmp_path / "event-reference")
    scripted = ScriptedExecutor("deadline_exceeded")
    consumer, _runner, _artifacts, _evidence, outputs = _consumer_stack(
        env, tmp_path / "consumer-reference", executor=scripted
    )
    lease = _claim(env)
    forged = replace(lease, payload=lease.payload + b" ")
    with pytest.raises((NoaaErrCompletionIntegrityError, NoaaErrCompletionConflict)):
        consumer.process_leased(forged, env["state"], threading.Event())
    assert scripted.requests == []
    assert outputs.load(lease.event_id) is None

    env2 = _event_environment(monkeypatch, tmp_path / "event-raw")
    scripted2 = ScriptedExecutor("deadline_exceeded")
    consumer2, _runner2, _a2, _e2, outputs2 = _consumer_stack(
        env2, tmp_path / "consumer-raw", executor=scripted2
    )
    netcdf = env2["receipts"]["netcdf4"]
    Path(netcdf.artifact_path).write_bytes(b"tampered")
    lease2 = _claim(env2)
    with pytest.raises(NoaaErrCompletionIntegrityError):
        consumer2.process_leased(lease2, env2["state"], threading.Event())
    assert scripted2.requests == []
    assert outputs2.load(lease2.event_id) is None


@pytest.mark.parametrize(
    ("process_outcome", "error_type"),
    [
        ("deadline_exceeded", NoaaErrCompletionDeadlineError),
        ("cpu_limit_exceeded", NoaaErrCompletionCapacityError),
        ("memory_limit_exceeded", NoaaErrCompletionCapacityError),
        ("result_too_large", NoaaErrCompletionCapacityError),
    ],
)
def test_hard_process_terminal_limits_never_ack(monkeypatch, tmp_path, process_outcome, error_type):
    env = _event_environment(monkeypatch, tmp_path / process_outcome)
    executor = ScriptedExecutor(process_outcome)
    consumer, _runner, _artifacts, _evidence, outputs = _consumer_stack(
        env, tmp_path / f"consumer-{process_outcome}", executor=executor
    )
    lease = _claim(env)
    with pytest.raises(error_type):
        consumer.process_leased(lease, env["state"], threading.Event())
    assert outputs.load(lease.event_id) is None


def test_cancel_and_short_lease_fail_before_spawn(monkeypatch, tmp_path):
    env = _event_environment(monkeypatch, tmp_path / "cancel-event")
    executor = ScriptedExecutor("succeeded")
    consumer, _runner, _artifacts, _evidence, outputs = _consumer_stack(
        env, tmp_path / "cancel-consumer", executor=executor
    )
    lease = _claim(env)
    cancelled = threading.Event()
    cancelled.set()
    with pytest.raises(NoaaErrCompletionCancelled):
        consumer.process_leased(lease, env["state"], cancelled)
    assert executor.requests == []
    assert outputs.load(lease.event_id) is None

    env["clock"].value = lease.lease_expires_at - 10.0
    with pytest.raises(NoaaErrCompletionDeadlineError):
        consumer.process_leased(lease, env["state"], threading.Event())
    assert executor.requests == []


def test_missing_target_region_authority_is_fail_closed_before_decode(monkeypatch, tmp_path):
    env = _event_environment(monkeypatch, tmp_path / "event")
    lease = _claim(env)
    plan, job = subject._job_from_router_proof(env["state"], lease)
    missing = replace(job, region_ids=(), target_ids=())
    monkeypatch.setattr(subject, "_job_from_router_proof", lambda _state, _lease: (plan, missing))
    with pytest.raises(NoaaErrCompletionIntegrityError, match="target-region binding is absent"):
        issue_noaa_err_region_authority(desired_state=env["state"], lease=lease)

    mismatched = replace(job, target_ids=("zone.forged",))
    monkeypatch.setattr(
        subject,
        "_job_from_router_proof",
        lambda _state, _lease: (plan, mismatched),
    )
    with pytest.raises(
        NoaaErrCompletionIntegrityError,
        match="target-region/target/radius authority differs",
    ):
        issue_noaa_err_region_authority(desired_state=env["state"], lease=lease)


def test_child_rejects_rehashed_cross_authority_binding(monkeypatch, tmp_path):
    env = _event_environment(monkeypatch, tmp_path / "event")
    lease = _claim(env)
    binding = subject._validate_router_lease(
        lease, env["state"], env["raw"], NoaaErrRegionPolicy()
    ).to_mapping()
    region = binding["region_authority"]
    region["desired_state_sha256"] = "f" * 64
    region_body = dict(region)
    region_body.pop("authority_sha256")
    region["authority_sha256"] = subject._digest(region_body)
    with pytest.raises(NoaaErrCompletionIntegrityError, match="region relation differs"):
        subject.NoaaErrLeaseBinding.from_mapping(binding)


def test_concurrent_duplicate_call_decodes_once(monkeypatch, tmp_path):
    env = _event_environment(monkeypatch, tmp_path / "event")
    executor = InlineExecutor()
    consumer, _runner, _artifacts, _evidence, outputs = _consumer_stack(
        env, tmp_path / "consumer", executor=executor
    )
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(
            pool.map(
                lambda owner: consumer.run_once(owner, lease_seconds=60.0),
                ("concurrent-owner-a", "concurrent-owner-b"),
            )
        )
    assert sorted(item.disposition for item in results) == ["idle", "processed"]
    assert [request["purpose"] for request in executor.requests] == [
        subject.PROCESS_PURPOSE_DECODE,
        subject.PROCESS_PURPOSE_FINAL_REPROOF,
    ]
    assert outputs.diagnostics()["receipts"] == 1


def test_output_store_detects_tamper_and_enforces_receipt_cap(monkeypatch, tmp_path):
    env = _event_environment(monkeypatch, tmp_path / "event")
    consumer, _runner, _artifacts, _evidence, outputs = _consumer_stack(
        env, tmp_path / "consumer", executor=InlineExecutor()
    )
    result = consumer.run_once("tamper-owner", lease_seconds=60.0)
    assert result.output_receipt is not None
    with closing(sqlite3.connect(outputs.path)) as database:
        body = bytearray(
            database.execute(
                "SELECT output_json FROM completion_outputs WHERE event_id=?",
                (result.event_id,),
            ).fetchone()[0]
        )
        body[-2] = ord("X")
        database.execute("DROP TRIGGER completion_outputs_no_update")
        database.execute(
            "CREATE TRIGGER completion_outputs_no_update "
            "BEFORE UPDATE ON completion_outputs BEGIN SELECT 1; END"
        )
        database.execute(
            "UPDATE completion_outputs SET output_json=? WHERE event_id=?",
            (bytes(body), result.event_id),
        )
        database.commit()
    with pytest.raises(NoaaErrCompletionIntegrityError):
        outputs.load(result.event_id)

    cap_store = NoaaErrCompletionOutputStore(
        (tmp_path / "cap.sqlite3").resolve(),
        limits=NoaaErrCompletionOutputLimits(max_receipts=1),
        clock=env["clock"],
    )
    original = result.output_receipt.output
    first = cap_store.commit(original)
    second = json.loads(json.dumps(original))
    second["event"]["event_id"] = "completion-" + "f" * 64
    second["event"]["event_sha256"] = "e" * 64
    second["event"]["producer_receipt_sha256"] = "d" * 64
    second["event"]["stream_sequence"] += 1
    with pytest.raises(NoaaErrCompletionCapacityError):
        cap_store.commit(second)
    assert cap_store.load(first.event_id) is not None


def test_output_store_v1_migrates_atomically_and_preserves_receipt(
    monkeypatch,
    tmp_path,
):
    env = _event_environment(monkeypatch, tmp_path / "event")
    consumer, _runner, _artifacts, _evidence, outputs = _consumer_stack(
        env,
        tmp_path / "consumer",
        executor=InlineExecutor(),
    )
    result = consumer.run_once("migration-owner", lease_seconds=60.0)
    assert result.output_receipt is not None
    expected_output_sha = result.output_receipt.output_sha256
    path = outputs.path
    _downgrade_output_store_to_v1(path)

    migrated = NoaaErrCompletionOutputStore(path, clock=env["clock"])
    loaded = migrated.load(result.event_id)
    assert loaded is not None
    assert loaded.output_sha256 == expected_output_sha
    assert subject._SHA.fullmatch(migrated.store_instance_sha256) is not None
    identity = migrated.identity_sha256
    assert subject._SHA.fullmatch(identity) is not None
    with closing(sqlite3.connect(path)) as database:
        metadata = database.execute(
            "SELECT contract,schema_version,store_instance_sha256 "
            "FROM output_metadata WHERE singleton=1"
        ).fetchone()
        triggers = {
            str(row[0])
            for row in database.execute(
                "SELECT name FROM sqlite_master WHERE type='trigger'"
            )
        }
    assert metadata == (
        subject.OUTPUT_STORE_CONTRACT,
        subject.OUTPUT_STORE_SCHEMA_VERSION,
        migrated.store_instance_sha256,
    )
    assert triggers == set(migrated._trigger_definitions())
    assert NoaaErrCompletionOutputStore(
        path,
        clock=env["clock"],
    ).identity_sha256 == identity


def test_output_store_v1_migration_faults_are_old_or_new(tmp_path):
    rollback_path = (tmp_path / "migration-rollback.sqlite3").resolve()
    NoaaErrCompletionOutputStore(rollback_path)
    _downgrade_output_store_to_v1(rollback_path)
    with pytest.raises(RuntimeError, match="migration.after_metadata"):
        NoaaErrCompletionOutputStore(
            rollback_path,
            fault_injector=FaultOnce("migration.after_metadata"),
        )
    with closing(sqlite3.connect(rollback_path)) as database:
        columns = {
            str(row[1])
            for row in database.execute("PRAGMA table_info(output_metadata)")
        }
        metadata = database.execute(
            "SELECT contract FROM output_metadata WHERE singleton=1"
        ).fetchone()
    assert columns == {"singleton", "contract", "limits_sha256"}
    assert metadata == (subject.LEGACY_OUTPUT_STORE_CONTRACT,)
    rolled_forward = NoaaErrCompletionOutputStore(rollback_path)
    assert subject._SHA.fullmatch(rolled_forward.store_instance_sha256) is not None

    committed_path = (tmp_path / "migration-committed.sqlite3").resolve()
    NoaaErrCompletionOutputStore(committed_path)
    _downgrade_output_store_to_v1(committed_path)
    with pytest.raises(RuntimeError, match="migration.after_commit"):
        NoaaErrCompletionOutputStore(
            committed_path,
            fault_injector=FaultOnce("migration.after_commit"),
        )
    committed = NoaaErrCompletionOutputStore(committed_path)
    assert subject._SHA.fullmatch(committed.store_instance_sha256) is not None


def test_output_store_live_sentinel_tamper_and_replacement_fail_closed(tmp_path):
    path = (tmp_path / "identity.sqlite3").resolve()
    store = NoaaErrCompletionOutputStore(path)
    instance_sha = store.store_instance_sha256
    identity_sha = store.identity_sha256
    with closing(sqlite3.connect(path)) as database:
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            database.execute(
                "UPDATE output_metadata SET store_instance_sha256=? "
                "WHERE singleton=1",
                ("f" * 64,),
            )
        database.rollback()
        database.execute("DROP TRIGGER output_metadata_no_update")
        database.execute(
            "UPDATE output_metadata SET store_instance_sha256=? "
            "WHERE singleton=1",
            ("f" * 64,),
        )
        database.execute(
            store._trigger_definitions()["output_metadata_no_update"]
        )
        database.commit()
    with pytest.raises(NoaaErrCompletionIntegrityError, match="instance authority"):
        store.assert_instance_held(identity_sha)

    replacement_path = (tmp_path / "replacement.sqlite3").resolve()
    replacement = NoaaErrCompletionOutputStore(replacement_path)
    replacement_instance = replacement.store_instance_sha256
    assert replacement_instance != instance_sha
    os.replace(replacement.path, path)
    with pytest.raises(NoaaErrCompletionIntegrityError, match="instance changed"):
        _ = store.identity_sha256
    reopened = NoaaErrCompletionOutputStore(path)
    assert reopened.store_instance_sha256 == replacement_instance
    assert reopened.identity_sha256 != identity_sha


def test_constructor_bound_replacement_never_acks_and_exact_restart_recovers(
    monkeypatch,
    tmp_path,
):
    env = _event_environment(monkeypatch, tmp_path / "event")
    executor = InlineExecutor()
    consumer, _runner, _artifacts, _evidence, outputs = _consumer_stack(
        env,
        tmp_path / "consumer",
        executor=executor,
    )
    lease = _claim(env, "replacement-owner")
    pending = consumer.begin_leased(lease, env["state"], threading.Event())
    original_identity = consumer._output_store_identity_sha256
    replacement_path = (tmp_path / "replacement-copy.sqlite3").resolve()
    shutil.copy2(outputs.path, replacement_path)
    os.replace(replacement_path, outputs.path)

    with pytest.raises(NoaaErrCompletionIntegrityError, match="instance changed"):
        consumer.finalize_leased(pending, threading.Event())

    assert env["lifecycle"].diagnostics()["outbox_states"]["consumed"] == 0
    assert [request["purpose"] for request in executor.requests] == [
        subject.PROCESS_PURPOSE_DECODE,
    ]
    env["clock"].value += 61.0
    recovery_executor = InlineExecutor()
    recovered_consumer, _runner, _artifacts, _evidence, recovered_outputs = (
        _consumer_stack(
            env,
            tmp_path / "consumer",
            executor=recovery_executor,
        )
    )
    assert recovered_consumer._output_store_identity_sha256 != original_identity
    recovered = recovered_consumer.run_once(
        "replacement-recovery-owner",
        lease_seconds=60.0,
    )
    assert recovered.disposition == "recovered_output"
    assert recovered.output_receipt is not None
    assert recovered_outputs.diagnostics()["receipts"] == 1
    assert [request["purpose"] for request in recovery_executor.requests] == [
        subject.PROCESS_PURPOSE_FINAL_REPROOF,
    ]


def test_completion_reproof_lease_blocks_ddl_delete_recreate_then_recovers(
    monkeypatch,
    tmp_path,
):
    env = _event_environment(monkeypatch, tmp_path / "event")
    executor = InlineExecutor()
    consumer, _runner, _artifacts, _evidence, outputs = _consumer_stack(
        env,
        tmp_path / "consumer",
        executor=executor,
    )
    attack = DdlDeleteRecreateAtFinalReproof(outputs)
    consumer._fault_injector = attack

    with pytest.raises(sqlite3.OperationalError, match="locked"):
        consumer.run_once("ddl-race-owner", lease_seconds=60.0)

    assert attack.hit
    assert outputs.diagnostics()["receipts"] == 1
    assert env["lifecycle"].diagnostics()["outbox_states"]["leased"] == 1
    assert env["lifecycle"].diagnostics()["outbox_states"]["consumed"] == 0
    with closing(sqlite3.connect(outputs.path)) as database:
        assert database.execute(
            "SELECT count(*) FROM completion_outputs"
        ).fetchone()[0] == 1
        assert {
            str(row[0])
            for row in database.execute(
                "SELECT name FROM sqlite_master WHERE type='trigger'"
            )
        } == set(outputs._trigger_definitions())

    env["clock"].value += 61.0
    recovered = consumer.run_once("ddl-recovery-owner", lease_seconds=60.0)
    assert recovered.disposition == "recovered_output"
    assert outputs.diagnostics()["receipts"] == 1
    assert env["lifecycle"].diagnostics()["outbox_states"]["consumed"] == 1
    assert [request["purpose"] for request in executor.requests] == [
        subject.PROCESS_PURPOSE_DECODE,
        subject.PROCESS_PURPOSE_FINAL_REPROOF,
        subject.PROCESS_PURPOSE_FINAL_REPROOF,
    ]


@pytest.mark.skipif(
    os.name != "posix",
    reason="POSIX permits rename over an open SQLite output path",
)
def test_posix_replacement_at_fenced_ack_boundary_never_succeeds_silently(
    monkeypatch,
    tmp_path,
):
    env = _event_environment(monkeypatch, tmp_path / "event")
    consumer, _runner, _artifacts, _evidence, outputs = _consumer_stack(
        env,
        tmp_path / "consumer",
        executor=InlineExecutor(),
    )
    replacement = NoaaErrCompletionOutputStore(
        (tmp_path / "replacement.sqlite3").resolve(),
        clock=env["clock"],
    )
    replaced = False

    def replace_at_boundary(point: str) -> None:
        nonlocal replaced
        if point != "consumer.after_final_reproof_before_ack":
            return
        os.replace(replacement.path, outputs.path)
        replaced = True

    consumer._fault_injector = replace_at_boundary
    with pytest.raises(NoaaErrCompletionIntegrityError, match="instance changed"):
        consumer.run_once("posix-replacement-owner", lease_seconds=60.0)

    assert replaced
    assert env["lifecycle"].diagnostics()["outbox_states"]["consumed"] == 0


def test_stale_desired_is_rejected_once_instead_of_reclaiming_the_head(
    monkeypatch,
    tmp_path,
):
    env = _event_environment(monkeypatch, tmp_path / "event")
    executor = NeverExecutor()
    consumer, _runner, _artifacts, _evidence, outputs = _consumer_stack(
        env,
        tmp_path / "consumer",
        executor=executor,
    )
    _supersede_desired(env)

    result = consumer.run_once("obsolete-owner", lease_seconds=60.0)

    assert result.disposition == "rejected"
    assert result.reason_code == "desired_state_superseded_before_processing"
    assert result.consumption_receipt is not None
    assert executor.calls == 0
    assert outputs.diagnostics()["receipts"] == 0
    assert env["lifecycle"].diagnostics()["rejection_receipts"] == 1
    assert consumer.run_once("after-rejection", lease_seconds=60.0).disposition == "idle"


def test_desired_switch_before_final_reproof_rejects_durable_output(
    monkeypatch,
    tmp_path,
):
    env = _event_environment(monkeypatch, tmp_path / "event")
    executor = InlineExecutor()
    switched = False

    def switch_desired(point):
        nonlocal switched
        if point == "consumer.before_final_reproof" and not switched:
            switched = True
            _supersede_desired(env)

    consumer, _runner, _artifacts, _evidence, outputs = _consumer_stack(
        env,
        tmp_path / "consumer",
        executor=executor,
        consumer_fault=switch_desired,
    )

    result = consumer.run_once("desired-race", lease_seconds=60.0)

    assert result.disposition == "rejected"
    assert result.reason_code == "desired_state_changed_before_ack"
    assert result.consumption_receipt is not None
    assert outputs.diagnostics()["receipts"] == 1
    assert [request["purpose"] for request in executor.requests] == [subject.PROCESS_PURPOSE_DECODE]
    assert consumer.run_once("after-desired-race", lease_seconds=60.0).disposition == "idle"


@pytest.mark.parametrize(
    ("changed_load", "message", "expected_processes"),
    [
        (4, "before final reproof", 1),
        (5, "acknowledgement boundary", 2),
    ],
)
def test_lease_change_at_each_pre_ack_barrier_never_acknowledges(
    monkeypatch,
    tmp_path,
    changed_load,
    message,
    expected_processes,
):
    env = _event_environment(monkeypatch, tmp_path / f"event-{changed_load}")
    executor = InlineExecutor()
    consumer, _runner, _artifacts, _evidence, _outputs = _consumer_stack(
        env,
        tmp_path / f"consumer-{changed_load}",
        executor=executor,
    )
    original = env["lifecycle"].load_leased
    calls = 0

    def changed_lease(event_id, lease_token):
        nonlocal calls
        calls += 1
        lease = original(event_id, lease_token)
        if calls == changed_load:
            return replace(lease, attempt_count=lease.attempt_count + 1)
        return lease

    monkeypatch.setattr(env["lifecycle"], "load_leased", changed_lease)

    with pytest.raises(NoaaErrCompletionConflict, match=message):
        consumer.run_once(f"lease-race-{changed_load}", lease_seconds=60.0)

    assert len(executor.requests) == expected_processes
    assert env["lifecycle"].diagnostics()["outbox_states"]["consumed"] == 0


@pytest.mark.parametrize("poison", ["raw", "artifact"])
def test_final_hard_reproof_detects_raw_and_candidate_artifact_poison(
    monkeypatch,
    tmp_path,
    poison,
):
    env = _event_environment(monkeypatch, tmp_path / f"event-{poison}")
    executor = InlineExecutor()
    consumer, _runner, artifacts, _evidence, outputs = _consumer_stack(
        env,
        tmp_path / f"consumer-{poison}",
        executor=executor,
    )
    poisoned = False

    def poison_at_barrier(point):
        nonlocal poisoned
        if point != "consumer.before_final_reproof" or poisoned:
            return
        poisoned = True
        if poison == "raw":
            Path(env["receipts"]["netcdf4"].artifact_path).write_bytes(b"tampered")
            return
        event_id = executor.requests[0]["binding"]["event_id"]
        receipt = outputs.load(event_id)
        assert receipt is not None
        artifact_sha = receipt.output["decoder"]["artifact_sha256"]
        (artifacts.objects_path / f"{artifact_sha}.neqr").write_bytes(b"tampered")

    consumer._fault_injector = poison_at_barrier

    with pytest.raises(NoaaErrCompletionIntegrityError):
        consumer.run_once(f"poison-{poison}", lease_seconds=60.0)

    assert len(executor.requests) == 2
    assert outputs.diagnostics()["receipts"] == 1
    assert env["lifecycle"].diagnostics()["outbox_states"]["consumed"] == 0


def test_forged_same_event_target_authority_cannot_suppress_exact_reproof(
    monkeypatch,
    tmp_path,
):
    env = _event_environment(monkeypatch, tmp_path / "event")
    first_executor = InlineExecutor()
    first, first_runner, artifacts, evidence, first_outputs = _consumer_stack(
        env,
        tmp_path / "first",
        executor=first_executor,
    )
    lease = _claim(env)
    first.process_leased(lease, env["state"], threading.Event())
    legitimate = first_outputs.load(lease.event_id)
    assert legitimate is not None

    forged_outputs = NoaaErrCompletionOutputStore(
        (tmp_path / "forged-output.sqlite3").resolve(),
        clock=env["clock"],
    )
    no_spawn = NeverExecutor()
    forged_runner = NoaaErrCompletionIsolatedRunner(
        raw_store=env["raw"],
        artifact_store=artifacts,
        evidence_store=evidence,
        output_store=forged_outputs,
        limits=first_runner.limits,
        executor=no_spawn,
    )
    forged = copy.deepcopy(legitimate.output)
    forged_region = forged["region_authority"]
    forged_region["target_ids"] = ["zone.forged"]
    region_body = dict(forged_region)
    region_body.pop("authority_sha256")
    forged_region["authority_sha256"] = subject._digest(region_body)
    forged["process_isolation"]["authority_sha256"] = forged_runner.authority_sha256
    forged_outputs.commit(forged)
    forged_consumer = NoaaErrCompletionConsumer(
        lifecycle_store=env["lifecycle"],
        desired_state_store=env["desired"],
        raw_store=env["raw"],
        artifact_store=artifacts,
        evidence_store=evidence,
        output_store=forged_outputs,
        process_runner=forged_runner,
        clock=env["clock"],
    )

    with pytest.raises(NoaaErrCompletionConflict, match="lease binding"):
        forged_consumer.process_leased(lease, env["state"], threading.Event())

    assert no_spawn.calls == 0
    assert env["lifecycle"].diagnostics()["outbox_states"]["consumed"] == 0


def test_process_and_parent_store_roles_must_be_disjoint(monkeypatch, tmp_path):
    env = _event_environment(monkeypatch, tmp_path / "event")
    _consumer, runner, artifacts, evidence, outputs = _consumer_stack(
        env,
        tmp_path / "consumer",
        executor=NeverExecutor(),
    )
    aliased = copy.deepcopy(runner._authority)
    aliased["stores"]["outputs"]["path"] = aliased["stores"]["canonical_evidence"]["path"]
    with pytest.raises(NoaaErrCompletionIntegrityError, match="database paths alias"):
        subject._load_process_authority(aliased, subject._digest(aliased))

    env["desired"].path = outputs.path
    with pytest.raises(NoaaErrCompletionIntegrityError, match="database store paths alias"):
        NoaaErrCompletionConsumer(
            lifecycle_store=env["lifecycle"],
            desired_state_store=env["desired"],
            raw_store=env["raw"],
            artifact_store=artifacts,
            evidence_store=evidence,
            output_store=outputs,
            process_runner=runner,
            clock=env["clock"],
        )


def test_forged_consumption_receipt_is_refused(monkeypatch, tmp_path):
    env = _event_environment(monkeypatch, tmp_path / "event")
    executor = InlineExecutor()
    consumer, _runner, _artifacts, _evidence, _outputs = _consumer_stack(
        env,
        tmp_path / "consumer",
        executor=executor,
    )

    def forged_ack(*_args, **_kwargs):
        binding = executor.requests[0]["binding"]
        return ConsumptionReceipt(
            receipt_sha256="0" * 64,
            event_id=binding["event_id"],
            event_sha256=binding["event_sha256"],
            producer_receipt_sha256=binding["producer_receipt_sha256"],
            consumer_id=CONSUMER_ID,
            stream_id=STREAM_ID,
            stream_sequence=binding["stream_sequence"],
            output_sha256="0" * 64,
            created_at=env["clock"](),
        )

    monkeypatch.setattr(env["lifecycle"], "acknowledge", forged_ack)

    with pytest.raises(
        NoaaErrCompletionIntegrityError,
        match="terminal receipt differs",
    ):
        consumer.run_once("forged-receipt", lease_seconds=60.0)

    assert env["lifecycle"].diagnostics()["outbox_states"]["consumed"] == 0


def test_cancellation_at_final_reproof_barrier_keeps_output_unacked(
    monkeypatch,
    tmp_path,
):
    env = _event_environment(monkeypatch, tmp_path / "event")
    executor = InlineExecutor()
    cancellation = threading.Event()

    def cancel_at_barrier(point):
        if point == "consumer.before_final_reproof":
            cancellation.set()

    consumer, _runner, _artifacts, _evidence, outputs = _consumer_stack(
        env,
        tmp_path / "consumer",
        executor=executor,
        consumer_fault=cancel_at_barrier,
    )

    with pytest.raises(NoaaErrCompletionCancelled, match="before final reproof"):
        consumer.run_once(
            "cancel-final",
            lease_seconds=60.0,
            cancel_event=cancellation,
        )

    assert len(executor.requests) == 1
    assert outputs.diagnostics()["receipts"] == 1
    assert env["lifecycle"].diagnostics()["outbox_states"]["consumed"] == 0


def test_preinflated_output_database_fails_at_hard_page_cap(tmp_path):
    limits = NoaaErrCompletionOutputLimits(max_database_bytes=1024 * 1024)
    path = (tmp_path / "bounded-output.sqlite3").resolve()
    NoaaErrCompletionOutputStore(path, limits=limits)
    with closing(sqlite3.connect(path)) as database:
        database.execute("PRAGMA max_page_count=8192")
        database.execute("CREATE TABLE padding(payload BLOB NOT NULL)")
        database.execute("INSERT INTO padding VALUES(zeroblob(2097152))")
        database.commit()
    assert path.stat().st_size > limits.max_database_bytes

    with pytest.raises(NoaaErrCompletionCapacityError, match="page cap"):
        NoaaErrCompletionOutputStore(path, limits=limits)


def test_consumer_remains_unwired_from_live_runtime_and_publication():
    entrypoints = (
        REPOSITORY / "storm_tracker_worker" / "service.py",
        REPOSITORY / "storm_tracker_worker" / "worker_first_composition.py",
        REPOSITORY / "storm_tracker_worker" / "worker_first_service_composition.py",
        REPOSITORY / "storm_tracker_worker" / "acquisition_activation.py",
        REPOSITORY / "storm_tracker_worker" / "operational_snapshot_runtime.py",
        REPOSITORY / "storm_tracker_worker" / "__init__.py",
        REPOSITORY / "custom_components" / "storm_tracker_v3" / "__init__.py",
    )
    assert all(
        "noaa_err_completion_consumer" not in path.read_text(encoding="utf-8")
        for path in entrypoints
        if path.exists()
    )
    source = Path(subject.__file__).read_text(encoding="utf-8")
    for forbidden in (
        "import paho",
        "publish_operational_snapshot",
        'meteorological_admission_granted": True',
        "capture_latest_noaa_err_netcdf4",
    ):
        assert forbidden not in source


def test_default_runner_is_spawn_only_with_explicit_cpu_rss_wall_output_caps(monkeypatch, tmp_path):
    env = _event_environment(monkeypatch, tmp_path / "event")
    _consumer, runner, _artifacts, _evidence, _outputs = _consumer_stack(
        env, tmp_path / "consumer", executor=None
    )
    assert isinstance(runner._executor, HardDecoderExecutor)
    assert runner._executor._context.get_start_method() == "spawn"
    limits = runner.limits.decoder_limits()
    assert limits.wall_seconds == TEST_LIMITS.wall_seconds
    assert limits.cpu_seconds == TEST_LIMITS.cpu_seconds
    assert limits.memory_bytes == TEST_LIMITS.rss_bytes
    assert limits.max_request_bytes == TEST_LIMITS.max_request_bytes
    assert limits.max_result_bytes == TEST_LIMITS.max_result_bytes
    assert hard_boundary_support().scope in {
        "decoder_job_object",
        "decoder_process_only_no_descendant_budget",
    }


@pytest.mark.skipif(
    not hard_boundary_support().supported,
    reason="hard kernel process boundary is unavailable",
)
def test_real_spawn_process_commits_before_lifecycle_ack(monkeypatch, tmp_path):
    env = _event_environment(monkeypatch, tmp_path / "event")
    limits = NoaaErrCompletionIsolationLimits(
        wall_seconds=30.0,
        cpu_seconds=25.0,
        rss_bytes=1536 * 1024 * 1024,
        max_request_bytes=1024 * 1024,
        max_result_bytes=16 * 1024,
    )
    consumer, _runner, _artifacts, _evidence, _outputs = _consumer_stack(
        env, tmp_path / "consumer", executor=None, limits=limits
    )
    result = consumer.run_once("real-spawn-owner", lease_seconds=90.0)
    assert result.disposition == "processed"
    assert result.output_receipt is not None
    assert result.consumption_receipt is not None
    assert result.consumption_receipt.output_sha256 == result.output_receipt.output_sha256


# This inventory is intentionally literal and test-owned.  The AST assertion
# below pins every production seam to exactly one call occurrence.
_NOAA_MIGRATION_OS_EXIT73_POINTS = (
    "migration.after_metadata",
    "migration.after_triggers",
    "migration.before_commit",
    "migration.after_commit",
)
_NOAA_RUNTIME_OS_EXIT73_EXPECTATIONS = {
    # output rows, terminal at crash, child calls at crash, recovery calls,
    # public recovery disposition
    "output.before_commit": (0, False, 1, 2, "processed"),
    "output.after_commit": (1, False, 1, 1, "recovered_output"),
    "consumer.after_output_commit": (1, False, 1, 1, "recovered_output"),
    "consumer.before_final_reproof": (1, False, 1, 1, "recovered_output"),
    "consumer.after_final_reproof_before_ack": (
        1,
        False,
        2,
        1,
        "recovered_output",
    ),
    "consumer.after_ack": (1, True, 2, 0, "idle"),
    "consumer.after_claim": (0, False, 0, 2, "processed"),
}
_NOAA_ALL_OS_EXIT73_POINTS = (
    *_NOAA_MIGRATION_OS_EXIT73_POINTS,
    *_NOAA_RUNTIME_OS_EXIT73_EXPECTATIONS,
)
_NOAA_RAW_MANIFEST_OS_EXIT73_CHILD = (
    REPOSITORY
    / "tests"
    / "helpers"
    / "noaa_raw_manifest_completion_os_exit73_child.py"
)


class _DirectNativePatch:
    """Minimal child-only replacement for pytest's monkeypatch fixture."""

    @staticmethod
    def setattr(target, name, value):
        setattr(target, name, value)


def _append_noaa_native_call(path: Path, purpose: str) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    try:
        os.write(descriptor, purpose.encode("ascii") + b"\n")
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _noaa_native_calls(path: Path) -> tuple[str, ...]:
    if not path.exists():
        return ()
    return tuple(line.decode("ascii") for line in path.read_bytes().splitlines())


class NoaaNativeCrashRecordingExecutor(InlineExecutor):
    def __init__(self, call_log: Path, *, output_fault=None) -> None:
        super().__init__()
        self.call_log = call_log
        self.output_fault = output_fault

    def execute(self, **kwargs):
        _append_noaa_native_call(
            self.call_log,
            str(kwargs["request"]["purpose"]),
        )
        if self.output_fault is None:
            return super().execute(**kwargs)
        original = NoaaErrCompletionOutputStore.__init__

        def initialize_with_fault(instance, *args, **init_kwargs):
            assert "fault_injector" not in init_kwargs
            init_kwargs["fault_injector"] = self.output_fault
            original(instance, *args, **init_kwargs)

        with patch.object(
            NoaaErrCompletionOutputStore,
            "__init__",
            initialize_with_fault,
        ):
            return super().execute(**kwargs)


def _build_noaa_native_crash_stack(root: Path, checkpoint: str, crash):
    env = _event_environment(_DirectNativePatch(), root)
    artifacts = NoaaErrDerivedArtifactStore((root / "derived").resolve())
    artifacts.initialize()
    evidence = CanonicalEvidenceStore(
        (root / "canonical.sqlite3").resolve(),
        raw_artifact_reader=env["raw"],
        clock=env["clock"],
    )
    outputs = NoaaErrCompletionOutputStore(
        (root / "outputs.sqlite3").resolve(),
        clock=env["clock"],
    )
    executor = NoaaNativeCrashRecordingExecutor(
        root / "native-child-calls.log",
        output_fault=(crash if checkpoint.startswith("output.") else None),
    )
    runner = NoaaErrCompletionIsolatedRunner(
        raw_store=env["raw"],
        artifact_store=artifacts,
        evidence_store=evidence,
        output_store=outputs,
        limits=TEST_LIMITS,
        executor=executor,
    )
    consumer = NoaaErrCompletionConsumer(
        lifecycle_store=env["lifecycle"],
        desired_state_store=env["desired"],
        raw_store=env["raw"],
        artifact_store=artifacts,
        evidence_store=evidence,
        output_store=outputs,
        process_runner=runner,
        clock=env["clock"],
        fault_injector=(crash if checkpoint.startswith("consumer.") else None),
    )
    return env, consumer


def run_noaa_completion_os_exit73_child(
    root: Path,
    checkpoint: str,
    crash,
) -> None:
    """Reach one allowlisted NOAA production seam in a fresh process."""

    if checkpoint not in set(_NOAA_ALL_OS_EXIT73_POINTS):
        raise RuntimeError("NOAA completion crash checkpoint is not allowlisted")
    if checkpoint in _NOAA_MIGRATION_OS_EXIT73_POINTS:
        path = (root / "outputs.sqlite3").resolve()
        NoaaErrCompletionOutputStore(path)
        _downgrade_output_store_to_v1(path)
        NoaaErrCompletionOutputStore(path, fault_injector=crash)
        return
    _env, consumer = _build_noaa_native_crash_stack(root, checkpoint, crash)
    consumer.run_once("noaa-native-crash", lease_seconds=60.0)


def _run_noaa_completion_os_exit73(root: Path, checkpoint: str):
    return subprocess.run(
        [
            sys.executable,
            str(_NOAA_RAW_MANIFEST_OS_EXIT73_CHILD),
            "noaa",
            str(root.resolve()),
            checkpoint,
        ],
        cwd=REPOSITORY,
        capture_output=True,
        check=False,
        timeout=180.0,
    )


def _noaa_sqlite_value(value):
    return ("bytes", value.hex()) if isinstance(value, bytes) else value


def _noaa_sqlite_rows(path: Path, table: str) -> tuple:
    quoted = table.replace('"', '""')
    with closing(sqlite3.connect(path)) as database:
        columns = tuple(
            str(row[1])
            for row in database.execute(f'PRAGMA table_info("{quoted}")')
        )
        rows = [
            tuple(_noaa_sqlite_value(value) for value in row)
            for row in database.execute(f'SELECT * FROM "{quoted}"')
        ]
    rows.sort(key=repr)
    return columns, tuple(rows)


def _noaa_sqlite_logical_sha256(path: Path) -> str:
    document = []
    with closing(sqlite3.connect(path)) as database:
        schema = [
            tuple(_noaa_sqlite_value(value) for value in row)
            for row in database.execute(
                "SELECT type,name,tbl_name,sql FROM sqlite_master "
                "WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name"
            )
        ]
        document.append(schema)
        tables = tuple(
            str(row[0])
            for row in database.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name NOT LIKE 'sqlite_%' ORDER BY name"
            )
        )
        for table in tables:
            document.append((table, _noaa_sqlite_rows(path, table)[1]))
    return hashlib.sha256(
        repr(document).encode("utf-8")
    ).hexdigest()


def _noaa_file_snapshot(root: Path) -> tuple[tuple[str, int, str], ...]:
    if not root.exists():
        return ()
    rows = []
    for path in sorted(root.rglob("*")):
        if not path.is_file() or path.is_symlink():
            continue
        if path.name.endswith((".sqlite3", ".sqlite3-journal", ".sqlite3-wal", ".sqlite3-shm")):
            continue
        payload = path.read_bytes()
        rows.append(
            (
                path.relative_to(root).as_posix(),
                len(payload),
                hashlib.sha256(payload).hexdigest(),
            )
        )
    return tuple(rows)


def _noaa_capture_authority_snapshot(raw: RawArtifactStore) -> tuple:
    with closing(sqlite3.connect(raw.catalog_path)) as database:
        capture_id = str(
            database.execute(
                "SELECT capture_id FROM raw_artifact_captures"
            ).fetchone()[0]
        )
    loaded = raw.load_capture_bundle_bytes(capture_id)
    assert loaded is not None
    capture, receipts, payloads = loaded
    return (
        capture,
        tuple(
            (
                role,
                receipts[role],
                len(payloads[role]),
                hashlib.sha256(payloads[role]).hexdigest(),
            )
            for role in sorted(receipts)
        ),
    )


def _noaa_migration_shape(path: Path) -> dict[str, object]:
    with closing(sqlite3.connect(path)) as database:
        columns = tuple(
            str(row[1])
            for row in database.execute("PRAGMA table_info(output_metadata)")
        )
        metadata = tuple(
            database.execute(
                "SELECT * FROM output_metadata WHERE singleton=1"
            ).fetchone()
        )
        triggers = tuple(
            sorted(
                str(row[0])
                for row in database.execute(
                    "SELECT name FROM sqlite_master WHERE type='trigger'"
                )
            )
        )
        quick_check = str(database.execute("PRAGMA quick_check").fetchone()[0])
        receipts = int(
            database.execute("SELECT count(*) FROM completion_outputs").fetchone()[0]
        )
    return {
        "columns": columns,
        "metadata": metadata,
        "quick_check": quick_check,
        "receipts": receipts,
        "triggers": triggers,
    }


def _assert_noaa_hot_journal(path: Path, *, expected: bool) -> None:
    journal = Path(str(path) + "-journal")
    if expected:
        assert journal.is_file() and journal.stat().st_size > 512
        assert any(journal.read_bytes()[512:])
    elif journal.exists():
        assert journal.is_file()
        # SQLite DELETE-mode can leave a recovered journal file on Windows;
        # a zeroed magic header proves it is no longer a hot journal.
        assert journal.read_bytes()[:8] == b"\0" * 8
    assert not Path(str(path) + "-wal").exists()
    assert not Path(str(path) + "-shm").exists()


def _reopen_noaa_native_crash_stack(root: Path):
    clock = MutableClock(NOW + 3.0)
    desired = desired_state.DesiredStateStore((root / "desired.sqlite3").resolve())
    raw = RawArtifactStore((root / "source-raw").resolve(), limits=_raw_limits())
    raw.initialize()
    lifecycle = AcquisitionEvidenceLifecycleStore(
        (root / "lifecycle").resolve(),
        clock=clock,
        monotonic=lambda: 1.0,
        token_factory=lambda: "f" * 64,
        directory_fsync=SYNC,
    )
    executor = NoaaNativeCrashRecordingExecutor(root / "native-child-calls.log")
    consumer, runner, artifacts, evidence, outputs = _consumer_stack(
        {
            "clock": clock,
            "desired": desired,
            "lifecycle": lifecycle,
            "raw": raw,
        },
        root,
        executor=executor,
    )
    return {
        "artifacts": artifacts,
        "consumer": consumer,
        "evidence": evidence,
        "executor": executor,
        "lifecycle": lifecycle,
        "outputs": outputs,
        "raw": raw,
        "runner": runner,
    }


def _noaa_runtime_rows(root: Path) -> dict[str, tuple]:
    lifecycle_path = root / "lifecycle" / "acquisition_evidence.sqlite3"
    output_path = root / "outputs.sqlite3"
    return {
        "consumption": _noaa_sqlite_rows(
            lifecycle_path, "consumption_receipts"
        ),
        "outbox": _noaa_sqlite_rows(lifecycle_path, "completion_outbox"),
        "outputs": _noaa_sqlite_rows(output_path, "completion_outputs"),
    }


def test_noaa_native_crash_inventory_pins_every_production_fault_occurrence():
    source_path = REPOSITORY / "storm_tracker_worker" / "noaa_err_completion_consumer.py"
    tree = ast.parse(source_path.read_text(encoding="utf-8"), filename=str(source_path))
    calls = []
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "_fault"
            and len(node.args) == 1
            and isinstance(node.args[0], ast.Constant)
            and isinstance(node.args[0].value, str)
        ):
            calls.append(node.args[0].value)
    expected = Counter({point: 1 for point in _NOAA_ALL_OS_EXIT73_POINTS})
    assert len(_NOAA_ALL_OS_EXIT73_POINTS) == 11
    assert len(set(_NOAA_ALL_OS_EXIT73_POINTS)) == 11
    assert Counter(calls) == expected


@pytest.mark.parametrize(
    "fault_point",
    _NOAA_MIGRATION_OS_EXIT73_POINTS,
    ids=lambda value: f"noaa-{value}",
)
def test_native_os_exit73_noaa_migration_is_exactly_old_or_new(
    tmp_path,
    fault_point,
):
    root = (tmp_path / fault_point.replace(".", "-")).resolve()
    root.mkdir()
    crashed = _run_noaa_completion_os_exit73(root, fault_point)
    assert crashed.returncode == 73
    assert crashed.stdout == b""
    assert crashed.stderr == b""
    path = (root / "outputs.sqlite3").resolve()
    rolled_back = fault_point != "migration.after_commit"
    _assert_noaa_hot_journal(path, expected=rolled_back)
    shape = _noaa_migration_shape(path)
    _assert_noaa_hot_journal(path, expected=False)
    assert shape["quick_check"] == "ok"
    assert shape["receipts"] == 0
    if rolled_back:
        assert shape["columns"] == (
            "singleton",
            "contract",
            "limits_sha256",
        )
        assert shape["metadata"][1] == subject.LEGACY_OUTPUT_STORE_CONTRACT
        assert shape["triggers"] == (
            "completion_outputs_no_delete",
            "completion_outputs_no_update",
        )
    else:
        assert shape["columns"] == (
            "singleton",
            "contract",
            "limits_sha256",
            "schema_version",
            "store_instance_sha256",
        )
        assert shape["metadata"][1] == subject.OUTPUT_STORE_CONTRACT
        assert shape["metadata"][3] == subject.OUTPUT_STORE_SCHEMA_VERSION
        assert subject._SHA.fullmatch(str(shape["metadata"][4])) is not None
        assert shape["triggers"] == tuple(
            sorted(NoaaErrCompletionOutputStore._trigger_definitions())
        )

    recovered = NoaaErrCompletionOutputStore(path)
    assert recovered.diagnostics() == {
        "contract": subject.OUTPUT_STORE_CONTRACT,
        "healthy": True,
        "receipt_bytes": 0,
        "receipts": 0,
    }
    recovered_shape = _noaa_migration_shape(path)
    recovered_hash = _noaa_sqlite_logical_sha256(path)
    replay = NoaaErrCompletionOutputStore(path)
    assert replay.identity_sha256 == recovered.identity_sha256
    assert _noaa_migration_shape(path) == recovered_shape
    assert _noaa_sqlite_logical_sha256(path) == recovered_hash
    _assert_noaa_hot_journal(path, expected=False)


@pytest.mark.parametrize(
    "fault_point",
    tuple(_NOAA_RUNTIME_OS_EXIT73_EXPECTATIONS),
    ids=lambda value: f"noaa-{value}",
)
def test_native_os_exit73_noaa_runtime_recovers_and_replays_exactly_once(
    tmp_path,
    fault_point,
):
    root = (tmp_path / fault_point.replace(".", "-")).resolve()
    root.mkdir()
    (
        expected_outputs,
        terminal_at_crash,
        calls_at_crash,
        recovery_calls,
        recovery_disposition,
    ) = _NOAA_RUNTIME_OS_EXIT73_EXPECTATIONS[fault_point]
    crashed = _run_noaa_completion_os_exit73(root, fault_point)
    assert crashed.returncode == 73
    assert crashed.stdout == b""
    assert crashed.stderr == b""

    output_path = (root / "outputs.sqlite3").resolve()
    call_log = root / "native-child-calls.log"
    _assert_noaa_hot_journal(
        output_path,
        expected=fault_point == "output.before_commit",
    )
    calls_before = _noaa_native_calls(call_log)
    assert len(calls_before) == calls_at_crash
    rows_before = _noaa_runtime_rows(root)
    _assert_noaa_hot_journal(output_path, expected=False)
    assert len(rows_before["outbox"][1]) == 1
    outbox = dict(zip(rows_before["outbox"][0], rows_before["outbox"][1][0]))
    assert outbox["state"] == ("consumed" if terminal_at_crash else "leased")
    assert outbox["attempt_count"] == 1
    assert len(rows_before["outputs"][1]) == expected_outputs
    assert len(rows_before["consumption"][1]) == int(terminal_at_crash)

    raw = RawArtifactStore((root / "source-raw").resolve(), limits=_raw_limits())
    raw.initialize()
    raw_stats = raw.stats()
    raw_files = _noaa_file_snapshot(root / "source-raw")
    raw_authority = _noaa_capture_authority_snapshot(raw)
    assert raw_stats["capture_count"] == 1
    assert raw_stats["receipt_count"] == 2
    assert raw.integrity_check()["healthy"] is True

    reopened = _reopen_noaa_native_crash_stack(root)
    reopened["consumer"]._clock.value = NOW + 65.0
    output_before = reopened["outputs"].load(str(outbox["event_id"]))
    assert (output_before is not None) is bool(expected_outputs)
    recovered = reopened["consumer"].run_once(
        "noaa-native-recovery",
        lease_seconds=60.0,
    )
    assert recovered.disposition == recovery_disposition
    assert len(_noaa_native_calls(call_log)) == calls_at_crash + recovery_calls

    output_after = reopened["outputs"].load(str(outbox["event_id"]))
    assert output_after is not None
    if output_before is not None:
        assert output_after == output_before
    terminal = reopened["lifecycle"].load_terminal_consumption(
        STREAM_ID,
        int(outbox["stream_sequence"]),
    )
    assert terminal is not None
    assert terminal.consumption_receipt.event_id == outbox["event_id"]
    assert terminal.consumption_receipt.output_sha256 == output_after.output_sha256
    assert subject._SHA.fullmatch(output_after.output_sha256) is not None
    assert subject._SHA.fullmatch(output_after.receipt_sha256) is not None
    assert subject._SHA.fullmatch(output_after.process_receipt_sha256) is not None
    assert reopened["outputs"].diagnostics()["receipts"] == 1
    assert reopened["evidence"].stats()["decoder_runs"] == 1
    assert reopened["evidence"].audit() == {
        "healthy": True,
        "checked_runs": 1,
        "issues": [],
    }
    assert reopened["raw"].stats() == raw_stats
    assert _noaa_capture_authority_snapshot(reopened["raw"]) == raw_authority
    assert _noaa_file_snapshot(root / "source-raw") == raw_files
    assert _noaa_capture_authority_snapshot(reopened["raw"]) == raw_authority
    assert reopened["raw"].integrity_check()["healthy"] is True

    output_hash = _noaa_sqlite_logical_sha256(output_path)
    evidence_hash = _noaa_sqlite_logical_sha256(root / "canonical.sqlite3")
    derived_files = _noaa_file_snapshot(root / "derived")
    final_rows = _noaa_runtime_rows(root)
    final_diagnostics = reopened["lifecycle"].diagnostics()
    assert final_diagnostics["outbox_states"]["consumed"] == 1
    assert sum(final_diagnostics["outbox_states"].values()) == 1
    serialized = json.dumps(output_after.output, sort_keys=True).casefold()
    assert str(root).casefold() not in serialized
    for forbidden in (
        "access_token",
        "api_key",
        "authorization",
        "lease_token",
        "password",
        "refresh_token",
    ):
        assert forbidden not in serialized

    replay = reopened["consumer"].run_once(
        "noaa-native-replay",
        lease_seconds=60.0,
    )
    assert replay.disposition == "idle"
    assert len(_noaa_native_calls(call_log)) == calls_at_crash + recovery_calls
    assert reopened["outputs"].load(str(outbox["event_id"])) == output_after
    assert _noaa_runtime_rows(root) == final_rows
    assert _noaa_sqlite_logical_sha256(output_path) == output_hash
    assert _noaa_sqlite_logical_sha256(root / "canonical.sqlite3") == evidence_hash
    assert _noaa_file_snapshot(root / "derived") == derived_files
    assert _noaa_file_snapshot(root / "source-raw") == raw_files
    assert reopened["evidence"].audit()["healthy"] is True
    assert reopened["raw"].integrity_check()["healthy"] is True
