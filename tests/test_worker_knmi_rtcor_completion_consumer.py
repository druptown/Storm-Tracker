from __future__ import annotations

import ast
from collections import Counter
import copy
from contextlib import closing
from dataclasses import replace
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import threading
import time

import pytest

import storm_tracker_worker.knmi_rtcor_completion_consumer as knmi_completion
from storm_tracker_worker.canonical_evidence_store import CanonicalEvidenceStore
from storm_tracker_worker.acquisition_evidence_lifecycle import (
    AcquisitionEvidenceConflict,
    AcquisitionEvidenceLifecycleStore,
)
from storm_tracker_worker.hard_decoder_isolation import (
    DecoderProcessResult,
    hard_boundary_support,
)
from storm_tracker_worker.knmi_rtcor_completion_consumer import (
    CONSUMER_ID,
    PROCESS_EFFECT_CONTRACT,
    PROCESS_TARGET,
    LEGACY_OUTPUT_STORE_CONTRACT,
    LEGACY_SCHEMA_VERSION,
    OUTPUT_STORE_CONTRACT,
    SCHEMA_VERSION,
    STREAM_ID,
    WORKLOAD_ID,
    KnmiRtcorCompletionCancelled,
    KnmiRtcorCompletionCapacityError,
    KnmiRtcorCompletionConflict,
    KnmiRtcorCompletionConsumer,
    KnmiRtcorCompletionDeadlineError,
    KnmiRtcorCompletionIntegrityError,
    KnmiRtcorCompletionIsolatedRunner,
    KnmiRtcorCompletionOutputLimits,
    KnmiRtcorCompletionOutputStore,
    KnmiRtcorCompletionProcessLimits,
    _digest,
    _knmi_rtcor_completion_child,
    _load_process_authority,
    validate_knmi_rtcor_completion_lease,
)
from storm_tracker_worker.knmi_rtcor_quantitative import (
    KnmiRtcorDerivedArtifactStore,
    decode_knmi_rtcor_capture,
)
from storm_tracker_worker.raw_artifacts import (
    NATIVE_CAPTURE_STAGE,
    NATIVE_REPRESENTATION,
    RawArtifactStore,
)
from storm_tracker_worker.desired_state import DesiredStateStore

from test_worker_terminal_raw_completion_router import (
    NOW,
    SYNC,
    _environment,
    _raw_limits,
    _router,
)
from test_worker_knmi_rtcor_quantitative import (
    FILENAME as VALID_FILENAME,
    PRODUCT_END as VALID_PRODUCT_END,
    _hdf_bytes,
)


REPOSITORY = Path(__file__).resolve().parents[1]


def _downgrade_output_store_to_v1(path: Path) -> None:
    """Turn a valid current store into the exact prior v1 schema."""

    with closing(sqlite3.connect(path)) as database:
        database.execute("BEGIN IMMEDIATE")
        database.execute("DROP TRIGGER output_metadata_no_update")
        database.execute("DROP TRIGGER output_metadata_no_delete")
        database.execute(
            "UPDATE output_metadata SET contract=?,schema_version=? "
            "WHERE singleton=1",
            (LEGACY_OUTPUT_STORE_CONTRACT, LEGACY_SCHEMA_VERSION),
        )
        database.execute(
            "ALTER TABLE output_metadata DROP COLUMN store_instance_sha256"
        )
        database.commit()


class InlineExecutor:
    def __init__(self) -> None:
        self.calls = 0

    def execute(self, *, target, request, effect_contract, cancel_event):
        self.calls += 1
        assert target == PROCESS_TARGET
        assert effect_contract == PROCESS_EFFECT_CONTRACT
        if cancel_event.is_set():
            return DecoderProcessResult("cancelled", detail_code="cancelled_before_spawn")
        started = time.monotonic()
        result = _knmi_rtcor_completion_child(request)
        return DecoderProcessResult(
            "succeeded",
            result=result,
            child_exit_code=0,
            elapsed_seconds=max(0.0, time.monotonic() - started),
        )


class TerminalExecutor:
    def __init__(self, outcome: str) -> None:
        self.outcome = outcome
        self.calls = 0

    def execute(self, *, target, request, effect_contract, cancel_event):
        self.calls += 1
        assert target == PROCESS_TARGET
        assert effect_contract == PROCESS_EFFECT_CONTRACT
        return DecoderProcessResult(self.outcome)


class ContainedInlineExecutor(InlineExecutor):
    def execute(self, **kwargs):
        try:
            return super().execute(**kwargs)
        except Exception:
            return DecoderProcessResult("decoder_failed")


class FaultOnce:
    def __init__(self, point: str) -> None:
        self.point = point
        self.hit = False

    def __call__(self, point: str) -> None:
        if point == self.point and not self.hit:
            self.hit = True
            raise RuntimeError(f"fault:{point}")


class MutableClock:
    def __init__(self, value: float) -> None:
        self.value = value

    def __call__(self) -> float:
        return self.value


def _setup(
    tmp_path,
    *,
    output_limits=None,
    process_limits=None,
    fault=None,
    output_fault=None,
):
    env = _environment(tmp_path, WORKLOAD_ID)
    published = _router(env).publish_next()
    assert published.status == "published"
    artifacts = KnmiRtcorDerivedArtifactStore((tmp_path / "derived").resolve())
    evidence = CanonicalEvidenceStore(
        (tmp_path / "canonical.sqlite3").resolve(),
        raw_artifact_reader=env["raw"],
        clock=lambda: NOW + 1.0,
    )
    outputs = KnmiRtcorCompletionOutputStore(
        (tmp_path / "knmi-completion.sqlite3").resolve(),
        limits=output_limits or KnmiRtcorCompletionOutputLimits(),
        clock=lambda: NOW + 4.0,
        fault_injector=output_fault,
    )
    runner = KnmiRtcorCompletionIsolatedRunner(
        raw_store=env["raw"],
        artifact_store=artifacts,
        evidence_store=evidence,
        output_store=outputs,
        limits=process_limits or KnmiRtcorCompletionProcessLimits(
            wall_seconds=5.0,
            cpu_seconds=4.0,
            rss_bytes=1536 * 1024 * 1024,
        ),
    )
    clock = MutableClock(NOW + 3.0)
    consumer = KnmiRtcorCompletionConsumer(
        lifecycle_store=env["lifecycle"],
        desired_state_store=env["desired"],
        runner=runner,
        clock=clock,
        fault_injector=fault,
    )
    return env, runner, consumer, clock


def _claim(env, *, now=NOW + 3.0, lease_seconds=120.0):
    lease = env["lifecycle"].claim_next(
        CONSUMER_ID,
        STREAM_ID,
        "knmi-consumer-test",
        now=now,
        lease_seconds=lease_seconds,
    )
    assert lease is not None
    return lease


def _terminal(env, lease):
    try:
        return env["lifecycle"].load_terminal_consumption(
            lease.stream_id, lease.stream_sequence
        )
    except AcquisitionEvidenceConflict:
        return None


def _activate_next_desired(env):
    active = env["desired"].load_active()
    assert active is not None
    successor = active.as_dict()
    successor["generation"] = active.generation + 1
    successor["issued_at"] = active.issued_at + 1.0
    successor["previous_state_sha256"] = active.state_sha256
    env["desired"].apply(successor, received_at=NOW - 58.0)
    changed = env["desired"].load_active()
    assert changed is not None
    assert changed.state_sha256 != active.state_sha256
    return changed


def test_router_reference_is_decoded_in_isolated_protocol_and_acked_only_after_output(tmp_path):
    env, runner, consumer, _clock = _setup(tmp_path)
    inline = InlineExecutor()
    runner._executor = inline

    result = consumer.run_once(owner_id="knmi-consumer-a", lease_seconds=120.0)

    assert result.status == "acknowledged"
    assert inline.calls == 1
    output = runner.output_store.load(result.event_id)
    assert output is not None
    assert output.output_sha256 == result.output_sha256
    assert output.output["mode"] == "shadow_inert"
    assert output.output["fact_revision"] is None
    assert output.output["claims"] == {
        "decoded": False,
        "fact_staged": False,
        "meteorological_admission": False,
        "operational_snapshot_published": False,
    }
    assert output.output["decoder"]["outcome"] != "decoded_complete"
    terminal = env["lifecycle"].load_terminal_consumption(STREAM_ID, 1)
    assert terminal is not None
    assert terminal.consumption_receipt.output_sha256 == output.output_sha256


def test_valid_router_capture_reaches_exact_quantitative_decoder_but_stays_factless(
    tmp_path, monkeypatch,
):
    payload = _hdf_bytes()
    content_sha256 = hashlib.sha256(payload).hexdigest()
    original_store = RawArtifactStore.store
    original_capture = RawArtifactStore.commit_capture

    def store(self, *args, **kwargs):
        if kwargs.get("product") == "knmi_nl_rdr_data_rtcor_5m_hdf5":
            kwargs["payload"] = payload
            kwargs["content_type"] = "application/x-hdf5"
            kwargs["upstream_id"] = (
                f"nl_rdr_data_rtcor_5m/1.0|filename={VALID_FILENAME}"
            )
            kwargs["provenance"] = {
                "capture_stage": NATIVE_CAPTURE_STAGE,
                "collector": "knmi_open_data_native_file",
                "collector_version": "worker-knmi-rtcor-5m-exact-raw-v1",
                "dataset_license": "CC-BY-4.0",
                "dataset_name": "nl_rdr_data_rtcor_5m",
                "dataset_version": "1.0",
                "declared_accumulation_minutes": 5.0,
                "declared_quantity": "precipitation_accumulation",
                "declared_semantic_class": "quantitative_precipitation_accumulation",
                "declared_unit": "mm",
                "download_size_bytes": len(payload),
                "listing_size_bytes": len(payload),
                "quantitative_semantics_verified_after_decode": False,
                "representation": NATIVE_REPRESENTATION,
                "selection_limit": 1,
                "selection_order": "created_descending",
                "structure_inspector_version": "knmi-rtcor-live-golden-identity-v2",
                "upstream_filename": VALID_FILENAME,
            }
        return original_store(self, *args, **kwargs)

    def commit_capture(self, *args, **kwargs):
        if kwargs.get("product") == "knmi_nl_rdr_data_rtcor_5m_capture":
            timestamp = VALID_PRODUCT_END.timestamp()
            kwargs["product_timestamp"] = timestamp
            kwargs["upstream_capture_id"] = (
                "knmi_nl_rdr_data_rtcor_5m_capture"
                f"|filename={VALID_FILENAME}"
                f"|product_timestamp={timestamp:.6f}"
                f"|content_sha256={content_sha256}"
            )
        return original_capture(self, *args, **kwargs)

    monkeypatch.setattr(RawArtifactStore, "store", store)
    monkeypatch.setattr(RawArtifactStore, "commit_capture", commit_capture)
    env = _environment(tmp_path, WORKLOAD_ID)
    assert _router(env).publish_next().status == "published"
    artifacts = KnmiRtcorDerivedArtifactStore((tmp_path / "derived-valid").resolve())
    evidence = CanonicalEvidenceStore(
        (tmp_path / "canonical-valid.sqlite3").resolve(),
        raw_artifact_reader=env["raw"],
        clock=lambda: NOW + 1.0,
    )
    outputs = KnmiRtcorCompletionOutputStore(
        (tmp_path / "outputs-valid.sqlite3").resolve(), clock=lambda: NOW + 4.0
    )
    runner = KnmiRtcorCompletionIsolatedRunner(
        raw_store=env["raw"],
        artifact_store=artifacts,
        evidence_store=evidence,
        output_store=outputs,
        limits=KnmiRtcorCompletionProcessLimits(
            wall_seconds=15.0,
            cpu_seconds=12.0,
            rss_bytes=1536 * 1024 * 1024,
        ),
    )
    runner._executor = InlineExecutor()
    consumer = KnmiRtcorCompletionConsumer(
        lifecycle_store=env["lifecycle"],
        desired_state_store=env["desired"],
        runner=runner,
        clock=lambda: NOW + 3.0,
    )

    lease = _claim(env, lease_seconds=30.0)
    desired = env["desired"].load_active()
    assert desired is not None
    binding = validate_knmi_rtcor_completion_lease(
        lease, desired_state=desired, raw_store=env["raw"]
    )
    result = consumer.process_leased(lease)
    assert result.status == "acknowledged"
    output = outputs.load(result.event_id)
    assert output is not None
    assert output.output["decoder"]["outcome"] == "decoded_complete"
    assert output.artifact_sha256 is not None
    assert output.output["claims"] == {
        "decoded": True,
        "fact_staged": False,
        "meteorological_admission": False,
        "operational_snapshot_published": False,
    }
    assert output.output["fact_revision"] is None

    object_path = artifacts.objects_path / f"{output.artifact_sha256}.krtq"
    artifact_bytes = object_path.read_bytes()
    object_path.write_bytes(bytes([artifact_bytes[0] ^ 1]) + artifact_bytes[1:])
    with pytest.raises(
        KnmiRtcorCompletionIntegrityError,
        match="derived decoder artifact failed reproof",
    ):
        runner._load_exact(lease, binding)


def test_crash_after_output_restarts_without_second_decode_and_then_acks(tmp_path):
    fault = FaultOnce("consumer.after_output_commit")
    env, runner, consumer, clock = _setup(tmp_path, fault=fault)
    inline = InlineExecutor()
    runner._executor = inline
    with pytest.raises(RuntimeError, match="after_output_commit"):
        consumer.run_once(owner_id="knmi-crash-a", lease_seconds=10.0)
    assert inline.calls == 1
    durable = runner.output_store.diagnostics()
    assert durable["receipts"] == 1

    clock.value = NOW + 14.0
    restarted = KnmiRtcorCompletionConsumer(
        lifecycle_store=env["lifecycle"],
        desired_state_store=env["desired"],
        runner=runner,
        clock=clock,
    )
    result = restarted.run_once(owner_id="knmi-crash-b", lease_seconds=10.0)
    assert result.status == "acknowledged"
    assert inline.calls == 1
    assert runner.output_store.diagnostics()["receipts"] == 1


def test_output_store_v1_migrates_once_and_preserves_exact_receipt(tmp_path):
    env, runner, _consumer, _clock = _setup(tmp_path)
    runner._executor = InlineExecutor()
    lease = _claim(env)
    desired = env["desired"].load_active()
    assert desired is not None
    process = runner.execute(
        lease=lease,
        desired_state=desired,
        cancel_event=threading.Event(),
    )
    assert process.output_receipt is not None
    receipt = process.output_receipt
    path = runner.output_store.path
    _downgrade_output_store_to_v1(path)

    migrated = KnmiRtcorCompletionOutputStore(
        path, clock=lambda: NOW + 4.0
    )
    assert migrated.load(lease.event_id) == receipt
    assert len(migrated.store_instance_sha256) == 64
    with closing(sqlite3.connect(path)) as database:
        metadata = database.execute(
            "SELECT contract,schema_version,store_instance_sha256 "
            "FROM output_metadata WHERE singleton=1"
        ).fetchone()
        triggers = {
            str(row[0]) for row in database.execute(
                "SELECT name FROM sqlite_master WHERE type='trigger'"
            )
        }
    assert metadata == (
        OUTPUT_STORE_CONTRACT,
        SCHEMA_VERSION,
        migrated.store_instance_sha256,
    )
    assert triggers == set(migrated._trigger_definitions())


@pytest.mark.parametrize(
    "fault_point",
    [
        "migration.after_metadata",
        "migration.after_triggers",
        "migration.before_commit",
    ],
)
def test_output_store_v1_migration_faults_roll_back_exactly(
    fault_point, tmp_path
):
    path = (tmp_path / f"rollback-{fault_point}.sqlite3").resolve()
    KnmiRtcorCompletionOutputStore(path)
    _downgrade_output_store_to_v1(path)
    with pytest.raises(RuntimeError, match=fault_point):
        KnmiRtcorCompletionOutputStore(
            path, fault_injector=FaultOnce(fault_point)
        )
    with closing(sqlite3.connect(path)) as database:
        metadata = database.execute(
            "SELECT contract,schema_version FROM output_metadata "
            "WHERE singleton=1"
        ).fetchone()
        columns = {
            str(row[1]) for row in database.execute(
                "PRAGMA table_info(output_metadata)"
            )
        }
        triggers = {
            str(row[0]) for row in database.execute(
                "SELECT name FROM sqlite_master WHERE type='trigger'"
            )
        }
    assert metadata == (
        LEGACY_OUTPUT_STORE_CONTRACT,
        LEGACY_SCHEMA_VERSION,
    )
    assert "store_instance_sha256" not in columns
    assert triggers == {
        "completion_outputs_no_update",
        "completion_outputs_no_delete",
    }
    assert len(KnmiRtcorCompletionOutputStore(path).store_instance_sha256) == 64


def test_output_store_v1_migration_after_commit_reopens_current(tmp_path):
    path = (tmp_path / "committed-migration.sqlite3").resolve()
    KnmiRtcorCompletionOutputStore(path)
    _downgrade_output_store_to_v1(path)
    with pytest.raises(RuntimeError, match="migration.after_commit"):
        KnmiRtcorCompletionOutputStore(
            path, fault_injector=FaultOnce("migration.after_commit")
        )
    committed = KnmiRtcorCompletionOutputStore(path)
    assert len(committed.store_instance_sha256) == 64


def test_output_metadata_is_exactly_immutable(tmp_path):
    path = (tmp_path / "immutable-output.sqlite3").resolve()
    store = KnmiRtcorCompletionOutputStore(path)
    with closing(sqlite3.connect(path)) as database:
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            database.execute(
                "UPDATE output_metadata SET store_instance_sha256=? "
                "WHERE singleton=1",
                ("f" * 64,),
            )
        database.rollback()
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            database.execute(
                "DELETE FROM output_metadata WHERE singleton=1"
            )
        database.rollback()
        database.execute("DROP TRIGGER output_metadata_no_update")
        database.execute(
            "CREATE TRIGGER output_metadata_no_update BEFORE UPDATE ON "
            "output_metadata WHEN 1 BEGIN SELECT RAISE(ABORT,"
            "'output metadata is immutable'); END"
        )
        database.commit()
    with pytest.raises(
        KnmiRtcorCompletionIntegrityError,
        match="immutability triggers differ",
    ):
        store.diagnostics()


def test_output_store_live_same_inode_instance_reproof_fails_closed(tmp_path):
    path = (tmp_path / "identity.sqlite3").resolve()
    store = KnmiRtcorCompletionOutputStore(path)
    original_instance = store.store_instance_sha256
    original_identity = store.identity_sha256
    assert KnmiRtcorCompletionOutputStore(path).identity_sha256 == original_identity
    original_inode = path.stat().st_ino

    replacement_path = (tmp_path / "replacement.sqlite3").resolve()
    replacement = KnmiRtcorCompletionOutputStore(replacement_path)
    replacement_bytes = replacement_path.read_bytes()
    assert replacement.store_instance_sha256 != original_instance
    with path.open("r+b") as target:
        target.seek(0)
        target.write(replacement_bytes)
        target.truncate()
        target.flush()
        os.fsync(target.fileno())
    assert path.stat().st_ino == original_inode
    with pytest.raises(
        KnmiRtcorCompletionIntegrityError,
        match="instance authority changed",
    ):
        store.diagnostics()
    adopted = KnmiRtcorCompletionOutputStore(path)
    assert adopted.store_instance_sha256 == replacement.store_instance_sha256
    assert adopted.identity_sha256 != original_identity


def test_completion_reproof_lease_fences_ddl_and_recovers_exact_ack(tmp_path):
    env, runner, consumer, _clock = _setup(tmp_path)
    inline = InlineExecutor()
    runner._executor = inline
    lease = _claim(env)
    attack_entered = False

    def attack(point):
        nonlocal attack_entered
        if point != "consumer.before_lifecycle_ack":
            return
        attack_entered = True
        with closing(sqlite3.connect(
            runner.output_store.path,
            timeout=0.05,
            isolation_level=None,
        )) as database:
            database.execute("PRAGMA busy_timeout=50")
            database.execute("DROP TRIGGER completion_outputs_no_delete")
            database.execute(
                "DELETE FROM completion_outputs WHERE event_id=?",
                (lease.event_id,),
            )
            database.execute(
                "CREATE TRIGGER completion_outputs_no_delete BEFORE DELETE "
                "ON completion_outputs BEGIN SELECT RAISE(ABORT,'immutable'); "
                "END"
            )

    consumer._fault_injector = attack
    with pytest.raises(sqlite3.OperationalError, match="locked"):
        consumer.process_leased(lease)
    assert attack_entered
    assert env["lifecycle"].load_leased(
        lease.event_id, lease.lease_token
    ) == lease
    assert _terminal(env, lease) is None
    assert runner.output_store.load(lease.event_id) is not None
    with closing(sqlite3.connect(runner.output_store.path)) as database:
        triggers = {
            str(row[0]) for row in database.execute(
                "SELECT name FROM sqlite_master WHERE type='trigger'"
            )
        }
    assert triggers == set(runner.output_store._trigger_definitions())

    consumer._fault_injector = None
    recovered = consumer.process_leased(lease)
    assert recovered.status == "acknowledged"
    assert inline.calls == 1
    assert _terminal(env, lease) is not None


@pytest.mark.skipif(
    os.name == "nt",
    reason="Windows denies replacement of the open SQLite database",
)
def test_completion_reproof_posix_path_replacement_never_acks(tmp_path):
    env, runner, consumer, _clock = _setup(tmp_path)
    inline = InlineExecutor()
    runner._executor = inline
    lease = _claim(env)
    replacement_path = (tmp_path / "visible-replacement.sqlite3").resolve()
    replacement = KnmiRtcorCompletionOutputStore(replacement_path)

    def replace_output(point):
        if point == "consumer.before_lifecycle_ack":
            os.replace(replacement_path, runner.output_store.path)

    consumer._fault_injector = replace_output
    with pytest.raises(
        KnmiRtcorCompletionIntegrityError,
        match="instance changed",
    ):
        consumer.process_leased(lease)
    assert inline.calls == 1
    assert _terminal(env, lease) is None
    visible = KnmiRtcorCompletionOutputStore(runner.output_store.path)
    assert visible.store_instance_sha256 == replacement.store_instance_sha256
    assert visible.load(lease.event_id) is None


def test_deadline_and_cancellation_never_ack(tmp_path):
    env, runner, consumer, _clock = _setup(tmp_path)
    runner._executor = TerminalExecutor("deadline_exceeded")
    lease = _claim(env)
    result = consumer.process_leased(lease)
    assert result.status == "deadline"
    assert _terminal(env, lease) is None

    event = threading.Event()
    event.set()
    with pytest.raises(KnmiRtcorCompletionCancelled):
        consumer.process_leased(lease, cancel_event=event)
    assert _terminal(env, lease) is None

    with pytest.raises(KnmiRtcorCompletionDeadlineError):
        consumer.run_once(owner_id="too-short", lease_seconds=1.0)


def test_output_capacity_fails_closed_without_ack(tmp_path):
    env, runner, consumer, _clock = _setup(
        tmp_path,
        output_limits=KnmiRtcorCompletionOutputLimits(
            max_receipts=1,
            max_receipt_bytes=1024,
            max_total_output_bytes=1024,
            max_database_bytes=16 * 1024 * 1024,
        ),
    )
    runner._executor = ContainedInlineExecutor()
    lease = _claim(env)
    result = consumer.process_leased(lease)
    assert result.status == "failed"
    assert _terminal(env, lease) is None


def test_tampered_reference_raw_and_output_are_rejected(tmp_path):
    env, runner, _consumer, _clock = _setup(tmp_path)
    lease = _claim(env)
    state = env["desired"].load_active()
    assert state is not None
    binding = validate_knmi_rtcor_completion_lease(
        lease, desired_state=state, raw_store=env["raw"]
    )
    assert binding.capture_id

    tampered_payload = json.loads(lease.payload)
    tampered_payload["capture_reference"]["manifest_sha256"] = "0" * 64
    with pytest.raises(KnmiRtcorCompletionIntegrityError):
        validate_knmi_rtcor_completion_lease(
            replace(
                lease,
                payload=json.dumps(
                    tampered_payload, sort_keys=True, separators=(",", ":")
                ).encode("ascii"),
            ),
            desired_state=state,
            raw_store=env["raw"],
        )

    runner._executor = InlineExecutor()
    process = runner.execute(lease=lease, desired_state=state, cancel_event=threading.Event())
    assert process.output_receipt is not None
    with closing(sqlite3.connect(runner.output_store.path)) as database:
        database.execute("DROP TRIGGER completion_outputs_no_update")
        database.execute(
            "UPDATE completion_outputs SET output_json=? WHERE event_id=?",
            (b"{}", lease.event_id),
        )
        database.commit()
    with pytest.raises(KnmiRtcorCompletionIntegrityError):
        runner.output_store.load(lease.event_id)


def test_raw_byte_tamper_and_proof_authority_tamper_fail_closed(tmp_path):
    env, _runner, _consumer, _clock = _setup(tmp_path)
    lease = _claim(env)
    state = env["desired"].load_active()
    assert state is not None

    changed_proof = json.loads(json.dumps(lease.proof))
    changed_proof["workload"]["job_sha256"] = "0" * 64
    with pytest.raises(KnmiRtcorCompletionIntegrityError):
        validate_knmi_rtcor_completion_lease(
            replace(lease, proof=changed_proof),
            desired_state=state,
            raw_store=env["raw"],
        )

    reference = json.loads(lease.payload)
    receipt_id = reference["capture_reference"]["members"][0]["receipt_id"]
    receipt, payload = env["raw"].read_receipt_bytes(receipt_id)
    replacement = bytes([payload[0] ^ 1]) + payload[1:]
    Path(receipt.artifact_path).write_bytes(replacement)
    with pytest.raises(KnmiRtcorCompletionIntegrityError):
        validate_knmi_rtcor_completion_lease(
            lease, desired_state=state, raw_store=env["raw"]
        )


def test_duplicate_process_and_cancel_after_output_are_idempotent_and_unacked(tmp_path):
    env, runner, consumer, _clock = _setup(tmp_path)
    inline = InlineExecutor()
    runner._executor = inline
    lease = _claim(env)
    state = env["desired"].load_active()
    assert state is not None
    first = runner.execute(
        lease=lease, desired_state=state, cancel_event=threading.Event()
    )
    second = runner.execute(
        lease=lease, desired_state=state, cancel_event=threading.Event()
    )
    assert first.output_receipt == second.output_receipt
    assert inline.calls == 1
    assert runner.output_store.diagnostics()["receipts"] == 1
    with closing(sqlite3.connect(runner.evidence_store.path)) as database:
        assert database.execute("SELECT count(*) FROM decoder_runs").fetchone()[0] == 1

    cancelled = threading.Event()

    def cancel_after_output(point):
        if point == "consumer.after_output_commit":
            cancelled.set()

    consumer._fault_injector = cancel_after_output
    with pytest.raises(KnmiRtcorCompletionCancelled):
        consumer.process_leased(lease, cancel_event=cancelled)
    assert _terminal(env, lease) is None
    assert runner.output_store.diagnostics()["receipts"] == 1


def test_obsolete_desired_event_is_rejected_once_and_cannot_poison_stream_head(tmp_path):
    env, runner, consumer, _clock = _setup(tmp_path)
    terminal_executor = TerminalExecutor("decoder_failed")
    runner._executor = terminal_executor
    lease = _claim(env)
    _activate_next_desired(env)

    result = consumer.process_leased(lease)

    assert result.status == "rejected"
    assert result.reason_code == "desired_state_superseded_before_processing"
    assert result.consumption_receipt_sha256 is not None
    assert terminal_executor.calls == 0
    assert runner.output_store.diagnostics()["receipts"] == 0
    assert _terminal(env, lease) is None
    assert env["lifecycle"].diagnostics()["rejection_receipts"] == 1
    assert env["lifecycle"].claim_next(
        CONSUMER_ID,
        STREAM_ID,
        "obsolete-reclaim",
        now=NOW + 4.0,
        lease_seconds=120.0,
    ) is None


def test_desired_change_at_final_ack_boundary_rejects_but_keeps_durable_output(
    tmp_path,
):
    env, runner, consumer, _clock = _setup(tmp_path)
    inline = InlineExecutor()
    runner._executor = inline

    def change_desired(point):
        if point == "consumer.before_lifecycle_ack":
            _activate_next_desired(env)

    consumer._fault_injector = change_desired
    result = consumer.run_once(owner_id="desired-final-race", lease_seconds=120.0)

    assert result.status == "rejected"
    assert result.reason_code == "desired_state_changed_before_ack"
    assert result.output_sha256 is None
    assert result.consumption_receipt_sha256 is not None
    assert inline.calls == 1
    durable = runner.output_store.load(result.event_id)
    assert durable is not None
    assert env["lifecycle"].diagnostics()["rejection_receipts"] == 1
    assert env["lifecycle"].diagnostics()["outbox_states"]["consumed"] == 1


def test_lease_reclaim_at_final_ack_boundary_cannot_ack_old_lease(tmp_path):
    env, runner, consumer, _clock = _setup(tmp_path)
    runner._executor = InlineExecutor()
    lease = _claim(env, lease_seconds=120.0)
    replacement = None

    def reclaim_lease(point):
        nonlocal replacement
        if point == "consumer.before_lifecycle_ack":
            replacement = env["lifecycle"].claim_next(
                CONSUMER_ID,
                STREAM_ID,
                "replacement-owner",
                now=lease.lease_expires_at + 1.0,
                lease_seconds=120.0,
            )

    consumer._fault_injector = reclaim_lease
    with pytest.raises(KnmiRtcorCompletionConflict, match="ack boundary"):
        consumer.process_leased(lease)
    assert replacement is not None
    assert replacement.attempt_count == lease.attempt_count + 1
    assert _terminal(env, lease) is None


@pytest.mark.parametrize(
    "outcome",
    ["cpu_limit_exceeded", "memory_limit_exceeded", "result_too_large"],
)
def test_hard_cpu_rss_and_child_result_caps_never_ack(outcome, tmp_path):
    env, runner, consumer, _clock = _setup(tmp_path)
    runner._executor = TerminalExecutor(outcome)
    lease = _claim(env)

    result = consumer.process_leased(lease)

    assert result.status == "resource_limited"
    assert result.reason_code == "hard_cpu_rss_or_output_limit"
    assert runner.output_store.diagnostics()["receipts"] == 0
    assert _terminal(env, lease) is None


def test_forged_same_job_output_and_private_artifact_path_cannot_suppress_decode(
    tmp_path,
):
    env, runner, _consumer, _clock = _setup(tmp_path)
    lease = _claim(env)
    desired = env["desired"].load_active()
    assert desired is not None
    runner._executor = InlineExecutor()
    legitimate = runner.execute(
        lease=lease, desired_state=desired, cancel_event=threading.Event()
    )
    assert legitimate.output_receipt is not None
    assert legitimate.output_receipt.output["decoder"]["outcome"] != "decoded_complete"

    forged_store = KnmiRtcorCompletionOutputStore(
        (tmp_path / "forged-output.sqlite3").resolve(), clock=lambda: NOW + 4.0
    )
    forged_runner = KnmiRtcorCompletionIsolatedRunner(
        raw_store=runner.raw_store,
        artifact_store=runner.artifact_store,
        evidence_store=runner.evidence_store,
        output_store=forged_store,
        limits=runner.limits,
    )
    no_spawn = TerminalExecutor("decoder_failed")
    forged_runner._executor = no_spawn
    forged = copy.deepcopy(legitimate.output_receipt.output)
    fake_sha = "a" * 64
    forged["claims"]["decoded"] = True
    forged["decoder"]["artifact"] = {
        "handle": f"/private/knmi/{fake_sha}",
        "sha256": fake_sha,
        "size_bytes": 1,
    }
    forged["decoder"]["outcome"] = "decoded_complete"
    forged["decoder"]["reason_code"] = "decoded_complete"
    forged["disposition"] = "shadow_candidate"
    forged["isolation"]["authority_sha256"] = forged_runner.authority_sha256
    with pytest.raises(KnmiRtcorCompletionIntegrityError):
        forged_store.commit(forged)

    forged["decoder"]["artifact"]["handle"] = (
        f"artifact:knmi-rtcor-quantitative:{fake_sha}"
    )
    forged_store.commit(forged)
    forged_consumer = KnmiRtcorCompletionConsumer(
        lifecycle_store=env["lifecycle"],
        desired_state_store=env["desired"],
        runner=forged_runner,
        clock=lambda: NOW + 3.0,
    )
    with pytest.raises(
        KnmiRtcorCompletionIntegrityError,
        match="canonical decoder evidence",
    ):
        forged_consumer.process_leased(lease)
    assert no_spawn.calls == 0
    assert _terminal(env, lease) is None


def test_child_authority_rejects_rehashed_store_role_alias(tmp_path):
    _env, runner, _consumer, _clock = _setup(tmp_path)
    authority = copy.deepcopy(runner._authority)
    authority["stores"]["outputs"]["path"] = authority["stores"][
        "canonical_evidence"
    ]["path"]

    with pytest.raises(
        KnmiRtcorCompletionIntegrityError,
        match="database paths alias",
    ):
        _load_process_authority(authority, _digest(authority))


def test_preinflated_output_database_is_rejected_above_hard_page_cap(tmp_path):
    limits = KnmiRtcorCompletionOutputLimits(max_database_bytes=1024 * 1024)
    path = (tmp_path / "bounded-output.sqlite3").resolve()
    KnmiRtcorCompletionOutputStore(path, limits=limits)
    sha = "0" * 64
    with closing(sqlite3.connect(path)) as database:
        database.execute("PRAGMA max_page_count=8192")
        database.execute(
            "INSERT INTO completion_outputs VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                f"completion-{sha}", sha, "1" * 64, "2" * 64, "3" * 64,
                CONSUMER_ID, STREAM_ID, f"capture-{sha}", "4" * 64,
                f"run-{sha}", "5" * 64, None, "6" * 64, "7" * 64,
                "8" * 64, "9" * 64, "a" * 64, "b" * 64,
                b"x" * (2 * 1024 * 1024), 2 * 1024 * 1024, NOW,
            ),
        )
        database.commit()
    assert path.stat().st_size > limits.max_database_bytes

    with pytest.raises(
        KnmiRtcorCompletionCapacityError,
        match="hard page cap",
    ):
        KnmiRtcorCompletionOutputStore(path, limits=limits)


def test_diagnostics_and_child_result_are_path_payload_secret_free(tmp_path):
    env, runner, consumer, _clock = _setup(tmp_path)
    runner._executor = InlineExecutor()
    result = consumer.run_once(owner_id="knmi-privacy", lease_seconds=120.0)
    output = runner.output_store.load(result.event_id)
    assert output is not None
    serialized = json.dumps({
        "diagnostics": consumer.diagnostics(),
        "output": output.output,
    }, sort_keys=True).lower()
    for forbidden in (
        str(tmp_path).lower(), "artifact_path", "upstream_id", '"provenance":',
        "api_key", "password", "private_", "http://", "https://",
    ):
        assert forbidden not in serialized


@pytest.mark.skipif(
    not hard_boundary_support().supported,
    reason="platform has no enforceable one-process hard boundary",
)
def test_real_spawn_boundary_returns_only_after_durable_output(tmp_path):
    _env, runner, consumer, _clock = _setup(
        tmp_path,
        process_limits=KnmiRtcorCompletionProcessLimits(
            wall_seconds=20.0,
            cpu_seconds=15.0,
            rss_bytes=1536 * 1024 * 1024,
        ),
    )
    result = consumer.run_once(owner_id="knmi-real-spawn", lease_seconds=30.0)
    assert result.status == "acknowledged"
    assert runner.output_store.load(result.event_id) is not None


def test_consumer_remains_absent_from_live_service_ha_and_mqtt_wiring():
    forbidden = tuple(
        path
        for path in (REPOSITORY / "storm_tracker_worker").rglob("*.py")
        if path.name
        not in {
            "knmi_rtcor_completion_consumer.py",
            "completion_output_archive_adapters.py",
            "completion_route_adapters.py",
        }
    )
    forbidden += tuple((REPOSITORY / "custom_components").rglob("*.py"))
    needle = "knmi_rtcor_completion_consumer"
    assert all(needle not in path.read_text(encoding="utf-8") for path in forbidden)


# Literal, test-owned inventory: it intentionally does not import a production
# checkpoint constant.  The AST guard below pins each production call to one
# occurrence and forces an explicit matrix update for any future seam.
_KNMI_MIGRATION_OS_EXIT73_POINTS = (
    "migration.after_metadata",
    "migration.after_triggers",
    "migration.before_commit",
    "migration.after_commit",
)
_KNMI_RUNTIME_OS_EXIT73_EXPECTATIONS = {
    "output.before_commit": (0, 1),
    "output.after_commit": (1, 0),
    "consumer.after_output_commit": (1, 0),
    "consumer.before_lifecycle_ack": (1, 0),
    "consumer.after_lifecycle_ack": (1, 0),
}
_KNMI_ALL_OS_EXIT73_POINTS = frozenset(
    _KNMI_MIGRATION_OS_EXIT73_POINTS
) | frozenset(_KNMI_RUNTIME_OS_EXIT73_EXPECTATIONS)
_COMPLETION_CONSUMER_OS_EXIT73_CHILD = (
    REPOSITORY / "tests" / "helpers" / "completion_consumer_os_exit73_child.py"
)


def _append_knmi_native_decode_call(path: Path) -> None:
    descriptor = os.open(
        path,
        os.O_WRONLY | os.O_CREAT | os.O_APPEND,
        0o600,
    )
    try:
        os.write(descriptor, b"decode\n")
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _knmi_native_decode_call_count(path: Path) -> int:
    return 0 if not path.exists() else len(path.read_bytes().splitlines())


class KnmiNativeCrashRecordingExecutor(InlineExecutor):
    def __init__(self, call_log: Path) -> None:
        super().__init__()
        self.call_log = call_log

    def execute(self, **kwargs):
        _append_knmi_native_decode_call(self.call_log)
        return super().execute(**kwargs)


def _knmi_run_output_store_crash_child(env, runner, call_log: Path) -> None:
    lease = _claim(env, now=NOW + 3.0, lease_seconds=10.0)
    desired = env["desired"].load_active()
    assert desired is not None
    binding = validate_knmi_rtcor_completion_lease(
        lease,
        desired_state=desired,
        raw_store=runner.raw_store,
    )
    _append_knmi_native_decode_call(call_log)
    decoded = decode_knmi_rtcor_capture(
        capture_id=binding.capture_id,
        decoder_job_id=binding.decoder_job_id,
        attempt_id=binding.decoder_attempt_id,
        raw_store=runner.raw_store,
        artifact_store=runner.artifact_store,
        evidence_store=runner.evidence_store,
        evaluated_at=binding.evaluated_at,
        monotonic_deadline=time.monotonic() + 4.0,
        monotonic_clock=time.monotonic,
    )
    document = knmi_completion._output_document(
        lease=lease,
        binding=binding,
        result=decoded,
        isolation_authority_sha256=runner.authority_sha256,
        isolation_limits_sha256=runner.limits.fingerprint,
    )
    runner.output_store.commit(document)


def run_knmi_completion_consumer_os_exit73_child(
    scenario_root: Path,
    checkpoint: str,
    crash,
) -> None:
    """Build a closed local fixture and reach the allowlisted production seam."""

    if checkpoint not in _KNMI_ALL_OS_EXIT73_POINTS:
        raise RuntimeError("KNMI completion crash checkpoint is not allowlisted")
    if checkpoint in _KNMI_MIGRATION_OS_EXIT73_POINTS:
        _env, runner, consumer, _clock = _setup(scenario_root)
        runner._executor = InlineExecutor()
        seeded = consumer.run_once(
            owner_id="knmi-migration-seed",
            lease_seconds=10.0,
        )
        assert seeded.status == "acknowledged"
        path = runner.output_store.path
        _downgrade_output_store_to_v1(path)
        KnmiRtcorCompletionOutputStore(path, fault_injector=crash)
        return

    _env, runner, consumer, _clock = _setup(
        scenario_root,
        fault=crash,
        output_fault=crash,
    )
    call_log = scenario_root / "native-crash-decode-calls.log"
    runner._executor = KnmiNativeCrashRecordingExecutor(call_log)
    if checkpoint in {"output.before_commit", "output.after_commit"}:
        _knmi_run_output_store_crash_child(_env, runner, call_log)
        return
    consumer.run_once(owner_id="knmi-native-crash", lease_seconds=10.0)


def _run_knmi_completion_os_exit73(root: Path, checkpoint: str):
    environment = os.environ.copy()
    path_entries = [str(REPOSITORY), str(REPOSITORY / "tests")]
    if environment.get("PYTHONPATH"):
        path_entries.append(environment["PYTHONPATH"])
    environment["PYTHONPATH"] = os.pathsep.join(path_entries)
    return subprocess.run(
        [
            sys.executable,
            str(_COMPLETION_CONSUMER_OS_EXIT73_CHILD),
            "knmi",
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


def _knmi_sqlite_value(value):
    if isinstance(value, bytes):
        return ("bytes", value.hex())
    return value


def _knmi_sqlite_table_rows(path: Path, table: str) -> tuple:
    with closing(sqlite3.connect(path)) as database:
        columns = tuple(
            str(row[1])
            for row in database.execute(f"PRAGMA table_info({table})")
        )
        rows = tuple(
            tuple(_knmi_sqlite_value(value) for value in row)
            for row in database.execute(f"SELECT * FROM {table} ORDER BY 1")
        )
    return columns, rows


def _knmi_migration_shape(path: Path) -> dict:
    with closing(sqlite3.connect(path)) as database:
        metadata = tuple(database.execute(
            "SELECT contract,schema_version FROM output_metadata WHERE singleton=1"
        ).fetchone())
        columns = tuple(
            str(row[1])
            for row in database.execute("PRAGMA table_info(output_metadata)")
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


def _assert_knmi_output_hot_journal(path: Path, *, expected: bool) -> None:
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


def _knmi_runtime_rows(root: Path) -> dict:
    lifecycle_path = root / "lifecycle" / "acquisition_evidence.sqlite3"
    output_path = root / "knmi-completion.sqlite3"
    return {
        "consumption": _knmi_sqlite_table_rows(
            lifecycle_path, "consumption_receipts"
        ),
        "outbox": _knmi_sqlite_table_rows(
            lifecycle_path, "completion_outbox"
        ),
        "outputs": _knmi_sqlite_table_rows(
            output_path, "completion_outputs"
        ),
    }


def _knmi_non_sqlite_tree(root: Path) -> tuple[tuple[str, int, str], ...]:
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


def _reopen_knmi_native_crash_environment(root: Path):
    clock = MutableClock(NOW + 14.0)
    raw = RawArtifactStore(
        (root / "source-raw").resolve(),
        limits=_raw_limits(),
    )
    raw.initialize()
    lifecycle = AcquisitionEvidenceLifecycleStore(
        (root / "lifecycle").resolve(),
        clock=clock,
        token_factory=lambda: "b" * 64,
        directory_fsync=SYNC,
    )
    desired = DesiredStateStore((root / "desired.sqlite3").resolve())
    artifacts = KnmiRtcorDerivedArtifactStore((root / "derived").resolve())
    evidence = CanonicalEvidenceStore(
        (root / "canonical.sqlite3").resolve(),
        raw_artifact_reader=raw,
        clock=clock,
    )
    outputs = KnmiRtcorCompletionOutputStore(
        (root / "knmi-completion.sqlite3").resolve(),
        clock=clock,
    )
    runner = KnmiRtcorCompletionIsolatedRunner(
        raw_store=raw,
        artifact_store=artifacts,
        evidence_store=evidence,
        output_store=outputs,
        limits=KnmiRtcorCompletionProcessLimits(
            wall_seconds=5.0,
            cpu_seconds=4.0,
            rss_bytes=1536 * 1024 * 1024,
        ),
    )
    executor = KnmiNativeCrashRecordingExecutor(
        root / "native-crash-decode-calls.log"
    )
    runner._executor = executor
    consumer = KnmiRtcorCompletionConsumer(
        lifecycle_store=lifecycle,
        desired_state_store=desired,
        runner=runner,
        clock=clock,
    )
    return {
        "consumer": consumer,
        "evidence": evidence,
        "executor": executor,
        "lifecycle": lifecycle,
        "outputs": outputs,
        "raw": raw,
    }


def test_knmi_completion_production_fault_inventory_is_literal_and_complete():
    source = (
        REPOSITORY / "storm_tracker_worker" / "knmi_rtcor_completion_consumer.py"
    ).read_text(encoding="utf-8")
    tree = ast.parse(source)
    calls = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or len(node.args) != 1:
            continue
        if not (
            isinstance(node.func, ast.Attribute)
            and node.func.attr == "_fault"
            and isinstance(node.args[0], ast.Constant)
            and isinstance(node.args[0].value, str)
        ):
            continue
        calls.append(node.args[0].value)
    assert Counter(calls) == Counter(
        {checkpoint: 1 for checkpoint in _KNMI_ALL_OS_EXIT73_POINTS}
    )


@pytest.mark.parametrize(
    "fault_point",
    _KNMI_MIGRATION_OS_EXIT73_POINTS,
    ids=lambda value: f"knmi-{value}",
)
def test_native_os_exit73_knmi_output_migration_is_exactly_old_or_new(
    tmp_path,
    fault_point,
):
    crashed = _run_knmi_completion_os_exit73(tmp_path, fault_point)
    assert crashed.returncode == 73, (crashed.stdout, crashed.stderr)
    assert crashed.stdout == ""
    assert crashed.stderr == ""
    path = (tmp_path / "knmi-completion.sqlite3").resolve()
    _assert_knmi_output_hot_journal(
        path,
        expected=fault_point != "migration.after_commit",
    )
    output_before = _knmi_sqlite_table_rows(path, "completion_outputs")
    assert len(output_before[1]) == 1
    shape = _knmi_migration_shape(path)
    assert shape["quick_check"] == "ok"
    if fault_point == "migration.after_commit":
        assert shape["metadata"] == (OUTPUT_STORE_CONTRACT, SCHEMA_VERSION)
        assert "store_instance_sha256" in shape["columns"]
        assert shape["triggers"] == (
            "completion_outputs_no_delete",
            "completion_outputs_no_update",
            "output_metadata_no_delete",
            "output_metadata_no_update",
        )
    else:
        assert shape["metadata"] == (
            LEGACY_OUTPUT_STORE_CONTRACT,
            LEGACY_SCHEMA_VERSION,
        )
        assert "store_instance_sha256" not in shape["columns"]
        assert shape["triggers"] == (
            "completion_outputs_no_delete",
            "completion_outputs_no_update",
        )

    recovered = KnmiRtcorCompletionOutputStore(path)
    _assert_knmi_output_hot_journal(path, expected=False)
    identity = recovered.identity_sha256
    assert recovered.diagnostics() == {
        "contract": OUTPUT_STORE_CONTRACT,
        "healthy": True,
        "output_bytes": output_before[1][0][19],
        "receipts": 1,
        "runtime_wiring": "inert",
    }
    output_after = _knmi_sqlite_table_rows(path, "completion_outputs")
    assert output_after == output_before
    current_shape = _knmi_migration_shape(path)
    assert current_shape["metadata"] == (OUTPUT_STORE_CONTRACT, SCHEMA_VERSION)
    assert current_shape["quick_check"] == "ok"
    tree_after_recovery = _knmi_non_sqlite_tree(tmp_path)

    replay = KnmiRtcorCompletionOutputStore(path)
    assert replay.identity_sha256 == identity
    assert replay.diagnostics()["receipts"] == 1
    assert _knmi_sqlite_table_rows(path, "completion_outputs") == output_after
    assert _knmi_migration_shape(path) == current_shape
    assert _knmi_non_sqlite_tree(tmp_path) == tree_after_recovery


@pytest.mark.parametrize(
    "fault_point",
    tuple(_KNMI_RUNTIME_OS_EXIT73_EXPECTATIONS),
    ids=lambda value: f"knmi-{value}",
)
def test_native_os_exit73_knmi_consumer_reopens_recovers_and_replays_once(
    tmp_path,
    fault_point,
):
    expected_outputs_before, expected_executor_calls = (
        _KNMI_RUNTIME_OS_EXIT73_EXPECTATIONS[fault_point]
    )
    crashed = _run_knmi_completion_os_exit73(tmp_path, fault_point)
    assert crashed.returncode == 73, (crashed.stdout, crashed.stderr)
    assert crashed.stdout == ""
    assert crashed.stderr == ""

    output_path = (tmp_path / "knmi-completion.sqlite3").resolve()
    call_log = tmp_path / "native-crash-decode-calls.log"
    _assert_knmi_output_hot_journal(
        output_path,
        expected=fault_point == "output.before_commit",
    )
    assert _knmi_native_decode_call_count(call_log) == 1
    rows_before = _knmi_runtime_rows(tmp_path)
    assert len(rows_before["outbox"][1]) == 1
    outbox_before = dict(zip(
        rows_before["outbox"][0], rows_before["outbox"][1][0]
    ))
    terminal_crash = fault_point == "consumer.after_lifecycle_ack"
    assert outbox_before["state"] == ("consumed" if terminal_crash else "leased")
    assert outbox_before["attempt_count"] == 1
    assert len(rows_before["outputs"][1]) == expected_outputs_before
    assert len(rows_before["consumption"][1]) == (1 if terminal_crash else 0)
    raw_before = _knmi_non_sqlite_tree(tmp_path / "source-raw")

    reopened = _reopen_knmi_native_crash_environment(tmp_path)
    _assert_knmi_output_hot_journal(output_path, expected=False)
    recovered = reopened["consumer"].run_once(
        owner_id="knmi-native-recovery",
        lease_seconds=10.0,
    )
    assert recovered.status == ("idle" if terminal_crash else "acknowledged")
    assert reopened["executor"].calls == expected_executor_calls
    assert _knmi_native_decode_call_count(call_log) == (
        1 + expected_executor_calls
    )

    rows_recovered = _knmi_runtime_rows(tmp_path)
    outbox_recovered = dict(zip(
        rows_recovered["outbox"][0], rows_recovered["outbox"][1][0]
    ))
    assert outbox_recovered["state"] == "consumed"
    assert outbox_recovered["attempt_count"] == (1 if terminal_crash else 2)
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
    assert _knmi_non_sqlite_tree(tmp_path / "source-raw") == raw_before

    diagnostics = reopened["lifecycle"].diagnostics()
    assert diagnostics["outbox_states"] == {
        "consumed": 1,
        "leased": 0,
        "ready": 0,
        "superseded": 0,
    }
    assert reopened["outputs"].diagnostics()["receipts"] == 1
    assert reopened["raw"].integrity_check()["healthy"] is True
    assert reopened["evidence"].audit()["healthy"] is True
    stable_files = _knmi_non_sqlite_tree(tmp_path)

    replay = reopened["consumer"].run_once(
        owner_id="knmi-native-replay",
        lease_seconds=10.0,
    )
    assert replay.status == "idle"
    assert reopened["executor"].calls == expected_executor_calls
    assert _knmi_native_decode_call_count(call_log) == (
        1 + expected_executor_calls
    )
    assert _knmi_runtime_rows(tmp_path) == rows_recovered
    assert _knmi_non_sqlite_tree(tmp_path) == stable_files
    output_json = output_event["output_json"]
    assert output_json[0] == "bytes"
    serialized_output = bytes.fromhex(output_json[1]).decode("utf-8")
    assert str(tmp_path) not in serialized_output
    assert "lease_token" not in serialized_output
