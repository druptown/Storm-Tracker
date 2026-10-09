from __future__ import annotations

import ast
from collections import Counter
import copy
from contextlib import closing
import csv
from dataclasses import replace
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import threading
import time
from unittest.mock import patch

import pytest

from storm_tracker_worker.acquisition_evidence_lifecycle import (
    AcquisitionEvidenceConflict,
)
from storm_tracker_worker.canonical_ground_evidence_store import (
    CanonicalGroundEvidenceStore,
    GroundEvidenceLimits,
)
from storm_tracker_worker.hard_decoder_isolation import (
    DecoderProcessResult,
    hard_boundary_support,
)
from storm_tracker_worker import metar_native as native
from storm_tracker_worker.metar_ground_completion_consumer import (
    CONSUMER_ID,
    PROCESS_EFFECT_CONTRACT,
    PROCESS_TARGET,
    LEGACY_OUTPUT_STORE_CONTRACT,
    LEGACY_SCHEMA_VERSION,
    OUTPUT_STORE_CONTRACT,
    SCHEMA_VERSION,
    STREAM_ID,
    WORKLOAD_ID,
    MetarGroundCompletionCancelled,
    MetarGroundCompletionCapacityError,
    MetarGroundCompletionConflict,
    MetarGroundCompletionConsumer,
    MetarGroundCompletionDeadlineError,
    MetarGroundCompletionIntegrityError,
    MetarGroundCompletionIsolatedRunner,
    MetarGroundCompletionOutputLimits,
    MetarGroundCompletionOutputStore,
    MetarGroundCompletionProcessLimits,
    _digest,
    _load_process_authority,
    _metar_ground_completion_child,
    validate_metar_ground_completion_lease,
)
from storm_tracker_worker.raw_artifacts import RawArtifactStore

from test_worker_terminal_raw_completion_router import (
    NOW,
    _environment,
    _reopen_environment,
    _router,
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


def _row(**overrides: str) -> list[str]:
    values = [""] * 44
    defaults = {
        0: "METAR EBAW 021200Z AUTO 00000KT 9999 -RA SCT020 "
           "18/16 Q1012 RMK AO2 P0001",
        1: "EBAW",
        2: "2026-08-02T12:00:00Z",
        3: "51.1894",
        4: "4.4603",
        14: "TRUE",
        15: "TRUE",
        21: "-RA",
        36: "0.01",
        42: "METAR",
        43: "12.30",
    }
    names = {
        name: index for index, name in enumerate(native.METAR_EXPECTED_HEADER)
    }
    for index, value in defaults.items():
        values[index] = value
    for name, value in overrides.items():
        values[names[name]] = value
    return values


def _gzip(rows: list[list[str]] | None = None) -> bytes:
    stream = io.StringIO(newline="")
    writer = csv.writer(stream)
    writer.writerow(native.METAR_EXPECTED_HEADER)
    writer.writerows(rows or [_row()])
    return gzip.compress(stream.getvalue().encode("utf-8"), mtime=0)


class InlineExecutor:
    def __init__(self) -> None:
        self.calls = 0

    def execute(self, *, target, request, effect_contract, cancel_event):
        self.calls += 1
        assert target == PROCESS_TARGET
        assert effect_contract == PROCESS_EFFECT_CONTRACT
        if cancel_event.is_set():
            return DecoderProcessResult(
                "cancelled", detail_code="cancelled_before_spawn"
            )
        started = time.monotonic()
        result = _metar_ground_completion_child(request)
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


def _exact_environment(tmp_path, *, payload: bytes | None = None):
    body = payload or _gzip()
    digest = hashlib.sha256(body).hexdigest()
    original_store = RawArtifactStore.store
    original_capture = RawArtifactStore.commit_capture

    def store(self, *args, **kwargs):
        if kwargs.get("product") == native.METAR_GZIP_PRODUCT:
            kwargs.update(
                payload=body,
                content_type="application/gzip",
                received_at=time.time() - 10.0,
                upstream_id=(
                    "aviationweather/metars.cache.csv.gz"
                    f"|content_sha256={digest}"
                ),
                provenance=native._native_provenance(),
            )
        return original_store(self, *args, **kwargs)

    def commit_capture(self, *args, **kwargs):
        if kwargs.get("product") == native.METAR_CAPTURE_PRODUCT:
            kwargs.update(
                product_timestamp=None,
                upstream_capture_id=(
                    f"{native.METAR_CAPTURE_PRODUCT}"
                    f"|content_sha256={digest}"
                ),
            )
        return original_capture(self, *args, **kwargs)

    with (
        patch.object(RawArtifactStore, "store", store),
        patch.object(RawArtifactStore, "commit_capture", commit_capture),
    ):
        env = _environment(tmp_path, WORKLOAD_ID)
        published = _router(env).publish_next()
    assert published.status == "published"
    return env


def _setup(
    tmp_path,
    *,
    payload: bytes | None = None,
    output_limits: MetarGroundCompletionOutputLimits | None = None,
    process_limits: MetarGroundCompletionProcessLimits | None = None,
    ground_limits: GroundEvidenceLimits | None = None,
    fault=None,
):
    env = _exact_environment(tmp_path, payload=payload)
    evidence = CanonicalGroundEvidenceStore(
        (tmp_path / "canonical-ground.sqlite3").resolve(),
        raw_artifact_reader=env["raw"],
        limits=ground_limits or GroundEvidenceLimits(
            max_catalog_bytes=32 * 1024 * 1024,
            max_observations=4096,
            max_member_links=4096,
            max_qc_rows=65536,
            max_query_rows=1000,
        ),
    )
    outputs = MetarGroundCompletionOutputStore(
        (tmp_path / "metar-completion.sqlite3").resolve(),
        limits=output_limits or MetarGroundCompletionOutputLimits(),
        clock=lambda: NOW + 4.0,
    )
    runner = MetarGroundCompletionIsolatedRunner(
        raw_store=env["raw"],
        evidence_store=evidence,
        output_store=outputs,
        limits=process_limits or MetarGroundCompletionProcessLimits(
            wall_seconds=8.0,
            cpu_seconds=6.0,
            rss_bytes=768 * 1024 * 1024,
            factset_reproof_seconds=1.0,
        ),
    )
    clock = MutableClock(NOW + 3.0)
    consumer = MetarGroundCompletionConsumer(
        lifecycle_store=env["lifecycle"],
        desired_state_store=env["desired"],
        runner=runner,
        clock=clock,
        fault_injector=fault,
    )
    return env, runner, consumer, clock


def _claim(env, *, now=NOW + 3.0, lease_seconds=30.0):
    lease = env["lifecycle"].claim_next(
        CONSUMER_ID,
        STREAM_ID,
        "metar-consumer-test",
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


def test_router_decode_commits_full_shadow_factset_before_exact_ack(tmp_path):
    env, runner, consumer, _clock = _setup(tmp_path)
    inline = InlineExecutor()
    runner._executor = inline

    result = consumer.run_once(
        owner_id="metar-consumer-a", lease_seconds=30.0
    )

    assert result.status == "acknowledged"
    assert inline.calls == 1
    output = runner.output_store.load(result.event_id)
    assert output is not None
    assert output.output_sha256 == result.output_sha256
    assert output.output["mode"] == "shadow_inert"
    assert output.output["fact_revision"] is None
    assert output.output["disposition"] == "shadow_evidence_committed"
    assert output.output["claims"] == {
        "canonical_ground_evidence_committed": True,
        "ground_confirmation": False,
        "meteorological_admission": False,
        "operational_fact_published": False,
        "operational_snapshot_published": False,
    }
    factset = output.output["decoder"]["factset"]
    assert factset["fact_count"] == output.fact_count
    assert output.fact_count >= 2
    assert runner.evidence_store.stats()["ground_facts"] == output.fact_count
    terminal = env["lifecycle"].load_terminal_consumption(STREAM_ID, 1)
    assert terminal is not None
    assert terminal.consumption_receipt.output_sha256 == output.output_sha256


def test_validation_binds_exact_router_job_desired_and_raw_capture(tmp_path):
    env, _runner, _consumer, _clock = _setup(tmp_path)
    lease = _claim(env)
    desired = env["desired"].load_active()
    assert desired is not None
    binding = validate_metar_ground_completion_lease(
        lease, desired_state=desired, raw_store=env["raw"]
    )
    assert binding.acquisition_job_id == env["job"].job_id
    assert binding.capture_id == env["capture"].capture_id
    assert binding.capture_manifest_sha256 == env["capture"].manifest_sha256

    changed = copy.deepcopy(lease.proof)
    changed["workload"]["job_sha256"] = "0" * 64
    with pytest.raises(MetarGroundCompletionIntegrityError):
        validate_metar_ground_completion_lease(
            replace(lease, proof=changed),
            desired_state=desired,
            raw_store=env["raw"],
        )


def test_crash_after_output_restarts_without_second_decode(tmp_path):
    fault = FaultOnce("consumer.after_output_commit")
    env, runner, consumer, clock = _setup(tmp_path, fault=fault)
    inline = InlineExecutor()
    runner._executor = inline

    with pytest.raises(RuntimeError, match="after_output_commit"):
        consumer.run_once(owner_id="metar-crash-a", lease_seconds=30.0)
    assert inline.calls == 1
    assert runner.output_store.diagnostics()["receipts"] == 1
    facts = runner.evidence_store.stats()["ground_facts"]

    clock.value = NOW + 34.0
    restarted = MetarGroundCompletionConsumer(
        lifecycle_store=env["lifecycle"],
        desired_state_store=env["desired"],
        runner=runner,
        clock=clock,
    )
    result = restarted.run_once(
        owner_id="metar-crash-b", lease_seconds=30.0
    )
    assert result.status == "acknowledged"
    assert inline.calls == 1
    assert runner.output_store.diagnostics()["receipts"] == 1
    assert runner.evidence_store.stats()["ground_facts"] == facts


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

    migrated = MetarGroundCompletionOutputStore(
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
    MetarGroundCompletionOutputStore(path)
    _downgrade_output_store_to_v1(path)
    with pytest.raises(RuntimeError, match=fault_point):
        MetarGroundCompletionOutputStore(
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
    assert len(MetarGroundCompletionOutputStore(path).store_instance_sha256) == 64


def test_output_store_v1_migration_after_commit_reopens_current(tmp_path):
    path = (tmp_path / "committed-migration.sqlite3").resolve()
    MetarGroundCompletionOutputStore(path)
    _downgrade_output_store_to_v1(path)
    with pytest.raises(RuntimeError, match="migration.after_commit"):
        MetarGroundCompletionOutputStore(
            path, fault_injector=FaultOnce("migration.after_commit")
        )
    assert len(
        MetarGroundCompletionOutputStore(path).store_instance_sha256
    ) == 64


def test_output_metadata_is_exactly_immutable(tmp_path):
    path = (tmp_path / "immutable-output.sqlite3").resolve()
    store = MetarGroundCompletionOutputStore(path)
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
        MetarGroundCompletionIntegrityError,
        match="immutability triggers differ",
    ):
        store.diagnostics()


def test_output_store_live_same_inode_instance_reproof_fails_closed(tmp_path):
    path = (tmp_path / "identity.sqlite3").resolve()
    store = MetarGroundCompletionOutputStore(path)
    original_instance = store.store_instance_sha256
    original_identity = store.identity_sha256
    assert MetarGroundCompletionOutputStore(path).identity_sha256 == original_identity
    original_inode = path.stat().st_ino

    replacement_path = (tmp_path / "replacement.sqlite3").resolve()
    replacement = MetarGroundCompletionOutputStore(replacement_path)
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
        MetarGroundCompletionIntegrityError,
        match="instance authority changed",
    ):
        store.diagnostics()
    adopted = MetarGroundCompletionOutputStore(path)
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
    replacement = MetarGroundCompletionOutputStore(replacement_path)

    def replace_output(point):
        if point == "consumer.before_lifecycle_ack":
            os.replace(replacement_path, runner.output_store.path)

    consumer._fault_injector = replace_output
    with pytest.raises(
        MetarGroundCompletionIntegrityError,
        match="instance changed",
    ):
        consumer.process_leased(lease)
    assert inline.calls == 1
    with pytest.raises(AcquisitionEvidenceConflict):
        env["lifecycle"].load_terminal_consumption(STREAM_ID, 1)
    visible = MetarGroundCompletionOutputStore(runner.output_store.path)
    assert visible.store_instance_sha256 == replacement.store_instance_sha256
    assert visible.load(lease.event_id) is None


def test_crash_after_ground_commit_before_output_retries_without_duplicate_facts(
    tmp_path,
):
    env, runner, consumer, _clock = _setup(tmp_path)
    inline = InlineExecutor()
    runner._executor = inline
    lease = _claim(env)
    original = MetarGroundCompletionOutputStore.commit
    crashed = False

    def crash_once(store, output):
        nonlocal crashed
        if not crashed:
            crashed = True
            raise RuntimeError("crash-between-ground-and-output")
        return original(store, output)

    with patch.object(MetarGroundCompletionOutputStore, "commit", crash_once):
        with pytest.raises(RuntimeError, match="between-ground-and-output"):
            consumer.process_leased(lease)
    facts_after_crash = runner.evidence_store.stats()["ground_facts"]
    assert facts_after_crash >= 2
    assert runner.output_store.diagnostics()["receipts"] == 0

    result = consumer.process_leased(lease)
    assert result.status == "acknowledged"
    assert inline.calls == 2
    assert runner.evidence_store.stats()["ground_facts"] == facts_after_crash
    assert runner.output_store.diagnostics()["receipts"] == 1



def test_decoder_failure_and_ground_capacity_are_atomic_and_unacked(tmp_path):
    env, runner, consumer, _clock = _setup(
        tmp_path / "malformed", payload=b"not-a-gzip-stream"
    )
    runner._executor = ContainedInlineExecutor()
    lease = _claim(env)
    result = consumer.process_leased(lease)
    assert result.status == "failed"
    assert runner.evidence_store.stats()["ground_facts"] == 0
    assert runner.output_store.diagnostics()["receipts"] == 0
    assert _terminal(env, lease) is None

    limited_env, limited_runner, limited_consumer, _ = _setup(
        tmp_path / "limited",
        ground_limits=GroundEvidenceLimits(
            max_catalog_bytes=8 * 1024 * 1024,
            max_observations=1,
            max_member_links=16,
            max_qc_rows=128,
            max_query_rows=16,
        ),
    )
    limited_runner._executor = ContainedInlineExecutor()
    limited_lease = _claim(limited_env)
    limited = limited_consumer.process_leased(limited_lease)
    assert limited.status == "failed"
    assert limited_runner.evidence_store.stats()["ground_facts"] == 0
    assert limited_runner.output_store.diagnostics()["receipts"] == 0
    assert _terminal(limited_env, limited_lease) is None


def test_deadline_cancellation_and_cancel_after_output_never_ack(tmp_path):
    env, runner, consumer, _clock = _setup(tmp_path)
    runner._executor = TerminalExecutor("deadline_exceeded")
    lease = _claim(env)
    result = consumer.process_leased(lease)
    assert result.status == "deadline"
    assert _terminal(env, lease) is None

    cancelled = threading.Event()
    cancelled.set()
    with pytest.raises(MetarGroundCompletionCancelled):
        consumer.process_leased(lease, cancel_event=cancelled)
    with pytest.raises(MetarGroundCompletionDeadlineError):
        consumer.run_once(owner_id="too-short", lease_seconds=1.0)

    env2, runner2, consumer2, _ = _setup(tmp_path / "after-output")
    runner2._executor = InlineExecutor()
    cancelled2 = threading.Event()

    def cancel_after_output(point):
        if point == "consumer.after_output_commit":
            cancelled2.set()

    consumer2._fault_injector = cancel_after_output
    lease2 = _claim(env2)
    with pytest.raises(MetarGroundCompletionCancelled):
        consumer2.process_leased(lease2, cancel_event=cancelled2)
    assert runner2.output_store.diagnostics()["receipts"] == 1
    assert _terminal(env2, lease2) is None


def test_raw_byte_tamper_before_validation_and_at_final_ack_fails_closed(tmp_path):
    env, _runner, _consumer, _clock = _setup(tmp_path / "before")
    lease = _claim(env)
    desired = env["desired"].load_active()
    assert desired is not None
    member = json.loads(lease.payload)["capture_reference"]["members"][0]
    receipt, payload = env["raw"].read_receipt_bytes(member["receipt_id"])
    Path(receipt.artifact_path).write_bytes(
        bytes([payload[0] ^ 1]) + payload[1:]
    )
    with pytest.raises(MetarGroundCompletionIntegrityError):
        validate_metar_ground_completion_lease(
            lease, desired_state=desired, raw_store=env["raw"]
        )

    env2, runner2, consumer2, _ = _setup(tmp_path / "final")
    runner2._executor = InlineExecutor()

    def tamper_at_ack(point):
        if point == "consumer.before_lifecycle_ack":
            capture = env2["capture"]
            receipt_id = capture.members[0].receipt_id
            raw_receipt, raw_bytes = env2["raw"].read_receipt_bytes(
                receipt_id
            )
            Path(raw_receipt.artifact_path).write_bytes(
                bytes([raw_bytes[0] ^ 1]) + raw_bytes[1:]
            )

    consumer2._fault_injector = tamper_at_ack
    lease2 = _claim(env2)
    with pytest.raises(
        MetarGroundCompletionIntegrityError,
        match="raw authority failed reproof",
    ):
        consumer2.process_leased(lease2)
    assert runner2.output_store.diagnostics()["receipts"] == 1
    assert _terminal(env2, lease2) is None


def test_physical_ground_schema_tamper_after_output_blocks_final_ack(tmp_path):
    env, runner, consumer, _clock = _setup(tmp_path)
    runner._executor = InlineExecutor()

    def tamper_ground(point):
        if point == "consumer.before_lifecycle_ack":
            with closing(sqlite3.connect(runner.evidence_store.path)) as database:
                database.execute("DROP TRIGGER ground_facts_no_update")
                database.commit()

    consumer._fault_injector = tamper_ground
    lease = _claim(env)
    with pytest.raises(
        MetarGroundCompletionIntegrityError,
        match="factset failed physical reproof",
    ):
        consumer.process_leased(lease)
    assert runner.output_store.diagnostics()["receipts"] == 1
    assert _terminal(env, lease) is None


def test_duplicate_and_concurrent_calls_are_idempotent(tmp_path):
    from concurrent.futures import ThreadPoolExecutor

    env, runner, _consumer, _clock = _setup(tmp_path)
    inline = InlineExecutor()
    runner._executor = inline
    lease = _claim(env)
    desired = env["desired"].load_active()
    assert desired is not None

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [
            pool.submit(
                runner.execute,
                lease=lease,
                desired_state=desired,
                cancel_event=threading.Event(),
            )
            for _ in range(2)
        ]
        results = [future.result() for future in futures]

    assert all(item.status == "committed" for item in results)
    assert results[0].output_receipt == results[1].output_receipt
    assert runner.output_store.diagnostics()["receipts"] == 1
    count = runner.evidence_store.stats()["ground_facts"]
    assert count == results[0].output_receipt.fact_count
    third = runner.execute(
        lease=lease,
        desired_state=desired,
        cancel_event=threading.Event(),
    )
    assert third.output_receipt == results[0].output_receipt
    assert inline.calls in {1, 2}


def test_obsolete_desired_is_durably_rejected_without_spawn(tmp_path):
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
    assert env["lifecycle"].diagnostics()["rejection_receipts"] == 1
    assert env["lifecycle"].claim_next(
        CONSUMER_ID,
        STREAM_ID,
        "obsolete-reclaim",
        now=NOW + 4.0,
        lease_seconds=30.0,
    ) is None


def test_desired_change_and_lease_reclaim_at_final_boundary_never_ack_old_proof(
    tmp_path,
):
    env, runner, consumer, _clock = _setup(tmp_path / "desired")
    runner._executor = InlineExecutor()

    def change_desired(point):
        if point == "consumer.before_lifecycle_ack":
            _activate_next_desired(env)

    consumer._fault_injector = change_desired
    result = consumer.run_once(
        owner_id="desired-final-race", lease_seconds=30.0
    )
    assert result.status == "rejected"
    assert result.reason_code == "desired_state_changed_before_ack"
    assert runner.output_store.diagnostics()["receipts"] == 1
    assert env["lifecycle"].diagnostics()["rejection_receipts"] == 1

    env2, runner2, consumer2, _ = _setup(tmp_path / "lease")
    runner2._executor = InlineExecutor()
    lease = _claim(env2)
    replacement = None

    def reclaim(point):
        nonlocal replacement
        if point == "consumer.before_lifecycle_ack":
            replacement = env2["lifecycle"].claim_next(
                CONSUMER_ID,
                STREAM_ID,
                "replacement-owner",
                now=lease.lease_expires_at + 1.0,
                lease_seconds=30.0,
            )

    consumer2._fault_injector = reclaim
    with pytest.raises(MetarGroundCompletionConflict, match="ack boundary"):
        consumer2.process_leased(lease)
    assert replacement is not None
    assert replacement.attempt_count == lease.attempt_count + 1
    assert _terminal(env2, lease) is None


@pytest.mark.parametrize(
    "outcome",
    ["cpu_limit_exceeded", "memory_limit_exceeded", "result_too_large"],
)
def test_hard_cpu_rss_and_result_caps_never_ack(outcome, tmp_path):
    env, runner, consumer, _clock = _setup(tmp_path)
    runner._executor = TerminalExecutor(outcome)
    lease = _claim(env)
    result = consumer.process_leased(lease)
    assert result.status == "resource_limited"
    assert result.reason_code == "hard_cpu_rss_or_output_limit"
    assert runner.output_store.diagnostics()["receipts"] == 0
    assert _terminal(env, lease) is None


def test_hard_request_and_output_caps_fail_closed(tmp_path):
    env, runner, consumer, _clock = _setup(
        tmp_path / "request",
        process_limits=MetarGroundCompletionProcessLimits(
            wall_seconds=8.0,
            cpu_seconds=6.0,
            rss_bytes=768 * 1024 * 1024,
            max_request_bytes=1024,
            factset_reproof_seconds=1.0,
        ),
    )
    lease = _claim(env)
    result = consumer.process_leased(lease)
    assert result.status == "contract_failed"
    assert runner.output_store.diagnostics()["receipts"] == 0
    assert _terminal(env, lease) is None

    env2, runner2, consumer2, _ = _setup(
        tmp_path / "output",
        output_limits=MetarGroundCompletionOutputLimits(
            max_receipts=1,
            max_receipt_bytes=1024,
            max_total_output_bytes=1024,
            max_database_bytes=8 * 1024 * 1024,
        ),
    )
    runner2._executor = ContainedInlineExecutor()
    lease2 = _claim(env2)
    result2 = consumer2.process_leased(lease2)
    assert result2.status == "failed"
    assert runner2.output_store.diagnostics()["receipts"] == 0
    assert _terminal(env2, lease2) is None


def test_forged_same_event_factset_and_process_receipt_cannot_suppress_decode(
    tmp_path,
):
    env, runner, _consumer, _clock = _setup(tmp_path)
    lease = _claim(env)
    desired = env["desired"].load_active()
    assert desired is not None
    runner._executor = InlineExecutor()
    legitimate = runner.execute(
        lease=lease,
        desired_state=desired,
        cancel_event=threading.Event(),
    )
    assert legitimate.output_receipt is not None

    forged_store = MetarGroundCompletionOutputStore(
        (tmp_path / "forged-output.sqlite3").resolve(),
        clock=lambda: NOW + 4.0,
    )
    forged_runner = MetarGroundCompletionIsolatedRunner(
        raw_store=runner.raw_store,
        evidence_store=runner.evidence_store,
        output_store=forged_store,
        limits=runner.limits,
    )
    no_spawn = TerminalExecutor("decoder_failed")
    forged_runner._executor = no_spawn
    forged = copy.deepcopy(legitimate.output_receipt.output)
    factset = forged["decoder"]["factset"]
    factset["factset_sha256"] = "a" * 64
    receipt_body = dict(factset)
    receipt_body.pop("receipt_sha256")
    factset["receipt_sha256"] = _digest(receipt_body)
    forged["isolation"]["authority_sha256"] = forged_runner.authority_sha256
    poisoned = forged_store.commit(forged)
    assert poisoned.process_receipt_sha256

    forged_consumer = MetarGroundCompletionConsumer(
        lifecycle_store=env["lifecycle"],
        desired_state_store=env["desired"],
        runner=forged_runner,
        clock=lambda: NOW + 3.0,
    )
    with pytest.raises(
        MetarGroundCompletionIntegrityError,
        match="factset differs",
    ):
        forged_consumer.process_leased(lease)
    assert no_spawn.calls == 0
    assert _terminal(env, lease) is None


def test_rehashed_process_authority_rejects_store_alias_and_raw_overlap(
    tmp_path,
):
    _env, runner, _consumer, _clock = _setup(tmp_path)
    authority = copy.deepcopy(runner._authority)
    authority["stores"]["outputs"]["path"] = authority["stores"][
        "canonical_ground_evidence"
    ]["path"]
    with pytest.raises(
        MetarGroundCompletionIntegrityError,
        match="database paths alias",
    ):
        _load_process_authority(authority, _digest(authority))

    nested_path = runner.raw_store.root / "nested-ground.sqlite3"
    nested_path.write_bytes(runner.evidence_store.path.read_bytes())
    overlap = copy.deepcopy(runner._authority)
    overlap["stores"]["canonical_ground_evidence"]["path"] = str(
        nested_path.resolve()
    )
    with pytest.raises(
        MetarGroundCompletionIntegrityError,
        match="overlaps the raw object store",
    ):
        _load_process_authority(overlap, _digest(overlap))


def test_output_and_process_receipt_tamper_is_detected(tmp_path):
    env, runner, _consumer, _clock = _setup(tmp_path)
    lease = _claim(env)
    desired = env["desired"].load_active()
    assert desired is not None
    runner._executor = InlineExecutor()
    process = runner.execute(
        lease=lease,
        desired_state=desired,
        cancel_event=threading.Event(),
    )
    assert process.output_receipt is not None

    with closing(sqlite3.connect(runner.output_store.path)) as database:
        database.execute("DROP TRIGGER completion_outputs_no_update")
        database.execute(
            "UPDATE completion_outputs SET process_receipt_sha256=? "
            "WHERE event_id=?",
            ("0" * 64, lease.event_id),
        )
        database.commit()
    with pytest.raises(
        MetarGroundCompletionIntegrityError,
        match=r"output .* differ",
    ):
        runner.output_store.load(lease.event_id)


def test_preinflated_output_database_is_rejected_above_page_cap(tmp_path):
    limits = MetarGroundCompletionOutputLimits(
        max_database_bytes=1024 * 1024
    )
    path = (tmp_path / "bounded-output.sqlite3").resolve()
    MetarGroundCompletionOutputStore(path, limits=limits)
    sha = "0" * 64
    with closing(sqlite3.connect(path)) as database:
        database.execute("PRAGMA max_page_count=8192")
        database.execute(
            "INSERT INTO completion_outputs VALUES("
            + ",".join("?" for _ in range(22))
            + ")",
            (
                f"completion-{sha}", sha, "1" * 64, "2" * 64, "3" * 64,
                CONSUMER_ID, STREAM_ID, f"capture-{sha}", "4" * 64,
                f"ground-factset-{sha}", "5" * 64, "6" * 64, 1,
                "7" * 64, "8" * 64, "9" * 64, "a" * 64, "b" * 64,
                "c" * 64, b"x" * (2 * 1024 * 1024),
                2 * 1024 * 1024, NOW,
            ),
        )
        database.commit()
    assert path.stat().st_size > limits.max_database_bytes
    with pytest.raises(
        MetarGroundCompletionCapacityError,
        match="hard page cap",
    ):
        MetarGroundCompletionOutputStore(path, limits=limits)


def test_diagnostics_output_and_child_result_are_private_data_free(tmp_path):
    env, runner, consumer, _clock = _setup(tmp_path)
    runner._executor = InlineExecutor()
    result = consumer.run_once(
        owner_id="metar-privacy", lease_seconds=30.0
    )
    output = runner.output_store.load(result.event_id)
    assert output is not None
    serialized = json.dumps(
        {
            "diagnostics": consumer.diagnostics(),
            "output": output.output,
        },
        sort_keys=True,
    ).lower()
    for forbidden in (
        str(tmp_path).lower(),
        "artifact_path",
        "upstream_id",
        '"provenance":',
        "api_key",
        "password",
        "private_",
        "http://",
        "https://",
        "raw_text",
        "lease_token",
    ):
        assert forbidden not in serialized
    assert len(
        json.dumps(output.output, sort_keys=True).encode("utf-8")
    ) <= runner.output_store.limits.max_receipt_bytes


def test_exact_consumption_receipt_binds_durable_process_output(tmp_path):
    env, runner, consumer, _clock = _setup(tmp_path)
    runner._executor = InlineExecutor()
    lease = _claim(env)
    result = consumer.process_leased(lease)
    terminal = env["lifecycle"].load_terminal_consumption(
        lease.stream_id, lease.stream_sequence
    )
    assert result.status == "acknowledged"
    assert terminal is not None
    receipt = terminal.consumption_receipt
    output = runner.output_store.load(lease.event_id)
    assert output is not None
    assert receipt.event_id == lease.event_id
    assert receipt.event_sha256 == lease.event_sha256
    assert receipt.producer_receipt_sha256 == lease.producer_receipt_sha256
    assert receipt.consumer_id == CONSUMER_ID
    assert receipt.stream_id == STREAM_ID
    assert receipt.stream_sequence == lease.stream_sequence
    assert receipt.output_sha256 == output.output_sha256
    assert result.process_receipt_sha256 == output.process_receipt_sha256


@pytest.mark.skipif(
    not hard_boundary_support().supported,
    reason="platform has no enforceable one-process hard boundary",
)
def test_real_spawn_boundary_returns_only_after_ground_and_output_are_durable(
    tmp_path,
):
    env, runner, consumer, _clock = _setup(
        tmp_path,
        process_limits=MetarGroundCompletionProcessLimits(
            wall_seconds=20.0,
            cpu_seconds=15.0,
            rss_bytes=1024 * 1024 * 1024,
            factset_reproof_seconds=2.0,
        ),
    )
    result = consumer.run_once(
        owner_id="metar-real-spawn", lease_seconds=40.0
    )
    assert result.status == "acknowledged"
    output = runner.output_store.load(result.event_id)
    assert output is not None
    assert runner.evidence_store.stats()["ground_facts"] == output.fact_count
    assert env["lifecycle"].load_terminal_consumption(STREAM_ID, 1) is not None


def test_consumer_is_absent_from_live_service_ha_mqtt_and_ground_fusion():
    forbidden = tuple(
        path
        for path in (REPOSITORY / "storm_tracker_worker").rglob("*.py")
        if path.name
        not in {
            "metar_ground_completion_consumer.py",
            "completion_output_archive_adapters.py",
            "completion_route_adapters.py",
        }
    )
    forbidden += tuple((REPOSITORY / "custom_components").rglob("*.py"))
    needle = "metar_ground_completion_consumer"
    assert all(
        needle not in path.read_text(encoding="utf-8")
        for path in forbidden
    )


COMPLETION_CONSUMERS_CRASH_CHILD = (
    Path(__file__).resolve().parent
    / "helpers"
    / "completion_consumers_os_exit73_child.py"
)
_METAR_MIGRATION_CRASH_POINTS = (
    "migration.after_metadata",
    "migration.after_triggers",
    "migration.before_commit",
    "migration.after_commit",
)
_METAR_OUTPUT_CRASH_POINTS = (
    "output.before_commit",
    "output.after_commit",
)
_METAR_CONSUMER_CRASH_POINTS = (
    "consumer.after_output_commit",
    "consumer.before_lifecycle_ack",
    "consumer.after_lifecycle_ack",
)
_METAR_ALL_CRASH_POINTS = (
    *_METAR_MIGRATION_CRASH_POINTS,
    *_METAR_OUTPUT_CRASH_POINTS,
    *_METAR_CONSUMER_CRASH_POINTS,
)
_METAR_CRASH_CASES = (
    *(("migration", point) for point in _METAR_MIGRATION_CRASH_POINTS),
    *(("output", point) for point in _METAR_OUTPUT_CRASH_POINTS),
    *(("consumer", point) for point in _METAR_CONSUMER_CRASH_POINTS),
)


class NativeCrashRecordingInlineExecutor(InlineExecutor):
    """Inline child target with one fsync-backed call witness."""

    def __init__(self, call_log: Path, *, output_fault=None) -> None:
        super().__init__()
        self.call_log = call_log
        self.output_fault = output_fault

    def execute(self, **kwargs):
        descriptor = os.open(
            self.call_log,
            os.O_WRONLY | os.O_CREAT | os.O_APPEND,
            0o600,
        )
        try:
            os.write(descriptor, b"decode\n")
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        if self.output_fault is None:
            return super().execute(**kwargs)

        original = MetarGroundCompletionOutputStore.__init__

        def initialize_with_fault(instance, *args, **init_kwargs):
            assert "fault_injector" not in init_kwargs
            init_kwargs["fault_injector"] = self.output_fault
            original(instance, *args, **init_kwargs)

        with patch.object(
            MetarGroundCompletionOutputStore,
            "__init__",
            initialize_with_fault,
        ):
            return super().execute(**kwargs)


def _reopen_metar_native_crash_case(
    root: Path,
    *,
    executor: NativeCrashRecordingInlineExecutor,
    consumer_fault=None,
):
    env = _reopen_environment(root, WORKLOAD_ID)
    evidence = CanonicalGroundEvidenceStore(
        (root / "canonical-ground.sqlite3").resolve(),
        raw_artifact_reader=env["raw"],
        limits=GroundEvidenceLimits(
            max_catalog_bytes=32 * 1024 * 1024,
            max_observations=4096,
            max_member_links=4096,
            max_qc_rows=65536,
            max_query_rows=1000,
        ),
    )
    outputs = MetarGroundCompletionOutputStore(
        (root / "metar-completion.sqlite3").resolve(),
        clock=lambda: NOW + 4.0,
    )
    runner = MetarGroundCompletionIsolatedRunner(
        raw_store=env["raw"],
        evidence_store=evidence,
        output_store=outputs,
        limits=MetarGroundCompletionProcessLimits(
            wall_seconds=8.0,
            cpu_seconds=6.0,
            rss_bytes=768 * 1024 * 1024,
            factset_reproof_seconds=1.0,
        ),
    )
    runner._executor = executor
    consumer = MetarGroundCompletionConsumer(
        lifecycle_store=env["lifecycle"],
        desired_state_store=env["desired"],
        runner=runner,
        clock=lambda: NOW + 3.0,
        fault_injector=consumer_fault,
    )
    return env, runner, consumer


def run_metar_ground_completion_crash_child(
    root: Path,
    phase: str,
    checkpoint: str,
    event_id: str | None,
    lease_token: str | None,
    crash,
) -> None:
    expected = {
        "migration": set(_METAR_MIGRATION_CRASH_POINTS),
        "output": set(_METAR_OUTPUT_CRASH_POINTS),
        "consumer": set(_METAR_CONSUMER_CRASH_POINTS),
    }
    assert checkpoint in expected[phase]
    if phase == "migration":
        assert event_id is None and lease_token is None
        MetarGroundCompletionOutputStore(
            (root / "metar-completion.sqlite3").resolve(),
            clock=lambda: NOW + 4.0,
            fault_injector=crash,
        )
        return

    assert event_id is not None and lease_token is not None
    call_log = root / "native-crash-decode-calls.log"
    executor = NativeCrashRecordingInlineExecutor(
        call_log,
        output_fault=crash if phase == "output" else None,
    )
    env, _runner, consumer = _reopen_metar_native_crash_case(
        root,
        executor=executor,
        consumer_fault=crash if phase == "consumer" else None,
    )
    lease = env["lifecycle"].load_leased(event_id, lease_token)
    assert lease is not None
    consumer.process_leased(lease)


def _run_metar_native_crash(
    root: Path,
    phase: str,
    checkpoint: str,
    lease=None,
) -> subprocess.CompletedProcess[bytes]:
    arguments = [
        sys.executable,
        str(COMPLETION_CONSUMERS_CRASH_CHILD),
        "metar-ground",
        str(root),
        phase,
        checkpoint,
    ]
    if lease is not None:
        arguments.extend((lease.event_id, lease.lease_token))
    return subprocess.run(
        arguments,
        cwd=REPOSITORY,
        capture_output=True,
        check=False,
        timeout=180.0,
    )


def _metar_fault_calls() -> Counter[str]:
    source = (
        REPOSITORY
        / "storm_tracker_worker"
        / "metar_ground_completion_consumer.py"
    )
    tree = ast.parse(source.read_text(encoding="utf-8"), filename=str(source))
    points: list[str] = []
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "_fault"
            and len(node.args) == 1
            and isinstance(node.args[0], ast.Constant)
            and isinstance(node.args[0].value, str)
        ):
            points.append(node.args[0].value)
    return Counter(points)


def _metar_migration_state(path: Path) -> dict[str, object]:
    with closing(sqlite3.connect(path)) as database:
        tables = {
            str(row[0])
            for row in database.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name NOT LIKE 'sqlite_%'"
            )
        }
        columns = {
            str(row[1])
            for row in database.execute("PRAGMA table_info(output_metadata)")
        }
        metadata_row = database.execute(
            "SELECT * FROM output_metadata WHERE singleton=1"
        ).fetchone()
        assert metadata_row is not None
        metadata_names = [
            str(row[1])
            for row in database.execute("PRAGMA table_info(output_metadata)")
        ]
        metadata = dict(zip(metadata_names, metadata_row, strict=True))
        triggers = {
            str(row[0])
            for row in database.execute(
                "SELECT name FROM sqlite_master WHERE type='trigger'"
            )
        }
        receipts = int(
            database.execute("SELECT count(*) FROM completion_outputs").fetchone()[0]
        )
    return {
        "columns": columns,
        "metadata": metadata,
        "receipts": receipts,
        "tables": tables,
        "triggers": triggers,
    }


def _metar_sqlite_logical_sha256(path: Path) -> str:
    def value(item):
        if isinstance(item, bytes):
            return {"bytes_hex": item.hex()}
        return item

    document: list[object] = []
    with closing(sqlite3.connect(path)) as database:
        schema = [
            tuple(value(item) for item in row)
            for row in database.execute(
                "SELECT type,name,tbl_name,sql FROM sqlite_master "
                "WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name"
            )
        ]
        document.append(schema)
        tables = [
            str(row[0])
            for row in database.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name NOT LIKE 'sqlite_%' ORDER BY name"
            )
        ]
        for table in tables:
            quoted = table.replace('"', '""')
            rows = [
                [value(item) for item in row]
                for row in database.execute(f'SELECT * FROM "{quoted}"')
            ]
            rows.sort(
                key=lambda row: json.dumps(
                    row, sort_keys=True, separators=(",", ":")
                )
            )
            document.append((table, rows))
    return hashlib.sha256(
        json.dumps(document, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
    ).hexdigest()


def _metar_capture_authority_snapshot(raw_store, capture_id: str):
    loaded = raw_store.load_capture_bundle_bytes(capture_id)
    assert loaded is not None
    capture, receipts, payloads = loaded
    return (
        capture,
        tuple(
            (
                role,
                receipts[role],
                hashlib.sha256(payloads[role]).hexdigest(),
                len(payloads[role]),
            )
            for role in sorted(receipts)
        ),
    )


def _metar_assert_journal(path: Path, *, expected: bool) -> None:
    journal = Path(str(path) + "-journal")
    assert journal.exists() is expected
    if expected:
        assert journal.is_file()
        assert journal.stat().st_size > 0
    assert not Path(str(path) + "-wal").exists()
    assert not Path(str(path) + "-shm").exists()


def _metar_call_count(path: Path) -> int:
    return 0 if not path.exists() else len(path.read_bytes().splitlines())


def test_native_crash_matrix_pins_every_metar_production_fault_occurrence():
    expected = Counter({point: 1 for point in _METAR_ALL_CRASH_POINTS})
    assert len(_METAR_ALL_CRASH_POINTS) == 9
    assert len(set(_METAR_ALL_CRASH_POINTS)) == 9
    assert Counter(point for _phase, point in _METAR_CRASH_CASES) == expected
    assert _metar_fault_calls() == expected


@pytest.mark.parametrize(
    ("phase", "checkpoint"),
    _METAR_CRASH_CASES,
    ids=[f"{phase}-{point}" for phase, point in _METAR_CRASH_CASES],
)
def test_native_os_exit73_metar_fault_recovers_exactly_once_without_growth(
    phase,
    checkpoint,
    tmp_path,
):
    root = (tmp_path / "metar-native-crash").resolve()
    root.mkdir(parents=True)
    output_path = (root / "metar-completion.sqlite3").resolve()

    if phase == "migration":
        MetarGroundCompletionOutputStore(output_path, clock=lambda: NOW + 4.0)
        _downgrade_output_store_to_v1(output_path)
        legacy = _metar_migration_state(output_path)
        assert legacy["tables"] == {"output_metadata", "completion_outputs"}
        assert legacy["columns"] == set(
            MetarGroundCompletionOutputStore._LEGACY_METADATA_COLUMNS
        )
        assert legacy["metadata"]["contract"] == LEGACY_OUTPUT_STORE_CONTRACT
        assert legacy["metadata"]["schema_version"] == LEGACY_SCHEMA_VERSION
        assert legacy["receipts"] == 0
        assert legacy["triggers"] == set(
            MetarGroundCompletionOutputStore._legacy_trigger_definitions()
        )

        crashed = _run_metar_native_crash(root, phase, checkpoint)
        assert crashed.returncode == 73
        assert crashed.stdout == b""
        assert crashed.stderr == b""
        rolled_back = checkpoint != "migration.after_commit"
        _metar_assert_journal(output_path, expected=rolled_back)
        crash_state = _metar_migration_state(output_path)
        if rolled_back:
            assert crash_state == legacy
        else:
            assert crash_state["columns"] == set(
                MetarGroundCompletionOutputStore._METADATA_COLUMNS
            )
            assert crash_state["metadata"]["contract"] == OUTPUT_STORE_CONTRACT
            assert crash_state["metadata"]["schema_version"] == SCHEMA_VERSION
            assert len(crash_state["metadata"]["store_instance_sha256"]) == 64
            assert crash_state["receipts"] == 0
            assert crash_state["triggers"] == set(
                MetarGroundCompletionOutputStore._trigger_definitions()
            )

        recovered = MetarGroundCompletionOutputStore(
            output_path, clock=lambda: NOW + 4.0
        )
        current = _metar_migration_state(output_path)
        assert current["tables"] == {"output_metadata", "completion_outputs"}
        assert current["columns"] == set(
            MetarGroundCompletionOutputStore._METADATA_COLUMNS
        )
        assert current["metadata"]["contract"] == OUTPUT_STORE_CONTRACT
        assert current["metadata"]["schema_version"] == SCHEMA_VERSION
        assert current["metadata"]["store_instance_sha256"] == (
            recovered.store_instance_sha256
        )
        assert len(recovered.store_instance_sha256) == 64
        assert current["triggers"] == set(recovered._trigger_definitions())
        assert recovered.diagnostics() == {
            "contract": OUTPUT_STORE_CONTRACT,
            "healthy": True,
            "output_bytes": 0,
            "receipts": 0,
            "runtime_wiring": "inert",
        }
        recovered_sha = _metar_sqlite_logical_sha256(output_path)
        reopened = MetarGroundCompletionOutputStore(
            output_path, clock=lambda: NOW + 4.0
        )
        assert reopened.identity_sha256 == recovered.identity_sha256
        assert _metar_sqlite_logical_sha256(output_path) == recovered_sha
        _metar_assert_journal(output_path, expected=False)
        return

    env, _runner, _consumer, _clock = _setup(root)
    lease = _claim(env)
    raw_before = _metar_capture_authority_snapshot(
        env["raw"], env["capture"].capture_id
    )
    raw_stats_before = env["raw"].stats()
    call_log = root / "native-crash-decode-calls.log"

    crashed = _run_metar_native_crash(root, phase, checkpoint, lease)
    assert crashed.returncode == 73
    assert crashed.stdout == b""
    assert crashed.stderr == b""
    _metar_assert_journal(
        output_path,
        expected=checkpoint == "output.before_commit",
    )
    assert _metar_call_count(call_log) == 1

    evidence_path = (root / "canonical-ground.sqlite3").resolve()
    evidence_before_sha = _metar_sqlite_logical_sha256(evidence_path)
    executor = NativeCrashRecordingInlineExecutor(call_log)
    reopened_env, runner, consumer = _reopen_metar_native_crash_case(
        root, executor=executor
    )
    output_before = runner.output_store.load(lease.event_id)
    expected_receipts = 0 if checkpoint == "output.before_commit" else 1
    assert runner.output_store.diagnostics()["receipts"] == expected_receipts
    assert (output_before is not None) is bool(expected_receipts)
    pre_terminal = _terminal(reopened_env, lease)
    terminal_at_crash = checkpoint == "consumer.after_lifecycle_ack"
    assert (pre_terminal is not None) is terminal_at_crash
    before_lifecycle = reopened_env["lifecycle"].diagnostics()
    assert sum(before_lifecycle["outbox_states"].values()) == 1
    assert before_lifecycle["outbox_states"][
        "consumed" if terminal_at_crash else "leased"
    ] == 1
    evidence_stats = runner.evidence_store.stats()
    assert evidence_stats["ground_facts"] >= 2
    evidence_audit = runner.evidence_store.audit()
    assert evidence_audit == {
        "healthy": True,
        "checked_facts": evidence_stats["ground_facts"],
        "issues": [],
    }

    if terminal_at_crash:
        recovered = consumer.run_once(
            owner_id="metar-native-crash-terminal",
            lease_seconds=30.0,
        )
        assert recovered.status == "idle"
    else:
        reloaded = reopened_env["lifecycle"].load_leased(
            lease.event_id, lease.lease_token
        )
        assert reloaded == lease
        recovered = consumer.process_leased(reloaded)
        assert recovered.status == "acknowledged"

    expected_calls = 2 if checkpoint == "output.before_commit" else 1
    assert _metar_call_count(call_log) == expected_calls
    output_after = runner.output_store.load(lease.event_id)
    assert output_after is not None
    if output_before is not None:
        assert output_after == output_before
    terminal = reopened_env["lifecycle"].load_terminal_consumption(
        STREAM_ID, lease.stream_sequence
    )
    assert terminal is not None
    assert terminal.consumption_receipt.event_id == lease.event_id
    assert terminal.consumption_receipt.event_sha256 == lease.event_sha256
    assert terminal.consumption_receipt.producer_receipt_sha256 == (
        lease.producer_receipt_sha256
    )
    assert terminal.consumption_receipt.output_sha256 == output_after.output_sha256
    assert output_after.event_sha256 == lease.event_sha256
    assert len(output_after.receipt_sha256) == 64
    assert len(output_after.output["decoder"]["factset"]["receipt_sha256"]) == 64
    assert len(output_after.output_sha256) == 64
    assert len(output_after.process_receipt_sha256) == 64
    assert runner.output_store.diagnostics()["receipts"] == 1
    assert runner.evidence_store.stats() == evidence_stats
    assert _metar_sqlite_logical_sha256(evidence_path) == evidence_before_sha
    assert _metar_capture_authority_snapshot(
        reopened_env["raw"], env["capture"].capture_id
    ) == raw_before
    assert reopened_env["raw"].stats() == raw_stats_before
    raw_audit = reopened_env["raw"].integrity_check()
    assert raw_audit["healthy"] is True
    assert raw_audit["issues"] == []

    output_sha = _metar_sqlite_logical_sha256(output_path)
    evidence_sha = _metar_sqlite_logical_sha256(evidence_path)
    final_lifecycle = reopened_env["lifecycle"].diagnostics()
    assert sum(final_lifecycle["outbox_states"].values()) == 1
    assert final_lifecycle["outbox_states"]["consumed"] == 1
    replay = consumer.run_once(
        owner_id="metar-native-crash-replay",
        lease_seconds=30.0,
    )
    assert replay.status == "idle"
    assert _metar_call_count(call_log) == expected_calls
    assert runner.output_store.load(lease.event_id) == output_after
    assert _metar_sqlite_logical_sha256(output_path) == output_sha
    assert _metar_sqlite_logical_sha256(evidence_path) == evidence_sha
    assert runner.evidence_store.audit()["healthy"] is True
    assert reopened_env["raw"].integrity_check()["healthy"] is True
    serialized = json.dumps(
        {
            "consumer": consumer.diagnostics(),
            "lifecycle": final_lifecycle,
            "output": output_after.output,
        },
        sort_keys=True,
    ).lower()
    for forbidden in (
        str(root).lower(),
        "artifact_path",
        "upstream_id",
        "api_key",
        "password",
        "private_",
        "http://",
        "https://",
        "lease_token",
    ):
        assert forbidden not in serialized
