from __future__ import annotations
import ast
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
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

import pytest

import storm_tracker_worker.raw_manifest_audit_completion_consumer as raw_audit
from storm_tracker_worker.acquisition_evidence_lifecycle import AcquisitionEvidenceConflict
from storm_tracker_worker.acquisition_evidence_lifecycle import ConsumptionReceipt
from storm_tracker_worker.desired_state import DesiredStateStore, parse_desired_state
from storm_tracker_worker.hard_decoder_isolation import DecoderProcessResult, hard_boundary_support
from storm_tracker_worker.raw_manifest_audit_completion_consumer import (
    AUDIT_CONSUMER_CATALOG,
    PROCESS_EFFECT_CONTRACT,
    PROCESS_TARGET,
    RawManifestAuditCancelled,
    RawManifestAuditCompletionConsumer,
    RawManifestAuditConflict,
    RawManifestAuditDeadlineError,
    RawManifestAuditIntegrityError,
    RawManifestAuditIsolatedRunner,
    RawManifestAuditOutputLimits,
    RawManifestAuditOutputStore,
    RawManifestAuditProcessLimits,
    _make_output,
    _raw_manifest_audit_child,
    validate_audit_consumer_catalog,
    validate_raw_manifest_audit_lease,
)
import test_worker_terminal_raw_completion_router as router_fixture
from test_worker_terminal_raw_completion_router import NOW, _environment, _router

REPOSITORY = Path(__file__).resolve().parents[1]
EXPECTED = {
    "radar.aemet.composite",
    "radar.dpc.sri",
    "radar.meteofrance.dpradar",
    "satellite.goes18.rrqpe",
    "satellite.goes19.rrqpe",
    "satellite.hsaf.h40b",
}
EXPECTED_CATALOG = {
    *EXPECTED,
    "ground.kmi.aws_10min",
    "ground.netatmo.region",
    "ground.synoptic.region",
    "lightning.eumetsat.li",
    "lightning.goes18.glm",
    "lightning.goes19.glm",
    "model.openmeteo.italiameteo_icon2i.region",
    "model.openmeteo.region",
    "radar.kmi.appv4_visual",
    "radar.kmi.public_wcs_unknown_quantity",
    "radar.opera.dbzh",
    "radar.rainviewer.visual",
    "validation.geosphere.location",
    "validation.italiameteo.dpc_sri_historical_catalog",
    "validation.meteolux.location",
}


class Inline:
    def __init__(self, crash_after=False):
        self.calls = 0
        self.crash_after = crash_after

    def execute(self, **kwargs):
        self.calls += 1
        assert kwargs["target"] == PROCESS_TARGET
        assert kwargs["effect_contract"] == PROCESS_EFFECT_CONTRACT
        result = _raw_manifest_audit_child(kwargs["request"])
        if self.crash_after:
            return DecoderProcessResult("child_crashed")
        return DecoderProcessResult("succeeded", result=result, child_exit_code=0)


class Terminal:
    def __init__(self, outcome):
        self.outcome = outcome

    def execute(self, **kwargs):
        return DecoderProcessResult(self.outcome)


class Contained(Inline):
    def execute(self, **kwargs):
        try:
            return super().execute(**kwargs)
        except Exception:
            return DecoderProcessResult("decoder_failed")


class Clock:
    def __init__(self, value):
        self.value = value

    def __call__(self):
        return self.value


class FaultOnce:
    def __init__(self, point):
        self.point = point
        self.hit = False

    def __call__(self, point):
        if point == self.point and not self.hit:
            self.hit = True
            raise RuntimeError(f"fault:{point}")


class DdlDeleteRace:
    """Attempt the complete trigger-drop/delete/recreate attack under the fence."""

    def __init__(self, store):
        self.path = store.path
        self.hit = False
        trigger = store._trigger_definitions()["audit_outputs_no_delete"]
        self.script = (
            "DROP TRIGGER audit_outputs_no_delete;"
            "DELETE FROM audit_outputs;"
            f"{trigger};"
        )

    def __call__(self, point):
        if point != "consumer.after_completion_reproof_before_ack" or self.hit:
            return
        self.hit = True
        with closing(sqlite3.connect(self.path, timeout=0.01, isolation_level=None)) as database:
            database.execute("PRAGMA busy_timeout=1")
            database.executescript(self.script)


def downgrade_output_store_to_v1(path):
    """Produce the exact prior v1 store boundary from a valid v2 store."""

    with closing(sqlite3.connect(path)) as database:
        database.execute("BEGIN IMMEDIATE")
        for trigger in raw_audit.RawManifestAuditOutputStore._trigger_definitions():
            database.execute(f"DROP TRIGGER {trigger}")
        database.execute(
            "UPDATE audit_metadata SET contract=?,schema_version=? WHERE singleton=1",
            (
                raw_audit.LEGACY_OUTPUT_STORE_CONTRACT,
                raw_audit.LEGACY_SCHEMA_VERSION,
            ),
        )
        database.execute(
            "ALTER TABLE audit_metadata DROP COLUMN store_instance_sha256"
        )
        for statement in (
            raw_audit.RawManifestAuditOutputStore._legacy_trigger_definitions().values()
        ):
            database.execute(statement)
        database.commit()


def setup_case(tmp_path, workload, limits=None, process=None, fault=None):
    profiles = {
        "radar.aemet.composite": ("ES", 40.4, -3.7),
        "radar.dpc.sri": ("IT", 42.5, 12.5),
        "radar.meteofrance.dpradar": ("FR", 46.0, 2.0),
        "satellite.goes18.rrqpe": ("US", 35.0, -100.0),
        "satellite.goes19.rrqpe": ("BE", 51.04, 4.48),
        "satellite.hsaf.h40b": ("BE", 51.04, 4.48),
    }
    original = router_fixture._desired

    def selected(_workload=None):
        doc = original(None).as_dict()
        country, lat, lon = profiles[workload]
        doc["targets"][0].update(
            country_code=country, latitude=lat, longitude=lon, display_name="Audit Target"
        )
        return parse_desired_state(doc)

    router_fixture._desired = selected
    try:
        env = _environment(tmp_path, workload)
    finally:
        router_fixture._desired = original
    assert _router(env).publish_next().status == "published"
    outputs = RawManifestAuditOutputStore(
        (tmp_path / "audit.sqlite3").resolve(),
        limits=limits or RawManifestAuditOutputLimits(),
        clock=lambda: NOW + 4,
    )
    runner = RawManifestAuditIsolatedRunner(
        raw_store=env["raw"],
        output_store=outputs,
        limits=process
        or RawManifestAuditProcessLimits(
            wall_seconds=5, cpu_seconds=4, rss_bytes=512 * 1024 * 1024
        ),
    )
    clock = Clock(NOW + 3)
    consumer = RawManifestAuditCompletionConsumer(
        lifecycle_store=env["lifecycle"],
        desired_state_store=env["desired"],
        runner=runner,
        clock=clock,
        fault_injector=fault,
    )
    return env, runner, consumer, clock


def supersede_desired(env, *, issued_at=NOW + 5):
    active = env["desired"].load_active()
    assert active is not None
    document = active.as_dict()
    document["generation"] += 1
    document["issued_at"] = issued_at
    document["previous_state_sha256"] = active.state_sha256
    document["targets"][0]["position_observed_at"] = issued_at
    state = parse_desired_state(document)
    env["desired"].apply(state.as_dict(), received_at=issued_at + 1)
    return state


def claim(env, workload, now=NOW + 3, seconds=120):
    b = AUDIT_CONSUMER_CATALOG[workload]
    lease = env["lifecycle"].claim_next(
        b.consumer_id, b.stream_id, "audit-test", now=now, lease_seconds=seconds
    )
    assert lease is not None
    return lease


def terminal(env, lease):
    try:
        return env["lifecycle"].load_terminal_consumption(lease.stream_id, lease.stream_sequence)
    except AcquisitionEvidenceConflict:
        return None


def test_catalogue_is_exact_closed_current_raw_manifest_audit_set():
    validate_audit_consumer_catalog()
    assert set(AUDIT_CONSUMER_CATALOG) == EXPECTED_CATALOG


@pytest.mark.parametrize("workload", sorted(EXPECTED))
def test_every_provider_role_set_is_reproved_in_child_and_acked_factless(tmp_path, workload):
    env, runner, consumer, _ = setup_case(tmp_path, workload)
    runner._executor = Inline()
    result = consumer.run_once(workload, owner_id="audit-owner", lease_seconds=120)
    assert result.status == "acknowledged"
    output = runner.output_store.load(result.event_id)
    assert output is not None
    recovery = env["binding"]
    assert [role["role"] for role in output.output["audit"]["roles"]] == sorted(
        recovery.required_roles
    )
    assert output.output["claims"] == {
        key: False
        for key in (
            "decoded",
            "fact_staged",
            "meteorological_admission",
            "operational_snapshot_published",
            "raw_bytes_copied",
            "raw_refetch_performed",
        )
    }
    assert output.output["fact_revision"] is None
    public_output = json.dumps(output.output, sort_keys=True).casefold()
    assert str(tmp_path).casefold() not in public_output
    assert not any(
        marker in public_output
        for marker in ("http://", "https://", "artifact_path", "private_", "password", "token=")
    )


def test_parent_validation_is_metadata_only_and_child_does_full_sha_pass(tmp_path, monkeypatch):
    workload = "radar.aemet.composite"
    env, runner, _consumer, _ = setup_case(tmp_path, workload)
    lease = claim(env, workload)
    state = env["desired"].load_active()
    assert state
    calls = []
    original = env["raw"].load_capture_bundle_bytes
    monkeypatch.setattr(
        env["raw"],
        "load_capture_bundle_bytes",
        lambda *a, **k: (calls.append(1), original(*a, **k))[1],
    )
    validate_raw_manifest_audit_lease(lease, desired_state=state)
    assert calls == []
    inline = Inline()
    runner._executor = inline
    runner.execute(lease, state, threading.Event())
    assert runner.output_store.load(lease.event_id)
    runner.reprove_for_ack(lease, state, threading.Event())
    assert inline.calls == 2


def test_child_crash_before_commit_does_not_ack_and_after_commit_reconciles(tmp_path):
    workload = "radar.dpc.sri"
    env, runner, consumer, _ = setup_case(tmp_path, workload)
    runner._executor = Terminal("child_crashed")
    lease = claim(env, workload)
    result = consumer.process_leased(lease)
    assert result.status == "failed"
    assert terminal(env, lease) is None
    runner._executor = Inline(crash_after=True)
    result = consumer.process_leased(lease)
    assert result.status == "failed"
    runner._executor = Inline()
    result = consumer.process_leased(lease)
    assert result.status == "acknowledged"


def test_coordinator_crash_after_commit_restarts_without_second_audit(tmp_path):
    hit = {"done": False}

    def fault(point):
        if point == "consumer.after_output_commit" and not hit["done"]:
            hit["done"] = True
            raise RuntimeError(point)

    workload = "satellite.hsaf.h40b"
    env, runner, consumer, clock = setup_case(tmp_path, workload, fault=fault)
    inline = Inline()
    runner._executor = inline
    with pytest.raises(RuntimeError):
        consumer.run_once(workload, owner_id="crash-a", lease_seconds=20)
    assert inline.calls == 1 and runner.output_store.diagnostics()["receipts"] == 1
    clock.value = NOW + 24
    restarted = RawManifestAuditCompletionConsumer(
        lifecycle_store=env["lifecycle"],
        desired_state_store=env["desired"],
        runner=runner,
        clock=clock,
    )
    assert (
        restarted.run_once(workload, owner_id="crash-b", lease_seconds=20).status == "acknowledged"
    )
    assert inline.calls == 2
    assert runner.output_store.diagnostics()["receipts"] == 1


def test_reference_proof_raw_and_output_poison_fail_closed(tmp_path):
    workload = "radar.meteofrance.dpradar"
    env, runner, _consumer, _ = setup_case(tmp_path, workload)
    lease = claim(env, workload)
    state = env["desired"].load_active()
    assert state
    proof = json.loads(json.dumps(lease.proof))
    proof["workload"]["job_sha256"] = "0" * 64
    with pytest.raises(RawManifestAuditIntegrityError):
        validate_raw_manifest_audit_lease(replace(lease, proof=proof), desired_state=state)
    reference = json.loads(lease.payload)
    rid = reference["capture_reference"]["members"][0]["receipt_id"]
    receipt, payload = env["raw"].read_receipt_bytes(rid)
    Path(receipt.artifact_path).write_bytes(bytes([payload[0] ^ 1]) + payload[1:])
    runner._executor = Contained()
    status, _, out = runner.execute(lease, state, threading.Event())
    assert status == "failed" and out is None


def test_durable_output_and_process_receipt_poison_fail_closed(tmp_path):
    workload = "satellite.hsaf.h40b"
    env, runner, _consumer, _ = setup_case(tmp_path, workload)
    lease = claim(env, workload)
    state = env["desired"].load_active()
    assert state is not None
    runner._executor = Inline()
    status, _, receipt = runner.execute(lease, state, threading.Event())
    assert status == "committed" and receipt is not None

    with closing(sqlite3.connect(runner.output_store.path)) as database:
        database.execute("DROP TRIGGER audit_outputs_no_update")
        database.execute(
            "CREATE TRIGGER audit_outputs_no_update BEFORE UPDATE ON audit_outputs "
            "BEGIN SELECT 1; END"
        )
        database.execute(
            "UPDATE audit_outputs SET process_receipt_sha256=? WHERE event_id=?",
            ("0" * 64, lease.event_id),
        )
    with pytest.raises(RawManifestAuditIntegrityError):
        runner.output_store.load(lease.event_id)


def test_stale_desired_is_durably_rejected_instead_of_reclaimed_forever(tmp_path):
    workload = "radar.dpc.sri"
    env, runner, consumer, _ = setup_case(tmp_path, workload)
    runner._executor = Inline()
    lease = claim(env, workload)
    supersede_desired(env)
    result = consumer.process_leased(lease)
    assert result.status == "rejected"
    assert result.reason_code == "desired_state_superseded_before_processing"
    assert result.consumption_receipt_sha256 is not None
    assert consumer.run_once(workload, owner_id="after-reject", lease_seconds=20).status == "idle"


def test_desired_switch_at_pre_ack_barrier_rejects_durable_output(tmp_path):
    workload = "satellite.goes19.rrqpe"
    switched = {"done": False}

    def fault(point):
        if point == "consumer.before_final_reproof" and not switched["done"]:
            switched["done"] = True
            supersede_desired(env)

    env, runner, consumer, _ = setup_case(tmp_path, workload, fault=fault)
    runner._executor = Inline()
    result = consumer.run_once(workload, owner_id="switch", lease_seconds=120)
    assert result.status == "rejected"
    assert result.reason_code == "desired_state_changed_before_ack"
    assert runner.output_store.diagnostics()["receipts"] == 1
    assert consumer.run_once(workload, owner_id="after-switch", lease_seconds=20).status == "idle"


def test_raw_tamper_at_final_reproof_barrier_never_acks(tmp_path):
    workload = "radar.meteofrance.dpradar"
    tampered = {"done": False}

    def fault(point):
        if point == "consumer.before_final_reproof" and not tampered["done"]:
            tampered["done"] = True
            binding = AUDIT_CONSUMER_CATALOG[workload]
            lease = env["lifecycle"].claim_next(
                binding.consumer_id,
                binding.stream_id,
                "impossible-second-claim",
                now=NOW + 3,
                lease_seconds=120,
            )
            assert lease is None
            reference = json.loads(current_lease.payload)
            receipt_id = reference["capture_reference"]["members"][0]["receipt_id"]
            receipt, payload = env["raw"].read_receipt_bytes(receipt_id)
            Path(receipt.artifact_path).write_bytes(bytes([payload[0] ^ 1]) + payload[1:])

    env, runner, consumer, _ = setup_case(tmp_path, workload, fault=fault)
    runner._executor = Contained()
    current_lease = claim(env, workload)
    result = consumer.process_leased(current_lease)
    assert result.status == "failed"
    assert terminal(env, current_lease) is None


def test_forged_same_event_output_never_suppresses_exact_reproof(tmp_path):
    workload = "satellite.goes18.rrqpe"
    env, runner, consumer, _ = setup_case(tmp_path, workload)
    lease = claim(env, workload)
    desired = env["desired"].load_active()
    assert desired is not None
    binding = validate_raw_manifest_audit_lease(lease, desired_state=desired)
    forged_binding = replace(binding, capture_manifest_sha256="0" * 64)
    runner.output_store.commit(
        _make_output(
            lease,
            forged_binding,
            runner.authority_sha256,
            runner.limits.fingerprint,
        )
    )
    runner._executor = Inline()
    with pytest.raises(RawManifestAuditConflict):
        consumer.process_leased(lease)
    assert terminal(env, lease) is None


def test_store_roles_must_be_disjoint(tmp_path):
    workload = "satellite.hsaf.h40b"
    env, runner, _consumer, _ = setup_case(tmp_path, workload)
    overlapping = RawManifestAuditOutputStore((env["raw"].root / "audit.sqlite3").resolve())
    with pytest.raises(RawManifestAuditIntegrityError):
        RawManifestAuditIsolatedRunner(raw_store=env["raw"], output_store=overlapping)
    with pytest.raises(RawManifestAuditIntegrityError):
        RawManifestAuditCompletionConsumer(
            lifecycle_store=env["lifecycle"],
            desired_state_store=DesiredStateStore(runner.output_store.path),
            runner=runner,
        )


def test_forged_consumption_receipt_is_refused(tmp_path, monkeypatch):
    workload = "satellite.goes19.rrqpe"
    env, runner, consumer, _ = setup_case(tmp_path, workload)
    runner._executor = Inline()
    lease = claim(env, workload)

    def forged_ack(**_kwargs):
        return ConsumptionReceipt(
            receipt_sha256="0" * 64,
            event_id=lease.event_id,
            event_sha256=lease.event_sha256,
            producer_receipt_sha256=lease.producer_receipt_sha256,
            consumer_id=lease.consumer_id,
            stream_id=lease.stream_id,
            stream_sequence=lease.stream_sequence,
            output_sha256="0" * 64,
            created_at=NOW + 3,
        )

    monkeypatch.setattr(env["lifecycle"], "acknowledge", forged_ack)
    with pytest.raises(RawManifestAuditIntegrityError):
        consumer.process_leased(lease)
    assert terminal(env, lease) is None


def test_duplicate_concurrency_converges_on_one_output(tmp_path):
    workload = "satellite.goes18.rrqpe"
    env, runner, _consumer, _ = setup_case(tmp_path, workload)
    lease = claim(env, workload)
    state = env["desired"].load_active()
    runner._executor = Inline()
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(
            pool.map(lambda _: runner.execute(lease, state, threading.Event()), range(2))
        )
    assert all(item[0] == "committed" for item in results)
    assert runner.output_store.diagnostics()["receipts"] == 1


def test_output_store_v1_migrates_atomically_and_preserves_receipt(tmp_path):
    workload = "radar.aemet.composite"
    env, runner, _consumer, _ = setup_case(tmp_path, workload)
    lease = claim(env, workload)
    desired = env["desired"].load_active()
    assert desired is not None
    runner._executor = Inline()
    status, _, receipt = runner.execute(lease, desired, threading.Event())
    assert status == "committed" and receipt is not None
    path = runner.output_store.path
    downgrade_output_store_to_v1(path)

    migrated = RawManifestAuditOutputStore(
        path, limits=runner.output_store.limits, clock=lambda: NOW + 4
    )
    loaded = migrated.load(lease.event_id)
    assert loaded is not None and loaded.output_sha256 == receipt.output_sha256
    assert len(migrated.store_instance_sha256) == 64
    identity = migrated.identity_sha256
    reopened = RawManifestAuditOutputStore(
        path, limits=runner.output_store.limits, clock=lambda: NOW + 4
    )
    assert reopened.identity_sha256 == identity
    with closing(sqlite3.connect(path)) as database:
        metadata = database.execute(
            "SELECT contract,schema_version,store_instance_sha256 "
            "FROM audit_metadata WHERE singleton=1"
        ).fetchone()
        triggers = {
            str(row[0])
            for row in database.execute(
                "SELECT name FROM sqlite_master WHERE type='trigger'"
            )
        }
    assert metadata == (
        raw_audit.OUTPUT_STORE_CONTRACT,
        raw_audit.SCHEMA_VERSION,
        migrated.store_instance_sha256,
    )
    assert triggers == set(migrated._trigger_definitions())


def test_output_store_v1_migration_faults_are_old_or_new(tmp_path):
    rollback_path = (tmp_path / "migration-rollback.sqlite3").resolve()
    RawManifestAuditOutputStore(rollback_path)
    downgrade_output_store_to_v1(rollback_path)
    with pytest.raises(RuntimeError, match="migration.after_metadata"):
        RawManifestAuditOutputStore(
            rollback_path,
            fault_injector=FaultOnce("migration.after_metadata"),
        )
    with closing(sqlite3.connect(rollback_path)) as database:
        metadata = database.execute(
            "SELECT contract,schema_version FROM audit_metadata WHERE singleton=1"
        ).fetchone()
        columns = {
            str(row[1]) for row in database.execute("PRAGMA table_info(audit_metadata)")
        }
        triggers = {
            str(row[0])
            for row in database.execute(
                "SELECT name FROM sqlite_master WHERE type='trigger'"
            )
        }
    assert metadata == (
        raw_audit.LEGACY_OUTPUT_STORE_CONTRACT,
        raw_audit.LEGACY_SCHEMA_VERSION,
    )
    assert "store_instance_sha256" not in columns
    assert triggers == set(RawManifestAuditOutputStore._legacy_trigger_definitions())
    rolled_forward = RawManifestAuditOutputStore(rollback_path)
    assert len(rolled_forward.store_instance_sha256) == 64

    committed_path = (tmp_path / "migration-committed.sqlite3").resolve()
    RawManifestAuditOutputStore(committed_path)
    downgrade_output_store_to_v1(committed_path)
    with pytest.raises(RuntimeError, match="migration.after_commit"):
        RawManifestAuditOutputStore(
            committed_path,
            fault_injector=FaultOnce("migration.after_commit"),
        )
    committed = RawManifestAuditOutputStore(committed_path)
    assert len(committed.store_instance_sha256) == 64


def test_output_store_instance_sentinel_is_immutable_and_live_reproved(tmp_path):
    path = (tmp_path / "identity.sqlite3").resolve()
    store = RawManifestAuditOutputStore(path)
    instance_sha = store.store_instance_sha256
    with closing(sqlite3.connect(path)) as database:
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            database.execute(
                "UPDATE audit_metadata SET store_instance_sha256=? WHERE singleton=1",
                ("f" * 64,),
            )
        database.rollback()
        database.execute("BEGIN IMMEDIATE")
        database.execute("DROP TRIGGER audit_metadata_no_update")
        database.execute(
            "UPDATE audit_metadata SET store_instance_sha256=? WHERE singleton=1",
            ("f" * 64,),
        )
        database.execute(store._trigger_definitions()["audit_metadata_no_update"])
        database.commit()
    assert instance_sha != "f" * 64
    with pytest.raises(RawManifestAuditIntegrityError, match="instance authority"):
        store.assert_instance_held()


def test_output_store_same_path_replacement_changes_lineage_and_live_store_fails(tmp_path):
    path = (tmp_path / "visible.sqlite3").resolve()
    store = RawManifestAuditOutputStore(path)
    original_identity = store.identity_sha256
    replacement = RawManifestAuditOutputStore((tmp_path / "replacement.sqlite3").resolve())
    replacement_instance = replacement.store_instance_sha256
    os.replace(replacement.path, path)
    with pytest.raises(RawManifestAuditIntegrityError, match="instance changed"):
        store.assert_instance_held()
    reopened = RawManifestAuditOutputStore(path)
    assert reopened.store_instance_sha256 == replacement_instance
    assert reopened.identity_sha256 != original_identity


def test_split_finalize_fences_trigger_drop_delete_recreate_race_then_recovers(tmp_path):
    workload = "satellite.goes18.rrqpe"
    env, runner, consumer, _ = setup_case(tmp_path, workload)
    inline = Inline()
    runner._executor = inline
    lease = claim(env, workload)
    pending = consumer.begin_leased(lease)
    assert isinstance(pending, raw_audit.RawManifestAuditPendingAck)
    attack = DdlDeleteRace(runner.output_store)
    consumer._fault = attack
    with pytest.raises(sqlite3.OperationalError, match="locked"):
        consumer.finalize_leased(pending)
    assert attack.hit and terminal(env, lease) is None
    assert runner.output_store.diagnostics()["receipts"] == 1
    with closing(sqlite3.connect(runner.output_store.path)) as database:
        assert database.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='trigger' "
            "AND name='audit_outputs_no_delete'"
        ).fetchone()[0] == 1
        assert database.execute("SELECT count(*) FROM audit_outputs").fetchone()[0] == 1
    consumer._fault = None
    recovered = consumer.finalize_leased(pending)
    assert recovered.status == "acknowledged"
    assert terminal(env, lease) is not None
    assert inline.calls == 3


def test_split_finalize_same_inode_sentinel_tamper_and_path_replacement_never_ack(tmp_path):
    workload = "satellite.goes19.rrqpe"
    env, runner, consumer, _ = setup_case(tmp_path / "sentinel", workload)
    runner._executor = Inline()
    lease = claim(env, workload)
    pending = consumer.begin_leased(lease)
    assert isinstance(pending, raw_audit.RawManifestAuditPendingAck)
    with closing(sqlite3.connect(runner.output_store.path)) as database:
        database.execute("BEGIN IMMEDIATE")
        database.execute("DROP TRIGGER audit_metadata_no_update")
        database.execute(
            "UPDATE audit_metadata SET store_instance_sha256=? WHERE singleton=1",
            ("e" * 64,),
        )
        database.execute(
            runner.output_store._trigger_definitions()["audit_metadata_no_update"]
        )
        database.commit()
    with pytest.raises(RawManifestAuditIntegrityError, match="instance authority"):
        consumer.finalize_leased(pending)
    assert terminal(env, lease) is None

    env2, runner2, consumer2, _ = setup_case(tmp_path / "replacement", workload)
    runner2._executor = Inline()
    lease2 = claim(env2, workload)
    pending2 = consumer2.begin_leased(lease2)
    assert isinstance(pending2, raw_audit.RawManifestAuditPendingAck)
    replacement = RawManifestAuditOutputStore(
        (tmp_path / "replacement-store.sqlite3").resolve(),
        limits=runner2.output_store.limits,
    )
    replacement_instance = replacement.store_instance_sha256
    os.replace(replacement.path, runner2.output_store.path)
    with pytest.raises(RawManifestAuditIntegrityError, match="instance changed"):
        consumer2.finalize_leased(pending2)
    assert terminal(env2, lease2) is None
    visible = RawManifestAuditOutputStore(
        runner2.output_store.path, limits=runner2.output_store.limits
    )
    assert visible.store_instance_sha256 == replacement_instance
    assert visible.load(lease2.event_id) is None


def test_crash_after_lifecycle_ack_recovers_as_terminal_without_second_output(tmp_path):
    workload = "radar.dpc.sri"
    fault = FaultOnce("consumer.after_lifecycle_ack_before_output_reproof_release")
    env, runner, consumer, _ = setup_case(tmp_path, workload, fault=fault)
    inline = Inline()
    runner._executor = inline
    with pytest.raises(RuntimeError, match="after_lifecycle_ack"):
        consumer.run_once(workload, owner_id="ack-crash", lease_seconds=120)
    binding = AUDIT_CONSUMER_CATALOG[workload]
    terminal_receipt = env["lifecycle"].load_terminal_consumption(binding.stream_id, 1)
    assert terminal_receipt is not None
    assert runner.output_store.diagnostics()["receipts"] == 1
    consumer._fault = None
    assert consumer.run_once(workload, owner_id="ack-restart", lease_seconds=120).status == "idle"
    assert inline.calls == 2


def test_cancel_deadline_capacity_privacy_and_unwired(tmp_path):
    workload = "satellite.goes19.rrqpe"
    limits = RawManifestAuditOutputLimits(
        max_receipts=1,
        max_receipt_bytes=1024,
        max_total_output_bytes=1024,
        max_database_bytes=16 * 1024 * 1024,
    )
    env, runner, consumer, _ = setup_case(tmp_path, workload, limits=limits)
    runner._executor = Contained()
    lease = claim(env, workload)
    result = consumer.process_leased(lease)
    assert result.status == "failed" and terminal(env, lease) is None
    cancel = threading.Event()
    cancel.set()
    with pytest.raises(RawManifestAuditCancelled):
        consumer.process_leased(lease, cancel)
    with pytest.raises(RawManifestAuditDeadlineError):
        consumer.run_once(workload, owner_id="short", lease_seconds=1)
    diagnostics = json.dumps(runner.output_store.diagnostics()).lower()
    assert str(tmp_path).lower() not in diagnostics
    assert "provenance" not in diagnostics and "payload" not in diagnostics
    files = (
        REPOSITORY / "storm_tracker_worker" / "service.py",
        REPOSITORY / "storm_tracker_worker" / "worker_first_composition.py",
        REPOSITORY / "storm_tracker_worker" / "worker_first_service_composition.py",
        REPOSITORY / "storm_tracker_worker" / "acquisition_activation.py",
        REPOSITORY / "storm_tracker_worker" / "__init__.py",
    )
    assert all(
        "raw_manifest_audit_completion_consumer" not in path.read_text(encoding="utf-8")
        for path in files
    )


@pytest.mark.parametrize(
    ("outcome", "expected"),
    (
        ("deadline_exceeded", "deadline"),
        ("cancelled", "cancelled"),
        ("busy", "busy"),
        ("cpu_limit_exceeded", "resource_limited"),
        ("memory_limit_exceeded", "resource_limited"),
        ("result_too_large", "resource_limited"),
    ),
)
def test_hard_boundary_terminal_caps_never_ack(tmp_path, outcome, expected):
    workload = "satellite.goes19.rrqpe"
    env, runner, consumer, _ = setup_case(tmp_path, workload)
    runner._executor = Terminal(outcome)
    lease = claim(env, workload)
    result = consumer.process_leased(lease)
    assert result.status == expected
    assert terminal(env, lease) is None


@pytest.mark.skipif(not hard_boundary_support().supported, reason="no hard process boundary")
def test_real_spawn_is_used_for_meaningful_bounded_raw_sha_work(tmp_path):
    workload = "radar.aemet.composite"
    _env, runner, consumer, _ = setup_case(
        tmp_path,
        workload,
        process=RawManifestAuditProcessLimits(
            wall_seconds=20, cpu_seconds=15, rss_bytes=512 * 1024 * 1024
        ),
    )
    assert (
        consumer.run_once(workload, owner_id="real-spawn", lease_seconds=60).status
        == "acknowledged"
    )


# Independent literal inventory: production does not export a checkpoint list,
# and the AST guard below pins every call to one occurrence.
_RAW_MANIFEST_MIGRATION_OS_EXIT73_POINTS = (
    "migration.after_metadata",
    "migration.after_triggers",
    "migration.before_commit",
    "migration.after_commit",
)
_RAW_MANIFEST_RUNTIME_OS_EXIT73_EXPECTATIONS = {
    # terminal at crash, child calls at crash, recovery calls, public status
    "consumer.after_output_commit": (False, 1, 1, "acknowledged"),
    "consumer.before_final_reproof": (False, 1, 1, "acknowledged"),
    "consumer.after_completion_reproof_before_ack": (
        False,
        2,
        1,
        "acknowledged",
    ),
    "consumer.after_lifecycle_ack_before_output_reproof_release": (
        True,
        2,
        0,
        "idle",
    ),
}
_RAW_MANIFEST_ALL_OS_EXIT73_POINTS = (
    *_RAW_MANIFEST_MIGRATION_OS_EXIT73_POINTS,
    *_RAW_MANIFEST_RUNTIME_OS_EXIT73_EXPECTATIONS,
)
_RAW_MANIFEST_NATIVE_WORKLOAD = "satellite.hsaf.h40b"
_NOAA_RAW_MANIFEST_OS_EXIT73_CHILD = (
    REPOSITORY
    / "tests"
    / "helpers"
    / "noaa_raw_manifest_completion_os_exit73_child.py"
)


def _append_raw_manifest_native_call(path: Path) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    try:
        os.write(descriptor, b"audit-child\n")
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _raw_manifest_native_call_count(path: Path) -> int:
    return 0 if not path.exists() else len(path.read_bytes().splitlines())


class RawManifestNativeCrashRecordingInline(Inline):
    def __init__(self, call_log: Path) -> None:
        super().__init__()
        self.call_log = call_log

    def execute(self, **kwargs):
        _append_raw_manifest_native_call(self.call_log)
        return super().execute(**kwargs)


def run_raw_manifest_completion_os_exit73_child(
    root: Path,
    checkpoint: str,
    crash,
) -> None:
    """Reach one allowlisted raw-manifest seam in a fresh process."""

    if checkpoint not in set(_RAW_MANIFEST_ALL_OS_EXIT73_POINTS):
        raise RuntimeError("raw-manifest crash checkpoint is not allowlisted")
    if checkpoint in _RAW_MANIFEST_MIGRATION_OS_EXIT73_POINTS:
        path = (root / "audit.sqlite3").resolve()
        RawManifestAuditOutputStore(path)
        downgrade_output_store_to_v1(path)
        RawManifestAuditOutputStore(path, fault_injector=crash)
        return
    _env, runner, consumer, _clock = setup_case(
        root,
        _RAW_MANIFEST_NATIVE_WORKLOAD,
        fault=crash,
    )
    runner._executor = RawManifestNativeCrashRecordingInline(
        root / "native-child-calls.log"
    )
    consumer.run_once(
        _RAW_MANIFEST_NATIVE_WORKLOAD,
        owner_id="raw-manifest-native-crash",
        lease_seconds=120.0,
    )


def _run_raw_manifest_completion_os_exit73(root: Path, checkpoint: str):
    return subprocess.run(
        [
            sys.executable,
            str(_NOAA_RAW_MANIFEST_OS_EXIT73_CHILD),
            "raw-manifest",
            str(root.resolve()),
            checkpoint,
        ],
        cwd=REPOSITORY,
        capture_output=True,
        check=False,
        timeout=180.0,
    )


def _raw_manifest_sqlite_value(value):
    return ("bytes", value.hex()) if isinstance(value, bytes) else value


def _raw_manifest_sqlite_rows(path: Path, table: str) -> tuple:
    quoted = table.replace('"', '""')
    with closing(sqlite3.connect(path)) as database:
        columns = tuple(
            str(row[1])
            for row in database.execute(f'PRAGMA table_info("{quoted}")')
        )
        rows = [
            tuple(_raw_manifest_sqlite_value(value) for value in row)
            for row in database.execute(f'SELECT * FROM "{quoted}"')
        ]
    rows.sort(key=repr)
    return columns, tuple(rows)


def _raw_manifest_sqlite_logical_sha256(path: Path) -> str:
    document = []
    with closing(sqlite3.connect(path)) as database:
        document.append(
            [
                tuple(_raw_manifest_sqlite_value(value) for value in row)
                for row in database.execute(
                    "SELECT type,name,tbl_name,sql FROM sqlite_master "
                    "WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name"
                )
            ]
        )
        tables = tuple(
            str(row[0])
            for row in database.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name NOT LIKE 'sqlite_%' ORDER BY name"
            )
        )
        for table in tables:
            document.append((table, _raw_manifest_sqlite_rows(path, table)[1]))
    return hashlib.sha256(repr(document).encode("utf-8")).hexdigest()


def _raw_manifest_file_snapshot(root: Path) -> tuple[tuple[str, int, str], ...]:
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


def _raw_manifest_capture_authority_snapshot(raw) -> tuple:
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


def _raw_manifest_migration_shape(path: Path) -> dict[str, object]:
    with closing(sqlite3.connect(path)) as database:
        columns = tuple(
            str(row[1])
            for row in database.execute("PRAGMA table_info(audit_metadata)")
        )
        metadata = tuple(
            database.execute(
                "SELECT * FROM audit_metadata WHERE singleton=1"
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
            database.execute("SELECT count(*) FROM audit_outputs").fetchone()[0]
        )
    return {
        "columns": columns,
        "metadata": metadata,
        "quick_check": quick_check,
        "receipts": receipts,
        "triggers": triggers,
    }


def _assert_raw_manifest_hot_journal(path: Path, *, expected: bool) -> None:
    journal = Path(str(path) + "-journal")
    if expected:
        assert journal.exists()
        assert journal.is_file()
        payload = journal.read_bytes()
        assert len(payload) > 512
        assert any(payload[512:])
    elif journal.exists():
        assert journal.is_file()
        assert journal.read_bytes()[:8] == b"\x00" * 8
    assert not Path(str(path) + "-wal").exists()
    assert not Path(str(path) + "-shm").exists()


def _reopen_raw_manifest_native_crash_stack(root: Path):
    env = router_fixture._reopen_environment(
        root,
        _RAW_MANIFEST_NATIVE_WORKLOAD,
    )
    outputs = RawManifestAuditOutputStore(
        (root / "audit.sqlite3").resolve(),
        clock=lambda: NOW + 4.0,
    )
    runner = RawManifestAuditIsolatedRunner(
        raw_store=env["raw"],
        output_store=outputs,
        limits=RawManifestAuditProcessLimits(
            wall_seconds=5,
            cpu_seconds=4,
            rss_bytes=512 * 1024 * 1024,
        ),
    )
    executor = RawManifestNativeCrashRecordingInline(
        root / "native-child-calls.log"
    )
    runner._executor = executor
    clock = Clock(NOW + 3.0)
    consumer = RawManifestAuditCompletionConsumer(
        lifecycle_store=env["lifecycle"],
        desired_state_store=env["desired"],
        runner=runner,
        clock=clock,
    )
    return env, runner, consumer, executor


def _raw_manifest_runtime_rows(root: Path) -> dict[str, tuple]:
    lifecycle_path = root / "lifecycle" / "acquisition_evidence.sqlite3"
    output_path = root / "audit.sqlite3"
    return {
        "consumption": _raw_manifest_sqlite_rows(
            lifecycle_path, "consumption_receipts"
        ),
        "outbox": _raw_manifest_sqlite_rows(
            lifecycle_path, "completion_outbox"
        ),
        "outputs": _raw_manifest_sqlite_rows(output_path, "audit_outputs"),
    }


def test_raw_manifest_native_crash_inventory_pins_every_production_occurrence():
    source_path = (
        REPOSITORY
        / "storm_tracker_worker"
        / "raw_manifest_audit_completion_consumer.py"
    )
    tree = ast.parse(source_path.read_text(encoding="utf-8"), filename=str(source_path))
    calls = []
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr in {"_fault", "_call_fault"}
            and len(node.args) == 1
            and isinstance(node.args[0], ast.Constant)
            and isinstance(node.args[0].value, str)
        ):
            calls.append(node.args[0].value)
    expected = Counter(
        {point: 1 for point in _RAW_MANIFEST_ALL_OS_EXIT73_POINTS}
    )
    assert len(_RAW_MANIFEST_ALL_OS_EXIT73_POINTS) == 8
    assert len(set(_RAW_MANIFEST_ALL_OS_EXIT73_POINTS)) == 8
    assert Counter(calls) == expected


@pytest.mark.parametrize(
    "fault_point",
    _RAW_MANIFEST_MIGRATION_OS_EXIT73_POINTS,
    ids=lambda value: f"raw-manifest-{value}",
)
def test_native_os_exit73_raw_manifest_migration_is_exactly_old_or_new(
    tmp_path,
    fault_point,
):
    root = (tmp_path / fault_point.replace(".", "-")).resolve()
    root.mkdir()
    crashed = _run_raw_manifest_completion_os_exit73(root, fault_point)
    assert crashed.returncode == 73
    assert crashed.stdout == b""
    assert crashed.stderr == b""
    path = (root / "audit.sqlite3").resolve()
    rolled_back = fault_point != "migration.after_commit"
    _assert_raw_manifest_hot_journal(path, expected=rolled_back)
    shape = _raw_manifest_migration_shape(path)
    _assert_raw_manifest_hot_journal(path, expected=False)
    assert shape["quick_check"] == "ok"
    assert shape["receipts"] == 0
    if rolled_back:
        assert shape["columns"] == tuple(
            item[0]
            for item in RawManifestAuditOutputStore._metadata_columns(legacy=True)
        )
        assert shape["metadata"][1:3] == (
            raw_audit.LEGACY_OUTPUT_STORE_CONTRACT,
            raw_audit.LEGACY_SCHEMA_VERSION,
        )
        assert shape["triggers"] == tuple(
            sorted(RawManifestAuditOutputStore._legacy_trigger_definitions())
        )
    else:
        assert shape["columns"] == tuple(
            item[0]
            for item in RawManifestAuditOutputStore._metadata_columns(legacy=False)
        )
        assert shape["metadata"][1:3] == (
            raw_audit.OUTPUT_STORE_CONTRACT,
            raw_audit.SCHEMA_VERSION,
        )
        assert len(str(shape["metadata"][-1])) == 64
        assert shape["triggers"] == tuple(
            sorted(RawManifestAuditOutputStore._trigger_definitions())
        )

    recovered = RawManifestAuditOutputStore(path)
    assert recovered.diagnostics() == {
        "contract": raw_audit.OUTPUT_STORE_CONTRACT,
        "healthy": True,
        "receipts": 0,
        "output_bytes": 0,
        "runtime_wiring": "inert",
    }
    recovered_shape = _raw_manifest_migration_shape(path)
    recovered_hash = _raw_manifest_sqlite_logical_sha256(path)
    replay = RawManifestAuditOutputStore(path)
    assert replay.identity_sha256 == recovered.identity_sha256
    assert _raw_manifest_migration_shape(path) == recovered_shape
    assert _raw_manifest_sqlite_logical_sha256(path) == recovered_hash
    _assert_raw_manifest_hot_journal(path, expected=False)


@pytest.mark.parametrize(
    "fault_point",
    tuple(_RAW_MANIFEST_RUNTIME_OS_EXIT73_EXPECTATIONS),
    ids=lambda value: f"raw-manifest-{value}",
)
def test_native_os_exit73_raw_manifest_runtime_recovers_and_replays_once(
    tmp_path,
    fault_point,
):
    root = (tmp_path / fault_point.replace(".", "-")).resolve()
    root.mkdir()
    (
        terminal_at_crash,
        calls_at_crash,
        recovery_calls,
        recovery_status,
    ) = _RAW_MANIFEST_RUNTIME_OS_EXIT73_EXPECTATIONS[fault_point]
    crashed = _run_raw_manifest_completion_os_exit73(root, fault_point)
    assert crashed.returncode == 73
    assert crashed.stdout == b""
    assert crashed.stderr == b""
    output_path = (root / "audit.sqlite3").resolve()
    _assert_raw_manifest_hot_journal(output_path, expected=False)
    call_log = root / "native-child-calls.log"
    assert _raw_manifest_native_call_count(call_log) == calls_at_crash
    rows_before = _raw_manifest_runtime_rows(root)
    assert len(rows_before["outbox"][1]) == 1
    outbox = dict(zip(rows_before["outbox"][0], rows_before["outbox"][1][0]))
    assert outbox["state"] == ("consumed" if terminal_at_crash else "leased")
    assert outbox["attempt_count"] == 1
    assert len(rows_before["outputs"][1]) == 1
    assert len(rows_before["consumption"][1]) == int(terminal_at_crash)

    env, runner, consumer, _executor = _reopen_raw_manifest_native_crash_stack(root)
    consumer._clock.value = NOW + 124.0
    raw_stats = env["raw"].stats()
    raw_files = _raw_manifest_file_snapshot(root / "source-raw")
    raw_authority = _raw_manifest_capture_authority_snapshot(env["raw"])
    assert raw_stats["capture_count"] == 1
    assert raw_stats["receipt_count"] >= 1
    assert env["raw"].integrity_check()["healthy"] is True
    output_before = runner.output_store.load(str(outbox["event_id"]))
    assert output_before is not None

    recovered = consumer.run_once(
        _RAW_MANIFEST_NATIVE_WORKLOAD,
        owner_id="raw-manifest-native-recovery",
        lease_seconds=120.0,
    )
    assert recovered.status == recovery_status
    assert _raw_manifest_native_call_count(call_log) == (
        calls_at_crash + recovery_calls
    )
    output_after = runner.output_store.load(str(outbox["event_id"]))
    assert output_after == output_before
    terminal_receipt = env["lifecycle"].load_terminal_consumption(
        str(outbox["stream_id"]),
        int(outbox["stream_sequence"]),
    )
    assert terminal_receipt is not None
    assert terminal_receipt.consumption_receipt.event_id == outbox["event_id"]
    assert terminal_receipt.consumption_receipt.output_sha256 == (
        output_after.output_sha256
    )
    for digest in (
        output_after.output_sha256,
        output_after.receipt_sha256,
        output_after.process_receipt_sha256,
        output_after.capture_manifest_sha256,
    ):
        assert len(digest) == 64
    assert runner.output_store.diagnostics()["receipts"] == 1
    assert env["raw"].stats() == raw_stats
    assert _raw_manifest_capture_authority_snapshot(env["raw"]) == raw_authority
    assert _raw_manifest_file_snapshot(root / "source-raw") == raw_files
    assert _raw_manifest_capture_authority_snapshot(env["raw"]) == raw_authority
    assert env["raw"].integrity_check()["healthy"] is True

    output_hash = _raw_manifest_sqlite_logical_sha256(output_path)
    final_rows = _raw_manifest_runtime_rows(root)
    final_diagnostics = env["lifecycle"].diagnostics()
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
        "http://",
        "https://",
    ):
        assert forbidden not in serialized

    replay = consumer.run_once(
        _RAW_MANIFEST_NATIVE_WORKLOAD,
        owner_id="raw-manifest-native-replay",
        lease_seconds=120.0,
    )
    assert replay.status == "idle"
    assert _raw_manifest_native_call_count(call_log) == (
        calls_at_crash + recovery_calls
    )
    assert runner.output_store.load(str(outbox["event_id"])) == output_after
    assert _raw_manifest_runtime_rows(root) == final_rows
    assert _raw_manifest_sqlite_logical_sha256(output_path) == output_hash
    assert _raw_manifest_file_snapshot(root / "source-raw") == raw_files
    assert env["raw"].stats() == raw_stats
    assert env["raw"].integrity_check()["healthy"] is True
