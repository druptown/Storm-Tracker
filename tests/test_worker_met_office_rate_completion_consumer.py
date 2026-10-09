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
from unittest.mock import patch

import pytest

import storm_tracker_worker.met_office_rate_completion_consumer as metoffice_completion
from storm_tracker_worker.acquisition_evidence_lifecycle import AcquisitionEvidenceConflict
from storm_tracker_worker import met_office_native as native
from storm_tracker_worker.hard_decoder_isolation import (
    DecoderProcessResult,
    hard_boundary_support,
)
from storm_tracker_worker.met_office_rate_completion_consumer import (
    CONSUMER_ID,
    PROCESS_EFFECT_CONTRACT,
    PROCESS_TARGET,
    STREAM_ID,
    WORKLOAD_ID,
    MetOfficeRateCompletionCancelled,
    MetOfficeRateCompletionCapacityError,
    MetOfficeRateCompletionConflict,
    MetOfficeRateCompletionConsumer,
    MetOfficeRateCompletionDeadlineError,
    MetOfficeRateCompletionIntegrityError,
    MetOfficeRateCompletionIsolatedRunner,
    MetOfficeRateCompletionOutputLimits,
    MetOfficeRateCompletionOutputStore,
    MetOfficeRateCompletionProcessLimits,
    _digest,
    _met_office_rate_completion_child,
    _load_process_authority,
    validate_met_office_rate_completion_lease,
)
from storm_tracker_worker.met_office_rate_quantitative import (
    CAPTURE_PRODUCT,
    HDF5_PRODUCT,
    LISTING_PRODUCT,
    MetOfficeRateDerivedArtifactStore,
    MetOfficeRateRawAuthority,
    open_met_office_rate_evidence_store,
)
from storm_tracker_worker.raw_artifacts import RawArtifactStore

from test_worker_terminal_raw_completion_router import (
    NOW,
    _environment,
    _reopen_environment,
    _router,
)
from test_worker_met_office_rate_quantitative import (
    CURRENT_PREFIX,
    ETAG,
    KEY,
    LAST_MODIFIED,
    PREVIOUS_PREFIX,
    PRODUCT_TIME,
    RECEIVED_AT,
    _listing,
    _odim,
    _response,
)


REPOSITORY = Path(__file__).resolve().parents[1]


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
        result = _met_office_rate_completion_child(request)
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


class DdlDeleteRace:
    def __init__(self, store) -> None:
        self.path = store.path
        self.hit = False
        trigger = store._trigger_definitions()["completion_outputs_no_delete"]
        self.script = (
            "DROP TRIGGER completion_outputs_no_delete;"
            "DELETE FROM completion_outputs;"
            f"{trigger};"
        )

    def __call__(self, point: str) -> None:
        if point != "consumer.before_lifecycle_ack" or self.hit:
            return
        self.hit = True
        with closing(sqlite3.connect(self.path, timeout=0.01, isolation_level=None)) as database:
            database.execute("PRAGMA busy_timeout=1")
            database.executescript(self.script)


def _downgrade_output_store_to_v1(path: Path) -> None:
    with closing(sqlite3.connect(path)) as database:
        database.execute("BEGIN IMMEDIATE")
        for trigger in MetOfficeRateCompletionOutputStore._trigger_definitions():
            database.execute(f"DROP TRIGGER {trigger}")
        database.execute(
            "UPDATE output_metadata SET contract=?,schema_version=? WHERE singleton=1",
            (
                metoffice_completion.LEGACY_OUTPUT_STORE_CONTRACT,
                metoffice_completion.LEGACY_SCHEMA_VERSION,
            ),
        )
        database.execute(
            "ALTER TABLE output_metadata DROP COLUMN store_instance_sha256"
        )
        for statement in (
            MetOfficeRateCompletionOutputStore._legacy_trigger_definitions().values()
        ):
            database.execute(statement)
        database.commit()


class MutableClock:
    def __init__(self, value: float) -> None:
        self.value = value

    def __call__(self) -> float:
        return self.value


def _exact_environment(
    tmp_path,
    *,
    valid_capture=False,
    provenance_tamper_role=None,
):
    hdf5 = _odim() if valid_capture else b"not-an-odim-hdf5"
    selected = native.MetOfficeObject(
        key=KEY,
        product_timestamp=PRODUCT_TIME,
        last_modified=LAST_MODIFIED,
        etag=ETAG,
        size_bytes=len(hdf5),
        storage_class="STANDARD",
        checksum_algorithm="SHA256",
        checksum_type="FULL_OBJECT",
    )
    previous = native.MetOfficeObject(
        key=(
            "radar/2026/08/01/"
            "202608012345_ODIM_ng_radar_rainrate_composite_1km_UK.h5"
        ),
        product_timestamp=PRODUCT_TIME - 5.5 * 60 * 60,
        last_modified=PRODUCT_TIME - 5.5 * 60 * 60 + 300,
        etag='"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"',
        size_bytes=123,
        storage_class="STANDARD",
        checksum_algorithm="SHA256",
        checksum_type="FULL_OBJECT",
    )
    listing_payloads = {
        "listing_current": _listing(CURRENT_PREFIX, (selected,)),
        "listing_previous": _listing(PREVIOUS_PREFIX, (previous,)),
    }
    original_store = RawArtifactStore.store
    original_capture = RawArtifactStore.commit_capture

    def store(self, *args, **kwargs):
        product = kwargs.get("product")
        fetch_id = kwargs.get("fetch_id", "")
        if product == LISTING_PRODUCT:
            role = (
                "listing_current"
                if fetch_id.endswith(":listing_current")
                else "listing_previous"
            )
            prefix = CURRENT_PREFIX if role == "listing_current" else PREVIOUS_PREFIX
            payload = listing_payloads[role]
            response = _response(
                payload=payload, content_type="application/xml", selected=None
            )
            provenance = native._listing_provenance(role, prefix, response)
            if provenance_tamper_role == role:
                provenance["collector_version"] = "forged-listing-provenance"
            kwargs.update(
                payload=payload,
                content_type=response.content_type,
                received_at=response.received_at,
                upstream_id=(
                    f"s3-list-v2:{native.BUCKET_NAME}:{prefix}:max-"
                    f"{native.LIST_MAX_KEYS}"
                ),
                provenance=provenance,
            )
        elif product == HDF5_PRODUCT:
            response = _response(
                payload=hdf5,
                content_type="binary/octet-stream",
                selected=selected,
            )
            provenance = native._object_provenance(selected, response)
            if provenance_tamper_role == "odim_hdf5":
                provenance["collector_version"] = "forged-object-provenance"
            kwargs.update(
                payload=hdf5,
                content_type=response.content_type,
                received_at=response.received_at,
                upstream_id=(
                    f"s3:{native.BUCKET_NAME}:{selected.key}:etag:{selected.etag}"
                ),
                provenance=provenance,
            )
        return original_store(self, *args, **kwargs)

    def commit_capture(self, *args, **kwargs):
        if kwargs.get("product") == CAPTURE_PRODUCT:
            kwargs["product_timestamp"] = PRODUCT_TIME
            kwargs["upstream_capture_id"] = (
                f"{CAPTURE_PRODUCT}|key={KEY}|etag={ETAG}"
                f"|previous={hashlib.sha256(listing_payloads['listing_previous']).hexdigest()}"
                f"|current={hashlib.sha256(listing_payloads['listing_current']).hexdigest()}"
                f"|hdf5={hashlib.sha256(hdf5).hexdigest()}"
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
    output_limits=None,
    process_limits=None,
    fault=None,
    valid_capture=False,
    provenance_tamper_role=None,
):
    env = _exact_environment(
        tmp_path,
        valid_capture=valid_capture,
        provenance_tamper_role=provenance_tamper_role,
    )
    artifacts = MetOfficeRateDerivedArtifactStore((tmp_path / "derived").resolve())
    raw_authority = MetOfficeRateRawAuthority(env["raw"])
    evidence = open_met_office_rate_evidence_store(
        (tmp_path / "canonical.sqlite3").resolve(),
        raw_authority=raw_authority,
        clock=lambda: NOW + 1.0,
    )
    outputs = MetOfficeRateCompletionOutputStore(
        (tmp_path / "metoffice-completion.sqlite3").resolve(),
        limits=output_limits or MetOfficeRateCompletionOutputLimits(),
        clock=lambda: NOW + 4.0,
    )
    runner = MetOfficeRateCompletionIsolatedRunner(
        raw_store=env["raw"],
        artifact_store=artifacts,
        evidence_store=evidence,
        output_store=outputs,
        limits=process_limits or MetOfficeRateCompletionProcessLimits(
            wall_seconds=5.0,
            cpu_seconds=4.0,
            rss_bytes=1536 * 1024 * 1024,
        ),
    )
    clock = MutableClock(NOW + 3.0)
    consumer = MetOfficeRateCompletionConsumer(
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
        "metoffice-consumer-test",
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

    result = consumer.run_once(owner_id="metoffice-consumer-a", lease_seconds=120.0)

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


@pytest.mark.parametrize(
    "role",
    ("listing_current", "listing_previous", "odim_hdf5"),
)
def test_each_listing_and_object_provenance_role_is_reproved(role, tmp_path):
    env, _runner, consumer, _clock = _setup(
        tmp_path, provenance_tamper_role=role
    )
    lease = _claim(env)

    with pytest.raises(
        MetOfficeRateCompletionIntegrityError,
        match="listing/object provenance",
    ):
        consumer.process_leased(lease)
    assert _terminal(env, lease) is None


def test_final_ack_reproof_detects_raw_odim_tamper_after_output_commit(tmp_path):
    env, runner, consumer, _clock = _setup(tmp_path)
    runner._executor = InlineExecutor()
    reference = None

    def tamper_at_ack(point):
        nonlocal reference
        if point == "consumer.before_lifecycle_ack":
            capture = env["capture"]
            receipt_id = next(
                member.receipt_id
                for member in capture.members
                if member.role == "odim_hdf5"
            )
            receipt, payload = env["raw"].read_receipt_bytes(receipt_id)
            replacement = bytes([payload[0] ^ 1]) + payload[1:]
            Path(receipt.artifact_path).write_bytes(replacement)
            reference = receipt_id

    consumer._fault_injector = tamper_at_ack
    lease = _claim(env)
    with pytest.raises(
        MetOfficeRateCompletionIntegrityError,
        match="raw authority failed reproof",
    ):
        consumer.process_leased(lease)
    assert reference is not None
    assert runner.output_store.diagnostics()["receipts"] == 1
    assert _terminal(env, lease) is None


def test_valid_router_capture_reaches_exact_quantitative_decoder_but_stays_factless(
    tmp_path,
):
    env, runner, consumer, _clock = _setup(tmp_path, valid_capture=True)
    runner._executor = InlineExecutor()

    lease = _claim(env, lease_seconds=120.0)
    desired = env["desired"].load_active()
    assert desired is not None
    binding = validate_met_office_rate_completion_lease(
        lease, desired_state=desired, raw_store=env["raw"]
    )
    result = consumer.process_leased(lease)
    assert result.status == "acknowledged"
    output = runner.output_store.load(result.event_id)
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

    object_path = (
        runner.artifact_store.objects_path / f"{output.artifact_sha256}.morq"
    )
    artifact_bytes = object_path.read_bytes()
    object_path.write_bytes(bytes([artifact_bytes[0] ^ 1]) + artifact_bytes[1:])
    with pytest.raises(
        MetOfficeRateCompletionIntegrityError,
        match="derived decoder artifact failed reproof",
    ):
        runner._load_exact(lease, binding)


def test_crash_after_output_restarts_without_second_decode_and_then_acks(tmp_path):
    fault = FaultOnce("consumer.after_output_commit")
    env, runner, consumer, clock = _setup(tmp_path, fault=fault)
    inline = InlineExecutor()
    runner._executor = inline
    with pytest.raises(RuntimeError, match="after_output_commit"):
        consumer.run_once(owner_id="metoffice-crash-a", lease_seconds=30.0)
    assert inline.calls == 1
    durable = runner.output_store.diagnostics()
    assert durable["receipts"] == 1

    clock.value = NOW + 34.0
    restarted = MetOfficeRateCompletionConsumer(
        lifecycle_store=env["lifecycle"],
        desired_state_store=env["desired"],
        runner=runner,
        clock=clock,
    )
    result = restarted.run_once(owner_id="metoffice-crash-b", lease_seconds=30.0)
    assert result.status == "acknowledged"
    assert inline.calls == 1
    assert runner.output_store.diagnostics()["receipts"] == 1


def test_output_store_v1_migrates_atomically_and_pending_output_recovers(tmp_path):
    env, runner, consumer, clock = _setup(
        tmp_path, fault=FaultOnce("consumer.after_output_commit")
    )
    inline = InlineExecutor()
    runner._executor = inline
    lease = _claim(env)
    with pytest.raises(RuntimeError, match="after_output_commit"):
        consumer.process_leased(lease)
    expected = runner.output_store.load(lease.event_id)
    assert expected is not None and inline.calls == 1
    path = runner.output_store.path
    _downgrade_output_store_to_v1(path)

    migrated = MetOfficeRateCompletionOutputStore(
        path, limits=runner.output_store.limits, clock=lambda: NOW + 4.0
    )
    loaded = migrated.load(lease.event_id)
    assert loaded is not None and loaded.output_sha256 == expected.output_sha256
    identity = migrated.identity_sha256
    assert len(migrated.store_instance_sha256) == 64
    assert MetOfficeRateCompletionOutputStore(
        path, limits=runner.output_store.limits, clock=lambda: NOW + 4.0
    ).identity_sha256 == identity

    recovered_runner = MetOfficeRateCompletionIsolatedRunner(
        raw_store=runner.raw_store,
        artifact_store=runner.artifact_store,
        evidence_store=runner.evidence_store,
        output_store=migrated,
        limits=runner.limits,
    )
    no_spawn = TerminalExecutor("decoder_failed")
    recovered_runner._executor = no_spawn
    recovered = MetOfficeRateCompletionConsumer(
        lifecycle_store=env["lifecycle"],
        desired_state_store=env["desired"],
        runner=recovered_runner,
        clock=clock,
    ).process_leased(lease)
    assert recovered.status == "acknowledged" and no_spawn.calls == 0
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
        metoffice_completion.OUTPUT_STORE_CONTRACT,
        metoffice_completion.SCHEMA_VERSION,
        migrated.store_instance_sha256,
    )
    assert triggers == set(migrated._trigger_definitions())


def test_output_store_v1_migration_faults_are_exact_old_or_new(tmp_path):
    rollback_path = (tmp_path / "migration-rollback.sqlite3").resolve()
    MetOfficeRateCompletionOutputStore(rollback_path)
    _downgrade_output_store_to_v1(rollback_path)
    with pytest.raises(RuntimeError, match="migration.after_metadata"):
        MetOfficeRateCompletionOutputStore(
            rollback_path,
            fault_injector=FaultOnce("migration.after_metadata"),
        )
    with closing(sqlite3.connect(rollback_path)) as database:
        metadata = database.execute(
            "SELECT contract,schema_version FROM output_metadata WHERE singleton=1"
        ).fetchone()
        columns = {
            str(row[1]) for row in database.execute("PRAGMA table_info(output_metadata)")
        }
        triggers = {
            str(row[0])
            for row in database.execute(
                "SELECT name FROM sqlite_master WHERE type='trigger'"
            )
        }
    assert metadata == (
        metoffice_completion.LEGACY_OUTPUT_STORE_CONTRACT,
        metoffice_completion.LEGACY_SCHEMA_VERSION,
    )
    assert "store_instance_sha256" not in columns
    assert triggers == set(MetOfficeRateCompletionOutputStore._legacy_trigger_definitions())
    assert len(MetOfficeRateCompletionOutputStore(rollback_path).store_instance_sha256) == 64

    committed_path = (tmp_path / "migration-committed.sqlite3").resolve()
    MetOfficeRateCompletionOutputStore(committed_path)
    _downgrade_output_store_to_v1(committed_path)
    with pytest.raises(RuntimeError, match="migration.after_commit"):
        MetOfficeRateCompletionOutputStore(
            committed_path,
            fault_injector=FaultOnce("migration.after_commit"),
        )
    assert len(MetOfficeRateCompletionOutputStore(committed_path).store_instance_sha256) == 64


def test_output_store_sentinel_tamper_and_same_path_replacement_fail_closed(tmp_path):
    path = (tmp_path / "identity.sqlite3").resolve()
    store = MetOfficeRateCompletionOutputStore(path)
    instance_sha = store.store_instance_sha256
    with closing(sqlite3.connect(path)) as database:
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            database.execute(
                "UPDATE output_metadata SET store_instance_sha256=? WHERE singleton=1",
                ("f" * 64,),
            )
        database.rollback()
        database.execute("BEGIN IMMEDIATE")
        database.execute("DROP TRIGGER output_metadata_no_update")
        database.execute(
            "UPDATE output_metadata SET store_instance_sha256=? WHERE singleton=1",
            ("f" * 64,),
        )
        database.execute(store._trigger_definitions()["output_metadata_no_update"])
        database.commit()
    assert instance_sha != "f" * 64
    with pytest.raises(MetOfficeRateCompletionIntegrityError, match="instance authority"):
        store.assert_instance_held()

    visible_path = (tmp_path / "visible.sqlite3").resolve()
    visible = MetOfficeRateCompletionOutputStore(visible_path)
    visible_identity = visible.identity_sha256
    replacement = MetOfficeRateCompletionOutputStore(
        (tmp_path / "replacement.sqlite3").resolve()
    )
    replacement_instance = replacement.store_instance_sha256
    os.replace(replacement.path, visible_path)
    with pytest.raises(MetOfficeRateCompletionIntegrityError, match="instance changed"):
        visible.assert_instance_held()
    reopened = MetOfficeRateCompletionOutputStore(visible_path)
    assert reopened.store_instance_sha256 == replacement_instance
    assert reopened.identity_sha256 != visible_identity


def test_completion_reproof_lease_blocks_ddl_delete_race_and_exact_retry_acks(tmp_path):
    env, runner, consumer, _clock = _setup(tmp_path)
    inline = InlineExecutor()
    runner._executor = inline
    lease = _claim(env)
    attack = DdlDeleteRace(runner.output_store)
    consumer._fault_injector = attack
    with pytest.raises(sqlite3.OperationalError, match="locked"):
        consumer.process_leased(lease)
    assert attack.hit and _terminal(env, lease) is None
    assert runner.output_store.diagnostics()["receipts"] == 1
    with closing(sqlite3.connect(runner.output_store.path)) as database:
        assert database.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='trigger' "
            "AND name='completion_outputs_no_delete'"
        ).fetchone()[0] == 1
        assert database.execute("SELECT count(*) FROM completion_outputs").fetchone()[0] == 1
    consumer._fault_injector = None
    recovered = consumer.process_leased(lease)
    assert recovered.status == "acknowledged"
    assert _terminal(env, lease) is not None
    assert inline.calls == 1


def test_recovery_rejects_same_inode_sentinel_tamper_and_path_replacement(tmp_path):
    for mode in ("sentinel", "replacement"):
        case_root = tmp_path / mode
        env, runner, consumer, _clock = _setup(
            case_root, fault=FaultOnce("consumer.after_output_commit")
        )
        runner._executor = InlineExecutor()
        lease = _claim(env)
        with pytest.raises(RuntimeError, match="after_output_commit"):
            consumer.process_leased(lease)
        consumer._fault_injector = None
        if mode == "sentinel":
            with closing(sqlite3.connect(runner.output_store.path)) as database:
                database.execute("BEGIN IMMEDIATE")
                database.execute("DROP TRIGGER output_metadata_no_update")
                database.execute(
                    "UPDATE output_metadata SET store_instance_sha256=? WHERE singleton=1",
                    ("e" * 64,),
                )
                database.execute(
                    runner.output_store._trigger_definitions()["output_metadata_no_update"]
                )
                database.commit()
            match = "instance authority"
        else:
            replacement = MetOfficeRateCompletionOutputStore(
                (case_root / "replacement.sqlite3").resolve(),
                limits=runner.output_store.limits,
            )
            os.replace(replacement.path, runner.output_store.path)
            match = "instance changed"
        with pytest.raises(MetOfficeRateCompletionIntegrityError, match=match):
            consumer.process_leased(lease)
        assert _terminal(env, lease) is None


def test_crash_after_lifecycle_ack_is_terminal_and_does_not_decode_again(tmp_path):
    env, runner, consumer, _clock = _setup(
        tmp_path, fault=FaultOnce("consumer.after_lifecycle_ack")
    )
    inline = InlineExecutor()
    runner._executor = inline
    with pytest.raises(RuntimeError, match="after_lifecycle_ack"):
        consumer.run_once(owner_id="metoffice-ack-crash", lease_seconds=120.0)
    assert env["lifecycle"].load_terminal_consumption(STREAM_ID, 1) is not None
    assert runner.output_store.diagnostics()["receipts"] == 1
    consumer._fault_injector = None
    assert consumer.run_once(owner_id="metoffice-ack-restart", lease_seconds=120.0).status == "idle"
    assert inline.calls == 1


def test_deadline_and_cancellation_never_ack(tmp_path):
    env, runner, consumer, _clock = _setup(tmp_path)
    runner._executor = TerminalExecutor("deadline_exceeded")
    lease = _claim(env)
    result = consumer.process_leased(lease)
    assert result.status == "deadline"
    assert _terminal(env, lease) is None

    event = threading.Event()
    event.set()
    with pytest.raises(MetOfficeRateCompletionCancelled):
        consumer.process_leased(lease, cancel_event=event)
    assert _terminal(env, lease) is None

    with pytest.raises(MetOfficeRateCompletionDeadlineError):
        consumer.run_once(owner_id="too-short", lease_seconds=1.0)


def test_output_capacity_fails_closed_without_ack(tmp_path):
    env, runner, consumer, _clock = _setup(
        tmp_path,
        output_limits=MetOfficeRateCompletionOutputLimits(
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
    binding = validate_met_office_rate_completion_lease(
        lease, desired_state=state, raw_store=env["raw"]
    )
    assert binding.capture_id

    tampered_payload = json.loads(lease.payload)
    tampered_payload["capture_reference"]["manifest_sha256"] = "0" * 64
    with pytest.raises(MetOfficeRateCompletionIntegrityError):
        validate_met_office_rate_completion_lease(
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
    with pytest.raises(MetOfficeRateCompletionIntegrityError):
        runner.output_store.load(lease.event_id)


def test_raw_byte_tamper_and_proof_authority_tamper_fail_closed(tmp_path):
    env, _runner, _consumer, _clock = _setup(tmp_path)
    lease = _claim(env)
    state = env["desired"].load_active()
    assert state is not None

    changed_proof = json.loads(json.dumps(lease.proof))
    changed_proof["workload"]["job_sha256"] = "0" * 64
    with pytest.raises(MetOfficeRateCompletionIntegrityError):
        validate_met_office_rate_completion_lease(
            replace(lease, proof=changed_proof),
            desired_state=state,
            raw_store=env["raw"],
        )

    reference = json.loads(lease.payload)
    receipt_id = reference["capture_reference"]["members"][0]["receipt_id"]
    receipt, payload = env["raw"].read_receipt_bytes(receipt_id)
    replacement = bytes([payload[0] ^ 1]) + payload[1:]
    Path(receipt.artifact_path).write_bytes(replacement)
    with pytest.raises(MetOfficeRateCompletionIntegrityError):
        validate_met_office_rate_completion_lease(
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
    with pytest.raises(MetOfficeRateCompletionCancelled):
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
    with pytest.raises(MetOfficeRateCompletionConflict, match="ack boundary"):
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


def test_hard_request_cap_rejects_before_spawn_and_never_acks(tmp_path):
    env, runner, consumer, _clock = _setup(
        tmp_path,
        process_limits=MetOfficeRateCompletionProcessLimits(
            wall_seconds=5.0,
            cpu_seconds=4.0,
            rss_bytes=1536 * 1024 * 1024,
            max_request_bytes=1024,
        ),
    )
    lease = _claim(env)

    result = consumer.process_leased(lease)

    assert result.status == "contract_failed"
    assert result.reason_code == "hard_process_protocol_failed"
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

    forged_store = MetOfficeRateCompletionOutputStore(
        (tmp_path / "forged-output.sqlite3").resolve(), clock=lambda: NOW + 4.0
    )
    forged_runner = MetOfficeRateCompletionIsolatedRunner(
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
        "handle": f"/private/metoffice/{fake_sha}",
        "sha256": fake_sha,
        "size_bytes": 1,
    }
    forged["decoder"]["outcome"] = "decoded_complete"
    forged["decoder"]["reason_code"] = "decoded_complete"
    forged["disposition"] = "shadow_candidate"
    forged["isolation"]["authority_sha256"] = forged_runner.authority_sha256
    with pytest.raises(MetOfficeRateCompletionIntegrityError):
        forged_store.commit(forged)

    forged["decoder"]["artifact"]["handle"] = (
        f"artifact:met-office-rate-quantitative:{fake_sha}"
    )
    forged_store.commit(forged)
    forged_consumer = MetOfficeRateCompletionConsumer(
        lifecycle_store=env["lifecycle"],
        desired_state_store=env["desired"],
        runner=forged_runner,
        clock=lambda: NOW + 3.0,
    )
    with pytest.raises(
        MetOfficeRateCompletionIntegrityError,
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
        MetOfficeRateCompletionIntegrityError,
        match="database paths alias",
    ):
        _load_process_authority(authority, _digest(authority))


def test_preinflated_output_database_is_rejected_above_hard_page_cap(tmp_path):
    limits = MetOfficeRateCompletionOutputLimits(max_database_bytes=1024 * 1024)
    path = (tmp_path / "bounded-output.sqlite3").resolve()
    MetOfficeRateCompletionOutputStore(path, limits=limits)
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
        MetOfficeRateCompletionCapacityError,
        match="hard page cap",
    ):
        MetOfficeRateCompletionOutputStore(path, limits=limits)


def test_diagnostics_and_child_result_are_path_payload_secret_free(tmp_path):
    env, runner, consumer, _clock = _setup(tmp_path)
    runner._executor = InlineExecutor()
    result = consumer.run_once(owner_id="metoffice-privacy", lease_seconds=120.0)
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
        process_limits=MetOfficeRateCompletionProcessLimits(
            wall_seconds=20.0,
            cpu_seconds=15.0,
            rss_bytes=1536 * 1024 * 1024,
        ),
    )
    result = consumer.run_once(owner_id="metoffice-real-spawn", lease_seconds=60.0)
    assert result.status == "acknowledged"
    assert runner.output_store.load(result.event_id) is not None


def test_consumer_remains_absent_from_live_service_ha_and_mqtt_wiring():
    forbidden = tuple(
        path
        for path in (REPOSITORY / "storm_tracker_worker").rglob("*.py")
        if path.name
            not in {
                "met_office_rate_completion_consumer.py",
                "completion_output_archive_adapters.py",
                "completion_route_adapters.py",
                # Static release-archive inventory, not executable provider,
                # service, MQTT or Home Assistant wiring.
                "raw_capture_release_requirements.py",
            }
    )
    forbidden += tuple((REPOSITORY / "custom_components").rglob("*.py"))
    needle = "met_office_rate_completion_consumer"
    assert all(needle not in path.read_text(encoding="utf-8") for path in forbidden)


COMPLETION_CONSUMERS_CRASH_CHILD = (
    Path(__file__).resolve().parent
    / "helpers"
    / "completion_consumers_os_exit73_child.py"
)
_MET_OFFICE_MIGRATION_CRASH_POINTS = (
    "migration.after_metadata",
    "migration.after_triggers",
    "migration.before_commit",
    "migration.after_commit",
)
_MET_OFFICE_OUTPUT_CRASH_POINTS = (
    "output.before_commit",
    "output.after_commit",
)
_MET_OFFICE_CONSUMER_CRASH_POINTS = (
    "consumer.after_output_commit",
    "consumer.before_lifecycle_ack",
    "consumer.after_lifecycle_ack",
)
_MET_OFFICE_ALL_CRASH_POINTS = (
    *_MET_OFFICE_MIGRATION_CRASH_POINTS,
    *_MET_OFFICE_OUTPUT_CRASH_POINTS,
    *_MET_OFFICE_CONSUMER_CRASH_POINTS,
)
_MET_OFFICE_CRASH_CASES = (
    *(("migration", point) for point in _MET_OFFICE_MIGRATION_CRASH_POINTS),
    *(("output", point) for point in _MET_OFFICE_OUTPUT_CRASH_POINTS),
    *(("consumer", point) for point in _MET_OFFICE_CONSUMER_CRASH_POINTS),
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

        original = MetOfficeRateCompletionOutputStore.__init__

        def initialize_with_fault(instance, *args, **init_kwargs):
            assert "fault_injector" not in init_kwargs
            init_kwargs["fault_injector"] = self.output_fault
            original(instance, *args, **init_kwargs)

        with patch.object(
            MetOfficeRateCompletionOutputStore,
            "__init__",
            initialize_with_fault,
        ):
            return super().execute(**kwargs)


def _reopen_met_office_native_crash_case(
    root: Path,
    *,
    executor: NativeCrashRecordingInlineExecutor,
    consumer_fault=None,
):
    env = _reopen_environment(root, WORKLOAD_ID)
    artifacts = MetOfficeRateDerivedArtifactStore((root / "derived").resolve())
    raw_authority = MetOfficeRateRawAuthority(env["raw"])
    evidence = open_met_office_rate_evidence_store(
        (root / "canonical.sqlite3").resolve(),
        raw_authority=raw_authority,
        clock=lambda: NOW + 1.0,
    )
    outputs = MetOfficeRateCompletionOutputStore(
        (root / "metoffice-completion.sqlite3").resolve(),
        clock=lambda: NOW + 4.0,
    )
    runner = MetOfficeRateCompletionIsolatedRunner(
        raw_store=env["raw"],
        artifact_store=artifacts,
        evidence_store=evidence,
        output_store=outputs,
        limits=MetOfficeRateCompletionProcessLimits(
            wall_seconds=5.0,
            cpu_seconds=4.0,
            rss_bytes=1536 * 1024 * 1024,
        ),
    )
    runner._executor = executor
    consumer = MetOfficeRateCompletionConsumer(
        lifecycle_store=env["lifecycle"],
        desired_state_store=env["desired"],
        runner=runner,
        clock=lambda: NOW + 3.0,
        fault_injector=consumer_fault,
    )
    return env, runner, consumer


def run_met_office_rate_completion_crash_child(
    root: Path,
    phase: str,
    checkpoint: str,
    event_id: str | None,
    lease_token: str | None,
    crash,
) -> None:
    expected = {
        "migration": set(_MET_OFFICE_MIGRATION_CRASH_POINTS),
        "output": set(_MET_OFFICE_OUTPUT_CRASH_POINTS),
        "consumer": set(_MET_OFFICE_CONSUMER_CRASH_POINTS),
    }
    assert checkpoint in expected[phase]
    if phase == "migration":
        assert event_id is None and lease_token is None
        MetOfficeRateCompletionOutputStore(
            (root / "metoffice-completion.sqlite3").resolve(),
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
    env, _runner, consumer = _reopen_met_office_native_crash_case(
        root,
        executor=executor,
        consumer_fault=crash if phase == "consumer" else None,
    )
    lease = env["lifecycle"].load_leased(event_id, lease_token)
    assert lease is not None
    consumer.process_leased(lease)


def _run_met_office_native_crash(
    root: Path,
    phase: str,
    checkpoint: str,
    lease=None,
) -> subprocess.CompletedProcess[bytes]:
    arguments = [
        sys.executable,
        str(COMPLETION_CONSUMERS_CRASH_CHILD),
        "met-office-rate",
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


def _met_office_fault_calls() -> Counter[str]:
    source = (
        REPOSITORY
        / "storm_tracker_worker"
        / "met_office_rate_completion_consumer.py"
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


def _met_office_migration_state(path: Path) -> dict[str, object]:
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


def _met_office_sqlite_logical_sha256(path: Path) -> str:
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


def _met_office_capture_authority_snapshot(raw_store, capture_id: str):
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


def _met_office_file_snapshot(root: Path):
    return tuple(
        (
            path.relative_to(root).as_posix(),
            path.stat().st_size,
            hashlib.sha256(path.read_bytes()).hexdigest(),
        )
        for path in sorted(item for item in root.rglob("*") if item.is_file())
    )


def _met_office_assert_journal(path: Path, *, expected: bool) -> None:
    journal = Path(str(path) + "-journal")
    assert journal.exists() is expected
    if expected:
        assert journal.is_file()
        assert journal.stat().st_size > 0
    assert not Path(str(path) + "-wal").exists()
    assert not Path(str(path) + "-shm").exists()


def _met_office_call_count(path: Path) -> int:
    return 0 if not path.exists() else len(path.read_bytes().splitlines())


def test_native_crash_matrix_pins_every_met_office_production_fault_occurrence():
    expected = Counter({point: 1 for point in _MET_OFFICE_ALL_CRASH_POINTS})
    assert len(_MET_OFFICE_ALL_CRASH_POINTS) == 9
    assert len(set(_MET_OFFICE_ALL_CRASH_POINTS)) == 9
    assert Counter(point for _phase, point in _MET_OFFICE_CRASH_CASES) == expected
    assert _met_office_fault_calls() == expected


@pytest.mark.parametrize(
    ("phase", "checkpoint"),
    _MET_OFFICE_CRASH_CASES,
    ids=[f"{phase}-{point}" for phase, point in _MET_OFFICE_CRASH_CASES],
)
def test_native_os_exit73_met_office_fault_recovers_exactly_once_without_growth(
    phase,
    checkpoint,
    tmp_path,
):
    root = (tmp_path / "met-office-native-crash").resolve()
    root.mkdir(parents=True)
    output_path = (root / "metoffice-completion.sqlite3").resolve()

    if phase == "migration":
        MetOfficeRateCompletionOutputStore(
            output_path, clock=lambda: NOW + 4.0
        )
        _downgrade_output_store_to_v1(output_path)
        legacy = _met_office_migration_state(output_path)
        assert legacy["tables"] == {"output_metadata", "completion_outputs"}
        assert legacy["columns"] == {
            column[0]
            for column in MetOfficeRateCompletionOutputStore._metadata_columns(
                legacy=True
            )
        }
        assert legacy["metadata"]["contract"] == (
            metoffice_completion.LEGACY_OUTPUT_STORE_CONTRACT
        )
        assert legacy["metadata"]["schema_version"] == (
            metoffice_completion.LEGACY_SCHEMA_VERSION
        )
        assert legacy["receipts"] == 0
        assert legacy["triggers"] == set(
            MetOfficeRateCompletionOutputStore._legacy_trigger_definitions()
        )

        crashed = _run_met_office_native_crash(root, phase, checkpoint)
        assert crashed.returncode == 73
        assert crashed.stdout == b""
        assert crashed.stderr == b""
        rolled_back = checkpoint != "migration.after_commit"
        _met_office_assert_journal(output_path, expected=rolled_back)
        crash_state = _met_office_migration_state(output_path)
        if rolled_back:
            assert crash_state == legacy
        else:
            assert crash_state["columns"] == {
                column[0]
                for column in MetOfficeRateCompletionOutputStore._metadata_columns(
                    legacy=False
                )
            }
            assert crash_state["metadata"]["contract"] == (
                metoffice_completion.OUTPUT_STORE_CONTRACT
            )
            assert crash_state["metadata"]["schema_version"] == (
                metoffice_completion.SCHEMA_VERSION
            )
            assert len(crash_state["metadata"]["store_instance_sha256"]) == 64
            assert crash_state["receipts"] == 0
            assert crash_state["triggers"] == set(
                MetOfficeRateCompletionOutputStore._trigger_definitions()
            )

        recovered = MetOfficeRateCompletionOutputStore(
            output_path, clock=lambda: NOW + 4.0
        )
        current = _met_office_migration_state(output_path)
        assert current["tables"] == {"output_metadata", "completion_outputs"}
        assert current["columns"] == {
            column[0]
            for column in MetOfficeRateCompletionOutputStore._metadata_columns(
                legacy=False
            )
        }
        assert current["metadata"]["contract"] == (
            metoffice_completion.OUTPUT_STORE_CONTRACT
        )
        assert current["metadata"]["schema_version"] == (
            metoffice_completion.SCHEMA_VERSION
        )
        assert current["metadata"]["store_instance_sha256"] == (
            recovered.store_instance_sha256
        )
        assert len(recovered.store_instance_sha256) == 64
        assert current["triggers"] == set(recovered._trigger_definitions())
        assert recovered.diagnostics() == {
            "contract": metoffice_completion.OUTPUT_STORE_CONTRACT,
            "healthy": True,
            "output_bytes": 0,
            "receipts": 0,
            "runtime_wiring": "inert",
        }
        recovered_sha = _met_office_sqlite_logical_sha256(output_path)
        reopened = MetOfficeRateCompletionOutputStore(
            output_path, clock=lambda: NOW + 4.0
        )
        assert reopened.identity_sha256 == recovered.identity_sha256
        assert _met_office_sqlite_logical_sha256(output_path) == recovered_sha
        _met_office_assert_journal(output_path, expected=False)
        return

    env, _runner, _consumer, _clock = _setup(root, valid_capture=True)
    lease = _claim(env)
    raw_before = _met_office_capture_authority_snapshot(
        env["raw"], env["capture"].capture_id
    )
    raw_stats_before = env["raw"].stats()
    call_log = root / "native-crash-decode-calls.log"

    crashed = _run_met_office_native_crash(root, phase, checkpoint, lease)
    assert crashed.returncode == 73
    assert crashed.stdout == b""
    assert crashed.stderr == b""
    _met_office_assert_journal(
        output_path,
        expected=checkpoint == "output.before_commit",
    )
    assert _met_office_call_count(call_log) == 1

    evidence_path = (root / "canonical.sqlite3").resolve()
    evidence_before_sha = _met_office_sqlite_logical_sha256(evidence_path)
    derived_before = _met_office_file_snapshot((root / "derived").resolve())
    executor = NativeCrashRecordingInlineExecutor(call_log)
    reopened_env, runner, consumer = _reopen_met_office_native_crash_case(
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
    assert evidence_stats == {"decoder_runs": 1, "member_links": 3}
    evidence_audit = runner.evidence_store.audit()
    assert evidence_audit == {
        "healthy": True,
        "checked_runs": 1,
        "issues": [],
    }

    if terminal_at_crash:
        recovered = consumer.run_once(
            owner_id="met-office-native-crash-terminal",
            lease_seconds=120.0,
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
    assert _met_office_call_count(call_log) == expected_calls
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
    evidence_document = output_after.output["decoder"]["evidence"]
    assert output_after.evidence_receipt_sha256 == evidence_document[
        "receipt_sha256"
    ]
    assert len(output_after.output_sha256) == 64
    assert len(output_after.receipt_sha256) == 64
    assert len(output_after.process_receipt_sha256) == 64
    assert output_after.artifact_sha256 is not None
    artifact_path = (
        runner.artifact_store.objects_path
        / f"{output_after.artifact_sha256}.morq"
    )
    assert artifact_path.is_file()
    assert hashlib.sha256(artifact_path.read_bytes()).hexdigest() == (
        output_after.artifact_sha256
    )
    assert runner.output_store.diagnostics()["receipts"] == 1
    assert runner.evidence_store.stats() == evidence_stats
    assert _met_office_sqlite_logical_sha256(evidence_path) == evidence_before_sha
    assert _met_office_file_snapshot((root / "derived").resolve()) == derived_before
    assert _met_office_capture_authority_snapshot(
        reopened_env["raw"], env["capture"].capture_id
    ) == raw_before
    assert reopened_env["raw"].stats() == raw_stats_before
    raw_audit = reopened_env["raw"].integrity_check()
    assert raw_audit["healthy"] is True
    assert raw_audit["issues"] == []

    output_sha = _met_office_sqlite_logical_sha256(output_path)
    evidence_sha = _met_office_sqlite_logical_sha256(evidence_path)
    derived_sha = _met_office_file_snapshot((root / "derived").resolve())
    final_lifecycle = reopened_env["lifecycle"].diagnostics()
    assert sum(final_lifecycle["outbox_states"].values()) == 1
    assert final_lifecycle["outbox_states"]["consumed"] == 1
    replay = consumer.run_once(
        owner_id="met-office-native-crash-replay",
        lease_seconds=120.0,
    )
    assert replay.status == "idle"
    assert _met_office_call_count(call_log) == expected_calls
    assert runner.output_store.load(lease.event_id) == output_after
    assert _met_office_sqlite_logical_sha256(output_path) == output_sha
    assert _met_office_sqlite_logical_sha256(evidence_path) == evidence_sha
    assert _met_office_file_snapshot((root / "derived").resolve()) == derived_sha
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
