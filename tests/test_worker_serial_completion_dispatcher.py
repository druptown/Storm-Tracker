from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import threading
from types import SimpleNamespace

import pytest

from storm_tracker_worker import serial_completion_dispatcher as dispatcher_subject
from storm_tracker_worker.acquisition_evidence_lifecycle import (
    AcquisitionEvidenceLifecycleStore,
    CompletionLease,
)
from storm_tracker_worker.completion_route_adapters import (
    AdapterContinuation,
    AdapterPhaseResult,
    DEFAULT_ROUTE_LEASE_SECONDS,
    DwdCompletionRouteAdapter,
    NoaaCompletionRouteAdapter,
    PermanentEventFailure,
    ROUTE_ORDER,
    RawManifestCompletionRouteAdapter,
    _SinglePhaseAdapter,
)
from storm_tracker_worker.desired_state import DesiredStateStore
from storm_tracker_worker.serial_completion_dispatcher import (
    MAX_DATABASE_BYTES,
    MAX_HEAVY_CHILDREN_PER_TICK,
    MAX_IDLE_CLAIMS_PER_TICK,
    PREWIRING_BLOCKERS,
    GuardedCompletionClock,
    SerialCompletionDispatcher,
    SerialCompletionDispatcherClockRollback,
    SerialCompletionDispatcherError,
    SerialCompletionDispatcherIntegrityError,
    SerialCompletionDispatcherOwnerBusy,
)
from storm_tracker_worker.terminal_raw_completion_router import (
    TERMINAL_RAW_CONSUMER_CATALOG,
)


SYNC = lambda _path: None
MIGRATION_HARD_EXIT_HELPER = (
    Path(__file__).resolve().parent
    / "helpers"
    / "worker_serial_completion_dispatcher_migration_os_exit73_child.py"
)


class MutableClock:
    def __init__(self, value: float) -> None:
        self.value = value

    def __call__(self) -> float:
        return self.value


class SequenceClock:
    def __init__(self, first: float) -> None:
        self.next_value = first
        self.values = []

    def __call__(self) -> float:
        value = self.next_value
        self.next_value += 1.0
        self.values.append(value)
        return value


class FaultOnce:
    def __init__(self, point: str) -> None:
        self.point = point
        self.hit = False

    def __call__(self, point: str) -> None:
        if point == self.point and not self.hit:
            self.hit = True
            raise RuntimeError("injected_dispatcher_fault")


class CrashOnce:
    class Crash(BaseException):
        pass

    def __init__(self, point: str) -> None:
        self.point = point
        self.hit = False

    def __call__(self, point: str) -> None:
        if point == self.point and not self.hit:
            self.hit = True
            raise self.Crash("simulated_process_crash")


def _sha(label: str) -> str:
    return hashlib.sha256(label.encode("ascii")).hexdigest()


def _digest(value) -> str:
    return hashlib.sha256(json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")).hexdigest()


_SINGLE_PHASE_TEST_WORKLOAD_ID = "ground.metar.global"


def _next_route_index(workload_id: str) -> int:
    return (ROUTE_ORDER.index(workload_id) + 1) % len(ROUTE_ORDER)


def _claim_matches(
    workload_id: str, consumer_id: str, stream_id: str
) -> bool:
    binding = TERMINAL_RAW_CONSUMER_CATALOG[workload_id]
    return (consumer_id, stream_id) == (
        binding.consumer_id,
        binding.stream_id,
    )


def _lifecycle(tmp_path: Path, clock: MutableClock):
    return AcquisitionEvidenceLifecycleStore(
        (tmp_path / "lifecycle").resolve(),
        clock=clock,
        directory_fsync=SYNC,
    )


def _desired(tmp_path: Path) -> DesiredStateStore:
    store = DesiredStateStore((tmp_path / "desired.sqlite3").resolve())
    assert store.load_active() is None
    return store


def _adapter_shells(lifecycle, guarded_clock, desired):
    adapters = {}
    for workload_id in ROUTE_ORDER:
        binding = TERMINAL_RAW_CONSUMER_CATALOG[workload_id]
        if workload_id == "radar.dwd.radolan_rv":
            cls = DwdCompletionRouteAdapter
        elif workload_id == "satellite.noaa.err":
            cls = NoaaCompletionRouteAdapter
        elif binding.consumer_kind == "raw_manifest_audit":
            cls = RawManifestCompletionRouteAdapter
        else:
            cls = _SinglePhaseAdapter
        adapter = object.__new__(cls)
        adapter.workload_id = workload_id
        adapter.consumer_id = binding.consumer_id
        adapter.stream_id = binding.stream_id
        adapter.lease_seconds = DEFAULT_ROUTE_LEASE_SECONDS
        adapter.fingerprint = _sha("adapter:" + workload_id)
        adapter.clock = guarded_clock
        route_store = desired.path.parent / (
            "dispatcher-route-store-" + _sha(workload_id)[:16] + ".bin"
        )
        route_store.touch(exist_ok=True)
        store_info = route_store.lstat()
        adapter.composition_store_bindings = ({
            "directory": False,
            "instance_authority_sha256": _sha(
                "dispatcher-route-store-instance:" + workload_id
            ),
            "path": str(route_store.resolve()),
            "persistent_instance_authority": True,
            "roles": ["test.output_store"],
            "store_class": "DispatcherTestStore",
            "witness": {
                "device": int(store_info.st_dev),
                "inode": int(store_info.st_ino),
                "links": int(store_info.st_nlink),
                "mode": int(store_info.st_mode),
            },
        },)
        adapter.composition_store_bindings_sha256 = _digest(
            adapter.composition_store_bindings
        )
        consumer = SimpleNamespace(
            lifecycle_store=lifecycle,
            lifecycle=lifecycle,
            desired_state_store=desired,
            desired=desired,
            output_store=SimpleNamespace(
                path=route_store,
                store_instance_sha256=_sha(
                    "dispatcher-route-store-value:" + workload_id
                ),
            ),
        )
        adapter._consumer = consumer
        if cls is DwdCompletionRouteAdapter:
            adapter._lifecycle = lifecycle
            adapter._desired = desired
            adapter._output_store_identity_sha256 = _sha(
                "dispatcher-dwd-output-store-identity"
            )
        adapters[workload_id] = adapter
    return adapters


def _lease(workload_id: str, *, token: str, expires_at: float) -> CompletionLease:
    binding = TERMINAL_RAW_CONSUMER_CATALOG[workload_id]
    event_sha = _sha("event:" + workload_id)
    return CompletionLease(
        event_id="completion-" + event_sha,
        event_sha256=event_sha,
        producer_receipt_sha256=_sha("producer:" + workload_id),
        acquisition_job_id="acq:test-dispatcher",
        consumer_id=binding.consumer_id,
        stream_id=binding.stream_id,
        stream_sequence=1,
        desired_state_sha256=_sha("desired"),
        authority_sha256=_sha("authority"),
        content_sha256=_sha("content"),
        size_bytes=19,
        attempt_count=1,
        lease_token=token,
        lease_expires_at=expires_at,
        proof={"public": True},
        payload=b"PRIVATE-DISPATCH-PAYLOAD",
    )


def _dispatcher(tmp_path, lifecycle, guarded, adapters, **kwargs):
    return SerialCompletionDispatcher(
        (tmp_path / "dispatcher.sqlite3").resolve(),
        lifecycle_store=lifecycle,
        adapters=adapters,
        guarded_clock=guarded,
        **kwargs,
    )


def _downgrade_dispatcher_database_to_v1(
    dispatcher,
    *,
    next_route_index: int,
    pending: bool = False,
) -> None:
    with closing(sqlite3.connect(dispatcher.path)) as database:
        database.row_factory = sqlite3.Row
        database.execute("BEGIN EXCLUSIVE")
        state = dispatcher._read_state(database)  # noqa: SLF001
        state.pop("state_sha256")
        state["next_route_index"] = next_route_index
        if pending:
            state.update({
                "pending_event_sha256": _sha("legacy-pending-event"),
                "pending_lease_expires_at": 2_000.0,
                "pending_owner_epoch": state["owner_epoch"],
                "pending_phase": "initial",
                "pending_stream_sequence": 1,
                "pending_workload_id": (
                    dispatcher_subject.LEGACY_ROUTE_ORDER_V1[
                        next_route_index
                    ]
                ),
            })
        database.execute(
            "ALTER TABLE dispatcher_state RENAME TO dispatcher_state_v2"
        )
        database.execute(
            dict(dispatcher_subject._LEGACY_TABLE_DDL_V1)[  # noqa: SLF001
                "dispatcher_state"
            ]
        )
        database.execute(
            "INSERT INTO dispatcher_state VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            dispatcher._state_values(state),  # noqa: SLF001
        )
        database.execute("DROP TABLE dispatcher_state_v2")
        database.execute(
            "UPDATE manifest SET value=? WHERE key='schema_version'",
            ("1",),
        )
        database.execute(
            "UPDATE manifest SET value=? WHERE key='sqlite_schema_sha256'",
            (dispatcher_subject.LEGACY_EXPECTED_SCHEMA_SHA256_V1,),
        )
        database.execute(
            "UPDATE manifest SET value=? WHERE key='route_catalog_sha256'",
            (dispatcher_subject.LEGACY_ROUTE_CATALOG_SHA256_V1,),
        )
        database.commit()


def _dispatcher_sql_snapshot(path: Path) -> tuple[tuple, tuple]:
    with closing(sqlite3.connect(path)) as database:
        schema = tuple(database.execute(
            "SELECT type,name,sql FROM sqlite_master "
            "WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name"
        ))
        rows = tuple(database.execute(
            "SELECT * FROM dispatcher_state ORDER BY singleton"
        ))
        manifest = tuple(database.execute(
            "SELECT key,value FROM manifest ORDER BY key"
        ))
    return schema, rows + manifest


def run_serial_completion_dispatcher_migration_os_exit73_child(
    root: Path,
) -> None:
    wall = MutableClock(1_000.0)
    lifecycle = _lifecycle(root, wall)
    desired = _desired(root)
    guarded = GuardedCompletionClock(wall)
    adapters = _adapter_shells(lifecycle, guarded, desired)

    class ExitAfterRebuild:
        def __call__(self, point: str) -> None:
            if point == "migration.after_rebuild_before_commit":
                os._exit(73)

    _dispatcher(
        root,
        lifecycle,
        guarded,
        adapters,
        fault_injector=ExitAfterRebuild(),
    )
    raise RuntimeError("dispatcher migration hard-exit checkpoint was missed")


@pytest.mark.parametrize(
    "legacy_index",
    range(dispatcher_subject.LEGACY_ROUTE_LIMIT_V1),
)
def test_v1_route_cursor_migration_maps_workload_identity_not_number(
    tmp_path,
    legacy_index,
):
    wall = MutableClock(1_000.0)
    lifecycle = _lifecycle(tmp_path, wall)
    desired = _desired(tmp_path)
    guarded = GuardedCompletionClock(wall)
    adapters = _adapter_shells(lifecycle, guarded, desired)
    original = _dispatcher(tmp_path, lifecycle, guarded, adapters)
    _downgrade_dispatcher_database_to_v1(
        original,
        next_route_index=legacy_index,
    )

    migrated = _dispatcher(tmp_path, lifecycle, guarded, adapters)
    with closing(sqlite3.connect(migrated.path)) as database:
        database.row_factory = sqlite3.Row
        state = migrated._read_state(database)  # noqa: SLF001
        manifest = dict(database.execute("SELECT key,value FROM manifest"))
        schema = dispatcher_subject._schema_inventory(database)  # noqa: SLF001
    legacy_workload = dispatcher_subject.LEGACY_ROUTE_ORDER_V1[legacy_index]
    assert state["next_route_index"] == ROUTE_ORDER.index(legacy_workload)
    assert manifest == migrated._manifest()  # noqa: SLF001
    assert schema == dispatcher_subject.EXPECTED_SCHEMA


@pytest.mark.parametrize("tamper", ("catalog_hash", "state_hash"))
def test_v1_migration_tamper_fails_before_any_state_change(tmp_path, tamper):
    wall = MutableClock(1_000.0)
    lifecycle = _lifecycle(tmp_path, wall)
    desired = _desired(tmp_path)
    guarded = GuardedCompletionClock(wall)
    adapters = _adapter_shells(lifecycle, guarded, desired)
    original = _dispatcher(tmp_path, lifecycle, guarded, adapters)
    _downgrade_dispatcher_database_to_v1(original, next_route_index=3)
    with closing(sqlite3.connect(original.path)) as database:
        if tamper == "catalog_hash":
            database.execute(
                "UPDATE manifest SET value=? WHERE key='route_catalog_sha256'",
                ("f" * 64,),
            )
        else:
            database.execute(
                "UPDATE dispatcher_state SET next_route_index=4 WHERE singleton=1"
            )
        database.commit()
    before = _dispatcher_sql_snapshot(original.path)

    with pytest.raises(SerialCompletionDispatcherIntegrityError):
        _dispatcher(tmp_path, lifecycle, guarded, adapters)

    assert _dispatcher_sql_snapshot(original.path) == before


def test_v1_pending_claim_blocks_migration_before_state_change(tmp_path):
    wall = MutableClock(1_000.0)
    lifecycle = _lifecycle(tmp_path, wall)
    desired = _desired(tmp_path)
    guarded = GuardedCompletionClock(wall)
    adapters = _adapter_shells(lifecycle, guarded, desired)
    original = _dispatcher(tmp_path, lifecycle, guarded, adapters)
    _downgrade_dispatcher_database_to_v1(
        original,
        next_route_index=2,
        pending=True,
    )
    before = _dispatcher_sql_snapshot(original.path)

    with pytest.raises(
        SerialCompletionDispatcherIntegrityError,
        match="pending claim",
    ):
        _dispatcher(tmp_path, lifecycle, guarded, adapters)

    assert _dispatcher_sql_snapshot(original.path) == before


def test_native_os_exit73_v1_migration_rolls_back_then_replays(tmp_path):
    wall = MutableClock(1_000.0)
    lifecycle = _lifecycle(tmp_path, wall)
    desired = _desired(tmp_path)
    guarded = GuardedCompletionClock(wall)
    adapters = _adapter_shells(lifecycle, guarded, desired)
    original = _dispatcher(tmp_path, lifecycle, guarded, adapters)
    legacy_index = 10
    _downgrade_dispatcher_database_to_v1(
        original,
        next_route_index=legacy_index,
    )
    environment = os.environ.copy()
    environment["PYTHONPATH"] = os.pathsep.join(
        value
        for value in (
            str(Path(__file__).resolve().parents[1]),
            str(Path(__file__).resolve().parent),
            environment.get("PYTHONPATH"),
        )
        if value
    )
    crashed = subprocess.run(
        [
            sys.executable,
            "-B",
            str(MIGRATION_HARD_EXIT_HELPER),
            str(tmp_path.resolve()),
        ],
        cwd=Path(__file__).resolve().parents[1],
        env=environment,
        capture_output=True,
        check=False,
        timeout=60.0,
    )
    assert crashed.returncode == 73, (crashed.stdout, crashed.stderr)
    journal = Path(str(original.path) + "-journal")
    assert journal.exists() and journal.stat().st_size > 512

    migrated = _dispatcher(tmp_path, lifecycle, guarded, adapters)
    with closing(sqlite3.connect(migrated.path)) as database:
        database.row_factory = sqlite3.Row
        state = migrated._read_state(database)  # noqa: SLF001
        assert dispatcher_subject._schema_inventory(  # noqa: SLF001
            database
        ) == dispatcher_subject.EXPECTED_SCHEMA
    expected_workload = dispatcher_subject.LEGACY_ROUTE_ORDER_V1[legacy_index]
    assert state["next_route_index"] == ROUTE_ORDER.index(expected_workload)


def _continuation_result(adapter, lease, *, children=1):
    continuation = AdapterContinuation.create(
        workload_id=adapter.workload_id,
        event_sha256=lease.event_sha256,
        lease_expires_at=lease.lease_expires_at,
        route_profile_sha256=adapter.fingerprint,
        opaque_pending=SimpleNamespace(lease=lease),
    )
    return AdapterPhaseResult(
        "continuation_pending",
        "final_reproof_pending",
        adapter.workload_id,
        lease.event_sha256,
        "initial",
        children,
        continuation=continuation,
    )


def _ack_result(workload_id, event_sha256, *, phase="single", children=1):
    return AdapterPhaseResult(
        "acknowledged",
        "durable_shadow_output_acknowledged",
        workload_id,
        event_sha256,
        phase,
        children,
        "acknowledged",
        _sha("output:" + event_sha256),
        _sha("output-receipt:" + event_sha256),
        _sha("process-receipt:" + event_sha256),
        _sha("consumption-receipt:" + event_sha256),
    )


def _reject_result(workload_id, event_sha256, *, phase, children=0):
    return AdapterPhaseResult(
        "rejected",
        "desired_state_superseded_before_processing",
        workload_id,
        event_sha256,
        phase,
        children,
        "rejected",
        consumption_receipt_sha256=_sha("consumption:" + event_sha256),
    )


def test_idle_scan_is_bounded_and_rotates_durable_cursor(monkeypatch, tmp_path):
    wall = MutableClock(1000.0)
    guarded = GuardedCompletionClock(wall)
    lifecycle = _lifecycle(tmp_path, wall)
    desired = _desired(tmp_path)
    adapters = _adapter_shells(lifecycle, guarded, desired)
    calls = []

    def no_claim(consumer_id, stream_id, owner_id, *, lease_seconds, now):
        calls.append((consumer_id, stream_id, owner_id, lease_seconds, now))
        return None

    monkeypatch.setattr(lifecycle, "claim_next", no_claim)
    dispatcher = _dispatcher(tmp_path, lifecycle, guarded, adapters).acquire()
    try:
        first = dispatcher.tick()
        second = dispatcher.tick()
    finally:
        dispatcher.close()

    assert first.status == second.status == "idle"
    assert (
        first.scanned_routes
        == second.scanned_routes
        == MAX_IDLE_CLAIMS_PER_TICK
        == len(ROUTE_ORDER)
    )
    assert first.child_invocations == second.child_invocations == 0
    assert len(calls) == 2 * len(ROUTE_ORDER)
    expected = [
        TERMINAL_RAW_CONSUMER_CATALOG[item].consumer_id for item in ROUTE_ORDER
    ]
    assert [item[0] for item in calls[: len(ROUTE_ORDER)]] == expected
    assert [item[0] for item in calls[len(ROUTE_ORDER) :]] == (
        expected[1:] + expected[:1]
    )


def test_process_and_os_owner_fence_is_lifetime_scoped(tmp_path):
    wall = MutableClock(1000.0)
    guarded_one = GuardedCompletionClock(wall)
    lifecycle = _lifecycle(tmp_path, wall)
    desired = _desired(tmp_path)
    first = _dispatcher(
        tmp_path, lifecycle, guarded_one,
        _adapter_shells(lifecycle, guarded_one, desired)
    )
    guarded_two = GuardedCompletionClock(wall)
    second = _dispatcher(
        tmp_path, lifecycle, guarded_two,
        _adapter_shells(lifecycle, guarded_two, desired)
    )
    first.acquire()
    try:
        with pytest.raises(SerialCompletionDispatcherOwnerBusy):
            second.acquire()
    finally:
        first.close()
    second.acquire()
    second.close()


def test_clock_rollback_blocks_before_any_claim(monkeypatch, tmp_path):
    wall = MutableClock(1000.0)
    guarded = GuardedCompletionClock(wall)
    lifecycle = _lifecycle(tmp_path, wall)
    desired = _desired(tmp_path)
    adapters = _adapter_shells(lifecycle, guarded, desired)
    claims = 0

    def no_claim(*args, **kwargs):
        nonlocal claims
        claims += 1
        return None

    monkeypatch.setattr(lifecycle, "claim_next", no_claim)
    dispatcher = _dispatcher(tmp_path, lifecycle, guarded, adapters).acquire()
    try:
        assert dispatcher.tick().status == "idle"
        assert claims == len(ROUTE_ORDER)
        wall.value = 999.0
        blocked = dispatcher.tick()
    finally:
        dispatcher.close()
    assert blocked.status == "blocked"
    assert blocked.reason_code == "clock_rollback_detected"
    assert claims == len(ROUTE_ORDER)


def test_split_crash_restart_never_persists_token_or_repeats_initial_child(
    monkeypatch, tmp_path
):
    wall = MutableClock(1000.0)
    lifecycle = _lifecycle(tmp_path, wall)
    desired = _desired(tmp_path)
    workload_id = "satellite.noaa.err"
    token_one = "LEASE-TOKEN-MUST-NEVER-PERSIST-ONE"
    token_two = "LEASE-TOKEN-MUST-NEVER-PERSIST-TWO"
    first_lease = _lease(workload_id, token=token_one, expires_at=1100.0)
    reclaimed_lease = _lease(workload_id, token=token_two, expires_at=1401.0)
    first_claimed = False
    reclaimed_claimed = False
    reclaim_ready = False
    durable_output = False
    initial_calls = 0
    final_calls = 0
    executor_calls = 0

    def claim(consumer_id, stream_id, owner_id, *, lease_seconds, now):
        nonlocal first_claimed, reclaimed_claimed
        binding = TERMINAL_RAW_CONSUMER_CATALOG[workload_id]
        if (consumer_id, stream_id) != (binding.consumer_id, binding.stream_id):
            return None
        if not first_claimed:
            first_claimed = True
            return first_lease
        if reclaim_ready and not reclaimed_claimed:
            reclaimed_claimed = True
            return reclaimed_lease
        return None

    def begin(adapter, lease, *, permit, permit_nonce, cancel_event):
        nonlocal durable_output, initial_calls, executor_calls
        initial_calls += 1
        children = 0
        if not durable_output:
            permit.consume(
                workload_id=workload_id, phase="initial", nonce=permit_nonce
            )
            durable_output = True
            executor_calls += 1
            children = 1
        pending = SimpleNamespace(lease=lease)
        continuation = AdapterContinuation.create(
            workload_id=workload_id,
            event_sha256=lease.event_sha256,
            lease_expires_at=lease.lease_expires_at,
            route_profile_sha256=adapter.fingerprint,
            opaque_pending=pending,
        )
        return AdapterPhaseResult(
            "continuation_pending",
            "final_reproof_pending",
            workload_id,
            lease.event_sha256,
            "initial",
            children,
            continuation=continuation,
        )

    def finalize(adapter, continuation, *, permit, permit_nonce, cancel_event):
        nonlocal final_calls, executor_calls
        final_calls += 1
        executor_calls += 1
        permit.consume(
            workload_id=workload_id,
            phase="final_reproof",
            nonce=permit_nonce,
        )
        return AdapterPhaseResult(
            "acknowledged",
            "durable_shadow_output_acknowledged",
            workload_id,
            continuation.event_sha256,
            "final_reproof",
            1,
            "acknowledged",
            _sha("output"),
            _sha("output-receipt"),
            _sha("process-receipt"),
            _sha("consumption-receipt"),
        )

    monkeypatch.setattr(lifecycle, "claim_next", claim)
    monkeypatch.setattr(NoaaCompletionRouteAdapter, "begin", begin)
    monkeypatch.setattr(NoaaCompletionRouteAdapter, "finalize", finalize)

    guarded_one = GuardedCompletionClock(wall)
    first = _dispatcher(
        tmp_path, lifecycle, guarded_one,
        _adapter_shells(lifecycle, guarded_one, desired)
    ).acquire()
    initial = first.tick()
    assert initial.status == "continuation_pending"
    assert initial.child_invocations == MAX_HEAVY_CHILDREN_PER_TICK == 1
    first.close()  # Simulated dispatcher-process loss: pending object is gone.

    database_path = tmp_path / "dispatcher.sqlite3"
    with closing(sqlite3.connect(database_path)) as db:
        columns = {
            str(row[1])
            for table in ("manifest", "dispatcher_state")
            for row in db.execute(f"PRAGMA table_info({table})")
        }
        values = [
            value
            for table in ("manifest", "dispatcher_state")
            for row in db.execute(f"SELECT * FROM {table}")
            for value in row
        ]
    serialized = "|".join(str(value) for value in values)
    raw_database = database_path.read_bytes()
    assert "lease_token" not in columns
    assert "event_id" not in columns
    assert "payload" not in columns
    assert token_one not in serialized and token_two not in serialized
    assert first_lease.event_id not in serialized
    assert "PRIVATE-DISPATCH-PAYLOAD" not in serialized
    for forbidden in (
        token_one.encode(), token_two.encode(), first_lease.event_id.encode(),
        b"PRIVATE-DISPATCH-PAYLOAD", b"opaque_pending", b"lease_token",
    ):
        assert forbidden not in raw_database

    guarded_two = GuardedCompletionClock(wall)
    second = _dispatcher(
        tmp_path, lifecycle, guarded_two,
        _adapter_shells(lifecycle, guarded_two, desired)
    ).acquire()
    try:
        before_expiry = second.tick()
        assert before_expiry.status == "idle"
        assert initial_calls == 1
        assert executor_calls == 1

        wall.value = 1101.0
        reclaim_ready = True
        recovered = second.tick()
        assert recovered.status == "continuation_pending"
        assert recovered.child_invocations == 0
        assert initial_calls == 2
        assert executor_calls == 1

        terminal = second.tick()
    finally:
        second.close()
    assert terminal.status == "acknowledged"
    assert terminal.child_invocations == 1
    assert final_calls == 1
    assert executor_calls == 2


def test_forged_permanent_failure_cannot_quarantine(monkeypatch, tmp_path):
    wall = MutableClock(1000.0)
    guarded = GuardedCompletionClock(wall)
    lifecycle = _lifecycle(tmp_path, wall)
    desired = _desired(tmp_path)
    adapters = _adapter_shells(lifecycle, guarded, desired)
    workload_id = _SINGLE_PHASE_TEST_WORKLOAD_ID
    lease = _lease(workload_id, token="not-persisted", expires_at=1300.0)
    claimed = False
    quarantine_calls = 0

    def claim(consumer_id, stream_id, owner_id, *, lease_seconds, now):
        nonlocal claimed
        if claimed or not _claim_matches(
            workload_id, consumer_id, stream_id
        ):
            return None
        claimed = True
        return lease

    def forged(adapter, held, *, permit, permit_nonce, cancel_event):
        raise PermanentEventFailure(
            lease=held,
            reason_code="unsupported_completion_contract",
            failure_stage="completion_contract",
            route_profile_sha256=adapter.fingerprint,
        )

    def quarantine(*args, **kwargs):
        nonlocal quarantine_calls
        quarantine_calls += 1
        raise AssertionError("untrusted typed failure reached quarantine")

    monkeypatch.setattr(lifecycle, "claim_next", claim)
    monkeypatch.setattr(lifecycle, "quarantine_leased", quarantine)
    monkeypatch.setattr(_SinglePhaseAdapter, "begin", forged)
    dispatcher = _dispatcher(tmp_path, lifecycle, guarded, adapters).acquire()
    try:
        result = dispatcher.tick()
    finally:
        dispatcher.close()
    assert result.status == "blocked"
    assert result.reason_code == "permanent_failure_authority_unavailable"
    assert quarantine_calls == 0


def test_close_during_noncooperative_child_retains_owner_until_quiescent(
    monkeypatch, tmp_path
):
    wall = MutableClock(1000.0)
    guarded_one = GuardedCompletionClock(wall)
    lifecycle = _lifecycle(tmp_path, wall)
    desired = _desired(tmp_path)
    workload_id = "satellite.noaa.err"
    lease = _lease(workload_id, token="private-close-token", expires_at=1300.0)
    claimed = False
    entered = threading.Event()
    release = threading.Event()
    child_calls = 0

    def claim(consumer_id, stream_id, owner_id, *, lease_seconds, now):
        nonlocal claimed
        binding = TERMINAL_RAW_CONSUMER_CATALOG[workload_id]
        if claimed or (consumer_id, stream_id) != (
            binding.consumer_id, binding.stream_id
        ):
            return None
        claimed = True
        return lease

    def noncooperative(adapter, held, *, permit, permit_nonce, cancel_event):
        nonlocal child_calls
        child_calls += 1
        permit.consume(
            workload_id=workload_id, phase="initial", nonce=permit_nonce
        )
        entered.set()
        assert release.wait(5.0)
        continuation = AdapterContinuation.create(
            workload_id=workload_id,
            event_sha256=held.event_sha256,
            lease_expires_at=held.lease_expires_at,
            route_profile_sha256=adapter.fingerprint,
            opaque_pending=SimpleNamespace(lease=held),
        )
        return AdapterPhaseResult(
            "continuation_pending", "final_reproof_pending", workload_id,
            held.event_sha256, "initial", 1, continuation=continuation,
        )

    monkeypatch.setattr(lifecycle, "claim_next", claim)
    monkeypatch.setattr(NoaaCompletionRouteAdapter, "begin", noncooperative)
    first = _dispatcher(
        tmp_path,
        lifecycle,
        guarded_one,
        _adapter_shells(lifecycle, guarded_one, desired),
    ).acquire()
    tick_result = []
    thread = threading.Thread(target=lambda: tick_result.append(first.tick()))
    thread.start()
    assert entered.wait(3.0)
    assert first.close(timeout_seconds=0.01) is False

    guarded_two = GuardedCompletionClock(wall)
    second = _dispatcher(
        tmp_path,
        lifecycle,
        guarded_two,
        _adapter_shells(lifecycle, guarded_two, desired),
    )
    with pytest.raises(SerialCompletionDispatcherOwnerBusy):
        second.acquire()
    assert child_calls == 1

    release.set()
    thread.join(5.0)
    assert not thread.is_alive()
    assert tick_result[0].status == "continuation_pending"
    assert first.close(timeout_seconds=1.0) is True
    second.acquire()
    second.close()


def test_state_hash_and_schema_tamper_fail_closed(tmp_path):
    wall = MutableClock(1000.0)
    guarded = GuardedCompletionClock(wall)
    lifecycle = _lifecycle(tmp_path, wall)
    desired = _desired(tmp_path)
    dispatcher = _dispatcher(
        tmp_path, lifecycle, guarded,
        _adapter_shells(lifecycle, guarded, desired)
    ).acquire()
    dispatcher.close()
    with closing(sqlite3.connect(tmp_path / "dispatcher.sqlite3")) as db:
        db.execute("UPDATE dispatcher_state SET state_sha256=?", ("f" * 64,))
        db.commit()
    next_clock = GuardedCompletionClock(wall)
    with pytest.raises(SerialCompletionDispatcherIntegrityError):
        _dispatcher(
            tmp_path,
            lifecycle,
            next_clock,
            _adapter_shells(lifecycle, next_clock, desired),
        )


@pytest.mark.parametrize(
    "point", ["owner_adopt.before_commit", "owner_adopt.after_commit"]
)
def test_owner_adopt_fault_never_leaks_owner_fence(point, tmp_path):
    wall = MutableClock(1000.0)
    lifecycle = _lifecycle(tmp_path, wall)
    desired = _desired(tmp_path)
    guarded = GuardedCompletionClock(wall)
    failed = _dispatcher(
        tmp_path,
        lifecycle,
        guarded,
        _adapter_shells(lifecycle, guarded, desired),
        fault_injector=FaultOnce(point),
    )
    with pytest.raises(RuntimeError, match="injected_dispatcher_fault"):
        failed.acquire()

    recovery_clock = GuardedCompletionClock(wall)
    recovered = _dispatcher(
        tmp_path,
        lifecycle,
        recovery_clock,
        _adapter_shells(lifecycle, recovery_clock, desired),
    ).acquire()
    assert recovered.close()


@pytest.mark.parametrize(
    "point", ["tick_begin.before_commit", "tick_begin.after_commit"]
)
def test_tick_begin_fault_blocks_before_claim(point, monkeypatch, tmp_path):
    wall = MutableClock(1000.0)
    guarded = GuardedCompletionClock(wall)
    lifecycle = _lifecycle(tmp_path, wall)
    desired = _desired(tmp_path)
    claims = 0

    def claim(*args, **kwargs):
        nonlocal claims
        claims += 1
        return None

    monkeypatch.setattr(lifecycle, "claim_next", claim)
    dispatcher = _dispatcher(
        tmp_path,
        lifecycle,
        guarded,
        _adapter_shells(lifecycle, guarded, desired),
        fault_injector=FaultOnce(point),
    ).acquire()
    try:
        result = dispatcher.tick()
    finally:
        dispatcher.close()
    assert result.status == "blocked"
    assert result.child_invocations == 0
    assert result.workload_id is None
    assert claims == 0


@pytest.mark.parametrize(
    ("point", "pending_phase"),
    [
        ("claim_persist.before_commit", None),
        ("claim_persist.after_commit", "initial"),
    ],
)
def test_claim_state_fault_never_invokes_adapter(
    point, pending_phase, monkeypatch, tmp_path
):
    wall = MutableClock(1000.0)
    guarded = GuardedCompletionClock(wall)
    lifecycle = _lifecycle(tmp_path, wall)
    desired = _desired(tmp_path)
    workload_id = _SINGLE_PHASE_TEST_WORKLOAD_ID
    lease = _lease(workload_id, token="private-claim-fault", expires_at=1300.0)
    claimed = False
    adapter_calls = 0

    def claim(consumer_id, stream_id, owner_id, *, lease_seconds, now):
        nonlocal claimed
        if claimed or not _claim_matches(
            workload_id, consumer_id, stream_id
        ):
            return None
        claimed = True
        return lease

    def forbidden(*args, **kwargs):
        nonlocal adapter_calls
        adapter_calls += 1
        raise AssertionError("adapter ran without durable claim state")

    monkeypatch.setattr(lifecycle, "claim_next", claim)
    monkeypatch.setattr(_SinglePhaseAdapter, "begin", forbidden)
    dispatcher = _dispatcher(
        tmp_path,
        lifecycle,
        guarded,
        _adapter_shells(lifecycle, guarded, desired),
        fault_injector=FaultOnce(point),
    ).acquire()
    try:
        result = dispatcher.tick()
        with closing(sqlite3.connect(dispatcher.path)) as db:
            durable_phase = db.execute(
                "SELECT pending_phase FROM dispatcher_state"
            ).fetchone()[0]
    finally:
        dispatcher.close()
    assert result.status in {"blocked", "retryable"}
    assert adapter_calls == 0
    assert durable_phase == pending_phase


@pytest.mark.parametrize(
    ("point", "pending_phase"),
    [
        ("continuation_persist.before_commit", "initial"),
        ("continuation_persist.after_commit", "final_reproof"),
    ],
)
def test_continuation_fault_keeps_only_safe_durable_marker(
    point, pending_phase, monkeypatch, tmp_path
):
    wall = MutableClock(1000.0)
    guarded = GuardedCompletionClock(wall)
    lifecycle = _lifecycle(tmp_path, wall)
    desired = _desired(tmp_path)
    workload_id = "satellite.noaa.err"
    lease = _lease(workload_id, token="private-continuation-fault", expires_at=1300.0)
    claimed = False
    child_calls = 0

    def claim(consumer_id, stream_id, owner_id, *, lease_seconds, now):
        nonlocal claimed
        binding = TERMINAL_RAW_CONSUMER_CATALOG[workload_id]
        if claimed or (consumer_id, stream_id) != (
            binding.consumer_id, binding.stream_id
        ):
            return None
        claimed = True
        return lease

    def begin(adapter, held, *, permit, permit_nonce, cancel_event):
        nonlocal child_calls
        child_calls += 1
        permit.consume(
            workload_id=workload_id, phase="initial", nonce=permit_nonce
        )
        return _continuation_result(adapter, held)

    monkeypatch.setattr(lifecycle, "claim_next", claim)
    monkeypatch.setattr(NoaaCompletionRouteAdapter, "begin", begin)
    dispatcher = _dispatcher(
        tmp_path,
        lifecycle,
        guarded,
        _adapter_shells(lifecycle, guarded, desired),
        fault_injector=FaultOnce(point),
    ).acquire()
    try:
        result = dispatcher.tick()
        with closing(sqlite3.connect(dispatcher.path)) as db:
            row = db.execute(
                "SELECT pending_phase,pending_event_sha256 FROM dispatcher_state"
            ).fetchone()
    finally:
        dispatcher.close()
    assert result.status in {"blocked", "retryable"}
    assert child_calls == 1
    assert row[0] == pending_phase
    assert row[1] == lease.event_sha256
    raw = (tmp_path / "dispatcher.sqlite3").read_bytes()
    assert lease.lease_token.encode() not in raw
    assert lease.event_id.encode() not in raw


@pytest.mark.parametrize(
    ("point", "pending_phase"),
    [
        ("pending_finish.before_commit", "initial"),
        ("pending_finish.after_commit", None),
    ],
)
def test_finish_fault_is_idempotent_and_never_runs_second_child(
    point, pending_phase, monkeypatch, tmp_path
):
    wall = MutableClock(1000.0)
    guarded = GuardedCompletionClock(wall)
    lifecycle = _lifecycle(tmp_path, wall)
    desired = _desired(tmp_path)
    workload_id = _SINGLE_PHASE_TEST_WORKLOAD_ID
    lease = _lease(workload_id, token="private-finish-fault", expires_at=1300.0)
    claimed = False
    calls = 0

    def claim(consumer_id, stream_id, owner_id, *, lease_seconds, now):
        nonlocal claimed
        if claimed or not _claim_matches(
            workload_id, consumer_id, stream_id
        ):
            return None
        claimed = True
        return lease

    def acknowledged(adapter, held, *, permit, permit_nonce, cancel_event):
        nonlocal calls
        calls += 1
        permit.consume(
            workload_id=workload_id, phase="single", nonce=permit_nonce
        )
        return _ack_result(workload_id, held.event_sha256)

    monkeypatch.setattr(lifecycle, "claim_next", claim)
    monkeypatch.setattr(_SinglePhaseAdapter, "begin", acknowledged)
    dispatcher = _dispatcher(
        tmp_path,
        lifecycle,
        guarded,
        _adapter_shells(lifecycle, guarded, desired),
        fault_injector=FaultOnce(point),
    ).acquire()
    try:
        result = dispatcher.tick()
        with closing(sqlite3.connect(dispatcher.path)) as db:
            durable_phase = db.execute(
                "SELECT pending_phase FROM dispatcher_state"
            ).fetchone()[0]
    finally:
        dispatcher.close()
    assert result.status == "blocked"
    assert result.child_invocations == 1
    assert result.workload_id == workload_id
    assert result.event_sha256 == lease.event_sha256
    assert result.phase == "single"
    assert calls == 1
    assert durable_phase == pending_phase


def test_stop_and_external_cancel_block_before_claim(monkeypatch, tmp_path):
    wall = MutableClock(1000.0)
    guarded = GuardedCompletionClock(wall)
    lifecycle = _lifecycle(tmp_path, wall)
    desired = _desired(tmp_path)
    claims = 0

    def claim(*args, **kwargs):
        nonlocal claims
        claims += 1
        return None

    monkeypatch.setattr(lifecycle, "claim_next", claim)
    dispatcher = _dispatcher(
        tmp_path, lifecycle, guarded,
        _adapter_shells(lifecycle, guarded, desired),
    ).acquire()
    external = threading.Event()
    external.set()
    assert dispatcher.tick(cancel_event=external).status == "stopped"
    dispatcher.request_stop()
    assert dispatcher.tick().status == "stopped"
    assert claims == 0
    dispatcher.close()


@pytest.mark.parametrize("lie", ["no_consume", "double_consume"])
def test_adapter_cannot_lie_or_reuse_permit(lie, monkeypatch, tmp_path):
    wall = MutableClock(1000.0)
    guarded = GuardedCompletionClock(wall)
    lifecycle = _lifecycle(tmp_path, wall)
    desired = _desired(tmp_path)
    workload_id = _SINGLE_PHASE_TEST_WORKLOAD_ID
    lease = _lease(workload_id, token="private-permit-lie", expires_at=1300.0)
    claimed = False

    def claim(consumer_id, stream_id, owner_id, *, lease_seconds, now):
        nonlocal claimed
        if claimed or not _claim_matches(
            workload_id, consumer_id, stream_id
        ):
            return None
        claimed = True
        return lease

    def lying(adapter, held, *, permit, permit_nonce, cancel_event):
        if lie == "double_consume":
            permit.consume(
                workload_id=workload_id, phase="single", nonce=permit_nonce
            )
            permit.consume(
                workload_id=workload_id, phase="single", nonce=permit_nonce
            )
        return _ack_result(workload_id, held.event_sha256)

    monkeypatch.setattr(lifecycle, "claim_next", claim)
    monkeypatch.setattr(_SinglePhaseAdapter, "begin", lying)
    dispatcher = _dispatcher(
        tmp_path, lifecycle, guarded,
        _adapter_shells(lifecycle, guarded, desired),
    ).acquire()
    try:
        result = dispatcher.tick()
    finally:
        dispatcher.close()
    assert result.status == "blocked"
    assert result.reason_code == "adapter_integrity_fenced"
    assert result.child_invocations == 1
    assert result.terminal_kind is None


@pytest.mark.parametrize(
    "error_type",
    [sqlite3.IntegrityError, sqlite3.ProgrammingError, sqlite3.DataError],
)
def test_sqlite_contract_errors_fence_and_retain_pending_accounting(
    error_type, monkeypatch, tmp_path
):
    wall = MutableClock(1000.0)
    guarded = GuardedCompletionClock(wall)
    lifecycle = _lifecycle(tmp_path, wall)
    desired = _desired(tmp_path)
    workload_id = _SINGLE_PHASE_TEST_WORKLOAD_ID
    lease = _lease(workload_id, token="private-sqlite-fatal", expires_at=1300.0)
    claimed = False

    def claim(consumer_id, stream_id, owner_id, *, lease_seconds, now):
        nonlocal claimed
        if claimed or not _claim_matches(
            workload_id, consumer_id, stream_id
        ):
            return None
        claimed = True
        return lease

    def fail(*args, **kwargs):
        raise error_type("forged adapter database contract failure")

    monkeypatch.setattr(lifecycle, "claim_next", claim)
    monkeypatch.setattr(_SinglePhaseAdapter, "begin", fail)
    dispatcher = _dispatcher(
        tmp_path, lifecycle, guarded,
        _adapter_shells(lifecycle, guarded, desired),
    ).acquire()
    try:
        result = dispatcher.tick()
        with closing(sqlite3.connect(dispatcher.path)) as db:
            fenced, pending = db.execute(
                "SELECT integrity_fenced,pending_workload_id "
                "FROM dispatcher_state"
            ).fetchone()
    finally:
        dispatcher.close()
    assert result.status == "blocked"
    assert result.reason_code == "adapter_integrity_fenced"
    assert result.child_invocations == 1
    assert fenced == 1
    assert pending == workload_id


def test_sqlite_busy_is_retryable_and_does_not_integrity_fence(
    monkeypatch, tmp_path
):
    wall = MutableClock(1000.0)
    guarded = GuardedCompletionClock(wall)
    lifecycle = _lifecycle(tmp_path, wall)
    desired = _desired(tmp_path)
    workload_id = _SINGLE_PHASE_TEST_WORKLOAD_ID
    lease = _lease(workload_id, token="private-sqlite-busy", expires_at=1300.0)
    claimed = False

    def claim(consumer_id, stream_id, owner_id, *, lease_seconds, now):
        nonlocal claimed
        if claimed or not _claim_matches(
            workload_id, consumer_id, stream_id
        ):
            return None
        claimed = True
        return lease

    def busy(*args, **kwargs):
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(lifecycle, "claim_next", claim)
    monkeypatch.setattr(_SinglePhaseAdapter, "begin", busy)
    dispatcher = _dispatcher(
        tmp_path, lifecycle, guarded,
        _adapter_shells(lifecycle, guarded, desired),
    ).acquire()
    try:
        result = dispatcher.tick()
        diagnostics = dispatcher.diagnostics()
    finally:
        dispatcher.close()
    assert result.status == "retryable"
    assert result.reason_code == "system_store_unavailable"
    assert result.child_invocations == 1
    assert diagnostics["integrity_fenced"] is False
    assert diagnostics["retryable_results"] == 1


def test_eleven_route_fairness_invokes_exactly_one_adapter_per_tick(
    monkeypatch, tmp_path
):
    wall = MutableClock(1000.0)
    guarded = GuardedCompletionClock(wall)
    lifecycle = _lifecycle(tmp_path, wall)
    desired = _desired(tmp_path)
    leases = {
        workload_id: _lease(
            workload_id,
            token="fair-" + str(index),
            expires_at=1300.0,
        )
        for index, workload_id in enumerate(ROUTE_ORDER)
    }
    available = set(ROUTE_ORDER)
    invoked = []

    def claim(consumer_id, stream_id, owner_id, *, lease_seconds, now):
        for workload_id in ROUTE_ORDER:
            binding = TERMINAL_RAW_CONSUMER_CATALOG[workload_id]
            if (
                workload_id in available
                and (consumer_id, stream_id)
                == (binding.consumer_id, binding.stream_id)
            ):
                available.remove(workload_id)
                return leases[workload_id]
        return None

    def complete(adapter, held, *, permit, permit_nonce, cancel_event):
        phase = (
            "initial"
            if isinstance(
                adapter,
                (NoaaCompletionRouteAdapter, RawManifestCompletionRouteAdapter),
            )
            else "single"
        )
        permit.consume(
            workload_id=adapter.workload_id,
            phase=phase,
            nonce=permit_nonce,
        )
        invoked.append(adapter.workload_id)
        if phase == "initial":
            return _reject_result(
                adapter.workload_id,
                held.event_sha256,
                phase=phase,
                children=1,
            )
        return _ack_result(adapter.workload_id, held.event_sha256, phase=phase)

    monkeypatch.setattr(lifecycle, "claim_next", claim)
    for adapter_type in (
        _SinglePhaseAdapter,
        DwdCompletionRouteAdapter,
        NoaaCompletionRouteAdapter,
        RawManifestCompletionRouteAdapter,
    ):
        monkeypatch.setattr(adapter_type, "begin", complete)
    dispatcher = _dispatcher(
        tmp_path, lifecycle, guarded,
        _adapter_shells(lifecycle, guarded, desired),
    ).acquire()
    try:
        results = [dispatcher.tick() for _ in ROUTE_ORDER]
    finally:
        dispatcher.close()
    assert invoked == list(ROUTE_ORDER)
    assert all(
        result.status
        == (
            "rejected"
            if result.workload_id == "satellite.noaa.err"
            or TERMINAL_RAW_CONSUMER_CATALOG[
                result.workload_id
            ].consumer_kind == "raw_manifest_audit"
            else "acknowledged"
        )
        for result in results
    )
    assert all(result.child_invocations == 1 for result in results)
    assert all(result.scanned_routes == 1 for result in results)


@pytest.mark.parametrize("barrier", ["final_reproof", "ack"])
def test_close_during_final_or_ack_keeps_fence(
    barrier, monkeypatch, tmp_path
):
    wall = MutableClock(1000.0)
    lifecycle = _lifecycle(tmp_path, wall)
    desired = _desired(tmp_path)
    workload_id = "satellite.noaa.err"
    lease = _lease(workload_id, token="private-final-close", expires_at=1300.0)
    claimed = False
    entered = threading.Event()
    release = threading.Event()
    final_children = 0

    def claim(consumer_id, stream_id, owner_id, *, lease_seconds, now):
        nonlocal claimed
        binding = TERMINAL_RAW_CONSUMER_CATALOG[workload_id]
        if claimed or (consumer_id, stream_id) != (
            binding.consumer_id, binding.stream_id
        ):
            return None
        claimed = True
        return lease

    def begin(adapter, held, *, permit, permit_nonce, cancel_event):
        permit.consume(
            workload_id=workload_id, phase="initial", nonce=permit_nonce
        )
        return _continuation_result(adapter, held)

    def finalize(adapter, continuation, *, permit, permit_nonce, cancel_event):
        nonlocal final_children
        if barrier == "final_reproof":
            entered.set()
            assert release.wait(5.0)
        permit.consume(
            workload_id=workload_id,
            phase="final_reproof",
            nonce=permit_nonce,
        )
        final_children += 1
        if barrier == "ack":
            entered.set()
            assert release.wait(5.0)
        return _ack_result(
            workload_id,
            continuation.event_sha256,
            phase="final_reproof",
        )

    monkeypatch.setattr(lifecycle, "claim_next", claim)
    monkeypatch.setattr(NoaaCompletionRouteAdapter, "begin", begin)
    monkeypatch.setattr(NoaaCompletionRouteAdapter, "finalize", finalize)
    guarded_one = GuardedCompletionClock(wall)
    first = _dispatcher(
        tmp_path, lifecycle, guarded_one,
        _adapter_shells(lifecycle, guarded_one, desired),
    ).acquire()
    assert first.tick().status == "continuation_pending"
    values = []
    thread = threading.Thread(target=lambda: values.append(first.tick()))
    thread.start()
    assert entered.wait(3.0)
    assert first.close(timeout_seconds=0.01) is False

    guarded_two = GuardedCompletionClock(wall)
    second = _dispatcher(
        tmp_path, lifecycle, guarded_two,
        _adapter_shells(lifecycle, guarded_two, desired),
    )
    with pytest.raises(SerialCompletionDispatcherOwnerBusy):
        second.acquire()
    release.set()
    thread.join(5.0)
    assert not thread.is_alive()
    assert values[0].status == "acknowledged"
    assert final_children == 1
    assert first.close(timeout_seconds=1.0)
    second.acquire()
    second.close()


def test_manifest_schema_filecap_and_composition_replacement_fail_closed(
    monkeypatch, tmp_path
):
    wall = MutableClock(1000.0)
    guarded = GuardedCompletionClock(wall)
    lifecycle = _lifecycle(tmp_path, wall)
    desired = _desired(tmp_path)
    dispatcher = _dispatcher(
        tmp_path, lifecycle, guarded,
        _adapter_shells(lifecycle, guarded, desired),
    ).acquire()
    dispatcher.close()
    path = tmp_path / "dispatcher.sqlite3"
    with closing(sqlite3.connect(path)) as db:
        db.execute(
            "UPDATE manifest SET value=? WHERE key='route_catalog_sha256'",
            ("0" * 64,),
        )
        db.commit()
    retry_clock = GuardedCompletionClock(wall)
    with pytest.raises(SerialCompletionDispatcherIntegrityError):
        _dispatcher(
            tmp_path, lifecycle, retry_clock,
            _adapter_shells(lifecycle, retry_clock, desired),
        )

    # A separate fresh instance proves the physical file envelope too.
    cap_root = tmp_path / "cap"
    cap_root.mkdir()
    cap_lifecycle = _lifecycle(cap_root, wall)
    cap_desired = _desired(cap_root)
    cap_clock = GuardedCompletionClock(wall)
    capped = _dispatcher(
        cap_root, cap_lifecycle, cap_clock,
        _adapter_shells(cap_lifecycle, cap_clock, cap_desired),
    ).acquire()
    capped.close()
    with open(cap_root / "dispatcher.sqlite3", "ab") as file:
        file.truncate(MAX_DATABASE_BYTES + 1)
    cap_retry_clock = GuardedCompletionClock(wall)
    with pytest.raises(SerialCompletionDispatcherError):
        _dispatcher(
            cap_root, cap_lifecycle, cap_retry_clock,
            _adapter_shells(cap_lifecycle, cap_retry_clock, cap_desired),
        )

    # Live path-witness replacement blocks before lifecycle claim.
    witness_root = tmp_path / "witness"
    witness_root.mkdir()
    witness_lifecycle = _lifecycle(witness_root, wall)
    witness_desired = _desired(witness_root)
    witness_clock = GuardedCompletionClock(wall)
    witness = _dispatcher(
        witness_root, witness_lifecycle, witness_clock,
        _adapter_shells(
            witness_lifecycle, witness_clock, witness_desired
        ),
    ).acquire()
    replacement = DesiredStateStore(
        (witness_root / "replacement.sqlite3").resolve()
    )
    assert replacement.load_active() is None
    os.replace(replacement.path, witness_desired.path)
    blocked = witness.tick()
    assert blocked.status == "blocked"
    witness.close()


def test_diagnostics_are_bounded_and_capability_free(monkeypatch, tmp_path):
    wall = MutableClock(1000.0)
    guarded = GuardedCompletionClock(wall)
    lifecycle = _lifecycle(tmp_path, wall)
    desired = _desired(tmp_path)
    monkeypatch.setattr(lifecycle, "claim_next", lambda *args, **kwargs: None)
    dispatcher = _dispatcher(
        tmp_path, lifecycle, guarded,
        _adapter_shells(lifecycle, guarded, desired),
    ).acquire()
    try:
        dispatcher.tick()
        diagnostics = dispatcher.diagnostics()
    finally:
        dispatcher.close()
    encoded = json.dumps(diagnostics, sort_keys=True)
    assert len(encoded.encode()) < 4096
    for forbidden in (
        str(tmp_path), "lease_token", "event_id", "payload", "latitude",
        "longitude", "authorization", "private-",
    ):
        assert forbidden not in encoded


def test_sequence_clock_has_bounded_state_commits_and_restart_floor(
    monkeypatch, tmp_path
):
    source = SequenceClock(1000.0)
    guarded = GuardedCompletionClock(source)
    lifecycle = _lifecycle(tmp_path, MutableClock(1000.0))
    desired = _desired(tmp_path)
    claim_times = []

    def idle_claim(*args, now, **kwargs):
        claim_times.append(now)
        return None

    monkeypatch.setattr(lifecycle, "claim_next", idle_claim)
    dispatcher = _dispatcher(
        tmp_path,
        lifecycle,
        guarded,
        _adapter_shells(lifecycle, guarded, desired),
    ).acquire()
    with closing(sqlite3.connect(dispatcher.path)) as db:
        before = db.execute(
            "SELECT revision FROM dispatcher_state"
        ).fetchone()[0]
    result = dispatcher.tick()
    with closing(sqlite3.connect(dispatcher.path)) as db:
        revision, durable_clock = db.execute(
            "SELECT revision,last_observed_at FROM dispatcher_state"
        ).fetchone()
    assert result.status == "idle"
    assert len(claim_times) == len(ROUTE_ORDER)
    assert claim_times[0] == 1003.0
    assert revision - before == 2  # tick_begin + one idle_rotate only
    assert durable_clock == source.values[-1]
    dispatcher.close()

    rollback = GuardedCompletionClock(MutableClock(durable_clock - 1.0))
    reopened = _dispatcher(
        tmp_path,
        lifecycle,
        rollback,
        _adapter_shells(lifecycle, rollback, desired),
    )
    with pytest.raises(SerialCompletionDispatcherClockRollback):
        reopened.acquire()


def test_lifecycle_clock_watermark_is_read_only_on_owner_acquire(
    monkeypatch, tmp_path
):
    wall = MutableClock(1000.0)
    guarded = GuardedCompletionClock(wall)
    lifecycle = _lifecycle(tmp_path, wall)
    desired = _desired(tmp_path)
    watermark_reads = 0
    original = lifecycle.durable_clock_high_water

    def counted_watermark():
        nonlocal watermark_reads
        watermark_reads += 1
        return original()

    monkeypatch.setattr(
        lifecycle, "durable_clock_high_water", counted_watermark
    )
    monkeypatch.setattr(
        lifecycle, "claim_next", lambda *args, **kwargs: None
    )
    dispatcher = _dispatcher(
        tmp_path, lifecycle, guarded,
        _adapter_shells(lifecycle, guarded, desired),
    )
    assert watermark_reads == 0
    dispatcher.acquire()
    assert watermark_reads == 1
    try:
        assert dispatcher.tick().status == "idle"
        assert dispatcher.tick().status == "idle"
    finally:
        dispatcher.close()
    assert watermark_reads == 1


@pytest.mark.parametrize("link_kind", ["symlink", "hardlink"])
def test_preexisting_link_leaf_never_mutates_external_bytes(
    link_kind, tmp_path
):
    wall = MutableClock(1000.0)
    lifecycle = _lifecycle(tmp_path, wall)
    desired = _desired(tmp_path)
    external = tmp_path / "external-sentinel.bin"
    sentinel = b"EXTERNAL-SENTINEL-MUST-NOT-CHANGE"
    external.write_bytes(sentinel)
    dispatcher_path = tmp_path / "dispatcher.sqlite3"
    try:
        if link_kind == "symlink":
            os.symlink(external, dispatcher_path)
        else:
            os.link(external, dispatcher_path)
    except OSError as exc:
        pytest.skip(f"{link_kind} unavailable: {exc}")
    guarded = GuardedCompletionClock(wall)
    with pytest.raises(SerialCompletionDispatcherIntegrityError):
        _dispatcher(
            tmp_path,
            lifecycle,
            guarded,
            _adapter_shells(lifecycle, guarded, desired),
        )
    assert external.read_bytes() == sentinel
    assert not (tmp_path / "dispatcher.sqlite3.owner.lock").exists()


@pytest.mark.parametrize(
    "drift", ["fingerprint", "store_binding", "consumer_store"]
)
def test_adapter_map_and_fields_are_pinned_before_any_claim(
    drift, monkeypatch, tmp_path
):
    wall = MutableClock(1000.0)
    guarded = GuardedCompletionClock(wall)
    lifecycle = _lifecycle(tmp_path, wall)
    desired = _desired(tmp_path)
    claims = 0

    def forbidden_claim(*args, **kwargs):
        nonlocal claims
        claims += 1
        raise AssertionError("adapter drift reached lifecycle claim")

    monkeypatch.setattr(lifecycle, "claim_next", forbidden_claim)
    adapters = _adapter_shells(lifecycle, guarded, desired)
    dispatcher = _dispatcher(
        tmp_path, lifecycle, guarded, adapters
    ).acquire()
    with pytest.raises(TypeError):
        dispatcher.adapters[ROUTE_ORDER[0]] = adapters[ROUTE_ORDER[1]]
    adapter = adapters[ROUTE_ORDER[0]]
    if drift == "fingerprint":
        adapter.fingerprint = _sha("drifted-adapter")
    elif drift == "store_binding":
        adapter.composition_store_bindings[0]["roles"].append(
            "forged.output_store"
        )
    else:
        adapter._consumer.output_store = SimpleNamespace(
            path=tmp_path / "different-consumer-store.sqlite3"
        )
    try:
        result = dispatcher.tick()
    finally:
        dispatcher.close()
    assert result.status == "blocked"
    assert result.reason_code == "dispatcher_integrity_blocked"
    assert claims == 0


def test_dispatcher_rejects_route_without_persistent_output_instance(tmp_path):
    wall = MutableClock(1000.0)
    guarded = GuardedCompletionClock(wall)
    lifecycle = _lifecycle(tmp_path, wall)
    desired = _desired(tmp_path)
    adapters = _adapter_shells(lifecycle, guarded, desired)
    forged = adapters[ROUTE_ORDER[0]]
    forged.composition_store_bindings[0][
        "instance_authority_sha256"
    ] = None
    forged.composition_store_bindings[0][
        "persistent_instance_authority"
    ] = False
    forged.composition_store_bindings_sha256 = _digest(
        forged.composition_store_bindings
    )
    with pytest.raises(
        SerialCompletionDispatcherIntegrityError,
        match="output instance authority is unavailable",
    ):
        _dispatcher(tmp_path, lifecycle, guarded, adapters)


def test_route_store_same_path_replacement_blocks_live_and_restart(tmp_path):
    wall = MutableClock(1000.0)
    lifecycle = _lifecycle(tmp_path, wall)
    desired = _desired(tmp_path)
    guarded = GuardedCompletionClock(wall)
    adapters = _adapter_shells(lifecycle, guarded, desired)
    dispatcher = _dispatcher(
        tmp_path, lifecycle, guarded, adapters
    ).acquire()
    route_path = Path(
        adapters[ROUTE_ORDER[0]].composition_store_bindings[0]["path"]
    )
    replacement = tmp_path / "valid-empty-route-store.bin"
    replacement.write_bytes(b"")
    os.replace(replacement, route_path)
    assert dispatcher.tick().status == "blocked"
    dispatcher.close()

    restart_clock = GuardedCompletionClock(wall)
    with pytest.raises(SerialCompletionDispatcherIntegrityError):
        _dispatcher(
            tmp_path,
            lifecycle,
            restart_clock,
            _adapter_shells(lifecycle, restart_clock, desired),
        )


def test_desired_store_valid_empty_same_inode_replacement_blocks_restart(
    tmp_path,
):
    wall = MutableClock(1000.0)
    lifecycle = _lifecycle(tmp_path, wall)
    desired = _desired(tmp_path)
    original_identity = desired.identity_sha256
    original_instance = desired.store_instance_sha256
    desired_path = desired.path
    original_inode = desired_path.stat().st_ino
    guarded = GuardedCompletionClock(wall)
    dispatcher = _dispatcher(
        tmp_path,
        lifecycle,
        guarded,
        _adapter_shells(lifecycle, guarded, desired),
    ).acquire()
    assert dispatcher.close()

    replacement_path = tmp_path / "valid-empty-desired.sqlite3"
    replacement = DesiredStateStore(replacement_path.resolve())
    assert replacement.load_active() is None
    replacement_bytes = replacement_path.read_bytes()
    assert replacement.store_instance_sha256 != original_instance
    with desired_path.open("r+b") as target:
        target.seek(0)
        target.write(replacement_bytes)
        target.truncate()
        target.flush()
        os.fsync(target.fileno())
    assert desired_path.stat().st_ino == original_inode

    adopted = DesiredStateStore(desired_path)
    assert adopted.load_active() is None
    assert adopted.store_instance_sha256 == replacement.store_instance_sha256
    assert adopted.identity_sha256 != original_identity
    restart_clock = GuardedCompletionClock(wall)
    with pytest.raises(SerialCompletionDispatcherIntegrityError):
        _dispatcher(
            tmp_path,
            lifecycle,
            restart_clock,
            _adapter_shells(lifecycle, restart_clock, adopted),
        )


def test_route_store_hardlink_drift_blocks_before_lifecycle_claim(
    monkeypatch, tmp_path
):
    wall = MutableClock(1000.0)
    guarded = GuardedCompletionClock(wall)
    lifecycle = _lifecycle(tmp_path, wall)
    desired = _desired(tmp_path)
    claims = 0

    def forbidden_claim(*args, **kwargs):
        nonlocal claims
        claims += 1
        raise AssertionError("hardlinked route store reached lifecycle claim")

    monkeypatch.setattr(lifecycle, "claim_next", forbidden_claim)
    adapters = _adapter_shells(lifecycle, guarded, desired)
    dispatcher = _dispatcher(
        tmp_path, lifecycle, guarded, adapters
    ).acquire()
    route_path = Path(
        adapters[ROUTE_ORDER[0]].composition_store_bindings[0]["path"]
    )
    try:
        os.link(route_path, tmp_path / "route-store-alias.bin")
    except OSError as exc:
        dispatcher.close()
        pytest.skip(f"hardlink unavailable: {exc}")
    try:
        result = dispatcher.tick()
    finally:
        dispatcher.close()
    assert result.status == "blocked"
    assert result.reason_code == "dispatcher_integrity_blocked"
    assert claims == 0


@pytest.mark.parametrize("store_kind", ["lifecycle", "desired"])
@pytest.mark.parametrize("link_kind", ["symlink", "hardlink"])
def test_composition_database_links_fail_before_dispatcher_write(
    store_kind, link_kind, tmp_path
):
    wall = MutableClock(1000.0)
    lifecycle = _lifecycle(tmp_path, wall)
    desired = _desired(tmp_path)
    guarded = GuardedCompletionClock(wall)
    source = lifecycle.path if store_kind == "lifecycle" else desired.path
    external = tmp_path / f"{store_kind}-{link_kind}-external.sqlite3"
    if link_kind == "symlink":
        os.replace(source, external)
        try:
            os.symlink(external, source)
        except OSError as exc:
            os.replace(external, source)
            pytest.skip(f"composition symlink unavailable: {exc}")
    else:
        try:
            os.link(source, external)
        except OSError as exc:
            pytest.skip(f"composition hardlink unavailable: {exc}")
    sentinel = external.read_bytes()
    with pytest.raises(SerialCompletionDispatcherIntegrityError):
        _dispatcher(
            tmp_path,
            lifecycle,
            guarded,
            _adapter_shells(lifecycle, guarded, desired),
        )
    assert external.read_bytes() == sentinel
    assert not (tmp_path / "dispatcher.sqlite3").exists()
    assert not (tmp_path / "dispatcher.sqlite3.owner.lock").exists()


def test_linked_lifecycle_root_fails_before_dispatcher_write(tmp_path):
    wall = MutableClock(1000.0)
    lifecycle = _lifecycle(tmp_path, wall)
    desired = _desired(tmp_path)
    guarded = GuardedCompletionClock(wall)
    lexical_root = lifecycle.root
    external_root = tmp_path / "external-lifecycle-root"
    os.replace(lexical_root, external_root)
    try:
        os.symlink(external_root, lexical_root, target_is_directory=True)
    except OSError as exc:
        os.replace(external_root, lexical_root)
        pytest.skip(f"directory symlink unavailable: {exc}")
    external_database = external_root / lifecycle.path.name
    sentinel = external_database.read_bytes()
    with pytest.raises(SerialCompletionDispatcherIntegrityError):
        _dispatcher(
            tmp_path,
            lifecycle,
            guarded,
            _adapter_shells(lifecycle, guarded, desired),
        )
    assert external_database.read_bytes() == sentinel
    assert not (tmp_path / "dispatcher.sqlite3").exists()
    assert not (tmp_path / "dispatcher.sqlite3.owner.lock").exists()


def test_terminal_crash_marker_does_not_stall_next_real_lifecycle_event(
    monkeypatch, tmp_path
):
    wall = MutableClock(1000.0)
    lifecycle = _lifecycle(tmp_path, wall)
    desired = _desired(tmp_path)
    workload_id = _SINGLE_PHASE_TEST_WORKLOAD_ID
    binding = TERMINAL_RAW_CONSUMER_CATALOG[workload_id]
    producers = [
        lifecycle.publish(
            acquisition_job_id=f"dispatcher-real-{index}",
            consumer_id=binding.consumer_id,
            stream_id=binding.stream_id,
            desired_state_sha256=_sha("desired"),
            authority_sha256=_sha("authority"),
            proof={"schema": "dispatcher-real-proof-v1", "index": index},
            payload=f"private-{index}".encode("ascii"),
        )
        for index in (1, 2)
    ]
    processed = []

    def acknowledge(adapter, lease, *, permit, permit_nonce, cancel_event):
        permit.consume(
            workload_id=workload_id,
            phase="single",
            nonce=permit_nonce,
        )
        output_sha = _sha("output:" + lease.event_sha256)
        lifecycle.acknowledge(
            lease.event_id,
            lease.lease_token,
            output_sha256=output_sha,
            now=wall(),
        )
        processed.append(lease.event_sha256)
        return _ack_result(workload_id, lease.event_sha256)

    monkeypatch.setattr(_SinglePhaseAdapter, "begin", acknowledge)
    first_clock = GuardedCompletionClock(wall)
    first = _dispatcher(
        tmp_path,
        lifecycle,
        first_clock,
        _adapter_shells(lifecycle, first_clock, desired),
        fault_injector=CrashOnce("pending_finish.before_commit"),
    ).acquire()
    with pytest.raises(CrashOnce.Crash, match="simulated_process_crash"):
        first.tick()
    first.close()
    with closing(sqlite3.connect(first.path)) as db:
        assert db.execute(
            "SELECT pending_phase FROM dispatcher_state"
        ).fetchone()[0] == "initial"

    second_clock = GuardedCompletionClock(wall)
    second = _dispatcher(
        tmp_path,
        lifecycle,
        second_clock,
        _adapter_shells(lifecycle, second_clock, desired),
    ).acquire()
    try:
        result = second.tick()
        diagnostics = second.diagnostics()
    finally:
        second.close()
    assert result.status == "acknowledged"
    assert result.scanned_routes <= len(ROUTE_ORDER)
    assert diagnostics["terminalizations"] == 2
    assert processed == [producer.event_sha256 for producer in producers]


def test_ack_crash_reopen_uses_lifecycle_durable_clock_floor(
    monkeypatch, tmp_path
):
    wall = MutableClock(1000.0)
    lifecycle = _lifecycle(tmp_path, wall)
    desired = _desired(tmp_path)
    workload_id = _SINGLE_PHASE_TEST_WORKLOAD_ID
    binding = TERMINAL_RAW_CONSUMER_CATALOG[workload_id]
    lifecycle.publish(
        acquisition_job_id="dispatcher-clock-crash",
        consumer_id=binding.consumer_id,
        stream_id=binding.stream_id,
        desired_state_sha256=_sha("desired"),
        authority_sha256=_sha("authority"),
        proof={"schema": "dispatcher-clock-crash-proof-v1"},
        payload=b"private-clock-crash",
    )

    def acknowledge(adapter, lease, *, permit, permit_nonce, cancel_event):
        permit.consume(
            workload_id=workload_id,
            phase="single",
            nonce=permit_nonce,
        )
        wall.value = 1200.0
        lifecycle.acknowledge(
            lease.event_id,
            lease.lease_token,
            output_sha256=_sha("clock-crash-output"),
            now=wall(),
        )
        return _ack_result(workload_id, lease.event_sha256)

    monkeypatch.setattr(_SinglePhaseAdapter, "begin", acknowledge)
    first_clock = GuardedCompletionClock(wall)
    first = _dispatcher(
        tmp_path,
        lifecycle,
        first_clock,
        _adapter_shells(lifecycle, first_clock, desired),
        fault_injector=CrashOnce("pending_finish.before_commit"),
    ).acquire()
    with pytest.raises(CrashOnce.Crash):
        first.tick()
    first.close()
    assert lifecycle.durable_clock_high_water() == 1200.0

    wall.value = 1100.0
    restart_clock = GuardedCompletionClock(wall)
    restart = _dispatcher(
        tmp_path,
        lifecycle,
        restart_clock,
        _adapter_shells(lifecycle, restart_clock, desired),
    )
    with pytest.raises(SerialCompletionDispatcherClockRollback):
        restart.acquire()


def test_prewiring_blockers_are_hashed_and_machine_readable(
    monkeypatch, tmp_path
):
    assert PREWIRING_BLOCKERS == (
        "posix_completion_output_path_replace_os_policy_required",
    )
    wall = MutableClock(1000.0)
    guarded = GuardedCompletionClock(wall)
    lifecycle = _lifecycle(tmp_path, wall)
    desired = _desired(tmp_path)
    monkeypatch.setattr(lifecycle, "claim_next", lambda *args, **kwargs: None)
    dispatcher = _dispatcher(
        tmp_path,
        lifecycle,
        guarded,
        _adapter_shells(lifecycle, guarded, desired),
    ).acquire()
    try:
        diagnostics = dispatcher.diagnostics()
    finally:
        dispatcher.close()
    assert tuple(diagnostics["prewiring_blockers"]) == PREWIRING_BLOCKERS
    assert tuple(dispatcher.profile_mapping["prewiring_blockers"]) == (
        PREWIRING_BLOCKERS
    )
    assert diagnostics["runtime_wiring"] == "inert"


# These are logical SQLite process-crash boundaries.  They deliberately make
# no claim about power loss or storage-device cache durability.
@dataclass(frozen=True)
class _NativeDispatcherCrashCase:
    prefix: str
    edge: str

    @property
    def checkpoint(self) -> str:
        return f"{self.prefix}.{self.edge}"

    @property
    def test_id(self) -> str:
        return f"{self.prefix}-{self.edge}"


NATIVE_DISPATCHER_CRASH_CASES = (
    _NativeDispatcherCrashCase("owner_adopt", "before_commit"),
    _NativeDispatcherCrashCase("owner_adopt", "after_commit"),
    _NativeDispatcherCrashCase("tick_begin", "before_commit"),
    _NativeDispatcherCrashCase("tick_begin", "after_commit"),
    _NativeDispatcherCrashCase("integrity_fence", "before_commit"),
    _NativeDispatcherCrashCase("integrity_fence", "after_commit"),
    _NativeDispatcherCrashCase("claim_persist", "before_commit"),
    _NativeDispatcherCrashCase("claim_persist", "after_commit"),
    _NativeDispatcherCrashCase("continuation_persist", "before_commit"),
    _NativeDispatcherCrashCase("continuation_persist", "after_commit"),
    _NativeDispatcherCrashCase("pending_finish", "before_commit"),
    _NativeDispatcherCrashCase("pending_finish", "after_commit"),
    _NativeDispatcherCrashCase("idle_rotate", "before_commit"),
    _NativeDispatcherCrashCase("idle_rotate", "after_commit"),
)
_NATIVE_DISPATCHER_PREFIXES = (
    "owner_adopt",
    "tick_begin",
    "integrity_fence",
    "claim_persist",
    "continuation_persist",
    "pending_finish",
    "idle_rotate",
)
assert len(NATIVE_DISPATCHER_CRASH_CASES) == 14
assert len({case.checkpoint for case in NATIVE_DISPATCHER_CRASH_CASES}) == 14
assert tuple(dict.fromkeys(
    case.prefix for case in NATIVE_DISPATCHER_CRASH_CASES
)) == _NATIVE_DISPATCHER_PREFIXES
assert all(
    sum(case.prefix == prefix for case in NATIVE_DISPATCHER_CRASH_CASES) == 2
    for prefix in _NATIVE_DISPATCHER_PREFIXES
)

_NATIVE_DISPATCHER_ROOT = Path(__file__).resolve().parents[1]
_NATIVE_DISPATCHER_CHILD = (
    Path(__file__).resolve().parent
    / "helpers"
    / "worker_serial_completion_dispatcher_os_exit73_child.py"
)
_NATIVE_DISPATCHER_EXIT = 73
_NATIVE_DISPATCHER_CHILD_CLOCK = 1_000.0
_NATIVE_DISPATCHER_RECOVERY_CLOCK = 2_000.0
_NATIVE_DISPATCHER_JOB = "dispatcher-native-crash-event"
_NATIVE_DISPATCHER_PAYLOAD = b"dispatcher-native-crash-payload-v1"
_NATIVE_DISPATCHER_PROOF = {
    "schema": "stv3-test-native-dispatcher-crash-event-v1",
}
_NATIVE_DISPATCHER_MARKER = b"native-dispatcher-initial-child-v1\n"


def _native_dispatcher_case(checkpoint: str) -> _NativeDispatcherCrashCase:
    matches = [
        case for case in NATIVE_DISPATCHER_CRASH_CASES
        if case.checkpoint == checkpoint
    ]
    if len(matches) != 1:
        raise SystemExit("dispatcher crash checkpoint is not allowlisted")
    return matches[0]


def _native_dispatcher_event_workload(prefix: str) -> str | None:
    if prefix == "continuation_persist":
        return "satellite.noaa.err"
    if prefix in {
        "claim_persist", "integrity_fence", "pending_finish",
    }:
        return _SINGLE_PHASE_TEST_WORKLOAD_ID
    return None


def _native_dispatcher_publish(lifecycle, workload_id: str):
    binding = TERMINAL_RAW_CONSUMER_CATALOG[workload_id]
    return lifecycle.publish(
        acquisition_job_id=_NATIVE_DISPATCHER_JOB,
        consumer_id=binding.consumer_id,
        stream_id=binding.stream_id,
        desired_state_sha256=_sha("native-dispatcher-desired"),
        authority_sha256=_sha("native-dispatcher-authority"),
        proof=_NATIVE_DISPATCHER_PROOF,
        payload=_NATIVE_DISPATCHER_PAYLOAD,
    )


def _native_dispatcher_write_marker(path: Path) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    flags |= getattr(os, "O_BINARY", 0) | getattr(os, "O_CLOEXEC", 0)
    descriptor = os.open(path, flags, 0o600)
    try:
        os.write(descriptor, _NATIVE_DISPATCHER_MARKER)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _native_dispatcher_ack_result(
    workload_id: str,
    event_sha256: str,
    *,
    phase: str,
    children: int,
    output_sha256: str,
    consumption_receipt_sha256: str,
) -> AdapterPhaseResult:
    return AdapterPhaseResult(
        "acknowledged",
        "durable_shadow_output_acknowledged",
        workload_id,
        event_sha256,
        phase,
        children,
        "acknowledged",
        output_sha256,
        _sha("native-dispatcher-output-receipt:" + event_sha256),
        _sha("native-dispatcher-process-receipt:" + event_sha256),
        consumption_receipt_sha256,
    )


def run_serial_completion_dispatcher_os_exit73_child(
    root: Path,
    *,
    checkpoint: str,
) -> None:
    """Reach one closed dispatcher boundary and terminate the interpreter."""

    case = _native_dispatcher_case(checkpoint)
    wall = MutableClock(_NATIVE_DISPATCHER_CHILD_CLOCK)
    lifecycle = _lifecycle(root, wall)
    desired = _desired(root)
    guarded = GuardedCompletionClock(wall)
    adapters = _adapter_shells(lifecycle, guarded, desired)
    workload_id = _native_dispatcher_event_workload(case.prefix)

    if case.prefix == "integrity_fence":
        def fail_integrity(
            _adapter, _lease, *, permit, permit_nonce, cancel_event
        ):
            raise sqlite3.IntegrityError(
                "test-only exact dispatcher integrity failure"
            )

        _SinglePhaseAdapter.begin = fail_integrity
    elif case.prefix == "continuation_persist":
        marker = root / "native-dispatcher-initial-child.marker"

        def persist_initial(
            adapter, lease, *, permit, permit_nonce, cancel_event
        ):
            permit.consume(
                workload_id=adapter.workload_id,
                phase="initial",
                nonce=permit_nonce,
            )
            _native_dispatcher_write_marker(marker)
            return _continuation_result(adapter, lease)

        NoaaCompletionRouteAdapter.begin = persist_initial
    elif case.prefix == "pending_finish":
        def acknowledge(
            adapter, lease, *, permit, permit_nonce, cancel_event
        ):
            permit.consume(
                workload_id=adapter.workload_id,
                phase="single",
                nonce=permit_nonce,
            )
            output_sha256 = _sha(
                "native-dispatcher-output:" + lease.event_sha256
            )
            receipt = lifecycle.acknowledge(
                lease.event_id,
                lease.lease_token,
                output_sha256=output_sha256,
                now=wall(),
            )
            return _native_dispatcher_ack_result(
                adapter.workload_id,
                lease.event_sha256,
                phase="single",
                children=1,
                output_sha256=output_sha256,
                consumption_receipt_sha256=receipt.receipt_sha256,
            )

        _SinglePhaseAdapter.begin = acknowledge

    class ExitAtExactCheckpoint:
        def __init__(self, target: str) -> None:
            self.target = target
            self.hits = 0

        def __call__(self, point: str) -> None:
            if point == self.target:
                self.hits += 1
                if self.hits != 1:
                    raise AssertionError("dispatcher checkpoint repeated")
                os._exit(_NATIVE_DISPATCHER_EXIT)

    dispatcher = _dispatcher(
        root,
        lifecycle,
        guarded,
        adapters,
        fault_injector=ExitAtExactCheckpoint(case.checkpoint),
    )
    dispatcher.acquire()
    if case.prefix == "owner_adopt":
        raise RuntimeError("owner adoption checkpoint was not reached")
    result = dispatcher.tick()
    dispatcher.close()
    raise RuntimeError(
        "dispatcher crash checkpoint was not reached: "
        f"{case.checkpoint}; result={result.status}/{result.reason_code}; "
        f"workload={workload_id}"
    )


def _run_native_dispatcher_crash_child(
    root: Path, case: _NativeDispatcherCrashCase
) -> subprocess.CompletedProcess[str]:
    environment = dict(os.environ)
    python_path = [
        str(_NATIVE_DISPATCHER_ROOT),
        str(_NATIVE_DISPATCHER_ROOT / ".testdeps"),
    ]
    existing = environment.get("PYTHONPATH")
    if existing:
        python_path.append(existing)
    environment["PYTHONPATH"] = os.pathsep.join(python_path)
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    return subprocess.run(
        [
            sys.executable,
            "-B",
            str(_NATIVE_DISPATCHER_CHILD),
            str(root),
            case.checkpoint,
        ],
        cwd=_NATIVE_DISPATCHER_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        timeout=90.0,
        check=False,
    )


@pytest.mark.parametrize("root_kind", ("relative", "missing", "file"))
def test_native_dispatcher_crash_child_rejects_unsafe_roots_without_writes(
    tmp_path: Path,
    root_kind: str,
) -> None:
    if root_kind == "relative":
        supplied = "relative-dispatcher-root"
    elif root_kind == "missing":
        supplied = str((tmp_path / "missing").resolve())
    else:
        candidate = (tmp_path / "not-a-directory").resolve()
        candidate.write_bytes(b"test-only dispatcher root probe\n")
        supplied = str(candidate)
    before = _native_regular_file_inventory(tmp_path)
    completed = subprocess.run(
        [
            sys.executable,
            "-B",
            str(_NATIVE_DISPATCHER_CHILD),
            supplied,
            NATIVE_DISPATCHER_CRASH_CASES[0].checkpoint,
        ],
        cwd=_NATIVE_DISPATCHER_ROOT,
        env={
            **os.environ,
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONPATH": os.pathsep.join((
                str(_NATIVE_DISPATCHER_ROOT),
                str(_NATIVE_DISPATCHER_ROOT / ".testdeps"),
            )),
        },
        capture_output=True,
        text=True,
        timeout=30.0,
        check=False,
    )
    assert completed.returncode not in {0, _NATIVE_DISPATCHER_EXIT}
    assert _native_regular_file_inventory(tmp_path) == before


def _native_dispatcher_sql_state(root: Path) -> dict[str, object]:
    path = root / "dispatcher.sqlite3"
    with closing(sqlite3.connect(path)) as database:
        database.row_factory = sqlite3.Row
        assert database.execute("PRAGMA quick_check").fetchone()[0] == "ok"
        assert database.execute("PRAGMA foreign_key_check").fetchall() == []
        assert database.execute(
            "SELECT count(*) FROM dispatcher_state"
        ).fetchone()[0] == 1
        assert database.execute(
            "SELECT count(*) FROM manifest"
        ).fetchone()[0] == 6
        return dict(database.execute(
            "SELECT * FROM dispatcher_state WHERE singleton=1"
        ).fetchone())


def _native_dispatcher_state_projection(
    state: dict[str, object],
) -> dict[str, object]:
    owner = str(state["owner_epoch"])
    pending_owner = state["pending_owner_epoch"]
    return {
        "revision": int(state["revision"]),
        "last_observed_at": float(state["last_observed_at"]),
        "next_route_index": int(state["next_route_index"]),
        "owner_is_initial": owner == "0" * 64,
        "owner_is_sha256": len(owner) == 64 and all(
            character in "0123456789abcdef" for character in owner
        ),
        "integrity_fenced": int(state["integrity_fenced"]),
        "pending_workload_id": state["pending_workload_id"],
        "pending_event_sha256": state["pending_event_sha256"],
        "pending_stream_sequence": state["pending_stream_sequence"],
        "pending_phase": state["pending_phase"],
        "pending_lease_expires_at": state["pending_lease_expires_at"],
        "pending_owner_matches_owner": (
            pending_owner == owner if pending_owner is not None else None
        ),
        "ticks": int(state["ticks"]),
        "claims": int(state["claims"]),
        "idle_scans": int(state["idle_scans"]),
        "terminalizations": int(state["terminalizations"]),
        "retryable_results": int(state["retryable_results"]),
        "state_sha256_is_sha256": (
            len(str(state["state_sha256"])) == 64
            and all(
                character in "0123456789abcdef"
                for character in str(state["state_sha256"])
            )
        ),
    }


def _native_dispatcher_expected_crash_state(
    case: _NativeDispatcherCrashCase,
    event_sha256: str | None,
) -> dict[str, object]:
    values: dict[str, object] = {
        "revision": 0,
        "last_observed_at": 0.0,
        "next_route_index": 0,
        "owner_is_initial": True,
        "owner_is_sha256": True,
        "integrity_fenced": 0,
        "pending_workload_id": None,
        "pending_event_sha256": None,
        "pending_stream_sequence": None,
        "pending_phase": None,
        "pending_lease_expires_at": None,
        "pending_owner_matches_owner": None,
        "ticks": 0,
        "claims": 0,
        "idle_scans": 0,
        "terminalizations": 0,
        "retryable_results": 0,
        "state_sha256_is_sha256": True,
    }
    committed = case.edge == "after_commit"
    if case.prefix == "owner_adopt":
        if committed:
            values.update(
                revision=1,
                last_observed_at=_NATIVE_DISPATCHER_CHILD_CLOCK,
                owner_is_initial=False,
            )
        return values

    values.update(
        revision=1,
        last_observed_at=_NATIVE_DISPATCHER_CHILD_CLOCK,
        owner_is_initial=False,
    )
    if case.prefix == "tick_begin":
        if committed:
            values.update(revision=2, ticks=1)
        return values

    values.update(revision=2, ticks=1)
    if case.prefix == "idle_rotate":
        if committed:
            values.update(revision=3, next_route_index=1, idle_scans=1)
        return values

    workload_id = _native_dispatcher_event_workload(case.prefix)
    assert workload_id is not None and event_sha256 is not None
    route_index = ROUTE_ORDER.index(workload_id)
    if case.prefix == "claim_persist":
        if committed:
            values.update(
                revision=3,
                next_route_index=(route_index + 1) % len(ROUTE_ORDER),
                claims=1,
                pending_workload_id=workload_id,
                pending_event_sha256=event_sha256,
                pending_stream_sequence=1,
                pending_phase="initial",
                pending_lease_expires_at=(
                    _NATIVE_DISPATCHER_CHILD_CLOCK
                    + DEFAULT_ROUTE_LEASE_SECONDS
                ),
                pending_owner_matches_owner=True,
            )
        return values

    values.update(
        revision=3,
        next_route_index=(route_index + 1) % len(ROUTE_ORDER),
        claims=1,
        pending_workload_id=workload_id,
        pending_event_sha256=event_sha256,
        pending_stream_sequence=1,
        pending_phase="initial",
        pending_lease_expires_at=(
            _NATIVE_DISPATCHER_CHILD_CLOCK + DEFAULT_ROUTE_LEASE_SECONDS
        ),
        pending_owner_matches_owner=True,
    )
    if case.prefix == "integrity_fence" and committed:
        values.update(revision=4, integrity_fenced=1)
    elif case.prefix == "continuation_persist" and committed:
        values.update(revision=4, pending_phase="final_reproof")
    elif case.prefix == "pending_finish" and committed:
        values.update(
            revision=4,
            pending_workload_id=None,
            pending_event_sha256=None,
            pending_stream_sequence=None,
            pending_phase=None,
            pending_lease_expires_at=None,
            pending_owner_matches_owner=None,
            terminalizations=1,
        )
    return values


def _native_lifecycle_projection(lifecycle) -> dict[str, object]:
    with closing(sqlite3.connect(lifecycle.path)) as database:
        database.row_factory = sqlite3.Row
        assert database.execute("PRAGMA quick_check").fetchone()[0] == "ok"
        assert database.execute("PRAGMA foreign_key_check").fetchall() == []
        states = {
            str(row["state"]): int(row["quantity"])
            for row in database.execute(
                "SELECT state,count(*) AS quantity FROM completion_outbox "
                "GROUP BY state ORDER BY state"
            )
        }
        attempts = [
            int(row[0]) for row in database.execute(
                "SELECT attempt_count FROM completion_outbox "
                "ORDER BY stream_id,stream_sequence"
            )
        ]
        return {
            "outbox": int(database.execute(
                "SELECT count(*) FROM completion_outbox"
            ).fetchone()[0]),
            "producer_receipts": int(database.execute(
                "SELECT count(*) FROM producer_receipts"
            ).fetchone()[0]),
            "consumption_receipts": int(database.execute(
                "SELECT count(*) FROM consumption_receipts"
            ).fetchone()[0]),
            "hot_objects": int(database.execute(
                "SELECT count(*) FROM hot_objects"
            ).fetchone()[0]),
            "states": states,
            "attempts": attempts,
        }


def _native_regular_file_inventory(directory: Path) -> tuple[tuple[object, ...], ...]:
    if not directory.exists():
        return ()
    return tuple(
        (
            path.relative_to(directory).as_posix(),
            path.stat().st_size,
            hashlib.sha256(path.read_bytes()).hexdigest(),
        )
        for path in sorted(directory.rglob("*"))
        if path.is_file()
    )


def _native_route_store_inventory(root: Path) -> tuple[tuple[object, ...], ...]:
    return tuple(
        (
            path.name,
            path.stat().st_size,
            hashlib.sha256(path.read_bytes()).hexdigest(),
        )
        for path in sorted(root.glob("dispatcher-route-store-*.bin"))
    )


def _native_expected_lifecycle_after_crash(
    case: _NativeDispatcherCrashCase,
) -> dict[str, object]:
    workload_id = _native_dispatcher_event_workload(case.prefix)
    if workload_id is None:
        return {
            "outbox": 0,
            "producer_receipts": 0,
            "consumption_receipts": 0,
            "hot_objects": 0,
            "states": {},
            "attempts": [],
        }
    terminal = case.prefix == "pending_finish"
    return {
        "outbox": 1,
        "producer_receipts": 1,
        "consumption_receipts": int(terminal),
        "hot_objects": 1,
        "states": {"consumed" if terminal else "leased": 1},
        "attempts": [1],
    }


def _native_assert_final_state(
    case: _NativeDispatcherCrashCase,
    state: dict[str, object],
    event_sha256: str | None,
) -> None:
    projection = _native_dispatcher_state_projection(state)
    crash_revision = int(
        _native_dispatcher_expected_crash_state(case, event_sha256)["revision"]
    )
    if case.prefix == "integrity_fence" and case.edge == "after_commit":
        workload_id = _native_dispatcher_event_workload(case.prefix)
        assert workload_id is not None
        expected_revision = crash_revision + 1
        expected = {
            "revision": expected_revision,
            "ticks": 1,
            "claims": 1,
            "idle_scans": 0,
            "terminalizations": 0,
            "next_route_index": _next_route_index(workload_id),
            "integrity_fenced": 1,
            "pending_workload_id": workload_id,
            "pending_event_sha256": event_sha256,
            "pending_stream_sequence": 1,
            "pending_phase": "initial",
            "pending_lease_expires_at": (
                _NATIVE_DISPATCHER_CHILD_CLOCK
                + DEFAULT_ROUTE_LEASE_SECONDS
            ),
        }
    elif case.prefix in {"owner_adopt", "tick_begin", "idle_rotate"}:
        expected = {
            "revision": crash_revision + 3,
            "ticks": int(
                _native_dispatcher_expected_crash_state(
                    case, event_sha256
                )["ticks"]
            ) + 1,
            "claims": 0,
            "idle_scans": int(
                _native_dispatcher_expected_crash_state(
                    case, event_sha256
                )["idle_scans"]
            ) + 1,
            "terminalizations": 0,
            "next_route_index": (
                int(_native_dispatcher_expected_crash_state(
                    case, event_sha256
                )["next_route_index"]) + 1
            ) % len(ROUTE_ORDER),
            "integrity_fenced": 0,
            "pending_phase": None,
        }
    elif case.prefix == "claim_persist":
        workload_id = _native_dispatcher_event_workload(case.prefix)
        assert workload_id is not None
        expected = {
            "revision": crash_revision + (
                5 if case.edge == "after_commit" else 4
            ),
            "ticks": 2,
            "claims": 2 if case.edge == "after_commit" else 1,
            "idle_scans": 0,
            "terminalizations": 1,
            "next_route_index": _next_route_index(workload_id),
            "integrity_fenced": 0,
            "pending_phase": None,
        }
    elif case.prefix == "integrity_fence":
        workload_id = _native_dispatcher_event_workload(case.prefix)
        assert workload_id is not None
        expected = {
            "revision": crash_revision + 5,
            "ticks": 2,
            "claims": 2,
            "idle_scans": 0,
            "terminalizations": 1,
            "next_route_index": _next_route_index(workload_id),
            "integrity_fenced": 0,
            "pending_phase": None,
        }
    elif case.prefix == "continuation_persist":
        workload_id = _native_dispatcher_event_workload(case.prefix)
        assert workload_id is not None
        expected = {
            "revision": crash_revision + 7,
            "ticks": 3,
            "claims": 2,
            "idle_scans": 0,
            "terminalizations": 1,
            "next_route_index": _next_route_index(workload_id),
            "integrity_fenced": 0,
            "pending_phase": None,
        }
    else:
        assert case.prefix == "pending_finish"
        workload_id = _native_dispatcher_event_workload(case.prefix)
        assert workload_id is not None
        expected = {
            "revision": 7,
            "ticks": 2,
            "claims": 1,
            "idle_scans": 1,
            "terminalizations": 1,
            "next_route_index": (
                _next_route_index(workload_id) + 1
            ) % len(ROUTE_ORDER),
            "integrity_fenced": 0,
            "pending_phase": None,
        }
    for key, value in expected.items():
        assert projection[key] == value, (case, key, projection)
    assert projection["last_observed_at"] == _NATIVE_DISPATCHER_RECOVERY_CLOCK
    assert projection["owner_is_initial"] is False
    assert projection["owner_is_sha256"] is True
    assert projection["retryable_results"] == 0
    assert projection["state_sha256_is_sha256"] is True
    if expected["pending_phase"] is None:
        assert projection["pending_workload_id"] is None
        assert projection["pending_event_sha256"] is None
        assert projection["pending_stream_sequence"] is None
        assert projection["pending_lease_expires_at"] is None
        assert projection["pending_owner_matches_owner"] is None
    else:
        assert projection["pending_workload_id"] == (
            _native_dispatcher_event_workload(case.prefix)
        )
        assert projection["pending_event_sha256"] == event_sha256
        assert projection["pending_stream_sequence"] == 1
        assert projection["pending_lease_expires_at"] == (
            _NATIVE_DISPATCHER_CHILD_CLOCK
            + DEFAULT_ROUTE_LEASE_SECONDS
        )
        assert projection["pending_owner_matches_owner"] is False


@pytest.mark.parametrize(
    "case",
    NATIVE_DISPATCHER_CRASH_CASES,
    ids=[case.test_id for case in NATIVE_DISPATCHER_CRASH_CASES],
)
def test_native_serial_completion_dispatcher_process_crash_is_exactly_old_or_new(
    case: _NativeDispatcherCrashCase,
    monkeypatch,
    tmp_path: Path,
) -> None:
    """A real process death yields one exact old/new state, then safe replay."""

    setup_wall = MutableClock(_NATIVE_DISPATCHER_CHILD_CLOCK)
    setup_lifecycle = _lifecycle(tmp_path, setup_wall)
    setup_desired = _desired(tmp_path)
    setup_guarded = GuardedCompletionClock(setup_wall)
    _dispatcher(
        tmp_path,
        setup_lifecycle,
        setup_guarded,
        _adapter_shells(
            setup_lifecycle, setup_guarded, setup_desired
        ),
    )
    workload_id = _native_dispatcher_event_workload(case.prefix)
    producer = (
        _native_dispatcher_publish(setup_lifecycle, workload_id)
        if workload_id is not None else None
    )
    event_sha256 = producer.event_sha256 if producer is not None else None
    hot_inventory = _native_regular_file_inventory(
        setup_lifecycle.hot_directory
    )
    route_inventory = _native_route_store_inventory(tmp_path)
    assert len(route_inventory) == len(ROUTE_ORDER)
    assert all(size == 0 for _name, size, _sha256 in route_inventory)

    crashed = _run_native_dispatcher_crash_child(tmp_path, case)
    assert crashed.returncode == _NATIVE_DISPATCHER_EXIT, (
        crashed.stdout,
        crashed.stderr,
    )
    assert crashed.stdout == ""
    assert crashed.stderr == ""

    crash_state = _native_dispatcher_sql_state(tmp_path)
    assert _native_dispatcher_state_projection(crash_state) == (
        _native_dispatcher_expected_crash_state(case, event_sha256)
    )
    crashed_lifecycle = _lifecycle(
        tmp_path, MutableClock(_NATIVE_DISPATCHER_RECOVERY_CLOCK)
    )
    assert _native_lifecycle_projection(crashed_lifecycle) == (
        _native_expected_lifecycle_after_crash(case)
    )
    assert _native_regular_file_inventory(
        crashed_lifecycle.hot_directory
    ) == hot_inventory
    assert _native_route_store_inventory(tmp_path) == route_inventory

    marker = tmp_path / "native-dispatcher-initial-child.marker"
    marker_hash = None
    if case.prefix == "continuation_persist":
        assert marker.read_bytes() == _NATIVE_DISPATCHER_MARKER
        marker_hash = hashlib.sha256(marker.read_bytes()).hexdigest()
    else:
        assert not marker.exists()

    wall = MutableClock(_NATIVE_DISPATCHER_RECOVERY_CLOCK)
    lifecycle = _lifecycle(tmp_path, wall)
    desired = _desired(tmp_path)
    guarded = GuardedCompletionClock(wall)
    adapters = _adapter_shells(lifecycle, guarded, desired)
    adapter_calls = []
    claim_calls = []
    original_claim = lifecycle.claim_next

    def counted_claim(
        consumer_id, stream_id, owner_id, *, lease_seconds, now
    ):
        claim_calls.append((consumer_id, stream_id))
        return original_claim(
            consumer_id,
            stream_id,
            owner_id,
            lease_seconds=lease_seconds,
            now=now,
        )

    monkeypatch.setattr(lifecycle, "claim_next", counted_claim)

    def recover_single(
        adapter, lease, *, permit, permit_nonce, cancel_event
    ):
        adapter_calls.append((adapter.workload_id, lease.event_sha256, 1))
        permit.consume(
            workload_id=adapter.workload_id,
            phase="single",
            nonce=permit_nonce,
        )
        output_sha256 = _sha(
            "native-dispatcher-output:" + lease.event_sha256
        )
        receipt = lifecycle.acknowledge(
            lease.event_id,
            lease.lease_token,
            output_sha256=output_sha256,
            now=wall(),
        )
        return _native_dispatcher_ack_result(
            adapter.workload_id,
            lease.event_sha256,
            phase="single",
            children=1,
            output_sha256=output_sha256,
            consumption_receipt_sha256=receipt.receipt_sha256,
        )

    def recover_continuation_begin(
        adapter, lease, *, permit, permit_nonce, cancel_event
    ):
        assert marker.read_bytes() == _NATIVE_DISPATCHER_MARKER
        adapter_calls.append((
            "initial", adapter.workload_id, lease.event_sha256, 0,
        ))
        return _continuation_result(adapter, lease, children=0)

    def recover_continuation_finalize(
        adapter, continuation, *, permit, permit_nonce, cancel_event
    ):
        assert marker.read_bytes() == _NATIVE_DISPATCHER_MARKER
        lease = continuation.opaque_pending.lease
        adapter_calls.append((
            "final_reproof", adapter.workload_id, lease.event_sha256, 0,
        ))
        output_sha256 = _sha(
            "native-dispatcher-output:" + lease.event_sha256
        )
        receipt = lifecycle.acknowledge(
            lease.event_id,
            lease.lease_token,
            output_sha256=output_sha256,
            now=wall(),
        )
        return _native_dispatcher_ack_result(
            adapter.workload_id,
            lease.event_sha256,
            phase="final_reproof",
            children=0,
            output_sha256=output_sha256,
            consumption_receipt_sha256=receipt.receipt_sha256,
        )

    def forbidden_adapter(*args, **kwargs):
        raise AssertionError("recovery unexpectedly invoked an adapter")

    if case.prefix in {"claim_persist", "integrity_fence"}:
        if not (
            case.prefix == "integrity_fence"
            and case.edge == "after_commit"
        ):
            monkeypatch.setattr(_SinglePhaseAdapter, "begin", recover_single)
        else:
            monkeypatch.setattr(_SinglePhaseAdapter, "begin", forbidden_adapter)
    elif case.prefix == "continuation_persist":
        monkeypatch.setattr(
            NoaaCompletionRouteAdapter, "begin", recover_continuation_begin
        )
        monkeypatch.setattr(
            NoaaCompletionRouteAdapter,
            "finalize",
            recover_continuation_finalize,
        )
    elif case.prefix == "pending_finish":
        monkeypatch.setattr(_SinglePhaseAdapter, "begin", forbidden_adapter)

    recovered = _dispatcher(
        tmp_path, lifecycle, guarded, adapters
    ).acquire()
    try:
        result = recovered.tick()
        if case.prefix == "continuation_persist":
            assert result.status == "continuation_pending"
            assert result.reason_code == "final_reproof_pending"
            assert result.phase == "initial"
            assert result.child_invocations == 0
            result = recovered.tick()
        if (
            case.prefix == "integrity_fence"
            and case.edge == "after_commit"
        ):
            before_repeat = _native_dispatcher_sql_state(tmp_path)
            repeated = recovered.tick()
            after_repeat = _native_dispatcher_sql_state(tmp_path)
            assert repeated == result
            assert after_repeat == before_repeat
    finally:
        assert recovered.close()

    if case.prefix in {"claim_persist", "integrity_fence"} and not (
        case.prefix == "integrity_fence" and case.edge == "after_commit"
    ):
        assert result.status == "acknowledged"
        assert adapter_calls == [(workload_id, event_sha256, 1)]
    elif case.prefix == "continuation_persist":
        assert result.status == "acknowledged"
        assert result.phase == "final_reproof"
        assert result.child_invocations == 0
        assert adapter_calls == [
            ("initial", workload_id, event_sha256, 0),
            ("final_reproof", workload_id, event_sha256, 0),
        ]
    elif case.prefix == "integrity_fence":
        assert result.status == "blocked"
        assert result.reason_code == "dispatcher_integrity_fenced"
        assert adapter_calls == []
        assert claim_calls == []
    else:
        assert result.status == "idle"
        assert result.reason_code == "no_ready_completion"
        assert adapter_calls == []

    if result.status == "acknowledged":
        with closing(sqlite3.connect(lifecycle.path)) as database:
            receipt_row = database.execute(
                "SELECT receipt_sha256,output_sha256 "
                "FROM consumption_receipts"
            ).fetchone()
        assert receipt_row is not None
        assert result.output_sha256 == receipt_row[1]
        assert result.consumption_receipt_sha256 == receipt_row[0]
        assert result.output_sha256 == _sha(
            "native-dispatcher-output:" + str(event_sha256)
        )

    final_state = _native_dispatcher_sql_state(tmp_path)
    assert str(final_state["owner_epoch"]) != str(crash_state["owner_epoch"])
    _native_assert_final_state(case, final_state, event_sha256)
    final_lifecycle = _native_lifecycle_projection(lifecycle)
    if workload_id is None:
        assert final_lifecycle == _native_expected_lifecycle_after_crash(case)
        assert len(claim_calls) == len(ROUTE_ORDER)
    elif case.prefix == "integrity_fence" and case.edge == "after_commit":
        assert final_lifecycle == _native_expected_lifecycle_after_crash(case)
    else:
        expected_attempts = (
            1 if case.prefix == "pending_finish" else 2
        )
        assert final_lifecycle == {
            "outbox": 1,
            "producer_receipts": 1,
            "consumption_receipts": 1,
            "hot_objects": 1,
            "states": {"consumed": 1},
            "attempts": [expected_attempts],
        }
        binding = TERMINAL_RAW_CONSUMER_CATALOG[workload_id]
        identity = lifecycle.terminal_event_identity(binding.stream_id, 1)
        assert identity is not None
        assert identity.event_sha256 == event_sha256
        assert identity.terminal_kind == "consumed"
        expected_claims = (
            ROUTE_ORDER.index(workload_id) + 1
            if case.prefix == "claim_persist"
            and case.edge == "before_commit"
            else len(ROUTE_ORDER)
        )
        assert len(claim_calls) == expected_claims

    if producer is not None:
        before_replay = _native_lifecycle_projection(lifecycle)
        replay = _native_dispatcher_publish(lifecycle, workload_id)
        assert replay == producer
        assert _native_lifecycle_projection(lifecycle) == before_replay

    if marker_hash is not None:
        assert hashlib.sha256(marker.read_bytes()).hexdigest() == marker_hash
    assert _native_regular_file_inventory(
        lifecycle.hot_directory
    ) == hot_inventory
    assert _native_route_store_inventory(tmp_path) == route_inventory

    validation_lifecycle = _lifecycle(tmp_path, wall)
    validation_desired = _desired(tmp_path)
    validation_clock = GuardedCompletionClock(wall)
    validation = _dispatcher(
        tmp_path,
        validation_lifecycle,
        validation_clock,
        _adapter_shells(
            validation_lifecycle, validation_clock, validation_desired
        ),
    )
    state_before_diagnostics = _native_dispatcher_sql_state(tmp_path)
    assert state_before_diagnostics == final_state
    diagnostics = validation.diagnostics()
    assert diagnostics["integrity_fenced"] is bool(
        final_state["integrity_fenced"]
    )
    assert _native_dispatcher_sql_state(tmp_path) == state_before_diagnostics
    assert not tuple(tmp_path.rglob("*-journal"))
    assert not tuple(tmp_path.rglob("*-wal"))
    assert not tuple(tmp_path.rglob("*-shm"))
