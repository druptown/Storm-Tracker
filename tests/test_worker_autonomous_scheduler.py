"""Adversarial tests for the worker-only autonomous scheduler foundation."""
from __future__ import annotations

import ast
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
import gc
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import warnings

import pytest

from storm_tracker_worker import autonomous_scheduler as scheduler
from storm_tracker_worker import desired_state as desired


NOW = 1_800_000_000.0


def _desired_value(
    *,
    generation: int = 1,
    previous: str | None = None,
    issued_at: float = NOW - 60.0,
    observed_at: float | None = None,
    kind: str = "home",
) -> dict:
    return {
        "contract": desired.CONTRACT,
        "producer_id": "ha-boot-a",
        "authority_epoch": 1,
        "generation": generation,
        "issued_at": issued_at,
        "previous_state_sha256": previous,
        "targets": [{
            "target_id": "zone.home",
            "display_name": "Thuis",
            "kind": kind,
            "available": True,
            "latitude": 51.04123,
            "longitude": 4.48123,
            "position_observed_at": (
                issued_at if observed_at is None else observed_at
            ),
            "location_accuracy_m": None if kind == "home" else 15.0,
            "observation_radius_km": 300.0,
            "country_code": "BE",
        }],
        "settings": {
            "ground_radius_km": 20.0,
            "lightning_source_mode": "auto",
            "model_guidance_enabled": False,
            "region_sharing_distance_km": 150.0,
        },
    }


def _configure(
    tmp_path: Path,
    *,
    issued_at: float = NOW - 60.0,
    received_at: float | None = None,
    observed_at: float | None = None,
    kind: str = "home",
) -> desired.DesiredStateStore:
    store = desired.DesiredStateStore(tmp_path / "desired.sqlite3")
    received = issued_at + 1.0 if received_at is None else received_at
    store.claim_producer(
        "ha-boot-a",
        "claim-a",
        claimed_at=max(0.0, issued_at - 10.0),
    )
    store.apply(
        _desired_value(
            issued_at=issued_at,
            observed_at=observed_at,
            kind=kind,
        ),
        received_at=received,
    )
    return store


def _intent(
    intent_id: str = "regional-radar",
    *,
    priority: int = 20,
    cadence: int = 300,
    deadline: int = 250,
    marker: str = "plan-a",
) -> scheduler.ScheduleIntent:
    return scheduler.ScheduleIntent(
        intent_id=intent_id,
        job_type="meteorological_cycle",
        cadence_seconds=cadence,
        deadline_seconds=deadline,
        priority=priority,
        payload={
            "credential_ref": "worker_environment",
            "latitude": 51.04123,
            "longitude": 4.48123,
            "marker": marker,
        },
    )


def _make_scheduler(
    tmp_path: Path,
    desired_store: desired.DesiredStateStore,
    plan_builder,
    **values,
) -> scheduler.AutonomousScheduler:
    return scheduler.AutonomousScheduler(
        tmp_path / "scheduler.sqlite3",
        desired_state_store=desired_store,
        plan_builder=plan_builder,
        **values,
    )


def _rows(path: Path) -> list[sqlite3.Row]:
    if not path.exists():
        return []
    with closing(sqlite3.connect(path)) as db:
        db.row_factory = sqlite3.Row
        return db.execute(
            "SELECT * FROM scheduler_outbox ORDER BY job_id"
        ).fetchall()


def test_idle_without_desired_state_never_calls_builder_or_creates_outbox(
    tmp_path,
) -> None:
    desired_store = desired.DesiredStateStore(tmp_path / "desired.sqlite3")
    calls = []

    def plan_builder(_state):
        calls.append(True)
        return [_intent()]

    engine = _make_scheduler(tmp_path, desired_store, plan_builder)
    result = engine.reconcile(now=NOW)

    assert result.status == "idle_unconfigured"
    assert result.inserted_count == 0
    assert calls == []
    assert engine.claim_next("serial-worker", now=NOW) is None
    assert not (tmp_path / "scheduler.sqlite3").exists()
    assert engine.diagnostics()["status"] == "idle"


def test_workload_identity_lookup_never_exposes_scheduler_payload(tmp_path):
    desired_store = _configure(tmp_path)
    intent = scheduler.ScheduleIntent(
        intent_id="dwd-radar",
        job_type="meteorological_acquisition",
        cadence_seconds=300,
        deadline_seconds=240,
        priority=20,
        payload={
            "workload_id": "radar.dwd.radolan_rv",
            "latitude": 51.04123,
            "longitude": 4.48123,
        },
    )
    engine = _make_scheduler(
        tmp_path,
        desired_store,
        lambda _state: [intent],
    )
    engine.reconcile(now=NOW)
    job_id = str(_rows(tmp_path / "scheduler.sqlite3")[0]["job_id"])

    assert engine.workload_id_for_job(job_id) == "radar.dwd.radolan_rv"
    assert engine.workload_id_for_job("unknown-job") is None
    assert engine.workload_ids_for_jobs((job_id, "unknown-job")) == {
        job_id: "radar.dwd.radolan_rv"
    }


def test_plan_is_durable_stable_idempotent_and_claims_only_one_serial_job(
    tmp_path,
) -> None:
    desired_store = _configure(tmp_path)

    def plan_builder(state):
        assert isinstance(state, desired.DesiredState)
        return [
            _intent("lower-priority", priority=20),
            _intent("highest-priority", priority=1),
        ]

    engine = _make_scheduler(tmp_path, desired_store, plan_builder)
    first = engine.reconcile(now=NOW)
    assert (first.inserted_count, first.idempotent_count) == (2, 0)
    retry = engine.reconcile(now=NOW + 1.0)
    assert (retry.inserted_count, retry.idempotent_count) == (0, 2)

    reopened = _make_scheduler(tmp_path, desired_store, plan_builder)
    claimed = reopened.claim_next("serial-worker", now=NOW + 2.0)
    assert claimed is not None
    assert claimed.priority == 1
    assert claimed.payload["marker"] == "plan-a"
    assert reopened.claim_next("serial-worker", now=NOW + 3.0) is None
    assert reopened.get_record(claimed.job_id).state == "leased"

    completed = reopened.complete(
        claimed.job_id,
        claimed.lease_token,
        now=NOW + 4.0,
    )
    assert completed.status == "completed"
    second = reopened.claim_next("serial-worker", now=NOW + 5.0)
    assert second is not None
    assert second.priority == 20
    assert second.job_id != claimed.job_id


def test_first_attempt_precedes_a_ready_higher_priority_retry(tmp_path) -> None:
    desired_store = _configure(tmp_path)
    engine = _make_scheduler(
        tmp_path,
        desired_store,
        lambda _state: [
            _intent("failing-high-priority", priority=1),
            _intent("uncaptured-lower-priority", priority=20),
        ],
        retry_base_seconds=2.0,
    )
    engine.reconcile(now=NOW)

    first = engine.claim_next("serial-worker", now=NOW + 1.0)
    assert first is not None and first.priority == 1
    retried = engine.retry(
        first.job_id,
        first.lease_token,
        "upstream_unavailable",
        now=NOW + 2.0,
    )
    assert retried.status == "retry"
    assert retried.next_attempt_at == NOW + 4.0

    uncaptured = engine.claim_next(
        "serial-worker",
        now=NOW + 4.0,
        prefer_unattempted=True,
    )
    assert uncaptured is not None
    assert uncaptured.priority == 20
    assert uncaptured.attempt_count == 1
    assert uncaptured.job_id != first.job_id


def test_older_uncaptured_slot_precedes_recurrent_high_priority_slot(
    tmp_path,
) -> None:
    desired_store = _configure(tmp_path)
    engine = _make_scheduler(
        tmp_path,
        desired_store,
        lambda _state: [
            _intent(
                "recurrent-high-priority",
                priority=1,
                cadence=10,
                deadline=100,
            ),
            _intent(
                "older-lower-priority",
                priority=20,
                cadence=100,
                deadline=100,
            ),
        ],
    )
    engine.reconcile(now=NOW)
    high = engine.claim_next(
        "serial-worker",
        now=NOW + 1.0,
        prefer_unattempted=True,
    )
    assert high is not None and high.priority == 1
    engine.complete(high.job_id, high.lease_token, now=NOW + 2.0)

    engine.reconcile(now=NOW + 10.0)
    older = engine.claim_next(
        "serial-worker",
        now=NOW + 11.0,
        prefer_unattempted=True,
    )
    assert older is not None
    assert older.priority == 20
    assert older.scheduled_for == NOW


@pytest.mark.parametrize(
    ("fault_point", "committed"),
    (
        ("reconcile.after_insert", False),
        ("reconcile.before_commit", False),
        ("reconcile.after_commit", True),
    ),
)
def test_reconcile_power_cut_before_or_after_commit_is_atomic_and_retryable(
    tmp_path,
    fault_point,
    committed,
) -> None:
    desired_store = _configure(tmp_path)

    def fault(point):
        if point == fault_point:
            raise RuntimeError("power_cut")

    broken = _make_scheduler(
        tmp_path,
        desired_store,
        lambda _state: [_intent()],
        fault_injector=fault,
    )
    with pytest.raises(RuntimeError, match="power_cut"):
        broken.reconcile(now=NOW)

    rows = _rows(tmp_path / "scheduler.sqlite3")
    assert (len(rows) == 1) is committed
    reopened = _make_scheduler(
        tmp_path,
        desired_store,
        lambda _state: [_intent()],
    )
    result = reopened.reconcile(now=NOW + 1.0)
    assert result.inserted_count == (0 if committed else 1)
    assert result.idempotent_count == (1 if committed else 0)
    assert len(_rows(tmp_path / "scheduler.sqlite3")) == 1


@pytest.mark.parametrize(
    ("fault_point", "leased"),
    (
        ("claim.after_update", False),
        ("claim.before_commit", False),
        ("claim.after_commit", True),
    ),
)
def test_claim_power_cut_recovers_same_stable_job_after_lease(
    tmp_path,
    fault_point,
    leased,
) -> None:
    desired_store = _configure(tmp_path)
    normal = _make_scheduler(
        tmp_path,
        desired_store,
        lambda _state: [_intent()],
        lease_seconds=10.0,
        retry_base_seconds=2.0,
    )
    normal.reconcile(now=NOW)
    original_job_id = _rows(tmp_path / "scheduler.sqlite3")[0]["job_id"]

    def fault(point):
        if point == fault_point:
            raise RuntimeError("power_cut")

    broken = _make_scheduler(
        tmp_path,
        desired_store,
        lambda _state: [_intent()],
        lease_seconds=10.0,
        retry_base_seconds=2.0,
        fault_injector=fault,
    )
    with pytest.raises(RuntimeError, match="power_cut"):
        broken.claim_next("serial-worker", now=NOW + 1.0)
    assert (_rows(tmp_path / "scheduler.sqlite3")[0]["state"] == "leased") is leased

    reopened = _make_scheduler(
        tmp_path,
        desired_store,
        lambda _state: [_intent()],
        lease_seconds=10.0,
        retry_base_seconds=2.0,
    )
    if not leased:
        recovered = reopened.claim_next("serial-worker", now=NOW + 2.0)
    else:
        assert reopened.claim_next("serial-worker", now=NOW + 5.0) is None
        # First call after lease expiry durably enters backoff.
        assert reopened.claim_next("serial-worker", now=NOW + 12.0) is None
        recovered = reopened.claim_next("serial-worker", now=NOW + 14.0)
    assert recovered is not None
    assert recovered.job_id == original_job_id
    assert recovered.attempt_count == (2 if leased else 1)


@pytest.mark.parametrize(
    ("operation", "checkpoint", "committed"),
    (
        ("reconcile", "reconcile.before_commit", False),
        ("reconcile", "reconcile.after_commit", True),
        ("claim", "claim.before_commit", False),
        ("claim", "claim.after_commit", True),
    ),
)
def test_real_process_power_cut_respects_sqlite_commit_boundary(
    tmp_path,
    operation,
    checkpoint,
    committed,
) -> None:
    desired_store = _configure(tmp_path)
    scheduler_path = tmp_path / "scheduler.sqlite3"
    if operation == "claim":
        _make_scheduler(
            tmp_path,
            desired_store,
            lambda _state: [_intent()],
            lease_seconds=10.0,
            retry_base_seconds=2.0,
        ).reconcile(now=NOW)
    helper = (
        Path(__file__).resolve().parent
        / "helpers"
        / "worker_autonomous_scheduler_crash_child.py"
    )
    environment = dict(os.environ)
    root = str(Path(__file__).resolve().parents[1])
    environment["PYTHONPATH"] = os.pathsep.join(
        value
        for value in (root, environment.get("PYTHONPATH"))
        if value
    )
    result = subprocess.run(
        [
            sys.executable,
            str(helper),
            str(scheduler_path),
            str(desired_store.path),
            operation,
            checkpoint,
            str(NOW if operation == "reconcile" else NOW + 1.0),
        ],
        cwd=Path(__file__).resolve().parents[1],
        env=environment,
        capture_output=True,
        check=False,
        timeout=20,
    )
    assert result.returncode == 79, result.stderr.decode(errors="replace")
    rows = _rows(scheduler_path)
    if operation == "reconcile":
        assert (len(rows) == 1) is committed
    else:
        assert len(rows) == 1
        assert (rows[0]["state"] == "leased") is committed


_SCHEDULER_ALL_NATIVE_CRASH_POINTS = {
    "reconcile": (
        "reconcile.after_insert",
        "reconcile.before_commit",
        "reconcile.after_commit",
    ),
    "claim_next": (
        "claim.after_update",
        "claim.before_commit",
        "claim.after_commit",
    ),
    "retry": (
        "retry.after_update",
        "retry.before_commit",
        "retry.after_commit",
    ),
    "complete": (
        "complete.after_update",
        "complete.before_commit",
        "complete.after_commit",
    ),
}
_SCHEDULER_LEGACY_NATIVE_CRASH_POINTS = frozenset({
    "reconcile.before_commit",
    "reconcile.after_commit",
    "claim.before_commit",
    "claim.after_commit",
})
_SCHEDULER_MISSING_NATIVE_CRASH_CASES = (
    ("reconcile", "reconcile.after_insert", False),
    ("claim", "claim.after_update", False),
    ("retry", "retry.after_update", False),
    ("retry", "retry.before_commit", False),
    ("retry", "retry.after_commit", True),
    ("complete", "complete.after_update", False),
    ("complete", "complete.before_commit", False),
    ("complete", "complete.after_commit", True),
)


def _scheduler_faultpoint_inventory() -> dict[str, tuple[str, ...]]:
    source = Path(scheduler.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(scheduler.__file__))
    scheduler_class = next(
        node
        for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == "AutonomousScheduler"
    )
    discovered: dict[str, tuple[str, ...]] = {}
    for method in scheduler_class.body:
        if not isinstance(method, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        calls = tuple(
            call
            for call in ast.walk(method)
            if (
                isinstance(call, ast.Call)
                and isinstance(call.func, ast.Attribute)
                and isinstance(call.func.value, ast.Name)
                and call.func.value.id == "self"
                and call.func.attr == "_fault"
                and len(call.args) == 1
                and isinstance(call.args[0], ast.Constant)
                and isinstance(call.args[0].value, str)
            )
        )
        if calls:
            discovered[method.name] = tuple(
                str(call.args[0].value)
                for call in sorted(
                    calls,
                    key=lambda item: (item.lineno, item.col_offset),
                )
            )
    return discovered


def test_scheduler_native_crash_inventory_pins_all_and_exact_missing_occurrences():
    discovered = _scheduler_faultpoint_inventory()
    assert discovered == _SCHEDULER_ALL_NATIVE_CRASH_POINTS
    all_points = frozenset(
        point for points in discovered.values() for point in points
    )
    missing = frozenset(
        point for _operation, point, _committed in _SCHEDULER_MISSING_NATIVE_CRASH_CASES
    )
    assert len(all_points) == sum(map(len, discovered.values())) == 12
    assert len(missing) == len(_SCHEDULER_MISSING_NATIVE_CRASH_CASES) == 8
    assert missing.isdisjoint(_SCHEDULER_LEGACY_NATIVE_CRASH_POINTS)
    assert missing | _SCHEDULER_LEGACY_NATIVE_CRASH_POINTS == all_points


@pytest.mark.parametrize(
    ("operation", "checkpoint", "committed"),
    _SCHEDULER_MISSING_NATIVE_CRASH_CASES,
    ids=(
        "reconcile-after-insert-old",
        "claim-after-update-old",
        "retry-after-update-old",
        "retry-before-commit-old",
        "retry-after-commit-new",
        "complete-after-update-old",
        "complete-before-commit-old",
        "complete-after-commit-new",
    ),
)
def test_missing_scheduler_native_crash_recovers_and_replays_exactly(
    tmp_path: Path,
    operation: str,
    checkpoint: str,
    committed: bool,
) -> None:
    desired_store = _configure(tmp_path)
    scheduler_path = (tmp_path / "scheduler.sqlite3").resolve()
    helper = (
        Path(__file__).resolve().parent
        / "helpers"
        / "worker_autonomous_scheduler_missing_crash_child.py"
    )
    environment = dict(os.environ)
    root = str(Path(__file__).resolve().parents[1])
    environment["PYTHONPATH"] = os.pathsep.join(
        value for value in (root, environment.get("PYTHONPATH", "")) if value
    )
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    crashed = subprocess.run(
        [
            sys.executable,
            str(helper),
            str(scheduler_path),
            str(desired_store.path),
            operation,
            checkpoint,
        ],
        cwd=root,
        env=environment,
        capture_output=True,
        check=False,
        timeout=30.0,
    )
    assert crashed.returncode == 73
    assert crashed.stdout == b""
    assert crashed.stderr == b""

    rows_at_crash = _rows(scheduler_path)
    if operation == "reconcile":
        assert rows_at_crash == []
    else:
        assert len(rows_at_crash) == 1
        crashed_row = rows_at_crash[0]
        assert int(crashed_row["attempt_count"]) == (0 if operation == "claim" else 1)
        if operation == "claim":
            expected_state = "pending"
        elif not committed:
            expected_state = "leased"
        else:
            expected_state = "retry" if operation == "retry" else "completed"
        assert crashed_row["state"] == expected_state
        if operation in {"retry", "complete"}:
            assert isinstance(crashed_row["last_attempt_token"], str)

    reopened = _make_scheduler(
        tmp_path,
        desired_store,
        lambda _state: [_intent()],
        lease_seconds=10.0,
        retry_base_seconds=2.0,
    )
    expected_job_id = (
        None if not rows_at_crash else str(rows_at_crash[0]["job_id"])
    )
    if operation == "reconcile":
        recovered = reopened.reconcile(now=NOW)
        assert (recovered.inserted_count, recovered.idempotent_count) == (1, 0)
        expected_job_id = str(_rows(scheduler_path)[0]["job_id"])
        replay = reopened.reconcile(now=NOW + 1.0)
        assert (replay.inserted_count, replay.idempotent_count) == (0, 1)
        final_row = _rows(scheduler_path)[0]
        assert final_row["state"] == "pending"
        assert int(final_row["attempt_count"]) == 0
    elif operation == "claim":
        recovered_claim = reopened.claim_next("serial-worker", now=NOW + 2.0)
        assert recovered_claim is not None
        assert recovered_claim.job_id == rows_at_crash[0]["job_id"]
        assert recovered_claim.attempt_count == 1
        assert reopened.claim_next("serial-worker", now=NOW + 3.0) is None
        assert reopened.complete(
            recovered_claim.job_id,
            recovered_claim.lease_token,
            now=NOW + 4.0,
        ).status == "completed"
        assert reopened.complete(
            recovered_claim.job_id,
            recovered_claim.lease_token,
            now=NOW + 5.0,
        ).status == "completed_idempotent"
        final_row = _rows(scheduler_path)[0]
        assert final_row["state"] == "completed"
    elif operation == "retry":
        token = str(rows_at_crash[0]["last_attempt_token"])
        recovered_retry = reopened.retry(
            str(rows_at_crash[0]["job_id"]),
            token,
            "upstream_timeout",
            now=NOW + 2.0,
        )
        assert recovered_retry.status == ("retry_idempotent" if committed else "retry")
        row_after_recovery = dict(_rows(scheduler_path)[0])
        replay = reopened.retry(
            str(rows_at_crash[0]["job_id"]),
            token,
            "upstream_timeout",
            now=NOW + 3.0,
        )
        assert replay.status == "retry_idempotent"
        assert replay.next_attempt_at == recovered_retry.next_attempt_at
        assert dict(_rows(scheduler_path)[0]) == row_after_recovery
        final_row = _rows(scheduler_path)[0]
        assert final_row["state"] == "retry"
        assert final_row["last_error_code"] == "upstream_timeout"
    else:
        token = str(rows_at_crash[0]["last_attempt_token"])
        recovered_complete = reopened.complete(
            str(rows_at_crash[0]["job_id"]),
            token,
            now=NOW + 2.0,
        )
        assert recovered_complete.status == (
            "completed_idempotent" if committed else "completed"
        )
        row_after_recovery = dict(_rows(scheduler_path)[0])
        replay = reopened.complete(
            str(rows_at_crash[0]["job_id"]),
            token,
            now=NOW + 3.0,
        )
        assert replay.status == "completed_idempotent"
        assert dict(_rows(scheduler_path)[0]) == row_after_recovery
        final_row = _rows(scheduler_path)[0]
        assert final_row["state"] == "completed"

    final_rows = _rows(scheduler_path)
    assert len(final_rows) == 1
    assert expected_job_id is not None
    assert final_rows[0]["job_id"] == expected_job_id
    diagnostics = reopened.diagnostics()
    assert sum(diagnostics["state_counts"].values()) == 1
    with closing(sqlite3.connect(scheduler_path)) as database:
        assert database.execute("PRAGMA integrity_check").fetchone()[0] == "ok"


def test_lease_expiry_and_explicit_retry_preserve_job_and_exactness(
    tmp_path,
) -> None:
    desired_store = _configure(tmp_path)
    engine = _make_scheduler(
        tmp_path,
        desired_store,
        lambda _state: [_intent()],
        lease_seconds=10.0,
        retry_base_seconds=2.0,
        retry_max_seconds=10.0,
    )
    engine.reconcile(now=NOW)
    first = engine.claim_next("serial-worker", now=NOW + 1.0)
    assert first is not None

    restarted = _make_scheduler(
        tmp_path,
        desired_store,
        lambda _state: [_intent()],
        lease_seconds=10.0,
        retry_base_seconds=2.0,
        retry_max_seconds=10.0,
    )
    assert restarted.claim_next("serial-worker", now=NOW + 11.0) is None
    retry_record = restarted.get_record(first.job_id)
    assert retry_record.state == "retry"
    assert retry_record.next_attempt_at == NOW + 13.0
    second = restarted.claim_next("serial-worker", now=NOW + 13.0)
    assert second.job_id == first.job_id
    assert second.lease_token != first.lease_token
    assert second.attempt_count == 2

    transition = restarted.retry(
        second.job_id,
        second.lease_token,
        "upstream_timeout",
        now=NOW + 14.0,
    )
    assert transition.status == "retry"
    assert transition.next_attempt_at == NOW + 18.0
    exact = restarted.retry(
        second.job_id,
        second.lease_token,
        "upstream_timeout",
        now=NOW + 15.0,
    )
    assert exact.status == "retry_idempotent"
    assert exact.next_attempt_at == transition.next_attempt_at
    with pytest.raises(scheduler.SchedulerConflict):
        restarted.retry(
            second.job_id,
            second.lease_token,
            "other_failure",
            now=NOW + 15.0,
        )

    third = restarted.claim_next("serial-worker", now=NOW + 18.0)
    assert third.job_id == first.job_id
    with pytest.raises(scheduler.SchedulerConflict):
        restarted.complete(
            third.job_id,
            second.lease_token,
            now=NOW + 19.0,
        )
    completed = restarted.complete(
        third.job_id,
        third.lease_token,
        now=NOW + 19.0,
    )
    assert completed.status == "completed"
    assert restarted.complete(
        third.job_id,
        third.lease_token,
        now=NOW + 20.0,
    ).status == "completed_idempotent"


def test_nondeterministic_plan_conflicts_without_mutating_existing_job(
    tmp_path,
) -> None:
    desired_store = _configure(tmp_path)
    first = _make_scheduler(
        tmp_path,
        desired_store,
        lambda _state: [_intent(marker="immutable-a")],
    )
    result = first.reconcile(now=NOW)
    job_id = _rows(tmp_path / "scheduler.sqlite3")[0]["job_id"]
    assert result.inserted_count == 1

    conflicting = _make_scheduler(
        tmp_path,
        desired_store,
        lambda _state: [_intent(marker="different-same-state")],
    )
    with pytest.raises(scheduler.SchedulerConflict, match="nondeterministic"):
        conflicting.reconcile(now=NOW + 1.0)
    row = _rows(tmp_path / "scheduler.sqlite3")[0]
    assert row["job_id"] == job_id
    assert json.loads(bytes(row["payload_json"]))["marker"] == "immutable-a"


def test_capacity_failure_is_atomic_and_preserves_existing_rows(tmp_path) -> None:
    desired_store = _configure(tmp_path)
    engine = _make_scheduler(
        tmp_path,
        desired_store,
        lambda _state: [_intent("a"), _intent("b")],
        max_outbox_rows=1,
    )
    with pytest.raises(scheduler.SchedulerCapacityError):
        engine.reconcile(now=NOW)
    assert _rows(tmp_path / "scheduler.sqlite3") == []

    one = _make_scheduler(
        tmp_path,
        desired_store,
        lambda _state: [_intent("a")],
        max_outbox_rows=1,
    )
    assert one.reconcile(now=NOW).inserted_count == 1
    with pytest.raises(scheduler.SchedulerCapacityError):
        one.reconcile(now=NOW + 300.0)
    rows = _rows(tmp_path / "scheduler.sqlite3")
    assert len(rows) == 1
    assert rows[0]["slot_index"] == int(NOW // 300)


def test_concurrent_claimers_can_create_only_one_active_lease(tmp_path) -> None:
    desired_store = _configure(tmp_path)
    plan = lambda _state: [_intent("a"), _intent("b")]
    engine = _make_scheduler(tmp_path, desired_store, plan)
    engine.reconcile(now=NOW)

    def claim(index):
        instance = _make_scheduler(tmp_path, desired_store, plan)
        return instance.claim_next(f"worker-{index}", now=NOW + 1.0)

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(claim, range(8)))
    claimed = [value for value in results if value is not None]
    assert len(claimed) == 1
    rows = _rows(tmp_path / "scheduler.sqlite3")
    assert sum(row["state"] == "leased" for row in rows) == 1
    assert sum(row["state"] == "pending" for row in rows) == 1


def test_backwards_clock_fails_closed_without_new_slot_or_lease(tmp_path) -> None:
    desired_store = _configure(tmp_path, issued_at=NOW - 200.0)
    engine = _make_scheduler(
        tmp_path,
        desired_store,
        lambda _state: [_intent(cadence=1, deadline=1)],
        clock_backward_tolerance_seconds=0.0,
    )
    engine.reconcile(now=NOW + 10.0)
    before = _rows(tmp_path / "scheduler.sqlite3")
    with pytest.raises(scheduler.SchedulerClockError) as error:
        engine.reconcile(now=NOW + 5.0)
    assert error.value.reason_code == "clock_regressed"
    assert len(_rows(tmp_path / "scheduler.sqlite3")) == len(before) == 1
    with pytest.raises(scheduler.SchedulerClockError):
        engine.claim_next("serial-worker", now=NOW + 5.0)


@pytest.mark.parametrize("failure", ("future", "stale_dynamic", "stale_global"))
def test_future_or_stale_desired_state_never_calls_builder_or_writes_outbox(
    tmp_path,
    failure,
) -> None:
    if failure == "future":
        issued = NOW + desired.MAX_CLOCK_FUTURE_SKEW_SECONDS + 10.0
        desired_store = _configure(tmp_path, issued_at=issued, received_at=issued)
        maximum_age = None
        expected = "desired_state_future"
    elif failure == "stale_dynamic":
        issued = NOW - desired.MAX_DYNAMIC_POSITION_AGE_SECONDS - 10.0
        desired_store = _configure(
            tmp_path,
            issued_at=issued,
            received_at=issued + 1.0,
            observed_at=issued,
            kind="person",
        )
        maximum_age = None
        expected = "desired_state_stale"
    else:
        issued = NOW - 10_000.0
        desired_store = _configure(
            tmp_path,
            issued_at=issued,
            received_at=issued + 1.0,
        )
        maximum_age = 100.0
        expected = "desired_state_stale"
    calls = []
    engine = _make_scheduler(
        tmp_path,
        desired_store,
        lambda _state: calls.append(True) or [_intent()],
        max_desired_state_age_seconds=maximum_age,
    )
    with pytest.raises(scheduler.SchedulerDesiredStateError) as error:
        engine.reconcile(now=NOW)
    assert error.value.reason_code == expected
    assert calls == []
    assert not (tmp_path / "scheduler.sqlite3").exists()


def test_desired_state_change_supersedes_unleased_work_and_creates_new_identity(
    tmp_path,
) -> None:
    desired_store = _configure(tmp_path)
    engine = _make_scheduler(
        tmp_path,
        desired_store,
        lambda state: [_intent(marker=f"generation-{state.generation}")],
    )
    first = engine.reconcile(now=NOW)
    old_job_id = _rows(tmp_path / "scheduler.sqlite3")[0]["job_id"]

    next_value = _desired_value(
        generation=2,
        previous=first.desired_state_sha256,
        issued_at=NOW + 1.0,
    )
    desired_store.apply(next_value, received_at=NOW + 2.0)
    updated = engine.reconcile(now=NOW + 2.0)
    rows = _rows(tmp_path / "scheduler.sqlite3")

    assert updated.inserted_count == 1
    assert updated.superseded_count == 1
    assert len(rows) == 2
    assert {row["state"] for row in rows} == {"pending", "superseded"}
    assert old_job_id != next(
        row["job_id"] for row in rows if row["state"] == "pending"
    )
    claimed = engine.claim_next("serial-worker", now=NOW + 3.0)
    assert claimed.desired_state_sha256 == updated.desired_state_sha256
    assert claimed.payload["marker"] == "generation-2"


def test_desired_refresh_preserves_first_pass_fairness_across_generations(
    tmp_path,
) -> None:
    desired_store = _configure(tmp_path)
    engine = _make_scheduler(
        tmp_path,
        desired_store,
        lambda _state: [
            _intent("already-captured", priority=1),
            _intent("still-uncaptured", priority=20),
        ],
    )
    first = engine.reconcile(now=NOW)
    captured = engine.claim_next(
        "serial-worker",
        now=NOW + 1.0,
        prefer_unattempted=True,
    )
    assert captured is not None
    assert captured.payload["marker"] == "plan-a"
    assert captured.priority == 1
    engine.complete(captured.job_id, captured.lease_token, now=NOW + 2.0)

    desired_store.apply(
        _desired_value(
            generation=2,
            previous=first.desired_state_sha256,
            issued_at=NOW + 3.0,
        ),
        received_at=NOW + 3.0,
    )
    engine.reconcile(now=NOW + 3.0)

    uncaptured = engine.claim_next(
        "serial-worker",
        now=NOW + 4.0,
        prefer_unattempted=True,
    )
    assert uncaptured is not None
    assert uncaptured.priority == 20


def test_desired_refresh_rotates_stable_intents_by_oldest_attempt(tmp_path) -> None:
    desired_store = _configure(tmp_path)
    engine = _make_scheduler(
        tmp_path,
        desired_store,
        lambda _state: [
            _intent("high-priority", priority=1),
            _intent("lower-priority", priority=20),
        ],
    )
    first = engine.reconcile(now=NOW)
    high = engine.claim_next(
        "serial-worker", now=NOW + 1.0, prefer_unattempted=True
    )
    assert high is not None and high.priority == 1
    engine.complete(high.job_id, high.lease_token, now=NOW + 2.0)
    low = engine.claim_next(
        "serial-worker", now=NOW + 3.0, prefer_unattempted=True
    )
    assert low is not None and low.priority == 20
    engine.complete(low.job_id, low.lease_token, now=NOW + 4.0)

    desired_store.apply(
        _desired_value(
            generation=2,
            previous=first.desired_state_sha256,
            issued_at=NOW + 5.0,
        ),
        received_at=NOW + 5.0,
    )
    second = engine.reconcile(now=NOW + 5.0)
    oldest = engine.claim_next(
        "serial-worker", now=NOW + 6.0, prefer_unattempted=True
    )
    assert oldest is not None and oldest.priority == 1
    engine.complete(oldest.job_id, oldest.lease_token, now=NOW + 7.0)

    desired_store.apply(
        _desired_value(
            generation=3,
            previous=second.desired_state_sha256,
            issued_at=NOW + 8.0,
        ),
        received_at=NOW + 8.0,
    )
    engine.reconcile(now=NOW + 8.0)
    rotated = engine.claim_next(
        "serial-worker", now=NOW + 9.0, prefer_unattempted=True
    )
    assert rotated is not None and rotated.priority == 20


def test_moving_target_refresh_preserves_fairness_by_stable_workload_id(
    tmp_path,
) -> None:
    desired_store = _configure(tmp_path, kind="device_tracker")

    def plan(state):
        return [
            scheduler.ScheduleIntent(
                intent_id=f"moving-radar-{state.generation}",
                job_type="meteorological_cycle",
                cadence_seconds=300,
                deadline_seconds=250,
                priority=1,
                payload={
                    "marker": f"generation-{state.generation}",
                    "workload_id": "radar.dwd.radolan_rv",
                },
            ),
            scheduler.ScheduleIntent(
                intent_id=f"moving-kmi-{state.generation}",
                job_type="meteorological_cycle",
                cadence_seconds=300,
                deadline_seconds=250,
                priority=20,
                payload={
                    "marker": f"generation-{state.generation}",
                    "workload_id": "radar.kmi.public_wcs_unknown_quantity",
                },
            ),
        ]

    engine = _make_scheduler(tmp_path, desired_store, plan)
    first = engine.reconcile(now=NOW)
    dwd = engine.claim_next(
        "serial-worker", now=NOW + 1.0, prefer_unattempted=True
    )
    assert dwd is not None
    assert dwd.payload["workload_id"] == "radar.dwd.radolan_rv"
    engine.complete(dwd.job_id, dwd.lease_token, now=NOW + 2.0)

    moved = _desired_value(
        generation=2,
        previous=first.desired_state_sha256,
        issued_at=NOW + 3.0,
        observed_at=NOW + 3.0,
        kind="device_tracker",
    )
    moved["targets"][0]["latitude"] = 51.15
    desired_store.apply(moved, received_at=NOW + 3.0)
    engine.reconcile(now=NOW + 3.0)

    kmi = engine.claim_next(
        "serial-worker", now=NOW + 4.0, prefer_unattempted=True
    )
    assert kmi is not None
    assert (
        kmi.payload["workload_id"]
        == "radar.kmi.public_wcs_unknown_quantity"
    )


def test_intent_and_plan_caps_fail_before_any_durable_admission(tmp_path) -> None:
    desired_store = _configure(tmp_path)
    invalid_plans = (
        lambda _state: [
            scheduler.ScheduleIntent(
                intent_id="secret-payload",
                job_type="cycle",
                cadence_seconds=60,
                deadline_seconds=30,
                priority=1,
                payload={"access_token": "must-never-persist"},
            )
        ],
        lambda _state: [_intent(marker="x" * scheduler.MAX_INTENT_PAYLOAD_BYTES)],
        lambda _state: [
            _intent(f"intent-{index}")
            for index in range(scheduler.MAX_PLAN_INTENTS + 1)
        ],
    )
    for index, plan in enumerate(invalid_plans):
        instance = scheduler.AutonomousScheduler(
            tmp_path / f"invalid-{index}.sqlite3",
            desired_state_store=desired_store,
            plan_builder=plan,
        )
        with pytest.raises(scheduler.ScheduleIntentError):
            instance.reconcile(now=NOW)
        assert not instance.path.exists()


def test_diagnostics_are_bounded_aggregate_only_and_hide_payload_coordinates(
    tmp_path,
) -> None:
    desired_store = _configure(tmp_path)
    engine = _make_scheduler(
        tmp_path,
        desired_store,
        lambda _state: [_intent(marker="private-diagnostic-marker")],
    )
    engine.reconcile(now=NOW)
    diagnostics = engine.diagnostics()
    encoded = json.dumps(diagnostics, sort_keys=True)

    assert len(encoded.encode("utf-8")) <= scheduler.MAX_DIAGNOSTIC_BYTES
    assert "private-diagnostic-marker" not in encoded
    assert "51.04123" not in encoded
    assert "4.48123" not in encoded
    assert "payload" not in encoded
    assert "job_id" not in encoded
    assert "intent_id" not in encoded
    assert diagnostics["state_counts"]["pending"] == 1


def test_module_has_no_ha_service_or_provider_execution_dependency() -> None:
    source_path = Path(scheduler.__file__)
    tree = ast.parse(source_path.read_text(encoding="utf-8"))
    imports = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imports.append(node.module or "")
    assert not any(name.startswith("homeassistant") for name in imports)
    assert not any(name.endswith("service") for name in imports)
    assert not any("provider" in name for name in imports)


def test_scheduler_connections_close_deterministically_and_on_setup_failure(
    tmp_path,
    monkeypatch,
) -> None:
    desired_store = _configure(tmp_path)
    engine = _make_scheduler(
        tmp_path,
        desired_store,
        lambda _state: [_intent()],
    )
    engine.reconcile(now=NOW)
    gc.collect()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", ResourceWarning)
        for _ in range(8):
            engine.diagnostics()
            engine.get_record(_rows(engine.path)[0]["job_id"])
        gc.collect()
        gc.collect()
    assert [
        warning
        for warning in caught
        if issubclass(warning.category, ResourceWarning)
    ] == []

    class FailingConnection:
        def __init__(self):
            self.closed = False
            self.row_factory = None

        def execute(self, _statement):
            raise RuntimeError("synthetic_pragma_failure")

        def close(self):
            self.closed = True

    connection = FailingConnection()
    monkeypatch.setattr(
        scheduler.sqlite3,
        "connect",
        lambda *_args, **_kwargs: connection,
    )
    failing = scheduler.AutonomousScheduler(
        tmp_path / "failing.sqlite3",
        desired_state_store=desired_store,
        plan_builder=lambda _state: [],
    )
    with pytest.raises(RuntimeError, match="synthetic_pragma_failure"):
        failing._connect()
    assert connection.closed
