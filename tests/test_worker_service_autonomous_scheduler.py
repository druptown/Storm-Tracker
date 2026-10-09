"""Adversarial service wiring tests for the optional scheduler sidecar."""
from __future__ import annotations

from contextlib import closing
import importlib
import inspect
import json
import os
from pathlib import Path
import sqlite3
import sys
import threading
import time
import types

import pytest

from storm_tracker_worker import autonomous_scheduler as scheduler_module
from storm_tracker_worker import desired_state as desired_module


def scheduled_child_handler(job):
    """Spawn-importable proof that only the ordinary Job reaches the child."""
    marker = Path(job.payload["marker_path"])
    marker.write_text(
        json.dumps({
            "execution_context": job.execution_context,
            "job_id": job.job_id,
            "payload": job.payload,
            "pid": os.getpid(),
            "repr": repr(job),
        }, sort_keys=True),
        encoding="utf-8",
    )
    return {"pid": os.getpid(), "value": job.payload.get("value")}


class FakeClient:
    def __init__(self, **_kwargs):
        self.published = []
        self.subscriptions = []
        self.events = []

    def username_pw_set(self, *_args):
        pass

    def will_set(self, *args, **kwargs):
        self.will = (args, kwargs)

    def connect(self, *_args, **_kwargs):
        pass

    def subscribe(self, topic, qos=0):
        self.subscriptions.append((topic, qos))

    def publish(self, topic, payload, **kwargs):
        self.published.append((topic, payload, kwargs))
        self.events.append(("publish", topic, payload))

    def loop_start(self):
        self.events.append(("loop_start",))

    def loop_stop(self):
        self.events.append(("loop_stop",))

    def disconnect(self):
        self.events.append(("disconnect",))


@pytest.fixture()
def service_module(monkeypatch):
    mqtt_module = types.ModuleType("paho.mqtt.client")
    mqtt_module.Client = FakeClient
    mqtt_module.CallbackAPIVersion = types.SimpleNamespace(VERSION2=2)
    mqtt_module.MQTTv311 = 4
    mqtt_package = types.ModuleType("paho.mqtt")
    mqtt_package.client = mqtt_module
    paho_package = types.ModuleType("paho")
    paho_package.mqtt = mqtt_package
    monkeypatch.setitem(sys.modules, "paho", paho_package)
    monkeypatch.setitem(sys.modules, "paho.mqtt", mqtt_package)
    monkeypatch.setitem(sys.modules, "paho.mqtt.client", mqtt_module)
    sys.modules.pop("storm_tracker_worker.service", None)
    return importlib.import_module("storm_tracker_worker.service")


class MutableClock:
    def __init__(self, value: float):
        self.value = value

    def __call__(self) -> float:
        return self.value


def _configured_desired(tmp_path: Path, now: float):
    store = desired_module.DesiredStateStore(tmp_path / "desired.sqlite3")
    store.claim_producer(
        "ha-boot-a",
        "claim-a",
        claimed_at=now - 20.0,
    )
    store.apply(
        {
            "contract": desired_module.CONTRACT,
            "producer_id": "ha-boot-a",
            "authority_epoch": 1,
            "generation": 1,
            "issued_at": now - 10.0,
            "previous_state_sha256": None,
            "targets": [{
                "target_id": "zone.home",
                "display_name": "Thuis",
                "kind": "home",
                "available": True,
                "latitude": 51.04,
                "longitude": 4.48,
                "position_observed_at": now - 10.0,
                "location_accuracy_m": None,
                "observation_radius_km": 300.0,
                "country_code": "BE",
            }],
            "settings": {
                "ground_radius_km": 20.0,
                "lightning_source_mode": "auto",
                "model_guidance_enabled": False,
                "region_sharing_distance_km": 150.0,
            },
        },
        received_at=now - 5.0,
    )
    return store


def _intent(
    *,
    payload: dict | None = None,
    job_type: str = "scheduled_job",
):
    return scheduler_module.ScheduleIntent(
        intent_id="scheduled-cycle",
        job_type=job_type,
        cadence_seconds=3_600,
        deadline_seconds=3_600,
        priority=20,
        payload=payload or {"value": 42},
    )


def _scheduler(
    tmp_path: Path,
    clock: MutableClock,
    plan_builder,
    *,
    lease_seconds: float = 5.0,
    retry_base_seconds: float = 0.1,
):
    return scheduler_module.AutonomousScheduler(
        tmp_path / "scheduler.sqlite3",
        desired_state_store=_configured_desired(tmp_path, clock.value),
        plan_builder=plan_builder,
        clock=clock,
        lease_seconds=lease_seconds,
        retry_base_seconds=retry_base_seconds,
        retry_max_seconds=10.0,
    )


def _safe_effect(service_module):
    return service_module.HandlerEffectContract(
        contract_id="scheduled-test-effect-v1",
        effect_class="bounded_test_effect",
        hard_kill_safe=True,
        preparation="none",
        compensation="retry_exact_stable_job_id",
    )


def _request(service_module, job_id, job_type, payload, deadline):
    return types.SimpleNamespace(payload=service_module.encode(
        service_module.envelope(
            "job_request",
            job_id=job_id,
            job_type=job_type,
            deadline=deadline,
            payload=payload,
        )
    ))


def _outbox_row(path: Path) -> sqlite3.Row:
    with closing(sqlite3.connect(path)) as db:
        db.row_factory = sqlite3.Row
        return db.execute("SELECT * FROM scheduler_outbox").fetchone()


def _wait_for(predicate, timeout: float = 5.0) -> None:
    limit = time.time() + timeout
    while not predicate() and time.time() < limit:
        time.sleep(0.01)
    assert predicate()


def test_none_injection_preserves_existing_external_shape_and_lifecycle(
    service_module,
) -> None:
    implicit = service_module.WorkerService(
        host="mqtt",
        process_isolation=False,
    )
    explicit = service_module.WorkerService(
        host="mqtt",
        process_isolation=False,
        autonomous_scheduler=None,
    )
    assert set(implicit._status("online")) == set(explicit._status("online"))
    assert "autonomous_scheduler" not in implicit._status("online")
    assert set(implicit.processes) == set(explicit.processes)
    assert implicit._scheduler_tick() is False

    request = _request(
        service_module,
        "external-job",
        "ping",
        {"value": 1},
        time.time() + 30.0,
    )
    implicit._on_message(implicit.client, None, request)
    thread = threading.Thread(target=implicit._run_jobs)
    thread.start()
    _wait_for(lambda: implicit.completed == 1)
    implicit.stop()
    thread.join(timeout=2)
    states = [
        json.loads(payload)["state"]
        for topic, payload, _kwargs in implicit.client.published
        if topic.endswith("/jobs/external-job/status")
    ]
    assert states == ["queued", "running", "completed"]
    assert "autonomous_scheduler" not in inspect.getsource(service_module.main)


def test_scheduler_job_uses_normal_queue_effect_and_child_without_lease_token(
    service_module,
    tmp_path,
) -> None:
    clock = MutableClock(time.time())
    marker = tmp_path / "child-context.json"
    autonomous = _scheduler(
        tmp_path,
        clock,
        lambda _state: [_intent(payload={
            "marker_path": str(marker),
            "value": 42,
        })],
    )
    receipt_path = tmp_path / "receipts.sqlite3"
    service = service_module.WorkerService(
        host="mqtt",
        job_handlers={"scheduled_job": scheduled_child_handler},
        job_effect_contracts={"scheduled_job": _safe_effect(service_module)},
        receipt_path=receipt_path,
        autonomous_scheduler=autonomous,
    )

    assert service._scheduler_tick() is True
    queued = service.jobs.queue[0][2]
    with service._scheduler_lock:
        parent_lease = service._scheduler_lease
    assert parent_lease is not None
    token = parent_lease.lease_token
    assert token not in repr(parent_lease)
    assert token not in repr(queued)
    assert token not in json.dumps(queued.payload)
    assert queued.execution_context is None

    thread = threading.Thread(
        target=service._run_jobs,
        name="scheduled-serial-dispatch",
    )
    thread.start()
    _wait_for(lambda: autonomous.get_record(queued.job_id).state == "completed")
    service.stop()
    thread.join(timeout=4)

    child = json.loads(marker.read_text(encoding="utf-8"))
    assert child["pid"] != os.getpid()
    assert child["job_id"] == queued.job_id
    assert child["execution_context"] is None
    assert token not in json.dumps(child)
    assert token.encode() not in receipt_path.read_bytes()
    assert all(
        token not in str(payload)
        for _topic, payload, _kwargs in service.client.published
    )
    status = service._status("online")
    assert status["autonomous_scheduler"]["completed_jobs"] == 1
    process = next(
        value
        for value in status["processes"]
        if value["process_id"] == "scheduled_job"
    )
    assert process["effect_contract_id"] == "scheduled-test-effect-v1"


def test_duplicate_service_completion_replays_without_scheduler_reexecution(
    service_module,
    tmp_path,
) -> None:
    clock = MutableClock(time.time())
    executions = []

    def handler(job):
        executions.append(job.job_id)
        return {"ok": True}

    autonomous = _scheduler(
        tmp_path,
        clock,
        lambda _state: [_intent()],
    )
    service = service_module.WorkerService(
        host="mqtt",
        process_isolation=False,
        job_handlers={"scheduled_job": handler},
        receipt_path=tmp_path / "receipts.sqlite3",
        autonomous_scheduler=autonomous,
    )
    assert service._scheduler_tick()
    job = service.jobs.queue[0][2]
    thread = threading.Thread(target=service._run_jobs)
    thread.start()
    _wait_for(lambda: autonomous.get_record(job.job_id).state == "completed")

    service._on_message(
        service.client,
        None,
        _request(
            service_module,
            job.job_id,
            job.job_type,
            job.payload,
            job.deadline,
        ),
    )
    assert service._scheduler_tick() is False
    service.stop()
    thread.join(timeout=2)

    assert executions == [job.job_id]
    assert autonomous.get_record(job.job_id).state == "completed"
    terminal = [
        json.loads(payload)["state"]
        for topic, payload, _kwargs in service.client.published
        if topic.endswith(f"/jobs/{job.job_id}/status")
    ]
    assert terminal.count("completed") >= 1


def test_queue_full_retries_scheduler_without_receipt_or_mqtt_failure(
    service_module,
    tmp_path,
) -> None:
    clock = MutableClock(time.time())
    autonomous = _scheduler(
        tmp_path,
        clock,
        lambda _state: [_intent()],
    )
    receipt_path = tmp_path / "receipts.sqlite3"
    service = service_module.WorkerService(
        host="mqtt",
        queue_size=1,
        process_isolation=False,
        job_handlers={"scheduled_job": lambda _job: {}},
        receipt_path=receipt_path,
        autonomous_scheduler=autonomous,
    )
    service.jobs.put_nowait((
        3,
        1,
        service_module.Job(
            "existing", "ping", time.time() + 60.0, {}
        ),
    ))

    assert service._scheduler_tick() is False
    row = _outbox_row(autonomous.path)
    assert row["state"] == "retry"
    assert row["last_error_code"] == "service_queue_full"
    assert service.jobs.qsize() == 1
    assert service._status("online")["autonomous_scheduler"][
        "queue_full_events"
    ] == 1
    with closing(sqlite3.connect(receipt_path)) as db:
        assert db.execute(
            "SELECT count(*) FROM worker_job_receipts"
        ).fetchone()[0] == 0
    assert not any(
        "/jobs/autosched:" in topic
        for topic, _payload, _kwargs in service.client.published
    )


@pytest.mark.parametrize(
    ("terminal_state", "expected_code"),
    (
        ("failed", "service_job_failed"),
        ("expired", "service_job_expired"),
    ),
)
def test_failed_or_expired_service_terminal_becomes_exact_scheduler_retry(
    service_module,
    tmp_path,
    terminal_state,
    expected_code,
) -> None:
    clock = MutableClock(time.time())
    autonomous = _scheduler(
        tmp_path,
        clock,
        lambda _state: [_intent()],
    )
    service = service_module.WorkerService(
        host="mqtt",
        process_isolation=False,
        job_handlers={"scheduled_job": lambda _job: {}},
        receipt_path=tmp_path / "receipts.sqlite3",
        autonomous_scheduler=autonomous,
    )
    assert service._scheduler_tick()
    _, _, job = service.jobs.get_nowait()
    service.jobs.task_done()
    service._finalize_job(
        job,
        service_module._ExecutionOutcome(
            terminal_state,
            {"error": "bounded_test_failure"},
        ),
    )
    row = _outbox_row(autonomous.path)
    assert row["state"] == "retry"
    assert row["last_error_code"] == expected_code
    assert service._scheduler_lease is None


def test_shutdown_cancellation_safely_retries_scheduler_lease(
    service_module,
    tmp_path,
) -> None:
    clock = MutableClock(time.time())
    autonomous = _scheduler(
        tmp_path,
        clock,
        lambda _state: [_intent()],
    )
    service = service_module.WorkerService(
        host="mqtt",
        process_isolation=False,
        job_handlers={"scheduled_job": lambda _job: {}},
        receipt_path=tmp_path / "receipts.sqlite3",
        autonomous_scheduler=autonomous,
    )
    assert service._scheduler_tick()
    job_id = service.jobs.queue[0][2].job_id

    service.stop()
    service._cancel_queued_jobs()

    row = _outbox_row(autonomous.path)
    assert row["job_id"] == job_id
    assert row["state"] == "retry"
    assert row["last_error_code"] == "service_job_cancelled"
    assert service._scheduler_lease is None
    states = [
        json.loads(payload)["state"]
        for topic, payload, _kwargs in service.client.published
        if topic.endswith(f"/jobs/{job_id}/status")
    ]
    assert states[-1] == "cancelled"


def test_completed_receipt_closes_scheduler_after_restart_crash_window(
    service_module,
    tmp_path,
    monkeypatch,
) -> None:
    clock = MutableClock(time.time())
    executions = []

    def handler(job):
        executions.append(job.job_id)
        return {"ok": True}

    plan = lambda _state: [_intent()]
    first_scheduler = _scheduler(
        tmp_path,
        clock,
        plan,
        lease_seconds=2.0,
        retry_base_seconds=0.1,
    )
    receipt_path = tmp_path / "receipts.sqlite3"
    first = service_module.WorkerService(
        host="mqtt",
        process_isolation=False,
        job_handlers={"scheduled_job": handler},
        receipt_path=receipt_path,
        autonomous_scheduler=first_scheduler,
    )
    assert first._scheduler_tick()
    job_id = first.jobs.queue[0][2].job_id
    real_complete = first_scheduler.complete

    def lose_scheduler_completion(*_args, **_kwargs):
        raise OSError("simulated_crash_window")

    monkeypatch.setattr(first_scheduler, "complete", lose_scheduler_completion)
    thread = threading.Thread(target=first._run_jobs)
    thread.start()
    _wait_for(lambda: first.completed == 1)
    first.stop()
    thread.join(timeout=2)
    assert _outbox_row(first_scheduler.path)["state"] == "leased"
    with closing(sqlite3.connect(receipt_path)) as db:
        assert db.execute(
            "SELECT state FROM worker_job_receipts WHERE job_id=?",
            (job_id,),
        ).fetchone()[0] == "completed"
    monkeypatch.setattr(first_scheduler, "complete", real_complete)

    second_scheduler = scheduler_module.AutonomousScheduler(
        first_scheduler.path,
        desired_state_store=first_scheduler._desired_state_store,
        plan_builder=plan,
        clock=clock,
        lease_seconds=2.0,
        retry_base_seconds=0.1,
        retry_max_seconds=10.0,
    )
    second = service_module.WorkerService(
        host="mqtt",
        process_isolation=False,
        job_handlers={"scheduled_job": handler},
        receipt_path=receipt_path,
        autonomous_scheduler=second_scheduler,
    )
    leased = _outbox_row(second_scheduler.path)
    clock.value = float(leased["lease_expires_at"]) + 0.01
    assert second._scheduler_tick() is False
    retry = _outbox_row(second_scheduler.path)
    assert retry["state"] == "retry"
    clock.value = float(retry["next_attempt_at"])
    assert second._scheduler_tick() is False

    assert second_scheduler.get_record(job_id).state == "completed"
    assert executions == [job_id]
    health = second._status("online")["autonomous_scheduler"]
    assert health["receipt_replays"] == 1
    assert health["completed_jobs"] == 1
    assert health["lease_inflight"] is False


def test_terminal_receipt_has_one_scheduler_transition_under_race(
    service_module,
    tmp_path,
    monkeypatch,
) -> None:
    clock = MutableClock(time.time())
    autonomous = _scheduler(
        tmp_path,
        clock,
        lambda _state: [_intent()],
    )
    service = service_module.WorkerService(
        host="mqtt",
        process_isolation=False,
        job_handlers={"scheduled_job": lambda _job: {}},
        receipt_path=tmp_path / "receipts.sqlite3",
        autonomous_scheduler=autonomous,
    )
    assert service._scheduler_tick()
    job = service.jobs.queue[0][2]
    assert service._publish_job(job, "completed", result={})

    entered = threading.Event()
    release = threading.Event()
    calls = []
    real_complete = autonomous.complete

    def blocking_complete(job_id, lease_token, **kwargs):
        calls.append((job_id, lease_token))
        entered.set()
        assert release.wait(timeout=3)
        return real_complete(job_id, lease_token, **kwargs)

    monkeypatch.setattr(autonomous, "complete", blocking_complete)
    tick = threading.Thread(target=service._scheduler_tick)
    tick.start()
    assert entered.wait(timeout=2)

    service._scheduler_settle_active_lease(job, "completed")
    release.set()
    tick.join(timeout=3)

    assert not tick.is_alive()
    assert len(calls) == 1
    assert autonomous.get_record(job.job_id).state == "completed"
    health = service._status("online")["autonomous_scheduler"]
    assert health["completed_jobs"] == 1
    assert health["receipt_replays"] == 1
    assert health["lease_inflight"] is False


def test_run_owns_optional_scheduler_sidecar_lifecycle(
    service_module,
    tmp_path,
) -> None:
    clock = MutableClock(time.time())
    executions = []

    def handler(job):
        executions.append(job.job_id)
        return {"ok": True}

    autonomous = _scheduler(
        tmp_path,
        clock,
        lambda _state: [_intent()],
    )
    service = service_module.WorkerService(
        host="mqtt",
        heartbeat_seconds=2.0,
        process_isolation=False,
        job_handlers={"scheduled_job": handler},
        receipt_path=tmp_path / "receipts.sqlite3",
        autonomous_scheduler=autonomous,
        scheduler_poll_seconds=0.1,
    )
    run_thread = threading.Thread(target=service.run)
    run_thread.start()

    def completed() -> bool:
        try:
            with closing(sqlite3.connect(autonomous.path)) as db:
                row = db.execute(
                    "SELECT state FROM scheduler_outbox"
                ).fetchone()
        except sqlite3.OperationalError:
            return False
        return row is not None and row[0] == "completed"

    try:
        _wait_for(completed)
    finally:
        service.stop()
        run_thread.join(timeout=7)

    assert not run_thread.is_alive()
    assert len(executions) == 1
    loop_start_index = service.client.events.index(("loop_start",))
    queued_index = next(
        index
        for index, event in enumerate(service.client.events)
        if event[0] == "publish"
        and "/jobs/autosched:" in event[1]
        and json.loads(event[2])["state"] == "queued"
    )
    disconnect_index = service.client.events.index(("disconnect",))
    loop_stop_index = service.client.events.index(("loop_stop",))
    assert loop_start_index < queued_index < disconnect_index < loop_stop_index


def test_scheduler_failure_and_slow_plan_do_not_block_mqtt_or_service(
    service_module,
    tmp_path,
) -> None:
    clock = MutableClock(time.time())
    entered = threading.Event()
    release = threading.Event()

    def slow_broken_plan(_state):
        entered.set()
        assert release.wait(timeout=3)
        raise RuntimeError("private-plan-detail")

    autonomous = _scheduler(tmp_path, clock, slow_broken_plan)
    service = service_module.WorkerService(
        host="mqtt",
        process_isolation=False,
        job_handlers={"scheduled_job": lambda _job: {}},
        receipt_path=tmp_path / "receipts.sqlite3",
        autonomous_scheduler=autonomous,
        scheduler_poll_seconds=0.1,
    )
    scheduler_thread = threading.Thread(target=service._run_scheduler)
    scheduler_thread.start()
    assert entered.wait(timeout=2)

    started = time.monotonic()
    service._touch_process("mqtt", "active")
    service._publish_status("online")
    assert time.monotonic() - started < 0.2
    service._on_message(
        service.client,
        None,
        _request(
            service_module,
            "external-ping",
            "ping",
            {"test": True},
            time.time() + 30.0,
        ),
    )
    assert service.jobs.qsize() == 1

    release.set()
    _wait_for(
        lambda: service._status("online")["autonomous_scheduler"][
            "error_events"
        ] >= 1
    )
    service.stop()
    scheduler_thread.join(timeout=2)
    status = service._status("online")
    assert status["autonomous_scheduler"]["state"] == "error"
    assert status["autonomous_scheduler"]["error_code"] == (
        "scheduler_plan_invalid"
    )
    encoded = json.dumps(status)
    assert "private-plan-detail" not in encoded
    assert status["processes"][0]["process_id"] == "mqtt"
