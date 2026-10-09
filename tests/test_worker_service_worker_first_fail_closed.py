from __future__ import annotations

import asyncio
import importlib
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
import types

import pytest


ROOT = Path(__file__).resolve().parents[1]
_SUBPROCESS_ENV_KEYS = frozenset({
    "COMSPEC", "HOME", "LOCALAPPDATA", "PATH", "PATHEXT",
    "SYSTEMDRIVE", "SYSTEMROOT", "TEMP", "TMP", "TMPDIR",
    "USERPROFILE", "WINDIR",
})


class _MessageInfo:
    rc = 0

    @staticmethod
    def is_published() -> bool:
        return True


class _Client:
    def __init__(self, **_kwargs):
        self.published = []
        self.events = []
        self.subscriptions = []
        self.subscribe_rc = 0
        self.subscribe_error = None
        self.loop_start_error = None
        self.subscribe_message = None
        self.delivery_entered = threading.Event()
        self.delivery_thread = None

    def username_pw_set(self, *_args):
        pass

    def tls_set(self):
        pass

    def will_set(self, *_args, **_kwargs):
        pass

    def connect(self, *_args, **_kwargs):
        pass

    def subscribe(self, *args, **kwargs):
        self.subscriptions.append((args, kwargs))
        if self.subscribe_error is not None:
            raise self.subscribe_error
        if self.subscribe_message is not None:
            message = self.subscribe_message

            def deliver():
                self.delivery_entered.set()
                self.on_message(self, None, message)

            self.delivery_thread = threading.Thread(target=deliver)
            self.delivery_thread.start()
            assert self.delivery_entered.wait(timeout=1)
        return (self.subscribe_rc, len(self.subscriptions))

    def publish(self, topic, payload, **kwargs):
        self.published.append((topic, payload, kwargs))
        return _MessageInfo()

    def loop_start(self):
        self.events.append("loop_start")
        if self.loop_start_error is not None:
            raise self.loop_start_error

    def loop_stop(self):
        self.events.append("loop_stop")

    def disconnect(self):
        self.events.append("disconnect")


@pytest.fixture()
def service_module(monkeypatch):
    mqtt_module = types.ModuleType("paho.mqtt.client")
    mqtt_module.Client = _Client
    mqtt_module.CallbackAPIVersion = types.SimpleNamespace(VERSION2=2)
    mqtt_module.MQTTv311 = 4
    mqtt_module.MQTT_ERR_SUCCESS = 0
    mqtt_package = types.ModuleType("paho.mqtt")
    mqtt_package.client = mqtt_module
    paho_package = types.ModuleType("paho")
    paho_package.mqtt = mqtt_package
    monkeypatch.setitem(sys.modules, "paho", paho_package)
    monkeypatch.setitem(sys.modules, "paho.mqtt", mqtt_package)
    monkeypatch.setitem(sys.modules, "paho.mqtt.client", mqtt_module)
    sys.modules.pop("storm_tracker_worker.service", None)
    return importlib.import_module("storm_tracker_worker.service")


def _wait_for(predicate, *, timeout: float = 3.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    assert predicate()


def _request(module, job_id: str):
    payload = module.encode(module.envelope(
        "job_request",
        job_id=job_id,
        job_type="ping",
        deadline=time.time() + 30,
        payload={},
    ))
    return types.SimpleNamespace(payload=payload)


class _Runtime:
    def __init__(
        self,
        *,
        start_gate=None,
        start_error=None,
        tick_error=None,
        stop_pending=0,
        stop_gate=None,
        ready_state="running",
        tick_gates=None,
    ):
        self.start_gate = start_gate
        self.start_error = start_error
        self.tick_error = tick_error
        self.started = threading.Event()
        self.start_calls = 0
        self.tick_calls = 0
        self.stop_calls = 0
        self.stop_pending = stop_pending
        self.stop_gate = stop_gate
        self.ready_state = ready_state
        self._tick_gate_lock = threading.Lock()
        self.tick_gates = list(tick_gates or ())

    def queue_tick_gates(self, *gates):
        with self._tick_gate_lock:
            first_tick = self.tick_calls + 1
            self.tick_gates.extend(gates)
            return first_tick

    def diagnostics(self):
        return {"state": "new", "closed": False}

    def _ready_diagnostics(self):
        return {
            "acquisition_admission": {
                "allowed_workload_ids": ["radar.dwd.radolan_rv"],
                "policy": "exact_dwd_radolan_rv_only_v1",
            },
            "contract": "stv3-worker-first-service-composition-runtime-v2",
            "live_wiring": True,
            "meteorological_admission": False,
            "mode": "service_worker_first_canary_opt_in",
            "ownership_fence_held": True,
            "production_enable_ready": False,
            "science_mode": "shadow",
            "shutdown_complete": False,
            "state": self.ready_state,
        }

    async def async_start(self):
        self.start_calls += 1
        self.started.set()
        while self.start_gate is not None and not self.start_gate.is_set():
            await asyncio.sleep(0.01)
        if self.start_error is not None:
            raise self.start_error
        return self._ready_diagnostics()

    async def async_tick(self):
        with self._tick_gate_lock:
            self.tick_calls += 1
            tick_gate = self.tick_gates.pop(0) if self.tick_gates else None
        while tick_gate is not None and not tick_gate.is_set():
            await asyncio.sleep(0.01)
        if self.tick_error is not None:
            raise self.tick_error
        return self._ready_diagnostics()

    async def async_stop(self):
        self.stop_calls += 1
        while self.stop_gate is not None and not self.stop_gate.is_set():
            await asyncio.sleep(0.01)
        if self.stop_calls <= self.stop_pending:
            return {
                "state": "stop_pending",
                "heavy_lane_active": True,
                "shutdown_complete": False,
                "ownership_fence_held": True,
            }
        return {
            "state": "stopped",
            "closed": True,
            "heavy_lane_active": False,
            "shutdown_complete": True,
            "ownership_fence_held": False,
        }


def _service(module, runtime, *, worker_first=True):
    kwargs = (
        {"worker_first_runtime_factory": lambda **_kwargs: runtime}
        if worker_first
        else {"operational_snapshot_runtime_factory": lambda **_kwargs: runtime}
    )
    return module.WorkerService(
        host="mqtt",
        heartbeat_seconds=2,
        process_isolation=False,
        require_effect_contracts=True,
        **kwargs,
    )


def _run_in_thread(service):
    failures = []

    def target():
        try:
            service.run()
        except BaseException as err:
            failures.append(err)

    thread = threading.Thread(target=target)
    thread.start()
    _wait_for(lambda: "loop_start" in service.client.events)
    return thread, failures


def test_worker_first_admission_opens_only_after_successful_start(service_module):
    gate = threading.Event()
    runtime = _Runtime(start_gate=gate)
    service = _service(service_module, runtime)
    thread, failures = _run_in_thread(service)
    service._on_connect(service.client, None, None, 0, None)
    _wait_for(runtime.started.is_set)

    assert service._status("online")["job_admission_open"] is False
    assert service.client.subscriptions == []
    assert service._job_thread is None
    service._on_message(
        service.client,
        None,
        _request(service_module, "before-start"),
    )
    rejected = [
        json.loads(payload)
        for topic, payload, _kwargs in service.client.published
        if topic.endswith("/jobs/before-start/status")
    ]
    assert rejected == []
    assert "before-start" not in service._seen

    gate.set()
    _wait_for(lambda: service._status("online")["job_admission_open"])
    _wait_for(lambda: len(service.client.subscriptions) == 1)
    assert service._job_thread is not None
    assert service._job_thread.is_alive()
    service.stop()
    thread.join(timeout=4)
    assert not thread.is_alive()
    assert failures == []
    assert runtime.start_calls == runtime.stop_calls == 1


@pytest.mark.parametrize("operation", ("start", "tick"))
def test_worker_first_runtime_failure_is_fatal_and_releases_lifecycle(
    service_module,
    operation,
):
    secret = "must-not-appear-in-fixed-runtime-error"
    runtime = _Runtime(
        start_error=ValueError(secret) if operation == "start" else None,
        tick_error=ValueError(secret) if operation == "tick" else None,
    )
    service = _service(service_module, runtime)
    service._mqtt_connected.set()

    with pytest.raises(service_module._WorkerFirstRuntimeFatalError) as raised:
        service.run()

    assert secret not in str(raised.value)
    assert service.stop_event.is_set()
    assert service._status("offline")["job_admission_open"] is False
    assert service._status("offline")["job_admission_rejection_code"] == (
        "worker_first_runtime_failed"
    )
    assert runtime.stop_calls == 1
    assert service.client.events[-2:] == ["disconnect", "loop_stop"]


def test_operational_snapshot_failure_keeps_legacy_admission_open(service_module):
    runtime = _Runtime(start_error=ValueError("legacy-sidecar-failure"))
    service = _service(service_module, runtime, worker_first=False)
    thread, failures = _run_in_thread(service)
    service._on_connect(service.client, None, None, 0, None)
    _wait_for(lambda: runtime.start_calls == 1)
    _wait_for(
        lambda: service._status("online")["operational_snapshot_runtime"][
            "last_error_code"
        ] == "sidecar_operation_failed"
    )

    assert service._status("online")["job_admission_open"] is True
    assert not service.stop_event.is_set()
    service.stop()
    thread.join(timeout=4)
    assert not thread.is_alive()
    assert failures == []
    assert runtime.stop_calls == 1


def test_worker_first_retries_stop_pending_until_real_quiescence(service_module):
    runtime = _Runtime(stop_pending=2)
    service = _service(service_module, runtime)
    thread, failures = _run_in_thread(service)
    service._on_connect(service.client, None, None, 0, None)
    _wait_for(lambda: service._status("online")["job_admission_open"])

    service.stop()
    thread.join(timeout=4)
    assert not thread.is_alive()
    assert failures == []
    assert runtime.stop_calls == 3
    assert service._operational_snapshot_thread is not None
    assert not service._operational_snapshot_thread.is_alive()


def test_worker_first_pending_stop_is_bounded_fatal(
    service_module,
    monkeypatch,
):
    monkeypatch.setattr(service_module, "WORKER_FIRST_STOP_MAX_ATTEMPTS", 3)
    monkeypatch.setattr(service_module, "WORKER_FIRST_STOP_TOTAL_SECONDS", 0.2)
    monkeypatch.setattr(service_module, "WORKER_FIRST_STOP_RETRY_SECONDS", 0.001)
    runtime = _Runtime(stop_pending=100)
    service = _service(service_module, runtime)
    thread, failures = _run_in_thread(service)
    service._on_connect(service.client, None, None, 0, None)
    _wait_for(lambda: service._status("online")["job_admission_open"])

    service.stop()
    thread.join(timeout=4)
    assert not thread.is_alive()
    assert len(failures) == 1
    assert isinstance(
        failures[0], service_module._WorkerFirstRuntimeFatalError
    )
    assert str(failures[0]) == "worker_first_shutdown_incomplete"
    assert runtime.stop_calls == 3


def test_worker_first_blocked_stop_is_bounded_fatal(
    service_module,
    monkeypatch,
):
    monkeypatch.setattr(service_module, "WORKER_FIRST_STOP_TOTAL_SECONDS", 0.05)
    runtime = _Runtime(stop_gate=threading.Event())
    service = _service(service_module, runtime)
    thread, failures = _run_in_thread(service)
    service._on_connect(service.client, None, None, 0, None)
    _wait_for(lambda: service._status("online")["job_admission_open"])

    service.stop()
    thread.join(timeout=4)
    assert not thread.is_alive()
    assert len(failures) == 1
    assert isinstance(
        failures[0], service_module._WorkerFirstRuntimeFatalError
    )
    assert str(failures[0]) == "worker_first_shutdown_timeout"
    assert runtime.stop_calls == 1


def test_worker_first_rejects_non_ready_start_diagnostics(service_module):
    runtime = _Runtime(ready_state="degraded")
    service = _service(service_module, runtime)
    service._mqtt_connected.set()

    with pytest.raises(service_module._WorkerFirstRuntimeFatalError) as raised:
        service.run()

    assert str(raised.value) == "worker_first_start_not_ready"
    assert service.client.subscriptions == []
    assert service._job_thread is None
    assert runtime.stop_calls == 1


def test_worker_first_subscription_failure_is_fatal(service_module):
    runtime = _Runtime()
    service = _service(service_module, runtime)
    service.client.subscribe_rc = 4
    service._mqtt_connected.set()

    with pytest.raises(service_module._WorkerFirstRuntimeFatalError) as raised:
        service.run()

    assert str(raised.value) == "worker_first_subscription_failed"
    assert service._status("offline")["job_admission_open"] is False
    assert runtime.stop_calls == 1


def test_worker_first_subscription_exception_on_reconnect_is_fatal(
    service_module,
):
    secret = "secret-reconnect-subscription-detail"
    runtime = _Runtime()
    service = _service(service_module, runtime)
    thread, failures = _run_in_thread(service)
    service._on_connect(service.client, None, None, 0, None)
    _wait_for(lambda: service._status("online")["job_admission_open"])

    service._on_disconnect(service.client, None, None, 7, None)
    service.client.subscribe_error = ValueError(secret)
    service._on_connect(service.client, None, None, 0, None)

    _wait_for(service.stop_event.is_set)
    assert service._status("offline")["job_admission_open"] is False
    thread.join(timeout=4)
    assert not thread.is_alive()
    assert len(failures) == 1
    assert str(failures[0]) == "worker_first_subscription_failed"
    assert secret not in str(failures[0])


def test_worker_first_mqtt_loop_start_exception_is_fixed_and_releases_runtime(
    service_module,
):
    secret = "secret-loop-start-detail"
    runtime = _Runtime()
    service = _service(service_module, runtime)
    service.client.loop_start_error = ValueError(secret)

    with pytest.raises(service_module._WorkerFirstRuntimeFatalError) as raised:
        service.run()

    assert str(raised.value) == "worker_first_mqtt_loop_start_failed"
    assert secret not in str(raised.value)
    assert runtime.stop_calls == 1
    assert service.stop_event.is_set()
    assert service._status("offline")["job_admission_open"] is False


def test_worker_first_sidecar_thread_start_exception_is_fixed(
    service_module,
    monkeypatch,
):
    secret = "secret-sidecar-thread-start-detail"
    runtime = _Runtime()
    service = _service(service_module, runtime)
    original_start = threading.Thread.start

    def fail_sidecar_start(thread):
        if thread.name == "storm-tracker-operational-snapshot":
            raise RuntimeError(secret)
        return original_start(thread)

    monkeypatch.setattr(threading.Thread, "start", fail_sidecar_start)
    with pytest.raises(service_module._WorkerFirstRuntimeFatalError) as raised:
        service.run()

    assert str(raised.value) == "worker_first_sidecar_start_failed"
    assert secret not in str(raised.value)
    assert service._operational_snapshot_thread is None
    assert service.stop_event.is_set()


@pytest.mark.parametrize(
    "diagnostics",
    (
        {
            "state": "stopped",
            "shutdown_complete": True,
            "ownership_fence_held": False,
        },
        {
            "state": "stopped",
            "heavy_lane_active": None,
            "shutdown_complete": True,
            "ownership_fence_held": False,
        },
        {
            "state": "stopped",
            "heavy_lane_active": 0,
            "shutdown_complete": True,
            "ownership_fence_held": False,
        },
    ),
)
def test_worker_first_stop_requires_exact_false_heavy_lane(
    service_module,
    diagnostics,
):
    assert service_module.WorkerService._worker_first_stop_is_terminal(
        diagnostics
    ) is False


def test_legacy_connect_order_remains_subscribe_status_then_replay(
    service_module,
):
    service = _service(service_module, _Runtime(), worker_first=False)
    events = []
    service.client.subscribe = lambda *_args, **_kwargs: events.append(
        "subscribe"
    )
    service._publish_status = lambda *_args, **_kwargs: events.append(
        "status"
    )
    service._replay_recovered_terminal_statuses = lambda: events.append(
        "replay"
    )

    service._on_connect(service.client, None, None, 0, None)

    assert events == ["subscribe", "status", "replay"]
    service._on_disconnect(service.client, None, None, 7, None)
    assert service._status("online")["job_admission_open"] is True


@pytest.mark.parametrize(
    ("operation", "expected_events"),
    (
        ("publish", []),
        ("disconnect", ["publish"]),
        ("loop_stop", ["publish", "disconnect"]),
    ),
)
def test_legacy_cleanup_preserves_first_transport_exception(
    service_module,
    operation,
    expected_events,
):
    secret = f"legacy-{operation}-detail"
    runtime = _Runtime()
    service = _service(service_module, runtime, worker_first=False)
    events = []
    service._publish_status = lambda *_args, **_kwargs: (
        (_ for _ in ()).throw(ValueError(secret))
        if operation == "publish"
        else events.append("publish")
    )
    service.client.disconnect = lambda: (
        (_ for _ in ()).throw(ValueError(secret))
        if operation == "disconnect"
        else events.append("disconnect")
    )
    service.client.loop_stop = lambda: (
        (_ for _ in ()).throw(ValueError(secret))
        if operation == "loop_stop"
        else events.append("loop_stop")
    )
    service.client.loop_start = service.stop

    with pytest.raises(ValueError, match=secret):
        service.run()

    assert events == expected_events


def test_delivery_during_subscribe_waits_for_ready_and_is_not_lost(
    service_module,
):
    runtime = _Runtime()
    service = _service(service_module, runtime)
    service.client.subscribe_message = _request(
        service_module,
        "during-subscribe",
    )
    thread, failures = _run_in_thread(service)
    service._on_connect(service.client, None, None, 0, None)

    _wait_for(lambda: service.accepted_terminal == 1)
    service.client.delivery_thread.join(timeout=2)
    assert not service.client.delivery_thread.is_alive()
    states = [
        json.loads(payload)["state"]
        for topic, payload, _kwargs in service.client.published
        if topic.endswith("/jobs/during-subscribe/status")
    ]
    assert states == ["queued", "running", "completed"]
    assert service.accepted == service.accepted_terminal == 1

    service.stop()
    thread.join(timeout=4)
    assert not thread.is_alive()
    assert failures == []


def test_worker_first_reconnect_requires_fresh_proof_before_queued_qos(
    service_module,
):
    runtime = _Runtime()
    service = _service(service_module, runtime)
    thread, failures = _run_in_thread(service)
    service._on_connect(service.client, None, None, 0, None)
    _wait_for(lambda: service._status("online")["job_admission_open"])
    assert len(service.client.subscriptions) == 1

    stale_tick_gate = threading.Event()
    fresh_tick_gate = threading.Event()
    stale_tick = runtime.queue_tick_gates(
        stale_tick_gate,
        fresh_tick_gate,
    )
    fresh_tick = stale_tick + 1
    service._signal_operational_snapshot_runtime(immediate=True)
    _wait_for(lambda: runtime.tick_calls == stale_tick)

    service._on_disconnect(service.client, None, None, 7, None)
    assert service._worker_first_runtime_ready.is_set() is False
    assert service._status("online")["job_admission_open"] is False
    assert service._status("online")["job_admission_rejection_code"] == (
        "worker_first_runtime_not_ready"
    )

    service.client.subscribe_message = _request(
        service_module,
        "queued-on-reconnect",
    )
    service._on_connect(service.client, None, None, 0, None)
    assert len(service.client.subscriptions) == 1
    assert service._status("online")["job_admission_open"] is False

    stale_tick_gate.set()
    _wait_for(lambda: runtime.tick_calls == fresh_tick)
    assert len(service.client.subscriptions) == 1
    assert service._worker_first_runtime_ready.is_set() is False
    assert "queued-on-reconnect" not in service._seen

    fresh_tick_gate.set()
    _wait_for(lambda: service.accepted_terminal == 1)
    service.client.delivery_thread.join(timeout=2)
    assert not service.client.delivery_thread.is_alive()
    states = [
        json.loads(payload)["state"]
        for topic, payload, _kwargs in service.client.published
        if topic.endswith("/jobs/queued-on-reconnect/status")
    ]
    assert states == ["queued", "running", "completed"]
    assert len(service.client.subscriptions) == 2
    assert service._worker_first_runtime_ready.is_set() is True
    assert service.accepted == service.accepted_terminal == 1

    service.stop()
    thread.join(timeout=4)
    assert not thread.is_alive()
    assert failures == []


def test_worker_first_dispatcher_start_failure_keeps_cleanup_and_fixed_fatal(
    service_module,
    monkeypatch,
):
    runtime = _Runtime()
    service = _service(service_module, runtime)
    original_start = threading.Thread.start

    def fail_dispatcher_start(thread):
        if thread.name == "storm-tracker-jobs":
            raise RuntimeError("secret-dispatcher-start-detail")
        return original_start(thread)

    monkeypatch.setattr(threading.Thread, "start", fail_dispatcher_start)
    service._mqtt_connected.set()

    with pytest.raises(service_module._WorkerFirstRuntimeFatalError) as raised:
        service.run()

    assert str(raised.value) == "worker_first_dispatcher_start_failed"
    assert service._job_thread is None
    assert runtime.stop_calls == 1
    assert service.client.events[-2:] == ["disconnect", "loop_stop"]


@pytest.mark.parametrize("scenario", ("raised", "watchdog"))
def test_fatal_main_hard_exits_despite_noncooperative_work(
    service_module,
    scenario,
):
    environment = {
        key: value for key, value in os.environ.items()
        if key in _SUBPROCESS_ENV_KEYS
    }
    environment["PYTHONPATH"] = os.pathsep.join((
        str(ROOT / ".testdeps"),
        str(ROOT),
    ))
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    environment["PYTHONNOUSERSITE"] = "1"
    completed = subprocess.run(
        [
            sys.executable,
            "-B",
            str(ROOT / "tests/helpers/worker_service_fatal_exit_child.py"),
            scenario,
        ],
        cwd=ROOT,
        env=environment,
        capture_output=True,
        check=False,
        timeout=5,
    )

    assert completed.returncode == service_module.WORKER_FIRST_FATAL_EXIT_CODE
    assert completed.stdout == b""
    assert b"private-detail-must-not-be-printed" not in completed.stderr
