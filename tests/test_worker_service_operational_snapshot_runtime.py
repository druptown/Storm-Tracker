from __future__ import annotations

import asyncio
import importlib
import sys
import threading
import time
import types

import pytest

from storm_tracker_worker.desired_state import DesiredStateStore
from storm_tracker_worker.operational_snapshot_publication import (
    PublicationOutboxStore,
)
from storm_tracker_worker.operational_snapshot_runtime import (
    OperationalSnapshotRuntime,
)
from storm_tracker_worker.operational_snapshot_store import (
    OperationalSnapshotStore,
)


NOW = 1_800_300_000.0


class FakeMessageInfo:
    def __init__(self, rc: int, *, acknowledged: bool, ack_error: bool = False):
        self.rc = rc
        self.acknowledged = threading.Event()
        if acknowledged:
            self.acknowledged.set()
        self.ack_error = ack_error

    def is_published(self):
        if self.ack_error:
            raise ValueError("fake_ack_error")
        return self.acknowledged.is_set()


class FakeClient:
    def __init__(self, **_kwargs):
        self.published = []
        self.snapshot_infos = []
        self.snapshot_rcs = []
        self.snapshot_auto_ack = True
        self.snapshot_ack_error = False
        self.subscriptions = []
        self.events = []
        self._lock = threading.Lock()

    def username_pw_set(self, *_args):
        pass

    def tls_set(self):
        pass

    def will_set(self, *args, **kwargs):
        self.will = (args, kwargs)

    def connect(self, *_args, **_kwargs):
        pass

    def subscribe(self, topic, qos=0):
        self.subscriptions.append((topic, qos))

    def publish(self, topic, payload, **kwargs):
        is_snapshot = topic.endswith("/operational/snapshot")
        with self._lock:
            rc = self.snapshot_rcs.pop(0) if (
                is_snapshot and self.snapshot_rcs
            ) else 0
            info = FakeMessageInfo(
                rc,
                acknowledged=(not is_snapshot or self.snapshot_auto_ack),
                ack_error=is_snapshot and self.snapshot_ack_error,
            )
            self.published.append((topic, payload, kwargs, info))
            self.events.append(("publish", topic, payload))
            if is_snapshot:
                self.snapshot_infos.append(info)
        return info

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
    mqtt_module.MQTT_ERR_SUCCESS = 0
    mqtt_module.MQTT_ERR_NO_CONN = 4
    mqtt_package = types.ModuleType("paho.mqtt")
    mqtt_package.client = mqtt_module
    paho_package = types.ModuleType("paho")
    paho_package.mqtt = mqtt_package
    monkeypatch.setitem(sys.modules, "paho", paho_package)
    monkeypatch.setitem(sys.modules, "paho.mqtt", mqtt_package)
    monkeypatch.setitem(sys.modules, "paho.mqtt.client", mqtt_module)
    sys.modules.pop("storm_tracker_worker.service", None)
    return importlib.import_module("storm_tracker_worker.service")


def _wait_for(predicate, *, timeout=4.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    assert predicate()


def _desired_document(authority_epoch: int) -> dict:
    return {
        "contract": "stv3-worker-desired-state-v1",
        "producer_id": "ha:service-runtime-test",
        "authority_epoch": authority_epoch,
        "generation": 1,
        "issued_at": NOW + 1,
        "previous_state_sha256": None,
        "targets": [{
            "target_id": "home",
            "display_name": "Thuis",
            "kind": "home",
            "available": True,
            "latitude": 50.8,
            "longitude": 4.2,
            "position_observed_at": NOW,
            "location_accuracy_m": 10.0,
            "observation_radius_km": 250.0,
            "country_code": "BE",
        }],
        "settings": {
            "ground_radius_km": 20.0,
            "lightning_source_mode": "auto",
            "model_guidance_enabled": True,
            "region_sharing_distance_km": 100.0,
        },
    }


def _configured_desired_store(path) -> DesiredStateStore:
    store = DesiredStateStore(path)
    claim = store.claim_producer(
        "ha:service-runtime-test",
        "service-runtime-claim",
        claimed_at=NOW,
    )
    store.apply(
        _desired_document(claim["authority_epoch"]),
        received_at=NOW + 1,
    )
    return store


def _runtime_factory(tmp_path, holder, *, prefix="runtime"):
    desired = _configured_desired_store(tmp_path / f"{prefix}-desired.sqlite3")
    snapshot = OperationalSnapshotStore(
        tmp_path / f"{prefix}-snapshots.sqlite3",
        clock=time.time,
    )
    outbox = PublicationOutboxStore(
        tmp_path / f"{prefix}-outbox.sqlite3",
        clock=time.time,
    )

    def factory(*, publisher, status_supplier):
        runtime = OperationalSnapshotRuntime(
            publisher=publisher,
            status_supplier=status_supplier,
            desired_state_store=desired,
            snapshot_store=snapshot,
            outbox_store=outbox,
            clock=time.time,
        )
        holder.update({
            "runtime": runtime,
            "desired": desired,
            "snapshot": snapshot,
            "outbox": outbox,
        })
        return runtime

    return factory


def _start(service):
    thread = threading.Thread(target=service.run)
    thread.start()
    _wait_for(lambda: ("loop_start",) in service.client.events)
    return thread


def _stop(service, thread):
    service.stop()
    thread.join(timeout=4)
    assert not thread.is_alive()


def _snapshot_publishes(client):
    return [
        item for item in client.published
        if item[0].endswith("/operational/snapshot")
    ]


def test_snapshot_publish_is_exact_qos1_retained_and_waits_for_puback(
    service_module,
    tmp_path,
):
    holder = {}
    service = service_module.WorkerService(
        host="mqtt",
        heartbeat_seconds=60,
        process_isolation=False,
        operational_snapshot_runtime_factory=_runtime_factory(tmp_path, holder),
    )
    service.client.snapshot_auto_ack = False
    thread = _start(service)
    try:
        service._on_connect(service.client, None, None, 0, None)
        _wait_for(lambda: len(_snapshot_publishes(service.client)) == 1)
        publish = _snapshot_publishes(service.client)[0]
        _wait_for(lambda: holder["outbox"].load().state == "pending")

        assert publish[0] == "storm_tracker/v1/operational/snapshot"
        assert publish[1] == holder["snapshot"].head()["canonical_envelope"]
        assert publish[2] == {"qos": 1, "retain": True}
        assert holder["outbox"].load().state == "pending"

        publish[3].acknowledged.set()
        _wait_for(lambda: holder["outbox"].load().state == "published")
    finally:
        _stop(service, thread)


def test_disconnect_then_reconnect_recovers_identical_pending_revision(
    service_module,
    tmp_path,
):
    holder = {}
    service = service_module.WorkerService(
        host="mqtt",
        heartbeat_seconds=60,
        process_isolation=False,
        operational_snapshot_runtime_factory=_runtime_factory(tmp_path, holder),
    )
    service.client.snapshot_auto_ack = False
    thread = _start(service)
    try:
        service._on_connect(service.client, None, None, 0, None)
        _wait_for(lambda: len(_snapshot_publishes(service.client)) == 1)
        _wait_for(lambda: holder["outbox"].load().state == "pending")
        exact = _snapshot_publishes(service.client)[0][1]

        service._on_disconnect(service.client, None, None, 7, None)
        _wait_for(
            lambda: holder["runtime"].diagnostics()["state"] == "degraded"
        )
        service.client.snapshot_auto_ack = True
        service._on_connect(service.client, None, None, 0, None)
        _wait_for(lambda: len(_snapshot_publishes(service.client)) == 2)
        _wait_for(lambda: holder["outbox"].load().state == "published")

        assert [item[1] for item in _snapshot_publishes(service.client)] == [
            exact,
            exact,
        ]
        assert holder["snapshot"].head()["revision"] == 1
        assert holder["runtime"].diagnostics()["counts"]["start_calls"] == 1
    finally:
        _stop(service, thread)


@pytest.mark.parametrize("failure", ("rc", "ack_error", "timeout"))
def test_mqtt_publish_failures_raise_without_transport_success(
    service_module,
    monkeypatch,
    failure,
):
    class NoopRuntime:
        def diagnostics(self):
            return {"state": "new", "last_error_code": None}

        async def async_start(self):
            return self.diagnostics()

        async def async_tick(self):
            return self.diagnostics()

        async def async_stop(self):
            return {"state": "stopped", "last_error_code": None}

    service = service_module.WorkerService(
        host="mqtt",
        process_isolation=False,
        operational_snapshot_runtime_factory=lambda **_kwargs: NoopRuntime(),
    )
    service._mqtt_connected.set()
    if failure == "rc":
        service.client.snapshot_rcs = [4]
    elif failure == "ack_error":
        service.client.snapshot_ack_error = True
    else:
        service.client.snapshot_auto_ack = False
        monkeypatch.setattr(
            service_module,
            "OPERATIONAL_SNAPSHOT_PUBLISH_TIMEOUT_SECONDS",
            0.03,
        )

    with pytest.raises(service_module._OperationalSnapshotMqttError):
        asyncio.run(service._publish_operational_snapshot(b"exact"))


def test_shutdown_cancels_unacked_publish_and_restart_recovers_exact_bytes(
    service_module,
    tmp_path,
):
    first_holder = {}
    factory = _runtime_factory(tmp_path, first_holder, prefix="restart")
    first = service_module.WorkerService(
        host="mqtt",
        heartbeat_seconds=60,
        process_isolation=False,
        operational_snapshot_runtime_factory=factory,
    )
    first.client.snapshot_auto_ack = False
    first_thread = _start(first)
    first._on_connect(first.client, None, None, 0, None)
    _wait_for(lambda: len(_snapshot_publishes(first.client)) == 1)
    exact = _snapshot_publishes(first.client)[0][1]
    _wait_for(lambda: first_holder["outbox"].load().state == "pending")

    started = time.monotonic()
    _stop(first, first_thread)
    assert time.monotonic() - started < 2.0
    assert first_holder["outbox"].load().state == "pending"
    first_health = first._status("offline")["operational_snapshot_runtime"]
    assert first_health["state"] == "stopped"
    assert first_health["closed"] is True
    assert first.client.events.index(("disconnect",)) > next(
        index
        for index, event in enumerate(first.client.events)
        if event[0] == "publish" and event[1].endswith("/operational/snapshot")
    )

    second_holder = {}

    def second_factory(*, publisher, status_supplier):
        runtime = OperationalSnapshotRuntime(
            publisher=publisher,
            status_supplier=status_supplier,
            desired_state_store=DesiredStateStore(
                first_holder["desired"].path
            ),
            snapshot_store=OperationalSnapshotStore(
                first_holder["snapshot"].path, clock=time.time
            ),
            outbox_store=PublicationOutboxStore(
                first_holder["outbox"].path, clock=time.time
            ),
            clock=time.time,
        )
        second_holder["runtime"] = runtime
        return runtime

    second = service_module.WorkerService(
        host="mqtt",
        heartbeat_seconds=60,
        process_isolation=False,
        operational_snapshot_runtime_factory=second_factory,
    )
    second_thread = _start(second)
    try:
        second._on_connect(second.client, None, None, 0, None)
        _wait_for(lambda: len(_snapshot_publishes(second.client)) == 1)
        assert _snapshot_publishes(second.client)[0][1] == exact
        assert OperationalSnapshotStore(
            first_holder["snapshot"].path, clock=time.time
        ).head()["revision"] == 1
    finally:
        _stop(second, second_thread)


def test_status_heartbeat_does_not_drive_heavy_runtime(
    service_module,
    monkeypatch,
):
    calls = []
    supplied = {}

    class RecordingRuntime:
        def diagnostics(self):
            return {
                "state": "new",
                "started": False,
                "closed": False,
                "last_error_code": None,
            }

        def _record(self, name):
            calls.append((name, threading.get_ident(), id(asyncio.get_running_loop())))
            return {
                "state": "stopped" if name == "stop" else "running",
                "started": name != "stop",
                "closed": name == "stop",
                "last_error_code": None,
            }

        async def async_start(self):
            return self._record("start")

        async def async_tick(self):
            return self._record("tick")

        async def async_stop(self):
            return self._record("stop")

    def factory(*, publisher, status_supplier):
        supplied["publisher"] = publisher
        supplied["status_supplier"] = status_supplier
        return RecordingRuntime()

    monkeypatch.setattr(
        service_module,
        "OPERATIONAL_SNAPSHOT_STATUS_WAKE_MIN_SECONDS",
        0.2,
    )
    service = service_module.WorkerService(
        host="mqtt",
        heartbeat_seconds=0.05,
        provider_cycle_seconds=60,
        process_isolation=False,
        operational_snapshot_runtime_factory=factory,
    )
    service.failed = service_module.OPERATIONAL_SNAPSHOT_MAX_COUNTER + 5
    service.jobs.put_nowait((3, 1, service_module.Job(
        job_id="queued",
        job_type="ping",
        deadline=time.time() + 10,
        payload={},
    )))
    assert supplied["status_supplier"]() == {
        "state": "online",
        "queue_depth": 1,
        "failed_jobs": service_module.OPERATIONAL_SNAPSHOT_MAX_COUNTER,
    }
    service.jobs.get_nowait()
    service.jobs.task_done()
    service.failed = 0
    thread = _start(service)
    try:
        service._on_connect(service.client, None, None, 1, None)
        time.sleep(0.05)
        assert not [call for call in calls if call[0] == "start"]

        service._on_connect(service.client, None, None, 0, None)
        _wait_for(lambda: len([call for call in calls if call[0] == "start"]) == 1)
        time.sleep(0.12)
        before = len([call for call in calls if call[0] == "tick"])
        for _ in range(50):
            service._publish_status("online")
        time.sleep(0.25)
        assert len([call for call in calls if call[0] == "tick"]) == before

        service.failed = 1
        service._publish_status("online")
        _wait_for(lambda: len([
            call for call in calls if call[0] == "tick"
        ]) == before + 1)

        service._on_disconnect(service.client, None, None, 7, None)
        service._on_connect(service.client, None, None, 0, None)
        _wait_for(
            lambda: len([call for call in calls if call[0] == "tick"]) > before + 1
        )
        assert len([call for call in calls if call[0] == "start"]) == 1

    finally:
        _stop(service, thread)

    lifecycle_calls = [call for call in calls if call[0] in {"start", "tick", "stop"}]
    assert len([call for call in lifecycle_calls if call[0] == "start"]) == 1
    assert len([call for call in lifecycle_calls if call[0] == "stop"]) == 1
    assert len({call[1] for call in lifecycle_calls}) == 1
    assert len({call[2] for call in lifecycle_calls}) == 1
