from __future__ import annotations

from contextlib import closing
import importlib
import importlib.util
import json
import os
from pathlib import Path
import sqlite3
import sys
import threading
import time
import types

import pytest


_CHILD_EXECUTION_COUNT = 0


def process_test_handler(job):
    """Spawn-importable handler for process lifecycle tests."""
    global _CHILD_EXECUTION_COUNT
    _CHILD_EXECUTION_COUNT += 1
    marker = job.payload.get("marker")
    if marker:
        Path(marker).write_text("started", encoding="utf-8")
    sleep_seconds = float(job.payload.get("sleep_seconds", 0.0))
    if sleep_seconds > 0:
        time.sleep(sleep_seconds)
    if job.payload.get("crash_process"):
        os._exit(17)
    if job.payload.get("raise_error"):
        raise RuntimeError("test_handler_error")
    return {
        "execution": _CHILD_EXECUTION_COUNT,
        "pid": os.getpid(),
        "value": job.payload.get("value"),
    }


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


def _load_technical_projector():
    path = (
        Path(__file__).resolve().parents[1]
        / "custom_components"
        / "storm_tracker_v3"
        / "worker_technical_diagnostics.py"
    )
    name = "worker_service_technical_diagnostics_contract_test"
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def test_worker_status_publishes_exact_secret_free_technical_source(
    service_module,
    monkeypatch,
):
    sentinel = "SUPERS3CRETTOKEN-DO-NOT-LEAK"
    monkeypatch.setenv("STV3_NETATMO_CLIENT_SECRET", sentinel)
    service = service_module.WorkerService(
        host="mqtt", process_isolation=False
    )
    with service._admission_lock:
        service.accepted_terminal = 7
    service.failed = 2

    status = service._status("online")
    source = status["technical_diagnostics"]
    projector = _load_technical_projector()

    assert set(source) == service_module.WORKER_TECHNICAL_SOURCE_FIELDS
    assert set(source) == projector.SOURCE_FIELDS
    assert source["contract"] == projector.SOURCE_CONTRACT
    assert source["worker_state"] == "online"
    assert source["worker_version"] == service_module.__version__
    assert source["build_sha256"] == "sha256:" + service.build_sha256
    assert len(service.build_sha256) == 64
    assert service.build_id == service.build_sha256[:16]
    assert source["queue_depth"] == 0
    assert source["terminal_jobs"] == 7
    assert source["failed_jobs"] == 2
    assert source["config_sha256"] is None
    assert source["provider_status"] == "unknown"
    assert source["frame_status"] == "unknown"
    assert source["cache_status"] == "unknown"
    assert source["retention_alarm"] == "unknown"
    assert source["capacity_alarm"] == "unknown"
    assert source["transition_status"] == "unknown"
    assert source["current_version"] is None
    assert source["next_version"] is None
    assert source["replay_contract_version"] is None
    assert source["replay_algorithm_version"] is None
    normalized = projector.normalize_worker_technical_source(
        source,
        worker_state="online",
        observed_at=status["timestamp"],
    )
    assert normalized is not None
    projected = projector.build_worker_technical_diagnostics(
        normalized,
        now=status["timestamp"],
    )
    assert projected["status"] == "current"
    assert projected["jobs"]["terminal_jobs"] == 7
    wire = json.dumps(source, sort_keys=True)
    assert sentinel not in wire
    for forbidden in ("credential", "password", "token", "payload", "topic"):
        assert forbidden not in wire.lower()


def test_worker_status_exposes_applied_compute_profile(service_module):
    service = service_module.WorkerService(
        host="mqtt",
        process_isolation=False,
        compute_profile="dedicated_nas",
        cpu_limit_percent=50,
        worker_nice=10,
        cpu_threads=2,
    )

    status = service._status("online")

    assert status["compute_profile"] == "dedicated_nas"
    assert status["cpu_limit_percent"] == 50
    assert status["worker_nice"] == 10


def test_worker_accepts_explicit_fast_nas_resource_envelope(service_module):
    service = service_module.WorkerService(
        host="localhost",
        compute_profile="dedicated_nas_fast",
        cpu_limit_percent=200,
        worker_nice=0,
        cpu_threads=2,
        provider_cycle_seconds=2,
    )

    status = service._status("online")

    assert status["compute_profile"] == "dedicated_nas_fast"
    assert status["cpu_limit_percent"] == 200
    assert status["worker_nice"] == 0
    assert status["cpu_threads"] == 2
    assert status["provider_cycle_seconds"] == 2.0
    assert service.provider_cycle_seconds == 2.0


@pytest.mark.parametrize("compute_profile", ["shared_ha", "dedicated_nas"])
def test_conservative_profiles_keep_thirty_second_provider_floor(
    service_module, compute_profile
):
    service = service_module.WorkerService(
        host="localhost",
        compute_profile=compute_profile,
        provider_cycle_seconds=2,
    )

    assert service.provider_cycle_seconds == 30.0
    assert service._status("online")["provider_cycle_seconds"] == 30.0


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"compute_profile": "turbo"}, "worker-computeprofiel"),
        ({"cpu_limit_percent": 0}, "cpu_limit_percent"),
        ({"worker_nice": 20}, "worker_nice"),
        ({"cpu_threads": True}, "cpu_threads"),
    ],
)
def test_worker_compute_profile_rejects_invalid_metadata(
    service_module, kwargs, message
):
    with pytest.raises(ValueError, match=message):
        service_module.WorkerService(
            host="mqtt", process_isolation=False, **kwargs
        )


def test_worker_technical_source_exposes_only_exact_desired_state_hash(
    service_module,
):
    service = service_module.WorkerService(
        host="mqtt", process_isolation=False
    )
    with service._operational_snapshot_lock:
        service._operational_snapshot_health = {
            "last_tick": {"desired_state_sha256": "d" * 64}
        }
    source = service._status("busy")["technical_diagnostics"]
    assert source["desired_state_sha256"] == "sha256:" + "d" * 64

    with service._operational_snapshot_lock:
        service._operational_snapshot_health = {
            "last_tick": {"desired_state_sha256": "not-a-hash"}
        }
    source = service._status("busy")["technical_diagnostics"]
    assert source["desired_state_sha256"] is None


def _technical_retention_guard(service_module, *, alarms=()):
    gibibyte = 1024 * 1024 * 1024
    capacity = {
        "cadence_catalog_headroom_bytes": 300,
        "cadence_capture_headroom": 400,
        "cadence_capture_max_entries": 1_000,
        "cadence_cycle_reserve_bytes": 50,
        "cadence_cycle_reserve_captures": 5,
        "cadence_cycle_reserve_entries": 5,
        "cadence_ledger_byte_headroom": 400,
        "cadence_ledger_catalog_headroom_bytes": 200,
        "cadence_ledger_catalog_max_bytes": 1_000,
        "cadence_ledger_entry_headroom": 400,
        "cadence_ledger_max_bytes": 1_000,
        "cadence_ledger_max_entries": 1_000,
        "cadence_ledger_max_reservations": 1_000,
        "cadence_raw_byte_headroom": 500,
        "cadence_raw_catalog_max_bytes": 1_000,
        "cadence_raw_entry_headroom": 500,
        "cadence_raw_max_bytes": 1_000,
        "cadence_raw_max_entries": 1_000,
        "cadence_receipt_headroom": 500,
        "cadence_receipt_max_entries": 1_000,
        "cadence_reservation_headroom": 500,
        "configured_persistent_max_bytes": 4 * gibibyte,
        "configured_raw_object_max_entries": 1_000,
        "event_control_headroom": 100,
        "event_payload_byte_headroom": 100,
        "event_payload_entry_headroom": 100,
        "filesystem_free_bytes": 8 * gibibyte,
        "filesystem_total_bytes": 16 * gibibyte,
        "host_operational_free_headroom_bytes": 5 * gibibyte,
        "host_operational_free_reserve_bytes": 2 * gibibyte,
        "persistent_envelope_free_required_bytes": gibibyte,
        "physical_reserve_headroom_bytes": 5 * gibibyte,
    }
    ordered = [
        code
        for code in service_module.RAW_CAPTURE_RETENTION_GUARD_ALARM_PRIORITY
        if code in alarms
    ]
    blocking = bool(
        set(ordered)
        & service_module.RAW_CAPTURE_RETENTION_GUARD_BLOCKING_ALARMS
    )
    return {
        "admission_ready": not blocking,
        "alarm_codes": ordered,
        "capacity": capacity,
        "capacity_policy": (
            "config_bound_optional_event_envelope_reserved_fail_closed_v2"
        ),
        "contract": service_module.RAW_CAPTURE_RETENTION_GUARD_CONTRACT,
        "event_capacity_claimed": False,
        "full_audit_completed": True,
        "inspection_count": 2,
        "live_delete_performed": False,
        "ownership": dict(
            service_module.WORKER_TECHNICAL_RETENTION_OWNERSHIP
        ),
        "reason_code": ordered[0] if ordered else "ready",
        "reclamation": dict(
            service_module.WORKER_TECHNICAL_RETENTION_RECLAMATION
        ),
        "unmanaged_data_action": "alarm_and_block_never_delete",
    }


@pytest.mark.parametrize(
    ("alarms", "expected_alarm"),
    (
        ((), "clear"),
        (("cadence_cycle_byte_capacity_low",), "warning"),
        (("cadence_cycle_byte_capacity_exhausted",), "critical"),
        (("storage_integrity_failed",), "critical"),
    ),
)
def test_worker_technical_storage_projects_only_exact_guard_v2(
    service_module,
    alarms,
    expected_alarm,
):
    service = service_module.WorkerService(
        host="mqtt", process_isolation=False
    )
    guard = _technical_retention_guard(service_module, alarms=alarms)
    with service._operational_snapshot_lock:
        service._operational_snapshot_health = {"retention_guard": guard}

    source = service._status("online")["technical_diagnostics"]

    assert source["retention_alarm"] == expected_alarm
    assert source["capacity_alarm"] == expected_alarm
    assert source["headroom_bytes"] == 200
    assert source["headroom_percent"] is None
    assert source["storage_reason_codes"] == list(alarms)
    assert "admission_ready" not in source
    assert "event_capacity_claimed" not in source
    projector = _load_technical_projector()
    normalized = projector.normalize_worker_technical_source(
        source,
        worker_state="online",
        observed_at=source["worker_observed_at"],
    )
    assert normalized is not None
    projected = projector.build_worker_technical_diagnostics(
        normalized,
        now=source["worker_observed_at"],
    )
    assert projected["storage"]["retention_alarm"] == expected_alarm
    assert projected["storage"]["capacity_alarm"] == expected_alarm
    assert projected["storage"]["headroom_bytes"] == 200
    assert projected["claims"] == {
        "meteorological_truth": False,
        "operational_admission": False,
        "alert": False,
    }


@pytest.mark.parametrize(
    "mutate",
    (
        lambda guard, secret: guard.update({"credential": secret}),
        lambda guard, secret: guard["capacity"].update({"token": secret}),
        lambda guard, _secret: guard.update({"contract": "guard-v3"}),
        lambda guard, _secret: guard.update({"full_audit_completed": False}),
        lambda guard, _secret: guard["ownership"].update(
            {"exclusive_process_owner_held": False}
        ),
        lambda guard, _secret: guard.update({
            "admission_ready": False,
            "alarm_codes": ["exclusive_process_owner_not_held"],
            "reason_code": "exclusive_process_owner_not_held",
        }),
        lambda guard, secret: guard.update(
            {"alarm_codes": [secret], "reason_code": secret}
        ),
    ),
)
def test_worker_technical_storage_fails_wholly_unknown_on_drift_or_secret(
    service_module,
    mutate,
):
    sentinel = "private-storage-token-do-not-project"
    service = service_module.WorkerService(
        host="mqtt", process_isolation=False
    )
    guard = _technical_retention_guard(service_module)
    mutate(guard, sentinel)
    with service._operational_snapshot_lock:
        service._operational_snapshot_health = {"retention_guard": guard}

    source = service._status("online")["technical_diagnostics"]

    assert source["retention_alarm"] == "unknown"
    assert source["capacity_alarm"] == "unknown"
    assert source["headroom_bytes"] is None
    assert source["headroom_percent"] is None
    assert source["storage_reason_codes"] is None
    assert sentinel not in json.dumps(source, sort_keys=True)


def _request(protocol, job_id, *, deadline=None, job_type="ping", payload=None):
    return types.SimpleNamespace(
        payload=protocol.encode(
            protocol.envelope(
                "job_request",
                job_id=job_id,
                job_type=job_type,
                deadline=deadline or time.time() + 30,
                payload=payload or {"test": True},
            )
        )
    )


def test_worker_executes_one_ping_and_reports_lifecycle(service_module):
    service = service_module.WorkerService(
        host="mqtt", process_isolation=False
    )
    assert service.jobs.maxsize == 16
    service._on_message(
        service.client,
        None,
        _request(service_module, "job-1"),
    )
    thread = threading.Thread(target=service._run_jobs)
    thread.start()
    limit = time.time() + 2
    while service.completed != 1 and time.time() < limit:
        time.sleep(0.01)
    service.stop()
    thread.join(timeout=2)

    states = [
        json.loads(payload)["state"]
        for topic, payload, _kwargs in service.client.published
        if topic.endswith("/jobs/job-1/status")
    ]
    assert states == ["queued", "running", "completed"]
    assert service.active_job is None
    assert service.jobs.empty()


def test_worker_replays_current_state_for_duplicate_and_expires_old_jobs(
    service_module,
):
    service = service_module.WorkerService(
        host="mqtt", process_isolation=False
    )
    service._on_message(
        service.client, None, _request(service_module, "duplicate")
    )
    service._on_message(
        service.client, None, _request(service_module, "duplicate")
    )
    service._on_message(
        service.client,
        None,
        _request(service_module, "expired", deadline=time.time() - 1),
    )
    states = [
        json.loads(payload)["state"]
        for topic, payload, _kwargs in service.client.published
        if "/jobs/" in topic
    ]
    assert states == ["queued", "queued", "expired"]
    assert service.jobs.qsize() == 1


def test_queue_prioritizes_control_then_ground_then_operational_then_low(
    service_module,
):
    handlers = {
        "metar_fetch": lambda job: {},
        "radar_frame_operational": lambda job: {},
        "calibration_corpus_ingest": lambda job: {},
        "custom_background": lambda job: {},
    }
    service = service_module.WorkerService(
        host="mqtt",
        job_handlers=handlers,
        process_isolation=False,
    )
    for job_id, job_type in (
        ("low", "calibration_corpus_ingest"),
        ("default", "custom_background"),
        ("radar", "radar_frame_operational"),
        ("ground", "metar_fetch"),
        ("control", "ping"),
    ):
        service._on_message(
            service.client,
            None,
            _request(service_module, job_id, job_type=job_type),
        )

    queued = [service.jobs.get_nowait()[2].job_id for _ in range(5)]

    assert queued == ["control", "ground", "radar", "default", "low"]


def test_queue_keeps_fifo_order_within_one_priority_tier(service_module):
    service = service_module.WorkerService(
        host="mqtt",
        job_handlers={
            "metar_fetch": lambda job: {},
            "synoptic_fetch": lambda job: {},
        },
        process_isolation=False,
    )
    for job_id, job_type in (
        ("ground-first", "metar_fetch"),
        ("ground-second", "synoptic_fetch"),
    ):
        service._on_message(
            service.client,
            None,
            _request(service_module, job_id, job_type=job_type),
        )

    queued = [service.jobs.get_nowait()[2].job_id for _ in range(2)]

    assert queued == ["ground-first", "ground-second"]


def test_worker_status_exposes_bounded_queue_tiers_and_active_tier(
    service_module,
):
    service = service_module.WorkerService(
        host="mqtt",
        job_handlers={
            "metar_fetch": lambda job: {},
            "radar_frame_operational": lambda job: {},
            "calibration_corpus_snapshot": lambda job: {},
        },
        process_isolation=False,
    )
    for job_id, job_type in (
        ("ground", "metar_fetch"),
        ("radar", "radar_frame_operational"),
        ("background", "calibration_corpus_snapshot"),
    ):
        service._on_message(
            service.client,
            None,
            _request(service_module, job_id, job_type=job_type),
        )
    service.active_job = service_module.Job(
        "active-ground", "metar_fetch", time.time() + 30, {}
    )

    status = service._status("busy")

    assert status["active_job_priority"] == "ground"
    assert status["queue_depth"] == 3
    assert status["queue_depth_by_priority"] == {
        "control": 0,
        "ground": 1,
        "meteorological": 1,
        "standard": 0,
        "background": 1,
    }


def test_completed_qos_redelivery_replays_result_without_reexecution(
    service_module,
):
    executions = 0

    def handler(job):
        nonlocal executions
        executions += 1
        return {"value": 42}

    service = service_module.WorkerService(
        host="mqtt",
        job_handlers={"test_job": handler},
        process_isolation=False,
    )
    request = _request(
        service_module, "qos-redelivery", job_type="test_job"
    )
    service._on_message(service.client, None, request)
    thread = threading.Thread(target=service._run_jobs)
    thread.start()
    limit = time.time() + 2
    while service.completed != 1 and time.time() < limit:
        time.sleep(0.01)
    service._on_message(service.client, None, request)
    service.stop()
    thread.join(timeout=2)

    statuses = [
        json.loads(payload)
        for topic, payload, _kwargs in service.client.published
        if topic.endswith("/jobs/qos-redelivery/status")
    ]
    assert [status["state"] for status in statuses] == [
        "queued", "running", "completed", "completed",
    ]
    assert statuses[-1]["result"] == {"value": 42}
    assert executions == 1
    assert service.accepted == 1
    assert service.accepted_terminal == 1
    assert service.accepted_completed == 1


def test_terminal_result_survives_worker_service_restart(
    service_module, tmp_path
):
    executions = 0

    def handler(job):
        nonlocal executions
        executions += 1
        return {"value": job.payload["value"]}

    receipt_path = tmp_path / "worker-receipts.sqlite3"
    request = _request(
        service_module,
        "durable-completed",
        job_type="durable_job",
        payload={"value": 42},
    )
    first = service_module.WorkerService(
        host="mqtt",
        job_handlers={"durable_job": handler},
        process_isolation=False,
        receipt_path=receipt_path,
    )
    first._on_message(first.client, None, request)
    thread = threading.Thread(target=first._run_jobs)
    thread.start()
    limit = time.time() + 2
    while first.completed != 1 and time.time() < limit:
        time.sleep(0.01)
    first.stop()
    thread.join(timeout=2)

    second = service_module.WorkerService(
        host="mqtt",
        job_handlers={"durable_job": handler},
        process_isolation=False,
        receipt_path=receipt_path,
    )
    second._on_message(second.client, None, request)

    statuses = [
        json.loads(payload)
        for topic, payload, _kwargs in second.client.published
        if topic.endswith("/jobs/durable-completed/status")
    ]
    assert [status["state"] for status in statuses] == ["completed"]
    assert statuses[0]["result"] == {"value": 42}
    assert executions == 1
    assert second.jobs.empty()
    worker_status = second._status("online")
    assert worker_status["durable_job_receipts"] is True
    assert (
        worker_status["job_receipt_schema_version"]
        == service_module.JOB_RECEIPT_SCHEMA_VERSION
    )


def test_durable_receipt_rejects_same_job_id_with_different_payload(
    service_module, tmp_path
):
    executions = 0

    def handler(job):
        nonlocal executions
        executions += 1
        return {"value": job.payload["value"]}

    receipt_path = tmp_path / "worker-receipts.sqlite3"
    first = service_module.WorkerService(
        host="mqtt",
        job_handlers={"durable_job": handler},
        process_isolation=False,
        receipt_path=receipt_path,
    )
    first._on_message(
        first.client,
        None,
        _request(
            service_module,
            "durable-conflict",
            job_type="durable_job",
            payload={"value": 1},
        ),
    )
    thread = threading.Thread(target=first._run_jobs)
    thread.start()
    limit = time.time() + 2
    while first.completed != 1 and time.time() < limit:
        time.sleep(0.01)
    first.stop()
    thread.join(timeout=2)

    second = service_module.WorkerService(
        host="mqtt",
        job_handlers={"durable_job": handler},
        process_isolation=False,
        receipt_path=receipt_path,
    )
    second._on_message(
        second.client,
        None,
        _request(
            service_module,
            "durable-conflict",
            job_type="durable_job",
            payload={"value": 2},
        ),
    )
    statuses = [
        json.loads(payload)
        for topic, payload, _kwargs in second.client.published
        if topic.endswith("/jobs/durable-conflict/status")
    ]
    assert [status["state"] for status in statuses] == ["failed"]
    assert statuses[0]["error"] == "idempotency_conflict"
    assert executions == 1
    assert second.jobs.empty()


def test_worker_restart_terminalizes_durable_accepted_backlog(
    service_module, tmp_path
):
    receipt_path = tmp_path / "worker-receipts.sqlite3"
    request = _request(service_module, "orphaned-accepted")
    first = service_module.WorkerService(
        host="mqtt",
        process_isolation=False,
        receipt_path=receipt_path,
    )
    first._on_message(first.client, None, request)
    assert first.jobs.qsize() == 1

    second = service_module.WorkerService(
        host="mqtt",
        process_isolation=False,
        receipt_path=receipt_path,
    )
    assert second.recovered_terminal_jobs == 1
    second._on_connect(second.client, None, None, 0, None)
    second._on_message(second.client, None, request)

    statuses = [
        json.loads(payload)
        for topic, payload, _kwargs in second.client.published
        if topic.endswith("/jobs/orphaned-accepted/status")
    ]
    assert [status["state"] for status in statuses] == [
        "cancelled",
        "cancelled",
    ]
    assert all(
        status["error"] == "worker_restarted" for status in statuses
    )
    assert second.jobs.empty()


def test_worker_receipt_store_rejects_unknown_schema_version(
    service_module, tmp_path
):
    receipt_path = tmp_path / "worker-receipts.sqlite3"
    store = service_module._JobReceiptStore(
        receipt_path,
        instance_id="first",
        build_id="build-a",
    )
    store.initialize()
    with closing(sqlite3.connect(receipt_path)) as db, db:
        db.execute(
            """
            UPDATE worker_receipt_metadata
            SET schema_version=?
            WHERE metadata_key='schema'
            """,
            (service_module.JOB_RECEIPT_SCHEMA_VERSION + 1,),
        )
    before = receipt_path.read_bytes()

    reopened = service_module._JobReceiptStore(
        receipt_path,
        instance_id="second",
        build_id="build-b",
    )
    with pytest.raises(RuntimeError, match="onbekende.*schemaversie"):
        reopened.initialize()

    with closing(sqlite3.connect(receipt_path)) as db, db:
        version = db.execute(
            """
            SELECT schema_version
            FROM worker_receipt_metadata
            WHERE metadata_key='schema'
            """
        ).fetchone()[0]
    assert version == service_module.JOB_RECEIPT_SCHEMA_VERSION + 1
    assert receipt_path.read_bytes() == before


def test_worker_receipt_store_rejects_unversioned_legacy_table(
    service_module, tmp_path
):
    receipt_path = tmp_path / "worker-receipts.sqlite3"
    with closing(sqlite3.connect(receipt_path)) as db, db:
        db.execute(
            """
            CREATE TABLE worker_job_receipts (
                job_id TEXT PRIMARY KEY
            )
            """
        )
    before = receipt_path.read_bytes()

    store = service_module._JobReceiptStore(
        receipt_path,
        instance_id="first",
        build_id="build-a",
    )
    with pytest.raises(RuntimeError, match="onbekend of onvolledig"):
        store.initialize()

    with closing(sqlite3.connect(receipt_path)) as db, db:
        tables = {
            row[0]
            for row in db.execute(
                """
                SELECT name
                FROM sqlite_master
                WHERE type='table'
                """
            )
        }
    assert tables == {"worker_job_receipts"}
    assert receipt_path.read_bytes() == before


def test_worker_receipt_store_rejects_corrupt_sqlite_without_mutation(
    service_module, tmp_path
):
    receipt_path = tmp_path / "corrupt-worker-receipts.sqlite3"
    corrupt_bytes = b"not a sqlite database; preserve exact incident evidence"
    receipt_path.write_bytes(corrupt_bytes)
    store = service_module._JobReceiptStore(
        receipt_path,
        instance_id="first",
        build_id="build-a",
    )

    with pytest.raises(sqlite3.DatabaseError):
        store.initialize()

    assert receipt_path.read_bytes() == corrupt_bytes


def test_worker_receipts_ignore_duplicate_and_out_of_order_transitions(
    service_module, tmp_path
):
    store = service_module._JobReceiptStore(
        tmp_path / "worker-receipts.sqlite3",
        instance_id="instance-a",
        build_id="build-a",
    )
    store.initialize()
    job = service_module.Job(
        job_id="monotone-receipt",
        job_type="ping",
        deadline=time.time() + 30,
        payload={"probe": 1},
    )

    queued, changed = store.record(
        job, "queued", {"queue_depth": 1}, accepted=True
    )
    assert changed is True
    assert queued["state"] == "queued"

    duplicate, changed = store.record(
        job, "queued", {"queue_depth": 99}, accepted=True
    )
    assert changed is False
    assert duplicate == queued

    running, changed = store.record(
        job, "running", {"process": "dispatcher"}, accepted=True
    )
    assert changed is True
    assert running["state"] == "running"

    late_queued, changed = store.record(
        job, "queued", {"queue_depth": 0}, accepted=True
    )
    assert changed is False
    assert late_queued == running
    assert store.load(job.job_id) == running

    completed, changed = store.record(
        job, "completed", {"result": {"ok": True}}, accepted=True
    )
    assert changed is True
    assert completed["state"] == "completed"

    late_running, changed = store.record(
        job, "running", {"process": "late"}, accepted=True
    )
    assert changed is False
    assert late_running == completed
    assert store.load(job.job_id) == completed


def test_qos_terminal_result_cache_is_bounded(service_module):
    service = service_module.WorkerService(
        host="mqtt", process_isolation=False
    )
    for index in range(service_module.JOB_STATUS_CACHE_MAX + 5):
        job = service_module.Job(
            job_id=f"terminal-{index}",
            job_type="ping",
            deadline=time.time() + 30,
            payload={},
        )
        service._seen[job.job_id] = time.time()
        service._publish_job(job, "completed", result={"index": index})

    assert (
        len(service._job_status_cache)
        == service_module.JOB_STATUS_CACHE_MAX
    )
    assert "terminal-0" not in service._job_status_cache
    assert "terminal-0" not in service._seen


def test_worker_queue_full_publishes_terminal_failure(service_module):
    service = service_module.WorkerService(
        host="mqtt",
        queue_size=1,
        process_isolation=False,
    )
    service._on_message(
        service.client, None, _request(service_module, "queued-job")
    )
    service._on_message(
        service.client, None, _request(service_module, "rejected-job")
    )

    rejected = [
        json.loads(payload)
        for topic, payload, _kwargs in service.client.published
        if topic.endswith("/jobs/rejected-job/status")
    ]
    assert [status["state"] for status in rejected] == ["failed"]
    assert rejected[0]["error"] == "queue_full"
    assert service.jobs.qsize() == 1
    assert service.failed == 1
    assert service.processes["dispatcher"].failed_jobs == 1
    assert "rejected-job" in service._seen

    worker_status = [
        json.loads(payload)
        for topic, payload, _kwargs in service.client.published
        if topic.endswith("/worker/status")
    ][-1]
    assert worker_status["failed_jobs"] == 1
    assert worker_status["queue_depth"] == 1


def test_queue_rejection_preserves_active_dispatcher_state(service_module):
    handler_started = threading.Event()
    release_handler = threading.Event()

    def blocking_handler(_job):
        handler_started.set()
        assert release_handler.wait(timeout=2)
        return {}

    service = service_module.WorkerService(
        host="mqtt",
        queue_size=1,
        job_handlers={"ping": blocking_handler},
        process_isolation=False,
    )
    service._on_message(
        service.client, None, _request(service_module, "active-job")
    )
    thread = threading.Thread(target=service._run_jobs)
    thread.start()
    assert handler_started.wait(timeout=2)

    service._on_message(
        service.client, None, _request(service_module, "queued-job")
    )
    service._on_message(
        service.client, None, _request(service_module, "rejected-job")
    )

    dispatcher = service.processes["dispatcher"]
    assert service.active_job.job_id == "active-job"
    assert dispatcher.state == "busy"
    assert dispatcher.active_job_id == "active-job"
    assert dispatcher.failed_jobs == 1
    assert dispatcher.error == "queue_full"

    release_handler.set()
    limit = time.time() + 2
    while service.completed != 2 and time.time() < limit:
        time.sleep(0.01)
    service.stop()
    thread.join(timeout=2)
    assert service.completed == 2


def test_invalid_identifiable_request_publishes_terminal_failure(
    service_module,
):
    service = service_module.WorkerService(
        host="mqtt", process_isolation=False
    )
    invalid_request = types.SimpleNamespace(
        payload=service_module.encode(
            service_module.envelope(
                "job_request",
                job_id="invalid-job",
                job_type="ping",
                payload={"test": True},
            )
        )
    )

    service._on_message(service.client, None, invalid_request)

    failures = [
        json.loads(payload)
        for topic, payload, _kwargs in service.client.published
        if topic.endswith("/jobs/invalid-job/status")
    ]
    assert [status["state"] for status in failures] == ["failed"]
    assert failures[0]["error"] == "invalid_request"
    assert "deadline" in failures[0]["error_detail"]
    assert service.failed == 1
    assert service.processes["dispatcher"].failed_jobs == 1
    assert service.jobs.empty()


def test_worker_job_log_detail_accepts_only_safe_reason_codes(service_module):
    assert service_module._safe_worker_job_error_detail(
        "raw_capture_desired_state_conflict"
    ) == "raw_capture_desired_state_conflict"
    assert service_module._safe_worker_job_error_detail(
        "token=must-not-be-logged"
    ) is None
    assert service_module._safe_worker_job_error_detail(None) is None


def test_invalid_contract_recovers_job_identity_for_terminal_failure(
    service_module,
):
    service = service_module.WorkerService(
        host="mqtt", process_isolation=False
    )
    invalid_request = types.SimpleNamespace(
        payload=json.dumps(
            {
                "contract_version": 999,
                "message_type": "job_request",
                "job_id": "recoverable-job",
                "job_type": "ping",
                "deadline": time.time() + 30,
                "payload": {},
            }
        ).encode("utf-8")
    )

    service._on_message(service.client, None, invalid_request)

    failures = [
        json.loads(payload)
        for topic, payload, _kwargs in service.client.published
        if topic.endswith("/jobs/recoverable-job/status")
    ]
    assert [status["state"] for status in failures] == ["failed"]
    assert failures[0]["error"] == "invalid_request"
    assert service.failed == 1


def test_invalid_compressed_request_publishes_terminal_failure(
    service_module,
):
    service = service_module.WorkerService(
        host="mqtt", process_isolation=False
    )
    invalid_request = types.SimpleNamespace(
        payload=service_module.encode(
            service_module.envelope(
                "job_request",
                job_id="bad-compression",
                job_type="ping",
                deadline=time.time() + 30,
                payload_codec="gzip+base64+json",
                payload_data="bm90LWd6aXA=",
            )
        )
    )

    service._on_message(service.client, None, invalid_request)

    failures = [
        json.loads(payload)
        for topic, payload, _kwargs in service.client.published
        if topic.endswith("/jobs/bad-compression/status")
    ]
    assert [status["state"] for status in failures] == ["failed"]
    assert failures[0]["error"] == "invalid_request"
    assert service.failed == 1
    assert service.jobs.empty()


def test_expired_dequeued_job_restores_handler_to_idle(service_module):
    service = service_module.WorkerService(
        host="mqtt", process_isolation=False
    )
    service.jobs.put_nowait(
        service_module.Job(
            job_id="expired-in-queue",
            job_type="ping",
            deadline=time.time() - 1,
            payload={},
        )
    )

    thread = threading.Thread(target=service._run_jobs)
    thread.start()
    limit = time.time() + 2
    while time.time() < limit:
        states = [
            json.loads(payload)["state"]
            for topic, payload, _kwargs in service.client.published
            if topic.endswith("/jobs/expired-in-queue/status")
        ]
        if "expired" in states:
            break
        time.sleep(0.01)
    service.stop()
    thread.join(timeout=2)

    states = [
        json.loads(payload)["state"]
        for topic, payload, _kwargs in service.client.published
        if topic.endswith("/jobs/expired-in-queue/status")
    ]
    assert states == ["running", "expired"]
    assert service.active_job is None
    assert service.jobs.empty()
    assert service.completed == 0
    assert service.failed == 0
    handler = service.processes["ping"]
    assert handler.state == "idle"
    assert handler.active_job_id is None
    assert handler.active_since is None
    assert handler.completed_jobs == 0
    assert handler.failed_jobs == 0
    dispatcher = service.processes["dispatcher"]
    assert dispatcher.state == "idle"
    assert dispatcher.active_job_id is None


def test_worker_dispatches_registered_provider_job(service_module):
    seen = []

    def provider_handler(job):
        seen.append((job.job_id, job.payload))
        return {"provider": "dwd_radolan", "count": 3}

    service = service_module.WorkerService(
        host="mqtt",
        job_handlers={"dwd_radolan_fetch": provider_handler},
        process_isolation=False,
    )
    service._on_message(
        service.client,
        None,
        _request(
            service_module,
            "provider-1",
            job_type="dwd_radolan_fetch",
            payload={"areas": [{"center_lat": 51.0}]},
        ),
    )
    thread = threading.Thread(target=service._run_jobs)
    thread.start()
    limit = time.time() + 2
    while service.completed != 1 and time.time() < limit:
        time.sleep(0.01)
    service.stop()
    thread.join(timeout=2)

    completed = [
        json.loads(payload)
        for topic, payload, _kwargs in service.client.published
        if topic.endswith("/jobs/provider-1/status")
        and json.loads(payload)["state"] == "completed"
    ]
    assert seen == [
        ("provider-1", {"areas": [{"center_lat": 51.0}]})
    ]
    assert completed[0]["result"] == {
        "provider": "dwd_radolan",
        "count": 3,
    }
    process = service.processes["dwd_radolan_fetch"]
    assert process.name == "DWD/RADOLAN"
    assert process.state == "idle"
    assert process.completed_jobs == 1
    assert process.failed_jobs == 0
    assert process.last_heartbeat > 0


def test_worker_status_contains_heartbeat_per_process(service_module):
    service = service_module.WorkerService(
        host="mqtt",
        job_handlers={
            "dwd_radolan_fetch": lambda job: {},
            "odim_hdf5_fetch": lambda job: {},
        },
        process_isolation=False,
    )
    status = service._status("online")
    processes = {
        process["process_id"]: process for process in status["processes"]
    }
    assert set(processes) == {
        "mqtt",
        "dispatcher",
        "ping",
        "dwd_radolan_fetch",
        "odim_hdf5_fetch",
    }
    assert processes["dwd_radolan_fetch"]["name"] == "DWD/RADOLAN"
    assert processes["odim_hdf5_fetch"]["name"] == "Met Office HDF5 (VK)"
    assert processes["mqtt"]["heartbeat_age_seconds"] >= 0
    assert processes["mqtt"]["healthy"] is True
    assert processes["dispatcher"]["active_seconds"] is None


def test_worker_status_names_operational_opera_process(service_module):
    service = service_module.WorkerService(
        host="mqtt",
        job_handlers={"opera_hdf5_fetch": lambda job: {}},
        process_isolation=False,
    )
    processes = {
        process["process_id"]: process
        for process in service._status("online")["processes"]
    }
    assert (
        processes["opera_hdf5_fetch"]["name"]
        == "OPERA HDF5 (operationeel)"
    )


def test_worker_status_names_motion_process(service_module):
    service = service_module.WorkerService(
        host="mqtt",
        job_handlers={"motion_preflight": lambda job: {}},
        process_isolation=False,
    )
    process = next(
        item for item in service._status("online")["processes"]
        if item["process_id"] == "motion_preflight"
    )
    assert process["name"] == "Bewegingsanalyse (operationeel)"


def test_worker_status_names_tracking_process(service_module):
    service = service_module.WorkerService(
        host="mqtt",
        job_handlers={"tracking_operational": lambda job: {}},
        process_isolation=False,
    )
    process = next(
        item for item in service._status("online")["processes"]
        if item["process_id"] == "tracking_operational"
    )
    assert process["name"] == "Stormstatus (operationeel)"


def test_idle_process_heartbeat_preserves_error_and_state(service_module):
    service = service_module.WorkerService(
        host="mqtt", process_isolation=False
    )
    process = service.processes["ping"]
    process.state = "error"
    process.error = "voorbeeld"
    previous = process.last_heartbeat

    service._heartbeat_process("ping")

    assert process.last_heartbeat >= previous
    assert process.state == "error"
    assert process.error == "voorbeeld"


def test_spawn_child_is_reused_and_preserves_process_cache(service_module):
    service = service_module.WorkerService(
        host="mqtt",
        job_handlers={
            "process_test": service_module._HandlerReference(
                process_test_handler.__module__,
                process_test_handler.__qualname__,
            )
        },
    )
    for index in (1, 2):
        service._on_message(
            service.client,
            None,
            _request(
                service_module,
                f"persistent-{index}",
                job_type="process_test",
                deadline=time.time() + 15,
                payload={"value": index},
            ),
        )

    thread = threading.Thread(target=service._run_jobs)
    thread.start()
    limit = time.time() + 12
    while service.completed != 2 and time.time() < limit:
        time.sleep(0.02)
    service.stop()
    thread.join(timeout=4)

    completed = [
        json.loads(payload)
        for topic, payload, _kwargs in service.client.published
        if "/jobs/persistent-" in topic
        and json.loads(payload)["state"] == "completed"
    ]
    assert not thread.is_alive()
    assert service.completed == 2
    assert [item["result"]["execution"] for item in completed] == [1, 2]
    assert len({item["result"]["pid"] for item in completed}) == 1
    assert service.accepted == 2
    assert service.accepted_terminal == 2
    assert service.accepted_completed == 2
    assert not service._accepted_job_ids
    assert service._executor is not None
    assert service._executor.alive is False


def test_hard_deadline_replaces_blocked_child_and_queue_continues(
    service_module,
    tmp_path,
):
    marker = tmp_path / "deadline-handler-started"
    service = service_module.WorkerService(
        host="mqtt",
        job_handlers={"process_test": process_test_handler},
    )
    service._on_message(
        service.client,
        None,
        _request(
            service_module,
            "deadline-blocked",
            job_type="process_test",
            deadline=time.time() + 3,
            payload={
                "marker": str(marker),
                "sleep_seconds": 30,
            },
        ),
    )
    service._on_message(
        service.client,
        None,
        _request(
            service_module,
            "deadline-recovery",
            job_type="process_test",
            deadline=time.time() + 15,
            payload={"value": "after-timeout"},
        ),
    )

    started_at = time.time()
    thread = threading.Thread(target=service._run_jobs)
    thread.start()
    marker_limit = time.time() + 2.5
    while not marker.exists() and time.time() < marker_limit:
        time.sleep(0.02)
    limit = time.time() + 10
    while service.completed != 1 and time.time() < limit:
        time.sleep(0.02)
    service.stop()
    thread.join(timeout=4)

    states = {}
    for job_id in ("deadline-blocked", "deadline-recovery"):
        states[job_id] = [
            json.loads(payload)["state"]
            for topic, payload, _kwargs in service.client.published
            if topic.endswith(f"/jobs/{job_id}/status")
        ]
    assert marker.exists()
    assert time.time() - started_at < 8
    assert states["deadline-blocked"] == [
        "queued",
        "running",
        "expired",
    ]
    assert states["deadline-recovery"] == [
        "queued",
        "running",
        "completed",
    ]
    assert service.accepted == 2
    assert service.accepted_terminal == 2
    assert service.accepted_completed == 1
    assert service.expired == 1
    assert not service._accepted_job_ids
    assert service.jobs.empty()
    assert not thread.is_alive()


def test_shutdown_cancels_active_and_queued_jobs_before_return(
    service_module,
    tmp_path,
):
    marker = tmp_path / "shutdown-handler-started"
    service = service_module.WorkerService(
        host="mqtt",
        job_handlers={"process_test": process_test_handler},
    )
    service._on_message(
        service.client,
        None,
        _request(
            service_module,
            "shutdown-active",
            job_type="process_test",
            deadline=time.time() + 60,
            payload={
                "marker": str(marker),
                "sleep_seconds": 30,
            },
        ),
    )
    service._on_message(
        service.client,
        None,
        _request(
            service_module,
            "shutdown-queued",
            job_type="process_test",
            deadline=time.time() + 60,
        ),
    )

    thread = threading.Thread(target=service._run_jobs)
    thread.start()
    limit = time.time() + 5
    while not marker.exists() and time.time() < limit:
        time.sleep(0.02)
    stopped_at = time.time()
    try:
        assert marker.exists()
    finally:
        service.stop()
        thread.join(timeout=4)

    states = {}
    for job_id in ("shutdown-active", "shutdown-queued"):
        states[job_id] = [
            json.loads(payload)["state"]
            for topic, payload, _kwargs in service.client.published
            if topic.endswith(f"/jobs/{job_id}/status")
        ]
    assert time.time() - stopped_at < 4
    assert not thread.is_alive()
    assert states["shutdown-active"] == [
        "queued",
        "running",
        "cancelled",
    ]
    assert states["shutdown-queued"] == ["queued", "cancelled"]
    assert service.accepted == 2
    assert service.accepted_terminal == 2
    assert service.cancelled == 2
    assert not service._accepted_job_ids
    assert service.jobs.empty()
    assert service.active_job is None
    assert service._executor is not None
    assert service._executor.alive is False


def test_child_crash_fails_one_job_and_does_not_poison_queue(
    service_module,
):
    service = service_module.WorkerService(
        host="mqtt",
        job_handlers={"process_test": process_test_handler},
    )
    service._on_message(
        service.client,
        None,
        _request(
            service_module,
            "crash-active",
            job_type="process_test",
            deadline=time.time() + 15,
            payload={"crash_process": True},
        ),
    )
    service._on_message(
        service.client,
        None,
        _request(
            service_module,
            "crash-recovery",
            job_type="process_test",
            deadline=time.time() + 15,
            payload={"value": "recovered"},
        ),
    )

    thread = threading.Thread(target=service._run_jobs)
    thread.start()
    limit = time.time() + 10
    while service.accepted_terminal != 2 and time.time() < limit:
        time.sleep(0.02)
    service.stop()
    thread.join(timeout=4)

    statuses = {
        job_id: [
            json.loads(payload)
            for topic, payload, _kwargs in service.client.published
            if topic.endswith(f"/jobs/{job_id}/status")
        ]
        for job_id in ("crash-active", "crash-recovery")
    }
    assert [
        status["state"] for status in statuses["crash-active"]
    ] == ["queued", "running", "failed"]
    assert statuses["crash-active"][-1]["error"] == "worker_child_crashed"
    assert [
        status["state"] for status in statuses["crash-recovery"]
    ] == ["queued", "running", "completed"]
    assert service.accepted == 2
    assert service.accepted_terminal == 2
    assert service.accepted_failed == 1
    assert service.accepted_completed == 1
    assert service.failed == 1
    assert service.completed == 1
    assert not thread.is_alive()


def test_run_publishes_all_shutdown_terminals_before_mqtt_disconnect(
    service_module,
    tmp_path,
):
    marker = tmp_path / "run-shutdown-handler-started"
    service = service_module.WorkerService(
        host="mqtt",
        heartbeat_seconds=2,
        job_handlers={"process_test": process_test_handler},
    )
    service._on_message(
        service.client,
        None,
        _request(
            service_module,
            "run-active",
            job_type="process_test",
            deadline=time.time() + 60,
            payload={
                "marker": str(marker),
                "sleep_seconds": 30,
            },
        ),
    )
    service._on_message(
        service.client,
        None,
        _request(
            service_module,
            "run-queued",
            job_type="process_test",
            deadline=time.time() + 60,
        ),
    )

    run_thread = threading.Thread(target=service.run)
    run_thread.start()
    limit = time.time() + 5
    while not marker.exists() and time.time() < limit:
        time.sleep(0.02)
    try:
        assert marker.exists()
    finally:
        service.stop()
        run_thread.join(timeout=7)

    disconnect_index = service.client.events.index(("disconnect",))
    terminal_indexes = []
    for index, event in enumerate(service.client.events):
        if event[0] != "publish" or "/jobs/run-" not in event[1]:
            continue
        if json.loads(event[2])["state"] in service_module.TERMINAL_JOB_STATES:
            terminal_indexes.append(index)
    assert not run_thread.is_alive()
    assert len(terminal_indexes) == 2
    assert max(terminal_indexes) < disconnect_index
    assert service.accepted_terminal == service.accepted == 2
    assert service.cancelled == 2


def test_stopped_worker_closes_admission_without_accepting_job(
    service_module,
):
    service = service_module.WorkerService(
        host="mqtt",
        process_isolation=False,
    )
    service.stop()

    service._on_message(
        service.client,
        None,
        _request(service_module, "after-stop"),
    )

    states = [
        json.loads(payload)["state"]
        for topic, payload, _kwargs in service.client.published
        if topic.endswith("/jobs/after-stop/status")
    ]
    assert states == ["cancelled"]
    assert service.accepted == 0
    assert service.accepted_terminal == 0
    assert service.jobs.empty()


def test_terminal_job_state_rejects_late_nonterminal_publication(
    service_module,
):
    service = service_module.WorkerService(
        host="mqtt",
        process_isolation=False,
    )
    job = service_module.Job(
        job_id="terminal-is-final",
        job_type="ping",
        deadline=time.time() + 30,
        payload={},
    )
    with service._admission_lock:
        service._accepted_job_ids.add(job.job_id)
        service.accepted += 1

    assert service._publish_job(
        job, "cancelled", error="worker_stopping"
    )
    assert not service._publish_job(job, "queued", queue_depth=1)
    assert not service._publish_job(job, "running")

    states = [
        json.loads(payload)["state"]
        for topic, payload, _kwargs in service.client.published
        if topic.endswith("/jobs/terminal-is-final/status")
    ]
    assert states == ["cancelled"]
    assert service.accepted == 1
    assert service.accepted_terminal == 1
    assert service.cancelled == 1
    assert not service._accepted_job_ids


def test_strict_effect_admission_fails_closed_for_missing_unsafe_unverified(
    service_module,
):
    def handler(_job):
        return {}

    safe = service_module.HandlerEffectContract(
        contract_id="test-safe-v1",
        effect_class="pure_test",
        hard_kill_safe=True,
        preparation="none",
        compensation="retry_test",
    )
    unsafe = service_module.HandlerEffectContract(
        contract_id="test-unsafe-v1",
        effect_class="unsafe_test",
        hard_kill_safe=False,
        preparation="none",
        compensation="not_safe",
    )
    unverified = service_module.HandlerEffectContract(
        contract_id="test-unverified-v1",
        effect_class="unknown_test",
        hard_kill_safe=True,
        preparation="none",
        compensation="unknown",
        verified=False,
    )
    with pytest.raises(ValueError, match="zonder expliciet effectcontract"):
        service_module.WorkerService(
            host="mqtt",
            job_handlers={"test": handler},
            process_isolation=False,
            require_effect_contracts=True,
        )
    for contract in (unsafe, unverified):
        with pytest.raises(ValueError, match="weigert"):
            service_module.WorkerService(
                host="mqtt",
                job_handlers={"test": handler},
                job_effect_contracts={"test": contract},
                process_isolation=False,
                require_effect_contracts=True,
            )
    service = service_module.WorkerService(
        host="mqtt",
        job_handlers={"test": handler},
        job_effect_contracts={"test": safe},
        process_isolation=False,
        require_effect_contracts=True,
    )
    status = service._status("online")
    assert status["effect_contracts_required"] is True
    assert status["effect_contract_coverage_complete"] is True
    process = next(
        item for item in status["processes"]
        if item["process_id"] == "test"
    )
    assert process["effect_contract_id"] == safe.contract_id
    assert process["effect_contract_verified"] is True
    assert process["hard_kill_safe"] is True
    assert process["effect_compensation"] == "retry_test"


def test_production_handlers_have_one_verified_kill_safe_contract(
    service_module,
):
    assert set(service_module.PRODUCTION_JOB_HANDLERS) == set(
        service_module.PRODUCTION_JOB_EFFECT_CONTRACTS
    )
    assert all(
        contract.verified and contract.hard_kill_safe
        for contract in service_module.PRODUCTION_JOB_EFFECT_CONTRACTS.values()
    )
    assert {
        job_type
        for job_type, contract
        in service_module.PRODUCTION_JOB_EFFECT_CONTRACTS.items()
        if contract.preparation != "none"
    } == {"netatmo_fetch"}
    corpus_contract = service_module.PRODUCTION_JOB_EFFECT_CONTRACTS[
        "calibration_corpus_ingest"
    ]
    assert corpus_contract.effect_class == "bounded_receipted_sqlite_transaction"
    assert "calibration_corpus_ingest" in service_module.JOB_PRIORITY_LOW
    snapshot_contract = service_module.PRODUCTION_JOB_EFFECT_CONTRACTS[
        "calibration_corpus_snapshot"
    ]
    assert snapshot_contract.effect_class == "bounded_idempotent_remote_read"
    assert "calibration_corpus_snapshot" in service_module.JOB_PRIORITY_LOW
    kmi_contract = service_module.PRODUCTION_JOB_EFFECT_CONTRACTS["kmi_fetch"]
    assert kmi_contract.contract_id == "stv3-worker-effect-kmi-raw-capture-v1"
    assert kmi_contract.effect_class == (
        "bounded_read_plus_immutable_objects_sqlite_receipts_and_capture"
    )
    assert kmi_contract.hard_kill_safe is True
    assert kmi_contract.verified is True
    assert kmi_contract.preparation == "none"
    assert kmi_contract.compensation == (
        "retry_exact_job_id_reuses_receipts_adopts_orphan_objects_"
        "and_commits_idempotent_capture"
    )
    assert "kmi_fetch" in service_module.JOB_PRIORITY_METEOROLOGICAL
    assert "kmi_fetch" not in service_module.JOB_PRIORITY_CONTROL
    assert "kmi_fetch" not in service_module.JOB_PRIORITY_GROUND
    assert "kmi_fetch" not in service_module.JOB_PRIORITY_LOW
    dwd_contract = service_module.PRODUCTION_JOB_EFFECT_CONTRACTS[
        "dwd_radolan_fetch"
    ]
    assert dwd_contract.contract_id == (
        "stv3-worker-effect-dwd-rv-raw-capture-v1"
    )
    assert dwd_contract.effect_class == (
        "bounded_read_plus_immutable_objects_sqlite_receipts_and_capture"
    )
    assert dwd_contract.hard_kill_safe is True
    assert dwd_contract.verified is True
    assert dwd_contract.preparation == "none"
    assert dwd_contract.compensation == (
        "retry_exact_job_id_reuses_receipt_adopts_orphan_object_"
        "commits_idempotent_capture_and_replaces_legacy_artifact_atomically"
    )
    assert "dwd_radolan_fetch" in (
        service_module.JOB_PRIORITY_METEOROLOGICAL
    )
    assert "dwd_radolan_fetch" not in service_module.JOB_PRIORITY_CONTROL
    assert "dwd_radolan_fetch" not in service_module.JOB_PRIORITY_GROUND
    assert "dwd_radolan_fetch" not in service_module.JOB_PRIORITY_LOW


def test_broker_secret_is_absent_from_digest_receipt_and_status(
    service_module,
    monkeypatch,
    tmp_path,
):
    from storm_tracker_worker import netatmo

    secret = "brokered-access-token-should-never-leak"
    monkeypatch.setattr(
        netatmo,
        "broker_netatmo_access_token",
        lambda payload, *, deadline: secret,
    )

    def echo_context(job):
        return {
            "access_token": job.execution_context["netatmo_access_token"],
            "detail": f"Bearer {job.execution_context['netatmo_access_token']}",
            "ok": True,
        }

    receipt_path = tmp_path / "receipts.sqlite3"
    service = service_module.WorkerService(
        host="mqtt",
        job_handlers={"netatmo_fetch": echo_context},
        job_effect_contracts={
            "netatmo_fetch": service_module._BROKERED_NETATMO_EFFECT,
        },
        process_isolation=False,
        receipt_path=receipt_path,
        require_effect_contracts=True,
    )
    job = service_module.Job(
        job_id="broker-secret",
        job_type="netatmo_fetch",
        deadline=time.time() + 60,
        payload={"latitude": 51.0, "longitude": 4.0},
    )
    prepared = service._prepare_execution_job(job)
    assert service_module._job_request_digest(job) == (
        service_module._job_request_digest(prepared)
    )

    service._on_message(
        service.client,
        None,
        _request(
            service_module,
            job.job_id,
            job_type=job.job_type,
            deadline=job.deadline,
            payload=job.payload,
        ),
    )
    thread = threading.Thread(target=service._run_jobs)
    thread.start()
    limit = time.time() + 2
    while service.accepted_terminal != 1 and time.time() < limit:
        time.sleep(0.01)
    service.stop()
    thread.join(timeout=2)

    published = b"\n".join(
        payload if isinstance(payload, bytes) else payload.encode()
        for _topic, payload, _kwargs in service.client.published
    )
    assert secret.encode() not in published
    assert secret.encode() not in receipt_path.read_bytes()
    completed = [
        json.loads(payload)
        for topic, payload, _kwargs in service.client.published
        if topic.endswith("/jobs/broker-secret/status")
        and json.loads(payload)["state"] == "completed"
    ][0]
    assert completed["result"]["access_token"] == "[redacted]"
    assert completed["result"]["detail"] == "Bearer [redacted]"


def test_parent_effect_preparation_finishes_before_executor_dispatch(
    service_module,
    monkeypatch,
):
    from storm_tracker_worker import netatmo

    events = []
    monkeypatch.setattr(
        netatmo,
        "broker_netatmo_access_token",
        lambda payload, *, deadline: events.append("durable_rotation")
        or "private-token",
    )

    def handler(_job):
        return {}

    service = service_module.WorkerService(
        host="mqtt",
        job_handlers={"netatmo_fetch": handler},
        job_effect_contracts={
            "netatmo_fetch": service_module._BROKERED_NETATMO_EFFECT,
        },
        process_isolation=False,
        require_effect_contracts=True,
    )

    class Executor:
        def execute(self, execution_job, stop_event):
            assert events == ["durable_rotation"]
            assert execution_job.execution_context[
                "netatmo_access_token"
            ] == "private-token"
            events.append("child_dispatch")
            return service_module._ExecutionOutcome(
                "expired", {"error": "deadline_exceeded"}
            )

    service._executor = Executor()
    outcome = service._execute_job(
        service_module.Job(
            job_id="ordered-broker",
            job_type="netatmo_fetch",
            deadline=time.time() + 60,
            payload={"latitude": 51.0, "longitude": 4.0},
        )
    )
    assert outcome.state == "expired"
    assert events == ["durable_rotation", "child_dispatch"]
