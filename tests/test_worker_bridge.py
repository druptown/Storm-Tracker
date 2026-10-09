from __future__ import annotations

import ast
import asyncio
import importlib.util
import inspect
import json
from pathlib import Path
import sys
import textwrap
import threading
import time
import types

import pytest

from storm_tracker_worker.tracking_core import operational_snapshot as snapshot_contract


def test_passage_reader_upgrades_schema_one_and_strictly_keeps_schema_two(
    worker_bridge_module,
):
    legacy = worker_bridge_module._normalize_operational_passages({
        "schema_version": 1,
        "evaluated_at": 100.0,
        "items": [{
            "target_id": "home", "cell_id": "cell-a", "provider_id": "radar",
            "entry_at": 120.0, "exit_at": 180.0,
            "method": "translated_footprint", "predicted_intensity_mm_h": 1.0,
            "fallback_reason": None, "horizon_clipped": False,
        }],
        "overflow_count": 0,
    })
    assert legacy["schema_version"] == 2
    assert legacy["items"][0]["status"] == "candidate"

    current = worker_bridge_module._normalize_operational_passages({
        "schema_version": 2,
        "evaluated_at": 100.0,
        "items": [{
            "forecast_issue_id": "forecast-issue:a", "passage_id": "passage:a",
            "target_id": "home", "passage_system_id": "system:a",
            "cell_id": "cell-a", "provider_id": "radar", "interval_ordinal": 0,
            "status": "indeterminate", "entry_at": 120.0, "exit_at": None,
            "entry_bracket": {"lower_at": 110.0, "upper_at": 130.0},
            "exit_bracket": None, "left_censored": False,
            "right_censored": True, "coverage_censored": False,
            "method": "advected_labelled_qpe",
            "predicted_mean_intensity_mm_h": 1.0,
            "predicted_peak_intensity_mm_h": 2.0,
            "quantity": "precipitation_rate", "unit": "mm/h",
            "policy_label": "provisional_unvalidated",
            "validation_label": "unvalidated", "scenario_outcomes": [],
            "provenance": {"algorithm_version": "scientific-passage-v1",
                           "policy_version": "candidate_policy_v1",
                           "field_fingerprint": "e" * 64},
        }],
        "overflow_count": 0,
    })
    assert current["items"][0]["exit_at"] is None
    assert current["items"][0]["status"] == "indeterminate"


@pytest.fixture()
def worker_bridge_module(monkeypatch):
    mqtt = types.ModuleType("homeassistant.components.mqtt")
    components = types.ModuleType("homeassistant.components")
    components.__path__ = []
    components.mqtt = mqtt
    homeassistant = types.ModuleType("homeassistant")
    homeassistant.__path__ = []
    core = types.ModuleType("homeassistant.core")
    core.HomeAssistant = object
    core.callback = lambda function: function
    helpers = types.ModuleType("homeassistant.helpers")
    helpers.__path__ = []
    event = types.ModuleType("homeassistant.helpers.event")
    event.async_track_time_interval = lambda *_args, **_kwargs: lambda: None
    const = types.ModuleType("storm_tracker_v3.const")
    const.DOMAIN = "storm_tracker_v3"
    const.DATA_WORKER_TECHNICAL_DIAGNOSTICS = (
        "worker_technical_diagnostics"
    )
    const.DATA_WORKER_TECHNICAL_SOURCE = "worker_technical_source"
    const.OPERATIONAL_SNAPSHOT_UPDATE_EVENT = (
        "storm_tracker_v3_operational_snapshot_update"
    )
    const.WORKER_TECHNICAL_DIAGNOSTICS_UPDATE_EVENT = (
        "storm_tracker_v3_worker_technical_diagnostics_update"
    )
    package = types.ModuleType("storm_tracker_v3")
    package.__path__ = []

    monkeypatch.setitem(sys.modules, "homeassistant", homeassistant)
    monkeypatch.setitem(sys.modules, "homeassistant.components", components)
    monkeypatch.setitem(sys.modules, "homeassistant.components.mqtt", mqtt)
    monkeypatch.setitem(sys.modules, "homeassistant.core", core)
    monkeypatch.setitem(sys.modules, "homeassistant.helpers", helpers)
    monkeypatch.setitem(sys.modules, "homeassistant.helpers.event", event)
    monkeypatch.setitem(sys.modules, "storm_tracker_v3", package)
    monkeypatch.setitem(sys.modules, "storm_tracker_v3.const", const)
    sys.modules.pop("storm_tracker_v3.operational_snapshot", None)
    sys.modules.pop("storm_tracker_v3.worker_bridge", None)

    component_path = (
        Path(__file__).resolve().parent.parent
        / "custom_components"
        / "storm_tracker_v3"
    )
    snapshot_spec = importlib.util.spec_from_file_location(
        "storm_tracker_v3.operational_snapshot",
        component_path / "operational_snapshot.py",
    )
    snapshot_module = importlib.util.module_from_spec(snapshot_spec)
    monkeypatch.setitem(sys.modules, snapshot_spec.name, snapshot_module)
    snapshot_spec.loader.exec_module(snapshot_module)

    technical_spec = importlib.util.spec_from_file_location(
        "storm_tracker_v3.worker_technical_diagnostics",
        component_path / "worker_technical_diagnostics.py",
    )
    technical_module = importlib.util.module_from_spec(technical_spec)
    monkeypatch.setitem(sys.modules, technical_spec.name, technical_module)
    technical_spec.loader.exec_module(technical_module)

    spec = importlib.util.spec_from_file_location(
        "storm_tracker_v3.worker_bridge", component_path / "worker_bridge.py"
    )
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, spec.name, module)
    spec.loader.exec_module(module)
    return module


class _Bus:
    def __init__(self):
        self.events = []
        self.thread_ids = []

    def async_fire(self, event_type, event_data=None):
        self.thread_ids.append(threading.get_ident())
        self.events.append((event_type, event_data))


class _Hass:
    def __init__(self):
        self.loop = asyncio.get_running_loop()
        self.bus = _Bus()
        self.data = {
            "storm_tracker_v3": {
                "unsubscribers": [],
                "worker_status": {
                    "state": "online",
                    "last_seen": time.time(),
                },
                "worker_pending_jobs": {},
            }
        }


def _snapshot(
    revision: int = 1,
    *,
    current_state: str = "dry",
    confirmed_ground: bool = False,
) -> dict:
    evaluated_at = time.time()
    if confirmed_ground:
        current_state = "raining"
        ground = {
            "status": "mixed",
            "freshness": "current",
            "confirmation": True,
            "same_track": True,
            "evidence_valid_at": 1_000.0,
            "observed_at_min": 995.0,
            "observed_at_max": 1_000.0,
            "received_at": 1_002.0,
            "physical_site_count": 3,
            "wet_site_count": 2,
            "source_ids": ["rainviewer"],
            "reason_codes": [],
        }
    else:
        ground = {
            "status": "unknown",
            "freshness": "current",
            "confirmation": None,
            "same_track": None,
            "evidence_valid_at": None,
            "observed_at_min": None,
            "observed_at_max": None,
            "received_at": None,
            "physical_site_count": 0,
            "wet_site_count": 0,
            "source_ids": [],
            "reason_codes": [],
        }
    return {
        "contract": "operational_snapshot_v1",
        "revision": revision,
        "desired_state_sha256": "d" * 64,
        "product_valid_at": 1_000.0,
        "received_at": 1_002.0,
        "evaluated_at": evaluated_at,
        "valid_until": evaluated_at + 300.0,
        "provenance_complete": True,
        "provenance_source_ids": ["rainviewer"],
        "provenance_reason_codes": [],
        "worker": {
            "state": "online",
            "queue_depth": 0,
            "failed_jobs": 0,
            "status_text": "operationeel",
        },
        "regions": {
            "complete": True,
            "overflow_count": 0,
            "items": [{
                "region_id": "region-home",
                "display_name": "Thuis",
                "state": "current",
                "provider_ids": ["rainviewer"],
                "target_ids": ["home"],
            }],
        },
        "targets": {
            "complete": True,
            "overflow_count": 0,
            "items": [{
                "target_id": "home",
                "display_name": "Thuis",
                "region_id": "region-home",
                "status_text": "operationele workerwaarheid",
                "current_precipitation": {
                    "state": current_state,
                    "freshness": "current",
                    "rain_rate_mm_h": (
                        1.2 if current_state == "raining" else 0.0
                    ),
                    "product_valid_at": 1_000.0,
                    "provider_ids": ["rainviewer"],
                    "reason_codes": [],
                    "ground_evidence": ground,
                },
                "next_threat": {
                    "state": "available",
                    "freshness": "current",
                    "track_id": "storm-1",
                    "calibrated_probability_percent": 73.5,
                    "probability_calibration_id": "empirical:threat-v1",
                    "eta_minutes": 20.0,
                    "distance_km": 18.5,
                    "passage_status": "expected",
                    "product_valid_at": 1_000.0,
                    "provider_ids": ["rainviewer"],
                    "reason_codes": [],
                },
            }],
        },
        "providers": {
            "complete": True,
            "overflow_count": 0,
            "items": [{
                "provider_id": "rainviewer",
                "display_name": "RainViewer",
                "state": "operational",
                "freshness": "current",
                "product_valid_at": 1_000.0,
                "received_at": 1_002.0,
                "error": None,
                "reason_codes": [],
            }],
        },
        "ui_storms": {
            "complete": True,
            "overflow_count": 0,
            "items": [{
                "storm_id": "storm-1",
                "region_id": "region-home",
                "display_name": "Storm 1",
                "state": "active",
                "diagnostic_quality_percent": 86.0,
                "diagnostic_quality_kind": "motion_fit",
                "motion_speed_kmh": 42.75,
                "motion_heading_deg": 227.5,
                "max_dbz": 51.0,
                "area_km2": 740.0,
                "product_valid_at": 1_000.0,
                "reason_codes": [],
            }],
        },
        "map_artifact": {
            "artifact_handle": "sha256:" + "a" * 64,
            "etag": '"map-etag-a"',
        },
    }


def _technical_source(**overrides):
    source = {
        "contract": "stv3-worker-technical-diagnostics-source-v1",
        "worker_state": "online",
        "worker_observed_at": 1.0,
        "worker_version": "9.8.7",
        "build_sha256": "sha256:" + "b" * 64,
        "config_sha256": None,
        "desired_state_sha256": "sha256:" + "d" * 64,
        "provider_age_seconds": None,
        "provider_status": "unknown",
        "frame_age_seconds": 4.0,
        "frame_status": "current",
        "cache_age_seconds": None,
        "cache_status": "unknown",
        "queue_depth": 3,
        "terminal_jobs": 8,
        "failed_jobs": 1,
        "retention_alarm": "unknown",
        "capacity_alarm": "unknown",
        "headroom_bytes": None,
        "headroom_percent": None,
        "storage_reason_codes": None,
        "transition_status": "unknown",
        "current_version": "9.8.7",
        "next_version": None,
        "transition_reason_codes": None,
        "replay_contract_version": None,
        "replay_algorithm_version": None,
    }
    source.update(overrides)
    return source


async def _setup_bridge(worker_bridge_module):
    subscriptions = []
    published = []

    async def wait_for_client(_hass):
        return True

    async def subscribe(_hass, topic, callback, qos=0, encoding="utf-8"):
        if topic == worker_bridge_module.OPERATIONAL_SNAPSHOT_TOPIC:
            assert encoding is None
        else:
            assert encoding == "utf-8"
        subscriptions.append((topic, callback, qos))
        return lambda: None

    async def publish(_hass, topic, payload, **kwargs):
        published.append((topic, payload, kwargs))

    worker_bridge_module.mqtt.async_wait_for_mqtt_client = wait_for_client
    worker_bridge_module.mqtt.async_subscribe = subscribe
    worker_bridge_module.mqtt.async_publish = publish
    hass = _Hass()
    await worker_bridge_module.async_setup_worker_bridge(hass)
    return hass, subscriptions, published


def test_foreign_mqtt_thread_is_marshaled_before_all_ha_state_mutations(
    worker_bridge_module,
):
    async def run():
        hass, subscriptions, _published = await _setup_bridge(
            worker_bridge_module
        )
        callbacks = {topic: callback for topic, callback, _qos in subscriptions}
        data = hass.data["storm_tracker_v3"]
        result_future = hass.loop.create_future()
        data["worker_pending_jobs"]["foreign-thread-job"] = result_future
        loop_thread_id = threading.get_ident()
        worker_payload = worker_bridge_module._encode({
            "contract_version": 1,
            "message_type": "worker_status",
            "state": "online",
            "worker_version": "1.14.0",
            "queue_depth": 0,
            "failed_jobs": 0,
        })
        snapshot_payload = snapshot_contract.encode_control_envelope(
            _snapshot()
        )
        job_payload = worker_bridge_module._encode({
            "contract_version": 1,
            "message_type": "job_status",
            "job_id": "foreign-thread-job",
            "job_type": "ping",
            "state": "completed",
            "result": {"ok": True},
        })

        foreign = threading.Thread(
            target=lambda: (
                callbacks[worker_bridge_module.STATUS_TOPIC](
                    types.SimpleNamespace(payload=worker_payload)
                ),
                callbacks[worker_bridge_module.OPERATIONAL_SNAPSHOT_TOPIC](
                    types.SimpleNamespace(payload=snapshot_payload)
                ),
                callbacks[worker_bridge_module.JOB_STATUS_TOPIC](
                    types.SimpleNamespace(payload=job_payload)
                ),
            ),
            name="stv3-mqtt-callback-test",
        )
        foreign.start()
        foreign.join(timeout=2.0)
        assert not foreign.is_alive()
        assert data["worker_status"]["state"] == "offline"
        assert data["operational_snapshot_cache"].revision == 0
        assert not result_future.done()

        for _ in range(8):
            await asyncio.sleep(0)
        assert data["worker_status"]["state"] == "online"
        assert data["operational_snapshot_cache"].revision == 1
        assert await result_future == {"ok": True}
        assert hass.bus.thread_ids
        assert set(hass.bus.thread_ids) == {loop_thread_id}

    asyncio.run(run())


def _snapshot_events(worker_bridge_module, hass):
    return [
        event
        for event in hass.bus.events
        if event[0] == worker_bridge_module.OPERATIONAL_SNAPSHOT_UPDATE_EVENT
    ]


def _technical_events(worker_bridge_module, hass):
    return [
        event
        for event in hass.bus.events
        if event[0]
        == worker_bridge_module.WORKER_TECHNICAL_DIAGNOSTICS_UPDATE_EVENT
    ]


def test_worker_technical_source_is_exact_locally_bound_and_event_bounded(
    worker_bridge_module,
):
    async def run():
        hass, subscriptions, _published = await _setup_bridge(
            worker_bridge_module
        )
        callbacks = {topic: callback for topic, callback, _qos in subscriptions}
        callbacks[worker_bridge_module.STATUS_TOPIC](types.SimpleNamespace(
            payload=worker_bridge_module._encode({
                "contract_version": 1,
                "message_type": "worker_status",
                "state": "online",
                "technical_diagnostics": _technical_source(),
            })
        ))

        data = hass.data["storm_tracker_v3"]
        source = data["worker_technical_source"]
        document = data["worker_technical_diagnostics"]
        assert source is not None
        assert source["worker_state"] == "online"
        assert source["worker_observed_at"] != 1.0
        assert source["build_sha256"] == "sha256:" + "b" * 64
        assert "technical_diagnostics" not in data["worker_status"]
        assert document["status"] == "current"
        assert document["jobs"]["terminal_jobs"] == 8
        assert document["freshness"]["frame"] == {
            "status": "current",
            "age_seconds": 4.0,
        }
        events = _technical_events(worker_bridge_module, hass)
        assert [event[1]["status"] for event in events] == [
            "unknown",
            "current",
        ]
        assert events[-1][1] == {
            "status": "current",
            "projected_at": document["projected_at"],
        }
        assert all(
            len(json.dumps(event[1]).encode("utf-8")) < 128
            for event in events
        )

    asyncio.run(run())


def test_invalid_or_secret_shaped_technical_source_fails_closed_without_leak(
    worker_bridge_module,
):
    async def run():
        hass, subscriptions, _published = await _setup_bridge(
            worker_bridge_module
        )
        callbacks = {topic: callback for topic, callback, _qos in subscriptions}
        sentinel = "SUPERS3CRETTOKEN-DO-NOT-LEAK"
        hostile = _technical_source()
        hostile["credential"] = sentinel
        callbacks[worker_bridge_module.STATUS_TOPIC](types.SimpleNamespace(
            payload=worker_bridge_module._encode({
                "contract_version": 1,
                "message_type": "worker_status",
                "state": "online",
                "technical_diagnostics": hostile,
                "provider_payload": {"token": sentinel},
            })
        ))

        data = hass.data["storm_tracker_v3"]
        assert data["worker_technical_source"] is None
        document = data["worker_technical_diagnostics"]
        assert document["status"] == "unknown"
        assert document["projection_reason_codes"] == [
            "diagnostic_source_missing"
        ]
        retained = json.dumps({
            "status": data["worker_status"],
            "source": data["worker_technical_source"],
            "document": document,
            "events": _technical_events(worker_bridge_module, hass),
        })
        assert sentinel not in retained
        for forbidden in ("credential", "token", "provider_payload"):
            assert forbidden not in retained.lower()

    asyncio.run(run())


def test_worker_offline_transition_reprojects_last_safe_technical_source_stale(
    worker_bridge_module,
):
    async def run():
        hass, subscriptions, _published = await _setup_bridge(
            worker_bridge_module
        )
        callbacks = {topic: callback for topic, callback, _qos in subscriptions}
        callbacks[worker_bridge_module.STATUS_TOPIC](types.SimpleNamespace(
            payload=worker_bridge_module._encode({
                "contract_version": 1,
                "message_type": "worker_status",
                "state": "online",
                "technical_diagnostics": _technical_source(),
            })
        ))
        data = hass.data["storm_tracker_v3"]
        observed_at = data["worker_technical_source"]["worker_observed_at"]

        worker_bridge_module._set_worker_offline(hass, data)

        source = data["worker_technical_source"]
        document = data["worker_technical_diagnostics"]
        assert source["worker_state"] == "offline"
        assert source["worker_observed_at"] == observed_at
        assert document["status"] == "stale"
        assert document["jobs"]["status"] == "stale"
        assert document["projection_reason_codes"] == [
            "worker_state_not_live"
        ]
        assert _technical_events(worker_bridge_module, hass)[-1][1][
            "status"
        ] == "stale"

    asyncio.run(run())


def test_worker_availability_is_heartbeat_bounded(worker_bridge_module):
    async def run():
        hass = _Hass()
        now = time.time()
        status = hass.data["storm_tracker_v3"]["worker_status"]
        status.update({"state": "busy", "last_seen": now - 1.0})
        assert worker_bridge_module.worker_is_available(hass, now=now)
        status["last_seen"] = now - worker_bridge_module.WORKER_STALE_SECONDS - 1
        assert not worker_bridge_module.worker_is_available(hass, now=now)
        status.update({"state": "offline", "last_seen": now})
        assert not worker_bridge_module.worker_is_available(hass, now=now)

    asyncio.run(run())


def test_request_republishes_the_exact_same_job_once(worker_bridge_module):
    async def run():
        published = []

        async def publish(_hass, topic, payload, **kwargs):
            published.append((topic, payload, kwargs))

        worker_bridge_module.mqtt.async_publish = publish
        hass = _Hass()
        task = asyncio.create_task(
            worker_bridge_module.async_request_worker_job(
                hass,
                "tracking_operational",
                {"batch_id": "batch-1", "value": 42},
                timeout=0.4,
                job_id="stable-tracking-job",
            )
        )
        limit = time.monotonic() + 1
        while len(published) < 2 and time.monotonic() < limit:
            await asyncio.sleep(0.01)
        assert len(published) == 2
        assert published[0] == published[1]
        future = hass.data["storm_tracker_v3"]["worker_pending_jobs"][
            "stable-tracking-job"
        ]
        future.set_result({"ok": True})
        assert await task == {"ok": True}
        assert not hass.data["storm_tracker_v3"]["worker_pending_jobs"]

    asyncio.run(run())


def test_concurrent_worker_requests_are_serialized(worker_bridge_module):
    async def run():
        published_job_ids = []
        first_published = asyncio.Event()
        second_attempted = asyncio.Event()
        second_published = asyncio.Event()

        async def publish(_hass, _topic, payload, **_kwargs):
            job_id = json.loads(payload)["job_id"]
            published_job_ids.append(job_id)
            if job_id == "queued-job-1":
                first_published.set()
            elif job_id == "queued-job-2":
                second_published.set()

        worker_bridge_module.mqtt.async_publish = publish
        hass = _Hass()
        first_task = asyncio.create_task(
            worker_bridge_module.async_request_worker_job(
                hass,
                "provider_fetch",
                {"provider": "first"},
                timeout=2.0,
                job_id="queued-job-1",
            )
        )
        await asyncio.wait_for(first_published.wait(), timeout=0.5)

        async def request_second():
            second_attempted.set()
            return await worker_bridge_module.async_request_worker_job(
                hass,
                "provider_fetch",
                {"provider": "second"},
                timeout=2.0,
                job_id="queued-job-2",
            )

        second_task = asyncio.create_task(request_second())
        await asyncio.wait_for(second_attempted.wait(), timeout=0.5)
        await asyncio.sleep(0)

        assert published_job_ids == ["queued-job-1"]
        assert not second_published.is_set()
        assert "queued-job-2" not in hass.data["storm_tracker_v3"][
            "worker_pending_jobs"
        ]

        first_future = hass.data["storm_tracker_v3"]["worker_pending_jobs"][
            "queued-job-1"
        ]
        first_future.set_result({"provider": "first", "ok": True})
        assert await first_task == {"provider": "first", "ok": True}

        await asyncio.wait_for(second_published.wait(), timeout=0.5)
        assert published_job_ids == ["queued-job-1", "queued-job-2"]
        second_future = hass.data["storm_tracker_v3"]["worker_pending_jobs"][
            "queued-job-2"
        ]
        second_future.set_result({"provider": "second", "ok": True})
        assert await second_task == {"provider": "second", "ok": True}
        assert hass.data["storm_tracker_v3"]["worker_pending_jobs"] == {}

    asyncio.run(run())


def test_cancelling_active_request_releases_serial_queue(worker_bridge_module):
    async def run():
        first_published = asyncio.Event()
        second_attempted = asyncio.Event()
        second_published = asyncio.Event()

        async def publish(_hass, _topic, payload, **_kwargs):
            job_id = json.loads(payload)["job_id"]
            if job_id == "cancelled-active-job":
                first_published.set()
            elif job_id == "job-after-cancellation":
                second_published.set()

        worker_bridge_module.mqtt.async_publish = publish
        hass = _Hass()
        first_task = asyncio.create_task(
            worker_bridge_module.async_request_worker_job(
                hass,
                "provider_fetch",
                {"provider": "cancelled"},
                timeout=2.0,
                job_id="cancelled-active-job",
            )
        )
        await asyncio.wait_for(first_published.wait(), timeout=0.5)

        async def request_second():
            second_attempted.set()
            return await worker_bridge_module.async_request_worker_job(
                hass,
                "provider_fetch",
                {"provider": "next"},
                timeout=2.0,
                job_id="job-after-cancellation",
            )

        second_task = asyncio.create_task(request_second())
        await asyncio.wait_for(second_attempted.wait(), timeout=0.5)
        await asyncio.sleep(0)
        assert not second_published.is_set()

        first_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first_task
        assert "cancelled-active-job" not in hass.data["storm_tracker_v3"][
            "worker_pending_jobs"
        ]

        await asyncio.wait_for(second_published.wait(), timeout=0.5)
        second_future = hass.data["storm_tracker_v3"]["worker_pending_jobs"][
            "job-after-cancellation"
        ]
        second_future.set_result({"provider": "next", "ok": True})
        assert await second_task == {"provider": "next", "ok": True}
        assert hass.data["storm_tracker_v3"]["worker_pending_jobs"] == {}

    asyncio.run(run())


def test_cancelled_worker_status_terminates_pending_future(
    worker_bridge_module,
):
    async def run():
        subscriptions = []

        async def wait_for_client(_hass):
            return True

        async def subscribe(
            _hass, topic, callback, qos=0, encoding="utf-8"
        ):
            subscriptions.append((topic, callback, qos))
            return lambda: None

        async def publish(*_args, **_kwargs):
            pass

        worker_bridge_module.mqtt.async_wait_for_mqtt_client = wait_for_client
        worker_bridge_module.mqtt.async_subscribe = subscribe
        worker_bridge_module.mqtt.async_publish = publish
        hass = _Hass()
        await worker_bridge_module.async_setup_worker_bridge(hass)
        callback = next(
            callback
            for topic, callback, _qos in subscriptions
            if topic == worker_bridge_module.JOB_STATUS_TOPIC
        )
        future = hass.loop.create_future()
        hass.data["storm_tracker_v3"]["worker_pending_jobs"][
            "cancelled-job"
        ] = future
        callback(
            types.SimpleNamespace(
                payload=worker_bridge_module._encode(
                    {
                        "contract_version": 1,
                        "message_type": "job_status",
                        "job_id": "cancelled-job",
                        "state": "cancelled",
                        "error": "worker_restarted",
                    }
                )
            )
        )
        with pytest.raises(RuntimeError, match="cancelled"):
            await future

    asyncio.run(run())


def test_worker_job_error_surfaces_only_safe_reason_code(worker_bridge_module):
    assert worker_bridge_module._worker_job_error({
        "error": "RawCaptureServiceIntegrityError",
        "error_detail": "raw_capture_desired_state_conflict",
    }) == (
        "RawCaptureServiceIntegrityError: "
        "raw_capture_desired_state_conflict"
    )
    assert worker_bridge_module._worker_job_error({
        "error": "RawCaptureServiceIntegrityError",
        "error_detail": "token=must-not-be-exposed",
    }) == "RawCaptureServiceIntegrityError"


def test_missing_worker_receipt_times_out_after_one_identical_retry(
    worker_bridge_module,
):
    async def run():
        published = []

        async def publish(_hass, topic, payload, **kwargs):
            published.append((topic, payload, kwargs))

        worker_bridge_module.mqtt.async_publish = publish
        hass = _Hass()

        with pytest.raises(TimeoutError):
            await worker_bridge_module.async_request_worker_job(
                hass,
                "tracking_operational",
                {"batch_id": "batch-missing"},
                timeout=0.08,
                job_id="missing-receipt",
            )

        assert len(published) == 2
        assert published[0] == published[1]
        assert hass.data["storm_tracker_v3"]["worker_pending_jobs"] == {}

    asyncio.run(run())


def test_duplicate_and_out_of_order_mqtt_statuses_complete_once(
    worker_bridge_module,
):
    async def run():
        subscriptions = []

        async def wait_for_client(_hass):
            return True

        async def subscribe(
            _hass, topic, callback, qos=0, encoding="utf-8"
        ):
            subscriptions.append((topic, callback, qos))
            return lambda: None

        async def publish(*_args, **_kwargs):
            pass

        worker_bridge_module.mqtt.async_wait_for_mqtt_client = wait_for_client
        worker_bridge_module.mqtt.async_subscribe = subscribe
        worker_bridge_module.mqtt.async_publish = publish
        hass = _Hass()
        await worker_bridge_module.async_setup_worker_bridge(hass)
        callback = next(
            callback
            for topic, callback, _qos in subscriptions
            if topic == worker_bridge_module.JOB_STATUS_TOPIC
        )
        future = hass.loop.create_future()
        hass.data["storm_tracker_v3"]["worker_pending_jobs"][
            "out-of-order-job"
        ] = future

        def status(state, **values):
            callback(
                types.SimpleNamespace(
                    payload=worker_bridge_module._encode(
                        {
                            "contract_version": 1,
                            "message_type": "job_status",
                            "job_id": "out-of-order-job",
                            "state": state,
                            **values,
                        }
                    )
                )
            )

        status("completed", result={"revision": 7})
        status("queued", queue_depth=1)
        status("running", process="tracking")
        status("completed", result={"revision": 999})

        assert await future == {"revision": 7}
        assert future.exception() is None

    asyncio.run(run())


def test_receiver_subscribes_qos1_and_applies_once_without_duplicate_fanout(
    worker_bridge_module,
):
    async def run():
        hass, subscriptions, _published = await _setup_bridge(
            worker_bridge_module
        )
        data = hass.data["storm_tracker_v3"]
        matching = [
            (callback, qos)
            for topic, callback, qos in subscriptions
            if topic == worker_bridge_module.OPERATIONAL_SNAPSHOT_TOPIC
        ]
        assert len(matching) == 1
        callback, qos = matching[0]
        assert qos == 1
        assert isinstance(
            data["operational_snapshot_cache"],
            worker_bridge_module.OperationalSnapshotCache,
        )

        payload = snapshot_contract.encode_control_envelope(_snapshot())
        callback(types.SimpleNamespace(payload=payload))
        target = data["operational_snapshot_cache"].get_target("home")
        assert target["current_precipitation"]["state"] == "dry"
        assert target["next_threat"]["state"] == "available"

        events = _snapshot_events(worker_bridge_module, hass)
        assert len(events) == 1
        assert set(events[0][1]) == {
            "revision",
            "snapshot_sha256",
            "availability",
            "reason",
            "envelope_bytes",
        }
        assert events[0][1]["revision"] == 1
        assert events[0][1]["availability"] == "current"
        assert events[0][1]["reason"] == "applied"
        assert len(json.dumps(events[0][1]).encode("utf-8")) < 1_024

        callback(types.SimpleNamespace(payload=payload))
        assert len(_snapshot_events(worker_bridge_module, hass)) == 1
        diagnostics = data["operational_snapshot_diagnostics"]
        assert diagnostics["applied_count"] == 1
        assert diagnostics["duplicate_count"] == 1
        assert diagnostics["rejected_count"] == 0

    asyncio.run(run())


def test_already_expired_recovered_snapshot_is_immediately_stale_while_online(
    worker_bridge_module,
    monkeypatch,
):
    async def run():
        monkeypatch.setattr(worker_bridge_module.time, "time", lambda: 1_200.0)
        hass, subscriptions, _published = await _setup_bridge(
            worker_bridge_module
        )
        callback = next(
            callback
            for topic, callback, _qos in subscriptions
            if topic == worker_bridge_module.OPERATIONAL_SNAPSHOT_TOPIC
        )
        data = hass.data["storm_tracker_v3"]
        data["worker_status"].update({"state": "online", "last_seen": 1_200.0})
        expired = _snapshot()
        expired["evaluated_at"] = 1_000.0
        expired["valid_until"] = 1_100.0

        callback(types.SimpleNamespace(
            payload=snapshot_contract.encode_control_envelope(expired)
        ))

        cache = data["operational_snapshot_cache"]
        target = cache.get_target("home")
        assert cache.revision == 1
        assert cache.availability == "stale"
        assert target["current_precipitation"]["state"] == "unknown"
        assert target["status_text"] == (
            "Snapshot verlopen; actuele toestand is onbekend"
        )
        assert "snapshot_expired" in target["current_precipitation"]["reason_codes"]
        assert data["worker_status"]["state"] == "online"
        assert data["operational_snapshot_diagnostics"]["last_action"] == (
            "snapshot_expired"
        )
        assert [event[1]["reason"] for event in _snapshot_events(
            worker_bridge_module, hass
        )] == ["snapshot_expired"]

    asyncio.run(run())


def test_worker_only_receiver_requires_exact_confirmed_desired_authority(
    worker_bridge_module,
    monkeypatch,
):
    async def run():
        monkeypatch.setattr(worker_bridge_module.time, "time", lambda: 1_000.0)
        hass, subscriptions, _published = await _setup_bridge(
            worker_bridge_module
        )
        callback = next(
            callback
            for topic, callback, _qos in subscriptions
            if topic == worker_bridge_module.OPERATIONAL_SNAPSHOT_TOPIC
        )
        data = hass.data["storm_tracker_v3"]
        data["operational_snapshot_authority_required"] = True
        data["operational_snapshot_expected_desired_state_sha256"] = None

        callback(types.SimpleNamespace(
            payload=snapshot_contract.encode_control_envelope(_snapshot(1))
        ))
        assert data["operational_snapshot_cache"].revision == 0
        assert data["operational_snapshot_diagnostics"]["rejected_count"] == 1

        data["operational_snapshot_expected_desired_state_sha256"] = "d" * 64
        callback(types.SimpleNamespace(
            payload=snapshot_contract.encode_control_envelope(_snapshot(1))
        ))
        cache = data["operational_snapshot_cache"]
        assert cache.revision == 1
        assert cache.availability == "current"

        assert worker_bridge_module.mark_operational_snapshot_authority_changed(
            hass,
            expected_desired_state_sha256=None,
        )
        assert cache.availability == "stale"
        assert data["operational_snapshot_diagnostics"]["last_action"] == (
            "desired_state_transition"
        )

        obsolete = _snapshot(2)
        callback(types.SimpleNamespace(
            payload=snapshot_contract.encode_control_envelope(obsolete)
        ))
        assert cache.revision == 1
        assert cache.availability == "stale"

        worker_bridge_module.mark_operational_snapshot_authority_changed(
            hass,
            expected_desired_state_sha256="e" * 64,
        )
        current = _snapshot(2)
        current["desired_state_sha256"] = "e" * 64
        callback(types.SimpleNamespace(
            payload=snapshot_contract.encode_control_envelope(current)
        ))
        assert cache.revision == 2
        assert cache.availability == "current"
        assert cache.get_status()["desired_state_sha256"] == "e" * 64
        assert [event[1]["reason"] for event in _snapshot_events(
            worker_bridge_module, hass
        )] == ["applied", "desired_state_transition", "applied"]

    asyncio.run(run())


def test_liveness_only_authority_transition_preserves_current_snapshot_until_replacement(
    worker_bridge_module,
    monkeypatch,
):
    async def run():
        monkeypatch.setattr(worker_bridge_module.time, "time", lambda: 1_000.0)
        hass, subscriptions, _published = await _setup_bridge(
            worker_bridge_module
        )
        callback = next(
            callback
            for topic, callback, _qos in subscriptions
            if topic == worker_bridge_module.OPERATIONAL_SNAPSHOT_TOPIC
        )
        data = hass.data["storm_tracker_v3"]
        data["operational_snapshot_authority_required"] = True
        data["operational_snapshot_expected_desired_state_sha256"] = "d" * 64

        callback(types.SimpleNamespace(
            payload=snapshot_contract.encode_control_envelope(_snapshot(1))
        ))
        cache = data["operational_snapshot_cache"]
        expiry_handle = data[worker_bridge_module._SNAPSHOT_EXPIRY_HANDLE]
        assert cache.availability == "current"

        assert not worker_bridge_module.mark_operational_snapshot_authority_changed(
            hass,
            expected_desired_state_sha256=None,
            preserve_current_snapshot=True,
        )
        assert cache.availability == "current"
        assert data[worker_bridge_module._SNAPSHOT_EXPIRY_HANDLE] is expiry_handle

        assert not worker_bridge_module.mark_operational_snapshot_authority_changed(
            hass,
            expected_desired_state_sha256="e" * 64,
            preserve_current_snapshot=True,
        )
        assert cache.availability == "current"

        fallback = _snapshot(2)
        fallback["desired_state_sha256"] = "e" * 64
        fallback["product_valid_at"] = None
        fallback["received_at"] = None
        fallback["provenance_complete"] = False
        fallback["provenance_source_ids"] = []
        fallback["provenance_reason_codes"] = [
            "awaiting_meteorological_evidence"
        ]
        fallback["providers"]["items"][0].update({
            "state": "unknown",
            "freshness": "unknown",
            "product_valid_at": None,
            "received_at": None,
            "reason_codes": ["awaiting_meteorological_evidence"],
        })
        fallback["regions"]["items"][0].update({
            "state": "unknown",
            "provider_ids": [],
        })
        current = fallback["targets"]["items"][0][
            "current_precipitation"
        ]
        current.update({
            "state": "unknown",
            "freshness": "unknown",
            "rain_rate_mm_h": None,
            "product_valid_at": None,
            "provider_ids": [],
            "reason_codes": ["awaiting_meteorological_evidence"],
        })
        current["ground_evidence"].update({
            "freshness": "unknown",
            "reason_codes": ["awaiting_meteorological_evidence"],
        })
        threat = fallback["targets"]["items"][0]["next_threat"]
        threat.update({
            "state": "unknown",
            "freshness": "unknown",
            "track_id": None,
            "calibrated_probability_percent": None,
            "probability_calibration_id": None,
            "eta_minutes": None,
            "distance_km": None,
            "passage_status": "unknown",
            "product_valid_at": None,
            "provider_ids": [],
            "reason_codes": ["awaiting_meteorological_evidence"],
        })
        fallback["targets"]["items"][0]["status_text"] = ""
        fallback["ui_storms"]["items"] = []
        fallback["map_artifact"] = {"artifact_handle": None, "etag": None}
        rejected_before = data["operational_snapshot_diagnostics"][
            "rejected_count"
        ]
        callback(types.SimpleNamespace(
            payload=snapshot_contract.encode_control_envelope(fallback)
        ))
        assert cache.revision == 1
        assert cache.availability == "current"
        assert data["operational_snapshot_diagnostics"][
            "rejected_count"
        ] == rejected_before

        replacement = _snapshot(3)
        replacement["desired_state_sha256"] = "e" * 64
        callback(types.SimpleNamespace(
            payload=snapshot_contract.encode_control_envelope(replacement)
        ))
        assert cache.revision == 3
        assert cache.availability == "current"
        assert [event[1]["reason"] for event in _snapshot_events(
            worker_bridge_module, hass
        )] == ["applied", "applied"]

    asyncio.run(run())


def test_startup_retained_snapshot_replays_once_after_authority_confirmation(
    worker_bridge_module,
    monkeypatch,
):
    async def run():
        monkeypatch.setattr(worker_bridge_module.time, "time", lambda: 1_000.0)
        hass, subscriptions, _published = await _setup_bridge(
            worker_bridge_module
        )
        callback = next(
            callback
            for topic, callback, _qos in subscriptions
            if topic == worker_bridge_module.OPERATIONAL_SNAPSHOT_TOPIC
        )
        data = hass.data["storm_tracker_v3"]
        data["operational_snapshot_authority_required"] = True
        data["operational_snapshot_expected_desired_state_sha256"] = None
        payload = snapshot_contract.encode_control_envelope(_snapshot(54))

        callback(types.SimpleNamespace(payload=payload))

        cache = data["operational_snapshot_cache"]
        pending_key = (
            worker_bridge_module._PENDING_OPERATIONAL_SNAPSHOT_PAYLOAD
        )
        assert cache.revision == 0
        assert cache.availability == "unknown"
        assert data[pending_key] == payload
        assert len(data[pending_key]) <= (
            snapshot_contract.MAX_CONTROL_ENVELOPE_BYTES
        )
        assert data["operational_snapshot_diagnostics"]["rejected_count"] == 1
        assert not _snapshot_events(worker_bridge_module, hass)

        # The controller emits an initial None notification during startup.  It
        # is not an authority reset and must not discard the retained message.
        assert not worker_bridge_module.mark_operational_snapshot_authority_changed(
            hass,
            expected_desired_state_sha256=None,
        )
        assert data[pending_key] == payload

        assert worker_bridge_module.mark_operational_snapshot_authority_changed(
            hass,
            expected_desired_state_sha256="d" * 64,
        )
        assert pending_key not in data
        assert cache.revision == 54
        assert cache.availability == "current"
        assert cache.get_status()["desired_state_sha256"] == "d" * 64
        diagnostics = data["operational_snapshot_diagnostics"]
        assert diagnostics["applied_count"] == 1
        assert diagnostics["rejected_count"] == 1
        assert [event[1]["reason"] for event in _snapshot_events(
            worker_bridge_module, hass
        )] == ["applied"]

    asyncio.run(run())


def test_startup_retained_snapshot_with_wrong_authority_is_rejected_once(
    worker_bridge_module,
    monkeypatch,
):
    async def run():
        monkeypatch.setattr(worker_bridge_module.time, "time", lambda: 1_000.0)
        hass, subscriptions, _published = await _setup_bridge(
            worker_bridge_module
        )
        callback = next(
            callback
            for topic, callback, _qos in subscriptions
            if topic == worker_bridge_module.OPERATIONAL_SNAPSHOT_TOPIC
        )
        data = hass.data["storm_tracker_v3"]
        data["operational_snapshot_authority_required"] = True
        data["operational_snapshot_expected_desired_state_sha256"] = None
        payload = snapshot_contract.encode_control_envelope(_snapshot(54))
        pending_key = (
            worker_bridge_module._PENDING_OPERATIONAL_SNAPSHOT_PAYLOAD
        )

        callback(types.SimpleNamespace(payload=payload))
        assert data[pending_key] == payload

        assert not worker_bridge_module.mark_operational_snapshot_authority_changed(
            hass,
            expected_desired_state_sha256="e" * 64,
        )
        cache = data["operational_snapshot_cache"]
        assert pending_key not in data
        assert cache.revision == 0
        assert cache.availability == "unknown"
        diagnostics = data["operational_snapshot_diagnostics"]
        assert diagnostics["applied_count"] == 0
        assert diagnostics["rejected_count"] == 2
        assert diagnostics["last_error_type"] == "SnapshotRevisionConflict"
        assert diagnostics["last_error"] == (
            "snapshot hoort bij een andere desired-state-authoriteit"
        )
        assert not _snapshot_events(worker_bridge_module, hass)

        # The rejected raw candidate was consumed and cannot be replayed under
        # a later authority.  Oversized input is never retained either.
        assert not worker_bridge_module.mark_operational_snapshot_authority_changed(
            hass,
            expected_desired_state_sha256="d" * 64,
        )
        assert cache.revision == 0
        worker_bridge_module.mark_operational_snapshot_authority_changed(
            hass,
            expected_desired_state_sha256=None,
        )
        oversized = (
            b"x" * (snapshot_contract.MAX_CONTROL_ENVELOPE_BYTES + 1)
        )
        callback(types.SimpleNamespace(payload=oversized))
        assert pending_key not in data
        assert payload not in data.values()
        assert oversized not in data.values()

        callback(types.SimpleNamespace(payload=payload))
        assert data[pending_key] == payload
        for unsubscribe in list(data["unsubscribers"]):
            unsubscribe()
        assert pending_key not in data

    asyncio.run(run())


def test_future_worker_clock_is_rejected_without_replacing_valid_cache(
    worker_bridge_module,
    monkeypatch,
):
    async def run():
        monkeypatch.setattr(worker_bridge_module.time, "time", lambda: 1_000.0)
        hass, subscriptions, _published = await _setup_bridge(
            worker_bridge_module
        )
        callback = next(
            callback
            for topic, callback, _qos in subscriptions
            if topic == worker_bridge_module.OPERATIONAL_SNAPSHOT_TOPIC
        )
        valid = _snapshot(1)
        callback(types.SimpleNamespace(
            payload=snapshot_contract.encode_control_envelope(valid)
        ))
        data = hass.data["storm_tracker_v3"]
        cache = data["operational_snapshot_cache"]
        original_digest = cache.snapshot_sha256

        future = _snapshot(2)
        future["evaluated_at"] = 1_000.0 + 24 * 60 * 60.0
        future["valid_until"] = future["evaluated_at"] + 300.0
        callback(types.SimpleNamespace(
            payload=snapshot_contract.encode_control_envelope(future)
        ))

        assert cache.revision == 1
        assert cache.snapshot_sha256 == original_digest
        assert cache.availability == "current"
        diagnostics = data["operational_snapshot_diagnostics"]
        assert diagnostics["last_action"] == "rejected"
        assert diagnostics["last_error_type"] == "SnapshotValidationError"
        assert diagnostics["last_error"] == (
            "snapshot.evaluated_at overschrijdt de ontvangende klokgrens"
        )
        assert len(diagnostics["last_error"]) <= (
            worker_bridge_module.MAX_DIAGNOSTIC_TEXT_CHARS
        )
        assert [event[1]["reason"] for event in _snapshot_events(
            worker_bridge_module,
            hass,
        )] == ["applied"]

    asyncio.run(run())


def test_ping_wall_clock_jump_cannot_bypass_monotonic_snapshot_deadline(
    worker_bridge_module,
    monkeypatch,
):
    async def run():
        now = [1_090.0]
        intervals = []
        monkeypatch.setattr(worker_bridge_module.time, "time", lambda: now[0])

        def track_interval(_hass, callback, _interval):
            intervals.append(callback)
            return lambda: None

        monkeypatch.setattr(
            worker_bridge_module,
            "async_track_time_interval",
            track_interval,
        )
        hass, subscriptions, _published = await _setup_bridge(
            worker_bridge_module
        )
        assert len(intervals) == 1
        callback = next(
            callback
            for topic, callback, _qos in subscriptions
            if topic == worker_bridge_module.OPERATIONAL_SNAPSHOT_TOPIC
        )
        expiring = _snapshot()
        expiring["evaluated_at"] = 1_000.0
        expiring["valid_until"] = 1_100.0
        callback(types.SimpleNamespace(
            payload=snapshot_contract.encode_control_envelope(expiring)
        ))
        callback(types.SimpleNamespace(payload=b"{}"))

        data = hass.data["storm_tracker_v3"]
        data["worker_status"].update({"state": "online", "last_seen": now[0]})
        cache = data["operational_snapshot_cache"]
        assert cache.revision == 1
        assert cache.availability == "current"
        assert data["operational_snapshot_diagnostics"]["last_action"] == "rejected"

        now[0] = 1_100.0
        data["worker_status"].update({"state": "online", "last_seen": now[0]})
        await intervals[0]()
        await intervals[0]()

        assert data["worker_status"]["state"] == "online"
        assert cache.availability == "current"
        assert cache.get_status()["valid_until"] == 1_100.0
        assert [event[1]["reason"] for event in _snapshot_events(
            worker_bridge_module, hass
        )] == ["applied"]
        assert data["operational_snapshot_diagnostics"]["last_action"] == (
            "rejected"
        )

    asyncio.run(run())


def test_receiver_rejects_noncanonical_encoded_and_oversize_payloads_compactly(
    worker_bridge_module,
):
    async def run():
        hass, subscriptions, _published = await _setup_bridge(
            worker_bridge_module
        )
        callback = next(
            callback
            for topic, callback, _qos in subscriptions
            if topic == worker_bridge_module.OPERATIONAL_SNAPSHOT_TOPIC
        )
        valid = snapshot_contract.encode_control_envelope(_snapshot())
        rejected = (
            b" " + valid,
            valid.decode("utf-8"),
            b"H4sIAAAAAAAA",
            b"\x1f\x8bcompressed-control-plane-is-forbidden",
            b"sensitive_marker" * 5_000,
        )
        for payload in rejected:
            callback(types.SimpleNamespace(payload=payload))

        data = hass.data["storm_tracker_v3"]
        cache = data["operational_snapshot_cache"]
        diagnostics = data["operational_snapshot_diagnostics"]
        assert cache.revision == 0
        assert cache.availability == "unknown"
        assert _snapshot_events(worker_bridge_module, hass) == []
        assert diagnostics["rejected_count"] == len(rejected)
        assert diagnostics["last_action"] == "rejected"
        assert "sensitive_marker" not in json.dumps(diagnostics)
        assert len(diagnostics["last_error"]) <= 160
        assert len(json.dumps(diagnostics).encode("utf-8")) < 1_024

    asyncio.run(run())


def test_receiver_revision_conflicts_are_atomic_and_emit_no_error_event(
    worker_bridge_module,
):
    async def run():
        hass, subscriptions, _published = await _setup_bridge(
            worker_bridge_module
        )
        callback = next(
            callback
            for topic, callback, _qos in subscriptions
            if topic == worker_bridge_module.OPERATIONAL_SNAPSHOT_TOPIC
        )
        first = _snapshot(1)
        callback(types.SimpleNamespace(
            payload=snapshot_contract.encode_control_envelope(first)
        ))

        equivocal = _snapshot(1)
        equivocal["targets"]["items"][0]["status_text"] = "ander resultaat"
        callback(types.SimpleNamespace(
            payload=snapshot_contract.encode_control_envelope(equivocal)
        ))
        cache = hass.data["storm_tracker_v3"]["operational_snapshot_cache"]
        assert cache.revision == 1
        assert cache.get_target("home")["status_text"] == (
            "operationele workerwaarheid"
        )

        second = _snapshot(2)
        second["targets"]["items"][0]["status_text"] = "revision twee"
        callback(types.SimpleNamespace(
            payload=snapshot_contract.encode_control_envelope(second)
        ))
        callback(types.SimpleNamespace(
            payload=snapshot_contract.encode_control_envelope(first)
        ))

        assert cache.revision == 2
        assert cache.get_target("home")["status_text"] == "revision twee"
        assert len(_snapshot_events(worker_bridge_module, hass)) == 2
        diagnostics = hass.data["storm_tracker_v3"][
            "operational_snapshot_diagnostics"
        ]
        assert diagnostics["applied_count"] == 2
        assert diagnostics["rejected_count"] == 2

    asyncio.run(run())


def test_worker_offline_degrades_atomically_to_unknown_stale_never_dry(
    worker_bridge_module,
):
    async def run():
        hass, subscriptions, _published = await _setup_bridge(
            worker_bridge_module
        )
        callbacks = {
            topic: callback for topic, callback, _qos in subscriptions
        }
        callbacks[worker_bridge_module.OPERATIONAL_SNAPSHOT_TOPIC](
            types.SimpleNamespace(
                payload=snapshot_contract.encode_control_envelope(
                    _snapshot(confirmed_ground=True)
                )
            )
        )
        cache = hass.data["storm_tracker_v3"]["operational_snapshot_cache"]
        before = cache.get_target("home")
        assert before["current_precipitation"]["state"] == "raining"
        assert before["current_precipitation"]["ground_evidence"][
            "confirmation"
        ] is True

        offline_status = worker_bridge_module._encode({
            "contract_version": 1,
            "message_type": "worker_status",
            "state": "offline",
        })
        callbacks[worker_bridge_module.STATUS_TOPIC](
            types.SimpleNamespace(payload=offline_status)
        )
        after = cache.get_target("home")
        assert cache.availability == "stale"
        assert after["current_precipitation"]["state"] == "unknown"
        assert after["current_precipitation"]["freshness"] == "stale"
        ground = after["current_precipitation"]["ground_evidence"]
        assert ground["confirmation"] is None
        assert ground["same_track"] is None
        assert after["next_threat"]["state"] == "unknown"
        assert after["next_threat"]["freshness"] == "stale"

        events = _snapshot_events(worker_bridge_module, hass)
        assert [event[1]["reason"] for event in events] == [
            "applied",
            "worker_offline",
        ]
        callbacks[worker_bridge_module.STATUS_TOPIC](
            types.SimpleNamespace(payload=offline_status)
        )
        assert len(_snapshot_events(worker_bridge_module, hass)) == 2

    asyncio.run(run())


def test_snapshot_deadline_is_monotonic_exact_and_ignores_wall_clock_rollback(
    worker_bridge_module,
    monkeypatch,
):
    async def run():
        wall_now = [2_000.0]
        monkeypatch.setattr(
            worker_bridge_module.time,
            "time",
            lambda: wall_now[0],
        )
        hass, subscriptions, _published = await _setup_bridge(
            worker_bridge_module
        )
        callbacks = {topic: callback for topic, callback, _qos in subscriptions}
        callbacks[worker_bridge_module.STATUS_TOPIC](types.SimpleNamespace(
            payload=worker_bridge_module._encode({
                "contract_version": 1,
                "message_type": "worker_status",
                "state": "online",
            })
        ))
        snapshot = _snapshot()
        snapshot["evaluated_at"] = wall_now[0]
        snapshot["valid_until"] = wall_now[0] + 0.03
        callbacks[worker_bridge_module.OPERATIONAL_SNAPSHOT_TOPIC](
            types.SimpleNamespace(
                payload=snapshot_contract.encode_control_envelope(snapshot)
            )
        )
        data = hass.data["storm_tracker_v3"]
        cache = data["operational_snapshot_cache"]
        assert cache.availability == "current"

        # A backwards correction of the wall clock cannot extend the accepted
        # receiver deadline: call_later owns the elapsed-time boundary.
        wall_now[0] -= 3_600.0
        await asyncio.sleep(0.06)
        assert data["worker_status"]["state"] == "online"
        assert cache.availability == "stale"
        assert cache.get_target("home")["current_precipitation"]["state"] == (
            "unknown"
        )
        assert [event[1]["reason"] for event in _snapshot_events(
            worker_bridge_module, hass
        )] == ["applied", "snapshot_expired"]

    asyncio.run(run())


def test_heartbeat_deadline_is_monotonic_and_immediately_fails_snapshot_closed(
    worker_bridge_module,
    monkeypatch,
):
    async def run():
        wall_now = [3_000.0]
        monkeypatch.setattr(worker_bridge_module, "WORKER_STALE_SECONDS", 0.03)
        monkeypatch.setattr(
            worker_bridge_module.time,
            "time",
            lambda: wall_now[0],
        )
        hass, subscriptions, _published = await _setup_bridge(
            worker_bridge_module
        )
        callbacks = {topic: callback for topic, callback, _qos in subscriptions}
        callbacks[worker_bridge_module.STATUS_TOPIC](types.SimpleNamespace(
            payload=worker_bridge_module._encode({
                "contract_version": 1,
                "message_type": "worker_status",
                "state": "degraded",
            })
        ))
        snapshot = _snapshot(1, confirmed_ground=True)
        snapshot["evaluated_at"] = wall_now[0]
        snapshot["valid_until"] = wall_now[0] + 30.0
        callbacks[worker_bridge_module.OPERATIONAL_SNAPSHOT_TOPIC](
            types.SimpleNamespace(
                payload=snapshot_contract.encode_control_envelope(snapshot)
            )
        )
        data = hass.data["storm_tracker_v3"]
        cache = data["operational_snapshot_cache"]
        assert worker_bridge_module.worker_is_available(
            hass, now=wall_now[0]
        )
        assert cache.get_target("home")["current_precipitation"]["state"] == (
            "raining"
        )

        wall_now[0] -= 600.0
        await asyncio.sleep(0.06)
        assert data["worker_status"]["state"] == "offline"
        assert cache.availability == "stale"
        assert cache.get_target("home")["current_precipitation"]["state"] == (
            "unknown"
        )
        assert _snapshot_events(worker_bridge_module, hass)[-1][1]["reason"] == (
            "worker_offline"
        )

    asyncio.run(run())


def test_worker_status_is_size_bounded_and_bridge_timers_unload_cleanly(
    worker_bridge_module,
):
    async def run():
        hass, subscriptions, _published = await _setup_bridge(
            worker_bridge_module
        )
        callbacks = {topic: callback for topic, callback, _qos in subscriptions}
        callbacks[worker_bridge_module.STATUS_TOPIC](types.SimpleNamespace(
            payload=worker_bridge_module._encode({
                "contract_version": 1,
                "message_type": "worker_status",
                "state": "online",
                "worker_version": "1.17.44",
                "compute_profile": "dedicated_nas_fast",
                "cpu_limit_percent": 200,
                "worker_nice": 0,
                "cpu_threads": 2,
                "provider_cycle_seconds": 2.0,
                "queue_depth": 4,
                "failed_jobs": 10**20,
                "operational_snapshot_runtime": {
                    "provider_count": 28,
                    "captured_provider_count": 7,
                    "snapshot_revision": 351,
                    "snapshot_transport_state": "published",
                    "snapshot_rich_built": 12,
                    "snapshot_rich_withheld": 1,
                    "snapshot_rich_builder_failures": 0,
                    "snapshot_unknown_built": 2,
                    "last_tick_workload_id": "lightning.goes19.glm",
                    "last_tick_status": "completed_shadow_raw",
                    "last_tick_reason_code": "shadow_raw_capture_complete",
                    "cycle_status_counts": {
                        "current": 7,
                        "stale": 0,
                        "error": 17,
                        "blocked": 2,
                        "not_applicable": 3,
                    },
                    "raw_calibration": {
                        "status": "ready",
                        "captures": 42,
                        "sources": 2,
                        "providers": 2,
                        "database_bytes": 16_384,
                        "latest_received_at": 1_234.5,
                        "source_statistics": [
                            {
                                "source_id": "lightning.goes19.glm",
                                "provider": "noaa_goes",
                                "captures": 17,
                                "latest_received_at": 1_234.5,
                            },
                            {
                                "source_id": "radar.dwd.radolan_rv",
                                "provider": "dwd",
                                "captures": 25,
                                "latest_received_at": 1_230.0,
                            },
                        ],
                        "untrusted": "x" * 100_000,
                    },
                    "postgis_projection": {
                        "contract": "stv3-postgis-raw-calibration-projection-v1",
                        "enabled": True,
                        "status": "current",
                        "batch": 4,
                        "ready": 3,
                        "leased": 0,
                        "projected": 39,
                        "last_success_at": 1_234.75,
                        "last_error_code": None,
                        "counts": {
                            "flushes": 12,
                            "projected": 39,
                            "retries": 1,
                            "conflicts": 0,
                        },
                    },
                    "parallel_acquisition": {
                        "cooldown_remaining_seconds": 0.0,
                        "fallback_lanes": 0,
                        "lane_limit": 5,
                        "last_batch_duration_seconds": 12.3456,
                        "last_batch_job_count": 5,
                        "last_batch_mean_runtime_seconds": 8.25,
                        "last_unhealthy_reasons": [],
                        "maximum_lanes": 27,
                        "mode": "full_parallel",
                        "concurrency_scope": "workload",
                    },
                    "tracking_active_tracks": 11,
                    "tracking_tracked_tracks": 8,
                    "tracking_motion_ready_tracks": 3,
                    "tracking_region_count": 2,
                    "tracking_shadow_arrival_candidates": 4,
                    "tracking_shadow_future_arrivals": 1,
                    "tracking_shadow_arrival_status": (
                        "shadow_candidate_unadmitted"
                    ),
                },
                "processes": [{"secret": "x" * 700_000}],
            })
        ))
        snapshot = _snapshot()
        callbacks[worker_bridge_module.OPERATIONAL_SNAPSHOT_TOPIC](
            types.SimpleNamespace(
                payload=snapshot_contract.encode_control_envelope(snapshot)
            )
        )
        data = hass.data["storm_tracker_v3"]
        status = data["worker_status"]
        assert "processes" not in status
        assert status["version"] == "1.17.44"
        assert status["compute_profile"] == "dedicated_nas_fast"
        assert status["cpu_limit_percent"] == 200
        assert status["worker_nice"] == 0
        assert status["cpu_threads"] == 2
        assert status["provider_cycle_seconds"] == 2.0
        assert status["failed_jobs"] == worker_bridge_module.MAX_DIAGNOSTIC_COUNT
        assert status["runtime_provider_count"] == 28
        assert status["runtime_captured_provider_count"] == 7
        assert status["worker_snapshot_revision"] == 351
        assert status["raw_calibration_status"] == "ready"
        assert status["raw_calibration_captures"] == 42
        assert status["raw_calibration_sources"] == 2
        assert status["raw_calibration_providers"] == 2
        assert status["postgis_projection_enabled"] is True
        assert status["postgis_projection_status"] == "current"
        assert status["postgis_projection_ready"] == 3
        assert status["postgis_projection_leased"] == 0
        assert status["postgis_projection_projected"] == 39
        assert status["postgis_projection_last_success_at"] == 1_234.75
        assert status["acquisition_parallel_mode"] == "full_parallel"
        assert status["acquisition_parallel_lane_limit"] == 5
        assert status["acquisition_parallel_maximum_lanes"] == 27
        assert status["acquisition_parallel_last_batch_jobs"] == 5
        assert status["acquisition_parallel_last_batch_seconds"] == 12.346
        assert status["acquisition_parallel_last_unhealthy_reasons"] == []
        assert status["tracking_active_tracks"] == 11
        assert status["tracking_tracked_tracks"] == 8
        assert status["tracking_motion_ready_tracks"] == 3
        assert status["tracking_region_count"] == 2
        assert status["tracking_shadow_arrival_candidates"] == 4
        assert status["tracking_shadow_future_arrivals"] == 1
        assert status["tracking_shadow_arrival_status"] == (
            "shadow_candidate_unadmitted"
        )
        assert status["raw_calibration_source_statistics"] == [
            {
                "source_id": "lightning.goes19.glm",
                "provider": "noaa_goes",
                "captures": 17,
                "latest_received_at": 1_234.5,
            },
            {
                "source_id": "radar.dwd.radolan_rv",
                "provider": "dwd",
                "captures": 25,
                "latest_received_at": 1_230.0,
            },
        ]
        assert status["cycle_error"] == 17
        assert status["snapshot_rich_built"] == 12
        assert status["snapshot_rich_withheld"] == 1
        assert status["snapshot_unknown_built"] == 2
        assert status["last_acquisition"] == (
            "lightning.goes19.glm|shadow_raw_capture_complete"
        )
        assert "untrusted" not in status
        assert len(json.dumps(status).encode("utf-8")) < 8_192

        snapshot_handle = data[worker_bridge_module._SNAPSHOT_EXPIRY_HANDLE]
        worker_handle = data[worker_bridge_module._WORKER_STALE_HANDLE]
        for unsubscribe in list(data["unsubscribers"]):
            unsubscribe()
        assert snapshot_handle.cancelled()
        assert worker_handle.cancelled()

        previous = dict(status)
        callbacks[worker_bridge_module.STATUS_TOPIC](types.SimpleNamespace(
            payload=b"x" * (worker_bridge_module.MAX_MQTT_MESSAGE_BYTES + 1)
        ))
        assert data["worker_status"] == previous

    asyncio.run(run())


def test_raw_calibration_source_inventory_rejects_untrusted_shapes(
    worker_bridge_module,
):
    valid_row = {
        "source_id": "radar.dwd.radolan_rv",
        "provider": "dwd",
        "captures": 1,
        "latest_received_at": 1_234.5,
    }
    duplicate = [valid_row, dict(valid_row)]
    assert (
        worker_bridge_module._bounded_raw_calibration_source_statistics(
            duplicate
        )
        is None
    )

    oversized = [
        {
            "source_id": f"source-{index}",
            "provider": "provider",
            "captures": 1,
            "latest_received_at": 1_234.5,
        }
        for index in range(65)
    ]
    assert (
        worker_bridge_module._bounded_raw_calibration_source_statistics(
            oversized
        )
        is None
    )

    unexpected_payload_key = dict(valid_row, payload="not allowed")
    assert (
        worker_bridge_module._bounded_raw_calibration_source_statistics(
            [unexpected_payload_key]
        )
        is None
    )


def test_postgis_projection_diagnostics_are_bounded_and_secret_free(
    worker_bridge_module,
):
    assert worker_bridge_module._bounded_postgis_projection({
        "enabled": True,
        "status": "degraded",
        "batch": 2,
        "ready": 2,
        "leased": 0,
        "projected": 40,
        "last_success_at": 1_234.5,
        "last_error_code": "postgis_projection_failed",
    }) == {
        "enabled": True,
        "status": "degraded",
        "batch": 2,
        "ready": 2,
        "leased": 0,
        "projected": 40,
        "last_success_at": 1_234.5,
        "last_error_code": "postgis_projection_failed",
    }
    assert worker_bridge_module._bounded_postgis_projection({
        "enabled": True,
        "status": "current",
        "batch": 0,
        "ready": 0,
        "leased": 0,
        "projected": 40,
        "last_success_at": 1_234.5,
        "last_error_code": None,
        "password": "must-not-cross-mqtt-boundary",
    }) is None


def test_provider_status_snapshot_is_bounded_and_secret_free(
    worker_bridge_module,
):
    source = {
        "desired_state_sha256": "d" * 64,
        "evaluated_at": 1_789_924_200.0,
        "valid_until": 1_789_924_500.0,
        "items": [{
            "provider_id": "radar.opera.dbzh",
            "display_name": "Opera Radar",
            "readiness": "ready",
            "state": "operational",
            "freshness": "current",
            "product_valid_at": 1_789_924_180.0,
            "received_at": 1_789_924_190.0,
            "error": None,
            "reason_codes": [],
        }],
    }

    assert worker_bridge_module._bounded_provider_status_snapshot(source) == source
    assert worker_bridge_module._bounded_provider_status_snapshot({
        **source,
        "token": "must-not-cross-mqtt-boundary",
    }) is None
    assert worker_bridge_module._bounded_provider_status_snapshot({
        **source,
        "items": [source["items"][0], dict(source["items"][0])],
    }) is None

def test_snapshot_receiver_path_is_sync_memory_only_and_has_no_codec_calls(
    worker_bridge_module,
):
    functions = (
        worker_bridge_module._apply_operational_snapshot_payload,
        worker_bridge_module._mark_operational_snapshot_expired,
        worker_bridge_module._mark_operational_snapshot_worker_offline,
    )
    forbidden_names = {
        "asyncio",
        "base64",
        "gzip",
        "open",
        "sqlite3",
        "run_in_executor",
        "to_thread",
    }
    for function in functions:
        assert not inspect.iscoroutinefunction(function)
        tree = ast.parse(textwrap.dedent(inspect.getsource(function)))
        assert not any(isinstance(node, ast.Await) for node in ast.walk(tree))
        names = {
            node.id
            for node in ast.walk(tree)
            if isinstance(node, ast.Name)
        }
        attributes = {
            node.attr
            for node in ast.walk(tree)
            if isinstance(node, ast.Attribute)
        }
        assert forbidden_names.isdisjoint(names | attributes)


def test_cache_exists_and_stale_snapshot_fails_closed_without_mqtt(
    worker_bridge_module,
):
    async def run():
        async def mqtt_unavailable(_hass):
            return False

        worker_bridge_module.mqtt.async_wait_for_mqtt_client = mqtt_unavailable
        hass = _Hass()
        data = hass.data["storm_tracker_v3"]
        cache = worker_bridge_module.OperationalSnapshotCache()
        cache.apply_bytes(snapshot_contract.encode_control_envelope(_snapshot()))
        data["operational_snapshot_cache"] = cache

        await worker_bridge_module.async_setup_worker_bridge(hass)

        assert data["operational_snapshot_cache"] is cache
        assert cache.availability == "stale"
        target = cache.get_target("home")
        assert target["current_precipitation"]["state"] == "unknown"
        assert target["current_precipitation"]["freshness"] == "stale"
        assert data["worker_status"]["state"] == "offline"
        assert len(_snapshot_events(worker_bridge_module, hass)) == 1

    asyncio.run(run())
