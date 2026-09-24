from __future__ import annotations


import asyncio
import json
import time

import pytest

import storm_tracker_worker.thin_mqtt_service as mqtt_service
from storm_tracker_worker.protocol import envelope, encode
from storm_tracker_worker.thin_mqtt_service import ThinMqttService


def test_heartbeat_version_tracks_the_installed_worker_package():
    from storm_tracker_worker import __version__
    from storm_tracker_worker.thin_mqtt_service import WORKER_VERSION

    assert WORKER_VERSION == __version__


def test_runtime_adapter_catalog_includes_meteofrance():
    builder = getattr(mqtt_service, "_build_provider_adapters", None)
    assert builder is not None, "runtime adapter builder is required"

    adapters = builder(object())

    assert "meteofrance_radar" in adapters


def test_runtime_adapter_catalog_includes_dpc_sri():
    adapters = mqtt_service._build_provider_adapters(object())

    assert "dpc_radar" in adapters


def test_runtime_adapter_catalog_includes_met_office_rate():
    adapters = mqtt_service._build_provider_adapters(object())

    assert "metoffice_radar" in adapters


def test_runtime_adapter_catalog_includes_global_rainviewer():
    adapters = mqtt_service._build_provider_adapters(object())

    assert "rainviewer" in adapters


def test_runtime_adapter_catalog_includes_kmi_appv4_and_native_access_gate():
    adapters = mqtt_service._build_provider_adapters(object())

    assert "kmi_appv4" in adapters
    assert "kmi_radqpe" in adapters
    assert "kmi_radar" not in adapters


def test_runtime_adapter_catalog_includes_public_kmi_aws_ground_observations():
    adapters = mqtt_service._build_provider_adapters(object())

    assert "kmi_aws" in adapters


def test_runtime_adapter_catalog_includes_italiameteo_icon2i():
    adapters = mqtt_service._build_provider_adapters(object())

    assert "italiameteo_icon2i" in adapters


def test_runtime_adapter_catalog_includes_geosphere_inca():
    adapters = mqtt_service._build_provider_adapters(object())

    assert "geosphere" in adapters


def test_runtime_adapter_catalog_includes_italiameteo_historical_catalog():
    adapters = mqtt_service._build_provider_adapters(object())

    assert "italiameteo_dpc_catalog" in adapters


def test_runtime_adapter_catalog_includes_meteolux_validation():
    adapters = mqtt_service._build_provider_adapters(object())

    assert "meteolux" in adapters


class FakeMqtt:
    def __init__(self, events):
        self.events = events
        self.on_message = None
        self.on_connect = None
        self.published = []

    def will_set(self, *args, **kwargs):
        pass

    def connect_async(self, *args, **kwargs):
        self.events.append("connect_async")

    def loop_start(self):
        self.events.append("loop_started")

    def subscribe(self, topic, qos=0):
        self.events.append(("subscribed", topic, qos))
        return (0, 1)

    def publish(self, topic, payload, qos=0, retain=False):
        message = json.loads(payload)
        self.published.append((topic, message, qos, retain))
        self.events.append(("published", topic, message.get("message_type")))

    def loop_stop(self):
        self.events.append("loop_stopped")

    def disconnect(self):
        self.events.append("disconnected")


class ConnectedOnlyMqtt(FakeMqtt):
    """Model the broker boundary: pre-CONNACK publishes are not delivered."""

    def __init__(self, events):
        super().__init__(events)
        self.connected = False

    def publish(self, topic, payload, qos=0, retain=False):
        if not self.connected:
            return
        super().publish(topic, payload, qos=qos, retain=retain)


class SlowRuntime:
    def __init__(self, events):
        self.events = events
        self.has_state = False

    async def run_cycle(self):
        self.events.append("cycle_started")
        await asyncio.sleep(30)

    def claim_authority(self, producer_id, claim_id):
        return {"status": "claimed"}

    def desired_state_snapshot(self):
        return {"status": "unconfigured"}

    def apply_desired_state(self, document):
        self.has_state = True
        return {"status": "activated"}

    def geojson_collection(self):
        return {
            "type": "FeatureCollection",
            "features": [{
                "type": "Feature",
                "id": "target:home",
                "geometry": {"type": "Point", "coordinates": [4.35, 50.85]},
                "properties": {"layer": "target"},
            }],
            "metadata": {"feature_count": 1},
        }


class CloseTrackingRuntime(SlowRuntime):
    def close(self):
        self.events.append("runtime_closed")


@pytest.mark.asyncio
async def test_stop_closes_runtime_before_mqtt_disconnect():
    events = []
    service = ThinMqttService(
        client=FakeMqtt(events),
        runtime=CloseTrackingRuntime(events),
    )
    await service.start()

    await service.stop()

    assert "runtime_closed" in events
    assert events.index("runtime_closed") < events.index("disconnected")


@pytest.mark.asyncio
async def test_mqtt_loop_and_online_heartbeat_precede_slow_provider_cycle():
    events = []
    mqtt = FakeMqtt(events)
    runtime = SlowRuntime(events)
    service = ThinMqttService(
        client=mqtt,
        runtime=runtime,
        heartbeat_seconds=10,
        cycle_seconds=60,
    )

    started = time.monotonic()
    await service.start()
    elapsed = time.monotonic() - started

    assert elapsed < 1.0
    loop_index = events.index("loop_started")
    heartbeat_index = next(
        index for index, event in enumerate(events)
        if isinstance(event, tuple) and event[1].endswith("worker/status")
    )
    assert loop_index < heartbeat_index
    assert "cycle_started" not in events
    await service.stop()


@pytest.mark.asyncio
async def test_desired_state_job_is_answered_and_wakes_immediate_cycle():
    events = []
    mqtt = FakeMqtt(events)
    runtime = SlowRuntime(events)
    service = ThinMqttService(client=mqtt, runtime=runtime)
    await service.start()
    request = envelope(
        "job_request",
        job_id="job-1",
        job_type="desired_state_apply",
        deadline=time.time() + 30,
        payload={"desired_state": {"contract": "test"}},
    )

    await service.handle_message("storm_tracker/v1/jobs/request", encode(request))
    await asyncio.sleep(0)

    statuses = [
        payload for topic, payload, _, _ in mqtt.published
        if topic == "storm_tracker/v1/jobs/job-1/status"
    ]
    assert statuses[-1]["state"] == "completed"
    assert statuses[-1]["result"] == {"status": "activated"}
    assert "cycle_started" in events
    await service.stop()


@pytest.mark.asyncio
async def test_job_subscription_is_reapplied_after_async_connect():
    events = []
    mqtt = FakeMqtt(events)
    service = ThinMqttService(client=mqtt, runtime=SlowRuntime(events))

    await service.start()
    assert callable(mqtt.on_connect)
    before = sum(
        event == ("subscribed", "storm_tracker/v1/jobs/request", 1)
        for event in events
    )

    mqtt.on_connect(mqtt, None, None, 0)

    after = sum(
        event == ("subscribed", "storm_tracker/v1/jobs/request", 1)
        for event in events
    )
    assert before == 1
    assert after == 2
    await service.stop()


@pytest.mark.asyncio
async def test_successful_connect_immediately_publishes_retained_online_status():
    events = []
    mqtt = ConnectedOnlyMqtt(events)
    service = ThinMqttService(client=mqtt, runtime=SlowRuntime(events))

    await service.start()
    mqtt.connected = True
    mqtt.on_connect(mqtt, None, None, 0)

    statuses = [
        (payload, qos, retain)
        for topic, payload, qos, retain in mqtt.published
        if topic == "storm_tracker/v1/worker/status"
    ]
    assert len(statuses) == 1
    assert statuses[0][0]["state"] == "online"
    assert statuses[0][1:] == (1, True)
    await service.stop()


@pytest.mark.asyncio
async def test_heartbeat_continues_when_asyncio_event_loop_is_blocked():
    events = []
    mqtt = ConnectedOnlyMqtt(events)
    service = ThinMqttService(
        client=mqtt,
        runtime=SlowRuntime(events),
        heartbeat_seconds=2,
    )

    await service.start()
    mqtt.connected = True
    mqtt.on_connect(mqtt, None, None, 0)
    mqtt.published.clear()

    time.sleep(2.3)

    statuses = [
        payload
        for topic, payload, _, _ in mqtt.published
        if topic == "storm_tracker/v1/worker/status"
    ]
    assert statuses
    assert statuses[-1]["state"] == "online"
    await service.stop()


def test_heartbeat_exposes_latest_postgis_projection_for_ha_dashboard():
    service = ThinMqttService(
        client=FakeMqtt([]),
        runtime=SlowRuntime([]),
    )
    service._postgis_projection_enabled = True
    service._postgis_projection_status = "current"
    service._postgis_projection_batch = 17
    service._postgis_projection_ready = 0
    service._postgis_projection_leased = 0
    service._postgis_projection_projected = 42
    service._postgis_projection_last_success_at = 1_789_924_200.0
    service._postgis_projection_last_error_code = None

    heartbeat = json.loads(service._heartbeat())
    projection = heartbeat["operational_snapshot_runtime"][
        "postgis_projection"
    ]

    assert projection == {
        "enabled": True,
        "status": "current",
        "batch": 17,
        "ready": 0,
        "leased": 0,
        "projected": 42,
        "last_success_at": 1_789_924_200.0,
        "last_error_code": None,
    }


@pytest.mark.asyncio
async def test_geojson_job_returns_latest_thin_map_without_starting_legacy_runtime():
    events = []
    mqtt = FakeMqtt(events)
    service = ThinMqttService(client=mqtt, runtime=SlowRuntime(events))
    request = envelope(
        "job_request",
        job_id="map-1",
        job_type="geojson_operational",
        deadline=time.time() + 30,
        payload={},
    )

    await service.handle_message("storm_tracker/v1/jobs/request", encode(request))

    status = next(
        payload for topic, payload, _, _ in mqtt.published
        if topic == "storm_tracker/v1/jobs/map-1/status"
    )
    assert status["state"] == "completed"
    assert status["result"]["collection"]["metadata"]["feature_count"] == 1


def test_thin_mode_has_direct_entrypoint_without_legacy_service_owners():
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    runner = (root / "storm_tracker_worker" / "run.sh").read_text(encoding="utf-8")
    config = (root / "storm_tracker_worker" / "config.yaml").read_text(encoding="utf-8")

    assert "thin_operational)" in runner
    assert "storm_tracker_worker.thin_mqtt_service" in runner
    assert "legacy|worker_first_canary|raw_capture_shadow|worker_only|thin_operational" in config
    thin_exec = runner[runner.index('if [[ "${STV3_RUNTIME_MODE}" == "thin_operational" ]]'):]
    assert "storm_tracker_worker.service" in thin_exec
    assert thin_exec.index("storm_tracker_worker.thin_mqtt_service") < thin_exec.index("storm_tracker_worker.service")


def test_thin_mode_packages_async_http_client():
    from pathlib import Path

    root = Path(__file__).resolve().parents[1] / "storm_tracker_worker"
    dockerfile = (root / "Dockerfile").read_text(encoding="utf-8")
    requirements = (root / "requirements.txt").read_text(encoding="utf-8")

    assert "aiohttp==" in dockerfile
    assert "aiohttp==" in requirements
