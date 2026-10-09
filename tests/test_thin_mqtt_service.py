from __future__ import annotations


import asyncio
import json
import time
from types import SimpleNamespace

import pytest

import storm_tracker_worker.thin_mqtt_service as mqtt_service
from storm_tracker_worker.protocol import envelope, encode
from storm_tracker_worker.thin_mqtt_service import ThinMqttService


def test_passage_payload_v2_preserves_status_brackets_nulls_and_provenance():
    passage = SimpleNamespace(
        target_id="home", cell_id="cell-a", provider_id="fusion:home",
        entry_at=1_100.0, exit_at=None,
        entry_bracket_lower_at=1_080.0, entry_bracket_upper_at=1_120.0,
        exit_bracket_lower_at=None, exit_bracket_upper_at=None,
        method="advected_labelled_qpe", predicted_intensity_mm_h=2.4,
        peak_intensity_mm_h=4.8, fallback_reason=None,
        horizon_clipped=True, left_censored=False, right_censored=True,
        coverage_censored=False, interval_ordinal=1,
        quantity="precipitation_rate", unit="mm/h",
        field_fingerprint="e" * 64, passage_system_id="system:a",
        passage_status="confirmed", motion_status="ready",
        forecast_issue_id="forecast-issue:immutable-a",
        passage_id="passage:immutable-a-1",
        motion_confidence="medium", motion_method="system_consensus",
        motion_reason_codes=(), motion_supporting_methods=("centroid",),
        motion_agreement_angle_deg=4.0,
    )

    value = mqtt_service._build_passages_contract(SimpleNamespace(
        evaluated_at=1_000.0, target_passages=(passage,),
    ))

    assert value["schema_version"] == 2
    item = value["items"][0]
    assert item["status"] == "confirmed"
    assert item["exit_at"] is None
    assert item["right_censored"] is True
    assert item["entry_bracket"] == {"lower_at": 1_080.0, "upper_at": 1_120.0}
    assert item["exit_bracket"] is None
    assert item["quantity"] == "precipitation_rate"
    assert item["unit"] == "mm/h"
    assert item["forecast_issue_id"] == "forecast-issue:immutable-a"
    assert item["passage_id"] == "passage:immutable-a-1"
    assert item["policy_label"] == "provisional_unvalidated"
    assert item["validation_label"] == "unvalidated"
    assert item["scenario_outcomes"][0]["exit_bracket"] is None


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


def test_live_runtime_starts_all_due_providers_in_one_parallel_wave(monkeypatch):
    captured = {}

    class CapturingProviderRuntime:
        def __init__(self, **kwargs):
            captured.update(kwargs)

    class FakeOperationalRuntime:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

    monkeypatch.setattr(mqtt_service, "ThinProviderRuntime", CapturingProviderRuntime)
    monkeypatch.setattr(mqtt_service, "ThinOperationalRuntime", FakeOperationalRuntime)
    monkeypatch.setattr(
        mqtt_service.ThinPostGISSettings,
        "from_environment",
        classmethod(lambda cls: None),
    )

    mqtt_service._build_runtime()

    assert captured["max_parallel"] == len(captured["adapters"])


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


def test_paho_callback_answers_control_job_while_asyncio_loop_is_stalled():
    """A CPU-bound weather cycle must not block desired-state control I/O."""

    events = []
    mqtt = FakeMqtt(events)
    runtime = SlowRuntime(events)
    service = ThinMqttService(client=mqtt, runtime=runtime)

    class StalledLoop:
        def __init__(self):
            self.callbacks = []

        def call_soon_threadsafe(self, callback):
            self.callbacks.append(callback)

    stalled_loop = StalledLoop()
    service._loop = stalled_loop
    request = envelope(
        "job_request",
        job_id="control-during-cycle-1",
        job_type="desired_state_apply",
        deadline=time.time() + 30,
        payload={"desired_state": {"contract": "test"}},
    )

    service._on_message(
        mqtt,
        None,
        SimpleNamespace(
            topic="storm_tracker/v1/jobs/request",
            payload=encode(request),
        ),
    )

    statuses = [
        payload
        for topic, payload, _, _ in mqtt.published
        if topic == (
            "storm_tracker/v1/jobs/control-during-cycle-1/status"
        )
    ]
    assert runtime.has_state is True
    assert statuses[-1]["state"] == "completed"
    assert statuses[-1]["result"] == {"status": "activated"}


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


def test_heartbeat_exposes_provider_statuses_independently_from_weather_snapshot():
    service = ThinMqttService(
        client=FakeMqtt([]),
        runtime=SlowRuntime([]),
    )
    service._provider_status_snapshot = {
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

    heartbeat = json.loads(service._heartbeat())

    assert heartbeat["operational_snapshot_runtime"][
        "provider_status_snapshot"
    ] == service._provider_status_snapshot


def test_heartbeat_exposes_complete_worker_technical_diagnostics_source():
    service = ThinMqttService(
        client=FakeMqtt([]),
        runtime=SlowRuntime([]),
    )
    service._last_snapshot_revision = 17
    service._last_snapshot_published_at = time.time() - 4.0

    heartbeat = json.loads(service._heartbeat())
    source = heartbeat["technical_diagnostics"]

    assert source["contract"] == (
        "stv3-worker-technical-diagnostics-source-v1"
    )
    assert source["worker_state"] == "online"
    assert source["worker_version"] == mqtt_service.WORKER_VERSION
    assert source["worker_observed_at"] <= time.time()
    assert source["provider_status"] == "current"
    assert source["frame_status"] == "current"
    assert source["cache_status"] == "current"
    assert source["queue_depth"] == 0
    assert source["failed_jobs"] == 0
    assert set(source) == mqtt_service.WORKER_TECHNICAL_SOURCE_FIELDS


@pytest.mark.asyncio
async def test_expired_control_job_failure_does_not_poison_same_job_id_retry():
    events = []
    mqtt = FakeMqtt(events)
    service = ThinMqttService(client=mqtt, runtime=SlowRuntime(events))
    expired = envelope(
        "job_request",
        job_id="control-retry-1",
        job_type="ping",
        deadline=time.time() - 1,
        payload={},
    )
    fresh = envelope(
        "job_request",
        job_id="control-retry-1",
        job_type="ping",
        deadline=time.time() + 30,
        payload={},
    )

    await service.handle_message(
        "storm_tracker/v1/jobs/request", encode(expired)
    )
    await service.handle_message(
        "storm_tracker/v1/jobs/request", encode(fresh)
    )

    statuses = [
        payload
        for topic, payload, _, _ in mqtt.published
        if topic == "storm_tracker/v1/jobs/control-retry-1/status"
    ]
    assert [status["state"] for status in statuses] == [
        "failed",
        "completed",
    ]
    assert statuses[-1]["result"] == {"pong": True}


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
