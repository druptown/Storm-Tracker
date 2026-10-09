"""Exclusive worker-only config-entry cut-over and lifecycle proof."""
from __future__ import annotations

import ast
import asyncio
import copy
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sys
import threading
import types

import pytest


ROOT = Path(__file__).resolve().parents[1]
COMPONENT = ROOT / "custom_components/storm_tracker_v3"
ENTRY_PATH = COMPONENT / "worker_only_entry.py"
NOW = 1_800_000_000.0


def _load_file(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class FakeConfigEntryNotReady(RuntimeError):
    pass


class FakePrepared:
    def __init__(self):
        self.entities = (
            "worker",
            "worker_technical",
            "home",
            "storms",
            "regions",
        )


class FakeRuntime:
    instances = []
    fail_prepare = False
    fail_start = False

    def __init__(self, hass, *, config_snapshot, target_snapshot):
        self.hass = hass
        self.config_snapshot = copy.deepcopy(config_snapshot)
        self.target_snapshot = copy.deepcopy(target_snapshot)
        self.prepared = FakePrepared()
        self.prepare_calls = 0
        self.start_calls = 0
        self.unload_calls = 0
        self.reconnect_calls = 0
        self.updates = []
        type(self).instances.append(self)

    def prepare(self):
        self.prepare_calls += 1
        if type(self).fail_prepare:
            raise RuntimeError("prepare failed")
        return self.prepared

    async def async_start(self, *, worker_admission_ready=True):
        self.start_calls += 1
        self.worker_admission_ready = worker_admission_ready
        if type(self).fail_start:
            raise RuntimeError("start failed")
        return self.prepared

    async def async_update(self, *, config_snapshot, target_snapshot):
        self.updates.append((
            copy.deepcopy(config_snapshot),
            copy.deepcopy(target_snapshot),
        ))
        await asyncio.sleep(0)
        return True

    async def async_worker_reconnected(self):
        self.reconnect_calls += 1

    async def async_unload(self):
        self.unload_calls += 1


class FakeState:
    def __init__(
        self,
        state="home",
        *,
        latitude=None,
        longitude=None,
        observed_at=NOW,
        **attributes,
    ):
        self.state = state
        self.attributes = dict(attributes)
        if latitude is not None:
            self.attributes["latitude"] = latitude
        if longitude is not None:
            self.attributes["longitude"] = longitude
        self.last_updated = datetime.fromtimestamp(observed_at, timezone.utc)
        self.last_changed = self.last_updated


class FakeTimerHandle:
    def __init__(self, delay, callback, args):
        self.delay = delay
        self.callback = callback
        self.args = args
        self.cancel_calls = 0
        self.fired = False

    def cancel(self):
        self.cancel_calls += 1

    def fire(self):
        assert self.cancel_calls == 0
        assert not self.fired
        self.fired = True
        self.callback(*self.args)


class FakeStates:
    def __init__(self, values=None):
        self.values = dict(values or {})

    def get(self, entity_id):
        return self.values.get(entity_id)


class FakeBus:
    def __init__(self):
        self.listeners = {}
        self.unsubscribed = 0

    def async_listen(self, event_name, callback):
        assert event_name not in self.listeners
        self.listeners[event_name] = callback
        active = True

        def unsubscribe():
            nonlocal active
            if active:
                active = False
                self.unsubscribed += 1
                self.listeners.pop(event_name, None)

        return unsubscribe


class FakeConfigEntries:
    def __init__(self):
        self.forward_calls = []
        self.unload_calls = []
        self.reload_calls = []
        self.fail_forward = False
        self.unload_result = True

    async def async_forward_entry_setups(self, entry, platforms):
        self.forward_calls.append((entry.entry_id, tuple(platforms)))
        if self.fail_forward:
            raise RuntimeError("forward failed")

    async def async_unload_platforms(self, entry, platforms):
        self.unload_calls.append((entry.entry_id, tuple(platforms)))
        return self.unload_result

    async def async_reload(self, entry_id):
        self.reload_calls.append(entry_id)


class FakeEntry:
    def __init__(self, data, *, entry_id="entry-a", options=None, version=3):
        self.entry_id = entry_id
        self.data = dict(data)
        self.options = dict(options or {})
        self.version = version
        self.update_listeners = []
        self.option_unsubscribed = 0

    def add_update_listener(self, callback):
        self.update_listeners.append(callback)
        active = True

        def unsubscribe():
            nonlocal active
            if active:
                active = False
                self.option_unsubscribed += 1

        return unsubscribe


class FakeHass:
    def __init__(self, states=None):
        self.data = {}
        self.states = FakeStates(states)
        self.bus = FakeBus()
        self.config_entries = FakeConfigEntries()
        self.config = types.SimpleNamespace(
            latitude=51.04,
            longitude=4.48,
            country="BE",
        )
        self.loop = asyncio.get_running_loop()
        self.task_thread_ids = []

    def async_create_task(self, coroutine, _name=None):
        self.task_thread_ids.append(threading.get_ident())
        return asyncio.create_task(coroutine)


class RealLifecycleSensorEntity:
    """Small SensorEntity surface for loading the real worker-only stack."""

    hass = None

    async def async_added_to_hass(self):
        return None

    def async_on_remove(self, callback):
        callbacks = getattr(self, "_test_remove_callbacks", None)
        if callbacks is None:
            callbacks = []
            self._test_remove_callbacks = callbacks
        callbacks.append(callback)

    def async_write_ha_state(self):
        self._test_write_count = getattr(self, "_test_write_count", 0) + 1


class RealLifecycleBus(FakeBus):
    """Synchronous event bus adequate for the real control-plane callbacks."""

    def __init__(self):
        super().__init__()
        self.events = []

    def async_fire(self, event_name, data=None):
        event = types.SimpleNamespace(
            event_type=event_name,
            data=dict(data or {}),
        )
        self.events.append(event)
        callback = self.listeners.get(event_name)
        if callback is not None:
            callback(event)


class RealLifecycleHass(FakeHass):
    """Fake HA instance that exposes the loop used by the real bridge."""

    def __init__(self, states=None):
        super().__init__(states)
        self.bus = RealLifecycleBus()
        self.loop = asyncio.get_running_loop()


@pytest.fixture
def entry_module(monkeypatch):
    package_name = "stv3_worker_only_entry_test_package"
    package = types.ModuleType(package_name)
    package.__path__ = [str(COMPONENT)]
    monkeypatch.setitem(sys.modules, package_name, package)

    homeassistant = types.ModuleType("homeassistant")
    core = types.ModuleType("homeassistant.core")
    core.HomeAssistant = object
    exceptions = types.ModuleType("homeassistant.exceptions")
    exceptions.ConfigEntryNotReady = FakeConfigEntryNotReady
    helpers = types.ModuleType("homeassistant.helpers")
    event = types.ModuleType("homeassistant.helpers.event")
    state_registrations = []

    def async_track_state_change_event(hass, entity_ids, callback):
        registration = {
            "hass": hass,
            "entity_ids": tuple(entity_ids),
            "callback": callback,
            "unsubscribed": 0,
        }
        state_registrations.append(registration)

        def unsubscribe():
            registration["unsubscribed"] += 1

        return unsubscribe

    event.async_track_state_change_event = async_track_state_change_event
    homeassistant.core = core
    homeassistant.exceptions = exceptions
    homeassistant.helpers = helpers
    helpers.event = event
    for name, module in {
        "homeassistant": homeassistant,
        "homeassistant.core": core,
        "homeassistant.exceptions": exceptions,
        "homeassistant.helpers": helpers,
        "homeassistant.helpers.event": event,
    }.items():
        monkeypatch.setitem(sys.modules, name, module)

    const = types.ModuleType(f"{package_name}.const")
    const.CONF_RUNTIME_MODE = "runtime_mode"
    const.DATA_ENTRY_RUNTIMES = "entry_runtimes"
    const.DATA_WORKER_TECHNICAL_DIAGNOSTICS = (
        "worker_technical_diagnostics"
    )
    const.DATA_WORKER_TECHNICAL_SOURCE = "worker_technical_source"
    const.DOMAIN = "storm_tracker_v3"
    const.INTEGRATION_VERSION = "1.14.46"
    const.RUNTIME_MODE_WORKER_ONLY = "worker_only"
    monkeypatch.setitem(sys.modules, f"{package_name}.const", const)

    runtime = types.ModuleType(f"{package_name}.worker_only_runtime")
    runtime.MAX_ADDITIONAL_TARGET_ENTITIES = 59
    runtime.MAX_RUNTIME_TARGETS = 64
    runtime.PreparedWorkerOnlyRuntime = FakePrepared
    runtime.WorkerOnlyRuntimeCandidate = FakeRuntime
    monkeypatch.setitem(
        sys.modules, f"{package_name}.worker_only_runtime", runtime
    )

    FakeRuntime.instances = []
    FakeRuntime.fail_prepare = False
    FakeRuntime.fail_start = False
    loaded = _load_file(f"{package_name}.worker_only_entry", ENTRY_PATH)
    loaded.TARGET_RECONCILE_DELAY_SECONDS = 0.0
    loaded._test_state_registrations = state_registrations
    return loaded


@pytest.fixture
def real_worker_only_entry_modules(monkeypatch):
    """Load the real entry, runtime, bridge and controller as one stack."""

    package_name = "stv3_worker_only_real_lifecycle_test_package"
    package = types.ModuleType(package_name)
    package.__path__ = [str(COMPONENT)]
    monkeypatch.setitem(sys.modules, package_name, package)

    homeassistant = types.ModuleType("homeassistant")
    components = types.ModuleType("homeassistant.components")
    mqtt = types.ModuleType("homeassistant.components.mqtt")
    sensor_module = types.ModuleType("homeassistant.components.sensor")
    sensor_module.SensorEntity = RealLifecycleSensorEntity
    core = types.ModuleType("homeassistant.core")
    core.HomeAssistant = object
    core.callback = lambda function: function
    exceptions = types.ModuleType("homeassistant.exceptions")
    exceptions.ConfigEntryNotReady = FakeConfigEntryNotReady
    helpers = types.ModuleType("homeassistant.helpers")
    event = types.ModuleType("homeassistant.helpers.event")

    mqtt_control = types.SimpleNamespace(
        available=False,
        subscriptions=[],
        publications=[],
    )
    state_registrations = []
    interval_registrations = []

    async def async_wait_for_mqtt_client(_hass):
        return mqtt_control.available

    async def async_subscribe(
        hass, topic, callback, qos=0, encoding="utf-8"
    ):
        registration = {
            "hass": hass,
            "topic": topic,
            "callback": callback,
            "qos": qos,
            "encoding": encoding,
            "active": True,
            "unsubscribed": 0,
        }
        mqtt_control.subscriptions.append(registration)

        def unsubscribe():
            if registration["active"]:
                registration["active"] = False
                registration["unsubscribed"] += 1

        return unsubscribe

    async def async_publish(hass, topic, payload, qos=0, retain=False):
        mqtt_control.publications.append({
            "hass": hass,
            "topic": topic,
            "payload": payload,
            "qos": qos,
            "retain": retain,
        })

    def async_track_state_change_event(hass, entity_ids, callback):
        registration = {
            "hass": hass,
            "entity_ids": tuple(entity_ids),
            "callback": callback,
            "active": True,
            "unsubscribed": 0,
        }
        state_registrations.append(registration)

        def unsubscribe():
            if registration["active"]:
                registration["active"] = False
                registration["unsubscribed"] += 1

        return unsubscribe

    def async_track_time_interval(hass, callback, interval):
        registration = {
            "hass": hass,
            "callback": callback,
            "interval": interval,
            "active": True,
            "unsubscribed": 0,
        }
        interval_registrations.append(registration)

        def unsubscribe():
            if registration["active"]:
                registration["active"] = False
                registration["unsubscribed"] += 1

        return unsubscribe

    mqtt.async_wait_for_mqtt_client = async_wait_for_mqtt_client
    mqtt.async_subscribe = async_subscribe
    mqtt.async_publish = async_publish
    event.async_track_state_change_event = async_track_state_change_event
    event.async_track_time_interval = async_track_time_interval
    homeassistant.components = components
    homeassistant.core = core
    homeassistant.exceptions = exceptions
    homeassistant.helpers = helpers
    components.mqtt = mqtt
    components.sensor = sensor_module
    helpers.event = event
    for name, module in {
        "homeassistant": homeassistant,
        "homeassistant.components": components,
        "homeassistant.components.mqtt": mqtt,
        "homeassistant.components.sensor": sensor_module,
        "homeassistant.core": core,
        "homeassistant.exceptions": exceptions,
        "homeassistant.helpers": helpers,
        "homeassistant.helpers.event": event,
    }.items():
        monkeypatch.setitem(sys.modules, name, module)

    loaded = {}
    for short_name in (
        "const",
        "operational_snapshot",
        "operational_snapshot_views",
        "operational_snapshot_sensors",
        "worker_technical_diagnostics",
        "worker_technical_diagnostics_sensor",
        "desired_state_builder",
        "worker_bridge",
        "desired_state_controller",
        "worker_only_runtime",
        "worker_only_entry",
    ):
        loaded[short_name] = _load_file(
            f"{package_name}.{short_name}", COMPONENT / f"{short_name}.py"
        )
    loaded["worker_only_entry"].TARGET_RECONCILE_DELAY_SECONDS = 0.0
    return types.SimpleNamespace(
        **loaded,
        mqtt_control=mqtt_control,
        state_registrations=state_registrations,
        interval_registrations=interval_registrations,
    )


def _entry_data(*, persons=("device_tracker.wim",), test_tracker=None):
    result = {
        "runtime_mode": "worker_only",
        "persons": list(persons),
        "radar_radius_km": 250.0,
        "engine_sharing_distance_km": 150.0,
        "netatmo_radius_km": 20.0,
        "lightning_source_mode": "auto",
        "open_meteo_enabled": False,
    }
    if test_tracker is not None:
        result["test_tracker_entity"] = test_tracker
    return result


def _states(*, with_person=True, with_test=False):
    values = {
        "zone.home": FakeState(
            latitude=51.04,
            longitude=4.48,
            observed_at=NOW - 100,
            friendly_name="Home",
        )
    }
    if with_person:
        values["device_tracker.wim"] = FakeState(
            latitude=51.10,
            longitude=4.50,
            observed_at=NOW - 10,
            friendly_name="Wim",
            gps_accuracy=12,
            country_code="be",
        )
    if with_test:
        values["device_tracker.test"] = FakeState(
            latitude=40.78,
            longitude=-73.96,
            observed_at=NOW - 5,
            friendly_name="New York test",
            country_code="US",
        )
    return values


async def _drain(turns=8):
    for _ in range(turns):
        await asyncio.sleep(0)


def test_worker_only_modules_have_a_light_top_level_import_boundary():
    init_tree = ast.parse((COMPONENT / "__init__.py").read_text(encoding="utf-8"))
    sensor_tree = ast.parse((COMPONENT / "sensor.py").read_text(encoding="utf-8"))

    def top_level_imports(tree):
        return {
            node.module or ""
            for node in tree.body
            if isinstance(node, ast.ImportFrom)
        }

    init_imports = top_level_imports(init_tree)
    sensor_imports = top_level_imports(sensor_tree)
    assert "homeassistant.helpers.storage" not in init_imports
    assert not any(
        value.startswith(("engine", "providers", "http", "geometry"))
        for value in init_imports
    )
    assert not any(
        value.startswith(("engine", "geojson_worker"))
        for value in sensor_imports
    )
    source = ENTRY_PATH.read_text(encoding="utf-8")
    for forbidden in (
        "providers.",
        "helpers.storage",
        "async_add_executor_job",
        "register_view",
        "sqlite",
    ):
        assert forbidden not in source.lower()


@pytest.mark.asyncio
async def test_setup_is_exclusive_bounded_and_forwards_only_snapshot_sensors(entry_module):
    hass = FakeHass(_states(with_test=True))
    entry = FakeEntry(
        _entry_data(test_tracker="device_tracker.test"),
        entry_id="abc123",
    )

    assert await entry_module.async_setup_worker_only_entry(hass, entry)
    runtime = FakeRuntime.instances[-1]
    assert runtime.start_calls == 1
    assert runtime.prepare_calls == 1
    assert hass.config_entries.forward_calls == [("abc123", ("sensor",))]
    assert len(entry_module._test_state_registrations) == 1
    assert entry_module._test_state_registrations[0]["entity_ids"] == (
        "device_tracker.test",
        "device_tracker.wim",
        "zone.home",
    )
    assert tuple(hass.bus.listeners) == (
        "storm_tracker_v3_worker_status_update",
    )
    assert runtime.config_snapshot == {
        "producer_id": "homeassistant.storm_tracker_v3.abc123",
        "unique_id_prefix": "storm_tracker_v3.abc123",
        "home_target_id": "home",
        "sensor_target_ids": ["person.wim", "test_tracker"],
        "settings": {
            "ground_radius_km": 20.0,
            "lightning_source_mode": "auto",
            "model_guidance_enabled": False,
            "region_sharing_distance_km": 150.0,
        },
    }
    targets = {target["target_id"]: target for target in runtime.target_snapshot}
    assert set(targets) == {"home", "person.wim", "test_tracker"}
    assert targets["person.wim"]["location_accuracy_m"] == 12.0
    assert targets["person.wim"]["country_code"] == "BE"
    assert targets["person.wim"]["position_observed_at"] == NOW - 10
    assert targets["test_tracker"]["kind"] == "test"
    prepared = entry_module.get_prepared_worker_only_runtime(hass, "abc123")
    assert prepared is runtime.prepared


@pytest.mark.asyncio
async def test_managed_test_tracker_publishes_modern_retained_discovery(
    entry_module,
    monkeypatch,
):
    entity_id = entry_module.MANAGED_TEST_TRACKER_ENTITY_ID
    states = _states()
    states[entity_id] = FakeState(
        latitude=50.1109,
        longitude=8.6821,
        observed_at=NOW - 5,
        friendly_name="Fictieve Tracker",
        gps_accuracy=10,
        country_code="DE",
    )
    hass = FakeHass(states)
    entry = FakeEntry(_entry_data(test_tracker=entity_id), entry_id="mqtt-test")
    publications = []

    mqtt = types.ModuleType("homeassistant.components.mqtt")

    async def async_wait_for_mqtt_client(_hass):
        return True

    async def async_publish(_hass, topic, payload, qos=0, retain=False):
        publications.append((topic, payload, qos, retain))

    mqtt.async_wait_for_mqtt_client = async_wait_for_mqtt_client
    mqtt.async_publish = async_publish
    components = types.ModuleType("homeassistant.components")
    components.mqtt = mqtt
    monkeypatch.setitem(sys.modules, "homeassistant.components", components)
    monkeypatch.setitem(sys.modules, "homeassistant.components.mqtt", mqtt)

    assert await entry_module.async_setup_worker_only_entry(hass, entry)
    assert len(publications) == 1
    topic, payload, qos, retain = publications[0]
    assert topic == entry_module.MANAGED_TEST_TRACKER_DISCOVERY_TOPIC
    assert qos == 0
    assert retain is True
    decoded = json.loads(payload)
    assert decoded["default_entity_id"] == entity_id
    assert decoded["unique_id"] == "storm_tracker_v3_fictieve_tracker"
    assert decoded["source_type"] == "gps"
    assert decoded["origin"]["sw_version"] == "1.14.46"
    assert FakeRuntime.instances[-1].target_snapshot[-1]["latitude"] == 50.1109


@pytest.mark.asyncio
async def test_worker_only_legacy_credentials_are_preserved_but_never_projected(
    entry_module,
):
    data_secret = "DATA_SECRET_SENTINEL_7f531"
    option_secret = "OPTION_SECRET_SENTINEL_91ac2"
    entry_data = _entry_data()
    entry_data.update({
        "knmi_api_key": data_secret,
        "eumetsat_consumer_secret": data_secret,
        "raw_retention_authority": data_secret,
    })
    entry = FakeEntry(
        entry_data,
        options={
            "netatmo_client_secret": option_secret,
            "hsaf_password": option_secret,
            "raw_retention_opt_in": option_secret,
        },
    )
    hass = FakeHass(_states())

    assert await entry_module.async_setup_worker_only_entry(hass, entry)
    runtime = FakeRuntime.instances[-1]
    record = hass.data["storm_tracker_v3"]["entry_runtimes"]["entry-a"]
    diagnostics = hass.data["storm_tracker_v3"]["runtime_diagnostics"]

    assert set(record["manager"]._runtime_config()) <= (
        entry_module.WORKER_ONLY_ENTRY_CONFIG_FIELDS
    )
    projected_surface = repr({
        "config_snapshot": runtime.config_snapshot,
        "target_snapshot": runtime.target_snapshot,
        "runtime_diagnostics": diagnostics,
    })
    for forbidden in (
        data_secret,
        option_secret,
        "knmi_api_key",
        "eumetsat_consumer_secret",
        "netatmo_client_secret",
        "hsaf_password",
        "raw_retention_authority",
        "raw_retention_opt_in",
    ):
        assert forbidden not in projected_surface

    assert entry.data["knmi_api_key"] == data_secret
    assert entry.options["netatmo_client_secret"] == option_secret
    assert diagnostics["provisioning"] == {
        "contract": "stv3-worker-provisioning-ownership-v1",
        "owner_location": "supervisor_worker_app",
        "status": "worker_app_owned_not_observed",
        "availability_claimed": False,
    }


@pytest.mark.asyncio
async def test_initial_missing_tracker_does_not_block_entry_and_joins_when_available(
    entry_module,
):
    hass = FakeHass(_states(with_person=False))
    entry = FakeEntry(_entry_data())

    assert await entry_module.async_setup_worker_only_entry(hass, entry)
    runtime = FakeRuntime.instances[-1]
    assert [
        target["target_id"] for target in runtime.target_snapshot
    ] == ["home"]
    assert runtime.config_snapshot["sensor_target_ids"] == []

    registration = entry_module._test_state_registrations[0]
    assert "device_tracker.wim" in registration["entity_ids"]
    hass.states.values["device_tracker.wim"] = FakeState(
        latitude=51.10,
        longitude=4.50,
        observed_at=NOW + 30,
        friendly_name="Wim",
    )
    registration["callback"](object())
    await _drain()

    assert len(runtime.updates) == 1
    assert [
        target["target_id"] for target in runtime.updates[-1][1]
    ] == ["home", "person.wim"]
    assert runtime.updates[-1][0]["sensor_target_ids"] == ["person.wim"]


@pytest.mark.asyncio
async def test_initial_stale_life360_expires_but_operator_test_tracker_stays_available(
    entry_module,
):
    states = _states(with_test=True)
    stale_at = NOW - entry_module.MAX_DYNAMIC_POSITION_AGE_SECONDS - 1
    states["device_tracker.wim"] = FakeState(
        latitude=51.10,
        longitude=4.50,
        observed_at=stale_at,
        friendly_name="Wim",
    )
    states["device_tracker.test"] = FakeState(
        latitude=40.78,
        longitude=-73.96,
        observed_at=stale_at,
        friendly_name="New York test",
    )
    hass = FakeHass(states)
    entry = FakeEntry(_entry_data(test_tracker="device_tracker.test"))
    manager = entry_module.WorkerOnlyEntryRuntime(
        hass,
        entry,
        clock=lambda: NOW,
    )

    assert await manager.async_setup()
    targets = {
        target["target_id"]: target
        for target in FakeRuntime.instances[-1].target_snapshot
    }
    assert targets["home"]["available"] is True
    assert targets["person.wim"]["available"] is False
    assert targets["test_tracker"]["available"] is True
    assert (
        targets["person.wim"]["latitude"],
        targets["person.wim"]["longitude"],
        targets["person.wim"]["position_observed_at"],
    ) == (51.10, 4.50, stale_at)
    assert (
        targets["test_tracker"]["latitude"],
        targets["test_tracker"]["longitude"],
        targets["test_tracker"]["position_observed_at"],
    ) == (40.78, -73.96, NOW)
    assert await entry_module.async_unload_worker_only_entry(hass, entry)


@pytest.mark.asyncio
async def test_initial_fresh_dynamic_targets_remain_available(entry_module):
    states = _states(with_test=True)
    fresh_at = NOW - entry_module.MAX_DYNAMIC_POSITION_AGE_SECONDS + 1
    states["device_tracker.wim"].last_updated = datetime.fromtimestamp(
        fresh_at, timezone.utc
    )
    states["device_tracker.wim"].last_changed = states[
        "device_tracker.wim"
    ].last_updated
    states["device_tracker.test"].last_updated = datetime.fromtimestamp(
        fresh_at, timezone.utc
    )
    states["device_tracker.test"].last_changed = states[
        "device_tracker.test"
    ].last_updated
    hass = FakeHass(states)
    entry = FakeEntry(_entry_data(test_tracker="device_tracker.test"))
    manager = entry_module.WorkerOnlyEntryRuntime(
        hass,
        entry,
        clock=lambda: NOW,
    )

    assert await manager.async_setup()
    targets = {
        target["target_id"]: target
        for target in FakeRuntime.instances[-1].target_snapshot
    }
    assert targets["person.wim"]["available"] is True
    assert targets["test_tracker"]["available"] is True
    assert targets["person.wim"]["position_observed_at"] == fresh_at
    assert targets["test_tracker"]["position_observed_at"] == NOW
    assert await entry_module.async_unload_worker_only_entry(hass, entry)


@pytest.mark.asyncio
async def test_implausibly_future_dynamic_target_is_unavailable(entry_module):
    states = _states()
    future_at = (
        NOW + entry_module.MAX_DYNAMIC_POSITION_FUTURE_SKEW_SECONDS + 1
    )
    states["device_tracker.wim"] = FakeState(
        latitude=51.10,
        longitude=4.50,
        observed_at=future_at,
        friendly_name="Wim",
    )
    hass = FakeHass(states)
    entry = FakeEntry(_entry_data())
    manager = entry_module.WorkerOnlyEntryRuntime(
        hass,
        entry,
        clock=lambda: NOW,
    )

    assert await manager.async_setup()
    target = next(
        target
        for target in FakeRuntime.instances[-1].target_snapshot
        if target["target_id"] == "person.wim"
    )
    assert target["available"] is False
    assert target["position_observed_at"] == future_at
    await manager.async_unload()


@pytest.mark.asyncio
async def test_fresh_target_automatically_expires_at_single_deadline(
    entry_module,
    monkeypatch,
):
    now = [NOW]
    observed_at = NOW - 10
    states = _states()
    states["device_tracker.wim"] = FakeState(
        latitude=51.10,
        longitude=4.50,
        observed_at=observed_at,
        friendly_name="Wim",
    )
    hass = FakeHass(states)
    scheduled = []

    def call_later(delay, callback, *args):
        handle = FakeTimerHandle(delay, callback, args)
        scheduled.append(handle)
        return handle

    monkeypatch.setattr(hass.loop, "call_later", call_later)
    entry = FakeEntry(_entry_data())
    manager = entry_module.WorkerOnlyEntryRuntime(
        hass,
        entry,
        clock=lambda: now[0],
        target_reconcile_delay_seconds=60.0,
    )

    assert await manager.async_setup()
    assert len(scheduled) == 1
    assert scheduled[0].delay == pytest.approx(
        entry_module.MAX_DYNAMIC_POSITION_AGE_SECONDS
        + entry_module._FRESHNESS_DEADLINE_GRACE_SECONDS
        - 10
    )

    now[0] = observed_at + entry_module.MAX_DYNAMIC_POSITION_AGE_SECONDS
    scheduled[0].fire()
    await _drain()

    runtime = FakeRuntime.instances[-1]
    assert runtime.updates == []
    assert len(scheduled) == 2
    assert scheduled[1].delay == pytest.approx(
        entry_module._FRESHNESS_DEADLINE_GRACE_SECONDS
    )

    now[0] += entry_module._FRESHNESS_DEADLINE_GRACE_SECONDS
    scheduled[1].fire()
    await _drain()

    assert len(runtime.updates) == 1
    expired = next(
        target
        for target in runtime.updates[-1][1]
        if target["target_id"] == "person.wim"
    )
    assert expired["available"] is False
    assert expired["position_observed_at"] == observed_at
    assert len(scheduled) == 2
    assert manager._freshness_handle is None
    await manager.async_unload()


@pytest.mark.asyncio
async def test_unload_cancels_owned_freshness_deadline(
    entry_module,
    monkeypatch,
):
    hass = FakeHass(_states())
    scheduled = []

    def call_later(delay, callback, *args):
        handle = FakeTimerHandle(delay, callback, args)
        scheduled.append(handle)
        return handle

    monkeypatch.setattr(hass.loop, "call_later", call_later)
    entry = FakeEntry(_entry_data())
    manager = entry_module.WorkerOnlyEntryRuntime(
        hass,
        entry,
        clock=lambda: NOW,
    )

    assert await manager.async_setup()
    assert len(scheduled) == 1
    handle = scheduled[0]
    assert await entry_module.async_unload_worker_only_entry(hass, entry)
    assert handle.cancel_calls == 1
    assert manager._freshness_handle is None


@pytest.mark.asyncio
async def test_oversized_target_config_is_rejected_before_state_or_runtime_work(entry_module):
    hass = FakeHass(_states(with_person=False))
    persons = tuple(
        f"device_tracker.person_{index:03d}" for index in range(60)
    )
    entry = FakeEntry(_entry_data(persons=persons))
    with pytest.raises(
        entry_module.WorkerOnlyEntryError, match="maximaal 60 targets"
    ):
        await entry_module.async_setup_worker_only_entry(hass, entry)
    assert FakeRuntime.instances == []
    assert entry_module._test_state_registrations == []
    assert hass.data == {}


@pytest.mark.asyncio
async def test_target_bursts_are_latest_wins_and_unavailable_retains_exact_position(entry_module):
    hass = FakeHass(_states())
    entry = FakeEntry(_entry_data())
    manager = entry_module.WorkerOnlyEntryRuntime(
        hass,
        entry,
        clock=lambda: NOW,
    )
    await manager.async_setup()
    runtime = FakeRuntime.instances[-1]
    registration = entry_module._test_state_registrations[0]

    original = next(
        target for target in runtime.target_snapshot
        if target["target_id"] == "person.wim"
    )
    hass.states.values["device_tracker.wim"] = FakeState(
        "unavailable", observed_at=NOW + 1
    )
    for _ in range(20):
        registration["callback"](object())
    await _drain()
    assert len(runtime.updates) == 1
    unavailable = next(
        target for target in runtime.updates[-1][1]
        if target["target_id"] == "person.wim"
    )
    assert unavailable["available"] is False
    for key in ("latitude", "longitude", "position_observed_at"):
        assert unavailable[key] == original[key]

    hass.states.values["device_tracker.wim"] = FakeState(
        latitude=51.25,
        longitude=4.65,
        observed_at=NOW + 30,
        friendly_name="Wim moved",
    )
    for _ in range(12):
        registration["callback"](object())
    await _drain()
    assert len(runtime.updates) == 2
    moved = next(
        target for target in runtime.updates[-1][1]
        if target["target_id"] == "person.wim"
    )
    assert moved["available"] is True
    assert moved["latitude"] == 51.25
    assert moved["longitude"] == 4.65
    assert moved["position_observed_at"] == NOW + 30
    await manager.async_unload()


@pytest.mark.asyncio
async def test_life360_target_burst_waits_for_a_stable_authority_window(
    entry_module,
):
    hass = FakeHass(_states())
    entry = FakeEntry(_entry_data())
    manager = entry_module.WorkerOnlyEntryRuntime(
        hass,
        entry,
        clock=lambda: NOW,
        target_reconcile_delay_seconds=0.03,
    )
    await manager.async_setup()
    runtime = FakeRuntime.instances[-1]
    registration = entry_module._test_state_registrations[0]

    hass.states.values["device_tracker.wim"] = FakeState(
        latitude=51.30,
        longitude=4.70,
        observed_at=NOW + 40,
        friendly_name="Wim Life360",
    )
    for _ in range(30):
        registration["callback"](object())

    await _drain()
    assert runtime.updates == []
    await asyncio.sleep(0.04)
    await _drain()
    assert len(runtime.updates) == 1
    target = next(
        target for target in runtime.updates[-1][1]
        if target["target_id"] == "person.wim"
    )
    assert (target["latitude"], target["longitude"]) == (51.30, 4.70)
    await manager.async_unload()


@pytest.mark.asyncio
async def test_reachable_worker_transitions_trigger_reconnect(entry_module):
    hass = FakeHass(_states())
    entry = FakeEntry(_entry_data())
    await entry_module.async_setup_worker_only_entry(hass, entry)
    runtime = FakeRuntime.instances[-1]
    callback = hass.bus.listeners["storm_tracker_v3_worker_status_update"]

    hass.data["storm_tracker_v3"]["worker_status"] = {
        "state": "busy",
        "job_admission_open": True,
    }
    callback(object())
    callback(object())
    await _drain()
    assert runtime.reconnect_calls == 1

    hass.data["storm_tracker_v3"]["worker_status"] = {"state": "offline"}
    callback(object())
    hass.data["storm_tracker_v3"]["worker_status"] = {
        "state": "degraded",
        "job_admission_open": True,
    }
    callback(object())
    await _drain()
    assert runtime.reconnect_calls == 2

    hass.data["storm_tracker_v3"]["worker_status"] = {"state": "offline"}
    callback(object())
    hass.data["storm_tracker_v3"]["worker_status"] = {
        "state": "online",
        "job_admission_open": True,
    }
    callback(object())
    await _drain()
    assert runtime.reconnect_calls == 3


@pytest.mark.asyncio
async def test_online_status_recovery_is_bounded_per_online_epoch(
    entry_module,
):
    hass = FakeHass(_states())
    entry = FakeEntry(_entry_data())
    await entry_module.async_setup_worker_only_entry(hass, entry)
    runtime = FakeRuntime.instances[-1]
    callback = hass.bus.listeners["storm_tracker_v3_worker_status_update"]
    hass.data["storm_tracker_v3"]["worker_status"] = {
        "state": "online",
        "job_admission_open": True,
    }
    callback(object())
    await _drain()
    assert runtime.reconnect_calls == 1

    runtime.diagnostics = lambda: {
        "controller": {
            "retry_blocked": True,
            "last_error_code": "worker_offline",
        }
    }
    for _ in range(entry_module.MAX_ONLINE_RECONNECT_ATTEMPTS - 1):
        callback(object())
        await _drain()
    assert runtime.reconnect_calls == entry_module.MAX_ONLINE_RECONNECT_ATTEMPTS

    for _ in range(12):
        callback(object())
        await _drain()
    assert runtime.reconnect_calls == entry_module.MAX_ONLINE_RECONNECT_ATTEMPTS

    hass.data["storm_tracker_v3"]["worker_status"] = {"state": "offline"}
    callback(object())
    hass.data["storm_tracker_v3"]["worker_status"] = {
        "state": "online",
        "job_admission_open": True,
    }
    callback(object())
    await _drain()
    assert runtime.reconnect_calls == (
        entry_module.MAX_ONLINE_RECONNECT_ATTEMPTS + 1
    )
    assert await entry_module.async_unload_worker_only_entry(hass, entry)


@pytest.mark.asyncio
async def test_repeated_online_status_does_not_retry_protocol_rejection(
    entry_module,
):
    hass = FakeHass(_states())
    entry = FakeEntry(_entry_data())
    await entry_module.async_setup_worker_only_entry(hass, entry)
    runtime = FakeRuntime.instances[-1]
    callback = hass.bus.listeners["storm_tracker_v3_worker_status_update"]
    hass.data["storm_tracker_v3"]["worker_status"] = {
        "state": "online",
        "job_admission_open": True,
    }
    callback(object())
    await _drain()
    assert runtime.reconnect_calls == 1

    runtime.diagnostics = lambda: {
        "controller": {
            "retry_blocked": True,
            "last_error_code": "worker_protocol_rejected",
        }
    }
    callback(object())
    await _drain()

    assert runtime.reconnect_calls == 1
    assert await entry_module.async_unload_worker_only_entry(hass, entry)


@pytest.mark.asyncio
async def test_repeated_online_status_retries_transient_worker_job_failure(
    entry_module,
):
    hass = FakeHass(_states())
    entry = FakeEntry(_entry_data())
    await entry_module.async_setup_worker_only_entry(hass, entry)
    runtime = FakeRuntime.instances[-1]
    callback = hass.bus.listeners["storm_tracker_v3_worker_status_update"]
    hass.data["storm_tracker_v3"]["worker_status"] = {
        "state": "online",
        "job_admission_open": True,
    }
    callback(object())
    await _drain()
    assert runtime.reconnect_calls == 1

    runtime.diagnostics = lambda: {
        "controller": {
            "retry_blocked": True,
            "last_error_code": "worker_job_failed",
        }
    }
    callback(object())
    await _drain()

    assert runtime.reconnect_calls == 2
    assert await entry_module.async_unload_worker_only_entry(hass, entry)


@pytest.mark.asyncio
async def test_worker_becoming_online_during_start_is_reconnected_once(
    entry_module,
    monkeypatch,
):
    async def start_while_worker_becomes_online(
        runtime, *, worker_admission_ready=True
    ):
        runtime.start_calls += 1
        runtime.hass.data["storm_tracker_v3"]["worker_status"] = {
            "state": "online",
            "job_admission_open": True,
        }
        return runtime.prepared

    monkeypatch.setattr(
        FakeRuntime,
        "async_start",
        start_while_worker_becomes_online,
    )
    hass = FakeHass(_states())
    entry = FakeEntry(_entry_data())

    assert await entry_module.async_setup_worker_only_entry(hass, entry)
    runtime = FakeRuntime.instances[-1]
    manager = hass.data["storm_tracker_v3"]["entry_runtimes"][
        entry.entry_id
    ]["manager"]
    assert manager._active is True
    assert manager._pending_reconnect is False
    assert runtime.reconnect_calls == 0

    await _drain()
    assert runtime.reconnect_calls == 1

    # Repeated copies of the same online status are not new transitions.
    callback = hass.bus.listeners["storm_tracker_v3_worker_status_update"]
    callback(object())
    callback(object())
    await _drain()
    assert runtime.reconnect_calls == 1


@pytest.mark.asyncio
async def test_foreign_thread_events_marshal_before_coroutine_creation(
    entry_module,
):
    """MQTT/state callbacks may never call HA task APIs from their thread."""

    hass = FakeHass(_states())
    entry = FakeEntry(_entry_data())
    await entry_module.async_setup_worker_only_entry(hass, entry)
    runtime = FakeRuntime.instances[-1]
    target_callback = entry_module._test_state_registrations[0]["callback"]
    status_callback = hass.bus.listeners[
        "storm_tracker_v3_worker_status_update"
    ]
    event_loop_thread = threading.get_ident()
    hass.data["storm_tracker_v3"]["worker_status"] = {
        "state": "online",
        "job_admission_open": True,
    }

    foreign_thread = threading.Thread(
        target=lambda: (
            target_callback(object()),
            status_callback(object()),
        ),
        name="stv3-foreign-callback-test",
    )
    foreign_thread.start()
    foreign_thread.join(timeout=2.0)
    assert not foreign_thread.is_alive()
    assert hass.task_thread_ids == []

    await _drain()
    assert len(runtime.updates) == 1
    assert runtime.reconnect_calls == 1
    assert hass.task_thread_ids
    assert set(hass.task_thread_ids) == {event_loop_thread}
    assert await entry_module.async_unload_worker_only_entry(hass, entry)


@pytest.mark.asyncio
async def test_global_runtime_slot_rejects_second_or_legacy_owner(entry_module):
    hass = FakeHass(_states())
    first_entry = FakeEntry(_entry_data(), entry_id="entry-first")
    assert await entry_module.async_setup_worker_only_entry(hass, first_entry)
    first_runtime = FakeRuntime.instances[-1]

    second_entry = FakeEntry(_entry_data(), entry_id="entry-second")
    with pytest.raises(
        entry_module.WorkerOnlyEntryError, match="globale runtime"
    ):
        await entry_module.async_setup_worker_only_entry(hass, second_entry)
    rejected_runtime = FakeRuntime.instances[-1]
    assert rejected_runtime is not first_runtime
    assert rejected_runtime.unload_calls == 1
    assert first_runtime.unload_calls == 0
    records = hass.data["storm_tracker_v3"]["entry_runtimes"]
    assert tuple(records) == ("entry-first",)
    assert records["entry-first"]["runtime_mode"] == "worker_only"
    assert hass.data["storm_tracker_v3"]["runtime_mode"] == "worker_only"
    assert hass.data["storm_tracker_v3"]["runtime_diagnostics"] == {
        "runtime_mode": "worker_only",
        "selection_source": "data",
        "legacy_compute_enabled": False,
        "provisioning": {
            "contract": "stv3-worker-provisioning-ownership-v1",
            "owner_location": "supervisor_worker_app",
            "status": "worker_app_owned_not_observed",
            "availability_claimed": False,
        },
    }

    legacy_hass = FakeHass(_states())
    legacy_marker = object()
    legacy_hass.data["storm_tracker_v3"] = {
        "async_shutdown_runtime": legacy_marker,
    }
    legacy_entry = FakeEntry(_entry_data(), entry_id="entry-after-legacy")
    with pytest.raises(
        entry_module.WorkerOnlyEntryError, match="globale runtime"
    ):
        await entry_module.async_setup_worker_only_entry(
            legacy_hass, legacy_entry
        )
    assert legacy_hass.data["storm_tracker_v3"] == {
        "async_shutdown_runtime": legacy_marker,
    }


@pytest.mark.asyncio
async def test_start_and_forward_failures_fully_rollback(entry_module):
    prepare_hass = FakeHass(_states())
    prepare_entry = FakeEntry(_entry_data(), entry_id="entry-prepare")
    FakeRuntime.fail_prepare = True
    with pytest.raises(RuntimeError, match="prepare failed"):
        await entry_module.async_setup_worker_only_entry(
            prepare_hass, prepare_entry
        )
    failed_prepare = FakeRuntime.instances[-1]
    assert failed_prepare.unload_calls == 1
    assert prepare_hass.data == {}
    assert entry_module._test_state_registrations == []

    FakeRuntime.fail_prepare = False
    hass = FakeHass(_states())
    entry = FakeEntry(_entry_data())
    FakeRuntime.fail_start = True
    with pytest.raises(RuntimeError, match="start failed"):
        await entry_module.async_setup_worker_only_entry(hass, entry)
    failed = FakeRuntime.instances[-1]
    assert failed.unload_calls == 1
    assert hass.config_entries.forward_calls == []
    assert hass.config_entries.unload_calls == []
    assert entry_module._test_state_registrations[-1]["unsubscribed"] == 1
    assert hass.bus.unsubscribed == 1
    assert hass.data == {}

    FakeRuntime.fail_start = False
    hass2 = FakeHass(_states())
    hass2.config_entries.fail_forward = True
    entry2 = FakeEntry(_entry_data(), entry_id="entry-b")
    with pytest.raises(RuntimeError, match="forward failed"):
        await entry_module.async_setup_worker_only_entry(hass2, entry2)
    failed_forward = FakeRuntime.instances[-1]
    assert failed_forward.unload_calls == 1
    assert hass2.config_entries.forward_calls == [("entry-b", ("sensor",))]
    assert hass2.config_entries.unload_calls == [("entry-b", ("sensor",))]
    assert hass2.data == {}

    hass3 = FakeHass(_states())
    entry3 = FakeEntry(_entry_data(), entry_id="entry-listener")
    entry3.add_update_listener = lambda _callback: None
    with pytest.raises(
        entry_module.WorkerOnlyEntryError,
        match="optionslistener",
    ):
        await entry_module.async_setup_worker_only_entry(hass3, entry3)
    failed_listener = FakeRuntime.instances[-1]
    assert failed_listener.unload_calls == 1
    # No platform was forwarded, so there can be no stranded entity platform.
    assert hass3.config_entries.forward_calls == []
    assert hass3.config_entries.unload_calls == []
    assert hass3.data == {}


@pytest.mark.asyncio
async def test_options_reload_and_unload_are_owned_once_by_recorded_mode(entry_module):
    hass = FakeHass(_states())
    entry = FakeEntry(_entry_data())
    await entry_module.async_setup_worker_only_entry(hass, entry)
    runtime = FakeRuntime.instances[-1]
    assert len(entry.update_listeners) == 1

    await entry.update_listeners[0](hass, entry)
    assert hass.config_entries.reload_calls == ["entry-a"]
    # Changing current options must not change which already-running runtime
    # owns unload.
    entry.options["runtime_mode"] = "legacy"
    assert await entry_module.async_unload_worker_only_entry(hass, entry)
    assert runtime.unload_calls == 1
    assert entry.option_unsubscribed == 1
    assert entry_module._test_state_registrations[0]["unsubscribed"] == 1
    assert hass.bus.unsubscribed == 1
    assert hass.config_entries.unload_calls == [("entry-a", ("sensor",))]
    assert not await entry_module.async_unload_worker_only_entry(hass, entry)
    assert runtime.unload_calls == 1
    assert hass.data == {}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mqtt_available",
    (False, True),
    ids=("mqtt-unavailable", "mqtt-available"),
)
async def test_real_stack_setup_unload_reload_reclaims_exact_request_lock(
    real_worker_only_entry_modules,
    mqtt_available,
):
    """Exercise entry -> real runtime -> real bridge/controller lifecycle."""

    modules = real_worker_only_entry_modules
    entry_module = modules.worker_only_entry
    modules.mqtt_control.available = mqtt_available
    hass = RealLifecycleHass(_states(with_person=False))
    entry = FakeEntry(_entry_data(persons=()), entry_id="entry-real")

    assert await entry_module.async_setup_worker_only_entry(hass, entry)
    first_domain_data = hass.data[entry_module.DOMAIN]
    first_lock = first_domain_data["worker_request_lock"]
    assert isinstance(first_lock, asyncio.Lock)
    assert not first_lock.locked()
    prepared = entry_module.get_prepared_worker_only_runtime(
        hass, entry.entry_id
    )
    assert prepared.controller.diagnostics()["retry_blocked"] is False
    assert prepared.controller.diagnostics()["state"] == "waiting_for_worker"

    second = FakeEntry(_entry_data(persons=()), entry_id="entry-second")
    with pytest.raises(
        entry_module.WorkerOnlyEntryError,
        match="globale runtime",
    ):
        await entry_module.async_setup_worker_only_entry(hass, second)
    assert hass.data[entry_module.DOMAIN]["worker_request_lock"] is first_lock
    assert tuple(
        hass.data[entry_module.DOMAIN][entry_module.DATA_ENTRY_RUNTIMES]
    ) == (entry.entry_id,)

    assert len(entry.update_listeners) == 1
    await entry.update_listeners[0](hass, entry)
    assert hass.config_entries.reload_calls == [entry.entry_id]

    # Top-level setup records that the process-wide HTTP view already exists.
    # This marker may not keep the entry-owned runtime slot occupied after
    # unload, otherwise a normal config-entry reload fails as globally busy.
    hass.data[entry_module.DOMAIN].update({
        "geojson_http_registered": True,
        "geojson_worker_collection": {"type": "FeatureCollection"},
        "geojson_worker_status": {"state": "operationeel"},
        "_geojson_mqtt_receipt_logged": True,
    })
    assert await entry_module.async_unload_worker_only_entry(hass, entry)
    assert entry_module.DOMAIN not in hass.data
    assert hass.bus.listeners == {}
    assert await entry_module.async_setup_worker_only_entry(hass, entry)
    second_lock = hass.data[entry_module.DOMAIN]["worker_request_lock"]
    assert isinstance(second_lock, asyncio.Lock)
    assert second_lock is not first_lock
    assert await entry_module.async_unload_worker_only_entry(hass, entry)
    assert hass.data == {}
    assert hass.bus.listeners == {}

    expected_mqtt_registrations = 12 if mqtt_available else 0
    expected_intervals = 2 if mqtt_available else 0
    assert len(modules.mqtt_control.subscriptions) == (
        expected_mqtt_registrations
    )
    assert all(
        not registration["active"]
        and registration["unsubscribed"] == 1
        for registration in modules.mqtt_control.subscriptions
    )
    assert len(modules.interval_registrations) == expected_intervals
    assert all(
        not registration["active"]
        and registration["unsubscribed"] == 1
        for registration in modules.interval_registrations
    )
    assert modules.mqtt_control.publications == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure_point",
    ("bridge_setup", "platform_forward"),
)
async def test_real_stack_setup_fault_releases_exact_request_lock(
    real_worker_only_entry_modules,
    monkeypatch,
    failure_point,
):
    """A fallible bridge or platform setup cannot strand global authority."""

    modules = real_worker_only_entry_modules
    entry_module = modules.worker_only_entry
    hass = RealLifecycleHass(_states(with_person=False))
    entry = FakeEntry(_entry_data(persons=()), entry_id="entry-fault")

    if failure_point == "bridge_setup":
        async def fail_bridge_setup(inner_hass):
            request_lock = inner_hass.data[entry_module.DOMAIN][
                "worker_request_lock"
            ]
            assert isinstance(request_lock, asyncio.Lock)
            raise RuntimeError("real bridge setup failed")

        monkeypatch.setattr(
            modules.worker_only_runtime,
            "async_setup_worker_bridge",
            fail_bridge_setup,
        )
        expected_error = "real bridge setup failed"
    else:
        modules.mqtt_control.available = True
        hass.config_entries.fail_forward = True
        expected_error = "forward failed"

    with pytest.raises(RuntimeError, match=expected_error):
        await entry_module.async_setup_worker_only_entry(hass, entry)

    assert hass.data == {}
    assert hass.bus.listeners == {}
    assert all(
        not registration["active"]
        and registration["unsubscribed"] == 1
        for registration in modules.state_registrations
    )
    assert all(
        not registration["active"]
        and registration["unsubscribed"] == 1
        for registration in modules.mqtt_control.subscriptions
    )
    assert all(
        not registration["active"]
        and registration["unsubscribed"] == 1
        for registration in modules.interval_registrations
    )
    if failure_point == "bridge_setup":
        assert hass.config_entries.forward_calls == []
        assert hass.config_entries.unload_calls == []
        assert entry.option_unsubscribed == 0
    else:
        assert hass.config_entries.forward_calls == [
            (entry.entry_id, ("sensor",))
        ]
        assert hass.config_entries.unload_calls == [
            (entry.entry_id, ("sensor",))
        ]
        assert entry.option_unsubscribed == 1


@pytest.mark.asyncio
async def test_platform_unload_refusal_leaves_runtime_wholly_owned_and_retryable(
    entry_module,
):
    hass = FakeHass(_states())
    entry = FakeEntry(_entry_data())
    await entry_module.async_setup_worker_only_entry(hass, entry)
    runtime = FakeRuntime.instances[-1]
    source = object()
    diagnostics = object()
    domain_data = hass.data["storm_tracker_v3"]
    domain_data[entry_module.DATA_WORKER_TECHNICAL_SOURCE] = source
    domain_data[entry_module.DATA_WORKER_TECHNICAL_DIAGNOSTICS] = diagnostics
    hass.config_entries.unload_result = False

    assert not await entry_module.async_unload_worker_only_entry(hass, entry)
    assert runtime.unload_calls == 0
    assert entry.option_unsubscribed == 0
    assert "storm_tracker_v3" in hass.data
    assert domain_data[entry_module.DATA_WORKER_TECHNICAL_SOURCE] is source
    assert (
        domain_data[entry_module.DATA_WORKER_TECHNICAL_DIAGNOSTICS]
        is diagnostics
    )

    hass.config_entries.unload_result = True
    assert await entry_module.async_unload_worker_only_entry(hass, entry)
    assert runtime.unload_calls == 1
    assert entry.option_unsubscribed == 1
    assert hass.data == {}


@pytest.mark.asyncio
async def test_unknown_or_missing_runtime_mode_starts_neither_runtime():
    tree = ast.parse((COMPONENT / "__init__.py").read_text(encoding="utf-8"))
    function = next(
        node for node in tree.body
        if isinstance(node, ast.AsyncFunctionDef)
        and node.name == "async_setup_entry"
    )
    isolated = ast.Module(body=[function], type_ignores=[])
    ast.fix_missing_locations(isolated)

    class Logger:
        def error(self, *_args):
            return None

    namespace = {
        "HomeAssistant": object,
        "DOMAIN": "storm_tracker_v3",
        "CONF_RUNTIME_MODE": "runtime_mode",
        "RUNTIME_MODE_LEGACY": "legacy",
        "RUNTIME_MODE_WORKER_ONLY": "worker_only",
        "CONFIG_ENTRY_VERSION": 3,
        "_explicit_entry_mode": lambda entry: (
            entry.data.get("runtime_mode")
            if entry.data.get("runtime_mode")
            in {"legacy", "worker_only"}
            and entry.options.get("runtime_mode")
            in {None, entry.data.get("runtime_mode")}
            else None
        ),
        "_LOGGER": Logger(),
    }
    exec(compile(isolated, "isolated_async_setup_entry", "exec"), namespace)
    hass = types.SimpleNamespace(data={})
    for data in ({}, {"runtime_mode": "future_mode"}):
        entry = types.SimpleNamespace(
            data=data,
            options={},
            entry_id="bad-entry",
        )
        assert await namespace["async_setup_entry"](hass, entry) is False

    occupied_hass = types.SimpleNamespace(
        data={"storm_tracker_v3": {"entry_runtimes": {"active": {}}}}
    )
    legacy = types.SimpleNamespace(
        data={"runtime_mode": "legacy"},
        options={},
        entry_id="legacy-second",
    )
    assert await namespace["async_setup_entry"](occupied_hass, legacy) is False


@pytest.mark.asyncio
async def test_migration_persists_exact_mode_and_is_transactional():
    tree = ast.parse((COMPONENT / "__init__.py").read_text(encoding="utf-8"))
    function = next(
        node for node in tree.body
        if isinstance(node, ast.AsyncFunctionDef)
        and node.name == "async_migrate_entry"
    )
    isolated = ast.Module(body=[function], type_ignores=[])
    ast.fix_missing_locations(isolated)
    namespace = {
        "HomeAssistant": object,
        "CONF_RUNTIME_MODE": "runtime_mode",
        "RUNTIME_MODE_LEGACY": "legacy",
        "RUNTIME_MODE_WORKER_ONLY": "worker_only",
        "CONFIG_ENTRY_VERSION": 3,
        "_explicit_entry_mode": lambda entry: (
            entry.data.get("runtime_mode")
            if entry.data.get("runtime_mode")
            in {"legacy", "worker_only"}
            and entry.options.get("runtime_mode")
            in {None, entry.data.get("runtime_mode")}
            else None
        ),
        "_LOGGER": types.SimpleNamespace(warning=lambda *_args: None),
    }
    exec(compile(isolated, "isolated_async_migrate_entry", "exec"), namespace)
    updates = []
    hass = types.SimpleNamespace(config_entries=types.SimpleNamespace(
        async_update_entry=lambda entry, **kwargs: updates.append((entry, kwargs))
    ))
    old = FakeEntry({"persons": ["device_tracker.wim"]}, version=1)
    assert await namespace["async_migrate_entry"](hass, old)
    assert updates[0][1] == {
        "data": {
            "persons": ["device_tracker.wim"],
            "runtime_mode": "worker_only",
        },
        "options": {},
        "version": 3,
    }

    explicit_worker = FakeEntry(
        {"persons": ["device_tracker.wim"]},
        options={"runtime_mode": "worker_only"},
        version=1,
    )
    assert await namespace["async_migrate_entry"](hass, explicit_worker)
    assert updates[1][1] == {
        "data": {
            "persons": ["device_tracker.wim"],
            "runtime_mode": "worker_only",
        },
        "options": {},
        "version": 3,
    }
    explicit_legacy = FakeEntry(
        {"runtime_mode": "legacy", "knmi_api_key": "preserved"},
        options={"runtime_mode": "legacy", "radar_radius_km": 250},
        version=2,
    )
    assert await namespace["async_migrate_entry"](hass, explicit_legacy)
    assert updates[2][1] == {
        "data": {
            "runtime_mode": "legacy",
            "knmi_api_key": "preserved",
        },
        "options": {"radar_radius_km": 250},
        "version": 3,
    }

    future = FakeEntry({}, version=4)
    assert not await namespace["async_migrate_entry"](hass, future)

    explicit_cutover = FakeEntry({"runtime_mode": "worker_only"}, version=3)
    assert await namespace["async_migrate_entry"](hass, explicit_cutover)
    assert len(updates) == 3

    overridden = FakeEntry(
        {"runtime_mode": "worker_only"},
        options={"runtime_mode": "legacy"},
        version=3,
    )
    assert not await namespace["async_migrate_entry"](hass, overridden)


def test_worker_only_setup_branch_cannot_reach_legacy_runtime_call():
    tree = ast.parse((COMPONENT / "__init__.py").read_text(encoding="utf-8"))
    setup = next(
        node for node in tree.body
        if isinstance(node, ast.AsyncFunctionDef)
        and node.name == "async_setup_entry"
    )
    rendered = ast.unparse(setup)
    assert "async_setup_worker_only_entry" in rendered
    for forbidden in (
        "_async_setup_runtime",
        "providers",
        "helpers.storage",
        "calibration",
        "pysteps",
        "radar_frame_worker",
    ):
        assert forbidden not in rendered


@pytest.mark.asyncio
async def test_yaml_never_silently_falls_back_to_legacy_compute():
    tree = ast.parse((COMPONENT / "__init__.py").read_text(encoding="utf-8"))
    function = next(
        node for node in tree.body
        if isinstance(node, ast.AsyncFunctionDef)
        and node.name == "async_setup"
    )
    isolated = ast.Module(body=[function], type_ignores=[])
    ast.fix_missing_locations(isolated)
    calls = []

    async def setup_runtime(hass, conf, config):
        calls.append((copy.deepcopy(conf), copy.deepcopy(config)))
        hass.data["storm_tracker_v3"] = {}
        return True

    namespace = {
        "HomeAssistant": object,
        "ConfigType": dict,
        "DOMAIN": "storm_tracker_v3",
        "CONF_RUNTIME_MODE": "runtime_mode",
        "RUNTIME_MODE_LEGACY": "legacy",
        "_LOGGER": types.SimpleNamespace(error=lambda *_args: None),
        "_async_setup_runtime": setup_runtime,
    }
    exec(compile(isolated, "isolated_async_setup", "exec"), namespace)

    for configuration in (
        {"storm_tracker_v3": {"home_lat": 51.04}},
        {"storm_tracker_v3": {"runtime_mode": "worker_only"}},
    ):
        hass = types.SimpleNamespace(data={})
        assert not await namespace["async_setup"](hass, configuration)
    assert calls == []

    hass = types.SimpleNamespace(data={})
    explicit = {
        "storm_tracker_v3": {
            "runtime_mode": "legacy",
            "home_lat": 51.04,
        }
    }
    assert not await namespace["async_setup"](hass, explicit)
    assert calls == []


def test_sensor_config_entry_registration_is_snapshot_only_and_non_polling():
    tree = ast.parse((COMPONENT / "sensor.py").read_text(encoding="utf-8"))
    setup = next(
        node for node in tree.body
        if isinstance(node, ast.AsyncFunctionDef)
        and node.name == "async_setup_entry"
    )
    rendered = ast.unparse(setup)
    assert "get_prepared_worker_only_runtime" in rendered
    assert "prepared.entities" in rendered
    assert "update_before_add=False" in rendered
    for forbidden in (
        "async_setup_platform",
        "_load_legacy_sensor_dependencies",
        "provider",
        "executor",
        "Store",
    ):
        assert forbidden not in rendered
