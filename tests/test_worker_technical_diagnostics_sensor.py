from __future__ import annotations

import ast
import asyncio
import importlib.util
import json
from pathlib import Path
import sys
import types


ROOT = Path(__file__).resolve().parents[1]
COMPONENT = ROOT / "custom_components" / "storm_tracker_v3"
SENSOR_PATH = COMPONENT / "worker_technical_diagnostics_sensor.py"
EVENT = "storm_tracker_v3_worker_technical_diagnostics_update"


class FakeSensorEntity:
    hass = None

    async def async_added_to_hass(self):
        return None

    def async_on_remove(self, cleanup):
        self._test_cleanups = getattr(self, "_test_cleanups", [])
        self._test_cleanups.append(cleanup)

    def async_write_ha_state(self):
        self._test_write_count = getattr(self, "_test_write_count", 0) + 1


class FakeBus:
    def __init__(self):
        self.listeners = {}
        self.unsubscribe_count = 0

    def async_listen(self, event, callback):
        self.listeners.setdefault(event, []).append(callback)
        active = True

        def unsubscribe():
            nonlocal active
            if active:
                active = False
                self.listeners[event].remove(callback)
                self.unsubscribe_count += 1

        return unsubscribe

    def fire(self, event, data=None):
        message = types.SimpleNamespace(event_type=event, data=data or {})
        for callback in tuple(self.listeners.get(event, ())):
            callback(message)


class FakeHass:
    def __init__(self):
        self.data = {"storm_tracker_v3": {}}
        self.bus = FakeBus()


def _load_modules():
    package_name = "stv3_worker_technical_sensor_test_package"
    package = types.ModuleType(package_name)
    package.__path__ = [str(COMPONENT)]
    sys.modules[package_name] = package

    homeassistant = types.ModuleType("homeassistant")
    components = types.ModuleType("homeassistant.components")
    sensor_component = types.ModuleType("homeassistant.components.sensor")
    sensor_component.SensorEntity = FakeSensorEntity
    core = types.ModuleType("homeassistant.core")
    core.callback = lambda function: function
    homeassistant.components = components
    components.sensor = sensor_component
    for name, module in {
        "homeassistant": homeassistant,
        "homeassistant.components": components,
        "homeassistant.components.sensor": sensor_component,
        "homeassistant.core": core,
    }.items():
        sys.modules[name] = module

    const = types.ModuleType(f"{package_name}.const")
    const.DATA_ENTRY_RUNTIMES = "entry_runtimes"
    const.DATA_WORKER_TECHNICAL_DIAGNOSTICS = (
        "worker_technical_diagnostics"
    )
    const.DATA_WORKER_TECHNICAL_SOURCE = "worker_technical_source"
    const.DOMAIN = "storm_tracker_v3"
    const.INTEGRATION_VERSION = "1.14.22"
    const.WORKER_TECHNICAL_DIAGNOSTICS_UPDATE_EVENT = EVENT
    sys.modules[f"{package_name}.const"] = const

    loaded = []
    for short_name in (
        "worker_technical_diagnostics",
        "worker_technical_diagnostics_sensor",
    ):
        name = f"{package_name}.{short_name}"
        spec = importlib.util.spec_from_file_location(
            name,
            COMPONENT / f"{short_name}.py",
        )
        assert spec is not None
        assert spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
        loaded.append(module)
    return tuple(loaded)


projector, sensor_module = _load_modules()


def _source(**overrides):
    source = {
        "contract": projector.SOURCE_CONTRACT,
        "worker_state": "online",
        "worker_observed_at": 1_000.0,
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


def _install(hass, source, *, now):
    document = projector.build_worker_technical_diagnostics(source, now=now)
    data = hass.data["storm_tracker_v3"]
    data["worker_technical_source"] = source
    data["worker_technical_diagnostics"] = document
    return document


def test_sensor_is_event_driven_and_exposes_exact_reproved_document():
    async def run():
        hass = FakeHass()
        source = _source()
        document = _install(hass, source, now=1_005.0)
        entity = sensor_module.WorkerTechnicalDiagnosticsSensor("entry-a")
        entity.hass = hass

        await entity.async_added_to_hass()

        assert entity._attr_unique_id == "entry-a_worker_technical_diagnostics"
        assert entity._attr_should_poll is False
        assert entity.available is True
        assert entity.native_value == "current"
        assert entity.extra_state_attributes == {
            **document,
            "integration_version": "1.14.22",
            "desired_state_control": sensor_module._unknown_control_diagnostics(),
        }
        assert entity._test_write_count == 1
        assert len(hass.bus.listeners[EVENT]) == 1

        stale_source = dict(source, worker_state="offline")
        stale = _install(hass, stale_source, now=1_006.0)
        hass.bus.fire(EVENT, {"untrusted": "ignored"})
        assert entity.native_value == "stale"
        assert entity.extra_state_attributes == {
            **stale,
            "integration_version": "1.14.22",
            "desired_state_control": sensor_module._unknown_control_diagnostics(),
        }
        assert entity._test_write_count == 2

        entity._test_cleanups[0]()
        assert hass.bus.unsubscribe_count == 1
        assert hass.bus.listeners[EVENT] == []

    asyncio.run(run())


def test_sensor_rejects_tampered_cache_and_never_copies_secret_data():
    async def run():
        hass = FakeHass()
        sentinel = "SUPERS3CRETTOKEN-DO-NOT-LEAK"
        source = _source()
        document = projector.build_worker_technical_diagnostics(
            source,
            now=1_005.0,
        )
        document["credential"] = sentinel
        document["worker"]["version"] = sentinel
        data = hass.data["storm_tracker_v3"]
        data["worker_technical_source"] = source
        data["worker_technical_diagnostics"] = document
        entity = sensor_module.WorkerTechnicalDiagnosticsSensor("entry-a")
        entity.hass = hass

        await entity.async_added_to_hass()

        assert entity.native_value == "unknown"
        attributes = entity.extra_state_attributes
        wire = json.dumps(attributes, sort_keys=True)
        assert sentinel not in wire
        assert "credential" not in wire.lower()
        assert attributes["projection_reason_codes"] == [
            "diagnostic_source_missing"
        ]

    asyncio.run(run())


def test_sensor_projects_only_bounded_controller_metadata():
    class Runtime:
        def diagnostics(self):
            return {
                "controller": {
                    "state": "error",
                    "started": True,
                    "dirty": True,
                    "in_flight": False,
                    "retry_blocked": True,
                    "target_count": 2,
                    "authority_claimed": False,
                    "head_generation": None,
                    "last_error_code": "worker_job_failed",
                    "last_error_type": "RuntimeError",
                    "secret": "SUPERS3CRETTOKEN-DO-NOT-LEAK",
                    "counters": {
                        "reconnects": 3,
                        "worker_requests": 4,
                        "failures": 4,
                        "private_counter": 999,
                    },
                }
            }

    hass = FakeHass()
    hass.data["storm_tracker_v3"]["entry_runtimes"] = {
        "entry-a": {"runtime": Runtime()}
    }

    projected = sensor_module._read_control_diagnostics(hass)

    assert projected == {
        "state": "error",
        "started": True,
        "dirty": True,
        "in_flight": False,
        "retry_blocked": True,
        "target_count": 2,
        "authority_claimed": False,
        "head_generation": None,
        "last_error_code": "worker_job_failed",
        "last_error_type": "RuntimeError",
        "reconnects": 3,
        "worker_requests": 4,
        "failures": 4,
    }
    assert "SUPERS3CRETTOKEN" not in json.dumps(projected, sort_keys=True)


def test_sensor_source_has_no_transport_or_meteorological_capability():
    source = SENSOR_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported_roots = {
        node.module.split(".", 1)[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module
    }
    imported_roots.update(
        alias.name.split(".", 1)[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    )
    assert imported_roots.isdisjoint(
        {"mqtt", "paho", "requests", "socket", "storm_tracker_worker", "urllib"}
    )
    lowered = source.lower()
    for forbidden in (
        "fetch(",
        "call_api",
        "call_service",
        "publish(",
        "subscribe(",
        "credential",
        "password",
        "token",
        "payload",
        "topic",
        "rain_rate",
        "precipitation",
        "next_threat",
        "storm_id",
    ):
        assert forbidden not in lowered
