"""Isolation and lifecycle proof for the thin HA worker-only runtime."""
from __future__ import annotations

import ast
import asyncio
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import types

import pytest


ROOT = Path(__file__).resolve().parents[1]
COMPONENT = ROOT / "custom_components/storm_tracker_v3"
MODULE_PATH = COMPONENT / "worker_only_runtime.py"
NOW = 1_800_000_000.0
EVENT_NAME = "storm_tracker_v3_operational_snapshot_update"
TECHNICAL_EVENT_NAME = "storm_tracker_v3_worker_technical_diagnostics_update"


class FakeSensorEntity:
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


class FakeHass:
    def __init__(self):
        self.data = {}
        self.bus = types.SimpleNamespace(async_listen=lambda *_args: lambda: None)


def _load_file(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def runtime_modules(monkeypatch):
    package_name = "stv3_worker_only_runtime_test_package"
    package = types.ModuleType(package_name)
    package.__path__ = [str(COMPONENT)]
    monkeypatch.setitem(sys.modules, package_name, package)

    homeassistant = types.ModuleType("homeassistant")
    components = types.ModuleType("homeassistant.components")
    sensor_module = types.ModuleType("homeassistant.components.sensor")
    sensor_module.SensorEntity = FakeSensorEntity
    core = types.ModuleType("homeassistant.core")
    core.HomeAssistant = object
    core.callback = lambda function: function
    homeassistant.components = components
    components.sensor = sensor_module
    for name, module in {
        "homeassistant": homeassistant,
        "homeassistant.components": components,
        "homeassistant.components.sensor": sensor_module,
        "homeassistant.core": core,
    }.items():
        monkeypatch.setitem(sys.modules, name, module)

        const = types.ModuleType(f"{package_name}.const")
        const.DATA_ENTRY_RUNTIMES = "entry_runtimes"
        const.DATA_WORKER_TECHNICAL_DIAGNOSTICS = (
            "worker_technical_diagnostics"
        )
    const.DATA_WORKER_TECHNICAL_SOURCE = "worker_technical_source"
    const.DOMAIN = "storm_tracker_v3"
    const.INTEGRATION_VERSION = "1.14.24"
    const.OPERATIONAL_SNAPSHOT_UPDATE_EVENT = EVENT_NAME
    const.RUNTIME_MODE_WORKER_ONLY = "worker_only"
    const.WORKER_TECHNICAL_DIAGNOSTICS_UPDATE_EVENT = TECHNICAL_EVENT_NAME
    monkeypatch.setitem(sys.modules, f"{package_name}.const", const)

    loaded = {}
    for short_name in (
        "operational_snapshot",
        "operational_snapshot_views",
        "operational_snapshot_sensors",
        "worker_technical_diagnostics",
        "worker_technical_diagnostics_sensor",
        "desired_state_builder",
    ):
        loaded[short_name] = _load_file(
            f"{package_name}.{short_name}", COMPONENT / f"{short_name}.py"
        )

    bridge = types.ModuleType(f"{package_name}.worker_bridge")
    bridge.DOMAIN = "storm_tracker_v3"

    async def unused_setup(_hass):
        raise AssertionError("test moet bridge_setup injecteren")

    async def unused_request(*_args, **_kwargs):
        raise AssertionError("test moet requester injecteren")

    bridge.async_setup_worker_bridge = unused_setup
    bridge.async_request_worker_job = unused_request

    def mark_authority(
        hass,
        *,
        expected_desired_state_sha256,
        preserve_current_snapshot=False,
    ):
        hass.data.setdefault("storm_tracker_v3", {})[
            "operational_snapshot_expected_desired_state_sha256"
        ] = expected_desired_state_sha256
        return False

    bridge.mark_operational_snapshot_authority_changed = mark_authority
    monkeypatch.setitem(sys.modules, f"{package_name}.worker_bridge", bridge)
    package.worker_bridge = bridge

    loaded["desired_state_controller"] = _load_file(
        f"{package_name}.desired_state_controller",
        COMPONENT / "desired_state_controller.py",
    )
    loaded["worker_only_runtime"] = _load_file(
        f"{package_name}.worker_only_runtime", MODULE_PATH
    )
    return types.SimpleNamespace(**loaded)


def _target(
    target_id: str = "home",
    *,
    observed_at: float = NOW,
    latitude: float = 51.04,
) -> dict:
    return {
        "target_id": target_id,
        "display_name": "Thuis" if target_id == "home" else target_id.upper(),
        "kind": "home" if target_id == "home" else "device_tracker",
        "available": True,
        "latitude": latitude,
        "longitude": 4.48,
        "position_observed_at": observed_at,
        "location_accuracy_m": None,
        "observation_radius_km": 250.0,
        "country_code": "BE",
    }


def _settings(*, ground_radius_km: float = 20.0) -> dict:
    return {
        "ground_radius_km": ground_radius_km,
        "lightning_source_mode": "auto",
        "model_guidance_enabled": False,
        "region_sharing_distance_km": 150.0,
    }


def _config(*, sensor_target_ids=(), settings=None) -> dict:
    return {
        "producer_id": "homeassistant.storm-tracker",
        "unique_id_prefix": "entry-a",
        "home_target_id": "home",
        "sensor_target_ids": list(sensor_target_ids),
        "settings": _settings() if settings is None else settings,
    }


def _worker_snapshot(revision: int = 1) -> dict:
    empty = {"complete": True, "overflow_count": 0, "items": []}
    return {
        "contract": "operational_snapshot_v1",
        "revision": revision,
        "desired_state_sha256": "d" * 64,
        "product_valid_at": NOW - 20,
        "received_at": NOW - 10,
        "evaluated_at": NOW - 1,
        "valid_until": NOW + 299,
        "provenance_complete": False,
        "provenance_source_ids": [],
        "provenance_reason_codes": ["initial_worker_snapshot"],
        "worker": {
            "state": "online",
            "queue_depth": 0,
            "failed_jobs": 0,
            "status_text": "operationeel",
        },
        "regions": copy.deepcopy(empty),
        "targets": copy.deepcopy(empty),
        "providers": copy.deepcopy(empty),
        "ui_storms": copy.deepcopy(empty),
        "map_artifact": {"artifact_handle": None, "etag": None},
    }


class FakeRequester:
    def __init__(self) -> None:
        self.calls = []
        self.epoch = 1
        self.claim_id = None
        self.head = {
            "method": "worker_desired_state_store_v1",
            "status": "unconfigured",
        }
        self.offline = False
        self.block = False
        self.block_started = asyncio.Event()
        self.release = asyncio.Event()

    async def __call__(
        self, _hass, job_type, payload, *, timeout, job_id
    ):
        self.calls.append((job_type, copy.deepcopy(payload), timeout, job_id))
        if self.offline:
            raise RuntimeError("Storm Tracker-worker is niet beschikbaar")
        if self.block:
            self.block_started.set()
            await self.release.wait()
            self.block = False
        if job_type == "desired_state_claim":
            status = "claimed" if self.claim_id is None else "claim_idempotent"
            self.claim_id = payload["claim_id"]
            if self.head["status"] == "unconfigured":
                self.head.update({
                    "producer_id": payload["producer_id"],
                    "authority_epoch": self.epoch,
                })
            return {
                "method": "worker_desired_state_store_v1",
                "status": status,
                "producer_id": payload["producer_id"],
                "authority_epoch": self.epoch,
                "claim_id": payload["claim_id"],
                "claimed_at": NOW - 1,
            }
        if job_type == "desired_state_snapshot":
            return copy.deepcopy(self.head)
        assert job_type == "desired_state_apply"
        document = payload["desired_state"]
        state_sha = hashlib.sha256(
            json.dumps(
                document,
                allow_nan=False,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
        ).hexdigest()
        self.head = {
            "method": "worker_desired_state_store_v1",
            "status": "configured",
            "state_sha256": state_sha,
            "producer_id": document["producer_id"],
            "authority_epoch": document["authority_epoch"],
            "generation": document["generation"],
            "issued_at": document["issued_at"],
            "first_received_at": NOW,
            "last_received_at": NOW,
            "receipt_count": 1,
            "activated_at": NOW,
            "target_count": len(document["targets"]),
        }
        return {
            key: value
            for key, value in self.head.items()
            if key != "activated_at"
        } | {"status": "activated"}


class FakeHooks:
    def __init__(self) -> None:
        self.bridge_setup_count = 0
        self.bridge_cleanup_count = 0
        self.event_subscribe_count = 0
        self.event_cleanup_count = 0
        self.event_callback = None
        self.cache = None

    async def bridge_setup(self, _hass, cache):
        self.bridge_setup_count += 1
        self.cache = cache
        cleaned = False

        async def cleanup():
            nonlocal cleaned
            if not cleaned:
                cleaned = True
                self.bridge_cleanup_count += 1

        return cleanup

    def event_subscribe(self, _hass, callback):
        self.event_subscribe_count += 1
        self.event_callback = callback
        cleaned = False

        def cleanup():
            nonlocal cleaned
            if not cleaned:
                cleaned = True
                self.event_cleanup_count += 1
                self.event_callback = None

        return cleanup


def _runtime(module, requester, hooks, *, config=None, targets=None, hass=None):
    return module.WorkerOnlyRuntimeCandidate(
        FakeHass() if hass is None else hass,
        config_snapshot=(
            _config(sensor_target_ids=("person.wim",))
            if config is None
            else config
        ),
        target_snapshot=(
            [_target(), _target("person.wim")] if targets is None else targets
        ),
        boot_id="boot-a",
        bridge_setup=hooks.bridge_setup,
        requester=requester,
        event_subscribe=hooks.event_subscribe,
        clock=lambda: NOW,
        debounce_seconds=0,
        request_timeout=5,
    )


def test_prepare_is_pure_idempotent_and_builds_one_bounded_composition(
    runtime_modules,
):
    module = runtime_modules.worker_only_runtime
    requester = FakeRequester()
    hooks = FakeHooks()
    runtime = _runtime(module, requester, hooks)

    first = runtime.prepare()
    second = runtime.prepare()
    assert first is second
    assert first.cache is second.cache
    assert first.controller is second.controller
    assert first.event_hub is second.event_hub
    assert len(first.entities) == first.entity_count == module.BASE_ENTITY_COUNT + 1
    technical_type = (
        runtime_modules.worker_technical_diagnostics_sensor
        .WorkerTechnicalDiagnosticsSensor
    )
    technical_entities = [
        entity
        for entity in first.entities
        if isinstance(entity, technical_type)
    ]
    assert len(technical_entities) == 1
    assert technical_entities[0]._attr_unique_id == (
        "entry-a_worker_technical_diagnostics"
    )
    snapshot_entities = [
        entity for entity in first.entities if hasattr(entity, "_hub")
    ]
    assert len(snapshot_entities) == module.BASE_ENTITY_COUNT
    provider_type = runtime_modules.operational_snapshot_sensors.SnapshotProviderStatusSensor
    provider_entities = [
        entity for entity in snapshot_entities if isinstance(entity, provider_type)
    ]
    assert len(provider_entities) == len(
        runtime_modules.operational_snapshot_sensors.WORKER_ONLY_PROVIDER_IDS
    ) == 26
    assert len({id(entity._hub) for entity in snapshot_entities}) == 1
    assert all(entity.hass is None for entity in first.entities)
    assert hooks.bridge_setup_count == 0
    assert hooks.event_subscribe_count == 0
    assert requester.calls == []
    assert runtime.diagnostics()["counts"]["prepare_calls"] == 2
    assert runtime.diagnostics()["runtime_mode"] == "worker_only"
    assert not hasattr(module, "async_setup_entry")
    assert not hasattr(module, "async_setup_platform")


def test_default_bridge_requires_authority_before_subscribe_and_cleans_it_up(
    runtime_modules,
    monkeypatch,
):
    async def run():
        module = runtime_modules.worker_only_runtime
        hass = FakeHass()
        cache = runtime_modules.operational_snapshot.OperationalSnapshotCache()
        cleaned = []

        async def setup(inner_hass):
            data = inner_hass.data[module.DOMAIN]
            assert isinstance(data["worker_request_lock"], asyncio.Lock)
            assert data["operational_snapshot_authority_required"] is True
            assert data[
                "operational_snapshot_expected_desired_state_sha256"
            ] is None
            data["unsubscribers"].append(lambda: cleaned.append(True))

        monkeypatch.setattr(module, "async_setup_worker_bridge", setup)
        cleanup = await module._default_bridge_setup(hass, cache)
        data = hass.data[module.DOMAIN]
        assert data["operational_snapshot_cache"] is cache
        await cleanup()
        assert cleaned == [True]
        assert "worker_request_lock" not in data
        assert "operational_snapshot_authority_required" not in data
        assert (
            "operational_snapshot_expected_desired_state_sha256" not in data
        )

    asyncio.run(run())


def test_default_bridge_setup_failure_rolls_back_exact_owned_request_lock(
    runtime_modules,
    monkeypatch,
):
    async def run():
        module = runtime_modules.worker_only_runtime
        hass = FakeHass()
        cache = runtime_modules.operational_snapshot.OperationalSnapshotCache()
        cleaned = []

        async def setup(inner_hass):
            data = inner_hass.data[module.DOMAIN]
            assert isinstance(data["worker_request_lock"], asyncio.Lock)
            data["unsubscribers"].append(lambda: cleaned.append(True))
            raise RuntimeError("bridge setup failed")

        monkeypatch.setattr(module, "async_setup_worker_bridge", setup)
        with pytest.raises(RuntimeError, match="bridge setup failed"):
            await module._default_bridge_setup(hass, cache)

        data = hass.data[module.DOMAIN]
        assert cleaned == [True]
        assert "worker_request_lock" not in data
        assert "operational_snapshot_authority_required" not in data
        assert (
            "operational_snapshot_expected_desired_state_sha256" not in data
        )

        foreign_hass = FakeHass()
        foreign_lock = asyncio.Lock()
        foreign_hass.data[module.DOMAIN] = {
            "worker_request_lock": foreign_lock,
        }
        with pytest.raises(RuntimeError, match="bridge setup failed"):
            await module._default_bridge_setup(
                foreign_hass,
                runtime_modules.operational_snapshot.OperationalSnapshotCache(),
            )
        assert cleaned == [True, True]
        assert (
            foreign_hass.data[module.DOMAIN]["worker_request_lock"]
            is foreign_lock
        )

    asyncio.run(run())


def test_default_bridge_cleanup_preserves_foreign_or_swapped_request_lock(
    runtime_modules,
    monkeypatch,
):
    async def run():
        module = runtime_modules.worker_only_runtime

        async def setup(_hass):
            return None

        monkeypatch.setattr(module, "async_setup_worker_bridge", setup)

        preexisting_hass = FakeHass()
        preexisting = asyncio.Lock()
        preexisting_hass.data[module.DOMAIN] = {
            "worker_request_lock": preexisting,
        }
        cleanup = await module._default_bridge_setup(
            preexisting_hass,
            runtime_modules.operational_snapshot.OperationalSnapshotCache(),
        )
        await cleanup()
        assert (
            preexisting_hass.data[module.DOMAIN]["worker_request_lock"]
            is preexisting
        )

        swapped_hass = FakeHass()
        cleanup = await module._default_bridge_setup(
            swapped_hass,
            runtime_modules.operational_snapshot.OperationalSnapshotCache(),
        )
        owned = swapped_hass.data[module.DOMAIN]["worker_request_lock"]
        foreign = asyncio.Lock()
        swapped_hass.data[module.DOMAIN]["worker_request_lock"] = foreign
        await cleanup()
        assert swapped_hass.data[module.DOMAIN]["worker_request_lock"] is foreign
        assert foreign is not owned

    asyncio.run(run())


def test_default_bridge_cleanup_refuses_locked_owned_request_authority_until_free(
    runtime_modules,
    monkeypatch,
):
    async def run():
        module = runtime_modules.worker_only_runtime

        async def setup(_hass):
            return None

        monkeypatch.setattr(module, "async_setup_worker_bridge", setup)
        hass = FakeHass()
        cleanup = await module._default_bridge_setup(
            hass,
            runtime_modules.operational_snapshot.OperationalSnapshotCache(),
        )
        data = hass.data[module.DOMAIN]
        owned = data["worker_request_lock"]
        await owned.acquire()

        with pytest.raises(
            module.WorkerOnlyRuntimeError,
            match="request-lock is nog actief",
        ):
            await cleanup()
        assert data["worker_request_lock"] is owned
        assert data["operational_snapshot_authority_required"] is True

        owned.release()
        await cleanup()
        await cleanup()
        assert "worker_request_lock" not in data
        assert "operational_snapshot_authority_required" not in data

    asyncio.run(run())


def test_start_twice_makes_one_bridge_subscription_controller_and_eventhub(
    runtime_modules,
):
    async def run():
        module = runtime_modules.worker_only_runtime
        requester = FakeRequester()
        requester.block = True
        hooks = FakeHooks()
        hass = FakeHass()
        runtime = _runtime(module, requester, hooks, hass=hass)
        first_start = asyncio.create_task(runtime.async_start())
        second_start = asyncio.create_task(runtime.async_start())
        await asyncio.wait_for(requester.block_started.wait(), timeout=1)
        assert hooks.bridge_setup_count == 1
        assert hooks.event_subscribe_count == 1
        requester.release.set()
        prepared, again = await asyncio.gather(first_start, second_start)
        third = await runtime.async_start()

        assert again is prepared
        assert third is prepared
        assert hooks.bridge_setup_count == 1
        assert hooks.event_subscribe_count == 1
        assert [call[0] for call in requester.calls] == [
            "desired_state_claim",
            "desired_state_snapshot",
            "desired_state_apply",
        ]
        diagnostics = runtime.diagnostics()
        assert diagnostics["state"] == "started"
        assert diagnostics["counts"]["controller_starts"] == 1
        assert diagnostics["counts"]["bridge_setups"] == 1
        assert diagnostics["counts"]["event_subscriptions"] == 1

        for entity in prepared.entities:
            entity.hass = hass
            await entity.async_added_to_hass()
        assert hooks.event_subscribe_count == 1
        hooks.event_callback(types.SimpleNamespace(event_type=EVENT_NAME, data={}))
        assert all(entity._test_write_count == 1 for entity in prepared.entities)

    asyncio.run(run())


def test_worker_offline_is_unknown_or_stale_and_controller_retains_dirty_state(
    runtime_modules,
):
    async def run():
        module = runtime_modules.worker_only_runtime
        requester = FakeRequester()
        requester.offline = True
        hooks = FakeHooks()

        async def offline_bridge(_hass, cache):
            hooks.bridge_setup_count += 1
            hooks.cache = cache
            cache.apply_snapshot(_worker_snapshot())
            assert cache.mark_worker_offline()

            async def cleanup():
                hooks.bridge_cleanup_count += 1

            return cleanup

        runtime = module.WorkerOnlyRuntimeCandidate(
            FakeHass(),
            config_snapshot=_config(),
            target_snapshot=[_target()],
            boot_id="boot-offline",
            bridge_setup=offline_bridge,
            requester=requester,
            event_subscribe=hooks.event_subscribe,
            clock=lambda: NOW,
            debounce_seconds=0,
        )
        prepared = await runtime.async_start()
        diagnostics = runtime.diagnostics()
        assert prepared.cache.availability == "stale"
        assert diagnostics["snapshot_availability"] == "stale"
        assert diagnostics["controller"]["state"] == "offline"
        assert diagnostics["controller"]["dirty"] is True
        assert diagnostics["controller"]["in_flight"] is False
        assert len(requester.calls) == 1

    asyncio.run(run())


def test_update_while_offline_is_one_dirty_snapshot_then_reconnects(
    runtime_modules,
):
    async def run():
        module = runtime_modules.worker_only_runtime
        requester = FakeRequester()
        hooks = FakeHooks()
        hass = FakeHass()
        runtime = _runtime(module, requester, hooks, hass=hass)
        prepared = await runtime.async_start()
        authority_key = (
            "operational_snapshot_expected_desired_state_sha256"
        )
        initial_authority = hass.data["storm_tracker_v3"][authority_key]
        assert isinstance(initial_authority, str) and len(initial_authority) == 64

        requester.offline = True
        changed = await runtime.async_update(
            config_snapshot=_config(
                sensor_target_ids=("person.wim",),
                settings=_settings(ground_radius_km=21),
            ),
            target_snapshot=[
                _target(),
                _target("person.wim", observed_at=NOW + 1, latitude=51.05),
            ],
        )
        assert changed
        assert hass.data["storm_tracker_v3"][authority_key] is None
        await prepared.controller.async_flush()
        assert prepared.controller.diagnostics()["dirty"] is True
        assert hass.data["storm_tracker_v3"][authority_key] is None
        calls_at_offline = len(requester.calls)

        requester.offline = False
        await runtime.async_worker_reconnected()
        assert len(requester.calls) > calls_at_offline
        assert prepared.controller.diagnostics()["dirty"] is False
        assert hass.data["storm_tracker_v3"][authority_key] != initial_authority
        assert runtime.diagnostics()["counts"]["updates"] == 1

    asyncio.run(run())


def test_unload_twice_removes_subscription_bridge_entities_and_tasks(
    runtime_modules,
):
    async def run():
        module = runtime_modules.worker_only_runtime
        requester = FakeRequester()
        hooks = FakeHooks()
        hass = FakeHass()
        runtime = _runtime(module, requester, hooks, hass=hass)
        prepared = await runtime.async_start()
        for entity in prepared.entities:
            entity.hass = hass
            await entity.async_added_to_hass()
        assert prepared.event_hub._entities

        await runtime.async_unload()
        await runtime.async_unload()
        diagnostics = runtime.diagnostics()
        assert diagnostics["state"] == "unloaded"
        assert diagnostics["counts"]["unloads"] == 1
        assert hooks.event_cleanup_count == 1
        assert hooks.bridge_cleanup_count == 1
        assert hooks.event_callback is None
        assert prepared.event_hub._entities == {}
        assert prepared.event_hub._unsubscribe is None
        assert prepared.controller.diagnostics()["in_flight"] is False

        for entity in prepared.entities:
            for callback in getattr(entity, "_test_remove_callbacks", ()):
                callback()
        assert hooks.event_cleanup_count == 1

    asyncio.run(run())


def test_unload_cancels_a_start_blocked_inside_controller(runtime_modules):
    async def run():
        module = runtime_modules.worker_only_runtime
        requester = FakeRequester()
        requester.block = True
        hooks = FakeHooks()
        runtime = _runtime(module, requester, hooks)
        start = asyncio.create_task(runtime.async_start())
        await asyncio.wait_for(requester.block_started.wait(), timeout=1)
        await runtime.async_unload()
        await asyncio.gather(start, return_exceptions=True)
        assert runtime.diagnostics()["state"] == "unloaded"
        assert hooks.event_cleanup_count == 1
        assert hooks.bridge_cleanup_count == 1
        assert runtime.prepare().controller.diagnostics()["in_flight"] is False

    asyncio.run(run())


def test_exact_64_target_and_entity_limits_are_enforced_before_setup(
    runtime_modules,
):
    module = runtime_modules.worker_only_runtime
    targets = [_target()] + [_target(f"person.t{index:02d}") for index in range(63)]
    sensor_ids = tuple(
        f"person.t{index:02d}"
        for index in range(module.MAX_ADDITIONAL_TARGET_ENTITIES)
    )
    requester = FakeRequester()
    hooks = FakeHooks()
    runtime = _runtime(
        module,
        requester,
        hooks,
        config=_config(sensor_target_ids=sensor_ids),
        targets=targets,
    )
    prepared = runtime.prepare()
    assert prepared.target_count == 64
    assert prepared.entity_count == 64

    too_many_targets = targets + [_target("person.t64")]
    oversized = _runtime(
        module,
        requester,
        hooks,
        config=_config(sensor_target_ids=sensor_ids),
        targets=too_many_targets,
    )
    with pytest.raises(ValueError, match="64|lijstgrens"):
        oversized.prepare()

    too_many_entities = _runtime(
        module,
        requester,
        hooks,
        config=_config(
            sensor_target_ids=tuple(
                f"person.t{index:02d}"
                for index in range(module.MAX_ADDITIONAL_TARGET_ENTITIES + 1)
            )
        ),
        targets=targets,
    )
    with pytest.raises(ValueError, match="entitygrens"):
        too_many_entities.prepare()
    assert hooks.bridge_setup_count == 0
    assert hooks.event_subscribe_count == 0


def test_sensor_reads_are_cached_o1_and_cannot_trigger_runtime_work(runtime_modules):
    module = runtime_modules.worker_only_runtime
    requester = FakeRequester()
    hooks = FakeHooks()
    runtime = _runtime(module, requester, hooks)
    prepared = runtime.prepare()
    before = (len(requester.calls), hooks.bridge_setup_count, hooks.event_subscribe_count)
    for entity in prepared.entities:
        for _ in range(100):
            _ = entity.native_value
            _ = entity.available
            _ = entity.extra_state_attributes
    assert (len(requester.calls), hooks.bridge_setup_count, hooks.event_subscribe_count) == before


def test_diagnostics_are_bounded_and_exclude_config_targets_and_coordinates(
    runtime_modules,
):
    module = runtime_modules.worker_only_runtime
    requester = FakeRequester()
    hooks = FakeHooks()
    runtime = _runtime(module, requester, hooks)
    runtime.prepare()
    encoded = json.dumps(runtime.diagnostics(), sort_keys=True)
    assert len(encoded.encode("utf-8")) < 4096
    assert "51.04" not in encoded
    assert "4.48" not in encoded
    assert "person.wim" not in encoded
    assert "unique_id_prefix" not in encoded
    assert "settings" not in encoded


def test_ast_and_runtime_graph_exclude_every_heavy_legacy_surface(runtime_modules):
    module = runtime_modules.worker_only_runtime
    tree = ast.parse(MODULE_PATH.read_text(encoding="utf-8"))
    imports = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imports.append(("." * node.level) + (node.module or ""))
    forbidden = (
        "providers",
        "engine",
        "calibration",
        "radar_frame_worker",
        "worker_radar_client",
        "geojson_worker",
        "sqlite3",
        "pathlib",
        "aiohttp",
        "requests",
        "urllib",
    )
    assert not any(token in name for name in imports for token in forbidden)
    assert ".worker_bridge" in imports
    assert ".desired_state_builder" in imports
    assert ".desired_state_controller" in imports
    assert ".operational_snapshot" in imports
    assert ".operational_snapshot_sensors" in imports

    source = MODULE_PATH.read_text(encoding="utf-8")
    assert "async_track_time_interval" not in source
    assert "DataUpdateCoordinator" not in source
    assert "run_in_executor" not in source
    assert "async_add_executor_job" not in source
    assert "async_setup_entry" not in source
    assert "async_setup_platform" not in source
    calls = {
        node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }
    assert calls.isdisjoint({"open", "print", "connect", "execute"})

    reachable_modules = {
        value.__name__
        for value in module.__dict__.values()
        if isinstance(value, types.ModuleType)
    }
    assert not any(
        token in name for name in reachable_modules for token in forbidden
    )
    loaded_candidate_modules = {
        name
        for name in sys.modules
        if name.startswith(module.__package__ + ".")
    }
    assert not any(
        token in name for name in loaded_candidate_modules for token in forbidden
    )
