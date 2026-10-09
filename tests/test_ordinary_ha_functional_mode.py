"""End-to-end contracts for the explicit ordinary-HA legacy mode."""

from __future__ import annotations

import ast
import asyncio
import copy
import time
from pathlib import Path
from types import SimpleNamespace

import pytest


ROOT = Path(__file__).resolve().parent.parent
COMPONENT = ROOT / "custom_components" / "storm_tracker_v3"
INIT_PATH = COMPONENT / "__init__.py"
SENSOR_PATH = COMPONENT / "sensor.py"
CONFIG_FLOW_PATH = COMPONENT / "config_flow.py"
DOMAIN = "storm_tracker_v3"


def _tree(path: Path) -> ast.Module:
    return ast.parse(path.read_text(encoding="utf-8"))


def _top_level(name: str, *, path: Path = INIT_PATH) -> ast.AST:
    return next(
        node
        for node in _tree(path).body
        if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == name
    )


def _runtime_nested(name: str) -> ast.AST:
    runtime = _top_level("_async_setup_runtime")
    return next(
        node
        for node in ast.walk(runtime)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == name
    )


def _class_method(class_name: str, method_name: str, path: Path) -> ast.AST:
    owner = _top_level(class_name, path=path)
    return next(
        node
        for node in owner.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == method_name
    )


def _load_nodes(nodes: list[ast.AST], namespace: dict) -> dict:
    module = ast.parse("from __future__ import annotations\n")
    module.body.extend(copy.deepcopy(nodes))
    ast.fix_missing_locations(module)
    exec(compile(module, str(INIT_PATH), "exec"), namespace)
    return namespace


class _Logger:
    def debug(self, *_args):
        return None

    def error(self, *_args):
        return None

    def warning(self, *_args):
        return None


class _Services:
    def has_service(self, *_args):
        return False

    def async_remove(self, *_args):
        return None


class _States:
    def __init__(self):
        self.values = {
            "device_tracker.alice": SimpleNamespace(
                attributes={"friendly_name": "Alice"}
            )
        }

    def get(self, entity_id):
        return self.values.get(entity_id)


class _ConfigEntries:
    def __init__(self):
        self.forward_calls = []
        self.unload_calls = []
        self.reload_calls = []
        self.active_platforms = 0
        self.fail_forward = False

    async def async_forward_entry_setups(self, entry, platforms):
        self.forward_calls.append((entry.entry_id, tuple(platforms)))
        self.active_platforms += 1
        if self.fail_forward:
            raise RuntimeError("synthetic platform failure")

    async def async_unload_platforms(self, entry, platforms):
        self.unload_calls.append((entry.entry_id, tuple(platforms)))
        self.active_platforms = max(0, self.active_platforms - 1)
        return True

    async def async_reload(self, entry_id):
        self.reload_calls.append(entry_id)


class _Hass:
    def __init__(self):
        self.data = {}
        self.config = SimpleNamespace(latitude=51.05, longitude=4.45)
        self.config_entries = _ConfigEntries()
        self.states = _States()
        self.services = _Services()


class _Entry:
    version = 3

    def __init__(self, entry_id="ordinary-ha"):
        self.entry_id = entry_id
        self.data = {
            "runtime_mode": "legacy",
            "persons": ["device_tracker.alice"],
            "radar_radius_km": 280.0,
            "knmi_api_key": "knmi-secret",
            "eumetsat_consumer_key": "eumetsat-key",
            "eumetsat_consumer_secret": "eumetsat-secret",
            "netatmo_client_id": "netatmo-id",
            "netatmo_client_secret": "netatmo-secret",
            "netatmo_refresh_token": "netatmo-refresh",
            "warning_notifications_enabled": True,
            "notify_service": "notify.mobile_app_testtelefoon",
            "unowned_injected_field": "must-not-pass",
        }
        self.options = {
            "radar_radius_km": 320.0,
            "analysis_mode": "thermal_throttle",
        }
        self.listeners = []
        self.unsubscribe_calls = 0

    def add_update_listener(self, listener):
        registration = {"listener": listener, "active": True}
        self.listeners.append(registration)

        def unsubscribe():
            if registration["active"]:
                registration["active"] = False
                self.unsubscribe_calls += 1

        return unsubscribe


def _lifecycle_namespace(runtime_setup):
    async def partial_cleanup(_hass):
        return None

    namespace = {
        "HomeAssistant": object,
        "ConfigType": dict,
        "DOMAIN": DOMAIN,
        "CONF_RUNTIME_MODE": "runtime_mode",
        "CONF_WARNING_NOTIFICATIONS_ENABLED": (
            "warning_notifications_enabled"
        ),
        "CONF_NOTIFY_SERVICE": "notify_service",
        "DATA_ENTRY_RUNTIMES": "entry_runtimes",
        "RUNTIME_MODE_LEGACY": "legacy",
        "RUNTIME_MODE_WORKER_ONLY": "worker_only",
        "CONFIG_ENTRY_VERSION": 3,
        "PLATFORMS": ["sensor"],
        "_LEGACY_ENTRY_FIELDS": frozenset({
            "analysis_mode",
            "engine_sharing_distance_km",
            "eumetsat_consumer_key",
            "eumetsat_consumer_secret",
            "hsaf_password",
            "hsaf_username",
            "knmi_api_key",
            "knmi_wms_api_key",
            "lightning_source_mode",
            "meteofrance_api_token",
            "meteofrance_application_id",
            "netatmo_client_id",
            "netatmo_client_secret",
            "netatmo_radius_km",
            "netatmo_refresh_token",
            "notify_service",
            "open_meteo_enabled",
            "persons",
            "radar_radius_km",
            "test_tracker_entity",
            "warning_notifications_enabled",
        }),
        "_LOGGER": _Logger(),
        "_async_setup_runtime": runtime_setup,
        "_async_partial_legacy_cleanup": partial_cleanup,
    }
    selected = [
        _top_level("LegacyEntryRuntimeError"),
        _top_level("_explicit_entry_mode"),
        _top_level("_legacy_entry_values"),
        _top_level("_legacy_runtime_config"),
        _top_level("_async_reload_legacy_entry"),
        _top_level("_LegacyEntryRuntime"),
        _top_level("_async_setup_legacy_entry"),
        _top_level("async_setup_entry"),
        _top_level("async_unload_entry"),
        _top_level("async_setup"),
    ]
    return _load_nodes(selected, namespace)


@pytest.mark.asyncio
async def test_explicit_legacy_setup_poll_outputs_unload_and_reload():
    setup_calls = []
    shutdown_calls = []

    async def runtime_setup(hass, conf, config):
        setup_calls.append((copy.deepcopy(conf), copy.deepcopy(config)))
        data = hass.data[DOMAIN]
        data.update({
            "provider_lifecycle_diagnostics": {
                "dwd_radolan": {"status": "active"},
                "met_office_radar": {"status": "standby"},
                "aemet_radar": {"status": "outside_coverage"},
            },
            "legacy_provider_configuration": {
                "radar": {"kmi": {"status": "configured"}},
                "satellite": {"hsaf_h40b": {"status": "credentials_missing"}},
                "lightning": {"blitzortung": {"status": "configured"}},
                "ground": {"metar": {"status": "configured"}},
                "model": {"open_meteo": {"status": "disabled"}},
                "notifications": {
                    "warning_dispatch": {"status": "disabled"}
                },
            },
            "storms": ["storm-result"],
            "precipitation_status": {
                "status": "waargenomen",
                "forecast_available": False,
                "warning_eligible": False,
            },
            "geojson_worker_collection": {
                "type": "FeatureCollection",
                "features": [{"id": "storm-result"}],
            },
            "warning_snapshot_registration": True,
            "poll_all_calls": data.get("poll_all_calls", 0) + 1,
        })

        async def shutdown():
            shutdown_calls.append(conf["home_lat"])

        data["async_shutdown_runtime"] = shutdown
        return True

    namespace = _lifecycle_namespace(runtime_setup)
    hass = _Hass()
    entry = _Entry()

    assert await namespace["async_setup_entry"](hass, entry)
    conf, runtime_config = setup_calls[0]
    assert conf["radar_radius_km"] == 320.0
    assert conf["analysis_mode"] == "thermal_throttle"
    assert conf["targets"] == [{
        "id": "alice",
        "name": "Alice",
        "location_entity": "device_tracker.alice",
    }]
    assert conf["knmi_api_key"] == "knmi-secret"
    assert conf["warning_notifications_enabled"] is True
    assert conf["notify_service"] == "notify.mobile_app_testtelefoon"
    assert "unowned_injected_field" not in conf
    assert "runtime_mode" not in conf
    assert runtime_config == {DOMAIN: conf}
    default_entry = _Entry("ordinary-ha-default-warning")
    default_entry.data.pop("warning_notifications_enabled")
    default_entry.data.pop("notify_service")
    default_conf = namespace["_legacy_runtime_config"](
        hass, default_entry
    )
    assert default_conf["warning_notifications_enabled"] is False
    assert default_conf["notify_service"] is None
    record = hass.data[DOMAIN]["entry_runtimes"][entry.entry_id]
    assert record == {
        "mode": "legacy",
        "runtime_mode": "legacy",
        "entry_id": entry.entry_id,
        "manager": record["manager"],
        "state": "active",
    }
    assert hass.data[DOMAIN]["runtime_diagnostics"] == {
        "runtime_mode": "legacy",
        "selection_source": "data",
        "legacy_compute_enabled": True,
        "owner_entry_id": entry.entry_id,
        "state": "active",
    }
    assert hass.data[DOMAIN]["poll_all_calls"] == 1
    assert hass.data[DOMAIN]["storms"] == ["storm-result"]
    assert hass.data[DOMAIN]["warning_snapshot_registration"] is True
    assert set(hass.data[DOMAIN]["legacy_provider_configuration"]) == {
        "radar", "satellite", "lightning", "ground", "model",
        "notifications",
    }
    assert hass.config_entries.active_platforms == 1

    listener = next(item for item in entry.listeners if item["active"])
    await listener["listener"](hass, entry)
    assert hass.config_entries.reload_calls == [entry.entry_id]

    assert await namespace["async_unload_entry"](hass, entry)
    assert DOMAIN not in hass.data
    assert hass.config_entries.active_platforms == 0
    assert shutdown_calls == [51.05]
    assert entry.unsubscribe_calls == 1

    assert await namespace["async_setup_entry"](hass, entry)
    assert hass.config_entries.active_platforms == 1
    assert hass.data[DOMAIN]["poll_all_calls"] == 1
    assert len(setup_calls) == 2
    assert await namespace["async_unload_entry"](hass, entry)
    assert hass.config_entries.active_platforms == 0
    assert shutdown_calls == [51.05, 51.05]


@pytest.mark.asyncio
async def test_partial_platform_failure_rolls_back_every_owner():
    shutdown_calls = []

    async def runtime_setup(hass, _conf, _config):
        async def shutdown():
            shutdown_calls.append(True)

        hass.data[DOMAIN]["async_shutdown_runtime"] = shutdown
        return True

    namespace = _lifecycle_namespace(runtime_setup)
    hass = _Hass()
    hass.config_entries.fail_forward = True
    entry = _Entry()

    with pytest.raises(RuntimeError, match="platform failure"):
        await namespace["async_setup_entry"](hass, entry)

    assert DOMAIN not in hass.data
    assert hass.config_entries.active_platforms == 0
    assert hass.config_entries.unload_calls == [
        (entry.entry_id, ("sensor",))
    ]
    assert shutdown_calls == [True]
    assert entry.unsubscribe_calls == 1

    class BrokenConfig:
        @property
        def latitude(self):
            raise RuntimeError("synthetic config projection failure")

        longitude = 4.45

    projection_hass = _Hass()
    projection_hass.config = BrokenConfig()
    with pytest.raises(RuntimeError, match="config projection failure"):
        await namespace["async_setup_entry"](
            projection_hass, _Entry("projection-failure")
        )
    assert DOMAIN not in projection_hass.data


@pytest.mark.asyncio
async def test_stale_legacy_manager_cannot_unload_a_different_owner():
    shutdown_calls = []

    async def runtime_setup(hass, _conf, _config):
        async def shutdown():
            shutdown_calls.append(True)

        hass.data[DOMAIN]["async_shutdown_runtime"] = shutdown
        return True

    namespace = _lifecycle_namespace(runtime_setup)
    hass = _Hass()
    entry = _Entry("exact-owner")
    assert await namespace["async_setup_entry"](hass, entry)
    record = hass.data[DOMAIN]["entry_runtimes"][entry.entry_id]
    manager = record["manager"]
    foreign_manager = object()
    record["manager"] = foreign_manager

    await manager.async_unload()

    assert hass.data[DOMAIN]["entry_runtimes"][entry.entry_id][
        "manager"
    ] is foreign_manager
    assert shutdown_calls == []
    assert entry.unsubscribe_calls == 0

    record["manager"] = manager
    await manager.async_unload()
    assert DOMAIN not in hass.data
    assert shutdown_calls == [True]
    assert entry.unsubscribe_calls == 1


@pytest.mark.asyncio
async def test_singleton_and_yaml_guards_never_infer_legacy():
    setup_count = 0

    async def runtime_setup(hass, _conf, _config):
        nonlocal setup_count
        setup_count += 1

        async def shutdown():
            return None

        hass.data[DOMAIN]["async_shutdown_runtime"] = shutdown
        return True

    namespace = _lifecycle_namespace(runtime_setup)
    hass = _Hass()
    first = _Entry("first")
    second = _Entry("second")
    assert await namespace["async_setup_entry"](hass, first)
    assert not await namespace["async_setup_entry"](hass, second)
    assert setup_count == 1
    assert await namespace["async_unload_entry"](hass, first)

    missing = _Entry("missing")
    missing.data.pop("runtime_mode")
    assert not await namespace["async_setup_entry"](_Hass(), missing)
    future = _Entry("future")
    future.data["runtime_mode"] = "future"
    assert not await namespace["async_setup_entry"](_Hass(), future)

    assert await namespace["async_setup"](_Hass(), {})
    assert not await namespace["async_setup"](
        _Hass(), {DOMAIN: {"runtime_mode": "legacy"}}
    )
    assert setup_count == 1


@pytest.mark.asyncio
async def test_ui_reconfigure_switches_modes_and_triggers_one_owner_reload():
    config_namespace = {
        "CONF_RUNTIME_MODE": "runtime_mode",
        "RUNTIME_MODE_LEGACY": "legacy",
        "RUNTIME_MODE_WORKER_ONLY": "worker_only",
        "RUNTIME_MODE_OPTIONS": ("worker_only", "legacy"),
        "WORKER_ONLY_SCHEMA_FIELDS": frozenset({
            "persons",
            "radar_radius_km",
        }),
        "LEGACY_SCHEMA_FIELDS": frozenset({
            "persons",
            "radar_radius_km",
            "knmi_api_key",
            "analysis_mode",
            "warning_notifications_enabled",
            "notify_service",
        }),
    }
    _load_nodes(
        [
            _top_level("_worker_only_input", path=CONFIG_FLOW_PATH),
            _top_level("_legacy_input", path=CONFIG_FLOW_PATH),
            _top_level("_entry_mode", path=CONFIG_FLOW_PATH),
            _class_method(
                "StormTrackerConfigFlow",
                "async_step_reconfigure",
                CONFIG_FLOW_PATH,
            ),
            _class_method(
                "StormTrackerConfigFlow",
                "async_step_reconfigure_worker_only",
                CONFIG_FLOW_PATH,
            ),
            _class_method(
                "StormTrackerConfigFlow",
                "async_step_reconfigure_legacy",
                CONFIG_FLOW_PATH,
            ),
        ],
        config_namespace,
    )

    class Flow:
        def __init__(self, entry):
            self.entry = entry
            self.updates = []

        def _get_reconfigure_entry(self):
            return self.entry

        async def async_step_reconfigure_legacy(self, user_input=None):
            return ("legacy", user_input)

        async def async_step_reconfigure_worker_only(self, user_input=None):
            return ("worker_only", user_input)

        def async_abort(self, *, reason):
            return {"type": "abort", "reason": reason}

        def async_show_form(self, **kwargs):
            return {"type": "form", **kwargs}

        def async_update_and_abort(self, entry, *, data, options):
            self.updates.append((copy.deepcopy(data), copy.deepcopy(options)))
            entry.data = dict(data)
            entry.options = dict(options)
            return {"type": "abort", "reason": "reconfigure_successful"}

    entry = SimpleNamespace(
        entry_id="mode-owner",
        data={
            "runtime_mode": "worker_only",
            "persons": ["device_tracker.alice"],
        },
        options={},
    )
    flow = Flow(entry)
    choose = await config_namespace["async_step_reconfigure"](
        flow, {"runtime_mode": "legacy"}
    )
    assert choose == ("legacy", None)
    assert flow._pending_reconfigure_mode == "legacy"
    result = await config_namespace["async_step_reconfigure_legacy"](
        flow,
        {
            "persons": ["device_tracker.alice"],
            "knmi_api_key": "legacy-secret",
            "analysis_mode": "full_send",
            "warning_notifications_enabled": True,
            "notify_service": "notify.mobile_app_testtelefoon",
            "injected": "drop",
        },
    )
    assert result == {"type": "abort", "reason": "reconfigure_successful"}
    assert flow.updates == [({
        "runtime_mode": "legacy",
        "persons": ["device_tracker.alice"],
        "knmi_api_key": "legacy-secret",
        "analysis_mode": "full_send",
        "warning_notifications_enabled": True,
        "notify_service": "notify.mobile_app_testtelefoon",
    }, {})]

    reload_namespace = {
        "HomeAssistant": object,
        "DOMAIN": DOMAIN,
        "DATA_ENTRY_RUNTIMES": "entry_runtimes",
        "CONF_RUNTIME_MODE": "runtime_mode",
        "RUNTIME_MODE_LEGACY": "legacy",
        "RUNTIME_MODE_WORKER_ONLY": "worker_only",
        "_LOGGER": _Logger(),
    }
    _load_nodes(
        [
            _top_level("_explicit_entry_mode"),
            _top_level("_async_options_updated"),
            _top_level("_async_reload_legacy_entry"),
        ],
        reload_namespace,
    )
    hass = _Hass()
    hass.data[DOMAIN] = {
        "entry_runtimes": {
            entry.entry_id: {"mode": "worker_only"},
        }
    }
    await reload_namespace["_async_options_updated"](hass, entry)
    assert hass.config_entries.reload_calls == [entry.entry_id]

    hass.data[DOMAIN]["entry_runtimes"][entry.entry_id]["mode"] = "legacy"
    reverse_flow = Flow(entry)
    choose = await config_namespace["async_step_reconfigure"](
        reverse_flow, {"runtime_mode": "worker_only"}
    )
    assert choose == ("worker_only", None)
    result = await config_namespace[
        "async_step_reconfigure_worker_only"
    ](
        reverse_flow,
        {
            "persons": ["device_tracker.alice"],
            "radar_radius_km": 300,
            "knmi_api_key": "must-not-project",
            "warning_notifications_enabled": True,
            "notify_service": "notify.mobile_app_must_not_project",
        },
    )
    assert result == {"type": "abort", "reason": "reconfigure_successful"}
    assert entry.data == {
        "runtime_mode": "worker_only",
        "persons": ["device_tracker.alice"],
        "radar_radius_km": 300,
    }
    assert entry.options == {}
    assert "knmi_api_key" not in entry.data
    await reload_namespace["_async_reload_legacy_entry"](hass, entry)
    assert hass.config_entries.reload_calls == [entry.entry_id, entry.entry_id]


@pytest.mark.asyncio
async def test_real_poll_all_executes_every_existing_provider_stage():
    stages = []
    direct_calls = []

    async def stage(name, awaitable, _timeout):
        stages.append(name)
        return await awaitable

    async def mark(name):
        direct_calls.append(name)

    namespace = {
        "asyncio": asyncio,
        "time": time,
        "DOMAIN": DOMAIN,
        "CoreState": SimpleNamespace(running="running"),
        "ha_started": SimpleNamespace(is_set=lambda: True),
        "hass": SimpleNamespace(
            data={DOMAIN: {
                "legacy_provider_configuration": {
                    "notifications": {"warning_dispatch": {}}
                }
            }},
            state="running",
        ),
        "storm_manager": SimpleNamespace(admission_ready=True),
        "_LOGGER": _Logger(),
        "_bounded_provider_stage": stage,
        "_sync_region_radar_providers": lambda: direct_calls.append(
            "sync_radar"
        ),
        "_poll_national_providers": lambda: mark("national"),
        "_poll_radar_comparison": lambda: mark("radar_comparison"),
        "_poll_radar": lambda: mark("radar"),
        "_poll_netatmo": lambda: mark("ground"),
        "_poll_open_meteo": lambda: mark("model"),
        "_update_virtual_probes_from_cache": lambda: mark("probe"),
        "_flush_region_observation_batches": lambda: mark("flush"),
        "_queue_target_verification_samples": lambda: (
            direct_calls.append("verification") or ("evaluation",)
        ),
        "warning_dispatcher": SimpleNamespace(
            async_process=lambda _evaluations: mark("warning_dispatch"),
            diagnostics={"status": "sent"},
        ),
        "_flush_calibration_data": lambda: mark("calibration"),
    }
    _load_nodes([_runtime_nested("_poll_all")], namespace)

    await namespace["_poll_all"]()

    assert stages == [
        "national",
        "radar_comparison",
        "radar",
        "netatmo",
        "open_meteo",
        "virtual_probe_evaluation",
        "observation_flush",
        "warning_dispatch",
        "calibration_storage",
    ]
    assert direct_calls == [
        "sync_radar",
        "national",
        "radar_comparison",
        "radar",
        "ground",
        "model",
        "probe",
        "flush",
        "verification",
        "warning_dispatch",
        "calibration",
    ]
    assert namespace["hass"].data[DOMAIN]["warning_dispatch_diagnostics"] == {
        "status": "sent"
    }


@pytest.mark.asyncio
async def test_legacy_startup_really_invokes_poll_all_after_recovery():
    calls = []

    async def mark(name, *_args, **_kwargs):
        calls.append(name)

    async def calibration_snapshot():
        calls.append("calibration_snapshot")

    def background_task(awaitable, name):
        calls.append(("background", name))
        awaitable.close()
        return SimpleNamespace()

    manager = SimpleNamespace(
        complete_durable_recovery=lambda: calls.append("recovery_complete")
    )
    hass = SimpleNamespace(
        state="running",
        data={
            DOMAIN: {
                "durable_processing": {},
                "providers_initialized": False,
            }
        },
        async_create_background_task=background_task,
    )
    namespace = {
        "ha_started": SimpleNamespace(is_set=lambda: True),
        "hass": hass,
        "CoreState": SimpleNamespace(running="running"),
        "DOMAIN": DOMAIN,
        "_update_fictieve_location": lambda: mark("fictieve_location"),
        "secondary_specs": [],
        "_update_secondary_target": lambda *_args, **_kwargs: mark(
            "secondary_target"
        ),
        "_restore_pending_region_frames_and_audit": lambda: calls.append(
            "restore_audit"
        ),
        "storm_manager": manager,
        "_init_location_providers": lambda *_args: mark("provider_init"),
        "home_lat": 51.05,
        "home_lon": 4.45,
        "_refresh_worker_calibration_snapshot": calibration_snapshot,
        "_activate_polling_schedules": lambda: calls.append("schedules"),
        "lightning_source_mode": "auto",
        "blitz": SimpleNamespace(start=lambda: calls.append("blitz_start")),
        "_poll_all": lambda: mark("poll_all"),
        "_poll_eumetsat_li": lambda: mark("eumetsat"),
        "_poll_goes_glm": lambda: mark("goes_glm"),
    }
    _load_nodes([_runtime_nested("_do_initial_setup")], namespace)

    await namespace["_do_initial_setup"]()

    assert calls == [
        "fictieve_location",
        "restore_audit",
        "recovery_complete",
        "provider_init",
        ("background", f"{DOMAIN}_calibration_corpus_snapshot"),
        "schedules",
        "blitz_start",
        "poll_all",
    ]
    assert hass.data[DOMAIN]["durable_processing"] == {
        "status": "ready",
        "admission_ready": True,
    }
    assert hass.data[DOMAIN]["providers_initialized"] is True


@pytest.mark.asyncio
async def test_explicit_legacy_worker_offline_commits_real_local_outputs(
    region_manager_module,
    observation_module,
    nowcast_module,
    geojson_module,
):
    hass = SimpleNamespace(
        data={DOMAIN: {"runtime_mode": "legacy"}},
        async_create_task=lambda awaitable: asyncio.create_task(awaitable),
    )
    namespace = {
        "hass": hass,
        "DOMAIN": DOMAIN,
        "RUNTIME_MODE_LEGACY": "legacy",
    }
    _load_nodes(
        [_runtime_nested("_run_independent_tracking_check")],
        namespace,
    )
    manager = region_manager_module.StormManager(
        hass=hass,
        observation_radius_km=350,
        on_observation_batch=namespace[
            "_run_independent_tracking_check"
        ],
        allow_local_processing=True,
    )
    region = manager.assign_target("zone.home", 51.05, 4.45)
    observation = observation_module.Observation(
        obs_type=observation_module.ObservationType.RADAR,
        lat=51.05,
        lon=4.45,
        timestamp=time.time(),
        intensity=5,
        max_dbz=42.0,
        area_km2=25.0,
        quality=0.9,
        source="ordinary-ha-functional-proof",
    )
    batch = region_manager_module.ObservationBatch([observation])

    await region.ofe._on_batch(batch)

    storms = region.storm_engine.get_active_storms()
    assert batch.committed is True
    assert region.storm_engine.revision == 1
    assert len(storms) == 1
    assert hass.data[DOMAIN]["tracking_worker"][region.engine_id] == {
        "matching_batches": 2,
        "mode": "legacy_local",
        "state": "lokale_verwerking_geselecteerd",
        "parity": False,
        "batch_id": batch.batch_id,
        "start_revision": 0,
        "worker_required": False,
    }

    status = nowcast_module.build_precipitation_status(
        storms,
        51.05,
        4.45,
        radar_source="ordinary-ha-functional-proof",
    )
    assert status["status"] == "waargenomen"
    assert status["storm_id"] == storms[0].storm_id
    assert "eta_minutes" in status
    assert "forecast_available" in status
    assert "warning_eligible" in status

    collection = geojson_module.build_feature_collection(
        {
            "home": {
                "id": "home",
                "name": "Thuis",
                "latitude": 51.05,
                "longitude": 4.45,
                "available": True,
                "region_engine_id": region.engine_id,
            }
        },
        [region],
        active_radar_source="ordinary-ha-functional-proof",
    )
    assert collection["metadata"]["feature_count"] > 0
    assert any(
        feature.get("properties", {}).get("storm_id")
        == storms[0].storm_id
        for feature in collection["features"]
    )


def test_legacy_sensor_platform_exposes_results_nowcast_map_and_diagnostics():
    factory = _top_level("_legacy_sensor_entities", path=SENSOR_PATH)
    constructors = {
        node.func.id
        for node in ast.walk(factory)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }
    assert {
        "ProviderLifecycleSensor",
        "PrecipitationStatusSensor",
        "TargetPrecipitationStatusSensor",
        "StormTellerSensor",
        "StormDetailSensor",
        "McsDetectieSensor",
        "StormMapGeoJsonSensor",
        "SynopticGroundSensor",
        "MetarGroundSensor",
        "NetatmoStationsSensor",
        "OpenMeteoGearSensor",
    } <= constructors

    sensor_source = SENSOR_PATH.read_text(encoding="utf-8")
    map_block = sensor_source.split(
        "class StormMapGeoJsonSensor", 1
    )[1]
    assert 'data.get("runtime_mode") == RUNTIME_MODE_LEGACY' in map_block
    assert "build_feature_collection(" in map_block
    precipitation = sensor_source.split(
        "class PrecipitationStatusSensor", 1
    )[1].split("class TargetPrecipitationStatusSensor", 1)[0]
    assert "build_precipitation_status(" in precipitation
    assert "extra_state_attributes" in precipitation

    runtime_source = INIT_PATH.read_text(encoding="utf-8")
    assert f'{DOMAIN}_warning_sent' not in runtime_source
    assert "WARNING_SENT_EVENT" in runtime_source
    assert 'own_attrs.get("warning_eligible")' in runtime_source
    assert "build_warning_evaluation(" in runtime_source
    assert '"warning_dispatch"' in runtime_source


def test_warning_sent_listener_preserves_exact_dispatch_snapshot():
    pending = []

    async def flush_calibration_data():
        return None

    def create_task(awaitable):
        awaitable.close()

    hass = SimpleNamespace(
        data={DOMAIN: {"verification_samples_pending": pending}},
        async_create_task=create_task,
    )
    namespace = {
        "callback": lambda function: function,
        "DOMAIN": DOMAIN,
        "hass": hass,
        "time": time,
        "_snapshot_bool": lambda value: value is True,
        "_flush_calibration_data": flush_calibration_data,
    }
    _load_nodes(
        [_runtime_nested("_record_warning_snapshot")],
        namespace,
    )
    snapshot = {
        "schema": "storm_tracker_warning_snapshot_v1",
        "target_id": "zone.home",
        "storm_id": "storm-42",
        "warning_stage": "nabij",
        "observed_at": 1_785_000_000.0,
        "latitude": 51.05,
        "longitude": 4.45,
        "status": "naderend",
        "distance_km": 23.0,
        "eta_minutes": 28,
        "passage_classification": "raak",
        "motion_confidence": "Hoog",
        "forecast_available": True,
        "official_warning": False,
    }
    event = SimpleNamespace(
        data=snapshot,
        context=SimpleNamespace(id="warning-event-42"),
    )

    namespace["_record_warning_snapshot"](event)

    assert len(pending) == 1
    assert pending[0]["sample_key"] == "warning:warning-event-42"
    assert pending[0]["sample_type"] == "warning_sent"
    assert pending[0]["snapshot"] == snapshot
    assert pending[0]["own_forecast_available"] is True


def test_legacy_runtime_keeps_every_existing_provider_family_visible():
    source = INIT_PATH.read_text(encoding="utf-8")
    for constructor in (
        "AemetRadarProvider",
        "BlitzortungProvider",
        "DpcRadarProvider",
        "DwdRadolanProvider",
        "GeoSphereAustriaProvider",
        "ItaliaMeteoRadarProvider",
        "KmiProvider",
        "KnmiProvider",
        "MetOfficeRadarProvider",
        "MeteoFranceRadarProvider",
        "MeteoLuxProvider",
        "NetatmoProvider",
        "NoaaEnterpriseRainRateProvider",
        "NoaaGoesGlmProvider",
        "NoaaGoesRrqpeProvider",
        "OpenMeteoProvider",
        "OperaProvider",
        "RainViewerProvider",
    ):
        assert f"{constructor}(" in source
    configuration = source.split(
        'hass.data[DOMAIN]["legacy_provider_configuration"] =', 1
    )[1].split('hass.data[DOMAIN]["lightning_source_mode"]', 1)[0]
    for family in ("radar", "satellite", "lightning", "ground", "model"):
        assert f'"{family}"' in configuration
    assert configuration.count('"credentials_missing"') >= 5
