import ast
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
COMPONENT = ROOT / "custom_components" / "storm_tracker_v3"


def test_config_flow_is_enabled_and_version_matches():
    manifest = json.loads((COMPONENT / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["config_flow"] is True
    assert manifest["version"] == "1.14.153"


def test_config_flow_and_translations_separate_worker_and_legacy_secrets():
    ast.parse((COMPONENT / "config_flow.py").read_text(encoding="utf-8"))
    strings = json.loads((COMPONENT / "strings.json").read_text(encoding="utf-8"))
    dutch = json.loads(
        (COMPONENT / "translations" / "nl.json").read_text(encoding="utf-8")
    )
    assert strings == dutch
    worker_fields = {
        "persons",
        "test_tracker_entity",
        "radar_radius_km",
        "engine_sharing_distance_km",
        "netatmo_radius_km",
        "lightning_source_mode",
        "open_meteo_enabled",
    }
    credential_fields = {
        "eumetsat_consumer_key",
        "eumetsat_consumer_secret",
        "meteofrance_api_token",
        "meteofrance_application_id",
        "knmi_api_key",
        "knmi_wms_api_key",
        "hsaf_username",
        "hsaf_password",
        "netatmo_client_id",
        "netatmo_client_secret",
        "netatmo_refresh_token",
    }
    warning_fields = {
        "warning_notifications_enabled",
        "notify_service",
    }
    for content in (strings, dutch):
        user_fields = set(content["config"]["step"]["user"]["data"])
        config_fields = set(
            content["config"]["step"]["worker_only"]["data"]
        )
        config_legacy_fields = set(
            content["config"]["step"]["legacy"]["data"]
        )
        option_fields = set(content["options"]["step"]["init"]["data"])
        legacy_fields = set(content["options"]["step"]["legacy"]["data"])
        reconfigure_fields = set(
            content["config"]["step"]["reconfigure"]["data"]
        )
        reconfigure_worker_fields = set(
            content["config"]["step"]["reconfigure_worker_only"]["data"]
        )
        reconfigure_legacy_fields = set(
            content["config"]["step"]["reconfigure_legacy"]["data"]
        )
        assert user_fields == {"runtime_mode"}
        assert reconfigure_fields == {"runtime_mode"}
        assert config_fields == worker_fields
        assert option_fields == worker_fields
        assert reconfigure_worker_fields == worker_fields
        assert credential_fields.isdisjoint(
            config_fields | option_fields | reconfigure_worker_fields
        )
        assert credential_fields <= config_legacy_fields
        assert credential_fields <= legacy_fields
        assert credential_fields <= reconfigure_legacy_fields
        assert warning_fields.isdisjoint(
            config_fields | option_fields | reconfigure_worker_fields
        )
        assert warning_fields <= config_legacy_fields
        assert warning_fields <= legacy_fields
        assert warning_fields <= reconfigure_legacy_fields
        assert "analysis_mode" not in (
            config_fields | option_fields | reconfigure_worker_fields
        )
        assert "analysis_mode" in config_legacy_fields
        assert "analysis_mode" in legacy_fields
        assert "analysis_mode" in reconfigure_legacy_fields
        assert "geen officiële waarschuwing" in (
            content["config"]["step"]["legacy"]["description"]
        )
        for step in (
            content["config"]["step"]["worker_only"],
            content["options"]["step"]["init"],
        ):
            description = step["description"]
            assert "Storm Tracker Worker App via Supervisor" in description
            assert "Providergeheimen" in description
            assert "RAW-retentie" in description
            assert "Home Assistant leest of gebruikt" in description
        assert set(content["selector"]["analysis_mode"]["options"]) == {
            "full_send", "thermal_throttle",
        }
        assert set(content["selector"]["runtime_mode"]["options"]) == {
            "worker_only", "legacy",
        }


def test_runtime_mode_is_visible_explicit_and_persisted_by_schema_v3():
    tree = ast.parse((COMPONENT / "config_flow.py").read_text(encoding="utf-8"))
    flow = next(
        node for node in tree.body
        if isinstance(node, ast.ClassDef)
        and node.name == "StormTrackerConfigFlow"
    )
    version = next(
        node for node in flow.body
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == "VERSION" for target in node.targets)
    )
    assert ast.literal_eval(version.value) == 3
    source = (COMPONENT / "config_flow.py").read_text(encoding="utf-8")
    worker_schema_source = source.split("def _base_fields", 1)[1].split(
        "def _schema", 1
    )[0]
    for forbidden in (
        "CONF_EUMETSAT_KEY",
        "CONF_EUMETSAT_SECRET",
        "CONF_METEOFRANCE_TOKEN",
        "CONF_METEOFRANCE_APPLICATION_ID",
        "CONF_KNMI_API_KEY",
        "CONF_KNMI_WMS_API_KEY",
        "CONF_HSAF_USERNAME",
        "CONF_HSAF_PASSWORD",
        "CONF_NETATMO_CLIENT_ID",
        "CONF_NETATMO_CLIENT_SECRET",
        "CONF_NETATMO_REFRESH_TOKEN",
        "CONF_ANALYSIS_MODE",
        "CONF_WARNING_NOTIFICATIONS_ENABLED",
        "CONF_NOTIFY_SERVICE",
        "CONF_RUNTIME_MODE",
    ):
        assert forbidden not in worker_schema_source
    assert "def _runtime_mode_schema" in source
    assert "vol.Required(CONF_RUNTIME_MODE, default=default)" in source
    assert "async def async_step_worker_only" in source
    assert "async def async_step_legacy" in source
    assert "data[CONF_RUNTIME_MODE] = RUNTIME_MODE_WORKER_ONLY" in source
    assert "data[CONF_RUNTIME_MODE] = RUNTIME_MODE_LEGACY" in source
    assert "options.pop(CONF_RUNTIME_MODE, None)" in source
    assert "RUNTIME_MODE_WORKER_ONLY" in source
    assert "RUNTIME_MODE_LEGACY" in source
    assert "_runtime_mode_schema(RUNTIME_MODE_WORKER_ONLY)" in source
    assert source.count("if self._async_current_entries():") == 3
    assert 'step_id="worker_only", data_schema=_worker_only_schema({})' in source
    assert 'step_id="legacy", data_schema=_schema({})' in source
    assert 'data_schema=_worker_only_schema(defaults)' in source
    assert "options = _preserve_hidden_options(" in source
    assert "defaults = _worker_only_entry_values(self.config_entry)" in source


def test_worker_only_input_filters_injected_credentials_and_preserves_stored_options():
    source = (COMPONENT / "config_flow.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    selected = {
        "_worker_only_input",
        "_legacy_input",
        "_worker_only_entry_values",
        "_legacy_entry_values",
        "_entry_mode",
        "_preserve_hidden_options",
    }
    isolated = ast.Module(
        body=[
            node for node in tree.body
            if isinstance(node, ast.FunctionDef) and node.name in selected
        ],
        type_ignores=[],
    )
    ast.fix_missing_locations(isolated)
    namespace = {
        "CONF_RUNTIME_MODE": "runtime_mode",
        "RUNTIME_MODE_LEGACY": "legacy",
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
    exec(compile(isolated, "isolated_config_flow_contract", "exec"), namespace)

    injected = namespace["_worker_only_input"]({
        "persons": ["device_tracker.wim"],
        "radar_radius_km": 250,
        "knmi_api_key": "NEW_SECRET_MUST_NOT_ENTER",
        "raw_retention_opt_in": True,
        "warning_notifications_enabled": True,
        "notify_service": "notify.mobile_app_injected",
    })
    assert injected == {
        "persons": ["device_tracker.wim"],
        "radar_radius_km": 250,
    }
    legacy = namespace["_legacy_input"]({
        **injected,
        "knmi_api_key": "EXPLICIT_LEGACY_SECRET",
        "analysis_mode": "full_send",
        "warning_notifications_enabled": True,
        "notify_service": "notify.mobile_app_testtelefoon",
        "unowned": "DROP_ME",
    })
    assert legacy == {
        **injected,
        "knmi_api_key": "EXPLICIT_LEGACY_SECRET",
        "analysis_mode": "full_send",
        "warning_notifications_enabled": True,
        "notify_service": "notify.mobile_app_testtelefoon",
    }

    class Entry:
        data = {
            "runtime_mode": "worker_only",
            "knmi_api_key": "DATA_SECRET_STAYS_IN_DATA",
        }
        options = {
            "knmi_api_key": "OPTION_SECRET_UNCHANGED",
            "analysis_mode": "thermal_throttle",
            "persons": ["device_tracker.old"],
            "warning_notifications_enabled": True,
            "notify_service": "notify.mobile_app_hidden",
        }

    assert namespace["_entry_mode"](Entry) == "worker_only"
    saved = namespace["_preserve_hidden_options"](
        Entry,
        {"persons": ["device_tracker.new"]},
    )
    assert saved == {
        "knmi_api_key": "OPTION_SECRET_UNCHANGED",
        "analysis_mode": "thermal_throttle",
        "warning_notifications_enabled": True,
        "notify_service": "notify.mobile_app_hidden",
        "persons": ["device_tracker.new"],
    }
    assert Entry.data["knmi_api_key"] == "DATA_SECRET_STAYS_IN_DATA"

    class TrackingDict(dict):
        def __init__(self, values):
            super().__init__(values)
            self.touched = []

        def __contains__(self, key):
            self.touched.append(("contains", key))
            return super().__contains__(key)

        def __getitem__(self, key):
            self.touched.append(("getitem", key))
            return super().__getitem__(key)

    tracked_data = TrackingDict({
        "persons": ["device_tracker.data"],
        "knmi_api_key": "UNREAD_DATA_SECRET",
    })
    tracked_options = TrackingDict({
        "radar_radius_km": 300,
        "netatmo_client_secret": "UNREAD_OPTION_SECRET",
        "warning_notifications_enabled": True,
        "notify_service": "notify.mobile_app_unread",
    })
    tracked_entry = type("TrackedEntry", (), {
        "data": tracked_data,
        "options": tracked_options,
    })()
    projected = namespace["_worker_only_entry_values"](tracked_entry)
    assert projected == {
        "persons": ["device_tracker.data"],
        "radar_radius_km": 300,
    }
    touched_keys = {
        key for _operation, key in tracked_data.touched + tracked_options.touched
    }
    assert "knmi_api_key" not in touched_keys
    assert "netatmo_client_secret" not in touched_keys
    assert "warning_notifications_enabled" not in touched_keys
    assert "notify_service" not in touched_keys


def test_legacy_options_schema_remains_complete_and_separate():
    source = (COMPONENT / "config_flow.py").read_text(encoding="utf-8")
    legacy_schema_source = source.split("def _schema", 1)[1].split(
        "def _worker_only_input", 1
    )[0]
    for required in (
        "CONF_EUMETSAT_KEY",
        "CONF_EUMETSAT_SECRET",
        "CONF_METEOFRANCE_TOKEN",
        "CONF_METEOFRANCE_APPLICATION_ID",
        "CONF_KNMI_API_KEY",
        "CONF_KNMI_WMS_API_KEY",
        "CONF_HSAF_USERNAME",
        "CONF_HSAF_PASSWORD",
        "CONF_NETATMO_CLIENT_ID",
        "CONF_NETATMO_CLIENT_SECRET",
        "CONF_NETATMO_REFRESH_TOKEN",
        "CONF_ANALYSIS_MODE",
        "CONF_WARNING_NOTIFICATIONS_ENABLED",
        "CONF_NOTIFY_SERVICE",
    ):
        assert required in legacy_schema_source
    assert (
        "defaults.get(CONF_WARNING_NOTIFICATIONS_ENABLED,False)"
        in "".join(legacy_schema_source.split())
    )
    options_flow_source = source.split("class StormTrackerOptionsFlow", 1)[1]
    assert "async def async_step_legacy" in options_flow_source
    assert "return await self.async_step_legacy(user_input)" in options_flow_source
    assert 'step_id="legacy"' in options_flow_source
    assert "data_schema=_schema(defaults)" in options_flow_source


def test_reconfigure_replaces_mode_payload_without_secret_projection():
    source = (COMPONENT / "config_flow.py").read_text(encoding="utf-8")
    flow = source.split("class StormTrackerConfigFlow", 1)[1].split(
        "class StormTrackerOptionsFlow", 1
    )[0]
    assert "async def async_step_reconfigure" in flow
    assert "async def async_step_reconfigure_worker_only" in flow
    assert "async def async_step_reconfigure_legacy" in flow
    assert flow.count("return self.async_update_and_abort(") == 2
    assert flow.count("options={}") == 2
    worker_step = flow.split(
        "async def async_step_reconfigure_worker_only", 1
    )[1].split("async def async_step_reconfigure_legacy", 1)[0]
    legacy_step = flow.split(
        "async def async_step_reconfigure_legacy", 1
    )[1]
    assert "_worker_only_input(dict(user_input))" in worker_step
    assert "_legacy_input(dict(user_input))" not in worker_step
    assert "_legacy_input(dict(user_input))" in legacy_step
    assert "_worker_only_input(dict(user_input))" not in legacy_step


def test_worker_and_legacy_netatmo_radius_defaults_and_ranges_do_not_drift():
    source = (COMPONENT / "config_flow.py").read_text(encoding="utf-8")
    worker = source.split("def _worker_only_schema", 1)[1].split(
        "def _schema", 1
    )[0]
    legacy = source.split("def _schema", 1)[1].split(
        "def _worker_only_input", 1
    )[0]

    assert "defaults.get(CONF_NETATMO_RADIUS, 20.0)" in worker
    assert "min=10, max=20, step=5" in worker
    assert "defaults.get(CONF_NETATMO_RADIUS, 175.0)" in legacy
    assert "min=25, max=500, step=25" in legacy


def test_runtime_is_config_entry_only_and_yaml_cannot_call_legacy():
    tree = ast.parse((COMPONENT / "__init__.py").read_text(encoding="utf-8"))
    functions = {
        node.name for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    assert {"async_setup", "async_setup_entry", "_async_setup_runtime"} <= functions
    source = (COMPONENT / "__init__.py").read_text(encoding="utf-8")
    yaml_schema = source.split("CONFIG_SCHEMA =", 1)[1].split(
        "async def async_setup", 1
    )[0]
    assert "config_entry_only_config_schema(DOMAIN)" in yaml_schema
    setup = next(
        node for node in tree.body
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "async_setup"
    )
    assert "_async_setup_runtime" not in ast.unparse(setup)


def test_config_entry_delegates_only_to_an_exact_explicit_runtime():
    source = (COMPONENT / "__init__.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    setup = next(
        node for node in tree.body
        if isinstance(node, ast.AsyncFunctionDef)
        and node.name == "async_setup_entry"
    )
    rendered = ast.unparse(setup)
    assert "async_setup_worker_only_entry" in rendered
    assert "_async_setup_runtime" not in rendered
    assert "_async_setup_legacy_entry" in rendered
    assert "_explicit_entry_mode" in rendered
    assert "RUNTIME_MODE_LEGACY" in rendered
    assert "RUNTIME_MODE_WORKER_ONLY" in rendered
    for forbidden in ("netatmo", "knmi", "provider", "poll", "Store"):
        assert forbidden not in rendered


def test_legacy_credentials_are_guarded_by_exact_legacy_mode():
    source = (COMPONENT / "config_flow.py").read_text(encoding="utf-8")
    options_flow = source.split("class StormTrackerOptionsFlow", 1)[1]
    legacy_step = options_flow.split(
        "async def async_step_legacy", 1
    )[1]
    assert "if mode == RUNTIME_MODE_LEGACY" in options_flow
    assert "if mode != RUNTIME_MODE_WORKER_ONLY" in options_flow
    assert "invalid_runtime_mode" in options_flow
    assert (
        "_entry_mode(self.config_entry) != RUNTIME_MODE_LEGACY"
        in legacy_step
    )
    assert "legacy_mode_required" in legacy_step


def test_manifest_declares_normal_ha_legacy_runtime_dependencies_exactly():
    manifest = json.loads((COMPONENT / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["requirements"] == [
        "h5py>=3.11,<4",
        "numpy>=2.0,<3",
        "pyproj>=3.6,<4",
    ]
    assert manifest["dependencies"] == ["mqtt"]
    assert manifest["iot_class"] == "local_push"


def test_netatmo_runtime_is_bounded_to_local_verification_radius():
    source = (COMPONENT / "__init__.py").read_text(encoding="utf-8")
    assert 'conf.get("netatmo_radius_km", 20.0)' in source
    assert "), 20.0" in source
