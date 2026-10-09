"""Ordinary Home Assistant integration-only release and recovery proofs."""
from __future__ import annotations

import ast
import hashlib
import json
from pathlib import Path

import pytest

import ordinary_ha_release_dry_run as ordinary
from storm_tracker_worker.release_artifacts import build_release_candidate


ROOT = Path(__file__).resolve().parents[1]


def _integration_sources(
    *,
    schema_version: int = 3,
    legacy_setup: bool = True,
    worker_only_setup: bool = True,
    legacy_requirements: bool = True,
    local_processing: bool = True,
    warning_opt_in: bool = True,
    mqtt_offline: bool = True,
    unknown_projection: str = "onvoldoende_data",
) -> dict[str, str]:
    legacy_setup_call = (
        "return await _async_setup_legacy_entry(hass, entry)"
        if legacy_setup
        else "return await _async_setup_runtime(hass, entry.data, {})"
    )
    worker_only_setup_block = (
        "    if mode == RUNTIME_MODE_WORKER_ONLY:\n"
        "        from .worker_only_entry import async_setup_worker_only_entry\n"
        "        return await async_setup_worker_only_entry(hass, entry)\n"
        "    return False\n"
        if worker_only_setup
        else "    return await _async_setup_runtime(hass, entry.data, {})\n"
    )
    local_processing_literal = "True" if local_processing else "False"
    warning_enabled_expression = (
        "conf.get(CONF_WARNING_NOTIFICATIONS_ENABLED) is True"
        if warning_opt_in
        else "bool(conf.get(CONF_WARNING_NOTIFICATIONS_ENABLED))"
    )
    offline_body = (
        "data['worker_status']['state'] = 'offline'\n"
        "        _mark_operational_snapshot_worker_offline(hass, data)\n"
        "        return"
        if mqtt_offline
        else "return"
    )
    return {
        "manifest.json": json.dumps({
            "domain": "storm_tracker_v3",
            "name": "Storm Tracker V3",
            "version": "9.8.7",
            "requirements": (
                ["h5py>=3.11,<4", "numpy>=2.0,<3", "pyproj>=3.6,<4"]
                if legacy_requirements else []
            ),
            "dependencies": ["mqtt"],
            "config_flow": True,
            "iot_class": "local_push",
        }),
        "__init__.py": (
            'STORE = Factory(build_id="integration-9.8.7-worker-6.5.4")\n'
            f"CONFIG_ENTRY_VERSION = {schema_version}\n"
            'RUNTIME_MODE_LEGACY = "legacy"\n'
            'RUNTIME_MODE_WORKER_ONLY = "worker_only"\n'
            'CONF_NOTIFY_SERVICE = "notify_service"\n'
            "CONF_WARNING_NOTIFICATIONS_ENABLED = "
            "'warning_notifications_enabled'\n"
            "def _explicit_entry_mode(entry):\n"
            "    mode = entry.data.get('runtime_mode')\n"
            "    if mode not in (RUNTIME_MODE_LEGACY, "
            "RUNTIME_MODE_WORKER_ONLY):\n"
            "        return None\n"
            "    return mode\n"
            "async def _async_setup_legacy_entry(hass, entry):\n"
            "    return await _async_setup_runtime(hass, entry.data, {})\n"
            "async def async_setup_entry(hass, entry):\n"
            "    if entry.version != CONFIG_ENTRY_VERSION:\n"
            "        return False\n"
            "    mode = _explicit_entry_mode(entry)\n"
            "    if mode is None:\n"
            "        return False\n"
            "    if mode == RUNTIME_MODE_LEGACY:\n"
            f"        {legacy_setup_call}\n"
            f"{worker_only_setup_block}"
            "async def async_migrate_entry(hass, entry):\n"
            "    valid_modes = (RUNTIME_MODE_LEGACY, "
            "RUNTIME_MODE_WORKER_ONLY)\n"
            "    selected_mode = entry.data.get('runtime_mode') or "
            "RUNTIME_MODE_WORKER_ONLY\n"
            "    if selected_mode not in valid_modes:\n"
            "        return False\n"
            "    hass.config_entries.async_update_entry(\n"
            "        entry, data={'runtime_mode': selected_mode},\n"
            "        version=CONFIG_ENTRY_VERSION,\n"
            "    )\n"
            "    return True\n"
            "async def _async_setup_runtime(hass, conf, config):\n"
            f"    enabled = {warning_enabled_expression}\n"
            "    warning_dispatcher = WarningDispatcher(\n"
            "        hass, enabled=enabled,\n"
            "        notify_service=conf.get(CONF_NOTIFY_SERVICE), store=STORE,\n"
            "    )\n"
            "    await warning_dispatcher.async_initialize()\n"
            "    storm_manager = StormManager(\n"
            "        allow_local_processing="
            f"{local_processing_literal}\n"
            "    )\n"
            "    return bool(storm_manager)\n"
        ),
        "config_flow.py": (
            "from .const import (CONF_NOTIFY_SERVICE, "
            "CONF_WARNING_NOTIFICATIONS_ENABLED, RUNTIME_MODE_LEGACY, "
            "RUNTIME_MODE_WORKER_ONLY)\n"
            "WORKER_ONLY_SCHEMA_FIELDS = frozenset({'persons'})\n"
            "LEGACY_SCHEMA_FIELDS = frozenset({\n"
            "    *WORKER_ONLY_SCHEMA_FIELDS, CONF_NOTIFY_SERVICE,\n"
            "    CONF_WARNING_NOTIFICATIONS_ENABLED,\n"
            "})\n"
            "RUNTIME_MODE_OPTIONS = (RUNTIME_MODE_WORKER_ONLY, "
            "RUNTIME_MODE_LEGACY)\n"
            "def _schema(defaults):\n"
            "    return (\n"
            "        defaults.get(CONF_WARNING_NOTIFICATIONS_ENABLED, False),\n"
            "        defaults.get(CONF_NOTIFY_SERVICE, ''),\n"
            "    )\n"
            "class StormTrackerConfigFlow:\n"
            f"    VERSION = {schema_version}\n"
            "    async def async_step_user(self, user_input=None):\n"
            "        return RUNTIME_MODE_OPTIONS\n"
            "    async def async_step_reconfigure(self, user_input=None):\n"
            "        return RUNTIME_MODE_OPTIONS\n"
        ),
        "const.py": (
            'RUNTIME_MODE_LEGACY = "legacy"\n'
            'RUNTIME_MODE_WORKER_ONLY = "worker_only"\n'
            'CONF_NOTIFY_SERVICE = "notify_service"\n'
            "CONF_WARNING_NOTIFICATIONS_ENABLED = "
            "'warning_notifications_enabled'\n"
            "WARNING_SENT_EVENT = 'storm_tracker_v3_warning_sent'\n"
        ),
        "engine/region_manager.py": (
            "class StormManager:\n"
            "    def __init__(self, *, allow_local_processing=False):\n"
            "        self._allow_local_processing = allow_local_processing\n"
            "    def _create_engine(self, storm_engine):\n"
            "        async def on_batch(observations):\n"
            "            if not self._allow_local_processing:\n"
            "                return False\n"
            "            await storm_engine.process_batch(observations)\n"
            "            return True\n"
            "        return on_batch\n"
        ),
        "worker_bridge.py": (
            "async def async_setup_worker_bridge(hass):\n"
            "    data = hass.data['storm_tracker_v3']\n"
            "    if not await mqtt.async_wait_for_mqtt_client(hass):\n"
            f"        {offline_body}\n"
        ),
        "operational_snapshot_views.py": (
            f"_CURRENT_STATE = {{'unknown': '{unknown_projection}', "
            "'dry': 'droog', 'raining': 'regen'}\n"
        ),
        "operational_snapshot_sensors.py": (
            "class SnapshotOnlySensorEntity:\n"
            "    def _fail_closed(self, projected):\n"
            "        if not projected:\n"
            "            return 'onvoldoende_data'\n"
            "        return projected\n"
        ),
        "sensor.py": "def _legacy_sensor_entities():\n    return ()\n",
        "warning_dispatcher.py": (
            "from .const import WARNING_SENT_EVENT\n"
            "class WarningDispatcher:\n"
            "    async def _async_save(self):\n"
            "        return None\n"
            "    async def async_initialize(self):\n"
            "        return None\n"
            "    def _notification_payload(self):\n"
            "        return {'official_warning': True}\n"
            "    async def _async_dispatch(self):\n"
            "        payload = self._notification_payload()\n"
            "        await self.hass.services.async_call(\n"
            "            'notify', 'test', payload\n"
            "        )\n"
            "        await self._async_save()\n"
            "        self.hass.bus.async_fire(\n"
            "            WARNING_SENT_EVENT, {'official_warning': False}\n"
            "        )\n"
            "    async def async_process(self):\n"
            "        await self._async_dispatch()\n"
        ),
        "worker_only_entry.py": "ENTRY_CONTRACT = 'worker_only'\n",
        "worker_only_runtime.py": "RUNTIME_CONTRACT = 'worker_only'\n",
    }


def _build(
    tmp_path: Path,
    *,
    schema_version: int = 3,
    legacy_setup: bool = True,
    worker_only_setup: bool = True,
    legacy_requirements: bool = True,
    local_processing: bool = True,
    warning_opt_in: bool = True,
    mqtt_offline: bool = True,
    unknown_projection: str = "onvoldoende_data",
):
    integration = tmp_path / "source" / "storm_tracker_v3"
    worker = tmp_path / "worker-source" / "storm_tracker_worker"
    integration.mkdir(parents=True)
    worker.mkdir(parents=True)
    for relative, content in _integration_sources(
        schema_version=schema_version,
        legacy_setup=legacy_setup,
        worker_only_setup=worker_only_setup,
        legacy_requirements=legacy_requirements,
        local_processing=local_processing,
        warning_opt_in=warning_opt_in,
        mqtt_offline=mqtt_offline,
        unknown_projection=unknown_projection,
    ).items():
        path = integration / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
    (worker / "config.yaml").write_text(
        'name: "Storm Tracker Worker"\nversion: "6.5.4"\n',
        encoding="utf-8",
    )
    (worker / "__init__.py").write_text(
        '__version__ = "6.5.4"\n', encoding="utf-8"
    )
    (worker / "run.sh").write_text(
        "#!/bin/sh\nexec python -m service\n", encoding="utf-8"
    )
    (worker / "service.py").write_text(
        "def main():\n    return 0\n", encoding="utf-8"
    )
    output = tmp_path / "candidate"
    output.mkdir()
    return build_release_candidate(
        integration_root=integration,
        worker_root=worker,
        integration_version="9.8.7",
        worker_version="6.5.4",
        output_directory=output,
    )


def _execute(tmp_path: Path, **kwargs):
    built = _build(tmp_path, **kwargs)
    result = ordinary.execute_ordinary_ha_integration_only_dry_run(
        candidate_evidence=built.evidence_path,
        run_root=tmp_path / "ordinary-ha-run",
    )
    return built, result


def _valid_target_layout() -> dict[str, object]:
    deploy_run = "/homeassistant/storm_tracker_deploy/test-run"
    active = "/homeassistant/custom_components/storm_tracker_v3"
    return {
        "custom_components_directory": "/homeassistant/custom_components",
        "active_component_directory": active,
        "deploy_run_directory": deploy_run,
        "non_active_component_directories": {
            "predeploy_snapshot": (
                f"{deploy_run}/predeploy-snapshot/storm_tracker_v3"
            ),
            "quarantine_failed_candidate": (
                f"{deploy_run}/quarantine/storm_tracker_v3.candidate_failed"
            ),
            "staging_initial": (
                f"{deploy_run}/staging-initial/storm_tracker_v3"
            ),
            "staging_same_version_recovery": (
                f"{deploy_run}/staging-recovery/storm_tracker_v3"
            ),
        },
        "storm_tracker_manifest_paths_below_custom_components": (
            f"{active}/manifest.json",
        ),
    }


def test_pair_evidence_drives_integration_only_install_and_same_version_recovery(
    tmp_path: Path,
):
    built, result = _execute(tmp_path)
    report = result.report

    assert report["contract"] == ordinary.ORDINARY_HA_DRY_RUN_CONTRACT
    assert report["deployment_mode"] == ordinary.ORDINARY_HA_DEPLOY_MODE
    assert report["deployment_performed"] is False
    assert report["network_performed"] is False
    assert report["live_paths_touched"] is False
    assert report["live_installation_ready"] is False
    assert report["integration_installation_only"] is True
    assert report["pair_release_evidence"]["pair_fully_verified"] is True
    assert report["pair_release_evidence"]["integration"] == {
        "bytes": built.integration_archive.stat().st_size,
        "filename": built.integration_archive.name,
        "sha256": built.integration_sha256,
        "version": "9.8.7",
    }
    worker = report["pair_release_evidence"]["worker"]
    assert worker["filename"] == built.worker_archive.name
    assert worker["sha256"] == built.worker_sha256
    assert worker["installed"] is False
    assert worker["read_only_pair_verification_only"] is True

    simulation = report["local_simulation"]
    assert simulation["local_simulation_passed"] is True
    assert simulation["canonical_non_overlapping_layout_verified"] is True
    assert simulation[
        "exact_one_storm_tracker_manifest_below_custom_components_verified"
    ] is True
    assert simulation[
        "support_component_directories_outside_custom_components"
    ] is True
    assert simulation["candidate_installed"] is True
    assert simulation["candidate_quarantined"] is True
    assert simulation["same_version_recovered"] is True
    assert simulation["config_entry_state_during_install"] == "disabled"
    assert simulation["config_entry_state_during_recovery"] == "disabled"
    assert simulation["worker_archive_copied"] is False
    assert simulation["worker_archive_extracted"] is False
    assert simulation["worker_archive_installed"] is False

    target_contract = report["target_filesystem_contract"]
    assert target_contract["reference_layout_only"] is True
    assert target_contract["live_observed"] is False
    assert target_contract["required_before_every_home_assistant_restart"] is True

    run_root = result.report_path.parent
    simulated_home_assistant = run_root / "simulated-target" / "homeassistant"
    custom_components = simulated_home_assistant / "custom_components"
    deploy_run = (
        simulated_home_assistant
        / "storm_tracker_deploy"
        / "ordinary-ha-unique-run-id"
    )
    active_manifest = custom_components / "storm_tracker_v3" / "manifest.json"
    assert active_manifest.is_file()
    assert tuple(custom_components.rglob("manifest.json")) == (active_manifest,)
    assert (
        deploy_run
        / "quarantine"
        / "storm_tracker_v3.candidate_failed"
        / "manifest.json"
    ).is_file()
    assert (
        deploy_run
        / "predeploy-snapshot"
        / "storm_tracker_v3"
        / "manifest.json"
    ).is_file()
    assert not (custom_components / ".storm_tracker_v3_predeploy").exists()
    assert not tuple(run_root.rglob("*.tar.gz"))
    assert not tuple(run_root.rglob("storm_tracker_worker"))


def test_static_contract_proves_schema_v3_worker_only_and_offline_truth(
    tmp_path: Path,
):
    _built, result = _execute(tmp_path)
    contract = result.report["static_runtime_contract"]
    assert contract == {
        "config_entry_schema_version": 3,
        "legacy_local_processing": True,
        "manifest_dependencies": ["mqtt"],
        "manifest_requirements": [
            "h5py>=3.11,<4",
            "numpy>=2.0,<3",
            "pyproj>=3.6,<4",
        ],
        "mqtt_absent_weather_state": "onvoldoende_data",
        "mqtt_absent_worker_state": "offline",
        "runtime_mode": "legacy",
        "supported_runtime_modes": ["legacy", "worker_only"],
        "static_contract_verified": True,
        "warning_dispatch": "explicit_opt_in_default_off",
        "worker_archive_installed": False,
        "worker_only_default_retained": True,
    }


def test_predeploy_backup_snapshot_disable_and_live_gates_remain_fail_closed(
    tmp_path: Path,
):
    _built, result = _execute(tmp_path)
    predeploy = result.report["predeploy_requirements"]
    assert predeploy == {
        "active_component_path_live_discovery_required": True,
        "active_component_snapshot_is_functional_rollback": False,
        "active_component_snapshot_required": True,
        "active_component_snapshot_sha256_required": True,
        "canonical_target_paths_required": True,
        "config_entry_disabled_before_component_replacement_required": True,
        "config_entry_legacy_ui_reconfigure_required": True,
        "deploy_run_must_not_overlap_custom_components": True,
        "exact_one_active_storm_tracker_manifest_before_every_restart_required": True,
        "full_home_assistant_backup_required": True,
        "live_requirements_observed": False,
        "non_active_component_directories_under_deploy_run_required": True,
        "predeploy_staging_quarantine_under_custom_components_forbidden": True,
        "required_runtime_mode": "legacy",
        "transfer_directory": "/homeassistant/storm_tracker_deploy",
        "uploaded_integration_sha256_match_required": True,
        "warning_notifications_default_off_required": True,
        "worker_installation_forbidden_for_this_route": True,
    }
    assert result.report["blocker_namespaces"] == {
        "local": [],
        "live": list(ordinary.LIVE_INSTALLATION_GATES),
    }
    assert set(ordinary.LIVE_INSTALLATION_GATES) >= {
        "full_home_assistant_backup_not_live_observed",
        "active_component_snapshot_and_sha256_not_live_observed",
        "config_entry_disabled_not_live_observed",
        "same_version_recovery_not_live_exercised",
        "canonical_non_overlapping_component_layout_not_live_observed",
        "exact_one_storm_tracker_manifest_below_custom_components_not_live_observed",
    }


def test_target_layout_contract_is_canonical_non_overlapping_and_one_manifest():
    contract = ordinary.verify_ordinary_ha_target_layout_contract(
        **_valid_target_layout()
    )
    assert contract["canonical_paths_verified"] is True
    assert contract[
        "pairwise_non_overlapping_component_directories_verified"
    ] is True
    assert contract[
        "non_active_components_outside_custom_components_verified"
    ] is True
    assert contract[
        "exact_one_storm_tracker_manifest_below_custom_components_verified"
    ] is True
    assert contract["storm_tracker_manifest_paths_below_custom_components"] == [
        "/homeassistant/custom_components/storm_tracker_v3/manifest.json"
    ]


@pytest.mark.parametrize(
    "role",
    (
        "predeploy_snapshot",
        "quarantine_failed_candidate",
        "staging_initial",
        "staging_same_version_recovery",
    ),
)
def test_target_layout_rejects_every_non_active_component_under_custom_components(
    role: str,
):
    arguments = _valid_target_layout()
    non_active = dict(arguments["non_active_component_directories"])
    non_active[role] = (
        f"/homeassistant/custom_components/.{role}/storm_tracker_v3"
    )
    arguments["non_active_component_directories"] = non_active
    with pytest.raises(
        ordinary.OrdinaryHaDryRunError,
        match="mag niet onder custom_components",
    ):
        ordinary.verify_ordinary_ha_target_layout_contract(**arguments)


def test_target_layout_rejects_noncanonical_or_overlapping_component_paths():
    noncanonical = _valid_target_layout()
    noncanonical["deploy_run_directory"] = (
        "/homeassistant/storm_tracker_deploy/../storm_tracker_deploy/test-run"
    )
    with pytest.raises(
        ordinary.OrdinaryHaDryRunError,
        match="geen canoniek POSIX-pad",
    ):
        ordinary.verify_ordinary_ha_target_layout_contract(**noncanonical)

    overlapping = _valid_target_layout()
    non_active = dict(overlapping["non_active_component_directories"])
    non_active["staging_initial"] = non_active["predeploy_snapshot"]
    overlapping["non_active_component_directories"] = non_active
    with pytest.raises(
        ordinary.OrdinaryHaDryRunError,
        match="componentpaden overlappen",
    ):
        ordinary.verify_ordinary_ha_target_layout_contract(**overlapping)


def test_target_layout_rejects_second_manifest_even_in_hidden_sibling():
    arguments = _valid_target_layout()
    arguments["storm_tracker_manifest_paths_below_custom_components"] = (
        "/homeassistant/custom_components/storm_tracker_v3/manifest.json",
        (
            "/homeassistant/custom_components/"
            ".storm_tracker_v3_predeploy/manifest.json"
        ),
    )
    with pytest.raises(
        ordinary.OrdinaryHaDryRunError,
        match="exact één Storm Tracker-manifest",
    ):
        ordinary.verify_ordinary_ha_target_layout_contract(**arguments)


def test_simulated_target_gate_rejects_a_second_manifest_on_disk(tmp_path: Path):
    custom_components = tmp_path / "homeassistant" / "custom_components"
    active = custom_components / "storm_tracker_v3"
    hidden_copy = custom_components / ".storm_tracker_v3_predeploy"
    deploy_run = tmp_path / "homeassistant" / "storm_tracker_deploy" / "run"
    predeploy = deploy_run / "predeploy" / "storm_tracker_v3"
    quarantine = deploy_run / "quarantine" / "storm_tracker_v3.failed"
    for directory in (active, hidden_copy, predeploy, quarantine):
        directory.mkdir(parents=True)
        (directory / "manifest.json").write_text(
            '{"domain":"storm_tracker_v3"}',
            encoding="utf-8",
        )
    with pytest.raises(
        ordinary.OrdinaryHaDryRunError,
        match="niet exact het actieve manifest",
    ):
        ordinary._verify_simulated_target_layout(
            custom_components=custom_components,
            active_component=active,
            deploy_run=deploy_run,
            retained_non_active_components=(predeploy, quarantine),
        )


def test_old_code_is_not_functional_rollback_and_same_version_is_exact(
    tmp_path: Path,
):
    built, result = _execute(tmp_path)
    recovery = result.report["recovery"]
    assert recovery["functional_old_code_rollback_claimed"] is False
    assert recovery["nonfunctional_code_rollback_versions"] == [
        "1.11.x", "1.13.1", "1.13.2"
    ]
    assert recovery["active_component_snapshot_is_forensic_only"] is True
    assert recovery["pre_schema_v3_recovery_requires_full_ha_backup_restore"] is True
    assert recovery["failure_action"] == (
        "keep_config_entry_disabled_and_quarantine_failed_component"
    )
    same = recovery["same_version_recovery"]
    assert same == {
        "archive_filename": built.integration_archive.name,
        "archive_sha256": built.integration_sha256,
        "config_entry_must_remain_disabled": True,
        "integration_version": "9.8.7",
        "live_exercised": False,
        "local_simulated": True,
        "procedure_name": "exact_same_version_integration_recovery",
    }


def test_report_is_content_addressed_and_reproducible_across_run_roots(
    tmp_path: Path,
):
    built = _build(tmp_path)
    first = ordinary.execute_ordinary_ha_integration_only_dry_run(
        candidate_evidence=built.evidence_path,
        run_root=tmp_path / "run-first",
    )
    second = ordinary.execute_ordinary_ha_integration_only_dry_run(
        candidate_evidence=built.evidence_path,
        run_root=tmp_path / "run-second",
    )
    assert first.report == second.report
    assert first.report_path.read_bytes() == second.report_path.read_bytes()
    assert first.report_file_sha256 == hashlib.sha256(
        first.report_path.read_bytes()
    ).hexdigest()
    unsigned = dict(first.report)
    supplied = unsigned.pop("report_sha256")
    assert supplied == hashlib.sha256(ordinary._canonical(unsigned)).hexdigest()


@pytest.mark.parametrize(
    ("changes", "message"),
    (
        ({"schema_version": 2}, "schema-v3"),
        ({"worker_only_setup": False}, "exclusief worker_only"),
        ({"mqtt_offline": False}, "offline geprojecteerd"),
        ({"unknown_projection": "droog"}, "onvoldoende_data"),
    ),
)
def test_static_runtime_contract_fails_closed(
    tmp_path: Path,
    changes: dict[str, object],
    message: str,
):
    built = _build(tmp_path, **changes)
    with pytest.raises(ordinary.OrdinaryHaDryRunError, match=message):
        ordinary.execute_ordinary_ha_integration_only_dry_run(
            candidate_evidence=built.evidence_path,
            run_root=tmp_path / "rejected-run",
        )


def test_tampered_pair_is_rejected_before_output_creation(tmp_path: Path):
    built = _build(tmp_path)
    with built.worker_archive.open("ab") as target:
        target.write(b"tampered")
    run_root = tmp_path / "must-remain-absent"
    with pytest.raises(
        ordinary.OrdinaryHaDryRunError,
        match="pair/RAW-compatibele release-evidence",
    ):
        ordinary.execute_ordinary_ha_integration_only_dry_run(
            candidate_evidence=built.evidence_path,
            run_root=run_root,
        )
    assert not run_root.exists()


def test_existing_run_root_is_never_reused(tmp_path: Path):
    built = _build(tmp_path)
    run_root = tmp_path / "existing"
    run_root.mkdir()
    marker = run_root / "owned.txt"
    marker.write_text("keep", encoding="utf-8")
    with pytest.raises(ordinary.OrdinaryHaDryRunError, match="nieuw en afwezig"):
        ordinary.execute_ordinary_ha_integration_only_dry_run(
            candidate_evidence=built.evidence_path,
            run_root=run_root,
        )
    assert marker.read_text(encoding="utf-8") == "keep"


def test_cli_and_module_have_no_live_deploy_or_network_capability():
    module_path = ROOT / "ordinary_ha_release_dry_run.py"
    script_path = ROOT / "scripts" / "dry_run_ordinary_ha_release_install.py"
    for path in (module_path, script_path):
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source)
        imported_roots = {
            alias.name.split(".", 1)[0]
            for node in ast.walk(tree)
            if isinstance(node, (ast.Import, ast.ImportFrom))
            for alias in node.names
        }
        assert imported_roots.isdisjoint({
            "asyncssh", "paramiko", "requests", "socket", "subprocess"
        })
        called = {
            node.func.attr
            for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
        }
        assert called.isdisjoint({
            "connect", "delete", "post", "put", "rmtree", "unlink"
        })


def test_documented_contract_literals_cannot_drift():
    module_source = (ROOT / "ordinary_ha_release_dry_run.py").read_text(
        encoding="utf-8"
    )
    script_source = (
        ROOT / "scripts" / "dry_run_ordinary_ha_release_install.py"
    ).read_text(encoding="utf-8")
    runbook_source = (
        ROOT / "docs" / "ORDINARY_HA_INTEGRATION_ONLY_DEPLOY.md"
    ).read_text(encoding="utf-8")
    assert "verify_release_candidate_evidence" in module_source
    assert "storm_tracker_worker_v" not in script_source
    assert "integration-only" in script_source
    assert "Er wordt niets gedeployed" in script_source
    assert ".storm_tracker_v3_predeploy_*` is verboden" in runbook_source
    assert "domeingefilterde set" in runbook_source
    assert "vóór iedere herstart exact één Storm Tracker-manifest" in runbook_source
