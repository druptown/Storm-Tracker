"""Contracttests voor strikt regionale Netatmo-verwerking."""
from __future__ import annotations

import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
INIT_SOURCE = (
    ROOT / "custom_components" / "storm_tracker_v3" / "__init__.py"
).read_text(encoding="utf-8")
SENSOR_SOURCE = (
    ROOT / "custom_components" / "storm_tracker_v3" / "sensor.py"
).read_text(encoding="utf-8")


def test_runtime_keeps_netatmo_state_per_engine():
    assert 'hass.data[DOMAIN]["netatmo_providers_by_engine"] = {}' in INIT_SOURCE
    assert 'hass.data[DOMAIN]["netatmo_observations_by_engine"] = {}' in INIT_SOURCE
    assert 'hass.data[DOMAIN]["netatmo_pressure_trends_by_engine"] = {}' in INIT_SOURCE
    assert "def _sync_region_netatmo_providers()" in INIT_SOURCE


def test_each_engine_gets_its_own_netatmo_geographic_provider():
    assert "NetatmoProvider(\n                    token,\n                    region.center_lat," in INIT_SOURCE
    assert "region.center_lon,\n                    netatmo_radius," in INIT_SOURCE


def test_netatmo_poll_fetches_and_tracks_every_engine_separately():
    tree = ast.parse(INIT_SOURCE)
    poll = next(
        node for node in ast.walk(tree)
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "_poll_netatmo"
    )
    rendered = ast.unparse(poll)
    assert "asyncio.gather" in rendered
    assert "observations_by_engine[engine_id] = obs" in rendered
    assert "trends_by_engine[engine_id] = tracker.update(obs)" in rendered
    assert "storm_manager.get_engine_for_target('zone.home')" in rendered


def test_target_status_uses_its_region_pressure_instead_of_global_pressure():
    assert 'get("netatmo_pressure_trends_by_engine", {}).get(engine_id)' in SENSOR_SOURCE
    assert "region.engine_id" in SENSOR_SOURCE
    target_block = SENSOR_SOURCE.split("class TargetPrecipitationStatusSensor", 1)[1]
    summary_block = target_block.split("def _summary", 1)[1].split(
        "@property\n    def native_value", 1
    )[0]
    assert "netatmo_pressure_trends_by_engine" in summary_block
    assert 'get("netatmo_pressure_trend")' not in summary_block


def test_legacy_pressure_sensor_is_explicitly_home_scoped():
    poll_block = INIT_SOURCE.split("async def _poll_netatmo", 1)[1].split(
        "async def _poll_open_meteo", 1
    )[0]
    assert 'get_engine_for_target("zone.home")' in poll_block
    assert 'hass.data[DOMAIN]["netatmo_pressure_trend"]' in poll_block


def test_pressure_store_remains_backward_compatible_without_ha_migration():
    assert 'Store(hass, 1, f"{DOMAIN}_pressure_trend")' in INIT_SOURCE
    assert 'restored_pressure_snapshot.get("engines", {})' in INIT_SOURCE
    assert '"stations" in restored_pressure_snapshot' in INIT_SOURCE
