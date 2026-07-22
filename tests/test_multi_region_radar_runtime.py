"""Contracttests voor echte radarverwerking per RegionEngine."""
from __future__ import annotations

import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
INIT_SOURCE = (
    ROOT / "custom_components" / "storm_tracker_v3" / "__init__.py"
).read_text(encoding="utf-8")


def test_runtime_keeps_provider_maps_per_engine():
    assert 'hass.data[DOMAIN]["opera_providers_by_engine"] = {}' in INIT_SOURCE
    assert 'hass.data[DOMAIN]["rainviewer_providers_by_engine"] = {}' in INIT_SOURCE
    assert "def _sync_region_radar_providers()" in INIT_SOURCE


def test_each_engine_gets_its_own_geographic_provider():
    assert "OperaProvider(\n                    region.center_lat," in INIT_SOURCE
    assert "RainViewerProvider(\n                    region.center_lat, region.center_lon" in INIT_SOURCE


def test_engine_decisions_use_engine_specific_health():
    assert "states = _radar_source_states(now_ts, region)" in INIT_SOURCE
    assert '.get(region.engine_id)' in INIT_SOURCE


def test_radar_poll_fetches_all_active_engine_providers():
    assert "for engine_id, provider in providers.items():" in INIT_SOURCE
    assert 'hass.data[DOMAIN]["rainviewer_diagnostics_by_engine"]' in INIT_SOURCE
    assert '"provider_count": len(providers)' in INIT_SOURCE


def test_opera_is_validated_and_counted_per_engine_before_selection():
    assert "verification_by_engine = {" in INIT_SOURCE
    assert "accepted_by_engine = {" in INIT_SOURCE
    assert 'hass.data[DOMAIN]["opera_observations_by_engine"]' in INIT_SOURCE
    assert 'hass.data[DOMAIN]["opera_observation_counts_by_engine"]' in INIT_SOURCE


def test_overlay_refresh_never_calls_itself():
    tree = ast.parse(INIT_SOURCE)
    functions = [
        node for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == "_refresh_radar_overlays"
    ]
    assert len(functions) == 1
    recursive_calls = [
        node for node in ast.walk(functions[0])
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "_refresh_radar_overlays"
    ]
    assert recursive_calls == []


def test_moved_target_starts_radar_cycle_immediately():
    tree = ast.parse(INIT_SOURCE)
    functions = [
        node for node in ast.walk(tree)
        if isinstance(node, ast.AsyncFunctionDef)
        and node.name == "_update_secondary_target"
    ]
    assert len(functions) == 1
    rendered = ast.unparse(functions[0])
    assert "_sync_region_radar_providers()" in rendered
    assert "hass.async_create_task(_poll_radar())" in rendered


def test_radar_cycle_binds_shared_calibration_observer_before_fallbacks():
    inner = INIT_SOURCE.split("async def _poll_radar_inner", 1)[1]
    before_hsaf = inner.split("if hsaf is not None and hsaf_regions", 1)[0]
    assert (
        'calibration_observer = hass.data[DOMAIN]["radar_calibration_observer"]'
        in before_hsaf
    )
