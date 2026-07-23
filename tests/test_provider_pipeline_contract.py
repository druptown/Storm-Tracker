"""Dwarsdoorsnede-contracten voor de volledige providerketen."""
from __future__ import annotations

import ast
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
COMPONENT = ROOT / "custom_components" / "storm_tracker_v3"
INIT_SOURCE = (COMPONENT / "__init__.py").read_text(encoding="utf-8")
POLICY = json.loads((COMPONENT / "provider_policy.json").read_text(encoding="utf-8"))


def _function_source(name: str) -> str:
    tree = ast.parse(INIT_SOURCE)
    node = next(
        item for item in ast.walk(tree)
        if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef))
        and item.name == name
    )
    return ast.unparse(node)


def test_only_one_ordered_five_minute_provider_cycle_is_scheduled():
    runtime = _function_source("_async_setup_runtime")
    assert "async_track_time_interval(hass, _poll_all, timedelta(minutes=5))" in runtime
    for old_callback in (
        "_poll_radar",
        "_poll_radar_comparison",
        "_poll_netatmo",
        "_poll_open_meteo",
        "_poll_national_providers",
    ):
        assert (
            f"async_track_time_interval(hass, {old_callback}, timedelta(minutes="
            not in runtime
        )
    cycle = _function_source("_poll_all")
    assert cycle.index("_poll_national_providers()") < cycle.index(
        "_poll_radar_comparison()"
    ) < cycle.index("_poll_radar()")
    assert "provider_cycle_lock" in cycle
    assert cycle.count("_bounded_provider_stage") == 6
    assert cycle.index("_poll_radar()") < cycle.index(
        "_flush_calibration_data()"
    )
    assert cycle.index("_poll_radar()") < cycle.index(
        "_flush_region_observation_batches()"
    ) < cycle.index("_queue_target_verification_samples()")
    bounded = _function_source("_bounded_provider_stage")
    assert "asyncio.wait_for" in bounded
    assert "provider_stage_timeouts" in bounded


def test_moved_target_runs_the_same_complete_cycle():
    update = _function_source("_update_secondary_target")
    assert "_sync_region_radar_providers()" in update
    assert "_sync_region_netatmo_providers()" in update
    assert "hass.async_create_task(_poll_all())" in update


def test_every_available_target_gets_its_own_verification_snapshot():
    queue = _function_source("_queue_target_verification_samples")
    assert "for target_id, target in target_runtime.items()" in queue
    assert "storm_manager.get_engine_for_target(entity_id)" in queue
    assert "'sample_type': 'target_cycle'" in queue
    assert "'latitude': float(lat)" in queue
    assert "'longitude': float(lon)" in queue
    assert "'radar_source_decision': dict(source_decision)" in queue
    assert "'scope': 'home_only'" in queue


def test_location_scoped_global_sources_are_region_aware():
    radar_sync = _function_source("_sync_region_radar_providers")
    assert "for region in regions.values()" in radar_sync
    assert "KmiProviderFactory.supports" in radar_sync
    assert "KnmiProviderFactory.supports" in radar_sync
    open_meteo_targets = _function_source("_open_meteo_targets")
    assert "target.get('available')" in open_meteo_targets
    assert "target.get('latitude')" in open_meteo_targets
    open_meteo_poll = _function_source("_poll_open_meteo")
    assert "open_meteo_provider.fetch(targets)" in open_meteo_poll
    assert "open_meteo_results_by_target" in open_meteo_poll
    assert "route_observation" not in open_meteo_poll
    assert "open_meteo_forecast" in open_meteo_poll
    assert "open_meteo_enabled" in open_meteo_poll


def test_knmi_health_uses_frame_time_even_when_frame_is_dry():
    states = _function_source("_radar_source_states")
    assert "getattr(kmi, 'last_frame_timestamp', None)" in states
    assert "getattr(knmi, 'last_frame_timestamp', None)" in states
    knmi_source = (COMPONENT / "providers" / "knmi.py").read_text(encoding="utf-8")
    assert "self.last_frame_timestamp = ts" in knmi_source
    assert "self.last_fetch_success = True" in knmi_source


def test_validation_only_sources_are_never_declared_as_primary_radar():
    engine_policy = (
        COMPONENT / "providers" / "engine_radar_policy.py"
    ).read_text(encoding="utf-8")
    local_mapping = engine_policy.split("LOCAL_RADAR_BY_COUNTRY =", 1)[1].split(
        "COUNTRY_CODE_ALIASES", 1
    )[0]
    assert "meteolux" not in local_mapping
    assert "italiameteo" not in local_mapping
    assert "geosphere_at" not in local_mapping
    assert "meteolux" in POLICY["countries"]["LU"]["model_guidance"]
    assert "geosphere_at" in POLICY["countries"]["AT"]["model_guidance"]
    assert "italiameteo" in POLICY["countries"]["IT"]["model_guidance"]


def test_every_operational_local_radar_has_state_routing_and_overlay_paths():
    states = _function_source("_radar_source_states")
    assert "provider_lifecycle.overlay(provider_id)" in states
    assert "product_timestamp" in states
    national_poll = _function_source("_poll_national_providers")
    overlay = _function_source("_refresh_radar_overlays")
    for source in (
        "kmi",
        "knmi",
        "dwd_radolan",
        "meteofrance_radar",
        "met_office_radar",
        "dpc_radar",
        "aemet_radar",
    ):
        assert source in states
        assert source in (national_poll + INIT_SOURCE)
        assert source in (overlay + INIT_SOURCE)


def test_lightning_fallbacks_remain_separate_from_radar_routing():
    eumetsat = _function_source("_poll_eumetsat_li")
    goes = _function_source("_poll_goes_glm")
    for poller in (eumetsat, goes):
        assert "route_observation(observation)" in poller
        assert "_record_lightning(observation)" in poller
        assert "_route_selected_radar" not in poller


def test_regional_radar_fetches_are_parallel_and_individually_bounded():
    rainviewer = _function_source("_poll_rv")
    opera = _function_source("_poll_opera")
    for poller in (rainviewer, opera):
        assert "asyncio.gather" in poller
        assert "asyncio.wait_for" in poller
        assert "return_exceptions=True" in poller


def test_source_switch_uses_persistent_directional_transition_profile():
    decisions = _function_source("_refresh_engine_radar_decisions")
    assert "radar_source_transitions" in decisions
    assert "active_until" in decisions
    assert "select_transition_profile" in decisions
    assert "transition_adjustment" in decisions
    assert "transition_window_seconds" in decisions
    sensor_source = (
        COMPONENT / "sensor.py"
    ).read_text(encoding="utf-8")
    assert 'result["source_transition_active"] = True' in sensor_source
    assert 'transition.get("confidence_penalty_percent", 10)' in sensor_source
    assert "max(0, confidence - penalty)" in sensor_source
