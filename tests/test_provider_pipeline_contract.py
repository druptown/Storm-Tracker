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
        "_poll_open_meteo",
        "_poll_national_providers",
    ):
        assert (
            f"async_track_time_interval(hass, {old_callback}, timedelta(minutes="
            not in runtime
        )
    assert (
        "async_track_time_interval(hass, _poll_netatmo, "
        "timedelta(minutes=1))"
        in runtime
    )
    cycle = _function_source("_poll_all")
    assert cycle.index("_poll_national_providers()") < cycle.index(
        "_poll_radar_comparison()"
    ) < cycle.index("_poll_radar()")
    assert "provider_cycle_lock" in cycle
    assert cycle.count("_bounded_provider_stage") == 9
    assert "asyncio.create_task(_poll_netatmo())" in cycle
    assert "asyncio.create_task(_poll_open_meteo())" in cycle
    assert "virtual_probe_evaluation" in cycle
    assert cycle.index("virtual_probe_evaluation") > cycle.index("open_meteo")
    assert cycle.index("_poll_radar()") < cycle.index(
        "_flush_calibration_data()"
    )
    assert cycle.index("_poll_radar()") < cycle.index(
        "_flush_region_observation_batches()"
    ) < cycle.index("_queue_target_verification_samples()")
    assert cycle.index("_queue_target_verification_samples()") < cycle.index(
        "warning_dispatcher.async_process"
    ) < cycle.index("_flush_calibration_data()")
    bounded = _function_source("_bounded_provider_stage")
    assert "asyncio.wait_for" in bounded
    assert "provider_stage_timeouts" in bounded


def test_tracking_worker_failure_never_runs_local_batch_fallback():
    tracking = _function_source("_run_independent_tracking_check")
    assert "worker_applied = False" in tracking
    assert "worker_applied = True" in tracking
    assert "return worker_applied" in tracking
    assert "DurableProcessingError" in tracking
    assert "zonder lokale CPU-fallback" in tracking
    assert "calculate_batch_candidate" not in tracking
    assert "lokale_tracking_fallback" not in tracking


def test_tracking_worker_result_is_bound_to_batch_state_and_revision():
    tracking = _function_source("_run_independent_tracking_check")
    for required in (
        "'tracking_protocol_version': 5",
        "'batch_id': batch_id",
        "'start_revision': start_revision",
        "'input_digest': input_digest",
        "'start_state_digest': start_state_digest",
        "independent_tracking_batch_v5",
        "_valid_tracking_v5_snapshot",
        "_matching_sha256",
        "_tracking_state_digest",
        "expected_revision=start_revision",
        "batch_id=batch_id",
    ):
        assert required in tracking
    assert tracking.index("_matching_sha256") < tracking.index(
        "replace_tracking_state"
    )


def test_netatmo_poll_does_not_publish_moved_probe_diagnostics():
    poll = _function_source("_poll_netatmo")
    assert "probe_diagnostics" not in poll
    evaluator = _function_source("_update_virtual_probes_from_cache")
    assert "virtual_probe_update" in evaluator


def test_virtual_probe_worker_is_background_and_never_blocks_provider_stage():
    evaluator = _function_source("_update_virtual_probes_from_cache")
    assert "virtual_probe_operational" in evaluator
    assert "async_create_background_task" in evaluator
    assert "await worker_task" not in evaluator
    assert "'mode': 'operationeel'" in evaluator
    assert "'state': 'worker_onbeschikbaar'" in evaluator
    assert "probe_manager.update(" not in evaluator


def test_all_provider_activity_waits_for_home_assistant_started():
    runtime = _function_source("_async_setup_runtime")
    initial = _function_source("_do_initial_setup")
    cycle = _function_source("_poll_all")
    timeout = _function_source("_schedule_start_timeout")
    starter = _function_source("_start_runtime_background")

    assert "HA_START_WAIT_TIMEOUT_S" in timeout
    assert "hass.loop.call_later" in timeout
    assert "async_create_background_task" in starter
    assert "_await_ha_started" not in runtime
    assert "CoreState.running" in cycle
    assert "_activate_polling_schedules()" in initial
    assert "blitz.start()" in initial
    assert initial.index("await _update_fictieve_location()") < initial.index(
        "_activate_polling_schedules()"
    )
    assert initial.index(
        "await _update_secondary_target(spec, initial=True)"
    ) < initial.index("_activate_polling_schedules()")
    assert initial.index("_activate_polling_schedules()") < initial.index(
        "blitz.start()"
    )
    assert runtime.index("blitz = BlitzortungProvider") < runtime.index(
        "async def _do_initial_setup"
    )
    source_setter = _function_source("_set_lightning_source_mode")
    assert "not ha_started.is_set()" in source_setter
    assert source_setter.index("not ha_started.is_set()") < source_setter.index(
        "blitz.start()"
    )


def test_global_startup_admission_opens_only_after_recovery_and_integrity():
    runtime = _function_source("_async_setup_runtime")
    initial = _function_source("_do_initial_setup")
    cycle = _function_source("_poll_all")
    source_setter = _function_source("_set_lightning_source_mode")

    assert runtime.count("storm_manager.begin_durable_recovery()") == 1
    assert runtime.index("storm_manager = StormManager") < runtime.index(
        "storm_manager.begin_durable_recovery()"
    ) < runtime.index("active_region = storm_manager.assign_target")
    assert runtime.index(
        "durable_processing_store.integrity_check"
    ) < runtime.index("storm_manager.verify_durable_integrity")
    assert (
        "integrity_healthy=bool(durable_integrity.get('healthy'))"
        in runtime
    )
    assert initial.index(
        "await _update_secondary_target(spec, initial=True)"
    ) < initial.index("storm_manager.complete_durable_recovery()")
    assert initial.index(
        "storm_manager.complete_durable_recovery()"
    ) < initial.index("_activate_polling_schedules()")
    assert "'status': 'integrity_verified'" in runtime
    assert "'admission_ready': False" in runtime
    assert "'admission_ready': True" in initial
    assert "storm_manager.admission_ready" in cycle
    assert "storm_manager.admission_ready" in source_setter


def test_system_telemetry_covers_live_entities_and_radar_hot_paths():
    assert "sensor.system_monitor_processor_use" in INIT_SOURCE
    assert "sensor.system_monitor_processortemperatuur" in INIT_SOURCE
    assert "raster_normalization:" in INIT_SOURCE
    assert "pysteps_preflight:" in INIT_SOURCE


def test_raster_normalization_and_frame_serialization_leave_event_loop():
    overlays = _function_source("_refresh_radar_overlays")
    assert "frame_from_overlay" not in overlays
    assert "async_add_executor_job" not in overlays
    assert "radar_frame_worker" in overlays
    assert ".async_process(overlays, regions)" in overlays
    assert "'mode': 'worker_first'" in overlays
    assert ".add_overlay(" not in overlays
    assert "def _serialize_and_save_frame_history" in overlays
    assert ".async_run_io(_serialize_and_save_frame_history)" in overlays
    assert "if not changed_frames:" in overlays
    assert "frame_history.serialized()" not in overlays
    assert (
        "save_pysteps_frames, frame_history.serialized()"
        not in overlays
    )


def test_pysteps_regions_use_two_bounded_executor_workers():
    overlays = _function_source("_refresh_radar_overlays")
    assert "asyncio.Semaphore(2)" in overlays
    assert "async_request_worker_job(" in overlays
    assert "motion_preflight" in overlays
    assert "evaluate_pysteps_frames" not in overlays
    assert "asyncio.gather" in overlays


def test_offline_worker_is_rejected_before_tracking_payload_work():
    tracking = _function_source("_run_independent_tracking_check")
    heartbeat = tracking.index("worker_is_available(hass)")
    serialization = tracking.index("for observation in observations")
    durable_prepare = tracking.index("durable_store.prepare_batch")
    assert heartbeat < serialization < durable_prepare
    assert "raise WorkerTrackingRequiredError" in tracking


def test_temperature_alone_never_throttles_experimental_analysis():
    calibration = _function_source("_record_calibration_frame")
    overlays = _function_source("_refresh_radar_overlays")
    throttling = _function_source("_system_reports_hardware_throttling")
    # Calibration capture is now a constant-size artifact descriptor.  It may
    # never discard meteorological evidence because of temperature or load.
    assert "analysis_mode" not in calibration
    assert "thermal_throttle" not in calibration
    assert "_system_reports_hardware_throttling()" not in calibration
    assert "worker_artifact_descriptors_v2" in calibration
    assert "temperature >=" not in calibration
    assert "analysis_mode" in overlays
    assert "thermal_throttle" in overlays
    assert "_system_reports_hardware_throttling()" in overlays
    assert "temperature >=" not in overlays
    assert "'status': 'thermal_throttled'" in overlays
    assert "'feeds_detection': False" in overlays
    assert "temperature" not in throttling
    assert "binary_sensor.cpu_throttling" in throttling


def test_radar_calibration_is_persisted_and_compared_only_in_worker_corpus():
    capture = _function_source("_record_calibration_frame")
    flush = _function_source("_flush_calibration_data_impl")
    assert "'region_key': str(region.storage_key)" in capture
    assert "f'{region.engine_id}@{region.storage_key}'" not in capture
    assert "calibration_corpus_ingest" in flush
    assert "worker_calibration_corpus_v1" in flush
    assert "merge_worker_calibration_stage" not in flush
    assert "calibration_prepare_v2" not in flush
    assert "_validated_worker_calibration_summary" in flush
    assert "corpus_profiles" in flush
    assert "corpus_result.get('statistics')" in flush
    assert "CALIBRATION_CORPUS_MAX_CAPTURES_PER_JOB" in flush
    assert "if capture_id not in captures_by_id" in flush


def test_worker_corpus_profiles_load_in_background_without_legacy_bias_input():
    runtime = _function_source("_async_setup_runtime")
    initial = _function_source("_do_initial_setup")
    refresh = _function_source("_refresh_worker_calibration_snapshot")
    assert "calibration_store.load_bias_profiles" not in runtime
    assert "bias_profiles = []" in runtime
    assert "calibration_corpus_snapshot" in refresh
    assert "worker_calibration_corpus_snapshot_v1" in refresh
    assert "worker_is_available(hass)" in refresh
    assert "async_create_background_task" in initial
    assert initial.index(
        "_refresh_worker_calibration_snapshot()"
    ) < initial.index("_activate_polling_schedules()")


def test_netatmo_worker_job_uses_reference_and_never_transports_secrets():
    source = _function_source("_cached_worker_netatmo")
    payload = source[source.index("'netatmo_fetch'"):]
    assert "'credential_ref': 'worker_environment'" in payload
    assert "'client_secret':" not in payload
    assert "'refresh_token':" not in payload


def test_netatmo_worker_job_admission_does_not_require_ha_oauth_state():
    poll = _function_source("_poll_netatmo")
    assert "worker_observations = _cached_worker_netatmo(" in poll
    assert "if engine_id in providers" not in poll
    assert "worker_observations is None and engine_id in providers" not in poll


def test_removed_region_immediately_invalidates_published_storms():
    callback = _function_source("_on_region_removed")
    assert "'removed_engine': True" in callback
    assert "f'{DOMAIN}_storms_updated'" in callback
    assert "key.startswith(f'{region.engine_id}:')" in callback
    assert "on_engine_removed=_on_region_removed" in INIT_SOURCE


def test_ha_stop_resets_every_ofe_before_saving_tracking_state():
    stop_runtime = _function_source("_stop_region_runtime")
    shutdown_runtime = _function_source("_shutdown_runtime")
    prepare_shutdown = _function_source("_prepare_shutdown")
    stop_callback = _function_source("_on_ha_stop")
    assert "region.ofe.reset()" in stop_runtime
    assert stop_runtime.index("region.ofe.reset()") < stop_runtime.index(
        "mcs_store.async_save_engine"
    )
    assert shutdown_runtime.index(
        "provider_lifecycle.async_stop_all()"
    ) < shutdown_runtime.index("_stop_region_runtime()")
    assert shutdown_runtime.index(
        "provider_cycle_lock"
    ) < shutdown_runtime.index("_stop_region_runtime()")
    assert "_shutdown_runtime()" in stop_callback
    assert "hass.data[DOMAIN]['unsubscribers'] = []" in prepare_shutdown
    assert "async_add_shutdown_job" in INIT_SOURCE
    assert "HassJob" in INIT_SOURCE


def test_quarantined_observations_have_an_operator_retry_service():
    retry_service = _function_source("_retry_quarantined_observations")
    assert "region.ofe.async_retry_quarantined()" in retry_service
    assert "retried_batches" in retry_service
    assert "SERVICE_RETRY_QUARANTINED_OBSERVATIONS" in INIT_SOURCE
    assert "hass.services.async_register" in INIT_SOURCE
    assert "hass.services.async_remove" in INIT_SOURCE
    services = (
        Path(__file__).resolve().parents[1]
        / "custom_components"
        / "storm_tracker_v3"
        / "services.yaml"
    ).read_text(encoding="utf-8")
    assert "retry_quarantined_observations:" in services


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


def test_pysteps_preflight_is_persisted_with_the_calibration_batch():
    refresh = _function_source("_refresh_radar_overlays")
    flush = _function_source("_flush_calibration_data_impl")
    assert "'pysteps_preflight_samples_pending'" in refresh
    assert "'region_key': region.storage_key" in refresh
    assert "'nominal_minute': latest.nominal_timestamp // 60" in refresh
    assert "'pysteps_preflight_samples_pending', 'pysteps_preflight_samples'" in flush
    assert "for runtime_key, batch_key in pending_fields" in flush
    assert "batch[batch_key] = tuple(values)" in flush
    assert "batch.get(batch_key, ())" in flush


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
    assert "model_guidance_enabled=open_meteo_enabled" in INIT_SOURCE


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


def test_every_lifecycle_raster_provider_is_persisted_without_whitelist():
    national_poll = _function_source("_poll_national_providers")
    assert "for provider_id, observations in results.items()" in national_poll
    assert "overlay = provider_lifecycle.overlay(provider_id)" in national_poll
    assert "if not overlay:" in national_poll
    assert "overlay.get('timestamp')" in national_poll
    assert "_record_calibration_frame(" in national_poll
    assert (
        'if provider_id in {'
        not in national_poll
    )


def test_lightning_fallbacks_remain_separate_from_radar_routing():
    eumetsat = _function_source("_poll_eumetsat_li")
    goes = _function_source("_poll_goes_glm")
    for poller in (eumetsat, goes):
        assert "route_observation(observation)" in poller
        assert "_record_lightning(observation)" in poller
        assert "_route_selected_radar" not in poller


def test_regional_radar_fetches_are_deduplicated_or_individually_bounded():
    rainviewer = _function_source("_poll_rv")
    opera = _function_source("_poll_opera")
    assert "async_fetch_worker_radar" in rainviewer
    assert "'regions': [" in rainviewer
    assert rainviewer.count("'rainviewer_fetch'") == 1
    assert "asyncio.gather" in opera
    assert "asyncio.wait_for" in opera
    assert "return_exceptions=True" in opera


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
