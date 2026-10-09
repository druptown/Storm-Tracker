"""Tests for the unregistered, snapshot-only HA sensor candidates."""
from __future__ import annotations

import ast
import asyncio
import copy
import importlib.util
import json
from pathlib import Path
import sys
import types

import pytest


ROOT = Path(__file__).resolve().parents[1]
COMPONENT = ROOT / "custom_components" / "storm_tracker_v3"
EVENT_NAME = "storm_tracker_v3_operational_snapshot_update"
WORKER_EVENT_NAME = "storm_tracker_v3_worker_status_update"


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


class FakeBus:
    def __init__(self):
        self.listeners = {}
        self.unsubscribe_count = 0

    def async_listen(self, event_name, callback):
        callbacks = self.listeners.setdefault(event_name, [])
        callbacks.append(callback)
        active = True

        def unsubscribe():
            nonlocal active
            if not active:
                return
            active = False
            callbacks.remove(callback)
            self.unsubscribe_count += 1

        return unsubscribe

    def fire(self, event_name, data=None):
        event = types.SimpleNamespace(event_type=event_name, data=data or {})
        for callback in tuple(self.listeners.get(event_name, ())):
            callback(event)


class FakeHass:
    def __init__(self):
        self.bus = FakeBus()
        self.data = {}


def _load_modules():
    package_name = "storm_tracker_v3_snapshot_sensor_candidate_test"
    package = types.ModuleType(package_name)
    package.__path__ = [str(COMPONENT)]
    sys.modules[package_name] = package

    loaded = []
    for short_name in ("operational_snapshot", "operational_snapshot_views"):
        full_name = f"{package_name}.{short_name}"
        spec = importlib.util.spec_from_file_location(
            full_name,
            COMPONENT / f"{short_name}.py",
        )
        module = importlib.util.module_from_spec(spec)
        sys.modules[full_name] = module
        assert spec.loader is not None
        spec.loader.exec_module(module)
        loaded.append(module)

    stub_names = (
        "homeassistant",
        "homeassistant.components",
        "homeassistant.components.sensor",
        "homeassistant.core",
    )
    previous = {name: sys.modules.get(name) for name in stub_names}
    homeassistant = types.ModuleType("homeassistant")
    components = types.ModuleType("homeassistant.components")
    sensor = types.ModuleType("homeassistant.components.sensor")
    sensor.SensorEntity = FakeSensorEntity
    core = types.ModuleType("homeassistant.core")
    core.callback = lambda function: function
    homeassistant.components = components
    components.sensor = sensor
    sys.modules["homeassistant"] = homeassistant
    sys.modules["homeassistant.components"] = components
    sys.modules["homeassistant.components.sensor"] = sensor
    sys.modules["homeassistant.core"] = core
    try:
        short_name = "operational_snapshot_sensors"
        full_name = f"{package_name}.{short_name}"
        spec = importlib.util.spec_from_file_location(
            full_name,
            COMPONENT / f"{short_name}.py",
        )
        module = importlib.util.module_from_spec(spec)
        sys.modules[full_name] = module
        assert spec.loader is not None
        spec.loader.exec_module(module)
        loaded.append(module)
    finally:
        for name, prior in previous.items():
            if prior is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = prior
    return tuple(loaded)


contract, views, sensors = _load_modules()


def _ground(
    *,
    status="unknown",
    freshness="current",
    confirmed=None,
    same_track=None,
):
    has_evidence = status != "unknown"
    return {
        "status": status,
        "freshness": freshness,
        "confirmation": confirmed,
        "same_track": same_track,
        "evidence_valid_at": 1_000.0 if has_evidence else None,
        "observed_at_min": 999.0 if has_evidence else None,
        "observed_at_max": 1_000.0 if has_evidence else None,
        "received_at": 1_002.0 if has_evidence else None,
        "physical_site_count": 1 if has_evidence else 0,
        "wet_site_count": 1 if status == "wet" else 0,
        "source_ids": ["netatmo"] if has_evidence else [],
        "reason_codes": [],
    }


def _current(state, *, ground=None):
    known = state in {"dry", "raining"}
    return {
        "state": state,
        "freshness": "current" if known else "unknown",
        "rain_rate_mm_h": (
            0.0 if state == "dry" else 1.4 if state == "raining" else None
        ),
        "product_valid_at": 1_000.0 if known else None,
        "provider_ids": ["opera"] if known else [],
        "reason_codes": [] if known else ["current_precipitation_unknown"],
        "ground_evidence": (
            ground
            if ground is not None
            else _ground(freshness="current" if known else "unknown")
        ),
    }


def _threat(state="available"):
    available = state == "available"
    return {
        "state": state,
        "freshness": "current" if state != "unknown" else "unknown",
        "track_id": "track-7" if available else None,
        "calibrated_probability_percent": 74.0 if available else None,
        "probability_calibration_id": (
            "empirical:held-out-v1" if available else None
        ),
        "eta_minutes": 18.0 if available else None,
        "distance_km": 12.0 if available else None,
        "passage_status": (
            "expected" if available else "none" if state == "none" else "unknown"
        ),
        "product_valid_at": 1_000.0 if state != "unknown" else None,
        "provider_ids": ["opera"] if state != "unknown" else [],
        "reason_codes": [],
    }


def _target(target_id, state, *, ground=None):
    return {
        "target_id": target_id,
        "display_name": "Thuis" if target_id == "home" else "Wim",
        "region_id": "region-home",
        "status_text": "Huidige toestand staat los van de volgende dreiging",
        "current_precipitation": _current(state, ground=ground),
        "next_threat": _threat(),
    }


def _provider(provider_id):
    return {
        "provider_id": provider_id,
        "display_name": provider_id.upper(),
        "state": "operational",
        "freshness": "current",
        "product_valid_at": 1_000.0,
        "received_at": 1_002.0,
        "error": None,
        "reason_codes": [],
    }


def _storm(storm_id):
    return {
        "storm_id": storm_id,
        "region_id": "region-home",
        "display_name": storm_id.upper(),
        "state": "active",
        "diagnostic_quality_percent": 88.0,
        "diagnostic_quality_kind": "motion_fit",
        "motion_speed_kmh": 41.0,
        "motion_heading_deg": 225.0,
        "max_dbz": 50.0,
        "area_km2": 700.0,
        "product_valid_at": 1_000.0,
        "reason_codes": [],
    }


def _snapshot(*, revision=1, home_state="dry", target_state="raining"):
    home_ground = (
        _ground(status="wet", confirmed=True, same_track=True)
        if home_state == "raining"
        else _ground()
    )
    target_ground = (
        _ground(status="wet", confirmed=True, same_track=True)
        if target_state == "raining"
        else _ground()
    )
    return {
        "contract": "operational_snapshot_v1",
        "revision": revision,
        "desired_state_sha256": "d" * 64,
        "product_valid_at": 1_000.0,
        "received_at": 1_002.0,
        "evaluated_at": 1_004.0,
        "valid_until": 1_304.0,
        "provenance_complete": True,
        "provenance_source_ids": ["netatmo", "opera"],
        "provenance_reason_codes": [],
        "worker": {
            "state": "online",
            "queue_depth": 0,
            "failed_jobs": 0,
            "status_text": "operationeel",
        },
        "regions": {
            "complete": True,
            "overflow_count": 0,
            "items": [{
                "region_id": "region-home",
                "display_name": "Thuisregio",
                "state": "current",
                "provider_ids": ["netatmo", "opera"],
                "target_ids": ["home", "person.wim"],
            }],
        },
        "targets": {
            "complete": True,
            "overflow_count": 0,
            "items": [
                _target("home", home_state, ground=home_ground),
                _target("person.wim", target_state, ground=target_ground),
            ],
        },
        "providers": {
            "complete": True,
            "overflow_count": 0,
            "items": [_provider("netatmo"), _provider("opera")],
        },
        "ui_storms": {
            "complete": True,
            "overflow_count": 0,
            "items": [_storm("storm-1"), _storm("storm-2")],
        },
        "map_artifact": {"artifact_handle": None, "etag": None},
    }


def _cache(snapshot=None):
    cache = contract.OperationalSnapshotCache()
    if snapshot is not None:
        cache.apply_snapshot(snapshot)
    return cache


def _entities(cache=None):
    return sensors.build_snapshot_sensor_candidates(
        _cache(_snapshot()) if cache is None else cache,
        unique_id_prefix="entry-a",
        home_target_id="home",
        additional_targets={"person.wim": "Wim"},
    )


def _one(entities, entity_type):
    return next(entity for entity in entities if isinstance(entity, entity_type))


def test_candidate_factory_builds_all_five_sensor_kinds_without_registration():
    entities = _entities()

    assert sensors.OPERATIONAL_SNAPSHOT_UPDATE_EVENT == EVENT_NAME
    assert len(entities) == 5
    assert [type(entity) for entity in entities] == [
        sensors.SnapshotWorkerStatusSensor,
        sensors.SnapshotHomePrecipitationSensor,
        sensors.SnapshotTargetPrecipitationSensor,
        sensors.SnapshotStormCountSensor,
        sensors.SnapshotRegionCountSensor,
    ]
    assert all(entity._attr_should_poll is False for entity in entities)
    assert len({entity._attr_unique_id for entity in entities}) == 5
    assert (
        _one(entities, sensors.SnapshotWorkerStatusSensor)._attr_unique_id
        == "stv3_worker_status"
    )
    assert (
        _one(entities, sensors.SnapshotWorkerStatusSensor)._attr_name
        == "STV3 Worker Status"
    )
    assert not hasattr(sensors, "async_setup_entry")
    assert not hasattr(sensors, "async_setup_platform")
    assert all(
        entity.extra_state_attributes["runtime_mode"] == "worker_only"
        for entity in entities
    )


def test_all_worker_catalog_providers_have_one_snapshot_only_status_sensor():
    from storm_tracker_worker.multi_source_operational_cycle import (
        EXPECTED_SOURCE_IDS,
    )

    assert sensors.WORKER_ONLY_PROVIDER_IDS == tuple(sorted(
        EXPECTED_SOURCE_IDS - {
            "ground.synoptic.region",
            "radar.kmi.public_wcs_unknown_quantity",
        }
    ))
    assert "ground.synoptic.region" not in sensors.WORKER_ONLY_PROVIDER_IDS
    snapshot = _snapshot()
    snapshot["providers"]["items"].append(_provider("ground.netatmo.region"))
    snapshot["providers"]["items"].sort(key=lambda item: item["provider_id"])
    snapshot["provenance_source_ids"] = sorted({
        *snapshot["provenance_source_ids"],
        "ground.netatmo.region",
    })
    entities = sensors.build_snapshot_sensor_candidates(
        _cache(snapshot),
        unique_id_prefix="entry-provider-catalog",
        home_target_id="home",
        provider_ids=sensors.WORKER_ONLY_PROVIDER_IDS,
    )

    provider_entities = [
        entity
        for entity in entities
        if isinstance(entity, sensors.SnapshotProviderStatusSensor)
    ]
    assert len(provider_entities) == len(sensors.WORKER_ONLY_PROVIDER_IDS) == 26
    assert len({entity._attr_unique_id for entity in provider_entities}) == 26
    netatmo = next(
        entity
        for entity in provider_entities
        if entity.extra_state_attributes["configured_provider_id"]
        == "ground.netatmo.region"
    )
    assert netatmo.native_value == "operational"
    assert netatmo.extra_state_attributes["provider_presentation_valid"] is True


def test_provider_sensor_refreshes_from_heartbeat_while_weather_snapshot_is_stale():
    async def run():
        cache = _cache(_snapshot())
        assert cache.mark_stale(
            reason="desired_state_transition",
            worker_state="unknown",
        )
        entities = sensors.build_snapshot_sensor_candidates(
            cache,
            unique_id_prefix="entry-provider-heartbeat",
            home_target_id="home",
            provider_ids=("opera",),
        )
        provider = _one(entities, sensors.SnapshotProviderStatusSensor)
        hass = FakeHass()
        hass.data["storm_tracker_v3"] = {
            "worker_status": {
                "runtime_provider_status_snapshot": {
                    "desired_state_sha256": "d" * 64,
                    "evaluated_at": 1_200.0,
                    "valid_until": 4_000_000_000.0,
                    "items": [{
                        "provider_id": "opera",
                        "display_name": "OPERA",
                        "readiness": "ready",
                        "state": "operational",
                        "freshness": "current",
                        "product_valid_at": 1_190.0,
                        "received_at": 1_195.0,
                        "error": None,
                        "reason_codes": [],
                    }],
                },
            },
        }
        provider.hass = hass

        await provider.async_added_to_hass()
        hass.bus.fire(WORKER_EVENT_NAME)

        assert provider.native_value == "operational"
        assert provider.extra_state_attributes[
            "provider_status_availability"
        ] == "current"
        assert provider.extra_state_attributes["snapshot_availability"] == "stale"
        assert provider._test_write_count == 1

    asyncio.run(run())


def test_current_rain_future_threat_and_ground_confirmation_stay_separate():
    entities = _entities()
    home = _one(entities, sensors.SnapshotHomePrecipitationSensor)
    target = _one(entities, sensors.SnapshotTargetPrecipitationSensor)

    assert home.native_value == "droog"
    assert home.extra_state_attributes["next_threat"] == "beschikbaar"
    assert home.extra_state_attributes["next_threat_eta_minutes"] == 18.0
    assert home.extra_state_attributes["ground_confirmation"] is None

    assert target.native_value == "regen"
    assert target.extra_state_attributes["rain_rate_mm_h"] == 1.4
    assert target.extra_state_attributes["ground_confirmation"] is True
    assert target.extra_state_attributes["next_threat_state"] == "available"


def test_unknown_current_fact_is_never_promoted_by_a_future_threat():
    snapshot = _snapshot(target_state="dry")
    snapshot["targets"]["items"][1] = _target("person.wim", "unknown")
    entities = _entities(_cache(snapshot))
    target = _one(entities, sensors.SnapshotTargetPrecipitationSensor)

    assert target.native_value == "onvoldoende_data"
    assert target.extra_state_attributes["current_precipitation_state"] == "unknown"
    assert target.extra_state_attributes["rain_rate_mm_h"] is None
    assert target.extra_state_attributes["next_threat"] == "beschikbaar"
    assert target.extra_state_attributes["current_presentation_valid"] is False


def test_certified_zero_radar_sample_is_visible_but_not_presented_as_dry():
    snapshot = _snapshot(target_state="dry")
    target = _target("person.wim", "unknown")
    target["status_text"] = (
        "Geen positieve neerslag in actuele radar; "
        "droogte niet onafhankelijk bevestigd"
    )
    target["current_precipitation"].update({
        "freshness": "current",
        "product_valid_at": 1_000.0,
        "provider_ids": ["opera"],
        "reason_codes": [
            "certified_zero_radar_sample_dry_claim_withheld"
        ],
    })
    snapshot["targets"]["items"][1] = target
    entities = _entities(_cache(snapshot))
    entity = _one(entities, sensors.SnapshotTargetPrecipitationSensor)

    assert entity.native_value == "geen_positieve_neerslag_waargenomen"
    assert entity.extra_state_attributes["current_precipitation_state"] == "unknown"
    assert entity.extra_state_attributes["rain_rate_mm_h"] is None
    assert entity.extra_state_attributes["current_presentation_valid"] is False
    assert entity.extra_state_attributes["zero_radar_observation_current"] is True


def test_certified_undetect_radar_sample_is_visible_but_not_presented_as_dry():
    snapshot = _snapshot(target_state="dry")
    target = _target("person.wim", "unknown")
    target["status_text"] = (
        "Neerslag onder detectiedrempel in actuele radar; "
        "droogte niet onafhankelijk bevestigd"
    )
    target["current_precipitation"].update({
        "freshness": "current",
        "product_valid_at": 1_000.0,
        "provider_ids": ["opera"],
        "reason_codes": [
            "certified_undetect_radar_sample_dry_claim_withheld"
        ],
    })
    snapshot["targets"]["items"][1] = target
    entity = _one(
        _entities(_cache(snapshot)),
        sensors.SnapshotTargetPrecipitationSensor,
    )

    assert entity.native_value == "geen_positieve_neerslag_waargenomen"
    assert entity.extra_state_attributes["current_presentation_valid"] is False
    assert entity.extra_state_attributes["zero_radar_observation_current"] is False
    assert entity.extra_state_attributes["undetect_radar_observation_current"] is True
    assert entity.extra_state_attributes["nonpositive_radar_observation_current"] is True


def test_certified_reflectivity_echo_has_distinct_non_rain_presentation():
    snapshot = _snapshot(target_state="dry")
    target = _target("person.wim", "unknown")
    target["status_text"] = (
        "Radarecho: 27.5 dBZ (regenintensiteit nog niet gekwantificeerd)"
    )
    target["current_precipitation"].update({
        "freshness": "current",
        "product_valid_at": 1_000.0,
        "provider_ids": ["radar.opera.dbzh"],
        "reason_codes": [
            "certified_positive_reflectivity_echo_rain_rate_withheld"
        ],
    })
    snapshot["targets"]["items"][1] = target
    snapshot["providers"]["items"].append(_provider("radar.opera.dbzh"))
    snapshot["providers"]["items"].sort(key=lambda item: item["provider_id"])
    snapshot["regions"]["items"][0]["provider_ids"].append("radar.opera.dbzh")
    snapshot["regions"]["items"][0]["provider_ids"].sort()
    snapshot["provenance_source_ids"].append("radar.opera.dbzh")
    snapshot["provenance_source_ids"].sort()
    entity = _one(
        _entities(_cache(snapshot)),
        sensors.SnapshotTargetPrecipitationSensor,
    )

    assert entity.native_value == "radarecho_waargenomen"
    attributes = entity.extra_state_attributes
    assert attributes["reflectivity_echo_current"] is True
    assert attributes["current_precipitation_state"] == "unknown"
    assert attributes["rain_rate_mm_h"] is None
    assert attributes["current_presentation_valid"] is False
    assert "current_precipitation_unknown" not in attributes[
        "presentation_reason_codes"
    ]


def test_current_ground_evidence_remains_separate_when_radar_fact_is_unknown():
    snapshot = _snapshot(target_state="dry")
    ground = _ground(status="wet", confirmed=False, same_track=False)
    target = _target("person.wim", "unknown", ground=ground)
    target["current_precipitation"].update({
        "freshness": "current",
        "product_valid_at": 1_000.0,
    })
    snapshot["targets"]["items"][1] = target
    entities = _entities(_cache(snapshot))
    target_entity = _one(
        entities,
        sensors.SnapshotTargetPrecipitationSensor,
    )

    assert target_entity.native_value == "onvoldoende_data"
    attributes = target_entity.extra_state_attributes
    assert attributes["current_precipitation_state"] == "unknown"
    assert attributes["ground_presentation_valid"] is True
    assert attributes["ground_status"] == "nat"
    assert attributes["ground_confirmation"] is False
    assert attributes["ground_same_track"] is False
    assert attributes["next_threat_presentation_valid"] is True


def test_empty_or_stale_snapshot_never_reports_dry_or_zero_counts():
    empty_entities = _entities(_cache())
    empty_home = _one(empty_entities, sensors.SnapshotHomePrecipitationSensor)
    empty_storms = _one(empty_entities, sensors.SnapshotStormCountSensor)
    empty_regions = _one(empty_entities, sensors.SnapshotRegionCountSensor)

    assert empty_home.native_value == "onvoldoende_data"
    assert empty_storms.native_value is None
    assert empty_regions.native_value is None
    assert empty_storms.extra_state_attributes["count_valid"] is False
    assert empty_storms.extra_state_attributes["bounded_snapshot_item_count"] == 0

    cache = _cache(_snapshot(home_state="raining"))
    assert cache.mark_worker_offline()
    stale_entities = _entities(cache)
    stale_home = _one(stale_entities, sensors.SnapshotHomePrecipitationSensor)
    stale_storms = _one(stale_entities, sensors.SnapshotStormCountSensor)

    assert stale_home.native_value == "onvoldoende_data"
    assert stale_home.extra_state_attributes["rain_rate_mm_h"] is None
    assert stale_home.extra_state_attributes["ground_confirmation"] is None
    assert stale_home.extra_state_attributes["next_threat_state"] == "unknown"
    assert stale_storms.native_value is None
    assert stale_storms.extra_state_attributes["bounded_snapshot_item_count"] == 2


def test_incomplete_storm_collection_reports_exact_total_and_transport_overflow():
    snapshot = _snapshot()
    snapshot["ui_storms"] = {
        "complete": False,
        "overflow_count": 3,
        "items": snapshot["ui_storms"]["items"],
    }
    entities = _entities(_cache(snapshot))
    storms = _one(entities, sensors.SnapshotStormCountSensor)

    assert storms.native_value == 5
    assert storms.available is True
    assert storms.extra_state_attributes["count_valid"] is True
    assert storms.extra_state_attributes["bounded_snapshot_item_count"] == 2
    assert storms.extra_state_attributes["omitted_snapshot_item_count"] == 3
    assert storms.extra_state_attributes["total_item_count"] == 5
    assert "collection_incomplete" in (
        storms.extra_state_attributes["presentation_reason_codes"]
    )


def test_complete_region_topology_count_does_not_require_weather_provenance():
    snapshot = _snapshot()
    snapshot["provenance_complete"] = False
    snapshot["provenance_reason_codes"] = [
        "multi_source_values_not_yet_fused"
    ]
    snapshot["regions"]["items"][0].update({
        "state": "unknown",
        "provider_ids": [],
    })
    for target in snapshot["targets"]["items"]:
        target.update({
            "status_text": "",
            "current_precipitation": _current("unknown"),
            "next_threat": _threat("unknown"),
        })
    snapshot["ui_storms"]["items"] = []
    entities = _entities(_cache(snapshot))
    regions = _one(entities, sensors.SnapshotRegionCountSensor)
    storms = _one(entities, sensors.SnapshotStormCountSensor)

    assert regions.native_value == 1
    assert regions.extra_state_attributes["count_valid"] is True
    assert regions.extra_state_attributes["provenance_required"] is False
    assert regions.extra_state_attributes["presentation_reason_codes"] == []
    assert storms.native_value is None
    assert storms.extra_state_attributes["count_valid"] is False
    assert storms.extra_state_attributes["provenance_required"] is True
    assert "provenance_incomplete" in (
        storms.extra_state_attributes["presentation_reason_codes"]
    )


def test_pending_region_is_never_presented_as_a_real_region_count():
    snapshot = _snapshot()
    snapshot["regions"]["items"] = [{
        "region_id": "region:awaiting_evidence",
        "display_name": "Nog niet meteorologisch toegewezen",
        "state": "unknown",
        "provider_ids": [],
        "target_ids": ["home", "person.wim"],
    }]
    for target in snapshot["targets"]["items"]:
        target["region_id"] = "region:awaiting_evidence"
    snapshot["ui_storms"]["items"] = []
    entities = _entities(_cache(snapshot))
    regions = _one(entities, sensors.SnapshotRegionCountSensor)

    assert regions.native_value is None
    assert regions.extra_state_attributes["bounded_snapshot_item_count"] == 1
    assert regions.extra_state_attributes["count_valid"] is False
    assert regions.extra_state_attributes["region_assignment_pending"] is True
    assert regions.extra_state_attributes["presentation_reason_codes"] == [
        "region_assignment_pending"
    ]


def test_pending_bucket_is_excluded_while_real_regions_remain_countable():
    snapshot = _snapshot()
    snapshot["regions"]["items"][0]["target_ids"] = ["home"]
    snapshot["regions"]["items"].append({
        "region_id": "region:awaiting_evidence",
        "display_name": "Nog niet meteorologisch toegewezen",
        "state": "unknown",
        "provider_ids": [],
        "target_ids": ["person.wim"],
    })
    snapshot["targets"]["items"][1]["region_id"] = (
        "region:awaiting_evidence"
    )
    entities = _entities(_cache(snapshot))
    regions = _one(entities, sensors.SnapshotRegionCountSensor)

    assert regions.native_value == 1
    assert regions.extra_state_attributes["bounded_snapshot_item_count"] == 2
    assert regions.extra_state_attributes["active_region_count"] == 1
    assert regions.extra_state_attributes["count_valid"] is True
    assert regions.extra_state_attributes["region_assignment_pending"] is True
    assert regions.extra_state_attributes["presentation_reason_codes"] == []


def test_one_event_listener_refreshes_every_entity_without_polling():
    cache = _cache(_snapshot())
    entities = _entities(cache)
    hass = FakeHass()
    hass.data["storm_tracker_v3"] = {
        "worker_status": {
            "state": "online",
            "version": "1.17.44",
            "compute_profile": "dedicated_nas_fast",
            "cpu_limit_percent": 200,
            "worker_nice": 0,
            "cpu_threads": 2,
            "provider_cycle_seconds": 2.0,
            "runtime_provider_count": 28,
            "runtime_captured_provider_count": 7,
            "raw_calibration_status": "ready",
            "raw_calibration_captures": 42,
            "raw_calibration_sources": 7,
            "raw_calibration_providers": 5,
            "postgis_projection_enabled": True,
            "postgis_projection_status": "current",
            "postgis_projection_ready": 0,
            "postgis_projection_leased": 0,
            "postgis_projection_projected": 42,
            "postgis_projection_last_success_at": 1_235.0,
            "acquisition_parallel_mode": "full_parallel",
            "acquisition_parallel_lane_limit": 5,
            "acquisition_parallel_maximum_lanes": 27,
            "acquisition_parallel_last_batch_jobs": 5,
            "acquisition_parallel_last_batch_seconds": 12.346,
            "acquisition_parallel_last_unhealthy_reasons": [],
            "tracking_active_tracks": 11,
            "tracking_tracked_tracks": 8,
            "tracking_motion_ready_tracks": 3,
            "tracking_region_count": 2,
            "tracking_shadow_arrival_candidates": 4,
            "tracking_shadow_future_arrivals": 1,
            "tracking_shadow_arrival_status": "shadow_candidate_unadmitted",
            "raw_calibration_source_statistics": [
                {
                    "source_id": "lightning.goes19.glm",
                    "provider": "noaa_goes",
                    "captures": 17,
                    "latest_received_at": 1_234.5,
                },
                {
                    "source_id": "radar.dwd.radolan_rv",
                    "provider": "dwd",
                    "captures": 25,
                    "latest_received_at": 1_230.0,
                },
            ],
            "snapshot_rich_built": 12,
            "last_acquisition": (
                "lightning.goes19.glm|shadow_raw_capture_complete"
            ),
            "snapshot_rich_withheld": 1,
            "snapshot_unknown_built": 2,
        }
    }
    for entity in entities:
        entity.hass = hass
        asyncio.run(entity.async_added_to_hass())

    assert list(hass.bus.listeners) == [EVENT_NAME, WORKER_EVENT_NAME]
    assert len(hass.bus.listeners[EVENT_NAME]) == 1
    assert len(hass.bus.listeners[WORKER_EVENT_NAME]) == 1
    assert all(getattr(entity, "_test_write_count", 0) == 0 for entity in entities)
    worker = _one(entities, sensors.SnapshotWorkerStatusSensor)
    assert worker.extra_state_attributes["worker_version"] == "1.17.44"
    assert worker.native_value == "online"
    assert worker.extra_state_attributes["worker_heartbeat_state"] == "online"
    assert worker.extra_state_attributes["snapshot_worker_state"] == "online"
    assert worker.extra_state_attributes["compute_profile"] == "dedicated_nas_fast"
    assert worker.extra_state_attributes["cpu_limit_percent"] == 200
    assert worker.extra_state_attributes["worker_nice"] == 0
    assert worker.extra_state_attributes["cpu_threads"] == 2
    assert worker.extra_state_attributes["provider_cycle_seconds"] == 2.0
    assert worker.extra_state_attributes["runtime_provider_count"] == 28
    assert worker.extra_state_attributes["runtime_captured_provider_count"] == 7
    assert worker.extra_state_attributes["raw_calibration_status"] == "ready"
    assert worker.extra_state_attributes["raw_calibration_captures"] == 42
    assert worker.extra_state_attributes["raw_calibration_sources"] == 7
    assert worker.extra_state_attributes["raw_calibration_providers"] == 5
    assert worker.extra_state_attributes["postgis_projection_enabled"] is True
    assert worker.extra_state_attributes["postgis_projection_status"] == "current"
    assert worker.extra_state_attributes["postgis_projection_ready"] == 0
    assert worker.extra_state_attributes["postgis_projection_leased"] == 0
    assert worker.extra_state_attributes["postgis_projection_projected"] == 42
    assert worker.extra_state_attributes[
        "postgis_projection_last_success_at"
    ] == 1_235.0
    assert worker.extra_state_attributes["acquisition_parallel_mode"] == (
        "full_parallel"
    )
    assert worker.extra_state_attributes["acquisition_parallel_lane_limit"] == 5
    assert worker.extra_state_attributes[
        "acquisition_parallel_maximum_lanes"
    ] == 27
    assert worker.extra_state_attributes[
        "acquisition_parallel_last_batch_jobs"
    ] == 5
    assert worker.extra_state_attributes[
        "acquisition_parallel_last_batch_seconds"
    ] == 12.346
    assert worker.extra_state_attributes[
        "acquisition_parallel_last_unhealthy_reasons"
    ] == []
    assert worker.extra_state_attributes["tracking_active_tracks"] == 11
    assert worker.extra_state_attributes["tracking_tracked_tracks"] == 8
    assert worker.extra_state_attributes["tracking_motion_ready_tracks"] == 3
    assert worker.extra_state_attributes["tracking_region_count"] == 2
    assert worker.extra_state_attributes[
        "tracking_shadow_arrival_candidates"
    ] == 4
    assert worker.extra_state_attributes["tracking_shadow_future_arrivals"] == 1
    assert worker.extra_state_attributes["tracking_shadow_arrival_status"] == (
        "shadow_candidate_unadmitted"
    )
    assert worker.extra_state_attributes[
        "raw_calibration_source_statistics"
    ] == [
        {
            "source_id": "lightning.goes19.glm",
            "provider": "noaa_goes",
            "captures": 17,
            "latest_received_at": 1_234.5,
        },
        {
            "source_id": "radar.dwd.radolan_rv",
            "provider": "dwd",
            "captures": 25,
            "latest_received_at": 1_230.0,
        },
    ]
    assert worker.extra_state_attributes["snapshot_rich_built"] == 12
    assert worker.extra_state_attributes["last_acquisition"] == (
        "lightning.goes19.glm|shadow_raw_capture_complete"
    )
    assert worker.extra_state_attributes["snapshot_rich_withheld"] == 1
    assert worker.extra_state_attributes["snapshot_unknown_built"] == 2

    hass.data["storm_tracker_v3"]["worker_status"]["last_acquisition"] = (
        "radar.dwd.radolan_rv|shadow_raw_capture_recovered"
    )
    hass.bus.fire(WORKER_EVENT_NAME)
    assert worker.extra_state_attributes["last_acquisition"] == (
        "radar.dwd.radolan_rv|shadow_raw_capture_recovered"
    )
    assert worker._test_write_count == 1
    assert all(
        getattr(entity, "_test_write_count", 0) == 0
        for entity in entities
        if entity is not worker
    )

    cache.mark_stale(reason="desired_state_transition", worker_state="unknown")
    hass.bus.fire(EVENT_NAME, {"revision": 1})
    assert worker.native_value == "online"
    assert worker.extra_state_attributes["worker_heartbeat_state"] == "online"
    assert worker.extra_state_attributes["snapshot_worker_state"] == "unknown"

    hass.data["storm_tracker_v3"]["worker_status"]["state"] = "offline"
    hass.bus.fire(WORKER_EVENT_NAME)
    assert worker.native_value == "offline"
    assert worker.extra_state_attributes["worker_heartbeat_state"] == "offline"
    assert worker.extra_state_attributes["snapshot_worker_state"] == "unknown"

    cache.apply_snapshot(_snapshot(revision=2, home_state="raining"))
    hass.bus.fire(EVENT_NAME, {"revision": 2})
    home = _one(entities, sensors.SnapshotHomePrecipitationSensor)
    assert home.native_value == "regen"
    assert worker._test_write_count == 4
    assert all(
        entity._test_write_count == 2
        for entity in entities
        if entity is not worker
    )

    for entity in entities:
        for remove in entity._test_remove_callbacks:
            remove()
    assert hass.bus.listeners[EVENT_NAME] == []
    assert hass.bus.listeners[WORKER_EVENT_NAME] == []
    assert hass.bus.unsubscribe_count == 2


def test_projection_hot_path_has_no_collection_iteration_or_io_calls():
    source = (COMPONENT / "operational_snapshot_sensors.py").read_text(
        encoding="utf-8"
    )
    tree = ast.parse(source)
    entity_class = next(
        node
        for node in tree.body
        if isinstance(node, ast.ClassDef)
        and node.name == "SnapshotOnlySensorEntity"
    )
    hot_methods = {
        node.name: node
        for node in entity_class.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name in {
            "native_value",
            "available",
            "extra_state_attributes",
            "_project",
            "_fail_closed",
            "_refresh_from_snapshot",
        }
    }
    assert set(hot_methods) == {
        "native_value",
        "available",
        "extra_state_attributes",
        "_project",
        "_fail_closed",
        "_refresh_from_snapshot",
    }
    forbidden_nodes = (ast.For, ast.AsyncFor, ast.ListComp, ast.SetComp, ast.DictComp)
    assert not any(
        isinstance(node, forbidden_nodes)
        for method in hot_methods.values()
        for node in ast.walk(method)
    )
    forbidden_calls = {"open", "connect", "execute", "read_text", "write_text"}
    assert not any(
        isinstance(node, ast.Call)
        and (
            isinstance(node.func, ast.Name)
            and node.func.id in forbidden_calls
            or isinstance(node.func, ast.Attribute)
            and node.func.attr in forbidden_calls
        )
        for method in hot_methods.values()
        for node in ast.walk(method)
    )


def test_candidate_module_has_only_light_snapshot_and_ha_dependencies():
    path = COMPONENT / "operational_snapshot_sensors.py"
    tree = ast.parse(path.read_text(encoding="utf-8"))
    imports = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imports.append(node.module or "")

    forbidden = (
        "sqlite3",
        "pathlib",
        "requests",
        "urllib",
        "aiohttp",
        "numpy",
        "pysteps",
        "providers",
        "engine",
        "sensor",
        "coordinator",
    )
    assert not any(
        name == prefix or name.startswith(prefix + ".")
        for name in imports
        for prefix in forbidden
    )
    source = path.read_text(encoding="utf-8")
    assert "async_track_time_interval" not in source
    assert "DataUpdateCoordinator" not in source
    assert "async_setup_entry" not in source
    assert "async_setup_platform" not in source


def test_maximum_worker_only_entity_set_stays_below_ha_attribute_warning_limit():
    snapshot = copy.deepcopy(_snapshot(home_state="raining"))
    provider_ids = [
        f"provider.{index:02d}." + ("p" * 20)
        for index in range(64)
    ]
    reason_codes = [
        f"reason.{index:02d}." + ("r" * 170)
        for index in range(32)
    ]
    home = snapshot["targets"]["items"][0]
    current = home["current_precipitation"]
    ground = current["ground_evidence"]
    threat = home["next_threat"]
    current["provider_ids"] = provider_ids
    current["reason_codes"] = reason_codes
    ground.update({
        "status": "wet",
        "freshness": "current",
        "confirmation": True,
        "same_track": True,
        "physical_site_count": 16,
        "wet_site_count": 16,
        "source_ids": provider_ids[:16],
        "reason_codes": reason_codes,
    })
    threat["provider_ids"] = provider_ids
    threat["reason_codes"] = reason_codes
    snapshot["targets"]["items"] = [home]
    snapshot["providers"]["items"] = [
        _provider(provider_id) for provider_id in provider_ids
    ]
    snapshot["regions"]["items"][0]["provider_ids"] = provider_ids
    snapshot["regions"]["items"][0]["target_ids"] = ["home"]
    snapshot["provenance_source_ids"] = provider_ids
    snapshot["ui_storms"]["items"] = []

    cache = _cache(snapshot)
    additional_targets = {
        f"person.p{index:02d}": f"Person {index:02d}"
        for index in range(60)
    }
    entities = sensors.build_snapshot_sensor_candidates(
        cache,
        unique_id_prefix="entry-max",
        home_target_id="home",
        additional_targets=additional_targets,
    )
    assert len(entities) == 64

    for entity in entities:
        attributes = entity.extra_state_attributes
        encoded = json.dumps(
            attributes,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        assert len(encoded) < sensors.HA_ATTRIBUTE_WARNING_BYTES
        assert attributes["runtime_mode"] == "worker_only"

    home_entity = _one(entities, sensors.SnapshotHomePrecipitationSensor)
    attributes = home_entity.extra_state_attributes
    assert home_entity.native_value == "onvoldoende_data"
    assert attributes["ha_attribute_budget_exceeded"] is True
    assert (
        attributes["ha_attribute_bytes_before_budget"]
        >= sensors.HA_ATTRIBUTE_WARNING_BYTES
    )
    assert attributes["presentation_reason_codes"] == [
        "ha_attribute_budget_exceeded"
    ]
    assert attributes["current_precipitation_state"] == "unknown"
    assert attributes["ground_status"] == "onbekend"
    assert attributes["next_threat_state"] == "unknown"
    assert attributes["current_provider_count"] == 64
    assert attributes["current_reason_count"] == 32
    assert attributes["ground_source_count"] == 16
    assert attributes["next_threat_provider_count"] == 64
    for removed in (
        "current_provider_ids",
        "current_reason_codes",
        "ground_source_ids",
        "ground_reason_codes",
        "next_threat_provider_ids",
        "next_threat_reason_codes",
    ):
        assert removed not in attributes

    with pytest.raises(ValueError, match="te veel extra targets"):
        sensors.build_snapshot_sensor_candidates(
            cache,
            unique_id_prefix="entry-overflow",
            home_target_id="home",
            additional_targets={
                f"person.x{index:02d}": f"Overflow {index:02d}"
                for index in range(61)
            },
        )


@pytest.mark.parametrize(
    "kwargs",
    (
        {"unique_id_prefix": "", "home_target_id": "home"},
        {"unique_id_prefix": "entry-a", "home_target_id": ""},
        {
            "unique_id_prefix": "entry-a",
            "home_target_id": "home",
            "additional_targets": {"home": "duplicate"},
        },
    ),
)
def test_invalid_candidate_identity_is_rejected(kwargs):
    with pytest.raises(ValueError):
        sensors.build_snapshot_sensor_candidates(_cache(), **kwargs)
