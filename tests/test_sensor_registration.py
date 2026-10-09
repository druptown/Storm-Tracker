"""Regressietests voor de lijst met geregistreerde sensoren."""

from __future__ import annotations

import ast
import math
import time
from collections.abc import Mapping
from enum import Enum
from pathlib import Path
from types import SimpleNamespace


SENSOR_MODULE = (
    Path(__file__).parents[1]
    / "custom_components"
    / "storm_tracker_v3"
    / "sensor.py"
)


class _SourceRecordIdKind(str, Enum):
    PROVIDER_RECORD = "provider_record"
    DERIVED_STATION_TIMESTAMP = "derived_station_timestamp"


def _load_pure_sensor_function(name: str):
    tree = ast.parse(SENSOR_MODULE.read_text(encoding="utf-8"))
    function = next(
        node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == name
    )
    namespace = {}
    exec(
        compile(
            ast.fix_missing_locations(
                ast.Module(body=[function], type_ignores=[])
            ),
            str(SENSOR_MODULE),
            "exec",
        ),
        namespace,
    )
    return namespace[name]


def test_open_meteo_gear_sensor_is_registered() -> None:
    """Voorkom dat de sensorclass bestaat zonder als HA-entiteit te laden."""
    tree = ast.parse(SENSOR_MODULE.read_text(encoding="utf-8"))

    setup = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef)
        and node.name == "_legacy_sensor_entities"
    )
    registered_classes = {
        call.func.id
        for call in ast.walk(setup)
        if isinstance(call, ast.Call) and isinstance(call.func, ast.Name)
    }

    assert "OpenMeteoGearSensor" in registered_classes
    assert "SynopticGroundSensor" in registered_classes
    assert "TargetPrecipitationStatusSensor" in registered_classes
    assert "StormMapGeoJsonSensor" in registered_classes


def test_current_footprint_shadow_uses_exact_latest_frame_scan():
    helper = _load_pure_sensor_function("_current_footprint_evidence")
    helper.__globals__.update({
        "hashlib": __import__("hashlib"),
        "json": __import__("json"),
        "math": math,
        "CURRENT_FOOTPRINT_DISTANCE_TOLERANCE_KM": 1e-6,
        "CURRENT_FOOTPRINT_FRESHNESS_LIMIT_SECONDS": 900.0,
        "CURRENT_FOOTPRINT_FRESHNESS_POLICY_ID": (
            "stv3-current-radar-footprint-freshness-v1"
        ),
        "CurrentFootprintEvidence": lambda **values: SimpleNamespace(**values),
    })
    minimum_timestamps = []

    class Storm:
        def __init__(self, storm_id, timestamp, distance):
            self.storm_id = storm_id
            self.radar_cells = {
                "cell": SimpleNamespace(timestamp=timestamp)
            }
            self._timestamp = timestamp
            self._distance = distance

        def closest_radar_point(
            self,
            target_lat,
            target_lon,
            *,
            min_timestamp,
        ):
            minimum_timestamps.append(min_timestamp)
            if self._timestamp < min_timestamp:
                return None
            return (self._distance, target_lat, target_lon)

    evidence = helper(
        [
            Storm("storm-current", 1_000.0, 0.0),
            Storm("storm-nearby", 1_000.0, 4.25),
            Storm("storm-stale", 800.0, 0.0),
        ],
        40.7,
        -74.0,
        tracker_id="tracker-new-york",
        radar_source="rainviewer",
        radar_covered=True,
        evaluated_at=1_005.0,
    )

    assert evidence.frame_valid_at == 1_000.0
    assert evidence.evaluated_storm_ids == (
        "storm-current",
        "storm-nearby",
    )
    assert evidence.containing_storm_ids == ("storm-current",)
    assert evidence.nearest_storm_id == "storm-current"
    assert evidence.minimum_distance_km == 0.0
    assert evidence.frame_age_seconds == 5.0
    assert evidence.fresh is True
    assert evidence.scan_complete is False
    assert minimum_timestamps == [940.0, 940.0, 940.0]


def test_current_ground_shadow_requires_typed_fresh_local_rows():
    helper = _load_pure_sensor_function("_current_ground_evidence")
    helper.__globals__.update({
        "Mapping": Mapping,
        "math": math,
        "time": time,
        "hashlib": __import__("hashlib"),
        "GROUND_FRESHNESS_LIMITS_SECONDS": (
            ("metar", 2_100.0),
            ("netatmo", 600.0),
            ("synoptic", 1_200.0),
        ),
        "GROUND_FRESHNESS_POLICY_ID": (
            "stv3-ground-current-freshness-v1"
        ),
        "SourceRecordIdKind": _SourceRecordIdKind,
        "GroundEvidenceKind": SimpleNamespace(
            PRECIPITATION_PRESENCE="precipitation_presence"
        ),
        "CurrentGroundEvidence": lambda **values: SimpleNamespace(**values),
    })
    summary = {
        "ground_precipitation_observed": True,
        "ground_evaluated_at": 1_000.0,
        "ground_freshness_policy_id": (
            "stv3-ground-current-freshness-v1"
        ),
        "ground_local_radius_km": 10.0,
        "ground_assessments": [
            {
                "station_id": "home-gauge",
                "physical_site_id": "site-home",
                "physical_site_basis": "provider_identity",
                "counts_as_independent": True,
                "provider": "netatmo",
                "source_record_id": "netatmo:home-gauge:970.000000",
                "source_record_id_kind": "derived_station_timestamp",
                "observed_at": 970.0,
                "age_seconds": 30.0,
                "freshness_limit_seconds": 600.0,
                "freshness_policy_id": (
                    "stv3-ground-current-freshness-v1"
                ),
                "freshness_policy_known": True,
                "distance_target_km": 0.2,
                "storm_id": "storm-heffen",
                "storm_match_ambiguous": False,
                "wet": True,
                "fresh": True,
            },
            {
                "station_id": "home-gauge-duplicate",
                "physical_site_id": "site-home",
                "physical_site_basis": (
                    "proximity_conservative_dedup"
                ),
                "counts_as_independent": False,
                "provider": "synoptic",
                "source_record_id": "synoptic:duplicate:980.000000",
                "source_record_id_kind": "derived_station_timestamp",
                "observed_at": 980.0,
                "age_seconds": 20.0,
                "freshness_limit_seconds": 1_200.0,
                "freshness_policy_id": (
                    "stv3-ground-current-freshness-v1"
                ),
                "freshness_policy_known": True,
                "distance_target_km": 0.21,
                "storm_id": "storm-heffen",
                "storm_match_ambiguous": False,
                "wet": True,
                "fresh": True,
            },
            {
                "station_id": "stale-gauge",
                "physical_site_id": "site-stale",
                "physical_site_basis": "provider_identity",
                "counts_as_independent": True,
                "provider": "netatmo",
                "source_record_id": "netatmo:stale:100.000000",
                "source_record_id_kind": "derived_station_timestamp",
                "observed_at": 100.0,
                "age_seconds": 900.0,
                "freshness_limit_seconds": 600.0,
                "freshness_policy_id": (
                    "stv3-ground-current-freshness-v1"
                ),
                "freshness_policy_known": True,
                "distance_target_km": 0.4,
                "storm_id": None,
                "storm_match_ambiguous": False,
                "wet": True,
                "fresh": False,
            },
        ],
    }

    evidence = helper(
        summary,
        "tracker-new-york",
        40.7,
        -74.0,
        evaluated_at=1_000.0,
    )

    assert len(evidence) == 1
    assert evidence[0].provider_id == "netatmo"
    assert evidence[0].physical_site_id == "site-home"
    assert evidence[0].observed_at == 970.0
    assert evidence[0].observation_age_seconds == 30.0
    assert evidence[0].distance_to_tracker_km == 0.2
    assert evidence[0].matched_storm_id == "storm-heffen"
    assert evidence[0].measurement_value is None


def test_current_ground_shadow_recomputes_local_representativeness():
    helper = _load_pure_sensor_function("_current_ground_evidence")
    helper.__globals__.update({
        "Mapping": Mapping,
        "math": math,
        "time": time,
        "hashlib": __import__("hashlib"),
        "GROUND_FRESHNESS_LIMITS_SECONDS": (
            ("metar", 2_100.0),
            ("netatmo", 600.0),
            ("synoptic", 1_200.0),
        ),
        "GROUND_FRESHNESS_POLICY_ID": (
            "stv3-ground-current-freshness-v1"
        ),
        "SourceRecordIdKind": _SourceRecordIdKind,
        "GroundEvidenceKind": SimpleNamespace(
            PRECIPITATION_PRESENCE="precipitation_presence"
        ),
        "CurrentGroundEvidence": lambda **values: SimpleNamespace(**values),
    })

    def row(station_id, site_id, distance):
        return {
            "station_id": station_id,
            "physical_site_id": site_id,
            "physical_site_basis": "provider_identity",
            "counts_as_independent": True,
            "provider": "netatmo",
            "source_record_id": f"netatmo:{station_id}:940.000000",
            "source_record_id_kind": "derived_station_timestamp",
            "observed_at": 940.0,
            "age_seconds": 60.0,
            "freshness_limit_seconds": 600.0,
            "freshness_policy_id": (
                "stv3-ground-current-freshness-v1"
            ),
            "freshness_policy_known": True,
            "distance_target_km": distance,
            "storm_id": None,
            "storm_match_ambiguous": False,
            "wet": True,
            "fresh": True,
        }

    base = {
        "ground_precipitation_observed": True,
        "ground_evaluated_at": 1_000.0,
        "ground_freshness_policy_id": (
            "stv3-ground-current-freshness-v1"
        ),
        "ground_local_radius_km": 10.0,
    }
    one_remote = {
        **base,
        "ground_assessments": [row("remote-a", "site-a", 3.7)],
    }
    two_remote = {
        **base,
        "ground_assessments": [
            row("remote-a", "site-a", 3.7),
            row("remote-b", "site-b", 4.1),
        ],
    }

    assert helper(
        one_remote,
        "tracker-a",
        51.0,
        4.0,
        evaluated_at=1_000.0,
    ) == ()
    assert len(helper(
        two_remote,
        "tracker-a",
        51.0,
        4.0,
        evaluated_at=1_000.0,
    )) == 2
    assert helper(
        {
            **base,
            "ground_assessments": [{
                "wet": True,
                "fresh": True,
                "counts_as_independent": True,
            }],
        },
        "tracker-a",
        51.0,
        4.0,
        evaluated_at=1_000.0,
    ) == ()


def test_current_ground_shadow_rejects_future_stale_and_unknown_provider_rows():
    helper = _load_pure_sensor_function("_current_ground_evidence")
    helper.__globals__.update({
        "Mapping": Mapping,
        "math": math,
        "time": time,
        "hashlib": __import__("hashlib"),
        "GROUND_FRESHNESS_LIMITS_SECONDS": (
            ("metar", 2_100.0),
            ("netatmo", 600.0),
            ("synoptic", 1_200.0),
        ),
        "GROUND_FRESHNESS_POLICY_ID": (
            "stv3-ground-current-freshness-v1"
        ),
        "SourceRecordIdKind": _SourceRecordIdKind,
        "GroundEvidenceKind": SimpleNamespace(
            PRECIPITATION_PRESENCE="precipitation_presence"
        ),
        "CurrentGroundEvidence": lambda **values: SimpleNamespace(**values),
    })

    def row(
        station_id,
        provider,
        observed_at,
        age_seconds,
        freshness_limit,
    ):
        return {
            "station_id": station_id,
            "physical_site_id": f"site-{station_id}",
            "physical_site_basis": "provider_identity",
            "counts_as_independent": True,
            "provider": provider,
            "source_record_id": f"record-{station_id}",
            "source_record_id_kind": "provider_record",
            "observed_at": observed_at,
            "age_seconds": age_seconds,
            "freshness_limit_seconds": freshness_limit,
            "freshness_policy_id": (
                "stv3-ground-current-freshness-v1"
            ),
            "freshness_policy_known": True,
            "distance_target_km": 0.2,
            "storm_id": None,
            "storm_match_ambiguous": False,
            "wet": True,
            "fresh": True,
        }

    summary = {
        "ground_precipitation_observed": True,
        "ground_evaluated_at": 1_000.0,
        "ground_freshness_policy_id": (
            "stv3-ground-current-freshness-v1"
        ),
        "ground_local_radius_km": 10.0,
        "ground_assessments": [
            row("future", "netatmo", 1_010.0, -10.0, 600.0),
            row("stale", "netatmo", 100.0, 900.0, 600.0),
            row("unknown", "mystery", 990.0, 10.0, 600.0),
        ],
    }

    assert helper(
        summary,
        "tracker-a",
        51.0,
        4.0,
        evaluated_at=1_000.0,
    ) == ()


def test_precipitation_sensors_publish_shadow_without_replacing_native_status():
    tree = ast.parse(SENSOR_MODULE.read_text(encoding="utf-8"))
    classes = {
        node.name: node for node in tree.body if isinstance(node, ast.ClassDef)
    }
    source = SENSOR_MODULE.read_text(encoding="utf-8")

    assert "from .engine.tracker_forecast_contract import" in source
    assert "def _attach_tracker_forecast_shadow" in source
    assert "def _current_ground_evidence" in source
    assert 'summary["forecast_semantics_shadow"]' in source
    assert "current_ground=ground" in source
    for class_name in (
        "PrecipitationStatusSensor",
        "TargetPrecipitationStatusSensor",
    ):
        rendered = ast.unparse(classes[class_name])
        assert "_attach_tracker_forecast_shadow" in rendered
        assert "self._current_summary()['status']" in rendered


def test_ground_confirmation_cannot_confirm_a_different_selected_system():
    apply_confirmation = _load_pure_sensor_function(
        "_apply_local_netatmo_confirmation"
    )
    result = {
        "storm_id": "storm-a",
        "tracked_system_storm_id": "storm-a",
    }
    apply_confirmation(result, {
        "ground_confirmed": True,
        "ground_precipitation_observed": True,
        "ground_same_system_required": True,
        "ground_confirmed_storm_ids": ["storm-b"],
        "ground_confidence_percent": 90,
    })

    assert result["status"] == "waargenomen"
    assert result["eta_reliable"] is False
    assert result["ground_selected_system_confirmed"] is False
    assert result["ground_confirmation_mismatch"] is True


def test_ground_confirmation_confirms_the_matching_selected_system():
    apply_confirmation = _load_pure_sensor_function(
        "_apply_local_netatmo_confirmation"
    )
    result = {
        "storm_id": "storm-a",
        "tracked_system_storm_id": "storm-a",
    }
    apply_confirmation(result, {
        "ground_confirmed": True,
        "ground_precipitation_observed": True,
        "ground_same_system_required": True,
        "ground_confirmed_storm_ids": ["storm-a"],
        "ground_confirmation_basis": "meerdere_grondnetwerken",
        "ground_confidence_percent": 95,
        "ground_confirmation_by_storm": {
            "storm-a": {
                "confirmation_score": 0.9,
                "confirmation_basis": "synoptic_grondmeting",
                "confidence_percent": 62,
            },
            "storm-b": {
                "confirmation_score": 1.8,
                "confirmation_basis": "netatmo_grondmeting",
                "confidence_percent": 89,
            },
        },
    })

    assert result["status"] == "bevestigd"
    assert result["eta_reliable"] is True
    assert result["eta_basis"] == "synoptic_grondmeting"
    assert result["forecast_confidence_percent"] == 62
    assert result["ground_selected_system_confirmation_score"] == 0.9
    assert result["ground_selected_system_confirmed"] is True
    assert result["ground_confirmation_mismatch"] is False


def test_pysteps_sensor_exposes_restart_recovery_audit() -> None:
    source = SENSOR_MODULE.read_text(encoding="utf-8")
    block = source.split("class PystepsPreflightSensor", 1)[1].split(
        "class KmiObservatieSensor", 1
    )[0]
    assert "restart_recovery_audit" in block


def test_pysteps_sensor_refreshes_when_dynamic_region_is_added() -> None:
    source = SENSOR_MODULE.read_text(encoding="utf-8")
    block = source[
        source.index("class PystepsPreflightSensor"):
        source.index("class KmiObservatieSensor")
    ]
    assert 'f"{DOMAIN}_targets_updated"' in block


def test_radar_calibration_sensor_publishes_compact_database_summary() -> None:
    source = SENSOR_MODULE.read_text(encoding="utf-8")
    block = source[
        source.index("class RadarCalibrationSensor"):
        source.index("class PystepsPreflightSensor")
    ]
    assert '"full_details": "secundaire_kalibratiedatabase"' in block
    assert "total_system_performance_samples" in block
    assert (
        'return self.hass.data.get(DOMAIN, {}).get("radar_calibration", {})'
        not in block
    )
    assert '"analysis"' not in block
    assert '"profiles"' not in block


def test_dynamic_region_sensors_listen_for_target_moves() -> None:
    """Een secundair target mag niet stale blijven na een enginewissel."""
    tree = ast.parse(SENSOR_MODULE.read_text(encoding="utf-8"))
    classes = {
        node.name: node for node in tree.body if isinstance(node, ast.ClassDef)
    }
    for class_name in ("FictieveTrackerSensor", "RegionEngineSensor"):
        rendered = ast.unparse(classes[class_name])
        assert "_targets_updated" in rendered


def test_region_sensor_exposes_durable_recovery_state() -> None:
    tree = ast.parse(SENSOR_MODULE.read_text(encoding="utf-8"))
    classes = {
        node.name: node for node in tree.body if isinstance(node, ast.ClassDef)
    }
    rendered = ast.unparse(classes["RegionEngineSensor"])
    assert "durable_processing" in rendered
    assert "durable_region_id" in rendered
    assert "recovery_ready" in rendered


def test_region_sensor_reads_precomputed_lossless_schedule_diagnostics() -> None:
    """Een dashboardread mag het computeplan niet opnieuw berekenen."""

    tree = ast.parse(SENSOR_MODULE.read_text(encoding="utf-8"))
    classes = {
        node.name: node for node in tree.body if isinstance(node, ast.ClassDef)
    }
    rendered = ast.unparse(classes["RegionEngineSensor"])
    assert "compute_schedule_diagnostics" in rendered
    assert "refresh_compute_schedule" not in rendered


def test_target_status_distinguishes_lightning_only_from_dry() -> None:
    tree = ast.parse(SENSOR_MODULE.read_text(encoding="utf-8"))
    classes = {
        node.name: node for node in tree.body if isinstance(node, ast.ClassDef)
    }
    rendered = ast.unparse(classes["TargetPrecipitationStatusSensor"])
    assert "_lightning_only_summary" in rendered
    assert "_lightning_update" in rendered
    assert "_lightning_status_update" in rendered


def test_home_status_uses_same_live_region_path_as_person_targets() -> None:
    """Thuis mag niet terugvallen op de verouderde globale stormlijst."""
    tree = ast.parse(SENSOR_MODULE.read_text(encoding="utf-8"))
    classes = {
        node.name: node for node in tree.body if isinstance(node, ast.ClassDef)
    }
    rendered = ast.unparse(classes["PrecipitationStatusSensor"])
    assert "get_engine_for_target" in rendered
    assert "zone.home" in rendered
    assert "region.storm_engine.get_active_storms()" in rendered
    assert "data.get('storms', [])" not in rendered
    assert "_targets_updated" in rendered


def test_target_status_exposes_open_meteo_as_model_context_only() -> None:
    source = SENSOR_MODULE.read_text(encoding="utf-8")
    helper = source.split("def _open_meteo_target_summary", 1)[1].split(
        "def _cardinal_direction", 1
    )[0]

    assert "open_meteo_status" in helper
    assert "open_meteo_max_90m_mm" in helper
    assert "open_meteo_first_wet_minutes" in helper
    assert "open_meteo_cape_max_3h_jkg" in helper
    assert "open_meteo_wind_700hpa_speed_kmh" in helper
    assert "open_meteo_pressure_msl_hpa" in helper
    assert "_open_meteo_update" in source


def test_precipitation_status_sensors_cache_summary_per_state_write() -> None:
    """HA mag de dure geometrie niet apart voor state en attributen uitvoeren."""
    tree = ast.parse(SENSOR_MODULE.read_text(encoding="utf-8"))
    classes = {
        node.name: node for node in tree.body if isinstance(node, ast.ClassDef)
    }
    base = ast.unparse(classes["StormTrackerBaseSensor"])
    assert "self._prepare_update()" in base
    assert "self.hass.loop.call_later(0.1" in base
    for class_name in (
        "PrecipitationStatusSensor",
        "TargetPrecipitationStatusSensor",
    ):
        rendered = ast.unparse(classes[class_name])
        assert "stabilize_eta_continuity" in rendered
        assert "self._last_published_summary" in rendered
        assert "self._current_summary()" in rendered


def test_ground_summary_reads_are_side_effect_free_and_events_are_deduped():
    helper = _load_pure_sensor_function("_netatmo_target_summary")
    summary = {
        "ground_assessments": [{
            "provider": "metar",
            "station_id": "KJFK",
            "fresh": True,
            "distance_target_km": 1.0,
            "storm_id": "storm-a",
        }],
        "ground_same_system_required": True,
        "ground_confirmed": False,
        "ground_status": "regen_waargenomen",
        "ground_confidence_percent": 45,
        "ground_confirmation_score": 0.8,
        "ground_matched_storm_ids": ["storm-a"],
        "ground_confirmed_storm_ids": [],
        "ground_confirmation_by_storm": {},
        "ground_raining_sources": ["metar"],
    }
    helper.__globals__["build_local_netatmo_summary"] = (
        lambda *args, **kwargs: dict(summary)
    )
    helper.__globals__["time"] = type(
        "Clock", (), {"time": staticmethod(lambda: 1_000.0)}
    )
    data = {
        "ground_quality_assessments_pending": [],
        "ground_confirmation_decisions_pending": [],
    }

    for _ in range(100):
        helper(data, None, 51.0, 4.0, "home")

    assert data["ground_quality_assessments_pending"] == []
    assert data["ground_confirmation_decisions_pending"] == []
    assert "ground_audit_decision_keys" not in data

    helper(
        data, None, 51.0, 4.0, "home", record_audit=True
    )
    helper(
        data, None, 51.0, 4.0, "home", record_audit=True
    )

    assert len(data["ground_quality_assessments_pending"]) == 1
    assert len(data["ground_confirmation_decisions_pending"]) == 1


def test_local_netatmo_confirmation_overrides_incoming_eta() -> None:
    source = SENSOR_MODULE.read_text(encoding="utf-8")
    block = source.split(
        "def _apply_local_netatmo_confirmation", 1
    )[1].split("def _lightning_only_summary", 1)[0]

    assert '"status": "bevestigd"' in block
    assert '"lokale_grondmeting_binnen_weersysteem"' in block
    assert '"actuele_lokale_grondmeting"' in block
    assert '"eta_minutes": 0.0' in block
    assert '"ground_confirmation_basis", "netatmo_grondmeting"' in block
