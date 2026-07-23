"""Regressietests voor de lijst met geregistreerde sensoren."""

from __future__ import annotations

import ast
from pathlib import Path


SENSOR_MODULE = (
    Path(__file__).parents[1]
    / "custom_components"
    / "storm_tracker_v3"
    / "sensor.py"
)


def test_open_meteo_gear_sensor_is_registered() -> None:
    """Voorkom dat de sensorclass bestaat zonder als HA-entiteit te laden."""
    tree = ast.parse(SENSOR_MODULE.read_text(encoding="utf-8"))

    setup = next(
        node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == "async_setup_platform"
    )
    registered_classes = {
        call.func.id
        for call in ast.walk(setup)
        if isinstance(call, ast.Call) and isinstance(call.func, ast.Name)
    }

    assert "OpenMeteoGearSensor" in registered_classes
    assert "TargetPrecipitationStatusSensor" in registered_classes
    assert "StormMapGeoJsonSensor" in registered_classes


def test_dynamic_region_sensors_listen_for_target_moves() -> None:
    """Een secundair target mag niet stale blijven na een enginewissel."""
    tree = ast.parse(SENSOR_MODULE.read_text(encoding="utf-8"))
    classes = {
        node.name: node for node in tree.body if isinstance(node, ast.ClassDef)
    }
    for class_name in ("FictieveTrackerSensor", "RegionEngineSensor"):
        rendered = ast.unparse(classes[class_name])
        assert "_targets_updated" in rendered


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
