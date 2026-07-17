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
