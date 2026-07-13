"""Storm Tracker V3 — tests/test_netatmo.py v0.1.0"""
from __future__ import annotations

import time

import pytest


def _station(station_id, lat, lon, rain_live=None, rain_60min=None,
              wind_strength=None, pressure=None, temperature=None, humidity=None):
    """Bouwt een station-item zoals de echte Netatmo public-data API het teruggeeft."""
    measures = {}
    m1 = {}
    if rain_live is not None:
        m1["rain_live"] = rain_live
    if rain_60min is not None:
        m1["rain_60min"] = rain_60min
    if wind_strength is not None:
        m1["wind_strength"] = wind_strength
    if m1:
        measures["mod_rain"] = m1

    res_types = []
    res_values = []
    if pressure is not None:
        res_types.append("pressure")
        res_values.append(pressure)
    if temperature is not None:
        res_types.append("temperature")
        res_values.append(temperature)
    if humidity is not None:
        res_types.append("humidity")
        res_values.append(humidity)
    if res_types:
        measures["mod_main"] = {
            "type": res_types,
            "res": {"1234567890": res_values},
        }

    return {
        "_id": station_id,
        "place": {"location": [lon, lat]},  # LET OP: Netatmo levert [lon, lat]
        "measures": measures,
    }


def test_parse_observations_basic_rain_station(netatmo_module):
    provider = netatmo_module.NetatmoProvider.__new__(netatmo_module.NetatmoProvider)
    data = {"body": [_station("station-1", 51.026, 4.478, rain_live=1.5)]}

    obs = provider._parse_observations(data)

    assert len(obs) == 1
    assert obs[0].lat == 51.026
    assert obs[0].lon == 4.478
    assert obs[0].rain_mm == 1.5
    assert obs[0].station_id == "station-1"
    assert obs[0].source == "netatmo"
    assert obs[0].obs_type == netatmo_module.ObservationType.RAIN


def test_parse_observations_lon_lat_order_is_not_swapped(netatmo_module):
    """Netatmo's 'location' is [lon, lat] — een regressie hier zou stad/land door elkaar halen."""
    provider = netatmo_module.NetatmoProvider.__new__(netatmo_module.NetatmoProvider)
    data = {"body": [_station("s1", lat=51.026, lon=4.478)]}
    obs = provider._parse_observations(data)
    assert obs[0].lat == pytest.approx(51.026)
    assert obs[0].lon == pytest.approx(4.478)


def test_parse_observations_rain_60min_divided_by_12_for_5min_estimate(netatmo_module):
    provider = netatmo_module.NetatmoProvider.__new__(netatmo_module.NetatmoProvider)
    data = {"body": [_station("s1", 51.0, 4.0, rain_60min=12.0)]}
    obs = provider._parse_observations(data)
    assert obs[0].rain_5min == pytest.approx(1.0)


def test_parse_observations_extracts_pressure_temp_humidity(netatmo_module):
    provider = netatmo_module.NetatmoProvider.__new__(netatmo_module.NetatmoProvider)
    data = {"body": [_station("s1", 51.0, 4.0, pressure=1013.2, temperature=18.5, humidity=72)]}
    obs = provider._parse_observations(data)
    assert obs[0].pressure == 1013.2
    assert obs[0].temperature == 18.5
    assert obs[0].humidity == 72


def test_parse_observations_missing_optional_fields_default_to_none_or_zero(netatmo_module):
    provider = netatmo_module.NetatmoProvider.__new__(netatmo_module.NetatmoProvider)
    data = {"body": [_station("s1", 51.0, 4.0)]}  # geen enkele meting
    obs = provider._parse_observations(data)
    assert obs[0].rain_mm == 0.0
    assert obs[0].pressure is None
    assert obs[0].temperature is None


def test_parse_observations_malformed_station_is_skipped_not_crashed(netatmo_module):
    provider = netatmo_module.NetatmoProvider.__new__(netatmo_module.NetatmoProvider)
    good = _station("s1", 51.0, 4.0, rain_live=0.5)
    bad = {"_id": "s2", "place": {"location": "niet-een-lijst"}, "measures": {}}
    data = {"body": [bad, good]}
    obs = provider._parse_observations(data)
    assert len(obs) == 1
    assert obs[0].station_id == "s1"


def test_parse_observations_empty_body_returns_empty_list(netatmo_module):
    provider = netatmo_module.NetatmoProvider.__new__(netatmo_module.NetatmoProvider)
    obs = provider._parse_observations({"body": []})
    assert obs == []


def test_netatmo_factory_always_supports(netatmo_module):
    assert netatmo_module.NetatmoProviderFactory.supports(0.0, 0.0, 100.0) is True
