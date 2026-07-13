"""Storm Tracker V3 — tests/test_open_meteo.py v0.1.0"""
from __future__ import annotations

import asyncio

import pytest


def test_generate_grid_point_count_matches_grid_rings(open_meteo_module):
    expected_total = sum(count for _, count in open_meteo_module.GRID_RINGS)
    points = open_meteo_module._generate_grid(51.026, 4.478)
    assert len(points) == expected_total


def test_generate_grid_points_are_roughly_within_max_radius(open_meteo_module, distance_module):
    points = open_meteo_module._generate_grid(51.026, 4.478)
    max_radius = max(r for r, _ in open_meteo_module.GRID_RINGS)
    for lat, lon in points:
        d = distance_module.haversine(51.026, 4.478, lat, lon)
        assert d <= max_radius * 1.05, f"gridpunt op {d:.1f}km, verwacht <= {max_radius}km"


class _FakePostResponse:
    def __init__(self, payload, status=200):
        self._payload = payload
        self.status = status

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def json(self, content_type=None):
        return self._payload


class _FakePostSession:
    def __init__(self, payload, status=200):
        self._payload = payload
        self._status = status

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    def post(self, url, json=None):
        return _FakePostResponse(self._payload, self._status)


def test_fetch_detects_rain_from_current_precipitation(open_meteo_module, monkeypatch):
    provider = open_meteo_module.OpenMeteoProvider(51.026, 4.478)
    n = len(provider._points)

    # Simuleer: 1 punt met huidige neerslag, de rest droog
    payload = [
        {"current": {"precipitation": 2.5 if i == 0 else 0.0}, "minutely_15": {"precipitation": []}}
        for i in range(n)
    ]

    monkeypatch.setattr(open_meteo_module.aiohttp, "ClientSession",
                         lambda *a, **kw: _FakePostSession(payload))

    result = asyncio.run(provider.fetch())
    assert result["is_raining"] is True
    assert result["wet_now"] == 1
    assert result["max_precipitation"] == pytest.approx(2.5)
    assert result["gear"] == "HIGH"


def test_fetch_no_precipitation_anywhere_is_dry(open_meteo_module, monkeypatch):
    provider = open_meteo_module.OpenMeteoProvider(51.026, 4.478)
    n = len(provider._points)
    payload = [
        {"current": {"precipitation": 0.0}, "minutely_15": {"precipitation": [0.0, 0.0]}}
        for _ in range(n)
    ]
    monkeypatch.setattr(open_meteo_module.aiohttp, "ClientSession",
                         lambda *a, **kw: _FakePostSession(payload))

    result = asyncio.run(provider.fetch())
    assert result["is_raining"] is False
    assert result["gear"] == "LOW"
    assert result["wet_points"] == 0


def test_fetch_uses_forecast_when_current_is_dry(open_meteo_module, monkeypatch):
    """Een punt zonder huidige neerslag maar met een natte 15-min-forecast telt mee in wet_forecast_90m."""
    provider = open_meteo_module.OpenMeteoProvider(51.026, 4.478)
    n = len(provider._points)
    payload = [
        {"current": {"precipitation": 0.0}, "minutely_15": {"precipitation": [0.0, 1.2] if i == 0 else [0.0]}}
        for i in range(n)
    ]
    monkeypatch.setattr(open_meteo_module.aiohttp, "ClientSession",
                         lambda *a, **kw: _FakePostSession(payload))

    result = asyncio.run(provider.fetch())
    assert result["wet_now"] == 0
    assert result["wet_forecast_90m"] == 1
    assert result["is_raining"] is True, "max(current, forecast) > 0 moet nog steeds is_raining=True geven"


def test_fetch_non_200_keeps_previous_result(open_meteo_module, monkeypatch):
    provider = open_meteo_module.OpenMeteoProvider(51.026, 4.478)
    previous = provider._last_result
    monkeypatch.setattr(open_meteo_module.aiohttp, "ClientSession",
                         lambda *a, **kw: _FakePostSession({}, status=500))

    result = asyncio.run(provider.fetch())
    assert result is previous


# ── Bevinding: __init__.py verwacht 'wet_locations_now', fetch() levert dat niet ──

def test_fetch_result_does_not_contain_wet_locations_now(open_meteo_module, monkeypatch):
    """
    BELANGRIJK — bevinding om met Wim te bespreken: `__init__.py._poll_open_meteo`
    doet `result.get("wet_locations_now", [])` en bouwt daarmee RAIN-Observations
    voor de OFE/StormEngine. Maar de `fetch()`-implementatie in DEZE zip
    (docstring zegt v0.2.0) bouwt dat sleutel helemaal niet op — CHANGELOG.md
    claimt dat "lat/lon van natte punten bijgehouden in wet_locations_now" al
    in v0.3.0 zit, maar dat bestand is niet meegeleverd in deze package.
    Praktisch gevolg: Open-Meteo-observaties bereiken momenteel NOOIT de
    StormEngine, ondanks dat de provider elke 10 minuten gepolld wordt en
    `is_raining`/`wet_now` wel degelijk correct berekend worden. Deze test
    documenteert het huidige (ontbrekende) gedrag; ik heb dit niet zelf
    "gefixt" door de sleutel toe te voegen, want ik weet niet wat de
    bedoelde vorm van elk item is (waarschijnlijk {'lat':.., 'lon':.., 'mm':..}
    naar analogie van hoe __init__.py het uitleest).
    """
    provider = open_meteo_module.OpenMeteoProvider(51.026, 4.478)
    n = len(provider._points)
    payload = [
        {"current": {"precipitation": 5.0}, "minutely_15": {"precipitation": []}}
        for _ in range(n)
    ]
    monkeypatch.setattr(open_meteo_module.aiohttp, "ClientSession",
                         lambda *a, **kw: _FakePostSession(payload))

    result = asyncio.run(provider.fetch())
    assert "wet_locations_now" not in result, (
        "als deze assertie ooit faalt, is het gat gedicht (goed nieuws) — "
        "pas dan gerust ook __init__.py's aanname na"
    )
