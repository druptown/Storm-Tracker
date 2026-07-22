"""Tests voor Open-Meteo grid, caching en rate-limitbackoff."""
from __future__ import annotations

import asyncio
import pytest


class _FakePostResponse:
    def __init__(self, payload, status=200, headers=None):
        self._payload = payload
        self.status = status
        self.headers = headers or {}

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def json(self, content_type=None):
        return self._payload


class _FakePostSession:
    def __init__(self, payload, status=200, headers=None):
        self.payload = payload
        self.status = status
        self.headers = headers

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    def post(self, url, json=None):
        return _FakePostResponse(self.payload, self.status, self.headers)


def _payload(count, current=0.0, forecast=0.0):
    return [
        {
            "current": {"precipitation": current if index == 0 else 0.0},
            "minutely_15": {"precipitation": [forecast if index == 0 else 0.0]},
        }
        for index in range(count)
    ]


def test_generate_grid_point_count_matches_grid_rings(open_meteo_module):
    expected = sum(count for _, count in open_meteo_module.GRID_RINGS)
    assert len(open_meteo_module._generate_grid(51.026, 4.478)) == expected == 324


def test_cold_start_cache_is_explicitly_dry_and_initializing(open_meteo_module):
    provider = open_meteo_module.OpenMeteoProvider(51.026, 4.478)
    result = provider.last_result

    assert result["provider_status"] == "initializing"
    assert result["is_raining"] is False
    assert result["wet_locations_now"] == []
    assert result.get("fetch_sequence", 0) == 0


def test_generate_grid_points_are_within_max_radius(open_meteo_module, distance_module):
    points = open_meteo_module._generate_grid(51.026, 4.478)
    max_radius = max(radius for radius, _ in open_meteo_module.GRID_RINGS)
    assert all(
        distance_module.haversine(51.026, 4.478, lat, lon) <= max_radius * 1.05
        for lat, lon in points
    )


def test_fetch_detects_current_and_forecast_rain(open_meteo_module, monkeypatch):
    provider = open_meteo_module.OpenMeteoProvider(51.026, 4.478)
    payload = _payload(len(provider._points), current=2.5, forecast=1.2)
    monkeypatch.setattr(
        open_meteo_module.aiohttp, "ClientSession",
        lambda *args, **kwargs: _FakePostSession(payload),
    )
    result = asyncio.run(provider.fetch())
    assert result["is_raining"] is True
    assert result["wet_now"] == 1
    assert result["wet_forecast_90m"] == 1
    assert result["max_precipitation"] == pytest.approx(2.5)
    assert result["wet_locations_now"][0]["mm"] == 2.5
    assert result["provider_status"] == "ok"


def test_fetch_no_precipitation_is_dry(open_meteo_module, monkeypatch):
    provider = open_meteo_module.OpenMeteoProvider(51.026, 4.478)
    payload = _payload(len(provider._points))
    monkeypatch.setattr(
        open_meteo_module.aiohttp, "ClientSession",
        lambda *args, **kwargs: _FakePostSession(payload),
    )
    result = asyncio.run(provider.fetch())
    assert result["is_raining"] is False
    assert result["wet_points"] == 0


def test_fetch_uses_cache_between_scheduled_polls(open_meteo_module, monkeypatch):
    provider = open_meteo_module.OpenMeteoProvider(51.026, 4.478)
    payload = _payload(len(provider._points))
    calls = 0

    def session(*args, **kwargs):
        nonlocal calls
        calls += 1
        return _FakePostSession(payload)

    monkeypatch.setattr(open_meteo_module.aiohttp, "ClientSession", session)
    asyncio.run(provider.fetch())
    asyncio.run(provider.fetch())
    assert calls == 1


def test_non_200_keeps_previous_result(open_meteo_module, monkeypatch):
    provider = open_meteo_module.OpenMeteoProvider(51.026, 4.478)
    previous = provider.last_result
    monkeypatch.setattr(
        open_meteo_module.aiohttp, "ClientSession",
        lambda *args, **kwargs: _FakePostSession({}, status=500),
    )
    assert asyncio.run(provider.fetch()) is previous


def test_429_activates_backoff_and_honours_retry_after(open_meteo_module, monkeypatch):
    provider = open_meteo_module.OpenMeteoProvider(51.026, 4.478)
    calls = 0

    def session(*args, **kwargs):
        nonlocal calls
        calls += 1
        return _FakePostSession({}, status=429, headers={"Retry-After": "7200"})

    monkeypatch.setattr(open_meteo_module.aiohttp, "ClientSession", session)
    first = asyncio.run(provider.fetch())
    second = asyncio.run(provider.fetch())
    assert first["provider_status"] == "rate_limited"
    assert second is first
    assert calls == 1
    assert provider._backoff_until > open_meteo_module.time.monotonic() + 7000
