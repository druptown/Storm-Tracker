"""Tests voor de targetgerichte Open-Meteo-broker."""
from __future__ import annotations

import asyncio

import pytest


class _FakeGetResponse:
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


class _FakeGetSession:
    def __init__(self, payload, status=200, headers=None):
        self.payload = payload
        self.status = status
        self.headers = headers
        self.calls = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    def get(self, url, params=None, timeout=None):
        self.calls.append({"url": url, "params": params, "timeout": timeout})
        return _FakeGetResponse(self.payload, self.status, self.headers)


def _location_payload(current=0.0, forecast=0.0):
    return {
        "latitude": 51.0,
        "longitude": 4.0,
        "elevation": 10.0,
        "timezone": "UTC",
        "current": {"precipitation": current},
        "minutely_15": {
            "precipitation": [forecast, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "rain": [forecast, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "showers": [0.0] * 7,
        },
    }


def test_cold_start_is_unknown_and_initializing(open_meteo_module):
    provider = open_meteo_module.OpenMeteoProvider()
    result = provider.last_result

    assert result["provider_status"] == "initializing"
    assert result["gear"] == "INITIALIZING"
    assert result["is_raining"] is None
    assert result["wet_now"] is None
    assert result["fetch_sequence"] == 0
    assert result["last_success_at"] is None


def test_targets_on_same_model_cell_are_deduplicated(open_meteo_module):
    coordinates, indices = open_meteo_module._normalise_targets({
        "home": (51.0001, 4.0001),
        "person": (51.0010, 4.0010),
        "remote": (48.85, 2.35),
    })

    assert len(coordinates) == 2
    assert indices["home"][0] == indices["person"][0]
    assert indices["remote"][0] != indices["home"][0]


def test_fetch_uses_documented_get_and_returns_target_forecasts(
    open_meteo_module, monkeypatch
):
    session = _FakeGetSession([
        _location_payload(current=2.5, forecast=1.2),
        _location_payload(),
    ])
    monkeypatch.setattr(
        open_meteo_module.aiohttp,
        "ClientSession",
        lambda *args, **kwargs: session,
    )
    provider = open_meteo_module.OpenMeteoProvider()
    result = asyncio.run(provider.fetch({
        "home": (51.0, 4.0),
        "remote": (48.85, 2.35),
    }))

    assert result["provider_status"] == "ok"
    assert result["gear"] == "HIGH"
    assert result["targets_requested"] == 2
    assert result["targets_received"] == 2
    assert result["total_points"] == 2
    assert result["wet_now"] == 1
    assert result["wet_forecast_90m"] == 1
    assert result["max_precipitation"] == pytest.approx(2.5)
    assert result["target_results"]["home"]["forecast_90m_max_mm"] == 1.2
    assert result["fetch_sequence"] == 1
    assert result["last_success_at"]
    assert session.calls[0]["params"]["forecast_minutely_15"] == "7"
    assert session.calls[0]["params"]["latitude"] == "51.00000,48.85000"


def test_single_location_response_is_supported(open_meteo_module, monkeypatch):
    session = _FakeGetSession(_location_payload())
    monkeypatch.setattr(
        open_meteo_module.aiohttp,
        "ClientSession",
        lambda *args, **kwargs: session,
    )
    provider = open_meteo_module.OpenMeteoProvider()
    result = asyncio.run(provider.fetch({"home": (51.0, 4.0)}))

    assert result["gear"] == "LOW"
    assert result["is_raining"] is False
    assert result["wet_points"] == 0


def test_home_assistant_shared_session_is_used(open_meteo_module):
    session = _FakeGetSession(_location_payload())
    provider = open_meteo_module.OpenMeteoProvider(session=session)

    result = asyncio.run(provider.fetch({"home": (51.0, 4.0)}))

    assert result["provider_status"] == "ok"
    assert len(session.calls) == 1
    assert session.calls[0]["timeout"].total == open_meteo_module.TIMEOUT_S


def test_cache_is_reused_for_unchanged_targets(open_meteo_module, monkeypatch):
    session = _FakeGetSession(_location_payload())
    monkeypatch.setattr(
        open_meteo_module.aiohttp,
        "ClientSession",
        lambda *args, **kwargs: session,
    )
    provider = open_meteo_module.OpenMeteoProvider()
    targets = {"home": (51.0, 4.0)}

    asyncio.run(provider.fetch(targets))
    asyncio.run(provider.fetch(targets))

    assert len(session.calls) == 1


def test_target_movement_bypasses_cache(open_meteo_module, monkeypatch):
    session = _FakeGetSession(_location_payload())
    monkeypatch.setattr(
        open_meteo_module.aiohttp,
        "ClientSession",
        lambda *args, **kwargs: session,
    )
    provider = open_meteo_module.OpenMeteoProvider()

    asyncio.run(provider.fetch({"person": (51.0, 4.0)}))
    asyncio.run(provider.fetch({"person": (50.85, 4.35)}))

    assert len(session.calls) == 2
    assert provider.last_result["fetch_sequence"] == 2


def test_429_activates_backoff_and_is_not_dry(
    open_meteo_module, monkeypatch
):
    session = _FakeGetSession(
        {}, status=429, headers={"Retry-After": "7200"}
    )
    monkeypatch.setattr(
        open_meteo_module.aiohttp,
        "ClientSession",
        lambda *args, **kwargs: session,
    )
    provider = open_meteo_module.OpenMeteoProvider()
    targets = {"home": (51.0, 4.0)}

    first = asyncio.run(provider.fetch(targets))
    second = asyncio.run(provider.fetch(targets))

    assert first["provider_status"] == "rate_limited"
    assert first["gear"] == "RATE_LIMITED"
    assert first["is_raining"] is None
    assert first["last_http_status"] == 429
    assert first["next_retry_at"]
    assert second["provider_status"] == "rate_limited"
    assert len(session.calls) == 1
    assert provider._backoff_until > open_meteo_module.time.monotonic() + 7000


def test_mismatched_location_count_is_rejected(
    open_meteo_module, monkeypatch
):
    session = _FakeGetSession([_location_payload()])
    monkeypatch.setattr(
        open_meteo_module.aiohttp,
        "ClientSession",
        lambda *args, **kwargs: session,
    )
    provider = open_meteo_module.OpenMeteoProvider()
    result = asyncio.run(provider.fetch({
        "home": (51.0, 4.0),
        "remote": (48.85, 2.35),
    }))

    assert result["provider_status"].startswith("location_count_mismatch")
    assert result["fetch_sequence"] == 0
    assert result["is_raining"] is None
