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
    precipitation = [forecast] + [0.0] * 12
    return {
        "latitude": 51.0,
        "longitude": 4.0,
        "elevation": 10.0,
        "timezone": "UTC",
        "generationtime_ms": 1.25,
        "current": {"precipitation": current},
        "minutely_15": {
            "precipitation": precipitation,
            "rain": precipitation,
            "showers": [0.0] * 13,
            "cape": [600.0, 1200.0] + [800.0] * 11,
            "lightning_potential": [None, 1.5] + [0.0] * 11,
            "wind_gusts_10m": [35.0, 55.0] + [30.0] * 11,
            "weather_code": [80.0] * 13,
            "freezing_level_height": [2300.0, 2100.0] + [2200.0] * 11,
        },
        "hourly": {
            "precipitation_probability": [20.0, 70.0, 40.0, 10.0, 5.0, 0.0],
            "pressure_msl": [1008.2, 1007.9, 1007.7, 1007.5, 1007.4, 1007.3],
            "lifted_index": [-1.0, -3.5, -2.0, 0.0, 1.0, 1.0],
            "convective_inhibition": [-10.0, -40.0, -15.0, 0.0, 0.0, 0.0],
            "wind_speed_850hPa": [45.0] * 6,
            "wind_direction_850hPa": [230.0] * 6,
            "wind_speed_700hPa": [60.0] * 6,
            "wind_direction_700hPa": [240.0] * 6,
            "relative_humidity_700hPa": [75.0] * 6,
            "cloud_cover": [80.0] * 6,
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
    assert result["target_results"]["home"]["forecast_3h_max_mm"] == 1.2
    assert (
        result["target_results"]["home"][
            "precipitation_probability_max_6h_percent"
        ]
        == 70.0
    )
    assert result["target_results"]["home"]["cape_max_3h_jkg"] == 1200.0
    assert (
        result["target_results"]["home"]["lightning_potential_max_3h"]
        == 1.5
    )
    assert result["target_results"]["home"]["wind_700hpa_speed_kmh"] == 60.0
    assert result["target_results"]["home"]["pressure_msl_hpa"] == 1008.2
    assert result["target_results"]["home"]["convective_guidance_available"]
    assert result["target_results"]["home"]["aloft_wind_guidance_available"]
    assert result["role"] == "model_guidance"
    assert result["requested_variable_count"] == 19
    assert result["fetch_sequence"] == 1
    assert result["last_success_at"]
    assert session.calls[0]["params"]["forecast_minutely_15"] == "13"
    assert session.calls[0]["params"]["forecast_hours"] == "6"
    assert "lightning_potential" in session.calls[0]["params"]["minutely_15"]
    assert "wind_speed_700hPa" in session.calls[0]["params"]["hourly"]
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
    assert first["targets_requested"] == 1
    assert first["total_points"] == 1
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


def test_missing_optional_guidance_stays_unknown(open_meteo_module):
    payload = _location_payload()
    payload["minutely_15"] = {
        "precipitation": [0.0] * 13,
        "rain": [0.0] * 13,
        "showers": [0.0] * 13,
    }
    payload["hourly"] = {}

    result = open_meteo_module._parse_location(
        payload, requested_lat=51.0, requested_lon=4.0
    )

    assert result["forecast_90m_max_mm"] == 0.0
    assert result["cape_max_3h_jkg"] is None
    assert result["pressure_msl_hpa"] is None
    assert result["convective_guidance_available"] is False
    assert result["aloft_wind_guidance_available"] is False
    assert "cape" not in result["available_variables"]
