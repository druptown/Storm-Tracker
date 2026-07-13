"""Storm Tracker V3 — tests/test_knmi.py v0.1.0"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest


def test_pixel_to_latlon_corners(knmi_module):
    lat_tl, lon_tl = knmi_module._pixel_to_latlon(0, 0, width=512, height=512)
    assert lat_tl == pytest.approx(knmi_module.KNMI_LAT_MAX)
    assert lon_tl == pytest.approx(knmi_module.KNMI_LON_MIN)

    lat_br, lon_br = knmi_module._pixel_to_latlon(512, 512, width=512, height=512)
    assert lat_br == pytest.approx(knmi_module.KNMI_LAT_MIN)
    assert lon_br == pytest.approx(knmi_module.KNMI_LON_MAX)


def test_intensity_from_rgba_transparent_is_zero(knmi_module):
    assert knmi_module._intensity_from_rgba(255, 0, 0, a=10) == 0


def test_intensity_from_rgba_red_is_high(knmi_module):
    assert knmi_module._intensity_from_rgba(220, 60, 60, 255) == 7


def test_intensity_from_rgba_dark_blue_is_low(knmi_module):
    assert knmi_module._intensity_from_rgba(10, 10, 150, 255) == 2


def test_knmi_factory_supports_inside_coverage(knmi_module):
    assert knmi_module.KnmiProviderFactory.supports(51.5, 5.0, 200.0) is True


def test_knmi_factory_rejects_far_outside(knmi_module):
    assert knmi_module.KnmiProviderFactory.supports(-33.9, 18.4, 200.0) is False


def test_knmi_factory_create_returns_provider_with_correct_keys(knmi_module):
    factory = knmi_module.KnmiProviderFactory(api_key="key-a", wms_api_key="key-b")
    provider = factory.create(hass=None, center_lat=51.5, center_lon=5.0, radius_km=200.0)
    assert isinstance(provider, knmi_module.KnmiProvider)
    assert provider._api_key == "key-a"
    assert provider._wms_key == "key-b"


def test_current_time_str_rounds_down_to_5_minutes(knmi_module, monkeypatch):
    fixed_now = datetime(2026, 7, 12, 12, 7, 30, tzinfo=timezone.utc)

    class _FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed_now

    monkeypatch.setattr(knmi_module, "datetime", _FixedDatetime)
    provider = knmi_module.KnmiProvider(51.5, 5.0, api_key="k")
    assert provider._current_time_str() == "2026-07-12T12:05:00Z"
