"""Storm Tracker V3 — tests/test_blitzortung.py v0.1.0"""
from __future__ import annotations

import json
import time

import pytest


def test_handle_message_valid_payload_calls_callback(blitzortung_module):
    received = []
    provider = blitzortung_module.BlitzortungProvider(on_observation=lambda obs: received.append(obs))

    payload = json.dumps({"lat": 51.026, "lon": 4.478, "time": 1_700_000_000_000_000_000}).encode()
    provider._handle_message(payload)

    assert len(received) == 1
    obs = received[0]
    assert obs.lat == 51.026
    assert obs.lon == 4.478
    assert obs.obs_type == blitzortung_module.ObservationType.LIGHTNING
    assert obs.source == "blitzortung"
    assert obs.timestamp == pytest.approx(1_700_000_000.0)


def test_handle_message_missing_lat_lon_is_ignored(blitzortung_module):
    received = []
    provider = blitzortung_module.BlitzortungProvider(on_observation=lambda obs: received.append(obs))

    payload = json.dumps({"time": 1_700_000_000_000_000_000}).encode()  # geen lat/lon
    provider._handle_message(payload)

    assert received == []


def test_handle_message_invalid_json_does_not_raise(blitzortung_module):
    provider = blitzortung_module.BlitzortungProvider(on_observation=lambda obs: None)
    provider._handle_message(b"dit is geen geldige JSON")  # mag geen exception geven


def test_handle_message_missing_time_falls_back_to_now(blitzortung_module):
    received = []
    provider = blitzortung_module.BlitzortungProvider(on_observation=lambda obs: received.append(obs))

    payload = json.dumps({"lat": 51.0, "lon": 4.0}).encode()  # geen 'time'
    before = time.time()
    provider._handle_message(payload)
    after = time.time()

    assert len(received) == 1
    assert before <= received[0].timestamp <= after


def test_blitzortung_factory_always_supports(blitzortung_module):
    factory = blitzortung_module.BlitzortungProviderFactory()
    assert factory.supports(0.0, 0.0, 100.0) is True


def test_blitzortung_factory_create_returns_provider(blitzortung_module):
    factory = blitzortung_module.BlitzortungProviderFactory()
    provider = factory.create(hass=None, center_lat=51.0, center_lon=4.0, radius_km=100.0)
    assert isinstance(provider, blitzortung_module.BlitzortungProvider)
