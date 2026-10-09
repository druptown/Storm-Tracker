from __future__ import annotations

from types import SimpleNamespace

import pytest

from storm_tracker_worker.desired_state import Target
from storm_tracker_worker.thin_observation import ObservationKind
from storm_tracker_worker.thin_provider_catalog import ThinProviderRequest
from storm_tracker_worker.thin_satellite_adapters import (
    EumetsatLightningAdapter,
    GoesGlmAdapter,
    GoesRrqpeAdapter,
    HsafH40bAdapter,
    NoaaErrAdapter,
)


NOW = 1_800_000_000.0


def _target(latitude: float, longitude: float) -> Target:
    return Target(
        target_id="fictive",
        display_name="Fictive",
        kind="test",
        available=True,
        latitude=latitude,
        longitude=longitude,
        position_observed_at=NOW,
        location_accuracy_m=10.0,
        observation_radius_km=100.0,
        country_code=None,
    )


def _request(provider_id: str, product_id: str, target: Target) -> ThinProviderRequest:
    return ThinProviderRequest(provider_id, product_id, (target,), NOW)


@pytest.mark.asyncio
async def test_empty_current_eumetsat_scene_is_operational():
    calls = []

    def fetch_job(job: SimpleNamespace):
        calls.append(job.payload)
        return {"observations": [], "product_id": "current-scene"}

    result = await EumetsatLightningAdapter(fetch_job=fetch_job).fetch(
        _request("eumetsat_li", "mtg_li", _target(51.05, 4.42)),
        None,
        now=NOW,
    )

    assert tuple(result) == ()
    assert calls == [{"last_product_id": None}]


@pytest.mark.asyncio
async def test_goes_glm_normalizes_only_current_nearby_flashes():
    def fetch_job(job: SimpleNamespace):
        assert job.payload["satellites"] == [19]
        return {
            "observations": [
                {
                    "obs_type": "lightning",
                    "lat": 40.01,
                    "lon": -75.01,
                    "timestamp": NOW - 30.0,
                    "source": "noaa_goes19_glm",
                },
                {
                    "obs_type": "lightning",
                    "lat": 10.0,
                    "lon": 10.0,
                    "timestamp": NOW - 30.0,
                    "source": "noaa_goes19_glm",
                },
                {
                    "obs_type": "lightning",
                    "lat": 40.01,
                    "lon": -75.01,
                    "timestamp": NOW - 1_000.0,
                    "source": "noaa_goes19_glm",
                },
            ],
            "processed_keys": ["key-1"],
            "status": {"19": "active"},
        }

    observations = await GoesGlmAdapter(19, fetch_job=fetch_job).fetch(
        _request("goes19_glm", "glm_l2", _target(40.0, -75.0)),
        None,
        now=NOW,
    )

    assert len(observations) == 1
    assert observations[0].kind is ObservationKind.LIGHTNING_STRIKE
    assert observations[0].provider_id == "goes19_glm"
    assert observations[0].target_id == "fictive"
    assert observations[0].metadata["source"] == "noaa_goes19_glm"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("adapter", "provider_id", "product_id", "source"),
    (
        (GoesRrqpeAdapter(19, fetch_job=lambda job: {}), "goes19_rrqpe", "rrqpe", "noaa_goes19_rrqpe"),
        (NoaaErrAdapter(fetch_job=lambda job: {}), "noaa_err", "enterprise_rain_rate", "noaa_err"),
        (HsafH40bAdapter(fetch_job=lambda job: {}), "hsaf_h40b", "h40b", "hsaf_h40b"),
    ),
)
async def test_satellite_adapters_emit_current_quantitative_rain_rate(
    adapter,
    provider_id,
    product_id,
    source,
):
    calls = []

    def fetch_job(job: SimpleNamespace):
        calls.append(job.payload)
        return {
            "frame_timestamp": NOW - 60.0,
            "rain_samples": [{
                "lat": 40.01,
                "lon": -75.01,
                "timestamp": NOW - 60.0,
                "rain_rate_mm_h": 2.5,
                "quality": 0.9,
                "source": source,
            }],
        }

    adapter._fetch_job = fetch_job
    observations = await adapter.fetch(
        _request(provider_id, product_id, _target(40.0, -75.0)),
        None,
        now=NOW,
    )

    assert len(observations) == 1
    assert observations[0].kind is ObservationKind.RAIN_RATE
    assert observations[0].value == pytest.approx(2.5)
    assert observations[0].unit == "mm/h"
    assert observations[0].target_id == "fictive"
    assert observations[0].geometry == {
        "type": "Point",
        "coordinates": (-75.01, 40.01),
    }
    assert calls and calls[0]["areas"] == [{
        "center_lat": 40.0,
        "center_lon": -75.0,
        "horizon_km": 100.0,
    }]


@pytest.mark.asyncio
async def test_satellite_adapter_rejects_stale_grid():
    def stale_fetch_job(job: SimpleNamespace):
        return {
            "frame_timestamp": NOW - 4_000.0,
            "rain_samples": [],
        }

    adapter = GoesRrqpeAdapter(19, fetch_job=stale_fetch_job)
    with pytest.raises(ValueError, match="stale"):
        await adapter.fetch(
            _request("goes19_rrqpe", "rrqpe", _target(40.0, -75.0)),
            None,
            now=NOW,
        )


@pytest.mark.asyncio
async def test_satellite_adapter_fails_closed_on_wrong_source_or_unit():
    def invalid_fetch_job(job: SimpleNamespace):
        return {
            "frame_timestamp": NOW - 60.0,
            "rain_samples": [{
                "lat": 40.0,
                "lon": -75.0,
                "timestamp": NOW - 60.0,
                "rain_rate_mm_h": 2.5,
                "unit": "dBZ",
                "quality": 0.9,
                "source": "wrong-source",
            }],
        }

    with pytest.raises(ValueError, match="identity|unit"):
        await GoesRrqpeAdapter(19, fetch_job=invalid_fetch_job).fetch(
            _request("goes19_rrqpe", "rrqpe", _target(40.0, -75.0)),
            None,
            now=NOW,
        )
