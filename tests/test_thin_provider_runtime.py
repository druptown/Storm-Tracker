from __future__ import annotations

import asyncio
from dataclasses import dataclass

import pytest

from storm_tracker_worker.desired_state import Target
from storm_tracker_worker.thin_observation import (
    NormalizedObservation,
    ObservationKind,
    ObservationQuality,
)
from storm_tracker_worker.thin_provider_adapters import ProviderBlocked, ProviderStale
from storm_tracker_worker.thin_provider_catalog import ThinProviderRequest
from storm_tracker_worker.thin_provider_runtime import ThinProviderRuntime


RAIN_OBSERVATION = NormalizedObservation.create(
    provider_id="healthy",
    product_id="rain",
    target_id="home",
    kind=ObservationKind.RAIN_RATE,
    value=2.0,
    unit="mm/h",
    observed_at=100.0,
    received_at=101.0,
    latitude=50.85,
    longitude=4.35,
    quality=ObservationQuality.GOOD,
)


def _request(provider_id: str) -> ThinProviderRequest:
    target = Target(
        target_id="home",
        display_name="Home",
        kind="test",
        available=True,
        latitude=50.85,
        longitude=4.35,
        position_observed_at=100.0,
        location_accuracy_m=5.0,
        observation_radius_km=50.0,
        country_code="BE",
    )
    return ThinProviderRequest(provider_id, "rain", (target,), 101.0)


@dataclass
class FixtureAdapter:
    observations: tuple[NormalizedObservation, ...]

    async def fetch(self, request, fetcher, *, now):
        return self.observations


class BrokenAdapter:
    def __init__(self):
        self.attempts = 0

    async def fetch(self, request, fetcher, *, now):
        self.attempts += 1
        raise ValueError("malformed provider response")


class BlockedAdapter:
    async def fetch(self, request, fetcher, *, now):
        raise ProviderBlocked("external_access_required")


class PendingAdapter:
    async def fetch(self, request, fetcher, *, now):
        raise ProviderBlocked("direct_decoder_pending", ready=False)


class StaleAdapter:
    def __init__(self):
        self.attempts = 0

    async def fetch(self, request, fetcher, *, now):
        self.attempts += 1
        raise ProviderStale("product_stale")


@pytest.mark.asyncio
async def test_runtime_isolates_one_malformed_provider():
    broken = BrokenAdapter()
    runtime = ThinProviderRuntime(
        adapters={
            "broken": broken,
            "healthy": FixtureAdapter((RAIN_OBSERVATION,)),
        },
        fetcher=object(),
        max_parallel=8,
        retry_delay_seconds=0.0,
    )

    results = await runtime.fetch((_request("broken"), _request("healthy")))

    assert results["broken"].status == "error"
    assert results["broken"].reason_code == "ValueError"
    assert results["healthy"].observations == (RAIN_OBSERVATION,)
    assert broken.attempts == 3


@pytest.mark.asyncio
async def test_runtime_reports_missing_access_as_blocked_without_retry():
    runtime = ThinProviderRuntime(
        adapters={"kmi_radar": BlockedAdapter()},
        fetcher=object(),
        retry_delay_seconds=0.0,
    )

    results = await runtime.fetch((_request("kmi_radar"),))

    assert results["kmi_radar"].ready is True
    assert results["kmi_radar"].status == "blocked"
    assert results["kmi_radar"].reason_code == "external_access_required"


@pytest.mark.asyncio
async def test_runtime_does_not_call_an_unfinished_adapter_ready():
    runtime = ThinProviderRuntime(
        adapters={"dwd_radolan": PendingAdapter()},
        fetcher=object(),
        retry_delay_seconds=0.0,
    )

    results = await runtime.fetch((_request("dwd_radolan"),))

    assert results["dwd_radolan"].ready is False
    assert results["dwd_radolan"].status == "blocked"
    assert results["dwd_radolan"].reason_code == "direct_decoder_pending"


@pytest.mark.asyncio
async def test_runtime_reports_stale_product_without_retry_or_error_backoff():
    adapter = StaleAdapter()
    runtime = ThinProviderRuntime(
        adapters={"opera_radar": adapter},
        fetcher=object(),
        retry_delay_seconds=0.0,
    )

    results = await runtime.fetch((_request("opera_radar"),))

    assert results["opera_radar"].ready is True
    assert results["opera_radar"].status == "stale"
    assert results["opera_radar"].reason_code == "product_stale"
    assert adapter.attempts == 1


@pytest.mark.asyncio
async def test_runtime_serializes_duplicate_requests_for_one_provider():
    class ConcurrencyProbe:
        def __init__(self):
            self.active = 0
            self.maximum = 0

        async def fetch(self, request, fetcher, *, now):
            self.active += 1
            self.maximum = max(self.maximum, self.active)
            await asyncio.sleep(0)
            self.active -= 1
            return (RAIN_OBSERVATION,)

    probe = ConcurrencyProbe()
    runtime = ThinProviderRuntime(
        adapters={"healthy": probe},
        fetcher=object(),
        max_parallel=8,
    )

    await runtime.fetch((_request("healthy"), _request("healthy")))

    assert probe.maximum == 1
