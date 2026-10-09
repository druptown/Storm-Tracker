"""Direct lightning and satellite adapters for the thin runtime.

The adapters deliberately reuse only the bounded provider fetch/decode
functions.  They do not enter the legacy job, receipt, RAW or audit pipeline.
"""

from __future__ import annotations

import asyncio
import math
import os
from types import SimpleNamespace
from typing import Any, Callable, Mapping

from .lightning import eumetsat_li_fetch, goes_glm_fetch
from .thin_observation import (
    NormalizedObservation,
    ObservationKind,
    ObservationQuality,
    ThinObservationError,
)
from .thin_provider_adapters import ProviderBlocked
from .thin_provider_catalog import ThinProviderRequest


EUMETSAT_MAX_AGE_SECONDS = 30 * 60.0
GOES_GLM_MAX_AGE_SECONDS = 5 * 60.0


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius = 6371.0088
    lat1_r, lat2_r = math.radians(lat1), math.radians(lat2)
    dlat = lat2_r - lat1_r
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2.0) ** 2 + (
        math.cos(lat1_r) * math.cos(lat2_r) * math.sin(dlon / 2.0) ** 2
    )
    return radius * 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))


def _lightning_observations(
    raw: object,
    request: ThinProviderRequest,
    *,
    now: float,
    maximum_age_seconds: float,
    expected_source: str,
) -> tuple[NormalizedObservation, ...]:
    if not isinstance(raw, list) or len(raw) > 4_000:
        raise ValueError("lightning_observations_invalid")
    result: list[NormalizedObservation] = []
    for item in raw:
        if not isinstance(item, Mapping):
            raise ValueError("lightning_observation_invalid")
        if item.get("obs_type") != "lightning" or item.get("source") != expected_source:
            raise ValueError("lightning_observation_identity_invalid")
        try:
            latitude = float(item["lat"])
            longitude = float(item["lon"])
            observed_at = float(item["timestamp"])
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError("lightning_observation_invalid") from error
        if not all(math.isfinite(value) for value in (
            latitude, longitude, observed_at,
        )):
            raise ValueError("lightning_observation_invalid")
        if not (-90.0 <= latitude <= 90.0 and -180.0 <= longitude <= 180.0):
            raise ValueError("lightning_observation_coordinates_invalid")
        if now - observed_at > maximum_age_seconds or observed_at - now > 5 * 60:
            continue
        for target in request.targets:
            if _haversine_km(
                latitude,
                longitude,
                target.latitude,
                target.longitude,
            ) > target.observation_radius_km:
                continue
            try:
                result.append(NormalizedObservation.create(
                    provider_id=request.provider_id,
                    product_id=request.product_id,
                    target_id=target.target_id,
                    kind=ObservationKind.LIGHTNING_STRIKE,
                    value=1.0,
                    unit="count",
                    observed_at=observed_at,
                    received_at=now,
                    latitude=latitude,
                    longitude=longitude,
                    quality=ObservationQuality.GOOD,
                    geometry={
                        "type": "Point",
                        "coordinates": [longitude, latitude],
                    },
                    metadata={"source": expected_source},
                    maximum_age_seconds=maximum_age_seconds,
                ))
            except ThinObservationError:
                continue
    return tuple(result)


class EumetsatLightningAdapter:
    def __init__(
        self,
        *,
        fetch_job: Callable[[Any], Mapping[str, Any]] | None = None,
    ) -> None:
        self._fetch_job = fetch_job or eumetsat_li_fetch
        self._uses_environment_credentials = fetch_job is None
        self._last_product_id: str | None = None

    async def fetch(self, request, fetcher, *, now):
        if self._uses_environment_credentials and not all(os.getenv(name) for name in (
            "STV3_EUMETSAT_CONSUMER_KEY",
            "STV3_EUMETSAT_CONSUMER_SECRET",
        )):
            raise ProviderBlocked("credentials_missing")
        payload = {"last_product_id": self._last_product_id}
        result = await asyncio.to_thread(
            self._fetch_job,
            SimpleNamespace(payload=payload),
        )
        if not isinstance(result, Mapping):
            raise ValueError("eumetsat_li_result_invalid")
        product_id = result.get("product_id")
        if product_id is not None:
            self._last_product_id = str(product_id)
        return _lightning_observations(
            result.get("observations"),
            request,
            now=float(now),
            maximum_age_seconds=EUMETSAT_MAX_AGE_SECONDS,
            expected_source="eumetsat_li",
        )


class GoesGlmAdapter:
    def __init__(
        self,
        satellite: int,
        *,
        fetch_job: Callable[[Any], Mapping[str, Any]] | None = None,
    ) -> None:
        if int(satellite) not in {18, 19}:
            raise ValueError("goes_glm_satellite_invalid")
        self._satellite = int(satellite)
        self._fetch_job = fetch_job or goes_glm_fetch
        self._processed_keys: tuple[str, ...] = ()

    async def fetch(self, request, fetcher, *, now):
        result = await asyncio.to_thread(
            self._fetch_job,
            SimpleNamespace(payload={
                "satellites": [self._satellite],
                "processed_keys": list(self._processed_keys),
            }),
        )
        if not isinstance(result, Mapping):
            raise ValueError("goes_glm_result_invalid")
        processed = result.get("processed_keys")
        if not isinstance(processed, list) or len(processed) > 200:
            raise ValueError("goes_glm_processed_keys_invalid")
        self._processed_keys = tuple(str(value) for value in processed)
        return _lightning_observations(
            result.get("observations"),
            request,
            now=float(now),
            maximum_age_seconds=GOES_GLM_MAX_AGE_SECONDS,
            expected_source=f"noaa_goes{self._satellite}_glm",
        )


__all__ = ["EumetsatLightningAdapter", "GoesGlmAdapter"]

