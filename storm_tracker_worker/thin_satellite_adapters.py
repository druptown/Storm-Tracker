"""Direct lightning and satellite adapters for the thin runtime.

The adapters deliberately reuse only the bounded provider fetch/decode
functions.  They do not enter the legacy job, receipt, RAW or audit pipeline.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import hashlib
import io
import math
import os
from types import SimpleNamespace
from typing import Any, Callable, Mapping

from .lightning import eumetsat_li_fetch, goes_glm_fetch
from .artifacts import read_json_artifact
from . import noaa_err_native
from .satellite_radar import goes_rrqpe_fetch, hsaf_h40b_fetch
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
GOES_RRQPE_MAX_AGE_SECONDS = 45 * 60.0
NOAA_ERR_MAX_AGE_SECONDS = 45 * 60.0
HSAF_H40B_MAX_AGE_SECONDS = 90 * 60.0
MAX_RAIN_SAMPLES = 10_000


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


def _areas(request: ThinProviderRequest) -> list[dict[str, float]]:
    return [{
        "center_lat": float(target.latitude),
        "center_lon": float(target.longitude),
        "horizon_km": float(target.observation_radius_km),
    } for target in request.targets]


def _artifact_result(fetch_job: Callable[[Any], Mapping[str, Any]], *, job_id: str, payload: dict[str, Any]) -> Mapping[str, Any]:
    metadata = fetch_job(SimpleNamespace(job_id=job_id, payload=payload))
    if not isinstance(metadata, Mapping):
        raise ValueError("satellite_artifact_metadata_invalid")
    result = read_json_artifact(dict(metadata))
    if not isinstance(result, Mapping):
        raise ValueError("satellite_artifact_invalid")
    return result


def _default_goes_rrqpe_fetch(satellite: int, payload: dict[str, Any]) -> Mapping[str, Any]:
    return _artifact_result(
        goes_rrqpe_fetch,
        job_id=f"thin-goes{satellite}-rrqpe",
        payload={**payload, "satellites": [int(satellite)]},
    )


def _default_hsaf_h40b_fetch(payload: dict[str, Any]) -> Mapping[str, Any]:
    return _artifact_result(
        hsaf_h40b_fetch,
        job_id="thin-hsaf-h40b",
        payload=payload,
    )


def _default_noaa_err_fetch(payload: dict[str, Any]) -> Mapping[str, Any]:
    """Fetch one public global blend and sample only requested target pixels."""
    areas = payload.get("areas")
    if not isinstance(areas, list) or not areas or len(areas) > 64:
        raise ValueError("noaa_err_areas_invalid")
    prefix = noaa_err_native._listing_prefix(datetime.now(timezone.utc))
    listing = noaa_err_native._fetch_listing(prefix)
    selected = noaa_err_native._parse_listing_xml(
        listing.payload,
        expected_prefix=prefix,
    )
    response = noaa_err_native._fetch_object(selected)
    identity = noaa_err_native._inspect_netcdf4_identity(
        response.payload,
        selected=selected,
    )
    samples: list[dict[str, Any]] = []
    import h5py

    with h5py.File(io.BytesIO(response.payload), "r") as product:
        rain = product["RRQPE"]
        quality = product["DQF"]
        for area in areas:
            try:
                latitude = float(area["center_lat"])
                longitude = float(area["center_lon"])
            except (KeyError, TypeError, ValueError) as error:
                raise ValueError("noaa_err_area_invalid") from error
            if not (-60.0 <= latitude <= 70.0 and -180.0 <= longitude < 180.0):
                continue
            row = int(round((70.0 - latitude) / 0.02))
            column = int(round((longitude + 180.0) / 0.02))
            raw = int(rain[row, column])
            dqf = int(quality[row, column])
            if raw == -9990 or dqf in {-1, 1, 3}:
                continue
            rain_rate = raw * 0.1
            if not math.isfinite(rain_rate) or rain_rate < 0.1:
                continue
            samples.append({
                "lat": latitude,
                "lon": longitude,
                "timestamp": float(identity.product_start),
                "rain_rate_mm_h": rain_rate,
                "quality": 1.0 if dqf == 0 else 0.5,
                "source": "noaa_err",
                "unit": "mm/h",
            })
    return {
        "frame_timestamp": float(identity.product_start),
        "product_key_sha256": hashlib.sha256(selected.key.encode("utf-8")).hexdigest(),
        "rain_samples": samples,
    }


def _rain_observations(
    raw: object,
    request: ThinProviderRequest,
    *,
    now: float,
    maximum_age_seconds: float,
    expected_source: str,
) -> tuple[NormalizedObservation, ...]:
    if not isinstance(raw, Mapping):
        raise ValueError("satellite_result_invalid")
    try:
        frame_timestamp = float(raw["frame_timestamp"])
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError("satellite_frame_time_invalid") from error
    if not math.isfinite(frame_timestamp) or frame_timestamp > now + 5 * 60:
        raise ValueError("satellite_frame_time_invalid")
    if now - frame_timestamp > maximum_age_seconds:
        raise ValueError("satellite_frame_stale")
    samples = raw.get("rain_samples")
    if not isinstance(samples, list) or len(samples) > MAX_RAIN_SAMPLES:
        raise ValueError("satellite_rain_samples_invalid")
    result: list[NormalizedObservation] = []
    for item in samples:
        if not isinstance(item, Mapping) or item.get("source") != expected_source:
            raise ValueError("satellite_sample_identity_invalid")
        if item.get("unit", "mm/h") != "mm/h":
            raise ValueError("satellite_sample_unit_invalid")
        try:
            latitude = float(item["lat"])
            longitude = float(item["lon"])
            observed_at = float(item.get("timestamp", frame_timestamp))
            rain_rate = float(item["rain_rate_mm_h"])
            quality_value = float(item.get("quality", 0.0))
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError("satellite_sample_invalid") from error
        if not all(math.isfinite(value) for value in (
            latitude, longitude, observed_at, rain_rate, quality_value,
        )):
            raise ValueError("satellite_sample_invalid")
        if not 0.0 <= quality_value <= 1.0:
            raise ValueError("satellite_sample_quality_invalid")
        quality = (
            ObservationQuality.GOOD
            if quality_value >= 0.75
            else ObservationQuality.SUSPECT
            if quality_value >= 0.4
            else ObservationQuality.POOR
        )
        geometry = item.get("geometry") or {
            "type": "Point",
            "coordinates": [longitude, latitude],
        }
        matched = False
        for target in request.targets:
            if _haversine_km(
                latitude, longitude, target.latitude, target.longitude,
            ) > target.observation_radius_km:
                continue
            matched = True
            try:
                result.append(NormalizedObservation.create(
                    provider_id=request.provider_id,
                    product_id=request.product_id,
                    target_id=target.target_id,
                    kind=ObservationKind.RAIN_RATE,
                    value=rain_rate,
                    unit="mm/h",
                    observed_at=observed_at,
                    received_at=now,
                    latitude=latitude,
                    longitude=longitude,
                    quality=quality,
                    geometry=geometry,
                    metadata={
                        "source": expected_source,
                        "source_quality": quality_value,
                    },
                    maximum_age_seconds=maximum_age_seconds,
                ))
            except ThinObservationError as error:
                raise ValueError(f"satellite_sample_{error}") from error
        if not matched:
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


class GoesRrqpeAdapter:
    def __init__(self, satellite: int, *, fetch_job: Callable[[Any], Mapping[str, Any]] | None = None) -> None:
        if int(satellite) not in {18, 19}:
            raise ValueError("goes_rrqpe_satellite_invalid")
        self._satellite = int(satellite)
        self._fetch_job = fetch_job

    async def fetch(self, request, fetcher, *, now):
        payload = {"areas": _areas(request), "satellites": [self._satellite]}
        result = await asyncio.to_thread(
            self._fetch_job,
            SimpleNamespace(payload=payload),
        ) if self._fetch_job is not None else await asyncio.to_thread(
            _default_goes_rrqpe_fetch,
            self._satellite,
            payload,
        )
        return _rain_observations(
            result,
            request,
            now=float(now),
            maximum_age_seconds=GOES_RRQPE_MAX_AGE_SECONDS,
            expected_source=f"noaa_goes{self._satellite}_rrqpe",
        )


class NoaaErrAdapter:
    def __init__(self, *, fetch_job: Callable[[Any], Mapping[str, Any]] | None = None) -> None:
        self._fetch_job = fetch_job

    async def fetch(self, request, fetcher, *, now):
        payload = {"areas": _areas(request)}
        result = await asyncio.to_thread(
            self._fetch_job,
            SimpleNamespace(payload=payload),
        ) if self._fetch_job is not None else await asyncio.to_thread(
            _default_noaa_err_fetch,
            payload,
        )
        return _rain_observations(
            result,
            request,
            now=float(now),
            maximum_age_seconds=NOAA_ERR_MAX_AGE_SECONDS,
            expected_source="noaa_err",
        )


class HsafH40bAdapter:
    def __init__(self, *, fetch_job: Callable[[Any], Mapping[str, Any]] | None = None) -> None:
        self._fetch_job = fetch_job
        self._uses_environment_credentials = fetch_job is None

    async def fetch(self, request, fetcher, *, now):
        if self._uses_environment_credentials and not all(os.getenv(name) for name in (
            "STV3_HSAF_USERNAME", "STV3_HSAF_PASSWORD",
        )):
            raise ProviderBlocked("credentials_missing")
        payload = {"areas": _areas(request)}
        result = await asyncio.to_thread(
            self._fetch_job,
            SimpleNamespace(payload=payload),
        ) if self._fetch_job is not None else await asyncio.to_thread(
            _default_hsaf_h40b_fetch,
            payload,
        )
        return _rain_observations(
            result,
            request,
            now=float(now),
            maximum_age_seconds=HSAF_H40B_MAX_AGE_SECONDS,
            expected_source="hsaf_h40b",
        )


__all__ = [
    "EumetsatLightningAdapter",
    "GoesGlmAdapter",
    "GoesRrqpeAdapter",
    "HsafH40bAdapter",
    "NoaaErrAdapter",
]
