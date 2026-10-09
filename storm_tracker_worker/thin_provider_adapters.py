"""Direct provider adapters for the thin operational pipeline."""

from __future__ import annotations

from collections import deque
from datetime import datetime, timezone
import io
import json
import math
import os
import xml.etree.ElementTree as ET
from typing import Any, Mapping, Protocol

from .thin_observation import (
    MAX_GROUND_SOURCE_AGE_SECONDS,
    NormalizedObservation,
    ObservationKind,
    ObservationQuality,
)
from .thin_provider_catalog import ThinProviderRequest


OPEN_METEO_ENDPOINT = "https://api.open-meteo.com/v1/forecast"
OPERA_LISTING_ENDPOINT = "https://s3.waw3-1.cloudferro.com/openradar-24h/"
OPERA_OBJECT_BASE = "https://s3.waw3-1.cloudferro.com/openradar-24h"
METAR_ENDPOINT = "https://aviationweather.gov/api/data/metar"
NETATMO_ENDPOINT = "https://api.netatmo.com/api/getpublicdata"


class ProviderBlocked(RuntimeError):
    def __init__(self, reason_code: str, *, ready: bool = True) -> None:
        super().__init__(reason_code)
        self.reason_code = reason_code
        self.ready = bool(ready)


class AsyncHttpFetcher(Protocol):
    async def get_json(
        self,
        url: str,
        *,
        params: Mapping[str, str] | None = None,
        headers: Mapping[str, str] | None = None,
        maximum_bytes: int | None = None,
    ) -> Any: ...

    async def get_bytes(
        self,
        url: str,
        *,
        params: Mapping[str, str] | None = None,
        headers: Mapping[str, str] | None = None,
        maximum_bytes: int | None = None,
    ) -> bytes: ...


class AioHttpFetcher:
    """Bounded HTTP client with no HA or persistence dependency."""

    def __init__(self, *, session: Any = None, timeout_seconds: float = 20.0) -> None:
        self._session = session
        self._timeout_seconds = float(timeout_seconds)

    async def get_bytes(
        self,
        url: str,
        *,
        params: Mapping[str, str] | None = None,
        headers: Mapping[str, str] | None = None,
        maximum_bytes: int | None = None,
    ) -> bytes:
        import aiohttp

        limit = int(maximum_bytes or 8 * 1024 * 1024)
        request_headers = {"Accept-Encoding": "identity", **dict(headers or {})}

        async def _read(session: Any) -> bytes:
            timeout = aiohttp.ClientTimeout(total=self._timeout_seconds)
            async with session.get(
                url,
                params=None if params is None else dict(params),
                headers=request_headers,
                timeout=timeout,
            ) as response:
                if response.status != 200:
                    raise RuntimeError(f"http_status_{response.status}")
                declared = response.headers.get("Content-Length")
                if declared is not None and int(declared) > limit:
                    raise ValueError("response_too_large")
                chunks: list[bytes] = []
                size = 0
                async for chunk in response.content.iter_chunked(64 * 1024):
                    size += len(chunk)
                    if size > limit:
                        raise ValueError("response_too_large")
                    chunks.append(bytes(chunk))
                return b"".join(chunks)

        if self._session is not None:
            return await _read(self._session)
        async with aiohttp.ClientSession() as session:
            return await _read(session)

    async def get_json(
        self,
        url: str,
        *,
        params: Mapping[str, str] | None = None,
        headers: Mapping[str, str] | None = None,
        maximum_bytes: int | None = None,
    ) -> Any:
        payload = await self.get_bytes(
            url,
            params=params,
            headers=headers,
            maximum_bytes=maximum_bytes,
        )
        return json.loads(payload.decode("utf-8"))


def _parse_timestamp(value: Any, *, fallback: float) -> float:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        result = float(value)
    elif isinstance(value, str):
        text = value.strip().replace("Z", "+00:00")
        try:
            result = float(text)
        except ValueError:
            parsed = datetime.fromisoformat(text)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            result = parsed.timestamp()
    else:
        result = float(fallback)
    return result if math.isfinite(result) and result >= 0.0 else float(fallback)


def _optional_float(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _point(longitude: float, latitude: float) -> dict[str, Any]:
    return {"type": "Point", "coordinates": [longitude, latitude]}


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius = 6371.0088
    lat1_r, lat2_r = math.radians(lat1), math.radians(lat2)
    dlat = lat2_r - lat1_r
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2.0) ** 2 + (
        math.cos(lat1_r) * math.cos(lat2_r) * math.sin(dlon / 2.0) ** 2
    )
    return radius * 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))


class OpenMeteoAdapter:
    def __init__(self, *, endpoint: str = OPEN_METEO_ENDPOINT) -> None:
        self._endpoint = endpoint

    async def fetch(
        self,
        request: ThinProviderRequest,
        fetcher: AsyncHttpFetcher,
        *,
        now: float,
    ) -> tuple[NormalizedObservation, ...]:
        targets = request.targets
        params = {
            "latitude": ",".join(f"{item.latitude:.5f}" for item in targets),
            "longitude": ",".join(f"{item.longitude:.5f}" for item in targets),
            "current": "precipitation",
            "minutely_15": "precipitation",
            "hourly": "precipitation_probability,pressure_msl",
            "forecast_minutely_15": "8",
            "forecast_hours": "6",
            "timezone": "UTC",
        }
        payload = await fetcher.get_json(
            self._endpoint,
            params=params,
            maximum_bytes=4 * 1024 * 1024,
        )
        items = payload if isinstance(payload, list) else [payload]
        if len(items) != len(targets) or not all(isinstance(item, Mapping) for item in items):
            raise ValueError("open_meteo_location_count_mismatch")

        observations: list[NormalizedObservation] = []
        for target, item in zip(targets, items):
            minutely = item.get("minutely_15")
            minutely = minutely if isinstance(minutely, Mapping) else {}
            current = item.get("current")
            current = current if isinstance(current, Mapping) else {}
            hourly = item.get("hourly")
            hourly = hourly if isinstance(hourly, Mapping) else {}

            minute_values = minutely.get("precipitation")
            accumulation = (
                _optional_float(minute_values[0])
                if isinstance(minute_values, list) and minute_values
                else _optional_float(current.get("precipitation"))
            )
            minute_times = minutely.get("time")
            observed_at = _parse_timestamp(
                minute_times[0]
                if isinstance(minute_times, list) and minute_times
                else current.get("time"),
                fallback=now,
            )
            common = {
                "provider_id": request.provider_id,
                "product_id": request.product_id,
                "target_id": target.target_id,
                "observed_at": observed_at,
                "received_at": now,
                "latitude": target.latitude,
                "longitude": target.longitude,
                "quality": ObservationQuality.GOOD,
                "geometry": _point(target.longitude, target.latitude),
                "metadata": {"role": "model_guidance"},
            }
            if accumulation is not None:
                observations.append(NormalizedObservation.create(
                    **common,
                    kind=ObservationKind.RAIN_RATE,
                    value=accumulation * 4.0,
                    unit="mm/h",
                ))

            probabilities = hourly.get("precipitation_probability")
            probability = (
                max(value for value in map(_optional_float, probabilities) if value is not None)
                if isinstance(probabilities, list)
                and any(_optional_float(value) is not None for value in probabilities)
                else None
            )
            pressures = hourly.get("pressure_msl")
            pressure = (
                _optional_float(pressures[0])
                if isinstance(pressures, list) and pressures
                else None
            )
            if probability is not None:
                observations.append(NormalizedObservation.create(
                    **common,
                    kind=ObservationKind.RAIN_PROBABILITY,
                    value=probability,
                    unit="%",
                ))
            if pressure is not None:
                observations.append(NormalizedObservation.create(
                    **common,
                    kind=ObservationKind.PRESSURE,
                    value=pressure,
                    unit="hPa",
                ))
        return tuple(observations)


_KMI_COLORS = (
    ((144, 238, 144), 1),
    ((0, 200, 100), 2),
    ((0, 150, 255), 3),
    ((0, 0, 255), 4),
    ((255, 255, 0), 5),
    ((255, 150, 0), 6),
    ((255, 0, 0), 7),
    ((180, 0, 180), 8),
)


def _kmi_level(pixel: tuple[int, int, int, int]) -> int:
    red, green, blue, alpha = pixel
    if alpha < 128:
        return 0
    distance, level = min(
        (
            (red - expected[0]) ** 2
            + (green - expected[1]) ** 2
            + (blue - expected[2]) ** 2,
            value,
        )
        for expected, value in _KMI_COLORS
    )
    return level if distance < 20_000 else 0


class KmiRadarAdapter:
    def __init__(
        self,
        *,
        forecast_url: str | None = None,
        access_enabled: bool = False,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        self._forecast_url = forecast_url
        self._access_enabled = access_enabled
        self._headers = dict(headers or {})

    async def fetch(self, request, fetcher, *, now):
        if not self._access_enabled or not self._forecast_url:
            raise ProviderBlocked("external_access_required")
        payload = await fetcher.get_json(
            self._forecast_url,
            headers=self._headers,
            maximum_bytes=4 * 1024 * 1024,
        )
        sequence = payload.get("animation", {}).get("sequence", [])
        if not isinstance(sequence, list) or not sequence:
            raise ValueError("kmi_sequence_missing")
        frames = [item for item in sequence if isinstance(item, Mapping) and item.get("uri")]
        if not frames:
            raise ValueError("kmi_frame_missing")
        latest = max(frames, key=lambda item: _parse_timestamp(item.get("time"), fallback=0.0))
        observed_at = _parse_timestamp(latest.get("time"), fallback=now)
        image_bytes = await fetcher.get_bytes(
            str(latest["uri"]),
            headers=self._headers,
            maximum_bytes=8 * 1024 * 1024,
        )
        from PIL import Image

        image = Image.open(io.BytesIO(image_bytes)).convert("RGBA")
        width, height = image.size
        observations: list[NormalizedObservation] = []
        for target in request.targets:
            column = min(width - 1, max(0, int((target.longitude + 2.5) / 13.0 * width)))
            row = min(height - 1, max(0, int((53.0 - target.latitude) / 6.5 * height)))
            level = _kmi_level(image.getpixel((column, row)))
            if level <= 0:
                continue
            observations.append(NormalizedObservation.create(
                provider_id=request.provider_id,
                product_id=request.product_id,
                target_id=target.target_id,
                kind=ObservationKind.RADAR_LEVEL,
                value=float(level),
                unit="level",
                observed_at=observed_at,
                received_at=now,
                latitude=target.latitude,
                longitude=target.longitude,
                quality=ObservationQuality.GOOD,
                geometry=_point(target.longitude, target.latitude),
                metadata={"pixel_column": column, "pixel_row": row},
            ))
        return tuple(observations)


def _decode_opera_targets(
    data: bytes,
    request: ThinProviderRequest,
    *,
    received_at: float,
) -> tuple[NormalizedObservation, ...]:
    import h5py
    from pyproj import CRS, Transformer

    observations: list[NormalizedObservation] = []
    with h5py.File(io.BytesIO(data), "r") as h5:
        where = h5["where"].attrs
        projdef = where["projdef"]
        if isinstance(projdef, bytes):
            projdef = projdef.decode("utf-8")
        xscale = float(where["xscale"])
        yscale = float(where["yscale"])
        radar = h5["dataset1/data1/data"]
        quality = h5["dataset1/data1/quality1/data"]
        attributes = h5["dataset1/data1/what"].attrs
        gain = float(attributes["gain"])
        offset = float(attributes["offset"])
        nodata = float(attributes["nodata"])
        undetect = float(attributes["undetect"])
        date_value = h5["what"].attrs.get("date", "")
        time_value = h5["what"].attrs.get("time", "")
        if isinstance(date_value, bytes):
            date_value = date_value.decode()
        if isinstance(time_value, bytes):
            time_value = time_value.decode()
        observed_at = _parse_timestamp(
            f"{date_value[:4]}-{date_value[4:6]}-{date_value[6:8]}"
            f"T{time_value[:2]}:{time_value[2:4]}:{time_value[4:6]}+00:00",
            fallback=received_at,
        )
        transformer = Transformer.from_crs(
            CRS.from_epsg(4326),
            CRS.from_user_input(str(projdef)),
            always_xy=True,
        )
        for target in request.targets:
            x_value, y_value = transformer.transform(target.longitude, target.latitude)
            column = math.floor(x_value / xscale)
            row = math.floor(-y_value / yscale)
            if not (0 <= row < radar.shape[0] and 0 <= column < radar.shape[1]):
                continue
            raw = float(radar[row, column])
            if raw in {nodata, undetect} or not math.isfinite(raw):
                continue
            dbz = raw * gain + offset
            if dbz < 8.0:
                continue
            quality_index = float(quality[row, column])
            observations.append(NormalizedObservation.create(
                provider_id=request.provider_id,
                product_id=request.product_id,
                target_id=target.target_id,
                kind=ObservationKind.REFLECTIVITY,
                value=dbz,
                unit="dBZ",
                observed_at=observed_at,
                received_at=received_at,
                latitude=target.latitude,
                longitude=target.longitude,
                quality=(
                    ObservationQuality.GOOD
                    if quality_index >= 0.5
                    else ObservationQuality.SUSPECT
                ),
                geometry=_point(target.longitude, target.latitude),
                metadata={"quality_index": quality_index},
            ))
    return tuple(observations)


class OperaRadarAdapter:
    def __init__(
        self,
        *,
        listing_url: str = OPERA_LISTING_ENDPOINT,
        object_base_url: str = OPERA_OBJECT_BASE,
    ) -> None:
        self._listing_url = listing_url
        self._object_base_url = object_base_url.rstrip("/")

    async def fetch(self, request, fetcher, *, now):
        date = datetime.fromtimestamp(request.requested_at, tz=timezone.utc)
        prefix = date.strftime("%Y/%m/%d/OPERA/COMP/")
        listing = await fetcher.get_bytes(
            self._listing_url,
            params={"list-type": "2", "prefix": prefix},
            maximum_bytes=4 * 1024 * 1024,
        )
        root = ET.fromstring(listing)
        keys = [
            str(node.text)
            for node in root.iter()
            if node.tag.rsplit("}", 1)[-1] == "Key"
            and node.text
            and node.text.endswith("@0@DBZH.h5")
        ]
        if not keys:
            raise ValueError("opera_product_missing")
        key = max(keys)
        data = await fetcher.get_bytes(
            f"{self._object_base_url}/{key}",
            maximum_bytes=384 * 1024 * 1024,
        )
        return _decode_opera_targets(data, request, received_at=now)


class MetarAdapter:
    def __init__(self, *, endpoint: str = METAR_ENDPOINT) -> None:
        self._endpoint = endpoint

    async def fetch(self, request, fetcher, *, now):
        south = min(item.latitude for item in request.targets) - 2.0
        north = max(item.latitude for item in request.targets) + 2.0
        west = min(item.longitude for item in request.targets) - 3.0
        east = max(item.longitude for item in request.targets) + 3.0
        payload = await fetcher.get_json(
            self._endpoint,
            params={
                # Aviation Weather documents bbox as lat0,lon0,lat1,lon1.
                "bbox": f"{south:.3f},{west:.3f},{north:.3f},{east:.3f}",
                "format": "json",
                "hours": "1",
            },
            maximum_bytes=8 * 1024 * 1024,
        )
        if not isinstance(payload, list):
            raise ValueError("metar_payload_invalid")
        observations: list[NormalizedObservation] = []
        for station in payload:
            if not isinstance(station, Mapping):
                continue
            latitude = _optional_float(station.get("lat"))
            longitude = _optional_float(station.get("lon"))
            if latitude is None or longitude is None:
                continue
            observed_at = _parse_timestamp(station.get("obsTime"), fallback=now)
            station_id = str(station.get("icaoId") or "unknown")
            for target in request.targets:
                if _haversine_km(latitude, longitude, target.latitude, target.longitude) > max(
                    150.0, target.observation_radius_km
                ):
                    continue
                common = {
                    "provider_id": request.provider_id,
                    "product_id": request.product_id,
                    "target_id": target.target_id,
                    "observed_at": observed_at,
                    "received_at": now,
                    "latitude": latitude,
                    "longitude": longitude,
                    "quality": ObservationQuality.GOOD,
                    "geometry": _point(longitude, latitude),
                    "metadata": {"station_id": station_id},
                    "maximum_age_seconds": MAX_GROUND_SOURCE_AGE_SECONDS,
                }
                temperature = _optional_float(station.get("temp"))
                pressure = _optional_float(station.get("altim"))
                if temperature is not None:
                    observations.append(NormalizedObservation.create(
                        **common,
                        kind=ObservationKind.TEMPERATURE,
                        value=temperature,
                        unit="°C",
                    ))
                if pressure is not None:
                    observations.append(NormalizedObservation.create(
                        **common,
                        kind=ObservationKind.PRESSURE,
                        value=pressure,
                        unit="hPa",
                    ))
        return tuple(observations)


class NetatmoAdapter:
    def __init__(self, *, access_token: str | None = None, endpoint: str = NETATMO_ENDPOINT) -> None:
        self._access_token = access_token or os.getenv("STV3_NETATMO_ACCESS_TOKEN")
        self._endpoint = endpoint

    async def fetch(self, request, fetcher, *, now):
        if not self._access_token:
            raise ProviderBlocked("external_access_required")
        payload = await fetcher.get_json(
            self._endpoint,
            params={
                "lat_ne": f"{max(item.latitude for item in request.targets) + 1.0:.5f}",
                "lon_ne": f"{max(item.longitude for item in request.targets) + 1.0:.5f}",
                "lat_sw": f"{min(item.latitude for item in request.targets) - 1.0:.5f}",
                "lon_sw": f"{min(item.longitude for item in request.targets) - 1.0:.5f}",
            },
            headers={"Authorization": f"Bearer {self._access_token}"},
            maximum_bytes=16 * 1024 * 1024,
        )
        observations: list[NormalizedObservation] = []
        for station in payload.get("body", ()) if isinstance(payload, Mapping) else ():
            place = station.get("place", {}) if isinstance(station, Mapping) else {}
            location = place.get("location") if isinstance(place, Mapping) else None
            if not isinstance(location, list) or len(location) != 2:
                continue
            longitude = _optional_float(location[0])
            latitude = _optional_float(location[1])
            if latitude is None or longitude is None:
                continue
            measures = station.get("measures", {})
            for target in request.targets:
                if _haversine_km(latitude, longitude, target.latitude, target.longitude) > target.observation_radius_km:
                    continue
                for measure in measures.values() if isinstance(measures, Mapping) else ():
                    if not isinstance(measure, Mapping):
                        continue
                    rain_live = _optional_float(measure.get("rain_live"))
                    observed_at = _parse_timestamp(measure.get("rain_timeutc"), fallback=now)
                    if rain_live is not None:
                        observations.append(NormalizedObservation.create(
                            provider_id=request.provider_id,
                            product_id=request.product_id,
                            target_id=target.target_id,
                            kind=ObservationKind.RAIN_RATE,
                            value=rain_live,
                            unit="mm/h",
                            observed_at=observed_at,
                            received_at=now,
                            latitude=latitude,
                            longitude=longitude,
                            quality=ObservationQuality.GOOD,
                            geometry=_point(longitude, latitude),
                            metadata={"station_id": str(station.get("_id", "unknown"))},
                        ))
        return tuple(observations)


class BlitzortungAdapter:
    def __init__(self, *, maximum_events: int = 20_000) -> None:
        self._events: deque[tuple[float, float, float, float]] = deque(maxlen=maximum_events)

    def feed_message(self, raw: bytes, *, received_at: float) -> None:
        try:
            payload = json.loads(raw)
            latitude = float(payload["lat"])
            longitude = float(payload["lon"])
            source_time = payload.get("time")
            observed_at = float(source_time) / 1_000_000_000 if source_time else float(received_at)
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            return
        if (
            math.isfinite(latitude)
            and math.isfinite(longitude)
            and math.isfinite(observed_at)
            and -90.0 <= latitude <= 90.0
            and -180.0 <= longitude <= 180.0
        ):
            self._events.append((observed_at, float(received_at), latitude, longitude))

    async def fetch(self, request, fetcher, *, now):
        while self._events and now - self._events[0][0] > 15 * 60:
            self._events.popleft()
        observations: list[NormalizedObservation] = []
        for observed_at, received_at, latitude, longitude in tuple(self._events):
            for target in request.targets:
                if _haversine_km(latitude, longitude, target.latitude, target.longitude) > target.observation_radius_km:
                    continue
                observations.append(NormalizedObservation.create(
                    provider_id=request.provider_id,
                    product_id=request.product_id,
                    target_id=target.target_id,
                    kind=ObservationKind.LIGHTNING_STRIKE,
                    value=1.0,
                    unit="count",
                    observed_at=observed_at,
                    received_at=max(received_at, observed_at),
                    latitude=latitude,
                    longitude=longitude,
                    quality=ObservationQuality.GOOD,
                    geometry=_point(longitude, latitude),
                    metadata={},
                ))
        self._events.clear()
        return tuple(observations)


class KnmiRadarAdapter:
    def __init__(self, *, api_key: str | None = None) -> None:
        self._api_key = api_key or os.getenv("STV3_KNMI_API_KEY")

    async def fetch(self, request, fetcher, *, now):
        if not self._api_key:
            raise ProviderBlocked("external_access_required")
        raise ProviderBlocked("direct_decoder_pending", ready=False)


class DwdRadolanAdapter:
    async def fetch(self, request, fetcher, *, now):
        raise ProviderBlocked("direct_decoder_pending", ready=False)


__all__ = [
    "AioHttpFetcher",
    "AsyncHttpFetcher",
    "BlitzortungAdapter",
    "DwdRadolanAdapter",
    "KmiRadarAdapter",
    "KnmiRadarAdapter",
    "MetarAdapter",
    "NetatmoAdapter",
    "OpenMeteoAdapter",
    "OperaRadarAdapter",
    "ProviderBlocked",
]
