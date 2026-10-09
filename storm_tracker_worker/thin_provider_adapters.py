"""Direct provider adapters for the thin operational pipeline."""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Awaitable, Callable, Iterator, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
import io
import json
import logging
import math
import os
import tarfile
import time
import xml.etree.ElementTree as ET
from typing import Any, Mapping, Protocol
from urllib.parse import quote, urlsplit

from .thin_observation import (
    MAX_GROUND_SOURCE_AGE_SECONDS,
    NormalizedObservation,
    ObservationKind,
    ObservationQuality,
    ThinObservationError,
)
from .thin_provider_catalog import ThinProviderRequest


_LOGGER = logging.getLogger(__name__)


OPEN_METEO_ENDPOINT = "https://api.open-meteo.com/v1/forecast"
OPERA_LISTING_ENDPOINT = "https://s3.waw3-1.cloudferro.com/openradar-24h/"
OPERA_OBJECT_BASE = "https://s3.waw3-1.cloudferro.com/openradar-24h"
METAR_ENDPOINT = "https://aviationweather.gov/api/data/metar"
NETATMO_ENDPOINT = "https://api.netatmo.com/api/getpublicdata"
NETATMO_TOKEN_ENDPOINT = "https://api.netatmo.com/oauth2/token"
MAX_OPERA_SPATIAL_CELLS = 64
DWD_RADOLAN_ENDPOINT = (
    "https://opendata.dwd.de/weather/radar/composite/rv/"
    "composite_rv_LATEST.tar"
)
KNMI_OPEN_DATA_BASE = (
    "https://api.dataplatform.knmi.nl/open-data/v1/datasets/"
    "nl_rdr_data_rtcor_5m/versions/1.0"
)
MAX_NATIONAL_RADAR_SPATIAL_CELLS = 64
MAX_RADAR_FRAME_AGE_SECONDS = 15 * 60
_RADAR_DECODE_SEMAPHORE = asyncio.Semaphore(2)
_KNMI_DOWNLOAD_HOST_SUFFIXES = (
    ".knmi.nl",
    ".amazonaws.com",
    ".googleapis.com",
    ".googleusercontent.com",
    ".cloudfront.net",
    ".blob.core.windows.net",
)


@dataclass(frozen=True, slots=True)
class ThinRadarField:
    """Decoded native radar data held in memory for the current work cycle."""

    provider_id: str
    product_id: str
    observed_at: float
    crs: str
    origin_x_m: float
    origin_y_m: float
    x_resolution_m: float
    y_resolution_m: float
    rain_rate_mm_h: Any
    valid_mask: Any

    def __post_init__(self) -> None:
        import numpy as np

        if not self.provider_id or not self.product_id or not self.crs:
            raise ValueError("radar_field_identity_invalid")
        if not math.isfinite(self.observed_at):
            raise ValueError("radar_field_time_invalid")
        if not all(math.isfinite(value) and value > 0.0 for value in (
            self.x_resolution_m, self.y_resolution_m,
        )):
            raise ValueError("radar_field_resolution_invalid")
        if not all(math.isfinite(value) for value in (
            self.origin_x_m, self.origin_y_m,
        )):
            raise ValueError("radar_field_origin_invalid")
        if not isinstance(self.rain_rate_mm_h, np.ndarray) or not isinstance(self.valid_mask, np.ndarray):
            raise TypeError("radar_field_requires_numpy_arrays")
        if self.rain_rate_mm_h.ndim != 2 or self.valid_mask.shape != self.rain_rate_mm_h.shape:
            raise ValueError("radar_field_shape_invalid")
        if self.rain_rate_mm_h.size == 0:
            raise ValueError("radar_field_empty")
        if not np.all(np.isfinite(self.rain_rate_mm_h[self.valid_mask])):
            raise ValueError("radar_field_valid_values_nonfinite")


@dataclass(frozen=True, slots=True, eq=False)
class ThinAdapterResult(Sequence[NormalizedObservation]):
    """Adapter result that remains a sequence for older direct callers."""

    observations: tuple[NormalizedObservation, ...]
    radar_fields: tuple[ThinRadarField, ...] = ()

    def __iter__(self) -> Iterator[NormalizedObservation]:
        return iter(self.observations)

    def __len__(self) -> int:
        return len(self.observations)

    def __getitem__(self, index: int | slice) -> Any:
        return self.observations[index]

    def __eq__(self, other: object) -> bool:
        if isinstance(other, ThinAdapterResult):
            return self.observations == other.observations and self.radar_fields == other.radar_fields
        return self.observations == other


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

    async def post_json(
        self,
        url: str,
        *,
        data: Mapping[str, str] | None = None,
        headers: Mapping[str, str] | None = None,
        maximum_bytes: int | None = None,
    ) -> Any: ...


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

    async def post_json(
        self,
        url: str,
        *,
        data: Mapping[str, str] | None = None,
        headers: Mapping[str, str] | None = None,
        maximum_bytes: int | None = None,
    ) -> Any:
        import aiohttp

        limit = int(maximum_bytes or 1024 * 1024)
        request_headers = {"Accept-Encoding": "identity", **dict(headers or {})}

        async def _read(session: Any) -> bytes:
            timeout = aiohttp.ClientTimeout(total=self._timeout_seconds)
            async with session.post(
                url,
                data=None if data is None else dict(data),
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
            payload = await _read(self._session)
        else:
            async with aiohttp.ClientSession() as session:
                payload = await _read(session)
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


def _component_polygon_geometry(
    component: list[tuple[int, int]],
    corner_to_lonlat: Any,
) -> dict[str, Any]:
    """Trace the exact outer edge (and holes) of a 4-connected grid shape."""
    edges: set[
        tuple[tuple[int, int], tuple[int, int]]
    ] = set()
    for row, column in component:
        for edge in (
            ((row, column), (row, column + 1)),
            ((row, column + 1), (row + 1, column + 1)),
            ((row + 1, column + 1), (row + 1, column)),
            ((row + 1, column), (row, column)),
        ):
            reverse = (edge[1], edge[0])
            if reverse in edges:
                edges.remove(reverse)
            else:
                edges.add(edge)

    outgoing: dict[tuple[int, int], list[tuple[int, int]]] = {}
    for start, end in edges:
        outgoing.setdefault(start, []).append(end)
    direction_index = {
        (0, 1): 0,
        (1, 0): 1,
        (0, -1): 2,
        (-1, 0): 3,
    }
    # Keep filled cells on the right at vertices where two loops touch.
    turn_rank = {1: 0, 0: 1, 3: 2, 2: 3}
    unused = set(edges)
    rings: list[list[tuple[int, int]]] = []
    while unused:
        first = min(unused)
        unused.remove(first)
        ring = [first[0], first[1]]
        while ring[-1] != ring[0]:
            candidates = [
                (ring[-1], end)
                for end in outgoing.get(ring[-1], ())
                if (ring[-1], end) in unused
            ]
            if not candidates:
                raise ValueError("radar_component_contour_invalid")
            previous = (
                ring[-1][0] - ring[-2][0],
                ring[-1][1] - ring[-2][1],
            )
            previous_index = direction_index[previous]

            def rank(edge: tuple[tuple[int, int], tuple[int, int]]) -> tuple[Any, ...]:
                following = (
                    edge[1][0] - edge[0][0],
                    edge[1][1] - edge[0][1],
                )
                turn = (direction_index[following] - previous_index) % 4
                return turn_rank[turn], edge

            next_edge = min(candidates, key=rank)
            unused.remove(next_edge)
            ring.append(next_edge[1])
            if len(ring) > len(edges) + 1:
                raise ValueError("radar_component_contour_invalid")

        simplified: list[tuple[int, int]] = []
        for index, current in enumerate(ring[:-1]):
            previous = ring[index - 1 if index else -2]
            following = ring[(index + 1) % (len(ring) - 1)]
            if (
                previous[0] == current[0] == following[0]
                or previous[1] == current[1] == following[1]
            ):
                continue
            simplified.append(current)
        if len(simplified) < 3:
            raise ValueError("radar_component_contour_invalid")
        simplified.append(simplified[0])
        rings.append(simplified)

    def signed_area(ring: list[tuple[int, int]]) -> float:
        return sum(
            column1 * row2 - column2 * row1
            for (row1, column1), (row2, column2) in zip(ring, ring[1:])
        ) / 2.0

    rings.sort(key=lambda ring: signed_area(ring), reverse=True)
    if not rings or signed_area(rings[0]) <= 0.0:
        raise ValueError("radar_component_contour_invalid")
    coordinates = []
    for ring in rings:
        projected = []
        for row, column in ring:
            longitude, latitude = corner_to_lonlat(row, column)
            projected.append([float(longitude), float(latitude)])
        coordinates.append(projected)
    return {"type": "Polygon", "coordinates": coordinates}


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
    import numpy as np
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
        projected_crs = CRS.from_user_input(str(projdef))
        transformer = Transformer.from_crs(
            CRS.from_epsg(4326),
            projected_crs,
            always_xy=True,
        )
        inverse_transformer = Transformer.from_crs(
            projected_crs,
            CRS.from_epsg(4326),
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

        # Tracking needs echo geometry around the targets, not only a value at
        # the target coordinate. Reduce the 1-km raster to 5-km blocks before
        # component labelling; this bounds CPU/memory while preserving useful
        # storm motion. Each regional component is emitted once with no target
        # id, so it can feed the map/tracker without biasing target fusion.
        spatial: dict[tuple[int, int], NormalizedObservation] = {}
        block_size = max(1, int(round(5_000.0 / max(xscale, yscale))))
        for target in request.targets:
            center_x, center_y = transformer.transform(target.longitude, target.latitude)
            center_column = math.floor(center_x / xscale)
            center_row = math.floor(-center_y / yscale)
            radius_columns = max(1, math.ceil(target.observation_radius_km * 1_000.0 / xscale))
            radius_rows = max(1, math.ceil(target.observation_radius_km * 1_000.0 / yscale))
            column0 = max(0, center_column - radius_columns)
            column1 = min(radar.shape[1], center_column + radius_columns + 1)
            row0 = max(0, center_row - radius_rows)
            row1 = min(radar.shape[0], center_row + radius_rows + 1)
            if column0 >= column1 or row0 >= row1:
                continue

            raw_slice = np.asarray(radar[row0:row1, column0:column1], dtype=float)
            quality_slice = np.asarray(quality[row0:row1, column0:column1], dtype=float)
            valid = (
                np.isfinite(raw_slice)
                & (raw_slice != nodata)
                & (raw_slice != undetect)
            )
            dbz_slice = np.where(valid, raw_slice * gain + offset, -np.inf)
            height, width = dbz_slice.shape
            padded_height = math.ceil(height / block_size) * block_size
            padded_width = math.ceil(width / block_size) * block_size
            padded_dbz = np.full((padded_height, padded_width), -np.inf, dtype=float)
            padded_quality = np.zeros((padded_height, padded_width), dtype=float)
            padded_dbz[:height, :width] = dbz_slice
            padded_quality[:height, :width] = np.where(valid, quality_slice, 0.0)
            pooled_dbz = padded_dbz.reshape(
                padded_height // block_size,
                block_size,
                padded_width // block_size,
                block_size,
            ).max(axis=(1, 3))
            pooled_quality = padded_quality.reshape(
                padded_height // block_size,
                block_size,
                padded_width // block_size,
                block_size,
            ).max(axis=(1, 3))
            mask = pooled_dbz >= 8.0
            visited = np.zeros(mask.shape, dtype=bool)
            for seed_row, seed_column in np.argwhere(mask):
                seed_row = int(seed_row)
                seed_column = int(seed_column)
                if visited[seed_row, seed_column]:
                    continue
                stack = [(seed_row, seed_column)]
                visited[seed_row, seed_column] = True
                component: list[tuple[int, int]] = []
                while stack:
                    component_row, component_column = stack.pop()
                    component.append((component_row, component_column))
                    for delta_row, delta_column in (
                        (-1, 0),
                        (1, 0),
                        (0, -1),
                        (0, 1),
                    ):
                        neighbour_row = component_row + delta_row
                        neighbour_column = component_column + delta_column
                        if not (
                            0 <= neighbour_row < mask.shape[0]
                            and 0 <= neighbour_column < mask.shape[1]
                            and mask[neighbour_row, neighbour_column]
                            and not visited[neighbour_row, neighbour_column]
                        ):
                            continue
                        visited[neighbour_row, neighbour_column] = True
                        stack.append((neighbour_row, neighbour_column))

                component_rows = [item[0] for item in component]
                component_columns = [item[1] for item in component]
                mean_row = sum(component_rows) / len(component_rows)
                mean_column = sum(component_columns) / len(component_columns)
                pixel_x = column0 + (mean_column + 0.5) * block_size
                pixel_y = row0 + (mean_row + 0.5) * block_size
                longitude, latitude = inverse_transformer.transform(
                    pixel_x * xscale,
                    -pixel_y * yscale,
                )
                minimum_column = column0 + min(component_columns) * block_size
                maximum_column = min(
                    radar.shape[1],
                    column0 + (max(component_columns) + 1) * block_size,
                )
                minimum_row = row0 + min(component_rows) * block_size
                maximum_row = min(
                    radar.shape[0],
                    row0 + (max(component_rows) + 1) * block_size,
                )
                def corner_to_lonlat(
                    grid_row: int,
                    grid_column: int,
                ) -> tuple[float, float]:
                    pixel_column = min(
                        radar.shape[1],
                        column0 + grid_column * block_size,
                    )
                    pixel_row = min(
                        radar.shape[0],
                        row0 + grid_row * block_size,
                    )
                    corner_lon, corner_lat = inverse_transformer.transform(
                        pixel_column * xscale,
                        -pixel_row * yscale,
                    )
                    return float(corner_lon), float(corner_lat)

                geometry = _component_polygon_geometry(
                    component,
                    corner_to_lonlat,
                )
                maximum_dbz = max(
                    float(pooled_dbz[item_row, item_column])
                    for item_row, item_column in component
                )
                mean_quality = sum(
                    float(pooled_quality[item_row, item_column])
                    for item_row, item_column in component
                ) / len(component)
                deduplication_key = (round(latitude * 100), round(longitude * 100))
                candidate = NormalizedObservation.create(
                    provider_id=request.provider_id,
                    product_id=request.product_id,
                    target_id=None,
                    kind=ObservationKind.REFLECTIVITY,
                    value=maximum_dbz,
                    unit="dBZ",
                    observed_at=observed_at,
                    received_at=received_at,
                    latitude=latitude,
                    longitude=longitude,
                    quality=(
                        ObservationQuality.GOOD
                        if mean_quality >= 0.5
                        else ObservationQuality.SUSPECT
                    ),
                    geometry=geometry,
                    metadata={
                        "quality_index": mean_quality,
                        "spatial_cell": True,
                        "block_count": len(component),
                        "block_size_m": block_size * max(xscale, yscale),
                    },
                )
                previous = spatial.get(deduplication_key)
                if previous is None or candidate.value > previous.value:
                    spatial[deduplication_key] = candidate

        observations.extend(
            sorted(
                spatial.values(),
                key=lambda item: item.value,
                reverse=True,
            )[:MAX_OPERA_SPATIAL_CELLS]
        )
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


class NetatmoOAuthTokenProvider:
    """Refresh the worker's existing Netatmo OAuth grant in memory only."""

    def __init__(
        self,
        *,
        fetcher: AsyncHttpFetcher,
        client_id: str | None = None,
        client_secret: str | None = None,
        refresh_token: str | None = None,
        endpoint: str = NETATMO_TOKEN_ENDPOINT,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._fetcher = fetcher
        self._client_id = client_id or os.getenv("STV3_NETATMO_CLIENT_ID")
        self._client_secret = client_secret or os.getenv("STV3_NETATMO_CLIENT_SECRET")
        self._refresh_token = refresh_token or os.getenv("STV3_NETATMO_REFRESH_TOKEN")
        self._endpoint = endpoint
        self._monotonic = monotonic
        self._access_token: str | None = None
        self._expires_at = 0.0

    async def get_access_token(self) -> str | None:
        if self._access_token and self._monotonic() < self._expires_at:
            return self._access_token
        if not all((self._client_id, self._client_secret, self._refresh_token)):
            return None
        payload = await self._fetcher.post_json(
            self._endpoint,
            data={
                "grant_type": "refresh_token",
                "client_id": self._client_id,
                "client_secret": self._client_secret,
                "refresh_token": self._refresh_token,
            },
            maximum_bytes=1024 * 1024,
        )
        token = payload.get("access_token") if isinstance(payload, Mapping) else None
        if not isinstance(token, str) or not token.strip():
            raise ValueError("netatmo_token_response_invalid")
        expires_in = _optional_float(payload.get("expires_in")) if isinstance(payload, Mapping) else None
        self._access_token = token.strip()
        self._expires_at = self._monotonic() + max(60.0, (expires_in or 3600.0) - 30.0)
        return self._access_token


_NETATMO_RES_TYPES: dict[str, tuple[ObservationKind, str]] = {
    "temperature": (ObservationKind.TEMPERATURE, "°C"),
    "humidity": (ObservationKind.RELATIVE_HUMIDITY, "%"),
    "pressure": (ObservationKind.PRESSURE, "hPa"),
}


def _netatmo_measurements(measures: Any) -> tuple[tuple[ObservationKind, float, str, str], ...]:
    """Convert Netatmo's module-oriented response to canonical readings."""

    values: list[tuple[ObservationKind, float, str, str]] = []
    for measure in measures.values() if isinstance(measures, Mapping) else ():
        if not isinstance(measure, Mapping):
            continue
        direct = (
            ("rain_live", ObservationKind.RAIN_RATE, "mm/h", "rain_rate"),
            ("rain_60min", ObservationKind.RAIN_ACCUMULATION, "mm", "rain_60min"),
            ("wind_strength", ObservationKind.WIND_SPEED, "km/h", "wind_speed"),
            ("gust_strength", ObservationKind.WIND_SPEED, "km/h", "wind_gust"),
            ("wind_angle", ObservationKind.WIND_DIRECTION, "°", "wind_direction"),
        )
        for key, kind, unit, label in direct:
            value = _optional_float(measure.get(key))
            if value is not None:
                values.append((kind, value, unit, label))
        measurement_types = measure.get("type")
        results = measure.get("res")
        if not isinstance(measurement_types, list) or not isinstance(results, Mapping) or not results:
            continue
        latest = next(reversed(results.values()))
        if not isinstance(latest, list):
            continue
        for index, type_name in enumerate(measurement_types):
            mapping = _NETATMO_RES_TYPES.get(type_name) if isinstance(type_name, str) else None
            if mapping is None or index >= len(latest):
                continue
            value = _optional_float(latest[index])
            if value is not None:
                values.append((mapping[0], value, mapping[1], type_name))
    return tuple(values)


class NetatmoAdapter:
    def __init__(
        self,
        *,
        access_token: str | None = None,
        access_token_provider: Callable[[], Awaitable[str | None]] | None = None,
        station_id_for_summary: str | None = None,
        endpoint: str = NETATMO_ENDPOINT,
    ) -> None:
        self._access_token = access_token or os.getenv("STV3_NETATMO_ACCESS_TOKEN")
        self._access_token_provider = access_token_provider
        self._station_id_for_summary = (station_id_for_summary or "").strip().lower()
        self._endpoint = endpoint
        self._last_summary_at = 0.0
        self._last_summary_signature: tuple[tuple[str, float], ...] = ()

    async def fetch(self, request, fetcher, *, now):
        access_token = self._access_token
        if not access_token and self._access_token_provider is not None:
            access_token = await self._access_token_provider()
        if not access_token:
            raise ProviderBlocked("external_access_required")
        observations: dict[
            tuple[str, str, ObservationKind, str],
            NormalizedObservation,
        ] = {}
        closest_summary: tuple[float, str, list[NormalizedObservation]] | None = None
        requested_targets = {
            target.target_id: target for target in request.targets
        }
        grouped_targets: list[tuple[Target, ...]] = []
        assigned_ids: set[str] = set()
        for region in request.regions:
            group = tuple(
                requested_targets[target.target_id]
                for target in region.targets
                if target.target_id in requested_targets
            )
            if group:
                grouped_targets.append(group)
                assigned_ids.update(target.target_id for target in group)
        grouped_targets.extend(
            (target,)
            for target in request.targets
            if target.target_id not in assigned_ids
        )
        local_radius_km = min(max(float(request.ground_radius_km), 1.0), 20.0)
        delta = local_radius_km / 111.0
        for group in grouped_targets:
            # Targets in one stable acquisition region share one bounded box.
            # The local ground radius remains independent from the wider
            # warning/radar horizon.
            payload = await fetcher.get_json(
                self._endpoint,
                params={
                    "lat_ne": f"{max(target.latitude for target in group) + delta:.5f}",
                    "lon_ne": f"{max(target.longitude for target in group) + delta:.5f}",
                    "lat_sw": f"{min(target.latitude for target in group) - delta:.5f}",
                    "lon_sw": f"{min(target.longitude for target in group) - delta:.5f}",
                    "filter": "true",
                },
                headers={"Authorization": f"Bearer {access_token}"},
                maximum_bytes=16 * 1024 * 1024,
            )
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
                measurements = _netatmo_measurements(measures)
                station_id = str(station.get("_id", "unknown"))
                for target in group:
                    distance_km = _haversine_km(
                        latitude,
                        longitude,
                        target.latitude,
                        target.longitude,
                    )
                    if distance_km > local_radius_km:
                        continue
                    station_observations: list[NormalizedObservation] = []
                    for kind, value, unit, label in measurements:
                        # Netatmo's rainfall timestamp is the last rainfall
                        # value, not the last station update. The bounded
                        # response itself is the freshness event for all fields.
                        try:
                            observation = NormalizedObservation.create(
                                provider_id=request.provider_id,
                                product_id=request.product_id,
                                target_id=target.target_id,
                                kind=kind,
                                value=value,
                                unit=unit,
                                observed_at=float(now),
                                received_at=now,
                                latitude=latitude,
                                longitude=longitude,
                                quality=ObservationQuality.GOOD,
                                geometry=_point(longitude, latitude),
                                metadata={
                                    "station_id": station_id,
                                    "measurement": label,
                                },
                            )
                        except ThinObservationError:
                            # One corrupt public sensor field must not discard
                            # the other readings from this physical station.
                            continue
                        station_observations.append(observation)
                    selected_station = (
                        bool(self._station_id_for_summary)
                        and station_id.lower() == self._station_id_for_summary
                    )
                    if (
                        self._station_id_for_summary
                        and target.target_id == "home"
                        and not selected_station
                    ):
                        continue
                    for observation in station_observations:
                        key = (
                            target.target_id,
                            station_id,
                            observation.kind,
                            str(observation.metadata.get("measurement", "")),
                        )
                        observations[key] = observation
                    if selected_station and station_observations:
                        closest_summary = (
                            0.0,
                            "geselecteerd station",
                            station_observations,
                        )
                    elif (
                        not self._station_id_for_summary
                        and target.target_id == "home"
                        and station_observations
                        and (
                            closest_summary is None
                            or distance_km < closest_summary[0]
                        )
                    ):
                        closest_summary = (
                            distance_km,
                            target.display_name,
                            station_observations,
                        )
        if closest_summary is not None:
            _, target_name, station_observations = closest_summary
            latest = {item.metadata.get("measurement", item.kind.value): item.value for item in station_observations}
            signature = tuple(sorted((str(key), float(value)) for key, value in latest.items()))
            if signature != self._last_summary_signature or now - self._last_summary_at >= 300.0:
                labels = ", ".join(f"{key}={value:g}" for key, value in signature)
                _LOGGER.info("Netatmo actuele meting nabij %s: %s", target_name, labels)
                self._last_summary_at = float(now)
                self._last_summary_signature = signature
        return tuple(observations.values())


class BlitzortungAdapter:
    def __init__(self, *, maximum_events: int = 20_000) -> None:
        self._events: deque[tuple[float, float, float, float]] = deque(maxlen=maximum_events)
        self._transport_connected = False

    def set_transport_state(self, connected: bool) -> None:
        """Expose the public MQTT transport state to provider health."""
        self._transport_connected = bool(connected)

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
        if not self._transport_connected:
            raise ProviderBlocked("transport_unavailable")
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


def _attribute_text(value: Any) -> str:
    if hasattr(value, "item"):
        value = value.item()
    if isinstance(value, bytes):
        value = value.decode("ascii", errors="strict")
    if not isinstance(value, str) or not value.strip():
        raise ValueError("radar_attribute_invalid")
    return value.strip()


def _attribute_number(value: Any) -> float:
    """Read either a scalar or KNMI's live one-item numeric attribute."""
    if hasattr(value, "size"):
        if int(value.size) != 1:
            raise ValueError("radar_attribute_invalid")
        value = value.reshape(-1)[0]
    elif isinstance(value, (list, tuple)):
        if len(value) != 1:
            raise ValueError("radar_attribute_invalid")
        value = value[0]
    if hasattr(value, "item"):
        value = value.item()
    try:
        result = float(value)
    except (TypeError, ValueError) as error:
        raise ValueError("radar_attribute_invalid") from error
    if not math.isfinite(result):
        raise ValueError("radar_attribute_invalid")
    return result


def _validate_radar_time(observed_at: float, received_at: float, *, prefix: str) -> None:
    if not math.isfinite(observed_at):
        raise ValueError(f"{prefix}_frame_time_invalid")
    if received_at - observed_at > MAX_RADAR_FRAME_AGE_SECONDS:
        raise ValueError(f"{prefix}_frame_stale")
    if observed_at - received_at > 5 * 60:
        raise ValueError(f"{prefix}_frame_time_invalid")


def _national_radar_observations(
    rain_rate: Any,
    valid: Any,
    request: ThinProviderRequest,
    *,
    observed_at: float,
    received_at: float,
    lonlat_to_pixel: Any,
    pixel_to_lonlat: Any,
    pixel_width_m: float,
    pixel_height_m: float,
) -> tuple[NormalizedObservation, ...]:
    """Emit target samples and bounded five-kilometre rain components."""
    import numpy as np

    if rain_rate.ndim != 2 or valid.shape != rain_rate.shape:
        raise ValueError("radar_grid_invalid")
    height, width = rain_rate.shape
    observations: list[NormalizedObservation] = []
    for target in request.targets:
        row_value, column_value = lonlat_to_pixel(target.longitude, target.latitude)
        row = math.floor(row_value)
        column = math.floor(column_value)
        if not (0 <= row < height and 0 <= column < width and valid[row, column]):
            continue
        value = float(rain_rate[row, column])
        if not math.isfinite(value) or value < 0.1:
            continue
        observations.append(NormalizedObservation.create(
            provider_id=request.provider_id,
            product_id=request.product_id,
            target_id=target.target_id,
            kind=ObservationKind.RAIN_RATE,
            value=value,
            unit="mm/h",
            observed_at=observed_at,
            received_at=received_at,
            latitude=target.latitude,
            longitude=target.longitude,
            quality=ObservationQuality.GOOD,
            geometry=_point(target.longitude, target.latitude),
            metadata={"pixel_column": column, "pixel_row": row},
        ))

    block_rows = max(1, int(round(5_000.0 / pixel_height_m)))
    block_columns = max(1, int(round(5_000.0 / pixel_width_m)))
    spatial: dict[tuple[int, int], NormalizedObservation] = {}
    for target in request.targets:
        center_row, center_column = lonlat_to_pixel(target.longitude, target.latitude)
        radius_rows = max(1, math.ceil(target.observation_radius_km * 1_000 / pixel_height_m))
        radius_columns = max(1, math.ceil(target.observation_radius_km * 1_000 / pixel_width_m))
        row0 = max(0, math.floor(center_row) - radius_rows)
        row1 = min(height, math.floor(center_row) + radius_rows + 1)
        column0 = max(0, math.floor(center_column) - radius_columns)
        column1 = min(width, math.floor(center_column) + radius_columns + 1)
        if row0 >= row1 or column0 >= column1:
            continue

        source = np.where(
            valid[row0:row1, column0:column1],
            rain_rate[row0:row1, column0:column1],
            -np.inf,
        )
        source_height, source_width = source.shape
        padded_height = math.ceil(source_height / block_rows) * block_rows
        padded_width = math.ceil(source_width / block_columns) * block_columns
        padded = np.full((padded_height, padded_width), -np.inf, dtype=float)
        padded[:source_height, :source_width] = source
        pooled = padded.reshape(
            padded_height // block_rows,
            block_rows,
            padded_width // block_columns,
            block_columns,
        ).max(axis=(1, 3))
        mask = pooled >= 0.1
        visited = np.zeros(mask.shape, dtype=bool)
        for seed_row, seed_column in np.argwhere(mask):
            seed_row, seed_column = int(seed_row), int(seed_column)
            if visited[seed_row, seed_column]:
                continue
            stack = [(seed_row, seed_column)]
            visited[seed_row, seed_column] = True
            component: list[tuple[int, int]] = []
            while stack:
                component_row, component_column = stack.pop()
                component.append((component_row, component_column))
                for delta_row, delta_column in (
                    (-1, 0),
                    (1, 0),
                    (0, -1),
                    (0, 1),
                ):
                    neighbour_row = component_row + delta_row
                    neighbour_column = component_column + delta_column
                    if (
                        0 <= neighbour_row < mask.shape[0]
                        and 0 <= neighbour_column < mask.shape[1]
                        and mask[neighbour_row, neighbour_column]
                        and not visited[neighbour_row, neighbour_column]
                    ):
                        visited[neighbour_row, neighbour_column] = True
                        stack.append((neighbour_row, neighbour_column))

            component_rows = [item[0] for item in component]
            component_columns = [item[1] for item in component]
            minimum_row = row0 + min(component_rows) * block_rows
            maximum_row = min(height, row0 + (max(component_rows) + 1) * block_rows)
            minimum_column = column0 + min(component_columns) * block_columns
            maximum_column = min(width, column0 + (max(component_columns) + 1) * block_columns)
            center_pixel_row = (minimum_row + maximum_row) / 2.0
            center_pixel_column = (minimum_column + maximum_column) / 2.0
            longitude, latitude = pixel_to_lonlat(center_pixel_row, center_pixel_column)
            def corner_to_lonlat(
                grid_row: int,
                grid_column: int,
            ) -> tuple[float, float]:
                pixel_row = min(
                    height,
                    row0 + grid_row * block_rows,
                )
                pixel_column = min(
                    width,
                    column0 + grid_column * block_columns,
                )
                corner_lon, corner_lat = pixel_to_lonlat(pixel_row, pixel_column)
                return float(corner_lon), float(corner_lat)

            geometry = _component_polygon_geometry(
                component,
                corner_to_lonlat,
            )
            maximum_rate = max(float(pooled[row, column]) for row, column in component)
            key = (round(float(latitude) * 100), round(float(longitude) * 100))
            candidate = NormalizedObservation.create(
                provider_id=request.provider_id,
                product_id=request.product_id,
                target_id=None,
                kind=ObservationKind.RAIN_RATE,
                value=maximum_rate,
                unit="mm/h",
                observed_at=observed_at,
                received_at=received_at,
                latitude=float(latitude),
                longitude=float(longitude),
                quality=ObservationQuality.GOOD,
                geometry=geometry,
                metadata={
                    "spatial_cell": True,
                    "block_count": len(component),
                    "block_size_m": max(
                        block_rows * pixel_height_m,
                        block_columns * pixel_width_m,
                    ),
                },
            )
            previous = spatial.get(key)
            if previous is None or candidate.value > previous.value:
                spatial[key] = candidate

    observations.extend(sorted(
        spatial.values(),
        key=lambda item: item.value,
        reverse=True,
    )[:MAX_NATIONAL_RADAR_SPATIAL_CELLS])
    return tuple(observations)


def _decode_dwd_radolan(
    payload: bytes,
    request: ThinProviderRequest,
    *,
    received_at: float,
) -> tuple[NormalizedObservation, ...]:
    import h5py
    import numpy as np
    from pyproj import CRS, Transformer

    try:
        with tarfile.open(fileobj=io.BytesIO(payload), mode="r:*") as archive:
            members = [
                item for item in archive.getmembers()
                if item.isfile() and item.name.endswith("_000-hd5")
            ]
            if not members:
                raise ValueError("dwd_current_frame_missing")
            member = max(members, key=lambda item: item.name)
            if member.size <= 0 or member.size > 40 * 1024 * 1024:
                raise ValueError("dwd_frame_size_invalid")
            stream = archive.extractfile(member)
            if stream is None:
                raise ValueError("dwd_frame_missing")
            frame = stream.read(40 * 1024 * 1024 + 1)
            if len(frame) != member.size or len(frame) > 40 * 1024 * 1024:
                raise ValueError("dwd_frame_size_invalid")
    except tarfile.TarError as error:
        raise ValueError("dwd_archive_invalid") from error

    with h5py.File(io.BytesIO(frame), "r") as root:
        data = np.asarray(root["dataset1/data1/data"], dtype=float)
        data_what = root["dataset1/data1/what"].attrs
        frame_what = root["dataset1/what"].attrs
        where = root["where"].attrs
        observed_at = datetime.strptime(
            _attribute_text(frame_what["enddate"])
            + _attribute_text(frame_what["endtime"]),
            "%Y%m%d%H%M%S",
        ).replace(tzinfo=timezone.utc).timestamp()
        _validate_radar_time(observed_at, received_at, prefix="dwd")
        gain = float(data_what["gain"])
        offset = float(data_what["offset"])
        nodata = float(data_what["nodata"])
        undetect = float(data_what["undetect"])
        source_valid = (
            np.isfinite(data)
            & (data != nodata)
        )
        rain_rate = np.where(
            source_valid,
            np.maximum(0.0, (data * gain + offset) * 12.0),
            0.0,
        )
        echo_valid = source_valid & (rain_rate >= 0.1)
        xscale = float(where["xscale"])
        yscale = float(where["yscale"])
        projected = CRS.from_user_input(_attribute_text(where["projdef"]))
        forward = Transformer.from_crs(CRS.from_epsg(4326), projected, always_xy=True)
        inverse = Transformer.from_crs(projected, CRS.from_epsg(4326), always_xy=True)
        if "UL_lon" in where and "UL_lat" in where:
            ul_x, ul_y = forward.transform(float(where["UL_lon"]), float(where["UL_lat"]))
        else:
            ul_x, ul_y = 0.0, int(where["ysize"]) * yscale

        def lonlat_to_pixel(longitude: float, latitude: float) -> tuple[float, float]:
            x_value, y_value = forward.transform(longitude, latitude)
            return (ul_y - y_value) / yscale, (x_value - ul_x) / xscale

        def pixel_to_lonlat(row: float, column: float) -> tuple[float, float]:
            return inverse.transform(ul_x + column * xscale, ul_y - row * yscale)

        observations = _national_radar_observations(
            rain_rate,
            echo_valid,
            request,
            observed_at=observed_at,
            received_at=received_at,
            lonlat_to_pixel=lonlat_to_pixel,
            pixel_to_lonlat=pixel_to_lonlat,
            pixel_width_m=xscale,
            pixel_height_m=yscale,
        )
        return ThinAdapterResult(
            observations=observations,
            radar_fields=(ThinRadarField(
                provider_id=request.provider_id,
                product_id=request.product_id,
                observed_at=observed_at,
                crs=projected.to_string(),
                origin_x_m=float(ul_x),
                origin_y_m=float(ul_y),
                x_resolution_m=xscale,
                y_resolution_m=yscale,
                rain_rate_mm_h=rain_rate,
                valid_mask=source_valid,
            ),),
        )


def _parse_knmi_timestamp(value: Any) -> float:
    text = _attribute_text(value).rstrip(".")
    for pattern in ("%d-%b-%Y;%H:%M:%S.%f", "%d-%b-%Y;%H:%M:%S"):
        try:
            return datetime.strptime(text.title(), pattern).replace(tzinfo=timezone.utc).timestamp()
        except ValueError:
            pass
    raise ValueError("knmi_frame_time_invalid")


def _decode_knmi_rtcor(
    payload: bytes,
    request: ThinProviderRequest,
    *,
    received_at: float,
) -> tuple[NormalizedObservation, ...]:
    import h5py
    import numpy as np
    from pyproj import CRS, Transformer

    with h5py.File(io.BytesIO(payload), "r") as root:
        overview = root["overview"].attrs
        geographic = root["geographic"].attrs
        image = root["image1"]
        if _attribute_text(overview["product_group_name"]) != "RAD_NL25_COR_NA":
            raise ValueError("knmi_product_invalid")
        if _attribute_text(image.attrs["image_product_name"]) != "RAD_NL25_COR_NA":
            raise ValueError("knmi_product_invalid")
        if _attribute_text(image.attrs["image_geo_parameter"]) != "PRECIP_[MM]":
            raise ValueError("knmi_quantity_invalid")
        observed_at = _parse_knmi_timestamp(overview["product_datetime_end"])
        _validate_radar_time(observed_at, received_at, prefix="knmi")
        rows = int(_attribute_number(geographic["geo_number_rows"]))
        columns = int(_attribute_number(geographic["geo_number_columns"]))
        if (rows, columns) != (765, 700):
            raise ValueError("knmi_grid_invalid")
        raw = np.asarray(image["image_data"])
        if raw.shape != (rows, columns):
            raise ValueError("knmi_grid_invalid")
        calibration = image["calibration"].attrs
        if _attribute_text(calibration["calibration_formulas"]).replace(" ", "") != "GEO=0.010000*PV+0.000000":
            raise ValueError("knmi_calibration_invalid")
        missing = int(_attribute_number(calibration["calibration_missing_data"]))
        outside = int(_attribute_number(calibration["calibration_out_of_image"]))
        source_valid = (
            (raw != missing)
            & (raw != outside)
        )
        rain_rate = np.where(
            source_valid,
            np.maximum(0.0, raw.astype(float) * 0.01 * 12.0),
            0.0,
        )
        echo_valid = source_valid & (rain_rate >= 0.1)
        pixel_x_km = _attribute_number(geographic["geo_pixel_size_x"])
        pixel_y_km = _attribute_number(geographic["geo_pixel_size_y"])
        column_offset_km = _attribute_number(geographic["geo_column_offset"])
        row_offset_km = _attribute_number(geographic["geo_row_offset"])
        proj4 = _attribute_text(root["geographic/map_projection"].attrs["projection_proj4_params"])
        projected_crs = CRS.from_user_input(proj4)
        geographic_crs = projected_crs.geodetic_crs
        if geographic_crs is None:
            raise ValueError("knmi_projection_invalid")
        forward = Transformer.from_crs(geographic_crs, projected_crs, always_xy=True)
        inverse = Transformer.from_crs(projected_crs, geographic_crs, always_xy=True)

        def lonlat_to_pixel(longitude: float, latitude: float) -> tuple[float, float]:
            x_value, y_value = forward.transform(longitude, latitude)
            return (
                (y_value + row_offset_km) / pixel_y_km,
                (x_value - column_offset_km) / pixel_x_km,
            )

        def pixel_to_lonlat(row: float, column: float) -> tuple[float, float]:
            return inverse.transform(
                column_offset_km + column * pixel_x_km,
                row * pixel_y_km - row_offset_km,
            )

        observations = _national_radar_observations(
            rain_rate,
            echo_valid,
            request,
            observed_at=observed_at,
            received_at=received_at,
            lonlat_to_pixel=lonlat_to_pixel,
            pixel_to_lonlat=pixel_to_lonlat,
            pixel_width_m=abs(pixel_x_km) * 1_000.0,
            pixel_height_m=abs(pixel_y_km) * 1_000.0,
        )
        return ThinAdapterResult(
            observations=observations,
            radar_fields=(ThinRadarField(
                provider_id=request.provider_id,
                product_id=request.product_id,
                observed_at=observed_at,
                crs=projected_crs.to_string(),
                origin_x_m=column_offset_km * 1_000.0,
                origin_y_m=-row_offset_km * 1_000.0,
                x_resolution_m=abs(pixel_x_km) * 1_000.0,
                y_resolution_m=abs(pixel_y_km) * 1_000.0,
                rain_rate_mm_h=rain_rate,
                valid_mask=source_valid,
            ),),
        )


class KnmiRadarAdapter:
    def __init__(
        self,
        *,
        api_key: str | None = None,
        open_data_base: str = KNMI_OPEN_DATA_BASE,
    ) -> None:
        self._api_key = api_key or os.getenv("STV3_KNMI_API_KEY")
        self._open_data_base = open_data_base.rstrip("/")

    async def fetch(self, request, fetcher, *, now):
        if not self._api_key:
            raise ProviderBlocked("external_access_required")
        headers = {"Authorization": self._api_key}
        listing = await fetcher.get_json(
            f"{self._open_data_base}/files",
            params={"maxKeys": "1", "orderBy": "created", "sorting": "desc"},
            headers=headers,
            maximum_bytes=256 * 1024,
        )
        files = listing.get("files") if isinstance(listing, Mapping) else None
        if not isinstance(files, list) or len(files) != 1 or not isinstance(files[0], Mapping):
            raise ValueError("knmi_latest_file_missing")
        filename = files[0].get("filename")
        if (
            not isinstance(filename, str)
            or not filename.startswith("RAD_NL25_RAC_RT_")
            or not filename.endswith(".h5")
            or "/" in filename
            or "\\" in filename
        ):
            raise ValueError("knmi_filename_invalid")
        descriptor = await fetcher.get_json(
            f"{self._open_data_base}/files/{quote(filename, safe='')}/url",
            headers=headers,
            maximum_bytes=256 * 1024,
        )
        download_url = (
            descriptor.get("temporaryDownloadUrl")
            if isinstance(descriptor, Mapping)
            else None
        )
        parsed = urlsplit(download_url if isinstance(download_url, str) else "")
        hostname = (parsed.hostname or "").lower()
        if (
            parsed.scheme != "https"
            or not hostname
            or not any(hostname.endswith(suffix) for suffix in _KNMI_DOWNLOAD_HOST_SUFFIXES)
        ):
            raise ValueError("knmi_download_url_invalid")
        payload = await fetcher.get_bytes(
            download_url,
            headers={},
            maximum_bytes=40 * 1024 * 1024,
        )
        async with _RADAR_DECODE_SEMAPHORE:
            return await asyncio.to_thread(
                _decode_knmi_rtcor,
                payload,
                request,
                received_at=now,
            )


class DwdRadolanAdapter:
    def __init__(self, *, endpoint: str = DWD_RADOLAN_ENDPOINT) -> None:
        self._endpoint = endpoint

    async def fetch(self, request, fetcher, *, now):
        payload = await fetcher.get_bytes(
            self._endpoint,
            maximum_bytes=8 * 1024 * 1024,
        )
        async with _RADAR_DECODE_SEMAPHORE:
            return await asyncio.to_thread(
                _decode_dwd_radolan,
                payload,
                request,
                received_at=now,
            )


__all__ = [
    "AioHttpFetcher",
    "AsyncHttpFetcher",
    "BlitzortungAdapter",
    "DwdRadolanAdapter",
    "KmiRadarAdapter",
    "KnmiRadarAdapter",
    "MetarAdapter",
    "NetatmoAdapter",
    "NetatmoOAuthTokenProvider",
    "OpenMeteoAdapter",
    "OperaRadarAdapter",
    "ProviderBlocked",
    "ThinAdapterResult",
    "ThinRadarField",
]
