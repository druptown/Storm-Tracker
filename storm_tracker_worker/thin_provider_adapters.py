"""Direct provider adapters for the thin operational pipeline."""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Awaitable, Callable, Iterator, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import io
import json
import logging
import math
import os
from pathlib import Path
import re
import tarfile
import time
import xml.etree.ElementTree as ET
from typing import Any, Mapping, Protocol
from urllib.parse import quote, urljoin, urlsplit
from zoneinfo import ZoneInfo

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
GEOSPHERE_INCA_ENDPOINT = (
    "https://dataset.api.hub.geosphere.at/v1/timeseries/forecast/"
    "nowcast-v1-15min-1km"
)
ITALIAMETEO_DPC_HISTORICAL_CATALOG_ENDPOINT = (
    "https://meteohub.agenziaitaliameteo.it/api/datasets/"
    "radar_sri_dpc/opendata"
)
METEOLUX_WEATHER_ENDPOINT = "https://metapi.ana.lu/api/v1/metapp/weather"
OPERA_LISTING_ENDPOINT = "https://s3.waw3-1.cloudferro.com/openradar-24h/"
OPERA_OBJECT_BASE = "https://s3.waw3-1.cloudferro.com/openradar-24h"
METAR_ENDPOINT = "https://aviationweather.gov/api/data/metar"
NETATMO_ENDPOINT = "https://api.netatmo.com/api/getpublicdata"
NETATMO_TOKEN_ENDPOINT = "https://api.netatmo.com/oauth2/token"
WUNDERGROUND_CURRENT_ENDPOINT = (
    "https://api.weather.com/v2/pws/observations/current"
)
KMI_APPV4_ENDPOINT = "https://app.meteo.be/services/appv4/"
KMI_AWS_ENDPOINT = "https://opendata.meteo.be/geoserver/ows"
KMI_APPV4_USER_AGENT = "be.meteo.app"
KMI_APPV4_MAX_FRAME_AGE_SECONDS = 20 * 60
KMI_APPV4_ALLOWED_IMAGE_HOSTS = frozenset({"app.meteo.be", "cdn.meteo.be"})
_KMI_APPV4_REQUEST_SECRET = "r9EnW374jkJ9acc"
MAX_OPERA_SPATIAL_CELLS = 64
OPERA_MAX_FRAME_AGE_SECONDS = 15 * 60
OPERA_KEY_RE = re.compile(
    r"^\d{4}/\d{2}/\d{2}/OPERA/COMP/OPERA@(?P<stamp>\d{8}T\d{4})@0@DBZH\.h5$"
)
DWD_RADOLAN_ENDPOINT = (
    "https://opendata.dwd.de/weather/radar/composite/rv/"
    "composite_rv_LATEST.tar"
)
AEMET_RADAR_ENDPOINT = "https://www.aemet.es/es/api-eltiempo/radar/download/compo"
AEMET_MAX_ARCHIVE_BYTES = 10 * 1024 * 1024
AEMET_MAX_FRAME_BYTES = 5 * 1024 * 1024
AEMET_MAX_FRAME_AGE_SECONDS = 45 * 60
AEMET_FRAME_RE = re.compile(r"^down_radw(\d{12})_4326\.tif$")
METEOFRANCE_RADAR_ENDPOINT = (
    "https://public-api.meteofrance.fr/public/DPRadar/v1/mosaiques/"
    "METROPOLE/observations/LAME_D_EAU/produit?maille=500"
)
METEOFRANCE_TOKEN_ENDPOINT = "https://portail-api.meteofrance.fr/token"
METEOFRANCE_MAX_FRAME_BYTES = 40 * 1024 * 1024
METEOFRANCE_MAX_FRAME_AGE_SECONDS = 25 * 60
DPC_CATALOG_ENDPOINT = (
    "https://radar-api.protezionecivile.it/findLastProductByType"
)
DPC_DOWNLOAD_ENDPOINT = "https://radar-api.protezionecivile.it/downloadProduct"
DPC_ORIGIN = "https://radar.protezionecivile.it"
DPC_MAX_FRAME_BYTES = 25 * 1024 * 1024
DPC_MAX_FRAME_AGE_SECONDS = 20 * 60
DPC_CRS = (
    "+proj=lcc +lat_0=42 +lon_0=12.5 +lat_1=42 +lat_2=42 "
    "+x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs"
)
METOFFICE_BUCKET_ENDPOINT = (
    "https://met-office-radar-obs-data.s3.eu-west-2.amazonaws.com/"
)
METOFFICE_KEY_RE = re.compile(
    r"^radar/\d{4}/\d{2}/\d{2}/"
    r"\d{12}_ODIM_ng_radar_rainrate_composite_1km_UK\.h5$"
)
METOFFICE_MAX_LISTING_BYTES = 2 * 1024 * 1024
METOFFICE_MAX_FRAME_BYTES = 20 * 1024 * 1024
METOFFICE_MAX_FRAME_AGE_SECONDS = 20 * 60
METOFFICE_MINIMUM_X_METRES = -405_000.0
METOFFICE_MAXIMUM_Y_METRES = 1_550_000.0
RAINVIEWER_MANIFEST_ENDPOINT = (
    "https://api.rainviewer.com/public/weather-maps.json"
)
RAINVIEWER_TILE_HOST = "https://tilecache.rainviewer.com"
RAINVIEWER_ZOOM = 5
RAINVIEWER_TILE_SIZE = 256
RAINVIEWER_MAX_FRAME_AGE_SECONDS = 20 * 60
RAINVIEWER_MIN_ALPHA = 130
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


def _radar_grid_id(
    *,
    crs: str,
    origin_x_m: float,
    origin_y_m: float,
    x_resolution_m: float,
    y_resolution_m: float,
    shape: tuple[int, int],
) -> str:
    payload = json.dumps(
        {
            "crs": crs,
            "origin_x_m": float(origin_x_m),
            "origin_y_m": float(origin_y_m),
            "x_resolution_m": float(x_resolution_m),
            "y_resolution_m": float(y_resolution_m),
            "shape": [int(shape[0]), int(shape[1])],
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return "radar-grid:" + hashlib.sha256(payload).hexdigest()


def _radar_content_fingerprint(values: Any, valid_mask: Any) -> str:
    import numpy as np

    digest = hashlib.sha256()
    normalized_values = np.asarray(values, dtype="<f8", order="C")
    normalized_mask = np.asarray(valid_mask, dtype=np.uint8, order="C")
    digest.update(str(normalized_values.shape).encode("ascii"))
    digest.update(normalized_values.tobytes(order="C"))
    digest.update(normalized_mask.tobytes(order="C"))
    return digest.hexdigest()


@dataclass(frozen=True, slots=True)
class RadarFieldIdentity:
    """Scientific identity and issue-time provenance for one radar raster."""

    quantity: str
    unit: str
    quality_kind: str
    processing_version: str
    source_composition: tuple[str, ...]
    canonical_valid_at: float
    received_at: float
    available_at: float
    grid_id: str
    content_fingerprint: str

    def __post_init__(self) -> None:
        if not all((
            self.quantity,
            self.unit,
            self.quality_kind,
            self.processing_version,
            self.grid_id,
        )):
            raise ValueError("radar_field_provenance_invalid")
        if not self.source_composition or any(
            not value for value in self.source_composition
        ):
            raise ValueError("radar_field_source_composition_invalid")
        if not all(math.isfinite(value) for value in (
            self.canonical_valid_at,
            self.received_at,
            self.available_at,
        )):
            raise ValueError("radar_field_provenance_time_invalid")
        if not re.fullmatch(r"[0-9a-f]{64}", self.content_fingerprint):
            raise ValueError("radar_field_content_fingerprint_invalid")

    @classmethod
    def create(
        cls,
        *,
        provider_id: str,
        quantity: str,
        unit: str,
        quality_kind: str,
        processing_version: str,
        source_composition: tuple[str, ...] | None,
        canonical_valid_at: float,
        received_at: float,
        available_at: float,
        crs: str,
        origin_x_m: float,
        origin_y_m: float,
        x_resolution_m: float,
        y_resolution_m: float,
        values: Any,
        valid_mask: Any,
        grid_id: str | None = None,
    ) -> "RadarFieldIdentity":
        return cls(
            quantity=quantity,
            unit=unit,
            quality_kind=quality_kind,
            processing_version=processing_version,
            source_composition=(provider_id,)
            if source_composition is None
            else tuple(source_composition),
            canonical_valid_at=float(canonical_valid_at),
            received_at=float(received_at),
            available_at=float(available_at),
            grid_id=grid_id or _radar_grid_id(
                crs=crs,
                origin_x_m=origin_x_m,
                origin_y_m=origin_y_m,
                x_resolution_m=x_resolution_m,
                y_resolution_m=y_resolution_m,
                shape=tuple(values.shape),
            ),
            content_fingerprint=_radar_content_fingerprint(values, valid_mask),
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
    identity: RadarFieldIdentity | None = None

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
        identity = self.identity
        if identity is None:
            identity = RadarFieldIdentity.create(
                provider_id=self.provider_id,
                quantity="precipitation_rate",
                unit="mm/h",
                quality_kind="source_valid_mask",
                processing_version="legacy-decoder-v1",
                source_composition=(self.provider_id,),
                canonical_valid_at=self.observed_at,
                received_at=self.observed_at,
                available_at=self.observed_at,
                crs=self.crs,
                origin_x_m=self.origin_x_m,
                origin_y_m=self.origin_y_m,
                x_resolution_m=self.x_resolution_m,
                y_resolution_m=self.y_resolution_m,
                values=self.rain_rate_mm_h,
                valid_mask=self.valid_mask,
            )
            object.__setattr__(self, "identity", identity)
        elif not math.isclose(
            identity.canonical_valid_at,
            self.observed_at,
            rel_tol=0.0,
            abs_tol=1e-6,
        ):
            raise ValueError("radar_field_canonical_time_mismatch")

    @property
    def is_quantitative_precipitation(self) -> bool:
        assert self.identity is not None
        return (
            self.identity.quantity.casefold()
            in {"precipitation_rate", "rain_rate", "qpe_rate"}
            and self.identity.unit.casefold().replace(" ", "")
            in {"mm/h", "mmh-1", "mmhr-1"}
        )

    @property
    def motion_compatibility_key(self) -> tuple[object, ...]:
        assert self.identity is not None
        return (
            self.identity.quantity.casefold(),
            self.identity.unit.casefold().replace(" ", ""),
            self.identity.grid_id,
            self.identity.processing_version,
            self.identity.source_composition,
        )


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


class ProviderStale(RuntimeError):
    """The provider transport works, but its newest product is too old."""

    def __init__(self, reason_code: str = "product_stale") -> None:
        super().__init__(reason_code)
        self.reason_code = str(reason_code)


def _projected_crs_in_metres(value: Any):
    """Return an equivalent projected CRS whose coordinate unit is metres."""
    from pyproj import CRS

    source = CRS.from_user_input(value)
    if not source.is_projected:
        raise ValueError("radar_crs_must_be_projected")
    if not isinstance(value, str) or "+proj=" not in value:
        raise ValueError("radar_crs_proj_definition_required")
    if re.search(r"(?:^|\s)\+units=[^\s]+", value):
        metric_definition = re.sub(
            r"(?:^|\s)\+units=[^\s]+",
            " +units=m",
            value,
            count=1,
        ).strip()
    else:
        metric_definition = value.strip() + " +units=m"
    metric = CRS.from_user_input(metric_definition)
    if not metric.axis_info or not math.isclose(
        metric.axis_info[0].unit_conversion_factor,
        1.0,
    ):
        raise ValueError("radar_crs_metre_normalization_failed")
    return metric


class BlockedProviderAdapter:
    """Complete provider wiring whose external data contract is unavailable."""

    def __init__(self, reason_code: str = "external_access_required") -> None:
        self._reason_code = str(reason_code)

    async def fetch(self, request, fetcher, *, now):
        raise ProviderBlocked(self._reason_code)


class HttpStatusError(RuntimeError):
    """HTTP response status preserved for provider-specific semantics."""

    def __init__(self, status_code: int) -> None:
        self.status_code = int(status_code)
        super().__init__(f"http_status_{self.status_code}")


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
        json_data: Mapping[str, Any] | None = None,
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
                    raise HttpStatusError(response.status)
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
        json_data: Mapping[str, Any] | None = None,
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
                json=None if json_data is None else dict(json_data),
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

    def split_repeated_vertices(
        closed_ring: list[tuple[int, int]],
    ) -> list[list[tuple[int, int]]]:
        """Separate a hole that touches its shell at one grid vertex."""

        pending = [closed_ring]
        separated: list[list[tuple[int, int]]] = []
        while pending:
            candidate = pending.pop()
            seen: dict[tuple[int, int], int] = {}
            for index, point in enumerate(candidate[:-1]):
                previous_index = seen.get(point)
                if previous_index is None:
                    seen[point] = index
                    continue
                first = candidate[previous_index:index + 1]
                second = candidate[:previous_index + 1] + candidate[index + 1:]
                if len(first) < 4 or len(second) < 4:
                    raise ValueError("radar_component_contour_invalid")
                pending.extend((first, second))
                break
            else:
                separated.append(candidate)
        return separated

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

        for simple_ring in split_repeated_vertices(ring):
            simplified: list[tuple[int, int]] = []
            for index, current in enumerate(simple_ring[:-1]):
                previous = simple_ring[index - 1 if index else -2]
                following = simple_ring[(index + 1) % (len(simple_ring) - 1)]
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


def _kmi_aws_validated(properties: Mapping[str, Any], field: str) -> bool:
    raw = properties.get("qc_flags")
    if not isinstance(raw, str):
        return False
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return False
    validated = parsed.get("validated") if isinstance(parsed, Mapping) else None
    return (
        isinstance(validated, Mapping)
        and validated.get(field.upper()) is True
    )


class KmiAwsAdapter:
    """Public KMI ten-minute automatic weather-station observations."""

    def __init__(self, *, endpoint: str = KMI_AWS_ENDPOINT) -> None:
        self._endpoint = endpoint

    async def fetch(
        self,
        request: ThinProviderRequest,
        fetcher: AsyncHttpFetcher,
        *,
        now: float,
    ) -> tuple[NormalizedObservation, ...]:
        payload = await fetcher.get_json(
            self._endpoint,
            params={
                "service": "WFS",
                "version": "2.0.0",
                "request": "GetFeature",
                "typeNames": "aws:aws_10min",
                "count": "512",
                "outputFormat": "application/json",
                "sortBy": "timestamp D",
                "srsName": "EPSG:4326",
            },
            maximum_bytes=32 * 1024 * 1024,
        )
        if not isinstance(payload, Mapping):
            raise ValueError("kmi_aws_payload_invalid")
        features = payload.get("features")
        if not isinstance(features, list) or len(features) > 512:
            raise ValueError("kmi_aws_features_invalid")

        newest_by_station: dict[str, tuple[float, Mapping[str, Any], float, float]] = {}
        for feature in features:
            if not isinstance(feature, Mapping):
                continue
            properties = feature.get("properties")
            geometry = feature.get("geometry")
            if not isinstance(properties, Mapping) or not isinstance(geometry, Mapping):
                continue
            coordinates = geometry.get("coordinates")
            if (
                geometry.get("type") != "Point"
                or not isinstance(coordinates, list)
                or len(coordinates) < 2
            ):
                continue
            longitude = _optional_float(coordinates[0])
            latitude = _optional_float(coordinates[1])
            station_value = properties.get("code")
            if latitude is None or longitude is None or station_value is None:
                continue
            station_id = str(station_value).strip()
            if not station_id:
                continue
            observed_at = _parse_timestamp(properties.get("timestamp"), fallback=now)
            previous = newest_by_station.get(station_id)
            if previous is None or observed_at > previous[0]:
                newest_by_station[station_id] = (
                    observed_at,
                    properties,
                    latitude,
                    longitude,
                )

        observations: list[NormalizedObservation] = []
        ground_radius_km = max(0.0, float(request.ground_radius_km))
        measurement_specs = (
            ("temp_dry_shelter_avg", ObservationKind.TEMPERATURE, "°C", "temperature"),
            ("pressure", ObservationKind.PRESSURE, "hPa", "pressure"),
            ("wind_speed_10m", ObservationKind.WIND_SPEED, "m/s", "wind_speed"),
            ("wind_gusts_speed", ObservationKind.WIND_SPEED, "m/s", "wind_gust"),
            ("wind_direction", ObservationKind.WIND_DIRECTION, "°", "wind_direction"),
            (
                "humidity_rel_shelter_avg",
                ObservationKind.RELATIVE_HUMIDITY,
                "%",
                "relative_humidity",
            ),
        )
        for station_id, (
            observed_at,
            properties,
            latitude,
            longitude,
        ) in newest_by_station.items():
            for target in request.targets:
                if _haversine_km(
                    latitude,
                    longitude,
                    target.latitude,
                    target.longitude,
                ) > ground_radius_km:
                    continue
                common = {
                    "provider_id": request.provider_id,
                    "product_id": request.product_id,
                    "target_id": target.target_id,
                    "observed_at": observed_at,
                    "received_at": now,
                    "latitude": latitude,
                    "longitude": longitude,
                    "geometry": _point(longitude, latitude),
                    "maximum_age_seconds": MAX_GROUND_SOURCE_AGE_SECONDS,
                }

                precipitation = _optional_float(properties.get("precip_quantity"))
                if precipitation is not None:
                    quality = (
                        ObservationQuality.GOOD
                        if _kmi_aws_validated(properties, "precip_quantity")
                        else ObservationQuality.UNKNOWN
                    )
                    for kind, value, measurement in (
                        (
                            ObservationKind.RAIN_ACCUMULATION,
                            precipitation,
                            "rain_accumulation_10min",
                        ),
                        (
                            ObservationKind.RAIN_RATE,
                            precipitation * 6.0,
                            "rain_rate_10min_equivalent",
                        ),
                    ):
                        observations.append(NormalizedObservation.create(
                            **common,
                            kind=kind,
                            value=value,
                            unit="mm" if kind is ObservationKind.RAIN_ACCUMULATION else "mm/h",
                            quality=quality,
                            metadata={
                                "station_id": station_id,
                                "measurement": measurement,
                                "accumulation_window_seconds": 600,
                            },
                        ))

                for field, kind, unit, measurement in measurement_specs:
                    value = _optional_float(properties.get(field))
                    if value is None:
                        continue
                    observations.append(NormalizedObservation.create(
                        **common,
                        kind=kind,
                        value=value,
                        unit=unit,
                        quality=(
                            ObservationQuality.GOOD
                            if _kmi_aws_validated(properties, field)
                            else ObservationQuality.UNKNOWN
                        ),
                        metadata={
                            "station_id": station_id,
                            "measurement": measurement,
                        },
                    ))
        return tuple(observations)


class ItaliaMeteoIcon2iAdapter:
    """Current target guidance from Open-Meteo's ItaliaMeteo ICON-2I."""

    MODEL_ID = "italia_meteo_arpae_icon_2i"
    CURRENT_FIELDS = (
        "precipitation,pressure_msl,wind_speed_10m,"
        "wind_direction_10m,wind_gusts_10m"
    )

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
        payload = await fetcher.get_json(
            self._endpoint,
            params={
                "latitude": ",".join(f"{item.latitude:.5f}" for item in targets),
                "longitude": ",".join(f"{item.longitude:.5f}" for item in targets),
                "models": self.MODEL_ID,
                "current": self.CURRENT_FIELDS,
                "timezone": "UTC",
            },
            maximum_bytes=2 * 1024 * 1024,
        )
        items = payload if isinstance(payload, list) else [payload]
        if len(items) != len(targets) or not all(
            isinstance(item, Mapping) for item in items
        ):
            raise ValueError("italiameteo_location_count_mismatch")

        observations: list[NormalizedObservation] = []
        for target, item in zip(targets, items):
            current = item.get("current")
            units = item.get("current_units")
            if not isinstance(current, Mapping) or not isinstance(units, Mapping):
                raise ValueError("italiameteo_current_missing")
            expected_units = {
                "precipitation": "mm",
                "pressure_msl": "hPa",
                "wind_speed_10m": "km/h",
                "wind_direction_10m": "°",
                "wind_gusts_10m": "km/h",
            }
            if any(units.get(key) != unit for key, unit in expected_units.items()):
                raise ValueError("italiameteo_units_invalid")
            interval = _optional_float(current.get("interval"))
            if interval is None or interval <= 0.0 or interval > 3600.0:
                raise ValueError("italiameteo_interval_invalid")
            observed_at = _parse_timestamp(current.get("time"), fallback=now)
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
            }
            precipitation = _optional_float(current.get("precipitation"))
            if precipitation is not None:
                observations.append(NormalizedObservation.create(
                    **common,
                    kind=ObservationKind.RAIN_RATE,
                    value=precipitation * 3600.0 / interval,
                    unit="mm/h",
                    metadata={
                        "role": "model_guidance",
                        "model": self.MODEL_ID,
                        "measurement": "precipitation_rate",
                        "source_interval_seconds": interval,
                    },
                ))
            for field, kind, unit in (
                ("pressure_msl", ObservationKind.PRESSURE, "hPa"),
                ("wind_speed_10m", ObservationKind.WIND_SPEED, "km/h"),
                ("wind_gusts_10m", ObservationKind.WIND_SPEED, "km/h"),
                ("wind_direction_10m", ObservationKind.WIND_DIRECTION, "°"),
            ):
                value = _optional_float(current.get(field))
                if value is None:
                    continue
                observations.append(NormalizedObservation.create(
                    **common,
                    kind=kind,
                    value=value,
                    unit=unit,
                    metadata={
                        "role": "model_guidance",
                        "model": self.MODEL_ID,
                        "measurement": field,
                    },
                ))
        return tuple(observations)


class GeoSphereNowcastAdapter:
    """GeoSphere Austria INCA 15-minute precipitation guidance."""

    INTERVAL_SECONDS = 15 * 60
    MAXIMUM_AGE_SECONDS = 90 * 60

    def __init__(self, *, endpoint: str = GEOSPHERE_INCA_ENDPOINT) -> None:
        self._endpoint = endpoint

    async def fetch(
        self,
        request: ThinProviderRequest,
        fetcher: AsyncHttpFetcher,
        *,
        now: float,
    ) -> tuple[NormalizedObservation, ...]:
        observations: list[NormalizedObservation] = []
        for target in request.targets:
            payload = await fetcher.get_json(
                self._endpoint,
                params={
                    "parameters": "rr",
                    "lat_lon": f"{target.latitude:.5f},{target.longitude:.5f}",
                    "output_format": "geojson",
                },
                maximum_bytes=4 * 1024 * 1024,
            )
            if not isinstance(payload, Mapping):
                raise ValueError("geosphere_payload_invalid")
            reference_text = payload.get("reference_time")
            reference_time = _parse_timestamp(reference_text, fallback=now)
            timestamps = payload.get("timestamps")
            features = payload.get("features")
            if (
                not isinstance(timestamps, list)
                or not timestamps
                or not isinstance(features, list)
                or len(features) != 1
                or not isinstance(features[0], Mapping)
            ):
                raise ValueError("geosphere_nowcast_missing")
            properties = features[0].get("properties")
            parameters = (
                properties.get("parameters")
                if isinstance(properties, Mapping)
                else None
            )
            rain = parameters.get("rr") if isinstance(parameters, Mapping) else None
            values = rain.get("data") if isinstance(rain, Mapping) else None
            if (
                not isinstance(values, list)
                or len(values) != len(timestamps)
                or rain.get("unit") != "kg m-2"
            ):
                raise ValueError("geosphere_precipitation_invalid")
            first = _optional_float(values[0])
            if first is None:
                continue
            valid_at = timestamps[0]
            _parse_timestamp(valid_at, fallback=reference_time)
            observations.append(NormalizedObservation.create(
                provider_id=request.provider_id,
                product_id=request.product_id,
                target_id=target.target_id,
                kind=ObservationKind.RAIN_RATE,
                value=first * 3600.0 / self.INTERVAL_SECONDS,
                unit="mm/h",
                observed_at=reference_time,
                received_at=now,
                latitude=target.latitude,
                longitude=target.longitude,
                quality=ObservationQuality.GOOD,
                geometry=_point(target.longitude, target.latitude),
                metadata={
                    "role": "model_location_validation",
                    "measurement": "precipitation_rate",
                    "forecast_valid_at": valid_at,
                    "source_interval_seconds": self.INTERVAL_SECONDS,
                    "forecast_steps": len(values),
                },
                maximum_age_seconds=self.MAXIMUM_AGE_SECONDS,
            ))
        return tuple(observations)


class ItaliaMeteoDpcHistoricalCatalogAdapter:
    """Validate the public DPC SRI replay catalog without feeding live fusion."""

    _FILENAME_RE = re.compile(
        r"^data-\d{8}T\d{6}Z-[A-Za-z0-9-]+\.grib$"
    )

    def __init__(
        self,
        *,
        endpoint: str = ITALIAMETEO_DPC_HISTORICAL_CATALOG_ENDPOINT,
    ) -> None:
        self._endpoint = endpoint

    async def fetch(
        self,
        request: ThinProviderRequest,
        fetcher: AsyncHttpFetcher,
        *,
        now: float,
    ) -> tuple[NormalizedObservation, ...]:
        del request, now
        payload = await fetcher.get_json(
            self._endpoint,
            maximum_bytes=4 * 1024 * 1024,
        )
        if not isinstance(payload, list) or not payload:
            raise ValueError("italiameteo_catalog_invalid")
        for item in payload:
            if not isinstance(item, Mapping):
                raise ValueError("italiameteo_catalog_entry_invalid")
            date_text = item.get("date")
            filename = item.get("filename")
            variables = item.get("vars")
            try:
                datetime.fromisoformat(date_text)
            except (TypeError, ValueError):
                raise ValueError("italiameteo_catalog_entry_invalid") from None
            if (
                not isinstance(filename, str)
                or self._FILENAME_RE.fullmatch(filename) is None
                or not isinstance(variables, list)
                or not variables
                or not all(isinstance(value, str) and value for value in variables)
            ):
                raise ValueError("italiameteo_catalog_entry_invalid")
        # This endpoint inventories historical GRIB files for replay. It has
        # no live meteorological value, so a successful validation deliberately
        # produces no observation for live fusion.
        return ()


class MeteoLuxValidationAdapter:
    """Validate MeteoLux location guidance without inventing data semantics."""

    def __init__(self, *, endpoint: str = METEOLUX_WEATHER_ENDPOINT) -> None:
        self._endpoint = endpoint

    async def fetch(
        self,
        request: ThinProviderRequest,
        fetcher: AsyncHttpFetcher,
        *,
        now: float,
    ) -> tuple[NormalizedObservation, ...]:
        del now
        for target in request.targets:
            payload = await fetcher.get_json(
                self._endpoint,
                params={
                    "langcode": "en",
                    "lat": f"{target.latitude:.5f}",
                    "long": f"{target.longitude:.5f}",
                },
                maximum_bytes=4 * 1024 * 1024,
            )
            if not isinstance(payload, Mapping):
                raise ValueError("meteolux_payload_invalid")
            city = payload.get("city")
            forecast = payload.get("forecast")
            current = (
                forecast.get("current")
                if isinstance(forecast, Mapping)
                else None
            )
            if (
                not isinstance(city, Mapping)
                or not isinstance(current, Mapping)
                or current.get("type") != "current"
                or not isinstance(current.get("temperature"), Mapping)
                or not isinstance(current.get("wind"), Mapping)
                or "rain" not in current
            ):
                raise ValueError("meteolux_current_invalid")
            try:
                datetime.fromisoformat(current.get("date"))
                city_latitude = float(city.get("lat"))
                city_longitude = float(city.get("long"))
            except (TypeError, ValueError):
                raise ValueError("meteolux_current_invalid") from None
            if (
                not math.isfinite(city_latitude)
                or not math.isfinite(city_longitude)
                or not -90.0 <= city_latitude <= 90.0
                or not -180.0 <= city_longitude <= 180.0
            ):
                raise ValueError("meteolux_current_invalid")
        # The endpoint does not declare units/accumulation intervals for its
        # display strings. Until that semantic contract is public, it proves
        # availability only and must not enter quantitative fusion.
        return ()


_KMI_COLORS = (
    ((233, 255, 255, 144), 1),
    ((82, 255, 241, 160), 2),
    ((75, 209, 220, 192), 3),
    ((68, 162, 200, 200), 4),
)

# The INCA-BE mobile images use Belgian Lambert 2008 (EPSG:3812).  The
# image axes are very slightly rotated relative to the projected axes.  These
# coefficients map projected metres to normalized image coordinates; the
# translation is anchored from the coordinates returned with every AppV4
# response so a server-side crop shift cannot move the radar layer.
_KMI_LAMBERT_TO_IMAGE = (
    (2.28737432e-6, -2.32300157e-8),
    (-2.22248944e-8, -3.00083858e-6),
)


def _kmi_appv4_key(timestamp: float) -> str:
    local_date = datetime.fromtimestamp(
        float(timestamp), ZoneInfo("Europe/Brussels")
    ).strftime("%d/%m/%Y")
    material = f"{_KMI_APPV4_REQUEST_SECRET};getForecasts;{local_date}"
    return hashlib.md5(material.encode("utf-8")).hexdigest()


def _kmi_appv4_frame_url(value: Any) -> str:
    candidate = urljoin(KMI_APPV4_ENDPOINT, str(value or ""))
    parsed = urlsplit(candidate)
    if (
        parsed.scheme != "https"
        or (parsed.hostname or "").lower() not in KMI_APPV4_ALLOWED_IMAGE_HOSTS
    ):
        raise ValueError("kmi_frame_url_invalid")
    return candidate


def _kmi_level(pixel: tuple[int, int, int, int]) -> int:
    return next(
        (level for expected, level in _KMI_COLORS if tuple(pixel) == expected),
        0,
    )


class KmiRadarAdapter:
    def __init__(
        self,
        *,
        forecast_url: str = KMI_APPV4_ENDPOINT,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        self._forecast_url = forecast_url
        self._headers = {
            "User-Agent": KMI_APPV4_USER_AGENT,
            **dict(headers or {}),
        }

    async def fetch(self, request, fetcher, *, now):
        representative = request.targets[0]
        payload = await fetcher.get_json(
            self._forecast_url,
            params={
                "s": "getForecasts",
                "k": _kmi_appv4_key(now),
                "lat": f"{representative.latitude:.5f}",
                "long": f"{representative.longitude:.5f}",
            },
            headers=self._headers,
            maximum_bytes=4 * 1024 * 1024,
        )
        animation = payload.get("animation", {})
        sequence = animation.get("sequence", [])
        if not isinstance(sequence, list) or not sequence:
            raise ValueError("kmi_sequence_missing")
        frames = [item for item in sequence if isinstance(item, Mapping) and item.get("uri")]
        if not frames:
            raise ValueError("kmi_frame_missing")
        historical = [
            item
            for item in frames
            if _parse_timestamp(item.get("time"), fallback=now) <= now + 30.0
        ]
        if not historical:
            raise ValueError("kmi_observation_frame_missing")
        latest = max(
            historical,
            key=lambda item: _parse_timestamp(item.get("time"), fallback=0.0),
        )
        observed_at = _parse_timestamp(latest.get("time"), fallback=now)
        frame_url = _kmi_appv4_frame_url(latest["uri"])
        image_bytes = await fetcher.get_bytes(
            frame_url,
            headers=self._headers,
            maximum_bytes=8 * 1024 * 1024,
        )
        from PIL import Image
        import numpy as np

        image = Image.open(io.BytesIO(image_bytes)).convert("RGBA")
        width, height = image.size
        rgba = np.asarray(image, dtype=np.uint8)
        levels = np.zeros((height, width), dtype=np.uint8)
        for expected, level in _KMI_COLORS:
            levels[np.all(rgba == np.asarray(expected, dtype=np.uint8), axis=2)] = level

        try:
            anchor_ratio_x = float(animation["localisationLayerRatioX"])
            anchor_ratio_y = float(animation["localisationLayerRatioY"])
        except (KeyError, TypeError, ValueError):
            raise ValueError("kmi_georeference_missing") from None
        if not (
            0.0 <= anchor_ratio_x <= 1.0
            and 0.0 <= anchor_ratio_y <= 1.0
        ):
            raise ValueError("kmi_georeference_invalid")

        from pyproj import Transformer

        to_lambert = Transformer.from_crs("EPSG:4326", "EPSG:3812", always_xy=True)
        to_geographic = Transformer.from_crs("EPSG:3812", "EPSG:4326", always_xy=True)
        (ratio_x_x, ratio_x_y), (ratio_y_x, ratio_y_y) = _KMI_LAMBERT_TO_IMAGE
        anchor_x, anchor_y = to_lambert.transform(
            representative.longitude,
            representative.latitude,
        )
        offset_x = anchor_ratio_x - ratio_x_x * anchor_x - ratio_x_y * anchor_y
        offset_y = anchor_ratio_y - ratio_y_x * anchor_x - ratio_y_y * anchor_y

        def projected_to_ratio(x: float, y: float) -> tuple[float, float]:
            return (
                ratio_x_x * x + ratio_x_y * y + offset_x,
                ratio_y_x * x + ratio_y_y * y + offset_y,
            )

        determinant = ratio_x_x * ratio_y_y - ratio_x_y * ratio_y_x

        def ratio_to_lonlat(ratio_x: float, ratio_y: float) -> tuple[float, float]:
            shifted_x = ratio_x - offset_x
            shifted_y = ratio_y - offset_y
            projected_x = (
                shifted_x * ratio_y_y - ratio_x_y * shifted_y
            ) / determinant
            projected_y = (
                ratio_x_x * shifted_y - ratio_y_x * shifted_x
            ) / determinant
            return to_geographic.transform(projected_x, projected_y)

        observations: list[NormalizedObservation] = []
        for target in request.targets:
            target_x, target_y = to_lambert.transform(
                target.longitude,
                target.latitude,
            )
            ratio_x, ratio_y = projected_to_ratio(target_x, target_y)
            if not (0.0 <= ratio_x < 1.0 and 0.0 <= ratio_y < 1.0):
                continue
            column = min(width - 1, max(0, int(ratio_x * width)))
            row = min(height - 1, max(0, int(ratio_y * height)))
            level = int(levels[row, column])
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
                quality=ObservationQuality.SUSPECT,
                geometry=_point(target.longitude, target.latitude),
                metadata={
                    "pixel_column": column,
                    "pixel_row": row,
                    "operational_use": "visualisation_only",
                    "quantitative_semantics_eligible": False,
                    "semantic_class": "categorical_visualisation",
                    "source_projection": "EPSG:3812",
                },
                maximum_age_seconds=KMI_APPV4_MAX_FRAME_AGE_SECONDS,
            ))

        roi = np.zeros(levels.shape, dtype=bool)
        for target in request.targets:
            center_x, center_y = to_lambert.transform(
                target.longitude,
                target.latitude,
            )
            radius_m = target.observation_radius_km * 1_000.0
            corners = (
                projected_to_ratio(center_x - radius_m, center_y - radius_m),
                projected_to_ratio(center_x - radius_m, center_y + radius_m),
                projected_to_ratio(center_x + radius_m, center_y - radius_m),
                projected_to_ratio(center_x + radius_m, center_y + radius_m),
            )
            columns = [ratio[0] * width for ratio in corners]
            rows = [ratio[1] * height for ratio in corners]
            column0 = max(0, math.floor(min(columns)))
            column1 = min(width, math.ceil(max(columns)))
            row0 = max(0, math.floor(min(rows)))
            row1 = min(height, math.ceil(max(rows)))
            if row0 < row1 and column0 < column1:
                roi[row0:row1, column0:column1] = True

        pixel_width_km = 1.0 / math.hypot(
            ratio_x_x * width,
            ratio_y_x * height,
        ) / 1_000.0
        pixel_height_km = 1.0 / math.hypot(
            ratio_x_y * width,
            ratio_y_y * height,
        ) / 1_000.0
        block_rows = max(1, int(round(5.0 / pixel_height_km)))
        block_columns = max(1, int(round(5.0 / max(pixel_width_km, 0.1))))
        source = np.where(roi, levels, 0)
        padded_height = math.ceil(height / block_rows) * block_rows
        padded_width = math.ceil(width / block_columns) * block_columns
        padded = np.zeros((padded_height, padded_width), dtype=np.uint8)
        padded[:height, :width] = source
        pooled = padded.reshape(
            padded_height // block_rows,
            block_rows,
            padded_width // block_columns,
            block_columns,
        ).max(axis=(1, 3))
        wet = pooled > 0
        visited = np.zeros(wet.shape, dtype=bool)
        spatial: list[NormalizedObservation] = []
        for seed_row, seed_column in np.argwhere(wet):
            seed_row, seed_column = int(seed_row), int(seed_column)
            if visited[seed_row, seed_column]:
                continue
            stack = [(seed_row, seed_column)]
            visited[seed_row, seed_column] = True
            component: list[tuple[int, int]] = []
            while stack:
                component_row, component_column = stack.pop()
                component.append((component_row, component_column))
                for delta_row, delta_column in ((-1, 0), (1, 0), (0, -1), (0, 1)):
                    neighbour_row = component_row + delta_row
                    neighbour_column = component_column + delta_column
                    if (
                        0 <= neighbour_row < wet.shape[0]
                        and 0 <= neighbour_column < wet.shape[1]
                        and wet[neighbour_row, neighbour_column]
                        and not visited[neighbour_row, neighbour_column]
                    ):
                        visited[neighbour_row, neighbour_column] = True
                        stack.append((neighbour_row, neighbour_column))

            def corner_to_lonlat(grid_row: int, grid_column: int) -> tuple[float, float]:
                pixel_row = min(height, grid_row * block_rows)
                pixel_column = min(width, grid_column * block_columns)
                return ratio_to_lonlat(pixel_column / width, pixel_row / height)

            rows = [item[0] for item in component]
            columns = [item[1] for item in component]
            center_row = (min(rows) + max(rows) + 1) * block_rows / 2.0
            center_column = (min(columns) + max(columns) + 1) * block_columns / 2.0
            longitude, latitude = ratio_to_lonlat(
                center_column / width,
                center_row / height,
            )
            spatial.append(NormalizedObservation.create(
                provider_id=request.provider_id,
                product_id=request.product_id,
                target_id=None,
                kind=ObservationKind.RADAR_LEVEL,
                value=float(max(pooled[row, column] for row, column in component)),
                unit="level",
                observed_at=observed_at,
                received_at=now,
                latitude=latitude,
                longitude=longitude,
                quality=ObservationQuality.SUSPECT,
                geometry=_component_polygon_geometry(component, corner_to_lonlat),
                metadata={
                    "spatial_cell": True,
                    "block_count": len(component),
                    "operational_use": "visualisation_only",
                    "quantitative_semantics_eligible": False,
                    "semantic_class": "categorical_visualisation",
                    "source_projection": "EPSG:3812",
                },
                maximum_age_seconds=KMI_APPV4_MAX_FRAME_AGE_SECONDS,
            ))
        observations.extend(sorted(
            spatial,
            key=lambda item: (item.value, item.metadata.get("block_count", 0)),
            reverse=True,
        )[:MAX_NATIONAL_RADAR_SPATIAL_CELLS])
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
        candidates = [
            (match.group("stamp"), str(node.text))
            for node in root.iter()
            if node.tag.rsplit("}", 1)[-1] == "Key"
            and node.text
            and (match := OPERA_KEY_RE.fullmatch(str(node.text))) is not None
        ]
        historical = [
            (stamp, key)
            for stamp, key in candidates
            if datetime.strptime(stamp, "%Y%m%dT%H%M").replace(
                tzinfo=timezone.utc,
            ).timestamp() <= now + 30.0
        ]
        if not historical:
            raise ValueError("opera_product_missing")
        stamp, key = max(historical)
        observed_at = datetime.strptime(stamp, "%Y%m%dT%H%M").replace(
            tzinfo=timezone.utc,
        ).timestamp()
        if now - observed_at > OPERA_MAX_FRAME_AGE_SECONDS:
            raise ProviderStale("product_stale")
        data = await fetcher.get_bytes(
            f"{self._object_base_url}/{key}",
            maximum_bytes=384 * 1024 * 1024,
        )
        # h5py keeps a process-wide lock.  Decoding OPERA inline can therefore
        # block the MQTT event loop while another HDF5 radar decoder owns that
        # lock, freezing heartbeats, control replies and the complete cycle.
        # Share the bounded radar decode pool used by KNMI/DWD so HDF5 work is
        # serialized and never executes on the event loop.
        async with _RADAR_DECODE_SEMAPHORE:
            return await asyncio.to_thread(
                _decode_opera_targets,
                data,
                request,
                received_at=now,
            )


def _latest_aemet_frame(payload: bytes) -> tuple[bytes, float]:
    try:
        with tarfile.open(fileobj=io.BytesIO(payload), mode="r:gz") as archive:
            candidates = []
            for member in archive.getmembers():
                match = AEMET_FRAME_RE.fullmatch(member.name)
                if (
                    match is not None
                    and member.isfile()
                    and 0 < member.size <= AEMET_MAX_FRAME_BYTES
                ):
                    candidates.append((match.group(1), member))
            if not candidates:
                raise ValueError("aemet_current_frame_missing")
            stamp, member = max(candidates, key=lambda item: item[0])
            stream = archive.extractfile(member)
            if stream is None:
                raise ValueError("aemet_frame_missing")
            frame = stream.read(AEMET_MAX_FRAME_BYTES + 1)
            if len(frame) != member.size or len(frame) > AEMET_MAX_FRAME_BYTES:
                raise ValueError("aemet_frame_size_invalid")
    except tarfile.TarError as error:
        raise ValueError("aemet_archive_invalid") from error
    observed_at = datetime.strptime(stamp, "%Y%m%d%H%M").replace(
        tzinfo=timezone.utc,
    ).timestamp()
    return frame, observed_at


def _decode_aemet_radar(
    frame: bytes,
    request: ThinProviderRequest,
    *,
    observed_at: float,
    received_at: float,
) -> tuple[NormalizedObservation, ...]:
    import numpy as np
    from PIL import Image

    if received_at - observed_at > AEMET_MAX_FRAME_AGE_SECONDS:
        raise ValueError("aemet_frame_stale")
    if observed_at - received_at > 5 * 60:
        raise ValueError("aemet_frame_time_invalid")
    with Image.open(io.BytesIO(frame)) as image:
        values = np.asarray(image, dtype=np.uint8)
        tie = image.tag_v2.get(33922)
        scale = image.tag_v2.get(33550)
    if values.ndim != 2:
        raise ValueError("aemet_grid_invalid")
    if not tie or len(tie) < 6 or not scale or len(scale) < 2:
        raise ValueError("aemet_georeference_missing")
    longitude_left = float(tie[3])
    latitude_top = float(tie[4])
    longitude_scale = abs(float(scale[0]))
    latitude_scale = abs(float(scale[1]))
    if not all(
        math.isfinite(value) and value > 0.0
        for value in (longitude_scale, latitude_scale)
    ):
        raise ValueError("aemet_georeference_invalid")

    valid = (values >= 1) & (values <= 8)

    def lonlat_to_pixel(longitude: float, latitude: float) -> tuple[float, float]:
        return (
            (latitude_top - latitude) / latitude_scale,
            (longitude - longitude_left) / longitude_scale,
        )

    def pixel_to_lonlat(row: float, column: float) -> tuple[float, float]:
        return (
            longitude_left + column * longitude_scale,
            latitude_top - row * latitude_scale,
        )

    approximate_latitude = latitude_top - values.shape[0] * latitude_scale / 2.0
    level_observations = _national_radar_observations(
        values.astype(float),
        valid,
        request,
        observed_at=observed_at,
        received_at=received_at,
        lonlat_to_pixel=lonlat_to_pixel,
        pixel_to_lonlat=pixel_to_lonlat,
        pixel_width_m=(
            longitude_scale
            * 111_320.0
            * max(0.1, abs(math.cos(math.radians(approximate_latitude))))
        ),
        pixel_height_m=latitude_scale * 110_574.0,
        maximum_age_seconds=AEMET_MAX_FRAME_AGE_SECONDS,
    )
    return tuple(
        NormalizedObservation.create(
            provider_id=item.provider_id,
            product_id=item.product_id,
            target_id=item.target_id,
            kind=ObservationKind.RADAR_LEVEL,
            value=item.value,
            unit="level",
            observed_at=item.observed_at,
            received_at=item.received_at,
            latitude=item.latitude,
            longitude=item.longitude,
            quality=item.quality,
            geometry=item.geometry,
            metadata={**dict(item.metadata), "aemet_visual_class": int(item.value)},
            maximum_age_seconds=AEMET_MAX_FRAME_AGE_SECONDS,
        )
        for item in level_observations
    )


class AemetRadarAdapter:
    def __init__(self, *, endpoint: str = AEMET_RADAR_ENDPOINT) -> None:
        self._endpoint = endpoint

    async def fetch(self, request, fetcher, *, now):
        payload = await fetcher.get_bytes(
            self._endpoint,
            maximum_bytes=AEMET_MAX_ARCHIVE_BYTES,
        )
        frame, observed_at = _latest_aemet_frame(payload)
        async with _RADAR_DECODE_SEMAPHORE:
            return await asyncio.to_thread(
                _decode_aemet_radar,
                frame,
                request,
                observed_at=observed_at,
                received_at=now,
            )


def _decode_meteofrance_radar(
    payload: bytes,
    request: ThinProviderRequest,
    *,
    received_at: float,
) -> ThinAdapterResult:
    import h5py
    import numpy as np
    from pyproj import CRS, Transformer

    with h5py.File(io.BytesIO(payload), "r") as product:
        data = np.asarray(product["dataset1/data1/data"])
        data_what = product["dataset1/data1/what"].attrs
        frame_what = product["dataset1/what"].attrs
        where = product["where"].attrs
        observed_at = datetime.strptime(
            _attribute_text(frame_what["enddate"])
            + _attribute_text(frame_what["endtime"]),
            "%Y%m%d%H%M%S",
        ).replace(tzinfo=timezone.utc).timestamp()
        if received_at - observed_at > METEOFRANCE_MAX_FRAME_AGE_SECONDS:
            raise ValueError("meteofrance_frame_stale")
        if observed_at - received_at > 5 * 60:
            raise ValueError("meteofrance_frame_time_invalid")

        raw = data.astype(np.float64)
        decoded = (
            raw * _attribute_number(data_what["gain"])
            + _attribute_number(data_what["offset"])
        )
        rain_rate = decoded * 12.0
        valid = (
            (raw != _attribute_number(data_what["nodata"]))
            & (raw != _attribute_number(data_what["undetect"]))
            & np.isfinite(rain_rate)
            & (rain_rate >= 0.1)
            & (rain_rate < 500.0)
        )
        crs = _attribute_text(where["projdef"])
        xscale = _attribute_number(where["xscale"])
        yscale = _attribute_number(where["yscale"])
        if xscale <= 0.0 or yscale <= 0.0:
            raise ValueError("meteofrance_resolution_invalid")
        projected = CRS.from_user_input(crs)
        to_lonlat = Transformer.from_crs(
            projected,
            "EPSG:4326",
            always_xy=True,
        )
        from_lonlat = Transformer.from_crs(
            "EPSG:4326",
            projected,
            always_xy=True,
        )
        if "UL_lon" in where and "UL_lat" in where:
            origin_x, origin_y = from_lonlat.transform(
                _attribute_number(where["UL_lon"]),
                _attribute_number(where["UL_lat"]),
            )
        else:
            origin_x = 0.0
            origin_y = data.shape[0] * yscale

    def lonlat_to_pixel(longitude: float, latitude: float) -> tuple[float, float]:
        x, y = from_lonlat.transform(longitude, latitude)
        return ((origin_y - y) / yscale, (x - origin_x) / xscale)

    def pixel_to_lonlat(row: float, column: float) -> tuple[float, float]:
        longitude, latitude = to_lonlat.transform(
            origin_x + column * xscale,
            origin_y - row * yscale,
        )
        return float(longitude), float(latitude)

    observations = _national_radar_observations(
        rain_rate,
        valid,
        request,
        observed_at=observed_at,
        received_at=received_at,
        lonlat_to_pixel=lonlat_to_pixel,
        pixel_to_lonlat=pixel_to_lonlat,
        pixel_width_m=xscale,
        pixel_height_m=yscale,
        maximum_age_seconds=METEOFRANCE_MAX_FRAME_AGE_SECONDS,
    )
    field = ThinRadarField(
        provider_id=request.provider_id,
        product_id=request.product_id,
        observed_at=observed_at,
        crs=crs,
        origin_x_m=float(origin_x),
        origin_y_m=float(origin_y),
        x_resolution_m=xscale,
        y_resolution_m=yscale,
        rain_rate_mm_h=np.where(valid, rain_rate, 0.0),
        valid_mask=valid,
    )
    return ThinAdapterResult(observations, (field,))


class MeteoFranceRadarAdapter:
    def __init__(
        self,
        *,
        access_token: str | None = None,
        access_token_provider: Callable[[], Awaitable[str | None]] | None = None,
        endpoint: str = METEOFRANCE_RADAR_ENDPOINT,
    ) -> None:
        self._access_token = access_token or os.getenv(
            "STV3_METEOFRANCE_API_TOKEN"
        )
        self._access_token_provider = access_token_provider
        self._endpoint = endpoint

    async def fetch(self, request, fetcher, *, now):
        access_token = None
        if self._access_token_provider is not None:
            access_token = await self._access_token_provider()
        if not access_token:
            access_token = self._access_token
        if not access_token:
            raise ProviderBlocked("external_access_required")
        payload = await fetcher.get_bytes(
            self._endpoint,
            headers={"Authorization": f"Bearer {access_token}"},
            maximum_bytes=METEOFRANCE_MAX_FRAME_BYTES,
        )
        async with _RADAR_DECODE_SEMAPHORE:
            return await asyncio.to_thread(
                _decode_meteofrance_radar,
                payload,
                request,
                received_at=now,
            )


def _decode_dpc_sri(
    payload: bytes,
    request: ThinProviderRequest,
    *,
    observed_at: float,
    received_at: float,
) -> ThinAdapterResult:
    import numpy as np
    from PIL import Image
    from pyproj import Transformer

    if received_at - observed_at > DPC_MAX_FRAME_AGE_SECONDS:
        raise ValueError("dpc_frame_stale")
    if observed_at - received_at > 5 * 60:
        raise ValueError("dpc_frame_time_invalid")
    with Image.open(io.BytesIO(payload)) as image:
        values = np.asarray(image, dtype=np.float32)
        tie = image.tag_v2.get(33922)
        scale = image.tag_v2.get(33550)
    if values.ndim != 2:
        raise ValueError("dpc_grid_invalid")
    if not tie or len(tie) < 6 or not scale or len(scale) < 2:
        raise ValueError("dpc_georeference_missing")
    origin_x = float(tie[3])
    origin_y = float(tie[4])
    xscale = abs(float(scale[0]))
    yscale = abs(float(scale[1]))
    if not all(
        math.isfinite(value) and value > 0.0
        for value in (xscale, yscale)
    ):
        raise ValueError("dpc_georeference_invalid")

    rain_rate = values.astype(np.float64)
    valid = np.isfinite(rain_rate) & (rain_rate >= 0.0) & (rain_rate < 500.0)
    to_lonlat = Transformer.from_crs(DPC_CRS, "EPSG:4326", always_xy=True)
    from_lonlat = Transformer.from_crs("EPSG:4326", DPC_CRS, always_xy=True)

    def lonlat_to_pixel(longitude: float, latitude: float) -> tuple[float, float]:
        x, y = from_lonlat.transform(longitude, latitude)
        return ((origin_y - y) / yscale, (x - origin_x) / xscale)

    def pixel_to_lonlat(row: float, column: float) -> tuple[float, float]:
        longitude, latitude = to_lonlat.transform(
            origin_x + column * xscale,
            origin_y - row * yscale,
        )
        return float(longitude), float(latitude)

    observations = _national_radar_observations(
        rain_rate,
        valid,
        request,
        observed_at=observed_at,
        received_at=received_at,
        lonlat_to_pixel=lonlat_to_pixel,
        pixel_to_lonlat=pixel_to_lonlat,
        pixel_width_m=xscale,
        pixel_height_m=yscale,
        maximum_age_seconds=DPC_MAX_FRAME_AGE_SECONDS,
    )
    field = ThinRadarField(
        provider_id=request.provider_id,
        product_id=request.product_id,
        observed_at=observed_at,
        crs=DPC_CRS,
        origin_x_m=origin_x,
        origin_y_m=origin_y,
        x_resolution_m=xscale,
        y_resolution_m=yscale,
        rain_rate_mm_h=np.where(valid, rain_rate, 0.0),
        valid_mask=valid,
    )
    return ThinAdapterResult(observations, (field,))


class DpcRadarAdapter:
    def __init__(
        self,
        *,
        catalog_endpoint: str = DPC_CATALOG_ENDPOINT,
        download_endpoint: str = DPC_DOWNLOAD_ENDPOINT,
    ) -> None:
        self._catalog_endpoint = catalog_endpoint
        self._download_endpoint = download_endpoint

    async def fetch(self, request, fetcher, *, now):
        headers = {"Origin": DPC_ORIGIN}
        catalog = await fetcher.get_json(
            self._catalog_endpoint,
            params={"type": "SRI"},
            headers=headers,
            maximum_bytes=2 * 1024 * 1024,
        )
        products = catalog.get("lastProducts") if isinstance(catalog, Mapping) else None
        if not isinstance(products, list) or not products:
            raise ValueError("dpc_current_frame_missing")
        product = products[0]
        if not isinstance(product, Mapping) or product.get("productType") not in (None, "SRI"):
            raise ValueError("dpc_catalog_invalid")
        try:
            timestamp_ms = int(product["time"])
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError("dpc_catalog_invalid") from error
        observed_at = timestamp_ms / 1_000.0
        if now - observed_at > DPC_MAX_FRAME_AGE_SECONDS:
            raise ValueError("dpc_frame_stale")
        if observed_at - now > 5 * 60:
            raise ValueError("dpc_frame_time_invalid")

        descriptor = await fetcher.post_json(
            self._download_endpoint,
            json_data={"productType": "SRI", "productDate": timestamp_ms},
            headers=headers,
            maximum_bytes=2 * 1024 * 1024,
        )
        download_url = descriptor.get("url") if isinstance(descriptor, Mapping) else None
        if not isinstance(download_url, str) or not download_url:
            raise ValueError("dpc_download_url_missing")
        payload = await fetcher.get_bytes(
            download_url,
            headers={},
            maximum_bytes=DPC_MAX_FRAME_BYTES,
        )
        async with _RADAR_DECODE_SEMAPHORE:
            return await asyncio.to_thread(
                _decode_dpc_sri,
                payload,
                request,
                observed_at=observed_at,
                received_at=now,
            )


def _latest_metoffice_key(payloads: Sequence[bytes]) -> str:
    keys: list[str] = []
    for payload in payloads:
        try:
            root = ET.fromstring(payload)
        except ET.ParseError as error:
            raise ValueError("metoffice_listing_invalid") from error
        for node in root.iter():
            if not node.tag.endswith("Key") or not node.text:
                continue
            key = node.text.strip()
            if METOFFICE_KEY_RE.fullmatch(key):
                keys.append(key)
    if not keys:
        raise ValueError("metoffice_current_frame_missing")
    return max(keys)


def _decode_metoffice_rate(
    payload: bytes,
    request: ThinProviderRequest,
    *,
    received_at: float,
) -> ThinAdapterResult:
    import h5py
    import numpy as np
    from pyproj import CRS, Transformer

    with h5py.File(io.BytesIO(payload), "r") as root:
        radar = np.asarray(root["dataset1/data1/data"], dtype=np.float64)
        data_what = root["dataset1/data1/what"].attrs
        frame_what = root["what"].attrs
        where = root["where"].attrs
        if _attribute_text(data_what["quantity"]) != "RATE":
            raise ValueError("metoffice_quantity_invalid")
        observed_at = datetime.strptime(
            _attribute_text(frame_what["date"])
            + _attribute_text(frame_what["time"]),
            "%Y%m%d%H%M%S",
        ).replace(tzinfo=timezone.utc).timestamp()
        if received_at - observed_at > METOFFICE_MAX_FRAME_AGE_SECONDS:
            raise ValueError("metoffice_frame_stale")
        if observed_at - received_at > 5 * 60:
            raise ValueError("metoffice_frame_time_invalid")
        xscale = _attribute_number(where["xscale"])
        yscale = _attribute_number(where["yscale"])
        if (
            xscale <= 0.0
            or yscale <= 0.0
            or int(_attribute_number(where["xsize"])) != radar.shape[1]
            or int(_attribute_number(where["ysize"])) != radar.shape[0]
        ):
            raise ValueError("metoffice_grid_invalid")
        gain = _attribute_number(data_what["gain"])
        offset = _attribute_number(data_what["offset"])
        nodata = _attribute_number(data_what["nodata"])
        undetect = _attribute_number(data_what["undetect"])
        rain_rate = radar * gain + offset
        valid = (
            np.isfinite(radar)
            & (radar != nodata)
            & (radar != undetect)
            & np.isfinite(rain_rate)
            & (rain_rate >= 0.0)
            & (rain_rate < 500.0)
        )
        crs = _attribute_text(where["projdef"])

    projected = CRS.from_user_input(crs)
    to_lonlat = Transformer.from_crs(projected, "EPSG:4326", always_xy=True)
    from_lonlat = Transformer.from_crs("EPSG:4326", projected, always_xy=True)

    def lonlat_to_pixel(longitude: float, latitude: float) -> tuple[float, float]:
        x, y = from_lonlat.transform(longitude, latitude)
        return (
            (METOFFICE_MAXIMUM_Y_METRES - y) / yscale,
            (x - METOFFICE_MINIMUM_X_METRES) / xscale,
        )

    def pixel_to_lonlat(row: float, column: float) -> tuple[float, float]:
        longitude, latitude = to_lonlat.transform(
            METOFFICE_MINIMUM_X_METRES + column * xscale,
            METOFFICE_MAXIMUM_Y_METRES - row * yscale,
        )
        return float(longitude), float(latitude)

    observations = _national_radar_observations(
        rain_rate,
        valid,
        request,
        observed_at=observed_at,
        received_at=received_at,
        lonlat_to_pixel=lonlat_to_pixel,
        pixel_to_lonlat=pixel_to_lonlat,
        pixel_width_m=xscale,
        pixel_height_m=yscale,
        maximum_age_seconds=METOFFICE_MAX_FRAME_AGE_SECONDS,
    )
    field = ThinRadarField(
        provider_id=request.provider_id,
        product_id=request.product_id,
        observed_at=observed_at,
        crs=crs,
        origin_x_m=METOFFICE_MINIMUM_X_METRES,
        origin_y_m=METOFFICE_MAXIMUM_Y_METRES,
        x_resolution_m=xscale,
        y_resolution_m=yscale,
        rain_rate_mm_h=np.where(valid, rain_rate, 0.0),
        valid_mask=valid,
    )
    return ThinAdapterResult(observations, (field,))


class MetOfficeRadarAdapter:
    def __init__(self, *, bucket_endpoint: str = METOFFICE_BUCKET_ENDPOINT) -> None:
        self._bucket_endpoint = bucket_endpoint.rstrip("/") + "/"

    async def fetch(self, request, fetcher, *, now):
        moment = datetime.fromtimestamp(now, tz=timezone.utc)
        listings = []
        for day in (moment, moment - timedelta(days=1)):
            listings.append(await fetcher.get_bytes(
                self._bucket_endpoint,
                params={
                    "list-type": "2",
                    "prefix": day.strftime("radar/%Y/%m/%d/"),
                },
                maximum_bytes=METOFFICE_MAX_LISTING_BYTES,
            ))
        key = _latest_metoffice_key(listings)
        payload = await fetcher.get_bytes(
            self._bucket_endpoint + key,
            maximum_bytes=METOFFICE_MAX_FRAME_BYTES,
        )
        async with _RADAR_DECODE_SEMAPHORE:
            return await asyncio.to_thread(
                _decode_metoffice_rate,
                payload,
                request,
                received_at=now,
            )


def _rainviewer_global_pixel(
    latitude: float,
    longitude: float,
) -> tuple[float, float]:
    count = 2**RAINVIEWER_ZOOM
    clipped_latitude = max(-85.05112878, min(85.05112878, latitude))
    latitude_radians = math.radians(clipped_latitude)
    tile_x = (longitude + 180.0) / 360.0 * count
    tile_y = (
        1.0
        - math.log(
            math.tan(latitude_radians) + 1.0 / math.cos(latitude_radians)
        ) / math.pi
    ) / 2.0 * count
    return tile_x * RAINVIEWER_TILE_SIZE, tile_y * RAINVIEWER_TILE_SIZE


def _rainviewer_tile_bounds(
    tile_x: int,
    tile_y: int,
) -> tuple[float, float, float, float]:
    count = 2**RAINVIEWER_ZOOM
    longitude_left = tile_x / count * 360.0 - 180.0
    longitude_right = (tile_x + 1) / count * 360.0 - 180.0
    latitude_top = math.degrees(math.atan(math.sinh(
        math.pi * (1.0 - 2.0 * tile_y / count)
    )))
    latitude_bottom = math.degrees(math.atan(math.sinh(
        math.pi * (1.0 - 2.0 * (tile_y + 1) / count)
    )))
    return latitude_top, latitude_bottom, longitude_left, longitude_right


def _decode_rainviewer_levels(payload: bytes) -> Any:
    from PIL import Image
    import numpy as np

    with Image.open(io.BytesIO(payload)) as image:
        rgba = np.asarray(image.convert("RGBA"), dtype=np.uint8)
    if rgba.shape != (RAINVIEWER_TILE_SIZE, RAINVIEWER_TILE_SIZE, 4):
        raise ValueError("rainviewer_tile_dimensions_invalid")
    rgb = rgba[..., :3].astype(np.int16)
    alpha = rgba[..., 3]
    levels = np.zeros(alpha.shape, dtype=np.uint8)
    translucent = (alpha >= RAINVIEWER_MIN_ALPHA) & (alpha < 255)
    estimated_dbz = 8 + np.rint(
        (alpha.astype(np.float64) - RAINVIEWER_MIN_ALPHA) * 6.0 / 60.0
    ).astype(np.int16)
    levels[translucent] = np.clip(
        1 + np.maximum(0, estimated_dbz[translucent] - 8) // 6,
        1,
        2,
    )
    saturation = np.max(rgb, axis=2) - np.min(rgb, axis=2)
    maximum = np.max(rgb, axis=2)
    minimum = np.min(rgb, axis=2)
    coloured = (alpha == 255) & ((saturation >= 40) | (minimum >= 240))
    levels[coloured & (maximum < 250)] = 3
    levels[coloured & (maximum >= 250)] = 8
    return levels


def _rainviewer_tile_observations(
    levels: Any,
    request: ThinProviderRequest,
    *,
    tile_x: int,
    tile_y: int,
    observed_at: float,
    received_at: float,
) -> tuple[NormalizedObservation, ...]:
    import numpy as np

    top, bottom, left, right = _rainviewer_tile_bounds(tile_x, tile_y)
    roi = np.zeros(levels.shape, dtype=bool)
    for target in request.targets:
        latitude_radius = target.observation_radius_km / 110.574
        longitude_radius = target.observation_radius_km / max(
            1.0,
            111.320 * math.cos(math.radians(target.latitude)),
        )
        north = min(85.05112878, target.latitude + latitude_radius)
        south = max(-85.05112878, target.latitude - latitude_radius)
        west = target.longitude - longitude_radius
        east = target.longitude + longitude_radius
        if east < left or west > right or north < bottom or south > top:
            continue
        west_px, north_px = _rainviewer_global_pixel(north, max(left, west))
        east_px, south_px = _rainviewer_global_pixel(south, min(right, east))
        local_left = tile_x * RAINVIEWER_TILE_SIZE
        local_top = tile_y * RAINVIEWER_TILE_SIZE
        column0 = max(0, min(RAINVIEWER_TILE_SIZE, math.floor(west_px - local_left)))
        column1 = max(0, min(RAINVIEWER_TILE_SIZE, math.ceil(east_px - local_left)))
        row0 = max(0, min(RAINVIEWER_TILE_SIZE, math.floor(north_px - local_top)))
        row1 = max(0, min(RAINVIEWER_TILE_SIZE, math.ceil(south_px - local_top)))
        if row0 < row1 and column0 < column1:
            roi[row0:row1, column0:column1] = True

    mean_latitude = (top + bottom) / 2.0
    pixel_height_km = abs(top - bottom) * 110.574 / RAINVIEWER_TILE_SIZE
    pixel_width_km = (
        abs(right - left)
        * 111.320
        * max(0.1, abs(math.cos(math.radians(mean_latitude))))
        / RAINVIEWER_TILE_SIZE
    )
    block_rows = max(1, int(round(5.0 / max(pixel_height_km, 0.1))))
    block_columns = max(1, int(round(5.0 / max(pixel_width_km, 0.1))))
    source = np.where(roi, levels, 0)
    padded_height = math.ceil(source.shape[0] / block_rows) * block_rows
    padded_width = math.ceil(source.shape[1] / block_columns) * block_columns
    padded = np.zeros((padded_height, padded_width), dtype=np.uint8)
    padded[:source.shape[0], :source.shape[1]] = source
    pooled = padded.reshape(
        padded_height // block_rows,
        block_rows,
        padded_width // block_columns,
        block_columns,
    ).max(axis=(1, 3))
    wet = pooled > 0
    visited = np.zeros(wet.shape, dtype=bool)
    observations: list[NormalizedObservation] = []
    for seed_row, seed_column in np.argwhere(wet):
        seed_row, seed_column = int(seed_row), int(seed_column)
        if visited[seed_row, seed_column]:
            continue
        stack = [(seed_row, seed_column)]
        visited[seed_row, seed_column] = True
        component: list[tuple[int, int]] = []
        while stack:
            row, column = stack.pop()
            component.append((row, column))
            for delta_row, delta_column in ((-1, 0), (1, 0), (0, -1), (0, 1)):
                neighbour_row = row + delta_row
                neighbour_column = column + delta_column
                if (
                    0 <= neighbour_row < wet.shape[0]
                    and 0 <= neighbour_column < wet.shape[1]
                    and wet[neighbour_row, neighbour_column]
                    and not visited[neighbour_row, neighbour_column]
                ):
                    visited[neighbour_row, neighbour_column] = True
                    stack.append((neighbour_row, neighbour_column))

        def corner_to_lonlat(row: int, column: int) -> tuple[float, float]:
            global_column = (
                tile_x * RAINVIEWER_TILE_SIZE
                + min(RAINVIEWER_TILE_SIZE, column * block_columns)
            )
            global_row = (
                tile_y * RAINVIEWER_TILE_SIZE
                + min(RAINVIEWER_TILE_SIZE, row * block_rows)
            )
            count_pixels = (2**RAINVIEWER_ZOOM) * RAINVIEWER_TILE_SIZE
            longitude = global_column / count_pixels * 360.0 - 180.0
            latitude = math.degrees(math.atan(math.sinh(
                math.pi * (1.0 - 2.0 * global_row / count_pixels)
            )))
            return longitude, latitude

        rows = [item[0] for item in component]
        columns = [item[1] for item in component]
        center_row = (min(rows) + max(rows) + 1) * block_rows / 2.0
        center_column = (min(columns) + max(columns) + 1) * block_columns / 2.0
        longitude, latitude = corner_to_lonlat(
            int(center_row / block_rows),
            int(center_column / block_columns),
        )
        observations.append(NormalizedObservation.create(
            provider_id=request.provider_id,
            product_id=request.product_id,
            target_id=None,
            kind=ObservationKind.RADAR_LEVEL,
            value=float(max(pooled[row, column] for row, column in component)),
            unit="level",
            observed_at=observed_at,
            received_at=received_at,
            latitude=latitude,
            longitude=longitude,
            quality=ObservationQuality.SUSPECT,
            geometry=_component_polygon_geometry(component, corner_to_lonlat),
            metadata={
                "spatial_cell": True,
                "block_count": len(component),
                "operational_use": "visualisation_only",
                "quantitative_semantics_eligible": False,
                "semantic_class": "categorical_composite_reflectivity",
            },
            maximum_age_seconds=RAINVIEWER_MAX_FRAME_AGE_SECONDS,
        ))
    return tuple(observations)


def _rainviewer_spatial_observations(
    decoded: Mapping[tuple[int, int], Any],
    request: ThinProviderRequest,
    *,
    observed_at: float,
    received_at: float,
) -> tuple[NormalizedObservation, ...]:
    spatial: list[NormalizedObservation] = []
    for (tile_x, tile_y), levels in decoded.items():
        spatial.extend(_rainviewer_tile_observations(
            levels,
            request,
            tile_x=tile_x,
            tile_y=tile_y,
            observed_at=observed_at,
            received_at=received_at,
        ))
    return tuple(spatial)


class RainViewerRadarAdapter:
    def __init__(self, *, manifest_url: str = RAINVIEWER_MANIFEST_ENDPOINT) -> None:
        self._manifest_url = manifest_url

    async def fetch(self, request, fetcher, *, now):
        manifest = await fetcher.get_json(
            self._manifest_url,
            maximum_bytes=2 * 1024 * 1024,
        )
        if not isinstance(manifest, Mapping) or manifest.get("host") != RAINVIEWER_TILE_HOST:
            raise ValueError("rainviewer_manifest_invalid")
        frames = manifest.get("radar", {}).get("past", [])
        if not isinstance(frames, list):
            raise ValueError("rainviewer_manifest_invalid")
        candidates = [
            item for item in frames
            if isinstance(item, Mapping)
            and isinstance(item.get("time"), (int, float))
            and isinstance(item.get("path"), str)
            and float(item["time"]) <= now + 30.0
            and re.fullmatch(r"/v2/radar/(?:\d{10}|[0-9a-f]{12})", item["path"])
        ]
        if not candidates:
            raise ValueError("rainviewer_current_frame_missing")
        frame = max(candidates, key=lambda item: float(item["time"]))
        observed_at = float(frame["time"])
        if now - observed_at > RAINVIEWER_MAX_FRAME_AGE_SECONDS:
            raise ValueError("rainviewer_frame_stale")
        centers = (
            tuple((item.center_latitude, item.center_longitude) for item in request.regions)
            or tuple((item.latitude, item.longitude) for item in request.targets)
        )
        count = 2**RAINVIEWER_ZOOM
        tile_keys: set[tuple[int, int]] = set()
        for latitude, longitude in centers:
            global_x, global_y = _rainviewer_global_pixel(latitude, longitude)
            center_x = int(global_x // RAINVIEWER_TILE_SIZE) % count
            center_y = min(count - 1, max(0, int(global_y // RAINVIEWER_TILE_SIZE)))
            for delta_x in (-1, 0, 1):
                for delta_y in (-1, 0, 1):
                    tile_keys.add(((center_x + delta_x) % count, min(count - 1, max(0, center_y + delta_y))))

        decoded: dict[tuple[int, int], Any] = {}
        for tile_x, tile_y in sorted(tile_keys):
            url = (
                f"{RAINVIEWER_TILE_HOST}{frame['path']}"
                f"/{RAINVIEWER_TILE_SIZE}/{RAINVIEWER_ZOOM}/{tile_x}/{tile_y}/2/0_0.png"
            )
            payload = await fetcher.get_bytes(url, maximum_bytes=4 * 1024 * 1024)
            async with _RADAR_DECODE_SEMAPHORE:
                decoded[(tile_x, tile_y)] = await asyncio.to_thread(
                    _decode_rainviewer_levels,
                    payload,
                )

        observations: list[NormalizedObservation] = []
        for target in request.targets:
            global_x, global_y = _rainviewer_global_pixel(target.latitude, target.longitude)
            tile_x = int(global_x // RAINVIEWER_TILE_SIZE) % count
            tile_y = min(count - 1, max(0, int(global_y // RAINVIEWER_TILE_SIZE)))
            levels = decoded.get((tile_x, tile_y))
            if levels is None:
                continue
            column = min(RAINVIEWER_TILE_SIZE - 1, max(0, int(global_x) % RAINVIEWER_TILE_SIZE))
            row = min(RAINVIEWER_TILE_SIZE - 1, max(0, int(global_y) % RAINVIEWER_TILE_SIZE))
            level = int(levels[row, column])
            if level > 0:
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
                    quality=ObservationQuality.SUSPECT,
                    geometry=_point(target.longitude, target.latitude),
                    metadata={
                        "pixel_column": column,
                        "pixel_row": row,
                        "operational_use": "visualisation_only",
                        "quantitative_semantics_eligible": False,
                        "semantic_class": "categorical_composite_reflectivity",
                    },
                    maximum_age_seconds=RAINVIEWER_MAX_FRAME_AGE_SECONDS,
                ))
        async with _RADAR_DECODE_SEMAPHORE:
            spatial = await asyncio.to_thread(
                _rainviewer_spatial_observations,
                decoded,
                request,
                observed_at=observed_at,
                received_at=now,
            )
        observations.extend(sorted(
            spatial,
            key=lambda item: (item.value, item.metadata.get("block_count", 0)),
            reverse=True,
        )[:MAX_NATIONAL_RADAR_SPATIAL_CELLS])
        return tuple(observations)


class MeteoFranceOAuthTokenProvider:
    """Obtain and cache Météo-France client-credential tokens in memory."""

    def __init__(
        self,
        *,
        fetcher: AsyncHttpFetcher,
        application_id: str | None = None,
        static_token: str | None = None,
        endpoint: str = METEOFRANCE_TOKEN_ENDPOINT,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        raw_application_id = application_id or os.getenv(
            "STV3_METEOFRANCE_APPLICATION_ID",
            "",
        )
        self._application_id = raw_application_id.removeprefix("Basic ").strip()
        self._static_token = (
            static_token or os.getenv("STV3_METEOFRANCE_API_TOKEN", "")
        ).strip()
        self._fetcher = fetcher
        self._endpoint = endpoint
        self._monotonic = monotonic
        self._access_token: str | None = None
        self._expires_at = 0.0

    async def get_access_token(self) -> str | None:
        now = self._monotonic()
        if self._access_token and now < self._expires_at:
            return self._access_token
        if not self._application_id:
            return self._static_token or None
        payload = await self._fetcher.post_json(
            self._endpoint,
            data={"grant_type": "client_credentials"},
            headers={
                "Authorization": f"Basic {self._application_id}",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            maximum_bytes=512 * 1024,
        )
        token = payload.get("access_token") if isinstance(payload, Mapping) else None
        if not isinstance(token, str) or not token.strip():
            raise ValueError("meteofrance_token_response_invalid")
        expires_in = (
            _optional_float(payload.get("expires_in"))
            if isinstance(payload, Mapping)
            else None
        )
        self._access_token = token.strip()
        self._expires_at = now + max(60.0, (expires_in or 3600.0) - 30.0)
        return self._access_token


class MetarAdapter:
    def __init__(self, *, endpoint: str = METAR_ENDPOINT) -> None:
        self._endpoint = endpoint

    async def fetch(self, request, fetcher, *, now):
        if request.station_selection_reason:
            raise ProviderBlocked(request.station_selection_reason)
        if request.station_selection_applied and not request.selected_station_ids:
            return ()
        south = min(item.latitude for item in request.targets) - 2.0
        north = max(item.latitude for item in request.targets) + 2.0
        west = min(item.longitude for item in request.targets) - 3.0
        east = max(item.longitude for item in request.targets) + 3.0
        params = {
            "format": "json",
            "hours": "1",
        }
        if request.station_selection_applied:
            params["ids"] = ",".join(sorted(set(request.selected_station_ids)))
        else:
            params["bbox"] = f"{south:.3f},{west:.3f},{north:.3f},{east:.3f}"
        try:
            payload = await fetcher.get_json(
                self._endpoint,
                params=params,
                maximum_bytes=8 * 1024 * 1024,
            )
        except HttpStatusError as error:
            if error.status_code == 204:
                return ()
            raise
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
                for key, kind, label in (
                    ("wspd", ObservationKind.WIND_SPEED, "wind_speed"),
                    ("wgst", ObservationKind.WIND_SPEED, "wind_gust"),
                ):
                    knots = _optional_float(station.get(key))
                    if knots is not None:
                        wind_common = dict(common)
                        wind_common["metadata"] = {
                            "station_id": station_id,
                            "measurement": label,
                        }
                        observations.append(NormalizedObservation.create(
                            **wind_common,
                            kind=kind,
                            value=knots * 0.514444,
                            unit="m/s",
                        ))
                direction = _optional_float(station.get("wdir"))
                if direction is not None:
                    direction_common = dict(common)
                    direction_common["metadata"] = {
                        "station_id": station_id,
                        "measurement": "wind_direction",
                    }
                    observations.append(NormalizedObservation.create(
                        **direction_common,
                        kind=ObservationKind.WIND_DIRECTION,
                        value=direction,
                        unit="°",
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
        refresh_token_state_path: str | Path | None = None,
    ) -> None:
        self._fetcher = fetcher
        self._client_id = client_id or os.getenv("STV3_NETATMO_CLIENT_ID")
        self._client_secret = client_secret or os.getenv("STV3_NETATMO_CLIENT_SECRET")
        self._refresh_token = refresh_token or os.getenv("STV3_NETATMO_REFRESH_TOKEN")
        self._endpoint = endpoint
        self._monotonic = monotonic
        raw_state_path = (
            refresh_token_state_path
            if refresh_token_state_path is not None
            else os.getenv("STV3_NETATMO_TOKEN_STATE")
        )
        self._refresh_token_state_path = (
            None if not raw_state_path else Path(raw_state_path)
        )
        if self._refresh_token_state_path is not None:
            try:
                state = json.loads(
                    self._refresh_token_state_path.read_text(encoding="utf-8")
                )
            except (OSError, json.JSONDecodeError):
                state = None
            persisted = state.get("refresh_token") if isinstance(state, Mapping) else None
            if isinstance(persisted, str) and persisted.strip():
                self._refresh_token = persisted.strip()
        self._access_token: str | None = None
        self._expires_at = 0.0

    async def get_access_token(self) -> str | None:
        now = self._monotonic()
        if self._access_token and now < self._expires_at:
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
        rotated_refresh_token = (
            payload.get("refresh_token") if isinstance(payload, Mapping) else None
        )
        if (
            isinstance(rotated_refresh_token, str)
            and rotated_refresh_token.strip()
            and rotated_refresh_token.strip() != self._refresh_token
        ):
            self._refresh_token = rotated_refresh_token.strip()
            self._persist_refresh_token()
        self._access_token = token.strip()
        self._expires_at = now + max(60.0, (expires_in or 3600.0) - 30.0)
        return self._access_token

    def _persist_refresh_token(self) -> None:
        path = self._refresh_token_state_path
        if path is None or not self._refresh_token:
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(path.name + ".tmp")
        descriptor = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
            0o600,
        )
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
                json.dump({"refresh_token": self._refresh_token}, stream)
                stream.write("\n")
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, path)
            path.chmod(0o600)
        finally:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass


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
        if request.station_selection_reason:
            raise ProviderBlocked(request.station_selection_reason)
        access_token = self._access_token
        if not access_token and self._access_token_provider is not None:
            access_token = await self._access_token_provider()
        if not access_token:
            raise ProviderBlocked("external_access_required")
        if request.station_selection_applied and not request.selected_station_ids:
            return ()
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
            allowed_station_ids = {
                station_id.casefold() for station_id in request.selected_station_ids
            }
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
                if (
                    request.station_selection_applied
                    and station_id.casefold() not in allowed_station_ids
                ):
                    continue
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
                        and not request.station_selection_applied
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


class WundergroundAdapter:
    """Fetch only catalog-selected PWS observations from the official API."""

    def __init__(
        self,
        *,
        api_key: str | None = None,
        endpoint: str = WUNDERGROUND_CURRENT_ENDPOINT,
    ) -> None:
        self._api_key = (api_key if api_key is not None else os.getenv(
            "STV3_WUNDERGROUND_API_KEY", ""
        )).strip()
        self._endpoint = endpoint

    async def fetch(self, request, fetcher, *, now):
        if request.station_selection_reason:
            raise ProviderBlocked(request.station_selection_reason)
        station_ids = tuple(sorted(set(request.selected_station_ids)))
        if not self._api_key:
            raise ProviderBlocked("blocked_api_key")
        if request.station_selection_applied and not station_ids:
            return ()
        if not request.station_selection_applied:
            raise ProviderBlocked("station_catalog_required", ready=False)

        observations: list[NormalizedObservation] = []
        field_map = (
            ("temp", ObservationKind.TEMPERATURE, "°C", "temperature"),
            ("windSpeed", ObservationKind.WIND_SPEED, "km/h", "wind_speed"),
            ("windGust", ObservationKind.WIND_SPEED, "km/h", "wind_gust"),
            ("pressure", ObservationKind.PRESSURE, "hPa", "pressure"),
            ("precipRate", ObservationKind.RAIN_RATE, "mm/h", "rain_rate"),
            ("precipTotal", ObservationKind.RAIN_ACCUMULATION, "mm", "rain_total"),
        )
        for station_id in station_ids:
            payload = await fetcher.get_json(
                self._endpoint,
                params={
                    "stationId": station_id,
                    "format": "json",
                    "units": "m",
                    "numericPrecision": "decimal",
                    "apiKey": self._api_key,
                },
                maximum_bytes=2 * 1024 * 1024,
            )
            rows = payload.get("observations", ()) if isinstance(payload, Mapping) else ()
            if not isinstance(rows, list) or not rows:
                continue
            row = rows[0]
            if not isinstance(row, Mapping):
                continue
            latitude = _optional_float(row.get("lat"))
            longitude = _optional_float(row.get("lon"))
            metric = row.get("metric")
            if latitude is None or longitude is None or not isinstance(metric, Mapping):
                continue
            observed_at = _parse_timestamp(row.get("epoch"), fallback=now)
            values = [
                (kind, _optional_float(metric.get(key)), unit, label)
                for key, kind, unit, label in field_map
            ]
            values.extend((
                (
                    ObservationKind.RELATIVE_HUMIDITY,
                    _optional_float(row.get("humidity")),
                    "%",
                    "humidity",
                ),
                (
                    ObservationKind.WIND_DIRECTION,
                    _optional_float(row.get("winddir")),
                    "°",
                    "wind_direction",
                ),
            ))
            actual_station_id = str(row.get("stationID") or station_id)
            for target in request.targets:
                if _haversine_km(
                    latitude, longitude, target.latitude, target.longitude
                ) > request.ground_radius_km:
                    continue
                for kind, value, unit, label in values:
                    if value is None:
                        continue
                    try:
                        observations.append(NormalizedObservation.create(
                            provider_id=request.provider_id,
                            product_id=request.product_id,
                            target_id=target.target_id,
                            kind=kind,
                            value=value,
                            unit=unit,
                            observed_at=observed_at,
                            received_at=now,
                            latitude=latitude,
                            longitude=longitude,
                            quality=ObservationQuality.GOOD,
                            geometry=_point(longitude, latitude),
                            metadata={
                                "station_id": actual_station_id,
                                "measurement": label,
                            },
                            maximum_age_seconds=MAX_GROUND_SOURCE_AGE_SECONDS,
                        ))
                    except ThinObservationError:
                        continue
        return tuple(observations)


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
    maximum_age_seconds: float = MAX_RADAR_FRAME_AGE_SECONDS,
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
            maximum_age_seconds=maximum_age_seconds,
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
                maximum_age_seconds=maximum_age_seconds,
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
        pixel_x_m = _attribute_number(geographic["geo_pixel_size_x"]) * 1_000.0
        pixel_y_m = _attribute_number(geographic["geo_pixel_size_y"]) * 1_000.0
        column_offset_m = _attribute_number(geographic["geo_column_offset"]) * 1_000.0
        row_offset_m = _attribute_number(geographic["geo_row_offset"]) * 1_000.0
        proj4 = _attribute_text(root["geographic/map_projection"].attrs["projection_proj4_params"])
        projected_crs = _projected_crs_in_metres(proj4)
        geographic_crs = projected_crs.geodetic_crs
        if geographic_crs is None:
            raise ValueError("knmi_projection_invalid")
        forward = Transformer.from_crs(geographic_crs, projected_crs, always_xy=True)
        inverse = Transformer.from_crs(projected_crs, geographic_crs, always_xy=True)

        def lonlat_to_pixel(longitude: float, latitude: float) -> tuple[float, float]:
            x_value, y_value = forward.transform(longitude, latitude)
            return (
                (y_value + row_offset_m) / pixel_y_m,
                (x_value - column_offset_m) / pixel_x_m,
            )

        def pixel_to_lonlat(row: float, column: float) -> tuple[float, float]:
            return inverse.transform(
                column_offset_m + column * pixel_x_m,
                row * pixel_y_m - row_offset_m,
            )

        observations = _national_radar_observations(
            rain_rate,
            echo_valid,
            request,
            observed_at=observed_at,
            received_at=received_at,
            lonlat_to_pixel=lonlat_to_pixel,
            pixel_to_lonlat=pixel_to_lonlat,
            pixel_width_m=abs(pixel_x_m),
            pixel_height_m=abs(pixel_y_m),
        )
        return ThinAdapterResult(
            observations=observations,
            radar_fields=(ThinRadarField(
                provider_id=request.provider_id,
                product_id=request.product_id,
                observed_at=observed_at,
                crs=projected_crs.to_string(),
                origin_x_m=column_offset_m,
                origin_y_m=-row_offset_m,
                x_resolution_m=abs(pixel_x_m),
                y_resolution_m=abs(pixel_y_m),
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
    "AemetRadarAdapter",
    "AioHttpFetcher",
    "AsyncHttpFetcher",
    "BlitzortungAdapter",
    "DwdRadolanAdapter",
    "DpcRadarAdapter",
    "GeoSphereNowcastAdapter",
    "ItaliaMeteoDpcHistoricalCatalogAdapter",
    "ItaliaMeteoIcon2iAdapter",
    "KmiAwsAdapter",
    "KmiRadarAdapter",
    "KnmiRadarAdapter",
    "MetarAdapter",
    "MeteoFranceOAuthTokenProvider",
    "MeteoFranceRadarAdapter",
    "MetOfficeRadarAdapter",
    "MeteoLuxValidationAdapter",
    "NetatmoAdapter",
    "NetatmoOAuthTokenProvider",
    "OpenMeteoAdapter",
    "OperaRadarAdapter",
    "ProviderBlocked",
    "ProviderStale",
    "RainViewerRadarAdapter",
    "ThinAdapterResult",
    "ThinRadarField",
    "WundergroundAdapter",
]
