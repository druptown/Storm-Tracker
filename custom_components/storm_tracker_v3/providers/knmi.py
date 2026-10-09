"""Storm Tracker V3 — providers/knmi.py v0.2.0

Provider: KNMI radar

Datasets:
  - radar_forecast_2.0: providerdata tot 120 min; operationeel gebruiken we 0-90 min
  - nl-rdr-data-rtcor-5m: huidig gecorrigeerd radar (vereist API key)

Dekking: lat 48.9-56.0°N, lon 0.0-10.9°O (Nederland + Noord-België + omgeving)

Versiegeschiedenis:
  v0.2.0 — correcte dataset/layer namen via GetCapabilities,
            juiste geo-grenzen, nowcast via TIME parameter
  v0.1.0 — eerste versie (verkeerde dataset/layer namen)
"""
from __future__ import annotations

import asyncio
import logging
import math
from datetime import datetime, timezone, timedelta
from typing import Optional

import aiohttp

from ..engine.bounded_payload import iter_limited_async
from ..engine.observation import Observation, ObservationType
from .raster_components import extract_components, extract_intensity_runs

_LOGGER = logging.getLogger(__name__)

KNMI_WMS_BASE  = "https://api.dataplatform.knmi.nl/wms/adaguc-server"
KNMI_TIMEOUT_S = 20
KNMI_IMG_SIZE  = 512
KNMI_FORECAST_CONCURRENCY = 5
KNMI_FORECAST_HORIZON_MIN = 90
KNMI_WMS_MAX_RESPONSE_BYTES = 8 * 1024 * 1024
_HTTP_CHUNK_BYTES = 64 * 1024

# Exacte dekkingsgrenzen uit GetCapabilities (EPSG:4326)
KNMI_LAT_MIN = 48.895
KNMI_LAT_MAX = 55.974
KNMI_LON_MIN = 0.0
KNMI_LON_MAX = 10.856

# Nowcast dataset (anonieme toegang mogelijk)
DATASET_FORECAST = "radar_forecast_2.0"
LAYER_FORECAST   = "precipitation_nowcast"

# Huidig gecorrigeerd radar (vereist API key)
DATASET_CURRENT  = "nl-rdr-data-rtcor-5m"
LAYER_CURRENT    = "RAD_NL25_RAC_RT"


async def _read_bounded_response(response, *, maximum_bytes: int) -> bytes:
    """Lees één ongecomprimeerde HTTP-body onder een expliciet bytecontract."""
    encoding = response.headers.get("Content-Encoding")
    if encoding and encoding.lower() != "identity":
        raise ValueError("KNMI-response gebruikt onverwachte transportcompressie")
    return await iter_limited_async(
        response.content.iter_chunked(_HTTP_CHUNK_BYTES),
        maximum_bytes=maximum_bytes,
        declared_bytes=response.headers.get("Content-Length"),
        require_declared_match=True,
    )


def _pixel_to_latlon(px: int, py: int, width: int, height: int) -> tuple[float, float]:
    """Pixel naar lat/lon binnen het KNMI dekkingsgebied."""
    lon = KNMI_LON_MIN + (px / width) * (KNMI_LON_MAX - KNMI_LON_MIN)
    lat = KNMI_LAT_MAX - (py / height) * (KNMI_LAT_MAX - KNMI_LAT_MIN)
    return round(lat, 4), round(lon, 4)


def _intensity_from_rgba(r: int, g: int, b: int, a: int) -> int:
    """
    Schat neerslagintensiteit (0-8) uit KNMI WMS pixelkleur.
    Gebruikt de radar/nearest stijl — blauw=licht, rood=zwaar.
    """
    if a < 64:
        return 0
    # Donkerblauw = drizzle, lichtblauw = licht, groen = matig,
    # geel = fors, oranje = zwaar, rood = zeer zwaar
    if r < 50 and g < 50 and b > 100:
        return 2
    if r < 100 and g > 100 and b > 150:
        return 3
    if r < 50 and g > 150 and b < 100:
        return 4
    if r > 200 and g > 200 and b < 50:
        return 5
    if r > 220 and g > 100 and b < 50:
        return 6
    if r > 200 and g < 80 and b < 80:
        return 7
    if r > 150 and g < 50 and b > 100:
        return 8
    if r + g + b > 100:
        return 1
    return 0


class KnmiProvider:
    """
    Haalt KNMI radardata op via WMS GetMap over het volledige dekkingsgebied.
    Levert alle natte pixels als RADAR Observations met werkelijke lat/lon.
    """

    def __init__(self, lat: float, lon: float, api_key: str, wms_api_key: str = None, hass=None) -> None:
        self._lat      = lat
        self._lon      = lon
        self._api_key  = api_key
        self._wms_key  = wms_api_key or api_key
        self._hass = hass
        self._headers = {
            "Accept-Encoding": "identity",
            "Authorization": api_key,
        }
        self._wms_headers = {
            "Accept-Encoding": "identity",
            "Authorization": self._wms_key,
        }
        self.overlay = None
        self.last_frame_timestamp: Optional[float] = None
        self.last_fetch_success = False

    async def fetch_observations(self) -> list[Observation]:
        self.last_fetch_success = False
        if self._hass is not None:
            try:
                from ..worker_radar_client import async_fetch_worker_radar

                observations, overlay, _, artifact = await async_fetch_worker_radar(
                    self._hass,
                    "knmi_fetch",
                    {"credential_ref": "worker_environment"},
                    provider="knmi",
                    timeout=115,
                    max_age_seconds=20 * 60,
                    allowed_sources=frozenset({"knmi", "knmi_forecast"}),
                )
                self.overlay = overlay
                self.last_worker_artifact = artifact
                self.last_frame_timestamp = float(overlay["timestamp"])
                self.last_fetch_success = True
                return observations
            except Exception as err:
                _LOGGER.warning(
                    "KNMI-worker niet beschikbaar; lokale decode is bewust uitgeschakeld: %s",
                    err,
                )
                return []
        obs = []

        # Huidige neerslag (gecorrigeerd)
        current = await self._fetch_frame(
            DATASET_CURRENT, LAYER_CURRENT,
            self._current_time_str(), "knmi", auth=True
        )
        obs.extend(current)

        # Haal vijfminutenstappen op tot dezelfde 90-minutenhorizon als de
        # operationele waarschuwing. De provider biedt ook 120 minuten,
        # maar die extra stappen worden nergens operationeel gebruikt.
        now = datetime.now(timezone.utc)
        semaphore = asyncio.Semaphore(KNMI_FORECAST_CONCURRENCY)

        async def _fetch_forecast_step(step: int) -> list[Observation]:
            future   = now + timedelta(minutes=step)
            time_str = future.replace(second=0, microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")
            source   = "knmi_forecast" if step > 0 else "knmi"
            async with semaphore:
                return await self._fetch_frame(
                    DATASET_FORECAST, LAYER_FORECAST,
                    time_str, source, auth=True,
                )

        frames = await asyncio.gather(*(
            _fetch_forecast_step(step)
            for step in range(0, KNMI_FORECAST_HORIZON_MIN + 1, 5)
        ))
        for frame in frames:
            obs.extend(frame)

        _LOGGER.debug(
            "KnmiProvider: %d totaal (%d huidig, %d nowcast, 19 tijdstappen)",
            len(obs),
            sum(1 for o in obs if o.source == "knmi"),
            sum(1 for o in obs if o.source == "knmi_forecast"),
        )
        return obs

    def _current_time_str(self) -> str:
        now = datetime.now(timezone.utc)
        minutes = (now.minute // 5) * 5
        return now.replace(minute=minutes, second=0, microsecond=0).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )

    async def _fetch_frame(
        self, dataset: str, layer: str, time_str: str, source: str, auth: bool = True
    ) -> list[Observation]:
        """Haal één radarframe op en zet natte pixels om naar Observations."""
        bbox   = f"{KNMI_LON_MIN},{KNMI_LAT_MIN},{KNMI_LON_MAX},{KNMI_LAT_MAX}"
        params = {
            "DATASET": dataset,
            "SERVICE": "WMS",
            # Dit is de WMS-protocolversie, niet de Storm Tracker-versie.
            "VERSION": "1.3.0",
            "REQUEST": "GetMap",
            "LAYERS":  layer,
            "CRS":     "EPSG:4326",
            "BBOX":    bbox,
            "WIDTH":   KNMI_IMG_SIZE,
            "HEIGHT":  KNMI_IMG_SIZE,
            "FORMAT":  "image/png",
            "TIME":    time_str,
            "STYLES":  "radar/nearest",
        }
        headers = self._wms_headers if auth else {}

        try:
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=KNMI_TIMEOUT_S)
            ) as session:
                async with session.get(KNMI_WMS_BASE, params=params, headers=headers) as resp:
                    if resp.status != 200:
                        _LOGGER.debug("KnmiProvider: %d voor %s t=%s", resp.status, dataset, time_str)
                        return []
                    image_data = await _read_bounded_response(
                        resp,
                        maximum_bytes=KNMI_WMS_MAX_RESPONSE_BYTES,
                    )

            return await asyncio.to_thread(
                self._extract_observations,
                image_data,
                time_str,
                source,
            )

        except ImportError:
            _LOGGER.warning("KnmiProvider: Pillow niet beschikbaar")
            return []
        except Exception:
            _LOGGER.exception("KnmiProvider: fout voor %s t=%s", dataset, time_str)
            return []

    def _extract_observations(
        self, image_data: bytes, time_str: str, source: str
    ) -> list[Observation]:
        """Decodeer een WMS-frame buiten de Home Assistant-eventloop."""
        try:
            from PIL import Image
            import io

            img = Image.open(io.BytesIO(image_data)).convert("RGBA")
            w, h   = img.size
            pixels = img.load()
            ts     = datetime.fromisoformat(time_str.replace("Z", "+00:00")).timestamp()
            import numpy as np
            intensity_grid = np.zeros((h, w), dtype=np.uint8)
            for py in range(h):
                for px in range(w):
                    r, g, b, a = pixels[px, py]
                    intensity  = _intensity_from_rgba(r, g, b, a)
                    intensity_grid[py, px] = intensity
            components = extract_components(
                intensity_grid,
                lambda row, col: _pixel_to_latlon(col, row, w, h),
            )
            if source == "knmi":
                self.last_frame_timestamp = ts
                self.last_fetch_success = True
                self.overlay = {
                    "source": "knmi", "timestamp": ts,
                    "runs": extract_intensity_runs(
                        intensity_grid,
                        lambda row, col: _pixel_to_latlon(col, row, w, h),
                    ),
                }
            obs = []
            frame_id = f"{source}:{ts:.0f}"
            for component in components:
                lat, lon = _pixel_to_latlon(
                    component.centroid_col, component.centroid_row, w, h
                )
                lat_km = (KNMI_LAT_MAX - KNMI_LAT_MIN) / h * 110.574
                lon_km = (
                    (KNMI_LON_MAX - KNMI_LON_MIN) / w * 111.320
                    * max(0.1, abs(math.cos(math.radians(lat))))
                )
                area_km2 = len(component.pixels) * lat_km * lon_km
                component_id = f"{frame_id}:c{component.index}"
                obs.append(Observation(
                    obs_type=ObservationType.RADAR,
                    lat=lat, lon=lon, timestamp=ts,
                    intensity=component.max_intensity,
                    area_km2=area_km2, quality=0.98,
                    footprint_points=component.boundary,
                    footprint_polygons=component.polygons,
                    radar_cell_id=component_id,
                    parent_system_id=component_id,
                    parent_area_km2=area_km2,
                    parent_footprint_points=component.boundary,
                    parent_footprint_polygons=component.polygons,
                    source=source,
                ))

            _LOGGER.debug("KnmiProvider: %d neerslagclusters t=%s", len(obs), time_str)
            return obs

        except ImportError:
            _LOGGER.warning("KnmiProvider: Pillow niet beschikbaar")
            return []
        except Exception:
            _LOGGER.exception("KnmiProvider: parsefout t=%s", time_str)
            return []


class KnmiProviderFactory:
    """
    Factory voor KnmiProvider.
    Dekkingsgebied: lat 48.9-56.0°N, lon 0.0-10.9°O
    """
    BUFFER_KM = 100.0

    def __init__(self, api_key: str, wms_api_key: str = None) -> None:
        self._api_key = api_key
        self._wms_key = wms_api_key or api_key

    @classmethod
    def supports(cls, center_lat: float, center_lon: float, radius_km: float) -> bool:
        nearest_lat = max(KNMI_LAT_MIN, min(center_lat, KNMI_LAT_MAX))
        nearest_lon = max(KNMI_LON_MIN, min(center_lon, KNMI_LON_MAX))
        dlat_km = abs(center_lat - nearest_lat) * 111.32
        dlon_km = abs(center_lon - nearest_lon) * 111.32 * math.cos(math.radians(center_lat))
        dist_km = math.sqrt(dlat_km ** 2 + dlon_km ** 2)
        return dist_km <= cls.BUFFER_KM

    def create(self, hass, center_lat: float, center_lon: float, radius_km: float):
        if not self.supports(center_lat, center_lon, radius_km):
            return None
        return KnmiProvider(center_lat, center_lon, self._api_key, self._wms_key if hasattr(self, "_wms_key") else self._api_key)
