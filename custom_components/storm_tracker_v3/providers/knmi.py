"""Storm Tracker V3 — providers/knmi.py v0.2.0

Provider: KNMI radar

Datasets:
  - radar_forecast_2.0: nowcast 0-120 min, layer: precipitation_nowcast
  - nl-rdr-data-rtcor-5m: huidig gecorrigeerd radar (vereist API key)

Dekking: lat 48.9-56.0°N, lon 0.0-10.9°O (Nederland + Noord-België + omgeving)

Versiegeschiedenis:
  v0.2.0 — correcte dataset/layer namen via GetCapabilities,
            juiste geo-grenzen, nowcast via TIME parameter
  v0.1.0 — eerste versie (verkeerde dataset/layer namen)
"""
from __future__ import annotations

import logging
import math
from datetime import datetime, timezone, timedelta
from typing import Optional

import aiohttp

from ..engine.observation import Observation, ObservationType
from .raster_components import extract_components, extract_intensity_runs

_LOGGER = logging.getLogger(__name__)

KNMI_WMS_BASE  = "https://api.dataplatform.knmi.nl/wms/adaguc-server"
KNMI_TIMEOUT_S = 20
KNMI_IMG_SIZE  = 512

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

    def __init__(self, lat: float, lon: float, api_key: str, wms_api_key: str = None) -> None:
        self._lat      = lat
        self._lon      = lon
        self._api_key  = api_key
        self._wms_key  = wms_api_key or api_key
        self._headers     = {"Authorization": api_key}
        self._wms_headers = {"Authorization": self._wms_key}
        self.overlay = None

    async def fetch_observations(self) -> list[Observation]:
        obs = []

        # Huidige neerslag (gecorrigeerd)
        current = await self._fetch_frame(
            DATASET_CURRENT, LAYER_CURRENT,
            self._current_time_str(), "knmi", auth=True
        )
        obs.extend(current)

        # Nowcast: alle 25 tijdstappen +0 tot +120 min (elke 5 min)
        now = datetime.now(timezone.utc)
        for step in range(0, 121, 5):
            future   = now + timedelta(minutes=step)
            time_str = future.replace(second=0, microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")
            source   = "knmi_forecast" if step > 0 else "knmi"
            frame    = await self._fetch_frame(
                DATASET_FORECAST, LAYER_FORECAST, time_str, source, auth=True
            )
            obs.extend(frame)

        _LOGGER.debug(
            "KnmiProvider: %d totaal (%d huidig, %d nowcast, 25 tijdstappen)",
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
                    image_data = await resp.read()

            from PIL import Image
            import io

            img    = Image.open(io.BytesIO(image_data)).convert("RGBA")
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
                    radar_cell_id=component_id,
                    parent_system_id=component_id,
                    parent_area_km2=area_km2,
                    parent_footprint_points=component.boundary,
                    source=source,
                ))

            _LOGGER.debug("KnmiProvider: %d neerslagclusters t=%s", len(obs), time_str)
            return obs

        except ImportError:
            _LOGGER.warning("KnmiProvider: Pillow niet beschikbaar")
            return []
        except Exception:
            _LOGGER.exception("KnmiProvider: fout voor %s t=%s", dataset, time_str)
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
