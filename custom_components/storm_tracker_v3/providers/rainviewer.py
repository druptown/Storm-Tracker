"""Storm Tracker V3 — providers/rainviewer.py v0.1.0

Provider: RainViewer wereldwijde radar tiles

Haalt radardata op via de publieke RainViewer API.
Downloadt een 3×3 grid van tiles rond de tracker locatie en converteert
alle natte pixels naar RADAR Observations met hun werkelijke lat/lon.

Geen authenticatie nodig. Wereldwijde dekking.

Versiegeschiedenis:
  v0.1.0 — eerste versie; 3×3 tile grid; pixel → lat/lon conversie

Provider: RainViewer

Verantwoordelijkheid: uitsluitend RADAR Observation-objecten leveren
voor regio's buiten het KMI-dekkingsgebied (USA, Spanje, wereldwijd).
Geen clustering, geen logica — puur data ophalen en doorgeven.

RainViewer levert 256x256 PNG-tiles per zoom/x/y-positie. Voor een
gegeven regio worden de relevante tiles berekend, opgehaald en
gecombineerd tot RADAR Observations.
"""
from __future__ import annotations

import logging
import math
import time
from typing import Optional

import aiohttp

from ..engine.observation import Observation, ObservationType

_LOGGER = logging.getLogger(__name__)

RAINVIEWER_API_URL = "https://api.rainviewer.com/public/weather-maps.json"
RAINVIEWER_TIMEOUT = 15
TILE_ZOOM          = 5   # zoom-level 5 = ~300km per tile
TILE_GRID          = 2   # 2x2 grid = ~600km x ~600km rond het centrum


def _latlon_to_tile(lat: float, lon: float, zoom: int) -> tuple[int, int]:
    """Converteer lat/lon naar tegel-coördinaten (x, y) bij gegeven zoom."""
    n    = 2 ** zoom
    x    = int((lon + 180.0) / 360.0 * n)
    lat_r = math.radians(lat)
    y    = int((1.0 - math.log(math.tan(lat_r) + 1.0 / math.cos(lat_r)) / math.pi) / 2.0 * n)
    return x, y


def _tile_bounds(x: int, y: int, zoom: int) -> tuple[float, float, float, float]:
    """Geef de geo-grenzen (lat_top, lat_bottom, lon_left, lon_right) van een tile."""
    n       = 2 ** zoom
    lon_l   = x / n * 360.0 - 180.0
    lon_r   = (x + 1) / n * 360.0 - 180.0
    lat_t   = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * y / n))))
    lat_b   = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * (y + 1) / n))))
    return lat_t, lat_b, lon_l, lon_r


def _pixel_to_latlon_tile(
    px: int, py: int, tile_size: int,
    lat_top: float, lat_bottom: float, lon_left: float, lon_right: float,
) -> tuple[float, float]:
    """Converteer een pixel binnen een tile naar lat/lon."""
    lat = lat_top - (py / tile_size) * (lat_top - lat_bottom)
    lon = lon_left + (px / tile_size) * (lon_right - lon_left)
    return lat, lon


class RainViewerProvider:
    """
    Haalt RainViewer-radarafbeeldingen op en converteert natte pixels
    naar RADAR Observation-objecten.

    Wordt periodiek aangeroepen door de RegionEngine. Beheert zelf
    geen pollingsinterval.
    """

    def __init__(self, center_lat: float, center_lon: float) -> None:
        """
        Args:
            center_lat/center_lon: centrum van de regio — bepaalt welke
            tiles worden opgehaald.
        """
        self._center_lat  = center_lat
        self._center_lon  = center_lon
        self._last_path:  Optional[str] = None   # om dubbele frames te skippen
        self._last_observations: list[Observation] = []

    async def fetch_observations(self) -> list[Observation]:
        """
        Haal het meest recente RainViewer-radarframe op en converteer
        natte pixels naar RADAR Observations.
        """
        try:
            path = await self._fetch_latest_path()
            if path is None:
                return []
            if path == self._last_path:
                return list(self._last_observations)
            self._last_path = path
            observations = await self._fetch_tile_observations(path)
            self._last_observations = list(observations)
            return observations

        except Exception:
            _LOGGER.exception("RainViewerProvider: fout bij ophalen radardata")
            return []

    async def _fetch_latest_path(self) -> Optional[str]:
        """Haal het API-manifest op en geef het pad van het meest recente frame."""
        try:
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=RAINVIEWER_TIMEOUT)
            ) as session:
                async with session.get(RAINVIEWER_API_URL) as resp:
                    if resp.status != 200:
                        return None
                    data = await resp.json(content_type=None)
                    radar = data.get("radar", {}).get("past", [])
                    if not radar:
                        return None
                    host = data.get("host", "https://tilecache.rainviewer.com")
                    return host + radar[-1].get("path", "")
        except Exception:
            _LOGGER.debug("RainViewerProvider: manifest-ophaling fout", exc_info=True)
            return None

    async def _fetch_tile_observations(self, path: str) -> list[Observation]:
        """Haal een 2×2 grid van tiles op rond het centrum voor een groot gebied."""
        cx, cy = _latlon_to_tile(self._center_lat, self._center_lon, TILE_ZOOM)
        obs    = []

        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=RAINVIEWER_TIMEOUT)
        ) as session:
            for dx in range(-TILE_GRID // 2, TILE_GRID // 2 + 1):
                for dy in range(-TILE_GRID // 2, TILE_GRID // 2 + 1):
                    tx, ty = cx + dx, cy + dy
                    url    = f"{path}/{TILE_ZOOM}/{tx}/{ty}/2/1_0.png"
                    try:
                        async with session.get(url) as resp:
                            if resp.status != 200:
                                continue
                            image_data = await resp.read()
                        obs.extend(self._extract_observations(image_data, tx, ty))
                    except Exception:
                        continue

        _LOGGER.debug("RainViewerProvider: %d observaties over %dx%d tiles",
                      len(obs), TILE_GRID + 1, TILE_GRID + 1)
        return obs

    def _extract_observations(
        self, image_data: bytes, tx: int, ty: int
    ) -> list[Observation]:
        try:
            from PIL import Image
            import io

            tile_size  = 256
            img        = Image.open(io.BytesIO(image_data)).convert("RGBA")
            pixels     = img.load()
            lat_t, lat_b, lon_l, lon_r = _tile_bounds(tx, ty, TILE_ZOOM)
            now        = time.time()
            obs        = []

            stride = 8   # elke 8e pixel (~3.5km op zoom-5)
            for py in range(0, tile_size, stride):
                for px in range(0, tile_size, stride):
                    r, g, b, a = pixels[px, py]
                    if a < 64 or (r + g + b) < 10:
                        continue   # transparant of zwart = droog

                    # Ruwe intensiteit schatten op basis van helderheid
                    intensity = min(8, max(1, int((r + g + b) / 96)))

                    lat, lon = _pixel_to_latlon_tile(
                        px + stride // 2, py + stride // 2, tile_size,
                        lat_t, lat_b, lon_l, lon_r
                    )
                    area_km2 = (stride * (lon_r - lon_l) / tile_size * 111.32) ** 2

                    obs.append(Observation(
                        obs_type  = ObservationType.RADAR,
                        lat       = lat,
                        lon       = lon,
                        timestamp = now,
                        intensity = intensity,
                        area_km2  = area_km2,
                        source    = "rainviewer",
                    ))

            _LOGGER.debug("RainViewerProvider: %d RADAR-observaties gegenereerd", len(obs))
            return obs

        except ImportError:
            _LOGGER.warning(
                "RainViewerProvider: Pillow niet beschikbaar — "
                "radarverwerking overgeslagen."
            )
            return []
        except Exception:
            _LOGGER.exception("RainViewerProvider: fout bij parseren tile")
            return []


class RainViewerProviderFactory:
    """
    Factory voor RainViewerProvider.
    RainViewer dekt de hele wereld — supports() geeft altijd True.
    """

    @staticmethod
    def supports(center_lat: float, center_lon: float, radius_km: float) -> bool:
        return True

    def create(self, hass, center_lat: float, center_lon: float, radius_km: float):
        if not self.supports(center_lat, center_lon, radius_km):
            return None
        return RainViewerProvider(center_lat, center_lon)
