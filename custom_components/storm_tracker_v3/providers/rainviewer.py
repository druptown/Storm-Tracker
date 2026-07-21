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
from dataclasses import dataclass
from typing import Optional

import aiohttp

from ..engine.observation import Observation, ObservationType
from .raster_components import extract_components

_LOGGER = logging.getLogger(__name__)

RAINVIEWER_API_URL = "https://api.rainviewer.com/public/weather-maps.json"
RAINVIEWER_TIMEOUT = 15
RAINVIEWER_MAX_FRAME_AGE_S = 20 * 60
TILE_ZOOM          = 5   # zoom-level 5 = ~300km per tile
TILE_GRID          = 2   # 2x2 grid = ~600km x ~600km rond het centrum
TILE_SIZE          = 256
RAINVIEWER_MIN_DBZ_ALPHA = 130  # Universal Blue: circa 8 dBZ


def _tile_url(path: str, tx: int, ty: int) -> str:
    """Build a current RainViewer v2 tile URL without interpolation."""
    return f"{path}/{TILE_SIZE}/{TILE_ZOOM}/{tx}/{ty}/2/0_0.png"


def _universal_blue_intensity(r: int, g: int, b: int, a: int) -> int:
    """Decode only plausible wet pixels from RainViewer Universal Blue.

    Below 15 dBZ this palette encodes reflectivity primarily through alpha.
    From 15 dBZ onward it is opaque and strongly coloured (or white at the
    extreme end). Grey opaque pixels are not radar and must never corroborate
    OPERA.
    """
    if a < RAINVIEWER_MIN_DBZ_ALPHA:
        return 0
    if a < 255:
        estimated_dbz = 8 + round((a - RAINVIEWER_MIN_DBZ_ALPHA) * 6 / 60)
        return max(1, min(2, 1 + max(0, estimated_dbz - 8) // 6))
    saturation = max(r, g, b) - min(r, g, b)
    if saturation < 40 and min(r, g, b) < 240:
        return 0
    return 3 if max(r, g, b) < 250 else 8


@dataclass(frozen=True, slots=True)
class RainViewerFrame:
    """Metadata van het nieuwste frame uit het RainViewer-manifest."""

    path: str
    timestamp: float


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
        self._last_poll_ts: Optional[float] = None
        self._last_success_ts: Optional[float] = None
        self._last_frame_ts: Optional[float] = None
        self._healthy = False
        self._last_error: Optional[str] = "nog niet opgehaald"
        self._consecutive_failures = 0

    @property
    def healthy(self) -> bool:
        return self._healthy

    @property
    def diagnostics(self) -> dict:
        now = time.time()
        age_s = (
            max(0.0, now - self._last_frame_ts)
            if self._last_frame_ts is not None
            else None
        )
        return {
            "healthy": self._healthy,
            "last_error": self._last_error,
            "last_poll_ts": self._last_poll_ts,
            "last_success_ts": self._last_success_ts,
            "last_frame_ts": self._last_frame_ts,
            "frame_age_minutes": round(age_s / 60.0, 1) if age_s is not None else None,
            "last_path": self._last_path,
            "max_frame_age_minutes": RAINVIEWER_MAX_FRAME_AGE_S // 60,
            "consecutive_failures": self._consecutive_failures,
        }

    def _mark_unhealthy(self, message: str) -> None:
        first_failure = self._consecutive_failures == 0
        self._healthy = False
        self._last_error = message
        self._consecutive_failures += 1
        if first_failure:
            _LOGGER.warning("RainViewerProvider ongezond: %s", message)

    def _mark_healthy(self, now: float) -> None:
        recovered = self._consecutive_failures > 0
        self._healthy = True
        self._last_error = None
        self._last_success_ts = now
        self._consecutive_failures = 0
        if recovered:
            _LOGGER.info("RainViewerProvider opnieuw gezond")

    async def fetch_observations(self) -> list[Observation]:
        """
        Haal het meest recente RainViewer-radarframe op en converteer
        natte pixels naar RADAR Observations.
        """
        now = time.time()
        self._last_poll_ts = now
        try:
            frame = await self._fetch_latest_frame()
            if frame is None:
                self._last_observations = []
                return []

            self._last_frame_ts = frame.timestamp
            age_s = max(0.0, now - frame.timestamp)
            if age_s > RAINVIEWER_MAX_FRAME_AGE_S:
                self._last_path = frame.path
                self._last_observations = []
                self._mark_unhealthy(
                    f"radarframe is {age_s / 60.0:.1f} minuten oud"
                )
                return []

            self._mark_healthy(now)
            if frame.path == self._last_path:
                return list(self._last_observations)
            self._last_path = frame.path
            observations = await self._fetch_tile_observations(
                frame.path, frame.timestamp
            )
            self._last_observations = list(observations)
            return observations

        except Exception as err:
            self._last_observations = []
            self._mark_unhealthy(f"onverwachte fout: {err}")
            _LOGGER.exception("RainViewerProvider: fout bij ophalen radardata")
            return []

    async def _fetch_latest_frame(self) -> Optional[RainViewerFrame]:
        """Haal pad en werkelijke radartijd uit het meest recente manifest."""
        try:
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=RAINVIEWER_TIMEOUT)
            ) as session:
                async with session.get(RAINVIEWER_API_URL) as resp:
                    if resp.status != 200:
                        self._mark_unhealthy(f"manifest gaf HTTP {resp.status}")
                        return None
                    data = await resp.json(content_type=None)
                    radar = data.get("radar", {}).get("past", [])
                    if not radar:
                        self._mark_unhealthy("manifest bevat geen radarframes")
                        return None
                    host = data.get("host", "https://tilecache.rainviewer.com")
                    latest = radar[-1]
                    path = latest.get("path", "")
                    frame_ts = latest.get("time")
                    if not path or not isinstance(frame_ts, (int, float)):
                        self._mark_unhealthy("nieuwste radarframe mist pad of tijdstip")
                        return None
                    return RainViewerFrame(host + path, float(frame_ts))
        except Exception as err:
            self._mark_unhealthy(f"manifest-ophaling mislukt: {err}")
            _LOGGER.debug("RainViewerProvider: manifest-ophaling fout", exc_info=True)
            return None

    async def _fetch_tile_observations(
        self, path: str, frame_timestamp: float
    ) -> list[Observation]:
        """Haal een 2×2 grid van tiles op rond het centrum voor een groot gebied."""
        cx, cy = _latlon_to_tile(self._center_lat, self._center_lon, TILE_ZOOM)
        obs    = []

        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=RAINVIEWER_TIMEOUT)
        ) as session:
            for dx in range(-TILE_GRID // 2, TILE_GRID // 2 + 1):
                for dy in range(-TILE_GRID // 2, TILE_GRID // 2 + 1):
                    tx, ty = cx + dx, cy + dy
                    url    = _tile_url(path, tx, ty)
                    try:
                        async with session.get(url) as resp:
                            if resp.status != 200:
                                continue
                            image_data = await resp.read()
                        obs.extend(self._extract_observations(
                            image_data, tx, ty, frame_timestamp
                        ))
                    except Exception:
                        continue

        _LOGGER.debug("RainViewerProvider: %d observaties over %dx%d tiles",
                      len(obs), TILE_GRID + 1, TILE_GRID + 1)
        return obs

    def _extract_observations(
        self, image_data: bytes, tx: int, ty: int,
        frame_timestamp: Optional[float] = None,
    ) -> list[Observation]:
        try:
            from PIL import Image
            import io

            tile_size  = TILE_SIZE
            img        = Image.open(io.BytesIO(image_data)).convert("RGBA")
            pixels     = img.load()
            lat_t, lat_b, lon_l, lon_r = _tile_bounds(tx, ty, TILE_ZOOM)
            observation_ts = frame_timestamp if frame_timestamp is not None else time.time()
            import numpy as np
            intensity_grid = np.zeros((tile_size, tile_size), dtype=np.uint8)
            for py in range(tile_size):
                for px in range(tile_size):
                    r, g, b, a = pixels[px, py]
                    intensity = _universal_blue_intensity(r, g, b, a)
                    intensity_grid[py, px] = intensity

            components = extract_components(
                intensity_grid,
                lambda row, col: _pixel_to_latlon_tile(
                    col, row, tile_size, lat_t, lat_b, lon_l, lon_r
                ),
            )
            obs = []
            frame_id = f"rainviewer:{observation_ts:.0f}:t{tx}_{ty}"
            for component in components:
                lat, lon = _pixel_to_latlon_tile(
                    component.centroid_col, component.centroid_row, tile_size,
                    lat_t, lat_b, lon_l, lon_r,
                )
                lat_km = abs(lat_t - lat_b) / tile_size * 110.574
                lon_km = (
                    abs(lon_r - lon_l) / tile_size * 111.320
                    * max(0.1, abs(math.cos(math.radians(lat))))
                )
                area_km2 = len(component.pixels) * lat_km * lon_km
                component_id = f"{frame_id}:c{component.index}"
                obs.append(Observation(
                    obs_type=ObservationType.RADAR,
                    lat=lat, lon=lon, timestamp=observation_ts,
                    intensity=component.max_intensity,
                    area_km2=area_km2, quality=0.70,
                    footprint_points=component.boundary,
                    radar_cell_id=component_id,
                    parent_system_id=component_id,
                    parent_area_km2=area_km2,
                    parent_footprint_points=component.boundary,
                    source="rainviewer",
                ))

            _LOGGER.debug("RainViewerProvider: %d neerslagclusters gegenereerd", len(obs))
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
