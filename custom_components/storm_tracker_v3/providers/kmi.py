"""Storm Tracker V3 — providers/kmi.py v0.3.0

Provider: KMI radar

Versiegeschiedenis:
  v0.3.0 — volledige radarplaatjes downloaden via URI uit animatiesequentie,
            alle natte pixels → RADAR Observations met lat/lon
  v0.2.0 — correcte API: getForecasts + lat/lon + User-Agent
  v0.1.0 — eerste versie (niet werkend)
"""
from __future__ import annotations

import hashlib
import logging
from datetime import datetime
from typing import Optional

import aiohttp

from ..engine.observation import Observation, ObservationType

_LOGGER = logging.getLogger(__name__)

KMI_BASE_URL  = "https://app.meteo.be/services/appv4/"
KMI_SECRET    = "r9EnW374jkJ9acc"
KMI_UA        = "be.meteo.app"
KMI_TIMEOUT_S = 15
KMI_IMG_SIZE  = 512   # pixels van het radarplaatje

# Geo-grenzen van het KMI radarplaatje
KMI_LAT_TOP    = 53.0
KMI_LAT_BOTTOM = 46.5
KMI_LON_LEFT   = -2.5
KMI_LON_RIGHT  = 10.5


def _kmi_key(service: str) -> str:
    date_str = datetime.now().strftime("%d/%m/%Y")
    raw = f"{KMI_SECRET};{service};{date_str}"
    return hashlib.md5(raw.encode()).hexdigest()


def pixel_to_latlon(px: int, py: int, width: int = KMI_IMG_SIZE, height: int = KMI_IMG_SIZE) -> tuple[float, float]:
    """Converteer KMI-pixelcoördinaat naar (lat, lon)."""
    lat = KMI_LAT_TOP - (py / height) * (KMI_LAT_TOP - KMI_LAT_BOTTOM)
    lon = KMI_LON_LEFT + (px / width) * (KMI_LON_RIGHT - KMI_LON_LEFT)
    return round(lat, 4), round(lon, 4)


def _color_to_intensity(r: int, g: int, b: int, a: int) -> int:
    """Bepaal intensiteit (0-8) op basis van KMI pixelkleur."""
    if a < 128:
        return 0
    # KMI kleurenschaal: groen → blauw → geel → oranje → rood → paars
    colors = [
        ((144, 238, 144), 1), ((0, 200, 100), 2),
        ((0, 150, 255), 3),   ((0, 0, 255),   4),
        ((255, 255, 0), 5),   ((255, 150, 0), 6),
        ((255, 0, 0),   7),   ((180, 0, 180), 8),
    ]
    best, best_dist = 0, float("inf")
    for (cr, cg, cb), val in colors:
        dist = (r-cr)**2 + (g-cg)**2 + (b-cb)**2
        if dist < best_dist:
            best_dist, best = dist, val
    return best if best_dist < 20000 else 0


def _ww_to_text(ww: int) -> str:
    """Vertaal KMI weercode naar leesbare tekst."""
    if ww >= 95: return "Onweer"
    if ww >= 80: return "Buien"
    if ww >= 71: return "Sneeuw"
    if ww >= 61: return "Regen"
    if ww >= 51: return "Motregen"
    if ww >= 40: return "Mist"
    if ww >= 10: return "Bewolkt"
    if ww > 0:   return "Licht bewolkt"
    return "Onbekend"


class KmiProvider:
    """
    Haalt KMI radardata op: eerst metadata via getForecasts, dan het
    volledige radarplaatje downloaden via de URI in de animatiesequentie.
    Alle natte pixels → RADAR Observations met hun werkelijke lat/lon.
    """

    def __init__(self, lat: float, lon: float) -> None:
        self._lat = lat
        self._lon = lon
        self._last_uri:  Optional[str]   = None
        self._last_ww:   int              = 0
        self._last_ww_ts: str             = ""
        self._last_temp: Optional[float]  = None
        self.last_frame_timestamp: Optional[float] = None
        self.last_fetch_updated = False

    async def fetch_observations(self) -> list[Observation]:
        self.last_fetch_updated = False
        try:
            # Stap 1: haal animatiesequentie op voor locatie
            url = (
                f"{KMI_BASE_URL}?s=getForecasts"
                f"&k={_kmi_key('getForecasts')}"
                f"&lat={self._lat}&long={self._lon}"
            )
            headers = {"User-Agent": KMI_UA}

            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=KMI_TIMEOUT_S)
            ) as session:
                async with session.get(url, headers=headers) as resp:
                    if resp.status != 200:
                        _LOGGER.warning("KmiProvider: %d", resp.status)
                        return []
                    data = await resp.json(content_type=None)

            # Weercode uitlezen uit huidige observatie
            obs = data.get("obs", {})
            self._last_ww    = int(obs.get("ww", 0) or 0)
            self._last_ww_ts = obs.get("timestamp", "")
            self._last_temp  = obs.get("temp")

            sequence = data.get("animation", {}).get("sequence", [])
            if not sequence:
                return []

            # Stap 2: meest recente niet-forecast frame kiezen
            now_dt = datetime.now().astimezone()
            historisch = [
                item for item in sequence
                if datetime.fromisoformat(item["time"]) <= now_dt
            ]
            if not historisch:
                historisch = [sequence[0]]

            latest = historisch[-1]
            uri = latest.get("uri", "")

            if not uri or uri == self._last_uri:
                return []
            self._last_uri = uri
            self.last_frame_timestamp = datetime.fromisoformat(latest["time"]).timestamp()

            # Stap 3: volledig radarplaatje downloaden en parsen
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=KMI_TIMEOUT_S)
            ) as session:
                async with session.get(uri, headers=headers) as resp:
                    if resp.status != 200:
                        _LOGGER.warning("KmiProvider: image %d voor %s", resp.status, uri)
                        return []
                    image_data = await resp.read()

            observations = self._extract_observations(
                image_data, self.last_frame_timestamp
            )
            self.last_fetch_updated = True
            return observations

        except Exception:
            _LOGGER.exception("KmiProvider: fout")
            return []

    def _extract_observations(self, image_data: bytes, timestamp: float) -> list[Observation]:
        try:
            from PIL import Image
            import io

            img    = Image.open(io.BytesIO(image_data)).convert("RGBA")
            width, height = img.size
            pixels = img.load()
            obs    = []
            stride = 4   # elke 4e pixel (~5km op KMI-schaal)

            for py in range(0, height, stride):
                for px in range(0, width, stride):
                    r, g, b, a  = pixels[px, py]
                    intensity   = _color_to_intensity(r, g, b, a)
                    if intensity < 1:
                        continue
                    lat, lon = pixel_to_latlon(px + stride // 2, py + stride // 2, width, height)
                    obs.append(Observation(
                        obs_type  = ObservationType.RADAR,
                        lat       = lat,
                        lon       = lon,
                        timestamp = timestamp,
                        intensity = intensity,
                        area_km2  = (stride * 1.3) ** 2,
                        source    = "kmi",
                    ))

            _LOGGER.debug("KmiProvider: %d RADAR-observaties", len(obs))
            return obs

        except ImportError:
            _LOGGER.warning("KmiProvider: Pillow niet beschikbaar")
            return []
        except Exception:
            _LOGGER.exception("KmiProvider: parse fout")
            return []


class KmiProviderFactory:
    LAT_MIN   = 46.5
    LAT_MAX   = 53.0
    LON_MIN   = -2.5
    LON_MAX   = 10.5
    BUFFER_KM = 150.0

    @classmethod
    def supports(cls, center_lat: float, center_lon: float, radius_km: float) -> bool:
        import math
        nearest_lat = max(cls.LAT_MIN, min(center_lat, cls.LAT_MAX))
        nearest_lon = max(cls.LON_MIN, min(center_lon, cls.LON_MAX))
        dlat_km = abs(center_lat - nearest_lat) * 111.32
        dlon_km = abs(center_lon - nearest_lon) * 111.32 * math.cos(math.radians(center_lat))
        dist_km = math.sqrt(dlat_km**2 + dlon_km**2)
        return dist_km <= cls.BUFFER_KM

    def create(self, hass, center_lat: float, center_lon: float, radius_km: float):
        if not self.supports(center_lat, center_lon, radius_km):
            return None
        return KmiProvider(center_lat, center_lon)
