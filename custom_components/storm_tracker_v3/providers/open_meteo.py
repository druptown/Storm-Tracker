"""Storm Tracker V3 — providers/open_meteo.py v0.3.0

Provider: Open-Meteo grid wachthond

Versiegeschiedenis:
  v0.3.0 — kleiner modelgrid, 30-minutencache en Retry-After/backoff bij 429
  v0.2.0 — timezone als array (vereist door Open-Meteo POST API bij meerdere locaties)
            minutely_15 precipitation voor nowcast tot 90 min vooruit
  v0.1.0 — eerste versie
"""
from __future__ import annotations

import asyncio
import logging
import math
import time
from typing import Optional

import aiohttp

_LOGGER = logging.getLogger(__name__)

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
TIMEOUT_S      = 20
CACHE_TTL_S = 30 * 60
INITIAL_BACKOFF_S = 30 * 60
MAX_BACKOFF_S = 6 * 60 * 60

# Grid definitie: (radius_km, aantal_punten)
GRID_RINGS = [
    (5,    8),
    (10,  12),
    (20,  20),
    (30,  28),
    (50,  36),
    (75,  44),
    (100, 52),
    (150, 60),
    (200, 64),
]  # totaal: 324 modelpunten; radar blijft de fijnmazige primaire bron


def _generate_grid(center_lat: float, center_lon: float) -> list[tuple[float, float]]:
    points = []
    cos_lat = math.cos(math.radians(center_lat))
    for radius_km, count in GRID_RINGS:
        for i in range(count):
            angle = 2 * math.pi * i / count
            lat = center_lat + (radius_km / 111.32) * math.cos(angle)
            lon = center_lon + (radius_km / (111.32 * cos_lat)) * math.sin(angle)
            points.append((round(lat, 5), round(lon, 5)))
    return points


class OpenMeteoProvider:
    def __init__(self, lat: float, lon: float) -> None:
        self._lat    = lat
        self._lon    = lon
        self._points = _generate_grid(lat, lon)
        self._last_result: dict = {
            "is_raining":        False,
            "max_precipitation": 0.0,
            "wet_points":        0,
            "wet_now":           0,
            "wet_forecast_90m":  0,
            "total_points":      len(self._points),
            "gear":              "LOW",
            "provider_status":   "initializing",
            "wet_locations_now": [],
        }
        self._last_fetch_monotonic = 0.0
        self._backoff_until = 0.0
        self._backoff_seconds = INITIAL_BACKOFF_S
        self._fetch_sequence = 0
        _LOGGER.info("OpenMeteoProvider: %d gridpunten over 9 cirkels (5-200km)", len(self._points))

    def set_callback(self, cb) -> None: pass
    def start(self) -> None: pass
    def stop(self) -> None: pass

    @property
    def last_result(self) -> dict:
        return self._last_result

    @property
    def is_raining(self) -> bool:
        return self._last_result["is_raining"]

    async def fetch(self) -> dict:
        now = time.monotonic()
        if self._last_fetch_monotonic and now - self._last_fetch_monotonic < CACHE_TTL_S:
            return self._last_result
        if now < self._backoff_until:
            return self._last_result
        try:
            n = len(self._points)
            payload = {
                "latitude":    [p[0] for p in self._points],
                "longitude":   [p[1] for p in self._points],
                "current":     ["precipitation"],
                "minutely_15": ["precipitation"],
                "timezone":    ["UTC"] * n,
            }

            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=TIMEOUT_S)
            ) as session:
                async with session.post(OPEN_METEO_URL, json=payload) as resp:
                    if resp.status == 429:
                        retry_after = _retry_after_seconds(resp)
                        delay = max(self._backoff_seconds, retry_after or 0)
                        delay = min(delay, MAX_BACKOFF_S)
                        self._backoff_until = now + delay
                        self._backoff_seconds = min(delay * 2, MAX_BACKOFF_S)
                        self._last_result["provider_status"] = "rate_limited"
                        _LOGGER.warning(
                            "OpenMeteoProvider: 429; volgende poging over %.0f minuten",
                            delay / 60,
                        )
                        return self._last_result
                    if resp.status != 200:
                        _LOGGER.warning("OpenMeteoProvider: %d", resp.status)
                        return self._last_result
                    data = await resp.json(content_type=None)

            if isinstance(data, list):
                current_values = [
                    float(item.get("current", {}).get("precipitation", 0) or 0)
                    for item in data
                ]
                forecast_values = []
                for item in data:
                    m15 = item.get("minutely_15", {}).get("precipitation", [])
                    forecast_values.append(max((float(v or 0) for v in m15), default=0.0))
            elif isinstance(data, dict):
                current_values  = [float(data.get("current", {}).get("precipitation", 0) or 0)]
                m15 = data.get("minutely_15", {}).get("precipitation", [])
                forecast_values = [max((float(v or 0) for v in m15), default=0.0)]
            else:
                return self._last_result

            all_values        = [max(c, f) for c, f in zip(current_values, forecast_values)]
            wet_points        = sum(1 for v in all_values if v > 0)
            max_precipitation = max(all_values) if all_values else 0.0
            is_raining        = max_precipitation > 0
            wet_now           = sum(1 for v in current_values if v > 0)
            wet_forecast      = sum(1 for v in forecast_values if v > 0)
            wet_locations = [
                {"lat": point[0], "lon": point[1], "mm": round(value, 2)}
                for point, value in zip(self._points, current_values)
                if value > 0
            ]

            self._last_result = {
                "is_raining":        is_raining,
                "max_precipitation": round(max_precipitation, 2),
                "wet_points":        wet_points,
                "wet_now":           wet_now,
                "wet_forecast_90m":  wet_forecast,
                "total_points":      n,
                "gear":              "HIGH" if is_raining else "LOW",
                "provider_status":   "ok",
                "wet_locations_now": wet_locations,
            }
            self._fetch_sequence += 1
            self._last_result["fetch_sequence"] = self._fetch_sequence
            self._last_fetch_monotonic = now
            self._backoff_until = 0.0
            self._backoff_seconds = INITIAL_BACKOFF_S

            _LOGGER.debug(
                "OpenMeteo: nu: %d nat | forecast 90m: %d nat | max %.2f mm",
                wet_now, wet_forecast, max_precipitation
            )
            return self._last_result

        except Exception:
            _LOGGER.exception("OpenMeteoProvider: fout bij ophalen data")
            return self._last_result


def _retry_after_seconds(response) -> Optional[float]:
    """Lees een numerieke Retry-After-header zonder aiohttp-internals."""
    headers = getattr(response, "headers", {}) or {}
    raw = headers.get("Retry-After") or headers.get("retry-after")
    if raw is None:
        return None
    try:
        return max(0.0, float(raw))
    except (TypeError, ValueError):
        return None
