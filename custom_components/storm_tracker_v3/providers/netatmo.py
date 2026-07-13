"""Storm Tracker V3 — providers/netatmo.py v0.2.0

Provider: Netatmo publieke stationsdata

Verantwoordelijkheid: uitsluitend RAIN Observation-objecten leveren.
Geen clustering, geen logica — puur data ophalen en doorgeven.

Credentials: client_id, client_secret, refresh_token via configuration.yaml
(via !secret verwijzingen naar secrets.yaml)

Versiegeschiedenis:
  v0.2.0 — credentials via configuration.yaml/secrets.yaml,
            token refresh exact zoals netatmo.py AppDaemon app
  v0.1.0 — eerste versie
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Optional

import aiohttp

from ..engine.observation import Observation, ObservationType

_LOGGER = logging.getLogger(__name__)

TOKEN_URL       = "https://api.netatmo.com/oauth2/token"
PUBLIC_DATA_URL = "https://api.netatmo.com/api/getpublicdata"
NETATMO_TIMEOUT = 15
RAIN_THRESHOLD  = 0.1


class NetatmoTokenManager:
    """
    OAuth2 tokenbeheer. Eén instantie per HA — geen conflict met
    AppDaemon app want we bewaren de vernieuwde token enkel intern.
    """

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        refresh_token: str,
    ) -> None:
        self._client_id     = client_id
        self._client_secret = client_secret
        self._refresh_token = refresh_token
        self._access_token: Optional[str] = None
        self._token_expires: float        = 0.0
        self._lock = asyncio.Lock()

    async def get_access_token(self) -> Optional[str]:
        async with self._lock:
            if self._access_token and time.time() < self._token_expires - 60:
                return self._access_token
            await self._refresh()
            return self._access_token

    async def _refresh(self) -> None:
        try:
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=NETATMO_TIMEOUT)
            ) as session:
                async with session.post(TOKEN_URL, data={
                    "grant_type":    "refresh_token",
                    "client_id":     self._client_id,
                    "client_secret": self._client_secret,
                    "refresh_token": self._refresh_token,
                }) as resp:
                    if resp.status != 200:
                        _LOGGER.warning("Netatmo token refresh mislukt: %d", resp.status)
                        return
                    result = await resp.json()
                    self._access_token  = result.get("access_token")
                    self._token_expires = time.time() + result.get("expires_in", 10800)
                    if "refresh_token" in result:
                        self._refresh_token = result["refresh_token"]
                    _LOGGER.debug("Netatmo token vernieuwd")
        except Exception:
            _LOGGER.exception("Netatmo token refresh fout")


class NetatmoProvider:
    """
    Haalt Netatmo grondstationsdata op en levert RAIN Observations.
    Creëert NOOIT zelf een WeatherSystem — puur verificatiebron.
    """

    def __init__(
        self,
        token_manager: NetatmoTokenManager,
        lat: float,
        lon: float,
        radius_km: float = 50.0,
    ) -> None:
        self._token    = token_manager
        self._lat      = lat
        self._lon      = lon
        self._radius   = radius_km
        self._callback = None

    def set_callback(self, on_observation) -> None:
        self._callback = on_observation

    def start(self) -> None:
        _LOGGER.debug("NetatmoProvider gestart voor (%.3f,%.3f) r=%.0fkm",
                      self._lat, self._lon, self._radius)

    def stop(self) -> None:
        pass

    async def fetch_observations(self) -> list[Observation]:
        try:
            token = await self._token.get_access_token()
            if not token:
                return []

            deg = self._radius / 111.0
            params = {
                "lat_ne": self._lat + deg,
                "lon_ne": self._lon + deg,
                "lat_sw": self._lat - deg,
                "lon_sw": self._lon - deg,
                "filter": "true",
            }

            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=NETATMO_TIMEOUT)
            ) as session:
                async with session.get(
                    PUBLIC_DATA_URL,
                    headers={"Authorization": f"Bearer {token}"},
                    params=params,
                ) as resp:
                    if resp.status != 200:
                        _LOGGER.warning("Netatmo data mislukt: %d", resp.status)
                        return []
                    data = await resp.json()

            return self._parse_observations(data)

        except Exception:
            _LOGGER.exception("NetatmoProvider: fout bij ophalen data")
            return []

    def _parse_observations(self, data: dict) -> list[Observation]:
        """
        Parsing exact zoals de originele netatmo.py AppDaemon app (v0.9.13+).
        Per station: rain_live, wind_strength, wind_angle, gust_strength,
        pressure, temperature, humidity.
        """
        now  = time.time()
        obs  = []

        for item in data.get("body", []):
            try:
                loc        = item.get("place", {}).get("location", [0, 0])
                slon       = float(loc[0])
                slat       = float(loc[1])
                station_id = item.get("_id", "")

                station = {
                    "rain_live":     0.0,
                    "rain_5min":     0.0,
                    "pressure":      None,
                    "temperature":   None,
                    "humidity":      None,
                    "wind_strength": None,
                    "wind_angle":    None,
                    "gust_strength": None,
                }

                for module_data in item.get("measures", {}).values():
                    # Directe velden (regen, wind)
                    if "rain_live" in module_data:
                        val = module_data.get("rain_live")
                        if val is not None:
                            station["rain_live"] = max(station["rain_live"], round(float(val), 2))
                    if "rain_60min" in module_data:
                        val = module_data.get("rain_60min")
                        if val is not None:
                            station["rain_5min"] = round(float(val) / 12, 2)
                    if "wind_strength" in module_data:
                        val = module_data.get("wind_strength")
                        if val is not None:
                            station["wind_strength"] = round(float(val), 1)
                    if "wind_angle" in module_data:
                        val = module_data.get("wind_angle")
                        if val is not None:
                            station["wind_angle"] = round(float(val), 0)
                    if "gust_strength" in module_data:
                        val = module_data.get("gust_strength")
                        if val is not None:
                            station["gust_strength"] = round(float(val), 1)

                    # Type/res velden (druk, temp, vochtigheid)
                    res   = module_data.get("res", {})
                    mtype = module_data.get("type", [])
                    if not res:
                        continue
                    latest = list(res.values())[-1] if res else []

                    if "pressure" in mtype and latest:
                        idx = mtype.index("pressure")
                        if idx < len(latest) and latest[idx] is not None:
                            station["pressure"] = round(float(latest[idx]), 1)
                    if "temperature" in mtype and latest:
                        idx = mtype.index("temperature")
                        if idx < len(latest) and latest[idx] is not None:
                            station["temperature"] = round(float(latest[idx]), 1)
                    if "humidity" in mtype and latest:
                        idx = mtype.index("humidity")
                        if idx < len(latest) and latest[idx] is not None:
                            station["humidity"] = round(float(latest[idx]), 0)

                obs.append(Observation(
                    obs_type      = ObservationType.RAIN,
                    lat           = slat,
                    lon           = slon,
                    timestamp     = now,
                    rain_mm       = station["rain_live"],
                    station_id    = station_id,
                    source        = "netatmo",
                    # Extra velden opgeslagen in extra_data
                    wind_speed    = station["wind_strength"],
                    wind_angle    = station["wind_angle"],
                    gust_speed    = station["gust_strength"],
                    pressure      = station["pressure"],
                    temperature   = station["temperature"],
                    humidity      = station["humidity"],
                    rain_5min     = station["rain_5min"],
                ))

            except Exception:
                continue

        raining = sum(1 for o in obs if (o.rain_mm or 0) >= RAIN_THRESHOLD)
        _LOGGER.debug(
            "NetatmoProvider: %d stations, %d met regen, %d met wind, %d met druk",
            len(obs), raining,
            sum(1 for o in obs if getattr(o, "wind_speed", None) is not None),
            sum(1 for o in obs if getattr(o, "pressure", None) is not None),
        )
        return obs


class NetatmoProviderFactory:
    """Factory voor NetatmoProvider. Wereldwijd beschikbaar."""

    def __init__(self, token_manager: NetatmoTokenManager, radius_km: float = 175.0) -> None:
        self._token     = token_manager
        self._radius_km = radius_km

    @staticmethod
    def supports(center_lat: float, center_lon: float, radius_km: float) -> bool:
        return True

    def create(self, hass, center_lat: float, center_lon: float, radius_km: float):
        return NetatmoProvider(self._token, center_lat, center_lon, self._radius_km)
