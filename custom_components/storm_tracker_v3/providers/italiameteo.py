"""ItaliaMeteo Radar SRI DPC catalogus als lokale radarvalidatiebron."""
from __future__ import annotations

from datetime import date, datetime, timezone
import json

from .base import Capability, CoverageResult

CATALOG_URL = "https://meteohub.agenziaitaliameteo.it/api/datasets/radar_sri_dpc/opendata"
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"


def latest_bundle(items: list[dict]) -> dict | None:
    candidates = [item for item in items if item.get("filename") and item.get("date")]
    return max(candidates, key=lambda item: item["date"], default=None)


def decode_json_response(text: str) -> dict:
    """Weiger een misleidende HTTP-200 tekst/HTML-respons expliciet."""
    payload = json.loads(text)
    if not isinstance(payload, dict):
        raise ValueError("forecastrespons is geen JSON-object")
    return payload


class ItaliaMeteoRadarProvider:
    plugin_id = "italiameteo"
    capabilities = frozenset({Capability.RADAR, Capability.NOWCAST})
    priority = 100

    def __init__(self, session):
        self._session, self._areas = session, ()
        self.diagnostics = {}

    def supports(self, area):
        margin = area.horizon_km / 90.0
        ok = 35.0 - margin <= area.center_lat <= 48.5 + margin and 5.5 - margin <= area.center_lon <= 19.0 + margin
        return CoverageResult(ok, 1.0 if ok else 0.0, 0.75 if ok else 0.0, "ItaliaMeteo Radar SRI DPC" if ok else "buiten ItaliaMeteo-dekking")

    async def async_start(self, context): self._areas = tuple(context.config.get("areas", (context.area,)))
    async def async_update_areas(self, areas): self._areas = tuple(areas)
    async def async_stop(self): self._areas = ()

    async def async_fetch(self):
        async with self._session.get(CATALOG_URL) as response:
            response.raise_for_status()
            items = await response.json()
        latest = latest_bundle(items if isinstance(items, list) else [])
        bundle_date = date.fromisoformat(latest["date"]) if latest else None
        age_days = (datetime.now(timezone.utc).date() - bundle_date).days if bundle_date else None
        forecasts = []
        forecast_errors = []
        for area in self._areas:
            base_params = {
                "latitude": area.center_lat, "longitude": area.center_lon,
                "models": "italia_meteo_arpae_icon_2i",
                "forecast_hours": 6, "timezone": "UTC",
            }
            forecast = None
            for variables in ("precipitation,lightning_potential", "precipitation"):
                try:
                    async with self._session.get(
                        FORECAST_URL, params={**base_params, "hourly": variables}
                    ) as response:
                        response.raise_for_status()
                        forecast = decode_json_response(await response.text())
                    break
                except Exception as exc:
                    forecast_errors.append(type(exc).__name__)
            if forecast is None:
                continue
            hourly = forecast.get("hourly") or {}
            precipitation = [float(value or 0) for value in hourly.get("precipitation", [])]
            lightning = [float(value or 0) for value in hourly.get("lightning_potential", [])]
            forecasts.append({
                "rain_next_6h_mm": round(sum(precipitation), 2),
                "max_hourly_mm": round(max(precipitation, default=0.0), 2),
                "max_lightning_potential": round(max(lightning, default=0.0), 2),
            })
        self.diagnostics = {
            "source_role": "forecast_and_historical_radar_validation",
            "latest_bundle_date": bundle_date.isoformat() if bundle_date else None,
            "latest_filename": latest.get("filename") if latest else None,
            "age_days": age_days,
            "operational": age_days == 0,
            "fallback_reason": None if age_days == 0 else "bundel niet realtime; OPERA/RainViewer blijft operationeel",
            "forecast_model": "ItaliaMeteo-ARPAE ICON-2I via Open-Meteo",
            "forecast_areas": len(forecasts),
            "rain_next_6h_mm_max": max((item["rain_next_6h_mm"] for item in forecasts), default=0.0),
            "max_lightning_potential": max((item["max_lightning_potential"] for item in forecasts), default=0.0),
            "forecast_errors": forecast_errors,
            "forecast_healthy": bool(forecasts),
            "official_api": CATALOG_URL,
        }
        # De dagelijkse GRIB-bundel wordt bewust niet als actuele radar gevoed.
        return []
