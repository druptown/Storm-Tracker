"""GeoSphere Austria INCA-nowcast als officiële lokale validatiebron."""
from __future__ import annotations

from datetime import datetime

from .base import Capability, CoverageResult

URL = "https://dataset.api.hub.geosphere.at/v1/timeseries/forecast/nowcast-v1-15min-1km"


def summarize_nowcast(payload: dict) -> dict:
    """Vat de 15-minutenneerslag voor één locatie compact samen."""
    features = payload.get("features") or []
    values = []
    if features:
        values = (((features[0].get("properties") or {}).get("parameters") or {}).get("rr") or {}).get("data") or []
    numeric = [float(value) for value in values if value is not None]
    return {
        "reference_time": payload.get("reference_time"),
        "forecast_steps": len(numeric),
        "rain_next_3h_mm": round(sum(numeric), 2),
        "max_15min_mm": round(max(numeric, default=0.0), 2),
    }


class GeoSphereAustriaProvider:
    plugin_id = "geosphere_at"
    capabilities = frozenset({Capability.NOWCAST})
    priority = 100

    def __init__(self, session):
        self._session, self._areas = session, ()
        self.diagnostics = {}

    def supports(self, area):
        margin = area.horizon_km / 100.0
        ok = 45.5 - margin <= area.center_lat <= 49.5 + margin and 8.1 - margin <= area.center_lon <= 17.75 + margin
        return CoverageResult(ok, 1.0 if ok else 0.0, 0.97 if ok else 0.0, "GeoSphere INCA 1 km" if ok else "buiten GeoSphere-dekking")

    async def async_start(self, context): self._areas = tuple(context.config.get("areas", (context.area,)))
    async def async_update_areas(self, areas): self._areas = tuple(areas)
    async def async_stop(self): self._areas = ()

    async def async_fetch(self):
        summaries = []
        for area in self._areas:
            async with self._session.get(URL, params={"parameters": "rr", "lat_lon": f"{area.center_lat},{area.center_lon}", "output_format": "geojson"}) as response:
                response.raise_for_status()
                summaries.append(summarize_nowcast(await response.json()))
        self.diagnostics = {
            "source_role": "nowcast_validation",
            "areas_queried": len(summaries),
            "rain_next_3h_mm_max": max((item["rain_next_3h_mm"] for item in summaries), default=0.0),
            "reference_time": max((item["reference_time"] for item in summaries if item["reference_time"]), default=None),
            "official_api": URL,
        }
        return []
