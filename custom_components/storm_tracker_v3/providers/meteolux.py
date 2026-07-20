"""Publieke MeteoLux-nowcast als lokale validatiebron."""
from __future__ import annotations

from .base import Capability, CoverageResult

URL = "https://metapi.ana.lu/api/v1/metapp/weather"


class MeteoLuxProvider:
    plugin_id = "meteolux"
    capabilities = frozenset({Capability.NOWCAST})
    priority = 100

    def __init__(self, session):
        self._session, self._areas = session, ()
        self.diagnostics = {}

    def supports(self, area):
        margin = area.horizon_km / 100.0
        ok = 48.5 - margin <= area.center_lat <= 51.0 + margin and 4.5 - margin <= area.center_lon <= 7.5 + margin
        return CoverageResult(ok, 1.0 if ok else 0.0, 0.95 if ok else 0.0, "MeteoLux lokale nowcast" if ok else "buiten MeteoLux-dekking")

    async def async_start(self, context): self._areas = tuple(context.config.get("areas", (context.area,)))
    async def async_update_areas(self, areas): self._areas = tuple(areas)
    async def async_stop(self): self._areas = ()

    async def async_fetch(self):
        snapshots = []
        for area in self._areas:
            async with self._session.get(URL, params={"langcode": "en", "lat": area.center_lat, "long": area.center_lon}) as response:
                response.raise_for_status()
                snapshots.append(await response.json())
        self.diagnostics = {"areas_queried": len(snapshots), "source_role": "validation_only", "official_api": URL}
        return []
