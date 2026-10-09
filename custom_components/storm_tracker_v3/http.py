"""Beveiligde HTTP-feed voor kaartclients."""
from __future__ import annotations

import time

from homeassistant.components.http import HomeAssistantView

from .const import DOMAIN
from .geojson_worker import async_refresh_worker_geojson


GEOJSON_REFRESH_SECONDS = 30.0


class StormTrackerGeoJsonView(HomeAssistantView):
    """Lever actuele GeoJSON zonder de Recorder-database te belasten."""

    url = "/api/storm_tracker_v3/geojson"
    name = "api:storm_tracker_v3:geojson"
    requires_auth = True

    def __init__(self, hass) -> None:
        self._hass = hass

    async def get(self, request):
        data = self._hass.data.get(DOMAIN, {})
        collection = data.get("geojson_worker_collection")
        updated_at = float(
            (data.get("geojson_worker_status") or {}).get("updated_at", 0.0)
            or 0.0
        )
        if (
            data.get("runtime_mode") != "legacy"
            and (
                not isinstance(collection, dict)
                or time.time() - updated_at >= GEOJSON_REFRESH_SECONDS
            )
        ):
            await async_refresh_worker_geojson(self._hass)
            collection = data.get("geojson_worker_collection")
        if not isinstance(collection, dict):
            collection = {
                "type": "FeatureCollection",
                "features": [],
                "metadata": {
                    "feature_count": 0,
                    "state": "wacht_op_worker",
                },
            }
        collection["radar_overlays"] = data.get("radar_overlays_by_engine", {})
        collection["pysteps_input"] = data.get("pysteps_input", {})
        return self.json(collection)
