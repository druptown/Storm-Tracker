"""Beveiligde HTTP-feed voor kaartclients."""
from __future__ import annotations

from homeassistant.components.http import HomeAssistantView

from .const import DOMAIN
from .geojson_http_payload import with_radar_overlays


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
        if not isinstance(collection, dict):
            collection = {
                "type": "FeatureCollection",
                "features": [],
                "metadata": {
                    "feature_count": 0,
                    "state": "wacht_op_worker",
                },
            }
        collection = with_radar_overlays(
            collection,
            data.get("radar_overlays_by_engine", {}),
        )
        collection["pysteps_input"] = data.get("pysteps_input", {})
        return self.json(collection)
