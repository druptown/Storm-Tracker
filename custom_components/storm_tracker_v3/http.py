"""Beveiligde HTTP-feed voor kaartclients."""
from __future__ import annotations

from homeassistant.components.http import HomeAssistantView

from .const import DOMAIN
from .engine.geojson import build_feature_collection


class StormTrackerGeoJsonView(HomeAssistantView):
    """Lever actuele GeoJSON zonder de Recorder-database te belasten."""

    url = "/api/storm_tracker_v3/geojson"
    name = "api:storm_tracker_v3:geojson"
    requires_auth = True

    def __init__(self, hass) -> None:
        self._hass = hass

    async def get(self, request):
        data = self._hass.data.get(DOMAIN, {})
        manager = data.get("storm_manager")
        collection = build_feature_collection(
            data.get("targets", {}),
            manager.get_all_engines() if manager else [],
        )
        return self.json(collection)
