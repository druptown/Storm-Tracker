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
            active_radar_source=data.get("active_radar_source"),
            radar_sources_by_engine=data.get("radar_sources_by_engine"),
            lightning_events=data.get("recent_lightning"),
        )
        collection["radar_overlays"] = data.get("radar_overlays_by_engine", {})
        return self.json(collection)
