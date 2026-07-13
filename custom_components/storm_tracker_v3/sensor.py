"""Storm Tracker V3 — sensor.py v0.2.0

Sensoren die de ruwe provider-data tonen.
Nog geen fusion, geen projectie — puur data zichtbaar maken.

Versiegeschiedenis:
  v0.2.0 — storm sensoren toegevoegd (StormTellerSensor, StormDetailSensor)
  v0.1.0 — eerste versie; provider sensoren

Sensoren:
  sensor.stv3_blitzortung_inslagen   — aantal blikseminslagen (60min buffer)
  sensor.stv3_blitzortung_laatste    — lat/lon/timestamp van laatste inslag
  sensor.stv3_kmi_observaties        — aantal KMI-radarobservaties
  sensor.stv3_kmi_intensiteit        — gemiddelde intensiteit KMI
  sensor.stv3_rainviewer_observaties — aantal RainViewer-observaties
  sensor.stv3_netatmo_stations       — totaal Netatmo-stations in bereik
  sensor.stv3_netatmo_regen          — stations met meetbare neerslag
"""
from __future__ import annotations

import logging
from datetime import datetime

from homeassistant.components.sensor import SensorEntity, SensorStateClass
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN

_LOGGER = logging.getLogger(__name__)


async def async_setup_platform(
    hass: HomeAssistant, config, async_add_entities: AddEntitiesCallback, discovery_info=None
):
    """Setup sensoren."""
    entities = [
        BlitzortungInslagenSensor(hass),
        OperaObservatieSensor(hass),
        ActiveRadarSourceSensor(hass),
        FictieveTrackerSensor(hass),
        BlitzortungLaatsteInslag(hass),
        KmiObservatieSensor(hass),
        KmiIntensiteitSensor(hass),
        RainViewerObservatieSensor(hass),
    ]

    # KNMI en Netatmo altijd toevoegen — providers worden later geïnitialiseerd
    entities.append(KnmiIntensiteitSensorNu(hass))
    entities.append(KnmiNowcastSensor(hass))
    entities.append(NetatmoStationsSensor(hass))
    entities.append(NetatmoRegenSensor(hass))

    # Storm sensoren
    entities.append(StormTellerSensor(hass))
    entities.append(StormDetailSensor(hass))
    entities.append(McsDetectieSensor(hass))
    entities.append(RegionEngineSensor(hass))

    async_add_entities(entities, update_before_add=True)


class StormTrackerBaseSensor(SensorEntity):
    """Basis voor alle Storm Tracker V3 sensoren."""

    def __init__(self, hass: HomeAssistant) -> None:
        self.hass = hass
        self._attr_should_poll = False
        self._unsubs = []

    async def async_added_to_hass(self) -> None:
        """Luister op HA-events van de providers."""
        @callback
        def _handle_update(event):
            self.async_write_ha_state()

        for event_name in self._listen_events:
            self._unsubs.append(self.hass.bus.async_listen(event_name, _handle_update))

    async def async_will_remove_from_hass(self) -> None:
        for unsub in self._unsubs:
            unsub()
        self._unsubs.clear()

    @property
    def _listen_events(self) -> list[str]:
        return []

    @property
    def device_info(self):
        return {
            "identifiers": {(DOMAIN, "storm_tracker_v3")},
            "name": "Storm Tracker V3",
            "manufacturer": "Custom",
            "model": "V3",
        }


class BlitzortungInslagenSensor(StormTrackerBaseSensor):
    _attr_name = "STV3 Blitzortung Inslagen"
    _attr_unique_id = "stv3_blitzortung_inslagen"
    _attr_icon = "mdi:lightning-bolt"
    _attr_native_unit_of_measurement = "inslagen"
    _attr_state_class = SensorStateClass.TOTAL

    @property
    def _listen_events(self):
        return [f"{DOMAIN}_lightning_update"]

    @property
    def native_value(self):
        return self.hass.data.get(DOMAIN, {}).get("lightning_count", 0)

    @property
    def extra_state_attributes(self):
        last = self.hass.data.get(DOMAIN, {}).get("last_lightning")
        if not last:
            return {}
        return {
            "laatste_lat": last.lat,
            "laatste_lon": last.lon,
            "laatste_ts": datetime.fromtimestamp(last.timestamp).isoformat(),
        }


class BlitzortungLaatsteInslag(StormTrackerBaseSensor):
    _attr_name = "STV3 Blitzortung Laatste Inslag"
    _attr_unique_id = "stv3_blitzortung_laatste"
    _attr_icon = "mdi:map-marker-radius"

    @property
    def _listen_events(self):
        return [f"{DOMAIN}_lightning_update"]

    @property
    def native_value(self):
        last = self.hass.data.get(DOMAIN, {}).get("last_lightning")
        if not last:
            return "Geen data"
        return f"{last.lat:.4f},{last.lon:.4f}"

    @property
    def extra_state_attributes(self):
        last = self.hass.data.get(DOMAIN, {}).get("last_lightning")
        if not last:
            return {}
        return {
            "lat": last.lat,
            "lon": last.lon,
            "timestamp": last.timestamp,
            "source": last.source,
        }


class OperaObservatieSensor(StormTrackerBaseSensor):
    """Toont het aantal OPERA radarcellen in het dekkingsgebied."""
    _attr_name      = "STV3 OPERA Observaties"
    _attr_unique_id = "stv3_opera_observaties"
    _attr_icon      = "mdi:radar"
    _attr_native_unit_of_measurement = "cellen"

    @property
    def _listen_events(self): return [f"{DOMAIN}_radar_update"]

    async def async_added_to_hass(self) -> None:
        await super().async_added_to_hass()
        self.async_write_ha_state()

    @property
    def available(self) -> bool:
        return True

    @property
    def native_value(self):
        return self.hass.data.get(DOMAIN, {}).get("opera_count", 0)

    @property
    def extra_state_attributes(self):
        return self.hass.data.get(DOMAIN, {}).get("opera_diagnostics", {})


class ActiveRadarSourceSensor(StormTrackerBaseSensor):
    """Operationele radarbron geselecteerd door primary/fallback-beleid."""
    _attr_name = "STV3 Actieve Radarbron"
    _attr_unique_id = "stv3_active_radar_source"
    _attr_icon = "mdi:radar"

    @property
    def _listen_events(self):
        return [f"{DOMAIN}_radar_source_update", f"{DOMAIN}_radar_update"]

    async def async_added_to_hass(self) -> None:
        await super().async_added_to_hass()
        self.async_write_ha_state()

    @property
    def available(self) -> bool:
        return True

    @property
    def native_value(self):
        return self.hass.data.get(DOMAIN, {}).get("active_radar_source") or "geen"

    @property
    def extra_state_attributes(self):
        data = self.hass.data.get(DOMAIN, {})
        return {"reason": data.get("radar_source_reason", "nog niet geselecteerd")}


class KmiObservatieSensor(StormTrackerBaseSensor):
    _attr_name = "STV3 KMI Observaties"
    _attr_unique_id = "stv3_kmi_observaties"
    _attr_icon = "mdi:radar"
    _attr_native_unit_of_measurement = "pixels"

    @property
    def _listen_events(self):
        return [f"{DOMAIN}_radar_update"]

    @property
    def native_value(self):
        return self.hass.data.get(DOMAIN, {}).get("kmi_count", 0)

    @property
    def extra_state_attributes(self):
        obs_list = self.hass.data.get(DOMAIN, {}).get("last_kmi_observations", [])
        if not obs_list:
            return {"status": "geen data"}
        intens = [o.intensity for o in obs_list if o.intensity]
        return {
            "aantal": len(obs_list),
            "gem_intensiteit": round(sum(intens) / len(intens), 1) if intens else 0,
            "max_intensiteit": max(intens) if intens else 0,
            "eerste_lat": obs_list[0].lat,
            "eerste_lon": obs_list[0].lon,
        }


class KmiIntensiteitSensor(StormTrackerBaseSensor):
    _attr_name = "STV3 KMI Intensiteit"
    _attr_unique_id = "stv3_kmi_intensiteit"
    _attr_icon = "mdi:water"
    _attr_native_unit_of_measurement = "level"

    @property
    def _listen_events(self):
        return [f"{DOMAIN}_radar_update"]

    @property
    def native_value(self):
        obs_list = self.hass.data.get(DOMAIN, {}).get("last_kmi_observations", [])
        if not obs_list:
            return 0
        intens = [o.intensity for o in obs_list if o.intensity]
        return max(intens) if intens else 0

    @property
    def extra_state_attributes(self):
        p = self.hass.data.get(DOMAIN, {}).get("kmi_provider")
        if not p:
            return {}
        from .providers.kmi import _ww_to_text
        return {
            "weercode":    getattr(p, "_last_ww", 0),
            "weertype":    _ww_to_text(getattr(p, "_last_ww", 0)),
            "temperatuur": getattr(p, "_last_temp", None),
            "ww_ts":       getattr(p, "_last_ww_ts", ""),
        }


class RainViewerObservatieSensor(StormTrackerBaseSensor):
    _attr_name = "STV3 RainViewer Observaties"
    _attr_unique_id = "stv3_rainviewer_observaties"
    _attr_icon = "mdi:radar"
    _attr_native_unit_of_measurement = "pixels"

    @property
    def _listen_events(self):
        return [f"{DOMAIN}_radar_update"]

    @property
    def native_value(self):
        return self.hass.data.get(DOMAIN, {}).get("rv_count", 0)

    @property
    def extra_state_attributes(self):
        obs_list = self.hass.data.get(DOMAIN, {}).get("last_rv_observations", [])
        if not obs_list:
            return {"status": "geen data"}
        intens = [o.intensity for o in obs_list if o.intensity]
        return {
            "aantal": len(obs_list),
            "gem_intensiteit": round(sum(intens) / len(intens), 1) if intens else 0,
            "max_intensiteit": max(intens) if intens else 0,
        }


class OpenMeteoGearSensor(StormTrackerBaseSensor):
    _attr_name      = "STV3 Open-Meteo Gear"
    _attr_unique_id = "stv3_open_meteo_gear"
    _attr_icon      = "mdi:speedometer"

    @property
    def _listen_events(self): return [f"{DOMAIN}_open_meteo_update"]

    @property
    def native_value(self):
        result = self.hass.data.get(DOMAIN, {}).get("open_meteo_result", {})
        return result.get("gear", "LOW")

    @property
    def extra_state_attributes(self):
        result = self.hass.data.get(DOMAIN, {}).get("open_meteo_result", {})
        return {
            "is_raining":        result.get("is_raining", False),
            "max_precipitation": result.get("max_precipitation", 0),
            "wet_points":        result.get("wet_points", 0),
            "wet_now":           result.get("wet_now", 0),
            "wet_forecast_90m":  result.get("wet_forecast_90m", 0),
            "total_points":      result.get("total_points", 0),
        }


class FictieveTrackerSensor(StormTrackerBaseSensor):
    _attr_name      = "STV3 Fictieve Tracker Locatie"
    _attr_unique_id = "stv3_fictieve_tracker"
    _attr_icon      = "mdi:map-marker-account"

    @property
    def _listen_events(self): return [f"{DOMAIN}_fictieve_update"]

    @property
    def native_value(self):
        lat = self.hass.data.get(DOMAIN, {}).get("fictieve_lat")
        lon = self.hass.data.get(DOMAIN, {}).get("fictieve_lon")
        if lat is None: return "Onbekend"
        return f"{lat:.4f},{lon:.4f}"

    @property
    def extra_state_attributes(self):
        return {
            "latitude":  self.hass.data.get(DOMAIN, {}).get("fictieve_lat"),
            "longitude": self.hass.data.get(DOMAIN, {}).get("fictieve_lon"),
        }


class KnmiIntensiteitSensorNu(StormTrackerBaseSensor):
    _attr_name       = "STV3 KNMI Intensiteit Nu"
    _attr_unique_id  = "stv3_knmi_intensiteit_nu"
    _attr_icon       = "mdi:radar"
    _attr_native_unit_of_measurement = "level"

    @property
    def _listen_events(self): return [f"{DOMAIN}_knmi_update"]

    async def async_added_to_hass(self) -> None:
        await super().async_added_to_hass()
        # Direct initiële waarde wegschrijven zodat sensor nooit unavailable is
        self.async_write_ha_state()

    @property
    def available(self) -> bool:
        return True

    @property
    def native_value(self):
        return self.hass.data.get(DOMAIN, {}).get("knmi_intensity_now", 0)

    @property
    def extra_state_attributes(self):
        return {
            "30min":  self.hass.data.get(DOMAIN, {}).get("knmi_intensity_30min", 0),
            "60min":  self.hass.data.get(DOMAIN, {}).get("knmi_intensity_60min", 0),
            "120min": self.hass.data.get(DOMAIN, {}).get("knmi_intensity_120min", 0),
            "nowcast_stappen": len(self.hass.data.get(DOMAIN, {}).get("knmi_forecast", [])),
        }


class KnmiNowcastSensor(StormTrackerBaseSensor):
    _attr_name       = "STV3 KNMI Nowcast"
    _attr_unique_id  = "stv3_knmi_nowcast"
    _attr_icon       = "mdi:clock-fast"
    _attr_native_unit_of_measurement = "level"

    @property
    def _listen_events(self): return [f"{DOMAIN}_knmi_update"]
    @property
    def native_value(self):
        return self.hass.data.get(DOMAIN, {}).get("knmi_intensity_30min", 0)

    @property
    def extra_state_attributes(self):
        import time
        forecast = self.hass.data.get(DOMAIN, {}).get("knmi_forecast", [])
        now      = time.time()
        attrs    = {}
        for o in sorted(forecast, key=lambda x: x.timestamp):
            minuten = round((o.timestamp - now) / 60)
            if 0 <= minuten <= 120:
                attrs[f"+{minuten}min"] = o.intensity
        attrs["intensiteit_30min"]  = self.hass.data.get(DOMAIN, {}).get("knmi_intensity_30min", 0)
        attrs["intensiteit_60min"]  = self.hass.data.get(DOMAIN, {}).get("knmi_intensity_60min", 0)
        attrs["intensiteit_120min"] = self.hass.data.get(DOMAIN, {}).get("knmi_intensity_120min", 0)
        return attrs


class NetatmoStationsSensor(StormTrackerBaseSensor):
    _attr_name = "STV3 Netatmo Stations"
    _attr_unique_id = "stv3_netatmo_stations"
    _attr_icon = "mdi:weather-pouring"
    _attr_native_unit_of_measurement = "stations"

    @property
    def _listen_events(self):
        return [f"{DOMAIN}_netatmo_update"]

    async def async_added_to_hass(self) -> None:
        await super().async_added_to_hass()
        self.async_write_ha_state()

    @property
    def available(self) -> bool:
        return True

    @property
    def native_value(self):
        return self.hass.data.get(DOMAIN, {}).get("netatmo_station_count", 0)

    @property
    def extra_state_attributes(self):
        import math
        obs_list  = self.hass.data.get(DOMAIN, {}).get("last_netatmo_observations", [])
        if not obs_list:
            return {}
        regen     = [o for o in obs_list if (o.rain_mm or 0) >= 0.1]
        rain_vals = sorted([o.rain_mm for o in regen if o.rain_mm], reverse=True)
        wind_obs  = [o for o in obs_list if getattr(o, "wind_speed", None) is not None]
        press_obs = [o for o in obs_list if getattr(o, "pressure", None) is not None]
        gusts     = [o.gust_speed for o in obs_list if getattr(o, "gust_speed", None) is not None]
        pressures = [o.pressure for o in press_obs]

        attrs = {
            "totaal_stations":   len(obs_list),
            "met_regen":         len(regen),
            "max_regen_mm":      rain_vals[0] if rain_vals else 0,
            "gem_regen_mm":      round(sum(rain_vals) / len(rain_vals), 2) if rain_vals else 0,
            "wind_stations":     len(wind_obs),
            "max_windstoot_kmh": round(max(gusts), 1) if gusts else None,
            "druk_stations":     len(press_obs),
            "gem_druk_mbar":     round(sum(pressures) / len(pressures), 1) if pressures else None,
            "min_druk_mbar":     round(min(pressures), 1) if pressures else None,
        }
        if wind_obs:
            speeds = [o.wind_speed for o in wind_obs]
            angles = [o.wind_angle for o in wind_obs if o.wind_angle is not None]
            attrs["gem_wind_kmh"] = round(sum(speeds) / len(speeds), 1)
            if angles:
                sin_sum = sum(math.sin(math.radians(a)) for a in angles)
                cos_sum = sum(math.cos(math.radians(a)) for a in angles)
                attrs["gem_windrichting"] = round(math.degrees(math.atan2(sin_sum, cos_sum)) % 360, 0)
        return attrs


class NetatmoRegenSensor(StormTrackerBaseSensor):
    _attr_name = "STV3 Netatmo Regen Stations"
    _attr_unique_id = "stv3_netatmo_regen"
    _attr_icon = "mdi:water-check"
    _attr_native_unit_of_measurement = "stations"

    @property
    def _listen_events(self):
        return [f"{DOMAIN}_netatmo_update"]

    @property
    def native_value(self):
        return self.hass.data.get(DOMAIN, {}).get("netatmo_rain_count", 0)

    async def async_added_to_hass(self) -> None:
        await super().async_added_to_hass()
        self.async_write_ha_state()

    @property
    def available(self) -> bool:
        return True


class StormTellerSensor(StormTrackerBaseSensor):
    """Toont het aantal actieve storms in de StormEngine."""
    _attr_name      = "STV3 Actieve Storms"
    _attr_unique_id = "stv3_storm_teller"
    _attr_icon      = "mdi:weather-lightning-rainy"
    _attr_native_unit_of_measurement = "storms"

    @property
    def _listen_events(self): return [f"{DOMAIN}_storms_updated"]

    async def async_added_to_hass(self) -> None:
        await super().async_added_to_hass()
        self.async_write_ha_state()

    @property
    def available(self) -> bool:
        return True

    @property
    def native_value(self):
        storms = self.hass.data.get(DOMAIN, {}).get("storms", [])
        return len(storms)

    @property
    def extra_state_attributes(self):
        storms = self.hass.data.get(DOMAIN, {}).get("storms", [])
        return {
            "storms": [
                {
                    "id":          s.storm_id,
                    "lat":         round(s.centroid_lat, 4),
                    "lon":         round(s.centroid_lon, 4),
                    "richting":    round(s.heading_deg, 0) if s.heading_deg is not None else None,
                    "snelheid":    round(s.speed_kmh, 1) if s.speed_kmh is not None else None,
                    "inslagen":    s.strike_count,
                    "vertrouwen":  s.confidence,
                    "plaatsnaam":  getattr(s, "place_name", None),
                    "radarcellen": len(getattr(s, "radar_cells", {})),
                    "bron_systemen": len(getattr(s, "source_system_ids", set())),
                    "type": getattr(s, "system_type", "unknown"),
                    "mcs_status": getattr(s, "mcs_status", "not_evaluated"),
                    "mcs_duur_min": getattr(s, "mcs_duration_minutes", 0.0),
                    "convectieve_span_km": getattr(s, "mcs_convective_span_km", 0.0),
                }
                for s in storms
            ]
        }


class StormDetailSensor(StormTrackerBaseSensor):
    """Toont details van de dichtstbijzijnde actieve storm."""
    _attr_name      = "STV3 Dichtstbijzijnde Storm"
    _attr_unique_id = "stv3_storm_dichtstbij"
    _attr_icon      = "mdi:map-marker-alert"

    @property
    def _listen_events(self): return [f"{DOMAIN}_storms_updated"]

    async def async_added_to_hass(self) -> None:
        await super().async_added_to_hass()
        self.async_write_ha_state()

    @property
    def available(self) -> bool:
        return True

    @property
    def native_value(self):
        """Afstand in km tot de dichtstbijzijnde storm."""
        storms = self.hass.data.get(DOMAIN, {}).get("storms", [])
        if not storms:
            return None
        lat = self.hass.data.get(DOMAIN, {}).get("fictieve_lat", 0)
        lon = self.hass.data.get(DOMAIN, {}).get("fictieve_lon", 0)
        import math
        def haversine(la1, lo1, la2, lo2):
            R = 6371.0
            dlat = math.radians(la2 - la1)
            dlon = math.radians(lo2 - lo1)
            a = math.sin(dlat/2)**2 + math.cos(math.radians(la1)) * math.cos(math.radians(la2)) * math.sin(dlon/2)**2
            return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
        def closest_point(storm):
            radar = storm.closest_radar_point(lat, lon)
            return radar or (
                haversine(lat, lon, storm.centroid_lat, storm.centroid_lon),
                storm.centroid_lat,
                storm.centroid_lon,
            )
        closest_data = min(
            ((closest_point(storm), storm) for storm in storms),
            key=lambda item: item[0][0],
        )
        return round(closest_data[0][0], 1)

    @property
    def extra_state_attributes(self):
        storms = self.hass.data.get(DOMAIN, {}).get("storms", [])
        if not storms:
            return {}
        lat = self.hass.data.get(DOMAIN, {}).get("fictieve_lat", 0)
        lon = self.hass.data.get(DOMAIN, {}).get("fictieve_lon", 0)
        import math
        def haversine(la1, lo1, la2, lo2):
            R = 6371.0
            dlat = math.radians(la2 - la1)
            dlon = math.radians(lo2 - lo1)
            a = math.sin(dlat/2)**2 + math.cos(math.radians(la1)) * math.cos(math.radians(la2)) * math.sin(dlon/2)**2
            return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
        def closest_point(storm):
            radar = storm.closest_radar_point(lat, lon)
            return radar or (
                haversine(lat, lon, storm.centroid_lat, storm.centroid_lon),
                storm.centroid_lat,
                storm.centroid_lon,
            )
        point, closest = min(
            ((closest_point(storm), storm) for storm in storms),
            key=lambda item: item[0][0],
        )
        afstand, impact_lat, impact_lon = point

        # ETA berekenen
        eta_min = None
        if closest.speed_kmh and closest.speed_kmh > 0:
            eta_min = round(afstand / closest.speed_kmh * 60, 0)

        return {
            "storm_id":    closest.storm_id,
            "lat":         round(impact_lat, 4),
            "lon":         round(impact_lon, 4),
            "system_lat":  round(closest.centroid_lat, 4),
            "system_lon":  round(closest.centroid_lon, 4),
            "afstand_km":  round(afstand, 1),
            "richting":    round(closest.heading_deg, 0) if closest.heading_deg is not None else None,
            "snelheid_kmh": round(closest.speed_kmh, 1) if closest.speed_kmh is not None else None,
            "eta_minuten": eta_min,
            "inslagen":    closest.strike_count,
            "vertrouwen":  closest.confidence,
            "plaatsnaam":  getattr(closest, "place_name", None),
            "radius_km":   round(closest.radius_km, 1),
            "radarcellen": len(getattr(closest, "radar_cells", {})),
            "bron_systemen": len(getattr(closest, "source_system_ids", set())),
            "type": getattr(closest, "system_type", "unknown"),
            "mcs_status": getattr(closest, "mcs_status", "not_evaluated"),
            "mcs_duur_min": getattr(closest, "mcs_duration_minutes", 0.0),
            "convectieve_span_km": getattr(closest, "mcs_convective_span_km", 0.0),
            "neerslag_span_km": getattr(closest, "mcs_precipitation_span_km", 0.0),
            "convectieve_cellen": getattr(closest, "mcs_convective_cells", 0),
            "intense_cellen": getattr(closest, "mcs_intense_cells", 0),
            "parent_oppervlakte_km2": getattr(closest, "mcs_parent_area_km2", 0.0),
        }


class McsDetectieSensor(StormTrackerBaseSensor):
    """Aantal bevestigde MCS'en, met kandidaten afzonderlijk zichtbaar."""
    _attr_name = "STV3 MCS Detectie"
    _attr_unique_id = "stv3_mcs_detectie"
    _attr_icon = "mdi:weather-hurricane"
    _attr_native_unit_of_measurement = "systemen"

    @property
    def _listen_events(self):
        return [f"{DOMAIN}_storms_updated"]

    @property
    def native_value(self):
        storms = self.hass.data.get(DOMAIN, {}).get("storms", [])
        return sum(
            1 for storm in storms
            if getattr(storm, "mcs_status", None) == "confirmed"
        )

    @property
    def extra_state_attributes(self):
        storms = self.hass.data.get(DOMAIN, {}).get("storms", [])
        relevant = [
            storm for storm in storms
            if getattr(storm, "mcs_status", None) in {"candidate", "confirmed"}
        ]
        return {
            "kandidaten": sum(
                1 for storm in relevant if storm.mcs_status == "candidate"
            ),
            "bevestigd": sum(
                1 for storm in relevant if storm.mcs_status == "confirmed"
            ),
            "systemen": [
                {
                    "id": storm.storm_id,
                    "status": storm.mcs_status,
                    "duur_min": storm.mcs_duration_minutes,
                    "convectieve_span_km": storm.mcs_convective_span_km,
                    "neerslag_span_km": storm.mcs_precipitation_span_km,
                    "convectieve_cellen": storm.mcs_convective_cells,
                    "intense_cellen": storm.mcs_intense_cells,
                    "oppervlakte_km2": storm.mcs_parent_area_km2,
                }
                for storm in relevant
            ],
            "criteria": {
                "min_convectieve_span_km": 100,
                "min_convectieve_cellen_40dbz": 2,
                "min_intense_cellen_50dbz": 1,
                "min_duur_min": 180,
            },
        }


class RegionEngineSensor(StormTrackerBaseSensor):
    """Diagnostiek van de werkelijk actieve dynamische runtime-regio's."""
    _attr_name = "STV3 Region Engines"
    _attr_unique_id = "stv3_region_engines"
    _attr_icon = "mdi:radar"
    _attr_native_unit_of_measurement = "engines"

    @property
    def _listen_events(self):
        return [f"{DOMAIN}_storms_updated", f"{DOMAIN}_fictieve_update"]

    @property
    def native_value(self):
        manager = self.hass.data.get(DOMAIN, {}).get("storm_manager")
        return len(manager.get_all_engines()) if manager else 0

    @property
    def extra_state_attributes(self):
        manager = self.hass.data.get(DOMAIN, {}).get("storm_manager")
        if not manager:
            return {"engines": []}
        return {
            "sharing_distance_km": manager.sharing_distance_km,
            "engines": [
                {
                    "id": engine.engine_id,
                    "centrum": [
                        round(engine.center_lat, 4),
                        round(engine.center_lon, 4),
                    ],
                    "observatieradius_km": engine.observation_radius_km,
                    "targets": sorted(engine.projection_targets),
                    "weather_systemen": len(engine.storm_engine.get_storms()),
                }
                for engine in manager.get_all_engines()
            ],
        }
