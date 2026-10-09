"""Storm Tracker V3 — __init__.py v0.2.0

Alle providers volgen de locatie van de fictieve tracker.
Bij elke verplaatsing worden de providers herinitialiseerd op de nieuwe locatie.
Providers die het nieuwe gebied niet dekken (KMI, KNMI) worden automatisch gestopt.

Versiegeschiedenis:
  v0.2.0 — providers volgen fictieve tracker, niet meer hardcoded op home_lat/lon
  v0.1.0 — eerste versie, providers hardcoded op home_lat/lon
"""
from __future__ import annotations

import logging
import math

from homeassistant.core import HomeAssistant, callback, CoreState
from homeassistant.const import EVENT_HOMEASSISTANT_STARTED
from homeassistant.helpers.typing import ConfigType
import homeassistant.helpers.config_validation as cv
import voluptuous as vol

from .const import DOMAIN
from .providers.blitzortung import BlitzortungProvider
from .plogger.provider_logger import (
    log_lightning, log_kmi, log_rainviewer, log_knmi, log_netatmo, log_open_meteo
)

_LOGGER = logging.getLogger(__name__)

PLATFORMS = ["sensor"]

CONFIG_SCHEMA = vol.Schema({
    DOMAIN: vol.Schema({
        vol.Required("home_lat"): cv.latitude,
        vol.Required("home_lon"): cv.longitude,
        vol.Optional("fictieve_tracker_entity", default="device_tracker.fictieve_tracker"): cv.string,
        vol.Optional("radar_radius_km", default=200): vol.Coerce(float),
        vol.Optional("knmi_api_key"): cv.string,
        vol.Optional("knmi_wms_api_key"): cv.string,
        vol.Optional("netatmo_client_id"): cv.string,
        vol.Optional("netatmo_client_secret"): cv.string,
        vol.Optional("netatmo_refresh_token"): cv.string,
        vol.Optional("netatmo_radius_km", default=175): vol.Coerce(float),
    })
}, extra=vol.ALLOW_EXTRA)


async def async_setup(hass: HomeAssistant, config: ConfigType) -> bool:
    conf = config.get(DOMAIN)
    if conf is None:
        return True

    hass.data.setdefault(DOMAIN, {})

    home_lat         = conf["home_lat"]
    home_lon         = conf["home_lon"]
    fictieve_entity  = conf["fictieve_tracker_entity"]
    knmi_api_key     = conf.get("knmi_api_key")
    knmi_wms_key     = conf.get("knmi_wms_api_key", knmi_api_key)
    netatmo_radius   = conf.get("netatmo_radius_km", 175.0)

    hass.data[DOMAIN]["fictieve_entity"] = fictieve_entity
    hass.data[DOMAIN]["fictieve_lat"]    = home_lat
    hass.data[DOMAIN]["fictieve_lon"]    = home_lon

    # ── Netatmo token (locatie-onafhankelijk) ─────────────────────────────
    client_id     = conf.get("netatmo_client_id")
    client_secret = conf.get("netatmo_client_secret")
    refresh_token = conf.get("netatmo_refresh_token")
    if client_id and client_secret and refresh_token:
        from .providers.netatmo import NetatmoTokenManager
        token_manager = NetatmoTokenManager(client_id, client_secret, refresh_token)
        hass.data[DOMAIN]["netatmo_token"] = token_manager

    # ── Blitzortung (wereldwijd, locatie-onafhankelijk) ───────────────────
    def _on_blitz(obs):
        hass.data[DOMAIN]["last_lightning"] = obs
        hass.data[DOMAIN].setdefault("lightning_count", 0)
        hass.data[DOMAIN]["lightning_count"] += 1
        hass.bus.async_fire(f"{DOMAIN}_lightning_update", {
            "lat": obs.lat, "lon": obs.lon, "timestamp": obs.timestamp
        })
        log_lightning(hass, obs.lat, obs.lon, obs.timestamp)

    blitz = BlitzortungProvider(on_observation=_on_blitz)
    blitz.start()
    hass.data[DOMAIN]["blitz_provider"] = blitz

    # ── Locatie-afhankelijke providers initialiseren ──────────────────────
    async def _init_location_providers(lat: float, lon: float) -> None:
        """Start of herstart alle locatie-afhankelijke providers."""
        from .providers.kmi import KmiProvider, KmiProviderFactory
        from .providers.rainviewer import RainViewerProvider
        from .providers.knmi import KnmiProvider, KnmiProviderFactory
        from .providers.open_meteo import OpenMeteoProvider
        from .providers.netatmo import NetatmoProvider

        _LOGGER.info("Providers initialiseren voor (%.4f,%.4f)", lat, lon)

        # KMI
        if KmiProviderFactory.supports(lat, lon, 700):
            hass.data[DOMAIN]["kmi_provider"] = KmiProvider(lat, lon)
            _LOGGER.info("KMI: gestart")
        else:
            hass.data[DOMAIN]["kmi_provider"] = None
            _LOGGER.info("KMI: buiten dekkingsgebied")

        # RainViewer (altijd)
        hass.data[DOMAIN]["rv_provider"] = RainViewerProvider(lat, lon)
        _LOGGER.info("RainViewer: gestart")

        # KNMI
        if knmi_api_key and KnmiProviderFactory.supports(lat, lon, 700):
            hass.data[DOMAIN]["knmi_provider"] = KnmiProvider(lat, lon, knmi_api_key, knmi_wms_key)
            _LOGGER.info("KNMI: gestart")
        else:
            hass.data[DOMAIN]["knmi_provider"] = None
            _LOGGER.info("KNMI: buiten dekkingsgebied of niet geconfigureerd")

        # Open-Meteo grid
        hass.data[DOMAIN]["open_meteo"] = OpenMeteoProvider(lat, lon)
        _LOGGER.info("Open-Meteo: gestart (%d gridpunten)", len(hass.data[DOMAIN]["open_meteo"]._points))

        # Netatmo
        token = hass.data[DOMAIN].get("netatmo_token")
        if token:
            hass.data[DOMAIN]["netatmo_provider"] = NetatmoProvider(token, lat, lon, netatmo_radius)
            _LOGGER.info("Netatmo: gestart (r=%.0fkm)", netatmo_radius)

        # Direct eerste poll
        hass.async_create_task(_poll_all())

    # ── Poll functies ─────────────────────────────────────────────────────
    async def _poll_kmi(now=None):
        p = hass.data[DOMAIN].get("kmi_provider")
        if not p: return
        obs = await p.fetch_observations()
        hass.data[DOMAIN]["last_kmi_observations"] = obs
        hass.data[DOMAIN]["kmi_count"] = len(obs)
        hass.bus.async_fire(f"{DOMAIN}_radar_update", {"source": "kmi", "count": len(obs)})
        lat = hass.data[DOMAIN].get("fictieve_lat", home_lat)
        lon = hass.data[DOMAIN].get("fictieve_lon", home_lon)
        log_kmi(hass, obs, lat, lon)


    async def _poll_rv(now=None):
        p = hass.data[DOMAIN].get("rv_provider")
        if not p: return
        obs = await p.fetch_observations()
        hass.data[DOMAIN]["last_rv_observations"] = obs
        hass.data[DOMAIN]["rv_count"] = len(obs)
        hass.bus.async_fire(f"{DOMAIN}_radar_update", {"source": "rainviewer", "count": len(obs)})
        lat = hass.data[DOMAIN].get("fictieve_lat", home_lat)
        lon = hass.data[DOMAIN].get("fictieve_lon", home_lon)
        log_rainviewer(hass, obs, lat, lon)

    async def _poll_knmi(now=None):
        import time as _time
        p = hass.data[DOMAIN].get("knmi_provider")
        if not p: return
        obs = await p.fetch_observations()
        current  = [o for o in obs if o.source == "knmi"]
        forecast = [o for o in obs if o.source == "knmi_forecast"]
        hass.data[DOMAIN]["knmi_current"]          = current
        hass.data[DOMAIN]["knmi_forecast"]         = forecast
        hass.data[DOMAIN]["knmi_intensity_now"]    = current[0].intensity if current else 0
        def _intens(m):
            t = _time.time() + m * 60
            c = min(forecast, key=lambda o: abs(o.timestamp - t), default=None)
            return c.intensity if c else 0
        hass.data[DOMAIN]["knmi_intensity_30min"]  = _intens(30)
        hass.data[DOMAIN]["knmi_intensity_60min"]  = _intens(60)
        hass.data[DOMAIN]["knmi_intensity_120min"] = _intens(120)
        # Altijd event vuren, ook als alle waarden 0 zijn
        hass.bus.async_fire(f"{DOMAIN}_knmi_update", {
            "current":  len(current),
            "forecast": len(forecast),
            "intensity_now": hass.data[DOMAIN]["knmi_intensity_now"],
        })
        lat = hass.data[DOMAIN].get("fictieve_lat", home_lat)
        lon = hass.data[DOMAIN].get("fictieve_lon", home_lon)
        log_knmi(hass, current, forecast,
                 hass.data[DOMAIN]["knmi_intensity_now"],
                 hass.data[DOMAIN]["knmi_intensity_30min"],
                 hass.data[DOMAIN]["knmi_intensity_60min"],
                 hass.data[DOMAIN]["knmi_intensity_120min"],
                 lat, lon)

    async def _poll_netatmo(now=None):
        p = hass.data[DOMAIN].get("netatmo_provider")
        if not p: return
        obs = await p.fetch_observations()
        hass.data[DOMAIN]["last_netatmo_observations"] = obs
        raining = [o for o in obs if (o.rain_mm or 0) >= 0.1]
        hass.data[DOMAIN]["netatmo_rain_count"]    = len(raining)
        hass.data[DOMAIN]["netatmo_station_count"] = len(obs)
        hass.bus.async_fire(f"{DOMAIN}_netatmo_update", {"stations": len(obs), "raining": len(raining)})

        lat = hass.data[DOMAIN].get("fictieve_lat", home_lat)
        lon = hass.data[DOMAIN].get("fictieve_lon", home_lon)
        log_netatmo(hass, obs, lat, lon)

    async def _poll_open_meteo(now=None):
        p = hass.data[DOMAIN].get("open_meteo")
        if not p: return
        result = await p.fetch()
        hass.data[DOMAIN]["open_meteo_result"] = result
        hass.bus.async_fire(f"{DOMAIN}_open_meteo_update", result)
        lat = hass.data[DOMAIN].get("fictieve_lat", home_lat)
        lon = hass.data[DOMAIN].get("fictieve_lon", home_lon)
        log_open_meteo(hass, result, lat, lon)
        if result["is_raining"] and not hass.data[DOMAIN].get("open_meteo_was_raining", False):
            _LOGGER.info("Open-Meteo: regen gedetecteerd — %d punten nat nu, %d binnen 90min",
                result.get("wet_now", 0), result.get("wet_forecast_90m", 0))
        hass.data[DOMAIN]["open_meteo_was_raining"] = result["is_raining"]

    async def _poll_all(now=None):
        """Poll radar providers — Open-Meteo heeft eigen interval, niet hier."""
        hass.async_create_task(_poll_kmi())
        hass.async_create_task(_poll_rv())
        hass.async_create_task(_poll_knmi())
        hass.async_create_task(_poll_netatmo())

    # ── Polling intervallen ───────────────────────────────────────────────
    from homeassistant.helpers.event import async_track_time_interval, async_track_state_change_event
    from homeassistant.helpers.event import async_track_state_change_event
    from datetime import timedelta

    async_track_time_interval(hass, _poll_kmi,        timedelta(minutes=5))
    async_track_time_interval(hass, _poll_rv,         timedelta(minutes=5))
    async_track_time_interval(hass, _poll_knmi,       timedelta(minutes=5))
    async_track_time_interval(hass, _poll_netatmo,    timedelta(minutes=5))
    async_track_time_interval(hass, _poll_open_meteo, timedelta(minutes=120))

    # ── Fictieve tracker locatie volgen ───────────────────────────────────
    async def _update_fictieve_location(now=None):
        state = hass.states.get(fictieve_entity)
        if not state:
            _LOGGER.debug("Fictieve tracker: geen state voor %s", fictieve_entity)
            return
        if not state.attributes.get("latitude"):
            _LOGGER.debug("Fictieve tracker: geen latitude (state=%s)", state.state)
            return

        lat = float(state.attributes["latitude"])
        lon = float(state.attributes["longitude"])

        old_lat = hass.data[DOMAIN].get("fictieve_lat", home_lat)
        old_lon = hass.data[DOMAIN].get("fictieve_lon", home_lon)

        dlat = abs(lat - old_lat) * 111.32
        dlon = abs(lon - old_lon) * 111.32 * math.cos(math.radians(lat))
        verplaatsing = math.sqrt(dlat**2 + dlon**2)

        _LOGGER.info(
            "Fictieve tracker: %.4f,%.4f | verplaatsing %.1fkm | initialized=%s",
            lat, lon, verplaatsing, hass.data[DOMAIN].get("providers_initialized")
        )

        if verplaatsing < 1.0 and hass.data[DOMAIN].get("providers_initialized"):
            _LOGGER.debug("Fictieve tracker: <1km verplaatsing, geen herinitialisatie")
            return

        hass.data[DOMAIN]["fictieve_lat"] = lat
        hass.data[DOMAIN]["fictieve_lon"] = lon
        _LOGGER.info("Fictieve tracker: (%.4f,%.4f) — providers herinitialiseren", lat, lon)
        hass.bus.async_fire(f"{DOMAIN}_fictieve_update", {"lat": lat, "lon": lon})

        await _init_location_providers(lat, lon)
        hass.data[DOMAIN]["providers_initialized"] = True

    # State change listener voor fictieve tracker
    @callback
    def _on_fictieve_state_change(event):
        hass.async_create_task(_update_fictieve_location())

    async_track_state_change_event(hass, [fictieve_entity], _on_fictieve_state_change)

    # Initiële locatie ophalen NA HA startup zodat MQTT retained messages verwerkt zijn
    async def _do_initial_setup(event=None):
        await _update_fictieve_location()
        if not hass.data[DOMAIN].get("providers_initialized"):
            await _init_location_providers(home_lat, home_lon)
            hass.data[DOMAIN]["providers_initialized"] = True

    if hass.state == CoreState.running:
        await _do_initial_setup()
    else:
        @callback
        def _on_ha_started(event):
            hass.async_create_task(_do_initial_setup())

        hass.bus.async_listen_once(EVENT_HOMEASSISTANT_STARTED, _on_ha_started)

    # ── Sensor platform laden ─────────────────────────────────────────────
    from homeassistant.helpers import discovery
    await discovery.async_load_platform(hass, "sensor", DOMAIN, {}, config)

    _LOGGER.info("Storm Tracker V3 gestart")
    return True