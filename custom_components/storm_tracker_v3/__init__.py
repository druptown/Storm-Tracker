"""Storm Tracker V3 — __init__.py v0.4.3

Hoofdsetup van de integratie. Verantwoordelijkheden:
  - Providers opstarten en pollen
  - Fictieve tracker locatie volgen
  - ObservationFusionEngine (OFE) en StormEngine activeren
  - Alle provider observaties doorsturen naar de OFE
  - StormEngine resultaten beschikbaar stellen via hass.data

Architectuur (vereenvoudigd, één RegionEngine voor fictieve tracker):

  Providers → OFE → StormEngine → hass.data["storms"]
                                 → HA sensoren

Versiegeschiedenis:
  v0.3.0 — OFE en StormEngine geactiveerd; alle provider observaties
            doorgestuurd naar OFE → StormEngine
  v0.2.0 — providers volgen fictieve tracker, niet meer hardcoded op home_lat/lon
  v0.1.0 — eerste versie, providers hardcoded op home_lat/lon
"""
from __future__ import annotations

import asyncio
import logging
import math

from homeassistant.core import HomeAssistant, callback, CoreState
from homeassistant.const import EVENT_HOMEASSISTANT_STARTED, EVENT_HOMEASSISTANT_STOP
from homeassistant.helpers.typing import ConfigType
import homeassistant.helpers.config_validation as cv
import voluptuous as vol

from .const import DOMAIN
from .providers.blitzortung import BlitzortungProvider
from .engine.observation_fusion_engine import ObservationFusionEngine
from .engine.storm_engine import StormEngine
from .plogger.provider_logger import (
    log_lightning, log_kmi, log_rainviewer, log_knmi, log_netatmo, log_open_meteo
)

_LOGGER = logging.getLogger(__name__)

PLATFORMS = ["sensor"]


def _distance_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle afstand tussen twee WGS84-punten."""
    radius_km = 6371.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)
    a = (
        math.sin(delta_phi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2
    )
    return radius_km * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

CONFIG_SCHEMA = vol.Schema({
    DOMAIN: vol.Schema({
        vol.Required("home_lat"): cv.latitude,
        vol.Required("home_lon"): cv.longitude,
        vol.Optional("fictieve_tracker_entity", default="device_tracker.fictieve_tracker"): cv.string,
        vol.Optional("radar_radius_km", default=300): vol.Coerce(float),
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
    hass.data[DOMAIN].setdefault("unsubscribers", [])

    home_lat        = conf["home_lat"]
    home_lon        = conf["home_lon"]
    fictieve_entity = conf["fictieve_tracker_entity"]
    knmi_api_key    = conf.get("knmi_api_key")
    knmi_wms_key    = conf.get("knmi_wms_api_key", knmi_api_key)
    netatmo_radius  = conf.get("netatmo_radius_km", 175.0)
    radar_radius    = conf.get("radar_radius_km", 200.0)
    from homeassistant.helpers.aiohttp_client import async_get_clientsession
    http_session = async_get_clientsession(hass)

    hass.data[DOMAIN]["fictieve_entity"] = fictieve_entity
    hass.data[DOMAIN]["fictieve_lat"]    = home_lat
    hass.data[DOMAIN]["fictieve_lon"]    = home_lon

    # ── StormEngine + OFE aanmaken ────────────────────────────────────────
    storm_engine = StormEngine()
    hass.data[DOMAIN]["storm_engine"] = storm_engine

    async def _on_ofe_batch(observations: list) -> None:
        """OFE stuurt een batch observaties naar de StormEngine."""
        center_lat = hass.data[DOMAIN].get("fictieve_lat", home_lat)
        center_lon = hass.data[DOMAIN].get("fictieve_lon", home_lon)
        scoped = [
            observation
            for observation in observations
            if _distance_km(
                center_lat,
                center_lon,
                observation.lat,
                observation.lon,
            ) <= radar_radius
        ]
        rejected = len(observations) - len(scoped)
        if rejected:
            _LOGGER.debug(
                "Dekkingsfilter: %d observaties buiten %.0f km genegeerd",
                rejected,
                radar_radius,
            )
        await storm_engine.process_batch(scoped)
        storms = storm_engine.get_storms()
        hass.data[DOMAIN]["storms"] = storms
        hass.bus.async_fire(f"{DOMAIN}_storms_updated", {"count": len(storms)})
        _LOGGER.debug("StormEngine: %d actieve storms na batch van %d observaties",
                      len(storms), len(observations))

    ofe = ObservationFusionEngine(on_batch=_on_ofe_batch)
    hass.data[DOMAIN]["ofe"] = ofe

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
        center_lat = hass.data[DOMAIN].get("fictieve_lat", home_lat)
        center_lon = hass.data[DOMAIN].get("fictieve_lon", home_lon)
        if _distance_km(center_lat, center_lon, obs.lat, obs.lon) > radar_radius:
            return
        hass.data[DOMAIN]["last_lightning"] = obs
        hass.data[DOMAIN].setdefault("lightning_count", 0)
        hass.data[DOMAIN]["lightning_count"] += 1
        hass.bus.async_fire(f"{DOMAIN}_lightning_update", {
            "lat": obs.lat, "lon": obs.lon, "timestamp": obs.timestamp
        })
        log_lightning(hass, obs.lat, obs.lon, obs.timestamp)
        hass.async_create_task(ofe.add_observation(obs))

    blitz = BlitzortungProvider(on_observation=_on_blitz)
    blitz.start()
    hass.data[DOMAIN]["blitz_provider"] = blitz

    # ── Locatie-afhankelijke providers initialiseren ──────────────────────
    async def _init_location_providers(lat: float, lon: float) -> None:
        """Start of herstart alle locatie-afhankelijke providers op nieuwe locatie."""
        from .providers.kmi import KmiProvider, KmiProviderFactory
        from .providers.rainviewer import RainViewerProvider
        from .providers.knmi import KnmiProvider, KnmiProviderFactory
        from .providers.open_meteo import OpenMeteoProvider
        from .providers.netatmo import NetatmoProvider

        _LOGGER.info("Providers initialiseren voor (%.4f,%.4f)", lat, lon)

        if KmiProviderFactory.supports(lat, lon, 700):
            hass.data[DOMAIN]["kmi_provider"] = KmiProvider(lat, lon)
            _LOGGER.info("KMI: gestart")
        else:
            hass.data[DOMAIN]["kmi_provider"] = None
            _LOGGER.info("KMI: buiten dekkingsgebied")

        hass.data[DOMAIN]["rv_provider"] = RainViewerProvider(lat, lon)
        _LOGGER.info("RainViewer: gestart")

        if knmi_api_key and KnmiProviderFactory.supports(lat, lon, 700):
            hass.data[DOMAIN]["knmi_provider"] = KnmiProvider(lat, lon, knmi_api_key, knmi_wms_key)
            _LOGGER.info("KNMI: gestart")
        else:
            hass.data[DOMAIN]["knmi_provider"] = None
            _LOGGER.info("KNMI: buiten dekkingsgebied of niet geconfigureerd")

        hass.data[DOMAIN]["open_meteo"] = OpenMeteoProvider(lat, lon)
        _LOGGER.info("Open-Meteo: gestart (%d gridpunten)",
                     len(hass.data[DOMAIN]["open_meteo"]._points))

        # OPERA (heel Europa, hoge kwaliteit)
        from .providers.opera import OperaProvider, OperaProviderFactory
        if OperaProviderFactory.supports(lat, lon, radar_radius):
            hass.data[DOMAIN]["opera_provider"] = OperaProvider(
                lat, lon, radar_radius, session=http_session
            )
            _LOGGER.info("OPERA: gestart")
        else:
            hass.data[DOMAIN]["opera_provider"] = None
            _LOGGER.info("OPERA: buiten dekkingsgebied")

        token = hass.data[DOMAIN].get("netatmo_token")
        if token:
            hass.data[DOMAIN]["netatmo_provider"] = NetatmoProvider(token, lat, lon, netatmo_radius)
            _LOGGER.info("Netatmo: gestart (r=%.0fkm)", netatmo_radius)

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
        # Comparison-only: KMI must not create duplicate operational radar systems.

    async def _poll_rv(now=None, operational: bool = False):
        p = hass.data[DOMAIN].get("rv_provider")
        if not p: return []
        obs = await p.fetch_observations()
        hass.data[DOMAIN]["last_rv_observations"] = obs
        hass.data[DOMAIN]["rv_count"] = len(obs)
        hass.bus.async_fire(f"{DOMAIN}_radar_update", {"source": "rainviewer", "count": len(obs)})
        lat = hass.data[DOMAIN].get("fictieve_lat", home_lat)
        lon = hass.data[DOMAIN].get("fictieve_lon", home_lon)
        log_rainviewer(hass, obs, lat, lon)
        if operational:
            for o in obs:
                hass.async_create_task(ofe.add_observation(o))
        return obs

    async def _poll_knmi(now=None):
        import time as _time
        p = hass.data[DOMAIN].get("knmi_provider")
        if not p: return
        obs = await p.fetch_observations()
        current  = [o for o in obs if o.source == "knmi"]
        forecast = [o for o in obs if o.source == "knmi_forecast"]
        hass.data[DOMAIN]["knmi_current"]       = current
        hass.data[DOMAIN]["knmi_forecast"]      = forecast
        hass.data[DOMAIN]["knmi_intensity_now"] = current[0].intensity if current else 0
        def _intens(m):
            t = _time.time() + m * 60
            c = min(forecast, key=lambda o: abs(o.timestamp - t), default=None)
            return c.intensity if c else 0
        hass.data[DOMAIN]["knmi_intensity_30min"]  = _intens(30)
        hass.data[DOMAIN]["knmi_intensity_60min"]  = _intens(60)
        hass.data[DOMAIN]["knmi_intensity_120min"] = _intens(120)
        hass.bus.async_fire(f"{DOMAIN}_knmi_update", {
            "current":       len(current),
            "forecast":      len(forecast),
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
        # Comparison-only: KNMI current/forecast remain visible in sensors,
        # but do not influence operational WeatherSystems.

    async def _poll_opera(now=None):
        p = hass.data[DOMAIN].get("opera_provider")
        if not p: return []
        raw_obs = await p.fetch_observations(hass)

        # A low OPERA quality score is not automatically dry: RainViewer or a
        # national radar may still confirm a genuine shower. Conversely,
        # unconfirmed low-quality echoes must not create phantom systems.
        from .providers.radar_policy import (
            OPERA_MIN_STANDALONE_QUALITY,
            verify_opera_observations,
        )
        references = list(hass.data[DOMAIN].get("last_kmi_observations", []))
        references.extend(hass.data[DOMAIN].get("knmi_current", []))
        references.extend(hass.data[DOMAIN].get("last_rv_observations", []))
        verification = verify_opera_observations(raw_obs, references)
        obs = list(verification.accepted)

        diagnostics = p.diagnostics
        accepted_locations = {
            (round(o.lat, 5), round(o.lon, 5)) for o in obs
        }
        for cell in diagnostics.get("cells", []):
            accepted = (
                round(cell["lat"], 5), round(cell["lon"], 5)
            ) in accepted_locations
            cell["accepted"] = accepted
            cell["verification"] = (
                "high_quality"
                if accepted and cell.get("quality", 0) >= OPERA_MIN_STANDALONE_QUALITY
                else "corroborated"
                if accepted
                else "rejected_unconfirmed"
            )
        diagnostics.update({
            "raw_count": len(raw_obs),
            "accepted_count": len(obs),
            "accepted_high_quality": verification.high_quality,
            "accepted_corroborated": verification.corroborated,
            "rejected_unconfirmed": verification.rejected,
            "corroboration_sources": {
                "kmi": len(hass.data[DOMAIN].get("last_kmi_observations", [])),
                "knmi": len(hass.data[DOMAIN].get("knmi_current", [])),
                "rainviewer": len(hass.data[DOMAIN].get("last_rv_observations", [])),
            },
        })
        hass.data[DOMAIN]["opera_count"] = len(obs)
        hass.data[DOMAIN]["opera_diagnostics"] = diagnostics
        hass.bus.async_fire(f"{DOMAIN}_radar_update", {"source": "opera", "count": len(obs)})
        _LOGGER.info(
            "OPERA verificatie: raw=%d accepted=%d (quality=%d confirmed=%d) rejected=%d",
            len(raw_obs), len(obs), verification.high_quality,
            verification.corroborated, verification.rejected,
        )
        for o in obs:
            hass.async_create_task(ofe.add_observation(o))
        return obs

    async def _poll_radar(now=None):
        """Poll exactly one operational radar source: OPERA, else RainViewer."""
        from .providers.radar_policy import select_radar_source

        lock = hass.data[DOMAIN].setdefault("radar_poll_lock", asyncio.Lock())
        if lock.locked():
            _LOGGER.debug("Radarcyclus overgeslagen: vorige cyclus is nog bezig")
            return

        async with lock:
            await _poll_radar_inner(select_radar_source)

    async def _poll_radar_inner(select_radar_source):
        """Inner radar cycle, protected by radar_poll_lock."""

        opera = hass.data[DOMAIN].get("opera_provider")
        rainviewer = hass.data[DOMAIN].get("rv_provider")
        rainviewer_obs = await _poll_rv(operational=False) if rainviewer else []
        if opera:
            await _poll_opera()

        decision = select_radar_source(
            opera_configured=opera is not None,
            opera_healthy=bool(opera and opera.healthy),
            rainviewer_configured=rainviewer is not None,
        )
        hass.data[DOMAIN]["active_radar_source"] = decision.source
        hass.data[DOMAIN]["radar_source_reason"] = decision.reason

        if decision.source == "rainviewer":
            for observation in rainviewer_obs:
                hass.async_create_task(ofe.add_observation(observation))
        elif decision.source is None:
            _LOGGER.warning("Geen operationele radarbron: %s", decision.reason)

        hass.bus.async_fire(f"{DOMAIN}_radar_source_update", {
            "source": decision.source,
            "reason": decision.reason,
        })

    async def _poll_radar_comparison(now=None):
        """Keep national products observable without feeding the OFE."""
        await _poll_kmi()
        await _poll_knmi()

    async def _poll_netatmo(now=None):
        p = hass.data[DOMAIN].get("netatmo_provider")
        if not p: return
        obs = await p.fetch_observations()
        hass.data[DOMAIN]["last_netatmo_observations"] = obs
        raining = [o for o in obs if (o.rain_mm or 0) >= 0.1]
        hass.data[DOMAIN]["netatmo_rain_count"]    = len(raining)
        hass.data[DOMAIN]["netatmo_station_count"] = len(obs)
        hass.bus.async_fire(f"{DOMAIN}_netatmo_update", {
            "stations": len(obs), "raining": len(raining)
        })
        lat = hass.data[DOMAIN].get("fictieve_lat", home_lat)
        lon = hass.data[DOMAIN].get("fictieve_lon", home_lon)
        log_netatmo(hass, obs, lat, lon)
        # Alleen natte stations naar OFE
        for o in raining:
            hass.async_create_task(ofe.add_observation(o))

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
        # Natte punten naar OFE
        from .engine.observation import Observation, ObservationType
        import time as _t
        now_ts = _t.time()
        for loc in result.get("wet_locations_now", []):
            obs = Observation(
                obs_type  = ObservationType.RAIN,
                lat       = loc["lat"],
                lon       = loc["lon"],
                timestamp = now_ts,
                rain_mm   = loc["mm"],
                source    = "open_meteo",
            )
            hass.async_create_task(ofe.add_observation(obs))

    async def _poll_all(now=None):
        """Initial coordinated poll."""
        # Establish national-radar evidence before the first OPERA validation.
        # Later five-minute cycles can safely reuse the previous comparison.
        await _poll_radar_comparison()
        await _poll_radar()
        hass.async_create_task(_poll_netatmo())

    # ── Polling intervallen ───────────────────────────────────────────────
    from homeassistant.helpers.event import async_track_time_interval, async_track_state_change_event
    from datetime import timedelta

    hass.data[DOMAIN]["unsubscribers"].extend([
        async_track_time_interval(hass, _poll_radar, timedelta(minutes=5)),
        async_track_time_interval(hass, _poll_radar_comparison, timedelta(minutes=5)),
        async_track_time_interval(hass, _poll_netatmo, timedelta(minutes=5)),
        async_track_time_interval(hass, _poll_open_meteo, timedelta(minutes=10)),
    ])

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
            return

        hass.data[DOMAIN]["fictieve_lat"] = lat
        hass.data[DOMAIN]["fictieve_lon"] = lon
        await ofe.reset()
        removed = storm_engine.retain_within(lat, lon, radar_radius)
        hass.data[DOMAIN]["storms"] = storm_engine.get_storms()
        hass.data[DOMAIN]["lightning_count"] = 0
        hass.data[DOMAIN].pop("last_lightning", None)
        hass.bus.async_fire(
            f"{DOMAIN}_storms_updated",
            {"count": len(hass.data[DOMAIN]["storms"]), "removed": removed},
        )
        _LOGGER.info(
            "Regiowissel: %d WeatherSystems buiten %.0f km verwijderd",
            removed,
            radar_radius,
        )
        _LOGGER.info("Fictieve tracker: (%.4f,%.4f) — providers herinitialiseren", lat, lon)
        hass.bus.async_fire(f"{DOMAIN}_fictieve_update", {"lat": lat, "lon": lon})
        await _init_location_providers(lat, lon)
        hass.data[DOMAIN]["providers_initialized"] = True

    @callback
    def _on_fictieve_state_change(event):
        hass.async_create_task(_update_fictieve_location())

    hass.data[DOMAIN]["unsubscribers"].append(
        async_track_state_change_event(hass, [fictieve_entity], _on_fictieve_state_change)
    )

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

    @callback
    def _on_ha_stop(event):
        blitz.stop()
        for unsubscribe in hass.data[DOMAIN].get("unsubscribers", []):
            unsubscribe()
        hass.data[DOMAIN]["unsubscribers"] = []

    hass.bus.async_listen_once(EVENT_HOMEASSISTANT_STOP, _on_ha_stop)

    # ── Sensor platform laden ─────────────────────────────────────────────
    from homeassistant.helpers import discovery
    await discovery.async_load_platform(hass, "sensor", DOMAIN, {}, config)

    _LOGGER.info("Storm Tracker V3 v0.4.10 gestart")
    return True
