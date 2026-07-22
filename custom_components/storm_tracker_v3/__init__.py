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
import time
from datetime import datetime, timezone
from pathlib import Path

from homeassistant.core import HomeAssistant, callback, CoreState
from homeassistant.const import EVENT_HOMEASSISTANT_STARTED, EVENT_HOMEASSISTANT_STOP
from homeassistant.helpers.typing import ConfigType
from homeassistant.helpers.storage import Store
import homeassistant.helpers.config_validation as cv
import voluptuous as vol

from .const import DOMAIN
from .providers.blitzortung import BlitzortungProvider
from .engine.region_manager import StormManager
from .engine.mcs_store import McsHistoryStore
from .engine.pressure_trend import PressureTrendTracker
from .engine.targets import build_target_specs, coordinates_from_state
from .plogger.provider_logger import (
    log_lightning, log_kmi, log_rainviewer, log_knmi, log_netatmo, log_open_meteo
)
from .http import StormTrackerGeoJsonView
from .geometry.location_resolver import load_places_json, resolve_location

_LOGGER = logging.getLogger(__name__)

PLATFORMS = ["sensor"]


def _use_satellite_lightning(blitz_connected: bool, source_mode: str) -> bool:
    """Bepaal of satellietbliksem actief moet pollen."""
    return source_mode == "satellite_test" or not blitz_connected


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

TARGET_SCHEMA = vol.Schema({
    vol.Required("id"): cv.string,
    vol.Optional("name"): cv.string,
    vol.Required("location_entity"): cv.entity_id,
    vol.Optional("latitude"): cv.latitude,
    vol.Optional("longitude"): cv.longitude,
})


CONFIG_SCHEMA = vol.Schema({
    DOMAIN: vol.Schema({
        vol.Required("home_lat"): cv.latitude,
        vol.Required("home_lon"): cv.longitude,
        vol.Optional("fictieve_tracker_entity"): cv.entity_id,
        vol.Optional("targets", default=[]): [TARGET_SCHEMA],
        vol.Optional("radar_radius_km", default=300): vol.Coerce(float),
        vol.Optional("engine_sharing_distance_km", default=150): vol.Coerce(float),
        vol.Optional("knmi_api_key"): cv.string,
        vol.Optional("knmi_wms_api_key"): cv.string,
        vol.Optional("netatmo_client_id"): cv.string,
        vol.Optional("netatmo_client_secret"): cv.string,
        vol.Optional("netatmo_refresh_token"): cv.string,
        vol.Optional("netatmo_radius_km", default=175): vol.Coerce(float),
        vol.Optional("eumetsat_consumer_key"): cv.string,
        vol.Optional("eumetsat_consumer_secret"): cv.string,
        vol.Optional("meteofrance_api_token"): cv.string,
        vol.Optional("meteofrance_application_id"): cv.string,
        vol.Optional("hsaf_username"): cv.string,
        vol.Optional("hsaf_password"): cv.string,
        vol.Optional("lightning_source_mode", default="auto"): vol.In({"auto", "satellite_test"}),
    })
}, extra=vol.ALLOW_EXTRA)


async def async_setup(hass: HomeAssistant, config: ConfigType) -> bool:
    """Behoud YAML-installaties; nieuwe installaties gebruiken een config entry."""
    conf = config.get(DOMAIN)
    if conf is None:
        return True
    return await _async_setup_runtime(hass, conf, config)


async def async_setup_entry(hass: HomeAssistant, entry) -> bool:
    """Start Storm Tracker vanuit de configuratie-UI."""
    raw = {**entry.data, **entry.options}
    targets = []
    for entity_id in raw.get("persons", []):
        state = hass.states.get(entity_id)
        name = state.attributes.get("friendly_name") if state is not None else None
        targets.append({
            "id": entity_id.split(".", 1)[-1],
            "name": name or entity_id.split(".", 1)[-1].replace("_", " ").title(),
            "location_entity": entity_id,
        })
    conf = {
        "home_lat": hass.config.latitude,
        "home_lon": hass.config.longitude,
        "targets": targets,
        "radar_radius_km": raw.get("radar_radius_km", 300.0),
        "engine_sharing_distance_km": raw.get("engine_sharing_distance_km", 150.0),
        "eumetsat_consumer_key": raw.get("eumetsat_consumer_key"),
        "eumetsat_consumer_secret": raw.get("eumetsat_consumer_secret"),
        "meteofrance_api_token": raw.get("meteofrance_api_token"),
        "meteofrance_application_id": raw.get("meteofrance_application_id"),
        "knmi_api_key": raw.get("knmi_api_key"),
        "knmi_wms_api_key": raw.get("knmi_wms_api_key"),
        "hsaf_username": raw.get("hsaf_username"),
        "hsaf_password": raw.get("hsaf_password"),
        "netatmo_client_id": raw.get("netatmo_client_id"),
        "netatmo_client_secret": raw.get("netatmo_client_secret"),
        "netatmo_refresh_token": raw.get("netatmo_refresh_token"),
        "netatmo_radius_km": raw.get("netatmo_radius_km", 175.0),
        "lightning_source_mode": raw.get("lightning_source_mode", "auto"),
    }
    if raw.get("test_tracker_entity"):
        conf["fictieve_tracker_entity"] = raw["test_tracker_entity"]
    setup_ok = await _async_setup_runtime(hass, conf, {DOMAIN: conf})
    if setup_ok:
        async def _async_options_updated(hass: HomeAssistant, updated_entry) -> None:
            """Pas provideropties live toe zonder Home Assistant te herstarten."""
            setter = hass.data.get(DOMAIN, {}).get("set_lightning_source_mode")
            if setter is not None:
                await setter(updated_entry.options.get("lightning_source_mode", "auto"))

        entry.async_on_unload(entry.add_update_listener(_async_options_updated))
    return setup_ok


async def _async_setup_runtime(
    hass: HomeAssistant, conf: dict, config: ConfigType
) -> bool:
    """Gedeelde runtime voor YAML en config entries."""

    hass.data.setdefault(DOMAIN, {})
    hass.data[DOMAIN].setdefault("unsubscribers", [])
    if not hass.data[DOMAIN].get("geojson_http_registered"):
        hass.http.register_view(StormTrackerGeoJsonView(hass))
        hass.data[DOMAIN]["geojson_http_registered"] = True

    home_lat        = conf["home_lat"]
    home_lon        = conf["home_lon"]
    fictieve_entity = "zone.home"
    knmi_api_key    = conf.get("knmi_api_key")
    knmi_wms_key    = conf.get("knmi_wms_api_key", knmi_api_key)
    netatmo_radius  = conf.get("netatmo_radius_km", 175.0)
    radar_radius    = conf.get("radar_radius_km", 200.0)
    sharing_distance = conf.get("engine_sharing_distance_km", 150.0)
    lightning_source_mode = conf.get("lightning_source_mode", "auto")
    hsaf_username = conf.get("hsaf_username")
    hsaf_password = conf.get("hsaf_password")
    hass.data[DOMAIN]["lightning_source_mode"] = lightning_source_mode
    target_specs = build_target_specs(
        home_lat,
        home_lon,
        conf.get("targets"),
        conf.get("fictieve_tracker_entity"),
    )
    from homeassistant.helpers.aiohttp_client import async_get_clientsession
    http_session = async_get_clientsession(hass)
    places_path = Path(hass.config.path("www", "places.json"))
    try:
        location_places = await hass.async_add_executor_job(load_places_json, places_path)
    except (OSError, ValueError) as err:
        _LOGGER.warning("Lokale plaatsendatabase %s kon niet worden geladen: %s", places_path, err)
        location_places = ()
    hass.data[DOMAIN]["location_places_count"] = len(location_places)
    home_location = resolve_location(home_lat, home_lon, location_places, preferred_place="Thuis")

    hass.data[DOMAIN]["fictieve_entity"] = fictieve_entity
    hass.data[DOMAIN]["fictieve_lat"]    = home_lat
    hass.data[DOMAIN]["fictieve_lon"]    = home_lon
    hass.data[DOMAIN]["target_specs"] = target_specs
    hass.data[DOMAIN]["targets"] = {
        spec.target_id: {
            "id": spec.target_id,
            "name": spec.name,
            "entity_id": spec.entity_id,
            "latitude": spec.fallback_lat,
            "longitude": spec.fallback_lon,
            "available": spec.fallback_lat is not None,
            "primary": spec.primary,
            "radar_covered": spec.primary,
            "region_engine_id": None,
            "location_place": "Thuis" if spec.primary else None,
            "location_address": None,
            "country_code": home_location.country_code if spec.primary else None,
            "location_accuracy_km": home_location.distance_km if spec.primary else None,
        }
        for spec in target_specs
    }
    # Houd dezelfde Store-versie: de nieuwe per-engine envelop kan de oude
    # globale snapshot zelf migreren, zonder een HA Store-migratiefunctie.
    pressure_store = Store(hass, 1, f"{DOMAIN}_pressure_trend")
    restored_pressure_snapshot = await pressure_store.async_load() or {}
    pressure_trackers_by_engine: dict[str, PressureTrendTracker] = {}
    hass.data[DOMAIN]["netatmo_pressure_trend_trackers_by_engine"] = (
        pressure_trackers_by_engine
    )
    hass.data[DOMAIN]["netatmo_pressure_trends_by_engine"] = {}
    hass.data[DOMAIN]["netatmo_observations_by_engine"] = {}
    hass.data[DOMAIN]["open_meteo_providers_by_engine"] = {}
    hass.data[DOMAIN]["open_meteo_results_by_engine"] = {}
    hass.data[DOMAIN]["open_meteo_processed_sequences_by_engine"] = {}

    # ── StormEngine + OFE aanmaken ────────────────────────────────────────
    mcs_store = McsHistoryStore(hass)
    await mcs_store.async_load()
    storm_manager = StormManager(
        hass,
        sharing_distance_km=sharing_distance,
        observation_radius_km=radar_radius,
    )
    active_region = storm_manager.assign_target(fictieve_entity, home_lat, home_lon)
    hass.data[DOMAIN]["targets"]["home"]["region_engine_id"] = active_region.engine_id
    storm_engine = active_region.storm_engine
    ofe = active_region.ofe
    prepared_regions: set[str] = set()

    def _prepare_region(region) -> int:
        """Koppel opslag en publicatie eenmaal aan een runtime-regio."""
        if region.engine_id in prepared_regions:
            return 0
        region_storm_engine = region.storm_engine
        restored = mcs_store.restore_engine(region.storage_key, region_storm_engine)

        def _publish(storms) -> None:
            if region is active_region:
                hass.data[DOMAIN]["storms"] = storms
            status_cache = hass.data[DOMAIN].setdefault("mcs_status_by_storm", {})
            active_keys = set()
            for storm in storms:
                cache_key = f"{region.engine_id}:{storm.storm_id}"
                active_keys.add(cache_key)
                previous = status_cache.get(cache_key)
                current = getattr(storm, "mcs_status", "not_evaluated")
                if previous != current:
                    diagnostics = storm.mcs_diagnostics()
                    diagnostics.update({
                        "region_engine": region.engine_id,
                        "previous_status": previous,
                        "transition": f"{previous or 'new'}->{current}",
                    })
                    status_cache[cache_key] = current
                    hass.bus.async_fire(
                        f"{DOMAIN}_mcs_transition", diagnostics
                    )
                    _LOGGER.info(
                        "MCS-evaluatie %s in %s: %s (%s; span=%.1f km; "
                        "convectief=%d; intens=%d; duur=%.1f min; frames=%d)",
                        storm.storm_id,
                        region.engine_id,
                        diagnostics["transition"],
                        diagnostics["reason"],
                        diagnostics["convective_span_km"],
                        diagnostics["convective_cells"],
                        diagnostics["intense_cells"],
                        diagnostics["duration_minutes"],
                        diagnostics["sequence_frames"],
                    )
            stale_keys = [
                key for key in status_cache
                if key.startswith(f"{region.engine_id}:") and key not in active_keys
            ]
            for key in stale_keys:
                status_cache.pop(key, None)
            hass.bus.async_fire(
                f"{DOMAIN}_storms_updated",
                {
                    "count": len(storms),
                    "region_engine": region.engine_id,
                    "targets": sorted(region.projection_targets),
                },
            )
            hass.bus.async_fire(
                f"{DOMAIN}_targets_updated",
                {"targets": sorted(region.projection_targets)},
            )
            hass.async_create_task(
                mcs_store.async_save_engine(region.storage_key, region_storm_engine)
            )

        storm_engine._on_updated = _publish
        prepared_regions.add(region.engine_id)
        return restored

    def _activate_region(region) -> int:
        """Maak een manager-engine primair zichtbaar voor legacy-sensoren."""
        nonlocal active_region, storm_engine, ofe
        active_region = region
        storm_engine = region.storm_engine
        ofe = region.ofe
        restored = _prepare_region(region)
        hass.data[DOMAIN].update({
            "storm_manager": storm_manager,
            "region_engine": region,
            "region_engines": storm_manager.get_all_engines(),
            "storm_engine": storm_engine,
            "ofe": ofe,
            "storms": storm_engine.get_active_storms(),
        })
        _LOGGER.info(
            "RegionEngine %s actief; %d persistente MCS-systemen hersteld",
            region.engine_id, restored,
        )
        return restored

    _activate_region(active_region)

    def _pressure_tracker_for_region(region) -> PressureTrendTracker:
        """Geef iedere RegionEngine een strikt gescheiden drukhistoriek."""
        tracker = pressure_trackers_by_engine.get(region.engine_id)
        if tracker is not None:
            return tracker
        tracker = PressureTrendTracker()
        snapshots = restored_pressure_snapshot.get("engines", {})
        snapshot = snapshots.get(region.storage_key)
        # Eenmalige migratie van de oude globale opslag naar de thuisengine.
        if snapshot is None and region is storm_manager.get_engine_for_target("zone.home"):
            if "stations" in restored_pressure_snapshot:
                snapshot = restored_pressure_snapshot
        restored = tracker.restore(snapshot, time.time())
        pressure_trackers_by_engine[region.engine_id] = tracker
        if restored:
            _LOGGER.info(
                "Netatmo-drukhistoriek hersteld voor %s: %d stations",
                region.engine_id,
                restored,
            )
        return tracker

    _pressure_tracker_for_region(active_region)

    # ── Netatmo token (locatie-onafhankelijk) ─────────────────────────────
    client_id     = conf.get("netatmo_client_id")
    client_secret = conf.get("netatmo_client_secret")
    refresh_token = conf.get("netatmo_refresh_token")
    if client_id and client_secret and refresh_token:
        from .providers.netatmo import NetatmoTokenManager
        token_manager = NetatmoTokenManager(client_id, client_secret, refresh_token)
        hass.data[DOMAIN]["netatmo_token"] = token_manager
    hass.data[DOMAIN]["netatmo_providers_by_engine"] = {}

    # ── Blitzortung (wereldwijd, locatie-onafhankelijk) ───────────────────
    def _record_lightning(observation) -> None:
        """Bewaar een compacte kaartbuffer, los van regenpolygonen."""
        cutoff = time.time() - 15 * 60
        recent = [
            item for item in hass.data[DOMAIN].get("recent_lightning", [])
            if float(item.get("timestamp", 0)) >= cutoff
        ]
        engine_ids = [
            region.engine_id
            for region in storm_manager.get_all_engines()
            if region.accepts_observation(observation.lat, observation.lon)
        ]
        recent.append({
            "lat": observation.lat,
            "lon": observation.lon,
            "timestamp": observation.timestamp,
            "source": observation.source,
            "engine_ids": engine_ids,
        })
        hass.data[DOMAIN]["recent_lightning"] = recent[-1000:]

    def _on_blitz(obs):
        if hass.data[DOMAIN].get("lightning_source_mode") == "satellite_test":
            return
        if storm_manager.route_observation(obs) == 0:
            return
        hass.data[DOMAIN]["lightning_source"] = "blitzortung"
        hass.data[DOMAIN]["last_lightning"] = obs
        hass.data[DOMAIN].setdefault("lightning_count", 0)
        hass.data[DOMAIN]["lightning_count"] += 1
        _record_lightning(obs)
        hass.bus.async_fire(f"{DOMAIN}_lightning_update", {
            "lat": obs.lat, "lon": obs.lon, "timestamp": obs.timestamp,
            "source": obs.source,
        })
        log_lightning(hass, obs.lat, obs.lon, obs.timestamp)

    def _blitz_regions():
        return [
            (region.center_lat, region.center_lon, region.observation_radius_km)
            for region in storm_manager.get_all_engines()
        ]

    blitz = BlitzortungProvider(on_observation=_on_blitz, regions=_blitz_regions())
    if lightning_source_mode != "satellite_test":
        blitz.start()
    hass.data[DOMAIN]["blitz_provider"] = blitz

    eumetsat = None
    if conf.get("eumetsat_consumer_key") and conf.get("eumetsat_consumer_secret"):
        from .providers.eumetsat_li import EumetsatLightningProvider
        eumetsat = EumetsatLightningProvider(
            http_session,
            conf["eumetsat_consumer_key"],
            conf["eumetsat_consumer_secret"],
        )
        hass.data[DOMAIN]["eumetsat_li_provider"] = eumetsat
        hass.data[DOMAIN]["eumetsat_li_status"] = "standby"
        _LOGGER.info("EUMETSAT LI geconfigureerd als Blitzortung-fallback")

    from .providers.noaa_goes_glm import (
        NoaaGoesGlmProvider,
        preferred_source_for_longitude,
        satellites_for_regions,
    )
    goes_glm = NoaaGoesGlmProvider(http_session)
    hass.data[DOMAIN]["goes_glm_provider"] = goes_glm
    hass.data[DOMAIN]["goes18_glm_status"] = "standby"
    hass.data[DOMAIN]["goes19_glm_status"] = "standby"
    _LOGGER.info("NOAA GOES-18/19 GLM geconfigureerd als wereldwijde fallback")

    from .providers.base import CoverageArea, ProviderContext
    from .providers.dwd_radolan import DwdRadolanProvider
    from .providers.met_office_radar import MetOfficeRadarProvider
    from .providers.meteolux import MeteoLuxProvider
    from .providers.geosphere_at import GeoSphereAustriaProvider
    from .providers.italiameteo import ItaliaMeteoRadarProvider
    from .providers.dpc_radar import DpcRadarProvider
    from .providers.aemet_radar import AemetRadarProvider
    from .providers.lifecycle import ProviderLifecycleController

    provider_lifecycle = ProviderLifecycleController(cooldown_seconds=300)
    def _national_context(plugin, areas):
        return ProviderContext(
            hass=hass,
            area=areas[0],
            on_observation=lambda observation: None,
            config={"areas": areas},
        )

    provider_lifecycle.register(DwdRadolanProvider(http_session), _national_context)
    provider_lifecycle.register(MetOfficeRadarProvider(http_session), _national_context)
    provider_lifecycle.register(MeteoLuxProvider(http_session), _national_context)
    provider_lifecycle.register(GeoSphereAustriaProvider(http_session), _national_context)
    provider_lifecycle.register(ItaliaMeteoRadarProvider(http_session), _national_context)
    provider_lifecycle.register(DpcRadarProvider(http_session), _national_context)
    provider_lifecycle.register(AemetRadarProvider(http_session), _national_context)
    if conf.get("meteofrance_api_token") or conf.get("meteofrance_application_id"):
        from .providers.meteofrance_radar import MeteoFranceRadarProvider
        provider_lifecycle.register(
            MeteoFranceRadarProvider(
                http_session,
                token=conf.get("meteofrance_api_token"),
                application_id=conf.get("meteofrance_application_id"),
            ),
            _national_context,
        )
    hass.data[DOMAIN]["provider_lifecycle"] = provider_lifecycle
    hass.data[DOMAIN]["provider_lifecycle_diagnostics"] = (
        provider_lifecycle.diagnostics()
    )
    hass.data[DOMAIN]["radar_sources_by_engine"] = {}
    hass.data[DOMAIN]["opera_providers_by_engine"] = {}
    hass.data[DOMAIN]["rainviewer_providers_by_engine"] = {}
    from .providers.noaa_goes_rrqpe import (
        NoaaGoesRrqpeProvider,
        satellite_for_longitude as goes_rrqpe_satellite_for_longitude,
    )
    hass.data[DOMAIN]["noaa_goes_rrqpe_provider"] = NoaaGoesRrqpeProvider(http_session)
    _LOGGER.info("NOAA GOES-18/19 RRQPE geconfigureerd als slapende Amerika-fallback")
    if hsaf_username and hsaf_password:
        from .providers.hsaf_h40b import HsafH40bProvider
        hass.data[DOMAIN]["hsaf_h40b_provider"] = HsafH40bProvider(
            hsaf_username, hsaf_password
        )
        _LOGGER.info("H SAF H40B geconfigureerd als slapende satellietfallback")
    else:
        hass.data[DOMAIN]["hsaf_h40b_provider"] = None

    def _engine_country_codes(region) -> tuple[str, ...]:
        return tuple(sorted({
            str(target.get("country_code")).upper()
            for target in hass.data[DOMAIN].get("targets", {}).values()
            if target.get("region_engine_id") == region.engine_id
            and target.get("country_code")
        }))

    def _radar_source_states(now_ts: float, region=None):
        from .providers.engine_radar_policy import SourceState
        lifecycle = provider_lifecycle.diagnostics()
        states = {}
        for provider_id in ("dwd_radolan", "met_office_radar", "meteofrance_radar", "meteolux", "dpc_radar", "aemet_radar"):
            details = lifecycle.get(provider_id, {})
            overlay = provider_lifecycle.overlay(provider_id) or {}
            product_timestamp = overlay.get("timestamp")
            states[provider_id] = SourceState(
                configured=provider_id in lifecycle,
                healthy=bool(
                    details.get("status") == "active"
                    and details.get("error") is None
                    and product_timestamp is not None
                    and now_ts - float(product_timestamp) <= 20 * 60
                ),
                last_success=(
                    float(product_timestamp)
                    if product_timestamp is not None else None
                ),
            )
        kmi = hass.data[DOMAIN].get("kmi_provider")
        kmi_timestamp = getattr(kmi, "last_frame_timestamp", None) if kmi else None
        states["kmi"] = SourceState(
            configured=kmi is not None,
            healthy=bool(kmi_timestamp and now_ts - float(kmi_timestamp) <= 20 * 60),
            last_success=float(kmi_timestamp) if kmi_timestamp else None,
        )
        knmi = hass.data[DOMAIN].get("knmi_provider")
        knmi_timestamp = getattr(knmi, "last_frame_timestamp", None) if knmi else None
        states["knmi"] = SourceState(
            configured=knmi is not None,
            healthy=bool(knmi_timestamp and now_ts - knmi_timestamp <= 20 * 60),
            last_success=knmi_timestamp,
        )
        opera = (
            hass.data[DOMAIN].get("opera_providers_by_engine", {}).get(region.engine_id)
            if region is not None else hass.data[DOMAIN].get("opera_provider")
        )
        opera_success = getattr(opera, "_last_success_ts", None) if opera else None
        states["opera"] = SourceState(
            configured=opera is not None,
            healthy=bool(opera and opera.healthy),
            last_success=opera_success,
        )
        rainviewer = (
            hass.data[DOMAIN].get("rainviewer_providers_by_engine", {}).get(region.engine_id)
            if region is not None else hass.data[DOMAIN].get("rv_provider")
        )
        rv_success = getattr(rainviewer, "_last_success_ts", None) if rainviewer else None
        states["rainviewer"] = SourceState(
            configured=rainviewer is not None,
            healthy=bool(rainviewer and rainviewer.healthy),
            last_success=rv_success,
        )
        hsaf = hass.data[DOMAIN].get("hsaf_h40b_provider")
        hsaf_success = getattr(hsaf, "_last_success_ts", None) if hsaf else None
        states["hsaf_h40b"] = SourceState(
            configured=hsaf is not None,
            healthy=bool(
                hsaf
                and hsaf.healthy
                and hsaf_success is not None
                and now_ts - float(hsaf_success) <= 90 * 60
            ),
            last_success=float(hsaf_success) if hsaf_success is not None else None,
        )
        goes = hass.data[DOMAIN].get("noaa_goes_rrqpe_provider")
        goes_success = getattr(goes, "_last_success_ts", None) if goes else None
        goes_supported = bool(
            region is not None
            and goes is not None
            and goes_rrqpe_satellite_for_longitude(region.center_lon) is not None
        )
        states["noaa_goes_rrqpe"] = SourceState(
            configured=goes_supported,
            healthy=bool(
                goes_supported and goes.healthy and goes_success is not None
                and now_ts - float(goes_success) <= 45 * 60
            ),
            last_success=float(goes_success) if goes_success is not None else None,
        )
        return states

    def _refresh_engine_radar_decisions():
        from .providers.engine_radar_policy import (
            apply_echo_availability,
            select_engine_radar_source,
        )
        now_ts = time.time()
        previous_decisions = hass.data[DOMAIN].get("radar_sources_by_engine", {})
        transitions = hass.data[DOMAIN].setdefault("radar_source_transitions", {})
        decisions = {}
        for region in storm_manager.get_all_engines():
            states = _radar_source_states(now_ts, region)
            decision = select_engine_radar_source(
                _engine_country_codes(region), states, now=now_ts
            )
            decision = apply_echo_availability(
                decision,
                states,
                opera_observations=hass.data[DOMAIN]
                .get("opera_observation_counts_by_engine", {})
                .get(region.engine_id, 0),
                rainviewer_observations=hass.data[DOMAIN]
                .get("rainviewer_observation_counts_by_engine", {})
                .get(region.engine_id, 0),
                now=now_ts,
                hsaf_observations=hass.data[DOMAIN]
                .get("hsaf_h40b_observation_counts_by_engine", {})
                .get(region.engine_id, 0),
                goes_observations=hass.data[DOMAIN]
                .get("noaa_goes_rrqpe_observation_counts_by_engine", {})
                .get(region.engine_id, 0),
                opera_coverage_complete=bool(
                    hass.data[DOMAIN]
                    .get("opera_providers_by_engine", {})
                    .get(region.engine_id)
                    .coverage_complete
                ) if hass.data[DOMAIN]
                .get("opera_providers_by_engine", {})
                .get(region.engine_id) is not None else True,
                opera_corroborated_observations=hass.data[DOMAIN]
                .get("opera_corroborated_counts_by_engine", {})
                .get(region.engine_id, 0),
            )
            previous_source = (
                previous_decisions.get(region.engine_id) or {}
            ).get("source")
            if previous_source and decision.source and previous_source != decision.source:
                transitions[region.engine_id] = {
                    "from": previous_source,
                    "to": decision.source,
                    "started_at": now_ts,
                    "active_until": now_ts + 10 * 60,
                }
            transition = transitions.get(region.engine_id)
            if transition and now_ts >= transition["active_until"]:
                transitions.pop(region.engine_id, None)
                transition = None
            decisions[region.engine_id] = {
                "source": decision.source,
                "reason": decision.reason,
                "country_codes": list(decision.country_codes),
                "age_seconds": round(decision.age_seconds, 1) if decision.age_seconds is not None else None,
                "transition": transition,
                "opera_accepted_observations": hass.data[DOMAIN]
                .get("opera_observation_counts_by_engine", {})
                .get(region.engine_id, 0),
                "rainviewer_observations": hass.data[DOMAIN]
                .get("rainviewer_observation_counts_by_engine", {})
                .get(region.engine_id, 0),
                "goes_rrqpe": {
                    "supported": states["noaa_goes_rrqpe"].configured,
                    "status": (
                        hass.data[DOMAIN]
                        .get("noaa_goes_rrqpe_diagnostics", {})
                        .get("status", "standby")
                    ),
                    "observations": hass.data[DOMAIN]
                    .get("noaa_goes_rrqpe_observation_counts_by_engine", {})
                    .get(region.engine_id, 0),
                    "satellites": hass.data[DOMAIN]
                    .get("noaa_goes_rrqpe_diagnostics", {})
                    .get("satellites", []),
                },
            }
        hass.data[DOMAIN]["radar_sources_by_engine"] = decisions
        unique = {item["source"] for item in decisions.values() if item["source"]}
        hass.data[DOMAIN]["active_radar_source"] = next(iter(unique)) if len(unique) == 1 else "per_engine"
        hass.data[DOMAIN]["radar_source_reason"] = (
            "afzonderlijke bronkeuze per RegionEngine" if len(unique) > 1
            else next(iter(decisions.values()), {}).get("reason", "geen actieve engine")
        )
        return decisions

    def _route_selected_radar(observations, source: str) -> int:
        decisions = hass.data[DOMAIN].get("radar_sources_by_engine", {})
        routed = 0
        for region in storm_manager.get_all_engines():
            if (decisions.get(region.engine_id) or {}).get("source") != source:
                continue
            for observation in observations:
                routed += int(storm_manager.route_observation_to_engine(
                    region.engine_id, observation
                ))
        return routed
    from .engine.radar_calibration import RadarCalibrationObserver
    from .engine.calibration_store import CalibrationDataStore
    hass.data[DOMAIN]["radar_calibration_observer"] = RadarCalibrationObserver(
        evaluation_center=(home_lat, home_lon),
        evaluation_radius_km=radar_radius,
    )
    calibration_store = CalibrationDataStore(
        hass.config.path(".storage", "storm_tracker_v3_calibration.sqlite3")
    )
    calibration_stats = await hass.async_add_executor_job(
        calibration_store.initialize
    )
    hass.data[DOMAIN]["radar_calibration_store"] = calibration_store
    hass.data[DOMAIN]["radar_calibration_storage"] = {
        "status": "ready", "frames_written": 0,
        "comparisons_written": 0, **calibration_stats,
    }
    hass.data[DOMAIN]["radar_calibration"] = (
        hass.data[DOMAIN]["radar_calibration_observer"].diagnostics()
    )

    def _record_calibration_frame(
        source: str,
        observations,
        timestamp: float | None = None,
        *,
        engine_id: str | None = None,
    ) -> int:
        """Registreer een providerframe uitsluitend binnen zijn RegionEngine(s)."""
        observer = hass.data[DOMAIN]["radar_calibration_observer"]
        observations = list(observations)
        if timestamp is None and observations:
            timestamp = float(observations[0].timestamp)
        if timestamp is None:
            return 0
        matched = 0
        for region in storm_manager.get_all_engines():
            if engine_id is not None and region.engine_id != engine_id:
                continue
            regional = [
                item for item in observations
                if region.accepts_observation(item.lat, item.lon)
            ]
            # Voor gedeelde producten zonder expliciete engine vermijden we
            # dat een regio buiten de productdekking als kunstmatig droog telt.
            if engine_id is None and not regional:
                continue
            matched += observer.record_frame(
                regional,
                source=source,
                timestamp=float(timestamp),
                region_id=f"{region.engine_id}@{region.storage_key}",
                evaluation_center=(region.center_lat, region.center_lon),
                evaluation_radius_km=region.observation_radius_km,
            )
        diagnostics = observer.diagnostics()
        diagnostics["storage"] = hass.data[DOMAIN].get(
            "radar_calibration_storage", {}
        )
        hass.data[DOMAIN]["radar_calibration"] = diagnostics
        hass.bus.async_fire(
            f"{DOMAIN}_calibration_update",
            {"samples": hass.data[DOMAIN]["radar_calibration"]["samples"]},
        )
        return matched

    # ── Locatie-afhankelijke providers initialiseren ──────────────────────
    async def _init_location_providers(lat: float, lon: float) -> None:
        """Start of herstart alle locatie-afhankelijke providers op nieuwe locatie."""
        _LOGGER.info("Providers initialiseren voor (%.4f,%.4f)", lat, lon)

    def _sync_region_netatmo_providers() -> None:
        """Houd één Netatmo-provider en druktracker per RegionEngine bij."""
        from .providers.netatmo import NetatmoProvider

        token = hass.data[DOMAIN].get("netatmo_token")
        providers = hass.data[DOMAIN]["netatmo_providers_by_engine"]
        regions = {region.engine_id: region for region in storm_manager.get_all_engines()}
        for engine_id in tuple(providers):
            if engine_id not in regions:
                providers.pop(engine_id, None)
                pressure_trackers_by_engine.pop(engine_id, None)
        if token is None:
            return
        for engine_id, region in regions.items():
            provider = providers.get(engine_id)
            if (
                provider is None
                or abs(provider._lat - region.center_lat) > 0.01
                or abs(provider._lon - region.center_lon) > 0.01
            ):
                providers[engine_id] = NetatmoProvider(
                    token,
                    region.center_lat,
                    region.center_lon,
                    netatmo_radius,
                )
                _LOGGER.info(
                    "Netatmo: %s gestart rond %.4f,%.4f (r=%.0fkm)",
                    engine_id,
                    region.center_lat,
                    region.center_lon,
                    netatmo_radius,
                )
            _pressure_tracker_for_region(region)

    def _sync_region_open_meteo_providers() -> None:
        """Houd het Open-Meteo-modelgrid strikt per RegionEngine gescheiden."""
        from .providers.open_meteo import OpenMeteoProvider

        providers = hass.data[DOMAIN]["open_meteo_providers_by_engine"]
        regions = {region.engine_id: region for region in storm_manager.get_all_engines()}
        for engine_id in tuple(providers):
            if engine_id not in regions:
                providers.pop(engine_id, None)
                hass.data[DOMAIN]["open_meteo_results_by_engine"].pop(engine_id, None)
                hass.data[DOMAIN]["open_meteo_processed_sequences_by_engine"].pop(
                    engine_id, None
                )
        for engine_id, region in regions.items():
            provider = providers.get(engine_id)
            if (
                provider is None
                or abs(provider._lat - region.center_lat) > 0.01
                or abs(provider._lon - region.center_lon) > 0.01
            ):
                providers[engine_id] = OpenMeteoProvider(
                    region.center_lat, region.center_lon
                )
                _LOGGER.info(
                    "Open-Meteo: %s gestart rond %.4f,%.4f",
                    engine_id,
                    region.center_lat,
                    region.center_lon,
                )

    def _sync_region_radar_providers() -> None:
        """Houd OPERA- en RainViewer-instanties gelijk aan actieve engines."""
        from .providers.kmi import KmiProvider, KmiProviderFactory
        from .providers.knmi import KnmiProvider, KnmiProviderFactory
        from .providers.opera import OperaProvider, OperaProviderFactory
        from .providers.rainviewer import RainViewerProvider

        regions = {region.engine_id: region for region in storm_manager.get_all_engines()}
        kmi_region = next((
            region for region in regions.values()
            if KmiProviderFactory.supports(
                region.center_lat, region.center_lon, region.observation_radius_km
            )
        ), None)
        if kmi_region is not None and hass.data[DOMAIN].get("kmi_provider") is None:
            hass.data[DOMAIN]["kmi_provider"] = KmiProvider(
                kmi_region.center_lat, kmi_region.center_lon
            )
            _LOGGER.info("KMI: geactiveerd voor %s", kmi_region.engine_id)
        elif kmi_region is None:
            hass.data[DOMAIN]["kmi_provider"] = None

        knmi_region = next((
            region for region in regions.values()
            if knmi_api_key and KnmiProviderFactory.supports(
                region.center_lat, region.center_lon, region.observation_radius_km
            )
        ), None)
        if knmi_region is not None and hass.data[DOMAIN].get("knmi_provider") is None:
            hass.data[DOMAIN]["knmi_provider"] = KnmiProvider(
                knmi_region.center_lat,
                knmi_region.center_lon,
                knmi_api_key,
                knmi_wms_key,
            )
            _LOGGER.info("KNMI: geactiveerd voor %s", knmi_region.engine_id)
        elif knmi_region is None:
            hass.data[DOMAIN]["knmi_provider"] = None

        opera_providers = hass.data[DOMAIN]["opera_providers_by_engine"]
        rainviewer_providers = hass.data[DOMAIN]["rainviewer_providers_by_engine"]
        for engine_id in tuple(opera_providers):
            if engine_id not in regions:
                opera_providers.pop(engine_id, None)
        for engine_id in tuple(rainviewer_providers):
            if engine_id not in regions:
                rainviewer_providers.pop(engine_id, None)
        for engine_id, region in regions.items():
            if engine_id not in rainviewer_providers:
                rainviewer_providers[engine_id] = RainViewerProvider(
                    region.center_lat, region.center_lon
                )
            if (
                engine_id not in opera_providers
                and OperaProviderFactory.supports(
                    region.center_lat, region.center_lon, region.observation_radius_km
                )
            ):
                opera_providers[engine_id] = OperaProvider(
                    region.center_lat,
                    region.center_lon,
                    region.observation_radius_km,
                    session=http_session,
                )
        home_region = storm_manager.get_engine_for_target("zone.home")
        hass.data[DOMAIN]["opera_provider"] = (
            opera_providers.get(home_region.engine_id) if home_region else None
        )
        hass.data[DOMAIN]["rv_provider"] = (
            rainviewer_providers.get(home_region.engine_id) if home_region else None
        )
        hass.data[DOMAIN]["radar_provider_engines"] = {
            "opera": sorted(opera_providers),
            "rainviewer": sorted(rainviewer_providers),
        }
        covered_engines = set(opera_providers) | set(rainviewer_providers)
        for target in hass.data[DOMAIN].get("targets", {}).values():
            target["radar_covered"] = target.get("region_engine_id") in covered_engines

    # ── Poll functies ─────────────────────────────────────────────────────

    async def _poll_kmi(now=None):
        p = hass.data[DOMAIN].get("kmi_provider")
        if not p: return
        obs = await p.fetch_observations()
        if not p.last_fetch_updated:
            return
        hass.data[DOMAIN]["last_kmi_observations"] = obs
        hass.data[DOMAIN]["kmi_frame_timestamp"] = p.last_frame_timestamp
        hass.data[DOMAIN]["kmi_count"] = len(obs)
        from .providers.kmi import KmiProviderFactory
        calibration_obs = [
            observation for observation in obs
            if (observation.intensity or 0) >= 2
        ]
        for region in storm_manager.get_all_engines():
            if KmiProviderFactory.supports(
                region.center_lat, region.center_lon, region.observation_radius_km
            ):
                _record_calibration_frame(
                    "kmi", calibration_obs, p.last_frame_timestamp,
                    engine_id=region.engine_id,
                )
        hass.bus.async_fire(f"{DOMAIN}_radar_update", {"source": "kmi", "count": len(obs)})
        lat = hass.data[DOMAIN].get("fictieve_lat", home_lat)
        lon = hass.data[DOMAIN].get("fictieve_lon", home_lon)
        log_kmi(hass, obs, lat, lon)
        # De router beslist per engine of KMI operationeel of vergelijking is.

    async def _poll_rv(now=None, operational: bool = False):
        _sync_region_radar_providers()
        providers = hass.data[DOMAIN].get("rainviewer_providers_by_engine", {})
        obs = []
        diagnostics = {}
        observation_counts = {}
        regions_by_id = {
            region.engine_id: region for region in storm_manager.get_all_engines()
        }
        engine_ids = tuple(providers)
        fetched = await asyncio.gather(*(
            asyncio.wait_for(
                providers[engine_id].fetch_observations(), timeout=20
            )
            for engine_id in engine_ids
        ), return_exceptions=True)
        for engine_id, result in zip(engine_ids, fetched):
            provider = providers[engine_id]
            if isinstance(result, Exception):
                provider._mark_unhealthy(
                    "regionale fetch-timeout" if isinstance(result, asyncio.TimeoutError)
                    else type(result).__name__
                )
                _LOGGER.warning("RainViewer %s mislukt: %s", engine_id, result)
                engine_obs = []
            else:
                engine_obs = result
            obs.extend(engine_obs)
            region = regions_by_id.get(engine_id)
            observation_counts[engine_id] = sum(
                1 for item in engine_obs
                if region is not None and region.accepts_observation(item.lat, item.lon)
            )
            diagnostics[engine_id] = {
                **provider.diagnostics,
                "observations": len(engine_obs),
            }
            _record_calibration_frame(
                "rainviewer", engine_obs,
                getattr(provider, "_last_frame_ts", None),
                engine_id=engine_id,
            )
        hass.data[DOMAIN]["last_rv_observations"] = obs
        hass.data[DOMAIN]["rv_count"] = len(obs)
        hass.data[DOMAIN]["rainviewer_diagnostics_by_engine"] = diagnostics
        hass.data[DOMAIN]["rainviewer_observation_counts_by_engine"] = observation_counts
        hass.bus.async_fire(f"{DOMAIN}_radar_update", {"source": "rainviewer", "count": len(obs)})
        lat = hass.data[DOMAIN].get("fictieve_lat", home_lat)
        lon = hass.data[DOMAIN].get("fictieve_lon", home_lon)
        log_rainviewer(hass, obs, lat, lon)
        if operational:
            for o in obs:
                storm_manager.route_observation(o)
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
        from .providers.knmi import KnmiProviderFactory
        for region in storm_manager.get_all_engines():
            if KnmiProviderFactory.supports(
                region.center_lat, region.center_lon, region.observation_radius_km
            ):
                _record_calibration_frame(
                    "knmi", current, getattr(p, "last_frame_timestamp", None),
                    engine_id=region.engine_id,
                )
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
        # Alleen actuele KNMI-observaties kunnen operationeel worden gerouteerd.

    async def _poll_opera(now=None):
        _sync_region_radar_providers()
        providers = hass.data[DOMAIN].get("opera_providers_by_engine", {})
        if not providers:
            return []
        raw_by_engine = {}
        provider_diagnostics = {}
        product_timestamps = []
        regions_by_id = {
            region.engine_id: region for region in storm_manager.get_all_engines()
        }
        engine_ids = tuple(providers)
        fetched = await asyncio.gather(*(
            asyncio.wait_for(
                providers[engine_id].fetch_observations(hass), timeout=40
            )
            for engine_id in engine_ids
        ), return_exceptions=True)
        for engine_id, result in zip(engine_ids, fetched):
            provider = providers[engine_id]
            if isinstance(result, Exception):
                provider._healthy = False
                provider._last_error = (
                    "regionale fetch-timeout" if isinstance(result, asyncio.TimeoutError)
                    else type(result).__name__
                )
                _LOGGER.warning("OPERA %s mislukt: %s", engine_id, result)
                engine_obs = []
            else:
                engine_obs = result
            raw_by_engine[engine_id] = list(engine_obs)
            provider_diagnostics[engine_id] = {
                **provider.diagnostics,
                "raw_observations": len(engine_obs),
            }
            if getattr(provider, "_last_product_ts", None) is not None:
                product_timestamps.append(float(provider._last_product_ts))
            _record_calibration_frame(
                "opera", engine_obs,
                getattr(provider, "_last_product_ts", None),
                engine_id=engine_id,
            )

        # A low OPERA quality score is not automatically dry: RainViewer or a
        # national radar may still confirm a genuine shower. Conversely,
        # unconfirmed low-quality echoes must not create phantom systems.
        from .providers.radar_policy import (
            OPERA_MIN_STANDALONE_QUALITY,
            corroboration_source_counts,
            usable_corroborating_observations,
            verify_opera_observations,
        )
        raw_references = list(hass.data[DOMAIN].get("last_kmi_observations", []))
        raw_references.extend(hass.data[DOMAIN].get("knmi_current", []))
        raw_references.extend(hass.data[DOMAIN].get("last_rv_observations", []))
        raw_references.extend(hass.data[DOMAIN].get("dwd_radolan_observations", []))
        raw_references.extend(hass.data[DOMAIN].get("met_office_radar_observations", []))
        raw_references.extend(hass.data[DOMAIN].get("meteofrance_radar_observations", []))
        raw_references.extend(hass.data[DOMAIN].get("meteolux_observations", []))
        raw_references.extend(hass.data[DOMAIN].get("dpc_radar_observations", []))
        raw_references.extend(hass.data[DOMAIN].get("aemet_radar_observations", []))
        raw_references.extend(hass.data[DOMAIN].get("hsaf_h40b_observations", []))
        raw_references.extend(hass.data[DOMAIN].get("noaa_goes_rrqpe_observations", []))
        references = usable_corroborating_observations(raw_references)
        verification_by_engine = {
            engine_id: verify_opera_observations(engine_obs, references)
            for engine_id, engine_obs in raw_by_engine.items()
        }
        accepted_by_engine = {
            engine_id: list(result.accepted)
            for engine_id, result in verification_by_engine.items()
        }
        for engine_id, provider in providers.items():
            provider.apply_accepted_overlay(
                accepted_by_engine.get(engine_id, [])
            )
        raw_obs = [item for values in raw_by_engine.values() for item in values]
        obs = [item for values in accepted_by_engine.values() for item in values]
        observation_counts = {
            engine_id: sum(
                1 for item in engine_obs
                if regions_by_id.get(engine_id) is not None
                and regions_by_id[engine_id].accepts_observation(item.lat, item.lon)
            )
            for engine_id, engine_obs in accepted_by_engine.items()
        }

        diagnostics = {
            "engines": provider_diagnostics,
            "provider_count": len(providers),
            "cells": [
                {**cell, "engine_id": engine_id}
                for engine_id, details in provider_diagnostics.items()
                for cell in details.get("cells", [])
            ],
        }
        for engine_id, details in provider_diagnostics.items():
            verification = verification_by_engine[engine_id]
            details.update({
                "accepted_observations": len(verification.accepted),
                "accepted_high_quality": verification.high_quality,
                "accepted_structured_echo": verification.structured_echo,
                "accepted_corroborated": verification.corroborated,
                "rejected_unconfirmed": verification.rejected,
            })
        accepted_ids_by_engine = {
            engine_id: {
                str(getattr(item, "radar_cell_id", ""))
                for item in engine_obs
            }
            for engine_id, engine_obs in accepted_by_engine.items()
        }
        for cell in diagnostics.get("cells", []):
            engine_id = cell["engine_id"]
            provider = providers[engine_id]
            timestamp = getattr(provider, "_last_product_ts", None)
            timestamp_text = (
                datetime.fromtimestamp(timestamp, timezone.utc).strftime("%Y%m%dT%H%M%SZ")
                if timestamp is not None else ""
            )
            cell_id = (
                f"opera:{timestamp_text}:p{cell.get('parent_component', 0)}:"
                f"c{cell.get('child_component', 0)}"
            )
            accepted = cell_id in accepted_ids_by_engine.get(engine_id, set())
            cell["accepted"] = accepted
            cell["verification"] = (
                "high_quality"
                if accepted and cell.get("quality", 0) >= OPERA_MIN_STANDALONE_QUALITY
                else "structured_echo"
                if accepted
                and cell.get("mean_dbz", 0) >= 20.0
                and cell.get("max_dbz", 0) >= 30.0
                and cell.get("area_km2", 0) >= 50.0
                else "corroborated"
                if accepted
                else "rejected_unconfirmed"
            )
        totals = {
            "high_quality": sum(v.high_quality for v in verification_by_engine.values()),
            "structured_echo": sum(v.structured_echo for v in verification_by_engine.values()),
            "corroborated": sum(v.corroborated for v in verification_by_engine.values()),
            "rejected": sum(v.rejected for v in verification_by_engine.values()),
        }
        diagnostics.update({
            "raw_count": len(raw_obs),
            "accepted_count": len(obs),
            "accepted_high_quality": totals["high_quality"],
            "accepted_structured_echo": totals["structured_echo"],
            "accepted_corroborated": totals["corroborated"],
            "rejected_unconfirmed": totals["rejected"],
            "corroboration_sources": corroboration_source_counts(references),
            "corroboration_references_raw": len(raw_references),
            "corroboration_references_usable": len(references),
        })
        hass.data[DOMAIN]["opera_count"] = len(obs)
        hass.data[DOMAIN]["opera_observation_counts_by_engine"] = observation_counts
        hass.data[DOMAIN]["opera_corroborated_counts_by_engine"] = {
            engine_id: result.corroborated
            for engine_id, result in verification_by_engine.items()
        }
        hass.data[DOMAIN]["opera_observations_by_engine"] = accepted_by_engine
        hass.data[DOMAIN]["opera_diagnostics"] = diagnostics
        hass.bus.async_fire(f"{DOMAIN}_radar_update", {"source": "opera", "count": len(obs)})
        _LOGGER.info(
            "OPERA verificatie: raw=%d accepted=%d (quality=%d structure=%d confirmed=%d) rejected=%d",
            len(raw_obs), len(obs), totals["high_quality"],
            totals["structured_echo"], totals["corroborated"],
            totals["rejected"],
        )
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
        _sync_region_radar_providers()
        rainviewer_obs = await _poll_rv(operational=False)
        opera_obs = await _poll_opera()
        decisions = _refresh_engine_radar_decisions()
        fallback_regions = [
            region for region in storm_manager.get_all_engines()
            if (decisions.get(region.engine_id) or {}).get("source")
            in {None, "rainviewer", "hsaf_h40b"}
        ]
        calibration_due = (
            time.time()
            - float(hass.data[DOMAIN].get("hsaf_h40b_last_calibration_probe", 0.0))
            >= 6 * 60 * 60
        )
        hsaf_regions = (
            fallback_regions
            if fallback_regions
            else list(storm_manager.get_all_engines()) if calibration_due else []
        )
        hsaf_obs = []
        hsaf = hass.data[DOMAIN].get("hsaf_h40b_provider")
        if hsaf is not None and hsaf_regions:
            hsaf_obs = await hsaf.async_fetch(tuple(
                CoverageArea(
                    region.center_lat,
                    region.center_lon,
                    region.observation_radius_km,
                )
                for region in hsaf_regions
            ))
            hass.data[DOMAIN]["hsaf_h40b_observations"] = hsaf_obs
            hass.data[DOMAIN]["hsaf_h40b_observation_counts_by_engine"] = {
                region.engine_id: sum(
                    1 for item in hsaf_obs
                    if region.accepts_observation(item.lat, item.lon)
                )
                for region in hsaf_regions
            }
            hass.data[DOMAIN]["hsaf_h40b_diagnostics"] = hsaf.diagnostics
            if hsaf.healthy:
                hass.data[DOMAIN]["hsaf_h40b_last_calibration_probe"] = time.time()
                overlay_timestamp = (
                    hsaf.overlay or {}
                ).get("timestamp")
                if overlay_timestamp is not None:
                    for region in hsaf_regions:
                        _record_calibration_frame(
                            "hsaf_h40b", hsaf_obs, float(overlay_timestamp),
                            engine_id=region.engine_id,
                        )
            decisions = _refresh_engine_radar_decisions()
        elif hsaf is not None:
            hsaf.sleep()
            hass.data[DOMAIN]["hsaf_h40b_diagnostics"] = hsaf.diagnostics

        goes_regions = [
            region for region in storm_manager.get_all_engines()
            if (decisions.get(region.engine_id) or {}).get("source")
            in {None, "rainviewer", "noaa_goes_rrqpe"}
        ]
        goes_obs = []
        goes_rrqpe = hass.data[DOMAIN].get("noaa_goes_rrqpe_provider")
        if goes_rrqpe is not None and goes_regions:
            goes_obs = await goes_rrqpe.async_fetch(tuple(
                CoverageArea(region.center_lat, region.center_lon, region.observation_radius_km)
                for region in goes_regions
            ))
            hass.data[DOMAIN]["noaa_goes_rrqpe_observations"] = goes_obs
            hass.data[DOMAIN]["noaa_goes_rrqpe_observation_counts_by_engine"] = {
                region.engine_id: sum(
                    1 for item in goes_obs if region.accepts_observation(item.lat, item.lon)
                )
                for region in goes_regions
            }
            hass.data[DOMAIN]["noaa_goes_rrqpe_diagnostics"] = goes_rrqpe.diagnostics
            if goes_rrqpe.healthy and goes_rrqpe.overlay:
                for region in goes_regions:
                    _record_calibration_frame(
                        "noaa_goes_rrqpe", goes_obs,
                        float(goes_rrqpe.overlay["timestamp"]),
                        engine_id=region.engine_id,
                    )
            decisions = _refresh_engine_radar_decisions()
        elif goes_rrqpe is not None:
            goes_rrqpe.sleep()
            hass.data[DOMAIN]["noaa_goes_rrqpe_diagnostics"] = goes_rrqpe.diagnostics
        _route_selected_radar(opera_obs, "opera")
        _route_selected_radar(rainviewer_obs, "rainviewer")
        _route_selected_radar(hsaf_obs, "hsaf_h40b")
        _route_selected_radar(goes_obs, "noaa_goes_rrqpe")
        _route_selected_radar(hass.data[DOMAIN].get("last_kmi_observations", []), "kmi")
        _route_selected_radar(hass.data[DOMAIN].get("knmi_current", []), "knmi")
        _refresh_radar_overlays(decisions)

        hass.bus.async_fire(f"{DOMAIN}_radar_source_update", {
            "source": hass.data[DOMAIN].get("active_radar_source"),
            "reason": hass.data[DOMAIN].get("radar_source_reason"),
            "engines": decisions,
        })

    async def _poll_radar_comparison(now=None):
        """Keep national products observable without feeding the OFE."""
        _sync_region_radar_providers()
        await _poll_kmi()
        await _poll_knmi()

    def _provider_areas():
        return tuple(
            CoverageArea(
                region.center_lat,
                region.center_lon,
                region.observation_radius_km,
            )
            for region in storm_manager.get_all_engines()
        )

    async def _poll_national_providers(now=None):
        await provider_lifecycle.async_reconcile(_provider_areas())
        results = await provider_lifecycle.async_fetch_active()
        if "dwd_radolan" in results:
            hass.data[DOMAIN]["dwd_radolan_observations"] = results["dwd_radolan"]
        if "met_office_radar" in results:
            hass.data[DOMAIN]["met_office_radar_observations"] = results["met_office_radar"]
        if "meteofrance_radar" in results:
            hass.data[DOMAIN]["meteofrance_radar_observations"] = results["meteofrance_radar"]
        if "meteolux" in results:
            hass.data[DOMAIN]["meteolux_observations"] = results["meteolux"]
        if "dpc_radar" in results:
            hass.data[DOMAIN]["dpc_radar_observations"] = results["dpc_radar"]
        if "aemet_radar" in results:
            hass.data[DOMAIN]["aemet_radar_observations"] = results["aemet_radar"]
        for provider_id, observations in results.items():
            if provider_id in {
                "dwd_radolan", "met_office_radar", "meteofrance_radar",
                "dpc_radar", "aemet_radar",
            }:
                _record_calibration_frame(provider_id, observations)
        decisions = _refresh_engine_radar_decisions()
        for provider_id in ("dwd_radolan", "met_office_radar", "meteofrance_radar", "meteolux", "dpc_radar", "aemet_radar"):
            if provider_id in results:
                _route_selected_radar(results[provider_id], provider_id)
        hass.data[DOMAIN]["provider_lifecycle_diagnostics"] = (
            provider_lifecycle.diagnostics()
        )

    def _refresh_radar_overlays(decisions):
        """Publiceer bronpixels voor de per-engine gekozen rasterprovider."""
        overlays = {}
        shared = {
            "kmi": hass.data[DOMAIN].get("kmi_provider"),
            "knmi": hass.data[DOMAIN].get("knmi_provider"),
            "hsaf_h40b": hass.data[DOMAIN].get("hsaf_h40b_provider"),
            "noaa_goes_rrqpe": hass.data[DOMAIN].get("noaa_goes_rrqpe_provider"),
        }
        per_engine = {
            "rainviewer": hass.data[DOMAIN].get("rainviewer_providers_by_engine", {}),
            "opera": hass.data[DOMAIN].get("opera_providers_by_engine", {}),
        }
        for region in storm_manager.get_all_engines():
            source = (decisions.get(region.engine_id) or {}).get("source")
            provider = per_engine.get(source, {}).get(region.engine_id)
            if provider is None:
                provider = shared.get(source)
            overlay = getattr(provider, "overlay", None) if provider else None
            if overlay is None:
                overlay = provider_lifecycle.overlay(source)
            if overlay:
                overlays[region.engine_id] = overlay
        hass.data[DOMAIN]["radar_overlays_by_engine"] = overlays
        hass.bus.async_fire(f"{DOMAIN}_provider_lifecycle_update")
        hass.bus.async_fire(f"{DOMAIN}_radar_source_update", {"engines": decisions})

    async def _poll_netatmo(now=None):
        _sync_region_netatmo_providers()
        providers = hass.data[DOMAIN].get("netatmo_providers_by_engine", {})
        if not providers:
            return
        engine_ids = tuple(providers)
        fetched = await asyncio.gather(
            *(providers[engine_id].fetch_observations() for engine_id in engine_ids),
            return_exceptions=True,
        )
        observations_by_engine = {}
        trends_by_engine = {}
        all_observations = {}
        all_raining = {}
        regions = {region.engine_id: region for region in storm_manager.get_all_engines()}
        for engine_id, result in zip(engine_ids, fetched):
            if isinstance(result, Exception):
                _LOGGER.error(
                    "Netatmo-poll voor %s mislukt: %s", engine_id, result
                )
                result = []
            obs = list(result)
            observations_by_engine[engine_id] = obs
            tracker = _pressure_tracker_for_region(regions[engine_id])
            trends_by_engine[engine_id] = tracker.update(obs)
            for item in obs:
                key = str(item.station_id or f"{item.lat:.5f},{item.lon:.5f}")
                all_observations[key] = item
                if (item.rain_mm or 0) >= 0.1:
                    all_raining[key] = item
            region = regions[engine_id]
            log_netatmo(
                hass, obs, region.center_lat, region.center_lon,
            )
        obs = list(all_observations.values())
        raining = list(all_raining.values())
        hass.data[DOMAIN]["netatmo_observations_by_engine"] = observations_by_engine
        hass.data[DOMAIN]["netatmo_pressure_trends_by_engine"] = trends_by_engine
        hass.data[DOMAIN]["last_netatmo_observations"] = obs
        hass.data[DOMAIN]["netatmo_rain_count"] = len(raining)
        hass.data[DOMAIN]["netatmo_station_count"] = len(obs)
        home_region = storm_manager.get_engine_for_target("zone.home")
        hass.data[DOMAIN]["netatmo_pressure_trend"] = (
            trends_by_engine.get(home_region.engine_id, {}) if home_region else {}
        )
        await pressure_store.async_save({
            "engines": {
                region.storage_key: pressure_trackers_by_engine[
                    region.engine_id
                ].to_snapshot()
                for region in storm_manager.get_all_engines()
                if region.engine_id in pressure_trackers_by_engine
            }
        })
        hass.bus.async_fire(f"{DOMAIN}_netatmo_update", {
            "stations": len(obs),
            "raining": len(raining),
            "engines": {
                engine_id: len(items)
                for engine_id, items in observations_by_engine.items()
            },
        })
        # Alleen natte stations naar OFE
        for o in raining:
            storm_manager.route_observation(o)

    async def _poll_open_meteo(now=None):
        _sync_region_open_meteo_providers()
        providers = hass.data[DOMAIN].get("open_meteo_providers_by_engine", {})
        if not providers:
            return
        engine_ids = tuple(providers)
        results = await asyncio.gather(
            *(providers[engine_id].fetch() for engine_id in engine_ids),
            return_exceptions=True,
        )
        regions = {region.engine_id: region for region in storm_manager.get_all_engines()}
        result_map = hass.data[DOMAIN]["open_meteo_results_by_engine"]
        processed = hass.data[DOMAIN]["open_meteo_processed_sequences_by_engine"]
        from .engine.observation import Observation, ObservationType
        import time as _t
        now_ts = _t.time()
        for engine_id, result in zip(engine_ids, results):
            if isinstance(result, Exception):
                _LOGGER.error("Open-Meteo-poll voor %s mislukt: %s", engine_id, result)
                continue
            result_map[engine_id] = result
            region = regions.get(engine_id)
            if region is None:
                continue
            log_open_meteo(hass, result, region.center_lat, region.center_lon)
            sequence = result.get("fetch_sequence", 0)
            if not sequence or sequence == processed.get(engine_id):
                continue
            processed[engine_id] = sequence
            for loc in result.get("wet_locations_now", []):
                observation = Observation(
                    obs_type=ObservationType.RAIN,
                    lat=loc["lat"],
                    lon=loc["lon"],
                    timestamp=now_ts,
                    rain_mm=loc["mm"],
                    source="open_meteo",
                )
                storm_manager.route_observation_to_engine(engine_id, observation)
        home_region = storm_manager.get_engine_for_target("zone.home")
        home_result = result_map.get(home_region.engine_id, {}) if home_region else {}
        hass.data[DOMAIN]["open_meteo_result"] = home_result
        hass.data[DOMAIN]["open_meteo"] = (
            providers.get(home_region.engine_id) if home_region else None
        )
        hass.bus.async_fire(
            f"{DOMAIN}_open_meteo_update",
            {"engines": sorted(result_map), **home_result},
        )

    async def _poll_eumetsat_li(now=None):
        """Gebruik satellietflashes uitsluitend zolang Blitzortung offline is."""
        if eumetsat is None:
            return
        source_mode = hass.data[DOMAIN].get("lightning_source_mode", "auto")
        if not _use_satellite_lightning(blitz.connected, source_mode):
            hass.data[DOMAIN]["lightning_source"] = "blitzortung"
            return
        try:
            observations = await asyncio.wait_for(
                eumetsat.fetch_observations(), timeout=30
            )
        except Exception:
            _LOGGER.exception("EUMETSAT LI fallback ophalen mislukt")
            hass.data[DOMAIN]["eumetsat_li_status"] = "error"
            hass.data[DOMAIN]["eumetsat_poll"] = {
                "timestamp": time.time(), "fetched": 0, "accepted": 0,
                "error": "fetch_failed",
            }
            hass.bus.async_fire(f"{DOMAIN}_lightning_status_update")
            return
        hass.data[DOMAIN]["eumetsat_li_status"] = "active"
        hass.data[DOMAIN]["lightning_source"] = "eumetsat_li"
        accepted = 0
        for observation in observations:
            if preferred_source_for_longitude(observation.lon) != observation.source:
                continue
            if storm_manager.route_observation(observation) == 0:
                continue
            accepted += 1
            hass.data[DOMAIN]["last_lightning"] = observation
            hass.data[DOMAIN].setdefault("lightning_count", 0)
            hass.data[DOMAIN]["lightning_count"] += 1
            _record_lightning(observation)
            hass.bus.async_fire(f"{DOMAIN}_lightning_update", {
                "lat": observation.lat,
                "lon": observation.lon,
                "timestamp": observation.timestamp,
                "source": observation.source,
            })
        if observations:
            _LOGGER.info(
                "EUMETSAT LI fallback: %d/%d flashes binnen actieve regio's",
                accepted, len(observations),
            )
        hass.data[DOMAIN]["eumetsat_poll"] = {
            "timestamp": time.time(), "fetched": len(observations),
            "accepted": accepted, "error": None,
        }
        hass.bus.async_fire(f"{DOMAIN}_lightning_status_update")

    async def _poll_goes_glm(now=None):
        """Gebruik NOAA GLM voor Amerika en de Pacific bij Blitz-uitval."""
        source_mode = hass.data[DOMAIN].get("lightning_source_mode", "auto")
        if not _use_satellite_lightning(blitz.connected, source_mode):
            hass.data[DOMAIN]["goes18_glm_status"] = "standby"
            hass.data[DOMAIN]["goes19_glm_status"] = "standby"
            return
        try:
            observations = await asyncio.wait_for(
                goes_glm.fetch_observations(
                    satellites_for_regions(_blitz_regions())
                ),
                timeout=30,
            )
        except Exception as exc:
            _LOGGER.error("NOAA GOES GLM fallback ophalen mislukt: %s", exc)
            hass.data[DOMAIN]["goes_poll"] = {
                "timestamp": time.time(),
                "fetched": 0,
                "accepted": 0,
                "error": "timeout" if isinstance(exc, asyncio.TimeoutError)
                else type(exc).__name__,
            }
            hass.bus.async_fire(f"{DOMAIN}_lightning_status_update")
            return
        hass.data[DOMAIN]["goes18_glm_status"] = goes_glm.status[18]
        hass.data[DOMAIN]["goes19_glm_status"] = goes_glm.status[19]
        accepted = 0
        for observation in observations:
            if preferred_source_for_longitude(observation.lon) != observation.source:
                continue
            if storm_manager.route_observation(observation) == 0:
                continue
            accepted += 1
            hass.data[DOMAIN]["lightning_source"] = observation.source
            hass.data[DOMAIN]["last_lightning"] = observation
            hass.data[DOMAIN].setdefault("lightning_count", 0)
            hass.data[DOMAIN]["lightning_count"] += 1
            _record_lightning(observation)
            hass.bus.async_fire(f"{DOMAIN}_lightning_update", {
                "lat": observation.lat,
                "lon": observation.lon,
                "timestamp": observation.timestamp,
                "source": observation.source,
            })
        if observations:
            _LOGGER.info(
                "NOAA GOES GLM fallback: %d/%d flashes binnen actieve regio's",
                accepted, len(observations),
            )
        failed = [
            f"goes{satellite}" for satellite in (18, 19)
            if goes_glm.status[satellite] == "error"
        ]
        hass.data[DOMAIN]["goes_poll"] = {
            "timestamp": time.time(), "fetched": len(observations),
            "accepted": accepted, "error": ",".join(failed) or None,
        }
        hass.bus.async_fire(f"{DOMAIN}_lightning_status_update")

    async def _set_lightning_source_mode(source_mode: str) -> None:
        """Schakel de bliksemprovider live om na een Options Flow-update."""
        if source_mode not in {"auto", "satellite_test"}:
            _LOGGER.warning("Onbekende bliksembronmodus genegeerd: %s", source_mode)
            return
        previous_mode = hass.data[DOMAIN].get("lightning_source_mode", "auto")
        hass.data[DOMAIN]["lightning_source_mode"] = source_mode
        if source_mode == "satellite_test":
            blitz.stop()
            await _poll_eumetsat_li()
            await _poll_goes_glm()
        else:
            hass.data[DOMAIN]["eumetsat_li_status"] = "standby"
            hass.data[DOMAIN]["goes18_glm_status"] = "standby"
            hass.data[DOMAIN]["goes19_glm_status"] = "standby"
            blitz.update_regions(_blitz_regions())
            blitz.start()
            hass.data[DOMAIN]["lightning_source"] = "blitzortung"
        hass.bus.async_fire(f"{DOMAIN}_lightning_status_update")
        _LOGGER.info(
            "Bliksembronmodus live gewijzigd van %s naar %s",
            previous_mode,
            source_mode,
        )

    hass.data[DOMAIN]["set_lightning_source_mode"] = _set_lightning_source_mode

    async def _bounded_provider_stage(name: str, awaitable, timeout_s: float):
        """Begrens een volledige providerfase zodat de cyclus kan herstellen."""
        try:
            return await asyncio.wait_for(awaitable, timeout=timeout_s)
        except asyncio.TimeoutError:
            _LOGGER.error(
                "Providerfase %s overschreed harde timeout van %.0f seconden",
                name,
                timeout_s,
            )
            hass.data[DOMAIN].setdefault("provider_stage_timeouts", {})[name] = time.time()
            return None

    async def _flush_calibration_data() -> None:
        """Schrijf de verzamelde kalibratiedata buiten de HA-eventloop weg."""
        observer = hass.data[DOMAIN]["radar_calibration_observer"]
        batch = observer.drain_collection_batch()
        if not batch["frames"] and not batch["comparisons"]:
            return
        try:
            result = await hass.async_add_executor_job(
                calibration_store.write_batch, batch
            )
        except Exception as exc:
            observer.restore_collection_batch(batch)
            hass.data[DOMAIN]["radar_calibration_storage"] = {
                "status": "error", "error": type(exc).__name__,
                "path": str(calibration_store.path),
            }
            _LOGGER.exception("Kalibratiedatabase schrijven mislukt")
            return
        hass.data[DOMAIN]["radar_calibration_storage"] = {
            "status": "ready", **result,
        }
        diagnostics = observer.diagnostics()
        diagnostics["storage"] = hass.data[DOMAIN]["radar_calibration_storage"]
        hass.data[DOMAIN]["radar_calibration"] = diagnostics
        hass.bus.async_fire(
            f"{DOMAIN}_calibration_update",
            {"samples": diagnostics["samples"]},
        )

    async def _poll_all(now=None):
        """Voer een volledige providercyclus in vaste, racevrije volgorde uit."""
        lock = hass.data[DOMAIN].setdefault("provider_cycle_lock", asyncio.Lock())
        if lock.locked():
            _LOGGER.debug("Providercyclus overgeslagen: vorige cyclus loopt nog")
            return
        async with lock:
            _sync_region_radar_providers()
            _sync_region_open_meteo_providers()
            await _bounded_provider_stage(
                "national", _poll_national_providers(), 25
            )
            await _bounded_provider_stage(
                "radar_comparison", _poll_radar_comparison(), 60
            )
            await _bounded_provider_stage("radar", _poll_radar(), 120)
            await _bounded_provider_stage(
                "ground_validation",
                asyncio.gather(_poll_netatmo(), _poll_open_meteo()),
                30,
            )
            await _bounded_provider_stage(
                "calibration_storage", _flush_calibration_data(), 15
            )

    # ── Polling intervallen ───────────────────────────────────────────────
    from homeassistant.helpers.event import async_track_time_interval, async_track_state_change_event
    from datetime import timedelta

    hass.data[DOMAIN]["unsubscribers"].extend([
        async_track_time_interval(hass, _poll_all, timedelta(minutes=5)),
        async_track_time_interval(hass, _poll_eumetsat_li, timedelta(minutes=2)),
        async_track_time_interval(hass, _poll_goes_glm, timedelta(minutes=1)),
    ])

    # ── Fictieve tracker locatie volgen ───────────────────────────────────
    async def _update_fictieve_location(now=None):
        nonlocal active_region, storm_engine, ofe
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
        previous_region = active_region
        region = storm_manager.assign_target(fictieve_entity, lat, lon)
        if region is not previous_region:
            await mcs_store.async_save_engine(
                previous_region.storage_key, previous_region.storm_engine
            )
            _activate_region(region)
        else:
            active_region.target_locations[fictieve_entity] = (lat, lon)
        primary_target = hass.data[DOMAIN]["targets"]["home"]
        primary_target.update({
            "latitude": lat,
            "longitude": lon,
            "available": True,
            "radar_covered": True,
            "region_engine_id": region.engine_id,
        })
        blitz.update_regions(_blitz_regions())
        removed = storm_engine.retain_within(
            active_region.center_lat, active_region.center_lon, radar_radius
        )
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

    async def _refresh_fictieve_location() -> None:
        await _update_fictieve_location()
        await _poll_all()

    @callback
    def _on_fictieve_state_change(event):
        hass.async_create_task(_refresh_fictieve_location())

    hass.data[DOMAIN]["unsubscribers"].append(
        async_track_state_change_event(hass, [fictieve_entity], _on_fictieve_state_change)
    )

    secondary_specs = [spec for spec in target_specs if not spec.primary]

    async def _update_secondary_target(spec, *, initial: bool = False):
        state = hass.states.get(spec.entity_id)
        coordinates = coordinates_from_state(state, spec)
        target_data = hass.data[DOMAIN]["targets"][spec.target_id]
        if coordinates is None:
            target_data["available"] = False
            hass.bus.async_fire(
                f"{DOMAIN}_targets_updated", {"targets": [spec.target_id]}
            )
            return

        lat, lon = coordinates
        attributes = state.attributes if state is not None else {}
        preferred_place = attributes.get("place") or attributes.get("stad")
        resolved_location = resolve_location(
            lat, lon, location_places, preferred_place=preferred_place
        )
        target_data.update({
            "location_place": resolved_location.place,
            "location_address": attributes.get("address"),
            "country_code": resolved_location.country_code,
            "location_accuracy_km": resolved_location.distance_km,
        })
        old_lat = target_data.get("latitude")
        old_lon = target_data.get("longitude")
        movement = (
            _distance_km(old_lat, old_lon, lat, lon)
            if old_lat is not None and old_lon is not None else float("inf")
        )
        current = storm_manager.get_engine_for_target(spec.entity_id)
        if movement < 1.0 and current is not None and not initial:
            hass.bus.async_fire(
                f"{DOMAIN}_targets_updated", {"targets": [spec.target_id]}
            )
            return
        if current is not None and movement >= 1.0:
            await mcs_store.async_save_engine(current.storage_key, current.storm_engine)
        region = storm_manager.assign_target(spec.entity_id, lat, lon)
        _prepare_region(region)
        target_data.update({
            "latitude": lat,
            "longitude": lon,
            "available": True,
            "radar_covered": _distance_km(
                hass.data[DOMAIN].get("fictieve_lat", home_lat),
                hass.data[DOMAIN].get("fictieve_lon", home_lon),
                lat,
                lon,
            ) <= radar_radius,
            "region_engine_id": region.engine_id,
        })
        hass.data[DOMAIN]["region_engines"] = storm_manager.get_all_engines()
        _sync_region_radar_providers()
        _sync_region_netatmo_providers()
        _sync_region_open_meteo_providers()
        blitz.update_regions(_blitz_regions())
        hass.bus.async_fire(
            f"{DOMAIN}_targets_updated", {"targets": [spec.target_id]}
        )
        _LOGGER.info(
            "Secundair target %s op %.4f,%.4f gekoppeld aan %s",
            spec.target_id, lat, lon, region.engine_id,
        )
        # Een verre verplaatsing creëert een nieuwe RegionEngine. Start meteen
        # een beschermde radarcyclus; de lock voorkomt dubbele gelijktijdige polls.
        if not initial:
            hass.async_create_task(_poll_all())

    @callback
    def _on_secondary_target_change(event):
        entity_id = event.data.get("entity_id")
        spec = next(
            (item for item in secondary_specs if item.entity_id == entity_id), None
        )
        if spec is not None:
            hass.async_create_task(_update_secondary_target(spec))

    if secondary_specs:
        hass.data[DOMAIN]["unsubscribers"].append(
            async_track_state_change_event(
                hass,
                [spec.entity_id for spec in secondary_specs],
                _on_secondary_target_change,
            )
        )

    async def _do_initial_setup(event=None):
        await _update_fictieve_location()
        for spec in secondary_specs:
            await _update_secondary_target(spec, initial=True)
        if not hass.data[DOMAIN].get("providers_initialized"):
            await _init_location_providers(home_lat, home_lon)
            hass.data[DOMAIN]["providers_initialized"] = True
        await _poll_all()

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
        hass.async_create_task(provider_lifecycle.async_stop_all())
        hass.async_create_task(_flush_calibration_data())
        for region in storm_manager.get_all_engines():
            hass.async_create_task(
                mcs_store.async_save_engine(region.storage_key, region.storm_engine)
            )
        for unsubscribe in hass.data[DOMAIN].get("unsubscribers", []):
            unsubscribe()
        hass.data[DOMAIN]["unsubscribers"] = []

    hass.bus.async_listen_once(EVENT_HOMEASSISTANT_STOP, _on_ha_stop)

    # ── Sensor platform laden ─────────────────────────────────────────────
    from homeassistant.helpers import discovery
    await discovery.async_load_platform(hass, "sensor", DOMAIN, {}, config)

    if lightning_source_mode == "satellite_test":
        # Diagnostische modus moet meteen bewijs leveren en niet eerst wachten
        # op de eerste intervalcallback.
        hass.async_create_task(_poll_eumetsat_li())
        hass.async_create_task(_poll_goes_glm())

    _LOGGER.info("Storm Tracker V3 gestart met radarroutering per RegionEngine")
    return True
