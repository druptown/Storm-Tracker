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
import hashlib
import hmac
import json
import logging
import math
import time
from dataclasses import asdict
from datetime import datetime, timezone
from functools import partial
from pathlib import Path

from homeassistant.core import HomeAssistant, callback, CoreState
from homeassistant.const import EVENT_HOMEASSISTANT_STARTED, EVENT_HOMEASSISTANT_STOP
from homeassistant.helpers.typing import ConfigType
import homeassistant.helpers.config_validation as cv
import voluptuous as vol

from .const import (
    CONF_NOTIFY_SERVICE,
    CONF_RUNTIME_MODE,
    CONF_WARNING_NOTIFICATIONS_ENABLED,
    DATA_ENTRY_RUNTIMES,
    DOMAIN,
    RUNTIME_MODE_LEGACY,
    RUNTIME_MODE_WORKER_ONLY,
    WARNING_SENT_EVENT,
)

_LOGGER = logging.getLogger(__name__)


def _tracking_canonical(value) -> bytes:
    """Exacte, platformonafhankelijke JSON-representatie voor protocolhashes."""
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _tracking_digest(value) -> str:
    return hashlib.sha256(_tracking_canonical(value)).hexdigest()


def _normalized_tracking_state(snapshots: list[dict]) -> list[dict]:
    return sorted(
        snapshots,
        key=lambda item: str(item.get("storm_id", "")),
    )


def _tracking_input_digest(observations: list[dict]) -> str:
    return _tracking_digest({
        "schema": "stv3-tracking-observations-v5",
        "observations": observations,
    })


def _tracking_state_digest(snapshots: list[dict]) -> str:
    return _tracking_digest({
        "schema": "stv3-tracking-state-v5",
        "snapshots": _normalized_tracking_state(snapshots),
    })


def _matching_sha256(actual, expected: str) -> bool:
    return (
        isinstance(actual, str)
        and len(actual) == 64
        and all(character in "0123456789abcdef" for character in actual)
        and hmac.compare_digest(actual, expected)
    )


_TRACKING_V5_RADAR_PROVENANCE_KEYS = frozenset({
    "timestamp",
    "source",
    "received_at",
    "parser_version",
    "artifact_id",
    "raw_artifact_receipt_id",
    "upstream_record_id",
    "contribution_id",
    "provenance_grade",
    "provenance",
})


def _valid_tracking_v5_radar_record(value) -> bool:
    """Weiger een als v5 vermomde snapshot die tijd/provenance laat vallen."""
    if (
        not isinstance(value, dict)
        or not _TRACKING_V5_RADAR_PROVENANCE_KEYS.issubset(value)
    ):
        return False
    timestamp = value.get("timestamp")
    received_at = value.get("received_at")
    if (
        isinstance(timestamp, bool)
        or not isinstance(timestamp, (int, float))
        or not math.isfinite(float(timestamp))
        or (
            received_at is not None
            and (
                isinstance(received_at, bool)
                or not isinstance(received_at, (int, float))
                or not math.isfinite(float(received_at))
            )
        )
    ):
        return False
    if any(
        not isinstance(value.get(key), str) or not value[key]
        for key in ("source", "parser_version", "provenance_grade")
    ):
        return False
    if any(
        value.get(key) is not None
        and (
            not isinstance(value[key], str)
            or not value[key]
        )
        for key in (
            "artifact_id",
            "raw_artifact_receipt_id",
            "upstream_record_id",
            "contribution_id",
        )
    ):
        return False
    return value.get("provenance") is None or isinstance(
        value["provenance"], dict
    )


def _valid_tracking_v5_snapshot(value) -> bool:
    if not isinstance(value, dict):
        return False
    cells = value.get("radar_cells")
    frames = value.get("frames")
    return (
        isinstance(cells, list)
        and isinstance(frames, list)
        and all(_valid_tracking_v5_radar_record(item) for item in cells)
        and all(_valid_tracking_v5_radar_record(item) for item in frames)
    )

PLATFORMS = ["sensor"]
HA_START_WAIT_TIMEOUT_S = 180.0
SERVICE_RETRY_QUARANTINED_OBSERVATIONS = (
    "retry_quarantined_observations"
)
RETRY_QUARANTINED_SERVICE_SCHEMA = vol.Schema({
    vol.Optional("region_engine"): cv.string,
})
VIRTUAL_PROBE_WORKER_MAX_RESTORED_ACTIVE_PROBES = 64
VIRTUAL_PROBE_WORKER_MAX_OBSERVATIONS = 2_000
CALIBRATION_CORPUS_MAX_CAPTURES_PER_JOB = 128


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


def _trim_virtual_probe_active_probes(
    probes: list[dict],
    observed_at: float,
    max_probes: int,
) -> list[dict]:
    if len(probes) <= max_probes:
        return probes

    def _distance_to_now(probe: dict) -> float:
        expected_at = probe.get("expected_at")
        if not isinstance(expected_at, (int, float)):
            return float("inf")
        return abs(float(expected_at) - float(observed_at))

    return sorted(probes, key=_distance_to_now)[:max_probes]


def _trim_virtual_probe_observations(
    observations_by_region: dict[str, list[dict]],
    max_observations: int,
) -> dict[str, list[dict]]:
    total_observations = sum(len(items) for items in observations_by_region.values())
    if total_observations <= max_observations:
        return observations_by_region

    ranked = []
    for region_id, observations in observations_by_region.items():
        for observation in observations:
            observed_at = observation.get("timestamp")
            if isinstance(observed_at, (int, float)):
                timestamp = float(observed_at)
            else:
                timestamp = float("-inf")
            ranked.append((timestamp, str(region_id), observation))

    ranked.sort(key=lambda item: item[0], reverse=True)
    allowed: dict[str, list[dict]] = {}
    for _, region_id, observation in ranked[:max_observations]:
        allowed.setdefault(region_id, []).append(observation)
    return allowed

TARGET_SCHEMA = vol.Schema({
    vol.Required("id"): cv.string,
    vol.Optional("name"): cv.string,
    vol.Required("location_entity"): cv.entity_id,
    vol.Optional("latitude"): cv.latitude,
    vol.Optional("longitude"): cv.longitude,
})


CONFIG_SCHEMA = cv.config_entry_only_config_schema(DOMAIN)
CONFIG_ENTRY_VERSION = 3
_LIGHTNING_SOURCE_MODES = frozenset({"auto", "satellite_test"})
_LEGACY_HTTP_REGISTRATION = f"{DOMAIN}_legacy_http_registered"
_LEGACY_ENTRY_FIELDS = frozenset({
    "analysis_mode",
    "engine_sharing_distance_km",
    "eumetsat_consumer_key",
    "eumetsat_consumer_secret",
    "hsaf_password",
    "hsaf_username",
    "knmi_api_key",
    "knmi_wms_api_key",
    "lightning_source_mode",
    "meteofrance_api_token",
    "meteofrance_application_id",
    "netatmo_client_id",
    "netatmo_client_secret",
    "netatmo_radius_km",
    "netatmo_refresh_token",
    CONF_NOTIFY_SERVICE,
    "open_meteo_enabled",
    "persons",
    "radar_radius_km",
    "test_tracker_entity",
    CONF_WARNING_NOTIFICATIONS_ENABLED,
})


class LegacyEntryRuntimeError(RuntimeError):
    """Raised when an explicit legacy entry cannot own a complete runtime."""


def _explicit_entry_mode(entry) -> str | None:
    """Return only a consistent, explicitly persisted schema-v3 mode."""

    data_mode = entry.data.get(CONF_RUNTIME_MODE)
    options_mode = entry.options.get(CONF_RUNTIME_MODE)
    if data_mode not in (RUNTIME_MODE_LEGACY, RUNTIME_MODE_WORKER_ONLY):
        return None
    if options_mode not in (None, data_mode):
        return None
    return data_mode


def _legacy_entry_values(entry) -> dict:
    """Merge only legacy-owned entry fields, with options taking precedence."""

    values = {
        key: entry.data[key]
        for key in _LEGACY_ENTRY_FIELDS
        if key in entry.data
    }
    values.update({
        key: entry.options[key]
        for key in _LEGACY_ENTRY_FIELDS
        if key in entry.options
    })
    return values


def _legacy_runtime_config(hass: HomeAssistant, entry) -> dict:
    """Build the closed legacy runtime input from one explicit owner entry."""

    raw = _legacy_entry_values(entry)
    persons = raw.get("persons", [])
    if not isinstance(persons, (list, tuple)):
        persons = []
    targets = []
    for entity_id in persons:
        if not isinstance(entity_id, str) or not entity_id:
            continue
        state = hass.states.get(entity_id)
        friendly_name = (
            state.attributes.get("friendly_name")
            if state is not None
            else None
        )
        target_id = entity_id.split(".", 1)[-1]
        targets.append({
            "id": target_id,
            "name": friendly_name or target_id.replace("_", " ").title(),
            "location_entity": entity_id,
        })

    conf = {
        "home_lat": hass.config.latitude,
        "home_lon": hass.config.longitude,
        "targets": targets,
        "radar_radius_km": raw.get("radar_radius_km", 300.0),
        "engine_sharing_distance_km": raw.get(
            "engine_sharing_distance_km", 150.0
        ),
        "eumetsat_consumer_key": raw.get("eumetsat_consumer_key"),
        "eumetsat_consumer_secret": raw.get("eumetsat_consumer_secret"),
        "meteofrance_api_token": raw.get("meteofrance_api_token"),
        "meteofrance_application_id": raw.get(
            "meteofrance_application_id"
        ),
        "knmi_api_key": raw.get("knmi_api_key"),
        "knmi_wms_api_key": raw.get("knmi_wms_api_key"),
        "hsaf_username": raw.get("hsaf_username"),
        "hsaf_password": raw.get("hsaf_password"),
        "netatmo_client_id": raw.get("netatmo_client_id"),
        "netatmo_client_secret": raw.get("netatmo_client_secret"),
        "netatmo_refresh_token": raw.get("netatmo_refresh_token"),
        "netatmo_radius_km": raw.get("netatmo_radius_km", 175.0),
        "lightning_source_mode": raw.get("lightning_source_mode", "auto"),
        "open_meteo_enabled": raw.get("open_meteo_enabled", False),
        "analysis_mode": raw.get("analysis_mode", "full_send"),
        CONF_WARNING_NOTIFICATIONS_ENABLED: raw.get(
            CONF_WARNING_NOTIFICATIONS_ENABLED, False
        ),
        CONF_NOTIFY_SERVICE: raw.get(CONF_NOTIFY_SERVICE),
    }
    test_tracker = raw.get("test_tracker_entity")
    if isinstance(test_tracker, str) and test_tracker:
        conf["fictieve_tracker_entity"] = test_tracker
    return conf


async def _async_options_updated(hass: HomeAssistant, updated_entry) -> None:
    """Pas een geldige bliksembronoptie toe op de reeds actieve runtime."""
    record = (
        hass.data.get(DOMAIN, {})
        .get(DATA_ENTRY_RUNTIMES, {})
        .get(updated_entry.entry_id)
    )
    active_mode = record.get("mode") if isinstance(record, dict) else None
    selected_mode = _explicit_entry_mode(updated_entry)
    if active_mode != selected_mode:
        # A reconfigure update is observed by the currently active owner.  The
        # listener performs the one reload; the 2026.6+ config-flow helper must
        # therefore only update-and-abort, not schedule a second reload.
        await hass.config_entries.async_reload(updated_entry.entry_id)
        return
    updated_config = {
        **updated_entry.data,
        **updated_entry.options,
    }
    source_mode = updated_config.get("lightning_source_mode", "auto")
    if (
        not isinstance(source_mode, str)
        or source_mode not in _LIGHTNING_SOURCE_MODES
    ):
        _LOGGER.warning(
            "Ongeldige bliksembronmodus uit Options Flow genegeerd: %r",
            source_mode,
        )
        return
    try:
        setter = hass.data[DOMAIN]["set_lightning_source_mode"]
    except (KeyError, TypeError):
        _LOGGER.debug(
            "Bliksembronoptie %s wacht op een actieve runtime-setter",
            source_mode,
        )
        return
    if not callable(setter):
        _LOGGER.warning(
            "Bliksembronoptie %s geweigerd: runtime-setter is niet callable",
            source_mode,
        )
        return
    await setter(source_mode)


async def _async_reload_legacy_entry(
    hass: HomeAssistant, updated_entry
) -> None:
    """Apply legacy options through one complete unload/setup transaction."""

    await hass.config_entries.async_reload(updated_entry.entry_id)


async def _async_partial_legacy_cleanup(hass: HomeAssistant) -> None:
    """Best-effort rollback for a legacy setup that failed before shutdown."""

    data = hass.data.get(DOMAIN, {})
    first_error = None
    unsubscribers = list(data.get("unsubscribers", ()))
    data["unsubscribers"] = []
    for unsubscribe in reversed(unsubscribers):
        if not callable(unsubscribe):
            continue
        try:
            unsubscribe()
        except BaseException as err:
            if first_error is None:
                first_error = err

    lifecycle = data.get("provider_lifecycle")
    stop_all = getattr(lifecycle, "async_stop_all", None)
    if callable(stop_all):
        try:
            await stop_all()
        except BaseException as err:
            if first_error is None:
                first_error = err

    radar_worker = data.get("radar_frame_worker")
    close_worker = getattr(radar_worker, "close", None)
    if callable(close_worker):
        try:
            close_worker()
        except BaseException as err:
            if first_error is None:
                first_error = err

    if hass.services.has_service(
        DOMAIN, SERVICE_RETRY_QUARANTINED_OBSERVATIONS
    ):
        hass.services.async_remove(
            DOMAIN, SERVICE_RETRY_QUARANTINED_OBSERVATIONS
        )
    if first_error is not None:
        raise first_error


class _LegacyEntryRuntime:
    """Own one explicit in-process provider/OFE runtime transactionally."""

    def __init__(self, hass: HomeAssistant, entry) -> None:
        self.hass = hass
        self.entry = entry
        self._options_unsubscribe = None
        self._platform_forward_attempted = False
        self._runtime_started = False
        self._shutdown_called = False
        self._unloaded = False

    def _record(self) -> dict:
        return {
            "mode": RUNTIME_MODE_LEGACY,
            "runtime_mode": RUNTIME_MODE_LEGACY,
            "entry_id": self.entry.entry_id,
            "manager": self,
            "state": "starting",
        }

    def _claim(self) -> None:
        domain_data = self.hass.data.setdefault(DOMAIN, {})
        if domain_data:
            raise LegacyEntryRuntimeError(
                "een andere Storm Tracker-runtime bezit de globale runtime"
            )
        record = self._record()
        domain_data[DATA_ENTRY_RUNTIMES] = {
            self.entry.entry_id: record,
        }
        domain_data["runtime_mode"] = RUNTIME_MODE_LEGACY
        domain_data["runtime_diagnostics"] = {
            "runtime_mode": RUNTIME_MODE_LEGACY,
            "selection_source": "data",
            "legacy_compute_enabled": True,
            "owner_entry_id": self.entry.entry_id,
            "state": "starting",
        }

    def _owned_record(self) -> dict | None:
        record = (
            self.hass.data.get(DOMAIN, {})
            .get(DATA_ENTRY_RUNTIMES, {})
            .get(self.entry.entry_id)
        )
        if (
            not isinstance(record, dict)
            or record.get("mode") != RUNTIME_MODE_LEGACY
            or record.get("manager") is not self
        ):
            return None
        return record

    async def async_setup(self) -> bool:
        self._claim()
        try:
            conf = _legacy_runtime_config(self.hass, self.entry)
            runtime_config = {DOMAIN: dict(conf)}
            self._runtime_started = await _async_setup_runtime(
                self.hass, conf, runtime_config
            )
            if not self._runtime_started:
                await self.async_unload()
                return False

            unsubscribe = self.entry.add_update_listener(
                _async_reload_legacy_entry
            )
            if not callable(unsubscribe):
                raise LegacyEntryRuntimeError(
                    "legacy-optionslistener gaf geen cleanupcallback terug"
                )
            self._options_unsubscribe = unsubscribe
            self._platform_forward_attempted = True
            await self.hass.config_entries.async_forward_entry_setups(
                self.entry, PLATFORMS
            )
            record = self._owned_record()
            if record is None:
                raise LegacyEntryRuntimeError(
                    "legacy-runtime verloor zijn exacte owner-record"
                )
            record["state"] = "active"
            diagnostics = self.hass.data[DOMAIN]["runtime_diagnostics"]
            diagnostics["state"] = "active"
            return True
        except BaseException:
            if self._platform_forward_attempted:
                try:
                    await self.hass.config_entries.async_unload_platforms(
                        self.entry, PLATFORMS
                    )
                except BaseException as err:
                    _LOGGER.error(
                        "Legacy-platformrollback faalde (%s)",
                        type(err).__name__,
                    )
            try:
                await self.async_unload()
            except BaseException as err:
                _LOGGER.error(
                    "Legacy-runtimerollback faalde (%s)",
                    type(err).__name__,
                )
            raise

    async def async_unload(self) -> None:
        if self._unloaded:
            return
        if self._owned_record() is None:
            return
        self._unloaded = True
        first_error = None
        unsubscribe = self._options_unsubscribe
        self._options_unsubscribe = None
        if callable(unsubscribe):
            try:
                unsubscribe()
            except BaseException as err:
                first_error = err

        domain_data = self.hass.data.get(DOMAIN, {})
        remove_shutdown_job = domain_data.pop("remove_shutdown_job", None)
        if callable(remove_shutdown_job):
            try:
                remove_shutdown_job()
            except BaseException as err:
                if first_error is None:
                    first_error = err

        shutdown = domain_data.get("async_shutdown_runtime")
        if callable(shutdown) and not self._shutdown_called:
            self._shutdown_called = True
            try:
                await shutdown()
            except BaseException as err:
                if first_error is None:
                    first_error = err
        else:
            try:
                await _async_partial_legacy_cleanup(self.hass)
            except BaseException as err:
                if first_error is None:
                    first_error = err

        record = self._owned_record()
        if record is not None:
            records = domain_data.get(DATA_ENTRY_RUNTIMES, {})
            records.pop(self.entry.entry_id, None)
            if not records and self.hass.data.get(DOMAIN) is domain_data:
                self.hass.data.pop(DOMAIN, None)
        if first_error is not None:
            raise first_error


async def async_setup(hass: HomeAssistant, config: ConfigType) -> bool:
    """Reject YAML unconditionally; the final runtime is config-entry-only."""
    if DOMAIN not in config:
        return True
    _LOGGER.error(
        "Storm Tracker YAML is niet meer ondersteund: kies expliciet "
        "worker_only of legacy in een configuratie-entry"
    )
    return False


async def _async_setup_legacy_entry(hass: HomeAssistant, entry) -> bool:
    manager = _LegacyEntryRuntime(hass, entry)
    return await manager.async_setup()


async def async_setup_entry(hass: HomeAssistant, entry) -> bool:
    """Start exactly one explicitly selected config-entry runtime."""
    if hass.data.get(DOMAIN):
        _LOGGER.error(
            "Storm Tracker-entry %s geweigerd: de globale runtime is bezet",
            entry.entry_id,
        )
        return False
    mode = _explicit_entry_mode(entry)
    if getattr(entry, "version", CONFIG_ENTRY_VERSION) != CONFIG_ENTRY_VERSION:
        mode = None
    if mode is None:
        _LOGGER.error(
            "Storm Tracker-entry %s mist een consistente expliciete "
            "schema-v%d runtime_mode; er wordt geen fallback gestart",
            entry.entry_id,
            CONFIG_ENTRY_VERSION,
        )
        return False
    if mode == RUNTIME_MODE_LEGACY:
        return await _async_setup_legacy_entry(hass, entry)
    if mode == RUNTIME_MODE_WORKER_ONLY:
        from .worker_only_entry import async_setup_worker_only_entry

        started = await async_setup_worker_only_entry(hass, entry)
        if not started:
            return False
        entry.async_on_unload(entry.add_update_listener(_async_options_updated))
        return True
    return False


async def async_unload_entry(hass: HomeAssistant, entry) -> bool:
    """Unload only the exact recorded owner; never infer from current options."""
    runtime = hass.data.get(DOMAIN, {})
    record = runtime.get(DATA_ENTRY_RUNTIMES, {}).get(entry.entry_id, {})
    mode = record.get("mode") if isinstance(record, dict) else None
    if mode == RUNTIME_MODE_WORKER_ONLY:
        from .worker_only_entry import async_unload_worker_only_entry

        return await async_unload_worker_only_entry(hass, entry)
    if mode != RUNTIME_MODE_LEGACY:
        return False
    manager = record.get("manager")
    if not isinstance(manager, _LegacyEntryRuntime):
        return False
    unloaded = await hass.config_entries.async_unload_platforms(
        entry, PLATFORMS
    )
    if not unloaded:
        return False
    await manager.async_unload()
    return True


async def async_migrate_entry(hass: HomeAssistant, entry) -> bool:
    """Persist an explicit schema-v3 mode without inventing legacy."""
    version = getattr(entry, "version", 1)
    if version == CONFIG_ENTRY_VERSION:
        return _explicit_entry_mode(entry) is not None
    if version not in (1, 2):
        return False

    data_mode = entry.data.get(CONF_RUNTIME_MODE)
    options_mode = entry.options.get(CONF_RUNTIME_MODE)
    valid_modes = (RUNTIME_MODE_LEGACY, RUNTIME_MODE_WORKER_ONLY)
    if data_mode not in (None, *valid_modes):
        return False
    if options_mode not in (None, *valid_modes):
        return False
    if data_mode is not None and options_mode not in (None, data_mode):
        return False
    selected_mode = data_mode or options_mode or RUNTIME_MODE_WORKER_ONLY
    migrated_data = dict(entry.data)
    migrated_options = dict(entry.options)
    migrated_data[CONF_RUNTIME_MODE] = selected_mode
    migrated_options.pop(CONF_RUNTIME_MODE, None)
    hass.config_entries.async_update_entry(
        entry,
        data=migrated_data,
        options=migrated_options,
        version=CONFIG_ENTRY_VERSION,
    )
    _LOGGER.warning(
        "Storm Tracker-entry %s gemigreerd van schema-v%d naar expliciete "
        "runtime_mode=%s",
        entry.entry_id,
        version,
        selected_mode,
    )
    return True


async def _async_setup_runtime(
    hass: HomeAssistant, conf: dict, config: ConfigType
) -> bool:
    """Start the in-process provider/OFE runtime for an explicit legacy entry."""

    # All database, provider, engine, decoder and HTTP imports live strictly
    # inside the legacy branch.  Importing the integration for worker-only mode
    # therefore cannot initialize the expensive in-process architecture.
    from homeassistant.helpers.storage import Store

    from .engine.lightning_backpressure import LightningUpdateCoalescer
    from .engine.mcs_store import McsHistoryStore
    from .engine.pressure_trend import PressureTrendTracker
    from .engine.radar_frame import (
        RadarFrameHistory,
        audit_storm_object_recovery,
    )
    from .engine.region_manager import StormManager, WorkerTrackingRequiredError
    from .engine.targets import build_target_specs, coordinates_from_state
    from .geometry.location_resolver import load_places_json, resolve_location
    from .http import StormTrackerGeoJsonView
    from .providers.blitzortung import BlitzortungProvider
    from .warning_dispatcher import (
        WarningDispatcher,
        build_warning_evaluation,
    )

    hass.data.setdefault(DOMAIN, {})
    hass.data[DOMAIN].setdefault("unsubscribers", [])

    async def _partial_shutdown_runtime() -> None:
        await _async_partial_legacy_cleanup(hass)

    hass.data[DOMAIN]["async_shutdown_runtime"] = (
        _partial_shutdown_runtime
    )
    from .worker_bridge import async_setup_worker_bridge
    await async_setup_worker_bridge(hass)
    from .radar_frame_worker import RadarFrameWorker
    hass.data[DOMAIN]["radar_frame_worker"] = RadarFrameWorker(hass)
    ha_started = asyncio.Event()
    if hass.state == CoreState.running:
        ha_started.set()
    hass.data[DOMAIN]["ha_started_barrier"] = ha_started
    if not hass.data.get(_LEGACY_HTTP_REGISTRATION):
        hass.http.register_view(StormTrackerGeoJsonView(hass))
        hass.data[_LEGACY_HTTP_REGISTRATION] = True
    hass.data[DOMAIN]["geojson_http_registered"] = True

    home_lat        = conf["home_lat"]
    home_lon        = conf["home_lon"]
    fictieve_entity = "zone.home"
    knmi_api_key    = conf.get("knmi_api_key")
    knmi_wms_key    = conf.get("knmi_wms_api_key", knmi_api_key)
    # Netatmo dient uitsluitend als lokale grondverificatie. Een brede
    # 175-km-query verdubbelt de API-belasting per regio en voegt naast
    # de regionale radar geen detectiewaarde toe.
    netatmo_radius = min(
        max(float(conf.get("netatmo_radius_km", 20.0)), 10.0), 20.0
    )
    radar_radius    = conf.get("radar_radius_km", 200.0)
    sharing_distance = conf.get("engine_sharing_distance_km", 150.0)
    lightning_source_mode = conf.get("lightning_source_mode", "auto")
    analysis_mode = conf.get("analysis_mode", "full_send")
    open_meteo_enabled = bool(conf.get("open_meteo_enabled", False))
    warning_notifications_enabled = (
        conf.get(CONF_WARNING_NOTIFICATIONS_ENABLED) is True
    )
    warning_notify_service = conf.get(CONF_NOTIFY_SERVICE)
    hsaf_username = conf.get("hsaf_username")
    hsaf_password = conf.get("hsaf_password")
    hass.data[DOMAIN]["legacy_provider_configuration"] = {
        "radar": {
            "aemet": {"status": "configured", "scope": "coverage_gated"},
            "dpc": {"status": "configured", "scope": "coverage_gated"},
            "dwd": {"status": "configured", "scope": "coverage_gated"},
            "geosphere": {
                "status": "configured", "scope": "coverage_gated"
            },
            "italiameteo": {
                "status": "configured", "scope": "coverage_gated"
            },
            "kmi": {"status": "configured", "scope": "coverage_gated"},
            "knmi": {
                "status": (
                    "configured" if knmi_api_key else "credentials_missing"
                ),
                "scope": "coverage_gated",
            },
            "meteofrance": {
                "status": (
                    "configured"
                    if conf.get("meteofrance_api_token")
                    or conf.get("meteofrance_application_id")
                    else "credentials_missing"
                ),
                "scope": "coverage_gated",
            },
            "meteolux": {
                "status": "configured", "scope": "coverage_gated"
            },
            "met_office": {
                "status": "configured", "scope": "coverage_gated"
            },
            "opera": {"status": "configured", "scope": "coverage_gated"},
            "rainviewer": {
                "status": "configured", "scope": "global_visual"
            },
        },
        "satellite": {
            "hsaf_h40b": {
                "status": (
                    "configured"
                    if hsaf_username and hsaf_password
                    else "credentials_missing"
                ),
                "scope": "coverage_gated",
            },
            "noaa_enterprise_rainrate": {
                "status": "configured", "scope": "coverage_gated"
            },
            "noaa_goes_rrqpe": {
                "status": "configured", "scope": "coverage_gated"
            },
        },
        "lightning": {
            "blitzortung": {"status": "configured", "scope": "global"},
            "eumetsat_li": {
                "status": (
                    "configured"
                    if conf.get("eumetsat_consumer_key")
                    and conf.get("eumetsat_consumer_secret")
                    else "credentials_missing"
                ),
                "scope": "coverage_gated",
            },
            "noaa_goes_glm": {
                "status": "configured", "scope": "coverage_gated"
            },
        },
        "ground": {
            "metar": {"status": "configured", "scope": "regional"},
            "netatmo": {
                "status": (
                    "configured"
                    if conf.get("netatmo_client_id")
                    and conf.get("netatmo_client_secret")
                    and conf.get("netatmo_refresh_token")
                    else "credentials_missing"
                ),
                "scope": "regional",
            },
            "synoptic": {"status": "configured", "scope": "regional"},
        },
        "model": {
            "open_meteo": {
                "status": "configured" if open_meteo_enabled else "disabled",
                "scope": "target",
            },
        },
        "notifications": {
            "warning_dispatch": {
                "status": (
                    "initializing"
                    if warning_notifications_enabled else "disabled"
                ),
                "enabled": warning_notifications_enabled,
                "notify_service_configured": bool(warning_notify_service),
                "official_warning": False,
            },
        },
    }
    hass.data[DOMAIN]["lightning_source_mode"] = lightning_source_mode
    hass.data[DOMAIN]["analysis_mode"] = analysis_mode
    hass.data[DOMAIN]["open_meteo_enabled"] = open_meteo_enabled
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
    hass.data[DOMAIN]["synoptic_observations_by_engine"] = {}
    hass.data[DOMAIN]["ground_observations_by_engine"] = {}
    hass.data[DOMAIN]["synoptic_diagnostics"] = {
        "status": "wachten",
        "station_count": 0,
        "wet_station_count": 0,
        "regions": {},
    }
    from .providers.open_meteo import OpenMeteoProvider
    open_meteo_provider = OpenMeteoProvider(session=http_session, hass=hass)
    hass.data[DOMAIN]["open_meteo"] = open_meteo_provider
    hass.data[DOMAIN]["open_meteo_result"] = (
        open_meteo_provider.last_result
        if open_meteo_enabled
        else {
            "gear": "DISABLED",
            "role": "optional_model_guidance",
            "provider_status": "disabled",
            "enabled": False,
            "target_results": {},
        }
    )
    hass.data[DOMAIN]["open_meteo_results_by_target"] = {}
    hass.data[DOMAIN]["open_meteo_processed_sequence"] = 0

    warning_dispatcher = WarningDispatcher(
        hass,
        enabled=warning_notifications_enabled,
        notify_service=warning_notify_service,
        store=Store(hass, 1, f"{DOMAIN}_warning_dispatch"),
    )
    await warning_dispatcher.async_initialize()
    hass.data[DOMAIN]["warning_dispatcher"] = warning_dispatcher
    hass.data[DOMAIN]["warning_dispatch_diagnostics"] = (
        warning_dispatcher.diagnostics
    )
    hass.data[DOMAIN].setdefault(
        "legacy_provider_configuration", {}
    ).setdefault("notifications", {})[
        "warning_dispatch"
    ] = warning_dispatcher.diagnostics

    # ── StormEngine + OFE aanmaken ────────────────────────────────────────
    mcs_store = McsHistoryStore(hass)
    await mcs_store.async_load()

    def _on_region_removed(region) -> None:
        """Publiceer onmiddellijk dat een lege runtime-regio verdwenen is."""
        status_cache = hass.data[DOMAIN].setdefault("mcs_status_by_storm", {})
        for key in tuple(status_cache):
            if key.startswith(f"{region.engine_id}:"):
                status_cache.pop(key, None)
        if hass.data[DOMAIN].get("region_engine") is region:
            hass.data[DOMAIN]["storms"] = []
        hass.bus.async_fire(
            f"{DOMAIN}_storms_updated",
            {
                "count": 0,
                "region_engine": region.engine_id,
                "targets": [],
                "removed_engine": True,
            },
        )
        hass.bus.async_fire(
            f"{DOMAIN}_targets_updated",
            {"targets": [], "removed_engine": region.engine_id},
        )

    async def _run_independent_tracking_check(
        region, observations, start_state, start_revision
    ) -> bool:
        """Selecteer exact lokale legacy- of duurzame workertracking."""
        validation = hass.data[DOMAIN].setdefault(
            "tracking_worker_validation", {}
        ).setdefault(region.engine_id, {
            "matching_batches": 2,
            "mode": "operationeel",
        })
        validation["mode"] = "operationeel"
        batch_id = getattr(observations, "batch_id", None)
        durable_store = hass.data[DOMAIN].get(
            "durable_processing_store"
        )
        durable_attempt_id: str | None = None

        async def _durable_call(method, **kwargs):
            return await hass.async_add_executor_job(
                partial(method, **kwargs)
            )

        async def _install_durable_candidate(
            snapshots: list[dict],
            attempt_id: str,
        ):
            # Valideer volledig vóór de DB-commit; na COMMITTED is het journal
            # de autoriteit en mag geen inhoudelijke parsefout meer volgen.
            validator = type(region.storm_engine)()
            validator.install_committed_tracking_state(
                list(snapshots),
                revision=start_revision + 1,
                committed_batch_ids=[batch_id],
                publish=False,
            )
            await _durable_call(
                durable_store.stage_result,
                region_id=region.storage_key,
                batch_id=batch_id,
                attempt_id=attempt_id,
                snapshots=snapshots,
            )
            committed_head = await _durable_call(
                durable_store.commit_staged,
                region_id=region.storage_key,
                batch_id=batch_id,
            )
            committed_ids = await hass.async_add_executor_job(
                durable_store.recent_committed_batch_ids,
                region.storage_key,
            )
            region.storm_engine.install_committed_tracking_state(
                list(committed_head.snapshots),
                revision=committed_head.revision,
                committed_batch_ids=committed_ids,
            )
            return committed_head

        if (
            not isinstance(batch_id, str)
            or len(batch_id) != 32
            or any(
                character not in "0123456789abcdef"
                for character in batch_id
            )
            or isinstance(start_revision, bool)
            or not isinstance(start_revision, int)
            or start_revision < 0
        ):
            raise ValueError("ongeldig durable trackingbatchcontract")
        if hass.data[DOMAIN].get("runtime_mode") == RUNTIME_MODE_LEGACY:
            validation.update({
                "state": "lokale_verwerking_geselecteerd",
                "mode": "legacy_local",
                "parity": False,
                "batch_id": batch_id,
                "start_revision": start_revision,
                "worker_required": False,
            })
            hass.data[DOMAIN].setdefault("tracking_worker", {})[
                region.engine_id
            ] = dict(validation)
            # De expliciete normale-HA-modus laat RegionManager dezelfde
            # observationbatch lokaal en idempotent door StormEngine committen.
            return False
        worker_first = True
        worker_applied = False
        # Do this before observation serialization, hashing and SQLite batch
        # preparation. Raw observations already have durable RECEIVED receipts
        # at OFE admission, so an offline worker can safely leave the intact
        # rootbatch queued without causing any HA compute storm.
        from .worker_bridge import worker_is_available
        if not worker_is_available(hass):
            validation.update({
                "state": "wacht_op_worker",
                "mode": "worker_required",
                "parity": False,
                "batch_id": batch_id,
            })
            hass.data[DOMAIN].setdefault("tracking_worker", {})[
                region.engine_id
            ] = dict(validation)
            raise WorkerTrackingRequiredError(
                "trackingworkerheartbeat ontbreekt of is verouderd"
            )
        serialized = []
        for observation in observations:
            raw = asdict(observation)
            raw["obs_type"] = observation.obs_type.value
            serialized.append(raw)
        start_state = _normalized_tracking_state(start_state)
        input_digest = _tracking_input_digest(serialized)
        start_state_digest = _tracking_state_digest(start_state)
        if durable_store is not None:
            durable_record = await _durable_call(
                durable_store.prepare_batch,
                region_id=region.storage_key,
                batch_id=batch_id,
                root_batch_id=getattr(
                    observations, "root_batch_id", batch_id
                ),
                parent_batch_id=getattr(
                    observations, "parent_batch_id", None
                ),
                observations=list(observations),
                start_revision=start_revision,
                start_state=start_state,
            )
            if durable_record.state == "COMMITTED":
                committed_head = await hass.async_add_executor_job(
                    durable_store.load_tracking_head,
                    region.storage_key,
                )
                if committed_head is None:
                    from .engine.durable_processing import (
                        DurableStateConflict,
                    )
                    raise DurableStateConflict(
                        "COMMITTED receipt zonder trackinghead"
                    )
                # Het wireprotocol gebruikt een v5-domein; het append-only
                # durable schema behoudt zijn bestaande v1-digestdomein zodat
                # reeds opgeslagen trackingheads verifieerbaar blijven.
                from .engine.durable_processing import (
                    tracking_state_digest as durable_tracking_state_digest,
                )
                current_digest = durable_tracking_state_digest(
                    region.storm_engine.export_tracking_state()
                )
                if (
                    region.storm_engine.revision
                    != committed_head.revision
                    or current_digest
                    != committed_head.snapshot_digest
                ):
                    committed_ids = await hass.async_add_executor_job(
                        durable_store.recent_committed_batch_ids,
                        region.storage_key,
                    )
                    region.storm_engine.install_committed_tracking_state(
                        list(committed_head.snapshots),
                        revision=committed_head.revision,
                        committed_batch_ids=committed_ids,
                    )
                acknowledge = getattr(
                    observations, "acknowledge", None
                )
                if callable(acknowledge):
                    acknowledge()
                return True
            if durable_record.state == "RESULT_STAGED":
                committed_head = await _durable_call(
                    durable_store.commit_staged,
                    region_id=region.storage_key,
                    batch_id=batch_id,
                )
                committed_ids = await hass.async_add_executor_job(
                    durable_store.recent_committed_batch_ids,
                    region.storage_key,
                )
                region.storm_engine.install_committed_tracking_state(
                    list(committed_head.snapshots),
                    revision=committed_head.revision,
                    committed_batch_ids=committed_ids,
                )
                acknowledge = getattr(
                    observations, "acknowledge", None
                )
                if callable(acknowledge):
                    acknowledge()
                return True
        elif region.storm_engine.has_applied_batch(batch_id):
            acknowledge = getattr(observations, "acknowledge", None)
            if callable(acknowledge):
                acknowledge()
            return True
        diagnostic = {
            "state": "transportcontrole",
            "mode": "transportcontrole",
            "parity": False,
            "batch_id": batch_id,
            "start_revision": start_revision,
            "input_digest": input_digest,
            "start_state_digest": start_state_digest,
            "input_observation_count": len(serialized),
        }
        tracking_worker_job_id: str | None = None
        try:
            from .worker_bridge import async_request_worker_job

            if durable_store is not None:
                tracking_worker_job_id = (
                    f"tracking-{batch_id}-"
                    f"{durable_record.attempt_count + 1}"
                )
                durable_attempt_id = await _durable_call(
                    durable_store.begin_attempt,
                    region_id=region.storage_key,
                    batch_id=batch_id,
                    executor="worker",
                    lease_seconds=20.0,
                    job_id=tracking_worker_job_id,
                )
            result = await async_request_worker_job(
                hass,
                "tracking_operational",
                {
                    "tracking_protocol_version": 5,
                    "region_id": region.engine_id,
                    "batch_id": batch_id,
                    "start_revision": start_revision,
                    "input_observation_count": len(serialized),
                    "start_state_count": len(start_state),
                    "observations": serialized,
                    "start_state": start_state,
                    "input_digest": input_digest,
                    "start_state_digest": start_state_digest,
                },
                timeout=12,
                job_id=tracking_worker_job_id,
            )
            if result.get("method") != "independent_tracking_batch_v5":
                raise ValueError("ongeldig zelfstandig trackingresultaat")
            snapshots = result.get("snapshots")
            active_tracks = result.get("active_tracks")
            if (
                result.get("tracking_protocol_version") != 5
                or result.get("region_id") != region.engine_id
                or result.get("batch_id") != batch_id
                or type(result.get("start_revision")) is not int
                or result.get("start_revision") != start_revision
                or type(result.get("result_revision")) is not int
                or result.get("result_revision") != start_revision + 1
                or type(result.get("input_observation_count")) is not int
                or result.get("input_observation_count") != len(serialized)
                or type(result.get("start_state_count")) is not int
                or result.get("start_state_count") != len(start_state)
                or not _matching_sha256(
                    result.get("input_digest"), input_digest
                )
                or not _matching_sha256(
                    result.get("start_state_digest"),
                    start_state_digest,
                )
                or not isinstance(snapshots, list)
                or len(snapshots) > 2048
                or type(result.get("snapshot_count")) is not int
                or result.get("snapshot_count") != len(snapshots)
                or not isinstance(active_tracks, list)
                or any(
                    not isinstance(snapshot, dict)
                    or not isinstance(snapshot.get("storm_id"), str)
                    or not snapshot.get("storm_id")
                    or not _valid_tracking_v5_snapshot(snapshot)
                    for snapshot in snapshots
                )
            ):
                raise ValueError("onvolledig zelfstandig trackingresultaat")
            snapshot_ids = [snapshot["storm_id"] for snapshot in snapshots]
            if len(set(snapshot_ids)) != len(snapshot_ids):
                raise ValueError("dubbele storm-ID in workerresultaat")
            snapshots = _normalized_tracking_state(snapshots)
            snapshot_digest = _tracking_state_digest(snapshots)
            if not _matching_sha256(
                result.get("snapshot_digest"), snapshot_digest
            ):
                raise ValueError("snapshot_digest komt niet overeen")
            if worker_first:
                if durable_store is not None:
                    await _install_durable_candidate(
                        snapshots, durable_attempt_id
                    )
                else:
                    region.storm_engine.replace_tracking_state(
                        snapshots,
                        expected_revision=start_revision,
                        batch_id=batch_id,
                    )
                parity = True
                local_digest = snapshot_digest
                worker_applied = True
                acknowledge = getattr(observations, "acknowledge", None)
                if callable(acknowledge):
                    acknowledge()
            diagnostic.update({
                "state": "operationeel",
                "mode": "worker_first",
                "parity": parity,
                "worker_digest": result.get("snapshot_digest"),
                "local_digest": local_digest,
                "batch_id": batch_id,
                "start_revision": start_revision,
                "result_revision": result.get("result_revision"),
                "input_digest": input_digest,
                "track_count": len(result.get("active_tracks", [])),
                "split_count": int(result.get("split_count", 0)),
                "merge_count": int(result.get("merge_count", 0)),
                "lineage_events": list(
                    result.get("lineage_events", [])
                )[:20],
                "start_state_digest": start_state_digest,
                "matching_batches": validation["matching_batches"],
            })
        except asyncio.CancelledError:
            if durable_store is not None and durable_attempt_id:
                try:
                    await _durable_call(
                        durable_store.fail_attempt,
                        region_id=region.storage_key,
                        batch_id=batch_id,
                        attempt_id=durable_attempt_id,
                        error_category="CancelledError",
                        error_detail="trackingcallback werd geannuleerd",
                    )
                except Exception:
                    _LOGGER.exception(
                        "Geannuleerde durable attempt kon niet veilig "
                        "naar PREPARED worden teruggezet"
                    )
            raise
        except (
            asyncio.TimeoutError,
            KeyError,
            RuntimeError,
            TypeError,
            ValueError,
        ) as err:
            durable_committed = False
            if durable_store is not None:
                durable_after_error = await _durable_call(
                    durable_store.load_batch,
                    region_id=region.storage_key,
                    batch_id=batch_id,
                )
                durable_committed = bool(
                    durable_after_error is not None
                    and durable_after_error.state == "COMMITTED"
                )
            if (
                region.storm_engine.has_applied_batch(batch_id)
                or durable_committed
            ):
                if (
                    durable_committed
                    and not region.storm_engine.has_applied_batch(batch_id)
                ):
                    committed_head = await hass.async_add_executor_job(
                        durable_store.load_tracking_head,
                        region.storage_key,
                    )
                    committed_ids = await hass.async_add_executor_job(
                        durable_store.recent_committed_batch_ids,
                        region.storage_key,
                    )
                    region.storm_engine.install_committed_tracking_state(
                        list(committed_head.snapshots),
                        revision=committed_head.revision,
                        committed_batch_ids=committed_ids,
                    )
                worker_applied = True
                acknowledge = getattr(observations, "acknowledge", None)
                if callable(acknowledge):
                    acknowledge()
                diagnostic.update({
                    "state": "operationeel",
                    "mode": "worker_first",
                    "parity": True,
                    "batch_id": batch_id,
                    "start_revision": start_revision,
                    "result_revision": region.storm_engine.revision,
                    "post_commit_warning": type(err).__name__,
                })
            elif durable_store is not None:
                from .engine.durable_processing import (
                    DurableProcessingError,
                )
                if durable_attempt_id:
                    await _durable_call(
                        durable_store.fail_attempt,
                        region_id=region.storage_key,
                        batch_id=batch_id,
                        attempt_id=durable_attempt_id,
                        error_category=type(err).__name__,
                        error_detail=str(err),
                    )
                raise DurableProcessingError(
                    "trackingworker niet beschikbaar; batch blijft duurzaam "
                    "wachten zonder lokale CPU-fallback"
                ) from err
            else:
                from .engine.durable_processing import DurableProcessingError
                raise DurableProcessingError(
                    "trackingworker niet beschikbaar en geen duurzaam journal"
                ) from err
        hass.data[DOMAIN].setdefault("tracking_worker", {})[
            region.engine_id
        ] = diagnostic
        hass.data[DOMAIN].setdefault(
            "verification_samples_pending", []
        ).append({
            "sample_key": (
                f"worker_tracking:{region.engine_id}:"
                f"{int(time.time() // 60)}"
            ),
            "sample_type": "worker_tracking",
            "target_id": region.engine_id,
            "nominal_minute": int(time.time() // 60),
            "observed_at": time.time(),
            "latitude": region.center_lat,
            "longitude": region.center_lon,
            "own_status": diagnostic["state"],
            "own_forecast_available": bool(diagnostic.get("parity")),
            "snapshot": {
                "mode": diagnostic["mode"],
                "parity": diagnostic.get("parity", False),
                "batch_id": diagnostic.get("batch_id"),
                "start_revision": diagnostic.get("start_revision"),
                "result_revision": diagnostic.get("result_revision"),
                "input_digest": diagnostic.get("input_digest"),
                "worker_digest": diagnostic.get("worker_digest"),
                "local_digest": diagnostic.get("local_digest"),
                "start_state_digest": diagnostic.get(
                    "start_state_digest"
                ),
                "input_observation_count": len(serialized),
                "matching_batches": diagnostic.get("matching_batches", 0),
                "split_count": diagnostic.get("split_count", 0),
                "merge_count": diagnostic.get("merge_count", 0),
                "lineage_events": diagnostic.get("lineage_events", []),
            },
        })
        hass.bus.async_fire(f"{DOMAIN}_worker_status_update")
        # Alleen een volledig gevalideerde workersnapshot bevestigt de batch.
        # Er bestaat bewust geen lokale CPU-fallback.
        return worker_applied

    storm_manager = StormManager(
        hass,
        sharing_distance_km=sharing_distance,
        observation_radius_km=radar_radius,
        on_engine_removed=_on_region_removed,
        on_observation_batch=_run_independent_tracking_check,
        allow_local_processing=True,
    )
    # Fail closed vanaf het eerste moment dat de manager kan worden gedeeld.
    # Geen directe caller mag de korte setupfase vóór journalherstel benutten.
    storm_manager.begin_durable_recovery()
    active_region = storm_manager.assign_target(fictieve_entity, home_lat, home_lon)
    hass.data[DOMAIN]["targets"]["home"]["region_engine_id"] = active_region.engine_id
    storm_engine = active_region.storm_engine
    ofe = active_region.ofe
    prepared_regions: set[str] = set()

    def _prepare_region(region) -> int:
        """Koppel opslag en publicatie eenmaal aan een runtime-regio."""
        if region.engine_id in prepared_regions:
            return 0
        durable_configurator = hass.data[DOMAIN].get(
            "configure_durable_region"
        )
        if durable_configurator is not None:
            durable_configurator(region)
        region_storm_engine = region.storm_engine
        restored = mcs_store.restore_engine(region.storage_key, region_storm_engine)
        # Targetbinding bestond al vóór restore; herbouw daarom nu ook het
        # lossless computeplan tegen de werkelijk herstelde trackingstate.
        # Sensorreads tonen alleen deze snapshot en starten geen berekening.
        region.refresh_compute_schedule()

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

    async def _retry_quarantined_observations(call) -> None:
        """Heractiveer expliciet bewaarde poisonbatches per regio of globaal."""
        requested_engine = call.data.get("region_engine")
        regions = list(storm_manager.get_all_engines())
        if requested_engine:
            regions = [
                region
                for region in regions
                if region.engine_id == requested_engine
            ]
            if not regions:
                _LOGGER.warning(
                    "Quarantaine-retry geweigerd: onbekende RegionEngine %s",
                    requested_engine,
                )
                return

        retried_batches = 0
        for region in regions:
            durable_store = hass.data[DOMAIN].get(
                "durable_processing_store"
            )
            durable_records = ()
            if durable_store is not None:
                durable_records = await hass.async_add_executor_job(
                    partial(
                        durable_store.retry_quarantined,
                        region_id=region.storage_key,
                    )
                )
            retried_batches += await (
                region.ofe.async_retry_quarantined()
            )
            retried_batches += await region.ofe.async_restore_batches(
                durable_records
            )
        hass.bus.async_fire(
            f"{DOMAIN}_storms_updated",
            {
                "reason": "quarantine_retry",
                "region_engine": requested_engine,
                "retried_batches": retried_batches,
            },
        )
        _LOGGER.info(
            "OFE-quarantaine opnieuw aangeboden: %d batches (%s)",
            retried_batches,
            requested_engine or "alle RegionEngines",
        )

    if not hass.services.has_service(
        DOMAIN, SERVICE_RETRY_QUARANTINED_OBSERVATIONS
    ):
        hass.services.async_register(
            DOMAIN,
            SERVICE_RETRY_QUARANTINED_OBSERVATIONS,
            _retry_quarantined_observations,
            schema=RETRY_QUARANTINED_SERVICE_SCHEMA,
        )

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
    hass.data[DOMAIN]["netatmo_roaming_provider"] = None
    hass.data[DOMAIN]["netatmo_roaming_center"] = None
    hass.data[DOMAIN]["netatmo_roaming_last_query"] = 0.0
    hass.data[DOMAIN]["netatmo_roaming_observations"] = []

    # ── Blitzortung (wereldwijd, locatie-onafhankelijk) ───────────────────
    def _record_lightning(observation) -> list[str]:
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
        return engine_ids

    lightning_updates = LightningUpdateCoalescer(
        hass.loop.call_later,
        lambda payload: hass.bus.async_fire(
            f"{DOMAIN}_lightning_update", payload
        ),
        lambda payload: hass.bus.async_fire(
            f"{DOMAIN}_lightning_map_update", payload
        ),
        lambda rows: hass.data[DOMAIN]["lightning_aggregates_pending"].extend(
            rows
        ),
    )
    hass.data[DOMAIN]["lightning_update_coalescer"] = lightning_updates

    def _queue_lightning_update(
        observation, *, log: bool = False, engine_ids: list[str] | None = None
    ) -> None:
        lightning_updates.submit({
            "lat": observation.lat,
            "lon": observation.lon,
            "timestamp": observation.timestamp,
            "source": observation.source,
            "engine_ids": engine_ids or [],
        }, log=log)

    async def _on_blitz(obs):
        if hass.data[DOMAIN].get("lightning_source_mode") == "satellite_test":
            return
        if await storm_manager.async_route_observation(obs) == 0:
            return
        hass.data[DOMAIN]["lightning_source"] = "blitzortung"
        hass.data[DOMAIN]["last_lightning"] = obs
        hass.data[DOMAIN].setdefault("lightning_count", 0)
        hass.data[DOMAIN]["lightning_count"] += 1
        engine_ids = _record_lightning(obs)
        _queue_lightning_update(obs, log=True, engine_ids=engine_ids)

    def _blitz_regions():
        return [
            (region.center_lat, region.center_lon, region.observation_radius_km)
            for region in storm_manager.get_all_engines()
        ]

    blitz = BlitzortungProvider(on_observation=_on_blitz, regions=_blitz_regions())
    hass.data[DOMAIN]["blitz_provider"] = blitz

    eumetsat = None
    if conf.get("eumetsat_consumer_key") and conf.get("eumetsat_consumer_secret"):
        from .providers.eumetsat_li import EumetsatLightningProvider
        eumetsat = EumetsatLightningProvider(
            http_session,
            conf["eumetsat_consumer_key"],
            conf["eumetsat_consumer_secret"],
            hass=hass,
        )
        hass.data[DOMAIN]["eumetsat_li_provider"] = eumetsat
        hass.data[DOMAIN]["eumetsat_li_status"] = "standby"
        _LOGGER.info("EUMETSAT LI geconfigureerd als Blitzortung-fallback")

    from .providers.noaa_goes_glm import (
        NoaaGoesGlmProvider,
        preferred_source_for_longitude,
        satellites_for_regions,
    )
    goes_glm = NoaaGoesGlmProvider(http_session, hass=hass)
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

    provider_lifecycle = ProviderLifecycleController(
        cooldown_seconds=300,
        # Worker-first national providers own deadlines up to 70 seconds.
        # This controller must never cancel their HA waiter while the accepted
        # workerjob continues orphaned in the isolated queue.
        fetch_timeout_seconds=90,
    )
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
    provider_lifecycle.register(
        ItaliaMeteoRadarProvider(
            http_session,
            model_guidance_enabled=open_meteo_enabled,
        ),
        _national_context,
    )
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
    hass.data[DOMAIN]["noaa_goes_rrqpe_provider"] = NoaaGoesRrqpeProvider(
        http_session, hass=hass
    )
    _LOGGER.info("NOAA GOES-18/19 RRQPE geconfigureerd als slapende Amerika-fallback")
    from .providers.noaa_enterprise_rainrate import (
        NoaaEnterpriseRainRateProvider,
    )
    hass.data[DOMAIN][
        "noaa_enterprise_rainrate_provider"
    ] = NoaaEnterpriseRainRateProvider(hass)
    _LOGGER.info(
        "NOAA Enterprise Rain Rate geconfigureerd als worker-first "
        "wereldwijde satellietfallback"
    )
    if hsaf_username and hsaf_password:
        from .providers.hsaf_h40b import HsafH40bProvider
        hass.data[DOMAIN]["hsaf_h40b_provider"] = HsafH40bProvider(
            hsaf_username, hsaf_password, hass=hass
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
        enterprise = hass.data[DOMAIN].get(
            "noaa_enterprise_rainrate_provider"
        )
        enterprise_success = (
            getattr(enterprise, "_last_success_ts", None)
            if enterprise else None
        )
        enterprise_supported = bool(
            region is not None
            and enterprise is not None
            and enterprise.supports(CoverageArea(
                region.center_lat,
                region.center_lon,
                region.observation_radius_km,
            )).supported
        )
        states["noaa_enterprise_rainrate"] = SourceState(
            configured=enterprise_supported,
            healthy=bool(
                enterprise_supported
                and enterprise.healthy
                and enterprise_success is not None
                and now_ts - float(enterprise_success) <= 45 * 60
            ),
            last_success=(
                float(enterprise_success)
                if enterprise_success is not None else None
            ),
        )
        return states

    def _refresh_engine_radar_decisions():
        from .engine.provider_bias import (
            select_transition_profile,
            transition_adjustment,
        )
        from .providers.engine_radar_policy import (
            apply_echo_availability,
            apply_quality_fallback,
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
                enterprise_observations=hass.data[DOMAIN]
                .get(
                    "noaa_enterprise_rainrate_observation_counts_by_engine",
                    {},
                )
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
            completeness_profile = None
            if decision.source not in {None, "rainviewer"}:
                completeness_profile = select_transition_profile(
                    hass.data[DOMAIN].get("radar_bias_profile_index", {}),
                    region_key=region.storage_key,
                    from_source="rainviewer",
                    to_source=decision.source,
                )
            decision = apply_quality_fallback(
                decision,
                states,
                rainviewer_observations=hass.data[DOMAIN]
                .get("rainviewer_observation_counts_by_engine", {})
                .get(region.engine_id, 0),
                completeness_profile=completeness_profile,
                now=now_ts,
            )
            previous_source = (
                previous_decisions.get(region.engine_id) or {}
            ).get("source")
            if previous_source and decision.source and previous_source != decision.source:
                bias_profile = select_transition_profile(
                    hass.data[DOMAIN].get("radar_bias_profile_index", {}),
                    region_key=region.storage_key,
                    from_source=previous_source,
                    to_source=decision.source,
                )
                adjustment = transition_adjustment(bias_profile)
                transitions[region.engine_id] = {
                    "from": previous_source,
                    "to": decision.source,
                    "started_at": now_ts,
                    "active_until": (
                        now_ts + adjustment["transition_window_seconds"]
                    ),
                    **adjustment,
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
                "enterprise_rainrate": {
                    "supported": states["noaa_enterprise_rainrate"].configured,
                    "status": (
                        hass.data[DOMAIN]
                        .get("noaa_enterprise_rainrate_diagnostics", {})
                        .get("status", "standby")
                    ),
                    "observations": hass.data[DOMAIN]
                    .get(
                        "noaa_enterprise_rainrate_observation_counts_by_engine",
                        {},
                    )
                    .get(region.engine_id, 0),
                    "mode": hass.data[DOMAIN]
                    .get("noaa_enterprise_rainrate_diagnostics", {})
                    .get("mode", "worker_first"),
                },
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

    async def _route_selected_radar(
        observations, source: str
    ) -> int:
        decisions = hass.data[DOMAIN].get("radar_sources_by_engine", {})
        routed = 0
        for region in storm_manager.get_all_engines():
            region_decision = decisions.get(region.engine_id) or {}
            if region_decision.get("fusion", {}).get("active") and source in set(
                region_decision.get("fusion", {}).get("sources", ())
            ):
                continue
            if region_decision.get("source") != source:
                continue
            routed += (
                await storm_manager.async_route_observations_to_engine(
                    region.engine_id, observations
                )
            )
        return routed

    async def _route_american_radar_fusion(
        rainviewer_observations,
        enterprise_observations,
        goes_observations,
    ) -> int:
        from .providers.radar_fusion import (
            fuse_radar_observations,
            is_american_region,
        )

        decisions = hass.data[DOMAIN].get("radar_sources_by_engine", {})
        routed = 0
        now_ts = time.time()
        for region in storm_manager.get_all_engines():
            decision = decisions.get(region.engine_id) or {}
            if (
                decision.get("source") != "rainviewer"
                or not is_american_region(decision.get("country_codes", ()))
            ):
                continue
            selected_primary = [
                item for item in rainviewer_observations
                if region.accepts_observation(item.lat, item.lon)
            ]
            selected_enterprise = [
                item for item in enterprise_observations
                if region.accepts_observation(item.lat, item.lon)
            ]
            selected_goes = [
                item for item in goes_observations
                if region.accepts_observation(item.lat, item.lon)
            ]
            fused, diagnostic = fuse_radar_observations(
                selected_primary,
                (selected_enterprise, selected_goes),
                now=now_ts,
            )
            decision["fusion"] = diagnostic
            if not diagnostic["active"]:
                continue
            decision["reason"] = (
                f"{decision['reason']}; RainViewer aangevuld met NOAA "
                "waar de primaire radar geen echo bevat"
            )
            routed += (
                await storm_manager.async_route_observations_to_engine(
                    region.engine_id, fused
                )
            )
        return routed
    from .engine.radar_calibration import RadarCalibrationObserver
    from .engine.calibration_store import CalibrationDataStore
    from .engine.provider_bias import build_profile_index
    from .engine.virtual_probe import VirtualProbeManager
    hass.data[DOMAIN]["radar_calibration_observer"] = RadarCalibrationObserver(
        evaluation_center=(home_lat, home_lon),
        evaluation_radius_km=radar_radius,
        compare_locally=False,
    )
    calibration_store = CalibrationDataStore(
        hass.config.path(".storage", "storm_tracker_v3_calibration.sqlite3")
    )
    calibration_stats = await hass.async_add_executor_job(
        calibration_store.initialize
    )
    # The HA-side database is retained for rollback and lightweight telemetry,
    # but its legacy radar profiles are no longer operational input. Start
    # conservatively empty until the worker corpus snapshot arrives.
    bias_profiles = []
    restored_virtual_probes = await hass.async_add_executor_job(
        calibration_store.load_pending_virtual_probes
    )
    restored_virtual_probe_history = await hass.async_add_executor_job(
        calibration_store.load_virtual_probe_history
    )
    hass.data[DOMAIN]["radar_calibration_store"] = calibration_store
    from .engine.durable_processing import (
        DurableProcessingStore,
        STATE_PREPARED,
        STATE_RESULT_STAGED,
    )
    durable_processing_store = DurableProcessingStore(
        calibration_store.path,
        build_id="integration-1.14.98-worker-1.17.109",
    )
    durable_processing_stats = await hass.async_add_executor_job(
        durable_processing_store.initialize
    )
    hass.data[DOMAIN][
        "durable_processing_store"
    ] = durable_processing_store

    def _configure_durable_region(region) -> None:
        async def _persist_region_observations(
            observations, *, deduplicated=None
        ):
            return await hass.async_add_executor_job(
                partial(
                    durable_processing_store.receive_observations,
                    region_id=region.storage_key,
                    observations=list(observations),
                    deduplicated=deduplicated,
                )
            )

        async def _persist_region_quarantine(batches, error):
            for batch in batches:
                lineage = batch._receipt.lineage
                await hass.async_add_executor_job(
                    partial(
                        durable_processing_store.quarantine_batch,
                        region_id=region.storage_key,
                        batch_id=batch.batch_id,
                        reason=(
                            f"{type(error).__name__}: {str(error)[:240]}"
                        ),
                        lineage={
                            "root_batch_id": batch.root_batch_id,
                            "failures": lineage.failures,
                            "failures_since_success": (
                                lineage.failures_since_success
                            ),
                            "successes": lineage.successes,
                            "epoch": lineage.epochs,
                        },
                    )
                )

        region.ofe.configure_durable_ingest(
            _persist_region_observations,
            classified_deduplication=True,
        )
        region.ofe.configure_durable_quarantine(
            _persist_region_quarantine
        )

    hass.data[DOMAIN][
        "configure_durable_region"
    ] = _configure_durable_region
    hass.data[DOMAIN][
        "durable_processing"
    ] = {
        "status": "recovering",
        **durable_processing_stats,
    }
    durable_recovered_engines: set[str] = set()

    async def _recover_durable_region(region) -> tuple[int, int]:
        if region.engine_id in durable_recovered_engines:
            return 0, 0
        region.durable_region_id = await hass.async_add_executor_job(
            partial(
                durable_processing_store.resolve_region_id,
                center_lat=region.center_lat,
                center_lon=region.center_lon,
                observation_radius_km=region.observation_radius_km,
                sharing_distance_km=storm_manager.sharing_distance_km,
                target_ids=sorted(region.projection_targets),
            )
        )
        _configure_durable_region(region)
        recovery_backlog = await hass.async_add_executor_job(
            partial(
                durable_processing_store.recover_after_restart,
                region_id=region.storage_key,
            )
        )
        for record in recovery_backlog:
            if record.state != STATE_RESULT_STAGED:
                continue
            await hass.async_add_executor_job(
                partial(
                    durable_processing_store.commit_staged,
                    region_id=region.storage_key,
                    batch_id=record.batch_id,
                )
            )
        durable_head = await hass.async_add_executor_job(
            durable_processing_store.load_tracking_head,
            region.storage_key,
        )
        if durable_head is not None:
            committed_ids = await hass.async_add_executor_job(
                durable_processing_store.recent_committed_batch_ids,
                region.storage_key,
            )
            region.storm_engine.install_committed_tracking_state(
                list(durable_head.snapshots),
                revision=durable_head.revision,
                committed_batch_ids=committed_ids,
            )
        prepared_backlog = [
            record
            for record in recovery_backlog
            if record.state == STATE_PREPARED
        ]
        restored_batches = await region.ofe.async_restore_batches(
            prepared_backlog
        )
        received_backlog = await hass.async_add_executor_job(
            partial(
                durable_processing_store
                .recover_received_observations,
                region_id=region.storage_key,
            )
        )
        restored_observations = (
            await region.ofe.async_restore_observations(
                received_backlog
            )
        )
        if prepared_backlog or received_backlog:
            await region.ofe.async_flush_pending()
        region.recovery_ready = True
        durable_recovered_engines.add(region.engine_id)
        return restored_batches, restored_observations

    hass.data[DOMAIN][
        "recover_durable_region"
    ] = _recover_durable_region
    recovered_batch_count = 0
    recovered_observation_count = 0
    for region in storm_manager.get_all_engines():
        restored_batches, restored_observations = (
            await _recover_durable_region(region)
        )
        recovered_batch_count += restored_batches
        recovered_observation_count += restored_observations
    durable_integrity = await hass.async_add_executor_job(
        durable_processing_store.integrity_check
    )
    storm_manager.verify_durable_integrity(
        integrity_healthy=bool(durable_integrity.get("healthy"))
    )
    hass.data[DOMAIN]["durable_processing"].update({
        "status": "integrity_verified",
        "admission_ready": False,
        "recovered_batches": recovered_batch_count,
        "recovered_observations": recovered_observation_count,
        **durable_integrity,
    })
    hass.data[DOMAIN]["radar_bias_profiles"] = bias_profiles
    hass.data[DOMAIN]["radar_bias_profile_index"] = build_profile_index(
        bias_profiles
    )
    hass.data[DOMAIN]["radar_calibration_storage"] = {
        "status": "ready", "frames_written": 0,
        "comparisons_written": 0, "verification_samples_written": 0,
        "pysteps_preflight_samples_written": 0,
        "system_performance_samples_written": 0,
        "lightning_aggregate_cells_written": 0,
        "provider_cycle_samples_written": 0,
        "ground_observations_written": 0,
        "ground_quality_assessments_written": 0,
        "ground_confirmation_decisions_written": 0,
        **calibration_stats,
    }
    hass.data[DOMAIN]["verification_samples_pending"] = []
    hass.data[DOMAIN]["pysteps_preflight_samples_pending"] = []
    hass.data[DOMAIN]["system_performance_samples_pending"] = []
    hass.data[DOMAIN]["lightning_aggregates_pending"] = []
    hass.data[DOMAIN]["provider_cycle_samples_pending"] = []
    hass.data[DOMAIN]["ground_observations_pending"] = []
    hass.data[DOMAIN]["ground_quality_assessments_pending"] = []
    hass.data[DOMAIN]["ground_confirmation_decisions_pending"] = []

    def _queue_provider_cycle_sample(
        provider: str,
        scope_key: str,
        latitude: float,
        longitude: float,
        metrics: dict,
        *,
        observed_at: float | None = None,
    ) -> None:
        timestamp = float(observed_at or time.time())
        nominal_minute = int(timestamp // 60)
        hass.data[DOMAIN]["provider_cycle_samples_pending"].append({
            "sample_key": (
                f"{provider}:{scope_key}:{nominal_minute}"
            ),
            "provider": provider,
            "scope_key": scope_key,
            "nominal_minute": nominal_minute,
            "observed_at": timestamp,
            "latitude": float(latitude),
            "longitude": float(longitude),
            "metrics": metrics,
        })
    hass.data[DOMAIN]["virtual_probe_manager"] = VirtualProbeManager(
        restored_virtual_probes,
        restored_virtual_probe_history,
    )
    hass.data[DOMAIN]["virtual_probe_diagnostics"] = hass.data[DOMAIN][
        "virtual_probe_manager"
    ].diagnostics()
    initial_diagnostics = (
        hass.data[DOMAIN]["radar_calibration_observer"].diagnostics()
    )
    initial_diagnostics["storage"] = hass.data[DOMAIN][
        "radar_calibration_storage"
    ]
    initial_diagnostics["bias_profiles"] = {
        "total": len(bias_profiles),
        "operational": sum(
            1 for profile in bias_profiles
            if profile.get("confidence") != "insufficient"
        ),
        "profiles": bias_profiles[:20],
    }
    hass.data[DOMAIN]["radar_calibration"] = initial_diagnostics
    # Voorbereiding op de geïsoleerde pySTEPS-worker: enkel observerend.
    hass.data[DOMAIN]["pysteps_frame_history"] = RadarFrameHistory()
    hass.data[DOMAIN]["pysteps_input"] = (
        hass.data[DOMAIN]["pysteps_frame_history"].diagnostics()
    )

    calibration_coverage_bounds = {
        "kmi": (-2.5, 46.5, 10.5, 53.0),
        "knmi": (0.0, 48.895, 10.856, 55.974),
        "dwd_radolan": (1.0, 45.5, 19.0, 56.0),
        "met_office_radar": (-12.0, 48.0, 4.0, 62.0),
        "meteofrance_radar": (-6.0, 40.5, 10.0, 52.0),
        "dpc_radar": (4.5, 35.0, 20.5, 48.0),
        "aemet_radar": (-12.2, 33.0, 6.2, 46.5),
    }
    minimum_calibration_coverage_fraction = 0.60

    def _region_bounds(region) -> tuple[float, float, float, float]:
        lat_delta = region.observation_radius_km / 110.574
        lon_delta = region.observation_radius_km / (
            111.320
            * max(0.1, abs(math.cos(math.radians(region.center_lat))))
        )
        return (
            region.center_lon - lon_delta,
            region.center_lat - lat_delta,
            region.center_lon + lon_delta,
            region.center_lat + lat_delta,
        )

    restored_pysteps_frames = await hass.async_add_executor_job(
        calibration_store.load_pysteps_frames
    )
    restored_pysteps_count = hass.data[DOMAIN][
        "pysteps_frame_history"
    ].restore(
        restored_pysteps_frames,
        {
            region.engine_id: _region_bounds(region)
            for region in storm_manager.get_all_engines()
        },
    )
    hass.data[DOMAIN]["pysteps_restored_frames"] = restored_pysteps_count
    hass.data[DOMAIN]["pysteps_input"] = hass.data[DOMAIN][
        "pysteps_frame_history"
    ].diagnostics()
    def _refresh_restart_recovery_audit() -> None:
        restart_recovery_audit = {}
        for region in storm_manager.get_all_engines():
            audit = audit_storm_object_recovery(
                hass.data[DOMAIN]["pysteps_frame_history"].frames(region.engine_id),
                region.storm_engine.get_storms(),
            )
            restart_recovery_audit[region.engine_id] = audit
            if audit["status"] == "ontbrekend_stormobject":
                _LOGGER.warning(
                    "Herstartaudit %s: recente radarframecache heeft geen "
                    "bijbehorend stormobject; eerste nieuwe contour start met "
                    "bewaarde rastervector maar zonder oude storm-ID",
                    region.engine_id,
                )
            elif audit["status"] == "hersteld":
                _LOGGER.info(
                    "Herstartaudit %s: stormobject(en) %s dekken de recente "
                    "radarframecache",
                    region.engine_id,
                    ", ".join(audit["matched_storm_ids"]),
                )
        hass.data[DOMAIN]["restart_recovery_audit"] = restart_recovery_audit

    def _restore_pending_region_frames_and_audit() -> None:
        """Herstel ook caches van regio's die pas na setup worden aangemaakt."""
        additional_restored = hass.data[DOMAIN]["pysteps_frame_history"].restore(
            restored_pysteps_frames,
            {
                region.engine_id: _region_bounds(region)
                for region in storm_manager.get_all_engines()
            },
        )
        if additional_restored:
            hass.data[DOMAIN]["pysteps_restored_frames"] += additional_restored
            hass.data[DOMAIN]["pysteps_input"] = hass.data[DOMAIN][
                "pysteps_frame_history"
            ].diagnostics()
        _refresh_restart_recovery_audit()

    _refresh_restart_recovery_audit()

    def _coverage_fraction(
        coverage: tuple[float, float, float, float],
        region_bounds: tuple[float, float, float, float],
    ) -> float:
        intersection = (
            max(coverage[0], region_bounds[0]),
            max(coverage[1], region_bounds[1]),
            min(coverage[2], region_bounds[2]),
            min(coverage[3], region_bounds[3]),
        )
        if (
            intersection[0] >= intersection[2]
            or intersection[1] >= intersection[3]
        ):
            return 0.0
        region_area = (
            (region_bounds[2] - region_bounds[0])
            * (region_bounds[3] - region_bounds[1])
        )
        intersection_area = (
            (intersection[2] - intersection[0])
            * (intersection[3] - intersection[1])
        )
        return min(1.0, max(0.0, intersection_area / region_area))

    def _calibration_coverage(source: str, region):
        region_bounds = _region_bounds(region)
        if source == "opera":
            provider = hass.data[DOMAIN].get(
                "opera_providers_by_engine", {}
            ).get(region.engine_id)
            if provider is not None:
                return tuple(float(value) for value in provider._bbox())
        if source in {
            "rainviewer",
            "hsaf_h40b",
            "noaa_goes_rrqpe",
        }:
            return region_bounds
        return calibration_coverage_bounds.get(source)

    def _record_calibration_frame(
        source: str,
        observations,
        timestamp: float | None = None,
        *,
        engine_id: str | None = None,
    ) -> int:
        """Queue only a small worker-artifact capture descriptor.

        ``observations`` is deliberately not iterated: rasterisation,
        footprint walking, comparison and original-overlay compression belong
        to the worker and may never stall Home Assistant's event loop.
        """
        if timestamp is None:
            return 0
        per_engine_providers = {
            "rainviewer": hass.data[DOMAIN].get(
                "rainviewer_providers_by_engine", {}
            ),
            "opera": hass.data[DOMAIN].get(
                "opera_providers_by_engine", {}
            ),
        }.get(source, {})
        if per_engine_providers:
            providers = (
                {engine_id: per_engine_providers.get(engine_id)}
                if engine_id is not None
                else per_engine_providers
            )
        else:
            shared_key = {
                "kmi": "kmi_provider",
                "knmi": "knmi_provider",
                "hsaf_h40b": "hsaf_h40b_provider",
                "noaa_goes_rrqpe": "noaa_goes_rrqpe_provider",
                "noaa_enterprise_rainrate":
                    "noaa_enterprise_rainrate_provider",
            }.get(source)
            shared_provider = (
                hass.data[DOMAIN].get(shared_key) if shared_key else None
            )
            providers = {engine_id or "shared": shared_provider}
        provider = next(
            (value for value in providers.values() if value is not None),
            None,
        )
        artifact = (
            getattr(provider, "last_worker_artifact", None)
            if provider is not None else None
        )
        if artifact is None:
            artifact = provider_lifecycle.artifact(source)
        required_artifact_keys = {
            "artifact_path", "artifact_sha256", "artifact_bytes",
            "content_type", "frame_timestamp",
        }
        if (
            not isinstance(artifact, dict)
            or not required_artifact_keys.issubset(artifact)
        ):
            skipped = hass.data[DOMAIN].setdefault(
                "calibration_capture_skipped", {}
            )
            skipped[str(source)] = {
                "reason": "validated_worker_artifact_missing",
                "timestamp": time.time(),
            }
            return 0
        immutable_artifact = {
            key: artifact[key] for key in sorted(required_artifact_keys)
        }
        pending = hass.data[DOMAIN].setdefault(
            "calibration_captures_pending", {}
        )
        queued = 0
        for region in storm_manager.get_all_engines():
            if engine_id is not None and region.engine_id != engine_id:
                continue
            coverage_bbox = _calibration_coverage(source, region)
            if coverage_bbox is None:
                continue
            coverage_fraction = _coverage_fraction(
                coverage_bbox, _region_bounds(region)
            )
            if (
                coverage_fraction
                < minimum_calibration_coverage_fraction
            ):
                continue
            descriptor = {
                "contract": "stv3-calibration-capture-v2",
                # Calibration evidence must remain comparable across HA
                # restarts. ``engine_id`` is a volatile runtime alias;
                # ``storage_key`` is the stable geographic identity.
                "region_key": str(region.storage_key),
                "region_id": str(region.engine_id),
                "source": str(source),
                "product_timestamp": float(timestamp),
                "grid_deg": 0.1,
                "minimum_intensity": 1,
                "evaluation": {
                    "center_lat": float(region.center_lat),
                    "center_lon": float(region.center_lon),
                    "radius_km": float(region.observation_radius_km),
                },
                "coverage_bbox": [
                    float(value) for value in coverage_bbox
                ],
                "coverage_fraction": float(coverage_fraction),
                "fetch_state": "success",
                "artifact": immutable_artifact,
            }
            canonical = json.dumps(
                descriptor,
                allow_nan=False,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
            capture_id = hashlib.sha256(canonical).hexdigest()
            descriptor["capture_id"] = capture_id
            if capture_id not in pending:
                pending[capture_id] = descriptor
                queued += 1
        # Capture admission stays intentionally silent and constant-size on
        # the HA event loop.  Diagnostics are refreshed once after the worker
        # stage has been merged, not once per provider and region.
        hass.data[DOMAIN]["calibration_capture_queue"] = {
            "mode": "worker_artifact_descriptors_v2",
            "pending": len(pending),
        }
        return queued

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

    def _open_meteo_targets() -> dict[str, tuple[float, float]]:
        """Geef uitsluitend beschikbare actuele targetlocaties aan de broker."""
        locations = {}
        for target_id, target in hass.data[DOMAIN].get("targets", {}).items():
            lat = target.get("latitude")
            lon = target.get("longitude")
            if not target.get("available") or lat is None or lon is None:
                continue
            locations[target_id] = (float(lat), float(lon))
        return locations

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
                kmi_region.center_lat, kmi_region.center_lon, hass=hass
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
                hass=hass,
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
                    region.center_lat, region.center_lon, hass=hass
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
        for region in storm_manager.get_all_engines():
            if KmiProviderFactory.supports(
                region.center_lat, region.center_lon, region.observation_radius_km
            ):
                _record_calibration_frame(
                    "kmi", obs, p.last_frame_timestamp,
                    engine_id=region.engine_id,
                )
        hass.bus.async_fire(f"{DOMAIN}_radar_update", {"source": "kmi", "count": len(obs)})
        lat = hass.data[DOMAIN].get("fictieve_lat", home_lat)
        lon = hass.data[DOMAIN].get("fictieve_lon", home_lon)
        intensities = [
            float(item.intensity) for item in obs if item.intensity is not None
        ]
        _queue_provider_cycle_sample("kmi", "belgium", lat, lon, {
            "observations": len(obs),
            "max_intensity": max(intensities) if intensities else 0,
            "mean_intensity": (
                sum(intensities) / len(intensities) if intensities else 0
            ),
        })
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
        try:
            from .worker_radar_client import async_fetch_worker_radar
            obs, overlay, worker_diagnostics, artifact = await async_fetch_worker_radar(
                hass,
                "rainviewer_fetch",
                {"regions": [
                    {
                        "center_lat": region.center_lat,
                        "center_lon": region.center_lon,
                        "horizon_km": region.observation_radius_km,
                    }
                    for region in regions_by_id.values()
                ]},
                provider="rainviewer",
                timeout=90,
                max_age_seconds=20 * 60,
            )
            for provider in providers.values():
                provider.overlay = overlay
                provider.last_worker_artifact = artifact
                provider._last_frame_ts = float(overlay["timestamp"])
                provider._mark_healthy(time.time())
        except Exception as err:
            _LOGGER.warning(
                "Gedeelde RainViewer-workerfout; laatst geldige data blijft actief: %s",
                err,
            )
            worker_diagnostics = {"error": type(err).__name__}
            cached = {
                str(item.radar_cell_id): item
                for provider in providers.values()
                for item in provider._last_observations
            }
            obs = list(cached.values())
            for provider in providers.values():
                provider._mark_unhealthy(f"workerfout: {type(err).__name__}")
        for engine_id in engine_ids:
            provider = providers[engine_id]
            region = regions_by_id.get(engine_id)
            engine_obs = [
                item for item in obs
                if region is not None
                and region.accepts_observation(item.lat, item.lon)
            ]
            provider._last_observations = list(engine_obs)
            observation_counts[engine_id] = sum(
                1 for item in engine_obs
                if region is not None and region.accepts_observation(item.lat, item.lon)
            )
            diagnostics[engine_id] = {
                **provider.diagnostics,
                **worker_diagnostics,
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
        intensities = [
            float(item.intensity) for item in obs if item.intensity is not None
        ]
        _queue_provider_cycle_sample("rainviewer", "all_regions", lat, lon, {
            "observations": len(obs),
            "observations_by_engine": observation_counts,
            "max_intensity": max(intensities) if intensities else 0,
            "mean_intensity": (
                sum(intensities) / len(intensities) if intensities else 0
            ),
        })
        if operational:
            await storm_manager.async_route_observations(obs)
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
            if m > 90:
                return None
            t = _time.time() + m * 60
            c = min(forecast, key=lambda o: abs(o.timestamp - t), default=None)
            return c.intensity if c else 0
        hass.data[DOMAIN]["knmi_intensity_30min"]  = _intens(30)
        hass.data[DOMAIN]["knmi_intensity_60min"]  = _intens(60)
        hass.data[DOMAIN]["knmi_intensity_90min"] = _intens(90)
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
        _queue_provider_cycle_sample("knmi", "netherlands", lat, lon, {
            "current_pixels": len(current),
            "forecast_pixels": len(forecast),
            "intensity_now": hass.data[DOMAIN]["knmi_intensity_now"],
            "intensity_30min": hass.data[DOMAIN]["knmi_intensity_30min"],
            "intensity_60min": hass.data[DOMAIN]["knmi_intensity_60min"],
            "intensity_90min": hass.data[DOMAIN]["knmi_intensity_90min"],
        })
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
        raw_references.extend(
            hass.data[DOMAIN].get(
                "noaa_enterprise_rainrate_observations", []
            )
        )
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
        enterprise_regions = [
            region for region in storm_manager.get_all_engines()
            if (decisions.get(region.engine_id) or {}).get("source")
            in {
                None,
                "rainviewer",
                "noaa_enterprise_rainrate",
                "hsaf_h40b",
                "noaa_goes_rrqpe",
            }
        ]
        enterprise_obs = []
        enterprise = hass.data[DOMAIN].get(
            "noaa_enterprise_rainrate_provider"
        )
        if enterprise is not None and enterprise_regions:
            enterprise_obs = await enterprise.async_fetch(tuple(
                CoverageArea(
                    region.center_lat,
                    region.center_lon,
                    region.observation_radius_km,
                )
                for region in enterprise_regions
            ))
            hass.data[DOMAIN][
                "noaa_enterprise_rainrate_observations"
            ] = enterprise_obs
            hass.data[DOMAIN][
                "noaa_enterprise_rainrate_observation_counts_by_engine"
            ] = {
                region.engine_id: sum(
                    1 for item in enterprise_obs
                    if region.accepts_observation(item.lat, item.lon)
                )
                for region in enterprise_regions
            }
            hass.data[DOMAIN][
                "noaa_enterprise_rainrate_diagnostics"
            ] = enterprise.diagnostics
            if enterprise.healthy and enterprise.overlay:
                for region in enterprise_regions:
                    _record_calibration_frame(
                        "noaa_enterprise_rainrate",
                        enterprise_obs,
                        float(enterprise.overlay["timestamp"]),
                        engine_id=region.engine_id,
                    )
            decisions = _refresh_engine_radar_decisions()
        elif enterprise is not None:
            enterprise.sleep()
            hass.data[DOMAIN][
                "noaa_enterprise_rainrate_diagnostics"
            ] = enterprise.diagnostics
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
        await _route_american_radar_fusion(
            rainviewer_obs, enterprise_obs, goes_obs
        )
        await _route_selected_radar(opera_obs, "opera")
        await _route_selected_radar(rainviewer_obs, "rainviewer")
        await _route_selected_radar(
            enterprise_obs,
            "noaa_enterprise_rainrate",
        )
        await _route_selected_radar(hsaf_obs, "hsaf_h40b")
        await _route_selected_radar(goes_obs, "noaa_goes_rrqpe")
        await _route_selected_radar(
            hass.data[DOMAIN].get("last_kmi_observations", []),
            "kmi",
        )
        await _route_selected_radar(
            hass.data[DOMAIN].get("knmi_current", []),
            "knmi",
        )
        await _refresh_radar_overlays(decisions)

        hass.bus.async_fire(f"{DOMAIN}_radar_source_update", {
            "source": hass.data[DOMAIN].get("active_radar_source"),
            "reason": hass.data[DOMAIN].get("radar_source_reason"),
            "engines": decisions,
        })

    async def _poll_radar_comparison(now=None):
        """Keep national products observable without feeding the OFE."""
        _sync_region_radar_providers()
        # KMI en KNMI zijn onafhankelijke kalibratiebronnen. Serieel pollen
        # stapelde bij netwerkproblemen hun volledige wachttijden op, waardoor
        # deze fase haar harde limiet van 60 seconden kon bereiken.
        await asyncio.gather(_poll_kmi(), _poll_knmi())

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
        # Bewaar iedere rasterbron die via de provider-lifecycle binnenkomt.
        # Geen whitelist: nieuw geregistreerde radarproviders worden hierdoor
        # automatisch op hun oorspronkelijke gedecodeerde resolutie bewaard.
        # Punt-nowcasts zonder overlay zijn geen radarframe en worden niet als
        # kunstmatig raster in de kalibratiedatabase geschreven.
        for provider_id, observations in results.items():
            overlay = provider_lifecycle.overlay(provider_id)
            if not overlay:
                continue
            overlay_timestamp = overlay.get("timestamp")
            if overlay_timestamp is None:
                continue
            _record_calibration_frame(
                provider_id,
                observations,
                float(overlay_timestamp),
            )
        decisions = _refresh_engine_radar_decisions()
        for provider_id in ("dwd_radolan", "met_office_radar", "meteofrance_radar", "meteolux", "dpc_radar", "aemet_radar"):
            if provider_id in results:
                await _route_selected_radar(
                    results[provider_id], provider_id
                )
        hass.data[DOMAIN]["provider_lifecycle_diagnostics"] = (
            provider_lifecycle.diagnostics()
        )

    async def _refresh_radar_overlays(decisions):
        """Laat de worker gekozen bronpixels begrenzen en normaliseren."""
        overlays = {}
        artifact_selections = {}
        changed_frames = {}
        provider_artifacts = hass.data[DOMAIN].get(
            "provider_worker_artifacts", {}
        )
        shared = {
            "kmi": hass.data[DOMAIN].get("kmi_provider"),
            "knmi": hass.data[DOMAIN].get("knmi_provider"),
            "hsaf_h40b": hass.data[DOMAIN].get("hsaf_h40b_provider"),
            "noaa_enterprise_rainrate": hass.data[DOMAIN].get(
                "noaa_enterprise_rainrate_provider"
            ),
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
                artifact = provider_artifacts.get(source)
                if isinstance(artifact, dict) and artifact.get("artifact_path"):
                    artifact_selections[region.engine_id] = {
                        "provider": source,
                        "artifact": artifact,
                        "bounds": list(_region_bounds(region)),
                    }
        started_at = time.time()
        before = _system_performance_snapshot()
        outcome = "ok"
        try:
            if overlays and len(artifact_selections) == len(overlays):
                frames, bounded_overlays = await hass.data[DOMAIN][
                    "radar_frame_worker"
                ].async_process_provider_artifacts(artifact_selections)
                transport_mode = "worker_artifact_chain"
            else:
                regions = {
                    region.engine_id: list(_region_bounds(region))
                    for region in storm_manager.get_all_engines()
                    if region.engine_id in overlays
                }
                frames, bounded_overlays = await hass.data[DOMAIN][
                    "radar_frame_worker"
                ].async_process(overlays, regions)
                transport_mode = "legacy_overlay_artifact"
            for engine_id, frame in frames.items():
                hass.data[DOMAIN]["pysteps_frame_history"].add_frame(
                    engine_id, frame
                )
                changed_frames.setdefault(engine_id, []).append(frame)
            hass.data[DOMAIN]["radar_overlays_by_engine"] = bounded_overlays
            hass.data[DOMAIN]["radar_frame_worker_diagnostics"] = {
                "status": "operationeel",
                "mode": "worker_first",
                "transport": transport_mode,
                "fallback_active": False,
                "regions": len(frames),
            }
        except Exception as err:
            outcome = "error"
            # Geen lokale CPU-fallback: behoud de laatst gevalideerde laag en
            # laat de volgende cyclus opnieuw proberen.
            hass.data[DOMAIN]["radar_frame_worker_diagnostics"] = {
                "status": "transportfout",
                "mode": "worker_first",
                "fallback_active": True,
                "error": f"{type(err).__name__}: {err}",
            }
            _LOGGER.warning("Regionale radarworker niet beschikbaar: %s", err)
        finally:
            finished_at = time.time()
            _queue_system_performance_sample(
                hass.data[DOMAIN].get(
                    "current_provider_cycle_id", "outside_cycle"
                ),
                "raster_normalization:worker",
                started_at,
                finished_at,
                outcome,
                before,
                _system_performance_snapshot(),
            )
        frame_history = hass.data[DOMAIN]["pysteps_frame_history"]
        pysteps_input = frame_history.diagnostics()
        preflight = {}
        analysis_limit = asyncio.Semaphore(2)

        async def _evaluate_region(region):
            frames = frame_history.frames(region.engine_id)
            started_at = time.time()
            before = _system_performance_snapshot()
            outcome = "ok"
            try:
                if (
                    hass.data[DOMAIN].get("analysis_mode")
                    == "thermal_throttle"
                    and _system_reports_hardware_throttling()
                ):
                    outcome = "thermal_throttled"
                    result = {
                        "status": "thermal_throttled",
                        "experimental": True,
                        "feeds_detection": False,
                        "frames_used": len(frames),
                        "method": "bounded_grid_match_v1",
                        "confidence": "insufficient",
                        "translation_km_per_hour": None,
                        "heading_deg": None,
                        "verification_iou": None,
                        "motion_match_iou": None,
                        "vector_consistency_deg": None,
                        "wet_cell_ratio": None,
                        "centroid_translation_km_per_hour": None,
                        "centroid_heading_deg": None,
                        "reason": "hardware_or_os_reported_throttling",
                    }
                else:
                    async with analysis_limit:
                        worker_diagnostic = {
                            "state": "error",
                            "mode": "worker_first",
                            "source": "worker",
                            "fallback_active": False,
                        }
                        try:
                            from .worker_bridge import async_request_worker_job

                            tasks = hass.data[DOMAIN].setdefault(
                                "motion_worker_tasks", {}
                            )
                            results = hass.data[DOMAIN].setdefault(
                                "motion_worker_results", {}
                            )
                            task = tasks.get(region.engine_id)
                            if task is not None and task.done():
                                try:
                                    results[region.engine_id] = (
                                        task.result().get("result")
                                    )
                                finally:
                                    tasks.pop(region.engine_id, None)
                                task = None
                            if task is None:
                                tasks[region.engine_id] = (
                                    hass.async_create_background_task(
                                        async_request_worker_job(
                                            hass,
                                            "motion_preflight",
                                            {
                                                "region_id": region.engine_id,
                                                "frames": [
                                                    frame.to_dict()
                                                    for frame in frames[-4:]
                                                ],
                                                "targets": [
                                                    {
                                                        "target_id": target_id,
                                                        "latitude": target[
                                                            "latitude"
                                                        ],
                                                        "longitude": target[
                                                            "longitude"
                                                        ],
                                                    }
                                                    for target_id, target in
                                                    hass.data[DOMAIN].get(
                                                        "targets", {}
                                                    ).items()
                                                    if target.get("available")
                                                    and target.get(
                                                        "latitude"
                                                    ) is not None
                                                    and target.get(
                                                        "longitude"
                                                    ) is not None
                                                ],
                                            },
                                            timeout=20,
                                        ),
                                        (
                                            f"{DOMAIN}_motion_worker_"
                                            f"{region.engine_id}"
                                        ),
                                    )
                                )
                            result = results.get(region.engine_id)
                            if (
                                not isinstance(result, dict)
                                or result.get("method")
                                != "bounded_grid_match_v1"
                                or int(result.get("frames_used", -1))
                                != min(3, len(frames))
                                or not isinstance(result.get("status"), str)
                            ):
                                raise ValueError(
                                    "nog geen geldig resultaat bewegingsworker"
                                )
                            worker_diagnostic["state"] = "active"
                        except (
                            asyncio.TimeoutError,
                            KeyError,
                            RuntimeError,
                            TypeError,
                            ValueError,
                        ) as err:
                            outcome = "worker_unavailable"
                            worker_diagnostic.update({
                                "state": "worker_onbeschikbaar",
                                "source": "bewaard_workerresultaat",
                                "fallback_active": False,
                                "error": type(err).__name__,
                            })
                            result = results.get(region.engine_id)
                            if not isinstance(result, dict):
                                result = {
                                    "status": "worker_unavailable",
                                    "experimental": True,
                                    "feeds_detection": False,
                                    "frames_used": len(frames),
                                    "method": "worker_required_v1",
                                    "confidence": "insufficient",
                                    "translation_km_per_hour": None,
                                    "heading_deg": None,
                                    "verification_iou": None,
                                    "motion_match_iou": None,
                                    "vector_consistency_deg": None,
                                    "wet_cell_ratio": None,
                                    "centroid_translation_km_per_hour": None,
                                    "centroid_heading_deg": None,
                                    "reason": "worker_unavailable",
                                }
                        result = {
                            **result,
                            "worker": worker_diagnostic,
                        }
            except Exception:
                outcome = "error"
                raise
            finally:
                finished_at = time.time()
                _queue_system_performance_sample(
                    hass.data[DOMAIN].get(
                        "current_provider_cycle_id", "outside_cycle"
                    ),
                    f"pysteps_preflight:{region.engine_id}",
                    started_at,
                    finished_at,
                    outcome,
                    before,
                    _system_performance_snapshot(),
                )
            return region, frames, result

        evaluated = await asyncio.gather(*(
            _evaluate_region(region)
            for region in storm_manager.get_all_engines()
        ))
        for region, frames, result in evaluated:
            preflight[region.engine_id] = result
            if frames:
                latest = frames[-1]
                hass.data[DOMAIN][
                    "pysteps_preflight_samples_pending"
                ].append({
                    "region_key": region.storage_key,
                    "source": latest.source,
                    "nominal_minute": latest.nominal_timestamp // 60,
                    "evaluated_at": time.time(),
                    **{
                        key: result.get(key) for key in (
                            "status", "frames_used",
                            "translation_km_per_hour", "heading_deg",
                            "verification_iou", "method", "confidence",
                            "motion_match_iou", "vector_consistency_deg",
                            "wet_cell_ratio",
                            "centroid_translation_km_per_hour",
                            "centroid_heading_deg", "reason",
                        )
                    },
                })
        pysteps_input["preflight"] = preflight

        tracking_results = dict(
            hass.data[DOMAIN].get("tracking_worker", {})
        )
        pysteps_input["tracking_worker"] = dict(tracking_results)
        hass.data[DOMAIN]["tracking_worker"] = dict(tracking_results)
        hass.data[DOMAIN]["pysteps_input"] = pysteps_input
        calibration_store = hass.data[DOMAIN]["radar_calibration_store"]

        def _serialize_and_save_frame_history():
            """Bewaar uitsluitend nieuwe frames, niet telkens de hele cache."""
            if not changed_frames:
                return 0
            return calibration_store.save_pysteps_frames(
                {
                    region_id: [frame.to_dict() for frame in frames]
                    for region_id, frames in changed_frames.items()
                }
            )

        await hass.data[DOMAIN]["radar_frame_worker"].async_run_io(
            _serialize_and_save_frame_history
        )
        hass.bus.async_fire(f"{DOMAIN}_provider_lifecycle_update")
        hass.bus.async_fire(f"{DOMAIN}_radar_source_update", {"engines": decisions})

    async def _poll_netatmo(now=None):
        from .providers.netatmo import NetatmoProvider
        from .engine.observation import (
            Observation,
            ObservationType,
            observation_is_wet,
        )

        def _ground_observations(result, default_source):
            def optional_float(value):
                return float(value) if value is not None else None

            observations = []
            for raw in result.get("observations", []):
                try:
                    observations.append(Observation(
                        obs_type=ObservationType.RAIN,
                        lat=float(raw["lat"]),
                        lon=float(raw["lon"]),
                        timestamp=float(raw["timestamp"]),
                        received_at=(
                            float(raw["received_at"])
                            if raw.get("received_at") is not None else None
                        ),
                        rain_mm=optional_float(raw.get("rain_mm")),
                        rain_5min=optional_float(raw.get("rain_5min")),
                        rain_60min=optional_float(raw.get("rain_60min")),
                        station_id=str(raw.get("station_id", "")),
                        rain_capable=bool(raw.get("rain_capable", False)),
                        precipitation_present=raw.get(
                            "precipitation_present"
                        ),
                        quantitative_precipitation=bool(
                            raw.get("quantitative_precipitation", True)
                        ),
                        wind_speed=raw.get("wind_speed"),
                        wind_angle=raw.get("wind_angle"),
                        gust_speed=raw.get("gust_speed"),
                        pressure=raw.get("pressure"),
                        temperature=raw.get("temperature"),
                        humidity=raw.get("humidity"),
                        source=str(raw.get("source") or default_source),
                        raw_text=raw.get("raw_text"),
                        raw_payload=raw.get("raw_payload"),
                        measurement_kind=raw.get("measurement_kind"),
                        measurement_unit=raw.get("measurement_unit"),
                        interval_minutes=raw.get("interval_minutes"),
                        parser_version=raw.get("parser_version"),
                        interpretation_reason=raw.get(
                            "interpretation_reason"
                        ),
                        quality_flags=tuple(raw.get("quality_flags") or ()),
                    ))
                except (KeyError, TypeError, ValueError):
                    continue
            return observations

        def _synoptic_applicable(latitude, longitude):
            """Synoptic heeft zijn bruikbare publieke dekking in Noord-Amerika."""
            return (
                15.0 <= float(latitude) <= 75.0
                and -170.0 <= float(longitude) <= -50.0
            )

        def _cached_worker_netatmo(
            query_key, latitude, longitude, query_kind
        ):
            """Start workerwerk los van de providerfase en geef cache direct terug."""
            cache = hass.data[DOMAIN].setdefault(
                "netatmo_worker_cache", {}
            )
            tasks = hass.data[DOMAIN].setdefault(
                "netatmo_worker_tasks", {}
            )
            current = cache.get(query_key)
            cache_age = (
                time.time() - float(current["received_at"])
                if current else None
            )
            if (
                query_key not in tasks
                and (cache_age is None or cache_age >= 5 * 60)
            ):
                async def _refresh():
                    try:
                        from .worker_bridge import async_request_worker_job

                        result = await async_request_worker_job(
                            hass,
                            "netatmo_fetch",
                            {
                                "query_key": query_key,
                                "query_kind": query_kind,
                                "latitude": float(latitude),
                                "longitude": float(longitude),
                                # Secrets belong to the add-on environment and
                                # never travel through MQTT job payloads.
                                "credential_ref": "worker_environment",
                            },
                            timeout=20,
                        )
                        if result.get("method") != "netatmo_worker_20km_v1":
                            raise ValueError("ongeldig Netatmo-workerresultaat")
                        cache[query_key] = {
                            "received_at": time.time(),
                            "result": result,
                        }
                    except (
                        asyncio.TimeoutError,
                        RuntimeError,
                        TypeError,
                        ValueError,
                    ) as err:
                        hass.data[DOMAIN]["netatmo_worker_error"] = {
                            "query_key": query_key,
                            "error": type(err).__name__,
                            "timestamp": time.time(),
                        }
                    finally:
                        tasks.pop(query_key, None)

                tasks[query_key] = hass.async_create_background_task(
                    _refresh(),
                    f"{DOMAIN}_netatmo_worker_{query_key}",
                )
            if (
                current
                and time.time() - float(current["received_at"])
                <= 15 * 60
            ):
                return _ground_observations(current["result"], "netatmo")
            return None

        def _cached_worker_synoptic(
            query_key, latitude, longitude, query_kind, radius_km
        ):
            """Start een begrensde Synoptic-query zonder de HA-cyclus te blokkeren."""
            cache = hass.data[DOMAIN].setdefault(
                "synoptic_worker_cache", {}
            )
            tasks = hass.data[DOMAIN].setdefault(
                "synoptic_worker_tasks", {}
            )
            current = cache.get(query_key)
            cache_age = (
                time.time() - float(current["received_at"])
                if current else None
            )
            if (
                query_key not in tasks
                and (cache_age is None or cache_age >= 5 * 60)
            ):
                async def _refresh():
                    try:
                        from .worker_bridge import async_request_worker_job

                        result = await async_request_worker_job(
                            hass,
                            "synoptic_fetch",
                            {
                                "query_key": query_key,
                                "query_kind": query_kind,
                                "latitude": float(latitude),
                                "longitude": float(longitude),
                                "radius_km": float(radius_km),
                            },
                            timeout=20,
                        )
                        if result.get("method") != "synoptic_worker_v1":
                            raise ValueError("ongeldig Synoptic-workerresultaat")
                        cache[query_key] = {
                            "received_at": time.time(),
                            "result": result,
                        }
                    except (
                        asyncio.TimeoutError,
                        RuntimeError,
                        TypeError,
                        ValueError,
                    ) as err:
                        hass.data[DOMAIN]["synoptic_worker_error"] = {
                            "query_key": query_key,
                            "error": type(err).__name__,
                            "timestamp": time.time(),
                        }
                    finally:
                        tasks.pop(query_key, None)

                tasks[query_key] = hass.async_create_background_task(
                    _refresh(),
                    f"{DOMAIN}_synoptic_worker_{query_key}",
                )
            if (
                current
                and time.time() - float(current["received_at"])
                <= 15 * 60
            ):
                return (
                    _ground_observations(current["result"], "synoptic"),
                    dict(current["result"]),
                )
            return None, None

        def _cached_worker_metar(
            query_key, latitude, longitude, query_kind, radius_km
        ):
            """Start een operationele METAR-query en gebruik de laatste goede cache."""
            cache = hass.data[DOMAIN].setdefault("metar_worker_cache", {})
            tasks = hass.data[DOMAIN].setdefault("metar_worker_tasks", {})
            current = cache.get(query_key)
            cache_age = (
                time.time() - float(current["received_at"])
                if current else None
            )
            if (
                query_key not in tasks
                and (cache_age is None or cache_age >= 60)
            ):
                async def _refresh():
                    try:
                        from .worker_bridge import async_request_worker_job

                        result = await async_request_worker_job(
                            hass,
                            "metar_fetch",
                            {
                                "query_key": query_key,
                                "query_kind": query_kind,
                                "latitude": float(latitude),
                                "longitude": float(longitude),
                                "radius_km": float(radius_km),
                            },
                            timeout=35,
                        )
                        if result.get("method") != "metar_worker_v1":
                            raise ValueError("ongeldig METAR-workerresultaat")
                        cache[query_key] = {
                            "received_at": time.time(),
                            "result": result,
                        }
                        hass.data[DOMAIN].pop("metar_worker_error", None)
                    except (
                        asyncio.TimeoutError, RuntimeError, TypeError, ValueError
                    ) as err:
                        hass.data[DOMAIN]["metar_worker_error"] = {
                            "query_key": query_key,
                            "error": type(err).__name__,
                            "timestamp": time.time(),
                        }
                    finally:
                        tasks.pop(query_key, None)

                tasks[query_key] = hass.async_create_background_task(
                    _refresh(), f"{DOMAIN}_metar_worker_{query_key}"
                )
            if (
                current
                and time.time() - float(current["received_at"]) <= 15 * 60
            ):
                return (
                    _ground_observations(current["result"], "metar"),
                    dict(current["result"]),
                )
            return None, None

        # Houd de legacy HA-providerobjecten alleen voor backwards-compatible
        # drukstate bij. Zij mogen de worker-owned Netatmo-ingest niet poorten:
        # OAuth-secrets wonen voortaan uitsluitend in de workeromgeving, zodat
        # ``netatmo_providers_by_engine`` in HA terecht leeg kan zijn.
        _sync_region_netatmo_providers()
        regions = {
            region.engine_id: region
            for region in storm_manager.get_all_engines()
        }
        if not regions:
            return
        engine_ids = tuple(regions)
        fetched = []
        synoptic_fetched = {}
        synoptic_results = {}
        metar_fetched = {}
        metar_results = {}
        for engine_id in engine_ids:
            region = regions[engine_id]
            worker_observations = _cached_worker_netatmo(
                f"local:{region.storage_key}",
                region.center_lat,
                region.center_lon,
                "local",
            )
            if worker_observations is None:
                # The worker is the sole OAuth owner. Starting the legacy HA
                # provider here could refresh the same rotating token at the
                # same time and invalidate one of both requests. Preserve the
                # previous ground observations during the bounded worker
                # refresh window instead.
                previous = hass.data[DOMAIN].get(
                    "netatmo_observations_by_engine", {}
                ).get(engine_id, [])
                fetched.append([
                    item for item in previous
                    if time.time() - float(item.timestamp) <= 15 * 60
                ])
            else:
                fetched.append(worker_observations or [])
            if _synoptic_applicable(region.center_lat, region.center_lon):
                synoptic_obs, synoptic_result = _cached_worker_synoptic(
                    f"local:{region.storage_key}:50",
                    region.center_lat,
                    region.center_lon,
                    "local",
                    50.0,
                )
                if synoptic_obs is None:
                    synoptic_obs = [
                        item for item in hass.data[DOMAIN].get(
                            "synoptic_observations_by_engine", {}
                        ).get(engine_id, [])
                        if time.time() - float(item.timestamp) <= 15 * 60
                    ]
                synoptic_fetched[engine_id] = synoptic_obs
                if synoptic_result is not None:
                    synoptic_results[engine_id] = synoptic_result
            else:
                synoptic_fetched[engine_id] = []
            metar_obs, metar_result = _cached_worker_metar(
                f"local:{region.storage_key}:50",
                region.center_lat,
                region.center_lon,
                "local",
                50.0,
            )
            if metar_obs is None:
                metar_obs = [
                    item for item in hass.data[DOMAIN].get(
                        "metar_observations_by_engine", {}
                    ).get(engine_id, [])
                    if time.time() - float(item.timestamp) <= 2 * 60 * 60
                ]
            metar_fetched[engine_id] = metar_obs
            if metar_result is not None:
                metar_results[engine_id] = metar_result
        observations_by_engine = {}
        netatmo_observations_by_engine = {}
        trends_by_engine = {}
        all_observations = {}
        all_raining = {}
        for engine_id, result in zip(engine_ids, fetched):
            if isinstance(result, Exception):
                _LOGGER.error(
                    "Netatmo-poll voor %s mislukt: %s", engine_id, result
                )
                result = []
            netatmo_obs = list(result)
            synoptic_obs = list(synoptic_fetched.get(engine_id, []))
            metar_obs = list(metar_fetched.get(engine_id, []))
            obs = netatmo_obs + synoptic_obs + metar_obs
            netatmo_observations_by_engine[engine_id] = netatmo_obs
            observations_by_engine[engine_id] = obs
            tracker = _pressure_tracker_for_region(regions[engine_id])
            trends_by_engine[engine_id] = tracker.update(obs)
            for item in obs:
                key = (
                    f"{item.source}:"
                    f"{item.station_id or f'{item.lat:.5f},{item.lon:.5f}'}"
                )
                all_observations[key] = item
                if observation_is_wet(item):
                    all_raining[key] = item
            region = regions[engine_id]
            raining_values = [
                float(item.rain_mm or 0.0) for item in obs
                if observation_is_wet(item)
            ]
            netatmo_raining_values = [
                float(item.rain_mm or 0.0) for item in netatmo_obs
                if observation_is_wet(item)
            ]
            _queue_provider_cycle_sample(
                "netatmo", engine_id, region.center_lat, region.center_lon, {
                    "stations": len(netatmo_obs),
                    "raining_stations": len(netatmo_raining_values),
                    "max_rain_mm_h": (
                        max(netatmo_raining_values)
                        if netatmo_raining_values else 0
                    ),
                    "mean_rain_mm_h": (
                        sum(netatmo_raining_values)
                        / len(netatmo_raining_values)
                        if netatmo_raining_values else 0
                    ),
                    "station_ids": sorted(
                        str(item.station_id) for item in netatmo_obs
                        if item.station_id
                    ),
                },
            )
            if synoptic_obs:
                synoptic_raining = [
                    float(item.rain_mm) for item in synoptic_obs
                    if observation_is_wet(item) and item.rain_mm is not None
                ]
                _queue_provider_cycle_sample(
                    "synoptic",
                    engine_id,
                    region.center_lat,
                    region.center_lon,
                    {
                        "query_kind": "local",
                        "radius_km": 50.0,
                        "stations": len(synoptic_obs),
                        "raining_stations": len(synoptic_raining),
                        "max_rain_mm_h": (
                            max(synoptic_raining) if synoptic_raining else 0
                        ),
                        "cache": (
                            synoptic_results.get(engine_id, {}).get("cache")
                        ),
                    },
                )
            if metar_obs:
                _queue_provider_cycle_sample(
                    "metar",
                    engine_id,
                    region.center_lat,
                    region.center_lon,
                    {
                        "query_kind": "local",
                        "radius_km": 50.0,
                        "stations": len(metar_obs),
                        "raining_stations": sum(
                            observation_is_wet(item) for item in metar_obs
                        ),
                        "station_ids": sorted(
                            str(item.station_id) for item in metar_obs
                            if item.station_id
                        ),
                        "observation_age_seconds": sorted(
                            round(max(0.0, time.time() - item.timestamp), 1)
                            for item in metar_obs
                        ),
                        "cache": metar_results.get(engine_id, {}).get("cache"),
                    },
                )
            for item in obs:
                observed_minute = int(float(item.timestamp) // 60)
                station_id = str(
                    item.station_id or f"{item.lat:.5f},{item.lon:.5f}"
                )
                hass.data[DOMAIN]["ground_observations_pending"].append({
                    "record_key": (
                        f"{item.source}:{engine_id}:{station_id}:"
                        f"{observed_minute}"
                    ),
                    "provider": str(item.source),
                    "region_key": engine_id,
                    "station_id": station_id,
                    "observed_at": float(item.timestamp),
                    "received_at": item.received_at,
                    "latitude": float(item.lat),
                    "longitude": float(item.lon),
                    "precipitation_present": item.precipitation_present,
                    "quantitative_precipitation": (
                        item.quantitative_precipitation
                    ),
                    "rain_mm": item.rain_mm,
                    "rain_5min": item.rain_5min,
                    "rain_60min": item.rain_60min,
                    "measurement_kind": item.measurement_kind,
                    "measurement_unit": item.measurement_unit,
                    "interval_minutes": item.interval_minutes,
                    "parser_version": item.parser_version,
                    "interpretation_reason": item.interpretation_reason,
                    "quality_flags": item.quality_flags,
                    "raw_text": item.raw_text,
                    "raw_payload": item.raw_payload,
                })
        obs = list(all_observations.values())
        raining = list(all_raining.values())
        hass.data[DOMAIN][
            "netatmo_observations_by_engine"
        ] = netatmo_observations_by_engine
        hass.data[DOMAIN]["ground_observations_by_engine"] = observations_by_engine
        hass.data[DOMAIN]["synoptic_observations_by_engine"] = synoptic_fetched
        hass.data[DOMAIN]["metar_observations_by_engine"] = metar_fetched
        hass.data[DOMAIN]["netatmo_pressure_trends_by_engine"] = trends_by_engine
        all_netatmo = {
            str(item.station_id or f"{item.lat:.5f},{item.lon:.5f}"): item
            for items in netatmo_observations_by_engine.values()
            for item in items
        }
        netatmo_obs = list(all_netatmo.values())
        netatmo_raining = [
            item for item in netatmo_obs
            if observation_is_wet(item)
        ]
        hass.data[DOMAIN]["last_netatmo_observations"] = netatmo_obs
        hass.data[DOMAIN]["netatmo_rain_count"] = len(netatmo_raining)
        hass.data[DOMAIN]["netatmo_station_count"] = len(netatmo_obs)
        synoptic_all = [
            item
            for items in synoptic_fetched.values()
            for item in items
        ]
        synoptic_wet = [
            item for item in synoptic_all
            if observation_is_wet(item)
        ]
        hass.data[DOMAIN]["synoptic_diagnostics"] = {
            "status": (
                next(iter(synoptic_results.values())).get("status", "operationeel")
                if synoptic_results
                else "fout"
                if hass.data[DOMAIN].get("synoptic_worker_error")
                else "wacht_op_eerste_meting"
            ),
            "station_count": len(synoptic_all),
            "fresh_station_count": len(synoptic_all),
            "wet_station_count": len(synoptic_wet),
            "local_radius_km": 50.0,
            "roaming_radius_km": 50.0,
            "regions": {
                engine_id: {
                    "stations": len(synoptic_fetched.get(engine_id, [])),
                    "wet_stations": sum(
                        observation_is_wet(item)
                        for item in synoptic_fetched.get(engine_id, [])
                    ),
                    "cache": synoptic_results.get(engine_id, {}).get("cache"),
                    "queried_at": synoptic_results.get(engine_id, {}).get(
                        "queried_at"
                    ),
                }
                for engine_id in engine_ids
                if _synoptic_applicable(
                    regions[engine_id].center_lat,
                    regions[engine_id].center_lon,
                )
            },
        }
        metar_all = [
            item for items in metar_fetched.values() for item in items
        ]
        hass.data[DOMAIN]["metar_diagnostics"] = {
            "status": (
                "operationeel"
                if metar_results
                else "fout"
                if hass.data[DOMAIN].get("metar_worker_error")
                else "wacht_op_eerste_meting"
            ),
            "station_count": len(metar_all),
            "wet_station_count": sum(
                observation_is_wet(item) for item in metar_all
            ),
            "regions": {
                engine_id: {
                    "stations": len(metar_fetched.get(engine_id, [])),
                    "wet_stations": sum(
                        observation_is_wet(item)
                        for item in metar_fetched.get(engine_id, [])
                    ),
                    "cache": metar_results.get(engine_id, {}).get("cache"),
                    "queried_at": metar_results.get(engine_id, {}).get("queried_at"),
                }
                for engine_id in engine_ids
            },
        }
        home_region = storm_manager.get_engine_for_target("zone.home")
        hass.data[DOMAIN]["netatmo_pressure_trend"] = (
            trends_by_engine.get(home_region.engine_id, {}) if home_region else {}
        )
        storms_by_engine = {
            engine_id: regions[engine_id].storm_engine.get_active_storms()
            for engine_id in engine_ids
            if engine_id in regions
        }
        probe_manager = hass.data[DOMAIN]["virtual_probe_manager"]
        roaming_target = probe_manager.choose_roaming_target(
            storms_by_engine=storms_by_engine,
            observations_by_engine=observations_by_engine,
        )
        roaming_observations = list(
            hass.data[DOMAIN].get("netatmo_roaming_observations", [])
        )
        synoptic_roaming_observations = list(
            hass.data[DOMAIN].get("synoptic_roaming_observations", [])
        )
        roaming_status = {
            "enabled": roaming_target is not None,
            "queried": False,
            "target": roaming_target,
            "station_count": (
                len(roaming_observations)
                + len(synoptic_roaming_observations)
            ),
            "query_interval_minutes": 10,
            "providers": {},
        }
        now_ts = time.time()
        if (
            roaming_target is not None
            and now_ts - float(
                hass.data[DOMAIN].get("netatmo_roaming_last_query", 0.0)
            ) >= 10 * 60
        ):
            center = (
                float(roaming_target["latitude"]),
                float(roaming_target["longitude"]),
            )
            roaming_key = f"roaming:{center[0]:.2f},{center[1]:.2f}"
            if (
                hass.data[DOMAIN].get("netatmo_roaming_query_key")
                != roaming_key
            ):
                roaming_observations = []
                hass.data[DOMAIN]["netatmo_roaming_observations"] = []
                hass.data[DOMAIN]["netatmo_roaming_query_key"] = roaming_key
            try:
                worker_roaming = _cached_worker_netatmo(
                    roaming_key,
                    center[0],
                    center[1],
                    "roaming",
                )
                if worker_roaming is not None:
                    roaming_observations = list(worker_roaming)
                hass.data[DOMAIN][
                    "netatmo_roaming_observations"
                ] = roaming_observations
                roaming_status["queried"] = worker_roaming is not None
                roaming_status["providers"]["netatmo"] = {
                    "queried": worker_roaming is not None,
                    "stations": len(roaming_observations),
                    "radius_km": 20.0,
                }
                if _synoptic_applicable(center[0], center[1]):
                    synoptic_key = (
                        f"roaming:{center[0]:.2f},{center[1]:.2f}:50"
                    )
                    synoptic_roaming, synoptic_roaming_result = (
                        _cached_worker_synoptic(
                            synoptic_key,
                            center[0],
                            center[1],
                            "roaming",
                            50.0,
                        )
                    )
                    if synoptic_roaming is not None:
                        synoptic_roaming_observations = list(
                            synoptic_roaming
                        )
                        hass.data[DOMAIN][
                            "synoptic_roaming_observations"
                        ] = synoptic_roaming_observations
                    roaming_status["providers"]["synoptic"] = {
                        "queried": synoptic_roaming is not None,
                        "stations": len(synoptic_roaming_observations),
                        "wet_stations": sum(
                            observation_is_wet(item)
                            for item in synoptic_roaming_observations
                        ),
                        "radius_km": 50.0,
                        "cache": (
                            synoptic_roaming_result.get("cache")
                            if synoptic_roaming_result else None
                        ),
                    }
                    roaming_status["queried"] = (
                        roaming_status["queried"]
                        or synoptic_roaming is not None
                    )
                    if synoptic_roaming_result is not None:
                        _queue_provider_cycle_sample(
                            "synoptic",
                            f"roaming:{roaming_target['region_id']}",
                            center[0],
                            center[1],
                            {
                                "query_kind": "roaming",
                                "radius_km": 50.0,
                                "stations": len(
                                    synoptic_roaming_observations
                                ),
                                "raining_stations": sum(
                                    observation_is_wet(item)
                                    for item in synoptic_roaming_observations
                                ),
                                "cache": synoptic_roaming_result.get("cache"),
                                "storm_id": roaming_target.get("storm_id"),
                            },
                        )
                roaming_status["station_count"] = (
                    len(roaming_observations)
                    + len(synoptic_roaming_observations)
                )
            except Exception as err:
                roaming_status["error"] = str(err)
                _LOGGER.warning("Reizende Netatmo-verkenner mislukt: %s", err)
            finally:
                if roaming_status["queried"]:
                    hass.data[DOMAIN]["netatmo_roaming_last_query"] = now_ts

        # De evaluatie gebeurt in een eigen providerfase na deze poll. Zo kan
        # een trage Netatmo- of Open-Meteo-call de geplande controle niet
        # annuleren. Bewaar hier alleen de laatste roamingdiagnostiek.
        hass.data[DOMAIN]["virtual_probe_roaming_status"] = roaming_status
        synoptic_diagnostics = hass.data[DOMAIN].get(
            "synoptic_diagnostics", {}
        )
        synoptic_diagnostics["roaming"] = roaming_status.get(
            "providers", {}
        ).get("synoptic", {
            "queried": False,
            "stations": len(synoptic_roaming_observations),
            "radius_km": 50.0,
        })
        hass.data[DOMAIN]["synoptic_diagnostics"] = synoptic_diagnostics
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
        hass.bus.async_fire(
            f"{DOMAIN}_synoptic_update",
            hass.data[DOMAIN]["synoptic_diagnostics"],
        )
        hass.bus.async_fire(
            f"{DOMAIN}_metar_update",
            hass.data[DOMAIN]["metar_diagnostics"],
        )
        # Alleen natte stations naar OFE
        await storm_manager.async_route_observations(raining)

    async def _update_virtual_probes_from_cache(now=None):
        """Evalueer verkenners onafhankelijk van provider-netwerktijd."""
        probe_manager = hass.data[DOMAIN]["virtual_probe_manager"]
        observed_at = time.time()
        storms_by_engine = {
            region.engine_id: region.storm_engine.get_active_storms()
            for region in storm_manager.get_all_engines()
        }
        probe_observations = {
            engine_id: list(items)
            for engine_id, items in hass.data[DOMAIN].get(
                "ground_observations_by_engine", {}
            ).items()
        }
        roaming_status = dict(hass.data[DOMAIN].get(
            "virtual_probe_roaming_status", {}
        ))
        roaming_target = roaming_status.get("target")
        if roaming_target is not None:
            probe_observations.setdefault(
                str(roaming_target["region_id"]), []
            ).extend(
                hass.data[DOMAIN].get("netatmo_roaming_observations", [])
            )
            probe_observations[str(roaming_target["region_id"])].extend(
                hass.data[DOMAIN].get(
                    "synoptic_roaming_observations", []
                )
            )
        active_probes = [
            {
                key: probe[key]
                for key in (
                    "probe_id", "storm_id", "region_id", "station_id",
                    "latitude", "longitude", "created_at", "expected_at",
                    "eta_minutes", "passage", "pass_distance_km",
                    "motion_confidence", "radar_source",
                )
            }
            for probe in probe_manager.diagnostics(observed_at).get("probes", [])
        ]
        active_probes = _trim_virtual_probe_active_probes(
            active_probes,
            observed_at=observed_at,
            max_probes=VIRTUAL_PROBE_WORKER_MAX_RESTORED_ACTIVE_PROBES,
        )
        diagnostics_before = probe_manager.diagnostics(observed_at)
        history = {
            "completed": diagnostics_before.get("completed_count", 0),
            "hits": diagnostics_before.get("confirmed_count", 0),
            "misses": diagnostics_before.get("dry_count", 0),
            "unverifiable": diagnostics_before.get(
                "unverifiable_count", 0
            ),
            "cancelled": diagnostics_before.get("cancelled_count", 0),
            "recent_outcomes": diagnostics_before.get(
                "recent_outcomes", []
            ),
        }
        payload = {
            "observed_at": observed_at,
            "active_probes": active_probes,
            "allow_trimmed_payload": True,
            "history": history,
            "states_by_region": {
                region.engine_id:
                    region.storm_engine.export_tracking_state()
                for region in storm_manager.get_all_engines()
            },
            "observations_by_region": _trim_virtual_probe_observations({
                str(engine_id): [
                    {
                        **asdict(item),
                        "obs_type": item.obs_type.value,
                    }
                    for item in items
                ]
                for engine_id, items in probe_observations.items()
            }, VIRTUAL_PROBE_WORKER_MAX_OBSERVATIONS),
            "radar_sources_by_region": hass.data[DOMAIN].get(
                "radar_sources_by_engine", {}
            ),
        }

        if not hass.data[DOMAIN].get("virtual_probe_worker_task"):
            async def _refresh_virtual_probes():
                try:
                    from .worker_bridge import async_request_worker_job

                    result = await async_request_worker_job(
                        hass,
                        "virtual_probe_operational",
                        payload,
                        timeout=12,
                    )
                    if result.get("method") != "virtual_probe_operational_v1":
                        raise ValueError("ongeldig verkennerresultaat")
                    hass.data[DOMAIN][
                        "virtual_probe_worker_result"
                    ] = result
                    hass.data[DOMAIN][
                        "virtual_probe_worker_last_success"
                    ] = time.time()
                    hass.data[DOMAIN].pop(
                        "virtual_probe_worker_error", None
                    )
                except (
                    asyncio.TimeoutError,
                    RuntimeError,
                    TypeError,
                    ValueError,
                ) as err:
                    hass.data[DOMAIN]["virtual_probe_worker_error"] = {
                        "error": type(err).__name__,
                        "timestamp": time.time(),
                    }
                finally:
                    hass.data[DOMAIN]["virtual_probe_worker_task"] = None

            hass.data[DOMAIN][
                "virtual_probe_worker_task"
            ] = hass.async_create_background_task(
                _refresh_virtual_probes(),
                f"{DOMAIN}_virtual_probe_worker",
            )

        worker_result = hass.data[DOMAIN].pop(
            "virtual_probe_worker_result", None
        )
        if worker_result is not None:
            probe_samples = list(worker_result.get("samples", []))
            probe_diagnostics = dict(
                worker_result.get("diagnostics", {})
            )
            restored = [
                {
                    key: probe[key]
                    for key in (
                        "probe_id", "storm_id", "region_id", "station_id",
                        "latitude", "longitude", "created_at", "expected_at",
                        "eta_minutes", "passage", "pass_distance_km",
                        "motion_confidence", "radar_source",
                    )
                }
                for probe in probe_diagnostics.get("probes", [])
            ]
            restored_history = {
                "completed": probe_diagnostics.get("completed_count", 0),
                "hits": probe_diagnostics.get("confirmed_count", 0),
                "misses": probe_diagnostics.get("dry_count", 0),
                "unverifiable": probe_diagnostics.get(
                    "unverifiable_count", 0
                ),
                "cancelled": probe_diagnostics.get("cancelled_count", 0),
                "recent_outcomes": probe_diagnostics.get(
                    "recent_outcomes", []
                ),
            }
            probe_manager = VirtualProbeManager(
                restored, restored_history
            )
            hass.data[DOMAIN]["virtual_probe_manager"] = probe_manager
            operation = {
                "state": "operationeel",
                "mode": "operationeel",
                "fallback": False,
            }
        else:
            probe_samples = []
            probe_diagnostics = diagnostics_before
            last_success = float(hass.data[DOMAIN].get(
                "virtual_probe_worker_last_success", 0.0
            ) or 0.0)
            if time.time() - last_success <= 15 * 60:
                operation = {
                    "state": "operationeel",
                    "mode": "operationeel",
                    "fallback": False,
                    "source": "laatste_geldige_workerresultaat",
                    "age_seconds": round(time.time() - last_success, 1),
                }
            else:
                operation = {
                    "state": "worker_onbeschikbaar",
                    "mode": "worker_first",
                    "fallback": False,
                    "source": "bewaarde_workerstatus",
                }
        probe_diagnostics["worker_operation"] = operation
        probe_diagnostics["roaming_query"] = roaming_status
        hass.data[DOMAIN]["verification_samples_pending"].extend(
            probe_samples
        )
        hass.data[DOMAIN]["virtual_probe_diagnostics"] = probe_diagnostics
        hass.bus.async_fire(
            f"{DOMAIN}_virtual_probe_update", probe_diagnostics
        )

    async def _poll_open_meteo(now=None):
        if not hass.data[DOMAIN].get("open_meteo_enabled", False):
            result = {
                "gear": "DISABLED",
                "role": "optional_model_guidance",
                "provider_status": "disabled",
                "enabled": False,
                "target_results": {},
            }
            hass.data[DOMAIN]["open_meteo_result"] = result
            hass.data[DOMAIN]["open_meteo_results_by_target"] = {}
            hass.bus.async_fire(
                f"{DOMAIN}_open_meteo_update",
                {
                    "provider_status": "disabled",
                    "gear": "DISABLED",
                    "targets": [],
                    "fetch_sequence": 0,
                },
            )
            return
        targets = _open_meteo_targets()
        result = await open_meteo_provider.fetch(targets)
        target_results = result.get("target_results", {})
        hass.data[DOMAIN]["open_meteo_result"] = result
        hass.data[DOMAIN]["open_meteo_results_by_target"] = target_results

        sequence = int(result.get("fetch_sequence") or 0)
        processed = int(
            hass.data[DOMAIN].get("open_meteo_processed_sequence") or 0
        )
        if result.get("provider_status") == "ok" and sequence > processed:
            hass.data[DOMAIN]["open_meteo_processed_sequence"] = sequence
            observed_at = time.time()
            nominal_minute = int(observed_at // 60)
            target_runtime = hass.data[DOMAIN].get("targets", {})
            for target_id, target_result in target_results.items():
                target = target_runtime.get(target_id, {})
                lat = float(target_result.get("requested_latitude"))
                lon = float(target_result.get("requested_longitude"))
                _queue_provider_cycle_sample(
                    "open_meteo", target_id, lat, lon, {
                        "provider_status": result.get("provider_status"),
                        "fetch_sequence": sequence,
                        "last_success_at": result.get("last_success_at"),
                        "current_precipitation_mm": target_result.get(
                            "current_precipitation_mm"
                        ),
                        "forecast_90m_max_mm": target_result.get(
                            "forecast_90m_max_mm"
                        ),
                        "forecast_90m_total_mm": target_result.get(
                            "forecast_90m_total_mm"
                        ),
                        "forecast_90m_first_wet_minutes": target_result.get(
                            "forecast_90m_first_wet_minutes"
                        ),
                        "forecast_3h_max_mm": target_result.get(
                            "forecast_3h_max_mm"
                        ),
                        "cape_max_3h_jkg": target_result.get(
                            "cape_max_3h_jkg"
                        ),
                        "lightning_potential_max_3h": target_result.get(
                            "lightning_potential_max_3h"
                        ),
                    },
                    observed_at=observed_at,
                )
                hass.data[DOMAIN]["verification_samples_pending"].append({
                    "sample_key": (
                        f"open_meteo:{target_id}:{nominal_minute}"
                    ),
                    "sample_type": "open_meteo_forecast",
                    "target_id": target_id,
                    "nominal_minute": nominal_minute,
                    "observed_at": observed_at,
                    "latitude": lat,
                    "longitude": lon,
                    "own_forecast_available": False,
                    "snapshot": {
                        "provider": "open_meteo",
                        "role": "model_guidance",
                        "guidance_schema_version": 2,
                        "provider_status": result.get("provider_status"),
                        "fetch_sequence": sequence,
                        "last_success_at": result.get("last_success_at"),
                        "requested_variable_count": result.get(
                            "requested_variable_count"
                        ),
                        "region_engine_id": target.get("region_engine_id"),
                        "country_code": target.get("country_code"),
                        "forecast": dict(target_result),
                    },
                })
        hass.bus.async_fire(
            f"{DOMAIN}_open_meteo_update",
            {
                "provider_status": result.get("provider_status"),
                "gear": result.get("gear"),
                "targets": sorted(target_results),
                "fetch_sequence": sequence,
            },
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
                eumetsat.fetch_observations(), timeout=65
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
            if (
                await storm_manager.async_route_observation(observation)
                == 0
            ):
                continue
            accepted += 1
            hass.data[DOMAIN]["last_lightning"] = observation
            hass.data[DOMAIN].setdefault("lightning_count", 0)
            hass.data[DOMAIN]["lightning_count"] += 1
            engine_ids = _record_lightning(observation)
            _queue_lightning_update(observation, engine_ids=engine_ids)
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
                timeout=65,
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
            if (
                await storm_manager.async_route_observation(observation)
                == 0
            ):
                continue
            accepted += 1
            hass.data[DOMAIN]["lightning_source"] = observation.source
            hass.data[DOMAIN]["last_lightning"] = observation
            hass.data[DOMAIN].setdefault("lightning_count", 0)
            hass.data[DOMAIN]["lightning_count"] += 1
            engine_ids = _record_lightning(observation)
            _queue_lightning_update(observation, engine_ids=engine_ids)
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
        if (
            not ha_started.is_set()
            or hass.state != CoreState.running
            or not storm_manager.admission_ready
        ):
            _LOGGER.info(
                "Bliksembronmodus %s opgeslagen; activering wacht op "
                "HA-start en duurzaam herstel",
                source_mode,
            )
            return
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

    def _set_analysis_mode(mode: str) -> None:
        """Schakel experimentele analyse en kalibratie live om."""
        if mode not in {"full_send", "thermal_throttle"}:
            _LOGGER.warning("Onbekende analysemodus genegeerd: %s", mode)
            return
        previous = hass.data[DOMAIN].get("analysis_mode", "full_send")
        hass.data[DOMAIN]["analysis_mode"] = mode
        hass.bus.async_fire(f"{DOMAIN}_provider_lifecycle_update")
        hass.bus.async_fire(f"{DOMAIN}_radar_source_update")
        _LOGGER.info(
            "Analysemodus live gewijzigd van %s naar %s", previous, mode
        )

    hass.data[DOMAIN]["set_analysis_mode"] = _set_analysis_mode

    def _system_reports_hardware_throttling() -> bool:
        """Use only an explicit CPU/OS throttling signal, never a degree threshold."""

        for entity_id in (
            "binary_sensor.cpu_throttling",
            "binary_sensor.thermal_throttling",
            "sensor.cpu_throttling",
            "sensor.thermal_throttling",
        ):
            state = hass.states.get(entity_id)
            if state is None:
                continue
            if str(state.state).strip().lower() in {
                "on",
                "true",
                "active",
                "throttled",
                "throttling",
            }:
                return True
        return False

    def _system_performance_snapshot() -> dict:
        """Lees goedkope, reeds beschikbare HA-systeemmetingen."""
        candidates = {
            "cpu_percent": (
                "sensor.system_monitor_processor_use",
                "sensor.processor_use",
                "sensor.processor_use_percent",
            ),
            "temperature_c": (
                "sensor.system_monitor_processortemperatuur",
                "sensor.processor_temperature",
                "sensor.cpu_temperature",
            ),
            "memory_percent": (
                "sensor.memory_use_percent",
                "sensor.memory_usage",
            ),
            "load": ("sensor.load_1m", "sensor.load_1m_percent"),
            "fan": ("sensor.fans", "sensor.fan_speed"),
        }
        snapshot = {}
        for metric, entity_ids in candidates.items():
            for entity_id in entity_ids:
                state = hass.states.get(entity_id)
                value = _numeric_state(state)
                if value is not None:
                    snapshot[metric] = value
                    snapshot[f"{metric}_entity_id"] = entity_id
                    break
        return snapshot

    def _queue_system_performance_sample(
        cycle_id: str,
        stage: str,
        started_at: float,
        finished_at: float,
        outcome: str,
        before: dict,
        after: dict,
    ) -> None:
        hass.data[DOMAIN]["system_performance_samples_pending"].append({
            "sample_key": f"{cycle_id}:{stage}",
            "cycle_id": cycle_id,
            "stage": stage,
            "started_at": started_at,
            "finished_at": finished_at,
            "duration_ms": round((finished_at - started_at) * 1000.0, 3),
            "outcome": outcome,
            "cpu_before": before.get("cpu_percent"),
            "cpu_after": after.get("cpu_percent"),
            "temperature_before_c": before.get("temperature_c"),
            "temperature_after_c": after.get("temperature_c"),
            "memory_before_percent": before.get("memory_percent"),
            "memory_after_percent": after.get("memory_percent"),
            "load_before": before.get("load"),
            "load_after": after.get("load"),
            "fan_before": before.get("fan"),
            "fan_after": after.get("fan"),
            "snapshot": {"before": before, "after": after},
        })

    async def _bounded_provider_stage(name: str, awaitable, timeout_s: float):
        """Begrens een volledige providerfase zodat de cyclus kan herstellen."""
        cycle_id = hass.data[DOMAIN].get(
            "current_provider_cycle_id", "outside_cycle"
        )
        started_at = time.time()
        before = _system_performance_snapshot()
        outcome = "ok"
        try:
            return await asyncio.wait_for(awaitable, timeout=timeout_s)
        except asyncio.TimeoutError:
            outcome = "timeout"
            _LOGGER.error(
                "Providerfase %s overschreed harde timeout van %.0f seconden",
                name,
                timeout_s,
            )
            hass.data[DOMAIN].setdefault("provider_stage_timeouts", {})[name] = time.time()
            return None
        except Exception:
            outcome = "error"
            raise
        finally:
            finished_at = time.time()
            _queue_system_performance_sample(
                cycle_id,
                name,
                started_at,
                finished_at,
                outcome,
                before,
                _system_performance_snapshot(),
            )

    async def _flush_calibration_data_impl_legacy() -> None:
        """Schrijf de verzamelde kalibratiedata buiten de HA-eventloop weg."""
        observer = hass.data[DOMAIN]["radar_calibration_observer"]
        batch = observer.drain_collection_batch()
        calibration_operation = {
            "state": "operationeel",
            "mode": "worker_first",
            "source": "worker",
            "fallback_active": False,
        }
        if batch.get("frames"):
            try:
                from .worker_bridge import async_request_worker_job

                serialized_frames = [
                    list(frame) for frame in batch["frames"]
                ]
                received = cached = 0
                collected_comparisons = []
                # Maximaal drie 0,1°-rasters per bericht houdt het MQTT-
                # contract ook bij veel natte cellen ruim onder 1 MiB.
                for offset in range(0, len(serialized_frames), 3):
                    response = await async_request_worker_job(
                        hass,
                        "calibration_operational",
                        {"frames": serialized_frames[offset:offset + 3]},
                        timeout=20,
                    )
                    if (
                        response.get("method")
                        != "worker_radar_calibration_v1"
                    ):
                        raise ValueError("ongeldig workerkalibratieresultaat")
                    received += int(response.get("frames_received", 0))
                    cached = int(response.get("cached_frames", cached))
                    collected_comparisons.extend(
                        response.get("comparisons", [])
                    )
                comparisons = tuple(
                    tuple(value) for value in collected_comparisons
                )
                batch["comparisons"] = comparisons
                observer.ingest_worker_comparisons(comparisons)
                calibration_operation.update({
                    "frames_received": received,
                    "comparisons_received": len(comparisons),
                    "cached_frames": cached,
                })
            except (
                asyncio.TimeoutError,
                RuntimeError,
                TypeError,
                ValueError,
            ) as err:
                batch["comparisons"] = ()
                calibration_operation.update({
                    "state": "worker_onbeschikbaar",
                    "mode": "worker_first",
                    "source": "bewaarde_frames",
                    "fallback_active": False,
                    "error": type(err).__name__,
                })
        hass.data[DOMAIN]["calibration_worker"] = calibration_operation
        pending = hass.data[DOMAIN].get("verification_samples_pending", [])
        hass.data[DOMAIN]["verification_samples_pending"] = []
        batch["verification_samples"] = tuple(pending)
        preflight_pending = hass.data[DOMAIN].get(
            "pysteps_preflight_samples_pending", []
        )
        hass.data[DOMAIN]["pysteps_preflight_samples_pending"] = []
        batch["pysteps_preflight_samples"] = tuple(preflight_pending)
        system_pending = hass.data[DOMAIN].get(
            "system_performance_samples_pending", []
        )
        hass.data[DOMAIN]["system_performance_samples_pending"] = []
        batch["system_performance_samples"] = tuple(system_pending)
        lightning_pending = hass.data[DOMAIN].get(
            "lightning_aggregates_pending", []
        )
        hass.data[DOMAIN]["lightning_aggregates_pending"] = []
        batch["lightning_aggregates"] = tuple(lightning_pending)
        provider_pending = hass.data[DOMAIN].get(
            "provider_cycle_samples_pending", []
        )
        hass.data[DOMAIN]["provider_cycle_samples_pending"] = []
        batch["provider_cycle_samples"] = tuple(provider_pending)
        ground_pending = hass.data[DOMAIN].get(
            "ground_observations_pending", []
        )
        hass.data[DOMAIN]["ground_observations_pending"] = []
        batch["ground_observations"] = tuple(ground_pending)
        ground_quality_pending = hass.data[DOMAIN].get(
            "ground_quality_assessments_pending", []
        )
        hass.data[DOMAIN]["ground_quality_assessments_pending"] = []
        batch["ground_quality_assessments"] = tuple(ground_quality_pending)
        ground_decisions_pending = hass.data[DOMAIN].get(
            "ground_confirmation_decisions_pending", []
        )
        hass.data[DOMAIN]["ground_confirmation_decisions_pending"] = []
        batch["ground_confirmation_decisions"] = tuple(
            ground_decisions_pending
        )
        original_pending = hass.data[DOMAIN].get(
            "original_radar_frames_pending", []
        )
        hass.data[DOMAIN]["original_radar_frames_pending"] = []
        batch["original_radar_frames"] = tuple(original_pending)
        if (
            not batch["frames"]
            and not batch["comparisons"]
            and not batch["verification_samples"]
            and not batch["pysteps_preflight_samples"]
            and not batch["system_performance_samples"]
            and not batch["lightning_aggregates"]
            and not batch["provider_cycle_samples"]
            and not batch["ground_observations"]
            and not batch["ground_quality_assessments"]
            and not batch["ground_confirmation_decisions"]
            and not batch["original_radar_frames"]
        ):
            return
        try:
            result = await hass.async_add_executor_job(
                calibration_store.write_batch, batch
            )
            if batch["original_radar_frames"]:
                await hass.async_add_executor_job(
                    calibration_store.save_original_radar_frames,
                    list(batch["original_radar_frames"]),
                )
        except Exception as exc:
            observer.restore_collection_batch(batch)
            hass.data[DOMAIN]["verification_samples_pending"][:0] = list(
                batch.get("verification_samples", ())
            )
            hass.data[DOMAIN]["pysteps_preflight_samples_pending"][:0] = list(
                batch.get("pysteps_preflight_samples", ())
            )
            hass.data[DOMAIN]["system_performance_samples_pending"][:0] = list(
                batch.get("system_performance_samples", ())
            )
            hass.data[DOMAIN]["lightning_aggregates_pending"][:0] = list(
                batch.get("lightning_aggregates", ())
            )
            hass.data[DOMAIN]["provider_cycle_samples_pending"][:0] = list(
                batch.get("provider_cycle_samples", ())
            )
            hass.data[DOMAIN]["ground_observations_pending"][:0] = list(
                batch.get("ground_observations", ())
            )
            hass.data[DOMAIN]["ground_quality_assessments_pending"][:0] = list(
                batch.get("ground_quality_assessments", ())
            )
            hass.data[DOMAIN][
                "ground_confirmation_decisions_pending"
            ][:0] = list(batch.get("ground_confirmation_decisions", ()))
            hass.data[DOMAIN]["original_radar_frames_pending"][:0] = list(
                batch.get("original_radar_frames", ())
            )
            hass.data[DOMAIN]["radar_calibration_storage"] = {
                "status": "error", "error": type(exc).__name__,
                "path": str(calibration_store.path),
            }
            _LOGGER.exception("Kalibratiedatabase schrijven mislukt")
            return
        profiles = result.pop("_bias_profiles", [])
        hass.data[DOMAIN]["radar_bias_profiles"] = profiles
        hass.data[DOMAIN]["radar_bias_profile_index"] = build_profile_index(
            profiles
        )
        hass.data[DOMAIN]["radar_calibration_storage"] = {
            "status": "ready", **result,
        }
        diagnostics = observer.diagnostics()
        diagnostics["worker_operation"] = dict(
            hass.data[DOMAIN].get("calibration_worker", {})
        )
        diagnostics["storage"] = hass.data[DOMAIN]["radar_calibration_storage"]
        diagnostics["bias_profiles"] = {
            "total": len(profiles),
            "operational": sum(
                1 for profile in profiles
                if profile.get("confidence") != "insufficient"
            ),
            "profiles": profiles[:20],
        }
        hass.data[DOMAIN]["radar_calibration"] = diagnostics
        hass.bus.async_fire(
            f"{DOMAIN}_calibration_update",
            {"samples": diagnostics["samples"]},
        )

    def _validated_worker_calibration_summary(
        response: object,
        *,
        expected_method: str,
    ) -> tuple[dict, dict, list[dict]]:
        """Validate the bounded worker read model before exposing it in HA."""
        if (
            not isinstance(response, dict)
            or response.get("method") != expected_method
            or not isinstance(response.get("statistics"), dict)
        ):
            raise ValueError("ongeldig worker-kalibratiecorpusresultaat")
        raw_profiles = response.get("profiles")
        if not isinstance(raw_profiles, list) or len(raw_profiles) > 128:
            raise ValueError("ongeldige worker-kalibratieprofiellijst")
        profiles = []
        numeric_fields = (
            "mean_detection_ratio", "mean_extra_fraction",
            "mean_wet_area_ratio", "mean_f1_score",
            "mean_intensity_bias", "mean_shift_lat_cells",
            "mean_shift_lon_cells", "mean_latency_seconds", "updated_at",
        )
        for raw_profile in raw_profiles:
            if not isinstance(raw_profile, dict) or not {
                "scope_key", "from_source", "to_source", "sample_count",
                "confidence",
            }.issubset(raw_profile):
                raise ValueError("worker-kalibratieprofiel mist velden")
            if any(
                not isinstance(raw_profile[key], str) or not raw_profile[key]
                for key in ("scope_key", "from_source", "to_source")
            ):
                raise ValueError("worker-kalibratieprofiel heeft ongeldige bron")
            sample_count = raw_profile["sample_count"]
            confidence = raw_profile["confidence"]
            if (
                isinstance(sample_count, bool)
                or not isinstance(sample_count, int)
                or sample_count < 0
                or not isinstance(confidence, str)
                or confidence not in {
                    "insufficient", "low", "medium", "high"
                }
            ):
                raise ValueError("worker-kalibratieprofiel heeft ongeldige telling")
            for key in numeric_fields:
                value = raw_profile.get(key)
                if value is not None and (
                    isinstance(value, bool)
                    or not isinstance(value, (int, float))
                    or not math.isfinite(float(value))
                ):
                    raise ValueError(
                        "worker-kalibratieprofiel heeft ongeldige metriek"
                    )
            profiles.append(dict(raw_profile))
        profiles_total = response.get("profiles_total")
        if (
            isinstance(profiles_total, bool)
            or not isinstance(profiles_total, int)
            or profiles_total < len(profiles)
            or type(response.get("profiles_truncated")) is not bool
        ):
            raise ValueError("worker-kalibratieprofielsamenvatting is ongeldig")
        return dict(response), dict(response["statistics"]), profiles

    async def _flush_calibration_data_impl() -> None:
        """Persist radar evidence in the worker and only tiny telemetry in HA."""
        observer = hass.data[DOMAIN]["radar_calibration_observer"]
        batch = {"frames": (), "comparisons": ()}
        pending_fields = (
            ("verification_samples_pending", "verification_samples"),
            ("pysteps_preflight_samples_pending", "pysteps_preflight_samples"),
            ("system_performance_samples_pending", "system_performance_samples"),
            ("lightning_aggregates_pending", "lightning_aggregates"),
            ("provider_cycle_samples_pending", "provider_cycle_samples"),
            ("ground_observations_pending", "ground_observations"),
            ("ground_quality_assessments_pending", "ground_quality_assessments"),
            (
                "ground_confirmation_decisions_pending",
                "ground_confirmation_decisions",
            ),
        )
        for runtime_key, batch_key in pending_fields:
            values = hass.data[DOMAIN].get(runtime_key, [])
            hass.data[DOMAIN][runtime_key] = []
            batch[batch_key] = tuple(values)
        all_captures_by_id = hass.data[DOMAIN].get(
            "calibration_captures_pending", {}
        )
        capture_ids = sorted(all_captures_by_id)[
            :CALIBRATION_CORPUS_MAX_CAPTURES_PER_JOB
        ]
        captures_by_id = {
            capture_id: all_captures_by_id[capture_id]
            for capture_id in capture_ids
        }
        hass.data[DOMAIN]["calibration_captures_pending"] = {
            capture_id: descriptor
            for capture_id, descriptor in all_captures_by_id.items()
            if capture_id not in captures_by_id
        }
        captures = [
            captures_by_id[key] for key in sorted(captures_by_id)
        ]
        if (
            not captures
            and not any(batch.get(batch_key) for _, batch_key in pending_fields)
        ):
            return

        def _restore_pending() -> None:
            for runtime_key, batch_key in pending_fields:
                hass.data[DOMAIN][runtime_key][:0] = list(
                    batch.get(batch_key, ())
                )
            current_captures = hass.data[DOMAIN].setdefault(
                "calibration_captures_pending", {}
            )
            for capture_id, descriptor in captures_by_id.items():
                current_captures.setdefault(capture_id, descriptor)

        calibration_operation = {
            "state": "operationeel",
            "mode": "worker_calibration_corpus_v1",
            "source": "worker",
            "fallback_active": False,
        }
        corpus_result = {}
        corpus_profiles = []
        try:
            if captures:
                from .worker_bridge import async_request_worker_job

                capture_digest = hashlib.sha256(
                    "\n".join(sorted(captures_by_id)).encode("ascii")
                ).hexdigest()
                response = await async_request_worker_job(
                    hass,
                    "calibration_corpus_ingest",
                    {"captures": captures},
                    timeout=90,
                    job_id=f"calcorpus-{capture_digest[:32]}",
                )
                corpus_result, statistics, corpus_profiles = (
                    _validated_worker_calibration_summary(
                        response,
                        expected_method="worker_calibration_corpus_v1",
                    )
                )
                receipt = dict(corpus_result.get("receipt") or {})
                calibration_operation.update({
                    "captures_received": int(
                        receipt.get("captures_received", 0)
                    ),
                    "captures_inserted": int(
                        receipt.get("captures_inserted", 0)
                    ),
                    "artifacts_inserted": int(
                        receipt.get("artifacts_inserted", 0)
                    ),
                    "groups_recomputed": int(
                        receipt.get("groups_recomputed", 0)
                    ),
                    "comparisons_recomputed": int(
                        receipt.get("comparisons_recomputed", 0)
                    ),
                    "already_applied": bool(
                        corpus_result.get("already_applied", False)
                    ),
                    "corpus": statistics,
                })

            # write_batch receives no radar grids or overlays. It persists
            # only small ground/verification/performance records.
            telemetry_result = await hass.async_add_executor_job(
                calibration_store.write_batch, batch
            )
            result = dict(telemetry_result)
        except asyncio.CancelledError:
            _restore_pending()
            raise
        except Exception as exc:
            _restore_pending()
            calibration_operation.update({
                "state": "worker_onbeschikbaar",
                "error": type(exc).__name__,
            })
            hass.data[DOMAIN]["calibration_worker"] = calibration_operation
            hass.data[DOMAIN]["radar_calibration_storage"] = {
                "status": "error", "error": type(exc).__name__,
                "path": str(calibration_store.path),
            }
            _LOGGER.exception("Worker-owned kalibratiecorpus verwerken mislukt")
            return

        hass.data[DOMAIN]["calibration_worker"] = calibration_operation
        local_profiles = result.pop("_bias_profiles", [])
        if corpus_result:
            profiles = corpus_profiles
            result["worker_corpus"] = dict(
                corpus_result.get("statistics") or {}
            )
            result["worker_corpus_profiles_total"] = int(
                corpus_result.get("profiles_total", len(profiles))
            )
            result["worker_corpus_profiles_truncated"] = bool(
                corpus_result.get("profiles_truncated", False)
            )
        else:
            profiles = list(
                hass.data[DOMAIN].get("radar_bias_profiles", local_profiles)
            )
        hass.data[DOMAIN]["radar_bias_profiles"] = profiles
        hass.data[DOMAIN]["radar_bias_profile_index"] = build_profile_index(
            profiles
        )
        hass.data[DOMAIN]["radar_calibration_storage"] = {
            "status": "ready", **result,
        }
        diagnostics = observer.diagnostics()
        diagnostics["worker_operation"] = calibration_operation
        diagnostics["storage"] = hass.data[DOMAIN]["radar_calibration_storage"]
        diagnostics["pending_captures"] = len(
            hass.data[DOMAIN].get("calibration_captures_pending", {})
        )
        diagnostics["bias_profiles"] = {
            "total": len(profiles),
            "operational": sum(
                1 for profile in profiles
                if profile.get("confidence") != "insufficient"
            ),
            "profiles": profiles[:20],
        }
        hass.data[DOMAIN]["radar_calibration"] = diagnostics
        hass.bus.async_fire(
            f"{DOMAIN}_calibration_update",
            {"samples": diagnostics["samples"]},
        )

    async def _refresh_worker_calibration_snapshot() -> bool:
        """Load the compact worker-owned corpus view without legacy DB scans."""
        from .worker_bridge import (
            async_request_worker_job,
            worker_is_available,
        )

        if not worker_is_available(hass):
            hass.data[DOMAIN]["calibration_worker"] = {
                "state": "wacht_op_worker",
                "mode": "worker_calibration_corpus_v1",
                "source": "worker",
                "fallback_active": False,
            }
            return False
        try:
            response = await async_request_worker_job(
                hass,
                "calibration_corpus_snapshot",
                {},
                timeout=20,
                job_id=(
                    "calibration-corpus-snapshot-"
                    f"{int(time.time() // 300)}"
                ),
            )
            corpus_result, statistics, profiles = (
                _validated_worker_calibration_summary(
                    response,
                    expected_method=(
                        "worker_calibration_corpus_snapshot_v1"
                    ),
                )
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            hass.data[DOMAIN]["calibration_worker"] = {
                "state": "worker_onbeschikbaar",
                "mode": "worker_calibration_corpus_v1",
                "source": "worker",
                "fallback_active": False,
                "error": type(exc).__name__,
            }
            _LOGGER.warning(
                "Worker-kalibratiecorpussnapshot niet beschikbaar: %s",
                type(exc).__name__,
            )
            return False

        hass.data[DOMAIN]["radar_bias_profiles"] = profiles
        hass.data[DOMAIN]["radar_bias_profile_index"] = build_profile_index(
            profiles
        )
        storage = dict(hass.data[DOMAIN].get("radar_calibration_storage", {}))
        storage.update({
            "status": "ready",
            "worker_corpus": statistics,
            "worker_corpus_profiles_total": int(
                corpus_result["profiles_total"]
            ),
            "worker_corpus_profiles_truncated": bool(
                corpus_result["profiles_truncated"]
            ),
        })
        hass.data[DOMAIN]["radar_calibration_storage"] = storage
        hass.data[DOMAIN]["calibration_worker"] = {
            "state": "operationeel",
            "mode": "worker_calibration_corpus_v1",
            "source": "worker",
            "fallback_active": False,
            "corpus": statistics,
        }
        diagnostics = hass.data[DOMAIN][
            "radar_calibration_observer"
        ].diagnostics()
        diagnostics["worker_operation"] = dict(
            hass.data[DOMAIN]["calibration_worker"]
        )
        diagnostics["storage"] = storage
        diagnostics["bias_profiles"] = {
            "total": len(profiles),
            "operational": sum(
                profile.get("confidence") != "insufficient"
                for profile in profiles
            ),
            "profiles": profiles[:20],
        }
        hass.data[DOMAIN]["radar_calibration"] = diagnostics
        hass.bus.async_fire(
            f"{DOMAIN}_calibration_update",
            {"samples": diagnostics["samples"]},
        )
        return True

    async def _flush_calibration_data() -> None:
        """Plan opslag/workerwerk zonder een providerfase erop te laten wachten."""
        task = hass.data[DOMAIN].get("calibration_flush_task")
        if task is not None and not task.done():
            return

        async def _run() -> None:
            try:
                await _flush_calibration_data_impl()
            finally:
                hass.data[DOMAIN]["calibration_flush_task"] = None

        hass.data[DOMAIN]["calibration_flush_task"] = (
            hass.async_create_background_task(
                _run(), f"{DOMAIN}_calibration_flush"
            )
        )

    def _numeric_state(entity_state):
        if entity_state is None or entity_state.state in {
            "unknown", "unavailable", "none", "",
        }:
            return None
        try:
            return float(entity_state.state)
        except (TypeError, ValueError):
            return None

    def _snapshot_bool(value) -> bool:
        """Normaliseer eventtemplates zonder de string 'false' als waar te zien."""
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "on"}
        return bool(value)

    def _queue_target_verification_samples() -> tuple:
        """Leg targetstatus vast en bouw immutable warning-evaluaties."""
        from .engine.nowcast import build_precipitation_status

        observed_at = time.time()
        warning_evaluations = []
        nominal_minute = int(observed_at // 60)
        average = hass.states.get("sensor.neerslagverwachting_gemiddeld")
        total = hass.states.get("sensor.neerslagverwachting_totaal")
        target_runtime = hass.data[DOMAIN].get("targets", {})
        radar_sources = hass.data[DOMAIN].get(
            "radar_sources_by_engine", {}
        )
        pressure_trends = hass.data[DOMAIN].get(
            "netatmo_pressure_trends_by_engine", {}
        )
        for target_id, target in target_runtime.items():
            lat = target.get("latitude")
            lon = target.get("longitude")
            entity_id = target.get("entity_id")
            if (
                not target.get("available")
                or lat is None
                or lon is None
                or not entity_id
            ):
                continue
            region = storm_manager.get_engine_for_target(entity_id)
            engine_id = (
                region.engine_id if region is not None
                else target.get("region_engine_id")
            )
            source_decision = radar_sources.get(engine_id, {})
            active_storms = (
                region.storm_engine.get_active_storms()
                if region else []
            )
            own_attrs = build_precipitation_status(
                active_storms,
                float(lat),
                float(lon),
                radar_source=source_decision.get("source"),
                pressure_trend=pressure_trends.get(engine_id),
                motion_guidance=hass.data[DOMAIN].get(
                    "pysteps_input", {}
                ).get("preflight", {}).get(engine_id),
            )
            if not target.get("radar_covered", False):
                own_attrs["status"] = "onvoldoende_data"
            data_age_seconds = source_decision.get("age_seconds")
            selected_storm_id = (
                own_attrs.get("storm_id")
                or own_attrs.get("tracked_system_storm_id")
            )
            selected_storm = next((
                storm for storm in active_storms
                if storm.storm_id == selected_storm_id
            ), None)
            if selected_storm is not None:
                timestamps = [
                    float(cell.timestamp)
                    for cell in selected_storm.radar_cells.values()
                    if isinstance(getattr(cell, "timestamp", None), (int, float))
                    and not isinstance(cell.timestamp, bool)
                    and math.isfinite(float(cell.timestamp))
                ]
                if timestamps:
                    data_age_seconds = max(
                        0.0, observed_at - max(timestamps)
                    )
            warning_evaluations.append(build_warning_evaluation(
                own_attrs,
                target_id=str(target_id),
                target_label=str(target.get("name") or target_id),
                latitude=float(lat),
                longitude=float(lon),
                observed_at=observed_at,
                data_age_seconds=data_age_seconds,
                # Dit is uitsluitend de lokale runtime-admission. De
                # expliciete notification-opt-in is dispatch-authority; een
                # worker/raw admissionflag mag legacy nooit stil blokkeren.
                runtime_admitted=storm_manager.admission_ready,
                target_available=target.get("available") is True,
                radar_covered=target.get("radar_covered") is True,
            ))
            eta = own_attrs.get("eta_minutes")
            warning_candidate = None
            if (
                own_attrs.get("warning_eligible") is True
                and eta is not None
            ):
                eta_value = float(eta)
                warning_candidate = (
                    "imminent" if eta_value <= 15
                    else "nabij" if eta_value <= 45
                    else "vroeg" if eta_value <= 90
                    else None
                )
            is_home = target_id == "home"
            buienradar_snapshot = {
                "average_mm_h": (
                    _numeric_state(average) if is_home else None
                ),
                "total_mm": _numeric_state(total) if is_home else None,
                "average_last_updated": (
                    average.last_updated.isoformat()
                    if is_home and average else None
                ),
                "total_last_updated": (
                    total.last_updated.isoformat()
                    if is_home and total else None
                ),
                "scope": "home_only",
            }
            hass.data[DOMAIN]["verification_samples_pending"].append({
                "sample_key": f"cycle:{target_id}:{nominal_minute}",
                "sample_type": "target_cycle",
                "target_id": target_id,
                "nominal_minute": nominal_minute,
                "observed_at": observed_at,
                "latitude": float(lat),
                "longitude": float(lon),
                "buienradar_average_mm_h": (
                    buienradar_snapshot["average_mm_h"]
                ),
                "buienradar_total_mm": (
                    buienradar_snapshot["total_mm"]
                ),
                "own_status": own_attrs.get("status"),
                "own_distance_km": own_attrs.get("distance_km"),
                "own_eta_minutes": eta,
                "own_passage": own_attrs.get(
                    "passage_classification"
                ),
                "own_confidence": own_attrs.get(
                    "motion_confidence"
                ),
                "own_forecast_available": own_attrs.get(
                    "forecast_available", False
                ),
                "warning_stage": warning_candidate,
                "snapshot": {
                    "schema_version": 2,
                    "stv3": dict(own_attrs),
                    "target": {
                        "id": target_id,
                        "name": target.get("name"),
                        "entity_id": entity_id,
                        "latitude": float(lat),
                        "longitude": float(lon),
                        "region_engine_id": engine_id,
                        "radar_covered": bool(
                            target.get("radar_covered", False)
                        ),
                        "location_place": target.get(
                            "location_place"
                        ),
                        "country_code": target.get("country_code"),
                    },
                    "radar_source_decision": dict(source_decision),
                    "buienradar": buienradar_snapshot,
                    "warning_candidate": warning_candidate,
                },
            })
        return tuple(warning_evaluations)

    @callback
    def _record_warning_snapshot(event) -> None:
        """Bewaar exact de snapshot waarmee een melding werd verzonden."""
        data = dict(event.data)
        observed_at = float(data.get("observed_at") or time.time())
        target_id = str(data.get("target_id") or "unknown")
        stage = str(data.get("warning_stage") or "unknown")
        event_id = getattr(event.context, "id", None) or str(observed_at)
        hass.data[DOMAIN]["verification_samples_pending"].append({
            "sample_key": f"warning:{event_id}",
            "sample_type": "warning_sent",
            "target_id": target_id,
            "nominal_minute": int(observed_at // 60),
            "observed_at": observed_at,
            "latitude": data.get("latitude"),
            "longitude": data.get("longitude"),
            "buienradar_average_mm_h": data.get("buienradar_average_mm_h"),
            "buienradar_total_mm": data.get("buienradar_total_mm"),
            "own_status": data.get("status"),
            "own_distance_km": data.get("distance_km"),
            "own_eta_minutes": data.get("eta_minutes"),
            "own_passage": data.get("passage_classification"),
            "own_confidence": data.get("motion_confidence"),
            "own_forecast_available": _snapshot_bool(
                data.get("forecast_available", False)
            ),
            "warning_stage": stage,
            "snapshot": data,
        })
        hass.async_create_task(_flush_calibration_data())

    hass.data[DOMAIN]["unsubscribers"].append(
        hass.bus.async_listen(
            WARNING_SENT_EVENT, _record_warning_snapshot
        )
    )

    async def _flush_region_observation_batches() -> int:
        """Maak alle geplande OFE-adds zichtbaar vóór verificatie/opslag."""
        await storm_manager.async_drain_ingest()
        delivered = await asyncio.gather(*(
            region.ofe.async_flush_pending()
            for region in storm_manager.get_all_engines()
        ))
        return sum(delivered)

    async def _poll_all(now=None):
        """Voer een volledige providercyclus in vaste, racevrije volgorde uit."""
        if (
            not ha_started.is_set()
            or hass.state != CoreState.running
            or not storm_manager.admission_ready
        ):
            _LOGGER.debug(
                "Providercyclus uitgesteld: startup-admission is nog gesloten"
            )
            return
        lock = hass.data[DOMAIN].setdefault("provider_cycle_lock", asyncio.Lock())
        if lock.locked():
            _LOGGER.debug("Providercyclus overgeslagen: vorige cyclus loopt nog")
            return
        async with lock:
            hass.data[DOMAIN]["current_provider_cycle_id"] = (
                f"cycle-{int(time.time() * 1000)}"
            )
            _sync_region_radar_providers()
            await _bounded_provider_stage(
                "national", _poll_national_providers(), 100
            )
            await _bounded_provider_stage(
                "radar_comparison", _poll_radar_comparison(), 125
            )
            await _bounded_provider_stage("radar", _poll_radar(), 120)
            # Start grondmeting en modelcontext gelijktijdig, maar begrens ze
            # afzonderlijk. Een trage modelcall mag Netatmo en de daarop
            # volgende verkennerevaluatie nooit meer mee annuleren.
            netatmo_task = asyncio.create_task(_poll_netatmo())
            open_meteo_task = asyncio.create_task(_poll_open_meteo())
            await _bounded_provider_stage("netatmo", netatmo_task, 30)
            await _bounded_provider_stage(
                "open_meteo", open_meteo_task, 30
            )
            await _bounded_provider_stage(
                "virtual_probe_evaluation",
                _update_virtual_probes_from_cache(),
                5,
            )
            await _bounded_provider_stage(
                "observation_flush", _flush_region_observation_batches(), 15
            )
            warning_evaluations = _queue_target_verification_samples()
            await _bounded_provider_stage(
                "warning_dispatch",
                warning_dispatcher.async_process(warning_evaluations),
                15,
            )
            hass.data[DOMAIN]["warning_dispatch_diagnostics"] = (
                warning_dispatcher.diagnostics
            )
            hass.data[DOMAIN].setdefault(
                "legacy_provider_configuration", {}
            ).setdefault("notifications", {})[
                "warning_dispatch"
            ] = warning_dispatcher.diagnostics
            await _bounded_provider_stage(
                "calibration_storage", _flush_calibration_data(), 15
            )

    # ── Polling intervallen ───────────────────────────────────────────────
    from homeassistant.helpers.event import async_track_time_interval, async_track_state_change_event
    from datetime import timedelta

    def _activate_polling_schedules() -> None:
        """Registreer intervalpolls pas nadat HA volledig running is."""
        if hass.data[DOMAIN].get("polling_schedules_active"):
            return
        hass.data[DOMAIN]["unsubscribers"].extend([
            async_track_time_interval(hass, _poll_all, timedelta(minutes=5)),
            # Grondmetingen mogen niet wachten op de zwaardere radarcyclus.
            # De bronhelpers hierboven begrenzen de externe frequentie:
            # METAR elke minuut, Netatmo/Synoptic maximaal elke vijf minuten.
            async_track_time_interval(
                hass, _poll_netatmo, timedelta(minutes=1)
            ),
            async_track_time_interval(
                hass, _poll_eumetsat_li, timedelta(minutes=2)
            ),
            async_track_time_interval(
                hass, _poll_goes_glm, timedelta(minutes=1)
            ),
        ])
        hass.data[DOMAIN]["polling_schedules_active"] = True

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
            recover_durable_region = hass.data[DOMAIN].get(
                "recover_durable_region"
            )
            if recover_durable_region is not None:
                await recover_durable_region(region)
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
        if storm_manager.admission_ready:
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
        recover_durable_region = hass.data[DOMAIN].get(
            "recover_durable_region"
        )
        if recover_durable_region is not None:
            await recover_durable_region(region)
        _restore_pending_region_frames_and_audit()
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
        if not ha_started.is_set() or hass.state != CoreState.running:
            return
        # Bouw eerst iedere dynamische regio op en herstel haar duurzame
        # trackinghead/backlog. Tot deze recoverybarrière voorbij is, mag geen
        # intervalpoll of pushprovider nieuwe input toelaten.
        await _update_fictieve_location()
        for spec in secondary_specs:
            await _update_secondary_target(spec, initial=True)
        # Secundaire targets kunnen pas na HA-start een extra RegionEngine
        # aanmaken. Geef zulke regio's alsnog hun bewaarde rasterketen en neem
        # ze daarna expliciet mee in de stormobject-herstartaudit.
        _restore_pending_region_frames_and_audit()
        # Dit is de enige startup-overgang die de globale routepoort opent:
        # iedere nu bekende regio is hersteld en de journalcontrole hierboven
        # is al geslaagd. Providerinitialisatie volgt pas hierna.
        storm_manager.complete_durable_recovery()
        hass.data[DOMAIN]["durable_processing"].update({
            "status": "ready",
            "admission_ready": True,
        })
        if not hass.data[DOMAIN].get("providers_initialized"):
            await _init_location_providers(home_lat, home_lon)
            hass.data[DOMAIN]["providers_initialized"] = True
        hass.async_create_background_task(
            _refresh_worker_calibration_snapshot(),
            f"{DOMAIN}_calibration_corpus_snapshot",
        )
        _activate_polling_schedules()
        if lightning_source_mode != "satellite_test":
            blitz.start()
        await _poll_all()
        if lightning_source_mode == "satellite_test":
            hass.async_create_task(_poll_eumetsat_li())
            hass.async_create_task(_poll_goes_glm())

    startup_timeout_handle = None

    @callback
    def _schedule_start_timeout() -> None:
        """Plan een lichte statuscontrole zonder HA-setup-task te maken."""
        nonlocal startup_timeout_handle
        startup_timeout_handle = hass.loop.call_later(
            HA_START_WAIT_TIMEOUT_S, _on_start_timeout
        )

    @callback
    def _on_start_timeout() -> None:
        nonlocal startup_timeout_handle
        startup_timeout_handle = None
        if ha_started.is_set() or hass.state == CoreState.running:
            return
        _LOGGER.warning(
            "Storm Tracker blijft gepauzeerd: Home Assistant is na "
            "%.0f seconden nog niet gestart",
            HA_START_WAIT_TIMEOUT_S,
        )
        _schedule_start_timeout()

    @callback
    def _start_runtime_background() -> None:
        task = hass.async_create_background_task(
            _do_initial_setup(),
            "storm_tracker_v3 initial setup",
        )
        hass.data[DOMAIN]["initial_setup_task"] = task

    if hass.state != CoreState.running:
        @callback
        def _on_ha_started(event):
            nonlocal startup_timeout_handle
            ha_started.set()
            if startup_timeout_handle is not None:
                startup_timeout_handle.cancel()
                startup_timeout_handle = None
            _start_runtime_background()
        hass.data[DOMAIN]["unsubscribers"].append(
            hass.bus.async_listen_once(
                EVENT_HOMEASSISTANT_STARTED, _on_ha_started
            )
        )
        _schedule_start_timeout()
    else:
        _start_runtime_background()

    async def _stop_region_runtime() -> None:
        """Sluit OFE-timers vóór loopshutdown en bewaar daarna trackingstate."""
        regions = list(storm_manager.get_all_engines())
        reset_results = await asyncio.gather(
            *(region.ofe.reset() for region in regions),
            return_exceptions=True,
        )
        for region, result in zip(regions, reset_results):
            if isinstance(result, Exception):
                _LOGGER.warning(
                    "OFE-stop voor %s mislukte: %s",
                    region.engine_id,
                    type(result).__name__,
                )
        await asyncio.gather(
            *(
                mcs_store.async_save_engine(
                    region.storage_key, region.storm_engine
                )
                for region in regions
            ),
            return_exceptions=True,
        )

    shutdown_prepared = False

    @callback
    def _prepare_shutdown() -> None:
        """Sluit alle nieuwe admission één keer vóór de async drain."""
        nonlocal startup_timeout_handle, shutdown_prepared
        if shutdown_prepared:
            return
        shutdown_prepared = True
        storm_manager.close_admission()
        if startup_timeout_handle is not None:
            startup_timeout_handle.cancel()
            startup_timeout_handle = None
        lightning_updates.close()
        blitz.stop()
        for unsubscribe in hass.data[DOMAIN].get("unsubscribers", []):
            unsubscribe()
        hass.data[DOMAIN]["unsubscribers"] = []
        if hass.services.has_service(
            DOMAIN, SERVICE_RETRY_QUARANTINED_OBSERVATIONS
        ):
            hass.services.async_remove(
                DOMAIN, SERVICE_RETRY_QUARANTINED_OBSERVATIONS
            )

    async def _shutdown_runtime() -> None:
        """Stop producenten, wacht cycli uit en sluit pas daarna alle OFE's."""
        _prepare_shutdown()
        initial_setup_task = hass.data[DOMAIN].pop(
            "initial_setup_task", None
        )
        if (
            initial_setup_task is not None
            and initial_setup_task is not asyncio.current_task()
            and not initial_setup_task.done()
        ):
            initial_setup_task.cancel()
            await asyncio.gather(
                initial_setup_task, return_exceptions=True
            )
        try:
            await provider_lifecycle.async_stop_all()
        except Exception:
            _LOGGER.exception(
                "Afsluiten van providerlifecycle mislukte"
            )

        enterprise_rainrate = hass.data[DOMAIN].get(
            "noaa_enterprise_rainrate_provider"
        )
        if enterprise_rainrate is not None:
            try:
                await enterprise_rainrate.async_stop()
            except Exception:
                _LOGGER.exception(
                    "Afsluiten van NOAA Enterprise Rainrate mislukte"
                )

        # De lifecyclelock wacht de interne fetch af; deze twee cyclussloten
        # wachten vervolgens ook de verwerking van het opgehaalde resultaat af.
        # Daardoor kan geen reeds lopende cyclus ná OFE.reset() nog observaties
        # toevoegen.
        for lock_name in ("provider_cycle_lock", "radar_poll_lock"):
            cycle_lock = hass.data[DOMAIN].get(lock_name)
            if cycle_lock is not None:
                async with cycle_lock:
                    pass

        await storm_manager.async_drain_ingest()
        await _flush_region_observation_batches()
        await _flush_calibration_data()
        calibration_flush_task = hass.data[DOMAIN].get(
            "calibration_flush_task"
        )
        if calibration_flush_task is not None:
            await asyncio.gather(
                calibration_flush_task, return_exceptions=True
            )
        await _stop_region_runtime()
        radar_frame_worker = hass.data[DOMAIN].get("radar_frame_worker")
        if radar_frame_worker is not None:
            radar_frame_worker.close()
        from .worker_radar_client import close_worker_radar_client
        close_worker_radar_client()

    hass.data[DOMAIN]["async_shutdown_runtime"] = _shutdown_runtime

    @callback
    def _on_ha_stop(event):
        """Compatibiliteitsroute voor HA-versies zonder shutdown jobs."""
        _prepare_shutdown()
        hass.data[DOMAIN]["shutdown_task"] = hass.async_create_task(
            _shutdown_runtime()
        )

    add_shutdown_job = getattr(hass, "async_add_shutdown_job", None)
    if callable(add_shutdown_job):
        from homeassistant.core import HassJob
        hass.data[DOMAIN]["remove_shutdown_job"] = add_shutdown_job(
            HassJob(
                _shutdown_runtime,
                "storm_tracker_v3 ordered shutdown",
            )
        )
    else:
        hass.data[DOMAIN]["unsubscribers"].append(
            hass.bus.async_listen_once(
                EVENT_HOMEASSISTANT_STOP, _on_ha_stop
            )
        )

    _LOGGER.info(
        "Storm Tracker V3 legacy-runtime geladen; providers wachten tot "
        "Home Assistant gestart is"
    )
    return True
