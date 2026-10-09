"""Exclusive, lightweight config-entry lifecycle for worker-only operation.

This module is the cut-over boundary: Home Assistant observes a bounded set of
locations, publishes desired state through the existing MQTT bridge and renders
already computed operational snapshots.  It never imports a provider, radar
decoder, meteorological engine, HTTP view, database or executor workload.
"""
from __future__ import annotations

import asyncio
from collections.abc import Callable, Coroutine, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import inspect
import json
import logging
import math
import re
import time
from typing import Any
import unicodedata

from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryNotReady
from homeassistant.helpers.event import async_track_state_change_event

from .const import (
    CONF_RUNTIME_MODE,
    DATA_ENTRY_RUNTIMES,
    DATA_WORKER_TECHNICAL_DIAGNOSTICS,
    DATA_WORKER_TECHNICAL_SOURCE,
    DOMAIN,
    INTEGRATION_VERSION,
    RUNTIME_MODE_WORKER_ONLY,
)
from .worker_only_runtime import (
    MAX_ADDITIONAL_TARGET_ENTITIES,
    MAX_RUNTIME_TARGETS,
    PreparedWorkerOnlyRuntime,
    WorkerOnlyRuntimeCandidate,
)


_LOGGER = logging.getLogger(__name__)

WORKER_STATUS_UPDATE_EVENT = f"{DOMAIN}_worker_status_update"
HOME_TARGET_ID = "home"
HOME_ENTITY_ID = "zone.home"
TEST_TARGET_ID = "test_tracker"
MANAGED_TEST_TRACKER_ENTITY_ID = (
    "device_tracker.storm_tracker_fictieve_tracker"
)
MANAGED_TEST_TRACKER_DISCOVERY_TOPIC = (
    "homeassistant/device_tracker/storm_tracker_v3_fictieve_tracker/config"
)
MANAGED_TEST_TRACKER_STATE_TOPIC = (
    "homeassistant/device_tracker/fictieve_tracker/state"
)
MANAGED_TEST_TRACKER_ATTRIBUTES_TOPIC = (
    "homeassistant/device_tracker/fictieve_tracker/attributes"
)
MAX_DYNAMIC_POSITION_AGE_SECONDS = 15 * 60
MAX_DYNAMIC_POSITION_FUTURE_SKEW_SECONDS = 5 * 60
# One all-source worker pass needs at least 29 * 2 seconds before network
# latency and bounded retries.  Sampling moving Life360 targets once per
# three minutes gives that pass useful headroom without making positions
# materially stale for storm-risk routing.  Home Assistant itself keeps
# receiving and displaying every tracker update.
TARGET_RECONCILE_DELAY_SECONDS = 3 * 60.0
# A worker can spend several minutes auditing retained raw captures before its
# control endpoint is ready.  Heartbeats remain cheap, but retries stay bounded
# per continuously-online epoch so a broken worker cannot create an endless
# reconnect loop.
MAX_ONLINE_RECONNECT_ATTEMPTS = 16
MAX_ENTRY_TARGETS = min(
    MAX_RUNTIME_TARGETS,
    MAX_ADDITIONAL_TARGET_ENTITIES + 1,
)
WORKER_PROVISIONING_DIAGNOSTICS_CONTRACT = (
    "stv3-worker-provisioning-ownership-v1"
)
WORKER_PROVISIONING_OWNER_LOCATION = "supervisor_worker_app"
WORKER_PROVISIONING_STATUS = "worker_app_owned_not_observed"
WORKER_ONLY_ENTRY_CONFIG_FIELDS = frozenset({
    "persons",
    "test_tracker_entity",
    "radar_radius_km",
    "engine_sharing_distance_km",
    "netatmo_radius_km",
    "lightning_source_mode",
    "open_meteo_enabled",
})

_UNAVAILABLE_STATES = frozenset({"unknown", "unavailable"})
_REACHABLE_WORKER_STATES = frozenset({"online", "busy", "degraded"})
_DYNAMIC_TARGET_KINDS = frozenset({"person", "device_tracker", "test"})
_FRESHNESS_DEADLINE_GRACE_SECONDS = 1.0
_ENTITY_ID_RE = re.compile(r"^[a-z0-9_]+\.[a-z0-9_]+$")


class WorkerOnlyEntryError(RuntimeError):
    """The config entry cannot be started without breaking exclusivity."""


def _managed_test_tracker_discovery_payload() -> str:
    """Return the stable modern MQTT-discovery contract for the test point."""
    return json.dumps(
        {
            "name": "Fictieve Tracker",
            "unique_id": "storm_tracker_v3_fictieve_tracker",
            "default_entity_id": MANAGED_TEST_TRACKER_ENTITY_ID,
            "state_topic": MANAGED_TEST_TRACKER_STATE_TOPIC,
            "json_attributes_topic": MANAGED_TEST_TRACKER_ATTRIBUTES_TOPIC,
            "source_type": "gps",
            "origin": {
                "name": "Storm Tracker V3",
                "sw_version": INTEGRATION_VERSION,
                "support_url": "https://github.com/druptown/Storm-Tracker",
            },
        },
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )


@dataclass(frozen=True, slots=True)
class _TargetSpec:
    target_id: str
    display_name: str
    kind: str
    entity_id: str


def _maybe_await(value: Any):
    if inspect.isawaitable(value):
        return value

    async def _completed():
        return value

    return _completed()


def _finite_number(
    value: Any,
    name: str,
    *,
    minimum: float,
    maximum: float,
) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise WorkerOnlyEntryError(f"{name} moet een getal zijn")
    result = float(value)
    if not math.isfinite(result) or not minimum <= result <= maximum:
        raise WorkerOnlyEntryError(f"{name} valt buiten de geldige grens")
    return result


def _coordinates(attributes: Mapping[str, Any]) -> tuple[float, float] | None:
    raw_latitude = attributes.get("latitude")
    raw_longitude = attributes.get("longitude")
    if isinstance(raw_latitude, bool) or isinstance(raw_longitude, bool):
        return None
    try:
        latitude = float(raw_latitude)
        longitude = float(raw_longitude)
    except (TypeError, ValueError):
        return None
    if (
        not math.isfinite(latitude)
        or not math.isfinite(longitude)
        or not -90.0 <= latitude <= 90.0
        or not -180.0 <= longitude <= 180.0
    ):
        return None
    return latitude, longitude


def _state_is_available(state: Any) -> bool:
    if state is None:
        return False
    value = getattr(state, "state", None)
    return not (
        isinstance(value, str)
        and value.strip().lower() in _UNAVAILABLE_STATES
    )


def _state_attributes(state: Any) -> Mapping[str, Any]:
    value = getattr(state, "attributes", None)
    return value if isinstance(value, Mapping) else {}


def _state_timestamp(state: Any) -> float | None:
    for attribute in ("last_updated", "last_changed"):
        value = getattr(state, attribute, None)
        if isinstance(value, datetime):
            if value.tzinfo is None:
                value = value.replace(tzinfo=timezone.utc)
            result = value.timestamp()
        elif isinstance(value, (int, float)) and not isinstance(value, bool):
            result = float(value)
        else:
            continue
        if math.isfinite(result) and result >= 0.0:
            return result
    return None


def _dynamic_position_is_fresh(observed_at: float, now: float) -> bool:
    return (
        observed_at <= now + MAX_DYNAMIC_POSITION_FUTURE_SKEW_SECONDS
        and now - observed_at <= MAX_DYNAMIC_POSITION_AGE_SECONDS
    )


def _accuracy(attributes: Mapping[str, Any]) -> float | None:
    for key in ("gps_accuracy", "location_accuracy", "accuracy"):
        value = attributes.get(key)
        if value is None:
            continue
        if isinstance(value, bool):
            continue
        try:
            result = float(value)
        except (TypeError, ValueError):
            continue
        if math.isfinite(result) and 0.0 <= result <= 100_000.0:
            return result
    return None


def _country_code(attributes: Mapping[str, Any], fallback: Any = None) -> str | None:
    value = attributes.get("country_code", fallback)
    if not isinstance(value, str):
        return None
    value = value.strip().upper()
    return value if len(value) == 2 and value.isascii() and value.isalpha() else None


def _display_name(value: Any, fallback: str) -> str:
    text = value if isinstance(value, str) else fallback
    text = unicodedata.normalize("NFC", text.strip())
    if (
        not text
        or len(text) > 128
        or any(ord(character) < 32 for character in text)
    ):
        text = fallback
    return text[:128]


def _entity_id(value: Any, name: str) -> str:
    if not isinstance(value, str):
        raise WorkerOnlyEntryError(f"{name} moet een entity-id zijn")
    result = value.strip().lower()
    if _ENTITY_ID_RE.fullmatch(result) is None:
        raise WorkerOnlyEntryError(f"{name} is geen geldig entity-id")
    return result


def _entry_token(entry_id: Any) -> str:
    value = str(entry_id or "")
    if re.fullmatch(r"[A-Za-z0-9_.:@/-]{1,80}", value):
        return value
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:32]


def _on_hass_event_loop(hass: HomeAssistant) -> bool:
    """Return true only while executing on this HA instance's event loop."""

    try:
        running_loop = asyncio.get_running_loop()
    except RuntimeError:
        return False
    hass_loop = getattr(hass, "loop", None)
    return hass_loop is None or running_loop is hass_loop


def _dispatch_to_hass_event_loop(
    hass: HomeAssistant,
    callback: Callable[..., None],
    *args: Any,
) -> bool:
    """Marshal a foreign-thread callback without constructing a coroutine."""

    if _on_hass_event_loop(hass):
        return False
    loop = getattr(hass, "loop", None)
    if loop is None or loop.is_closed():
        _LOGGER.error("Worker-only eventloopdispatch geweigerd (loop_unavailable)")
        return True
    try:
        loop.call_soon_threadsafe(callback, *args)
    except RuntimeError:
        _LOGGER.error("Worker-only eventloopdispatch geweigerd (loop_closed)")
    return True


def _spawn(
    hass: HomeAssistant,
    coroutine_factory: Callable[[], Coroutine[Any, Any, Any]],
    name: str,
) -> asyncio.Task:
    """Create one task on the HA loop; never leak a foreign-thread coroutine."""

    if not _on_hass_event_loop(hass):
        raise WorkerOnlyEntryError(
            "worker-only taak mag alleen op de HA-eventloop starten"
        )
    coroutine = coroutine_factory()
    creator = getattr(hass, "async_create_task", None)
    try:
        if callable(creator):
            try:
                return creator(coroutine, name)
            except TypeError:
                return creator(coroutine)
        return asyncio.create_task(coroutine, name=name)
    except BaseException:
        coroutine.close()
        raise


class WorkerOnlyEntryRuntime:
    """Own every listener, task, platform and thin runtime for one entry."""

    def __init__(
        self,
        hass: HomeAssistant,
        entry: Any,
        *,
        runtime_factory: Callable[..., WorkerOnlyRuntimeCandidate] | None = None,
        state_listener_factory: Callable[..., Callable[[], Any]] | None = None,
        clock: Callable[[], float] = time.time,
        target_reconcile_delay_seconds: float | None = None,
    ) -> None:
        self.hass = hass
        self.entry = entry
        self._runtime_factory = runtime_factory or WorkerOnlyRuntimeCandidate
        self._state_listener_factory = (
            state_listener_factory or async_track_state_change_event
        )
        self._clock = clock
        self._target_reconcile_delay_seconds = _finite_number(
            TARGET_RECONCILE_DELAY_SECONDS
            if target_reconcile_delay_seconds is None
            else target_reconcile_delay_seconds,
            "target_reconcile_delay_seconds",
            minimum=0.0,
            maximum=15 * 60.0,
        )
        self._runtime: WorkerOnlyRuntimeCandidate | None = None
        self._prepared: PreparedWorkerOnlyRuntime | None = None
        self._target_specs: tuple[_TargetSpec, ...] = ()
        self._retained: dict[str, dict[str, Any]] = {}
        self._home_observed_at: float | None = None
        self._target_unsubscribe: Callable[[], Any] | None = None
        self._status_unsubscribe: Callable[[], Any] | None = None
        self._options_unsubscribe: Callable[[], Any] | None = None
        self._target_task: asyncio.Task | None = None
        self._freshness_reconcile_wakeup = asyncio.Event()
        self._reconnect_task: asyncio.Task | None = None
        self._freshness_handle: asyncio.TimerHandle | None = None
        self._freshness_deadline: float | None = None
        self._target_revision = 0
        self._processed_target_revision = 0
        self._pending_reconnect = False
        self._worker_control_ready = False
        self._online_reconnect_attempts = 0
        self._active = False
        self._unloaded = False
        self._runtime_unloaded = False
        self._platform_forward_attempted = False

    @property
    def prepared(self) -> PreparedWorkerOnlyRuntime | None:
        return self._prepared

    def _runtime_config(self) -> dict[str, Any]:
        """Project only HA-owned, non-secret fields into worker-only runtime."""

        projected = {
            key: self.entry.data[key]
            for key in WORKER_ONLY_ENTRY_CONFIG_FIELDS
            if key in self.entry.data
        }
        projected.update({
            key: self.entry.options[key]
            for key in WORKER_ONLY_ENTRY_CONFIG_FIELDS
            if key in self.entry.options
        })
        return projected

    async def _async_ensure_managed_test_tracker(
        self,
        raw: Mapping[str, Any],
    ) -> None:
        """Provision the persistent modern MQTT tracker before target capture."""
        if raw.get("test_tracker_entity") != MANAGED_TEST_TRACKER_ENTITY_ID:
            return

        # Keep MQTT imports inside this opt-in bootstrap path. Worker-only HA
        # still imports no provider, decoder, database or executor workload.
        from homeassistant.components import mqtt

        if not await mqtt.async_wait_for_mqtt_client(self.hass):
            raise ConfigEntryNotReady(
                "MQTT is nog niet klaar voor de fictieve tracker"
            )
        await mqtt.async_publish(
            self.hass,
            MANAGED_TEST_TRACKER_DISCOVERY_TOPIC,
            _managed_test_tracker_discovery_payload(),
            qos=0,
            retain=True,
        )

        state = self.hass.states.get(MANAGED_TEST_TRACKER_ENTITY_ID)
        if _coordinates(_state_attributes(state)) is not None:
            return

        home_state = self.hass.states.get(HOME_ENTITY_ID)
        home_coordinates = _coordinates(_state_attributes(home_state))
        if home_coordinates is None:
            home_coordinates = (
                _finite_number(
                    self.hass.config.latitude,
                    "home latitude",
                    minimum=-90.0,
                    maximum=90.0,
                ),
                _finite_number(
                    self.hass.config.longitude,
                    "home longitude",
                    minimum=-180.0,
                    maximum=180.0,
                ),
            )
        await mqtt.async_publish(
            self.hass,
            MANAGED_TEST_TRACKER_STATE_TOPIC,
            "not_home",
            qos=0,
            retain=True,
        )
        await mqtt.async_publish(
            self.hass,
            MANAGED_TEST_TRACKER_ATTRIBUTES_TOPIC,
            json.dumps(
                {
                    "latitude": home_coordinates[0],
                    "longitude": home_coordinates[1],
                    "gps_accuracy": 10,
                    "stad": "Startpositie thuis",
                },
                ensure_ascii=True,
                separators=(",", ":"),
                sort_keys=True,
            ),
            qos=0,
            retain=True,
        )

        # MQTT discovery and retained state are event-driven. Bound startup
        # waiting to five seconds; HA's ConfigEntry retry owns later recovery.
        for _attempt in range(50):
            await asyncio.sleep(0.1)
            state = self.hass.states.get(MANAGED_TEST_TRACKER_ENTITY_ID)
            if _coordinates(_state_attributes(state)) is not None:
                return
        raise ConfigEntryNotReady(
            f"Tracker {MANAGED_TEST_TRACKER_ENTITY_ID} is nog niet aangemaakt"
        )

    def _build_specs(self, raw: Mapping[str, Any]) -> tuple[_TargetSpec, ...]:
        persons = raw.get("persons", ())
        if isinstance(persons, (str, bytes, bytearray)) or not isinstance(
            persons, Sequence
        ):
            raise WorkerOnlyEntryError("persons moet een begrensde lijst zijn")
        raw_test = raw.get("test_tracker_entity")
        requested_count = 1 + len(persons) + (1 if raw_test else 0)
        if requested_count > MAX_ENTRY_TARGETS:
            raise WorkerOnlyEntryError(
                f"worker-only ondersteunt maximaal {MAX_ENTRY_TARGETS} targets"
            )
        specs = [
            _TargetSpec(HOME_TARGET_ID, "Thuis", "home", HOME_ENTITY_ID)
        ]
        seen_entities: set[str] = set()
        for index, raw_entity_id in enumerate(persons):
            entity_id = _entity_id(raw_entity_id, f"persons[{index}]")
            if entity_id in seen_entities:
                raise WorkerOnlyEntryError("een person-tracker is dubbel ingesteld")
            seen_entities.add(entity_id)
            object_id = entity_id.split(".", 1)[1]
            specs.append(_TargetSpec(
                f"person.{object_id}",
                object_id.replace("_", " ").title(),
                "person",
                entity_id,
            ))
        if raw_test:
            test_entity_id = _entity_id(raw_test, "test_tracker_entity")
            specs.append(_TargetSpec(
                TEST_TARGET_ID,
                "Fictieve tracker (test)",
                "test",
                test_entity_id,
            ))
        return tuple(specs)

    def _home_target(self, radius: float, now: float) -> dict[str, Any]:
        state = self.hass.states.get(HOME_ENTITY_ID)
        attributes = _state_attributes(state)
        coordinates = (
            _coordinates(attributes) if _state_is_available(state) else None
        )
        state_coordinates_used = coordinates is not None
        if coordinates is None:
            coordinates = (
                _finite_number(
                    self.hass.config.latitude,
                    "home latitude",
                    minimum=-90.0,
                    maximum=90.0,
                ),
                _finite_number(
                    self.hass.config.longitude,
                    "home longitude",
                    minimum=-180.0,
                    maximum=180.0,
                ),
            )
        observed_at = _state_timestamp(state) if state_coordinates_used else None
        if observed_at is None:
            if self._home_observed_at is None:
                self._home_observed_at = now
            observed_at = self._home_observed_at
        else:
            self._home_observed_at = observed_at
        return {
            "target_id": HOME_TARGET_ID,
            "display_name": "Thuis",
            "kind": "home",
            "available": True,
            "latitude": coordinates[0],
            "longitude": coordinates[1],
            "position_observed_at": observed_at,
            "location_accuracy_m": _accuracy(attributes),
            "observation_radius_km": radius,
            "country_code": _country_code(
                attributes, getattr(self.hass.config, "country", None)
            ),
        }

    def _tracked_target(
        self,
        spec: _TargetSpec,
        radius: float,
        now: float,
        *,
        initial: bool,
    ) -> dict[str, Any] | None:
        state = self.hass.states.get(spec.entity_id)
        attributes = _state_attributes(state)
        coordinates = _coordinates(attributes) if _state_is_available(state) else None
        retained = self._retained.get(spec.target_id)
        if coordinates is None:
            if retained is None:
                return None
            unavailable = dict(retained)
            unavailable["available"] = False
            unavailable["observation_radius_km"] = radius
            return unavailable

        observed_at = _state_timestamp(state)
        # The explicitly configured fictive tracker is an operator-controlled
        # test point, not a telemetry feed. Its HA state commonly remains
        # unchanged while it is deliberately parked in another country. Keep
        # the coordinates current for control-authority purposes as long as
        # the entity itself still exposes valid coordinates; real Life360
        # targets retain the strict 15-minute freshness gate below.
        if spec.kind == "test":
            observed_at = now
        if observed_at is None:
            if retained is not None and (
                retained["latitude"], retained["longitude"]
            ) == coordinates:
                observed_at = retained["position_observed_at"]
            else:
                observed_at = now
        if (
            retained is not None
            and observed_at < retained["position_observed_at"]
        ):
            current = dict(retained)
            current["available"] = _dynamic_position_is_fresh(
                current["position_observed_at"], now
            )
            current["observation_radius_km"] = radius
            return current
        target = {
            "target_id": spec.target_id,
            "display_name": _display_name(
                attributes.get("friendly_name"), spec.display_name
            ),
            "kind": spec.kind,
            "available": _dynamic_position_is_fresh(observed_at, now),
            "latitude": coordinates[0],
            "longitude": coordinates[1],
            "position_observed_at": observed_at,
            "location_accuracy_m": _accuracy(attributes),
            "observation_radius_km": radius,
            "country_code": _country_code(
                attributes, attributes.get("country")
            ),
        }
        self._retained[spec.target_id] = dict(target)
        return target

    def _snapshots(
        self,
        *,
        initial: bool,
    ) -> tuple[dict[str, Any], tuple[dict[str, Any], ...]]:
        raw = self._runtime_config()
        radius = _finite_number(
            raw.get("radar_radius_km", 300.0),
            "radar_radius_km",
            minimum=1.0,
            maximum=1000.0,
        )
        now = float(self._clock())
        if not math.isfinite(now) or now < 0.0:
            raise WorkerOnlyEntryError("runtimeklok is ongeldig")
        targets: list[dict[str, Any]] = [self._home_target(radius, now)]
        for spec in self._target_specs[1:]:
            target = self._tracked_target(
                spec, radius, now, initial=initial
            )
            if target is not None:
                targets.append(target)
        sensor_ids = sorted(target["target_id"] for target in targets[1:])
        token = _entry_token(self.entry.entry_id)
        config_snapshot = {
            "producer_id": f"homeassistant.{DOMAIN}.{token}",
            "unique_id_prefix": f"{DOMAIN}.{token}",
            "home_target_id": HOME_TARGET_ID,
            "sensor_target_ids": sensor_ids,
            "settings": {
                "ground_radius_km": _finite_number(
                    raw.get("netatmo_radius_km", 20.0),
                    "netatmo_radius_km",
                    minimum=1.0,
                    maximum=100.0,
                ),
                "lightning_source_mode": raw.get(
                    "lightning_source_mode", "auto"
                ),
                "model_guidance_enabled": bool(
                    raw.get("open_meteo_enabled", False)
                ),
                "region_sharing_distance_km": _finite_number(
                    raw.get("engine_sharing_distance_km", 150.0),
                    "engine_sharing_distance_km",
                    minimum=1.0,
                    maximum=500.0,
                ),
            },
        }
        return config_snapshot, tuple(targets)

    def _entry_record(self) -> dict[str, Any]:
        return {
            "mode": RUNTIME_MODE_WORKER_ONLY,
            "runtime_mode": RUNTIME_MODE_WORKER_ONLY,
            "manager": self,
            "runtime": self._runtime,
            "prepared": self._prepared,
        }

    def _register_record(self) -> None:
        domain_data = self.hass.data.setdefault(DOMAIN, {})
        # The MQTT bridge, cache and desired-state authority are process-wide
        # singletons.  ConfigFlow's UI guard is not a sufficient runtime fence:
        # restored/corrupt entries and YAML can bypass it.  Claim only a truly
        # empty domain slot, synchronously and before the first setup await.
        if domain_data:
            raise WorkerOnlyEntryError(
                "een andere Storm Tracker-runtime bezit de globale runtime"
            )
        records: dict[str, Any] = {}
        domain_data[DATA_ENTRY_RUNTIMES] = records
        records[self.entry.entry_id] = self._entry_record()
        domain_data["runtime_mode"] = RUNTIME_MODE_WORKER_ONLY
        domain_data["runtime_diagnostics"] = {
            "runtime_mode": RUNTIME_MODE_WORKER_ONLY,
            "selection_source": (
                "options"
                if CONF_RUNTIME_MODE in self.entry.options
                else "data"
            ),
            "legacy_compute_enabled": False,
            "provisioning": {
                "contract": WORKER_PROVISIONING_DIAGNOSTICS_CONTRACT,
                "owner_location": WORKER_PROVISIONING_OWNER_LOCATION,
                "status": WORKER_PROVISIONING_STATUS,
                "availability_claimed": False,
            },
        }

    def _remove_record(self) -> None:
        domain_data = self.hass.data.get(DOMAIN)
        if not isinstance(domain_data, dict):
            return
        records = domain_data.get(DATA_ENTRY_RUNTIMES)
        if isinstance(records, dict):
            record = records.get(self.entry.entry_id)
            if isinstance(record, Mapping) and record.get("manager") is self:
                records.pop(self.entry.entry_id, None)
            if not records:
                domain_data.pop(DATA_ENTRY_RUNTIMES, None)

    def _worker_is_online(self) -> bool:
        status = self.hass.data.get(DOMAIN, {}).get("worker_status", {})
        return (
            isinstance(status, Mapping)
            and status.get("state") in _REACHABLE_WORKER_STATES
        )

    def _worker_admission_is_ready(self) -> bool:
        """Require both a live heartbeat and explicit worker job admission."""

        status = self.hass.data.get(DOMAIN, {}).get("worker_status", {})
        return (
            isinstance(status, Mapping)
            and status.get("state") in _REACHABLE_WORKER_STATES
            and status.get("job_admission_open") is True
        )

    def _online_status_can_recover_controller(self) -> bool:
        """Return whether a fresh reachable status can unblock control I/O."""

        runtime = self._runtime
        diagnostics = getattr(runtime, "diagnostics", None)
        if not callable(diagnostics):
            return False
        try:
            controller = diagnostics().get("controller")
        except Exception as err:
            _LOGGER.debug(
                "Worker-only controllerdiagnostiek geweigerd (%s)",
                type(err).__name__,
            )
            return False
        return (
            isinstance(controller, Mapping)
            and controller.get("retry_blocked") is True
            and controller.get("last_error_code")
            in {"worker_offline", "worker_timeout"}
        )

    def _register_listeners(self) -> None:
        entity_ids = tuple(sorted({spec.entity_id for spec in self._target_specs}))
        target_unsubscribe = self._state_listener_factory(
            self.hass, entity_ids, self._on_target_state_event
        )
        if not callable(target_unsubscribe):
            raise WorkerOnlyEntryError("targetlistener gaf geen cleanup terug")
        self._target_unsubscribe = target_unsubscribe
        self._worker_control_ready = self._worker_admission_is_ready()
        status_unsubscribe = self.hass.bus.async_listen(
            WORKER_STATUS_UPDATE_EVENT, self._on_worker_status_event
        )
        if not callable(status_unsubscribe):
            raise WorkerOnlyEntryError("workerstatuslistener gaf geen cleanup terug")
        self._status_unsubscribe = status_unsubscribe

    def _on_target_state_event(self, _event: Any) -> None:
        if _dispatch_to_hass_event_loop(
            self.hass, self._on_target_state_event, _event
        ):
            return
        if self._unloaded:
            return
        self._target_revision += 1
        if self._active:
            self._schedule_target_reconcile()

    def _schedule_target_reconcile(self) -> None:
        if self._unloaded or not self._active:
            return
        task = self._target_task
        if task is None or task.done():
            self._target_task = _spawn(
                self.hass,
                self._async_reconcile_targets,
                f"{DOMAIN}-worker-only-targets",
            )

    def _cancel_freshness_deadline(self) -> None:
        handle = self._freshness_handle
        self._freshness_handle = None
        self._freshness_deadline = None
        if handle is not None:
            handle.cancel()

    def _arm_freshness_deadline(self, deadline: float) -> None:
        now = float(self._clock())
        if not math.isfinite(now) or now < 0.0:
            raise WorkerOnlyEntryError("runtimeklok is ongeldig")
        loop = getattr(self.hass, "loop", None)
        if loop is None or loop.is_closed():
            raise WorkerOnlyEntryError("freshnessdeadline mist HA-eventloop")
        self._freshness_deadline = deadline
        self._freshness_handle = loop.call_later(
            max(0.0, deadline - now),
            self._on_freshness_deadline,
        )

    def _reschedule_freshness_deadline(
        self,
        target_snapshot: Sequence[Mapping[str, Any]],
    ) -> None:
        self._cancel_freshness_deadline()
        if self._unloaded or not self._active:
            return
        deadlines = tuple(
            float(target["position_observed_at"])
            + MAX_DYNAMIC_POSITION_AGE_SECONDS
            + _FRESHNESS_DEADLINE_GRACE_SECONDS
            for target in target_snapshot
            if target.get("available") is True
            and target.get("kind") in _DYNAMIC_TARGET_KINDS
            and isinstance(target.get("position_observed_at"), (int, float))
            and not isinstance(target.get("position_observed_at"), bool)
            and math.isfinite(float(target["position_observed_at"]))
        )
        if deadlines:
            self._arm_freshness_deadline(min(deadlines))

    def _on_freshness_deadline(self) -> None:
        if _dispatch_to_hass_event_loop(
            self.hass, self._on_freshness_deadline
        ):
            return
        deadline = self._freshness_deadline
        self._freshness_handle = None
        self._freshness_deadline = None
        if self._unloaded or not self._active:
            return
        try:
            now = float(self._clock())
        except (TypeError, ValueError):
            now = math.nan
        if deadline is not None and math.isfinite(now) and now < deadline:
            try:
                self._arm_freshness_deadline(deadline)
            except Exception as err:
                _LOGGER.error(
                    "Worker-only freshnessdeadline geweigerd (%s)",
                    type(err).__name__,
                )
            return
        self._target_revision += 1
        # Expiring a dynamic position is a safety transition, not another
        # bursty Life360 update.  Wake the existing debounce task (or make the
        # next one immediate) so one stale person cannot leave the worker's
        # complete desired state invalid for the normal three-minute window.
        self._freshness_reconcile_wakeup.set()
        self._schedule_target_reconcile()

    async def _async_reconcile_targets(self) -> None:
        try:
            while (
                not self._unloaded
                and self._processed_target_revision < self._target_revision
            ):
                # Life360 updates several trackers continuously.  Publish one
                # latest-wins snapshot per bounded window so the worker gets a
                # stable authority interval in which to commit and publish.
                # A revision arriving during async_update waits for the next
                # window instead of immediately superseding the in-flight one.
                if self._freshness_reconcile_wakeup.is_set():
                    self._freshness_reconcile_wakeup.clear()
                    await asyncio.sleep(0)
                elif self._target_reconcile_delay_seconds > 0.0:
                    try:
                        await asyncio.wait_for(
                            self._freshness_reconcile_wakeup.wait(),
                            timeout=self._target_reconcile_delay_seconds,
                        )
                    except TimeoutError:
                        pass
                    finally:
                        self._freshness_reconcile_wakeup.clear()
                else:
                    await asyncio.sleep(0)
                if self._unloaded:
                    return
                revision = self._target_revision
                config_snapshot, target_snapshot = self._snapshots(initial=False)
                runtime = self._runtime
                if runtime is None:
                    return
                await runtime.async_update(
                    config_snapshot=config_snapshot,
                    target_snapshot=target_snapshot,
                )
                self._processed_target_revision = revision
                self._reschedule_freshness_deadline(target_snapshot)
        except asyncio.CancelledError:
            raise
        except Exception as err:
            _LOGGER.error(
                "Worker-only targetsnapshot geweigerd (%s)",
                type(err).__name__,
            )

    def _on_worker_status_event(self, _event: Any) -> None:
        if _dispatch_to_hass_event_loop(
            self.hass, self._on_worker_status_event, _event
        ):
            return
        if self._unloaded:
            return
        ready = self._worker_admission_is_ready()
        transition = ready and not self._worker_control_ready
        self._worker_control_ready = ready
        if not ready:
            self._online_reconnect_attempts = 0
            return
        if self._online_reconnect_attempts >= MAX_ONLINE_RECONNECT_ATTEMPTS:
            return
        recover_blocked = self._online_status_can_recover_controller()
        if not transition and not recover_blocked:
            return
        # A reachable heartbeat may repair a controller that missed the
        # offline->online edge.  Only a retry-blocked offline/timeout controller
        # is eligible after that edge, and the number of actual reconnect tasks
        # stays bounded per online epoch.
        if not self._active:
            self._pending_reconnect = True
            return
        self._schedule_reconnect()

    def _schedule_reconnect(self) -> bool:
        task = self._reconnect_task
        if (
            self._unloaded
            or (task is not None and not task.done())
            or self._online_reconnect_attempts >= MAX_ONLINE_RECONNECT_ATTEMPTS
        ):
            return False
        self._online_reconnect_attempts += 1
        self._reconnect_task = _spawn(
            self.hass,
            self._async_reconnect,
            f"{DOMAIN}-worker-only-reconnect",
        )
        return True

    async def _async_reconnect(self) -> None:
        try:
            runtime = self._runtime
            if runtime is not None and not self._unloaded:
                await runtime.async_worker_reconnected()
        except asyncio.CancelledError:
            raise
        except Exception as err:
            _LOGGER.error(
                "Worker-only reconnect geweigerd (%s)", type(err).__name__
            )

    async def _async_options_updated(self, hass: HomeAssistant, entry: Any) -> None:
        await hass.config_entries.async_reload(entry.entry_id)

    async def async_setup(self) -> bool:
        raw = self._runtime_config()
        await self._async_ensure_managed_test_tracker(raw)
        self._target_specs = self._build_specs(raw)
        config_snapshot, target_snapshot = self._snapshots(initial=True)
        self._runtime = self._runtime_factory(
            self.hass,
            config_snapshot=config_snapshot,
            target_snapshot=target_snapshot,
        )
        try:
            self._prepared = self._runtime.prepare()
            self._register_record()
            self._register_listeners()
            await self._runtime.async_start(
                worker_admission_ready=self._worker_admission_is_ready()
            )
            self._active = True
            # A retained MQTT status can arrive while ``async_start`` is
            # awaiting the controller's first (offline) flush.  Re-sample once
            # after activation and collapse that state with any startup edge
            # into exactly one reconnect task.
            ready_after_start = self._worker_admission_is_ready()
            self._worker_control_ready = ready_after_start
            if self._pending_reconnect or ready_after_start:
                self._pending_reconnect = False
                self._schedule_reconnect()
            self._reschedule_freshness_deadline(target_snapshot)
            if self._target_revision:
                self._schedule_target_reconcile()
            unsubscribe = self.entry.add_update_listener(
                self._async_options_updated
            )
            if not callable(unsubscribe):
                raise WorkerOnlyEntryError(
                    "optionslistener gaf geen cleanupcallback terug"
                )
            self._options_unsubscribe = unsubscribe
            # Register every fallible local lifecycle hook before forwarding
            # the platform.  After a successful forward there is deliberately
            # no remaining operation that can fail and strand live entities.
            self._platform_forward_attempted = True
            await self.hass.config_entries.async_forward_entry_setups(
                self.entry, ["sensor"]
            )
            return True
        except BaseException:
            if self._platform_forward_attempted:
                try:
                    await self.hass.config_entries.async_unload_platforms(
                        self.entry, ["sensor"]
                    )
                except BaseException as err:
                    _LOGGER.error(
                        "Worker-only platformrollback faalde (%s)",
                        type(err).__name__,
                    )
            try:
                await self.async_unload()
            except BaseException as err:
                _LOGGER.error(
                    "Worker-only runtimerollback faalde (%s)",
                    type(err).__name__,
                )
            raise

    async def async_unload(self) -> None:
        if self._unloaded:
            return
        self._unloaded = True
        self._active = False
        self._cancel_freshness_deadline()
        tasks = tuple(
            task
            for task in (self._target_task, self._reconnect_task)
            if task is not None and not task.done()
        )
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._target_task = None
        self._reconnect_task = None
        first_error: BaseException | None = None
        for attribute in (
            "_options_unsubscribe",
            "_status_unsubscribe",
            "_target_unsubscribe",
        ):
            cleanup = getattr(self, attribute)
            setattr(self, attribute, None)
            if cleanup is None:
                continue
            try:
                await _maybe_await(cleanup())
            except BaseException as err:
                if first_error is None:
                    first_error = err
        if self._runtime is not None and not self._runtime_unloaded:
            self._runtime_unloaded = True
            try:
                await self._runtime.async_unload()
            except BaseException as err:
                if first_error is None:
                    first_error = err
        self._remove_record()
        self._clean_domain_data()
        if first_error is not None:
            raise first_error

    def _clean_domain_data(self) -> None:
        domain_data = self.hass.data.get(DOMAIN)
        if not isinstance(domain_data, dict):
            return
        records = domain_data.get(DATA_ENTRY_RUNTIMES)
        if isinstance(records, Mapping) and records:
            return
        for key in (
            "operational_snapshot_cache",
            "operational_snapshot_diagnostics",
            "operational_snapshot_authority_required",
            "operational_snapshot_expected_desired_state_sha256",
            "worker_status",
            "worker_pending_jobs",
            "worker_passages",
            "worker_validation",
            "geojson_worker_collection",
            "geojson_worker_status",
            "_geojson_mqtt_receipt_logged",
            "runtime_mode",
            "runtime_diagnostics",
            "geojson_http_registered",
            DATA_WORKER_TECHNICAL_DIAGNOSTICS,
            DATA_WORKER_TECHNICAL_SOURCE,
        ):
            domain_data.pop(key, None)
        unsubscribers = domain_data.get("unsubscribers")
        if isinstance(unsubscribers, list) and not unsubscribers:
            domain_data.pop("unsubscribers", None)
        if not domain_data:
            self.hass.data.pop(DOMAIN, None)


def get_prepared_worker_only_runtime(
    hass: HomeAssistant,
    entry_id: str,
) -> PreparedWorkerOnlyRuntime:
    """Return only the prepared runtime belonging to this exact entry."""

    record = (
        hass.data.get(DOMAIN, {})
        .get(DATA_ENTRY_RUNTIMES, {})
        .get(entry_id)
    )
    if (
        not isinstance(record, Mapping)
        or record.get("mode") != RUNTIME_MODE_WORKER_ONLY
        or not isinstance(record.get("prepared"), PreparedWorkerOnlyRuntime)
    ):
        raise WorkerOnlyEntryError("worker-only runtime is niet voorbereid")
    return record["prepared"]


async def async_setup_worker_only_entry(hass: HomeAssistant, entry: Any) -> bool:
    manager = WorkerOnlyEntryRuntime(hass, entry)
    return await manager.async_setup()


async def async_unload_worker_only_entry(hass: HomeAssistant, entry: Any) -> bool:
    record = (
        hass.data.get(DOMAIN, {})
        .get(DATA_ENTRY_RUNTIMES, {})
        .get(entry.entry_id)
    )
    if not isinstance(record, Mapping) or record.get("mode") != RUNTIME_MODE_WORKER_ONLY:
        return False
    manager = record.get("manager")
    if not isinstance(manager, WorkerOnlyEntryRuntime):
        return False
    unloaded = await hass.config_entries.async_unload_platforms(entry, ["sensor"])
    if not unloaded:
        return False
    await manager.async_unload()
    return True


__all__ = [
    "HOME_ENTITY_ID",
    "HOME_TARGET_ID",
    "MAX_ENTRY_TARGETS",
    "MAX_ONLINE_RECONNECT_ATTEMPTS",
    "TARGET_RECONCILE_DELAY_SECONDS",
    "TEST_TARGET_ID",
    "WORKER_STATUS_UPDATE_EVENT",
    "WORKER_ONLY_ENTRY_CONFIG_FIELDS",
    "WORKER_PROVISIONING_DIAGNOSTICS_CONTRACT",
    "WORKER_PROVISIONING_OWNER_LOCATION",
    "WORKER_PROVISIONING_STATUS",
    "WorkerOnlyEntryError",
    "WorkerOnlyEntryRuntime",
    "async_setup_worker_only_entry",
    "async_unload_worker_only_entry",
    "get_prepared_worker_only_runtime",
]
