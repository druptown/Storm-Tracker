"""Thin Home Assistant composition used by the explicit worker-only cutover.

This candidate only composes the MQTT worker bridge, the bounded desired-state
controller, one in-memory operational snapshot cache and the snapshot-only
sensor entities.  Importing it has no side effects; lifecycle wiring lives in
``worker_only_entry`` so this composition remains independently testable.
"""
from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
import inspect
import re
from typing import Any

from homeassistant.core import HomeAssistant

from .desired_state_builder import MAX_TARGETS, build_desired_state
from .desired_state_controller import DesiredStateController
from .operational_snapshot import OperationalSnapshotCache
from .operational_snapshot_sensors import (
    OPERATIONAL_SNAPSHOT_UPDATE_EVENT,
    SnapshotHomePrecipitationSensor,
    SnapshotOnlySensorEntity,
    SnapshotProviderStatusSensor,
    SnapshotRegionCountSensor,
    SnapshotSensorEventHub,
    SnapshotStormCountSensor,
    SnapshotTargetPrecipitationSensor,
    SnapshotWorkerStatusSensor,
    WORKER_ONLY_PROVIDER_IDS,
)
from .const import RUNTIME_MODE_WORKER_ONLY
from .worker_bridge import (
    DOMAIN,
    async_request_worker_job,
    async_setup_worker_bridge,
    mark_operational_snapshot_authority_changed,
)
from .worker_technical_diagnostics_sensor import (
    WorkerTechnicalDiagnosticsSensor,
)


RUNTIME_CONTRACT = "stv3-ha-worker-only-runtime-candidate-v1"
MAX_RUNTIME_TARGETS = MAX_TARGETS
MAX_RUNTIME_ENTITIES = 64
BASE_ENTITY_COUNT = 5 + len(WORKER_ONLY_PROVIDER_IDS)
MAX_ADDITIONAL_TARGET_ENTITIES = MAX_RUNTIME_ENTITIES - BASE_ENTITY_COUNT
MAX_DIAGNOSTIC_COUNT = 1_000_000_000
_WORKER_REQUEST_LOCK_KEY = "worker_request_lock"

_RUNTIME_CONFIG_KEYS = frozenset({
    "producer_id",
    "unique_id_prefix",
    "home_target_id",
    "sensor_target_ids",
    "settings",
})
_SAFE_TEXT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,159}$")

Cleanup = Callable[[], Any]
BridgeSetupHook = Callable[
    [HomeAssistant, OperationalSnapshotCache],
    Awaitable[Cleanup] | Cleanup,
]
EventSubscribeHook = Callable[
    [HomeAssistant, Callable[[Any], None]],
    Awaitable[Cleanup] | Cleanup,
]


class WorkerOnlyRuntimeError(RuntimeError):
    """The thin runtime cannot safely compose the supplied snapshots/hooks."""


@dataclass(frozen=True, slots=True)
class PreparedWorkerOnlyRuntime:
    """Stable identities produced once by :meth:`prepare`."""

    cache: OperationalSnapshotCache
    controller: DesiredStateController
    event_hub: "RuntimeSnapshotEventHub"
    entities: tuple[
        SnapshotOnlySensorEntity | WorkerTechnicalDiagnosticsSensor, ...
    ]
    target_count: int
    entity_count: int


@dataclass(frozen=True, slots=True)
class _NormalizedComposition:
    producer_id: str
    unique_id_prefix: str
    home_target_id: str
    sensor_target_ids: tuple[str, ...]
    targets: tuple[dict[str, Any], ...]
    settings: dict[str, Any]


def _exact_mapping(value: Any, expected: frozenset[str], name: str) -> Mapping:
    if not isinstance(value, Mapping) or set(value) != expected:
        raise ValueError(f"{name} heeft geen exact gesloten schema")
    if any(not isinstance(key, str) for key in value):
        raise ValueError(f"{name} bevat een niet-tekstuele sleutel")
    return value


def _safe_text(value: Any, name: str) -> str:
    if (
        not isinstance(value, str)
        or value != value.strip()
        or _SAFE_TEXT_RE.fullmatch(value) is None
    ):
        raise ValueError(f"{name} is geen veilig begrensde tekst")
    return value


def _normalize_composition(
    config_snapshot: Mapping[str, Any],
    target_snapshot: Sequence[Mapping[str, Any]],
) -> _NormalizedComposition:
    config = _exact_mapping(
        config_snapshot, _RUNTIME_CONFIG_KEYS, "runtime config_snapshot"
    )
    producer_id = _safe_text(config["producer_id"], "producer_id")
    unique_id_prefix = _safe_text(
        config["unique_id_prefix"], "unique_id_prefix"
    )
    home_target_id = _safe_text(config["home_target_id"], "home_target_id")
    requested_sensor_ids = config["sensor_target_ids"]
    if isinstance(requested_sensor_ids, (str, bytes, bytearray)) or not isinstance(
        requested_sensor_ids, Sequence
    ):
        raise ValueError("sensor_target_ids moet een begrensde sequence zijn")
    if len(requested_sensor_ids) > MAX_ADDITIONAL_TARGET_ENTITIES:
        raise ValueError("sensor_target_ids overschrijdt de entitygrens")
    sensor_target_ids = tuple(
        _safe_text(value, f"sensor_target_ids[{index}]")
        for index, value in enumerate(requested_sensor_ids)
    )
    if sensor_target_ids != tuple(sorted(set(sensor_target_ids))):
        raise ValueError("sensor_target_ids moet uniek en gesorteerd zijn")
    if home_target_id in sensor_target_ids:
        raise ValueError("home_target_id mag geen extra targetsensor zijn")

    normalized = build_desired_state(
        producer_id=producer_id,
        authority_epoch=1,
        generation=0,
        issued_at=0.0,
        previous_state_sha256=None,
        targets=target_snapshot,
        settings=config["settings"],
    ).as_dict()
    targets = tuple(dict(target) for target in normalized["targets"])
    if len(targets) > MAX_RUNTIME_TARGETS:
        raise ValueError("target_snapshot overschrijdt de targetgrens")
    target_by_id = {target["target_id"]: target for target in targets}
    if home_target_id not in target_by_id:
        raise ValueError("home_target_id ontbreekt in target_snapshot")
    missing = [
        target_id
        for target_id in sensor_target_ids
        if target_id not in target_by_id
    ]
    if missing:
        raise ValueError("sensor_target_ids ontbreken in target_snapshot")
    if BASE_ENTITY_COUNT + len(sensor_target_ids) > MAX_RUNTIME_ENTITIES:
        raise ValueError("snapshotsensoren overschrijden de entitygrens")
    return _NormalizedComposition(
        producer_id=producer_id,
        unique_id_prefix=unique_id_prefix,
        home_target_id=home_target_id,
        sensor_target_ids=sensor_target_ids,
        targets=targets,
        settings=dict(normalized["settings"]),
    )


async def _maybe_await(value: Any) -> Any:
    return await value if inspect.isawaitable(value) else value


async def _run_cleanup(cleanup: Cleanup | None) -> None:
    if cleanup is None:
        return
    await _maybe_await(cleanup())


def _default_event_subscribe(
    hass: HomeAssistant,
    callback: Callable[[Any], None],
) -> Cleanup:
    return hass.bus.async_listen(OPERATIONAL_SNAPSHOT_UPDATE_EVENT, callback)


async def _default_bridge_setup(
    hass: HomeAssistant,
    cache: OperationalSnapshotCache,
) -> Cleanup:
    """Set up exactly one existing bridge and return cleanup for its additions."""

    domain_data = hass.data.setdefault(DOMAIN, {})
    existing_cache = domain_data.get("operational_snapshot_cache")
    if existing_cache is not None and existing_cache is not cache:
        raise WorkerOnlyRuntimeError(
            "een andere operational snapshot cache is al actief"
        )
    if (
        _WORKER_REQUEST_LOCK_KEY in domain_data
        and not isinstance(domain_data[_WORKER_REQUEST_LOCK_KEY], asyncio.Lock)
    ):
        raise WorkerOnlyRuntimeError("worker request-lock authority is ongeldig")
    unsubscribe_registry = domain_data.setdefault("unsubscribers", [])
    if not isinstance(unsubscribe_registry, list):
        raise WorkerOnlyRuntimeError("worker bridge unsubscribe-register is ongeldig")

    request_lock_created = _WORKER_REQUEST_LOCK_KEY not in domain_data
    if request_lock_created:
        request_lock = asyncio.Lock()
        domain_data[_WORKER_REQUEST_LOCK_KEY] = request_lock
    else:
        request_lock = domain_data[_WORKER_REQUEST_LOCK_KEY]

    def release_owned_request_lock() -> None:
        if (
            not request_lock_created
            or domain_data.get(_WORKER_REQUEST_LOCK_KEY) is not request_lock
        ):
            return
        if request_lock.locked():
            raise WorkerOnlyRuntimeError(
                "worker request-lock is nog actief tijdens cleanup"
            )
        domain_data.pop(_WORKER_REQUEST_LOCK_KEY, None)

    domain_data["operational_snapshot_cache"] = cache
    domain_data["operational_snapshot_authority_required"] = True
    domain_data["operational_snapshot_expected_desired_state_sha256"] = None
    previous_ids = {id(callback) for callback in unsubscribe_registry}
    try:
        await async_setup_worker_bridge(hass)
    except BaseException:
        try:
            added = [
                callback
                for callback in tuple(unsubscribe_registry)
                if id(callback) not in previous_ids
            ]
            for callback in reversed(added):
                if callback in unsubscribe_registry:
                    unsubscribe_registry.remove(callback)
                await _run_cleanup(callback)
        finally:
            domain_data.pop("operational_snapshot_authority_required", None)
            domain_data.pop(
                "operational_snapshot_expected_desired_state_sha256", None
            )
            release_owned_request_lock()
        raise
    bridge_cleanups = tuple(
        callback
        for callback in unsubscribe_registry
        if id(callback) not in previous_ids
    )
    cleaned = False

    async def cleanup() -> None:
        nonlocal cleaned
        if cleaned:
            return
        if (
            request_lock_created
            and domain_data.get(_WORKER_REQUEST_LOCK_KEY) is request_lock
            and request_lock.locked()
        ):
            raise WorkerOnlyRuntimeError(
                "worker request-lock is nog actief tijdens cleanup"
            )
        for callback in reversed(bridge_cleanups):
            if callback in unsubscribe_registry:
                unsubscribe_registry.remove(callback)
            await _run_cleanup(callback)
        pending = domain_data.get("worker_pending_jobs", {})
        if isinstance(pending, Mapping):
            for future in tuple(pending.values()):
                if hasattr(future, "done") and not future.done():
                    future.cancel()
        domain_data.pop("operational_snapshot_authority_required", None)
        domain_data.pop(
            "operational_snapshot_expected_desired_state_sha256", None
        )
        release_owned_request_lock()
        cleaned = True

    return cleanup


class RuntimeSnapshotEventHub(SnapshotSensorEventHub):
    """One runtime-owned subscription shared by every sensor candidate."""

    __slots__ = ("_event_subscribe", "_runtime_subscription_started")

    def __init__(
        self,
        cache: OperationalSnapshotCache,
        event_subscribe: EventSubscribeHook,
    ) -> None:
        super().__init__(cache)
        if not callable(event_subscribe):
            raise TypeError("event_subscribe moet callable zijn")
        self._event_subscribe = event_subscribe
        self._runtime_subscription_started = False

    async def async_start(self, hass: HomeAssistant) -> None:
        if self._runtime_subscription_started:
            if self._hass is not hass:
                raise RuntimeError("eventhub hoort al bij een andere HA-instance")
            return
        cleanup = self._event_subscribe(hass, self._handle_snapshot_update)
        cleanup = await _maybe_await(cleanup)
        if not callable(cleanup):
            raise WorkerOnlyRuntimeError(
                "event_subscribe moet een cleanupcallback teruggeven"
            )
        self._hass = hass
        self._unsubscribe = cleanup
        self._runtime_subscription_started = True

    def attach(self, entity: SnapshotOnlySensorEntity) -> Cleanup:
        """Attach without opening a second listener; runtime owns the listener."""

        hass = entity.hass
        if not self._runtime_subscription_started or self._unsubscribe is None:
            raise RuntimeError("runtime-eventhub is nog niet gestart")
        if hass is None or hass is not self._hass:
            raise RuntimeError("snapshotsensor hoort niet bij deze HA-instance")
        key = id(entity)
        self._entities[key] = entity
        active = True

        def detach() -> None:
            nonlocal active
            if not active:
                return
            active = False
            self._entities.pop(key, None)

        return detach

    async def async_close(self) -> None:
        cleanup = self._unsubscribe
        self._unsubscribe = None
        self._runtime_subscription_started = False
        self._hass = None
        self._entities.clear()
        await _run_cleanup(cleanup)


class WorkerOnlyRuntimeCandidate:
    """Explicit lifecycle for the unwired thin worker-only HA candidate."""

    def __init__(
        self,
        hass: HomeAssistant,
        *,
        config_snapshot: Mapping[str, Any],
        target_snapshot: Sequence[Mapping[str, Any]],
        boot_id: str | None = None,
        bridge_setup: BridgeSetupHook | None = None,
        requester: Callable[..., Awaitable[dict[str, Any]]] | None = None,
        event_subscribe: EventSubscribeHook | None = None,
        clock: Callable[[], float] | None = None,
        debounce_seconds: float = 0.35,
        request_timeout: float = 35.0,
    ) -> None:
        self._hass = hass
        self._raw_config_snapshot = config_snapshot
        self._raw_target_snapshot = target_snapshot
        self._boot_id = boot_id
        self._bridge_setup = (
            _default_bridge_setup if bridge_setup is None else bridge_setup
        )
        self._requester = (
            async_request_worker_job if requester is None else requester
        )
        self._event_subscribe = (
            _default_event_subscribe
            if event_subscribe is None
            else event_subscribe
        )
        self._clock = clock
        self._debounce_seconds = debounce_seconds
        self._request_timeout = request_timeout
        if not callable(self._bridge_setup):
            raise TypeError("bridge_setup moet callable zijn")
        if not callable(self._requester):
            raise TypeError("requester moet callable zijn")
        if not callable(self._event_subscribe):
            raise TypeError("event_subscribe moet callable zijn")

        self._normalized: _NormalizedComposition | None = None
        self._prepared: PreparedWorkerOnlyRuntime | None = None
        self._bridge_cleanup: Cleanup | None = None
        self._start_task: asyncio.Task[None] | None = None
        self._started = False
        self._unloaded = False
        self._state = "new"
        self._last_error_type: str | None = None
        self._counts = {
            "prepare_calls": 0,
            "bridge_setups": 0,
            "event_subscriptions": 0,
            "controller_starts": 0,
            "updates": 0,
            "unloads": 0,
        }

    def _increment(self, name: str) -> None:
        self._counts[name] = min(MAX_DIAGNOSTIC_COUNT, self._counts[name] + 1)

    def _snapshot_authority_changed(
        self,
        value: str | None,
        *,
        preserve_current_snapshot: bool = False,
    ) -> None:
        """Bind snapshot acceptance to the controller's confirmed head."""

        mark_operational_snapshot_authority_changed(
            self._hass,
            expected_desired_state_sha256=value,
            preserve_current_snapshot=preserve_current_snapshot,
        )

    def prepare(self) -> PreparedWorkerOnlyRuntime:
        """Validate and compose once, without setup, registration or I/O."""

        self._increment("prepare_calls")
        if self._prepared is not None:
            return self._prepared
        if self._unloaded:
            raise RuntimeError("worker-only runtime is al unloaded")
        normalized = _normalize_composition(
            self._raw_config_snapshot, self._raw_target_snapshot
        )
        cache = OperationalSnapshotCache()
        hub = RuntimeSnapshotEventHub(cache, self._event_subscribe)
        controller_kwargs = {
            "producer_id": normalized.producer_id,
            "boot_id": self._boot_id,
            "requester": self._requester,
            "authority_callback": self._snapshot_authority_changed,
            "debounce_seconds": self._debounce_seconds,
            "request_timeout": self._request_timeout,
        }
        if self._clock is not None:
            controller_kwargs["clock"] = self._clock
        controller = DesiredStateController(
            self._hass,
            **controller_kwargs,
        )
        target_by_id = {
            target["target_id"]: target for target in normalized.targets
        }
        entities: list[
            SnapshotOnlySensorEntity | WorkerTechnicalDiagnosticsSensor
        ] = [
            SnapshotWorkerStatusSensor(hub, normalized.unique_id_prefix),
            WorkerTechnicalDiagnosticsSensor(normalized.unique_id_prefix),
            SnapshotHomePrecipitationSensor(
                hub,
                normalized.unique_id_prefix,
                normalized.home_target_id,
            ),
        ]
        for target_id in normalized.sensor_target_ids:
            entities.append(
                SnapshotTargetPrecipitationSensor(
                    hub,
                    normalized.unique_id_prefix,
                    target_id,
                    target_by_id[target_id]["display_name"],
                )
            )
        for provider_id in WORKER_ONLY_PROVIDER_IDS:
            entities.append(SnapshotProviderStatusSensor(
                hub,
                normalized.unique_id_prefix,
                provider_id,
            ))
        entities.extend((
            SnapshotStormCountSensor(hub, normalized.unique_id_prefix),
            SnapshotRegionCountSensor(hub, normalized.unique_id_prefix),
        ))
        if len(entities) > MAX_RUNTIME_ENTITIES:
            raise WorkerOnlyRuntimeError("runtime entitygrens is overschreden")
        self._normalized = normalized
        # Drop caller-owned containers once detached, validated bounded copies
        # exist.  A later external mutation can therefore not grow runtime state.
        self._raw_config_snapshot = None
        self._raw_target_snapshot = None
        self._prepared = PreparedWorkerOnlyRuntime(
            cache=cache,
            controller=controller,
            event_hub=hub,
            entities=tuple(entities),
            target_count=len(normalized.targets),
            entity_count=len(entities),
        )
        self._state = "prepared"
        return self._prepared

    async def _async_start_once(self, *, worker_admission_ready: bool) -> None:
        prepared = self.prepare()
        hub_started = False
        bridge_started = False
        controller_called = False
        try:
            await prepared.event_hub.async_start(self._hass)
            hub_started = True
            self._increment("event_subscriptions")
            cleanup = await _maybe_await(
                self._bridge_setup(self._hass, prepared.cache)
            )
            if not callable(cleanup):
                raise WorkerOnlyRuntimeError(
                    "bridge_setup moet een cleanupcallback teruggeven"
                )
            self._bridge_cleanup = cleanup
            bridge_started = True
            self._increment("bridge_setups")
            controller_called = True
            self._increment("controller_starts")
            assert self._normalized is not None
            await prepared.controller.async_start(
                targets=self._normalized.targets,
                settings=self._normalized.settings,
                defer_initial_flush=not worker_admission_ready,
            )
            self._started = True
            self._state = "started"
            self._last_error_type = None
        except BaseException as err:
            self._last_error_type = type(err).__name__[:64]
            self._state = "error"
            if controller_called:
                await prepared.controller.async_stop()
            if bridge_started:
                cleanup = self._bridge_cleanup
                self._bridge_cleanup = None
                await _run_cleanup(cleanup)
            if hub_started:
                await prepared.event_hub.async_close()
            raise

    async def async_start(
        self, *, worker_admission_ready: bool = True
    ) -> PreparedWorkerOnlyRuntime:
        """Start one hub subscription, one bridge and one controller."""

        prepared = self.prepare()
        if type(worker_admission_ready) is not bool:
            raise TypeError("worker_admission_ready moet bool zijn")
        if self._unloaded:
            raise RuntimeError("worker-only runtime is al unloaded")
        if self._started:
            return prepared
        task = self._start_task
        if task is None or task.done():
            task = asyncio.create_task(
                self._async_start_once(
                    worker_admission_ready=worker_admission_ready
                ),
                name="stv3-worker-only-runtime-start",
            )
            self._start_task = task
        try:
            await asyncio.shield(task)
        finally:
            if task.done() and self._start_task is task:
                self._start_task = None
        return prepared

    async def async_update(
        self,
        *,
        config_snapshot: Mapping[str, Any],
        target_snapshot: Sequence[Mapping[str, Any]],
    ) -> bool:
        """Forward one latest normalized snapshot without recomposing entities."""

        if not self._started or self._unloaded or self._prepared is None:
            raise RuntimeError("worker-only runtime is niet gestart")
        updated = _normalize_composition(config_snapshot, target_snapshot)
        current = self._normalized
        assert current is not None
        if (
            updated.producer_id != current.producer_id
            or updated.unique_id_prefix != current.unique_id_prefix
            or updated.home_target_id != current.home_target_id
            or updated.sensor_target_ids != current.sensor_target_ids
        ):
            raise ValueError("runtime-identiteit of entityset mag niet live wijzigen")
        if updated == current:
            return False
        changed = await self._prepared.controller.async_update(
            targets=updated.targets,
            settings=updated.settings,
        )
        self._normalized = updated
        if changed:
            self._increment("updates")
        return changed

    async def async_worker_reconnected(self) -> None:
        if self._started and not self._unloaded and self._prepared is not None:
            await self._prepared.controller.async_worker_reconnected()

    async def async_unload(self) -> None:
        """Remove every runtime-owned listener/task; safe to repeat."""

        if self._unloaded:
            return
        self._unloaded = True
        self._state = "unloading"
        self._increment("unloads")
        start_task = self._start_task
        if start_task is not None and not start_task.done():
            start_task.cancel()
            await asyncio.gather(start_task, return_exceptions=True)
        self._start_task = None
        first_error: BaseException | None = None
        if self._prepared is not None:
            try:
                await self._prepared.controller.async_unload()
            except BaseException as err:
                first_error = err
        cleanup = self._bridge_cleanup
        self._bridge_cleanup = None
        try:
            await _run_cleanup(cleanup)
        except BaseException as err:
            if first_error is None:
                first_error = err
        if self._prepared is not None:
            try:
                await self._prepared.event_hub.async_close()
            except BaseException as err:
                if first_error is None:
                    first_error = err
        self._started = False
        self._state = "unloaded"
        if first_error is not None:
            raise first_error

    def diagnostics(self) -> dict[str, Any]:
        """Return only bounded control metadata, never config or target facts."""

        prepared = self._prepared
        controller = None if prepared is None else prepared.controller.diagnostics()
        return {
            "contract": RUNTIME_CONTRACT,
            "runtime_mode": RUNTIME_MODE_WORKER_ONLY,
            "state": self._state,
            "prepared": prepared is not None,
            "started": self._started,
            "unloaded": self._unloaded,
            "target_count": 0 if prepared is None else prepared.target_count,
            "target_limit": MAX_RUNTIME_TARGETS,
            "entity_count": 0 if prepared is None else prepared.entity_count,
            "entity_limit": MAX_RUNTIME_ENTITIES,
            "snapshot_revision": 0 if prepared is None else prepared.cache.revision,
            "snapshot_availability": (
                "unknown" if prepared is None else prepared.cache.availability
            ),
            "controller": controller,
            "last_error_type": self._last_error_type,
            "counts": dict(self._counts),
        }


__all__ = [
    "BASE_ENTITY_COUNT",
    "MAX_ADDITIONAL_TARGET_ENTITIES",
    "MAX_RUNTIME_ENTITIES",
    "MAX_RUNTIME_TARGETS",
    "PreparedWorkerOnlyRuntime",
    "RUNTIME_CONTRACT",
    "RuntimeSnapshotEventHub",
    "WorkerOnlyRuntimeCandidate",
    "WorkerOnlyRuntimeError",
]
