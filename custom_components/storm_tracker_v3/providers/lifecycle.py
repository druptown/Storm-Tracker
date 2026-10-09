"""Gedeelde slaap-/activatielifecycle voor locatiegebonden providers."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from enum import StrEnum
import logging
import time
from typing import Callable, Iterable

from .base import CoverageArea, ProviderContext, ProviderPlugin

_LOGGER = logging.getLogger(__name__)


class ProviderStatus(StrEnum):
    SLEEPING = "sleeping"
    INITIALIZING = "initializing"
    ACTIVE = "active"
    COOLDOWN = "cooldown"
    STALE = "stale"
    RATE_LIMITED = "rate_limited"
    ERROR = "error"


@dataclass
class ProviderRuntime:
    plugin: ProviderPlugin
    context_factory: Callable[[ProviderPlugin, tuple[CoverageArea, ...]], ProviderContext]
    status: ProviderStatus = ProviderStatus.SLEEPING
    matching_areas: tuple[CoverageArea, ...] = ()
    cooldown_started: float | None = None
    last_poll: float | None = None
    fetched: int = 0
    error: str | None = None
    consecutive_failures: int = 0
    circuit_open_until: float | None = None
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    fetch_task: asyncio.Task | None = None


class ProviderLifecycleController:
    """Activeer één gedeelde provider uitsluitend bij relevante engines."""

    def __init__(
        self,
        *,
        cooldown_seconds: float = 300.0,
        fetch_timeout_seconds: float = 20.0,
        failure_threshold: int = 3,
        circuit_breaker_seconds: float = 900.0,
        clock=time.monotonic,
        fault_injector: Callable[[str], None] | None = None,
    ):
        self._cooldown_seconds = float(cooldown_seconds)
        self._fetch_timeout_seconds = float(fetch_timeout_seconds)
        self._failure_threshold = max(1, int(failure_threshold))
        self._circuit_breaker_seconds = float(circuit_breaker_seconds)
        self._clock = clock
        if fault_injector is not None and not callable(fault_injector):
            raise TypeError("fault_injector must be callable")
        self._fault_injector = fault_injector
        self._runtimes: dict[str, ProviderRuntime] = {}
        self._lifecycle_lock = asyncio.Lock()
        self._stopping = False

    def _checkpoint(self, point: str) -> None:
        if self._fault_injector is not None:
            self._fault_injector(point)

    def register(self, plugin: ProviderPlugin, context_factory) -> None:
        if plugin.plugin_id in self._runtimes:
            raise ValueError(f"Provider al geregistreerd: {plugin.plugin_id}")
        self._runtimes[plugin.plugin_id] = ProviderRuntime(plugin, context_factory)

    def _matching(self, runtime: ProviderRuntime, areas: Iterable[CoverageArea]):
        return tuple(area for area in areas if runtime.plugin.supports(area).supported)

    async def async_reconcile(self, areas: Iterable[CoverageArea]) -> None:
        """Pas actieve providers aan de actuele RegionEngine-gebieden aan."""
        async with self._lifecycle_lock:
            if self._stopping:
                return
            areas = tuple(areas)
            now = self._clock()
            for runtime in self._runtimes.values():
                matching = self._matching(runtime, areas)
                previous_matching = runtime.matching_areas
                runtime.matching_areas = matching
                if matching:
                    if runtime.circuit_open_until is not None:
                        if now < runtime.circuit_open_until:
                            runtime.status = ProviderStatus.COOLDOWN
                            continue
                        # Half-open: laat precies de volgende normale fetch als
                        # herstelprobe door. Succes sluit het circuit opnieuw.
                        runtime.circuit_open_until = None
                        runtime.status = ProviderStatus.ACTIVE
                    runtime.cooldown_started = None
                    if runtime.status == ProviderStatus.SLEEPING:
                        runtime.status = ProviderStatus.INITIALIZING
                        try:
                            self._checkpoint("lifecycle.start.before")
                            await runtime.plugin.async_start(
                                runtime.context_factory(
                                    runtime.plugin, matching
                                )
                            )
                            self._checkpoint("lifecycle.start.after")
                        except Exception as exc:
                            runtime.status = ProviderStatus.ERROR
                            runtime.error = type(exc).__name__
                            _LOGGER.exception(
                                "Provider %s starten mislukt",
                                runtime.plugin.plugin_id,
                            )
                        else:
                            runtime.status = ProviderStatus.ACTIVE
                            runtime.error = None
                    elif runtime.status in {
                        ProviderStatus.COOLDOWN,
                        ProviderStatus.ERROR,
                    }:
                        runtime.status = ProviderStatus.ACTIVE
                    elif matching != previous_matching and hasattr(
                        runtime.plugin, "async_update_areas"
                    ):
                        self._checkpoint("lifecycle.update_areas.before")
                        await runtime.plugin.async_update_areas(matching)
                        self._checkpoint("lifecycle.update_areas.after")
                    continue

                if runtime.status in {
                    ProviderStatus.SLEEPING,
                    ProviderStatus.ERROR,
                }:
                    continue
                if runtime.cooldown_started is None:
                    runtime.cooldown_started = now
                    runtime.status = ProviderStatus.COOLDOWN
                    continue
                if now - runtime.cooldown_started < self._cooldown_seconds:
                    continue
                try:
                    self._checkpoint("lifecycle.cooldown_stop.before")
                    await runtime.plugin.async_stop()
                    self._checkpoint("lifecycle.cooldown_stop.after")
                finally:
                    runtime.status = ProviderStatus.SLEEPING
                    runtime.cooldown_started = None

    async def async_fetch_active(self) -> dict[str, list]:
        """Poll actieve providers parallel met een harde timeout per provider."""
        if self._stopping:
            return {}

        def _record_failure(runtime: ProviderRuntime, error: str) -> None:
            runtime.consecutive_failures += 1
            runtime.error = error
            if runtime.consecutive_failures >= self._failure_threshold:
                runtime.status = ProviderStatus.COOLDOWN
                runtime.circuit_open_until = (
                    self._clock() + self._circuit_breaker_seconds
                )
            else:
                # Blijf fetchbaar voor de volgende cyclus, maar ongezond voor
                # bronselectie zolang de fout niet door succes is gewist.
                runtime.status = ProviderStatus.ACTIVE

        async def _fetch_one(provider_id: str, runtime: ProviderRuntime):
            async with runtime.lock:
                if self._stopping:
                    return provider_id, None
                self._checkpoint("lifecycle.fetch.before")
                fetch_task = asyncio.create_task(
                    runtime.plugin.async_fetch(),
                    name=f"storm_tracker fetch {provider_id}",
                )
                runtime.fetch_task = fetch_task
                try:
                    observations = await asyncio.wait_for(
                        fetch_task,
                        timeout=self._fetch_timeout_seconds,
                    )
                    self._checkpoint("lifecycle.fetch.after")
                except asyncio.CancelledError:
                    if self._stopping:
                        return provider_id, None
                    raise
                except asyncio.TimeoutError:
                    _record_failure(runtime, "timeout")
                    _LOGGER.warning(
                        "Provider %s overschreed timeout van %.0f seconden",
                        provider_id,
                        self._fetch_timeout_seconds,
                    )
                    return provider_id, None
                except Exception as exc:
                    _record_failure(runtime, type(exc).__name__)
                    _LOGGER.exception(
                        "Provider %s pollen mislukt", provider_id
                    )
                    return provider_id, None
                finally:
                    if runtime.fetch_task is fetch_task:
                        runtime.fetch_task = None
                runtime.last_poll = time.time()
                runtime.fetched = len(observations)
                runtime.error = None
                runtime.consecutive_failures = 0
                runtime.circuit_open_until = None
                return provider_id, observations

        pending = [
            _fetch_one(provider_id, runtime)
            for provider_id, runtime in self._runtimes.items()
            if (
                not self._stopping
                and runtime.status == ProviderStatus.ACTIVE
                and not runtime.lock.locked()
            )
        ]
        if not pending:
            return {}
        fetched = await asyncio.gather(*pending)
        return {
            provider_id: observations
            for provider_id, observations in fetched
            if observations is not None
        }

    async def async_stop_all(self) -> None:
        """Sluit admission en wacht op iedere fetch vóór providerstop."""

        async def _stop_one(runtime: ProviderRuntime) -> None:
            async with runtime.lock:
                if runtime.status != ProviderStatus.SLEEPING:
                    try:
                        self._checkpoint(
                            "lifecycle.shutdown.provider_stop.before"
                        )
                        await runtime.plugin.async_stop()
                        self._checkpoint(
                            "lifecycle.shutdown.provider_stop.after"
                        )
                    except Exception:
                        _LOGGER.exception(
                            "Provider %s stoppen mislukt",
                            runtime.plugin.plugin_id,
                        )
                runtime.status = ProviderStatus.SLEEPING
                runtime.matching_areas = ()
                runtime.cooldown_started = None
                runtime.circuit_open_until = None

        async with self._lifecycle_lock:
            self._stopping = True
            self._checkpoint("lifecycle.shutdown.admission_closed")
            active_fetches = [
                runtime.fetch_task
                for runtime in self._runtimes.values()
                if (
                    runtime.fetch_task is not None
                    and not runtime.fetch_task.done()
                )
            ]
            for fetch_task in active_fetches:
                fetch_task.cancel()
            self._checkpoint("lifecycle.shutdown.cancel_requested")
            if active_fetches:
                await asyncio.gather(
                    *active_fetches, return_exceptions=True
                )
            self._checkpoint("lifecycle.shutdown.fetches_drained")
            await asyncio.gather(
                *(
                    _stop_one(runtime)
                    for runtime in self._runtimes.values()
                )
            )
            self._checkpoint("lifecycle.shutdown.complete")

    def diagnostics(self) -> dict[str, dict]:
        diagnostics = {}
        for provider_id, runtime in self._runtimes.items():
            details = getattr(runtime.plugin, "diagnostics", {})
            diagnostics[provider_id] = {
                "status": runtime.status.value,
                "matching_engines": len(runtime.matching_areas),
                "last_poll": runtime.last_poll,
                "fetched": runtime.fetched,
                "error": runtime.error,
                "consecutive_failures": runtime.consecutive_failures,
                "circuit_open_until": runtime.circuit_open_until,
                **(details if isinstance(details, dict) else {}),
            }
        return diagnostics

    def overlay(self, provider_id: str):
        """Geef het laatste veilige kaartoverlay-contract van een provider."""
        runtime = self._runtimes.get(provider_id)
        return getattr(runtime.plugin, "overlay", None) if runtime else None

    def artifact(self, provider_id: str):
        """Geef het gevalideerde workerartefact van precies deze provider."""
        runtime = self._runtimes.get(provider_id)
        return (
            getattr(runtime.plugin, "last_worker_artifact", None)
            if runtime else None
        )
