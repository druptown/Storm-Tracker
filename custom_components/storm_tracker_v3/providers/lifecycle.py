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
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


class ProviderLifecycleController:
    """Activeer één gedeelde provider uitsluitend bij relevante engines."""

    def __init__(
        self,
        *,
        cooldown_seconds: float = 300.0,
        fetch_timeout_seconds: float = 20.0,
        clock=time.monotonic,
    ):
        self._cooldown_seconds = float(cooldown_seconds)
        self._fetch_timeout_seconds = float(fetch_timeout_seconds)
        self._clock = clock
        self._runtimes: dict[str, ProviderRuntime] = {}

    def register(self, plugin: ProviderPlugin, context_factory) -> None:
        if plugin.plugin_id in self._runtimes:
            raise ValueError(f"Provider al geregistreerd: {plugin.plugin_id}")
        self._runtimes[plugin.plugin_id] = ProviderRuntime(plugin, context_factory)

    def _matching(self, runtime: ProviderRuntime, areas: Iterable[CoverageArea]):
        return tuple(area for area in areas if runtime.plugin.supports(area).supported)

    async def async_reconcile(self, areas: Iterable[CoverageArea]) -> None:
        """Pas actieve providers aan de actuele RegionEngine-gebieden aan."""
        areas = tuple(areas)
        now = self._clock()
        for runtime in self._runtimes.values():
            matching = self._matching(runtime, areas)
            previous_matching = runtime.matching_areas
            runtime.matching_areas = matching
            if matching:
                runtime.cooldown_started = None
                if runtime.status in {ProviderStatus.SLEEPING, ProviderStatus.ERROR}:
                    runtime.status = ProviderStatus.INITIALIZING
                    try:
                        await runtime.plugin.async_start(
                            runtime.context_factory(runtime.plugin, matching)
                        )
                    except Exception as exc:
                        runtime.status = ProviderStatus.ERROR
                        runtime.error = type(exc).__name__
                        _LOGGER.exception("Provider %s starten mislukt", runtime.plugin.plugin_id)
                    else:
                        runtime.status = ProviderStatus.ACTIVE
                        runtime.error = None
                elif runtime.status == ProviderStatus.COOLDOWN:
                    runtime.status = ProviderStatus.ACTIVE
                elif matching != previous_matching and hasattr(
                    runtime.plugin, "async_update_areas"
                ):
                    await runtime.plugin.async_update_areas(matching)
                continue

            if runtime.status in {ProviderStatus.SLEEPING, ProviderStatus.ERROR}:
                continue
            if runtime.cooldown_started is None:
                runtime.cooldown_started = now
                runtime.status = ProviderStatus.COOLDOWN
                continue
            if now - runtime.cooldown_started < self._cooldown_seconds:
                continue
            try:
                await runtime.plugin.async_stop()
            finally:
                runtime.status = ProviderStatus.SLEEPING
                runtime.cooldown_started = None

    async def async_fetch_active(self) -> dict[str, list]:
        """Poll actieve providers parallel met een harde timeout per provider."""
        async def _fetch_one(provider_id: str, runtime: ProviderRuntime):
            async with runtime.lock:
                try:
                    observations = await asyncio.wait_for(
                        runtime.plugin.async_fetch(),
                        timeout=self._fetch_timeout_seconds,
                    )
                except asyncio.TimeoutError:
                    runtime.status = ProviderStatus.ERROR
                    runtime.error = "timeout"
                    _LOGGER.warning(
                        "Provider %s overschreed timeout van %.0f seconden",
                        provider_id,
                        self._fetch_timeout_seconds,
                    )
                    return provider_id, None
                except Exception as exc:
                    runtime.status = ProviderStatus.ERROR
                    runtime.error = type(exc).__name__
                    _LOGGER.exception("Provider %s pollen mislukt", provider_id)
                    return provider_id, None
                runtime.last_poll = time.time()
                runtime.fetched = len(observations)
                runtime.error = None
                return provider_id, observations

        pending = [
            _fetch_one(provider_id, runtime)
            for provider_id, runtime in self._runtimes.items()
            if runtime.status == ProviderStatus.ACTIVE and not runtime.lock.locked()
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
        """Stop actieve providers onmiddellijk bij het afsluiten van HA."""
        for runtime in self._runtimes.values():
            if runtime.status == ProviderStatus.SLEEPING:
                continue
            try:
                await runtime.plugin.async_stop()
            finally:
                runtime.status = ProviderStatus.SLEEPING
                runtime.matching_areas = ()
                runtime.cooldown_started = None

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
                **(details if isinstance(details, dict) else {}),
            }
        return diagnostics

    def overlay(self, provider_id: str):
        """Geef het laatste veilige kaartoverlay-contract van een provider."""
        runtime = self._runtimes.get(provider_id)
        return getattr(runtime.plugin, "overlay", None) if runtime else None
