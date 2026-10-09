"""Failure-isolated concurrent execution for thin provider adapters."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import time
from typing import Any, Callable, Mapping

from .thin_observation import NormalizedObservation
from .thin_provider_adapters import ProviderBlocked, ThinAdapterResult, ThinRadarField
from .thin_provider_catalog import ThinProviderRequest


@dataclass(frozen=True, slots=True)
class ProviderResult:
    provider_id: str
    ready: bool
    status: str
    reason_code: str | None
    observations: tuple[NormalizedObservation, ...]
    attempted_at: float
    completed_at: float
    radar_fields: tuple[ThinRadarField, ...] = ()


class ThinProviderRuntime:
    def __init__(
        self,
        *,
        adapters: Mapping[str, Any],
        fetcher: Any,
        max_parallel: int = 8,
        timeout_seconds: float = 45.0,
        retries: int = 2,
        retry_delay_seconds: float = 0.25,
        clock: Callable[[], float] = time.time,
        request_preparer: Any | None = None,
    ) -> None:
        if max_parallel < 1:
            raise ValueError("max_parallel must be positive")
        if retries < 0 or retries > 2:
            raise ValueError("retries must be between zero and two")
        self._adapters = dict(adapters)
        self._fetcher = fetcher
        self._semaphore = asyncio.Semaphore(max_parallel)
        self._timeout_seconds = float(timeout_seconds)
        self._retries = int(retries)
        self._retry_delay_seconds = float(retry_delay_seconds)
        self._clock = clock
        self._request_preparer = request_preparer
        self._locks = {provider_id: asyncio.Lock() for provider_id in self._adapters}
        self._failure_counts: dict[str, int] = {}
        self._backoff_until: dict[str, float] = {}

    @property
    def ready_provider_ids(self) -> frozenset[str]:
        """Providers with a complete adapter, independent of current scope."""
        return frozenset(self._adapters)

    async def _one(self, request: ThinProviderRequest) -> ProviderResult:
        attempted_at = float(self._clock())
        adapter = self._adapters.get(request.provider_id)
        if adapter is None:
            return ProviderResult(
                provider_id=request.provider_id,
                ready=False,
                status="blocked",
                reason_code="adapter_not_configured",
                observations=(),
                attempted_at=attempted_at,
                completed_at=float(self._clock()),
            )
        if attempted_at < self._backoff_until.get(request.provider_id, 0.0):
            return ProviderResult(
                provider_id=request.provider_id,
                ready=True,
                status="backoff",
                reason_code="provider_backoff",
                observations=(),
                attempted_at=attempted_at,
                completed_at=float(self._clock()),
            )

        lock = self._locks.setdefault(request.provider_id, asyncio.Lock())
        async with self._semaphore, lock:
            for attempt in range(self._retries + 1):
                try:
                    adapter_result = await asyncio.wait_for(
                        adapter.fetch(
                            request,
                            self._fetcher,
                            now=float(self._clock()),
                        ),
                        timeout=self._timeout_seconds,
                    )
                    if isinstance(adapter_result, ThinAdapterResult):
                        result = adapter_result.observations
                        radar_fields = adapter_result.radar_fields
                    else:
                        result = tuple(adapter_result)
                        radar_fields = ()
                    if not all(isinstance(item, NormalizedObservation) for item in result):
                        raise TypeError("adapter_returned_invalid_observation")
                    self._failure_counts.pop(request.provider_id, None)
                    self._backoff_until.pop(request.provider_id, None)
                    return ProviderResult(
                        provider_id=request.provider_id,
                        ready=True,
                        status="operational" if result else "empty",
                        reason_code=None if result else "no_observations",
                        observations=result,
                        attempted_at=attempted_at,
                        completed_at=float(self._clock()),
                        radar_fields=radar_fields,
                    )
                except ProviderBlocked as error:
                    return ProviderResult(
                        provider_id=request.provider_id,
                        ready=error.ready,
                        status="blocked",
                        reason_code=error.reason_code,
                        observations=(),
                        attempted_at=attempted_at,
                        completed_at=float(self._clock()),
                    )
                except (asyncio.TimeoutError, Exception) as error:
                    if attempt < self._retries:
                        if self._retry_delay_seconds > 0.0:
                            await asyncio.sleep(self._retry_delay_seconds * (attempt + 1))
                        continue
                    failures = self._failure_counts.get(request.provider_id, 0) + 1
                    self._failure_counts[request.provider_id] = failures
                    delay = min(300.0, 5.0 * (2 ** (failures - 1)))
                    self._backoff_until[request.provider_id] = float(self._clock()) + delay
                    return ProviderResult(
                        provider_id=request.provider_id,
                        ready=True,
                        status="error",
                        reason_code=type(error).__name__,
                        observations=(),
                        attempted_at=attempted_at,
                        completed_at=float(self._clock()),
                    )
        raise RuntimeError("provider_execution_unreachable")

    async def fetch(
        self,
        requests: tuple[ThinProviderRequest, ...],
    ) -> Mapping[str, ProviderResult]:
        if self._request_preparer is not None:
            requests = await asyncio.to_thread(
                self._request_preparer.prepare,
                requests,
                now=float(self._clock()),
            )
        results = await asyncio.gather(*(self._one(request) for request in requests))
        return {item.provider_id: item for item in results}


__all__ = ["ProviderResult", "ThinProviderRuntime"]
