"""Failure-isolated concurrent execution for thin provider adapters."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
from uuid import uuid4
import time
from typing import Any, Callable, Mapping

from .thin_observation import NormalizedObservation
from .thin_provider_adapters import (
    ProviderBlocked,
    ProviderStale,
    ThinAdapterResult,
    ThinRadarField,
    ThinVisualRadarField,
)
from .thin_provider_catalog import ThinProviderRequest
from .thin_raw_archive import RawCaptureContext, capture_scope, current_capture_references


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
    visual_radar_fields: tuple[ThinVisualRadarField, ...] = ()
    raw_capture_ids: tuple[str, ...] = ()
    archive_reason_codes: tuple[str, ...] = ()


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
        archive: Any | None = None,
        code_revision: str = 'unregistered',
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
        self.archive = archive
        self.code_revision = code_revision
        self._locks = {provider_id: asyncio.Lock() for provider_id in self._adapters}
        self._failure_counts: dict[str, int] = {}
        self._backoff_until: dict[str, float] = {}

    @property
    def ready_provider_ids(self) -> frozenset[str]:
        """Providers with a complete adapter, independent of current scope."""
        return frozenset(self._adapters)

    async def _one(self, request: ThinProviderRequest) -> ProviderResult:
        if self.archive is None:
            return await self._one_impl(request)
        context = RawCaptureContext(str(uuid4()), request.provider_id, request.product_id,
            request.requested_at, 0, tuple(r.region_revision_id for r in request.regions),
            tuple((t.target_id,t.latitude,t.longitude,t.position_observed_at) for t in request.targets),
            self.code_revision)
        with capture_scope(context,self.archive):
            result = await self._one_impl(request,context)
            references = current_capture_references()
        receipts = tuple(r.capture_id for r in self.archive.pending()
                         if r.context.request_id == context.request_id)
        if result.status == 'error' and not receipts and not references:
            self.archive.mark_gap('provider_input_not_received',context)
        return replace(result,raw_capture_ids=tuple(sorted(set(receipts) | set(references))),
                       archive_reason_codes=self.archive.status()['reason_codes'])

    async def _fetch_scoped(self, adapter, request, context, attempt):
        if context is None:
            return await adapter.fetch(request,self._fetcher,now=float(self._clock()))
        with capture_scope(replace(context,attempt=attempt),self.archive):
            return await adapter.fetch(request,self._fetcher,now=float(self._clock()))

    async def _one_impl(self, request: ThinProviderRequest, context=None) -> ProviderResult:
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
                        self._fetch_scoped(adapter,request,context,attempt),
                        timeout=self._timeout_seconds,
                    )
                    if isinstance(adapter_result, ThinAdapterResult):
                        result = adapter_result.observations
                        radar_fields = adapter_result.radar_fields
                        visual_radar_fields = adapter_result.visual_radar_fields
                    else:
                        result = tuple(adapter_result)
                        radar_fields = ()
                        visual_radar_fields = ()
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
                        visual_radar_fields=visual_radar_fields,
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
                except ProviderStale as error:
                    self._failure_counts.pop(request.provider_id, None)
                    self._backoff_until.pop(request.provider_id, None)
                    return ProviderResult(
                        provider_id=request.provider_id,
                        ready=True,
                        status="stale",
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
