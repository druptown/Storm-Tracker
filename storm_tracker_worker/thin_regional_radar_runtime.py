"""Non-blocking two-thread coordinator for regional radar fusion."""

from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
import hashlib
import json
from types import MappingProxyType
from typing import Any, Callable, Mapping

import numpy as np

from .thin_provider_runtime import ProviderResult
from .thin_region_planner import ThinAcquisitionRegion, ThinRegionPlan
from .thin_regional_radar_fusion import (
    FusedRegionalRadarField,
    RegionalFusionSummary,
    fuse_regional_radar_fields,
)


@dataclass(frozen=True, slots=True)
class _FusionJob:
    region: ThinAcquisitionRegion
    fingerprint: str
    evaluated_at: float
    future: Future[Any]


@dataclass(frozen=True, slots=True)
class _CompletedFusion:
    fingerprint: str
    result: Any


def _input_fingerprint(
    region: ThinAcquisitionRegion,
    provider_results: Mapping[str, ProviderResult],
) -> str:
    fields = []
    for mapping_id, result in sorted(provider_results.items()):
        if not isinstance(result, ProviderResult):
            continue
        for field in result.radar_fields:
            fields.append((
                str(mapping_id),
                field.provider_id,
                field.product_id,
                float(field.observed_at),
                tuple(int(value) for value in field.rain_rate_mm_h.shape),
            ))
    payload = {
        "region_id": region.region_id,
        "center": [region.center_latitude, region.center_longitude],
        "radius_km": region.observation_radius_km,
        "fields": sorted(fields),
    }
    encoded = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _error_result(
    region: ThinAcquisitionRegion,
    *,
    evaluated_at: float,
) -> FusedRegionalRadarField:
    summary = RegionalFusionSummary(
        region_id=region.region_id,
        grid_id=None,
        status="degraded",
        mode="fallback",
        evaluated_at=evaluated_at,
        field_observed_at=None,
        contributors=(),
        excluded=MappingProxyType({}),
        standby=MappingProxyType({}),
        reason_codes=("fusion_error",),
        coverage_fraction=0.0,
        mean_confidence=0.0,
        disagreement_fraction=0.0,
    )
    return FusedRegionalRadarField(
        region_id=region.region_id,
        field=None,
        confidence=np.zeros((0, 0), dtype=np.float64),
        contributor_mask=np.zeros((0, 0), dtype=np.uint64),
        summary=summary,
    )


class ThinRegionalRadarFusionCoordinator:
    """Run bounded regional fusion without ever blocking ``poll``."""

    def __init__(
        self,
        *,
        max_workers: int = 2,
        fuse: Callable[..., Any] = fuse_regional_radar_fields,
    ) -> None:
        if isinstance(max_workers, bool) or not 1 <= int(max_workers) <= 2:
            raise ValueError("regional_fusion_max_workers_must_be_between_1_and_2")
        self.max_workers = int(max_workers)
        self._fuse = fuse
        self._executor = ThreadPoolExecutor(
            max_workers=self.max_workers,
            thread_name_prefix="storm-regional-fusion",
        )
        self._jobs: dict[str, _FusionJob] = {}
        self._completed: dict[str, _CompletedFusion] = {}
        self._closed = False

    def submit(
        self,
        region_plan: ThinRegionPlan,
        provider_results: Mapping[str, ProviderResult],
        *,
        evaluated_at: float,
    ) -> None:
        if self._closed:
            return
        providers = dict(provider_results)
        for region in region_plan.regions:
            fingerprint = _input_fingerprint(region, providers)
            job = self._jobs.get(region.region_id)
            if job is not None and job.fingerprint == fingerprint:
                continue
            completed = self._completed.get(region.region_id)
            if completed is not None and completed.fingerprint == fingerprint:
                continue
            if job is not None:
                job.future.cancel()
            self._completed.pop(region.region_id, None)
            future = self._executor.submit(
                self._fuse,
                region,
                providers,
                evaluated_at=float(evaluated_at),
            )
            self._jobs[region.region_id] = _FusionJob(
                region=region,
                fingerprint=fingerprint,
                evaluated_at=float(evaluated_at),
                future=future,
            )

        active_ids = {region.region_id for region in region_plan.regions}
        for region_id in set(self._jobs) - active_ids:
            self._jobs.pop(region_id).future.cancel()
        for region_id in set(self._completed) - active_ids:
            self._completed.pop(region_id, None)

    def poll(self) -> Mapping[str, FusedRegionalRadarField]:
        for region_id, job in tuple(self._jobs.items()):
            if not job.future.done():
                continue
            try:
                result = job.future.result()
            except Exception:
                result = _error_result(
                    job.region,
                    evaluated_at=job.evaluated_at,
                )
            self._completed[region_id] = _CompletedFusion(
                fingerprint=job.fingerprint,
                result=result,
            )
            self._jobs.pop(region_id, None)
        return {
            region_id: completed.result
            for region_id, completed in sorted(self._completed.items())
        }

    def pending_region_ids(self) -> frozenset[str]:
        return frozenset(self._jobs)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for job in self._jobs.values():
            job.future.cancel()
        self._jobs.clear()
        self._completed.clear()
        self._executor.shutdown(wait=True, cancel_futures=True)


__all__ = ["ThinRegionalRadarFusionCoordinator"]
