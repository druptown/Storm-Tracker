"""Non-blocking two-thread coordinator for regional radar fusion."""

from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, replace
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
    projected_native_field,
)
from .thin_source_radar_forecast import SourceRadarForecastEngine, MAX_SOURCE_COUNT


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
                field.identity.grid_id,
                field.identity.content_fingerprint,
                field.identity.processing_version,
                field.identity.source_composition,
            ))
    payload = {
        "region_id": region.region_id,
        "region_revision_id": region.region_revision_id,
        "center": [region.center_latitude, region.center_longitude],
        "radius_km": region.observation_radius_km,
        "radar_analysis_radius_km": region.radar_analysis_radius_km,
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
        source_engine_factory: Callable[..., Any] = SourceRadarForecastEngine,
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
        self._source_engine_factory = source_engine_factory
        self._source_engines: dict[tuple[str, str], Any] = {}
        self._active_revisions: dict[str, str] = {}
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
        self._active_revisions = {r.region_id: r.region_revision_id
                                  for r in region_plan.regions}
        active_keys = set(self._active_revisions.items())
        # Running jobs retain their local reference. New revisions cannot use it.
        for key in set(self._source_engines) - active_keys:
            self._source_engines.pop(key, None)
        for region in region_plan.regions:
            fingerprint = _input_fingerprint(region, providers)
            job = self._jobs.get(region.region_id)
            # Never replace a running fusion job. With rapid provider refreshes
            # its fingerprint can change before the two-thread calculation
            # completes; continually cancelling/replacing it starves the map
            # of every completed regional raster. The next cycle schedules the
            # newest fingerprint after ``poll`` has collected this result.
            if job is not None:
                continue
            completed = self._completed.get(region.region_id)
            if completed is not None and getattr(completed.result, "region_revision_id", "") != region.region_revision_id:
                self._completed.pop(region.region_id, None)
                completed = None
            if completed is not None and completed.fingerprint == fingerprint:
                continue
            key = (region.region_id, region.region_revision_id)
            engine = self._source_engines.get(key)
            if engine is None:
                engine = self._source_engine_factory()
                self._source_engines[key] = engine
            future = self._executor.submit(
                self._fuse_with_archive_provenance,
                region,
                providers,
                evaluated_at=float(evaluated_at),
                source_engine=engine,
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

    def _fuse_with_archive_provenance(self, region, providers, *, evaluated_at, source_engine):
        result = self._fuse(region, providers, evaluated_at=evaluated_at)
        if not isinstance(result, FusedRegionalRadarField):
            return result
        inputs = tuple((field, provider.raw_capture_ids)
                       for provider in providers.values()
                       if isinstance(provider, ProviderResult)
                       for field in provider.radar_fields)
        # These are the actual submitted inputs, never a newer provider cache.
        native = sorted(result.native_layers, key=lambda item: (
            item[1].native_resolution_m, -item[0].observed_at, item[0].provider_id,
        ))[:MAX_SOURCE_COUNT]
        motions = tuple(source_engine.advance(field, layer, received_at=evaluated_at)
                        for field,layer in native)
        tracking_fields = tuple(projected_native_field(field, layer)
                                for field,layer in native if field.observed_at <= evaluated_at)
        return replace(result, archive_inputs=inputs, source_motions=motions,
                       native_tracking_fields=tracking_fields,
                       region_revision_id=region.region_revision_id)

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
            if self._active_revisions.get(region_id) == job.region.region_revision_id:
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
        self._source_engines.clear()
        self._executor.shutdown(wait=True, cancel_futures=True)


__all__ = ["ThinRegionalRadarFusionCoordinator"]
