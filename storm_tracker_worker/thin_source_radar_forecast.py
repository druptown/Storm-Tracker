"""Causal source-clock motion and quantitative target field predictions.

No composite is a motion input. Unknown support never becomes zero rainfall.
This deterministic extrapolation is not an ensemble or calibrated probability.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
import hashlib
import json
import math
from typing import Callable, Any

import numpy as np
from pyproj import Transformer

from .thin_provider_adapters import ThinRadarField
from .thin_regional_radar_fusion import RegionalRadarLayer
from .thin_fusion import MAX_FUSION_AGE_SECONDS, PROVIDER_FUSION_AGE_SECONDS
from .tracking_core.engine.analysis_grid import AnalysisGrid
from .tracking_core.engine.hierarchical_detection import NormalizedRasterFrame
from .tracking_core.engine.pyramidal_sparse_flow import (
    SparsePyramidalConfig, estimate_latest_sparse_flow,
)


POLICY_VERSION = "source-clock-field-v1"
MAX_SOURCE_COUNT = 3
MAX_PAIR_GAP_SECONDS = 1200.
HORIZON_SECONDS = 7200.
STEP_SECONDS = 60.


@dataclass(frozen=True, slots=True)
class SourceRadarMotion:
    field: ThinRadarField
    layer: RegionalRadarLayer
    received_at: float
    previous_fingerprint: str | None = None
    delta_seconds: float | None = None
    vectors: tuple[tuple[float, float, float, float], ...] = ()
    reason_code: str | None = None

    def provenance(self) -> dict[str, Any]:
        identity = self.field.identity
        assert identity is not None
        return {
            "provider_id": self.field.provider_id,
            "product_id": self.field.product_id,
            "product_at": self.field.observed_at,
            "available_at": self.received_at,
            "previous_fingerprint": self.previous_fingerprint,
            "field_fingerprint": identity.content_fingerprint,
            "native_grid_id": identity.grid_id,
            "projected_grid_id": self.layer.grid.grid_id,
            "processing_version": identity.processing_version,
            "source_composition": list(identity.source_composition),
            "quantity": identity.quantity, "unit": identity.unit,
            "native_resolution_m": self.layer.native_resolution_m,
            "pair_dt_seconds": self.delta_seconds,
            "motion_vector_count": len(self.vectors),
            "reason_code": self.reason_code,
            "policy_version": POLICY_VERSION,
        }


def _compatible(item: SourceRadarMotion) -> tuple[Any, ...]:
    f = item.field; identity = f.identity
    assert identity is not None
    return (f.provider_id, f.product_id, identity.quantity, identity.unit,
            identity.processing_version, identity.source_composition,
            identity.grid_id, item.layer.grid)


def _flow_config() -> SparsePyramidalConfig:
    return SparsePyramidalConfig(
        maximum_input_pixels=131072, max_features=24,
        minimum_feature_distance_pixels=6., feature_window_radius_pixels=2,
        minimum_feature_valid_fraction=.8, minimum_feature_eigenvalue=.0001,
        pyramid_levels=3, minimum_pyramid_valid_fraction=.8,
        tracking_window_radius_pixels=3, minimum_tracking_samples=20,
        maximum_iterations=20, convergence_threshold_pixels=.03,
        minimum_tracking_eigenvalue=.0001, maximum_condition_number=1000000.,
        regularization=0., residual_variance_floor=1e-6,
        velocity_process_noise_covariance_m2_s2=((1., 0.), (0., 1.)),
    )


def estimate_region_motion(old: SourceRadarMotion, new: SourceRadarMotion):
    """Bounded distributed LK, only converged vectors from this source pair."""
    if _compatible(old) != _compatible(new):
        raise ValueError("source_pair_incompatible")
    a, b = old.layer, new.layer
    if a.rain_rate_mm_h.shape != b.rain_rate_mm_h.shape:
        raise ValueError("source_pair_shape_changed")
    grid = b.grid
    common = a.valid_mask & b.valid_mask
    if common.size > 250000:
        raise ValueError("source_motion_grid_capacity")
    kept = {}
    config = _flow_config()
    height, width = common.shape
    for row in range(0, height, 96):
        for column in range(0, width, 96):
            h, w = min(128, height-row), min(128, width-column)
            if min(h, w) < 32:
                continue
            sl = np.s_[row:row+h, column:column+w]
            if not np.any(common[sl]):
                continue
            tile_grid = AnalysisGrid(
                f"{grid.grid_id}:source-tile:{row}:{column}", grid.crs,
                grid.origin_x_m+column*grid.x_resolution_m,
                grid.origin_y_m-row*grid.y_resolution_m,
                grid.x_resolution_m, grid.y_resolution_m, w, h,
            )
            def frame(item):
                image = np.where(common[sl], np.log1p(
                    np.maximum(item.layer.rain_rate_mm_h[sl], 0.)/.1), np.nan)
                rows = tuple(tuple(None if not np.isfinite(v) else float(v)
                                   for v in values) for values in image)
                return NormalizedRasterFrame(
                    item.field.identity.content_fingerprint+f":{row}:{column}",
                    item.field.observed_at, item.field.provider_id,
                    "log1p_precipitation_rate", "dimensionless", tile_grid, rows,
                )
            result = estimate_latest_sparse_flow((frame(old), frame(new)), config)
            for vector in result.vectors:
                if vector.status != "available_converged":
                    continue
                r, c = row+vector.start_row, column+vector.start_column
                x = grid.origin_x_m+(c+.5+vector.displacement_column_pixels)*grid.x_resolution_m
                y = grid.origin_y_m-(r+.5+vector.displacement_row_pixels)*grid.y_resolution_m
                entry = (x, y, float(vector.vx_m_s), float(vector.vy_m_s))
                score = float(vector.forward_residual_rms)
                if (r, c) not in kept or score < kept[(r,c)][0]:
                    kept[(r,c)] = (score, entry)
    return tuple(value[1] for _, value in sorted(kept.items()))


class SourceRadarForecastEngine:
    """One region revision's bounded per-source history, called in one worker."""
    def __init__(self, *, estimate: Callable = estimate_region_motion):
        self._estimate = estimate
        self._latest: dict[str, SourceRadarMotion] = {}

    def advance(self, field, layer, *, received_at):
        incoming = SourceRadarMotion(field, layer, float(received_at),
                                     reason_code="source_pair_missing")
        provider = field.provider_id
        if provider.startswith("fusion:") or not field.is_quantitative_precipitation:
            return replace(incoming, reason_code="not_native_quantitative_source")
        if field.observed_at > received_at:
            return replace(incoming, reason_code="future_product")
        old = self._latest.get(provider)
        if old is not None:
            if field.observed_at < old.field.observed_at:
                return old
            if field.observed_at == old.field.observed_at:
                if field.identity == old.field.identity and _compatible(old) == _compatible(incoming):
                    return old
                revised = replace(incoming, reason_code="same_time_revision_requires_new_pair")
                self._latest[provider] = revised
                return revised
            dt = field.observed_at-old.field.observed_at
            if _compatible(old) != _compatible(incoming):
                incoming = replace(incoming, reason_code="source_pair_incompatible")
            elif not 0. < dt <= MAX_PAIR_GAP_SECONDS:
                incoming = replace(incoming, reason_code="source_pair_gap")
            else:
                vectors = tuple(self._estimate(old, incoming))
                incoming = replace(
                    incoming, vectors=vectors, delta_seconds=dt,
                    previous_fingerprint=old.field.identity.content_fingerprint,
                    reason_code=None if vectors else "motion_support_missing",
                )
        self._latest[provider] = incoming
        # Sources are selected at regional admission; this is a second bound.
        while len(self._latest) > MAX_SOURCE_COUNT:
            oldest = min(self._latest, key=lambda p: self._latest[p].field.observed_at)
            self._latest.pop(oldest)
        return incoming


def _local_velocity(vectors, x, y):
    if len(vectors) < 3:
        return None
    array = np.asarray(vectors, dtype=float)
    distances = np.sum((array[:, :2]-[x, y])**2, axis=1)
    indices = np.argsort(distances, kind="stable")[:8]
    indices = indices[distances[indices] <= 60000.**2]
    if len(indices) < 3:
        return None
    speeds = array[indices, 2:]
    if not np.all(np.isfinite(speeds)):
        return None
    weights = 1./(distances[indices]+1500.**2)
    weights /= np.sum(weights)
    vector = np.sum(speeds*weights[:, None], axis=0)
    rms = float(np.sqrt(np.sum(weights*np.sum((speeds-vector)**2, axis=1))))
    return None if rms > 8. else vector


def trace_target(motion: SourceRadarMotion, latitude, longitude, issued_at):
    """RK2 inverse trajectory including product age; no target-side reraster."""
    issued = float(issued_at); product = motion.field.observed_at
    if not all(math.isfinite(value) for value in (issued, product, motion.received_at)):
        raise ValueError("source_time_invalid")
    reason = ("future_product" if issued < product else
              "future_receipt" if issued < motion.received_at else
              "source_stale" if issued-product > PROVIDER_FUSION_AGE_SECONDS.get(
                  motion.field.provider_id, MAX_FUSION_AGE_SECONDS) else motion.reason_code)
    grid = motion.layer.grid
    x, y = Transformer.from_crs("EPSG:4326", grid.crs, always_xy=True).transform(longitude, latitude)
    values, valid = motion.layer.rain_rate_mm_h, motion.layer.valid_mask
    def advance(duration):
        nonlocal x, y, reason
        left = duration
        while left > 1e-9 and reason is None:
            first = _local_velocity(motion.vectors, x, y)
            if first is None:
                reason = "motion_support_missing"; break
            speed = math.hypot(*first)
            dt = min(left, 30., .25*min(grid.x_resolution_m,grid.y_resolution_m)/max(speed,1e-9))
            mid = _local_velocity(motion.vectors, x-first[0]*dt/2., y-first[1]*dt/2.)
            if mid is None:
                reason = "motion_support_missing"; break
            x -= mid[0]*dt; y -= mid[1]*dt; left -= dt
    advance(max(0., issued-product))
    samples = []
    for index in range(121):
        if index:
            advance(STEP_SECONDS)
        sample_reason = reason
        row = math.floor((grid.origin_y_m-y)/grid.y_resolution_m)
        col = math.floor((x-grid.origin_x_m)/grid.x_resolution_m)
        rate = None
        if sample_reason is None:
            if not (0 <= row < grid.height and 0 <= col < grid.width):
                sample_reason = "source_outside_radar"
            elif not valid[row,col] or not math.isfinite(float(values[row,col])):
                sample_reason = "source_nodata"
            else:
                rate = float(values[row,col])
        samples.append({"at": issued+index*STEP_SECONDS, "rate_mm_h": rate,
                        "reason": sample_reason, "source_x_m": float(x), "source_y_m": float(y)})
    return samples


def rain_intervals(samples, threshold=.1):
    result = []; active = None; previous = None
    def finish(status, bracket=None):
        nonlocal active
        rates = active.pop("rates")
        active.update(exit_at=None if bracket is None else sum(bracket)/2.,
                      exit_bracket=bracket, end_status=status,
                      mean_mm_h=sum(rates)/len(rates), peak_mm_h=max(rates))
        result.append(active); active = None
    for index, sample in enumerate(samples):
        at, rate = sample["at"], sample["rate_mm_h"]
        if rate is None:
            if active is not None:
                finish("support_censored")
            previous = None; continue
        if rate >= threshold:
            if active is None:
                bracket = None if previous is None else [previous, at]
                active = {"entry_at": None if bracket is None else sum(bracket)/2.,
                          "entry_bracket": bracket, "left_censored": index == 0,
                          "first_supported_at": at, "last_supported_at": at, "rates": []}
            active["last_supported_at"] = at; active["rates"].append(rate)
        elif active is not None:
            finish("bracketed", [active["last_supported_at"], at])
        previous = at
    if active is not None:
        finish("horizon_censored")
    return result


def select_primary(candidates, previous_provider=None):
    """Operational source priority, never a calibrated reliability weight."""
    candidates = tuple(candidates)
    if candidates:
        times = [s["at"] for s in candidates[0]["samples"]]
        if any([s["at"] for s in c["samples"]] != times for c in candidates):
            raise ValueError("unaligned_prediction_times")
    supported = [c for c in candidates if any(s["rate_mm_h"] is not None for s in c["samples"])]
    primary = next((c for c in supported if c["provider_id"] == previous_provider), None)
    selection_reason = "retained_supported_primary"
    if primary is None and supported:
        primary = min(supported, key=lambda c: (c["native_resolution_m"], -c["product_at"], c["provider_id"]))
        selection_reason = "native_resolution_then_product_recency_proxy"
    conflict = False
    if primary is not None:
        # Any unequal supported quantitative prediction is retained as a
        # disagreement. No empirically unjustified 'small enough' tolerance.
        for c in supported:
            for a, b in zip(primary["samples"], c["samples"]):
                if a["rate_mm_h"] is not None and b["rate_mm_h"] is not None:
                    if not math.isclose(a["rate_mm_h"], b["rate_mm_h"], rel_tol=0., abs_tol=1e-12):
                        conflict = True; break
    return {"primary_provider": None if primary is None else primary["provider_id"],
            "source_conflict": conflict,
            "selection_reason": "no_supported_source" if primary is None else selection_reason,
            "samples": [] if primary is None else primary["samples"],
            "candidates": list(candidates), "policy_version": POLICY_VERSION}


def motion_digest(motion):
    return hashlib.sha256(json.dumps(motion.vectors, separators=(",", ":"),
                                    allow_nan=False).encode()).hexdigest()
