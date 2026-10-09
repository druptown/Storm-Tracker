"""Bounded per-region cell tracking for the worker-only radar path.

This module is deliberately a shadow boundary.  It turns only already
verified, metric radar fields into small target-region rasters and advances
the immutable :mod:`tracking_core` cell engine.  It never publishes an ETA,
probability, wet/dry conclusion or warning.  Those claims require the later
same-track ground and forecast gates.

The ROI is source-pixel aligned and capped before any tuple materialisation or
detector work.  A Life360 target far from the other targets therefore becomes
its own bounded acquisition region instead of widening one computation across
the complete provider grid.
"""
from __future__ import annotations

from dataclasses import dataclass
from contextlib import closing
import hashlib
import json
import math
from pathlib import Path
import sqlite3
import threading
import time
from typing import Any, Mapping
import zlib

import numpy as np
from pyproj import CRS, Transformer
from pyproj.exceptions import CRSError, ProjError

from .analysis_grid_transform import native_candidate_content_sha256
from .canonical_ground_evidence_store import GroundObservationReceipt
from .desired_state import DesiredState
from .multi_source_radar_facts import (
    RADAR_PROVIDER_PROFILES,
    VerifiedRadarFieldSource,
)
from .tracking_core.engine.analysis_grid import AnalysisGrid
from .tracking_core.engine.cell_track_engine import (
    CellTrackEnginePolicy,
    CellTrackEngineState,
    advance_cell_track_engine,
    create_cell_track_engine_state,
)
from .tracking_core.engine.cell_track_lifecycle import CellTrackLifecyclePolicy
from .tracking_core.engine.cell_track_state_codec import (
    CellTrackStateCodecError,
    decode_cell_track_state_value,
    encode_cell_track_state_value,
)
from .tracking_core.engine.hierarchical_detection import (
    HierarchicalDetectorConfig,
    NormalizedRasterFrame,
)
from .tracking_core.engine.lucas_kanade_flow import (
    DetectionFlowAggregationConfig,
    LucasKanadeConfig,
)
from .tracking_core.engine.recursive_track_estimator import (
    RecursiveTrackEstimatorPolicy,
)
from .tracking_core.engine.weather_system_grouping import (
    WeatherSystemGroupingPolicy,
)
from .worker_acquisition_plan import build_worker_acquisition_regions


CONTRACT = "stv3-worker-only-cell-tracking-v1"
ADMISSION_STATUS = "shadow_candidate_unadmitted"
TRACKABLE_WORKLOAD_IDS = frozenset({
    "radar.dwd.radolan_rv",
    "radar.metoffice.uk_rate",
})
MAX_ROI_PIXELS = 100_000
MAX_ROI_HALF_EXTENT_M = 150_000.0
MAX_TRACKING_STATES = 128
ROI_ANCHOR_STRIDE_PIXELS = 32
TRACKING_STORE_SCHEMA_VERSION = 1
MAX_TRACKING_STORE_BYTES = 128 * 1024 * 1024
MAX_TRACKING_STATE_BYTES = 32 * 1024 * 1024
SHADOW_ARRIVAL_HORIZON_SECONDS = 90.0 * 60.0
MAX_SHADOW_ARRIVAL_PIXEL_TARGET_PAIRS = 1_000_000
SHADOW_ARRIVAL_METHOD = "exact_constant_velocity_pixel_union_intersection_v1"
MAX_GROUND_TRACK_RECEIPTS = 4_096
MAX_GROUND_TRACK_PAIRS = 1_000_000
GROUND_TRACK_MATCH_METHOD = "representative_time_back_advected_pixel_match_v1"

_ZERO4 = (
    (0.0, 0.0, 0.0, 0.0),
    (0.0, 0.0, 0.0, 0.0),
    (0.0, 0.0, 0.0, 0.0),
    (0.0, 0.0, 0.0, 0.0),
)
_INITIAL4 = (
    (1_000_000.0, 0.0, 0.0, 0.0),
    (0.0, 1_000_000.0, 0.0, 0.0),
    (0.0, 0.0, 400.0, 0.0),
    (0.0, 0.0, 0.0, 400.0),
)


class WorkerOnlyCellTrackingError(RuntimeError):
    """The bounded shadow tracker could not preserve its contract."""


class WorkerOnlyCellTrackingStoreError(WorkerOnlyCellTrackingError):
    """The durable shadow tracking head failed integrity or capacity checks."""


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value)).hexdigest()


def _safe_id(value: object, label: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or "\0" in value
        or len(value) > 512
    ):
        raise ValueError(f"{label} is ongeldig")
    return value


def _bounded_count(value: object, label: str, maximum: int) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not 0 <= value <= maximum
    ):
        raise ValueError(f"{label} ligt buiten de grens")
    return value


@dataclass(frozen=True, slots=True)
class WorkerOnlyProjectedTarget:
    target_id: str
    x_m: float
    y_m: float
    location_accuracy_m: float | None

    def __post_init__(self) -> None:
        _safe_id(self.target_id, "target_id")
        if not math.isfinite(self.x_m) or not math.isfinite(self.y_m):
            raise ValueError("projected target moet eindige coordinaten hebben")
        if self.location_accuracy_m is not None and (
            not math.isfinite(self.location_accuracy_m)
            or self.location_accuracy_m < 0.0
        ):
            raise ValueError("targetnauwkeurigheid is ongeldig")


@dataclass(frozen=True, slots=True)
class WorkerOnlyTrackingFrameCandidate:
    workload_id: str
    region_id: str
    region_lineage_id: str
    target_ids: tuple[str, ...]
    capture_id: str
    source_candidate_id: str
    source_candidate_content_sha256: str
    requested_radius_km: float
    covered_radius_km: float
    coverage_complete: bool
    frame: NormalizedRasterFrame
    projected_targets: tuple[WorkerOnlyProjectedTarget, ...]
    admission_status: str = ADMISSION_STATUS

    def __post_init__(self) -> None:
        for label in (
            "workload_id",
            "region_id",
            "region_lineage_id",
            "capture_id",
            "source_candidate_id",
        ):
            _safe_id(getattr(self, label), label)
        if (
            not isinstance(self.source_candidate_content_sha256, str)
            or len(self.source_candidate_content_sha256) != 64
            or any(
                character not in "0123456789abcdef"
                for character in self.source_candidate_content_sha256
            )
        ):
            raise ValueError("source candidate checksum is ongeldig")
        if (
            not isinstance(self.target_ids, tuple)
            or self.target_ids != tuple(sorted(set(self.target_ids)))
            or not self.target_ids
        ):
            raise ValueError("target_ids moeten canoniek niet-leeg zijn")
        for target_id in self.target_ids:
            _safe_id(target_id, "target_id")
        if (
            not isinstance(self.projected_targets, tuple)
            or any(
                not isinstance(item, WorkerOnlyProjectedTarget)
                for item in self.projected_targets
            )
            or tuple(item.target_id for item in self.projected_targets)
            != self.target_ids
        ):
            raise ValueError(
                "projected_targets moeten iedere target canoniek afdekken"
            )
        for value, label in (
            (self.requested_radius_km, "requested_radius_km"),
            (self.covered_radius_km, "covered_radius_km"),
        ):
            if not math.isfinite(value) or value < 0.0:
                raise ValueError(f"{label} is ongeldig")
        if type(self.coverage_complete) is not bool:
            raise TypeError("coverage_complete moet boolean zijn")
        if not isinstance(self.frame, NormalizedRasterFrame):
            raise TypeError("tracking candidate vereist NormalizedRasterFrame")
        if self.frame.grid.width * self.frame.grid.height > MAX_ROI_PIXELS:
            raise ValueError("tracking frame overschrijdt ROI-pixelbudget")
        if self.admission_status != ADMISSION_STATUS:
            raise ValueError("tracking candidate mag geen admission verlenen")


@dataclass(frozen=True, slots=True)
class WorkerOnlyTrackingDisposition:
    workload_id: str
    region_id: str
    state: str
    reason_code: str
    candidate: WorkerOnlyTrackingFrameCandidate | None

    def __post_init__(self) -> None:
        _safe_id(self.workload_id, "workload_id")
        _safe_id(self.region_id, "region_id")
        _safe_id(self.reason_code, "reason_code")
        if self.state not in {"candidate", "withheld"}:
            raise ValueError("tracking disposition state is ongeldig")
        if (self.state == "candidate") != (self.candidate is not None):
            raise ValueError("tracking disposition candidatebinding verschilt")
        if self.candidate is not None and (
            self.candidate.workload_id != self.workload_id
            or self.candidate.region_id != self.region_id
        ):
            raise ValueError("tracking disposition verwijst naar andere scope")


@dataclass(frozen=True, slots=True)
class WorkerOnlyTrackingBatch:
    workload_id: str
    source_candidate_id: str
    dispositions: tuple[WorkerOnlyTrackingDisposition, ...]
    contract: str = CONTRACT

    def __post_init__(self) -> None:
        _safe_id(self.workload_id, "workload_id")
        _safe_id(self.source_candidate_id, "source_candidate_id")
        if not isinstance(self.dispositions, tuple) or any(
            not isinstance(item, WorkerOnlyTrackingDisposition)
            for item in self.dispositions
        ):
            raise TypeError("tracking batch dispositions zijn ongeldig")
        keys = tuple(
            (item.workload_id, item.region_id) for item in self.dispositions
        )
        if keys != tuple(sorted(keys)) or len(keys) != len(set(keys)):
            raise ValueError("tracking batch dispositions zijn niet canoniek")
        if any(item.workload_id != self.workload_id for item in self.dispositions):
            raise ValueError("tracking batch bevat een andere workload")
        if self.contract != CONTRACT:
            raise ValueError("tracking batch contract is onbekend")


@dataclass(frozen=True, slots=True)
class WorkerOnlyTrackingUpdate:
    workload_id: str
    candidate_regions: int
    withheld_regions: int
    advanced_regions: int
    duplicate_regions: int
    reset_regions: int
    indeterminate_regions: int
    active_tracks: int
    tracked_tracks: int
    preliminary_tracks: int
    coasting_tracks: int
    motion_ready_tracks: int
    shadow_arrival_candidates: int
    shadow_future_arrival_candidates: int
    shadow_arrival_evaluated_pairs: int
    admission_status: str = ADMISSION_STATUS

    def __post_init__(self) -> None:
        _safe_id(self.workload_id, "workload_id")
        for name in (
            "candidate_regions",
            "withheld_regions",
            "advanced_regions",
            "duplicate_regions",
            "reset_regions",
            "indeterminate_regions",
            "active_tracks",
            "tracked_tracks",
            "preliminary_tracks",
            "coasting_tracks",
            "motion_ready_tracks",
            "shadow_arrival_candidates",
            "shadow_future_arrival_candidates",
            "shadow_arrival_evaluated_pairs",
        ):
            _bounded_count(getattr(self, name), name, 1_000_000_000)
        if self.admission_status != ADMISSION_STATUS:
            raise ValueError("tracking update mag geen admission verlenen")


@dataclass(frozen=True, slots=True)
class WorkerOnlyShadowArrivalCandidate:
    candidate_id: str
    workload_id: str
    region_lineage_id: str
    target_id: str
    track_id: str
    detection_id: str
    source_candidate_id: str
    source_candidate_content_sha256: str
    analysis_grid_id: str
    reference_valid_at: float
    horizon_seconds: float
    entry_seconds: float
    exit_seconds: float
    vx_m_s: float
    vy_m_s: float
    track_sample_count: int
    footprint_pixel_count: int
    area_km2: float
    mean_rain_rate_mm_h: float
    maximum_rain_rate_mm_h: float
    target_location_accuracy_m: float | None
    low_confidence_fragment: bool
    passage_state: str
    method: str = SHADOW_ARRIVAL_METHOD
    admission_status: str = ADMISSION_STATUS

    def __post_init__(self) -> None:
        for name in (
            "candidate_id",
            "workload_id",
            "region_lineage_id",
            "target_id",
            "track_id",
            "detection_id",
            "source_candidate_id",
            "analysis_grid_id",
            "method",
        ):
            _safe_id(getattr(self, name), name)
        if (
            not isinstance(self.source_candidate_content_sha256, str)
            or len(self.source_candidate_content_sha256) != 64
            or any(
                character not in "0123456789abcdef"
                for character in self.source_candidate_content_sha256
            )
        ):
            raise ValueError("arrival source checksum is ongeldig")
        for name in (
            "reference_valid_at",
            "horizon_seconds",
            "entry_seconds",
            "exit_seconds",
            "vx_m_s",
            "vy_m_s",
            "area_km2",
            "mean_rain_rate_mm_h",
            "maximum_rain_rate_mm_h",
        ):
            if not math.isfinite(getattr(self, name)):
                raise ValueError(f"{name} moet eindig zijn")
        if (
            self.horizon_seconds <= 0.0
            or self.entry_seconds < 0.0
            or self.exit_seconds < self.entry_seconds
            or self.exit_seconds > self.horizon_seconds
        ):
            raise ValueError("arrival tijdvenster is ongeldig")
        _bounded_count(self.track_sample_count, "track_sample_count", 1_000_000)
        _bounded_count(
            self.footprint_pixel_count,
            "footprint_pixel_count",
            MAX_ROI_PIXELS,
        )
        if self.track_sample_count < 2 or self.footprint_pixel_count < 1:
            raise ValueError("arrival vereist trackhistorie en footprint")
        if (
            self.area_km2 <= 0.0
            or self.mean_rain_rate_mm_h < 0.0
            or self.maximum_rain_rate_mm_h < self.mean_rain_rate_mm_h
        ):
            raise ValueError("arrival intensiteiten zijn ongeldig")
        if self.target_location_accuracy_m is not None and (
            not math.isfinite(self.target_location_accuracy_m)
            or self.target_location_accuracy_m < 0.0
        ):
            raise ValueError("arrival targetnauwkeurigheid is ongeldig")
        if type(self.low_confidence_fragment) is not bool:
            raise TypeError("low_confidence_fragment moet boolean zijn")
        expected_state = (
            "currently_intersecting"
            if self.entry_seconds == 0.0
            else "future_intersection"
        )
        if self.passage_state != expected_state:
            raise ValueError("arrival passage_state verschilt van entry")
        if self.method != SHADOW_ARRIVAL_METHOD:
            raise ValueError("arrival methode is onbekend")
        if self.admission_status != ADMISSION_STATUS:
            raise ValueError("arrival candidate mag geen admission verlenen")


@dataclass(frozen=True, slots=True)
class WorkerOnlyShadowArrivalResult:
    candidates: tuple[WorkerOnlyShadowArrivalCandidate, ...]
    eligible_tracks: int
    evaluated_pixel_target_pairs: int
    reason_codes: tuple[str, ...]

    def __post_init__(self) -> None:
        if (
            not isinstance(self.candidates, tuple)
            or any(
                not isinstance(item, WorkerOnlyShadowArrivalCandidate)
                for item in self.candidates
            )
            or self.candidates
            != tuple(sorted(
                self.candidates,
                key=lambda item: (
                    item.target_id,
                    item.entry_seconds,
                    item.track_id,
                ),
            ))
        ):
            raise ValueError("arrival candidates zijn niet canoniek")
        _bounded_count(self.eligible_tracks, "eligible_tracks", 1_000_000)
        _bounded_count(
            self.evaluated_pixel_target_pairs,
            "evaluated_pixel_target_pairs",
            MAX_SHADOW_ARRIVAL_PIXEL_TARGET_PAIRS,
        )
        if self.reason_codes != tuple(sorted(set(self.reason_codes))):
            raise ValueError("arrival reason_codes zijn niet canoniek")
        for reason in self.reason_codes:
            _safe_id(reason, "arrival reason")


@dataclass(frozen=True, slots=True)
class WorkerOnlyGroundTrackMatchCandidate:
    candidate_id: str
    ground_workload_id: str
    radar_workload_id: str
    provider: str
    product: str
    receipt_sha256: str
    fact_sha256: str
    station_id: str
    track_id: str
    detection_id: str
    analysis_grid_id: str
    observation_representative_at: float
    track_reference_valid_at: float
    back_advection_seconds: float
    vx_m_s: float
    vy_m_s: float
    measurement_variable: str
    measurement_state: str
    measurement_representation: str
    measurement_unit: str
    site_identity_status: str = "unresolved"
    semantics_status: str = "uninterpreted_canonical_measurement"
    method: str = GROUND_TRACK_MATCH_METHOD
    admission_status: str = ADMISSION_STATUS

    def __post_init__(self) -> None:
        for name in (
            "candidate_id",
            "ground_workload_id",
            "radar_workload_id",
            "provider",
            "product",
            "station_id",
            "track_id",
            "detection_id",
            "analysis_grid_id",
            "measurement_variable",
            "measurement_state",
            "measurement_representation",
            "measurement_unit",
            "site_identity_status",
            "semantics_status",
            "method",
        ):
            _safe_id(getattr(self, name), name)
        for name in ("receipt_sha256", "fact_sha256"):
            value = getattr(self, name)
            if (
                not isinstance(value, str)
                or len(value) != 64
                or any(character not in "0123456789abcdef" for character in value)
            ):
                raise ValueError(f"{name} is ongeldig")
        for name in (
            "observation_representative_at",
            "track_reference_valid_at",
            "back_advection_seconds",
            "vx_m_s",
            "vy_m_s",
        ):
            if not math.isfinite(getattr(self, name)):
                raise ValueError(f"{name} moet eindig zijn")
        if (
            self.observation_representative_at > self.track_reference_valid_at
            or self.back_advection_seconds
            != self.track_reference_valid_at - self.observation_representative_at
        ):
            raise ValueError("ground-track tijdsbinding is ongeldig")
        if self.site_identity_status != "unresolved":
            raise ValueError("ground-track match mag geen site-identiteit claimen")
        if self.semantics_status != "uninterpreted_canonical_measurement":
            raise ValueError("ground-track match mag geen neerslagsemantiek claimen")
        if self.method != GROUND_TRACK_MATCH_METHOD:
            raise ValueError("ground-track matchmethode is onbekend")
        if self.admission_status != ADMISSION_STATUS:
            raise ValueError("ground-track match mag geen admission verlenen")


@dataclass(frozen=True, slots=True)
class WorkerOnlyGroundTrackMatchResult:
    candidates: tuple[WorkerOnlyGroundTrackMatchCandidate, ...]
    eligible_tracks: int
    eligible_receipts: int
    evaluated_pairs: int
    reason_codes: tuple[str, ...]

    def __post_init__(self) -> None:
        if (
            not isinstance(self.candidates, tuple)
            or any(
                not isinstance(item, WorkerOnlyGroundTrackMatchCandidate)
                for item in self.candidates
            )
            or self.candidates
            != tuple(sorted(
                self.candidates,
                key=lambda item: (
                    item.track_id,
                    item.observation_representative_at,
                    item.receipt_sha256,
                ),
            ))
        ):
            raise ValueError("ground-track candidates zijn niet canoniek")
        _bounded_count(self.eligible_tracks, "eligible_tracks", 1_000_000)
        _bounded_count(
            self.eligible_receipts,
            "eligible_receipts",
            MAX_GROUND_TRACK_RECEIPTS,
        )
        _bounded_count(self.evaluated_pairs, "evaluated_pairs", MAX_GROUND_TRACK_PAIRS)
        if self.reason_codes != tuple(sorted(set(self.reason_codes))):
            raise ValueError("ground-track reason_codes zijn niet canoniek")


def worker_only_tracking_policy() -> CellTrackEnginePolicy:
    """Return the explicit low-resource, still-unadmitted tracking policy."""

    return CellTrackEnginePolicy(
        policy_id="worker-only-bounded-shadow-tracking-v1",
        detector=HierarchicalDetectorConfig(
            shield_threshold=1.0,
            core_threshold=4.0,
            minimum_shield_area_m2=1_000_000.0,
            minimum_core_area_m2=1_000_000.0,
            shield_position_sigma_m=1_000.0,
            core_position_sigma_m=750.0,
            connectivity=8,
            centroid_method="geometric",
        ),
        lifecycle=CellTrackLifecyclePolicy(
            policy_id="worker-only-bounded-shadow-lifecycle-v1",
            confirmation_observations=2,
            maximum_gap_frames=2,
        ),
        estimator=RecursiveTrackEstimatorPolicy(
            policy_id="worker-only-bounded-shadow-estimator-v1",
            acceleration_spectral_density_m2_s3=1.0,
        ),
        lucas_kanade=LucasKanadeConfig(
            window_radius_pixels=2,
            minimum_valid_samples=6,
            minimum_structure_eigenvalue=1e-9,
            maximum_condition_number=1e10,
            regularization=0.0,
            residual_variance_floor=1e-6,
            velocity_process_noise_covariance_m2_s2=(
                (1.0, 0.0),
                (0.0, 1.0),
            ),
        ),
        flow_aggregation=DetectionFlowAggregationConfig(
            minimum_valid_pixels=1,
            minimum_valid_fraction=0.1,
            velocity_covariance_floor_m2_s2=(
                (1.0, 0.0),
                (0.0, 1.0),
            ),
            residual_position_covariance_m2=(
                (1_000_000.0, 0.0),
                (0.0, 1_000_000.0),
            ),
            method="conservative_mean",
        ),
        weather_system=WeatherSystemGroupingPolicy(
            policy_id="worker-only-bounded-shadow-systems-v1",
            pixel_connectivity=8,
            maximum_footprint_gap_m=20_000.0,
            maximum_motion_difference_m_s=1_000_000.0,
            maximum_motion_age_s=10_000.0,
            maximum_motion_time_difference_s=10_000.0,
        ),
        initial_state_covariance=_INITIAL4,
        prior_process_noise_covariance=_ZERO4,
        association_motion_sigma_m_s=100.0,
        motion_nll_weight=1.0,
        object_kind_change_cost=50.0,
        absolute_log_area_weight=1.0,
        mean_value_difference_weight=0.1,
        maximum_value_difference_weight=0.1,
        footprint_nonoverlap_weight=1.0,
        birth_cost=15.0,
        death_cost=15.0,
        exact_solution_limit=4,
        maximum_exact_current_detections=12,
        maximum_frame_pixels=MAX_ROI_PIXELS,
        maximum_detected_objects=128,
        maximum_active_tracks=128,
        maximum_candidate_edges=16_384,
        maximum_total_tracks=4_096,
        maximum_lifecycle_events=32_768,
        maximum_track_history_detections=2_048,
        multi_hypothesis_identity=None,
        scalable_one_best_fallback=True,
    )


def _axis_translation_interval(
    point: float,
    minimum: float,
    maximum: float,
    velocity: float,
) -> tuple[float, float] | None:
    """Return times when one translated closed axis interval contains a point."""

    if velocity == 0.0:
        return (-math.inf, math.inf) if minimum <= point <= maximum else None
    first = (point - maximum) / velocity
    second = (point - minimum) / velocity
    return min(first, second), max(first, second)


def _pixel_target_interval(
    *,
    grid: AnalysisGrid,
    pixel: tuple[int, int],
    target: WorkerOnlyProjectedTarget,
    vx_m_s: float,
    vy_m_s: float,
    horizon_seconds: float,
) -> tuple[float, float] | None:
    row, column = pixel
    if row >= grid.height or column >= grid.width:
        raise WorkerOnlyCellTrackingError("arrival_footprint_pixel_outside_grid")
    west = grid.origin_x_m + column * grid.x_resolution_m
    east = west + grid.x_resolution_m
    north = grid.origin_y_m - row * grid.y_resolution_m
    south = north - grid.y_resolution_m
    x_interval = _axis_translation_interval(target.x_m, west, east, vx_m_s)
    y_interval = _axis_translation_interval(target.y_m, south, north, vy_m_s)
    if x_interval is None or y_interval is None:
        return None
    entry = max(0.0, x_interval[0], y_interval[0])
    exit_at = min(horizon_seconds, x_interval[1], y_interval[1])
    if exit_at < entry:
        return None
    return entry, exit_at


def _first_union_interval(
    intervals: list[tuple[float, float]],
) -> tuple[float, float] | None:
    if not intervals:
        return None
    intervals.sort()
    first_entry, first_exit = intervals[0]
    for entry, exit_at in intervals[1:]:
        if entry > first_exit:
            break
        first_exit = max(first_exit, exit_at)
    return first_entry, first_exit


def build_worker_only_shadow_arrivals(
    *,
    workload_id: str,
    candidate: WorkerOnlyTrackingFrameCandidate,
    state: CellTrackEngineState,
    maximum_pairs: int = MAX_SHADOW_ARRIVAL_PIXEL_TARGET_PAIRS,
) -> WorkerOnlyShadowArrivalResult:
    """Intersect same-track translated rain footprints with exact target points.

    This is deterministic geometry only.  It intentionally emits neither a
    passage probability nor an operational ETA/warning admission.
    """

    if workload_id != candidate.workload_id:
        raise WorkerOnlyCellTrackingError("arrival_workload_binding_mismatch")
    _bounded_count(
        maximum_pairs,
        "maximum_pairs",
        MAX_SHADOW_ARRIVAL_PIXEL_TARGET_PAIRS,
    )
    if state.last_frame is None or not state.last_frame.grid.same_geometry(
        candidate.frame.grid
    ):
        raise WorkerOnlyCellTrackingError("arrival_state_grid_binding_mismatch")
    if state.last_frame.frame_id != candidate.frame.frame_id:
        raise WorkerOnlyCellTrackingError("arrival_state_frame_binding_mismatch")

    eligible = []
    for track in state.ledger.tracks:
        if (
            track.lifecycle_state != "confirmed"
            or track.sample_count < 2
            or track.latest_detection.valid_at != candidate.frame.valid_at
        ):
            continue
        detected = state.hierarchy(track.track_id)
        estimate = state.estimate(track.track_id)
        if (
            detected.object_kind != "rain_shield"
            or estimate.latest_interval_vx_m_s is None
            or estimate.latest_interval_vy_m_s is None
        ):
            continue
        eligible.append((track, detected, estimate))

    operation_count = sum(
        len(detected.footprint_pixels) * len(candidate.projected_targets)
        for _track, detected, _estimate in eligible
    )
    if operation_count > maximum_pairs:
        return WorkerOnlyShadowArrivalResult(
            candidates=(),
            eligible_tracks=len(eligible),
            evaluated_pixel_target_pairs=0,
            reason_codes=("shadow_arrival_capacity_exceeded",),
        )

    arrivals: list[WorkerOnlyShadowArrivalCandidate] = []
    grid = candidate.frame.grid
    for track, detected, estimate in eligible:
        vx_m_s = float(estimate.latest_interval_vx_m_s)
        vy_m_s = float(estimate.latest_interval_vy_m_s)
        for target in candidate.projected_targets:
            intervals = []
            for pixel in detected.footprint_pixels:
                interval = _pixel_target_interval(
                    grid=grid,
                    pixel=pixel,
                    target=target,
                    vx_m_s=vx_m_s,
                    vy_m_s=vy_m_s,
                    horizon_seconds=SHADOW_ARRIVAL_HORIZON_SECONDS,
                )
                if interval is not None:
                    intervals.append(interval)
            first = _first_union_interval(intervals)
            if first is None:
                continue
            entry_seconds, exit_seconds = first
            passage_state = (
                "currently_intersecting"
                if entry_seconds == 0.0
                else "future_intersection"
            )
            identity = {
                "contract": "stv3-worker-only-shadow-arrival-candidate-v1",
                "detection_id": detected.detection.detection_id,
                "entry_seconds": entry_seconds,
                "exit_seconds": exit_seconds,
                "method": SHADOW_ARRIVAL_METHOD,
                "region_lineage_id": candidate.region_lineage_id,
                "source_candidate_content_sha256": (
                    candidate.source_candidate_content_sha256
                ),
                "target_id": target.target_id,
                "track_id": track.track_id,
                "vx_m_s": vx_m_s,
                "vy_m_s": vy_m_s,
                "workload_id": workload_id,
            }
            arrivals.append(WorkerOnlyShadowArrivalCandidate(
                candidate_id="shadow-arrival:" + _digest(identity),
                workload_id=workload_id,
                region_lineage_id=candidate.region_lineage_id,
                target_id=target.target_id,
                track_id=track.track_id,
                detection_id=detected.detection.detection_id,
                source_candidate_id=candidate.source_candidate_id,
                source_candidate_content_sha256=(
                    candidate.source_candidate_content_sha256
                ),
                analysis_grid_id=grid.grid_id,
                reference_valid_at=candidate.frame.valid_at,
                horizon_seconds=SHADOW_ARRIVAL_HORIZON_SECONDS,
                entry_seconds=entry_seconds,
                exit_seconds=exit_seconds,
                vx_m_s=vx_m_s,
                vy_m_s=vy_m_s,
                track_sample_count=track.sample_count,
                footprint_pixel_count=len(detected.footprint_pixels),
                area_km2=detected.area_m2 / 1_000_000.0,
                mean_rain_rate_mm_h=detected.mean_value,
                maximum_rain_rate_mm_h=detected.maximum_value,
                target_location_accuracy_m=target.location_accuracy_m,
                low_confidence_fragment=detected.low_confidence_fragment,
                passage_state=passage_state,
            ))

    ordered = tuple(sorted(
        arrivals,
        key=lambda item: (item.target_id, item.entry_seconds, item.track_id),
    ))
    if ordered:
        reasons: tuple[str, ...] = ()
    elif not eligible:
        reasons = ("no_confirmed_same_track_motion",)
    else:
        reasons = ("no_shadow_intersection_within_horizon",)
    return WorkerOnlyShadowArrivalResult(
        candidates=ordered,
        eligible_tracks=len(eligible),
        evaluated_pixel_target_pairs=operation_count,
        reason_codes=reasons,
    )


def build_worker_only_ground_track_matches(
    *,
    ground_workload_id: str,
    radar_workload_id: str,
    state: CellTrackEngineState,
    receipts: tuple[GroundObservationReceipt, ...],
) -> WorkerOnlyGroundTrackMatchResult:
    """Bind canonical ground points to a same-track footprint without semantics.

    The latest footprint is translated back only within the exact final track
    interval.  A geometric match is not a wet/dry interpretation, physical-site
    identity proof, independent vote, forecast validation or admission.
    """

    _safe_id(ground_workload_id, "ground_workload_id")
    _safe_id(radar_workload_id, "radar_workload_id")
    if not isinstance(state, CellTrackEngineState) or state.last_frame is None:
        raise TypeError("ground-track matching vereist een niet-lege trackstate")
    if (
        not isinstance(receipts, tuple)
        or len(receipts) > MAX_GROUND_TRACK_RECEIPTS
        or any(not isinstance(item, GroundObservationReceipt) for item in receipts)
    ):
        raise ValueError("ground-track receipts overschrijden het contract")

    eligible_tracks = []
    for track in state.ledger.tracks:
        if (
            track.lifecycle_state != "confirmed"
            or track.sample_count < 2
            or track.latest_detection.valid_at != state.last_frame.valid_at
        ):
            continue
        detected = state.hierarchy(track.track_id)
        estimate = state.estimate(track.track_id)
        if (
            detected.object_kind != "rain_shield"
            or estimate.latest_interval_vx_m_s is None
            or estimate.latest_interval_vy_m_s is None
        ):
            continue
        eligible_tracks.append((track, detected, estimate))

    positioned_receipts = tuple(
        receipt
        for receipt in receipts
        if receipt.fact.station.position_state == "known"
        and receipt.fact.station.latitude_decimal is not None
        and receipt.fact.station.longitude_decimal is not None
    )
    pair_count = len(eligible_tracks) * len(positioned_receipts)
    if pair_count > MAX_GROUND_TRACK_PAIRS:
        return WorkerOnlyGroundTrackMatchResult(
            candidates=(),
            eligible_tracks=len(eligible_tracks),
            eligible_receipts=len(positioned_receipts),
            evaluated_pairs=0,
            reason_codes=("ground_track_match_capacity_exceeded",),
        )

    grid = state.last_frame.grid
    try:
        transformer = Transformer.from_crs(
            "EPSG:4326",
            grid.crs,
            always_xy=True,
        )
    except CRSError as error:
        raise WorkerOnlyCellTrackingError(
            "ground_track_grid_projection_invalid"
        ) from error
    matches: list[WorkerOnlyGroundTrackMatchCandidate] = []
    for track, detected, estimate in eligible_tracks:
        start_at = track.detections[-2].valid_at
        end_at = track.latest_detection.valid_at
        footprint = frozenset(detected.footprint_pixels)
        vx_m_s = float(estimate.latest_interval_vx_m_s)
        vy_m_s = float(estimate.latest_interval_vy_m_s)
        for receipt in positioned_receipts:
            fact = receipt.fact
            representative_at = float(fact.observed_at)
            if not start_at <= representative_at <= end_at:
                continue
            try:
                station_longitude = float(fact.station.longitude_decimal)
                station_latitude = float(fact.station.latitude_decimal)
                station_x_m, station_y_m = transformer.transform(
                    station_longitude,
                    station_latitude,
                )
            except (TypeError, ValueError, ProjError):
                continue
            if not math.isfinite(station_x_m) or not math.isfinite(station_y_m):
                continue
            delta_seconds = representative_at - end_at
            reference_x_m = station_x_m - vx_m_s * delta_seconds
            reference_y_m = station_y_m - vy_m_s * delta_seconds
            try:
                pixel = grid.point_to_pixel(reference_x_m, reference_y_m)
            except ValueError:
                continue
            if pixel not in footprint:
                continue
            identity = {
                "contract": "stv3-worker-only-ground-track-match-v1",
                "detection_id": detected.detection.detection_id,
                "method": GROUND_TRACK_MATCH_METHOD,
                "observation_representative_at": representative_at,
                "receipt_sha256": receipt.receipt_sha256,
                "track_id": track.track_id,
                "ground_workload_id": ground_workload_id,
                "radar_workload_id": radar_workload_id,
            }
            measurement = fact.measurement
            matches.append(WorkerOnlyGroundTrackMatchCandidate(
                candidate_id="ground-track-match:" + _digest(identity),
                ground_workload_id=ground_workload_id,
                radar_workload_id=radar_workload_id,
                provider=fact.provider,
                product=fact.product,
                receipt_sha256=receipt.receipt_sha256,
                fact_sha256=receipt.fact_sha256,
                station_id=fact.station.provider_station_id,
                track_id=track.track_id,
                detection_id=detected.detection.detection_id,
                analysis_grid_id=grid.grid_id,
                observation_representative_at=representative_at,
                track_reference_valid_at=end_at,
                back_advection_seconds=end_at - representative_at,
                vx_m_s=vx_m_s,
                vy_m_s=vy_m_s,
                measurement_variable=measurement.variable,
                measurement_state=measurement.state,
                measurement_representation=measurement.representation,
                measurement_unit=measurement.unit,
            ))

    ordered = tuple(sorted(
        matches,
        key=lambda item: (
            item.track_id,
            item.observation_representative_at,
            item.receipt_sha256,
        ),
    ))
    if ordered:
        reasons: tuple[str, ...] = ()
    elif not eligible_tracks:
        reasons = ("no_confirmed_same_track_motion",)
    elif not positioned_receipts:
        reasons = ("no_positioned_canonical_ground_receipts",)
    else:
        reasons = ("no_same_track_ground_geometry_match",)
    return WorkerOnlyGroundTrackMatchResult(
        candidates=ordered,
        eligible_tracks=len(eligible_tracks),
        eligible_receipts=len(positioned_receipts),
        evaluated_pairs=pair_count,
        reason_codes=reasons,
    )


class WorkerOnlyCellTrackingStore:
    """Crash-durable latest head per stable provider/target lineage."""

    def __init__(
        self,
        path: str | Path,
        *,
        clock=time.time,
    ) -> None:
        self.path = Path(path)
        if not callable(clock):
            raise TypeError("tracking store clock moet callable zijn")
        self._clock = clock
        self._lock = threading.RLock()
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        database = sqlite3.connect(self.path, timeout=5.0)
        try:
            database.row_factory = sqlite3.Row
            database.execute("PRAGMA journal_mode=WAL")
            database.execute("PRAGMA synchronous=FULL")
            database.execute("PRAGMA foreign_keys=ON")
            database.execute("PRAGMA wal_autocheckpoint=64")
            database.execute("PRAGMA journal_size_limit=8388608")
            page_size = int(database.execute("PRAGMA page_size").fetchone()[0])
            page_count = int(database.execute("PRAGMA page_count").fetchone()[0])
            if page_count * page_size > MAX_TRACKING_STORE_BYTES:
                raise WorkerOnlyCellTrackingStoreError(
                    "tracking_store_file_size_exceeded"
                )
            database.execute(
                f"PRAGMA max_page_count={MAX_TRACKING_STORE_BYTES // page_size}"
            )
            return database
        except BaseException:
            database.close()
            raise

    def _initialize(self) -> None:
        with self._lock, closing(self._connect()) as database, database:
            database.executescript(
                """
                CREATE TABLE IF NOT EXISTS tracking_store_meta (
                    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                    schema_version INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS tracking_heads (
                    state_key TEXT PRIMARY KEY,
                    grid_id TEXT NOT NULL,
                    policy_fingerprint TEXT NOT NULL,
                    state_revision INTEGER NOT NULL CHECK(state_revision >= 1),
                    valid_at REAL NOT NULL,
                    raw_size_bytes INTEGER NOT NULL CHECK(raw_size_bytes >= 1),
                    raw_sha256 TEXT NOT NULL,
                    compressed_state BLOB NOT NULL,
                    updated_at REAL NOT NULL
                );
                """
            )
            row = database.execute(
                "SELECT schema_version FROM tracking_store_meta WHERE singleton=1"
            ).fetchone()
            if row is None:
                database.execute(
                    "INSERT INTO tracking_store_meta(singleton,schema_version) VALUES(1,?)",
                    (TRACKING_STORE_SCHEMA_VERSION,),
                )
            elif int(row["schema_version"]) != TRACKING_STORE_SCHEMA_VERSION:
                raise WorkerOnlyCellTrackingStoreError(
                    "tracking_store_schema_mismatch"
                )
            integrity = database.execute("PRAGMA quick_check").fetchone()
            if integrity is None or integrity[0] != "ok":
                raise WorkerOnlyCellTrackingStoreError(
                    "tracking_store_integrity_failed"
                )
            count = int(database.execute(
                "SELECT COUNT(*) FROM tracking_heads"
            ).fetchone()[0])
            if count > MAX_TRACKING_STATES:
                raise WorkerOnlyCellTrackingStoreError(
                    "tracking_store_state_capacity_exceeded"
                )

    @staticmethod
    def _encoded_state(state: CellTrackEngineState) -> tuple[bytes, bytes, str]:
        if not isinstance(state, CellTrackEngineState) or state.revision < 1:
            raise WorkerOnlyCellTrackingStoreError(
                "tracking_store_state_invalid"
            )
        try:
            raw = _canonical_json(encode_cell_track_state_value(state))
        except CellTrackStateCodecError as error:
            raise WorkerOnlyCellTrackingStoreError(
                "tracking_store_state_encode_failed"
            ) from error
        if not 1 <= len(raw) <= MAX_TRACKING_STATE_BYTES:
            raise WorkerOnlyCellTrackingStoreError(
                "tracking_store_state_size_exceeded"
            )
        compressed = zlib.compress(raw, level=6)
        if not 1 <= len(compressed) <= MAX_TRACKING_STATE_BYTES:
            raise WorkerOnlyCellTrackingStoreError(
                "tracking_store_compressed_size_exceeded"
            )
        return raw, compressed, hashlib.sha256(raw).hexdigest()

    @staticmethod
    def _decoded_row(row: sqlite3.Row) -> CellTrackEngineState:
        raw_size = int(row["raw_size_bytes"])
        compressed = bytes(row["compressed_state"])
        if (
            not 1 <= raw_size <= MAX_TRACKING_STATE_BYTES
            or not 1 <= len(compressed) <= MAX_TRACKING_STATE_BYTES
        ):
            raise WorkerOnlyCellTrackingStoreError(
                "tracking_store_state_size_invalid"
            )
        inflater = zlib.decompressobj()
        try:
            raw = inflater.decompress(compressed, MAX_TRACKING_STATE_BYTES + 1)
            raw += inflater.flush(MAX_TRACKING_STATE_BYTES + 1 - len(raw))
        except zlib.error as error:
            raise WorkerOnlyCellTrackingStoreError(
                "tracking_store_state_compression_invalid"
            ) from error
        if (
            len(raw) != raw_size
            or not inflater.eof
            or hashlib.sha256(raw).hexdigest() != row["raw_sha256"]
        ):
            raise WorkerOnlyCellTrackingStoreError(
                "tracking_store_state_checksum_mismatch"
            )
        try:
            document = json.loads(raw.decode("ascii"))
            state = decode_cell_track_state_value(document)
        except (UnicodeDecodeError, json.JSONDecodeError, CellTrackStateCodecError) as error:
            raise WorkerOnlyCellTrackingStoreError(
                "tracking_store_state_decode_failed"
            ) from error
        if not isinstance(state, CellTrackEngineState):
            raise WorkerOnlyCellTrackingStoreError(
                "tracking_store_head_type_invalid"
            )
        if (
            state.revision != int(row["state_revision"])
            or state.policy_fingerprint != row["policy_fingerprint"]
            or state.last_frame is None
            or state.last_frame.grid.grid_id != row["grid_id"]
            or state.last_frame.valid_at != float(row["valid_at"])
        ):
            raise WorkerOnlyCellTrackingStoreError(
                "tracking_store_head_binding_mismatch"
            )
        return state

    def load_heads(
        self,
        *,
        policy_fingerprint: str,
    ) -> dict[str, CellTrackEngineState]:
        with self._lock, closing(self._connect()) as database:
            rows = database.execute(
                "SELECT * FROM tracking_heads ORDER BY state_key"
            ).fetchall()
            if len(rows) > MAX_TRACKING_STATES:
                raise WorkerOnlyCellTrackingStoreError(
                    "tracking_store_state_capacity_exceeded"
                )
            result: dict[str, CellTrackEngineState] = {}
            for row in rows:
                state_key = _safe_id(row["state_key"], "state_key")
                if row["policy_fingerprint"] != policy_fingerprint:
                    raise WorkerOnlyCellTrackingStoreError(
                        "tracking_store_policy_mismatch"
                    )
                result[state_key] = self._decoded_row(row)
            return result

    def save_head(
        self,
        *,
        state_key: str,
        state: CellTrackEngineState,
        expected_previous_revision: int,
        grid_reset: bool,
    ) -> None:
        _safe_id(state_key, "state_key")
        _bounded_count(
            expected_previous_revision,
            "expected_previous_revision",
            2**31 - 1,
        )
        if type(grid_reset) is not bool:
            raise TypeError("grid_reset moet boolean zijn")
        raw, compressed, raw_sha = self._encoded_state(state)
        assert state.last_frame is not None
        updated_at = float(self._clock())
        if not math.isfinite(updated_at) or updated_at < 0.0:
            raise WorkerOnlyCellTrackingStoreError(
                "tracking_store_clock_invalid"
            )
        with self._lock, closing(self._connect()) as database:
            database.execute("BEGIN IMMEDIATE")
            current = database.execute(
                "SELECT grid_id,state_revision FROM tracking_heads WHERE state_key=?",
                (state_key,),
            ).fetchone()
            if current is None:
                count = int(database.execute(
                    "SELECT COUNT(*) FROM tracking_heads"
                ).fetchone()[0])
                if count >= MAX_TRACKING_STATES:
                    raise WorkerOnlyCellTrackingStoreError(
                        "tracking_store_state_capacity_exceeded"
                    )
                if expected_previous_revision != 0:
                    raise WorkerOnlyCellTrackingStoreError(
                        "tracking_store_revision_conflict"
                    )
            elif grid_reset:
                if current["grid_id"] == state.last_frame.grid.grid_id:
                    raise WorkerOnlyCellTrackingStoreError(
                        "tracking_store_false_grid_reset"
                    )
            elif (
                current["grid_id"] != state.last_frame.grid.grid_id
                or int(current["state_revision"]) != expected_previous_revision
            ):
                raise WorkerOnlyCellTrackingStoreError(
                    "tracking_store_revision_conflict"
                )
            database.execute(
                """
                INSERT INTO tracking_heads(
                    state_key,grid_id,policy_fingerprint,state_revision,
                    valid_at,raw_size_bytes,raw_sha256,compressed_state,updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?)
                ON CONFLICT(state_key) DO UPDATE SET
                    grid_id=excluded.grid_id,
                    policy_fingerprint=excluded.policy_fingerprint,
                    state_revision=excluded.state_revision,
                    valid_at=excluded.valid_at,
                    raw_size_bytes=excluded.raw_size_bytes,
                    raw_sha256=excluded.raw_sha256,
                    compressed_state=excluded.compressed_state,
                    updated_at=excluded.updated_at
                """,
                (
                    state_key,
                    state.last_frame.grid.grid_id,
                    state.policy_fingerprint,
                    state.revision,
                    state.last_frame.valid_at,
                    len(raw),
                    raw_sha,
                    sqlite3.Binary(compressed),
                    updated_at,
                ),
            )
            database.commit()

    def diagnostics(self) -> dict[str, int]:
        with self._lock, closing(self._connect()) as database:
            row = database.execute(
                "SELECT COUNT(*),COALESCE(SUM(raw_size_bytes),0),"
                "COALESCE(SUM(LENGTH(compressed_state)),0) FROM tracking_heads"
            ).fetchone()
        return {
            "state_count": int(row[0]),
            "raw_state_bytes": int(row[1]),
            "compressed_state_bytes": int(row[2]),
        }


def _metric_source_grid(
    workload_id: str,
    source: VerifiedRadarFieldSource,
) -> tuple[AnalysisGrid | None, str | None]:
    field = source.field
    if workload_id == "radar.dwd.radolan_rv":
        certificate = source.native_grid_certificate
        if certificate is None:
            return None, "dwd_metric_grid_certificate_missing"
        if (
            certificate.source_width != field.native_grid.width
            or certificate.source_height != field.native_grid.height
            or certificate.source_crs != field.native_crs
        ):
            raise WorkerOnlyCellTrackingError(
                "dwd_metric_grid_certificate_mismatch"
            )
        grid = AnalysisGrid(
            # The certificate is intentionally bound to one frame's content,
            # so its fingerprint changes for every valid DWD scan.  It may be
            # used to validate the geometry, but it must never become the
            # temporal track grid identity.  The stable geometry-only ID is
            # installed below for every trackable provider.
            grid_id=certificate.certificate_id,
            crs=certificate.target_crs,
            origin_x_m=certificate.source_west_edge_target_m,
            origin_y_m=certificate.source_north_edge_target_m,
            x_resolution_m=certificate.source_x_resolution_target_m,
            y_resolution_m=certificate.source_y_resolution_target_m,
            width=certificate.source_width,
            height=certificate.source_height,
        )
    elif workload_id == "radar.metoffice.uk_rate":
        if field.analysis_grid is None:
            return None, "metoffice_metric_grid_missing"
        grid = field.analysis_grid
    elif workload_id == "radar.knmi.rtcor":
        return None, "knmi_metric_grid_semantics_withheld"
    elif workload_id == "satellite.noaa.err":
        return None, "noaa_metric_reprojection_not_certified"
    else:
        return None, "tracking_workload_not_supported"
    try:
        crs = CRS.from_user_input(grid.crs)
    except CRSError:
        return None, "metric_grid_crs_invalid"
    if not crs.is_projected:
        return None, "metric_grid_crs_not_projected"
    if grid.width != field.native_grid.width or grid.height != field.native_grid.height:
        raise WorkerOnlyCellTrackingError("metric_grid_shape_mismatch")
    return _stable_metric_source_grid(workload_id, grid), None


def _stable_metric_source_grid(
    workload_id: str,
    grid: AnalysisGrid,
) -> AnalysisGrid:
    """Replace a frame-bound provider grid ID with exact geometry identity.

    Provider evidence and transform certificates remain fully content-bound.
    Cell tracking, however, must compare consecutive values on the same fixed
    geometry.  Including a capture/certificate ID here resets every track on
    every frame and makes motion mathematically impossible.
    """

    _safe_id(workload_id, "workload_id")
    geometry_id = "worker-only-source-grid:" + _digest({
        "axis_order": grid.axis_order,
        "contract": "stv3-worker-only-metric-grid-geometry-v1",
        "coordinate_unit": grid.coordinate_unit,
        "crs": grid.crs,
        "height": grid.height,
        "origin_x_m": grid.origin_x_m,
        "origin_y_m": grid.origin_y_m,
        "row_order": grid.row_order,
        "width": grid.width,
        "workload_id": workload_id,
        "x_resolution_m": grid.x_resolution_m,
        "y_resolution_m": grid.y_resolution_m,
    })
    return AnalysisGrid(
        grid_id=geometry_id,
        crs=grid.crs,
        origin_x_m=grid.origin_x_m,
        origin_y_m=grid.origin_y_m,
        x_resolution_m=grid.x_resolution_m,
        y_resolution_m=grid.y_resolution_m,
        width=grid.width,
        height=grid.height,
        coordinate_unit=grid.coordinate_unit,
        axis_order=grid.axis_order,
        row_order=grid.row_order,
        contract_version=grid.contract_version,
    )


def _region_lineage_id(workload_id: str, target_ids: tuple[str, ...]) -> str:
    return "tracking-region:" + _digest({
        "contract": "stv3-worker-only-tracking-region-lineage-v1",
        "target_ids": list(target_ids),
        "workload_id": workload_id,
    })


def _roi_shape(
    grid: AnalysisGrid,
    requested_radius_m: float,
) -> tuple[int, int]:
    radius = min(float(requested_radius_m), MAX_ROI_HALF_EXTENT_M)
    width = min(grid.width, max(1, 2 * math.ceil(radius / grid.x_resolution_m) + 1))
    height = min(grid.height, max(1, 2 * math.ceil(radius / grid.y_resolution_m) + 1))
    pixels = width * height
    if pixels > MAX_ROI_PIXELS:
        scale = math.sqrt(MAX_ROI_PIXELS / pixels)
        width = max(1, min(width, int(width * scale)))
        height = max(1, min(height, MAX_ROI_PIXELS // width))
    while width * height > MAX_ROI_PIXELS:
        if width >= height:
            width -= 1
        else:
            height -= 1
    return width, height


def _anchored_start(center: float, size: int, maximum: int) -> int:
    if size >= maximum:
        return 0
    stride = min(ROI_ANCHOR_STRIDE_PIXELS, max(1, size // 4))
    anchor = math.floor(center / stride) * stride
    return min(max(0, int(anchor - size // 2)), maximum - size)


def _candidate_for_region(
    workload_id: str,
    desired_state: DesiredState,
    source: VerifiedRadarFieldSource,
    grid: AnalysisGrid,
    region,
) -> WorkerOnlyTrackingDisposition:
    try:
        transformer = Transformer.from_crs(
            "EPSG:4326",
            grid.crs,
            always_xy=True,
        )
        x_m, y_m = transformer.transform(
            region.center_longitude,
            region.center_latitude,
        )
        targets_by_id = {
            item.target_id: item
            for item in desired_state.targets
            if item.available
        }
        projected = []
        for target_id in region.target_ids:
            target = targets_by_id[target_id]
            target_x_m, target_y_m = transformer.transform(
                target.longitude,
                target.latitude,
            )
            projected.append(WorkerOnlyProjectedTarget(
                target_id=target_id,
                x_m=float(target_x_m),
                y_m=float(target_y_m),
                location_accuracy_m=target.location_accuracy_m,
            ))
        projected_targets = tuple(projected)
    except (CRSError, ProjError):
        return WorkerOnlyTrackingDisposition(
            workload_id,
            region.region_id,
            "withheld",
            "target_projection_failed",
            None,
        )
    except KeyError as error:
        raise WorkerOnlyCellTrackingError(
            "tracking_region_target_binding_mismatch"
        ) from error
    if not math.isfinite(x_m) or not math.isfinite(y_m):
        return WorkerOnlyTrackingDisposition(
            workload_id,
            region.region_id,
            "withheld",
            "target_projection_nonfinite",
            None,
        )
    fractional_row, fractional_column = grid.fractional_center_index(x_m, y_m)
    if (
        fractional_row < -0.5
        or fractional_row >= grid.height - 0.5
        or fractional_column < -0.5
        or fractional_column >= grid.width - 0.5
    ):
        return WorkerOnlyTrackingDisposition(
            workload_id,
            region.region_id,
            "withheld",
            "region_center_outside_source_grid",
            None,
        )

    width, height = _roi_shape(grid, region.radius_km * 1_000.0)
    start_column = _anchored_start(fractional_column, width, grid.width)
    start_row = _anchored_start(fractional_row, height, grid.height)
    end_column = start_column + width
    end_row = start_row + height
    crop_grid = AnalysisGrid(
        grid_id="tracking-grid:" + _digest({
            "height": height,
            "source_grid_id": grid.grid_id,
            "start_column": start_column,
            "start_row": start_row,
            "width": width,
        }),
        crs=grid.crs,
        origin_x_m=grid.origin_x_m + start_column * grid.x_resolution_m,
        origin_y_m=grid.origin_y_m - start_row * grid.y_resolution_m,
        x_resolution_m=grid.x_resolution_m,
        y_resolution_m=grid.y_resolution_m,
        width=width,
        height=height,
    )
    field = source.field
    rates = field.rain_rate_mm_h[start_row:end_row, start_column:end_column]
    valid = field.valid_mask[start_row:end_row, start_column:end_column]
    if rates.shape != (height, width) or valid.shape != (height, width):
        raise WorkerOnlyCellTrackingError("tracking_roi_shape_mismatch")
    values = tuple(
        tuple(float(rates[row, column]) if valid[row, column] else None for column in range(width))
        for row in range(height)
    )
    source_content_sha = native_candidate_content_sha256(field)
    lineage_id = _region_lineage_id(workload_id, region.target_ids)
    frame_id = "tracking-frame:" + _digest({
        "capture_id": field.capture_id,
        "grid_id": crop_grid.grid_id,
        "region_lineage_id": lineage_id,
        "source_candidate_content_sha256": source_content_sha,
        "valid_at": field.valid_at,
    })
    frame = NormalizedRasterFrame(
        frame_id=frame_id,
        valid_at=field.valid_at,
        source=f"{workload_id}:{lineage_id}",
        quantity=field.output_quantity,
        unit=field.output_unit,
        grid=crop_grid,
        values=values,
    )
    west, south, east, north = crop_grid.extent_edges_m
    covered_radius_km = max(
        0.0,
        min(x_m - west, east - x_m, y_m - south, north - y_m) / 1_000.0,
    )
    candidate = WorkerOnlyTrackingFrameCandidate(
        workload_id=workload_id,
        region_id=region.region_id,
        region_lineage_id=lineage_id,
        target_ids=region.target_ids,
        capture_id=field.capture_id,
        source_candidate_id=field.candidate_id,
        source_candidate_content_sha256=source_content_sha,
        requested_radius_km=float(region.radius_km),
        covered_radius_km=covered_radius_km,
        coverage_complete=(covered_radius_km >= float(region.radius_km)),
        frame=frame,
        projected_targets=projected_targets,
    )
    return WorkerOnlyTrackingDisposition(
        workload_id,
        region.region_id,
        "candidate",
        "bounded_metric_roi_ready",
        candidate,
    )


def build_worker_only_tracking_batch(
    *,
    workload_id: str,
    desired_state: DesiredState,
    source: VerifiedRadarFieldSource,
) -> WorkerOnlyTrackingBatch:
    """Build exact, bounded metric frames or an explicit withheld result."""

    if not isinstance(desired_state, DesiredState):
        raise TypeError("desired_state heeft het verkeerde type")
    if not isinstance(source, VerifiedRadarFieldSource):
        raise TypeError("source heeft het verkeerde type")
    _safe_id(workload_id, "workload_id")
    matching_profiles = tuple(
        item for item in RADAR_PROVIDER_PROFILES if item.workload_id == workload_id
    )
    if len(matching_profiles) != 1 or (
        source.field.semantics_certificate_id
        != matching_profiles[0].certificate_id
    ):
        raise WorkerOnlyCellTrackingError(
            "tracking_source_workload_binding_mismatch"
        )
    regions = build_worker_acquisition_regions(desired_state)
    grid, reason = _metric_source_grid(workload_id, source)
    if grid is None:
        dispositions = tuple(
            WorkerOnlyTrackingDisposition(
                workload_id,
                region.region_id,
                "withheld",
                reason or "metric_grid_unavailable",
                None,
            )
            for region in regions
        )
    else:
        dispositions = tuple(
            _candidate_for_region(
                workload_id,
                desired_state,
                source,
                grid,
                region,
            )
            for region in regions
        )
    return WorkerOnlyTrackingBatch(
        workload_id=workload_id,
        source_candidate_id=source.field.candidate_id,
        dispositions=tuple(
            sorted(dispositions, key=lambda item: (item.workload_id, item.region_id))
        ),
    )


class WorkerOnlyCellTrackingRuntime:
    """Serialize bounded frame transitions without operational publication."""

    def __init__(
        self,
        *,
        policy: CellTrackEnginePolicy | None = None,
        maximum_states: int = MAX_TRACKING_STATES,
        state_path: str | Path | None = None,
        clock=time.time,
    ) -> None:
        self._policy = policy or worker_only_tracking_policy()
        if not isinstance(self._policy, CellTrackEnginePolicy):
            raise TypeError("tracking policy heeft het verkeerde type")
        if (
            isinstance(maximum_states, bool)
            or not isinstance(maximum_states, int)
            or not 1 <= maximum_states <= MAX_TRACKING_STATES
        ):
            raise ValueError("maximum_states ligt buiten de grens")
        self._maximum_states = maximum_states
        self._states: dict[str, CellTrackEngineState] = {}
        self._state_grid_ids: dict[str, str] = {}
        self._store: WorkerOnlyCellTrackingStore | None = None
        self._store_error: str | None = None
        self._last_dispositions: dict[str, str] = {}
        self._shadow_arrivals: dict[
            str,
            tuple[WorkerOnlyShadowArrivalCandidate, ...],
        ] = {}
        self._shadow_arrival_reasons: dict[str, tuple[str, ...]] = {}
        self._ground_receipts: dict[
            str,
            tuple[GroundObservationReceipt, ...],
        ] = {}
        self._ground_track_matches: dict[
            str,
            tuple[WorkerOnlyGroundTrackMatchCandidate, ...],
        ] = {}
        self._ground_track_reasons: dict[str, tuple[str, ...]] = {}
        self._counts = {
            "advanced": 0,
            "duplicates": 0,
            "indeterminate": 0,
            "resets": 0,
            "restored": 0,
            "ground_track_match_candidates": 0,
            "ground_track_match_evaluated_pairs": 0,
            "shadow_arrival_candidates": 0,
            "shadow_arrival_evaluated_pairs": 0,
            "withheld": 0,
        }
        self._lock = threading.RLock()
        if state_path is not None:
            try:
                store = WorkerOnlyCellTrackingStore(state_path, clock=clock)
                restored = store.load_heads(
                    policy_fingerprint=self._policy.fingerprint,
                )
                if len(restored) > self._maximum_states:
                    raise WorkerOnlyCellTrackingStoreError(
                        "tracking_store_state_capacity_exceeded"
                    )
                self._store = store
                self._states.update(restored)
                self._state_grid_ids.update({
                    key: state.last_frame.grid.grid_id
                    for key, state in restored.items()
                    if state.last_frame is not None
                })
                self._counts["restored"] = len(restored)
            except Exception:
                self._store = None
                self._store_error = "tracking_store_restore_failed"

    @staticmethod
    def _state_key(candidate: WorkerOnlyTrackingFrameCandidate) -> str:
        return f"{candidate.workload_id}|{candidate.region_lineage_id}"

    def _track_counts(self) -> Mapping[str, int]:
        tracks = tuple(
            track
            for state in self._states.values()
            for track in state.ledger.tracks
            if track.lifecycle_state != "ended"
        )
        return {
            "active": len(tracks),
            "tracked": sum(item.lifecycle_state == "confirmed" for item in tracks),
            "preliminary": sum(
                item.lifecycle_state == "tentative" for item in tracks
            ),
            "coasting": sum(item.lifecycle_state == "coasting" for item in tracks),
            "motion_ready": sum(
                item.lifecycle_state == "confirmed" and item.sample_count >= 2
                for item in tracks
            ),
        }

    def _recompute_ground_track_matches_locked(self) -> None:
        matches: dict[str, tuple[WorkerOnlyGroundTrackMatchCandidate, ...]] = {}
        reasons: dict[str, tuple[str, ...]] = {}
        evaluated_pairs = 0
        candidate_count = 0
        positioned_receipt_count = sum(
            receipt.fact.station.position_state == "known"
            and receipt.fact.station.latitude_decimal is not None
            and receipt.fact.station.longitude_decimal is not None
            for receipts in self._ground_receipts.values()
            for receipt in receipts
        )
        eligible_track_count = sum(
            track.lifecycle_state == "confirmed"
            and track.sample_count >= 2
            and state.last_frame is not None
            and track.latest_detection.valid_at == state.last_frame.valid_at
            and state.hierarchy(track.track_id).object_kind == "rain_shield"
            for state in self._states.values()
            for track in state.ledger.tracks
        )
        if eligible_track_count * positioned_receipt_count > MAX_GROUND_TRACK_PAIRS:
            self._ground_track_matches = {}
            self._ground_track_reasons = {
                "global": ("ground_track_match_capacity_exceeded",)
            }
            return
        for state_key, state in sorted(self._states.items()):
            radar_workload_id = state_key.split("|", 1)[0]
            for ground_workload_id, receipts in sorted(self._ground_receipts.items()):
                result = build_worker_only_ground_track_matches(
                    ground_workload_id=ground_workload_id,
                    radar_workload_id=radar_workload_id,
                    state=state,
                    receipts=receipts,
                )
                key = f"{state_key}|{ground_workload_id}"
                matches[key] = result.candidates
                reasons[key] = result.reason_codes
                evaluated_pairs += result.evaluated_pairs
                candidate_count += len(result.candidates)
        self._ground_track_matches = matches
        self._ground_track_reasons = reasons
        self._counts["ground_track_match_candidates"] += candidate_count
        self._counts["ground_track_match_evaluated_pairs"] += evaluated_pairs

    def process_batch(
        self,
        batch: WorkerOnlyTrackingBatch,
    ) -> WorkerOnlyTrackingUpdate:
        if not isinstance(batch, WorkerOnlyTrackingBatch):
            raise TypeError("tracking runtime vereist WorkerOnlyTrackingBatch")
        with self._lock:
            candidates = 0
            withheld = 0
            advanced = 0
            duplicates = 0
            resets = 0
            indeterminate = 0
            arrival_candidates = 0
            future_arrival_candidates = 0
            arrival_evaluated_pairs = 0
            for disposition in batch.dispositions:
                diagnostic_key = (
                    f"{disposition.workload_id}|{disposition.region_id}"
                )
                self._last_dispositions[diagnostic_key] = disposition.reason_code
                if disposition.candidate is None:
                    withheld += 1
                    self._counts["withheld"] += 1
                    continue
                candidates += 1
                candidate = disposition.candidate
                state_key = self._state_key(candidate)
                existing = self._states.get(state_key)
                grid_id = candidate.frame.grid.grid_id
                if existing is None:
                    if len(self._states) >= self._maximum_states:
                        raise WorkerOnlyCellTrackingError(
                            "tracking_state_capacity_exceeded"
                        )
                    state = create_cell_track_engine_state(
                        "worker-only-cell-engine:"
                        + _digest({"state_key": state_key}),
                        self._policy,
                    )
                    grid_reset = False
                elif self._state_grid_ids[state_key] != grid_id:
                    state = create_cell_track_engine_state(
                        "worker-only-cell-engine:"
                        + _digest({"grid_id": grid_id, "state_key": state_key}),
                        self._policy,
                    )
                    resets += 1
                    self._counts["resets"] += 1
                    grid_reset = True
                else:
                    state = existing
                    grid_reset = False
                if (
                    state.last_frame is not None
                    and candidate.frame.valid_at <= state.last_frame.valid_at
                ):
                    duplicates += 1
                    self._counts["duplicates"] += 1
                    continue
                transition = advance_cell_track_engine(
                    state,
                    candidate.frame,
                    self._policy,
                )
                if transition.status == "advanced":
                    if self._store is not None:
                        try:
                            self._store.save_head(
                                state_key=state_key,
                                state=transition.next_state,
                                expected_previous_revision=state.revision,
                                grid_reset=grid_reset,
                            )
                            self._store_error = None
                        except Exception as error:
                            self._store_error = "tracking_store_write_failed"
                            raise WorkerOnlyCellTrackingStoreError(
                                "tracking_store_write_failed"
                            ) from error
                    self._states[state_key] = transition.next_state
                    self._state_grid_ids[state_key] = grid_id
                    arrival = build_worker_only_shadow_arrivals(
                        workload_id=batch.workload_id,
                        candidate=candidate,
                        state=transition.next_state,
                        maximum_pairs=max(
                            0,
                            MAX_SHADOW_ARRIVAL_PIXEL_TARGET_PAIRS
                            - arrival_evaluated_pairs,
                        ),
                    )
                    self._shadow_arrivals[state_key] = arrival.candidates
                    self._shadow_arrival_reasons[state_key] = arrival.reason_codes
                    arrival_candidates += len(arrival.candidates)
                    future_arrival_candidates += sum(
                        item.passage_state == "future_intersection"
                        for item in arrival.candidates
                    )
                    arrival_evaluated_pairs += (
                        arrival.evaluated_pixel_target_pairs
                    )
                    self._counts["shadow_arrival_candidates"] += len(
                        arrival.candidates
                    )
                    self._counts["shadow_arrival_evaluated_pairs"] += (
                        arrival.evaluated_pixel_target_pairs
                    )
                    advanced += 1
                    self._counts["advanced"] += 1
                else:
                    indeterminate += 1
                    self._counts["indeterminate"] += 1
                    self._last_dispositions[diagnostic_key] = (
                        transition.reason_codes[0]
                        if transition.reason_codes
                        else "tracking_indeterminate"
                    )
            counts = self._track_counts()
            if advanced:
                self._recompute_ground_track_matches_locked()
            return WorkerOnlyTrackingUpdate(
                workload_id=batch.workload_id,
                candidate_regions=candidates,
                withheld_regions=withheld,
                advanced_regions=advanced,
                duplicate_regions=duplicates,
                reset_regions=resets,
                indeterminate_regions=indeterminate,
                active_tracks=counts["active"],
                tracked_tracks=counts["tracked"],
                preliminary_tracks=counts["preliminary"],
                coasting_tracks=counts["coasting"],
                motion_ready_tracks=counts["motion_ready"],
                shadow_arrival_candidates=arrival_candidates,
                shadow_future_arrival_candidates=future_arrival_candidates,
                shadow_arrival_evaluated_pairs=arrival_evaluated_pairs,
            )

    def process_source(
        self,
        *,
        workload_id: str,
        desired_state: DesiredState,
        source: VerifiedRadarFieldSource,
    ) -> WorkerOnlyTrackingUpdate:
        return self.process_batch(build_worker_only_tracking_batch(
            workload_id=workload_id,
            desired_state=desired_state,
            source=source,
        ))

    def process_ground_receipts(
        self,
        *,
        workload_id: str,
        receipts: tuple[GroundObservationReceipt, ...],
    ) -> int:
        """Replace one canonical ground head and rebuild unadmitted track matches."""

        _safe_id(workload_id, "workload_id")
        if (
            not isinstance(receipts, tuple)
            or len(receipts) > MAX_GROUND_TRACK_RECEIPTS
            or any(not isinstance(item, GroundObservationReceipt) for item in receipts)
        ):
            raise ValueError("ground-track receipts overschrijden het contract")
        with self._lock:
            self._ground_receipts[workload_id] = receipts
            self._recompute_ground_track_matches_locked()
            return sum(
                len(values) for values in self._ground_track_matches.values()
            )

    def diagnostics(self) -> dict[str, Any]:
        with self._lock:
            counts = self._track_counts()
            arrivals = tuple(
                item
                for values in self._shadow_arrivals.values()
                for item in values
            )
            ground_matches = tuple(
                item
                for values in self._ground_track_matches.values()
                for item in values
            )
            return {
                "admission_status": ADMISSION_STATUS,
                "active_tracks": counts["active"],
                "contract": CONTRACT,
                "coasting_tracks": counts["coasting"],
                "counts": dict(self._counts),
                "dispositions": dict(sorted(self._last_dispositions.items())),
                "forecast_generated": False,
                "ground_track_match_admission_status": ADMISSION_STATUS,
                "ground_track_match_candidate_count": len(ground_matches),
                "ground_track_match_reasons": {
                    key: list(value)
                    for key, value in sorted(self._ground_track_reasons.items())
                },
                "motion_ready_tracks": counts["motion_ready"],
                "preliminary_tracks": counts["preliminary"],
                "region_states": len(self._states),
                "restart_persistent": (
                    self._store is not None and self._store_error is None
                ),
                "shadow_arrival_admission_status": ADMISSION_STATUS,
                "shadow_arrival_candidate_count": len(arrivals),
                "shadow_arrival_future_count": sum(
                    item.passage_state == "future_intersection"
                    for item in arrivals
                ),
                "shadow_arrival_reasons": {
                    key: list(value)
                    for key, value in sorted(self._shadow_arrival_reasons.items())
                },
                "store_error": self._store_error,
                "store_state_bytes": (
                    0
                    if self._store is None
                    else self._store.diagnostics()["compressed_state_bytes"]
                ),
                "tracked_tracks": counts["tracked"],
            }

    def ground_track_matches(
        self,
    ) -> tuple[WorkerOnlyGroundTrackMatchCandidate, ...]:
        """Return geometric same-track candidates without semantic admission."""

        with self._lock:
            return tuple(sorted(
                (
                    item
                    for values in self._ground_track_matches.values()
                    for item in values
                ),
                key=lambda item: (
                    item.track_id,
                    item.observation_representative_at,
                    item.receipt_sha256,
                ),
            ))

    def shadow_arrivals(self) -> tuple[WorkerOnlyShadowArrivalCandidate, ...]:
        """Return immutable unadmitted candidates for replay/live comparison."""

        with self._lock:
            return tuple(sorted(
                (
                    item
                    for values in self._shadow_arrivals.values()
                    for item in values
                ),
                key=lambda item: (
                    item.target_id,
                    item.entry_seconds,
                    item.track_id,
                ),
            ))


__all__ = [
    "ADMISSION_STATUS",
    "CONTRACT",
    "MAX_ROI_PIXELS",
    "MAX_GROUND_TRACK_PAIRS",
    "MAX_GROUND_TRACK_RECEIPTS",
    "MAX_SHADOW_ARRIVAL_PIXEL_TARGET_PAIRS",
    "SHADOW_ARRIVAL_HORIZON_SECONDS",
    "SHADOW_ARRIVAL_METHOD",
    "GROUND_TRACK_MATCH_METHOD",
    "TRACKABLE_WORKLOAD_IDS",
    "WorkerOnlyCellTrackingError",
    "WorkerOnlyCellTrackingRuntime",
    "WorkerOnlyCellTrackingStore",
    "WorkerOnlyCellTrackingStoreError",
    "WorkerOnlyProjectedTarget",
    "WorkerOnlyGroundTrackMatchCandidate",
    "WorkerOnlyGroundTrackMatchResult",
    "WorkerOnlyShadowArrivalCandidate",
    "WorkerOnlyShadowArrivalResult",
    "WorkerOnlyTrackingBatch",
    "WorkerOnlyTrackingDisposition",
    "WorkerOnlyTrackingFrameCandidate",
    "WorkerOnlyTrackingUpdate",
    "build_worker_only_tracking_batch",
    "build_worker_only_ground_track_matches",
    "build_worker_only_shadow_arrivals",
    "worker_only_tracking_policy",
]
