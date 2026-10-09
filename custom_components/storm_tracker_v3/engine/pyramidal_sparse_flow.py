"""Production-bounded pure-NumPy sparse pyramidal Lucas-Kanade challenger.

The scientific shape follows the sparse optical-flow route documented by
pySTEPS: deterministic Shi-Tomasi corners, coarse-to-fine iterative
Lucas-Kanade tracking, sparse vectors and object-level aggregation.  pySTEPS
uses OpenCV for that route; this isolated challenger intentionally depends on
NumPy only and makes no skill claim without held-out meteorological hindcasts.
"""
from __future__ import annotations

import hashlib
import json
import math
from collections import OrderedDict
from dataclasses import dataclass
from typing import Callable, Iterable

import numpy as np

from .hierarchical_detection import (
    HierarchicalCellDetection,
    NormalizedRasterFrame,
)
from .motion_prediction import Covariance2D, OpticalFlowSample


SPARSE_PYRAMIDAL_FLOW_VERSION = "stv3-numpy-sparse-pyramidal-lk-v1"
MAX_FEATURES_HARD_LIMIT = 4_096
MAX_INPUT_PIXELS_HARD_LIMIT = 262_144
MAX_PYRAMID_LEVELS_HARD_LIMIT = 8
MAX_WINDOW_RADIUS_HARD_LIMIT = 16
MAX_ITERATIONS_HARD_LIMIT = 50


@dataclass(frozen=True, slots=True)
class SparsePyramidalConfig:
    maximum_input_pixels: int
    max_features: int
    minimum_feature_distance_pixels: float
    feature_window_radius_pixels: int
    minimum_feature_valid_fraction: float
    minimum_feature_eigenvalue: float
    pyramid_levels: int
    minimum_pyramid_valid_fraction: float
    tracking_window_radius_pixels: int
    minimum_tracking_samples: int
    maximum_iterations: int
    convergence_threshold_pixels: float
    minimum_tracking_eigenvalue: float
    maximum_condition_number: float
    regularization: float
    residual_variance_floor: float
    velocity_process_noise_covariance_m2_s2: Covariance2D

    def __post_init__(self) -> None:
        _bounded_positive_integer(
            self.maximum_input_pixels,
            "maximum_input_pixels",
            MAX_INPUT_PIXELS_HARD_LIMIT,
        )
        _bounded_positive_integer(
            self.max_features,
            "max_features",
            MAX_FEATURES_HARD_LIMIT,
        )
        _bounded_positive_integer(
            self.pyramid_levels,
            "pyramid_levels",
            MAX_PYRAMID_LEVELS_HARD_LIMIT,
        )
        _bounded_positive_integer(
            self.feature_window_radius_pixels,
            "feature_window_radius_pixels",
            MAX_WINDOW_RADIUS_HARD_LIMIT,
        )
        _bounded_positive_integer(
            self.tracking_window_radius_pixels,
            "tracking_window_radius_pixels",
            MAX_WINDOW_RADIUS_HARD_LIMIT,
        )
        _bounded_positive_integer(
            self.maximum_iterations,
            "maximum_iterations",
            MAX_ITERATIONS_HARD_LIMIT,
        )
        if (
            isinstance(self.minimum_tracking_samples, bool)
            or not isinstance(self.minimum_tracking_samples, int)
            or self.minimum_tracking_samples < 2
        ):
            raise ValueError("minimum_tracking_samples moet minstens twee zijn")
        for label, value in (
            (
                "minimum_feature_distance_pixels",
                self.minimum_feature_distance_pixels,
            ),
            (
                "minimum_feature_valid_fraction",
                self.minimum_feature_valid_fraction,
            ),
            (
                "minimum_feature_eigenvalue",
                self.minimum_feature_eigenvalue,
            ),
            (
                "minimum_pyramid_valid_fraction",
                self.minimum_pyramid_valid_fraction,
            ),
            (
                "convergence_threshold_pixels",
                self.convergence_threshold_pixels,
            ),
            (
                "minimum_tracking_eigenvalue",
                self.minimum_tracking_eigenvalue,
            ),
            ("maximum_condition_number", self.maximum_condition_number),
            ("regularization", self.regularization),
            ("residual_variance_floor", self.residual_variance_floor),
        ):
            if not math.isfinite(value):
                raise ValueError(f"{label} moet eindig zijn")
        if self.minimum_feature_distance_pixels < 0.0:
            raise ValueError("minimum_feature_distance_pixels mag niet negatief zijn")
        for label, value in (
            (
                "minimum_feature_valid_fraction",
                self.minimum_feature_valid_fraction,
            ),
            (
                "minimum_pyramid_valid_fraction",
                self.minimum_pyramid_valid_fraction,
            ),
        ):
            if value <= 0.0 or value > 1.0:
                raise ValueError(f"{label} moet in ]0,1] liggen")
        if (
            self.minimum_feature_eigenvalue < 0.0
            or self.minimum_tracking_eigenvalue < 0.0
            or self.regularization < 0.0
            or self.residual_variance_floor < 0.0
        ):
            raise ValueError("eigenwaarden/regularisatie/residualvloer zijn niet-negatief")
        if self.convergence_threshold_pixels <= 0.0:
            raise ValueError("convergence_threshold_pixels moet positief zijn")
        if self.maximum_condition_number < 1.0:
            raise ValueError("maximum_condition_number moet minstens één zijn")
        _validate_covariance2(
            self.velocity_process_noise_covariance_m2_s2,
            "velocity_process_noise_covariance_m2_s2",
        )

    @property
    def fingerprint(self) -> str:
        return _sha256(
            {
                "algorithm": SPARSE_PYRAMIDAL_FLOW_VERSION,
                **{
                    name: (
                        _covariance_token(value)
                        if name
                        == "velocity_process_noise_covariance_m2_s2"
                        else (
                            _float_token(value)
                            if isinstance(value, float)
                            else value
                        )
                    )
                    for name, value in (
                        (
                            "maximum_input_pixels",
                            self.maximum_input_pixels,
                        ),
                        ("max_features", self.max_features),
                        (
                            "minimum_feature_distance_pixels",
                            self.minimum_feature_distance_pixels,
                        ),
                        (
                            "feature_window_radius_pixels",
                            self.feature_window_radius_pixels,
                        ),
                        (
                            "minimum_feature_valid_fraction",
                            self.minimum_feature_valid_fraction,
                        ),
                        (
                            "minimum_feature_eigenvalue",
                            self.minimum_feature_eigenvalue,
                        ),
                        ("pyramid_levels", self.pyramid_levels),
                        (
                            "minimum_pyramid_valid_fraction",
                            self.minimum_pyramid_valid_fraction,
                        ),
                        (
                            "tracking_window_radius_pixels",
                            self.tracking_window_radius_pixels,
                        ),
                        (
                            "minimum_tracking_samples",
                            self.minimum_tracking_samples,
                        ),
                        ("maximum_iterations", self.maximum_iterations),
                        (
                            "convergence_threshold_pixels",
                            self.convergence_threshold_pixels,
                        ),
                        (
                            "minimum_tracking_eigenvalue",
                            self.minimum_tracking_eigenvalue,
                        ),
                        (
                            "maximum_condition_number",
                            self.maximum_condition_number,
                        ),
                        ("regularization", self.regularization),
                        (
                            "residual_variance_floor",
                            self.residual_variance_floor,
                        ),
                        (
                            "velocity_process_noise_covariance_m2_s2",
                            self.velocity_process_noise_covariance_m2_s2,
                        ),
                    )
                },
            }
        )


@dataclass(frozen=True, slots=True)
class FeatureSelectionAudit:
    candidate_count: int
    selected_count: int
    pyramid_margin_suppressed_count: int
    distance_suppressed_count: int
    budget_dropped_count: int
    max_features: int
    candidate_coverage_fraction: float

    def __post_init__(self) -> None:
        if (
            self.selected_count > self.max_features
            or self.candidate_count
            != self.selected_count
            + self.pyramid_margin_suppressed_count
            + self.distance_suppressed_count
            + self.budget_dropped_count
        ):
            raise ValueError("Shi-Tomasi featureaudit is intern inconsistent")


@dataclass(frozen=True, slots=True)
class SparseFlowVector:
    feature_id: str
    selected_rank: int
    start_row: int
    start_column: int
    feature_quality: float
    status: str
    displacement_column_pixels: float | None
    displacement_row_pixels: float | None
    vx_m_s: float | None
    vy_m_s: float | None
    levels_completed: int
    iterations: int
    converged: bool
    forward_residual_rms: float | None
    structure_min_eigenvalue: float | None
    structure_max_eigenvalue: float | None
    condition_number: float | None
    structure_quality: float | None
    velocity_covariance_m2_s2: Covariance2D | None
    failure_level: int | None
    failure_reason: str | None

    @property
    def usable(self) -> bool:
        return self.status in ("available_converged", "available_iteration_limit")


@dataclass(frozen=True, slots=True)
class SparseFlowPairResult:
    previous_frame_id: str
    current_frame_id: str
    previous_valid_at: float
    current_valid_at: float
    elapsed_seconds: float
    analysis_grid_id: str
    quantity: str
    unit: str
    config_fingerprint: str
    feature_audit: FeatureSelectionAudit
    vectors: tuple[SparseFlowVector, ...]
    checksum: str
    algorithm_version: str = SPARSE_PYRAMIDAL_FLOW_VERSION

    @property
    def status_counts(self) -> tuple[tuple[str, int], ...]:
        counts: dict[str, int] = {}
        for vector in self.vectors:
            counts[vector.status] = counts.get(vector.status, 0) + 1
        return tuple(sorted(counts.items()))


@dataclass(frozen=True, slots=True)
class SparseFlowSequence:
    frame_ids: tuple[str, ...]
    pairs: tuple[SparseFlowPairResult, ...]
    config_fingerprint: str
    checksum: str
    algorithm_version: str = SPARSE_PYRAMIDAL_FLOW_VERSION

    @property
    def latest(self) -> SparseFlowPairResult:
        return self.pairs[-1]

    def pair_for_current_frame(self, frame_id: str) -> SparseFlowPairResult:
        matches = tuple(
            item for item in self.pairs if item.current_frame_id == frame_id
        )
        if len(matches) != 1:
            raise KeyError(frame_id)
        return matches[0]


@dataclass(frozen=True, slots=True)
class SparseDetectionAggregationConfig:
    minimum_usable_vectors: int
    minimum_usable_fraction: float
    include_iteration_limit: bool
    residual_weight_floor: float
    velocity_covariance_floor_m2_s2: Covariance2D
    residual_position_covariance_m2: Covariance2D
    method: str

    def __post_init__(self) -> None:
        _positive_integer(
            self.minimum_usable_vectors,
            "minimum_usable_vectors",
        )
        if (
            not math.isfinite(self.minimum_usable_fraction)
            or self.minimum_usable_fraction <= 0.0
            or self.minimum_usable_fraction > 1.0
        ):
            raise ValueError("minimum_usable_fraction moet in ]0,1] liggen")
        if not isinstance(self.include_iteration_limit, bool):
            raise TypeError("include_iteration_limit moet bool zijn")
        if (
            not math.isfinite(self.residual_weight_floor)
            or self.residual_weight_floor <= 0.0
        ):
            raise ValueError("residual_weight_floor moet positief en eindig zijn")
        _validate_covariance2(
            self.velocity_covariance_floor_m2_s2,
            "velocity_covariance_floor_m2_s2",
        )
        _validate_covariance2(
            self.residual_position_covariance_m2,
            "residual_position_covariance_m2",
        )
        if self.method not in (
            "conservative_mean",
            "inverse_residual_weighted_mean",
        ):
            raise ValueError("onbekende sparse-flowaggregatiemethode")

    @property
    def fingerprint(self) -> str:
        return _sha256(
            {
                "minimum_usable_vectors": self.minimum_usable_vectors,
                "minimum_usable_fraction": _float_token(
                    self.minimum_usable_fraction
                ),
                "include_iteration_limit": self.include_iteration_limit,
                "residual_weight_floor": _float_token(
                    self.residual_weight_floor
                ),
                "velocity_covariance_floor_m2_s2": _covariance_token(
                    self.velocity_covariance_floor_m2_s2
                ),
                "residual_position_covariance_m2": _covariance_token(
                    self.residual_position_covariance_m2
                ),
                "method": self.method,
            }
        )


@dataclass(frozen=True, slots=True)
class SparseDetectionFlowAggregation:
    status: str
    detection_id: str
    global_candidate_features: int
    global_selected_features: int
    selected_vectors_in_footprint: int
    selected_vector_coverage_fraction: float
    usable_vectors: int
    usable_fraction: float
    vector_status_counts: tuple[tuple[str, int], ...]
    aggregation_method: str
    minimum_vector_weight: float | None
    maximum_vector_weight: float | None
    mean_forward_residual_rms: float | None
    sample: OpticalFlowSample | None

    @property
    def available(self) -> bool:
        return self.status == "available"


def estimate_sparse_pyramidal_motion(
    frames: Iterable[NormalizedRasterFrame],
    config: SparsePyramidalConfig,
) -> SparseFlowSequence:
    """Estimate every consecutive pair for two or more compatible frames."""

    values = tuple(frames)
    if len(values) < 2:
        raise ValueError("sparse pyramidal motion vereist minstens twee frames")
    if any(not isinstance(item, NormalizedRasterFrame) for item in values):
        raise TypeError("alle motionframes moeten NormalizedRasterFrame zijn")
    if not isinstance(config, SparsePyramidalConfig):
        raise TypeError("motionservice vereist SparsePyramidalConfig")
    _validate_sequence(values, config)
    pairs = tuple(
        _estimate_pair(previous, current, config)
        for previous, current in zip(values, values[1:])
    )
    checksum = _sha256(
        {
            "algorithm": SPARSE_PYRAMIDAL_FLOW_VERSION,
            "config": config.fingerprint,
            "frame_ids": [item.frame_id for item in values],
            "pair_checksums": [item.checksum for item in pairs],
        }
    )
    return SparseFlowSequence(
        frame_ids=tuple(item.frame_id for item in values),
        pairs=pairs,
        config_fingerprint=config.fingerprint,
        checksum=checksum,
    )


def estimate_latest_sparse_flow(
    frames: Iterable[NormalizedRasterFrame],
    config: SparsePyramidalConfig,
) -> SparseFlowPairResult:
    """Replay-friendly helper returning the latest consecutive pair."""

    return estimate_sparse_pyramidal_motion(frames, config).latest


def aggregate_sparse_detection_flow(
    pair: SparseFlowPairResult,
    detection: HierarchicalCellDetection,
    config: SparseDetectionAggregationConfig,
) -> SparseDetectionFlowAggregation:
    """Aggregate start vectors inside a rain object or return unavailable."""

    if not isinstance(pair, SparseFlowPairResult):
        raise TypeError("aggregatie vereist SparseFlowPairResult")
    if not isinstance(detection, HierarchicalCellDetection):
        raise TypeError("aggregatie vereist HierarchicalCellDetection")
    if not isinstance(config, SparseDetectionAggregationConfig):
        raise TypeError("aggregatie vereist SparseDetectionAggregationConfig")
    if detection.detection.analysis_grid_id != pair.analysis_grid_id:
        raise ValueError("detectie en sparse flow gebruiken verschillende grids")
    if not math.isclose(
        detection.detection.valid_at,
        pair.previous_valid_at,
        rel_tol=0.0,
        abs_tol=1e-6,
    ):
        raise ValueError("detectie hoort niet bij het vorige sparse-flowframe")
    footprint = frozenset(detection.footprint_pixels)
    selected = tuple(
        item
        for item in pair.vectors
        if (item.start_row, item.start_column) in footprint
    )
    allowed_statuses = {"available_converged"}
    if config.include_iteration_limit:
        allowed_statuses.add("available_iteration_limit")
    usable = tuple(item for item in selected if item.status in allowed_statuses)
    fraction = len(usable) / len(selected) if selected else 0.0
    status_counts: dict[str, int] = {}
    for item in selected:
        status_counts[item.status] = status_counts.get(item.status, 0) + 1
    common = {
        "detection_id": detection.detection.detection_id,
        "global_candidate_features": pair.feature_audit.candidate_count,
        "global_selected_features": pair.feature_audit.selected_count,
        "selected_vectors_in_footprint": len(selected),
        "selected_vector_coverage_fraction": (
            len(selected) / pair.feature_audit.selected_count
            if pair.feature_audit.selected_count
            else 0.0
        ),
        "usable_vectors": len(usable),
        "usable_fraction": fraction,
        "vector_status_counts": tuple(sorted(status_counts.items())),
        "aggregation_method": config.method,
    }
    if not selected:
        return SparseDetectionFlowAggregation(
            status="unavailable_no_selected_features",
            minimum_vector_weight=None,
            maximum_vector_weight=None,
            mean_forward_residual_rms=None,
            sample=None,
            **common,
        )
    residual_mean = (
        sum(float(item.forward_residual_rms) for item in usable) / len(usable)
        if usable
        else None
    )
    if config.method == "inverse_residual_weighted_mean" and usable:
        weights = np.asarray(
            [
                1.0
                / max(
                    float(item.forward_residual_rms),
                    config.residual_weight_floor,
                )
                ** 2
                for item in usable
            ],
            dtype=np.float64,
        )
    else:
        weights = np.ones(len(usable), dtype=np.float64)
    minimum_weight = float(np.min(weights)) if len(weights) else None
    maximum_weight = float(np.max(weights)) if len(weights) else None
    if (
        len(usable) < config.minimum_usable_vectors
        or fraction < config.minimum_usable_fraction
    ):
        return SparseDetectionFlowAggregation(
            status="unavailable_insufficient_usable_vectors",
            minimum_vector_weight=minimum_weight,
            maximum_vector_weight=maximum_weight,
            mean_forward_residual_rms=residual_mean,
            sample=None,
            **common,
        )
    vectors = np.asarray(
        [(float(item.vx_m_s), float(item.vy_m_s)) for item in usable],
        dtype=np.float64,
    )
    weight_sum = float(np.sum(weights))
    mean_vector = np.sum(vectors * weights[:, None], axis=0) / weight_sum
    local_covariances = np.asarray(
        [item.velocity_covariance_m2_s2 for item in usable],
        dtype=np.float64,
    )
    mean_local_covariance = np.sum(
        local_covariances * weights[:, None, None],
        axis=0,
    ) / weight_sum
    deviations = vectors - mean_vector
    spread = np.einsum(
        "n,ni,nj->ij",
        weights,
        deviations,
        deviations,
    ) / weight_sum
    covariance = (
        mean_local_covariance
        + spread
        + np.asarray(
            config.velocity_covariance_floor_m2_s2,
            dtype=np.float64,
        )
    )
    sample_id = "sparse-lk:" + hashlib.sha256(
        (
            f"{pair.checksum}\0{detection.detection.detection_id}\0"
            f"{config.fingerprint}"
        ).encode("utf-8")
    ).hexdigest()
    sample = OpticalFlowSample(
        sample_id=sample_id,
        analysis_grid_id=pair.analysis_grid_id,
        reference_valid_at=pair.previous_valid_at,
        vx_m_s=float(mean_vector[0]),
        vy_m_s=float(mean_vector[1]),
        velocity_covariance_m2_s2=_array_covariance(covariance),
        residual_covariance_m2=config.residual_position_covariance_m2,
        method=f"numpy_sparse_pyramidal_lk_{config.method}_v1",
        support_frame_ids=(
            pair.previous_frame_id,
            pair.current_frame_id,
        ),
    )
    return SparseDetectionFlowAggregation(
        status="available",
        minimum_vector_weight=minimum_weight,
        maximum_vector_weight=maximum_weight,
        mean_forward_residual_rms=residual_mean,
        sample=sample,
        **common,
    )


@dataclass(frozen=True, slots=True)
class SparseReplayFlowSampleFactory:
    """Callable adapter for the existing replay flow-sample extension."""

    sequence: SparseFlowSequence
    aggregation_config: SparseDetectionAggregationConfig
    unavailable_factory: Callable[
        [HierarchicalCellDetection, object, SparseDetectionFlowAggregation],
        OpticalFlowSample,
    ]

    def __call__(
        self,
        detection: HierarchicalCellDetection,
        current_frame: object,
    ) -> OpticalFlowSample:
        frame_id = getattr(current_frame, "frame_id", None)
        if not isinstance(frame_id, str) or not frame_id:
            raise TypeError("replay current_frame vereist frame_id")
        pair = self.sequence.pair_for_current_frame(frame_id)
        aggregation = aggregate_sparse_detection_flow(
            pair,
            detection,
            self.aggregation_config,
        )
        if aggregation.sample is not None:
            return aggregation.sample
        fallback = self.unavailable_factory(
            detection,
            current_frame,
            aggregation,
        )
        if not isinstance(fallback, OpticalFlowSample):
            raise TypeError("unavailable_factory moet OpticalFlowSample leveren")
        return fallback


class SparsePyramidalFlowCache:
    def __init__(self, maximum_entries: int) -> None:
        _positive_integer(maximum_entries, "maximum_entries")
        self._maximum_entries = maximum_entries
        self._values: OrderedDict[
            tuple[tuple[str, ...], str, str],
            tuple[str, SparseFlowSequence],
        ] = OrderedDict()
        self.hits = 0
        self.misses = 0

    def estimate(
        self,
        frames: Iterable[NormalizedRasterFrame],
        config: SparsePyramidalConfig,
    ) -> SparseFlowSequence:
        values = tuple(frames)
        key = (
            tuple(item.frame_id for item in values),
            SPARSE_PYRAMIDAL_FLOW_VERSION,
            config.fingerprint,
        )
        input_checksum = _input_checksum(values)
        cached = self._values.get(key)
        if cached is not None:
            if cached[0] != input_checksum:
                raise ValueError("sparse-flow cachekey kreeg gewijzigde rasterbytes")
            self._values.move_to_end(key)
            self.hits += 1
            return cached[1]
        result = estimate_sparse_pyramidal_motion(values, config)
        self._values[key] = (input_checksum, result)
        self._values.move_to_end(key)
        while len(self._values) > self._maximum_entries:
            self._values.popitem(last=False)
        self.misses += 1
        return result


def _estimate_pair(
    previous: NormalizedRasterFrame,
    current: NormalizedRasterFrame,
    config: SparsePyramidalConfig,
) -> SparseFlowPairResult:
    previous_array = _frame_array(previous)
    current_array = _frame_array(current)
    previous_pyramid = _build_pyramid(previous_array, config)
    current_pyramid = _build_pyramid(current_array, config)
    candidates, audit = _select_features(
        previous_array,
        config,
        actual_pyramid_levels=len(previous_pyramid),
    )
    elapsed = current.valid_at - previous.valid_at
    vectors = tuple(
        _track_feature(
            row,
            column,
            quality,
            rank,
            previous_pyramid,
            current_pyramid,
            previous,
            current,
            elapsed,
            config,
        )
        for rank, (row, column, quality) in enumerate(candidates, start=1)
    )
    checksum = _sha256(
        {
            "algorithm": SPARSE_PYRAMIDAL_FLOW_VERSION,
            "input": _input_checksum((previous, current)),
            "config": config.fingerprint,
            "audit": audit.__dict__
            if hasattr(audit, "__dict__")
            else {
                "candidate_count": audit.candidate_count,
                "selected_count": audit.selected_count,
                "pyramid_margin_suppressed_count": (
                    audit.pyramid_margin_suppressed_count
                ),
                "distance_suppressed_count": audit.distance_suppressed_count,
                "budget_dropped_count": audit.budget_dropped_count,
                "max_features": audit.max_features,
                "candidate_coverage_fraction": _float_token(
                    audit.candidate_coverage_fraction
                ),
            },
            "vectors": [_vector_token(item) for item in vectors],
        }
    )
    return SparseFlowPairResult(
        previous_frame_id=previous.frame_id,
        current_frame_id=current.frame_id,
        previous_valid_at=previous.valid_at,
        current_valid_at=current.valid_at,
        elapsed_seconds=elapsed,
        analysis_grid_id=previous.grid.grid_id,
        quantity=previous.quantity,
        unit=previous.unit,
        config_fingerprint=config.fingerprint,
        feature_audit=audit,
        vectors=vectors,
        checksum=checksum,
    )


def _select_features(
    image: np.ndarray,
    config: SparsePyramidalConfig,
    *,
    actual_pyramid_levels: int,
) -> tuple[tuple[tuple[int, int, float], ...], FeatureSelectionAudit]:
    gradient_x, gradient_y, gradient_valid = _gradients(image)
    radius = config.feature_window_radius_pixels
    area = (2 * radius + 1) ** 2
    minimum_samples = math.ceil(area * config.minimum_feature_valid_fraction)
    candidates: list[tuple[int, int, float]] = []
    height, width = image.shape
    for row in range(radius + 1, height - radius - 1):
        for column in range(radius + 1, width - radius - 1):
            valid = gradient_valid[
                row - radius : row + radius + 1,
                column - radius : column + radius + 1,
            ]
            if int(np.count_nonzero(valid)) < minimum_samples:
                continue
            gx = gradient_x[
                row - radius : row + radius + 1,
                column - radius : column + radius + 1,
            ][valid]
            gy = gradient_y[
                row - radius : row + radius + 1,
                column - radius : column + radius + 1,
            ][valid]
            structure = np.asarray(
                ((gx @ gx, gx @ gy), (gx @ gy, gy @ gy)),
                dtype=np.float64,
            )
            quality = float(np.linalg.eigvalsh(structure)[0])
            if quality >= config.minimum_feature_eigenvalue:
                candidates.append((row, column, quality))
    candidates.sort(key=lambda item: (-item[2], item[0], item[1]))
    selected: list[tuple[int, int, float]] = []
    pyramid_margin_suppressed = 0
    distance_suppressed = 0
    budget_dropped = 0
    minimum_distance_squared = config.minimum_feature_distance_pixels**2
    coarsest_scale = 2 ** (actual_pyramid_levels - 1)
    coarsest_height = math.ceil(image.shape[0] / coarsest_scale)
    coarsest_width = math.ceil(image.shape[1] / coarsest_scale)
    tracking_margin = config.tracking_window_radius_pixels
    for candidate in candidates:
        row, column, _quality = candidate
        coarse_x = (column + 0.5) / coarsest_scale - 0.5
        coarse_y = (row + 0.5) / coarsest_scale - 0.5
        if (
            coarse_x < tracking_margin
            or coarse_y < tracking_margin
            or coarse_x > coarsest_width - tracking_margin - 1
            or coarse_y > coarsest_height - tracking_margin - 1
        ):
            pyramid_margin_suppressed += 1
        elif any(
            (row - selected_row) ** 2 + (column - selected_column) ** 2
            < minimum_distance_squared
            for selected_row, selected_column, _value in selected
        ):
            distance_suppressed += 1
        elif len(selected) >= config.max_features:
            budget_dropped += 1
        else:
            selected.append(candidate)
    finite_pixels = int(np.count_nonzero(np.isfinite(image)))
    audit = FeatureSelectionAudit(
        candidate_count=len(candidates),
        selected_count=len(selected),
        pyramid_margin_suppressed_count=pyramid_margin_suppressed,
        distance_suppressed_count=distance_suppressed,
        budget_dropped_count=budget_dropped,
        max_features=config.max_features,
        candidate_coverage_fraction=(
            len(candidates) / finite_pixels if finite_pixels else 0.0
        ),
    )
    return tuple(selected), audit


def _track_feature(
    row: int,
    column: int,
    feature_quality: float,
    rank: int,
    previous_pyramid: tuple[np.ndarray, ...],
    current_pyramid: tuple[np.ndarray, ...],
    previous_frame: NormalizedRasterFrame,
    current_frame: NormalizedRasterFrame,
    elapsed: float,
    config: SparsePyramidalConfig,
) -> SparseFlowVector:
    feature_id = _feature_id(
        previous_frame,
        current_frame,
        row,
        column,
        rank,
        config,
    )
    displacement = np.zeros(2, dtype=np.float64)
    total_iterations = 0
    levels_completed = 0
    hit_iteration_limit = False
    final_metrics: tuple[
        float,
        float,
        float,
        float,
        np.ndarray,
    ] | None = None
    for level in range(len(previous_pyramid) - 1, -1, -1):
        if level < len(previous_pyramid) - 1:
            displacement *= 2.0
        scale = 2**level
        point = np.asarray(
            (
                (column + 0.5) / scale - 0.5,
                (row + 0.5) / scale - 0.5,
            ),
            dtype=np.float64,
        )
        outcome = _track_at_level(
            previous_pyramid[level],
            current_pyramid[level],
            point,
            displacement,
            config,
        )
        total_iterations += outcome["iterations"]
        if outcome["status"] != "available":
            partial = displacement * scale
            return _failed_vector(
                row=row,
                column=column,
                feature_id=feature_id,
                rank=rank,
                feature_quality=feature_quality,
                status=outcome["status"],
                displacement=partial,
                levels_completed=levels_completed,
                iterations=total_iterations,
                failure_level=level,
                reason=outcome["reason"],
                metrics=outcome["metrics"],
            )
        displacement = outcome["displacement"]
        levels_completed += 1
        hit_iteration_limit = (
            hit_iteration_limit or not outcome["converged"]
        )
        final_metrics = outcome["metrics"]
    if final_metrics is None:
        raise AssertionError("pyramidal tracker leverde geen finest-level metrics")
    residual_rms, minimum_eigen, maximum_eigen, condition, pixel_covariance = (
        final_metrics
    )
    metric_transform = np.asarray(
        (
            (previous_frame.grid.x_resolution_m / elapsed, 0.0),
            (0.0, -previous_frame.grid.y_resolution_m / elapsed),
        ),
        dtype=np.float64,
    )
    velocity_covariance = (
        metric_transform @ pixel_covariance @ metric_transform.T
        + np.asarray(
            config.velocity_process_noise_covariance_m2_s2,
            dtype=np.float64,
        )
    )
    vx_m_s = (
        float(displacement[0])
        * previous_frame.grid.x_resolution_m
        / elapsed
    )
    vy_m_s = (
        -float(displacement[1])
        * previous_frame.grid.y_resolution_m
        / elapsed
    )
    return SparseFlowVector(
        feature_id=feature_id,
        selected_rank=rank,
        start_row=row,
        start_column=column,
        feature_quality=feature_quality,
        status=(
            "available_iteration_limit"
            if hit_iteration_limit
            else "available_converged"
        ),
        displacement_column_pixels=float(displacement[0]),
        displacement_row_pixels=float(displacement[1]),
        vx_m_s=vx_m_s,
        vy_m_s=vy_m_s,
        levels_completed=levels_completed,
        iterations=total_iterations,
        converged=not hit_iteration_limit,
        forward_residual_rms=residual_rms,
        structure_min_eigenvalue=minimum_eigen,
        structure_max_eigenvalue=maximum_eigen,
        condition_number=condition,
        structure_quality=(
            minimum_eigen / maximum_eigen if maximum_eigen > 0.0 else 0.0
        ),
        velocity_covariance_m2_s2=_array_covariance(
            velocity_covariance
        ),
        failure_level=None,
        failure_reason=None,
    )


def _track_at_level(
    previous: np.ndarray,
    current: np.ndarray,
    point: np.ndarray,
    initial_displacement: np.ndarray,
    config: SparsePyramidalConfig,
) -> dict[str, object]:
    radius = config.tracking_window_radius_pixels
    offsets_y, offsets_x = np.mgrid[-radius : radius + 1, -radius : radius + 1]
    offsets_x = offsets_x.astype(np.float64).ravel()
    offsets_y = offsets_y.astype(np.float64).ravel()
    template, template_valid = _bilinear_sample(
        previous,
        point[0] + offsets_x,
        point[1] + offsets_y,
    )
    gradient_x, gradient_y, _valid = _gradients(current)
    displacement = initial_displacement.astype(np.float64, copy=True)
    last_metrics = None
    for iteration in range(1, config.maximum_iterations + 1):
        sample_x = point[0] + offsets_x + displacement[0]
        sample_y = point[1] + offsets_y + displacement[1]
        warped, warped_valid = _bilinear_sample(current, sample_x, sample_y)
        gx, gx_valid = _bilinear_sample(gradient_x, sample_x, sample_y)
        gy, gy_valid = _bilinear_sample(gradient_y, sample_x, sample_y)
        valid = template_valid & warped_valid & gx_valid & gy_valid
        sample_count = int(np.count_nonzero(valid))
        if sample_count < config.minimum_tracking_samples:
            return {
                "status": "insufficient_support",
                "reason": "tracking_window_has_too_few_valid_samples",
                "iterations": iteration - 1,
                "metrics": last_metrics,
            }
        design = np.column_stack((gx[valid], gy[valid]))
        residual = warped[valid] - template[valid]
        structure = design.T @ design
        try:
            eigenvalues = np.linalg.eigvalsh(structure)
        except np.linalg.LinAlgError:
            return {
                "status": "numerical_failure",
                "reason": "tracking_eigendecomposition_failed",
                "iterations": iteration - 1,
                "metrics": last_metrics,
            }
        minimum_eigen = float(eigenvalues[0])
        maximum_eigen = float(eigenvalues[1])
        condition = (
            math.inf
            if minimum_eigen <= 0.0
            else maximum_eigen / minimum_eigen
        )
        if (
            minimum_eigen < config.minimum_tracking_eigenvalue
            or condition > config.maximum_condition_number
        ):
            return {
                "status": "ill_conditioned",
                "reason": "tracking_structure_tensor_failed_conditioning",
                "iterations": iteration - 1,
                "metrics": (
                    math.sqrt(float(residual @ residual) / sample_count),
                    minimum_eigen,
                    maximum_eigen,
                    condition,
                    np.zeros((2, 2), dtype=np.float64),
                ),
            }
        normal = structure + np.eye(2) * config.regularization
        try:
            delta = np.linalg.solve(normal, -(design.T @ residual))
        except np.linalg.LinAlgError:
            return {
                "status": "numerical_failure",
                "reason": "tracking_linear_solve_failed",
                "iterations": iteration - 1,
                "metrics": last_metrics,
            }
        if not np.all(np.isfinite(delta)):
            return {
                "status": "numerical_failure",
                "reason": "tracking_delta_is_non_finite",
                "iterations": iteration - 1,
                "metrics": last_metrics,
            }
        displacement += delta
        corrected_residual = design @ delta + residual
        residual_sum_squares = float(
            corrected_residual @ corrected_residual
        )
        residual_variance = max(
            residual_sum_squares / max(sample_count - 2, 1),
            config.residual_variance_floor,
        )
        try:
            inverse_normal = np.linalg.inv(normal)
        except np.linalg.LinAlgError:
            return {
                "status": "numerical_failure",
                "reason": "tracking_covariance_inverse_failed",
                "iterations": iteration,
                "metrics": last_metrics,
            }
        covariance = (
            residual_variance
            * inverse_normal
            @ structure
            @ inverse_normal.T
        )
        last_metrics = (
            math.sqrt(residual_sum_squares / sample_count),
            minimum_eigen,
            maximum_eigen,
            condition,
            covariance,
        )
        if float(np.linalg.norm(delta)) <= config.convergence_threshold_pixels:
            return {
                "status": "available",
                "reason": None,
                "iterations": iteration,
                "displacement": displacement,
                "converged": True,
                "metrics": last_metrics,
            }
    return {
        "status": "available",
        "reason": "iteration_limit_reached",
        "iterations": config.maximum_iterations,
        "displacement": displacement,
        "converged": False,
        "metrics": last_metrics,
    }


def _failed_vector(
    *,
    row: int,
    column: int,
    feature_id: str,
    rank: int,
    feature_quality: float,
    status: str,
    displacement: np.ndarray,
    levels_completed: int,
    iterations: int,
    failure_level: int,
    reason: str,
    metrics: object,
) -> SparseFlowVector:
    residual = minimum = maximum = condition = quality = None
    if metrics is not None:
        residual, minimum, maximum, condition, _covariance = metrics
        quality = minimum / maximum if maximum > 0.0 else 0.0
    return SparseFlowVector(
        feature_id=feature_id,
        selected_rank=rank,
        start_row=row,
        start_column=column,
        feature_quality=feature_quality,
        status=status,
        displacement_column_pixels=float(displacement[0]),
        displacement_row_pixels=float(displacement[1]),
        vx_m_s=None,
        vy_m_s=None,
        levels_completed=levels_completed,
        iterations=iterations,
        converged=False,
        forward_residual_rms=residual,
        structure_min_eigenvalue=minimum,
        structure_max_eigenvalue=maximum,
        condition_number=condition,
        structure_quality=quality,
        velocity_covariance_m2_s2=None,
        failure_level=failure_level,
        failure_reason=reason,
    )


def _build_pyramid(
    image: np.ndarray,
    config: SparsePyramidalConfig,
) -> tuple[np.ndarray, ...]:
    levels = [image]
    for _level in range(1, config.pyramid_levels):
        source = levels[-1]
        if min(source.shape) < 4:
            break
        height = (source.shape[0] + 1) // 2
        width = (source.shape[1] + 1) // 2
        target = np.full((height, width), np.nan, dtype=np.float64)
        for row in range(height):
            for column in range(width):
                block = source[
                    row * 2 : min(row * 2 + 2, source.shape[0]),
                    column * 2 : min(column * 2 + 2, source.shape[1]),
                ]
                finite = np.isfinite(block)
                if (
                    np.count_nonzero(finite) / block.size
                    >= config.minimum_pyramid_valid_fraction
                ):
                    target[row, column] = float(np.mean(block[finite]))
        maximum_gradient_rows = max(0, target.shape[0] - 2)
        maximum_gradient_columns = max(0, target.shape[1] - 2)
        maximum_window_support = (
            min(
                2 * config.tracking_window_radius_pixels + 1,
                maximum_gradient_rows,
            )
            * min(
                2 * config.tracking_window_radius_pixels + 1,
                maximum_gradient_columns,
            )
        )
        if maximum_window_support < config.minimum_tracking_samples:
            break
        levels.append(target)
    return tuple(levels)


def _gradients(
    image: np.ndarray,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    finite = np.isfinite(image)
    gx = np.full_like(image, np.nan, dtype=np.float64)
    gy = np.full_like(image, np.nan, dtype=np.float64)
    valid = np.zeros_like(finite, dtype=bool)
    if image.shape[0] >= 3 and image.shape[1] >= 3:
        interior = (
            finite[1:-1, 1:-1]
            & finite[1:-1, :-2]
            & finite[1:-1, 2:]
            & finite[:-2, 1:-1]
            & finite[2:, 1:-1]
        )
        valid[1:-1, 1:-1] = interior
        gx[1:-1, 1:-1] = (
            image[1:-1, 2:] - image[1:-1, :-2]
        ) * 0.5
        gy[1:-1, 1:-1] = (
            image[2:, 1:-1] - image[:-2, 1:-1]
        ) * 0.5
        gx[~valid] = np.nan
        gy[~valid] = np.nan
    return gx, gy, valid


def _bilinear_sample(
    image: np.ndarray,
    x: np.ndarray,
    y: np.ndarray,
) -> tuple[np.ndarray, np.ndarray]:
    x0 = np.floor(x).astype(np.int64)
    y0 = np.floor(y).astype(np.int64)
    x1 = x0 + 1
    y1 = y0 + 1
    inside = (
        (x0 >= 0)
        & (y0 >= 0)
        & (x1 < image.shape[1])
        & (y1 < image.shape[0])
    )
    values = np.full(x.shape, np.nan, dtype=np.float64)
    valid_indices = np.flatnonzero(inside)
    if valid_indices.size:
        xa = x0[valid_indices]
        xb = x1[valid_indices]
        ya = y0[valid_indices]
        yb = y1[valid_indices]
        neighbours = np.column_stack(
            (
                image[ya, xa],
                image[ya, xb],
                image[yb, xa],
                image[yb, xb],
            )
        )
        finite = np.all(np.isfinite(neighbours), axis=1)
        accepted = valid_indices[finite]
        if accepted.size:
            x_fraction = x[accepted] - x0[accepted]
            y_fraction = y[accepted] - y0[accepted]
            values[accepted] = (
                image[y0[accepted], x0[accepted]]
                * (1.0 - x_fraction)
                * (1.0 - y_fraction)
                + image[y0[accepted], x1[accepted]]
                * x_fraction
                * (1.0 - y_fraction)
                + image[y1[accepted], x0[accepted]]
                * (1.0 - x_fraction)
                * y_fraction
                + image[y1[accepted], x1[accepted]]
                * x_fraction
                * y_fraction
            )
    return values, np.isfinite(values)


def _validate_sequence(
    frames: tuple[NormalizedRasterFrame, ...],
    config: SparsePyramidalConfig,
) -> None:
    first = frames[0]
    if first.grid.width * first.grid.height > config.maximum_input_pixels:
        raise ValueError("raster overschrijdt caller maximum_input_pixels")
    identifiers = tuple(item.frame_id for item in frames)
    if len(identifiers) != len(set(identifiers)):
        raise ValueError("motionsequence vereist unieke frame-ID's")
    for previous, current in zip(frames, frames[1:]):
        previous.grid.assert_same_geometry(current.grid)
        if current.valid_at <= previous.valid_at:
            raise ValueError("motionframetijden moeten strikt oplopen")
        if previous.quantity != current.quantity or previous.unit != current.unit:
            raise ValueError("motionframes moeten quantity en unit delen")


def _frame_array(frame: NormalizedRasterFrame) -> np.ndarray:
    return np.asarray(
        tuple(
            tuple(np.nan if item is None else float(item) for item in row)
            for row in frame.values
        ),
        dtype=np.float64,
    )


def _feature_id(
    previous: NormalizedRasterFrame,
    current: NormalizedRasterFrame,
    row: int,
    column: int,
    rank: int,
    config: SparsePyramidalConfig,
) -> str:
    return "shi-tomasi:" + hashlib.sha256(
        (
            f"{previous.frame_id}\0{current.frame_id}\0{row}\0{column}\0"
            f"{rank}\0{config.fingerprint}"
        ).encode("utf-8")
    ).hexdigest()


def _vector_token(vector: SparseFlowVector) -> dict[str, object]:
    return {
        "feature_id": vector.feature_id,
        "rank": vector.selected_rank,
        "row": vector.start_row,
        "column": vector.start_column,
        "feature_quality": _float_token(vector.feature_quality),
        "status": vector.status,
        "dx": _optional_float(vector.displacement_column_pixels),
        "dy": _optional_float(vector.displacement_row_pixels),
        "vx": _optional_float(vector.vx_m_s),
        "vy": _optional_float(vector.vy_m_s),
        "levels_completed": vector.levels_completed,
        "iterations": vector.iterations,
        "converged": vector.converged,
        "residual": _optional_float(vector.forward_residual_rms),
        "min_eigen": _optional_float(vector.structure_min_eigenvalue),
        "max_eigen": _optional_float(vector.structure_max_eigenvalue),
        "condition": _optional_float(vector.condition_number),
        "quality": _optional_float(vector.structure_quality),
        "covariance": (
            None
            if vector.velocity_covariance_m2_s2 is None
            else _covariance_token(vector.velocity_covariance_m2_s2)
        ),
        "failure_level": vector.failure_level,
        "failure_reason": vector.failure_reason,
    }


def _input_checksum(frames: tuple[NormalizedRasterFrame, ...]) -> str:
    return _sha256(
        [
            {
                "frame_id": frame.frame_id,
                "valid_at": _float_token(frame.valid_at),
                "source": frame.source,
                "quantity": frame.quantity,
                "unit": frame.unit,
                "grid": (
                    frame.grid.grid_id,
                    frame.grid.crs,
                    _float_token(frame.grid.origin_x_m),
                    _float_token(frame.grid.origin_y_m),
                    _float_token(frame.grid.x_resolution_m),
                    _float_token(frame.grid.y_resolution_m),
                    frame.grid.width,
                    frame.grid.height,
                ),
                "values": [
                    [
                        None if item is None else _float_token(item)
                        for item in row
                    ]
                    for row in frame.values
                ],
            }
            for frame in frames
        ]
    )


def _array_covariance(value: np.ndarray) -> Covariance2D:
    result: Covariance2D = (
        (float(value[0, 0]), float(value[0, 1])),
        (float(value[1, 0]), float(value[1, 1])),
    )
    _validate_covariance2(result, "berekende sparse-flowcovariantie")
    return result


def _validate_covariance2(value: Covariance2D, label: str) -> None:
    if (
        not isinstance(value, tuple)
        or len(value) != 2
        or any(not isinstance(row, tuple) or len(row) != 2 for row in value)
    ):
        raise ValueError(f"{label} moet een immutable 2x2 tuple zijn")
    xx, xy = value[0]
    yx, yy = value[1]
    if not all(math.isfinite(item) for item in (xx, xy, yx, yy)):
        raise ValueError(f"{label} moet eindig zijn")
    if not math.isclose(xy, yx, rel_tol=1e-9, abs_tol=1e-9):
        raise ValueError(f"{label} moet symmetrisch zijn")
    determinant = xx * yy - xy * yx
    tolerance = 1e-9 * max(1.0, abs(xx * yy), abs(xy * yx))
    if xx < 0.0 or yy < 0.0 or determinant < -tolerance:
        raise ValueError(f"{label} moet positief semidefiniet zijn")


def _positive_integer(value: int, label: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{label} moet een positief geheel getal zijn")


def _bounded_positive_integer(value: int, label: str, maximum: int) -> None:
    _positive_integer(value, label)
    if value > maximum:
        raise ValueError(f"{label} overschrijdt harde productielimiet {maximum}")


def _covariance_token(value: Covariance2D) -> list[list[str]]:
    return [[_float_token(item) for item in row] for row in value]


def _float_token(value: float) -> str:
    return float(value).hex()


def _optional_float(value: float | None) -> str | None:
    return None if value is None else _float_token(value)


def _sha256(value: object) -> str:
    payload = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()
