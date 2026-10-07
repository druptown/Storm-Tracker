"""Exact last-two-position and explicit cold-start priors for CellTrack.

The interval prior implements the user's scientifically valid base observation:
two associated metric positions define one exact interval velocity.  It never
caps, smooths or rejects that vector.  Covariance follows from both independent
position measurements and a caller-supplied process-noise covariance.
"""
from __future__ import annotations

import itertools
import math
from dataclasses import dataclass

from .cell_track import CellDetection
from .motion_prediction import Covariance2D


TRACK_MOTION_PRIOR_VERSION = "stv3-track-motion-prior-v1"
Covariance4D = tuple[
    tuple[float, float, float, float],
    tuple[float, float, float, float],
    tuple[float, float, float, float],
    tuple[float, float, float, float],
]


@dataclass(frozen=True, slots=True)
class TrackMotionPrior:
    """Auditable constant-velocity prediction in ``[x, y, vx, vy]`` order."""

    prior_id: str
    detection_id: str
    supporting_detection_ids: tuple[str, ...]
    analysis_grid_id: str
    source_valid_at: float
    target_valid_at: float
    source_x_m: float
    source_y_m: float
    vx_m_s: float
    vy_m_s: float
    predicted_x_m: float
    predicted_y_m: float
    source_state_covariance: Covariance4D
    process_noise_covariance: Covariance4D
    predicted_state_covariance: Covariance4D
    method: str
    algorithm_version: str = TRACK_MOTION_PRIOR_VERSION

    def __post_init__(self) -> None:
        for label, value in (
            ("prior_id", self.prior_id),
            ("detection_id", self.detection_id),
            ("analysis_grid_id", self.analysis_grid_id),
            ("method", self.method),
        ):
            if not isinstance(value, str) or not value:
                raise ValueError(f"TrackMotionPrior vereist {label}")
        if (
            not isinstance(self.supporting_detection_ids, tuple)
            or not self.supporting_detection_ids
            or len(self.supporting_detection_ids)
            != len(set(self.supporting_detection_ids))
        ):
            raise ValueError(
                "TrackMotionPrior vereist unieke supporting_detection_ids"
            )
        scalars = (
            self.source_valid_at,
            self.target_valid_at,
            self.source_x_m,
            self.source_y_m,
            self.vx_m_s,
            self.vy_m_s,
            self.predicted_x_m,
            self.predicted_y_m,
        )
        if not all(math.isfinite(value) for value in scalars):
            raise ValueError("TrackMotionPrior-waarden moeten eindig zijn")
        if self.target_valid_at <= self.source_valid_at:
            raise ValueError("TrackMotionPrior target moet later zijn")
        for label, covariance in (
            ("source_state_covariance", self.source_state_covariance),
            ("process_noise_covariance", self.process_noise_covariance),
            ("predicted_state_covariance", self.predicted_state_covariance),
        ):
            _validate_covariance4(covariance, label)
        elapsed = self.target_valid_at - self.source_valid_at
        if not math.isclose(
            self.predicted_x_m,
            self.source_x_m + self.vx_m_s * elapsed,
            rel_tol=1e-12,
            abs_tol=1e-9,
        ) or not math.isclose(
            self.predicted_y_m,
            self.source_y_m + self.vy_m_s * elapsed,
            rel_tol=1e-12,
            abs_tol=1e-9,
        ):
            raise ValueError("TrackMotionPrior-positie past niet bij velocity")
        expected = _propagate_covariance(
            self.source_state_covariance,
            elapsed,
            self.process_noise_covariance,
        )
        if any(
            not math.isclose(
                expected[row][column],
                self.predicted_state_covariance[row][column],
                rel_tol=1e-12,
                abs_tol=1e-8,
            )
            for row in range(4)
            for column in range(4)
        ):
            raise ValueError("TrackMotionPrior-covariantiepropagatie klopt niet")

    @property
    def sample_id(self) -> str:
        """Compatibility identifier for association-feature provenance."""

        return self.prior_id

    @property
    def predicted_position_covariance_m2(self) -> Covariance2D:
        return (
            (
                self.predicted_state_covariance[0][0],
                self.predicted_state_covariance[0][1],
            ),
            (
                self.predicted_state_covariance[1][0],
                self.predicted_state_covariance[1][1],
            ),
        )

    @property
    def interval_speed_kmh(self) -> float:
        return math.hypot(self.vx_m_s, self.vy_m_s) * 3.6


def build_two_position_track_prior(
    prior_id: str,
    older: CellDetection,
    latest: CellDetection,
    *,
    target_valid_at: float,
    process_noise_covariance: Covariance4D,
) -> TrackMotionPrior:
    """Use the exact last-two-position interval velocity without smoothing."""

    if not isinstance(older, CellDetection) or not isinstance(
        latest,
        CellDetection,
    ):
        raise TypeError("two-position prior vereist twee CellDetection-objecten")
    if latest.valid_at <= older.valid_at:
        raise ValueError("latest detectie moet meteorologisch later zijn")
    if latest.analysis_grid_id != older.analysis_grid_id:
        raise ValueError("two-position detecties moeten hetzelfde grid delen")
    if not math.isfinite(target_valid_at) or target_valid_at <= latest.valid_at:
        raise ValueError("target_valid_at moet later zijn dan latest detectie")
    _validate_covariance4(
        process_noise_covariance,
        "process_noise_covariance",
    )
    interval = latest.valid_at - older.valid_at
    vx_m_s = (latest.x_m - older.x_m) / interval
    vy_m_s = (latest.y_m - older.y_m) / interval
    older_variance = older.position_sigma_m**2
    latest_variance = latest.position_sigma_m**2
    velocity_variance = (
        older_variance + latest_variance
    ) / interval**2
    position_velocity_covariance = latest_variance / interval
    source_covariance: Covariance4D = (
        (
            latest_variance,
            0.0,
            position_velocity_covariance,
            0.0,
        ),
        (
            0.0,
            latest_variance,
            0.0,
            position_velocity_covariance,
        ),
        (
            position_velocity_covariance,
            0.0,
            velocity_variance,
            0.0,
        ),
        (
            0.0,
            position_velocity_covariance,
            0.0,
            velocity_variance,
        ),
    )
    return _build_prior(
        prior_id=prior_id,
        detection=latest,
        supporting_detection_ids=(older.detection_id, latest.detection_id),
        target_valid_at=target_valid_at,
        vx_m_s=vx_m_s,
        vy_m_s=vy_m_s,
        source_state_covariance=source_covariance,
        process_noise_covariance=process_noise_covariance,
        method="last_two_associated_positions",
    )


def build_cold_start_track_prior(
    prior_id: str,
    detection: CellDetection,
    *,
    target_valid_at: float,
    vx_m_s: float,
    vy_m_s: float,
    caller_initial_state_covariance: Covariance4D,
    process_noise_covariance: Covariance4D,
    method: str,
) -> TrackMotionPrior:
    """Build an explicit zero/flow prior when two positions do not yet exist."""

    if not isinstance(detection, CellDetection):
        raise TypeError("cold-start prior vereist een CellDetection")
    if method not in ("cold_start_zero", "cold_start_optical_flow"):
        raise ValueError("cold-start method moet zero of optical_flow zijn")
    if method == "cold_start_zero" and (vx_m_s != 0.0 or vy_m_s != 0.0):
        raise ValueError("cold_start_zero vereist exact nulvelocity")
    if not math.isfinite(vx_m_s) or not math.isfinite(vy_m_s):
        raise ValueError("cold-start velocity moet eindig zijn")
    _validate_covariance4(
        caller_initial_state_covariance,
        "caller_initial_state_covariance",
    )
    _validate_covariance4(
        process_noise_covariance,
        "process_noise_covariance",
    )
    return _build_prior(
        prior_id=prior_id,
        detection=detection,
        supporting_detection_ids=(detection.detection_id,),
        target_valid_at=target_valid_at,
        vx_m_s=vx_m_s,
        vy_m_s=vy_m_s,
        source_state_covariance=caller_initial_state_covariance,
        process_noise_covariance=process_noise_covariance,
        method=method,
    )


def _build_prior(
    *,
    prior_id: str,
    detection: CellDetection,
    supporting_detection_ids: tuple[str, ...],
    target_valid_at: float,
    vx_m_s: float,
    vy_m_s: float,
    source_state_covariance: Covariance4D,
    process_noise_covariance: Covariance4D,
    method: str,
) -> TrackMotionPrior:
    elapsed = target_valid_at - detection.valid_at
    predicted_covariance = _propagate_covariance(
        source_state_covariance,
        elapsed,
        process_noise_covariance,
    )
    return TrackMotionPrior(
        prior_id=prior_id,
        detection_id=detection.detection_id,
        supporting_detection_ids=supporting_detection_ids,
        analysis_grid_id=detection.analysis_grid_id,
        source_valid_at=detection.valid_at,
        target_valid_at=target_valid_at,
        source_x_m=detection.x_m,
        source_y_m=detection.y_m,
        vx_m_s=vx_m_s,
        vy_m_s=vy_m_s,
        predicted_x_m=detection.x_m + vx_m_s * elapsed,
        predicted_y_m=detection.y_m + vy_m_s * elapsed,
        source_state_covariance=source_state_covariance,
        process_noise_covariance=process_noise_covariance,
        predicted_state_covariance=predicted_covariance,
        method=method,
    )


def _propagate_covariance(
    source: Covariance4D,
    elapsed_seconds: float,
    process_noise: Covariance4D,
) -> Covariance4D:
    transition = (
        (1.0, 0.0, elapsed_seconds, 0.0),
        (0.0, 1.0, 0.0, elapsed_seconds),
        (0.0, 0.0, 1.0, 0.0),
        (0.0, 0.0, 0.0, 1.0),
    )
    left = _multiply4(transition, source)
    propagated = _multiply4(left, _transpose4(transition))
    result = tuple(
        tuple(
            propagated[row][column] + process_noise[row][column]
            for column in range(4)
        )
        for row in range(4)
    )
    _validate_covariance4(result, "predicted_state_covariance")
    return result  # type: ignore[return-value]


def _multiply4(
    left: tuple[tuple[float, ...], ...],
    right: tuple[tuple[float, ...], ...],
) -> tuple[tuple[float, ...], ...]:
    return tuple(
        tuple(
            sum(left[row][inner] * right[inner][column] for inner in range(4))
            for column in range(4)
        )
        for row in range(4)
    )


def _transpose4(
    value: tuple[tuple[float, ...], ...],
) -> tuple[tuple[float, ...], ...]:
    return tuple(tuple(value[column][row] for column in range(4)) for row in range(4))


def _validate_covariance4(value: Covariance4D, label: str) -> None:
    if (
        not isinstance(value, tuple)
        or len(value) != 4
        or any(not isinstance(row, tuple) or len(row) != 4 for row in value)
    ):
        raise ValueError(f"{label} moet een immutable 4x4 tuple zijn")
    if not all(math.isfinite(item) for row in value for item in row):
        raise ValueError(f"{label} moet eindig zijn")
    for row in range(4):
        for column in range(4):
            if not math.isclose(
                value[row][column],
                value[column][row],
                rel_tol=1e-12,
                abs_tol=1e-12,
            ):
                raise ValueError(f"{label} moet symmetrisch zijn")
    scale = max(1.0, *(abs(item) for row in value for item in row))
    for size in range(1, 5):
        for indices in itertools.combinations(range(4), size):
            minor = tuple(
                tuple(value[row][column] for column in indices)
                for row in indices
            )
            determinant = _determinant(minor)
            tolerance = 1e-10 * scale**size
            if determinant < -tolerance:
                raise ValueError(f"{label} moet positief semidefiniet zijn")


def _determinant(value: tuple[tuple[float, ...], ...]) -> float:
    size = len(value)
    if size == 1:
        return value[0][0]
    return sum(
        (
            (-1.0 if column % 2 else 1.0)
            * value[0][column]
            * _determinant(
                tuple(
                    tuple(row[item] for item in range(size) if item != column)
                    for row in value[1:]
                )
            )
        )
        for column in range(size)
    )
