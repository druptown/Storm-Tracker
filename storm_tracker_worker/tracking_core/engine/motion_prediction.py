"""Optical-flow motion prediction contract with explicit uncertainty.

This module does not estimate optical flow and does not emit a confidence
score.  A provider/algorithm supplies a metric velocity vector, its covariance
and an independently calibrated residual covariance for the prediction
interval.  The resulting position distribution can then be audited directly.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Callable, Protocol

from .cell_track import CellDetection


MOTION_PREDICTION_VERSION = "stv3-explicit-optical-flow-prediction-v1"
Covariance2D = tuple[tuple[float, float], tuple[float, float]]


@dataclass(frozen=True, slots=True)
class OpticalFlowSample:
    """A caller-produced local velocity sample in SI units."""

    sample_id: str
    analysis_grid_id: str
    reference_valid_at: float
    vx_m_s: float
    vy_m_s: float
    velocity_covariance_m2_s2: Covariance2D
    residual_covariance_m2: Covariance2D
    method: str
    support_frame_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        for label, value in (
            ("sample_id", self.sample_id),
            ("analysis_grid_id", self.analysis_grid_id),
            ("method", self.method),
        ):
            if not isinstance(value, str) or not value:
                raise ValueError(f"OpticalFlowSample vereist {label}")
        for label, value in (
            ("reference_valid_at", self.reference_valid_at),
            ("vx_m_s", self.vx_m_s),
            ("vy_m_s", self.vy_m_s),
        ):
            if not math.isfinite(value):
                raise ValueError(f"OpticalFlowSample {label} moet eindig zijn")
        _validate_covariance(
            self.velocity_covariance_m2_s2,
            "velocity_covariance_m2_s2",
        )
        _validate_covariance(
            self.residual_covariance_m2,
            "residual_covariance_m2",
        )
        if (
            not isinstance(self.support_frame_ids, tuple)
            or not self.support_frame_ids
            or any(
                not isinstance(item, str) or not item
                for item in self.support_frame_ids
            )
        ):
            raise ValueError("OpticalFlowSample vereist immutable support_frame_ids")
        if len(self.support_frame_ids) != len(set(self.support_frame_ids)):
            raise ValueError("OpticalFlowSample support_frame_ids moeten uniek zijn")


@dataclass(frozen=True, slots=True)
class MotionPrediction:
    """Predicted metric position and fully decomposed covariance."""

    detection_id: str
    sample_id: str
    analysis_grid_id: str
    source_valid_at: float
    target_valid_at: float
    source_x_m: float
    source_y_m: float
    predicted_x_m: float
    predicted_y_m: float
    vx_m_s: float
    vy_m_s: float
    measurement_covariance_m2: Covariance2D
    propagated_flow_covariance_m2: Covariance2D
    residual_covariance_m2: Covariance2D
    predicted_position_covariance_m2: Covariance2D
    method: str
    support_frame_ids: tuple[str, ...]
    algorithm_version: str = MOTION_PREDICTION_VERSION

    def __post_init__(self) -> None:
        for label, value in (
            ("detection_id", self.detection_id),
            ("sample_id", self.sample_id),
            ("analysis_grid_id", self.analysis_grid_id),
            ("method", self.method),
        ):
            if not isinstance(value, str) or not value:
                raise ValueError(f"MotionPrediction vereist {label}")
        scalars = (
            self.source_valid_at,
            self.target_valid_at,
            self.source_x_m,
            self.source_y_m,
            self.predicted_x_m,
            self.predicted_y_m,
            self.vx_m_s,
            self.vy_m_s,
        )
        if not all(math.isfinite(value) for value in scalars):
            raise ValueError("MotionPrediction-waarden moeten eindig zijn")
        if self.target_valid_at <= self.source_valid_at:
            raise ValueError("MotionPrediction target moet later zijn dan source")
        for label, value in (
            ("measurement_covariance_m2", self.measurement_covariance_m2),
            (
                "propagated_flow_covariance_m2",
                self.propagated_flow_covariance_m2,
            ),
            ("residual_covariance_m2", self.residual_covariance_m2),
            (
                "predicted_position_covariance_m2",
                self.predicted_position_covariance_m2,
            ),
        ):
            _validate_covariance(value, label)
        expected = _add_covariances(
            self.measurement_covariance_m2,
            self.propagated_flow_covariance_m2,
            self.residual_covariance_m2,
        )
        if any(
            not math.isclose(
                expected[row][column],
                self.predicted_position_covariance_m2[row][column],
                rel_tol=1e-12,
                abs_tol=1e-9,
            )
            for row in range(2)
            for column in range(2)
        ):
            raise ValueError("MotionPrediction-covariantiedecompositie klopt niet")
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
            raise ValueError("MotionPrediction-positie past niet bij vector en tijd")
        if (
            not isinstance(self.support_frame_ids, tuple)
            or not self.support_frame_ids
            or len(self.support_frame_ids) != len(
                set(self.support_frame_ids)
            )
        ):
            raise ValueError(
                "MotionPrediction vereist unieke immutable support_frame_ids"
            )

    @property
    def elapsed_seconds(self) -> float:
        return self.target_valid_at - self.source_valid_at

    @property
    def interval_speed_kmh(self) -> float:
        return math.hypot(self.vx_m_s, self.vy_m_s) * 3.6


class MotionPredictor(Protocol):
    """Minimal injectable optical-flow prediction interface."""

    def predict(
        self,
        detection: CellDetection,
        target_valid_at: float,
    ) -> MotionPrediction:
        ...


@dataclass(frozen=True, slots=True)
class SuppliedOpticalFlowPredictor:
    """Adapter around a caller-owned metric vector-field sampler."""

    sampler: Callable[[CellDetection, float], OpticalFlowSample]

    def predict(
        self,
        detection: CellDetection,
        target_valid_at: float,
    ) -> MotionPrediction:
        sample = self.sampler(detection, target_valid_at)
        if not isinstance(sample, OpticalFlowSample):
            raise TypeError("optical-flow sampler moet OpticalFlowSample teruggeven")
        return predict_with_optical_flow(detection, target_valid_at, sample)


def predict_with_optical_flow(
    detection: CellDetection,
    target_valid_at: float,
    sample: OpticalFlowSample,
) -> MotionPrediction:
    """Propagate a detection without clipping the supplied motion vector."""

    if not isinstance(detection, CellDetection):
        raise TypeError("motion prediction vereist een CellDetection")
    if not isinstance(sample, OpticalFlowSample):
        raise TypeError("motion prediction vereist een OpticalFlowSample")
    if not math.isfinite(target_valid_at) or target_valid_at <= detection.valid_at:
        raise ValueError("target_valid_at moet eindig en meteorologisch later zijn")
    if sample.analysis_grid_id != detection.analysis_grid_id:
        raise ValueError("optical flow en detectie moeten hetzelfde grid delen")
    if not math.isclose(
        sample.reference_valid_at,
        detection.valid_at,
        rel_tol=0.0,
        abs_tol=1e-6,
    ):
        raise ValueError("optical-flow referentietijd verschilt van detectietijd")

    elapsed = target_valid_at - detection.valid_at
    measurement = (
        (detection.position_sigma_m**2, 0.0),
        (0.0, detection.position_sigma_m**2),
    )
    propagated = _scale_covariance(
        sample.velocity_covariance_m2_s2,
        elapsed**2,
    )
    combined = _add_covariances(
        measurement,
        propagated,
        sample.residual_covariance_m2,
    )
    return MotionPrediction(
        detection_id=detection.detection_id,
        sample_id=sample.sample_id,
        analysis_grid_id=detection.analysis_grid_id,
        source_valid_at=detection.valid_at,
        target_valid_at=target_valid_at,
        source_x_m=detection.x_m,
        source_y_m=detection.y_m,
        predicted_x_m=detection.x_m + sample.vx_m_s * elapsed,
        predicted_y_m=detection.y_m + sample.vy_m_s * elapsed,
        vx_m_s=sample.vx_m_s,
        vy_m_s=sample.vy_m_s,
        measurement_covariance_m2=measurement,
        propagated_flow_covariance_m2=propagated,
        residual_covariance_m2=sample.residual_covariance_m2,
        predicted_position_covariance_m2=combined,
        method=sample.method,
        support_frame_ids=sample.support_frame_ids,
    )


def _validate_covariance(value: Covariance2D, label: str) -> None:
    if (
        not isinstance(value, tuple)
        or len(value) != 2
        or any(not isinstance(row, tuple) or len(row) != 2 for row in value)
    ):
        raise ValueError(f"{label} moet een 2x2 tuple zijn")
    xx, xy = value[0]
    yx, yy = value[1]
    if not all(math.isfinite(item) for item in (xx, xy, yx, yy)):
        raise ValueError(f"{label} moet eindig zijn")
    if not math.isclose(xy, yx, rel_tol=1e-12, abs_tol=1e-12):
        raise ValueError(f"{label} moet symmetrisch zijn")
    if xx < 0.0 or yy < 0.0:
        raise ValueError(f"{label} moet positief semidefiniet zijn")
    determinant = xx * yy - xy * yx
    tolerance = 1e-12 * max(1.0, abs(xx * yy), abs(xy * yx))
    if determinant < -tolerance:
        raise ValueError(f"{label} moet positief semidefiniet zijn")


def _scale_covariance(value: Covariance2D, factor: float) -> Covariance2D:
    return (
        (value[0][0] * factor, value[0][1] * factor),
        (value[1][0] * factor, value[1][1] * factor),
    )


def _add_covariances(*values: Covariance2D) -> Covariance2D:
    result = (
        (
            sum(value[0][0] for value in values),
            sum(value[0][1] for value in values),
        ),
        (
            sum(value[1][0] for value in values),
            sum(value[1][1] for value in values),
        ),
    )
    _validate_covariance(result, "predicted_position_covariance_m2")
    return result
