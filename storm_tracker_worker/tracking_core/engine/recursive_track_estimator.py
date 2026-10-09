"""Uncapped recursive constant-velocity estimator for associated CellDetections.

The exact velocity between the latest two observed positions remains part of
every revision.  The recursive state is a separate prediction challenger: it
assimilates every already-associated detection without a speed or innovation
gate and records the innovation instead of silently rejecting unusual motion.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import itertools
import math

from .cell_track import CellDetection


RECURSIVE_TRACK_ESTIMATOR_VERSION = "stv3-recursive-track-estimator-v1"
Matrix4 = tuple[
    tuple[float, float, float, float],
    tuple[float, float, float, float],
    tuple[float, float, float, float],
    tuple[float, float, float, float],
]
Matrix2 = tuple[tuple[float, float], tuple[float, float]]


@dataclass(frozen=True, slots=True)
class RecursiveTrackEstimatorPolicy:
    """Explicit white-acceleration process model; no hidden defaults."""

    policy_id: str
    acceleration_spectral_density_m2_s3: float
    algorithm_version: str = RECURSIVE_TRACK_ESTIMATOR_VERSION

    def __post_init__(self) -> None:
        _identifier(self.policy_id, "policy_id")
        value = self.acceleration_spectral_density_m2_s3
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError("acceleration_spectral_density moet numeriek zijn")
        if not math.isfinite(value) or value < 0.0:
            raise ValueError(
                "acceleration_spectral_density moet eindig en niet-negatief zijn"
            )
        if self.algorithm_version != RECURSIVE_TRACK_ESTIMATOR_VERSION:
            raise ValueError("onbekende recursive-estimatorversie")

    @property
    def fingerprint(self) -> str:
        return hashlib.sha256(
            "\0".join((
                self.policy_id,
                format(self.acceleration_spectral_density_m2_s3, ".17g"),
                self.algorithm_version,
            )).encode("utf-8")
        ).hexdigest()


@dataclass(frozen=True, slots=True)
class RecursiveTrackEstimate:
    """One immutable posterior revision in ``[x, y, vx, vy]`` order."""

    estimate_id: str
    analysis_grid_id: str
    valid_at: float
    state: tuple[float, float, float, float]
    covariance: Matrix4
    supporting_detection_ids: tuple[str, ...]
    last_observed_x_m: float
    last_observed_y_m: float
    latest_interval_vx_m_s: float | None
    latest_interval_vy_m_s: float | None
    innovation_x_m: float | None
    innovation_y_m: float | None
    normalized_innovation_squared: float | None
    policy_id: str
    policy_fingerprint: str
    revision: int
    method: str = "recursive_constant_velocity_no_gate"
    algorithm_version: str = RECURSIVE_TRACK_ESTIMATOR_VERSION

    def __post_init__(self) -> None:
        for label, value in (
            ("estimate_id", self.estimate_id),
            ("analysis_grid_id", self.analysis_grid_id),
            ("policy_id", self.policy_id),
            ("method", self.method),
        ):
            _identifier(value, label)
        if not math.isfinite(self.valid_at):
            raise ValueError("valid_at moet eindig zijn")
        if (
            not isinstance(self.state, tuple)
            or len(self.state) != 4
            or not all(math.isfinite(value) for value in self.state)
        ):
            raise ValueError("state moet een eindige immutable 4-vector zijn")
        _validate_covariance4(self.covariance, "covariance")
        if (
            not isinstance(self.supporting_detection_ids, tuple)
            or not self.supporting_detection_ids
            or len(self.supporting_detection_ids)
            != len(set(self.supporting_detection_ids))
        ):
            raise ValueError("supporting_detection_ids moeten uniek zijn")
        for identifier in self.supporting_detection_ids:
            _identifier(identifier, "supporting detection")
        if self.revision != len(self.supporting_detection_ids):
            raise ValueError("revision moet het aantal assimilaties volgen")
        for label, value in (
            ("last_observed_x_m", self.last_observed_x_m),
            ("last_observed_y_m", self.last_observed_y_m),
        ):
            if not math.isfinite(value):
                raise ValueError(f"{label} moet eindig zijn")
        optional_pairs = (
            (self.latest_interval_vx_m_s, self.latest_interval_vy_m_s),
            (self.innovation_x_m, self.innovation_y_m),
        )
        for first, second in optional_pairs:
            if (first is None) != (second is None):
                raise ValueError("optionele vectorcomponenten moeten samen bestaan")
            if first is not None and not all(
                math.isfinite(value) for value in (first, second)
            ):
                raise ValueError("optionele vector moet eindig zijn")
        if self.revision == 1 and (
            self.latest_interval_vx_m_s is not None
            or self.innovation_x_m is not None
            or self.normalized_innovation_squared is not None
        ):
            raise ValueError("eerste revisie heeft nog geen interval/innovatie")
        if self.revision > 1 and (
            self.latest_interval_vx_m_s is None
            or self.innovation_x_m is None
            or self.normalized_innovation_squared is None
        ):
            raise ValueError("latere revisie vereist interval en innovatie")
        if (
            self.normalized_innovation_squared is not None
            and (
                not math.isfinite(self.normalized_innovation_squared)
                or self.normalized_innovation_squared < 0.0
            )
        ):
            raise ValueError("normalized innovation moet niet-negatief zijn")
        _sha256(self.policy_fingerprint, "policy_fingerprint")
        if self.algorithm_version != RECURSIVE_TRACK_ESTIMATOR_VERSION:
            raise ValueError("onbekende recursive-estimatorversie")

    @property
    def x_m(self) -> float:
        return self.state[0]

    @property
    def y_m(self) -> float:
        return self.state[1]

    @property
    def vx_m_s(self) -> float:
        return self.state[2]

    @property
    def vy_m_s(self) -> float:
        return self.state[3]

    @property
    def filtered_speed_kmh(self) -> float:
        return math.hypot(self.vx_m_s, self.vy_m_s) * 3.6

    @property
    def latest_interval_speed_kmh(self) -> float | None:
        if self.latest_interval_vx_m_s is None:
            return None
        return math.hypot(
            self.latest_interval_vx_m_s,
            self.latest_interval_vy_m_s or 0.0,
        ) * 3.6


@dataclass(frozen=True, slots=True)
class RecursiveTrackPrediction:
    estimate_id: str
    analysis_grid_id: str
    source_valid_at: float
    target_valid_at: float
    state: tuple[float, float, float, float]
    covariance: Matrix4
    policy_id: str
    policy_fingerprint: str
    algorithm_version: str = RECURSIVE_TRACK_ESTIMATOR_VERSION

    def __post_init__(self) -> None:
        _identifier(self.estimate_id, "estimate_id")
        _identifier(self.analysis_grid_id, "analysis_grid_id")
        _identifier(self.policy_id, "policy_id")
        if (
            not math.isfinite(self.source_valid_at)
            or not math.isfinite(self.target_valid_at)
            or self.target_valid_at <= self.source_valid_at
        ):
            raise ValueError("prediction target moet eindig en later zijn")
        if (
            not isinstance(self.state, tuple)
            or len(self.state) != 4
            or not all(math.isfinite(value) for value in self.state)
        ):
            raise ValueError("prediction state moet eindig zijn")
        _validate_covariance4(self.covariance, "prediction covariance")
        _sha256(self.policy_fingerprint, "policy_fingerprint")

    @property
    def predicted_x_m(self) -> float:
        return self.state[0]

    @property
    def predicted_y_m(self) -> float:
        return self.state[1]


def initialize_recursive_track_estimate(
    estimate_id: str,
    detection: CellDetection,
    *,
    initial_vx_m_s: float,
    initial_vy_m_s: float,
    initial_state_covariance: Matrix4,
    policy: RecursiveTrackEstimatorPolicy,
) -> RecursiveTrackEstimate:
    """Initialize explicitly; the caller owns the cold-start uncertainty."""

    _policy(policy)
    if not isinstance(detection, CellDetection):
        raise TypeError("initialisatie vereist CellDetection")
    if not all(math.isfinite(value) for value in (initial_vx_m_s, initial_vy_m_s)):
        raise ValueError("initial velocity moet eindig zijn")
    _validate_covariance4(initial_state_covariance, "initial_state_covariance")
    return RecursiveTrackEstimate(
        estimate_id=estimate_id,
        analysis_grid_id=detection.analysis_grid_id,
        valid_at=detection.valid_at,
        state=(
            detection.x_m,
            detection.y_m,
            float(initial_vx_m_s),
            float(initial_vy_m_s),
        ),
        covariance=initial_state_covariance,
        supporting_detection_ids=(detection.detection_id,),
        last_observed_x_m=detection.x_m,
        last_observed_y_m=detection.y_m,
        latest_interval_vx_m_s=None,
        latest_interval_vy_m_s=None,
        innovation_x_m=None,
        innovation_y_m=None,
        normalized_innovation_squared=None,
        policy_id=policy.policy_id,
        policy_fingerprint=policy.fingerprint,
        revision=1,
    )


def assimilate_recursive_detection(
    estimate: RecursiveTrackEstimate,
    detection: CellDetection,
    policy: RecursiveTrackEstimatorPolicy,
) -> RecursiveTrackEstimate:
    """Assimilate an associated detection and record, never gate, innovation."""

    _compatible(estimate, detection, policy)
    elapsed = detection.valid_at - estimate.valid_at
    predicted_state, predicted_covariance = _propagate(
        estimate.state,
        estimate.covariance,
        elapsed,
        policy.acceleration_spectral_density_m2_s3,
    )
    measurement_variance = detection.position_sigma_m**2
    innovation = (
        detection.x_m - predicted_state[0],
        detection.y_m - predicted_state[1],
    )
    innovation_covariance: Matrix2 = (
        (
            predicted_covariance[0][0] + measurement_variance,
            predicted_covariance[0][1],
        ),
        (
            predicted_covariance[1][0],
            predicted_covariance[1][1] + measurement_variance,
        ),
    )
    inverse = _inverse2(innovation_covariance)
    gain = tuple(
        tuple(
            predicted_covariance[row][0] * inverse[0][column]
            + predicted_covariance[row][1] * inverse[1][column]
            for column in range(2)
        )
        for row in range(4)
    )
    posterior_state = tuple(
        predicted_state[row]
        + gain[row][0] * innovation[0]
        + gain[row][1] * innovation[1]
        for row in range(4)
    )
    identity_minus_gain_h = tuple(
        tuple(
            (1.0 if row == column else 0.0)
            - (gain[row][column] if column < 2 else 0.0)
            for column in range(4)
        )
        for row in range(4)
    )
    joseph_left = _multiply4(
        _multiply4(identity_minus_gain_h, predicted_covariance),
        _transpose4(identity_minus_gain_h),
    )
    joseph_measurement = tuple(
        tuple(
            measurement_variance
            * (
                gain[row][0] * gain[column][0]
                + gain[row][1] * gain[column][1]
            )
            for column in range(4)
        )
        for row in range(4)
    )
    posterior_covariance = _symmetrize4(tuple(
        tuple(
            joseph_left[row][column]
            + joseph_measurement[row][column]
            for column in range(4)
        )
        for row in range(4)
    ))
    _validate_covariance4(posterior_covariance, "posterior_covariance")
    normalized_innovation = (
        innovation[0]
        * (inverse[0][0] * innovation[0] + inverse[0][1] * innovation[1])
        + innovation[1]
        * (inverse[1][0] * innovation[0] + inverse[1][1] * innovation[1])
    )
    return RecursiveTrackEstimate(
        estimate_id=estimate.estimate_id,
        analysis_grid_id=estimate.analysis_grid_id,
        valid_at=detection.valid_at,
        state=posterior_state,  # type: ignore[arg-type]
        covariance=posterior_covariance,
        supporting_detection_ids=(
            estimate.supporting_detection_ids + (detection.detection_id,)
        ),
        last_observed_x_m=detection.x_m,
        last_observed_y_m=detection.y_m,
        latest_interval_vx_m_s=(
            detection.x_m - estimate.last_observed_x_m
        ) / elapsed,
        latest_interval_vy_m_s=(
            detection.y_m - estimate.last_observed_y_m
        ) / elapsed,
        innovation_x_m=innovation[0],
        innovation_y_m=innovation[1],
        normalized_innovation_squared=max(0.0, normalized_innovation),
        policy_id=policy.policy_id,
        policy_fingerprint=policy.fingerprint,
        revision=estimate.revision + 1,
    )


def predict_recursive_track(
    estimate: RecursiveTrackEstimate,
    target_valid_at: float,
    policy: RecursiveTrackEstimatorPolicy,
) -> RecursiveTrackPrediction:
    """Predict through a gap without fabricating a detection or changing identity."""

    _policy(policy)
    _policy_match(estimate, policy)
    if (
        isinstance(target_valid_at, bool)
        or not isinstance(target_valid_at, (int, float))
        or not math.isfinite(target_valid_at)
        or target_valid_at <= estimate.valid_at
    ):
        raise ValueError("target_valid_at moet eindig en later zijn")
    state, covariance = _propagate(
        estimate.state,
        estimate.covariance,
        float(target_valid_at) - estimate.valid_at,
        policy.acceleration_spectral_density_m2_s3,
    )
    return RecursiveTrackPrediction(
        estimate_id=estimate.estimate_id,
        analysis_grid_id=estimate.analysis_grid_id,
        source_valid_at=estimate.valid_at,
        target_valid_at=float(target_valid_at),
        state=state,
        covariance=covariance,
        policy_id=policy.policy_id,
        policy_fingerprint=policy.fingerprint,
    )


def build_recursive_track_estimate(
    estimate_id: str,
    detections: tuple[CellDetection, ...],
    *,
    initial_vx_m_s: float,
    initial_vy_m_s: float,
    initial_state_covariance: Matrix4,
    policy: RecursiveTrackEstimatorPolicy,
) -> RecursiveTrackEstimate:
    """Deterministically replay a caller-associated detection sequence."""

    if not isinstance(detections, tuple) or not detections:
        raise ValueError("detections moet een niet-lege immutable tuple zijn")
    estimate = initialize_recursive_track_estimate(
        estimate_id,
        detections[0],
        initial_vx_m_s=initial_vx_m_s,
        initial_vy_m_s=initial_vy_m_s,
        initial_state_covariance=initial_state_covariance,
        policy=policy,
    )
    for detection in detections[1:]:
        estimate = assimilate_recursive_detection(estimate, detection, policy)
    return estimate


def _compatible(
    estimate: RecursiveTrackEstimate,
    detection: CellDetection,
    policy: RecursiveTrackEstimatorPolicy,
) -> None:
    if not isinstance(estimate, RecursiveTrackEstimate):
        raise TypeError("assimilatie vereist RecursiveTrackEstimate")
    if not isinstance(detection, CellDetection):
        raise TypeError("assimilatie vereist CellDetection")
    _policy(policy)
    _policy_match(estimate, policy)
    if detection.analysis_grid_id != estimate.analysis_grid_id:
        raise ValueError("detectie en estimate moeten hetzelfde grid delen")
    if detection.valid_at <= estimate.valid_at:
        raise ValueError("detectie moet meteorologisch later zijn")
    if detection.detection_id in estimate.supporting_detection_ids:
        raise ValueError("detectie werd al geassimileerd")


def _policy_match(
    estimate: RecursiveTrackEstimate,
    policy: RecursiveTrackEstimatorPolicy,
) -> None:
    if (
        estimate.policy_id != policy.policy_id
        or estimate.policy_fingerprint != policy.fingerprint
    ):
        raise ValueError("estimatorbeleid verschilt tussen revisies")


def _propagate(
    state: tuple[float, float, float, float],
    covariance: Matrix4,
    elapsed: float,
    spectral_density: float,
) -> tuple[tuple[float, float, float, float], Matrix4]:
    transition: Matrix4 = (
        (1.0, 0.0, elapsed, 0.0),
        (0.0, 1.0, 0.0, elapsed),
        (0.0, 0.0, 1.0, 0.0),
        (0.0, 0.0, 0.0, 1.0),
    )
    dt2 = elapsed**2
    dt3 = elapsed**3
    process: Matrix4 = (
        (spectral_density * dt3 / 3.0, 0.0, spectral_density * dt2 / 2.0, 0.0),
        (0.0, spectral_density * dt3 / 3.0, 0.0, spectral_density * dt2 / 2.0),
        (spectral_density * dt2 / 2.0, 0.0, spectral_density * elapsed, 0.0),
        (0.0, spectral_density * dt2 / 2.0, 0.0, spectral_density * elapsed),
    )
    predicted_state = (
        state[0] + state[2] * elapsed,
        state[1] + state[3] * elapsed,
        state[2],
        state[3],
    )
    propagated = _multiply4(
        _multiply4(transition, covariance),
        _transpose4(transition),
    )
    predicted_covariance = _symmetrize4(tuple(
        tuple(
            propagated[row][column] + process[row][column]
            for column in range(4)
        )
        for row in range(4)
    ))
    _validate_covariance4(predicted_covariance, "predicted_covariance")
    return predicted_state, predicted_covariance


def _inverse2(value: Matrix2) -> Matrix2:
    determinant = value[0][0] * value[1][1] - value[0][1] * value[1][0]
    if not math.isfinite(determinant) or determinant <= 0.0:
        raise ValueError("innovation covariance is niet positief definiet")
    return (
        (value[1][1] / determinant, -value[0][1] / determinant),
        (-value[1][0] / determinant, value[0][0] / determinant),
    )


def _multiply4(
    left: tuple[tuple[float, ...], ...],
    right: tuple[tuple[float, ...], ...],
) -> Matrix4:
    return tuple(
        tuple(
            sum(
                left[row][inner] * right[inner][column]
                for inner in range(4)
            )
            for column in range(4)
        )
        for row in range(4)
    )  # type: ignore[return-value]


def _transpose4(value: tuple[tuple[float, ...], ...]) -> Matrix4:
    return tuple(
        tuple(value[column][row] for column in range(4))
        for row in range(4)
    )  # type: ignore[return-value]


def _symmetrize4(value: tuple[tuple[float, ...], ...]) -> Matrix4:
    return tuple(
        tuple(
            (value[row][column] + value[column][row]) / 2.0
            for column in range(4)
        )
        for row in range(4)
    )  # type: ignore[return-value]


def _validate_covariance4(value: Matrix4, label: str) -> None:
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
                rel_tol=1e-10,
                abs_tol=1e-8,
            ):
                raise ValueError(f"{label} moet symmetrisch zijn")
    scale = max(1.0, *(abs(item) for row in value for item in row))
    for size in range(1, 5):
        for indices in itertools.combinations(range(4), size):
            minor = tuple(
                tuple(value[row][column] for column in indices)
                for row in indices
            )
            if _determinant(minor) < -1e-9 * scale**size:
                raise ValueError(f"{label} moet positief semidefiniet zijn")


def _determinant(value: tuple[tuple[float, ...], ...]) -> float:
    if len(value) == 1:
        return value[0][0]
    return sum(
        (-1.0 if column % 2 else 1.0)
        * value[0][column]
        * _determinant(tuple(
            tuple(row[index] for index in range(len(value)) if index != column)
            for row in value[1:]
        ))
        for column in range(len(value))
    )


def _identifier(value: object, label: str) -> str:
    if not isinstance(value, str) or not value or "\0" in value:
        raise ValueError(f"{label} moet een veilige niet-lege string zijn")
    return value


def _sha256(value: object, label: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError(f"{label} moet lowercase SHA-256 zijn")
    return value


def _policy(value: RecursiveTrackEstimatorPolicy) -> None:
    if not isinstance(value, RecursiveTrackEstimatorPolicy):
        raise TypeError("estimator vereist RecursiveTrackEstimatorPolicy")


__all__ = [
    "Matrix4",
    "RECURSIVE_TRACK_ESTIMATOR_VERSION",
    "RecursiveTrackEstimate",
    "RecursiveTrackEstimatorPolicy",
    "RecursiveTrackPrediction",
    "assimilate_recursive_detection",
    "build_recursive_track_estimate",
    "initialize_recursive_track_estimate",
    "predict_recursive_track",
]
