"""Pure shadow primitive for a preliminary two-detection CellTrack.

This module intentionally does not perform association.  Deciding whether two
radar detections represent the same physical cell belongs to the future
association graph.  Once that decision has been made, this estimator provides
the exact observed interval velocity and its measurement covariance without a
speed cap, smoothing, acceleration, or WeatherSystem-level motion.
"""
from __future__ import annotations

import math
from dataclasses import dataclass


PRELIMINARY_CELL_TRACK_ALGORITHM = "stv3-celltrack-two-detection-v1"


@dataclass(frozen=True, slots=True)
class CellDetection:
    """Minimal immutable cell detection needed by the first state estimator.

    Coordinates are SI values on one externally defined metric analysis grid.
    ``position_sigma_m`` is the one-standard-deviation horizontal measurement
    uncertainty.  Both must be supplied by the detector/grid contract; this
    estimator deliberately does not invent a projection or a universal
    uncertainty for products with different native resolutions.
    """

    detection_id: str
    valid_at: float
    analysis_grid_id: str
    x_m: float
    y_m: float
    position_sigma_m: float
    source: str = "unknown"

    def __post_init__(self) -> None:
        if not isinstance(self.detection_id, str) or not self.detection_id:
            raise ValueError("CellDetection vereist een niet-lege detection_id")
        if not math.isfinite(self.valid_at):
            raise ValueError("CellDetection valid_at moet eindig zijn")
        if not isinstance(self.analysis_grid_id, str) or not self.analysis_grid_id:
            raise ValueError(
                "CellDetection vereist een niet-lege analysis_grid_id"
            )
        if not math.isfinite(self.x_m) or not math.isfinite(self.y_m):
            raise ValueError("CellDetection gridcoordinaten moeten eindig zijn")
        if (
            not math.isfinite(self.position_sigma_m)
            or self.position_sigma_m <= 0.0
        ):
            raise ValueError(
                "CellDetection position_sigma_m moet positief en eindig zijn"
            )
        if not isinstance(self.source, str) or not self.source:
            raise ValueError("CellDetection source moet een niet-lege string zijn")


@dataclass(frozen=True, slots=True)
class CellTrackSnapshot:
    """Immutable preliminary state ``[x, y, vx, vy]`` after two detections.

    Position is expressed in metres and velocity in metres per second on one
    fixed analysis grid.  Consequently the 4x4 covariance contains the
    corresponding mixed SI units.
    """

    track_id: str
    lifecycle_state: str
    first_detection_id: str
    latest_detection_id: str
    first_valid_at: float
    valid_at: float
    analysis_grid_id: str
    x_m: float
    y_m: float
    vx_m_s: float
    vy_m_s: float
    state_covariance: tuple[
        tuple[float, float, float, float],
        tuple[float, float, float, float],
        tuple[float, float, float, float],
        tuple[float, float, float, float],
    ]
    sample_count: int = 2
    motion_method: str = "two_detection_interval"
    algorithm_version: str = PRELIMINARY_CELL_TRACK_ALGORITHM

    @property
    def interval_seconds(self) -> float:
        return self.valid_at - self.first_valid_at

    @property
    def speed_kmh(self) -> float:
        return math.hypot(self.vx_m_s, self.vy_m_s) * 3.6

    @property
    def heading_deg(self) -> float | None:
        if self.speed_kmh < 0.5:
            return None
        return math.degrees(math.atan2(self.vx_m_s, self.vy_m_s)) % 360.0


def estimate_preliminary_cell_track(
    track_id: str,
    first: CellDetection,
    second: CellDetection,
) -> CellTrackSnapshot:
    """Estimate exact interval motion for an externally associated pair.

    No displacement or speed is rejected here.  An extreme result remains an
    auditable measurement with correspondingly supplied uncertainty; it is
    evidence for the association graph, not permission to silently join two
    identities.
    """

    if not isinstance(track_id, str) or not track_id:
        raise ValueError("CellTrack vereist een niet-lege track_id")
    if not isinstance(first, CellDetection) or not isinstance(
        second, CellDetection
    ):
        raise TypeError("CellTrack-estimatie vereist twee CellDetection-objecten")
    if second.valid_at <= first.valid_at:
        raise ValueError(
            "de tweede CellDetection moet meteorologisch later geldig zijn"
        )
    if second.analysis_grid_id != first.analysis_grid_id:
        raise ValueError(
            "CellDetections moeten op exact hetzelfde analysis grid staan"
        )

    elapsed_seconds = second.valid_at - first.valid_at
    vx_m_s = (second.x_m - first.x_m) / elapsed_seconds
    vy_m_s = (second.y_m - first.y_m) / elapsed_seconds

    first_variance = first.position_sigma_m**2
    second_variance = second.position_sigma_m**2
    velocity_variance = (
        first_variance + second_variance
    ) / elapsed_seconds**2
    position_velocity_covariance = second_variance / elapsed_seconds
    zero = 0.0
    covariance = (
        (
            second_variance,
            zero,
            position_velocity_covariance,
            zero,
        ),
        (
            zero,
            second_variance,
            zero,
            position_velocity_covariance,
        ),
        (
            position_velocity_covariance,
            zero,
            velocity_variance,
            zero,
        ),
        (
            zero,
            position_velocity_covariance,
            zero,
            velocity_variance,
        ),
    )

    return CellTrackSnapshot(
        track_id=track_id,
        lifecycle_state="preliminary",
        first_detection_id=first.detection_id,
        latest_detection_id=second.detection_id,
        first_valid_at=first.valid_at,
        valid_at=second.valid_at,
        analysis_grid_id=first.analysis_grid_id,
        x_m=second.x_m,
        y_m=second.y_m,
        vx_m_s=vx_m_s,
        vy_m_s=vy_m_s,
        state_covariance=covariance,
    )
