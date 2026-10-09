"""Conservative precipitation-motion consensus for correlated estimates.

Optical flow and object tracking are derived from the same radar frames and
are therefore not independent measurements.  This module uses local optical
flow as the primary vector and object motion as a consistency check or bounded
fallback.  It never averages compass headings and fails closed on material
disagreement.
"""

from __future__ import annotations

from dataclasses import dataclass
import math
from typing import TYPE_CHECKING, Iterable

if TYPE_CHECKING:
    from .cell_track_engine import CellTrackTransitionResult


Covariance2D = tuple[tuple[float, float], tuple[float, float]]
_ROLES = frozenset({
    "optical_flow", "object_track", "global_match", "intensity_centroid",
})


def _finite(value: object, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{label} must be numeric")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"{label} must be finite")
    return result


def _covariance(value: Covariance2D) -> None:
    if (
        not isinstance(value, tuple)
        or len(value) != 2
        or any(not isinstance(row, tuple) or len(row) != 2 for row in value)
    ):
        raise ValueError("motion covariance must be an immutable 2x2 matrix")
    a, b = _finite(value[0][0], "covariance"), _finite(value[0][1], "covariance")
    c, d = _finite(value[1][0], "covariance"), _finite(value[1][1], "covariance")
    if not math.isclose(b, c, rel_tol=0.0, abs_tol=1e-12):
        raise ValueError("motion covariance must be symmetric")
    if a < 0.0 or d < 0.0 or a * d - b * c < -1e-12:
        raise ValueError("motion covariance must be positive semidefinite")


@dataclass(frozen=True, slots=True)
class MotionVectorCandidate:
    method: str
    role: str
    vx_m_s: float
    vy_m_s: float
    covariance_m2_s2: Covariance2D
    support_count: int
    support_fraction: float
    history_samples: int
    hindcast_skill: float | None

    def __post_init__(self) -> None:
        if not isinstance(self.method, str) or not self.method:
            raise ValueError("motion method is invalid")
        if self.role not in _ROLES:
            raise ValueError("motion role is invalid")
        _finite(self.vx_m_s, "vx_m_s")
        _finite(self.vy_m_s, "vy_m_s")
        _covariance(self.covariance_m2_s2)
        if isinstance(self.support_count, bool) or not isinstance(self.support_count, int) or self.support_count < 0:
            raise ValueError("motion support_count is invalid")
        if not 0.0 <= _finite(self.support_fraction, "support_fraction") <= 1.0:
            raise ValueError("motion support_fraction is invalid")
        if isinstance(self.history_samples, bool) or not isinstance(self.history_samples, int) or self.history_samples < 0:
            raise ValueError("motion history_samples is invalid")
        if self.hindcast_skill is not None and not 0.0 <= _finite(self.hindcast_skill, "hindcast_skill") <= 1.0:
            raise ValueError("motion hindcast_skill is invalid")

    @property
    def speed_m_s(self) -> float:
        return math.hypot(self.vx_m_s, self.vy_m_s)

    @property
    def heading_deg(self) -> float | None:
        if self.speed_m_s < 1e-12:
            return None
        return math.degrees(math.atan2(self.vx_m_s, self.vy_m_s)) % 360.0


@dataclass(frozen=True, slots=True)
class MotionConsensusPolicy:
    minimum_motion_speed_m_s: float = 0.5
    maximum_angle_difference_deg: float = 45.0
    maximum_speed_ratio: float = 2.0
    maximum_normalized_difference: float = 9.21
    minimum_flow_support_count: int = 3
    minimum_flow_support_fraction: float = 0.10
    minimum_fallback_history_samples: int = 3
    minimum_hindcast_skill: float = 0.15


@dataclass(frozen=True, slots=True)
class MotionConsensusResult:
    status: str
    confidence: str
    vx_m_s: float | None
    vy_m_s: float | None
    speed_m_s: float | None
    heading_deg: float | None
    selected_method: str | None
    supporting_methods: tuple[str, ...]
    agreement_angle_deg: float | None
    speed_ratio: float | None
    normalized_difference: float | None
    reason_codes: tuple[str, ...]


def _best(values: tuple[MotionVectorCandidate, ...]) -> MotionVectorCandidate:
    return max(values, key=lambda item: (
        item.hindcast_skill if item.hindcast_skill is not None else -1.0,
        item.support_fraction, item.support_count, item.history_samples, item.method,
    ))


def _angle(left: MotionVectorCandidate, right: MotionVectorCandidate) -> float:
    assert left.heading_deg is not None and right.heading_deg is not None
    return abs((left.heading_deg - right.heading_deg + 180.0) % 360.0 - 180.0)


def _normalized(left: MotionVectorCandidate, right: MotionVectorCandidate) -> float:
    a = left.covariance_m2_s2[0][0] + right.covariance_m2_s2[0][0]
    b = left.covariance_m2_s2[0][1] + right.covariance_m2_s2[0][1]
    d = left.covariance_m2_s2[1][1] + right.covariance_m2_s2[1][1]
    dx, dy = left.vx_m_s - right.vx_m_s, left.vy_m_s - right.vy_m_s
    determinant = a * d - b * b
    if determinant <= 1e-12:
        return math.inf if math.hypot(dx, dy) > 1e-12 else 0.0
    return (d * dx * dx - 2.0 * b * dx * dy + a * dy * dy) / determinant


def _without_vector(
    reasons: Iterable[str],
    *,
    methods: tuple[str, ...] = (),
    angle: float | None = None,
    ratio: float | None = None,
    normalized: float | None = None,
) -> MotionConsensusResult:
    return MotionConsensusResult(
        "indeterminate", "indeterminate", None, None, None, None, None,
        tuple(sorted(set(methods))), angle, ratio, normalized,
        tuple(sorted(set(reasons))),
    )


def _with_vector(
    candidate: MotionVectorCandidate,
    *,
    status: str,
    confidence: str,
    methods: tuple[str, ...],
    reasons: Iterable[str] = (),
    angle: float | None = None,
    ratio: float | None = None,
    normalized: float | None = None,
) -> MotionConsensusResult:
    return MotionConsensusResult(
        status, confidence, candidate.vx_m_s, candidate.vy_m_s,
        candidate.speed_m_s, candidate.heading_deg, candidate.method,
        tuple(sorted(set(methods))), angle, ratio, normalized,
        tuple(sorted(set(reasons))),
    )


def select_motion_consensus(
    candidates: Iterable[MotionVectorCandidate],
    policy: MotionConsensusPolicy = MotionConsensusPolicy(),
) -> MotionConsensusResult:
    """Admit one vector only when its correlated evidence is coherent."""

    values = tuple(candidates)
    if any(not isinstance(item, MotionVectorCandidate) for item in values):
        raise TypeError("motion candidates contain an invalid type")
    flows = tuple(item for item in values if (
        item.role == "optical_flow"
        and item.support_count >= policy.minimum_flow_support_count
        and item.support_fraction >= policy.minimum_flow_support_fraction
    ))
    tracks = tuple(item for item in values if item.role == "object_track")
    reasons: list[str] = []
    if any(item.role == "optical_flow" for item in values) and not flows:
        reasons.append("optical_flow_support_insufficient")
    flow = _best(flows) if flows else None
    track = _best(tracks) if tracks else None
    if flow is not None and track is not None:
        methods = (flow.method, track.method)
        if flow.speed_m_s < policy.minimum_motion_speed_m_s and track.speed_m_s < policy.minimum_motion_speed_m_s:
            return MotionConsensusResult(
                "stationary", "high", 0.0, 0.0, 0.0, None, flow.method,
                tuple(sorted(set(methods))), None, 1.0, _normalized(flow, track),
                ("both_estimators_below_motion_threshold",),
            )
        if min(flow.speed_m_s, track.speed_m_s) < policy.minimum_motion_speed_m_s:
            return _without_vector(
                ("motion_candidates_disagree", "stationary_motion_conflict"),
                methods=methods,
            )
        angle = _angle(flow, track)
        ratio = max(flow.speed_m_s, track.speed_m_s) / min(flow.speed_m_s, track.speed_m_s)
        normalized = _normalized(flow, track)
        if (
            angle > policy.maximum_angle_difference_deg
            or ratio > policy.maximum_speed_ratio
            or normalized > policy.maximum_normalized_difference
        ):
            return _without_vector(
                ("motion_candidates_disagree",), methods=methods,
                angle=angle, ratio=ratio, normalized=normalized,
            )
        skilled = all(
            item.hindcast_skill is not None
            and item.hindcast_skill >= policy.minimum_hindcast_skill
            for item in (flow, track)
        )
        return _with_vector(
            flow,
            status="ready_high" if skilled else "ready_medium",
            confidence="high" if skilled else "medium",
            methods=methods,
            reasons=reasons,
            angle=angle,
            ratio=ratio,
            normalized=normalized,
        )
    if flow is not None:
        return _with_vector(
            flow, status="ready_medium", confidence="medium",
            methods=(flow.method,), reasons=(*reasons, "object_track_unavailable"),
        )
    reasons.append("optical_flow_unavailable")
    if track is not None and track.history_samples >= policy.minimum_fallback_history_samples:
        if track.speed_m_s < policy.minimum_motion_speed_m_s:
            return MotionConsensusResult(
                "stationary", "medium", 0.0, 0.0, 0.0, None, track.method,
                (track.method,), None, None, None,
                tuple(sorted(set((*reasons, "fallback_stationary")))),
            )
        return _with_vector(
            track, status="fallback", confidence="medium",
            methods=(track.method,), reasons=reasons,
        )
    if track is not None:
        reasons.append("fallback_history_insufficient")
    else:
        reasons.append("no_admissible_motion_candidate")
    return _without_vector(reasons, methods=tuple(item.method for item in values))


def build_transition_motion_consensus(
    transition: "CellTrackTransitionResult",
    policy: MotionConsensusPolicy = MotionConsensusPolicy(),
) -> tuple[tuple[str, MotionConsensusResult], ...]:
    """Bind each current rain shield to its own flow and track estimate."""

    if transition.status != "advanced":
        return ()
    state = transition.next_state
    flow_by_detection = {item.detection_id: item for item in transition.flow_aggregations}
    results: list[tuple[str, MotionConsensusResult]] = []
    for track in state.ledger.tracks:
        if (
            track.lifecycle_state != "confirmed"
            or track.sample_count < 2
            or state.last_frame is None
            or not math.isclose(
                track.latest_detection.valid_at, state.last_frame.valid_at,
                rel_tol=0.0, abs_tol=1e-6,
            )
            or state.hierarchy(track.track_id).object_kind != "rain_shield"
        ):
            continue
        estimate = state.estimate(track.track_id)
        candidates: list[MotionVectorCandidate] = []
        aggregation = flow_by_detection.get(track.detections[-2].detection_id)
        if aggregation is not None and aggregation.sample is not None:
            sample = aggregation.sample
            candidates.append(MotionVectorCandidate(
                sample.method, "optical_flow", sample.vx_m_s, sample.vy_m_s,
                sample.velocity_covariance_m2_s2,
                aggregation.valid_pixels, aggregation.valid_fraction,
                track.sample_count, None,
            ))
        candidates.append(MotionVectorCandidate(
            estimate.method, "object_track", estimate.vx_m_s, estimate.vy_m_s,
            (
                (estimate.covariance[2][2], estimate.covariance[2][3]),
                (estimate.covariance[3][2], estimate.covariance[3][3]),
            ),
            track.sample_count, 1.0, track.sample_count, None,
        ))
        results.append((track.track_id, select_motion_consensus(candidates, policy)))
    return tuple(sorted(results, key=lambda item: item[0]))


__all__ = [
    "MotionConsensusPolicy", "MotionConsensusResult", "MotionVectorCandidate",
    "build_transition_motion_consensus", "select_motion_consensus",
]
