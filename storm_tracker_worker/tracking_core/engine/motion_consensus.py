"""Conservative consensus for correlated precipitation-motion estimates.

The candidates normally originate from the same radar frames.  They are not
independent measurements, so this module never performs an inverse-covariance
fusion that would make the result spuriously certain.  Local optical flow is
the primary vector; object-track motion is a consistency check and bounded
fallback.  Diagnostic global/centroid candidates remain visible but cannot
admit a vector by themselves.
"""
from __future__ import annotations

from dataclasses import dataclass
import math
from typing import TYPE_CHECKING, Iterable

if TYPE_CHECKING:
    from .cell_track_engine import CellTrackTransitionResult


Covariance2D = tuple[tuple[float, float], tuple[float, float]]
_ROLES = frozenset({
    "optical_flow",
    "object_track",
    "global_match",
    "intensity_centroid",
})


def _finite_number(value: object, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{label} moet numeriek zijn")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"{label} moet eindig zijn")
    return result


def _validate_covariance(value: Covariance2D) -> None:
    if (
        not isinstance(value, tuple)
        or len(value) != 2
        or any(not isinstance(row, tuple) or len(row) != 2 for row in value)
    ):
        raise ValueError("covariance moet een immutable 2x2-matrix zijn")
    a = _finite_number(value[0][0], "covariance")
    b = _finite_number(value[0][1], "covariance")
    c = _finite_number(value[1][0], "covariance")
    d = _finite_number(value[1][1], "covariance")
    if not math.isclose(b, c, rel_tol=0.0, abs_tol=1e-12):
        raise ValueError("covariance moet symmetrisch zijn")
    if a < 0.0 or d < 0.0 or a * d - b * c < -1e-12:
        raise ValueError("covariance moet positief semidefiniet zijn")


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
            raise ValueError("motion method is ongeldig")
        if self.role not in _ROLES:
            raise ValueError("motion role is onbekend")
        _finite_number(self.vx_m_s, "vx_m_s")
        _finite_number(self.vy_m_s, "vy_m_s")
        _validate_covariance(self.covariance_m2_s2)
        if (
            isinstance(self.support_count, bool)
            or not isinstance(self.support_count, int)
            or self.support_count < 0
        ):
            raise ValueError("support_count is ongeldig")
        fraction = _finite_number(self.support_fraction, "support_fraction")
        if not 0.0 <= fraction <= 1.0:
            raise ValueError("support_fraction ligt buiten [0, 1]")
        if (
            isinstance(self.history_samples, bool)
            or not isinstance(self.history_samples, int)
            or self.history_samples < 0
        ):
            raise ValueError("history_samples is ongeldig")
        if self.hindcast_skill is not None:
            skill = _finite_number(self.hindcast_skill, "hindcast_skill")
            if not 0.0 <= skill <= 1.0:
                raise ValueError("hindcast_skill ligt buiten [0, 1]")

    @property
    def speed_m_s(self) -> float:
        return math.hypot(self.vx_m_s, self.vy_m_s)

    @property
    def heading_deg(self) -> float | None:
        if self.speed_m_s < 1e-12:
            return None
        return (math.degrees(math.atan2(self.vx_m_s, self.vy_m_s)) + 360.0) % 360.0


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

    def __post_init__(self) -> None:
        for name in (
            "minimum_motion_speed_m_s",
            "maximum_angle_difference_deg",
            "maximum_speed_ratio",
            "maximum_normalized_difference",
            "minimum_flow_support_fraction",
            "minimum_hindcast_skill",
        ):
            value = _finite_number(getattr(self, name), name)
            if value < 0.0:
                raise ValueError(f"{name} moet niet-negatief zijn")
        if self.maximum_angle_difference_deg > 180.0:
            raise ValueError("maximum_angle_difference_deg is te groot")
        if self.maximum_speed_ratio < 1.0:
            raise ValueError("maximum_speed_ratio moet minstens 1 zijn")
        if self.minimum_flow_support_fraction > 1.0:
            raise ValueError("minimum_flow_support_fraction is te groot")
        if self.minimum_hindcast_skill > 1.0:
            raise ValueError("minimum_hindcast_skill is te groot")
        for name in (
            "minimum_flow_support_count",
            "minimum_fallback_history_samples",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"{name} moet een positief geheel getal zijn")


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


def _angle_difference(left: MotionVectorCandidate, right: MotionVectorCandidate) -> float:
    assert left.heading_deg is not None and right.heading_deg is not None
    return abs((left.heading_deg - right.heading_deg + 180.0) % 360.0 - 180.0)


def _normalized_difference(
    left: MotionVectorCandidate,
    right: MotionVectorCandidate,
) -> float:
    a = left.covariance_m2_s2[0][0] + right.covariance_m2_s2[0][0]
    b = left.covariance_m2_s2[0][1] + right.covariance_m2_s2[0][1]
    d = left.covariance_m2_s2[1][1] + right.covariance_m2_s2[1][1]
    dx = left.vx_m_s - right.vx_m_s
    dy = left.vy_m_s - right.vy_m_s
    determinant = a * d - b * b
    if determinant <= 1e-12:
        return math.inf if math.hypot(dx, dy) > 1e-12 else 0.0
    return (d * dx * dx - 2.0 * b * dx * dy + a * dy * dy) / determinant


def _best(candidates: tuple[MotionVectorCandidate, ...]) -> MotionVectorCandidate:
    return max(
        candidates,
        key=lambda item: (
            item.hindcast_skill if item.hindcast_skill is not None else -1.0,
            item.support_fraction,
            item.support_count,
            item.history_samples,
            item.method,
        ),
    )


def _with_vector(
    candidate: MotionVectorCandidate,
    *,
    status: str,
    confidence: str,
    supporting_methods: tuple[str, ...],
    reasons: tuple[str, ...] = (),
    angle: float | None = None,
    speed_ratio: float | None = None,
    normalized: float | None = None,
) -> MotionConsensusResult:
    return MotionConsensusResult(
        status=status,
        confidence=confidence,
        vx_m_s=candidate.vx_m_s,
        vy_m_s=candidate.vy_m_s,
        speed_m_s=candidate.speed_m_s,
        heading_deg=candidate.heading_deg,
        selected_method=candidate.method,
        supporting_methods=supporting_methods,
        agreement_angle_deg=angle,
        speed_ratio=speed_ratio,
        normalized_difference=normalized,
        reason_codes=tuple(sorted(set(reasons))),
    )


def _without_vector(
    status: str,
    reasons: tuple[str, ...],
    *,
    methods: tuple[str, ...] = (),
    angle: float | None = None,
    speed_ratio: float | None = None,
    normalized: float | None = None,
) -> MotionConsensusResult:
    return MotionConsensusResult(
        status=status,
        confidence="indeterminate",
        vx_m_s=None,
        vy_m_s=None,
        speed_m_s=None,
        heading_deg=None,
        selected_method=None,
        supporting_methods=methods,
        agreement_angle_deg=angle,
        speed_ratio=speed_ratio,
        normalized_difference=normalized,
        reason_codes=tuple(sorted(set(reasons))),
    )


def select_motion_consensus(
    candidates: Iterable[MotionVectorCandidate],
    policy: MotionConsensusPolicy = MotionConsensusPolicy(),
) -> MotionConsensusResult:
    """Select a supported vector without claiming candidate independence."""

    values = tuple(candidates)
    if any(not isinstance(item, MotionVectorCandidate) for item in values):
        raise TypeError("motion candidates bevatten een fout type")
    if not isinstance(policy, MotionConsensusPolicy):
        raise TypeError("motion consensus policy heeft een fout type")

    reasons: list[str] = []
    flows = tuple(
        item
        for item in values
        if item.role == "optical_flow"
        and item.support_count >= policy.minimum_flow_support_count
        and item.support_fraction >= policy.minimum_flow_support_fraction
    )
    if any(item.role == "optical_flow" for item in values) and not flows:
        reasons.append("optical_flow_support_insufficient")
    tracks = tuple(item for item in values if item.role == "object_track")
    flow = _best(flows) if flows else None
    track = _best(tracks) if tracks else None

    if flow is not None and track is not None:
        methods = tuple(sorted((flow.method, track.method)))
        if (
            flow.speed_m_s < policy.minimum_motion_speed_m_s
            and track.speed_m_s < policy.minimum_motion_speed_m_s
        ):
            return MotionConsensusResult(
                status="stationary",
                confidence="high",
                vx_m_s=0.0,
                vy_m_s=0.0,
                speed_m_s=0.0,
                heading_deg=None,
                selected_method=flow.method,
                supporting_methods=methods,
                agreement_angle_deg=None,
                speed_ratio=1.0,
                normalized_difference=_normalized_difference(flow, track),
                reason_codes=("both_estimators_below_motion_threshold",),
            )
        if min(flow.speed_m_s, track.speed_m_s) < policy.minimum_motion_speed_m_s:
            return _without_vector(
                "indeterminate",
                ("motion_candidates_disagree", "stationary_motion_conflict"),
                methods=methods,
            )
        angle = _angle_difference(flow, track)
        ratio = max(flow.speed_m_s, track.speed_m_s) / min(
            flow.speed_m_s,
            track.speed_m_s,
        )
        normalized = _normalized_difference(flow, track)
        if (
            angle > policy.maximum_angle_difference_deg
            or ratio > policy.maximum_speed_ratio
            or normalized > policy.maximum_normalized_difference
        ):
            return _without_vector(
                "indeterminate",
                ("motion_candidates_disagree",),
                methods=methods,
                angle=angle,
                speed_ratio=ratio,
                normalized=normalized,
            )
        hindcast_ready = all(
            item.hindcast_skill is not None
            and item.hindcast_skill >= policy.minimum_hindcast_skill
            for item in (flow, track)
        )
        return _with_vector(
            flow,
            status="ready_high" if hindcast_ready else "ready_medium",
            confidence="high" if hindcast_ready else "medium",
            supporting_methods=methods,
            reasons=tuple(reasons),
            angle=angle,
            speed_ratio=ratio,
            normalized=normalized,
        )

    if flow is not None:
        return _with_vector(
            flow,
            status="ready_medium",
            confidence="medium",
            supporting_methods=(flow.method,),
            reasons=tuple((*reasons, "object_track_unavailable")),
        )

    reasons.append("optical_flow_unavailable")
    if track is not None:
        if track.history_samples < policy.minimum_fallback_history_samples:
            return _without_vector(
                "indeterminate",
                tuple((*reasons, "fallback_history_insufficient")),
                methods=(track.method,),
            )
        if track.speed_m_s < policy.minimum_motion_speed_m_s:
            return MotionConsensusResult(
                status="stationary",
                confidence="medium",
                vx_m_s=0.0,
                vy_m_s=0.0,
                speed_m_s=0.0,
                heading_deg=None,
                selected_method=track.method,
                supporting_methods=(track.method,),
                agreement_angle_deg=None,
                speed_ratio=None,
                normalized_difference=None,
                reason_codes=tuple(sorted(set((*reasons, "fallback_stationary")))),
            )
        return _with_vector(
            track,
            status="fallback",
            confidence="medium",
            supporting_methods=(track.method,),
            reasons=tuple(reasons),
        )

    return _without_vector(
        "indeterminate",
        tuple((*reasons, "no_admissible_motion_candidate")),
        methods=tuple(sorted(item.method for item in values)),
    )


def build_transition_motion_consensus(
    transition: "CellTrackTransitionResult",
    policy: MotionConsensusPolicy = MotionConsensusPolicy(),
) -> tuple[tuple[str, MotionConsensusResult], ...]:
    """Bind last-pair flow and recursive motion to each current rain shield.

    Flow aggregations belong to the previous detection footprint, whereas the
    recursive estimate belongs to the current track revision.  Binding through
    the track's penultimate detection preserves that temporal meaning and
    avoids accidentally applying one cell's flow to another cell.
    """

    from .cell_track_engine import CellTrackTransitionResult

    if not isinstance(transition, CellTrackTransitionResult):
        raise TypeError("motion consensus vereist CellTrackTransitionResult")
    if transition.status != "advanced":
        return ()
    state = transition.next_state
    flow_by_detection = {
        item.detection_id: item for item in transition.flow_aggregations
    }
    output: list[tuple[str, MotionConsensusResult]] = []
    for track in state.ledger.tracks:
        if (
            track.lifecycle_state != "confirmed"
            or track.sample_count < 2
            or state.last_frame is None
            or not math.isclose(
                track.latest_detection.valid_at,
                state.last_frame.valid_at,
                rel_tol=0.0,
                abs_tol=1e-6,
            )
        ):
            continue
        detected = state.hierarchy(track.track_id)
        if detected.object_kind != "rain_shield":
            continue
        estimate = state.estimate(track.track_id)
        candidates: list[MotionVectorCandidate] = []
        previous_detection_id = track.detections[-2].detection_id
        aggregation = flow_by_detection.get(previous_detection_id)
        if aggregation is not None and aggregation.sample is not None:
            sample = aggregation.sample
            candidates.append(MotionVectorCandidate(
                method=sample.method,
                role="optical_flow",
                vx_m_s=sample.vx_m_s,
                vy_m_s=sample.vy_m_s,
                covariance_m2_s2=sample.velocity_covariance_m2_s2,
                support_count=aggregation.valid_pixels,
                support_fraction=aggregation.valid_fraction,
                history_samples=track.sample_count,
                hindcast_skill=None,
            ))
        velocity_covariance = (
            (estimate.covariance[2][2], estimate.covariance[2][3]),
            (estimate.covariance[3][2], estimate.covariance[3][3]),
        )
        candidates.append(MotionVectorCandidate(
            method=estimate.method,
            role="object_track",
            vx_m_s=estimate.vx_m_s,
            vy_m_s=estimate.vy_m_s,
            covariance_m2_s2=velocity_covariance,
            support_count=track.sample_count,
            support_fraction=1.0,
            history_samples=track.sample_count,
            hindcast_skill=None,
        ))
        output.append((
            track.track_id,
            select_motion_consensus(tuple(candidates), policy),
        ))
    return tuple(sorted(output, key=lambda item: item[0]))


__all__ = [
    "MotionConsensusPolicy",
    "MotionConsensusResult",
    "MotionVectorCandidate",
    "build_transition_motion_consensus",
    "select_motion_consensus",
]
