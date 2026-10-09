"""Fail-closed WeatherSystem motion admission for the thin live path."""

from __future__ import annotations

from dataclasses import dataclass
import math
from statistics import median
from typing import Mapping, Protocol

from .tracking_core.engine.motion_consensus import MotionConsensusResult


class _MemberTrack(Protocol):
    track_id: str
    object_kind: str


class _Predecessor(Protocol):
    previous_system_id: str


class _WeatherSystem(Protocol):
    system_id: str
    member_tracks: tuple[_MemberTrack, ...]
    predecessor_candidates: tuple[_Predecessor, ...]
    system_uncertainty: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class SystemMotionAdmissionPolicy:
    policy_id: str = "candidate_policy_v1"
    maximum_member_difference_m_s: float = 15.0

    def __post_init__(self) -> None:
        if not self.policy_id:
            raise ValueError("system_motion_policy_id_invalid")
        if (
            not math.isfinite(self.maximum_member_difference_m_s)
            or self.maximum_member_difference_m_s <= 0.0
        ):
            raise ValueError("system_motion_difference_limit_invalid")


@dataclass(frozen=True, slots=True)
class SystemMotionAdmission:
    status: str
    confidence: str
    vx_m_s: float | None
    vy_m_s: float | None
    motion_ready: bool
    supporting_track_ids: tuple[str, ...]
    supporting_methods: tuple[str, ...]
    raw_member_vectors: tuple[tuple[str, float, float], ...]
    maximum_member_difference_m_s: float | None
    reason_codes: tuple[str, ...]
    lineage_status: str
    predecessor_system_ids: tuple[str, ...]


def admit_weather_system_motion(
    system: _WeatherSystem,
    member_consensus: Mapping[str, MotionConsensusResult],
    policy: SystemMotionAdmissionPolicy = SystemMotionAdmissionPolicy(),
) -> SystemMotionAdmission:
    """Admit one system vector only if every current rain shield is admitted."""

    supporting_tracks = tuple(sorted(
        member.track_id
        for member in system.member_tracks
        if member.object_kind == "rain_shield"
    ))
    predecessors = tuple(sorted(
        item.previous_system_id for item in system.predecessor_candidates
    ))
    ambiguous = (
        len(predecessors) > 1
        or "multiple_predecessor_system_candidates" in system.system_uncertainty
        or any(
            "ambiguous" in reason or "uncertain" in reason
            for reason in system.system_uncertainty
        )
    )
    lineage_status = "ambiguous" if ambiguous else (
        "continued" if predecessors else "birth"
    )
    missing = tuple(
        track_id for track_id in supporting_tracks
        if track_id not in member_consensus
    )
    admitted_statuses = {
        "ready_high", "ready_medium", "fallback", "stationary",
    }
    rejected = tuple(
        track_id for track_id in supporting_tracks
        if track_id in member_consensus
        and (
            member_consensus[track_id].status not in admitted_statuses
            or member_consensus[track_id].confidence not in {"high", "medium"}
            or member_consensus[track_id].vx_m_s is None
            or member_consensus[track_id].vy_m_s is None
        )
    )
    if not supporting_tracks or missing or rejected:
        reasons = []
        if not supporting_tracks:
            reasons.append("rain_shield_member_missing")
        if missing:
            reasons.append("member_consensus_missing")
        if rejected:
            reasons.append("member_consensus_rejected")
        return SystemMotionAdmission(
            status="indeterminate",
            confidence="indeterminate",
            vx_m_s=None,
            vy_m_s=None,
            motion_ready=False,
            supporting_track_ids=supporting_tracks,
            supporting_methods=(),
            raw_member_vectors=(),
            maximum_member_difference_m_s=None,
            reason_codes=tuple(sorted(reasons)),
            lineage_status=lineage_status,
            predecessor_system_ids=predecessors,
        )

    vectors = tuple(
        (
            track_id,
            float(member_consensus[track_id].vx_m_s),
            float(member_consensus[track_id].vy_m_s),
        )
        for track_id in supporting_tracks
    )
    maximum_difference = max(
        (
            math.hypot(left[1] - right[1], left[2] - right[2])
            for index, left in enumerate(vectors)
            for right in vectors[index + 1:]
        ),
        default=0.0,
    )
    methods = tuple(sorted({
        method
        for track_id in supporting_tracks
        for method in member_consensus[track_id].supporting_methods
    }))
    if maximum_difference > policy.maximum_member_difference_m_s:
        return SystemMotionAdmission(
            status="indeterminate",
            confidence="indeterminate",
            vx_m_s=None,
            vy_m_s=None,
            motion_ready=False,
            supporting_track_ids=supporting_tracks,
            supporting_methods=methods,
            raw_member_vectors=vectors,
            maximum_member_difference_m_s=maximum_difference,
            reason_codes=("member_motion_incoherent",),
            lineage_status=lineage_status,
            predecessor_system_ids=predecessors,
        )

    confidence = "high" if all(
        member_consensus[track_id].confidence == "high"
        for track_id in supporting_tracks
    ) else "medium"
    return SystemMotionAdmission(
        status="ready_high" if confidence == "high" else "ready_medium",
        confidence=confidence,
        vx_m_s=float(median(item[1] for item in vectors)),
        vy_m_s=float(median(item[2] for item in vectors)),
        motion_ready=True,
        supporting_track_ids=supporting_tracks,
        supporting_methods=methods,
        raw_member_vectors=vectors,
        maximum_member_difference_m_s=maximum_difference,
        reason_codes=(),
        lineage_status=lineage_status,
        predecessor_system_ids=predecessors,
    )


__all__ = [
    "SystemMotionAdmission",
    "SystemMotionAdmissionPolicy",
    "admit_weather_system_motion",
]
