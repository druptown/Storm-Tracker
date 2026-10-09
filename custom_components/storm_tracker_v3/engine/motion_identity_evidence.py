"""Fail-closed identity semantics for shadow cell-association replays.

The global assignment solver must choose a mathematically best solution, even
when the supplied motion evidence is unavailable or weak.  That solver output
is a hypothesis, not proof that two detections are the same meteorological
cell.  This module preserves every candidate and retained solution while
making that distinction explicit.

No speed or displacement threshold is used here.  Extreme candidates remain
available for inspection and can still be selected by the caller-owned cost
model.  Independent validation is deliberately required before any match can
be described as confirmed identity.
"""
from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from typing import Iterable

from .association_features import (
    AssociationFeatureTable,
    AssociationFeatureVector,
)
from .global_assignment import (
    AssignmentEdgeCost,
    GlobalAssignmentResult,
)


MOTION_IDENTITY_EVIDENCE_VERSION = (
    "stv3-shadow-motion-identity-evidence-v1"
)
_MOTION_EVIDENCE_STATUSES = frozenset({
    "available",
    "unavailable",
    "not_assessed",
})
_IDENTITY_STATUSES = frozenset({
    "indeterminate_motion_not_assessed",
    "indeterminate_motion_unavailable",
    "indeterminate_alternatives_not_available",
    "indeterminate_within_caller_ambiguity_delta",
    "supported_hypothesis_not_validated",
})


@dataclass(frozen=True, slots=True)
class MotionEvidenceRecord:
    """Caller-owned evidence assessment for one previous detection."""

    previous_detection_id: str
    target_valid_at: float
    status: str
    reason: str
    method: str
    support_frame_ids: tuple[str, ...]
    selected_vectors: int | None = None
    usable_vectors: int | None = None
    usable_fraction: float | None = None

    def __post_init__(self) -> None:
        for label, value in (
            ("previous_detection_id", self.previous_detection_id),
            ("reason", self.reason),
            ("method", self.method),
        ):
            if not isinstance(value, str) or not value:
                raise ValueError(f"MotionEvidenceRecord vereist {label}")
        if self.status not in _MOTION_EVIDENCE_STATUSES:
            raise ValueError("onbekende motion-evidencestatus")
        if not math.isfinite(self.target_valid_at):
            raise ValueError("motion-evidence target_valid_at moet eindig zijn")
        if (
            not isinstance(self.support_frame_ids, tuple)
            or not self.support_frame_ids
            or any(
                not isinstance(item, str) or not item
                for item in self.support_frame_ids
            )
            or len(self.support_frame_ids) != len(set(self.support_frame_ids))
        ):
            raise ValueError(
                "motion-evidence vereist unieke immutable support_frame_ids"
            )
        optional_counts = (
            ("selected_vectors", self.selected_vectors),
            ("usable_vectors", self.usable_vectors),
        )
        for label, value in optional_counts:
            if value is not None and (
                isinstance(value, bool)
                or not isinstance(value, int)
                or value < 0
            ):
                raise ValueError(f"{label} moet niet-negatief geheel zijn")
        if (
            self.selected_vectors is not None
            and self.usable_vectors is not None
            and self.usable_vectors > self.selected_vectors
        ):
            raise ValueError("usable_vectors kan selected_vectors niet overtreffen")
        if self.usable_fraction is not None and (
            not math.isfinite(self.usable_fraction)
            or self.usable_fraction < 0.0
            or self.usable_fraction > 1.0
        ):
            raise ValueError("usable_fraction moet in [0,1] liggen")
        if self.status == "available" and (
            self.usable_vectors is not None and self.usable_vectors == 0
        ):
            raise ValueError("beschikbare motion-evidence vereist bruikbare vectoren")

    @property
    def fingerprint(self) -> str:
        return _fingerprint({
            "previous_detection_id": self.previous_detection_id,
            "target_valid_at": self.target_valid_at.hex(),
            "status": self.status,
            "reason": self.reason,
            "method": self.method,
            "support_frame_ids": list(self.support_frame_ids),
            "selected_vectors": self.selected_vectors,
            "usable_vectors": self.usable_vectors,
            "usable_fraction": (
                self.usable_fraction.hex()
                if self.usable_fraction is not None
                else None
            ),
        })

    def to_dict(self) -> dict:
        return {
            "previous_detection_id": self.previous_detection_id,
            "target_valid_at": self.target_valid_at,
            "status": self.status,
            "reason": self.reason,
            "method": self.method,
            "support_frame_ids": list(self.support_frame_ids),
            "selected_vectors": self.selected_vectors,
            "usable_vectors": self.usable_vectors,
            "usable_fraction": self.usable_fraction,
            "fingerprint": self.fingerprint,
        }


@dataclass(frozen=True, slots=True)
class IdentityCandidateAudit:
    """One complete previous/current edge retained as a hypothesis."""

    previous_detection_id: str
    current_detection_id: str
    cost: float
    cost_model_id: str
    cost_components: tuple[tuple[str, float], ...]
    observed_interval_speed_kmh: float
    residual_distance_m: float
    mahalanobis_squared: float
    absolute_log_area_ratio: float
    footprint_iou_unadvected: float
    flow_sample_id: str
    flow_method: str
    previous_cost_rank: int
    current_cost_rank: int

    def to_dict(self) -> dict:
        return {
            "previous_detection_id": self.previous_detection_id,
            "current_detection_id": self.current_detection_id,
            "cost": self.cost,
            "cost_model_id": self.cost_model_id,
            "cost_components": [
                {"name": name, "value": value}
                for name, value in self.cost_components
            ],
            "observed_interval_speed_kmh":
                self.observed_interval_speed_kmh,
            "residual_distance_m": self.residual_distance_m,
            "mahalanobis_squared": self.mahalanobis_squared,
            "absolute_log_area_ratio": self.absolute_log_area_ratio,
            "footprint_iou_unadvected": self.footprint_iou_unadvected,
            "flow_sample_id": self.flow_sample_id,
            "flow_method": self.flow_method,
            "previous_cost_rank": self.previous_cost_rank,
            "current_cost_rank": self.current_cost_rank,
        }


@dataclass(frozen=True, slots=True)
class AssignmentHypothesisAudit:
    """One exact solution retained by the bounded assignment solver."""

    rank: int
    total_cost: float
    matches: tuple[tuple[str, str], ...]
    deaths: tuple[str, ...]
    births: tuple[str, ...]

    def to_dict(self) -> dict:
        return {
            "rank": self.rank,
            "total_cost": self.total_cost,
            "matches": [list(item) for item in self.matches],
            "deaths": list(self.deaths),
            "births": list(self.births),
        }


@dataclass(frozen=True, slots=True)
class MatchIdentityAssessment:
    """Evidence semantics for one match in the one-best hypothesis."""

    previous_detection_id: str
    current_detection_id: str
    status: str
    reason_codes: tuple[str, ...]
    motion_evidence_status: str
    motion_evidence_reason: str
    motion_evidence_fingerprint: str
    selected_cost: float
    observed_interval_speed_kmh: float
    local_next_edge_cost_gap: float | None
    match_break_cost_delta: float
    global_alternative_cost_gap: float | None
    appears_in_all_retained_solutions: bool | None
    confirmed_identity: bool = False

    def __post_init__(self) -> None:
        if self.status not in _IDENTITY_STATUSES:
            raise ValueError("onbekende identity-assessmentstatus")
        if self.motion_evidence_status not in _MOTION_EVIDENCE_STATUSES:
            raise ValueError("onbekende motion-evidencestatus")
        if self.confirmed_identity:
            raise ValueError(
                "shadow-associatie mag zonder onafhankelijke waarheid "
                "geen identiteit bevestigen"
            )

    def to_dict(self) -> dict:
        return {
            "previous_detection_id": self.previous_detection_id,
            "current_detection_id": self.current_detection_id,
            "status": self.status,
            "reason_codes": list(self.reason_codes),
            "motion_evidence_status": self.motion_evidence_status,
            "motion_evidence_reason": self.motion_evidence_reason,
            "motion_evidence_fingerprint":
                self.motion_evidence_fingerprint,
            "selected_cost": self.selected_cost,
            "observed_interval_speed_kmh":
                self.observed_interval_speed_kmh,
            "local_next_edge_cost_gap": self.local_next_edge_cost_gap,
            "match_break_cost_delta": self.match_break_cost_delta,
            "global_alternative_cost_gap":
                self.global_alternative_cost_gap,
            "appears_in_all_retained_solutions":
                self.appears_in_all_retained_solutions,
            "confirmed_identity": self.confirmed_identity,
        }


@dataclass(frozen=True, slots=True)
class ShadowIdentityFrameAssessment:
    """Lossless, fail-closed identity audit for one frame transition."""

    previous_valid_at: float
    current_valid_at: float
    candidates: tuple[IdentityCandidateAudit, ...]
    solutions: tuple[AssignmentHypothesisAudit, ...]
    motion_evidence: tuple[MotionEvidenceRecord, ...]
    best_match_assessments: tuple[MatchIdentityAssessment, ...]
    candidate_evidence_sha256: str
    caller_ambiguity_cost_delta: float
    solver: str
    assignment_algorithm_version: str
    alternatives_available: bool
    all_candidates_retained: bool
    identity_confirmation_claimed: bool = False
    speed_cap_kmh: None = None
    candidate_rejection_by_speed: bool = False
    algorithm_version: str = MOTION_IDENTITY_EVIDENCE_VERSION

    @property
    def indeterminate_match_count(self) -> int:
        return sum(
            item.status.startswith("indeterminate_")
            for item in self.best_match_assessments
        )

    @property
    def supported_unvalidated_match_count(self) -> int:
        return sum(
            item.status == "supported_hypothesis_not_validated"
            for item in self.best_match_assessments
        )

    @property
    def motion_unavailable_match_count(self) -> int:
        return sum(
            item.motion_evidence_status == "unavailable"
            for item in self.best_match_assessments
        )

    def to_dict(self, *, include_candidates: bool = True) -> dict:
        result = {
            "previous_valid_at": self.previous_valid_at,
            "current_valid_at": self.current_valid_at,
            "candidate_count": len(self.candidates),
            "solution_count": len(self.solutions),
            "motion_evidence_count": len(self.motion_evidence),
            "best_match_count": len(self.best_match_assessments),
            "indeterminate_match_count": self.indeterminate_match_count,
            "supported_unvalidated_match_count":
                self.supported_unvalidated_match_count,
            "motion_unavailable_match_count":
                self.motion_unavailable_match_count,
            "candidate_evidence_sha256": self.candidate_evidence_sha256,
            "caller_ambiguity_cost_delta":
                self.caller_ambiguity_cost_delta,
            "solver": self.solver,
            "assignment_algorithm_version":
                self.assignment_algorithm_version,
            "alternatives_available": self.alternatives_available,
            "all_candidates_retained": self.all_candidates_retained,
            "identity_confirmation_claimed":
                self.identity_confirmation_claimed,
            "speed_cap_kmh": self.speed_cap_kmh,
            "candidate_rejection_by_speed":
                self.candidate_rejection_by_speed,
            "motion_evidence": [
                item.to_dict() for item in self.motion_evidence
            ],
            "solutions": [item.to_dict() for item in self.solutions],
            "best_match_assessments": [
                item.to_dict() for item in self.best_match_assessments
            ],
            "algorithm_version": self.algorithm_version,
        }
        if include_candidates:
            result["candidates"] = [
                item.to_dict() for item in self.candidates
            ]
        return result


def unassessed_motion_evidence(
    *,
    previous_detection_id: str,
    target_valid_at: float,
    method: str,
    support_frame_ids: tuple[str, ...],
    reason: str = "caller_did_not_supply_motion_evidence_assessment",
) -> MotionEvidenceRecord:
    """Create explicit fail-closed metadata without inferring from a method."""

    return MotionEvidenceRecord(
        previous_detection_id=previous_detection_id,
        target_valid_at=target_valid_at,
        status="not_assessed",
        reason=reason,
        method=method,
        support_frame_ids=support_frame_ids,
    )


def assess_shadow_identity_frame(
    feature_table: AssociationFeatureTable,
    assignment: GlobalAssignmentResult,
    motion_evidence: Iterable[MotionEvidenceRecord],
    *,
    ambiguity_cost_delta: float,
) -> ShadowIdentityFrameAssessment:
    """Retain all evidence and label the one-best identity fail-closed."""

    if not isinstance(feature_table, AssociationFeatureTable):
        raise TypeError("identity-audit vereist AssociationFeatureTable")
    if not feature_table.is_complete:
        raise ValueError("identity-audit vereist een complete featuretable")
    if not isinstance(assignment, GlobalAssignmentResult):
        raise TypeError("identity-audit vereist GlobalAssignmentResult")
    if (
        not math.isfinite(ambiguity_cost_delta)
        or ambiguity_cost_delta < 0.0
    ):
        raise ValueError(
            "ambiguity_cost_delta moet niet-negatief en eindig zijn"
        )
    problem = assignment.problem
    if (
        set(problem.previous_detection_ids)
        != set(feature_table.previous_detection_ids)
        or set(problem.current_detection_ids)
        != set(feature_table.current_detection_ids)
    ):
        raise ValueError("assignment en featuretable dekken andere detecties")

    features = {
        (item.previous_detection_id, item.current_detection_id): item
        for item in feature_table.features
    }
    edges = {
        (item.previous_detection_id, item.current_detection_id): item
        for item in problem.edge_costs
    }
    if set(features) != set(edges):
        raise ValueError("assignment verloor een complete featurecandidate")

    evidence_values = tuple(motion_evidence)
    evidence_by_previous = {
        item.previous_detection_id: item for item in evidence_values
    }
    if len(evidence_by_previous) != len(evidence_values):
        raise ValueError("dubbele motion-evidence voor vorige detectie")
    if set(evidence_by_previous) != set(feature_table.previous_detection_ids):
        raise ValueError(
            "motion-evidence moet exact alle vorige detecties afdekken"
        )
    for item in evidence_values:
        if not math.isclose(
            item.target_valid_at,
            feature_table.current_valid_at,
            rel_tol=0.0,
            abs_tol=1e-6,
        ):
            raise ValueError("motion-evidence gebruikt een andere targettijd")

    previous_ranks = _edge_ranks(
        problem.edge_costs,
        group_by_previous=True,
    )
    current_ranks = _edge_ranks(
        problem.edge_costs,
        group_by_previous=False,
    )
    candidates = tuple(
        _candidate_audit(
            features[pair],
            edge,
            previous_ranks[pair],
            current_ranks[pair],
        )
        for pair, edge in sorted(edges.items())
    )
    solutions = tuple(
        AssignmentHypothesisAudit(
            rank=item.rank,
            total_cost=item.total_cost,
            matches=item.matches,
            deaths=item.deaths,
            births=item.births,
        )
        for item in assignment.solutions
    )
    alternatives_available = bool(assignment.alternatives)
    global_gap = (
        assignment.alternatives[0].total_cost
        - assignment.best.total_cost
        if alternatives_available
        else None
    )
    death_costs = dict(problem.death_costs)
    birth_costs = dict(problem.birth_costs)
    assessments: list[MatchIdentityAssessment] = []
    for pair in assignment.best.matches:
        old_id, new_id = pair
        feature = features[pair]
        edge = edges[pair]
        evidence = evidence_by_previous[old_id]
        local_competitors = sorted(
            candidate.cost
            for candidate_pair, candidate in edges.items()
            if candidate_pair[0] == old_id and candidate_pair != pair
        )
        local_gap = (
            local_competitors[0] - edge.cost
            if local_competitors
            else None
        )
        appears_in_all = (
            all(pair in solution.matches for solution in assignment.solutions)
            if alternatives_available
            else None
        )
        status, reasons = _identity_status(
            evidence,
            alternatives_available=alternatives_available,
            global_alternative_cost_gap=global_gap,
            appears_in_all_retained_solutions=appears_in_all,
            ambiguity_cost_delta=ambiguity_cost_delta,
        )
        assessments.append(MatchIdentityAssessment(
            previous_detection_id=old_id,
            current_detection_id=new_id,
            status=status,
            reason_codes=reasons,
            motion_evidence_status=evidence.status,
            motion_evidence_reason=evidence.reason,
            motion_evidence_fingerprint=evidence.fingerprint,
            selected_cost=edge.cost,
            observed_interval_speed_kmh=(
                feature.observed_interval_speed_kmh
            ),
            local_next_edge_cost_gap=local_gap,
            match_break_cost_delta=(
                death_costs[old_id] + birth_costs[new_id] - edge.cost
            ),
            global_alternative_cost_gap=global_gap,
            appears_in_all_retained_solutions=appears_in_all,
        ))

    candidate_hash = _fingerprint([
        _canonical_candidate(item) for item in candidates
    ])
    return ShadowIdentityFrameAssessment(
        previous_valid_at=feature_table.previous_valid_at,
        current_valid_at=feature_table.current_valid_at,
        candidates=candidates,
        solutions=solutions,
        motion_evidence=tuple(sorted(
            evidence_values,
            key=lambda item: item.previous_detection_id,
        )),
        best_match_assessments=tuple(sorted(
            assessments,
            key=lambda item: (
                item.previous_detection_id,
                item.current_detection_id,
            ),
        )),
        candidate_evidence_sha256=candidate_hash,
        caller_ambiguity_cost_delta=ambiguity_cost_delta,
        solver=assignment.solver,
        assignment_algorithm_version=assignment.algorithm_version,
        alternatives_available=alternatives_available,
        all_candidates_retained=(
            len(candidates) == feature_table.expected_feature_count
        ),
    )


def _candidate_audit(
    feature: AssociationFeatureVector,
    edge: AssignmentEdgeCost,
    previous_rank: int,
    current_rank: int,
) -> IdentityCandidateAudit:
    return IdentityCandidateAudit(
        previous_detection_id=feature.previous_detection_id,
        current_detection_id=feature.current_detection_id,
        cost=edge.cost,
        cost_model_id=edge.cost_model_id,
        cost_components=edge.components,
        observed_interval_speed_kmh=feature.observed_interval_speed_kmh,
        residual_distance_m=feature.residual_distance_m,
        mahalanobis_squared=feature.mahalanobis_squared,
        absolute_log_area_ratio=feature.absolute_log_area_ratio,
        footprint_iou_unadvected=feature.footprint_iou_unadvected,
        flow_sample_id=feature.flow_sample_id,
        flow_method=feature.flow_method,
        previous_cost_rank=previous_rank,
        current_cost_rank=current_rank,
    )


def _edge_ranks(
    edges: Iterable[AssignmentEdgeCost],
    *,
    group_by_previous: bool,
) -> dict[tuple[str, str], int]:
    groups: dict[str, list[AssignmentEdgeCost]] = {}
    for edge in edges:
        key = (
            edge.previous_detection_id
            if group_by_previous
            else edge.current_detection_id
        )
        groups.setdefault(key, []).append(edge)
    result: dict[tuple[str, str], int] = {}
    for values in groups.values():
        ordered = sorted(
            values,
            key=lambda item: (
                item.cost,
                item.previous_detection_id,
                item.current_detection_id,
            ),
        )
        for rank, item in enumerate(ordered, start=1):
            result[
                (item.previous_detection_id, item.current_detection_id)
            ] = rank
    return result


def _identity_status(
    evidence: MotionEvidenceRecord,
    *,
    alternatives_available: bool,
    global_alternative_cost_gap: float | None,
    appears_in_all_retained_solutions: bool | None,
    ambiguity_cost_delta: float,
) -> tuple[str, tuple[str, ...]]:
    if evidence.status == "not_assessed":
        return (
            "indeterminate_motion_not_assessed",
            (
                "motion_evidence_not_assessed",
                "one_best_assignment_is_hypothesis_only",
            ),
        )
    if evidence.status == "unavailable":
        return (
            "indeterminate_motion_unavailable",
            (
                "motion_evidence_unavailable",
                evidence.reason,
                "one_best_assignment_is_hypothesis_only",
            ),
        )
    if not alternatives_available:
        return (
            "indeterminate_alternatives_not_available",
            (
                "solver_returned_one_best_without_alternative",
                "one_best_assignment_is_hypothesis_only",
            ),
        )
    if (
        global_alternative_cost_gap is None
        or global_alternative_cost_gap <= ambiguity_cost_delta
        or appears_in_all_retained_solutions is not True
    ):
        return (
            "indeterminate_within_caller_ambiguity_delta",
            (
                "retained_alternatives_contest_identity",
                "caller_cost_delta_is_not_probability",
            ),
        )
    return (
        "supported_hypothesis_not_validated",
        (
            "motion_evidence_available",
            "retained_solutions_agree_above_caller_cost_delta",
            "independent_identity_truth_unavailable",
        ),
    )


def _canonical_candidate(item: IdentityCandidateAudit) -> dict:
    return {
        "previous_detection_id": item.previous_detection_id,
        "current_detection_id": item.current_detection_id,
        "cost": item.cost.hex(),
        "cost_model_id": item.cost_model_id,
        "cost_components": [
            [name, value.hex()] for name, value in item.cost_components
        ],
        "observed_interval_speed_kmh":
            item.observed_interval_speed_kmh.hex(),
        "residual_distance_m": item.residual_distance_m.hex(),
        "mahalanobis_squared": item.mahalanobis_squared.hex(),
        "absolute_log_area_ratio": item.absolute_log_area_ratio.hex(),
        "footprint_iou_unadvected": item.footprint_iou_unadvected.hex(),
        "flow_sample_id": item.flow_sample_id,
        "flow_method": item.flow_method,
        "previous_cost_rank": item.previous_cost_rank,
        "current_cost_rank": item.current_cost_rank,
    }


def _fingerprint(value: object) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("ascii")
    ).hexdigest()
