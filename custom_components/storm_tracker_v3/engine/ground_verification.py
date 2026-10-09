"""Pure event-held-out verification for shadow ground wet/dry decisions.

This module does not create truth, infer dry weather from missing evidence,
select thresholds, or make release decisions.  It only admits exact
candidate/label pairs from an ``ExternalTruthContext``-compatible object and
computes transparent event-weighted categorical metrics.

The module is deliberately not imported by the live integration or worker.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import math
from typing import Callable, Iterable


GROUND_VERIFICATION_VERSION = "stv3-event-held-out-ground-verification-v1"
CROSS_STORM_CORRECTNESS_EXTERNAL_GATE = (
    "requires_independent_causal_cell_or_track_identity_labels"
)


class GroundVerificationPartition(str, Enum):
    """Immutable corpus partition selected by the caller."""

    TRAIN = "train"
    VALIDATION = "validation"
    TEST = "test"


class CandidateWetDryState(str, Enum):
    """Categorical candidate output without an implicit binary fallback."""

    WET = "wet"
    DRY = "dry"
    UNKNOWN = "unknown"
    UNAVAILABLE = "unavailable"


class GroundAdmissionDisposition(str, Enum):
    ADMITTED = "admitted"
    EXCLUDED = "excluded"


class GroundExclusionReason(str, Enum):
    EVENT_OR_FRAME_MISSING = "event_or_frame_missing"
    PARTITION_MISMATCH = "partition_mismatch"
    EVENT_FRAME_MISMATCH = "event_frame_mismatch"
    LABEL_MISSING = "label_missing"
    LABEL_NOT_WET_DRY = "label_not_wet_dry"
    LABEL_NOT_CONFIRMED = "label_not_confirmed"
    TRUTH_UNKNOWN = "truth_unknown"
    SOURCE_MISSING = "source_missing"
    SOURCE_NOT_INDEPENDENT = "source_not_independent"
    LABEL_EVENT_FRAME_MISMATCH = "label_event_frame_mismatch"
    LABEL_SITE_MISMATCH = "label_site_mismatch"
    LABEL_INTERVAL_MISMATCH = "label_interval_mismatch"
    TRUTH_DRY_NOT_DRY_CAPABLE = "truth_dry_not_dry_capable"
    CANDIDATE_DRY_NOT_DRY_CAPABLE = "candidate_dry_not_dry_capable"


class GroundStratumDimension(str, Enum):
    OVERALL = "overall"
    PROVIDER = "provider"
    CAPABILITY = "capability"


@dataclass(frozen=True, slots=True)
class GroundWetDryCandidate:
    """One immutable candidate decision to compare with external truth."""

    candidate_id: str
    decision_id: str
    evaluation_stratum_id: str
    algorithm_version: str
    event_id: str
    frame_id: str
    physical_site_id: str
    interval_start_utc: float
    interval_end_utc: float
    provider_id: str
    capability_id: str
    candidate_can_observe_absence: bool
    state: CandidateWetDryState
    track_id: str | None

    def __post_init__(self) -> None:
        for name, value in (
            ("candidate_id", self.candidate_id),
            ("decision_id", self.decision_id),
            ("evaluation_stratum_id", self.evaluation_stratum_id),
            ("algorithm_version", self.algorithm_version),
            ("event_id", self.event_id),
            ("frame_id", self.frame_id),
            ("physical_site_id", self.physical_site_id),
            ("provider_id", self.provider_id),
            ("capability_id", self.capability_id),
        ):
            _identifier(value, name)
        start = _finite(self.interval_start_utc, "interval_start_utc")
        end = _finite(self.interval_end_utc, "interval_end_utc")
        if end <= start:
            raise ValueError("candidate interval must have positive duration")
        if not isinstance(self.candidate_can_observe_absence, bool):
            raise TypeError("candidate_can_observe_absence must be bool")
        if not isinstance(self.state, CandidateWetDryState):
            raise TypeError("state must be CandidateWetDryState")
        if self.track_id is not None:
            _identifier(self.track_id, "track_id")


@dataclass(frozen=True, slots=True)
class GroundVerificationCase:
    """One candidate paired to one exact, confirmed independent label."""

    candidate: GroundWetDryCandidate
    partition: GroundVerificationPartition
    label_id: str
    label_source_id: str
    truth_state: CandidateWetDryState
    truth_capable_of_dry_detection: bool

    def __post_init__(self) -> None:
        if not isinstance(self.candidate, GroundWetDryCandidate):
            raise TypeError("candidate must be GroundWetDryCandidate")
        if not isinstance(self.partition, GroundVerificationPartition):
            raise TypeError("partition must be GroundVerificationPartition")
        _identifier(self.label_id, "label_id")
        _identifier(self.label_source_id, "label_source_id")
        if self.truth_state not in (
            CandidateWetDryState.WET,
            CandidateWetDryState.DRY,
        ):
            raise ValueError("verification truth must be determinate wet or dry")
        if not isinstance(self.truth_capable_of_dry_detection, bool):
            raise TypeError("truth_capable_of_dry_detection must be bool")
        if (
            self.truth_state is CandidateWetDryState.DRY
            and not self.truth_capable_of_dry_detection
        ):
            raise ValueError("dry truth requires a dry-capable source")


@dataclass(frozen=True, slots=True)
class GroundCaseAdmission:
    """Auditable result of attempting to pair one candidate and label."""

    candidate: GroundWetDryCandidate
    expected_partition: GroundVerificationPartition
    requested_label_id: str
    disposition: GroundAdmissionDisposition
    case: GroundVerificationCase | None
    exclusion_reason: GroundExclusionReason | None

    def __post_init__(self) -> None:
        if not isinstance(self.candidate, GroundWetDryCandidate):
            raise TypeError("candidate must be GroundWetDryCandidate")
        if not isinstance(
            self.expected_partition,
            GroundVerificationPartition,
        ):
            raise TypeError(
                "expected_partition must be GroundVerificationPartition"
            )
        _identifier(self.requested_label_id, "requested_label_id")
        if not isinstance(self.disposition, GroundAdmissionDisposition):
            raise TypeError("disposition must be GroundAdmissionDisposition")
        if self.disposition is GroundAdmissionDisposition.ADMITTED:
            if not isinstance(self.case, GroundVerificationCase):
                raise ValueError("admitted result requires a typed case")
            if self.exclusion_reason is not None:
                raise ValueError("admitted result cannot have exclusion reason")
            if self.case.candidate != self.candidate:
                raise ValueError("admitted case/candidate mismatch")
            if self.case.partition is not self.expected_partition:
                raise ValueError("admitted case/partition mismatch")
            if self.case.label_id != self.requested_label_id:
                raise ValueError("admitted case/label mismatch")
        else:
            if self.case is not None:
                raise ValueError("excluded result cannot carry a case")
            if not isinstance(self.exclusion_reason, GroundExclusionReason):
                raise ValueError("excluded result requires an exclusion reason")


@dataclass(frozen=True, slots=True)
class EventWeightedGroundConfusion:
    """Raw categorical counts plus event-weighted conditional rates."""

    admitted_case_count: int
    event_count: int
    truth_wet_count: int
    truth_dry_count: int
    determinate_prediction_count: int
    unknown_prediction_count: int
    unavailable_prediction_count: int
    indeterminate_prediction_count: int
    true_positive_count: int
    false_negative_count: int
    true_negative_count: int
    false_positive_count: int
    sensitivity: float | None
    specificity: float | None
    false_negative_rate: float | None
    false_positive_rate: float | None
    sensitivity_event_count: int
    specificity_event_count: int
    event_weighted_decision_coverage: float | None
    event_weighted_wet_decision_coverage: float | None
    event_weighted_dry_decision_coverage: float | None
    truth_dry_capable_case_count: int
    truth_wet_only_case_count: int


@dataclass(frozen=True, slots=True)
class GroundStratumVerification:
    """Metrics for one explicitly identified provider/capability stratum."""

    dimension: GroundStratumDimension
    provider_id: str | None
    capability_id: str | None
    candidate_can_observe_absence: bool | None
    metrics: EventWeightedGroundConfusion


@dataclass(frozen=True, slots=True)
class ExactLabeledReuseAudit:
    """Overlap counters based only on exactly admitted external labels."""

    exact_label_group_count: int
    duplicate_site_group_count: int
    duplicate_site_extra_candidate_count: int
    cross_provider_duplicate_site_group_count: int
    conflicting_determinate_duplicate_site_group_count: int
    cross_track_overlap_group_count: int
    cross_track_extra_assignment_count: int
    cross_track_missing_track_metadata_case_count: int
    cross_storm_correctness_evaluable: bool
    cross_storm_correctness_external_gate: str


@dataclass(frozen=True, slots=True)
class GroundVerificationReport:
    """Complete shadow report; it contains no pass/fail release decision."""

    evaluation_stratum_id: str
    algorithm_version: str
    partition: GroundVerificationPartition
    attempt_count: int
    admitted_count: int
    excluded_count: int
    exclusions_by_reason: tuple[tuple[GroundExclusionReason, int], ...]
    overall: GroundStratumVerification
    provider_strata: tuple[GroundStratumVerification, ...]
    capability_strata: tuple[GroundStratumVerification, ...]
    exact_labeled_reuse_audit: ExactLabeledReuseAudit
    verification_version: str = GROUND_VERIFICATION_VERSION


def admit_ground_verification_case(
    candidate: GroundWetDryCandidate,
    *,
    expected_partition: GroundVerificationPartition,
    truth_context: object,
    wet_dry_label_id: str,
) -> GroundCaseAdmission:
    """Admit only an exact independently confirmed WetDry label pairing.

    Data-quality failures are returned as typed exclusions so a report can
    count them.  Malformed API arguments still raise immediately.
    """

    if not isinstance(candidate, GroundWetDryCandidate):
        raise TypeError("candidate must be GroundWetDryCandidate")
    if not isinstance(expected_partition, GroundVerificationPartition):
        raise TypeError(
            "expected_partition must be GroundVerificationPartition"
        )
    _identifier(wet_dry_label_id, "wet_dry_label_id")
    _validate_truth_context_surface(truth_context)

    split = truth_context.split
    event_partition = split.partition_for_event(candidate.event_id)
    frame_partition = split.partition_for_frame(candidate.frame_id)
    if event_partition is None or frame_partition is None:
        return _excluded(
            candidate,
            expected_partition,
            wet_dry_label_id,
            GroundExclusionReason.EVENT_OR_FRAME_MISSING,
        )
    if (
        getattr(event_partition, "value", None) != expected_partition.value
        or getattr(frame_partition, "value", None)
        != expected_partition.value
    ):
        return _excluded(
            candidate,
            expected_partition,
            wet_dry_label_id,
            GroundExclusionReason.PARTITION_MISMATCH,
        )
    if dict(split.frame_events).get(candidate.frame_id) != candidate.event_id:
        return _excluded(
            candidate,
            expected_partition,
            wet_dry_label_id,
            GroundExclusionReason.EVENT_FRAME_MISMATCH,
        )

    label = truth_context.labels.label(wet_dry_label_id)
    if label is None:
        return _excluded(
            candidate,
            expected_partition,
            wet_dry_label_id,
            GroundExclusionReason.LABEL_MISSING,
        )
    if getattr(label.label_kind, "value", None) != "wet_dry":
        return _excluded(
            candidate,
            expected_partition,
            wet_dry_label_id,
            GroundExclusionReason.LABEL_NOT_WET_DRY,
        )
    payload = label.payload
    if getattr(payload.state, "value", None) == "unknown":
        return _excluded(
            candidate,
            expected_partition,
            wet_dry_label_id,
            GroundExclusionReason.TRUTH_UNKNOWN,
        )
    if getattr(label.state, "value", None) != "confirmed":
        return _excluded(
            candidate,
            expected_partition,
            wet_dry_label_id,
            GroundExclusionReason.LABEL_NOT_CONFIRMED,
        )

    source = truth_context.labels.source(label.source_id)
    if source is None:
        return _excluded(
            candidate,
            expected_partition,
            wet_dry_label_id,
            GroundExclusionReason.SOURCE_MISSING,
        )
    if source.is_external_truth_eligible is not True:
        return _excluded(
            candidate,
            expected_partition,
            wet_dry_label_id,
            GroundExclusionReason.SOURCE_NOT_INDEPENDENT,
        )
    if (
        label.event_id != candidate.event_id
        or label.frame_id != candidate.frame_id
    ):
        return _excluded(
            candidate,
            expected_partition,
            wet_dry_label_id,
            GroundExclusionReason.LABEL_EVENT_FRAME_MISMATCH,
        )
    if payload.site_id != candidate.physical_site_id:
        return _excluded(
            candidate,
            expected_partition,
            wet_dry_label_id,
            GroundExclusionReason.LABEL_SITE_MISMATCH,
        )
    if (
        float(payload.interval_start_utc)
        != float(candidate.interval_start_utc)
        or float(payload.interval_end_utc)
        != float(candidate.interval_end_utc)
    ):
        return _excluded(
            candidate,
            expected_partition,
            wet_dry_label_id,
            GroundExclusionReason.LABEL_INTERVAL_MISMATCH,
        )

    truth_value = getattr(payload.state, "value", None)
    if truth_value == "wet":
        truth_state = CandidateWetDryState.WET
    elif truth_value == "dry":
        truth_state = CandidateWetDryState.DRY
    else:
        return _excluded(
            candidate,
            expected_partition,
            wet_dry_label_id,
            GroundExclusionReason.TRUTH_UNKNOWN,
        )
    dry_capable = getattr(payload, "capable_of_dry_detection", None)
    if not isinstance(dry_capable, bool):
        raise TypeError(
            "WetDry payload capable_of_dry_detection must be bool"
        )
    if truth_state is CandidateWetDryState.DRY and not dry_capable:
        return _excluded(
            candidate,
            expected_partition,
            wet_dry_label_id,
            GroundExclusionReason.TRUTH_DRY_NOT_DRY_CAPABLE,
        )
    if (
        candidate.state is CandidateWetDryState.DRY
        and not candidate.candidate_can_observe_absence
    ):
        return _excluded(
            candidate,
            expected_partition,
            wet_dry_label_id,
            GroundExclusionReason.CANDIDATE_DRY_NOT_DRY_CAPABLE,
        )

    case = GroundVerificationCase(
        candidate=candidate,
        partition=expected_partition,
        label_id=label.label_id,
        label_source_id=source.source_id,
        truth_state=truth_state,
        truth_capable_of_dry_detection=dry_capable,
    )
    return GroundCaseAdmission(
        candidate=candidate,
        expected_partition=expected_partition,
        requested_label_id=wet_dry_label_id,
        disposition=GroundAdmissionDisposition.ADMITTED,
        case=case,
        exclusion_reason=None,
    )


def verify_ground_admissions(
    admissions: Iterable[GroundCaseAdmission],
) -> GroundVerificationReport:
    """Compute event-weighted categorical verification and audit coverage."""

    ordered = tuple(
        sorted(admissions, key=lambda item: item.candidate.candidate_id)
    )
    if not ordered or any(
        not isinstance(item, GroundCaseAdmission) for item in ordered
    ):
        raise ValueError("verification requires typed admission records")
    candidate_ids = [item.candidate.candidate_id for item in ordered]
    if len(candidate_ids) != len(set(candidate_ids)):
        raise ValueError("candidate_id must be unique within one report")

    first = ordered[0]
    signature = (
        first.candidate.evaluation_stratum_id,
        first.candidate.algorithm_version,
        first.expected_partition,
    )
    if any(
        (
            item.candidate.evaluation_stratum_id,
            item.candidate.algorithm_version,
            item.expected_partition,
        )
        != signature
        for item in ordered[1:]
    ):
        raise ValueError(
            "ground verification attempts cross evaluation strata, "
            "algorithms, or partitions"
        )

    cases = tuple(
        item.case
        for item in ordered
        if item.disposition is GroundAdmissionDisposition.ADMITTED
        and item.case is not None
    )
    truth_states_by_unit: dict[
        tuple[str, str, str, float, float],
        set[CandidateWetDryState],
    ] = {}
    for case in cases:
        truth_states_by_unit.setdefault(
            _exact_truth_unit_key(case),
            set(),
        ).add(case.truth_state)
    if any(len(states) > 1 for states in truth_states_by_unit.values()):
        raise ValueError(
            "conflicting independent wet/dry truth for one exact "
            "event/frame/site/interval unit"
        )
    excluded_counts = tuple(
        (reason, count)
        for reason in GroundExclusionReason
        if (
            count := sum(
                item.exclusion_reason is reason for item in ordered
            )
        )
    )
    overall = _stratum(
        cases,
        dimension=GroundStratumDimension.OVERALL,
        provider_id=None,
        capability_id=None,
        candidate_can_observe_absence=None,
    )

    providers = sorted({case.candidate.provider_id for case in cases})
    provider_strata = tuple(
        _stratum(
            tuple(
                case
                for case in cases
                if case.candidate.provider_id == provider_id
            ),
            dimension=GroundStratumDimension.PROVIDER,
            provider_id=provider_id,
            capability_id=None,
            candidate_can_observe_absence=None,
        )
        for provider_id in providers
    )
    capability_keys = sorted(
        {
            (
                case.candidate.provider_id,
                case.candidate.capability_id,
                case.candidate.candidate_can_observe_absence,
            )
            for case in cases
        }
    )
    capability_strata = tuple(
        _stratum(
            tuple(
                case
                for case in cases
                if (
                    case.candidate.provider_id,
                    case.candidate.capability_id,
                    case.candidate.candidate_can_observe_absence,
                )
                == key
            ),
            dimension=GroundStratumDimension.CAPABILITY,
            provider_id=key[0],
            capability_id=key[1],
            candidate_can_observe_absence=key[2],
        )
        for key in capability_keys
    )

    return GroundVerificationReport(
        evaluation_stratum_id=signature[0],
        algorithm_version=signature[1],
        partition=signature[2],
        attempt_count=len(ordered),
        admitted_count=len(cases),
        excluded_count=len(ordered) - len(cases),
        exclusions_by_reason=excluded_counts,
        overall=overall,
        provider_strata=provider_strata,
        capability_strata=capability_strata,
        exact_labeled_reuse_audit=_exact_labeled_reuse_audit(cases),
    )


def _stratum(
    cases: tuple[GroundVerificationCase, ...],
    *,
    dimension: GroundStratumDimension,
    provider_id: str | None,
    capability_id: str | None,
    candidate_can_observe_absence: bool | None,
) -> GroundStratumVerification:
    determinate = tuple(
        case
        for case in cases
        if case.candidate.state
        in (CandidateWetDryState.WET, CandidateWetDryState.DRY)
    )
    true_positive = sum(
        case.truth_state is CandidateWetDryState.WET
        and case.candidate.state is CandidateWetDryState.WET
        for case in determinate
    )
    false_negative = sum(
        case.truth_state is CandidateWetDryState.WET
        and case.candidate.state is CandidateWetDryState.DRY
        for case in determinate
    )
    true_negative = sum(
        case.truth_state is CandidateWetDryState.DRY
        and case.candidate.state is CandidateWetDryState.DRY
        for case in determinate
    )
    false_positive = sum(
        case.truth_state is CandidateWetDryState.DRY
        and case.candidate.state is CandidateWetDryState.WET
        for case in determinate
    )

    sensitivity, sensitivity_events = _event_weighted_ratio(
        cases,
        denominator=lambda case: (
            case.truth_state is CandidateWetDryState.WET
            and case.candidate.state
            in (CandidateWetDryState.WET, CandidateWetDryState.DRY)
        ),
        numerator=lambda case: (
            case.truth_state is CandidateWetDryState.WET
            and case.candidate.state is CandidateWetDryState.WET
        ),
    )
    specificity, specificity_events = _event_weighted_ratio(
        cases,
        denominator=lambda case: (
            case.truth_state is CandidateWetDryState.DRY
            and case.candidate.state
            in (CandidateWetDryState.WET, CandidateWetDryState.DRY)
        ),
        numerator=lambda case: (
            case.truth_state is CandidateWetDryState.DRY
            and case.candidate.state is CandidateWetDryState.DRY
        ),
    )
    decision_coverage, _ = _event_weighted_ratio(
        cases,
        denominator=lambda case: True,
        numerator=lambda case: (
            case.candidate.state
            in (CandidateWetDryState.WET, CandidateWetDryState.DRY)
        ),
    )
    wet_coverage, _ = _event_weighted_ratio(
        cases,
        denominator=lambda case: (
            case.truth_state is CandidateWetDryState.WET
        ),
        numerator=lambda case: (
            case.truth_state is CandidateWetDryState.WET
            and case.candidate.state
            in (CandidateWetDryState.WET, CandidateWetDryState.DRY)
        ),
    )
    dry_coverage, _ = _event_weighted_ratio(
        cases,
        denominator=lambda case: (
            case.truth_state is CandidateWetDryState.DRY
        ),
        numerator=lambda case: (
            case.truth_state is CandidateWetDryState.DRY
            and case.candidate.state
            in (CandidateWetDryState.WET, CandidateWetDryState.DRY)
        ),
    )

    metrics = EventWeightedGroundConfusion(
        admitted_case_count=len(cases),
        event_count=len({case.candidate.event_id for case in cases}),
        truth_wet_count=sum(
            case.truth_state is CandidateWetDryState.WET for case in cases
        ),
        truth_dry_count=sum(
            case.truth_state is CandidateWetDryState.DRY for case in cases
        ),
        determinate_prediction_count=len(determinate),
        unknown_prediction_count=sum(
            case.candidate.state is CandidateWetDryState.UNKNOWN
            for case in cases
        ),
        unavailable_prediction_count=sum(
            case.candidate.state is CandidateWetDryState.UNAVAILABLE
            for case in cases
        ),
        indeterminate_prediction_count=sum(
            case.candidate.state
            in (
                CandidateWetDryState.UNKNOWN,
                CandidateWetDryState.UNAVAILABLE,
            )
            for case in cases
        ),
        true_positive_count=true_positive,
        false_negative_count=false_negative,
        true_negative_count=true_negative,
        false_positive_count=false_positive,
        sensitivity=sensitivity,
        specificity=specificity,
        false_negative_rate=(
            None if sensitivity is None else 1.0 - sensitivity
        ),
        false_positive_rate=(
            None if specificity is None else 1.0 - specificity
        ),
        sensitivity_event_count=sensitivity_events,
        specificity_event_count=specificity_events,
        event_weighted_decision_coverage=decision_coverage,
        event_weighted_wet_decision_coverage=wet_coverage,
        event_weighted_dry_decision_coverage=dry_coverage,
        truth_dry_capable_case_count=sum(
            case.truth_capable_of_dry_detection for case in cases
        ),
        truth_wet_only_case_count=sum(
            not case.truth_capable_of_dry_detection for case in cases
        ),
    )
    return GroundStratumVerification(
        dimension=dimension,
        provider_id=provider_id,
        capability_id=capability_id,
        candidate_can_observe_absence=candidate_can_observe_absence,
        metrics=metrics,
    )


def _event_weighted_ratio(
    cases: tuple[GroundVerificationCase, ...],
    *,
    denominator: Callable[[GroundVerificationCase], bool],
    numerator: Callable[[GroundVerificationCase], bool],
) -> tuple[float | None, int]:
    ratios: list[float] = []
    for event_id in sorted({case.candidate.event_id for case in cases}):
        event_cases = tuple(
            case for case in cases if case.candidate.event_id == event_id
        )
        denominator_count = sum(denominator(case) for case in event_cases)
        if denominator_count == 0:
            continue
        numerator_count = sum(numerator(case) for case in event_cases)
        ratios.append(numerator_count / denominator_count)
    if not ratios:
        return None, 0
    return math.fsum(ratios) / len(ratios), len(ratios)


def _exact_labeled_reuse_audit(
    cases: tuple[GroundVerificationCase, ...],
) -> ExactLabeledReuseAudit:
    exact_groups: dict[
        tuple[str, str, str, float, float],
        list[GroundVerificationCase],
    ] = {}
    decision_site_groups: dict[
        tuple[str, str, str, str, float, float],
        list[GroundVerificationCase],
    ] = {}
    for case in cases:
        candidate = case.candidate
        exact_key = _exact_truth_unit_key(case)
        exact_groups.setdefault(exact_key, []).append(case)
        decision_key = (candidate.decision_id,) + exact_key
        decision_site_groups.setdefault(decision_key, []).append(case)

    duplicate_groups = tuple(
        group for group in decision_site_groups.values() if len(group) > 1
    )
    cross_track_groups = tuple(
        group
        for group in exact_groups.values()
        if len(
            {
                case.candidate.track_id
                for case in group
                if case.candidate.track_id is not None
            }
        )
        > 1
    )
    return ExactLabeledReuseAudit(
        exact_label_group_count=len(exact_groups),
        duplicate_site_group_count=len(duplicate_groups),
        duplicate_site_extra_candidate_count=sum(
            len(group) - 1 for group in duplicate_groups
        ),
        cross_provider_duplicate_site_group_count=sum(
            len({case.candidate.provider_id for case in group}) > 1
            for group in duplicate_groups
        ),
        conflicting_determinate_duplicate_site_group_count=sum(
            {
                case.candidate.state
                for case in group
                if case.candidate.state
                in (CandidateWetDryState.WET, CandidateWetDryState.DRY)
            }
            == {CandidateWetDryState.WET, CandidateWetDryState.DRY}
            for group in duplicate_groups
        ),
        cross_track_overlap_group_count=len(cross_track_groups),
        cross_track_extra_assignment_count=sum(
            len(
                {
                    case.candidate.track_id
                    for case in group
                    if case.candidate.track_id is not None
                }
            )
            - 1
            for group in cross_track_groups
        ),
        cross_track_missing_track_metadata_case_count=sum(
            case.candidate.track_id is None for case in cases
        ),
        cross_storm_correctness_evaluable=False,
        cross_storm_correctness_external_gate=(
            CROSS_STORM_CORRECTNESS_EXTERNAL_GATE
        ),
    )


def _exact_truth_unit_key(
    case: GroundVerificationCase,
) -> tuple[str, str, str, float, float]:
    candidate = case.candidate
    return (
        candidate.event_id,
        candidate.frame_id,
        candidate.physical_site_id,
        candidate.interval_start_utc,
        candidate.interval_end_utc,
    )


def _excluded(
    candidate: GroundWetDryCandidate,
    expected_partition: GroundVerificationPartition,
    label_id: str,
    reason: GroundExclusionReason,
) -> GroundCaseAdmission:
    return GroundCaseAdmission(
        candidate=candidate,
        expected_partition=expected_partition,
        requested_label_id=label_id,
        disposition=GroundAdmissionDisposition.EXCLUDED,
        case=None,
        exclusion_reason=reason,
    )


def _validate_truth_context_surface(context: object) -> None:
    if (
        context is None
        or not hasattr(context, "split")
        or not hasattr(context, "labels")
        or not callable(
            getattr(context.split, "partition_for_event", None)
        )
        or not callable(
            getattr(context.split, "partition_for_frame", None)
        )
        or not hasattr(context.split, "frame_events")
        or not callable(getattr(context.labels, "label", None))
        or not callable(getattr(context.labels, "source", None))
    ):
        raise TypeError(
            "truth_context is not ExternalTruthContext-compatible"
        )


def _identifier(value: object, label: str) -> None:
    if not isinstance(value, str) or not value.strip() or "\0" in value:
        raise ValueError(f"{label} must be a non-empty identifier")


def _finite(value: object, label: str) -> float:
    if isinstance(value, bool):
        raise TypeError(f"{label} must be finite")
    number = float(value)
    if not math.isfinite(number):
        raise ValueError(f"{label} must be finite")
    return number
