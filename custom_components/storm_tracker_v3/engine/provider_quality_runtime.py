"""Pure, auditable provider-quality ranking for shadow radar frames.

The runtime preserves every assessment. It distinguishes storage/diagnostic
value from eligibility as the *current* source, so a stale but technically
healthy frame is never silently promoted. All weights and freshness curves are
caller supplied; this module contains no provider priority or release margin.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from enum import Enum
from typing import Iterable


PROVIDER_QUALITY_ALGORITHM_VERSION = "stv3-provider-quality-runtime-v1"
_COMPONENT_IDS = (
    "freshness",
    "completeness",
    "upstream_quality",
    "geolocation_consistency",
    "echo_continuity",
    "operational_health",
    "empirical_replay_performance",
    "handoff_readiness",
)
_TOLERANCE = 1e-12


class ProviderSelectionState(str, Enum):
    SELECTED = "selected"
    NO_ELIGIBLE_FRAME = "no_eligible_frame"
    TIED = "tied"


@dataclass(frozen=True, slots=True)
class ProviderFreshnessPolicy:
    """Explicit piecewise-linear age score for one provider/product."""

    provider_id: str
    full_score_through_seconds: float
    current_until_seconds: float

    def __post_init__(self) -> None:
        _identifier(self.provider_id, "provider_id")
        _nonnegative_finite(
            self.full_score_through_seconds,
            "full_score_through_seconds",
        )
        _positive_finite(self.current_until_seconds, "current_until_seconds")
        if self.full_score_through_seconds >= self.current_until_seconds:
            raise ValueError(
                "full_score_through_seconds must be smaller than "
                "current_until_seconds"
            )


@dataclass(frozen=True, slots=True)
class ProviderQualityPolicy:
    """Caller-owned scoring policy; there are deliberately no defaults."""

    policy_id: str
    component_weights: tuple[tuple[str, float], ...]
    freshness_by_provider: tuple[ProviderFreshnessPolicy, ...]
    maximum_future_skew_seconds: float
    tie_score_tolerance: float

    def __post_init__(self) -> None:
        _identifier(self.policy_id, "policy_id")
        if not isinstance(self.component_weights, tuple):
            raise TypeError("component_weights must be tuple")
        component_ids = tuple(item[0] for item in self.component_weights)
        if component_ids != _COMPONENT_IDS:
            raise ValueError(
                "component_weights must contain every canonical component "
                "once and in canonical order"
            )
        weights = tuple(item[1] for item in self.component_weights)
        for weight in weights:
            _nonnegative_finite(weight, "component weight")
        if sum(weights) <= 0.0:
            raise ValueError("at least one component weight must be positive")
        if not isinstance(self.freshness_by_provider, tuple):
            raise TypeError("freshness_by_provider must be tuple")
        if not self.freshness_by_provider or any(
            not isinstance(item, ProviderFreshnessPolicy)
            for item in self.freshness_by_provider
        ):
            raise ValueError(
                "freshness_by_provider requires typed provider policies"
            )
        provider_ids = tuple(
            item.provider_id.casefold() for item in self.freshness_by_provider
        )
        if len(set(provider_ids)) != len(provider_ids):
            raise ValueError("freshness_by_provider contains duplicate providers")
        _nonnegative_finite(
            self.maximum_future_skew_seconds,
            "maximum_future_skew_seconds",
        )
        _nonnegative_finite(self.tie_score_tolerance, "tie_score_tolerance")

    def freshness(self, provider_id: str) -> ProviderFreshnessPolicy | None:
        key = _identifier(provider_id, "provider_id").casefold()
        return next(
            (
                item
                for item in self.freshness_by_provider
                if item.provider_id.casefold() == key
            ),
            None,
        )


@dataclass(frozen=True, slots=True)
class ProviderFrameQualityInput:
    """Measured quality evidence for one immutable provider frame."""

    provider_id: str
    frame_id: str
    product_id: str
    observed_at: float
    received_at: float
    evaluated_at: float
    total_pixel_count: int
    valid_pixel_count: int
    upstream_quality: float | None
    geolocation_consistency: float | None
    echo_continuity: float | None
    parser_error: str | None
    fetch_error: str | None
    empirical_replay_performance: float | None
    empirical_replay_metric_id: str | None
    empirical_replay_event_count: int | None
    empirical_replay_corpus_sha256: str | None
    empirical_replay_split_sha256: str | None
    empirical_replay_evidence_sha256: str | None
    handoff_readiness: float | None
    input_artifact_sha256: str
    parser_version: str

    def __post_init__(self) -> None:
        for label, value in (
            ("provider_id", self.provider_id),
            ("frame_id", self.frame_id),
            ("product_id", self.product_id),
            ("parser_version", self.parser_version),
        ):
            _identifier(value, label)
        for label, value in (
            ("observed_at", self.observed_at),
            ("received_at", self.received_at),
            ("evaluated_at", self.evaluated_at),
        ):
            _finite(value, label)
        if self.evaluated_at < self.received_at:
            raise ValueError("evaluated_at cannot precede received_at")
        _nonnegative_int(self.total_pixel_count, "total_pixel_count")
        _nonnegative_int(self.valid_pixel_count, "valid_pixel_count")
        if self.total_pixel_count == 0:
            raise ValueError("total_pixel_count must be positive")
        if self.valid_pixel_count > self.total_pixel_count:
            raise ValueError("valid_pixel_count cannot exceed total_pixel_count")
        for label, value in (
            ("upstream_quality", self.upstream_quality),
            ("geolocation_consistency", self.geolocation_consistency),
            ("echo_continuity", self.echo_continuity),
            (
                "empirical_replay_performance",
                self.empirical_replay_performance,
            ),
            ("handoff_readiness", self.handoff_readiness),
        ):
            _optional_unit_interval(value, label)
        for label, value in (
            ("parser_error", self.parser_error),
            ("fetch_error", self.fetch_error),
        ):
            if value is not None:
                _identifier(value, label)
        replay_fields = (
            self.empirical_replay_performance,
            self.empirical_replay_metric_id,
            self.empirical_replay_event_count,
            self.empirical_replay_corpus_sha256,
            self.empirical_replay_split_sha256,
            self.empirical_replay_evidence_sha256,
        )
        if any(value is not None for value in replay_fields):
            if any(value is None for value in replay_fields):
                raise ValueError(
                    "empirical replay score, event count, and evidence hash "
                    "must be present together"
                )
            _positive_int(
                self.empirical_replay_event_count,
                "empirical_replay_event_count",
            )
            _identifier(
                self.empirical_replay_metric_id,
                "empirical_replay_metric_id",
            )
            _sha256(
                self.empirical_replay_corpus_sha256,
                "empirical_replay_corpus_sha256",
            )
            _sha256(
                self.empirical_replay_split_sha256,
                "empirical_replay_split_sha256",
            )
            _sha256(
                self.empirical_replay_evidence_sha256,
                "empirical_replay_evidence_sha256",
            )
        _sha256(self.input_artifact_sha256, "input_artifact_sha256")

    @property
    def completeness(self) -> float:
        return self.valid_pixel_count / self.total_pixel_count


@dataclass(frozen=True, slots=True)
class ProviderQualityComponent:
    component_id: str
    value: float | None
    weight: float
    weighted_contribution: float | None
    evidence_state: str


@dataclass(frozen=True, slots=True)
class ProviderQualityAssessment:
    provider_id: str
    frame_id: str
    product_id: str
    observed_at: float
    frame_age_seconds: float
    current_until_seconds: float | None
    is_future: bool
    is_stale: bool
    score: float | None
    eligible_for_current_selection: bool
    reason_codes: tuple[str, ...]
    components: tuple[ProviderQualityComponent, ...]
    input_artifact_sha256: str
    parser_version: str
    empirical_replay_metric_id: str | None
    empirical_replay_event_count: int | None
    empirical_replay_corpus_sha256: str | None
    empirical_replay_split_sha256: str | None
    empirical_replay_evidence_sha256: str | None
    policy_id: str
    algorithm_version: str = PROVIDER_QUALITY_ALGORITHM_VERSION


@dataclass(frozen=True, slots=True)
class ProviderSelection:
    state: ProviderSelectionState
    selected_provider_id: str | None
    selected_frame_id: str | None
    tied_frame_ids: tuple[str, ...]
    assessments: tuple[ProviderQualityAssessment, ...]
    reason_codes: tuple[str, ...]
    policy_id: str
    algorithm_version: str = PROVIDER_QUALITY_ALGORITHM_VERSION


def assess_provider_frame(
    frame: ProviderFrameQualityInput,
    *,
    policy: ProviderQualityPolicy,
) -> ProviderQualityAssessment:
    """Assess one frame without deleting or suppressing its evidence."""

    if not isinstance(frame, ProviderFrameQualityInput):
        raise TypeError("frame must be ProviderFrameQualityInput")
    if not isinstance(policy, ProviderQualityPolicy):
        raise TypeError("policy must be ProviderQualityPolicy")
    freshness_policy = policy.freshness(frame.provider_id)
    age = frame.evaluated_at - frame.observed_at
    is_future = age < -policy.maximum_future_skew_seconds
    is_stale = (
        freshness_policy is not None
        and age > freshness_policy.current_until_seconds
    )
    freshness_score = (
        _freshness_score(age, freshness_policy)
        if freshness_policy is not None and not is_future
        else None
    )
    operational_health = float(
        frame.parser_error is None and frame.fetch_error is None
    )
    values = {
        "freshness": freshness_score,
        "completeness": frame.completeness,
        "upstream_quality": frame.upstream_quality,
        "geolocation_consistency": frame.geolocation_consistency,
        "echo_continuity": frame.echo_continuity,
        "operational_health": operational_health,
        "empirical_replay_performance": frame.empirical_replay_performance,
        "handoff_readiness": frame.handoff_readiness,
    }
    components: list[ProviderQualityComponent] = []
    missing_weighted: list[str] = []
    weighted_sum = 0.0
    total_weight = 0.0
    for component_id, weight in policy.component_weights:
        value = values[component_id]
        if value is None:
            if weight > 0.0:
                missing_weighted.append(component_id)
            components.append(
                ProviderQualityComponent(
                    component_id=component_id,
                    value=None,
                    weight=weight,
                    weighted_contribution=None,
                    evidence_state="missing",
                )
            )
            continue
        contribution = value * weight
        weighted_sum += contribution
        total_weight += weight
        components.append(
            ProviderQualityComponent(
                component_id=component_id,
                value=value,
                weight=weight,
                weighted_contribution=contribution,
                evidence_state="measured",
            )
        )

    reasons: list[str] = []
    if freshness_policy is None:
        reasons.append("freshness_policy_missing")
    if is_future:
        reasons.append("frame_time_beyond_future_tolerance")
    if is_stale:
        reasons.append("frame_stale_for_current_selection")
    if frame.parser_error is not None:
        reasons.append("parser_error_present")
    if frame.fetch_error is not None:
        reasons.append("fetch_error_present")
    reasons.extend(
        f"weighted_component_missing:{component_id}"
        for component_id in missing_weighted
    )
    complete_score = not missing_weighted and total_weight > 0.0
    score = weighted_sum / total_weight if complete_score else None
    eligible = (
        complete_score
        and freshness_policy is not None
        and not is_future
        and not is_stale
        and frame.parser_error is None
        and frame.fetch_error is None
    )
    if eligible:
        reasons.append("eligible_for_current_selection")
    elif not reasons:
        reasons.append("not_eligible_for_current_selection")
    return ProviderQualityAssessment(
        provider_id=frame.provider_id,
        frame_id=frame.frame_id,
        product_id=frame.product_id,
        observed_at=frame.observed_at,
        frame_age_seconds=age,
        current_until_seconds=(
            freshness_policy.current_until_seconds
            if freshness_policy is not None
            else None
        ),
        is_future=is_future,
        is_stale=is_stale,
        score=score,
        eligible_for_current_selection=eligible,
        reason_codes=tuple(reasons),
        components=tuple(components),
        input_artifact_sha256=frame.input_artifact_sha256,
        parser_version=frame.parser_version,
        empirical_replay_metric_id=frame.empirical_replay_metric_id,
        empirical_replay_event_count=frame.empirical_replay_event_count,
        empirical_replay_corpus_sha256=(
            frame.empirical_replay_corpus_sha256
        ),
        empirical_replay_split_sha256=frame.empirical_replay_split_sha256,
        empirical_replay_evidence_sha256=(
            frame.empirical_replay_evidence_sha256
        ),
        policy_id=policy.policy_id,
    )


def select_current_provider(
    frames: Iterable[ProviderFrameQualityInput],
    *,
    policy: ProviderQualityPolicy,
) -> ProviderSelection:
    """Rank all supplied frames while failing closed on a true top tie."""

    if not isinstance(policy, ProviderQualityPolicy):
        raise TypeError("policy must be ProviderQualityPolicy")
    supplied = tuple(frames)
    if not supplied or any(
        not isinstance(item, ProviderFrameQualityInput) for item in supplied
    ):
        raise ValueError("selection requires typed provider frames")
    ordered_frames = tuple(
        sorted(supplied, key=lambda item: (item.provider_id, item.frame_id))
    )
    frame_ids = tuple(item.frame_id for item in ordered_frames)
    if len(set(frame_ids)) != len(frame_ids):
        raise ValueError("frame_id must be unique in one selection")
    assessments = tuple(
        assess_provider_frame(item, policy=policy) for item in ordered_frames
    )
    eligible = tuple(
        item for item in assessments if item.eligible_for_current_selection
    )
    if not eligible:
        return ProviderSelection(
            state=ProviderSelectionState.NO_ELIGIBLE_FRAME,
            selected_provider_id=None,
            selected_frame_id=None,
            tied_frame_ids=(),
            assessments=assessments,
            reason_codes=("no_frame_eligible_for_current_selection",),
            policy_id=policy.policy_id,
        )
    best_score = max(float(item.score) for item in eligible)
    score_tied = tuple(
        item
        for item in eligible
        if math.isclose(
            float(item.score),
            best_score,
            rel_tol=0.0,
            abs_tol=policy.tie_score_tolerance,
        )
    )
    newest_time = max(item.observed_at for item in score_tied)
    finalists = tuple(
        item
        for item in score_tied
        if math.isclose(
            item.observed_at,
            newest_time,
            rel_tol=0.0,
            abs_tol=_TOLERANCE,
        )
    )
    if len(finalists) != 1:
        return ProviderSelection(
            state=ProviderSelectionState.TIED,
            selected_provider_id=None,
            selected_frame_id=None,
            tied_frame_ids=tuple(
                sorted(item.frame_id for item in finalists)
            ),
            assessments=assessments,
            reason_codes=("top_quality_and_valid_time_tied",),
            policy_id=policy.policy_id,
        )
    selected = finalists[0]
    return ProviderSelection(
        state=ProviderSelectionState.SELECTED,
        selected_provider_id=selected.provider_id,
        selected_frame_id=selected.frame_id,
        tied_frame_ids=(),
        assessments=assessments,
        reason_codes=("highest_complete_current_quality_score",),
        policy_id=policy.policy_id,
    )


def _freshness_score(
    age_seconds: float,
    policy: ProviderFreshnessPolicy,
) -> float:
    age = max(0.0, age_seconds)
    if age <= policy.full_score_through_seconds:
        return 1.0
    if age >= policy.current_until_seconds:
        return 0.0
    span = (
        policy.current_until_seconds - policy.full_score_through_seconds
    )
    return (
        policy.current_until_seconds - age
    ) / span


def _identifier(value: object, label: str) -> str:
    if not isinstance(value, str) or not value.strip() or "\0" in value:
        raise ValueError(f"{label} must be a non-empty identifier")
    return value.strip()


def _finite(value: object, label: str) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
    ):
        raise ValueError(f"{label} must be finite")
    return float(value)


def _nonnegative_finite(value: object, label: str) -> float:
    number = _finite(value, label)
    if number < 0.0:
        raise ValueError(f"{label} must be non-negative")
    return number


def _positive_finite(value: object, label: str) -> float:
    number = _finite(value, label)
    if number <= 0.0:
        raise ValueError(f"{label} must be positive")
    return number


def _nonnegative_int(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{label} must be a non-negative integer")
    return value


def _positive_int(value: object, label: str) -> int:
    number = _nonnegative_int(value, label)
    if number <= 0:
        raise ValueError(f"{label} must be a positive integer")
    return number


def _optional_unit_interval(value: object, label: str) -> None:
    if value is None:
        return
    number = _finite(value, label)
    if not 0.0 <= number <= 1.0:
        raise ValueError(f"{label} must lie in [0, 1]")


def _sha256(value: object, label: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError(f"{label} must be lowercase SHA-256 hex")
    return value
