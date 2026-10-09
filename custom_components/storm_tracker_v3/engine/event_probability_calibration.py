"""Leakage-safe empirical calibration for tracker passage probabilities.

The module is deliberately a pure shadow component.  It fits only caller-
supplied training cases, balances meteorological events instead of individual
rows, and refuses to turn quality scores or incomplete truth into
probabilities.  Model selection order and support requirements are explicit;
no regional or provider fallback is hidden in this module.
"""
from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from enum import Enum
from typing import Iterable


EVENT_PROBABILITY_CALIBRATION_VERSION = (
    "stv3-event-balanced-isotonic-calibration-v1"
)


class CalibrationPartition(str, Enum):
    TRAIN = "train"
    VALIDATION = "validation"
    TEST = "test"


class CalibrationWeighting(str, Enum):
    EVENT_BALANCED = "event_balanced"


class CalibrationInterpolation(str, Enum):
    CLIPPED_LINEAR = "clipped_linear"


class InsufficientCalibrationSupport(ValueError):
    """Raised when a caller-supplied stratum cannot support a calibrator."""


@dataclass(frozen=True, slots=True)
class CalibrationScope:
    """One explicit calibration stratum.

    A ``None`` dimension means that dimension is pooled.  Callers may build
    exact and pooled models, but the caller must provide their selection order.
    """

    scope_id: str
    provider_id: str | None
    region_id: str | None
    lead_bin_id: str | None
    threshold_id: str | None

    def __post_init__(self) -> None:
        _identifier(self.scope_id, "scope_id")
        for label, value in (
            ("provider_id", self.provider_id),
            ("region_id", self.region_id),
            ("lead_bin_id", self.lead_bin_id),
            ("threshold_id", self.threshold_id),
        ):
            if value is not None:
                _identifier(value, label)

    def matches(self, value: object) -> bool:
        return all(
            expected is None or getattr(value, name, None) == expected
            for name, expected in (
                ("provider_id", self.provider_id),
                ("region_id", self.region_id),
                ("lead_bin_id", self.lead_bin_id),
                ("threshold_id", self.threshold_id),
            )
        )

    @property
    def canonical(self) -> dict[str, str | None]:
        return {
            "scope_id": self.scope_id,
            "provider_id": self.provider_id,
            "region_id": self.region_id,
            "lead_bin_id": self.lead_bin_id,
            "threshold_id": self.threshold_id,
        }


@dataclass(frozen=True, slots=True)
class CalibrationPolicy:
    minimum_events: int
    minimum_positive_events: int
    minimum_negative_events: int
    weighting: CalibrationWeighting
    interpolation: CalibrationInterpolation
    policy_id: str

    def __post_init__(self) -> None:
        _identifier(self.policy_id, "policy_id")
        for label, value in (
            ("minimum_events", self.minimum_events),
            ("minimum_positive_events", self.minimum_positive_events),
            ("minimum_negative_events", self.minimum_negative_events),
        ):
            _positive_integer(value, label)
        if self.minimum_events < (
            max(self.minimum_positive_events, self.minimum_negative_events)
        ):
            raise ValueError(
                "minimum_events cannot be smaller than a class-specific minimum"
            )
        if self.weighting is not CalibrationWeighting.EVENT_BALANCED:
            raise ValueError("only explicit event-balanced weighting is supported")
        if self.interpolation is not CalibrationInterpolation.CLIPPED_LINEAR:
            raise ValueError("only explicit clipped-linear interpolation is supported")

    @property
    def canonical(self) -> dict[str, object]:
        return {
            "policy_id": self.policy_id,
            "minimum_events": self.minimum_events,
            "minimum_positive_events": self.minimum_positive_events,
            "minimum_negative_events": self.minimum_negative_events,
            "weighting": self.weighting.value,
            "interpolation": self.interpolation.value,
        }


@dataclass(frozen=True, slots=True)
class PassageCalibrationCase:
    """One candidate-independent labelled passage case."""

    case_id: str
    event_id: str
    frame_id: str
    partition: CalibrationPartition
    raw_probability: float
    passage_occurred: bool
    provider_id: str
    region_id: str
    lead_bin_id: str
    threshold_id: str
    corpus_sha256: str
    split_sha256: str
    candidate_artifact_sha256: str
    truth_artifact_sha256: str
    truth_label_confirmed: bool
    truth_source_eligible: bool

    def __post_init__(self) -> None:
        for label, value in (
            ("case_id", self.case_id),
            ("event_id", self.event_id),
            ("frame_id", self.frame_id),
            ("provider_id", self.provider_id),
            ("region_id", self.region_id),
            ("lead_bin_id", self.lead_bin_id),
            ("threshold_id", self.threshold_id),
        ):
            _identifier(value, label)
        if not isinstance(self.partition, CalibrationPartition):
            raise TypeError("partition must be CalibrationPartition")
        _probability(self.raw_probability, "raw_probability")
        if not isinstance(self.passage_occurred, bool):
            raise TypeError("passage_occurred must be bool")
        for label, value in (
            ("corpus_sha256", self.corpus_sha256),
            ("split_sha256", self.split_sha256),
            ("candidate_artifact_sha256", self.candidate_artifact_sha256),
            ("truth_artifact_sha256", self.truth_artifact_sha256),
        ):
            _sha256(value, label)
        if self.candidate_artifact_sha256 == self.truth_artifact_sha256:
            raise ValueError("candidate and independent truth artifacts must differ")
        if self.truth_label_confirmed is not True:
            raise ValueError("calibration truth label must be confirmed")
        if self.truth_source_eligible is not True:
            raise ValueError("calibration truth source must be independently eligible")

    @property
    def canonical(self) -> dict[str, object]:
        return {
            "case_id": self.case_id,
            "event_id": self.event_id,
            "frame_id": self.frame_id,
            "partition": self.partition.value,
            "raw_probability": _float_token(self.raw_probability),
            "passage_occurred": self.passage_occurred,
            "provider_id": self.provider_id,
            "region_id": self.region_id,
            "lead_bin_id": self.lead_bin_id,
            "threshold_id": self.threshold_id,
            "corpus_sha256": self.corpus_sha256,
            "split_sha256": self.split_sha256,
            "candidate_artifact_sha256": self.candidate_artifact_sha256,
            "truth_artifact_sha256": self.truth_artifact_sha256,
        }


@dataclass(frozen=True, slots=True)
class ProbabilityCalibrationQuery:
    query_id: str
    raw_probability: float
    provider_id: str
    region_id: str
    lead_bin_id: str
    threshold_id: str

    def __post_init__(self) -> None:
        for label, value in (
            ("query_id", self.query_id),
            ("provider_id", self.provider_id),
            ("region_id", self.region_id),
            ("lead_bin_id", self.lead_bin_id),
            ("threshold_id", self.threshold_id),
        ):
            _identifier(value, label)
        _probability(self.raw_probability, "raw_probability")


@dataclass(frozen=True, slots=True)
class IsotonicKnot:
    raw_probability: float
    calibrated_probability: float
    event_balanced_weight: float
    case_count: int

    def __post_init__(self) -> None:
        _probability(self.raw_probability, "knot raw_probability")
        _probability(self.calibrated_probability, "knot calibrated_probability")
        if (
            not math.isfinite(self.event_balanced_weight)
            or self.event_balanced_weight <= 0.0
        ):
            raise ValueError("knot event_balanced_weight must be positive")
        _positive_integer(self.case_count, "knot case_count")


@dataclass(frozen=True, slots=True)
class EventBalancedIsotonicCalibrator:
    calibration_version: str
    scope: CalibrationScope
    policy: CalibrationPolicy
    corpus_sha256: str
    split_sha256: str
    training_data_sha256: str
    training_case_ids: tuple[str, ...]
    training_event_ids: tuple[str, ...]
    positive_event_count: int
    negative_event_count: int
    knots: tuple[IsotonicKnot, ...]
    algorithm_version: str = EVENT_PROBABILITY_CALIBRATION_VERSION

    def __post_init__(self) -> None:
        _identifier(self.calibration_version, "calibration_version")
        if not isinstance(self.scope, CalibrationScope):
            raise TypeError("scope must be CalibrationScope")
        if not isinstance(self.policy, CalibrationPolicy):
            raise TypeError("policy must be CalibrationPolicy")
        for label, value in (
            ("corpus_sha256", self.corpus_sha256),
            ("split_sha256", self.split_sha256),
            ("training_data_sha256", self.training_data_sha256),
        ):
            _sha256(value, label)
        _sorted_unique_ids(self.training_case_ids, "training_case_ids")
        _sorted_unique_ids(self.training_event_ids, "training_event_ids")
        _positive_integer(self.positive_event_count, "positive_event_count")
        _positive_integer(self.negative_event_count, "negative_event_count")
        if (
            not isinstance(self.knots, tuple)
            or not self.knots
            or any(not isinstance(value, IsotonicKnot) for value in self.knots)
        ):
            raise ValueError("calibrator requires immutable isotonic knots")
        raw = tuple(value.raw_probability for value in self.knots)
        fitted = tuple(value.calibrated_probability for value in self.knots)
        if any(current <= previous for previous, current in zip(raw, raw[1:])):
            raise ValueError("isotonic raw-probability knots must strictly increase")
        if any(
            current < previous for previous, current in zip(fitted, fitted[1:])
        ):
            raise ValueError("isotonic calibrated probabilities must not decrease")

    def calibrate(self, raw_probability: float) -> tuple[float, bool]:
        """Return clipped-linear isotonic output and extrapolation flag."""

        _probability(raw_probability, "raw_probability")
        if raw_probability <= self.knots[0].raw_probability:
            return (
                self.knots[0].calibrated_probability,
                raw_probability < self.knots[0].raw_probability,
            )
        if raw_probability >= self.knots[-1].raw_probability:
            return (
                self.knots[-1].calibrated_probability,
                raw_probability > self.knots[-1].raw_probability,
            )
        for left, right in zip(self.knots, self.knots[1:]):
            if raw_probability <= right.raw_probability:
                fraction = (
                    (raw_probability - left.raw_probability)
                    / (right.raw_probability - left.raw_probability)
                )
                value = left.calibrated_probability + fraction * (
                    right.calibrated_probability
                    - left.calibrated_probability
                )
                return min(1.0, max(0.0, value)), False
        raise AssertionError("isotonic interpolation failed to select a knot interval")


@dataclass(frozen=True, slots=True)
class CalibratedProbability:
    query_id: str
    raw_probability: float
    calibrated_probability: float
    calibration_version: str
    scope_id: str
    extrapolated: bool
    selection_rank: int
    selection_order: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class HeldOutCalibrationEvaluation:
    evaluation_id: str
    calibration_version: str
    partition: CalibrationPartition
    event_count: int
    positive_event_count: int
    negative_event_count: int
    raw_event_balanced_brier: float
    calibrated_event_balanced_brier: float
    calibrated_minus_raw_brier: float
    evidence_sha256: str


def fit_event_balanced_isotonic_calibrator(
    cases: Iterable[PassageCalibrationCase],
    *,
    scope: CalibrationScope,
    policy: CalibrationPolicy,
) -> EventBalancedIsotonicCalibrator:
    """Fit PAV isotonic regression using training events with equal total weight."""

    values = _validated_cases(cases)
    if any(value.partition is not CalibrationPartition.TRAIN for value in values):
        raise ValueError("calibrator fitting accepts training cases only")
    if any(not scope.matches(value) for value in values):
        raise ValueError("training case falls outside the requested calibration scope")
    corpus_sha256, split_sha256 = _single_evidence_context(values)
    event_cases = _group_by_event(values)
    positive_events = sum(
        any(case.passage_occurred for case in grouped)
        for grouped in event_cases.values()
    )
    negative_events = sum(
        any(not case.passage_occurred for case in grouped)
        for grouped in event_cases.values()
    )
    _require_support(
        event_count=len(event_cases),
        positive_event_count=positive_events,
        negative_event_count=negative_events,
        policy=policy,
    )

    weighted_rows: list[tuple[float, float, float, str]] = []
    for event_id, grouped in sorted(event_cases.items()):
        case_weight = 1.0 / len(grouped)
        for case in grouped:
            weighted_rows.append(
                (
                    case.raw_probability,
                    1.0 if case.passage_occurred else 0.0,
                    case_weight,
                    event_id,
                )
            )
    knots = _pav_knots(weighted_rows)
    training_data_sha256 = _stable_sha256(
        {"cases": [value.canonical for value in values]}
    )
    version_payload = {
        "algorithm_version": EVENT_PROBABILITY_CALIBRATION_VERSION,
        "scope": scope.canonical,
        "policy": policy.canonical,
        "corpus_sha256": corpus_sha256,
        "split_sha256": split_sha256,
        "training_data_sha256": training_data_sha256,
        "knots": [
            {
                "raw_probability": _float_token(knot.raw_probability),
                "calibrated_probability": _float_token(
                    knot.calibrated_probability
                ),
                "event_balanced_weight": _float_token(
                    knot.event_balanced_weight
                ),
                "case_count": knot.case_count,
            }
            for knot in knots
        ],
    }
    calibration_version = "calibration:" + _stable_sha256(version_payload)
    return EventBalancedIsotonicCalibrator(
        calibration_version=calibration_version,
        scope=scope,
        policy=policy,
        corpus_sha256=corpus_sha256,
        split_sha256=split_sha256,
        training_data_sha256=training_data_sha256,
        training_case_ids=tuple(sorted(value.case_id for value in values)),
        training_event_ids=tuple(sorted(event_cases)),
        positive_event_count=positive_events,
        negative_event_count=negative_events,
        knots=knots,
    )


def calibrate_query(
    query: ProbabilityCalibrationQuery,
    ordered_calibrators: tuple[EventBalancedIsotonicCalibrator, ...],
) -> CalibratedProbability:
    """Apply the first caller-ordered matching calibrator, never a hidden fallback."""

    if not isinstance(query, ProbabilityCalibrationQuery):
        raise TypeError("query must be ProbabilityCalibrationQuery")
    if (
        not isinstance(ordered_calibrators, tuple)
        or not ordered_calibrators
        or any(
            not isinstance(value, EventBalancedIsotonicCalibrator)
            for value in ordered_calibrators
        )
    ):
        raise ValueError("ordered_calibrators must be a non-empty immutable tuple")
    versions = tuple(value.calibration_version for value in ordered_calibrators)
    if len(versions) != len(set(versions)):
        raise ValueError("ordered_calibrators contains duplicate versions")
    for index, calibrator in enumerate(ordered_calibrators):
        if calibrator.scope.matches(query):
            probability, extrapolated = calibrator.calibrate(
                query.raw_probability
            )
            return CalibratedProbability(
                query_id=query.query_id,
                raw_probability=query.raw_probability,
                calibrated_probability=probability,
                calibration_version=calibrator.calibration_version,
                scope_id=calibrator.scope.scope_id,
                extrapolated=extrapolated,
                selection_rank=index,
                selection_order=versions,
            )
    raise LookupError("no caller-supplied calibrator matches the probability query")


def evaluate_calibrator_held_out(
    calibrator: EventBalancedIsotonicCalibrator,
    cases: Iterable[PassageCalibrationCase],
    *,
    partition: CalibrationPartition,
) -> HeldOutCalibrationEvaluation:
    """Report held-out Brier values without choosing or enforcing a release gate."""

    if not isinstance(calibrator, EventBalancedIsotonicCalibrator):
        raise TypeError("calibrator must be EventBalancedIsotonicCalibrator")
    if partition not in (
        CalibrationPartition.VALIDATION,
        CalibrationPartition.TEST,
    ):
        raise ValueError("held-out calibration evaluation requires validation or test")
    values = _validated_cases(cases)
    if any(value.partition is not partition for value in values):
        raise ValueError("evaluation cases must all use the requested held-out partition")
    if any(not calibrator.scope.matches(value) for value in values):
        raise ValueError("held-out case falls outside the calibrator scope")
    corpus_sha256, split_sha256 = _single_evidence_context(values)
    if (
        corpus_sha256 != calibrator.corpus_sha256
        or split_sha256 != calibrator.split_sha256
    ):
        raise ValueError("held-out corpus or split differs from calibrator provenance")
    training_events = set(calibrator.training_event_ids)
    heldout_events = {value.event_id for value in values}
    overlap = training_events & heldout_events
    if overlap:
        raise ValueError("training and held-out meteorological events overlap")

    grouped = _group_by_event(values)
    raw_by_event: list[float] = []
    calibrated_by_event: list[float] = []
    for event_cases in grouped.values():
        raw_losses: list[float] = []
        calibrated_losses: list[float] = []
        for case in event_cases:
            truth = 1.0 if case.passage_occurred else 0.0
            calibrated, _ = calibrator.calibrate(case.raw_probability)
            raw_losses.append((case.raw_probability - truth) ** 2)
            calibrated_losses.append((calibrated - truth) ** 2)
        raw_by_event.append(sum(raw_losses) / len(raw_losses))
        calibrated_by_event.append(
            sum(calibrated_losses) / len(calibrated_losses)
        )
    raw_brier = sum(raw_by_event) / len(raw_by_event)
    calibrated_brier = sum(calibrated_by_event) / len(calibrated_by_event)
    positive_events = sum(
        any(case.passage_occurred for case in event_cases)
        for event_cases in grouped.values()
    )
    negative_events = sum(
        any(not case.passage_occurred for case in event_cases)
        for event_cases in grouped.values()
    )
    evidence_sha256 = _stable_sha256(
        {
            "calibration_version": calibrator.calibration_version,
            "partition": partition.value,
            "cases": [value.canonical for value in values],
            "raw_event_balanced_brier": _float_token(raw_brier),
            "calibrated_event_balanced_brier": _float_token(calibrated_brier),
        }
    )
    return HeldOutCalibrationEvaluation(
        evaluation_id="calibration-evaluation:" + evidence_sha256,
        calibration_version=calibrator.calibration_version,
        partition=partition,
        event_count=len(grouped),
        positive_event_count=positive_events,
        negative_event_count=negative_events,
        raw_event_balanced_brier=raw_brier,
        calibrated_event_balanced_brier=calibrated_brier,
        calibrated_minus_raw_brier=calibrated_brier - raw_brier,
        evidence_sha256=evidence_sha256,
    )


def _pav_knots(
    rows: list[tuple[float, float, float, str]],
) -> tuple[IsotonicKnot, ...]:
    by_probability: dict[float, list[tuple[float, float, str]]] = {}
    for probability, outcome, weight, event_id in rows:
        by_probability.setdefault(probability, []).append(
            (outcome, weight, event_id)
        )
    ordered_probabilities = sorted(by_probability)
    point_weights: list[float] = []
    point_weighted_outcomes: list[float] = []
    point_case_counts: list[int] = []
    for probability in ordered_probabilities:
        values = by_probability[probability]
        point_weights.append(sum(weight for _, weight, _ in values))
        point_weighted_outcomes.append(
            sum(outcome * weight for outcome, weight, _ in values)
        )
        point_case_counts.append(len(values))

    blocks: list[dict[str, float | int]] = []
    for index, (weight, weighted_outcome) in enumerate(
        zip(point_weights, point_weighted_outcomes)
    ):
        blocks.append(
            {
                "start": index,
                "end": index,
                "weight": weight,
                "weighted_outcome": weighted_outcome,
            }
        )
        while len(blocks) >= 2 and _block_mean(blocks[-2]) > _block_mean(
            blocks[-1]
        ):
            right = blocks.pop()
            left = blocks.pop()
            blocks.append(
                {
                    "start": int(left["start"]),
                    "end": int(right["end"]),
                    "weight": float(left["weight"]) + float(right["weight"]),
                    "weighted_outcome": float(left["weighted_outcome"])
                    + float(right["weighted_outcome"]),
                }
            )

    fitted = [0.0] * len(ordered_probabilities)
    for block in blocks:
        mean = _block_mean(block)
        for index in range(int(block["start"]), int(block["end"]) + 1):
            fitted[index] = mean
    return tuple(
        IsotonicKnot(
            raw_probability=probability,
            calibrated_probability=fitted[index],
            event_balanced_weight=point_weights[index],
            case_count=point_case_counts[index],
        )
        for index, probability in enumerate(ordered_probabilities)
    )


def _block_mean(block: dict[str, float | int]) -> float:
    return float(block["weighted_outcome"]) / float(block["weight"])


def _validated_cases(
    cases: Iterable[PassageCalibrationCase],
) -> tuple[PassageCalibrationCase, ...]:
    values = tuple(cases)
    if not values:
        raise ValueError("calibration requires at least one case")
    if any(not isinstance(value, PassageCalibrationCase) for value in values):
        raise TypeError("calibration cases must be PassageCalibrationCase")
    case_ids = [value.case_id for value in values]
    if len(case_ids) != len(set(case_ids)):
        raise ValueError("calibration case IDs must be unique")
    return tuple(sorted(values, key=lambda value: value.case_id))


def _single_evidence_context(
    values: tuple[PassageCalibrationCase, ...],
) -> tuple[str, str]:
    corpora = {value.corpus_sha256 for value in values}
    splits = {value.split_sha256 for value in values}
    if len(corpora) != 1 or len(splits) != 1:
        raise ValueError("calibration cases must share one immutable corpus and split")
    return next(iter(corpora)), next(iter(splits))


def _group_by_event(
    values: tuple[PassageCalibrationCase, ...],
) -> dict[str, tuple[PassageCalibrationCase, ...]]:
    grouped: dict[str, list[PassageCalibrationCase]] = {}
    for value in values:
        grouped.setdefault(value.event_id, []).append(value)
    return {
        event_id: tuple(sorted(event_cases, key=lambda value: value.case_id))
        for event_id, event_cases in sorted(grouped.items())
    }


def _require_support(
    *,
    event_count: int,
    positive_event_count: int,
    negative_event_count: int,
    policy: CalibrationPolicy,
) -> None:
    shortages = []
    if event_count < policy.minimum_events:
        shortages.append(f"events {event_count}/{policy.minimum_events}")
    if positive_event_count < policy.minimum_positive_events:
        shortages.append(
            "positive events "
            f"{positive_event_count}/{policy.minimum_positive_events}"
        )
    if negative_event_count < policy.minimum_negative_events:
        shortages.append(
            "negative events "
            f"{negative_event_count}/{policy.minimum_negative_events}"
        )
    if shortages:
        raise InsufficientCalibrationSupport(
            "insufficient calibration support: " + ", ".join(shortages)
        )


def _identifier(value: object, label: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} must be a non-empty string")


def _sha256(value: object, label: str) -> None:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError(f"{label} must be a lowercase SHA-256")


def _probability(value: object, label: str) -> None:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        or float(value) < 0.0
        or float(value) > 1.0
    ):
        raise ValueError(f"{label} must be finite and in [0, 1]")


def _positive_integer(value: object, label: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{label} must be a positive integer")


def _sorted_unique_ids(values: tuple[str, ...], label: str) -> None:
    if (
        not isinstance(values, tuple)
        or not values
        or any(not isinstance(value, str) or not value for value in values)
        or tuple(sorted(values)) != values
        or len(values) != len(set(values))
    ):
        raise ValueError(f"{label} must be a sorted unique immutable tuple")


def _float_token(value: float) -> str:
    return format(float(value), ".17g")


def _stable_sha256(value: object) -> str:
    payload = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()
