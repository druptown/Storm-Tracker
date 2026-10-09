"""Pure, event-held-out verification metrics for vNext shadow forecasts.

The module is independent of pySTEPS at runtime and never creates truth,
probabilities, calibration, margins, release thresholds, or default scientific
sample sizes.  Every bootstrap draw samples meteorological event IDs, never
frames or individual forecast cases.
"""
from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass
from enum import Enum
from typing import Callable, Iterable

from .analysis_grid import AnalysisGrid
from .tracker_forecast_contract import (
    ConditionalDistributionKind,
    ForecastState,
    TrackerForecast,
)


FORECAST_VERIFICATION_VERSION = "stv3-event-held-out-verification-v2"
BOOTSTRAP_ALGORITHM_VERSION = "sha256-event-resampling-v1"
FSS_ALGORITHM_VERSION = "masked-full-window-fss-v1"
ROC_ALGORITHM_VERSION = "event-weighted-empirical-roc-v1"
SCALAR_ENSEMBLE_ALGORITHM_VERSION = "event-weighted-ensemble-crps-rank-v1"
_TOLERANCE = 1e-12
_TIME_TOLERANCE_SECONDS = 1e-6


class VerificationPartition(str, Enum):
    TRAIN = "train"
    VALIDATION = "validation"
    TEST = "test"


class ThresholdOperator(str, Enum):
    GREATER_OR_EQUAL = "greater_or_equal"


@dataclass(frozen=True, slots=True)
class BootstrapConfig:
    """Caller-supplied event-bootstrap configuration with no defaults."""

    seed: str
    replicates: int
    lower_probability: float
    upper_probability: float
    minimum_events: int

    def __post_init__(self) -> None:
        _identifier(self.seed, "seed")
        _positive_int(self.replicates, "replicates")
        _positive_int(self.minimum_events, "minimum_events")
        if self.minimum_events < 2:
            raise ValueError("minimum_events must be at least two")
        _finite(self.lower_probability, "lower_probability")
        _finite(self.upper_probability, "upper_probability")
        if not (
            0.0
            < self.lower_probability
            < self.upper_probability
            < 1.0
        ):
            raise ValueError("bootstrap interval probabilities must lie inside (0, 1)")

    @property
    def seed_sha256(self) -> str:
        return hashlib.sha256(self.seed.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class BootstrapEstimate:
    metric_id: str
    point_estimate: float
    lower: float
    upper: float
    event_count: int
    replicates: int
    lower_probability: float
    upper_probability: float
    seed_sha256: str
    algorithm_version: str = BOOTSTRAP_ALGORITHM_VERSION

    def __post_init__(self) -> None:
        _identifier(self.metric_id, "metric_id")
        for label, value in (
            ("point_estimate", self.point_estimate),
            ("lower", self.lower),
            ("upper", self.upper),
            ("lower_probability", self.lower_probability),
            ("upper_probability", self.upper_probability),
        ):
            _finite(value, label)
        if self.lower > self.upper:
            raise ValueError("bootstrap lower cannot exceed upper")
        _positive_int(self.event_count, "event_count")
        _positive_int(self.replicates, "replicates")
        _sha256(self.seed_sha256, "seed_sha256")
        _identifier(self.algorithm_version, "algorithm_version")


@dataclass(frozen=True, slots=True)
class BaselinePassageForecast:
    """Explicit probability baseline paired to one candidate forecast case."""

    baseline_forecast_id: str
    evaluation_stratum_id: str
    event_id: str
    frame_id: str
    partition: VerificationPartition
    tracker_id: str
    reference_valid_at: float
    horizon_seconds: float
    passage_probability: float
    no_passage_probability: float
    algorithm_version: str
    input_field_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        for label, value in (
            ("baseline_forecast_id", self.baseline_forecast_id),
            ("evaluation_stratum_id", self.evaluation_stratum_id),
            ("event_id", self.event_id),
            ("frame_id", self.frame_id),
            ("tracker_id", self.tracker_id),
            ("algorithm_version", self.algorithm_version),
        ):
            _identifier(value, label)
        if not isinstance(self.partition, VerificationPartition):
            raise TypeError("partition must be VerificationPartition")
        _finite(self.reference_valid_at, "reference_valid_at")
        _positive_finite(self.horizon_seconds, "horizon_seconds")
        _probability_pair(
            self.passage_probability,
            self.no_passage_probability,
            "baseline",
        )
        _unique_ids(self.input_field_ids, "input_field_ids")


@dataclass(frozen=True, slots=True)
class PassageVerificationCase:
    """One independently labeled candidate/baseline passage pair."""

    case_id: str
    evaluation_stratum_id: str
    event_id: str
    frame_id: str
    partition: VerificationPartition
    forecast: TrackerForecast
    baseline: BaselinePassageForecast
    label_id: str
    label_source_id: str
    label_state: str
    truth_source_eligible: bool
    passage_occurred: bool
    passage_time_utc: float | None

    def __post_init__(self) -> None:
        for label, value in (
            ("case_id", self.case_id),
            ("evaluation_stratum_id", self.evaluation_stratum_id),
            ("event_id", self.event_id),
            ("frame_id", self.frame_id),
            ("label_id", self.label_id),
            ("label_source_id", self.label_source_id),
        ):
            _identifier(value, label)
        if not isinstance(self.partition, VerificationPartition):
            raise TypeError("partition must be VerificationPartition")
        if not isinstance(self.forecast, TrackerForecast):
            raise TypeError("forecast must be TrackerForecast")
        if self.forecast.state is not ForecastState.DETERMINATE:
            raise ValueError("verification requires a DETERMINATE forecast")
        if not isinstance(self.baseline, BaselinePassageForecast):
            raise TypeError("baseline must be BaselinePassageForecast")
        if self.label_state != "confirmed":
            raise ValueError("passage truth label must be confirmed")
        if self.truth_source_eligible is not True:
            raise ValueError("passage truth source is not independently eligible")
        if not isinstance(self.passage_occurred, bool):
            raise TypeError("passage_occurred must be bool")
        if self.passage_occurred:
            _finite(self.passage_time_utc, "passage_time_utc")
            if not (
                self.forecast.reference_valid_at
                <= float(self.passage_time_utc)
                <= self.forecast.reference_valid_at
                + self.forecast.horizon_seconds
            ):
                raise ValueError("passage time lies outside forecast horizon")
        elif self.passage_time_utc is not None:
            raise ValueError("no-passage truth cannot carry passage time")
        if self.baseline.evaluation_stratum_id != self.evaluation_stratum_id:
            raise ValueError("baseline/candidate evaluation stratum mismatch")
        if self.baseline.baseline_forecast_id == self.forecast.forecast_id:
            raise ValueError("baseline and candidate forecast IDs must differ")
        if (
            self.baseline.event_id != self.event_id
            or self.baseline.frame_id != self.frame_id
            or self.baseline.partition is not self.partition
        ):
            raise ValueError("baseline/candidate event, frame, or partition mismatch")
        if self.baseline.tracker_id != self.forecast.tracker_id:
            raise ValueError("baseline/candidate tracker mismatch")
        if not math.isclose(
            self.baseline.reference_valid_at,
            self.forecast.reference_valid_at,
            rel_tol=0.0,
            abs_tol=_TIME_TOLERANCE_SECONDS,
        ) or not math.isclose(
            self.baseline.horizon_seconds,
            self.forecast.horizon_seconds,
            rel_tol=1e-12,
            abs_tol=_TIME_TOLERANCE_SECONDS,
        ):
            raise ValueError("baseline/candidate time contract mismatch")


@dataclass(frozen=True, slots=True)
class ReliabilityBin:
    lower_probability: float
    upper_probability: float
    includes_upper: bool
    sample_count: int
    event_count: int
    forecast_mean: float | None
    outcome_frequency: float | None


@dataclass(frozen=True, slots=True)
class SharpnessBin:
    lower_probability: float
    upper_probability: float
    includes_upper: bool
    sample_count: int
    event_count: int
    event_weighted_mass: float


@dataclass(frozen=True, slots=True)
class SharpnessSummary:
    bins: tuple[SharpnessBin, ...]
    event_weighted_mean_probability: float
    event_weighted_variance: float


@dataclass(frozen=True, slots=True)
class PassageClassificationSummary:
    decision_threshold: float
    true_positive_count: int
    true_negative_count: int
    false_passage_count: int
    missed_passage_count: int
    observed_passage_count: int
    observed_no_passage_count: int
    false_passage_rate: float
    missed_passage_rate: float


@dataclass(frozen=True, slots=True)
class RocPoint:
    """One empirical ROC operating point.

    ``threshold=None`` is the explicit no-positive endpoint above the largest
    forecast probability. All other points use ``probability >= threshold``.
    Counts are raw cases; rates first average within a meteorological event and
    then across events containing the relevant truth class.
    """

    threshold: float | None
    true_positive_count: int
    false_positive_count: int
    true_positive_rate: float
    false_positive_rate: float


@dataclass(frozen=True, slots=True)
class PassageRocVerification:
    points: tuple[RocPoint, ...]
    area_under_curve: float
    observed_passage_count: int
    observed_no_passage_count: int
    passage_event_count: int
    no_passage_event_count: int
    algorithm_version: str = ROC_ALGORITHM_VERSION


@dataclass(frozen=True, slots=True)
class PassageProbabilityVerification:
    evaluation_stratum_id: str
    partition: VerificationPartition
    case_count: int
    event_count: int
    candidate_brier: BootstrapEstimate
    baseline_brier: BootstrapEstimate
    brier_skill_score: BootstrapEstimate
    reliability_bins: tuple[ReliabilityBin, ...]
    sharpness: SharpnessSummary
    classification: PassageClassificationSummary
    roc: PassageRocVerification
    verification_version: str = FORECAST_VERIFICATION_VERSION


@dataclass(frozen=True, slots=True)
class QuantilePinballResult:
    probability: float
    loss_seconds: BootstrapEstimate


@dataclass(frozen=True, slots=True)
class ConditionalEtaVerification:
    evaluation_stratum_id: str
    partition: VerificationPartition
    passage_case_count: int
    passage_event_count: int
    excluded_no_passage_case_count: int
    median_mae_seconds: BootstrapEstimate
    median_bias_seconds: BootstrapEstimate
    quantile_pinball: tuple[QuantilePinballResult, ...]
    interval_coverage: BootstrapEstimate
    mean_interval_width_seconds: BootstrapEstimate
    verification_version: str = FORECAST_VERIFICATION_VERSION


@dataclass(frozen=True, slots=True)
class MaskedVerificationField:
    """Immutable forecast or independent truth field with explicit nodata."""

    field_id: str
    grid: AnalysisGrid
    valid_at: float
    variable_id: str
    unit_id: str
    values: tuple[tuple[float, ...], ...]
    valid_mask: tuple[tuple[bool, ...], ...]
    algorithm_version: str
    artifact_sha256: str | None

    def __post_init__(self) -> None:
        for label, value in (
            ("field_id", self.field_id),
            ("variable_id", self.variable_id),
            ("unit_id", self.unit_id),
            ("algorithm_version", self.algorithm_version),
        ):
            _identifier(value, label)
        if not isinstance(self.grid, AnalysisGrid):
            raise TypeError("field grid must be AnalysisGrid")
        _finite(self.valid_at, "valid_at")
        _matrix(self.values, self.grid, "values")
        _matrix(self.valid_mask, self.grid, "valid_mask")
        if any(
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
            for row in self.values
            for value in row
        ):
            raise ValueError("field values must be finite")
        if any(
            not isinstance(value, bool)
            for row in self.valid_mask
            for value in row
        ):
            raise TypeError("valid_mask must contain booleans")
        if self.artifact_sha256 is not None:
            _sha256(self.artifact_sha256, "artifact_sha256")


@dataclass(frozen=True, slots=True)
class FieldVerificationCase:
    case_id: str
    evaluation_stratum_id: str
    event_id: str
    frame_id: str
    partition: VerificationPartition
    candidate: MaskedVerificationField
    truth: MaskedVerificationField
    label_id: str
    label_source_id: str
    label_state: str
    truth_source_eligible: bool

    def __post_init__(self) -> None:
        for label, value in (
            ("case_id", self.case_id),
            ("evaluation_stratum_id", self.evaluation_stratum_id),
            ("event_id", self.event_id),
            ("frame_id", self.frame_id),
            ("label_id", self.label_id),
            ("label_source_id", self.label_source_id),
        ):
            _identifier(value, label)
        if not isinstance(self.partition, VerificationPartition):
            raise TypeError("partition must be VerificationPartition")
        if not isinstance(self.candidate, MaskedVerificationField) or not isinstance(
            self.truth,
            MaskedVerificationField,
        ):
            raise TypeError("FSS case requires typed candidate and truth fields")
        if self.label_state != "confirmed":
            raise ValueError("precipitation field label must be confirmed")
        if self.truth_source_eligible is not True:
            raise ValueError("precipitation truth source is not independently eligible")
        if self.truth.artifact_sha256 is None:
            raise ValueError("independent truth field requires artifact SHA-256")
        if self.candidate.artifact_sha256 is not None and (
            self.candidate.artifact_sha256 == self.truth.artifact_sha256
        ):
            raise ValueError("candidate and independent truth artifacts must differ")
        self.candidate.grid.assert_same_geometry(self.truth.grid)
        if (
            self.candidate.variable_id != self.truth.variable_id
            or self.candidate.unit_id != self.truth.unit_id
        ):
            raise ValueError("candidate/truth variable or unit mismatch")
        if not math.isclose(
            self.candidate.valid_at,
            self.truth.valid_at,
            rel_tol=0.0,
            abs_tol=_TIME_TOLERANCE_SECONDS,
        ):
            raise ValueError("candidate/truth valid time mismatch")
        if self.candidate.valid_mask != self.truth.valid_mask:
            raise ValueError("candidate/truth nodata masks differ")
        if not any(value for row in self.truth.valid_mask for value in row):
            raise ValueError("field verification has no valid pixels")


@dataclass(frozen=True, slots=True)
class FssConfig:
    threshold: float
    threshold_operator: ThresholdOperator
    window_rows: int
    window_columns: int
    minimum_valid_fraction: float

    def __post_init__(self) -> None:
        _finite(self.threshold, "threshold")
        if not isinstance(self.threshold_operator, ThresholdOperator):
            raise TypeError("threshold_operator must be ThresholdOperator")
        _positive_int(self.window_rows, "window_rows")
        _positive_int(self.window_columns, "window_columns")
        _finite(self.minimum_valid_fraction, "minimum_valid_fraction")
        if not 0.0 < self.minimum_valid_fraction <= 1.0:
            raise ValueError("minimum_valid_fraction must lie in (0, 1]")


@dataclass(frozen=True, slots=True)
class MaskedFssVerification:
    evaluation_stratum_id: str
    partition: VerificationPartition
    variable_id: str
    unit_id: str
    grid_id: str
    threshold: float
    threshold_operator: ThresholdOperator
    window_rows: int
    window_columns: int
    window_width_m: float
    window_height_m: float
    minimum_valid_fraction: float
    case_count: int
    event_count: int
    eligible_window_count: int
    excluded_window_count: int
    fractions_brier_score: float
    fractions_reference_mse: float
    fractions_skill_score: BootstrapEstimate
    algorithm_version: str = FSS_ALGORITHM_VERSION


@dataclass(frozen=True, slots=True)
class ScalarEnsembleVerificationCase:
    """One complete exchangeable scalar ensemble and independent point truth."""

    case_id: str
    evaluation_stratum_id: str
    event_id: str
    frame_id: str
    partition: VerificationPartition
    ensemble_id: str
    valid_at: float
    quantity_id: str
    unit_id: str
    member_ids: tuple[str, ...]
    member_values: tuple[float, ...]
    members_exchangeable: bool
    member_generation_algorithm_version: str
    calibration_version: str | None
    candidate_artifact_sha256: str | None
    observed_value: float
    truth_artifact_sha256: str
    label_id: str
    label_source_id: str
    label_state: str
    truth_source_eligible: bool

    def __post_init__(self) -> None:
        for label, value in (
            ("case_id", self.case_id),
            ("evaluation_stratum_id", self.evaluation_stratum_id),
            ("event_id", self.event_id),
            ("frame_id", self.frame_id),
            ("ensemble_id", self.ensemble_id),
            ("quantity_id", self.quantity_id),
            ("unit_id", self.unit_id),
            (
                "member_generation_algorithm_version",
                self.member_generation_algorithm_version,
            ),
            ("label_id", self.label_id),
            ("label_source_id", self.label_source_id),
        ):
            _identifier(value, label)
        if not isinstance(self.partition, VerificationPartition):
            raise TypeError("partition must be VerificationPartition")
        _finite(self.valid_at, "valid_at")
        if len(self.member_ids) < 2:
            raise ValueError("scalar ensemble requires at least two members")
        _unique_ids(self.member_ids, "member_ids")
        if len(self.member_values) != len(self.member_ids):
            raise ValueError("member_ids and member_values length mismatch")
        for value in self.member_values:
            _finite(value, "member value")
        if self.members_exchangeable is not True:
            raise ValueError(
                "rank verification requires explicitly exchangeable members"
            )
        if self.calibration_version is not None:
            _identifier(self.calibration_version, "calibration_version")
        if self.candidate_artifact_sha256 is not None:
            _sha256(self.candidate_artifact_sha256, "candidate_artifact_sha256")
        _finite(self.observed_value, "observed_value")
        _sha256(self.truth_artifact_sha256, "truth_artifact_sha256")
        if self.candidate_artifact_sha256 == self.truth_artifact_sha256:
            raise ValueError("candidate and independent truth artifacts must differ")
        if self.label_state != "confirmed":
            raise ValueError("scalar truth label must be confirmed")
        if self.truth_source_eligible is not True:
            raise ValueError("scalar truth source is not independently eligible")


@dataclass(frozen=True, slots=True)
class RankHistogramBin:
    """Verification-rank bin with fractional tie handling."""

    rank_index: int
    raw_fractional_count: float
    event_weighted_mass: float


@dataclass(frozen=True, slots=True)
class ScalarEnsembleVerification:
    evaluation_stratum_id: str
    partition: VerificationPartition
    quantity_id: str
    unit_id: str
    member_count: int
    case_count: int
    event_count: int
    ensemble_mean_mae: BootstrapEstimate
    ensemble_mean_bias: BootstrapEstimate
    crps: BootstrapEstimate
    rank_histogram: tuple[RankHistogramBin, ...]
    algorithm_version: str = SCALAR_ENSEMBLE_ALGORITHM_VERSION
    verification_version: str = FORECAST_VERIFICATION_VERSION


def build_passage_verification_case(
    *,
    case_id: str,
    evaluation_stratum_id: str,
    event_id: str,
    frame_id: str,
    expected_partition: VerificationPartition,
    forecast: TrackerForecast,
    baseline: BaselinePassageForecast,
    truth_context: object,
    passage_label_id: str,
) -> PassageVerificationCase:
    """Bind a forecast to one confirmed independent PassageLabel.

    ``truth_context`` is intentionally duck-typed so this exact engine mirror
    remains independent from the worker-only ``validation_corpus`` module.
    Its required surface is ``split.partition_for_*`` and
    ``labels.label/source``.
    """

    _validate_truth_context_surface(truth_context)
    if not isinstance(expected_partition, VerificationPartition):
        raise TypeError("expected_partition must be VerificationPartition")
    split = truth_context.split
    event_partition = split.partition_for_event(event_id)
    frame_partition = split.partition_for_frame(frame_id)
    if event_partition is None or frame_partition is None:
        raise ValueError("event or frame is absent from validation split")
    if (
        getattr(event_partition, "value", None) != expected_partition.value
        or getattr(frame_partition, "value", None) != expected_partition.value
    ):
        raise ValueError("event/frame partition mismatch")
    frame_events = dict(split.frame_events)
    if frame_events.get(frame_id) != event_id:
        raise ValueError("frame does not belong to requested event")

    label = truth_context.labels.label(passage_label_id)
    if label is None:
        raise ValueError("required passage label is missing")
    if (
        getattr(label.label_kind, "value", None) != "passage"
        or getattr(label.state, "value", None) != "confirmed"
    ):
        raise ValueError("passage label is missing, ambiguous, or unknown")
    if label.event_id != event_id or label.frame_id != frame_id:
        raise ValueError("passage label event/frame mismatch")
    source = truth_context.labels.source(label.source_id)
    if source is None or source.is_external_truth_eligible is not True:
        raise ValueError("passage label source is not independent external truth")
    source_tokens = " ".join(str(getattr(source, name, "")).lower() for name in (
        "source_id", "producer", "methodology_id",
    ))
    if "storm_tracker" in source_tokens or "fusion:" in source_tokens:
        raise ValueError("internal fused radar is not independent external truth")
    payload = label.payload
    if payload.tracker_id != forecast.tracker_id:
        raise ValueError("passage label/candidate tracker mismatch")
    if payload.occurred is None:
        raise ValueError("passage label outcome is unknown")
    if not math.isclose(
        payload.window_start_utc,
        forecast.reference_valid_at,
        rel_tol=0.0,
        abs_tol=_TIME_TOLERANCE_SECONDS,
    ) or not math.isclose(
        payload.window_end_utc,
        forecast.reference_valid_at + forecast.horizon_seconds,
        rel_tol=0.0,
        abs_tol=_TIME_TOLERANCE_SECONDS,
    ):
        raise ValueError("passage label window and forecast horizon mismatch")
    return PassageVerificationCase(
        case_id=case_id,
        evaluation_stratum_id=evaluation_stratum_id,
        event_id=event_id,
        frame_id=frame_id,
        partition=expected_partition,
        forecast=forecast,
        baseline=baseline,
        label_id=label.label_id,
        label_source_id=source.source_id,
        label_state=label.state.value,
        truth_source_eligible=source.is_external_truth_eligible,
        passage_occurred=payload.occurred,
        passage_time_utc=payload.passage_time_utc,
    )


def verify_passage_probabilities(
    cases: Iterable[PassageVerificationCase],
    *,
    reliability_bin_edges: tuple[float, ...],
    decision_threshold: float,
    bootstrap: BootstrapConfig,
) -> PassageProbabilityVerification:
    """Compute event-weighted passage probability verification."""

    ordered = _passage_cases(cases, bootstrap)
    _probability_edges(reliability_bin_edges)
    _finite(decision_threshold, "decision_threshold")
    if not 0.0 <= decision_threshold <= 1.0:
        raise ValueError("decision_threshold must lie in [0, 1]")

    candidate_by_event = _event_means(
        ordered,
        lambda case: (
            float(case.forecast.passage_probability)
            - float(case.passage_occurred)
        )
        ** 2,
    )
    baseline_by_event = _event_means(
        ordered,
        lambda case: (
            case.baseline.passage_probability
            - float(case.passage_occurred)
        )
        ** 2,
    )
    candidate_brier_value = _mean(tuple(candidate_by_event.values()))
    baseline_brier_value = _mean(tuple(baseline_by_event.values()))
    if baseline_brier_value == 0.0:
        raise ValueError("Brier Skill Score baseline denominator is zero")
    bss_value = 1.0 - candidate_brier_value / baseline_brier_value

    candidate_brier = _bootstrap_scalar(
        "candidate_brier_score",
        candidate_by_event,
        bootstrap,
    )
    baseline_brier = _bootstrap_scalar(
        "baseline_brier_score",
        baseline_by_event,
        bootstrap,
    )
    bss = _bootstrap_ratio_skill(
        "brier_skill_score",
        candidate_by_event,
        baseline_by_event,
        bootstrap,
        expected_point=bss_value,
    )
    reliability = _reliability(ordered, reliability_bin_edges)
    sharpness = _sharpness(ordered, reliability_bin_edges)
    classification = _classification(ordered, decision_threshold)
    roc = _roc(ordered)
    return PassageProbabilityVerification(
        evaluation_stratum_id=ordered[0].evaluation_stratum_id,
        partition=ordered[0].partition,
        case_count=len(ordered),
        event_count=len(candidate_by_event),
        candidate_brier=candidate_brier,
        baseline_brier=baseline_brier,
        brier_skill_score=bss,
        reliability_bins=reliability,
        sharpness=sharpness,
        classification=classification,
        roc=roc,
    )


def verify_conditional_eta(
    cases: Iterable[PassageVerificationCase],
    *,
    bootstrap: BootstrapConfig,
) -> ConditionalEtaVerification:
    """Verify ETA only for independently confirmed actual passages."""

    ordered = _passage_cases(cases, bootstrap, require_minimum=False)
    passage = tuple(case for case in ordered if case.passage_occurred)
    passage_events = {case.event_id for case in passage}
    _minimum_events(passage_events, bootstrap, "conditional ETA")
    if not passage:
        raise ValueError("conditional ETA has no actual-passage cases")
    quantile_levels: tuple[float, ...] | None = None
    interval_levels: tuple[float, float] | None = None
    distribution_signature: tuple[object, ...] | None = None
    for case in passage:
        distribution = case.forecast.conditional_eta_distribution
        interval = case.forecast.conditional_eta_interval
        if (
            distribution is None
            or distribution.kind
            is not ConditionalDistributionKind.CONDITIONAL_ETA
            or interval is None
        ):
            raise ValueError(
                "actual passage cannot be verified without conditional ETA evidence"
            )
        levels = tuple(point.probability for point in distribution.quantiles)
        current_interval = (
            interval.lower_probability,
            interval.upper_probability,
        )
        current_signature = (
            distribution.basis,
            distribution.quantity,
            distribution.unit,
            distribution.calibration_version,
            distribution.source_algorithm_version,
        )
        if quantile_levels is None:
            quantile_levels = levels
            interval_levels = current_interval
            distribution_signature = current_signature
        elif (
            levels != quantile_levels
            or current_interval != interval_levels
            or current_signature != distribution_signature
        ):
            raise ValueError("ETA quantile or interval definitions differ within stratum")

    def actual_eta(case: PassageVerificationCase) -> float:
        return float(case.passage_time_utc) - case.forecast.reference_valid_at

    median_error_by_event = _event_means(
        passage,
        lambda case: case.forecast.conditional_eta_distribution.quantile_value(0.5)
        - actual_eta(case),
    )
    median_absolute_by_event = _event_means(
        passage,
        lambda case: abs(
            case.forecast.conditional_eta_distribution.quantile_value(0.5)
            - actual_eta(case)
        ),
    )
    coverage_by_event = _event_means(
        passage,
        lambda case: float(
            case.forecast.conditional_eta_interval.lower_value
            <= actual_eta(case)
            <= case.forecast.conditional_eta_interval.upper_value
        ),
    )
    width_by_event = _event_means(
        passage,
        lambda case: (
            case.forecast.conditional_eta_interval.upper_value
            - case.forecast.conditional_eta_interval.lower_value
        ),
    )
    pinball: list[QuantilePinballResult] = []
    for probability in quantile_levels or ():
        losses = _event_means(
            passage,
            lambda case, probability=probability: _pinball_loss(
                probability,
                case.forecast.conditional_eta_distribution.quantile_value(
                    probability
                ),
                actual_eta(case),
            ),
        )
        pinball.append(
            QuantilePinballResult(
                probability=probability,
                loss_seconds=_bootstrap_scalar(
                    f"eta_pinball_p{probability:.12g}",
                    losses,
                    bootstrap,
                ),
            )
        )
    return ConditionalEtaVerification(
        evaluation_stratum_id=ordered[0].evaluation_stratum_id,
        partition=ordered[0].partition,
        passage_case_count=len(passage),
        passage_event_count=len(passage_events),
        excluded_no_passage_case_count=len(ordered) - len(passage),
        median_mae_seconds=_bootstrap_scalar(
            "eta_median_mae_seconds",
            median_absolute_by_event,
            bootstrap,
        ),
        median_bias_seconds=_bootstrap_scalar(
            "eta_median_bias_seconds",
            median_error_by_event,
            bootstrap,
        ),
        quantile_pinball=tuple(pinball),
        interval_coverage=_bootstrap_scalar(
            "eta_interval_coverage",
            coverage_by_event,
            bootstrap,
        ),
        mean_interval_width_seconds=_bootstrap_scalar(
            "eta_interval_width_seconds",
            width_by_event,
            bootstrap,
        ),
    )


def build_field_verification_case(
    *,
    case_id: str,
    evaluation_stratum_id: str,
    event_id: str,
    frame_id: str,
    expected_partition: VerificationPartition,
    candidate: MaskedVerificationField,
    truth: MaskedVerificationField,
    truth_context: object,
    precipitation_label_id: str,
) -> FieldVerificationCase:
    """Bind equal-grid fields to one confirmed independent field label."""

    _validate_truth_context_surface(truth_context)
    if not isinstance(expected_partition, VerificationPartition):
        raise TypeError("expected_partition must be VerificationPartition")
    split = truth_context.split
    event_partition = split.partition_for_event(event_id)
    frame_partition = split.partition_for_frame(frame_id)
    if (
        event_partition is None
        or frame_partition is None
        or getattr(event_partition, "value", None) != expected_partition.value
        or getattr(frame_partition, "value", None) != expected_partition.value
    ):
        raise ValueError("field event/frame partition mismatch")
    if dict(split.frame_events).get(frame_id) != event_id:
        raise ValueError("field frame does not belong to requested event")
    label = truth_context.labels.label(precipitation_label_id)
    if label is None:
        raise ValueError("required precipitation field label is missing")
    if (
        getattr(label.label_kind, "value", None) != "precipitation_field"
        or getattr(label.state, "value", None) != "confirmed"
    ):
        raise ValueError("precipitation field label is ambiguous or unknown")
    if label.event_id != event_id or label.frame_id != frame_id:
        raise ValueError("precipitation label event/frame mismatch")
    source = truth_context.labels.source(label.source_id)
    if source is None or source.is_external_truth_eligible is not True:
        raise ValueError("precipitation label source is not independent truth")
    payload = label.payload
    if (
        payload.grid_id != truth.grid.grid_id
        or payload.variable_id != truth.variable_id
        or payload.unit_id != truth.unit_id
        or payload.field_artifact_sha256 != truth.artifact_sha256
        or not math.isclose(
            payload.valid_time_utc,
            truth.valid_at,
            rel_tol=0.0,
            abs_tol=_TIME_TOLERANCE_SECONDS,
        )
    ):
        raise ValueError("precipitation label metadata and truth field mismatch")
    return FieldVerificationCase(
        case_id=case_id,
        evaluation_stratum_id=evaluation_stratum_id,
        event_id=event_id,
        frame_id=frame_id,
        partition=expected_partition,
        candidate=candidate,
        truth=truth,
        label_id=label.label_id,
        label_source_id=source.source_id,
        label_state=label.state.value,
        truth_source_eligible=source.is_external_truth_eligible,
    )


def verify_masked_fss(
    cases: Iterable[FieldVerificationCase],
    *,
    config: FssConfig,
    bootstrap: BootstrapConfig,
) -> MaskedFssVerification:
    """Compute masked FSS from full, unpadded moving windows.

    Fractions use only cells where the identical candidate/truth mask is true.
    A full rectangular window is eligible when its valid fraction reaches the
    explicit caller threshold.  Exceedance is ``value >= threshold``.
    """

    if not isinstance(config, FssConfig):
        raise TypeError("config must be FssConfig")
    ordered = tuple(sorted(cases, key=lambda item: item.case_id))
    if not ordered or any(not isinstance(item, FieldVerificationCase) for item in ordered):
        raise ValueError("FSS requires typed field cases")
    if len({item.case_id for item in ordered}) != len(ordered):
        raise ValueError("FSS case_id must be unique")
    _homogeneous_field_stratum(ordered)
    _minimum_events({item.event_id for item in ordered}, bootstrap, "FSS")
    grid = ordered[0].candidate.grid
    if config.window_rows > grid.height or config.window_columns > grid.width:
        raise ValueError("FSS window exceeds analysis grid")

    case_scores: dict[str, tuple[float, float, int, int]] = {}
    for case in ordered:
        numerator_values: list[float] = []
        reference_values: list[float] = []
        excluded = 0
        area = config.window_rows * config.window_columns
        for top in range(grid.height - config.window_rows + 1):
            for left in range(grid.width - config.window_columns + 1):
                valid = [
                    (row, column)
                    for row in range(top, top + config.window_rows)
                    for column in range(left, left + config.window_columns)
                    if case.truth.valid_mask[row][column]
                ]
                if len(valid) / area < config.minimum_valid_fraction:
                    excluded += 1
                    continue
                if not valid:
                    excluded += 1
                    continue
                forecast_fraction = _fraction_exceeding(
                    case.candidate,
                    valid,
                    config,
                )
                observed_fraction = _fraction_exceeding(
                    case.truth,
                    valid,
                    config,
                )
                numerator_values.append(
                    (forecast_fraction - observed_fraction) ** 2
                )
                reference_values.append(
                    forecast_fraction**2 + observed_fraction**2
                )
        if not numerator_values:
            raise ValueError(f"FSS case {case.case_id} has no eligible windows")
        case_scores[case.case_id] = (
            _mean(tuple(numerator_values)),
            _mean(tuple(reference_values)),
            len(numerator_values),
            excluded,
        )

    numerator_by_event: dict[str, list[float]] = {}
    reference_by_event: dict[str, list[float]] = {}
    for case in ordered:
        numerator_by_event.setdefault(case.event_id, []).append(
            case_scores[case.case_id][0]
        )
        reference_by_event.setdefault(case.event_id, []).append(
            case_scores[case.case_id][1]
        )
    event_numerator = {
        event_id: _mean(tuple(values))
        for event_id, values in numerator_by_event.items()
    }
    event_reference = {
        event_id: _mean(tuple(values))
        for event_id, values in reference_by_event.items()
    }
    numerator = _mean(tuple(event_numerator.values()))
    reference = _mean(tuple(event_reference.values()))
    if reference == 0.0:
        raise ValueError("FSS reference denominator is zero")
    fss_value = 1.0 - numerator / reference
    return MaskedFssVerification(
        evaluation_stratum_id=ordered[0].evaluation_stratum_id,
        partition=ordered[0].partition,
        variable_id=ordered[0].candidate.variable_id,
        unit_id=ordered[0].candidate.unit_id,
        grid_id=grid.grid_id,
        threshold=config.threshold,
        threshold_operator=config.threshold_operator,
        window_rows=config.window_rows,
        window_columns=config.window_columns,
        window_width_m=config.window_columns * grid.x_resolution_m,
        window_height_m=config.window_rows * grid.y_resolution_m,
        minimum_valid_fraction=config.minimum_valid_fraction,
        case_count=len(ordered),
        event_count=len(event_numerator),
        eligible_window_count=sum(value[2] for value in case_scores.values()),
        excluded_window_count=sum(value[3] for value in case_scores.values()),
        fractions_brier_score=numerator,
        fractions_reference_mse=reference,
        fractions_skill_score=_bootstrap_ratio_skill(
            "fractions_skill_score",
            event_numerator,
            event_reference,
            bootstrap,
            expected_point=fss_value,
        ),
    )


def verify_scalar_ensemble(
    cases: Iterable[ScalarEnsembleVerificationCase],
    *,
    bootstrap: BootstrapConfig,
) -> ScalarEnsembleVerification:
    """Verify complete scalar ensemble members against independent point truth.

    CRPS uses the exact empirical-ensemble identity. Rank ties are not broken
    randomly: one case contributes equal fractional mass to every rank
    compatible with the tie. Scalar scores and rank masses are averaged first
    within each meteorological event and then equally across events.
    """

    if not isinstance(bootstrap, BootstrapConfig):
        raise TypeError("bootstrap must be BootstrapConfig")
    ordered = tuple(sorted(cases, key=lambda item: item.case_id))
    if not ordered or any(
        not isinstance(item, ScalarEnsembleVerificationCase) for item in ordered
    ):
        raise ValueError("verification requires typed scalar ensemble cases")
    if len({item.case_id for item in ordered}) != len(ordered):
        raise ValueError("scalar ensemble case_id must be unique")
    _minimum_events(
        {item.event_id for item in ordered},
        bootstrap,
        "scalar ensemble",
    )
    first = ordered[0]
    signature = (
        first.evaluation_stratum_id,
        first.partition,
        first.quantity_id,
        first.unit_id,
        len(first.member_values),
        first.member_generation_algorithm_version,
        first.calibration_version,
        first.label_source_id,
    )
    for case in ordered[1:]:
        current = (
            case.evaluation_stratum_id,
            case.partition,
            case.quantity_id,
            case.unit_id,
            len(case.member_values),
            case.member_generation_algorithm_version,
            case.calibration_version,
            case.label_source_id,
        )
        if current != signature:
            raise ValueError(
                "scalar ensemble cases cross evaluation strata, units, "
                "member counts, algorithms, calibration, or truth sources"
            )

    mean_error_by_event = _event_means(
        ordered,
        lambda case: _mean(case.member_values) - case.observed_value,
    )
    mean_absolute_by_event = _event_means(
        ordered,
        lambda case: abs(_mean(case.member_values) - case.observed_value),
    )
    crps_by_event = _event_means(
        ordered,
        lambda case: _empirical_ensemble_crps(
            case.member_values,
            case.observed_value,
        ),
    )

    member_count = len(first.member_values)
    raw_rank_mass = [0.0] * (member_count + 1)
    event_rank_cases: dict[str, list[tuple[float, ...]]] = {}
    for case in ordered:
        contribution = _rank_contribution(
            case.member_values,
            case.observed_value,
        )
        event_rank_cases.setdefault(case.event_id, []).append(contribution)
        for rank_index, value in enumerate(contribution):
            raw_rank_mass[rank_index] += value
    event_rank_mass: dict[str, tuple[float, ...]] = {
        event_id: tuple(
            _mean(tuple(case_mass[rank] for case_mass in case_masses))
            for rank in range(member_count + 1)
        )
        for event_id, case_masses in sorted(event_rank_cases.items())
    }
    rank_histogram = tuple(
        RankHistogramBin(
            rank_index=rank,
            raw_fractional_count=raw_rank_mass[rank],
            event_weighted_mass=_mean(
                tuple(mass[rank] for mass in event_rank_mass.values())
            ),
        )
        for rank in range(member_count + 1)
    )
    if not math.isclose(
        sum(item.event_weighted_mass for item in rank_histogram),
        1.0,
        rel_tol=0.0,
        abs_tol=_TOLERANCE,
    ):
        raise RuntimeError("rank histogram mass does not sum to one")

    return ScalarEnsembleVerification(
        evaluation_stratum_id=first.evaluation_stratum_id,
        partition=first.partition,
        quantity_id=first.quantity_id,
        unit_id=first.unit_id,
        member_count=member_count,
        case_count=len(ordered),
        event_count=len(event_rank_mass),
        ensemble_mean_mae=_bootstrap_scalar(
            "scalar_ensemble_mean_mae",
            mean_absolute_by_event,
            bootstrap,
        ),
        ensemble_mean_bias=_bootstrap_scalar(
            "scalar_ensemble_mean_bias",
            mean_error_by_event,
            bootstrap,
        ),
        crps=_bootstrap_scalar(
            "scalar_ensemble_crps",
            crps_by_event,
            bootstrap,
        ),
        rank_histogram=rank_histogram,
    )


def _passage_cases(
    cases: Iterable[PassageVerificationCase],
    bootstrap: BootstrapConfig,
    *,
    require_minimum: bool = True,
) -> tuple[PassageVerificationCase, ...]:
    if not isinstance(bootstrap, BootstrapConfig):
        raise TypeError("bootstrap must be BootstrapConfig")
    ordered = tuple(sorted(cases, key=lambda item: item.case_id))
    if not ordered or any(
        not isinstance(item, PassageVerificationCase) for item in ordered
    ):
        raise ValueError("verification requires typed passage cases")
    if len({item.case_id for item in ordered}) != len(ordered):
        raise ValueError("passage case_id must be unique")
    first = ordered[0]
    signature = (
        first.evaluation_stratum_id,
        first.partition,
        first.forecast.algorithm_version,
        first.forecast.calibration_version,
        first.forecast.lead_time_seconds,
        first.forecast.horizon_seconds,
        first.forecast.probability_evidence.basis,
        first.forecast.probability_evidence.sample_size,
        first.forecast.probability_evidence.source_algorithm_version,
        first.baseline.algorithm_version,
    )
    for case in ordered[1:]:
        current = (
            case.evaluation_stratum_id,
            case.partition,
            case.forecast.algorithm_version,
            case.forecast.calibration_version,
            case.forecast.lead_time_seconds,
            case.forecast.horizon_seconds,
            case.forecast.probability_evidence.basis,
            case.forecast.probability_evidence.sample_size,
            case.forecast.probability_evidence.source_algorithm_version,
            case.baseline.algorithm_version,
        )
        if current != signature:
            raise ValueError("passage verification cases cross evaluation strata")
    if require_minimum:
        _minimum_events({item.event_id for item in ordered}, bootstrap, "passage")
    return ordered


def _reliability(
    cases: tuple[PassageVerificationCase, ...],
    edges: tuple[float, ...],
) -> tuple[ReliabilityBin, ...]:
    output: list[ReliabilityBin] = []
    for index, (lower, upper) in enumerate(zip(edges, edges[1:])):
        includes_upper = index == len(edges) - 2
        selected = tuple(
            case
            for case in cases
            if _in_bin(
                float(case.forecast.passage_probability),
                lower,
                upper,
                includes_upper,
            )
        )
        if not selected:
            output.append(
                ReliabilityBin(
                    lower,
                    upper,
                    includes_upper,
                    0,
                    0,
                    None,
                    None,
                )
            )
            continue
        forecast_by_event = _event_means(
            selected,
            lambda case: float(case.forecast.passage_probability),
        )
        outcome_by_event = _event_means(
            selected,
            lambda case: float(case.passage_occurred),
        )
        output.append(
            ReliabilityBin(
                lower_probability=lower,
                upper_probability=upper,
                includes_upper=includes_upper,
                sample_count=len(selected),
                event_count=len(forecast_by_event),
                forecast_mean=_mean(tuple(forecast_by_event.values())),
                outcome_frequency=_mean(tuple(outcome_by_event.values())),
            )
        )
    return tuple(output)


def _sharpness(
    cases: tuple[PassageVerificationCase, ...],
    edges: tuple[float, ...],
) -> SharpnessSummary:
    by_event: dict[str, list[float]] = {}
    for case in cases:
        by_event.setdefault(case.event_id, []).append(
            float(case.forecast.passage_probability)
        )
    bins: list[SharpnessBin] = []
    for index, (lower, upper) in enumerate(zip(edges, edges[1:])):
        includes_upper = index == len(edges) - 2
        sample_count = sum(
            _in_bin(value, lower, upper, includes_upper)
            for values in by_event.values()
            for value in values
        )
        events_present = sum(
            any(_in_bin(value, lower, upper, includes_upper) for value in values)
            for values in by_event.values()
        )
        mass = _mean(
            tuple(
                sum(
                    _in_bin(value, lower, upper, includes_upper)
                    for value in values
                )
                / len(values)
                for values in by_event.values()
            )
        )
        bins.append(
            SharpnessBin(
                lower,
                upper,
                includes_upper,
                sample_count,
                events_present,
                mass,
            )
        )
    mean_probability = _mean(
        tuple(_mean(tuple(values)) for values in by_event.values())
    )
    second_moment = _mean(
        tuple(
            _mean(tuple(value * value for value in values))
            for values in by_event.values()
        )
    )
    return SharpnessSummary(
        bins=tuple(bins),
        event_weighted_mean_probability=mean_probability,
        event_weighted_variance=max(
            0.0,
            second_moment - mean_probability**2,
        ),
    )


def _classification(
    cases: tuple[PassageVerificationCase, ...],
    threshold: float,
) -> PassageClassificationSummary:
    tp = tn = false = missed = 0
    for case in cases:
        predicted = float(case.forecast.passage_probability) >= threshold
        if predicted and case.passage_occurred:
            tp += 1
        elif predicted:
            false += 1
        elif case.passage_occurred:
            missed += 1
        else:
            tn += 1
    observed_passage = tp + missed
    observed_no_passage = tn + false
    if observed_passage == 0 or observed_no_passage == 0:
        raise ValueError(
            "false/missed passage rates require both outcome classes"
        )
    by_event: dict[str, list[tuple[bool, bool]]] = {}
    for case in cases:
        predicted = float(case.forecast.passage_probability) >= threshold
        by_event.setdefault(case.event_id, []).append(
            (predicted, case.passage_occurred)
        )
    false_event_rates = tuple(
        sum(predicted and not occurred for predicted, occurred in values)
        / sum(not occurred for _predicted, occurred in values)
        for values in by_event.values()
        if any(not occurred for _predicted, occurred in values)
    )
    missed_event_rates = tuple(
        sum(not predicted and occurred for predicted, occurred in values)
        / sum(occurred for _predicted, occurred in values)
        for values in by_event.values()
        if any(occurred for _predicted, occurred in values)
    )
    return PassageClassificationSummary(
        decision_threshold=threshold,
        true_positive_count=tp,
        true_negative_count=tn,
        false_passage_count=false,
        missed_passage_count=missed,
        observed_passage_count=observed_passage,
        observed_no_passage_count=observed_no_passage,
        false_passage_rate=_mean(false_event_rates),
        missed_passage_rate=_mean(missed_event_rates),
    )


def _roc(
    cases: tuple[PassageVerificationCase, ...],
) -> PassageRocVerification:
    observed_passage = sum(case.passage_occurred for case in cases)
    observed_no_passage = len(cases) - observed_passage
    if observed_passage == 0 or observed_no_passage == 0:
        raise ValueError("ROC requires both outcome classes")
    passage_events = {
        case.event_id for case in cases if case.passage_occurred
    }
    no_passage_events = {
        case.event_id for case in cases if not case.passage_occurred
    }
    points = [
        RocPoint(
            threshold=None,
            true_positive_count=0,
            false_positive_count=0,
            true_positive_rate=0.0,
            false_positive_rate=0.0,
        )
    ]
    thresholds = tuple(
        sorted(
            {
                float(case.forecast.passage_probability)
                for case in cases
            },
            reverse=True,
        )
    )
    for threshold in thresholds:
        tp = sum(
            float(case.forecast.passage_probability) >= threshold
            and case.passage_occurred
            for case in cases
        )
        fp = sum(
            float(case.forecast.passage_probability) >= threshold
            and not case.passage_occurred
            for case in cases
        )
        grouped: dict[str, list[PassageVerificationCase]] = {}
        for case in cases:
            grouped.setdefault(case.event_id, []).append(case)
        tpr = _mean(
            tuple(
                sum(
                    float(case.forecast.passage_probability) >= threshold
                    and case.passage_occurred
                    for case in event_cases
                )
                / sum(case.passage_occurred for case in event_cases)
                for event_cases in grouped.values()
                if any(case.passage_occurred for case in event_cases)
            )
        )
        fpr = _mean(
            tuple(
                sum(
                    float(case.forecast.passage_probability) >= threshold
                    and not case.passage_occurred
                    for case in event_cases
                )
                / sum(not case.passage_occurred for case in event_cases)
                for event_cases in grouped.values()
                if any(not case.passage_occurred for case in event_cases)
            )
        )
        points.append(
            RocPoint(
                threshold=threshold,
                true_positive_count=tp,
                false_positive_count=fp,
                true_positive_rate=tpr,
                false_positive_rate=fpr,
            )
        )
    auc = sum(
        (right.false_positive_rate - left.false_positive_rate)
        * (right.true_positive_rate + left.true_positive_rate)
        / 2.0
        for left, right in zip(points, points[1:])
    )
    if not -_TOLERANCE <= auc <= 1.0 + _TOLERANCE:
        raise RuntimeError("empirical ROC AUC lies outside [0, 1]")
    return PassageRocVerification(
        points=tuple(points),
        area_under_curve=min(1.0, max(0.0, auc)),
        observed_passage_count=observed_passage,
        observed_no_passage_count=observed_no_passage,
        passage_event_count=len(passage_events),
        no_passage_event_count=len(no_passage_events),
    )


def _empirical_ensemble_crps(
    member_values: tuple[float, ...],
    observed_value: float,
) -> float:
    ordered = tuple(sorted(member_values))
    member_count = len(ordered)
    mean_absolute_error = _mean(
        tuple(abs(value - observed_value) for value in ordered)
    )
    pairwise_half_sum = sum(
        (2 * index - member_count + 1) * value
        for index, value in enumerate(ordered)
    )
    score = mean_absolute_error - pairwise_half_sum / (member_count**2)
    if score < -_TOLERANCE:
        raise RuntimeError("empirical CRPS cannot be negative")
    return max(0.0, score)


def _rank_contribution(
    member_values: tuple[float, ...],
    observed_value: float,
) -> tuple[float, ...]:
    less = sum(value < observed_value for value in member_values)
    equal = sum(value == observed_value for value in member_values)
    contribution = [0.0] * (len(member_values) + 1)
    tie_mass = 1.0 / (equal + 1)
    for rank in range(less, less + equal + 1):
        contribution[rank] = tie_mass
    return tuple(contribution)


def _homogeneous_field_stratum(
    cases: tuple[FieldVerificationCase, ...],
) -> None:
    first = cases[0]
    signature = (
        first.evaluation_stratum_id,
        first.partition,
        first.candidate.variable_id,
        first.candidate.unit_id,
        first.candidate.algorithm_version,
        first.truth.algorithm_version,
        first.label_source_id,
    )
    grid = first.candidate.grid
    for case in cases[1:]:
        current = (
            case.evaluation_stratum_id,
            case.partition,
            case.candidate.variable_id,
            case.candidate.unit_id,
            case.candidate.algorithm_version,
            case.truth.algorithm_version,
            case.label_source_id,
        )
        if current != signature:
            raise ValueError("FSS cases cross evaluation strata or units")
        grid.assert_same_geometry(case.candidate.grid)


def _fraction_exceeding(
    field: MaskedVerificationField,
    pixels: list[tuple[int, int]],
    config: FssConfig,
) -> float:
    if config.threshold_operator is not ThresholdOperator.GREATER_OR_EQUAL:
        raise ValueError("unsupported threshold operator")
    return sum(
        float(field.values[row][column]) >= config.threshold
        for row, column in pixels
    ) / len(pixels)


def _event_means(
    cases: Iterable,
    value: Callable[[object], float],
) -> dict[str, float]:
    grouped: dict[str, list[float]] = {}
    for case in cases:
        measured = value(case)
        _finite(measured, "event metric value")
        grouped.setdefault(case.event_id, []).append(float(measured))
    return {
        event_id: _mean(tuple(values))
        for event_id, values in sorted(grouped.items())
    }


def _bootstrap_scalar(
    metric_id: str,
    event_values: dict[str, float],
    config: BootstrapConfig,
) -> BootstrapEstimate:
    event_ids = tuple(sorted(event_values))
    _minimum_events(set(event_ids), config, metric_id)
    point = _mean(tuple(event_values[event_id] for event_id in event_ids))
    replicates = tuple(
        _mean(
            tuple(
                event_values[event_ids[index]]
                for index in _bootstrap_indices(
                    len(event_ids),
                    config,
                    replicate,
                )
            )
        )
        for replicate in range(config.replicates)
    )
    return _estimate(metric_id, point, replicates, len(event_ids), config)


def _bootstrap_ratio_skill(
    metric_id: str,
    numerator_by_event: dict[str, float],
    denominator_by_event: dict[str, float],
    config: BootstrapConfig,
    *,
    expected_point: float,
) -> BootstrapEstimate:
    event_ids = tuple(sorted(numerator_by_event))
    if event_ids != tuple(sorted(denominator_by_event)):
        raise ValueError("paired bootstrap event IDs differ")
    _minimum_events(set(event_ids), config, metric_id)
    replicates: list[float] = []
    for replicate in range(config.replicates):
        indices = _bootstrap_indices(len(event_ids), config, replicate)
        numerator = _mean(
            tuple(numerator_by_event[event_ids[index]] for index in indices)
        )
        denominator = _mean(
            tuple(denominator_by_event[event_ids[index]] for index in indices)
        )
        if denominator == 0.0:
            raise ValueError(
                f"{metric_id} bootstrap baseline denominator is zero"
            )
        replicates.append(1.0 - numerator / denominator)
    return _estimate(
        metric_id,
        expected_point,
        tuple(replicates),
        len(event_ids),
        config,
    )


def _bootstrap_indices(
    event_count: int,
    config: BootstrapConfig,
    replicate: int,
) -> tuple[int, ...]:
    return tuple(
        int.from_bytes(
            hashlib.sha256(
                (
                    BOOTSTRAP_ALGORITHM_VERSION
                    + "\x00"
                    + config.seed
                    + "\x00"
                    + str(replicate)
                    + "\x00"
                    + str(draw)
                ).encode("utf-8")
            ).digest()[:8],
            "big",
        )
        % event_count
        for draw in range(event_count)
    )


def _estimate(
    metric_id: str,
    point: float,
    replicates: tuple[float, ...],
    event_count: int,
    config: BootstrapConfig,
) -> BootstrapEstimate:
    return BootstrapEstimate(
        metric_id=metric_id,
        point_estimate=point,
        lower=_sample_quantile(replicates, config.lower_probability),
        upper=_sample_quantile(replicates, config.upper_probability),
        event_count=event_count,
        replicates=config.replicates,
        lower_probability=config.lower_probability,
        upper_probability=config.upper_probability,
        seed_sha256=config.seed_sha256,
    )


def _sample_quantile(values: tuple[float, ...], probability: float) -> float:
    ordered = tuple(sorted(values))
    if not ordered:
        raise ValueError("cannot take quantile of empty bootstrap")
    position = (len(ordered) - 1) * probability
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] * (1.0 - fraction) + ordered[upper] * fraction


def _pinball_loss(probability: float, forecast: float, observed: float) -> float:
    error = observed - forecast
    return probability * error if error >= 0.0 else (probability - 1.0) * error


def _minimum_events(
    event_ids: set[str],
    config: BootstrapConfig,
    metric: str,
) -> None:
    if len(event_ids) < config.minimum_events:
        raise ValueError(
            f"{metric} has {len(event_ids)} events; "
            f"caller requires {config.minimum_events}"
        )


def _probability_edges(edges: tuple[float, ...]) -> None:
    if not isinstance(edges, tuple) or len(edges) < 2:
        raise ValueError("reliability_bin_edges requires immutable bin boundaries")
    for value in edges:
        _finite(value, "reliability bin edge")
    if edges[0] != 0.0 or edges[-1] != 1.0:
        raise ValueError("reliability bins must cover exactly [0, 1]")
    if any(current <= previous for previous, current in zip(edges, edges[1:])):
        raise ValueError("reliability bin edges must be strictly increasing")


def _in_bin(
    value: float,
    lower: float,
    upper: float,
    includes_upper: bool,
) -> bool:
    return lower <= value <= upper if includes_upper else lower <= value < upper


def _validate_truth_context_surface(context: object) -> None:
    if (
        context is None
        or not hasattr(context, "split")
        or not hasattr(context, "labels")
        or not callable(getattr(context.split, "partition_for_event", None))
        or not callable(getattr(context.split, "partition_for_frame", None))
        or not callable(getattr(context.labels, "label", None))
        or not callable(getattr(context.labels, "source", None))
    ):
        raise TypeError("truth_context is not an ExternalTruthContext-compatible object")


def _probability_pair(passage: float, no_passage: float, label: str) -> None:
    _finite(passage, f"{label} passage probability")
    _finite(no_passage, f"{label} no-passage probability")
    if not 0.0 <= passage <= 1.0 or not 0.0 <= no_passage <= 1.0:
        raise ValueError(f"{label} probabilities must lie in [0, 1]")
    if not math.isclose(
        passage + no_passage,
        1.0,
        rel_tol=0.0,
        abs_tol=_TOLERANCE,
    ):
        raise ValueError(f"{label} passage probabilities must sum to one")


def _matrix(value: object, grid: AnalysisGrid, label: str) -> None:
    if not isinstance(value, tuple) or any(
        not isinstance(row, tuple) for row in value
    ):
        raise TypeError(f"{label} must be immutable tuples")
    grid.validate_raster_shape(value)


def _unique_ids(values: tuple[str, ...], label: str) -> None:
    if (
        not isinstance(values, tuple)
        or not values
        or any(not isinstance(value, str) or not value for value in values)
        or len(values) != len(set(values))
    ):
        raise ValueError(f"{label} must contain unique IDs")


def _mean(values: tuple[float, ...]) -> float:
    if not values:
        raise ValueError("cannot average empty values")
    return math.fsum(values) / len(values)


def _identifier(value: object, label: str) -> None:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{label} must be a non-empty string")


def _positive_int(value: object, label: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{label} must be a positive integer")


def _positive_finite(value: object, label: str) -> None:
    _finite(value, label)
    if float(value) <= 0.0:
        raise ValueError(f"{label} must be positive")


def _finite(value: object, label: str) -> None:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
    ):
        raise ValueError(f"{label} must be finite")


def _sha256(value: object, label: str) -> None:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError(f"{label} must be lowercase SHA-256")
