"""Immutable, fail-closed tracker forecast contract for vNext shadow work.

This module contains no forecast algorithm and no calibration routine.  It
accepts probabilities only with explicit ensemble-frequency or versioned
empirical-calibration evidence.  Quality scores, confidence labels and other
heuristics cannot be inserted as probabilities through this contract.
"""
from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from typing import Any


TRACKER_FORECAST_CONTRACT_VERSION = "stv3-tracker-forecast-contract-v1"
TRACKER_STATUS_SEPARATION_VERSION = "stv3-tracker-status-separation-shadow-v3"
CURRENT_FOOTPRINT_DISTANCE_TOLERANCE_KM = 1e-6
CURRENT_FOOTPRINT_FRESHNESS_LIMIT_SECONDS = 15 * 60.0
CURRENT_FOOTPRINT_FRESHNESS_POLICY_ID = (
    "stv3-current-radar-footprint-freshness-v1"
)
GROUND_FRESHNESS_POLICY_ID = "stv3-ground-current-freshness-v1"
GROUND_FRESHNESS_LIMITS_SECONDS = (
    ("metar", 35 * 60.0),
    ("netatmo", 10 * 60.0),
    ("synoptic", 20 * 60.0),
)
MAX_CURRENT_GROUND_EVIDENCE_RECORDS = 32
MAX_LEGACY_CANDIDATE_RECORDS = 128
MAX_DIAGNOSTIC_REASON_CODES = 32
MAX_IDENTIFIER_LENGTH = 512
_NUMERIC_TOLERANCE = 1e-12
_TIME_TOLERANCE_SECONDS = 1e-6


class ForecastState(str, Enum):
    """Whether all required meteorological forecast facts are available."""

    DETERMINATE = "determinate"
    INDETERMINATE = "indeterminate"


class ProbabilityEvidenceBasis(str, Enum):
    """The only accepted origins for a passage probability."""

    ENSEMBLE_FREQUENCY = "ensemble_frequency"
    EMPIRICAL_CALIBRATION = "empirical_calibration"


class DistributionEvidenceBasis(str, Enum):
    """Auditable origins for ETA and local-intensity distributions."""

    ENSEMBLE_MEMBERS = "ensemble_members"
    EMPIRICAL_SAMPLES = "empirical_samples"


class ConditionalDistributionKind(str, Enum):
    CONDITIONAL_ETA = "conditional_eta"
    CONDITIONAL_LOCAL_INTENSITY = "conditional_local_intensity"


class PrecipitationPresence(str, Enum):
    """Evidence state for precipitation at or near one tracker."""

    PRESENT = "present"
    ABSENT = "absent"
    INDETERMINATE = "indeterminate"


class GroundEvidenceKind(str, Enum):
    """Typed positive observations that can establish local precipitation."""

    PRECIPITATION_PRESENCE = "precipitation_presence"
    QUANTITATIVE_RATE = "quantitative_rate"


class SourceRecordIdKind(str, Enum):
    """Whether a ground record ID is native or explicitly derived."""

    PROVIDER_RECORD = "provider_record"
    DERIVED_STATION_TIMESTAMP = "derived_station_timestamp"


class NextThreatState(str, Enum):
    """Whether a separate future local threat was detected."""

    DETECTED = "detected"
    NOT_DETECTED = "not_detected"
    INDETERMINATE = "indeterminate"


class PassageRelation(str, Enum):
    """Non-probabilistic geometric relation from the legacy motion layer."""

    CURRENT = "current"
    HIT = "hit"
    EDGE = "edge"
    MISS = "miss"
    INDETERMINATE = "indeterminate"


class TrackerDisplayState(str, Enum):
    """Additive shadow presentation; never replaces the legacy HA state."""

    CURRENT_PRECIPITATION = "current_precipitation"
    NEXT_THREAT = "next_threat"
    NEARBY_PRECIPITATION = "nearby_precipitation"
    NO_CURRENT_OR_NEARBY_PRECIPITATION = (
        "no_current_or_nearby_precipitation"
    )
    INDETERMINATE = "indeterminate"


@dataclass(frozen=True, slots=True)
class CurrentFootprintEvidence:
    """Exact current-frame scan of every evaluated storm footprint.

    This is deliberately richer than a rounded dashboard distance.  The
    containing set is derived from the same current-frame polygons that were
    evaluated for the tracker.  A motion classification such as ``mist`` is
    therefore unable to erase a positive current-footprint fact.
    """

    evidence_id: str
    tracker_id: str
    tracker_latitude: float
    tracker_longitude: float
    frame_valid_at: float
    evaluated_at: float
    frame_age_seconds: float
    freshness_limit_seconds: float
    freshness_policy_id: str
    fresh: bool
    source_id: str
    source_algorithm_version: str
    input_storm_count: int
    scan_complete: bool
    scan_sha256: str
    evaluated_storm_ids: tuple[str, ...]
    containing_storm_ids: tuple[str, ...]
    nearest_storm_id: str | None
    minimum_distance_km: float | None

    def __post_init__(self) -> None:
        for label, value in (
            ("evidence_id", self.evidence_id),
            ("tracker_id", self.tracker_id),
            ("freshness_policy_id", self.freshness_policy_id),
            ("source_id", self.source_id),
            ("source_algorithm_version", self.source_algorithm_version),
        ):
            _require_nonempty_string(value, label)
        _require_lat_lon(self.tracker_latitude, self.tracker_longitude)
        for label, value in (
            ("frame_valid_at", self.frame_valid_at),
            ("evaluated_at", self.evaluated_at),
            ("frame_age_seconds", self.frame_age_seconds),
            ("freshness_limit_seconds", self.freshness_limit_seconds),
        ):
            _require_nonnegative_finite(value, label)
        derived_age = float(self.evaluated_at) - float(self.frame_valid_at)
        if derived_age < -_TIME_TOLERANCE_SECONDS:
            raise ValueError("radar footprint frame cannot be from the future")
        if not math.isclose(
            max(0.0, derived_age),
            float(self.frame_age_seconds),
            rel_tol=0.0,
            abs_tol=0.11,
        ):
            raise ValueError(
                "frame_age_seconds must match evaluated_at-frame_valid_at"
            )
        if self.freshness_policy_id != (
            CURRENT_FOOTPRINT_FRESHNESS_POLICY_ID
        ):
            raise ValueError("unknown current-footprint freshness policy")
        if not math.isclose(
            float(self.freshness_limit_seconds),
            CURRENT_FOOTPRINT_FRESHNESS_LIMIT_SECONDS,
            rel_tol=0.0,
            abs_tol=_NUMERIC_TOLERANCE,
        ):
            raise ValueError("current-footprint freshness limit mismatch")
        expected_fresh = (
            derived_age >= -_TIME_TOLERANCE_SECONDS
            and derived_age <= float(self.freshness_limit_seconds)
        )
        if self.fresh is not expected_fresh:
            raise ValueError("current-footprint fresh flag violates time policy")
        _require_nonnegative_int(self.input_storm_count, "input_storm_count")
        if not isinstance(self.scan_complete, bool):
            raise TypeError("scan_complete must be bool")
        _require_sha256(self.scan_sha256, "scan_sha256")
        _validate_id_tuple(
            self.evaluated_storm_ids,
            "evaluated_storm_ids",
            allow_empty=True,
            canonical=True,
        )
        _validate_id_tuple(
            self.containing_storm_ids,
            "containing_storm_ids",
            allow_empty=True,
            canonical=True,
        )
        evaluated = set(self.evaluated_storm_ids)
        containing = set(self.containing_storm_ids)
        if not containing.issubset(evaluated):
            raise ValueError(
                "containing_storm_ids must be a subset of evaluated_storm_ids"
            )
        if len(evaluated) > self.input_storm_count:
            raise ValueError("evaluated storms exceed input_storm_count")
        if self.scan_complete != (
            len(evaluated) == self.input_storm_count
        ):
            raise ValueError(
                "scan_complete must exactly reflect evaluated input storms"
            )
        if not evaluated:
            if (
                self.nearest_storm_id is not None
                or self.minimum_distance_km is not None
                or containing
            ):
                raise ValueError(
                    "empty footprint scan cannot expose nearest/containing data"
                )
            return
        _require_nonempty_string(
            self.nearest_storm_id,
            "nearest_storm_id",
        )
        _require_nonnegative_finite(
            self.minimum_distance_km,
            "minimum_distance_km",
        )
        if self.nearest_storm_id not in evaluated:
            raise ValueError("nearest_storm_id was not evaluated")
        inside = (
            float(self.minimum_distance_km)
            <= CURRENT_FOOTPRINT_DISTANCE_TOLERANCE_KM
        )
        if inside != bool(containing):
            raise ValueError(
                "containing_storm_ids must exactly match zero-distance evidence"
            )
        if inside and self.nearest_storm_id not in containing:
            raise ValueError(
                "zero-distance nearest_storm_id must contain the tracker"
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "evidence_id": self.evidence_id,
            "tracker_id": self.tracker_id,
            "tracker_latitude": self.tracker_latitude,
            "tracker_longitude": self.tracker_longitude,
            "frame_valid_at": self.frame_valid_at,
            "evaluated_at": self.evaluated_at,
            "frame_age_seconds": self.frame_age_seconds,
            "freshness_limit_seconds": self.freshness_limit_seconds,
            "freshness_policy_id": self.freshness_policy_id,
            "fresh": self.fresh,
            "source_id": self.source_id,
            "source_algorithm_version": self.source_algorithm_version,
            "input_storm_count": self.input_storm_count,
            "scan_complete": self.scan_complete,
            "scan_sha256": self.scan_sha256,
            "evaluated_storm_ids": list(self.evaluated_storm_ids),
            "containing_storm_ids": list(self.containing_storm_ids),
            "nearest_storm_id": self.nearest_storm_id,
            "minimum_distance_km": self.minimum_distance_km,
        }


@dataclass(frozen=True, slots=True)
class CurrentGroundEvidence:
    """One fresh, positive and spatially explicit local ground observation.

    This positive-only type deliberately cannot represent a generic confidence
    flag, a dry observation or a stale station.  ``distance_to_tracker_km``
    remains visible because a local gauge observation is not the same claim as
    exact radar-footprint containment.  Qualitative human or aviation reports
    use ``PRECIPITATION_PRESENCE``; an actual rain-rate value uses
    ``QUANTITATIVE_RATE``.
    """

    evidence_id: str
    kind: GroundEvidenceKind
    tracker_id: str
    tracker_latitude: float
    tracker_longitude: float
    provider_id: str
    station_id: str
    physical_site_id: str
    physical_site_basis: str
    source_record_id: str
    source_record_id_kind: SourceRecordIdKind
    source_algorithm_version: str
    observed_at: float
    evaluated_at: float
    observation_age_seconds: float
    freshness_limit_seconds: float
    freshness_policy_id: str
    distance_to_tracker_km: float
    precipitation_present: bool
    fresh: bool
    measurement_value: float | None = None
    measurement_unit: str | None = None
    matched_storm_id: str | None = None

    def __post_init__(self) -> None:
        for label, value in (
            ("evidence_id", self.evidence_id),
            ("tracker_id", self.tracker_id),
            ("provider_id", self.provider_id),
            ("station_id", self.station_id),
            ("physical_site_id", self.physical_site_id),
            ("physical_site_basis", self.physical_site_basis),
            ("source_record_id", self.source_record_id),
            ("source_algorithm_version", self.source_algorithm_version),
            ("freshness_policy_id", self.freshness_policy_id),
        ):
            _require_nonempty_string(value, label)
        if not isinstance(self.kind, GroundEvidenceKind):
            raise TypeError("kind must be GroundEvidenceKind")
        if not isinstance(self.source_record_id_kind, SourceRecordIdKind):
            raise TypeError("source_record_id_kind must be SourceRecordIdKind")
        _require_lat_lon(self.tracker_latitude, self.tracker_longitude)
        for label, value in (
            ("observed_at", self.observed_at),
            ("evaluated_at", self.evaluated_at),
            ("observation_age_seconds", self.observation_age_seconds),
            ("freshness_limit_seconds", self.freshness_limit_seconds),
            ("distance_to_tracker_km", self.distance_to_tracker_km),
        ):
            _require_nonnegative_finite(value, label)
        if float(self.observed_at) <= 0.0:
            raise ValueError("observed_at must be an absolute Unix timestamp")
        if float(self.observed_at) > (
            float(self.evaluated_at) + _TIME_TOLERANCE_SECONDS
        ):
            raise ValueError("ground observation cannot be from the future")
        derived_age = float(self.evaluated_at) - float(self.observed_at)
        if not math.isclose(
            derived_age,
            float(self.observation_age_seconds),
            rel_tol=0.0,
            abs_tol=0.11,
        ):
            raise ValueError(
                "observation_age_seconds must match evaluated_at-observed_at"
            )
        if self.precipitation_present is not True:
            raise ValueError(
                "CurrentGroundEvidence accepts positive precipitation only"
            )
        if self.fresh is not True:
            raise ValueError("CurrentGroundEvidence must be fresh")
        if self.freshness_policy_id != GROUND_FRESHNESS_POLICY_ID:
            raise ValueError("unknown ground freshness policy")
        expected_limit = _ground_freshness_limit(self.provider_id)
        if expected_limit is None:
            raise ValueError("ground provider has no freshness policy")
        if not math.isclose(
            float(self.freshness_limit_seconds),
            expected_limit,
            rel_tol=0.0,
            abs_tol=_NUMERIC_TOLERANCE,
        ):
            raise ValueError("ground freshness limit does not match provider")
        if derived_age > expected_limit + _TIME_TOLERANCE_SECONDS:
            raise ValueError("CurrentGroundEvidence is stale for its provider")
        if self.matched_storm_id is not None:
            _require_nonempty_string(
                self.matched_storm_id,
                "matched_storm_id",
            )
        if self.kind is GroundEvidenceKind.QUANTITATIVE_RATE:
            _require_nonnegative_finite(
                self.measurement_value,
                "measurement_value",
            )
            if float(self.measurement_value) <= 0.0:
                raise ValueError(
                    "quantitative precipitation rate must be positive"
                )
            _require_nonempty_string(
                self.measurement_unit,
                "measurement_unit",
            )
        elif (
            self.measurement_value is not None
            or self.measurement_unit is not None
        ):
            raise ValueError(
                "presence observation cannot expose a quantitative value"
            )

    def to_dict(self) -> dict[str, Any]:
        """Return explicit provenance without implying radar containment."""

        return {
            "evidence_id": self.evidence_id,
            "kind": self.kind.value,
            "tracker_id": self.tracker_id,
            "tracker_latitude": self.tracker_latitude,
            "tracker_longitude": self.tracker_longitude,
            "provider_id": self.provider_id,
            "station_id": self.station_id,
            "physical_site_id": self.physical_site_id,
            "physical_site_basis": self.physical_site_basis,
            "source_record_id": self.source_record_id,
            "source_record_id_kind": self.source_record_id_kind.value,
            "source_algorithm_version": self.source_algorithm_version,
            "observed_at": self.observed_at,
            "evaluated_at": self.evaluated_at,
            "observation_age_seconds": self.observation_age_seconds,
            "freshness_limit_seconds": self.freshness_limit_seconds,
            "freshness_policy_id": self.freshness_policy_id,
            "distance_to_tracker_km": self.distance_to_tracker_km,
            "precipitation_present": self.precipitation_present,
            "fresh": self.fresh,
            "measurement_value": self.measurement_value,
            "measurement_unit": self.measurement_unit,
            "matched_storm_id": self.matched_storm_id,
        }


@dataclass(frozen=True, slots=True)
class BoundedCollectionDiagnostic:
    """Bounded serialization metadata for a potentially larger input set."""

    input_total_count: int
    inspected_count: int
    accepted_total_count: int
    serialized_count: int
    malformed_count: int
    overflow_count: int
    complete: bool
    content_sha256: str
    reason_codes: tuple[str, ...]

    def __post_init__(self) -> None:
        for label, value in (
            ("input_total_count", self.input_total_count),
            ("inspected_count", self.inspected_count),
            ("accepted_total_count", self.accepted_total_count),
            ("serialized_count", self.serialized_count),
            ("malformed_count", self.malformed_count),
            ("overflow_count", self.overflow_count),
        ):
            _require_nonnegative_int(value, label)
        if self.inspected_count > self.input_total_count:
            raise ValueError("inspected_count exceeds input_total_count")
        if self.accepted_total_count > self.inspected_count:
            raise ValueError("accepted_total_count exceeds inspected_count")
        if self.serialized_count > self.accepted_total_count:
            raise ValueError("serialized_count exceeds accepted_total_count")
        if self.malformed_count > self.inspected_count:
            raise ValueError("malformed_count exceeds inspected_count")
        if (
            self.accepted_total_count + self.malformed_count
            > self.inspected_count
        ):
            raise ValueError(
                "accepted and malformed counts exceed inspected_count"
            )
        if self.overflow_count != (
            self.accepted_total_count - self.serialized_count
        ):
            raise ValueError("overflow_count does not match serialized count")
        if not isinstance(self.complete, bool):
            raise TypeError("complete must be bool")
        if self.complete != (
            self.inspected_count == self.input_total_count
            and
            self.malformed_count == 0
            and self.overflow_count == 0
            and not self.reason_codes
        ):
            raise ValueError(
                "complete must reflect malformed and overflow counts"
            )
        _require_sha256(self.content_sha256, "content_sha256")
        _validate_reason_codes(self.reason_codes)
        if len(self.reason_codes) > MAX_DIAGNOSTIC_REASON_CODES:
            raise ValueError("too many collection diagnostic reason codes")

    def to_dict(self) -> dict[str, Any]:
        return {
            "input_total_count": self.input_total_count,
            "inspected_count": self.inspected_count,
            "accepted_total_count": self.accepted_total_count,
            "serialized_count": self.serialized_count,
            "malformed_count": self.malformed_count,
            "overflow_count": self.overflow_count,
            "complete": self.complete,
            "content_sha256": self.content_sha256,
            "reason_codes": list(self.reason_codes),
        }


@dataclass(frozen=True, slots=True)
class PassageProbabilityEvidence:
    """Provenance needed to interpret passage/no-passage probabilities.

    For a raw ensemble frequency, ``passage_count`` and ``sample_size`` define
    the probability exactly.  For empirical calibration, ``passage_count`` is
    absent and a real, caller-supplied ``calibration_version`` is mandatory.
    """

    evidence_id: str
    basis: ProbabilityEvidenceBasis
    sample_size: int
    source_id: str
    source_algorithm_version: str
    passage_count: int | None
    calibration_version: str | None

    def __post_init__(self) -> None:
        for label, value in (
            ("evidence_id", self.evidence_id),
            ("source_id", self.source_id),
            ("source_algorithm_version", self.source_algorithm_version),
        ):
            _require_nonempty_string(value, label)
        if not isinstance(self.basis, ProbabilityEvidenceBasis):
            raise TypeError("basis must be ProbabilityEvidenceBasis")
        _require_positive_int(self.sample_size, "sample_size")
        if self.basis is ProbabilityEvidenceBasis.ENSEMBLE_FREQUENCY:
            if (
                isinstance(self.passage_count, bool)
                or not isinstance(self.passage_count, int)
                or self.passage_count < 0
                or self.passage_count > self.sample_size
            ):
                raise ValueError(
                    "ensemble evidence requires passage_count in [0, sample_size]"
                )
            if self.calibration_version is not None:
                raise ValueError(
                    "raw ensemble frequency cannot claim a calibration version"
                )
        else:
            if self.passage_count is not None:
                raise ValueError(
                    "empirical calibration cannot masquerade as ensemble counts"
                )
            _require_nonempty_string(
                self.calibration_version,
                "calibration_version",
            )


@dataclass(frozen=True, slots=True)
class QuantilePoint:
    """One quantile of an explicitly sourced conditional distribution."""

    probability: float
    value: float

    def __post_init__(self) -> None:
        _require_finite(self.probability, "quantile probability")
        _require_finite(self.value, "quantile value")
        if self.probability < 0.0 or self.probability > 1.0:
            raise ValueError("quantile probability must be in [0, 1]")


@dataclass(frozen=True, slots=True)
class DistributionInterval:
    """An interval whose endpoint probabilities and values are explicit."""

    lower_probability: float
    upper_probability: float
    lower_value: float
    upper_value: float

    def __post_init__(self) -> None:
        for label, value in (
            ("lower_probability", self.lower_probability),
            ("upper_probability", self.upper_probability),
            ("lower_value", self.lower_value),
            ("upper_value", self.upper_value),
        ):
            _require_finite(value, label)
        if not 0.0 <= self.lower_probability < self.upper_probability <= 1.0:
            raise ValueError("interval probabilities must be strictly ordered")
        if self.lower_value > self.upper_value:
            raise ValueError("interval values must be ordered")

    @property
    def probability_mass(self) -> float:
        return self.upper_probability - self.lower_probability


@dataclass(frozen=True, slots=True)
class ConditionalDistribution:
    """Quantiles conditional on passage, never an unconditional point guess."""

    distribution_id: str
    kind: ConditionalDistributionKind
    quantity: str
    unit: str
    basis: DistributionEvidenceBasis
    sample_size: int
    source_id: str
    source_algorithm_version: str
    quantiles: tuple[QuantilePoint, ...]
    calibration_version: str | None
    conditional_on_passage: bool = True

    def __post_init__(self) -> None:
        for label, value in (
            ("distribution_id", self.distribution_id),
            ("quantity", self.quantity),
            ("unit", self.unit),
            ("source_id", self.source_id),
            ("source_algorithm_version", self.source_algorithm_version),
        ):
            _require_nonempty_string(value, label)
        if not isinstance(self.kind, ConditionalDistributionKind):
            raise TypeError("kind must be ConditionalDistributionKind")
        if not isinstance(self.basis, DistributionEvidenceBasis):
            raise TypeError("basis must be DistributionEvidenceBasis")
        _require_positive_int(self.sample_size, "sample_size")
        if self.conditional_on_passage is not True:
            raise ValueError("forecast distributions must be conditional on passage")
        if (
            not isinstance(self.quantiles, tuple)
            or len(self.quantiles) < 3
            or any(not isinstance(value, QuantilePoint) for value in self.quantiles)
        ):
            raise ValueError("distribution requires at least three immutable quantiles")
        probabilities = tuple(value.probability for value in self.quantiles)
        values = tuple(value.value for value in self.quantiles)
        if any(
            current <= previous
            for previous, current in zip(probabilities, probabilities[1:])
        ):
            raise ValueError("quantile probabilities must be strictly increasing")
        if any(
            current < previous
            for previous, current in zip(values, values[1:])
        ):
            raise ValueError("quantile values must be nondecreasing")
        if not any(
            math.isclose(
                probability,
                0.5,
                rel_tol=0.0,
                abs_tol=_NUMERIC_TOLERANCE,
            )
            for probability in probabilities
        ):
            raise ValueError("distribution must include its median (p=0.5)")
        if self.basis is DistributionEvidenceBasis.EMPIRICAL_SAMPLES:
            _require_nonempty_string(
                self.calibration_version,
                "calibration_version",
            )
        elif self.calibration_version is not None:
            _require_nonempty_string(
                self.calibration_version,
                "calibration_version",
            )
        if self.kind is ConditionalDistributionKind.CONDITIONAL_ETA:
            if self.quantity != "eta_after_reference" or self.unit != "s":
                raise ValueError("ETA distribution must use eta_after_reference in s")
            if any(value < 0.0 for value in values):
                raise ValueError("conditional ETA cannot precede reference time")

    def quantile_value(self, probability: float) -> float:
        """Return an explicitly stored quantile; never interpolate a new one."""

        _require_finite(probability, "probability")
        for point in self.quantiles:
            if math.isclose(
                point.probability,
                probability,
                rel_tol=0.0,
                abs_tol=_NUMERIC_TOLERANCE,
            ):
                return point.value
        raise KeyError("requested quantile is not present in the evidence")


@dataclass(frozen=True, slots=True)
class TrackerForecast:
    """Complete local tracker forecast or an explicit indeterminate record."""

    forecast_id: str
    tracker_id: str
    state: ForecastState
    analysis_grid_id: str
    tracker_x_m: float
    tracker_y_m: float
    reference_valid_at: float
    forecast_valid_at: float
    lead_time_seconds: float
    horizon_seconds: float
    input_track_ids: tuple[str, ...]
    input_field_ids: tuple[str, ...]
    algorithm_version: str
    calibration_version: str | None
    passage_probability: float | None
    no_passage_probability: float | None
    probability_evidence: PassageProbabilityEvidence | None
    conditional_eta_distribution: ConditionalDistribution | None
    conditional_eta_interval: DistributionInterval | None
    local_intensity_distribution: ConditionalDistribution | None
    indeterminate_reasons: tuple[str, ...]
    contract_version: str = TRACKER_FORECAST_CONTRACT_VERSION

    def __post_init__(self) -> None:
        for label, value in (
            ("forecast_id", self.forecast_id),
            ("tracker_id", self.tracker_id),
            ("analysis_grid_id", self.analysis_grid_id),
            ("algorithm_version", self.algorithm_version),
            ("contract_version", self.contract_version),
        ):
            _require_nonempty_string(value, label)
        if not isinstance(self.state, ForecastState):
            raise TypeError("state must be ForecastState")
        for label, value in (
            ("tracker_x_m", self.tracker_x_m),
            ("tracker_y_m", self.tracker_y_m),
            ("reference_valid_at", self.reference_valid_at),
            ("forecast_valid_at", self.forecast_valid_at),
            ("lead_time_seconds", self.lead_time_seconds),
            ("horizon_seconds", self.horizon_seconds),
        ):
            _require_finite(value, label)
        if self.lead_time_seconds < 0.0:
            raise ValueError("lead_time_seconds cannot be negative")
        if self.horizon_seconds <= 0.0:
            raise ValueError("horizon_seconds must be positive")
        if self.lead_time_seconds > self.horizon_seconds:
            raise ValueError("lead time cannot exceed the valid horizon")
        if not math.isclose(
            self.forecast_valid_at - self.reference_valid_at,
            self.lead_time_seconds,
            rel_tol=1e-12,
            abs_tol=_TIME_TOLERANCE_SECONDS,
        ):
            raise ValueError("forecast timestamp must equal reference plus lead time")
        _validate_unique_ids(self.input_track_ids, "input_track_ids")
        _validate_unique_ids(self.input_field_ids, "input_field_ids")
        if self.calibration_version is not None:
            _require_nonempty_string(
                self.calibration_version,
                "calibration_version",
            )
        _validate_reason_codes(self.indeterminate_reasons)

        if self.state is ForecastState.INDETERMINATE:
            self._validate_indeterminate()
        else:
            self._validate_determinate()

    def _validate_indeterminate(self) -> None:
        if not self.indeterminate_reasons:
            raise ValueError("INDETERMINATE forecast requires reason codes")
        if any(
            value is not None
            for value in (
                self.passage_probability,
                self.no_passage_probability,
                self.probability_evidence,
                self.conditional_eta_distribution,
                self.conditional_eta_interval,
                self.local_intensity_distribution,
            )
        ):
            raise ValueError(
                "INDETERMINATE forecast cannot expose incomplete forecast products"
            )

    def _validate_determinate(self) -> None:
        if self.indeterminate_reasons:
            raise ValueError("determinate forecast cannot carry indeterminate reasons")
        if not isinstance(self.probability_evidence, PassageProbabilityEvidence):
            raise ValueError("determinate forecast requires probability evidence")
        _validate_probability_pair(
            self.passage_probability,
            self.no_passage_probability,
        )
        passage_probability = float(self.passage_probability)
        evidence = self.probability_evidence
        if evidence.basis is ProbabilityEvidenceBasis.ENSEMBLE_FREQUENCY:
            expected = float(evidence.passage_count) / evidence.sample_size
            if not math.isclose(
                passage_probability,
                expected,
                rel_tol=0.0,
                abs_tol=_NUMERIC_TOLERANCE,
            ):
                raise ValueError(
                    "passage probability must equal raw ensemble frequency"
                )
            uses_empirical_calibration = False
        else:
            if self.calibration_version != evidence.calibration_version:
                raise ValueError(
                    "forecast and empirical probability calibration versions differ"
                )
            uses_empirical_calibration = True

        if passage_probability == 0.0:
            if any(
                value is not None
                for value in (
                    self.conditional_eta_distribution,
                    self.conditional_eta_interval,
                    self.local_intensity_distribution,
                )
            ):
                raise ValueError(
                    "conditional distributions are undefined when passage chance is zero"
                )
            if self.calibration_version is not None and not uses_empirical_calibration:
                raise ValueError(
                    "forecast calibration version requires empirical evidence"
                )
            return

        if (
            not isinstance(
                self.conditional_eta_distribution,
                ConditionalDistribution,
            )
            or self.conditional_eta_distribution.kind
            is not ConditionalDistributionKind.CONDITIONAL_ETA
        ):
            raise ValueError("positive passage chance requires conditional ETA")
        if not isinstance(self.conditional_eta_interval, DistributionInterval):
            raise ValueError("positive passage chance requires an ETA interval")
        if (
            not isinstance(
                self.local_intensity_distribution,
                ConditionalDistribution,
            )
            or self.local_intensity_distribution.kind
            is not ConditionalDistributionKind.CONDITIONAL_LOCAL_INTENSITY
        ):
            raise ValueError(
                "positive passage chance requires local intensity distribution"
            )
        _validate_interval_against_distribution(
            self.conditional_eta_interval,
            self.conditional_eta_distribution,
        )
        if any(
            point.value > self.horizon_seconds
            for point in self.conditional_eta_distribution.quantiles
        ):
            raise ValueError("conditional ETA extends beyond valid forecast horizon")
        for distribution in (
            self.conditional_eta_distribution,
            self.local_intensity_distribution,
        ):
            if (
                distribution.basis is DistributionEvidenceBasis.EMPIRICAL_SAMPLES
            ):
                uses_empirical_calibration = True
                if distribution.calibration_version != self.calibration_version:
                    raise ValueError(
                        "forecast and empirical distribution calibration versions differ"
                    )
        if self.calibration_version is not None and not uses_empirical_calibration:
            raise ValueError(
                "forecast calibration version requires empirical evidence"
            )


@dataclass(frozen=True, slots=True)
class TrackerStatusSeparation:
    """Small JSON-safe shadow view with four independent semantics.

    ``current_precipitation`` combines two explicit positive bases: exact
    current-frame radar-footprint evidence and typed local ground evidence.
    Their source-specific states remain separate. ``nearby_precipitation`` is
    radar spatial context. ``next_threat`` is a deterministic legacy-motion
    candidate, while ``passage_forecast`` is populated only by a valid
    :class:`TrackerForecast`. None of these fields mutates or reinterprets the
    legacy Home Assistant entity state.
    """

    current_state: PrecipitationPresence
    current_radar_state: PrecipitationPresence
    current_storm_ids: tuple[str, ...]
    current_evidence_id: str | None
    current_valid_at: float | None
    current_footprint_scan: CurrentFootprintEvidence | None
    current_ground_evidence: tuple[CurrentGroundEvidence, ...]
    current_ground_collection: BoundedCollectionDiagnostic
    candidate_collection: BoundedCollectionDiagnostic
    nearby_state: PrecipitationPresence
    nearby_storm_id: str | None
    nearby_distance_km: float | None
    next_threat_state: NextThreatState
    next_threat_storm_id: str | None
    next_threat_eta_minutes: float | None
    next_threat_relation: PassageRelation
    passage_forecast_state: ForecastState
    passage_forecast_id: str | None
    passage_probability: float | None
    no_passage_probability: float | None
    passage_reason_codes: tuple[str, ...]
    legacy_selected_relation: PassageRelation
    legacy_status: str | None
    display_state: TrackerDisplayState
    separation_reason_codes: tuple[str, ...]
    contract_version: str = TRACKER_STATUS_SEPARATION_VERSION

    def __post_init__(self) -> None:
        for label, value, expected in (
            ("current_state", self.current_state, PrecipitationPresence),
            (
                "current_radar_state",
                self.current_radar_state,
                PrecipitationPresence,
            ),
            ("nearby_state", self.nearby_state, PrecipitationPresence),
            ("next_threat_state", self.next_threat_state, NextThreatState),
            (
                "next_threat_relation",
                self.next_threat_relation,
                PassageRelation,
            ),
            (
                "passage_forecast_state",
                self.passage_forecast_state,
                ForecastState,
            ),
            (
                "legacy_selected_relation",
                self.legacy_selected_relation,
                PassageRelation,
            ),
            ("display_state", self.display_state, TrackerDisplayState),
        ):
            if not isinstance(value, expected):
                raise TypeError(f"{label} must be {expected.__name__}")
        _require_nonempty_string(self.contract_version, "contract_version")
        _validate_reason_codes(self.passage_reason_codes)
        _validate_reason_codes(self.separation_reason_codes)
        if self.legacy_status is not None:
            _require_nonempty_string(self.legacy_status, "legacy_status")

        if (
            self.current_footprint_scan is not None
            and not isinstance(
                self.current_footprint_scan,
                CurrentFootprintEvidence,
            )
        ):
            raise TypeError(
                "current_footprint_scan must be CurrentFootprintEvidence"
            )
        for label, diagnostic in (
            ("current_ground_collection", self.current_ground_collection),
            ("candidate_collection", self.candidate_collection),
        ):
            if not isinstance(diagnostic, BoundedCollectionDiagnostic):
                raise TypeError(
                    f"{label} must be BoundedCollectionDiagnostic"
                )
        _validate_current_ground_evidence(self.current_ground_evidence)
        if len(self.current_ground_evidence) > (
            MAX_CURRENT_GROUND_EVIDENCE_RECORDS
        ):
            raise ValueError("serialized current ground evidence is unbounded")
        if self.current_ground_collection.serialized_count != len(
            self.current_ground_evidence
        ):
            raise ValueError("ground collection serialized count mismatch")
        if (
            self.current_ground_evidence
            and self.current_footprint_scan is not None
            and (
                any(
                    evidence.tracker_id
                    != self.current_footprint_scan.tracker_id
                    for evidence in self.current_ground_evidence
                )
                or any(
                    not math.isclose(
                        coordinate,
                        footprint_coordinate,
                        rel_tol=0.0,
                        abs_tol=1e-9,
                    )
                    for coordinate, footprint_coordinate in (
                        (
                            self.current_ground_evidence[0].tracker_latitude,
                            self.current_footprint_scan.tracker_latitude,
                        ),
                        (
                            self.current_ground_evidence[0].tracker_longitude,
                            self.current_footprint_scan.tracker_longitude,
                        ),
                    )
                )
            )
        ):
            raise ValueError(
                "radar and ground evidence target different trackers"
            )
        if self.current_radar_state is PrecipitationPresence.PRESENT:
            _validate_unique_ids(
                self.current_storm_ids,
                "current_storm_ids",
            )
            _require_nonempty_string(
                self.current_evidence_id,
                "current_evidence_id",
            )
            _require_finite(self.current_valid_at, "current_valid_at")
            if (
                self.current_footprint_scan is None
                or not self.current_footprint_scan.fresh
                or self.current_storm_ids
                != self.current_footprint_scan.containing_storm_ids
            ):
                raise ValueError(
                    "present radar state requires matching fresh footprint scan"
                )
        elif self.current_radar_state is PrecipitationPresence.ABSENT:
            if self.current_storm_ids:
                raise ValueError(
                    "absent radar state cannot expose footprint members"
                )
            if (self.current_evidence_id is None) != (
                self.current_valid_at is None
            ):
                raise ValueError(
                    "absent current evidence ID and valid time must coexist"
                )
            if self.current_evidence_id is not None:
                _require_nonempty_string(
                    self.current_evidence_id,
                    "current_evidence_id",
                )
                _require_finite(self.current_valid_at, "current_valid_at")
            if (
                self.current_footprint_scan is None
                or not self.current_footprint_scan.fresh
                or not self.current_footprint_scan.scan_complete
            ):
                raise ValueError(
                    "absent radar state requires a fresh complete footprint scan"
                )
        elif (
            self.current_storm_ids
            or self.current_evidence_id is not None
            or self.current_valid_at is not None
        ):
            raise ValueError(
                "indeterminate radar state cannot expose footprint evidence"
            )

        expected_current_state = (
            PrecipitationPresence.PRESENT
            if (
                self.current_radar_state is PrecipitationPresence.PRESENT
                or self.current_ground_evidence
            )
            else self.current_radar_state
        )
        if self.current_state is not expected_current_state:
            raise ValueError(
                "current_state must preserve positive radar or ground evidence"
            )

        if self.nearby_state is PrecipitationPresence.PRESENT:
            _require_nonempty_string(
                self.nearby_storm_id,
                "nearby_storm_id",
            )
            _require_nonnegative_finite(
                self.nearby_distance_km,
                "nearby_distance_km",
            )
            if (
                float(self.nearby_distance_km)
                <= CURRENT_FOOTPRINT_DISTANCE_TOLERANCE_KM
            ):
                raise ValueError(
                    "nearby precipitation cannot be inside the tracker footprint"
                )
        elif (
            self.nearby_storm_id is not None
            or self.nearby_distance_km is not None
        ):
            raise ValueError(
                "non-present nearby state cannot expose a nearby storm"
            )
        if self.nearby_state is PrecipitationPresence.ABSENT and (
            self.current_footprint_scan is None
            or not self.current_footprint_scan.fresh
            or not self.current_footprint_scan.scan_complete
        ):
            raise ValueError(
                "absent nearby state requires a fresh complete footprint scan"
            )

        if self.next_threat_state is NextThreatState.DETECTED:
            _require_nonempty_string(
                self.next_threat_storm_id,
                "next_threat_storm_id",
            )
            _require_nonnegative_finite(
                self.next_threat_eta_minutes,
                "next_threat_eta_minutes",
            )
            if self.next_threat_relation not in {
                PassageRelation.HIT,
                PassageRelation.EDGE,
            }:
                raise ValueError(
                    "detected next threat must be a hit or edge passage"
                )
        elif (
            self.next_threat_storm_id is not None
            or self.next_threat_eta_minutes is not None
            or self.next_threat_relation is not PassageRelation.INDETERMINATE
        ):
            raise ValueError(
                "non-detected next threat cannot expose a threat candidate"
            )
        if (
            self.next_threat_state is not NextThreatState.INDETERMINATE
            and not self.candidate_collection.complete
        ):
            raise ValueError(
                "determinate next-threat state requires complete candidate scan"
            )

        if self.passage_forecast_state is ForecastState.DETERMINATE:
            _require_nonempty_string(
                self.passage_forecast_id,
                "passage_forecast_id",
            )
            _validate_probability_pair(
                self.passage_probability,
                self.no_passage_probability,
            )
            if self.passage_reason_codes:
                raise ValueError(
                    "determinate passage forecast cannot carry reason codes"
                )
        elif (
            self.passage_forecast_id is not None
            or self.passage_probability is not None
            or self.no_passage_probability is not None
            or not self.passage_reason_codes
        ):
            raise ValueError(
                "indeterminate passage forecast requires only reason codes"
            )

        expected_display = _shadow_display_state(
            self.current_state,
            self.nearby_state,
            self.next_threat_state,
        )
        if self.display_state is not expected_display:
            raise ValueError(
                "display_state violates current/next/nearby precedence"
            )

    def to_dict(self) -> dict[str, Any]:
        """Return a compact HA-attribute-safe representation."""

        positive_bases = []
        if self.current_radar_state is PrecipitationPresence.PRESENT:
            positive_bases.append("current_radar_footprint")
        if self.current_ground_evidence:
            positive_bases.append("positive_local_ground")
        associated_storm_ids = sorted({
            *self.current_storm_ids,
            *(
                evidence.matched_storm_id
                for evidence in self.current_ground_evidence
                if evidence.matched_storm_id is not None
            ),
        })
        return {
            "contract_version": self.contract_version,
            "mode": "shadow_only",
            "display_state": self.display_state.value,
            "current_precipitation": {
                "state": self.current_state.value,
                "positive_bases": positive_bases,
                "associated_storm_ids": associated_storm_ids,
                "storm_ids": list(self.current_storm_ids),
                "evidence_id": self.current_evidence_id,
                "valid_at": self.current_valid_at,
                "radar_footprint": {
                    "state": self.current_radar_state.value,
                    "storm_ids": list(self.current_storm_ids),
                    "evidence_id": self.current_evidence_id,
                    "valid_at": self.current_valid_at,
                    "scan": (
                        self.current_footprint_scan.to_dict()
                        if self.current_footprint_scan is not None
                        else None
                    ),
                },
                "ground_evidence": [
                    evidence.to_dict()
                    for evidence in self.current_ground_evidence
                ],
                "ground_evidence_collection": (
                    self.current_ground_collection.to_dict()
                ),
            },
            "nearby_precipitation": {
                "state": self.nearby_state.value,
                "storm_id": self.nearby_storm_id,
                "distance_km": self.nearby_distance_km,
            },
            "next_threat": {
                "state": self.next_threat_state.value,
                "storm_id": self.next_threat_storm_id,
                "eta_minutes": self.next_threat_eta_minutes,
                "passage_relation": self.next_threat_relation.value,
                "candidate_collection": self.candidate_collection.to_dict(),
            },
            "passage_forecast": {
                "state": self.passage_forecast_state.value,
                "forecast_id": self.passage_forecast_id,
                "passage_probability": self.passage_probability,
                "no_passage_probability": self.no_passage_probability,
                "reason_codes": list(self.passage_reason_codes),
            },
            "legacy_selected_relation": self.legacy_selected_relation.value,
            "legacy_status": self.legacy_status,
            "reason_codes": list(self.separation_reason_codes),
        }


def build_tracker_forecast_shadow(
    legacy_summary: Mapping[str, Any],
    *,
    current_footprint: CurrentFootprintEvidence | None = None,
    current_ground: tuple[CurrentGroundEvidence, ...] = (),
    passage_forecast: TrackerForecast | None = None,
) -> TrackerStatusSeparation:
    """Separate legacy observations from a formal passage forecast.

    The adapter is intentionally additive and read-only. Rounded legacy
    distances never become current-footprint truth. Radar containment must
    arrive as :class:`CurrentFootprintEvidence`; local current precipitation
    must arrive as typed positive :class:`CurrentGroundEvidence`. Future
    ``mist`` motion then remains visible as a separate legacy relation but
    cannot replace either current observation.
    """

    if not isinstance(legacy_summary, Mapping):
        raise TypeError("legacy_summary must be a mapping")
    if current_footprint is not None and not isinstance(
        current_footprint,
        CurrentFootprintEvidence,
    ):
        raise TypeError("current_footprint must be CurrentFootprintEvidence")
    _validate_current_ground_evidence(current_ground)
    if passage_forecast is not None and not isinstance(
        passage_forecast,
        TrackerForecast,
    ):
        raise TypeError("passage_forecast must be TrackerForecast")

    reasons: list[str] = []
    current_ground, ground_collection = _bounded_ground_collection(
        current_ground
    )
    radar_covered = legacy_summary.get("radar_covered")
    radar_covered = radar_covered if isinstance(radar_covered, bool) else None
    active_count = _optional_nonnegative_int(
        legacy_summary.get("active_system_count")
    )
    legacy_tracker_id = _optional_nonempty_string(
        legacy_summary.get("target_id")
    )

    if current_ground and legacy_tracker_id is not None and any(
        evidence.tracker_id != legacy_tracker_id
        for evidence in current_ground
    ):
        reasons.append("current_ground_tracker_mismatch")
        current_ground = ()
        ground_collection = _with_collection_reason(
            ground_collection,
            "current_ground_tracker_mismatch",
            serialize_none=True,
        )
    if not ground_collection.complete:
        reasons.extend(ground_collection.reason_codes)

    footprint_tracker_matches = not (
        current_footprint is not None
        and legacy_tracker_id is not None
        and current_footprint.tracker_id != legacy_tracker_id
    )
    usable_footprint = current_footprint
    if current_footprint is not None and radar_covered is False:
        usable_footprint = None
        reasons.append("current_footprint_ignored_without_radar_coverage")
    elif current_footprint is not None and not footprint_tracker_matches:
        usable_footprint = None
        reasons.append("current_footprint_tracker_mismatch")
    elif current_footprint is not None and not current_footprint.fresh:
        usable_footprint = None
        reasons.append("current_footprint_stale")
    if current_footprint is not None and radar_covered is False:
        reasons.append("radar_negative_claims_disabled_without_coverage")

    footprint_complete = bool(
        usable_footprint is not None
        and usable_footprint.scan_complete
        and radar_covered is True
        and (
            active_count is None
            or active_count == usable_footprint.input_storm_count
        )
    )
    if (
        usable_footprint is not None
        and active_count is not None
        and active_count != usable_footprint.input_storm_count
    ):
        reasons.append("current_footprint_active_count_mismatch")
    if usable_footprint is not None and not usable_footprint.scan_complete:
        reasons.append("current_footprint_scan_incomplete")

    if usable_footprint is not None and usable_footprint.containing_storm_ids:
        current_ids = usable_footprint.containing_storm_ids
        current_radar_state = PrecipitationPresence.PRESENT
        current_evidence_id = usable_footprint.evidence_id
        current_valid_at = usable_footprint.frame_valid_at
    elif usable_footprint is not None and footprint_complete:
        current_radar_state = PrecipitationPresence.ABSENT
        current_ids = ()
        current_evidence_id = usable_footprint.evidence_id
        current_valid_at = usable_footprint.frame_valid_at
    else:
        current_radar_state = PrecipitationPresence.INDETERMINATE
        current_ids = ()
        current_evidence_id = None
        current_valid_at = None
        reasons.append("exact_current_footprint_evidence_missing")

    current_state = (
        PrecipitationPresence.PRESENT
        if (
            current_radar_state is PrecipitationPresence.PRESENT
            or current_ground
        )
        else current_radar_state
    )
    if current_ground:
        reasons.append("current_precipitation_from_positive_local_ground")
        if current_radar_state is PrecipitationPresence.ABSENT:
            reasons.append("radar_ground_current_disagreement")
        elif current_radar_state is PrecipitationPresence.INDETERMINATE:
            reasons.append(
                "current_ground_without_radar_footprint_determination"
            )
    elif legacy_summary.get("ground_precipitation_observed") is True:
        reasons.append("typed_positive_ground_evidence_missing_or_rejected")

    candidate_records, candidate_collection = _legacy_candidate_records(
        legacy_summary.get("approaching_systems")
    )
    candidate_reasons = list(candidate_collection.reason_codes)
    if not footprint_complete:
        candidate_reasons.append("typed_complete_candidate_scan_missing")
    evaluated_id_set = (
        set(usable_footprint.evaluated_storm_ids)
        if usable_footprint is not None else set()
    )
    candidate_id_set = {
        record["storm_id"] for record in candidate_records
    }
    if candidate_id_set - evaluated_id_set:
        candidate_reasons.append("candidate_not_in_typed_footprint_scan")
    if footprint_complete and candidate_id_set != evaluated_id_set:
        candidate_reasons.append(
            "candidate_scan_does_not_cover_typed_footprint"
        )
    for reason in tuple(sorted(set(candidate_reasons))):
        if reason not in candidate_collection.reason_codes:
            candidate_collection = _with_collection_reason(
                candidate_collection,
                reason,
            )
    reasons.extend(candidate_collection.reason_codes)
    candidate_scan_complete = candidate_collection.complete
    current_radar_id_set = set(current_ids)
    current_threat_id_set = {
        *current_ids,
        *(
            evidence.matched_storm_id
            for evidence in current_ground
            if evidence.matched_storm_id is not None
        ),
    }
    next_candidates = [
        record
        for record in candidate_records
        if record["storm_id"] not in current_threat_id_set
        and record["relation"] in {
            PassageRelation.HIT,
            PassageRelation.EDGE,
        }
        and record["eta_minutes"] is not None
    ]
    if next_candidates and candidate_scan_complete:
        selected_next = min(
            next_candidates,
            key=lambda item: (
                float(item["eta_minutes"]),
                (
                    float(item["distance_km"])
                    if item["distance_km"] is not None
                    else math.inf
                ),
                item["storm_id"],
            ),
        )
        next_state = NextThreatState.DETECTED
        next_storm_id = selected_next["storm_id"]
        next_eta = float(selected_next["eta_minutes"])
        next_relation = selected_next["relation"]
    elif candidate_scan_complete:
        next_state = NextThreatState.NOT_DETECTED
        next_storm_id = None
        next_eta = None
        next_relation = PassageRelation.INDETERMINATE
    else:
        next_state = NextThreatState.INDETERMINATE
        next_storm_id = None
        next_eta = None
        next_relation = PassageRelation.INDETERMINATE
        reasons.append("next_threat_candidate_scan_indeterminate")

    nearby_candidates = [
        record
        for record in candidate_records
        if record["storm_id"] not in current_radar_id_set
        and record["distance_km"] is not None
        and float(record["distance_km"])
        > CURRENT_FOOTPRINT_DISTANCE_TOLERANCE_KM
    ]
    if (
        usable_footprint is not None
        and not current_ids
        and usable_footprint.nearest_storm_id is not None
    ):
        nearby_candidates.append(
            {
                "storm_id": usable_footprint.nearest_storm_id,
                "distance_km": usable_footprint.minimum_distance_km,
                "eta_minutes": None,
                "relation": PassageRelation.INDETERMINATE,
            }
        )
    if nearby_candidates and candidate_scan_complete:
        selected_nearby = min(
            nearby_candidates,
            key=lambda item: (
                float(item["distance_km"]),
                item["storm_id"],
            ),
        )
        nearby_state = PrecipitationPresence.PRESENT
        nearby_storm_id = selected_nearby["storm_id"]
        nearby_distance = float(selected_nearby["distance_km"])
    elif (
        usable_footprint is not None
        and not current_ids
        and usable_footprint.nearest_storm_id is not None
    ):
        nearby_state = PrecipitationPresence.PRESENT
        nearby_storm_id = usable_footprint.nearest_storm_id
        nearby_distance = float(usable_footprint.minimum_distance_km)
    elif (
        footprint_complete
        and usable_footprint is not None
        and set(usable_footprint.evaluated_storm_ids).issubset(
            current_radar_id_set
        )
    ):
        nearby_state = PrecipitationPresence.ABSENT
        nearby_storm_id = None
        nearby_distance = None
    else:
        nearby_state = PrecipitationPresence.INDETERMINATE
        nearby_storm_id = None
        nearby_distance = None
        reasons.append("nearby_precipitation_evidence_incomplete")

    (
        passage_state,
        passage_forecast_id,
        passage_probability,
        no_passage_probability,
        passage_reasons,
    ) = _passage_shadow(legacy_summary, passage_forecast)
    legacy_relation = _legacy_passage_relation(
        legacy_summary.get("passage_classification")
    )
    legacy_status = _optional_nonempty_string(
        legacy_summary.get("status")
    )
    display_state = _shadow_display_state(
        current_state,
        nearby_state,
        next_state,
    )
    return TrackerStatusSeparation(
        current_state=current_state,
        current_radar_state=current_radar_state,
        current_storm_ids=current_ids,
        current_evidence_id=current_evidence_id,
        current_valid_at=current_valid_at,
        current_footprint_scan=current_footprint,
        current_ground_evidence=current_ground,
        current_ground_collection=ground_collection,
        candidate_collection=candidate_collection,
        nearby_state=nearby_state,
        nearby_storm_id=nearby_storm_id,
        nearby_distance_km=nearby_distance,
        next_threat_state=next_state,
        next_threat_storm_id=next_storm_id,
        next_threat_eta_minutes=next_eta,
        next_threat_relation=next_relation,
        passage_forecast_state=passage_state,
        passage_forecast_id=passage_forecast_id,
        passage_probability=passage_probability,
        no_passage_probability=no_passage_probability,
        passage_reason_codes=passage_reasons,
        legacy_selected_relation=legacy_relation,
        legacy_status=legacy_status,
        display_state=display_state,
        separation_reason_codes=tuple(sorted(set(reasons))),
    )


def _passage_shadow(
    legacy_summary: Mapping[str, Any],
    passage_forecast: TrackerForecast | None,
) -> tuple[
    ForecastState,
    str | None,
    float | None,
    float | None,
    tuple[str, ...],
]:
    if passage_forecast is None:
        return (
            ForecastState.INDETERMINATE,
            None,
            None,
            None,
            ("formal_passage_forecast_missing",),
        )
    tracker_id = _optional_nonempty_string(legacy_summary.get("target_id"))
    if tracker_id is not None and passage_forecast.tracker_id != tracker_id:
        return (
            ForecastState.INDETERMINATE,
            None,
            None,
            None,
            ("passage_forecast_tracker_mismatch",),
        )
    if passage_forecast.state is ForecastState.INDETERMINATE:
        return (
            ForecastState.INDETERMINATE,
            None,
            None,
            None,
            passage_forecast.indeterminate_reasons,
        )
    return (
        ForecastState.DETERMINATE,
        passage_forecast.forecast_id,
        passage_forecast.passage_probability,
        passage_forecast.no_passage_probability,
        (),
    )


def _legacy_candidate_records(
    raw_candidates: object,
) -> tuple[list[dict[str, Any]], BoundedCollectionDiagnostic]:
    if not isinstance(raw_candidates, (list, tuple)):
        return [], BoundedCollectionDiagnostic(
            input_total_count=0,
            inspected_count=0,
            accepted_total_count=0,
            serialized_count=0,
            malformed_count=0,
            overflow_count=0,
            complete=False,
            content_sha256=_stable_collection_sha256((raw_candidates,)),
            reason_codes=("legacy_candidate_collection_missing",),
        )
    input_total = len(raw_candidates)
    content_sha256 = _stable_collection_sha256(raw_candidates)
    records: list[dict[str, Any]] = []
    malformed_count = 0
    diagnostic_reasons = set()
    if input_total > MAX_LEGACY_CANDIDATE_RECORDS:
        diagnostic_reasons.add("legacy_candidate_collection_resource_limit")
    inspected = raw_candidates[:MAX_LEGACY_CANDIDATE_RECORDS]
    seen_storm_ids = set()
    for raw_candidate in inspected:
        if not isinstance(raw_candidate, Mapping):
            malformed_count += 1
            diagnostic_reasons.add("legacy_candidate_not_mapping")
            continue
        storm_id = _optional_nonempty_string(raw_candidate.get("storm_id"))
        if storm_id is None or len(storm_id) > MAX_IDENTIFIER_LENGTH:
            malformed_count += 1
            diagnostic_reasons.add("legacy_candidate_invalid_storm_id")
            continue
        if storm_id in seen_storm_ids:
            malformed_count += 1
            diagnostic_reasons.add("legacy_candidate_duplicate_storm_id")
            continue
        distance = _optional_nonnegative_finite(
            raw_candidate.get("distance_km")
        )
        if distance is None:
            malformed_count += 1
            diagnostic_reasons.add("legacy_candidate_invalid_distance")
            continue
        raw_relation = raw_candidate.get("passage_classification")
        relation = _legacy_passage_relation(raw_relation)
        if relation is PassageRelation.INDETERMINATE:
            malformed_count += 1
            diagnostic_reasons.add("legacy_candidate_invalid_relation")
            continue
        eta = _optional_nonnegative_finite(
            raw_candidate.get("eta_minutes")
        )
        if (
            relation in {PassageRelation.HIT, PassageRelation.EDGE}
            and eta is None
        ):
            malformed_count += 1
            diagnostic_reasons.add("legacy_candidate_hit_without_eta")
            continue
        if (
            raw_candidate.get("eta_minutes") is not None
            and eta is None
        ):
            malformed_count += 1
            diagnostic_reasons.add("legacy_candidate_invalid_eta")
            continue
        seen_storm_ids.add(storm_id)
        records.append(
            {
                "storm_id": storm_id,
                "distance_km": distance,
                "eta_minutes": eta,
                "relation": relation,
            }
        )
    reason_codes = tuple(sorted(diagnostic_reasons))
    return records, BoundedCollectionDiagnostic(
        input_total_count=input_total,
        inspected_count=len(inspected),
        accepted_total_count=len(records),
        serialized_count=len(records),
        malformed_count=malformed_count,
        overflow_count=0,
        complete=not reason_codes and malformed_count == 0,
        content_sha256=content_sha256,
        reason_codes=reason_codes,
    )


def _bounded_ground_collection(
    values: tuple[CurrentGroundEvidence, ...],
) -> tuple[
    tuple[CurrentGroundEvidence, ...],
    BoundedCollectionDiagnostic,
]:
    content_sha256 = _stable_collection_sha256(
        tuple(value.to_dict() for value in values)
    )
    serialized = values[:MAX_CURRENT_GROUND_EVIDENCE_RECORDS]
    overflow = len(values) - len(serialized)
    reason_codes = (
        ("ground_evidence_serialization_bounded",)
        if overflow else ()
    )
    return serialized, BoundedCollectionDiagnostic(
        input_total_count=len(values),
        inspected_count=len(values),
        accepted_total_count=len(values),
        serialized_count=len(serialized),
        malformed_count=0,
        overflow_count=overflow,
        complete=overflow == 0,
        content_sha256=content_sha256,
        reason_codes=reason_codes,
    )


def _with_collection_reason(
    diagnostic: BoundedCollectionDiagnostic,
    reason: str,
    *,
    serialize_none: bool = False,
) -> BoundedCollectionDiagnostic:
    _require_nonempty_string(reason, "collection reason")
    serialized_count = (
        0 if serialize_none else diagnostic.serialized_count
    )
    overflow_count = (
        diagnostic.accepted_total_count - serialized_count
    )
    reasons = tuple(sorted({
        *diagnostic.reason_codes,
        reason,
    }))[:MAX_DIAGNOSTIC_REASON_CODES]
    return BoundedCollectionDiagnostic(
        input_total_count=diagnostic.input_total_count,
        inspected_count=diagnostic.inspected_count,
        accepted_total_count=diagnostic.accepted_total_count,
        serialized_count=serialized_count,
        malformed_count=diagnostic.malformed_count,
        overflow_count=overflow_count,
        complete=False,
        content_sha256=diagnostic.content_sha256,
        reason_codes=reasons,
    )


def _stable_collection_sha256(values: object) -> str:
    digest = hashlib.sha256()
    iterable = values if isinstance(values, (list, tuple)) else (values,)
    for value in iterable:
        if hasattr(value, "to_dict"):
            value = value.to_dict()
        try:
            encoded = json.dumps(
                value,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
                allow_nan=False,
                default=lambda item: {
                    "unsupported_type": type(item).__name__,
                },
            ).encode("utf-8")
        except (TypeError, ValueError):
            encoded = json.dumps(
                {"unencodable_type": type(value).__name__},
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
    return digest.hexdigest()


def _legacy_passage_relation(value: object) -> PassageRelation:
    return {
        "actueel": PassageRelation.CURRENT,
        "raak": PassageRelation.HIT,
        "rand": PassageRelation.EDGE,
        "mist": PassageRelation.MISS,
    }.get(value, PassageRelation.INDETERMINATE)


def _shadow_display_state(
    current_state: PrecipitationPresence,
    nearby_state: PrecipitationPresence,
    next_state: NextThreatState,
) -> TrackerDisplayState:
    if current_state is PrecipitationPresence.PRESENT:
        return TrackerDisplayState.CURRENT_PRECIPITATION
    if next_state is NextThreatState.DETECTED:
        return TrackerDisplayState.NEXT_THREAT
    if nearby_state is PrecipitationPresence.PRESENT:
        return TrackerDisplayState.NEARBY_PRECIPITATION
    if (
        current_state is PrecipitationPresence.ABSENT
        and nearby_state is PrecipitationPresence.ABSENT
        and next_state is NextThreatState.NOT_DETECTED
    ):
        return TrackerDisplayState.NO_CURRENT_OR_NEARBY_PRECIPITATION
    return TrackerDisplayState.INDETERMINATE


def _validate_probability_pair(
    passage_probability: float | None,
    no_passage_probability: float | None,
) -> None:
    _require_finite(passage_probability, "passage_probability")
    _require_finite(no_passage_probability, "no_passage_probability")
    passage = float(passage_probability)
    no_passage = float(no_passage_probability)
    if not 0.0 <= passage <= 1.0 or not 0.0 <= no_passage <= 1.0:
        raise ValueError("passage probabilities must be in [0, 1]")
    if not math.isclose(
        passage + no_passage,
        1.0,
        rel_tol=0.0,
        abs_tol=_NUMERIC_TOLERANCE,
    ):
        raise ValueError("passage and no-passage probabilities must sum to one")


def _validate_interval_against_distribution(
    interval: DistributionInterval,
    distribution: ConditionalDistribution,
) -> None:
    try:
        expected_lower = distribution.quantile_value(interval.lower_probability)
        expected_upper = distribution.quantile_value(interval.upper_probability)
    except KeyError as error:
        raise ValueError(
            "ETA interval endpoints must be explicit stored quantiles"
        ) from error
    if not math.isclose(
        interval.lower_value,
        expected_lower,
        rel_tol=1e-12,
        abs_tol=_NUMERIC_TOLERANCE,
    ) or not math.isclose(
        interval.upper_value,
        expected_upper,
        rel_tol=1e-12,
        abs_tol=_NUMERIC_TOLERANCE,
    ):
        raise ValueError("ETA interval values do not match ETA quantiles")


def _validate_unique_ids(values: tuple[str, ...], label: str) -> None:
    _validate_id_tuple(values, label, allow_empty=False)


def _validate_id_tuple(
    values: tuple[str, ...],
    label: str,
    *,
    allow_empty: bool,
    canonical: bool = False,
) -> None:
    if (
        not isinstance(values, tuple)
        or (not allow_empty and not values)
        or any(
            not isinstance(value, str)
            or not value
            or len(value) > MAX_IDENTIFIER_LENGTH
            for value in values
        )
        or len(values) != len(set(values))
    ):
        raise ValueError(f"{label} must contain unique bounded non-empty IDs")
    if canonical and tuple(sorted(values)) != values:
        raise ValueError(f"{label} must be canonically sorted")


def _validate_current_ground_evidence(
    values: tuple[CurrentGroundEvidence, ...],
) -> None:
    if not isinstance(values, tuple) or any(
        not isinstance(value, CurrentGroundEvidence)
        for value in values
    ):
        raise TypeError(
            "current_ground must be a tuple of CurrentGroundEvidence"
        )
    evidence_ids = tuple(value.evidence_id for value in values)
    if len(evidence_ids) != len(set(evidence_ids)):
        raise ValueError("current_ground evidence IDs must be unique")
    if tuple(sorted(evidence_ids)) != evidence_ids:
        raise ValueError("current_ground evidence must be canonically sorted")
    physical_site_ids = tuple(value.physical_site_id for value in values)
    if len(physical_site_ids) != len(set(physical_site_ids)):
        raise ValueError(
            "current_ground cannot count one physical site more than once"
        )
    station_keys = tuple(
        (value.provider_id, value.station_id) for value in values
    )
    if len(station_keys) != len(set(station_keys)):
        raise ValueError(
            "current_ground cannot count one provider station more than once"
        )
    source_record_ids = tuple(value.source_record_id for value in values)
    if len(source_record_ids) != len(set(source_record_ids)):
        raise ValueError("current_ground source record IDs must be unique")
    if values:
        tracker_ids = {value.tracker_id for value in values}
        if len(tracker_ids) != 1:
            raise ValueError("current_ground mixes tracker IDs")
        reference = values[0]
        for value in values[1:]:
            if (
                not math.isclose(
                    value.tracker_latitude,
                    reference.tracker_latitude,
                    rel_tol=0.0,
                    abs_tol=1e-9,
                )
                or not math.isclose(
                    value.tracker_longitude,
                    reference.tracker_longitude,
                    rel_tol=0.0,
                    abs_tol=1e-9,
                )
            ):
                raise ValueError("current_ground mixes tracker coordinates")


def _validate_reason_codes(values: tuple[str, ...]) -> None:
    if (
        not isinstance(values, tuple)
        or any(
            not isinstance(value, str)
            or not value
            or len(value) > MAX_IDENTIFIER_LENGTH
            for value in values
        )
        or len(values) != len(set(values))
        or len(values) > MAX_DIAGNOSTIC_REASON_CODES
    ):
        raise ValueError("indeterminate_reasons must be unique immutable codes")


def _require_nonempty_string(value: object, label: str) -> None:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > MAX_IDENTIFIER_LENGTH
    ):
        raise ValueError(f"{label} must be a bounded non-empty string")


def _require_positive_int(value: object, label: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{label} must be a positive integer")


def _require_nonnegative_int(value: object, label: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{label} must be a non-negative integer")


def _require_lat_lon(latitude: object, longitude: object) -> None:
    _require_finite(latitude, "tracker_latitude")
    _require_finite(longitude, "tracker_longitude")
    if not -90.0 <= float(latitude) <= 90.0:
        raise ValueError("tracker_latitude must be in [-90, 90]")
    if not -180.0 <= float(longitude) <= 180.0:
        raise ValueError("tracker_longitude must be in [-180, 180]")


def _require_sha256(value: object, label: str) -> None:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError(f"{label} must be a lowercase SHA-256 hex digest")


def _ground_freshness_limit(provider_id: str) -> float | None:
    return dict(GROUND_FRESHNESS_LIMITS_SECONDS).get(provider_id)


def _require_finite(value: object, label: str) -> None:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
    ):
        raise ValueError(f"{label} must be a finite number")


def _require_nonnegative_finite(value: object, label: str) -> None:
    _require_finite(value, label)
    if float(value) < 0.0:
        raise ValueError(f"{label} cannot be negative")


def _optional_nonnegative_finite(value: object) -> float | None:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or value < 0.0
    ):
        return None
    return float(value)


def _optional_nonnegative_int(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value


def _optional_nonempty_string(value: object) -> str | None:
    return (
        value
        if (
            isinstance(value, str)
            and value
            and len(value) <= MAX_IDENTIFIER_LENGTH
        )
        else None
    )
