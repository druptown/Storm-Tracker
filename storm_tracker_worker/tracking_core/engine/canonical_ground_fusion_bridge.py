"""Fail-closed bridge from canonical ground facts to shadow ground fusion.

The canonical ground store deliberately persists provider facts without making
weather claims.  This module is the separate, derived layer that may turn a
raw-reverified :class:`GroundObservationReceipt` into a ``GroundObservation``
only when all of the following are exact:

* provider, product, decoder version and decoder configuration;
* an immutable semantics certificate backed by primary sources;
* a verified provider alias in the physical-site registry;
* the provider-specific variable, unit, temporal support and QC contract.

No proximity-based site identity is created here.  Unsupported and ambiguous
facts are retained as explicit issues and never become wet or dry evidence.
The module is shadow-only and has no live service or Home Assistant wiring.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from enum import Enum
import hashlib
import json
import math
import re
import time
from typing import Callable

from ...canonical_ground_evidence_store import (
    GROUND_EVIDENCE_RECEIPT_SCHEMA,
    CanonicalGroundEvidenceStore,
    GroundObservationFact,
    GroundObservationReceipt,
    ground_fact_id,
    ground_fact_sha256,
    ground_observation_key,
    parse_ground_observation,
)
from .ground_fusion import (
    GroundObservation,
    MeasurementSemantics,
    PhysicalSite,
    PhysicalSiteRegistry,
    PrecipitationState,
    RawGroundProvenance,
    StationAlias,
)


GROUND_BRIDGE_ALGORITHM = "canonical-ground-fusion-bridge-shadow-v1"
GROUND_BRIDGE_ADMISSION_STATUS = "candidate_only_unadmitted"
HARD_MAX_BRIDGE_FACTS = 1_000
HARD_MAX_SEMANTICS_CERTIFICATES = 128
HARD_MAX_SITE_REGISTRY_SITES = 250_000
HARD_MAX_SITE_REGISTRY_ALIASES = 500_000
DEFAULT_BRIDGE_SECONDS = 5.0

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SYNOPTIC_INTERVAL_RE = re.compile(
    r"^synoptic\.native\.(?P<sensor_set>precip_intervals_set_[A-Za-z0-9_]+)"
    r"\.between_consecutive_reports$"
)
_METAR_6RRRR_RE = re.compile(r"^6(?P<hundredths>[0-9]{4})$")
_METAR_DESCRIPTORS = frozenset({"MI", "PR", "BC", "DR", "BL", "SH", "TS", "FZ"})
_METAR_PHENOMENA = frozenset({
    "DZ", "RA", "SN", "SG", "IC", "PL", "GR", "GS", "UP",
    "BR", "FG", "FU", "VA", "DU", "SA", "HZ", "PY",
    "PO", "SQ", "FC", "SS", "DS",
})
_METAR_PRECIPITATION = frozenset({
    "DZ", "RA", "SN", "SG", "IC", "PL", "GR", "GS", "UP",
})
_METAR_WITHHOLD_FLAGS = frozenset({"PWINO", "PNO", "FZRANO", "TSNO", "$"})


class GroundSemanticsProfile(str, Enum):
    """Exact interpretation implemented by one reviewed certificate."""

    METAR_PRESENT_WEATHER_V1 = "metar_present_weather_v1"
    METAR_INTERVAL_ACCUMULATION_V1 = "metar_interval_accumulation_v1"
    SYNOPTIC_INTERVAL_ACCUMULATION_V1 = (
        "synoptic_interval_accumulation_v1"
    )


class GroundBridgeIssueState(str, Enum):
    """Why a canonical fact did not become candidate fusion evidence."""

    WITHHELD = "withheld"
    INVALID = "invalid"
    DUPLICATE_INPUT = "duplicate_input"


def _identifier(value: str, name: str, maximum: int = 256) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
    result = value.strip()
    if len(result) > maximum or "\0" in result:
        raise ValueError(f"{name} is outside its identifier bound")
    return result


def _sha256(value: str, name: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f"{name} must be a lowercase SHA-256")
    return value


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=True,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")


@dataclass(frozen=True, slots=True)
class GroundSemanticsCertificate:
    """Exact reviewed source/decoder binding; no wildcard fields exist."""

    certificate_id: str
    certificate_version: str
    provider: str
    product: str
    decoder_identity: str
    decoder_version: str
    config_sha256: str
    profile: GroundSemanticsProfile
    primary_sources: tuple[str, ...]
    review_evidence_sha256: str

    def __post_init__(self) -> None:
        for name in (
            "certificate_id",
            "certificate_version",
            "provider",
            "product",
            "decoder_identity",
            "decoder_version",
        ):
            object.__setattr__(self, name, _identifier(getattr(self, name), name))
        object.__setattr__(
            self,
            "config_sha256",
            _sha256(self.config_sha256, "config_sha256"),
        )
        object.__setattr__(
            self,
            "review_evidence_sha256",
            _sha256(self.review_evidence_sha256, "review_evidence_sha256"),
        )
        if not isinstance(self.profile, GroundSemanticsProfile):
            raise TypeError("profile must be GroundSemanticsProfile")
        if (
            not isinstance(self.primary_sources, tuple)
            or not self.primary_sources
            or any(
                not isinstance(item, str)
                or not item.startswith("https://")
                or len(item) > 2048
                for item in self.primary_sources
            )
        ):
            raise ValueError("primary_sources must be a non-empty HTTPS tuple")
        if len(set(self.primary_sources)) != len(self.primary_sources):
            raise ValueError("primary_sources contains duplicates")

    @property
    def fingerprint(self) -> str:
        payload = {
            "algorithm": GROUND_BRIDGE_ALGORITHM,
            "certificate_id": self.certificate_id,
            "certificate_version": self.certificate_version,
            "provider": self.provider,
            "product": self.product,
            "decoder_identity": self.decoder_identity,
            "decoder_version": self.decoder_version,
            "config_sha256": self.config_sha256,
            "profile": self.profile.value,
            "primary_sources": list(self.primary_sources),
            "review_evidence_sha256": self.review_evidence_sha256,
        }
        return hashlib.sha256(_canonical_json(payload)).hexdigest()

    def matches(self, fact: GroundObservationFact) -> bool:
        return (
            self.provider == fact.provider
            and self.product == fact.product
            and self.decoder_identity == fact.decoder_identity
            and self.decoder_version == fact.decoder_version
            and self.config_sha256 == fact.config_sha256
        )


@dataclass(frozen=True, slots=True)
class GroundBridgeIssue:
    source_index: int
    fact_id: str
    fact_sha256: str
    receipt_sha256: str
    provider: str
    provider_station_id: str
    state: GroundBridgeIssueState
    code: str
    detail: str


@dataclass(frozen=True, slots=True)
class AdaptedCanonicalGroundFact:
    source_index: int
    fact_id: str
    fact_sha256: str
    receipt_sha256: str
    certificate_fingerprint: str
    physical_site_id: str
    mapping_fingerprint: str
    observation: GroundObservation


@dataclass(frozen=True, slots=True)
class CanonicalGroundBridgeBatch:
    source_fact_count: int
    facts: tuple[AdaptedCanonicalGroundFact, ...]
    issues: tuple[GroundBridgeIssue, ...]
    registry_fingerprint: str

    def __post_init__(self) -> None:
        if (
            isinstance(self.source_fact_count, bool)
            or not isinstance(self.source_fact_count, int)
            or not 0 <= self.source_fact_count <= HARD_MAX_BRIDGE_FACTS
        ):
            raise ValueError("source_fact_count is outside the bridge bound")
        dispositions = tuple(item.source_index for item in self.facts) + tuple(
            item.source_index for item in self.issues
        )
        if tuple(sorted(dispositions)) != tuple(range(self.source_fact_count)):
            raise ValueError("every source fact must have exactly one disposition")

    @property
    def observations(self) -> tuple[GroundObservation, ...]:
        return tuple(item.observation for item in self.facts)

    @property
    def operational_admission_eligible(self) -> bool:
        return False

    @property
    def admission_status(self) -> str:
        return GROUND_BRIDGE_ADMISSION_STATUS

    @property
    def fingerprint(self) -> str:
        payload = {
            "algorithm": GROUND_BRIDGE_ALGORITHM,
            "source_fact_count": self.source_fact_count,
            "registry_fingerprint": self.registry_fingerprint,
            "facts": [
                {
                    "source_index": item.source_index,
                    "fact_id": item.fact_id,
                    "fact_sha256": item.fact_sha256,
                    "receipt_sha256": item.receipt_sha256,
                    "certificate_fingerprint": item.certificate_fingerprint,
                    "physical_site_id": item.physical_site_id,
                    "mapping_fingerprint": item.mapping_fingerprint,
                    "observation_id": item.observation.observation_id,
                }
                for item in self.facts
            ],
            "issues": [
                {
                    "source_index": item.source_index,
                    "fact_id": item.fact_id,
                    "fact_sha256": item.fact_sha256,
                    "receipt_sha256": item.receipt_sha256,
                    "provider": item.provider,
                    "provider_station_id": item.provider_station_id,
                    "state": item.state.value,
                    "code": item.code,
                    "detail": item.detail,
                }
                for item in self.issues
            ],
            "operational_admission_eligible": False,
        }
        return hashlib.sha256(_canonical_json(payload)).hexdigest()


class GroundBridgeCapacityError(RuntimeError):
    pass


class GroundBridgeDeadlineError(RuntimeError):
    pass


class _Withheld(RuntimeError):
    def __init__(self, code: str, detail: str) -> None:
        super().__init__(detail)
        self.code = code
        self.detail = detail


def _deadline(
    deadline_at: float | None,
    monotonic: Callable[[], float],
) -> float:
    now = _monotonic_now(monotonic)
    if deadline_at is not None and (
        isinstance(deadline_at, bool)
        or not isinstance(deadline_at, (int, float))
    ):
        raise ValueError("deadline_at must be a finite number")
    result = (
        now + DEFAULT_BRIDGE_SECONDS
        if deadline_at is None
        else float(deadline_at)
    )
    if not math.isfinite(result):
        raise ValueError("deadline_at must be finite")
    return result


def _monotonic_now(monotonic: Callable[[], float]) -> float:
    value = float(monotonic())
    if not math.isfinite(value):
        raise ValueError("monotonic clock must return a finite number")
    return value


def _check_deadline(deadline_at: float, monotonic: Callable[[], float]) -> None:
    if _monotonic_now(monotonic) >= deadline_at:
        raise GroundBridgeDeadlineError("canonical ground bridge deadline elapsed")


def _qc_values(fact: GroundObservationFact, flag: str) -> tuple[str | None, ...]:
    return tuple(item.value for item in fact.provider_qc if item.flag == flag)


def _single_qc(fact: GroundObservationFact, flag: str) -> str | None:
    values = _qc_values(fact, flag)
    if len(values) != 1:
        raise _Withheld(
            "provider_qc_not_unique",
            f"required provider QC flag is absent or non-unique: {flag}",
        )
    return values[0]


def _decimal(value: str | None, *, code: str) -> Decimal:
    if value is None:
        raise _Withheld(code, "the exact provider numeric lexeme is absent")
    try:
        result = Decimal(value)
    except InvalidOperation as exc:
        raise _Withheld(code, "the exact provider numeric lexeme is invalid") from exc
    if not result.is_finite() or result < 0:
        raise _Withheld(code, "the provider numeric value is negative or non-finite")
    try:
        converted = float(result)
    except (OverflowError, ValueError) as exc:
        raise _Withheld(code, "the provider numeric value exceeds the fusion bound") from exc
    if not math.isfinite(converted):
        raise _Withheld(code, "the provider numeric value exceeds the fusion bound")
    return result


def _expected_profile(fact: GroundObservationFact) -> GroundSemanticsProfile:
    variable = fact.measurement.variable
    if fact.provider == "netatmo":
        raise _Withheld(
            "netatmo_semantics_uncertified",
            "Netatmo API field, unit and temporal support remain uncertified",
        )
    if fact.provider == "metar" and variable == "present_weather":
        return GroundSemanticsProfile.METAR_PRESENT_WEATHER_V1
    if fact.provider == "metar" and variable == "precipitation_accumulation":
        return GroundSemanticsProfile.METAR_INTERVAL_ACCUMULATION_V1
    if fact.provider == "synoptic" and _SYNOPTIC_INTERVAL_RE.fullmatch(variable):
        return GroundSemanticsProfile.SYNOPTIC_INTERVAL_ACCUMULATION_V1
    raise _Withheld(
        "provider_variable_semantics_uncertified",
        "no implemented semantics profile exists for this exact provider variable",
    )


def _select_certificate(
    fact: GroundObservationFact,
    certificates: tuple[GroundSemanticsCertificate, ...],
) -> GroundSemanticsCertificate:
    profile = _expected_profile(fact)
    matches = tuple(
        item for item in certificates
        if item.profile is profile and item.matches(fact)
    )
    if not matches:
        raise _Withheld(
            "semantics_certificate_missing",
            "no exact provider/product/decoder/config semantics certificate matched",
        )
    if len(matches) != 1:
        raise _Withheld(
            "semantics_certificate_ambiguous",
            "multiple exact semantics certificates matched the same fact",
        )
    return matches[0]


def _validate_receipt(receipt: GroundObservationReceipt) -> GroundObservationFact:
    if not isinstance(receipt, GroundObservationReceipt):
        raise TypeError("bridge inputs must be GroundObservationReceipt objects")
    if not isinstance(receipt.fact, GroundObservationFact):
        raise TypeError("receipt.fact must be a GroundObservationFact")
    fact = parse_ground_observation(receipt.fact.to_mapping())
    if ground_fact_id(fact) != receipt.fact_id:
        raise _Withheld("receipt_fact_id_mismatch", "fact identity does not match its receipt")
    if ground_fact_sha256(fact) != receipt.fact_sha256:
        raise _Withheld("receipt_fact_sha256_mismatch", "fact digest does not match its receipt")
    if ground_observation_key(fact) != receipt.observation_key:
        raise _Withheld(
            "receipt_observation_key_mismatch",
            "observation identity does not match its receipt",
        )
    fact_sha256 = _sha256(receipt.fact_sha256, "receipt.fact_sha256")
    receipt_sha256 = _sha256(receipt.receipt_sha256, "receipt.receipt_sha256")
    capture_manifest_sha256 = _sha256(
        receipt.capture_manifest_sha256,
        "receipt.capture_manifest_sha256",
    )
    if (
        isinstance(receipt.committed_at, bool)
        or not isinstance(receipt.committed_at, (int, float))
        or not math.isfinite(float(receipt.committed_at))
    ):
        raise _Withheld(
            "receipt_committed_at_invalid",
            "receipt commit time is not finite",
        )
    committed_at = float(receipt.committed_at)
    if committed_at < fact.ingested_at:
        raise _Withheld(
            "receipt_commit_predates_ingest",
            "receipt commit time predates canonical fact ingestion",
        )
    expected_receipt_sha256 = hashlib.sha256(_canonical_json({
        "capture_manifest_sha256": capture_manifest_sha256,
        "capture_receipt_sha256": fact.capture_receipt_sha256,
        "committed_at": committed_at,
        "fact_id": receipt.fact_id,
        "fact_sha256": fact_sha256,
        "observation_key": receipt.observation_key,
        "schema": GROUND_EVIDENCE_RECEIPT_SCHEMA,
    })).hexdigest()
    if receipt_sha256 != expected_receipt_sha256:
        raise _Withheld(
            "receipt_sha256_mismatch",
            "canonical ground receipt digest is invalid",
        )
    if sum(item.role == fact.source_member_role for item in fact.members) != 1:
        raise _Withheld(
            "source_member_role_ambiguous",
            "the selected raw source member role is absent or non-unique",
        )
    return fact


def _station_coordinates(fact: GroundObservationFact) -> tuple[float, float]:
    station = fact.station
    if (
        station.position_state != "known"
        or station.latitude_decimal is None
        or station.longitude_decimal is None
    ):
        raise _Withheld(
            "station_position_unknown",
            "an exact provider station position is required for cell matching",
        )
    try:
        latitude = float(Decimal(station.latitude_decimal))
        longitude = float(Decimal(station.longitude_decimal))
    except (InvalidOperation, OverflowError, ValueError) as exc:
        raise _Withheld(
            "station_position_invalid",
            "the exact provider station position is invalid",
        ) from exc
    if (
        not math.isfinite(latitude)
        or not math.isfinite(longitude)
        or not -90.0 <= latitude <= 90.0
        or not -180.0 <= longitude <= 180.0
    ):
        raise _Withheld(
            "station_position_invalid",
            "the exact provider station position is outside Earth bounds",
        )
    return latitude, longitude


def _metar_time_and_sensor_qc(fact: GroundObservationFact) -> None:
    if _single_qc(fact, "decoder.time_state") != "current":
        raise _Withheld(
            "metar_report_not_current_at_capture",
            "the decoder did not classify the report as current at capture",
        )
    flags = {value for value in _qc_values(fact, "metar.provider_flag") if value}
    if flags & _METAR_WITHHOLD_FLAGS:
        raise _Withheld(
            "metar_sensor_or_maintenance_flag",
            "a provider sensor/maintenance flag prevents a local precipitation claim",
        )
    for flag in (
        "awc.csv.maintenance_indicator_on",
        "awc.csv.no_signal",
        "awc.csv.present_weather_sensor_off",
    ):
        values = _qc_values(fact, flag)
        if len(values) != 1:
            raise _Withheld(
                "provider_qc_not_unique",
                f"required provider QC flag is absent or non-unique: {flag}",
            )
        value = (values[0] or "").strip().upper()
        if value == "TRUE":
            raise _Withheld(
                "metar_sensor_or_maintenance_flag",
                "the provider reports unavailable or maintenance-affected sensing",
            )
        if value not in {"", "FALSE"}:
            raise _Withheld(
                "metar_sensor_status_ambiguous",
                "a required provider sensor-status lexeme is not certified",
            )


def _metar_phenomena(token: str) -> tuple[bool, tuple[str, ...]]:
    value = token
    if not value or len(value) > 32 or any(ord(item) < 33 or ord(item) > 126 for item in value):
        raise _Withheld("metar_weather_token_invalid", "present-weather token is invalid")
    if value[:1] in {"+", "-"}:
        value = value[1:]
        if value.startswith("VC"):
            raise _Withheld(
                "metar_weather_token_invalid",
                "intensity and vicinity cannot be combined in this decoder profile",
            )
    vicinity = value.startswith("VC")
    if vicinity:
        value = value[2:]
    descriptor = value[:2] if value[:2] in _METAR_DESCRIPTORS else None
    if descriptor is not None:
        value = value[2:]
    if not value:
        if vicinity and descriptor in {"SH", "TS"}:
            return vicinity, ()
        raise _Withheld("metar_weather_token_invalid", "present-weather grammar is incomplete")
    if len(value) % 2:
        raise _Withheld("metar_weather_token_invalid", "present-weather grammar is incomplete")
    phenomena = tuple(value[index:index + 2] for index in range(0, len(value), 2))
    if any(item not in _METAR_PHENOMENA for item in phenomena):
        raise _Withheld("metar_weather_token_invalid", "present-weather grammar is unknown")
    return vicinity, phenomena


def _interpret_metar_present_weather(
    fact: GroundObservationFact,
) -> tuple[PrecipitationState, MeasurementSemantics, float | None, str | None]:
    _metar_time_and_sensor_qc(fact)
    measurement = fact.measurement
    if (
        measurement.state != "observed"
        or measurement.representation != "text"
        or measurement.unit != "metar_code"
        or measurement.exact_value is None
    ):
        raise _Withheld(
            "metar_present_weather_not_observed",
            "missing/unknown present weather is neither wet nor dry evidence",
        )
    if fact.representative_interval_start != fact.representative_interval_end:
        raise _Withheld(
            "metar_present_weather_time_support_invalid",
            "present weather must retain point-time support",
        )
    if fact.observed_at != fact.representative_interval_end:
        raise _Withheld(
            "metar_present_weather_time_support_invalid",
            "present-weather observation time must equal its point support",
        )
    if _single_qc(fact, "metar.present_weather_token_profile") != "recognized":
        raise _Withheld(
            "metar_weather_token_uncertified",
            "the exact present-weather token was not recognized by the decoder profile",
        )
    if _single_qc(fact, "awc.present_weather_token") != measurement.exact_value:
        raise _Withheld(
            "metar_weather_token_binding_mismatch",
            "canonical value and exact provider weather-token QC do not match",
        )
    if _qc_values(fact, "metar.station_relation"):
        raise _Withheld(
            "metar_vicinity_not_at_station",
            "vicinity weather cannot confirm precipitation at the physical site",
        )
    vicinity, phenomena = _metar_phenomena(measurement.exact_value)
    if vicinity:
        raise _Withheld(
            "metar_vicinity_not_at_station",
            "vicinity weather cannot confirm precipitation at the physical site",
        )
    if not any(item in _METAR_PRECIPITATION for item in phenomena):
        raise _Withheld(
            "metar_non_precipitation_weather_not_dry",
            "a non-precipitation weather token is not a certified dry observation",
        )
    return (
        PrecipitationState.PRESENT,
        MeasurementSemantics.INSTANTANEOUS_STATE,
        None,
        None,
    )


def _interpret_metar_accumulation(
    fact: GroundObservationFact,
) -> tuple[PrecipitationState, MeasurementSemantics, float | None, str | None]:
    _metar_time_and_sensor_qc(fact)
    measurement = fact.measurement
    if (
        measurement.state != "observed"
        or measurement.representation != "decimal"
        or measurement.unit != "in"
    ):
        raise _Withheld(
            "metar_accumulation_not_quantitative",
            "only an exact observed inch accumulation is quantitatively supported",
        )
    if fact.representative_interval_end <= fact.representative_interval_start:
        raise _Withheld(
            "metar_accumulation_interval_invalid",
            "a positive exact provider interval is required",
        )
    if fact.observed_at != fact.representative_interval_end:
        raise _Withheld(
            "metar_accumulation_interval_invalid",
            "METAR accumulation observation time must equal the interval end",
        )
    hours_lexeme = _single_qc(fact, "metar.6rrrr_interval_hours")
    if hours_lexeme not in {"3", "6"}:
        raise _Withheld(
            "metar_accumulation_interval_qc_invalid",
            "6RRRR duration must be exactly three or six hours",
        )
    interval_seconds = fact.representative_interval_end - fact.representative_interval_start
    if interval_seconds != int(hours_lexeme) * 3600.0:
        raise _Withheld(
            "metar_accumulation_interval_qc_invalid",
            "provider duration QC does not match the retained interval",
        )
    token = _single_qc(fact, "metar.6rrrr_token")
    token_match = None if token is None else _METAR_6RRRR_RE.fullmatch(token)
    if token_match is None:
        raise _Withheld(
            "metar_accumulation_token_invalid",
            "the exact 6RRRR provider token is missing or invalid",
        )
    if _single_qc(fact, "awc.csv.accumulation_lexeme") != measurement.exact_value:
        raise _Withheld(
            "metar_accumulation_lexeme_mismatch",
            "decoded amount and exact AWC accumulation lexeme do not match",
        )
    if _single_qc(fact, "metar.6rrrr_zero_semantics") != "not_applicable":
        raise _Withheld(
            "metar_zero_origin_uncertified",
            "METAR zero/omission semantics cannot provide a dry vote here",
        )
    amount = _decimal(measurement.exact_value, code="metar_accumulation_invalid")
    if amount <= 0:
        raise _Withheld(
            "metar_zero_origin_uncertified",
            "METAR zero/omission semantics cannot provide a dry vote here",
        )
    token_amount = Decimal(int(token_match.group("hundredths"))) / Decimal(100)
    if amount != token_amount:
        raise _Withheld(
            "metar_accumulation_token_mismatch",
            "decoded amount does not equal the exact 6RRRR token",
        )
    return (
        PrecipitationState.PRESENT,
        MeasurementSemantics.INTERVAL_ACCUMULATION,
        float(amount),
        "in",
    )


def _interpret_synoptic_interval(
    fact: GroundObservationFact,
) -> tuple[PrecipitationState, MeasurementSemantics, float | None, str | None]:
    measurement = fact.measurement
    if (
        measurement.state != "observed"
        or measurement.representation != "decimal"
        or measurement.unit != "Millimeters"
    ):
        raise _Withheld(
            "synoptic_interval_not_quantitative",
            "only an exact observed Millimeters interval value is supported",
        )
    if fact.representative_interval_end <= fact.representative_interval_start:
        raise _Withheld(
            "synoptic_interval_support_missing",
            "the first or cumulative anchor is not a complete precipitation interval",
        )
    if fact.observed_at != fact.representative_interval_end:
        raise _Withheld(
            "synoptic_interval_support_mismatch",
            "Synoptic observation time must equal the consecutive interval end",
        )
    variable_match = _SYNOPTIC_INTERVAL_RE.fullmatch(measurement.variable)
    if variable_match is None or _single_qc(
        fact,
        "synoptic.sensor_set",
    ) != variable_match.group("sensor_set"):
        raise _Withheld(
            "synoptic_sensor_set_binding_mismatch",
            "canonical variable and exact provider sensorset QC do not match",
        )
    if (
        _single_qc(fact, "synoptic.decoder.temporal_semantics")
        != "between_consecutive_provider_observations"
    ):
        raise _Withheld(
            "synoptic_temporal_semantics_mismatch",
            "the exact fact is not a between-consecutive-observations interval",
        )
    if _single_qc(fact, "synoptic.provider.provisional") != "true":
        raise _Withheld(
            "synoptic_provider_status_missing",
            "the provider provisional-status disclosure is absent",
        )
    if _single_qc(fact, "synoptic.qc_flagged") != "false":
        raise _Withheld(
            "synoptic_provider_qc_not_clean",
            "flagged or ambiguous provider QC is retained but cannot vote",
        )
    amount = _decimal(measurement.exact_value, code="synoptic_interval_invalid")
    return (
        PrecipitationState.PRESENT if amount > 0 else PrecipitationState.ABSENT,
        MeasurementSemantics.INTERVAL_ACCUMULATION,
        float(amount),
        "mm",
    )


def _interpret(
    fact: GroundObservationFact,
    profile: GroundSemanticsProfile,
) -> tuple[PrecipitationState, MeasurementSemantics, float | None, str | None]:
    if profile is GroundSemanticsProfile.METAR_PRESENT_WEATHER_V1:
        return _interpret_metar_present_weather(fact)
    if profile is GroundSemanticsProfile.METAR_INTERVAL_ACCUMULATION_V1:
        return _interpret_metar_accumulation(fact)
    if profile is GroundSemanticsProfile.SYNOPTIC_INTERVAL_ACCUMULATION_V1:
        return _interpret_synoptic_interval(fact)
    raise _Withheld("semantics_profile_unsupported", "semantics profile is unsupported")


def _issue(
    source_index: int,
    receipt: GroundObservationReceipt,
    state: GroundBridgeIssueState,
    code: str,
    detail: str,
) -> GroundBridgeIssue:
    fact = receipt.fact
    return GroundBridgeIssue(
        source_index=source_index,
        fact_id=(
            receipt.fact_id
            if isinstance(receipt.fact_id, str)
            else "<invalid-fact-id>"
        ),
        fact_sha256=(
            receipt.fact_sha256
            if isinstance(receipt.fact_sha256, str)
            else "<invalid-fact-sha256>"
        ),
        receipt_sha256=(
            receipt.receipt_sha256
            if isinstance(receipt.receipt_sha256, str)
            else "<invalid-receipt-sha256>"
        ),
        provider=(
            fact.provider
            if isinstance(fact, GroundObservationFact)
            else "<invalid-provider>"
        ),
        provider_station_id=(
            fact.station.provider_station_id
            if isinstance(fact, GroundObservationFact)
            else "<invalid-station>"
        ),
        state=state,
        code=code,
        detail=detail,
    )


def _adapt_raw_reverified_ground_receipts(
    receipts: tuple[GroundObservationReceipt, ...],
    *,
    certificates: tuple[GroundSemanticsCertificate, ...],
    site_registry: PhysicalSiteRegistry,
    deadline_at: float | None = None,
    monotonic: Callable[[], float] = time.monotonic,
) -> CanonicalGroundBridgeBatch:
    """Internal pure transform for a page just returned by the canonical store.

    This function is deliberately private.  The only public entry point first
    invokes ``CanonicalGroundEvidenceStore.query()``, which re-verifies every
    fact against schema-3 raw authority.  Unit tests may call this pure core
    with correctly materialized receipts, but runtime code may not bypass the
    store boundary.
    """
    if not isinstance(receipts, tuple):
        raise TypeError("receipts must be the immutable tuple returned by the store")
    if len(receipts) > HARD_MAX_BRIDGE_FACTS:
        raise GroundBridgeCapacityError("canonical ground bridge fact bound exceeded")
    if not isinstance(site_registry, PhysicalSiteRegistry):
        raise TypeError("site_registry must be PhysicalSiteRegistry")
    if not isinstance(certificates, tuple):
        raise TypeError("certificates must be an immutable tuple")
    if any(not isinstance(item, GroundSemanticsCertificate) for item in certificates):
        raise TypeError("certificates must contain GroundSemanticsCertificate objects")
    if len(certificates) > HARD_MAX_SEMANTICS_CERTIFICATES:
        raise GroundBridgeCapacityError("ground semantics certificate bound exceeded")
    if len(site_registry.sites) > HARD_MAX_SITE_REGISTRY_SITES:
        raise GroundBridgeCapacityError("physical-site registry site bound exceeded")
    active_deadline = _deadline(deadline_at, monotonic)
    _check_deadline(active_deadline, monotonic)
    if not receipts:
        return CanonicalGroundBridgeBatch(
            source_fact_count=0,
            facts=(),
            issues=(),
            registry_fingerprint=site_registry.registry_fingerprint,
        )

    # Resolve exact aliases in one bounded registry pass.  Calling the
    # registry's linear resolver once per fact would otherwise permit
    # O(facts*sites) work on the NUC.  This projection still never creates an
    # identity from geographic proximity.
    wanted_aliases = {
        (
            receipt.fact.provider.casefold(),
            receipt.fact.station.provider_station_id.casefold(),
        )
        for receipt in receipts
        if isinstance(receipt, GroundObservationReceipt)
        and isinstance(receipt.fact, GroundObservationFact)
    }
    alias_projection: dict[
        tuple[str, str],
        tuple[PhysicalSite, StationAlias],
    ] = {}
    alias_count = 0
    for site_index, site in enumerate(site_registry.sites):
        if site_index % 256 == 0:
            _check_deadline(active_deadline, monotonic)
        for alias in site.aliases:
            alias_count += 1
            if alias_count % 256 == 0:
                _check_deadline(active_deadline, monotonic)
            if alias_count > HARD_MAX_SITE_REGISTRY_ALIASES:
                raise GroundBridgeCapacityError(
                    "physical-site registry alias bound exceeded"
                )
            if alias.key in wanted_aliases:
                alias_projection[alias.key] = (site, alias)
    _check_deadline(active_deadline, monotonic)

    adapted: list[AdaptedCanonicalGroundFact] = []
    issues: list[GroundBridgeIssue] = []
    seen_fact_ids: set[str] = set()
    for source_index, receipt in enumerate(receipts):
        _check_deadline(active_deadline, monotonic)
        if not isinstance(receipt, GroundObservationReceipt):
            raise TypeError("bridge inputs must be GroundObservationReceipt objects")
        try:
            fact = _validate_receipt(receipt)
            if receipt.fact_id in seen_fact_ids:
                issues.append(_issue(
                    source_index,
                    receipt,
                    GroundBridgeIssueState.DUPLICATE_INPUT,
                    "duplicate_fact_receipt",
                    "the same immutable fact receipt appeared more than once",
                ))
                continue
            seen_fact_ids.add(receipt.fact_id)
            certificate = _select_certificate(fact, certificates)
            if site_registry.catalog_fingerprint is None:
                raise _Withheld(
                    "physical_site_catalog_proof_missing",
                    "the physical-site registry has no immutable catalog fingerprint",
                )
            resolved = alias_projection.get(
                (
                    fact.provider.casefold(),
                    fact.station.provider_station_id.casefold(),
                )
            )
            if resolved is None:
                raise _Withheld(
                    "physical_site_alias_unverified",
                    "no exact provider/station alias exists in the physical-site registry",
                )
            physical_site, station_alias = resolved
            if station_alias.mapping_fingerprint is None:
                raise _Withheld(
                    "physical_site_mapping_proof_missing",
                    "the exact alias exists but has no mapping evidence fingerprint",
                )
            latitude, longitude = _station_coordinates(fact)
            state, semantics, amount, unit = _interpret(fact, certificate.profile)
            provenance = RawGroundProvenance(
                provider_record_id=receipt.fact_id,
                parser_version=f"{fact.decoder_identity}@{fact.decoder_version}",
                raw_artifact_receipt_id=next(
                    item.receipt_id
                    for item in fact.members
                    if item.role == fact.source_member_role
                ),
                raw_content_sha256=fact.source_member_content_sha256,
                raw_text=None,
            )
            observation = GroundObservation(
                observation_id=(
                    f"canonical:{receipt.fact_id}:{certificate.fingerprint[:16]}"
                ),
                provider_id=fact.provider,
                station_id=fact.station.provider_station_id,
                latitude=latitude,
                longitude=longitude,
                interval_start_at=fact.representative_interval_start,
                interval_end_at=fact.representative_interval_end,
                representative_at=fact.observed_at,
                received_at=fact.received_at,
                precipitation_state=state,
                measurement_semantics=semantics,
                provenance=provenance,
                amount=amount,
                measurement_unit=unit,
                quality_flags=(
                    f"canonical_fact_id:{receipt.fact_id}",
                    f"canonical_fact_sha256:{receipt.fact_sha256}",
                    f"canonical_receipt_sha256:{receipt.receipt_sha256}",
                    f"source_record_sha256:{fact.source_record_sha256}",
                    f"semantics_certificate:{certificate.fingerprint}",
                    f"physical_site_registry:{site_registry.registry_fingerprint}",
                    f"physical_site_mapping:{station_alias.mapping_fingerprint}",
                ),
            )
            adapted.append(AdaptedCanonicalGroundFact(
                source_index=source_index,
                fact_id=receipt.fact_id,
                fact_sha256=receipt.fact_sha256,
                receipt_sha256=receipt.receipt_sha256,
                certificate_fingerprint=certificate.fingerprint,
                physical_site_id=physical_site.physical_site_id,
                mapping_fingerprint=station_alias.mapping_fingerprint,
                observation=observation,
            ))
        except _Withheld as exc:
            issues.append(_issue(
                source_index,
                receipt,
                GroundBridgeIssueState.WITHHELD,
                exc.code,
                exc.detail,
            ))
        except (AttributeError, TypeError, ValueError) as exc:
            issues.append(_issue(
                source_index,
                receipt,
                GroundBridgeIssueState.INVALID,
                "canonical_fact_contract_invalid",
                f"{type(exc).__name__}: {str(exc)[:384]}",
            ))

    return CanonicalGroundBridgeBatch(
        source_fact_count=len(receipts),
        facts=tuple(adapted),
        issues=tuple(issues),
        registry_fingerprint=site_registry.registry_fingerprint,
    )


def query_and_adapt_canonical_ground(
    evidence_store: CanonicalGroundEvidenceStore,
    *,
    certificates: tuple[GroundSemanticsCertificate, ...],
    site_registry: PhysicalSiteRegistry,
    provider: str | None = None,
    provider_station_id: str | None = None,
    site_id: str | None = None,
    variable: str | None = None,
    observed_from: float | None = None,
    observed_through: float | None = None,
    limit: int = 100,
    deadline_at: float | None = None,
    monotonic: Callable[[], float] = time.monotonic,
) -> CanonicalGroundBridgeBatch:
    """Use the store's bounded raw-reverified query as the only read path."""
    if type(evidence_store) is not CanonicalGroundEvidenceStore:
        raise TypeError(
            "evidence_store must be the concrete CanonicalGroundEvidenceStore"
        )
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= HARD_MAX_BRIDGE_FACTS:
        raise GroundBridgeCapacityError("canonical ground query limit is outside bounds")
    active_deadline = _deadline(deadline_at, monotonic)
    _check_deadline(active_deadline, monotonic)
    receipts = evidence_store.query(
        provider=provider,
        provider_station_id=provider_station_id,
        site_id=site_id,
        variable=variable,
        observed_from=observed_from,
        observed_through=observed_through,
        limit=limit,
        deadline_at=active_deadline,
    )
    _check_deadline(active_deadline, monotonic)
    return _adapt_raw_reverified_ground_receipts(
        receipts,
        certificates=certificates,
        site_registry=site_registry,
        deadline_at=active_deadline,
        monotonic=monotonic,
    )


__all__ = [
    "AdaptedCanonicalGroundFact",
    "CanonicalGroundBridgeBatch",
    "GROUND_BRIDGE_ADMISSION_STATUS",
    "GROUND_BRIDGE_ALGORITHM",
    "GroundBridgeCapacityError",
    "GroundBridgeDeadlineError",
    "GroundBridgeIssue",
    "GroundBridgeIssueState",
    "GroundSemanticsCertificate",
    "GroundSemanticsProfile",
    "HARD_MAX_BRIDGE_FACTS",
    "HARD_MAX_SEMANTICS_CERTIFICATES",
    "HARD_MAX_SITE_REGISTRY_ALIASES",
    "HARD_MAX_SITE_REGISTRY_SITES",
    "query_and_adapt_canonical_ground",
]
