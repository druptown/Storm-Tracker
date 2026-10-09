"""Closed, immutable input cycle for every configured meteorological source.

This module is the fail-closed seam between acquisition/decoding and a future
multi-source fusion stage.  It performs no I/O, network access, meteorological
admission, prediction, or alerting.  A cycle is useful here only when every
catalog identity has exactly one explicit outcome and every target has an
explicit applicability decision for every identity.

The acquisition catalog currently contains 27 cadence identities: 26 are
dispatchable and ``radar.kmi.radqpe_native`` is externally blocked.  The
Blitzortung push ingress is a twenty-eighth identity.  All 28 remain visible in
every cycle; blocked or out-of-scope sources are never collapsed to unknown.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import hashlib
import json
import math
import re
from typing import Any, ClassVar, Iterable, Mapping

from .worker_acquisition_plan import acquisition_catalog_snapshot


CYCLE_CONTRACT = "stv3-multi-source-operational-cycle-v1"
TARGET_CONTRACT = "stv3-multi-source-cycle-target-v1"
CONTRIBUTION_CONTRACT = "stv3-multi-source-cycle-contribution-v1"
CATALOG_DESCRIPTOR_CONTRACT = "stv3-multi-source-catalog-descriptor-v1"

RADAR_INPUT_CONTRACT = "stv3-multi-source-radar-input-v1"
SATELLITE_INPUT_CONTRACT = "stv3-multi-source-satellite-input-v1"
LIGHTNING_INPUT_CONTRACT = "stv3-multi-source-lightning-input-v1"
GROUND_INPUT_CONTRACT = "stv3-multi-source-ground-input-v1"
MODEL_INPUT_CONTRACT = "stv3-multi-source-model-input-v1"
VALIDATION_INPUT_CONTRACT = "stv3-multi-source-validation-input-v1"

EXPECTED_ACQUISITION_CATALOG_SHA256 = (
    "e924e78c13ecdcb2083051214383e0d0da8fed7f917a24d8bb4856b354843184"
)

MAX_TARGETS = 64
MAX_EVIDENCE_REFERENCES_PER_SOURCE = 128
MAX_TOTAL_EVIDENCE_REFERENCES = 2048
MAX_EVIDENCE_BYTES_PER_SOURCE = 2 * 1024 * 1024 * 1024
MAX_SINGLE_EVIDENCE_BYTES = 512 * 1024 * 1024
MAX_CYCLE_BYTES = 4 * 1024 * 1024
MAX_CYCLE_DURATION_MS = 24 * 60 * 60 * 1000
MAX_LIST_VALUES = 64
MAX_TIMESTAMP_MS = 2**53 - 1

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SAFE_ID_RE = re.compile(r"^[a-z0-9][a-z0-9_.:@/-]{0,191}$")
_TOKEN_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,191}$")
_REASON_RE = re.compile(r"^[a-z][a-z0-9_]{0,127}$")
_COUNTRY_RE = re.compile(r"^[A-Z]{2}$")


class MultiSourceCycleError(ValueError):
    """A catalog, contribution, or complete cycle is not exact and canonical."""


class SourceKind(str, Enum):
    RADAR = "radar"
    SATELLITE = "satellite"
    LIGHTNING = "lightning"
    GROUND = "ground"
    MODEL = "model"
    VALIDATION = "validation"


class CycleStatus(str, Enum):
    CURRENT = "current"
    STALE = "stale"
    ERROR = "error"
    BLOCKED = "blocked"
    NOT_APPLICABLE = "not_applicable"


_EXPECTED_CADENCE_SOURCE_IDS = frozenset({
    "ground.kmi.aws_10min",
    "ground.metar.global",
    "ground.netatmo.region",
    "ground.synoptic.region",
    "lightning.eumetsat.li",
    "lightning.goes18.glm",
    "lightning.goes19.glm",
    "model.openmeteo.italiameteo_icon2i.region",
    "model.openmeteo.region",
    "radar.aemet.composite",
    "radar.dpc.sri",
    "radar.dwd.radolan_rv",
    "radar.kmi.appv4_visual",
    "radar.kmi.public_wcs_unknown_quantity",
    "radar.kmi.radqpe_native",
    "radar.knmi.rtcor",
    "radar.meteofrance.dpradar",
    "radar.metoffice.uk_rate",
    "radar.opera.dbzh",
    "radar.rainviewer.visual",
    "satellite.goes18.rrqpe",
    "satellite.goes19.rrqpe",
    "satellite.hsaf.h40b",
    "satellite.noaa.err",
    "validation.geosphere.location",
    "validation.italiameteo.dpc_sri_historical_catalog",
    "validation.meteolux.location",
})
_EXPECTED_EVENT_INGRESS_IDS = frozenset({
    "lightning.blitzortung.event_ingress",
})
EXPECTED_SOURCE_IDS = frozenset(
    _EXPECTED_CADENCE_SOURCE_IDS | _EXPECTED_EVENT_INGRESS_IDS
)
CADENCE_SOURCE_COUNT = len(_EXPECTED_CADENCE_SOURCE_IDS)
DISPATCHABLE_CADENCE_SOURCE_COUNT = 26
EVENT_INGRESS_SOURCE_COUNT = len(_EXPECTED_EVENT_INGRESS_IDS)
SOURCE_COUNT = len(EXPECTED_SOURCE_IDS)
MAX_APPLICABILITY_ROWS = SOURCE_COUNT * MAX_TARGETS


def _canonical_json(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
    except (TypeError, ValueError, UnicodeEncodeError) as err:
        raise MultiSourceCycleError("contract is geen begrensde canonieke JSON") from err


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value)).hexdigest()


def _exact_mapping(value: object, expected: set[str], label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or set(value) != expected:
        raise MultiSourceCycleError(f"{label} schema is niet exact")
    return value


def _safe_id(value: object, label: str) -> str:
    if (
        not isinstance(value, str)
        or _SAFE_ID_RE.fullmatch(value) is None
        or "://" in value
    ):
        raise MultiSourceCycleError(f"{label} is geen veilig begrensd ID")
    return value


def _reason(value: object, label: str = "reason_code") -> str:
    if not isinstance(value, str) or _REASON_RE.fullmatch(value) is None:
        raise MultiSourceCycleError(f"{label} is geen begrensde reason code")
    return value


def _bounded_token(value: object, label: str) -> str:
    if (
        not isinstance(value, str)
        or _TOKEN_RE.fullmatch(value) is None
        or "://" in value
    ):
        raise MultiSourceCycleError(f"{label} is geen begrensd token")
    return value


def _sha256(value: object, label: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise MultiSourceCycleError(f"{label} is geen lowercase SHA-256")
    return value


def _integer(
    value: object,
    label: str,
    *,
    minimum: int = 0,
    maximum: int = MAX_TIMESTAMP_MS,
) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not minimum <= value <= maximum
    ):
        raise MultiSourceCycleError(f"{label} ligt buiten de integergrens")
    return value


def _optional_timestamp_ms(value: object, label: str) -> int | None:
    if value is None:
        return None
    return _integer(value, label)


def _finite(value: object, label: str, minimum: float, maximum: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise MultiSourceCycleError(f"{label} is geen getal")
    result = float(value)
    if not math.isfinite(result) or not minimum <= result <= maximum:
        raise MultiSourceCycleError(f"{label} ligt buiten de grens")
    return result


def _bbox(value: object, label: str, *, allow_none: bool) -> tuple[float, ...] | None:
    if value is None and allow_none:
        return None
    if not isinstance(value, (tuple, list)) or len(value) != 4:
        raise MultiSourceCycleError(f"{label} moet [zuid,west,noord,oost] zijn")
    south = _finite(value[0], f"{label}.south", -90.0, 90.0)
    west = _finite(value[1], f"{label}.west", -180.0, 180.0)
    north = _finite(value[2], f"{label}.north", -90.0, 90.0)
    east = _finite(value[3], f"{label}.east", -180.0, 180.0)
    if south > north or west > east:
        raise MultiSourceCycleError(f"{label} is omgekeerd of kruist de datumgrens")
    return (south, west, north, east)


def _optional_id(value: object, label: str) -> str | None:
    return None if value is None else _safe_id(value, label)


def _optional_token(value: object, label: str) -> str | None:
    return None if value is None else _bounded_token(value, label)


def _canonical_ids(
    values: object,
    label: str,
    *,
    allow_empty: bool,
) -> tuple[str, ...]:
    if not isinstance(values, tuple) or len(values) > MAX_LIST_VALUES:
        raise MultiSourceCycleError(f"{label} moet een begrensde tuple zijn")
    result = tuple(_safe_id(item, label) for item in values)
    if result != tuple(sorted(set(result))):
        raise MultiSourceCycleError(f"{label} is niet uniek en gesorteerd")
    if not allow_empty and not result:
        raise MultiSourceCycleError(f"{label} mag niet leeg zijn")
    return result


@dataclass(frozen=True, slots=True)
class EvidenceReference:
    """Opaque content/receipt identity; provider bytes never enter this contract."""

    role: str
    content_sha256: str
    receipt_sha256: str
    size_bytes: int
    schema_id: str

    def __post_init__(self) -> None:
        _safe_id(self.role, "evidence.role")
        _sha256(self.content_sha256, "evidence.content_sha256")
        _sha256(self.receipt_sha256, "evidence.receipt_sha256")
        _integer(
            self.size_bytes,
            "evidence.size_bytes",
            minimum=1,
            maximum=MAX_SINGLE_EVIDENCE_BYTES,
        )
        _safe_id(self.schema_id, "evidence.schema_id")

    def to_mapping(self) -> dict[str, Any]:
        return {
            "content_sha256": self.content_sha256,
            "receipt_sha256": self.receipt_sha256,
            "role": self.role,
            "schema_id": self.schema_id,
            "size_bytes": self.size_bytes,
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "EvidenceReference":
        raw = _exact_mapping(
            value,
            {"content_sha256", "receipt_sha256", "role", "schema_id", "size_bytes"},
            "evidence",
        )
        return cls(
            role=raw["role"],
            content_sha256=raw["content_sha256"],
            receipt_sha256=raw["receipt_sha256"],
            size_bytes=raw["size_bytes"],
            schema_id=raw["schema_id"],
        )


def _evidence(values: object) -> tuple[EvidenceReference, ...]:
    if not isinstance(values, tuple):
        raise MultiSourceCycleError("evidence moet een tuple zijn")
    if len(values) > MAX_EVIDENCE_REFERENCES_PER_SOURCE:
        raise MultiSourceCycleError("evidence overschrijdt de bronlimiet")
    if any(not isinstance(item, EvidenceReference) for item in values):
        raise MultiSourceCycleError("evidence bevat een fout type")
    keys = tuple((item.role, item.content_sha256) for item in values)
    if keys != tuple(sorted(keys)) or len({item.role for item in values}) != len(values):
        raise MultiSourceCycleError("evidence is niet canoniek of bevat dubbele rollen")
    if sum(item.size_bytes for item in values) > MAX_EVIDENCE_BYTES_PER_SOURCE:
        raise MultiSourceCycleError("evidence overschrijdt de byteverwijzingslimiet")
    return values


def _parse_evidence(value: object) -> tuple[EvidenceReference, ...]:
    if not isinstance(value, list):
        raise MultiSourceCycleError("payload.evidence moet een lijst zijn")
    return tuple(EvidenceReference.from_mapping(item) for item in value)


@dataclass(frozen=True, slots=True)
class RadarInput:
    observed_at_ms: int | None
    coverage_bbox: tuple[float, ...] | None
    quantity_id: str | None
    unit_id: str | None
    sample_count: int
    valid_sample_count: int
    evidence: tuple[EvidenceReference, ...]
    contract: ClassVar[str] = RADAR_INPUT_CONTRACT

    def __post_init__(self) -> None:
        _optional_timestamp_ms(self.observed_at_ms, "radar.observed_at_ms")
        _bbox(self.coverage_bbox, "radar.coverage_bbox", allow_none=True)
        _optional_token(self.quantity_id, "radar.quantity_id")
        _optional_token(self.unit_id, "radar.unit_id")
        _integer(self.sample_count, "radar.sample_count", maximum=2**31 - 1)
        _integer(
            self.valid_sample_count,
            "radar.valid_sample_count",
            maximum=2**31 - 1,
        )
        if self.valid_sample_count > self.sample_count:
            raise MultiSourceCycleError("radar valid_sample_count overschrijdt sample_count")
        _evidence(self.evidence)
        supplied = self.has_data
        complete = all((
            self.observed_at_ms is not None,
            self.coverage_bbox is not None,
            self.quantity_id is not None,
            self.unit_id is not None,
            self.sample_count > 0,
            self.valid_sample_count > 0,
        ))
        if supplied != complete:
            raise MultiSourceCycleError("radarinput is gedeeltelijk ingevuld")
        if not supplied and any((
            self.observed_at_ms is not None,
            self.coverage_bbox is not None,
            self.quantity_id is not None,
            self.unit_id is not None,
            self.sample_count,
            self.valid_sample_count,
        )):
            raise MultiSourceCycleError("lege radarinput bevat toch datavelden")

    @property
    def has_data(self) -> bool:
        return bool(self.evidence)

    def to_mapping(self) -> dict[str, Any]:
        return {
            "contract": self.contract,
            "coverage_bbox": list(self.coverage_bbox) if self.coverage_bbox else None,
            "evidence": [item.to_mapping() for item in self.evidence],
            "observed_at_ms": self.observed_at_ms,
            "quantity_id": self.quantity_id,
            "sample_count": self.sample_count,
            "unit_id": self.unit_id,
            "valid_sample_count": self.valid_sample_count,
        }


@dataclass(frozen=True, slots=True)
class SatelliteInput:
    observed_at_ms: int | None
    coverage_bbox: tuple[float, ...] | None
    platform_id: str | None
    quantity_id: str | None
    unit_id: str | None
    sample_count: int
    valid_sample_count: int
    evidence: tuple[EvidenceReference, ...]
    contract: ClassVar[str] = SATELLITE_INPUT_CONTRACT

    def __post_init__(self) -> None:
        _optional_timestamp_ms(self.observed_at_ms, "satellite.observed_at_ms")
        _bbox(self.coverage_bbox, "satellite.coverage_bbox", allow_none=True)
        _optional_token(self.platform_id, "satellite.platform_id")
        _optional_token(self.quantity_id, "satellite.quantity_id")
        _optional_token(self.unit_id, "satellite.unit_id")
        _integer(self.sample_count, "satellite.sample_count", maximum=2**31 - 1)
        _integer(
            self.valid_sample_count,
            "satellite.valid_sample_count",
            maximum=2**31 - 1,
        )
        if self.valid_sample_count > self.sample_count:
            raise MultiSourceCycleError("satellite valid_sample_count is te groot")
        _evidence(self.evidence)
        supplied = self.has_data
        complete = all((
            self.observed_at_ms is not None,
            self.coverage_bbox is not None,
            self.platform_id is not None,
            self.quantity_id is not None,
            self.unit_id is not None,
            self.sample_count > 0,
            self.valid_sample_count > 0,
        ))
        if supplied != complete:
            raise MultiSourceCycleError("satellietinput is gedeeltelijk ingevuld")
        if not supplied and any((
            self.observed_at_ms is not None,
            self.coverage_bbox is not None,
            self.platform_id is not None,
            self.quantity_id is not None,
            self.unit_id is not None,
            self.sample_count,
            self.valid_sample_count,
        )):
            raise MultiSourceCycleError("lege satellietinput bevat toch datavelden")

    @property
    def has_data(self) -> bool:
        return bool(self.evidence)

    def to_mapping(self) -> dict[str, Any]:
        return {
            "contract": self.contract,
            "coverage_bbox": list(self.coverage_bbox) if self.coverage_bbox else None,
            "evidence": [item.to_mapping() for item in self.evidence],
            "observed_at_ms": self.observed_at_ms,
            "platform_id": self.platform_id,
            "quantity_id": self.quantity_id,
            "sample_count": self.sample_count,
            "unit_id": self.unit_id,
            "valid_sample_count": self.valid_sample_count,
        }


@dataclass(frozen=True, slots=True)
class LightningInput:
    window_start_ms: int | None
    window_end_ms: int | None
    event_count: int
    gap_count: int
    absence_claimed: bool
    evidence: tuple[EvidenceReference, ...]
    contract: ClassVar[str] = LIGHTNING_INPUT_CONTRACT

    def __post_init__(self) -> None:
        start = _optional_timestamp_ms(self.window_start_ms, "lightning.window_start_ms")
        end = _optional_timestamp_ms(self.window_end_ms, "lightning.window_end_ms")
        _integer(self.event_count, "lightning.event_count", maximum=2**31 - 1)
        _integer(self.gap_count, "lightning.gap_count", maximum=2**31 - 1)
        if self.absence_claimed is not False:
            raise MultiSourceCycleError("lightninginput mag nooit afwezigheid claimen")
        _evidence(self.evidence)
        supplied = self.has_data
        complete = start is not None and end is not None and start <= end
        if supplied != complete:
            raise MultiSourceCycleError("lightninginput is gedeeltelijk ingevuld")
        if not supplied and any((
            start is not None,
            end is not None,
            self.event_count,
            self.gap_count,
        )):
            raise MultiSourceCycleError("lege lightninginput bevat toch datavelden")

    @property
    def has_data(self) -> bool:
        return bool(self.evidence)

    def to_mapping(self) -> dict[str, Any]:
        return {
            "absence_claimed": self.absence_claimed,
            "contract": self.contract,
            "event_count": self.event_count,
            "evidence": [item.to_mapping() for item in self.evidence],
            "gap_count": self.gap_count,
            "window_end_ms": self.window_end_ms,
            "window_start_ms": self.window_start_ms,
        }


@dataclass(frozen=True, slots=True)
class GroundInput:
    window_start_ms: int | None
    window_end_ms: int | None
    station_count: int
    observation_count: int
    quantity_ids: tuple[str, ...]
    evidence: tuple[EvidenceReference, ...]
    contract: ClassVar[str] = GROUND_INPUT_CONTRACT

    def __post_init__(self) -> None:
        start = _optional_timestamp_ms(self.window_start_ms, "ground.window_start_ms")
        end = _optional_timestamp_ms(self.window_end_ms, "ground.window_end_ms")
        _integer(self.station_count, "ground.station_count", maximum=2**31 - 1)
        _integer(
            self.observation_count,
            "ground.observation_count",
            maximum=2**31 - 1,
        )
        _canonical_ids(self.quantity_ids, "ground.quantity_ids", allow_empty=True)
        _evidence(self.evidence)
        supplied = self.has_data
        complete = all((
            start is not None,
            end is not None,
            start <= end if start is not None and end is not None else False,
            self.station_count > 0,
            self.observation_count > 0,
            self.quantity_ids,
        ))
        if supplied != bool(complete):
            raise MultiSourceCycleError("groundinput is gedeeltelijk ingevuld")
        if not supplied and any((
            start is not None,
            end is not None,
            self.station_count,
            self.observation_count,
            self.quantity_ids,
        )):
            raise MultiSourceCycleError("lege groundinput bevat toch datavelden")

    @property
    def has_data(self) -> bool:
        return bool(self.evidence)

    def to_mapping(self) -> dict[str, Any]:
        return {
            "contract": self.contract,
            "evidence": [item.to_mapping() for item in self.evidence],
            "observation_count": self.observation_count,
            "quantity_ids": list(self.quantity_ids),
            "station_count": self.station_count,
            "window_end_ms": self.window_end_ms,
            "window_start_ms": self.window_start_ms,
        }


@dataclass(frozen=True, slots=True)
class ModelInput:
    run_at_ms: int | None
    valid_from_ms: int | None
    valid_through_ms: int | None
    lead_time_count: int
    variable_ids: tuple[str, ...]
    evidence: tuple[EvidenceReference, ...]
    contract: ClassVar[str] = MODEL_INPUT_CONTRACT

    def __post_init__(self) -> None:
        run_at = _optional_timestamp_ms(self.run_at_ms, "model.run_at_ms")
        valid_from = _optional_timestamp_ms(self.valid_from_ms, "model.valid_from_ms")
        valid_through = _optional_timestamp_ms(
            self.valid_through_ms,
            "model.valid_through_ms",
        )
        _integer(self.lead_time_count, "model.lead_time_count", maximum=10000)
        _canonical_ids(self.variable_ids, "model.variable_ids", allow_empty=True)
        _evidence(self.evidence)
        supplied = self.has_data
        complete = all((
            run_at is not None,
            valid_from is not None,
            valid_through is not None,
            run_at <= valid_from <= valid_through
            if run_at is not None and valid_from is not None and valid_through is not None
            else False,
            self.lead_time_count > 0,
            self.variable_ids,
        ))
        if supplied != bool(complete):
            raise MultiSourceCycleError("modelinput is gedeeltelijk ingevuld")
        if not supplied and any((
            run_at is not None,
            valid_from is not None,
            valid_through is not None,
            self.lead_time_count,
            self.variable_ids,
        )):
            raise MultiSourceCycleError("lege modelinput bevat toch datavelden")

    @property
    def has_data(self) -> bool:
        return bool(self.evidence)

    def to_mapping(self) -> dict[str, Any]:
        return {
            "contract": self.contract,
            "evidence": [item.to_mapping() for item in self.evidence],
            "lead_time_count": self.lead_time_count,
            "run_at_ms": self.run_at_ms,
            "valid_from_ms": self.valid_from_ms,
            "valid_through_ms": self.valid_through_ms,
            "variable_ids": list(self.variable_ids),
        }


@dataclass(frozen=True, slots=True)
class ValidationInput:
    reference_at_ms: int | None
    record_count: int
    validation_role: str | None
    evidence: tuple[EvidenceReference, ...]
    contract: ClassVar[str] = VALIDATION_INPUT_CONTRACT

    def __post_init__(self) -> None:
        _optional_timestamp_ms(self.reference_at_ms, "validation.reference_at_ms")
        _integer(self.record_count, "validation.record_count", maximum=2**31 - 1)
        _optional_id(self.validation_role, "validation.validation_role")
        _evidence(self.evidence)
        supplied = self.has_data
        complete = all((
            self.reference_at_ms is not None,
            self.record_count > 0,
            self.validation_role is not None,
        ))
        if supplied != complete:
            raise MultiSourceCycleError("validationinput is gedeeltelijk ingevuld")
        if not supplied and any((
            self.reference_at_ms is not None,
            self.record_count,
            self.validation_role is not None,
        )):
            raise MultiSourceCycleError("lege validationinput bevat toch datavelden")

    @property
    def has_data(self) -> bool:
        return bool(self.evidence)

    def to_mapping(self) -> dict[str, Any]:
        return {
            "contract": self.contract,
            "evidence": [item.to_mapping() for item in self.evidence],
            "record_count": self.record_count,
            "reference_at_ms": self.reference_at_ms,
            "validation_role": self.validation_role,
        }


SourceInput = (
    RadarInput
    | SatelliteInput
    | LightningInput
    | GroundInput
    | ModelInput
    | ValidationInput
)

_PAYLOAD_TYPE = {
    SourceKind.RADAR: RadarInput,
    SourceKind.SATELLITE: SatelliteInput,
    SourceKind.LIGHTNING: LightningInput,
    SourceKind.GROUND: GroundInput,
    SourceKind.MODEL: ModelInput,
    SourceKind.VALIDATION: ValidationInput,
}


def _parse_payload(kind: SourceKind, value: object) -> SourceInput:
    if not isinstance(value, Mapping):
        raise MultiSourceCycleError("contribution.payload is geen object")
    contract = value.get("contract")
    common_evidence = _parse_evidence(value.get("evidence"))
    if kind is SourceKind.RADAR:
        raw = _exact_mapping(value, {
            "contract", "coverage_bbox", "evidence", "observed_at_ms",
            "quantity_id", "sample_count", "unit_id", "valid_sample_count",
        }, "radar payload")
        if contract != RADAR_INPUT_CONTRACT:
            raise MultiSourceCycleError("radar payloadcontract is onbekend")
        return RadarInput(
            observed_at_ms=raw["observed_at_ms"],
            coverage_bbox=_bbox(raw["coverage_bbox"], "radar.coverage_bbox", allow_none=True),
            quantity_id=raw["quantity_id"],
            unit_id=raw["unit_id"],
            sample_count=raw["sample_count"],
            valid_sample_count=raw["valid_sample_count"],
            evidence=common_evidence,
        )
    if kind is SourceKind.SATELLITE:
        raw = _exact_mapping(value, {
            "contract", "coverage_bbox", "evidence", "observed_at_ms", "platform_id",
            "quantity_id", "sample_count", "unit_id", "valid_sample_count",
        }, "satellite payload")
        if contract != SATELLITE_INPUT_CONTRACT:
            raise MultiSourceCycleError("satellite payloadcontract is onbekend")
        return SatelliteInput(
            observed_at_ms=raw["observed_at_ms"],
            coverage_bbox=_bbox(
                raw["coverage_bbox"],
                "satellite.coverage_bbox",
                allow_none=True,
            ),
            platform_id=raw["platform_id"],
            quantity_id=raw["quantity_id"],
            unit_id=raw["unit_id"],
            sample_count=raw["sample_count"],
            valid_sample_count=raw["valid_sample_count"],
            evidence=common_evidence,
        )
    if kind is SourceKind.LIGHTNING:
        raw = _exact_mapping(value, {
            "absence_claimed", "contract", "event_count", "evidence", "gap_count",
            "window_end_ms", "window_start_ms",
        }, "lightning payload")
        if contract != LIGHTNING_INPUT_CONTRACT:
            raise MultiSourceCycleError("lightning payloadcontract is onbekend")
        return LightningInput(
            window_start_ms=raw["window_start_ms"],
            window_end_ms=raw["window_end_ms"],
            event_count=raw["event_count"],
            gap_count=raw["gap_count"],
            absence_claimed=raw["absence_claimed"],
            evidence=common_evidence,
        )
    if kind is SourceKind.GROUND:
        raw = _exact_mapping(value, {
            "contract", "evidence", "observation_count", "quantity_ids",
            "station_count", "window_end_ms", "window_start_ms",
        }, "ground payload")
        if contract != GROUND_INPUT_CONTRACT or not isinstance(raw["quantity_ids"], list):
            raise MultiSourceCycleError("ground payloadcontract of quantities is ongeldig")
        return GroundInput(
            window_start_ms=raw["window_start_ms"],
            window_end_ms=raw["window_end_ms"],
            station_count=raw["station_count"],
            observation_count=raw["observation_count"],
            quantity_ids=tuple(raw["quantity_ids"]),
            evidence=common_evidence,
        )
    if kind is SourceKind.MODEL:
        raw = _exact_mapping(value, {
            "contract", "evidence", "lead_time_count", "run_at_ms", "valid_from_ms",
            "valid_through_ms", "variable_ids",
        }, "model payload")
        if contract != MODEL_INPUT_CONTRACT or not isinstance(raw["variable_ids"], list):
            raise MultiSourceCycleError("model payloadcontract of variables is ongeldig")
        return ModelInput(
            run_at_ms=raw["run_at_ms"],
            valid_from_ms=raw["valid_from_ms"],
            valid_through_ms=raw["valid_through_ms"],
            lead_time_count=raw["lead_time_count"],
            variable_ids=tuple(raw["variable_ids"]),
            evidence=common_evidence,
        )
    raw = _exact_mapping(value, {
        "contract", "evidence", "record_count", "reference_at_ms", "validation_role",
    }, "validation payload")
    if contract != VALIDATION_INPUT_CONTRACT:
        raise MultiSourceCycleError("validation payloadcontract is onbekend")
    return ValidationInput(
        reference_at_ms=raw["reference_at_ms"],
        record_count=raw["record_count"],
        validation_role=raw["validation_role"],
        evidence=common_evidence,
    )


def empty_source_input(kind: SourceKind) -> SourceInput:
    """Return the exact type-specific empty envelope for a non-data status."""

    if kind is SourceKind.RADAR:
        return RadarInput(None, None, None, None, 0, 0, ())
    if kind is SourceKind.SATELLITE:
        return SatelliteInput(None, None, None, None, None, 0, 0, ())
    if kind is SourceKind.LIGHTNING:
        return LightningInput(None, None, 0, 0, False, ())
    if kind is SourceKind.GROUND:
        return GroundInput(None, None, 0, 0, (), ())
    if kind is SourceKind.MODEL:
        return ModelInput(None, None, None, 0, (), ())
    if kind is SourceKind.VALIDATION:
        return ValidationInput(None, 0, None, ())
    raise MultiSourceCycleError("source kind is onbekend")


@dataclass(frozen=True, slots=True)
class CycleTarget:
    target_id: str
    latitude: float
    longitude: float
    country_code: str | None
    available: bool
    target_sha256: str
    contract: str = TARGET_CONTRACT

    def __post_init__(self) -> None:
        _bounded_token(self.target_id, "target_id")
        _finite(self.latitude, "target.latitude", -90.0, 90.0)
        _finite(self.longitude, "target.longitude", -180.0, 180.0)
        if self.country_code is not None and (
            not isinstance(self.country_code, str)
            or _COUNTRY_RE.fullmatch(self.country_code) is None
        ):
            raise MultiSourceCycleError("target.country_code is geen ISO alpha-2")
        if type(self.available) is not bool:
            raise MultiSourceCycleError("target.available moet boolean zijn")
        if self.contract != TARGET_CONTRACT:
            raise MultiSourceCycleError("targetcontract is onbekend")
        _sha256(self.target_sha256, "target_sha256")
        if self.target_sha256 != _digest(self.payload_mapping()):
            raise MultiSourceCycleError("target_sha256 klopt niet")

    def payload_mapping(self) -> dict[str, Any]:
        return {
            "available": self.available,
            "contract": self.contract,
            "country_code": self.country_code,
            "latitude": float(self.latitude),
            "longitude": float(self.longitude),
            "target_id": self.target_id,
        }

    def to_mapping(self) -> dict[str, Any]:
        return {**self.payload_mapping(), "target_sha256": self.target_sha256}

    @classmethod
    def create(
        cls,
        *,
        target_id: str,
        latitude: float,
        longitude: float,
        country_code: str | None,
        available: bool = True,
    ) -> "CycleTarget":
        payload = {
            "available": available,
            "contract": TARGET_CONTRACT,
            "country_code": country_code,
            "latitude": float(latitude),
            "longitude": float(longitude),
            "target_id": target_id,
        }
        return cls(
            target_id=target_id,
            latitude=latitude,
            longitude=longitude,
            country_code=country_code,
            available=available,
            target_sha256=_digest(payload),
        )

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "CycleTarget":
        raw = _exact_mapping(value, {
            "available", "contract", "country_code", "latitude", "longitude",
            "target_id", "target_sha256",
        }, "target")
        return cls(
            target_id=raw["target_id"],
            latitude=raw["latitude"],
            longitude=raw["longitude"],
            country_code=raw["country_code"],
            available=raw["available"],
            target_sha256=raw["target_sha256"],
            contract=raw["contract"],
        )


@dataclass(frozen=True, slots=True)
class CatalogSource:
    source_id: str
    kind: SourceKind
    provider_id: str
    product_id: str
    execution_model: str
    cadence_seconds: int | None
    country_codes: tuple[str, ...]
    coverage_bbox: tuple[float, ...] | None
    satellite_id: str | None
    source_role: str
    catalog_status: str
    catalog_blocked_reason: str | None
    absence_semantics: str | None
    descriptor_sha256: str
    contract: str = CATALOG_DESCRIPTOR_CONTRACT

    def __post_init__(self) -> None:
        _safe_id(self.source_id, "catalog.source_id")
        if not isinstance(self.kind, SourceKind):
            raise MultiSourceCycleError("catalog.kind is onbekend")
        _safe_id(self.provider_id, "catalog.provider_id")
        _safe_id(self.product_id, "catalog.product_id")
        _safe_id(self.execution_model, "catalog.execution_model")
        if self.cadence_seconds is not None:
            _integer(
                self.cadence_seconds,
                "catalog.cadence_seconds",
                minimum=1,
                maximum=7 * 24 * 60 * 60,
            )
        if self.country_codes != tuple(sorted(set(self.country_codes))):
            raise MultiSourceCycleError("catalog.country_codes is niet canoniek")
        if any(_COUNTRY_RE.fullmatch(item) is None for item in self.country_codes):
            raise MultiSourceCycleError("catalog.country_codes bevat geen ISO-code")
        _bbox(self.coverage_bbox, "catalog.coverage_bbox", allow_none=True)
        _optional_token(self.satellite_id, "catalog.satellite_id")
        _safe_id(self.source_role, "catalog.source_role")
        _safe_id(self.catalog_status, "catalog.catalog_status")
        if self.catalog_blocked_reason is not None:
            _reason(self.catalog_blocked_reason, "catalog.catalog_blocked_reason")
        if self.absence_semantics is not None:
            _safe_id(self.absence_semantics, "catalog.absence_semantics")
        if self.contract != CATALOG_DESCRIPTOR_CONTRACT:
            raise MultiSourceCycleError("catalog descriptorcontract is onbekend")
        _sha256(self.descriptor_sha256, "catalog.descriptor_sha256")
        if self.descriptor_sha256 != _digest(self.payload_mapping()):
            raise MultiSourceCycleError("catalog descriptor_sha256 klopt niet")

    @property
    def catalog_blocked(self) -> bool:
        return self.catalog_status != "contract_ready_unwired"

    def payload_mapping(self) -> dict[str, Any]:
        return {
            "absence_semantics": self.absence_semantics,
            "cadence_seconds": self.cadence_seconds,
            "catalog_blocked_reason": self.catalog_blocked_reason,
            "catalog_status": self.catalog_status,
            "contract": self.contract,
            "country_codes": list(self.country_codes),
            "coverage_bbox": list(self.coverage_bbox) if self.coverage_bbox else None,
            "execution_model": self.execution_model,
            "kind": self.kind.value,
            "product_id": self.product_id,
            "provider_id": self.provider_id,
            "satellite_id": self.satellite_id,
            "source_id": self.source_id,
            "source_role": self.source_role,
        }


def _catalog_source(
    *,
    source_id: str,
    kind: SourceKind,
    provider_id: str,
    product_id: str,
    execution_model: str,
    cadence_seconds: int | None,
    country_codes: tuple[str, ...],
    coverage_bbox: tuple[float, ...] | None,
    satellite_id: str | None,
    source_role: str,
    catalog_status: str,
    catalog_blocked_reason: str | None,
    absence_semantics: str | None,
) -> CatalogSource:
    payload = {
        "absence_semantics": absence_semantics,
        "cadence_seconds": cadence_seconds,
        "catalog_blocked_reason": catalog_blocked_reason,
        "catalog_status": catalog_status,
        "contract": CATALOG_DESCRIPTOR_CONTRACT,
        "country_codes": list(country_codes),
        "coverage_bbox": list(coverage_bbox) if coverage_bbox else None,
        "execution_model": execution_model,
        "kind": kind.value,
        "product_id": product_id,
        "provider_id": provider_id,
        "satellite_id": satellite_id,
        "source_id": source_id,
        "source_role": source_role,
    }
    return CatalogSource(
        source_id=source_id,
        kind=kind,
        provider_id=provider_id,
        product_id=product_id,
        execution_model=execution_model,
        cadence_seconds=cadence_seconds,
        country_codes=country_codes,
        coverage_bbox=coverage_bbox,
        satellite_id=satellite_id,
        source_role=source_role,
        catalog_status=catalog_status,
        catalog_blocked_reason=catalog_blocked_reason,
        absence_semantics=absence_semantics,
        descriptor_sha256=_digest(payload),
    )


def _kind_for(source_id: str, workload_class: object) -> SourceKind:
    prefix = source_id.split(".", 1)[0]
    candidate = "validation" if prefix == "validation" else workload_class
    try:
        return SourceKind(candidate)
    except (TypeError, ValueError) as err:
        raise MultiSourceCycleError(f"catalogus bevat onbekende soort voor {source_id}") from err


def verify_acquisition_catalog(snapshot: Mapping[str, Any]) -> tuple[CatalogSource, ...]:
    """Verify the complete upstream catalog and derive immutable descriptors."""

    raw = _exact_mapping(snapshot, {
        "cadence_profile_id", "cadence_profile_sha256", "catalog_sha256", "contract",
        "event_ingresses", "method", "migrations", "workloads",
    }, "acquisition catalog")
    claimed_sha = _sha256(raw["catalog_sha256"], "catalog.catalog_sha256")
    payload = dict(raw)
    payload.pop("catalog_sha256")
    payload.pop("cadence_profile_id")
    payload.pop("cadence_profile_sha256")
    if _digest(payload) != claimed_sha:
        raise MultiSourceCycleError("acquisition cataloginhoud en hash verschillen")
    if claimed_sha != EXPECTED_ACQUISITION_CATALOG_SHA256:
        raise MultiSourceCycleError("acquisition catalog driftte van de gesloten cyclelaag")
    workloads = raw["workloads"]
    ingresses = raw["event_ingresses"]
    if not isinstance(workloads, list) or not isinstance(ingresses, list):
        raise MultiSourceCycleError("acquisition cataloglijsten ontbreken")
    workload_ids = [item.get("workload_id") for item in workloads if isinstance(item, Mapping)]
    ingress_ids = [item.get("ingress_id") for item in ingresses if isinstance(item, Mapping)]
    if (
        len(workloads) != CADENCE_SOURCE_COUNT
        or len(workload_ids) != len(workloads)
        or len(set(workload_ids)) != len(workload_ids)
        or frozenset(workload_ids) != _EXPECTED_CADENCE_SOURCE_IDS
    ):
        raise MultiSourceCycleError("cadencecatalogus is dubbel, onvolledig of uitgebreid")
    if (
        len(ingresses) != EVENT_INGRESS_SOURCE_COUNT
        or len(ingress_ids) != len(ingresses)
        or len(set(ingress_ids)) != len(ingress_ids)
        or frozenset(ingress_ids) != _EXPECTED_EVENT_INGRESS_IDS
    ):
        raise MultiSourceCycleError("event-ingresscatalogus is niet exact")

    result: list[CatalogSource] = []
    for row in workloads:
        if not isinstance(row, Mapping):
            raise MultiSourceCycleError("cadencecatalogus bevat geen object")
        source_id = _safe_id(row.get("workload_id"), "catalog.workload_id")
        countries = row.get("country_codes")
        if not isinstance(countries, list):
            raise MultiSourceCycleError("catalog.country_codes is geen lijst")
        coverage = _bbox(row.get("coverage_bbox"), "catalog.coverage_bbox", allow_none=True)
        result.append(_catalog_source(
            source_id=source_id,
            kind=_kind_for(source_id, row.get("workload_class")),
            provider_id=row.get("provider_id"),
            product_id=row.get("product_id"),
            execution_model=row.get("execution_model"),
            cadence_seconds=row.get("cadence_seconds"),
            country_codes=tuple(countries),
            coverage_bbox=coverage,
            satellite_id=row.get("satellite_id"),
            source_role=row.get("source_role"),
            catalog_status=row.get("status"),
            catalog_blocked_reason=row.get("blocked_reason"),
            absence_semantics=None,
        ))
    for row in ingresses:
        if not isinstance(row, Mapping):
            raise MultiSourceCycleError("event-ingresscatalogus bevat geen object")
        source_id = _safe_id(row.get("ingress_id"), "catalog.ingress_id")
        result.append(_catalog_source(
            source_id=source_id,
            kind=_kind_for(source_id, row.get("workload_class")),
            provider_id=row.get("provider_id"),
            product_id=row.get("product_id"),
            execution_model=row.get("delivery_model"),
            cadence_seconds=None,
            country_codes=(),
            coverage_bbox=None,
            satellite_id=None,
            source_role="lightning_event_candidate",
            catalog_status=row.get("status"),
            catalog_blocked_reason=row.get("blocked_reason"),
            absence_semantics=row.get("absence_semantics"),
        ))
    ordered = tuple(sorted(result, key=lambda item: item.source_id))
    if tuple(item.source_id for item in ordered) != tuple(sorted(EXPECTED_SOURCE_IDS)):
        raise MultiSourceCycleError("afgeleide sourcecatalogus is niet exact gesloten")
    if sum(not item.catalog_blocked for item in ordered if item.cadence_seconds) != (
        DISPATCHABLE_CADENCE_SOURCE_COUNT
    ):
        raise MultiSourceCycleError("dispatchbare cadencetelling is gewijzigd")
    return ordered


CATALOG_SOURCES = verify_acquisition_catalog(acquisition_catalog_snapshot())
CATALOG_BY_ID = {item.source_id: item for item in CATALOG_SOURCES}


@dataclass(frozen=True, slots=True)
class TargetApplicability:
    target_id: str
    country_assessment: str
    bbox_assessment: str
    applicable: bool
    reason_code: str

    def __post_init__(self) -> None:
        _bounded_token(self.target_id, "applicability.target_id")
        if self.country_assessment not in {
            "match", "mismatch", "not_constrained", "unknown",
        }:
            raise MultiSourceCycleError("country_assessment is onbekend")
        if self.bbox_assessment not in {"match", "mismatch", "not_constrained"}:
            raise MultiSourceCycleError("bbox_assessment is onbekend")
        if type(self.applicable) is not bool:
            raise MultiSourceCycleError("applicable moet boolean zijn")
        _reason(self.reason_code, "applicability.reason_code")

    def to_mapping(self) -> dict[str, Any]:
        return {
            "applicable": self.applicable,
            "bbox_assessment": self.bbox_assessment,
            "country_assessment": self.country_assessment,
            "reason_code": self.reason_code,
            "target_id": self.target_id,
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "TargetApplicability":
        raw = _exact_mapping(value, {
            "applicable", "bbox_assessment", "country_assessment", "reason_code",
            "target_id",
        }, "target applicability")
        return cls(
            target_id=raw["target_id"],
            country_assessment=raw["country_assessment"],
            bbox_assessment=raw["bbox_assessment"],
            applicable=raw["applicable"],
            reason_code=raw["reason_code"],
        )


def evaluate_target_applicability(
    source: CatalogSource,
    target: CycleTarget,
) -> TargetApplicability:
    """Apply the closed country-first, bbox-fallback catalog scope rule."""

    if not isinstance(source, CatalogSource) or not isinstance(target, CycleTarget):
        raise MultiSourceCycleError("applicability vereist gevalideerde source en target")
    if target.country_code is None:
        country_assessment = "unknown" if source.country_codes else "not_constrained"
    elif not source.country_codes:
        country_assessment = "not_constrained"
    elif target.country_code in source.country_codes:
        country_assessment = "match"
    else:
        country_assessment = "mismatch"

    if source.coverage_bbox is None:
        bbox_assessment = "not_constrained"
    else:
        south, west, north, east = source.coverage_bbox
        bbox_assessment = (
            "match"
            if south <= target.latitude <= north and west <= target.longitude <= east
            else "mismatch"
        )

    if not target.available:
        applicable, reason_code = False, "target_unavailable"
    elif country_assessment == "match":
        applicable, reason_code = True, "country_match"
    elif country_assessment == "mismatch":
        applicable, reason_code = False, "country_mismatch"
    elif country_assessment == "unknown" and bbox_assessment == "match":
        applicable, reason_code = True, "bbox_match_country_unknown"
    elif country_assessment == "unknown":
        applicable, reason_code = False, "country_unknown_without_bbox_match"
    elif bbox_assessment == "mismatch":
        applicable, reason_code = False, "bbox_mismatch"
    elif bbox_assessment == "match":
        applicable, reason_code = True, "bbox_match"
    else:
        applicable, reason_code = True, "unconstrained_global_scope"
    return TargetApplicability(
        target_id=target.target_id,
        country_assessment=country_assessment,
        bbox_assessment=bbox_assessment,
        applicable=applicable,
        reason_code=reason_code,
    )


def _validate_contribution_state(
    source: CatalogSource,
    status: CycleStatus,
    reason_code: str | None,
    fresh_until_ms: int | None,
    applicability: tuple[TargetApplicability, ...],
    payload: SourceInput,
) -> None:
    any_applicable = any(item.applicable for item in applicability)
    if any_applicable and status is CycleStatus.NOT_APPLICABLE:
        raise MultiSourceCycleError("toepasselijke bron mag niet not_applicable zijn")
    if not any_applicable and status is not CycleStatus.NOT_APPLICABLE:
        raise MultiSourceCycleError("bron buiten alle targets moet not_applicable zijn")
    if status is CycleStatus.NOT_APPLICABLE:
        if reason_code != "target_outside_catalog_scope":
            raise MultiSourceCycleError("not_applicable mist de vaste scope-reden")
    elif status is CycleStatus.CURRENT:
        if reason_code is not None:
            raise MultiSourceCycleError("current mag geen reason_code hebben")
    elif reason_code is None:
        raise MultiSourceCycleError("niet-current status mist reason_code")
    if reason_code is not None:
        _reason(reason_code)
    if source.catalog_blocked and any_applicable:
        if (
            status is not CycleStatus.BLOCKED
            or reason_code != source.catalog_blocked_reason
        ):
            raise MultiSourceCycleError("cataloggeblokkeerde bron is niet exact blocked")
    if status in {CycleStatus.CURRENT, CycleStatus.STALE}:
        if not payload.has_data or fresh_until_ms is None:
            raise MultiSourceCycleError("current/stale bron mist inhoud of freshness")
    else:
        if payload.has_data or fresh_until_ms is not None:
            raise MultiSourceCycleError("niet-data status mag geen brondata dragen")


@dataclass(frozen=True, slots=True)
class SourceContribution:
    source_id: str
    kind: SourceKind
    provider_id: str
    product_id: str
    source_descriptor_sha256: str
    status: CycleStatus
    reason_code: str | None
    fresh_until_ms: int | None
    applicability: tuple[TargetApplicability, ...]
    payload: SourceInput
    contribution_sha256: str
    contract: str = CONTRIBUTION_CONTRACT

    def __post_init__(self) -> None:
        source = CATALOG_BY_ID.get(self.source_id)
        if source is None:
            raise MultiSourceCycleError("contribution verwijst naar onbekende bron")
        if (
            self.kind is not source.kind
            or self.provider_id != source.provider_id
            or self.product_id != source.product_id
            or self.source_descriptor_sha256 != source.descriptor_sha256
        ):
            raise MultiSourceCycleError("contribution identity driftte van catalogus")
        if not isinstance(self.status, CycleStatus):
            raise MultiSourceCycleError("contribution status is onbekend")
        _optional_timestamp_ms(self.fresh_until_ms, "contribution.fresh_until_ms")
        if not isinstance(self.applicability, tuple):
            raise MultiSourceCycleError("applicability moet een tuple zijn")
        if len(self.applicability) > MAX_TARGETS:
            raise MultiSourceCycleError("applicability overschrijdt targetlimiet")
        if any(not isinstance(item, TargetApplicability) for item in self.applicability):
            raise MultiSourceCycleError("applicability bevat fout type")
        ids = tuple(item.target_id for item in self.applicability)
        if ids != tuple(sorted(set(ids))):
            raise MultiSourceCycleError("applicability is niet uniek en gesorteerd")
        expected_type = _PAYLOAD_TYPE[self.kind]
        if not isinstance(self.payload, expected_type):
            raise MultiSourceCycleError("contribution payloadtype past niet bij bronsoort")
        _validate_contribution_state(
            source,
            self.status,
            self.reason_code,
            self.fresh_until_ms,
            self.applicability,
            self.payload,
        )
        if self.contract != CONTRIBUTION_CONTRACT:
            raise MultiSourceCycleError("contribution contract is onbekend")
        _sha256(self.contribution_sha256, "contribution_sha256")
        if self.contribution_sha256 != _digest(self.payload_mapping()):
            raise MultiSourceCycleError("contribution_sha256 klopt niet")

    def payload_mapping(self) -> dict[str, Any]:
        return {
            "applicability": [item.to_mapping() for item in self.applicability],
            "contract": self.contract,
            "fresh_until_ms": self.fresh_until_ms,
            "kind": self.kind.value,
            "payload": self.payload.to_mapping(),
            "product_id": self.product_id,
            "provider_id": self.provider_id,
            "reason_code": self.reason_code,
            "source_descriptor_sha256": self.source_descriptor_sha256,
            "source_id": self.source_id,
            "status": self.status.value,
        }

    def to_mapping(self) -> dict[str, Any]:
        return {
            **self.payload_mapping(),
            "contribution_sha256": self.contribution_sha256,
        }

    @classmethod
    def create(
        cls,
        *,
        source_id: str,
        targets: Iterable[CycleTarget],
        status: CycleStatus,
        payload: SourceInput,
        reason_code: str | None = None,
        fresh_until_ms: int | None = None,
    ) -> "SourceContribution":
        source = CATALOG_BY_ID.get(source_id)
        if source is None:
            raise MultiSourceCycleError("onbekende source_id")
        target_tuple = tuple(sorted(targets, key=lambda item: item.target_id))
        applicability = tuple(
            evaluate_target_applicability(source, target) for target in target_tuple
        )
        provisional = {
            "applicability": [item.to_mapping() for item in applicability],
            "contract": CONTRIBUTION_CONTRACT,
            "fresh_until_ms": fresh_until_ms,
            "kind": source.kind.value,
            "payload": payload.to_mapping(),
            "product_id": source.product_id,
            "provider_id": source.provider_id,
            "reason_code": reason_code,
            "source_descriptor_sha256": source.descriptor_sha256,
            "source_id": source.source_id,
            "status": status.value if isinstance(status, CycleStatus) else status,
        }
        return cls(
            source_id=source.source_id,
            kind=source.kind,
            provider_id=source.provider_id,
            product_id=source.product_id,
            source_descriptor_sha256=source.descriptor_sha256,
            status=status,
            reason_code=reason_code,
            fresh_until_ms=fresh_until_ms,
            applicability=applicability,
            payload=payload,
            contribution_sha256=_digest(provisional),
        )

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "SourceContribution":
        raw = _exact_mapping(value, {
            "applicability", "contract", "contribution_sha256", "fresh_until_ms",
            "kind", "payload", "product_id", "provider_id", "reason_code",
            "source_descriptor_sha256", "source_id", "status",
        }, "source contribution")
        if not isinstance(raw["applicability"], list):
            raise MultiSourceCycleError("contribution applicability is geen lijst")
        try:
            kind = SourceKind(raw["kind"])
            status = CycleStatus(raw["status"])
        except (TypeError, ValueError) as err:
            raise MultiSourceCycleError("contribution kind/status is onbekend") from err
        return cls(
            source_id=raw["source_id"],
            kind=kind,
            provider_id=raw["provider_id"],
            product_id=raw["product_id"],
            source_descriptor_sha256=raw["source_descriptor_sha256"],
            status=status,
            reason_code=raw["reason_code"],
            fresh_until_ms=raw["fresh_until_ms"],
            applicability=tuple(
                TargetApplicability.from_mapping(item)
                for item in raw["applicability"]
            ),
            payload=_parse_payload(kind, raw["payload"]),
            contribution_sha256=raw["contribution_sha256"],
            contract=raw["contract"],
        )


@dataclass(frozen=True, slots=True)
class CycleCoverage:
    source_count: int
    cadence_source_count: int
    dispatchable_cadence_source_count: int
    event_ingress_source_count: int
    target_count: int
    applicability_row_count: int
    applicable_source_count: int
    evidence_reference_count: int
    referenced_content_bytes: int
    status_counts: tuple[tuple[str, int], ...]

    def __post_init__(self) -> None:
        for name in (
            "source_count", "cadence_source_count", "dispatchable_cadence_source_count",
            "event_ingress_source_count", "target_count", "applicability_row_count",
            "applicable_source_count", "evidence_reference_count",
            "referenced_content_bytes",
        ):
            _integer(getattr(self, name), f"coverage.{name}")
        expected_keys = tuple(sorted(item.value for item in CycleStatus))
        if tuple(key for key, _ in self.status_counts) != expected_keys:
            raise MultiSourceCycleError("coverage.status_counts is niet exact")
        for _, count in self.status_counts:
            _integer(count, "coverage.status_count", maximum=SOURCE_COUNT)

    def to_mapping(self) -> dict[str, Any]:
        return {
            "applicability_row_count": self.applicability_row_count,
            "applicable_source_count": self.applicable_source_count,
            "cadence_source_count": self.cadence_source_count,
            "dispatchable_cadence_source_count": self.dispatchable_cadence_source_count,
            "event_ingress_source_count": self.event_ingress_source_count,
            "evidence_reference_count": self.evidence_reference_count,
            "referenced_content_bytes": self.referenced_content_bytes,
            "source_count": self.source_count,
            "status_counts": dict(self.status_counts),
            "target_count": self.target_count,
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "CycleCoverage":
        raw = _exact_mapping(value, {
            "applicability_row_count", "applicable_source_count", "cadence_source_count",
            "dispatchable_cadence_source_count", "event_ingress_source_count",
            "evidence_reference_count", "referenced_content_bytes", "source_count",
            "status_counts", "target_count",
        }, "cycle coverage")
        counts = raw["status_counts"]
        if not isinstance(counts, Mapping):
            raise MultiSourceCycleError("coverage.status_counts is geen object")
        return cls(
            source_count=raw["source_count"],
            cadence_source_count=raw["cadence_source_count"],
            dispatchable_cadence_source_count=raw["dispatchable_cadence_source_count"],
            event_ingress_source_count=raw["event_ingress_source_count"],
            target_count=raw["target_count"],
            applicability_row_count=raw["applicability_row_count"],
            applicable_source_count=raw["applicable_source_count"],
            evidence_reference_count=raw["evidence_reference_count"],
            referenced_content_bytes=raw["referenced_content_bytes"],
            status_counts=tuple(sorted(counts.items())),
        )


def _payload_evidence(payload: SourceInput) -> tuple[EvidenceReference, ...]:
    return payload.evidence


def _coverage(
    targets: tuple[CycleTarget, ...],
    contributions: tuple[SourceContribution, ...],
) -> CycleCoverage:
    status_counts = tuple(
        (
            status.value,
            sum(item.status is status for item in contributions),
        )
        for status in sorted(CycleStatus, key=lambda item: item.value)
    )
    evidence = tuple(
        evidence
        for contribution in contributions
        for evidence in _payload_evidence(contribution.payload)
    )
    if len(evidence) > MAX_TOTAL_EVIDENCE_REFERENCES:
        raise MultiSourceCycleError("cycle overschrijdt totale evidencelimiet")
    rows = len(targets) * len(contributions)
    if rows > MAX_APPLICABILITY_ROWS:
        raise MultiSourceCycleError("cycle overschrijdt applicabilitylimiet")
    return CycleCoverage(
        source_count=len(contributions),
        cadence_source_count=CADENCE_SOURCE_COUNT,
        dispatchable_cadence_source_count=DISPATCHABLE_CADENCE_SOURCE_COUNT,
        event_ingress_source_count=EVENT_INGRESS_SOURCE_COUNT,
        target_count=len(targets),
        applicability_row_count=rows,
        applicable_source_count=sum(
            any(row.applicable for row in item.applicability)
            for item in contributions
        ),
        evidence_reference_count=len(evidence),
        referenced_content_bytes=sum(item.size_bytes for item in evidence),
        status_counts=status_counts,
    )


@dataclass(frozen=True, slots=True)
class MultiSourceOperationalCycle:
    desired_state_sha256: str
    authority_epoch: int
    cycle_sequence: int
    producer_id: str
    cycle_started_at_ms: int
    cycle_completed_at_ms: int
    targets: tuple[CycleTarget, ...]
    target_set_sha256: str
    contributions: tuple[SourceContribution, ...]
    coverage: CycleCoverage
    cycle_sha256: str
    acquisition_catalog_sha256: str = EXPECTED_ACQUISITION_CATALOG_SHA256
    contract: str = CYCLE_CONTRACT

    def __post_init__(self) -> None:
        _sha256(self.desired_state_sha256, "cycle.desired_state_sha256")
        _integer(self.authority_epoch, "cycle.authority_epoch", minimum=1)
        _integer(self.cycle_sequence, "cycle.cycle_sequence", minimum=1)
        _bounded_token(self.producer_id, "cycle.producer_id")
        started = _integer(self.cycle_started_at_ms, "cycle.started_at_ms")
        completed = _integer(self.cycle_completed_at_ms, "cycle.completed_at_ms")
        if completed < started or completed - started > MAX_CYCLE_DURATION_MS:
            raise MultiSourceCycleError("cycletijdsvenster is omgekeerd of te groot")
        if not isinstance(self.targets, tuple) or not 1 <= len(self.targets) <= MAX_TARGETS:
            raise MultiSourceCycleError("cycle bevat geen of te veel targets")
        if any(not isinstance(item, CycleTarget) for item in self.targets):
            raise MultiSourceCycleError("cycle targets bevat fout type")
        target_ids = tuple(item.target_id for item in self.targets)
        if target_ids != tuple(sorted(set(target_ids))):
            raise MultiSourceCycleError("cycle targets is niet uniek en gesorteerd")
        if self.target_set_sha256 != _digest([
            item.to_mapping() for item in self.targets
        ]):
            raise MultiSourceCycleError("target_set_sha256 klopt niet")
        if not isinstance(self.contributions, tuple):
            raise MultiSourceCycleError("cycle contributions moet een tuple zijn")
        source_ids = tuple(item.source_id for item in self.contributions)
        if (
            len(self.contributions) != SOURCE_COUNT
            or source_ids != tuple(sorted(EXPECTED_SOURCE_IDS))
        ):
            raise MultiSourceCycleError("cycle mist, dupliceert of voegt een bron toe")
        for contribution in self.contributions:
            expected = tuple(
                evaluate_target_applicability(
                    CATALOG_BY_ID[contribution.source_id],
                    target,
                )
                for target in self.targets
            )
            if contribution.applicability != expected:
                raise MultiSourceCycleError("contribution targetdekking is niet exact")
            if contribution.status is CycleStatus.CURRENT:
                if contribution.fresh_until_ms < completed:  # type: ignore[operator]
                    raise MultiSourceCycleError("current contribution is al stale")
            elif contribution.status is CycleStatus.STALE:
                if contribution.fresh_until_ms >= completed:  # type: ignore[operator]
                    raise MultiSourceCycleError("stale contribution is nog current")
        expected_coverage = _coverage(self.targets, self.contributions)
        if self.coverage != expected_coverage:
            raise MultiSourceCycleError("cycle coverage-samenvatting klopt niet")
        if self.acquisition_catalog_sha256 != EXPECTED_ACQUISITION_CATALOG_SHA256:
            raise MultiSourceCycleError("cycle gebruikt een andere acquisition catalog")
        if self.contract != CYCLE_CONTRACT:
            raise MultiSourceCycleError("cyclecontract is onbekend")
        _sha256(self.cycle_sha256, "cycle_sha256")
        if self.cycle_sha256 != _digest(self.payload_mapping()):
            raise MultiSourceCycleError("cycle_sha256 klopt niet")
        if len(_canonical_json(self.to_mapping())) > MAX_CYCLE_BYTES:
            raise MultiSourceCycleError("cycle overschrijdt de documentlimiet")

    def payload_mapping(self) -> dict[str, Any]:
        return {
            "acquisition_catalog_sha256": self.acquisition_catalog_sha256,
            "authority_epoch": self.authority_epoch,
            "contract": self.contract,
            "contributions": [item.to_mapping() for item in self.contributions],
            "coverage": self.coverage.to_mapping(),
            "cycle_completed_at_ms": self.cycle_completed_at_ms,
            "cycle_sequence": self.cycle_sequence,
            "cycle_started_at_ms": self.cycle_started_at_ms,
            "desired_state_sha256": self.desired_state_sha256,
            "producer_id": self.producer_id,
            "target_set_sha256": self.target_set_sha256,
            "targets": [item.to_mapping() for item in self.targets],
        }

    def to_mapping(self) -> dict[str, Any]:
        return {**self.payload_mapping(), "cycle_sha256": self.cycle_sha256}

    def to_canonical_bytes(self) -> bytes:
        return _canonical_json(self.to_mapping())

    @classmethod
    def create(
        cls,
        *,
        desired_state_sha256: str,
        authority_epoch: int,
        cycle_sequence: int,
        producer_id: str,
        cycle_started_at_ms: int,
        cycle_completed_at_ms: int,
        targets: Iterable[CycleTarget],
        contributions: Iterable[SourceContribution],
    ) -> "MultiSourceOperationalCycle":
        target_tuple = tuple(sorted(targets, key=lambda item: item.target_id))
        if not 1 <= len(target_tuple) <= MAX_TARGETS:
            raise MultiSourceCycleError("cycle bevat geen of te veel targets")
        contribution_tuple = tuple(
            sorted(contributions, key=lambda item: item.source_id)
        )
        target_sha = _digest([item.to_mapping() for item in target_tuple])
        coverage = _coverage(target_tuple, contribution_tuple)
        payload = {
            "acquisition_catalog_sha256": EXPECTED_ACQUISITION_CATALOG_SHA256,
            "authority_epoch": authority_epoch,
            "contract": CYCLE_CONTRACT,
            "contributions": [item.to_mapping() for item in contribution_tuple],
            "coverage": coverage.to_mapping(),
            "cycle_completed_at_ms": cycle_completed_at_ms,
            "cycle_sequence": cycle_sequence,
            "cycle_started_at_ms": cycle_started_at_ms,
            "desired_state_sha256": desired_state_sha256,
            "producer_id": producer_id,
            "target_set_sha256": target_sha,
            "targets": [item.to_mapping() for item in target_tuple],
        }
        return cls(
            desired_state_sha256=desired_state_sha256,
            authority_epoch=authority_epoch,
            cycle_sequence=cycle_sequence,
            producer_id=producer_id,
            cycle_started_at_ms=cycle_started_at_ms,
            cycle_completed_at_ms=cycle_completed_at_ms,
            targets=target_tuple,
            target_set_sha256=target_sha,
            contributions=contribution_tuple,
            coverage=coverage,
            cycle_sha256=_digest(payload),
        )

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "MultiSourceOperationalCycle":
        raw = _exact_mapping(value, {
            "acquisition_catalog_sha256", "authority_epoch", "contract", "contributions",
            "coverage", "cycle_completed_at_ms", "cycle_sequence", "cycle_sha256",
            "cycle_started_at_ms", "desired_state_sha256", "producer_id",
            "target_set_sha256", "targets",
        }, "multi-source cycle")
        if not isinstance(raw["targets"], list) or not isinstance(raw["contributions"], list):
            raise MultiSourceCycleError("cycle targets/contributions zijn geen lijsten")
        return cls(
            desired_state_sha256=raw["desired_state_sha256"],
            authority_epoch=raw["authority_epoch"],
            cycle_sequence=raw["cycle_sequence"],
            producer_id=raw["producer_id"],
            cycle_started_at_ms=raw["cycle_started_at_ms"],
            cycle_completed_at_ms=raw["cycle_completed_at_ms"],
            targets=tuple(CycleTarget.from_mapping(item) for item in raw["targets"]),
            target_set_sha256=raw["target_set_sha256"],
            contributions=tuple(
                SourceContribution.from_mapping(item) for item in raw["contributions"]
            ),
            coverage=CycleCoverage.from_mapping(raw["coverage"]),
            cycle_sha256=raw["cycle_sha256"],
            acquisition_catalog_sha256=raw["acquisition_catalog_sha256"],
            contract=raw["contract"],
        )


def _reject_duplicate_json_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise MultiSourceCycleError(f"JSON bevat dubbele sleutel {key}")
        result[key] = value
    return result


def parse_operational_cycle(
    value: bytes | bytearray | Mapping[str, Any],
) -> MultiSourceOperationalCycle:
    """Parse, bound, rehash, and fully revalidate one untrusted cycle."""

    if isinstance(value, (bytes, bytearray)):
        raw_bytes = bytes(value)
        if len(raw_bytes) > MAX_CYCLE_BYTES:
            raise MultiSourceCycleError("cyclebytes overschrijden de documentlimiet")
        try:
            decoded = json.loads(
                raw_bytes.decode("ascii"),
                object_pairs_hook=_reject_duplicate_json_keys,
            )
        except MultiSourceCycleError:
            raise
        except (UnicodeDecodeError, json.JSONDecodeError) as err:
            raise MultiSourceCycleError("cyclebytes zijn geen canonieke ASCII-JSON") from err
    elif isinstance(value, Mapping):
        if len(_canonical_json(value)) > MAX_CYCLE_BYTES:
            raise MultiSourceCycleError("cyclemapping overschrijdt de documentlimiet")
        decoded = value
    else:
        raise MultiSourceCycleError("cycle moet bytes of een mapping zijn")
    cycle = MultiSourceOperationalCycle.from_mapping(decoded)
    if isinstance(value, (bytes, bytearray)) and cycle.to_canonical_bytes() != raw_bytes:
        raise MultiSourceCycleError("cyclebytes zijn niet exact canoniek geserialiseerd")
    return cycle


__all__ = [
    "CADENCE_SOURCE_COUNT",
    "CATALOG_BY_ID",
    "CATALOG_DESCRIPTOR_CONTRACT",
    "CATALOG_SOURCES",
    "CONTRIBUTION_CONTRACT",
    "CYCLE_CONTRACT",
    "CycleCoverage",
    "CycleStatus",
    "CycleTarget",
    "DISPATCHABLE_CADENCE_SOURCE_COUNT",
    "EVENT_INGRESS_SOURCE_COUNT",
    "EXPECTED_ACQUISITION_CATALOG_SHA256",
    "EXPECTED_SOURCE_IDS",
    "EvidenceReference",
    "GROUND_INPUT_CONTRACT",
    "GroundInput",
    "LIGHTNING_INPUT_CONTRACT",
    "LightningInput",
    "MAX_CYCLE_BYTES",
    "MAX_EVIDENCE_REFERENCES_PER_SOURCE",
    "MAX_TOTAL_EVIDENCE_REFERENCES",
    "MAX_TARGETS",
    "MODEL_INPUT_CONTRACT",
    "ModelInput",
    "MultiSourceCycleError",
    "MultiSourceOperationalCycle",
    "RADAR_INPUT_CONTRACT",
    "RadarInput",
    "SATELLITE_INPUT_CONTRACT",
    "SOURCE_COUNT",
    "SatelliteInput",
    "SourceContribution",
    "SourceInput",
    "SourceKind",
    "TARGET_CONTRACT",
    "TargetApplicability",
    "VALIDATION_INPUT_CONTRACT",
    "ValidationInput",
    "empty_source_input",
    "evaluate_target_applicability",
    "parse_operational_cycle",
    "verify_acquisition_catalog",
]
