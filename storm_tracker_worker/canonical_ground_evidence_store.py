"""Candidate-only canonical ground-observation facts with exact raw authority.

This store is deliberately separate from radar evidence and from operational
ground confirmation.  It persists the lowest decoded fact level only: which
station reported which variable/value/QC and when, bound to one complete,
verified schema-3 :mod:`raw_artifacts` capture.  It never derives rain, dry,
wet-station, confirmation, confidence, forecast or admission claims.

The store contains no upstream payload bytes.  Every read reverifies the
complete raw capture through the supplied raw authority before returning a
fact.  Consequently an unavailable or damaged authority fails closed.
"""
from __future__ import annotations

from collections.abc import Sequence
from contextlib import closing
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
import hashlib
from itertools import islice
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import stat
import threading
import time
from typing import Any, Callable, Literal, Mapping, Protocol, runtime_checkable

from .raw_artifacts import (
    RAW_ARTIFACT_CAPTURE_RECEIPT_SCHEMA,
    RAW_ARTIFACT_CAPTURE_SCHEMA,
    RAW_ARTIFACT_SCHEMA_VERSION,
)


GROUND_EVIDENCE_CONTRACT = "stv3-worker-canonical-ground-fact-v1"
GROUND_EVIDENCE_RECEIPT_SCHEMA = (
    "stv3-worker-canonical-ground-fact-receipt-v1"
)
GROUND_OBSERVATION_IDENTITY_SCHEMA = (
    "stv3-worker-canonical-ground-observation-identity-v1"
)
GROUND_FACT_IDENTITY_SCHEMA = "stv3-worker-canonical-ground-fact-identity-v1"
STORE_SCHEMA_VERSION = 1
STORE_ALGORITHM_VERSION = "canonical-ground-evidence-store-v1"
ADMISSION_STATUS = "candidate_only_unadmitted"
LIMITS_PROFILE_SCHEMA = "stv3-worker-canonical-ground-limits-profile-v1"
CATALOG_JOURNAL_MODE = "delete"
MAIN_DATABASE_ENVELOPE_DIVISOR = 4
DIRECTORY_FSYNC_POLICY = "posix_required_windows_unavailable"
DIRECTORY_FSYNC_SUPPORTED = os.name != "nt"

HARD_MAX_OPERATION_BYTES = 64 * 1024
HARD_MAX_CAPTURE_MEMBERS = 64
HARD_MAX_QC_FLAGS = 64
HARD_MAX_BATCH_FACTS = 64_000
HARD_MAX_BATCH_CANONICAL_BYTES = 64 * 1024 * 1024
MAX_TIMESTAMP_SECONDS = 253_402_300_799.0
DEFAULT_MAX_CATALOG_BYTES = 256 * 1024 * 1024
# Compatibility label for code which only imports the former constant.  The
# value now means the complete explicit catalog envelope, not only main pages.
DEFAULT_MAX_DATABASE_BYTES = DEFAULT_MAX_CATALOG_BYTES

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_CAPTURE_ID_RE = re.compile(r"^capture-[0-9a-f]{64}$")
_FACT_ID_RE = re.compile(r"^ground-fact-[0-9a-f]{64}$")
_OBSERVATION_KEY_RE = re.compile(r"^ground-observation-[0-9a-f]{64}$")
_VALUE_STATES = frozenset({
    "observed",
    "missing",
    "trace",
    "censored_below",
    "censored_above",
    "unknown",
})
_VALUE_REPRESENTATIONS = frozenset({"decimal", "text", "none"})
_POSITION_STATES = frozenset({"known", "unknown"})

GroundValueState = Literal[
    "observed",
    "missing",
    "trace",
    "censored_below",
    "censored_above",
    "unknown",
]
GroundValueRepresentation = Literal["decimal", "text", "none"]


class CanonicalGroundEvidenceError(RuntimeError):
    """Base failure for candidate ground-fact persistence."""


class CanonicalGroundEvidenceConflict(CanonicalGroundEvidenceError):
    """A stable observation identity was reused for different facts."""


class CanonicalGroundEvidenceIntegrityError(CanonicalGroundEvidenceError):
    """The local catalog or referenced raw authority failed verification."""


class CanonicalGroundEvidenceCapacityError(CanonicalGroundEvidenceError):
    """A hard row, operation or database boundary rejected a whole commit."""


class CanonicalGroundEvidenceDeadlineError(CanonicalGroundEvidenceError):
    """A cooperative operation deadline elapsed; no weather state is implied."""


class GroundCaptureVerificationError(CanonicalGroundEvidenceIntegrityError):
    """The referenced exact schema-3 capture was absent or did not match."""


@dataclass(frozen=True, slots=True)
class GroundEvidenceLimits:
    """Hard local bounds.  Rejection never means dry or missing weather."""

    max_catalog_bytes: int = DEFAULT_MAX_CATALOG_BYTES
    max_observations: int = 1_000_000
    max_member_links: int = 64_000_000
    max_qc_rows: int = 64_000_000
    max_query_rows: int = 1_000
    max_operation_bytes: int = HARD_MAX_OPERATION_BYTES
    max_batch_facts: int = HARD_MAX_BATCH_FACTS
    max_batch_canonical_bytes: int = HARD_MAX_BATCH_CANONICAL_BYTES
    sqlite_busy_timeout_seconds: float = 5.0

    def __post_init__(self) -> None:
        for name in (
            "max_catalog_bytes",
            "max_observations",
            "max_member_links",
            "max_qc_rows",
            "max_query_rows",
            "max_operation_bytes",
            "max_batch_facts",
            "max_batch_canonical_bytes",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
        if self.max_catalog_bytes < 1024 * 1024:
            raise ValueError("max_catalog_bytes must be at least 1 MiB")
        if self.max_operation_bytes > HARD_MAX_OPERATION_BYTES:
            raise ValueError("max_operation_bytes exceeds the hard contract bound")
        if self.max_batch_facts > HARD_MAX_BATCH_FACTS:
            raise ValueError("max_batch_facts exceeds the hard contract bound")
        if self.max_batch_canonical_bytes > HARD_MAX_BATCH_CANONICAL_BYTES:
            raise ValueError(
                "max_batch_canonical_bytes exceeds the hard contract bound"
            )
        timeout = self.sqlite_busy_timeout_seconds
        if (
            isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or not math.isfinite(float(timeout))
            or float(timeout) <= 0.0
            or float(timeout) > 30.0
        ):
            raise ValueError("sqlite_busy_timeout_seconds is invalid")

    def to_mapping(self) -> dict[str, Any]:
        """Return the immutable store-bound capacity/durability profile."""
        return {
            "schema": LIMITS_PROFILE_SCHEMA,
            "max_catalog_bytes": self.max_catalog_bytes,
            "max_observations": self.max_observations,
            "max_member_links": self.max_member_links,
            "max_qc_rows": self.max_qc_rows,
            "max_query_rows": self.max_query_rows,
            "max_operation_bytes": self.max_operation_bytes,
            "max_batch_facts": self.max_batch_facts,
            "max_batch_canonical_bytes": self.max_batch_canonical_bytes,
            "sqlite_busy_timeout_seconds": float(
                self.sqlite_busy_timeout_seconds
            ),
            "journal_mode": CATALOG_JOURNAL_MODE,
            "main_database_envelope_divisor": (
                MAIN_DATABASE_ENVELOPE_DIVISOR
            ),
            "directory_fsync_policy": DIRECTORY_FSYNC_POLICY,
        }


@runtime_checkable
class RawCaptureMemberReceiptView(Protocol):
    @property
    def member_id(self) -> str: ...

    role: str
    receipt_id: str
    content_sha256: str
    receipt_sha256: str
    received_at: float


@runtime_checkable
class RawCaptureReceiptView(Protocol):
    capture_id: str
    provider: str
    product: str
    fetch_id: str
    upstream_capture_id: str
    product_timestamp: float | None
    manifest_sha256: str
    capture_receipt_sha256: str
    members: tuple[RawCaptureMemberReceiptView, ...]

    @property
    def raw_store_schema_version(self) -> int: ...

    @property
    def capture_schema(self) -> str: ...

    @property
    def capture_receipt_schema(self) -> str: ...

    @property
    def complete(self) -> bool: ...


@runtime_checkable
class RawArtifactCaptureReader(Protocol):
    def load_capture_receipt(
        self,
        capture_id: str,
    ) -> RawCaptureReceiptView | None: ...


@dataclass(frozen=True, slots=True)
class GroundCaptureMemberBinding:
    member_id: str
    role: str
    receipt_id: str
    content_sha256: str
    receipt_sha256: str
    received_at: float

    def to_mapping(self) -> dict[str, Any]:
        return {
            "member_id": self.member_id,
            "role": self.role,
            "receipt_id": self.receipt_id,
            "content_sha256": self.content_sha256,
            "receipt_sha256": self.receipt_sha256,
            "received_at": self.received_at,
        }


@dataclass(frozen=True, slots=True)
class GroundStationSite:
    provider_station_id: str
    site_id: str
    network: str
    station_name: str | None
    position_state: Literal["known", "unknown"]
    latitude_decimal: str | None
    longitude_decimal: str | None
    elevation_state: Literal["known", "unknown"]
    elevation_m_decimal: str | None

    def to_mapping(self) -> dict[str, Any]:
        return {
            "provider_station_id": self.provider_station_id,
            "site_id": self.site_id,
            "network": self.network,
            "station_name": self.station_name,
            "position_state": self.position_state,
            "latitude_decimal": self.latitude_decimal,
            "longitude_decimal": self.longitude_decimal,
            "elevation_state": self.elevation_state,
            "elevation_m_decimal": self.elevation_m_decimal,
        }


@dataclass(frozen=True, slots=True)
class GroundMeasurementValue:
    variable: str
    state: GroundValueState
    representation: GroundValueRepresentation
    exact_value: str | None
    unit: str
    censor_limit: str | None

    def to_mapping(self) -> dict[str, Any]:
        return {
            "variable": self.variable,
            "state": self.state,
            "representation": self.representation,
            "exact_value": self.exact_value,
            "unit": self.unit,
            "censor_limit": self.censor_limit,
        }


@dataclass(frozen=True, slots=True)
class ProviderQcFact:
    flag: str
    value: str | None
    text: str | None

    def to_mapping(self) -> dict[str, Any]:
        return {"flag": self.flag, "value": self.value, "text": self.text}


@dataclass(frozen=True, slots=True)
class GroundObservationFact:
    capture_id: str
    capture_receipt_sha256: str
    members: tuple[GroundCaptureMemberBinding, ...]
    source_member_role: str
    source_member_content_sha256: str
    source_member_receipt_sha256: str
    source_record_locator: str
    source_record_sha256: str
    decoder_identity: str
    decoder_version: str
    config_sha256: str
    provider: str
    product: str
    station: GroundStationSite
    observed_at: float
    representative_interval_start: float
    representative_interval_end: float
    received_at: float
    ingested_at: float
    measurement: GroundMeasurementValue
    provider_qc: tuple[ProviderQcFact, ...]

    @property
    def operational_admission_eligible(self) -> bool:
        return False

    @property
    def admission_status(self) -> str:
        return ADMISSION_STATUS

    def to_mapping(self) -> dict[str, Any]:
        return {
            "contract": GROUND_EVIDENCE_CONTRACT,
            "operational_admission_eligible": False,
            "admission_status": ADMISSION_STATUS,
            "capture": {
                "capture_id": self.capture_id,
                "capture_receipt_sha256": self.capture_receipt_sha256,
                "members": [member.to_mapping() for member in self.members],
            },
            "source": {
                "member_role": self.source_member_role,
                "member_content_sha256": self.source_member_content_sha256,
                "member_receipt_sha256": self.source_member_receipt_sha256,
                "record_locator": self.source_record_locator,
                "record_sha256": self.source_record_sha256,
            },
            "decoder": {
                "identity": self.decoder_identity,
                "version": self.decoder_version,
                "config_sha256": self.config_sha256,
            },
            "provider": self.provider,
            "product": self.product,
            "station": self.station.to_mapping(),
            "time": {
                "observed_at": self.observed_at,
                "representative_interval_start": (
                    self.representative_interval_start
                ),
                "representative_interval_end": self.representative_interval_end,
                "received_at": self.received_at,
                "ingested_at": self.ingested_at,
            },
            "measurement": self.measurement.to_mapping(),
            "provider_qc": [item.to_mapping() for item in self.provider_qc],
        }


@dataclass(frozen=True, slots=True)
class GroundObservationReceipt:
    fact_id: str
    observation_key: str
    fact_sha256: str
    receipt_sha256: str
    capture_manifest_sha256: str
    committed_at: float
    fact: GroundObservationFact

    @property
    def operational_admission_eligible(self) -> bool:
        return False

    @property
    def admission_status(self) -> str:
        return ADMISSION_STATUS

    def to_metadata(self) -> dict[str, Any]:
        return {
            "schema": GROUND_EVIDENCE_RECEIPT_SCHEMA,
            "fact_id": self.fact_id,
            "observation_key": self.observation_key,
            "fact_sha256": self.fact_sha256,
            "receipt_sha256": self.receipt_sha256,
            "capture_manifest_sha256": self.capture_manifest_sha256,
            "committed_at": self.committed_at,
            "operational_admission_eligible": False,
            "admission_status": ADMISSION_STATUS,
            "fact": self.fact.to_mapping(),
        }


@dataclass(frozen=True, slots=True)
class _VerifiedCapture:
    capture_id: str
    provider: str
    product: str
    manifest_sha256: str
    capture_receipt_sha256: str
    members: tuple[GroundCaptureMemberBinding, ...]


@dataclass(frozen=True, slots=True)
class _PreparedGroundFact:
    fact: GroundObservationFact
    canonical: bytes
    observation_key: str
    fact_sha256: str
    fact_id: str


def _exact_keys(
    value: object,
    expected: frozenset[str],
    name: str,
) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} must be an object")
    actual = frozenset(value.keys())
    string_keys = frozenset(key for key in actual if isinstance(key, str))
    if actual != expected or len(string_keys) != len(actual):
        missing = sorted(expected - string_keys)
        extra = sorted(repr(key) for key in actual if key not in expected)
        raise ValueError(f"{name} keys mismatch; missing={missing}, extra={extra}")
    return value


def _identifier(value: object, name: str, maximum: int) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value) > maximum
        or any(ord(character) < 32 for character in value)
    ):
        raise ValueError(f"{name} is invalid")
    return value


def _verbatim(
    value: object,
    name: str,
    maximum: int,
    *,
    empty: bool = True,
) -> str:
    if (
        not isinstance(value, str)
        or (not empty and not value)
        or len(value) > maximum
        or any(ord(character) < 32 for character in value)
    ):
        raise ValueError(f"{name} is invalid")
    return value


def _optional_verbatim(
    value: object,
    name: str,
    maximum: int,
) -> str | None:
    return None if value is None else _verbatim(value, name, maximum)


def _sha256(value: object, name: str) -> str:
    if not isinstance(value, str) or not _SHA256_RE.fullmatch(value):
        raise ValueError(f"{name} is not a lowercase SHA-256")
    return value


def _timestamp(value: object, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} is invalid")
    result = float(value)
    if (
        not math.isfinite(result)
        or result < 0.0
        or result > MAX_TIMESTAMP_SECONDS
    ):
        raise ValueError(f"{name} is invalid")
    return result


def _decimal_text(
    value: object,
    name: str,
    *,
    minimum: Decimal | None = None,
    maximum: Decimal | None = None,
) -> str:
    text = _verbatim(value, name, 128, empty=False)
    if text != text.strip():
        raise ValueError(f"{name} is invalid")
    try:
        parsed = Decimal(text)
    except InvalidOperation as exc:
        raise ValueError(f"{name} is not an exact finite decimal") from exc
    if not parsed.is_finite() or parsed.copy_abs() > Decimal("1e100"):
        raise ValueError(f"{name} is not an exact finite decimal")
    if minimum is not None and parsed < minimum:
        raise ValueError(f"{name} lies outside its allowed range")
    if maximum is not None and parsed > maximum:
        raise ValueError(f"{name} lies outside its allowed range")
    return text


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _parse_member(value: object, index: int) -> GroundCaptureMemberBinding:
    raw = _exact_keys(
        value,
        frozenset({
            "member_id",
            "role",
            "receipt_id",
            "content_sha256",
            "receipt_sha256",
            "received_at",
        }),
        f"capture.members[{index}]",
    )
    member_id = _identifier(raw["member_id"], "member_id", 128)
    role = _identifier(raw["role"], "role", 128)
    if member_id != role:
        raise ValueError("schema-3 member_id must equal its unique role")
    return GroundCaptureMemberBinding(
        member_id=member_id,
        role=role,
        receipt_id=_identifier(raw["receipt_id"], "receipt_id", 96),
        content_sha256=_sha256(raw["content_sha256"], "content_sha256"),
        receipt_sha256=_sha256(raw["receipt_sha256"], "receipt_sha256"),
        received_at=_timestamp(raw["received_at"], "member.received_at"),
    )


def _parse_station(value: object) -> GroundStationSite:
    raw = _exact_keys(
        value,
        frozenset({
            "provider_station_id",
            "site_id",
            "network",
            "station_name",
            "position_state",
            "latitude_decimal",
            "longitude_decimal",
            "elevation_state",
            "elevation_m_decimal",
        }),
        "station",
    )
    position_state = _identifier(raw["position_state"], "position_state", 16)
    elevation_state = _identifier(raw["elevation_state"], "elevation_state", 16)
    if position_state not in _POSITION_STATES:
        raise ValueError("position_state is unsupported")
    if elevation_state not in _POSITION_STATES:
        raise ValueError("elevation_state is unsupported")
    latitude = raw["latitude_decimal"]
    longitude = raw["longitude_decimal"]
    elevation = raw["elevation_m_decimal"]
    if position_state == "known":
        latitude = _decimal_text(
            latitude,
            "latitude_decimal",
            minimum=Decimal("-90"),
            maximum=Decimal("90"),
        )
        longitude = _decimal_text(
            longitude,
            "longitude_decimal",
            minimum=Decimal("-180"),
            maximum=Decimal("180"),
        )
    elif latitude is not None or longitude is not None:
        raise ValueError("unknown position cannot expose coordinates")
    if elevation_state == "known":
        elevation = _decimal_text(elevation, "elevation_m_decimal")
    elif elevation is not None:
        raise ValueError("unknown elevation cannot expose a value")
    return GroundStationSite(
        provider_station_id=_identifier(
            raw["provider_station_id"], "provider_station_id", 256
        ),
        site_id=_identifier(raw["site_id"], "site_id", 256),
        network=_identifier(raw["network"], "network", 128),
        station_name=_optional_verbatim(
            raw["station_name"], "station_name", 512
        ),
        position_state=position_state,  # type: ignore[arg-type]
        latitude_decimal=latitude,
        longitude_decimal=longitude,
        elevation_state=elevation_state,  # type: ignore[arg-type]
        elevation_m_decimal=elevation,
    )


def _parse_measurement(value: object) -> GroundMeasurementValue:
    raw = _exact_keys(
        value,
        frozenset({
            "variable",
            "state",
            "representation",
            "exact_value",
            "unit",
            "censor_limit",
        }),
        "measurement",
    )
    state = _identifier(raw["state"], "measurement.state", 32)
    representation = _identifier(
        raw["representation"], "measurement.representation", 16
    )
    if state not in _VALUE_STATES:
        raise ValueError("measurement.state is unsupported")
    if representation not in _VALUE_REPRESENTATIONS:
        raise ValueError("measurement.representation is unsupported")
    exact_value = raw["exact_value"]
    censor_limit = raw["censor_limit"]
    if state == "observed":
        if representation == "decimal":
            exact_value = _decimal_text(exact_value, "measurement.exact_value")
        elif representation == "text":
            exact_value = _verbatim(
                exact_value,
                "measurement.exact_value",
                1024,
                empty=False,
            )
        else:
            raise ValueError("observed value requires decimal or text representation")
        if censor_limit is not None:
            raise ValueError("observed value cannot expose a censor limit")
    elif state in {"censored_below", "censored_above"}:
        if representation != "none" or exact_value is not None:
            raise ValueError("censored value cannot pretend to be observed")
        censor_limit = _decimal_text(
            censor_limit,
            "measurement.censor_limit",
        )
    else:
        if representation != "none" or exact_value is not None or censor_limit is not None:
            raise ValueError(
                "missing, trace and unknown states require an explicit absent value"
            )
    return GroundMeasurementValue(
        variable=_identifier(raw["variable"], "measurement.variable", 128),
        state=state,  # type: ignore[arg-type]
        representation=representation,  # type: ignore[arg-type]
        exact_value=exact_value,
        unit=_identifier(raw["unit"], "measurement.unit", 128),
        censor_limit=censor_limit,
    )


def _parse_qc(value: object, index: int) -> ProviderQcFact:
    raw = _exact_keys(
        value,
        frozenset({"flag", "value", "text"}),
        f"provider_qc[{index}]",
    )
    return ProviderQcFact(
        flag=_identifier(raw["flag"], "provider_qc.flag", 128),
        value=_optional_verbatim(raw["value"], "provider_qc.value", 512),
        text=_optional_verbatim(raw["text"], "provider_qc.text", 2048),
    )


def parse_ground_observation(value: object) -> GroundObservationFact:
    """Parse one exact closed fact contract; unknown keys fail closed."""
    raw = _exact_keys(
        value,
        frozenset({
            "contract",
            "operational_admission_eligible",
            "admission_status",
            "capture",
            "source",
            "decoder",
            "provider",
            "product",
            "station",
            "time",
            "measurement",
            "provider_qc",
        }),
        "ground observation",
    )
    if raw["contract"] != GROUND_EVIDENCE_CONTRACT:
        raise ValueError("unsupported ground evidence contract")
    if raw["operational_admission_eligible"] is not False:
        raise ValueError("canonical ground facts are candidate-only and unadmitted")
    if raw["admission_status"] != ADMISSION_STATUS:
        raise ValueError("canonical ground facts cannot claim admission")
    capture = _exact_keys(
        raw["capture"],
        frozenset({"capture_id", "capture_receipt_sha256", "members"}),
        "capture",
    )
    raw_members = capture["members"]
    if not isinstance(raw_members, list):
        raise ValueError("capture.members must be a list")
    if not 1 <= len(raw_members) <= HARD_MAX_CAPTURE_MEMBERS:
        raise ValueError("capture.members count is outside bounds")
    members = tuple(
        sorted(
            (_parse_member(item, index) for index, item in enumerate(raw_members)),
            key=lambda member: member.member_id,
        )
    )
    if (
        len({item.member_id for item in members}) != len(members)
        or len({item.receipt_id for item in members}) != len(members)
    ):
        raise ValueError("capture members are not uniquely bound")
    source = _exact_keys(
        raw["source"],
        frozenset({
            "member_role",
            "member_content_sha256",
            "member_receipt_sha256",
            "record_locator",
            "record_sha256",
        }),
        "source",
    )
    source_member_role = _identifier(
        source["member_role"], "source.member_role", 128
    )
    if source_member_role not in {member.role for member in members}:
        raise ValueError("source.member_role is not in the complete capture")
    selected_member = next(
        member for member in members if member.role == source_member_role
    )
    source_member_content_sha256 = _sha256(
        source["member_content_sha256"], "source.member_content_sha256"
    )
    source_member_receipt_sha256 = _sha256(
        source["member_receipt_sha256"], "source.member_receipt_sha256"
    )
    if (
        source_member_content_sha256 != selected_member.content_sha256
        or source_member_receipt_sha256 != selected_member.receipt_sha256
    ):
        raise ValueError("source member hashes do not match its capture binding")
    decoder = _exact_keys(
        raw["decoder"],
        frozenset({"identity", "version", "config_sha256"}),
        "decoder",
    )
    timing = _exact_keys(
        raw["time"],
        frozenset({
            "observed_at",
            "representative_interval_start",
            "representative_interval_end",
            "received_at",
            "ingested_at",
        }),
        "time",
    )
    observed_at = _timestamp(timing["observed_at"], "time.observed_at")
    interval_start = _timestamp(
        timing["representative_interval_start"],
        "time.representative_interval_start",
    )
    interval_end = _timestamp(
        timing["representative_interval_end"],
        "time.representative_interval_end",
    )
    received_at = _timestamp(timing["received_at"], "time.received_at")
    ingested_at = _timestamp(timing["ingested_at"], "time.ingested_at")
    if interval_start > observed_at or observed_at > interval_end:
        raise ValueError("observed_at must lie inside its representative interval")
    if ingested_at < received_at:
        raise ValueError("ingested_at cannot predate received_at")
    raw_qc = raw["provider_qc"]
    if not isinstance(raw_qc, list):
        raise ValueError("provider_qc must be a list")
    if len(raw_qc) > HARD_MAX_QC_FLAGS:
        raise ValueError("provider_qc count is outside bounds")
    fact = GroundObservationFact(
        capture_id=_identifier(capture["capture_id"], "capture_id", 72),
        capture_receipt_sha256=_sha256(
            capture["capture_receipt_sha256"],
            "capture_receipt_sha256",
        ),
        members=members,
        source_member_role=source_member_role,
        source_member_content_sha256=source_member_content_sha256,
        source_member_receipt_sha256=source_member_receipt_sha256,
        source_record_locator=_identifier(
            source["record_locator"], "source.record_locator", 1024
        ),
        source_record_sha256=_sha256(
            source["record_sha256"], "source.record_sha256"
        ),
        decoder_identity=_identifier(
            decoder["identity"], "decoder.identity", 256
        ),
        decoder_version=_identifier(
            decoder["version"], "decoder.version", 128
        ),
        config_sha256=_sha256(
            decoder["config_sha256"], "decoder.config_sha256"
        ),
        provider=_identifier(raw["provider"], "provider", 128),
        product=_identifier(raw["product"], "product", 256),
        station=_parse_station(raw["station"]),
        observed_at=observed_at,
        representative_interval_start=interval_start,
        representative_interval_end=interval_end,
        received_at=received_at,
        ingested_at=ingested_at,
        measurement=_parse_measurement(raw["measurement"]),
        provider_qc=tuple(_parse_qc(item, index) for index, item in enumerate(raw_qc)),
    )
    if not _CAPTURE_ID_RE.fullmatch(fact.capture_id):
        raise ValueError("capture_id is not a schema-3 capture identity")
    encoded = _canonical_json(fact.to_mapping())
    if len(encoded) > HARD_MAX_OPERATION_BYTES:
        raise ValueError("ground fact exceeds the hard canonical byte bound")
    return fact


def _validated_fact(
    fact: GroundObservationFact | Mapping[str, Any],
) -> GroundObservationFact:
    return parse_ground_observation(
        fact.to_mapping() if isinstance(fact, GroundObservationFact) else fact
    )


def _observation_key_for(fact: GroundObservationFact) -> str:
    document = {
        "capture_id": fact.capture_id,
        "capture_receipt_sha256": fact.capture_receipt_sha256,
        "config_sha256": fact.config_sha256,
        "decoder_identity": fact.decoder_identity,
        "decoder_version": fact.decoder_version,
        "observed_at": fact.observed_at,
        "product": fact.product,
        "provider": fact.provider,
        "provider_station_id": fact.station.provider_station_id,
        "representative_interval_end": fact.representative_interval_end,
        "representative_interval_start": fact.representative_interval_start,
        "schema": GROUND_OBSERVATION_IDENTITY_SCHEMA,
        "site_id": fact.station.site_id,
        "source_member_role": fact.source_member_role,
        "source_member_content_sha256": fact.source_member_content_sha256,
        "source_member_receipt_sha256": fact.source_member_receipt_sha256,
        "source_record_locator": fact.source_record_locator,
        "source_record_sha256": fact.source_record_sha256,
        "variable": fact.measurement.variable,
    }
    return "ground-observation-" + hashlib.sha256(_canonical_json(document)).hexdigest()


def ground_observation_key(
    fact: GroundObservationFact | Mapping[str, Any],
) -> str:
    return _observation_key_for(_validated_fact(fact))


def _fact_sha_for(fact: GroundObservationFact) -> str:
    return hashlib.sha256(_canonical_json(fact.to_mapping())).hexdigest()


def ground_fact_sha256(
    fact: GroundObservationFact | Mapping[str, Any],
) -> str:
    return _fact_sha_for(_validated_fact(fact))


def _fact_id_for(fact: GroundObservationFact) -> str:
    document = {
        "fact_sha256": _fact_sha_for(fact),
        "observation_key": _observation_key_for(fact),
        "schema": GROUND_FACT_IDENTITY_SCHEMA,
    }
    return "ground-fact-" + hashlib.sha256(_canonical_json(document)).hexdigest()


def ground_fact_id(
    fact: GroundObservationFact | Mapping[str, Any],
) -> str:
    return _fact_id_for(_validated_fact(fact))


def _normalize_schema_sql(value: str) -> str:
    source = value.strip()
    if source.endswith(";"):
        source = source[:-1].rstrip()
    result: list[str] = []
    quote: str | None = None
    pending_space = False
    index = 0
    while index < len(source):
        character = source[index]
        if quote is not None:
            result.append(character)
            closing = "]" if quote == "[" else quote
            if character == closing:
                if index + 1 < len(source) and source[index + 1] == closing:
                    index += 1
                    result.append(source[index])
                else:
                    quote = None
            index += 1
            continue
        if character.isspace():
            pending_space = True
            index += 1
            continue
        if pending_space and result:
            result.append(" ")
        pending_space = False
        result.append(character)
        if character in ("'", '"', "`", "["):
            quote = character
        index += 1
    return "".join(result)


_TABLE_DDL: tuple[tuple[str, str], ...] = (
    (
        "manifest",
        """CREATE TABLE manifest(
            key TEXT PRIMARY KEY,value TEXT NOT NULL
        ) WITHOUT ROWID""",
    ),
    (
        "ground_facts",
        """CREATE TABLE ground_facts(
            fact_id TEXT PRIMARY KEY,observation_key TEXT NOT NULL UNIQUE,
            fact_sha256 TEXT NOT NULL,receipt_sha256 TEXT NOT NULL,
            canonical_fact BLOB NOT NULL,canonical_fact_bytes INTEGER NOT NULL,
            capture_id TEXT NOT NULL,capture_receipt_sha256 TEXT NOT NULL,
            capture_manifest_sha256 TEXT NOT NULL,
            source_member_role TEXT NOT NULL,source_member_content_sha256 TEXT NOT NULL,
            source_member_receipt_sha256 TEXT NOT NULL,
            source_record_locator TEXT NOT NULL,source_record_sha256 TEXT NOT NULL,
            decoder_identity TEXT NOT NULL,decoder_version TEXT NOT NULL,
            config_sha256 TEXT NOT NULL,provider TEXT NOT NULL,product TEXT NOT NULL,
            provider_station_id TEXT NOT NULL,site_id TEXT NOT NULL,
            network TEXT NOT NULL,station_name TEXT,
            position_state TEXT NOT NULL,latitude_decimal TEXT,
            longitude_decimal TEXT,elevation_state TEXT NOT NULL,
            elevation_m_decimal TEXT,
            observed_at REAL NOT NULL,interval_start REAL NOT NULL,
            interval_end REAL NOT NULL,received_at REAL NOT NULL,
            ingested_at REAL NOT NULL,variable TEXT NOT NULL,
            value_state TEXT NOT NULL,value_representation TEXT NOT NULL,
            exact_value TEXT,unit TEXT NOT NULL,censor_limit TEXT,
            member_count INTEGER NOT NULL,qc_count INTEGER NOT NULL,
            committed_at REAL NOT NULL,algorithm_version TEXT NOT NULL,
            CHECK(length(fact_id)=76),CHECK(length(observation_key)=83),
            CHECK(length(fact_sha256)=64),CHECK(length(receipt_sha256)=64),
            CHECK(length(capture_receipt_sha256)=64),
            CHECK(length(capture_manifest_sha256)=64),
            CHECK(length(source_member_content_sha256)=64),
            CHECK(length(source_member_receipt_sha256)=64),
            CHECK(length(source_record_sha256)=64),CHECK(length(config_sha256)=64),
            CHECK(canonical_fact_bytes BETWEEN 1 AND 65536),
            CHECK(member_count BETWEEN 1 AND 64),
            CHECK(qc_count BETWEEN 0 AND 64),
            CHECK(interval_start<=observed_at AND observed_at<=interval_end),
            CHECK(received_at<=ingested_at),
            CHECK(value_state IN ('observed','missing','trace','censored_below','censored_above','unknown')),
            CHECK(value_representation IN ('decimal','text','none')),
            CHECK(position_state IN ('known','unknown')),
            CHECK(elevation_state IN ('known','unknown'))
        ) WITHOUT ROWID""",
    ),
    (
        "ground_fact_members",
        """CREATE TABLE ground_fact_members(
            fact_id TEXT NOT NULL REFERENCES ground_facts(fact_id),
            member_id TEXT NOT NULL,role TEXT NOT NULL,receipt_id TEXT NOT NULL,
            content_sha256 TEXT NOT NULL,receipt_sha256 TEXT NOT NULL,
            received_at REAL NOT NULL,algorithm_version TEXT NOT NULL,
            PRIMARY KEY(fact_id,member_id),UNIQUE(fact_id,role),
            UNIQUE(fact_id,receipt_id),CHECK(member_id=role),
            CHECK(length(content_sha256)=64),CHECK(length(receipt_sha256)=64)
        ) WITHOUT ROWID""",
    ),
    (
        "ground_fact_qc",
        """CREATE TABLE ground_fact_qc(
            fact_id TEXT NOT NULL REFERENCES ground_facts(fact_id),
            ordinal INTEGER NOT NULL,flag TEXT NOT NULL,value TEXT,text TEXT,
            algorithm_version TEXT NOT NULL,
            PRIMARY KEY(fact_id,ordinal),CHECK(ordinal BETWEEN 0 AND 63)
        ) WITHOUT ROWID""",
    ),
)

_INDEX_DDL: tuple[tuple[str, str, str], ...] = (
    (
        "ground_facts_station_time",
        "ground_facts",
        """CREATE INDEX ground_facts_station_time
            ON ground_facts(provider,provider_station_id,observed_at,fact_id)""",
    ),
    (
        "ground_facts_site_variable_time",
        "ground_facts",
        """CREATE INDEX ground_facts_site_variable_time
            ON ground_facts(site_id,variable,observed_at,fact_id)""",
    ),
    (
        "ground_facts_capture",
        "ground_facts",
        """CREATE INDEX ground_facts_capture
            ON ground_facts(capture_id,fact_id)""",
    ),
)

_TRIGGER_DDL: tuple[tuple[str, str, str], ...] = tuple(
    (
        f"{table}_no_{action}",
        table,
        f"""CREATE TRIGGER {table}_no_{action}
            BEFORE {action.upper()} ON {table} BEGIN
                SELECT RAISE(ABORT,'append-only table');
            END""",
    )
    for table in ("manifest", "ground_facts", "ground_fact_members", "ground_fact_qc")
    for action in ("update", "delete")
)

_EXPECTED_SCHEMA_OBJECTS = tuple(sorted(
    [
        ("table", name, name, _normalize_schema_sql(sql))
        for name, sql in _TABLE_DDL
    ]
    + [
        ("index", name, table, _normalize_schema_sql(sql))
        for name, table, sql in _INDEX_DDL
    ]
    + [
        ("trigger", name, table, _normalize_schema_sql(sql))
        for name, table, sql in _TRIGGER_DDL
    ]
))
_EXPECTED_SCHEMA_FINGERPRINT = hashlib.sha256(
    _canonical_json(_EXPECTED_SCHEMA_OBJECTS)
).hexdigest()


class CanonicalGroundEvidenceStore:
    """Crash-safe, append-only, candidate-only ground-fact catalog."""

    def __init__(
        self,
        path: str | Path,
        *,
        raw_artifact_reader: RawArtifactCaptureReader,
        limits: GroundEvidenceLimits | None = None,
        clock: Callable[[], float] = time.time,
        monotonic: Callable[[], float] = time.monotonic,
        fault_injector: Callable[[str], None] | None = None,
        initialization_deadline_at: float | None = None,
    ) -> None:
        supplied = Path(path)
        if not supplied.is_absolute():
            raise ValueError("canonical ground evidence path must be absolute")
        if not isinstance(raw_artifact_reader, RawArtifactCaptureReader):
            raise TypeError("raw_artifact_reader must provide load_capture_receipt")
        if limits is not None and not isinstance(limits, GroundEvidenceLimits):
            raise TypeError("limits must be GroundEvidenceLimits")
        self.path = supplied
        self._raw_artifact_reader = raw_artifact_reader
        self._limits = limits or GroundEvidenceLimits()
        self._clock = clock
        self._monotonic = monotonic
        self._fault_injector = fault_injector
        self._lock = threading.RLock()
        self.initialize(deadline_at=initialization_deadline_at)

    def initialize(self, *, deadline_at: float | None = None) -> dict[str, int]:
        deadline = self._deadline(deadline_at)
        self._check_deadline(deadline)
        with self._lock:
            self._prepare_path()
            self._check_catalog_envelope()
            try:
                with closing(self._connect_unconfigured(deadline)) as database:
                    database.execute("PRAGMA foreign_keys=ON")
                    database.execute("PRAGMA synchronous=FULL")
                    mode = str(
                        database.execute(
                            f"PRAGMA journal_mode={CATALOG_JOURNAL_MODE}"
                        ).fetchone()[0]
                    ).lower()
                    if mode != CATALOG_JOURNAL_MODE:
                        raise CanonicalGroundEvidenceIntegrityError(
                            "SQLite could not enforce DELETE rollback journaling"
                        )
                    database.execute("PRAGMA temp_store=MEMORY")
                    database.execute("BEGIN IMMEDIATE")
                    try:
                        self._fault("initialize.after_begin")
                        if not self._table_names(database):
                            self._create_schema(database)
                        self._validate_schema(database)
                        self._check_deadline(deadline)
                        self._fault("initialize.before_commit")
                        database.commit()
                        self._fault("initialize.after_commit")
                    except BaseException:
                        if database.in_transaction:
                            database.rollback()
                        raise
                with closing(self._connect(deadline)) as database:
                    self._validate_schema(database, full_integrity=True)
                self._durability_barrier()
            except sqlite3.DatabaseError as exc:
                self._raise_sqlite(exc, deadline)
        return self.stats(deadline_at=deadline)

    def commit(
        self,
        fact: GroundObservationFact | Mapping[str, Any],
        *,
        deadline_at: float | None = None,
    ) -> GroundObservationReceipt:
        """Commit one fact through the same atomic machinery as a batch."""
        deadline = self._deadline(deadline_at)
        receipts = self._commit_batch_impl(
            (fact,),
            deadline=deadline,
            fault_prefix="commit",
            enforce_batch_limits=False,
        )
        if len(receipts) != 1:
            raise CanonicalGroundEvidenceIntegrityError(
                "single ground commit returned an invalid receipt count"
            )
        return receipts[0]

    def commit_batch(
        self,
        facts: Sequence[GroundObservationFact | Mapping[str, Any]],
        *,
        deadline_at: float | None = None,
    ) -> tuple[GroundObservationReceipt, ...]:
        """Atomically commit one bounded, single-capture fact sequence.

        The raw schema-3 authority is loaded exactly once. Every fact, byte
        bound, in-batch duplicate, existing-row conflict and aggregate row
        capacity is checked before the first insert. Returned receipts follow
        the caller's order, including exact duplicate inputs.
        """
        deadline = self._deadline(deadline_at)
        return self._commit_batch_impl(
            facts,
            deadline=deadline,
            fault_prefix="commit_batch",
            enforce_batch_limits=True,
        )

    def _prepare_batch(
        self,
        facts: Sequence[GroundObservationFact | Mapping[str, Any]],
        *,
        deadline: float | None,
        enforce_batch_limits: bool,
    ) -> tuple[_PreparedGroundFact, ...]:
        self._check_deadline(deadline)
        if isinstance(facts, (str, bytes, bytearray)) or not isinstance(
            facts, Sequence
        ):
            raise TypeError("facts must be a concrete bounded sequence")
        try:
            count = len(facts)
        except (OverflowError, TypeError, ValueError) as exc:
            raise TypeError("facts must expose a finite concrete length") from exc
        maximum = self._limits.max_batch_facts if enforce_batch_limits else 1
        if count < 0 or count > maximum:
            raise CanonicalGroundEvidenceCapacityError(
                "ground batch fact-count bound exceeded"
            )
        if count == 0:
            return ()

        concrete: list[GroundObservationFact | Mapping[str, Any]] = []
        for index in range(count):
            self._check_deadline(deadline)
            try:
                concrete.append(facts[index])
            except (IndexError, KeyError, TypeError) as exc:
                raise TypeError(
                    "facts sequence changed while being materialized"
                ) from exc

        prepared: list[_PreparedGroundFact] = []
        total_bytes = 0
        authority: tuple[object, ...] | None = None
        by_observation: dict[str, _PreparedGroundFact] = {}
        by_fact_id: dict[str, _PreparedGroundFact] = {}
        for item in concrete:
            self._check_deadline(deadline)
            parsed = _validated_fact(item)
            canonical = _canonical_json(parsed.to_mapping())
            if len(canonical) > self._limits.max_operation_bytes:
                raise CanonicalGroundEvidenceCapacityError(
                    "ground fact exceeds configured canonical byte bound"
                )
            total_bytes += len(canonical)
            if (
                enforce_batch_limits
                and total_bytes > self._limits.max_batch_canonical_bytes
            ):
                raise CanonicalGroundEvidenceCapacityError(
                    "ground batch canonical-byte bound exceeded"
                )
            observation_key = _observation_key_for(parsed)
            fact_sha256 = _fact_sha_for(parsed)
            fact_id = _fact_id_for(parsed)
            current = _PreparedGroundFact(
                fact=parsed,
                canonical=canonical,
                observation_key=observation_key,
                fact_sha256=fact_sha256,
                fact_id=fact_id,
            )
            current_authority = (
                parsed.capture_id,
                parsed.capture_receipt_sha256,
                parsed.provider,
                parsed.product,
                parsed.members,
            )
            if authority is None:
                authority = current_authority
            elif current_authority != authority:
                raise GroundCaptureVerificationError(
                    "ground batch facts do not share one exact raw authority"
                )

            previous = by_observation.get(observation_key)
            if previous is not None and (
                previous.fact_id != fact_id
                or previous.fact_sha256 != fact_sha256
                or previous.canonical != canonical
            ):
                raise CanonicalGroundEvidenceConflict(
                    "batch observation identity was reused for different facts"
                )
            previous_id = by_fact_id.get(fact_id)
            if previous_id is not None and (
                previous_id.observation_key != observation_key
                or previous_id.canonical != canonical
            ):
                raise CanonicalGroundEvidenceConflict(
                    "batch fact identity has different observation content"
                )
            by_observation.setdefault(observation_key, current)
            by_fact_id.setdefault(fact_id, current)
            prepared.append(current)
        self._check_deadline(deadline)
        return tuple(prepared)

    def _commit_batch_impl(
        self,
        facts: Sequence[GroundObservationFact | Mapping[str, Any]],
        *,
        deadline: float | None,
        fault_prefix: str,
        enforce_batch_limits: bool,
    ) -> tuple[GroundObservationReceipt, ...]:
        prepared = self._prepare_batch(
            facts,
            deadline=deadline,
            enforce_batch_limits=enforce_batch_limits,
        )
        if not prepared:
            return ()

        # One and only one raw-authority read for the complete batch.
        capture = self._verify_capture(prepared[0].fact, deadline)
        for item in prepared[1:]:
            self._check_deadline(deadline)
            self._validate_fact_capture_binding(item.fact, capture)
        self._check_deadline(deadline)

        unique: list[_PreparedGroundFact] = []
        seen_observations: set[str] = set()
        for item in prepared:
            if item.observation_key not in seen_observations:
                seen_observations.add(item.observation_key)
                unique.append(item)

        receipts_by_observation: dict[str, GroundObservationReceipt] = {}
        with self._lock, closing(self._connect(deadline)) as database:
            try:
                database.execute("BEGIN IMMEDIATE")
                self._validate_schema(database)
                self._check_deadline(deadline)
                self._fault(f"{fault_prefix}.after_begin")

                pending: list[_PreparedGroundFact] = []
                for item in unique:
                    self._check_deadline(deadline)
                    existing = database.execute(
                        "SELECT * FROM ground_facts WHERE observation_key=?",
                        (item.observation_key,),
                    ).fetchone()
                    if existing is not None:
                        if (
                            str(existing["fact_id"]) != item.fact_id
                            or str(existing["fact_sha256"]) != item.fact_sha256
                            or bytes(existing["canonical_fact"]) != item.canonical
                        ):
                            raise CanonicalGroundEvidenceConflict(
                                "observation identity was reused for different facts"
                            )
                        receipt = self._validated_receipt(database, existing)
                        self._validate_capture_snapshot(receipt, capture)
                        receipts_by_observation[item.observation_key] = receipt
                        continue
                    if database.execute(
                        "SELECT 1 FROM ground_facts WHERE fact_id=?",
                        (item.fact_id,),
                    ).fetchone() is not None:
                        raise CanonicalGroundEvidenceConflict(
                            "ground fact identity has a different observation identity"
                        )
                    pending.append(item)

                self._enforce_batch_capacity(
                    database, tuple(item.fact for item in pending)
                )
                committed_at: float | None = None
                if pending:
                    committed_at = _timestamp(self._clock(), "committed_at")
                    if committed_at < max(
                        item.fact.ingested_at for item in pending
                    ):
                        raise ValueError("committed_at cannot predate ingested_at")

                for item in pending:
                    self._check_deadline(deadline)
                    self._fault(f"{fault_prefix}.before_fact")
                    if committed_at is None:
                        raise CanonicalGroundEvidenceIntegrityError(
                            "new batch fact has no commit timestamp"
                        )
                    receipt = self._insert_prepared_fact(
                        database,
                        item,
                        capture=capture,
                        committed_at=committed_at,
                        fault_prefix=fault_prefix,
                    )
                    receipts_by_observation[item.observation_key] = receipt
                    self._fault(f"{fault_prefix}.after_batch_fact")

                self._check_deadline(deadline)
                self._fault(f"{fault_prefix}.before_commit")
                database.commit()
                self._fault(f"{fault_prefix}.after_sqlite_commit")
            except CanonicalGroundEvidenceError:
                if database.in_transaction:
                    database.rollback()
                raise
            except sqlite3.DatabaseError as exc:
                if database.in_transaction:
                    database.rollback()
                self._raise_sqlite(exc, deadline)
            except BaseException:
                if database.in_transaction:
                    database.rollback()
                raise

        # Exactly one barrier for new, mixed, and all-existing exact retries.
        self._durability_barrier()
        self._fault(f"{fault_prefix}.after_commit")
        try:
            return tuple(
                receipts_by_observation[item.observation_key]
                for item in prepared
            )
        except KeyError as exc:
            raise CanonicalGroundEvidenceIntegrityError(
                "ground batch lost an ordered receipt"
            ) from exc

    def _insert_prepared_fact(
        self,
        database: sqlite3.Connection,
        prepared: _PreparedGroundFact,
        *,
        capture: _VerifiedCapture,
        committed_at: float,
        fault_prefix: str,
    ) -> GroundObservationReceipt:
        parsed = prepared.fact
        station = parsed.station
        measurement = parsed.measurement
        receipt_sha256 = self._receipt_sha256(
            fact_id=prepared.fact_id,
            observation_key=prepared.observation_key,
            fact_sha256=prepared.fact_sha256,
            capture_manifest_sha256=capture.manifest_sha256,
            capture_receipt_sha256=capture.capture_receipt_sha256,
            committed_at=committed_at,
        )
        database.execute(
            """
            INSERT INTO ground_facts(
                fact_id,observation_key,fact_sha256,receipt_sha256,
                canonical_fact,canonical_fact_bytes,capture_id,
                capture_receipt_sha256,capture_manifest_sha256,
                source_member_role,source_member_content_sha256,
                source_member_receipt_sha256,source_record_locator,
                source_record_sha256,
                decoder_identity,decoder_version,config_sha256,provider,product,
                provider_station_id,site_id,network,station_name,
                position_state,latitude_decimal,longitude_decimal,
                elevation_state,elevation_m_decimal,observed_at,interval_start,
                interval_end,received_at,ingested_at,variable,value_state,
                value_representation,exact_value,unit,censor_limit,
                member_count,qc_count,committed_at,algorithm_version
            ) VALUES(
                :fact_id,:observation_key,:fact_sha256,:receipt_sha256,
                :canonical_fact,:canonical_fact_bytes,:capture_id,
                :capture_receipt_sha256,:capture_manifest_sha256,
                :source_member_role,:source_member_content_sha256,
                :source_member_receipt_sha256,:source_record_locator,
                :source_record_sha256,
                :decoder_identity,:decoder_version,:config_sha256,:provider,:product,
                :provider_station_id,:site_id,:network,:station_name,
                :position_state,:latitude_decimal,:longitude_decimal,
                :elevation_state,:elevation_m_decimal,:observed_at,:interval_start,
                :interval_end,:received_at,:ingested_at,:variable,:value_state,
                :value_representation,:exact_value,:unit,:censor_limit,
                :member_count,:qc_count,:committed_at,:algorithm_version
            )
            """,
            {
                "fact_id": prepared.fact_id,
                "observation_key": prepared.observation_key,
                "fact_sha256": prepared.fact_sha256,
                "receipt_sha256": receipt_sha256,
                "canonical_fact": sqlite3.Binary(prepared.canonical),
                "canonical_fact_bytes": len(prepared.canonical),
                "capture_id": parsed.capture_id,
                "capture_receipt_sha256": parsed.capture_receipt_sha256,
                "capture_manifest_sha256": capture.manifest_sha256,
                "source_member_role": parsed.source_member_role,
                "source_member_content_sha256": (
                    parsed.source_member_content_sha256
                ),
                "source_member_receipt_sha256": (
                    parsed.source_member_receipt_sha256
                ),
                "source_record_locator": parsed.source_record_locator,
                "source_record_sha256": parsed.source_record_sha256,
                "decoder_identity": parsed.decoder_identity,
                "decoder_version": parsed.decoder_version,
                "config_sha256": parsed.config_sha256,
                "provider": parsed.provider,
                "product": parsed.product,
                "provider_station_id": station.provider_station_id,
                "site_id": station.site_id,
                "network": station.network,
                "station_name": station.station_name,
                "position_state": station.position_state,
                "latitude_decimal": station.latitude_decimal,
                "longitude_decimal": station.longitude_decimal,
                "elevation_state": station.elevation_state,
                "elevation_m_decimal": station.elevation_m_decimal,
                "observed_at": parsed.observed_at,
                "interval_start": parsed.representative_interval_start,
                "interval_end": parsed.representative_interval_end,
                "received_at": parsed.received_at,
                "ingested_at": parsed.ingested_at,
                "variable": measurement.variable,
                "value_state": measurement.state,
                "value_representation": measurement.representation,
                "exact_value": measurement.exact_value,
                "unit": measurement.unit,
                "censor_limit": measurement.censor_limit,
                "member_count": len(parsed.members),
                "qc_count": len(parsed.provider_qc),
                "committed_at": committed_at,
                "algorithm_version": STORE_ALGORITHM_VERSION,
            },
        )
        self._fault(f"{fault_prefix}.after_fact")
        database.executemany(
            """INSERT INTO ground_fact_members(
                fact_id,member_id,role,receipt_id,content_sha256,
                receipt_sha256,received_at,algorithm_version
            ) VALUES(?,?,?,?,?,?,?,?)""",
            [
                (
                    prepared.fact_id,
                    member.member_id,
                    member.role,
                    member.receipt_id,
                    member.content_sha256,
                    member.receipt_sha256,
                    member.received_at,
                    STORE_ALGORITHM_VERSION,
                )
                for member in parsed.members
            ],
        )
        self._fault(f"{fault_prefix}.after_members")
        database.executemany(
            """INSERT INTO ground_fact_qc(
                fact_id,ordinal,flag,value,text,algorithm_version
            ) VALUES(?,?,?,?,?,?)""",
            [
                (
                    prepared.fact_id,
                    ordinal,
                    item.flag,
                    item.value,
                    item.text,
                    STORE_ALGORITHM_VERSION,
                )
                for ordinal, item in enumerate(parsed.provider_qc)
            ],
        )
        self._fault(f"{fault_prefix}.after_qc")
        return GroundObservationReceipt(
            fact_id=prepared.fact_id,
            observation_key=prepared.observation_key,
            fact_sha256=prepared.fact_sha256,
            receipt_sha256=receipt_sha256,
            capture_manifest_sha256=capture.manifest_sha256,
            committed_at=committed_at,
            fact=parsed,
        )

    def load(
        self,
        fact_id: str,
        *,
        deadline_at: float | None = None,
    ) -> GroundObservationReceipt | None:
        if not isinstance(fact_id, str) or not _FACT_ID_RE.fullmatch(fact_id):
            raise ValueError("fact_id is invalid")
        deadline = self._deadline(deadline_at)
        self._check_deadline(deadline)
        with self._lock, closing(self._connect_read_only(deadline)) as database:
            self._validate_schema(database)
            row = database.execute(
                "SELECT * FROM ground_facts WHERE fact_id=?", (fact_id,)
            ).fetchone()
            if row is None:
                return None
            receipt = self._validated_receipt(database, row)
        capture = self._verify_capture(receipt.fact, deadline)
        self._validate_capture_snapshot(receipt, capture)
        self._check_deadline(deadline)
        return receipt

    def load_observation(
        self,
        observation_key: str,
        *,
        deadline_at: float | None = None,
    ) -> GroundObservationReceipt | None:
        if (
            not isinstance(observation_key, str)
            or not _OBSERVATION_KEY_RE.fullmatch(observation_key)
        ):
            raise ValueError("observation_key is invalid")
        deadline = self._deadline(deadline_at)
        with self._lock, closing(self._connect_read_only(deadline)) as database:
            row = database.execute(
                "SELECT fact_id FROM ground_facts WHERE observation_key=?",
                (observation_key,),
            ).fetchone()
        return None if row is None else self.load(
            str(row["fact_id"]), deadline_at=deadline
        )

    def query(
        self,
        *,
        provider: str | None = None,
        provider_station_id: str | None = None,
        site_id: str | None = None,
        variable: str | None = None,
        observed_from: float | None = None,
        observed_through: float | None = None,
        limit: int = 100,
        deadline_at: float | None = None,
    ) -> tuple[GroundObservationReceipt, ...]:
        """Return a strictly bounded, raw-reverified candidate fact page."""
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
            raise ValueError("query limit must be a positive integer")
        if limit > self._limits.max_query_rows:
            raise CanonicalGroundEvidenceCapacityError(
                "query row bound exceeded"
            )
        clauses: list[str] = []
        parameters: list[Any] = []
        for column, value, maximum in (
            ("provider", provider, 128),
            ("provider_station_id", provider_station_id, 256),
            ("site_id", site_id, 256),
            ("variable", variable, 128),
        ):
            if value is not None:
                clauses.append(f"{column}=?")
                parameters.append(_identifier(value, column, maximum))
        start = None if observed_from is None else _timestamp(observed_from, "observed_from")
        end = None if observed_through is None else _timestamp(
            observed_through, "observed_through"
        )
        if start is not None and end is not None and start > end:
            raise ValueError("query time range is inverted")
        if start is not None:
            clauses.append("observed_at>=?")
            parameters.append(start)
        if end is not None:
            clauses.append("observed_at<=?")
            parameters.append(end)
        parameters.append(limit)
        where = "" if not clauses else " WHERE " + " AND ".join(clauses)
        deadline = self._deadline(deadline_at)
        self._check_deadline(deadline)
        with self._lock, closing(self._connect_read_only(deadline)) as database:
            rows = database.execute(
                "SELECT fact_id FROM ground_facts"
                + where
                + " ORDER BY observed_at DESC,fact_id LIMIT ?",
                parameters,
            ).fetchall()
        receipts: list[GroundObservationReceipt] = []
        for row in rows:
            self._check_deadline(deadline)
            receipt = self.load(str(row["fact_id"]), deadline_at=deadline)
            if receipt is None:
                raise CanonicalGroundEvidenceIntegrityError(
                    "bounded query lost an append-only fact"
                )
            receipts.append(receipt)
        return tuple(receipts)

    def stats(self, *, deadline_at: float | None = None) -> dict[str, int]:
        deadline = self._deadline(deadline_at)
        with self._lock, closing(self._connect_read_only(deadline)) as database:
            self._validate_schema(database)
            row = database.execute(
                "SELECT (SELECT count(*) FROM ground_facts),"
                "(SELECT count(*) FROM ground_fact_members),"
                "(SELECT count(*) FROM ground_fact_qc)"
            ).fetchone()
            page_size = int(database.execute("PRAGMA page_size").fetchone()[0])
            page_count = int(database.execute("PRAGMA page_count").fetchone()[0])
        return {
            "ground_facts": int(row[0]),
            "member_links": int(row[1]),
            "qc_rows": int(row[2]),
            "database_page_bytes": page_size * page_count,
            "max_main_database_bytes": self._main_database_byte_limit(
                page_size
            ),
            "catalog_file_bytes": self._catalog_physical_bytes(),
            "max_catalog_bytes": self._limits.max_catalog_bytes,
            "max_observations": self._limits.max_observations,
            "max_query_rows": self._limits.max_query_rows,
            "max_batch_facts": self._limits.max_batch_facts,
            "max_batch_canonical_bytes": (
                self._limits.max_batch_canonical_bytes
            ),
            "directory_fsync_supported": int(DIRECTORY_FSYNC_SUPPORTED),
        }

    def audit(
        self,
        *,
        deadline_at: float | None = None,
    ) -> dict[str, Any]:
        deadline = self._deadline(deadline_at)
        issues: list[str] = []
        checked = 0
        try:
            with self._lock, closing(self._connect_read_only(deadline)) as database:
                self._validate_schema(database, full_integrity=True)
                total = int(
                    database.execute("SELECT count(*) FROM ground_facts").fetchone()[0]
                )
                if total > self._limits.max_observations:
                    raise CanonicalGroundEvidenceCapacityError(
                        "ground fact row bound exceeded"
                    )
                cursor = database.execute(
                    "SELECT fact_id FROM ground_facts ORDER BY fact_id"
                )
                while True:
                    self._check_deadline(deadline)
                    batch = cursor.fetchmany(self._limits.max_query_rows)
                    if not batch:
                        break
                    for row in batch:
                        receipt = self.load(
                            str(row["fact_id"]), deadline_at=deadline
                        )
                        if receipt is None:
                            raise CanonicalGroundEvidenceIntegrityError(
                                "audit lost an append-only fact"
                            )
                        checked += 1
        except CanonicalGroundEvidenceDeadlineError:
            raise
        except (
            CanonicalGroundEvidenceError,
            sqlite3.DatabaseError,
            OSError,
            TypeError,
            ValueError,
        ) as exc:
            issues.append(str(exc))
        return {"healthy": not issues, "checked_facts": checked, "issues": issues}

    integrity_check = audit

    def _verify_capture(
        self,
        fact: GroundObservationFact,
        deadline: float | None,
    ) -> _VerifiedCapture:
        self._check_deadline(deadline)
        try:
            view = self._raw_artifact_reader.load_capture_receipt(fact.capture_id)
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            raise GroundCaptureVerificationError(
                "schema-3 raw capture verification failed"
            ) from exc
        self._check_deadline(deadline)
        if view is None:
            raise GroundCaptureVerificationError("schema-3 raw capture is missing")
        try:
            capture_id = _identifier(view.capture_id, "raw.capture_id", 72)
            if not _CAPTURE_ID_RE.fullmatch(capture_id):
                raise ValueError("raw capture identity is invalid")
            if view.raw_store_schema_version != RAW_ARTIFACT_SCHEMA_VERSION:
                raise ValueError("raw store schema is not schema 3")
            if view.capture_schema != RAW_ARTIFACT_CAPTURE_SCHEMA:
                raise ValueError("raw capture schema is incompatible")
            if view.capture_receipt_schema != RAW_ARTIFACT_CAPTURE_RECEIPT_SCHEMA:
                raise ValueError("raw capture receipt schema is incompatible")
            if view.complete is not True:
                raise ValueError("raw capture is not complete")
            provider = _identifier(view.provider, "raw.provider", 128)
            product = _identifier(view.product, "raw.product", 256)
            fetch_id = _identifier(view.fetch_id, "raw.fetch_id", 512)
            upstream_capture_id = _identifier(
                view.upstream_capture_id, "raw.upstream_capture_id", 1024
            )
            product_timestamp = (
                None
                if view.product_timestamp is None
                else _timestamp(view.product_timestamp, "raw.product_timestamp")
            )
            manifest_sha256 = _sha256(
                view.manifest_sha256, "raw.manifest_sha256"
            )
            capture_receipt_sha256 = _sha256(
                view.capture_receipt_sha256, "raw.capture_receipt_sha256"
            )
            source_members = list(
                islice(iter(view.members), HARD_MAX_CAPTURE_MEMBERS + 1)
            )
            if not 1 <= len(source_members) <= HARD_MAX_CAPTURE_MEMBERS:
                raise ValueError("raw capture member count is invalid")
            members = tuple(
                sorted(
                    (
                        GroundCaptureMemberBinding(
                            member_id=_identifier(
                                member.member_id, "raw.member_id", 128
                            ),
                            role=_identifier(member.role, "raw.member.role", 128),
                            receipt_id=_identifier(
                                member.receipt_id, "raw.member.receipt_id", 96
                            ),
                            content_sha256=_sha256(
                                member.content_sha256,
                                "raw.member.content_sha256",
                            ),
                            receipt_sha256=_sha256(
                                member.receipt_sha256,
                                "raw.member.receipt_sha256",
                            ),
                            received_at=_timestamp(
                                member.received_at, "raw.member.received_at"
                            ),
                        )
                        for member in source_members
                    ),
                    key=lambda member: member.member_id,
                )
            )
        except (AttributeError, TypeError, ValueError) as exc:
            raise GroundCaptureVerificationError(
                "raw capture receipt view is invalid"
            ) from exc
        if any(member.member_id != member.role for member in members):
            raise GroundCaptureVerificationError(
                "raw schema-3 member identity is not its role"
            )
        if (
            len({member.member_id for member in members}) != len(members)
            or len({member.receipt_id for member in members}) != len(members)
        ):
            raise GroundCaptureVerificationError("raw capture members are not unique")
        recomputed = self._raw_capture_receipt_sha256(
            capture_id=capture_id,
            provider=provider,
            product=product,
            fetch_id=fetch_id,
            upstream_capture_id=upstream_capture_id,
            product_timestamp=product_timestamp,
            manifest_sha256=manifest_sha256,
            members=members,
        )
        if recomputed != capture_receipt_sha256:
            raise GroundCaptureVerificationError(
                "raw capture receipt digest is invalid"
            )
        capture = _VerifiedCapture(
            capture_id=capture_id,
            provider=provider,
            product=product,
            manifest_sha256=manifest_sha256,
            capture_receipt_sha256=capture_receipt_sha256,
            members=members,
        )
        self._validate_fact_capture_binding(fact, capture)
        return capture

    @staticmethod
    def _validate_fact_capture_binding(
        fact: GroundObservationFact,
        capture: _VerifiedCapture,
    ) -> None:
        if (
            fact.capture_id != capture.capture_id
            or fact.capture_receipt_sha256 != capture.capture_receipt_sha256
            or fact.members != capture.members
            or fact.provider != capture.provider
            or fact.product != capture.product
        ):
            raise GroundCaptureVerificationError(
                "ground fact does not exactly bind its raw capture"
            )
        selected = next(
            (
                member
                for member in capture.members
                if member.role == fact.source_member_role
            ),
            None,
        )
        if (
            selected is None
            or fact.received_at != selected.received_at
            or fact.source_member_content_sha256 != selected.content_sha256
            or fact.source_member_receipt_sha256 != selected.receipt_sha256
        ):
            raise GroundCaptureVerificationError(
                "selected source binding is not the exact raw member receipt"
            )
        if fact.ingested_at < max(
            member.received_at for member in capture.members
        ):
            raise GroundCaptureVerificationError(
                "ingested_at predates completion of the raw capture"
            )

    def _validated_receipt(
        self,
        database: sqlite3.Connection,
        row: sqlite3.Row,
    ) -> GroundObservationReceipt:
        try:
            canonical = bytes(row["canonical_fact"])
            if len(canonical) != int(row["canonical_fact_bytes"]):
                raise ValueError("canonical fact byte count is invalid")
            if len(canonical) > self._limits.max_operation_bytes:
                raise ValueError("canonical fact exceeds configured byte bound")
            decoded = json.loads(canonical.decode("utf-8"))
            fact = parse_ground_observation(decoded)
            if _canonical_json(fact.to_mapping()) != canonical:
                raise ValueError("canonical fact is not canonical JSON")
            fact_sha256 = _fact_sha_for(fact)
            observation_key = _observation_key_for(fact)
            fact_id = _fact_id_for(fact)
            committed_at = _timestamp(row["committed_at"], "committed_at")
            receipt_sha256 = self._receipt_sha256(
                fact_id=fact_id,
                observation_key=observation_key,
                fact_sha256=fact_sha256,
                capture_manifest_sha256=_sha256(
                    row["capture_manifest_sha256"],
                    "capture_manifest_sha256",
                ),
                capture_receipt_sha256=fact.capture_receipt_sha256,
                committed_at=committed_at,
            )
            receipt = GroundObservationReceipt(
                fact_id=fact_id,
                observation_key=observation_key,
                fact_sha256=fact_sha256,
                receipt_sha256=receipt_sha256,
                capture_manifest_sha256=str(row["capture_manifest_sha256"]),
                committed_at=committed_at,
                fact=fact,
            )
            self._validate_materialized(row, receipt)
            member_rows = database.execute(
                "SELECT * FROM ground_fact_members WHERE fact_id=? ORDER BY member_id",
                (fact_id,),
            ).fetchall()
            members = tuple(
                GroundCaptureMemberBinding(
                    member_id=str(member["member_id"]),
                    role=str(member["role"]),
                    receipt_id=str(member["receipt_id"]),
                    content_sha256=str(member["content_sha256"]),
                    receipt_sha256=str(member["receipt_sha256"]),
                    received_at=float(member["received_at"]),
                )
                for member in member_rows
            )
            if members != fact.members or any(
                str(member["algorithm_version"]) != STORE_ALGORITHM_VERSION
                for member in member_rows
            ):
                raise ValueError("ground fact member bindings are invalid")
            qc_rows = database.execute(
                "SELECT * FROM ground_fact_qc WHERE fact_id=? ORDER BY ordinal",
                (fact_id,),
            ).fetchall()
            qc = tuple(
                ProviderQcFact(
                    flag=str(item["flag"]),
                    value=None if item["value"] is None else str(item["value"]),
                    text=None if item["text"] is None else str(item["text"]),
                )
                for item in qc_rows
            )
            if (
                qc != fact.provider_qc
                or [int(item["ordinal"]) for item in qc_rows]
                != list(range(len(qc_rows)))
                or any(
                    str(item["algorithm_version"]) != STORE_ALGORITHM_VERSION
                    for item in qc_rows
                )
            ):
                raise ValueError("provider QC rows are invalid")
            return receipt
        except (
            AttributeError,
            json.JSONDecodeError,
            KeyError,
            TypeError,
            UnicodeDecodeError,
            ValueError,
        ) as exc:
            raise CanonicalGroundEvidenceIntegrityError(
                "canonical ground receipt is invalid"
            ) from exc

    @staticmethod
    def _validate_materialized(
        row: sqlite3.Row,
        receipt: GroundObservationReceipt,
    ) -> None:
        fact = receipt.fact
        station = fact.station
        measurement = fact.measurement
        expected = {
            "fact_id": receipt.fact_id,
            "observation_key": receipt.observation_key,
            "fact_sha256": receipt.fact_sha256,
            "receipt_sha256": receipt.receipt_sha256,
            "capture_id": fact.capture_id,
            "capture_receipt_sha256": fact.capture_receipt_sha256,
            "source_member_role": fact.source_member_role,
            "source_member_content_sha256": fact.source_member_content_sha256,
            "source_member_receipt_sha256": fact.source_member_receipt_sha256,
            "source_record_locator": fact.source_record_locator,
            "source_record_sha256": fact.source_record_sha256,
            "decoder_identity": fact.decoder_identity,
            "decoder_version": fact.decoder_version,
            "config_sha256": fact.config_sha256,
            "provider": fact.provider,
            "product": fact.product,
            "provider_station_id": station.provider_station_id,
            "site_id": station.site_id,
            "network": station.network,
            "station_name": station.station_name,
            "position_state": station.position_state,
            "latitude_decimal": station.latitude_decimal,
            "longitude_decimal": station.longitude_decimal,
            "elevation_state": station.elevation_state,
            "elevation_m_decimal": station.elevation_m_decimal,
            "observed_at": fact.observed_at,
            "interval_start": fact.representative_interval_start,
            "interval_end": fact.representative_interval_end,
            "received_at": fact.received_at,
            "ingested_at": fact.ingested_at,
            "variable": measurement.variable,
            "value_state": measurement.state,
            "value_representation": measurement.representation,
            "exact_value": measurement.exact_value,
            "unit": measurement.unit,
            "censor_limit": measurement.censor_limit,
            "member_count": len(fact.members),
            "qc_count": len(fact.provider_qc),
            "algorithm_version": STORE_ALGORITHM_VERSION,
        }
        for column, value in expected.items():
            actual = row[column]
            if isinstance(value, float):
                actual = float(actual)
            elif isinstance(value, int):
                actual = int(actual)
            elif value is not None:
                actual = str(actual)
            if actual != value:
                raise CanonicalGroundEvidenceIntegrityError(
                    f"materialized ground fact column {column} is invalid"
                )

    @staticmethod
    def _validate_capture_snapshot(
        receipt: GroundObservationReceipt,
        capture: _VerifiedCapture,
    ) -> None:
        if (
            receipt.capture_manifest_sha256 != capture.manifest_sha256
            or receipt.fact.capture_receipt_sha256
            != capture.capture_receipt_sha256
            or receipt.fact.members != capture.members
        ):
            raise GroundCaptureVerificationError(
                "stored ground receipt no longer matches raw authority"
            )

    def _enforce_capacity(
        self,
        database: sqlite3.Connection,
        fact: GroundObservationFact,
    ) -> None:
        self._enforce_batch_capacity(database, (fact,))

    def _enforce_batch_capacity(
        self,
        database: sqlite3.Connection,
        facts: tuple[GroundObservationFact, ...],
    ) -> None:
        if not facts:
            return
        row = database.execute(
            "SELECT (SELECT count(*) FROM ground_facts),"
            "(SELECT count(*) FROM ground_fact_members),"
            "(SELECT count(*) FROM ground_fact_qc)"
        ).fetchone()
        if int(row[0]) + len(facts) > self._limits.max_observations:
            raise CanonicalGroundEvidenceCapacityError(
                "ground fact row bound reached"
            )
        member_count = sum(len(fact.members) for fact in facts)
        if int(row[1]) + member_count > self._limits.max_member_links:
            raise CanonicalGroundEvidenceCapacityError(
                "ground member-link row bound reached"
            )
        qc_count = sum(len(fact.provider_qc) for fact in facts)
        if int(row[2]) + qc_count > self._limits.max_qc_rows:
            raise CanonicalGroundEvidenceCapacityError(
                "ground QC row bound reached"
            )

    def _limits_profile_json(self) -> str:
        return _canonical_json(self._limits.to_mapping()).decode("utf-8")

    def _limits_profile_sha256(self) -> str:
        return hashlib.sha256(
            self._limits_profile_json().encode("utf-8")
        ).hexdigest()

    def _main_database_page_limit(self, page_size: int) -> int:
        if isinstance(page_size, bool) or not isinstance(page_size, int) or page_size <= 0:
            raise CanonicalGroundEvidenceIntegrityError(
                "SQLite returned an invalid database page size"
            )
        return max(
            1,
            self._limits.max_catalog_bytes
            // MAIN_DATABASE_ENVELOPE_DIVISOR
            // page_size,
        )

    def _main_database_byte_limit(self, page_size: int) -> int:
        return self._main_database_page_limit(page_size) * page_size

    @staticmethod
    def _regular_file_size(path: Path, *, name: str) -> int:
        try:
            status = path.lstat()
        except FileNotFoundError:
            return 0
        except OSError as exc:
            raise CanonicalGroundEvidenceIntegrityError(
                f"cannot inspect canonical ground {name}"
            ) from exc
        if stat.S_ISLNK(status.st_mode) or not stat.S_ISREG(status.st_mode):
            raise CanonicalGroundEvidenceIntegrityError(
                f"canonical ground {name} is not a safe regular file"
            )
        return int(status.st_size)

    def _catalog_physical_bytes(self) -> int:
        """Return explicit main+DELETE-journal bytes; reject WAL sidecars."""
        for suffix, label in (("-wal", "WAL"), ("-shm", "shared-memory")):
            sidecar = Path(str(self.path) + suffix)
            try:
                exists = sidecar.exists() or sidecar.is_symlink()
            except OSError as exc:
                raise CanonicalGroundEvidenceIntegrityError(
                    f"cannot inspect forbidden SQLite {label} sidecar"
                ) from exc
            if exists:
                raise CanonicalGroundEvidenceIntegrityError(
                    f"forbidden SQLite {label} sidecar exists for DELETE-journal store"
                )
        main_bytes = self._regular_file_size(self.path, name="main database")
        journal_bytes = self._regular_file_size(
            Path(str(self.path) + "-journal"),
            name="rollback journal",
        )
        main_envelope = (
            self._limits.max_catalog_bytes // MAIN_DATABASE_ENVELOPE_DIVISOR
        )
        journal_envelope = self._limits.max_catalog_bytes - main_envelope
        if main_bytes > main_envelope:
            raise CanonicalGroundEvidenceCapacityError(
                "canonical ground main database exceeded its physical envelope"
            )
        if journal_bytes > journal_envelope:
            raise CanonicalGroundEvidenceCapacityError(
                "canonical ground rollback journal exceeded its physical reserve"
            )
        total = main_bytes + journal_bytes
        if total > self._limits.max_catalog_bytes:
            raise CanonicalGroundEvidenceCapacityError(
                "canonical ground catalog exceeded its physical file envelope"
            )
        return total

    def _check_catalog_envelope(self) -> None:
        self._catalog_physical_bytes()

    @staticmethod
    def _fsync_directory(path: Path) -> None:
        """Persist directory entries on POSIX; Windows has no equivalent API."""
        if not DIRECTORY_FSYNC_SUPPORTED:
            return
        descriptor: int | None = None
        try:
            flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
            descriptor = os.open(str(path), flags)
            os.fsync(descriptor)
        except OSError as exc:
            raise CanonicalGroundEvidenceIntegrityError(
                "canonical ground directory fsync failed"
            ) from exc
        finally:
            if descriptor is not None:
                try:
                    os.close(descriptor)
                except OSError as exc:
                    raise CanonicalGroundEvidenceIntegrityError(
                        "canonical ground directory descriptor close failed"
                    ) from exc

    def _durability_barrier(self) -> None:
        self._check_catalog_envelope()
        self._fsync_directory(self.path.parent)
        self._check_catalog_envelope()

    def _prepare_path(self) -> None:
        parent = self.path.parent
        missing: list[Path] = []
        probe = parent
        while not probe.exists():
            missing.append(probe)
            if probe.parent == probe:
                break
            probe = probe.parent
        parent.mkdir(parents=True, exist_ok=True)
        if parent.is_symlink() or not parent.is_dir():
            raise CanonicalGroundEvidenceIntegrityError(
                "canonical ground evidence parent is unsafe"
            )
        if self.path.exists() and (self.path.is_symlink() or not self.path.is_file()):
            raise CanonicalGroundEvidenceIntegrityError(
                "canonical ground evidence database path is unsafe"
            )
        if missing:
            for created in reversed(missing):
                self._fsync_directory(created)
                self._fsync_directory(created.parent)
        else:
            self._fsync_directory(parent)

    def _connect_unconfigured(
        self,
        deadline: float | None,
    ) -> sqlite3.Connection:
        self._check_deadline(deadline)
        self._check_catalog_envelope()
        timeout = self._sqlite_timeout(deadline)
        database = sqlite3.connect(self.path, timeout=timeout)
        database.row_factory = sqlite3.Row
        try:
            database.execute(f"PRAGMA busy_timeout={max(1, int(timeout * 1000))}")
            self._install_progress_handler(database, deadline)
            page_size = int(database.execute("PRAGMA page_size").fetchone()[0])
            max_pages = self._main_database_page_limit(page_size)
            if int(database.execute("PRAGMA page_count").fetchone()[0]) > max_pages:
                raise CanonicalGroundEvidenceCapacityError(
                    "canonical ground evidence main database exceeds its reserved envelope"
                )
            applied = int(
                database.execute(f"PRAGMA max_page_count={max_pages}").fetchone()[0]
            )
            if applied > max_pages:
                raise CanonicalGroundEvidenceCapacityError(
                    "SQLite could not enforce database byte bound"
                )
            return database
        except BaseException:
            database.close()
            raise

    def _connect(self, deadline: float | None) -> sqlite3.Connection:
        database = self._connect_unconfigured(deadline)
        try:
            database.execute("PRAGMA foreign_keys=ON")
            mode = str(
                database.execute(
                    f"PRAGMA journal_mode={CATALOG_JOURNAL_MODE}"
                ).fetchone()[0]
            ).lower()
            if mode != CATALOG_JOURNAL_MODE:
                raise CanonicalGroundEvidenceIntegrityError(
                    "SQLite could not enforce DELETE rollback journaling"
                )
            database.execute("PRAGMA synchronous=FULL")
            database.execute("PRAGMA temp_store=MEMORY")
            return database
        except BaseException:
            database.close()
            raise

    def _connect_read_only(self, deadline: float | None) -> sqlite3.Connection:
        self._check_deadline(deadline)
        if not self.path.exists() or self.path.is_symlink():
            raise CanonicalGroundEvidenceIntegrityError(
                "canonical ground evidence database is missing or unsafe"
            )
        timeout = self._sqlite_timeout(deadline)
        database = sqlite3.connect(
            self.path.resolve().as_uri() + "?mode=ro",
            uri=True,
            timeout=timeout,
        )
        database.row_factory = sqlite3.Row
        try:
            database.execute(f"PRAGMA busy_timeout={max(1, int(timeout * 1000))}")
            database.execute("PRAGMA query_only=ON")
            database.execute("PRAGMA foreign_keys=ON")
            self._install_progress_handler(database, deadline)
            page_size = int(database.execute("PRAGMA page_size").fetchone()[0])
            page_count = int(database.execute("PRAGMA page_count").fetchone()[0])
            if page_count > self._main_database_page_limit(page_size):
                raise CanonicalGroundEvidenceCapacityError(
                    "canonical ground evidence main database exceeds its reserved envelope"
                )
            self._check_catalog_envelope()
            return database
        except BaseException:
            database.close()
            raise

    @staticmethod
    def _table_names(database: sqlite3.Connection) -> set[str]:
        return {
            str(row[0])
            for row in database.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name NOT LIKE 'sqlite_%'"
            )
        }

    def _create_schema(self, database: sqlite3.Connection) -> None:
        for name, sql in _TABLE_DDL:
            database.execute(sql)
            self._fault(f"initialize.after_ddl.table.{name}")
        for name, _table, sql in _INDEX_DDL:
            database.execute(sql)
            self._fault(f"initialize.after_ddl.index.{name}")
        manifest = (
            ("schema_version", str(STORE_SCHEMA_VERSION)),
            ("contract", GROUND_EVIDENCE_CONTRACT),
            ("algorithm_version", STORE_ALGORITHM_VERSION),
            ("admission_status", ADMISSION_STATUS),
            ("sqlite_schema_sha256", _EXPECTED_SCHEMA_FINGERPRINT),
            ("limits_profile_json", self._limits_profile_json()),
            ("limits_profile_sha256", self._limits_profile_sha256()),
        )
        for key, value in manifest:
            database.execute("INSERT INTO manifest(key,value) VALUES(?,?)", (key, value))
            self._fault(f"initialize.after_manifest.{key}")
        for name, _table, sql in _TRIGGER_DDL:
            database.execute(sql)
            self._fault(f"initialize.after_ddl.trigger.{name}")

    @staticmethod
    def _schema_objects(
        database: sqlite3.Connection,
    ) -> tuple[tuple[str, str, str, str], ...]:
        rows = database.execute(
            "SELECT type,name,tbl_name,sql FROM sqlite_schema "
            "WHERE type IN ('table','index','view','trigger') "
            "AND name NOT LIKE 'sqlite_%' ORDER BY type,name"
        ).fetchall()
        return tuple(sorted(
            (
                str(row[0]),
                str(row[1]),
                str(row[2]),
                _normalize_schema_sql(str(row[3])),
            )
            for row in rows
        ))

    def _validate_schema(
        self,
        database: sqlite3.Connection,
        *,
        full_integrity: bool = False,
    ) -> None:
        if self._schema_objects(database) != _EXPECTED_SCHEMA_OBJECTS:
            raise CanonicalGroundEvidenceIntegrityError(
                "canonical ground evidence schema fingerprint is invalid"
            )
        manifest = {
            str(row[0]): str(row[1])
            for row in database.execute("SELECT key,value FROM manifest")
        }
        expected = {
            "schema_version": str(STORE_SCHEMA_VERSION),
            "contract": GROUND_EVIDENCE_CONTRACT,
            "algorithm_version": STORE_ALGORITHM_VERSION,
            "admission_status": ADMISSION_STATUS,
            "sqlite_schema_sha256": _EXPECTED_SCHEMA_FINGERPRINT,
            "limits_profile_json": self._limits_profile_json(),
            "limits_profile_sha256": self._limits_profile_sha256(),
        }
        if manifest != expected:
            raise CanonicalGroundEvidenceIntegrityError(
                "canonical ground evidence manifest or limits profile is invalid"
            )
        if full_integrity:
            result = database.execute("PRAGMA quick_check(1)").fetchone()
            if result is None or str(result[0]).lower() != "ok":
                raise CanonicalGroundEvidenceIntegrityError(
                    "canonical ground evidence quick_check failed"
                )
            if database.execute("PRAGMA foreign_key_check").fetchone() is not None:
                raise CanonicalGroundEvidenceIntegrityError(
                    "canonical ground evidence foreign key check failed"
                )

    @staticmethod
    def _receipt_sha256(
        *,
        fact_id: str,
        observation_key: str,
        fact_sha256: str,
        capture_manifest_sha256: str,
        capture_receipt_sha256: str,
        committed_at: float,
    ) -> str:
        document = {
            "capture_manifest_sha256": capture_manifest_sha256,
            "capture_receipt_sha256": capture_receipt_sha256,
            "committed_at": committed_at,
            "fact_id": fact_id,
            "fact_sha256": fact_sha256,
            "observation_key": observation_key,
            "schema": GROUND_EVIDENCE_RECEIPT_SCHEMA,
        }
        return hashlib.sha256(_canonical_json(document)).hexdigest()

    @staticmethod
    def _raw_capture_receipt_sha256(
        *,
        capture_id: str,
        provider: str,
        product: str,
        fetch_id: str,
        upstream_capture_id: str,
        product_timestamp: float | None,
        manifest_sha256: str,
        members: tuple[GroundCaptureMemberBinding, ...],
    ) -> str:
        document = {
            "capture_id": capture_id,
            "capture_schema": RAW_ARTIFACT_CAPTURE_SCHEMA,
            "complete": True,
            "fetch_id": fetch_id,
            "manifest_sha256": manifest_sha256,
            "members": [member.to_mapping() for member in members],
            "product": product,
            "product_timestamp": product_timestamp,
            "provider": provider,
            "raw_store_schema_version": RAW_ARTIFACT_SCHEMA_VERSION,
            "schema": RAW_ARTIFACT_CAPTURE_RECEIPT_SCHEMA,
            "upstream_capture_id": upstream_capture_id,
        }
        return hashlib.sha256(_canonical_json(document)).hexdigest()

    def _deadline(self, value: float | None) -> float | None:
        if value is None:
            return None
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(float(value))
        ):
            raise ValueError("deadline_at is invalid")
        return float(value)

    def _check_deadline(self, deadline: float | None) -> None:
        if deadline is not None and self._monotonic() >= deadline:
            raise CanonicalGroundEvidenceDeadlineError(
                "canonical ground evidence operation deadline elapsed"
            )

    def _sqlite_timeout(self, deadline: float | None) -> float:
        configured = float(self._limits.sqlite_busy_timeout_seconds)
        if deadline is None:
            return configured
        remaining = deadline - self._monotonic()
        if remaining <= 0.0:
            raise CanonicalGroundEvidenceDeadlineError(
                "canonical ground evidence operation deadline elapsed"
            )
        return max(0.001, min(configured, remaining))

    def _install_progress_handler(
        self,
        database: sqlite3.Connection,
        deadline: float | None,
    ) -> None:
        if deadline is not None:
            database.set_progress_handler(
                lambda: 1 if self._monotonic() >= deadline else 0,
                1000,
            )

    def _raise_sqlite(
        self,
        exc: sqlite3.DatabaseError,
        deadline: float | None,
    ) -> None:
        message = str(exc).lower()
        if deadline is not None and self._monotonic() >= deadline:
            raise CanonicalGroundEvidenceDeadlineError(
                "canonical ground evidence operation deadline elapsed"
            ) from exc
        if "full" in message or "disk is full" in message:
            raise CanonicalGroundEvidenceCapacityError(
                "canonical ground evidence database capacity was exhausted"
            ) from exc
        raise CanonicalGroundEvidenceIntegrityError(
            "canonical ground evidence SQLite operation failed closed"
        ) from exc

    def _fault(self, point: str) -> None:
        if self._fault_injector is not None:
            self._fault_injector(point)


__all__ = [
    "ADMISSION_STATUS",
    "CATALOG_JOURNAL_MODE",
    "CanonicalGroundEvidenceCapacityError",
    "CanonicalGroundEvidenceConflict",
    "CanonicalGroundEvidenceDeadlineError",
    "CanonicalGroundEvidenceError",
    "CanonicalGroundEvidenceIntegrityError",
    "CanonicalGroundEvidenceStore",
    "DEFAULT_MAX_CATALOG_BYTES",
    "DEFAULT_MAX_DATABASE_BYTES",
    "DIRECTORY_FSYNC_POLICY",
    "DIRECTORY_FSYNC_SUPPORTED",
    "GROUND_EVIDENCE_CONTRACT",
    "GROUND_EVIDENCE_RECEIPT_SCHEMA",
    "GROUND_FACT_IDENTITY_SCHEMA",
    "GROUND_OBSERVATION_IDENTITY_SCHEMA",
    "GroundCaptureMemberBinding",
    "GroundCaptureVerificationError",
    "GroundEvidenceLimits",
    "GroundMeasurementValue",
    "GroundObservationFact",
    "GroundObservationReceipt",
    "GroundStationSite",
    "GroundValueRepresentation",
    "GroundValueState",
    "HARD_MAX_BATCH_CANONICAL_BYTES",
    "HARD_MAX_BATCH_FACTS",
    "HARD_MAX_CAPTURE_MEMBERS",
    "HARD_MAX_OPERATION_BYTES",
    "HARD_MAX_QC_FLAGS",
    "LIMITS_PROFILE_SCHEMA",
    "MAIN_DATABASE_ENVELOPE_DIVISOR",
    "ProviderQcFact",
    "RawArtifactCaptureReader",
    "RawCaptureMemberReceiptView",
    "RawCaptureReceiptView",
    "STORE_ALGORITHM_VERSION",
    "STORE_SCHEMA_VERSION",
    "ground_fact_id",
    "ground_fact_sha256",
    "ground_observation_key",
    "parse_ground_observation",
]
