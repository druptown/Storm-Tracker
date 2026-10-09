"""Sealed provider-neutral completion-output reference archive.

This module is deliberately inert and unwired.  It stores only canonical,
privacy-safe completion-output documents plus their already-issued receipt
hashes.  It does not copy raw bytes, decode provider data, acknowledge a
lifecycle event, or mutate any provider hot store.

The archive is append-only.  ``plan_hot_window`` is a read-only planning
primitive for a later, separately reviewed provider adapter; it never deletes
or archives a candidate by itself.
"""

from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, Callable, ContextManager, Iterable, Mapping, Sequence
import hashlib
import hmac
import json
import math
import re
import sqlite3
import stat
import threading
import time


ARCHIVE_STORE_CONTRACT = "stv3-completion-output-reference-archive-v1"
ARCHIVE_RECORD_SCHEMA = "stv3-completion-output-reference-record-v1"
ARCHIVE_SEAL_SCHEMA = "stv3-completion-output-reference-seal-v1"
ARCHIVE_SOURCE_PROFILE_SCHEMA = "stv3-completion-output-archive-source-profile-v1"
ARCHIVE_SOURCE_CATALOG_SCHEMA = "stv3-completion-output-archive-source-catalog-v1"
ARCHIVE_SOURCE_ROW_REPROOF_SCHEMA = "stv3-completion-output-source-row-reproof-v1"
ARCHIVE_SOURCE_REPROOF_BINDING_SCHEMA = (
    "stv3-completion-output-source-reproof-binding-v1"
)
ARCHIVE_SOURCE_REPROOF_AUTHORITY_SCHEMA = (
    "stv3-completion-output-source-reproof-authority-v1"
)
ARCHIVE_LIMITS_SCHEMA = "stv3-completion-output-archive-limits-v1"
ARCHIVE_HOT_POLICY_SCHEMA = "stv3-completion-output-hot-window-policy-v1"
ARCHIVE_AUTHORITY_SCHEMA = "stv3-completion-output-reference-archive-authority-v1"
ARCHIVE_HOT_PLAN_SCHEMA = "stv3-completion-output-hot-window-plan-v1"
ARCHIVE_CAPACITY_HORIZON_SCHEMA = "stv3-completion-output-capacity-horizon-v1"
ARCHIVE_SCHEMA_VERSION = 1

_SHA = re.compile(r"^[0-9a-f]{64}$")
_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,191}$")
_EVENT_ID = re.compile(r"^completion-[0-9a-f]{64}$")
_SECRET_KEY = re.compile(
    r"(?:authorization|cookie|password|passwd|credential|secret|"
    r"api[_-]?key|access[_-]?token|refresh[_-]?token|private[_-]?key|"
    r"raw[_-]?(?:payload|bytes)|payload[_-]?bytes|(?:^|[_-])path$|"
    r"(?:^|[_-])url$|(?:^|[_-])uri$)",
    re.IGNORECASE,
)
_PRIVATE_TEXT = re.compile(
    r"(?:^[/\\]|[A-Za-z]:[\\/]|(?:https?|ftp)://|"
    r"(?:bearer|basic)\s+|private_[A-Za-z0-9_-]{8,}|"
    r"(?:password|passwd|secret|token)=)",
    re.IGNORECASE,
)
_MAX_PRIVACY_NODES = 100_000
_MAX_PRIVACY_DEPTH = 32
_MAX_STRING_BYTES = 64 * 1024
_MAX_SOURCE_PROFILES = 512
_MAX_TIMESTAMP = 32_503_680_000.0  # year 3000, bounded and platform-neutral
SOURCE_IMMUTABILITY_MODES = frozenset(
    {"append_only_sqlite_triggers", "checksum_reproof_required"}
)


class CompletionOutputArchiveError(RuntimeError):
    """Base error for the inert archive authority."""


class CompletionOutputArchiveIntegrityError(CompletionOutputArchiveError):
    """Stored bytes, schema, authority, or caller proof differ."""


class CompletionOutputArchivePrivacyError(CompletionOutputArchiveIntegrityError):
    """An archive record contains path, endpoint, credential, or raw material."""


class CompletionOutputArchiveConflict(CompletionOutputArchiveError):
    """A content-addressed identity already binds different canonical bytes."""


class CompletionOutputArchiveCapacityError(CompletionOutputArchiveError):
    """A configured row, byte, planning, or SQLite page cap was reached."""


class CompletionOutputArchiveClockError(CompletionOutputArchiveIntegrityError):
    """The commit clock moved behind the last durable archive commit."""


def _canonical(value: object) -> bytes:
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
    except (TypeError, ValueError, UnicodeError) as exc:
        raise CompletionOutputArchiveIntegrityError(
            "archive value is not canonical JSON"
        ) from exc


def _reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise CompletionOutputArchiveIntegrityError(
                "archive JSON contains duplicate keys"
            )
        result[key] = value
    return result


def _decode_canonical(value: bytes, label: str) -> Any:
    if not isinstance(value, bytes) or not value:
        raise CompletionOutputArchiveIntegrityError(f"{label} is empty")
    try:
        decoded = json.loads(
            value.decode("ascii"), object_pairs_hook=_reject_duplicates
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CompletionOutputArchiveIntegrityError(f"{label} is corrupt") from exc
    if _canonical(decoded) != value:
        raise CompletionOutputArchiveIntegrityError(f"{label} is not canonical")
    return decoded


def _digest(value: object) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _sha(value: object, label: str) -> str:
    if not isinstance(value, str) or _SHA.fullmatch(value) is None:
        raise CompletionOutputArchiveIntegrityError(f"{label} is not SHA-256")
    return value


def _identifier(value: object, label: str) -> str:
    if not isinstance(value, str) or _IDENTIFIER.fullmatch(value) is None:
        raise CompletionOutputArchiveIntegrityError(f"{label} is invalid")
    return value


def _event_id(value: object) -> str:
    if not isinstance(value, str) or _EVENT_ID.fullmatch(value) is None:
        raise CompletionOutputArchiveIntegrityError("archive event ID is invalid")
    return value


def _integer(value: object, label: str, *, minimum: int, maximum: int) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not minimum <= value <= maximum
    ):
        raise CompletionOutputArchiveIntegrityError(f"{label} is outside its bound")
    return value


def _timestamp(value: object, label: str) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        or not 0.0 <= float(value) <= _MAX_TIMESTAMP
    ):
        raise CompletionOutputArchiveIntegrityError(f"{label} is invalid")
    return float(value)


def _mapping(value: object, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise CompletionOutputArchiveIntegrityError(f"{label} is not a mapping")
    return value


def _exact(value: object, keys: set[str], label: str) -> Mapping[str, Any]:
    mapping = _mapping(value, label)
    if set(mapping) != keys or any(not isinstance(key, str) for key in mapping):
        raise CompletionOutputArchiveIntegrityError(f"{label} shape differs")
    return mapping


def _assert_public_json(value: object) -> None:
    nodes = 0

    def visit(item: object, depth: int) -> None:
        nonlocal nodes
        nodes += 1
        if nodes > _MAX_PRIVACY_NODES or depth > _MAX_PRIVACY_DEPTH:
            raise CompletionOutputArchiveCapacityError(
                "archive JSON exceeds privacy scan boundary"
            )
        if item is None or isinstance(item, bool):
            return
        if isinstance(item, int) and not isinstance(item, bool):
            return
        if isinstance(item, float):
            if not math.isfinite(item):
                raise CompletionOutputArchiveIntegrityError(
                    "archive JSON contains a non-finite number"
                )
            return
        if isinstance(item, str):
            if len(item.encode("utf-8")) > _MAX_STRING_BYTES:
                raise CompletionOutputArchiveCapacityError(
                    "archive JSON string exceeds boundary"
                )
            if _PRIVATE_TEXT.search(item):
                raise CompletionOutputArchivePrivacyError(
                    "archive JSON contains private text"
                )
            return
        if isinstance(item, list):
            for child in item:
                visit(child, depth + 1)
            return
        if isinstance(item, dict):
            for key, child in item.items():
                if not isinstance(key, str):
                    raise CompletionOutputArchiveIntegrityError(
                        "archive JSON key is not text"
                    )
                if _SECRET_KEY.search(key):
                    # This exact raw-manifest audit assertion carries no raw
                    # material: it is a closed boolean proof that the source
                    # consumer completed its bounded SHA pass.  No alias,
                    # false value, string or nested value is admitted.
                    allowed_raw_assertion = (
                        key == "raw_bytes_verified" and child is True
                    ) or (key == "raw_bytes_copied" and child is False)
                    if not allowed_raw_assertion:
                        raise CompletionOutputArchivePrivacyError(
                            "archive JSON contains a private field"
                        )
                visit(key, depth + 1)
                visit(child, depth + 1)
            return
        raise CompletionOutputArchiveIntegrityError(
            "archive JSON contains an unsupported value"
        )

    visit(value, 0)


@dataclass(frozen=True, slots=True)
class CompletionOutputArchiveSourceProfile:
    workload_id: str
    consumer_id: str
    stream_id: str
    cadence_seconds: int
    source_store_contract: str
    source_store_schema_version: int
    output_document_schema: str
    output_receipt_schema: str
    process_receipt_schema: str
    source_limits_sha256: str
    effect_contract_sha256: str
    acquisition_catalog_sha256: str
    consumer_binding_sha256: str
    source_immutability_mode: str
    source_row_reproof_contract: str

    def __post_init__(self) -> None:
        for value, label in (
            (self.workload_id, "source workload ID"),
            (self.consumer_id, "source consumer ID"),
            (self.stream_id, "source stream ID"),
            (self.source_store_contract, "source store contract"),
            (self.output_document_schema, "source output schema"),
            (self.output_receipt_schema, "source output receipt schema"),
            (self.process_receipt_schema, "source process receipt schema"),
            (self.source_row_reproof_contract, "source row reproof contract"),
        ):
            _identifier(value, label)
        if self.source_immutability_mode not in SOURCE_IMMUTABILITY_MODES:
            raise CompletionOutputArchiveIntegrityError(
                "source immutability mode is outside the closed authority"
            )
        _integer(
            self.cadence_seconds,
            "source cadence",
            minimum=1,
            maximum=86_400,
        )
        _integer(
            self.source_store_schema_version,
            "source store schema version",
            minimum=1,
            maximum=1_000_000,
        )
        for value, label in (
            (self.source_limits_sha256, "source limits SHA-256"),
            (self.effect_contract_sha256, "source effect contract SHA-256"),
            (self.acquisition_catalog_sha256, "acquisition catalog SHA-256"),
            (self.consumer_binding_sha256, "consumer binding SHA-256"),
        ):
            _sha(value, label)

    def as_dict(self) -> dict[str, Any]:
        return {
            "acquisition_catalog_sha256": self.acquisition_catalog_sha256,
            "cadence_seconds": self.cadence_seconds,
            "consumer_binding_sha256": self.consumer_binding_sha256,
            "consumer_id": self.consumer_id,
            "effect_contract_sha256": self.effect_contract_sha256,
            "output_document_schema": self.output_document_schema,
            "output_receipt_schema": self.output_receipt_schema,
            "process_receipt_schema": self.process_receipt_schema,
            "schema": ARCHIVE_SOURCE_PROFILE_SCHEMA,
            "source_limits_sha256": self.source_limits_sha256,
            "source_immutability_mode": self.source_immutability_mode,
            "source_row_reproof_contract": self.source_row_reproof_contract,
            "source_store_contract": self.source_store_contract,
            "source_store_schema_version": self.source_store_schema_version,
            "stream_id": self.stream_id,
            "workload_id": self.workload_id,
        }

    @property
    def fingerprint(self) -> str:
        return _digest(self.as_dict())


@dataclass(frozen=True, slots=True)
class CompletionOutputArchiveSourceRowReproof:
    source_profile_sha256: str
    event_id: str
    source_row_sha256: str
    source_row_bytes: int
    reproof_contract: str
    verified_at: float

    def __post_init__(self) -> None:
        _sha(self.source_profile_sha256, "source reproof profile SHA-256")
        _event_id(self.event_id)
        _sha(self.source_row_sha256, "source row SHA-256")
        _integer(
            self.source_row_bytes,
            "source row bytes",
            minimum=1,
            maximum=16 * 1024**2,
        )
        _identifier(self.reproof_contract, "source row reproof contract")
        _timestamp(self.verified_at, "source row reproof time")

    def as_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "reproof_contract": self.reproof_contract,
            "schema": ARCHIVE_SOURCE_ROW_REPROOF_SCHEMA,
            "source_profile_sha256": self.source_profile_sha256,
            "source_row_bytes": self.source_row_bytes,
            "source_row_sha256": self.source_row_sha256,
            "verified_at": float(self.verified_at),
        }

    @property
    def fingerprint(self) -> str:
        return _digest(self.as_dict())


@dataclass(frozen=True, slots=True)
class CompletionOutputArchiveSourceReproofBinding:
    source_profile_sha256: str
    resolver_id: str
    resolver_sha256: str
    source_store_identity_sha256: str
    reproof_contract: str
    resolver: Callable[
        ["CompletionOutputArchiveRecord"],
        ContextManager[CompletionOutputArchiveSourceRowReproof],
    ]

    def __post_init__(self) -> None:
        _sha(self.source_profile_sha256, "source resolver profile SHA-256")
        _identifier(self.resolver_id, "source resolver ID")
        _sha(self.resolver_sha256, "source resolver SHA-256")
        _sha(self.source_store_identity_sha256, "source store identity SHA-256")
        _identifier(self.reproof_contract, "source resolver contract")
        if not callable(self.resolver):
            raise TypeError("source reproof resolver is not callable")

    def as_dict(self) -> dict[str, Any]:
        return {
            "reproof_contract": self.reproof_contract,
            "resolver_id": self.resolver_id,
            "resolver_sha256": self.resolver_sha256,
            "schema": ARCHIVE_SOURCE_REPROOF_BINDING_SCHEMA,
            "source_profile_sha256": self.source_profile_sha256,
            "source_store_identity_sha256": self.source_store_identity_sha256,
        }

    @property
    def fingerprint(self) -> str:
        return _digest(self.as_dict())


@dataclass(frozen=True, slots=True)
class CompletionOutputArchiveSourceReproofAuthority:
    authority_id: str
    bindings: tuple[CompletionOutputArchiveSourceReproofBinding, ...]

    def __post_init__(self) -> None:
        _identifier(self.authority_id, "source reproof authority ID")
        bindings = tuple(self.bindings)
        if not 1 <= len(bindings) <= _MAX_SOURCE_PROFILES:
            raise CompletionOutputArchiveIntegrityError(
                "source reproof authority size differs"
            )
        if any(
            not isinstance(item, CompletionOutputArchiveSourceReproofBinding)
            for item in bindings
        ):
            raise TypeError("source reproof authority binding type differs")
        profile_ids = [item.source_profile_sha256 for item in bindings]
        if len(set(profile_ids)) != len(profile_ids):
            raise CompletionOutputArchiveConflict(
                "source reproof authority contains a duplicate profile"
            )
        object.__setattr__(self, "bindings", bindings)

    def as_dict(self) -> dict[str, Any]:
        return {
            "authority_id": self.authority_id,
            "bindings": [
                item.as_dict()
                for item in sorted(
                    self.bindings, key=lambda value: value.source_profile_sha256
                )
            ],
            "schema": ARCHIVE_SOURCE_REPROOF_AUTHORITY_SCHEMA,
        }

    @property
    def fingerprint(self) -> str:
        return _digest(self.as_dict())

    def binding_for(
        self, source_profile_sha256: str
    ) -> CompletionOutputArchiveSourceReproofBinding:
        exact = _sha(source_profile_sha256, "source reproof profile SHA-256")
        for binding in self.bindings:
            if hmac.compare_digest(binding.source_profile_sha256, exact):
                return binding
        raise CompletionOutputArchiveIntegrityError(
            "source reproof profile is outside the bound authority"
        )

    def open(
        self, record: "CompletionOutputArchiveRecord"
    ) -> ContextManager[CompletionOutputArchiveSourceRowReproof]:
        if not isinstance(record, CompletionOutputArchiveRecord):
            raise TypeError("source reproof record type differs")
        return self.binding_for(record.source_profile_sha256).resolver(record)


@dataclass(frozen=True, slots=True)
class CompletionOutputArchiveSourceCatalog:
    catalog_id: str
    profiles: tuple[CompletionOutputArchiveSourceProfile, ...]

    def __post_init__(self) -> None:
        _identifier(self.catalog_id, "archive source catalog ID")
        profiles = tuple(self.profiles)
        if not 1 <= len(profiles) <= _MAX_SOURCE_PROFILES:
            raise CompletionOutputArchiveIntegrityError(
                "archive source catalog size differs"
            )
        if any(
            not isinstance(profile, CompletionOutputArchiveSourceProfile)
            for profile in profiles
        ):
            raise TypeError("archive source catalog profile type differs")
        fingerprints = [profile.fingerprint for profile in profiles]
        if len(set(fingerprints)) != len(fingerprints):
            raise CompletionOutputArchiveConflict(
                "archive source catalog contains a duplicate profile"
            )
        object.__setattr__(self, "profiles", profiles)

    def as_dict(self) -> dict[str, Any]:
        return {
            "catalog_id": self.catalog_id,
            "profiles": [
                profile.as_dict()
                for profile in sorted(self.profiles, key=lambda item: item.fingerprint)
            ],
            "schema": ARCHIVE_SOURCE_CATALOG_SCHEMA,
        }

    @property
    def fingerprint(self) -> str:
        return _digest(self.as_dict())

    @property
    def by_fingerprint(
        self,
    ) -> Mapping[str, CompletionOutputArchiveSourceProfile]:
        return MappingProxyType(
            {profile.fingerprint: profile for profile in self.profiles}
        )

    def require(self, fingerprint: str) -> CompletionOutputArchiveSourceProfile:
        exact = _sha(fingerprint, "archive source profile SHA-256")
        profile = self.by_fingerprint.get(exact)
        if profile is None:
            raise CompletionOutputArchiveIntegrityError(
                "archive source profile is outside the closed catalog"
            )
        return profile


@dataclass(frozen=True, slots=True)
class CompletionOutputArchiveLimits:
    max_records: int
    max_record_bytes: int
    max_total_record_bytes: int
    max_database_bytes: int
    busy_timeout_ms: int

    def __post_init__(self) -> None:
        for value, label, minimum, maximum in (
            (self.max_records, "archive record cap", 1, 10_000_000),
            (self.max_record_bytes, "archive record byte cap", 1_024, 4 * 1024**2),
            (
                self.max_total_record_bytes,
                "archive total record byte cap",
                self.max_record_bytes,
                512 * 1024**3,
            ),
            (
                self.max_database_bytes,
                "archive database byte cap",
                64 * 1024,
                512 * 1024**3,
            ),
            (self.busy_timeout_ms, "archive busy timeout", 1, 60_000),
        ):
            _integer(value, label, minimum=minimum, maximum=maximum)
        if self.max_database_bytes < self.max_record_bytes:
            raise CompletionOutputArchiveIntegrityError(
                "archive database cap is below one record cap"
            )

    def as_dict(self) -> dict[str, Any]:
        return {
            "busy_timeout_ms": self.busy_timeout_ms,
            "max_database_bytes": self.max_database_bytes,
            "max_record_bytes": self.max_record_bytes,
            "max_records": self.max_records,
            "max_total_record_bytes": self.max_total_record_bytes,
            "schema": ARCHIVE_LIMITS_SCHEMA,
        }

    @property
    def fingerprint(self) -> str:
        return _digest(self.as_dict())


@dataclass(frozen=True, slots=True)
class CompletionOutputHotWindowPolicy:
    hot_window_seconds: int
    safety_records_per_stream: int
    max_plan_records: int

    def __post_init__(self) -> None:
        _integer(
            self.hot_window_seconds,
            "hot-window seconds",
            minimum=60,
            maximum=31 * 86_400,
        )
        _integer(
            self.safety_records_per_stream,
            "hot-window safety records",
            minimum=0,
            maximum=4_096,
        )
        _integer(
            self.max_plan_records,
            "hot-window planning cap",
            minimum=1,
            maximum=1_000_000,
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "hot_window_seconds": self.hot_window_seconds,
            "max_plan_records": self.max_plan_records,
            "safety_records_per_stream": self.safety_records_per_stream,
            "schema": ARCHIVE_HOT_POLICY_SCHEMA,
        }

    @property
    def fingerprint(self) -> str:
        return _digest(self.as_dict())

    def minimum_hot_records(
        self, profile: CompletionOutputArchiveSourceProfile
    ) -> int:
        if not isinstance(profile, CompletionOutputArchiveSourceProfile):
            raise TypeError("hot-window source profile type differs")
        return (
            math.ceil(self.hot_window_seconds / profile.cadence_seconds)
            + self.safety_records_per_stream
        )


@dataclass(frozen=True, slots=True)
class CompletionOutputArchiveRecord:
    source_profile_sha256: str
    event_id: str
    event_sha256: str
    producer_receipt_sha256: str
    workload_id: str
    consumer_id: str
    stream_id: str
    stream_sequence: int
    created_at: float
    output_sha256: str
    output_receipt_sha256: str
    process_receipt_sha256: str
    source_row_sha256: str
    source_row_bytes: int
    output_json: bytes

    def __post_init__(self) -> None:
        _sha(self.source_profile_sha256, "record source profile SHA-256")
        _event_id(self.event_id)
        for value, label in (
            (self.event_sha256, "record event SHA-256"),
            (self.producer_receipt_sha256, "record producer receipt SHA-256"),
            (self.output_sha256, "record output SHA-256"),
            (self.output_receipt_sha256, "record output receipt SHA-256"),
            (self.process_receipt_sha256, "record process receipt SHA-256"),
            (self.source_row_sha256, "record source row SHA-256"),
        ):
            _sha(value, label)
        for value, label in (
            (self.workload_id, "record workload ID"),
            (self.consumer_id, "record consumer ID"),
            (self.stream_id, "record stream ID"),
        ):
            _identifier(value, label)
        _integer(
            self.stream_sequence,
            "record stream sequence",
            minimum=0,
            maximum=2**63 - 1,
        )
        _timestamp(self.created_at, "record creation time")
        _integer(
            self.source_row_bytes,
            "record source row bytes",
            minimum=1,
            maximum=16 * 1024**2,
        )
        output = _decode_canonical(self.output_json, "record output JSON")
        if not isinstance(output, dict):
            raise CompletionOutputArchiveIntegrityError(
                "record output JSON is not an object"
            )
        _assert_public_json(output)
        actual_output_sha = hashlib.sha256(self.output_json).hexdigest()
        if not hmac.compare_digest(actual_output_sha, self.output_sha256):
            raise CompletionOutputArchiveIntegrityError(
                "record output checksum differs"
            )

    @classmethod
    def from_output(
        cls,
        *,
        source_profile_sha256: str,
        event_id: str,
        event_sha256: str,
        producer_receipt_sha256: str,
        workload_id: str,
        consumer_id: str,
        stream_id: str,
        stream_sequence: int,
        created_at: float,
        output_receipt_sha256: str,
        process_receipt_sha256: str,
        source_row_sha256: str,
        source_row_bytes: int,
        output: Mapping[str, Any],
    ) -> "CompletionOutputArchiveRecord":
        output_json = _canonical(dict(_mapping(output, "archive output")))
        return cls(
            source_profile_sha256=source_profile_sha256,
            event_id=event_id,
            event_sha256=event_sha256,
            producer_receipt_sha256=producer_receipt_sha256,
            workload_id=workload_id,
            consumer_id=consumer_id,
            stream_id=stream_id,
            stream_sequence=stream_sequence,
            created_at=created_at,
            output_sha256=hashlib.sha256(output_json).hexdigest(),
            output_receipt_sha256=output_receipt_sha256,
            process_receipt_sha256=process_receipt_sha256,
            source_row_sha256=source_row_sha256,
            source_row_bytes=source_row_bytes,
            output_json=output_json,
        )

    @property
    def output(self) -> dict[str, Any]:
        value = _decode_canonical(self.output_json, "record output JSON")
        assert isinstance(value, dict)
        return value

    def as_dict(self) -> dict[str, Any]:
        return {
            "consumer_id": self.consumer_id,
            "created_at": float(self.created_at),
            "event_id": self.event_id,
            "event_sha256": self.event_sha256,
            "output": self.output,
            "output_receipt_sha256": self.output_receipt_sha256,
            "output_sha256": self.output_sha256,
            "process_receipt_sha256": self.process_receipt_sha256,
            "producer_receipt_sha256": self.producer_receipt_sha256,
            "schema": ARCHIVE_RECORD_SCHEMA,
            "source_profile_sha256": self.source_profile_sha256,
            "source_row_bytes": self.source_row_bytes,
            "source_row_sha256": self.source_row_sha256,
            "stream_id": self.stream_id,
            "stream_sequence": self.stream_sequence,
            "workload_id": self.workload_id,
        }

    @property
    def canonical_bytes(self) -> bytes:
        return _canonical(self.as_dict())

    @property
    def record_sha256(self) -> str:
        return hashlib.sha256(self.canonical_bytes).hexdigest()

    @property
    def archive_id(self) -> str:
        return "completion-output-" + self.record_sha256

    @classmethod
    def from_mapping(cls, value: object) -> "CompletionOutputArchiveRecord":
        exact = _exact(
            value,
            {
                "consumer_id",
                "created_at",
                "event_id",
                "event_sha256",
                "output",
                "output_receipt_sha256",
                "output_sha256",
                "process_receipt_sha256",
                "producer_receipt_sha256",
                "schema",
                "source_profile_sha256",
                "source_row_bytes",
                "source_row_sha256",
                "stream_id",
                "stream_sequence",
                "workload_id",
            },
            "archive record",
        )
        if exact["schema"] != ARCHIVE_RECORD_SCHEMA:
            raise CompletionOutputArchiveIntegrityError(
                "archive record schema differs"
            )
        output_json = _canonical(dict(_mapping(exact["output"], "record output")))
        record = cls(
            source_profile_sha256=exact["source_profile_sha256"],
            event_id=exact["event_id"],
            event_sha256=exact["event_sha256"],
            producer_receipt_sha256=exact["producer_receipt_sha256"],
            workload_id=exact["workload_id"],
            consumer_id=exact["consumer_id"],
            stream_id=exact["stream_id"],
            stream_sequence=exact["stream_sequence"],
            created_at=exact["created_at"],
            output_sha256=exact["output_sha256"],
            output_receipt_sha256=exact["output_receipt_sha256"],
            process_receipt_sha256=exact["process_receipt_sha256"],
            source_row_sha256=exact["source_row_sha256"],
            source_row_bytes=exact["source_row_bytes"],
            output_json=output_json,
        )
        if record.as_dict() != dict(exact):
            raise CompletionOutputArchiveIntegrityError(
                "archive record canonical values differ"
            )
        return record


@dataclass(frozen=True, slots=True)
class CompletionOutputArchiveSeal:
    archive_id: str
    archive_authority_sha256: str
    source_profile_sha256: str
    event_id: str
    record_sha256: str
    record_bytes: int
    archived_at: float
    seal_sha256: str

    def __post_init__(self) -> None:
        _identifier(self.archive_id, "archive content ID")
        _event_id(self.event_id)
        for value, label in (
            (self.archive_authority_sha256, "archive authority SHA-256"),
            (self.source_profile_sha256, "seal source profile SHA-256"),
            (self.record_sha256, "seal record SHA-256"),
            (self.seal_sha256, "seal SHA-256"),
        ):
            _sha(value, label)
        _integer(
            self.record_bytes,
            "seal record bytes",
            minimum=1,
            maximum=4 * 1024**2,
        )
        _timestamp(self.archived_at, "seal archive time")

    def body(self) -> dict[str, Any]:
        return {
            "archive_authority_sha256": self.archive_authority_sha256,
            "archive_id": self.archive_id,
            "archived_at": float(self.archived_at),
            "event_id": self.event_id,
            "record_bytes": self.record_bytes,
            "record_sha256": self.record_sha256,
            "schema": ARCHIVE_SEAL_SCHEMA,
            "source_profile_sha256": self.source_profile_sha256,
        }

    def as_dict(self) -> dict[str, Any]:
        return {**self.body(), "seal_sha256": self.seal_sha256}


@dataclass(frozen=True, slots=True)
class CompletionOutputArchiveEntry:
    record: CompletionOutputArchiveRecord
    seal: CompletionOutputArchiveSeal


@dataclass(frozen=True, slots=True)
class CompletionOutputHotCandidate:
    source_profile_sha256: str
    event_id: str
    stream_sequence: int
    created_at: float
    source_row_sha256: str
    source_row_bytes: int

    def __post_init__(self) -> None:
        _sha(self.source_profile_sha256, "hot candidate source profile SHA-256")
        _event_id(self.event_id)
        _integer(
            self.stream_sequence,
            "hot candidate stream sequence",
            minimum=0,
            maximum=2**63 - 1,
        )
        _timestamp(self.created_at, "hot candidate creation time")
        _sha(self.source_row_sha256, "hot candidate source row SHA-256")
        _integer(
            self.source_row_bytes,
            "hot candidate source row bytes",
            minimum=1,
            maximum=16 * 1024**2,
        )

    @classmethod
    def from_record(
        cls, record: CompletionOutputArchiveRecord
    ) -> "CompletionOutputHotCandidate":
        if not isinstance(record, CompletionOutputArchiveRecord):
            raise TypeError("hot candidate source record type differs")
        return cls(
            source_profile_sha256=record.source_profile_sha256,
            event_id=record.event_id,
            stream_sequence=record.stream_sequence,
            created_at=record.created_at,
            source_row_sha256=record.source_row_sha256,
            source_row_bytes=record.source_row_bytes,
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "created_at": float(self.created_at),
            "event_id": self.event_id,
            "source_profile_sha256": self.source_profile_sha256,
            "source_row_bytes": self.source_row_bytes,
            "source_row_sha256": self.source_row_sha256,
            "stream_sequence": self.stream_sequence,
        }


@dataclass(frozen=True, slots=True)
class CompletionOutputHotWindowPlan:
    evaluated_at: float
    retained: tuple[CompletionOutputHotCandidate, ...]
    eligible_for_archive: tuple[CompletionOutputHotCandidate, ...]
    clock_rollback_detected: bool
    minimum_hot_records: Mapping[str, int]
    plan_sha256: str


@dataclass(frozen=True, slots=True)
class CompletionOutputArchiveCapacityHorizon:
    nominal_records_per_day: float
    count_cap_days: float
    worst_case_record_byte_cap_days: float
    logical_cap_days: float

    def as_dict(self) -> dict[str, Any]:
        return {
            "count_cap_days": self.count_cap_days,
            "logical_cap_days": self.logical_cap_days,
            "nominal_records_per_day": self.nominal_records_per_day,
            "schema": ARCHIVE_CAPACITY_HORIZON_SCHEMA,
            "worst_case_record_byte_cap_days": (
                self.worst_case_record_byte_cap_days
            ),
        }


def calculate_cadence_horizon(
    catalog: CompletionOutputArchiveSourceCatalog,
    limits: CompletionOutputArchiveLimits,
) -> CompletionOutputArchiveCapacityHorizon:
    if not isinstance(catalog, CompletionOutputArchiveSourceCatalog):
        raise TypeError("archive horizon catalog type differs")
    if not isinstance(limits, CompletionOutputArchiveLimits):
        raise TypeError("archive horizon limits type differs")
    rate = sum(86_400.0 / profile.cadence_seconds for profile in catalog.profiles)
    if not math.isfinite(rate) or rate <= 0.0:
        raise CompletionOutputArchiveIntegrityError(
            "archive cadence rate is invalid"
        )
    count_days = limits.max_records / rate
    byte_records = limits.max_total_record_bytes / limits.max_record_bytes
    byte_days = byte_records / rate
    return CompletionOutputArchiveCapacityHorizon(
        nominal_records_per_day=rate,
        count_cap_days=count_days,
        worst_case_record_byte_cap_days=byte_days,
        logical_cap_days=min(count_days, byte_days),
    )


def plan_hot_window(
    candidates: Iterable[CompletionOutputHotCandidate],
    *,
    catalog: CompletionOutputArchiveSourceCatalog,
    policy: CompletionOutputHotWindowPolicy,
    evaluated_at: float,
) -> CompletionOutputHotWindowPlan:
    if not isinstance(catalog, CompletionOutputArchiveSourceCatalog):
        raise TypeError("hot-window catalog type differs")
    if not isinstance(policy, CompletionOutputHotWindowPolicy):
        raise TypeError("hot-window policy type differs")
    now = _timestamp(evaluated_at, "hot-window evaluation time")
    materialized = tuple(candidates)
    if len(materialized) > policy.max_plan_records:
        raise CompletionOutputArchiveCapacityError(
            "hot-window candidate cap reached"
        )
    if any(not isinstance(item, CompletionOutputHotCandidate) for item in materialized):
        raise TypeError("hot-window candidate type differs")
    keys = [(item.source_profile_sha256, item.event_id) for item in materialized]
    if len(set(keys)) != len(keys):
        raise CompletionOutputArchiveConflict(
            "hot-window candidates contain a duplicate event"
        )
    sequence_keys = [
        (item.source_profile_sha256, item.stream_sequence)
        for item in materialized
    ]
    if len(set(sequence_keys)) != len(sequence_keys):
        raise CompletionOutputArchiveConflict(
            "hot-window candidates contain a duplicate stream sequence"
        )
    groups: dict[str, list[CompletionOutputHotCandidate]] = {}
    minimums: dict[str, int] = {}
    for item in materialized:
        profile = catalog.require(item.source_profile_sha256)
        minimums[item.source_profile_sha256] = policy.minimum_hot_records(profile)
        groups.setdefault(item.source_profile_sha256, []).append(item)
    retained: list[CompletionOutputHotCandidate] = []
    eligible: list[CompletionOutputHotCandidate] = []
    rollback = False
    cutoff = now - policy.hot_window_seconds
    for profile_sha, group in sorted(groups.items()):
        newest = sorted(
            group,
            key=lambda item: (item.stream_sequence, item.created_at, item.event_id),
            reverse=True,
        )
        minimum = minimums[profile_sha]
        for rank, item in enumerate(newest):
            if item.created_at > now:
                rollback = True
            if rank < minimum or item.created_at > cutoff:
                retained.append(item)
            else:
                eligible.append(item)
    order = lambda item: (
        item.source_profile_sha256,
        item.stream_sequence,
        item.created_at,
        item.event_id,
    )
    retained_tuple = tuple(sorted(retained, key=order))
    eligible_tuple = tuple(sorted(eligible, key=order))
    body = {
        "clock_rollback_detected": rollback,
        "eligible_for_archive": [item.as_dict() for item in eligible_tuple],
        "evaluated_at": now,
        "hot_policy_sha256": policy.fingerprint,
        "minimum_hot_records": dict(sorted(minimums.items())),
        "retained": [item.as_dict() for item in retained_tuple],
        "schema": ARCHIVE_HOT_PLAN_SCHEMA,
        "source_catalog_sha256": catalog.fingerprint,
    }
    return CompletionOutputHotWindowPlan(
        evaluated_at=now,
        retained=retained_tuple,
        eligible_for_archive=eligible_tuple,
        clock_rollback_detected=rollback,
        minimum_hot_records=MappingProxyType(dict(sorted(minimums.items()))),
        plan_sha256=_digest(body),
    )


_METADATA_DDL = """
CREATE TABLE archive_metadata(
    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
    contract TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    source_catalog_sha256 TEXT NOT NULL,
    limits_sha256 TEXT NOT NULL,
    hot_policy_sha256 TEXT NOT NULL,
    source_reproof_authority_sha256 TEXT NOT NULL,
    authority_sha256 TEXT NOT NULL,
    max_records INTEGER NOT NULL,
    max_record_bytes INTEGER NOT NULL,
    max_total_record_bytes INTEGER NOT NULL,
    max_database_bytes INTEGER NOT NULL,
    busy_timeout_ms INTEGER NOT NULL
)
""".strip()

_RECORDS_DDL = """
CREATE TABLE archive_records(
    archive_id TEXT PRIMARY KEY,
    source_profile_sha256 TEXT NOT NULL,
    event_id TEXT NOT NULL,
    event_sha256 TEXT NOT NULL,
    producer_receipt_sha256 TEXT NOT NULL,
    workload_id TEXT NOT NULL,
    consumer_id TEXT NOT NULL,
    stream_id TEXT NOT NULL,
    stream_sequence INTEGER NOT NULL CHECK(stream_sequence >= 0),
    created_at REAL NOT NULL CHECK(created_at >= 0),
    output_sha256 TEXT NOT NULL,
    output_receipt_sha256 TEXT NOT NULL,
    process_receipt_sha256 TEXT NOT NULL,
    source_row_sha256 TEXT NOT NULL,
    source_row_bytes INTEGER NOT NULL CHECK(source_row_bytes > 0),
    record_json BLOB NOT NULL,
    record_bytes INTEGER NOT NULL CHECK(record_bytes > 0),
    record_sha256 TEXT NOT NULL,
    archived_at REAL NOT NULL CHECK(archived_at >= 0),
    seal_sha256 TEXT NOT NULL
)
""".strip()

_SOURCE_EVENT_INDEX_DDL = """
CREATE UNIQUE INDEX archive_records_source_event
ON archive_records(source_profile_sha256,event_id)
""".strip()

_RECORD_SHA_INDEX_DDL = """
CREATE UNIQUE INDEX archive_records_record_sha256
ON archive_records(record_sha256)
""".strip()

_METADATA_NO_UPDATE_DDL = """
CREATE TRIGGER archive_metadata_no_update
BEFORE UPDATE ON archive_metadata
BEGIN SELECT RAISE(ABORT,'immutable'); END
""".strip()

_METADATA_NO_DELETE_DDL = """
CREATE TRIGGER archive_metadata_no_delete
BEFORE DELETE ON archive_metadata
BEGIN SELECT RAISE(ABORT,'immutable'); END
""".strip()

_RECORDS_NO_UPDATE_DDL = """
CREATE TRIGGER archive_records_no_update
BEFORE UPDATE ON archive_records
BEGIN SELECT RAISE(ABORT,'immutable'); END
""".strip()

_RECORDS_NO_DELETE_DDL = """
CREATE TRIGGER archive_records_no_delete
BEFORE DELETE ON archive_records
BEGIN SELECT RAISE(ABORT,'immutable'); END
""".strip()

_SCHEMA_OBJECTS = {
    "archive_metadata": ("table", _METADATA_DDL),
    "archive_records": ("table", _RECORDS_DDL),
    "archive_records_source_event": ("index", _SOURCE_EVENT_INDEX_DDL),
    "archive_records_record_sha256": ("index", _RECORD_SHA_INDEX_DDL),
    "archive_metadata_no_update": ("trigger", _METADATA_NO_UPDATE_DDL),
    "archive_metadata_no_delete": ("trigger", _METADATA_NO_DELETE_DDL),
    "archive_records_no_update": ("trigger", _RECORDS_NO_UPDATE_DDL),
    "archive_records_no_delete": ("trigger", _RECORDS_NO_DELETE_DDL),
}


def _normal_sql(value: str) -> str:
    return " ".join(value.casefold().split())


class CompletionOutputArchiveStore:
    """Bounded, sealed, append-only completion-output reference authority."""

    def __init__(
        self,
        path: str | Path,
        *,
        source_catalog: CompletionOutputArchiveSourceCatalog,
        source_reproof_authority: CompletionOutputArchiveSourceReproofAuthority,
        expected_source_reproof_authority_sha256: str,
        limits: CompletionOutputArchiveLimits,
        hot_policy: CompletionOutputHotWindowPolicy,
        disjoint_paths: Iterable[str | Path] = (),
        clock: Callable[[], float] = time.time,
        fault_injector: Callable[[str], None] | None = None,
    ) -> None:
        if not isinstance(source_catalog, CompletionOutputArchiveSourceCatalog):
            raise TypeError("archive source catalog type differs")
        if not isinstance(limits, CompletionOutputArchiveLimits):
            raise TypeError("archive limits type differs")
        if not isinstance(hot_policy, CompletionOutputHotWindowPolicy):
            raise TypeError("archive hot policy type differs")
        if not isinstance(
            source_reproof_authority,
            CompletionOutputArchiveSourceReproofAuthority,
        ):
            raise TypeError("source reproof authority type differs")
        expected_reproof_authority = _sha(
            expected_source_reproof_authority_sha256,
            "expected source reproof authority SHA-256",
        )
        if not hmac.compare_digest(
            source_reproof_authority.fingerprint, expected_reproof_authority
        ):
            raise CompletionOutputArchiveIntegrityError(
                "source reproof authority checksum differs"
            )
        catalog_profiles = set(source_catalog.by_fingerprint)
        authority_profiles = {
            binding.source_profile_sha256
            for binding in source_reproof_authority.bindings
        }
        if authority_profiles != catalog_profiles:
            raise CompletionOutputArchiveIntegrityError(
                "source reproof authority profile inventory differs"
            )
        for profile_sha in sorted(catalog_profiles):
            profile = source_catalog.require(profile_sha)
            binding = source_reproof_authority.binding_for(profile_sha)
            if binding.reproof_contract != profile.source_row_reproof_contract:
                raise CompletionOutputArchiveIntegrityError(
                    "source reproof binding contract differs from its profile"
                )
        if not callable(clock):
            raise TypeError("archive clock is not callable")
        if fault_injector is not None and not callable(fault_injector):
            raise TypeError("archive fault injector is not callable")
        self.path = self._canonical_path(path, "archive path")
        self.source_catalog = source_catalog
        self.source_reproof_authority = source_reproof_authority
        self.limits = limits
        self.hot_policy = hot_policy
        self._clock = clock
        self._fault_injector = fault_injector
        self._lock = threading.RLock()
        self._disjoint_paths = tuple(
            self._canonical_path(value, "disjoint path") for value in disjoint_paths
        )
        self._validate_path_aliases()
        authority = {
            "contract": ARCHIVE_STORE_CONTRACT,
            "hot_policy_sha256": hot_policy.fingerprint,
            "limits_sha256": limits.fingerprint,
            "schema": ARCHIVE_AUTHORITY_SCHEMA,
            "schema_version": ARCHIVE_SCHEMA_VERSION,
            "source_catalog_sha256": source_catalog.fingerprint,
            "source_reproof_authority_sha256": expected_reproof_authority,
        }
        self.authority_sha256 = _digest(authority)
        self.initialize()

    @staticmethod
    def _canonical_path(value: str | Path, label: str) -> Path:
        if isinstance(value, str) and (not value or "\0" in value or len(value) > 4096):
            raise CompletionOutputArchiveIntegrityError(f"{label} is invalid")
        path = Path(value)
        if not path.is_absolute():
            raise ValueError(f"{label} must be absolute")
        resolved = path.resolve(strict=False)
        if path != resolved:
            raise CompletionOutputArchiveIntegrityError(f"{label} is aliased")
        cursor = path
        while True:
            if cursor.exists() and cursor.is_symlink():
                raise CompletionOutputArchiveIntegrityError(f"{label} uses a symlink")
            parent = cursor.parent
            if parent == cursor:
                break
            cursor = parent
        return path

    def _validate_path_aliases(self) -> None:
        for other in self._disjoint_paths:
            if (
                self.path == other
                or other in self.path.parents
                or self.path in other.parents
            ):
                raise CompletionOutputArchiveIntegrityError(
                    "archive path aliases another authority"
                )
            if self.path.exists() and other.exists():
                try:
                    if self.path.samefile(other):
                        raise CompletionOutputArchiveIntegrityError(
                            "archive path aliases another authority"
                        )
                except OSError as exc:
                    raise CompletionOutputArchiveIntegrityError(
                        "archive path identity is unavailable"
                    ) from exc

    def _prepare_path(self) -> None:
        self._canonical_path(self.path, "archive path")
        self._validate_path_aliases()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if self.path.exists():
            mode = self.path.lstat().st_mode
            if self.path.is_symlink() or not stat.S_ISREG(mode):
                raise CompletionOutputArchiveIntegrityError(
                    "archive database path is unsafe"
                )

    def _fault(self, point: str) -> None:
        if self._fault_injector is not None:
            self._fault_injector(point)

    def _connect(self) -> sqlite3.Connection:
        self._prepare_path()
        database = sqlite3.connect(
            self.path,
            timeout=self.limits.busy_timeout_ms / 1000.0,
            isolation_level=None,
        )
        database.row_factory = sqlite3.Row
        try:
            database.execute("PRAGMA foreign_keys=ON")
            database.execute("PRAGMA synchronous=FULL")
            if int(database.execute("PRAGMA synchronous").fetchone()[0]) != 2:
                raise CompletionOutputArchiveIntegrityError(
                    "archive synchronous mode differs"
                )
            journal = str(
                database.execute("PRAGMA journal_mode=DELETE").fetchone()[0]
            ).casefold()
            if journal != "delete":
                raise CompletionOutputArchiveIntegrityError(
                    "archive journal mode differs"
                )
            database.execute(f"PRAGMA busy_timeout={self.limits.busy_timeout_ms}")
            page_size = int(database.execute("PRAGMA page_size").fetchone()[0])
            maximum_pages = self.limits.max_database_bytes // page_size
            if maximum_pages < 1:
                raise CompletionOutputArchiveCapacityError(
                    "archive database page cap is empty"
                )
            configured = int(
                database.execute(f"PRAGMA max_page_count={maximum_pages}").fetchone()[0]
            )
            current = int(database.execute("PRAGMA page_count").fetchone()[0])
            if configured != maximum_pages or current > maximum_pages:
                raise CompletionOutputArchiveCapacityError(
                    "archive database exceeds its page cap"
                )
            return database
        except BaseException:
            database.close()
            raise

    @staticmethod
    def _objects(database: sqlite3.Connection) -> dict[str, tuple[str, str]]:
        return {
            str(row[0]): (str(row[1]), str(row[2]))
            for row in database.execute(
                "SELECT name,type,sql FROM sqlite_master "
                "WHERE name NOT LIKE 'sqlite_%' ORDER BY name"
            )
        }

    def _create_schema(self, database: sqlite3.Connection) -> None:
        for _, sql in _SCHEMA_OBJECTS.values():
            database.execute(sql)
        database.execute(
            "INSERT INTO archive_metadata VALUES(1,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                ARCHIVE_STORE_CONTRACT,
                ARCHIVE_SCHEMA_VERSION,
                self.source_catalog.fingerprint,
                self.limits.fingerprint,
                self.hot_policy.fingerprint,
                self.source_reproof_authority.fingerprint,
                self.authority_sha256,
                self.limits.max_records,
                self.limits.max_record_bytes,
                self.limits.max_total_record_bytes,
                self.limits.max_database_bytes,
                self.limits.busy_timeout_ms,
            ),
        )

    def _validate_schema(
        self, database: sqlite3.Connection, *, full: bool = False
    ) -> None:
        objects = self._objects(database)
        if set(objects) != set(_SCHEMA_OBJECTS):
            raise CompletionOutputArchiveIntegrityError(
                "archive schema object inventory differs"
            )
        for name, (expected_type, expected_sql) in _SCHEMA_OBJECTS.items():
            actual_type, actual_sql = objects[name]
            if (
                actual_type != expected_type
                or _normal_sql(actual_sql) != _normal_sql(expected_sql)
            ):
                raise CompletionOutputArchiveIntegrityError(
                    "archive schema object differs"
                )
        metadata = database.execute(
            "SELECT * FROM archive_metadata WHERE singleton=1"
        ).fetchone()
        if (
            metadata is None
            or int(database.execute("SELECT count(*) FROM archive_metadata").fetchone()[0])
            != 1
            or str(metadata["contract"]) != ARCHIVE_STORE_CONTRACT
            or int(metadata["schema_version"]) != ARCHIVE_SCHEMA_VERSION
            or str(metadata["source_catalog_sha256"])
            != self.source_catalog.fingerprint
            or str(metadata["limits_sha256"]) != self.limits.fingerprint
            or str(metadata["hot_policy_sha256"]) != self.hot_policy.fingerprint
            or str(metadata["source_reproof_authority_sha256"])
            != self.source_reproof_authority.fingerprint
            or str(metadata["authority_sha256"]) != self.authority_sha256
            or int(metadata["max_records"]) != self.limits.max_records
            or int(metadata["max_record_bytes"]) != self.limits.max_record_bytes
            or int(metadata["max_total_record_bytes"])
            != self.limits.max_total_record_bytes
            or int(metadata["max_database_bytes"])
            != self.limits.max_database_bytes
            or int(metadata["busy_timeout_ms"]) != self.limits.busy_timeout_ms
        ):
            raise CompletionOutputArchiveIntegrityError(
                "archive metadata authority differs"
            )
        if full and database.execute("PRAGMA quick_check").fetchone()[0] != "ok":
            raise CompletionOutputArchiveIntegrityError(
                "archive database integrity check failed"
            )

    def initialize(self) -> None:
        try:
            with self._lock, closing(self._connect()) as database:
                database.execute("BEGIN IMMEDIATE")
                if not self._objects(database):
                    self._create_schema(database)
                self._validate_schema(database)
                database.commit()
            with self._lock, closing(self._connect()) as database:
                self._validate_schema(database, full=True)
        except CompletionOutputArchiveError:
            raise
        except sqlite3.DatabaseError as exc:
            if "full" in str(exc).casefold():
                raise CompletionOutputArchiveCapacityError(
                    "archive database capacity reached"
                ) from exc
            raise CompletionOutputArchiveIntegrityError(
                "archive database is invalid"
            ) from exc

    def _seal_for(
        self, record: CompletionOutputArchiveRecord, archived_at: float
    ) -> CompletionOutputArchiveSeal:
        provisional = CompletionOutputArchiveSeal(
            archive_id=record.archive_id,
            archive_authority_sha256=self.authority_sha256,
            source_profile_sha256=record.source_profile_sha256,
            event_id=record.event_id,
            record_sha256=record.record_sha256,
            record_bytes=len(record.canonical_bytes),
            archived_at=archived_at,
            seal_sha256="0" * 64,
        )
        return CompletionOutputArchiveSeal(
            archive_id=provisional.archive_id,
            archive_authority_sha256=provisional.archive_authority_sha256,
            source_profile_sha256=provisional.source_profile_sha256,
            event_id=provisional.event_id,
            record_sha256=provisional.record_sha256,
            record_bytes=provisional.record_bytes,
            archived_at=provisional.archived_at,
            seal_sha256=_digest(provisional.body()),
        )

    def _entry_from_row(self, row: sqlite3.Row) -> CompletionOutputArchiveEntry:
        record_bytes = bytes(row["record_json"])
        if (
            len(record_bytes) != int(row["record_bytes"])
            or not 0 < len(record_bytes) <= self.limits.max_record_bytes
        ):
            raise CompletionOutputArchiveIntegrityError(
                "stored archive record size differs"
            )
        value = _decode_canonical(record_bytes, "stored archive record")
        record = CompletionOutputArchiveRecord.from_mapping(value)
        seal = self._seal_for(
            record, _timestamp(row["archived_at"], "stored archive time")
        )
        checks = (
            (str(row["archive_id"]), record.archive_id),
            (str(row["source_profile_sha256"]), record.source_profile_sha256),
            (str(row["event_id"]), record.event_id),
            (str(row["event_sha256"]), record.event_sha256),
            (str(row["producer_receipt_sha256"]), record.producer_receipt_sha256),
            (str(row["workload_id"]), record.workload_id),
            (str(row["consumer_id"]), record.consumer_id),
            (str(row["stream_id"]), record.stream_id),
            (int(row["stream_sequence"]), record.stream_sequence),
            (float(row["created_at"]), float(record.created_at)),
            (str(row["output_sha256"]), record.output_sha256),
            (str(row["output_receipt_sha256"]), record.output_receipt_sha256),
            (str(row["process_receipt_sha256"]), record.process_receipt_sha256),
            (str(row["source_row_sha256"]), record.source_row_sha256),
            (int(row["source_row_bytes"]), record.source_row_bytes),
            (str(row["record_sha256"]), record.record_sha256),
            (str(row["seal_sha256"]), seal.seal_sha256),
            (record_bytes, record.canonical_bytes),
        )
        if any(actual != expected for actual, expected in checks):
            raise CompletionOutputArchiveIntegrityError(
                "stored archive row differs from its sealed record"
            )
        profile = self.source_catalog.require(record.source_profile_sha256)
        if (
            profile.workload_id != record.workload_id
            or profile.consumer_id != record.consumer_id
            or profile.stream_id != record.stream_id
        ):
            raise CompletionOutputArchiveIntegrityError(
                "stored archive record differs from its source profile"
            )
        return CompletionOutputArchiveEntry(record=record, seal=seal)

    def load(
        self, source_profile_sha256: str, event_id: str
    ) -> CompletionOutputArchiveEntry | None:
        profile_sha = self.source_catalog.require(source_profile_sha256).fingerprint
        event = _event_id(event_id)
        with self._lock, closing(self._connect()) as database:
            self._validate_schema(database)
            row = database.execute(
                "SELECT * FROM archive_records "
                "WHERE source_profile_sha256=? AND event_id=?",
                (profile_sha, event),
            ).fetchone()
            return None if row is None else self._entry_from_row(row)

    def seal(
        self,
        record: CompletionOutputArchiveRecord,
    ) -> CompletionOutputArchiveSeal:
        if not isinstance(record, CompletionOutputArchiveRecord):
            raise TypeError("archive record type differs")
        profile = self.source_catalog.require(record.source_profile_sha256)
        if (
            profile.workload_id != record.workload_id
            or profile.consumer_id != record.consumer_id
            or profile.stream_id != record.stream_id
        ):
            raise CompletionOutputArchiveIntegrityError(
                "archive record route differs from its source profile"
            )
        body = record.canonical_bytes
        if len(body) > self.limits.max_record_bytes:
            raise CompletionOutputArchiveCapacityError(
                "archive record exceeds its byte cap"
            )
        try:
            with self._lock, closing(self._connect()) as database:
                database.execute("BEGIN IMMEDIATE")
                self._validate_schema(database)
                existing = database.execute(
                    "SELECT * FROM archive_records "
                    "WHERE source_profile_sha256=? AND event_id=?",
                    (record.source_profile_sha256, record.event_id),
                ).fetchone()
                if existing is not None:
                    entry = self._entry_from_row(existing)
                    if (
                        not hmac.compare_digest(
                            entry.record.record_sha256, record.record_sha256
                        )
                        or entry.record.canonical_bytes != body
                    ):
                        raise CompletionOutputArchiveConflict(
                            "archive event already binds different canonical bytes"
                        )
                    database.commit()
                    return entry.seal
                count, total = database.execute(
                    "SELECT count(*),coalesce(sum(record_bytes),0) "
                    "FROM archive_records"
                ).fetchone()
                if int(count) >= self.limits.max_records:
                    raise CompletionOutputArchiveCapacityError(
                        "archive record count cap reached"
                    )
                if int(total) + len(body) > self.limits.max_total_record_bytes:
                    raise CompletionOutputArchiveCapacityError(
                        "archive total record byte cap reached"
                    )
                archived_at = _timestamp(self._clock(), "archive clock")
                last = database.execute(
                    "SELECT max(archived_at) FROM archive_records"
                ).fetchone()[0]
                if last is not None and archived_at < float(last):
                    raise CompletionOutputArchiveClockError(
                        "archive clock moved behind durable history"
                    )
                seal = self._seal_for(record, archived_at)
                binding = self.source_reproof_authority.binding_for(
                    record.source_profile_sha256
                )
                with self.source_reproof_authority.open(record) as source_proof:
                    if not isinstance(
                        source_proof, CompletionOutputArchiveSourceRowReproof
                    ):
                        raise TypeError("source row reproof type differs")
                    proof_checks = (
                        (
                            source_proof.source_profile_sha256,
                            record.source_profile_sha256,
                        ),
                        (source_proof.event_id, record.event_id),
                        (source_proof.source_row_sha256, record.source_row_sha256),
                        (source_proof.source_row_bytes, record.source_row_bytes),
                        (
                            source_proof.reproof_contract,
                            binding.reproof_contract,
                        ),
                    )
                    if any(actual != expected for actual, expected in proof_checks):
                        raise CompletionOutputArchiveConflict(
                            "seal-time source row reproof differs from the planned row"
                        )
                    self._fault("archive.before_insert")
                    database.execute(
                        "INSERT INTO archive_records "
                        "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (
                            seal.archive_id,
                            record.source_profile_sha256,
                            record.event_id,
                            record.event_sha256,
                            record.producer_receipt_sha256,
                            record.workload_id,
                            record.consumer_id,
                            record.stream_id,
                            record.stream_sequence,
                            float(record.created_at),
                            record.output_sha256,
                            record.output_receipt_sha256,
                            record.process_receipt_sha256,
                            record.source_row_sha256,
                            record.source_row_bytes,
                            body,
                            len(body),
                            record.record_sha256,
                            archived_at,
                            seal.seal_sha256,
                        ),
                    )
                    self._fault("archive.before_commit")
                    database.commit()
                    self._fault("archive.after_commit")
        except CompletionOutputArchiveError:
            raise
        except sqlite3.DatabaseError as exc:
            if "full" in str(exc).casefold():
                raise CompletionOutputArchiveCapacityError(
                    "archive database capacity reached"
                ) from exc
            if "unique" in str(exc).casefold():
                raise CompletionOutputArchiveConflict(
                    "archive content-addressed identity conflicts"
                ) from exc
            raise CompletionOutputArchiveIntegrityError(
                "archive commit failed"
            ) from exc
        entry = self.load(record.source_profile_sha256, record.event_id)
        if entry is None or entry.record.canonical_bytes != body:
            raise CompletionOutputArchiveIntegrityError(
                "committed archive record is unavailable"
            )
        return entry.seal

    def verify_for_ack(
        self,
        source_profile_sha256: str,
        event_id: str,
        *,
        output_sha256: str,
        output_receipt_sha256: str,
        process_receipt_sha256: str,
        record_sha256: str | None = None,
    ) -> CompletionOutputArchiveEntry:
        entry = self.load(source_profile_sha256, event_id)
        if entry is None:
            raise CompletionOutputArchiveIntegrityError(
                "sealed archive record is unavailable"
            )
        checks = (
            (entry.record.output_sha256, _sha(output_sha256, "ack output SHA-256")),
            (
                entry.record.output_receipt_sha256,
                _sha(output_receipt_sha256, "ack output receipt SHA-256"),
            ),
            (
                entry.record.process_receipt_sha256,
                _sha(process_receipt_sha256, "ack process receipt SHA-256"),
            ),
        )
        if record_sha256 is not None:
            checks += (
                (
                    entry.record.record_sha256,
                    _sha(record_sha256, "ack archive record SHA-256"),
                ),
            )
        if any(
            not hmac.compare_digest(actual, expected)
            for actual, expected in checks
        ):
            raise CompletionOutputArchiveConflict(
                "ack proof differs from the sealed archive record"
            )
        return entry

    def diagnostics(self) -> dict[str, Any]:
        with self._lock, closing(self._connect()) as database:
            self._validate_schema(database, full=True)
            count, total = database.execute(
                "SELECT count(*),coalesce(sum(record_bytes),0) FROM archive_records"
            ).fetchone()
            page_size = int(database.execute("PRAGMA page_size").fetchone()[0])
            page_count = int(database.execute("PRAGMA page_count").fetchone()[0])
        result = {
            "archive_authority_sha256": self.authority_sha256,
            "contract": ARCHIVE_STORE_CONTRACT,
            "database_bytes": page_size * page_count,
            "healthy": True,
            "hot_policy_sha256": self.hot_policy.fingerprint,
            "logical_record_bytes": int(total),
            "records": int(count),
            "runtime_wiring": "inert",
            "source_catalog_sha256": self.source_catalog.fingerprint,
            "source_reproof_authority_sha256": (
                self.source_reproof_authority.fingerprint
            ),
        }
        if len(_canonical(result)) > 4_096:
            raise CompletionOutputArchiveCapacityError(
                "archive diagnostics exceed boundary"
            )
        return result


__all__ = [
    "ARCHIVE_AUTHORITY_SCHEMA",
    "ARCHIVE_CAPACITY_HORIZON_SCHEMA",
    "ARCHIVE_HOT_PLAN_SCHEMA",
    "ARCHIVE_HOT_POLICY_SCHEMA",
    "ARCHIVE_LIMITS_SCHEMA",
    "ARCHIVE_RECORD_SCHEMA",
    "ARCHIVE_SCHEMA_VERSION",
    "ARCHIVE_SEAL_SCHEMA",
    "ARCHIVE_SOURCE_CATALOG_SCHEMA",
    "ARCHIVE_SOURCE_PROFILE_SCHEMA",
    "ARCHIVE_SOURCE_REPROOF_AUTHORITY_SCHEMA",
    "ARCHIVE_SOURCE_REPROOF_BINDING_SCHEMA",
    "ARCHIVE_SOURCE_ROW_REPROOF_SCHEMA",
    "ARCHIVE_STORE_CONTRACT",
    "CompletionOutputArchiveCapacityError",
    "CompletionOutputArchiveCapacityHorizon",
    "CompletionOutputArchiveClockError",
    "CompletionOutputArchiveConflict",
    "CompletionOutputArchiveEntry",
    "CompletionOutputArchiveError",
    "CompletionOutputArchiveIntegrityError",
    "CompletionOutputArchiveLimits",
    "CompletionOutputArchivePrivacyError",
    "CompletionOutputArchiveRecord",
    "CompletionOutputArchiveSeal",
    "CompletionOutputArchiveSourceCatalog",
    "CompletionOutputArchiveSourceProfile",
    "CompletionOutputArchiveSourceReproofAuthority",
    "CompletionOutputArchiveSourceReproofBinding",
    "CompletionOutputArchiveSourceRowReproof",
    "CompletionOutputArchiveStore",
    "CompletionOutputHotCandidate",
    "CompletionOutputHotWindowPlan",
    "CompletionOutputHotWindowPolicy",
    "SOURCE_IMMUTABILITY_MODES",
    "calculate_cadence_horizon",
    "plan_hot_window",
]
