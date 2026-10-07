"""Inert closed-catalog downstream consumer/stream/archive-resolver authority.

The registry closes the gap between the terminal-raw consumer catalogue and
the archive source reproof authority.  It does not delete source bytes, apply
retention plans, run consumers, or wire itself into the reclamation
coordinator.  Its only mutable operation creates a monotone, create-once epoch
for a fully content-addressed registry root.
"""

from __future__ import annotations

from contextlib import closing, contextmanager
from dataclasses import dataclass
import hashlib
import hmac
import json
import math
import os
from pathlib import Path
import re
import secrets
import sqlite3
import stat
import threading
import time
from types import MappingProxyType
from typing import Any, Callable, Final, Iterator, Mapping
from urllib.parse import quote

from .acquisition_evidence_lifecycle import (
    CompletionProducerReceipt,
    ConsumptionReceipt,
    TerminalEventIdentity,
)
from .completion_output_archive import (
    CompletionOutputArchiveSourceCatalog,
    CompletionOutputArchiveSourceProfile,
    CompletionOutputArchiveSourceReproofAuthority,
    CompletionOutputArchiveSourceReproofBinding,
)
from .completion_output_archive_adapters import (
    ALL_ADAPTER_AUTHORITY_ID,
    ALL_ADAPTER_CATALOG_ID,
    ALL_ARCHIVE_WORKLOAD_IDS,
)
from .terminal_raw_completion_router import (
    TERMINAL_RAW_CONSUMER_CATALOG,
    TerminalRawConsumerBinding,
    validate_terminal_raw_consumer_catalog,
)
from .worker_acquisition_plan import CATALOG_SHA256, acquisition_catalog_snapshot


REGISTRY_CONTRACT: Final = "stv3-acquisition-consumer-registry-v1"
REGISTRY_ID: Final = "terminal-raw-exact-11-consumer-registry-v1"
MEMBER_SCHEMA: Final = "stv3-acquisition-consumer-registry-member-v1"
ROOT_SCHEMA: Final = "stv3-acquisition-consumer-registry-root-v1"
EPOCH_ROOT_SCHEMA: Final = "stv3-acquisition-consumer-registry-epoch-root-v1"
EPOCH_RECEIPT_SCHEMA: Final = (
    "stv3-acquisition-consumer-registry-epoch-receipt-v1"
)
VERIFICATION_SCHEMA: Final = (
    "stv3-acquisition-consumer-registry-epoch-verification-v1"
)
CONSUMER_CLOSURE_SCHEMA: Final = (
    "stv3-acquisition-consumer-registry-closure-receipt-v1"
)
REQUIRED_ARCHIVE_BINDING_SCHEMA: Final = (
    "stv3-acquisition-consumer-required-archive-binding-v1"
)
SOURCE_ARCHIVE_BINDING_SCHEMA: Final = (
    "stv3-acquisition-source-archive-binding-v1"
)
STORE_CONTRACT: Final = "stv3-acquisition-consumer-registry-store-v1"
STORE_IDENTITY_SCHEMA: Final = (
    "stv3-acquisition-consumer-registry-store-identity-v1"
)
STORE_SCHEMA_VERSION: Final = 1
EXPECTED_MEMBER_COUNT: Final = len(TERMINAL_RAW_CONSUMER_CATALOG)
MAX_DIAGNOSTIC_BYTES: Final = 4096

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,191}$")
_FORBIDDEN_KEY = re.compile(
    r"(?:^|[_-])(?:path|payload|raw|url|uri|authorization|cookie|password|"
    r"passwd|secret|api[_-]?key|access[_-]?token|refresh[_-]?token|"
    r"private[_-]?key)(?:$|[_-])",
    re.IGNORECASE,
)
_FORBIDDEN_TEXT = re.compile(
    r"(?:[A-Za-z]:[\\/]|(?:https?|ftp)://|(?:bearer|basic)\s+|"
    r"(?:password|passwd|secret|token)=)",
    re.IGNORECASE,
)


class AcquisitionConsumerRegistryError(RuntimeError):
    """Base error for the inert registry authority."""


class AcquisitionConsumerRegistryIntegrityError(AcquisitionConsumerRegistryError):
    """A closed mapping, durable checksum, schema, owner or instance differs."""


class AcquisitionConsumerRegistryConflict(AcquisitionConsumerRegistryError):
    """A create-once identity was reused for different registry facts."""


class AcquisitionConsumerRegistryCapacityError(AcquisitionConsumerRegistryError):
    """A bounded root, epoch inventory or SQLite file reached its cap."""


class AcquisitionConsumerRegistryOwnershipError(AcquisitionConsumerRegistryError):
    """Another process or authority instance owns the registry writer fence."""


class AcquisitionConsumerRegistryPrivacyError(AcquisitionConsumerRegistryError):
    """Registry material contains a path, capability or unbounded private text."""


def _canonical(value: object) -> bytes:
    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
    except (TypeError, ValueError, UnicodeError) as error:
        raise AcquisitionConsumerRegistryIntegrityError(
            "consumer registry value is not canonical JSON"
        ) from error


def _digest(value: object) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _sha(value: object, label: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise AcquisitionConsumerRegistryIntegrityError(
            f"{label} is not SHA-256"
        )
    return value


def _identifier(value: object, label: str) -> str:
    if not isinstance(value, str) or _IDENTIFIER.fullmatch(value) is None:
        raise AcquisitionConsumerRegistryIntegrityError(f"{label} is invalid")
    return value


def _timestamp(value: object, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise AcquisitionConsumerRegistryIntegrityError(f"{label} is invalid")
    result = float(value)
    if not math.isfinite(result) or result < 0.0:
        raise AcquisitionConsumerRegistryIntegrityError(f"{label} is invalid")
    return result


def _integer(
    value: object,
    label: str,
    *,
    minimum: int,
    maximum: int,
) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not minimum <= value <= maximum
    ):
        raise AcquisitionConsumerRegistryIntegrityError(f"{label} is invalid")
    return value


def _exact(value: object, keys: set[str], label: str) -> Mapping[str, Any]:
    if (
        not isinstance(value, Mapping)
        or any(not isinstance(key, str) for key in value)
        or set(value) != keys
    ):
        raise AcquisitionConsumerRegistryIntegrityError(f"{label} schema differs")
    return value


def _assert_private_safe(value: object, *, depth: int = 0) -> None:
    if depth > 12:
        raise AcquisitionConsumerRegistryCapacityError(
            "consumer registry nesting exceeds cap"
        )
    if isinstance(value, Mapping):
        if len(value) > 128:
            raise AcquisitionConsumerRegistryCapacityError(
                "consumer registry object exceeds cap"
            )
        for key, item in value.items():
            if not isinstance(key, str) or _FORBIDDEN_KEY.search(key):
                raise AcquisitionConsumerRegistryPrivacyError(
                    "consumer registry contains a private-capability key"
                )
            _assert_private_safe(item, depth=depth + 1)
        return
    if isinstance(value, (list, tuple)):
        if len(value) > 64:
            raise AcquisitionConsumerRegistryCapacityError(
                "consumer registry sequence exceeds cap"
            )
        for item in value:
            _assert_private_safe(item, depth=depth + 1)
        return
    if isinstance(value, str):
        if len(value.encode("utf-8")) > 4096 or _FORBIDDEN_TEXT.search(value):
            raise AcquisitionConsumerRegistryPrivacyError(
                "consumer registry contains private-capability text"
            )
        return
    if value is None or isinstance(value, (bool, int, float)):
        if isinstance(value, float) and not math.isfinite(value):
            raise AcquisitionConsumerRegistryIntegrityError(
                "consumer registry number is not finite"
            )
        return
    raise AcquisitionConsumerRegistryIntegrityError(
        "consumer registry contains an unsupported value"
    )


def _terminal_catalog_sha256() -> str:
    validate_terminal_raw_consumer_catalog(TERMINAL_RAW_CONSUMER_CATALOG)
    return _digest(
        {
            "contract": REGISTRY_CONTRACT,
            "members": [
                TERMINAL_RAW_CONSUMER_CATALOG[workload_id].as_dict()
                for workload_id in sorted(TERMINAL_RAW_CONSUMER_CATALOG)
            ],
            "role": "terminal_raw_consumer_catalog",
        }
    )


def _catalog_cadences() -> Mapping[str, int]:
    snapshot = acquisition_catalog_snapshot()
    if snapshot.get("catalog_sha256") != CATALOG_SHA256:
        raise AcquisitionConsumerRegistryIntegrityError(
            "acquisition catalog checksum differs"
        )
    result: dict[str, int] = {}
    for row in snapshot.get("workloads", ()):
        if not isinstance(row, Mapping):
            raise AcquisitionConsumerRegistryIntegrityError(
                "acquisition workload catalog differs"
            )
        workload_id = row.get("workload_id")
        cadence = row.get("cadence_seconds")
        if isinstance(workload_id, str) and isinstance(cadence, int):
            result[workload_id] = cadence
    return MappingProxyType(result)


@dataclass(frozen=True, slots=True)
class AcquisitionConsumerRegistryMember:
    workload_id: str
    consumer_id: str
    stream_id: str
    consumer_binding_sha256: str
    archive_source_profile_sha256: str
    archive_resolver_id: str
    archive_resolver_sha256: str
    archive_source_store_identity_sha256: str
    archive_reproof_contract: str

    def __post_init__(self) -> None:
        for value, label in (
            (self.workload_id, "registry workload ID"),
            (self.consumer_id, "registry consumer ID"),
            (self.stream_id, "registry stream ID"),
            (self.archive_resolver_id, "registry archive resolver ID"),
            (self.archive_reproof_contract, "registry archive reproof contract"),
        ):
            _identifier(value, label)
        for value, label in (
            (self.consumer_binding_sha256, "registry consumer binding SHA-256"),
            (
                self.archive_source_profile_sha256,
                "registry archive source profile SHA-256",
            ),
            (self.archive_resolver_sha256, "registry archive resolver SHA-256"),
            (
                self.archive_source_store_identity_sha256,
                "registry archive store identity SHA-256",
            ),
        ):
            _sha(value, label)
        binding = TERMINAL_RAW_CONSUMER_CATALOG.get(self.workload_id)
        if (
            not isinstance(binding, TerminalRawConsumerBinding)
            or self.consumer_id != binding.consumer_id
            or self.stream_id != binding.stream_id
            or self.stream_id != self.workload_id
            or self.consumer_binding_sha256 != binding.fingerprint
            or self.archive_resolver_id
            != f"all-terminal-source-row:{self.workload_id}:v1"
        ):
            raise AcquisitionConsumerRegistryIntegrityError(
                "registry member differs from the closed consumer catalogue"
            )

    def as_dict(self) -> dict[str, str]:
        return {
            "archive_reproof_contract": self.archive_reproof_contract,
            "archive_resolver_id": self.archive_resolver_id,
            "archive_resolver_sha256": self.archive_resolver_sha256,
            "archive_source_profile_sha256": (
                self.archive_source_profile_sha256
            ),
            "archive_source_store_identity_sha256": (
                self.archive_source_store_identity_sha256
            ),
            "consumer_binding_sha256": self.consumer_binding_sha256,
            "consumer_id": self.consumer_id,
            "schema": MEMBER_SCHEMA,
            "stream_id": self.stream_id,
            "workload_id": self.workload_id,
        }

    @property
    def fingerprint(self) -> str:
        return _digest(self.as_dict())


@dataclass(frozen=True, slots=True)
class AcquisitionConsumerRegistryRoot:
    terminal_consumer_catalog_sha256: str
    archive_source_catalog_sha256: str
    archive_reproof_authority_sha256: str
    members: tuple[AcquisitionConsumerRegistryMember, ...]

    def __post_init__(self) -> None:
        for value, label in (
            (
                self.terminal_consumer_catalog_sha256,
                "terminal consumer catalog SHA-256",
            ),
            (
                self.archive_source_catalog_sha256,
                "archive source catalog SHA-256",
            ),
            (
                self.archive_reproof_authority_sha256,
                "archive reproof authority SHA-256",
            ),
        ):
            _sha(value, label)
        members = tuple(self.members)
        if (
            len(members) != EXPECTED_MEMBER_COUNT
            or any(
                not isinstance(item, AcquisitionConsumerRegistryMember)
                for item in members
            )
            or tuple(item.workload_id for item in members)
            != tuple(sorted(TERMINAL_RAW_CONSUMER_CATALOG))
            or set(item.workload_id for item in members)
            != set(ALL_ARCHIVE_WORKLOAD_IDS)
            or len({item.consumer_id for item in members})
            != EXPECTED_MEMBER_COUNT
            or len({item.archive_source_profile_sha256 for item in members})
            != EXPECTED_MEMBER_COUNT
            or len({item.archive_resolver_id for item in members})
            != EXPECTED_MEMBER_COUNT
            or len({item.archive_resolver_sha256 for item in members})
            != EXPECTED_MEMBER_COUNT
        ):
            raise AcquisitionConsumerRegistryIntegrityError(
                "registry root is not the closed exact-catalog membership"
            )
        if self.terminal_consumer_catalog_sha256 != _terminal_catalog_sha256():
            raise AcquisitionConsumerRegistryIntegrityError(
                "registry terminal catalogue root differs"
            )
        object.__setattr__(self, "members", members)
        _assert_private_safe(self._core())

    def _core(self) -> dict[str, Any]:
        return {
            "archive_reproof_authority_id": ALL_ADAPTER_AUTHORITY_ID,
            "archive_reproof_authority_sha256": (
                self.archive_reproof_authority_sha256
            ),
            "archive_source_catalog_id": ALL_ADAPTER_CATALOG_ID,
            "archive_source_catalog_sha256": self.archive_source_catalog_sha256,
            "contract": REGISTRY_CONTRACT,
            "member_count": EXPECTED_MEMBER_COUNT,
            "members": [item.as_dict() for item in self.members],
            "registry_id": REGISTRY_ID,
            "runtime_wiring": "inert",
            "schema": ROOT_SCHEMA,
            "terminal_consumer_catalog_sha256": (
                self.terminal_consumer_catalog_sha256
            ),
        }

    @property
    def root_sha256(self) -> str:
        return _digest(self._core())

    def as_dict(self) -> dict[str, Any]:
        result = self._core()
        result["registry_root_sha256"] = self.root_sha256
        return result

    @property
    def by_workload(self) -> Mapping[str, AcquisitionConsumerRegistryMember]:
        return MappingProxyType({item.workload_id: item for item in self.members})


def build_consumer_registry_root(
    *,
    source_catalog: CompletionOutputArchiveSourceCatalog,
    source_reproof_authority: CompletionOutputArchiveSourceReproofAuthority,
) -> AcquisitionConsumerRegistryRoot:
    """Cross-bind the exact current consumers to exact archive resolvers."""

    if not isinstance(source_catalog, CompletionOutputArchiveSourceCatalog):
        raise TypeError("source_catalog has wrong type")
    if not isinstance(
        source_reproof_authority,
        CompletionOutputArchiveSourceReproofAuthority,
    ):
        raise TypeError("source_reproof_authority has wrong type")
    if source_catalog.catalog_id != ALL_ADAPTER_CATALOG_ID:
        raise AcquisitionConsumerRegistryIntegrityError(
            "archive source catalog ID differs"
        )
    if source_reproof_authority.authority_id != ALL_ADAPTER_AUTHORITY_ID:
        raise AcquisitionConsumerRegistryIntegrityError(
            "archive reproof authority ID differs"
        )
    if (
        len(source_catalog.profiles) != EXPECTED_MEMBER_COUNT
        or len(source_reproof_authority.bindings) != EXPECTED_MEMBER_COUNT
    ):
        raise AcquisitionConsumerRegistryIntegrityError(
            "archive authority is not exact-catalog"
        )
    profiles: dict[str, CompletionOutputArchiveSourceProfile] = {}
    for profile in source_catalog.profiles:
        if profile.workload_id in profiles:
            raise AcquisitionConsumerRegistryConflict(
                "archive source catalog duplicates a workload"
            )
        profiles[profile.workload_id] = profile
    if set(profiles) != set(TERMINAL_RAW_CONSUMER_CATALOG) or set(profiles) != set(
        ALL_ARCHIVE_WORKLOAD_IDS
    ):
        raise AcquisitionConsumerRegistryIntegrityError(
            "archive source workload membership differs"
        )
    bindings = {
        item.source_profile_sha256: item
        for item in source_reproof_authority.bindings
    }
    if len(bindings) != EXPECTED_MEMBER_COUNT:
        raise AcquisitionConsumerRegistryConflict(
            "archive resolver authority duplicates a profile"
        )
    cadences = _catalog_cadences()
    members: list[AcquisitionConsumerRegistryMember] = []
    for workload_id in sorted(TERMINAL_RAW_CONSUMER_CATALOG):
        consumer = TERMINAL_RAW_CONSUMER_CATALOG[workload_id]
        profile = profiles[workload_id]
        binding = bindings.get(profile.fingerprint)
        if (
            not isinstance(binding, CompletionOutputArchiveSourceReproofBinding)
            or profile.consumer_id != consumer.consumer_id
            or profile.stream_id != consumer.stream_id
            or profile.consumer_binding_sha256 != consumer.fingerprint
            or profile.acquisition_catalog_sha256 != CATALOG_SHA256
            or profile.cadence_seconds != cadences.get(workload_id)
            or binding.reproof_contract != profile.source_row_reproof_contract
            or binding.resolver_id
            != f"all-terminal-source-row:{workload_id}:v1"
        ):
            raise AcquisitionConsumerRegistryIntegrityError(
                "archive resolver differs from its closed consumer membership"
            )
        members.append(
            AcquisitionConsumerRegistryMember(
                workload_id=workload_id,
                consumer_id=consumer.consumer_id,
                stream_id=consumer.stream_id,
                consumer_binding_sha256=consumer.fingerprint,
                archive_source_profile_sha256=profile.fingerprint,
                archive_resolver_id=binding.resolver_id,
                archive_resolver_sha256=binding.resolver_sha256,
                archive_source_store_identity_sha256=(
                    binding.source_store_identity_sha256
                ),
                archive_reproof_contract=binding.reproof_contract,
            )
        )
    if set(bindings) != {
        item.archive_source_profile_sha256 for item in members
    }:
        raise AcquisitionConsumerRegistryIntegrityError(
            "archive resolver authority has an unbound profile"
        )
    return AcquisitionConsumerRegistryRoot(
        terminal_consumer_catalog_sha256=_terminal_catalog_sha256(),
        archive_source_catalog_sha256=source_catalog.fingerprint,
        archive_reproof_authority_sha256=source_reproof_authority.fingerprint,
        members=tuple(members),
    )


@dataclass(frozen=True, slots=True)
class AcquisitionConsumerRegistryLimits:
    max_epochs: int = 1024
    max_root_bytes: int = 128 * 1024
    max_database_bytes: int = 16 * 1024 * 1024
    busy_timeout_ms: int = 5000

    def __post_init__(self) -> None:
        for label, value, minimum, maximum in (
            ("max_epochs", self.max_epochs, 1, 1_000_000),
            ("max_root_bytes", self.max_root_bytes, 4096, 1024 * 1024),
            (
                "max_database_bytes",
                self.max_database_bytes,
                1024 * 1024,
                1024 * 1024 * 1024,
            ),
            ("busy_timeout_ms", self.busy_timeout_ms, 1, 60_000),
        ):
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or not minimum <= value <= maximum
            ):
                raise ValueError(f"{label} is outside its registry bound")

    @property
    def fingerprint(self) -> str:
        return _digest(
            {
                "busy_timeout_ms": self.busy_timeout_ms,
                "max_database_bytes": self.max_database_bytes,
                "max_epochs": self.max_epochs,
                "max_root_bytes": self.max_root_bytes,
                "schema": "stv3-acquisition-consumer-registry-limits-v1",
            }
        )


@dataclass(frozen=True, slots=True)
class AcquisitionConsumerRegistryEpochReceipt:
    epoch: int
    registry_root_sha256: str
    previous_epoch_root_sha256: str | None
    authority_instance_sha256: str
    created_at: float
    epoch_root_sha256: str
    receipt_sha256: str

    def __post_init__(self) -> None:
        _integer(self.epoch, "consumer registry epoch", minimum=1, maximum=2**63 - 1)
        _sha(self.registry_root_sha256, "consumer registry root SHA-256")
        if self.previous_epoch_root_sha256 is not None:
            _sha(self.previous_epoch_root_sha256, "previous epoch root SHA-256")
        _sha(self.authority_instance_sha256, "registry authority instance SHA-256")
        _timestamp(self.created_at, "consumer registry epoch clock")
        _sha(self.epoch_root_sha256, "consumer registry epoch root SHA-256")
        _sha(self.receipt_sha256, "consumer registry epoch receipt SHA-256")
        if self.epoch_root_sha256 != _digest(self._root_core()):
            raise AcquisitionConsumerRegistryIntegrityError(
                "consumer registry epoch root checksum differs"
            )
        if self.receipt_sha256 != _digest(self._receipt_core()):
            raise AcquisitionConsumerRegistryIntegrityError(
                "consumer registry epoch receipt checksum differs"
            )

    def _root_core(self) -> dict[str, Any]:
        return {
            "authority_instance_sha256": self.authority_instance_sha256,
            "created_at": float(self.created_at),
            "epoch": self.epoch,
            "previous_epoch_root_sha256": self.previous_epoch_root_sha256,
            "registry_root_sha256": self.registry_root_sha256,
            "schema": EPOCH_ROOT_SCHEMA,
        }

    def _receipt_core(self) -> dict[str, Any]:
        return {
            **self._root_core(),
            "epoch_root_sha256": self.epoch_root_sha256,
            "schema": EPOCH_RECEIPT_SCHEMA,
        }

    def as_dict(self) -> dict[str, Any]:
        return {**self._receipt_core(), "receipt_sha256": self.receipt_sha256}

    @classmethod
    def create(
        cls,
        *,
        epoch: int,
        registry_root_sha256: str,
        previous_epoch_root_sha256: str | None,
        authority_instance_sha256: str,
        created_at: float,
    ) -> "AcquisitionConsumerRegistryEpochReceipt":
        root_core = {
            "authority_instance_sha256": authority_instance_sha256,
            "created_at": float(created_at),
            "epoch": epoch,
            "previous_epoch_root_sha256": previous_epoch_root_sha256,
            "registry_root_sha256": registry_root_sha256,
            "schema": EPOCH_ROOT_SCHEMA,
        }
        epoch_root = _digest(root_core)
        receipt_core = {
            **root_core,
            "epoch_root_sha256": epoch_root,
            "schema": EPOCH_RECEIPT_SCHEMA,
        }
        return cls(
            epoch=epoch,
            registry_root_sha256=registry_root_sha256,
            previous_epoch_root_sha256=previous_epoch_root_sha256,
            authority_instance_sha256=authority_instance_sha256,
            created_at=created_at,
            epoch_root_sha256=epoch_root,
            receipt_sha256=_digest(receipt_core),
        )


@dataclass(frozen=True, slots=True)
class AcquisitionConsumerRegistryEpochVerification:
    registry_root_sha256: str
    epoch_root_sha256: str
    epoch_receipt_sha256: str
    epoch: int
    member_count: int
    store_identity_sha256: str

    def __post_init__(self) -> None:
        for value, label in (
            (self.registry_root_sha256, "verified registry root SHA-256"),
            (self.epoch_root_sha256, "verified epoch root SHA-256"),
            (self.epoch_receipt_sha256, "verified epoch receipt SHA-256"),
            (self.store_identity_sha256, "verified registry store identity SHA-256"),
        ):
            _sha(value, label)
        _integer(self.epoch, "verified registry epoch", minimum=1, maximum=2**63 - 1)
        if self.member_count != EXPECTED_MEMBER_COUNT:
            raise AcquisitionConsumerRegistryIntegrityError(
                "verified registry member count differs"
            )

    def as_dict(self) -> dict[str, Any]:
        return {
            "current": True,
            "epoch": self.epoch,
            "epoch_receipt_sha256": self.epoch_receipt_sha256,
            "epoch_root_sha256": self.epoch_root_sha256,
            "member_count": self.member_count,
            "registry_root_sha256": self.registry_root_sha256,
            "schema": VERIFICATION_SCHEMA,
            "store_identity_sha256": self.store_identity_sha256,
        }


@dataclass(frozen=True, slots=True, repr=False)
class AcquisitionConsumerClosureReceipt:
    """Exact route/lifecycle/archive closure for one workload acquisition job."""

    receipt_sha256: str
    candidate_sha256: str
    workload_id: str
    acquisition_job_id: str
    consumer_id: str
    stream_id: str
    stream_sequence: int
    registry_root_sha256: str
    registry_epoch_root_sha256: str
    registry_epoch_receipt_sha256: str
    registry_store_identity_sha256: str
    registry_epoch: int
    registry_member_sha256: str
    lifecycle_event_id: str
    lifecycle_event_sha256: str
    lifecycle_producer_receipt_sha256: str
    lifecycle_consumption_receipt_sha256: str
    lifecycle_terminal_output_sha256: str
    lifecycle_terminal_identity_sha256: str
    lifecycle_storage_status: str
    lifecycle_archive_segment_id: str | None
    lifecycle_terminal_at: float
    archive_source_profile_sha256: str
    archive_resolver_id: str
    archive_resolver_sha256: str
    archive_source_store_identity_sha256: str
    archive_reproof_contract: str
    required_archive_binding_sha256: str
    source_archive_anchor_sha256: str
    source_archive_segment_sha256: str
    source_archive_record_sha256: str
    source_archive_binding_sha256: str

    def __post_init__(self) -> None:
        for value, label in (
            (self.workload_id, "consumer closure workload ID"),
            (self.acquisition_job_id, "consumer closure acquisition job ID"),
            (self.consumer_id, "consumer closure consumer ID"),
            (self.stream_id, "consumer closure stream ID"),
            (self.lifecycle_event_id, "consumer closure lifecycle event ID"),
            (self.archive_resolver_id, "consumer closure archive resolver ID"),
            (self.archive_reproof_contract, "consumer closure reproof contract"),
        ):
            _identifier(value, label)
        for value, label in (
            (self.receipt_sha256, "consumer closure receipt SHA-256"),
            (self.candidate_sha256, "consumer closure candidate SHA-256"),
            (self.registry_root_sha256, "consumer closure registry root SHA-256"),
            (
                self.registry_epoch_root_sha256,
                "consumer closure registry epoch root SHA-256",
            ),
            (
                self.registry_epoch_receipt_sha256,
                "consumer closure registry epoch receipt SHA-256",
            ),
            (
                self.registry_store_identity_sha256,
                "consumer closure registry store identity SHA-256",
            ),
            (self.registry_member_sha256, "consumer closure member SHA-256"),
            (self.lifecycle_event_sha256, "consumer closure event SHA-256"),
            (
                self.lifecycle_producer_receipt_sha256,
                "consumer closure producer receipt SHA-256",
            ),
            (
                self.lifecycle_consumption_receipt_sha256,
                "consumer closure consumption receipt SHA-256",
            ),
            (
                self.lifecycle_terminal_output_sha256,
                "consumer closure terminal output SHA-256",
            ),
            (
                self.lifecycle_terminal_identity_sha256,
                "consumer closure terminal identity SHA-256",
            ),
            (
                self.archive_source_profile_sha256,
                "consumer closure archive source profile SHA-256",
            ),
            (
                self.archive_resolver_sha256,
                "consumer closure archive resolver SHA-256",
            ),
            (
                self.archive_source_store_identity_sha256,
                "consumer closure archive source store identity SHA-256",
            ),
            (
                self.required_archive_binding_sha256,
                "consumer closure required archive binding SHA-256",
            ),
            (
                self.source_archive_anchor_sha256,
                "consumer closure source archive anchor SHA-256",
            ),
            (
                self.source_archive_segment_sha256,
                "consumer closure source archive segment SHA-256",
            ),
            (
                self.source_archive_record_sha256,
                "consumer closure source archive record SHA-256",
            ),
            (
                self.source_archive_binding_sha256,
                "consumer closure source archive binding SHA-256",
            ),
        ):
            _sha(value, label)
        _integer(
            self.stream_sequence,
            "consumer closure stream sequence",
            minimum=1,
            maximum=2**63 - 1,
        )
        _integer(
            self.registry_epoch,
            "consumer closure registry epoch",
            minimum=1,
            maximum=2**63 - 1,
        )
        _timestamp(self.lifecycle_terminal_at, "consumer closure terminal clock")
        if self.lifecycle_storage_status not in {"hot", "archived"}:
            raise AcquisitionConsumerRegistryIntegrityError(
                "consumer closure lifecycle storage status differs"
            )
        if self.lifecycle_storage_status == "hot":
            if self.lifecycle_archive_segment_id is not None:
                raise AcquisitionConsumerRegistryIntegrityError(
                    "hot consumer closure has an archive segment"
                )
        elif self.lifecycle_archive_segment_id is None:
            raise AcquisitionConsumerRegistryIntegrityError(
                "archived consumer closure lacks an archive segment"
            )
        else:
            _identifier(
                self.lifecycle_archive_segment_id,
                "consumer closure lifecycle archive segment ID",
            )
        if self.required_archive_binding_sha256 != _digest(
            self._required_archive_binding_core()
        ):
            raise AcquisitionConsumerRegistryIntegrityError(
                "consumer closure required archive binding checksum differs"
            )
        if self.source_archive_binding_sha256 != _digest(
            self._source_archive_binding_core()
        ):
            raise AcquisitionConsumerRegistryIntegrityError(
                "consumer closure source archive binding checksum differs"
            )
        if self.receipt_sha256 != _digest(self._core()):
            raise AcquisitionConsumerRegistryIntegrityError(
                "consumer closure receipt checksum differs"
            )
        _assert_private_safe(self.as_dict())

    def _required_archive_binding_core(self) -> dict[str, str]:
        return {
            "archive_reproof_contract": self.archive_reproof_contract,
            "archive_resolver_id": self.archive_resolver_id,
            "archive_resolver_sha256": self.archive_resolver_sha256,
            "archive_source_profile_sha256": self.archive_source_profile_sha256,
            "archive_source_store_identity_sha256": (
                self.archive_source_store_identity_sha256
            ),
            "registry_member_sha256": self.registry_member_sha256,
            "schema": REQUIRED_ARCHIVE_BINDING_SCHEMA,
            "workload_id": self.workload_id,
        }

    def _source_archive_binding_core(self) -> dict[str, str]:
        return {
            "candidate_sha256": self.candidate_sha256,
            "schema": SOURCE_ARCHIVE_BINDING_SCHEMA,
            "source_archive_anchor_sha256": self.source_archive_anchor_sha256,
            "source_archive_record_sha256": self.source_archive_record_sha256,
            "source_archive_segment_sha256": self.source_archive_segment_sha256,
        }

    def _core(self) -> dict[str, Any]:
        return {
            "acquisition_job_id": self.acquisition_job_id,
            "archive_reproof_contract": self.archive_reproof_contract,
            "archive_resolver_id": self.archive_resolver_id,
            "archive_resolver_sha256": self.archive_resolver_sha256,
            "archive_source_profile_sha256": self.archive_source_profile_sha256,
            "archive_source_store_identity_sha256": (
                self.archive_source_store_identity_sha256
            ),
            "candidate_sha256": self.candidate_sha256,
            "consumer_id": self.consumer_id,
            "lifecycle_archive_segment_id": self.lifecycle_archive_segment_id,
            "lifecycle_consumption_receipt_sha256": (
                self.lifecycle_consumption_receipt_sha256
            ),
            "lifecycle_event_id": self.lifecycle_event_id,
            "lifecycle_event_sha256": self.lifecycle_event_sha256,
            "lifecycle_producer_receipt_sha256": (
                self.lifecycle_producer_receipt_sha256
            ),
            "lifecycle_storage_status": self.lifecycle_storage_status,
            "lifecycle_terminal_at": float(self.lifecycle_terminal_at),
            "lifecycle_terminal_identity_sha256": (
                self.lifecycle_terminal_identity_sha256
            ),
            "lifecycle_terminal_kind": "consumed",
            "lifecycle_terminal_output_sha256": (
                self.lifecycle_terminal_output_sha256
            ),
            "registry_epoch": self.registry_epoch,
            "registry_epoch_receipt_sha256": (
                self.registry_epoch_receipt_sha256
            ),
            "registry_epoch_root_sha256": self.registry_epoch_root_sha256,
            "registry_member_sha256": self.registry_member_sha256,
            "registry_root_sha256": self.registry_root_sha256,
            "registry_store_identity_sha256": (
                self.registry_store_identity_sha256
            ),
            "required_archive_binding_sha256": (
                self.required_archive_binding_sha256
            ),
            "schema": CONSUMER_CLOSURE_SCHEMA,
            "source_archive_anchor_sha256": self.source_archive_anchor_sha256,
            "source_archive_binding_sha256": self.source_archive_binding_sha256,
            "source_archive_record_sha256": self.source_archive_record_sha256,
            "source_archive_segment_sha256": self.source_archive_segment_sha256,
            "stream_id": self.stream_id,
            "stream_sequence": self.stream_sequence,
            "workload_id": self.workload_id,
        }

    def as_dict(self) -> dict[str, Any]:
        return {**self._core(), "receipt_sha256": self.receipt_sha256}

    @classmethod
    def create(
        cls,
        *,
        candidate_sha256: str,
        acquisition_job_id: str,
        member: AcquisitionConsumerRegistryMember,
        verification: AcquisitionConsumerRegistryEpochVerification,
        producer_receipt: CompletionProducerReceipt,
        consumption_receipt: ConsumptionReceipt,
        terminal_identity: TerminalEventIdentity,
        source_archive_anchor_sha256: str,
        source_archive_segment_sha256: str,
        source_archive_record_sha256: str,
    ) -> "AcquisitionConsumerClosureReceipt":
        if not isinstance(member, AcquisitionConsumerRegistryMember):
            raise TypeError("consumer closure member has wrong type")
        if not isinstance(
            verification,
            AcquisitionConsumerRegistryEpochVerification,
        ):
            raise TypeError("consumer closure verification has wrong type")
        if not isinstance(producer_receipt, CompletionProducerReceipt):
            raise TypeError("consumer closure producer receipt has wrong type")
        if not isinstance(consumption_receipt, ConsumptionReceipt):
            raise TypeError("consumer closure consumption receipt has wrong type")
        if not isinstance(terminal_identity, TerminalEventIdentity):
            raise TypeError("consumer closure terminal identity has wrong type")
        candidate = _sha(candidate_sha256, "consumer closure candidate SHA-256")
        acquisition_job = _identifier(
            acquisition_job_id,
            "consumer closure acquisition job ID",
        )
        for value, label in (
            (source_archive_anchor_sha256, "source archive anchor SHA-256"),
            (source_archive_segment_sha256, "source archive segment SHA-256"),
            (source_archive_record_sha256, "source archive record SHA-256"),
        ):
            _sha(value, label)
        if (
            producer_receipt.acquisition_job_id != acquisition_job
            or producer_receipt.consumer_id != member.consumer_id
            or producer_receipt.stream_id != member.stream_id
            or producer_receipt.stream_id != member.workload_id
            or consumption_receipt.event_id != producer_receipt.event_id
            or consumption_receipt.event_sha256 != producer_receipt.event_sha256
            or consumption_receipt.producer_receipt_sha256
            != producer_receipt.producer_receipt_sha256
            or consumption_receipt.consumer_id != producer_receipt.consumer_id
            or consumption_receipt.stream_id != producer_receipt.stream_id
            or consumption_receipt.stream_sequence
            != producer_receipt.stream_sequence
            or terminal_identity.event_id != producer_receipt.event_id
            or terminal_identity.event_sha256 != producer_receipt.event_sha256
            or terminal_identity.producer_receipt_sha256
            != producer_receipt.producer_receipt_sha256
            or terminal_identity.consumer_id != producer_receipt.consumer_id
            or terminal_identity.stream_id != producer_receipt.stream_id
            or terminal_identity.stream_sequence
            != producer_receipt.stream_sequence
            or terminal_identity.terminal_kind != "consumed"
            or terminal_identity.terminal_output_sha256
            != consumption_receipt.output_sha256
            or terminal_identity.terminal_at != consumption_receipt.created_at
        ):
            raise AcquisitionConsumerRegistryIntegrityError(
                "consumer closure route or lifecycle terminal identity differs"
            )
        required_archive_core = {
            "archive_reproof_contract": member.archive_reproof_contract,
            "archive_resolver_id": member.archive_resolver_id,
            "archive_resolver_sha256": member.archive_resolver_sha256,
            "archive_source_profile_sha256": (
                member.archive_source_profile_sha256
            ),
            "archive_source_store_identity_sha256": (
                member.archive_source_store_identity_sha256
            ),
            "registry_member_sha256": member.fingerprint,
            "schema": REQUIRED_ARCHIVE_BINDING_SCHEMA,
            "workload_id": member.workload_id,
        }
        source_archive_core = {
            "candidate_sha256": candidate,
            "schema": SOURCE_ARCHIVE_BINDING_SCHEMA,
            "source_archive_anchor_sha256": source_archive_anchor_sha256,
            "source_archive_record_sha256": source_archive_record_sha256,
            "source_archive_segment_sha256": source_archive_segment_sha256,
        }
        values: dict[str, Any] = {
            "candidate_sha256": candidate,
            "workload_id": member.workload_id,
            "acquisition_job_id": acquisition_job,
            "consumer_id": member.consumer_id,
            "stream_id": member.stream_id,
            "stream_sequence": producer_receipt.stream_sequence,
            "registry_root_sha256": verification.registry_root_sha256,
            "registry_epoch_root_sha256": verification.epoch_root_sha256,
            "registry_epoch_receipt_sha256": (
                verification.epoch_receipt_sha256
            ),
            "registry_store_identity_sha256": (
                verification.store_identity_sha256
            ),
            "registry_epoch": verification.epoch,
            "registry_member_sha256": member.fingerprint,
            "lifecycle_event_id": terminal_identity.event_id,
            "lifecycle_event_sha256": terminal_identity.event_sha256,
            "lifecycle_producer_receipt_sha256": (
                terminal_identity.producer_receipt_sha256
            ),
            "lifecycle_consumption_receipt_sha256": (
                consumption_receipt.receipt_sha256
            ),
            "lifecycle_terminal_output_sha256": consumption_receipt.output_sha256,
            "lifecycle_terminal_identity_sha256": (
                terminal_identity.identity_sha256
            ),
            "lifecycle_storage_status": terminal_identity.status,
            "lifecycle_archive_segment_id": (
                terminal_identity.archive_segment_id
            ),
            "lifecycle_terminal_at": terminal_identity.terminal_at,
            "archive_source_profile_sha256": (
                member.archive_source_profile_sha256
            ),
            "archive_resolver_id": member.archive_resolver_id,
            "archive_resolver_sha256": member.archive_resolver_sha256,
            "archive_source_store_identity_sha256": (
                member.archive_source_store_identity_sha256
            ),
            "archive_reproof_contract": member.archive_reproof_contract,
            "required_archive_binding_sha256": _digest(required_archive_core),
            "source_archive_anchor_sha256": source_archive_anchor_sha256,
            "source_archive_segment_sha256": source_archive_segment_sha256,
            "source_archive_record_sha256": source_archive_record_sha256,
            "source_archive_binding_sha256": _digest(source_archive_core),
        }
        core = {
            "acquisition_job_id": values["acquisition_job_id"],
            "archive_reproof_contract": values["archive_reproof_contract"],
            "archive_resolver_id": values["archive_resolver_id"],
            "archive_resolver_sha256": values["archive_resolver_sha256"],
            "archive_source_profile_sha256": values[
                "archive_source_profile_sha256"
            ],
            "archive_source_store_identity_sha256": values[
                "archive_source_store_identity_sha256"
            ],
            "candidate_sha256": values["candidate_sha256"],
            "consumer_id": values["consumer_id"],
            "lifecycle_archive_segment_id": values[
                "lifecycle_archive_segment_id"
            ],
            "lifecycle_consumption_receipt_sha256": values[
                "lifecycle_consumption_receipt_sha256"
            ],
            "lifecycle_event_id": values["lifecycle_event_id"],
            "lifecycle_event_sha256": values["lifecycle_event_sha256"],
            "lifecycle_producer_receipt_sha256": values[
                "lifecycle_producer_receipt_sha256"
            ],
            "lifecycle_storage_status": values["lifecycle_storage_status"],
            "lifecycle_terminal_at": float(values["lifecycle_terminal_at"]),
            "lifecycle_terminal_identity_sha256": values[
                "lifecycle_terminal_identity_sha256"
            ],
            "lifecycle_terminal_kind": "consumed",
            "lifecycle_terminal_output_sha256": values[
                "lifecycle_terminal_output_sha256"
            ],
            "registry_epoch": values["registry_epoch"],
            "registry_epoch_receipt_sha256": values[
                "registry_epoch_receipt_sha256"
            ],
            "registry_epoch_root_sha256": values[
                "registry_epoch_root_sha256"
            ],
            "registry_member_sha256": values["registry_member_sha256"],
            "registry_root_sha256": values["registry_root_sha256"],
            "registry_store_identity_sha256": values[
                "registry_store_identity_sha256"
            ],
            "required_archive_binding_sha256": values[
                "required_archive_binding_sha256"
            ],
            "schema": CONSUMER_CLOSURE_SCHEMA,
            "source_archive_anchor_sha256": values[
                "source_archive_anchor_sha256"
            ],
            "source_archive_binding_sha256": values[
                "source_archive_binding_sha256"
            ],
            "source_archive_record_sha256": values[
                "source_archive_record_sha256"
            ],
            "source_archive_segment_sha256": values[
                "source_archive_segment_sha256"
            ],
            "stream_id": values["stream_id"],
            "stream_sequence": values["stream_sequence"],
            "workload_id": values["workload_id"],
        }
        return cls(receipt_sha256=_digest(core), **values)


def parse_consumer_closure_receipt(
    value: object,
) -> AcquisitionConsumerClosureReceipt:
    exact = _exact(
        value,
        {
            "acquisition_job_id",
            "archive_reproof_contract",
            "archive_resolver_id",
            "archive_resolver_sha256",
            "archive_source_profile_sha256",
            "archive_source_store_identity_sha256",
            "candidate_sha256",
            "consumer_id",
            "lifecycle_archive_segment_id",
            "lifecycle_consumption_receipt_sha256",
            "lifecycle_event_id",
            "lifecycle_event_sha256",
            "lifecycle_producer_receipt_sha256",
            "lifecycle_storage_status",
            "lifecycle_terminal_at",
            "lifecycle_terminal_identity_sha256",
            "lifecycle_terminal_kind",
            "lifecycle_terminal_output_sha256",
            "receipt_sha256",
            "registry_epoch",
            "registry_epoch_receipt_sha256",
            "registry_epoch_root_sha256",
            "registry_member_sha256",
            "registry_root_sha256",
            "registry_store_identity_sha256",
            "required_archive_binding_sha256",
            "schema",
            "source_archive_anchor_sha256",
            "source_archive_binding_sha256",
            "source_archive_record_sha256",
            "source_archive_segment_sha256",
            "stream_id",
            "stream_sequence",
            "workload_id",
        },
        "consumer closure receipt",
    )
    if (
        exact["schema"] != CONSUMER_CLOSURE_SCHEMA
        or exact["lifecycle_terminal_kind"] != "consumed"
    ):
        raise AcquisitionConsumerRegistryIntegrityError(
            "consumer closure receipt contract differs"
        )
    return AcquisitionConsumerClosureReceipt(
        receipt_sha256=exact["receipt_sha256"],
        candidate_sha256=exact["candidate_sha256"],
        workload_id=exact["workload_id"],
        acquisition_job_id=exact["acquisition_job_id"],
        consumer_id=exact["consumer_id"],
        stream_id=exact["stream_id"],
        stream_sequence=exact["stream_sequence"],
        registry_root_sha256=exact["registry_root_sha256"],
        registry_epoch_root_sha256=exact["registry_epoch_root_sha256"],
        registry_epoch_receipt_sha256=exact[
            "registry_epoch_receipt_sha256"
        ],
        registry_store_identity_sha256=exact[
            "registry_store_identity_sha256"
        ],
        registry_epoch=exact["registry_epoch"],
        registry_member_sha256=exact["registry_member_sha256"],
        lifecycle_event_id=exact["lifecycle_event_id"],
        lifecycle_event_sha256=exact["lifecycle_event_sha256"],
        lifecycle_producer_receipt_sha256=exact[
            "lifecycle_producer_receipt_sha256"
        ],
        lifecycle_consumption_receipt_sha256=exact[
            "lifecycle_consumption_receipt_sha256"
        ],
        lifecycle_terminal_output_sha256=exact[
            "lifecycle_terminal_output_sha256"
        ],
        lifecycle_terminal_identity_sha256=exact[
            "lifecycle_terminal_identity_sha256"
        ],
        lifecycle_storage_status=exact["lifecycle_storage_status"],
        lifecycle_archive_segment_id=exact["lifecycle_archive_segment_id"],
        lifecycle_terminal_at=exact["lifecycle_terminal_at"],
        archive_source_profile_sha256=exact[
            "archive_source_profile_sha256"
        ],
        archive_resolver_id=exact["archive_resolver_id"],
        archive_resolver_sha256=exact["archive_resolver_sha256"],
        archive_source_store_identity_sha256=exact[
            "archive_source_store_identity_sha256"
        ],
        archive_reproof_contract=exact["archive_reproof_contract"],
        required_archive_binding_sha256=exact[
            "required_archive_binding_sha256"
        ],
        source_archive_anchor_sha256=exact[
            "source_archive_anchor_sha256"
        ],
        source_archive_segment_sha256=exact[
            "source_archive_segment_sha256"
        ],
        source_archive_record_sha256=exact[
            "source_archive_record_sha256"
        ],
        source_archive_binding_sha256=exact[
            "source_archive_binding_sha256"
        ],
    )


_PROCESS_FENCE_LOCK = threading.Lock()
_PROCESS_FENCE_PATHS: set[str] = set()


def _normalized_path(path: Path) -> str:
    return os.path.normcase(os.path.normpath(str(path)))


def _file_witness(path: Path, label: str) -> tuple[int, int, int, int]:
    try:
        details = path.lstat()
    except OSError as error:
        raise AcquisitionConsumerRegistryIntegrityError(
            f"{label} witness is unavailable"
        ) from error
    if (
        path.is_symlink()
        or not stat.S_ISREG(details.st_mode)
        or int(details.st_nlink) != 1
    ):
        raise AcquisitionConsumerRegistryIntegrityError(f"{label} path is unsafe")
    return (
        int(details.st_dev),
        int(details.st_ino),
        int(details.st_mode),
        int(details.st_nlink),
    )


def _root_from_mapping(value: object) -> AcquisitionConsumerRegistryRoot:
    exact = _exact(
        value,
        {
            "archive_reproof_authority_id",
            "archive_reproof_authority_sha256",
            "archive_source_catalog_id",
            "archive_source_catalog_sha256",
            "contract",
            "member_count",
            "members",
            "registry_id",
            "registry_root_sha256",
            "runtime_wiring",
            "schema",
            "terminal_consumer_catalog_sha256",
        },
        "consumer registry root",
    )
    if (
        exact["archive_reproof_authority_id"] != ALL_ADAPTER_AUTHORITY_ID
        or exact["archive_source_catalog_id"] != ALL_ADAPTER_CATALOG_ID
        or exact["contract"] != REGISTRY_CONTRACT
        or exact["member_count"] != EXPECTED_MEMBER_COUNT
        or exact["registry_id"] != REGISTRY_ID
        or exact["runtime_wiring"] != "inert"
        or exact["schema"] != ROOT_SCHEMA
        or not isinstance(exact["members"], list)
    ):
        raise AcquisitionConsumerRegistryIntegrityError(
            "consumer registry root contract differs"
        )
    members: list[AcquisitionConsumerRegistryMember] = []
    for value_member in exact["members"]:
        member = _exact(
            value_member,
            {
                "archive_reproof_contract",
                "archive_resolver_id",
                "archive_resolver_sha256",
                "archive_source_profile_sha256",
                "archive_source_store_identity_sha256",
                "consumer_binding_sha256",
                "consumer_id",
                "schema",
                "stream_id",
                "workload_id",
            },
            "consumer registry member",
        )
        if member["schema"] != MEMBER_SCHEMA:
            raise AcquisitionConsumerRegistryIntegrityError(
                "consumer registry member contract differs"
            )
        members.append(
            AcquisitionConsumerRegistryMember(
                workload_id=member["workload_id"],
                consumer_id=member["consumer_id"],
                stream_id=member["stream_id"],
                consumer_binding_sha256=member["consumer_binding_sha256"],
                archive_source_profile_sha256=member[
                    "archive_source_profile_sha256"
                ],
                archive_resolver_id=member["archive_resolver_id"],
                archive_resolver_sha256=member["archive_resolver_sha256"],
                archive_source_store_identity_sha256=member[
                    "archive_source_store_identity_sha256"
                ],
                archive_reproof_contract=member["archive_reproof_contract"],
            )
        )
    root = AcquisitionConsumerRegistryRoot(
        terminal_consumer_catalog_sha256=exact[
            "terminal_consumer_catalog_sha256"
        ],
        archive_source_catalog_sha256=exact["archive_source_catalog_sha256"],
        archive_reproof_authority_sha256=exact[
            "archive_reproof_authority_sha256"
        ],
        members=tuple(members),
    )
    if root.root_sha256 != _sha(
        exact["registry_root_sha256"],
        "stored consumer registry root SHA-256",
    ):
        raise AcquisitionConsumerRegistryIntegrityError(
            "stored consumer registry root checksum differs"
        )
    _assert_private_safe(root.as_dict())
    return root


class _OwnerFileFence:
    """Cross-process OS lock plus same-process duplicate-owner exclusion."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._key = _normalized_path(path)
        self._handle: Any | None = None
        self._witness: tuple[int, int, int, int] | None = None
        self._pid = os.getpid()
        with _PROCESS_FENCE_LOCK:
            if self._key in _PROCESS_FENCE_PATHS:
                raise AcquisitionConsumerRegistryOwnershipError(
                    "consumer registry already has a process owner"
                )
            _PROCESS_FENCE_PATHS.add(self._key)
        try:
            flags = os.O_RDWR | os.O_CREAT
            flags |= getattr(os, "O_BINARY", 0)
            flags |= getattr(os, "O_CLOEXEC", 0)
            flags |= getattr(os, "O_NOFOLLOW", 0)
            descriptor = os.open(path, flags, 0o600)
            try:
                os.set_inheritable(descriptor, False)
                details = os.fstat(descriptor)
                if (
                    not stat.S_ISREG(details.st_mode)
                    or int(details.st_nlink) != 1
                ):
                    raise AcquisitionConsumerRegistryIntegrityError(
                        "consumer registry owner fence is unsafe"
                    )
                if int(details.st_size) == 0:
                    os.write(descriptor, b"\0")
                    os.fsync(descriptor)
                handle = os.fdopen(descriptor, "r+b", buffering=0)
                descriptor = -1
            finally:
                if descriptor >= 0:
                    os.close(descriptor)
            self._acquire_os_lock(handle)
            self._handle = handle
            self._witness = _file_witness(
                path,
                "consumer registry owner fence",
            )
            self.assert_held()
        except BaseException:
            self.close()
            raise

    @staticmethod
    def _acquire_os_lock(handle: Any) -> None:
        try:
            if os.name == "nt":
                import msvcrt

                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (OSError, IOError) as error:
            handle.close()
            raise AcquisitionConsumerRegistryOwnershipError(
                "consumer registry OS owner fence is held"
            ) from error

    @staticmethod
    def _release_os_lock(handle: Any) -> None:
        if os.name == "nt":
            import msvcrt

            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    @property
    def witness(self) -> tuple[int, int, int, int]:
        self.assert_held()
        assert self._witness is not None
        return self._witness

    def assert_held(self) -> None:
        if self._handle is None or self._witness is None:
            raise AcquisitionConsumerRegistryOwnershipError(
                "consumer registry owner fence is closed"
            )
        if os.getpid() != self._pid:
            raise AcquisitionConsumerRegistryOwnershipError(
                "consumer registry owner fence crossed a process boundary"
            )
        details = os.fstat(self._handle.fileno())
        descriptor_witness = (
            int(details.st_dev),
            int(details.st_ino),
            int(details.st_mode),
            int(details.st_nlink),
        )
        if (
            descriptor_witness != self._witness
            or _file_witness(
                self.path,
                "consumer registry owner fence",
            )
            != self._witness
        ):
            raise AcquisitionConsumerRegistryOwnershipError(
                "consumer registry owner fence instance changed"
            )

    def close(self) -> None:
        handle = self._handle
        self._handle = None
        if handle is not None:
            try:
                self._release_os_lock(handle)
            except OSError:
                pass
            finally:
                handle.close()
        with _PROCESS_FENCE_LOCK:
            _PROCESS_FENCE_PATHS.discard(self._key)


class AcquisitionConsumerRegistryAuthority:
    """Single-writer durable epoch authority; intentionally not runtime-wired."""

    def __init__(
        self,
        path: str | Path,
        *,
        limits: AcquisitionConsumerRegistryLimits | None = None,
        clock: Callable[[], float] = time.time,
        token_factory: Callable[[], str] | None = None,
        fault_injector: Callable[[str], None] | None = None,
    ) -> None:
        self.path = Path(os.path.abspath(os.fspath(path)))
        self.lock_path = self.path.with_name(self.path.name + ".owner.lock")
        self.limits = limits or AcquisitionConsumerRegistryLimits()
        if not callable(clock):
            raise TypeError("consumer registry clock is not callable")
        if token_factory is not None and not callable(token_factory):
            raise TypeError("consumer registry token factory is not callable")
        if fault_injector is not None and not callable(fault_injector):
            raise TypeError("consumer registry fault injector is not callable")
        self._clock = clock
        self._token_factory = token_factory or (lambda: secrets.token_hex(32))
        self._fault_injector = fault_injector
        self._lock = threading.RLock()
        self._closed = False
        self._database_witness: tuple[int, int, int, int]
        self._store_instance_sha256: str
        self._owner_epoch: int
        self._owner_token_sha256: str
        self._owner_process_id = os.getpid()
        self._owner_fence: _OwnerFileFence | None = None
        self._prepare_parent()
        try:
            self._owner_fence = _OwnerFileFence(self.lock_path)
            self._database_witness = self._prepare_database_path()
            self._initialize()
            self._claim_durable_owner()
        except BaseException:
            self.close()
            raise

    def __enter__(self) -> "AcquisitionConsumerRegistryAuthority":
        self._assert_open()
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            fence = self._owner_fence
            self._owner_fence = None
            if fence is not None:
                fence.close()

    def _assert_open(self) -> None:
        if self._closed:
            raise AcquisitionConsumerRegistryOwnershipError(
                "consumer registry authority is closed"
            )
        if os.getpid() != self._owner_process_id:
            raise AcquisitionConsumerRegistryOwnershipError(
                "consumer registry authority crossed a process boundary"
            )
        if self._owner_fence is None:
            raise AcquisitionConsumerRegistryOwnershipError(
                "consumer registry owner fence is unavailable"
            )
        self._owner_fence.assert_held()

    def _fault(self, point: str) -> None:
        if self._fault_injector is not None:
            self._fault_injector(point)

    def _prepare_parent(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        parent = self.path.parent
        for candidate in (parent, *parent.parents):
            try:
                details = candidate.lstat()
            except OSError as error:
                raise AcquisitionConsumerRegistryIntegrityError(
                    "consumer registry parent is unavailable"
                ) from error
            if candidate.is_symlink() or not stat.S_ISDIR(details.st_mode):
                raise AcquisitionConsumerRegistryIntegrityError(
                    "consumer registry parent is unsafe"
                )
        if _normalized_path(parent.resolve(strict=True)) != _normalized_path(parent):
            raise AcquisitionConsumerRegistryIntegrityError(
                "consumer registry parent is aliased"
            )

    def _prepare_database_path(self) -> tuple[int, int, int, int]:
        if self.path.is_symlink():
            raise AcquisitionConsumerRegistryIntegrityError(
                "consumer registry database path is unsafe"
            )
        if not self.path.exists():
            flags = os.O_RDWR | os.O_CREAT | os.O_EXCL
            flags |= getattr(os, "O_BINARY", 0)
            flags |= getattr(os, "O_CLOEXEC", 0)
            flags |= getattr(os, "O_NOFOLLOW", 0)
            try:
                descriptor = os.open(self.path, flags, 0o600)
                try:
                    os.set_inheritable(descriptor, False)
                    details = os.fstat(descriptor)
                    if (
                        not stat.S_ISREG(details.st_mode)
                        or int(details.st_nlink) != 1
                    ):
                        raise AcquisitionConsumerRegistryIntegrityError(
                            "consumer registry database leaf is unsafe"
                        )
                    os.fsync(descriptor)
                finally:
                    os.close(descriptor)
            except AcquisitionConsumerRegistryError:
                raise
            except OSError as error:
                raise AcquisitionConsumerRegistryIntegrityError(
                    "consumer registry database creation failed"
                ) from error
        witness = _file_witness(self.path, "consumer registry database")
        if _normalized_path(self.path.resolve(strict=True)) != _normalized_path(
            self.path
        ):
            raise AcquisitionConsumerRegistryIntegrityError(
                "consumer registry database path is aliased"
            )
        return witness

    def _verify_database_path(self) -> None:
        self._assert_open()
        try:
            resolved = self.path.resolve(strict=True)
        except OSError as error:
            raise AcquisitionConsumerRegistryIntegrityError(
                "consumer registry database instance changed"
            ) from error
        if (
            _normalized_path(resolved) != _normalized_path(self.path)
            or _file_witness(self.path, "consumer registry database")
            != self._database_witness
        ):
            raise AcquisitionConsumerRegistryIntegrityError(
                "consumer registry database instance changed"
            )

    def _connect(self) -> sqlite3.Connection:
        self._verify_database_path()
        uri_path = quote(self.path.as_posix(), safe="/:")
        database = sqlite3.connect(
            f"file:{uri_path}?mode=rw&nofollow=1",
            timeout=self.limits.busy_timeout_ms / 1000.0,
            isolation_level=None,
            uri=True,
        )
        database.row_factory = sqlite3.Row
        try:
            self._verify_database_path()
            database.execute("PRAGMA foreign_keys=ON")
            journal = str(
                database.execute("PRAGMA journal_mode=DELETE").fetchone()[0]
            ).casefold()
            if journal != "delete":
                raise AcquisitionConsumerRegistryIntegrityError(
                    "consumer registry journal mode differs"
                )
            database.execute("PRAGMA synchronous=FULL")
            database.execute("PRAGMA trusted_schema=OFF")
            database.execute(
                f"PRAGMA busy_timeout={self.limits.busy_timeout_ms}"
            )
            page_size = int(database.execute("PRAGMA page_size").fetchone()[0])
            page_count = int(database.execute("PRAGMA page_count").fetchone()[0])
            max_pages = max(1, self.limits.max_database_bytes // page_size)
            enforced = int(
                database.execute(f"PRAGMA max_page_count={max_pages}").fetchone()[0]
            )
            if page_count > max_pages or enforced > max_pages:
                raise AcquisitionConsumerRegistryCapacityError(
                    "consumer registry database exceeds page cap"
                )
        except BaseException:
            database.close()
            raise
        return database

    @staticmethod
    def _tables(database: sqlite3.Connection) -> set[str]:
        return {
            str(row[0])
            for row in database.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
            if not str(row[0]).startswith("sqlite_")
        }

    @staticmethod
    def _definition(
        database: sqlite3.Connection,
        table: str,
    ) -> tuple[tuple[str, str, int, int], ...]:
        return tuple(
            (str(row[1]), str(row[2]), int(row[3]), int(row[5]))
            for row in database.execute(f"PRAGMA table_info({table})")
        )

    @staticmethod
    def _normalized_sql(value: str) -> str:
        return " ".join(value.split()).casefold()

    @classmethod
    def _trigger_definitions(cls) -> dict[str, str]:
        return {
            "registry_metadata_no_update": (
                "CREATE TRIGGER registry_metadata_no_update BEFORE UPDATE ON "
                "registry_metadata BEGIN SELECT RAISE(ABORT,'immutable'); END"
            ),
            "registry_metadata_no_delete": (
                "CREATE TRIGGER registry_metadata_no_delete BEFORE DELETE ON "
                "registry_metadata BEGIN SELECT RAISE(ABORT,'immutable'); END"
            ),
            "registry_roots_no_update": (
                "CREATE TRIGGER registry_roots_no_update BEFORE UPDATE ON "
                "registry_roots BEGIN SELECT RAISE(ABORT,'immutable'); END"
            ),
            "registry_roots_no_delete": (
                "CREATE TRIGGER registry_roots_no_delete BEFORE DELETE ON "
                "registry_roots BEGIN SELECT RAISE(ABORT,'immutable'); END"
            ),
            "registry_epochs_no_update": (
                "CREATE TRIGGER registry_epochs_no_update BEFORE UPDATE ON "
                "registry_epochs BEGIN SELECT RAISE(ABORT,'immutable'); END"
            ),
            "registry_epochs_no_delete": (
                "CREATE TRIGGER registry_epochs_no_delete BEFORE DELETE ON "
                "registry_epochs BEGIN SELECT RAISE(ABORT,'immutable'); END"
            ),
            "registry_epochs_monotone_insert": (
                "CREATE TRIGGER registry_epochs_monotone_insert BEFORE INSERT ON "
                "registry_epochs WHEN NEW.epoch != COALESCE((SELECT MAX(epoch)+1 "
                "FROM registry_epochs),1) OR (NEW.epoch=1 AND "
                "NEW.previous_epoch_root_sha256 IS NOT NULL) OR (NEW.epoch>1 AND "
                "NEW.previous_epoch_root_sha256 IS NOT (SELECT epoch_root_sha256 "
                "FROM registry_epochs ORDER BY epoch DESC LIMIT 1)) BEGIN SELECT "
                "RAISE(ABORT,'non_monotone'); END"
            ),
            "registry_owner_no_delete": (
                "CREATE TRIGGER registry_owner_no_delete BEFORE DELETE ON "
                "registry_owner BEGIN SELECT RAISE(ABORT,'owner_fence'); END"
            ),
            "registry_owner_update_guard": (
                "CREATE TRIGGER registry_owner_update_guard BEFORE UPDATE ON "
                "registry_owner WHEN NEW.singleton != 1 OR NEW.owner_epoch != "
                "OLD.owner_epoch+1 OR length(NEW.owner_token_sha256) != 64 OR "
                "NEW.owner_token_sha256 GLOB '*[^0-9a-f]*' OR "
                "NEW.owner_process_id <= 0 OR NEW.acquired_at < 0 BEGIN SELECT "
                "RAISE(ABORT,'owner_fence'); END"
            ),
        }

    @classmethod
    def _install_triggers(cls, database: sqlite3.Connection) -> None:
        for statement in cls._trigger_definitions().values():
            database.execute(statement)

    @classmethod
    def _validate_triggers(cls, database: sqlite3.Connection) -> None:
        actual = {
            str(row[0]): str(row[1])
            for row in database.execute(
                "SELECT name,sql FROM sqlite_master WHERE type='trigger'"
            )
        }
        expected = cls._trigger_definitions()
        if set(actual) != set(expected) or any(
            cls._normalized_sql(actual[name])
            != cls._normalized_sql(statement)
            for name, statement in expected.items()
        ):
            raise AcquisitionConsumerRegistryIntegrityError(
                "consumer registry immutability triggers differ"
            )

    @staticmethod
    def _expected_definitions() -> dict[
        str,
        tuple[tuple[str, str, int, int], ...],
    ]:
        return {
            "registry_metadata": (
                ("singleton", "INTEGER", 0, 1),
                ("contract", "TEXT", 1, 0),
                ("schema_version", "INTEGER", 1, 0),
                ("max_epochs", "INTEGER", 1, 0),
                ("max_root_bytes", "INTEGER", 1, 0),
                ("max_database_bytes", "INTEGER", 1, 0),
                ("busy_timeout_ms", "INTEGER", 1, 0),
                ("limits_sha256", "TEXT", 1, 0),
                ("store_instance_sha256", "TEXT", 1, 0),
            ),
            "registry_owner": (
                ("singleton", "INTEGER", 0, 1),
                ("owner_epoch", "INTEGER", 1, 0),
                ("owner_token_sha256", "TEXT", 1, 0),
                ("owner_process_id", "INTEGER", 1, 0),
                ("acquired_at", "REAL", 1, 0),
            ),
            "registry_roots": (
                ("registry_root_sha256", "TEXT", 0, 1),
                ("root_json", "BLOB", 1, 0),
                ("root_bytes", "INTEGER", 1, 0),
                ("terminal_catalog_sha256", "TEXT", 1, 0),
                ("archive_catalog_sha256", "TEXT", 1, 0),
                ("archive_reproof_sha256", "TEXT", 1, 0),
                ("registered_at", "REAL", 1, 0),
            ),
            "registry_epochs": (
                ("epoch", "INTEGER", 0, 1),
                ("registry_root_sha256", "TEXT", 1, 0),
                ("previous_epoch_root_sha256", "TEXT", 0, 0),
                ("authority_instance_sha256", "TEXT", 1, 0),
                ("created_at", "REAL", 1, 0),
                ("epoch_root_sha256", "TEXT", 1, 0),
                ("receipt_sha256", "TEXT", 1, 0),
            ),
        }

    @staticmethod
    def _validate_unique_indexes(database: sqlite3.Connection) -> None:
        expected = {
            "registry_roots": {"registry_root_sha256", "root_json"},
            "registry_epochs": {
                "registry_root_sha256",
                "previous_epoch_root_sha256",
                "epoch_root_sha256",
                "receipt_sha256",
            },
        }
        for table, expected_columns in expected.items():
            actual: set[str] = set()
            for row in database.execute(f"PRAGMA index_list({table})"):
                if int(row[2]) != 1:
                    continue
                members = tuple(
                    database.execute(
                        "SELECT name FROM pragma_index_info(?)",
                        (str(row[1]),),
                    )
                )
                if len(members) != 1:
                    raise AcquisitionConsumerRegistryIntegrityError(
                        "consumer registry uniqueness arity differs"
                    )
                actual.add(str(members[0][0]))
            if actual != expected_columns:
                raise AcquisitionConsumerRegistryIntegrityError(
                    "consumer registry uniqueness differs"
                )

    def _create_schema(self, database: sqlite3.Connection) -> None:
        database.execute(
            """CREATE TABLE registry_metadata(
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                contract TEXT NOT NULL,
                schema_version INTEGER NOT NULL,
                max_epochs INTEGER NOT NULL,
                max_root_bytes INTEGER NOT NULL,
                max_database_bytes INTEGER NOT NULL,
                busy_timeout_ms INTEGER NOT NULL,
                limits_sha256 TEXT NOT NULL,
                store_instance_sha256 TEXT NOT NULL
            )"""
        )
        database.execute(
            """CREATE TABLE registry_owner(
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                owner_epoch INTEGER NOT NULL,
                owner_token_sha256 TEXT NOT NULL,
                owner_process_id INTEGER NOT NULL,
                acquired_at REAL NOT NULL
            )"""
        )
        database.execute(
            """CREATE TABLE registry_roots(
                registry_root_sha256 TEXT PRIMARY KEY,
                root_json BLOB NOT NULL UNIQUE,
                root_bytes INTEGER NOT NULL CHECK(root_bytes > 0),
                terminal_catalog_sha256 TEXT NOT NULL,
                archive_catalog_sha256 TEXT NOT NULL,
                archive_reproof_sha256 TEXT NOT NULL,
                registered_at REAL NOT NULL CHECK(registered_at >= 0)
            )"""
        )
        database.execute(
            """CREATE TABLE registry_epochs(
                epoch INTEGER PRIMARY KEY CHECK(epoch > 0),
                registry_root_sha256 TEXT NOT NULL UNIQUE,
                previous_epoch_root_sha256 TEXT UNIQUE,
                authority_instance_sha256 TEXT NOT NULL,
                created_at REAL NOT NULL CHECK(created_at >= 0),
                epoch_root_sha256 TEXT NOT NULL UNIQUE,
                receipt_sha256 TEXT NOT NULL UNIQUE,
                FOREIGN KEY(registry_root_sha256)
                    REFERENCES registry_roots(registry_root_sha256)
            )"""
        )
        instance_sha = secrets.token_hex(32)
        database.execute(
            "INSERT INTO registry_metadata VALUES(1,?,?,?,?,?,?,?,?)",
            (
                STORE_CONTRACT,
                STORE_SCHEMA_VERSION,
                self.limits.max_epochs,
                self.limits.max_root_bytes,
                self.limits.max_database_bytes,
                self.limits.busy_timeout_ms,
                self.limits.fingerprint,
                instance_sha,
            ),
        )
        database.execute(
            "INSERT INTO registry_owner VALUES(1,0,?,0,0.0)",
            ("0" * 64,),
        )
        self._install_triggers(database)

    def _initialize(self) -> None:
        try:
            with self._lock, closing(self._connect()) as database:
                try:
                    database.execute("BEGIN IMMEDIATE")
                    if not self._tables(database):
                        self._create_schema(database)
                    self._validate_schema(database, full=True)
                    self._fault("initialize.before_commit")
                    database.commit()
                    self._fault("initialize.after_commit")
                except BaseException:
                    if database.in_transaction:
                        database.rollback()
                    raise
            with self._lock, closing(self._connect()) as database:
                self._store_instance_sha256 = self._validate_schema(
                    database,
                    full=True,
                )
        except sqlite3.DatabaseError as error:
            raise AcquisitionConsumerRegistryIntegrityError(
                "consumer registry database is invalid"
            ) from error

    def _claim_durable_owner(self) -> None:
        token = _sha(self._token_factory(), "consumer registry owner token")
        token_sha = hashlib.sha256(token.encode("ascii")).hexdigest()
        now = _timestamp(self._clock(), "consumer registry owner clock")
        with self._lock, closing(self._connect()) as database:
            try:
                database.execute("BEGIN IMMEDIATE")
                self._validate_schema(database, full=True)
                row = database.execute(
                    "SELECT * FROM registry_owner WHERE singleton=1"
                ).fetchone()
                if row is None:
                    raise AcquisitionConsumerRegistryIntegrityError(
                        "consumer registry owner row is unavailable"
                    )
                owner_epoch = int(row["owner_epoch"]) + 1
                if owner_epoch > 2**63 - 1:
                    raise AcquisitionConsumerRegistryCapacityError(
                        "consumer registry owner epoch is exhausted"
                    )
                database.execute(
                    "UPDATE registry_owner SET owner_epoch=?,"
                    "owner_token_sha256=?,owner_process_id=?,acquired_at=? "
                    "WHERE singleton=1",
                    (owner_epoch, token_sha, os.getpid(), now),
                )
                self._fault("owner.before_commit")
                database.commit()
                self._owner_epoch = owner_epoch
                self._owner_token_sha256 = token_sha
                self._fault("owner.after_commit")
            except BaseException:
                if database.in_transaction:
                    database.rollback()
                raise

    def _identity_sha256(self, instance_sha256: str) -> str:
        assert self._owner_fence is not None
        database_witness = self._database_witness
        lock_witness = self._owner_fence.witness
        return _digest(
            {
                "contract": STORE_CONTRACT,
                "database_witness": {
                    "st_dev": database_witness[0],
                    "st_ino": database_witness[1],
                    "st_mode": database_witness[2],
                    "st_nlink": database_witness[3],
                },
                "limits_sha256": self.limits.fingerprint,
                "lock_witness": {
                    "st_dev": lock_witness[0],
                    "st_ino": lock_witness[1],
                    "st_mode": lock_witness[2],
                    "st_nlink": lock_witness[3],
                },
                "path_sha256": hashlib.sha256(
                    _normalized_path(self.path).encode("utf-8")
                ).hexdigest(),
                "schema": STORE_IDENTITY_SCHEMA,
                "schema_version": STORE_SCHEMA_VERSION,
                "store_instance_sha256": instance_sha256,
            }
        )

    @property
    def store_instance_sha256(self) -> str:
        with self._lock, closing(self._connect()) as database:
            return self._validate_schema(database)

    @property
    def identity_sha256(self) -> str:
        with self._lock, closing(self._connect()) as database:
            instance_sha = self._validate_schema(database, full=True)
            self._assert_durable_owner(database)
            return self._identity_sha256(instance_sha)

    def assert_instance_held(self, expected_identity_sha256: str | None = None) -> str:
        identity = self.identity_sha256
        if expected_identity_sha256 is not None and not hmac.compare_digest(
            identity,
            _sha(
                expected_identity_sha256,
                "expected consumer registry store identity SHA-256",
            ),
        ):
            raise AcquisitionConsumerRegistryIntegrityError(
                "consumer registry store identity authority differs"
            )
        return identity

    def _validate_metadata(self, database: sqlite3.Connection) -> str:
        row = database.execute(
            "SELECT * FROM registry_metadata WHERE singleton=1"
        ).fetchone()
        if row is None:
            raise AcquisitionConsumerRegistryIntegrityError(
                "consumer registry metadata is unavailable"
            )
        schema_version = _integer(
            row["schema_version"],
            "consumer registry schema version",
            minimum=1,
            maximum=1_000_000,
        )
        max_epochs = _integer(
            row["max_epochs"],
            "consumer registry durable epoch cap",
            minimum=1,
            maximum=1_000_000,
        )
        max_root_bytes = _integer(
            row["max_root_bytes"],
            "consumer registry durable root cap",
            minimum=4096,
            maximum=1024 * 1024,
        )
        max_database_bytes = _integer(
            row["max_database_bytes"],
            "consumer registry durable database cap",
            minimum=1024 * 1024,
            maximum=1024 * 1024 * 1024,
        )
        busy_timeout_ms = _integer(
            row["busy_timeout_ms"],
            "consumer registry durable busy timeout",
            minimum=1,
            maximum=60_000,
        )
        if (
            int(
                database.execute(
                    "SELECT count(*) FROM registry_metadata"
                ).fetchone()[0]
            )
            != 1
            or str(row["contract"]) != STORE_CONTRACT
            or schema_version != STORE_SCHEMA_VERSION
            or max_epochs != self.limits.max_epochs
            or max_root_bytes != self.limits.max_root_bytes
            or max_database_bytes != self.limits.max_database_bytes
            or busy_timeout_ms != self.limits.busy_timeout_ms
            or str(row["limits_sha256"]) != self.limits.fingerprint
        ):
            raise AcquisitionConsumerRegistryIntegrityError(
                "consumer registry metadata differs"
            )
        instance_sha = _sha(
            row["store_instance_sha256"],
            "consumer registry store instance SHA-256",
        )
        if hasattr(self, "_store_instance_sha256") and not hmac.compare_digest(
            self._store_instance_sha256,
            instance_sha,
        ):
            raise AcquisitionConsumerRegistryIntegrityError(
                "consumer registry store instance authority changed"
            )
        return instance_sha

    @staticmethod
    def _validate_owner_shape(database: sqlite3.Connection) -> sqlite3.Row:
        row = database.execute(
            "SELECT * FROM registry_owner WHERE singleton=1"
        ).fetchone()
        if (
            row is None
            or int(
                database.execute(
                    "SELECT count(*) FROM registry_owner"
                ).fetchone()[0]
            )
            != 1
        ):
            raise AcquisitionConsumerRegistryIntegrityError(
                "consumer registry owner inventory differs"
            )
        epoch = _integer(
            row["owner_epoch"],
            "consumer registry owner epoch",
            minimum=0,
            maximum=2**63 - 1,
        )
        token_sha = _sha(
            row["owner_token_sha256"],
            "consumer registry durable owner token SHA-256",
        )
        process_id = _integer(
            row["owner_process_id"],
            "consumer registry durable owner process",
            minimum=0,
            maximum=2**63 - 1,
        )
        acquired_at = _timestamp(
            row["acquired_at"],
            "consumer registry durable owner clock",
        )
        if epoch == 0:
            if token_sha != "0" * 64 or process_id != 0 or acquired_at != 0.0:
                raise AcquisitionConsumerRegistryIntegrityError(
                    "consumer registry unclaimed owner row differs"
                )
        elif process_id <= 0:
            raise AcquisitionConsumerRegistryIntegrityError(
                "consumer registry durable owner process differs"
            )
        return row

    def _assert_durable_owner(self, database: sqlite3.Connection) -> None:
        self._assert_open()
        if not hasattr(self, "_owner_epoch") or not hasattr(
            self,
            "_owner_token_sha256",
        ):
            raise AcquisitionConsumerRegistryOwnershipError(
                "consumer registry durable owner is not claimed"
            )
        row = self._validate_owner_shape(database)
        if (
            int(row["owner_epoch"]) != self._owner_epoch
            or not hmac.compare_digest(
                str(row["owner_token_sha256"]),
                self._owner_token_sha256,
            )
            or int(row["owner_process_id"]) != self._owner_process_id
            or os.getpid() != self._owner_process_id
        ):
            raise AcquisitionConsumerRegistryOwnershipError(
                "consumer registry durable owner fence differs"
            )

    def _root_from_row(self, row: sqlite3.Row) -> AcquisitionConsumerRegistryRoot:
        body = bytes(row["root_json"])
        root_bytes = _integer(
            row["root_bytes"],
            "stored consumer registry root bytes",
            minimum=1,
            maximum=self.limits.max_root_bytes,
        )
        if (
            len(body) != root_bytes
            or not 0 < len(body) <= self.limits.max_root_bytes
        ):
            raise AcquisitionConsumerRegistryCapacityError(
                "stored consumer registry root exceeds cap"
            )
        try:
            value = json.loads(body)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise AcquisitionConsumerRegistryIntegrityError(
                "stored consumer registry root JSON is corrupt"
            ) from error
        if _canonical(value) != body:
            raise AcquisitionConsumerRegistryIntegrityError(
                "stored consumer registry root is not canonical"
            )
        root = _root_from_mapping(value)
        if (
            str(row["registry_root_sha256"]) != root.root_sha256
            or str(row["terminal_catalog_sha256"])
            != root.terminal_consumer_catalog_sha256
            or str(row["archive_catalog_sha256"])
            != root.archive_source_catalog_sha256
            or str(row["archive_reproof_sha256"])
            != root.archive_reproof_authority_sha256
        ):
            raise AcquisitionConsumerRegistryIntegrityError(
                "stored consumer registry root row differs"
            )
        _timestamp(row["registered_at"], "consumer registry registration clock")
        return root

    @staticmethod
    def _epoch_from_row(
        row: sqlite3.Row,
    ) -> AcquisitionConsumerRegistryEpochReceipt:
        return AcquisitionConsumerRegistryEpochReceipt(
            epoch=row["epoch"],
            registry_root_sha256=str(row["registry_root_sha256"]),
            previous_epoch_root_sha256=(
                None
                if row["previous_epoch_root_sha256"] is None
                else str(row["previous_epoch_root_sha256"])
            ),
            authority_instance_sha256=str(row["authority_instance_sha256"]),
            created_at=row["created_at"],
            epoch_root_sha256=str(row["epoch_root_sha256"]),
            receipt_sha256=str(row["receipt_sha256"]),
        )

    def _validate_schema(
        self,
        database: sqlite3.Connection,
        *,
        full: bool = False,
    ) -> str:
        expected_tables = set(self._expected_definitions())
        if self._tables(database) != expected_tables:
            raise AcquisitionConsumerRegistryIntegrityError(
                "consumer registry schema objects differ"
            )
        for table, definition in self._expected_definitions().items():
            if self._definition(database, table) != definition:
                raise AcquisitionConsumerRegistryIntegrityError(
                    "consumer registry table columns differ"
                )
        self._validate_triggers(database)
        self._validate_unique_indexes(database)
        foreign_keys = tuple(
            (
                str(row[2]),
                str(row[3]),
                str(row[4]),
            )
            for row in database.execute("PRAGMA foreign_key_list(registry_epochs)")
        )
        if foreign_keys != (
            (
                "registry_roots",
                "registry_root_sha256",
                "registry_root_sha256",
            ),
        ):
            raise AcquisitionConsumerRegistryIntegrityError(
                "consumer registry foreign-key authority differs"
            )
        instance_sha = self._validate_metadata(database)
        self._validate_owner_shape(database)
        root_count = int(
            database.execute("SELECT count(*) FROM registry_roots").fetchone()[0]
        )
        epoch_count = int(
            database.execute("SELECT count(*) FROM registry_epochs").fetchone()[0]
        )
        if root_count != epoch_count:
            raise AcquisitionConsumerRegistryIntegrityError(
                "consumer registry root/epoch terminal counts differ"
            )
        if (
            root_count > self.limits.max_epochs
            or epoch_count > self.limits.max_epochs
        ):
            raise AcquisitionConsumerRegistryCapacityError(
                "consumer registry epoch inventory exceeds its cap"
            )
        if database.execute("PRAGMA foreign_key_check").fetchone() is not None:
            raise AcquisitionConsumerRegistryIntegrityError(
                "consumer registry contains an orphan epoch"
            )
        if full:
            if database.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                raise AcquisitionConsumerRegistryIntegrityError(
                    "consumer registry SQLite integrity check failed"
                )
            roots: dict[str, AcquisitionConsumerRegistryRoot] = {}
            for row in database.execute(
                "SELECT * FROM registry_roots ORDER BY registry_root_sha256"
            ):
                root = self._root_from_row(row)
                if root.root_sha256 in roots:
                    raise AcquisitionConsumerRegistryConflict(
                        "consumer registry duplicates a root"
                    )
                roots[root.root_sha256] = root
            epochs = tuple(
                self._epoch_from_row(row)
                for row in database.execute(
                    "SELECT * FROM registry_epochs ORDER BY epoch"
                )
            )
            previous: str | None = None
            for expected_epoch, receipt in enumerate(epochs, start=1):
                if (
                    receipt.epoch != expected_epoch
                    or receipt.previous_epoch_root_sha256 != previous
                    or receipt.registry_root_sha256 not in roots
                    or receipt.authority_instance_sha256 != instance_sha
                ):
                    raise AcquisitionConsumerRegistryIntegrityError(
                        "consumer registry epoch chain differs"
                    )
                previous = receipt.epoch_root_sha256
            if len(roots) != len(epochs):
                raise AcquisitionConsumerRegistryIntegrityError(
                    "consumer registry root/epoch relation differs"
                )
        return instance_sha

    def ensure_epoch(
        self,
        *,
        source_catalog: CompletionOutputArchiveSourceCatalog,
        source_reproof_authority: CompletionOutputArchiveSourceReproofAuthority,
    ) -> AcquisitionConsumerRegistryEpochReceipt:
        """Create one monotone epoch for a new exact root, or replay current."""

        root = build_consumer_registry_root(
            source_catalog=source_catalog,
            source_reproof_authority=source_reproof_authority,
        )
        root_body = _canonical(root.as_dict())
        _assert_private_safe(root.as_dict())
        if not 0 < len(root_body) <= self.limits.max_root_bytes:
            raise AcquisitionConsumerRegistryCapacityError(
                "consumer registry root exceeds byte cap"
            )
        with self._lock, closing(self._connect()) as database:
            try:
                database.execute("BEGIN IMMEDIATE")
                instance_sha = self._validate_schema(database, full=True)
                self._assert_durable_owner(database)
                current_row = database.execute(
                    "SELECT * FROM registry_epochs ORDER BY epoch DESC LIMIT 1"
                ).fetchone()
                existing_root_row = database.execute(
                    "SELECT * FROM registry_roots WHERE registry_root_sha256=?",
                    (root.root_sha256,),
                ).fetchone()
                if existing_root_row is not None:
                    existing_root = self._root_from_row(existing_root_row)
                    if existing_root.as_dict() != root.as_dict():
                        raise AcquisitionConsumerRegistryConflict(
                            "consumer registry root hash was reused"
                        )
                    epoch_row = database.execute(
                        "SELECT * FROM registry_epochs "
                        "WHERE registry_root_sha256=?",
                        (root.root_sha256,),
                    ).fetchone()
                    if epoch_row is None:
                        raise AcquisitionConsumerRegistryIntegrityError(
                            "consumer registry root has no epoch"
                        )
                    receipt = self._epoch_from_row(epoch_row)
                    if current_row is None or int(current_row["epoch"]) != receipt.epoch:
                        raise AcquisitionConsumerRegistryConflict(
                            "historical consumer registry root cannot be reactivated"
                        )
                    database.commit()
                    return receipt
                count = int(
                    database.execute(
                        "SELECT count(*) FROM registry_epochs"
                    ).fetchone()[0]
                )
                if count >= self.limits.max_epochs:
                    raise AcquisitionConsumerRegistryCapacityError(
                        "consumer registry epoch cap reached"
                    )
                now = _timestamp(self._clock(), "consumer registry epoch clock")
                database.execute(
                    "INSERT INTO registry_roots VALUES(?,?,?,?,?,?,?)",
                    (
                        root.root_sha256,
                        root_body,
                        len(root_body),
                        root.terminal_consumer_catalog_sha256,
                        root.archive_source_catalog_sha256,
                        root.archive_reproof_authority_sha256,
                        now,
                    ),
                )
                self._fault("epoch.after_root_insert")
                previous_root = (
                    None
                    if current_row is None
                    else str(current_row["epoch_root_sha256"])
                )
                epoch = count + 1
                receipt = AcquisitionConsumerRegistryEpochReceipt.create(
                    epoch=epoch,
                    registry_root_sha256=root.root_sha256,
                    previous_epoch_root_sha256=previous_root,
                    authority_instance_sha256=instance_sha,
                    created_at=now,
                )
                database.execute(
                    "INSERT INTO registry_epochs VALUES(?,?,?,?,?,?,?)",
                    (
                        receipt.epoch,
                        receipt.registry_root_sha256,
                        receipt.previous_epoch_root_sha256,
                        receipt.authority_instance_sha256,
                        receipt.created_at,
                        receipt.epoch_root_sha256,
                        receipt.receipt_sha256,
                    ),
                )
                self._fault("epoch.after_epoch_insert")
                inserted = database.execute(
                    "SELECT * FROM registry_epochs WHERE epoch=?",
                    (epoch,),
                ).fetchone()
                if inserted is None or self._epoch_from_row(inserted) != receipt:
                    raise AcquisitionConsumerRegistryIntegrityError(
                        "consumer registry epoch insert cannot be reproved"
                    )
                self._fault("epoch.before_commit")
                database.commit()
                self._fault("epoch.after_commit")
            except sqlite3.IntegrityError as error:
                if database.in_transaction:
                    database.rollback()
                raise AcquisitionConsumerRegistryConflict(
                    "consumer registry create-once constraint failed"
                ) from error
            except sqlite3.DatabaseError as error:
                if database.in_transaction:
                    database.rollback()
                if "full" in str(error).casefold():
                    raise AcquisitionConsumerRegistryCapacityError(
                        "consumer registry database is full"
                    ) from error
                raise AcquisitionConsumerRegistryIntegrityError(
                    "consumer registry database write failed"
                ) from error
            except BaseException:
                if database.in_transaction:
                    database.rollback()
                raise
        current = self.load_current()
        if current is None or current[0] != root or current[1] != receipt:
            raise AcquisitionConsumerRegistryIntegrityError(
                "consumer registry committed epoch cannot be reproved"
            )
        return receipt

    def load_current(
        self,
    ) -> tuple[
        AcquisitionConsumerRegistryRoot,
        AcquisitionConsumerRegistryEpochReceipt,
    ] | None:
        with self._lock, closing(self._connect()) as database:
            database.execute("BEGIN")
            self._validate_schema(database, full=True)
            self._assert_durable_owner(database)
            epoch_row = database.execute(
                "SELECT * FROM registry_epochs ORDER BY epoch DESC LIMIT 1"
            ).fetchone()
            if epoch_row is None:
                database.rollback()
                return None
            receipt = self._epoch_from_row(epoch_row)
            root_row = database.execute(
                "SELECT * FROM registry_roots WHERE registry_root_sha256=?",
                (receipt.registry_root_sha256,),
            ).fetchone()
            if root_row is None:
                raise AcquisitionConsumerRegistryIntegrityError(
                    "current consumer registry root is unavailable"
                )
            root = self._root_from_row(root_row)
            database.rollback()
            return root, receipt

    def verify_current_epoch_roots(
        self,
        *,
        claimed_registry_root_sha256: str,
        claimed_epoch_root_sha256: str,
    ) -> AcquisitionConsumerRegistryEpochVerification:
        """Fail closed unless both claims name the exact latest durable epoch."""

        claimed_registry = _sha(
            claimed_registry_root_sha256,
            "claimed consumer registry root SHA-256",
        )
        claimed_epoch = _sha(
            claimed_epoch_root_sha256,
            "claimed consumer registry epoch root SHA-256",
        )
        with self._lock, closing(self._connect()) as database:
            database.execute("BEGIN")
            instance_sha = self._validate_schema(database, full=True)
            self._assert_durable_owner(database)
            row = database.execute(
                "SELECT * FROM registry_epochs ORDER BY epoch DESC LIMIT 1"
            ).fetchone()
            if row is None:
                raise AcquisitionConsumerRegistryIntegrityError(
                    "consumer registry has no current epoch"
                )
            receipt = self._epoch_from_row(row)
            root_row = database.execute(
                "SELECT * FROM registry_roots WHERE registry_root_sha256=?",
                (receipt.registry_root_sha256,),
            ).fetchone()
            if root_row is None:
                raise AcquisitionConsumerRegistryIntegrityError(
                    "consumer registry current root is unavailable"
                )
            root = self._root_from_row(root_row)
            if (
                not hmac.compare_digest(root.root_sha256, claimed_registry)
                or not hmac.compare_digest(receipt.epoch_root_sha256, claimed_epoch)
            ):
                raise AcquisitionConsumerRegistryConflict(
                    "claimed consumer registry epoch roots are not current"
                )
            identity = self._identity_sha256(instance_sha)
            database.rollback()
        return AcquisitionConsumerRegistryEpochVerification(
            registry_root_sha256=root.root_sha256,
            epoch_root_sha256=receipt.epoch_root_sha256,
            epoch_receipt_sha256=receipt.receipt_sha256,
            epoch=receipt.epoch,
            member_count=len(root.members),
            store_identity_sha256=identity,
        )

    @contextmanager
    def hold_current_epoch(
        self,
    ) -> Iterator["AcquisitionConsumerRegistryBoundEpochVerifier"]:
        """Hold one exact current epoch under the store writer authority.

        The authority RLock and one ``BEGIN IMMEDIATE`` transaction remain
        held across the caller's critical section.  The exact root, epoch,
        receipt, store identity and durable owner are reproved before release.
        """

        with self._lock, closing(self._connect()) as database:
            database.execute("BEGIN IMMEDIATE")
            try:
                instance_sha = self._validate_schema(database, full=True)
                self._assert_durable_owner(database)
                epoch_row = database.execute(
                    "SELECT * FROM registry_epochs ORDER BY epoch DESC LIMIT 1"
                ).fetchone()
                if epoch_row is None:
                    raise AcquisitionConsumerRegistryIntegrityError(
                        "consumer registry has no current epoch"
                    )
                receipt = self._epoch_from_row(epoch_row)
                root_row = database.execute(
                    "SELECT * FROM registry_roots WHERE registry_root_sha256=?",
                    (receipt.registry_root_sha256,),
                ).fetchone()
                if root_row is None:
                    raise AcquisitionConsumerRegistryIntegrityError(
                        "consumer registry current root is unavailable"
                    )
                root = self._root_from_row(root_row)
                verification = AcquisitionConsumerRegistryEpochVerification(
                    registry_root_sha256=root.root_sha256,
                    epoch_root_sha256=receipt.epoch_root_sha256,
                    epoch_receipt_sha256=receipt.receipt_sha256,
                    epoch=receipt.epoch,
                    member_count=len(root.members),
                    store_identity_sha256=self._identity_sha256(instance_sha),
                )
                bound = AcquisitionConsumerRegistryBoundEpochVerifier(
                    authority=self,
                    registry_root=root,
                    epoch_receipt=receipt,
                    verification=verification,
                )
                yield bound
                bound.verify_held()
                self._verify_database_path()
                repeated_instance = self._validate_schema(database, full=True)
                self._assert_durable_owner(database)
                repeated_epoch = database.execute(
                    "SELECT * FROM registry_epochs ORDER BY epoch DESC LIMIT 1"
                ).fetchone()
                repeated_root = database.execute(
                    "SELECT * FROM registry_roots WHERE registry_root_sha256=?",
                    (receipt.registry_root_sha256,),
                ).fetchone()
                if (
                    repeated_instance != instance_sha
                    or repeated_epoch is None
                    or repeated_root is None
                    or self._epoch_from_row(repeated_epoch) != receipt
                    or self._root_from_row(repeated_root) != root
                    or self._identity_sha256(repeated_instance)
                    != verification.store_identity_sha256
                ):
                    raise AcquisitionConsumerRegistryIntegrityError(
                        "held current consumer registry epoch changed"
                    )
            finally:
                if database.in_transaction:
                    database.rollback()

    def _bind_current_epoch(
        self,
        *,
        expected_root: AcquisitionConsumerRegistryRoot,
        claimed_epoch_root_sha256: str,
    ) -> "AcquisitionConsumerRegistryBoundEpochVerifier":
        if not isinstance(expected_root, AcquisitionConsumerRegistryRoot):
            raise TypeError("expected consumer registry root has wrong type")
        claimed_epoch = _sha(
            claimed_epoch_root_sha256,
            "claimed consumer registry epoch root SHA-256",
        )
        with self._lock, closing(self._connect()) as database:
            database.execute("BEGIN")
            instance_sha = self._validate_schema(database, full=True)
            self._assert_durable_owner(database)
            row = database.execute(
                "SELECT * FROM registry_epochs ORDER BY epoch DESC LIMIT 1"
            ).fetchone()
            if row is None:
                raise AcquisitionConsumerRegistryIntegrityError(
                    "consumer registry has no current epoch"
                )
            receipt = self._epoch_from_row(row)
            root_row = database.execute(
                "SELECT * FROM registry_roots WHERE registry_root_sha256=?",
                (receipt.registry_root_sha256,),
            ).fetchone()
            if root_row is None:
                raise AcquisitionConsumerRegistryIntegrityError(
                    "consumer registry current root is unavailable"
                )
            root = self._root_from_row(root_row)
            if (
                root != expected_root
                or not hmac.compare_digest(
                    root.root_sha256,
                    expected_root.root_sha256,
                )
                or not hmac.compare_digest(
                    receipt.epoch_root_sha256,
                    claimed_epoch,
                )
            ):
                raise AcquisitionConsumerRegistryConflict(
                    "claimed consumer registry epoch roots are not current"
                )
            verification = AcquisitionConsumerRegistryEpochVerification(
                registry_root_sha256=root.root_sha256,
                epoch_root_sha256=receipt.epoch_root_sha256,
                epoch_receipt_sha256=receipt.receipt_sha256,
                epoch=receipt.epoch,
                member_count=len(root.members),
                store_identity_sha256=self._identity_sha256(instance_sha),
            )
            database.rollback()
        return AcquisitionConsumerRegistryBoundEpochVerifier(
            authority=self,
            registry_root=root,
            epoch_receipt=receipt,
            verification=verification,
        )

    def verifier(
        self,
        *,
        source_catalog: CompletionOutputArchiveSourceCatalog,
        source_reproof_authority: CompletionOutputArchiveSourceReproofAuthority,
    ) -> "AcquisitionConsumerRegistryEpochRootVerifier":
        expected_root = build_consumer_registry_root(
            source_catalog=source_catalog,
            source_reproof_authority=source_reproof_authority,
        )
        return AcquisitionConsumerRegistryEpochRootVerifier(
            authority=self,
            expected_store_identity_sha256=self.identity_sha256,
            expected_registry_root=expected_root,
        )

    def diagnostics(self) -> dict[str, Any]:
        with self._lock, closing(self._connect()) as database:
            database.execute("BEGIN")
            self._validate_schema(database, full=True)
            self._assert_durable_owner(database)
            root_count = int(
                database.execute("SELECT count(*) FROM registry_roots").fetchone()[0]
            )
            row = database.execute(
                "SELECT epoch,registry_root_sha256,epoch_root_sha256 "
                "FROM registry_epochs ORDER BY epoch DESC LIMIT 1"
            ).fetchone()
            database.rollback()
        result = {
            "contract": STORE_CONTRACT,
            "current_epoch": None if row is None else int(row["epoch"]),
            "current_epoch_root_sha256": (
                None if row is None else str(row["epoch_root_sha256"])
            ),
            "current_registry_root_sha256": (
                None if row is None else str(row["registry_root_sha256"])
            ),
            "healthy": True,
            "member_count": 0 if row is None else EXPECTED_MEMBER_COUNT,
            "registry_roots": root_count,
            "runtime_wiring": "inert",
        }
        if len(_canonical(result)) > MAX_DIAGNOSTIC_BYTES:
            raise AcquisitionConsumerRegistryCapacityError(
                "consumer registry diagnostics exceed cap"
            )
        return result


class AcquisitionConsumerRegistryEpochRootVerifier:
    """Instance-bound verifier intended for later reclamation injection."""

    __slots__ = (
        "_authority",
        "_expected_registry_root",
        "_expected_store_identity_sha256",
    )

    def __init__(
        self,
        *,
        authority: AcquisitionConsumerRegistryAuthority,
        expected_store_identity_sha256: str,
        expected_registry_root: AcquisitionConsumerRegistryRoot,
    ) -> None:
        if not isinstance(authority, AcquisitionConsumerRegistryAuthority):
            raise TypeError("consumer registry authority has wrong type")
        if not isinstance(expected_registry_root, AcquisitionConsumerRegistryRoot):
            raise TypeError("consumer registry verifier root has wrong type")
        self._authority = authority
        self._expected_store_identity_sha256 = _sha(
            expected_store_identity_sha256,
            "expected consumer registry verifier identity SHA-256",
        )
        self._expected_registry_root = expected_registry_root

    @property
    def store_identity_sha256(self) -> str:
        return self._expected_store_identity_sha256

    @property
    def registry_root_sha256(self) -> str:
        return self._expected_registry_root.root_sha256

    def verify(
        self,
        *,
        claimed_registry_root_sha256: str,
        claimed_epoch_root_sha256: str,
    ) -> AcquisitionConsumerRegistryEpochVerification:
        claimed_registry = _sha(
            claimed_registry_root_sha256,
            "claimed consumer registry root SHA-256",
        )
        if not hmac.compare_digest(
            claimed_registry,
            self._expected_registry_root.root_sha256,
        ):
            raise AcquisitionConsumerRegistryConflict(
                "claimed registry root differs from live archive membership"
            )
        self._authority.assert_instance_held(
            self._expected_store_identity_sha256
        )
        result = self._authority.verify_current_epoch_roots(
            claimed_registry_root_sha256=claimed_registry,
            claimed_epoch_root_sha256=claimed_epoch_root_sha256,
        )
        if not hmac.compare_digest(
            result.store_identity_sha256,
            self._expected_store_identity_sha256,
        ):
            raise AcquisitionConsumerRegistryIntegrityError(
                "consumer registry verifier store identity changed"
            )
        return result

    def bind_current_epoch(
        self,
        *,
        claimed_epoch_root_sha256: str,
    ) -> "AcquisitionConsumerRegistryBoundEpochVerifier":
        """Bind this live membership to one exact current epoch and store."""

        self._authority.assert_instance_held(
            self._expected_store_identity_sha256
        )
        bound = self._authority._bind_current_epoch(
            expected_root=self._expected_registry_root,
            claimed_epoch_root_sha256=claimed_epoch_root_sha256,
        )
        if not hmac.compare_digest(
            bound.store_identity_sha256,
            self._expected_store_identity_sha256,
        ):
            raise AcquisitionConsumerRegistryIntegrityError(
                "consumer registry bound verifier store identity changed"
            )
        return bound


@dataclass(frozen=True, slots=True, repr=False)
class AcquisitionConsumerRegistryBoundEpochVerifier:
    """Immutable current registry/epoch/store binding used by PREPARED proofs."""

    authority: AcquisitionConsumerRegistryAuthority
    registry_root: AcquisitionConsumerRegistryRoot
    epoch_receipt: AcquisitionConsumerRegistryEpochReceipt
    verification: AcquisitionConsumerRegistryEpochVerification

    def __post_init__(self) -> None:
        if not isinstance(self.authority, AcquisitionConsumerRegistryAuthority):
            raise TypeError("bound consumer registry authority has wrong type")
        if not isinstance(self.registry_root, AcquisitionConsumerRegistryRoot):
            raise TypeError("bound consumer registry root has wrong type")
        if not isinstance(
            self.epoch_receipt,
            AcquisitionConsumerRegistryEpochReceipt,
        ):
            raise TypeError("bound consumer registry epoch receipt has wrong type")
        if not isinstance(
            self.verification,
            AcquisitionConsumerRegistryEpochVerification,
        ):
            raise TypeError("bound consumer registry verification has wrong type")
        if (
            self.registry_root.root_sha256
            != self.verification.registry_root_sha256
            or self.epoch_receipt.registry_root_sha256
            != self.verification.registry_root_sha256
            or self.epoch_receipt.epoch_root_sha256
            != self.verification.epoch_root_sha256
            or self.epoch_receipt.receipt_sha256
            != self.verification.epoch_receipt_sha256
            or self.epoch_receipt.epoch != self.verification.epoch
            or len(self.registry_root.members) != self.verification.member_count
        ):
            raise AcquisitionConsumerRegistryIntegrityError(
                "bound consumer registry epoch identity differs"
            )
        self.verify_held()

    @property
    def registry_root_sha256(self) -> str:
        return self.verification.registry_root_sha256

    @property
    def epoch_root_sha256(self) -> str:
        return self.verification.epoch_root_sha256

    @property
    def epoch_receipt_sha256(self) -> str:
        return self.verification.epoch_receipt_sha256

    @property
    def store_identity_sha256(self) -> str:
        return self.verification.store_identity_sha256

    def verify_held(self) -> AcquisitionConsumerRegistryEpochVerification:
        """Reprove that the immutable constructor binding is still current."""

        self.authority.assert_instance_held(self.store_identity_sha256)
        result = self.authority.verify_current_epoch_roots(
            claimed_registry_root_sha256=self.registry_root_sha256,
            claimed_epoch_root_sha256=self.epoch_root_sha256,
        )
        if result != self.verification:
            raise AcquisitionConsumerRegistryIntegrityError(
                "bound consumer registry epoch verification changed"
            )
        return result

    def assert_disjoint_root(self, root: str | Path) -> None:
        """Refuse a coordinator root that could contain the registry store."""

        self.verify_held()
        supplied = Path(os.path.abspath(os.fspath(root)))
        for protected in (self.authority.path, self.authority.lock_path):
            exact = protected.resolve(strict=True)
            if exact == supplied or exact.is_relative_to(supplied):
                raise AcquisitionConsumerRegistryIntegrityError(
                    "consumer registry store overlaps the requested root"
                )

    def member(
        self,
        workload_id: str,
    ) -> AcquisitionConsumerRegistryMember:
        workload = _identifier(workload_id, "consumer closure workload ID")
        self.verify_held()
        member = self.registry_root.by_workload.get(workload)
        if member is None:
            raise AcquisitionConsumerRegistryConflict(
                "consumer closure workload is not registered"
            )
        return member

    def issue_consumer_closure(
        self,
        *,
        workload_id: str,
        candidate_sha256: str,
        acquisition_job_id: str,
        producer_receipt: CompletionProducerReceipt,
        consumption_receipt: ConsumptionReceipt,
        terminal_identity: TerminalEventIdentity,
        source_archive_anchor_sha256: str,
        source_archive_segment_sha256: str,
        source_archive_record_sha256: str,
    ) -> AcquisitionConsumerClosureReceipt:
        verification = self.verify_held()
        member = self.registry_root.by_workload.get(
            _identifier(workload_id, "consumer closure workload ID")
        )
        if member is None:
            raise AcquisitionConsumerRegistryConflict(
                "consumer closure workload is not registered"
            )
        return AcquisitionConsumerClosureReceipt.create(
            candidate_sha256=candidate_sha256,
            acquisition_job_id=acquisition_job_id,
            member=member,
            verification=verification,
            producer_receipt=producer_receipt,
            consumption_receipt=consumption_receipt,
            terminal_identity=terminal_identity,
            source_archive_anchor_sha256=source_archive_anchor_sha256,
            source_archive_segment_sha256=source_archive_segment_sha256,
            source_archive_record_sha256=source_archive_record_sha256,
        )

    def validate_consumer_closure(
        self,
        receipt: AcquisitionConsumerClosureReceipt,
    ) -> None:
        if not isinstance(receipt, AcquisitionConsumerClosureReceipt):
            raise TypeError("consumer closure receipt has wrong type")
        verification = self.verify_held()
        member = self.registry_root.by_workload.get(receipt.workload_id)
        if (
            member is None
            or receipt.registry_root_sha256
            != verification.registry_root_sha256
            or receipt.registry_epoch_root_sha256
            != verification.epoch_root_sha256
            or receipt.registry_epoch_receipt_sha256
            != verification.epoch_receipt_sha256
            or receipt.registry_store_identity_sha256
            != verification.store_identity_sha256
            or receipt.registry_epoch != verification.epoch
            or receipt.registry_member_sha256 != member.fingerprint
            or receipt.consumer_id != member.consumer_id
            or receipt.stream_id != member.stream_id
            or receipt.archive_source_profile_sha256
            != member.archive_source_profile_sha256
            or receipt.archive_resolver_id != member.archive_resolver_id
            or receipt.archive_resolver_sha256 != member.archive_resolver_sha256
            or receipt.archive_source_store_identity_sha256
            != member.archive_source_store_identity_sha256
            or receipt.archive_reproof_contract != member.archive_reproof_contract
        ):
            raise AcquisitionConsumerRegistryIntegrityError(
                "consumer closure differs from its bound registry member"
            )


__all__ = [
    "AcquisitionConsumerRegistryAuthority",
    "AcquisitionConsumerRegistryBoundEpochVerifier",
    "AcquisitionConsumerRegistryCapacityError",
    "AcquisitionConsumerClosureReceipt",
    "AcquisitionConsumerRegistryConflict",
    "AcquisitionConsumerRegistryEpochReceipt",
    "AcquisitionConsumerRegistryEpochRootVerifier",
    "AcquisitionConsumerRegistryEpochVerification",
    "AcquisitionConsumerRegistryError",
    "AcquisitionConsumerRegistryIntegrityError",
    "AcquisitionConsumerRegistryLimits",
    "AcquisitionConsumerRegistryMember",
    "AcquisitionConsumerRegistryOwnershipError",
    "AcquisitionConsumerRegistryPrivacyError",
    "AcquisitionConsumerRegistryRoot",
    "EXPECTED_MEMBER_COUNT",
    "CONSUMER_CLOSURE_SCHEMA",
    "REGISTRY_CONTRACT",
    "STORE_CONTRACT",
    "build_consumer_registry_root",
    "parse_consumer_closure_receipt",
]
