"""Closed science-to-operational-fact staging boundary.

This module is intentionally *not* wired into the worker service.  It can
re-verify a durable :class:`ScienceShadowReceipt`, but every receipt produced by
the current KMI/DWD science runners is structurally unadmitted and therefore
can only yield a ``withheld`` result.  A separate, conspicuously test-only,
content-addressed authority format exercises the fact-store hand-off without
creating a production admission path.

The bridge performs no tracking, forecasting, networking or publication.  A
successful synthetic call stages one exact ``CommittedOperationalFactRevision``
and returns the existing ``FactPublicationBinding`` which a later, separately
owned publication lifecycle would have to consume.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import math
import os
from pathlib import Path
import re
import stat
import threading
import time
from typing import Any, Literal, Mapping

from .desired_state import DesiredState, DesiredStateStore, Target
from .operational_fact_revision import (
    AdmissionDisposition,
    CommittedOperationalFactRevision,
    FactAdmissionReceipt,
    OperationalFactKind,
)
from .operational_fact_store import (
    OperationalFactRecord,
    OperationalFactRevisionStore,
    OperationalFactStoreConflict,
)
from .science_shadow_orchestrator import (
    SCIENCE_SHADOW_ADMISSION_STATUS,
    ScienceShadowOutcome,
    ScienceShadowReceipt,
    ScienceShadowReceiptStore,
    ScienceShadowStoreError,
    ScienceShadowWindowExpired,
)
from .tracking_core.operational_snapshot import canonical_json_bytes, validate_snapshot
from .worker_first_runtime import FactPublicationBinding


BRIDGE_CONTRACT = "stv3-science-operational-fact-bridge-v1"
BRIDGE_REQUEST_CONTRACT = "stv3-science-operational-fact-request-v1"
TARGET_FOOTPRINT_CONTRACT = "stv3-science-target-footprint-v1"
PROVENANCE_BINDING_CONTRACT = "stv3-science-fact-provenance-binding-v1"
SYNTHETIC_TEST_RECEIPT_CONTRACT = (
    "stv3-science-operational-fact-synthetic-test-receipt-v1"
)
SYNTHETIC_TEST_AUTHORITY_PREFIX = "synthetic-test:"

MAX_AUTHORITY_BYTES = 64 * 1024
MAX_BRIDGE_REQUEST_BYTES = 32 * 1024
MAX_REFERENCE_HASHES = 16
MAX_SOURCE_AGE_SECONDS = 15 * 60.0
MAX_SNAPSHOT_TTL_SECONDS = 5 * 60.0
MAX_RAIN_RATE_MM_H = 1_000.0

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SAFE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,191}$")
_SECRET_RE = re.compile(
    r"(?:authorization|password|passwd|secret|credential|api[_-]?key|"
    r"application[_-]?key|access[_-]?token|refresh[_-]?token)",
    re.IGNORECASE,
)


class ScienceOperationalFactBridgeError(RuntimeError):
    """An input, authority or durable hand-off failed closed."""


class ScienceOperationalFactAuthorityError(ScienceOperationalFactBridgeError):
    """The referenced receipt authority is absent, stale or corrupt."""


def _canonical(value: Any) -> bytes:
    try:
        body = json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError, RecursionError) as error:
        raise ScienceOperationalFactBridgeError("value is not canonical JSON") from error
    return body


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _sha(value: object, label: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ScienceOperationalFactBridgeError(f"{label} is not a lowercase SHA-256")
    return value


def _identifier(value: object, label: str) -> str:
    if (
        not isinstance(value, str)
        or _SAFE_ID_RE.fullmatch(value) is None
        or "://" in value
        or _SECRET_RE.search(value) is not None
    ):
        raise ScienceOperationalFactBridgeError(f"{label} is not a safe bounded ID")
    return value


def _text(value: object, label: str, maximum: int = 160) -> str:
    if (
        not isinstance(value, str)
        or not value
        or "\0" in value
        or len(value) > maximum
        or _SECRET_RE.search(value) is not None
    ):
        raise ScienceOperationalFactBridgeError(f"{label} is not bounded text")
    return value


def _timestamp(value: object, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ScienceOperationalFactBridgeError(f"{label} is not a timestamp")
    result = float(value)
    if not math.isfinite(result) or not 0.0 <= result <= 2**53:
        raise ScienceOperationalFactBridgeError(f"{label} is not a bounded timestamp")
    return result


def _bounded_hashes(value: object, label: str, *, allow_empty: bool) -> tuple[str, ...]:
    if not isinstance(value, tuple) or len(value) > MAX_REFERENCE_HASHES:
        raise ScienceOperationalFactBridgeError(f"{label} exceeds its item bound")
    result = tuple(_sha(item, label) for item in value)
    if result != tuple(sorted(set(result))) or (not allow_empty and not result):
        raise ScienceOperationalFactBridgeError(f"{label} is not canonical")
    return result


def _reject_secret_material(value: Any) -> None:
    stack: list[tuple[Any, int]] = [(value, 0)]
    nodes = 0
    while stack:
        current, depth = stack.pop()
        nodes += 1
        if nodes > 4_096 or depth > 24:
            raise ScienceOperationalFactBridgeError("bridge payload exceeds structural bounds")
        if isinstance(current, Mapping):
            for key, item in current.items():
                if not isinstance(key, str) or _SECRET_RE.search(key):
                    raise ScienceOperationalFactBridgeError("bridge payload contains secret material")
                stack.append((item, depth + 1))
        elif isinstance(current, (list, tuple)):
            stack.extend((item, depth + 1) for item in current)
        elif isinstance(current, str) and _SECRET_RE.search(current):
            raise ScienceOperationalFactBridgeError("bridge payload contains secret material")


@dataclass(frozen=True, slots=True)
class ScienceTargetFootprint:
    """Exact desired-target point/radius used by the science candidate."""

    target_id: str
    latitude: float
    longitude: float
    observation_radius_km: float
    position_observed_at: float | None
    location_accuracy_m: float | None
    contract: str = TARGET_FOOTPRINT_CONTRACT

    def __post_init__(self) -> None:
        _identifier(self.target_id, "target_id")
        if self.contract != TARGET_FOOTPRINT_CONTRACT:
            raise ScienceOperationalFactBridgeError("target footprint contract differs")
        for value, label, minimum, maximum in (
            (self.latitude, "latitude", -90.0, 90.0),
            (self.longitude, "longitude", -180.0, 180.0),
            (self.observation_radius_km, "observation_radius_km", 0.1, 2_000.0),
        ):
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(float(value))
                or not minimum <= float(value) <= maximum
            ):
                raise ScienceOperationalFactBridgeError(f"{label} is outside bounds")
        if self.position_observed_at is not None:
            _timestamp(self.position_observed_at, "position_observed_at")
        if self.location_accuracy_m is not None and (
            isinstance(self.location_accuracy_m, bool)
            or not isinstance(self.location_accuracy_m, (int, float))
            or not math.isfinite(float(self.location_accuracy_m))
            or not 0.0 <= float(self.location_accuracy_m) <= 1_000_000.0
        ):
            raise ScienceOperationalFactBridgeError("location_accuracy_m is outside bounds")

    @classmethod
    def from_target(cls, target: Target) -> "ScienceTargetFootprint":
        if not isinstance(target, Target):
            raise TypeError("target must be a validated DesiredState target")
        return cls(
            target_id=target.target_id,
            latitude=target.latitude,
            longitude=target.longitude,
            observation_radius_km=target.observation_radius_km,
            position_observed_at=target.position_observed_at,
            location_accuracy_m=target.location_accuracy_m,
        )

    def to_mapping(self) -> dict[str, Any]:
        return {
            "contract": self.contract,
            "latitude": float(self.latitude),
            "location_accuracy_m": (
                None if self.location_accuracy_m is None else float(self.location_accuracy_m)
            ),
            "longitude": float(self.longitude),
            "observation_radius_km": float(self.observation_radius_km),
            "position_observed_at": (
                None
                if self.position_observed_at is None
                else float(self.position_observed_at)
            ),
            "target_id": self.target_id,
        }

    @property
    def fingerprint(self) -> str:
        return _digest(self.to_mapping())


@dataclass(frozen=True, slots=True)
class ScienceFactProvenanceBinding:
    """Fixed-size provenance references; arbitrary attributes are forbidden."""

    science_provenance_sha256: str
    profile_sha256: str
    source_receipt_sha256s: tuple[str, ...]
    certificate_sha256s: tuple[str, ...]
    contract: str = PROVENANCE_BINDING_CONTRACT

    def __post_init__(self) -> None:
        _sha(self.science_provenance_sha256, "science_provenance_sha256")
        _sha(self.profile_sha256, "profile_sha256")
        _bounded_hashes(
            self.source_receipt_sha256s,
            "source_receipt_sha256s",
            allow_empty=False,
        )
        _bounded_hashes(
            self.certificate_sha256s,
            "certificate_sha256s",
            allow_empty=True,
        )
        if self.contract != PROVENANCE_BINDING_CONTRACT:
            raise ScienceOperationalFactBridgeError("provenance contract differs")

    def to_mapping(self) -> dict[str, Any]:
        return {
            "certificate_sha256s": list(self.certificate_sha256s),
            "contract": self.contract,
            "profile_sha256": self.profile_sha256,
            "science_provenance_sha256": self.science_provenance_sha256,
            "source_receipt_sha256s": list(self.source_receipt_sha256s),
        }


@dataclass(frozen=True, slots=True)
class ScienceOperationalFactRequest:
    """One bounded current-rain candidate; never a forecast or absence claim."""

    science_receipt_sha256: str
    science_attempt_id: str
    science_request_sha256: str
    canonical_run_id: str
    engine_id: str
    runtime_profile_sha256: str
    process_authority_sha256: str
    process_receipt_sha256: str
    desired_state_sha256: str
    authority_epoch: int
    snapshot_revision: int
    footprint: ScienceTargetFootprint
    provenance: ScienceFactProvenanceBinding
    product_valid_at: float
    received_at: float
    evaluated_at: float
    valid_until: float
    rain_rate_mm_h: float
    provider_id: str
    provider_display_name: str
    region_id: str
    region_display_name: str
    contract: str = BRIDGE_REQUEST_CONTRACT

    def __post_init__(self) -> None:
        for value, label in (
            (self.science_receipt_sha256, "science_receipt_sha256"),
            (self.science_request_sha256, "science_request_sha256"),
            (self.runtime_profile_sha256, "runtime_profile_sha256"),
            (self.process_authority_sha256, "process_authority_sha256"),
            (self.process_receipt_sha256, "process_receipt_sha256"),
            (self.desired_state_sha256, "desired_state_sha256"),
        ):
            _sha(value, label)
        for value, label in (
            (self.science_attempt_id, "science_attempt_id"),
            (self.engine_id, "engine_id"),
            (self.provider_id, "provider_id"),
            (self.region_id, "region_id"),
        ):
            _identifier(value, label)
        if not isinstance(self.canonical_run_id, str) or not re.fullmatch(
            r"run-[0-9a-f]{64}", self.canonical_run_id
        ):
            raise ScienceOperationalFactBridgeError("canonical_run_id is invalid")
        _text(self.provider_display_name, "provider_display_name")
        _text(self.region_display_name, "region_display_name")
        for value, label in (
            (self.authority_epoch, "authority_epoch"),
            (self.snapshot_revision, "snapshot_revision"),
        ):
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or not 1 <= value <= 2**63 - 1
            ):
                raise ScienceOperationalFactBridgeError(f"{label} is invalid")
        if not isinstance(self.footprint, ScienceTargetFootprint):
            raise TypeError("footprint has wrong type")
        if not isinstance(self.provenance, ScienceFactProvenanceBinding):
            raise TypeError("provenance has wrong type")
        product = _timestamp(self.product_valid_at, "product_valid_at")
        received = _timestamp(self.received_at, "received_at")
        evaluated = _timestamp(self.evaluated_at, "evaluated_at")
        valid_until = _timestamp(self.valid_until, "valid_until")
        if not product <= received <= evaluated < valid_until:
            raise ScienceOperationalFactBridgeError("fact timestamps are not monotonic")
        if evaluated - product > MAX_SOURCE_AGE_SECONDS:
            raise ScienceOperationalFactBridgeError("science observation is too old")
        if valid_until - evaluated > MAX_SNAPSHOT_TTL_SECONDS:
            raise ScienceOperationalFactBridgeError("fact validity exceeds the TTL")
        if (
            isinstance(self.rain_rate_mm_h, bool)
            or not isinstance(self.rain_rate_mm_h, (int, float))
            or not math.isfinite(float(self.rain_rate_mm_h))
            or not 0.0 < float(self.rain_rate_mm_h) <= MAX_RAIN_RATE_MM_H
        ):
            raise ScienceOperationalFactBridgeError("rain_rate_mm_h is outside bounds")
        if self.contract != BRIDGE_REQUEST_CONTRACT:
            raise ScienceOperationalFactBridgeError("bridge request contract differs")
        payload = self.claim_mapping()
        _reject_secret_material(payload)
        if len(_canonical(payload)) > MAX_BRIDGE_REQUEST_BYTES:
            raise ScienceOperationalFactBridgeError("bridge request exceeds byte bound")

    def claim_mapping(self) -> dict[str, Any]:
        """Claim bytes bound by the source receipt; excludes its own identity."""

        return {
            "authority_epoch": self.authority_epoch,
            "canonical_run_id": self.canonical_run_id,
            "contract": self.contract,
            "desired_state_sha256": self.desired_state_sha256,
            "engine_id": self.engine_id,
            "evaluated_at": float(self.evaluated_at),
            "footprint": self.footprint.to_mapping(),
            "process_authority_sha256": self.process_authority_sha256,
            "process_receipt_sha256": self.process_receipt_sha256,
            "product_valid_at": float(self.product_valid_at),
            "provenance": self.provenance.to_mapping(),
            "provider_display_name": self.provider_display_name,
            "provider_id": self.provider_id,
            "rain_rate_mm_h": float(self.rain_rate_mm_h),
            "received_at": float(self.received_at),
            "region_display_name": self.region_display_name,
            "region_id": self.region_id,
            "runtime_profile_sha256": self.runtime_profile_sha256,
            "science_attempt_id": self.science_attempt_id,
            "science_request_sha256": self.science_request_sha256,
            "snapshot_revision": self.snapshot_revision,
            "valid_until": float(self.valid_until),
        }

    @property
    def claim_sha256(self) -> str:
        return _digest(self.claim_mapping())

    @property
    def bridge_request_sha256(self) -> str:
        return _digest({
            "claim_sha256": self.claim_sha256,
            "science_receipt_sha256": self.science_receipt_sha256,
        })


def science_receipt_provenance_sha256(receipt: ScienceShadowReceipt) -> str:
    if not isinstance(receipt, ScienceShadowReceipt):
        raise TypeError("receipt must be ScienceShadowReceipt")
    value = None if receipt.provenance is None else receipt.provenance.to_mapping()
    return _digest({"contract": "stv3-science-receipt-provenance-v1", "value": value})


@dataclass(frozen=True, slots=True)
class VerifiedScienceFactAuthority:
    source_kind: Literal["science_shadow", "synthetic_test"]
    receipt_sha256: str
    admitted_for_staging: bool
    synthetic_test_only: bool
    blocker_codes: tuple[str, ...]
    certificate_sha256s: tuple[str, ...]
    decided_at: float | None
    valid_until: float | None

    def __post_init__(self) -> None:
        if self.source_kind not in {"science_shadow", "synthetic_test"}:
            raise ScienceOperationalFactBridgeError("authority source kind differs")
        _sha(self.receipt_sha256, "verified receipt_sha256")
        if type(self.admitted_for_staging) is not bool or type(self.synthetic_test_only) is not bool:
            raise TypeError("authority flags must be booleans")
        if self.admitted_for_staging != self.synthetic_test_only:
            raise ScienceOperationalFactBridgeError(
                "only a synthetic-test authority can admit staging"
            )
        if self.blocker_codes != tuple(sorted(set(self.blocker_codes))):
            raise ScienceOperationalFactBridgeError("blocker codes are not canonical")
        for code in self.blocker_codes:
            _identifier(code, "blocker_code")
        _bounded_hashes(
            self.certificate_sha256s,
            "verified certificate_sha256s",
            allow_empty=not self.admitted_for_staging,
        )
        if self.admitted_for_staging:
            if self.decided_at is None or self.valid_until is None:
                raise ScienceOperationalFactBridgeError(
                    "admitted authority lacks its decision window"
                )
            decided = _timestamp(self.decided_at, "verified decided_at")
            expiry = _timestamp(self.valid_until, "verified valid_until")
            if not decided < expiry or expiry - decided > MAX_SNAPSHOT_TTL_SECONDS:
                raise ScienceOperationalFactBridgeError(
                    "verified authority decision window is invalid"
                )
        elif self.decided_at is not None or self.valid_until is not None:
            raise ScienceOperationalFactBridgeError(
                "withheld authority cannot carry an admission window"
            )


class DurableScienceShadowFactAuthority:
    """Read and revalidate one receipt from the existing durable science store."""

    def __init__(self, store: ScienceShadowReceiptStore) -> None:
        if not isinstance(store, ScienceShadowReceiptStore):
            raise TypeError("store must be ScienceShadowReceiptStore")
        self._store = store

    def verify(self, request: ScienceOperationalFactRequest) -> VerifiedScienceFactAuthority:
        try:
            receipt = self._store.load_attempt(
                request.science_attempt_id,
                request_sha256=request.science_request_sha256,
            )
        except (ScienceShadowStoreError, ScienceShadowWindowExpired) as error:
            raise ScienceOperationalFactAuthorityError(
                "durable science receipt authority rejected the row"
            ) from error
        if receipt is None:
            raise ScienceOperationalFactAuthorityError("durable science receipt row is missing")
        if (
            receipt.receipt_sha256 != request.science_receipt_sha256
            or receipt.engine_id != request.engine_id
            or receipt.runtime_profile_sha256 != request.runtime_profile_sha256
            or receipt.canonical_run_id != request.canonical_run_id
            or science_receipt_provenance_sha256(receipt)
            != request.provenance.science_provenance_sha256
            or request.provenance.profile_sha256 != receipt.runtime_profile_sha256
            or receipt.receipt_sha256 not in request.provenance.source_receipt_sha256s
        ):
            raise ScienceOperationalFactAuthorityError(
                "science receipt does not exactly bind the bridge request"
            )
        blockers = set(receipt.operational_admission_blockers)
        if receipt.outcome is not ScienceShadowOutcome.ADVANCED:
            blockers.add("science_outcome_not_advanced")
        if receipt.admission_status == SCIENCE_SHADOW_ADMISSION_STATUS:
            blockers.add("science_receipt_shadow_only")
        if not receipt.process_isolation_certified:
            blockers.add("durable_process_admission_certificate_missing")
        if not receipt.meteorological_admission_granted:
            blockers.add("meteorological_admission_not_granted")
        if not blockers:
            # The v2 receipt class currently makes this path unreachable.  Keep
            # it fail-closed if a future receipt contract changes underneath us.
            blockers.add("science_receipt_contract_not_admitted_by_bridge")
        return VerifiedScienceFactAuthority(
            source_kind="science_shadow",
            receipt_sha256=receipt.receipt_sha256,
            admitted_for_staging=False,
            synthetic_test_only=False,
            blocker_codes=tuple(sorted(blockers)),
            certificate_sha256s=(),
            decided_at=None,
            valid_until=None,
        )


@dataclass(frozen=True, slots=True)
class SyntheticScienceTestReceipt:
    """Content-addressed fixture receipt; impossible to use for real targets."""

    receipt_sha256: str
    authority_id: str
    claim_sha256: str
    desired_state_sha256: str
    authority_epoch: int
    target_id: str
    target_footprint_sha256: str
    runtime_profile_sha256: str
    process_authority_sha256: str
    process_receipt_sha256: str
    provenance_sha256: str
    certificate_sha256s: tuple[str, ...]
    decided_at: float
    valid_until: float
    admitted_for_test_only: bool = True
    meteorological_truth_claimed: bool = False
    contract: str = SYNTHETIC_TEST_RECEIPT_CONTRACT

    def __post_init__(self) -> None:
        for value, label in (
            (self.receipt_sha256, "synthetic receipt_sha256"),
            (self.claim_sha256, "synthetic claim_sha256"),
            (self.desired_state_sha256, "synthetic desired_state_sha256"),
            (self.target_footprint_sha256, "synthetic target_footprint_sha256"),
            (self.runtime_profile_sha256, "synthetic runtime_profile_sha256"),
            (self.process_authority_sha256, "synthetic process_authority_sha256"),
            (self.process_receipt_sha256, "synthetic process_receipt_sha256"),
            (self.provenance_sha256, "synthetic provenance_sha256"),
        ):
            _sha(value, label)
        for value, label in (
            (self.authority_id, "synthetic authority_id"),
            (self.target_id, "synthetic target_id"),
        ):
            _identifier(value, label)
            if not value.startswith(SYNTHETIC_TEST_AUTHORITY_PREFIX):
                raise ScienceOperationalFactBridgeError(
                    f"{label} is not visibly test-only"
                )
        if (
            isinstance(self.authority_epoch, bool)
            or not isinstance(self.authority_epoch, int)
            or not 1 <= self.authority_epoch <= 2**63 - 1
        ):
            raise ScienceOperationalFactBridgeError("synthetic authority_epoch is invalid")
        _bounded_hashes(
            self.certificate_sha256s,
            "synthetic certificate_sha256s",
            allow_empty=False,
        )
        decided = _timestamp(self.decided_at, "synthetic decided_at")
        expiry = _timestamp(self.valid_until, "synthetic valid_until")
        if not decided < expiry or expiry - decided > MAX_SNAPSHOT_TTL_SECONDS:
            raise ScienceOperationalFactBridgeError("synthetic receipt validity is invalid")
        if self.admitted_for_test_only is not True or self.meteorological_truth_claimed is not False:
            raise ScienceOperationalFactBridgeError("synthetic receipt overclaims admission")
        if self.contract != SYNTHETIC_TEST_RECEIPT_CONTRACT:
            raise ScienceOperationalFactBridgeError("synthetic receipt contract differs")
        if self.receipt_sha256 != _digest(self.payload_mapping()):
            raise ScienceOperationalFactBridgeError("synthetic receipt checksum differs")

    def payload_mapping(self) -> dict[str, Any]:
        return {
            "admitted_for_test_only": self.admitted_for_test_only,
            "authority_epoch": self.authority_epoch,
            "authority_id": self.authority_id,
            "certificate_sha256s": list(self.certificate_sha256s),
            "claim_sha256": self.claim_sha256,
            "contract": self.contract,
            "decided_at": float(self.decided_at),
            "desired_state_sha256": self.desired_state_sha256,
            "meteorological_truth_claimed": self.meteorological_truth_claimed,
            "process_authority_sha256": self.process_authority_sha256,
            "process_receipt_sha256": self.process_receipt_sha256,
            "provenance_sha256": self.provenance_sha256,
            "runtime_profile_sha256": self.runtime_profile_sha256,
            "target_footprint_sha256": self.target_footprint_sha256,
            "target_id": self.target_id,
            "valid_until": float(self.valid_until),
        }

    def to_mapping(self) -> dict[str, Any]:
        return {**self.payload_mapping(), "receipt_sha256": self.receipt_sha256}

    @classmethod
    def create(
        cls,
        *,
        authority_id: str,
        request: ScienceOperationalFactRequest,
        certificate_sha256s: tuple[str, ...],
        decided_at: float,
    ) -> "SyntheticScienceTestReceipt":
        values = {
            "authority_id": authority_id,
            "claim_sha256": request.claim_sha256,
            "desired_state_sha256": request.desired_state_sha256,
            "authority_epoch": request.authority_epoch,
            "target_id": request.footprint.target_id,
            "target_footprint_sha256": request.footprint.fingerprint,
            "runtime_profile_sha256": request.runtime_profile_sha256,
            "process_authority_sha256": request.process_authority_sha256,
            "process_receipt_sha256": request.process_receipt_sha256,
            "provenance_sha256": request.provenance.science_provenance_sha256,
            "certificate_sha256s": certificate_sha256s,
            "decided_at": decided_at,
            "valid_until": request.valid_until,
        }
        temporary = object.__new__(cls)
        for name, value in values.items():
            object.__setattr__(temporary, name, value)
        object.__setattr__(temporary, "receipt_sha256", "0" * 64)
        object.__setattr__(temporary, "admitted_for_test_only", True)
        object.__setattr__(temporary, "meteorological_truth_claimed", False)
        object.__setattr__(temporary, "contract", SYNTHETIC_TEST_RECEIPT_CONTRACT)
        digest = _digest(temporary.payload_mapping())
        return cls(receipt_sha256=digest, **values)

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "SyntheticScienceTestReceipt":
        expected = {
            "admitted_for_test_only",
            "authority_epoch",
            "authority_id",
            "certificate_sha256s",
            "claim_sha256",
            "contract",
            "decided_at",
            "desired_state_sha256",
            "meteorological_truth_claimed",
            "process_authority_sha256",
            "process_receipt_sha256",
            "provenance_sha256",
            "receipt_sha256",
            "runtime_profile_sha256",
            "target_footprint_sha256",
            "target_id",
            "valid_until",
        }
        if not isinstance(value, Mapping) or set(value) != expected:
            raise ScienceOperationalFactAuthorityError(
                "synthetic authority schema is not exact"
            )
        certificates = value["certificate_sha256s"]
        if not isinstance(certificates, list):
            raise ScienceOperationalFactAuthorityError(
                "synthetic certificate list is invalid"
            )
        try:
            return cls(
                receipt_sha256=value["receipt_sha256"],
                authority_id=value["authority_id"],
                claim_sha256=value["claim_sha256"],
                desired_state_sha256=value["desired_state_sha256"],
                authority_epoch=value["authority_epoch"],
                target_id=value["target_id"],
                target_footprint_sha256=value["target_footprint_sha256"],
                runtime_profile_sha256=value["runtime_profile_sha256"],
                process_authority_sha256=value["process_authority_sha256"],
                process_receipt_sha256=value["process_receipt_sha256"],
                provenance_sha256=value["provenance_sha256"],
                certificate_sha256s=tuple(certificates),
                decided_at=value["decided_at"],
                valid_until=value["valid_until"],
                admitted_for_test_only=value["admitted_for_test_only"],
                meteorological_truth_claimed=value["meteorological_truth_claimed"],
                contract=value["contract"],
            )
        except (TypeError, ValueError, ScienceOperationalFactBridgeError) as error:
            if isinstance(error, ScienceOperationalFactAuthorityError):
                raise
            raise ScienceOperationalFactAuthorityError(
                "synthetic authority values are invalid"
            ) from error


def encode_synthetic_science_test_receipt(receipt: SyntheticScienceTestReceipt) -> bytes:
    if not isinstance(receipt, SyntheticScienceTestReceipt):
        raise TypeError("receipt must be SyntheticScienceTestReceipt")
    return _canonical(receipt.to_mapping())


class SyntheticScienceTestFileAuthority:
    """Read one immutable canonical test receipt from an authorized file."""

    def __init__(self, path: str | Path, *, expected_file_sha256: str) -> None:
        self._path = Path(path)
        if not self._path.is_absolute():
            raise ValueError("synthetic authority path must be absolute")
        self._expected_file_sha256 = _sha(
            expected_file_sha256,
            "synthetic authority file SHA",
        )

    def _read(self) -> SyntheticScienceTestReceipt:
        path = self._path
        if path.is_symlink():
            raise ScienceOperationalFactAuthorityError("synthetic authority path is unsafe")
        flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_CLOEXEC", 0)
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            descriptor = os.open(path, flags)
        except OSError as error:
            raise ScienceOperationalFactAuthorityError(
                "synthetic authority file is unavailable"
            ) from error
        try:
            before = os.fstat(descriptor)
            if not stat.S_ISREG(before.st_mode) or not 1 <= before.st_size <= MAX_AUTHORITY_BYTES:
                raise ScienceOperationalFactAuthorityError(
                    "synthetic authority file size/type is invalid"
                )
            body = b""
            while len(body) <= before.st_size:
                chunk = os.read(descriptor, min(65_536, before.st_size + 1 - len(body)))
                if not chunk:
                    break
                body += chunk
            after = os.fstat(descriptor)
        finally:
            os.close(descriptor)
        if (
            len(body) != before.st_size
            or before.st_size != after.st_size
            or before.st_mtime_ns != after.st_mtime_ns
            or before.st_ino != after.st_ino
            or hashlib.sha256(body).hexdigest() != self._expected_file_sha256
        ):
            raise ScienceOperationalFactAuthorityError(
                "synthetic authority bytes changed or checksum differs"
            )
        try:
            value = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ScienceOperationalFactAuthorityError(
                "synthetic authority JSON is invalid"
            ) from error
        if not isinstance(value, dict) or _canonical(value) != body:
            raise ScienceOperationalFactAuthorityError(
                "synthetic authority is not canonical JSON"
            )
        return SyntheticScienceTestReceipt.from_mapping(value)

    def verify(self, request: ScienceOperationalFactRequest) -> VerifiedScienceFactAuthority:
        receipt = self._read()
        expected = {
            "claim_sha256": request.claim_sha256,
            "desired_state_sha256": request.desired_state_sha256,
            "authority_epoch": request.authority_epoch,
            "target_id": request.footprint.target_id,
            "target_footprint_sha256": request.footprint.fingerprint,
            "runtime_profile_sha256": request.runtime_profile_sha256,
            "process_authority_sha256": request.process_authority_sha256,
            "process_receipt_sha256": request.process_receipt_sha256,
            "provenance_sha256": request.provenance.science_provenance_sha256,
            "valid_until": float(request.valid_until),
        }
        actual = {name: getattr(receipt, name) for name in expected}
        if (
            receipt.receipt_sha256 != request.science_receipt_sha256
            or actual != expected
            or receipt.certificate_sha256s
            != request.provenance.certificate_sha256s
            or receipt.decided_at > request.evaluated_at
        ):
            raise ScienceOperationalFactAuthorityError(
                "synthetic test receipt does not bind the exact request"
            )
        return VerifiedScienceFactAuthority(
            source_kind="synthetic_test",
            receipt_sha256=receipt.receipt_sha256,
            admitted_for_staging=True,
            synthetic_test_only=True,
            blocker_codes=(),
            certificate_sha256s=receipt.certificate_sha256s,
            decided_at=receipt.decided_at,
            valid_until=receipt.valid_until,
        )


@dataclass(frozen=True, slots=True)
class ScienceOperationalFactBridgeResult:
    status: Literal["staged", "duplicate", "withheld", "rejected"]
    bridge_request_sha256: str
    science_receipt_sha256: str
    reason_codes: tuple[str, ...]
    binding: FactPublicationBinding | None
    created_ready_fact: bool
    contract: str = BRIDGE_CONTRACT

    def __post_init__(self) -> None:
        if self.status not in {"staged", "duplicate", "withheld", "rejected"}:
            raise ScienceOperationalFactBridgeError("bridge result status differs")
        _sha(self.bridge_request_sha256, "bridge_request_sha256")
        _sha(self.science_receipt_sha256, "science_receipt_sha256")
        if self.reason_codes != tuple(sorted(set(self.reason_codes))):
            raise ScienceOperationalFactBridgeError("result reasons are not canonical")
        for reason in self.reason_codes:
            _identifier(reason, "reason_code")
        if type(self.created_ready_fact) is not bool:
            raise TypeError("created_ready_fact must be boolean")
        if self.status == "staged":
            if self.binding is None or not self.created_ready_fact or self.reason_codes:
                raise ScienceOperationalFactBridgeError("staged result is inconsistent")
        elif self.status == "duplicate":
            if self.binding is None or self.created_ready_fact or self.reason_codes != (
                "duplicate_request",
            ):
                raise ScienceOperationalFactBridgeError("duplicate result is inconsistent")
        elif self.binding is not None or self.created_ready_fact or not self.reason_codes:
            raise ScienceOperationalFactBridgeError("non-staged result is inconsistent")
        if self.contract != BRIDGE_CONTRACT:
            raise ScienceOperationalFactBridgeError("bridge result contract differs")

    def to_mapping(self) -> dict[str, Any]:
        binding = None
        if self.binding is not None:
            binding = {
                "authority_epoch": self.binding.authority_epoch,
                "desired_state_sha256": self.binding.desired_state_sha256,
                "pipeline_item_id": self.binding.pipeline_item_id,
                "process_receipt_sha256": self.binding.process_receipt_sha256,
                "revision_sha256": self.binding.revision_sha256,
                "snapshot_revision": self.binding.snapshot_revision,
                "snapshot_sha256": self.binding.snapshot_sha256,
            }
        return {
            "binding": binding,
            "bridge_request_sha256": self.bridge_request_sha256,
            "contract": self.contract,
            "created_ready_fact": self.created_ready_fact,
            "reason_codes": list(self.reason_codes),
            "science_receipt_sha256": self.science_receipt_sha256,
            "status": self.status,
        }


def _target_for_request(desired: DesiredState, request: ScienceOperationalFactRequest) -> Target:
    matches = tuple(
        item for item in desired.targets if item.target_id == request.footprint.target_id
    )
    if len(matches) != 1:
        raise ScienceOperationalFactBridgeError("request target is not in desired state")
    target = matches[0]
    if not target.available or ScienceTargetFootprint.from_target(target) != request.footprint:
        raise ScienceOperationalFactBridgeError(
            "request footprint differs from active desired state"
        )
    return target


def _synthetic_snapshot(
    desired: DesiredState,
    request: ScienceOperationalFactRequest,
    target: Target,
) -> dict[str, Any]:
    unknown_ground = {
        "confirmation": None,
        "evidence_valid_at": None,
        "freshness": "unknown",
        "observed_at_max": None,
        "observed_at_min": None,
        "physical_site_count": 0,
        "reason_codes": ["synthetic_test_ground_not_claimed"],
        "received_at": None,
        "same_track": None,
        "source_ids": [],
        "status": "unknown",
        "wet_site_count": 0,
    }
    unknown_threat = {
        "calibrated_probability_percent": None,
        "distance_km": None,
        "eta_minutes": None,
        "freshness": "unknown",
        "passage_status": "unknown",
        "probability_calibration_id": None,
        "product_valid_at": None,
        "provider_ids": [],
        "reason_codes": ["synthetic_test_forecast_not_claimed"],
        "state": "unknown",
        "track_id": None,
    }
    return validate_snapshot({
        "contract": "operational_snapshot_v1",
        "desired_state_sha256": desired.state_sha256,
        "evaluated_at": float(request.evaluated_at),
        "map_artifact": {"artifact_handle": None, "etag": None},
        "product_valid_at": float(request.product_valid_at),
        "providers": {
            "complete": True,
            "items": [{
                "display_name": request.provider_display_name,
                "error": None,
                "freshness": "current",
                "product_valid_at": float(request.product_valid_at),
                "provider_id": request.provider_id,
                "reason_codes": ["synthetic_test_authority_only"],
                "received_at": float(request.received_at),
                "state": "operational",
            }],
            "overflow_count": 0,
        },
        "provenance_complete": False,
        "provenance_reason_codes": ["synthetic_test_no_real_admission"],
        "provenance_source_ids": [request.provider_id],
        "received_at": float(request.received_at),
        "regions": {
            "complete": True,
            "items": [{
                "display_name": request.region_display_name,
                "provider_ids": [request.provider_id],
                "region_id": request.region_id,
                "state": "current",
                "target_ids": [target.target_id],
            }],
            "overflow_count": 0,
        },
        "revision": request.snapshot_revision,
        "targets": {
            "complete": True,
            "items": [{
                "current_precipitation": {
                    "freshness": "current",
                    "ground_evidence": unknown_ground,
                    "product_valid_at": float(request.product_valid_at),
                    "provider_ids": [request.provider_id],
                    "rain_rate_mm_h": float(request.rain_rate_mm_h),
                    "reason_codes": ["synthetic_test_positive_pixel"],
                    "state": "raining",
                },
                "display_name": target.display_name,
                "next_threat": unknown_threat,
                "region_id": request.region_id,
                "status_text": "Synthetic test only",
                "target_id": target.target_id,
            }],
            "overflow_count": 0,
        },
        "ui_storms": {"complete": True, "items": [], "overflow_count": 0},
        "valid_until": float(request.valid_until),
        "worker": {
            "failed_jobs": 0,
            "queue_depth": 0,
            "state": "online",
            "status_text": "Synthetic test authority; not meteorological truth",
        },
    })


def _synthetic_revision(
    desired: DesiredState,
    request: ScienceOperationalFactRequest,
    target: Target,
    authority: VerifiedScienceFactAuthority,
) -> CommittedOperationalFactRevision:
    snapshot = _synthetic_snapshot(desired, request, target)
    values = (
        (
            OperationalFactKind.REGION_CURRENT,
            request.region_id,
            snapshot["regions"]["items"][0],
        ),
        (
            OperationalFactKind.PROVIDER_CURRENT,
            request.provider_id,
            snapshot["providers"]["items"][0],
        ),
        (
            OperationalFactKind.CURRENT_RAINING,
            target.target_id,
            snapshot["targets"]["items"][0]["current_precipitation"],
        ),
    )
    receipts = tuple(
        FactAdmissionReceipt.create(
            fact_kind=kind,
            scope_id=scope_id,
            fact_value=value,
            desired_state_sha256=desired.state_sha256,
            authority_epoch=desired.authority_epoch,
            source_receipt_sha256s=(authority.receipt_sha256,),
            certificate_sha256s=authority.certificate_sha256s,
            process_receipt_sha256=request.process_receipt_sha256,
            full_scan_complete=False,
            external_truth_validated=False,
            decided_at=float(authority.decided_at),
            valid_until=float(authority.valid_until),
            disposition=AdmissionDisposition.ADMITTED,
            reason_codes=("synthetic_test_authority_only",),
        )
        for kind, scope_id, value in values
    )
    return CommittedOperationalFactRevision.create(
        desired_state_sha256=desired.state_sha256,
        authority_epoch=desired.authority_epoch,
        snapshot=snapshot,
        admission_receipts=receipts,
    )


def _publication_binding(
    record: OperationalFactRecord,
    request: ScienceOperationalFactRequest,
) -> FactPublicationBinding:
    snapshot_sha = hashlib.sha256(canonical_json_bytes(record.revision.snapshot)).hexdigest()
    return FactPublicationBinding(
        pipeline_item_id=record.pipeline_item_id,
        revision_sha256=record.revision_sha256,
        snapshot_sha256=snapshot_sha,
        process_receipt_sha256=request.process_receipt_sha256,
        desired_state_sha256=record.desired_state_sha256,
        authority_epoch=record.authority_epoch,
        snapshot_revision=record.snapshot_revision,
    )


class ScienceOperationalFactBridge:
    """Fail-closed, single-item staging bridge with no publication side effect."""

    def __init__(
        self,
        *,
        desired_state_store: DesiredStateStore,
        fact_revision_store: OperationalFactRevisionStore,
        clock: Any = time.time,
    ) -> None:
        if not isinstance(desired_state_store, DesiredStateStore):
            raise TypeError("desired_state_store has wrong type")
        if not isinstance(fact_revision_store, OperationalFactRevisionStore):
            raise TypeError("fact_revision_store has wrong type")
        if not callable(clock):
            raise TypeError("clock must be callable")
        self._desired = desired_state_store
        self._facts = fact_revision_store
        self._clock = clock
        self._lock = threading.RLock()

    @staticmethod
    def _result(
        request: ScienceOperationalFactRequest,
        *,
        status: Literal["staged", "duplicate", "withheld", "rejected"],
        reasons: tuple[str, ...] = (),
        binding: FactPublicationBinding | None = None,
        created: bool = False,
    ) -> ScienceOperationalFactBridgeResult:
        return ScienceOperationalFactBridgeResult(
            status=status,
            bridge_request_sha256=request.bridge_request_sha256,
            science_receipt_sha256=request.science_receipt_sha256,
            reason_codes=tuple(sorted(set(reasons))),
            binding=binding,
            created_ready_fact=created,
        )

    def stage(
        self,
        request: ScienceOperationalFactRequest,
        *,
        authority: DurableScienceShadowFactAuthority | SyntheticScienceTestFileAuthority,
    ) -> ScienceOperationalFactBridgeResult:
        if not isinstance(request, ScienceOperationalFactRequest):
            raise TypeError("request has wrong type")
        if type(authority) not in {
            DurableScienceShadowFactAuthority,
            SyntheticScienceTestFileAuthority,
        }:
            raise TypeError("authority type is not admitted by the bridge")
        with self._lock:
            try:
                verified = authority.verify(request)
            except ScienceOperationalFactAuthorityError:
                return self._result(
                    request,
                    status="rejected",
                    reasons=("receipt_authority_rejected",),
                )
            desired = self._desired.load_active()
            if desired is None:
                return self._result(
                    request,
                    status="rejected",
                    reasons=("desired_state_unavailable",),
                )
            if (
                desired.state_sha256 != request.desired_state_sha256
                or desired.authority_epoch != request.authority_epoch
            ):
                return self._result(
                    request,
                    status="rejected",
                    reasons=("desired_state_stale",),
                )
            try:
                target = _target_for_request(desired, request)
            except ScienceOperationalFactBridgeError:
                return self._result(
                    request,
                    status="rejected",
                    reasons=("target_footprint_mismatch",),
                )
            now = _timestamp(self._clock(), "bridge clock")
            if not request.evaluated_at <= now < request.valid_until:
                return self._result(
                    request,
                    status="rejected",
                    reasons=("fact_not_current",),
                )
            if not verified.admitted_for_staging:
                return self._result(
                    request,
                    status="withheld",
                    reasons=verified.blocker_codes,
                )
            if (
                not verified.synthetic_test_only
                or verified.source_kind != "synthetic_test"
                or not desired.producer_id.startswith(SYNTHETIC_TEST_AUTHORITY_PREFIX)
                or len(desired.targets) != 1
                or not target.target_id.startswith(SYNTHETIC_TEST_AUTHORITY_PREFIX)
                or not request.provider_id.startswith(SYNTHETIC_TEST_AUTHORITY_PREFIX)
                or not request.region_id.startswith(SYNTHETIC_TEST_AUTHORITY_PREFIX)
            ):
                return self._result(
                    request,
                    status="rejected",
                    reasons=("synthetic_test_scope_violation",),
                )
            revision = _synthetic_revision(desired, request, target, verified)
            pipeline_item_id = "science-fact:" + request.bridge_request_sha256
            try:
                existing = self._facts.load(pipeline_item_id)
            except OperationalFactStoreConflict:
                existing = None
            if existing is not None:
                if existing.revision_sha256 != revision.revision_sha256:
                    raise ScienceOperationalFactBridgeError(
                        "pipeline identity collides with another revision"
                    )
                return self._result(
                    request,
                    status="duplicate",
                    reasons=("duplicate_request",),
                    binding=_publication_binding(existing, request),
                )
            # Re-read immediately before the cross-store transition.  A live
            # coordinator still needs an external ownership/fencing authority;
            # this never-live bridge does not claim cross-database atomicity.
            active = self._desired.load_active()
            if active is None or (
                active.state_sha256 != desired.state_sha256
                or active.authority_epoch != desired.authority_epoch
            ):
                return self._result(
                    request,
                    status="rejected",
                    reasons=("desired_state_changed_before_stage",),
                )
            record = self._facts.stage(
                pipeline_item_id,
                desired_state=desired,
                revision=revision,
            )
            if record.revision_sha256 != revision.revision_sha256:
                raise ScienceOperationalFactBridgeError(
                    "fact store returned another revision identity"
                )
            return self._result(
                request,
                status="staged",
                binding=_publication_binding(record, request),
                created=True,
            )


__all__ = [
    "BRIDGE_CONTRACT",
    "BRIDGE_REQUEST_CONTRACT",
    "DurableScienceShadowFactAuthority",
    "MAX_AUTHORITY_BYTES",
    "MAX_BRIDGE_REQUEST_BYTES",
    "MAX_REFERENCE_HASHES",
    "PROVENANCE_BINDING_CONTRACT",
    "ScienceFactProvenanceBinding",
    "ScienceOperationalFactAuthorityError",
    "ScienceOperationalFactBridge",
    "ScienceOperationalFactBridgeError",
    "ScienceOperationalFactBridgeResult",
    "ScienceOperationalFactRequest",
    "ScienceTargetFootprint",
    "SyntheticScienceTestFileAuthority",
    "SyntheticScienceTestReceipt",
    "TARGET_FOOTPRINT_CONTRACT",
    "encode_synthetic_science_test_receipt",
    "science_receipt_provenance_sha256",
]
