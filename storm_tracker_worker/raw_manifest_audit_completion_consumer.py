"""Hard-isolated, durable and deliberately unwired raw-manifest audits.

The closed input set is exactly the ``raw_manifest_audit`` subset of the
terminal raw completion router catalogue.  This module performs no decoding,
fact staging, admission, snapshot publication, provider request, or refetch.
"""

from __future__ import annotations

from contextlib import closing, contextmanager
from dataclasses import asdict, dataclass, field
import hashlib
import hmac
import json
import math
import multiprocessing
import os
from pathlib import Path
import re
import secrets
import sqlite3
import stat
import threading
import time
from types import MappingProxyType
from typing import Any, Callable, Iterator, Mapping
from urllib.parse import quote

from .acquisition_activation import ACTIVATION_RULES, ActivationRule, ActivationStatus
from .acquisition_capture_recovery import (
    RECOVERY_BINDINGS,
    CaptureRecoveryError,
    RawRecoveryBinding,
    recovery_binding_sha256,
    recovery_evidence_digest,
)
from .acquisition_evidence_lifecycle import (
    COMPLETION_EVENT_SCHEMA,
    CONSUMPTION_RECEIPT_SCHEMA,
    AcquisitionEvidenceLifecycleStore,
    CompletionLease,
    ConsumptionReceipt,
    desired_authority_proof,
)
from .acquisition_execution_store import CONTRACT as EXECUTION_CONTRACT
from .acquisition_execution_store import ExecutionReceipt
from .acquisition_query_authority import safe_native_job_id
from .acquisition_scheduler_bridge import (
    JOB_TYPE,
    build_acquisition_schedule_intents,
    expected_autoscheduler_job_id,
    reconstruct_acquisition_job_for_slot,
)
from .autonomous_scheduler import SchedulerTerminalProof
from .desired_state import DesiredState, DesiredStateStore, parse_desired_state
from .hard_decoder_isolation import (
    DecoderEffectContract,
    DecoderProcessResult,
    DecoderResourceLimits,
    HardDecoderExecutor,
)
from .raw_artifacts import (
    RAW_ARTIFACT_CAPTURE_RECEIPT_SCHEMA,
    RAW_ARTIFACT_CAPTURE_SCHEMA,
    RAW_ARTIFACT_SCHEMA_VERSION,
    RawArtifactError,
    RawArtifactLimits,
    RawArtifactStore,
)
from .terminal_raw_completion_router import (
    REFERENCE_PAYLOAD_SCHEMA,
    REFERENCE_PROOF_SCHEMA,
    REFERENCE_TRANSFER_MODE,
    TERMINAL_RAW_CONSUMER_CATALOG,
    TerminalRawConsumerBinding,
)
from .worker_acquisition_plan import (
    AcquisitionJob,
    build_worker_acquisition_plan,
    validate_worker_acquisition_job,
    validate_worker_acquisition_plan,
)


AUDIT_CONSUMER_CATALOG = MappingProxyType(
    {
        workload_id: binding
        for workload_id, binding in TERMINAL_RAW_CONSUMER_CATALOG.items()
        if binding.consumer_kind == "raw_manifest_audit"
    }
)
OUTPUT_STORE_CONTRACT = "stv3-raw-manifest-audit-output-store-v2"
LEGACY_OUTPUT_STORE_CONTRACT = "stv3-raw-manifest-audit-output-store-v1"
OUTPUT_DOCUMENT_SCHEMA = "stv3-raw-manifest-audit-output-v1"
OUTPUT_RECEIPT_SCHEMA = "stv3-raw-manifest-audit-output-receipt-v1"
PROCESS_RECEIPT_SCHEMA = "stv3-raw-manifest-audit-process-receipt-v1"
PROCESS_AUTHORITY_SCHEMA = "stv3-raw-manifest-audit-process-authority-v1"
PROCESS_REQUEST_SCHEMA = "stv3-raw-manifest-audit-process-request-v2"
PROCESS_CHILD_RESULT_SCHEMA = "stv3-raw-manifest-audit-child-result-v1"
PENDING_ACK_SCHEMA = "stv3-raw-manifest-audit-pending-ack-v2"
OUTPUT_STORE_IDENTITY_SCHEMA = "stv3-raw-manifest-audit-output-store-identity-v1"
PROCESS_TARGET = (
    "storm_tracker_worker.raw_manifest_audit_completion_consumer:" "_raw_manifest_audit_child"
)
OUTPUT_MODE = "raw_manifest_audit_shadow_inert"
SCHEMA_VERSION = 2
LEGACY_SCHEMA_VERSION = 1
PROCESS_EFFECT_CONTRACT = DecoderEffectContract(
    contract_id="raw-manifest-audit.transactional-output.v1",
    hard_kill_safe=True,
    durable_effects="transactional",
    spawns_descendants=False,
)

_SHA = re.compile(r"^[0-9a-f]{64}$")
_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,191}$")
_EVENT = re.compile(r"^completion-[0-9a-f]{64}$")
_PRIVATE = re.compile(
    r"(?:[A-Za-z]:[\\/]|(?:https?|ftp)://|(?:bearer|basic)\s+|"
    r"private_[A-Za-z0-9_-]{8,}|(?:password|passwd|secret|token)=)",
    re.I,
)
_SECRET_KEY = re.compile(
    r"(?:authorization|api[_-]?key|access[_-]?token|refresh[_-]?token|"
    r"password|passwd|credential|private[_-]?key)",
    re.I,
)


class RawManifestAuditError(RuntimeError):
    pass


class RawManifestAuditIntegrityError(RawManifestAuditError):
    pass


class RawManifestAuditConflict(RawManifestAuditError):
    pass


class RawManifestAuditCapacityError(RawManifestAuditError):
    pass


class RawManifestAuditDeadlineError(RawManifestAuditError):
    pass


class RawManifestAuditCancelled(RawManifestAuditError):
    pass


def _canonical(value: Any, *, ascii_only: bool = True) -> bytes:
    try:
        text = json.dumps(
            value, allow_nan=False, ensure_ascii=ascii_only, separators=(",", ":"), sort_keys=True
        )
        return text.encode("ascii" if ascii_only else "utf-8")
    except (TypeError, ValueError, UnicodeError) as exc:
        raise RawManifestAuditIntegrityError("value is not canonical JSON") from exc


def _digest(value: Any, *, ascii_only: bool = True) -> str:
    return hashlib.sha256(_canonical(value, ascii_only=ascii_only)).hexdigest()


def _sha(value: object, label: str) -> str:
    if not isinstance(value, str) or _SHA.fullmatch(value) is None:
        raise RawManifestAuditIntegrityError(f"{label} is not SHA-256")
    return value


def _identifier(value: object, label: str) -> str:
    if not isinstance(value, str) or _ID.fullmatch(value) is None:
        raise RawManifestAuditIntegrityError(f"{label} is invalid")
    return value


def _integer(value: object, label: str, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise RawManifestAuditIntegrityError(f"{label} is invalid")
    return value


def _timestamp(value: object, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise RawManifestAuditIntegrityError(f"{label} is invalid")
    result = float(value)
    if not math.isfinite(result) or not 0 <= result <= 2**53:
        raise RawManifestAuditIntegrityError(f"{label} is invalid")
    return result


def _exact(value: object, keys: set[str], label: str) -> Mapping[str, Any]:
    if (
        not isinstance(value, Mapping)
        or set(value) != keys
        or any(not isinstance(key, str) for key in value)
    ):
        raise RawManifestAuditIntegrityError(f"{label} schema differs")
    return value


def _reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise RawManifestAuditIntegrityError("JSON has duplicate keys")
        result[key] = value
    return result


def _decode(payload: bytes) -> dict[str, Any]:
    if not isinstance(payload, bytes) or not payload or len(payload) > 64 * 1024:
        raise RawManifestAuditCapacityError("reference payload exceeds cap")
    try:
        value = json.loads(payload.decode("ascii"), object_pairs_hook=_reject_duplicates)
    except RawManifestAuditError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RawManifestAuditIntegrityError("reference JSON is invalid") from exc
    if not isinstance(value, dict) or _canonical(value) != payload:
        raise RawManifestAuditIntegrityError("reference JSON is not canonical")
    return value


def _public(value: Any, depth: int = 0) -> None:
    if depth > 20:
        raise RawManifestAuditCapacityError("public document nesting exceeds cap")
    if isinstance(value, Mapping):
        if len(value) > 256:
            raise RawManifestAuditCapacityError("public object exceeds cap")
        for key, item in value.items():
            if not isinstance(key, str) or _SECRET_KEY.search(key):
                raise RawManifestAuditIntegrityError("credential-like output key")
            _public(item, depth + 1)
    elif isinstance(value, (list, tuple)):
        if len(value) > 64:
            raise RawManifestAuditCapacityError("public sequence exceeds cap")
        for item in value:
            _public(item, depth + 1)
    elif isinstance(value, str):
        if len(value.encode()) > 4096 or _PRIVATE.search(value):
            raise RawManifestAuditIntegrityError("private capability in output")
    elif value is None or isinstance(value, (bool, int, float)):
        if isinstance(value, float) and not math.isfinite(value):
            raise RawManifestAuditIntegrityError("non-finite output number")
    else:
        raise RawManifestAuditIntegrityError("unsupported output value")


def _effect() -> dict[str, Any]:
    return {
        "contract_id": PROCESS_EFFECT_CONTRACT.contract_id,
        "durable_effects": PROCESS_EFFECT_CONTRACT.durable_effects,
        "hard_kill_safe": PROCESS_EFFECT_CONTRACT.hard_kill_safe,
        "spawns_descendants": PROCESS_EFFECT_CONTRACT.spawns_descendants,
    }


PROCESS_EFFECT_CONTRACT_SHA256 = _digest(_effect())


def validate_audit_consumer_catalog() -> None:
    expected = {
        workload_id
        for workload_id, binding in TERMINAL_RAW_CONSUMER_CATALOG.items()
        if binding.consumer_kind == "raw_manifest_audit"
    }
    if not expected or set(AUDIT_CONSUMER_CATALOG) != expected:
        raise RawManifestAuditIntegrityError("audit consumer catalogue is not closed")
    identities: set[tuple[str, str]] = set()
    for workload_id, binding in AUDIT_CONSUMER_CATALOG.items():
        if (
            not isinstance(binding, TerminalRawConsumerBinding)
            or binding.workload_id != workload_id
            or binding.stream_id != workload_id
            or binding.consumer_kind != "raw_manifest_audit"
            or binding.implementation_id != "terminal_raw_reference_audit_v1"
            or binding.transfer_mode != REFERENCE_TRANSFER_MODE
            or binding.as_dict()["meteorological_admission"] is not False
            or binding.as_dict()["runtime_wiring"] != "inert"
            or (binding.consumer_id, binding.stream_id) in identities
        ):
            raise RawManifestAuditIntegrityError("audit consumer binding drifted")
        identities.add((binding.consumer_id, binding.stream_id))


validate_audit_consumer_catalog()


@dataclass(frozen=True, slots=True)
class RawManifestAuditProcessLimits:
    wall_seconds: float = 45.0
    cpu_seconds: float = 30.0
    rss_bytes: int = 512 * 1024 * 1024
    max_request_bytes: int = 512 * 1024
    max_result_bytes: int = 16 * 1024

    def __post_init__(self) -> None:
        DecoderResourceLimits(
            self.wall_seconds,
            self.cpu_seconds,
            self.rss_bytes,
            self.max_request_bytes,
            self.max_result_bytes,
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "cpu_seconds": float(self.cpu_seconds),
            "max_request_bytes": self.max_request_bytes,
            "max_result_bytes": self.max_result_bytes,
            "rss_bytes": self.rss_bytes,
            "wall_seconds": float(self.wall_seconds),
        }

    @property
    def fingerprint(self) -> str:
        return _digest(
            {
                **self.as_dict(),
                "effect_contract_sha256": PROCESS_EFFECT_CONTRACT_SHA256,
                "schema": "stv3-raw-manifest-audit-process-limits-v1",
            }
        )

    def decoder_limits(self) -> DecoderResourceLimits:
        return DecoderResourceLimits(
            self.wall_seconds,
            self.cpu_seconds,
            self.rss_bytes,
            self.max_request_bytes,
            self.max_result_bytes,
        )


@dataclass(frozen=True, slots=True)
class RawManifestAuditOutputLimits:
    max_receipts: int = 8192
    max_receipt_bytes: int = 64 * 1024
    max_total_output_bytes: int = 128 * 1024 * 1024
    max_database_bytes: int = 256 * 1024 * 1024

    def __post_init__(self) -> None:
        for value, low, high in (
            (self.max_receipts, 1, 1_000_000),
            (self.max_receipt_bytes, 1024, 1024 * 1024),
            (self.max_total_output_bytes, self.max_receipt_bytes, 4 * 1024**3),
            (self.max_database_bytes, 1024 * 1024, 4 * 1024**3),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or not low <= value <= high:
                raise ValueError("audit output limit is invalid")

    @property
    def fingerprint(self) -> str:
        return _digest({**asdict(self), "schema": "stv3-raw-manifest-audit-output-limits-v1"})


@dataclass(frozen=True, slots=True)
class AuditRole:
    role: str
    receipt_id: str
    content_sha256: str
    receipt_sha256: str
    size_bytes: int
    content_type: str
    provider: str
    product: str

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class RawManifestAuditBinding:
    workload_id: str
    consumer_id: str
    stream_id: str
    acquisition_job_id: str
    provider_id: str
    product_id: str
    capture_id: str
    capture_manifest_sha256: str
    capture_receipt_sha256: str
    role_set_content_sha256: str
    role_set_receipt_sha256: str
    total_bytes: int
    roles: tuple[AuditRole, ...]

    def as_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["roles"] = [role.as_dict() for role in self.roles]
        return result


@dataclass(frozen=True, slots=True)
class RawManifestAuditOutputReceipt:
    receipt_sha256: str
    process_receipt_sha256: str
    output_sha256: str
    event_id: str
    event_sha256: str
    producer_receipt_sha256: str
    workload_id: str
    consumer_id: str
    stream_id: str
    desired_state_sha256: str
    authority_sha256: str
    capture_id: str
    capture_manifest_sha256: str
    isolation_authority_sha256: str
    isolation_limits_sha256: str
    created_at: float
    output: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class RawManifestAuditRunResult:
    status: str
    reason_code: str
    event_id: str | None = None
    output_sha256: str | None = None
    output_receipt_sha256: str | None = None
    process_receipt_sha256: str | None = None
    consumption_receipt_sha256: str | None = None
    phase_child_invocations: int = 0


@dataclass(frozen=True, slots=True)
class RawManifestAuditPendingAck:
    """Private in-memory continuation between initial audit and final reproof."""

    lease: CompletionLease = field(repr=False)
    desired_state: DesiredState = field(repr=False)
    binding: RawManifestAuditBinding = field(repr=False)
    output_receipt: RawManifestAuditOutputReceipt
    recovered_output: bool
    initial_child_invocations: int
    output_store_identity_sha256: str
    pending_sha256: str

    @classmethod
    def create(
        cls,
        *,
        lease: CompletionLease,
        desired_state: DesiredState,
        binding: RawManifestAuditBinding,
        output_receipt: RawManifestAuditOutputReceipt,
        recovered_output: bool,
        initial_child_invocations: int,
        output_store_identity_sha256: str,
    ) -> "RawManifestAuditPendingAck":
        provisional = cls(
            lease,
            desired_state,
            binding,
            output_receipt,
            recovered_output,
            initial_child_invocations,
            _sha(output_store_identity_sha256, "output store identity SHA"),
            "0" * 64,
        )
        result = cls(
            lease,
            desired_state,
            binding,
            output_receipt,
            recovered_output,
            initial_child_invocations,
            provisional.output_store_identity_sha256,
            _digest(provisional.identity_mapping()),
        )
        result.validate()
        return result

    def identity_mapping(self) -> dict[str, Any]:
        return {
            "binding_sha256": _digest(self.binding.as_dict()),
            "desired_state_sha256": self.desired_state.state_sha256,
            "event_sha256": self.lease.event_sha256,
            "initial_child_invocations": self.initial_child_invocations,
            "isolation_authority_sha256": self.output_receipt.isolation_authority_sha256,
            "isolation_limits_sha256": self.output_receipt.isolation_limits_sha256,
            "lease_expires_at": self.lease.lease_expires_at,
            "output_receipt_sha256": self.output_receipt.receipt_sha256,
            "output_sha256": self.output_receipt.output_sha256,
            "output_store_identity_sha256": self.output_store_identity_sha256,
            "process_receipt_sha256": self.output_receipt.process_receipt_sha256,
            "producer_receipt_sha256": self.lease.producer_receipt_sha256,
            "recovered_output": self.recovered_output,
            "schema": PENDING_ACK_SCHEMA,
            "stream_sequence": self.lease.stream_sequence,
            "workload_id": self.binding.workload_id,
        }

    def validate(self) -> None:
        if (
            not isinstance(self.lease, CompletionLease)
            or not isinstance(self.desired_state, DesiredState)
            or not isinstance(self.binding, RawManifestAuditBinding)
            or not isinstance(self.output_receipt, RawManifestAuditOutputReceipt)
            or not isinstance(self.recovered_output, bool)
            or self.initial_child_invocations not in {0, 1}
            or self.initial_child_invocations != int(not self.recovered_output)
            or _SHA.fullmatch(self.output_store_identity_sha256) is None
            or _SHA.fullmatch(self.pending_sha256) is None
            or _digest(self.identity_mapping()) != self.pending_sha256
            or self.lease.event_id != self.output_receipt.event_id
            or self.lease.event_sha256 != self.output_receipt.event_sha256
            or self.lease.consumer_id != self.binding.consumer_id
            or self.lease.stream_id != self.binding.stream_id
            or self.lease.desired_state_sha256 != self.desired_state.state_sha256
        ):
            raise RawManifestAuditIntegrityError(
                "pending raw audit acknowledgement identity differs"
            )


_PAYLOAD_KEYS = {
    "acquisition_job_id",
    "capture_reference",
    "claims",
    "consumer_binding_sha256",
    "consumer_id",
    "schema",
    "stream_id",
    "workload_id",
}
_CLAIM_KEYS = {
    "decoded",
    "fact_staged",
    "meteorological_admission",
    "operational_snapshot_published",
    "raw_bytes_copied",
    "raw_refetch_performed",
}
_CAPTURE_KEYS = {
    "capture_id",
    "capture_receipt_schema",
    "capture_receipt_sha256",
    "capture_schema",
    "manifest_sha256",
    "members",
    "product",
    "provider",
    "raw_store_schema_version",
    "role_set_content_sha256",
    "role_set_receipt_sha256",
    "total_bytes",
}
_MEMBER_KEYS = {
    "content_sha256",
    "content_type",
    "product",
    "provider",
    "receipt_id",
    "receipt_sha256",
    "role",
    "size_bytes",
}
_PROOF_KEYS = {
    "capacity_commit",
    "capture_reference",
    "claims",
    "consumer_binding",
    "consumer_binding_sha256",
    "desired_authority",
    "execution_receipt",
    "recovery_binding_sha256",
    "reference_content_sha256",
    "scheduler_terminal",
    "schema",
    "workload",
}


def _audit_roles_from_reference(
    recovery: RawRecoveryBinding,
    member_values: object,
) -> tuple[AuditRole, ...]:
    """Bind one static or dynamic manifest reference without reading bytes."""

    if (
        not isinstance(member_values, list)
        or not recovery.minimum_artifacts
        <= len(member_values)
        <= recovery.maximum_artifacts
    ):
        raise RawManifestAuditIntegrityError("capture role count differs")
    references = [
        _exact(value, _MEMBER_KEYS, "capture member")
        for value in member_values
    ]
    role_names = tuple(
        _identifier(reference["role"], "capture role")
        for reference in references
    )
    if role_names != tuple(sorted(role_names)):
        raise RawManifestAuditIntegrityError("capture roles are not canonical")
    try:
        recovery.members_for_roles(role_names)
    except Exception as exc:
        raise RawManifestAuditIntegrityError("capture role set differs") from exc
    lightning_kinds = {
        "eumetsat_mtg_li_search_v1",
        "eumetsat_mtg_li_netcdf_v1",
        "noaa_goes_glm_listing_v1",
        "noaa_goes_glm_netcdf_v1",
    }
    roles: list[AuditRole] = []
    for reference in references:
        member = recovery.member(reference["role"])
        minimum_size = 0 if member.provenance_kind in lightning_kinds else 1
        if (
            reference["role"] != member.role
            or reference["provider"] != recovery.raw_provider
            or reference["product"] != member.raw_product
            or reference["content_type"] not in member.allowed_content_types
            or _integer(
                reference["size_bytes"], "member bytes", minimum_size
            ) > member.maximum_transport_bytes
        ):
            raise RawManifestAuditIntegrityError("capture role binding differs")
        roles.append(AuditRole(
            member.role,
            _identifier(reference["receipt_id"], "receipt ID"),
            _sha(reference["content_sha256"], "content SHA"),
            _sha(reference["receipt_sha256"], "receipt SHA"),
            reference["size_bytes"],
            reference["content_type"],
            reference["provider"],
            reference["product"],
        ))
    return tuple(roles)


def _scheduler(value: object) -> SchedulerTerminalProof:
    mapping = _exact(
        value,
        set(SchedulerTerminalProof.__dataclass_fields__) | {"proof_sha256"},
        "scheduler terminal",
    )
    body = {key: mapping[key] for key in mapping if key != "proof_sha256"}
    try:
        proof = SchedulerTerminalProof(**body)
    except (TypeError, ValueError) as exc:
        raise RawManifestAuditIntegrityError("scheduler terminal is invalid") from exc
    if (
        proof.as_dict() != body
        or proof.proof_sha256 != _sha(mapping["proof_sha256"], "scheduler proof")
        or proof.job_type != JOB_TYPE
        or proof.state != "completed"
        or proof.last_transition != "completed"
        or proof.last_error_code is not None
        or proof.attempt_count < 1
    ):
        raise RawManifestAuditIntegrityError("scheduler terminal differs")
    return proof


def _execution(value: object) -> ExecutionReceipt:
    mapping = _exact(
        value, set(ExecutionReceipt.__dataclass_fields__) | {"contract"}, "execution receipt"
    )
    if mapping["contract"] != EXECUTION_CONTRACT:
        raise RawManifestAuditIntegrityError("execution contract differs")
    try:
        receipt = ExecutionReceipt(
            **{key: mapping[key] for key in ExecutionReceipt.__dataclass_fields__}
        )
    except (TypeError, ValueError) as exc:
        raise RawManifestAuditIntegrityError("execution receipt is invalid") from exc
    unhashed = receipt.as_dict()
    unhashed.pop("receipt_sha256")
    if (
        receipt.as_dict() != dict(mapping)
        or _digest(unhashed) != receipt.receipt_sha256
        or not receipt.terminal
        or receipt.outcome != "completed_shadow_raw"
        or receipt.reason_code != "shadow_raw_capture_complete"
    ):
        raise RawManifestAuditIntegrityError("execution receipt checksum differs")
    return receipt


def _raw_job_id(binding: RawRecoveryBinding, job_id: str) -> str:
    if binding.job_identity_kind == "origin_v1":
        return job_id
    if binding.job_identity_kind == "safe_native_alias_v1":
        try:
            return safe_native_job_id(job_id)
        except ValueError as exc:
            raise RawManifestAuditIntegrityError("raw job alias cannot be derived") from exc
    raise RawManifestAuditIntegrityError("raw job identity kind is unsupported")


def _receipt_fetch_matches(fetch_id: str, raw_job_id: str, member: Any) -> bool:
    prefix = f"{raw_job_id}{member.receipt_suffix}"
    if member.receipt_attempt_limit is None:
        return fetch_id == prefix
    attempt = fetch_id[len(prefix) :] if fetch_id.startswith(prefix) else ""
    return (
        attempt.isascii()
        and attempt.isdigit()
        and not attempt.startswith("0")
        and 1 <= int(attempt) <= member.receipt_attempt_limit
    )


def validate_raw_manifest_audit_lease(
    lease: CompletionLease,
    *,
    desired_state: DesiredState,
    raw_store: RawArtifactStore | None = None,
    verify_raw_bytes: bool = False,
) -> RawManifestAuditBinding:
    """Validate the router reference; raw SHA passes occur only in the child."""
    if not isinstance(lease, CompletionLease) or not isinstance(desired_state, DesiredState):
        raise TypeError("lease/desired state type differs")
    if verify_raw_bytes != (raw_store is not None):
        raise TypeError("raw store is required exactly for byte verification")
    payload = _exact(_decode(lease.payload), _PAYLOAD_KEYS, "reference payload")
    workload_id = _identifier(payload["workload_id"], "workload ID")
    consumer = AUDIT_CONSUMER_CATALOG.get(workload_id)
    if consumer is None:
        raise RawManifestAuditIntegrityError("completion is not a raw-manifest audit route")
    if (
        lease.consumer_id != consumer.consumer_id
        or lease.stream_id != consumer.stream_id
        or payload["consumer_id"] != consumer.consumer_id
        or payload["stream_id"] != consumer.stream_id
        or payload["acquisition_job_id"] != lease.acquisition_job_id
        or payload["schema"] != REFERENCE_PAYLOAD_SCHEMA
        or lease.size_bytes != len(lease.payload)
        or hashlib.sha256(lease.payload).hexdigest() != lease.content_sha256
        or lease.stream_sequence < 1
        or lease.attempt_count < 1
    ):
        raise RawManifestAuditIntegrityError("completion route differs")
    event_sha = _digest(
        {
            "consumer_id": lease.consumer_id,
            "producer_receipt_sha256": lease.producer_receipt_sha256,
            "schema": COMPLETION_EVENT_SCHEMA,
            "stream_id": lease.stream_id,
            "stream_sequence": lease.stream_sequence,
        },
        ascii_only=False,
    )
    event_id = (
        "completion-"
        + hashlib.sha256(
            (COMPLETION_EVENT_SCHEMA + lease.producer_receipt_sha256).encode("ascii")
        ).hexdigest()
    )
    if (
        _EVENT.fullmatch(lease.event_id) is None
        or event_id != lease.event_id
        or event_sha != lease.event_sha256
    ):
        raise RawManifestAuditIntegrityError("completion event identity differs")
    if desired_state.state_sha256 != lease.desired_state_sha256:
        raise RawManifestAuditConflict("active desired state differs")
    authority = desired_authority_proof(desired_state)
    if (
        authority["authority_sha256"] != lease.authority_sha256
        or _digest(
            {k: v for k, v in authority.items() if k != "authority_sha256"}, ascii_only=False
        )
        != lease.authority_sha256
    ):
        raise RawManifestAuditIntegrityError("desired authority differs")
    proof = _exact(lease.proof, _PROOF_KEYS, "reference proof")
    claims = _exact(payload["claims"], _CLAIM_KEYS, "reference claims")
    capture_ref = _exact(payload["capture_reference"], _CAPTURE_KEYS, "capture reference")
    if (
        proof["schema"] != REFERENCE_PROOF_SCHEMA
        or proof["reference_content_sha256"] != lease.content_sha256
        or proof["consumer_binding"] != consumer.as_dict()
        or proof["consumer_binding_sha256"] != consumer.fingerprint
        or payload["consumer_binding_sha256"] != consumer.fingerprint
        or proof["desired_authority"] != authority
        or proof["claims"] != claims
        or proof["capture_reference"] != capture_ref
        or any(value is not False for value in claims.values())
    ):
        raise RawManifestAuditIntegrityError("reference authority or claims differ")
    terminal = _scheduler(proof["scheduler_terminal"])
    plan = validate_worker_acquisition_plan(build_worker_acquisition_plan(desired_state))
    try:
        job = validate_worker_acquisition_job(
            reconstruct_acquisition_job_for_slot(
                plan, intent_id=terminal.intent_id, scheduled_for=terminal.scheduled_for
            )
        )
    except Exception as exc:
        raise RawManifestAuditIntegrityError("acquisition job cannot be reconstructed") from exc
    schedules = tuple(
        item
        for item in build_acquisition_schedule_intents(plan)
        if item.intent_id == terminal.intent_id
    )
    if len(schedules) != 1:
        raise RawManifestAuditIntegrityError("scheduler intent differs")
    schedule = schedules[0]
    expected_sched = expected_autoscheduler_job_id(
        snapshot_sha256=desired_state.state_sha256,
        intent_id=schedule.intent_id,
        cadence_seconds=schedule.cadence_seconds,
        slot_index=terminal.slot_index,
    )
    expected_slot = "slot:" + expected_sched.removeprefix("autosched:")
    payload_sha = hashlib.sha256(_canonical(dict(schedule.payload), ascii_only=False)).hexdigest()
    record = {
        "cadence_seconds": schedule.cadence_seconds,
        "deadline_at": terminal.deadline_at,
        "desired_state_sha256": desired_state.state_sha256,
        "intent_id": schedule.intent_id,
        "job_id": expected_sched,
        "job_type": schedule.job_type,
        "payload": dict(schedule.payload),
        "payload_sha256": payload_sha,
        "priority": schedule.priority,
        "scheduled_for": terminal.scheduled_for,
        "slot_id": expected_slot,
        "slot_index": terminal.slot_index,
    }
    workload = _exact(
        proof["workload"],
        {
            "job_id",
            "job_sha256",
            "plan_sha256",
            "product_id",
            "provider_id",
            "snapshot_sha256",
            "workload_id",
        },
        "workload",
    )
    if (
        terminal.job_id != expected_sched
        or terminal.slot_id != expected_slot
        or terminal.payload_sha256 != payload_sha
        or terminal.record_sha256
        != hashlib.sha256(_canonical(record, ascii_only=False)).hexdigest()
        or terminal.slot_index != job.slot_index
        or terminal.deadline_at != job.deadline_at
        or workload
        != {
            "job_id": job.job_id,
            "job_sha256": job.job_sha256,
            "plan_sha256": job.plan_sha256,
            "product_id": job.product_id,
            "provider_id": job.provider_id,
            "snapshot_sha256": job.snapshot_sha256,
            "workload_id": job.workload_id,
        }
        or job.job_id != lease.acquisition_job_id
        or job.workload_id != workload_id
    ):
        raise RawManifestAuditIntegrityError("scheduler/job authority differs")
    rule = ACTIVATION_RULES.get(workload_id)
    if (
        not isinstance(rule, ActivationRule)
        or rule.status is not ActivationStatus.ACTIVE_SHADOW_RAW
        or not rule.executable
        or rule.provider_id != job.provider_id
        or rule.product_id != job.product_id
    ):
        raise RawManifestAuditIntegrityError("activation rule differs")
    recovery = RECOVERY_BINDINGS.get(str(rule.handler_id))
    if (
        not isinstance(recovery, RawRecoveryBinding)
        or recovery.workload_id != workload_id
        or recovery.required_roles != rule.required_roles
        or recovery.dynamic_role_pattern != rule.dynamic_role_pattern
        or recovery.dynamic_role_minimum != rule.dynamic_role_minimum
        or recovery.dynamic_role_maximum != rule.dynamic_role_maximum
        or recovery.ledger_provider != job.provider_id
        or recovery.maximum_transport_bytes != rule.maximum_transport_bytes
        or recovery.maximum_artifacts != rule.maximum_artifacts
        or proof["recovery_binding_sha256"] != recovery_binding_sha256(recovery)
    ):
        raise RawManifestAuditIntegrityError("recovery binding differs")
    execution = _execution(proof["execution_receipt"])
    if (
        execution.scheduler_job_id != terminal.job_id
        or execution.acquisition_job_id != job.job_id
        or execution.attempt_count != terminal.attempt_count
    ):
        raise RawManifestAuditIntegrityError("execution scheduler binding differs")
    if (
        capture_ref["capture_schema"] != RAW_ARTIFACT_CAPTURE_SCHEMA
        or capture_ref["capture_receipt_schema"] != RAW_ARTIFACT_CAPTURE_RECEIPT_SCHEMA
        or capture_ref["raw_store_schema_version"] != RAW_ARTIFACT_SCHEMA_VERSION
        or capture_ref["provider"] != recovery.raw_provider
        or capture_ref["product"] != recovery.capture_product
    ):
        raise RawManifestAuditIntegrityError("capture contract differs")
    raw_job = _raw_job_id(recovery, job.job_id)
    expected_capture = RawArtifactStore._capture_id(  # noqa: SLF001
        provider=recovery.raw_provider,
        product=recovery.capture_product,
        fetch_id=f"{raw_job}{recovery.capture_suffix}",
    )
    if capture_ref["capture_id"] != expected_capture:
        raise RawManifestAuditIntegrityError("capture identity differs")
    member_values = capture_ref["members"]
    roles = list(_audit_roles_from_reference(recovery, member_values))
    total = sum(role.size_bytes for role in roles)
    if capture_ref["total_bytes"] != total or total > recovery.maximum_transport_bytes:
        raise RawManifestAuditIntegrityError("capture byte accounting differs")
    capacity = _exact(
        proof["capacity_commit"],
        {
            "actual_bytes",
            "actual_entries",
            "commit_sha256",
            "provider",
            "request_sha256",
            "reservation_sha256",
            "status",
        },
        "capacity commit",
    )
    for key in ("commit_sha256", "request_sha256", "reservation_sha256"):
        _sha(capacity[key], key)
    if (
        capacity["status"] != "COMMITTED"
        or capacity["provider"] != recovery.ledger_provider
        or capacity["actual_bytes"] != total
        or capacity["actual_entries"] != len(roles)
        or execution.content_sha256 != capture_ref["role_set_content_sha256"]
        or execution.raw_receipt_sha256 != capture_ref["role_set_receipt_sha256"]
        or execution.capture_sha256 != capture_ref["capture_receipt_sha256"]
        or execution.capacity_commit_sha256 != capacity["commit_sha256"]
    ):
        raise RawManifestAuditIntegrityError("capacity/execution/reference differs")
    binding = RawManifestAuditBinding(
        workload_id,
        consumer.consumer_id,
        consumer.stream_id,
        job.job_id,
        job.provider_id,
        job.product_id,
        expected_capture,
        _sha(capture_ref["manifest_sha256"], "manifest SHA"),
        _sha(capture_ref["capture_receipt_sha256"], "capture receipt SHA"),
        _sha(capture_ref["role_set_content_sha256"], "role content SHA"),
        _sha(capture_ref["role_set_receipt_sha256"], "role receipt SHA"),
        total,
        tuple(roles),
    )
    if verify_raw_bytes:
        assert raw_store is not None
        try:
            loaded = raw_store.load_capture_bundle_bytes(expected_capture)
        except RawArtifactError as exc:
            raise RawManifestAuditIntegrityError("raw bytes failed verification") from exc
        if loaded is None:
            raise RawManifestAuditIntegrityError("raw capture is unavailable")
        capture, receipts, payloads = loaded
        if set(receipts) != {role.role for role in roles} or set(payloads) != set(receipts):
            raise RawManifestAuditIntegrityError("raw role set differs")
        for role in roles:
            receipt = receipts[role.role]
            member = recovery.member(role.role)
            if (
                receipt.receipt_id != role.receipt_id
                or receipt.content_sha256 != role.content_sha256
                or receipt.receipt_sha256 != role.receipt_sha256
                or receipt.size_bytes != role.size_bytes
                or receipt.content_type != role.content_type
                or receipt.provider != role.provider
                or receipt.product != role.product
                or not _receipt_fetch_matches(receipt.fetch_id, raw_job, member)
                or hashlib.sha256(payloads[role.role]).hexdigest() != role.content_sha256
                or len(payloads[role.role]) != role.size_bytes
            ):
                raise RawManifestAuditIntegrityError("raw role bytes differ")
        try:
            evidence = recovery_evidence_digest(
                binding=recovery, receipts_by_role=receipts, capture=capture
            )
        except CaptureRecoveryError as exc:
            raise RawManifestAuditIntegrityError("raw recovery evidence differs") from exc
        if (
            capture.capture_id != binding.capture_id
            or capture.manifest_sha256 != binding.capture_manifest_sha256
            or capture.capture_receipt_sha256 != binding.capture_receipt_sha256
            or evidence.content_sha256 != binding.role_set_content_sha256
            or evidence.receipt_sha256 != binding.role_set_receipt_sha256
            or evidence.actual_bytes != binding.total_bytes
        ):
            raise RawManifestAuditIntegrityError("raw manifest authority differs")
    return binding


def _output_document(
    value: object,
) -> tuple[
    dict[str, Any], Mapping[str, Any], Mapping[str, Any], Mapping[str, Any], Mapping[str, Any]
]:
    doc = dict(
        _exact(
            value,
            {
                "audit",
                "authority",
                "claims",
                "consumer_id",
                "disposition",
                "event",
                "fact_revision",
                "isolation",
                "mode",
                "schema",
                "stream_id",
                "workload_id",
            },
            "audit output",
        )
    )
    workload = doc["workload_id"]
    catalog = AUDIT_CONSUMER_CATALOG.get(workload) if isinstance(workload, str) else None
    if (
        catalog is None
        or doc["schema"] != OUTPUT_DOCUMENT_SCHEMA
        or doc["mode"] != OUTPUT_MODE
        or doc["consumer_id"] != catalog.consumer_id
        or doc["stream_id"] != catalog.stream_id
        or doc["disposition"] != "raw_manifest_verified_shadow"
        or doc["fact_revision"] is not None
    ):
        raise RawManifestAuditIntegrityError("audit output route differs")
    event = _exact(
        doc["event"],
        {"event_id", "event_sha256", "producer_receipt_sha256", "stream_sequence"},
        "output event",
    )
    authority = _exact(
        doc["authority"], {"authority_sha256", "desired_state_sha256"}, "output authority"
    )
    audit = _exact(
        doc["audit"],
        {
            "acquisition_job_id",
            "capture_id",
            "capture_manifest_sha256",
            "capture_receipt_sha256",
            "product_id",
            "provider_id",
            "raw_bytes_verified",
            "role_set_content_sha256",
            "role_set_receipt_sha256",
            "roles",
            "total_bytes",
        },
        "audit evidence",
    )
    isolation = _exact(
        doc["isolation"],
        {
            "authority_sha256",
            "effect_contract_sha256",
            "limits_sha256",
            "process_boundary",
            "resource_reason",
            "runtime_wiring",
        },
        "output isolation",
    )
    claims = _exact(doc["claims"], _CLAIM_KEYS, "output claims")
    if (
        any(value is not False for value in claims.values())
        or audit["raw_bytes_verified"] is not True
        or isolation["effect_contract_sha256"] != PROCESS_EFFECT_CONTRACT_SHA256
        or isolation["process_boundary"] != "fresh_spawn_hard_cpu_rss_wall_output_caps"
        or isolation["resource_reason"] != "full_sha_pass_over_bounded_multi_role_raw_bytes"
        or isolation["runtime_wiring"] != "inert"
    ):
        raise RawManifestAuditIntegrityError("audit output claims differ")
    if _EVENT.fullmatch(_identifier(event["event_id"], "event ID")) is None:
        raise RawManifestAuditIntegrityError("event ID differs")
    for field in ("event_sha256", "producer_receipt_sha256"):
        _sha(event[field], field)
    _integer(event["stream_sequence"], "stream sequence", 1)
    for field in ("authority_sha256", "desired_state_sha256"):
        _sha(authority[field], field)
    for field in (
        "capture_manifest_sha256",
        "capture_receipt_sha256",
        "role_set_content_sha256",
        "role_set_receipt_sha256",
    ):
        _sha(audit[field], field)
    _identifier(audit["acquisition_job_id"], "acquisition job")
    _identifier(audit["capture_id"], "capture ID")
    _identifier(audit["provider_id"], "provider ID")
    _identifier(audit["product_id"], "product ID")
    _integer(audit["total_bytes"], "total bytes", 1)
    roles = audit["roles"]
    if not isinstance(roles, list) or not 1 <= len(roles) <= 64:
        raise RawManifestAuditIntegrityError("audit roles differ")
    for role in roles:
        mapping = _exact(role, set(AuditRole.__dataclass_fields__), "audit role")
        _identifier(mapping["role"], "role")
        _identifier(mapping["receipt_id"], "receipt ID")
        _sha(mapping["content_sha256"], "content SHA")
        _sha(mapping["receipt_sha256"], "receipt SHA")
        _integer(mapping["size_bytes"], "role bytes", 1)
    _sha(isolation["authority_sha256"], "isolation authority")
    _sha(isolation["limits_sha256"], "isolation limits")
    _public(doc)
    return doc, event, authority, audit, isolation


class RawManifestAuditOutputStore:
    """Bounded append-only output and process receipts with instance identity."""

    def __init__(
        self,
        path: str | Path,
        *,
        limits: RawManifestAuditOutputLimits = RawManifestAuditOutputLimits(),
        clock: Callable[[], float] = time.time,
        fault_injector: Callable[[str], None] | None = None,
    ):
        self.path = Path(os.path.abspath(os.fspath(path)))
        self.limits = limits
        self._clock = clock
        self._fault_injector = fault_injector
        self._lock = threading.RLock()
        self._database_witness: tuple[int, int, int]
        self._store_instance_sha256: str
        if not self.path.is_absolute() or not isinstance(limits, RawManifestAuditOutputLimits):
            raise ValueError("audit output store configuration differs")
        self.initialize()

    def _fault(self, point: str) -> None:
        if self._fault_injector is not None:
            self._fault_injector(point)

    @staticmethod
    def _normalized_path(path: Path) -> str:
        return os.path.normcase(os.path.normpath(str(path)))

    @staticmethod
    def _path_witness(path: Path) -> tuple[int, int, int]:
        try:
            details = path.lstat()
        except OSError as exc:
            raise RawManifestAuditIntegrityError(
                "audit output store witness is unavailable"
            ) from exc
        if (
            path.is_symlink()
            or not stat.S_ISREG(details.st_mode)
            or int(details.st_nlink) != 1
        ):
            raise RawManifestAuditIntegrityError("audit output store path is unsafe")
        return int(details.st_dev), int(details.st_ino), int(details.st_mode)

    def _prepare_path(self) -> tuple[int, int, int]:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        parent = self.path.parent
        for candidate in (parent, *parent.parents):
            try:
                details = candidate.lstat()
            except OSError as exc:
                raise RawManifestAuditIntegrityError(
                    "audit output store parent is unavailable"
                ) from exc
            if candidate.is_symlink() or not stat.S_ISDIR(details.st_mode):
                raise RawManifestAuditIntegrityError(
                    "audit output store parent is unsafe"
                )
        if self._normalized_path(parent.resolve(strict=True)) != self._normalized_path(parent):
            raise RawManifestAuditIntegrityError("audit output store parent is aliased")
        if self.path.is_symlink():
            raise RawManifestAuditIntegrityError("audit output store path is unsafe")
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
                    if not stat.S_ISREG(details.st_mode):
                        raise RawManifestAuditIntegrityError(
                            "audit output store leaf is not regular"
                        )
                    os.fsync(descriptor)
                finally:
                    os.close(descriptor)
            except RawManifestAuditError:
                raise
            except OSError as exc:
                raise RawManifestAuditIntegrityError(
                    "audit output store leaf creation failed"
                ) from exc
        witness = self._path_witness(self.path)
        if self._normalized_path(self.path.resolve(strict=True)) != self._normalized_path(
            self.path
        ):
            raise RawManifestAuditIntegrityError("audit output store path is aliased")
        return witness

    def _verify_path(self) -> None:
        try:
            resolved = self.path.resolve(strict=True)
        except OSError as exc:
            raise RawManifestAuditIntegrityError(
                "audit output store instance changed"
            ) from exc
        if (
            self._normalized_path(resolved) != self._normalized_path(self.path)
            or self._path_witness(self.path) != self._database_witness
        ):
            raise RawManifestAuditIntegrityError("audit output store instance changed")

    def _connect(self) -> sqlite3.Connection:
        self._verify_path()
        uri_path = quote(self.path.as_posix(), safe="/:")
        db = sqlite3.connect(
            f"file:{uri_path}?mode=rw&nofollow=1",
            timeout=5.0,
            isolation_level=None,
            uri=True,
        )
        db.row_factory = sqlite3.Row
        try:
            self._verify_path()
            db.execute("PRAGMA synchronous=FULL")
            db.execute("PRAGMA foreign_keys=ON")
            db.execute("PRAGMA trusted_schema=OFF")
            db.execute("PRAGMA busy_timeout=5000")
            if str(db.execute("PRAGMA journal_mode=DELETE").fetchone()[0]).lower() != "delete":
                raise RawManifestAuditIntegrityError("audit output journal mode differs")
            page = int(db.execute("PRAGMA page_size").fetchone()[0])
            maximum_pages = max(1, self.limits.max_database_bytes // page)
            page_count = int(db.execute("PRAGMA page_count").fetchone()[0])
            configured_pages = int(
                db.execute(f"PRAGMA max_page_count={maximum_pages}").fetchone()[0]
            )
            if page_count > maximum_pages or configured_pages != maximum_pages:
                raise RawManifestAuditCapacityError("audit output database exceeds cap")
        except BaseException:
            db.close()
            raise
        return db

    @staticmethod
    def _tables(db: sqlite3.Connection) -> set[str]:
        return {
            str(row[0])
            for row in db.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            )
        }

    @staticmethod
    def _columns(db: sqlite3.Connection, table: str) -> tuple[tuple[str, str, int, int], ...]:
        return tuple(
            (str(row[1]), str(row[2]), int(row[3]), int(row[5]))
            for row in db.execute(f"PRAGMA table_info({table})")
        )

    @staticmethod
    def _normalized_sql(value: str) -> str:
        return " ".join(value.casefold().split())

    @classmethod
    def _trigger_definitions(cls) -> dict[str, str]:
        return {
            "audit_outputs_no_update": (
                "CREATE TRIGGER audit_outputs_no_update BEFORE UPDATE ON audit_outputs "
                "BEGIN SELECT RAISE(ABORT,'immutable'); END"
            ),
            "audit_outputs_no_delete": (
                "CREATE TRIGGER audit_outputs_no_delete BEFORE DELETE ON audit_outputs "
                "BEGIN SELECT RAISE(ABORT,'immutable'); END"
            ),
            "audit_metadata_no_update": (
                "CREATE TRIGGER audit_metadata_no_update BEFORE UPDATE ON audit_metadata "
                "BEGIN SELECT RAISE(ABORT,'immutable'); END"
            ),
            "audit_metadata_no_delete": (
                "CREATE TRIGGER audit_metadata_no_delete BEFORE DELETE ON audit_metadata "
                "BEGIN SELECT RAISE(ABORT,'immutable'); END"
            ),
        }

    @classmethod
    def _legacy_trigger_definitions(cls) -> dict[str, str]:
        definitions = cls._trigger_definitions()
        return {
            name: statement
            for name, statement in definitions.items()
            if name.startswith("audit_outputs_")
        }

    @classmethod
    def _install_triggers(
        cls, db: sqlite3.Connection, *, metadata_only: bool = False
    ) -> None:
        for name, statement in cls._trigger_definitions().items():
            if metadata_only and not name.startswith("audit_metadata_"):
                continue
            db.execute(statement)

    @classmethod
    def _validate_triggers(
        cls, db: sqlite3.Connection, expected: Mapping[str, str]
    ) -> None:
        actual = {
            str(row[0]): str(row[1])
            for row in db.execute(
                "SELECT name,sql FROM sqlite_master WHERE type='trigger' ORDER BY name"
            )
        }
        if set(actual) != set(expected) or any(
            cls._normalized_sql(actual[name]) != cls._normalized_sql(statement)
            for name, statement in expected.items()
        ):
            raise RawManifestAuditIntegrityError("audit output triggers differ")

    @staticmethod
    def _metadata_columns(*, legacy: bool) -> tuple[tuple[str, str, int, int], ...]:
        columns = (
            ("singleton", "INTEGER", 0, 1),
            ("contract", "TEXT", 1, 0),
            ("schema_version", "INTEGER", 1, 0),
            ("limits_sha256", "TEXT", 1, 0),
            ("max_receipts", "INTEGER", 1, 0),
            ("max_receipt_bytes", "INTEGER", 1, 0),
            ("max_total_output_bytes", "INTEGER", 1, 0),
            ("max_database_bytes", "INTEGER", 1, 0),
        )
        return columns if legacy else (*columns, ("store_instance_sha256", "TEXT", 1, 0))

    @staticmethod
    def _output_columns() -> tuple[tuple[str, str, int, int], ...]:
        return (
            ("event_id", "TEXT", 0, 1),
            ("event_sha256", "TEXT", 1, 0),
            ("producer_receipt_sha256", "TEXT", 1, 0),
            ("workload_id", "TEXT", 1, 0),
            ("consumer_id", "TEXT", 1, 0),
            ("stream_id", "TEXT", 1, 0),
            ("desired_state_sha256", "TEXT", 1, 0),
            ("authority_sha256", "TEXT", 1, 0),
            ("capture_id", "TEXT", 1, 0),
            ("capture_manifest_sha256", "TEXT", 1, 0),
            ("isolation_authority_sha256", "TEXT", 1, 0),
            ("isolation_limits_sha256", "TEXT", 1, 0),
            ("output_sha256", "TEXT", 1, 0),
            ("receipt_sha256", "TEXT", 1, 0),
            ("process_receipt_sha256", "TEXT", 1, 0),
            ("output_json", "BLOB", 1, 0),
            ("output_bytes", "INTEGER", 1, 0),
            ("created_at", "REAL", 1, 0),
        )

    def _create_schema(self, db: sqlite3.Connection) -> None:
        db.execute(
            """CREATE TABLE audit_metadata(
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                contract TEXT NOT NULL,
                schema_version INTEGER NOT NULL,
                limits_sha256 TEXT NOT NULL,
                max_receipts INTEGER NOT NULL,
                max_receipt_bytes INTEGER NOT NULL,
                max_total_output_bytes INTEGER NOT NULL,
                max_database_bytes INTEGER NOT NULL,
                store_instance_sha256 TEXT NOT NULL
            )"""
        )
        db.execute(
            """CREATE TABLE audit_outputs(
                event_id TEXT PRIMARY KEY,
                event_sha256 TEXT NOT NULL,
                producer_receipt_sha256 TEXT NOT NULL UNIQUE,
                workload_id TEXT NOT NULL,
                consumer_id TEXT NOT NULL,
                stream_id TEXT NOT NULL,
                desired_state_sha256 TEXT NOT NULL,
                authority_sha256 TEXT NOT NULL,
                capture_id TEXT NOT NULL,
                capture_manifest_sha256 TEXT NOT NULL,
                isolation_authority_sha256 TEXT NOT NULL,
                isolation_limits_sha256 TEXT NOT NULL,
                output_sha256 TEXT NOT NULL UNIQUE,
                receipt_sha256 TEXT NOT NULL UNIQUE,
                process_receipt_sha256 TEXT NOT NULL UNIQUE,
                output_json BLOB NOT NULL,
                output_bytes INTEGER NOT NULL,
                created_at REAL NOT NULL
            )"""
        )
        db.execute(
            "INSERT INTO audit_metadata VALUES(1,?,?,?,?,?,?,?,?)",
            (
                OUTPUT_STORE_CONTRACT,
                SCHEMA_VERSION,
                self.limits.fingerprint,
                self.limits.max_receipts,
                self.limits.max_receipt_bytes,
                self.limits.max_total_output_bytes,
                self.limits.max_database_bytes,
                secrets.token_hex(32),
            ),
        )
        self._install_triggers(db)

    def _validate_unique_columns(self, db: sqlite3.Connection) -> None:
        unique_columns: set[str] = set()
        for index in db.execute("PRAGMA index_list(audit_outputs)"):
            if int(index[2]) != 1:
                continue
            members = tuple(
                str(row[0])
                for row in db.execute("SELECT name FROM pragma_index_info(?)", (str(index[1]),))
            )
            if len(members) != 1:
                raise RawManifestAuditIntegrityError("audit output uniqueness arity differs")
            unique_columns.add(members[0])
        if unique_columns != {
            "event_id",
            "producer_receipt_sha256",
            "output_sha256",
            "receipt_sha256",
            "process_receipt_sha256",
        }:
            raise RawManifestAuditIntegrityError("audit output uniqueness differs")

    def _validate_metadata(
        self, db: sqlite3.Connection, *, contract: str, schema_version: int
    ) -> sqlite3.Row:
        row = db.execute("SELECT * FROM audit_metadata WHERE singleton=1").fetchone()
        if (
            row is None
            or db.execute("SELECT count(*) FROM audit_metadata").fetchone()[0] != 1
            or row["contract"] != contract
            or row["schema_version"] != schema_version
            or row["limits_sha256"] != self.limits.fingerprint
            or row["max_receipts"] != self.limits.max_receipts
            or row["max_receipt_bytes"] != self.limits.max_receipt_bytes
            or row["max_total_output_bytes"] != self.limits.max_total_output_bytes
            or row["max_database_bytes"] != self.limits.max_database_bytes
        ):
            raise RawManifestAuditIntegrityError("audit output metadata differs")
        return row

    def _validate_capacity(self, db: sqlite3.Connection) -> None:
        count, total, invalid = db.execute(
            "SELECT count(*),coalesce(sum(output_bytes),0),"
            "coalesce(sum(CASE WHEN output_bytes < 1 OR output_bytes > ? THEN 1 ELSE 0 END),0) "
            "FROM audit_outputs",
            (self.limits.max_receipt_bytes,),
        ).fetchone()
        if (
            int(count) > self.limits.max_receipts
            or int(total) > self.limits.max_total_output_bytes
            or int(invalid) != 0
        ):
            raise RawManifestAuditCapacityError("audit output capacity differs")

    def _validate_legacy_v1(self, db: sqlite3.Connection) -> None:
        if self._tables(db) != {"audit_metadata", "audit_outputs"}:
            raise RawManifestAuditIntegrityError("legacy audit output tables differ")
        if self._columns(db, "audit_metadata") != self._metadata_columns(legacy=True):
            raise RawManifestAuditIntegrityError("legacy audit metadata columns differ")
        if self._columns(db, "audit_outputs") != self._output_columns():
            raise RawManifestAuditIntegrityError("legacy audit output columns differ")
        self._validate_unique_columns(db)
        self._validate_metadata(
            db,
            contract=LEGACY_OUTPUT_STORE_CONTRACT,
            schema_version=LEGACY_SCHEMA_VERSION,
        )
        self._validate_triggers(db, self._legacy_trigger_definitions())
        self._validate_capacity(db)
        if db.execute("PRAGMA quick_check").fetchone()[0] != "ok":
            raise RawManifestAuditIntegrityError("legacy audit output integrity check failed")
        for row in db.execute("SELECT * FROM audit_outputs"):
            self._row(row)

    def _is_legacy_v1(self, db: sqlite3.Connection) -> bool:
        if self._tables(db) != {"audit_metadata", "audit_outputs"}:
            return False
        if self._columns(db, "audit_metadata") != self._metadata_columns(legacy=True):
            return False
        row = db.execute(
            "SELECT contract,schema_version FROM audit_metadata WHERE singleton=1"
        ).fetchone()
        return bool(
            row is not None
            and row["contract"] == LEGACY_OUTPUT_STORE_CONTRACT
            and int(row["schema_version"]) == LEGACY_SCHEMA_VERSION
        )

    def _migrate_v1_to_v2(self, db: sqlite3.Connection) -> None:
        self._validate_legacy_v1(db)
        db.execute(
            "ALTER TABLE audit_metadata ADD COLUMN store_instance_sha256 "
            "TEXT NOT NULL DEFAULT ''"
        )
        db.execute(
            "UPDATE audit_metadata SET contract=?,schema_version=?,store_instance_sha256=? "
            "WHERE singleton=1",
            (OUTPUT_STORE_CONTRACT, SCHEMA_VERSION, secrets.token_hex(32)),
        )
        self._fault("migration.after_metadata")
        self._install_triggers(db, metadata_only=True)
        self._fault("migration.after_triggers")

    def initialize(self) -> None:
        with self._lock:
            self._database_witness = self._prepare_path()
            try:
                with closing(self._connect()) as db:
                    try:
                        db.execute("BEGIN IMMEDIATE")
                        if not self._tables(db):
                            self._create_schema(db)
                        elif self._is_legacy_v1(db):
                            self._migrate_v1_to_v2(db)
                        self._validate(db)
                        self._fault("migration.before_commit")
                        db.commit()
                        self._fault("migration.after_commit")
                    except BaseException:
                        if db.in_transaction:
                            db.rollback()
                        raise
                with closing(self._connect()) as db:
                    self._store_instance_sha256 = self._validate(db, full=True)
            except sqlite3.DatabaseError as exc:
                raise RawManifestAuditIntegrityError("audit output SQLite failure") from exc

    def _validate(self, db: sqlite3.Connection, full: bool = False) -> str:
        if self._tables(db) != {"audit_metadata", "audit_outputs"}:
            raise RawManifestAuditIntegrityError("audit output table inventory differs")
        if self._columns(db, "audit_metadata") != self._metadata_columns(legacy=False):
            raise RawManifestAuditIntegrityError("audit output metadata columns differ")
        if self._columns(db, "audit_outputs") != self._output_columns():
            raise RawManifestAuditIntegrityError("audit output columns differ")
        self._validate_unique_columns(db)
        metadata = self._validate_metadata(
            db, contract=OUTPUT_STORE_CONTRACT, schema_version=SCHEMA_VERSION
        )
        instance_sha = _sha(metadata["store_instance_sha256"], "output store instance SHA")
        if hasattr(self, "_store_instance_sha256") and not hmac.compare_digest(
            self._store_instance_sha256, instance_sha
        ):
            raise RawManifestAuditIntegrityError("audit output store instance authority changed")
        self._validate_triggers(db, self._trigger_definitions())
        self._validate_capacity(db)
        if full and db.execute("PRAGMA quick_check").fetchone()[0] != "ok":
            raise RawManifestAuditIntegrityError("audit output integrity check failed")
        if full:
            for output_row in db.execute("SELECT * FROM audit_outputs"):
                self._row(output_row)
        return instance_sha

    def _identity_for_instance(self, instance_sha: str) -> str:
        path_text = self._normalized_path(self.path)
        return _digest(
            {
                "contract": OUTPUT_STORE_CONTRACT,
                "file_witness": {
                    "st_dev": self._database_witness[0],
                    "st_ino": self._database_witness[1],
                    "st_mode": self._database_witness[2],
                },
                "limits_sha256": self.limits.fingerprint,
                "path_sha256": hashlib.sha256(path_text.encode("utf-8")).hexdigest(),
                "schema": OUTPUT_STORE_IDENTITY_SCHEMA,
                "schema_version": SCHEMA_VERSION,
                "store_instance_sha256": _sha(instance_sha, "output store instance SHA"),
            }
        )

    @property
    def store_instance_sha256(self) -> str:
        self._verify_path()
        return self._store_instance_sha256

    @property
    def identity_sha256(self) -> str:
        self._verify_path()
        return self._identity_for_instance(self._store_instance_sha256)

    def assert_instance_held(self, expected_identity_sha256: str | None = None) -> str:
        """Reprove the visible file witness and persistent instance sentinel."""

        with self._lock, closing(self._connect()) as db:
            instance_sha = self._validate(db)
            identity = self._identity_for_instance(instance_sha)
        if expected_identity_sha256 is not None and not hmac.compare_digest(
            identity, _sha(expected_identity_sha256, "expected output store identity SHA")
        ):
            raise RawManifestAuditIntegrityError("audit output store identity authority differs")
        return identity

    def _row(self, row: sqlite3.Row) -> RawManifestAuditOutputReceipt:
        body = bytes(row["output_json"])
        if len(body) != row["output_bytes"] or not 0 < len(body) <= self.limits.max_receipt_bytes:
            raise RawManifestAuditIntegrityError("audit output size differs")
        try:
            raw = json.loads(body.decode("ascii"), object_pairs_hook=_reject_duplicates)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RawManifestAuditIntegrityError("audit output JSON corrupt") from exc
        doc, event, authority, audit, isolation = _output_document(raw)
        output_sha = hashlib.sha256(body).hexdigest()
        created = _timestamp(row["created_at"], "created at")
        receipt_sha = _digest(
            {"created_at": created, "output_sha256": output_sha, "schema": OUTPUT_RECEIPT_SCHEMA}
        )
        process_sha = _digest(
            {
                "created_at": created,
                "effect_contract_sha256": PROCESS_EFFECT_CONTRACT_SHA256,
                "event_id": event["event_id"],
                "isolation_authority_sha256": isolation["authority_sha256"],
                "limits_sha256": isolation["limits_sha256"],
                "output_receipt_sha256": receipt_sha,
                "output_sha256": output_sha,
                "schema": PROCESS_RECEIPT_SCHEMA,
            }
        )
        checks = (
            (row["event_id"], event["event_id"]),
            (row["event_sha256"], event["event_sha256"]),
            (row["producer_receipt_sha256"], event["producer_receipt_sha256"]),
            (row["workload_id"], doc["workload_id"]),
            (row["consumer_id"], doc["consumer_id"]),
            (row["stream_id"], doc["stream_id"]),
            (row["desired_state_sha256"], authority["desired_state_sha256"]),
            (row["authority_sha256"], authority["authority_sha256"]),
            (row["capture_id"], audit["capture_id"]),
            (row["capture_manifest_sha256"], audit["capture_manifest_sha256"]),
            (row["isolation_authority_sha256"], isolation["authority_sha256"]),
            (row["isolation_limits_sha256"], isolation["limits_sha256"]),
            (row["output_sha256"], output_sha),
            (row["receipt_sha256"], receipt_sha),
            (row["process_receipt_sha256"], process_sha),
            (_canonical(doc), body),
        )
        if any(a != b for a, b in checks):
            raise RawManifestAuditIntegrityError("audit output row differs")
        return RawManifestAuditOutputReceipt(
            receipt_sha,
            process_sha,
            output_sha,
            event["event_id"],
            event["event_sha256"],
            event["producer_receipt_sha256"],
            doc["workload_id"],
            doc["consumer_id"],
            doc["stream_id"],
            authority["desired_state_sha256"],
            authority["authority_sha256"],
            audit["capture_id"],
            audit["capture_manifest_sha256"],
            isolation["authority_sha256"],
            isolation["limits_sha256"],
            created,
            doc,
        )

    def load(self, event_id: str) -> RawManifestAuditOutputReceipt | None:
        with self._lock, closing(self._connect()) as db:
            self._validate(db)
            row = db.execute(
                "SELECT * FROM audit_outputs WHERE event_id=?", (_identifier(event_id, "event ID"),)
            ).fetchone()
            return None if row is None else self._row(row)

    def commit(self, output: Mapping[str, Any]) -> RawManifestAuditOutputReceipt:
        doc, event, authority, audit, isolation = _output_document(output)
        body = _canonical(doc)
        if len(body) > self.limits.max_receipt_bytes:
            raise RawManifestAuditCapacityError("audit output exceeds cap")
        output_sha = hashlib.sha256(body).hexdigest()
        try:
            with self._lock, closing(self._connect()) as db:
                db.execute("BEGIN IMMEDIATE")
                self._validate(db)
                existing = db.execute(
                    "SELECT * FROM audit_outputs WHERE event_id=?", (event["event_id"],)
                ).fetchone()
                if existing is not None:
                    receipt = self._row(existing)
                    if receipt.output_sha256 != output_sha:
                        raise RawManifestAuditConflict("audit output replay differs")
                    db.commit()
                    return receipt
                count, total = db.execute(
                    "SELECT count(*),coalesce(sum(output_bytes),0) FROM audit_outputs"
                ).fetchone()
                if (
                    count >= self.limits.max_receipts
                    or total + len(body) > self.limits.max_total_output_bytes
                ):
                    raise RawManifestAuditCapacityError("audit output capacity reached")
                created = _timestamp(self._clock(), "output clock")
                receipt_sha = _digest(
                    {
                        "created_at": created,
                        "output_sha256": output_sha,
                        "schema": OUTPUT_RECEIPT_SCHEMA,
                    }
                )
                process_sha = _digest(
                    {
                        "created_at": created,
                        "effect_contract_sha256": PROCESS_EFFECT_CONTRACT_SHA256,
                        "event_id": event["event_id"],
                        "isolation_authority_sha256": isolation["authority_sha256"],
                        "limits_sha256": isolation["limits_sha256"],
                        "output_receipt_sha256": receipt_sha,
                        "output_sha256": output_sha,
                        "schema": PROCESS_RECEIPT_SCHEMA,
                    }
                )
                db.execute(
                    "INSERT INTO audit_outputs VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        event["event_id"],
                        event["event_sha256"],
                        event["producer_receipt_sha256"],
                        doc["workload_id"],
                        doc["consumer_id"],
                        doc["stream_id"],
                        authority["desired_state_sha256"],
                        authority["authority_sha256"],
                        audit["capture_id"],
                        audit["capture_manifest_sha256"],
                        isolation["authority_sha256"],
                        isolation["limits_sha256"],
                        output_sha,
                        receipt_sha,
                        process_sha,
                        body,
                        len(body),
                        created,
                    ),
                )
                db.commit()
        except sqlite3.DatabaseError as exc:
            if "full" in str(exc).lower():
                raise RawManifestAuditCapacityError("audit SQLite full") from exc
            raise RawManifestAuditIntegrityError("audit SQLite failure") from exc
        receipt = self.load(event["event_id"])
        if receipt is None:
            raise RawManifestAuditIntegrityError("audit output disappeared")
        return receipt

    def _verify_receipt(
        self,
        receipt: RawManifestAuditOutputReceipt,
        lease: CompletionLease,
        binding: RawManifestAuditBinding,
        authority_sha: str,
        limits_sha: str,
    ) -> RawManifestAuditOutputReceipt:
        audit = receipt.output["audit"]
        checks = (
            (receipt.event_sha256, lease.event_sha256),
            (receipt.producer_receipt_sha256, lease.producer_receipt_sha256),
            (receipt.workload_id, binding.workload_id),
            (receipt.consumer_id, binding.consumer_id),
            (receipt.stream_id, binding.stream_id),
            (receipt.desired_state_sha256, lease.desired_state_sha256),
            (receipt.authority_sha256, lease.authority_sha256),
            (receipt.capture_id, binding.capture_id),
            (receipt.capture_manifest_sha256, binding.capture_manifest_sha256),
            (receipt.isolation_authority_sha256, authority_sha),
            (receipt.isolation_limits_sha256, limits_sha),
            (audit["roles"], [role.as_dict() for role in binding.roles]),
            (audit["total_bytes"], binding.total_bytes),
            (audit["role_set_content_sha256"], binding.role_set_content_sha256),
            (audit["role_set_receipt_sha256"], binding.role_set_receipt_sha256),
            (receipt.output["fact_revision"], None),
            (receipt.output["claims"], {key: False for key in _CLAIM_KEYS}),
        )
        if any(a != b for a, b in checks):
            raise RawManifestAuditConflict("durable audit proof differs")
        return receipt

    def verify(
        self,
        lease: CompletionLease,
        binding: RawManifestAuditBinding,
        authority_sha: str,
        limits_sha: str,
    ) -> RawManifestAuditOutputReceipt:
        receipt = self.load(lease.event_id)
        if receipt is None:
            raise RawManifestAuditIntegrityError("durable audit output missing")
        return self._verify_receipt(receipt, lease, binding, authority_sha, limits_sha)

    @contextmanager
    def completion_reproof_lease(
        self,
        *,
        lease: CompletionLease,
        binding: RawManifestAuditBinding,
        authority_sha: str,
        limits_sha: str,
        expected_identity_sha256: str,
    ) -> Iterator[RawManifestAuditOutputReceipt]:
        """Fence one exact audit row against SQLite writers through lifecycle ACK."""

        if not isinstance(lease, CompletionLease) or not isinstance(
            binding, RawManifestAuditBinding
        ):
            raise TypeError("audit completion reproof authority differs")
        expected_identity = _sha(expected_identity_sha256, "expected output store identity SHA")
        event_id = _identifier(lease.event_id, "event ID")
        with self._lock, closing(self._connect()) as db:
            try:
                db.execute("BEGIN IMMEDIATE")
                instance_sha = self._validate(db, full=True)
                if not hmac.compare_digest(
                    self._identity_for_instance(instance_sha), expected_identity
                ):
                    raise RawManifestAuditIntegrityError(
                        "audit output store identity authority differs"
                    )
                row = db.execute(
                    "SELECT * FROM audit_outputs WHERE event_id=?", (event_id,)
                ).fetchone()
                if row is None:
                    raise RawManifestAuditIntegrityError("durable audit output missing")
                receipt = self._verify_receipt(
                    self._row(row), lease, binding, authority_sha, limits_sha
                )
                yield receipt
                self._verify_path()
                final_instance = self._validate(db)
                if not hmac.compare_digest(
                    self._identity_for_instance(final_instance), expected_identity
                ):
                    raise RawManifestAuditIntegrityError(
                        "audit output store changed while reproof was fenced"
                    )
                final_row = db.execute(
                    "SELECT * FROM audit_outputs WHERE event_id=?", (event_id,)
                ).fetchone()
                if final_row is None:
                    raise RawManifestAuditIntegrityError(
                        "durable audit disappeared while reproof was fenced"
                    )
                final_receipt = self._verify_receipt(
                    self._row(final_row), lease, binding, authority_sha, limits_sha
                )
                if final_receipt != receipt:
                    raise RawManifestAuditIntegrityError(
                        "durable audit changed while reproof was fenced"
                    )
            finally:
                if db.in_transaction:
                    db.rollback()

    def diagnostics(self) -> dict[str, Any]:
        with self._lock, closing(self._connect()) as db:
            self._validate(db, True)
            count, total = db.execute(
                "SELECT count(*),coalesce(sum(output_bytes),0) FROM audit_outputs"
            ).fetchone()
        return {
            "contract": OUTPUT_STORE_CONTRACT,
            "healthy": True,
            "receipts": int(count),
            "output_bytes": int(total),
            "runtime_wiring": "inert",
        }


def _safe_path(value: object, label: str, directory: bool) -> Path:
    if not isinstance(value, str) or not value or "\0" in value or len(value) > 1024:
        raise RawManifestAuditIntegrityError(f"{label} path invalid")
    path = Path(value)
    if (
        not path.is_absolute()
        or path.is_symlink()
        or not path.exists()
        or path.resolve(strict=True) != path
    ):
        raise RawManifestAuditIntegrityError(f"{label} path unsafe")
    mode = path.lstat().st_mode
    if (directory and not stat.S_ISDIR(mode)) or (not directory and not stat.S_ISREG(mode)):
        raise RawManifestAuditIntegrityError(f"{label} path type differs")
    return path


def _assert_process_store_paths_disjoint(*, raw_root: Path, output_path: Path) -> None:
    """Reject authority aliasing between the source object store and output DB."""

    if output_path.is_relative_to(raw_root):
        raise RawManifestAuditIntegrityError("audit output store overlaps source raw store")


def _assert_consumer_store_paths_disjoint(
    *,
    raw_root: Path,
    output_path: Path,
    lifecycle_root: Path,
    lifecycle_path: Path,
    desired_path: Path,
) -> None:
    """Keep independently authoritative stores in disjoint filesystem roles."""

    if (
        raw_root.samefile(lifecycle_root)
        or raw_root.is_relative_to(lifecycle_root)
        or lifecycle_root.is_relative_to(raw_root)
    ):
        raise RawManifestAuditIntegrityError("raw and lifecycle store roots overlap")
    databases = (output_path, lifecycle_path, desired_path)
    for index, left in enumerate(databases):
        for right in databases[index + 1 :]:
            if left.samefile(right):
                raise RawManifestAuditIntegrityError("consumer database store paths alias")
    for database_path in (output_path, desired_path):
        if database_path.is_relative_to(raw_root) or database_path.is_relative_to(lifecycle_root):
            raise RawManifestAuditIntegrityError("consumer database overlaps an object store")


def _catalog_profile() -> dict[str, Any]:
    return {key: value.as_dict() for key, value in sorted(AUDIT_CONSUMER_CATALOG.items())}


def _authority(
    value: object, expected_sha: str
) -> tuple[
    Path, RawArtifactLimits, Path, RawManifestAuditOutputLimits, RawManifestAuditProcessLimits
]:
    mapping = _exact(
        value,
        {
            "catalog",
            "catalog_sha256",
            "effect_contract",
            "effect_contract_sha256",
            "process_limits",
            "schema",
            "stores",
        },
        "process authority",
    )
    if (
        mapping["schema"] != PROCESS_AUTHORITY_SCHEMA
        or _digest(mapping) != _sha(expected_sha, "authority SHA")
        or mapping["catalog"] != _catalog_profile()
        or mapping["catalog_sha256"] != _digest(_catalog_profile())
        or mapping["effect_contract"] != _effect()
        or mapping["effect_contract_sha256"] != PROCESS_EFFECT_CONTRACT_SHA256
    ):
        raise RawManifestAuditIntegrityError("process authority differs")
    process_value = _exact(
        mapping["process_limits"],
        {"cpu_seconds", "max_request_bytes", "max_result_bytes", "rss_bytes", "wall_seconds"},
        "process limits",
    )
    process_limits = RawManifestAuditProcessLimits(**dict(process_value))
    stores = _exact(mapping["stores"], {"outputs", "source_raw"}, "process stores")
    raw_value = _exact(stores["source_raw"], {"limits", "root"}, "raw store")
    raw_limits = RawArtifactLimits(
        **dict(
            _exact(raw_value["limits"], set(RawArtifactLimits.__dataclass_fields__), "raw limits")
        )
    )
    output_value = _exact(stores["outputs"], {"limits", "path"}, "output store")
    output_limits = RawManifestAuditOutputLimits(
        **dict(
            _exact(
                output_value["limits"],
                set(RawManifestAuditOutputLimits.__dataclass_fields__),
                "output limits",
            )
        )
    )
    raw_root = _safe_path(raw_value["root"], "source raw", True)
    output_path = _safe_path(output_value["path"], "output", False)
    _assert_process_store_paths_disjoint(raw_root=raw_root, output_path=output_path)
    return (
        raw_root,
        raw_limits,
        output_path,
        output_limits,
        process_limits,
    )


def _lease_wire(lease: CompletionLease) -> dict[str, Any]:
    return {
        "acquisition_job_id": lease.acquisition_job_id,
        "attempt_count": lease.attempt_count,
        "authority_sha256": lease.authority_sha256,
        "consumer_id": lease.consumer_id,
        "content_sha256": lease.content_sha256,
        "desired_state_sha256": lease.desired_state_sha256,
        "event_id": lease.event_id,
        "event_sha256": lease.event_sha256,
        "lease_expires_at": lease.lease_expires_at,
        "payload": _decode(lease.payload),
        "producer_receipt_sha256": lease.producer_receipt_sha256,
        "proof": json.loads(_canonical(lease.proof)),
        "size_bytes": lease.size_bytes,
        "stream_id": lease.stream_id,
        "stream_sequence": lease.stream_sequence,
    }


def _lease_unwire(value: object) -> CompletionLease:
    m = _exact(
        value,
        {
            "acquisition_job_id",
            "attempt_count",
            "authority_sha256",
            "consumer_id",
            "content_sha256",
            "desired_state_sha256",
            "event_id",
            "event_sha256",
            "lease_expires_at",
            "payload",
            "producer_receipt_sha256",
            "proof",
            "size_bytes",
            "stream_id",
            "stream_sequence",
        },
        "isolated lease",
    )
    return CompletionLease(
        _identifier(m["event_id"], "event"),
        _sha(m["event_sha256"], "event SHA"),
        _sha(m["producer_receipt_sha256"], "producer SHA"),
        _identifier(m["acquisition_job_id"], "job"),
        _identifier(m["consumer_id"], "consumer"),
        _identifier(m["stream_id"], "stream"),
        _integer(m["stream_sequence"], "sequence", 1),
        _sha(m["desired_state_sha256"], "desired SHA"),
        _sha(m["authority_sha256"], "authority SHA"),
        _sha(m["content_sha256"], "content SHA"),
        _integer(m["size_bytes"], "size", 1),
        _integer(m["attempt_count"], "attempt", 1),
        "0" * 64,
        _timestamp(m["lease_expires_at"], "expiry"),
        dict(m["proof"]),
        _canonical(m["payload"]),
    )


def _make_output(
    lease: CompletionLease, binding: RawManifestAuditBinding, authority_sha: str, limits_sha: str
) -> dict[str, Any]:
    output = {
        "audit": {
            "acquisition_job_id": binding.acquisition_job_id,
            "capture_id": binding.capture_id,
            "capture_manifest_sha256": binding.capture_manifest_sha256,
            "capture_receipt_sha256": binding.capture_receipt_sha256,
            "product_id": binding.product_id,
            "provider_id": binding.provider_id,
            "raw_bytes_verified": True,
            "role_set_content_sha256": binding.role_set_content_sha256,
            "role_set_receipt_sha256": binding.role_set_receipt_sha256,
            "roles": [role.as_dict() for role in binding.roles],
            "total_bytes": binding.total_bytes,
        },
        "authority": {
            "authority_sha256": lease.authority_sha256,
            "desired_state_sha256": lease.desired_state_sha256,
        },
        "claims": {key: False for key in _CLAIM_KEYS},
        "consumer_id": binding.consumer_id,
        "disposition": "raw_manifest_verified_shadow",
        "event": {
            "event_id": lease.event_id,
            "event_sha256": lease.event_sha256,
            "producer_receipt_sha256": lease.producer_receipt_sha256,
            "stream_sequence": lease.stream_sequence,
        },
        "fact_revision": None,
        "isolation": {
            "authority_sha256": authority_sha,
            "effect_contract_sha256": PROCESS_EFFECT_CONTRACT_SHA256,
            "limits_sha256": limits_sha,
            "process_boundary": "fresh_spawn_hard_cpu_rss_wall_output_caps",
            "resource_reason": "full_sha_pass_over_bounded_multi_role_raw_bytes",
            "runtime_wiring": "inert",
        },
        "mode": OUTPUT_MODE,
        "schema": OUTPUT_DOCUMENT_SCHEMA,
        "stream_id": binding.stream_id,
        "workload_id": binding.workload_id,
    }
    _output_document(output)
    return output


def _raw_manifest_audit_child(request: Mapping[str, Any]) -> dict[str, Any]:
    mapping = _exact(
        request,
        {
            "authority",
            "authority_sha256",
            "desired_state",
            "lease",
            "output_store_identity_sha256",
            "request_sha256",
            "schema",
        },
        "process request",
    )
    body = {key: mapping[key] for key in mapping if key != "request_sha256"}
    if mapping["schema"] != PROCESS_REQUEST_SCHEMA or _digest(body) != _sha(
        mapping["request_sha256"], "request SHA"
    ):
        raise RawManifestAuditIntegrityError("process request differs")
    raw_root, raw_limits, output_path, output_limits, process_limits = _authority(
        mapping["authority"], mapping["authority_sha256"]
    )
    desired = parse_desired_state(mapping["desired_state"])
    lease = _lease_unwire(mapping["lease"])
    expected_output_identity = _sha(
        mapping["output_store_identity_sha256"], "output store identity SHA"
    )
    raw = RawArtifactStore(raw_root, limits=raw_limits)
    raw.initialize()
    binding = validate_raw_manifest_audit_lease(
        lease, desired_state=desired, raw_store=raw, verify_raw_bytes=True
    )
    outputs = RawManifestAuditOutputStore(output_path, limits=output_limits)
    outputs.assert_instance_held(expected_output_identity)
    receipt = outputs.commit(
        _make_output(lease, binding, mapping["authority_sha256"], process_limits.fingerprint)
    )
    outputs.verify(lease, binding, mapping["authority_sha256"], process_limits.fingerprint)
    outputs.assert_instance_held(expected_output_identity)
    return {
        "event_id": receipt.event_id,
        "output_receipt_sha256": receipt.receipt_sha256,
        "output_sha256": receipt.output_sha256,
        "process_receipt_sha256": receipt.process_receipt_sha256,
        "schema": PROCESS_CHILD_RESULT_SCHEMA,
    }


class RawManifestAuditIsolatedRunner:
    def __init__(
        self,
        *,
        raw_store: RawArtifactStore,
        output_store: RawManifestAuditOutputStore,
        limits: RawManifestAuditProcessLimits = RawManifestAuditProcessLimits(),
        context: multiprocessing.context.BaseContext | None = None,
    ):
        if not isinstance(raw_store, RawArtifactStore) or not isinstance(
            output_store, RawManifestAuditOutputStore
        ):
            raise TypeError("audit runner store type differs")
        self.raw_store = raw_store
        self.output_store = output_store
        self.limits = limits
        self.output_store_identity_sha256 = output_store.assert_instance_held()
        for path, label, directory in (
            (raw_store.root, "raw", True),
            (output_store.path, "output", False),
        ):
            _safe_path(str(path), label, directory)
        _assert_process_store_paths_disjoint(
            raw_root=raw_store.root,
            output_path=output_store.path,
        )
        self._authority = {
            "catalog": _catalog_profile(),
            "catalog_sha256": _digest(_catalog_profile()),
            "effect_contract": _effect(),
            "effect_contract_sha256": PROCESS_EFFECT_CONTRACT_SHA256,
            "process_limits": limits.as_dict(),
            "schema": PROCESS_AUTHORITY_SCHEMA,
            "stores": {
                "outputs": {"limits": asdict(output_store.limits), "path": str(output_store.path)},
                "source_raw": {"limits": asdict(raw_store.limits), "root": str(raw_store.root)},
            },
        }
        self.authority_sha256 = _digest(self._authority)
        _authority(self._authority, self.authority_sha256)
        self._executor = HardDecoderExecutor(limits=limits.decoder_limits(), context=context)

    @property
    def minimum_lease_seconds(self) -> float:
        return 2.0 * self.limits.wall_seconds + 4.0

    @property
    def final_reproof_seconds(self) -> float:
        return self.limits.wall_seconds + 2.0

    def _request(self, lease: CompletionLease, desired: DesiredState) -> dict[str, Any]:
        body = {
            "authority": self._authority,
            "authority_sha256": self.authority_sha256,
            "desired_state": desired.as_dict(),
            "lease": _lease_wire(lease),
            "output_store_identity_sha256": self.output_store_identity_sha256,
            "schema": PROCESS_REQUEST_SCHEMA,
        }
        return {**body, "request_sha256": _digest(body)}

    @staticmethod
    def _technical_status(result: DecoderProcessResult) -> tuple[str, str]:
        status = {
            "deadline_exceeded": "deadline",
            "cancelled": "cancelled",
            "busy": "busy",
            "cpu_limit_exceeded": "resource_limited",
            "memory_limit_exceeded": "resource_limited",
            "result_too_large": "resource_limited",
        }.get(result.outcome, "failed")
        return status, "hard_process_" + result.outcome

    def execute(
        self, lease: CompletionLease, desired: DesiredState, cancel: threading.Event
    ) -> tuple[str, str, RawManifestAuditOutputReceipt | None]:
        self.output_store.assert_instance_held(self.output_store_identity_sha256)
        binding = validate_raw_manifest_audit_lease(lease, desired_state=desired)
        existing = self.output_store.load(lease.event_id)
        if existing is not None:
            return (
                "committed",
                "durable_audit_reconciled_before_spawn",
                self.output_store.verify(
                    lease, binding, self.authority_sha256, self.limits.fingerprint
                ),
            )
        result = self._executor.execute(
            target=PROCESS_TARGET,
            request=self._request(lease, desired),
            effect_contract=PROCESS_EFFECT_CONTRACT,
            cancel_event=cancel,
        )
        self.output_store.assert_instance_held(self.output_store_identity_sha256)
        try:
            durable = self.output_store.load(lease.event_id)
        except RawManifestAuditError:
            return "durable_unknown", "durable_audit_verification_failed", None
        if durable is not None:
            return (
                "committed",
                "durable_audit_reconciled_after_child",
                self.output_store.verify(
                    lease, binding, self.authority_sha256, self.limits.fingerprint
                ),
            )
        status, reason = self._technical_status(result)
        return status, reason, None

    def reprove_for_ack(
        self,
        lease: CompletionLease,
        desired: DesiredState,
        cancel: threading.Event,
    ) -> tuple[str, str, RawManifestAuditOutputReceipt | None]:
        """Repeat the full raw SHA pass in a hard child immediately before ack."""

        self.output_store.assert_instance_held(self.output_store_identity_sha256)
        binding = validate_raw_manifest_audit_lease(lease, desired_state=desired)
        result = self._executor.execute(
            target=PROCESS_TARGET,
            request=self._request(lease, desired),
            effect_contract=PROCESS_EFFECT_CONTRACT,
            cancel_event=cancel,
        )
        self.output_store.assert_instance_held(self.output_store_identity_sha256)
        if result.outcome != "succeeded":
            status, reason = self._technical_status(result)
            return status, "final_reproof_" + reason, None
        child = _exact(
            result.result,
            {
                "event_id",
                "output_receipt_sha256",
                "output_sha256",
                "process_receipt_sha256",
                "schema",
            },
            "final reproof child result",
        )
        durable = self.output_store.verify(
            lease,
            binding,
            self.authority_sha256,
            self.limits.fingerprint,
        )
        if (
            child["schema"] != PROCESS_CHILD_RESULT_SCHEMA
            or child["event_id"] != lease.event_id
            or child["output_sha256"] != durable.output_sha256
            or child["output_receipt_sha256"] != durable.receipt_sha256
            or child["process_receipt_sha256"] != durable.process_receipt_sha256
        ):
            raise RawManifestAuditIntegrityError("final raw reproof result differs")
        return "committed", "final_raw_and_output_reproof_complete", durable


class RawManifestAuditCompletionConsumer:
    def __init__(
        self,
        *,
        lifecycle_store: AcquisitionEvidenceLifecycleStore,
        desired_state_store: DesiredStateStore,
        runner: RawManifestAuditIsolatedRunner,
        clock: Callable[[], float] = time.time,
        fault_injector: Callable[[str], None] | None = None,
    ):
        self.lifecycle = lifecycle_store
        self.desired = desired_state_store
        self.runner = runner
        self._clock = clock
        self._fault = fault_injector
        if not isinstance(lifecycle_store, AcquisitionEvidenceLifecycleStore) or not isinstance(
            desired_state_store, DesiredStateStore
        ):
            raise TypeError("audit consumer store type differs")

        _assert_consumer_store_paths_disjoint(
            raw_root=_safe_path(str(runner.raw_store.root), "raw", True),
            output_path=_safe_path(str(runner.output_store.path), "output", False),
            lifecycle_root=_safe_path(str(lifecycle_store.root), "lifecycle", True),
            lifecycle_path=_safe_path(str(lifecycle_store.path), "lifecycle DB", False),
            desired_path=_safe_path(str(desired_state_store.path), "desired DB", False),
        )

    def _call_fault(self, point: str) -> None:
        if self._fault is not None:
            self._fault(point)

    @staticmethod
    def _validate_consumption_receipt(
        receipt: ConsumptionReceipt,
        lease: CompletionLease,
        *,
        expected_output_sha256: str | None,
    ) -> None:
        if not isinstance(receipt, ConsumptionReceipt):
            raise RawManifestAuditIntegrityError("lifecycle terminalization returned no receipt")
        document = receipt.as_dict()
        receipt_sha = document.pop("receipt_sha256", None)
        if (
            set(document)
            != {
                "consumer_id",
                "created_at",
                "event_id",
                "event_sha256",
                "output_sha256",
                "producer_receipt_sha256",
                "schema",
                "stream_id",
                "stream_sequence",
            }
            or document["schema"] != CONSUMPTION_RECEIPT_SCHEMA
            or receipt.event_id != lease.event_id
            or receipt.event_sha256 != lease.event_sha256
            or receipt.producer_receipt_sha256 != lease.producer_receipt_sha256
            or receipt.consumer_id != lease.consumer_id
            or receipt.stream_id != lease.stream_id
            or receipt.stream_sequence != lease.stream_sequence
            or _SHA.fullmatch(receipt.output_sha256) is None
            or _SHA.fullmatch(receipt.receipt_sha256) is None
            or receipt_sha != receipt.receipt_sha256
            or _digest(document) != receipt.receipt_sha256
            or (
                expected_output_sha256 is not None
                and receipt.output_sha256 != expected_output_sha256
            )
        ):
            raise RawManifestAuditIntegrityError("lifecycle terminal receipt differs")
        _timestamp(receipt.created_at, "consumption receipt clock")

    def _reject_obsolete(
        self,
        lease: CompletionLease,
        *,
        active_desired: DesiredState | None,
        reason_code: str,
        now: float,
        phase_child_invocations: int = 0,
    ) -> RawManifestAuditRunResult:
        rejection_reason = (
            "active_desired_state_unavailable" if active_desired is None else reason_code
        )
        receipt = self.lifecycle.reject_leased(
            lease.event_id,
            lease.lease_token,
            expected_event_sha256=lease.event_sha256,
            reason_code=rejection_reason,
            active_desired_state_sha256=(
                None if active_desired is None else active_desired.state_sha256
            ),
            now=now,
        )
        self._validate_consumption_receipt(
            receipt,
            lease,
            expected_output_sha256=None,
        )
        return RawManifestAuditRunResult(
            "rejected",
            rejection_reason,
            lease.event_id,
            consumption_receipt_sha256=receipt.receipt_sha256,
            phase_child_invocations=phase_child_invocations,
        )

    def begin_leased(
        self,
        lease: CompletionLease,
        cancel_event: threading.Event | None = None,
        *,
        before_child: Callable[[], None] | None = None,
    ) -> RawManifestAuditPendingAck | RawManifestAuditRunResult:
        """Run at most one initial audit child and defer final reproof/ack."""

        cancel = cancel_event or threading.Event()
        if not isinstance(lease, CompletionLease):
            raise TypeError("lease must be CompletionLease")
        if not callable(getattr(cancel, "is_set", None)):
            raise TypeError("cancel_event must expose is_set()")
        if before_child is not None and not callable(before_child):
            raise TypeError("before_child must be callable")
        output_store_identity = self.runner.output_store.assert_instance_held(
            self.runner.output_store_identity_sha256
        )
        now = _timestamp(self._clock(), "consumer clock")
        if cancel.is_set():
            raise RawManifestAuditCancelled("cancelled before audit")
        reloaded_initial = self.lifecycle.load_leased(lease.event_id, lease.lease_token)
        if reloaded_initial != lease:
            raise RawManifestAuditConflict("lifecycle lease changed before validation")
        if lease.lease_expires_at - now < self.runner.minimum_lease_seconds:
            raise RawManifestAuditDeadlineError("lease cannot contain hard audit")
        desired = self.desired.load_active()
        if desired is None:
            return self._reject_obsolete(
                lease,
                active_desired=None,
                reason_code="active_desired_state_unavailable",
                now=now,
            )
        if desired.state_sha256 != lease.desired_state_sha256:
            return self._reject_obsolete(
                lease,
                active_desired=desired,
                reason_code="desired_state_superseded_before_processing",
                now=now,
            )
        binding = validate_raw_manifest_audit_lease(lease, desired_state=desired)
        existing = self.runner.output_store.load(lease.event_id)
        recovered = existing is not None
        if existing is None:
            if before_child is not None:
                before_child()
            status, reason, receipt = self.runner.execute(lease, desired, cancel)
            if receipt is None:
                return RawManifestAuditRunResult(
                    status, reason, lease.event_id, phase_child_invocations=1
                )
            initial_children = 1
        else:
            receipt = self.runner.output_store.verify(
                lease,
                binding,
                self.runner.authority_sha256,
                self.runner.limits.fingerprint,
            )
            initial_children = 0
        self._call_fault("consumer.after_output_commit")
        if cancel.is_set():
            raise RawManifestAuditCancelled("cancelled after durable audit")
        now = _timestamp(self._clock(), "post-audit clock")
        if lease.lease_expires_at - now < self.runner.final_reproof_seconds:
            raise RawManifestAuditDeadlineError("lease cannot contain final raw reproof")
        reloaded = self.lifecycle.load_leased(lease.event_id, lease.lease_token)
        if reloaded != lease:
            raise RawManifestAuditConflict("lifecycle lease changed before ack")
        active_after = self.desired.load_active()
        if active_after is None or active_after.state_sha256 != desired.state_sha256:
            return self._reject_obsolete(
                reloaded,
                active_desired=active_after,
                reason_code="desired_state_changed_after_processing",
                now=now,
                phase_child_invocations=1,
            )
        durable = self.runner.output_store.verify(
            lease, binding, self.runner.authority_sha256, self.runner.limits.fingerprint
        )
        if receipt != durable:
            raise RawManifestAuditIntegrityError("durable audit changed after processing")
        self.runner.output_store.assert_instance_held(output_store_identity)
        return RawManifestAuditPendingAck.create(
            lease=lease,
            desired_state=desired,
            binding=binding,
            output_receipt=durable,
            recovered_output=recovered,
            initial_child_invocations=initial_children,
            output_store_identity_sha256=output_store_identity,
        )

    def finalize_leased(
        self,
        pending: RawManifestAuditPendingAck,
        cancel_event: threading.Event | None = None,
        *,
        before_child: Callable[[], None] | None = None,
    ) -> RawManifestAuditRunResult:
        """Run one final-reproof child and acknowledge the held lease."""

        if not isinstance(pending, RawManifestAuditPendingAck):
            raise TypeError("pending must be RawManifestAuditPendingAck")
        pending.validate()
        cancel = cancel_event or threading.Event()
        if not callable(getattr(cancel, "is_set", None)):
            raise TypeError("cancel_event must expose is_set()")
        if before_child is not None and not callable(before_child):
            raise TypeError("before_child must be callable")
        lease = pending.lease
        desired = pending.desired_state
        binding = pending.binding
        if not hmac.compare_digest(
            pending.output_store_identity_sha256,
            self.runner.output_store_identity_sha256,
        ):
            raise RawManifestAuditIntegrityError(
                "pending audit output store authority differs"
            )
        self.runner.output_store.assert_instance_held(
            pending.output_store_identity_sha256
        )
        durable = self.runner.output_store.verify(
            lease,
            binding,
            self.runner.authority_sha256,
            self.runner.limits.fingerprint,
        )
        if durable != pending.output_receipt:
            raise RawManifestAuditIntegrityError(
                "durable audit changed before final reproof"
            )
        self._call_fault("consumer.before_final_reproof")
        now = _timestamp(self._clock(), "consumer final reproof clock")
        if cancel.is_set():
            raise RawManifestAuditCancelled("cancelled before final raw reproof")
        if lease.lease_expires_at - now < self.runner.final_reproof_seconds:
            raise RawManifestAuditDeadlineError("lease expired before final raw reproof")
        final_lease = self.lifecycle.load_leased(lease.event_id, lease.lease_token)
        if final_lease != lease:
            raise RawManifestAuditConflict("lifecycle lease changed at final reproof boundary")
        active_before_reproof = self.desired.load_active()
        if (
            active_before_reproof is None
            or active_before_reproof.state_sha256 != desired.state_sha256
        ):
            return self._reject_obsolete(
                final_lease,
                active_desired=active_before_reproof,
                reason_code="desired_state_changed_before_ack",
                now=now,
            )
        if before_child is not None:
            before_child()
        final_status, final_reason, final_durable = self.runner.reprove_for_ack(
            final_lease,
            desired,
            cancel,
        )
        if final_durable is None:
            return RawManifestAuditRunResult(
                final_status,
                final_reason,
                lease.event_id,
                phase_child_invocations=1,
            )
        if final_durable != durable:
            raise RawManifestAuditIntegrityError("durable audit changed during final reproof")
        with self.runner.output_store.completion_reproof_lease(
            lease=lease,
            binding=binding,
            authority_sha=self.runner.authority_sha256,
            limits_sha=self.runner.limits.fingerprint,
            expected_identity_sha256=pending.output_store_identity_sha256,
        ) as final_verified:
            if final_verified != final_durable:
                raise RawManifestAuditIntegrityError(
                    "fenced durable audit differs at ack boundary"
                )
            self._call_fault("consumer.after_completion_reproof_before_ack")
            self.runner.output_store._verify_path()
            if cancel.is_set():
                raise RawManifestAuditCancelled("cancelled at lifecycle ack boundary")
            now = _timestamp(self._clock(), "consumer final ack clock")
            if now >= lease.lease_expires_at:
                raise RawManifestAuditDeadlineError("lease expired at lifecycle ack boundary")
            ack_lease = self.lifecycle.load_leased(lease.event_id, lease.lease_token)
            if ack_lease != lease:
                raise RawManifestAuditConflict("lifecycle lease changed at ack boundary")
            active_before_ack = self.desired.load_active()
            if (
                active_before_ack is None
                or active_before_ack.state_sha256 != desired.state_sha256
            ):
                return self._reject_obsolete(
                    ack_lease,
                    active_desired=active_before_ack,
                    reason_code="desired_state_changed_before_ack",
                    now=now,
                    phase_child_invocations=1,
                )
            consumed = self.lifecycle.acknowledge(
                event_id=ack_lease.event_id,
                lease_token=ack_lease.lease_token,
                output_sha256=final_verified.output_sha256,
                now=now,
            )
            self._validate_consumption_receipt(
                consumed,
                ack_lease,
                expected_output_sha256=final_verified.output_sha256,
            )
            self._call_fault(
                "consumer.after_lifecycle_ack_before_output_reproof_release"
            )
            return RawManifestAuditRunResult(
                "acknowledged",
                "durable_raw_manifest_audit_acknowledged",
                lease.event_id,
                final_verified.output_sha256,
                final_verified.receipt_sha256,
                final_verified.process_receipt_sha256,
                consumed.receipt_sha256,
                1,
            )

    def process_leased(
        self, lease: CompletionLease, cancel_event: threading.Event | None = None
    ) -> RawManifestAuditRunResult:
        """Backward-compatible standalone composition of both process phases."""

        cancellation = cancel_event or threading.Event()
        begun = self.begin_leased(lease, cancellation)
        if isinstance(begun, RawManifestAuditRunResult):
            return begun
        return self.finalize_leased(begun, cancellation)

    def run_once(
        self,
        workload_id: str,
        *,
        owner_id: str,
        lease_seconds: float | None = None,
        cancel_event: threading.Event | None = None,
    ) -> RawManifestAuditRunResult:
        binding = AUDIT_CONSUMER_CATALOG.get(workload_id)
        if binding is None:
            raise RawManifestAuditIntegrityError("workload is not an audit route")
        duration = (
            self.runner.minimum_lease_seconds if lease_seconds is None else float(lease_seconds)
        )
        if not self.runner.minimum_lease_seconds <= duration <= 3600:
            raise RawManifestAuditDeadlineError("lease duration invalid")
        lease = self.lifecycle.claim_next(
            binding.consumer_id,
            binding.stream_id,
            _identifier(owner_id, "owner"),
            lease_seconds=duration,
            now=_timestamp(self._clock(), "claim clock"),
        )
        if lease is None:
            return RawManifestAuditRunResult("idle", "no_ready_audit")
        return self.process_leased(lease, cancel_event)


__all__ = [
    "AUDIT_CONSUMER_CATALOG",
    "LEGACY_OUTPUT_STORE_CONTRACT",
    "LEGACY_SCHEMA_VERSION",
    "OUTPUT_STORE_CONTRACT",
    "OUTPUT_STORE_IDENTITY_SCHEMA",
    "SCHEMA_VERSION",
    "RawManifestAuditCompletionConsumer",
    "RawManifestAuditIsolatedRunner",
    "RawManifestAuditOutputLimits",
    "RawManifestAuditOutputStore",
    "RawManifestAuditPendingAck",
    "RawManifestAuditProcessLimits",
    "RawManifestAuditIntegrityError",
    "RawManifestAuditConflict",
    "RawManifestAuditCapacityError",
    "RawManifestAuditDeadlineError",
    "RawManifestAuditCancelled",
    "validate_audit_consumer_catalog",
    "validate_raw_manifest_audit_lease",
]
