"""Inert, crash-durable Met Office UK RATE completion consumer.

This module consumes only the manifest-reference events emitted for
``radar.metoffice.uk_rate`` by :mod:`terminal_raw_completion_router`.  It re-proves
the desired-state/job/capture authorities, invokes the existing quantitative
decoder in one fresh hard-bounded process, and commits an immutable output and
process receipt before the lifecycle event can be acknowledged.

It is intentionally not imported by the worker service, Home Assistant, MQTT,
snapshot publication, fact staging, or meteorological admission code.
"""
from __future__ import annotations

from contextlib import closing, contextmanager
from dataclasses import asdict, dataclass
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
from typing import Any, Callable, Iterator, Mapping
from urllib.parse import quote

from .acquisition_activation import ACTIVATION_RULES, ActivationRule, ActivationStatus
from .acquisition_capture_recovery import (
    RECOVERY_BINDINGS,
    CaptureRecoveryError,
    RawRecoveryBinding,
    _validate_capture as validate_recovery_capture,
    _validate_receipt as validate_recovery_receipt,
    recovery_binding_sha256,
    recovery_evidence_digest,
)
from .acquisition_evidence_lifecycle import (
    COMPLETION_EVENT_SCHEMA,
    AcquisitionEvidenceLifecycleStore,
    CompletionLease,
    ConsumptionReceipt,
    desired_authority_proof,
)
from .acquisition_execution_store import CONTRACT as EXECUTION_CONTRACT
from .acquisition_execution_store import ExecutionReceipt
from .acquisition_scheduler_bridge import (
    JOB_TYPE,
    build_acquisition_schedule_intents,
    expected_autoscheduler_job_id,
    reconstruct_acquisition_job_for_slot,
)
from .autonomous_scheduler import SchedulerTerminalProof
from .canonical_evidence_store import CanonicalEvidenceError, CanonicalEvidenceStore
from .desired_state import DesiredState, DesiredStateStore, parse_desired_state
from .hard_decoder_isolation import (
    DecoderEffectContract,
    DecoderProcessResult,
    DecoderResourceLimits,
    HardDecoderExecutor,
)
from .met_office_rate_quantitative import (
    CAPTURE_PRODUCT,
    DECODER_CONFIG_SHA256,
    DECODER_IDENTITY,
    DECODER_VERSION,
    PROVIDER,
    MetOfficeRateArtifactError,
    MetOfficeRateArtifactRef,
    MetOfficeRateDecodeDeadlineError,
    MetOfficeRateDerivedArtifactStore,
    MetOfficeRateRawAuthority,
    decode_met_office_rate_capture,
    open_met_office_rate_evidence_store,
)
from .raw_artifacts import (
    RAW_ARTIFACT_CAPTURE_RECEIPT_SCHEMA,
    RAW_ARTIFACT_CAPTURE_SCHEMA,
    RAW_ARTIFACT_SCHEMA_VERSION,
    RawArtifactLimits,
    RawArtifactError,
    RawArtifactStore,
)
from .terminal_raw_completion_router import (
    REFERENCE_PAYLOAD_SCHEMA,
    REFERENCE_PROOF_SCHEMA,
    REFERENCE_TRANSFER_MODE,
    TERMINAL_RAW_CONSUMER_CATALOG,
)
from .worker_acquisition_plan import (
    AcquisitionJob,
    build_worker_acquisition_plan,
    validate_worker_acquisition_job,
    validate_worker_acquisition_plan,
)


WORKLOAD_ID = "radar.metoffice.uk_rate"
STREAM_ID = WORKLOAD_ID
CONSUMER_ID = "metoffice-rate-quantitative-decoder-reference-v1"
IMPLEMENTATION_ID = "met_office_rate_quantitative_v1"
EXPECTED_ROLES = ("listing_current", "listing_previous", "odim_hdf5")

OUTPUT_STORE_CONTRACT = "stv3-met-office-rate-completion-output-store-v2"
LEGACY_OUTPUT_STORE_CONTRACT = "stv3-met-office-rate-completion-output-store-v1"
OUTPUT_DOCUMENT_SCHEMA = "stv3-met-office-rate-completion-output-v1"
OUTPUT_RECEIPT_SCHEMA = "stv3-met-office-rate-completion-output-receipt-v1"
PROCESS_RECEIPT_SCHEMA = "stv3-met-office-rate-hard-process-receipt-v1"
PROCESS_AUTHORITY_SCHEMA = "stv3-met-office-rate-hard-process-authority-v1"
PROCESS_REQUEST_SCHEMA = "stv3-met-office-rate-hard-process-request-v2"
PROCESS_CHILD_RESULT_SCHEMA = "stv3-met-office-rate-hard-process-child-result-v1"
PROCESS_TARGET = (
    "storm_tracker_worker.met_office_rate_completion_consumer:"
    "_met_office_rate_completion_child"
)
OUTPUT_MODE = "shadow_inert"
OUTPUT_STORE_IDENTITY_SCHEMA = "stv3-met-office-rate-output-store-identity-v1"
SCHEMA_VERSION = 2
LEGACY_SCHEMA_VERSION = 1
MAX_DIAGNOSTIC_BYTES = 4096

PROCESS_EFFECT_CONTRACT = DecoderEffectContract(
    contract_id="met-office-rate-completion.idempotent-receipts.v1",
    hard_kill_safe=True,
    durable_effects="idempotent",
    spawns_descendants=False,
)

_SHA = re.compile(r"^[0-9a-f]{64}$")
_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,191}$")
_EVENT_ID = re.compile(r"^completion-[0-9a-f]{64}$")
_SECRET_KEY = re.compile(
    r"(?:authorization|api[_-]?key|access[_-]?token|refresh[_-]?token|"
    r"password|passwd|credential|private[_-]?key)",
    re.IGNORECASE,
)
_PRIVATE_TEXT = re.compile(
    r"(?:^[/\\]|[A-Za-z]:[\\/]|(?:https?|ftp)://|(?:bearer|basic)\s+|"
    r"private_[A-Za-z0-9_-]{8,}|(?:password|passwd|secret|token)=)",
    re.IGNORECASE,
)


class MetOfficeRateCompletionError(RuntimeError):
    """Base error for the inert Met Office completion boundary."""


class MetOfficeRateCompletionIntegrityError(MetOfficeRateCompletionError):
    """Durable authorities, bytes, or checksums differ."""


class MetOfficeRateCompletionConflict(MetOfficeRateCompletionError):
    """An immutable identity was reused for a different result."""


class MetOfficeRateCompletionCapacityError(MetOfficeRateCompletionError):
    """A hard receipt, database, process, or payload bound was reached."""


class MetOfficeRateCompletionDeadlineError(MetOfficeRateCompletionError):
    """The lifecycle lease cannot contain the hard process deadline."""


class MetOfficeRateCompletionCancelled(MetOfficeRateCompletionError):
    """Cancellation prevented the lifecycle acknowledgement."""


def _canonical(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
    except (TypeError, ValueError, UnicodeError) as exc:
        raise MetOfficeRateCompletionIntegrityError("value is not canonical JSON") from exc


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _canonical_utf8(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError) as exc:
        raise MetOfficeRateCompletionIntegrityError("value is not canonical UTF-8 JSON") from exc


def _sha(value: object, label: str) -> str:
    if not isinstance(value, str) or _SHA.fullmatch(value) is None:
        raise MetOfficeRateCompletionIntegrityError(f"{label} is not SHA-256")
    return value


def _identifier(value: object, label: str) -> str:
    if not isinstance(value, str) or _ID.fullmatch(value) is None:
        raise MetOfficeRateCompletionIntegrityError(f"{label} is invalid")
    return value


def _integer(value: object, label: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise MetOfficeRateCompletionIntegrityError(f"{label} is invalid")
    return value


def _timestamp(value: object, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise MetOfficeRateCompletionIntegrityError(f"{label} is invalid")
    result = float(value)
    if not math.isfinite(result) or not 0.0 <= result <= 2**53:
        raise MetOfficeRateCompletionIntegrityError(f"{label} is invalid")
    return result


def _exact(value: object, keys: set[str], label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or set(value) != keys:
        raise MetOfficeRateCompletionIntegrityError(f"{label} schema differs")
    if any(not isinstance(key, str) for key in value):
        raise MetOfficeRateCompletionIntegrityError(f"{label} has a non-text key")
    return value


def _reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise MetOfficeRateCompletionIntegrityError("reference JSON has duplicate keys")
        result[key] = value
    return result


def _decode_reference(payload: bytes) -> dict[str, Any]:
    if not isinstance(payload, bytes) or not payload or len(payload) > 64 * 1024:
        raise MetOfficeRateCompletionCapacityError("reference payload exceeds its cap")
    try:
        value = json.loads(
            payload.decode("ascii"),
            object_pairs_hook=_reject_duplicates,
            parse_constant=lambda token: (_ for _ in ()).throw(
                MetOfficeRateCompletionIntegrityError(
                    f"reference JSON has non-finite number {token}"
                )
            ),
        )
    except MetOfficeRateCompletionError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MetOfficeRateCompletionIntegrityError("reference payload is invalid JSON") from exc
    if not isinstance(value, dict) or _canonical(value) != payload:
        raise MetOfficeRateCompletionIntegrityError("reference payload is not canonical")
    return value


def _assert_public_document(value: Any, *, depth: int = 0) -> None:
    if depth > 20:
        raise MetOfficeRateCompletionCapacityError("document nesting exceeds cap")
    if isinstance(value, Mapping):
        if len(value) > 256:
            raise MetOfficeRateCompletionCapacityError("document object exceeds cap")
        for key, item in value.items():
            if not isinstance(key, str) or _SECRET_KEY.search(key):
                raise MetOfficeRateCompletionIntegrityError(
                    "document contains a credential-like key"
                )
            _assert_public_document(item, depth=depth + 1)
        return
    if isinstance(value, (list, tuple)):
        if len(value) > 256:
            raise MetOfficeRateCompletionCapacityError("document sequence exceeds cap")
        for item in value:
            _assert_public_document(item, depth=depth + 1)
        return
    if isinstance(value, str):
        if len(value.encode("utf-8")) > 4096 or _PRIVATE_TEXT.search(value):
            raise MetOfficeRateCompletionIntegrityError(
                "document contains private capability text"
            )
        return
    if value is None or isinstance(value, (bool, int, float)):
        if isinstance(value, float) and not math.isfinite(value):
            raise MetOfficeRateCompletionIntegrityError("document has non-finite number")
        return
    raise MetOfficeRateCompletionIntegrityError("document has unsupported value")


def _effect_mapping() -> dict[str, Any]:
    return {
        "contract_id": PROCESS_EFFECT_CONTRACT.contract_id,
        "durable_effects": PROCESS_EFFECT_CONTRACT.durable_effects,
        "hard_kill_safe": PROCESS_EFFECT_CONTRACT.hard_kill_safe,
        "spawns_descendants": PROCESS_EFFECT_CONTRACT.spawns_descendants,
    }


PROCESS_EFFECT_CONTRACT_SHA256 = _digest(_effect_mapping())


@dataclass(frozen=True, slots=True)
class MetOfficeRateCompletionOutputLimits:
    max_receipts: int = 4096
    max_receipt_bytes: int = 64 * 1024
    max_total_output_bytes: int = 64 * 1024 * 1024
    max_database_bytes: int = 128 * 1024 * 1024

    def __post_init__(self) -> None:
        values = (
            ("max_receipts", self.max_receipts, 1, 1_000_000),
            ("max_receipt_bytes", self.max_receipt_bytes, 1024, 1024 * 1024),
            (
                "max_total_output_bytes",
                self.max_total_output_bytes,
                self.max_receipt_bytes,
                4 * 1024**3,
            ),
            ("max_database_bytes", self.max_database_bytes, 1024 * 1024, 4 * 1024**3),
        )
        for label, value, minimum, maximum in values:
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or not minimum <= value <= maximum
            ):
                raise ValueError(f"{label} is outside its hard boundary")

    @property
    def fingerprint(self) -> str:
        return _digest({
            **asdict(self),
            "schema": "stv3-met-office-rate-completion-output-limits-v1",
        })


@dataclass(frozen=True, slots=True)
class MetOfficeRateCompletionProcessLimits:
    wall_seconds: float = 105.0
    cpu_seconds: float = 90.0
    rss_bytes: int = 1536 * 1024 * 1024
    max_request_bytes: int = 512 * 1024
    max_result_bytes: int = 16 * 1024

    def __post_init__(self) -> None:
        DecoderResourceLimits(
            wall_seconds=self.wall_seconds,
            cpu_seconds=self.cpu_seconds,
            memory_bytes=self.rss_bytes,
            max_request_bytes=self.max_request_bytes,
            max_result_bytes=self.max_result_bytes,
        )

    def to_mapping(self) -> dict[str, int | float]:
        return {
            "cpu_seconds": float(self.cpu_seconds),
            "max_request_bytes": self.max_request_bytes,
            "max_result_bytes": self.max_result_bytes,
            "rss_bytes": self.rss_bytes,
            "wall_seconds": float(self.wall_seconds),
        }

    @property
    def fingerprint(self) -> str:
        return _digest({
            **self.to_mapping(),
            "effect_contract_sha256": PROCESS_EFFECT_CONTRACT_SHA256,
            "schema": "stv3-met-office-rate-hard-process-limits-v1",
        })

    def decoder_limits(self) -> DecoderResourceLimits:
        return DecoderResourceLimits(
            wall_seconds=float(self.wall_seconds),
            cpu_seconds=float(self.cpu_seconds),
            memory_bytes=self.rss_bytes,
            max_request_bytes=self.max_request_bytes,
            max_result_bytes=self.max_result_bytes,
        )


@dataclass(frozen=True, slots=True)
class MetOfficeRateCompletionBinding:
    acquisition_job_id: str
    capture_id: str
    capture_manifest_sha256: str
    capture_receipt_sha256: str
    role_set_content_sha256: str
    role_set_receipt_sha256: str
    evaluated_at: float
    decoder_job_id: str
    decoder_attempt_id: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "acquisition_job_id": self.acquisition_job_id,
            "capture_id": self.capture_id,
            "capture_manifest_sha256": self.capture_manifest_sha256,
            "capture_receipt_sha256": self.capture_receipt_sha256,
            "decoder_attempt_id": self.decoder_attempt_id,
            "decoder_job_id": self.decoder_job_id,
            "evaluated_at": self.evaluated_at,
            "role_set_content_sha256": self.role_set_content_sha256,
            "role_set_receipt_sha256": self.role_set_receipt_sha256,
        }


@dataclass(frozen=True, slots=True)
class MetOfficeRateCompletionOutputReceipt:
    receipt_sha256: str
    process_receipt_sha256: str
    output_sha256: str
    event_id: str
    event_sha256: str
    producer_receipt_sha256: str
    desired_state_sha256: str
    authority_sha256: str
    capture_id: str
    capture_manifest_sha256: str
    decoder_run_id: str
    evidence_receipt_sha256: str
    artifact_sha256: str | None
    isolation_authority_sha256: str
    isolation_limits_sha256: str
    created_at: float
    output: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class MetOfficeRateCompletionProcessResult:
    status: str
    reason_code: str
    execution_outcome: str
    output_receipt: MetOfficeRateCompletionOutputReceipt | None
    isolation_enforced: bool
    elapsed_seconds: float
    child_exit_code: int | None = None
    child_cpu_seconds: float | None = None
    child_peak_rss_bytes: int | None = None
    child_peak_boundary_bytes: int | None = None


@dataclass(frozen=True, slots=True)
class MetOfficeRateCompletionRunResult:
    status: str
    reason_code: str
    event_id: str | None = None
    output_sha256: str | None = None
    output_receipt_sha256: str | None = None
    process_receipt_sha256: str | None = None
    consumption_receipt_sha256: str | None = None


def _catalog_binding():
    binding = TERMINAL_RAW_CONSUMER_CATALOG.get(WORKLOAD_ID)
    if (
        binding is None
        or binding.workload_id != WORKLOAD_ID
        or binding.stream_id != STREAM_ID
        or binding.consumer_id != CONSUMER_ID
        or binding.consumer_kind != "decoder_reference"
        or binding.implementation_id != IMPLEMENTATION_ID
        or binding.transfer_mode != REFERENCE_TRANSFER_MODE
        or binding.as_dict().get("meteorological_admission") is not False
        or binding.as_dict().get("runtime_wiring") != "inert"
    ):
        raise MetOfficeRateCompletionIntegrityError("Met Office consumer catalog binding drifted")
    return binding


def _met_office_recovery_binding(
    job: AcquisitionJob,
) -> tuple[ActivationRule, RawRecoveryBinding]:
    rule = ACTIVATION_RULES.get(WORKLOAD_ID)
    if (
        not isinstance(rule, ActivationRule)
        or rule.status is not ActivationStatus.ACTIVE_SHADOW_RAW
        or not rule.executable
        or rule.workload_id != WORKLOAD_ID
        or rule.provider_id != job.provider_id
        or rule.product_id != job.product_id
        or rule.required_roles != EXPECTED_ROLES
    ):
        raise MetOfficeRateCompletionIntegrityError("Met Office activation binding drifted")
    recovery = RECOVERY_BINDINGS.get(str(rule.handler_id))
    if (
        not isinstance(recovery, RawRecoveryBinding)
        or recovery.workload_id != WORKLOAD_ID
        or recovery.required_roles != EXPECTED_ROLES
        or recovery.job_identity_kind != "origin_v1"
        or recovery.ledger_provider != job.provider_id
        or recovery.maximum_transport_bytes != rule.maximum_transport_bytes
        or recovery.maximum_artifacts != rule.maximum_artifacts
        or job.budget.max_transport_bytes != rule.maximum_transport_bytes
        or job.budget.max_artifacts != rule.maximum_artifacts
        or job.budget.max_wall_seconds != rule.maximum_wall_seconds
    ):
        raise MetOfficeRateCompletionIntegrityError("Met Office raw recovery binding drifted")
    return rule, recovery


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


def _validate_scheduler_proof(value: object) -> SchedulerTerminalProof:
    keys = set(SchedulerTerminalProof.__dataclass_fields__) | {"proof_sha256"}
    mapping = _exact(value, keys, "scheduler terminal")
    proof_sha = _sha(mapping["proof_sha256"], "scheduler proof SHA-256")
    body = {key: mapping[key] for key in mapping if key != "proof_sha256"}
    try:
        proof = SchedulerTerminalProof(**body)
    except (TypeError, ValueError) as exc:
        raise MetOfficeRateCompletionIntegrityError("scheduler proof is invalid") from exc
    if proof.as_dict() != body or not hmac.compare_digest(proof.proof_sha256, proof_sha):
        raise MetOfficeRateCompletionIntegrityError("scheduler proof checksum differs")
    if (
        proof.job_type != JOB_TYPE
        or proof.state != "completed"
        or proof.last_transition != "completed"
        or proof.last_error_code is not None
        or proof.attempt_count < 1
        or proof.terminal_at < proof.scheduled_for
    ):
        raise MetOfficeRateCompletionIntegrityError("scheduler terminal is not completed")
    return proof


def _validate_execution(value: object) -> ExecutionReceipt:
    expected_keys = set(ExecutionReceipt.__dataclass_fields__) | {"contract"}
    mapping = _exact(value, expected_keys, "execution receipt")
    if mapping["contract"] != EXECUTION_CONTRACT:
        raise MetOfficeRateCompletionIntegrityError("execution contract differs")
    body = {key: mapping[key] for key in ExecutionReceipt.__dataclass_fields__}
    try:
        receipt = ExecutionReceipt(**body)
    except (TypeError, ValueError) as exc:
        raise MetOfficeRateCompletionIntegrityError("execution receipt is invalid") from exc
    unhashed = receipt.as_dict()
    unhashed.pop("receipt_sha256")
    if (
        receipt.as_dict() != dict(mapping)
        or not hmac.compare_digest(_digest(unhashed), receipt.receipt_sha256)
        or not receipt.terminal
        or receipt.outcome != "completed_shadow_raw"
        or receipt.reason_code != "shadow_raw_capture_complete"
    ):
        raise MetOfficeRateCompletionIntegrityError("execution receipt checksum differs")
    return receipt


def _event_identity(lease: CompletionLease) -> None:
    expected_event = _digest({
        "consumer_id": lease.consumer_id,
        "producer_receipt_sha256": lease.producer_receipt_sha256,
        "schema": COMPLETION_EVENT_SCHEMA,
        "stream_id": lease.stream_id,
        "stream_sequence": lease.stream_sequence,
    })
    expected_id = "completion-" + hashlib.sha256(
        (COMPLETION_EVENT_SCHEMA + lease.producer_receipt_sha256).encode("ascii")
    ).hexdigest()
    if (
        _EVENT_ID.fullmatch(lease.event_id) is None
        or not hmac.compare_digest(expected_id, lease.event_id)
        or not hmac.compare_digest(expected_event, lease.event_sha256)
    ):
        raise MetOfficeRateCompletionIntegrityError("completion event identity differs")


def _stable_decoder_ids(lease: CompletionLease, capture_id: str) -> tuple[str, str]:
    seed = {
        "capture_id": capture_id,
        "consumer_id": CONSUMER_ID,
        "decoder_config_sha256": DECODER_CONFIG_SHA256,
        "event_id": lease.event_id,
        "event_sha256": lease.event_sha256,
        "producer_receipt_sha256": lease.producer_receipt_sha256,
        "schema": "stv3-met-office-rate-decoder-identity-v1",
    }
    job = "decoder-job-" + hashlib.sha256(_canonical(seed)).hexdigest()
    attempt = "decoder-attempt-" + hashlib.sha256(
        _canonical({**seed, "schema": "stv3-met-office-rate-decoder-attempt-v1"})
    ).hexdigest()
    return job, attempt


def validate_met_office_rate_completion_lease(
    lease: CompletionLease,
    *,
    desired_state: DesiredState,
    raw_store: RawArtifactStore,
) -> MetOfficeRateCompletionBinding:
    """Reprove one router reference against desired/job/raw authorities."""

    if not isinstance(lease, CompletionLease):
        raise TypeError("lease must be CompletionLease")
    if not isinstance(desired_state, DesiredState):
        raise TypeError("desired_state must be DesiredState")
    if not isinstance(raw_store, RawArtifactStore):
        raise TypeError("raw_store must be RawArtifactStore")
    catalog = _catalog_binding()
    if (
        lease.consumer_id != CONSUMER_ID
        or lease.stream_id != STREAM_ID
        or lease.acquisition_job_id == ""
        or lease.stream_sequence < 1
        or lease.attempt_count < 1
        or lease.size_bytes != len(lease.payload)
        or not hmac.compare_digest(
            hashlib.sha256(lease.payload).hexdigest(), lease.content_sha256
        )
    ):
        raise MetOfficeRateCompletionIntegrityError("completion lease route differs")
    _event_identity(lease)
    if (
        desired_state.state_sha256 != lease.desired_state_sha256
        or desired_state.state_sha256 != _sha(
            lease.desired_state_sha256, "lease desired SHA-256"
        )
    ):
        raise MetOfficeRateCompletionConflict("active desired state differs from completion")
    authority = desired_authority_proof(desired_state)
    if (
        authority.get("authority_sha256") != lease.authority_sha256
        or _digest({key: authority[key] for key in authority if key != "authority_sha256"})
        != lease.authority_sha256
    ):
        raise MetOfficeRateCompletionIntegrityError("desired authority differs")

    payload = _exact(_decode_reference(lease.payload), _PAYLOAD_KEYS, "reference payload")
    proof = _exact(lease.proof, _PROOF_KEYS, "reference proof")
    if (
        payload["schema"] != REFERENCE_PAYLOAD_SCHEMA
        or proof["schema"] != REFERENCE_PROOF_SCHEMA
        or payload["workload_id"] != WORKLOAD_ID
        or payload["consumer_id"] != CONSUMER_ID
        or payload["stream_id"] != STREAM_ID
        or payload["acquisition_job_id"] != lease.acquisition_job_id
        or proof["reference_content_sha256"] != lease.content_sha256
        or proof["reference_content_sha256"] != hashlib.sha256(lease.payload).hexdigest()
        or proof["consumer_binding"] != catalog.as_dict()
        or payload["consumer_binding_sha256"] != catalog.fingerprint
        or proof["consumer_binding_sha256"] != catalog.fingerprint
        or proof["desired_authority"] != authority
    ):
        raise MetOfficeRateCompletionIntegrityError("reference route authority differs")
    claims = _exact(payload["claims"], _CLAIM_KEYS, "reference claims")
    if any(value is not False for value in claims.values()) or proof["claims"] != claims:
        raise MetOfficeRateCompletionIntegrityError("reference makes an operational claim")
    capture_ref = _exact(payload["capture_reference"], _CAPTURE_KEYS, "capture reference")
    if proof["capture_reference"] != capture_ref:
        raise MetOfficeRateCompletionIntegrityError("proof capture reference differs")

    scheduler = _validate_scheduler_proof(proof["scheduler_terminal"])
    if scheduler.desired_state_sha256 != desired_state.state_sha256:
        raise MetOfficeRateCompletionIntegrityError("scheduler desired authority differs")
    plan = validate_worker_acquisition_plan(build_worker_acquisition_plan(desired_state))
    try:
        job = validate_worker_acquisition_job(reconstruct_acquisition_job_for_slot(
            plan,
            intent_id=scheduler.intent_id,
            scheduled_for=scheduler.scheduled_for,
        ))
    except Exception as exc:
        raise MetOfficeRateCompletionIntegrityError("acquisition slot cannot be reconstructed") from exc
    schedule_matches = tuple(
        item
        for item in build_acquisition_schedule_intents(plan)
        if item.intent_id == scheduler.intent_id
    )
    if len(schedule_matches) != 1:
        raise MetOfficeRateCompletionIntegrityError("scheduler intent is not singular")
    schedule = schedule_matches[0]
    expected_scheduler_job = expected_autoscheduler_job_id(
        snapshot_sha256=desired_state.state_sha256,
        intent_id=schedule.intent_id,
        cadence_seconds=schedule.cadence_seconds,
        slot_index=scheduler.slot_index,
    )
    expected_slot = "slot:" + expected_scheduler_job.removeprefix("autosched:")
    payload_sha = hashlib.sha256(_canonical_utf8(dict(schedule.payload))).hexdigest()
    scheduler_record = {
        "cadence_seconds": schedule.cadence_seconds,
        "deadline_at": scheduler.deadline_at,
        "desired_state_sha256": desired_state.state_sha256,
        "intent_id": schedule.intent_id,
        "job_id": expected_scheduler_job,
        "job_type": schedule.job_type,
        "payload": dict(schedule.payload),
        "payload_sha256": payload_sha,
        "priority": schedule.priority,
        "scheduled_for": scheduler.scheduled_for,
        "slot_id": expected_slot,
        "slot_index": scheduler.slot_index,
    }
    if (
        scheduler.job_id != expected_scheduler_job
        or scheduler.slot_id != expected_slot
        or scheduler.payload_sha256 != payload_sha
        or scheduler.record_sha256
        != hashlib.sha256(_canonical_utf8(scheduler_record)).hexdigest()
        or scheduler.slot_index != job.slot_index
        or scheduler.scheduled_for != job.scheduled_for
        or scheduler.deadline_at != job.deadline_at
        or schedule.cadence_seconds != job.cadence_seconds
        or schedule.priority != job.priority
    ):
        raise MetOfficeRateCompletionIntegrityError("scheduler slot authority differs")
    workload = _exact(proof["workload"], {
        "job_id", "job_sha256", "plan_sha256", "product_id", "provider_id",
        "snapshot_sha256", "workload_id",
    }, "workload proof")
    expected_workload = {
        "job_id": job.job_id,
        "job_sha256": job.job_sha256,
        "plan_sha256": job.plan_sha256,
        "product_id": job.product_id,
        "provider_id": job.provider_id,
        "snapshot_sha256": job.snapshot_sha256,
        "workload_id": job.workload_id,
    }
    if (
        workload != expected_workload
        or job.job_id != lease.acquisition_job_id
        or job.workload_id != WORKLOAD_ID
        or job.snapshot_sha256 != desired_state.state_sha256
    ):
        raise MetOfficeRateCompletionIntegrityError("acquisition job binding differs")
    _rule, recovery = _met_office_recovery_binding(job)
    if proof["recovery_binding_sha256"] != recovery_binding_sha256(recovery):
        raise MetOfficeRateCompletionIntegrityError("recovery binding checksum differs")

    execution = _validate_execution(proof["execution_receipt"])
    if (
        execution.scheduler_job_id != scheduler.job_id
        or execution.acquisition_job_id != job.job_id
        or execution.attempt_count != scheduler.attempt_count
    ):
        raise MetOfficeRateCompletionIntegrityError("execution scheduler binding differs")

    if (
        capture_ref["capture_schema"] != RAW_ARTIFACT_CAPTURE_SCHEMA
        or capture_ref["capture_receipt_schema"] != RAW_ARTIFACT_CAPTURE_RECEIPT_SCHEMA
        or capture_ref["raw_store_schema_version"] != RAW_ARTIFACT_SCHEMA_VERSION
        or capture_ref["provider"] != recovery.raw_provider
        or capture_ref["product"] != recovery.capture_product
    ):
        raise MetOfficeRateCompletionIntegrityError("capture contract differs")
    capture_id = _identifier(capture_ref["capture_id"], "capture ID")
    expected_capture_id = RawArtifactStore._capture_id(  # noqa: SLF001
        provider=recovery.raw_provider,
        product=recovery.capture_product,
        fetch_id=f"{job.job_id}{recovery.capture_suffix}",
    )
    if capture_id != expected_capture_id:
        raise MetOfficeRateCompletionIntegrityError("capture ID changed job identity")
    try:
        loaded = raw_store.load_capture_bundle_bytes(capture_id)
    except RawArtifactError as exc:
        raise MetOfficeRateCompletionIntegrityError(
            "referenced raw capture failed verification"
        ) from exc
    if loaded is None:
        raise MetOfficeRateCompletionIntegrityError("referenced raw capture is unavailable")
    capture, receipts, payloads = loaded
    expected_roles = set(EXPECTED_ROLES)
    if set(receipts) != expected_roles or set(payloads) != expected_roles:
        raise MetOfficeRateCompletionIntegrityError("Met Office capture role set differs")
    members_value = capture_ref["members"]
    if (
        not isinstance(members_value, list)
        or len(members_value) != len(EXPECTED_ROLES)
    ):
        raise MetOfficeRateCompletionIntegrityError("capture member reference differs")
    expected_members: list[dict[str, Any]] = []
    metadata_by_role: dict[str, str | None] = {}
    received_at = 0.0
    try:
        for index, role in enumerate(sorted(EXPECTED_ROLES)):
            receipt = receipts[role]
            member = recovery.member(role)
            member_ref = _exact(
                members_value[index], _MEMBER_KEYS, f"capture member {role}"
            )
            expected_member = {
                "content_sha256": receipt.content_sha256,
                "content_type": receipt.content_type,
                "product": receipt.product,
                "provider": receipt.provider,
                "receipt_id": receipt.receipt_id,
                "receipt_sha256": receipt.receipt_sha256,
                "role": role,
                "size_bytes": receipt.size_bytes,
            }
            receipt_job_id, metadata = validate_recovery_receipt(
                receipt, recovery, member
            )
            if (
                member_ref != expected_member
                or receipt_job_id != job.job_id
                or receipt.fetch_id != f"{job.job_id}{member.receipt_suffix}"
                or hashlib.sha256(payloads[role]).hexdigest()
                != receipt.content_sha256
                or len(payloads[role]) != receipt.size_bytes
            ):
                raise MetOfficeRateCompletionIntegrityError(
                    "raw member authority differs"
                )
            metadata_by_role[role] = metadata
            expected_members.append(expected_member)
            received_at = max(received_at, receipt.received_at)
        capture_job_id = validate_recovery_capture(
            capture,
            binding=recovery,
            receipts_by_role=receipts,
            metadata_by_role=metadata_by_role,
        )
        if capture_job_id != job.job_id:
            raise MetOfficeRateCompletionIntegrityError(
                "raw capture changed acquisition job identity"
            )
    except CaptureRecoveryError as exc:
        raise MetOfficeRateCompletionIntegrityError(
            "Met Office listing/object provenance failed verification"
        ) from exc
    try:
        evidence = recovery_evidence_digest(
            binding=recovery, receipts_by_role=receipts, capture=capture
        )
    except CaptureRecoveryError as exc:
        raise MetOfficeRateCompletionIntegrityError(
            "referenced raw recovery evidence failed verification"
        ) from exc
    expected_capture = {
        "capture_id": capture.capture_id,
        "capture_receipt_schema": capture.capture_receipt_schema,
        "capture_receipt_sha256": capture.capture_receipt_sha256,
        "capture_schema": capture.capture_schema,
        "manifest_sha256": capture.manifest_sha256,
        "members": expected_members,
        "product": capture.product,
        "provider": capture.provider,
        "raw_store_schema_version": capture.raw_store_schema_version,
        "role_set_content_sha256": evidence.content_sha256,
        "role_set_receipt_sha256": evidence.receipt_sha256,
        "total_bytes": evidence.actual_bytes,
    }
    if capture_ref != expected_capture:
        raise MetOfficeRateCompletionIntegrityError("capture manifest reference differs")

    capacity = _exact(proof["capacity_commit"], {
        "actual_bytes", "actual_entries", "commit_sha256", "provider",
        "request_sha256", "reservation_sha256", "status",
    }, "capacity commit")
    for field in ("commit_sha256", "request_sha256", "reservation_sha256"):
        _sha(capacity[field], f"capacity {field}")
    if (
        capacity["status"] != "COMMITTED"
        or capacity["provider"] != recovery.ledger_provider
        or capacity["actual_bytes"] != evidence.actual_bytes
        or capacity["actual_entries"] != evidence.actual_entries
        or execution.content_sha256 != evidence.content_sha256
        or execution.raw_receipt_sha256 != evidence.receipt_sha256
        or execution.capture_sha256 != evidence.capture_sha256
        or execution.capacity_commit_sha256 != capacity["commit_sha256"]
    ):
        raise MetOfficeRateCompletionIntegrityError("capacity/execution/raw evidence differs")
    evaluated_at = _timestamp(execution.created_at, "decoder evaluation time")
    if evaluated_at < received_at:
        raise MetOfficeRateCompletionIntegrityError("decoder evaluation predates raw bytes")
    decoder_job_id, attempt_id = _stable_decoder_ids(lease, capture_id)
    return MetOfficeRateCompletionBinding(
        acquisition_job_id=job.job_id,
        capture_id=capture_id,
        capture_manifest_sha256=capture.manifest_sha256,
        capture_receipt_sha256=capture.capture_receipt_sha256,
        role_set_content_sha256=evidence.content_sha256,
        role_set_receipt_sha256=evidence.receipt_sha256,
        evaluated_at=evaluated_at,
        decoder_job_id=decoder_job_id,
        decoder_attempt_id=attempt_id,
    )


def _validated_output_document(value: object) -> tuple[
    dict[str, Any], Mapping[str, Any], Mapping[str, Any], Mapping[str, Any],
    Mapping[str, Any], Mapping[str, Any]
]:
    document = dict(_exact(value, {
        "authority", "claims", "consumer_id", "decoder", "disposition",
        "event", "fact_revision", "input", "isolation", "mode", "schema",
        "stream_id", "workload_id",
    }, "Met Office output"))
    if (
        document["schema"] != OUTPUT_DOCUMENT_SCHEMA
        or document["mode"] != OUTPUT_MODE
        or document["workload_id"] != WORKLOAD_ID
        or document["consumer_id"] != CONSUMER_ID
        or document["stream_id"] != STREAM_ID
        or document["fact_revision"] is not None
        or document["disposition"] not in {"shadow_candidate", "shadow_rejected"}
    ):
        raise MetOfficeRateCompletionIntegrityError("Met Office output route differs")
    event = _exact(document["event"], {
        "event_id", "event_sha256", "producer_receipt_sha256", "stream_sequence",
    }, "output event")
    if _EVENT_ID.fullmatch(_identifier(event["event_id"], "output event ID")) is None:
        raise MetOfficeRateCompletionIntegrityError("output event ID differs")
    _sha(event["event_sha256"], "output event SHA-256")
    _sha(event["producer_receipt_sha256"], "output producer SHA-256")
    _integer(event["stream_sequence"], "output stream sequence", minimum=1)
    authority = _exact(document["authority"], {
        "authority_sha256", "desired_state_sha256",
    }, "output authority")
    _sha(authority["authority_sha256"], "output authority SHA-256")
    _sha(authority["desired_state_sha256"], "output desired SHA-256")
    input_value = _exact(document["input"], {
        "acquisition_job_id", "capture_id", "capture_manifest_sha256",
        "capture_receipt_sha256", "reference_content_sha256",
        "role_set_content_sha256", "role_set_receipt_sha256",
    }, "output input")
    _identifier(input_value["acquisition_job_id"], "output acquisition job ID")
    _identifier(input_value["capture_id"], "output capture ID")
    for field in (
        "capture_manifest_sha256", "capture_receipt_sha256",
        "reference_content_sha256", "role_set_content_sha256",
        "role_set_receipt_sha256",
    ):
        _sha(input_value[field], f"output {field}")
    decoder = _exact(document["decoder"], {
        "artifact", "attempt_id", "config_sha256", "evidence", "identity",
        "job_id", "outcome", "reason_code", "version",
    }, "output decoder")
    outcome = decoder["outcome"]
    reason_code = decoder["reason_code"]
    if (
        decoder["identity"] != DECODER_IDENTITY
        or decoder["version"] != DECODER_VERSION
        or decoder["config_sha256"] != DECODER_CONFIG_SHA256
        or outcome not in {"decoded_complete", "decode_error", "unsupported", "incomplete"}
        or not isinstance(reason_code, str)
        or re.fullmatch(r"[a-z][a-z0-9_.:-]{0,127}", reason_code) is None
        or reason_code
        != (
            "decoded_complete"
            if outcome == "decoded_complete"
            else reason_code
        )
        or (
            outcome != "decoded_complete"
            and not reason_code.startswith(f"{outcome}:")
        )
        or not isinstance(decoder["job_id"], str)
        or re.fullmatch(r"decoder-job-[0-9a-f]{64}", decoder["job_id"]) is None
        or not isinstance(decoder["attempt_id"], str)
        or re.fullmatch(r"decoder-attempt-[0-9a-f]{64}", decoder["attempt_id"]) is None
    ):
        raise MetOfficeRateCompletionIntegrityError("output decoder binding differs")
    evidence = _exact(decoder["evidence"], {
        "capture_manifest_sha256", "commit_id", "receipt_sha256", "result_sha256",
        "revision_sha256", "run_id",
    }, "output evidence")
    for field in (
        "capture_manifest_sha256", "receipt_sha256", "result_sha256",
        "revision_sha256",
    ):
        _sha(evidence[field], f"output evidence {field}")
    if (
        not isinstance(evidence["run_id"], str)
        or re.fullmatch(r"run-[0-9a-f]{64}", evidence["run_id"]) is None
        or not isinstance(evidence["commit_id"], str)
        or re.fullmatch(r"commit-[0-9a-f]{64}", evidence["commit_id"]) is None
    ):
        raise MetOfficeRateCompletionIntegrityError("output evidence identity differs")
    artifact = decoder["artifact"]
    if artifact is not None:
        artifact = _exact(artifact, {"handle", "sha256", "size_bytes"}, "output artifact")
        if (
            not isinstance(artifact["handle"], str)
            or not artifact["handle"].endswith(_sha(artifact["sha256"], "artifact SHA-256"))
            or _integer(artifact["size_bytes"], "artifact bytes", minimum=1) > 64 * 1024 * 1024
        ):
            raise MetOfficeRateCompletionIntegrityError("output artifact differs")
    claims = _exact(document["claims"], {
        "decoded", "fact_staged", "meteorological_admission",
        "operational_snapshot_published",
    }, "output claims")
    if (
        type(claims["decoded"]) is not bool
        or claims["fact_staged"] is not False
        or claims["meteorological_admission"] is not False
        or claims["operational_snapshot_published"] is not False
        or claims["decoded"] != (decoder["outcome"] == "decoded_complete")
        or (artifact is None) != (decoder["outcome"] != "decoded_complete")
        or document["disposition"]
        != ("shadow_candidate" if claims["decoded"] else "shadow_rejected")
    ):
        raise MetOfficeRateCompletionIntegrityError("output claims differ")
    isolation = _exact(document["isolation"], {
        "authority_sha256", "effect_contract_sha256", "limits_sha256",
        "process_boundary", "runtime_wiring",
    }, "output isolation")
    if (
        isolation["effect_contract_sha256"] != PROCESS_EFFECT_CONTRACT_SHA256
        or isolation["process_boundary"] != "fresh_spawn_hard_cpu_rss_wall_output_caps"
        or isolation["runtime_wiring"] != "inert"
    ):
        raise MetOfficeRateCompletionIntegrityError("output isolation differs")
    _sha(isolation["authority_sha256"], "isolation authority SHA-256")
    _sha(isolation["limits_sha256"], "isolation limits SHA-256")
    _assert_public_document(document)
    return document, event, authority, input_value, decoder, isolation


class MetOfficeRateCompletionOutputStore:
    """Bounded append-only SQLite output/process receipt authority."""

    _TABLES = frozenset({"output_metadata", "completion_outputs"})
    def __init__(
        self,
        path: str | Path,
        *,
        limits: MetOfficeRateCompletionOutputLimits = MetOfficeRateCompletionOutputLimits(),
        clock: Callable[[], float] = time.time,
        fault_injector: Callable[[str], None] | None = None,
    ) -> None:
        self.path = Path(os.path.abspath(os.fspath(path)))
        if not self.path.is_absolute():
            raise ValueError("Met Office completion output path must be absolute")
        if not isinstance(limits, MetOfficeRateCompletionOutputLimits):
            raise TypeError("Met Office completion output limits have wrong type")
        if not callable(clock):
            raise TypeError("Met Office completion output clock must be callable")
        if fault_injector is not None and not callable(fault_injector):
            raise TypeError("Met Office completion fault injector must be callable")
        self.limits = limits
        self._clock = clock
        self._fault_injector = fault_injector
        self._lock = threading.RLock()
        self._database_witness: tuple[int, int, int]
        self._store_instance_sha256: str
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
            raise MetOfficeRateCompletionIntegrityError(
                "output store witness is unavailable"
            ) from exc
        if (
            path.is_symlink()
            or not stat.S_ISREG(details.st_mode)
            or int(details.st_nlink) != 1
        ):
            raise MetOfficeRateCompletionIntegrityError("output store path is unsafe")
        return int(details.st_dev), int(details.st_ino), int(details.st_mode)

    def _prepare_path(self) -> tuple[int, int, int]:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        parent = self.path.parent
        for candidate in (parent, *parent.parents):
            try:
                details = candidate.lstat()
            except OSError as exc:
                raise MetOfficeRateCompletionIntegrityError(
                    "output store parent is unavailable"
                ) from exc
            if candidate.is_symlink() or not stat.S_ISDIR(details.st_mode):
                raise MetOfficeRateCompletionIntegrityError("output store parent is unsafe")
        if self._normalized_path(parent.resolve(strict=True)) != self._normalized_path(parent):
            raise MetOfficeRateCompletionIntegrityError("output store parent is aliased")
        if self.path.is_symlink():
            raise MetOfficeRateCompletionIntegrityError("output store path is unsafe")
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
                        raise MetOfficeRateCompletionIntegrityError(
                            "output store leaf is not regular"
                        )
                    os.fsync(descriptor)
                finally:
                    os.close(descriptor)
            except MetOfficeRateCompletionError:
                raise
            except OSError as exc:
                raise MetOfficeRateCompletionIntegrityError(
                    "output store leaf creation failed"
                ) from exc
        witness = self._path_witness(self.path)
        if self._normalized_path(self.path.resolve(strict=True)) != self._normalized_path(
            self.path
        ):
            raise MetOfficeRateCompletionIntegrityError("output store path is aliased")
        return witness

    def _verify_path(self) -> None:
        try:
            resolved = self.path.resolve(strict=True)
        except OSError as exc:
            raise MetOfficeRateCompletionIntegrityError(
                "output store instance changed"
            ) from exc
        if (
            self._normalized_path(resolved) != self._normalized_path(self.path)
            or self._path_witness(self.path) != self._database_witness
        ):
            raise MetOfficeRateCompletionIntegrityError("output store instance changed")

    def _connect(self) -> sqlite3.Connection:
        self._verify_path()
        uri_path = quote(self.path.as_posix(), safe="/:")
        database = sqlite3.connect(
            f"file:{uri_path}?mode=rw&nofollow=1",
            timeout=5.0,
            isolation_level=None,
            uri=True,
        )
        database.row_factory = sqlite3.Row
        try:
            self._verify_path()
            database.execute("PRAGMA foreign_keys=ON")
            database.execute("PRAGMA synchronous=FULL")
            database.execute("PRAGMA trusted_schema=OFF")
            database.execute("PRAGMA busy_timeout=5000")
            mode = str(database.execute("PRAGMA journal_mode=DELETE").fetchone()[0]).lower()
            if mode != "delete":
                raise MetOfficeRateCompletionIntegrityError("output journal mode differs")
            page_size = int(database.execute("PRAGMA page_size").fetchone()[0])
            maximum_pages = max(1, self.limits.max_database_bytes // page_size)
            configured_pages = int(database.execute(
                f"PRAGMA max_page_count={maximum_pages}"
            ).fetchone()[0])
            current_pages = int(database.execute("PRAGMA page_count").fetchone()[0])
            if configured_pages != maximum_pages or current_pages > maximum_pages:
                raise MetOfficeRateCompletionCapacityError(
                    "output database exceeds its hard page cap"
                )
        except BaseException:
            database.close()
            raise
        return database

    @staticmethod
    def _tables(database: sqlite3.Connection) -> frozenset[str]:
        return frozenset(
            str(row[0])
            for row in database.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            )
        )

    @staticmethod
    def _columns(
        database: sqlite3.Connection, table: str
    ) -> tuple[tuple[str, str, int, int], ...]:
        return tuple(
            (str(row[1]), str(row[2]), int(row[3]), int(row[5]))
            for row in database.execute(f"PRAGMA table_info({table})")
        )

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
            ("desired_state_sha256", "TEXT", 1, 0),
            ("authority_sha256", "TEXT", 1, 0),
            ("consumer_id", "TEXT", 1, 0),
            ("stream_id", "TEXT", 1, 0),
            ("capture_id", "TEXT", 1, 0),
            ("capture_manifest_sha256", "TEXT", 1, 0),
            ("decoder_run_id", "TEXT", 1, 0),
            ("evidence_receipt_sha256", "TEXT", 1, 0),
            ("artifact_sha256", "TEXT", 0, 0),
            ("isolation_authority_sha256", "TEXT", 1, 0),
            ("isolation_effect_contract_sha256", "TEXT", 1, 0),
            ("isolation_limits_sha256", "TEXT", 1, 0),
            ("output_sha256", "TEXT", 1, 0),
            ("receipt_sha256", "TEXT", 1, 0),
            ("process_receipt_sha256", "TEXT", 1, 0),
            ("output_json", "BLOB", 1, 0),
            ("output_bytes", "INTEGER", 1, 0),
            ("created_at", "REAL", 1, 0),
        )

    @staticmethod
    def _normalized_sql(value: str) -> str:
        return " ".join(value.casefold().split())

    @classmethod
    def _trigger_definitions(cls) -> dict[str, str]:
        return {
            "completion_outputs_no_update": (
                "CREATE TRIGGER completion_outputs_no_update BEFORE UPDATE ON "
                "completion_outputs BEGIN SELECT RAISE(ABORT,'immutable'); END"
            ),
            "completion_outputs_no_delete": (
                "CREATE TRIGGER completion_outputs_no_delete BEFORE DELETE ON "
                "completion_outputs BEGIN SELECT RAISE(ABORT,'immutable'); END"
            ),
            "output_metadata_no_update": (
                "CREATE TRIGGER output_metadata_no_update BEFORE UPDATE ON "
                "output_metadata BEGIN SELECT RAISE(ABORT,'immutable'); END"
            ),
            "output_metadata_no_delete": (
                "CREATE TRIGGER output_metadata_no_delete BEFORE DELETE ON "
                "output_metadata BEGIN SELECT RAISE(ABORT,'immutable'); END"
            ),
        }

    @classmethod
    def _legacy_trigger_definitions(cls) -> dict[str, str]:
        return {
            name: statement
            for name, statement in cls._trigger_definitions().items()
            if name.startswith("completion_outputs_")
        }

    @classmethod
    def _install_triggers(
        cls, database: sqlite3.Connection, *, metadata_only: bool = False
    ) -> None:
        for name, statement in cls._trigger_definitions().items():
            if metadata_only and not name.startswith("output_metadata_"):
                continue
            database.execute(statement)

    @classmethod
    def _validate_triggers(
        cls, database: sqlite3.Connection, expected: Mapping[str, str]
    ) -> None:
        actual = {
            str(row[0]): str(row[1])
            for row in database.execute(
                "SELECT name,sql FROM sqlite_master WHERE type='trigger' ORDER BY name"
            )
        }
        if set(actual) != set(expected) or any(
            cls._normalized_sql(actual[name]) != cls._normalized_sql(statement)
            for name, statement in expected.items()
        ):
            raise MetOfficeRateCompletionIntegrityError(
                "output immutability triggers differ"
            )

    def _validate_unique_columns(self, database: sqlite3.Connection) -> None:
        unique_columns: set[str] = set()
        for index in database.execute("PRAGMA index_list(completion_outputs)"):
            if int(index[2]) != 1:
                continue
            members = tuple(
                str(row[0])
                for row in database.execute(
                    "SELECT name FROM pragma_index_info(?)", (str(index[1]),)
                )
            )
            if len(members) != 1:
                raise MetOfficeRateCompletionIntegrityError(
                    "output uniqueness arity differs"
                )
            unique_columns.add(members[0])
        if unique_columns != {
            "event_id",
            "producer_receipt_sha256",
            "output_sha256",
            "receipt_sha256",
            "process_receipt_sha256",
        }:
            raise MetOfficeRateCompletionIntegrityError("output uniqueness differs")

    def _create_schema(self, database: sqlite3.Connection) -> None:
        database.execute(
            """CREATE TABLE output_metadata(
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
        database.execute(
            """CREATE TABLE completion_outputs(
                event_id TEXT PRIMARY KEY,
                event_sha256 TEXT NOT NULL,
                producer_receipt_sha256 TEXT NOT NULL UNIQUE,
                desired_state_sha256 TEXT NOT NULL,
                authority_sha256 TEXT NOT NULL,
                consumer_id TEXT NOT NULL,
                stream_id TEXT NOT NULL,
                capture_id TEXT NOT NULL,
                capture_manifest_sha256 TEXT NOT NULL,
                decoder_run_id TEXT NOT NULL,
                evidence_receipt_sha256 TEXT NOT NULL,
                artifact_sha256 TEXT,
                isolation_authority_sha256 TEXT NOT NULL,
                isolation_effect_contract_sha256 TEXT NOT NULL,
                isolation_limits_sha256 TEXT NOT NULL,
                output_sha256 TEXT NOT NULL UNIQUE,
                receipt_sha256 TEXT NOT NULL UNIQUE,
                process_receipt_sha256 TEXT NOT NULL UNIQUE,
                output_json BLOB NOT NULL,
                output_bytes INTEGER NOT NULL,
                created_at REAL NOT NULL
            )"""
        )
        database.execute(
            "INSERT INTO output_metadata VALUES(1,?,?,?,?,?,?,?,?)",
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
        self._install_triggers(database)

    def _validate_metadata(
        self,
        database: sqlite3.Connection,
        *,
        contract: str,
        schema_version: int,
    ) -> sqlite3.Row:
        row = database.execute(
            "SELECT * FROM output_metadata WHERE singleton=1"
        ).fetchone()
        if (
            row is None
            or database.execute("SELECT count(*) FROM output_metadata").fetchone()[0] != 1
            or str(row["contract"]) != contract
            or int(row["schema_version"]) != schema_version
            or str(row["limits_sha256"]) != self.limits.fingerprint
            or int(row["max_receipts"]) != self.limits.max_receipts
            or int(row["max_receipt_bytes"]) != self.limits.max_receipt_bytes
            or int(row["max_total_output_bytes"]) != self.limits.max_total_output_bytes
            or int(row["max_database_bytes"]) != self.limits.max_database_bytes
        ):
            raise MetOfficeRateCompletionIntegrityError("output metadata differs")
        return row

    def _validate_capacity(self, database: sqlite3.Connection) -> None:
        count, total, invalid = database.execute(
            "SELECT count(*),coalesce(sum(output_bytes),0),"
            "coalesce(sum(CASE WHEN output_bytes < 1 OR output_bytes > ? THEN 1 ELSE 0 END),0) "
            "FROM completion_outputs",
            (self.limits.max_receipt_bytes,),
        ).fetchone()
        if (
            int(count) > self.limits.max_receipts
            or int(total) > self.limits.max_total_output_bytes
            or int(invalid) != 0
        ):
            raise MetOfficeRateCompletionCapacityError("output capacity differs")

    def _validate_legacy_v1(self, database: sqlite3.Connection) -> None:
        if self._tables(database) != self._TABLES:
            raise MetOfficeRateCompletionIntegrityError("legacy output tables differ")
        if self._columns(database, "output_metadata") != self._metadata_columns(legacy=True):
            raise MetOfficeRateCompletionIntegrityError(
                "legacy output metadata columns differ"
            )
        if self._columns(database, "completion_outputs") != self._output_columns():
            raise MetOfficeRateCompletionIntegrityError("legacy output columns differ")
        self._validate_unique_columns(database)
        self._validate_metadata(
            database,
            contract=LEGACY_OUTPUT_STORE_CONTRACT,
            schema_version=LEGACY_SCHEMA_VERSION,
        )
        self._validate_triggers(database, self._legacy_trigger_definitions())
        self._validate_capacity(database)
        if database.execute("PRAGMA quick_check").fetchone()[0] != "ok":
            raise MetOfficeRateCompletionIntegrityError(
                "legacy output database integrity check failed"
            )
        for row in database.execute("SELECT * FROM completion_outputs"):
            self._from_row(row)

    def _is_legacy_v1(self, database: sqlite3.Connection) -> bool:
        if self._tables(database) != self._TABLES:
            return False
        if self._columns(database, "output_metadata") != self._metadata_columns(legacy=True):
            return False
        row = database.execute(
            "SELECT contract,schema_version FROM output_metadata WHERE singleton=1"
        ).fetchone()
        return bool(
            row is not None
            and str(row["contract"]) == LEGACY_OUTPUT_STORE_CONTRACT
            and int(row["schema_version"]) == LEGACY_SCHEMA_VERSION
        )

    def _migrate_v1_to_v2(self, database: sqlite3.Connection) -> None:
        self._validate_legacy_v1(database)
        database.execute(
            "ALTER TABLE output_metadata ADD COLUMN store_instance_sha256 "
            "TEXT NOT NULL DEFAULT ''"
        )
        database.execute(
            "UPDATE output_metadata SET contract=?,schema_version=?,store_instance_sha256=? "
            "WHERE singleton=1",
            (OUTPUT_STORE_CONTRACT, SCHEMA_VERSION, secrets.token_hex(32)),
        )
        self._fault("migration.after_metadata")
        self._install_triggers(database, metadata_only=True)
        self._fault("migration.after_triggers")

    def _validate_schema(self, database: sqlite3.Connection, *, full: bool = False) -> str:
        if self._tables(database) != self._TABLES:
            raise MetOfficeRateCompletionIntegrityError("output table inventory differs")
        if self._columns(database, "output_metadata") != self._metadata_columns(legacy=False):
            raise MetOfficeRateCompletionIntegrityError("output metadata columns differ")
        if self._columns(database, "completion_outputs") != self._output_columns():
            raise MetOfficeRateCompletionIntegrityError("output columns differ")
        self._validate_unique_columns(database)
        metadata = self._validate_metadata(
            database, contract=OUTPUT_STORE_CONTRACT, schema_version=SCHEMA_VERSION
        )
        instance_sha = _sha(metadata["store_instance_sha256"], "output store instance SHA")
        if hasattr(self, "_store_instance_sha256") and not hmac.compare_digest(
            self._store_instance_sha256, instance_sha
        ):
            raise MetOfficeRateCompletionIntegrityError(
                "output store instance authority changed"
            )
        self._validate_triggers(database, self._trigger_definitions())
        self._validate_capacity(database)
        if full and database.execute("PRAGMA quick_check").fetchone()[0] != "ok":
            raise MetOfficeRateCompletionIntegrityError("output database integrity check failed")
        if full:
            for row in database.execute("SELECT * FROM completion_outputs"):
                self._from_row(row)
        return instance_sha

    def initialize(self) -> None:
        with self._lock:
            self._database_witness = self._prepare_path()
            try:
                with closing(self._connect()) as database:
                    try:
                        database.execute("BEGIN IMMEDIATE")
                        if not self._tables(database):
                            self._create_schema(database)
                        elif self._is_legacy_v1(database):
                            self._migrate_v1_to_v2(database)
                        self._validate_schema(database)
                        self._fault("migration.before_commit")
                        database.commit()
                        self._fault("migration.after_commit")
                    except BaseException:
                        if database.in_transaction:
                            database.rollback()
                        raise
                with closing(self._connect()) as database:
                    self._store_instance_sha256 = self._validate_schema(
                        database, full=True
                    )
            except sqlite3.DatabaseError as exc:
                raise MetOfficeRateCompletionIntegrityError(
                    "output database is invalid"
                ) from exc

    def _identity_for_instance(self, instance_sha: str) -> str:
        path_text = self._normalized_path(self.path)
        return _digest({
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
        })

    @property
    def store_instance_sha256(self) -> str:
        self._verify_path()
        return self._store_instance_sha256

    @property
    def identity_sha256(self) -> str:
        self._verify_path()
        return self._identity_for_instance(self._store_instance_sha256)

    def assert_instance_held(self, expected_identity_sha256: str | None = None) -> str:
        """Reprove the live file witness and persistent store sentinel."""

        with self._lock, closing(self._connect()) as database:
            instance_sha = self._validate_schema(database)
            identity = self._identity_for_instance(instance_sha)
        if expected_identity_sha256 is not None and not hmac.compare_digest(
            identity, _sha(expected_identity_sha256, "expected output store identity SHA")
        ):
            raise MetOfficeRateCompletionIntegrityError(
                "output store identity authority differs"
            )
        return identity

    def _from_row(self, row: sqlite3.Row) -> MetOfficeRateCompletionOutputReceipt:
        output_bytes = bytes(row["output_json"])
        if (
            len(output_bytes) != int(row["output_bytes"])
            or not 0 < len(output_bytes) <= self.limits.max_receipt_bytes
        ):
            raise MetOfficeRateCompletionIntegrityError("stored output size differs")
        try:
            raw = json.loads(output_bytes.decode("ascii"), object_pairs_hook=_reject_duplicates)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise MetOfficeRateCompletionIntegrityError("stored output JSON is corrupt") from exc
        document, event, authority, input_value, decoder, isolation = (
            _validated_output_document(raw)
        )
        if _canonical(document) != output_bytes:
            raise MetOfficeRateCompletionIntegrityError("stored output is not canonical")
        output_sha = hashlib.sha256(output_bytes).hexdigest()
        created_at = _timestamp(row["created_at"], "output created time")
        receipt_sha = _digest({
            "created_at": created_at,
            "output_sha256": output_sha,
            "schema": OUTPUT_RECEIPT_SCHEMA,
        })
        process_sha = _digest({
            "created_at": created_at,
            "effect_contract_sha256": isolation["effect_contract_sha256"],
            "event_id": event["event_id"],
            "isolation_authority_sha256": isolation["authority_sha256"],
            "limits_sha256": isolation["limits_sha256"],
            "output_receipt_sha256": receipt_sha,
            "output_sha256": output_sha,
            "schema": PROCESS_RECEIPT_SCHEMA,
        })
        evidence = decoder["evidence"]
        artifact = decoder["artifact"]
        checks = (
            (str(row["event_id"]), event["event_id"]),
            (str(row["event_sha256"]), event["event_sha256"]),
            (str(row["producer_receipt_sha256"]), event["producer_receipt_sha256"]),
            (str(row["desired_state_sha256"]), authority["desired_state_sha256"]),
            (str(row["authority_sha256"]), authority["authority_sha256"]),
            (str(row["consumer_id"]), CONSUMER_ID),
            (str(row["stream_id"]), STREAM_ID),
            (str(row["capture_id"]), input_value["capture_id"]),
            (str(row["capture_manifest_sha256"]), input_value["capture_manifest_sha256"]),
            (str(row["decoder_run_id"]), evidence["run_id"]),
            (str(row["evidence_receipt_sha256"]), evidence["receipt_sha256"]),
            (
                None if row["artifact_sha256"] is None else str(row["artifact_sha256"]),
                None if artifact is None else artifact["sha256"],
            ),
            (str(row["isolation_authority_sha256"]), isolation["authority_sha256"]),
            (
                str(row["isolation_effect_contract_sha256"]),
                isolation["effect_contract_sha256"],
            ),
            (str(row["isolation_limits_sha256"]), isolation["limits_sha256"]),
            (str(row["output_sha256"]), output_sha),
            (str(row["receipt_sha256"]), receipt_sha),
            (str(row["process_receipt_sha256"]), process_sha),
        )
        if any(actual != expected for actual, expected in checks):
            raise MetOfficeRateCompletionIntegrityError("output row differs from document")
        return MetOfficeRateCompletionOutputReceipt(
            receipt_sha256=receipt_sha,
            process_receipt_sha256=process_sha,
            output_sha256=output_sha,
            event_id=event["event_id"],
            event_sha256=event["event_sha256"],
            producer_receipt_sha256=event["producer_receipt_sha256"],
            desired_state_sha256=authority["desired_state_sha256"],
            authority_sha256=authority["authority_sha256"],
            capture_id=input_value["capture_id"],
            capture_manifest_sha256=input_value["capture_manifest_sha256"],
            decoder_run_id=evidence["run_id"],
            evidence_receipt_sha256=evidence["receipt_sha256"],
            artifact_sha256=None if artifact is None else artifact["sha256"],
            isolation_authority_sha256=isolation["authority_sha256"],
            isolation_limits_sha256=isolation["limits_sha256"],
            created_at=created_at,
            output=document,
        )

    def load(self, event_id: str) -> MetOfficeRateCompletionOutputReceipt | None:
        event = _identifier(event_id, "output event ID")
        with self._lock, closing(self._connect()) as database:
            self._validate_schema(database)
            row = database.execute(
                "SELECT * FROM completion_outputs WHERE event_id=?", (event,)
            ).fetchone()
            return None if row is None else self._from_row(row)

    def commit(self, output: Mapping[str, Any]) -> MetOfficeRateCompletionOutputReceipt:
        document, event, authority, input_value, decoder, isolation = (
            _validated_output_document(output)
        )
        body = _canonical(document)
        if not 0 < len(body) <= self.limits.max_receipt_bytes:
            raise MetOfficeRateCompletionCapacityError("output receipt exceeds byte cap")
        output_sha = hashlib.sha256(body).hexdigest()
        evidence = decoder["evidence"]
        artifact = decoder["artifact"]
        try:
            with self._lock, closing(self._connect()) as database:
                database.execute("BEGIN IMMEDIATE")
                self._validate_schema(database)
                existing = database.execute(
                    "SELECT * FROM completion_outputs WHERE event_id=?",
                    (event["event_id"],),
                ).fetchone()
                if existing is not None:
                    receipt = self._from_row(existing)
                    if not hmac.compare_digest(receipt.output_sha256, output_sha):
                        raise MetOfficeRateCompletionConflict(
                            "completion event already has a different output"
                        )
                    database.commit()
                    return receipt
                count, total = database.execute(
                    "SELECT count(*),coalesce(sum(output_bytes),0) FROM completion_outputs"
                ).fetchone()
                if int(count) >= self.limits.max_receipts:
                    raise MetOfficeRateCompletionCapacityError("output receipt count cap reached")
                if int(total) + len(body) > self.limits.max_total_output_bytes:
                    raise MetOfficeRateCompletionCapacityError("output receipt byte cap reached")
                created_at = _timestamp(self._clock(), "output clock")
                receipt_sha = _digest({
                    "created_at": created_at,
                    "output_sha256": output_sha,
                    "schema": OUTPUT_RECEIPT_SCHEMA,
                })
                process_sha = _digest({
                    "created_at": created_at,
                    "effect_contract_sha256": isolation["effect_contract_sha256"],
                    "event_id": event["event_id"],
                    "isolation_authority_sha256": isolation["authority_sha256"],
                    "limits_sha256": isolation["limits_sha256"],
                    "output_receipt_sha256": receipt_sha,
                    "output_sha256": output_sha,
                    "schema": PROCESS_RECEIPT_SCHEMA,
                })
                database.execute(
                    "INSERT INTO completion_outputs("
                    "event_id,event_sha256,producer_receipt_sha256,"
                    "desired_state_sha256,authority_sha256,consumer_id,stream_id,"
                    "capture_id,capture_manifest_sha256,decoder_run_id,"
                    "evidence_receipt_sha256,artifact_sha256,"
                    "isolation_authority_sha256,isolation_effect_contract_sha256,"
                    "isolation_limits_sha256,output_sha256,receipt_sha256,"
                    "process_receipt_sha256,output_json,output_bytes,created_at) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        event["event_id"], event["event_sha256"],
                        event["producer_receipt_sha256"],
                        authority["desired_state_sha256"], authority["authority_sha256"],
                        CONSUMER_ID, STREAM_ID, input_value["capture_id"],
                        input_value["capture_manifest_sha256"], evidence["run_id"],
                        evidence["receipt_sha256"],
                        None if artifact is None else artifact["sha256"],
                        isolation["authority_sha256"],
                        isolation["effect_contract_sha256"], isolation["limits_sha256"],
                        output_sha, receipt_sha, process_sha, body, len(body), created_at,
                    ),
                )
                self._fault("output.before_commit")
                database.commit()
                self._fault("output.after_commit")
        except MetOfficeRateCompletionError:
            raise
        except sqlite3.DatabaseError as exc:
            message = str(exc).lower()
            if "full" in message:
                raise MetOfficeRateCompletionCapacityError(
                    "output database capacity reached"
                ) from exc
            raise MetOfficeRateCompletionIntegrityError("output database commit failed") from exc
        receipt = self.load(event["event_id"])
        if receipt is None:  # pragma: no cover - commit invariant
            raise MetOfficeRateCompletionIntegrityError("committed output disappeared")
        return receipt

    def _verify_loaded_for_ack(
        self,
        *,
        receipt: MetOfficeRateCompletionOutputReceipt,
        lease: CompletionLease,
        binding: MetOfficeRateCompletionBinding,
        isolation_authority_sha256: str,
        isolation_limits_sha256: str,
        output_sha256: str,
        output_receipt_sha256: str,
        process_receipt_sha256: str,
    ) -> MetOfficeRateCompletionOutputReceipt:
        document_input = receipt.output["input"]
        document_event = receipt.output["event"]
        document_decoder = receipt.output["decoder"]
        document_isolation = receipt.output["isolation"]
        checks = (
            (receipt.event_sha256, lease.event_sha256),
            (receipt.producer_receipt_sha256, lease.producer_receipt_sha256),
            (receipt.desired_state_sha256, lease.desired_state_sha256),
            (receipt.authority_sha256, lease.authority_sha256),
            (receipt.capture_id, binding.capture_id),
            (receipt.capture_manifest_sha256, binding.capture_manifest_sha256),
            (document_event["stream_sequence"], lease.stream_sequence),
            (document_input["acquisition_job_id"], binding.acquisition_job_id),
            (document_input["capture_receipt_sha256"], binding.capture_receipt_sha256),
            (document_input["reference_content_sha256"], lease.content_sha256),
            (
                document_input["role_set_content_sha256"],
                binding.role_set_content_sha256,
            ),
            (
                document_input["role_set_receipt_sha256"],
                binding.role_set_receipt_sha256,
            ),
            (document_decoder["job_id"], binding.decoder_job_id),
            (document_decoder["attempt_id"], binding.decoder_attempt_id),
            (
                document_decoder["evidence"]["capture_manifest_sha256"],
                binding.capture_manifest_sha256,
            ),
            (receipt.isolation_authority_sha256, _sha(
                isolation_authority_sha256, "ack isolation authority"
            )),
            (receipt.isolation_limits_sha256, _sha(
                isolation_limits_sha256, "ack isolation limits"
            )),
            (receipt.output_sha256, _sha(output_sha256, "ack output SHA-256")),
            (receipt.receipt_sha256, _sha(
                output_receipt_sha256, "ack output receipt SHA-256"
            )),
            (receipt.process_receipt_sha256, _sha(
                process_receipt_sha256, "ack process receipt SHA-256"
            )),
            (receipt.output.get("fact_revision"), None),
            (receipt.output["claims"]["fact_staged"], False),
            (receipt.output["claims"]["meteorological_admission"], False),
            (receipt.output["claims"]["operational_snapshot_published"], False),
            (
                document_isolation["effect_contract_sha256"],
                PROCESS_EFFECT_CONTRACT_SHA256,
            ),
            (receipt.output["mode"], OUTPUT_MODE),
            (receipt.output["workload_id"], WORKLOAD_ID),
            (receipt.output["consumer_id"], CONSUMER_ID),
            (receipt.output["stream_id"], STREAM_ID),
        )
        if any(actual != expected for actual, expected in checks):
            raise MetOfficeRateCompletionConflict("ack proof differs from durable output")
        return receipt

    def verify_for_ack(
        self,
        *,
        lease: CompletionLease,
        binding: MetOfficeRateCompletionBinding,
        isolation_authority_sha256: str,
        isolation_limits_sha256: str,
        output_sha256: str,
        output_receipt_sha256: str,
        process_receipt_sha256: str,
    ) -> MetOfficeRateCompletionOutputReceipt:
        receipt = self.load(lease.event_id)
        if receipt is None:
            raise MetOfficeRateCompletionIntegrityError("durable output is unavailable")
        return self._verify_loaded_for_ack(
            receipt=receipt,
            lease=lease,
            binding=binding,
            isolation_authority_sha256=isolation_authority_sha256,
            isolation_limits_sha256=isolation_limits_sha256,
            output_sha256=output_sha256,
            output_receipt_sha256=output_receipt_sha256,
            process_receipt_sha256=process_receipt_sha256,
        )

    @contextmanager
    def completion_reproof_lease(
        self,
        *,
        lease: CompletionLease,
        binding: MetOfficeRateCompletionBinding,
        isolation_authority_sha256: str,
        isolation_limits_sha256: str,
        output_sha256: str,
        output_receipt_sha256: str,
        process_receipt_sha256: str,
        expected_identity_sha256: str,
    ) -> Iterator[MetOfficeRateCompletionOutputReceipt]:
        """Hold one exact output row under BEGIN IMMEDIATE through lifecycle ACK."""

        if not isinstance(lease, CompletionLease) or not isinstance(
            binding, MetOfficeRateCompletionBinding
        ):
            raise TypeError("completion output reproof authority has wrong type")
        expected_identity = _sha(expected_identity_sha256, "expected output store identity SHA")
        event_id = _identifier(lease.event_id, "output event ID")
        with self._lock, closing(self._connect()) as database:
            try:
                database.execute("BEGIN IMMEDIATE")
                instance_sha = self._validate_schema(database, full=True)
                if not hmac.compare_digest(
                    self._identity_for_instance(instance_sha), expected_identity
                ):
                    raise MetOfficeRateCompletionIntegrityError(
                        "output store identity authority differs"
                    )
                row = database.execute(
                    "SELECT * FROM completion_outputs WHERE event_id=?", (event_id,)
                ).fetchone()
                if row is None:
                    raise MetOfficeRateCompletionIntegrityError(
                        "durable output is unavailable"
                    )
                receipt = self._verify_loaded_for_ack(
                    receipt=self._from_row(row),
                    lease=lease,
                    binding=binding,
                    isolation_authority_sha256=isolation_authority_sha256,
                    isolation_limits_sha256=isolation_limits_sha256,
                    output_sha256=output_sha256,
                    output_receipt_sha256=output_receipt_sha256,
                    process_receipt_sha256=process_receipt_sha256,
                )
                yield receipt
                self._verify_path()
                final_instance = self._validate_schema(database)
                if not hmac.compare_digest(
                    self._identity_for_instance(final_instance), expected_identity
                ):
                    raise MetOfficeRateCompletionIntegrityError(
                        "output store changed while reproof was fenced"
                    )
                final_row = database.execute(
                    "SELECT * FROM completion_outputs WHERE event_id=?", (event_id,)
                ).fetchone()
                if final_row is None:
                    raise MetOfficeRateCompletionIntegrityError(
                        "durable output disappeared while reproof was fenced"
                    )
                final_receipt = self._verify_loaded_for_ack(
                    receipt=self._from_row(final_row),
                    lease=lease,
                    binding=binding,
                    isolation_authority_sha256=isolation_authority_sha256,
                    isolation_limits_sha256=isolation_limits_sha256,
                    output_sha256=output_sha256,
                    output_receipt_sha256=output_receipt_sha256,
                    process_receipt_sha256=process_receipt_sha256,
                )
                if final_receipt != receipt:
                    raise MetOfficeRateCompletionIntegrityError(
                        "durable output changed while reproof was fenced"
                    )
            finally:
                if database.in_transaction:
                    database.rollback()

    def diagnostics(self) -> dict[str, Any]:
        with self._lock, closing(self._connect()) as database:
            self._validate_schema(database, full=True)
            count, total = database.execute(
                "SELECT count(*),coalesce(sum(output_bytes),0) FROM completion_outputs"
            ).fetchone()
        result = {
            "contract": OUTPUT_STORE_CONTRACT,
            "healthy": True,
            "output_bytes": int(total),
            "receipts": int(count),
            "runtime_wiring": "inert",
        }
        if len(_canonical(result)) > MAX_DIAGNOSTIC_BYTES:
            raise MetOfficeRateCompletionCapacityError("output diagnostics exceed cap")
        return result


def _safe_existing_path(value: object, label: str, *, directory: bool) -> Path:
    if not isinstance(value, str) or not value or "\0" in value or len(value) > 1024:
        raise MetOfficeRateCompletionIntegrityError(f"{label} path is invalid")
    path = Path(value)
    if not path.is_absolute() or path.is_symlink() or not path.exists():
        raise MetOfficeRateCompletionIntegrityError(f"{label} path is unsafe")
    resolved = path.resolve(strict=True)
    if resolved != path:
        raise MetOfficeRateCompletionIntegrityError(f"{label} path is not normalized")
    mode = path.lstat().st_mode
    if (directory and not stat.S_ISDIR(mode)) or (
        not directory and not stat.S_ISREG(mode)
    ):
        raise MetOfficeRateCompletionIntegrityError(f"{label} path type differs")
    return path


def _assert_process_store_paths_disjoint(
    *,
    raw_root: Path,
    artifact_root: Path,
    evidence_path: Path,
    output_path: Path,
) -> None:
    """Reject store-role aliasing even when the authority hash is recomputed."""

    try:
        roots_alias = raw_root.samefile(artifact_root)
        databases_alias = evidence_path.samefile(output_path)
    except OSError:
        raise MetOfficeRateCompletionIntegrityError(
            "process authority store identity is unavailable"
        ) from None
    if (
        roots_alias
        or raw_root.is_relative_to(artifact_root)
        or artifact_root.is_relative_to(raw_root)
    ):
        raise MetOfficeRateCompletionIntegrityError(
            "process authority store roots overlap"
        )
    if databases_alias:
        raise MetOfficeRateCompletionIntegrityError(
            "process authority database paths alias"
        )
    for database_path in (evidence_path, output_path):
        if (
            database_path.is_relative_to(raw_root)
            or database_path.is_relative_to(artifact_root)
        ):
            raise MetOfficeRateCompletionIntegrityError(
                "process authority database overlaps an object store"
            )


@dataclass(frozen=True, slots=True)
class _LoadedProcessAuthority:
    sha256: str
    raw_root: Path
    raw_limits: RawArtifactLimits
    artifact_root: Path
    artifact_max_total_bytes: int
    artifact_max_objects: int
    artifact_max_staging_objects: int
    evidence_path: Path
    evidence_max_database_bytes: int
    output_path: Path
    output_limits: MetOfficeRateCompletionOutputLimits
    process_limits: MetOfficeRateCompletionProcessLimits


def _load_process_authority(value: object, expected_sha256: str) -> _LoadedProcessAuthority:
    mapping = _exact(value, {
        "consumer_binding", "consumer_binding_sha256", "effect_contract",
        "effect_contract_sha256", "process_limits", "schema", "stores",
    }, "Met Office process authority")
    if (
        mapping["schema"] != PROCESS_AUTHORITY_SCHEMA
        or _digest(mapping) != _sha(expected_sha256, "process authority SHA-256")
        or mapping["consumer_binding"] != _catalog_binding().as_dict()
        or mapping["consumer_binding_sha256"] != _catalog_binding().fingerprint
        or mapping["effect_contract"] != _effect_mapping()
        or mapping["effect_contract_sha256"] != PROCESS_EFFECT_CONTRACT_SHA256
    ):
        raise MetOfficeRateCompletionIntegrityError("process authority differs")
    limit_mapping = _exact(mapping["process_limits"], {
        "cpu_seconds", "max_request_bytes", "max_result_bytes", "rss_bytes",
        "wall_seconds",
    }, "process limits")
    limits = MetOfficeRateCompletionProcessLimits(**dict(limit_mapping))
    if limits.to_mapping() != dict(limit_mapping):
        raise MetOfficeRateCompletionIntegrityError("process limits roundtrip differs")
    stores = _exact(mapping["stores"], {
        "canonical_evidence", "derived_artifacts", "outputs", "source_raw",
    }, "process stores")
    raw_value = _exact(stores["source_raw"], {"limits", "root"}, "raw authority")
    raw_limits = RawArtifactLimits(**dict(_exact(
        raw_value["limits"], set(RawArtifactLimits.__dataclass_fields__), "raw limits"
    )))
    artifact = _exact(stores["derived_artifacts"], {
        "max_objects", "max_staging_objects", "max_total_bytes", "root",
    }, "artifact authority")
    evidence = _exact(stores["canonical_evidence"], {
        "max_database_bytes", "path",
    }, "evidence authority")
    output = _exact(stores["outputs"], {"limits", "path"}, "output authority")
    output_limits = MetOfficeRateCompletionOutputLimits(**dict(_exact(
        output["limits"],
        set(MetOfficeRateCompletionOutputLimits.__dataclass_fields__),
        "output limits",
    )))
    integer_limits = (
        artifact["max_objects"], artifact["max_staging_objects"],
        artifact["max_total_bytes"], evidence["max_database_bytes"],
    )
    if any(isinstance(item, bool) or not isinstance(item, int) or item < 1 for item in integer_limits):
        raise MetOfficeRateCompletionIntegrityError("store limits are invalid")
    raw_root = _safe_existing_path(raw_value["root"], "source raw", directory=True)
    artifact_root = _safe_existing_path(
        artifact["root"], "derived artifacts", directory=True
    )
    evidence_path = _safe_existing_path(
        evidence["path"], "canonical evidence", directory=False
    )
    output_path = _safe_existing_path(
        output["path"], "completion output", directory=False
    )
    _assert_process_store_paths_disjoint(
        raw_root=raw_root,
        artifact_root=artifact_root,
        evidence_path=evidence_path,
        output_path=output_path,
    )
    return _LoadedProcessAuthority(
        sha256=expected_sha256,
        raw_root=raw_root,
        raw_limits=raw_limits,
        artifact_root=artifact_root,
        artifact_max_total_bytes=artifact["max_total_bytes"],
        artifact_max_objects=artifact["max_objects"],
        artifact_max_staging_objects=artifact["max_staging_objects"],
        evidence_path=evidence_path,
        evidence_max_database_bytes=evidence["max_database_bytes"],
        output_path=output_path,
        output_limits=output_limits,
        process_limits=limits,
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
        "payload": _decode_reference(lease.payload),
        "producer_receipt_sha256": lease.producer_receipt_sha256,
        "proof": json.loads(_canonical(lease.proof)),
        "size_bytes": lease.size_bytes,
        "stream_id": lease.stream_id,
        "stream_sequence": lease.stream_sequence,
    }


def _lease_from_wire(value: object) -> CompletionLease:
    mapping = _exact(value, {
        "acquisition_job_id", "attempt_count", "authority_sha256", "consumer_id",
        "content_sha256", "desired_state_sha256", "event_id", "event_sha256",
        "lease_expires_at", "payload", "producer_receipt_sha256", "proof",
        "size_bytes", "stream_id", "stream_sequence",
    }, "isolated lease")
    payload = _canonical(mapping["payload"])
    if not isinstance(mapping["proof"], dict):
        raise MetOfficeRateCompletionIntegrityError("isolated proof is invalid")
    return CompletionLease(
        event_id=_identifier(mapping["event_id"], "isolated event ID"),
        event_sha256=_sha(mapping["event_sha256"], "isolated event SHA-256"),
        producer_receipt_sha256=_sha(
            mapping["producer_receipt_sha256"], "isolated producer SHA-256"
        ),
        acquisition_job_id=_identifier(
            mapping["acquisition_job_id"], "isolated acquisition job ID"
        ),
        consumer_id=_identifier(mapping["consumer_id"], "isolated consumer ID"),
        stream_id=_identifier(mapping["stream_id"], "isolated stream ID"),
        stream_sequence=_integer(
            mapping["stream_sequence"], "isolated stream sequence", minimum=1
        ),
        desired_state_sha256=_sha(
            mapping["desired_state_sha256"], "isolated desired SHA-256"
        ),
        authority_sha256=_sha(
            mapping["authority_sha256"], "isolated authority SHA-256"
        ),
        content_sha256=_sha(mapping["content_sha256"], "isolated content SHA-256"),
        size_bytes=_integer(mapping["size_bytes"], "isolated size", minimum=1),
        attempt_count=_integer(mapping["attempt_count"], "isolated attempt", minimum=1),
        lease_token="0" * 64,
        lease_expires_at=_timestamp(mapping["lease_expires_at"], "isolated expiry"),
        proof=mapping["proof"],
        payload=payload,
    )


def _output_document(
    *,
    lease: CompletionLease,
    binding: MetOfficeRateCompletionBinding,
    result: Any,
    isolation_authority_sha256: str,
    isolation_limits_sha256: str,
) -> dict[str, Any]:
    evidence = result.evidence_receipt
    run = evidence.run
    if (
        run.decoder_job_id != binding.decoder_job_id
        or run.attempt_id != binding.decoder_attempt_id
        or run.capture_id != binding.capture_id
        or run.capture_receipt_sha256 != binding.capture_receipt_sha256
        or run.decoder_identity != DECODER_IDENTITY
        or run.decoder_version != DECODER_VERSION
        or run.config_sha256 != DECODER_CONFIG_SHA256
        or run.provider != PROVIDER
        or run.product != CAPTURE_PRODUCT
        or run.evaluated_at != binding.evaluated_at
        or run.outcome != result.outcome
        or run.reason_code != result.reason_code
        or evidence.capture_manifest_sha256 != binding.capture_manifest_sha256
    ):
        raise MetOfficeRateCompletionIntegrityError("decoder evidence differs from input")
    artifact = result.artifact
    if (artifact is None) != (result.outcome != "decoded_complete"):
        raise MetOfficeRateCompletionIntegrityError("decoder artifact/outcome differs")
    if artifact is not None:
        candidate = run.non_operational_candidate_artifact
        if (
            candidate is None
            or candidate.handle != artifact.handle
            or candidate.sha256 != artifact.sha256
        ):
            raise MetOfficeRateCompletionIntegrityError("decoder artifact evidence differs")
    output = {
        "authority": {
            "authority_sha256": lease.authority_sha256,
            "desired_state_sha256": lease.desired_state_sha256,
        },
        "claims": {
            "decoded": result.outcome == "decoded_complete",
            "fact_staged": False,
            "meteorological_admission": False,
            "operational_snapshot_published": False,
        },
        "consumer_id": CONSUMER_ID,
        "decoder": {
            "artifact": None if artifact is None else {
                "handle": artifact.handle,
                "sha256": artifact.sha256,
                "size_bytes": artifact.size_bytes,
            },
            "attempt_id": binding.decoder_attempt_id,
            "config_sha256": DECODER_CONFIG_SHA256,
            "evidence": {
                "capture_manifest_sha256": evidence.capture_manifest_sha256,
                "commit_id": evidence.commit_id,
                "receipt_sha256": evidence.receipt_sha256,
                "result_sha256": evidence.result_sha256,
                "revision_sha256": evidence.revision_sha256,
                "run_id": evidence.run_id,
            },
            "identity": DECODER_IDENTITY,
            "job_id": binding.decoder_job_id,
            "outcome": result.outcome,
            "reason_code": result.reason_code,
            "version": DECODER_VERSION,
        },
        "disposition": (
            "shadow_candidate" if result.outcome == "decoded_complete" else "shadow_rejected"
        ),
        "event": {
            "event_id": lease.event_id,
            "event_sha256": lease.event_sha256,
            "producer_receipt_sha256": lease.producer_receipt_sha256,
            "stream_sequence": lease.stream_sequence,
        },
        "fact_revision": None,
        "input": {
            "acquisition_job_id": binding.acquisition_job_id,
            "capture_id": binding.capture_id,
            "capture_manifest_sha256": binding.capture_manifest_sha256,
            "capture_receipt_sha256": binding.capture_receipt_sha256,
            "reference_content_sha256": lease.content_sha256,
            "role_set_content_sha256": binding.role_set_content_sha256,
            "role_set_receipt_sha256": binding.role_set_receipt_sha256,
        },
        "isolation": {
            "authority_sha256": isolation_authority_sha256,
            "effect_contract_sha256": PROCESS_EFFECT_CONTRACT_SHA256,
            "limits_sha256": isolation_limits_sha256,
            "process_boundary": "fresh_spawn_hard_cpu_rss_wall_output_caps",
            "runtime_wiring": "inert",
        },
        "mode": OUTPUT_MODE,
        "schema": OUTPUT_DOCUMENT_SCHEMA,
        "stream_id": STREAM_ID,
        "workload_id": WORKLOAD_ID,
    }
    _validated_output_document(output)
    return output


def _met_office_rate_completion_child(request: Mapping[str, Any]) -> dict[str, Any]:
    """Importable child entry; returns hashes only after durable commit."""

    mapping = _exact(request, {
        "authority", "authority_sha256", "desired_state", "lease",
        "output_store_identity_sha256", "request_sha256", "schema",
    }, "Met Office process request")
    request_body = {key: mapping[key] for key in mapping if key != "request_sha256"}
    if (
        mapping["schema"] != PROCESS_REQUEST_SCHEMA
        or _digest(request_body) != _sha(mapping["request_sha256"], "request SHA-256")
    ):
        raise MetOfficeRateCompletionIntegrityError("process request differs")
    authority = _load_process_authority(
        mapping["authority"], mapping["authority_sha256"]
    )
    desired = parse_desired_state(mapping["desired_state"])
    lease = _lease_from_wire(mapping["lease"])
    expected_output_identity = _sha(
        mapping["output_store_identity_sha256"], "output store identity SHA"
    )
    raw_store = RawArtifactStore(authority.raw_root, limits=authority.raw_limits)
    raw_store.initialize()
    binding = validate_met_office_rate_completion_lease(
        lease, desired_state=desired, raw_store=raw_store
    )
    artifact_store = MetOfficeRateDerivedArtifactStore(
        authority.artifact_root,
        max_total_bytes=authority.artifact_max_total_bytes,
        max_objects=authority.artifact_max_objects,
        max_staging_objects=authority.artifact_max_staging_objects,
    )
    raw_authority = MetOfficeRateRawAuthority(raw_store)
    # CanonicalEvidenceStore reconciles the deterministic commit identity on
    # retry; its committed_at remains the honest child wall-clock commit time.
    evidence_store = open_met_office_rate_evidence_store(
        authority.evidence_path,
        raw_authority=raw_authority,
        max_database_bytes=authority.evidence_max_database_bytes,
    )
    output_store = MetOfficeRateCompletionOutputStore(
        authority.output_path, limits=authority.output_limits
    )
    output_store.assert_instance_held(expected_output_identity)
    monotonic_deadline = time.monotonic() + min(
        90.0, max(0.05, authority.process_limits.wall_seconds - 1.0)
    )
    result = decode_met_office_rate_capture(
        capture_id=binding.capture_id,
        decoder_job_id=binding.decoder_job_id,
        attempt_id=binding.decoder_attempt_id,
        raw_authority=raw_authority,
        artifact_store=artifact_store,
        evidence_store=evidence_store,
        evaluated_at=binding.evaluated_at,
        monotonic_deadline=monotonic_deadline,
        monotonic_clock=time.monotonic,
    )
    output = _output_document(
        lease=lease,
        binding=binding,
        result=result,
        isolation_authority_sha256=authority.sha256,
        isolation_limits_sha256=authority.process_limits.fingerprint,
    )
    receipt = output_store.commit(output)
    # Reopen/reverify after commit so success never means merely "write called".
    durable = output_store.verify_for_ack(
        lease=lease,
        binding=binding,
        isolation_authority_sha256=authority.sha256,
        isolation_limits_sha256=authority.process_limits.fingerprint,
        output_sha256=receipt.output_sha256,
        output_receipt_sha256=receipt.receipt_sha256,
        process_receipt_sha256=receipt.process_receipt_sha256,
    )
    output_store.assert_instance_held(expected_output_identity)
    return {
        "event_id": durable.event_id,
        "output_receipt_sha256": durable.receipt_sha256,
        "output_sha256": durable.output_sha256,
        "process_receipt_sha256": durable.process_receipt_sha256,
        "schema": PROCESS_CHILD_RESULT_SCHEMA,
    }


def _technical_status(result: DecoderProcessResult) -> tuple[str, str]:
    if result.outcome == "deadline_exceeded":
        return "deadline", "hard_parent_wall_deadline"
    if result.outcome == "cancelled":
        return "cancelled", "hard_process_cancelled"
    if result.outcome == "busy":
        return "busy", "global_hard_process_slot_busy"
    if result.outcome in {"cpu_limit_exceeded", "memory_limit_exceeded", "result_too_large"}:
        return "resource_limited", "hard_cpu_rss_or_output_limit"
    if result.outcome in {
        "unsupported_platform", "boundary_setup_failed", "unsafe_effect_contract",
    }:
        return "boundary_unavailable", "hard_process_boundary_unavailable"
    if result.outcome in {"invalid_request", "result_not_json", "protocol_error"}:
        return "contract_failed", "hard_process_protocol_failed"
    return "failed", "hard_process_failed"


class MetOfficeRateCompletionIsolatedRunner:
    """Fresh hard child plus durable-output reconciliation on every outcome."""

    def __init__(
        self,
        *,
        raw_store: RawArtifactStore,
        artifact_store: MetOfficeRateDerivedArtifactStore,
        evidence_store: CanonicalEvidenceStore,
        output_store: MetOfficeRateCompletionOutputStore,
        limits: MetOfficeRateCompletionProcessLimits = MetOfficeRateCompletionProcessLimits(),
        context: multiprocessing.context.BaseContext | None = None,
    ) -> None:
        if not isinstance(raw_store, RawArtifactStore):
            raise TypeError("raw_store has wrong type")
        if not isinstance(artifact_store, MetOfficeRateDerivedArtifactStore):
            raise TypeError("artifact_store has wrong type")
        if not isinstance(evidence_store, CanonicalEvidenceStore):
            raise TypeError("evidence_store has wrong type")
        evidence_raw_authority = getattr(
            evidence_store, "_raw_artifact_reader", None
        )
        if (
            not isinstance(evidence_raw_authority, MetOfficeRateRawAuthority)
            or evidence_raw_authority.raw_store is not raw_store
        ):
            raise TypeError(
                "evidence_store must share the exact Met Office raw authority"
            )
        if not isinstance(output_store, MetOfficeRateCompletionOutputStore):
            raise TypeError("output_store has wrong type")
        if not isinstance(limits, MetOfficeRateCompletionProcessLimits):
            raise TypeError("process limits have wrong type")
        self.raw_store = raw_store
        self.artifact_store = artifact_store
        self.evidence_store = evidence_store
        self.output_store = output_store
        self.limits = limits
        self.output_store_identity_sha256 = output_store.assert_instance_held()
        self._authority = self._authority_mapping()
        self.authority_sha256 = _digest(self._authority)
        _load_process_authority(self._authority, self.authority_sha256)
        self._executor = HardDecoderExecutor(
            limits=limits.decoder_limits(), context=context
        )

    @property
    def minimum_lease_seconds(self) -> float:
        # Three parent-side physical artifact reproofs may each consume their
        # own five-second bound after the child wall envelope.
        return float(self.limits.wall_seconds) + 17.0

    def _authority_mapping(self) -> dict[str, Any]:
        for path, label, directory in (
            (self.raw_store.root, "source raw", True),
            (self.artifact_store.root, "derived artifacts", True),
            (self.evidence_store.path, "canonical evidence", False),
            (self.output_store.path, "completion output", False),
        ):
            _safe_existing_path(str(path), label, directory=directory)
        return {
            "consumer_binding": _catalog_binding().as_dict(),
            "consumer_binding_sha256": _catalog_binding().fingerprint,
            "effect_contract": _effect_mapping(),
            "effect_contract_sha256": PROCESS_EFFECT_CONTRACT_SHA256,
            "process_limits": self.limits.to_mapping(),
            "schema": PROCESS_AUTHORITY_SCHEMA,
            "stores": {
                "canonical_evidence": {
                    "max_database_bytes": self.evidence_store._max_database_bytes,
                    "path": str(self.evidence_store.path),
                },
                "derived_artifacts": {
                    "max_objects": self.artifact_store.max_objects,
                    "max_staging_objects": self.artifact_store.max_staging_objects,
                    "max_total_bytes": self.artifact_store.max_total_bytes,
                    "root": str(self.artifact_store.root),
                },
                "outputs": {
                    "limits": asdict(self.output_store.limits),
                    "path": str(self.output_store.path),
                },
                "source_raw": {
                    "limits": asdict(self.raw_store.limits),
                    "root": str(self.raw_store.root),
                },
            },
        }

    def _load_exact(
        self,
        lease: CompletionLease,
        binding: MetOfficeRateCompletionBinding,
    ) -> MetOfficeRateCompletionOutputReceipt | None:
        receipt = self.output_store.load(lease.event_id)
        if receipt is None:
            return None
        verified = self.output_store.verify_for_ack(
            lease=lease,
            binding=binding,
            isolation_authority_sha256=self.authority_sha256,
            isolation_limits_sha256=self.limits.fingerprint,
            output_sha256=receipt.output_sha256,
            output_receipt_sha256=receipt.receipt_sha256,
            process_receipt_sha256=receipt.process_receipt_sha256,
        )
        self._reprove_decoder_authorities(verified, binding)
        return verified

    def _reprove_decoder_authorities(
        self,
        receipt: MetOfficeRateCompletionOutputReceipt,
        binding: MetOfficeRateCompletionBinding,
    ) -> None:
        """Reopen canonical evidence and the derived object before any ack."""

        self._reprove_raw_authority(binding)
        decoder = receipt.output["decoder"]
        evidence_document = decoder["evidence"]
        artifact_document = decoder["artifact"]
        try:
            evidence = self.evidence_store.load(receipt.decoder_run_id)
        except (CanonicalEvidenceError, OSError, sqlite3.DatabaseError, ValueError):
            raise MetOfficeRateCompletionIntegrityError(
                "canonical decoder evidence failed reproof"
            ) from None
        if evidence is None:
            raise MetOfficeRateCompletionIntegrityError(
                "canonical decoder evidence is unavailable"
            )
        run = evidence.run
        checks = (
            (evidence.run_id, receipt.decoder_run_id),
            (evidence.run_id, evidence_document["run_id"]),
            (evidence.commit_id, evidence_document["commit_id"]),
            (evidence.receipt_sha256, receipt.evidence_receipt_sha256),
            (evidence.receipt_sha256, evidence_document["receipt_sha256"]),
            (evidence.result_sha256, evidence_document["result_sha256"]),
            (evidence.revision_sha256, evidence_document["revision_sha256"]),
            (
                evidence.capture_manifest_sha256,
                evidence_document["capture_manifest_sha256"],
            ),
            (evidence.capture_manifest_sha256, binding.capture_manifest_sha256),
            (run.decoder_job_id, binding.decoder_job_id),
            (run.attempt_id, binding.decoder_attempt_id),
            (run.capture_id, binding.capture_id),
            (run.capture_receipt_sha256, binding.capture_receipt_sha256),
            (run.decoder_identity, DECODER_IDENTITY),
            (run.decoder_version, DECODER_VERSION),
            (run.config_sha256, DECODER_CONFIG_SHA256),
            (run.provider, PROVIDER),
            (run.product, CAPTURE_PRODUCT),
            (run.evaluated_at, binding.evaluated_at),
            (run.outcome, decoder["outcome"]),
            (run.reason_code, decoder["reason_code"]),
        )
        if any(actual != expected for actual, expected in checks):
            raise MetOfficeRateCompletionIntegrityError(
                "completion output differs from canonical decoder evidence"
            )

        candidate = run.non_operational_candidate_artifact
        if artifact_document is None:
            if candidate is not None or run.outcome == "decoded_complete":
                raise MetOfficeRateCompletionIntegrityError(
                    "canonical decoder artifact binding differs"
                )
            return
        if (
            candidate is None
            or run.outcome != "decoded_complete"
            or candidate.handle != artifact_document["handle"]
            or candidate.sha256 != artifact_document["sha256"]
        ):
            raise MetOfficeRateCompletionIntegrityError(
                "completion artifact differs from canonical decoder evidence"
            )
        reference = MetOfficeRateArtifactRef(
            handle=artifact_document["handle"],
            sha256=artifact_document["sha256"],
            size_bytes=artifact_document["size_bytes"],
        )
        reproof_deadline = time.monotonic() + 5.0

        def checkpoint(_stage: str) -> None:
            if time.monotonic() >= reproof_deadline:
                raise MetOfficeRateDecodeDeadlineError(
                    "derived artifact reproof deadline expired"
                )

        try:
            self.artifact_store.read_bytes(reference, checkpoint=checkpoint)
        except (
            MetOfficeRateArtifactError,
            MetOfficeRateDecodeDeadlineError,
            OSError,
            ValueError,
            TypeError,
        ):
            raise MetOfficeRateCompletionIntegrityError(
                "derived decoder artifact failed reproof"
            ) from None

    def _reprove_raw_authority(
        self,
        binding: MetOfficeRateCompletionBinding,
    ) -> None:
        """Rehash all three raw roles and reapply recovery provenance rules."""

        rule = ACTIVATION_RULES.get(WORKLOAD_ID)
        recovery = (
            None
            if not isinstance(rule, ActivationRule)
            else RECOVERY_BINDINGS.get(str(rule.handler_id))
        )
        if (
            not isinstance(rule, ActivationRule)
            or rule.status is not ActivationStatus.ACTIVE_SHADOW_RAW
            or rule.required_roles != EXPECTED_ROLES
            or not isinstance(recovery, RawRecoveryBinding)
            or recovery.required_roles != EXPECTED_ROLES
        ):
            raise MetOfficeRateCompletionIntegrityError(
                "Met Office recovery authority drifted during reproof"
            )
        try:
            loaded = self.raw_store.load_capture_bundle_bytes(binding.capture_id)
            if loaded is None:
                raise MetOfficeRateCompletionIntegrityError(
                    "Met Office raw capture disappeared during reproof"
                )
            capture, receipts, payloads = loaded
            if (
                set(receipts) != set(EXPECTED_ROLES)
                or set(payloads) != set(EXPECTED_ROLES)
            ):
                raise MetOfficeRateCompletionIntegrityError(
                    "Met Office raw roles changed during reproof"
                )
            metadata_by_role: dict[str, str | None] = {}
            for role in EXPECTED_ROLES:
                raw_receipt = receipts[role]
                raw_job_id, metadata = validate_recovery_receipt(
                    raw_receipt, recovery, recovery.member(role)
                )
                if (
                    raw_job_id != binding.acquisition_job_id
                    or hashlib.sha256(payloads[role]).hexdigest()
                    != raw_receipt.content_sha256
                    or len(payloads[role]) != raw_receipt.size_bytes
                ):
                    raise MetOfficeRateCompletionIntegrityError(
                        "Met Office raw bytes changed during reproof"
                    )
                metadata_by_role[role] = metadata
            capture_job_id = validate_recovery_capture(
                capture,
                binding=recovery,
                receipts_by_role=receipts,
                metadata_by_role=metadata_by_role,
            )
            evidence = recovery_evidence_digest(
                binding=recovery,
                receipts_by_role=receipts,
                capture=capture,
            )
        except (CaptureRecoveryError, RawArtifactError, OSError, ValueError):
            raise MetOfficeRateCompletionIntegrityError(
                "Met Office raw authority failed reproof"
            ) from None
        if (
            capture_job_id != binding.acquisition_job_id
            or capture.capture_id != binding.capture_id
            or capture.manifest_sha256 != binding.capture_manifest_sha256
            or capture.capture_receipt_sha256 != binding.capture_receipt_sha256
            or evidence.content_sha256 != binding.role_set_content_sha256
            or evidence.receipt_sha256 != binding.role_set_receipt_sha256
        ):
            raise MetOfficeRateCompletionIntegrityError(
                "Met Office raw authority differs from completion binding"
            )

    def execute(
        self,
        *,
        lease: CompletionLease,
        desired_state: DesiredState,
        cancel_event: threading.Event,
    ) -> MetOfficeRateCompletionProcessResult:
        started = time.monotonic()
        if not callable(getattr(cancel_event, "is_set", None)):
            raise TypeError("cancel_event must expose is_set()")
        self.output_store.assert_instance_held(self.output_store_identity_sha256)
        binding = validate_met_office_rate_completion_lease(
            lease, desired_state=desired_state, raw_store=self.raw_store
        )
        existing = self._load_exact(lease, binding)
        if existing is not None:
            return MetOfficeRateCompletionProcessResult(
                status="committed",
                reason_code="durable_output_reconciled_before_spawn",
                execution_outcome="durable_duplicate",
                output_receipt=existing,
                isolation_enforced=True,
                elapsed_seconds=max(0.0, time.monotonic() - started),
            )
        if cancel_event.is_set():
            return MetOfficeRateCompletionProcessResult(
                status="cancelled",
                reason_code="cancelled_before_spawn",
                execution_outcome="cancelled",
                output_receipt=None,
                isolation_enforced=False,
                elapsed_seconds=max(0.0, time.monotonic() - started),
            )
        body = {
            "authority": self._authority,
            "authority_sha256": self.authority_sha256,
            "desired_state": desired_state.as_dict(),
            "lease": _lease_wire(lease),
            "output_store_identity_sha256": self.output_store_identity_sha256,
            "schema": PROCESS_REQUEST_SCHEMA,
        }
        request = {**body, "request_sha256": _digest(body)}
        decoder = self._executor.execute(
            target=PROCESS_TARGET,
            request=request,
            effect_contract=PROCESS_EFFECT_CONTRACT,
            cancel_event=cancel_event,
        )
        self.output_store.assert_instance_held(self.output_store_identity_sha256)
        isolation_enforced = decoder.outcome not in {
            "busy", "unsupported_platform", "boundary_setup_failed",
            "unsafe_effect_contract", "invalid_request",
        } and not (
            decoder.outcome == "cancelled"
            and decoder.detail_code == "cancelled_before_spawn"
        )
        # Durable state always wins over a lost pipe, crash, timeout, or kill.
        try:
            receipt = self._load_exact(lease, binding)
        except MetOfficeRateCompletionError:
            receipt = None
            return MetOfficeRateCompletionProcessResult(
                status="durable_unknown",
                reason_code="durable_output_verification_failed",
                execution_outcome=decoder.outcome,
                output_receipt=None,
                isolation_enforced=isolation_enforced,
                elapsed_seconds=decoder.elapsed_seconds,
                child_exit_code=decoder.child_exit_code,
                child_cpu_seconds=decoder.child_cpu_seconds,
                child_peak_rss_bytes=decoder.child_peak_rss_bytes,
                child_peak_boundary_bytes=decoder.child_peak_boundary_bytes,
            )
        if receipt is not None:
            return MetOfficeRateCompletionProcessResult(
                status="committed",
                reason_code="durable_output_reconciled_after_child",
                execution_outcome=decoder.outcome,
                output_receipt=receipt,
                isolation_enforced=True,
                elapsed_seconds=decoder.elapsed_seconds,
                child_exit_code=decoder.child_exit_code,
                child_cpu_seconds=decoder.child_cpu_seconds,
                child_peak_rss_bytes=decoder.child_peak_rss_bytes,
                child_peak_boundary_bytes=decoder.child_peak_boundary_bytes,
            )
        status, reason = _technical_status(decoder)
        return MetOfficeRateCompletionProcessResult(
            status=status,
            reason_code=reason,
            execution_outcome=decoder.outcome,
            output_receipt=None,
            isolation_enforced=isolation_enforced,
            elapsed_seconds=decoder.elapsed_seconds,
            child_exit_code=decoder.child_exit_code,
            child_cpu_seconds=decoder.child_cpu_seconds,
            child_peak_rss_bytes=decoder.child_peak_rss_bytes,
            child_peak_boundary_bytes=decoder.child_peak_boundary_bytes,
        )


class MetOfficeRateCompletionConsumer:
    """Lease -> hard child -> durable receipt -> lifecycle ack coordinator."""

    def __init__(
        self,
        *,
        lifecycle_store: AcquisitionEvidenceLifecycleStore,
        desired_state_store: DesiredStateStore,
        runner: MetOfficeRateCompletionIsolatedRunner,
        clock: Callable[[], float] = time.time,
        fault_injector: Callable[[str], None] | None = None,
    ) -> None:
        if not isinstance(lifecycle_store, AcquisitionEvidenceLifecycleStore):
            raise TypeError("lifecycle_store has wrong type")
        if not isinstance(desired_state_store, DesiredStateStore):
            raise TypeError("desired_state_store has wrong type")
        if not isinstance(runner, MetOfficeRateCompletionIsolatedRunner):
            raise TypeError("runner has wrong type")
        if not callable(clock):
            raise TypeError("consumer clock must be callable")
        if fault_injector is not None and not callable(fault_injector):
            raise TypeError("consumer fault injector must be callable")
        self.lifecycle_store = lifecycle_store
        self.desired_state_store = desired_state_store
        self.runner = runner
        self._clock = clock
        self._fault_injector = fault_injector

    def _fault(self, point: str) -> None:
        if self._fault_injector is not None:
            self._fault_injector(point)

    @staticmethod
    def _validate_consumption_receipt(
        receipt: ConsumptionReceipt,
        lease: CompletionLease,
        *,
        expected_output_sha256: str | None,
    ) -> None:
        if not isinstance(receipt, ConsumptionReceipt):
            raise MetOfficeRateCompletionIntegrityError(
                "lifecycle terminalization returned no receipt"
            )
        if (
            receipt.event_id != lease.event_id
            or receipt.event_sha256 != lease.event_sha256
            or receipt.producer_receipt_sha256 != lease.producer_receipt_sha256
            or receipt.consumer_id != lease.consumer_id
            or receipt.stream_id != lease.stream_id
            or receipt.stream_sequence != lease.stream_sequence
            or not isinstance(receipt.output_sha256, str)
            or _SHA.fullmatch(receipt.output_sha256) is None
            or not isinstance(receipt.receipt_sha256, str)
            or _SHA.fullmatch(receipt.receipt_sha256) is None
            or (
                expected_output_sha256 is not None
                and receipt.output_sha256 != expected_output_sha256
            )
        ):
            raise MetOfficeRateCompletionIntegrityError(
                "lifecycle terminal receipt differs"
            )

    def _reject_obsolete(
        self,
        lease: CompletionLease,
        *,
        active_desired: DesiredState | None,
        reason_code: str,
        now: float,
    ) -> MetOfficeRateCompletionRunResult:
        rejection_reason = (
            "active_desired_state_unavailable"
            if active_desired is None
            else reason_code
        )
        receipt = self.lifecycle_store.reject_leased(
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
            receipt, lease, expected_output_sha256=None
        )
        return MetOfficeRateCompletionRunResult(
            status="rejected",
            reason_code=rejection_reason,
            event_id=lease.event_id,
            consumption_receipt_sha256=receipt.receipt_sha256,
        )

    def process_leased(
        self,
        lease: CompletionLease,
        *,
        cancel_event: threading.Event | None = None,
    ) -> MetOfficeRateCompletionRunResult:
        if not isinstance(lease, CompletionLease):
            raise TypeError("lease must be CompletionLease")
        cancellation = cancel_event or threading.Event()
        if not callable(getattr(cancellation, "is_set", None)):
            raise TypeError("cancel_event must expose is_set()")
        now = _timestamp(self._clock(), "consumer clock")
        if cancellation.is_set():
            raise MetOfficeRateCompletionCancelled("completion cancelled before validation")
        reloaded_initial = self.lifecycle_store.load_leased(
            lease.event_id, lease.lease_token
        )
        if reloaded_initial != lease:
            raise MetOfficeRateCompletionConflict(
                "lifecycle lease changed before validation"
            )
        if lease.lease_expires_at - now < self.runner.minimum_lease_seconds:
            raise MetOfficeRateCompletionDeadlineError(
                "completion lease cannot contain hard process deadline"
            )
        desired = self.desired_state_store.load_active()
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
        binding = validate_met_office_rate_completion_lease(
            lease, desired_state=desired, raw_store=self.runner.raw_store
        )
        process = self.runner.execute(
            lease=lease, desired_state=desired, cancel_event=cancellation
        )
        if process.status != "committed" or process.output_receipt is None:
            return MetOfficeRateCompletionRunResult(
                status=process.status,
                reason_code=process.reason_code,
                event_id=lease.event_id,
            )
        output = process.output_receipt
        # Fault point proves a committed output can survive a coordinator crash.
        self._fault("consumer.after_output_commit")
        if cancellation.is_set():
            raise MetOfficeRateCompletionCancelled(
                "completion cancelled after durable output and before ack"
            )
        now = _timestamp(self._clock(), "consumer post-process clock")
        if now >= lease.lease_expires_at:
            raise MetOfficeRateCompletionDeadlineError(
                "completion lease expired after durable output and before ack"
            )
        reloaded = self.lifecycle_store.load_leased(lease.event_id, lease.lease_token)
        if reloaded != lease:
            raise MetOfficeRateCompletionConflict("lifecycle lease changed before ack")
        active_after = self.desired_state_store.load_active()
        if (
            active_after is None
            or active_after.state_sha256 != desired.state_sha256
        ):
            return self._reject_obsolete(
                reloaded,
                active_desired=active_after,
                reason_code="desired_state_changed_after_processing",
                now=now,
            )
        verified = self.runner._load_exact(reloaded, binding)
        if verified is None or (
            verified.output_sha256 != output.output_sha256
            or verified.receipt_sha256 != output.receipt_sha256
            or verified.process_receipt_sha256 != output.process_receipt_sha256
        ):
            raise MetOfficeRateCompletionIntegrityError(
                "durable output changed before lifecycle ack"
            )
        expected_output_identity = self.runner.output_store.assert_instance_held(
            self.runner.output_store_identity_sha256
        )
        with self.runner.output_store.completion_reproof_lease(
            lease=lease,
            binding=binding,
            isolation_authority_sha256=self.runner.authority_sha256,
            isolation_limits_sha256=self.runner.limits.fingerprint,
            output_sha256=verified.output_sha256,
            output_receipt_sha256=verified.receipt_sha256,
            process_receipt_sha256=verified.process_receipt_sha256,
            expected_identity_sha256=expected_output_identity,
        ) as final_verified:
            if final_verified != verified:
                raise MetOfficeRateCompletionIntegrityError(
                    "fenced output differs at lifecycle ack boundary"
                )
            self._fault("consumer.before_lifecycle_ack")
            self.runner.output_store._verify_path()
            if cancellation.is_set():
                raise MetOfficeRateCompletionCancelled(
                    "completion cancelled at lifecycle ack boundary"
                )
            now = _timestamp(self._clock(), "consumer final ack clock")
            if now >= lease.lease_expires_at:
                raise MetOfficeRateCompletionDeadlineError(
                    "completion lease expired at lifecycle ack boundary"
                )
            final_lease = self.lifecycle_store.load_leased(
                lease.event_id, lease.lease_token
            )
            if final_lease != lease:
                raise MetOfficeRateCompletionConflict(
                    "lifecycle lease changed at ack boundary"
                )
            active_before_ack = self.desired_state_store.load_active()
            if (
                active_before_ack is None
                or active_before_ack.state_sha256 != desired.state_sha256
            ):
                return self._reject_obsolete(
                    final_lease,
                    active_desired=active_before_ack,
                    reason_code="desired_state_changed_before_ack",
                    now=now,
                )
            self.runner._reprove_decoder_authorities(final_verified, binding)
            if cancellation.is_set():
                raise MetOfficeRateCompletionCancelled(
                    "completion cancelled after final authority reproof"
                )
            now = _timestamp(self._clock(), "consumer post-reproof ack clock")
            if now >= lease.lease_expires_at:
                raise MetOfficeRateCompletionDeadlineError(
                    "completion lease expired after final authority reproof"
                )
            final_lease = self.lifecycle_store.load_leased(
                lease.event_id, lease.lease_token
            )
            if final_lease != lease:
                raise MetOfficeRateCompletionConflict(
                    "lifecycle lease changed after final authority reproof"
                )
            active_before_ack = self.desired_state_store.load_active()
            if (
                active_before_ack is None
                or active_before_ack.state_sha256 != desired.state_sha256
            ):
                return self._reject_obsolete(
                    final_lease,
                    active_desired=active_before_ack,
                    reason_code="desired_state_changed_before_ack",
                    now=now,
                )
            self.runner.output_store._verify_path()
            consumption = self.lifecycle_store.acknowledge(
                event_id=final_lease.event_id,
                lease_token=final_lease.lease_token,
                output_sha256=final_verified.output_sha256,
                now=now,
            )
            self._validate_consumption_receipt(
                consumption,
                final_lease,
                expected_output_sha256=final_verified.output_sha256,
            )
            self._fault("consumer.after_lifecycle_ack")
            return MetOfficeRateCompletionRunResult(
                status="acknowledged",
                reason_code="durable_shadow_output_acknowledged",
                event_id=lease.event_id,
                output_sha256=final_verified.output_sha256,
                output_receipt_sha256=final_verified.receipt_sha256,
                process_receipt_sha256=final_verified.process_receipt_sha256,
                consumption_receipt_sha256=consumption.receipt_sha256,
            )

    def run_once(
        self,
        *,
        owner_id: str,
        lease_seconds: float | None = None,
        cancel_event: threading.Event | None = None,
    ) -> MetOfficeRateCompletionRunResult:
        owner = _identifier(owner_id, "consumer owner ID")
        duration = (
            self.runner.minimum_lease_seconds
            if lease_seconds is None
            else float(lease_seconds)
        )
        if (
            not math.isfinite(duration)
            or duration < self.runner.minimum_lease_seconds
            or duration > 3600.0
        ):
            raise MetOfficeRateCompletionDeadlineError(
                "requested lifecycle lease cannot contain hard process deadline"
            )
        cancellation = cancel_event or threading.Event()
        if cancellation.is_set():
            raise MetOfficeRateCompletionCancelled("completion cancelled before claim")
        now = _timestamp(self._clock(), "consumer claim clock")
        lease = self.lifecycle_store.claim_next(
            CONSUMER_ID,
            STREAM_ID,
            owner,
            lease_seconds=duration,
            now=now,
        )
        if lease is None:
            return MetOfficeRateCompletionRunResult("idle", "no_ready_completion")
        return self.process_leased(lease, cancel_event=cancellation)

    def diagnostics(self) -> dict[str, Any]:
        output = self.runner.output_store.diagnostics()
        result = {
            "consumer_id": CONSUMER_ID,
            "hard_process_effect_contract_sha256": PROCESS_EFFECT_CONTRACT_SHA256,
            "hard_process_limits_sha256": self.runner.limits.fingerprint,
            "output": output,
            "runtime_wiring": "inert",
            "stream_id": STREAM_ID,
            "workload_id": WORKLOAD_ID,
        }
        _assert_public_document(result)
        if len(_canonical(result)) > MAX_DIAGNOSTIC_BYTES:
            raise MetOfficeRateCompletionCapacityError("consumer diagnostics exceed cap")
        return result


__all__ = [
    "CONSUMER_ID",
    "IMPLEMENTATION_ID",
    "LEGACY_OUTPUT_STORE_CONTRACT",
    "LEGACY_SCHEMA_VERSION",
    "MetOfficeRateCompletionBinding",
    "MetOfficeRateCompletionCancelled",
    "MetOfficeRateCompletionCapacityError",
    "MetOfficeRateCompletionConflict",
    "MetOfficeRateCompletionConsumer",
    "MetOfficeRateCompletionDeadlineError",
    "MetOfficeRateCompletionError",
    "MetOfficeRateCompletionIntegrityError",
    "MetOfficeRateCompletionIsolatedRunner",
    "MetOfficeRateCompletionOutputLimits",
    "MetOfficeRateCompletionOutputReceipt",
    "MetOfficeRateCompletionOutputStore",
    "MetOfficeRateCompletionProcessLimits",
    "MetOfficeRateCompletionProcessResult",
    "MetOfficeRateCompletionRunResult",
    "OUTPUT_DOCUMENT_SCHEMA",
    "OUTPUT_MODE",
    "OUTPUT_RECEIPT_SCHEMA",
    "OUTPUT_STORE_IDENTITY_SCHEMA",
    "OUTPUT_STORE_CONTRACT",
    "PROCESS_EFFECT_CONTRACT_SHA256",
    "PROCESS_RECEIPT_SCHEMA",
    "SCHEMA_VERSION",
    "STREAM_ID",
    "WORKLOAD_ID",
    "validate_met_office_rate_completion_lease",
]
