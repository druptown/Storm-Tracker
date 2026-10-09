"""Inert, crash-durable METAR canonical-ground completion consumer.

This module consumes only the manifest-reference events emitted for
``ground.metar.global`` by :mod:`terminal_raw_completion_router`.  It re-proves
the desired-state/job/capture authorities, invokes the existing canonical-ground
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
from .canonical_ground_evidence_store import (
    CanonicalGroundEvidenceError,
    CanonicalGroundEvidenceStore,
    GroundEvidenceLimits,
    GroundObservationReceipt,
)
from .desired_state import DesiredState, DesiredStateStore, parse_desired_state
from .hard_decoder_isolation import (
    DecoderEffectContract,
    DecoderProcessResult,
    DecoderResourceLimits,
    HardDecoderExecutor,
)
from .metar_ground_evidence import (
    DEFAULT_CONFIG_SHA256 as DECODER_CONFIG_SHA256,
    DEFAULT_DECODE_SECONDS,
    DECODER_IDENTITY,
    DECODER_VERSION,
    MetarGroundDecodeLimits,
    decode_metar_ground_capture,
)
from .metar_native import METAR_CAPTURE_PRODUCT as CAPTURE_PRODUCT
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


WORKLOAD_ID = "ground.metar.global"
STREAM_ID = WORKLOAD_ID
CONSUMER_ID = "metar-canonical-ground-decoder-reference-v1"
IMPLEMENTATION_ID = "metar_canonical_ground_decoder_v1"
EXPECTED_ROLES = ("csv_gzip",)
PROVIDER = "metar"

LEGACY_OUTPUT_STORE_CONTRACT = "stv3-metar-ground-completion-output-store-v1"
OUTPUT_STORE_CONTRACT = "stv3-metar-ground-completion-output-store-v2"
OUTPUT_STORE_IDENTITY_SCHEMA = (
    "stv3-metar-ground-completion-output-store-identity-v1"
)
OUTPUT_DOCUMENT_SCHEMA = "stv3-metar-ground-completion-output-v1"
OUTPUT_RECEIPT_SCHEMA = "stv3-metar-ground-completion-output-receipt-v1"
PROCESS_RECEIPT_SCHEMA = "stv3-metar-ground-hard-process-receipt-v1"
PROCESS_AUTHORITY_SCHEMA = "stv3-metar-ground-hard-process-authority-v1"
PROCESS_REQUEST_SCHEMA = "stv3-metar-ground-hard-process-request-v1"
PROCESS_CHILD_RESULT_SCHEMA = "stv3-metar-ground-hard-process-child-result-v1"
FACTSET_DIGEST_SCHEMA = "stv3-metar-ground-canonical-factset-digest-v1"
FACTSET_RECEIPT_SCHEMA = "stv3-metar-ground-canonical-factset-receipt-v1"
PROCESS_TARGET = (
    "storm_tracker_worker.metar_ground_completion_consumer:"
    "_metar_ground_completion_child"
)
OUTPUT_MODE = "shadow_inert"
LEGACY_SCHEMA_VERSION = 1
SCHEMA_VERSION = 2
MAX_DIAGNOSTIC_BYTES = 4096

PROCESS_EFFECT_CONTRACT = DecoderEffectContract(
    contract_id="metar-ground-completion.idempotent-receipts.v1",
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


class MetarGroundCompletionError(RuntimeError):
    """Base error for the inert METAR completion boundary."""


class MetarGroundCompletionIntegrityError(MetarGroundCompletionError):
    """Durable authorities, bytes, or checksums differ."""


class MetarGroundCompletionConflict(MetarGroundCompletionError):
    """An immutable identity was reused for a different result."""


class MetarGroundCompletionCapacityError(MetarGroundCompletionError):
    """A hard receipt, database, process, or payload bound was reached."""


class MetarGroundCompletionDeadlineError(MetarGroundCompletionError):
    """The lifecycle lease cannot contain the hard process deadline."""


class MetarGroundCompletionCancelled(MetarGroundCompletionError):
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
        raise MetarGroundCompletionIntegrityError("value is not canonical JSON") from exc


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
        raise MetarGroundCompletionIntegrityError("value is not canonical UTF-8 JSON") from exc


def _sha(value: object, label: str) -> str:
    if not isinstance(value, str) or _SHA.fullmatch(value) is None:
        raise MetarGroundCompletionIntegrityError(f"{label} is not SHA-256")
    return value


def _identifier(value: object, label: str) -> str:
    if not isinstance(value, str) or _ID.fullmatch(value) is None:
        raise MetarGroundCompletionIntegrityError(f"{label} is invalid")
    return value


def _integer(value: object, label: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise MetarGroundCompletionIntegrityError(f"{label} is invalid")
    return value


def _timestamp(value: object, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise MetarGroundCompletionIntegrityError(f"{label} is invalid")
    result = float(value)
    if not math.isfinite(result) or not 0.0 <= result <= 2**53:
        raise MetarGroundCompletionIntegrityError(f"{label} is invalid")
    return result


def _exact(value: object, keys: set[str], label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or set(value) != keys:
        raise MetarGroundCompletionIntegrityError(f"{label} schema differs")
    if any(not isinstance(key, str) for key in value):
        raise MetarGroundCompletionIntegrityError(f"{label} has a non-text key")
    return value


def _reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise MetarGroundCompletionIntegrityError("reference JSON has duplicate keys")
        result[key] = value
    return result


def _decode_reference(payload: bytes) -> dict[str, Any]:
    if not isinstance(payload, bytes) or not payload or len(payload) > 64 * 1024:
        raise MetarGroundCompletionCapacityError("reference payload exceeds its cap")
    try:
        value = json.loads(
            payload.decode("ascii"),
            object_pairs_hook=_reject_duplicates,
            parse_constant=lambda token: (_ for _ in ()).throw(
                MetarGroundCompletionIntegrityError(
                    f"reference JSON has non-finite number {token}"
                )
            ),
        )
    except MetarGroundCompletionError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MetarGroundCompletionIntegrityError("reference payload is invalid JSON") from exc
    if not isinstance(value, dict) or _canonical(value) != payload:
        raise MetarGroundCompletionIntegrityError("reference payload is not canonical")
    return value


def _assert_public_document(value: Any, *, depth: int = 0) -> None:
    if depth > 20:
        raise MetarGroundCompletionCapacityError("document nesting exceeds cap")
    if isinstance(value, Mapping):
        if len(value) > 256:
            raise MetarGroundCompletionCapacityError("document object exceeds cap")
        for key, item in value.items():
            if not isinstance(key, str) or _SECRET_KEY.search(key):
                raise MetarGroundCompletionIntegrityError(
                    "document contains a credential-like key"
                )
            _assert_public_document(item, depth=depth + 1)
        return
    if isinstance(value, (list, tuple)):
        if len(value) > 256:
            raise MetarGroundCompletionCapacityError("document sequence exceeds cap")
        for item in value:
            _assert_public_document(item, depth=depth + 1)
        return
    if isinstance(value, str):
        if len(value.encode("utf-8")) > 4096 or _PRIVATE_TEXT.search(value):
            raise MetarGroundCompletionIntegrityError(
                "document contains private capability text"
            )
        return
    if value is None or isinstance(value, (bool, int, float)):
        if isinstance(value, float) and not math.isfinite(value):
            raise MetarGroundCompletionIntegrityError("document has non-finite number")
        return
    raise MetarGroundCompletionIntegrityError("document has unsupported value")


def _effect_mapping() -> dict[str, Any]:
    return {
        "contract_id": PROCESS_EFFECT_CONTRACT.contract_id,
        "durable_effects": PROCESS_EFFECT_CONTRACT.durable_effects,
        "hard_kill_safe": PROCESS_EFFECT_CONTRACT.hard_kill_safe,
        "spawns_descendants": PROCESS_EFFECT_CONTRACT.spawns_descendants,
    }


PROCESS_EFFECT_CONTRACT_SHA256 = _digest(_effect_mapping())


@dataclass(frozen=True, slots=True)
class MetarGroundCompletionOutputLimits:
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
            "schema": "stv3-metar-ground-completion-output-limits-v1",
        })


@dataclass(frozen=True, slots=True)
class MetarGroundCompletionProcessLimits:
    wall_seconds: float = 90.0
    cpu_seconds: float = 60.0
    rss_bytes: int = 1024 * 1024 * 1024
    max_request_bytes: int = 512 * 1024
    max_result_bytes: int = 16 * 1024
    factset_reproof_seconds: float = 45.0

    def __post_init__(self) -> None:
        DecoderResourceLimits(
            wall_seconds=self.wall_seconds,
            cpu_seconds=self.cpu_seconds,
            memory_bytes=self.rss_bytes,
            max_request_bytes=self.max_request_bytes,
            max_result_bytes=self.max_result_bytes,
        )
        value = self.factset_reproof_seconds
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(float(value))
            or not 0.05 <= float(value) <= 120.0
            or float(value) >= float(self.wall_seconds)
        ):
            raise ValueError("factset_reproof_seconds is outside its hard boundary")

    def to_mapping(self) -> dict[str, int | float]:
        return {
            "cpu_seconds": float(self.cpu_seconds),
            "factset_reproof_seconds": float(self.factset_reproof_seconds),
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
            "schema": "stv3-metar-ground-hard-process-limits-v1",
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
class MetarGroundCompletionBinding:
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
class MetarGroundCompletionOutputReceipt:
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
    factset_id: str
    factset_receipt_sha256: str
    factset_sha256: str
    fact_count: int
    isolation_authority_sha256: str
    isolation_limits_sha256: str
    created_at: float
    output: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class MetarGroundCompletionProcessResult:
    status: str
    reason_code: str
    execution_outcome: str
    output_receipt: MetarGroundCompletionOutputReceipt | None
    isolation_enforced: bool
    elapsed_seconds: float
    child_exit_code: int | None = None
    child_cpu_seconds: float | None = None
    child_peak_rss_bytes: int | None = None
    child_peak_boundary_bytes: int | None = None


@dataclass(frozen=True, slots=True)
class MetarGroundCompletionRunResult:
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
        raise MetarGroundCompletionIntegrityError("METAR consumer catalog binding drifted")
    return binding


def _metar_recovery_binding(
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
        raise MetarGroundCompletionIntegrityError("METAR activation binding drifted")
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
        raise MetarGroundCompletionIntegrityError("METAR raw recovery binding drifted")
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
        raise MetarGroundCompletionIntegrityError("scheduler proof is invalid") from exc
    if proof.as_dict() != body or not hmac.compare_digest(proof.proof_sha256, proof_sha):
        raise MetarGroundCompletionIntegrityError("scheduler proof checksum differs")
    if (
        proof.job_type != JOB_TYPE
        or proof.state != "completed"
        or proof.last_transition != "completed"
        or proof.last_error_code is not None
        or proof.attempt_count < 1
        or proof.terminal_at < proof.scheduled_for
    ):
        raise MetarGroundCompletionIntegrityError("scheduler terminal is not completed")
    return proof


def _validate_execution(value: object) -> ExecutionReceipt:
    expected_keys = set(ExecutionReceipt.__dataclass_fields__) | {"contract"}
    mapping = _exact(value, expected_keys, "execution receipt")
    if mapping["contract"] != EXECUTION_CONTRACT:
        raise MetarGroundCompletionIntegrityError("execution contract differs")
    body = {key: mapping[key] for key in ExecutionReceipt.__dataclass_fields__}
    try:
        receipt = ExecutionReceipt(**body)
    except (TypeError, ValueError) as exc:
        raise MetarGroundCompletionIntegrityError("execution receipt is invalid") from exc
    unhashed = receipt.as_dict()
    unhashed.pop("receipt_sha256")
    if (
        receipt.as_dict() != dict(mapping)
        or not hmac.compare_digest(_digest(unhashed), receipt.receipt_sha256)
        or not receipt.terminal
        or receipt.outcome != "completed_shadow_raw"
        or receipt.reason_code != "shadow_raw_capture_complete"
    ):
        raise MetarGroundCompletionIntegrityError("execution receipt checksum differs")
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
        raise MetarGroundCompletionIntegrityError("completion event identity differs")


def _stable_decoder_ids(lease: CompletionLease, capture_id: str) -> tuple[str, str]:
    seed = {
        "capture_id": capture_id,
        "consumer_id": CONSUMER_ID,
        "decoder_config_sha256": DECODER_CONFIG_SHA256,
        "event_id": lease.event_id,
        "event_sha256": lease.event_sha256,
        "producer_receipt_sha256": lease.producer_receipt_sha256,
        "schema": "stv3-metar-ground-decoder-identity-v1",
    }
    job = "decoder-job-" + hashlib.sha256(_canonical(seed)).hexdigest()
    attempt = "decoder-attempt-" + hashlib.sha256(
        _canonical({**seed, "schema": "stv3-metar-ground-decoder-attempt-v1"})
    ).hexdigest()
    return job, attempt


def validate_metar_ground_completion_lease(
    lease: CompletionLease,
    *,
    desired_state: DesiredState,
    raw_store: RawArtifactStore,
) -> MetarGroundCompletionBinding:
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
        raise MetarGroundCompletionIntegrityError("completion lease route differs")
    _event_identity(lease)
    if (
        desired_state.state_sha256 != lease.desired_state_sha256
        or desired_state.state_sha256 != _sha(
            lease.desired_state_sha256, "lease desired SHA-256"
        )
    ):
        raise MetarGroundCompletionConflict("active desired state differs from completion")
    authority = desired_authority_proof(desired_state)
    if (
        authority.get("authority_sha256") != lease.authority_sha256
        or _digest({key: authority[key] for key in authority if key != "authority_sha256"})
        != lease.authority_sha256
    ):
        raise MetarGroundCompletionIntegrityError("desired authority differs")

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
        raise MetarGroundCompletionIntegrityError("reference route authority differs")
    claims = _exact(payload["claims"], _CLAIM_KEYS, "reference claims")
    if any(value is not False for value in claims.values()) or proof["claims"] != claims:
        raise MetarGroundCompletionIntegrityError("reference makes an operational claim")
    capture_ref = _exact(payload["capture_reference"], _CAPTURE_KEYS, "capture reference")
    if proof["capture_reference"] != capture_ref:
        raise MetarGroundCompletionIntegrityError("proof capture reference differs")

    scheduler = _validate_scheduler_proof(proof["scheduler_terminal"])
    if scheduler.desired_state_sha256 != desired_state.state_sha256:
        raise MetarGroundCompletionIntegrityError("scheduler desired authority differs")
    plan = validate_worker_acquisition_plan(build_worker_acquisition_plan(desired_state))
    try:
        job = validate_worker_acquisition_job(reconstruct_acquisition_job_for_slot(
            plan,
            intent_id=scheduler.intent_id,
            scheduled_for=scheduler.scheduled_for,
        ))
    except Exception as exc:
        raise MetarGroundCompletionIntegrityError("acquisition slot cannot be reconstructed") from exc
    schedule_matches = tuple(
        item
        for item in build_acquisition_schedule_intents(plan)
        if item.intent_id == scheduler.intent_id
    )
    if len(schedule_matches) != 1:
        raise MetarGroundCompletionIntegrityError("scheduler intent is not singular")
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
        raise MetarGroundCompletionIntegrityError("scheduler slot authority differs")
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
        raise MetarGroundCompletionIntegrityError("acquisition job binding differs")
    _rule, recovery = _metar_recovery_binding(job)
    if proof["recovery_binding_sha256"] != recovery_binding_sha256(recovery):
        raise MetarGroundCompletionIntegrityError("recovery binding checksum differs")

    execution = _validate_execution(proof["execution_receipt"])
    if (
        execution.scheduler_job_id != scheduler.job_id
        or execution.acquisition_job_id != job.job_id
        or execution.attempt_count != scheduler.attempt_count
    ):
        raise MetarGroundCompletionIntegrityError("execution scheduler binding differs")

    if (
        capture_ref["capture_schema"] != RAW_ARTIFACT_CAPTURE_SCHEMA
        or capture_ref["capture_receipt_schema"] != RAW_ARTIFACT_CAPTURE_RECEIPT_SCHEMA
        or capture_ref["raw_store_schema_version"] != RAW_ARTIFACT_SCHEMA_VERSION
        or capture_ref["provider"] != recovery.raw_provider
        or capture_ref["product"] != recovery.capture_product
    ):
        raise MetarGroundCompletionIntegrityError("capture contract differs")
    capture_id = _identifier(capture_ref["capture_id"], "capture ID")
    expected_capture_id = RawArtifactStore._capture_id(  # noqa: SLF001
        provider=recovery.raw_provider,
        product=recovery.capture_product,
        fetch_id=f"{job.job_id}{recovery.capture_suffix}",
    )
    if capture_id != expected_capture_id:
        raise MetarGroundCompletionIntegrityError("capture ID changed job identity")
    try:
        loaded = raw_store.load_capture_bundle_bytes(capture_id)
    except RawArtifactError as exc:
        raise MetarGroundCompletionIntegrityError(
            "referenced raw capture failed verification"
        ) from exc
    if loaded is None:
        raise MetarGroundCompletionIntegrityError("referenced raw capture is unavailable")
    capture, receipts, payloads = loaded
    expected_roles = set(EXPECTED_ROLES)
    if set(receipts) != expected_roles or set(payloads) != expected_roles:
        raise MetarGroundCompletionIntegrityError("METAR capture role set differs")
    members_value = capture_ref["members"]
    if (
        not isinstance(members_value, list)
        or len(members_value) != len(EXPECTED_ROLES)
    ):
        raise MetarGroundCompletionIntegrityError("capture member reference differs")
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
                raise MetarGroundCompletionIntegrityError(
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
            raise MetarGroundCompletionIntegrityError(
                "raw capture changed acquisition job identity"
            )
    except CaptureRecoveryError as exc:
        raise MetarGroundCompletionIntegrityError(
            "METAR raw provenance failed verification"
        ) from exc
    try:
        evidence = recovery_evidence_digest(
            binding=recovery, receipts_by_role=receipts, capture=capture
        )
    except CaptureRecoveryError as exc:
        raise MetarGroundCompletionIntegrityError(
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
        raise MetarGroundCompletionIntegrityError("capture manifest reference differs")

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
        raise MetarGroundCompletionIntegrityError("capacity/execution/raw evidence differs")
    evaluated_at = _timestamp(execution.created_at, "decoder evaluation time")
    if evaluated_at < received_at:
        raise MetarGroundCompletionIntegrityError("decoder evaluation predates raw bytes")
    decoder_job_id, attempt_id = _stable_decoder_ids(lease, capture_id)
    return MetarGroundCompletionBinding(
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
    }, "METAR ground output"))
    if (
        document["schema"] != OUTPUT_DOCUMENT_SCHEMA
        or document["mode"] != OUTPUT_MODE
        or document["workload_id"] != WORKLOAD_ID
        or document["consumer_id"] != CONSUMER_ID
        or document["stream_id"] != STREAM_ID
        or document["fact_revision"] is not None
        or document["disposition"] != "shadow_evidence_committed"
    ):
        raise MetarGroundCompletionIntegrityError("METAR ground output route differs")
    event = _exact(document["event"], {
        "event_id", "event_sha256", "producer_receipt_sha256", "stream_sequence",
    }, "output event")
    if _EVENT_ID.fullmatch(_identifier(event["event_id"], "output event ID")) is None:
        raise MetarGroundCompletionIntegrityError("output event ID differs")
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
        "attempt_id", "config_sha256", "factset", "identity", "job_id", "version",
    }, "output decoder")
    if (
        decoder["identity"] != DECODER_IDENTITY
        or decoder["version"] != DECODER_VERSION
        or decoder["config_sha256"] != DECODER_CONFIG_SHA256
        or not isinstance(decoder["job_id"], str)
        or re.fullmatch(r"decoder-job-[0-9a-f]{64}", decoder["job_id"]) is None
        or not isinstance(decoder["attempt_id"], str)
        or re.fullmatch(r"decoder-attempt-[0-9a-f]{64}", decoder["attempt_id"]) is None
    ):
        raise MetarGroundCompletionIntegrityError("output decoder binding differs")
    factset = _exact(decoder["factset"], {
        "capture_id", "capture_manifest_sha256", "decoder_config_sha256",
        "decoder_identity", "decoder_version", "fact_count", "factset_id",
        "factset_sha256", "receipt_sha256", "schema",
    }, "output factset receipt")
    if (
        factset["schema"] != FACTSET_RECEIPT_SCHEMA
        or factset["capture_id"] != input_value["capture_id"]
        or factset["capture_manifest_sha256"] != input_value["capture_manifest_sha256"]
        or factset["decoder_identity"] != DECODER_IDENTITY
        or factset["decoder_version"] != DECODER_VERSION
        or factset["decoder_config_sha256"] != DECODER_CONFIG_SHA256
        or not isinstance(factset["factset_id"], str)
        or re.fullmatch(r"ground-factset-[0-9a-f]{64}", factset["factset_id"]) is None
        or _integer(factset["fact_count"], "factset count") > MetarGroundDecodeLimits().max_facts
    ):
        raise MetarGroundCompletionIntegrityError("output factset binding differs")
    _sha(factset["factset_sha256"], "factset SHA-256")
    receipt_sha = _sha(factset["receipt_sha256"], "factset receipt SHA-256")
    receipt_body = dict(factset)
    receipt_body.pop("receipt_sha256")
    if not hmac.compare_digest(_digest(receipt_body), receipt_sha):
        raise MetarGroundCompletionIntegrityError("factset receipt checksum differs")
    claims = _exact(document["claims"], {
        "canonical_ground_evidence_committed", "ground_confirmation",
        "meteorological_admission", "operational_fact_published",
        "operational_snapshot_published",
    }, "output claims")
    if (
        claims["canonical_ground_evidence_committed"] is not True
        or claims["ground_confirmation"] is not False
        or claims["meteorological_admission"] is not False
        or claims["operational_fact_published"] is not False
        or claims["operational_snapshot_published"] is not False
    ):
        raise MetarGroundCompletionIntegrityError("output claims differ")
    isolation = _exact(document["isolation"], {
        "authority_sha256", "effect_contract_sha256", "limits_sha256",
        "process_boundary", "runtime_wiring",
    }, "output isolation")
    if (
        isolation["effect_contract_sha256"] != PROCESS_EFFECT_CONTRACT_SHA256
        or isolation["process_boundary"] != "fresh_spawn_hard_cpu_rss_wall_output_caps"
        or isolation["runtime_wiring"] != "inert"
    ):
        raise MetarGroundCompletionIntegrityError("output isolation differs")
    _sha(isolation["authority_sha256"], "isolation authority SHA-256")
    _sha(isolation["limits_sha256"], "isolation limits SHA-256")
    _assert_public_document(document)
    return document, event, authority, input_value, decoder, isolation
class MetarGroundCompletionOutputStore:
    """Bounded append-only SQLite output/process receipt authority."""

    _TABLES = frozenset({"output_metadata", "completion_outputs"})
    _METADATA_COLUMNS = frozenset({
        "singleton", "contract", "schema_version", "limits_sha256",
        "max_receipts", "max_receipt_bytes", "max_total_output_bytes",
        "max_database_bytes", "store_instance_sha256",
    })
    _LEGACY_METADATA_COLUMNS = _METADATA_COLUMNS - {
        "store_instance_sha256"
    }
    _COLUMNS = frozenset({
        "authority_sha256", "capture_id", "capture_manifest_sha256",
        "consumer_id", "created_at", "desired_state_sha256", "event_id",
        "event_sha256", "fact_count", "factset_id",
        "factset_receipt_sha256", "factset_sha256",
        "isolation_authority_sha256", "isolation_effect_contract_sha256",
        "isolation_limits_sha256", "output_bytes", "output_json",
        "output_sha256", "process_receipt_sha256", "producer_receipt_sha256",
        "receipt_sha256", "stream_id",
    })

    def __init__(
        self,
        path: str | Path,
        *,
        limits: MetarGroundCompletionOutputLimits = MetarGroundCompletionOutputLimits(),
        clock: Callable[[], float] = time.time,
        fault_injector: Callable[[str], None] | None = None,
    ) -> None:
        supplied = Path(path)
        if not supplied.is_absolute():
            raise ValueError("METAR completion output path must be absolute")
        self.path = Path(os.path.abspath(os.fspath(supplied)))
        if not isinstance(limits, MetarGroundCompletionOutputLimits):
            raise TypeError("METAR completion output limits have wrong type")
        if not callable(clock):
            raise TypeError("METAR completion output clock must be callable")
        if fault_injector is not None and not callable(fault_injector):
            raise TypeError("METAR completion fault injector must be callable")
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
            raise MetarGroundCompletionIntegrityError(
                "output database witness is unavailable"
            ) from exc
        if (
            path.is_symlink()
            or not stat.S_ISREG(details.st_mode)
            or int(details.st_nlink) != 1
        ):
            raise MetarGroundCompletionIntegrityError(
                "output database path is unsafe"
            )
        return (
            int(details.st_dev),
            int(details.st_ino),
            int(details.st_mode),
        )

    def _prepare_path(self) -> tuple[int, int, int]:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        parent = self.path.parent
        for candidate in (parent, *parent.parents):
            try:
                details = candidate.lstat()
            except OSError as exc:
                raise MetarGroundCompletionIntegrityError(
                    "output database parent is unavailable"
                ) from exc
            if candidate.is_symlink() or not stat.S_ISDIR(details.st_mode):
                raise MetarGroundCompletionIntegrityError(
                    "output database parent is unsafe"
                )
        if self._normalized_path(parent.resolve(strict=True)) != (
            self._normalized_path(parent)
        ):
            raise MetarGroundCompletionIntegrityError(
                "output database parent is aliased"
            )
        if self.path.is_symlink():
            raise MetarGroundCompletionIntegrityError(
                "output database path is unsafe"
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
                    if not stat.S_ISREG(details.st_mode):
                        raise MetarGroundCompletionIntegrityError(
                            "output database leaf is not regular"
                        )
                    os.fsync(descriptor)
                finally:
                    os.close(descriptor)
            except MetarGroundCompletionError:
                raise
            except OSError as exc:
                raise MetarGroundCompletionIntegrityError(
                    "output database leaf creation failed"
                ) from exc
        witness = self._path_witness(self.path)
        if self._normalized_path(self.path.resolve(strict=True)) != (
            self._normalized_path(self.path)
        ):
            raise MetarGroundCompletionIntegrityError(
                "output database path is aliased"
            )
        return witness

    def _verify_path(self) -> None:
        try:
            resolved = self.path.resolve(strict=True)
        except OSError as exc:
            raise MetarGroundCompletionIntegrityError(
                "output database path is unavailable"
            ) from exc
        if (
            self._normalized_path(resolved) != self._normalized_path(self.path)
            or self._path_witness(self.path) != self._database_witness
        ):
            raise MetarGroundCompletionIntegrityError(
                "output database instance changed"
            )

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
            database.execute("PRAGMA journal_mode=DELETE")
            database.execute("PRAGMA synchronous=FULL")
            database.execute("PRAGMA trusted_schema=OFF")
            database.execute("PRAGMA busy_timeout=5000")
            mode = str(
                database.execute("PRAGMA journal_mode").fetchone()[0]
            ).lower()
            if mode != "delete":
                raise MetarGroundCompletionIntegrityError(
                    "output journal mode differs"
                )
            page_size = int(database.execute("PRAGMA page_size").fetchone()[0])
            maximum_pages = max(
                1, self.limits.max_database_bytes // page_size
            )
            configured_pages = int(database.execute(
                f"PRAGMA max_page_count={maximum_pages}"
            ).fetchone()[0])
            current_pages = int(
                database.execute("PRAGMA page_count").fetchone()[0]
            )
            if configured_pages > maximum_pages or current_pages > maximum_pages:
                raise MetarGroundCompletionCapacityError(
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
                "SELECT name FROM sqlite_master "
                "WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            )
        )

    @staticmethod
    def _columns(
        database: sqlite3.Connection, table: str
    ) -> frozenset[str]:
        return frozenset(
            str(row[1])
            for row in database.execute(f"PRAGMA table_info({table})")
        )

    @staticmethod
    def _normalized_sql(value: str) -> str:
        return " ".join(value.split()).casefold()

    @classmethod
    def _legacy_trigger_definitions(cls) -> dict[str, str]:
        return {
            "completion_outputs_no_update": (
                "CREATE TRIGGER completion_outputs_no_update BEFORE UPDATE "
                "ON completion_outputs BEGIN SELECT RAISE(ABORT,'immutable'); "
                "END"
            ),
            "completion_outputs_no_delete": (
                "CREATE TRIGGER completion_outputs_no_delete BEFORE DELETE "
                "ON completion_outputs BEGIN SELECT RAISE(ABORT,'immutable'); "
                "END"
            ),
        }

    @classmethod
    def _trigger_definitions(cls) -> dict[str, str]:
        return {
            **cls._legacy_trigger_definitions(),
            "output_metadata_no_update": (
                "CREATE TRIGGER output_metadata_no_update BEFORE UPDATE ON "
                "output_metadata BEGIN SELECT RAISE(ABORT,"
                "'output metadata is immutable'); END"
            ),
            "output_metadata_no_delete": (
                "CREATE TRIGGER output_metadata_no_delete BEFORE DELETE ON "
                "output_metadata BEGIN SELECT RAISE(ABORT,"
                "'output metadata is immutable'); END"
            ),
        }

    @classmethod
    def _validate_triggers(
        cls, database: sqlite3.Connection, *, legacy: bool = False
    ) -> None:
        rows = database.execute(
            "SELECT name,sql FROM sqlite_master WHERE type='trigger'"
        ).fetchall()
        actual = {str(row[0]): str(row[1]) for row in rows}
        expected = (
            cls._legacy_trigger_definitions()
            if legacy
            else cls._trigger_definitions()
        )
        if set(actual) != set(expected) or any(
            cls._normalized_sql(actual[name])
            != cls._normalized_sql(statement)
            for name, statement in expected.items()
        ):
            raise MetarGroundCompletionIntegrityError(
                "output immutability triggers differ"
            )

    @classmethod
    def _install_metadata_triggers(
        cls, database: sqlite3.Connection
    ) -> None:
        for name in ("output_metadata_no_update", "output_metadata_no_delete"):
            database.execute(cls._trigger_definitions()[name])

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
                factset_id TEXT NOT NULL,
                factset_receipt_sha256 TEXT NOT NULL,
                factset_sha256 TEXT NOT NULL,
                fact_count INTEGER NOT NULL,
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
        instance_sha = secrets.token_hex(32)
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
                instance_sha,
            ),
        )

        for statement in self._trigger_definitions().values():
            database.execute(statement)

    def _validate_legacy_v1_schema(
        self, database: sqlite3.Connection
    ) -> None:
        if self._tables(database) != self._TABLES:
            raise MetarGroundCompletionIntegrityError(
                "legacy output table inventory differs"
            )
        if self._columns(database, "output_metadata") != (
            self._LEGACY_METADATA_COLUMNS
        ) or self._columns(database, "completion_outputs") != self._COLUMNS:
            raise MetarGroundCompletionIntegrityError(
                "legacy output columns differ"
            )
        row = database.execute(
            "SELECT * FROM output_metadata WHERE singleton=1"
        ).fetchone()
        if (
            row is None
            or int(database.execute(
                "SELECT count(*) FROM output_metadata"
            ).fetchone()[0]) != 1
            or str(row["contract"]) != LEGACY_OUTPUT_STORE_CONTRACT
            or int(row["schema_version"]) != LEGACY_SCHEMA_VERSION
            or str(row["limits_sha256"]) != self.limits.fingerprint
            or int(row["max_receipts"]) != self.limits.max_receipts
            or int(row["max_receipt_bytes"]) != self.limits.max_receipt_bytes
            or int(row["max_total_output_bytes"])
            != self.limits.max_total_output_bytes
            or int(row["max_database_bytes"])
            != self.limits.max_database_bytes
        ):
            raise MetarGroundCompletionIntegrityError(
                "legacy output metadata differs"
            )
        self._validate_triggers(database, legacy=True)
        if database.execute("PRAGMA quick_check").fetchone()[0] != "ok":
            raise MetarGroundCompletionIntegrityError(
                "legacy output database integrity check failed"
            )
        for output_row in database.execute("SELECT * FROM completion_outputs"):
            self._from_row(output_row)

    def _is_legacy_v1(self, database: sqlite3.Connection) -> bool:
        if self._tables(database) != self._TABLES:
            return False
        if "store_instance_sha256" in self._columns(
            database, "output_metadata"
        ):
            return False
        row = database.execute(
            "SELECT contract,schema_version FROM output_metadata "
            "WHERE singleton=1"
        ).fetchone()
        return bool(
            row is not None
            and str(row["contract"]) == LEGACY_OUTPUT_STORE_CONTRACT
            and int(row["schema_version"]) == LEGACY_SCHEMA_VERSION
        )

    def _migrate_v1_to_v2(self, database: sqlite3.Connection) -> None:
        self._validate_legacy_v1_schema(database)
        instance_sha = secrets.token_hex(32)
        database.execute(
            "ALTER TABLE output_metadata ADD COLUMN store_instance_sha256 TEXT"
        )
        database.execute(
            "UPDATE output_metadata SET contract=?,schema_version=?,"
            "store_instance_sha256=? WHERE singleton=1",
            (OUTPUT_STORE_CONTRACT, SCHEMA_VERSION, instance_sha),
        )
        self._fault("migration.after_metadata")
        self._install_metadata_triggers(database)
        self._fault("migration.after_triggers")

    def _validate_schema(
        self, database: sqlite3.Connection, *, full: bool = False
    ) -> str:
        if self._tables(database) != self._TABLES:
            raise MetarGroundCompletionIntegrityError("output table inventory differs")
        if self._columns(database, "output_metadata") != self._METADATA_COLUMNS:
            raise MetarGroundCompletionIntegrityError(
                "output metadata columns differ"
            )
        row = database.execute(
            "SELECT * FROM output_metadata WHERE singleton=1"
        ).fetchone()
        if (
            row is None
            or int(database.execute(
                "SELECT count(*) FROM output_metadata"
            ).fetchone()[0]) != 1
            or str(row["contract"]) != OUTPUT_STORE_CONTRACT
            or int(row["schema_version"]) != SCHEMA_VERSION
            or str(row["limits_sha256"]) != self.limits.fingerprint
            or int(row["max_receipts"]) != self.limits.max_receipts
            or int(row["max_receipt_bytes"]) != self.limits.max_receipt_bytes
            or int(row["max_total_output_bytes"]) != self.limits.max_total_output_bytes
            or int(row["max_database_bytes"]) != self.limits.max_database_bytes
        ):
            raise MetarGroundCompletionIntegrityError("output metadata differs")
        instance_sha = _sha(
            row["store_instance_sha256"], "output store instance SHA-256"
        )
        if (
            hasattr(self, "_store_instance_sha256")
            and not hmac.compare_digest(
                self._store_instance_sha256, instance_sha
            )
        ):
            raise MetarGroundCompletionIntegrityError(
                "output store instance authority changed"
            )
        if self._columns(database, "completion_outputs") != self._COLUMNS:
            raise MetarGroundCompletionIntegrityError("output columns differ")
        self._validate_triggers(database)
        if full and database.execute("PRAGMA quick_check").fetchone()[0] != "ok":
            raise MetarGroundCompletionIntegrityError(
                "output database integrity check failed"
            )
        if full:
            for output_row in database.execute(
                "SELECT * FROM completion_outputs"
            ):
                self._from_row(output_row)
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
                raise MetarGroundCompletionIntegrityError(
                    "output database is invalid"
                ) from exc

    @property
    def store_instance_sha256(self) -> str:
        self._verify_path()
        with self._lock, closing(self._connect()) as database:
            current = self._validate_schema(database)
        if not hmac.compare_digest(current, self._store_instance_sha256):
            raise MetarGroundCompletionIntegrityError(
                "output store instance authority changed"
            )
        return self._store_instance_sha256

    @property
    def identity_sha256(self) -> str:
        return _digest({
            "contract": OUTPUT_STORE_CONTRACT,
            "file_witness": {
                "st_dev": self._database_witness[0],
                "st_ino": self._database_witness[1],
                "st_mode": self._database_witness[2],
            },
            "limits_sha256": self.limits.fingerprint,
            "path_sha256": hashlib.sha256(
                self._normalized_path(self.path).encode("utf-8")
            ).hexdigest(),
            "schema": OUTPUT_STORE_IDENTITY_SCHEMA,
            "schema_version": SCHEMA_VERSION,
            "store_instance_sha256": self.store_instance_sha256,
        })

    def assert_instance_held(
        self, expected_identity_sha256: str | None = None
    ) -> str:
        identity = self.identity_sha256
        if expected_identity_sha256 is not None and not hmac.compare_digest(
            identity,
            _sha(
                expected_identity_sha256,
                "expected output store identity SHA-256",
            ),
        ):
            raise MetarGroundCompletionIntegrityError(
                "output store identity authority differs"
            )
        return identity

    def _from_row(self, row: sqlite3.Row) -> MetarGroundCompletionOutputReceipt:
        output_bytes = bytes(row["output_json"])
        if (
            len(output_bytes) != int(row["output_bytes"])
            or not 0 < len(output_bytes) <= self.limits.max_receipt_bytes
        ):
            raise MetarGroundCompletionIntegrityError("stored output size differs")
        try:
            raw = json.loads(
                output_bytes.decode("ascii"), object_pairs_hook=_reject_duplicates
            )
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise MetarGroundCompletionIntegrityError(
                "stored output JSON is corrupt"
            ) from exc
        document, event, authority, input_value, decoder, isolation = (
            _validated_output_document(raw)
        )
        if _canonical(document) != output_bytes:
            raise MetarGroundCompletionIntegrityError(
                "stored output is not canonical"
            )
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
        factset = decoder["factset"]
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
            (str(row["factset_id"]), factset["factset_id"]),
            (str(row["factset_receipt_sha256"]), factset["receipt_sha256"]),
            (str(row["factset_sha256"]), factset["factset_sha256"]),
            (int(row["fact_count"]), factset["fact_count"]),
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
            raise MetarGroundCompletionIntegrityError(
                "output row differs from document"
            )
        return MetarGroundCompletionOutputReceipt(
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
            factset_id=factset["factset_id"],
            factset_receipt_sha256=factset["receipt_sha256"],
            factset_sha256=factset["factset_sha256"],
            fact_count=factset["fact_count"],
            isolation_authority_sha256=isolation["authority_sha256"],
            isolation_limits_sha256=isolation["limits_sha256"],
            created_at=created_at,
            output=document,
        )

    def load(self, event_id: str) -> MetarGroundCompletionOutputReceipt | None:
        event = _identifier(event_id, "output event ID")
        with self._lock, closing(self._connect()) as database:
            self._validate_schema(database)
            row = database.execute(
                "SELECT * FROM completion_outputs WHERE event_id=?", (event,)
            ).fetchone()
            return None if row is None else self._from_row(row)

    def commit(self, output: Mapping[str, Any]) -> MetarGroundCompletionOutputReceipt:
        document, event, authority, input_value, decoder, isolation = (
            _validated_output_document(output)
        )
        body = _canonical(document)
        if not 0 < len(body) <= self.limits.max_receipt_bytes:
            raise MetarGroundCompletionCapacityError(
                "output receipt exceeds byte cap"
            )
        output_sha = hashlib.sha256(body).hexdigest()
        factset = decoder["factset"]
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
                        raise MetarGroundCompletionConflict(
                            "completion event already has a different output"
                        )
                    database.commit()
                    return receipt
                count, total = database.execute(
                    "SELECT count(*),coalesce(sum(output_bytes),0) "
                    "FROM completion_outputs"
                ).fetchone()
                if int(count) >= self.limits.max_receipts:
                    raise MetarGroundCompletionCapacityError(
                        "output receipt count cap reached"
                    )
                if int(total) + len(body) > self.limits.max_total_output_bytes:
                    raise MetarGroundCompletionCapacityError(
                        "output receipt byte cap reached"
                    )
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
                    "capture_id,capture_manifest_sha256,factset_id,"
                    "factset_receipt_sha256,factset_sha256,fact_count,"
                    "isolation_authority_sha256,isolation_effect_contract_sha256,"
                    "isolation_limits_sha256,output_sha256,receipt_sha256,"
                    "process_receipt_sha256,output_json,output_bytes,created_at) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        event["event_id"], event["event_sha256"],
                        event["producer_receipt_sha256"],
                        authority["desired_state_sha256"], authority["authority_sha256"],
                        CONSUMER_ID, STREAM_ID, input_value["capture_id"],
                        input_value["capture_manifest_sha256"],
                        factset["factset_id"], factset["receipt_sha256"],
                        factset["factset_sha256"], factset["fact_count"],
                        isolation["authority_sha256"],
                        isolation["effect_contract_sha256"],
                        isolation["limits_sha256"], output_sha, receipt_sha,
                        process_sha, body, len(body), created_at,
                    ),
                )
                self._fault("output.before_commit")
                database.commit()
                self._fault("output.after_commit")
        except MetarGroundCompletionError:
            raise
        except sqlite3.DatabaseError as exc:
            if "full" in str(exc).lower():
                raise MetarGroundCompletionCapacityError(
                    "output database capacity reached"
                ) from exc
            raise MetarGroundCompletionIntegrityError(
                "output database commit failed"
            ) from exc
        receipt = self.load(event["event_id"])
        if receipt is None:
            raise MetarGroundCompletionIntegrityError(
                "committed output disappeared"
            )
        return receipt

    def _verify_loaded_for_ack(
        self,
        receipt: MetarGroundCompletionOutputReceipt,
        *,
        lease: CompletionLease,
        binding: MetarGroundCompletionBinding,
        isolation_authority_sha256: str,
        isolation_limits_sha256: str,
        output_sha256: str,
        output_receipt_sha256: str,
        process_receipt_sha256: str,
    ) -> MetarGroundCompletionOutputReceipt:
        document_input = receipt.output["input"]
        document_event = receipt.output["event"]
        document_decoder = receipt.output["decoder"]
        factset = document_decoder["factset"]
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
            (document_input["role_set_content_sha256"], binding.role_set_content_sha256),
            (document_input["role_set_receipt_sha256"], binding.role_set_receipt_sha256),
            (document_decoder["job_id"], binding.decoder_job_id),
            (document_decoder["attempt_id"], binding.decoder_attempt_id),
            (factset["capture_id"], binding.capture_id),
            (factset["capture_manifest_sha256"], binding.capture_manifest_sha256),
            (receipt.factset_id, factset["factset_id"]),
            (receipt.factset_receipt_sha256, factset["receipt_sha256"]),
            (receipt.factset_sha256, factset["factset_sha256"]),
            (receipt.fact_count, factset["fact_count"]),
            (
                receipt.isolation_authority_sha256,
                _sha(isolation_authority_sha256, "ack isolation authority"),
            ),
            (
                receipt.isolation_limits_sha256,
                _sha(isolation_limits_sha256, "ack isolation limits"),
            ),
            (receipt.output_sha256, _sha(output_sha256, "ack output SHA-256")),
            (
                receipt.receipt_sha256,
                _sha(output_receipt_sha256, "ack output receipt SHA-256"),
            ),
            (
                receipt.process_receipt_sha256,
                _sha(process_receipt_sha256, "ack process receipt SHA-256"),
            ),
            (receipt.output.get("fact_revision"), None),
            (receipt.output["claims"]["ground_confirmation"], False),
            (receipt.output["claims"]["meteorological_admission"], False),
            (receipt.output["claims"]["operational_fact_published"], False),
            (receipt.output["claims"]["operational_snapshot_published"], False),
            (document_isolation["effect_contract_sha256"], PROCESS_EFFECT_CONTRACT_SHA256),
            (receipt.output["mode"], OUTPUT_MODE),
            (receipt.output["workload_id"], WORKLOAD_ID),
            (receipt.output["consumer_id"], CONSUMER_ID),
            (receipt.output["stream_id"], STREAM_ID),
        )
        if any(actual != expected for actual, expected in checks):
            raise MetarGroundCompletionConflict(
                "ack proof differs from durable output"
            )
        return receipt

    def verify_for_ack(
        self,
        *,
        lease: CompletionLease,
        binding: MetarGroundCompletionBinding,
        isolation_authority_sha256: str,
        isolation_limits_sha256: str,
        output_sha256: str,
        output_receipt_sha256: str,
        process_receipt_sha256: str,
    ) -> MetarGroundCompletionOutputReceipt:
        receipt = self.load(lease.event_id)
        if receipt is None:
            raise MetarGroundCompletionIntegrityError(
                "durable output is unavailable"
            )
        return self._verify_loaded_for_ack(
            receipt,
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
        binding: MetarGroundCompletionBinding,
        isolation_authority_sha256: str,
        isolation_limits_sha256: str,
        output_sha256: str,
        output_receipt_sha256: str,
        process_receipt_sha256: str,
    ) -> Iterator[MetarGroundCompletionOutputReceipt]:
        """Hold the exact output writer fence through lifecycle terminalization."""

        event = _identifier(lease.event_id, "output event ID")
        with self._lock, closing(self._connect()) as database:
            try:
                database.execute("BEGIN IMMEDIATE")
                self._validate_schema(database, full=True)
                row = database.execute(
                    "SELECT * FROM completion_outputs WHERE event_id=?",
                    (event,),
                ).fetchone()
                if row is None:
                    raise MetarGroundCompletionIntegrityError(
                        "durable output is unavailable"
                    )
                receipt = self._from_row(row)
                verified = self._verify_loaded_for_ack(
                    receipt,
                    lease=lease,
                    binding=binding,
                    isolation_authority_sha256=isolation_authority_sha256,
                    isolation_limits_sha256=isolation_limits_sha256,
                    output_sha256=output_sha256,
                    output_receipt_sha256=output_receipt_sha256,
                    process_receipt_sha256=process_receipt_sha256,
                )
                yield verified
                self._verify_path()
                self._validate_schema(database)
                final_row = database.execute(
                    "SELECT * FROM completion_outputs WHERE event_id=?",
                    (event,),
                ).fetchone()
                if final_row is None:
                    raise MetarGroundCompletionIntegrityError(
                        "durable output disappeared while reproof was fenced"
                    )
                final_receipt = self._from_row(final_row)
                if (
                    final_receipt.output_sha256 != receipt.output_sha256
                    or final_receipt.receipt_sha256 != receipt.receipt_sha256
                    or final_receipt.process_receipt_sha256
                    != receipt.process_receipt_sha256
                ):
                    raise MetarGroundCompletionIntegrityError(
                        "durable output changed while reproof was fenced"
                    )
            finally:
                if database.in_transaction:
                    database.rollback()

    def diagnostics(self) -> dict[str, Any]:
        with self._lock, closing(self._connect()) as database:
            self._validate_schema(database, full=True)
            count, total = database.execute(
                "SELECT count(*),coalesce(sum(output_bytes),0) "
                "FROM completion_outputs"
            ).fetchone()
        result = {
            "contract": OUTPUT_STORE_CONTRACT,
            "healthy": True,
            "output_bytes": int(total),
            "receipts": int(count),
            "runtime_wiring": "inert",
        }
        if len(_canonical(result)) > MAX_DIAGNOSTIC_BYTES:
            raise MetarGroundCompletionCapacityError(
                "output diagnostics exceed cap"
            )
        return result


def _safe_existing_path(value: object, label: str, *, directory: bool) -> Path:
    if not isinstance(value, str) or not value or "\0" in value or len(value) > 1024:
        raise MetarGroundCompletionIntegrityError(f"{label} path is invalid")
    path = Path(value)
    if not path.is_absolute() or path.is_symlink() or not path.exists():
        raise MetarGroundCompletionIntegrityError(f"{label} path is unsafe")
    resolved = path.resolve(strict=True)
    if resolved != path:
        raise MetarGroundCompletionIntegrityError(f"{label} path is not normalized")
    mode = path.lstat().st_mode
    if (directory and not stat.S_ISDIR(mode)) or (
        not directory and not stat.S_ISREG(mode)
    ):
        raise MetarGroundCompletionIntegrityError(f"{label} path type differs")
    return path


def _assert_process_store_paths_disjoint(
    *,
    raw_root: Path,
    ground_path: Path,
    output_path: Path,
) -> None:
    """Reject database aliasing and database placement inside the raw tree."""

    try:
        databases_alias = ground_path.samefile(output_path)
    except OSError:
        raise MetarGroundCompletionIntegrityError(
            "process authority store identity is unavailable"
        ) from None
    if databases_alias:
        raise MetarGroundCompletionIntegrityError(
            "process authority database paths alias"
        )
    for database_path in (ground_path, output_path):
        if database_path.is_relative_to(raw_root):
            raise MetarGroundCompletionIntegrityError(
                "process authority database overlaps the raw object store"
            )


@dataclass(frozen=True, slots=True)
class _LoadedProcessAuthority:
    sha256: str
    raw_root: Path
    raw_limits: RawArtifactLimits
    ground_path: Path
    ground_limits: GroundEvidenceLimits
    output_path: Path
    output_identity_sha256: str
    output_limits: MetarGroundCompletionOutputLimits
    process_limits: MetarGroundCompletionProcessLimits


def _load_process_authority(
    value: object,
    expected_sha256: str,
) -> _LoadedProcessAuthority:
    mapping = _exact(value, {
        "consumer_binding", "consumer_binding_sha256", "effect_contract",
        "effect_contract_sha256", "process_limits", "schema", "stores",
    }, "METAR process authority")
    if (
        mapping["schema"] != PROCESS_AUTHORITY_SCHEMA
        or _digest(mapping) != _sha(expected_sha256, "process authority SHA-256")
        or mapping["consumer_binding"] != _catalog_binding().as_dict()
        or mapping["consumer_binding_sha256"] != _catalog_binding().fingerprint
        or mapping["effect_contract"] != _effect_mapping()
        or mapping["effect_contract_sha256"] != PROCESS_EFFECT_CONTRACT_SHA256
    ):
        raise MetarGroundCompletionIntegrityError("process authority differs")
    limit_mapping = _exact(mapping["process_limits"], {
        "cpu_seconds", "factset_reproof_seconds", "max_request_bytes",
        "max_result_bytes", "rss_bytes", "wall_seconds",
    }, "process limits")
    limits = MetarGroundCompletionProcessLimits(**dict(limit_mapping))
    if limits.to_mapping() != dict(limit_mapping):
        raise MetarGroundCompletionIntegrityError(
            "process limits roundtrip differs"
        )
    stores = _exact(mapping["stores"], {
        "canonical_ground_evidence", "outputs", "source_raw",
    }, "process stores")
    raw_value = _exact(
        stores["source_raw"], {"limits", "root"}, "raw authority"
    )
    raw_limits = RawArtifactLimits(**dict(_exact(
        raw_value["limits"],
        set(RawArtifactLimits.__dataclass_fields__),
        "raw limits",
    )))
    ground_value = _exact(
        stores["canonical_ground_evidence"],
        {"limits", "path"},
        "canonical ground authority",
    )
    ground_limits = GroundEvidenceLimits(**dict(_exact(
        ground_value["limits"],
        set(GroundEvidenceLimits.__dataclass_fields__),
        "canonical ground limits",
    )))
    output = _exact(
        stores["outputs"],
        {"identity_sha256", "limits", "path"},
        "output authority",
    )
    output_identity_sha256 = _sha(
        output["identity_sha256"], "output identity SHA-256"
    )
    output_limits = MetarGroundCompletionOutputLimits(**dict(_exact(
        output["limits"],
        set(MetarGroundCompletionOutputLimits.__dataclass_fields__),
        "output limits",
    )))
    raw_root = _safe_existing_path(
        raw_value["root"], "source raw", directory=True
    )
    ground_path = _safe_existing_path(
        ground_value["path"], "canonical ground evidence", directory=False
    )
    output_path = _safe_existing_path(
        output["path"], "completion output", directory=False
    )
    _assert_process_store_paths_disjoint(
        raw_root=raw_root,
        ground_path=ground_path,
        output_path=output_path,
    )
    return _LoadedProcessAuthority(
        sha256=expected_sha256,
        raw_root=raw_root,
        raw_limits=raw_limits,
        ground_path=ground_path,
        ground_limits=ground_limits,
        output_path=output_path,
        output_identity_sha256=output_identity_sha256,
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
        raise MetarGroundCompletionIntegrityError("isolated proof is invalid")
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


def _factset_receipt(
    receipts: tuple[GroundObservationReceipt, ...] | list[GroundObservationReceipt],
    binding: MetarGroundCompletionBinding,
) -> dict[str, Any]:
    """Build one bounded digest over every full canonical ground receipt."""

    maximum = MetarGroundDecodeLimits().max_facts
    if len(receipts) > maximum:
        raise MetarGroundCompletionCapacityError(
            "canonical ground factset exceeds decoder cap"
        )
    ordered = sorted(receipts, key=lambda item: item.fact_id)
    if len({item.fact_id for item in ordered}) != len(ordered):
        raise MetarGroundCompletionIntegrityError(
            "canonical ground factset contains duplicate identities"
        )
    identity = {
        "capture_id": binding.capture_id,
        "capture_manifest_sha256": binding.capture_manifest_sha256,
        "decoder_config_sha256": DECODER_CONFIG_SHA256,
        "decoder_identity": DECODER_IDENTITY,
        "decoder_version": DECODER_VERSION,
        "schema": FACTSET_DIGEST_SCHEMA,
    }
    factset_id = "ground-factset-" + hashlib.sha256(
        _canonical(identity)
    ).hexdigest()
    digest = hashlib.sha256()
    header = _canonical({**identity, "factset_id": factset_id})
    digest.update(len(header).to_bytes(8, "big"))
    digest.update(header)
    for receipt in ordered:
        if not isinstance(receipt, GroundObservationReceipt):
            raise MetarGroundCompletionIntegrityError(
                "canonical ground decoder returned a non-receipt"
            )
        fact = receipt.fact
        if (
            fact.capture_id != binding.capture_id
            or receipt.capture_manifest_sha256
            != binding.capture_manifest_sha256
            or fact.capture_receipt_sha256
            != binding.capture_receipt_sha256
            or fact.decoder_identity != DECODER_IDENTITY
            or fact.decoder_version != DECODER_VERSION
            or fact.config_sha256 != DECODER_CONFIG_SHA256
            or fact.provider != PROVIDER
            or fact.product != CAPTURE_PRODUCT
            or tuple(member.role for member in fact.members) != EXPECTED_ROLES
            or fact.operational_admission_eligible is not False
            or receipt.operational_admission_eligible is not False
        ):
            raise MetarGroundCompletionIntegrityError(
                "canonical ground receipt differs from METAR authority"
            )
        encoded = _canonical(receipt.to_metadata())
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
    body = {
        **identity,
        "fact_count": len(ordered),
        "factset_id": factset_id,
        "factset_sha256": digest.hexdigest(),
        "schema": FACTSET_RECEIPT_SCHEMA,
    }
    return {**body, "receipt_sha256": _digest(body)}


def _reprove_canonical_factset(
    evidence_store: CanonicalGroundEvidenceStore,
    binding: MetarGroundCompletionBinding,
    expected: Mapping[str, Any],
    *,
    timeout_seconds: float,
) -> dict[str, Any]:
    """Physically reload every exact fact and its decoder receipt."""

    if not isinstance(evidence_store, CanonicalGroundEvidenceStore):
        raise TypeError("evidence_store has wrong type")
    deadline = evidence_store._monotonic() + float(timeout_seconds)
    receipts: list[GroundObservationReceipt] = []
    try:
        evidence_store._check_deadline(deadline)
        with evidence_store._lock, closing(
            evidence_store._connect_read_only(deadline)
        ) as database:
            evidence_store._validate_schema(database, full_integrity=True)
            cursor = database.execute(
                "SELECT * FROM ground_facts "
                "WHERE capture_id=? AND decoder_identity=? "
                "AND decoder_version=? AND config_sha256=? "
                "ORDER BY fact_id",
                (
                    binding.capture_id,
                    DECODER_IDENTITY,
                    DECODER_VERSION,
                    DECODER_CONFIG_SHA256,
                ),
            )
            maximum = MetarGroundDecodeLimits().max_facts
            while True:
                evidence_store._check_deadline(deadline)
                rows = cursor.fetchmany(256)
                if not rows:
                    break
                for row in rows:
                    if len(receipts) >= maximum:
                        raise MetarGroundCompletionCapacityError(
                            "physical canonical factset exceeds decoder cap"
                        )
                    receipts.append(
                        evidence_store._validated_receipt(database, row)
                    )
        if receipts:
            capture = evidence_store._verify_capture(
                receipts[0].fact, deadline
            )
            for receipt in receipts:
                evidence_store._check_deadline(deadline)
                evidence_store._validate_capture_snapshot(receipt, capture)
        actual = _factset_receipt(receipts, binding)
    except MetarGroundCompletionError:
        raise
    except (
        CanonicalGroundEvidenceError,
        OSError,
        sqlite3.DatabaseError,
        TypeError,
        ValueError,
    ) as exc:
        raise MetarGroundCompletionIntegrityError(
            "canonical ground factset failed physical reproof"
        ) from exc
    if not isinstance(expected, Mapping) or actual != dict(expected):
        raise MetarGroundCompletionIntegrityError(
            "canonical ground factset differs from durable decoder receipt"
        )
    return actual


def _output_document(
    *,
    lease: CompletionLease,
    binding: MetarGroundCompletionBinding,
    result: Any,
    isolation_authority_sha256: str,
    isolation_limits_sha256: str,
) -> dict[str, Any]:
    if (
        result.capture_id != binding.capture_id
        or result.decoder_identity != DECODER_IDENTITY
        or result.decoder_version != DECODER_VERSION
        or result.config_sha256 != DECODER_CONFIG_SHA256
        or result.fact_count != len(result.receipts)
        or result.operational_admission_eligible is not False
    ):
        raise MetarGroundCompletionIntegrityError(
            "METAR decoder result differs from completion input"
        )
    factset = _factset_receipt(list(result.receipts), binding)
    output = {
        "authority": {
            "authority_sha256": lease.authority_sha256,
            "desired_state_sha256": lease.desired_state_sha256,
        },
        "claims": {
            "canonical_ground_evidence_committed": True,
            "ground_confirmation": False,
            "meteorological_admission": False,
            "operational_fact_published": False,
            "operational_snapshot_published": False,
        },
        "consumer_id": CONSUMER_ID,
        "decoder": {
            "attempt_id": binding.decoder_attempt_id,
            "config_sha256": DECODER_CONFIG_SHA256,
            "factset": factset,
            "identity": DECODER_IDENTITY,
            "job_id": binding.decoder_job_id,
            "version": DECODER_VERSION,
        },
        "disposition": "shadow_evidence_committed",
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


def _metar_ground_completion_child(request: Mapping[str, Any]) -> dict[str, Any]:
    """Importable child entry; returns hashes only after durable reproof."""

    mapping = _exact(request, {
        "authority", "authority_sha256", "desired_state", "lease",
        "request_sha256", "schema",
    }, "METAR process request")
    request_body = {
        key: mapping[key] for key in mapping if key != "request_sha256"
    }
    if (
        mapping["schema"] != PROCESS_REQUEST_SCHEMA
        or _digest(request_body)
        != _sha(mapping["request_sha256"], "request SHA-256")
    ):
        raise MetarGroundCompletionIntegrityError("process request differs")
    authority = _load_process_authority(
        mapping["authority"], mapping["authority_sha256"]
    )
    desired = parse_desired_state(mapping["desired_state"])
    lease = _lease_from_wire(mapping["lease"])
    raw_store = RawArtifactStore(
        authority.raw_root, limits=authority.raw_limits
    )
    raw_store.initialize()
    binding = validate_metar_ground_completion_lease(
        lease, desired_state=desired, raw_store=raw_store
    )
    evidence_store = CanonicalGroundEvidenceStore(
        authority.ground_path,
        raw_artifact_reader=raw_store,
        limits=authority.ground_limits,
    )
    output_store = MetarGroundCompletionOutputStore(
        authority.output_path, limits=authority.output_limits
    )
    output_store.assert_instance_held(authority.output_identity_sha256)
    reserve = authority.process_limits.factset_reproof_seconds + 1.0
    decode_seconds = min(
        DEFAULT_DECODE_SECONDS,
        max(0.05, authority.process_limits.wall_seconds - reserve),
    )
    result = decode_metar_ground_capture(
        binding.capture_id,
        raw_store=raw_store,
        evidence_store=evidence_store,
        limits=MetarGroundDecodeLimits(),
        deadline_at=time.monotonic() + decode_seconds,
    )
    output = _output_document(
        lease=lease,
        binding=binding,
        result=result,
        isolation_authority_sha256=authority.sha256,
        isolation_limits_sha256=authority.process_limits.fingerprint,
    )
    _reprove_canonical_factset(
        evidence_store,
        binding,
        output["decoder"]["factset"],
        timeout_seconds=authority.process_limits.factset_reproof_seconds,
    )
    receipt = output_store.commit(output)
    durable = output_store.verify_for_ack(
        lease=lease,
        binding=binding,
        isolation_authority_sha256=authority.sha256,
        isolation_limits_sha256=authority.process_limits.fingerprint,
        output_sha256=receipt.output_sha256,
        output_receipt_sha256=receipt.receipt_sha256,
        process_receipt_sha256=receipt.process_receipt_sha256,
    )
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


class MetarGroundCompletionIsolatedRunner:
    """Fresh hard child plus full durable-ground reconciliation."""

    def __init__(
        self,
        *,
        raw_store: RawArtifactStore,
        evidence_store: CanonicalGroundEvidenceStore,
        output_store: MetarGroundCompletionOutputStore,
        limits: MetarGroundCompletionProcessLimits = MetarGroundCompletionProcessLimits(),
        context: multiprocessing.context.BaseContext | None = None,
    ) -> None:
        if not isinstance(raw_store, RawArtifactStore):
            raise TypeError("raw_store has wrong type")
        if not isinstance(evidence_store, CanonicalGroundEvidenceStore):
            raise TypeError("evidence_store has wrong type")
        if evidence_store._raw_artifact_reader is not raw_store:
            raise TypeError(
                "evidence_store must share the exact METAR raw authority"
            )
        if not isinstance(output_store, MetarGroundCompletionOutputStore):
            raise TypeError("output_store has wrong type")
        if not isinstance(limits, MetarGroundCompletionProcessLimits):
            raise TypeError("process limits have wrong type")
        self.raw_store = raw_store
        self.evidence_store = evidence_store
        self.output_store = output_store
        self.limits = limits
        self._authority = self._authority_mapping()
        self.authority_sha256 = _digest(self._authority)
        _load_process_authority(self._authority, self.authority_sha256)
        self._executor = HardDecoderExecutor(
            limits=limits.decoder_limits(), context=context
        )

    @property
    def minimum_lease_seconds(self) -> float:
        # Output reproof after child and twice around the final ack.
        return (
            float(self.limits.wall_seconds)
            + 3.0 * float(self.limits.factset_reproof_seconds)
            + 2.0
        )

    def _authority_mapping(self) -> dict[str, Any]:
        for path, label, directory in (
            (self.raw_store.root, "source raw", True),
            (self.evidence_store.path, "canonical ground evidence", False),
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
                "canonical_ground_evidence": {
                    "limits": asdict(self.evidence_store._limits),
                    "path": str(self.evidence_store.path),
                },
                "outputs": {
                    "identity_sha256": self.output_store.identity_sha256,
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
        binding: MetarGroundCompletionBinding,
    ) -> MetarGroundCompletionOutputReceipt | None:
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
        self._reprove_raw_authority(binding)
        _reprove_canonical_factset(
            self.evidence_store,
            binding,
            verified.output["decoder"]["factset"],
            timeout_seconds=self.limits.factset_reproof_seconds,
        )
        return verified

    def _reprove_raw_authority(
        self,
        binding: MetarGroundCompletionBinding,
    ) -> None:
        """Rehash the exact raw role and reapply recovery provenance rules."""

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
            raise MetarGroundCompletionIntegrityError(
                "METAR recovery authority drifted during reproof"
            )
        try:
            loaded = self.raw_store.load_capture_bundle_bytes(
                binding.capture_id
            )
            if loaded is None:
                raise MetarGroundCompletionIntegrityError(
                    "METAR raw capture disappeared during reproof"
                )
            capture, receipts, payloads = loaded
            if (
                set(receipts) != set(EXPECTED_ROLES)
                or set(payloads) != set(EXPECTED_ROLES)
            ):
                raise MetarGroundCompletionIntegrityError(
                    "METAR raw roles changed during reproof"
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
                    raise MetarGroundCompletionIntegrityError(
                        "METAR raw bytes changed during reproof"
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
            raise MetarGroundCompletionIntegrityError(
                "METAR raw authority failed reproof"
            ) from None
        if (
            capture_job_id != binding.acquisition_job_id
            or capture.capture_id != binding.capture_id
            or capture.manifest_sha256 != binding.capture_manifest_sha256
            or capture.capture_receipt_sha256
            != binding.capture_receipt_sha256
            or evidence.content_sha256 != binding.role_set_content_sha256
            or evidence.receipt_sha256 != binding.role_set_receipt_sha256
        ):
            raise MetarGroundCompletionIntegrityError(
                "METAR raw authority differs from completion binding"
            )

    def execute(
        self,
        *,
        lease: CompletionLease,
        desired_state: DesiredState,
        cancel_event: threading.Event,
    ) -> MetarGroundCompletionProcessResult:
        started = time.monotonic()
        if not callable(getattr(cancel_event, "is_set", None)):
            raise TypeError("cancel_event must expose is_set()")
        binding = validate_metar_ground_completion_lease(
            lease, desired_state=desired_state, raw_store=self.raw_store
        )
        existing = self._load_exact(lease, binding)
        if existing is not None:
            return MetarGroundCompletionProcessResult(
                status="committed",
                reason_code="durable_output_reconciled_before_spawn",
                execution_outcome="durable_duplicate",
                output_receipt=existing,
                isolation_enforced=True,
                elapsed_seconds=max(0.0, time.monotonic() - started),
            )
        if cancel_event.is_set():
            return MetarGroundCompletionProcessResult(
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
            "schema": PROCESS_REQUEST_SCHEMA,
        }
        request = {**body, "request_sha256": _digest(body)}
        decoder = self._executor.execute(
            target=PROCESS_TARGET,
            request=request,
            effect_contract=PROCESS_EFFECT_CONTRACT,
            cancel_event=cancel_event,
        )
        isolation_enforced = decoder.outcome not in {
            "busy", "unsupported_platform", "boundary_setup_failed",
            "unsafe_effect_contract", "invalid_request",
        } and not (
            decoder.outcome == "cancelled"
            and decoder.detail_code == "cancelled_before_spawn"
        )
        try:
            receipt = self._load_exact(lease, binding)
        except MetarGroundCompletionError:
            return MetarGroundCompletionProcessResult(
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
            return MetarGroundCompletionProcessResult(
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
        return MetarGroundCompletionProcessResult(
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



class MetarGroundCompletionConsumer:
    """Lease -> hard child -> durable receipt -> lifecycle ack coordinator."""

    def __init__(
        self,
        *,
        lifecycle_store: AcquisitionEvidenceLifecycleStore,
        desired_state_store: DesiredStateStore,
        runner: MetarGroundCompletionIsolatedRunner,
        clock: Callable[[], float] = time.time,
        fault_injector: Callable[[str], None] | None = None,
    ) -> None:
        if not isinstance(lifecycle_store, AcquisitionEvidenceLifecycleStore):
            raise TypeError("lifecycle_store has wrong type")
        if not isinstance(desired_state_store, DesiredStateStore):
            raise TypeError("desired_state_store has wrong type")
        if not isinstance(runner, MetarGroundCompletionIsolatedRunner):
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
            raise MetarGroundCompletionIntegrityError(
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
            raise MetarGroundCompletionIntegrityError(
                "lifecycle terminal receipt differs"
            )

    def _reject_obsolete(
        self,
        lease: CompletionLease,
        *,
        active_desired: DesiredState | None,
        reason_code: str,
        now: float,
    ) -> MetarGroundCompletionRunResult:
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
        return MetarGroundCompletionRunResult(
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
    ) -> MetarGroundCompletionRunResult:
        if not isinstance(lease, CompletionLease):
            raise TypeError("lease must be CompletionLease")
        cancellation = cancel_event or threading.Event()
        if not callable(getattr(cancellation, "is_set", None)):
            raise TypeError("cancel_event must expose is_set()")
        now = _timestamp(self._clock(), "consumer clock")
        if cancellation.is_set():
            raise MetarGroundCompletionCancelled("completion cancelled before validation")
        reloaded_initial = self.lifecycle_store.load_leased(
            lease.event_id, lease.lease_token
        )
        if reloaded_initial != lease:
            raise MetarGroundCompletionConflict(
                "lifecycle lease changed before validation"
            )
        if lease.lease_expires_at - now < self.runner.minimum_lease_seconds:
            raise MetarGroundCompletionDeadlineError(
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
        binding = validate_metar_ground_completion_lease(
            lease, desired_state=desired, raw_store=self.runner.raw_store
        )
        process = self.runner.execute(
            lease=lease, desired_state=desired, cancel_event=cancellation
        )
        if process.status != "committed" or process.output_receipt is None:
            return MetarGroundCompletionRunResult(
                status=process.status,
                reason_code=process.reason_code,
                event_id=lease.event_id,
            )
        output = process.output_receipt
        # Fault point proves a committed output can survive a coordinator crash.
        self._fault("consumer.after_output_commit")
        if cancellation.is_set():
            raise MetarGroundCompletionCancelled(
                "completion cancelled after durable output and before ack"
            )
        now = _timestamp(self._clock(), "consumer post-process clock")
        if now >= lease.lease_expires_at:
            raise MetarGroundCompletionDeadlineError(
                "completion lease expired after durable output and before ack"
            )
        reloaded = self.lifecycle_store.load_leased(lease.event_id, lease.lease_token)
        if reloaded != lease:
            raise MetarGroundCompletionConflict("lifecycle lease changed before ack")
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
            raise MetarGroundCompletionIntegrityError(
                "durable output changed before lifecycle ack"
            )
        expected_output_identity = self.runner.output_store.identity_sha256
        with self.runner.output_store.completion_reproof_lease(
            lease=reloaded,
            binding=binding,
            isolation_authority_sha256=self.runner.authority_sha256,
            isolation_limits_sha256=self.runner.limits.fingerprint,
            output_sha256=verified.output_sha256,
            output_receipt_sha256=verified.receipt_sha256,
            process_receipt_sha256=verified.process_receipt_sha256,
        ) as final_verified:
            if (
                final_verified.output_sha256 != verified.output_sha256
                or final_verified.receipt_sha256 != verified.receipt_sha256
                or final_verified.process_receipt_sha256
                != verified.process_receipt_sha256
            ):
                raise MetarGroundCompletionIntegrityError(
                    "durable output changed at lifecycle ack boundary"
                )
            self._fault("consumer.before_lifecycle_ack")
            if cancellation.is_set():
                raise MetarGroundCompletionCancelled(
                    "completion cancelled at lifecycle ack boundary"
                )
            now = _timestamp(self._clock(), "consumer final ack clock")
            if now >= lease.lease_expires_at:
                raise MetarGroundCompletionDeadlineError(
                    "completion lease expired at lifecycle ack boundary"
                )
            final_lease = self.lifecycle_store.load_leased(
                lease.event_id, lease.lease_token
            )
            if final_lease != lease:
                raise MetarGroundCompletionConflict(
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
            self.runner._reprove_raw_authority(binding)
            _reprove_canonical_factset(
                self.runner.evidence_store,
                binding,
                final_verified.output["decoder"]["factset"],
                timeout_seconds=self.runner.limits.factset_reproof_seconds,
            )
            self.runner.output_store.assert_instance_held(
                expected_output_identity
            )
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
        return MetarGroundCompletionRunResult(
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
    ) -> MetarGroundCompletionRunResult:
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
            raise MetarGroundCompletionDeadlineError(
                "requested lifecycle lease cannot contain hard process deadline"
            )
        cancellation = cancel_event or threading.Event()
        if cancellation.is_set():
            raise MetarGroundCompletionCancelled("completion cancelled before claim")
        now = _timestamp(self._clock(), "consumer claim clock")
        lease = self.lifecycle_store.claim_next(
            CONSUMER_ID,
            STREAM_ID,
            owner,
            lease_seconds=duration,
            now=now,
        )
        if lease is None:
            return MetarGroundCompletionRunResult("idle", "no_ready_completion")
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
            raise MetarGroundCompletionCapacityError("consumer diagnostics exceed cap")
        return result


__all__ = [
    "CONSUMER_ID",
    "IMPLEMENTATION_ID",
    "LEGACY_OUTPUT_STORE_CONTRACT",
    "LEGACY_SCHEMA_VERSION",
    "MetarGroundCompletionBinding",
    "MetarGroundCompletionCancelled",
    "MetarGroundCompletionCapacityError",
    "MetarGroundCompletionConflict",
    "MetarGroundCompletionConsumer",
    "MetarGroundCompletionDeadlineError",
    "MetarGroundCompletionError",
    "MetarGroundCompletionIntegrityError",
    "MetarGroundCompletionIsolatedRunner",
    "MetarGroundCompletionOutputLimits",
    "MetarGroundCompletionOutputReceipt",
    "MetarGroundCompletionOutputStore",
    "MetarGroundCompletionProcessLimits",
    "MetarGroundCompletionProcessResult",
    "MetarGroundCompletionRunResult",
    "OUTPUT_DOCUMENT_SCHEMA",
    "OUTPUT_MODE",
    "OUTPUT_RECEIPT_SCHEMA",
    "OUTPUT_STORE_CONTRACT",
    "OUTPUT_STORE_IDENTITY_SCHEMA",
    "PROCESS_EFFECT_CONTRACT_SHA256",
    "PROCESS_RECEIPT_SCHEMA",
    "SCHEMA_VERSION",
    "STREAM_ID",
    "WORKLOAD_ID",
    "validate_metar_ground_completion_lease",
]
