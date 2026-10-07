"""Crash-durable shadow evidence lifecycle and completion outbox.

This module is deliberately inert: :mod:`storm_tracker_worker.service` does not
import it.  It mirrors a *complete* acquisition proof and the exact raw bytes
into a bounded, content-addressed hot window.  A producer receipt is committed
before the corresponding completion becomes ready.  Consumers advance a
durable per-stream cursor only after an immutable consumption receipt exists.

Terminal completions can be copied into a hash-chained sealed archive and are
then pruned from the mirror child-first through a durable multi-phase plan.
The original scheduler, execution and raw stores remain independent
authorities; this module never deletes or rewrites them and never refetches a
provider payload.
"""
from __future__ import annotations

from contextlib import ExitStack, closing, contextmanager, nullcontext
from dataclasses import asdict, dataclass, field
import errno
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
from typing import Any, Callable, Final, Iterator, Mapping

from .acquisition_source_reclamation_protocol import (
    HeldCommittedDecisionAuthority,
    PARTICIPANT_APPLY_ORDER,
    SourceReclamationCandidateAuthority,
    SourceReclamationCandidateBindingV1,
    SourceReclamationCandidatePageV1,
    SourceLocalAbortReceiptV1,
    SourceLocalApplyReceiptV1,
    SourceLocalPrepareReceiptV2,
    SourceReclamationRequestV2,
    archive_binding_root as reclamation_archive_binding_root,
    candidate_binding_root as reclamation_candidate_binding_root,
    candidate_participant_view_sha256,
    canonical_bytes as reclamation_canonical_bytes,
    digest_mapping as reclamation_digest_mapping,
    require_sha256 as require_reclamation_sha256,
)
from .acquisition_source_reclamation_views import (
    participant_candidate_view_sha256 as build_participant_candidate_view_sha256,
)

from .acquisition_execution_store import (
    ExecutionReceipt,
    ExecutionReceiptStore,
)
from . import acquisition_scheduler_bridge
from . import worker_acquisition_plan
from .autonomous_scheduler import AutonomousScheduler, SchedulerTerminalProof
from .desired_state import DesiredState
from .dwd_rv_capture_handoff import (
    HANDOFF_ADAPTER_ID,
    HANDOFF_ADAPTER_VERSION,
    HANDOFF_CONFIG_SHA256,
    DwdRvCaptureHandoffReceipt,
    adapt_dwd_rv_raw_capture,
)
from .raw_artifacts import (
    RawArtifactCaptureReceipt,
    RawArtifactReceipt,
    RawArtifactStore,
)
from .raw_capacity_ledger import RawCapacityLedger


CONTRACT = "stv3-worker-acquisition-evidence-lifecycle-v1"
PRODUCER_RECEIPT_SCHEMA = "stv3-worker-acquisition-producer-receipt-v1"
COMPLETION_EVENT_SCHEMA = "stv3-worker-acquisition-completion-event-v1"
CONSUMPTION_RECEIPT_SCHEMA = "stv3-worker-acquisition-consumption-receipt-v1"
REJECTION_RECEIPT_SCHEMA = "stv3-worker-acquisition-rejection-receipt-v1"
QUARANTINE_RECEIPT_SCHEMA = "stv3-worker-acquisition-quarantine-receipt-v1"
ARCHIVE_SEGMENT_SCHEMA = "stv3-worker-acquisition-evidence-archive-v1"
ARCHIVE_MANIFEST_SCHEMA = "stv3-worker-acquisition-evidence-manifest-v1"
ARCHIVED_COMPLETION_SCHEMA = "stv3-worker-acquisition-archived-completion-v3"
LEGACY_ARCHIVED_COMPLETION_SCHEMA_V2 = (
    "stv3-worker-acquisition-archived-completion-v2"
)
LEGACY_ARCHIVED_COMPLETION_SCHEMA = (
    "stv3-worker-acquisition-archived-completion-v1"
)
DWD_COMPLETION_PROOF_SCHEMA = "stv3-worker-dwd-rv-completion-proof-v1"
DESIRED_AUTHORITY_SCHEMA = "stv3-worker-desired-authority-proof-v1"
SCHEMA_VERSION = 4
CLOCK_AUTHORITY_SCHEMA = "stv3-worker-acquisition-evidence-clock-authority-v1"
STORE_IDENTITY_SCHEMA = "stv3-worker-acquisition-evidence-store-identity-v1"
RECLAMATION_META_SCHEMA = (
    "stv3-worker-acquisition-evidence-reclamation-meta-v1"
)
RECLAMATION_MEMBER_SCHEMA = (
    "stv3-worker-acquisition-evidence-reclamation-member-v1"
)
RECLAMATION_FILE_SCHEMA = (
    "stv3-worker-acquisition-evidence-reclamation-file-v1"
)
RECLAMATION_OBJECT_REFERENCE_SCHEMA = (
    "stv3-worker-acquisition-evidence-reclamation-object-reference-v2"
)
RECLAMATION_OBJECT_EFFECT_BINDING_SCHEMA = (
    "stv3-worker-acquisition-evidence-reclamation-object-effect-binding-v1"
)
RECLAMATION_ARCHIVE_INDEX_SCHEMA = (
    "stv3-worker-acquisition-evidence-reclamation-archive-index-v1"
)
RECLAMATION_FLOOR_SCHEMA = (
    "stv3-worker-acquisition-evidence-archive-floor-v1"
)
RECLAMATION_EFFECT_SCHEMA = (
    "stv3-worker-acquisition-evidence-reclamation-effect-v1"
)
RECLAMATION_POST_APPLY_SCHEMA = (
    "stv3-worker-acquisition-evidence-reclamation-post-apply-v1"
)
RECLAMATION_FENCE_SCHEMA = (
    "stv3-worker-acquisition-evidence-reclamation-fence-v1"
)
RECLAMATION_RELEASE_SCHEMA = (
    "stv3-worker-acquisition-evidence-reclamation-release-v1"
)
LIFECYCLE_RECLAMATION_PREWIRING_BLOCKERS: Final[tuple[str, ...]] = (
    "exclusive_filesystem_owner_dirfd_policy_required_for_"
    "crash_atomic_hardlink_exclusion",
)
RECLAMATION_STATES = frozenset({
    "PREPARED", "APPLYING", "APPLIED", "ABORTED",
})
MAX_DIAGNOSTIC_BYTES = 4096

# Exact normalized ``sqlite_schema.sql`` digests for the pre-v4/base tables.
# The v4 reclamation tables are kept as readable canonical SQL below because
# their constraints form the deletion authority itself.
_V4_BASE_TABLE_SQL_SHA256 = {
    "archive_index": "0afa8c4623eb78312b27a914337c977cc08f1fb84545019a41b4c25ef64dbb72",
    "archive_objects": "aadf076702a76c714485b4ad725ed9fe874da8125cf3dccd681c2265d9a1edb8",
    "archive_segments": "eb621ddffcf78d8f57abfe866daee2e4fa547afd7d966de3c74d813f3364afb0",
    "completion_outbox": "cc5c47d5cde173f670e4cf90d32d40cd7654dc98fe822d97c930ed4e9ac7d088",
    "consumer_cursors": "cdcb8aa5f7f1845586e2cb9ba61520ae7dd0fb799589b118bab9e26a699aa496",
    "consumption_receipts": "035b273b62b45a4198a0b99c578d1f58b67a11a0ac44fc1262dda6c05c53e1ad",
    "hot_objects": "35cbe5c9067b9f9e6692c49f9d4d4a45c9557ad19b3f64d363cb75ec09fdcc15",
    "lifecycle_clock": "74d2dc95aae4ca0d3cd2d4b127cb44c6da72f405203c06c56486d99833d36d2b",
    "lifecycle_heads": "9bcfd8fccaa7a10d6107b14bfb8ee090ecee4e36b3100a64913c997b2f867390",
    "lifecycle_schema": "abebeaee9a2e593ebecd646eeee759cafaf59c20ad6e0da8034e4da3bcc7a2f0",
    "producer_receipts": "a1eae2d482982e310ee0b10016bf6517ccfc32f9110421fc45b5eee464bb94b0",
    "prune_plans": "fc5a40777a55e7894741a49e03dde1c3d45a3fa0dd2ac007563dd24d66d34a00",
    "quarantine_receipts": "8d74abf0e008d885e01333a3dc315aec2ea51e266683a138fc2f596b7d2515ee",
    "rejection_receipts": "bbcd3fded5ad21e9ab90616ed2948688f5489c593b551eb1b2ba850e6024a244",
    "stream_heads": "a374b6545aab2521a8675caf16fa96a5fcfbdf19079314bd35ccf1b318976bcc",
}

_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,191}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SAFE_FILENAME = re.compile(r"^[0-9a-f]{64}\.(?:raw|sealed\.json)$")
_OBJECT_STAGE = re.compile(r"^\.([0-9a-f]{64})\.([0-9a-f]{16})$")
_SEGMENT_STAGE = re.compile(r"^\.([0-9a-f]{64})\.tmp$")
_SECRET_KEY = re.compile(
    r"(^|[_-])(password|passwd|secret|api[_-]?key|application[_-]?key|"
    r"access[_-]?token|refresh[_-]?token|authorization|cookie)([_-]|$)",
    re.IGNORECASE,
)
_SECRET_TEXT = re.compile(
    r"(?:authorization\s*:|(?:bearer|basic)\s+[A-Za-z0-9+/=_-]|"
    r"(?:api[_-]?key|access[_-]?token|refresh[_-]?token|password|passwd|secret)=)",
    re.IGNORECASE,
)
_OUTBOX_STATES = frozenset({"ready", "leased", "consumed", "superseded"})
REJECTION_REASON_CODES = frozenset({
    "active_desired_state_unavailable",
    "desired_state_changed_after_processing",
    "desired_state_changed_before_ack",
    "desired_state_superseded_before_processing",
})
QUARANTINE_REASON_CODES = frozenset({
    "canonical_evidence_integrity_failed",
    "completion_reference_integrity_failed",
    "decoder_output_integrity_failed",
    "source_raw_integrity_failed",
    "unsupported_completion_contract",
})


class AcquisitionEvidenceLifecycleError(RuntimeError):
    """Base error for validation, durability and bounded lifecycle failures."""


class AcquisitionEvidenceConflict(AcquisitionEvidenceLifecycleError):
    """A stable identity or CAS token was reused for different evidence."""


class AcquisitionEvidenceIntegrityError(AcquisitionEvidenceLifecycleError):
    """Persisted proof, bytes, archive chain or cursor failed verification."""


class AcquisitionEvidenceCapacityError(AcquisitionEvidenceLifecycleError):
    """A hard hot/archive/database bound rejected work without eviction."""


class AcquisitionEvidenceDurabilityError(AcquisitionEvidenceLifecycleError):
    """A required file or directory persistence barrier was unavailable."""


class CompletionWindowExpired(AcquisitionEvidenceLifecycleError):
    """The requested item is no longer in the hot consumer window."""


@dataclass(frozen=True, slots=True)
class AcquisitionEvidenceLimits:
    max_object_bytes: int = 128 * 1024 * 1024
    max_hot_bytes: int = 2 * 1024 * 1024 * 1024
    max_hot_objects: int = 16_384
    max_hot_completions: int = 65_536
    max_proof_bytes: int = 512 * 1024
    max_database_bytes: int = 256 * 1024 * 1024
    max_archive_bytes: int = 16 * 1024 * 1024 * 1024
    max_archive_objects: int = 131_072
    max_archive_segments: int = 16_384
    max_archived_completions: int = 1_048_576
    max_segment_completions: int = 256
    max_segment_bytes: int = 128 * 1024 * 1024

    def __post_init__(self) -> None:
        values = asdict(self)
        for name, value in values.items():
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"{name} must be a positive integer")
        if self.max_object_bytes > self.max_hot_bytes:
            raise ValueError("object bound exceeds hot byte bound")
        if self.max_segment_completions > self.max_archived_completions:
            raise ValueError("segment completion bound exceeds archive bound")
        if self.max_database_bytes < 1024 * 1024:
            raise ValueError("database bound must be at least one MiB")


@dataclass(frozen=True, slots=True)
class EvidenceRetentionPolicy:
    archive_namespace: str = "acquisition-evidence-shadow-v1"
    min_terminal_age_seconds: float = 0.0
    max_segment_completions: int = 128

    def __post_init__(self) -> None:
        _identifier(self.archive_namespace, "archive namespace")
        _finite(self.min_terminal_age_seconds, "minimum terminal age", minimum=0.0)
        if (
            isinstance(self.max_segment_completions, bool)
            or not isinstance(self.max_segment_completions, int)
            or self.max_segment_completions < 1
        ):
            raise ValueError("max_segment_completions must be positive")

    @property
    def fingerprint(self) -> str:
        return _digest({
            "archive_namespace": self.archive_namespace,
            "contract": ARCHIVE_MANIFEST_SCHEMA,
            "max_segment_completions": self.max_segment_completions,
            "min_terminal_age_seconds": float(self.min_terminal_age_seconds),
        })


@dataclass(frozen=True, slots=True)
class CompletionProducerReceipt:
    producer_receipt_sha256: str
    event_id: str
    event_sha256: str
    acquisition_job_id: str
    consumer_id: str
    stream_id: str
    stream_sequence: int
    global_sequence: int
    desired_state_sha256: str
    authority_sha256: str
    proof_sha256: str
    content_sha256: str
    size_bytes: int
    created_at: float
    ready_at: float

    def as_dict(self) -> dict[str, Any]:
        return {
            "acquisition_job_id": self.acquisition_job_id,
            "authority_sha256": self.authority_sha256,
            "consumer_id": self.consumer_id,
            "content_sha256": self.content_sha256,
            "created_at": self.created_at,
            "desired_state_sha256": self.desired_state_sha256,
            "event_id": self.event_id,
            "event_sha256": self.event_sha256,
            "global_sequence": self.global_sequence,
            "producer_receipt_sha256": self.producer_receipt_sha256,
            "proof_sha256": self.proof_sha256,
            "ready_at": self.ready_at,
            "schema": PRODUCER_RECEIPT_SCHEMA,
            "size_bytes": self.size_bytes,
            "stream_id": self.stream_id,
            "stream_sequence": self.stream_sequence,
        }


@dataclass(frozen=True, slots=True)
class CompletionLease:
    event_id: str
    event_sha256: str
    producer_receipt_sha256: str
    acquisition_job_id: str
    consumer_id: str
    stream_id: str
    stream_sequence: int
    desired_state_sha256: str
    authority_sha256: str
    content_sha256: str
    size_bytes: int
    attempt_count: int
    lease_token: str
    lease_expires_at: float
    proof: dict[str, Any]
    payload: bytes


@dataclass(frozen=True, slots=True)
class ConsumptionReceipt:
    receipt_sha256: str
    event_id: str
    event_sha256: str
    producer_receipt_sha256: str
    consumer_id: str
    stream_id: str
    stream_sequence: int
    output_sha256: str
    created_at: float

    def as_dict(self) -> dict[str, Any]:
        return {
            "consumer_id": self.consumer_id,
            "created_at": self.created_at,
            "event_id": self.event_id,
            "event_sha256": self.event_sha256,
            "output_sha256": self.output_sha256,
            "producer_receipt_sha256": self.producer_receipt_sha256,
            "receipt_sha256": self.receipt_sha256,
            "schema": CONSUMPTION_RECEIPT_SCHEMA,
            "stream_id": self.stream_id,
            "stream_sequence": self.stream_sequence,
        }


@dataclass(frozen=True, slots=True)
class RejectionReceipt:
    """Durable output identity for a fail-closed desired-state rejection."""

    rejection_receipt_sha256: str
    event_id: str
    event_sha256: str
    producer_receipt_sha256: str
    consumer_id: str
    stream_id: str
    stream_sequence: int
    rejected_desired_state_sha256: str
    active_desired_state_sha256: str | None
    reason_code: str
    created_at: float

    def as_dict(self) -> dict[str, Any]:
        return {
            "active_desired_state_sha256": self.active_desired_state_sha256,
            "consumer_id": self.consumer_id,
            "created_at": self.created_at,
            "event_id": self.event_id,
            "event_sha256": self.event_sha256,
            "producer_receipt_sha256": self.producer_receipt_sha256,
            "reason_code": self.reason_code,
            "rejected_desired_state_sha256": self.rejected_desired_state_sha256,
            "rejection_receipt_sha256": self.rejection_receipt_sha256,
            "schema": REJECTION_RECEIPT_SCHEMA,
            "stream_id": self.stream_id,
            "stream_sequence": self.stream_sequence,
        }


@dataclass(frozen=True, slots=True)
class QuarantineReceipt:
    """Durable identity for one event-bound permanent quarantine decision."""

    quarantine_receipt_sha256: str
    event_id: str
    event_sha256: str
    producer_receipt_sha256: str
    consumer_id: str
    stream_id: str
    stream_sequence: int
    evidence_fingerprint_sha256: str
    reason_code: str
    created_at: float

    def as_dict(self) -> dict[str, Any]:
        return {
            "consumer_id": self.consumer_id,
            "created_at": self.created_at,
            "event_id": self.event_id,
            "event_sha256": self.event_sha256,
            "evidence_fingerprint_sha256": self.evidence_fingerprint_sha256,
            "producer_receipt_sha256": self.producer_receipt_sha256,
            "quarantine_receipt_sha256": self.quarantine_receipt_sha256,
            "reason_code": self.reason_code,
            "schema": QUARANTINE_RECEIPT_SCHEMA,
            "stream_id": self.stream_id,
            "stream_sequence": self.stream_sequence,
        }


@dataclass(frozen=True, slots=True)
class QuarantineTerminalResult:
    """Public exact pair proving quarantine and terminal consumption."""

    quarantine_receipt: QuarantineReceipt
    consumption_receipt: ConsumptionReceipt

    @property
    def terminal_kind(self) -> str:
        return "quarantined"

    def as_dict(self) -> dict[str, Any]:
        return {
            "consumption_receipt": self.consumption_receipt.as_dict(),
            "quarantine_receipt": self.quarantine_receipt.as_dict(),
            "terminal_kind": self.terminal_kind,
        }


@dataclass(frozen=True, slots=True)
class CompletionLookup:
    status: str
    stream_id: str
    stream_sequence: int
    event_id: str | None
    producer_receipt_sha256: str | None
    archive_segment_id: str | None
    proof: dict[str, Any] | None = None
    payload: bytes | None = None


@dataclass(frozen=True, slots=True)
class TerminalEventIdentity:
    """Bounded checksum-verified identity of any terminal completion kind."""

    status: str
    event_id: str
    event_sha256: str
    producer_receipt_sha256: str
    consumer_id: str
    stream_id: str
    stream_sequence: int
    terminal_kind: str
    terminal_output_sha256: str | None
    terminal_at: float
    archive_segment_id: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "archive_segment_id": self.archive_segment_id,
            "consumer_id": self.consumer_id,
            "event_id": self.event_id,
            "event_sha256": self.event_sha256,
            "producer_receipt_sha256": self.producer_receipt_sha256,
            "status": self.status,
            "stream_id": self.stream_id,
            "stream_sequence": self.stream_sequence,
            "terminal_at": self.terminal_at,
            "terminal_kind": self.terminal_kind,
            "terminal_output_sha256": self.terminal_output_sha256,
        }

    @property
    def identity_sha256(self) -> str:
        return _digest({
            **self.as_dict(),
            "schema": "stv3-worker-terminal-event-identity-v1",
        })


@dataclass(frozen=True, slots=True)
class AcquisitionCompletionLookup:
    """Exact hot or archived completion selected by acquisition identity."""

    status: str
    producer_receipt: CompletionProducerReceipt
    proof: dict[str, Any] = field(repr=False)
    payload: bytes = field(repr=False)
    archive_segment_id: str | None = None


@dataclass(frozen=True, slots=True)
class TerminalConsumptionEvidence:
    """Exact producer/consumer pair, loaded from hot or sealed storage."""

    status: str
    producer_receipt: CompletionProducerReceipt
    consumption_receipt: ConsumptionReceipt
    archive_segment_id: str | None
    proof: dict[str, Any] = field(repr=False)
    payload: bytes = field(repr=False)


@dataclass(frozen=True, slots=True)
class ArchiveRotationResult:
    status: str
    segment_id: str | None
    archived_completions: int
    pending_prune_plans: int
    hot_completions: int
    archive_anchor_sha256: str


@dataclass(frozen=True, slots=True)
class LifecycleReclamationArchiveMemberV2:
    """Archive-only binding consumed by the native lifecycle participant.

    The source-retention v2 resolver creates these immutable values from one
    sealed prefix.  They deliberately contain no payload, proof, path, token,
    error text or provider response.
    """

    candidate_sha256: str
    acquisition_job_id: str
    event_id: str
    event_sha256: str
    producer_receipt_sha256: str
    consumer_id: str
    stream_id: str
    stream_sequence: int
    terminal_identity_sha256: str
    lifecycle_archive_segment_id: str
    retention_anchor_sha256: str
    retention_segment_sha256: str
    retention_record_sha256: str
    archive_metadata_sha256: str
    archive_object_reference_root_sha256: str
    archive_object_count: int
    archive_object_total_bytes: int

    def __post_init__(self) -> None:
        for value, label in (
            (self.candidate_sha256, "candidate SHA-256"),
            (self.event_sha256, "event SHA-256"),
            (self.producer_receipt_sha256, "producer receipt SHA-256"),
            (self.terminal_identity_sha256, "terminal identity SHA-256"),
            (self.retention_anchor_sha256, "retention anchor SHA-256"),
            (self.retention_segment_sha256, "retention segment SHA-256"),
            (self.retention_record_sha256, "retention record SHA-256"),
            (self.archive_metadata_sha256, "archive metadata SHA-256"),
            (
                self.archive_object_reference_root_sha256,
                "archive object reference root SHA-256",
            ),
        ):
            _sha(value, label)
        for value, label in (
            (self.acquisition_job_id, "acquisition job ID"),
            (self.event_id, "event ID"),
            (self.consumer_id, "consumer ID"),
            (self.stream_id, "stream ID"),
            (self.lifecycle_archive_segment_id, "lifecycle archive segment ID"),
        ):
            _identifier(value, label)
        if (
            isinstance(self.stream_sequence, bool)
            or not isinstance(self.stream_sequence, int)
            or not 1 <= self.stream_sequence <= 2**63 - 1
        ):
            raise AcquisitionEvidenceLifecycleError(
                "invalid reclamation stream sequence"
            )
        for value, label in (
            (self.archive_object_count, "archive object count"),
            (self.archive_object_total_bytes, "archive object bytes"),
        ):
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or not 0 <= value <= 2**63 - 1
            ):
                raise AcquisitionEvidenceLifecycleError(f"invalid {label}")

    def as_dict(self) -> dict[str, Any]:
        return {
            "acquisition_job_id": self.acquisition_job_id,
            "candidate_sha256": self.candidate_sha256,
            "consumer_id": self.consumer_id,
            "event_id": self.event_id,
            "event_sha256": self.event_sha256,
            "lifecycle_archive_segment_id": (
                self.lifecycle_archive_segment_id
            ),
            "producer_receipt_sha256": self.producer_receipt_sha256,
            "archive_metadata_sha256": self.archive_metadata_sha256,
            "archive_object_count": self.archive_object_count,
            "archive_object_reference_root_sha256": (
                self.archive_object_reference_root_sha256
            ),
            "archive_object_total_bytes": self.archive_object_total_bytes,
            "retention_anchor_sha256": self.retention_anchor_sha256,
            "retention_record_sha256": self.retention_record_sha256,
            "retention_segment_sha256": self.retention_segment_sha256,
            "schema": RECLAMATION_MEMBER_SCHEMA,
            "stream_id": self.stream_id,
            "stream_sequence": self.stream_sequence,
            "terminal_identity_sha256": self.terminal_identity_sha256,
        }


def _canonical(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise AcquisitionEvidenceLifecycleError("value is not canonical JSON") from exc


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _sqlite_floor_sha256(
    authority_schema: object,
    pruned_through_segment_ordinal: object,
    pruned_through_segment_id: object,
    pruned_through_file_sha256: object,
    pruned_through_global_sequence: object,
    reclaimed_completion_count: object,
    reclaimed_segment_count: object,
    reclaimed_file_bytes: object,
    last_apply_receipt_sha256: object,
) -> str:
    """Return the canonical floor digest used by durable SQL guards."""

    return reclamation_digest_mapping({
        "authority_schema": str(authority_schema),
        "last_apply_receipt_sha256": last_apply_receipt_sha256,
        "pruned_through_file_sha256": pruned_through_file_sha256,
        "pruned_through_global_sequence": int(
            pruned_through_global_sequence
        ),
        "pruned_through_segment_id": pruned_through_segment_id,
        "pruned_through_segment_ordinal": int(
            pruned_through_segment_ordinal
        ),
        "reclaimed_completion_count": int(reclaimed_completion_count),
        "reclaimed_file_bytes": int(reclaimed_file_bytes),
        "reclaimed_segment_count": int(reclaimed_segment_count),
        "schema": RECLAMATION_FLOOR_SCHEMA,
    })


def _identifier(value: object, field: str) -> str:
    if not isinstance(value, str) or _IDENTIFIER.fullmatch(value) is None:
        raise AcquisitionEvidenceLifecycleError(f"invalid {field}")
    return value


def _sha(value: object, field: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise AcquisitionEvidenceLifecycleError(f"invalid {field}")
    return value


def _finite(value: object, field: str, *, minimum: float = 0.0) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise AcquisitionEvidenceLifecycleError(f"invalid {field}")
    result = float(value)
    if not math.isfinite(result) or not minimum <= result <= 2**53:
        raise AcquisitionEvidenceLifecycleError(f"invalid {field}")
    return result


def _count(value: object, field: str, maximum: int = 2**63 - 1) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not 0 <= value <= maximum
    ):
        raise AcquisitionEvidenceLifecycleError(f"invalid {field}")
    return value


def _assert_secret_free(value: Any, *, depth: int = 0) -> None:
    if depth > 24:
        raise AcquisitionEvidenceCapacityError("proof nesting exceeds bound")
    if isinstance(value, Mapping):
        if len(value) > 4096:
            raise AcquisitionEvidenceCapacityError("proof mapping exceeds bound")
        for key, item in value.items():
            if not isinstance(key, str):
                raise AcquisitionEvidenceLifecycleError("proof key is not text")
            if key == "secret_requirement_ids":
                if not isinstance(item, (list, tuple)) or any(
                    not isinstance(requirement, str)
                    or not requirement.startswith("worker_secret.")
                    or _IDENTIFIER.fullmatch(requirement) is None
                    for requirement in item
                ):
                    raise AcquisitionEvidenceLifecycleError(
                        "secret requirements must remain abstract worker IDs"
                    )
            elif _SECRET_KEY.search(key):
                raise AcquisitionEvidenceLifecycleError(
                    "credential-like key is forbidden in completion proof"
                )
            _assert_secret_free(item, depth=depth + 1)
        return
    if isinstance(value, (list, tuple)):
        if len(value) > 4096:
            raise AcquisitionEvidenceCapacityError("proof sequence exceeds bound")
        for item in value:
            _assert_secret_free(item, depth=depth + 1)
        return
    if isinstance(value, str):
        if len(value.encode("utf-8")) > 64 * 1024:
            raise AcquisitionEvidenceCapacityError("proof string exceeds bound")
        if _SECRET_TEXT.search(value):
            raise AcquisitionEvidenceLifecycleError(
                "credential-like text is forbidden in completion proof"
            )
        return
    if value is None or isinstance(value, (bool, int, float)):
        if isinstance(value, float) and not math.isfinite(value):
            raise AcquisitionEvidenceLifecycleError("non-finite proof number")
        return
    raise AcquisitionEvidenceLifecycleError("proof contains unsupported value")


def desired_authority_proof(state: DesiredState) -> dict[str, Any]:
    if not isinstance(state, DesiredState):
        raise TypeError("state must be DesiredState")
    proof = {
        "authority_epoch": state.authority_epoch,
        "generation": state.generation,
        "producer_id": state.producer_id,
        "schema": DESIRED_AUTHORITY_SCHEMA,
        "state_sha256": state.state_sha256,
    }
    proof["authority_sha256"] = _digest(proof)
    return proof


class AcquisitionEvidenceLifecycleStore:
    """Bounded hot evidence mirror, completion outbox and sealed archive."""

    def __init__(
        self,
        root: str | Path,
        *,
        clock: Callable[[], float] = time.time,
        monotonic: Callable[[], float] = time.monotonic,
        token_factory: Callable[[], str] | None = None,
        limits: AcquisitionEvidenceLimits | None = None,
        fault_injector: Callable[[str], None] | None = None,
        directory_fsync: Callable[[Path], None] | None = None,
    ) -> None:
        self.root = Path(root)
        if not self.root.is_absolute():
            raise ValueError("lifecycle root must be absolute")
        self.path = self.root / "acquisition_evidence.sqlite3"
        self.hot_directory = self.root / "hot_objects"
        self.archive_directory = self.root / "archive"
        self.archive_object_directory = self.archive_directory / "objects"
        self.archive_segment_directory = self.archive_directory / "segments"
        if not callable(clock) or not callable(monotonic):
            raise TypeError("lifecycle clocks must be callable")
        if token_factory is not None and not callable(token_factory):
            raise TypeError("token_factory must be callable")
        if fault_injector is not None and not callable(fault_injector):
            raise TypeError("fault_injector must be callable")
        if directory_fsync is not None and not callable(directory_fsync):
            raise TypeError("directory_fsync must be callable")
        self._clock = clock
        self._monotonic = monotonic
        self._token_factory = token_factory or (lambda: secrets.token_hex(32))
        self._limits = limits or AcquisitionEvidenceLimits()
        self._fault_injector = fault_injector
        self._directory_fsync = directory_fsync or self._default_directory_fsync
        self._lock = threading.RLock()
        self.initialize()

    @staticmethod
    def _default_directory_fsync(path: Path) -> None:
        if os.name == "nt":
            raise AcquisitionEvidenceDurabilityError(
                "directory fsync is unavailable on this platform"
            )
        descriptor: int | None = None
        try:
            descriptor = os.open(
                str(path), os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
            )
            os.fsync(descriptor)
        except OSError as exc:
            if exc.errno == errno.ENOSPC:
                raise AcquisitionEvidenceCapacityError(
                    "directory durability barrier ran out of space"
                ) from exc
            raise AcquisitionEvidenceDurabilityError(
                "directory fsync failed"
            ) from exc
        finally:
            if descriptor is not None:
                os.close(descriptor)

    def _fault(self, point: str) -> None:
        if self._fault_injector is not None:
            self._fault_injector(point)

    def _connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.path, timeout=5.0)
        db.row_factory = sqlite3.Row
        db.create_function(
            "lifecycle_floor_sha256",
            9,
            _sqlite_floor_sha256,
            deterministic=True,
        )
        db.execute("PRAGMA foreign_keys=ON")
        db.execute("PRAGMA synchronous=FULL")
        mode = str(db.execute("PRAGMA journal_mode=DELETE").fetchone()[0]).lower()
        if mode != "delete":
            db.close()
            raise AcquisitionEvidenceDurabilityError("journal mode drift")
        page_size = int(db.execute("PRAGMA page_size").fetchone()[0])
        actual_maximum = int(db.execute(
            f"PRAGMA max_page_count={max(1, self._limits.max_database_bytes // page_size)}"
        ).fetchone()[0])
        if actual_maximum * page_size > self._limits.max_database_bytes:
            db.close()
            raise AcquisitionEvidenceCapacityError(
                "existing lifecycle database exceeds byte bound"
            )
        if hasattr(self, "_store_instance_sha256"):
            try:
                current_instance = self._load_store_instance(db)
            except BaseException:
                db.close()
                raise
            if not hmac.compare_digest(
                current_instance, self._store_instance_sha256
            ):
                db.close()
                raise AcquisitionEvidenceIntegrityError(
                    "lifecycle store-instance authority changed"
                )
        return db

    def _prepare_directories(self) -> None:
        for path in (
            self.root,
            self.hot_directory,
            self.archive_directory,
            self.archive_object_directory,
            self.archive_segment_directory,
        ):
            if path.exists() and (path.is_symlink() or not path.is_dir()):
                raise AcquisitionEvidenceIntegrityError("unsafe lifecycle directory")
            missing = not path.exists()
            path.mkdir(parents=True, exist_ok=True)
            if missing:
                self._directory_fsync(path)
                self._directory_fsync(path.parent)

    @staticmethod
    def _store_instance_trigger_definitions() -> dict[str, str]:
        return {
            "lifecycle_schema_no_update": (
                "CREATE TRIGGER lifecycle_schema_no_update BEFORE UPDATE ON "
                "lifecycle_schema BEGIN SELECT RAISE(ABORT, "
                "'lifecycle schema authority is immutable'); END"
            ),
            "lifecycle_schema_no_delete": (
                "CREATE TRIGGER lifecycle_schema_no_delete BEFORE DELETE ON "
                "lifecycle_schema BEGIN SELECT RAISE(ABORT, "
                "'lifecycle schema authority is immutable'); END"
            ),
        }

    @classmethod
    def _initialize_store_instance_authority(
        cls, db: sqlite3.Connection, *, allow_create: bool = True
    ) -> None:
        expected = cls._store_instance_trigger_definitions()
        for name, statement in expected.items():
            exists = db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='trigger' AND name=?",
                (name,),
            ).fetchone()
            if exists is None and allow_create:
                db.execute(statement)
        rows = db.execute(
            "SELECT name,sql FROM sqlite_master WHERE type='trigger' "
            "AND name IN ('lifecycle_schema_no_update',"
            "'lifecycle_schema_no_delete')"
        ).fetchall()
        actual = {str(row["name"]): str(row["sql"]) for row in rows}
        if set(actual) != set(expected) or any(
            cls._normalized_sql(actual[name])
            != cls._normalized_sql(statement)
            for name, statement in expected.items()
        ):
            raise AcquisitionEvidenceIntegrityError(
                "lifecycle store-instance trigger authority differs"
            )
        cls._load_store_instance(db)

    @staticmethod
    def _load_store_instance(db: sqlite3.Connection) -> str:
        rows = db.execute(
            "SELECT schema_version,store_instance_sha256 "
            "FROM lifecycle_schema WHERE singleton=1"
        ).fetchall()
        if (
            len(rows) != 1
            or int(rows[0]["schema_version"]) != SCHEMA_VERSION
        ):
            raise AcquisitionEvidenceIntegrityError(
                "lifecycle store-instance metadata differs"
            )
        return _sha(
            rows[0]["store_instance_sha256"],
            "lifecycle store-instance SHA-256",
        )

    @staticmethod
    def _clock_sources() -> tuple[tuple[str, tuple[str, ...]], ...]:
        return (
            ("hot_objects", ("created_at",)),
            ("producer_receipts", ("created_at",)),
            ("completion_outbox", ("ready_at", "terminal_at")),
            ("consumer_cursors", ("updated_at",)),
            ("consumption_receipts", ("created_at",)),
            ("rejection_receipts", ("created_at",)),
            ("quarantine_receipts", ("created_at",)),
            ("archive_objects", ("created_at",)),
            ("archive_segments", ("created_at",)),
            ("prune_plans", ("completed_at",)),
            (
                "lifecycle_reclamation_decisions",
                ("prepared_at", "applied_at", "aborted_at"),
            ),
        )

    @staticmethod
    def _normalized_sql(value: str) -> str:
        return " ".join(value.split()).casefold()

    @classmethod
    def _clock_trigger_definitions(cls) -> dict[str, str]:
        definitions = {
            "lifecycle_clock_no_regression": (
                "CREATE TRIGGER lifecycle_clock_no_regression BEFORE UPDATE ON "
                "lifecycle_clock WHEN NEW.singleton<>1 OR "
                "NEW.authority_schema<>OLD.authority_schema OR "
                "NEW.high_water<OLD.high_water BEGIN SELECT RAISE(ABORT, "
                "'lifecycle clock authority cannot regress'); END"
            ),
            "lifecycle_clock_no_delete": (
                "CREATE TRIGGER lifecycle_clock_no_delete BEFORE DELETE ON "
                "lifecycle_clock BEGIN SELECT RAISE(ABORT, "
                "'lifecycle clock authority is immutable'); END"
            ),
        }
        for table, columns in cls._clock_sources():
            values = ",".join(
                f"coalesce(NEW.{column},0.0)" for column in columns
            )
            observed = (
                values if len(columns) == 1 else f"max({values})"
            )
            for operation in ("INSERT", "UPDATE"):
                name = f"lifecycle_clock_{table}_{operation.casefold()}"
                definitions[name] = (
                    f"CREATE TRIGGER {name} AFTER {operation} ON {table} BEGIN "
                    "UPDATE lifecycle_clock SET high_water=CASE WHEN "
                    f"({observed})>high_water THEN ({observed}) ELSE high_water "
                    "END WHERE singleton=1; END"
                )
        return definitions

    @classmethod
    def _validate_clock_triggers(cls, db: sqlite3.Connection) -> None:
        expected = cls._clock_trigger_definitions()
        placeholders = ",".join("?" for _ in expected)
        rows = db.execute(
            "SELECT name,sql FROM sqlite_master WHERE type='trigger' "
            f"AND name IN ({placeholders})",
            tuple(expected),
        ).fetchall()
        actual = {str(row["name"]): str(row["sql"]) for row in rows}
        if set(actual) != set(expected) or any(
            cls._normalized_sql(actual[name])
            != cls._normalized_sql(statement)
            for name, statement in expected.items()
        ):
            raise AcquisitionEvidenceIntegrityError(
                "lifecycle clock trigger authority differs"
            )

    @classmethod
    def _initialize_clock_authority(cls, db: sqlite3.Connection) -> None:
        existed = db.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='table' "
            "AND name='lifecycle_clock'"
        ).fetchone()[0] == 1
        db.execute(
            "CREATE TABLE IF NOT EXISTS lifecycle_clock("
            "singleton INTEGER PRIMARY KEY CHECK(singleton=1),"
            "authority_schema TEXT NOT NULL,"
            "high_water REAL NOT NULL CHECK(high_water>=0))"
        )
        columns = {
            str(row[1]) for row in db.execute(
                "PRAGMA table_info(lifecycle_clock)"
            )
        }
        if columns != {"singleton", "authority_schema", "high_water"}:
            raise AcquisitionEvidenceIntegrityError(
                "lifecycle clock table schema differs"
            )
        if not existed:
            db.execute(
                "INSERT INTO lifecycle_clock VALUES(1,?,0.0)",
                (CLOCK_AUTHORITY_SCHEMA,),
            )
        for statement in cls._clock_trigger_definitions().values():
            db.execute(
                statement.replace(
                    "CREATE TRIGGER ", "CREATE TRIGGER IF NOT EXISTS ", 1
                )
            )
        cls._validate_clock_triggers(db)
        rows = db.execute(
            "SELECT * FROM lifecycle_clock WHERE singleton=1"
        ).fetchall()
        if (
            len(rows) != 1
            or str(rows[0]["authority_schema"]) != CLOCK_AUTHORITY_SCHEMA
        ):
            raise AcquisitionEvidenceIntegrityError(
                "lifecycle clock authority metadata differs"
            )
        high_water = _finite(
            rows[0]["high_water"], "lifecycle durable clock high-water"
        )
        if not existed:
            for table, source_columns in cls._clock_sources():
                for column in source_columns:
                    row = db.execute(
                        f"SELECT coalesce(max({column}),0.0) AS observed "
                        f"FROM {table}"
                    ).fetchone()
                    if row is not None:
                        high_water = max(
                            high_water,
                            _finite(
                                row["observed"],
                                f"lifecycle {table} migration clock",
                            ),
                        )
            db.execute(
                "UPDATE lifecycle_clock SET high_water=? WHERE singleton=1",
                (high_water,),
            )

    @staticmethod
    def _execute_transactional_ddl(
        db: sqlite3.Connection,
        script: str,
    ) -> None:
        """Execute a DDL script without ``executescript``'s implicit commit."""

        statement = ""
        for line in script.splitlines():
            statement += line + "\n"
            if sqlite3.complete_statement(statement):
                sql = statement.strip()
                statement = ""
                if sql:
                    db.execute(sql)
        if statement.strip():
            raise AcquisitionEvidenceIntegrityError(
                "lifecycle schema DDL is incomplete"
            )

    @staticmethod
    def _lifecycle_tables(db: sqlite3.Connection) -> frozenset[str]:
        return frozenset(
            str(row[0])
            for row in db.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name NOT LIKE 'sqlite_%'"
            )
        )

    @classmethod
    def _reclamation_trigger_definitions(cls) -> dict[str, str]:
        return {
            "prune_plans_insert_guard": (
                "CREATE TRIGGER prune_plans_insert_guard BEFORE INSERT ON "
                "prune_plans WHEN NEW.phase<>0 OR NEW.completed_at IS NOT NULL "
                "BEGIN SELECT RAISE(ABORT,'prune plan must start at phase zero'); END"
            ),
            "prune_plans_transition_guard": (
                "CREATE TRIGGER prune_plans_transition_guard BEFORE UPDATE ON "
                "prune_plans WHEN NEW.segment_id<>OLD.segment_id OR "
                "NEW.event_ids_json<>OLD.event_ids_json OR "
                "NEW.producer_ids_json<>OLD.producer_ids_json OR "
                "NEW.object_ids_json<>OLD.object_ids_json OR "
                "OLD.completed_at IS NOT NULL OR NEW.phase<>OLD.phase+1 OR "
                "(NEW.phase<6 AND NEW.completed_at IS NOT NULL) OR "
                "(NEW.phase=6 AND NEW.completed_at IS NULL) OR NEW.phase>6 "
                "BEGIN SELECT RAISE(ABORT,'prune plan transition differs'); END"
            ),
            "prune_plans_no_delete": (
                "CREATE TRIGGER prune_plans_no_delete BEFORE DELETE ON "
                "prune_plans BEGIN SELECT RAISE(ABORT,'prune plan is immutable'); END"
            ),
            "lifecycle_reclamation_decisions_insert_guard": (
                "CREATE TRIGGER lifecycle_reclamation_decisions_insert_guard "
                "BEFORE INSERT ON lifecycle_reclamation_decisions WHEN "
                "NEW.state<>'PREPARED' OR NEW.active_slot<>1 OR "
                "NEW.journal_sealed<>0 OR "
                "NEW.committed_decision_sha256 IS NOT NULL OR "
                "NEW.committed_receipt_sha256 IS NOT NULL OR "
                "NEW.effect_sha256 IS NULL OR NEW.effect_json IS NULL OR "
                "NEW.post_apply_view_sha256 IS NOT NULL OR "
                "NEW.apply_receipt_sha256 IS NOT NULL OR "
                "NEW.apply_receipt_json IS NOT NULL OR "
                "NEW.abort_decision_sha256 IS NOT NULL OR "
                "NEW.fence_release_root_sha256 IS NOT NULL OR "
                "NEW.abort_receipt_json IS NOT NULL OR NEW.applied_at IS NOT NULL OR "
                "NEW.aborted_at IS NOT NULL BEGIN SELECT RAISE(ABORT,"
                "'reclamation decision must start prepared'); END"
            ),
            "lifecycle_reclamation_decisions_transition_guard": (
                "CREATE TRIGGER lifecycle_reclamation_decisions_transition_guard "
                "BEFORE UPDATE ON lifecycle_reclamation_decisions WHEN "
                "NEW.request_sha256 IS NOT OLD.request_sha256 OR "
                "NEW.request_json IS NOT OLD.request_json OR "
                "NEW.prepare_receipt_sha256 IS NOT OLD.prepare_receipt_sha256 OR "
                "NEW.prepare_receipt_json IS NOT OLD.prepare_receipt_json OR "
                "NEW.source_fence_sha256 IS NOT OLD.source_fence_sha256 OR "
                "NEW.source_view_sha256 IS NOT OLD.source_view_sha256 OR "
                "NEW.child_reference_root_sha256 IS NOT "
                "OLD.child_reference_root_sha256 OR "
                "NEW.floor_before_sha256 IS NOT OLD.floor_before_sha256 OR "
                "NEW.floor_before_json IS NOT OLD.floor_before_json OR "
                "NEW.prior_apply_receipt_sha256 IS NOT "
                "OLD.prior_apply_receipt_sha256 OR "
                "NEW.expected_floor_after_sha256 IS NOT "
                "OLD.expected_floor_after_sha256 OR "
                "NEW.expected_reclaimed_completion_count IS NOT "
                "OLD.expected_reclaimed_completion_count OR "
                "NEW.expected_reclaimed_segment_count IS NOT "
                "OLD.expected_reclaimed_segment_count OR "
                "NEW.expected_reclaimed_file_bytes IS NOT "
                "OLD.expected_reclaimed_file_bytes OR "
                "NEW.target_floor_ordinal IS NOT OLD.target_floor_ordinal OR "
                "NEW.target_floor_segment_id IS NOT OLD.target_floor_segment_id OR "
                "NEW.target_floor_file_sha256 IS NOT "
                "OLD.target_floor_file_sha256 OR "
                "NEW.target_floor_global_sequence IS NOT "
                "OLD.target_floor_global_sequence OR "
                "NEW.target_segment_count IS NOT OLD.target_segment_count OR "
                "NEW.candidate_count IS NOT OLD.candidate_count OR "
                "NEW.releasable_bytes IS NOT OLD.releasable_bytes OR "
                "NEW.releasable_entries IS NOT OLD.releasable_entries OR "
                "NEW.coordinator_authority_sha256 IS NOT "
                "OLD.coordinator_authority_sha256 OR "
                "NEW.effect_json IS NOT OLD.effect_json OR "
                "NEW.effect_sha256 IS NOT OLD.effect_sha256 OR "
                "NEW.prepared_at IS NOT OLD.prepared_at OR "
                "NEW.fence_epoch IS NOT OLD.fence_epoch OR NOT ("
                "(OLD.state='PREPARED' AND NEW.state='PREPARED' AND "
                "OLD.journal_sealed=0 AND NEW.journal_sealed=1 AND "
                "NEW.active_slot=1 AND "
                "NEW.committed_decision_sha256 IS NULL AND "
                "NEW.committed_receipt_sha256 IS NULL AND "
                "NEW.post_apply_view_sha256 IS NULL AND "
                "NEW.apply_receipt_sha256 IS NULL AND "
                "NEW.apply_receipt_json IS NULL AND "
                "NEW.abort_decision_sha256 IS NULL AND "
                "NEW.fence_release_root_sha256 IS NULL AND "
                "NEW.abort_receipt_json IS NULL AND NEW.applied_at IS NULL "
                "AND NEW.aborted_at IS NULL) OR "
                "(OLD.state='PREPARED' AND NEW.state='APPLYING' AND "
                "OLD.journal_sealed=1 AND NEW.journal_sealed=1 AND "
                "NEW.active_slot=1 AND NEW.committed_decision_sha256 IS NOT NULL "
                "AND NEW.committed_receipt_sha256 IS NOT NULL AND "
                "NEW.post_apply_view_sha256 IS NULL AND "
                "NEW.apply_receipt_sha256 IS NULL AND "
                "NEW.apply_receipt_json IS NULL AND "
                "NEW.abort_decision_sha256 IS NULL AND "
                "NEW.fence_release_root_sha256 IS NULL AND "
                "NEW.abort_receipt_json IS NULL AND NEW.applied_at IS NULL AND "
                "NEW.aborted_at IS NULL) OR "
                "(OLD.state='PREPARED' AND NEW.state='ABORTED' AND "
                "OLD.journal_sealed=1 AND NEW.journal_sealed=1 AND "
                "NEW.active_slot IS NULL AND "
                "NEW.committed_decision_sha256 IS NULL AND "
                "NEW.committed_receipt_sha256 IS NULL AND "
                "NEW.post_apply_view_sha256 IS NULL AND "
                "NEW.apply_receipt_sha256 IS NULL AND "
                "NEW.apply_receipt_json IS NULL AND "
                "NEW.abort_decision_sha256 IS NOT NULL AND "
                "NEW.fence_release_root_sha256 IS NOT NULL AND "
                "NEW.abort_receipt_json IS NOT NULL AND NEW.applied_at IS NULL "
                "AND NEW.aborted_at IS NOT NULL) OR "
                "(OLD.state='APPLYING' AND NEW.state='APPLIED' AND "
                "OLD.journal_sealed=1 AND NEW.journal_sealed=1 AND "
                "NEW.active_slot IS NULL AND "
                "NEW.committed_decision_sha256 IS OLD.committed_decision_sha256 "
                "AND NEW.committed_receipt_sha256 IS OLD.committed_receipt_sha256 "
                "AND NEW.effect_sha256 IS OLD.effect_sha256 AND "
                "NEW.post_apply_view_sha256 IS NOT NULL AND "
                "NEW.apply_receipt_sha256 IS NOT NULL AND "
                "NEW.apply_receipt_json IS NOT NULL AND "
                "NEW.abort_decision_sha256 IS NULL AND "
                "NEW.fence_release_root_sha256 IS NULL AND "
                "NEW.abort_receipt_json IS NULL AND NEW.applied_at IS NOT NULL "
                "AND NEW.aborted_at IS NULL)) BEGIN SELECT RAISE(ABORT,"
                "'reclamation decision transition differs'); END"
            ),
            "lifecycle_reclamation_meta_no_delete": (
                "CREATE TRIGGER lifecycle_reclamation_meta_no_delete BEFORE DELETE "
                "ON lifecycle_reclamation_meta BEGIN SELECT RAISE(ABORT,"
                "'reclamation metadata is immutable'); END"
            ),
            "lifecycle_reclamation_meta_transition_guard": (
                "CREATE TRIGGER lifecycle_reclamation_meta_transition_guard BEFORE "
                "UPDATE ON lifecycle_reclamation_meta WHEN NEW.singleton<>OLD.singleton "
                "OR NEW.authority_schema<>OLD.authority_schema OR "
                "NEW.next_fence_epoch<>OLD.next_fence_epoch+1 OR "
                "NEW.last_observed_at IS NULL OR (OLD.last_observed_at IS NOT NULL "
                "AND NEW.last_observed_at<OLD.last_observed_at) BEGIN SELECT "
                "RAISE(ABORT,'reclamation metadata transition differs'); END"
            ),
            "lifecycle_reclamation_decisions_no_delete": (
                "CREATE TRIGGER lifecycle_reclamation_decisions_no_delete "
                "BEFORE DELETE ON lifecycle_reclamation_decisions BEGIN SELECT "
                "RAISE(ABORT,'reclamation decision is immutable'); END"
            ),
            "lifecycle_reclamation_members_no_update": (
                "CREATE TRIGGER lifecycle_reclamation_members_no_update BEFORE "
                "UPDATE ON lifecycle_reclamation_members BEGIN SELECT RAISE(ABORT,"
                "'reclamation member is immutable'); END"
            ),
            "lifecycle_reclamation_members_no_delete": (
                "CREATE TRIGGER lifecycle_reclamation_members_no_delete BEFORE "
                "DELETE ON lifecycle_reclamation_members BEGIN SELECT RAISE(ABORT,"
                "'reclamation member is immutable'); END"
            ),
            "lifecycle_reclamation_files_no_update": (
                "CREATE TRIGGER lifecycle_reclamation_files_no_update BEFORE "
                "UPDATE ON lifecycle_reclamation_files BEGIN SELECT RAISE(ABORT,"
                "'reclamation file is immutable'); END"
            ),
            "lifecycle_reclamation_files_no_delete": (
                "CREATE TRIGGER lifecycle_reclamation_files_no_delete BEFORE "
                "DELETE ON lifecycle_reclamation_files BEGIN SELECT RAISE(ABORT,"
                "'reclamation file is immutable'); END"
            ),
            "lifecycle_archive_floor_no_delete": (
                "CREATE TRIGGER lifecycle_archive_floor_no_delete BEFORE DELETE "
                "ON lifecycle_archive_floor BEGIN SELECT RAISE(ABORT,"
                "'archive reclamation floor is immutable'); END"
            ),
            "lifecycle_archive_floor_transition_guard": (
                "CREATE TRIGGER lifecycle_archive_floor_transition_guard BEFORE "
                "UPDATE ON lifecycle_archive_floor WHEN "
                "NEW.singleton<>OLD.singleton OR NEW.authority_schema<>"
                "OLD.authority_schema OR (NOT EXISTS (SELECT 1 FROM "
                "lifecycle_reclamation_decisions AS decision WHERE "
                "decision.state='APPLYING' AND "
                "lifecycle_floor_sha256(OLD.authority_schema,"
                "OLD.pruned_through_segment_ordinal,OLD.pruned_through_segment_id,"
                "OLD.pruned_through_file_sha256,"
                "OLD.pruned_through_global_sequence,"
                "OLD.reclaimed_completion_count,OLD.reclaimed_segment_count,"
                "OLD.reclaimed_file_bytes,OLD.last_apply_receipt_sha256)="
                "decision.floor_before_sha256 AND "
                "lifecycle_floor_sha256(NEW.authority_schema,"
                "NEW.pruned_through_segment_ordinal,NEW.pruned_through_segment_id,"
                "NEW.pruned_through_file_sha256,"
                "NEW.pruned_through_global_sequence,"
                "NEW.reclaimed_completion_count,NEW.reclaimed_segment_count,"
                "NEW.reclaimed_file_bytes,NEW.last_apply_receipt_sha256)="
                "decision.expected_floor_after_sha256 AND "
                "NEW.pruned_through_segment_ordinal=decision.target_floor_ordinal "
                "AND NEW.pruned_through_segment_id=decision.target_floor_segment_id "
                "AND NEW.pruned_through_file_sha256="
                "decision.target_floor_file_sha256 AND "
                "NEW.pruned_through_global_sequence="
                "decision.target_floor_global_sequence AND "
                "NEW.reclaimed_completion_count="
                "decision.expected_reclaimed_completion_count AND "
                "NEW.reclaimed_segment_count="
                "decision.expected_reclaimed_segment_count AND "
                "NEW.reclaimed_file_bytes=decision.expected_reclaimed_file_bytes "
                "AND NEW.last_apply_receipt_sha256 IS "
                "decision.prior_apply_receipt_sha256) AND NOT EXISTS (SELECT 1 "
                "FROM lifecycle_reclamation_decisions AS decision WHERE "
                "decision.state='APPLIED' AND "
                "decision.apply_receipt_sha256=NEW.last_apply_receipt_sha256 AND "
                "OLD.last_apply_receipt_sha256 IS "
                "decision.prior_apply_receipt_sha256 AND "
                "NEW.pruned_through_segment_ordinal="
                "OLD.pruned_through_segment_ordinal AND "
                "NEW.pruned_through_segment_id IS OLD.pruned_through_segment_id "
                "AND NEW.pruned_through_file_sha256 IS "
                "OLD.pruned_through_file_sha256 AND "
                "NEW.pruned_through_global_sequence="
                "OLD.pruned_through_global_sequence AND "
                "NEW.reclaimed_completion_count=OLD.reclaimed_completion_count "
                "AND NEW.reclaimed_segment_count=OLD.reclaimed_segment_count "
                "AND NEW.reclaimed_file_bytes=OLD.reclaimed_file_bytes AND "
                "lifecycle_floor_sha256(OLD.authority_schema,"
                "OLD.pruned_through_segment_ordinal,OLD.pruned_through_segment_id,"
                "OLD.pruned_through_file_sha256,"
                "OLD.pruned_through_global_sequence,"
                "OLD.reclaimed_completion_count,OLD.reclaimed_segment_count,"
                "OLD.reclaimed_file_bytes,OLD.last_apply_receipt_sha256)="
                "decision.expected_floor_after_sha256)) BEGIN SELECT RAISE(ABORT,"
                "'archive reclamation floor transition differs'); END"
            ),
            "lifecycle_reclamation_applied_floor_head": (
                "CREATE TRIGGER lifecycle_reclamation_applied_floor_head AFTER "
                "UPDATE OF state ON lifecycle_reclamation_decisions WHEN "
                "OLD.state='APPLYING' AND NEW.state='APPLIED' BEGIN UPDATE "
                "lifecycle_archive_floor SET last_apply_receipt_sha256="
                "NEW.apply_receipt_sha256 WHERE singleton=1 AND "
                "last_apply_receipt_sha256 IS NEW.prior_apply_receipt_sha256 "
                "AND lifecycle_floor_sha256(authority_schema,"
                "pruned_through_segment_ordinal,pruned_through_segment_id,"
                "pruned_through_file_sha256,pruned_through_global_sequence,"
                "reclaimed_completion_count,reclaimed_segment_count,"
                "reclaimed_file_bytes,last_apply_receipt_sha256)="
                "NEW.expected_floor_after_sha256; SELECT CASE WHEN changes()<>1 "
                "THEN RAISE(ABORT,'archive reclamation floor head differs') END; END"
            ),
            "archive_index_no_delete": (
                "CREATE TRIGGER archive_index_no_delete BEFORE DELETE ON "
                "archive_index WHEN NOT EXISTS (SELECT 1 FROM "
                "lifecycle_reclamation_members AS member JOIN "
                "lifecycle_reclamation_decisions AS decision USING(request_sha256) "
                "WHERE decision.state='APPLYING' AND member.event_id=OLD.event_id "
                "AND member.acquisition_job_id=OLD.acquisition_job_id AND "
                "member.event_sha256=OLD.event_sha256 AND "
                "member.producer_receipt_sha256=OLD.producer_receipt_sha256 AND "
                "member.consumer_id=OLD.consumer_id AND "
                "member.stream_id=OLD.stream_id AND "
                "member.stream_sequence=OLD.stream_sequence AND "
                "member.archive_global_sequence=OLD.global_sequence AND "
                "member.archive_content_sha256=OLD.content_sha256 AND "
                "member.lifecycle_archive_segment_id=OLD.segment_id) "
                "BEGIN SELECT RAISE(ABORT,'archive index is immutable'); END"
            ),
            "archive_segments_no_delete": (
                "CREATE TRIGGER archive_segments_no_delete BEFORE DELETE ON "
                "archive_segments BEGIN SELECT RAISE(ABORT,"
                "'archive segment is immutable'); END"
            ),
            "archive_segments_no_update": (
                "CREATE TRIGGER archive_segments_no_update BEFORE UPDATE ON "
                "archive_segments BEGIN SELECT RAISE(ABORT,"
                "'archive segment is immutable'); END"
            ),
            "archive_index_no_update": (
                "CREATE TRIGGER archive_index_no_update BEFORE UPDATE ON "
                "archive_index BEGIN SELECT RAISE(ABORT,"
                "'archive index is immutable'); END"
            ),
            "archive_objects_no_update": (
                "CREATE TRIGGER archive_objects_no_update BEFORE UPDATE ON "
                "archive_objects BEGIN SELECT RAISE(ABORT,"
                "'archive object is immutable'); END"
            ),
            "archive_objects_no_delete": (
                "CREATE TRIGGER archive_objects_no_delete BEFORE DELETE ON "
                "archive_objects WHEN NOT EXISTS (SELECT 1 FROM "
                "lifecycle_reclamation_files AS file JOIN "
                "lifecycle_reclamation_decisions AS decision USING(request_sha256) "
                "WHERE decision.state='APPLYING' AND file.file_kind='object' "
                "AND file.identity_sha256=OLD.content_sha256 AND "
                "file.content_sha256=OLD.content_sha256 AND "
                "file.size_bytes=OLD.size_bytes AND file.filename=OLD.filename) "
                "BEGIN SELECT RAISE(ABORT,"
                "'archive object is immutable'); END"
            ),
            "lifecycle_reclamation_members_insert_guard": (
                "CREATE TRIGGER lifecycle_reclamation_members_insert_guard "
                "BEFORE INSERT ON lifecycle_reclamation_members WHEN NOT EXISTS "
                "(SELECT 1 FROM lifecycle_reclamation_decisions AS decision "
                "WHERE decision.request_sha256=NEW.request_sha256 AND "
                "decision.state='PREPARED' AND decision.journal_sealed=0) BEGIN "
                "SELECT RAISE(ABORT,'reclamation member journal is sealed'); END"
            ),
            "lifecycle_reclamation_files_insert_guard": (
                "CREATE TRIGGER lifecycle_reclamation_files_insert_guard "
                "BEFORE INSERT ON lifecycle_reclamation_files WHEN NOT EXISTS "
                "(SELECT 1 FROM lifecycle_reclamation_decisions AS decision "
                "WHERE decision.request_sha256=NEW.request_sha256 AND "
                "decision.state='PREPARED' AND decision.journal_sealed=0) BEGIN "
                "SELECT RAISE(ABORT,'reclamation file journal is sealed'); END"
            ),
            "lifecycle_reclamation_object_references_insert_guard": (
                "CREATE TRIGGER lifecycle_reclamation_object_references_insert_guard "
                "BEFORE INSERT ON lifecycle_reclamation_object_references WHEN "
                "NOT EXISTS (SELECT 1 FROM lifecycle_reclamation_decisions AS "
                "decision WHERE decision.request_sha256=NEW.request_sha256 AND "
                "decision.state='PREPARED' AND decision.journal_sealed=0) BEGIN "
                "SELECT RAISE(ABORT,'reclamation object journal is sealed'); END"
            ),
            "lifecycle_reclamation_object_references_no_update": (
                "CREATE TRIGGER lifecycle_reclamation_object_references_no_update "
                "BEFORE UPDATE ON lifecycle_reclamation_object_references BEGIN "
                "SELECT RAISE(ABORT,'reclamation object journal is immutable'); END"
            ),
            "lifecycle_reclamation_object_references_no_delete": (
                "CREATE TRIGGER lifecycle_reclamation_object_references_no_delete "
                "BEFORE DELETE ON lifecycle_reclamation_object_references BEGIN "
                "SELECT RAISE(ABORT,'reclamation object journal is immutable'); END"
            ),
            "producer_receipts_reclamation_insert_guard": (
                "CREATE TRIGGER producer_receipts_reclamation_insert_guard BEFORE "
                "INSERT ON producer_receipts WHEN EXISTS (SELECT 1 FROM "
                "lifecycle_reclamation_members AS member JOIN "
                "lifecycle_reclamation_decisions AS decision USING(request_sha256) "
                "WHERE decision.state IN ('PREPARED','APPLYING','APPLIED') AND "
                "(NEW.producer_receipt_sha256=member.producer_receipt_sha256 OR "
                "NEW.acquisition_job_id=member.acquisition_job_id OR "
                "(NEW.stream_id=member.stream_id AND "
                "NEW.stream_sequence=member.stream_sequence))) BEGIN SELECT "
                "RAISE(ABORT,'reclaimed producer identity is fenced'); END"
            ),
            "completion_outbox_reclamation_insert_guard": (
                "CREATE TRIGGER completion_outbox_reclamation_insert_guard BEFORE "
                "INSERT ON completion_outbox WHEN EXISTS (SELECT 1 FROM "
                "lifecycle_reclamation_members AS member JOIN "
                "lifecycle_reclamation_decisions AS decision USING(request_sha256) "
                "WHERE decision.state IN ('PREPARED','APPLYING','APPLIED') AND "
                "(NEW.event_id=member.event_id OR "
                "NEW.event_sha256=member.event_sha256 OR "
                "NEW.producer_receipt_sha256=member.producer_receipt_sha256 OR "
                "(NEW.consumer_id=member.consumer_id AND "
                "NEW.stream_id=member.stream_id AND "
                "NEW.stream_sequence=member.stream_sequence))) BEGIN SELECT "
                "RAISE(ABORT,'reclaimed completion identity is fenced'); END"
            ),
            "consumption_receipts_reclamation_insert_guard": (
                "CREATE TRIGGER consumption_receipts_reclamation_insert_guard "
                "BEFORE INSERT ON consumption_receipts WHEN EXISTS (SELECT 1 FROM "
                "lifecycle_reclamation_members AS member JOIN "
                "lifecycle_reclamation_decisions AS decision USING(request_sha256) "
                "WHERE decision.state IN ('PREPARED','APPLYING','APPLIED') AND "
                "(NEW.event_id=member.event_id OR "
                "NEW.event_sha256=member.event_sha256 OR "
                "NEW.producer_receipt_sha256=member.producer_receipt_sha256 OR "
                "(NEW.consumer_id=member.consumer_id AND "
                "NEW.stream_id=member.stream_id AND "
                "NEW.stream_sequence=member.stream_sequence))) BEGIN SELECT "
                "RAISE(ABORT,'reclaimed consumption identity is fenced'); END"
            ),
            "rejection_receipts_reclamation_insert_guard": (
                "CREATE TRIGGER rejection_receipts_reclamation_insert_guard "
                "BEFORE INSERT ON rejection_receipts WHEN EXISTS (SELECT 1 FROM "
                "lifecycle_reclamation_members AS member JOIN "
                "lifecycle_reclamation_decisions AS decision USING(request_sha256) "
                "WHERE decision.state IN ('PREPARED','APPLYING','APPLIED') AND "
                "(NEW.event_id=member.event_id OR "
                "NEW.event_sha256=member.event_sha256 OR "
                "NEW.producer_receipt_sha256=member.producer_receipt_sha256 OR "
                "(NEW.consumer_id=member.consumer_id AND "
                "NEW.stream_id=member.stream_id AND "
                "NEW.stream_sequence=member.stream_sequence))) BEGIN SELECT "
                "RAISE(ABORT,'reclaimed rejection identity is fenced'); END"
            ),
            "quarantine_receipts_reclamation_insert_guard": (
                "CREATE TRIGGER quarantine_receipts_reclamation_insert_guard "
                "BEFORE INSERT ON quarantine_receipts WHEN EXISTS (SELECT 1 FROM "
                "lifecycle_reclamation_members AS member JOIN "
                "lifecycle_reclamation_decisions AS decision USING(request_sha256) "
                "WHERE decision.state IN ('PREPARED','APPLYING','APPLIED') AND "
                "(NEW.event_id=member.event_id OR "
                "NEW.event_sha256=member.event_sha256 OR "
                "NEW.producer_receipt_sha256=member.producer_receipt_sha256 OR "
                "(NEW.consumer_id=member.consumer_id AND "
                "NEW.stream_id=member.stream_id AND "
                "NEW.stream_sequence=member.stream_sequence))) BEGIN SELECT "
                "RAISE(ABORT,'reclaimed quarantine identity is fenced'); END"
            ),
            "archive_segments_reclamation_insert_guard": (
                "CREATE TRIGGER archive_segments_reclamation_insert_guard BEFORE "
                "INSERT ON archive_segments WHEN NEW.segment_ordinal<="
                "(SELECT pruned_through_segment_ordinal FROM "
                "lifecycle_archive_floor WHERE singleton=1) OR EXISTS (SELECT 1 "
                "FROM lifecycle_reclamation_files AS file JOIN "
                "lifecycle_reclamation_decisions AS decision USING(request_sha256) "
                "WHERE decision.state IN ('PREPARED','APPLYING','APPLIED') AND "
                "file.file_kind='segment' AND (file.identity_sha256=NEW.segment_id "
                "OR file.content_sha256=NEW.file_sha256)) BEGIN SELECT "
                "RAISE(ABORT,'reclaimed archive segment is fenced'); END"
            ),
            "archive_index_reclamation_insert_guard": (
                "CREATE TRIGGER archive_index_reclamation_insert_guard BEFORE "
                "INSERT ON archive_index WHEN EXISTS (SELECT 1 FROM "
                "archive_segments AS segment JOIN lifecycle_archive_floor AS floor "
                "ON floor.singleton=1 WHERE segment.segment_id=NEW.segment_id AND "
                "segment.segment_ordinal<=floor.pruned_through_segment_ordinal) OR "
                "EXISTS (SELECT 1 FROM lifecycle_reclamation_members AS member "
                "JOIN lifecycle_reclamation_decisions AS decision "
                "USING(request_sha256) WHERE decision.state IN "
                "('PREPARED','APPLYING','APPLIED') AND "
                "(NEW.event_id=member.event_id OR "
                "NEW.event_sha256=member.event_sha256 OR "
                "NEW.producer_receipt_sha256=member.producer_receipt_sha256 OR "
                "NEW.acquisition_job_id=member.acquisition_job_id OR "
                "(NEW.stream_id=member.stream_id AND "
                "NEW.stream_sequence=member.stream_sequence))) OR EXISTS (SELECT 1 "
                "FROM lifecycle_reclamation_files AS file JOIN "
                "lifecycle_reclamation_decisions AS decision USING(request_sha256) "
                "WHERE decision.state IN ('PREPARED','APPLYING','APPLIED') AND "
                "file.file_kind='segment' AND file.identity_sha256=NEW.segment_id) "
                "OR EXISTS (SELECT 1 FROM "
                "lifecycle_reclamation_object_references AS reference JOIN "
                "lifecycle_reclamation_decisions AS decision USING(request_sha256) "
                "WHERE decision.state IN ('PREPARED','APPLYING') AND "
                "reference.content_sha256=NEW.content_sha256) BEGIN SELECT "
                "RAISE(ABORT,'reclaimed archive index is fenced'); END"
            ),
            "archive_objects_reclamation_insert_guard": (
                "CREATE TRIGGER archive_objects_reclamation_insert_guard BEFORE "
                "INSERT ON archive_objects WHEN EXISTS (SELECT 1 FROM "
                "lifecycle_reclamation_object_references AS reference JOIN "
                "lifecycle_reclamation_decisions AS decision USING(request_sha256) "
                "WHERE decision.state IN ('PREPARED','APPLYING') AND "
                "reference.content_sha256=NEW.content_sha256) OR EXISTS (SELECT 1 "
                "FROM lifecycle_reclamation_object_references AS reference JOIN "
                "lifecycle_reclamation_decisions AS decision USING(request_sha256) "
                "WHERE decision.state='APPLIED' AND "
                "reference.content_sha256=NEW.content_sha256 AND "
                "(reference.size_bytes<>NEW.size_bytes OR "
                "reference.filename<>NEW.filename OR NOT EXISTS (SELECT 1 "
                "FROM hot_objects AS hot WHERE hot.content_sha256="
                "NEW.content_sha256 AND hot.size_bytes=NEW.size_bytes AND "
                "hot.filename=NEW.filename))) BEGIN SELECT RAISE(ABORT,"
                "'reclaimed archive object identity differs'); END"
            ),
        }

    @classmethod
    def _initialize_reclamation_triggers(
        cls,
        db: sqlite3.Connection,
        *,
        allow_create: bool,
    ) -> None:
        expected = cls._reclamation_trigger_definitions()
        for name, statement in expected.items():
            row = db.execute(
                "SELECT sql FROM sqlite_master WHERE type='trigger' AND name=?",
                (name,),
            ).fetchone()
            if row is None and allow_create:
                db.execute(statement)
        complete_expected = {
            **cls._store_instance_trigger_definitions(),
            **cls._clock_trigger_definitions(),
            **expected,
        }
        rows = db.execute(
            "SELECT name,sql FROM sqlite_master WHERE type='trigger'"
        ).fetchall()
        actual = {str(row["name"]): str(row["sql"]) for row in rows}
        if set(actual) != set(complete_expected) or any(
            cls._normalized_sql(actual[name])
            != cls._normalized_sql(statement)
            for name, statement in complete_expected.items()
        ):
            raise AcquisitionEvidenceIntegrityError(
                "lifecycle reclamation trigger authority differs"
            )

    @staticmethod
    def _v4_reclamation_table_definitions() -> dict[str, str]:
        return {
            "lifecycle_archive_floor": """
                CREATE TABLE lifecycle_archive_floor(
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                    authority_schema TEXT NOT NULL,
                    pruned_through_segment_ordinal INTEGER NOT NULL
                        CHECK(pruned_through_segment_ordinal>=0),
                    pruned_through_segment_id TEXT,
                    pruned_through_file_sha256 TEXT,
                    pruned_through_global_sequence INTEGER NOT NULL
                        CHECK(pruned_through_global_sequence>=0),
                    reclaimed_completion_count INTEGER NOT NULL
                        CHECK(reclaimed_completion_count>=0),
                    reclaimed_segment_count INTEGER NOT NULL
                        CHECK(reclaimed_segment_count>=0),
                    reclaimed_file_bytes INTEGER NOT NULL
                        CHECK(reclaimed_file_bytes>=0),
                    last_apply_receipt_sha256 TEXT
                )
            """,
            "lifecycle_reclamation_meta": """
                CREATE TABLE lifecycle_reclamation_meta(
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                    authority_schema TEXT NOT NULL,
                    next_fence_epoch INTEGER NOT NULL
                        CHECK(next_fence_epoch>=1),
                    last_observed_at REAL
                )
            """,
            "lifecycle_reclamation_decisions": """
                CREATE TABLE lifecycle_reclamation_decisions(
                    request_sha256 TEXT PRIMARY KEY,
                    state TEXT NOT NULL CHECK(state IN
                        ('PREPARED','APPLYING','APPLIED','ABORTED')),
                    active_slot INTEGER UNIQUE CHECK(active_slot=1),
                    request_json BLOB NOT NULL,
                    prepare_receipt_sha256 TEXT NOT NULL UNIQUE,
                    prepare_receipt_json BLOB NOT NULL,
                    source_fence_sha256 TEXT NOT NULL UNIQUE,
                    source_view_sha256 TEXT NOT NULL,
                    child_reference_root_sha256 TEXT NOT NULL,
                    floor_before_sha256 TEXT NOT NULL,
                    floor_before_json BLOB NOT NULL,
                    prior_apply_receipt_sha256 TEXT,
                    expected_floor_after_sha256 TEXT NOT NULL,
                    expected_reclaimed_completion_count INTEGER NOT NULL,
                    expected_reclaimed_segment_count INTEGER NOT NULL,
                    expected_reclaimed_file_bytes INTEGER NOT NULL,
                    target_floor_ordinal INTEGER NOT NULL,
                    target_floor_segment_id TEXT NOT NULL,
                    target_floor_file_sha256 TEXT NOT NULL,
                    target_floor_global_sequence INTEGER NOT NULL,
                    target_segment_count INTEGER NOT NULL,
                    candidate_count INTEGER NOT NULL,
                    releasable_bytes INTEGER NOT NULL,
                    releasable_entries INTEGER NOT NULL,
                    coordinator_authority_sha256 TEXT NOT NULL,
                    committed_decision_sha256 TEXT,
                    committed_receipt_sha256 TEXT,
                    effect_json BLOB NOT NULL,
                    effect_sha256 TEXT NOT NULL,
                    post_apply_view_sha256 TEXT,
                    apply_receipt_sha256 TEXT,
                    apply_receipt_json BLOB,
                    abort_decision_sha256 TEXT,
                    fence_release_root_sha256 TEXT,
                    abort_receipt_json BLOB,
                    prepared_at REAL NOT NULL,
                    applied_at REAL,
                    aborted_at REAL,
                    fence_epoch INTEGER NOT NULL UNIQUE,
                    journal_sealed INTEGER NOT NULL
                        CHECK(journal_sealed IN (0,1)),
                    CHECK((state IN ('PREPARED','APPLYING') AND active_slot=1)
                        OR (state IN ('APPLIED','ABORTED') AND active_slot IS NULL))
                )
            """,
            "lifecycle_reclamation_members": """
                CREATE TABLE lifecycle_reclamation_members(
                    request_sha256 TEXT NOT NULL REFERENCES
                        lifecycle_reclamation_decisions(request_sha256),
                    candidate_sha256 TEXT NOT NULL,
                    acquisition_job_id TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    event_sha256 TEXT NOT NULL,
                    producer_receipt_sha256 TEXT NOT NULL,
                    consumer_id TEXT NOT NULL,
                    stream_id TEXT NOT NULL,
                    stream_sequence INTEGER NOT NULL,
                    archive_global_sequence INTEGER NOT NULL,
                    archive_content_sha256 TEXT NOT NULL,
                    archive_index_sha256 TEXT NOT NULL,
                    terminal_identity_sha256 TEXT NOT NULL,
                    lifecycle_archive_segment_id TEXT NOT NULL,
                    retention_anchor_sha256 TEXT NOT NULL,
                    retention_segment_sha256 TEXT NOT NULL,
                    retention_record_sha256 TEXT NOT NULL,
                    archive_metadata_sha256 TEXT NOT NULL,
                    archive_object_reference_root_sha256 TEXT NOT NULL,
                    archive_object_count INTEGER NOT NULL
                        CHECK(archive_object_count>=0),
                    archive_object_total_bytes INTEGER NOT NULL
                        CHECK(archive_object_total_bytes>=0),
                    member_sha256 TEXT NOT NULL UNIQUE,
                    PRIMARY KEY(request_sha256,candidate_sha256),
                    UNIQUE(request_sha256,event_id),
                    UNIQUE(request_sha256,stream_id,stream_sequence)
                )
            """,
            "lifecycle_reclamation_files": """
                CREATE TABLE lifecycle_reclamation_files(
                    request_sha256 TEXT NOT NULL REFERENCES
                        lifecycle_reclamation_decisions(request_sha256),
                    file_kind TEXT NOT NULL CHECK(file_kind IN
                        ('segment','object')),
                    identity_sha256 TEXT NOT NULL,
                    content_sha256 TEXT NOT NULL,
                    size_bytes INTEGER NOT NULL CHECK(size_bytes>=0),
                    filename TEXT NOT NULL,
                    file_sha256 TEXT NOT NULL UNIQUE,
                    PRIMARY KEY(request_sha256,file_kind,identity_sha256)
                )
            """,
            "lifecycle_reclamation_object_references": """
                CREATE TABLE lifecycle_reclamation_object_references(
                    request_sha256 TEXT NOT NULL REFERENCES
                        lifecycle_reclamation_decisions(request_sha256),
                    content_sha256 TEXT NOT NULL,
                    size_bytes INTEGER NOT NULL CHECK(size_bytes>=0),
                    filename TEXT NOT NULL,
                    target_reference_count INTEGER NOT NULL
                        CHECK(target_reference_count>=1),
                    retained_reference_count INTEGER NOT NULL
                        CHECK(retained_reference_count>=0),
                    delete_file INTEGER NOT NULL CHECK(delete_file IN (0,1)),
                    catalog_row_sha256 TEXT NOT NULL,
                    effect_binding_sha256 TEXT NOT NULL,
                    reference_sha256 TEXT NOT NULL,
                    PRIMARY KEY(request_sha256,content_sha256),
                    UNIQUE(request_sha256,reference_sha256)
                )
            """,
        }

    @classmethod
    def _validate_v4_reclamation_schema(cls, db: sqlite3.Connection) -> None:
        expected = {
            "lifecycle_archive_floor": {
                "singleton", "authority_schema",
                "pruned_through_segment_ordinal",
                "pruned_through_segment_id",
                "pruned_through_file_sha256",
                "pruned_through_global_sequence",
                "reclaimed_completion_count", "reclaimed_segment_count",
                "reclaimed_file_bytes", "last_apply_receipt_sha256",
            },
            "lifecycle_reclamation_meta": {
                "singleton", "authority_schema", "next_fence_epoch",
                "last_observed_at",
            },
            "lifecycle_reclamation_decisions": {
                "request_sha256", "state", "active_slot", "request_json",
                "prepare_receipt_sha256", "prepare_receipt_json",
                "source_fence_sha256", "source_view_sha256",
                "child_reference_root_sha256",
                "floor_before_sha256", "floor_before_json",
                "prior_apply_receipt_sha256",
                "expected_floor_after_sha256",
                "expected_reclaimed_completion_count",
                "expected_reclaimed_segment_count",
                "expected_reclaimed_file_bytes", "target_floor_ordinal",
                "target_floor_segment_id", "target_floor_file_sha256",
                "target_floor_global_sequence", "target_segment_count",
                "candidate_count",
                "releasable_bytes", "releasable_entries",
                "coordinator_authority_sha256", "committed_decision_sha256",
                "committed_receipt_sha256", "effect_json", "effect_sha256",
                "post_apply_view_sha256", "apply_receipt_sha256",
                "apply_receipt_json",
                "abort_decision_sha256", "fence_release_root_sha256",
                "abort_receipt_json", "prepared_at", "applied_at",
                "aborted_at", "fence_epoch", "journal_sealed",
            },
            "lifecycle_reclamation_members": {
                "request_sha256", "candidate_sha256", "acquisition_job_id",
                "event_id", "event_sha256", "producer_receipt_sha256",
                "consumer_id", "stream_id", "stream_sequence",
                "archive_global_sequence", "archive_content_sha256",
                "archive_index_sha256",
                "terminal_identity_sha256", "lifecycle_archive_segment_id",
                "retention_anchor_sha256", "retention_segment_sha256",
                "retention_record_sha256", "archive_metadata_sha256",
                "archive_object_reference_root_sha256",
                "archive_object_count", "archive_object_total_bytes",
                "member_sha256",
            },
            "lifecycle_reclamation_files": {
                "request_sha256", "file_kind", "identity_sha256",
                "content_sha256", "size_bytes", "filename", "file_sha256",
            },
            "lifecycle_reclamation_object_references": {
                "request_sha256", "content_sha256", "size_bytes",
                "filename", "target_reference_count",
                "retained_reference_count", "delete_file",
                "catalog_row_sha256", "effect_binding_sha256",
                "reference_sha256",
            },
        }
        definitions = cls._v4_reclamation_table_definitions()
        tables = cls._lifecycle_tables(db)
        expected_tables = set(_V4_BASE_TABLE_SQL_SHA256) | set(definitions)
        if tables != expected_tables:
            raise AcquisitionEvidenceIntegrityError(
                "lifecycle v4 table inventory differs"
            )
        table_sql = {
            str(row["name"]): str(row["sql"])
            for row in db.execute(
                "SELECT name,sql FROM sqlite_master WHERE type='table' "
                "AND name NOT LIKE 'sqlite_%'"
            )
        }
        for table, expected_sha256 in _V4_BASE_TABLE_SQL_SHA256.items():
            normalized = cls._normalized_sql(table_sql.get(table, ""))
            if hashlib.sha256(normalized.encode("utf-8")).hexdigest() != (
                expected_sha256
            ):
                raise AcquisitionEvidenceIntegrityError(
                    f"lifecycle v4 {table} SQL differs"
                )
        for table, statement in definitions.items():
            if cls._normalized_sql(table_sql.get(table, "")) != (
                cls._normalized_sql(statement)
            ):
                raise AcquisitionEvidenceIntegrityError(
                    f"lifecycle v4 {table} SQL differs"
                )
        explicit_indexes = db.execute(
            "SELECT name,sql FROM sqlite_master WHERE type='index' "
            "AND sql IS NOT NULL"
        ).fetchall()
        if explicit_indexes:
            raise AcquisitionEvidenceIntegrityError(
                "lifecycle v4 explicit index inventory differs"
            )
        for table, columns in expected.items():
            actual = {
                str(row[1])
                for row in db.execute(f"PRAGMA table_info({table})")
            }
            if actual != columns:
                raise AcquisitionEvidenceIntegrityError(
                    f"lifecycle v4 {table} columns differ"
                )
        if db.execute("PRAGMA foreign_key_check").fetchone() is not None:
            raise AcquisitionEvidenceIntegrityError(
                "lifecycle v4 foreign-key authority differs"
            )
        floor = db.execute(
            "SELECT * FROM lifecycle_archive_floor WHERE singleton=1"
        ).fetchall()
        meta = db.execute(
            "SELECT * FROM lifecycle_reclamation_meta WHERE singleton=1"
        ).fetchall()
        if (
            len(floor) != 1
            or str(floor[0]["authority_schema"]) != RECLAMATION_FLOOR_SCHEMA
            or len(meta) != 1
            or str(meta[0]["authority_schema"]) != RECLAMATION_META_SCHEMA
        ):
            raise AcquisitionEvidenceIntegrityError(
                "lifecycle v4 reclamation singleton authority differs"
            )

    def initialize(self) -> None:
        if self.root.exists() and self.root.is_symlink():
            raise AcquisitionEvidenceIntegrityError("lifecycle root is a symlink")
        self._prepare_directories()
        self._recover_staging()
        if self.path.exists() and self.path.is_symlink():
            raise AcquisitionEvidenceIntegrityError("lifecycle database is a symlink")
        with self._lock, closing(self._connect()) as db:
            try:
                db.execute("BEGIN IMMEDIATE")
                existing_tables = self._lifecycle_tables(db)
                prior_schema_version: int | None = None
                if existing_tables:
                    if "lifecycle_schema" not in existing_tables:
                        raise AcquisitionEvidenceIntegrityError(
                            "lifecycle schema authority is unavailable"
                        )
                    schema_rows = db.execute(
                        "SELECT schema_version FROM lifecycle_schema "
                        "WHERE singleton=1"
                    ).fetchall()
                    if len(schema_rows) != 1:
                        raise AcquisitionEvidenceIntegrityError(
                            "lifecycle schema authority differs"
                        )
                    prior_schema_version = int(schema_rows[0][0])
                    if prior_schema_version not in {1, 2, 3, 4}:
                        raise AcquisitionEvidenceIntegrityError(
                            "lifecycle schema mismatch"
                        )
                    if prior_schema_version == 4:
                        self._validate_v4_reclamation_schema(db)
                        self._initialize_store_instance_authority(
                            db, allow_create=False
                        )
                        self._validate_clock_triggers(db)
                        self._initialize_reclamation_triggers(
                            db, allow_create=False
                        )
                    else:
                        legacy_tables = set(_V4_BASE_TABLE_SQL_SHA256)
                        if prior_schema_version == 2:
                            legacy_tables.remove("quarantine_receipts")
                        elif prior_schema_version == 1:
                            legacy_tables.difference_update({
                                "quarantine_receipts",
                                "rejection_receipts",
                            })
                        if existing_tables != legacy_tables:
                            raise AcquisitionEvidenceIntegrityError(
                                "lifecycle migration table inventory differs"
                            )
                        trigger_rows = db.execute(
                            "SELECT name,sql FROM sqlite_master "
                            "WHERE type='trigger'"
                        ).fetchall()
                        actual_legacy_triggers = {
                            str(row["name"]): str(row["sql"])
                            for row in trigger_rows
                        }
                        current_clock_triggers = (
                            self._clock_trigger_definitions()
                        )
                        legacy_clock_trigger_names = {
                            "lifecycle_clock_no_delete",
                            "lifecycle_clock_no_regression",
                        } | {
                            f"lifecycle_clock_{table}_{operation}"
                            for table, _columns in self._clock_sources()
                            if table in legacy_tables
                            for operation in ("insert", "update")
                        }
                        expected_legacy_triggers = {
                            name: statement
                            for name, statement in current_clock_triggers.items()
                            if name in legacy_clock_trigger_names
                        }
                        if (
                            set(actual_legacy_triggers)
                            != set(expected_legacy_triggers)
                            or any(
                                self._normalized_sql(
                                    actual_legacy_triggers[name]
                                ) != self._normalized_sql(statement)
                                for name, statement in (
                                    expected_legacy_triggers.items()
                                )
                            )
                        ):
                            raise AcquisitionEvidenceIntegrityError(
                                "lifecycle migration trigger inventory differs"
                            )
                self._execute_transactional_ddl(
                    db,
                    """
                    CREATE TABLE IF NOT EXISTS lifecycle_schema(
                        singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                        schema_version INTEGER NOT NULL,
                        store_instance_sha256 TEXT
                    );
                    CREATE TABLE IF NOT EXISTS lifecycle_heads(
                        singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                        next_global_sequence INTEGER NOT NULL,
                        archive_namespace TEXT,
                        retention_policy_sha256 TEXT,
                        last_segment_id TEXT,
                        last_segment_file_sha256 TEXT,
                        segment_count INTEGER NOT NULL,
                        archived_completion_count INTEGER NOT NULL,
                        archive_bytes INTEGER NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS stream_heads(
                        stream_id TEXT PRIMARY KEY,
                        consumer_id TEXT NOT NULL,
                        last_sequence INTEGER NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS hot_objects(
                        content_sha256 TEXT PRIMARY KEY,
                        size_bytes INTEGER NOT NULL,
                        filename TEXT NOT NULL UNIQUE,
                        state TEXT NOT NULL CHECK(state IN ('active','pruning')),
                        created_at REAL NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS producer_receipts(
                        producer_receipt_sha256 TEXT PRIMARY KEY,
                        acquisition_job_id TEXT NOT NULL UNIQUE,
                        consumer_id TEXT NOT NULL,
                        stream_id TEXT NOT NULL,
                        stream_sequence INTEGER NOT NULL,
                        global_sequence INTEGER NOT NULL UNIQUE,
                        desired_state_sha256 TEXT NOT NULL,
                        authority_sha256 TEXT NOT NULL,
                        proof_sha256 TEXT NOT NULL,
                        proof_json BLOB NOT NULL,
                        content_sha256 TEXT NOT NULL REFERENCES hot_objects(content_sha256),
                        size_bytes INTEGER NOT NULL,
                        created_at REAL NOT NULL,
                        UNIQUE(stream_id,stream_sequence)
                    );
                    CREATE TABLE IF NOT EXISTS completion_outbox(
                        event_id TEXT PRIMARY KEY,
                        event_sha256 TEXT NOT NULL UNIQUE,
                        producer_receipt_sha256 TEXT NOT NULL UNIQUE REFERENCES
                            producer_receipts(producer_receipt_sha256),
                        consumer_id TEXT NOT NULL,
                        stream_id TEXT NOT NULL,
                        stream_sequence INTEGER NOT NULL,
                        state TEXT NOT NULL CHECK(state IN
                            ('ready','leased','consumed','superseded')),
                        attempt_count INTEGER NOT NULL,
                        lease_owner TEXT,
                        lease_token_sha256 TEXT,
                        lease_expires_at REAL,
                        superseded_by_event_id TEXT,
                        supersession_sha256 TEXT,
                        ready_at REAL NOT NULL,
                        terminal_at REAL,
                        UNIQUE(consumer_id,stream_id,stream_sequence)
                    );
                    CREATE TABLE IF NOT EXISTS consumer_cursors(
                        consumer_id TEXT NOT NULL,
                        stream_id TEXT NOT NULL,
                        last_sequence INTEGER NOT NULL,
                        last_event_sha256 TEXT,
                        updated_at REAL NOT NULL,
                        PRIMARY KEY(consumer_id,stream_id)
                    );
                    CREATE TABLE IF NOT EXISTS consumption_receipts(
                        receipt_sha256 TEXT PRIMARY KEY,
                        event_id TEXT NOT NULL UNIQUE REFERENCES completion_outbox(event_id),
                        event_sha256 TEXT NOT NULL,
                        producer_receipt_sha256 TEXT NOT NULL,
                        consumer_id TEXT NOT NULL,
                        stream_id TEXT NOT NULL,
                        stream_sequence INTEGER NOT NULL,
                        output_sha256 TEXT NOT NULL,
                        created_at REAL NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS rejection_receipts(
                        rejection_receipt_sha256 TEXT PRIMARY KEY,
                        event_id TEXT NOT NULL UNIQUE REFERENCES completion_outbox(event_id),
                        event_sha256 TEXT NOT NULL,
                        producer_receipt_sha256 TEXT NOT NULL,
                        consumer_id TEXT NOT NULL,
                        stream_id TEXT NOT NULL,
                        stream_sequence INTEGER NOT NULL,
                        rejected_desired_state_sha256 TEXT NOT NULL,
                        active_desired_state_sha256 TEXT,
                        reason_code TEXT NOT NULL,
                        created_at REAL NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS quarantine_receipts(
                        quarantine_receipt_sha256 TEXT PRIMARY KEY,
                        event_id TEXT NOT NULL UNIQUE REFERENCES
                            completion_outbox(event_id),
                        event_sha256 TEXT NOT NULL,
                        producer_receipt_sha256 TEXT NOT NULL,
                        consumer_id TEXT NOT NULL,
                        stream_id TEXT NOT NULL,
                        stream_sequence INTEGER NOT NULL,
                        evidence_fingerprint_sha256 TEXT NOT NULL,
                        reason_code TEXT NOT NULL,
                        created_at REAL NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS archive_objects(
                        content_sha256 TEXT PRIMARY KEY,
                        size_bytes INTEGER NOT NULL,
                        filename TEXT NOT NULL UNIQUE,
                        created_at REAL NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS archive_segments(
                        segment_id TEXT PRIMARY KEY,
                        segment_ordinal INTEGER NOT NULL UNIQUE,
                        previous_segment_id TEXT,
                        previous_file_sha256 TEXT,
                        first_global_sequence INTEGER NOT NULL,
                        last_global_sequence INTEGER NOT NULL,
                        completion_count INTEGER NOT NULL,
                        content_sha256 TEXT NOT NULL,
                        file_sha256 TEXT NOT NULL,
                        filename TEXT NOT NULL UNIQUE,
                        file_bytes INTEGER NOT NULL,
                        manifest_json BLOB NOT NULL,
                        created_at REAL NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS archive_index(
                        producer_receipt_sha256 TEXT PRIMARY KEY,
                        acquisition_job_id TEXT NOT NULL UNIQUE,
                        event_id TEXT NOT NULL UNIQUE,
                        event_sha256 TEXT NOT NULL,
                        consumer_id TEXT NOT NULL,
                        stream_id TEXT NOT NULL,
                        stream_sequence INTEGER NOT NULL,
                        global_sequence INTEGER NOT NULL UNIQUE,
                        content_sha256 TEXT NOT NULL REFERENCES archive_objects(content_sha256),
                        segment_id TEXT NOT NULL REFERENCES archive_segments(segment_id),
                        UNIQUE(stream_id,stream_sequence)
                    );
                    CREATE TABLE IF NOT EXISTS prune_plans(
                        segment_id TEXT PRIMARY KEY REFERENCES archive_segments(segment_id),
                        phase INTEGER NOT NULL,
                        event_ids_json BLOB NOT NULL,
                        producer_ids_json BLOB NOT NULL,
                        object_ids_json BLOB NOT NULL,
                        completed_at REAL
                    );
                    CREATE TABLE IF NOT EXISTS lifecycle_archive_floor(
                        singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                        authority_schema TEXT NOT NULL,
                        pruned_through_segment_ordinal INTEGER NOT NULL
                            CHECK(pruned_through_segment_ordinal>=0),
                        pruned_through_segment_id TEXT,
                        pruned_through_file_sha256 TEXT,
                        pruned_through_global_sequence INTEGER NOT NULL
                            CHECK(pruned_through_global_sequence>=0),
                        reclaimed_completion_count INTEGER NOT NULL
                            CHECK(reclaimed_completion_count>=0),
                        reclaimed_segment_count INTEGER NOT NULL
                            CHECK(reclaimed_segment_count>=0),
                        reclaimed_file_bytes INTEGER NOT NULL
                            CHECK(reclaimed_file_bytes>=0),
                        last_apply_receipt_sha256 TEXT
                    );
                    CREATE TABLE IF NOT EXISTS lifecycle_reclamation_meta(
                        singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                        authority_schema TEXT NOT NULL,
                        next_fence_epoch INTEGER NOT NULL
                            CHECK(next_fence_epoch>=1),
                        last_observed_at REAL
                    );
                    CREATE TABLE IF NOT EXISTS lifecycle_reclamation_decisions(
                        request_sha256 TEXT PRIMARY KEY,
                        state TEXT NOT NULL CHECK(state IN
                            ('PREPARED','APPLYING','APPLIED','ABORTED')),
                        active_slot INTEGER UNIQUE CHECK(active_slot=1),
                        request_json BLOB NOT NULL,
                        prepare_receipt_sha256 TEXT NOT NULL UNIQUE,
                        prepare_receipt_json BLOB NOT NULL,
                        source_fence_sha256 TEXT NOT NULL UNIQUE,
                        source_view_sha256 TEXT NOT NULL,
                        child_reference_root_sha256 TEXT NOT NULL,
                        floor_before_sha256 TEXT NOT NULL,
                        floor_before_json BLOB NOT NULL,
                        prior_apply_receipt_sha256 TEXT,
                        expected_floor_after_sha256 TEXT NOT NULL,
                        expected_reclaimed_completion_count INTEGER NOT NULL,
                        expected_reclaimed_segment_count INTEGER NOT NULL,
                        expected_reclaimed_file_bytes INTEGER NOT NULL,
                        target_floor_ordinal INTEGER NOT NULL,
                        target_floor_segment_id TEXT NOT NULL,
                        target_floor_file_sha256 TEXT NOT NULL,
                        target_floor_global_sequence INTEGER NOT NULL,
                        target_segment_count INTEGER NOT NULL,
                        candidate_count INTEGER NOT NULL,
                        releasable_bytes INTEGER NOT NULL,
                        releasable_entries INTEGER NOT NULL,
                        coordinator_authority_sha256 TEXT NOT NULL,
                        committed_decision_sha256 TEXT,
                        committed_receipt_sha256 TEXT,
                        effect_json BLOB NOT NULL,
                        effect_sha256 TEXT NOT NULL,
                        post_apply_view_sha256 TEXT,
                        apply_receipt_sha256 TEXT,
                        apply_receipt_json BLOB,
                        abort_decision_sha256 TEXT,
                        fence_release_root_sha256 TEXT,
                        abort_receipt_json BLOB,
                        prepared_at REAL NOT NULL,
                        applied_at REAL,
                        aborted_at REAL,
                        fence_epoch INTEGER NOT NULL UNIQUE,
                        journal_sealed INTEGER NOT NULL
                            CHECK(journal_sealed IN (0,1)),
                        CHECK((state IN ('PREPARED','APPLYING') AND active_slot=1)
                            OR (state IN ('APPLIED','ABORTED') AND active_slot IS NULL))
                    );
                    CREATE TABLE IF NOT EXISTS lifecycle_reclamation_members(
                        request_sha256 TEXT NOT NULL REFERENCES
                            lifecycle_reclamation_decisions(request_sha256),
                        candidate_sha256 TEXT NOT NULL,
                        acquisition_job_id TEXT NOT NULL,
                        event_id TEXT NOT NULL,
                        event_sha256 TEXT NOT NULL,
                        producer_receipt_sha256 TEXT NOT NULL,
                        consumer_id TEXT NOT NULL,
                        stream_id TEXT NOT NULL,
                        stream_sequence INTEGER NOT NULL,
                        archive_global_sequence INTEGER NOT NULL,
                        archive_content_sha256 TEXT NOT NULL,
                        archive_index_sha256 TEXT NOT NULL,
                        terminal_identity_sha256 TEXT NOT NULL,
                        lifecycle_archive_segment_id TEXT NOT NULL,
                        retention_anchor_sha256 TEXT NOT NULL,
                        retention_segment_sha256 TEXT NOT NULL,
                        retention_record_sha256 TEXT NOT NULL,
                        archive_metadata_sha256 TEXT NOT NULL,
                        archive_object_reference_root_sha256 TEXT NOT NULL,
                        archive_object_count INTEGER NOT NULL
                            CHECK(archive_object_count>=0),
                        archive_object_total_bytes INTEGER NOT NULL
                            CHECK(archive_object_total_bytes>=0),
                        member_sha256 TEXT NOT NULL UNIQUE,
                        PRIMARY KEY(request_sha256,candidate_sha256),
                        UNIQUE(request_sha256,event_id),
                        UNIQUE(request_sha256,stream_id,stream_sequence)
                    );
                    CREATE TABLE IF NOT EXISTS lifecycle_reclamation_files(
                        request_sha256 TEXT NOT NULL REFERENCES
                            lifecycle_reclamation_decisions(request_sha256),
                        file_kind TEXT NOT NULL CHECK(file_kind IN
                            ('segment','object')),
                        identity_sha256 TEXT NOT NULL,
                        content_sha256 TEXT NOT NULL,
                        size_bytes INTEGER NOT NULL CHECK(size_bytes>=0),
                        filename TEXT NOT NULL,
                        file_sha256 TEXT NOT NULL UNIQUE,
                        PRIMARY KEY(request_sha256,file_kind,identity_sha256)
                    );
                    CREATE TABLE IF NOT EXISTS
                        lifecycle_reclamation_object_references(
                        request_sha256 TEXT NOT NULL REFERENCES
                            lifecycle_reclamation_decisions(request_sha256),
                        content_sha256 TEXT NOT NULL,
                        size_bytes INTEGER NOT NULL CHECK(size_bytes>=0),
                        filename TEXT NOT NULL,
                        target_reference_count INTEGER NOT NULL
                            CHECK(target_reference_count>=1),
                        retained_reference_count INTEGER NOT NULL
                            CHECK(retained_reference_count>=0),
                        delete_file INTEGER NOT NULL
                            CHECK(delete_file IN (0,1)),
                        catalog_row_sha256 TEXT NOT NULL,
                        effect_binding_sha256 TEXT NOT NULL,
                        reference_sha256 TEXT NOT NULL,
                        PRIMARY KEY(request_sha256,content_sha256),
                        UNIQUE(request_sha256,reference_sha256)
                    );
                    CREATE TRIGGER IF NOT EXISTS archive_segments_no_update
                    BEFORE UPDATE ON archive_segments BEGIN
                        SELECT RAISE(ABORT,'archive segment is immutable');
                    END;
                    CREATE TRIGGER IF NOT EXISTS archive_index_no_update
                    BEFORE UPDATE ON archive_index BEGIN
                        SELECT RAISE(ABORT,'archive index is immutable');
                    END;
                    """
                )
                schema_columns = {
                    str(row[1]) for row in db.execute(
                        "PRAGMA table_info(lifecycle_schema)"
                    )
                }
                if "store_instance_sha256" not in schema_columns:
                    db.execute(
                        "ALTER TABLE lifecycle_schema ADD COLUMN "
                        "store_instance_sha256 TEXT"
                    )
                    schema_columns.add("store_instance_sha256")
                if schema_columns != {
                    "singleton", "schema_version", "store_instance_sha256"
                }:
                    raise AcquisitionEvidenceIntegrityError(
                        "lifecycle schema authority columns differ"
                    )
                db.execute(
                    "INSERT OR IGNORE INTO lifecycle_schema("
                    "singleton,schema_version,store_instance_sha256) "
                    "VALUES(1,?,?)",
                    (SCHEMA_VERSION, secrets.token_hex(32)),
                )
                db.execute(
                    "UPDATE lifecycle_schema SET store_instance_sha256=? "
                    "WHERE singleton=1 AND store_instance_sha256 IS NULL",
                    (secrets.token_hex(32),),
                )
                db.execute(
                    "INSERT OR IGNORE INTO lifecycle_heads VALUES"
                    "(1,1,NULL,NULL,NULL,NULL,0,0,0)"
                )
                db.execute(
                    "INSERT OR IGNORE INTO lifecycle_archive_floor VALUES"
                    "(1,?,0,NULL,NULL,0,0,0,0,NULL)",
                    (RECLAMATION_FLOOR_SCHEMA,),
                )
                db.execute(
                    "INSERT OR IGNORE INTO lifecycle_reclamation_meta VALUES"
                    "(1,?,1,NULL)",
                    (RECLAMATION_META_SCHEMA,),
                )
                schema = db.execute(
                    "SELECT schema_version FROM lifecycle_schema WHERE singleton=1"
                ).fetchone()
                if schema is not None and int(schema[0]) == 1:
                    # Version 2 adds only the immutable rejection-receipt child
                    # table.  CREATE TABLE above is idempotent, so upgrading a
                    # v1 shadow store neither rewrites nor reinterprets evidence.
                    db.execute(
                        "UPDATE lifecycle_schema SET schema_version=2 "
                        "WHERE singleton=1 AND schema_version=1"
                    )
                    schema = db.execute(
                        "SELECT schema_version FROM lifecycle_schema WHERE singleton=1"
                    ).fetchone()
                if schema is not None and int(schema[0]) == 2:
                    # Version 3 appends the immutable quarantine-receipt child
                    # table.  Existing events and v1/v2 archive records keep
                    # their original interpretation and byte identities.
                    db.execute(
                        "UPDATE lifecycle_schema SET schema_version=3 "
                        "WHERE singleton=1 AND schema_version=2"
                    )
                    schema = db.execute(
                        "SELECT schema_version FROM lifecycle_schema WHERE singleton=1"
                    ).fetchone()
                if schema is not None and int(schema[0]) == 3:
                    db.execute(
                        "UPDATE lifecycle_schema SET schema_version=4 "
                        "WHERE singleton=1 AND schema_version=3"
                    )
                    schema = db.execute(
                        "SELECT schema_version FROM lifecycle_schema WHERE singleton=1"
                    ).fetchone()
                if schema is None or int(schema[0]) != SCHEMA_VERSION:
                    raise AcquisitionEvidenceIntegrityError("lifecycle schema mismatch")
                allow_trigger_create = prior_schema_version != 4
                if allow_trigger_create:
                    self._initialize_clock_authority(db)
                self._validate_v4_reclamation_schema(db)
                self._initialize_store_instance_authority(
                    db, allow_create=allow_trigger_create
                )
                if not allow_trigger_create:
                    self._validate_clock_triggers(db)
                self._initialize_reclamation_triggers(
                    db, allow_create=allow_trigger_create
                )
                db.commit()
                self._store_instance_sha256 = self._load_store_instance(db)
            except BaseException:
                if db.in_transaction:
                    db.rollback()
                raise
        self._directory_fsync(self.root)

    @property
    def store_instance_sha256(self) -> str:
        with self._lock, closing(self._connect()) as db:
            instance = self._load_store_instance(db)
        if not hmac.compare_digest(instance, self._store_instance_sha256):
            raise AcquisitionEvidenceIntegrityError(
                "lifecycle store-instance authority changed"
            )
        return instance

    @property
    def identity_sha256(self) -> str:
        path_text = os.path.normcase(os.path.normpath(str(self.path)))
        return _digest({
            "contract": CONTRACT,
            "limits_sha256": _digest(asdict(self._limits)),
            "path_sha256": hashlib.sha256(
                path_text.encode("utf-8")
            ).hexdigest(),
            "schema": STORE_IDENTITY_SCHEMA,
            "schema_version": SCHEMA_VERSION,
            "store_instance_sha256": self.store_instance_sha256,
        })

    def _recover_staging(self) -> None:
        """Promote only checksum-complete crash staging; corrupt stages block."""

        for directory, maximum, size_limit in (
            (
                self.hot_directory,
                self._limits.max_hot_objects + 1,
                self._limits.max_object_bytes,
            ),
            (
                self.archive_object_directory,
                self._limits.max_archive_objects + 1,
                self._limits.max_object_bytes,
            ),
        ):
            for item in self._bounded_files(directory, maximum):
                match = _OBJECT_STAGE.fullmatch(item.name)
                if match is None:
                    continue
                if item.is_symlink() or not item.is_file():
                    raise AcquisitionEvidenceIntegrityError(
                        "unsafe object staging entry"
                    )
                expected = match.group(1)
                if item.stat().st_size > size_limit:
                    raise AcquisitionEvidenceCapacityError(
                        "staged object exceeds object bound"
                    )
                target = directory / f"{expected}.raw"
                if target.exists():
                    target_bytes = self._verified_file(
                        target, expected, target.stat().st_size
                    )
                    staged = item.read_bytes()
                    if staged and staged != target_bytes:
                        # A partial pre-fsync stage may differ; the already
                        # published exact target remains authoritative.
                        if hashlib.sha256(staged).hexdigest() == expected:
                            raise AcquisitionEvidenceIntegrityError(
                                "complete staging conflicts with exact target"
                            )
                    item.unlink()
                    self._directory_fsync(directory)
                    continue
                body = item.read_bytes()
                if not body or hashlib.sha256(body).hexdigest() != expected:
                    raise AcquisitionEvidenceIntegrityError(
                        "incomplete object staging requires explicit recovery"
                    )
                os.replace(item, target)
                self._directory_fsync(directory)
                self._verified_file(target, expected, len(body))

        for item in self._bounded_files(
            self.archive_segment_directory,
            self._limits.max_archive_segments + 1,
        ):
            match = _SEGMENT_STAGE.fullmatch(item.name)
            if match is None:
                continue
            if item.is_symlink() or not item.is_file():
                raise AcquisitionEvidenceIntegrityError(
                    "unsafe archive segment staging entry"
                )
            if item.stat().st_size > self._limits.max_segment_bytes:
                raise AcquisitionEvidenceCapacityError(
                    "staged archive segment exceeds bound"
                )
            expected = match.group(1)
            body = item.read_bytes()
            try:
                decoded = json.loads(body)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise AcquisitionEvidenceIntegrityError(
                    "incomplete archive segment staging requires recovery"
                ) from exc
            if not isinstance(decoded, dict):
                raise AcquisitionEvidenceIntegrityError(
                    "staged archive segment is not an object"
                )
            core = dict(decoded)
            core.pop("records", None)
            segment_id = core.pop("segment_id", None)
            if (
                segment_id != expected
                or hashlib.sha256(_canonical(core)).hexdigest() != expected
            ):
                raise AcquisitionEvidenceIntegrityError(
                    "staged archive segment identity differs"
                )
            target = self.archive_segment_directory / f"{expected}.sealed.json"
            if target.exists():
                if target.read_bytes() != body:
                    raise AcquisitionEvidenceIntegrityError(
                        "staged archive segment conflicts with target"
                    )
                item.unlink()
            else:
                os.replace(item, target)
            self._directory_fsync(self.archive_segment_directory)

    def _bounded_files(self, directory: Path, maximum: int) -> tuple[Path, ...]:
        result: list[Path] = []
        for entry in directory.iterdir():
            result.append(entry)
            if len(result) > maximum:
                raise AcquisitionEvidenceCapacityError("filesystem entry bound exceeded")
        return tuple(result)

    @staticmethod
    def _verified_file(path: Path, sha256: str, size_bytes: int) -> bytes:
        metadata = path.lstat()
        witness = (
            int(metadata.st_dev),
            int(metadata.st_ino),
            int(metadata.st_mode),
            int(metadata.st_nlink),
            int(metadata.st_size),
            int(metadata.st_mtime_ns),
        )
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_nlink != 1
            or metadata.st_size != size_bytes
        ):
            raise AcquisitionEvidenceIntegrityError("evidence object file differs")
        payload = path.read_bytes()
        repeated = path.lstat()
        if witness != (
            int(repeated.st_dev),
            int(repeated.st_ino),
            int(repeated.st_mode),
            int(repeated.st_nlink),
            int(repeated.st_size),
            int(repeated.st_mtime_ns),
        ):
            raise AcquisitionEvidenceIntegrityError("evidence object file changed")
        if len(payload) != size_bytes or not hmac.compare_digest(
            hashlib.sha256(payload).hexdigest(), sha256
        ):
            raise AcquisitionEvidenceIntegrityError("evidence object checksum differs")
        return payload

    @staticmethod
    def _verified_file_after_publish(
        path: Path, sha256: str, size_bytes: int,
    ) -> bytes:
        """Bounded retry for transient Windows cross-publisher file handles."""

        last_error: OSError | None = None
        for attempt in range(32):
            try:
                return AcquisitionEvidenceLifecycleStore._verified_file(
                    path, sha256, size_bytes
                )
            except (PermissionError, FileNotFoundError) as exc:
                last_error = exc
                if attempt == 31:
                    break
                time.sleep(0.002)
        raise AcquisitionEvidenceDurabilityError(
            "published evidence file remained unavailable"
        ) from last_error

    def _hot_usage(self) -> tuple[int, int]:
        files = self._bounded_files(
            self.hot_directory, self._limits.max_hot_objects + 1
        )
        total = 0
        existing_count = 0
        for path in files:
            staging = _OBJECT_STAGE.fullmatch(path.name)
            try:
                info = path.stat()
            except FileNotFoundError:
                if staging is not None:
                    continue
                raise AcquisitionEvidenceIntegrityError(
                    "hot object disappeared during accounting"
                )
            if (
                path.is_symlink()
                or not path.is_file()
                or (
                    staging is None
                    and (
                        _SAFE_FILENAME.fullmatch(path.name) is None
                        or not path.name.endswith(".raw")
                    )
                )
            ):
                raise AcquisitionEvidenceIntegrityError("unsafe hot object entry")
            if staging is not None and info.st_size > self._limits.max_object_bytes:
                raise AcquisitionEvidenceCapacityError(
                    "concurrent hot staging exceeds object bound"
                )
            total += info.st_size
            existing_count += 1
            if total > self._limits.max_hot_bytes:
                raise AcquisitionEvidenceCapacityError("hot byte capacity exceeded")
        return total, existing_count

    def _write_exact_object(
        self, directory: Path, payload: bytes, sha256: str, *, archive: bool
    ) -> tuple[Path, bool]:
        target = directory / f"{sha256}.raw"
        if target.exists():
            self._verified_file_after_publish(target, sha256, len(payload))
            return target, True
        if archive:
            total, object_entries, _ = self._archive_usage()
            if object_entries + 1 > self._limits.max_archive_objects:
                raise AcquisitionEvidenceCapacityError("archive object capacity exceeded")
            if total + len(payload) > self._limits.max_archive_bytes:
                raise AcquisitionEvidenceCapacityError("archive byte capacity exceeded")
        else:
            total, entries_count = self._hot_usage()
            if entries_count + 1 > self._limits.max_hot_objects:
                raise AcquisitionEvidenceCapacityError("hot object capacity exceeded")
            if total + len(payload) > self._limits.max_hot_bytes:
                raise AcquisitionEvidenceCapacityError("hot byte capacity exceeded")
        temporary = directory / f".{sha256}.{secrets.token_hex(8)}"
        try:
            with open(temporary, "xb") as output:
                output.write(payload)
                output.flush()
                self._fault("object.after_write")
                os.fsync(output.fileno())
                self._fault("object.after_fsync")
            self._directory_fsync(directory)
            replaced = False
            for attempt in range(32):
                try:
                    os.replace(temporary, target)
                    replaced = True
                    break
                except PermissionError:
                    # Windows may deny replace while a concurrent publisher is
                    # completing its checksum read. Exact target bytes already
                    # satisfy this content-addressed publication.
                    if target.exists():
                        try:
                            self._verified_file_after_publish(
                                target, sha256, len(payload)
                            )
                        except (OSError, AcquisitionEvidenceIntegrityError):
                            pass
                        else:
                            temporary.unlink(missing_ok=True)
                            self._directory_fsync(directory)
                            return target, True
                    if attempt == 31:
                        raise
                    time.sleep(0.002)
            if not replaced:  # pragma: no cover - loop contract
                raise AcquisitionEvidenceDurabilityError(
                    "object atomic publication did not complete"
                )
            self._fault("object.after_replace")
            self._directory_fsync(directory)
            self._verified_file_after_publish(target, sha256, len(payload))
        except OSError as exc:
            if temporary.exists():
                temporary.unlink(missing_ok=True)
            if exc.errno == errno.ENOSPC:
                raise AcquisitionEvidenceCapacityError("object filesystem is full") from exc
            raise AcquisitionEvidenceDurabilityError("object publication failed") from exc
        return target, False

    @staticmethod
    def _producer_core(
        *, acquisition_job_id: str, consumer_id: str, stream_id: str,
        desired_state_sha256: str, authority_sha256: str,
        proof_sha256: str, content_sha256: str, size_bytes: int,
    ) -> dict[str, Any]:
        return {
            "acquisition_job_id": acquisition_job_id,
            "authority_sha256": authority_sha256,
            "consumer_id": consumer_id,
            "content_sha256": content_sha256,
            "desired_state_sha256": desired_state_sha256,
            "proof_sha256": proof_sha256,
            "schema": PRODUCER_RECEIPT_SCHEMA,
            "size_bytes": size_bytes,
            "stream_id": stream_id,
        }

    def publish(
        self,
        *,
        acquisition_job_id: str,
        consumer_id: str,
        stream_id: str,
        desired_state_sha256: str,
        authority_sha256: str,
        proof: Mapping[str, Any],
        payload: bytes,
    ) -> CompletionProducerReceipt:
        """Commit producer evidence first, then make one completion ready."""

        job = _identifier(acquisition_job_id, "acquisition job ID")
        consumer = _identifier(consumer_id, "consumer ID")
        stream = _identifier(stream_id, "stream ID")
        desired = _sha(desired_state_sha256, "desired state SHA-256")
        authority = _sha(authority_sha256, "authority SHA-256")
        if not isinstance(proof, Mapping):
            raise TypeError("proof must be a mapping")
        proof_copy = json.loads(_canonical(dict(proof)))
        _assert_secret_free(proof_copy)
        proof_bytes = _canonical(proof_copy)
        if len(proof_bytes) > self._limits.max_proof_bytes:
            raise AcquisitionEvidenceCapacityError("completion proof capacity exceeded")
        if not isinstance(payload, bytes) or not payload:
            raise AcquisitionEvidenceLifecycleError("payload must be non-empty bytes")
        if len(payload) > self._limits.max_object_bytes:
            raise AcquisitionEvidenceCapacityError("completion object capacity exceeded")
        proof_sha = hashlib.sha256(proof_bytes).hexdigest()
        content_sha = hashlib.sha256(payload).hexdigest()
        core = self._producer_core(
            acquisition_job_id=job,
            consumer_id=consumer,
            stream_id=stream,
            desired_state_sha256=desired,
            authority_sha256=authority,
            proof_sha256=proof_sha,
            content_sha256=content_sha,
            size_bytes=len(payload),
        )
        stable_sha = _digest(core)
        # A fully archived replay is satisfied from sealed evidence without
        # recreating a hot object or refetching anything.
        with self._lock, closing(self._connect()) as replay_db:
            archived = replay_db.execute(
                "SELECT event_id FROM archive_index WHERE acquisition_job_id=?",
                (job,),
            ).fetchone()
        if archived is not None:
            record, archived_payload, _ = self._archived_record_by_event(
                str(archived["event_id"])
            )
            archived_producer = record["producer_receipt"]
            archived_core = self._producer_core(
                acquisition_job_id=archived_producer["acquisition_job_id"],
                consumer_id=archived_producer["consumer_id"],
                stream_id=archived_producer["stream_id"],
                desired_state_sha256=archived_producer[
                    "desired_state_sha256"
                ],
                authority_sha256=archived_producer["authority_sha256"],
                proof_sha256=archived_producer["proof_sha256"],
                content_sha256=archived_producer["content_sha256"],
                size_bytes=archived_producer["size_bytes"],
            )
            if (
                _digest(archived_core) != stable_sha
                or record["proof"] != proof_copy
                or archived_payload != payload
            ):
                raise AcquisitionEvidenceConflict("archived producer replay conflicts")
            return self._load_archived_producer(str(archived["event_id"]))
        with self._lock, closing(self._connect()) as catalog_db:
            catalog_object = catalog_db.execute(
                "SELECT * FROM hot_objects WHERE content_sha256=?",
                (content_sha,),
            ).fetchone()
        if catalog_object is not None:
            if (
                int(catalog_object["size_bytes"]) != len(payload)
                or str(catalog_object["state"]) != "active"
                or str(catalog_object["filename"]) != f"{content_sha}.raw"
            ):
                raise AcquisitionEvidenceIntegrityError(
                    "known hot object catalog differs before publish"
                )
            known_path = self.hot_directory / f"{content_sha}.raw"
            if not known_path.exists():
                raise AcquisitionEvidenceIntegrityError(
                    "known hot object bytes are missing"
                )
            self._verified_file(known_path, content_sha, len(payload))
        self._write_exact_object(self.hot_directory, payload, content_sha, archive=False)
        self._fault("publish.after_hot_object")

        with self._lock, closing(self._connect()) as db:
            try:
                db.execute("BEGIN IMMEDIATE")
                archived = db.execute(
                    "SELECT * FROM archive_index WHERE acquisition_job_id=?",
                    (job,),
                ).fetchone()
                if archived is not None:
                    db.commit()
                    archived_event = str(archived["event_id"])
                    record, archived_payload, _ = self._archived_record_by_event(
                        archived_event
                    )
                    archived_producer = record["producer_receipt"]
                    archived_core = self._producer_core(
                        acquisition_job_id=archived_producer[
                            "acquisition_job_id"
                        ],
                        consumer_id=archived_producer["consumer_id"],
                        stream_id=archived_producer["stream_id"],
                        desired_state_sha256=archived_producer[
                            "desired_state_sha256"
                        ],
                        authority_sha256=archived_producer[
                            "authority_sha256"
                        ],
                        proof_sha256=archived_producer["proof_sha256"],
                        content_sha256=archived_producer["content_sha256"],
                        size_bytes=archived_producer["size_bytes"],
                    )
                    if (
                        _digest(archived_core) != stable_sha
                        or record["proof"] != proof_copy
                        or archived_payload != payload
                    ):
                        raise AcquisitionEvidenceConflict(
                            "archived producer replay conflicts"
                        )
                    # A cross-process archive race can leave only the exact hot
                    # object file. It has no producer/catalog parent, so remove
                    # that duplicate after verifying the archive is authoritative.
                    with closing(self._connect()) as cleanup_db:
                        referenced = int(cleanup_db.execute(
                            "SELECT count(*) FROM hot_objects "
                            "WHERE content_sha256=?",
                            (content_sha,),
                        ).fetchone()[0])
                    if not referenced:
                        duplicate = self.hot_directory / f"{content_sha}.raw"
                        if duplicate.exists():
                            self._verified_file(
                                duplicate, content_sha, len(payload)
                            )
                            duplicate.unlink()
                            self._directory_fsync(self.hot_directory)
                    return self._load_archived_producer(archived_event)
                existing = db.execute(
                    "SELECT * FROM producer_receipts WHERE acquisition_job_id=?",
                    (job,),
                ).fetchone()
                now = _finite(self._clock(), "producer receipt clock")
                if existing is None:
                    count = int(db.execute(
                        "SELECT count(*) FROM producer_receipts"
                    ).fetchone()[0])
                    if count >= self._limits.max_hot_completions:
                        raise AcquisitionEvidenceCapacityError(
                            "hot completion capacity exceeded"
                        )
                    row = db.execute(
                        "SELECT size_bytes,state FROM hot_objects WHERE content_sha256=?",
                        (content_sha,),
                    ).fetchone()
                    if row is None:
                        db.execute(
                            "INSERT INTO hot_objects VALUES(?,?,?,'active',?)",
                            (content_sha, len(payload), f"{content_sha}.raw", now),
                        )
                    elif int(row["size_bytes"]) != len(payload) or row["state"] != "active":
                        raise AcquisitionEvidenceConflict("hot object catalog conflict")
                    stream_row = db.execute(
                        "SELECT consumer_id,last_sequence FROM stream_heads WHERE stream_id=?",
                        (stream,),
                    ).fetchone()
                    if stream_row is not None and str(stream_row["consumer_id"]) != consumer:
                        raise AcquisitionEvidenceConflict(
                            "one completion stream cannot change consumer authority"
                        )
                    stream_sequence = (
                        1 if stream_row is None else int(stream_row["last_sequence"]) + 1
                    )
                    global_sequence = int(db.execute(
                        "SELECT next_global_sequence FROM lifecycle_heads WHERE singleton=1"
                    ).fetchone()[0])
                    created_at = now
                    receipt_body = {
                        **core,
                        "created_at": created_at,
                        "global_sequence": global_sequence,
                        "stream_sequence": stream_sequence,
                    }
                    producer_sha = _digest(receipt_body)
                    db.execute(
                        "INSERT INTO producer_receipts VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (
                            producer_sha, job, consumer, stream, stream_sequence,
                            global_sequence, desired, authority, proof_sha,
                            sqlite3.Binary(proof_bytes), content_sha, len(payload), created_at,
                        ),
                    )
                    db.execute(
                        "INSERT INTO stream_heads VALUES(?,?,?) "
                        "ON CONFLICT(stream_id) DO UPDATE SET "
                        "last_sequence=excluded.last_sequence",
                        (stream, consumer, stream_sequence),
                    )
                    db.execute(
                        "UPDATE lifecycle_heads SET next_global_sequence=? WHERE singleton=1",
                        (global_sequence + 1,),
                    )
                    self._fault("publish.after_producer_insert")
                    self._fault("publish.before_producer_commit")
                    db.commit()
                    self._fault("publish.after_producer_commit")
                else:
                    stored_core = self._producer_core(
                        acquisition_job_id=str(existing["acquisition_job_id"]),
                        consumer_id=str(existing["consumer_id"]),
                        stream_id=str(existing["stream_id"]),
                        desired_state_sha256=str(existing["desired_state_sha256"]),
                        authority_sha256=str(existing["authority_sha256"]),
                        proof_sha256=str(existing["proof_sha256"]),
                        content_sha256=str(existing["content_sha256"]),
                        size_bytes=int(existing["size_bytes"]),
                    )
                    if _digest(stored_core) != stable_sha or bytes(existing["proof_json"]) != proof_bytes:
                        raise AcquisitionEvidenceConflict("producer replay conflicts")
                    producer_sha = str(existing["producer_receipt_sha256"])
                    stream_sequence = int(existing["stream_sequence"])
                    global_sequence = int(existing["global_sequence"])
                    created_at = float(existing["created_at"])
                    expected_body = {
                        **stored_core,
                        "created_at": created_at,
                        "global_sequence": global_sequence,
                        "stream_sequence": stream_sequence,
                    }
                    if _digest(expected_body) != producer_sha:
                        raise AcquisitionEvidenceIntegrityError(
                            "producer receipt checksum mismatch"
                        )
                    db.commit()

            except BaseException:
                if db.in_transaction:
                    db.rollback()
                raise

        with self._lock, closing(self._connect()) as db:
            try:
                db.execute("BEGIN IMMEDIATE")
                producer = db.execute(
                    "SELECT * FROM producer_receipts WHERE producer_receipt_sha256=?",
                    (producer_sha,),
                ).fetchone()
                if producer is None:
                    raise AcquisitionEvidenceIntegrityError("producer receipt disappeared")
                event_id = "completion-" + hashlib.sha256(
                    (COMPLETION_EVENT_SCHEMA + producer_sha).encode("ascii")
                ).hexdigest()
                event_body = {
                    "consumer_id": consumer,
                    "producer_receipt_sha256": producer_sha,
                    "schema": COMPLETION_EVENT_SCHEMA,
                    "stream_id": stream,
                    "stream_sequence": stream_sequence,
                }
                event_sha = _digest(event_body)
                outbox = db.execute(
                    "SELECT * FROM completion_outbox WHERE producer_receipt_sha256=?",
                    (producer_sha,),
                ).fetchone()
                if outbox is None:
                    ready_at = _finite(self._clock(), "completion ready clock")
                    db.execute(
                        "INSERT INTO completion_outbox VALUES(?,?,?,?,?,?,'ready',0,"
                        "NULL,NULL,NULL,NULL,NULL,?,NULL)",
                        (
                            event_id, event_sha, producer_sha, consumer, stream,
                            stream_sequence, ready_at,
                        ),
                    )
                    self._fault("publish.before_ready_commit")
                    db.commit()
                    self._fault("publish.after_ready_commit")
                else:
                    if (
                        str(outbox["event_id"]) != event_id
                        or str(outbox["event_sha256"]) != event_sha
                        or str(outbox["consumer_id"]) != consumer
                        or str(outbox["stream_id"]) != stream
                        or int(outbox["stream_sequence"]) != stream_sequence
                    ):
                        raise AcquisitionEvidenceConflict("completion event replay conflicts")
                    ready_at = float(outbox["ready_at"])
                    db.commit()
            except BaseException:
                if db.in_transaction:
                    db.rollback()
                raise
        return CompletionProducerReceipt(
            producer_receipt_sha256=producer_sha,
            event_id=event_id,
            event_sha256=event_sha,
            acquisition_job_id=job,
            consumer_id=consumer,
            stream_id=stream,
            stream_sequence=stream_sequence,
            global_sequence=global_sequence,
            desired_state_sha256=desired,
            authority_sha256=authority,
            proof_sha256=proof_sha,
            content_sha256=content_sha,
            size_bytes=len(payload),
            created_at=created_at,
            ready_at=ready_at,
        )

    def contains_acquisition_job(self, acquisition_job_id: str) -> bool:
        """Return whether an exact hot or archived completion already exists.

        This metadata-only query lets a crash-recovery bridge make monotonic
        progress through the bounded terminal acquisition inventory without
        reopening or copying any raw payload.  Duplicate presence across hot
        and archive authorities is treated as corruption, never as success.
        """

        job = _identifier(acquisition_job_id, "acquisition job ID")
        with self._lock, closing(self._connect()) as db:
            hot = int(db.execute(
                "SELECT count(*) FROM producer_receipts "
                "WHERE acquisition_job_id=?",
                (job,),
            ).fetchone()[0])
            archived = int(db.execute(
                "SELECT count(*) FROM archive_index WHERE acquisition_job_id=?",
                (job,),
            ).fetchone()[0])
        if hot not in {0, 1} or archived not in {0, 1} or hot + archived > 1:
            raise AcquisitionEvidenceIntegrityError(
                "completion acquisition identity is duplicated"
            )
        return hot + archived == 1

    def load_acquisition_completion(
        self,
        acquisition_job_id: str,
    ) -> AcquisitionCompletionLookup | None:
        """Load and reprove one completion by its unique acquisition job.

        A terminal-inventory bridge must not treat mere identity presence as
        proof that the existing consumer, stream, desired authority, proof and
        payload are the completion it would have published.  This read returns
        the complete checksum-verified hot or sealed authority so that such a
        bridge can compare exact expected bytes before advancing its cursor.
        """

        job = _identifier(acquisition_job_id, "acquisition job ID")
        with self._lock, closing(self._connect()) as db:
            producer = db.execute(
                "SELECT * FROM producer_receipts WHERE acquisition_job_id=?",
                (job,),
            ).fetchone()
            archived = db.execute(
                "SELECT * FROM archive_index WHERE acquisition_job_id=?",
                (job,),
            ).fetchone()
            if producer is not None and archived is not None:
                raise AcquisitionEvidenceIntegrityError(
                    "completion acquisition identity exists hot and archived"
                )
            if producer is not None:
                outbox = db.execute(
                    "SELECT * FROM completion_outbox "
                    "WHERE producer_receipt_sha256=?",
                    (str(producer["producer_receipt_sha256"]),),
                ).fetchone()
                if outbox is None:
                    raise AcquisitionEvidenceIntegrityError(
                        "hot acquisition completion has no event"
                    )
                receipt = self._producer_from_rows(producer, outbox)
                try:
                    proof = json.loads(bytes(producer["proof_json"]))
                except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                    raise AcquisitionEvidenceIntegrityError(
                        "hot acquisition completion proof is corrupt"
                    ) from exc
                content_sha = receipt.content_sha256
                size_bytes = receipt.size_bytes
            elif archived is not None:
                archived_event = str(archived["event_id"])
            else:
                return None
        if producer is not None:
            payload = self._verified_file(
                self.hot_directory / f"{content_sha}.raw",
                content_sha,
                size_bytes,
            )
            return AcquisitionCompletionLookup(
                status="hot",
                producer_receipt=receipt,
                proof=proof,
                payload=payload,
            )
        record, payload, segment_id = self._archived_record_by_event(
            archived_event
        )
        archived_producer = self._load_archived_producer(archived_event)
        if archived_producer.acquisition_job_id != job:
            raise AcquisitionEvidenceIntegrityError(
                "archived acquisition lookup changed job identity"
            )
        proof = record.get("proof")
        if not isinstance(proof, dict):
            raise AcquisitionEvidenceIntegrityError(
                "archived acquisition completion proof differs"
            )
        return AcquisitionCompletionLookup(
            status="archived",
            producer_receipt=archived_producer,
            proof=proof,
            payload=payload,
            archive_segment_id=segment_id,
        )

    @staticmethod
    def _event_sha(row: sqlite3.Row) -> str:
        expected = _digest({
            "consumer_id": str(row["consumer_id"]),
            "producer_receipt_sha256": str(row["producer_receipt_sha256"]),
            "schema": COMPLETION_EVENT_SCHEMA,
            "stream_id": str(row["stream_id"]),
            "stream_sequence": int(row["stream_sequence"]),
        })
        if not hmac.compare_digest(expected, str(row["event_sha256"])):
            raise AcquisitionEvidenceIntegrityError("completion event checksum mismatch")
        event_id = "completion-" + hashlib.sha256(
            (COMPLETION_EVENT_SCHEMA + str(row["producer_receipt_sha256"])).encode(
                "ascii"
            )
        ).hexdigest()
        if event_id != str(row["event_id"]):
            raise AcquisitionEvidenceIntegrityError("completion event ID mismatch")
        if str(row["state"]) not in _OUTBOX_STATES:
            raise AcquisitionEvidenceIntegrityError("completion event state invalid")
        return expected

    @staticmethod
    def _consumption_from_row(row: sqlite3.Row) -> ConsumptionReceipt:
        receipt = ConsumptionReceipt(
            receipt_sha256=str(row["receipt_sha256"]),
            event_id=str(row["event_id"]),
            event_sha256=str(row["event_sha256"]),
            producer_receipt_sha256=str(row["producer_receipt_sha256"]),
            consumer_id=str(row["consumer_id"]),
            stream_id=str(row["stream_id"]),
            stream_sequence=int(row["stream_sequence"]),
            output_sha256=str(row["output_sha256"]),
            created_at=float(row["created_at"]),
        )
        body = receipt.as_dict()
        body.pop("receipt_sha256")
        if not hmac.compare_digest(_digest(body), receipt.receipt_sha256):
            raise AcquisitionEvidenceIntegrityError(
                "consumption receipt checksum mismatch"
            )
        return receipt

    @staticmethod
    def _rejection_from_mapping(value: Mapping[str, Any]) -> RejectionReceipt:
        """Recompute an immutable rejection identity from persisted fields."""

        try:
            rejection_sha = _sha(
                value["rejection_receipt_sha256"],
                "rejection receipt SHA-256",
            )
            event_id = _identifier(value["event_id"], "rejection event ID")
            event_sha = _sha(value["event_sha256"], "rejection event SHA-256")
            producer_sha = _sha(
                value["producer_receipt_sha256"],
                "rejection producer receipt SHA-256",
            )
            consumer_id = _identifier(
                value["consumer_id"], "rejection consumer ID"
            )
            stream_id = _identifier(value["stream_id"], "rejection stream ID")
            stream_sequence = _count(
                value["stream_sequence"], "rejection stream sequence"
            )
            if stream_sequence < 1:
                raise AcquisitionEvidenceLifecycleError(
                    "rejection stream sequence must be positive"
                )
            rejected_desired = _sha(
                value["rejected_desired_state_sha256"],
                "rejected desired state SHA-256",
            )
            active_value = value["active_desired_state_sha256"]
            active_desired = (
                None
                if active_value is None
                else _sha(active_value, "active desired state SHA-256")
            )
            reason = value["reason_code"]
            if not isinstance(reason, str) or reason not in REJECTION_REASON_CODES:
                raise AcquisitionEvidenceLifecycleError(
                    "rejection reason is not allowlisted"
                )
            if reason == "active_desired_state_unavailable":
                if active_desired is not None:
                    raise AcquisitionEvidenceLifecycleError(
                        "unavailable desired-state rejection cannot bind an active SHA"
                    )
            elif active_desired is None or hmac.compare_digest(
                active_desired, rejected_desired
            ):
                raise AcquisitionEvidenceLifecycleError(
                    "obsolete desired-state rejection must bind a different active SHA"
                )
            created_at = _finite(value["created_at"], "rejection timestamp")
        except (
            KeyError,
            TypeError,
            ValueError,
            AcquisitionEvidenceLifecycleError,
        ) as exc:
            if isinstance(exc, AcquisitionEvidenceIntegrityError):
                raise
            raise AcquisitionEvidenceIntegrityError(
                "rejection receipt fields are invalid"
            ) from exc
        receipt = RejectionReceipt(
            rejection_receipt_sha256=rejection_sha,
            event_id=event_id,
            event_sha256=event_sha,
            producer_receipt_sha256=producer_sha,
            consumer_id=consumer_id,
            stream_id=stream_id,
            stream_sequence=stream_sequence,
            rejected_desired_state_sha256=rejected_desired,
            active_desired_state_sha256=active_desired,
            reason_code=reason,
            created_at=created_at,
        )
        body = receipt.as_dict()
        body.pop("rejection_receipt_sha256")
        if not hmac.compare_digest(_digest(body), rejection_sha):
            raise AcquisitionEvidenceIntegrityError(
                "rejection receipt checksum mismatch"
            )
        _assert_secret_free(receipt.as_dict())
        return receipt

    @classmethod
    def _rejection_from_row(cls, row: sqlite3.Row) -> RejectionReceipt:
        return cls._rejection_from_mapping(dict(row))

    @staticmethod
    def _quarantine_from_mapping(value: Mapping[str, Any]) -> QuarantineReceipt:
        """Recompute an immutable quarantine identity from persisted fields."""

        try:
            quarantine_sha = _sha(
                value["quarantine_receipt_sha256"],
                "quarantine receipt SHA-256",
            )
            event_id = _identifier(value["event_id"], "quarantine event ID")
            event_sha = _sha(value["event_sha256"], "quarantine event SHA-256")
            producer_sha = _sha(
                value["producer_receipt_sha256"],
                "quarantine producer receipt SHA-256",
            )
            consumer_id = _identifier(
                value["consumer_id"], "quarantine consumer ID"
            )
            stream_id = _identifier(value["stream_id"], "quarantine stream ID")
            stream_sequence = _count(
                value["stream_sequence"], "quarantine stream sequence"
            )
            if stream_sequence < 1:
                raise AcquisitionEvidenceLifecycleError(
                    "quarantine stream sequence must be positive"
                )
            fingerprint = _sha(
                value["evidence_fingerprint_sha256"],
                "quarantine evidence fingerprint SHA-256",
            )
            reason = value["reason_code"]
            if not isinstance(reason, str) or reason not in QUARANTINE_REASON_CODES:
                raise AcquisitionEvidenceLifecycleError(
                    "quarantine reason is not allowlisted"
                )
            created_at = _finite(value["created_at"], "quarantine timestamp")
        except (KeyError, TypeError, ValueError, AcquisitionEvidenceLifecycleError) as exc:
            if isinstance(exc, AcquisitionEvidenceIntegrityError):
                raise
            raise AcquisitionEvidenceIntegrityError(
                "quarantine receipt fields are invalid"
            ) from exc
        receipt = QuarantineReceipt(
            quarantine_receipt_sha256=quarantine_sha,
            event_id=event_id,
            event_sha256=event_sha,
            producer_receipt_sha256=producer_sha,
            consumer_id=consumer_id,
            stream_id=stream_id,
            stream_sequence=stream_sequence,
            evidence_fingerprint_sha256=fingerprint,
            reason_code=reason,
            created_at=created_at,
        )
        body = receipt.as_dict()
        body.pop("quarantine_receipt_sha256")
        if not hmac.compare_digest(_digest(body), quarantine_sha):
            raise AcquisitionEvidenceIntegrityError(
                "quarantine receipt checksum mismatch"
            )
        _assert_secret_free(receipt.as_dict())
        return receipt

    @classmethod
    def _quarantine_from_row(cls, row: sqlite3.Row) -> QuarantineReceipt:
        return cls._quarantine_from_mapping(dict(row))

    @classmethod
    def _validate_quarantine_child(
        cls,
        quarantine_row: sqlite3.Row,
        *,
        outbox: sqlite3.Row,
        event_sha256: str,
        consumption: ConsumptionReceipt,
    ) -> QuarantineReceipt:
        quarantine = cls._quarantine_from_row(quarantine_row)
        if (
            quarantine.event_id != str(outbox["event_id"])
            or quarantine.event_sha256 != event_sha256
            or quarantine.producer_receipt_sha256
            != str(outbox["producer_receipt_sha256"])
            or quarantine.consumer_id != str(outbox["consumer_id"])
            or quarantine.stream_id != str(outbox["stream_id"])
            or quarantine.stream_sequence != int(outbox["stream_sequence"])
            or quarantine.created_at != consumption.created_at
            or outbox["terminal_at"] is None
            or float(outbox["terminal_at"]) != quarantine.created_at
            or consumption.output_sha256
            != quarantine.quarantine_receipt_sha256
        ):
            raise AcquisitionEvidenceIntegrityError(
                "quarantine, consumption and completion identities differ"
            )
        return quarantine

    @staticmethod
    def _quarantine_terminal_result(
        quarantine: QuarantineReceipt,
        consumption: ConsumptionReceipt,
    ) -> QuarantineTerminalResult:
        quarantine_body = quarantine.as_dict()
        quarantine_sha = quarantine_body.pop("quarantine_receipt_sha256")
        consumption_body = consumption.as_dict()
        consumption_sha = consumption_body.pop("receipt_sha256")
        if (
            not hmac.compare_digest(_digest(quarantine_body), quarantine_sha)
            or not hmac.compare_digest(_digest(consumption_body), consumption_sha)
            or consumption.event_id != quarantine.event_id
            or consumption.event_sha256 != quarantine.event_sha256
            or consumption.producer_receipt_sha256
            != quarantine.producer_receipt_sha256
            or consumption.consumer_id != quarantine.consumer_id
            or consumption.stream_id != quarantine.stream_id
            or consumption.stream_sequence != quarantine.stream_sequence
            or consumption.created_at != quarantine.created_at
            or consumption.output_sha256
            != quarantine.quarantine_receipt_sha256
        ):
            raise AcquisitionEvidenceIntegrityError(
                "quarantine terminal receipt pair differs"
            )
        result = QuarantineTerminalResult(
            quarantine_receipt=quarantine,
            consumption_receipt=consumption,
        )
        mapping = result.as_dict()
        _assert_secret_free(mapping)
        if len(_canonical(mapping)) > MAX_DIAGNOSTIC_BYTES:
            raise AcquisitionEvidenceCapacityError(
                "quarantine terminal result exceeds bound"
            )
        return result

    def _producer_from_rows(
        self, producer: sqlite3.Row, outbox: sqlite3.Row,
    ) -> CompletionProducerReceipt:
        proof_bytes = bytes(producer["proof_json"])
        if not hmac.compare_digest(
            hashlib.sha256(proof_bytes).hexdigest(), str(producer["proof_sha256"])
        ):
            raise AcquisitionEvidenceIntegrityError("producer proof checksum mismatch")
        try:
            proof = json.loads(proof_bytes)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise AcquisitionEvidenceIntegrityError("producer proof is corrupt") from exc
        _assert_secret_free(proof)
        core = self._producer_core(
            acquisition_job_id=str(producer["acquisition_job_id"]),
            consumer_id=str(producer["consumer_id"]),
            stream_id=str(producer["stream_id"]),
            desired_state_sha256=str(producer["desired_state_sha256"]),
            authority_sha256=str(producer["authority_sha256"]),
            proof_sha256=str(producer["proof_sha256"]),
            content_sha256=str(producer["content_sha256"]),
            size_bytes=int(producer["size_bytes"]),
        )
        expected = _digest({
            **core,
            "created_at": float(producer["created_at"]),
            "global_sequence": int(producer["global_sequence"]),
            "stream_sequence": int(producer["stream_sequence"]),
        })
        if not hmac.compare_digest(
            expected, str(producer["producer_receipt_sha256"])
        ):
            raise AcquisitionEvidenceIntegrityError("producer receipt checksum mismatch")
        event_sha = self._event_sha(outbox)
        if (
            str(outbox["producer_receipt_sha256"]) != expected
            or str(outbox["consumer_id"]) != str(producer["consumer_id"])
            or str(outbox["stream_id"]) != str(producer["stream_id"])
            or int(outbox["stream_sequence"]) != int(producer["stream_sequence"])
        ):
            raise AcquisitionEvidenceIntegrityError(
                "producer receipt and completion event differ"
            )
        return CompletionProducerReceipt(
            producer_receipt_sha256=expected,
            event_id=str(outbox["event_id"]),
            event_sha256=event_sha,
            acquisition_job_id=str(producer["acquisition_job_id"]),
            consumer_id=str(producer["consumer_id"]),
            stream_id=str(producer["stream_id"]),
            stream_sequence=int(producer["stream_sequence"]),
            global_sequence=int(producer["global_sequence"]),
            desired_state_sha256=str(producer["desired_state_sha256"]),
            authority_sha256=str(producer["authority_sha256"]),
            proof_sha256=str(producer["proof_sha256"]),
            content_sha256=str(producer["content_sha256"]),
            size_bytes=int(producer["size_bytes"]),
            created_at=float(producer["created_at"]),
            ready_at=float(outbox["ready_at"]),
        )

    @staticmethod
    def _cursor(
        db: sqlite3.Connection, consumer: str, stream: str, now: float,
    ) -> sqlite3.Row:
        row = db.execute(
            "SELECT * FROM consumer_cursors WHERE consumer_id=? AND stream_id=?",
            (consumer, stream),
        ).fetchone()
        if row is None:
            archived = int(db.execute(
                "SELECT count(*) FROM archive_index WHERE consumer_id=? AND stream_id=?",
                (consumer, stream),
            ).fetchone()[0])
            if archived:
                raise AcquisitionEvidenceIntegrityError(
                    "consumer cursor missing behind archived window"
                )
            db.execute(
                "INSERT INTO consumer_cursors VALUES(?,?,0,NULL,?)",
                (consumer, stream, now),
            )
            row = db.execute(
                "SELECT * FROM consumer_cursors WHERE consumer_id=? AND stream_id=?",
                (consumer, stream),
            ).fetchone()
        if row is None:  # pragma: no cover - insert contract
            raise AcquisitionEvidenceIntegrityError("consumer cursor disappeared")
        return row

    def _advance_terminal_cursor(
        self, db: sqlite3.Connection, consumer: str, stream: str, now: float,
    ) -> sqlite3.Row:
        """Reconcile receipts and skip superseded events in exact order."""

        cursor = self._cursor(db, consumer, stream, now)
        while True:
            expected_sequence = int(cursor["last_sequence"]) + 1
            row = db.execute(
                "SELECT * FROM completion_outbox WHERE consumer_id=? AND stream_id=? "
                "AND stream_sequence=?",
                (consumer, stream, expected_sequence),
            ).fetchone()
            if row is None:
                head = db.execute(
                    "SELECT consumer_id,last_sequence FROM stream_heads WHERE stream_id=?",
                    (stream,),
                ).fetchone()
                if head is not None and str(head["consumer_id"]) != consumer:
                    raise AcquisitionEvidenceIntegrityError(
                        "completion stream consumer authority differs"
                    )
                if head is not None and int(head["last_sequence"]) >= expected_sequence:
                    archived = db.execute(
                        "SELECT event_sha256 FROM archive_index WHERE consumer_id=? "
                        "AND stream_id=? AND stream_sequence=?",
                        (consumer, stream, expected_sequence),
                    ).fetchone()
                    if archived is None:
                        raise AcquisitionEvidenceIntegrityError(
                            "completion stream contains a gap"
                        )
                    # Archived rows were cursor-past before pruning. A cursor
                    # behind them is corruption, not an implicit replay.
                    raise AcquisitionEvidenceIntegrityError(
                        "consumer cursor regressed behind archive"
                    )
                return cursor
            event_sha = self._event_sha(row)
            state = str(row["state"])
            if state == "superseded":
                replacement = row["superseded_by_event_id"]
                supersession = row["supersession_sha256"]
                expected_supersession = _digest({
                    "event_id": str(row["event_id"]),
                    "event_sha256": event_sha,
                    "replacement_event_id": replacement,
                    "schema": "stv3-worker-acquisition-supersession-v1",
                })
                if (
                    replacement is None
                    or supersession is None
                    or not hmac.compare_digest(
                        expected_supersession, str(supersession)
                    )
                ):
                    raise AcquisitionEvidenceIntegrityError(
                        "supersession receipt checksum mismatch"
                    )
            elif state in {"ready", "leased"}:
                receipt_row = db.execute(
                    "SELECT * FROM consumption_receipts WHERE event_id=?",
                    (str(row["event_id"]),),
                ).fetchone()
                if receipt_row is None:
                    return cursor
                receipt = self._consumption_from_row(receipt_row)
                if (
                    receipt.event_sha256 != event_sha
                    or receipt.producer_receipt_sha256
                    != str(row["producer_receipt_sha256"])
                    or receipt.consumer_id != consumer
                    or receipt.stream_id != stream
                    or receipt.stream_sequence != expected_sequence
                ):
                    raise AcquisitionEvidenceIntegrityError(
                        "consumption receipt and event differ"
                    )
                changed = db.execute(
                    "UPDATE completion_outbox SET state='consumed',lease_owner=NULL,"
                    "lease_token_sha256=NULL,lease_expires_at=NULL,terminal_at=? "
                    "WHERE event_id=? AND state IN ('ready','leased')",
                    (receipt.created_at, receipt.event_id),
                ).rowcount
                if changed != 1:
                    raise AcquisitionEvidenceConflict(
                        "completion changed during receipt reconciliation"
                    )
                state = "consumed"
            elif state == "consumed":
                receipt_row = db.execute(
                    "SELECT * FROM consumption_receipts WHERE event_id=?",
                    (str(row["event_id"]),),
                ).fetchone()
                if receipt_row is None:
                    raise AcquisitionEvidenceIntegrityError(
                        "consumed completion lacks receipt"
                    )
                receipt = self._consumption_from_row(receipt_row)
                if receipt.event_sha256 != event_sha:
                    raise AcquisitionEvidenceIntegrityError(
                        "consumed receipt event checksum differs"
                    )
            else:  # pragma: no cover - SQL CHECK plus event validation
                raise AcquisitionEvidenceIntegrityError("unknown completion state")
            if state == "consumed":
                rejection_row = db.execute(
                    "SELECT * FROM rejection_receipts WHERE event_id=?",
                    (str(row["event_id"]),),
                ).fetchone()
                quarantine_row = db.execute(
                    "SELECT * FROM quarantine_receipts WHERE event_id=?",
                    (str(row["event_id"]),),
                ).fetchone()
                if rejection_row is not None and quarantine_row is not None:
                    raise AcquisitionEvidenceIntegrityError(
                        "consumed completion has conflicting terminal children"
                    )
                if rejection_row is not None:
                    rejection = self._rejection_from_row(rejection_row)
                    if (
                        rejection.event_id != str(row["event_id"])
                        or rejection.event_sha256 != event_sha
                        or rejection.producer_receipt_sha256
                        != str(row["producer_receipt_sha256"])
                        or rejection.consumer_id != consumer
                        or rejection.stream_id != stream
                        or rejection.stream_sequence != expected_sequence
                        or receipt.output_sha256
                        != rejection.rejection_receipt_sha256
                    ):
                        raise AcquisitionEvidenceIntegrityError(
                            "rejection, consumption and completion identities differ"
                        )
                elif quarantine_row is not None:
                    self._validate_quarantine_child(
                        quarantine_row,
                        outbox=row,
                        event_sha256=event_sha,
                        consumption=receipt,
                    )
            if state not in {"consumed", "superseded"}:
                return cursor
            changed = db.execute(
                "UPDATE consumer_cursors SET last_sequence=?,last_event_sha256=?,"
                "updated_at=? WHERE consumer_id=? AND stream_id=? AND last_sequence=?",
                (
                    expected_sequence, event_sha, now, consumer, stream,
                    expected_sequence - 1,
                ),
            ).rowcount
            if changed != 1:
                raise AcquisitionEvidenceConflict("consumer cursor CAS failed")
            cursor = db.execute(
                "SELECT * FROM consumer_cursors WHERE consumer_id=? AND stream_id=?",
                (consumer, stream),
            ).fetchone()
            if cursor is None:  # pragma: no cover - update contract
                raise AcquisitionEvidenceIntegrityError("consumer cursor disappeared")

    def claim_next(
        self,
        consumer_id: str,
        stream_id: str,
        owner_id: str,
        *,
        lease_seconds: float = 120.0,
        now: float | None = None,
    ) -> CompletionLease | None:
        """Lease exactly the cursor's next ready completion; never skip gaps."""

        consumer = _identifier(consumer_id, "consumer ID")
        stream = _identifier(stream_id, "stream ID")
        owner = _identifier(owner_id, "lease owner")
        duration = _finite(lease_seconds, "lease duration", minimum=0.001)
        if duration > 3600.0:
            raise AcquisitionEvidenceLifecycleError("lease duration exceeds bound")
        wall_now = _finite(self._clock() if now is None else now, "claim clock")
        token = _identifier(self._token_factory(), "lease token")
        token_sha = hashlib.sha256(token.encode("utf-8")).hexdigest()
        with self._lock, closing(self._connect()) as db:
            try:
                db.execute("BEGIN IMMEDIATE")
                db.execute(
                    "UPDATE completion_outbox SET state='ready',lease_owner=NULL,"
                    "lease_token_sha256=NULL,lease_expires_at=NULL "
                    "WHERE consumer_id=? AND stream_id=? AND state='leased' "
                    "AND lease_expires_at<=?",
                    (consumer, stream, wall_now),
                )
                cursor = self._advance_terminal_cursor(db, consumer, stream, wall_now)
                sequence = int(cursor["last_sequence"]) + 1
                row = db.execute(
                    "SELECT outbox.*,producer.* FROM completion_outbox AS outbox "
                    "JOIN producer_receipts AS producer USING(producer_receipt_sha256) "
                    "WHERE outbox.consumer_id=? AND outbox.stream_id=? "
                    "AND outbox.stream_sequence=?",
                    (consumer, stream, sequence),
                ).fetchone()
                if row is None:
                    db.commit()
                    return None
                self._event_sha(row)
                if str(row["state"]) == "leased":
                    db.commit()
                    return None
                if str(row["state"]) != "ready":
                    raise AcquisitionEvidenceIntegrityError(
                        "cursor points at a non-ready terminal completion"
                    )
                expires = wall_now + duration
                changed = db.execute(
                    "UPDATE completion_outbox SET state='leased',attempt_count="
                    "attempt_count+1,lease_owner=?,lease_token_sha256=?,"
                    "lease_expires_at=? WHERE event_id=? AND state='ready'",
                    (owner, token_sha, expires, str(row["event_id"])),
                ).rowcount
                if changed != 1:
                    raise AcquisitionEvidenceConflict("completion lease CAS failed")
                self._fault("claim.before_commit")
                db.commit()
                self._fault("claim.after_commit")
            except BaseException:
                if db.in_transaction:
                    db.rollback()
                raise
        return self.load_leased(str(row["event_id"]), token)

    def load_leased(self, event_id: str, lease_token: str) -> CompletionLease:
        """Load exact proof and bytes only for the current lease capability."""

        event = _identifier(event_id, "event ID")
        token = _identifier(lease_token, "lease token")
        token_sha = hashlib.sha256(token.encode("utf-8")).hexdigest()
        with self._lock, closing(self._connect()) as db:
            row = db.execute(
                "SELECT outbox.*,producer.* FROM completion_outbox AS outbox "
                "JOIN producer_receipts AS producer USING(producer_receipt_sha256) "
                "WHERE outbox.event_id=?",
                (event,),
            ).fetchone()
            if row is None:
                raise CompletionWindowExpired("completion is outside the hot window")
            event_sha = self._event_sha(row)
            if (
                str(row["state"]) != "leased"
                or row["lease_token_sha256"] is None
                or not hmac.compare_digest(str(row["lease_token_sha256"]), token_sha)
            ):
                raise AcquisitionEvidenceConflict("completion lease capability differs")
            proof_bytes = bytes(row["proof_json"])
            proof_sha = hashlib.sha256(proof_bytes).hexdigest()
            if not hmac.compare_digest(proof_sha, str(row["proof_sha256"])):
                raise AcquisitionEvidenceIntegrityError("completion proof checksum differs")
            try:
                proof = json.loads(proof_bytes)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise AcquisitionEvidenceIntegrityError("completion proof is corrupt") from exc
            _assert_secret_free(proof)
            content_sha = str(row["content_sha256"])
            size = int(row["size_bytes"])
            object_row = db.execute(
                "SELECT * FROM hot_objects WHERE content_sha256=?",
                (content_sha,),
            ).fetchone()
            if (
                object_row is None
                or str(object_row["state"]) != "active"
                or int(object_row["size_bytes"]) != size
            ):
                raise AcquisitionEvidenceIntegrityError("hot object catalog differs")
        payload = self._verified_file(
            self.hot_directory / f"{content_sha}.raw", content_sha, size
        )
        return CompletionLease(
            event_id=event,
            event_sha256=event_sha,
            producer_receipt_sha256=str(row["producer_receipt_sha256"]),
            acquisition_job_id=str(row["acquisition_job_id"]),
            consumer_id=str(row["consumer_id"]),
            stream_id=str(row["stream_id"]),
            stream_sequence=int(row["stream_sequence"]),
            desired_state_sha256=str(row["desired_state_sha256"]),
            authority_sha256=str(row["authority_sha256"]),
            content_sha256=content_sha,
            size_bytes=size,
            attempt_count=int(row["attempt_count"]),
            lease_token=token,
            lease_expires_at=float(row["lease_expires_at"]),
            proof=proof,
            payload=payload,
        )

    def acknowledge(
        self,
        event_id: str,
        lease_token: str,
        *,
        output_sha256: str,
        now: float | None = None,
    ) -> ConsumptionReceipt:
        """Durably receipt an output before terminal state/cursor advancement."""

        event = _identifier(event_id, "event ID")
        token = _identifier(lease_token, "lease token")
        output = _sha(output_sha256, "consumer output SHA-256")
        token_sha = hashlib.sha256(token.encode("utf-8")).hexdigest()
        wall_now = _finite(self._clock() if now is None else now, "ack clock")
        with self._lock, closing(self._connect()) as db:
            try:
                db.execute("BEGIN IMMEDIATE")
                prior_row = db.execute(
                    "SELECT * FROM consumption_receipts WHERE event_id=?",
                    (event,),
                ).fetchone()
                quarantine_row = db.execute(
                    "SELECT * FROM quarantine_receipts WHERE event_id=?",
                    (event,),
                ).fetchone()
                rejection_row = db.execute(
                    "SELECT 1 FROM rejection_receipts WHERE event_id=?",
                    (event,),
                ).fetchone()
                row = db.execute(
                    "SELECT * FROM completion_outbox WHERE event_id=?",
                    (event,),
                ).fetchone()
                if row is None:
                    archived = db.execute(
                        "SELECT segment_id FROM archive_index WHERE event_id=?",
                        (event,),
                    ).fetchone()
                    if archived is not None:
                        db.commit()
                        archived_record, _, _ = self._archived_record_by_event(
                            event
                        )
                        if archived_record.get("quarantine_receipt") is not None:
                            raise AcquisitionEvidenceConflict(
                                "quarantined completion is not normal consumption"
                            )
                        if archived_record.get("rejection_receipt") is not None:
                            raise AcquisitionEvidenceConflict(
                                "rejected completion is not normal consumption"
                            )
                        archived_receipt = self._load_archived_consumption(event)
                        if archived_receipt.output_sha256 != output:
                            raise AcquisitionEvidenceConflict(
                                "archived consumer output receipt conflicts"
                            )
                        return archived_receipt
                    raise AcquisitionEvidenceConflict("completion event is unknown")
                event_sha = self._event_sha(row)
                if prior_row is not None:
                    receipt = self._consumption_from_row(prior_row)
                    if rejection_row is not None and quarantine_row is not None:
                        raise AcquisitionEvidenceIntegrityError(
                            "consumed completion has conflicting terminal children"
                        )
                    if quarantine_row is not None:
                        self._validate_quarantine_child(
                            quarantine_row,
                            outbox=row,
                            event_sha256=event_sha,
                            consumption=receipt,
                        )
                        raise AcquisitionEvidenceConflict(
                            "quarantined completion is not normal consumption"
                        )
                    if rejection_row is not None:
                        raise AcquisitionEvidenceConflict(
                            "rejected completion is not normal consumption"
                        )
                    if receipt.output_sha256 != output:
                        raise AcquisitionEvidenceConflict(
                            "consumer output receipt conflicts"
                        )
                    db.commit()
                else:
                    if quarantine_row is not None:
                        raise AcquisitionEvidenceIntegrityError(
                            "quarantine receipt lacks its consumption receipt"
                        )
                    if rejection_row is not None:
                        raise AcquisitionEvidenceIntegrityError(
                            "rejection receipt lacks its consumption receipt"
                        )
                    if (
                        str(row["state"]) != "leased"
                        or row["lease_token_sha256"] is None
                        or not hmac.compare_digest(
                            str(row["lease_token_sha256"]), token_sha
                        )
                        or float(row["lease_expires_at"]) <= wall_now
                    ):
                        raise AcquisitionEvidenceConflict(
                            "completion acknowledgement lease differs or expired"
                        )
                    body = {
                        "consumer_id": str(row["consumer_id"]),
                        "created_at": wall_now,
                        "event_id": event,
                        "event_sha256": event_sha,
                        "output_sha256": output,
                        "producer_receipt_sha256": str(
                            row["producer_receipt_sha256"]
                        ),
                        "schema": CONSUMPTION_RECEIPT_SCHEMA,
                        "stream_id": str(row["stream_id"]),
                        "stream_sequence": int(row["stream_sequence"]),
                    }
                    receipt_sha = _digest(body)
                    db.execute(
                        "INSERT INTO consumption_receipts VALUES(?,?,?,?,?,?,?,?,?)",
                        (
                            receipt_sha, event, event_sha,
                            str(row["producer_receipt_sha256"]),
                            str(row["consumer_id"]), str(row["stream_id"]),
                            int(row["stream_sequence"]), output, wall_now,
                        ),
                    )
                    receipt = ConsumptionReceipt(
                        receipt_sha256=receipt_sha,
                        event_id=event,
                        event_sha256=event_sha,
                        producer_receipt_sha256=str(
                            row["producer_receipt_sha256"]
                        ),
                        consumer_id=str(row["consumer_id"]),
                        stream_id=str(row["stream_id"]),
                        stream_sequence=int(row["stream_sequence"]),
                        output_sha256=output,
                        created_at=wall_now,
                    )
                    self._fault("ack.before_receipt_commit")
                    db.commit()
                    self._fault("ack.after_receipt_commit")
            except BaseException:
                if db.in_transaction:
                    db.rollback()
                raise
        with self._lock, closing(self._connect()) as db:
            try:
                db.execute("BEGIN IMMEDIATE")
                self._advance_terminal_cursor(
                    db, receipt.consumer_id, receipt.stream_id, wall_now
                )
                self._fault("ack.before_cursor_commit")
                db.commit()
                self._fault("ack.after_cursor_commit")
            except BaseException:
                if db.in_transaction:
                    db.rollback()
                raise
        return receipt

    @staticmethod
    def _assert_rejection_request_matches(
        rejection: RejectionReceipt,
        *,
        expected_event_sha256: str,
        reason_code: str,
        active_desired_state_sha256: str | None,
    ) -> None:
        if (
            not hmac.compare_digest(
                rejection.event_sha256, expected_event_sha256
            )
            or rejection.reason_code != reason_code
            or rejection.active_desired_state_sha256
            != active_desired_state_sha256
        ):
            raise AcquisitionEvidenceConflict(
                "desired-state rejection receipt conflicts"
            )

    def reject_leased(
        self,
        event_id: str,
        lease_token: str,
        *,
        expected_event_sha256: str,
        reason_code: str,
        active_desired_state_sha256: str | None,
        now: float | None = None,
    ) -> ConsumptionReceipt:
        """Durably reject an obsolete leased completion, then advance its cursor.

        The returned consumption receipt never contains a fabricated pipeline
        output.  Its ``output_sha256`` is the checksum of a separately persisted
        rejection receipt binding the event, producer desired-state identity,
        active desired-state identity and an allowlisted reason.  Both receipts
        commit atomically before terminal state or cursor movement.
        """

        event = _identifier(event_id, "event ID")
        token = _identifier(lease_token, "lease token")
        expected_event = _sha(
            expected_event_sha256, "expected completion event SHA-256"
        )
        if not isinstance(reason_code, str) or reason_code not in REJECTION_REASON_CODES:
            raise AcquisitionEvidenceLifecycleError(
                "rejection reason is not allowlisted"
            )
        active_desired = (
            None
            if active_desired_state_sha256 is None
            else _sha(
                active_desired_state_sha256,
                "active desired state SHA-256",
            )
        )
        if reason_code == "active_desired_state_unavailable":
            if active_desired is not None:
                raise AcquisitionEvidenceLifecycleError(
                    "unavailable desired-state rejection cannot bind an active SHA"
                )
        elif active_desired is None:
            raise AcquisitionEvidenceLifecycleError(
                "obsolete desired-state rejection requires an active SHA"
            )
        token_sha = hashlib.sha256(token.encode("utf-8")).hexdigest()
        wall_now = _finite(self._clock() if now is None else now, "reject clock")

        with self._lock, closing(self._connect()) as db:
            try:
                db.execute("BEGIN IMMEDIATE")
                prior_consumption_row = db.execute(
                    "SELECT * FROM consumption_receipts WHERE event_id=?",
                    (event,),
                ).fetchone()
                prior_rejection_row = db.execute(
                    "SELECT * FROM rejection_receipts WHERE event_id=?",
                    (event,),
                ).fetchone()
                row = db.execute(
                    "SELECT outbox.*,producer.desired_state_sha256 "
                    "FROM completion_outbox AS outbox JOIN producer_receipts AS producer "
                    "USING(producer_receipt_sha256) WHERE outbox.event_id=?",
                    (event,),
                ).fetchone()
                if row is None:
                    archived = db.execute(
                        "SELECT segment_id FROM archive_index WHERE event_id=?",
                        (event,),
                    ).fetchone()
                    if archived is not None:
                        db.commit()
                        archived_rejection = self._load_archived_rejection(event)
                        self._assert_rejection_request_matches(
                            archived_rejection,
                            expected_event_sha256=expected_event,
                            reason_code=reason_code,
                            active_desired_state_sha256=active_desired,
                        )
                        return self._load_archived_consumption(event)
                    raise AcquisitionEvidenceConflict("completion event is unknown")

                event_sha = self._event_sha(row)
                if not hmac.compare_digest(event_sha, expected_event):
                    raise AcquisitionEvidenceConflict(
                        "expected completion event checksum differs"
                    )
                rejected_desired = _sha(
                    row["desired_state_sha256"],
                    "producer desired state SHA-256",
                )
                if (
                    reason_code != "active_desired_state_unavailable"
                    and active_desired is not None
                    and hmac.compare_digest(active_desired, rejected_desired)
                ):
                    raise AcquisitionEvidenceLifecycleError(
                        "obsolete desired-state rejection must bind a different active SHA"
                    )

                if prior_consumption_row is not None:
                    if prior_rejection_row is None:
                        raise AcquisitionEvidenceConflict(
                            "completion already has a non-rejection output receipt"
                        )
                    rejection = self._rejection_from_row(prior_rejection_row)
                    receipt = self._consumption_from_row(prior_consumption_row)
                    if (
                        rejection.event_id != event
                        or rejection.event_sha256 != event_sha
                        or rejection.producer_receipt_sha256
                        != str(row["producer_receipt_sha256"])
                        or rejection.consumer_id != str(row["consumer_id"])
                        or rejection.stream_id != str(row["stream_id"])
                        or rejection.stream_sequence != int(row["stream_sequence"])
                        or rejection.rejected_desired_state_sha256
                        != rejected_desired
                        or receipt.output_sha256
                        != rejection.rejection_receipt_sha256
                    ):
                        raise AcquisitionEvidenceIntegrityError(
                            "rejection, consumption and completion identities differ"
                        )
                    self._assert_rejection_request_matches(
                        rejection,
                        expected_event_sha256=expected_event,
                        reason_code=reason_code,
                        active_desired_state_sha256=active_desired,
                    )
                    db.commit()
                else:
                    if prior_rejection_row is not None:
                        raise AcquisitionEvidenceIntegrityError(
                            "rejection receipt lacks its consumption receipt"
                        )
                    if (
                        str(row["state"]) != "leased"
                        or row["lease_token_sha256"] is None
                        or not hmac.compare_digest(
                            str(row["lease_token_sha256"]), token_sha
                        )
                        or float(row["lease_expires_at"]) <= wall_now
                    ):
                        raise AcquisitionEvidenceConflict(
                            "completion rejection lease differs or expired"
                        )
                    rejection_body = {
                        "active_desired_state_sha256": active_desired,
                        "consumer_id": str(row["consumer_id"]),
                        "created_at": wall_now,
                        "event_id": event,
                        "event_sha256": event_sha,
                        "producer_receipt_sha256": str(
                            row["producer_receipt_sha256"]
                        ),
                        "reason_code": reason_code,
                        "rejected_desired_state_sha256": rejected_desired,
                        "schema": REJECTION_RECEIPT_SCHEMA,
                        "stream_id": str(row["stream_id"]),
                        "stream_sequence": int(row["stream_sequence"]),
                    }
                    rejection_sha = _digest(rejection_body)
                    rejection = RejectionReceipt(
                        rejection_receipt_sha256=rejection_sha,
                        event_id=event,
                        event_sha256=event_sha,
                        producer_receipt_sha256=str(
                            row["producer_receipt_sha256"]
                        ),
                        consumer_id=str(row["consumer_id"]),
                        stream_id=str(row["stream_id"]),
                        stream_sequence=int(row["stream_sequence"]),
                        rejected_desired_state_sha256=rejected_desired,
                        active_desired_state_sha256=active_desired,
                        reason_code=reason_code,
                        created_at=wall_now,
                    )
                    consumption_body = {
                        "consumer_id": rejection.consumer_id,
                        "created_at": wall_now,
                        "event_id": event,
                        "event_sha256": event_sha,
                        "output_sha256": rejection_sha,
                        "producer_receipt_sha256": rejection.producer_receipt_sha256,
                        "schema": CONSUMPTION_RECEIPT_SCHEMA,
                        "stream_id": rejection.stream_id,
                        "stream_sequence": rejection.stream_sequence,
                    }
                    receipt_sha = _digest(consumption_body)
                    db.execute(
                        "INSERT INTO rejection_receipts("
                        "rejection_receipt_sha256,event_id,event_sha256,"
                        "producer_receipt_sha256,consumer_id,stream_id,"
                        "stream_sequence,rejected_desired_state_sha256,"
                        "active_desired_state_sha256,reason_code,created_at) "
                        "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                        (
                            rejection_sha,
                            event,
                            event_sha,
                            rejection.producer_receipt_sha256,
                            rejection.consumer_id,
                            rejection.stream_id,
                            rejection.stream_sequence,
                            rejected_desired,
                            active_desired,
                            reason_code,
                            wall_now,
                        ),
                    )
                    db.execute(
                        "INSERT INTO consumption_receipts VALUES(?,?,?,?,?,?,?,?,?)",
                        (
                            receipt_sha,
                            event,
                            event_sha,
                            rejection.producer_receipt_sha256,
                            rejection.consumer_id,
                            rejection.stream_id,
                            rejection.stream_sequence,
                            rejection_sha,
                            wall_now,
                        ),
                    )
                    receipt = ConsumptionReceipt(
                        receipt_sha256=receipt_sha,
                        event_id=event,
                        event_sha256=event_sha,
                        producer_receipt_sha256=rejection.producer_receipt_sha256,
                        consumer_id=rejection.consumer_id,
                        stream_id=rejection.stream_id,
                        stream_sequence=rejection.stream_sequence,
                        output_sha256=rejection_sha,
                        created_at=wall_now,
                    )
                    self._fault("reject.before_receipt_commit")
                    db.commit()
                    self._fault("reject.after_receipt_commit")
            except BaseException:
                if db.in_transaction:
                    db.rollback()
                raise

        with self._lock, closing(self._connect()) as db:
            try:
                db.execute("BEGIN IMMEDIATE")
                self._advance_terminal_cursor(
                    db, receipt.consumer_id, receipt.stream_id, wall_now
                )
                self._fault("reject.before_cursor_commit")
                db.commit()
                self._fault("reject.after_cursor_commit")
            except BaseException:
                if db.in_transaction:
                    db.rollback()
                raise
        return receipt

    @staticmethod
    def _assert_quarantine_request_matches(
        quarantine: QuarantineReceipt,
        *,
        expected_event_sha256: str,
        reason_code: str,
        evidence_fingerprint_sha256: str,
    ) -> None:
        if (
            not hmac.compare_digest(
                quarantine.event_sha256, expected_event_sha256
            )
            or quarantine.reason_code != reason_code
            or not hmac.compare_digest(
                quarantine.evidence_fingerprint_sha256,
                evidence_fingerprint_sha256,
            )
        ):
            raise AcquisitionEvidenceConflict(
                "permanent quarantine receipt conflicts"
            )

    def quarantine_leased(
        self,
        event_id: str,
        lease_token: str,
        *,
        expected_event_sha256: str,
        reason_code: str,
        evidence_fingerprint_sha256: str,
        now: float | None = None,
    ) -> QuarantineTerminalResult:
        """Terminally quarantine one leased, permanently unusable event.

        Only the closed set of event-bound integrity/contract failures is
        accepted.  Transient cancellation, deadline, capacity, disk and system
        store failures have no reason code here and therefore remain retryable.
        The immutable quarantine child, ordinary consumption receipt, terminal
        outbox row and ordered cursor movement commit in one SQLite transaction.
        """

        event = _identifier(event_id, "event ID")
        token = _identifier(lease_token, "lease token")
        expected_event = _sha(
            expected_event_sha256, "expected completion event SHA-256"
        )
        if (
            not isinstance(reason_code, str)
            or reason_code not in QUARANTINE_REASON_CODES
        ):
            raise AcquisitionEvidenceLifecycleError(
                "quarantine reason is not allowlisted"
            )
        fingerprint = _sha(
            evidence_fingerprint_sha256,
            "quarantine evidence fingerprint SHA-256",
        )
        token_sha = hashlib.sha256(token.encode("utf-8")).hexdigest()
        wall_now = _finite(
            self._clock() if now is None else now, "quarantine clock"
        )

        with self._lock, closing(self._connect()) as db:
            try:
                db.execute("BEGIN IMMEDIATE")
                prior_consumption_row = db.execute(
                    "SELECT * FROM consumption_receipts WHERE event_id=?",
                    (event,),
                ).fetchone()
                prior_quarantine_row = db.execute(
                    "SELECT * FROM quarantine_receipts WHERE event_id=?",
                    (event,),
                ).fetchone()
                prior_rejection_row = db.execute(
                    "SELECT 1 FROM rejection_receipts WHERE event_id=?",
                    (event,),
                ).fetchone()
                row = db.execute(
                    "SELECT * FROM completion_outbox WHERE event_id=?",
                    (event,),
                ).fetchone()
                if row is None:
                    archived = db.execute(
                        "SELECT segment_id FROM archive_index WHERE event_id=?",
                        (event,),
                    ).fetchone()
                    if archived is not None:
                        db.commit()
                        archived_quarantine = self._load_archived_quarantine(
                            event
                        )
                        self._assert_quarantine_request_matches(
                            archived_quarantine,
                            expected_event_sha256=expected_event,
                            reason_code=reason_code,
                            evidence_fingerprint_sha256=fingerprint,
                        )
                        archived_consumption = self._load_archived_consumption(
                            event
                        )
                        return self._quarantine_terminal_result(
                            archived_quarantine,
                            archived_consumption,
                        )
                    raise AcquisitionEvidenceConflict(
                        "completion event is unknown"
                    )

                event_sha = self._event_sha(row)
                if not hmac.compare_digest(event_sha, expected_event):
                    raise AcquisitionEvidenceConflict(
                        "expected completion event checksum differs"
                    )
                if prior_consumption_row is not None:
                    if prior_quarantine_row is None:
                        terminal_kind = (
                            "rejection"
                            if prior_rejection_row is not None
                            else "normal consumption"
                        )
                        raise AcquisitionEvidenceConflict(
                            "completion already has " + terminal_kind
                        )
                    if prior_rejection_row is not None:
                        raise AcquisitionEvidenceIntegrityError(
                            "consumed completion has conflicting terminal children"
                        )
                    consumption = self._consumption_from_row(
                        prior_consumption_row
                    )
                    quarantine = self._validate_quarantine_child(
                        prior_quarantine_row,
                        outbox=row,
                        event_sha256=event_sha,
                        consumption=consumption,
                    )
                    self._assert_quarantine_request_matches(
                        quarantine,
                        expected_event_sha256=expected_event,
                        reason_code=reason_code,
                        evidence_fingerprint_sha256=fingerprint,
                    )
                    if str(row["state"]) != "consumed":
                        raise AcquisitionEvidenceIntegrityError(
                            "quarantine receipts lack terminal outbox state"
                        )
                    cursor = db.execute(
                        "SELECT last_sequence,last_event_sha256 "
                        "FROM consumer_cursors WHERE consumer_id=? AND stream_id=?",
                        (quarantine.consumer_id, quarantine.stream_id),
                    ).fetchone()
                    if (
                        cursor is None
                        or int(cursor["last_sequence"])
                        < quarantine.stream_sequence
                        or (
                            int(cursor["last_sequence"])
                            == quarantine.stream_sequence
                            and cursor["last_event_sha256"]
                            != quarantine.event_sha256
                        )
                    ):
                        raise AcquisitionEvidenceIntegrityError(
                            "quarantine terminal cursor differs"
                        )
                    db.commit()
                    return self._quarantine_terminal_result(
                        quarantine,
                        consumption,
                    )

                if prior_quarantine_row is not None:
                    raise AcquisitionEvidenceIntegrityError(
                        "quarantine receipt lacks its consumption receipt"
                    )
                if prior_rejection_row is not None:
                    raise AcquisitionEvidenceIntegrityError(
                        "rejection receipt lacks its consumption receipt"
                    )
                if (
                    str(row["state"]) != "leased"
                    or row["lease_token_sha256"] is None
                    or not hmac.compare_digest(
                        str(row["lease_token_sha256"]), token_sha
                    )
                    or float(row["lease_expires_at"]) <= wall_now
                ):
                    raise AcquisitionEvidenceConflict(
                        "completion quarantine lease differs or expired"
                    )

                quarantine_body = {
                    "consumer_id": str(row["consumer_id"]),
                    "created_at": wall_now,
                    "event_id": event,
                    "event_sha256": event_sha,
                    "evidence_fingerprint_sha256": fingerprint,
                    "producer_receipt_sha256": str(
                        row["producer_receipt_sha256"]
                    ),
                    "reason_code": reason_code,
                    "schema": QUARANTINE_RECEIPT_SCHEMA,
                    "stream_id": str(row["stream_id"]),
                    "stream_sequence": int(row["stream_sequence"]),
                }
                quarantine_sha = _digest(quarantine_body)
                quarantine = QuarantineReceipt(
                    quarantine_receipt_sha256=quarantine_sha,
                    event_id=event,
                    event_sha256=event_sha,
                    producer_receipt_sha256=str(
                        row["producer_receipt_sha256"]
                    ),
                    consumer_id=str(row["consumer_id"]),
                    stream_id=str(row["stream_id"]),
                    stream_sequence=int(row["stream_sequence"]),
                    evidence_fingerprint_sha256=fingerprint,
                    reason_code=reason_code,
                    created_at=wall_now,
                )
                consumption_body = {
                    "consumer_id": quarantine.consumer_id,
                    "created_at": wall_now,
                    "event_id": event,
                    "event_sha256": event_sha,
                    "output_sha256": quarantine_sha,
                    "producer_receipt_sha256": (
                        quarantine.producer_receipt_sha256
                    ),
                    "schema": CONSUMPTION_RECEIPT_SCHEMA,
                    "stream_id": quarantine.stream_id,
                    "stream_sequence": quarantine.stream_sequence,
                }
                consumption_sha = _digest(consumption_body)
                consumption = ConsumptionReceipt(
                    receipt_sha256=consumption_sha,
                    event_id=event,
                    event_sha256=event_sha,
                    producer_receipt_sha256=(
                        quarantine.producer_receipt_sha256
                    ),
                    consumer_id=quarantine.consumer_id,
                    stream_id=quarantine.stream_id,
                    stream_sequence=quarantine.stream_sequence,
                    output_sha256=quarantine_sha,
                    created_at=wall_now,
                )
                self._fault("quarantine.before_receipt_commit")
                db.execute(
                    "INSERT INTO quarantine_receipts("
                    "quarantine_receipt_sha256,event_id,event_sha256,"
                    "producer_receipt_sha256,consumer_id,stream_id,"
                    "stream_sequence,evidence_fingerprint_sha256,"
                    "reason_code,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (
                        quarantine_sha,
                        event,
                        event_sha,
                        quarantine.producer_receipt_sha256,
                        quarantine.consumer_id,
                        quarantine.stream_id,
                        quarantine.stream_sequence,
                        fingerprint,
                        reason_code,
                        wall_now,
                    ),
                )
                db.execute(
                    "INSERT INTO consumption_receipts VALUES(?,?,?,?,?,?,?,?,?)",
                    (
                        consumption_sha,
                        event,
                        event_sha,
                        quarantine.producer_receipt_sha256,
                        quarantine.consumer_id,
                        quarantine.stream_id,
                        quarantine.stream_sequence,
                        quarantine_sha,
                        wall_now,
                    ),
                )
                self._fault("quarantine.after_receipt_commit")
                changed = db.execute(
                    "UPDATE completion_outbox SET state='consumed',"
                    "lease_owner=NULL,lease_token_sha256=NULL,"
                    "lease_expires_at=NULL,terminal_at=? WHERE event_id=? "
                    "AND state='leased' AND lease_token_sha256=?",
                    (wall_now, event, token_sha),
                ).rowcount
                if changed != 1:
                    raise AcquisitionEvidenceConflict(
                        "completion changed during quarantine commit"
                    )
                self._fault("quarantine.before_cursor_commit")
                self._advance_terminal_cursor(
                    db,
                    quarantine.consumer_id,
                    quarantine.stream_id,
                    wall_now,
                )
                db.commit()
                self._fault("quarantine.after_cursor_commit")
                return self._quarantine_terminal_result(
                    quarantine,
                    consumption,
                )
            except BaseException:
                if db.in_transaction:
                    db.rollback()
                raise

    def supersede(
        self,
        event_id: str,
        *,
        expected_event_sha256: str,
        replacement_event_id: str,
        now: float | None = None,
    ) -> str:
        """CAS-supersede an unleased ready event with a later same-stream event."""

        event = _identifier(event_id, "event ID")
        expected = _sha(expected_event_sha256, "expected event SHA-256")
        replacement = _identifier(replacement_event_id, "replacement event ID")
        wall_now = _finite(self._clock() if now is None else now, "supersede clock")
        with self._lock, closing(self._connect()) as db:
            try:
                db.execute("BEGIN IMMEDIATE")
                row = db.execute(
                    "SELECT * FROM completion_outbox WHERE event_id=?", (event,)
                ).fetchone()
                successor = db.execute(
                    "SELECT * FROM completion_outbox WHERE event_id=?",
                    (replacement,),
                ).fetchone()
                if row is None or successor is None:
                    raise AcquisitionEvidenceConflict("supersession event is unknown")
                actual = self._event_sha(row)
                self._event_sha(successor)
                if not hmac.compare_digest(actual, expected):
                    raise AcquisitionEvidenceConflict("supersession CAS hash differs")
                if (
                    str(row["consumer_id"]) != str(successor["consumer_id"])
                    or str(row["stream_id"]) != str(successor["stream_id"])
                    or int(successor["stream_sequence"])
                    <= int(row["stream_sequence"])
                ):
                    raise AcquisitionEvidenceConflict(
                        "supersession replacement is not a later same-stream event"
                    )
                receipt_sha = _digest({
                    "event_id": event,
                    "event_sha256": actual,
                    "replacement_event_id": replacement,
                    "schema": "stv3-worker-acquisition-supersession-v1",
                })
                if str(row["state"]) == "superseded":
                    if (
                        str(row["superseded_by_event_id"]) != replacement
                        or str(row["supersession_sha256"]) != receipt_sha
                    ):
                        raise AcquisitionEvidenceConflict("supersession replay conflicts")
                elif str(row["state"]) == "ready":
                    changed = db.execute(
                        "UPDATE completion_outbox SET state='superseded',"
                        "superseded_by_event_id=?,supersession_sha256=?,terminal_at=? "
                        "WHERE event_id=? AND state='ready'",
                        (replacement, receipt_sha, wall_now, event),
                    ).rowcount
                    if changed != 1:
                        raise AcquisitionEvidenceConflict("supersession CAS failed")
                else:
                    raise AcquisitionEvidenceConflict(
                        "leased or consumed completion cannot be superseded"
                    )
                self._advance_terminal_cursor(
                    db, str(row["consumer_id"]), str(row["stream_id"]), wall_now
                )
                self._fault("supersede.before_commit")
                db.commit()
                self._fault("supersede.after_commit")
            except BaseException:
                if db.in_transaction:
                    db.rollback()
                raise
        return receipt_sha

    @staticmethod
    def _archive_anchor(row: sqlite3.Row) -> str:
        return _digest({
            "archive_bytes": int(row["archive_bytes"]),
            "archive_namespace": row["archive_namespace"],
            "archived_completion_count": int(row["archived_completion_count"]),
            "contract": ARCHIVE_MANIFEST_SCHEMA,
            "last_segment_file_sha256": row["last_segment_file_sha256"],
            "last_segment_id": row["last_segment_id"],
            "retention_policy_sha256": row["retention_policy_sha256"],
            "segment_count": int(row["segment_count"]),
        })

    def _archive_usage(self) -> tuple[int, int, int]:
        objects = self._bounded_files(
            self.archive_object_directory, self._limits.max_archive_objects + 1
        )
        segments = self._bounded_files(
            self.archive_segment_directory, self._limits.max_archive_segments + 1
        )
        total = 0
        object_count = 0
        segment_count = 0
        for item, kind in (
            *((entry, "object") for entry in objects),
            *((entry, "segment") for entry in segments),
        ):
            staging = (
                _OBJECT_STAGE.fullmatch(item.name)
                if kind == "object"
                else _SEGMENT_STAGE.fullmatch(item.name)
            )
            try:
                info = item.stat()
            except FileNotFoundError:
                if staging is not None:
                    continue
                raise AcquisitionEvidenceIntegrityError(
                    "archive entry disappeared during accounting"
                )
            if item.is_symlink() or not item.is_file():
                raise AcquisitionEvidenceIntegrityError("unsafe archive entry")
            total += info.st_size
            if kind == "object":
                object_count += 1
            else:
                segment_count += 1
            if total > self._limits.max_archive_bytes:
                raise AcquisitionEvidenceCapacityError("archive byte capacity exceeded")
        return total, object_count, segment_count

    @staticmethod
    def _outbox_archive_mapping(row: sqlite3.Row) -> dict[str, Any]:
        if row["lease_owner"] is not None or row["lease_token_sha256"] is not None:
            raise AcquisitionEvidenceIntegrityError("terminal archive row retains lease")
        return {
            "attempt_count": int(row["attempt_count"]),
            "consumer_id": str(row["consumer_id"]),
            "event_id": str(row["event_id"]),
            "event_sha256": str(row["event_sha256"]),
            "ready_at": float(row["ready_at"]),
            "state": str(row["state"]),
            "stream_id": str(row["stream_id"]),
            "stream_sequence": int(row["stream_sequence"]),
            "superseded_by_event_id": row["superseded_by_event_id"],
            "supersession_sha256": row["supersession_sha256"],
            "terminal_at": float(row["terminal_at"]),
        }

    def _archive_record(
        self,
        producer: sqlite3.Row,
        outbox: sqlite3.Row,
        consumption: sqlite3.Row | None,
        rejection: sqlite3.Row | None,
        quarantine: sqlite3.Row | None,
    ) -> tuple[dict[str, Any], bytes]:
        producer_receipt = self._producer_from_rows(producer, outbox)
        proof_bytes = bytes(producer["proof_json"])
        proof = json.loads(proof_bytes)
        outbox_mapping = self._outbox_archive_mapping(outbox)
        state = outbox_mapping["state"]
        if state == "consumed":
            if consumption is None:
                raise AcquisitionEvidenceIntegrityError(
                    "consumed archive candidate lacks receipt"
                )
            consumption_receipt = self._consumption_from_row(consumption)
            consumption_mapping = consumption_receipt.as_dict()
            if rejection is not None and quarantine is not None:
                raise AcquisitionEvidenceIntegrityError(
                    "consumed archive candidate has conflicting terminal children"
                )
            if rejection is None:
                rejection_mapping = None
            else:
                rejection_receipt = self._rejection_from_row(rejection)
                if (
                    rejection_receipt.event_id != producer_receipt.event_id
                    or rejection_receipt.event_sha256
                    != producer_receipt.event_sha256
                    or rejection_receipt.producer_receipt_sha256
                    != producer_receipt.producer_receipt_sha256
                    or rejection_receipt.consumer_id
                    != producer_receipt.consumer_id
                    or rejection_receipt.stream_id != producer_receipt.stream_id
                    or rejection_receipt.stream_sequence
                    != producer_receipt.stream_sequence
                    or rejection_receipt.rejected_desired_state_sha256
                    != producer_receipt.desired_state_sha256
                    or consumption_mapping["output_sha256"]
                    != rejection_receipt.rejection_receipt_sha256
                ):
                    raise AcquisitionEvidenceIntegrityError(
                        "rejection archive identity differs"
                )
                rejection_mapping = rejection_receipt.as_dict()
            if quarantine is None:
                quarantine_mapping = None
            else:
                quarantine_receipt = self._validate_quarantine_child(
                    quarantine,
                    outbox=outbox,
                    event_sha256=producer_receipt.event_sha256,
                    consumption=consumption_receipt,
                )
                quarantine_mapping = quarantine_receipt.as_dict()
            if quarantine_mapping is not None:
                terminal_kind = "quarantined"
            elif rejection_mapping is not None:
                terminal_kind = "rejected"
            else:
                terminal_kind = "consumed"
        elif state == "superseded":
            if (
                consumption is not None
                or rejection is not None
                or quarantine is not None
            ):
                raise AcquisitionEvidenceIntegrityError(
                    "superseded candidate has terminal child receipt"
                )
            consumption_mapping = None
            rejection_mapping = None
            quarantine_mapping = None
            terminal_kind = "superseded"
        else:
            raise AcquisitionEvidenceConflict("non-terminal completion cannot archive")
        object_path = self.hot_directory / f"{producer_receipt.content_sha256}.raw"
        payload = self._verified_file(
            object_path, producer_receipt.content_sha256, producer_receipt.size_bytes
        )
        record = {
            "completion_event": outbox_mapping,
            "consumption_receipt": consumption_mapping,
            "producer_receipt": producer_receipt.as_dict(),
            "proof": proof,
            "raw_object": {
                "content_sha256": producer_receipt.content_sha256,
                "filename": f"{producer_receipt.content_sha256}.raw",
                "size_bytes": producer_receipt.size_bytes,
            },
            "quarantine_receipt": quarantine_mapping,
            "rejection_receipt": rejection_mapping,
            "schema": ARCHIVED_COMPLETION_SCHEMA,
            "terminal_kind": terminal_kind,
        }
        _assert_secret_free(record)
        if len(_canonical(record)) > self._limits.max_proof_bytes + 64 * 1024:
            raise AcquisitionEvidenceCapacityError(
                "archived completion metadata capacity exceeded"
            )
        return record, payload

    def _build_segment(
        self,
        *,
        head: sqlite3.Row,
        policy: EvidenceRetentionPolicy,
        records: list[dict[str, Any]],
    ) -> tuple[str, str, bytes, bytes, dict[str, Any]]:
        if not records:
            raise AcquisitionEvidenceLifecycleError("empty archive segment")
        sequences = [
            int(item["producer_receipt"]["global_sequence"]) for item in records
        ]
        if sequences != sorted(sequences) or len(set(sequences)) != len(sequences):
            raise AcquisitionEvidenceIntegrityError("archive sequence order differs")
        content_sha = _digest(records)
        core = {
            "archive_namespace": policy.archive_namespace,
            "completion_count": len(records),
            "content_sha256": content_sha,
            "first_global_sequence": sequences[0],
            "last_global_sequence": sequences[-1],
            "previous_file_sha256": head["last_segment_file_sha256"],
            "previous_segment_id": head["last_segment_id"],
            "retention_policy_sha256": policy.fingerprint,
            "schema": ARCHIVE_SEGMENT_SCHEMA,
            "segment_ordinal": int(head["segment_count"]) + 1,
        }
        segment_id = hashlib.sha256(_canonical(core)).hexdigest()
        body_mapping = {**core, "records": records, "segment_id": segment_id}
        body = _canonical(body_mapping)
        if len(body) > self._limits.max_segment_bytes:
            raise AcquisitionEvidenceCapacityError("archive segment byte cap exceeded")
        file_sha = hashlib.sha256(body).hexdigest()
        filename = f"{segment_id}.sealed.json"
        manifest = {
            **core,
            "file_bytes": len(body),
            "file_sha256": file_sha,
            "filename": filename,
            "manifest_schema": ARCHIVE_MANIFEST_SCHEMA,
            "segment_id": segment_id,
        }
        return segment_id, file_sha, body, _canonical(manifest), manifest

    def _publish_segment_file(
        self, segment_id: str, body: bytes,
    ) -> tuple[Path, bool]:
        target = self.archive_segment_directory / f"{segment_id}.sealed.json"
        if target.exists():
            if target.is_symlink() or not target.is_file() or target.read_bytes() != body:
                raise AcquisitionEvidenceIntegrityError(
                    "published orphan segment differs from retry"
                )
            return target, True
        total, _, segments = self._archive_usage()
        if segments + 1 > self._limits.max_archive_segments:
            raise AcquisitionEvidenceCapacityError("archive segment capacity exceeded")
        if total + len(body) > self._limits.max_archive_bytes:
            raise AcquisitionEvidenceCapacityError("archive byte capacity exceeded")
        temporary = self.archive_segment_directory / f".{segment_id}.tmp"
        if temporary.exists():
            raise AcquisitionEvidenceConflict(
                "concurrent archive segment publication is in progress"
            )
        owns_temporary = False
        try:
            output = open(temporary, "xb")
            owns_temporary = True
            with output:
                output.write(body)
                output.flush()
                self._fault("archive.after_segment_write")
                os.fsync(output.fileno())
                self._fault("archive.after_segment_fsync")
            self._directory_fsync(self.archive_segment_directory)
            os.replace(temporary, target)
            self._fault("archive.after_segment_replace")
            self._directory_fsync(self.archive_segment_directory)
            if target.read_bytes() != body:
                raise AcquisitionEvidenceIntegrityError(
                    "archive segment changed during publication"
                )
        except FileExistsError as exc:
            raise AcquisitionEvidenceConflict(
                "concurrent archive segment publication won staging CAS"
            ) from exc
        except OSError as exc:
            if owns_temporary:
                temporary.unlink(missing_ok=True)
            if exc.errno == errno.ENOSPC:
                raise AcquisitionEvidenceCapacityError(
                    "archive segment filesystem is full"
                ) from exc
            raise AcquisitionEvidenceDurabilityError(
                "archive segment publication failed"
            ) from exc
        return target, False

    def rotate_archive(
        self,
        *,
        policy: EvidenceRetentionPolicy,
        now: float | None = None,
    ) -> ArchiveRotationResult:
        """Seal one bounded terminal batch, then resume its prune plan."""

        if not isinstance(policy, EvidenceRetentionPolicy):
            raise TypeError("policy must be EvidenceRetentionPolicy")
        maximum = min(
            policy.max_segment_completions,
            self._limits.max_segment_completions,
        )
        wall_now = _finite(self._clock() if now is None else now, "archive clock")
        self.resume_pending_prunes()
        with self._lock, closing(self._connect()) as db:
            head = db.execute(
                "SELECT * FROM lifecycle_heads WHERE singleton=1"
            ).fetchone()
            if head is None:
                raise AcquisitionEvidenceIntegrityError("lifecycle head is missing")
            if int(head["segment_count"]):
                if (
                    str(head["archive_namespace"]) != policy.archive_namespace
                    or str(head["retention_policy_sha256"]) != policy.fingerprint
                ):
                    raise AcquisitionEvidenceConflict("archive policy drift")
            ordered = db.execute(
                "SELECT producer.producer_receipt_sha256,outbox.state,"
                "outbox.terminal_at FROM producer_receipts AS producer "
                "JOIN completion_outbox AS outbox USING(producer_receipt_sha256) "
                "LEFT JOIN archive_index AS archived USING(producer_receipt_sha256) "
                "WHERE archived.producer_receipt_sha256 IS NULL "
                "ORDER BY producer.global_sequence LIMIT ?",
                (maximum + 1,),
            ).fetchall()
            # Preserve one global, gap-free archive sequence. A later terminal
            # completion may not jump over an older ready/leased one.
            candidates = []
            cutoff = wall_now - policy.min_terminal_age_seconds
            for item in ordered[:maximum]:
                if (
                    str(item["state"]) not in {"consumed", "superseded"}
                    or item["terminal_at"] is None
                    or float(item["terminal_at"]) > cutoff
                ):
                    break
                candidates.append(item)
            captured_anchor = self._archive_anchor(head)
        if not candidates:
            with closing(self._connect()) as db:
                pending = int(db.execute(
                    "SELECT count(*) FROM prune_plans WHERE completed_at IS NULL"
                ).fetchone()[0])
                hot = int(db.execute(
                    "SELECT count(*) FROM producer_receipts"
                ).fetchone()[0])
                head = db.execute(
                    "SELECT * FROM lifecycle_heads WHERE singleton=1"
                ).fetchone()
            return ArchiveRotationResult(
                status="idle",
                segment_id=None,
                archived_completions=0,
                pending_prune_plans=pending,
                hot_completions=hot,
                archive_anchor_sha256=self._archive_anchor(head),
            )

        records: list[dict[str, Any]] = []
        payloads: dict[str, bytes] = {}
        event_ids: list[str] = []
        producer_ids: list[str] = []
        object_ids: list[str] = []
        with self._lock, closing(self._connect()) as db:
            for candidate in candidates:
                producer = db.execute(
                    "SELECT * FROM producer_receipts WHERE producer_receipt_sha256=?",
                    (str(candidate[0]),),
                ).fetchone()
                outbox = db.execute(
                    "SELECT * FROM completion_outbox WHERE producer_receipt_sha256=?",
                    (str(candidate[0]),),
                ).fetchone()
                if producer is None or outbox is None:
                    raise AcquisitionEvidenceIntegrityError(
                        "archive candidate changed during snapshot"
                    )
                cursor = db.execute(
                    "SELECT last_sequence FROM consumer_cursors WHERE consumer_id=? "
                    "AND stream_id=?",
                    (str(outbox["consumer_id"]), str(outbox["stream_id"])),
                ).fetchone()
                if cursor is None or int(cursor[0]) < int(outbox["stream_sequence"]):
                    raise AcquisitionEvidenceConflict(
                        "archive candidate is not cursor-past"
                    )
                consumption = db.execute(
                    "SELECT * FROM consumption_receipts WHERE event_id=?",
                    (str(outbox["event_id"]),),
                ).fetchone()
                rejection = db.execute(
                    "SELECT * FROM rejection_receipts WHERE event_id=?",
                    (str(outbox["event_id"]),),
                ).fetchone()
                quarantine = db.execute(
                    "SELECT * FROM quarantine_receipts WHERE event_id=?",
                    (str(outbox["event_id"]),),
                ).fetchone()
                record, payload = self._archive_record(
                    producer, outbox, consumption, rejection, quarantine
                )
                records.append(record)
                content_sha = str(producer["content_sha256"])
                if content_sha in payloads and payloads[content_sha] != payload:
                    raise AcquisitionEvidenceIntegrityError(
                        "same content identity has different bytes"
                    )
                payloads[content_sha] = payload
                event_ids.append(str(outbox["event_id"]))
                producer_ids.append(str(producer["producer_receipt_sha256"]))
                object_ids.append(content_sha)

        segment_id, file_sha, body, manifest_bytes, manifest = self._build_segment(
            head=head, policy=policy, records=records
        )
        archive_bytes, archive_object_count, archive_segment_count = (
            self._archive_usage()
        )
        with self._lock, closing(self._connect()) as archive_catalog_db:
            for content_sha, payload in payloads.items():
                known = archive_catalog_db.execute(
                    "SELECT * FROM archive_objects WHERE content_sha256=?",
                    (content_sha,),
                ).fetchone()
                if known is None:
                    continue
                path = self.archive_object_directory / f"{content_sha}.raw"
                if (
                    int(known["size_bytes"]) != len(payload)
                    or str(known["filename"]) != f"{content_sha}.raw"
                    or not path.exists()
                ):
                    raise AcquisitionEvidenceIntegrityError(
                        "known archive object is unavailable or differs"
                    )
                self._verified_file(path, content_sha, len(payload))
        new_payloads = {
            content_sha: payload
            for content_sha, payload in payloads.items()
            if not (self.archive_object_directory / f"{content_sha}.raw").exists()
        }
        segment_exists = (
            self.archive_segment_directory / f"{segment_id}.sealed.json"
        ).exists()
        if (
            archive_object_count + len(new_payloads)
            > self._limits.max_archive_objects
            or archive_segment_count + int(not segment_exists)
            > self._limits.max_archive_segments
            or archive_bytes
            + sum(len(payload) for payload in new_payloads.values())
            + (0 if segment_exists else len(body))
            > self._limits.max_archive_bytes
        ):
            raise AcquisitionEvidenceCapacityError(
                "complete archive rotation exceeds hard filesystem capacity"
            )
        for content_sha, payload in payloads.items():
            self._write_exact_object(
                self.archive_object_directory,
                payload,
                content_sha,
                archive=True,
            )
            self._fault("archive.after_object")
        self._publish_segment_file(segment_id, body)
        self._fault("archive.after_segment_publish")

        with self._lock, closing(self._connect()) as db:
            try:
                db.execute("BEGIN IMMEDIATE")
                durable_head = db.execute(
                    "SELECT * FROM lifecycle_heads WHERE singleton=1"
                ).fetchone()
                if durable_head is None:
                    raise AcquisitionEvidenceIntegrityError("lifecycle head disappeared")
                existing_segment = db.execute(
                    "SELECT * FROM archive_segments WHERE segment_id=?",
                    (segment_id,),
                ).fetchone()
                if existing_segment is not None:
                    if str(existing_segment["file_sha256"]) != file_sha:
                        raise AcquisitionEvidenceConflict("archive segment identity conflicts")
                    db.commit()
                else:
                    if self._archive_anchor(durable_head) != captured_anchor:
                        raise AcquisitionEvidenceConflict("archive anchor CAS failed")
                    if int(durable_head["segment_count"]) + 1 > self._limits.max_archive_segments:
                        raise AcquisitionEvidenceCapacityError(
                            "archive segment capacity exceeded"
                        )
                    if (
                        int(durable_head["archived_completion_count"]) + len(records)
                        > self._limits.max_archived_completions
                    ):
                        raise AcquisitionEvidenceCapacityError(
                            "archive completion capacity exceeded"
                        )
                    new_object_bytes = 0
                    for content_sha, payload in payloads.items():
                        known = db.execute(
                            "SELECT size_bytes FROM archive_objects WHERE content_sha256=?",
                            (content_sha,),
                        ).fetchone()
                        if known is None:
                            new_object_bytes += len(payload)
                        elif int(known[0]) != len(payload):
                            raise AcquisitionEvidenceConflict(
                                "archive object size identity conflicts"
                            )
                    if (
                        int(durable_head["archive_bytes"])
                        + len(body)
                        + new_object_bytes
                        > self._limits.max_archive_bytes
                    ):
                        raise AcquisitionEvidenceCapacityError(
                            "archive manifest byte capacity exceeded"
                        )
                    created_at = wall_now
                    for content_sha, payload in payloads.items():
                        db.execute(
                            "INSERT OR IGNORE INTO archive_objects VALUES(?,?,?,?)",
                            (content_sha, len(payload), f"{content_sha}.raw", created_at),
                        )
                        object_row = db.execute(
                            "SELECT * FROM archive_objects WHERE content_sha256=?",
                            (content_sha,),
                        ).fetchone()
                        if (
                            object_row is None
                            or int(object_row["size_bytes"]) != len(payload)
                            or str(object_row["filename"]) != f"{content_sha}.raw"
                        ):
                            raise AcquisitionEvidenceConflict(
                                "archive object catalog conflicts"
                            )
                    db.execute(
                        "INSERT INTO archive_segments VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (
                            segment_id, int(manifest["segment_ordinal"]),
                            manifest["previous_segment_id"],
                            manifest["previous_file_sha256"],
                            int(manifest["first_global_sequence"]),
                            int(manifest["last_global_sequence"]), len(records),
                            str(manifest["content_sha256"]), file_sha,
                            str(manifest["filename"]), len(body),
                            sqlite3.Binary(manifest_bytes), created_at,
                        ),
                    )
                    for record in records:
                        producer = record["producer_receipt"]
                        event = record["completion_event"]
                        db.execute(
                            "INSERT INTO archive_index VALUES(?,?,?,?,?,?,?,?,?,?)",
                            (
                                producer["producer_receipt_sha256"],
                                producer["acquisition_job_id"], event["event_id"],
                                event["event_sha256"], producer["consumer_id"],
                                producer["stream_id"], producer["stream_sequence"],
                                producer["global_sequence"], producer["content_sha256"],
                                segment_id,
                            ),
                        )
                    db.execute(
                        "INSERT INTO prune_plans VALUES(?,?,?,?,?,NULL)",
                        (
                            segment_id, 0, sqlite3.Binary(_canonical(event_ids)),
                            sqlite3.Binary(_canonical(producer_ids)),
                            sqlite3.Binary(_canonical(sorted(set(object_ids)))),
                        ),
                    )
                    db.execute(
                        "UPDATE lifecycle_heads SET archive_namespace=?,"
                        "retention_policy_sha256=?,last_segment_id=?,"
                        "last_segment_file_sha256=?,segment_count=segment_count+1,"
                        "archived_completion_count=archived_completion_count+?,"
                        "archive_bytes=archive_bytes+? WHERE singleton=1",
                        (
                            policy.archive_namespace, policy.fingerprint,
                            segment_id, file_sha, len(records),
                            len(body) + new_object_bytes,
                        ),
                    )
                    self._fault("archive.before_manifest_commit")
                    db.commit()
                    self._fault("archive.after_manifest_commit")
            except BaseException:
                if db.in_transaction:
                    db.rollback()
                raise
        self._directory_fsync(self.root)
        self.resume_pending_prunes()
        with closing(self._connect()) as db:
            pending = int(db.execute(
                "SELECT count(*) FROM prune_plans WHERE completed_at IS NULL"
            ).fetchone()[0])
            hot = int(db.execute(
                "SELECT count(*) FROM producer_receipts"
            ).fetchone()[0])
            durable_head = db.execute(
                "SELECT * FROM lifecycle_heads WHERE singleton=1"
            ).fetchone()
        return ArchiveRotationResult(
            status="archived",
            segment_id=segment_id,
            archived_completions=len(records),
            pending_prune_plans=pending,
            hot_completions=hot,
            archive_anchor_sha256=self._archive_anchor(durable_head),
        )

    @staticmethod
    def _decode_id_list(blob: object, field: str) -> tuple[str, ...]:
        try:
            decoded = json.loads(bytes(blob))
        except (TypeError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise AcquisitionEvidenceIntegrityError(f"{field} is corrupt") from exc
        if not isinstance(decoded, list) or len(decoded) > 4096:
            raise AcquisitionEvidenceIntegrityError(f"{field} exceeds bound")
        result = tuple(_identifier(item, field) for item in decoded)
        if len(set(result)) != len(result):
            raise AcquisitionEvidenceIntegrityError(f"{field} contains duplicates")
        return result

    def _segment_bundle(self, row: sqlite3.Row) -> dict[str, Any]:
        filename = str(row["filename"])
        if filename != f"{row['segment_id']}.sealed.json":
            raise AcquisitionEvidenceIntegrityError("archive segment filename differs")
        path = self.archive_segment_directory / filename
        body = self._verified_file(
            path, str(row["file_sha256"]), int(row["file_bytes"])
        )
        try:
            decoded = json.loads(body)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise AcquisitionEvidenceIntegrityError("archive segment JSON is corrupt") from exc
        if not isinstance(decoded, dict) or decoded.get("schema") != ARCHIVE_SEGMENT_SCHEMA:
            raise AcquisitionEvidenceIntegrityError("archive segment schema differs")
        records = decoded.get("records")
        if (
            not isinstance(records, list)
            or len(records) != int(row["completion_count"])
            or len(records) > self._limits.max_segment_completions
            or _digest(records) != str(row["content_sha256"])
        ):
            raise AcquisitionEvidenceIntegrityError("archive segment content differs")
        core = dict(decoded)
        core.pop("records", None)
        segment_id = core.pop("segment_id", None)
        if segment_id != str(row["segment_id"]) or hashlib.sha256(
            _canonical(core)
        ).hexdigest() != segment_id:
            raise AcquisitionEvidenceIntegrityError("archive segment ID differs")
        manifest = {
            **core,
            "file_bytes": len(body),
            "file_sha256": str(row["file_sha256"]),
            "filename": filename,
            "manifest_schema": ARCHIVE_MANIFEST_SCHEMA,
            "segment_id": segment_id,
        }
        if bytes(row["manifest_json"]) != _canonical(manifest):
            raise AcquisitionEvidenceIntegrityError("archive manifest differs")
        return decoded

    def _verify_prune_plan(self, db: sqlite3.Connection, plan: sqlite3.Row) -> None:
        segment = db.execute(
            "SELECT * FROM archive_segments WHERE segment_id=?",
            (str(plan["segment_id"]),),
        ).fetchone()
        if segment is None:
            raise AcquisitionEvidenceIntegrityError("prune plan lost archive segment")
        bundle = self._segment_bundle(segment)
        event_ids = self._decode_id_list(plan["event_ids_json"], "prune event IDs")
        producer_ids = self._decode_id_list(
            plan["producer_ids_json"], "prune producer IDs"
        )
        records = bundle["records"]
        if (
            tuple(item["completion_event"]["event_id"] for item in records)
            != event_ids
            or tuple(
                item["producer_receipt"]["producer_receipt_sha256"]
                for item in records
            )
            != producer_ids
        ):
            raise AcquisitionEvidenceIntegrityError("prune plan differs from archive")
        for record in records:
            raw = record["raw_object"]
            sha = _sha(raw.get("content_sha256"), "archived content SHA-256")
            size = _count(raw.get("size_bytes"), "archived object size")
            object_row = db.execute(
                "SELECT * FROM archive_objects WHERE content_sha256=?", (sha,)
            ).fetchone()
            if object_row is None or int(object_row["size_bytes"]) != size:
                raise AcquisitionEvidenceIntegrityError("archived object index differs")
            self._verified_file(
                self.archive_object_directory / f"{sha}.raw", sha, size
            )

    def resume_pending_prunes(self) -> int:
        """Resume archive-proven child-first plans after any crash boundary."""

        completed = 0
        while True:
            with self._lock, closing(self._connect()) as db:
                plan = db.execute(
                    "SELECT * FROM prune_plans WHERE completed_at IS NULL "
                    "ORDER BY rowid LIMIT 1"
                ).fetchone()
            if plan is None:
                return completed
            segment_id = str(plan["segment_id"])
            event_ids = self._decode_id_list(plan["event_ids_json"], "prune event IDs")
            producer_ids = self._decode_id_list(
                plan["producer_ids_json"], "prune producer IDs"
            )
            object_ids = self._decode_id_list(plan["object_ids_json"], "prune object IDs")
            phase = int(plan["phase"])
            with self._lock, closing(self._connect()) as db:
                try:
                    db.execute("BEGIN IMMEDIATE")
                    current = db.execute(
                        "SELECT * FROM prune_plans WHERE segment_id=?", (segment_id,)
                    ).fetchone()
                    if current is None or current["completed_at"] is not None:
                        db.commit()
                        continue
                    if int(current["phase"]) != phase:
                        db.commit()
                        continue
                    self._verify_prune_plan(db, current)
                    placeholders = ",".join("?" for _ in event_ids)
                    if phase == 0:
                        db.execute(
                            "DELETE FROM quarantine_receipts WHERE event_id "
                            f"IN ({placeholders})",
                            event_ids,
                        )
                        db.execute(
                            f"DELETE FROM rejection_receipts WHERE event_id IN ({placeholders})",
                            event_ids,
                        )
                        db.execute(
                            f"DELETE FROM consumption_receipts WHERE event_id IN ({placeholders})",
                            event_ids,
                        )
                        db.execute(
                            "UPDATE prune_plans SET phase=1 WHERE segment_id=? AND phase=0",
                            (segment_id,),
                        )
                    elif phase == 1:
                        remaining_quarantines = int(db.execute(
                            "SELECT count(*) FROM quarantine_receipts "
                            f"WHERE event_id IN ({placeholders})",
                            event_ids,
                        ).fetchone()[0])
                        remaining_rejections = int(db.execute(
                            f"SELECT count(*) FROM rejection_receipts WHERE event_id IN ({placeholders})",
                            event_ids,
                        ).fetchone()[0])
                        remaining_consumptions = int(db.execute(
                            f"SELECT count(*) FROM consumption_receipts WHERE event_id IN ({placeholders})",
                            event_ids,
                        ).fetchone()[0])
                        if (
                            remaining_quarantines
                            or remaining_rejections
                            or remaining_consumptions
                        ):
                            raise AcquisitionEvidenceIntegrityError(
                                "prune child receipts remain"
                            )
                        db.execute(
                            f"DELETE FROM completion_outbox WHERE event_id IN ({placeholders})",
                            event_ids,
                        )
                        db.execute(
                            "UPDATE prune_plans SET phase=2 WHERE segment_id=? AND phase=1",
                            (segment_id,),
                        )
                    elif phase == 2:
                        producer_placeholders = ",".join("?" for _ in producer_ids)
                        remaining = int(db.execute(
                            f"SELECT count(*) FROM completion_outbox WHERE producer_receipt_sha256 IN ({producer_placeholders})",
                            producer_ids,
                        ).fetchone()[0])
                        if remaining:
                            raise AcquisitionEvidenceIntegrityError(
                                "prune outbox children remain"
                            )
                        db.execute(
                            f"DELETE FROM producer_receipts WHERE producer_receipt_sha256 IN ({producer_placeholders})",
                            producer_ids,
                        )
                        db.execute(
                            "UPDATE prune_plans SET phase=3 WHERE segment_id=? AND phase=2",
                            (segment_id,),
                        )
                    elif phase == 3:
                        for content_sha in object_ids:
                            referenced = int(db.execute(
                                "SELECT count(*) FROM producer_receipts WHERE content_sha256=?",
                                (content_sha,),
                            ).fetchone()[0])
                            if not referenced:
                                db.execute(
                                    "UPDATE hot_objects SET state='pruning' "
                                    "WHERE content_sha256=? AND state='active'",
                                    (content_sha,),
                                )
                        db.execute(
                            "UPDATE prune_plans SET phase=4 WHERE segment_id=? AND phase=3",
                            (segment_id,),
                        )
                    elif phase == 4:
                        # Files are removed outside this transaction after the
                        # durable `pruning` intent becomes visible.
                        db.commit()
                        for content_sha in object_ids:
                            with closing(self._connect()) as inspect_db:
                                object_row = inspect_db.execute(
                                    "SELECT * FROM hot_objects WHERE content_sha256=?",
                                    (content_sha,),
                                ).fetchone()
                            if object_row is None or str(object_row["state"]) != "pruning":
                                continue
                            path = self.hot_directory / f"{content_sha}.raw"
                            if path.exists():
                                self._verified_file(
                                    path, content_sha, int(object_row["size_bytes"])
                                )
                                path.unlink()
                                self._fault("prune.after_hot_object_unlink")
                                self._directory_fsync(self.hot_directory)
                        with closing(self._connect()) as finish_db:
                            finish_db.execute("BEGIN IMMEDIATE")
                            finish_db.execute(
                                "UPDATE prune_plans SET phase=5 WHERE segment_id=? AND phase=4",
                                (segment_id,),
                            )
                            self._fault("prune.before_file_phase_commit")
                            finish_db.commit()
                            self._fault("prune.after_file_phase_commit")
                        continue
                    elif phase == 5:
                        for content_sha in object_ids:
                            row = db.execute(
                                "SELECT * FROM hot_objects WHERE content_sha256=?",
                                (content_sha,),
                            ).fetchone()
                            if row is None:
                                continue
                            if str(row["state"]) == "pruning":
                                path = self.hot_directory / f"{content_sha}.raw"
                                if path.exists():
                                    raise AcquisitionEvidenceIntegrityError(
                                        "pruned object file unexpectedly remains"
                                    )
                                db.execute(
                                    "DELETE FROM hot_objects WHERE content_sha256=? "
                                    "AND state='pruning'",
                                    (content_sha,),
                                )
                        db.execute(
                            "UPDATE prune_plans SET phase=6,completed_at=? "
                            "WHERE segment_id=? AND phase=5",
                            (_finite(self._clock(), "prune completion clock"), segment_id),
                        )
                    else:
                        raise AcquisitionEvidenceIntegrityError("invalid prune phase")
                    self._fault(f"prune.before_phase_{phase}_commit")
                    db.commit()
                    self._fault(f"prune.after_phase_{phase}_commit")
                    if phase == 5:
                        completed += 1
                except BaseException:
                    if db.in_transaction:
                        db.rollback()
                    raise

    @staticmethod
    def _validate_archived_record(record: object) -> dict[str, Any]:
        if not isinstance(record, dict):
            raise AcquisitionEvidenceIntegrityError(
                "archived completion schema differs"
            )
        schema = record.get("schema")
        legacy_keys = {
            "completion_event",
            "consumption_receipt",
            "producer_receipt",
            "proof",
            "raw_object",
            "schema",
        }
        version_two_keys = legacy_keys | {"rejection_receipt"}
        current_keys = version_two_keys | {
            "quarantine_receipt",
            "terminal_kind",
        }
        if (
            schema == LEGACY_ARCHIVED_COMPLETION_SCHEMA
            and set(record) == legacy_keys
        ):
            rejection_value: object = None
            quarantine_value: object = None
            terminal_kind: object = None
        elif (
            schema == LEGACY_ARCHIVED_COMPLETION_SCHEMA_V2
            and set(record) == version_two_keys
        ):
            rejection_value = record["rejection_receipt"]
            quarantine_value = None
            terminal_kind = None
        elif schema == ARCHIVED_COMPLETION_SCHEMA and set(record) == current_keys:
            rejection_value = record["rejection_receipt"]
            quarantine_value = record["quarantine_receipt"]
            terminal_kind = record["terminal_kind"]
        else:
            raise AcquisitionEvidenceIntegrityError(
                "archived completion contract differs"
            )
        producer = record["producer_receipt"]
        event = record["completion_event"]
        raw = record["raw_object"]
        if not isinstance(producer, dict) or producer.get("schema") != PRODUCER_RECEIPT_SCHEMA:
            raise AcquisitionEvidenceIntegrityError("archived producer schema differs")
        if not isinstance(event, dict) or not isinstance(raw, dict):
            raise AcquisitionEvidenceIntegrityError("archived event metadata differs")
        proof_bytes = _canonical(record["proof"])
        if hashlib.sha256(proof_bytes).hexdigest() != producer.get("proof_sha256"):
            raise AcquisitionEvidenceIntegrityError("archived proof checksum differs")
        _assert_secret_free(record)
        core = AcquisitionEvidenceLifecycleStore._producer_core(
            acquisition_job_id=_identifier(
                producer.get("acquisition_job_id"), "archived acquisition job ID"
            ),
            consumer_id=_identifier(
                producer.get("consumer_id"), "archived consumer ID"
            ),
            stream_id=_identifier(producer.get("stream_id"), "archived stream ID"),
            desired_state_sha256=_sha(
                producer.get("desired_state_sha256"),
                "archived desired state SHA-256",
            ),
            authority_sha256=_sha(
                producer.get("authority_sha256"), "archived authority SHA-256"
            ),
            proof_sha256=_sha(
                producer.get("proof_sha256"), "archived proof SHA-256"
            ),
            content_sha256=_sha(
                producer.get("content_sha256"), "archived content SHA-256"
            ),
            size_bytes=_count(producer.get("size_bytes"), "archived size"),
        )
        receipt_body = {
            **core,
            "created_at": _finite(
                producer.get("created_at"), "archived producer timestamp"
            ),
            "global_sequence": _count(
                producer.get("global_sequence"), "archived global sequence"
            ),
            "stream_sequence": _count(
                producer.get("stream_sequence"), "archived stream sequence"
            ),
        }
        producer_sha = _digest(receipt_body)
        if producer_sha != producer.get("producer_receipt_sha256"):
            raise AcquisitionEvidenceIntegrityError(
                "archived producer receipt checksum differs"
            )
        event_body = {
            "consumer_id": producer["consumer_id"],
            "producer_receipt_sha256": producer_sha,
            "schema": COMPLETION_EVENT_SCHEMA,
            "stream_id": producer["stream_id"],
            "stream_sequence": producer["stream_sequence"],
        }
        event_sha = _digest(event_body)
        expected_event_id = "completion-" + hashlib.sha256(
            (COMPLETION_EVENT_SCHEMA + producer_sha).encode("ascii")
        ).hexdigest()
        if (
            event.get("event_sha256") != event_sha
            or event.get("event_id") != expected_event_id
            or event.get("consumer_id") != producer["consumer_id"]
            or event.get("stream_id") != producer["stream_id"]
            or event.get("stream_sequence") != producer["stream_sequence"]
            or producer.get("event_id") != expected_event_id
            or producer.get("event_sha256") != event_sha
        ):
            raise AcquisitionEvidenceIntegrityError(
                "archived event and producer receipt differ"
            )
        state = event.get("state")
        if state == "consumed":
            consumption = record["consumption_receipt"]
            if not isinstance(consumption, dict):
                raise AcquisitionEvidenceIntegrityError(
                    "archived consumed event lacks receipt"
                )
            consumption_body = dict(consumption)
            receipt_sha = consumption_body.pop("receipt_sha256", None)
            if (
                consumption_body.get("schema") != CONSUMPTION_RECEIPT_SCHEMA
                or _digest(consumption_body) != receipt_sha
                or consumption_body.get("event_id") != expected_event_id
                or consumption_body.get("event_sha256") != event_sha
                or consumption_body.get("producer_receipt_sha256") != producer_sha
                or consumption_body.get("consumer_id") != producer["consumer_id"]
                or consumption_body.get("stream_id") != producer["stream_id"]
                or consumption_body.get("stream_sequence")
                != producer["stream_sequence"]
            ):
                raise AcquisitionEvidenceIntegrityError(
                    "archived consumption receipt differs"
                )
            if rejection_value is not None and quarantine_value is not None:
                raise AcquisitionEvidenceIntegrityError(
                    "archived consumed event has conflicting terminal children"
                )
            if rejection_value is not None:
                if not isinstance(rejection_value, dict) or set(rejection_value) != {
                    "active_desired_state_sha256",
                    "consumer_id",
                    "created_at",
                    "event_id",
                    "event_sha256",
                    "producer_receipt_sha256",
                    "reason_code",
                    "rejected_desired_state_sha256",
                    "rejection_receipt_sha256",
                    "schema",
                    "stream_id",
                    "stream_sequence",
                }:
                    raise AcquisitionEvidenceIntegrityError(
                        "archived rejection receipt schema differs"
                    )
                if rejection_value.get("schema") != REJECTION_RECEIPT_SCHEMA:
                    raise AcquisitionEvidenceIntegrityError(
                        "archived rejection receipt contract differs"
                    )
                rejection = (
                    AcquisitionEvidenceLifecycleStore._rejection_from_mapping(
                        rejection_value
                    )
                )
                if (
                    rejection.event_id != expected_event_id
                    or rejection.event_sha256 != event_sha
                    or rejection.producer_receipt_sha256 != producer_sha
                    or rejection.consumer_id != producer["consumer_id"]
                    or rejection.stream_id != producer["stream_id"]
                    or rejection.stream_sequence != producer["stream_sequence"]
                    or rejection.rejected_desired_state_sha256
                    != producer["desired_state_sha256"]
                    or consumption_body.get("output_sha256")
                    != rejection.rejection_receipt_sha256
                ):
                    raise AcquisitionEvidenceIntegrityError(
                        "archived rejection receipt differs"
                    )
            if quarantine_value is not None:
                if not isinstance(quarantine_value, dict) or set(
                    quarantine_value
                ) != {
                    "consumer_id",
                    "created_at",
                    "event_id",
                    "event_sha256",
                    "evidence_fingerprint_sha256",
                    "producer_receipt_sha256",
                    "quarantine_receipt_sha256",
                    "reason_code",
                    "schema",
                    "stream_id",
                    "stream_sequence",
                }:
                    raise AcquisitionEvidenceIntegrityError(
                        "archived quarantine receipt schema differs"
                    )
                if quarantine_value.get("schema") != QUARANTINE_RECEIPT_SCHEMA:
                    raise AcquisitionEvidenceIntegrityError(
                        "archived quarantine receipt contract differs"
                    )
                quarantine = (
                    AcquisitionEvidenceLifecycleStore._quarantine_from_mapping(
                        quarantine_value
                    )
                )
                if (
                    quarantine.event_id != expected_event_id
                    or quarantine.event_sha256 != event_sha
                    or quarantine.producer_receipt_sha256 != producer_sha
                    or quarantine.consumer_id != producer["consumer_id"]
                    or quarantine.stream_id != producer["stream_id"]
                    or quarantine.stream_sequence
                    != producer["stream_sequence"]
                    or quarantine.created_at
                    != consumption_body.get("created_at")
                    or quarantine.created_at != event.get("terminal_at")
                    or consumption_body.get("output_sha256")
                    != quarantine.quarantine_receipt_sha256
                ):
                    raise AcquisitionEvidenceIntegrityError(
                        "archived quarantine receipt differs"
                    )
            expected_terminal_kind = (
                "quarantined"
                if quarantine_value is not None
                else "rejected"
                if rejection_value is not None
                else "consumed"
            )
        elif state == "superseded":
            if (
                record["consumption_receipt"] is not None
                or rejection_value is not None
                or quarantine_value is not None
            ):
                raise AcquisitionEvidenceIntegrityError(
                    "archived superseded event has terminal child receipt"
                )
            expected_supersession = _digest({
                "event_id": expected_event_id,
                "event_sha256": event_sha,
                "replacement_event_id": event.get("superseded_by_event_id"),
                "schema": "stv3-worker-acquisition-supersession-v1",
            })
            if expected_supersession != event.get("supersession_sha256"):
                raise AcquisitionEvidenceIntegrityError(
                    "archived supersession receipt differs"
                )
            expected_terminal_kind = "superseded"
        else:
            raise AcquisitionEvidenceIntegrityError(
                "archived event is not terminal"
            )
        if (
            schema == ARCHIVED_COMPLETION_SCHEMA
            and terminal_kind != expected_terminal_kind
        ):
            raise AcquisitionEvidenceIntegrityError(
                "archived terminal kind differs from child receipts"
            )
        if (
            raw.get("content_sha256") != producer["content_sha256"]
            or raw.get("size_bytes") != producer["size_bytes"]
            or raw.get("filename") != f"{producer['content_sha256']}.raw"
        ):
            raise AcquisitionEvidenceIntegrityError(
                "archived raw object metadata differs"
            )
        return record

    def _archived_record_by_event(
        self, event_id: str, *, load_payload: bool = True,
    ) -> tuple[dict[str, Any], bytes | None, str]:
        event = _identifier(event_id, "event ID")
        with self._lock, closing(self._connect()) as db:
            index = db.execute(
                "SELECT * FROM archive_index WHERE event_id=?", (event,)
            ).fetchone()
            if index is None:
                raise CompletionWindowExpired("completion is not in the archive")
            segment = db.execute(
                "SELECT * FROM archive_segments WHERE segment_id=?",
                (str(index["segment_id"]),),
            ).fetchone()
            if segment is None:
                raise AcquisitionEvidenceIntegrityError(
                    "archive index references no segment"
                )
            bundle = self._segment_bundle(segment)
            matches = [
                item
                for item in bundle["records"]
                if item.get("completion_event", {}).get("event_id") == event
            ]
            if len(matches) != 1:
                raise AcquisitionEvidenceIntegrityError(
                    "archive event index is absent or ambiguous"
                )
            record = self._validate_archived_record(matches[0])
            producer = record["producer_receipt"]
            if (
                str(index["producer_receipt_sha256"])
                != producer["producer_receipt_sha256"]
                or str(index["acquisition_job_id"])
                != producer["acquisition_job_id"]
                or str(index["event_sha256"])
                != record["completion_event"]["event_sha256"]
                or str(index["consumer_id"]) != producer["consumer_id"]
                or str(index["stream_id"]) != producer["stream_id"]
                or int(index["stream_sequence"]) != producer["stream_sequence"]
                or int(index["global_sequence"]) != producer["global_sequence"]
                or str(index["content_sha256"]) != producer["content_sha256"]
            ):
                raise AcquisitionEvidenceIntegrityError(
                    "archive lookup index differs from sealed completion"
                )
            object_row = db.execute(
                "SELECT * FROM archive_objects WHERE content_sha256=?",
                (producer["content_sha256"],),
            ).fetchone()
            if (
                object_row is None
                or int(object_row["size_bytes"]) != producer["size_bytes"]
            ):
                raise AcquisitionEvidenceIntegrityError(
                    "archive object index differs from completion"
                )
        payload = None
        if load_payload:
            payload = self._verified_file(
                self.archive_object_directory
                / f"{producer['content_sha256']}.raw",
                producer["content_sha256"],
                producer["size_bytes"],
            )
        return record, payload, str(index["segment_id"])

    def _load_archived_producer(self, event_id: str) -> CompletionProducerReceipt:
        record, _, _ = self._archived_record_by_event(event_id)
        producer = record["producer_receipt"]
        return CompletionProducerReceipt(
            producer_receipt_sha256=producer["producer_receipt_sha256"],
            event_id=producer["event_id"],
            event_sha256=producer["event_sha256"],
            acquisition_job_id=producer["acquisition_job_id"],
            consumer_id=producer["consumer_id"],
            stream_id=producer["stream_id"],
            stream_sequence=producer["stream_sequence"],
            global_sequence=producer["global_sequence"],
            desired_state_sha256=producer["desired_state_sha256"],
            authority_sha256=producer["authority_sha256"],
            proof_sha256=producer["proof_sha256"],
            content_sha256=producer["content_sha256"],
            size_bytes=producer["size_bytes"],
            created_at=producer["created_at"],
            ready_at=producer["ready_at"],
        )

    def _load_archived_consumption(self, event_id: str) -> ConsumptionReceipt:
        record, _, _ = self._archived_record_by_event(event_id)
        value = record["consumption_receipt"]
        if not isinstance(value, dict):
            raise AcquisitionEvidenceConflict(
                "superseded completion has no consumer receipt"
            )
        return ConsumptionReceipt(
            receipt_sha256=value["receipt_sha256"],
            event_id=value["event_id"],
            event_sha256=value["event_sha256"],
            producer_receipt_sha256=value["producer_receipt_sha256"],
            consumer_id=value["consumer_id"],
            stream_id=value["stream_id"],
            stream_sequence=value["stream_sequence"],
            output_sha256=value["output_sha256"],
            created_at=value["created_at"],
        )

    def _load_archived_rejection(self, event_id: str) -> RejectionReceipt:
        record, _, _ = self._archived_record_by_event(event_id)
        value = record.get("rejection_receipt")
        if not isinstance(value, dict):
            raise AcquisitionEvidenceConflict(
                "archived completion has no desired-state rejection receipt"
            )
        return self._rejection_from_mapping(value)

    def _load_archived_quarantine(self, event_id: str) -> QuarantineReceipt:
        record, _, _ = self._archived_record_by_event(event_id)
        value = record.get("quarantine_receipt")
        if not isinstance(value, dict):
            raise AcquisitionEvidenceConflict(
                "archived completion has no permanent quarantine receipt"
            )
        return self._quarantine_from_mapping(value)

    @staticmethod
    def _reclaimed_member_by_sequence(
        db: sqlite3.Connection,
        stream_id: str,
        stream_sequence: int,
    ) -> sqlite3.Row | None:
        rows = db.execute(
            "SELECT member.* FROM lifecycle_reclamation_members AS member "
            "JOIN lifecycle_reclamation_decisions AS decision "
            "USING(request_sha256) WHERE member.stream_id=? AND "
            "member.stream_sequence=? AND decision.state IN "
            "('APPLYING','APPLIED') ORDER BY decision.fence_epoch DESC LIMIT 2",
            (stream_id, stream_sequence),
        ).fetchall()
        if len(rows) > 1:
            raise AcquisitionEvidenceIntegrityError(
                "completion belongs to multiple positive reclamations"
            )
        return rows[0] if rows else None

    def lookup_sequence(
        self,
        stream_id: str,
        stream_sequence: int,
        *,
        include_archive: bool = True,
    ) -> CompletionLookup:
        """Return ``hot``, ``archived``, ``window_expired`` or ``unknown``."""

        stream = _identifier(stream_id, "stream ID")
        sequence = _count(stream_sequence, "stream sequence")
        if sequence < 1:
            raise AcquisitionEvidenceLifecycleError("stream sequence must be positive")
        with self._lock, closing(self._connect()) as db:
            producer = db.execute(
                "SELECT * FROM producer_receipts WHERE stream_id=? AND stream_sequence=?",
                (stream, sequence),
            ).fetchone()
            if producer is not None:
                outbox = db.execute(
                    "SELECT * FROM completion_outbox WHERE producer_receipt_sha256=?",
                    (str(producer["producer_receipt_sha256"]),),
                ).fetchone()
                if outbox is None:
                    raise AcquisitionEvidenceIntegrityError(
                        "hot producer has no completion event"
                    )
                receipt = self._producer_from_rows(producer, outbox)
                proof = json.loads(bytes(producer["proof_json"]))
                content_sha = receipt.content_sha256
                size = receipt.size_bytes
                event = receipt.event_id
                producer_sha = receipt.producer_receipt_sha256
            else:
                index = db.execute(
                    "SELECT * FROM archive_index WHERE stream_id=? AND stream_sequence=?",
                    (stream, sequence),
                ).fetchone()
                if index is not None:
                    if not include_archive:
                        return CompletionLookup(
                            status="window_expired",
                            stream_id=stream,
                            stream_sequence=sequence,
                            event_id=str(index["event_id"]),
                            producer_receipt_sha256=str(
                                index["producer_receipt_sha256"]
                            ),
                            archive_segment_id=str(index["segment_id"]),
                        )
                    archived_event = str(index["event_id"])
                else:
                    reclaimed = self._reclaimed_member_by_sequence(
                        db, stream, sequence
                    )
                    if reclaimed is not None:
                        return CompletionLookup(
                            status="window_expired",
                            stream_id=stream,
                            stream_sequence=sequence,
                            event_id=str(reclaimed["event_id"]),
                            producer_receipt_sha256=str(
                                reclaimed["producer_receipt_sha256"]
                            ),
                            archive_segment_id=str(
                                reclaimed["lifecycle_archive_segment_id"]
                            ),
                        )
                    head = db.execute(
                        "SELECT last_sequence FROM stream_heads WHERE stream_id=?",
                        (stream,),
                    ).fetchone()
                    if head is None or sequence > int(head[0]):
                        return CompletionLookup(
                            status="unknown",
                            stream_id=stream,
                            stream_sequence=sequence,
                            event_id=None,
                            producer_receipt_sha256=None,
                            archive_segment_id=None,
                        )
                    raise AcquisitionEvidenceIntegrityError(
                        "known completion stream contains an unexplained gap"
                    )
        if producer is not None:
            payload = self._verified_file(
                self.hot_directory / f"{content_sha}.raw", content_sha, size
            )
            return CompletionLookup(
                status="hot",
                stream_id=stream,
                stream_sequence=sequence,
                event_id=event,
                producer_receipt_sha256=producer_sha,
                archive_segment_id=None,
                proof=proof,
                payload=payload,
            )
        record, payload, segment_id = self._archived_record_by_event(archived_event)
        return CompletionLookup(
            status="archived",
            stream_id=stream,
            stream_sequence=sequence,
            event_id=archived_event,
            producer_receipt_sha256=record["producer_receipt"][
                "producer_receipt_sha256"
            ],
            archive_segment_id=segment_id,
            proof=record["proof"],
            payload=payload,
        )

    @staticmethod
    def _assert_terminal_pair(
        producer: CompletionProducerReceipt,
        consumption: ConsumptionReceipt,
    ) -> None:
        if (
            consumption.event_id != producer.event_id
            or consumption.event_sha256 != producer.event_sha256
            or consumption.producer_receipt_sha256
            != producer.producer_receipt_sha256
            or consumption.consumer_id != producer.consumer_id
            or consumption.stream_id != producer.stream_id
            or consumption.stream_sequence != producer.stream_sequence
        ):
            raise AcquisitionEvidenceIntegrityError(
                "terminal consumption differs from its producer receipt"
            )

    def terminal_event_identity(
        self,
        stream_id: str,
        stream_sequence: int,
    ) -> TerminalEventIdentity | None:
        """Return exact metadata for normal, rejected or superseded terminality.

        Ready or genuinely leased work returns ``None``.  A consumption receipt
        committed immediately before a crash is terminal evidence even if its
        outbox/cursor reconciliation has not yet run.  No payload, proof,
        capability token, owner or local path leaves this method.
        """

        stream = _identifier(stream_id, "stream ID")
        sequence = _count(stream_sequence, "stream sequence")
        if sequence < 1:
            raise AcquisitionEvidenceLifecycleError(
                "stream sequence must be positive"
            )
        archived_event: str | None = None
        with self._lock, closing(self._connect()) as db:
            producer_row = db.execute(
                "SELECT * FROM producer_receipts WHERE stream_id=? "
                "AND stream_sequence=?",
                (stream, sequence),
            ).fetchone()
            index = db.execute(
                "SELECT * FROM archive_index WHERE stream_id=? "
                "AND stream_sequence=?",
                (stream, sequence),
            ).fetchone()
            if producer_row is not None and index is not None:
                raise AcquisitionEvidenceIntegrityError(
                    "terminal completion exists hot and archived"
                )
            if producer_row is None:
                if index is not None:
                    archived_event = str(index["event_id"])
                else:
                    reclaimed = self._reclaimed_member_by_sequence(
                        db, stream, sequence
                    )
                    if reclaimed is not None:
                        raise CompletionWindowExpired(
                            "terminal completion was positively reclaimed"
                        )
                    head = db.execute(
                        "SELECT last_sequence FROM stream_heads WHERE stream_id=?",
                        (stream,),
                    ).fetchone()
                    if head is None or sequence > int(head["last_sequence"]):
                        return None
                    raise AcquisitionEvidenceIntegrityError(
                        "known terminal stream contains an unexplained gap"
                    )
            else:
                outbox = db.execute(
                    "SELECT * FROM completion_outbox "
                    "WHERE producer_receipt_sha256=?",
                    (str(producer_row["producer_receipt_sha256"]),),
                ).fetchone()
                if outbox is None:
                    raise AcquisitionEvidenceIntegrityError(
                        "hot producer has no completion event"
                    )
                producer = self._producer_from_rows(producer_row, outbox)
                event_sha = self._event_sha(outbox)
                if producer.event_sha256 != event_sha:
                    raise AcquisitionEvidenceIntegrityError(
                        "producer and terminal event checksums differ"
                    )
                state = str(outbox["state"])
                consumption_row = db.execute(
                    "SELECT * FROM consumption_receipts WHERE event_id=?",
                    (producer.event_id,),
                ).fetchone()
                rejection_row = db.execute(
                    "SELECT * FROM rejection_receipts WHERE event_id=?",
                    (producer.event_id,),
                ).fetchone()
                quarantine_row = db.execute(
                    "SELECT * FROM quarantine_receipts WHERE event_id=?",
                    (producer.event_id,),
                ).fetchone()
                if rejection_row is not None and quarantine_row is not None:
                    raise AcquisitionEvidenceIntegrityError(
                        "terminal completion has conflicting child receipts"
                    )
                if state == "superseded":
                    if any(
                        row is not None
                        for row in (
                            consumption_row, rejection_row, quarantine_row
                        )
                    ):
                        raise AcquisitionEvidenceIntegrityError(
                            "superseded completion has a terminal child receipt"
                        )
                    replacement = outbox["superseded_by_event_id"]
                    supersession = outbox["supersession_sha256"]
                    expected_supersession = _digest({
                        "event_id": producer.event_id,
                        "event_sha256": event_sha,
                        "replacement_event_id": replacement,
                        "schema": "stv3-worker-acquisition-supersession-v1",
                    })
                    if (
                        replacement is None
                        or supersession is None
                        or not hmac.compare_digest(
                            expected_supersession, str(supersession)
                        )
                        or outbox["terminal_at"] is None
                    ):
                        raise AcquisitionEvidenceIntegrityError(
                            "supersession receipt checksum differs"
                        )
                    return TerminalEventIdentity(
                        status="hot",
                        event_id=producer.event_id,
                        event_sha256=event_sha,
                        producer_receipt_sha256=(
                            producer.producer_receipt_sha256
                        ),
                        consumer_id=producer.consumer_id,
                        stream_id=producer.stream_id,
                        stream_sequence=producer.stream_sequence,
                        terminal_kind="superseded",
                        terminal_output_sha256=None,
                        terminal_at=_finite(
                            outbox["terminal_at"], "supersession timestamp"
                        ),
                    )
                if consumption_row is None:
                    if state == "consumed":
                        raise AcquisitionEvidenceIntegrityError(
                            "consumed completion lacks its receipt"
                        )
                    if rejection_row is not None or quarantine_row is not None:
                        raise AcquisitionEvidenceIntegrityError(
                            "terminal child has no consumption receipt"
                        )
                    return None
                if state not in {"ready", "leased", "consumed"}:
                    raise AcquisitionEvidenceIntegrityError(
                        "completion terminal state is invalid"
                    )
                consumption = self._consumption_from_row(consumption_row)
                self._assert_terminal_pair(producer, consumption)
                if state == "consumed" and (
                    outbox["terminal_at"] is None
                    or _finite(outbox["terminal_at"], "terminal timestamp")
                    != consumption.created_at
                ):
                    raise AcquisitionEvidenceIntegrityError(
                        "terminal receipt timestamp differs"
                    )
                if rejection_row is not None:
                    rejection = self._rejection_from_row(rejection_row)
                    if (
                        rejection.event_id != producer.event_id
                        or rejection.event_sha256 != event_sha
                        or rejection.producer_receipt_sha256
                        != producer.producer_receipt_sha256
                        or rejection.consumer_id != producer.consumer_id
                        or rejection.stream_id != producer.stream_id
                        or rejection.stream_sequence
                        != producer.stream_sequence
                        or consumption.output_sha256
                        != rejection.rejection_receipt_sha256
                        or rejection.created_at != consumption.created_at
                    ):
                        raise AcquisitionEvidenceIntegrityError(
                            "rejection terminal identity differs"
                        )
                    terminal_kind = "rejected"
                elif quarantine_row is not None:
                    self._validate_quarantine_child(
                        quarantine_row,
                        outbox=outbox,
                        event_sha256=event_sha,
                        consumption=consumption,
                    )
                    terminal_kind = "quarantined"
                else:
                    terminal_kind = "consumed"
                return TerminalEventIdentity(
                    status="hot",
                    event_id=producer.event_id,
                    event_sha256=event_sha,
                    producer_receipt_sha256=producer.producer_receipt_sha256,
                    consumer_id=producer.consumer_id,
                    stream_id=producer.stream_id,
                    stream_sequence=producer.stream_sequence,
                    terminal_kind=terminal_kind,
                    terminal_output_sha256=consumption.output_sha256,
                    terminal_at=consumption.created_at,
                )
        if archived_event is None:  # pragma: no cover - established above
            raise AssertionError("archive event identity is unavailable")
        record, _, segment_id = self._archived_record_by_event(
            archived_event, load_payload=False
        )
        producer = record["producer_receipt"]
        event = record["completion_event"]
        consumption = record.get("consumption_receipt")
        terminal_kind = record.get("terminal_kind")
        if terminal_kind not in {
            "consumed", "rejected", "quarantined", "superseded"
        }:
            raise AcquisitionEvidenceIntegrityError(
                "archived terminal kind is invalid"
            )
        output_sha = None
        if terminal_kind != "superseded":
            if not isinstance(consumption, dict):
                raise AcquisitionEvidenceIntegrityError(
                    "archived terminal consumption is unavailable"
                )
            output_sha = _sha(
                consumption.get("output_sha256"),
                "archived terminal output SHA-256",
            )
        return TerminalEventIdentity(
            status="archived",
            event_id=_identifier(event.get("event_id"), "archived event ID"),
            event_sha256=_sha(
                event.get("event_sha256"), "archived event SHA-256"
            ),
            producer_receipt_sha256=_sha(
                producer.get("producer_receipt_sha256"),
                "archived producer receipt SHA-256",
            ),
            consumer_id=_identifier(
                producer.get("consumer_id"), "archived consumer ID"
            ),
            stream_id=_identifier(
                producer.get("stream_id"), "archived stream ID"
            ),
            stream_sequence=_count(
                producer.get("stream_sequence"), "archived stream sequence"
            ),
            terminal_kind=terminal_kind,
            terminal_output_sha256=output_sha,
            terminal_at=_finite(
                event.get("terminal_at"), "archived terminal timestamp"
            ),
            archive_segment_id=_identifier(
                segment_id, "archive segment ID"
            ),
        )

    def source_reclamation_candidate_view_sha256(
        self,
        binding: SourceReclamationCandidateBindingV1,
    ) -> str:
        """Return the bounded lifecycle projection sealed by retention v2."""

        if not isinstance(binding, SourceReclamationCandidateBindingV1):
            raise TypeError(
                "binding must be SourceReclamationCandidateBindingV1"
            )
        return self.source_reclamation_terminal_view_sha256(
            candidate_sha256=binding.candidate_sha256,
            acquisition_job_id=binding.acquisition_job_id,
            stream_id=binding.evidence_stream_id,
            stream_sequence=binding.evidence_stream_sequence,
        )

    def source_reclamation_terminal_view_sha256(
        self,
        *,
        candidate_sha256: str,
        acquisition_job_id: str,
        stream_id: str,
        stream_sequence: int,
    ) -> str:
        """Hash the shared, payload-free lifecycle candidate projection."""

        candidate = require_reclamation_sha256(
            candidate_sha256, "lifecycle projection candidate SHA-256"
        )
        acquisition_job = _identifier(
            acquisition_job_id, "lifecycle projection acquisition job ID"
        )
        stream = _identifier(stream_id, "lifecycle projection stream ID")
        sequence = _count(stream_sequence, "lifecycle projection sequence")
        if sequence < 1:
            raise AcquisitionEvidenceLifecycleError(
                "lifecycle projection sequence must be positive"
            )
        identity = self.terminal_event_identity(stream, sequence)
        if identity is None or identity.status != "archived":
            raise AcquisitionEvidenceConflict(
                "lifecycle candidate is not archived terminal evidence"
            )
        with self._lock, closing(self._connect()) as db:
            row = db.execute(
                "SELECT acquisition_job_id,event_id,event_sha256,"
                "producer_receipt_sha256,consumer_id,segment_id FROM "
                "archive_index WHERE stream_id=? AND stream_sequence=?",
                (
                    stream,
                    sequence,
                ),
            ).fetchone()
        if (
            row is None
            or str(row["acquisition_job_id"]) != acquisition_job
            or str(row["event_id"]) != identity.event_id
            or str(row["event_sha256"]) != identity.event_sha256
            or str(row["producer_receipt_sha256"])
            != identity.producer_receipt_sha256
            or str(row["consumer_id"]) != identity.consumer_id
            or str(row["segment_id"]) != identity.archive_segment_id
        ):
            raise AcquisitionEvidenceIntegrityError(
                "lifecycle candidate archive projection differs"
            )
        return build_participant_candidate_view_sha256(
            role=PARTICIPANT_APPLY_ORDER[0],
            candidate_sha256=candidate,
            projection={
                "downstream_terminal": {
                    "acquisition_job_id": acquisition_job,
                    "terminal_identity": identity.as_dict(),
                    "terminal_identity_sha256": identity.identity_sha256,
                },
            },
        )

    def load_terminal_consumption(
        self,
        stream_id: str,
        stream_sequence: int,
    ) -> TerminalConsumptionEvidence | None:
        """Load one checksum-verified normal terminal consumption.

        Rejected/superseded and merely ready/leased events fail closed.  The
        same API follows an archived index into its sealed segment, so callers
        never infer terminality from cursor position or object presence.
        """

        stream = _identifier(stream_id, "stream ID")
        sequence = _count(stream_sequence, "stream sequence")
        if sequence < 1:
            raise AcquisitionEvidenceLifecycleError(
                "stream sequence must be positive"
            )
        archived_event: str | None = None
        with self._lock, closing(self._connect()) as db:
            producer_row = db.execute(
                "SELECT * FROM producer_receipts WHERE stream_id=? "
                "AND stream_sequence=?",
                (stream, sequence),
            ).fetchone()
            if producer_row is not None:
                outbox = db.execute(
                    "SELECT * FROM completion_outbox "
                    "WHERE producer_receipt_sha256=?",
                    (str(producer_row["producer_receipt_sha256"]),),
                ).fetchone()
                if outbox is None:
                    raise AcquisitionEvidenceIntegrityError(
                        "hot producer has no completion event"
                    )
                if str(outbox["state"]) != "consumed":
                    raise AcquisitionEvidenceConflict(
                        "completion has no terminal consumption"
                    )
                consumption_row = db.execute(
                    "SELECT * FROM consumption_receipts WHERE event_id=?",
                    (str(outbox["event_id"]),),
                ).fetchone()
                if consumption_row is None:
                    raise AcquisitionEvidenceIntegrityError(
                        "consumed completion lacks its receipt"
                    )
                rejection = db.execute(
                    "SELECT 1 FROM rejection_receipts WHERE event_id=?",
                    (str(outbox["event_id"]),),
                ).fetchone()
                quarantine = db.execute(
                    "SELECT * FROM quarantine_receipts WHERE event_id=?",
                    (str(outbox["event_id"]),),
                ).fetchone()
                if rejection is not None and quarantine is not None:
                    raise AcquisitionEvidenceIntegrityError(
                        "consumed completion has conflicting terminal children"
                    )
                producer = self._producer_from_rows(producer_row, outbox)
                consumption = self._consumption_from_row(consumption_row)
                self._assert_terminal_pair(producer, consumption)
                if quarantine is not None:
                    self._validate_quarantine_child(
                        quarantine,
                        outbox=outbox,
                        event_sha256=producer.event_sha256,
                        consumption=consumption,
                    )
                    raise AcquisitionEvidenceConflict(
                        "quarantined completion is not normal consumption"
                    )
                if rejection is not None:
                    raise AcquisitionEvidenceConflict(
                        "rejected completion is not normal consumption"
                    )
                try:
                    proof = json.loads(bytes(producer_row["proof_json"]))
                except (TypeError, UnicodeDecodeError, json.JSONDecodeError) as err:
                    raise AcquisitionEvidenceIntegrityError(
                        "producer proof is not canonical JSON"
                    ) from err
                payload = self._verified_file(
                    self.hot_directory / f"{producer.content_sha256}.raw",
                    producer.content_sha256,
                    producer.size_bytes,
                )
                return TerminalConsumptionEvidence(
                    status="hot",
                    producer_receipt=producer,
                    consumption_receipt=consumption,
                    archive_segment_id=None,
                    proof=proof,
                    payload=payload,
                )
            index = db.execute(
                "SELECT * FROM archive_index WHERE stream_id=? "
                "AND stream_sequence=?",
                (stream, sequence),
            ).fetchone()
            if index is not None:
                archived_event = str(index["event_id"])
            else:
                reclaimed = self._reclaimed_member_by_sequence(
                    db, stream, sequence
                )
                if reclaimed is not None:
                    raise CompletionWindowExpired(
                        "terminal consumption was positively reclaimed"
                    )
                head = db.execute(
                    "SELECT last_sequence FROM stream_heads WHERE stream_id=?",
                    (stream,),
                ).fetchone()
                if head is None or sequence > int(head[0]):
                    return None
                raise AcquisitionEvidenceIntegrityError(
                    "known completion stream contains an unexplained gap"
                )
        if archived_event is None:  # pragma: no cover - established above
            raise AssertionError("archive event identity ontbreekt")
        record, payload, segment_id = self._archived_record_by_event(
            archived_event
        )
        producer_value = record["producer_receipt"]
        consumption_value = record.get("consumption_receipt")
        if record.get("quarantine_receipt") is not None:
            raise AcquisitionEvidenceConflict(
                "quarantined completion is not normal consumption"
            )
        if record.get("rejection_receipt") is not None:
            raise AcquisitionEvidenceConflict(
                "rejected completion is not normal consumption"
            )
        if not isinstance(consumption_value, dict):
            raise AcquisitionEvidenceConflict(
                "completion has no terminal consumption"
            )
        producer = CompletionProducerReceipt(
            producer_receipt_sha256=producer_value[
                "producer_receipt_sha256"
            ],
            event_id=producer_value["event_id"],
            event_sha256=producer_value["event_sha256"],
            acquisition_job_id=producer_value["acquisition_job_id"],
            consumer_id=producer_value["consumer_id"],
            stream_id=producer_value["stream_id"],
            stream_sequence=producer_value["stream_sequence"],
            global_sequence=producer_value["global_sequence"],
            desired_state_sha256=producer_value["desired_state_sha256"],
            authority_sha256=producer_value["authority_sha256"],
            proof_sha256=producer_value["proof_sha256"],
            content_sha256=producer_value["content_sha256"],
            size_bytes=producer_value["size_bytes"],
            created_at=producer_value["created_at"],
            ready_at=producer_value["ready_at"],
        )
        consumption = ConsumptionReceipt(
            receipt_sha256=consumption_value["receipt_sha256"],
            event_id=consumption_value["event_id"],
            event_sha256=consumption_value["event_sha256"],
            producer_receipt_sha256=consumption_value[
                "producer_receipt_sha256"
            ],
            consumer_id=consumption_value["consumer_id"],
            stream_id=consumption_value["stream_id"],
            stream_sequence=consumption_value["stream_sequence"],
            output_sha256=consumption_value["output_sha256"],
            created_at=consumption_value["created_at"],
        )
        self._assert_terminal_pair(producer, consumption)
        proof = record.get("proof")
        if not isinstance(proof, dict):
            raise AcquisitionEvidenceIntegrityError(
                "archived producer proof is invalid"
            )
        return TerminalConsumptionEvidence(
            status="archived",
            producer_receipt=producer,
            consumption_receipt=consumption,
            archive_segment_id=segment_id,
            proof=proof,
            payload=payload,
        )

    def audit_archive(self) -> dict[str, Any]:
        """Verify every sealed segment/object, chain edge and lookup index."""

        self.resume_pending_prunes()
        with self._lock, closing(self._connect()) as db:
            head = db.execute(
                "SELECT * FROM lifecycle_heads WHERE singleton=1"
            ).fetchone()
            segments = db.execute(
                "SELECT * FROM archive_segments ORDER BY segment_ordinal"
            ).fetchall()
            indexes = db.execute(
                "SELECT * FROM archive_index ORDER BY global_sequence"
            ).fetchall()
            objects = db.execute(
                "SELECT * FROM archive_objects ORDER BY content_sha256"
            ).fetchall()
            hot_objects = db.execute(
                "SELECT * FROM hot_objects ORDER BY content_sha256"
            ).fetchall()
            hot_producers = db.execute(
                "SELECT * FROM producer_receipts ORDER BY global_sequence"
            ).fetchall()
            floor = db.execute(
                "SELECT * FROM lifecycle_archive_floor WHERE singleton=1"
            ).fetchone()
            if head is None or floor is None:
                raise AcquisitionEvidenceIntegrityError(
                    "archive head or reclamation floor is unavailable"
                )
            applying = int(db.execute(
                "SELECT count(*) FROM lifecycle_reclamation_decisions "
                "WHERE state='APPLYING'"
            ).fetchone()[0])
            if applying:
                raise AcquisitionEvidenceDurabilityError(
                    "lifecycle reclamation requires roll-forward recovery"
                )
            floor_ordinal = int(floor["pruned_through_segment_ordinal"])
            reclaimed_completions = int(floor["reclaimed_completion_count"])
            if (
                len(segments) != int(head["segment_count"])
                or len(indexes)
                != int(head["archived_completion_count"]) - reclaimed_completions
                or not 0 <= floor_ordinal <= len(segments)
            ):
                raise AcquisitionEvidenceIntegrityError(
                    "archive head counts differ"
                )
            expected_ordinal = 1
            expected_global = 1
            previous_id: str | None = None
            previous_sha: str | None = None
            checked_file_bytes = 0
            archived_quarantines = 0
            archived_rejections = 0
            seen_indexes: dict[str, sqlite3.Row] = {
                str(item["event_id"]): item for item in indexes
            }
            for segment in segments:
                if (
                    int(segment["segment_ordinal"]) != expected_ordinal
                    or segment["previous_segment_id"] != previous_id
                    or segment["previous_file_sha256"] != previous_sha
                    or int(segment["first_global_sequence"]) != expected_global
                    or int(segment["last_global_sequence"])
                    != expected_global + int(segment["completion_count"]) - 1
                ):
                    raise AcquisitionEvidenceIntegrityError(
                        "archive segment chain has a gap or fork"
                    )
                if expected_ordinal <= floor_ordinal:
                    segment_path = self.archive_segment_directory / str(
                        segment["filename"]
                    )
                    if segment_path.exists() or segment_path.is_symlink():
                        raise AcquisitionEvidenceIntegrityError(
                            "reclaimed archive segment file remains"
                        )
                    indexed = db.execute(
                        "SELECT 1 FROM archive_index WHERE segment_id=? LIMIT 1",
                        (str(segment["segment_id"]),),
                    ).fetchone()
                    journal = db.execute(
                        "SELECT 1 FROM lifecycle_reclamation_files AS file "
                        "JOIN lifecycle_reclamation_decisions AS decision "
                        "USING(request_sha256) WHERE decision.state='APPLIED' "
                        "AND file.file_kind='segment' AND "
                        "file.identity_sha256=? AND file.content_sha256=? "
                        "AND file.filename=? AND file.size_bytes=? LIMIT 1",
                        (
                            str(segment["segment_id"]),
                            str(segment["file_sha256"]),
                            str(segment["filename"]),
                            int(segment["file_bytes"]),
                        ),
                    ).fetchone()
                    if indexed is not None or journal is None:
                        raise AcquisitionEvidenceIntegrityError(
                            "reclaimed archive segment tombstone differs"
                        )
                else:
                    bundle = self._segment_bundle(segment)
                    for record in bundle["records"]:
                        checked = self._validate_archived_record(record)
                        if checked.get("quarantine_receipt") is not None:
                            archived_quarantines += 1
                        elif checked.get("rejection_receipt") is not None:
                            archived_rejections += 1
                        event_id = checked["completion_event"]["event_id"]
                        if event_id not in seen_indexes:
                            raise AcquisitionEvidenceIntegrityError(
                                "sealed event lacks archive index"
                            )
                        seen_indexes.pop(event_id)
                    checked_file_bytes += int(segment["file_bytes"])
                expected_ordinal += 1
                expected_global = int(segment["last_global_sequence"]) + 1
                previous_id = str(segment["segment_id"])
                previous_sha = str(segment["file_sha256"])
            if seen_indexes:
                raise AcquisitionEvidenceIntegrityError(
                    "archive index contains unsealed events"
                )
            pruned_segments = tuple(
                item for item in segments
                if int(item["segment_ordinal"]) <= floor_ordinal
            )
            floor_segment = pruned_segments[-1] if pruned_segments else None
            applied_member_count = int(db.execute(
                "SELECT count(*) FROM lifecycle_reclamation_members AS member "
                "JOIN lifecycle_reclamation_decisions AS decision "
                "USING(request_sha256) WHERE decision.state='APPLIED'"
            ).fetchone()[0])
            applied_segment_count = int(db.execute(
                "SELECT count(*) FROM lifecycle_reclamation_files AS file "
                "JOIN lifecycle_reclamation_decisions AS decision "
                "USING(request_sha256) WHERE decision.state='APPLIED' "
                "AND file.file_kind='segment'"
            ).fetchone()[0])
            applied_file_bytes = int(db.execute(
                "SELECT coalesce(sum(file.size_bytes),0) FROM "
                "lifecycle_reclamation_files AS file JOIN "
                "lifecycle_reclamation_decisions AS decision "
                "USING(request_sha256) WHERE decision.state='APPLIED'"
            ).fetchone()[0])
            if floor_segment is None:
                floor_edge_differs = any((
                    floor["pruned_through_segment_id"] is not None,
                    floor["pruned_through_file_sha256"] is not None,
                    int(floor["pruned_through_global_sequence"]) != 0,
                    floor["last_apply_receipt_sha256"] is not None,
                ))
            else:
                floor_edge_differs = (
                    floor["pruned_through_segment_id"]
                    != floor_segment["segment_id"]
                    or floor["pruned_through_file_sha256"]
                    != floor_segment["file_sha256"]
                    or int(floor["pruned_through_global_sequence"])
                    != int(floor_segment["last_global_sequence"])
                    or floor["last_apply_receipt_sha256"] is None
                )
            if (
                floor_edge_differs
                or len(pruned_segments)
                != int(floor["reclaimed_segment_count"])
                or sum(int(item["completion_count"]) for item in pruned_segments)
                != reclaimed_completions
                or applied_member_count != reclaimed_completions
                or applied_segment_count
                != int(floor["reclaimed_segment_count"])
                or applied_file_bytes != int(floor["reclaimed_file_bytes"])
            ):
                raise AcquisitionEvidenceIntegrityError(
                    "archive reclamation floor accounting differs"
                )
            if floor_segment is not None:
                latest = db.execute(
                    "SELECT request_sha256,apply_receipt_json FROM "
                    "lifecycle_reclamation_decisions WHERE state='APPLIED' "
                    "ORDER BY target_floor_ordinal DESC LIMIT 1"
                ).fetchone()
                if latest is None:
                    raise AcquisitionEvidenceIntegrityError(
                        "archive reclamation receipt is unavailable"
                    )
                latest_receipt = (
                    AcquisitionEvidenceLifecycleReclamationParticipantV2
                    ._apply_from_blob(latest["apply_receipt_json"])
                )
                if (
                    latest_receipt.request_sha256
                    != str(latest["request_sha256"])
                    or latest_receipt.apply_receipt_sha256
                    != str(floor["last_apply_receipt_sha256"])
                ):
                    raise AcquisitionEvidenceIntegrityError(
                        "archive reclamation head receipt differs"
                    )
            if (
                previous_id != head["last_segment_id"]
                or previous_sha != head["last_segment_file_sha256"]
            ):
                raise AcquisitionEvidenceIntegrityError(
                    "archive chain head differs"
                )
            archive_object_bytes = 0
            for item in objects:
                self._verified_file(
                    self.archive_object_directory / str(item["filename"]),
                    str(item["content_sha256"]), int(item["size_bytes"]),
                )
                archive_object_bytes += int(item["size_bytes"])
            if (
                archive_object_bytes + checked_file_bytes
                != int(head["archive_bytes"]) - int(floor["reclaimed_file_bytes"])
            ):
                raise AcquisitionEvidenceIntegrityError(
                    "archive byte accounting differs from exact files"
                )
            archive_object_names = {
                item.name for item in self._bounded_files(
                    self.archive_object_directory,
                    self._limits.max_archive_objects + 1,
                )
            }
            segment_names = {
                item.name for item in self._bounded_files(
                    self.archive_segment_directory,
                    self._limits.max_archive_segments + 1,
                )
            }
            if archive_object_names != {str(item["filename"]) for item in objects}:
                raise AcquisitionEvidenceIntegrityError(
                    "archive object directory has unindexed entries"
                )
            if segment_names != {
                str(item["filename"])
                for item in segments
                if int(item["segment_ordinal"]) > floor_ordinal
            }:
                raise AcquisitionEvidenceIntegrityError(
                    "archive segment directory has unindexed entries"
                )
            for producer in hot_producers:
                outbox = db.execute(
                    "SELECT * FROM completion_outbox WHERE producer_receipt_sha256=?",
                    (str(producer["producer_receipt_sha256"]),),
                ).fetchone()
                if outbox is None:
                    raise AcquisitionEvidenceIntegrityError(
                        "hot producer lacks outbox row"
                    )
                self._producer_from_rows(producer, outbox)
            for item in hot_objects:
                if str(item["state"]) != "active":
                    raise AcquisitionEvidenceIntegrityError(
                        "completed prune left non-active hot object"
                    )
                self._verified_file(
                    self.hot_directory / str(item["filename"]),
                    str(item["content_sha256"]), int(item["size_bytes"]),
                )
            hot_names = {
                item.name for item in self._bounded_files(
                    self.hot_directory, self._limits.max_hot_objects + 1
                )
            }
            if hot_names != {str(item["filename"]) for item in hot_objects}:
                raise AcquisitionEvidenceIntegrityError(
                    "hot directory has unaccounted bytes"
                )
        result = {
            "archive_anchor_sha256": self._archive_anchor(head),
            "archive_bytes": int(head["archive_bytes"]),
            "archive_objects": len(objects),
            "archived_completions": len(indexes),
            "archived_quarantines": archived_quarantines,
            "archived_rejections": archived_rejections,
            "healthy": True,
            "hot_completions": len(hot_producers),
            "hot_objects": len(hot_objects),
            "segment_count": len(segments),
        }
        if len(_canonical(result)) > MAX_DIAGNOSTIC_BYTES:
            raise AcquisitionEvidenceCapacityError("archive audit output exceeds bound")
        return result

    def durable_clock_high_water(self) -> float:
        """Return the greatest wall-clock value durably used by the store.

        Lease expiries are deliberately excluded because they are future
        deadlines, not observed wall-clock values.  The result lets an owning
        composition reject clock rollback after a hard crash in the narrow
        window where a lifecycle terminal receipt committed before its own
        coordinator sidecar could persist the same observation.
        """

        with self._lock, closing(self._connect()) as db:
            try:
                schema_rows = db.execute(
                    "SELECT schema_version FROM lifecycle_schema "
                    "WHERE singleton=1"
                ).fetchall()
                if (
                    len(schema_rows) != 1
                    or int(schema_rows[0]["schema_version"]) != SCHEMA_VERSION
                ):
                    raise AcquisitionEvidenceIntegrityError(
                        "lifecycle clock authority schema differs"
                    )
                self._validate_clock_triggers(db)
                rows = db.execute(
                    "SELECT authority_schema,high_water FROM lifecycle_clock "
                    "WHERE singleton=1"
                ).fetchall()
                if (
                    len(rows) != 1
                    or str(rows[0]["authority_schema"])
                    != CLOCK_AUTHORITY_SCHEMA
                ):
                    raise AcquisitionEvidenceIntegrityError(
                        "lifecycle durable clock authority differs"
                    )
                return _finite(
                    rows[0]["high_water"],
                    "lifecycle durable clock high-water",
                )
            except AcquisitionEvidenceLifecycleError:
                raise
            except (sqlite3.DatabaseError, KeyError, TypeError, ValueError) as exc:
                raise AcquisitionEvidenceIntegrityError(
                    "lifecycle durable clock authority is invalid"
                ) from exc

    def diagnostics(self) -> dict[str, Any]:
        """Return bounded aggregate state without IDs, paths, payloads or proofs."""

        with self._lock, closing(self._connect()) as db:
            self._validate_v4_reclamation_schema(db)
            self._initialize_reclamation_triggers(db, allow_create=False)
            head = db.execute(
                "SELECT * FROM lifecycle_heads WHERE singleton=1"
            ).fetchone()
            states = {
                state: int(db.execute(
                    "SELECT count(*) FROM completion_outbox WHERE state=?", (state,)
                ).fetchone()[0])
                for state in sorted(_OUTBOX_STATES)
            }
            rejection_count = int(db.execute(
                "SELECT count(*) FROM rejection_receipts"
            ).fetchone()[0])
            quarantine_count = int(db.execute(
                "SELECT count(*) FROM quarantine_receipts"
            ).fetchone()[0])
            terminal_rejections = int(db.execute(
                "SELECT count(*) FROM rejection_receipts AS child "
                "JOIN completion_outbox AS outbox USING(event_id) "
                "WHERE outbox.state='consumed'"
            ).fetchone()[0])
            terminal_quarantines = int(db.execute(
                "SELECT count(*) FROM quarantine_receipts AS child "
                "JOIN completion_outbox AS outbox USING(event_id) "
                "WHERE outbox.state='consumed'"
            ).fetchone()[0])
            integrity = db.execute(
                "SELECT "
                "(SELECT count(*) FROM rejection_receipts AS rejection "
                "JOIN quarantine_receipts AS quarantine USING(event_id)) AS overlap,"
                "(SELECT count(*) FROM quarantine_receipts AS child "
                "LEFT JOIN consumption_receipts AS consumption USING(event_id) "
                "LEFT JOIN completion_outbox AS outbox USING(event_id) "
                "WHERE consumption.event_id IS NULL "
                "OR consumption.output_sha256<>child.quarantine_receipt_sha256 "
                "OR outbox.event_id IS NULL OR outbox.state<>'consumed') "
                "AS invalid_quarantine,"
                "(SELECT count(*) FROM rejection_receipts AS child "
                "LEFT JOIN consumption_receipts AS consumption USING(event_id) "
                "LEFT JOIN completion_outbox AS outbox USING(event_id) "
                "WHERE consumption.event_id IS NULL "
                "OR consumption.output_sha256<>child.rejection_receipt_sha256 "
                "OR outbox.event_id IS NULL "
                "OR outbox.state NOT IN ('leased','consumed')) "
                "AS invalid_rejection,"
                "(SELECT count(*) FROM completion_outbox AS outbox "
                "LEFT JOIN consumption_receipts AS consumption USING(event_id) "
                "WHERE outbox.state='consumed' AND consumption.event_id IS NULL) "
                "AS consumed_without_receipt,"
                "(SELECT count(*) FROM consumption_receipts AS consumption "
                "LEFT JOIN completion_outbox AS outbox USING(event_id) "
                "WHERE outbox.event_id IS NULL "
                "OR outbox.state IN ('ready','superseded')) "
                "AS invalid_consumption_state"
            ).fetchone()
            if integrity is None or any(int(integrity[key]) for key in integrity.keys()):
                raise AcquisitionEvidenceIntegrityError(
                    "terminal receipt topology differs"
                )
            reclamation_states = {
                state: int(db.execute(
                    "SELECT count(*) FROM lifecycle_reclamation_decisions "
                    "WHERE state=?",
                    (state,),
                ).fetchone()[0])
                for state in sorted(RECLAMATION_STATES)
            }
            floor = db.execute(
                "SELECT * FROM lifecycle_archive_floor WHERE singleton=1"
            ).fetchone()
            reclamation_integrity = db.execute(
                "SELECT "
                "(SELECT count(*) FROM (SELECT decision.request_sha256 FROM "
                "lifecycle_reclamation_decisions AS decision LEFT JOIN "
                "lifecycle_reclamation_members AS member USING(request_sha256) "
                "GROUP BY decision.request_sha256,decision.candidate_count,"
                "decision.releasable_entries HAVING count(member.candidate_sha256)"
                "<>decision.candidate_count OR count(member.candidate_sha256)"
                "<>decision.releasable_entries)) AS member_mismatch,"
                "(SELECT count(*) FROM lifecycle_reclamation_members AS member "
                "JOIN lifecycle_reclamation_decisions AS decision "
                "USING(request_sha256) JOIN archive_index AS archived "
                "USING(event_id) WHERE decision.state IN ('APPLYING','APPLIED')) "
                "AS positive_archive_overlap,"
                "(SELECT count(*) FROM lifecycle_reclamation_members AS member "
                "JOIN lifecycle_reclamation_decisions AS decision "
                "USING(request_sha256) JOIN producer_receipts AS hot "
                "ON hot.stream_id=member.stream_id AND "
                "hot.stream_sequence=member.stream_sequence WHERE "
                "decision.state IN ('APPLYING','APPLIED')) AS positive_hot_overlap,"
                "(SELECT count(*) FROM lifecycle_reclamation_decisions "
                "WHERE active_slot=1 AND state NOT IN ('PREPARED','APPLYING')) "
                "AS invalid_active_state"
            ).fetchone()
            if (
                floor is None
                or reclamation_integrity is None
                or any(
                    int(reclamation_integrity[key])
                    for key in reclamation_integrity.keys()
                )
                or reclamation_states["PREPARED"]
                + reclamation_states["APPLYING"] > 1
            ):
                raise AcquisitionEvidenceIntegrityError(
                    "reclamation receipt topology differs"
                )
            normal_consumed = (
                states["consumed"]
                - terminal_rejections
                - terminal_quarantines
            )
            if normal_consumed < 0:
                raise AcquisitionEvidenceIntegrityError(
                    "terminal child counts exceed consumed completions"
                )
            result = {
                "archive_anchor_sha256": self._archive_anchor(head),
                "archive_bytes": int(head["archive_bytes"]),
                "archived_completions": int(head["archived_completion_count"]),
                "contract": CONTRACT,
                "consumer_cursor_count": int(db.execute(
                    "SELECT count(*) FROM consumer_cursors"
                ).fetchone()[0]),
                "hot_completions": int(db.execute(
                    "SELECT count(*) FROM producer_receipts"
                ).fetchone()[0]),
                "hot_objects": int(db.execute(
                    "SELECT count(*) FROM hot_objects"
                ).fetchone()[0]),
                "outbox_states": states,
                "pending_prune_plans": int(db.execute(
                    "SELECT count(*) FROM prune_plans WHERE completed_at IS NULL"
                ).fetchone()[0]),
                "pending_terminal_receipts": int(db.execute(
                    "SELECT count(*) FROM consumption_receipts AS receipt "
                    "JOIN completion_outbox AS outbox USING(event_id) "
                    "WHERE outbox.state='leased'"
                ).fetchone()[0]),
                "quarantine_receipts": quarantine_count,
                "reclamation": {
                    "reclaimed_completions": int(
                        floor["reclaimed_completion_count"]
                    ),
                    "reclaimed_file_bytes": int(
                        floor["reclaimed_file_bytes"]
                    ),
                    "reclaimed_segments": int(
                        floor["reclaimed_segment_count"]
                    ),
                    "states": reclamation_states,
                },
                "rejection_receipts": rejection_count,
                "segment_count": int(head["segment_count"]),
                "terminal_kinds": {
                    "normal_consumed": normal_consumed,
                    "quarantined": terminal_quarantines,
                    "rejected": terminal_rejections,
                    "superseded": states["superseded"],
                },
            }
        if len(_canonical(result)) > MAX_DIAGNOSTIC_BYTES:
            raise AcquisitionEvidenceCapacityError("lifecycle diagnostics exceed bound")
        return result

    def reclamation_participant_v2(
        self,
        *,
        candidate_authority: SourceReclamationCandidateAuthority,
        committed_decision_authority: HeldCommittedDecisionAuthority,
    ) -> "AcquisitionEvidenceLifecycleReclamationParticipantV2":
        """Bind the inert v4 store to exact archive/coordinator authorities."""

        return AcquisitionEvidenceLifecycleReclamationParticipantV2(
            self,
            candidate_authority=candidate_authority,
            committed_decision_authority=committed_decision_authority,
        )


class AcquisitionEvidenceLifecycleReclamationParticipantV2:
    """Archived-prefix-only native participant for lifecycle schema v4.

    PREPARED is mutation-free.  The first durable APPLYING commit is the
    roll-forward linearization point: it removes the local lookup rows, advances
    an immutable prefix floor and leaves an exact file journal.  Files are
    verified and unlinked afterwards; APPLIED is emitted only after directory
    barriers prove that every journalled file is absent.
    """

    role = PARTICIPANT_APPLY_ORDER[0]

    def __init__(
        self,
        store: AcquisitionEvidenceLifecycleStore,
        *,
        candidate_authority: SourceReclamationCandidateAuthority,
        committed_decision_authority: HeldCommittedDecisionAuthority,
    ) -> None:
        if not isinstance(store, AcquisitionEvidenceLifecycleStore):
            raise TypeError("store must be AcquisitionEvidenceLifecycleStore")
        if not isinstance(
            committed_decision_authority, HeldCommittedDecisionAuthority
        ):
            raise TypeError(
                "committed_decision_authority must be HeldCommittedDecisionAuthority"
            )
        if not isinstance(candidate_authority, SourceReclamationCandidateAuthority):
            raise TypeError(
                "candidate_authority must be SourceReclamationCandidateAuthority"
            )
        self._store = store
        self._candidates = candidate_authority
        self._committed = committed_decision_authority
        self._candidate_authority_sha256 = candidate_authority.authority_sha256
        self._source_store_identity_sha256 = store.identity_sha256
        self._coordinator_authority_sha256 = (
            committed_decision_authority.authority_sha256
        )

    @property
    def source_store_identity_sha256(self) -> str:
        current = self._store.identity_sha256
        if not hmac.compare_digest(
            current, self._source_store_identity_sha256
        ):
            raise AcquisitionEvidenceIntegrityError(
                "lifecycle source-store identity changed"
            )
        return current

    @property
    def coordinator_authority_sha256(self) -> str:
        if not hmac.compare_digest(
            self._committed.authority_sha256,
            self._coordinator_authority_sha256,
        ):
            raise AcquisitionEvidenceIntegrityError(
                "held coordinator authority changed"
            )
        return self._coordinator_authority_sha256

    @property
    def candidate_authority_sha256(self) -> str:
        current = self._candidates.authority_sha256
        if not hmac.compare_digest(current, self._candidate_authority_sha256):
            raise AcquisitionEvidenceIntegrityError(
                "candidate authority changed"
            )
        return current

    @staticmethod
    def _floor_mapping(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "authority_schema": str(row["authority_schema"]),
            "last_apply_receipt_sha256": row["last_apply_receipt_sha256"],
            "pruned_through_file_sha256": row[
                "pruned_through_file_sha256"
            ],
            "pruned_through_global_sequence": int(
                row["pruned_through_global_sequence"]
            ),
            "pruned_through_segment_id": row["pruned_through_segment_id"],
            "pruned_through_segment_ordinal": int(
                row["pruned_through_segment_ordinal"]
            ),
            "reclaimed_completion_count": int(
                row["reclaimed_completion_count"]
            ),
            "reclaimed_file_bytes": int(row["reclaimed_file_bytes"]),
            "reclaimed_segment_count": int(row["reclaimed_segment_count"]),
            "schema": RECLAMATION_FLOOR_SCHEMA,
        }

    @classmethod
    def _floor_sha256(cls, row: sqlite3.Row) -> str:
        return reclamation_digest_mapping(cls._floor_mapping(row))

    @staticmethod
    def _decode_json(blob: object, *, label: str) -> dict[str, Any]:
        try:
            payload = bytes(blob)
            value = json.loads(payload.decode("ascii"))
        except (TypeError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise AcquisitionEvidenceIntegrityError(
                f"{label} is corrupt"
            ) from exc
        if (
            not isinstance(value, dict)
            or payload != reclamation_canonical_bytes(value)
        ):
            raise AcquisitionEvidenceIntegrityError(
                f"{label} is not canonical"
            )
        return value

    @classmethod
    def _prepare_from_blob(cls, blob: object) -> SourceLocalPrepareReceiptV2:
        value = cls._decode_json(blob, label="lifecycle prepare receipt")
        expected = {
            "archive_binding_root_sha256", "candidate_bindings_sha256",
            "candidate_authority_sha256", "candidate_count",
            "child_reference_root_sha256",
            "consumer_epoch_root_sha256", "consumer_registry_root_sha256",
            "coordinator_authority_sha256", "prepared_at",
            "prepare_receipt_sha256", "protocol_contract",
            "releasable_bytes", "releasable_entries", "request_sha256",
            "role", "schema", "sealed_archive_anchor_sha256",
            "source_fence_sha256", "source_store_identity_sha256",
            "source_view_sha256",
        }
        if set(value) != expected:
            raise AcquisitionEvidenceIntegrityError(
                "lifecycle prepare receipt shape differs"
            )
        return SourceLocalPrepareReceiptV2(**{
            key: value[key]
            for key in SourceLocalPrepareReceiptV2.__dataclass_fields__
        })

    @classmethod
    def _apply_from_blob(cls, blob: object) -> SourceLocalApplyReceiptV1:
        value = cls._decode_json(blob, label="lifecycle apply receipt")
        expected_fields = set(SourceLocalApplyReceiptV1.__dataclass_fields__)
        values = {key: value[key] for key in expected_fields}
        if set(value) != expected_fields | {"protocol_contract", "schema"}:
            raise AcquisitionEvidenceIntegrityError(
                "lifecycle apply receipt shape differs"
            )
        return SourceLocalApplyReceiptV1(**values)

    @classmethod
    def _abort_from_blob(cls, blob: object) -> SourceLocalAbortReceiptV1:
        value = cls._decode_json(blob, label="lifecycle abort receipt")
        expected_fields = set(SourceLocalAbortReceiptV1.__dataclass_fields__)
        values = {key: value[key] for key in expected_fields}
        if set(value) != expected_fields | {"protocol_contract", "schema"}:
            raise AcquisitionEvidenceIntegrityError(
                "lifecycle abort receipt shape differs"
            )
        return SourceLocalAbortReceiptV1(**values)

    def _assert_bound_authorities(self) -> None:
        self.source_store_identity_sha256
        self.coordinator_authority_sha256
        self.candidate_authority_sha256

    @contextmanager
    def _held_members(
        self,
        request: SourceReclamationRequestV2,
    ) -> Iterator[tuple[LifecycleReclamationArchiveMemberV2, ...]]:
        self._assert_bound_authorities()
        if not isinstance(request, SourceReclamationRequestV2):
            raise TypeError("request must be SourceReclamationRequestV2")
        if request.candidate_authority_sha256 != self.candidate_authority_sha256:
            raise AcquisitionEvidenceConflict(
                "reclamation request candidate authority differs"
            )
        with ExitStack() as stack:
            cursor = 0
            bindings: list[SourceReclamationCandidateBindingV1] = []
            records: list[dict[str, Any]] = []
            record_payloads: list[bytes] = []
            while cursor < request.candidate_count:
                page = stack.enter_context(
                    self._candidates.hold_candidate_page_v1(
                        request,
                        after_cursor=cursor,
                        max_candidates=min(64, request.candidate_count - cursor),
                    )
                )
                if not isinstance(page, SourceReclamationCandidatePageV1):
                    raise AcquisitionEvidenceIntegrityError(
                        "candidate authority returned an invalid page"
                    )
                if (
                    page.request_sha256 != request.request_sha256
                    or page.candidate_authority_sha256
                    != self.candidate_authority_sha256
                    or page.sealed_archive_anchor_sha256
                    != request.sealed_archive_anchor_sha256
                    or page.archive_binding_root_sha256
                    != request.archive_binding_root_sha256
                    or page.candidate_bindings_sha256
                    != request.candidate_bindings_sha256
                    or page.consumer_registry_root_sha256
                    != request.consumer_registry_root_sha256
                    or page.consumer_epoch_root_sha256
                    != request.consumer_epoch_root_sha256
                    or page.after_cursor != cursor
                    or page.total_candidate_count != request.candidate_count
                    or page.next_cursor <= cursor
                ):
                    raise AcquisitionEvidenceIntegrityError(
                        "candidate page differs from its request"
                    )
                bindings.extend(page.bindings)
                records.extend(page.decoded_records())
                record_payloads.extend(page.record_payloads)
                cursor = page.next_cursor
                if page.exhausted != (cursor == request.candidate_count):
                    raise AcquisitionEvidenceIntegrityError(
                        "candidate page exhaustion differs"
                    )
            binding_tuple = tuple(bindings)
            if (
                len(binding_tuple) != request.candidate_count
                or tuple(item.candidate_sha256 for item in binding_tuple)
                != tuple(sorted(item.candidate_sha256 for item in binding_tuple))
                or reclamation_candidate_binding_root(binding_tuple)
                != request.candidate_bindings_sha256
            ):
                raise AcquisitionEvidenceIntegrityError(
                    "candidate binding inventory differs"
                )
            archive_root = reclamation_archive_binding_root(
                binding_tuple, tuple(record_payloads)
            )
            if archive_root != request.archive_binding_root_sha256:
                raise AcquisitionEvidenceIntegrityError(
                    "candidate archive binding root differs"
                )
            members: list[LifecycleReclamationArchiveMemberV2] = []
            for binding, record in zip(binding_tuple, records, strict=True):
                identity = self._store.terminal_event_identity(
                    binding.evidence_stream_id,
                    binding.evidence_stream_sequence,
                )
                if (
                    identity is None
                    or identity.status != "archived"
                    or record.get("candidate_sha256")
                    != binding.candidate_sha256
                    or record.get("record_sha256")
                    != binding.sealed_archive_record_sha256
                    or record.get("sealed_archive_segment_sha256")
                    != binding.sealed_archive_segment_sha256
                    or identity.stream_id != binding.evidence_stream_id
                    or identity.stream_sequence
                    != binding.evidence_stream_sequence
                    or candidate_participant_view_sha256(record, self.role)
                    != self._store.source_reclamation_candidate_view_sha256(
                        binding
                    )
                ):
                    raise AcquisitionEvidenceIntegrityError(
                        "candidate record lifecycle binding differs"
                    )
                members.append(LifecycleReclamationArchiveMemberV2(
                    candidate_sha256=binding.candidate_sha256,
                    acquisition_job_id=binding.acquisition_job_id,
                    event_id=identity.event_id,
                    event_sha256=identity.event_sha256,
                    producer_receipt_sha256=identity.producer_receipt_sha256,
                    consumer_id=identity.consumer_id,
                    stream_id=identity.stream_id,
                    stream_sequence=identity.stream_sequence,
                    terminal_identity_sha256=identity.identity_sha256,
                    lifecycle_archive_segment_id=identity.archive_segment_id,
                    retention_anchor_sha256=(
                        request.sealed_archive_anchor_sha256
                    ),
                    retention_segment_sha256=(
                        binding.sealed_archive_segment_sha256
                    ),
                    retention_record_sha256=(
                        binding.sealed_archive_record_sha256
                    ),
                    archive_metadata_sha256=record["metadata_sha256"],
                    archive_object_reference_root_sha256=(
                        record["archive_object_reference_root_sha256"]
                    ),
                    archive_object_count=record["archive_object_count"],
                    archive_object_total_bytes=(
                        record["archive_object_total_bytes"]
                    ),
                ))
            result = tuple(members)
            if len({item.event_id for item in result}) != len(result):
                raise AcquisitionEvidenceIntegrityError(
                    "candidate lifecycle member inventory contains duplicates"
                )
            yield result

    def _snapshot(
        self,
        request: SourceReclamationRequestV2,
        members: tuple[LifecycleReclamationArchiveMemberV2, ...],
        *,
        database: sqlite3.Connection | None = None,
    ) -> dict[str, Any]:
        store = self._store
        connection = (
            closing(store._connect())
            if database is None
            else nullcontext(database)
        )
        with store._lock, connection as db:
            floor = db.execute(
                "SELECT * FROM lifecycle_archive_floor WHERE singleton=1"
            ).fetchone()
            if floor is None:
                raise AcquisitionEvidenceIntegrityError(
                    "lifecycle archive floor is unavailable"
                )
            floor_ordinal = int(floor["pruned_through_segment_ordinal"])
            retained_segments = db.execute(
                "SELECT * FROM archive_segments WHERE segment_ordinal>? "
                "ORDER BY segment_ordinal",
                (floor_ordinal,),
            ).fetchall()
            if not retained_segments:
                raise AcquisitionEvidenceConflict(
                    "lifecycle archived prefix is unavailable"
                )
            member_segment_ids = {
                item.lifecycle_archive_segment_id for item in members
            }
            ordinals = {
                str(row["segment_id"]): int(row["segment_ordinal"])
                for row in retained_segments
            }
            if not member_segment_ids <= set(ordinals):
                raise AcquisitionEvidenceConflict(
                    "lifecycle archived member segment is unavailable"
                )
            target_ordinal = max(ordinals[item] for item in member_segment_ids)
            target_segments = tuple(
                row for row in retained_segments
                if int(row["segment_ordinal"]) <= target_ordinal
            )
            if (
                not target_segments
                or int(target_segments[0]["segment_ordinal"])
                != floor_ordinal + 1
                or {
                    str(row["segment_id"]) for row in target_segments
                } != member_segment_ids
            ):
                raise AcquisitionEvidenceConflict(
                    "lifecycle reclamation requires the complete archived prefix"
                )
            target_ids = tuple(str(row["segment_id"]) for row in target_segments)
            placeholders = ",".join("?" for _ in target_ids)
            indexes = db.execute(
                "SELECT * FROM archive_index WHERE segment_id IN "
                f"({placeholders}) ORDER BY global_sequence",
                target_ids,
            ).fetchall()
            by_event = {item.event_id: item for item in members}
            if (
                len(indexes) != len(members)
                or set(by_event) != {str(row["event_id"]) for row in indexes}
            ):
                raise AcquisitionEvidenceConflict(
                    "lifecycle reclamation prefix is partial or shared"
                )
            archive_indexes: list[dict[str, Any]] = []
            archive_index_by_event: dict[str, dict[str, Any]] = {}
            for row in indexes:
                member = by_event[str(row["event_id"])]
                if (
                    str(row["producer_receipt_sha256"])
                    != member.producer_receipt_sha256
                    or str(row["event_sha256"]) != member.event_sha256
                    or str(row["consumer_id"]) != member.consumer_id
                    or str(row["stream_id"]) != member.stream_id
                    or int(row["stream_sequence"]) != member.stream_sequence
                    or str(row["segment_id"])
                    != member.lifecycle_archive_segment_id
                ):
                    raise AcquisitionEvidenceIntegrityError(
                        "lifecycle archived member identity differs"
                    )
                index_core = {
                    "acquisition_job_id": str(row["acquisition_job_id"]),
                    "consumer_id": str(row["consumer_id"]),
                    "content_sha256": str(row["content_sha256"]),
                    "event_id": str(row["event_id"]),
                    "event_sha256": str(row["event_sha256"]),
                    "global_sequence": int(row["global_sequence"]),
                    "producer_receipt_sha256": str(
                        row["producer_receipt_sha256"]
                    ),
                    "schema": RECLAMATION_ARCHIVE_INDEX_SCHEMA,
                    "segment_id": str(row["segment_id"]),
                    "stream_id": str(row["stream_id"]),
                    "stream_sequence": int(row["stream_sequence"]),
                }
                indexed = {
                    **index_core,
                    "archive_index_sha256": reclamation_digest_mapping(
                        index_core
                    ),
                }
                archive_indexes.append(indexed)
                archive_index_by_event[str(row["event_id"])] = indexed
            pending = db.execute(
                "SELECT segment_id FROM prune_plans WHERE segment_id IN "
                f"({placeholders}) AND completed_at IS NULL LIMIT 1",
                target_ids,
            ).fetchone()
            if pending is not None:
                raise AcquisitionEvidenceConflict(
                    "lifecycle hot prune is not complete"
                )
            hot_overlap = db.execute(
                "SELECT 1 FROM producer_receipts AS producer JOIN archive_index "
                "AS archived USING(stream_id,stream_sequence) WHERE "
                "archived.segment_id IN "
                f"({placeholders}) LIMIT 1",
                target_ids,
            ).fetchone()
            if hot_overlap is not None:
                raise AcquisitionEvidenceIntegrityError(
                    "lifecycle archived prefix overlaps hot evidence"
                )
            segment_catalogs: list[dict[str, Any]] = []
            sealed_event_ids: set[str] = set()
            for segment_row in target_segments:
                bundle = store._segment_bundle(segment_row)
                bundle_events = {
                    str(item["completion_event"]["event_id"])
                    for item in bundle["records"]
                }
                segment_events = {
                    str(row["event_id"])
                    for row in indexes
                    if str(row["segment_id"])
                    == str(segment_row["segment_id"])
                }
                if bundle_events != segment_events:
                    raise AcquisitionEvidenceIntegrityError(
                        "lifecycle target segment record inventory differs"
                    )
                sealed_event_ids.update(bundle_events)
                segment_core = {
                    "completion_count": int(segment_row["completion_count"]),
                    "content_sha256": str(segment_row["content_sha256"]),
                    "created_at": float(segment_row["created_at"]),
                    "file_bytes": int(segment_row["file_bytes"]),
                    "file_sha256": str(segment_row["file_sha256"]),
                    "filename": str(segment_row["filename"]),
                    "first_global_sequence": int(
                        segment_row["first_global_sequence"]
                    ),
                    "last_global_sequence": int(
                        segment_row["last_global_sequence"]
                    ),
                    "previous_file_sha256": segment_row[
                        "previous_file_sha256"
                    ],
                    "previous_segment_id": segment_row[
                        "previous_segment_id"
                    ],
                    "manifest_sha256": hashlib.sha256(
                        bytes(segment_row["manifest_json"])
                    ).hexdigest(),
                    "segment_id": str(segment_row["segment_id"]),
                    "segment_ordinal": int(segment_row["segment_ordinal"]),
                }
                segment_catalogs.append({
                    **segment_core,
                    "catalog_row_sha256": reclamation_digest_mapping({
                        **segment_core,
                        "schema": (
                            "stv3-worker-lifecycle-reclamation-segment-catalog-v1"
                        ),
                    }),
                })
            if sealed_event_ids != set(by_event):
                raise AcquisitionEvidenceIntegrityError(
                    "lifecycle target segment/member inventory differs"
                )
            content_ids = tuple(sorted({
                str(row["content_sha256"]) for row in indexes
            }))
            object_files: list[dict[str, Any]] = []
            object_reference_cores: list[dict[str, Any]] = []
            for content_sha in content_ids:
                target_references = sum(
                    str(row["content_sha256"]) == content_sha
                    for row in indexes
                )
                remaining = int(db.execute(
                    "SELECT count(*) FROM archive_index WHERE content_sha256=? "
                    f"AND segment_id NOT IN ({placeholders})",
                    (content_sha, *target_ids),
                ).fetchone()[0])
                object_row = db.execute(
                    "SELECT * FROM archive_objects WHERE content_sha256=?",
                    (content_sha,),
                ).fetchone()
                if object_row is None:
                    raise AcquisitionEvidenceIntegrityError(
                        "lifecycle archive object index is unavailable"
                    )
                store._verified_file(
                    store.archive_object_directory / str(object_row["filename"]),
                    content_sha,
                    int(object_row["size_bytes"]),
                )
                catalog_core = {
                    "content_sha256": content_sha,
                    "filename": str(object_row["filename"]),
                    "size_bytes": int(object_row["size_bytes"]),
                }
                catalog_sha = reclamation_digest_mapping({
                    **catalog_core,
                    "schema": (
                        "stv3-worker-lifecycle-reclamation-object-catalog-v1"
                    ),
                })
                reference_core = {
                    **catalog_core,
                    "catalog_row_sha256": catalog_sha,
                    "delete_file": remaining == 0,
                    "request_sha256": request.request_sha256,
                    "retained_reference_count": remaining,
                    "schema": RECLAMATION_OBJECT_REFERENCE_SCHEMA,
                    "target_reference_count": target_references,
                }
                object_reference_cores.append(reference_core)
                if remaining:
                    continue
                object_files.append({
                    "content_sha256": content_sha,
                    "file_kind": "object",
                    "filename": str(object_row["filename"]),
                    "identity_sha256": content_sha,
                    "size_bytes": int(object_row["size_bytes"]),
                })
            segment_files = [{
                "content_sha256": str(row["file_sha256"]),
                "file_kind": "segment",
                "filename": str(row["filename"]),
                "identity_sha256": str(row["segment_id"]),
                "size_bytes": int(row["file_bytes"]),
            } for row in target_segments]
            files = tuple(sorted(
                (*segment_files, *object_files),
                key=lambda item: (item["file_kind"], item["identity_sha256"]),
            ))
            target = target_segments[-1]
            floor_before = self._floor_mapping(floor)
            floor_sha = reclamation_digest_mapping(floor_before)
            releasable_bytes = sum(item["size_bytes"] for item in files)
            expected_floor_after = {
                **floor_before,
                "pruned_through_file_sha256": str(target["file_sha256"]),
                "pruned_through_global_sequence": int(
                    target["last_global_sequence"]
                ),
                "pruned_through_segment_id": str(target["segment_id"]),
                "pruned_through_segment_ordinal": int(
                    target["segment_ordinal"]
                ),
                "reclaimed_completion_count": int(
                    floor["reclaimed_completion_count"]
                ) + len(members),
                "reclaimed_file_bytes": int(floor["reclaimed_file_bytes"])
                + releasable_bytes,
                "reclaimed_segment_count": int(
                    floor["reclaimed_segment_count"]
                ) + len(target_segments),
            }
            expected_floor_sha = reclamation_digest_mapping(
                expected_floor_after
            )
            object_effect_binding_sha = (
                self._object_reference_effect_binding(
                    archive_indexes=archive_indexes,
                    files=files,
                    floor_before_sha256=floor_sha,
                    object_reference_cores=object_reference_cores,
                    request_sha256=request.request_sha256,
                    segments=segment_catalogs,
                    target_floor_after_sha256=expected_floor_sha,
                )
            )
            object_references = [{
                **reference_core,
                "effect_binding_sha256": object_effect_binding_sha,
                "reference_sha256": reclamation_digest_mapping({
                    **reference_core,
                    "effect_binding_sha256": object_effect_binding_sha,
                }),
            } for reference_core in object_reference_cores]
            effect_core = {
                "archive_indexes": archive_indexes,
                "files": list(files),
                "floor_before_sha256": floor_sha,
                "object_references": object_references,
                "request_sha256": request.request_sha256,
                "schema": RECLAMATION_EFFECT_SCHEMA,
                "segments": segment_catalogs,
                "target_floor_after_sha256": expected_floor_sha,
            }
            effect_sha = reclamation_digest_mapping(effect_core)
            effect = {**effect_core, "effect_sha256": effect_sha}
            source_view = reclamation_digest_mapping({
                "candidate_authority_sha256": (
                    self.candidate_authority_sha256
                ),
                "candidate_bindings_sha256": (
                    request.candidate_bindings_sha256
                ),
                "floor_before_sha256": floor_sha,
                "effect_sha256": effect_sha,
                "members": [item.as_dict() for item in members],
                "schema": "stv3-worker-lifecycle-reclamation-source-view-v1",
                "source_store_identity_sha256": self.source_store_identity_sha256,
            })
            return {
                "archive_index_by_event": archive_index_by_event,
                "child_reference_root_sha256": reclamation_digest_mapping(
                    [item.as_dict() for item in members]
                ),
                "effect": effect,
                "effect_sha256": effect_sha,
                "files": files,
                "floor_before": floor_before,
                "floor_before_sha256": floor_sha,
                "expected_floor_after": expected_floor_after,
                "expected_floor_after_sha256": expected_floor_sha,
                "object_references": tuple(object_references),
                "releasable_bytes": releasable_bytes,
                "releasable_entries": len(members),
                "segment_catalogs": tuple(segment_catalogs),
                "source_view_sha256": source_view,
                "target_floor_file_sha256": str(target["file_sha256"]),
                "target_floor_global_sequence": int(target["last_global_sequence"]),
                "target_floor_ordinal": int(target["segment_ordinal"]),
                "target_floor_segment_id": str(target["segment_id"]),
                "target_segment_count": len(target_segments),
            }

    @staticmethod
    def _member_sha256(member: LifecycleReclamationArchiveMemberV2) -> str:
        return reclamation_digest_mapping(member.as_dict())

    @staticmethod
    def _member_from_journal_row(
        row: sqlite3.Row,
    ) -> LifecycleReclamationArchiveMemberV2:
        return LifecycleReclamationArchiveMemberV2(
            candidate_sha256=str(row["candidate_sha256"]),
            acquisition_job_id=str(row["acquisition_job_id"]),
            event_id=str(row["event_id"]),
            event_sha256=str(row["event_sha256"]),
            producer_receipt_sha256=str(row["producer_receipt_sha256"]),
            consumer_id=str(row["consumer_id"]),
            stream_id=str(row["stream_id"]),
            stream_sequence=int(row["stream_sequence"]),
            terminal_identity_sha256=str(row["terminal_identity_sha256"]),
            lifecycle_archive_segment_id=str(
                row["lifecycle_archive_segment_id"]
            ),
            retention_anchor_sha256=str(row["retention_anchor_sha256"]),
            retention_segment_sha256=str(row["retention_segment_sha256"]),
            retention_record_sha256=str(row["retention_record_sha256"]),
            archive_metadata_sha256=str(row["archive_metadata_sha256"]),
            archive_object_reference_root_sha256=str(
                row["archive_object_reference_root_sha256"]
            ),
            archive_object_count=int(row["archive_object_count"]),
            archive_object_total_bytes=int(row["archive_object_total_bytes"]),
        )

    def _validate_reclamation_authority(
        self,
        db: sqlite3.Connection,
    ) -> None:
        self._store._validate_v4_reclamation_schema(db)
        self._store._initialize_reclamation_triggers(
            db,
            allow_create=False,
        )

    def _request_from_decision_row(
        self,
        row: sqlite3.Row,
    ) -> SourceReclamationRequestV2:
        value = self._decode_json(
            row["request_json"],
            label="lifecycle reclamation request",
        )
        try:
            request = SourceReclamationRequestV2(**{
                key: value[key]
                for key in SourceReclamationRequestV2.__dataclass_fields__
            })
        except (KeyError, TypeError, ValueError) as exc:
            raise AcquisitionEvidenceIntegrityError(
                "lifecycle reclamation request shape differs"
            ) from exc
        if (
            value != request.as_dict()
            or request.request_sha256 != str(row["request_sha256"])
        ):
            raise AcquisitionEvidenceIntegrityError(
                "lifecycle reclamation request identity differs"
            )
        return request

    def _journal_members(
        self,
        db: sqlite3.Connection,
        request_sha256: str,
    ) -> tuple[tuple[LifecycleReclamationArchiveMemberV2, sqlite3.Row], ...]:
        rows = db.execute(
            "SELECT * FROM lifecycle_reclamation_members "
            "WHERE request_sha256=? ORDER BY candidate_sha256",
            (request_sha256,),
        ).fetchall()
        result: list[tuple[LifecycleReclamationArchiveMemberV2, sqlite3.Row]] = []
        for row in rows:
            member = self._member_from_journal_row(row)
            if (
                str(row["request_sha256"]) != request_sha256
                or str(row["member_sha256"])
                != self._member_sha256(member)
            ):
                raise AcquisitionEvidenceIntegrityError(
                    "lifecycle reclamation member journal differs"
                )
            result.append((member, row))
        members = tuple(result)
        if len({item[0].candidate_sha256 for item in members}) != len(members):
            raise AcquisitionEvidenceIntegrityError(
                "lifecycle reclamation member journal contains duplicates"
            )
        return members

    def _journal_object_references(
        self,
        db: sqlite3.Connection,
        request_sha256: str,
    ) -> tuple[dict[str, Any], ...]:
        rows = db.execute(
            "SELECT * FROM lifecycle_reclamation_object_references "
            "WHERE request_sha256=? ORDER BY content_sha256",
            (request_sha256,),
        ).fetchall()
        references: list[dict[str, Any]] = []
        for row in rows:
            catalog_core = {
                "content_sha256": str(row["content_sha256"]),
                "filename": str(row["filename"]),
                "size_bytes": int(row["size_bytes"]),
            }
            catalog_sha = reclamation_digest_mapping({
                **catalog_core,
                "schema": (
                    "stv3-worker-lifecycle-reclamation-object-catalog-v1"
                ),
            })
            core = {
                **catalog_core,
                "catalog_row_sha256": catalog_sha,
                "delete_file": bool(int(row["delete_file"])),
                "request_sha256": request_sha256,
                "retained_reference_count": int(
                    row["retained_reference_count"]
                ),
                "schema": RECLAMATION_OBJECT_REFERENCE_SCHEMA,
                "target_reference_count": int(row["target_reference_count"]),
            }
            effect_binding_sha256 = require_reclamation_sha256(
                str(row["effect_binding_sha256"]),
                "lifecycle object effect binding SHA-256",
            )
            reference = {
                **core,
                "effect_binding_sha256": effect_binding_sha256,
                "reference_sha256": reclamation_digest_mapping({
                    **core,
                    "effect_binding_sha256": effect_binding_sha256,
                }),
            }
            if (
                str(row["request_sha256"]) != request_sha256
                or str(row["catalog_row_sha256"]) != catalog_sha
                or str(row["reference_sha256"])
                != reference["reference_sha256"]
                or reference["target_reference_count"] < 1
                or reference["retained_reference_count"] < 0
                or reference["delete_file"]
                != (reference["retained_reference_count"] == 0)
                or reference["filename"]
                != f"{reference['content_sha256']}.raw"
            ):
                raise AcquisitionEvidenceIntegrityError(
                    "lifecycle reclamation object-reference journal differs"
                )
            require_reclamation_sha256(
                reference["content_sha256"],
                "lifecycle object content SHA-256",
            )
            references.append(reference)
        return tuple(references)

    @staticmethod
    def _object_reference_core(item: Mapping[str, Any]) -> dict[str, Any]:
        expected = {
            "catalog_row_sha256", "content_sha256", "delete_file",
            "effect_binding_sha256", "filename", "reference_sha256",
            "request_sha256", "retained_reference_count", "schema",
            "size_bytes", "target_reference_count",
        }
        if set(item) != expected:
            raise AcquisitionEvidenceIntegrityError(
                "lifecycle reclamation object-reference shape differs"
            )
        return {
            key: item[key]
            for key in expected
            if key not in {"effect_binding_sha256", "reference_sha256"}
        }

    @staticmethod
    def _object_reference_effect_binding(
        *,
        archive_indexes: list[dict[str, Any]] | tuple[dict[str, Any], ...],
        files: list[dict[str, Any]] | tuple[dict[str, Any], ...],
        floor_before_sha256: str,
        object_reference_cores: (
            list[dict[str, Any]] | tuple[dict[str, Any], ...]
        ),
        request_sha256: str,
        segments: list[dict[str, Any]] | tuple[dict[str, Any], ...],
        target_floor_after_sha256: str,
    ) -> str:
        return reclamation_digest_mapping({
            "archive_indexes": list(archive_indexes),
            "files": list(files),
            "floor_before_sha256": floor_before_sha256,
            "object_reference_cores": list(object_reference_cores),
            "request_sha256": request_sha256,
            "schema": RECLAMATION_OBJECT_EFFECT_BINDING_SCHEMA,
            "segments": list(segments),
            "target_floor_after_sha256": target_floor_after_sha256,
        })

    @staticmethod
    def _segment_catalog_core(item: Mapping[str, Any]) -> dict[str, Any]:
        expected = {
            "catalog_row_sha256", "completion_count", "content_sha256",
            "created_at", "file_bytes", "file_sha256", "filename",
            "first_global_sequence", "last_global_sequence",
            "manifest_sha256", "previous_file_sha256",
            "previous_segment_id", "segment_id", "segment_ordinal",
        }
        if set(item) != expected:
            raise AcquisitionEvidenceIntegrityError(
                "lifecycle reclamation segment catalog shape differs"
            )
        return {
            key: item[key]
            for key in expected
            if key != "catalog_row_sha256"
        }

    def _load_reclamation_journal(
        self,
        db: sqlite3.Connection,
        row: sqlite3.Row,
    ) -> dict[str, Any]:
        request = self._request_from_decision_row(row)
        request_sha = request.request_sha256
        prepared = self._prepare_from_blob(row["prepare_receipt_json"])
        if (
            prepared.request_sha256 != request_sha
            or prepared.prepare_receipt_sha256
            != str(row["prepare_receipt_sha256"])
            or prepared.role != self.role
            or prepared.source_store_identity_sha256
            != self.source_store_identity_sha256
            or prepared.coordinator_authority_sha256
            != self.coordinator_authority_sha256
            or prepared.candidate_authority_sha256
            != self.candidate_authority_sha256
            or prepared.sealed_archive_anchor_sha256
            != request.sealed_archive_anchor_sha256
            or prepared.archive_binding_root_sha256
            != request.archive_binding_root_sha256
            or prepared.candidate_bindings_sha256
            != request.candidate_bindings_sha256
            or prepared.consumer_registry_root_sha256
            != request.consumer_registry_root_sha256
            or prepared.consumer_epoch_root_sha256
            != request.consumer_epoch_root_sha256
            or prepared.source_view_sha256 != str(row["source_view_sha256"])
            or prepared.child_reference_root_sha256
            != str(row["child_reference_root_sha256"])
            or prepared.source_fence_sha256 != str(row["source_fence_sha256"])
            or prepared.releasable_bytes != int(row["releasable_bytes"])
            or prepared.releasable_entries != int(row["releasable_entries"])
            or prepared.candidate_count != int(row["candidate_count"])
            or prepared.prepared_at != float(row["prepared_at"])
            or str(row["coordinator_authority_sha256"])
            != self.coordinator_authority_sha256
            or int(row["journal_sealed"]) != 1
        ):
            raise AcquisitionEvidenceIntegrityError(
                "lifecycle prepare journal binding differs"
            )
        members_with_rows = self._journal_members(db, request_sha)
        members = tuple(item[0] for item in members_with_rows)
        if (
            len(members) != request.candidate_count
            or len(members) != int(row["candidate_count"])
            or reclamation_digest_mapping(
                [item.as_dict() for item in members]
            ) != str(row["child_reference_root_sha256"])
        ):
            raise AcquisitionEvidenceIntegrityError(
                "lifecycle reclamation member root differs"
            )
        archive_indexes: list[dict[str, Any]] = []
        for member, member_row in members_with_rows:
            core = {
                "acquisition_job_id": member.acquisition_job_id,
                "consumer_id": member.consumer_id,
                "content_sha256": str(member_row["archive_content_sha256"]),
                "event_id": member.event_id,
                "event_sha256": member.event_sha256,
                "global_sequence": int(member_row["archive_global_sequence"]),
                "producer_receipt_sha256": member.producer_receipt_sha256,
                "schema": RECLAMATION_ARCHIVE_INDEX_SCHEMA,
                "segment_id": member.lifecycle_archive_segment_id,
                "stream_id": member.stream_id,
                "stream_sequence": member.stream_sequence,
            }
            indexed = {
                **core,
                "archive_index_sha256": reclamation_digest_mapping(core),
            }
            if indexed["archive_index_sha256"] != str(
                member_row["archive_index_sha256"]
            ):
                raise AcquisitionEvidenceIntegrityError(
                    "lifecycle reclamation archive-index journal differs"
                )
            archive_indexes.append(indexed)
        archive_indexes.sort(key=lambda item: item["global_sequence"])
        files = self._journal_files(request_sha, database=db)
        references = self._journal_object_references(db, request_sha)
        effect = self._decode_json(
            row["effect_json"],
            label="lifecycle reclamation effect",
        )
        effect_keys = {
            "archive_indexes", "effect_sha256", "files",
            "floor_before_sha256", "object_references", "request_sha256",
            "schema", "segments", "target_floor_after_sha256",
        }
        if set(effect) != effect_keys:
            raise AcquisitionEvidenceIntegrityError(
                "lifecycle reclamation effect shape differs"
            )
        effect_core = dict(effect)
        effect_sha = effect_core.pop("effect_sha256", None)
        if (
            effect.get("schema") != RECLAMATION_EFFECT_SCHEMA
            or effect.get("request_sha256") != request_sha
            or effect_sha != reclamation_digest_mapping(effect_core)
            or effect_sha != str(row["effect_sha256"])
            or effect.get("archive_indexes") != archive_indexes
            or effect.get("files") != list(files)
            or effect.get("object_references") != list(references)
        ):
            raise AcquisitionEvidenceIntegrityError(
                "lifecycle reclamation effect identity differs"
            )
        floor_before = self._decode_json(
            row["floor_before_json"],
            label="lifecycle reclamation floor-before",
        )
        floor_before_sha = reclamation_digest_mapping(floor_before)
        if (
            floor_before.get("schema") != RECLAMATION_FLOOR_SCHEMA
            or floor_before_sha != str(row["floor_before_sha256"])
            or effect.get("floor_before_sha256") != floor_before_sha
            or row["prior_apply_receipt_sha256"]
            != floor_before.get("last_apply_receipt_sha256")
        ):
            raise AcquisitionEvidenceIntegrityError(
                "lifecycle reclamation floor-before differs"
            )
        expected_floor_after = {
            **floor_before,
            "pruned_through_file_sha256": str(
                row["target_floor_file_sha256"]
            ),
            "pruned_through_global_sequence": int(
                row["target_floor_global_sequence"]
            ),
            "pruned_through_segment_id": str(row["target_floor_segment_id"]),
            "pruned_through_segment_ordinal": int(row["target_floor_ordinal"]),
            "reclaimed_completion_count": int(
                row["expected_reclaimed_completion_count"]
            ),
            "reclaimed_file_bytes": int(row["expected_reclaimed_file_bytes"]),
            "reclaimed_segment_count": int(
                row["expected_reclaimed_segment_count"]
            ),
        }
        expected_floor_sha = reclamation_digest_mapping(expected_floor_after)
        if (
            expected_floor_sha != str(row["expected_floor_after_sha256"])
            or effect.get("target_floor_after_sha256") != expected_floor_sha
        ):
            raise AcquisitionEvidenceIntegrityError(
                "lifecycle expected floor authority differs"
            )
        segment_items = effect.get("segments")
        if not isinstance(segment_items, list):
            raise AcquisitionEvidenceIntegrityError(
                "lifecycle reclamation segment catalog differs"
            )
        segments: list[dict[str, Any]] = []
        previous_id = floor_before.get("pruned_through_segment_id")
        previous_file = floor_before.get("pruned_through_file_sha256")
        previous_ordinal = int(floor_before["pruned_through_segment_ordinal"])
        for value in segment_items:
            if not isinstance(value, dict):
                raise AcquisitionEvidenceIntegrityError(
                    "lifecycle reclamation segment catalog differs"
                )
            core = self._segment_catalog_core(value)
            expected_catalog_sha = reclamation_digest_mapping({
                **core,
                "schema": (
                    "stv3-worker-lifecycle-reclamation-segment-catalog-v1"
                ),
            })
            if (
                value["catalog_row_sha256"] != expected_catalog_sha
                or int(core["segment_ordinal"]) != previous_ordinal + 1
                or core["previous_segment_id"] != previous_id
                or core["previous_file_sha256"] != previous_file
                or core["filename"] != f"{core['segment_id']}.sealed.json"
                or int(core["completion_count"]) < 1
                or int(core["first_global_sequence"])
                > int(core["last_global_sequence"])
            ):
                raise AcquisitionEvidenceIntegrityError(
                    "lifecycle reclamation segment chain differs"
                )
            require_reclamation_sha256(
                str(core["file_sha256"]),
                "lifecycle segment file SHA-256",
            )
            require_reclamation_sha256(
                str(core["manifest_sha256"]),
                "lifecycle segment manifest SHA-256",
            )
            segments.append(dict(value))
            previous_id = core["segment_id"]
            previous_file = core["file_sha256"]
            previous_ordinal = int(core["segment_ordinal"])
        if (
            len(segments) != int(row["target_segment_count"])
            or not segments
            or previous_id != str(row["target_floor_segment_id"])
            or previous_file != str(row["target_floor_file_sha256"])
            or previous_ordinal != int(row["target_floor_ordinal"])
            or int(segments[-1]["last_global_sequence"])
            != int(row["target_floor_global_sequence"])
            or sum(int(item["completion_count"]) for item in segments)
            != len(members)
        ):
            raise AcquisitionEvidenceIntegrityError(
                "lifecycle reclamation segment inventory differs"
            )
        expected_object_effect_binding = (
            self._object_reference_effect_binding(
                archive_indexes=archive_indexes,
                files=files,
                floor_before_sha256=floor_before_sha,
                object_reference_cores=[
                    self._object_reference_core(item)
                    for item in references
                ],
                request_sha256=request_sha,
                segments=segments,
                target_floor_after_sha256=expected_floor_sha,
            )
        )
        if any(
            item["request_sha256"] != request_sha
            or item["effect_binding_sha256"]
            != expected_object_effect_binding
            for item in references
        ):
            raise AcquisitionEvidenceIntegrityError(
                "lifecycle reclamation object effect binding differs"
            )
        by_segment: dict[str, list[dict[str, Any]]] = {
            str(item["segment_id"]): [] for item in segments
        }
        for indexed in archive_indexes:
            target = by_segment.get(str(indexed["segment_id"]))
            if target is None:
                raise AcquisitionEvidenceIntegrityError(
                    "lifecycle reclamation archive-index segment differs"
                )
            target.append(indexed)
        for segment in segments:
            indexed = sorted(
                by_segment[str(segment["segment_id"])],
                key=lambda item: item["global_sequence"],
            )
            if (
                len(indexed) != int(segment["completion_count"])
                or indexed[0]["global_sequence"]
                != int(segment["first_global_sequence"])
                or indexed[-1]["global_sequence"]
                != int(segment["last_global_sequence"])
                or tuple(item["global_sequence"] for item in indexed)
                != tuple(range(
                    int(segment["first_global_sequence"]),
                    int(segment["last_global_sequence"]) + 1,
                ))
            ):
                raise AcquisitionEvidenceIntegrityError(
                    "lifecycle reclamation segment record range differs"
                )
        content_counts: dict[str, int] = {}
        for indexed in archive_indexes:
            content = str(indexed["content_sha256"])
            content_counts[content] = content_counts.get(content, 0) + 1
        if (
            set(content_counts)
            != {str(item["content_sha256"]) for item in references}
            or any(
                content_counts[str(item["content_sha256"])]
                != int(item["target_reference_count"])
                for item in references
            )
        ):
            raise AcquisitionEvidenceIntegrityError(
                "lifecycle reclamation object-reference inventory differs"
            )
        expected_files = sorted(
            [
                {
                    "content_sha256": str(item["file_sha256"]),
                    "file_kind": "segment",
                    "filename": str(item["filename"]),
                    "identity_sha256": str(item["segment_id"]),
                    "size_bytes": int(item["file_bytes"]),
                }
                for item in segments
            ] + [
                {
                    "content_sha256": str(item["content_sha256"]),
                    "file_kind": "object",
                    "filename": str(item["filename"]),
                    "identity_sha256": str(item["content_sha256"]),
                    "size_bytes": int(item["size_bytes"]),
                }
                for item in references
                if bool(item["delete_file"])
            ],
            key=lambda item: (item["file_kind"], item["identity_sha256"]),
        )
        source_view = reclamation_digest_mapping({
            "candidate_authority_sha256": self.candidate_authority_sha256,
            "candidate_bindings_sha256": request.candidate_bindings_sha256,
            "floor_before_sha256": floor_before_sha,
            "effect_sha256": effect_sha,
            "members": [item.as_dict() for item in members],
            "schema": "stv3-worker-lifecycle-reclamation-source-view-v1",
            "source_store_identity_sha256": self.source_store_identity_sha256,
        })
        source_fence = reclamation_digest_mapping({
            "fence_epoch": int(row["fence_epoch"]),
            "floor_before_sha256": floor_before_sha,
            "request_sha256": request_sha,
            "schema": RECLAMATION_FENCE_SCHEMA,
            "source_store_identity_sha256": self.source_store_identity_sha256,
            "source_view_sha256": source_view,
        })
        if (
            list(files) != expected_files
            or int(row["releasable_bytes"])
            != sum(item["size_bytes"] for item in expected_files)
            or int(row["releasable_entries"]) != len(members)
            or source_view != str(row["source_view_sha256"])
            or source_fence != str(row["source_fence_sha256"])
        ):
            raise AcquisitionEvidenceIntegrityError(
                "lifecycle reclamation sealed journal differs"
            )
        return {
            "archive_indexes": tuple(archive_indexes),
            "effect": effect,
            "expected_floor_after": expected_floor_after,
            "files": files,
            "floor_before": floor_before,
            "members": members,
            "object_references": references,
            "prepared": prepared,
            "request": request,
            "segments": tuple(segments),
        }

    def _journal_file_path(self, item: Mapping[str, Any]) -> Path:
        directory = (
            self._store.archive_segment_directory
            if item["file_kind"] == "segment"
            else self._store.archive_object_directory
        )
        return directory / str(item["filename"])

    @staticmethod
    def _segment_row_matches_catalog(
        row: sqlite3.Row,
        catalog: Mapping[str, Any],
    ) -> bool:
        return (
            str(row["segment_id"]) == catalog["segment_id"]
            and int(row["segment_ordinal"]) == catalog["segment_ordinal"]
            and row["previous_segment_id"] == catalog["previous_segment_id"]
            and row["previous_file_sha256"]
            == catalog["previous_file_sha256"]
            and int(row["first_global_sequence"])
            == catalog["first_global_sequence"]
            and int(row["last_global_sequence"])
            == catalog["last_global_sequence"]
            and int(row["completion_count"]) == catalog["completion_count"]
            and str(row["content_sha256"]) == catalog["content_sha256"]
            and str(row["file_sha256"]) == catalog["file_sha256"]
            and str(row["filename"]) == catalog["filename"]
            and int(row["file_bytes"]) == catalog["file_bytes"]
            and hashlib.sha256(bytes(row["manifest_json"])).hexdigest()
            == catalog["manifest_sha256"]
            and float(row["created_at"]) == catalog["created_at"]
        )

    @staticmethod
    def _archive_index_row_matches(
        row: sqlite3.Row,
        indexed: Mapping[str, Any],
    ) -> bool:
        return all((
            str(row["acquisition_job_id"])
            == indexed["acquisition_job_id"],
            str(row["consumer_id"]) == indexed["consumer_id"],
            str(row["content_sha256"]) == indexed["content_sha256"],
            str(row["event_id"]) == indexed["event_id"],
            str(row["event_sha256"]) == indexed["event_sha256"],
            int(row["global_sequence"]) == indexed["global_sequence"],
            str(row["producer_receipt_sha256"])
            == indexed["producer_receipt_sha256"],
            str(row["segment_id"]) == indexed["segment_id"],
            str(row["stream_id"]) == indexed["stream_id"],
            int(row["stream_sequence"]) == indexed["stream_sequence"],
        ))

    def _verify_reclaimed_semantic_absence(
        self,
        db: sqlite3.Connection,
        journal: Mapping[str, Any],
    ) -> None:
        for member in journal["members"]:
            if db.execute(
                "SELECT 1 FROM archive_index WHERE event_id=? OR event_sha256=? "
                "OR producer_receipt_sha256=? OR acquisition_job_id=? OR "
                "(stream_id=? AND stream_sequence=?) LIMIT 1",
                (
                    member.event_id,
                    member.event_sha256,
                    member.producer_receipt_sha256,
                    member.acquisition_job_id,
                    member.stream_id,
                    member.stream_sequence,
                ),
            ).fetchone() is not None:
                raise AcquisitionEvidenceIntegrityError(
                    "reclaimed lifecycle archive index remains"
                )
            if db.execute(
                "SELECT 1 FROM producer_receipts WHERE "
                "producer_receipt_sha256=? OR acquisition_job_id=? OR "
                "(stream_id=? AND stream_sequence=?) LIMIT 1",
                (
                    member.producer_receipt_sha256,
                    member.acquisition_job_id,
                    member.stream_id,
                    member.stream_sequence,
                ),
            ).fetchone() is not None:
                raise AcquisitionEvidenceIntegrityError(
                    "reclaimed lifecycle producer identity remains"
                )
            if db.execute(
                "SELECT 1 FROM completion_outbox WHERE event_id=? OR "
                "event_sha256=? OR producer_receipt_sha256=? OR "
                "(consumer_id=? AND stream_id=? AND stream_sequence=?) LIMIT 1",
                (
                    member.event_id,
                    member.event_sha256,
                    member.producer_receipt_sha256,
                    member.consumer_id,
                    member.stream_id,
                    member.stream_sequence,
                ),
            ).fetchone() is not None:
                raise AcquisitionEvidenceIntegrityError(
                    "reclaimed lifecycle completion identity remains"
                )
            for table in (
                "consumption_receipts",
                "rejection_receipts",
                "quarantine_receipts",
            ):
                if db.execute(
                    f"SELECT 1 FROM {table} WHERE event_id=? OR "
                    "event_sha256=? OR producer_receipt_sha256=? OR "
                    "(consumer_id=? AND stream_id=? AND stream_sequence=?) "
                    "LIMIT 1",
                    (
                        member.event_id,
                        member.event_sha256,
                        member.producer_receipt_sha256,
                        member.consumer_id,
                        member.stream_id,
                        member.stream_sequence,
                    ),
                ).fetchone() is not None:
                    raise AcquisitionEvidenceIntegrityError(
                        "reclaimed lifecycle terminal child remains"
                    )

    def _verify_applied_object_reuse(
        self,
        db: sqlite3.Connection,
        reference: Mapping[str, Any],
        floor_ordinal: int,
    ) -> bool:
        object_row = db.execute(
            "SELECT * FROM archive_objects WHERE content_sha256=?",
            (reference["content_sha256"],),
        ).fetchone()
        path = self._store.archive_object_directory / str(reference["filename"])
        if object_row is None:
            if path.exists() or path.is_symlink():
                raise AcquisitionEvidenceIntegrityError(
                    "reclaimed lifecycle object bytes are orphaned"
                )
            return False
        if (
            int(object_row["size_bytes"]) != reference["size_bytes"]
            or str(object_row["filename"]) != reference["filename"]
        ):
            raise AcquisitionEvidenceIntegrityError(
                "reused lifecycle archive object identity differs"
            )
        retained = int(db.execute(
            "SELECT count(*) FROM archive_index AS indexed JOIN archive_segments "
            "AS segment USING(segment_id) WHERE indexed.content_sha256=? AND "
            "segment.segment_ordinal>?",
            (reference["content_sha256"], floor_ordinal),
        ).fetchone()[0])
        if retained < 1:
            raise AcquisitionEvidenceIntegrityError(
                "reused lifecycle archive object lacks a retained reference"
            )
        self._store._verified_file(
            path,
            str(reference["content_sha256"]),
            int(reference["size_bytes"]),
        )
        return True

    def _verify_reclamation_state(
        self,
        db: sqlite3.Connection,
        row: sqlite3.Row,
        *,
        require_files_absent: bool,
    ) -> dict[str, Any]:
        journal = self._load_reclamation_journal(db, row)
        state = str(row["state"])
        if state not in RECLAMATION_STATES:
            raise AcquisitionEvidenceIntegrityError(
                "lifecycle reclamation decision state differs"
            )
        if state == "PREPARED":
            invalid = (
                row["active_slot"] != 1
                or row["committed_decision_sha256"] is not None
                or row["committed_receipt_sha256"] is not None
                or row["apply_receipt_sha256"] is not None
                or row["apply_receipt_json"] is not None
                or row["abort_receipt_json"] is not None
            )
        elif state == "APPLYING":
            invalid = (
                row["active_slot"] != 1
                or row["committed_decision_sha256"] is None
                or row["committed_receipt_sha256"] is None
                or row["apply_receipt_sha256"] is not None
                or row["apply_receipt_json"] is not None
                or row["abort_receipt_json"] is not None
            )
        elif state == "APPLIED":
            invalid = (
                row["active_slot"] is not None
                or row["committed_decision_sha256"] is None
                or row["committed_receipt_sha256"] is None
                or row["apply_receipt_sha256"] is None
                or row["apply_receipt_json"] is None
                or row["post_apply_view_sha256"] is None
                or row["applied_at"] is None
                or row["abort_receipt_json"] is not None
            )
        else:
            invalid = (
                row["active_slot"] is not None
                or row["committed_decision_sha256"] is not None
                or row["committed_receipt_sha256"] is not None
                or row["apply_receipt_json"] is not None
                or row["abort_decision_sha256"] is None
                or row["fence_release_root_sha256"] is None
                or row["abort_receipt_json"] is None
                or row["aborted_at"] is None
            )
        if invalid:
            raise AcquisitionEvidenceIntegrityError(
                "lifecycle reclamation decision fields differ"
            )
        if state == "ABORTED":
            return journal
        floor = db.execute(
            "SELECT * FROM lifecycle_archive_floor WHERE singleton=1"
        ).fetchone()
        if floor is None:
            raise AcquisitionEvidenceIntegrityError(
                "lifecycle archive floor is unavailable"
            )
        floor_mapping = self._floor_mapping(floor)
        expected_after = journal["expected_floor_after"]
        if state == "PREPARED":
            if floor_mapping != journal["floor_before"]:
                raise AcquisitionEvidenceConflict(
                    "lifecycle archive floor changed under PREPARED"
                )
        elif state == "APPLYING":
            if floor_mapping != expected_after:
                raise AcquisitionEvidenceIntegrityError(
                    "lifecycle APPLYING floor differs"
                )
        else:
            own_applied_floor = {
                **expected_after,
                "last_apply_receipt_sha256": str(
                    row["apply_receipt_sha256"]
                ),
            }
            if int(floor_mapping["pruned_through_segment_ordinal"]) < int(
                own_applied_floor["pruned_through_segment_ordinal"]
            ) or any(
                int(floor_mapping[key]) < int(own_applied_floor[key])
                for key in (
                    "pruned_through_global_sequence",
                    "reclaimed_completion_count",
                    "reclaimed_segment_count",
                    "reclaimed_file_bytes",
                )
            ):
                raise AcquisitionEvidenceIntegrityError(
                    "lifecycle APPLIED floor regressed"
                )
            if (
                int(floor_mapping["pruned_through_segment_ordinal"])
                == int(own_applied_floor["pruned_through_segment_ordinal"])
                and floor_mapping != own_applied_floor
            ):
                raise AcquisitionEvidenceIntegrityError(
                    "lifecycle APPLIED floor edge differs"
                )
        for catalog in journal["segments"]:
            segment_row = db.execute(
                "SELECT * FROM archive_segments WHERE segment_id=?",
                (catalog["segment_id"],),
            ).fetchone()
            if segment_row is None or not self._segment_row_matches_catalog(
                segment_row,
                catalog,
            ):
                raise AcquisitionEvidenceIntegrityError(
                    "lifecycle reclamation segment tombstone differs"
                )
        if state == "PREPARED":
            for indexed in journal["archive_indexes"]:
                indexed_row = db.execute(
                    "SELECT * FROM archive_index WHERE event_id=?",
                    (indexed["event_id"],),
                ).fetchone()
                if indexed_row is None or not self._archive_index_row_matches(
                    indexed_row,
                    indexed,
                ):
                    raise AcquisitionEvidenceConflict(
                        "lifecycle PREPARED archive index changed"
                    )
        else:
            self._verify_reclaimed_semantic_absence(db, journal)
        current_floor_ordinal = int(
            floor_mapping["pruned_through_segment_ordinal"]
        )
        for reference in journal["object_references"]:
            total_references = int(db.execute(
                "SELECT count(*) FROM archive_index WHERE content_sha256=?",
                (reference["content_sha256"],),
            ).fetchone()[0])
            object_row = db.execute(
                "SELECT * FROM archive_objects WHERE content_sha256=?",
                (reference["content_sha256"],),
            ).fetchone()
            if state == "PREPARED":
                if (
                    total_references
                    != reference["target_reference_count"]
                    + reference["retained_reference_count"]
                    or object_row is None
                    or int(object_row["size_bytes"]) != reference["size_bytes"]
                    or str(object_row["filename"]) != reference["filename"]
                ):
                    raise AcquisitionEvidenceConflict(
                        "lifecycle PREPARED object reference changed"
                    )
            elif state == "APPLYING":
                if total_references != reference["retained_reference_count"]:
                    raise AcquisitionEvidenceIntegrityError(
                        "lifecycle APPLYING object reference differs"
                    )
                if reference["delete_file"]:
                    if object_row is not None:
                        raise AcquisitionEvidenceIntegrityError(
                            "lifecycle reclaimed object catalog remains"
                        )
                elif (
                    object_row is None
                    or int(object_row["size_bytes"]) != reference["size_bytes"]
                    or str(object_row["filename"]) != reference["filename"]
                ):
                    raise AcquisitionEvidenceIntegrityError(
                        "lifecycle retained object catalog differs"
                    )
            else:
                self._verify_applied_object_reuse(
                    db,
                    reference,
                    current_floor_ordinal,
                )
        for item in journal["files"]:
            path = self._journal_file_path(item)
            present = path.exists() or path.is_symlink()
            if state == "APPLIED" and item["file_kind"] == "object":
                reference = next(
                    ref for ref in journal["object_references"]
                    if ref["content_sha256"] == item["identity_sha256"]
                )
                reused = self._verify_applied_object_reuse(
                    db,
                    reference,
                    current_floor_ordinal,
                )
                if present != reused:
                    raise AcquisitionEvidenceIntegrityError(
                        "lifecycle applied object-file authority differs"
                    )
                continue
            if require_files_absent:
                if present:
                    raise AcquisitionEvidenceDurabilityError(
                        "lifecycle reclaimed file is still present"
                    )
            elif present:
                self._store._verified_file(
                    path,
                    str(item["content_sha256"]),
                    int(item["size_bytes"]),
                )
        if state == "APPLIED":
            receipt = self._apply_from_blob(row["apply_receipt_json"])
            logical_post_view = reclamation_digest_mapping({
                "floor": expected_after,
                "request_sha256": journal["request"].request_sha256,
                "schema": RECLAMATION_POST_APPLY_SCHEMA,
                "source_store_identity_sha256": (
                    self.source_store_identity_sha256
                ),
            })
            if (
                receipt.request_sha256 != journal["request"].request_sha256
                or receipt.role != self.role
                or receipt.source_store_identity_sha256
                != self.source_store_identity_sha256
                or receipt.prepare_receipt_sha256
                != journal["prepared"].prepare_receipt_sha256
                or receipt.committed_decision_sha256
                != str(row["committed_decision_sha256"])
                or receipt.committed_decision_receipt_sha256
                != str(row["committed_receipt_sha256"])
                or receipt.predecessor_apply_receipt_sha256 is not None
                or receipt.effect_sha256 != str(row["effect_sha256"])
                or receipt.post_apply_view_sha256 != logical_post_view
                or logical_post_view != str(row["post_apply_view_sha256"])
                or receipt.released_bytes != int(row["releasable_bytes"])
                or receipt.released_entries != int(row["releasable_entries"])
                or receipt.applied_at != float(row["applied_at"])
                or receipt.apply_receipt_sha256
                != str(row["apply_receipt_sha256"])
            ):
                raise AcquisitionEvidenceIntegrityError(
                    "lifecycle APPLIED receipt authority differs"
                )
            journal["apply_receipt"] = receipt
        return journal

    def prepare_source_local_v2(
        self,
        request: SourceReclamationRequestV2,
    ) -> SourceLocalPrepareReceiptV2:
        with self._held_members(request) as members:
            return self._prepare_source_local_held(request, members)

    def _prepare_source_local_held(
        self,
        request: SourceReclamationRequestV2,
        members: tuple[LifecycleReclamationArchiveMemberV2, ...],
    ) -> SourceLocalPrepareReceiptV2:
        store = self._store
        with store._lock, closing(store._connect()) as db:
            try:
                db.execute("BEGIN IMMEDIATE")
                self._validate_reclamation_authority(db)
                snapshot = self._snapshot(request, members, database=db)
                existing = db.execute(
                    "SELECT * FROM lifecycle_reclamation_decisions "
                    "WHERE request_sha256=?",
                    (request.request_sha256,),
                ).fetchone()
                if existing is not None:
                    journal = self._verify_reclamation_state(
                        db,
                        existing,
                        require_files_absent=str(existing["state"]) == "APPLIED",
                    )
                    receipt = journal["prepared"]
                    if (
                        bytes(existing["request_json"])
                        != reclamation_canonical_bytes(request.as_dict())
                        or receipt.source_view_sha256
                        != snapshot["source_view_sha256"]
                        or receipt.child_reference_root_sha256
                        != snapshot["child_reference_root_sha256"]
                        or str(existing["effect_sha256"])
                        != snapshot["effect_sha256"]
                    ):
                        raise AcquisitionEvidenceConflict(
                            "lifecycle prepare identity was reused"
                        )
                    db.commit()
                    return receipt
                active = db.execute(
                    "SELECT request_sha256 FROM lifecycle_reclamation_decisions "
                    "WHERE active_slot=1"
                ).fetchone()
                if active is not None:
                    raise AcquisitionEvidenceConflict(
                        "lifecycle reclamation fence is held"
                    )
                meta = db.execute(
                    "SELECT * FROM lifecycle_reclamation_meta WHERE singleton=1"
                ).fetchone()
                if meta is None:
                    raise AcquisitionEvidenceIntegrityError(
                        "lifecycle reclamation metadata is unavailable"
                    )
                observed_at = _finite(store._clock(), "reclamation prepare clock")
                last = meta["last_observed_at"]
                if last is not None and observed_at < float(last):
                    raise AcquisitionEvidenceConflict(
                        "lifecycle reclamation clock rollback"
                    )
                fence_epoch = int(meta["next_fence_epoch"])
                source_fence = reclamation_digest_mapping({
                    "fence_epoch": fence_epoch,
                    "floor_before_sha256": snapshot["floor_before_sha256"],
                    "request_sha256": request.request_sha256,
                    "schema": RECLAMATION_FENCE_SCHEMA,
                    "source_store_identity_sha256": (
                        self.source_store_identity_sha256
                    ),
                    "source_view_sha256": snapshot["source_view_sha256"],
                })
                receipt = SourceLocalPrepareReceiptV2.create(
                    request_sha256=request.request_sha256,
                    role=self.role,
                    source_store_identity_sha256=(
                        self.source_store_identity_sha256
                    ),
                    coordinator_authority_sha256=(
                        self.coordinator_authority_sha256
                    ),
                    candidate_authority_sha256=(
                        self.candidate_authority_sha256
                    ),
                    sealed_archive_anchor_sha256=(
                        request.sealed_archive_anchor_sha256
                    ),
                    archive_binding_root_sha256=(
                        request.archive_binding_root_sha256
                    ),
                    candidate_bindings_sha256=(
                        request.candidate_bindings_sha256
                    ),
                    consumer_registry_root_sha256=(
                        request.consumer_registry_root_sha256
                    ),
                    consumer_epoch_root_sha256=(
                        request.consumer_epoch_root_sha256
                    ),
                    source_view_sha256=snapshot["source_view_sha256"],
                    child_reference_root_sha256=(
                        snapshot["child_reference_root_sha256"]
                    ),
                    source_fence_sha256=source_fence,
                    candidate_count=request.candidate_count,
                    releasable_bytes=snapshot["releasable_bytes"],
                    releasable_entries=snapshot["releasable_entries"],
                    prepared_at=observed_at,
                )
                db.execute(
                    "INSERT INTO lifecycle_reclamation_decisions("
                    "request_sha256,state,active_slot,request_json,"
                    "prepare_receipt_sha256,prepare_receipt_json,"
                    "source_fence_sha256,source_view_sha256,"
                    "child_reference_root_sha256,floor_before_sha256,"
                    "floor_before_json,prior_apply_receipt_sha256,"
                    "expected_floor_after_sha256,"
                    "expected_reclaimed_completion_count,"
                    "expected_reclaimed_segment_count,"
                    "expected_reclaimed_file_bytes,"
                    "target_floor_ordinal,target_floor_segment_id,"
                    "target_floor_file_sha256,target_floor_global_sequence,"
                    "target_segment_count,"
                    "candidate_count,releasable_bytes,releasable_entries,"
                    "coordinator_authority_sha256,effect_json,effect_sha256,"
                    "prepared_at,fence_epoch,journal_sealed) VALUES("
                    ":request_sha256,'PREPARED',1,:request_json,"
                    ":prepare_receipt_sha256,:prepare_receipt_json,"
                    ":source_fence_sha256,:source_view_sha256,"
                    ":child_reference_root_sha256,:floor_before_sha256,"
                    ":floor_before_json,:prior_apply_receipt_sha256,"
                    ":expected_floor_after_sha256,"
                    ":expected_reclaimed_completion_count,"
                    ":expected_reclaimed_segment_count,"
                    ":expected_reclaimed_file_bytes,"
                    ":target_floor_ordinal,:target_floor_segment_id,"
                    ":target_floor_file_sha256,:target_floor_global_sequence,"
                    ":target_segment_count,"
                    ":candidate_count,:releasable_bytes,:releasable_entries,"
                    ":coordinator_authority_sha256,:effect_json,:effect_sha256,"
                    ":prepared_at,:fence_epoch,0)",
                    {
                        "candidate_count": request.candidate_count,
                        "child_reference_root_sha256": (
                            receipt.child_reference_root_sha256
                        ),
                        "coordinator_authority_sha256": (
                            self.coordinator_authority_sha256
                        ),
                        "effect_json": sqlite3.Binary(
                            reclamation_canonical_bytes(snapshot["effect"])
                        ),
                        "effect_sha256": snapshot["effect_sha256"],
                        "expected_floor_after_sha256": (
                            snapshot["expected_floor_after_sha256"]
                        ),
                        "expected_reclaimed_completion_count": (
                            snapshot["expected_floor_after"]
                            ["reclaimed_completion_count"]
                        ),
                        "expected_reclaimed_file_bytes": (
                            snapshot["expected_floor_after"]
                            ["reclaimed_file_bytes"]
                        ),
                        "expected_reclaimed_segment_count": (
                            snapshot["expected_floor_after"]
                            ["reclaimed_segment_count"]
                        ),
                        "fence_epoch": fence_epoch,
                        "floor_before_sha256": snapshot["floor_before_sha256"],
                        "floor_before_json": sqlite3.Binary(
                            reclamation_canonical_bytes(snapshot["floor_before"])
                        ),
                        "prior_apply_receipt_sha256": snapshot["floor_before"]
                        ["last_apply_receipt_sha256"],
                        "prepare_receipt_json": sqlite3.Binary(
                            reclamation_canonical_bytes(receipt.as_dict())
                        ),
                        "prepare_receipt_sha256": (
                            receipt.prepare_receipt_sha256
                        ),
                        "prepared_at": observed_at,
                        "releasable_bytes": snapshot["releasable_bytes"],
                        "releasable_entries": snapshot["releasable_entries"],
                        "request_json": sqlite3.Binary(
                            reclamation_canonical_bytes(request.as_dict())
                        ),
                        "request_sha256": request.request_sha256,
                        "source_fence_sha256": receipt.source_fence_sha256,
                        "source_view_sha256": receipt.source_view_sha256,
                        "target_floor_file_sha256": (
                            snapshot["target_floor_file_sha256"]
                        ),
                        "target_floor_global_sequence": (
                            snapshot["target_floor_global_sequence"]
                        ),
                        "target_floor_ordinal": snapshot["target_floor_ordinal"],
                        "target_floor_segment_id": (
                            snapshot["target_floor_segment_id"]
                        ),
                        "target_segment_count": snapshot["target_segment_count"],
                    },
                )
                for member in members:
                    indexed = snapshot["archive_index_by_event"][member.event_id]
                    db.execute(
                        "INSERT INTO lifecycle_reclamation_members("
                        "request_sha256,candidate_sha256,acquisition_job_id,"
                        "event_id,event_sha256,producer_receipt_sha256,consumer_id,"
                        "stream_id,stream_sequence,archive_global_sequence,"
                        "archive_content_sha256,archive_index_sha256,"
                        "terminal_identity_sha256,"
                        "lifecycle_archive_segment_id,retention_anchor_sha256,"
                        "retention_segment_sha256,retention_record_sha256,"
                        "archive_metadata_sha256,"
                        "archive_object_reference_root_sha256,"
                        "archive_object_count,archive_object_total_bytes,"
                        "member_sha256) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (
                            request.request_sha256,
                            member.candidate_sha256,
                            member.acquisition_job_id,
                            member.event_id,
                            member.event_sha256,
                            member.producer_receipt_sha256,
                            member.consumer_id,
                            member.stream_id,
                            member.stream_sequence,
                            indexed["global_sequence"],
                            indexed["content_sha256"],
                            indexed["archive_index_sha256"],
                            member.terminal_identity_sha256,
                            member.lifecycle_archive_segment_id,
                            member.retention_anchor_sha256,
                            member.retention_segment_sha256,
                            member.retention_record_sha256,
                            member.archive_metadata_sha256,
                            member.archive_object_reference_root_sha256,
                            member.archive_object_count,
                            member.archive_object_total_bytes,
                            self._member_sha256(member),
                        ),
                    )
                for item in snapshot["files"]:
                    file_sha = reclamation_digest_mapping({
                        **item,
                        "request_sha256": request.request_sha256,
                        "schema": RECLAMATION_FILE_SCHEMA,
                    })
                    db.execute(
                        "INSERT INTO lifecycle_reclamation_files VALUES(?,?,?,?,?,?,?)",
                        (
                            request.request_sha256,
                            item["file_kind"],
                            item["identity_sha256"],
                            item["content_sha256"],
                            item["size_bytes"],
                            item["filename"],
                            file_sha,
                        ),
                    )
                for reference in snapshot["object_references"]:
                    db.execute(
                        "INSERT INTO lifecycle_reclamation_object_references "
                        "VALUES(?,?,?,?,?,?,?,?,?,?)",
                        (
                            request.request_sha256,
                            reference["content_sha256"],
                            reference["size_bytes"],
                            reference["filename"],
                            reference["target_reference_count"],
                            reference["retained_reference_count"],
                            int(reference["delete_file"]),
                            reference["catalog_row_sha256"],
                            reference["effect_binding_sha256"],
                            reference["reference_sha256"],
                        ),
                    )
                changed = db.execute(
                    "UPDATE lifecycle_reclamation_decisions SET journal_sealed=1 "
                    "WHERE request_sha256=? AND state='PREPARED' AND "
                    "journal_sealed=0",
                    (request.request_sha256,),
                ).rowcount
                if changed != 1:
                    raise AcquisitionEvidenceIntegrityError(
                        "lifecycle reclamation journal seal failed"
                    )
                changed = db.execute(
                    "UPDATE lifecycle_reclamation_meta SET next_fence_epoch=?,"
                    "last_observed_at=? WHERE singleton=1",
                    (fence_epoch + 1, observed_at),
                ).rowcount
                if changed != 1:
                    raise AcquisitionEvidenceIntegrityError(
                        "lifecycle reclamation fence epoch update failed"
                    )
                sealed = db.execute(
                    "SELECT * FROM lifecycle_reclamation_decisions "
                    "WHERE request_sha256=?",
                    (request.request_sha256,),
                ).fetchone()
                if sealed is None:
                    raise AcquisitionEvidenceIntegrityError(
                        "lifecycle reclamation journal disappeared"
                    )
                self._verify_reclamation_state(
                    db,
                    sealed,
                    require_files_absent=False,
                )
                store._fault("reclamation.prepare.before_commit")
                db.commit()
                store._fault("reclamation.prepare.after_commit")
            except BaseException:
                if db.in_transaction:
                    db.rollback()
                raise
        store._directory_fsync(store.root)
        return receipt

    def load_source_local_prepare_v2(
        self,
        request_sha256: str,
    ) -> SourceLocalPrepareReceiptV2 | None:
        request = require_reclamation_sha256(
            request_sha256, "lifecycle prepare request SHA-256"
        )
        self._assert_bound_authorities()
        with self._store._lock, closing(self._store._connect()) as db:
            try:
                db.execute("BEGIN")
                self._validate_reclamation_authority(db)
                row = db.execute(
                    "SELECT * FROM lifecycle_reclamation_decisions "
                    "WHERE request_sha256=?",
                    (request,),
                ).fetchone()
                if row is None:
                    db.commit()
                    return None
                journal = self._verify_reclamation_state(
                    db,
                    row,
                    require_files_absent=str(row["state"]) == "APPLIED",
                )
                db.commit()
            except BaseException:
                if db.in_transaction:
                    db.rollback()
                raise
        receipt = journal["prepared"]
        if receipt.request_sha256 != request:
            raise AcquisitionEvidenceIntegrityError(
                "lifecycle prepare receipt index differs"
            )
        return receipt

    def _load_decision_row(self, request_sha256: str) -> sqlite3.Row | None:
        with self._store._lock, closing(self._store._connect()) as db:
            return db.execute(
                "SELECT * FROM lifecycle_reclamation_decisions "
                "WHERE request_sha256=?",
                (request_sha256,),
            ).fetchone()

    def load_source_local_apply_v1(
        self,
        request_sha256: str,
    ) -> SourceLocalApplyReceiptV1 | None:
        request = require_reclamation_sha256(
            request_sha256, "lifecycle apply request SHA-256"
        )
        self._assert_bound_authorities()
        with self._store._lock, closing(self._store._connect()) as db:
            try:
                db.execute("BEGIN")
                self._validate_reclamation_authority(db)
                row = db.execute(
                    "SELECT * FROM lifecycle_reclamation_decisions "
                    "WHERE request_sha256=?",
                    (request,),
                ).fetchone()
                if row is None or row["apply_receipt_json"] is None:
                    db.commit()
                    return None
                if str(row["state"]) != "APPLIED":
                    raise AcquisitionEvidenceIntegrityError(
                        "lifecycle apply receipt state differs"
                    )
                journal = self._verify_reclamation_state(
                    db,
                    row,
                    require_files_absent=True,
                )
                db.commit()
            except BaseException:
                if db.in_transaction:
                    db.rollback()
                raise
        return journal["apply_receipt"]

    def load_source_local_abort_v1(
        self,
        request_sha256: str,
    ) -> SourceLocalAbortReceiptV1 | None:
        request = require_reclamation_sha256(
            request_sha256, "lifecycle abort request SHA-256"
        )
        self._assert_bound_authorities()
        with self._store._lock, closing(self._store._connect()) as db:
            try:
                db.execute("BEGIN")
                self._validate_reclamation_authority(db)
                row = db.execute(
                    "SELECT * FROM lifecycle_reclamation_decisions "
                    "WHERE request_sha256=?",
                    (request,),
                ).fetchone()
                if row is None or row["abort_receipt_json"] is None:
                    db.commit()
                    return None
                if str(row["state"]) != "ABORTED":
                    raise AcquisitionEvidenceIntegrityError(
                        "lifecycle abort receipt state differs"
                    )
                journal = self._verify_reclamation_state(
                    db,
                    row,
                    require_files_absent=False,
                )
                receipt = self._abort_from_blob(row["abort_receipt_json"])
                db.commit()
            except BaseException:
                if db.in_transaction:
                    db.rollback()
                raise
        if (
            receipt.request_sha256 != request
            or receipt.prepare_receipt_sha256
            != journal["prepared"].prepare_receipt_sha256
            or receipt.coordinator_abort_decision_sha256
            != str(row["abort_decision_sha256"])
            or receipt.fence_release_root_sha256
            != str(row["fence_release_root_sha256"])
            or receipt.aborted_at != float(row["aborted_at"])
        ):
            raise AcquisitionEvidenceIntegrityError(
                "lifecycle abort receipt index differs"
            )
        return receipt

    def abort_source_local_v1(
        self,
        request_sha256: str,
        *,
        expected_abort_decision_sha256: str,
    ) -> SourceLocalAbortReceiptV1:
        request = require_reclamation_sha256(
            request_sha256, "lifecycle abort request SHA-256"
        )
        abort_decision = require_reclamation_sha256(
            expected_abort_decision_sha256,
            "coordinator abort decision SHA-256",
        )
        self._assert_bound_authorities()
        store = self._store
        witness_row = self._load_decision_row(request)
        if witness_row is None:
            raise AcquisitionEvidenceConflict(
                "lifecycle prepare is unavailable"
            )
        witness_prepare = self._prepare_from_blob(
            witness_row["prepare_receipt_json"]
        )
        with self._committed.hold_abort_decision_v1(
            request,
            expected_abort_decision_sha256=abort_decision,
            aborting_role=self.role,
            expected_prepare_receipt_sha256=(
                witness_prepare.prepare_receipt_sha256
            ),
            expected_source_store_identity_sha256=(
                self.source_store_identity_sha256
            ),
            expected_source_fence_sha256=witness_prepare.source_fence_sha256,
        ) as held_abort:
            if (
                held_abort.request_sha256 != request
                or held_abort.abort_decision_sha256 != abort_decision
            ):
                raise AcquisitionEvidenceConflict(
                    "held coordinator abort decision differs"
                )
            with store._lock, closing(store._connect()) as db:
                try:
                    db.execute("BEGIN IMMEDIATE")
                    self._validate_reclamation_authority(db)
                    row = db.execute(
                        "SELECT * FROM lifecycle_reclamation_decisions "
                        "WHERE request_sha256=?",
                        (request,),
                    ).fetchone()
                    if row is None:
                        raise AcquisitionEvidenceConflict(
                            "lifecycle prepare is unavailable"
                        )
                    if str(row["state"]) == "ABORTED":
                        self._verify_reclamation_state(
                            db,
                            row,
                            require_files_absent=False,
                        )
                        existing = self._abort_from_blob(
                            row["abort_receipt_json"]
                        )
                        if (
                            existing.coordinator_abort_decision_sha256
                            != abort_decision
                        ):
                            raise AcquisitionEvidenceConflict(
                                "lifecycle abort decision differs"
                            )
                        db.commit()
                        return existing
                    if str(row["state"]) != "PREPARED":
                        raise AcquisitionEvidenceConflict(
                            "lifecycle apply cannot be aborted"
                        )
                    journal = self._verify_reclamation_state(
                        db,
                        row,
                        require_files_absent=False,
                    )
                    prepared = journal["prepared"]
                    observed = _finite(
                        store._clock(), "reclamation abort clock"
                    )
                    release_root = reclamation_digest_mapping({
                        "abort_decision_receipt_sha256": held_abort.receipt_sha256,
                        "abort_decision_sha256": abort_decision,
                        "request_sha256": request,
                        "schema": RECLAMATION_RELEASE_SCHEMA,
                        "source_fence_sha256": prepared.source_fence_sha256,
                    })
                    receipt = SourceLocalAbortReceiptV1.create(
                        request_sha256=request,
                        role=self.role,
                        source_store_identity_sha256=(
                            self.source_store_identity_sha256
                        ),
                        prepare_receipt_sha256=prepared.prepare_receipt_sha256,
                        coordinator_abort_decision_sha256=abort_decision,
                        fence_release_root_sha256=release_root,
                        aborted_at=observed,
                    )
                    changed = db.execute(
                        "UPDATE lifecycle_reclamation_decisions SET "
                        "state='ABORTED',active_slot=NULL,"
                        "abort_decision_sha256=?,fence_release_root_sha256=?,"
                        "abort_receipt_json=?,aborted_at=? WHERE "
                        "request_sha256=? AND state='PREPARED'",
                        (
                            abort_decision,
                            release_root,
                            sqlite3.Binary(
                                reclamation_canonical_bytes(receipt.as_dict())
                            ),
                            observed,
                            request,
                        ),
                    ).rowcount
                    if changed != 1:
                        raise AcquisitionEvidenceIntegrityError(
                            "lifecycle abort transition failed"
                        )
                    aborted = db.execute(
                        "SELECT * FROM lifecycle_reclamation_decisions "
                        "WHERE request_sha256=?",
                        (request,),
                    ).fetchone()
                    if aborted is None:
                        raise AcquisitionEvidenceIntegrityError(
                            "lifecycle abort journal disappeared"
                        )
                    self._verify_reclamation_state(
                        db,
                        aborted,
                        require_files_absent=False,
                    )
                    store._fault("reclamation.abort.before_commit")
                    db.commit()
                    store._fault("reclamation.abort.after_commit")
                except BaseException:
                    if db.in_transaction:
                        db.rollback()
                    raise
        store._directory_fsync(store.root)
        return receipt

    def _journal_files(
        self,
        request_sha256: str,
        *,
        database: sqlite3.Connection | None = None,
    ) -> tuple[dict[str, Any], ...]:
        connection = (
            closing(self._store._connect())
            if database is None
            else nullcontext(database)
        )
        with self._store._lock, connection as db:
            rows = db.execute(
                "SELECT * FROM lifecycle_reclamation_files "
                "WHERE request_sha256=? ORDER BY file_kind,identity_sha256",
                (request_sha256,),
            ).fetchall()
        files: list[dict[str, Any]] = []
        for row in rows:
            item = {
                "content_sha256": str(row["content_sha256"]),
                "file_kind": str(row["file_kind"]),
                "filename": str(row["filename"]),
                "identity_sha256": str(row["identity_sha256"]),
                "size_bytes": int(row["size_bytes"]),
            }
            file_sha = reclamation_digest_mapping({
                **item,
                "request_sha256": request_sha256,
                "schema": RECLAMATION_FILE_SCHEMA,
            })
            expected_name = (
                f"{item['identity_sha256']}.sealed.json"
                if item["file_kind"] == "segment"
                else f"{item['identity_sha256']}.raw"
            )
            if (
                str(row["request_sha256"]) != request_sha256
                or str(row["file_sha256"]) != file_sha
                or item["filename"] != expected_name
                or item["size_bytes"] < 0
                or item["file_kind"] not in {"segment", "object"}
            ):
                raise AcquisitionEvidenceIntegrityError(
                    "lifecycle reclamation file journal differs"
                )
            require_reclamation_sha256(
                item["content_sha256"],
                "lifecycle reclamation file content SHA-256",
            )
            require_reclamation_sha256(
                item["identity_sha256"],
                "lifecycle reclamation file identity SHA-256",
            )
            files.append(item)
        return tuple(files)

    def _unlink_journal_files(self, request_sha256: str) -> None:
        store = self._store
        for item in self._journal_files(request_sha256):
            with store._lock, closing(store._connect()) as proof_db:
                try:
                    proof_db.execute("BEGIN IMMEDIATE")
                    self._validate_reclamation_authority(proof_db)
                    row = proof_db.execute(
                        "SELECT * FROM lifecycle_reclamation_decisions "
                        "WHERE request_sha256=?",
                        (request_sha256,),
                    ).fetchone()
                    if row is None or str(row["state"]) != "APPLYING":
                        raise AcquisitionEvidenceIntegrityError(
                            "lifecycle APPLYING file authority disappeared"
                        )
                    journal = self._verify_reclamation_state(
                        proof_db,
                        row,
                        require_files_absent=False,
                    )
                    if item not in journal["files"]:
                        raise AcquisitionEvidenceIntegrityError(
                            "lifecycle file left its sealed journal"
                        )
                    proof_db.commit()
                except BaseException:
                    if proof_db.in_transaction:
                        proof_db.rollback()
                    raise
            path = self._journal_file_path(item)
            directory = path.parent
            if path.exists() or path.is_symlink():
                store._verified_file(
                    path, item["content_sha256"], item["size_bytes"]
                )
                store._fault("reclamation.apply.before_file_unlink")
                path.unlink()
                store._fault("reclamation.apply.after_file_unlink")
                store._directory_fsync(directory)
            if path.exists() or path.is_symlink():
                raise AcquisitionEvidenceDurabilityError(
                    "lifecycle reclaimed file remains"
                )
            with store._lock, closing(store._connect()) as reproof_db:
                try:
                    reproof_db.execute("BEGIN IMMEDIATE")
                    self._validate_reclamation_authority(reproof_db)
                    row = reproof_db.execute(
                        "SELECT * FROM lifecycle_reclamation_decisions "
                        "WHERE request_sha256=?",
                        (request_sha256,),
                    ).fetchone()
                    if row is None or str(row["state"]) != "APPLYING":
                        raise AcquisitionEvidenceIntegrityError(
                            "lifecycle APPLYING file authority disappeared"
                        )
                    self._verify_reclamation_state(
                        reproof_db,
                        row,
                        require_files_absent=False,
                    )
                    if path.exists() or path.is_symlink():
                        raise AcquisitionEvidenceDurabilityError(
                            "lifecycle reclaimed file returned"
                        )
                    reproof_db.commit()
                except BaseException:
                    if reproof_db.in_transaction:
                        reproof_db.rollback()
                    raise

    def apply_source_local_v1(
        self,
        request_sha256: str,
        *,
        predecessor_apply_receipt_sha256: str | None,
    ) -> SourceLocalApplyReceiptV1:
        request_sha = require_reclamation_sha256(
            request_sha256, "lifecycle apply request SHA-256"
        )
        if predecessor_apply_receipt_sha256 is not None:
            raise AcquisitionEvidenceIntegrityError(
                "downstream lifecycle is the first apply participant"
            )
        existing = self.load_source_local_apply_v1(request_sha)
        if existing is not None:
            return existing
        store = self._store
        with store._lock, closing(store._connect()) as witness_db:
            try:
                witness_db.execute("BEGIN")
                self._validate_reclamation_authority(witness_db)
                row = witness_db.execute(
                    "SELECT * FROM lifecycle_reclamation_decisions "
                    "WHERE request_sha256=?",
                    (request_sha,),
                ).fetchone()
                if row is None:
                    raise AcquisitionEvidenceConflict(
                        "lifecycle prepare is unavailable"
                    )
                journal = self._verify_reclamation_state(
                    witness_db,
                    row,
                    require_files_absent=str(row["state"]) == "APPLIED",
                )
                if str(row["state"]) == "ABORTED":
                    raise AcquisitionEvidenceConflict(
                        "lifecycle prepare was aborted"
                    )
                request = journal["request"]
                prepared = journal["prepared"]
                initial_state = str(row["state"])
                witness_db.commit()
            except BaseException:
                if witness_db.in_transaction:
                    witness_db.rollback()
                raise
        with self._committed.hold_committed_decision_v1(
            request_sha,
            applying_role=self.role,
            expected_predecessor_apply_receipt_sha256=None,
        ) as committed:
            binding = next(
                (
                    item for item in committed.participant_prepares
                    if item.role == self.role
                ),
                None,
            )
            if (
                committed.request_sha256 != request_sha
                or committed.sealed_archive_anchor_sha256
                != request.sealed_archive_anchor_sha256
                or committed.archive_binding_root_sha256
                != request.archive_binding_root_sha256
                or committed.candidate_bindings_sha256
                != request.candidate_bindings_sha256
                or committed.candidate_authority_sha256
                != request.candidate_authority_sha256
                or committed.consumer_registry_root_sha256
                != request.consumer_registry_root_sha256
                or committed.consumer_epoch_root_sha256
                != request.consumer_epoch_root_sha256
                or committed.candidate_count != request.candidate_count
                or binding is None
                or binding.prepare_receipt_sha256
                != prepared.prepare_receipt_sha256
                or binding.source_store_identity_sha256
                != self.source_store_identity_sha256
                or binding.source_fence_sha256 != prepared.source_fence_sha256
            ):
                raise AcquisitionEvidenceConflict(
                    "held committed lifecycle decision differs"
                )
            with ExitStack() as candidate_holds:
                members: tuple[
                    LifecycleReclamationArchiveMemberV2, ...
                ] | None = None
                if initial_state == "PREPARED":
                    members = candidate_holds.enter_context(
                        self._held_members(request)
                    )
                with store._lock, closing(store._connect()) as db:
                    try:
                        db.execute("BEGIN IMMEDIATE")
                        self._validate_reclamation_authority(db)
                        current = db.execute(
                            "SELECT * FROM lifecycle_reclamation_decisions "
                            "WHERE request_sha256=?",
                            (request_sha,),
                        ).fetchone()
                        if current is None or str(current["state"]) not in {
                            "PREPARED", "APPLYING", "APPLIED",
                        }:
                            raise AcquisitionEvidenceConflict(
                                "lifecycle prepare is no longer applicable"
                            )
                        if str(current["state"]) == "APPLIED":
                            completed = self._verify_reclamation_state(
                                db,
                                current,
                                require_files_absent=True,
                            )["apply_receipt"]
                            db.commit()
                            return completed
                        if str(current["state"]) == "PREPARED":
                            if members is None:
                                raise AcquisitionEvidenceConflict(
                                    "lifecycle PREPARED candidate hold is unavailable"
                                )
                            sealed = self._verify_reclamation_state(
                                db,
                                current,
                                require_files_absent=False,
                            )
                            snapshot = self._snapshot(
                                request,
                                members,
                                database=db,
                            )
                            if (
                                snapshot["source_view_sha256"]
                                != prepared.source_view_sha256
                                or snapshot["child_reference_root_sha256"]
                                != prepared.child_reference_root_sha256
                                or snapshot["effect"] != sealed["effect"]
                                or snapshot["floor_before"]
                                != sealed["floor_before"]
                            ):
                                raise AcquisitionEvidenceConflict(
                                    "lifecycle source fence changed before apply"
                                )
                            changed = db.execute(
                                "UPDATE lifecycle_reclamation_decisions SET "
                                "state='APPLYING',committed_decision_sha256=?,"
                                "committed_receipt_sha256=? "
                                "WHERE request_sha256=? AND state='PREPARED'",
                                (
                                    committed.decision_sha256,
                                    committed.receipt_sha256,
                                    request_sha,
                                ),
                            ).rowcount
                            if changed != 1:
                                raise AcquisitionEvidenceIntegrityError(
                                    "lifecycle APPLYING transition failed"
                                )
                            for indexed in sealed["archive_indexes"]:
                                changed = db.execute(
                                    "DELETE FROM archive_index WHERE "
                                    "producer_receipt_sha256=? AND "
                                    "acquisition_job_id=? AND event_id=? AND "
                                    "event_sha256=? AND consumer_id=? AND "
                                    "stream_id=? AND stream_sequence=? AND "
                                    "global_sequence=? AND content_sha256=? AND "
                                    "segment_id=?",
                                    (
                                        indexed["producer_receipt_sha256"],
                                        indexed["acquisition_job_id"],
                                        indexed["event_id"],
                                        indexed["event_sha256"],
                                        indexed["consumer_id"],
                                        indexed["stream_id"],
                                        indexed["stream_sequence"],
                                        indexed["global_sequence"],
                                        indexed["content_sha256"],
                                        indexed["segment_id"],
                                    ),
                                ).rowcount
                                if changed != 1:
                                    raise AcquisitionEvidenceIntegrityError(
                                        "lifecycle archive-index delete differs"
                                    )
                            for reference in sealed["object_references"]:
                                remaining = int(db.execute(
                                    "SELECT count(*) FROM archive_index "
                                    "WHERE content_sha256=?",
                                    (reference["content_sha256"],),
                                ).fetchone()[0])
                                if remaining != reference["retained_reference_count"]:
                                    raise AcquisitionEvidenceIntegrityError(
                                        "lifecycle object-sharing effect differs"
                                    )
                                if not reference["delete_file"]:
                                    continue
                                changed = db.execute(
                                    "DELETE FROM archive_objects WHERE "
                                    "content_sha256=? AND size_bytes=? AND filename=?",
                                    (
                                        reference["content_sha256"],
                                        reference["size_bytes"],
                                        reference["filename"],
                                    ),
                                ).rowcount
                                if changed != 1:
                                    raise AcquisitionEvidenceIntegrityError(
                                        "lifecycle archive-object delete differs"
                                    )
                            floor = db.execute(
                                "SELECT * FROM lifecycle_archive_floor WHERE singleton=1"
                            ).fetchone()
                            if (
                                floor is None
                                or self._floor_sha256(floor)
                                != str(current["floor_before_sha256"])
                            ):
                                raise AcquisitionEvidenceConflict(
                                    "lifecycle archive floor changed before apply"
                                )
                            expected_floor = sealed["expected_floor_after"]
                            changed = db.execute(
                                "UPDATE lifecycle_archive_floor SET "
                                "pruned_through_segment_ordinal=?,"
                                "pruned_through_segment_id=?,"
                                "pruned_through_file_sha256=?,"
                                "pruned_through_global_sequence=?,"
                                "reclaimed_completion_count=?,"
                                "reclaimed_segment_count=?,"
                                "reclaimed_file_bytes=?,"
                                "last_apply_receipt_sha256=? "
                                "WHERE singleton=1",
                                (
                                    expected_floor[
                                        "pruned_through_segment_ordinal"
                                    ],
                                    expected_floor["pruned_through_segment_id"],
                                    expected_floor[
                                        "pruned_through_file_sha256"
                                    ],
                                    expected_floor[
                                        "pruned_through_global_sequence"
                                    ],
                                    expected_floor[
                                        "reclaimed_completion_count"
                                    ],
                                    expected_floor["reclaimed_segment_count"],
                                    expected_floor["reclaimed_file_bytes"],
                                    expected_floor[
                                        "last_apply_receipt_sha256"
                                    ],
                                ),
                            ).rowcount
                            if changed != 1:
                                raise AcquisitionEvidenceIntegrityError(
                                    "lifecycle archive floor transition failed"
                                )
                            applying = db.execute(
                                "SELECT * FROM lifecycle_reclamation_decisions "
                                "WHERE request_sha256=?",
                                (request_sha,),
                            ).fetchone()
                            if applying is None:
                                raise AcquisitionEvidenceIntegrityError(
                                    "lifecycle APPLYING journal disappeared"
                                )
                            self._verify_reclamation_state(
                                db,
                                applying,
                                require_files_absent=False,
                            )
                            store._fault("reclamation.apply.before_logical_commit")
                            db.commit()
                            store._fault("reclamation.apply.after_logical_commit")
                        else:
                            if (
                                str(current["committed_decision_sha256"])
                                != committed.decision_sha256
                                or str(current["committed_receipt_sha256"])
                                != committed.receipt_sha256
                            ):
                                raise AcquisitionEvidenceConflict(
                                    "lifecycle APPLYING decision differs"
                                )
                            self._verify_reclamation_state(
                                db,
                                current,
                                require_files_absent=False,
                            )
                            db.commit()
                    except BaseException:
                        if db.in_transaction:
                            db.rollback()
                        raise
            store._directory_fsync(store.root)
            self._unlink_journal_files(request_sha)
            with store._lock, closing(store._connect()) as finish_db:
                try:
                    finish_db.execute("BEGIN IMMEDIATE")
                    self._validate_reclamation_authority(finish_db)
                    current = finish_db.execute(
                        "SELECT * FROM lifecycle_reclamation_decisions "
                        "WHERE request_sha256=?",
                        (request_sha,),
                    ).fetchone()
                    if current is None or str(current["state"]) not in {
                        "APPLYING", "APPLIED",
                    }:
                        raise AcquisitionEvidenceIntegrityError(
                            "lifecycle APPLYING journal disappeared"
                        )
                    if str(current["state"]) == "APPLIED":
                        completed = self._verify_reclamation_state(
                            finish_db,
                            current,
                            require_files_absent=True,
                        )["apply_receipt"]
                        finish_db.commit()
                        return completed
                    if (
                        str(current["committed_decision_sha256"])
                        != committed.decision_sha256
                        or str(current["committed_receipt_sha256"])
                        != committed.receipt_sha256
                    ):
                        raise AcquisitionEvidenceConflict(
                            "lifecycle APPLYING decision differs"
                        )
                    final_journal = self._verify_reclamation_state(
                        finish_db,
                        current,
                        require_files_absent=True,
                    )
                    post_view = reclamation_digest_mapping({
                        "floor": final_journal["expected_floor_after"],
                        "request_sha256": request_sha,
                        "schema": RECLAMATION_POST_APPLY_SCHEMA,
                        "source_store_identity_sha256": (
                            self.source_store_identity_sha256
                        ),
                    })
                    observed = _finite(store._clock(), "reclamation apply clock")
                    receipt = SourceLocalApplyReceiptV1.create(
                        request_sha256=request_sha,
                        role=self.role,
                        source_store_identity_sha256=(
                            self.source_store_identity_sha256
                        ),
                        prepare_receipt_sha256=prepared.prepare_receipt_sha256,
                        committed_decision_sha256=committed.decision_sha256,
                        committed_decision_receipt_sha256=committed.receipt_sha256,
                        predecessor_apply_receipt_sha256=None,
                        effect_sha256=str(current["effect_sha256"]),
                        post_apply_view_sha256=post_view,
                        released_bytes=int(current["releasable_bytes"]),
                        released_entries=int(current["releasable_entries"]),
                        applied_at=observed,
                    )
                    changed = finish_db.execute(
                        "UPDATE lifecycle_reclamation_decisions SET "
                        "state='APPLIED',active_slot=NULL,"
                        "post_apply_view_sha256=?,apply_receipt_sha256=?,"
                        "apply_receipt_json=?,applied_at=? "
                        "WHERE request_sha256=? AND state='APPLYING'",
                        (
                            post_view,
                            receipt.apply_receipt_sha256,
                            sqlite3.Binary(
                                reclamation_canonical_bytes(receipt.as_dict())
                            ),
                            observed,
                            request_sha,
                        ),
                    ).rowcount
                    if changed != 1:
                        raise AcquisitionEvidenceIntegrityError(
                            "lifecycle APPLIED transition failed"
                        )
                    applied = finish_db.execute(
                        "SELECT * FROM lifecycle_reclamation_decisions "
                        "WHERE request_sha256=?",
                        (request_sha,),
                    ).fetchone()
                    if applied is None:
                        raise AcquisitionEvidenceIntegrityError(
                            "lifecycle APPLIED journal disappeared"
                        )
                    reproved = self._verify_reclamation_state(
                        finish_db,
                        applied,
                        require_files_absent=True,
                    )["apply_receipt"]
                    if reproved != receipt:
                        raise AcquisitionEvidenceIntegrityError(
                            "lifecycle APPLIED receipt changed in transaction"
                        )
                    store._fault("reclamation.apply.before_applied_commit")
                    finish_db.commit()
                    store._fault("reclamation.apply.after_applied_commit")
                except BaseException:
                    if finish_db.in_transaction:
                        finish_db.rollback()
                    raise
            store._directory_fsync(store.root)
            return receipt


def _safe_raw_receipt(receipt: RawArtifactReceipt) -> dict[str, Any]:
    return {
        "content_sha256": receipt.content_sha256,
        "content_type": receipt.content_type,
        "created_at": receipt.created_at,
        "fetch_id": receipt.fetch_id,
        "product": receipt.product,
        "provenance": receipt.provenance,
        "provenance_sha256": receipt.provenance_sha256,
        "provider": receipt.provider,
        "receipt_id": receipt.receipt_id,
        "receipt_sha256": receipt.receipt_sha256,
        "received_at": receipt.received_at,
        "schema": "stv3-worker-raw-receipt-safe-evidence-v1",
        "size_bytes": receipt.size_bytes,
        "upstream_id": receipt.upstream_id,
    }


def _safe_capture(capture: RawArtifactCaptureReceipt) -> dict[str, Any]:
    return capture.to_metadata()


def _execution_mapping(receipt: ExecutionReceipt) -> dict[str, Any]:
    return receipt.as_dict()


def publish_dwd_completion(
    *,
    lifecycle_store: AcquisitionEvidenceLifecycleStore,
    acquisition_plan: worker_acquisition_plan.AcquisitionPlan,
    desired_state: DesiredState,
    scheduler: AutonomousScheduler,
    execution_store: ExecutionReceiptStore,
    capacity_ledger: RawCapacityLedger,
    source_raw_store: RawArtifactStore,
    handoff_raw_store: RawArtifactStore,
    source_capture_id: str,
    acquisition_job_id: str,
    consumer_id: str = "dwd-rv-quantitative-decoder-v1",
    handoff_fault_injector: Callable[[str], None] | None = None,
) -> CompletionProducerReceipt:
    """Publish the concrete DWD raw-v2 -> decoder handoff completion.

    All authorities are reloaded and checksum-verified.  The handoff adapter is
    network-free and idempotent; its target archive must remain byte-identical
    to the source capture.  This function emits no decoded quantity and makes
    no operational or meteorological truth claim.
    """

    if not isinstance(lifecycle_store, AcquisitionEvidenceLifecycleStore):
        raise TypeError("lifecycle_store must be AcquisitionEvidenceLifecycleStore")
    if not isinstance(desired_state, DesiredState):
        raise TypeError("desired_state must be DesiredState")
    if not isinstance(scheduler, AutonomousScheduler):
        raise TypeError("scheduler must be AutonomousScheduler")
    if not isinstance(execution_store, ExecutionReceiptStore):
        raise TypeError("execution_store must be ExecutionReceiptStore")
    if not isinstance(capacity_ledger, RawCapacityLedger):
        raise TypeError("capacity_ledger must be RawCapacityLedger")
    if not isinstance(source_raw_store, RawArtifactStore) or not isinstance(
        handoff_raw_store, RawArtifactStore
    ):
        raise TypeError("raw stores must be RawArtifactStore")
    job_id = _identifier(acquisition_job_id, "acquisition job ID")
    valid_plan = worker_acquisition_plan.validate_worker_acquisition_plan(
        acquisition_plan
    )
    if valid_plan.snapshot_sha256 != desired_state.state_sha256:
        raise AcquisitionEvidenceConflict("plan and desired authority differ")
    executions = execution_store.load_job_receipts(job_id)
    terminals = tuple(item for item in executions if item.terminal)
    if len(terminals) != 1 or terminals[0].outcome != "completed_shadow_raw":
        raise AcquisitionEvidenceConflict(
            "DWD completion lacks one successful terminal execution receipt"
        )
    terminal = terminals[0]
    scheduler_proof = scheduler.terminal_proof(terminal.scheduler_job_id)
    if (
        not isinstance(scheduler_proof, SchedulerTerminalProof)
        or scheduler_proof.state != "completed"
        or scheduler_proof.last_transition != "completed"
        or scheduler_proof.job_type
        != acquisition_scheduler_bridge.JOB_TYPE
        or scheduler_proof.desired_state_sha256 != desired_state.state_sha256
    ):
        raise AcquisitionEvidenceConflict(
            "DWD execution lacks matching terminal scheduler proof"
        )
    job = acquisition_scheduler_bridge.reconstruct_acquisition_job_for_slot(
        valid_plan,
        intent_id=scheduler_proof.intent_id,
        scheduled_for=scheduler_proof.scheduled_for,
    )
    if (
        job.job_id != job_id
        or job.workload_id != "radar.dwd.radolan_rv"
        or job.provider_id != "dwd"
        or job.product_id != "dwd_radolan_rv_native_tar"
        or job.secret_requirement_ids
        or job.snapshot_sha256 != desired_state.state_sha256
    ):
        raise AcquisitionEvidenceConflict(
            "terminal scheduler slot is not the closed no-secret DWD workload"
        )
    capacity = capacity_ledger.load_for_job(provider="dwd", job_identity=job_id)
    if capacity is None or capacity.status != "COMMITTED":
        raise AcquisitionEvidenceConflict("DWD raw capture lacks committed capacity")
    source_bundle = source_raw_store.load_capture_bundle_bytes(source_capture_id)
    if source_bundle is None:
        raise AcquisitionEvidenceConflict("DWD source capture is unavailable")
    source_capture, source_receipts, source_payloads = source_bundle
    if (
        source_capture.provider != "dwd"
        or source_capture.product != "dwd_open_data_composite_rv_raw_capture"
        or source_capture.fetch_id != f"{job_id}:capture"
        or set(source_receipts) != {"rv_tar"}
        or set(source_payloads) != {"rv_tar"}
    ):
        raise AcquisitionEvidenceConflict("DWD source capture identity differs")
    source_receipt = source_receipts["rv_tar"]
    source_payload = source_payloads["rv_tar"]
    if (
        source_receipt.fetch_id != f"{job_id}:rv_tar"
        or terminal.content_sha256 != source_receipt.content_sha256
        or terminal.raw_receipt_sha256 != source_receipt.receipt_sha256
        or terminal.capture_sha256 != source_capture.capture_receipt_sha256
        or terminal.capacity_commit_sha256 != capacity.commit_sha256
        or capacity.content_sha256 != source_receipt.content_sha256
        or capacity.receipt_sha256 != source_receipt.receipt_sha256
        or capacity.capture_sha256 != source_capture.capture_receipt_sha256
        or capacity.actual_bytes != len(source_payload)
        or capacity.actual_entries != 1
    ):
        raise AcquisitionEvidenceConflict(
            "DWD execution, capacity and raw capture hashes differ"
        )
    handoff = adapt_dwd_rv_raw_capture(
        source_capture_id=source_capture.capture_id,
        source_raw_store=source_raw_store,
        handoff_raw_store=handoff_raw_store,
        fault_injector=handoff_fault_injector,
    )
    target_bundle = handoff_raw_store.load_capture_bundle_bytes(
        handoff.target_capture_id
    )
    if target_bundle is None:
        raise AcquisitionEvidenceIntegrityError("DWD handoff target disappeared")
    target_capture, target_receipts, target_payloads = target_bundle
    if set(target_receipts) != {"rv_tar"} or set(target_payloads) != {"rv_tar"}:
        raise AcquisitionEvidenceIntegrityError("DWD handoff target roles differ")
    target_receipt = target_receipts["rv_tar"]
    target_payload = target_payloads["rv_tar"]
    if (
        target_payload != source_payload
        or target_receipt.content_sha256 != source_receipt.content_sha256
        or handoff.archive_sha256 != source_receipt.content_sha256
    ):
        raise AcquisitionEvidenceIntegrityError(
            "DWD handoff is not exact source raw bytes"
        )
    authority = desired_authority_proof(desired_state)
    proof = {
        "adapter": {
            "adapter_config_sha256": HANDOFF_CONFIG_SHA256,
            "adapter_id": HANDOFF_ADAPTER_ID,
            "adapter_version": HANDOFF_ADAPTER_VERSION,
            "receipt": asdict(handoff),
        },
        "capacity_receipt": {
            **asdict(capacity),
            "schema": "stv3-worker-raw-capacity-receipt-evidence-v1",
        },
        "desired_authority": authority,
        "execution_receipts": [_execution_mapping(item) for item in executions],
        "handoff_capture": _safe_capture(target_capture),
        "handoff_receipts": {
            role: _safe_raw_receipt(receipt)
            for role, receipt in sorted(target_receipts.items())
        },
        "meteorological_claim": False,
        "observation_policy": "base_plus000_only",
        "raw_refetch_performed": False,
        "scheduler_terminal": {
            **scheduler_proof.as_dict(),
            "proof_sha256": scheduler_proof.proof_sha256,
        },
        "schema": DWD_COMPLETION_PROOF_SCHEMA,
        "source_capture": _safe_capture(source_capture),
        "source_receipts": {
            role: _safe_raw_receipt(receipt)
            for role, receipt in sorted(source_receipts.items())
        },
        "workload": job.as_dict(),
    }
    return lifecycle_store.publish(
        acquisition_job_id=job_id,
        consumer_id=consumer_id,
        stream_id="radar.dwd.radolan_rv",
        desired_state_sha256=desired_state.state_sha256,
        authority_sha256=authority["authority_sha256"],
        proof=proof,
        payload=target_payload,
    )


__all__ = [
    "ARCHIVE_MANIFEST_SCHEMA",
    "ARCHIVE_SEGMENT_SCHEMA",
    "ARCHIVED_COMPLETION_SCHEMA",
    "AcquisitionEvidenceCapacityError",
    "AcquisitionEvidenceConflict",
    "AcquisitionEvidenceDurabilityError",
    "AcquisitionEvidenceIntegrityError",
    "AcquisitionEvidenceLifecycleError",
    "AcquisitionEvidenceLifecycleStore",
    "AcquisitionEvidenceLimits",
    "AcquisitionCompletionLookup",
    "ArchiveRotationResult",
    "COMPLETION_EVENT_SCHEMA",
    "CONSUMPTION_RECEIPT_SCHEMA",
    "CONTRACT",
    "CompletionLease",
    "CompletionLookup",
    "CompletionProducerReceipt",
    "CompletionWindowExpired",
    "ConsumptionReceipt",
    "DESIRED_AUTHORITY_SCHEMA",
    "DWD_COMPLETION_PROOF_SCHEMA",
    "EvidenceRetentionPolicy",
    "LIFECYCLE_RECLAMATION_PREWIRING_BLOCKERS",
    "PRODUCER_RECEIPT_SCHEMA",
    "QUARANTINE_REASON_CODES",
    "QUARANTINE_RECEIPT_SCHEMA",
    "REJECTION_REASON_CODES",
    "REJECTION_RECEIPT_SCHEMA",
    "TerminalConsumptionEvidence",
    "TerminalEventIdentity",
    "QuarantineReceipt",
    "QuarantineTerminalResult",
    "RejectionReceipt",
    "desired_authority_proof",
    "publish_dwd_completion",
]
