"""Inert, fail-closed source-retention proof and sealed export boundary.

This module deliberately never deletes source bytes or source-store rows.  It
can prove that one raw-v3/raw-v2 capture is cross-bound to terminal scheduler,
execution, capacity and downstream-consumption authorities, and can copy that
proof into a content-addressed sealed archive.  The current source stores do
not expose one cross-store compare-and-swap prune transaction, so a successful
export is *not* a reclamation authority.
"""
from __future__ import annotations

import base64
from contextlib import contextmanager
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
import stat
import threading
import time
from typing import Any, Callable, Iterator, Mapping

from .acquisition_evidence_lifecycle import (
    AcquisitionEvidenceConflict,
    AcquisitionEvidenceLifecycleStore,
    CompletionProducerReceipt,
    ConsumptionReceipt,
    DWD_COMPLETION_PROOF_SCHEMA,
    TerminalEventIdentity,
    TerminalConsumptionEvidence,
)
from .acquisition_execution_store import (
    CONTRACT as EXECUTION_CONTRACT,
    ExecutionReceipt,
    ExecutionReceiptStore,
)
from .autonomous_scheduler import (
    CONTRACT as SCHEDULER_CONTRACT,
    AutonomousScheduler,
    SchedulerConflict,
    SchedulerTerminalProof,
)
from .raw_artifacts import (
    RAW_ARTIFACT_CAPTURE_SCHEMA,
    RAW_ARTIFACT_SCHEMA_VERSION,
    RawArtifactCaptureReceipt,
    RawArtifactReceipt,
    RawArtifactStore,
)
from .raw_capacity_ledger import (
    LEDGER_CONTRACT,
    CapacityReservationReceipt,
    RawCapacityLedger,
)
from .acquisition_consumer_registry import (
    AcquisitionConsumerClosureReceipt,
    AcquisitionConsumerRegistryAuthority,
    AcquisitionConsumerRegistryBoundEpochVerifier,
    AcquisitionConsumerRegistryError,
)
from .acquisition_source_reclamation_protocol import (
    PARTICIPANT_APPLY_ORDER,
    SourceReclamationCandidateAuthority,
    SourceReclamationCandidateBindingV1,
    SourceReclamationCandidatePageV1,
    SourceReclamationParticipantV2,
    SourceReclamationRequestV2,
    archive_binding_root as reclamation_archive_binding_root,
    candidate_binding_root as reclamation_candidate_binding_root,
    canonical_bytes as reclamation_canonical_bytes,
    digest_mapping as reclamation_digest_mapping,
)
from .acquisition_source_reclamation_views import (
    participant_candidate_view_sha256,
)


CONTRACT = "stv3-worker-acquisition-source-retention-v1"
PLAN_SCHEMA = "stv3-worker-acquisition-source-retention-plan-v1"
SEGMENT_SCHEMA = "stv3-worker-acquisition-source-sealed-segment-v1"
MANIFEST_SCHEMA = "stv3-worker-acquisition-source-archive-manifest-v1"
ANCHOR_SCHEMA = "stv3-worker-acquisition-source-archive-anchor-v1"
RECORD_SCHEMA = "stv3-worker-acquisition-source-retention-record-v1"
LOCK_BYTES = b"STV3_ACQUISITION_SOURCE_ARCHIVE_LOCK_V1\n"
MAX_DIAGNOSTIC_BYTES = 4096
_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,255}$")
_SHA = re.compile(r"^[0-9a-f]{64}$")
_SAFE_FILE = re.compile(r"^[0-9a-f]{64}\.(sealed|manifest)\.json$")
_WITHHELD_REASONS = frozenset({
    "capacity_commit_unavailable",
    "downstream_terminal_unavailable",
    "execution_terminal_unavailable",
    "hot_window_active",
    "raw_capture_shape_unsupported",
    "raw_capture_unavailable",
    "scheduler_terminal_unavailable",
})

V2_CONTRACT = "stv3-worker-acquisition-source-retention-v2"
V2_AUTHORITY_SCHEMA = "stv3-worker-source-retention-authority-v2"
V2_HEAD_SCHEMA = "stv3-worker-source-retention-head-v2"
V2_OBJECT_REFERENCE_ROOT_SCHEMA = (
    "stv3-worker-source-retention-object-reference-root-v2"
)
V2_OBJECT_REFERENCE_LINK_SCHEMA = (
    "stv3-worker-source-retention-object-reference-link-v2"
)
V2_METADATA_SCHEMA = "stv3-worker-source-retention-metadata-v2"
V2_REFERENCE_SCHEMA = (
    "stv3-worker-acquisition-source-reclamation-candidate-reference-v1"
)
V2_SEGMENT_SCHEMA = "stv3-worker-source-retention-segment-v2"
V2_MANIFEST_SCHEMA = "stv3-worker-source-retention-manifest-v2"
V2_ANCHOR_SCHEMA = "stv3-worker-source-retention-anchor-v2"
V2_PREFIX_SCHEMA = "stv3-worker-source-retention-prefix-link-v2"
V2_CANDIDATE_SET_SCHEMA = "stv3-worker-source-retention-candidate-set-v2"
V2_PENDING_SCHEMA = "stv3-worker-source-retention-pending-publication-v2"
V2_LOCK_BYTES = b"STV3_ACQUISITION_SOURCE_ARCHIVE_LOCK_V2\n"
V2_MAX_CANDIDATES = 4096
V2_MAX_REFERENCE_BYTES = 64 * 1024
V2_MAX_METADATA_BYTES = 4 * 1024 * 1024
V2_MAX_CANDIDATE_SET_BYTES = 16 * 1024 * 1024
V2_MAX_PENDING_BYTES = 16 * 1024 * 1024
V2_ROLE_STORE_NAMES = {
    "capacity_ledger": "capacity",
    "downstream_lifecycle": "evidence",
    "execution_receipts": "execution",
    "raw_artifacts": "raw",
    "scheduler": "scheduler",
}
_V2_CONTENT_NAME = re.compile(
    r"^[0-9a-f]{64}\.(?:raw|metadata\.json|reference\.json|segment\.json|"
    r"manifest\.json|anchor\.json|set\.json)$"
)


class AcquisitionSourceRetentionError(RuntimeError):
    """Base failure with bounded, non-sensitive reason text."""


class AcquisitionSourceRetentionIntegrityError(AcquisitionSourceRetentionError):
    pass


class AcquisitionSourceRetentionCapacityError(AcquisitionSourceRetentionError):
    pass


class AcquisitionSourceRetentionDurabilityError(AcquisitionSourceRetentionError):
    pass


class AcquisitionSourceRetentionBusy(AcquisitionSourceRetentionError):
    pass


class AcquisitionSourceRetentionConflict(AcquisitionSourceRetentionError):
    pass


class AcquisitionSourceRetentionWindowExpired(AcquisitionSourceRetentionError):
    pass


@dataclass(frozen=True, slots=True)
class AcquisitionSourceRetentionLimits:
    max_candidates: int = 128
    max_execution_receipts_per_job: int = 256
    min_terminal_age_seconds: float = 3600.0
    max_segment_bytes: int = 256 * 1024 * 1024
    max_archive_segments: int = 4096
    max_archived_jobs: int = 65_536
    max_total_archive_bytes: int = 16 * 1024 * 1024 * 1024
    max_staging_files: int = 8
    lock_timeout_seconds: float = 5.0

    def __post_init__(self) -> None:
        for name, value, maximum in (
            ("max_candidates", self.max_candidates, 4096),
            (
                "max_execution_receipts_per_job",
                self.max_execution_receipts_per_job,
                65_536,
            ),
            ("max_segment_bytes", self.max_segment_bytes, 1024**3),
            ("max_archive_segments", self.max_archive_segments, 65_536),
            ("max_archived_jobs", self.max_archived_jobs, 1_000_000),
            (
                "max_total_archive_bytes",
                self.max_total_archive_bytes,
                2**53,
            ),
            ("max_staging_files", self.max_staging_files, 64),
        ):
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or not 1 <= value <= maximum
            ):
                raise ValueError(f"invalid {name}")
        if self.max_total_archive_bytes < self.max_segment_bytes:
            raise ValueError("total archive bound is smaller than segment bound")
        _finite(
            self.min_terminal_age_seconds,
            "minimum terminal age",
            minimum=0.0,
            maximum=3650 * 24 * 3600.0,
        )
        _finite(
            self.lock_timeout_seconds,
            "archive lock timeout",
            minimum=0.001,
            maximum=60.0,
        )

    @property
    def fingerprint(self) -> str:
        return _digest({"contract": CONTRACT, **asdict(self)})


@dataclass(frozen=True, slots=True, repr=False)
class AcquisitionSourceCandidate:
    acquisition_job_id: str
    scheduler_job_id: str
    raw_capture_id: str
    evidence_stream_id: str
    evidence_stream_sequence: int

    def __post_init__(self) -> None:
        for value, label in (
            (self.acquisition_job_id, "acquisition job ID"),
            (self.scheduler_job_id, "scheduler job ID"),
            (self.raw_capture_id, "raw capture ID"),
            (self.evidence_stream_id, "evidence stream ID"),
        ):
            _identifier(value, label)
        if (
            isinstance(self.evidence_stream_sequence, bool)
            or not isinstance(self.evidence_stream_sequence, int)
            or not 1 <= self.evidence_stream_sequence <= 2**63 - 1
        ):
            raise ValueError("invalid evidence stream sequence")

    def as_dict(self) -> dict[str, Any]:
        return {
            "acquisition_job_id": self.acquisition_job_id,
            "evidence_stream_id": self.evidence_stream_id,
            "evidence_stream_sequence": self.evidence_stream_sequence,
            "raw_capture_id": self.raw_capture_id,
            "scheduler_job_id": self.scheduler_job_id,
        }

    @property
    def candidate_sha256(self) -> str:
        return _digest({"schema": PLAN_SCHEMA, **self.as_dict()})


@dataclass(frozen=True, slots=True)
class RetentionDisposition:
    candidate_sha256: str
    status: str
    reason_code: str
    record_sha256: str | None


@dataclass(frozen=True, slots=True, repr=False)
class AcquisitionSourceRetentionPlan:
    plan_sha256: str
    source_binding_sha256: str
    source_view_sha256: str
    created_at: float
    dispositions: tuple[RetentionDisposition, ...]
    _record_payloads: tuple[bytes, ...] = field(repr=False)

    @property
    def eligible_count(self) -> int:
        return len(self._record_payloads)

    @property
    def reclaim_authorized(self) -> bool:
        return False


@dataclass(frozen=True, slots=True)
class AcquisitionSourceArchiveResult:
    status: str
    segment_sha256: str | None
    archived_jobs: int
    archive_anchor_sha256: str
    source_reclaimed: bool = False


@dataclass(frozen=True, slots=True, repr=False)
class AcquisitionSourceArchiveLookup:
    status: str
    candidate_sha256: str
    segment_sha256: str | None
    record: dict[str, Any] | None = field(default=None, repr=False)


@dataclass(frozen=True, slots=True)
class AcquisitionSourceRetentionLimitsV2:
    max_candidates: int = V2_MAX_CANDIDATES
    max_execution_receipts_per_job: int = 256
    min_terminal_age_seconds: float = 3600.0
    max_object_bytes: int = 128 * 1024 * 1024
    max_metadata_bytes: int = V2_MAX_METADATA_BYTES
    max_segment_bytes: int = 32 * 1024 * 1024
    max_archive_segments: int = 4096
    max_archived_jobs: int = 65_536
    max_total_archive_bytes: int = 16 * 1024 * 1024 * 1024
    max_staging_files: int = 16
    lock_timeout_seconds: float = 5.0

    def __post_init__(self) -> None:
        for name, value, maximum in (
            ("max_candidates", self.max_candidates, V2_MAX_CANDIDATES),
            (
                "max_execution_receipts_per_job",
                self.max_execution_receipts_per_job,
                65_536,
            ),
            ("max_object_bytes", self.max_object_bytes, 1024**3),
            ("max_metadata_bytes", self.max_metadata_bytes, 16 * 1024 * 1024),
            ("max_segment_bytes", self.max_segment_bytes, 64 * 1024 * 1024),
            ("max_archive_segments", self.max_archive_segments, 65_536),
            ("max_archived_jobs", self.max_archived_jobs, 1_000_000),
            ("max_total_archive_bytes", self.max_total_archive_bytes, 2**53),
            ("max_staging_files", self.max_staging_files, 64),
        ):
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or not 1 <= value <= maximum
            ):
                raise ValueError(f"invalid v2 {name}")
        if self.max_metadata_bytes > self.max_segment_bytes:
            raise ValueError("v2 metadata bound exceeds segment bound")
        _finite(
            self.min_terminal_age_seconds,
            "v2 minimum terminal age",
            minimum=0.0,
            maximum=3650 * 24 * 3600.0,
        )
        _finite(
            self.lock_timeout_seconds,
            "v2 archive lock timeout",
            minimum=0.001,
            maximum=60.0,
        )

    @property
    def fingerprint(self) -> str:
        return _digest({"contract": V2_CONTRACT, **asdict(self)})


@dataclass(frozen=True, slots=True)
class AcquisitionSourceArchiveSealResultV2:
    status: str
    archive_anchor_sha256: str
    archive_segment_sha256: str
    candidate_authority_sha256: str
    candidate_bindings_sha256: str
    archive_binding_root_sha256: str
    consumer_registry_root_sha256: str
    consumer_epoch_root_sha256: str
    consumer_closure_root_sha256: str
    source_store_set_root_sha256: str
    candidate_count: int
    object_count: int
    object_bytes: int


@dataclass(frozen=True, slots=True)
class AcquisitionSourceArchiveAncestorProofV2:
    proof_sha256: str
    archive_authority_sha256: str
    ancestor_anchor_sha256: str
    descendant_anchor_sha256: str
    ancestor_generation: int
    descendant_generation: int
    anchor_chain: tuple[str, ...]

    def __post_init__(self) -> None:
        for value, label in (
            (self.proof_sha256, "ancestor proof SHA-256"),
            (self.archive_authority_sha256, "archive authority SHA-256"),
            (self.ancestor_anchor_sha256, "ancestor anchor SHA-256"),
            (self.descendant_anchor_sha256, "descendant anchor SHA-256"),
        ):
            _sha(value, label)
        if (
            not isinstance(self.anchor_chain, tuple)
            or not self.anchor_chain
            or len(self.anchor_chain) > 4097
            or self.anchor_chain[0] != self.ancestor_anchor_sha256
            or self.anchor_chain[-1] != self.descendant_anchor_sha256
            or len(set(self.anchor_chain)) != len(self.anchor_chain)
            or any(_SHA.fullmatch(item) is None for item in self.anchor_chain)
            or not 0 <= self.ancestor_generation <= self.descendant_generation
            or len(self.anchor_chain)
            != self.descendant_generation - self.ancestor_generation + 1
        ):
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 ancestor proof shape differs"
            )
        if self.proof_sha256 != _digest(self._core()):
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 ancestor proof checksum differs"
            )

    def _core(self) -> dict[str, Any]:
        return {
            "ancestor_anchor_sha256": self.ancestor_anchor_sha256,
            "ancestor_generation": self.ancestor_generation,
            "anchor_chain": list(self.anchor_chain),
            "archive_authority_sha256": self.archive_authority_sha256,
            "descendant_anchor_sha256": self.descendant_anchor_sha256,
            "descendant_generation": self.descendant_generation,
            "schema": "stv3-worker-source-retention-ancestor-proof-v2",
        }

    def as_dict(self) -> dict[str, Any]:
        return {**self._core(), "proof_sha256": self.proof_sha256}


def _canonical(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
    except (TypeError, ValueError, RecursionError) as err:
        raise AcquisitionSourceRetentionIntegrityError(
            "retention evidence is not canonical"
        ) from err


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _identifier(value: object, label: str) -> str:
    if not isinstance(value, str) or _ID.fullmatch(value) is None:
        raise ValueError(f"invalid {label}")
    return value


def _sha(value: object, label: str) -> str:
    if not isinstance(value, str) or _SHA.fullmatch(value) is None:
        raise AcquisitionSourceRetentionIntegrityError(f"invalid {label}")
    return value


def _finite(
    value: object,
    label: str,
    *,
    minimum: float,
    maximum: float,
) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"invalid {label}")
    result = float(value)
    if not math.isfinite(result) or not minimum <= result <= maximum:
        raise ValueError(f"invalid {label}")
    return result


def _envelope(schema: str, hash_field: str, body: Mapping[str, Any]) -> bytes:
    core = {"schema": schema, **dict(body)}
    return _canonical({**core, hash_field: _digest(core)})


def _decode_envelope(
    payload: bytes,
    *,
    schema: str,
    hash_field: str,
) -> dict[str, Any]:
    try:
        decoded = json.loads(payload.decode("ascii"))
    except (UnicodeDecodeError, json.JSONDecodeError) as err:
        raise AcquisitionSourceRetentionIntegrityError(
            "sealed archive JSON is corrupt"
        ) from err
    if not isinstance(decoded, dict) or decoded.get("schema") != schema:
        raise AcquisitionSourceRetentionIntegrityError(
            "sealed archive schema differs"
        )
    claimed = decoded.get(hash_field)
    core = dict(decoded)
    core.pop(hash_field, None)
    if _sha(claimed, hash_field) != _digest(core) or payload != _canonical(decoded):
        raise AcquisitionSourceRetentionIntegrityError(
            "sealed archive checksum differs"
        )
    return decoded


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


_PROCESS_LOCKS_GUARD = threading.Lock()
_PROCESS_LOCKS: dict[str, threading.RLock] = {}


def _process_lock(path: Path) -> threading.RLock:
    key = os.path.normcase(str(path))
    with _PROCESS_LOCKS_GUARD:
        return _PROCESS_LOCKS.setdefault(key, threading.RLock())


def _lock_descriptor(descriptor: int) -> None:
    os.lseek(descriptor, 0, os.SEEK_SET)
    if os.name == "nt":
        import msvcrt

        msvcrt.locking(descriptor, msvcrt.LK_NBLCK, 1)
    else:
        import fcntl

        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)


def _unlock_descriptor(descriptor: int) -> None:
    os.lseek(descriptor, 0, os.SEEK_SET)
    if os.name == "nt":
        import msvcrt

        msvcrt.locking(descriptor, msvcrt.LK_UNLCK, 1)
    else:
        import fcntl

        fcntl.flock(descriptor, fcntl.LOCK_UN)


class AcquisitionSourceRetentionArchive:
    """Read-only source planner plus durable sealed-export authority."""

    def __init__(
        self,
        archive_root: str | Path,
        *,
        raw_store: RawArtifactStore,
        scheduler: AutonomousScheduler,
        execution_store: ExecutionReceiptStore,
        capacity_ledger: RawCapacityLedger,
        evidence_store: AcquisitionEvidenceLifecycleStore,
        limits: AcquisitionSourceRetentionLimits | None = None,
        clock: Callable[[], float] = time.time,
        fault_injector: Callable[[str], None] | None = None,
        directory_fsync: Callable[[Path], None] | None = None,
    ) -> None:
        root = Path(archive_root)
        if not root.is_absolute():
            raise ValueError("archive root must be absolute")
        if not isinstance(raw_store, RawArtifactStore):
            raise TypeError("raw_store must be RawArtifactStore")
        if not isinstance(scheduler, AutonomousScheduler):
            raise TypeError("scheduler must be AutonomousScheduler")
        if not isinstance(execution_store, ExecutionReceiptStore):
            raise TypeError("execution_store must be ExecutionReceiptStore")
        if not isinstance(capacity_ledger, RawCapacityLedger):
            raise TypeError("capacity_ledger must be RawCapacityLedger")
        if not isinstance(evidence_store, AcquisitionEvidenceLifecycleStore):
            raise TypeError(
                "evidence_store must be AcquisitionEvidenceLifecycleStore"
            )
        if not callable(clock):
            raise TypeError("clock must be callable")
        if fault_injector is not None and not callable(fault_injector):
            raise TypeError("fault injector must be callable")
        if directory_fsync is not None and not callable(directory_fsync):
            raise TypeError("directory fsync must be callable")
        self.root = root
        self.segment_directory = root / "segments"
        self.manifest_directory = root / "manifests"
        self.staging_directory = root / "staging"
        self.anchor_path = root / "anchor.json"
        self.lock_path = root / "rotation.lock"
        self._raw = raw_store
        self._scheduler = scheduler
        self._executions = execution_store
        self._capacity = capacity_ledger
        self._evidence = evidence_store
        self._limits = limits or AcquisitionSourceRetentionLimits()
        self._clock = clock
        self._fault_injector = fault_injector
        self._directory_fsync = directory_fsync or self._default_directory_fsync
        self._local_lock = threading.RLock()
        self._process_lock = _process_lock(self.lock_path)
        self._exclusive_owner_thread: int | None = None
        self._exclusive_depth = 0
        self._exclusive_descriptor: int | None = None
        self._source_binding_sha256 = self._source_binding()
        self.initialize()

    @staticmethod
    def _default_directory_fsync(path: Path) -> None:
        if os.name == "nt":
            raise AcquisitionSourceRetentionDurabilityError(
                "directory_fsync_unavailable"
            )
        descriptor: int | None = None
        descriptor_locked = False
        try:
            descriptor = os.open(
                str(path), os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
            )
            os.fsync(descriptor)
        except OSError as err:
            if err.errno == errno.ENOSPC:
                raise AcquisitionSourceRetentionCapacityError(
                    "archive_filesystem_full"
                ) from err
            raise AcquisitionSourceRetentionDurabilityError(
                "directory_fsync_failed"
            ) from err
        finally:
            if descriptor is not None:
                os.close(descriptor)

    def _fault(self, point: str) -> None:
        if self._fault_injector is not None:
            self._fault_injector(point)

    def _source_binding(self) -> str:
        raw_stats = self._raw.stats()
        identities = {
            "capacity": _digest({
                "contract": LEDGER_CONTRACT,
                "profile": self._capacity.profile.to_mapping(),
            }),
            "evidence": _digest({
                "contract": "stv3-worker-acquisition-evidence-lifecycle-v1",
                "schema_version": 2,
            }),
            "execution": _digest({"contract": EXECUTION_CONTRACT}),
            "raw": _sha(
                raw_stats.get("store_profile_sha256"),
                "raw store profile SHA-256",
            ),
            "scheduler": _digest({"contract": SCHEDULER_CONTRACT}),
        }
        paths = {
            "capacity": self._capacity.path,
            "evidence": self._evidence.root,
            "execution": self._executions.path,
            "raw": self._raw.root,
            "scheduler": self._scheduler.path,
        }
        normalized: set[str] = set()
        bindings = []
        for role in sorted(paths):
            path = paths[role]
            if not path.is_absolute() or path.is_symlink():
                raise AcquisitionSourceRetentionIntegrityError(
                    "source store path is unsafe"
                )
            key = os.path.normcase(os.path.normpath(str(path)))
            if key in normalized:
                raise AcquisitionSourceRetentionIntegrityError(
                    "source store roles overlap"
                )
            normalized.add(key)
            bindings.append({
                "identity_sha256": identities[role],
                "path_sha256": hashlib.sha256(
                    str(path).encode("utf-8")
                ).hexdigest(),
                "role": role,
            })
        return _digest({"contract": CONTRACT, "stores": bindings})

    def _prepare_layout(self) -> None:
        if self.root.exists() and self.root.is_symlink():
            raise AcquisitionSourceRetentionIntegrityError(
                "archive root is a symlink"
            )
        created = not self.root.exists()
        self.root.mkdir(parents=True, exist_ok=True)
        for directory in (
            self.segment_directory,
            self.manifest_directory,
            self.staging_directory,
        ):
            if directory.exists() and directory.is_symlink():
                raise AcquisitionSourceRetentionIntegrityError(
                    "archive directory is a symlink"
                )
            directory.mkdir(exist_ok=True)
        if created:
            self._directory_fsync(self.root.parent)
        if not self.lock_path.exists():
            try:
                with self.lock_path.open("xb") as stream:
                    stream.write(LOCK_BYTES)
                    stream.flush()
                    os.fsync(stream.fileno())
                self._directory_fsync(self.root)
            except FileExistsError:
                pass
        if self.lock_path.is_symlink() or self.lock_path.read_bytes() != LOCK_BYTES:
            raise AcquisitionSourceRetentionIntegrityError(
                "archive lock anchor differs"
            )

    @contextmanager
    def _exclusive(self) -> Iterator[None]:
        thread_id = threading.get_ident()
        process = self._process_lock
        if not process.acquire(timeout=self._limits.lock_timeout_seconds):
            raise AcquisitionSourceRetentionBusy("archive_lock_busy")
        if self._exclusive_depth:
            if (
                self._exclusive_owner_thread != thread_id
                or self._exclusive_descriptor is None
            ):
                process.release()
                raise AcquisitionSourceRetentionIntegrityError(
                    "archive reentrant lock authority differs"
                )
            self._exclusive_depth += 1
            try:
                yield
            finally:
                self._exclusive_depth -= 1
                process.release()
            return
        descriptor: int | None = None
        locked = False
        try:
            descriptor = os.open(str(self.lock_path), os.O_RDWR)
            deadline = time.monotonic() + self._limits.lock_timeout_seconds
            while True:
                try:
                    _lock_descriptor(descriptor)
                    locked = True
                    break
                except (BlockingIOError, PermissionError, OSError) as err:
                    if isinstance(err, OSError) and err.errno not in {
                        None,
                        errno.EACCES,
                        errno.EAGAIN,
                        errno.EDEADLK,
                    }:
                        raise
                    if time.monotonic() >= deadline:
                        raise AcquisitionSourceRetentionBusy(
                            "archive_lock_busy"
                        ) from None
                    time.sleep(0.01)
            self._exclusive_owner_thread = thread_id
            self._exclusive_depth = 1
            self._exclusive_descriptor = descriptor
            os.lseek(descriptor, 0, os.SEEK_SET)
            if os.read(descriptor, len(LOCK_BYTES) + 1) != LOCK_BYTES:
                raise AcquisitionSourceRetentionIntegrityError(
                    "archive lock anchor differs"
                )
            yield
        finally:
            if descriptor is not None:
                try:
                    if locked:
                        if (
                            self._exclusive_depth != 1
                            or self._exclusive_owner_thread != thread_id
                            or self._exclusive_descriptor != descriptor
                        ):
                            raise AcquisitionSourceRetentionIntegrityError(
                                "archive lock depth differs"
                            )
                        self._exclusive_depth = 0
                        self._exclusive_owner_thread = None
                        self._exclusive_descriptor = None
                        _unlock_descriptor(descriptor)
                finally:
                    os.close(descriptor)
            process.release()

    def _cleanup_staging(self) -> None:
        entries = tuple(self.staging_directory.iterdir())
        if len(entries) > self._limits.max_staging_files:
            raise AcquisitionSourceRetentionCapacityError(
                "archive_staging_capacity"
            )
        changed = False
        for entry in entries:
            if (
                entry.is_symlink()
                or not entry.is_file()
                or not entry.name.endswith(".pending")
            ):
                raise AcquisitionSourceRetentionIntegrityError(
                    "archive staging contains an unknown entry"
                )
            entry.unlink()
            changed = True
        if changed:
            self._directory_fsync(self.staging_directory)

    def _write_stage(self, target: Path, payload: bytes, *, label: str) -> None:
        if target.exists():
            if target.is_symlink() or target.read_bytes() != payload:
                raise AcquisitionSourceRetentionIntegrityError(
                    f"{label}_content_conflict"
                )
            return
        stage = self.staging_directory / f"{target.name}.pending"
        if stage.exists():
            raise AcquisitionSourceRetentionIntegrityError(
                "archive stage was not recovered"
            )
        self._fault(f"{label}.before_stage")
        try:
            with stage.open("xb") as stream:
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            self._fault(f"{label}.after_stage_fsync")
            os.replace(stage, target)
            self._fault(f"{label}.after_publish")
            self._directory_fsync(target.parent)
            self._fault(f"{label}.after_directory_fsync")
        except OSError as err:
            if err.errno == errno.ENOSPC:
                raise AcquisitionSourceRetentionCapacityError(
                    "archive_filesystem_full"
                ) from err
            raise

    def _write_anchor(
        self,
        payload: bytes,
        *,
        expected_anchor_sha256: str | None,
    ) -> None:
        if self.anchor_path.exists():
            current = self._read_anchor()
            if (
                expected_anchor_sha256 is not None
                and current["anchor_sha256"] != expected_anchor_sha256
            ):
                raise AcquisitionSourceRetentionConflict(
                    "archive_anchor_cas_conflict"
                )
        stage = self.staging_directory / "anchor.json.pending"
        if stage.exists():
            raise AcquisitionSourceRetentionIntegrityError(
                "archive stage was not recovered"
            )
        self._fault("anchor.before_stage")
        try:
            with stage.open("xb") as stream:
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            self._fault("anchor.after_stage_fsync")
            os.replace(stage, self.anchor_path)
            self._fault("anchor.after_publish")
            self._directory_fsync(self.root)
            self._fault("anchor.after_directory_fsync")
        except OSError as err:
            if err.errno == errno.ENOSPC:
                raise AcquisitionSourceRetentionCapacityError(
                    "archive_filesystem_full"
                ) from err
            raise

    def initialize(self) -> dict[str, Any]:
        with self._local_lock:
            self._prepare_layout()
            with self._exclusive():
                self._cleanup_staging()
                if not self.anchor_path.exists():
                    manifest_bytes = _envelope(
                        MANIFEST_SCHEMA,
                        "manifest_sha256",
                        {
                            "archived_job_count": 0,
                            "generation": 0,
                            "previous_manifest_sha256": None,
                            "segment_bytes": 0,
                            "segments": [],
                            "source_binding_sha256": (
                                self._source_binding_sha256
                            ),
                        },
                    )
                    manifest = _decode_envelope(
                        manifest_bytes,
                        schema=MANIFEST_SCHEMA,
                        hash_field="manifest_sha256",
                    )
                    manifest_path = self.manifest_directory / (
                        f"{manifest['manifest_sha256']}.manifest.json"
                    )
                    self._write_stage(
                        manifest_path,
                        manifest_bytes,
                        label="manifest",
                    )
                    anchor_bytes = _envelope(
                        ANCHOR_SCHEMA,
                        "anchor_sha256",
                        {
                            "archived_job_count": 0,
                            "generation": 0,
                            "head_segment_sha256": None,
                            "manifest_sha256": manifest[
                                "manifest_sha256"
                            ],
                            "segment_bytes": 0,
                            "source_binding_sha256": (
                                self._source_binding_sha256
                            ),
                        },
                    )
                    self._write_anchor(
                        anchor_bytes,
                        expected_anchor_sha256=None,
                    )
                return self._audit_locked()

    def _read_anchor(self) -> dict[str, Any]:
        if self.anchor_path.is_symlink() or not self.anchor_path.is_file():
            raise AcquisitionSourceRetentionIntegrityError(
                "archive anchor is unavailable"
            )
        anchor = _decode_envelope(
            self.anchor_path.read_bytes(),
            schema=ANCHOR_SCHEMA,
            hash_field="anchor_sha256",
        )
        if anchor.get("source_binding_sha256") != self._source_binding_sha256:
            raise AcquisitionSourceRetentionIntegrityError(
                "archive source authority differs"
            )
        return anchor

    def _read_manifest(self, manifest_sha256: str) -> dict[str, Any]:
        digest = _sha(manifest_sha256, "manifest SHA-256")
        path = self.manifest_directory / f"{digest}.manifest.json"
        if path.is_symlink() or not path.is_file():
            raise AcquisitionSourceRetentionIntegrityError(
                "archive manifest is unavailable"
            )
        manifest = _decode_envelope(
            path.read_bytes(),
            schema=MANIFEST_SCHEMA,
            hash_field="manifest_sha256",
        )
        if manifest["manifest_sha256"] != digest:
            raise AcquisitionSourceRetentionIntegrityError(
                "archive manifest filename differs"
            )
        return manifest

    def _read_segment(self, segment_sha256: str) -> tuple[dict[str, Any], int]:
        digest = _sha(segment_sha256, "segment SHA-256")
        path = self.segment_directory / f"{digest}.sealed.json"
        if path.is_symlink() or not path.is_file():
            raise AcquisitionSourceRetentionIntegrityError(
                "sealed segment is unavailable"
            )
        payload = path.read_bytes()
        segment = _decode_envelope(
            payload,
            schema=SEGMENT_SCHEMA,
            hash_field="segment_sha256",
        )
        if segment["segment_sha256"] != digest:
            raise AcquisitionSourceRetentionIntegrityError(
                "sealed segment filename differs"
            )
        self._validate_segment_records(segment)
        return segment, len(payload)

    @staticmethod
    def _record_core(record: Mapping[str, Any]) -> dict[str, Any]:
        core = dict(record)
        core.pop("record_sha256", None)
        return core

    def _validate_segment_records(self, segment: Mapping[str, Any]) -> None:
        records = segment.get("records")
        if not isinstance(records, list) or not records:
            raise AcquisitionSourceRetentionIntegrityError(
                "sealed segment has no records"
            )
        if len(records) > self._limits.max_candidates:
            raise AcquisitionSourceRetentionCapacityError(
                "sealed segment record capacity"
            )
        candidates: list[str] = []
        for record in records:
            if not isinstance(record, dict) or record.get("schema") != RECORD_SCHEMA:
                raise AcquisitionSourceRetentionIntegrityError(
                    "sealed retention record schema differs"
                )
            record_sha = _sha(record.get("record_sha256"), "record SHA-256")
            if record_sha != _digest(self._record_core(record)):
                raise AcquisitionSourceRetentionIntegrityError(
                    "sealed retention record checksum differs"
                )
            candidate_sha = _sha(
                record.get("candidate_sha256"), "candidate SHA-256"
            )
            candidates.append(candidate_sha)
            if record.get("source_reclaim_authorized") is not False:
                raise AcquisitionSourceRetentionIntegrityError(
                    "sealed export claims unsupported source reclamation"
                )
            if (
                record.get("source_binding_sha256")
                != self._source_binding_sha256
            ):
                raise AcquisitionSourceRetentionIntegrityError(
                    "sealed retention record source authority differs"
                )
            raw_objects = record.get("raw_objects")
            receipts = record.get("raw_receipts")
            capture = record.get("raw_capture")
            if (
                not isinstance(raw_objects, dict)
                or not isinstance(receipts, dict)
                or not isinstance(capture, dict)
                or capture.get("schema") != RAW_ARTIFACT_CAPTURE_SCHEMA
                or capture.get("raw_store_schema_version")
                != RAW_ARTIFACT_SCHEMA_VERSION
            ):
                raise AcquisitionSourceRetentionIntegrityError(
                    "sealed raw capture structure differs"
                )
            members = capture.get("members")
            if not isinstance(members, list) or set(raw_objects) != set(receipts):
                raise AcquisitionSourceRetentionIntegrityError(
                    "sealed raw roles differ"
                )
            member_roles = {
                item.get("role")
                for item in members
                if isinstance(item, dict)
            }
            if member_roles != set(raw_objects):
                raise AcquisitionSourceRetentionIntegrityError(
                    "sealed capture members differ"
                )
            for role, value in raw_objects.items():
                if not isinstance(value, dict):
                    raise AcquisitionSourceRetentionIntegrityError(
                        "sealed raw object is invalid"
                    )
                try:
                    payload = base64.b64decode(
                        value.get("payload_base64"), validate=True
                    )
                except (TypeError, ValueError) as err:
                    raise AcquisitionSourceRetentionIntegrityError(
                        "sealed raw object encoding differs"
                    ) from err
                if (
                    hashlib.sha256(payload).hexdigest()
                    != value.get("content_sha256")
                    or len(payload) != value.get("size_bytes")
                    or receipts[role].get("content_sha256")
                    != value.get("content_sha256")
                ):
                    raise AcquisitionSourceRetentionIntegrityError(
                        "sealed raw object checksum differs"
                    )
        if candidates != sorted(candidates) or len(candidates) != len(set(candidates)):
            raise AcquisitionSourceRetentionIntegrityError(
                "sealed candidate ordering differs"
            )

    def _audit_locked(self) -> dict[str, Any]:
        anchor = self._read_anchor()
        manifest = self._read_manifest(str(anchor["manifest_sha256"]))
        if (
            manifest.get("generation") != anchor.get("generation")
            or manifest.get("archived_job_count")
            != anchor.get("archived_job_count")
            or manifest.get("segment_bytes") != anchor.get("segment_bytes")
            or manifest.get("source_binding_sha256")
            != self._source_binding_sha256
        ):
            raise AcquisitionSourceRetentionIntegrityError(
                "archive anchor and manifest differ"
            )
        segments = manifest.get("segments")
        if not isinstance(segments, list):
            raise AcquisitionSourceRetentionIntegrityError(
                "archive manifest segments are invalid"
            )
        if len(segments) > self._limits.max_archive_segments:
            raise AcquisitionSourceRetentionCapacityError(
                "archive segment capacity"
            )
        expected_previous: str | None = None
        expected_ordinal = 1
        archived_jobs = 0
        segment_bytes = 0
        candidate_index: dict[str, tuple[str, str]] = {}
        for entry in segments:
            if not isinstance(entry, dict):
                raise AcquisitionSourceRetentionIntegrityError(
                    "archive manifest segment entry differs"
                )
            digest = _sha(entry.get("segment_sha256"), "segment SHA-256")
            segment, file_bytes = self._read_segment(digest)
            if (
                file_bytes != entry.get("file_bytes")
                or segment.get("previous_segment_sha256") != expected_previous
                or segment.get("segment_ordinal") != expected_ordinal
                or len(segment["records"]) != entry.get("record_count")
            ):
                raise AcquisitionSourceRetentionIntegrityError(
                    "archive segment chain has a gap"
                )
            for record in segment["records"]:
                candidate = str(record["candidate_sha256"])
                if candidate in candidate_index:
                    raise AcquisitionSourceRetentionIntegrityError(
                        "archive candidate is duplicated"
                    )
                candidate_index[candidate] = (
                    digest,
                    str(record["record_sha256"]),
                )
            expected_previous = digest
            expected_ordinal += 1
            archived_jobs += len(segment["records"])
            segment_bytes += file_bytes
        if (
            archived_jobs != anchor.get("archived_job_count")
            or segment_bytes != anchor.get("segment_bytes")
            or expected_previous != anchor.get("head_segment_sha256")
            or archived_jobs > self._limits.max_archived_jobs
            or segment_bytes > self._limits.max_total_archive_bytes
        ):
            raise AcquisitionSourceRetentionIntegrityError(
                "archive aggregate accounting differs"
            )
        segment_files = tuple(self.segment_directory.iterdir())
        manifest_files = tuple(self.manifest_directory.iterdir())
        anchored_segments = {str(item["segment_sha256"]) for item in segments}
        if len(segment_files) > len(anchored_segments) + 1:
            raise AcquisitionSourceRetentionCapacityError(
                "archive orphan segment capacity"
            )
        orphan_segments: dict[str, tuple[dict[str, Any], int]] = {}
        physical_segment_bytes = 0
        for path in segment_files:
            if path.is_symlink() or _SAFE_FILE.fullmatch(path.name) is None:
                raise AcquisitionSourceRetentionIntegrityError(
                    "archive segment directory has an unknown entry"
                )
            digest = path.name.split(".", 1)[0]
            segment_value, file_bytes = self._read_segment(digest)
            physical_segment_bytes += file_bytes
            if digest not in anchored_segments:
                orphan_segments[digest] = (segment_value, file_bytes)
        if len(orphan_segments) > 1:
            raise AcquisitionSourceRetentionCapacityError(
                "archive orphan segment capacity"
            )
        for orphan_segment, _ in orphan_segments.values():
            if (
                orphan_segment.get("source_binding_sha256")
                != self._source_binding_sha256
                or orphan_segment.get("previous_segment_sha256")
                != anchor.get("head_segment_sha256")
                or orphan_segment.get("segment_ordinal")
                != int(anchor["generation"]) + 1
            ):
                raise AcquisitionSourceRetentionIntegrityError(
                    "archive orphan segment is not the immediate successor"
                )
        if physical_segment_bytes > self._limits.max_total_archive_bytes:
            raise AcquisitionSourceRetentionCapacityError(
                "archive physical byte capacity"
            )
        anchored_manifests: set[str] = set()
        cursor: dict[str, Any] | None = manifest
        expected_generation = int(anchor["generation"])
        while cursor is not None:
            digest = str(cursor["manifest_sha256"])
            if (
                digest in anchored_manifests
                or cursor.get("generation") != expected_generation
                or cursor.get("source_binding_sha256")
                != self._source_binding_sha256
            ):
                raise AcquisitionSourceRetentionIntegrityError(
                    "archive manifest history has a gap or cycle"
                )
            anchored_manifests.add(digest)
            previous = cursor.get("previous_manifest_sha256")
            if previous is None:
                if expected_generation != 0:
                    raise AcquisitionSourceRetentionIntegrityError(
                        "archive manifest history ends early"
                    )
                cursor = None
            else:
                expected_generation -= 1
                cursor = self._read_manifest(str(previous))
        if len(manifest_files) > len(anchored_manifests) + 1:
            raise AcquisitionSourceRetentionCapacityError(
                "archive orphan manifest capacity"
            )
        orphan_manifests: dict[str, dict[str, Any]] = {}
        for path in manifest_files:
            if path.is_symlink() or _SAFE_FILE.fullmatch(path.name) is None:
                raise AcquisitionSourceRetentionIntegrityError(
                    "archive manifest directory has an unknown entry"
                )
            digest = path.name.split(".", 1)[0]
            manifest_value = self._read_manifest(digest)
            if digest not in anchored_manifests:
                orphan_manifests[digest] = manifest_value
        if len(orphan_manifests) > 1:
            raise AcquisitionSourceRetentionCapacityError(
                "archive orphan manifest capacity"
            )
        for orphan_manifest in orphan_manifests.values():
            orphan_manifest_segments = orphan_manifest.get("segments")
            if (
                len(orphan_segments) != 1
                or not isinstance(orphan_manifest_segments, list)
                or orphan_manifest.get("source_binding_sha256")
                != self._source_binding_sha256
                or orphan_manifest.get("previous_manifest_sha256")
                != anchor.get("manifest_sha256")
                or orphan_manifest.get("generation")
                != int(anchor["generation"]) + 1
                or len(orphan_manifest_segments) != len(segments) + 1
                or orphan_manifest_segments[:-1] != segments
            ):
                raise AcquisitionSourceRetentionIntegrityError(
                    "archive orphan manifest is not the immediate successor"
                )
            orphan_digest, (orphan_segment, orphan_file_bytes) = next(
                iter(orphan_segments.items())
            )
            final_entry = orphan_manifest_segments[-1]
            if (
                not isinstance(final_entry, dict)
                or final_entry.get("segment_sha256") != orphan_digest
                or final_entry.get("file_bytes") != orphan_file_bytes
                or final_entry.get("record_count")
                != len(orphan_segment["records"])
                or orphan_manifest.get("archived_job_count")
                != int(anchor["archived_job_count"])
                + len(orphan_segment["records"])
                or orphan_manifest.get("segment_bytes")
                != int(anchor["segment_bytes"]) + orphan_file_bytes
            ):
                raise AcquisitionSourceRetentionIntegrityError(
                    "archive orphan manifest accounting differs"
                )
        result = {
            "archive_anchor_sha256": anchor["anchor_sha256"],
            "archived_job_count": archived_jobs,
            "contract": CONTRACT,
            "healthy": True,
            "orphan_manifest_count": len(orphan_manifests),
            "orphan_segment_count": len(orphan_segments),
            "reclaim_enabled": False,
            "physical_segment_bytes": physical_segment_bytes,
            "segment_bytes": segment_bytes,
            "segment_count": len(segments),
            "source_binding_sha256": self._source_binding_sha256,
        }
        if len(_canonical(result)) > MAX_DIAGNOSTIC_BYTES:
            raise AcquisitionSourceRetentionCapacityError(
                "archive diagnostics exceed bound"
            )
        return result

    def audit(self) -> dict[str, Any]:
        with self._local_lock, self._exclusive():
            self._cleanup_staging()
            return self._audit_locked()

    def _source_view(self) -> dict[str, Any]:
        raw = self._raw.stats()
        scheduler = self._scheduler.diagnostics()
        executions = self._executions.stats()
        capacity = self._capacity.stats()
        evidence = self._evidence.diagnostics()
        return {
            "capacity": capacity,
            "evidence": evidence,
            "execution": executions,
            "raw": raw,
            "scheduler": scheduler,
        }

    @staticmethod
    def _withheld(candidate: AcquisitionSourceCandidate, reason: str) -> tuple[
        RetentionDisposition,
        None,
    ]:
        return (
            RetentionDisposition(
                candidate_sha256=candidate.candidate_sha256,
                status="withheld",
                reason_code=reason,
                record_sha256=None,
            ),
            None,
        )

    @staticmethod
    def _terminal_execution(
        history: tuple[ExecutionReceipt, ...],
    ) -> ExecutionReceipt | None:
        terminal = tuple(item for item in history if item.terminal)
        if len(terminal) != 1:
            return None
        return terminal[0]

    def _plan_candidate(
        self,
        candidate: AcquisitionSourceCandidate,
        *,
        now: float,
    ) -> tuple[RetentionDisposition, dict[str, Any] | None]:
        bundle = self._raw.load_capture_bundle_bytes(candidate.raw_capture_id)
        if bundle is None:
            return self._withheld(candidate, "raw_capture_unavailable")
        capture, receipts, payloads = bundle
        if len(receipts) != 1 or set(receipts) != set(payloads):
            return self._withheld(candidate, "raw_capture_shape_unsupported")
        role = next(iter(receipts))
        raw_receipt = receipts[role]
        raw_payload = payloads[role]
        history = self._executions.load_job_receipts(
            candidate.acquisition_job_id
        )
        if len(history) > self._limits.max_execution_receipts_per_job:
            raise AcquisitionSourceRetentionCapacityError(
                "execution history exceeds retention bound"
            )
        terminal = self._terminal_execution(history)
        if terminal is None:
            return self._withheld(candidate, "execution_terminal_unavailable")
        if (
            terminal.outcome != "completed_shadow_raw"
            or terminal.scheduler_job_id != candidate.scheduler_job_id
            or terminal.capture_sha256 != capture.capture_receipt_sha256
            or terminal.raw_receipt_sha256 != raw_receipt.receipt_sha256
            or terminal.content_sha256 != raw_receipt.content_sha256
        ):
            raise AcquisitionSourceRetentionIntegrityError(
                "execution and raw capture bindings differ"
            )
        try:
            scheduler = self._scheduler.terminal_proof(
                candidate.scheduler_job_id
            )
        except SchedulerConflict:
            return self._withheld(candidate, "scheduler_terminal_unavailable")
        if scheduler is None:
            return self._withheld(candidate, "scheduler_terminal_unavailable")
        if (
            scheduler.state != "completed"
            or scheduler.last_transition != "completed"
            or scheduler.attempt_count != terminal.attempt_count
        ):
            raise AcquisitionSourceRetentionIntegrityError(
                "scheduler and execution terminal bindings differ"
            )
        capacity = self._capacity.load_for_job(
            provider=capture.provider,
            job_identity=candidate.acquisition_job_id,
        )
        if capacity is None or capacity.status != "COMMITTED":
            return self._withheld(candidate, "capacity_commit_unavailable")
        if (
            capacity.commit_sha256 != terminal.capacity_commit_sha256
            or capacity.capture_sha256 != capture.capture_receipt_sha256
            or capacity.receipt_sha256 != raw_receipt.receipt_sha256
            or capacity.content_sha256 != raw_receipt.content_sha256
            or capacity.actual_bytes != len(raw_payload)
            or capacity.actual_entries != 1
        ):
            raise AcquisitionSourceRetentionIntegrityError(
                "capacity and raw capture bindings differ"
            )
        try:
            downstream = self._evidence.load_terminal_consumption(
                candidate.evidence_stream_id,
                candidate.evidence_stream_sequence,
            )
        except AcquisitionEvidenceConflict:
            return self._withheld(candidate, "downstream_terminal_unavailable")
        if downstream is None:
            return self._withheld(candidate, "downstream_terminal_unavailable")
        producer = downstream.producer_receipt
        consumption = downstream.consumption_receipt
        if (
            producer.acquisition_job_id != candidate.acquisition_job_id
            or producer.stream_id != candidate.evidence_stream_id
            or producer.stream_sequence != candidate.evidence_stream_sequence
            or producer.content_sha256 != raw_receipt.content_sha256
            or producer.size_bytes != len(raw_payload)
            or downstream.payload != raw_payload
        ):
            raise AcquisitionSourceRetentionIntegrityError(
                "downstream producer and source raw bindings differ"
            )
        proof = downstream.proof
        expected_capacity = {
            **asdict(capacity),
            "schema": "stv3-worker-raw-capacity-receipt-evidence-v1",
        }
        expected_scheduler = {
            **scheduler.as_dict(),
            "proof_sha256": scheduler.proof_sha256,
        }
        if (
            proof.get("schema") != DWD_COMPLETION_PROOF_SCHEMA
            or proof.get("meteorological_claim") is not False
            or proof.get("source_capture") != capture.to_metadata()
            or proof.get("source_receipts")
            != {role: _safe_raw_receipt(raw_receipt)}
            or proof.get("execution_receipts")
            != [item.as_dict() for item in history]
            or proof.get("capacity_receipt") != expected_capacity
            or proof.get("scheduler_terminal") != expected_scheduler
        ):
            raise AcquisitionSourceRetentionIntegrityError(
                "downstream proof is not exact source evidence"
            )
        terminal_watermark = max(
            scheduler.terminal_at,
            terminal.created_at,
            consumption.created_at,
            float(capacity.committed_at or 0.0),
            capture.created_at,
        )
        if terminal_watermark > now - self._limits.min_terminal_age_seconds:
            return self._withheld(candidate, "hot_window_active")
        record = {
            "candidate": candidate.as_dict(),
            "candidate_sha256": candidate.candidate_sha256,
            "capacity_commit": capacity.to_mapping(),
            "downstream": {
                "archive_segment_id": downstream.archive_segment_id,
                "consumption_receipt": consumption.as_dict(),
                "producer_receipt": producer.as_dict(),
                "proof_sha256": _digest(proof),
                "storage_status": downstream.status,
            },
            "execution_history": [item.as_dict() for item in history],
            "raw_capture": capture.to_metadata(),
            "raw_objects": {
                role: {
                    "content_sha256": raw_receipt.content_sha256,
                    "payload_base64": "",
                    "size_bytes": len(raw_payload),
                }
            },
            "raw_receipts": {role: _safe_raw_receipt(raw_receipt)},
            "reclaim_withheld_reason": (
                "cross_store_atomic_prune_contract_unavailable"
            ),
            "scheduler_terminal": {
                **scheduler.as_dict(),
                "proof_sha256": scheduler.proof_sha256,
            },
            "schema": RECORD_SCHEMA,
            "source_binding_sha256": self._source_binding_sha256,
            "source_reclaim_authorized": False,
            "terminal_watermark": terminal_watermark,
        }
        encoded_payload_bytes = 4 * ((len(raw_payload) + 2) // 3)
        record_with_checksum = {**record, "record_sha256": "0" * 64}
        record_byte_upper_bound = (
            len(_canonical(record_with_checksum)) + encoded_payload_bytes
        )
        segment_shell_bytes = _envelope(
            SEGMENT_SCHEMA,
            "segment_sha256",
            {
                "batch_sha256": "0" * 64,
                "previous_segment_sha256": "0" * 64,
                "records": [],
                "sealed_at": terminal_watermark,
                "segment_ordinal": self._limits.max_archive_segments,
                "source_binding_sha256": self._source_binding_sha256,
            },
        )
        if (
            len(segment_shell_bytes) + record_byte_upper_bound
            > self._limits.max_segment_bytes
        ):
            raise AcquisitionSourceRetentionCapacityError(
                "raw payload exceeds pre-encoding segment budget"
            )
        record["raw_objects"][role]["payload_base64"] = (
            base64.b64encode(raw_payload).decode("ascii")
        )
        record["record_sha256"] = _digest(self._record_core(record))
        return (
            RetentionDisposition(
                candidate_sha256=candidate.candidate_sha256,
                status="sealed_export_eligible",
                reason_code="all_authorities_cross_bound",
                record_sha256=record["record_sha256"],
            ),
            record,
        )

    def plan(
        self,
        candidates: tuple[AcquisitionSourceCandidate, ...],
        *,
        now: float | None = None,
    ) -> AcquisitionSourceRetentionPlan:
        if not isinstance(candidates, tuple) or any(
            not isinstance(item, AcquisitionSourceCandidate)
            for item in candidates
        ):
            raise TypeError("candidates must be a tuple of retention candidates")
        if not 1 <= len(candidates) <= self._limits.max_candidates:
            raise AcquisitionSourceRetentionCapacityError(
                "retention candidate capacity"
            )
        candidate_ids = tuple(item.candidate_sha256 for item in candidates)
        if len(candidate_ids) != len(set(candidate_ids)):
            raise AcquisitionSourceRetentionConflict(
                "duplicate_retention_candidate"
            )
        wall_now = _finite(
            self._clock() if now is None else now,
            "retention plan time",
            minimum=0.0,
            maximum=2**53,
        )
        source_before = self._source_view()
        dispositions: list[RetentionDisposition] = []
        records: list[dict[str, Any]] = []
        for candidate in sorted(
            candidates, key=lambda item: item.candidate_sha256
        ):
            disposition, record = self._plan_candidate(
                candidate,
                now=wall_now,
            )
            dispositions.append(disposition)
            if record is not None:
                records.append(record)
        source_after = self._source_view()
        if source_before != source_after:
            raise AcquisitionSourceRetentionConflict(
                "source_view_changed_during_plan"
            )
        source_view_sha = _digest(source_after)
        plan_core = {
            "created_at": wall_now,
            "dispositions": [asdict(item) for item in dispositions],
            "limits_sha256": self._limits.fingerprint,
            "record_sha256s": [item["record_sha256"] for item in records],
            "schema": PLAN_SCHEMA,
            "source_binding_sha256": self._source_binding_sha256,
            "source_reclaim_authorized": False,
            "source_view_sha256": source_view_sha,
        }
        return AcquisitionSourceRetentionPlan(
            plan_sha256=_digest(plan_core),
            source_binding_sha256=self._source_binding_sha256,
            source_view_sha256=source_view_sha,
            created_at=wall_now,
            dispositions=tuple(dispositions),
            _record_payloads=tuple(_canonical(item) for item in records),
        )

    def _validate_plan(
        self,
        plan: AcquisitionSourceRetentionPlan,
    ) -> tuple[dict[str, Any], ...]:
        plan_sha256 = _sha(plan.plan_sha256, "retention plan SHA-256")
        source_binding_sha256 = _sha(
            plan.source_binding_sha256,
            "retention source binding SHA-256",
        )
        source_view_sha256 = _sha(
            plan.source_view_sha256,
            "retention source view SHA-256",
        )
        _finite(
            plan.created_at,
            "retention plan time",
            minimum=0.0,
            maximum=2**53,
        )
        if (
            not isinstance(plan.dispositions, tuple)
            or not 1 <= len(plan.dispositions) <= self._limits.max_candidates
            or not isinstance(plan._record_payloads, tuple)
            or len(plan._record_payloads) > self._limits.max_candidates
        ):
            raise AcquisitionSourceRetentionCapacityError(
                "retention plan capacity"
            )
        records: list[dict[str, Any]] = []
        payload_bytes = 0
        for payload in plan._record_payloads:
            if not isinstance(payload, bytes):
                raise AcquisitionSourceRetentionIntegrityError(
                    "retention record payload type differs"
                )
            payload_bytes += len(payload)
            if payload_bytes > self._limits.max_segment_bytes:
                raise AcquisitionSourceRetentionCapacityError(
                    "retention plan payload capacity"
                )
            try:
                decoded = json.loads(payload.decode("ascii"))
            except (UnicodeDecodeError, json.JSONDecodeError) as err:
                raise AcquisitionSourceRetentionIntegrityError(
                    "retention record payload is corrupt"
                ) from err
            if not isinstance(decoded, dict) or payload != _canonical(decoded):
                raise AcquisitionSourceRetentionIntegrityError(
                    "retention record payload is not canonical"
                )
            records.append(decoded)
        if records:
            self._validate_segment_records({"records": records})
        record_index = {
            str(record["candidate_sha256"]): str(record["record_sha256"])
            for record in records
        }
        candidate_sha256s: list[str] = []
        eligible_candidates: set[str] = set()
        for disposition in plan.dispositions:
            if not isinstance(disposition, RetentionDisposition):
                raise AcquisitionSourceRetentionIntegrityError(
                    "retention disposition type differs"
                )
            candidate_sha256 = _sha(
                disposition.candidate_sha256,
                "candidate SHA-256",
            )
            candidate_sha256s.append(candidate_sha256)
            if disposition.status == "sealed_export_eligible":
                record_sha256 = _sha(
                    disposition.record_sha256,
                    "retention record SHA-256",
                )
                if record_index.get(candidate_sha256) != record_sha256:
                    raise AcquisitionSourceRetentionIntegrityError(
                        "retention disposition and record differ"
                    )
                if disposition.reason_code != "all_authorities_cross_bound":
                    raise AcquisitionSourceRetentionIntegrityError(
                        "eligible retention reason differs"
                    )
                eligible_candidates.add(candidate_sha256)
            elif (
                disposition.status != "withheld"
                or disposition.record_sha256 is not None
                or disposition.reason_code not in _WITHHELD_REASONS
            ):
                raise AcquisitionSourceRetentionIntegrityError(
                    "retention disposition status differs"
                )
        if (
            candidate_sha256s != sorted(candidate_sha256s)
            or len(candidate_sha256s) != len(set(candidate_sha256s))
            or eligible_candidates != set(record_index)
        ):
            raise AcquisitionSourceRetentionIntegrityError(
                "retention plan candidate accounting differs"
            )
        plan_core = {
            "created_at": plan.created_at,
            "dispositions": [asdict(item) for item in plan.dispositions],
            "limits_sha256": self._limits.fingerprint,
            "record_sha256s": [
                item["record_sha256"] for item in records
            ],
            "schema": PLAN_SCHEMA,
            "source_binding_sha256": source_binding_sha256,
            "source_reclaim_authorized": False,
            "source_view_sha256": source_view_sha256,
        }
        if _digest(plan_core) != plan_sha256:
            raise AcquisitionSourceRetentionIntegrityError(
                "retention plan checksum differs"
            )
        return tuple(records)

    def validate_plan_evidence(
        self,
        plan: AcquisitionSourceRetentionPlan,
    ) -> dict[str, Any]:
        """Return a bounded, identifier-free summary after full plan validation.

        This is a read-only bridge for the separate reclamation-gap authority.
        It intentionally exposes neither record bytes nor a positive reclaim
        flag.  The private canonical record payloads, limits fingerprint and
        source binding are all revalidated before the summary is returned.
        """

        if not isinstance(plan, AcquisitionSourceRetentionPlan):
            raise TypeError("plan must be AcquisitionSourceRetentionPlan")
        records = self._validate_plan(plan)
        if plan.source_binding_sha256 != self._source_binding_sha256:
            raise AcquisitionSourceRetentionConflict(
                "retention plan source authority differs"
            )
        eligible = sum(
            item.status == "sealed_export_eligible"
            for item in plan.dispositions
        )
        summary = {
            "candidate_count": len(plan.dispositions),
            "contract": CONTRACT,
            "eligible_count": eligible,
            "plan_sha256": plan.plan_sha256,
            "record_count": len(records),
            "source_binding_sha256": plan.source_binding_sha256,
            "source_reclaim_authorized": False,
            "source_view_sha256": plan.source_view_sha256,
            "withheld_count": len(plan.dispositions) - eligible,
        }
        if eligible != len(records) or len(_canonical(summary)) > MAX_DIAGNOSTIC_BYTES:
            raise AcquisitionSourceRetentionIntegrityError(
                "retention plan evidence accounting differs"
            )
        return summary

    def _manifest_candidate_index(
        self,
        manifest: Mapping[str, Any],
    ) -> dict[str, tuple[str, str]]:
        result: dict[str, tuple[str, str]] = {}
        for entry in manifest["segments"]:
            segment, _ = self._read_segment(str(entry["segment_sha256"]))
            for record in segment["records"]:
                result[str(record["candidate_sha256"])] = (
                    str(segment["segment_sha256"]),
                    str(record["record_sha256"]),
                )
        return result

    def _assert_fresh_plan_records(
        self,
        records: tuple[dict[str, Any], ...],
        *,
        now: float,
    ) -> None:
        expected_candidate_fields = {
            "acquisition_job_id",
            "evidence_stream_id",
            "evidence_stream_sequence",
            "raw_capture_id",
            "scheduler_job_id",
        }
        for record in records:
            candidate_value = record.get("candidate")
            if (
                not isinstance(candidate_value, dict)
                or set(candidate_value) != expected_candidate_fields
            ):
                raise AcquisitionSourceRetentionIntegrityError(
                    "retention record candidate differs"
                )
            try:
                candidate = AcquisitionSourceCandidate(**candidate_value)
            except (TypeError, ValueError) as err:
                raise AcquisitionSourceRetentionIntegrityError(
                    "retention record candidate is invalid"
                ) from err
            if candidate.candidate_sha256 != record.get("candidate_sha256"):
                raise AcquisitionSourceRetentionIntegrityError(
                    "retention record candidate checksum differs"
                )
            disposition, fresh = self._plan_candidate(candidate, now=now)
            if disposition.status != "sealed_export_eligible" or fresh is None:
                raise AcquisitionSourceRetentionConflict(
                    "retention candidate is no longer eligible"
                )
            if _canonical(fresh) != _canonical(record):
                raise AcquisitionSourceRetentionIntegrityError(
                    "retention record differs from fresh source evidence"
                )

    def export(
        self,
        plan: AcquisitionSourceRetentionPlan,
        *,
        expected_anchor_sha256: str | None = None,
    ) -> AcquisitionSourceArchiveResult:
        if not isinstance(plan, AcquisitionSourceRetentionPlan):
            raise TypeError("plan must be AcquisitionSourceRetentionPlan")
        plan_records = self._validate_plan(plan)
        if plan.source_binding_sha256 != self._source_binding_sha256:
            raise AcquisitionSourceRetentionConflict(
                "retention plan source authority differs"
            )
        if expected_anchor_sha256 is not None:
            _sha(expected_anchor_sha256, "expected anchor SHA-256")
        with self._local_lock, self._exclusive():
            wall_now = _finite(
                self._clock(),
                "retention export time",
                minimum=0.0,
                maximum=2**53,
            )
            source_before_sha256 = _digest(self._source_view())
            if source_before_sha256 != plan.source_view_sha256:
                raise AcquisitionSourceRetentionConflict(
                    "retention plan source view expired"
                )
            self._assert_fresh_plan_records(plan_records, now=wall_now)
            if _digest(self._source_view()) != source_before_sha256:
                raise AcquisitionSourceRetentionConflict(
                    "source_view_changed_during_export_validation"
                )
            self._cleanup_staging()
            self._audit_locked()
            anchor = self._read_anchor()
            if (
                expected_anchor_sha256 is not None
                and anchor["anchor_sha256"] != expected_anchor_sha256
            ):
                raise AcquisitionSourceRetentionConflict(
                    "archive_anchor_cas_conflict"
                )
            manifest = self._read_manifest(str(anchor["manifest_sha256"]))
            archived = self._manifest_candidate_index(manifest)
            pending: list[dict[str, Any]] = []
            for record in plan_records:
                candidate = str(record["candidate_sha256"])
                prior = archived.get(candidate)
                if prior is None:
                    pending.append(record)
                elif prior[1] != record["record_sha256"]:
                    raise AcquisitionSourceRetentionIntegrityError(
                        "archived candidate proof differs"
                    )
            if not pending:
                return AcquisitionSourceArchiveResult(
                    status=("no_eligible_records" if not plan_records else "idempotent"),
                    segment_sha256=None,
                    archived_jobs=0,
                    archive_anchor_sha256=str(anchor["anchor_sha256"]),
                )
            if (
                len(manifest["segments"]) + 1
                > self._limits.max_archive_segments
                or int(anchor["archived_job_count"]) + len(pending)
                > self._limits.max_archived_jobs
            ):
                raise AcquisitionSourceRetentionCapacityError(
                    "archive row capacity"
                )
            pending.sort(key=lambda item: str(item["candidate_sha256"]))
            batch_sha = _digest([
                {
                    "candidate_sha256": item["candidate_sha256"],
                    "record_sha256": item["record_sha256"],
                }
                for item in pending
            ])
            segment_bytes = _envelope(
                SEGMENT_SCHEMA,
                "segment_sha256",
                {
                    "batch_sha256": batch_sha,
                    "previous_segment_sha256": anchor[
                        "head_segment_sha256"
                    ],
                    "records": pending,
                    "sealed_at": max(
                        float(item["terminal_watermark"]) for item in pending
                    ),
                    "segment_ordinal": int(anchor["generation"]) + 1,
                    "source_binding_sha256": self._source_binding_sha256,
                },
            )
            if len(segment_bytes) > self._limits.max_segment_bytes:
                raise AcquisitionSourceRetentionCapacityError(
                    "archive segment byte capacity"
                )
            if (
                int(anchor["segment_bytes"]) + len(segment_bytes)
                > self._limits.max_total_archive_bytes
            ):
                raise AcquisitionSourceRetentionCapacityError(
                    "archive total byte capacity"
                )
            segment = _decode_envelope(
                segment_bytes,
                schema=SEGMENT_SCHEMA,
                hash_field="segment_sha256",
            )
            segment_path = self.segment_directory / (
                f"{segment['segment_sha256']}.sealed.json"
            )
            anchored_segment_sha256s = {
                str(item["segment_sha256"])
                for item in manifest["segments"]
            }
            orphan_segment_sha256s = {
                item.name.split(".", 1)[0]
                for item in self.segment_directory.iterdir()
                if item.is_file()
                and not item.is_symlink()
                and item.name.split(".", 1)[0]
                not in anchored_segment_sha256s
            }
            if orphan_segment_sha256s - {str(segment["segment_sha256"])}:
                raise AcquisitionSourceRetentionConflict(
                    "archive_recovery_requires_exact_retry"
                )
            physical_bytes = sum(
                item.stat().st_size
                for item in self.segment_directory.iterdir()
                if item.is_file() and not item.is_symlink()
            )
            if (
                not segment_path.exists()
                and physical_bytes + len(segment_bytes)
                > self._limits.max_total_archive_bytes
            ):
                raise AcquisitionSourceRetentionCapacityError(
                    "archive physical byte capacity"
                )
            self._write_stage(
                segment_path,
                segment_bytes,
                label="segment",
            )
            segment_entry = {
                "file_bytes": len(segment_bytes),
                "record_count": len(pending),
                "segment_sha256": segment["segment_sha256"],
            }
            manifest_bytes = _envelope(
                MANIFEST_SCHEMA,
                "manifest_sha256",
                {
                    "archived_job_count": int(anchor["archived_job_count"])
                    + len(pending),
                    "generation": int(anchor["generation"]) + 1,
                    "previous_manifest_sha256": anchor[
                        "manifest_sha256"
                    ],
                    "segment_bytes": int(anchor["segment_bytes"])
                    + len(segment_bytes),
                    "segments": [*manifest["segments"], segment_entry],
                    "source_binding_sha256": self._source_binding_sha256,
                },
            )
            next_manifest = _decode_envelope(
                manifest_bytes,
                schema=MANIFEST_SCHEMA,
                hash_field="manifest_sha256",
            )
            self._write_stage(
                self.manifest_directory
                / f"{next_manifest['manifest_sha256']}.manifest.json",
                manifest_bytes,
                label="manifest",
            )
            anchor_bytes = _envelope(
                ANCHOR_SCHEMA,
                "anchor_sha256",
                {
                    "archived_job_count": next_manifest[
                        "archived_job_count"
                    ],
                    "generation": next_manifest["generation"],
                    "head_segment_sha256": segment["segment_sha256"],
                    "manifest_sha256": next_manifest[
                        "manifest_sha256"
                    ],
                    "segment_bytes": next_manifest["segment_bytes"],
                    "source_binding_sha256": self._source_binding_sha256,
                },
            )
            self._write_anchor(
                anchor_bytes,
                expected_anchor_sha256=str(anchor["anchor_sha256"]),
            )
            durable = self._audit_locked()
            return AcquisitionSourceArchiveResult(
                status="archived",
                segment_sha256=str(segment["segment_sha256"]),
                archived_jobs=len(pending),
                archive_anchor_sha256=str(
                    durable["archive_anchor_sha256"]
                ),
            )

    def lookup(
        self,
        candidate_sha256: str,
        *,
        include_archive: bool = True,
    ) -> AcquisitionSourceArchiveLookup:
        candidate = _sha(candidate_sha256, "candidate SHA-256")
        with self._local_lock, self._exclusive():
            self._cleanup_staging()
            self._audit_locked()
            anchor = self._read_anchor()
            manifest = self._read_manifest(str(anchor["manifest_sha256"]))
            for entry in manifest["segments"]:
                segment, _ = self._read_segment(
                    str(entry["segment_sha256"])
                )
                for record in segment["records"]:
                    if record["candidate_sha256"] != candidate:
                        continue
                    if not include_archive:
                        raise AcquisitionSourceRetentionWindowExpired(
                            "window_expired"
                        )
                    return AcquisitionSourceArchiveLookup(
                        status="archived",
                        candidate_sha256=candidate,
                        segment_sha256=str(segment["segment_sha256"]),
                        record=json.loads(_canonical(record)),
                    )
        return AcquisitionSourceArchiveLookup(
            status="unknown",
            candidate_sha256=candidate,
            segment_sha256=None,
        )

    def diagnostics(self) -> dict[str, Any]:
        return self.audit()


def _v2_ordered_root(
    *,
    schema: str,
    item_sha256s: tuple[str, ...],
) -> str:
    if not item_sha256s:
        return _digest({"count": 0, "schema": schema})
    head = _digest({"count": len(item_sha256s), "schema": schema, "start": True})
    for ordinal, item_sha in enumerate(item_sha256s):
        head = _digest({
            "item_sha256": _sha(item_sha, "v2 ordered-root item SHA-256"),
            "ordinal": ordinal,
            "previous_sha256": head,
            "schema": V2_OBJECT_REFERENCE_LINK_SCHEMA,
        })
    return _digest({
        "count": len(item_sha256s),
        "head_sha256": head,
        "schema": schema,
    })


def _v2_file_witness(path: Path) -> tuple[int, int, int, int]:
    try:
        metadata = path.lstat()
    except OSError as err:
        raise AcquisitionSourceRetentionIntegrityError(
            "v2 archive path authority is unavailable"
        ) from err
    if (
        stat.S_ISLNK(metadata.st_mode)
        or not stat.S_ISREG(metadata.st_mode)
        or metadata.st_nlink != 1
    ):
        raise AcquisitionSourceRetentionIntegrityError(
            "v2 archive path authority is unsafe"
        )
    return (
        int(metadata.st_dev),
        int(metadata.st_ino),
        int(metadata.st_size),
        int(metadata.st_mtime_ns),
    )


def _v2_root_sha256(path: Path) -> str:
    try:
        canonical = os.path.normcase(str(path.resolve(strict=True)))
    except OSError as err:
        raise AcquisitionSourceRetentionIntegrityError(
            "v2 archive root authority is unavailable"
        ) from err
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class AcquisitionSourceRetentionArchiveV2(SourceReclamationCandidateAuthority):
    """Content-addressed v2 source archive and held candidate authority.

    V1 remains byte-for-byte forensic.  This class owns a separate root and
    stores raw provider bytes only as checksum-addressed object files.  Segment
    and CandidatePage records contain bounded metadata references, never inline
    payloads, paths, tokens or error text.
    """

    def __init__(
        self,
        archive_root: str | Path,
        *,
        source_archive: AcquisitionSourceRetentionArchive,
        consumer_registry_authority: AcquisitionConsumerRegistryAuthority,
        raw_store: RawArtifactStore,
        scheduler: AutonomousScheduler,
        execution_store: ExecutionReceiptStore,
        capacity_ledger: RawCapacityLedger,
        evidence_store: AcquisitionEvidenceLifecycleStore,
        limits: AcquisitionSourceRetentionLimitsV2 | None = None,
        clock: Callable[[], float] = time.time,
        fault_injector: Callable[[str], None] | None = None,
        directory_fsync: Callable[[Path], None] | None = None,
    ) -> None:
        root = Path(archive_root)
        if not root.is_absolute():
            raise ValueError("v2 archive root must be absolute")
        if not isinstance(source_archive, AcquisitionSourceRetentionArchive):
            raise TypeError(
                "source_archive must be AcquisitionSourceRetentionArchive"
            )
        if not isinstance(
            consumer_registry_authority,
            AcquisitionConsumerRegistryAuthority,
        ):
            raise TypeError(
                "consumer_registry_authority must be a registry authority"
            )
        if not isinstance(raw_store, RawArtifactStore):
            raise TypeError("raw_store must be RawArtifactStore")
        if not isinstance(scheduler, AutonomousScheduler):
            raise TypeError("scheduler must be AutonomousScheduler")
        if not isinstance(execution_store, ExecutionReceiptStore):
            raise TypeError("execution_store must be ExecutionReceiptStore")
        if not isinstance(capacity_ledger, RawCapacityLedger):
            raise TypeError("capacity_ledger must be RawCapacityLedger")
        if not isinstance(evidence_store, AcquisitionEvidenceLifecycleStore):
            raise TypeError(
                "evidence_store must be AcquisitionEvidenceLifecycleStore"
            )
        if not callable(clock):
            raise TypeError("v2 clock must be callable")
        if fault_injector is not None and not callable(fault_injector):
            raise TypeError("v2 fault injector must be callable")
        if directory_fsync is not None and not callable(directory_fsync):
            raise TypeError("v2 directory fsync must be callable")
        if (
            source_archive._raw is not raw_store
            or source_archive._scheduler is not scheduler
            or source_archive._executions is not execution_store
            or source_archive._capacity is not capacity_ledger
            or source_archive._evidence is not evidence_store
        ):
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 closure source archive owns different source stores"
            )
        self.root = root
        self.object_directory = root / "objects"
        self.metadata_directory = root / "metadata"
        self.reference_directory = root / "references"
        self.segment_directory = root / "segments"
        self.manifest_directory = root / "manifests"
        self.anchor_directory = root / "anchors"
        self.candidate_set_directory = root / "candidate_sets"
        self.staging_directory = root / "staging"
        self.authority_path = root / "authority.json"
        self.head_path = root / "HEAD.json"
        self.pending_path = root / "PENDING.json"
        self.lock_path = root / "retention-v2.lock"
        self._raw = raw_store
        self._scheduler = scheduler
        self._executions = execution_store
        self._capacity = capacity_ledger
        self._evidence = evidence_store
        self._source_archive_v1 = source_archive
        self._consumer_registry_authority_v2 = consumer_registry_authority
        self._limits_v2 = limits or AcquisitionSourceRetentionLimitsV2()
        self._clock_v2 = clock
        self._fault_injector_v2 = fault_injector
        self._directory_fsync_v2 = (
            directory_fsync or AcquisitionSourceRetentionArchive._default_directory_fsync
        )
        self._local_lock_v2 = threading.RLock()
        self._process_lock_v2 = _process_lock(self.lock_path)
        self._exclusive_owner_thread_v2: int | None = None
        self._exclusive_depth_v2 = 0
        self._exclusive_descriptor_v2: int | None = None
        self._authority_witness: tuple[int, int, int, int] | None = None
        self._lock_witness: tuple[int, int, int, int] | None = None
        self._source_participants_v2: (
            tuple[SourceReclamationParticipantV2, ...] | None
        ) = None
        self._source_store_bindings_snapshot_v2: (
            tuple[dict[str, str], ...] | None
        ) = None
        self._coordinator_authority_sha256_v2: str | None = None
        self._assert_closure_authority_roots_disjoint_v2()
        registry_identity = consumer_registry_authority.assert_instance_held()
        source_archive_audit = source_archive.audit()
        self._consumer_registry_binding_v2 = {
            "consumer_registry_path_sha256": _v2_root_sha256(
                consumer_registry_authority.path
            ),
            "consumer_registry_store_identity_sha256": registry_identity,
        }
        self._closure_source_archive_binding_v2 = {
            "closure_source_archive_root_sha256": _v2_root_sha256(
                source_archive.root
            ),
            "closure_source_archive_source_binding_sha256": _sha(
                source_archive_audit.get("source_binding_sha256"),
                "v2 closure source archive binding SHA-256",
            ),
        }
        authority = self._initialize_v2()
        super().__init__(str(authority["authority_sha256"]))
        self._archive_instance_sha256 = str(
            authority["archive_instance_sha256"]
        )
        with self._local_lock_v2, self._exclusive_v2():
            self._recover_pending_v2()
            self._audit_locked_v2(verify_objects=False)

    def _assert_closure_authority_roots_disjoint_v2(self) -> None:
        """Keep both proof authorities physically outside the v2 root."""

        try:
            source_root = self._source_archive_v1.root.resolve(strict=True)
            target_root = self.root.resolve(strict=False)
            registry_paths = tuple(
                item.resolve(strict=True)
                for item in (
                    self._consumer_registry_authority_v2.path,
                    self._consumer_registry_authority_v2.lock_path,
                )
            )
        except OSError as err:
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 closure source archive root is unavailable"
            ) from err
        if (
            source_root == target_root
            or source_root.is_relative_to(target_root)
            or target_root.is_relative_to(source_root)
            or any(
                item == target_root or item.is_relative_to(target_root)
                for item in registry_paths
            )
        ):
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 closure source archive root overlaps v2 authority"
            )

    def _verify_registry_store_v2(self) -> str:
        """Reprove only the stable registry store/path authority."""

        try:
            identity = self._consumer_registry_authority_v2.assert_instance_held(
                self._consumer_registry_binding_v2[
                    "consumer_registry_store_identity_sha256"
                ]
            )
        except AcquisitionConsumerRegistryError as err:
            raise AcquisitionSourceRetentionConflict(
                "v2 consumer registry store authority is unavailable"
            ) from err
        if (
            _v2_root_sha256(self._consumer_registry_authority_v2.path)
            != self._consumer_registry_binding_v2[
                "consumer_registry_path_sha256"
            ]
        ):
            raise AcquisitionSourceRetentionConflict(
                "v2 consumer registry path changed"
            )
        return identity

    def _assert_source_archive_audit_v2(
        self,
        source_audit: Mapping[str, Any],
    ) -> None:
        current_source = {
            "closure_source_archive_root_sha256": _v2_root_sha256(
                self._source_archive_v1.root
            ),
            "closure_source_archive_source_binding_sha256": _sha(
                source_audit.get("source_binding_sha256"),
                "v2 closure source archive binding SHA-256",
            ),
        }
        if (
            source_audit.get("healthy") is not True
            or current_source != self._closure_source_archive_binding_v2
        ):
            raise AcquisitionSourceRetentionConflict(
                "v2 closure source archive advanced"
            )

    def _verify_live_closure_authorities_v2(self) -> Any:
        """Reprove the two stable create-once store/path bindings."""

        identity = self._verify_registry_store_v2()
        self._assert_source_archive_audit_v2(self._source_archive_v1.audit())
        return identity

    @contextmanager
    def _hold_source_archive_records_v2(
        self,
        candidates: tuple[AcquisitionSourceCandidate, ...],
        *,
        source_archive_anchor_sha256: str,
        require_current_anchor: bool,
    ) -> Iterator[dict[str, tuple[str, dict[str, Any]]]]:
        """Hold one audited current or historical v1 archive prefix."""

        candidate_sha256s = tuple(item.candidate_sha256 for item in candidates)
        if (
            not candidates
            or len(candidates) > self._limits_v2.max_candidates
            or len(set(candidate_sha256s)) != len(candidate_sha256s)
        ):
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 closure source lookup inventory differs"
            )
        requested = set(candidate_sha256s)
        archive = self._source_archive_v1
        pinned_anchor = _sha(
            source_archive_anchor_sha256,
            "v2 pinned closure source archive anchor SHA-256",
        )
        self._verify_registry_store_v2()
        with archive._local_lock, archive._exclusive():
            archive._cleanup_staging()
            source_audit = archive._audit_locked()
            self._assert_source_archive_audit_v2(source_audit)
            if (
                require_current_anchor
                and source_audit.get("archive_anchor_sha256") != pinned_anchor
            ):
                raise AcquisitionSourceRetentionConflict(
                    "v2 closure source archive anchor is stale"
                )
            manifest = self._v1_manifest_for_anchor_locked_v2(pinned_anchor)
            records: dict[str, tuple[str, dict[str, Any]]] = {}
            for entry in manifest["segments"]:
                segment, _payload = archive._read_segment(
                    str(entry["segment_sha256"])
                )
                for record in segment["records"]:
                    candidate_sha = str(record["candidate_sha256"])
                    if candidate_sha not in requested:
                        continue
                    if candidate_sha in records:
                        raise AcquisitionSourceRetentionIntegrityError(
                            "v2 closure source archive repeats a candidate"
                        )
                    records[candidate_sha] = (
                        str(segment["segment_sha256"]),
                        json.loads(_canonical(record)),
                    )
            if set(records) != requested:
                raise AcquisitionSourceRetentionConflict(
                    "v2 consumer closure source archive record is unavailable"
                )
            yield records
            archive._cleanup_staging()
            repeated_audit = archive._audit_locked()
            if repeated_audit != source_audit:
                raise AcquisitionSourceRetentionIntegrityError(
                    "v2 held closure source archive changed"
                )
            self._assert_source_archive_audit_v2(repeated_audit)
        self._verify_registry_store_v2()

    @staticmethod
    def _v1_anchor_from_manifest_v2(
        manifest: Mapping[str, Any],
    ) -> dict[str, Any]:
        segments = manifest.get("segments")
        if not isinstance(segments, list):
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 closure source manifest segment inventory differs"
            )
        head_segment = None if not segments else segments[-1].get(
            "segment_sha256"
        )
        anchor_bytes = _envelope(
            ANCHOR_SCHEMA,
            "anchor_sha256",
            {
                "archived_job_count": manifest.get("archived_job_count"),
                "generation": manifest.get("generation"),
                "head_segment_sha256": head_segment,
                "manifest_sha256": manifest.get("manifest_sha256"),
                "segment_bytes": manifest.get("segment_bytes"),
                "source_binding_sha256": manifest.get(
                    "source_binding_sha256"
                ),
            },
        )
        return _decode_envelope(
            anchor_bytes,
            schema=ANCHOR_SCHEMA,
            hash_field="anchor_sha256",
        )

    def _v1_manifest_for_anchor_locked_v2(
        self,
        anchor_sha256: str,
    ) -> dict[str, Any]:
        """Resolve an audited historical v1 anchor on the current prefix."""

        archive = self._source_archive_v1
        anchor_sha = _sha(
            anchor_sha256,
            "v2 historical closure source anchor SHA-256",
        )
        current = archive._read_anchor()
        manifest = archive._read_manifest(str(current["manifest_sha256"]))
        seen: set[str] = set()
        while True:
            manifest_sha = _sha(
                manifest.get("manifest_sha256"),
                "v2 closure source manifest SHA-256",
            )
            if manifest_sha in seen:
                raise AcquisitionSourceRetentionIntegrityError(
                    "v2 closure source manifest chain contains a cycle"
                )
            seen.add(manifest_sha)
            if len(seen) > archive._limits.max_archive_segments + 1:
                raise AcquisitionSourceRetentionCapacityError(
                    "v2 closure source manifest chain exceeds cap"
                )
            derived = self._v1_anchor_from_manifest_v2(manifest)
            if derived["anchor_sha256"] == anchor_sha:
                return manifest
            previous = manifest.get("previous_manifest_sha256")
            if previous is None:
                break
            manifest = archive._read_manifest(str(previous))
        raise AcquisitionSourceRetentionConflict(
            "v2 closure source anchor is not on the current prefix"
        )

    def _assert_v1_anchor_successor_v2(
        self,
        ancestor_anchor_sha256: str,
        descendant_anchor_sha256: str,
    ) -> None:
        """Prove a strict v1 manifest-prefix successor under its archive lock."""

        archive = self._source_archive_v1
        with archive._local_lock, archive._exclusive():
            archive._cleanup_staging()
            audit = archive._audit_locked()
            self._assert_source_archive_audit_v2(audit)
            ancestor = self._v1_manifest_for_anchor_locked_v2(
                ancestor_anchor_sha256
            )
            descendant = self._v1_manifest_for_anchor_locked_v2(
                descendant_anchor_sha256
            )
            ancestor_segments = tuple(
                str(item["segment_sha256"])
                for item in ancestor["segments"]
            )
            descendant_segments = tuple(
                str(item["segment_sha256"])
                for item in descendant["segments"]
            )
            if (
                int(descendant["generation"]) <= int(ancestor["generation"])
                or descendant_segments[:len(ancestor_segments)]
                != ancestor_segments
            ):
                raise AcquisitionSourceRetentionConflict(
                    "v2 closure source anchor is not a strict descendant"
                )

    def _reprove_consumer_closure_record_v2(
        self,
        candidate: AcquisitionSourceCandidate,
        closure: AcquisitionConsumerClosureReceipt,
        *,
        registry_verifier: AcquisitionConsumerRegistryBoundEpochVerifier | None,
        source_archive_anchor_sha256: str,
        source_segment_sha256: str,
        record: Mapping[str, Any],
        require_live_lifecycle: bool,
    ) -> None:
        """Reconstruct one closure from live registry, archive and lifecycle."""

        member = None
        if registry_verifier is not None:
            try:
                registry_verifier.validate_consumer_closure(closure)
            except AcquisitionConsumerRegistryError as err:
                raise AcquisitionSourceRetentionIntegrityError(
                    "v2 consumer closure differs from its current registry"
                ) from err
            member = registry_verifier.registry_root.by_workload.get(
                closure.workload_id
            )
            if member is None:
                raise AcquisitionSourceRetentionIntegrityError(
                    "v2 consumer closure workload is not registered"
                )
        record_candidate = record.get("candidate")
        if (
            not isinstance(record_candidate, dict)
            or source_segment_sha256 != closure.source_archive_segment_sha256
            or record.get("record_sha256")
            != closure.source_archive_record_sha256
            or closure.source_archive_anchor_sha256
            != source_archive_anchor_sha256
            or record_candidate.get("acquisition_job_id")
            != candidate.acquisition_job_id
            or record_candidate.get("evidence_stream_id")
            != candidate.evidence_stream_id
            or record_candidate.get("evidence_stream_sequence")
            != candidate.evidence_stream_sequence
            or closure.acquisition_job_id != candidate.acquisition_job_id
            or closure.stream_id != candidate.evidence_stream_id
            or closure.stream_sequence != candidate.evidence_stream_sequence
        ):
            raise AcquisitionSourceRetentionConflict(
                "v2 consumer closure source archive binding changed"
            )
        downstream = record.get("downstream")
        if not isinstance(downstream, dict):
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 closure source archive lifecycle proof is unavailable"
            )
        producer_value = downstream.get("producer_receipt")
        consumption_value = downstream.get("consumption_receipt")
        producer_fields = frozenset(CompletionProducerReceipt.__dataclass_fields__)
        consumption_fields = frozenset(ConsumptionReceipt.__dataclass_fields__)
        if (
            not isinstance(producer_value, dict)
            or frozenset(producer_value) != producer_fields | {"schema"}
            or not isinstance(consumption_value, dict)
            or frozenset(consumption_value) != consumption_fields | {"schema"}
            or downstream.get("storage_status") != "archived"
            or not isinstance(downstream.get("archive_segment_id"), str)
        ):
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 closure source archive lifecycle proof differs"
            )
        try:
            producer = CompletionProducerReceipt(**{
                key: producer_value[key] for key in producer_fields
            })
            consumption = ConsumptionReceipt(**{
                key: consumption_value[key] for key in consumption_fields
            })
            identity = TerminalEventIdentity(
                status="archived",
                event_id=producer.event_id,
                event_sha256=producer.event_sha256,
                producer_receipt_sha256=producer.producer_receipt_sha256,
                consumer_id=producer.consumer_id,
                stream_id=producer.stream_id,
                stream_sequence=producer.stream_sequence,
                terminal_kind="consumed",
                terminal_output_sha256=consumption.output_sha256,
                terminal_at=consumption.created_at,
                archive_segment_id=str(downstream["archive_segment_id"]),
            )
        except (KeyError, TypeError, ValueError) as err:
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 closure source archive lifecycle proof is invalid"
            ) from err
        if require_live_lifecycle:
            if registry_verifier is None:
                raise AcquisitionSourceRetentionIntegrityError(
                    "v2 live lifecycle closure lacks current registry hold"
                )
            live_terminal = self._evidence.load_terminal_consumption(
                closure.stream_id,
                closure.stream_sequence,
            )
            live_identity = self._evidence.terminal_event_identity(
                closure.stream_id,
                closure.stream_sequence,
            )
            if (
                live_terminal is None
                or live_terminal.status != "archived"
                or live_identity is None
                or live_identity.status != "archived"
                or live_terminal.producer_receipt != producer
                or live_terminal.consumption_receipt != consumption
                or live_identity != identity
            ):
                raise AcquisitionSourceRetentionConflict(
                    "v2 consumer closure live lifecycle terminal differs"
                )
        if registry_verifier is not None:
            try:
                expected = AcquisitionConsumerClosureReceipt.create(
                    candidate_sha256=candidate.candidate_sha256,
                    acquisition_job_id=candidate.acquisition_job_id,
                    member=member,
                    verification=registry_verifier.verification,
                    producer_receipt=producer,
                    consumption_receipt=consumption,
                    terminal_identity=identity,
                    source_archive_anchor_sha256=source_archive_anchor_sha256,
                    source_archive_segment_sha256=source_segment_sha256,
                    source_archive_record_sha256=str(record["record_sha256"]),
                )
            except AcquisitionConsumerRegistryError as err:
                raise AcquisitionSourceRetentionConflict(
                    "v2 consumer closure cannot be reconstructed"
                ) from err
            if expected != closure:
                raise AcquisitionSourceRetentionIntegrityError(
                    "v2 consumer closure receipt cannot be reproved"
                )
        elif (
            closure.registry_store_identity_sha256
            != self._consumer_registry_binding_v2[
                "consumer_registry_store_identity_sha256"
            ]
            or closure.lifecycle_event_id != producer.event_id
            or closure.lifecycle_event_sha256 != producer.event_sha256
            or closure.lifecycle_producer_receipt_sha256
            != producer.producer_receipt_sha256
            or closure.lifecycle_consumption_receipt_sha256
            != consumption.receipt_sha256
            or closure.lifecycle_terminal_output_sha256
            != consumption.output_sha256
            or closure.lifecycle_terminal_identity_sha256
            != identity.identity_sha256
            or closure.lifecycle_storage_status != "archived"
            or closure.lifecycle_archive_segment_id
            != identity.archive_segment_id
            or closure.lifecycle_terminal_at != identity.terminal_at
        ):
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 historical consumer closure differs from v1 record"
            )

    @contextmanager
    def _hold_reproved_consumer_closures_v2(
        self,
        pairs: tuple[
            tuple[AcquisitionSourceCandidate, AcquisitionConsumerClosureReceipt],
            ...,
        ],
        *,
        registry_verifier: AcquisitionConsumerRegistryBoundEpochVerifier | None,
        source_archive_anchor_sha256: str,
        require_current_source_anchor: bool,
        require_live_lifecycle: bool,
    ) -> Iterator[None]:
        """Hold and reprove the exact closure slice before and after use."""

        with self._hold_reproved_consumer_closure_groups_v2(
            ((source_archive_anchor_sha256, pairs),),
            registry_verifier=registry_verifier,
            require_current_source_anchor=require_current_source_anchor,
            require_live_lifecycle=require_live_lifecycle,
        ):
            yield

    @contextmanager
    def _hold_reproved_consumer_closure_groups_v2(
        self,
        groups: tuple[
            tuple[
                str,
                tuple[
                    tuple[
                        AcquisitionSourceCandidate,
                        AcquisitionConsumerClosureReceipt,
                    ],
                    ...,
                ],
            ],
            ...,
        ],
        *,
        registry_verifier: AcquisitionConsumerRegistryBoundEpochVerifier | None,
        require_current_source_anchor: bool,
        require_live_lifecycle: bool,
        source_anchor_supersession_edges: tuple[tuple[str, str], ...] = (),
    ) -> Iterator[None]:
        """Batch historical/current closure reproof under one v1 audit."""

        if not groups or any(not pairs for _anchor, pairs in groups):
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 closure reproof group inventory differs"
            )
        archive = self._source_archive_v1
        self._verify_registry_store_v2()
        with archive._local_lock, archive._exclusive():
            archive._cleanup_staging()
            source_audit = archive._audit_locked()
            self._assert_source_archive_audit_v2(source_audit)
            current = archive._read_anchor()
            manifest = archive._read_manifest(str(current["manifest_sha256"]))
            manifests: dict[str, dict[str, Any]] = {}
            seen: set[str] = set()
            while True:
                manifest_sha = _sha(
                    manifest.get("manifest_sha256"),
                    "v2 closure batch manifest SHA-256",
                )
                if manifest_sha in seen:
                    raise AcquisitionSourceRetentionIntegrityError(
                        "v2 closure source manifest chain contains a cycle"
                    )
                seen.add(manifest_sha)
                derived = self._v1_anchor_from_manifest_v2(manifest)
                manifests[str(derived["anchor_sha256"])] = manifest
                previous = manifest.get("previous_manifest_sha256")
                if previous is None:
                    break
                manifest = archive._read_manifest(str(previous))
            pinned_anchors = {
                _sha(anchor, "v2 closure group anchor SHA-256")
                for anchor, _pairs in groups
            }
            if not pinned_anchors <= set(manifests):
                raise AcquisitionSourceRetentionConflict(
                    "v2 closure source anchor is not on the current prefix"
                )
            if require_current_source_anchor and pinned_anchors != {
                source_audit["archive_anchor_sha256"]
            }:
                raise AcquisitionSourceRetentionConflict(
                    "v2 closure source archive anchor is stale"
                )
            for ancestor_sha, descendant_sha in source_anchor_supersession_edges:
                ancestor = manifests.get(_sha(
                    ancestor_sha,
                    "v2 superseded closure source anchor SHA-256",
                ))
                descendant = manifests.get(_sha(
                    descendant_sha,
                    "v2 successor closure source anchor SHA-256",
                ))
                if ancestor is None or descendant is None:
                    raise AcquisitionSourceRetentionIntegrityError(
                        "v2 closure source supersession anchor is unavailable"
                    )
                ancestor_segments = tuple(
                    str(item["segment_sha256"])
                    for item in ancestor["segments"]
                )
                descendant_segments = tuple(
                    str(item["segment_sha256"])
                    for item in descendant["segments"]
                )
                if (
                    int(descendant["generation"])
                    <= int(ancestor["generation"])
                    or descendant_segments[:len(ancestor_segments)]
                    != ancestor_segments
                ):
                    raise AcquisitionSourceRetentionIntegrityError(
                        "v2 closure source supersession is not a descendant"
                    )
            requested = {
                candidate.candidate_sha256
                for _anchor, pairs in groups
                for candidate, _closure in pairs
            }
            current_manifest = archive._read_manifest(
                str(current["manifest_sha256"])
            )
            records: dict[str, tuple[str, dict[str, Any]]] = {}
            for entry in current_manifest["segments"]:
                segment, _payload = archive._read_segment(
                    str(entry["segment_sha256"])
                )
                for record in segment["records"]:
                    candidate_sha = str(record["candidate_sha256"])
                    if candidate_sha not in requested:
                        continue
                    if candidate_sha in records:
                        raise AcquisitionSourceRetentionIntegrityError(
                            "v2 closure source archive repeats a candidate"
                        )
                    records[candidate_sha] = (
                        str(segment["segment_sha256"]),
                        json.loads(_canonical(record)),
                    )
            if set(records) != requested:
                raise AcquisitionSourceRetentionConflict(
                    "v2 closure source archive record is unavailable"
                )

            def reprove() -> None:
                for anchor, pairs in groups:
                    segment_ids = {
                        str(item["segment_sha256"])
                        for item in manifests[anchor]["segments"]
                    }
                    for candidate, closure in pairs:
                        segment_sha, record = records[
                            candidate.candidate_sha256
                        ]
                        if segment_sha not in segment_ids:
                            raise AcquisitionSourceRetentionConflict(
                                "v2 closure record is outside its pinned anchor"
                            )
                        self._reprove_consumer_closure_record_v2(
                            candidate,
                            closure,
                            registry_verifier=registry_verifier,
                            source_archive_anchor_sha256=anchor,
                            source_segment_sha256=segment_sha,
                            record=record,
                            require_live_lifecycle=require_live_lifecycle,
                        )

            reprove()
            yield
            reprove()
            archive._cleanup_staging()
            repeated_audit = archive._audit_locked()
            if repeated_audit != source_audit:
                raise AcquisitionSourceRetentionIntegrityError(
                    "v2 held closure source archive changed"
                )
            self._assert_source_archive_audit_v2(repeated_audit)
        self._verify_registry_store_v2()

    def _fault_v2(self, point: str) -> None:
        if self._fault_injector_v2 is not None:
            self._fault_injector_v2(f"retention_v2.{point}")

    def _barrier_v2(self, path: Path) -> None:
        self._directory_fsync_v2(path)

    def _prepare_layout_v2(self) -> None:
        if self.root.exists() and self.root.is_symlink():
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 archive root is a symlink"
            )
        created = not self.root.exists()
        self.root.mkdir(parents=True, exist_ok=True)
        for directory in (
            self.object_directory,
            self.metadata_directory,
            self.reference_directory,
            self.segment_directory,
            self.manifest_directory,
            self.anchor_directory,
            self.candidate_set_directory,
            self.staging_directory,
        ):
            if directory.exists() and directory.is_symlink():
                raise AcquisitionSourceRetentionIntegrityError(
                    "v2 archive directory is a symlink"
                )
            directory.mkdir(exist_ok=True)
            if directory.is_symlink() or not directory.is_dir():
                raise AcquisitionSourceRetentionIntegrityError(
                    "v2 archive directory is unsafe"
                )
        if created:
            self._barrier_v2(self.root.parent)
        if not self.lock_path.exists():
            try:
                descriptor = os.open(
                    self.lock_path,
                    os.O_WRONLY
                    | os.O_CREAT
                    | os.O_EXCL
                    | getattr(os, "O_BINARY", 0),
                    0o600,
                )
                try:
                    os.write(descriptor, V2_LOCK_BYTES)
                    os.fsync(descriptor)
                finally:
                    os.close(descriptor)
                self._barrier_v2(self.root)
            except FileExistsError:
                pass
        if self.lock_path.read_bytes() != V2_LOCK_BYTES:
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 archive lock authority differs"
            )
        self._lock_witness = _v2_file_witness(self.lock_path)

    def _verify_owner_paths_v2(self) -> None:
        if (
            self.root.is_symlink()
            or not self.root.is_dir()
            or self.lock_path.is_symlink()
            or self._lock_witness is None
            or _v2_file_witness(self.lock_path) != self._lock_witness
        ):
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 archive owner path changed"
            )
        if self._authority_witness is not None and (
            _v2_file_witness(self.authority_path) != self._authority_witness
        ):
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 archive authority file changed"
            )

    @contextmanager
    def _exclusive_v2(self) -> Iterator[None]:
        thread_id = threading.get_ident()
        if self._exclusive_depth_v2:
            if (
                self._exclusive_owner_thread_v2 != thread_id
                or self._exclusive_descriptor_v2 is None
            ):
                raise AcquisitionSourceRetentionIntegrityError(
                    "v2 archive reentrant lock authority differs"
                )
            self._exclusive_depth_v2 += 1
            try:
                self._verify_owner_paths_v2()
                yield
                self._verify_owner_paths_v2()
            finally:
                self._exclusive_depth_v2 -= 1
            return
        started = time.monotonic()
        while True:
            if self._process_lock_v2.acquire(blocking=False):
                break
            if time.monotonic() - started >= self._limits_v2.lock_timeout_seconds:
                raise AcquisitionSourceRetentionBusy("v2_archive_busy")
            time.sleep(0.01)
        descriptor: int | None = None
        descriptor_locked = False
        try:
            self._verify_owner_paths_v2()
            descriptor = os.open(
                self.lock_path,
                os.O_RDWR
                | getattr(os, "O_BINARY", 0)
                | getattr(os, "O_NOFOLLOW", 0),
            )
            if _v2_file_witness(self.lock_path) != self._lock_witness:
                raise AcquisitionSourceRetentionIntegrityError(
                    "v2 archive lock inode changed"
                )
            _lock_descriptor(descriptor)
            descriptor_locked = True
            self._exclusive_owner_thread_v2 = thread_id
            self._exclusive_depth_v2 = 1
            self._exclusive_descriptor_v2 = descriptor
            self._verify_owner_paths_v2()
            yield
            self._verify_owner_paths_v2()
        except BlockingIOError as err:
            raise AcquisitionSourceRetentionBusy("v2_archive_busy") from err
        finally:
            if descriptor is not None:
                try:
                    if descriptor_locked:
                        if self._exclusive_depth_v2 != 1:
                            raise AcquisitionSourceRetentionIntegrityError(
                                "v2 archive lock depth differs"
                            )
                        self._exclusive_depth_v2 = 0
                        self._exclusive_owner_thread_v2 = None
                        self._exclusive_descriptor_v2 = None
                        _unlock_descriptor(descriptor)
                finally:
                    os.close(descriptor)
            self._process_lock_v2.release()

    def _bounded_directory_v2(
        self,
        directory: Path,
        maximum: int,
    ) -> tuple[Path, ...]:
        items = tuple(directory.iterdir())
        if len(items) > maximum:
            raise AcquisitionSourceRetentionCapacityError(
                "v2 archive directory entry capacity exceeded"
            )
        if any(item.is_symlink() for item in items):
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 archive directory contains a symlink"
            )
        return items

    def _cleanup_staging_v2(self) -> None:
        for item in self._bounded_directory_v2(
            self.staging_directory,
            self._limits_v2.max_staging_files + 1,
        ):
            if not item.is_file() or item.stat().st_nlink != 1:
                raise AcquisitionSourceRetentionIntegrityError(
                    "v2 staging entry is unsafe"
                )
            item.unlink()
        self._barrier_v2(self.staging_directory)

    @staticmethod
    def _relative_target_v2(root: Path, relative: str) -> Path:
        if (
            not isinstance(relative, str)
            or "\\" in relative
            or relative.startswith("/")
            or relative.count("/") != 1
        ):
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 publication target path differs"
            )
        directory, filename = relative.split("/", 1)
        if directory not in {
            "objects", "metadata", "references", "segments", "manifests",
            "anchors", "candidate_sets",
        } or _V2_CONTENT_NAME.fullmatch(filename) is None:
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 publication target name differs"
            )
        target = root / directory / filename
        try:
            target.relative_to(root)
        except ValueError as err:  # pragma: no cover - shape precludes this
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 publication target escaped its root"
            ) from err
        return target

    def _read_exact_v2(
        self,
        path: Path,
        *,
        maximum: int,
        expected_sha256: str | None = None,
        expected_bytes: int | None = None,
    ) -> bytes:
        before = _v2_file_witness(path)
        if before[2] > maximum or (
            expected_bytes is not None and before[2] != expected_bytes
        ):
            raise AcquisitionSourceRetentionCapacityError(
                "v2 archive file exceeds its byte authority"
            )
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(path, flags)
        try:
            chunks: list[bytes] = []
            remaining = maximum + 1
            while remaining:
                chunk = os.read(descriptor, min(1024 * 1024, remaining))
                if not chunk:
                    break
                chunks.append(chunk)
                remaining -= len(chunk)
            payload = b"".join(chunks)
        finally:
            os.close(descriptor)
        if len(payload) > maximum or _v2_file_witness(path) != before:
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 archive file changed while reading"
            )
        if expected_sha256 is not None and not hmac.compare_digest(
            hashlib.sha256(payload).hexdigest(),
            _sha(expected_sha256, "v2 archive file SHA-256"),
        ):
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 archive file checksum differs"
            )
        return payload

    def _verify_exact_v2(
        self,
        path: Path,
        *,
        maximum: int,
        expected_sha256: str,
        expected_bytes: int,
    ) -> None:
        """Verify one immutable file in constant memory under one inode witness."""

        before = _v2_file_witness(path)
        if before[2] != expected_bytes or before[2] > maximum:
            raise AcquisitionSourceRetentionCapacityError(
                "v2 archive file exceeds its byte authority"
            )
        digest = hashlib.sha256()
        total = 0
        descriptor = os.open(
            path,
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
        )
        try:
            while True:
                chunk = os.read(descriptor, 1024 * 1024)
                if not chunk:
                    break
                total += len(chunk)
                if total > maximum:
                    raise AcquisitionSourceRetentionCapacityError(
                        "v2 archive file exceeds its byte authority"
                    )
                digest.update(chunk)
        finally:
            os.close(descriptor)
        if (
            total != expected_bytes
            or _v2_file_witness(path) != before
            or not hmac.compare_digest(
                digest.hexdigest(),
                _sha(expected_sha256, "v2 archive file SHA-256"),
            )
        ):
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 archive file checksum differs"
            )

    def _write_stage_v2(self, target: Path, payload: bytes) -> bool:
        expected = hashlib.sha256(payload).hexdigest()
        if target.exists() or target.is_symlink():
            self._read_exact_v2(
                target,
                maximum=max(len(payload), 1),
                expected_sha256=expected,
                expected_bytes=len(payload),
            )
            return False
        stage = self.staging_directory / (
            f".{target.name}.{secrets.token_hex(8)}.tmp"
        )
        descriptor: int | None = None
        try:
            descriptor = os.open(
                stage,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                0o600,
            )
            written = 0
            while written < len(payload):
                written += os.write(descriptor, payload[written:])
            os.fsync(descriptor)
            os.close(descriptor)
            descriptor = None
            os.replace(stage, target)
            self._barrier_v2(target.parent)
            self._read_exact_v2(
                target,
                maximum=max(len(payload), 1),
                expected_sha256=expected,
                expected_bytes=len(payload),
            )
            return True
        except OSError as err:
            if err.errno == errno.ENOSPC:
                raise AcquisitionSourceRetentionCapacityError(
                    "v2 archive filesystem full"
                ) from err
            raise
        finally:
            if descriptor is not None:
                os.close(descriptor)
            if stage.exists() and not stage.is_symlink():
                stage.unlink()

    def _write_mutable_v2(self, target: Path, payload: bytes) -> None:
        stage = self.staging_directory / (
            f".{target.name}.{secrets.token_hex(8)}.tmp"
        )
        descriptor: int | None = None
        try:
            descriptor = os.open(
                stage,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                0o600,
            )
            written = 0
            while written < len(payload):
                written += os.write(descriptor, payload[written:])
            os.fsync(descriptor)
            os.close(descriptor)
            descriptor = None
            os.replace(stage, target)
            self._barrier_v2(target.parent)
        finally:
            if descriptor is not None:
                os.close(descriptor)
            if stage.exists() and not stage.is_symlink():
                stage.unlink()

    def _read_envelope_v2(
        self,
        path: Path,
        *,
        schema: str,
        hash_field: str,
        maximum: int,
        expected_file_sha256: str | None = None,
    ) -> dict[str, Any]:
        payload = self._read_exact_v2(
            path,
            maximum=maximum,
            expected_sha256=expected_file_sha256,
        )
        return _decode_envelope(
            payload,
            schema=schema,
            hash_field=hash_field,
        )

    @staticmethod
    def _require_exact_keys_v2(
        value: Mapping[str, Any],
        expected: frozenset[str],
        *,
        label: str,
    ) -> None:
        if not isinstance(value, Mapping) or frozenset(value) != expected:
            raise AcquisitionSourceRetentionIntegrityError(
                f"v2 {label} shape differs"
            )

    @staticmethod
    def _require_count_v2(
        value: object,
        *,
        label: str,
        maximum: int,
    ) -> int:
        if (
            isinstance(value, bool)
            or not isinstance(value, int)
            or not 0 <= value <= maximum
        ):
            raise AcquisitionSourceRetentionIntegrityError(
                f"v2 {label} is outside its bound"
            )
        return value

    def _initialize_v2(self) -> dict[str, Any]:
        self._prepare_layout_v2()
        with self._local_lock_v2, self._exclusive_v2():
            self._cleanup_staging_v2()
            if self.authority_path.exists() or self.authority_path.is_symlink():
                authority = self._read_envelope_v2(
                    self.authority_path,
                    schema=V2_AUTHORITY_SCHEMA,
                    hash_field="authority_sha256",
                    maximum=4096,
                )
            else:
                if any(
                    self._bounded_directory_v2(directory, 1)
                    for directory in (
                        self.object_directory,
                        self.metadata_directory,
                        self.reference_directory,
                        self.segment_directory,
                        self.manifest_directory,
                        self.anchor_directory,
                        self.candidate_set_directory,
                    )
                ):
                    raise AcquisitionSourceRetentionIntegrityError(
                        "v2 archive lacks its create-once authority"
                    )
                authority_bytes = _envelope(
                    V2_AUTHORITY_SCHEMA,
                    "authority_sha256",
                    {
                        "archive_instance_sha256": secrets.token_hex(32),
                        "canonical_root_sha256": _v2_root_sha256(self.root),
                        **self._consumer_registry_binding_v2,
                        **self._closure_source_archive_binding_v2,
                        "contract": V2_CONTRACT,
                        "limits_sha256": self._limits_v2.fingerprint,
                        "lock_witness": list(self._lock_witness or ()),
                        "schema_version": 2,
                    },
                )
                self._write_mutable_v2(self.authority_path, authority_bytes)
                authority = _decode_envelope(
                    authority_bytes,
                    schema=V2_AUTHORITY_SCHEMA,
                    hash_field="authority_sha256",
                )
            self._require_exact_keys_v2(
                authority,
                frozenset({
                    "archive_instance_sha256",
                    "authority_sha256",
                    "canonical_root_sha256",
                    "closure_source_archive_root_sha256",
                    "closure_source_archive_source_binding_sha256",
                    "consumer_registry_path_sha256",
                    "consumer_registry_store_identity_sha256",
                    "contract",
                    "limits_sha256",
                    "lock_witness",
                    "schema",
                    "schema_version",
                }),
                label="archive authority",
            )
            if (
                authority.get("contract") != V2_CONTRACT
                or authority.get("schema_version") != 2
                or authority.get("canonical_root_sha256")
                != _v2_root_sha256(self.root)
                or authority.get("limits_sha256")
                != self._limits_v2.fingerprint
                or authority.get("lock_witness")
                != list(self._lock_witness or ())
                or any(
                    authority.get(key) != value
                    for key, value in {
                        **self._consumer_registry_binding_v2,
                        **self._closure_source_archive_binding_v2,
                    }.items()
                )
            ):
                raise AcquisitionSourceRetentionIntegrityError(
                    "v2 archive authority contract differs"
                )
            _sha(
                authority.get("archive_instance_sha256"),
                "v2 archive instance SHA-256",
            )
            self._verify_live_closure_authorities_v2()
            self._authority_witness = _v2_file_witness(self.authority_path)
            if not self.head_path.exists():
                prefix_root = _digest({
                    "archive_authority_sha256": authority["authority_sha256"],
                    "schema": V2_PREFIX_SCHEMA,
                    "start": True,
                })
                manifest_bytes = _envelope(
                    V2_MANIFEST_SCHEMA,
                    "manifest_sha256",
                    {
                        "archive_authority_sha256": authority["authority_sha256"],
                        "cumulative_file_bytes": 0,
                        "cumulative_object_bytes": 0,
                        "cumulative_object_count": 0,
                        "cumulative_record_count": 0,
                        "generation": 0,
                        "prefix_root_sha256": prefix_root,
                        "previous_manifest_sha256": None,
                        "segment_file_sha256": None,
                        "segment_sha256": None,
                        "source_store_set_root_sha256": None,
                    },
                )
                manifest = _decode_envelope(
                    manifest_bytes,
                    schema=V2_MANIFEST_SCHEMA,
                    hash_field="manifest_sha256",
                )
                anchor_bytes = _envelope(
                    V2_ANCHOR_SCHEMA,
                    "anchor_sha256",
                    {
                        "archive_authority_sha256": authority["authority_sha256"],
                        "cumulative_file_bytes": 0,
                        "cumulative_object_bytes": 0,
                        "cumulative_object_count": 0,
                        "cumulative_record_count": 0,
                        "generation": 0,
                        "head_segment_sha256": None,
                        "manifest_sha256": manifest["manifest_sha256"],
                        "prefix_root_sha256": prefix_root,
                        "previous_anchor_sha256": None,
                        "sealed_at": 0.0,
                        "source_store_set_root_sha256": None,
                    },
                )
                anchor = _decode_envelope(
                    anchor_bytes,
                    schema=V2_ANCHOR_SCHEMA,
                    hash_field="anchor_sha256",
                )
                self._write_stage_v2(
                    self.manifest_directory
                    / f"{manifest['manifest_sha256']}.manifest.json",
                    manifest_bytes,
                )
                self._write_stage_v2(
                    self.anchor_directory
                    / f"{anchor['anchor_sha256']}.anchor.json",
                    anchor_bytes,
                )
                head_bytes = _envelope(
                    V2_HEAD_SCHEMA,
                    "head_sha256",
                    {
                        "anchor_sha256": anchor["anchor_sha256"],
                        "archive_authority_sha256": authority["authority_sha256"],
                        "archive_instance_sha256": authority[
                            "archive_instance_sha256"
                        ],
                        "generation": 0,
                        "last_sealed_at": 0.0,
                        "prefix_root_sha256": prefix_root,
                        "source_store_set_root_sha256": None,
                    },
                )
                self._write_mutable_v2(self.head_path, head_bytes)
        return authority

    def _clear_pending_v2(self) -> None:
        if not (self.pending_path.exists() or self.pending_path.is_symlink()):
            return
        witness = _v2_file_witness(self.pending_path)
        if _v2_file_witness(self.pending_path) != witness:
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 pending publication changed before removal"
            )
        self.pending_path.unlink()
        self._barrier_v2(self.root)
        if self.pending_path.exists() or self.pending_path.is_symlink():
            raise AcquisitionSourceRetentionDurabilityError(
                "v2 pending publication remains"
            )

    def _pending_target_limit_v2(self, relative: str) -> int:
        directory = relative.split("/", 1)[0]
        limits = {
            "anchors": 64 * 1024,
            "candidate_sets": V2_MAX_CANDIDATE_SET_BYTES,
            "manifests": 64 * 1024,
            "metadata": self._limits_v2.max_metadata_bytes,
            "objects": self._limits_v2.max_object_bytes,
            "references": V2_MAX_REFERENCE_BYTES,
            "segments": self._limits_v2.max_segment_bytes,
        }
        try:
            return limits[directory]
        except KeyError as err:  # pragma: no cover - path parser closes this
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 pending target kind differs"
            ) from err

    def _recover_pending_v2(self) -> None:
        if not (self.pending_path.exists() or self.pending_path.is_symlink()):
            return
        pending = self._read_envelope_v2(
            self.pending_path,
            schema=V2_PENDING_SCHEMA,
            hash_field="pending_sha256",
            maximum=V2_MAX_PENDING_BYTES,
        )
        self._require_exact_keys_v2(
            pending,
            frozenset({
                "expected_head_sha256",
                "next_head",
                "pending_sha256",
                "schema",
                "targets",
            }),
            label="pending publication",
        )
        expected_head = _sha(
            pending["expected_head_sha256"],
            "v2 pending expected head SHA-256",
        )
        next_value = pending["next_head"]
        if not isinstance(next_value, dict):
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 pending next head shape differs"
            )
        next_head = _decode_envelope(
            _canonical(next_value),
            schema=V2_HEAD_SCHEMA,
            hash_field="head_sha256",
        )
        self._require_exact_keys_v2(
            next_head,
            frozenset({
                "anchor_sha256",
                "archive_authority_sha256",
                "archive_instance_sha256",
                "generation",
                "head_sha256",
                "last_sealed_at",
                "prefix_root_sha256",
                "schema",
                "source_store_set_root_sha256",
            }),
            label="pending next head",
        )
        targets_value = pending["targets"]
        maximum_targets = self._limits_v2.max_candidates * 3 + 4
        if (
            not isinstance(targets_value, list)
            or not 1 <= len(targets_value) <= maximum_targets
        ):
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 pending target inventory differs"
            )
        targets: list[tuple[Path, int, str, bool]] = []
        relative_names: set[str] = set()
        for item in targets_value:
            self._require_exact_keys_v2(
                item,
                frozenset({"bytes", "preexisting", "relative_path", "sha256"}),
                label="pending target",
            )
            relative = item["relative_path"]
            target = self._relative_target_v2(self.root, relative)
            size = self._require_count_v2(
                item["bytes"],
                label="pending target bytes",
                maximum=self._pending_target_limit_v2(relative),
            )
            content_sha = _sha(item["sha256"], "v2 pending target SHA-256")
            preexisting = item["preexisting"]
            if (
                not isinstance(preexisting, bool)
                or relative in relative_names
            ):
                raise AcquisitionSourceRetentionIntegrityError(
                    "v2 pending target inventory repeats"
                )
            relative_names.add(relative)
            targets.append((target, size, content_sha, preexisting))
        head = self._read_head_v2()
        if (
            next_head["archive_authority_sha256"] != self.authority_sha256
            or next_head["archive_instance_sha256"]
            != self._archive_instance_sha256
            or int(next_head["generation"]) != int(head["generation"]) + (
                0 if head["head_sha256"] == next_head["head_sha256"] else 1
            )
            or expected_head == next_head["head_sha256"]
        ):
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 pending head transition differs"
            )
        if head["head_sha256"] == next_head["head_sha256"]:
            if head != next_head:
                raise AcquisitionSourceRetentionIntegrityError(
                    "v2 installed pending head differs"
                )
            for target, size, content_sha, _preexisting in targets:
                self._read_exact_v2(
                    target,
                    maximum=max(size, 1),
                    expected_sha256=content_sha,
                    expected_bytes=size,
                )
            self._clear_pending_v2()
            return
        if head["head_sha256"] != expected_head:
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 pending publication has an unrelated head"
            )
        for target, size, content_sha, preexisting in reversed(targets):
            present = target.exists() or target.is_symlink()
            if preexisting:
                if not present:
                    raise AcquisitionSourceRetentionIntegrityError(
                        "v2 preexisting pending target disappeared"
                    )
                self._read_exact_v2(
                    target,
                    maximum=max(size, 1),
                    expected_sha256=content_sha,
                    expected_bytes=size,
                )
                continue
            if present:
                witness = _v2_file_witness(target)
                self._read_exact_v2(
                    target,
                    maximum=max(size, 1),
                    expected_sha256=content_sha,
                    expected_bytes=size,
                )
                if _v2_file_witness(target) != witness:
                    raise AcquisitionSourceRetentionIntegrityError(
                        "v2 pending target changed before rollback"
                    )
                target.unlink()
                self._barrier_v2(target.parent)
                if target.exists() or target.is_symlink():
                    raise AcquisitionSourceRetentionDurabilityError(
                        "v2 pending target remains after rollback"
                    )
        self._clear_pending_v2()

    def _read_head_v2(self) -> dict[str, Any]:
        head = self._read_envelope_v2(
            self.head_path,
            schema=V2_HEAD_SCHEMA,
            hash_field="head_sha256",
            maximum=8192,
        )
        self._require_exact_keys_v2(
            head,
            frozenset({
                "anchor_sha256",
                "archive_authority_sha256",
                "archive_instance_sha256",
                "generation",
                "head_sha256",
                "last_sealed_at",
                "prefix_root_sha256",
                "schema",
                "source_store_set_root_sha256",
            }),
            label="head",
        )
        if (
            head.get("archive_authority_sha256") != self.authority_sha256
            or head.get("archive_instance_sha256")
            != self._archive_instance_sha256
            or not isinstance(head.get("generation"), int)
            or not 0 <= int(head["generation"]) <= self._limits_v2.max_archive_segments
        ):
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 archive head authority differs"
            )
        for key in ("anchor_sha256", "head_sha256", "prefix_root_sha256"):
            _sha(head.get(key), f"v2 head {key}")
        source_root = head.get("source_store_set_root_sha256")
        if source_root is not None:
            _sha(source_root, "v2 head source-store root SHA-256")
        _finite(
            head.get("last_sealed_at"),
            "v2 head clock",
            minimum=0.0,
            maximum=32_503_680_000.0,
        )
        return head

    def _read_anchor_v2(self, anchor_sha256: str) -> dict[str, Any]:
        anchor_sha = _sha(anchor_sha256, "v2 anchor SHA-256")
        anchor = self._read_envelope_v2(
            self.anchor_directory / f"{anchor_sha}.anchor.json",
            schema=V2_ANCHOR_SCHEMA,
            hash_field="anchor_sha256",
            maximum=64 * 1024,
        )
        self._require_exact_keys_v2(
            anchor,
            frozenset({
                "anchor_sha256",
                "archive_authority_sha256",
                "cumulative_file_bytes",
                "cumulative_object_bytes",
                "cumulative_object_count",
                "cumulative_record_count",
                "generation",
                "head_segment_sha256",
                "manifest_sha256",
                "prefix_root_sha256",
                "previous_anchor_sha256",
                "schema",
                "sealed_at",
                "source_store_set_root_sha256",
            }),
            label="anchor",
        )
        if (
            anchor.get("anchor_sha256") != anchor_sha
            or anchor.get("archive_authority_sha256") != self.authority_sha256
        ):
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 anchor identity differs"
            )
        return anchor

    def _read_manifest_v2(self, manifest_sha256: str) -> dict[str, Any]:
        manifest_sha = _sha(manifest_sha256, "v2 manifest SHA-256")
        manifest = self._read_envelope_v2(
            self.manifest_directory / f"{manifest_sha}.manifest.json",
            schema=V2_MANIFEST_SCHEMA,
            hash_field="manifest_sha256",
            maximum=64 * 1024,
        )
        self._require_exact_keys_v2(
            manifest,
            frozenset({
                "archive_authority_sha256",
                "cumulative_file_bytes",
                "cumulative_object_bytes",
                "cumulative_object_count",
                "cumulative_record_count",
                "generation",
                "manifest_sha256",
                "prefix_root_sha256",
                "previous_manifest_sha256",
                "schema",
                "segment_file_sha256",
                "segment_sha256",
                "source_store_set_root_sha256",
            }),
            label="manifest",
        )
        if (
            manifest.get("manifest_sha256") != manifest_sha
            or manifest.get("archive_authority_sha256")
            != self.authority_sha256
        ):
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 manifest identity differs"
            )
        return manifest

    def _read_segment_v2(
        self,
        segment_sha256: str,
        *,
        expected_file_sha256: str | None = None,
    ) -> tuple[dict[str, Any], bytes]:
        segment_sha = _sha(segment_sha256, "v2 segment SHA-256")
        path = self.segment_directory / f"{segment_sha}.segment.json"
        payload = self._read_exact_v2(
            path,
            maximum=self._limits_v2.max_segment_bytes,
            expected_sha256=expected_file_sha256,
        )
        segment = _decode_envelope(
            payload,
            schema=V2_SEGMENT_SCHEMA,
            hash_field="segment_file_identity_sha256",
        )
        self._require_exact_keys_v2(
            segment,
            frozenset({
                "archive_authority_sha256",
                "archive_binding_root_sha256",
                "batch_record_root_sha256",
                "candidate_bindings_sha256",
                "generation",
                "previous_segment_sha256",
                "record_reference_sha256s",
                "schema",
                "sealed_at",
                "segment_file_identity_sha256",
                "segment_sha256",
                "source_store_set_root_sha256",
            }),
            label="segment",
        )
        if segment.get("segment_sha256") != segment_sha:
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 segment identity differs"
            )
        return segment, payload

    def _read_candidate_set_v2(
        self,
        candidate_bindings_sha256: str,
    ) -> dict[str, Any]:
        root = _sha(
            candidate_bindings_sha256,
            "v2 candidate-set root SHA-256",
        )
        candidate_set = self._read_envelope_v2(
            self.candidate_set_directory / f"{root}.set.json",
            schema=V2_CANDIDATE_SET_SCHEMA,
            hash_field="candidate_set_sha256",
            maximum=V2_MAX_CANDIDATE_SET_BYTES,
        )
        self._require_exact_keys_v2(
            candidate_set,
            frozenset({
                "anchor_sha256",
                "archive_binding_root_sha256",
                "candidate_authority_sha256",
                "candidate_bindings_sha256",
                "candidate_count",
                "candidate_set_sha256",
                "closure_source_archive_anchor_sha256",
                "closure_source_archive_root_sha256",
                "closure_source_archive_source_binding_sha256",
                "consumer_closure_root_sha256",
                "consumer_registry_epoch",
                "consumer_registry_epoch_receipt_sha256",
                "consumer_epoch_root_sha256",
                "consumer_registry_root_sha256",
                "consumer_registry_store_identity_sha256",
                "entries",
                "object_bytes",
                "object_count",
                "schema",
                "segment_sha256",
                "source_store_set_root_sha256",
                "superseded_candidate_bindings_sha256",
                "superseded_consumer_registry_epoch",
            }),
            label="candidate set",
        )
        if candidate_set.get("candidate_bindings_sha256") != root:
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 candidate-set identity differs"
            )
        return candidate_set

    def _validate_source_participants_v2(
        self,
        participants: tuple[SourceReclamationParticipantV2, ...],
    ) -> tuple[tuple[dict[str, str], ...], str]:
        if (
            not isinstance(participants, tuple)
            or len(participants) != len(PARTICIPANT_APPLY_ORDER)
            or any(
                not isinstance(item, SourceReclamationParticipantV2)
                for item in participants
            )
            or len({id(item) for item in participants}) != len(participants)
        ):
            raise TypeError(
                "v2 source participants must be one exact protocol tuple"
            )
        roles = tuple(item.role for item in participants)
        if roles != PARTICIPANT_APPLY_ORDER:
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 source participant role order differs"
            )
        stores: dict[str, object] = {
            "capacity_ledger": self._capacity,
            "downstream_lifecycle": self._evidence,
            "execution_receipts": self._executions,
            "raw_artifacts": self._raw,
            "scheduler": self._scheduler,
        }
        bindings: list[dict[str, str]] = []
        coordinator_authorities: set[str] = set()
        for role, participant in zip(
            PARTICIPANT_APPLY_ORDER,
            participants,
            strict=True,
        ):
            store = stores[role]
            owner_matches = (
                participant is store
                if role == "raw_artifacts"
                else getattr(participant, "_store", None) is store
            )
            if not owner_matches:
                raise AcquisitionSourceRetentionIntegrityError(
                    f"v2 {role} participant owns another store object"
                )
            candidate_authority = _sha(
                participant.candidate_authority_sha256,
                f"v2 {role} candidate authority SHA-256",
            )
            if not hmac.compare_digest(
                candidate_authority,
                self.authority_sha256,
            ):
                raise AcquisitionSourceRetentionIntegrityError(
                    f"v2 {role} candidate authority differs"
                )
            coordinator_authorities.add(_sha(
                participant.coordinator_authority_sha256,
                f"v2 {role} coordinator authority SHA-256",
            ))
            bindings.append({
                "role": role,
                "source_store_identity_sha256": _sha(
                    participant.source_store_identity_sha256,
                    f"v2 {role} source-store identity SHA-256",
                ),
            })
        if (
            len(coordinator_authorities) != 1
            or self.authority_sha256 in coordinator_authorities
        ):
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 source participant coordinator authority differs"
            )
        if (
            len({item["source_store_identity_sha256"] for item in bindings})
            != len(PARTICIPANT_APPLY_ORDER)
        ):
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 source-store identities overlap"
            )
        return tuple(bindings), next(iter(coordinator_authorities))

    def bind_source_participants_v2(
        self,
        participants: tuple[SourceReclamationParticipantV2, ...],
    ) -> str:
        """Create-once bind the five live stores through their participants."""

        with self._local_lock_v2, self._exclusive_v2():
            self._recover_pending_v2()
            bindings, coordinator_authority = (
                self._validate_source_participants_v2(participants)
            )
            source_root = self._source_store_set_root_v2(bindings)
            if self._source_participants_v2 is not None:
                if (
                    len(self._source_participants_v2) != len(participants)
                    or any(
                        current is not repeated
                        for current, repeated in zip(
                            self._source_participants_v2,
                            participants,
                            strict=True,
                        )
                    )
                    or bindings != self._source_store_bindings_snapshot_v2
                    or coordinator_authority
                    != self._coordinator_authority_sha256_v2
                ):
                    raise AcquisitionSourceRetentionConflict(
                        "v2 source participants are already bound"
                    )
                return source_root
            audit = self._audit_locked_v2(verify_objects=False)
            persisted_root = audit["source_store_set_root_sha256"]
            if persisted_root not in {None, source_root}:
                raise AcquisitionSourceRetentionIntegrityError(
                    "v2 live source-store root differs from archive head"
                )
            self._source_participants_v2 = participants
            self._source_store_bindings_snapshot_v2 = bindings
            self._coordinator_authority_sha256_v2 = coordinator_authority
            return source_root

    def _source_store_bindings_v2(self) -> tuple[dict[str, str], ...]:
        participants = self._source_participants_v2
        expected = self._source_store_bindings_snapshot_v2
        expected_coordinator = self._coordinator_authority_sha256_v2
        if (
            participants is None
            or expected is None
            or expected_coordinator is None
        ):
            raise AcquisitionSourceRetentionConflict(
                "source_participants_unbound"
            )
        bindings, coordinator_authority = (
            self._validate_source_participants_v2(participants)
        )
        if (
            bindings != expected
            or not hmac.compare_digest(
                coordinator_authority,
                expected_coordinator,
            )
        ):
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 bound source participants changed"
            )
        return bindings

    def _reproof_source_store_root_v2(
        self,
        expected_root_sha256: str | None,
    ) -> str:
        bindings = self._source_store_bindings_v2()
        source_root = self._source_store_set_root_v2(bindings)
        if expected_root_sha256 is not None and not hmac.compare_digest(
            source_root,
            _sha(
                expected_root_sha256,
                "v2 expected source-store root SHA-256",
            ),
        ):
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 live source-store root differs"
            )
        return source_root

    @staticmethod
    def _source_store_set_root_v2(
        bindings: tuple[dict[str, str], ...],
    ) -> str:
        if tuple(item["role"] for item in bindings) != PARTICIPANT_APPLY_ORDER:
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 source-store role inventory differs"
            )
        return _v2_ordered_root(
            schema="stv3-worker-source-retention-store-set-root-v2",
            item_sha256s=tuple(_digest(item) for item in bindings),
        )

    @staticmethod
    def _consumer_closure_root_v2(
        closures: tuple[AcquisitionConsumerClosureReceipt, ...],
    ) -> str:
        return _v2_ordered_root(
            schema="stv3-worker-source-retention-consumer-closure-root-v2",
            item_sha256s=tuple(item.receipt_sha256 for item in closures),
        )

    def _candidate_snapshot_v2(
        self,
        candidate: AcquisitionSourceCandidate,
        closure: AcquisitionConsumerClosureReceipt,
        *,
        now: float,
        source_store_bindings: tuple[dict[str, str], ...],
        source_store_set_root_sha256: str,
    ) -> tuple[dict[str, Any], tuple[tuple[str, bytes], ...]]:
        if (
            not isinstance(closure, AcquisitionConsumerClosureReceipt)
            or closure.candidate_sha256 != candidate.candidate_sha256
            or closure.acquisition_job_id != candidate.acquisition_job_id
            or closure.stream_id != candidate.evidence_stream_id
            or closure.stream_sequence != candidate.evidence_stream_sequence
            or closure.lifecycle_storage_status != "archived"
        ):
            raise AcquisitionSourceRetentionConflict(
                "consumer_closure_candidate_binding_differs"
            )
        bundle = self._raw.load_capture_bundle_bytes(candidate.raw_capture_id)
        if bundle is None:
            raise AcquisitionSourceRetentionConflict(
                "raw_capture_unavailable"
            )
        capture, receipts, payloads = bundle
        if (
            not receipts
            or set(receipts) != set(payloads)
            or len(receipts) > 64
        ):
            raise AcquisitionSourceRetentionConflict(
                "raw_capture_shape_unsupported"
            )
        history = self._executions.load_job_receipts(
            candidate.acquisition_job_id
        )
        if len(history) > self._limits_v2.max_execution_receipts_per_job:
            raise AcquisitionSourceRetentionCapacityError(
                "v2 execution history exceeds bound"
            )
        terminal = AcquisitionSourceRetentionArchive._terminal_execution(history)
        if terminal is None or terminal.outcome != "completed_shadow_raw":
            raise AcquisitionSourceRetentionConflict(
                "execution_terminal_unavailable"
            )
        try:
            scheduler = self._scheduler.terminal_proof(
                candidate.scheduler_job_id
            )
        except SchedulerConflict as err:
            raise AcquisitionSourceRetentionConflict(
                "scheduler_terminal_unavailable"
            ) from err
        if scheduler is None:
            raise AcquisitionSourceRetentionConflict(
                "scheduler_terminal_unavailable"
            )
        capacity = self._capacity.load_for_job(
            provider=capture.provider,
            job_identity=candidate.acquisition_job_id,
        )
        if capacity is None or capacity.status != "COMMITTED":
            raise AcquisitionSourceRetentionConflict(
                "capacity_commit_unavailable"
            )
        downstream = self._evidence.load_terminal_consumption(
            candidate.evidence_stream_id,
            candidate.evidence_stream_sequence,
        )
        if downstream is None or downstream.status != "archived":
            raise AcquisitionSourceRetentionConflict(
                "downstream_terminal_unavailable"
            )
        if len(receipts) != 1:
            raise AcquisitionSourceRetentionConflict(
                "raw_capture_shape_unsupported"
            )
        raw_role = next(iter(sorted(receipts)))
        raw_receipt = receipts[raw_role]
        raw_payload = payloads[raw_role]
        producer = downstream.producer_receipt
        consumption = downstream.consumption_receipt
        identity = self._evidence.terminal_event_identity(
            candidate.evidence_stream_id,
            candidate.evidence_stream_sequence,
        )
        if identity is None or identity.status != "archived":
            raise AcquisitionSourceRetentionConflict(
                "downstream_terminal_unavailable"
            )
        if (
            terminal.scheduler_job_id != candidate.scheduler_job_id
            or terminal.capture_sha256 != capture.capture_receipt_sha256
            or terminal.raw_receipt_sha256 != raw_receipt.receipt_sha256
            or terminal.content_sha256 != raw_receipt.content_sha256
            or terminal.capacity_commit_sha256 != capacity.commit_sha256
            or scheduler.state != "completed"
            or scheduler.last_transition != "completed"
            or scheduler.attempt_count != terminal.attempt_count
            or capacity.capture_sha256 != capture.capture_receipt_sha256
            or capacity.receipt_sha256 != raw_receipt.receipt_sha256
            or capacity.content_sha256 != raw_receipt.content_sha256
            or capacity.actual_bytes != len(raw_payload)
            or capacity.actual_entries != 1
            or producer.acquisition_job_id != candidate.acquisition_job_id
            or producer.content_sha256 != raw_receipt.content_sha256
            or producer.size_bytes != len(raw_payload)
            or downstream.payload != raw_payload
            or identity.identity_sha256
            != closure.lifecycle_terminal_identity_sha256
            or identity.event_id != closure.lifecycle_event_id
            or identity.event_sha256 != closure.lifecycle_event_sha256
            or identity.producer_receipt_sha256
            != closure.lifecycle_producer_receipt_sha256
            or consumption.receipt_sha256
            != closure.lifecycle_consumption_receipt_sha256
        ):
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 source authorities differ"
            )
        terminal_watermark = max(
            scheduler.terminal_at,
            terminal.created_at,
            consumption.created_at,
            float(capacity.committed_at or 0.0),
            capture.created_at,
        )
        if terminal_watermark > now - self._limits_v2.min_terminal_age_seconds:
            raise AcquisitionSourceRetentionConflict("hot_window_active")
        raw_receipt_view = {
            role: _safe_raw_receipt(receipt)
            for role, receipt in sorted(receipts.items())
        }
        raw_object_view = {
            role: {
                "content_sha256": hashlib.sha256(payload).hexdigest(),
                "size_bytes": len(payload),
            }
            for role, payload in sorted(payloads.items())
        }
        raw_projection = {
            "raw_capture": capture.to_metadata(),
            "raw_objects": raw_object_view,
            "raw_receipts": raw_receipt_view,
        }
        role_views = {
            "capacity_ledger": participant_candidate_view_sha256(
                role="capacity_ledger",
                candidate_sha256=candidate.candidate_sha256,
                projection={"capacity_commit": capacity.to_mapping()},
            ),
            "downstream_lifecycle": (
                self._evidence.source_reclamation_terminal_view_sha256(
                    candidate_sha256=candidate.candidate_sha256,
                    acquisition_job_id=candidate.acquisition_job_id,
                    stream_id=candidate.evidence_stream_id,
                    stream_sequence=candidate.evidence_stream_sequence,
                )
            ),
            "execution_receipts": participant_candidate_view_sha256(
                role="execution_receipts",
                candidate_sha256=candidate.candidate_sha256,
                projection={
                    "execution_history": [item.as_dict() for item in history]
                },
            ),
            "raw_artifacts": participant_candidate_view_sha256(
                role="raw_artifacts",
                candidate_sha256=candidate.candidate_sha256,
                projection=raw_projection,
            ),
            "scheduler": participant_candidate_view_sha256(
                role="scheduler",
                candidate_sha256=candidate.candidate_sha256,
                projection={
                    "scheduler_terminal": {
                        **scheduler.as_dict(),
                        "proof_sha256": scheduler.proof_sha256,
                    }
                },
            ),
        }
        object_references = tuple({
            "content_sha256": hashlib.sha256(payload).hexdigest(),
            "role": role,
            "size_bytes": len(payload),
        } for role, payload in sorted(payloads.items()))
        object_root = _v2_ordered_root(
            schema=V2_OBJECT_REFERENCE_ROOT_SCHEMA,
            item_sha256s=tuple(_digest(item) for item in object_references),
        )
        metadata_core = {
            "candidate": candidate.as_dict(),
            "candidate_sha256": candidate.candidate_sha256,
            "consumer_closure": closure.as_dict(),
            "object_references": list(object_references),
            "object_reference_root_sha256": object_root,
            "participant_view_sha256s": role_views,
            "schema": V2_METADATA_SCHEMA,
            "source_store_bindings": list(source_store_bindings),
            "source_store_set_root_sha256": source_store_set_root_sha256,
            "terminal_watermark": terminal_watermark,
        }
        record_sha = _digest(metadata_core)
        metadata = {**metadata_core, "record_sha256": record_sha}
        metadata_bytes = _canonical(metadata)
        if len(metadata_bytes) > self._limits_v2.max_metadata_bytes:
            raise AcquisitionSourceRetentionCapacityError(
                "v2 candidate metadata exceeds bound"
            )
        return ({
            "archive_object_count": len(object_references),
            "archive_object_reference_root_sha256": object_root,
            "archive_object_total_bytes": sum(
                item["size_bytes"] for item in object_references
            ),
            "candidate": candidate,
            "closure": closure,
            "metadata": metadata,
            "metadata_bytes": metadata_bytes,
            "metadata_sha256": hashlib.sha256(metadata_bytes).hexdigest(),
            "payload_fingerprints": tuple(
                (
                    role,
                    hashlib.sha256(payload).hexdigest(),
                    len(payload),
                )
                for role, payload in sorted(payloads.items())
            ),
            "record_sha256": record_sha,
            "role_views": role_views,
        }, tuple(sorted(payloads.items())))

    @staticmethod
    def _seal_result_from_set_v2(
        candidate_set: Mapping[str, Any],
    ) -> AcquisitionSourceArchiveSealResultV2:
        return AcquisitionSourceArchiveSealResultV2(
            status="sealed",
            archive_anchor_sha256=str(candidate_set["anchor_sha256"]),
            archive_segment_sha256=str(candidate_set["segment_sha256"]),
            candidate_authority_sha256=str(
                candidate_set["candidate_authority_sha256"]
            ),
            candidate_bindings_sha256=str(
                candidate_set["candidate_bindings_sha256"]
            ),
            archive_binding_root_sha256=str(
                candidate_set["archive_binding_root_sha256"]
            ),
            consumer_registry_root_sha256=str(
                candidate_set["consumer_registry_root_sha256"]
            ),
            consumer_epoch_root_sha256=str(
                candidate_set["consumer_epoch_root_sha256"]
            ),
            consumer_closure_root_sha256=str(
                candidate_set["consumer_closure_root_sha256"]
            ),
            source_store_set_root_sha256=str(
                candidate_set["source_store_set_root_sha256"]
            ),
            candidate_count=int(candidate_set["candidate_count"]),
            object_count=int(candidate_set["object_count"]),
            object_bytes=int(candidate_set["object_bytes"]),
        )

    def _existing_candidate_sets_v2(self) -> tuple[dict[str, Any], ...]:
        files = self._bounded_directory_v2(
            self.candidate_set_directory,
            self._limits_v2.max_archive_segments + 1,
        )
        result: list[dict[str, Any]] = []
        for path in sorted(files, key=lambda item: item.name):
            match = re.fullmatch(r"([0-9a-f]{64})\.set\.json", path.name)
            if match is None or not path.is_file():
                raise AcquisitionSourceRetentionIntegrityError(
                    "v2 candidate-set directory differs"
                )
            result.append(self._read_candidate_set_v2(match.group(1)))
        return tuple(result)

    @staticmethod
    def _binding_from_mapping_v2(
        value: object,
    ) -> SourceReclamationCandidateBindingV1:
        if not isinstance(value, dict):
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 candidate binding is not a mapping"
            )
        fields = frozenset(
            SourceReclamationCandidateBindingV1.__dataclass_fields__
        )
        if frozenset(value) != fields | {"schema"}:
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 candidate binding shape differs"
            )
        try:
            return SourceReclamationCandidateBindingV1(**{
                key: value[key] for key in fields
            })
        except (TypeError, ValueError, RuntimeError) as err:
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 candidate binding is invalid"
            ) from err

    def _validate_metadata_v2(
        self,
        *,
        binding: SourceReclamationCandidateBindingV1,
        metadata_sha256: str,
        reference: Mapping[str, Any],
        verify_objects: bool,
    ) -> dict[str, Any]:
        metadata_sha = _sha(metadata_sha256, "v2 metadata SHA-256")
        payload = self._read_exact_v2(
            self.metadata_directory / f"{metadata_sha}.metadata.json",
            maximum=self._limits_v2.max_metadata_bytes,
            expected_sha256=metadata_sha,
        )
        try:
            metadata = json.loads(payload.decode("ascii"))
        except (UnicodeDecodeError, json.JSONDecodeError) as err:
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 candidate metadata is corrupt"
            ) from err
        self._require_exact_keys_v2(
            metadata,
            frozenset({
                "candidate",
                "candidate_sha256",
                "consumer_closure",
                "object_reference_root_sha256",
                "object_references",
                "participant_view_sha256s",
                "record_sha256",
                "schema",
                "source_store_bindings",
                "source_store_set_root_sha256",
                "terminal_watermark",
            }),
            label="candidate metadata",
        )
        if payload != _canonical(metadata) or metadata["schema"] != V2_METADATA_SCHEMA:
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 candidate metadata is not canonical"
            )
        record_sha = _sha(metadata["record_sha256"], "v2 record SHA-256")
        record_core = dict(metadata)
        record_core.pop("record_sha256")
        if (
            _digest(record_core) != record_sha
            or record_sha != binding.sealed_archive_record_sha256
            or reference.get("record_sha256") != record_sha
            or reference.get("metadata_sha256") != metadata_sha
            or metadata.get("candidate_sha256") != binding.candidate_sha256
        ):
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 candidate metadata binding differs"
            )
        candidate_value = metadata["candidate"]
        if not isinstance(candidate_value, dict) or frozenset(candidate_value) != (
            frozenset(AcquisitionSourceCandidate.__dataclass_fields__)
        ):
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 archived candidate shape differs"
            )
        try:
            candidate = AcquisitionSourceCandidate(**candidate_value)
        except (TypeError, ValueError, RuntimeError) as err:
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 archived candidate is invalid"
            ) from err
        if (
            candidate.candidate_sha256 != binding.candidate_sha256
            or candidate.acquisition_job_id != binding.acquisition_job_id
            or candidate.scheduler_job_id != binding.scheduler_job_id
            or candidate.raw_capture_id != binding.raw_capture_id
            or candidate.evidence_stream_id != binding.evidence_stream_id
            or candidate.evidence_stream_sequence
            != binding.evidence_stream_sequence
        ):
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 archived candidate identity differs"
            )
        closure_value = metadata["consumer_closure"]
        closure_fields = frozenset(
            AcquisitionConsumerClosureReceipt.__dataclass_fields__
        )
        if (
            not isinstance(closure_value, dict)
            or frozenset(closure_value)
            != closure_fields | {"lifecycle_terminal_kind", "schema"}
        ):
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 archived consumer closure shape differs"
            )
        try:
            closure = AcquisitionConsumerClosureReceipt(**{
                key: closure_value[key] for key in closure_fields
            })
        except (TypeError, ValueError, RuntimeError) as err:
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 archived consumer closure is invalid"
            ) from err
        if (
            closure.receipt_sha256
            != binding.consumer_closure_receipt_sha256
            or closure.candidate_sha256 != binding.candidate_sha256
            or closure.acquisition_job_id != binding.acquisition_job_id
            or closure.stream_id != binding.evidence_stream_id
            or closure.stream_sequence != binding.evidence_stream_sequence
            or closure.lifecycle_storage_status != "archived"
        ):
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 archived consumer closure binding differs"
            )
        bindings_value = metadata["source_store_bindings"]
        if (
            not isinstance(bindings_value, list)
            or len(bindings_value) != len(PARTICIPANT_APPLY_ORDER)
        ):
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 source-store binding inventory differs"
            )
        source_bindings: list[dict[str, str]] = []
        for ordinal, item in enumerate(bindings_value):
            self._require_exact_keys_v2(
                item,
                frozenset({"role", "source_store_identity_sha256"}),
                label="source-store binding",
            )
            if item["role"] != PARTICIPANT_APPLY_ORDER[ordinal]:
                raise AcquisitionSourceRetentionIntegrityError(
                    "v2 source-store binding order differs"
                )
            source_bindings.append({
                "role": str(item["role"]),
                "source_store_identity_sha256": _sha(
                    item["source_store_identity_sha256"],
                    "v2 source-store identity SHA-256",
                ),
            })
        source_root = self._source_store_set_root_v2(tuple(source_bindings))
        if metadata["source_store_set_root_sha256"] != source_root:
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 source-store root differs"
            )
        participant_views = metadata["participant_view_sha256s"]
        if (
            not isinstance(participant_views, dict)
            or tuple(sorted(participant_views))
            != tuple(sorted(PARTICIPANT_APPLY_ORDER))
        ):
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 participant view inventory differs"
            )
        for role in PARTICIPANT_APPLY_ORDER:
            view_sha = _sha(
                participant_views[role],
                f"v2 {role} participant view SHA-256",
            )
            if reference.get(f"{role}_view_sha256") != view_sha:
                raise AcquisitionSourceRetentionIntegrityError(
                    "v2 participant metadata reference differs"
                )
        object_values = metadata["object_references"]
        if (
            not isinstance(object_values, list)
            or not 1 <= len(object_values) <= 64
        ):
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 raw object reference inventory differs"
            )
        object_references: list[dict[str, Any]] = []
        for item in object_values:
            self._require_exact_keys_v2(
                item,
                frozenset({"content_sha256", "role", "size_bytes"}),
                label="raw object reference",
            )
            role = _identifier(item["role"], "v2 raw object role")
            content_sha = _sha(
                item["content_sha256"], "v2 raw object SHA-256"
            )
            size = self._require_count_v2(
                item["size_bytes"],
                label="raw object bytes",
                maximum=self._limits_v2.max_object_bytes,
            )
            object_references.append({
                "content_sha256": content_sha,
                "role": role,
                "size_bytes": size,
            })
            path = self.object_directory / f"{content_sha}.raw"
            if verify_objects:
                self._verify_exact_v2(
                    path,
                    maximum=max(size, 1),
                    expected_sha256=content_sha,
                    expected_bytes=size,
                )
            else:
                witness = _v2_file_witness(path)
                if witness[2] != size:
                    raise AcquisitionSourceRetentionIntegrityError(
                        "v2 raw object size differs"
                    )
        if (
            tuple(item["role"] for item in object_references)
            != tuple(sorted(item["role"] for item in object_references))
            or len({item["role"] for item in object_references})
            != len(object_references)
        ):
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 raw object reference order differs"
            )
        object_root = _v2_ordered_root(
            schema=V2_OBJECT_REFERENCE_ROOT_SCHEMA,
            item_sha256s=tuple(_digest(item) for item in object_references),
        )
        object_bytes = sum(item["size_bytes"] for item in object_references)
        if (
            metadata["object_reference_root_sha256"] != object_root
            or reference.get("archive_object_reference_root_sha256")
            != object_root
            or reference.get("archive_object_count") != len(object_references)
            or reference.get("archive_object_total_bytes") != object_bytes
        ):
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 raw object reference root differs"
            )
        _finite(
            metadata["terminal_watermark"],
            "v2 terminal watermark",
            minimum=0.0,
            maximum=32_503_680_000.0,
        )
        return {
            "consumer_closure": closure,
            "metadata_bytes": len(payload),
            "object_references": tuple(object_references),
            "record_sha256": record_sha,
            "registry_epoch_root_sha256": (
                closure.registry_epoch_root_sha256
            ),
            "registry_root_sha256": closure.registry_root_sha256,
            "source_store_set_root_sha256": source_root,
        }

    def _validate_candidate_set_locked_v2(
        self,
        candidate_set: Mapping[str, Any],
        *,
        verify_objects: bool,
    ) -> dict[str, Any]:
        self._verify_registry_store_v2()
        expected_authorities = {
            "consumer_registry_store_identity_sha256": (
                self._consumer_registry_binding_v2[
                    "consumer_registry_store_identity_sha256"
                ]
            ),
            **self._closure_source_archive_binding_v2,
        }
        if any(
            candidate_set.get(key) != value
            for key, value in expected_authorities.items()
        ):
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 candidate-set closure authorities differ"
            )
        self._require_count_v2(
            candidate_set.get("consumer_registry_epoch"),
            label="candidate-set consumer registry epoch",
            maximum=2**63 - 1,
        )
        for key, label in (
            ("closure_source_archive_anchor_sha256", "closure source anchor"),
            ("consumer_epoch_root_sha256", "consumer epoch root"),
            (
                "consumer_registry_epoch_receipt_sha256",
                "consumer epoch receipt",
            ),
            ("consumer_registry_root_sha256", "consumer registry root"),
        ):
            _sha(candidate_set.get(key), f"v2 candidate-set {label} SHA-256")
        superseded_root = candidate_set.get(
            "superseded_candidate_bindings_sha256"
        )
        superseded_epoch = candidate_set.get(
            "superseded_consumer_registry_epoch"
        )
        if (superseded_root is None) != (superseded_epoch is None):
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 candidate-set supersession binding is partial"
            )
        if superseded_root is not None:
            _sha(
                superseded_root,
                "v2 superseded candidate bindings SHA-256",
            )
            prior_epoch = self._require_count_v2(
                superseded_epoch,
                label="superseded consumer registry epoch",
                maximum=2**63 - 1,
            )
            if prior_epoch >= int(candidate_set["consumer_registry_epoch"]):
                raise AcquisitionSourceRetentionIntegrityError(
                    "v2 candidate-set supersession epoch is not older"
                )
        candidate_count = self._require_count_v2(
            candidate_set.get("candidate_count"),
            label="candidate-set count",
            maximum=self._limits_v2.max_candidates,
        )
        entries_value = candidate_set.get("entries")
        if (
            candidate_count < 1
            or not isinstance(entries_value, list)
            or len(entries_value) != candidate_count
        ):
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 candidate-set entry inventory differs"
            )
        bindings: list[SourceReclamationCandidateBindingV1] = []
        reference_payloads: list[bytes] = []
        metadata_sha256s: set[str] = set()
        reference_sha256s: set[str] = set()
        metadata_bytes = 0
        object_references: dict[str, int] = {}
        logical_object_count = 0
        logical_object_bytes = 0
        closures: list[AcquisitionConsumerClosureReceipt] = []
        object_references_by_candidate: list[tuple[dict[str, Any], ...]] = []
        source_store_roots: set[str] = set()
        for item in entries_value:
            self._require_exact_keys_v2(
                item,
                frozenset({
                    "binding", "candidate_sha256", "metadata_sha256",
                    "reference_sha256",
                }),
                label="candidate-set entry",
            )
            binding = self._binding_from_mapping_v2(item["binding"])
            reference_sha = _sha(
                item["reference_sha256"], "v2 reference SHA-256"
            )
            reference_sha256s.add(reference_sha)
            metadata_sha256s.add(_sha(
                item["metadata_sha256"], "v2 metadata SHA-256"
            ))
            reference_payload = self._read_exact_v2(
                self.reference_directory / f"{reference_sha}.reference.json",
                maximum=V2_MAX_REFERENCE_BYTES,
                expected_sha256=reference_sha,
            )
            try:
                reference = json.loads(reference_payload.decode("ascii"))
            except (UnicodeDecodeError, json.JSONDecodeError) as err:
                raise AcquisitionSourceRetentionIntegrityError(
                    "v2 candidate reference is corrupt"
                ) from err
            if (
                reference_payload != reclamation_canonical_bytes(reference)
                or item["candidate_sha256"] != binding.candidate_sha256
                or reference.get("candidate_sha256")
                != binding.candidate_sha256
            ):
                raise AcquisitionSourceRetentionIntegrityError(
                    "v2 candidate reference binding differs"
                )
            metadata_result = self._validate_metadata_v2(
                binding=binding,
                metadata_sha256=item["metadata_sha256"],
                reference=reference,
                verify_objects=verify_objects,
            )
            bindings.append(binding)
            reference_payloads.append(reference_payload)
            closures.append(metadata_result["consumer_closure"])
            object_references_by_candidate.append(
                metadata_result["object_references"]
            )
            source_store_roots.add(
                metadata_result["source_store_set_root_sha256"]
            )
            metadata_bytes += int(metadata_result["metadata_bytes"])
            for object_reference in metadata_result["object_references"]:
                content_sha = str(object_reference["content_sha256"])
                size = int(object_reference["size_bytes"])
                prior = object_references.setdefault(content_sha, size)
                if prior != size:
                    raise AcquisitionSourceRetentionIntegrityError(
                        "v2 raw object identity was reused with another size"
                    )
                logical_object_count += 1
                logical_object_bytes += size
        binding_tuple = tuple(bindings)
        reference_tuple = tuple(reference_payloads)
        if (
            tuple(item.candidate_sha256 for item in binding_tuple)
            != tuple(sorted(item.candidate_sha256 for item in binding_tuple))
            or len({item.candidate_sha256 for item in binding_tuple})
            != candidate_count
            or reclamation_candidate_binding_root(binding_tuple)
            != candidate_set.get("candidate_bindings_sha256")
            or reclamation_archive_binding_root(binding_tuple, reference_tuple)
            != candidate_set.get("archive_binding_root_sha256")
            or any(
                item.consumer_closure_root_sha256
                != candidate_set.get("consumer_closure_root_sha256")
                for item in binding_tuple
            )
            or any(
                item.sealed_archive_segment_sha256
                != candidate_set.get("segment_sha256")
                for item in binding_tuple
            )
            or self._consumer_closure_root_v2(tuple(closures))
            != candidate_set.get("consumer_closure_root_sha256")
            or any(
                item.registry_root_sha256
                != candidate_set.get("consumer_registry_root_sha256")
                for item in closures
            )
            or any(
                item.registry_epoch_root_sha256
                != candidate_set.get("consumer_epoch_root_sha256")
                for item in closures
            )
            or any(
                item.registry_epoch_receipt_sha256
                != candidate_set.get("consumer_registry_epoch_receipt_sha256")
                or item.registry_store_identity_sha256
                != candidate_set.get("consumer_registry_store_identity_sha256")
                or item.registry_epoch
                != candidate_set.get("consumer_registry_epoch")
                or item.source_archive_anchor_sha256
                != candidate_set.get("closure_source_archive_anchor_sha256")
                for item in closures
            )
            or source_store_roots
            != {candidate_set.get("source_store_set_root_sha256")}
            or candidate_set.get("candidate_authority_sha256")
            != self.authority_sha256
            or int(candidate_set.get("object_count", -1))
            != logical_object_count
            or int(candidate_set.get("object_bytes", -1))
            != logical_object_bytes
        ):
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 candidate-set root or accounting differs"
            )
        self._verify_registry_store_v2()
        return {
            "bindings": binding_tuple,
            "candidates": tuple(
                AcquisitionSourceCandidate(
                    acquisition_job_id=item.acquisition_job_id,
                    scheduler_job_id=item.scheduler_job_id,
                    raw_capture_id=item.raw_capture_id,
                    evidence_stream_id=item.evidence_stream_id,
                    evidence_stream_sequence=item.evidence_stream_sequence,
                )
                for item in binding_tuple
            ),
            "consumer_closures": tuple(closures),
            "metadata_sha256s": frozenset(metadata_sha256s),
            "metadata_bytes": metadata_bytes,
            "object_references": object_references,
            "object_references_by_candidate": tuple(
                object_references_by_candidate
            ),
            "record_reference_sha256s": tuple(
                hashlib.sha256(item).hexdigest() for item in reference_tuple
            ),
            "reference_payloads": reference_tuple,
            "reference_bytes": sum(len(item) for item in reference_tuple),
            "reference_sha256s": frozenset(reference_sha256s),
            "source_store_set_root_sha256": next(
                iter(source_store_roots)
            ),
        }

    def _assert_anchor_ancestor_locked_v2(
        self,
        ancestor_anchor_sha256: str,
        descendant_anchor_sha256: str,
    ) -> AcquisitionSourceArchiveAncestorProofV2:
        """Prove one exact anchor is on the immutable chain to another."""

        ancestor_sha = _sha(
            ancestor_anchor_sha256,
            "v2 ancestor anchor SHA-256",
        )
        descendant_sha = _sha(
            descendant_anchor_sha256,
            "v2 descendant anchor SHA-256",
        )
        descendant = self._read_anchor_v2(descendant_sha)
        descendant_generation = self._require_count_v2(
            descendant.get("generation"),
            label="descendant anchor generation",
            maximum=self._limits_v2.max_archive_segments,
        )
        reverse_chain: list[str] = []
        current_sha = descendant_sha
        expected_generation = descendant_generation
        ancestor_generation: int | None = None
        while True:
            if len(reverse_chain) > self._limits_v2.max_archive_segments:
                raise AcquisitionSourceRetentionCapacityError(
                    "v2 anchor chain exceeds its bound"
                )
            anchor = self._read_anchor_v2(current_sha)
            generation = self._require_count_v2(
                anchor.get("generation"),
                label="anchor generation",
                maximum=self._limits_v2.max_archive_segments,
            )
            if generation != expected_generation:
                raise AcquisitionSourceRetentionIntegrityError(
                    "v2 anchor chain generation differs"
                )
            reverse_chain.append(current_sha)
            if current_sha == ancestor_sha:
                ancestor_generation = generation
                break
            previous = anchor.get("previous_anchor_sha256")
            if generation == 0 or previous is None:
                raise AcquisitionSourceRetentionConflict(
                    "v2 requested anchor is not an ancestor"
                )
            current_sha = _sha(
                previous,
                "v2 previous anchor SHA-256",
            )
            expected_generation -= 1
        chain = tuple(reversed(reverse_chain))
        if ancestor_generation is None:  # pragma: no cover - loop invariant
            raise AssertionError("ancestor generation is unavailable")
        core = {
            "ancestor_anchor_sha256": ancestor_sha,
            "ancestor_generation": ancestor_generation,
            "anchor_chain": list(chain),
            "archive_authority_sha256": self.authority_sha256,
            "descendant_anchor_sha256": descendant_sha,
            "descendant_generation": descendant_generation,
            "schema": "stv3-worker-source-retention-ancestor-proof-v2",
        }
        return AcquisitionSourceArchiveAncestorProofV2(
            proof_sha256=_digest(core),
            archive_authority_sha256=self.authority_sha256,
            ancestor_anchor_sha256=ancestor_sha,
            descendant_anchor_sha256=descendant_sha,
            ancestor_generation=ancestor_generation,
            descendant_generation=descendant_generation,
            anchor_chain=chain,
        )

    def _audit_locked_v2(self, *, verify_objects: bool) -> dict[str, Any]:
        """Recompute the complete v2 archive prefix and exact file inventory."""

        if self.pending_path.exists() or self.pending_path.is_symlink():
            raise AcquisitionSourceRetentionDurabilityError(
                "v2 pending publication requires recovery"
            )
        expected_root_names = {
            "HEAD.json",
            "anchors",
            "authority.json",
            "candidate_sets",
            "manifests",
            "metadata",
            "objects",
            "references",
            "retention-v2.lock",
            "segments",
            "staging",
        }
        root_items = tuple(self.root.iterdir())
        if (
            {item.name for item in root_items} != expected_root_names
            or any(item.is_symlink() for item in root_items)
            or any(
                not item.is_dir()
                for item in root_items
                if item.name in {
                    "anchors", "candidate_sets", "manifests", "metadata",
                    "objects", "references", "segments", "staging",
                }
            )
            or any(
                not item.is_file()
                for item in root_items
                if item.name in {
                    "HEAD.json", "authority.json", "retention-v2.lock",
                }
            )
        ):
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 archive root inventory differs"
            )
        if self._bounded_directory_v2(
            self.staging_directory,
            self._limits_v2.max_staging_files + 1,
        ):
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 staging directory is not empty"
            )
        head = self._read_head_v2()
        head_generation = self._require_count_v2(
            head.get("generation"),
            label="head generation",
            maximum=self._limits_v2.max_archive_segments,
        )
        reverse_anchors: list[dict[str, Any]] = []
        current_sha = str(head["anchor_sha256"])
        expected_generation = head_generation
        seen_anchors: set[str] = set()
        while True:
            if current_sha in seen_anchors:
                raise AcquisitionSourceRetentionIntegrityError(
                    "v2 anchor chain contains a cycle"
                )
            seen_anchors.add(current_sha)
            anchor = self._read_anchor_v2(current_sha)
            generation = self._require_count_v2(
                anchor.get("generation"),
                label="anchor generation",
                maximum=self._limits_v2.max_archive_segments,
            )
            if generation != expected_generation:
                raise AcquisitionSourceRetentionIntegrityError(
                    "v2 anchor chain generation differs"
                )
            reverse_anchors.append(anchor)
            if generation == 0:
                if anchor.get("previous_anchor_sha256") is not None:
                    raise AcquisitionSourceRetentionIntegrityError(
                        "v2 genesis anchor has a predecessor"
                    )
                break
            current_sha = _sha(
                anchor.get("previous_anchor_sha256"),
                "v2 previous anchor SHA-256",
            )
            expected_generation -= 1
        anchors = tuple(reversed(reverse_anchors))
        if len(anchors) != head_generation + 1:
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 anchor chain length differs"
            )
        candidate_sets = self._existing_candidate_sets_v2()
        if len(candidate_sets) != head_generation:
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 candidate-set generation inventory differs"
            )
        sets_by_anchor: dict[str, dict[str, Any]] = {}
        for candidate_set in candidate_sets:
            anchor_sha = _sha(
                candidate_set.get("anchor_sha256"),
                "v2 candidate-set anchor SHA-256",
            )
            if anchor_sha in sets_by_anchor:
                raise AcquisitionSourceRetentionIntegrityError(
                    "v2 candidate sets repeat an anchor"
                )
            sets_by_anchor[anchor_sha] = candidate_set

        expected_anchor_names: set[str] = set()
        expected_manifest_names: set[str] = set()
        expected_segment_names: set[str] = set()
        expected_candidate_names: set[str] = set()
        expected_metadata_names: set[str] = set()
        expected_reference_names: set[str] = set()
        expected_object_names: set[str] = set()
        previous_anchor: dict[str, Any] | None = None
        previous_manifest: dict[str, Any] | None = None
        previous_segment_sha: str | None = None
        cumulative_file_bytes = 0
        cumulative_object_bytes = 0
        cumulative_object_count = 0
        cumulative_record_count = 0
        closure_groups: list[
            tuple[
                str,
                tuple[
                    tuple[
                        AcquisitionSourceCandidate,
                        AcquisitionConsumerClosureReceipt,
                    ],
                    ...,
                ],
            ]
        ] = []
        source_anchor_supersession_edges: list[tuple[str, str]] = []
        candidate_inventory_by_sha: dict[str, tuple[str, ...]] = {}
        latest_candidate_set_by_inventory: dict[
            tuple[str, ...], dict[str, Any]
        ] = {}
        source_store_root: str | None = None
        for generation, anchor in enumerate(anchors):
            anchor_sha = str(anchor["anchor_sha256"])
            expected_anchor_names.add(f"{anchor_sha}.anchor.json")
            manifest_sha = _sha(
                anchor.get("manifest_sha256"),
                "v2 anchor manifest SHA-256",
            )
            manifest = self._read_manifest_v2(manifest_sha)
            expected_manifest_names.add(f"{manifest_sha}.manifest.json")
            for value, label in (
                (anchor.get("cumulative_file_bytes"), "anchor file bytes"),
                (anchor.get("cumulative_object_bytes"), "anchor object bytes"),
                (anchor.get("cumulative_object_count"), "anchor object count"),
                (anchor.get("cumulative_record_count"), "anchor record count"),
                (manifest.get("cumulative_file_bytes"), "manifest file bytes"),
                (manifest.get("cumulative_object_bytes"), "manifest object bytes"),
                (manifest.get("cumulative_object_count"), "manifest object count"),
                (manifest.get("cumulative_record_count"), "manifest record count"),
            ):
                self._require_count_v2(
                    value,
                    label=label,
                    maximum=2**53,
                )
            _finite(
                anchor.get("sealed_at"),
                "v2 anchor clock",
                minimum=0.0,
                maximum=32_503_680_000.0,
            )
            if (
                int(anchor["generation"]) != generation
                or int(manifest["generation"]) != generation
                or manifest.get("archive_authority_sha256")
                != self.authority_sha256
                or anchor.get("prefix_root_sha256")
                != manifest.get("prefix_root_sha256")
                or anchor.get("source_store_set_root_sha256")
                != manifest.get("source_store_set_root_sha256")
                or anchor.get("cumulative_file_bytes")
                != manifest.get("cumulative_file_bytes")
                or anchor.get("cumulative_object_bytes")
                != manifest.get("cumulative_object_bytes")
                or anchor.get("cumulative_object_count")
                != manifest.get("cumulative_object_count")
                or anchor.get("cumulative_record_count")
                != manifest.get("cumulative_record_count")
            ):
                raise AcquisitionSourceRetentionIntegrityError(
                    "v2 anchor and manifest binding differs"
                )
            if generation == 0:
                genesis_prefix = _digest({
                    "archive_authority_sha256": self.authority_sha256,
                    "schema": V2_PREFIX_SCHEMA,
                    "start": True,
                })
                if (
                    anchor.get("head_segment_sha256") is not None
                    or anchor.get("previous_anchor_sha256") is not None
                    or anchor.get("source_store_set_root_sha256") is not None
                    or float(anchor["sealed_at"]) != 0.0
                    or manifest.get("previous_manifest_sha256") is not None
                    or manifest.get("segment_file_sha256") is not None
                    or manifest.get("segment_sha256") is not None
                    or manifest.get("source_store_set_root_sha256") is not None
                    or anchor.get("prefix_root_sha256") != genesis_prefix
                    or any(int(anchor[key]) != 0 for key in (
                        "cumulative_file_bytes", "cumulative_object_bytes",
                        "cumulative_object_count", "cumulative_record_count",
                    ))
                ):
                    raise AcquisitionSourceRetentionIntegrityError(
                        "v2 genesis archive authority differs"
                    )
                previous_anchor = anchor
                previous_manifest = manifest
                continue
            if previous_anchor is None or previous_manifest is None:
                raise AssertionError("v2 previous archive authority is unavailable")
            candidate_set = sets_by_anchor.get(anchor_sha)
            if candidate_set is None:
                raise AcquisitionSourceRetentionIntegrityError(
                    "v2 anchor lacks its candidate set"
                )
            validation = self._validate_candidate_set_locked_v2(
                candidate_set,
                verify_objects=verify_objects,
            )
            closure_groups.append((
                str(candidate_set["closure_source_archive_anchor_sha256"]),
                tuple(zip(
                    validation["candidates"],
                    validation["consumer_closures"],
                    strict=True,
                )),
            ))
            candidate_inventory = tuple(
                item.candidate_sha256 for item in validation["bindings"]
            )
            for candidate_sha in candidate_inventory:
                prior_inventory = candidate_inventory_by_sha.setdefault(
                    candidate_sha,
                    candidate_inventory,
                )
                if prior_inventory != candidate_inventory:
                    raise AcquisitionSourceRetentionIntegrityError(
                        "v2 successor candidate inventory partially overlaps"
                    )
            prior_set = latest_candidate_set_by_inventory.get(
                candidate_inventory
            )
            if prior_set is None:
                if (
                    candidate_set.get(
                        "superseded_candidate_bindings_sha256"
                    ) is not None
                    or candidate_set.get(
                        "superseded_consumer_registry_epoch"
                    ) is not None
                ):
                    raise AcquisitionSourceRetentionIntegrityError(
                        "v2 first candidate set claims a predecessor"
                    )
            else:
                if (
                    candidate_set.get(
                        "superseded_candidate_bindings_sha256"
                    ) != prior_set.get("candidate_bindings_sha256")
                    or candidate_set.get(
                        "superseded_consumer_registry_epoch"
                    ) != prior_set.get("consumer_registry_epoch")
                    or int(candidate_set["consumer_registry_epoch"])
                    <= int(prior_set["consumer_registry_epoch"])
                ):
                    raise AcquisitionSourceRetentionIntegrityError(
                        "v2 successor candidate-set chain differs"
                    )
                prior_source_anchor = str(
                    prior_set["closure_source_archive_anchor_sha256"]
                )
                candidate_source_anchor = str(
                    candidate_set["closure_source_archive_anchor_sha256"]
                )
                if candidate_source_anchor != prior_source_anchor:
                    source_anchor_supersession_edges.append((
                        prior_source_anchor,
                        candidate_source_anchor,
                    ))
            latest_candidate_set_by_inventory[candidate_inventory] = dict(
                candidate_set
            )
            segment_sha = _sha(
                anchor.get("head_segment_sha256"),
                "v2 anchor segment SHA-256",
            )
            segment, segment_payload = self._read_segment_v2(
                segment_sha,
                expected_file_sha256=_sha(
                    manifest.get("segment_file_sha256"),
                    "v2 manifest segment-file SHA-256",
                ),
            )
            segment_file_sha = hashlib.sha256(segment_payload).hexdigest()
            expected_segment_names.add(f"{segment_sha}.segment.json")
            candidate_root = str(candidate_set["candidate_bindings_sha256"])
            expected_candidate_names.add(f"{candidate_root}.set.json")
            entries = candidate_set["entries"]
            expected_metadata_names.update(
                f"{item['metadata_sha256']}.metadata.json" for item in entries
            )
            expected_reference_names.update(
                f"{item['reference_sha256']}.reference.json" for item in entries
            )
            expected_object_names.update(
                f"{item}.raw" for item in validation["object_references"]
            )
            segment_identity = _digest({
                "archive_authority_sha256": self.authority_sha256,
                "batch_record_root_sha256": segment["batch_record_root_sha256"],
                "generation": generation,
                "previous_segment_sha256": previous_segment_sha,
                "schema": "stv3-worker-source-retention-segment-identity-v2",
                "source_store_set_root_sha256": segment[
                    "source_store_set_root_sha256"
                ],
            })
            prefix_root = _digest({
                "generation": generation,
                "previous_prefix_root_sha256": previous_anchor[
                    "prefix_root_sha256"
                ],
                "schema": V2_PREFIX_SCHEMA,
                "segment_file_sha256": segment_file_sha,
                "segment_sha256": segment_sha,
            })
            batch_root = _v2_ordered_root(
                schema="stv3-worker-source-retention-record-batch-root-v2",
                item_sha256s=tuple(
                    item.sealed_archive_record_sha256
                    for item in validation["bindings"]
                ),
            )
            batch_object_bytes = sum(
                int(item) for item in validation["object_references"].values()
            )
            cumulative_file_bytes += (
                batch_object_bytes
                + int(validation["metadata_bytes"])
                + int(validation["reference_bytes"])
                + len(segment_payload)
            )
            cumulative_object_bytes += batch_object_bytes
            cumulative_object_count += len(validation["object_references"])
            cumulative_record_count += len(validation["bindings"])
            current_source_root = _sha(
                candidate_set.get("source_store_set_root_sha256"),
                "v2 candidate source-store root SHA-256",
            )
            if source_store_root is None:
                source_store_root = current_source_root
            if (
                current_source_root != source_store_root
                or candidate_set.get("anchor_sha256") != anchor_sha
                or candidate_set.get("segment_sha256") != segment_sha
                or candidate_set.get("archive_binding_root_sha256")
                != segment.get("archive_binding_root_sha256")
                or candidate_set.get("candidate_bindings_sha256")
                != segment.get("candidate_bindings_sha256")
                or segment.get("archive_authority_sha256")
                != self.authority_sha256
                or int(segment.get("generation", -1)) != generation
                or segment.get("previous_segment_sha256") != previous_segment_sha
                or segment.get("segment_sha256") != segment_identity
                or segment.get("batch_record_root_sha256") != batch_root
                or segment.get("record_reference_sha256s")
                != list(validation["record_reference_sha256s"])
                or segment.get("source_store_set_root_sha256")
                != source_store_root
                or float(segment.get("sealed_at", -1.0))
                != float(anchor["sealed_at"])
                or manifest.get("previous_manifest_sha256")
                != previous_manifest["manifest_sha256"]
                or manifest.get("segment_sha256") != segment_sha
                or manifest.get("segment_file_sha256") != segment_file_sha
                or manifest.get("prefix_root_sha256") != prefix_root
                or manifest.get("source_store_set_root_sha256")
                != source_store_root
                or anchor.get("previous_anchor_sha256")
                != previous_anchor["anchor_sha256"]
                or anchor.get("prefix_root_sha256") != prefix_root
                or anchor.get("source_store_set_root_sha256")
                != source_store_root
                or int(manifest["cumulative_file_bytes"])
                != cumulative_file_bytes
                or int(manifest["cumulative_object_bytes"])
                != cumulative_object_bytes
                or int(manifest["cumulative_object_count"])
                != cumulative_object_count
                or int(manifest["cumulative_record_count"])
                != cumulative_record_count
            ):
                raise AcquisitionSourceRetentionIntegrityError(
                    "v2 archive generation binding differs"
                )
            previous_anchor = anchor
            previous_manifest = manifest
            previous_segment_sha = segment_sha

        last_anchor = anchors[-1]
        if (
            head.get("anchor_sha256") != last_anchor["anchor_sha256"]
            or head.get("prefix_root_sha256") != last_anchor["prefix_root_sha256"]
            or head.get("source_store_set_root_sha256")
            != last_anchor["source_store_set_root_sha256"]
            or float(head["last_sealed_at"]) != float(last_anchor["sealed_at"])
        ):
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 head differs from its anchor"
            )

        inventories = (
            (self.anchor_directory, expected_anchor_names, head_generation + 1),
            (self.manifest_directory, expected_manifest_names, head_generation + 1),
            (self.segment_directory, expected_segment_names, head_generation),
            (self.candidate_set_directory, expected_candidate_names, head_generation),
            (
                self.metadata_directory,
                expected_metadata_names,
                self._limits_v2.max_archived_jobs,
            ),
            (
                self.reference_directory,
                expected_reference_names,
                self._limits_v2.max_archived_jobs,
            ),
            (
                self.object_directory,
                expected_object_names,
                self._limits_v2.max_archived_jobs * 64,
            ),
        )
        physical_bytes = 0
        for directory, expected_names, maximum in inventories:
            items = self._bounded_directory_v2(directory, maximum + 1)
            if (
                any(not item.is_file() for item in items)
                or {item.name for item in items} != expected_names
            ):
                raise AcquisitionSourceRetentionIntegrityError(
                    "v2 archive file inventory differs"
                )
            physical_bytes += sum(item.stat().st_size for item in items)
        if (
            cumulative_record_count > self._limits_v2.max_archived_jobs
            or physical_bytes > self._limits_v2.max_total_archive_bytes
        ):
            raise AcquisitionSourceRetentionCapacityError(
                "v2 archive exceeds its durable capacity"
            )
        if closure_groups:
            with self._hold_reproved_consumer_closure_groups_v2(
                tuple(closure_groups),
                registry_verifier=None,
                require_current_source_anchor=False,
                require_live_lifecycle=False,
                source_anchor_supersession_edges=tuple(
                    source_anchor_supersession_edges
                ),
            ):
                pass
        else:
            self._verify_live_closure_authorities_v2()
        return {
            "archive_anchor_sha256": str(head["anchor_sha256"]),
            "archive_authority_sha256": self.authority_sha256,
            "archive_generation": head_generation,
            "archived_candidate_count": cumulative_record_count,
            "healthy": True,
            "object_count": len(expected_object_names),
            "physical_archive_bytes": physical_bytes,
            "source_store_set_root_sha256": source_store_root,
        }

    def audit_v2(self, *, verify_objects: bool = True) -> dict[str, Any]:
        if not isinstance(verify_objects, bool):
            raise TypeError("verify_objects must be bool")
        with self._local_lock_v2, self._exclusive_v2():
            self._recover_pending_v2()
            result = self._audit_locked_v2(verify_objects=verify_objects)
            self._reproof_source_store_root_v2(
                result["source_store_set_root_sha256"]
            )
            return result

    def prove_anchor_ancestor_v2(
        self,
        ancestor_anchor_sha256: str,
        *,
        descendant_anchor_sha256: str | None = None,
    ) -> AcquisitionSourceArchiveAncestorProofV2:
        with self._local_lock_v2, self._exclusive_v2():
            self._recover_pending_v2()
            head = self._read_head_v2()
            descendant = (
                str(head["anchor_sha256"])
                if descendant_anchor_sha256 is None
                else descendant_anchor_sha256
            )
            return self._assert_anchor_ancestor_locked_v2(
                ancestor_anchor_sha256,
                descendant,
            )

    def _held_candidate_snapshot_v2(
        self,
        request: SourceReclamationRequestV2,
    ) -> tuple[
        dict[str, Any],
        dict[str, Any],
        dict[str, Any],
        AcquisitionSourceArchiveAncestorProofV2,
    ]:
        head = self._read_head_v2()
        candidate_set = self._read_candidate_set_v2(
            request.candidate_bindings_sha256
        )
        validation = self._validate_candidate_set_locked_v2(
            candidate_set,
            verify_objects=False,
        )
        self._reproof_source_store_root_v2(
            validation["source_store_set_root_sha256"]
        )
        if (
            candidate_set.get("candidate_authority_sha256")
            != self.authority_sha256
            or candidate_set.get("anchor_sha256")
            != request.sealed_archive_anchor_sha256
            or candidate_set.get("archive_binding_root_sha256")
            != request.archive_binding_root_sha256
            or candidate_set.get("candidate_bindings_sha256")
            != request.candidate_bindings_sha256
            or candidate_set.get("consumer_registry_root_sha256")
            != request.consumer_registry_root_sha256
            or candidate_set.get("consumer_epoch_root_sha256")
            != request.consumer_epoch_root_sha256
            or int(candidate_set.get("candidate_count", -1))
            != request.candidate_count
        ):
            raise AcquisitionSourceRetentionConflict(
                "v2 request differs from its sealed candidate set"
            )
        ancestor = self._assert_anchor_ancestor_locked_v2(
            request.sealed_archive_anchor_sha256,
            str(head["anchor_sha256"]),
        )
        anchor = self._read_anchor_v2(request.sealed_archive_anchor_sha256)
        if anchor.get("head_segment_sha256") != candidate_set.get(
            "segment_sha256"
        ):
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 candidate set differs from its anchor"
            )
        manifest = self._read_manifest_v2(str(anchor["manifest_sha256"]))
        segment, segment_payload = self._read_segment_v2(
            str(candidate_set["segment_sha256"]),
            expected_file_sha256=_sha(
                manifest.get("segment_file_sha256"),
                "v2 held segment-file SHA-256",
            ),
        )
        if (
            manifest.get("segment_sha256") != candidate_set.get("segment_sha256")
            or segment.get("candidate_bindings_sha256")
            != request.candidate_bindings_sha256
            or segment.get("archive_binding_root_sha256")
            != request.archive_binding_root_sha256
            or segment.get("record_reference_sha256s")
            != list(validation["record_reference_sha256s"])
            or hashlib.sha256(segment_payload).hexdigest()
            != manifest.get("segment_file_sha256")
        ):
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 held segment binding differs"
            )
        return candidate_set, validation, head, ancestor

    def _held_page_object_inventory_v2(
        self,
        validation: Mapping[str, Any],
        *,
        after_cursor: int,
        max_candidates: int,
    ) -> tuple[int, tuple[tuple[str, int], ...]]:
        """Bound a page by both metadata records and unique provider bytes."""

        by_candidate = validation.get("object_references_by_candidate")
        bindings = validation.get("bindings")
        if (
            not isinstance(by_candidate, tuple)
            or not isinstance(bindings, tuple)
            or len(by_candidate) != len(bindings)
        ):
            raise AcquisitionSourceRetentionIntegrityError(
                "v2 held object inventory differs"
            )
        requested_upper = min(
            len(bindings),
            after_cursor + max_candidates,
        )
        selected: dict[str, int] = {}
        selected_bytes = 0
        upper = after_cursor
        for ordinal in range(after_cursor, requested_upper):
            candidate_refs = by_candidate[ordinal]
            if not isinstance(candidate_refs, tuple):
                raise AcquisitionSourceRetentionIntegrityError(
                    "v2 held candidate object inventory differs"
                )
            next_items: list[tuple[str, int]] = []
            for item in candidate_refs:
                if not isinstance(item, dict):
                    raise AcquisitionSourceRetentionIntegrityError(
                        "v2 held object reference differs"
                    )
                content_sha = _sha(
                    item.get("content_sha256"),
                    "v2 held raw object SHA-256",
                )
                size = self._require_count_v2(
                    item.get("size_bytes"),
                    label="held raw object bytes",
                    maximum=self._limits_v2.max_object_bytes,
                )
                prior = selected.get(content_sha)
                if prior is not None and prior != size:
                    raise AcquisitionSourceRetentionIntegrityError(
                        "v2 held raw object identity differs"
                    )
                if prior is None:
                    next_items.append((content_sha, size))
            next_bytes = sum(item[1] for item in next_items)
            if (
                len(selected) + len(next_items) > 64
                or selected_bytes + next_bytes
                > self._limits_v2.max_object_bytes
            ):
                break
            for content_sha, size in next_items:
                selected[content_sha] = size
            selected_bytes += next_bytes
            upper = ordinal + 1
        if upper == after_cursor or not selected:
            raise AcquisitionSourceRetentionCapacityError(
                "v2 held candidate page exceeds provider-byte bound"
            )
        return upper, tuple(sorted(selected.items()))

    def _verify_held_page_objects_v2(
        self,
        inventory: tuple[tuple[str, int], ...],
        *,
        expected_witnesses: Mapping[str, tuple[int, int, int, int]] | None = None,
    ) -> dict[str, tuple[int, int, int, int]]:
        """Stream-hash each selected provider object under one path witness."""

        witnesses: dict[str, tuple[int, int, int, int]] = {}
        for content_sha, size in inventory:
            path = self.object_directory / f"{content_sha}.raw"
            before = _v2_file_witness(path)
            if (
                expected_witnesses is not None
                and expected_witnesses.get(content_sha) != before
            ):
                raise AcquisitionSourceRetentionIntegrityError(
                    "v2 held raw object path changed"
                )
            self._verify_exact_v2(
                path,
                maximum=max(size, 1),
                expected_sha256=content_sha,
                expected_bytes=size,
            )
            after = _v2_file_witness(path)
            if after != before:
                raise AcquisitionSourceRetentionIntegrityError(
                    "v2 held raw object changed after verification"
                )
            witnesses[content_sha] = after
        return witnesses

    @contextmanager
    def hold_candidate_page_v1(
        self,
        request: SourceReclamationRequestV2,
        *,
        after_cursor: int,
        max_candidates: int,
    ) -> Iterator[SourceReclamationCandidatePageV1]:
        """Hold one metadata-only page and all archive path witnesses."""

        if not isinstance(request, SourceReclamationRequestV2):
            raise TypeError("request must be SourceReclamationRequestV2")
        for value, label, maximum in (
            (after_cursor, "candidate page cursor", V2_MAX_CANDIDATES),
            (max_candidates, "candidate page size", 64),
        ):
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or not 0 <= value <= maximum
            ):
                raise AcquisitionSourceRetentionCapacityError(
                    f"v2 {label} is outside its bound"
                )
        if max_candidates < 1 or after_cursor >= request.candidate_count:
            raise AcquisitionSourceRetentionCapacityError(
                "v2 candidate page request is outside its bound"
            )
        if request.candidate_authority_sha256 != self.authority_sha256:
            raise AcquisitionSourceRetentionConflict(
                "v2 candidate page authority differs"
            )
        with self._local_lock_v2, self._exclusive_v2():
            self._recover_pending_v2()
            candidate_set, validation, head, ancestor = (
                self._held_candidate_snapshot_v2(request)
            )
            bindings = validation["bindings"]
            payloads = validation["reference_payloads"]
            upper, object_inventory = self._held_page_object_inventory_v2(
                validation,
                after_cursor=after_cursor,
                max_candidates=max_candidates,
            )
            closure_pairs = tuple(zip(
                validation["candidates"][after_cursor:upper],
                validation["consumer_closures"][after_cursor:upper],
                strict=True,
            ))
            with self._hold_reproved_consumer_closures_v2(
                closure_pairs,
                registry_verifier=None,
                source_archive_anchor_sha256=str(
                    candidate_set["closure_source_archive_anchor_sha256"]
                ),
                require_current_source_anchor=False,
                require_live_lifecycle=False,
            ):
                object_witnesses = self._verify_held_page_objects_v2(
                    object_inventory
                )
                page = SourceReclamationCandidatePageV1.create(
                    request_sha256=request.request_sha256,
                    candidate_authority_sha256=self.authority_sha256,
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
                    after_cursor=after_cursor,
                    total_candidate_count=request.candidate_count,
                    bindings=bindings[after_cursor:upper],
                    record_payloads=payloads[after_cursor:upper],
                )
                self._fault_v2("candidate_hold.before_yield")
                yield page
                repeated_set, repeated_validation, repeated_head, repeated_ancestor = (
                    self._held_candidate_snapshot_v2(request)
                )
                if (
                    repeated_set != candidate_set
                    or repeated_validation["bindings"] != bindings
                    or repeated_validation["reference_payloads"] != payloads
                    or repeated_head != head
                    or repeated_ancestor != ancestor
                ):
                    raise AcquisitionSourceRetentionIntegrityError(
                        "v2 held candidate archive changed"
                    )
                repeated_upper, repeated_inventory = (
                    self._held_page_object_inventory_v2(
                        repeated_validation,
                        after_cursor=after_cursor,
                        max_candidates=max_candidates,
                    )
                )
                if (
                    repeated_upper != upper
                    or repeated_inventory != object_inventory
                ):
                    raise AcquisitionSourceRetentionIntegrityError(
                        "v2 held candidate object page changed"
                    )
                self._verify_held_page_objects_v2(
                    object_inventory,
                    expected_witnesses=object_witnesses,
                )
                self._fault_v2("candidate_hold.after_reproof")

    def seal_candidates_v2(
        self,
        candidates: tuple[AcquisitionSourceCandidate, ...],
        *,
        consumer_closures: tuple[AcquisitionConsumerClosureReceipt, ...],
        now: float | None = None,
    ) -> AcquisitionSourceArchiveSealResultV2:
        if (
            not isinstance(candidates, tuple)
            or not 1 <= len(candidates) <= self._limits_v2.max_candidates
            or any(not isinstance(item, AcquisitionSourceCandidate) for item in candidates)
            or not isinstance(consumer_closures, tuple)
            or len(consumer_closures) != len(candidates)
            or any(
                not isinstance(item, AcquisitionConsumerClosureReceipt)
                for item in consumer_closures
            )
        ):
            raise TypeError("v2 candidates/consumer closures have wrong shape")
        pairs = tuple(sorted(
            zip(candidates, consumer_closures, strict=True),
            key=lambda item: item[0].candidate_sha256,
        ))
        if (
            len({item[0].candidate_sha256 for item in pairs}) != len(pairs)
            or tuple(item[1].candidate_sha256 for item in pairs)
            != tuple(item[0].candidate_sha256 for item in pairs)
        ):
            raise AcquisitionSourceRetentionConflict(
                "v2 candidate inventory differs"
            )
        observed = _finite(
            self._clock_v2() if now is None else now,
            "v2 seal clock",
            minimum=0.0,
            maximum=32_503_680_000.0,
        )
        with (
            self._local_lock_v2,
            self._exclusive_v2(),
            self._consumer_registry_authority_v2.hold_current_epoch()
            as current_registry,
        ):
            self._cleanup_staging_v2()
            self._recover_pending_v2()
            head = self._read_head_v2()
            if observed < float(head["last_sealed_at"]):
                raise AcquisitionSourceRetentionConflict(
                    "v2 archive clock rollback"
                )
            source_bindings = self._source_store_bindings_v2()
            source_root = self._source_store_set_root_v2(source_bindings)
            if head["source_store_set_root_sha256"] not in {None, source_root}:
                raise AcquisitionSourceRetentionIntegrityError(
                    "v2 source-store instance set changed"
                )
            registry_roots = {item[1].registry_root_sha256 for item in pairs}
            epoch_roots = {item[1].registry_epoch_root_sha256 for item in pairs}
            epoch_receipts = {
                item[1].registry_epoch_receipt_sha256 for item in pairs
            }
            registry_epochs = {item[1].registry_epoch for item in pairs}
            registry_stores = {
                item[1].registry_store_identity_sha256 for item in pairs
            }
            source_archive_anchors = {
                item[1].source_archive_anchor_sha256 for item in pairs
            }
            if (
                registry_roots != {current_registry.registry_root_sha256}
                or epoch_roots != {current_registry.epoch_root_sha256}
                or epoch_receipts
                != {current_registry.epoch_receipt_sha256}
                or registry_epochs != {current_registry.verification.epoch}
                or registry_stores != {current_registry.store_identity_sha256}
                or len(source_archive_anchors) != 1
            ):
                raise AcquisitionSourceRetentionConflict(
                    "v2 consumer registry epoch inventory differs"
                )
            source_archive_anchor = next(iter(source_archive_anchors))
            closures = tuple(item[1] for item in pairs)
            closure_root = self._consumer_closure_root_v2(closures)
            snapshots: list[dict[str, Any]] = []
            payload_inventory: list[tuple[str, bytes]] = []
            with self._hold_reproved_consumer_closures_v2(
                pairs,
                registry_verifier=current_registry,
                source_archive_anchor_sha256=source_archive_anchor,
                require_current_source_anchor=True,
                require_live_lifecycle=True,
            ):
                for candidate, closure in pairs:
                    snapshot, payloads = self._candidate_snapshot_v2(
                        candidate,
                        closure,
                        now=observed,
                        source_store_bindings=source_bindings,
                        source_store_set_root_sha256=source_root,
                    )
                    snapshots.append(snapshot)
                    payload_inventory.extend(payloads)
            requested_candidate_sha256s = tuple(
                item[0].candidate_sha256 for item in pairs
            )
            requested_candidate_set = set(requested_candidate_sha256s)
            superseded_candidate_bindings_sha256: str | None = None
            superseded_consumer_registry_epoch: int | None = None
            for existing_set in self._existing_candidate_sets_v2():
                existing_candidate_sha256s = tuple(
                    str(item["candidate_sha256"])
                    for item in existing_set.get("entries", ())
                    if isinstance(item, dict)
                )
                existing_candidate_set = set(existing_candidate_sha256s)
                if not existing_candidate_set & requested_candidate_set:
                    continue
                if existing_candidate_sha256s != requested_candidate_sha256s:
                    raise AcquisitionSourceRetentionConflict(
                        "v2 sealed candidate inventory overlaps"
                    )
                existing_validation = self._validate_candidate_set_locked_v2(
                    existing_set,
                    verify_objects=False,
                )
                existing_bindings = existing_validation["bindings"]
                if (
                    len(existing_bindings) != len(pairs)
                    or existing_set.get("source_store_set_root_sha256")
                    != source_root
                    or existing_validation["source_store_set_root_sha256"]
                    != source_root
                    or any(
                        binding.candidate_sha256
                        != candidate.candidate_sha256
                        or binding.acquisition_job_id
                        != candidate.acquisition_job_id
                        or binding.scheduler_job_id
                        != candidate.scheduler_job_id
                        or binding.raw_capture_id != candidate.raw_capture_id
                        or binding.evidence_stream_id
                        != candidate.evidence_stream_id
                        or binding.evidence_stream_sequence
                        != candidate.evidence_stream_sequence
                        for binding, (candidate, _closure) in zip(
                            existing_bindings,
                            pairs,
                            strict=True,
                        )
                    )
                ):
                    raise AcquisitionSourceRetentionIntegrityError(
                        "v2 successor candidate identity differs"
                    )
                exact_retry = (
                    existing_set.get("consumer_registry_root_sha256")
                    == next(iter(registry_roots))
                    and existing_set.get("consumer_epoch_root_sha256")
                    == next(iter(epoch_roots))
                    and existing_set.get(
                        "consumer_registry_epoch_receipt_sha256"
                    ) == current_registry.epoch_receipt_sha256
                    and existing_set.get("consumer_registry_epoch")
                    == current_registry.verification.epoch
                    and existing_set.get(
                        "closure_source_archive_anchor_sha256"
                    ) == source_archive_anchor
                    and existing_set.get("consumer_closure_root_sha256")
                    == closure_root
                    and all(
                        binding.sealed_archive_record_sha256
                        == snapshot["record_sha256"]
                        and binding.consumer_closure_receipt_sha256
                        == closure.receipt_sha256
                        and binding.consumer_closure_root_sha256
                        == closure_root
                        for binding, (_candidate, closure), snapshot in zip(
                            existing_bindings,
                            pairs,
                            snapshots,
                            strict=True,
                        )
                    )
                )
                if exact_retry:
                    self._assert_anchor_ancestor_locked_v2(
                        str(existing_set["anchor_sha256"]),
                        str(head["anchor_sha256"]),
                    )
                    return self._seal_result_from_set_v2(existing_set)
                existing_epoch = int(existing_set["consumer_registry_epoch"])
                if current_registry.verification.epoch <= existing_epoch:
                    raise AcquisitionSourceRetentionConflict(
                        "v2 successor candidate registry epoch is not newer"
                    )
                existing_source_anchor = str(
                    existing_set["closure_source_archive_anchor_sha256"]
                )
                if existing_source_anchor != source_archive_anchor:
                    self._assert_v1_anchor_successor_v2(
                        existing_source_anchor,
                        source_archive_anchor,
                    )
                if (
                    superseded_consumer_registry_epoch is None
                    or existing_epoch > superseded_consumer_registry_epoch
                ):
                    superseded_consumer_registry_epoch = existing_epoch
                    superseded_candidate_bindings_sha256 = str(
                        existing_set["candidate_bindings_sha256"]
                    )
                elif existing_epoch == superseded_consumer_registry_epoch:
                    raise AcquisitionSourceRetentionIntegrityError(
                        "v2 successor candidate epoch is ambiguous"
                    )
            if int(head["generation"]) >= self._limits_v2.max_archive_segments:
                raise AcquisitionSourceRetentionCapacityError(
                    "v2 archive segment capacity exhausted"
                )
            next_generation = int(head["generation"]) + 1
            previous_anchor = self._read_anchor_v2(str(head["anchor_sha256"]))
            previous_manifest = self._read_manifest_v2(
                str(previous_anchor["manifest_sha256"])
            )
            batch_root = _v2_ordered_root(
                schema="stv3-worker-source-retention-record-batch-root-v2",
                item_sha256s=tuple(item["record_sha256"] for item in snapshots),
            )
            segment_identity_body = {
                "archive_authority_sha256": self.authority_sha256,
                "batch_record_root_sha256": batch_root,
                "generation": next_generation,
                "previous_segment_sha256": previous_anchor[
                    "head_segment_sha256"
                ],
                "source_store_set_root_sha256": source_root,
            }
            segment_sha = _digest({
                "schema": (
                    "stv3-worker-source-retention-segment-identity-v2"
                ),
                **segment_identity_body,
            })
            bindings: list[SourceReclamationCandidateBindingV1] = []
            references: list[bytes] = []
            for snapshot in snapshots:
                candidate = snapshot["candidate"]
                closure = snapshot["closure"]
                reference = {
                    "archive_object_count": snapshot["archive_object_count"],
                    "archive_object_reference_root_sha256": snapshot[
                        "archive_object_reference_root_sha256"
                    ],
                    "archive_object_total_bytes": snapshot[
                        "archive_object_total_bytes"
                    ],
                    "candidate_sha256": candidate.candidate_sha256,
                    "capacity_ledger_view_sha256": snapshot["role_views"][
                        "capacity_ledger"
                    ],
                    "downstream_lifecycle_view_sha256": snapshot["role_views"][
                        "downstream_lifecycle"
                    ],
                    "execution_receipts_view_sha256": snapshot["role_views"][
                        "execution_receipts"
                    ],
                    "metadata_sha256": snapshot["metadata_sha256"],
                    "raw_artifacts_view_sha256": snapshot["role_views"][
                        "raw_artifacts"
                    ],
                    "record_sha256": snapshot["record_sha256"],
                    "scheduler_view_sha256": snapshot["role_views"]["scheduler"],
                    "schema": V2_REFERENCE_SCHEMA,
                    "sealed_archive_segment_sha256": segment_sha,
                }
                reference_bytes = reclamation_canonical_bytes(reference)
                if len(reference_bytes) > V2_MAX_REFERENCE_BYTES:
                    raise AcquisitionSourceRetentionCapacityError(
                        "v2 candidate reference exceeds protocol bound"
                    )
                binding = SourceReclamationCandidateBindingV1.create(
                    candidate_sha256=candidate.candidate_sha256,
                    acquisition_job_id=candidate.acquisition_job_id,
                    scheduler_job_id=candidate.scheduler_job_id,
                    raw_capture_id=candidate.raw_capture_id,
                    evidence_stream_id=candidate.evidence_stream_id,
                    evidence_stream_sequence=candidate.evidence_stream_sequence,
                    sealed_archive_segment_sha256=segment_sha,
                    sealed_archive_record_sha256=snapshot["record_sha256"],
                    consumer_closure_receipt_sha256=closure.receipt_sha256,
                    consumer_closure_root_sha256=closure_root,
                )
                bindings.append(binding)
                references.append(reference_bytes)
            binding_tuple = tuple(bindings)
            reference_tuple = tuple(references)
            candidate_root = reclamation_candidate_binding_root(binding_tuple)
            archive_root = reclamation_archive_binding_root(
                binding_tuple,
                reference_tuple,
            )
            segment_bytes = _envelope(
                V2_SEGMENT_SCHEMA,
                "segment_file_identity_sha256",
                {
                    **segment_identity_body,
                    "archive_binding_root_sha256": archive_root,
                    "candidate_bindings_sha256": candidate_root,
                    "record_reference_sha256s": [
                        hashlib.sha256(item).hexdigest() for item in reference_tuple
                    ],
                    "segment_sha256": segment_sha,
                    "sealed_at": observed,
                },
            )
            if len(segment_bytes) > self._limits_v2.max_segment_bytes:
                raise AcquisitionSourceRetentionCapacityError(
                    "v2 segment exceeds byte bound"
                )
            segment_file_sha = hashlib.sha256(segment_bytes).hexdigest()
            prefix_root = _digest({
                "generation": next_generation,
                "previous_prefix_root_sha256": head["prefix_root_sha256"],
                "schema": V2_PREFIX_SCHEMA,
                "segment_file_sha256": segment_file_sha,
                "segment_sha256": segment_sha,
            })
            unique_payloads: dict[str, bytes] = {}
            for _role, payload in payload_inventory:
                content_sha = hashlib.sha256(payload).hexdigest()
                prior = unique_payloads.setdefault(content_sha, payload)
                if prior != payload:
                    raise AcquisitionSourceRetentionIntegrityError(
                        "v2 raw object hash collision"
                    )
            metadata_payloads = {
                item["metadata_sha256"]: item["metadata_bytes"]
                for item in snapshots
            }
            reference_payloads = {
                hashlib.sha256(item).hexdigest(): item for item in reference_tuple
            }
            payload_bytes_added = (
                sum(len(item) for item in unique_payloads.values())
                + sum(len(item) for item in metadata_payloads.values())
                + sum(len(item) for item in reference_payloads.values())
                + len(segment_bytes)
            )
            manifest_bytes = _envelope(
                V2_MANIFEST_SCHEMA,
                "manifest_sha256",
                {
                    "archive_authority_sha256": self.authority_sha256,
                    "cumulative_file_bytes": int(
                        previous_manifest["cumulative_file_bytes"]
                    ) + payload_bytes_added,
                    "cumulative_object_bytes": int(
                        previous_manifest["cumulative_object_bytes"]
                    ) + sum(len(item) for item in unique_payloads.values()),
                    "cumulative_object_count": int(
                        previous_manifest["cumulative_object_count"]
                    ) + len(unique_payloads),
                    "cumulative_record_count": int(
                        previous_manifest["cumulative_record_count"]
                    ) + len(bindings),
                    "generation": next_generation,
                    "prefix_root_sha256": prefix_root,
                    "previous_manifest_sha256": previous_manifest[
                        "manifest_sha256"
                    ],
                    "segment_file_sha256": segment_file_sha,
                    "segment_sha256": segment_sha,
                    "source_store_set_root_sha256": source_root,
                },
            )
            manifest = _decode_envelope(
                manifest_bytes,
                schema=V2_MANIFEST_SCHEMA,
                hash_field="manifest_sha256",
            )
            anchor_bytes = _envelope(
                V2_ANCHOR_SCHEMA,
                "anchor_sha256",
                {
                    "archive_authority_sha256": self.authority_sha256,
                    "cumulative_file_bytes": manifest["cumulative_file_bytes"],
                    "cumulative_object_bytes": manifest[
                        "cumulative_object_bytes"
                    ],
                    "cumulative_object_count": manifest[
                        "cumulative_object_count"
                    ],
                    "cumulative_record_count": manifest[
                        "cumulative_record_count"
                    ],
                    "generation": next_generation,
                    "head_segment_sha256": segment_sha,
                    "manifest_sha256": manifest["manifest_sha256"],
                    "prefix_root_sha256": prefix_root,
                    "previous_anchor_sha256": head["anchor_sha256"],
                    "sealed_at": observed,
                    "source_store_set_root_sha256": source_root,
                },
            )
            anchor = _decode_envelope(
                anchor_bytes,
                schema=V2_ANCHOR_SCHEMA,
                hash_field="anchor_sha256",
            )
            entries = [{
                "binding": binding.as_dict(),
                "candidate_sha256": binding.candidate_sha256,
                "metadata_sha256": snapshot["metadata_sha256"],
                "reference_sha256": hashlib.sha256(reference).hexdigest(),
            } for binding, reference, snapshot in zip(
                binding_tuple, reference_tuple, snapshots, strict=True
            )]
            candidate_set_bytes = _envelope(
                V2_CANDIDATE_SET_SCHEMA,
                "candidate_set_sha256",
                {
                    "anchor_sha256": anchor["anchor_sha256"],
                    "archive_binding_root_sha256": archive_root,
                    "candidate_authority_sha256": self.authority_sha256,
                    "candidate_bindings_sha256": candidate_root,
                    "candidate_count": len(bindings),
                    **self._closure_source_archive_binding_v2,
                    "closure_source_archive_anchor_sha256": (
                        source_archive_anchor
                    ),
                    "consumer_closure_root_sha256": closure_root,
                    "consumer_epoch_root_sha256": next(iter(epoch_roots)),
                    "consumer_registry_epoch": (
                        current_registry.verification.epoch
                    ),
                    "consumer_registry_epoch_receipt_sha256": (
                        current_registry.epoch_receipt_sha256
                    ),
                    "consumer_registry_root_sha256": next(iter(registry_roots)),
                    "consumer_registry_store_identity_sha256": (
                        self._consumer_registry_binding_v2[
                            "consumer_registry_store_identity_sha256"
                        ]
                    ),
                    "entries": entries,
                    "object_bytes": sum(
                        item["archive_object_total_bytes"] for item in snapshots
                    ),
                    "object_count": sum(
                        item["archive_object_count"] for item in snapshots
                    ),
                    "segment_sha256": segment_sha,
                    "source_store_set_root_sha256": source_root,
                    "superseded_candidate_bindings_sha256": (
                        superseded_candidate_bindings_sha256
                    ),
                    "superseded_consumer_registry_epoch": (
                        superseded_consumer_registry_epoch
                    ),
                },
            )
            if len(candidate_set_bytes) > V2_MAX_CANDIDATE_SET_BYTES:
                raise AcquisitionSourceRetentionCapacityError(
                    "v2 candidate-set exceeds byte bound"
                )
            candidate_set = _decode_envelope(
                candidate_set_bytes,
                schema=V2_CANDIDATE_SET_SCHEMA,
                hash_field="candidate_set_sha256",
            )
            head_bytes = _envelope(
                V2_HEAD_SCHEMA,
                "head_sha256",
                {
                    "anchor_sha256": anchor["anchor_sha256"],
                    "archive_authority_sha256": self.authority_sha256,
                    "archive_instance_sha256": self._archive_instance_sha256,
                    "generation": next_generation,
                    "last_sealed_at": observed,
                    "prefix_root_sha256": prefix_root,
                    "source_store_set_root_sha256": source_root,
                },
            )
            next_head = _decode_envelope(
                head_bytes,
                schema=V2_HEAD_SCHEMA,
                hash_field="head_sha256",
            )
            targets: list[tuple[str, Path, bytes]] = []
            targets.extend((
                f"objects/{sha}.raw",
                self.object_directory / f"{sha}.raw",
                payload,
            ) for sha, payload in sorted(unique_payloads.items()))
            targets.extend((
                f"metadata/{sha}.metadata.json",
                self.metadata_directory / f"{sha}.metadata.json",
                payload,
            ) for sha, payload in sorted(metadata_payloads.items()))
            targets.extend((
                f"references/{sha}.reference.json",
                self.reference_directory / f"{sha}.reference.json",
                payload,
            ) for sha, payload in sorted(reference_payloads.items()))
            targets.extend((
                (
                    f"segments/{segment_sha}.segment.json",
                    self.segment_directory / f"{segment_sha}.segment.json",
                    segment_bytes,
                ),
                (
                    f"manifests/{manifest['manifest_sha256']}.manifest.json",
                    self.manifest_directory
                    / f"{manifest['manifest_sha256']}.manifest.json",
                    manifest_bytes,
                ),
                (
                    f"anchors/{anchor['anchor_sha256']}.anchor.json",
                    self.anchor_directory
                    / f"{anchor['anchor_sha256']}.anchor.json",
                    anchor_bytes,
                ),
                (
                    f"candidate_sets/{candidate_root}.set.json",
                    self.candidate_set_directory
                    / f"{candidate_root}.set.json",
                    candidate_set_bytes,
                ),
            ))
            projected_physical = sum(
                len(payload)
                for _relative, target, payload in targets
                if not target.exists()
            )
            current_physical = sum(
                item.stat().st_size
                for directory in (
                    self.object_directory,
                    self.metadata_directory,
                    self.reference_directory,
                    self.segment_directory,
                    self.manifest_directory,
                    self.anchor_directory,
                    self.candidate_set_directory,
                )
                for item in directory.iterdir()
                if item.is_file() and not item.is_symlink()
            )
            if (
                current_physical + projected_physical
                > self._limits_v2.max_total_archive_bytes
                or int(manifest["cumulative_record_count"])
                > self._limits_v2.max_archived_jobs
            ):
                raise AcquisitionSourceRetentionCapacityError(
                    "v2 archive total capacity exhausted"
                )
            self._verify_live_closure_authorities_v2()
            pending_targets = []
            for relative, target, payload in targets:
                preexisting = target.exists() or target.is_symlink()
                if preexisting:
                    self._read_exact_v2(
                        target,
                        maximum=max(len(payload), 1),
                        expected_sha256=hashlib.sha256(payload).hexdigest(),
                        expected_bytes=len(payload),
                    )
                pending_targets.append({
                    "bytes": len(payload),
                    "preexisting": preexisting,
                    "relative_path": relative,
                    "sha256": hashlib.sha256(payload).hexdigest(),
                })
            pending_bytes = _envelope(
                V2_PENDING_SCHEMA,
                "pending_sha256",
                {
                    "expected_head_sha256": head["head_sha256"],
                    "next_head": next_head,
                    "targets": pending_targets,
                },
            )
            if len(pending_bytes) > V2_MAX_PENDING_BYTES:
                raise AcquisitionSourceRetentionCapacityError(
                    "v2 pending publication exceeds bound"
                )
            self._write_mutable_v2(self.pending_path, pending_bytes)
            self._fault_v2("seal.after_pending")
            for relative, target, payload in targets:
                self._write_stage_v2(target, payload)
                self._fault_v2(f"seal.after_target.{relative.split('/', 1)[0]}")
            with self._hold_reproved_consumer_closures_v2(
                pairs,
                registry_verifier=current_registry,
                source_archive_anchor_sha256=source_archive_anchor,
                require_current_source_anchor=True,
                require_live_lifecycle=True,
            ):
                for (candidate, closure), snapshot in zip(
                    pairs, snapshots, strict=True
                ):
                    repeated, _repeated_payloads = self._candidate_snapshot_v2(
                        candidate,
                        closure,
                        now=observed,
                        source_store_bindings=source_bindings,
                        source_store_set_root_sha256=source_root,
                    )
                    if (
                        repeated["record_sha256"] != snapshot["record_sha256"]
                        or repeated["metadata_bytes"] != snapshot["metadata_bytes"]
                        or repeated["payload_fingerprints"]
                        != snapshot["payload_fingerprints"]
                    ):
                        raise AcquisitionSourceRetentionIntegrityError(
                            "v2 source authorities changed during seal"
                        )
            repeated_source_bindings = self._source_store_bindings_v2()
            if (
                repeated_source_bindings != source_bindings
                or self._source_store_set_root_v2(repeated_source_bindings)
                != source_root
            ):
                raise AcquisitionSourceRetentionIntegrityError(
                    "v2 source participants changed during seal"
                )
            self._verify_live_closure_authorities_v2()
            self._fault_v2("seal.before_head")
            self._write_mutable_v2(self.head_path, head_bytes)
            self._fault_v2("seal.after_head")
            self._verify_live_closure_authorities_v2()
            self._clear_pending_v2()
            self._audit_locked_v2(verify_objects=False)
            return self._seal_result_from_set_v2(candidate_set)

    @staticmethod
    def _assert_candidate_set_current_registry_v2(
        candidate_set: Mapping[str, Any],
        current_registry: AcquisitionConsumerRegistryBoundEpochVerifier,
    ) -> None:
        if (
            candidate_set.get("consumer_registry_root_sha256")
            != current_registry.registry_root_sha256
            or candidate_set.get("consumer_epoch_root_sha256")
            != current_registry.epoch_root_sha256
            or candidate_set.get("consumer_registry_epoch_receipt_sha256")
            != current_registry.epoch_receipt_sha256
            or candidate_set.get("consumer_registry_epoch")
            != current_registry.verification.epoch
            or candidate_set.get("consumer_registry_store_identity_sha256")
            != current_registry.store_identity_sha256
        ):
            raise AcquisitionSourceRetentionConflict(
                "v2 reclamation request registry epoch is stale"
            )

    @contextmanager
    def hold_current_request_authority_v1(
        self,
        request: SourceReclamationRequestV2,
    ) -> Iterator[SourceReclamationCandidatePageV1]:
        """Hold the exact current epoch/source authority for PREPARE or COMMIT."""

        if not isinstance(request, SourceReclamationRequestV2):
            raise TypeError("request must be SourceReclamationRequestV2")
        if request.candidate_authority_sha256 != self.authority_sha256:
            raise AcquisitionSourceRetentionConflict(
                "v2 current request candidate authority differs"
            )
        with (
            self._local_lock_v2,
            self._exclusive_v2(),
            self._consumer_registry_authority_v2.hold_current_epoch()
            as current_registry,
        ):
            self._recover_pending_v2()
            candidate_set, validation, head, ancestor = (
                self._held_candidate_snapshot_v2(request)
            )
            self._assert_candidate_set_current_registry_v2(
                candidate_set,
                current_registry,
            )
            pairs = tuple(zip(
                validation["candidates"],
                validation["consumer_closures"],
                strict=True,
            ))
            with self._hold_reproved_consumer_closures_v2(
                pairs,
                registry_verifier=current_registry,
                source_archive_anchor_sha256=str(
                    candidate_set["closure_source_archive_anchor_sha256"]
                ),
                require_current_source_anchor=False,
                require_live_lifecycle=True,
            ):
                self._reproof_source_store_root_v2(
                    validation["source_store_set_root_sha256"]
                )
                upper, object_inventory = self._held_page_object_inventory_v2(
                    validation,
                    after_cursor=0,
                    max_candidates=1,
                )
                object_witnesses = self._verify_held_page_objects_v2(
                    object_inventory
                )
                page = SourceReclamationCandidatePageV1.create(
                    request_sha256=request.request_sha256,
                    candidate_authority_sha256=self.authority_sha256,
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
                    after_cursor=0,
                    total_candidate_count=request.candidate_count,
                    bindings=validation["bindings"][:upper],
                    record_payloads=validation["reference_payloads"][:upper],
                )
                self._fault_v2("current_request_hold.before_yield")
                yield page
                repeated_set, repeated_validation, repeated_head, repeated_ancestor = (
                    self._held_candidate_snapshot_v2(request)
                )
                if (
                    repeated_set != candidate_set
                    or repeated_validation["bindings"]
                    != validation["bindings"]
                    or repeated_validation["reference_payloads"]
                    != validation["reference_payloads"]
                    or repeated_head != head
                    or repeated_ancestor != ancestor
                ):
                    raise AcquisitionSourceRetentionIntegrityError(
                        "v2 current request authority changed"
                    )
                self._assert_candidate_set_current_registry_v2(
                    repeated_set,
                    current_registry,
                )
                self._verify_held_page_objects_v2(
                    object_inventory,
                    expected_witnesses=object_witnesses,
                )
                self._fault_v2("current_request_hold.after_reproof")

    def create_reclamation_request_v2(
        self,
        candidate_bindings_sha256: str,
        *,
        source_binding_sha256: str,
        source_view_sha256: str,
        created_at: float | None = None,
    ) -> SourceReclamationRequestV2:
        with (
            self._local_lock_v2,
            self._exclusive_v2(),
            self._consumer_registry_authority_v2.hold_current_epoch()
            as current_registry,
        ):
            self._recover_pending_v2()
            candidate_set = self._read_candidate_set_v2(
                candidate_bindings_sha256
            )
            validation = self._validate_candidate_set_locked_v2(
                candidate_set,
                verify_objects=False,
            )
            pairs = tuple(zip(
                validation["candidates"],
                validation["consumer_closures"],
                strict=True,
            ))
            self._assert_candidate_set_current_registry_v2(
                candidate_set,
                current_registry,
            )
            with self._hold_reproved_consumer_closures_v2(
                pairs,
                registry_verifier=current_registry,
                source_archive_anchor_sha256=str(
                    candidate_set["closure_source_archive_anchor_sha256"]
                ),
                require_current_source_anchor=False,
                require_live_lifecycle=True,
            ):
                self._reproof_source_store_root_v2(
                    validation["source_store_set_root_sha256"]
                )
                self._assert_anchor_ancestor_locked_v2(
                    str(candidate_set["anchor_sha256"]),
                    str(self._read_head_v2()["anchor_sha256"]),
                )
                request = SourceReclamationRequestV2.create(
                    source_binding_sha256=_sha(
                        source_binding_sha256,
                        "v2 reclamation source binding SHA-256",
                    ),
                    source_view_sha256=_sha(
                        source_view_sha256,
                        "v2 reclamation source view SHA-256",
                    ),
                    sealed_archive_anchor_sha256=str(
                        candidate_set["anchor_sha256"]
                    ),
                    archive_binding_root_sha256=str(
                        candidate_set["archive_binding_root_sha256"]
                    ),
                    candidate_bindings_sha256=str(
                        candidate_set["candidate_bindings_sha256"]
                    ),
                    candidate_authority_sha256=self.authority_sha256,
                    consumer_registry_root_sha256=str(
                        candidate_set["consumer_registry_root_sha256"]
                    ),
                    consumer_epoch_root_sha256=str(
                        candidate_set["consumer_epoch_root_sha256"]
                    ),
                    candidate_count=int(candidate_set["candidate_count"]),
                    created_at=_finite(
                        self._clock_v2() if created_at is None else created_at,
                        "v2 reclamation request clock",
                        minimum=0.0,
                        maximum=32_503_680_000.0,
                    ),
                )
            return request

__all__ = [
    "ANCHOR_SCHEMA",
    "CONTRACT",
    "MANIFEST_SCHEMA",
    "PLAN_SCHEMA",
    "RECORD_SCHEMA",
    "SEGMENT_SCHEMA",
    "AcquisitionSourceArchiveLookup",
    "AcquisitionSourceArchiveResult",
    "AcquisitionSourceArchiveAncestorProofV2",
    "AcquisitionSourceArchiveSealResultV2",
    "AcquisitionSourceCandidate",
    "AcquisitionSourceRetentionArchive",
    "AcquisitionSourceRetentionArchiveV2",
    "AcquisitionSourceRetentionBusy",
    "AcquisitionSourceRetentionCapacityError",
    "AcquisitionSourceRetentionConflict",
    "AcquisitionSourceRetentionDurabilityError",
    "AcquisitionSourceRetentionError",
    "AcquisitionSourceRetentionIntegrityError",
    "AcquisitionSourceRetentionLimits",
    "AcquisitionSourceRetentionLimitsV2",
    "AcquisitionSourceRetentionPlan",
    "AcquisitionSourceRetentionWindowExpired",
    "RetentionDisposition",
    "V2_CONTRACT",
]
