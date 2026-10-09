"""Isolated immutable storage for exact upstream-native payload bytes.

This module proves byte identity and durable provenance receipts; it does *not*
inspect a payload and infer that decoded data is raw.  A caller must explicitly
attest the representation and capture stage in ``provenance``.

Published objects remain append-only unless the store explicitly opts into the
closed source-reclamation extension and an instance-bound coordinator proves
the complete deletion protocol.  There is no general-purpose delete API.
Capacity rejection is an explicit exception, never a weather observation and
never an implicit dry result.
"""
from __future__ import annotations

from contextlib import ExitStack, closing, contextmanager
from dataclasses import dataclass, field
from functools import lru_cache
import hashlib
import hmac
import json
import math
import os
from pathlib import Path, PurePosixPath
import re
import secrets
import sqlite3
import stat
import tempfile
import threading
import time
from typing import Any, Callable, Final, Iterator, Mapping

from .acquisition_source_reclamation_protocol import (
    MAX_CANDIDATE_PAGE_RECORDS as RECLAMATION_MAX_PAGE_CANDIDATES,
    MAX_CANDIDATE_REFERENCE_RECORD_BYTES as RECLAMATION_MAX_REFERENCE_BYTES,
    CommittedReclamationDecisionReceiptV1,
    CoordinatorAbortDecisionReceiptV1,
    HeldCommittedDecisionAuthority,
    ParticipantPrepareBindingV1,
    RawUnlinkJournalReceiptV1,
    SourceLocalAbortReceiptV1,
    SourceLocalApplyReceiptV1,
    SourceLocalPrepareReceiptV2,
    SourceReclamationCandidateAuthority,
    SourceReclamationCandidateBindingV1,
    SourceReclamationRequestV2,
    archive_binding_root as reclamation_archive_binding_root,
    candidate_binding_root as reclamation_candidate_binding_root,
    candidate_participant_view_sha256,
    canonical_bytes as reclamation_canonical_bytes,
    digest_mapping as reclamation_digest_mapping,
    require_sha256 as require_reclamation_sha256,
)
from .acquisition_source_reclamation_views import (
    participant_candidate_view_sha256 as reclamation_candidate_view_sha256,
    participant_inventory_view_sha256 as reclamation_inventory_view_sha256,
)


RAW_ARTIFACT_SCHEMA_VERSION = 3
RAW_ARTIFACT_RECEIPT_SCHEMA = "stv3-worker-raw-native-receipt-v2"
RAW_ARTIFACT_CAPTURE_SCHEMA = "stv3-worker-raw-native-capture-v2"
RAW_ARTIFACT_CAPTURE_RECEIPT_SCHEMA = (
    "stv3-worker-raw-native-capture-receipt-v1"
)
NATIVE_REPRESENTATION = "upstream_native_bytes"
NATIVE_CAPTURE_STAGE = "before_decode"
CATALOG_FILENAME = "raw_artifacts.sqlite3"
PROFILE_FILENAME = "raw_artifact_store_profile.json"
PROFILE_STAGING_FILENAME = ".raw_artifact_store_profile.stage"
STORE_PROFILE_SCHEMA = "stv3-worker-raw-store-profile-v1"
RAW_ARTIFACT_RECLAMATION_EXTENSION_SCHEMA_VERSION = 1
RAW_ARTIFACT_RECLAMATION_EXTENSION_SCHEMA = (
    "stv3-worker-raw-artifact-reclamation-extension-v1"
)
RAW_ARTIFACT_RECLAMATION_STORE_IDENTITY_SCHEMA = (
    "stv3-worker-raw-artifact-reclamation-store-identity-v1"
)
RAW_ARTIFACT_RECLAMATION_CATALOG_APPLY_SCHEMA = (
    "stv3-worker-raw-artifact-reclamation-catalog-apply-v1"
)
RAW_ARTIFACT_RECLAMATION_UNLINK_INTENT_SCHEMA = (
    "stv3-worker-raw-artifact-reclamation-unlink-intent-v1"
)
RAW_ARTIFACT_RECLAMATION_UNLINK_DURABLE_SCHEMA = (
    "stv3-worker-raw-artifact-reclamation-unlink-durable-v1"
)
RAW_ARTIFACT_RECLAMATION_TOMBSTONE_SCHEMA = (
    "stv3-worker-raw-artifact-reclamation-tombstone-v1"
)
RAW_ARTIFACT_RECLAMATION_UNLINK_FAILURE_SCHEMA = (
    "stv3-worker-raw-artifact-reclamation-unlink-failure-v1"
)
RAW_ARTIFACT_RECLAMATION_PREWIRING_BLOCKERS: Final[tuple[str, ...]] = (
    "exclusive_filesystem_owner_dirfd_policy_required_for_"
    "crash_atomic_hardlink_exclusion",
)
RAW_RECLAMATION_FILES_PENDING = "FILES_PENDING"
RAW_RECLAMATION_UNLINKED_DURABLE = "UNLINKED_DURABLE"
RAW_RECLAMATION_APPLIED = "APPLIED"
RAW_RECLAMATION_ABORTED = "ABORTED"
RAW_RECLAMATION_FAILED_FENCED = "FAILED_FENCED"
OBJECT_DIRECTORY = "objects"
OBJECT_FILE_SUFFIX = ".raw"
STAGING_DIRECTORY = "staging"
MAX_PROVENANCE_BYTES = 64 * 1024
_CATALOG_PHYSICAL_SAFETY_FACTOR = 4
_SQLITE_MIN_PAGE_SIZE_BYTES = 512
_SQLITE_MAX_PAGE_SIZE_BYTES = 64 * 1024
_SQLITE_BTREE_GROWTH_PAGES_PER_RAW_ENTRY = 64
_SQLITE_BTREE_GROWTH_PAGES_PER_CAPTURE = 16
_RAW_CATALOG_ENTRY_FIXED_RESERVE_BYTES = MAX_PROVENANCE_BYTES + 8 * 1024
_RAW_CATALOG_CAPTURE_FIXED_RESERVE_BYTES = 64 * 1024
FILE_STORE_CHUNK_BYTES = 1024 * 1024
FILE_STORE_STAGE_SUFFIX = "file_v1"

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_OBJECT_NAME_RE = re.compile(r"^[0-9a-f]{64}\.raw$")
_STAGING_NAME_RE = re.compile(r"^\.[0-9a-f]{64}\.[A-Za-z0-9_-]{1,128}$")
_REQUIRED_PROVENANCE = frozenset({
    "representation",
    "capture_stage",
    "collector",
    "collector_version",
})


def _validated_sqlite_page_size(value: object) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not _SQLITE_MIN_PAGE_SIZE_BYTES
        <= value
        <= _SQLITE_MAX_PAGE_SIZE_BYTES
        or value & (value - 1)
    ):
        raise ValueError("SQLite-pagegrootte is ongeldig")
    return value


def raw_catalog_main_database_max_bytes(
    *,
    max_catalog_bytes: int,
    page_size_bytes: int,
) -> int:
    """Return the exact writable SQLite-main envelope for a RAW catalog."""

    if (
        isinstance(max_catalog_bytes, bool)
        or not isinstance(max_catalog_bytes, int)
        or max_catalog_bytes <= 0
    ):
        raise ValueError("raw-catalogusgrens is ongeldig")
    page_size = _validated_sqlite_page_size(page_size_bytes)
    maximum_pages = max(
        1,
        max_catalog_bytes
        // (_CATALOG_PHYSICAL_SAFETY_FACTOR * page_size),
    )
    return maximum_pages * page_size


def raw_catalog_cycle_reserve_bytes(
    *,
    artifact_entries: int,
    captures: int,
    page_size_bytes: int,
) -> int:
    """Conservatively bound main-DB growth for one complete RAW cycle.

    Each artifact may add an object, receipt and capture-member row plus all
    associated indexes, and its provenance alone may occupy 64 KiB.  The page
    budgets cover a cascading split at every level of all touched B-trees;
    64 pages is above SQLite's maximum possible B-tree depth.  Capture rows
    receive a separate manifest and B-tree growth allowance.
    """

    for value, name in (
        (artifact_entries, "artifact_entries"),
        (captures, "captures"),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError(f"{name} is ongeldig")
    if artifact_entries < captures:
        raise ValueError("een RAW-cyclus kan niet minder artifacts dan captures hebben")
    page_size = _validated_sqlite_page_size(page_size_bytes)
    per_entry = (
        _RAW_CATALOG_ENTRY_FIXED_RESERVE_BYTES
        + _SQLITE_BTREE_GROWTH_PAGES_PER_RAW_ENTRY * page_size
    )
    per_capture = (
        _RAW_CATALOG_CAPTURE_FIXED_RESERVE_BYTES
        + _SQLITE_BTREE_GROWTH_PAGES_PER_CAPTURE * page_size
    )
    result = artifact_entries * per_entry + captures * per_capture
    if result > (1 << 63) - 1:
        raise ValueError("RAW-cataloguscyclusreserve is te groot")
    return max(page_size, result)


class RawArtifactError(RuntimeError):
    """Base error for the isolated raw/native store."""


class RawArtifactCapacityError(RawArtifactError):
    """A hard object or total byte boundary rejected an entire write."""

    def __init__(
        self,
        *,
        scope: str,
        current_bytes: int,
        attempted_bytes: int,
        limit_bytes: int,
    ) -> None:
        self.scope = scope
        self.current_bytes = int(current_bytes)
        self.attempted_bytes = int(attempted_bytes)
        self.limit_bytes = int(limit_bytes)
        super().__init__(
            f"raw-artifactcapaciteit ({scope}) overschreden: "
            f"{self.current_bytes}+{self.attempted_bytes}>"
            f"{self.limit_bytes} bytes"
        )


class RawArtifactEntryCapacityError(RawArtifactCapacityError):
    """A hard object- or staging-entry boundary rejected a write or scan."""

    def __init__(
        self,
        *,
        scope: str,
        current_entries: int,
        attempted_entries: int,
        limit_entries: int,
    ) -> None:
        self.scope = scope
        self.current_entries = int(current_entries)
        self.attempted_entries = int(attempted_entries)
        self.limit_entries = int(limit_entries)
        RawArtifactError.__init__(
            self,
            f"raw-artifactcapaciteit ({scope}) overschreden: "
            f"{self.current_entries}+{self.attempted_entries}>"
            f"{self.limit_entries} bestanden",
        )


class RawArtifactConflictError(RawArtifactError):
    """An immutable upstream identity was reused for different facts."""


class RawArtifactIntegrityError(RawArtifactError):
    """Stored bytes, metadata or a filesystem path failed verification."""


class RawArtifactProvenanceError(RawArtifactError):
    """The caller did not make a valid explicit native-byte provenance claim."""


class RawArtifactNotFoundError(RawArtifactError):
    """A requested immutable receipt does not exist."""


@dataclass(frozen=True, slots=True)
class RawArtifactLimits:
    """Hard payload limits; no truncation, eviction or fallback is allowed."""

    max_object_bytes: int = 128 * 1024 * 1024
    max_total_bytes: int = 8 * 1024 * 1024 * 1024
    max_object_entries: int = 65_536
    max_staging_entries: int = 64
    max_receipt_entries: int = 262_144
    max_capture_entries: int = 262_144
    max_capture_members: int = 64
    max_catalog_bytes: int = 1024 * 1024 * 1024

    def __post_init__(self) -> None:
        for name in ("max_object_bytes", "max_total_bytes"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"raw-artifactgrens {name} moet positief geheel zijn")
        if self.max_object_bytes > self.max_total_bytes:
            raise ValueError("objectgrens mag de totale raw-artifactgrens niet overschrijden")
        for name in (
            "max_object_entries",
            "max_staging_entries",
            "max_receipt_entries",
            "max_capture_entries",
            "max_capture_members",
            "max_catalog_bytes",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"raw-artifactgrens {name} moet positief geheel zijn")
        if self.max_catalog_bytes < 1024 * 1024:
            raise ValueError("raw-artifactcatalogusgrens moet minstens 1 MiB zijn")


@dataclass(frozen=True, slots=True)
class _StorageUsage:
    total_bytes: int
    entries: frozenset[str]
    object_entries: int
    staging_entries: int
    fingerprints: tuple[tuple[int, int, int, int], tuple[int, int, int, int]]


@dataclass(frozen=True, slots=True)
class _ExternalFileAuthority:
    """Held identity for one caller-owned, regular, single-link source file."""

    path: Path
    descriptor: int
    file_identity: tuple[int, int, int, int, int]
    path_time_identity: tuple[int, int]
    descriptor_time_identity: tuple[int, int]
    ancestor_identities: tuple[tuple[Path, int, int, int], ...]


@dataclass(frozen=True, slots=True)
class _ReclamationCandidateEvidence:
    binding: SourceReclamationCandidateBindingV1
    metadata_reference: bytes
    record: dict[str, Any]


@dataclass(frozen=True, slots=True)
class _ReclamationPreparePlan:
    candidate_rows: tuple[dict[str, Any], ...]
    member_rows: tuple[dict[str, Any], ...]
    receipt_rows: tuple[dict[str, Any], ...]
    object_rows: tuple[dict[str, Any], ...]
    source_view_sha256: str
    child_reference_root_sha256: str
    source_fence_sha256: str
    releasable_bytes: int
    releasable_entries: int


@dataclass(frozen=True, slots=True)
class _ReclamationFileIdentity:
    content_sha256: str
    relative_path: str
    size_bytes: int
    st_dev: int
    st_ino: int
    st_mode: int
    st_nlink: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "content_sha256": self.content_sha256,
            "relative_path": self.relative_path,
            "size_bytes": self.size_bytes,
            "st_dev": self.st_dev,
            "st_ino": self.st_ino,
            "st_mode": self.st_mode,
            "st_nlink": self.st_nlink,
        }


@dataclass(frozen=True, slots=True)
class _ReclamationAuthorityPairSnapshot:
    candidate: SourceReclamationCandidateAuthority
    decision: HeldCommittedDecisionAuthority
    candidate_sha256: str
    decision_sha256: str


@dataclass(frozen=True, slots=True)
class RawArtifactReceipt:
    """Payload-free receipt for one exact immutable upstream object."""

    receipt_id: str
    artifact_path: str
    content_sha256: str
    size_bytes: int
    provider: str
    product: str
    fetch_id: str
    content_type: str
    received_at: float
    upstream_id: str
    provenance_sha256: str
    receipt_sha256: str
    created_at: float
    _provenance_json: str = field(repr=False)

    @property
    def provenance(self) -> dict[str, Any]:
        """Return an independent copy of the caller-supplied provenance."""
        return json.loads(self._provenance_json)

    def to_metadata(self) -> dict[str, Any]:
        """Return explicit metadata without embedding any payload bytes."""
        return {
            "schema": RAW_ARTIFACT_RECEIPT_SCHEMA,
            "receipt_id": self.receipt_id,
            "artifact_path": self.artifact_path,
            "content_sha256": self.content_sha256,
            "size_bytes": self.size_bytes,
            "provider": self.provider,
            "product": self.product,
            "fetch_id": self.fetch_id,
            "content_type": self.content_type,
            "received_at": self.received_at,
            "upstream_id": self.upstream_id,
            "provenance": self.provenance,
            "provenance_sha256": self.provenance_sha256,
            "receipt_sha256": self.receipt_sha256,
            "created_at": self.created_at,
        }


@dataclass(frozen=True, slots=True)
class RawArtifactCaptureMember:
    """One role-bound raw receipt in a complete capture manifest."""

    role: str
    receipt_id: str
    content_sha256: str
    receipt_sha256: str
    received_at: float

    @property
    def member_id(self) -> str:
        """The schema-3 capture's unique member identity is its role."""
        return self.role


@dataclass(frozen=True, slots=True)
class RawArtifactCaptureReceipt:
    """Atomic proof that all required raw members form one capture."""

    capture_id: str
    provider: str
    product: str
    fetch_id: str
    upstream_capture_id: str
    product_timestamp: float | None
    manifest_sha256: str
    capture_receipt_sha256: str
    created_at: float
    members: tuple[RawArtifactCaptureMember, ...]

    @property
    def raw_store_schema_version(self) -> int:
        return RAW_ARTIFACT_SCHEMA_VERSION

    @property
    def capture_schema(self) -> str:
        return RAW_ARTIFACT_CAPTURE_SCHEMA

    @property
    def capture_receipt_schema(self) -> str:
        return RAW_ARTIFACT_CAPTURE_RECEIPT_SCHEMA

    @property
    def complete(self) -> bool:
        return True

    def to_metadata(self) -> dict[str, Any]:
        return {
            "schema": RAW_ARTIFACT_CAPTURE_SCHEMA,
            "raw_store_schema_version": RAW_ARTIFACT_SCHEMA_VERSION,
            "complete": self.complete,
            "capture_receipt_schema": (
                RAW_ARTIFACT_CAPTURE_RECEIPT_SCHEMA
            ),
            "capture_receipt_sha256": self.capture_receipt_sha256,
            "capture_id": self.capture_id,
            "provider": self.provider,
            "product": self.product,
            "fetch_id": self.fetch_id,
            "upstream_capture_id": self.upstream_capture_id,
            "product_timestamp": self.product_timestamp,
            "manifest_sha256": self.manifest_sha256,
            "created_at": self.created_at,
            "members": [
                {
                    "member_id": member.member_id,
                    "role": member.role,
                    "receipt_id": member.receipt_id,
                    "content_sha256": member.content_sha256,
                    "receipt_sha256": member.receipt_sha256,
                    "received_at": member.received_at,
                }
                for member in self.members
            ],
        }


class RawArtifactStore:
    """Filesystem objects plus a crash-safe, idempotent SQLite receipt catalog."""

    # ``_TABLES`` deliberately remains the exact legacy-v3 catalogue.  A
    # pre-extension binary therefore rejects every extension-v1 store instead
    # of silently ignoring reclamation authority that it cannot understand.
    _TABLES = frozenset({
        "raw_artifact_metadata",
        "raw_artifact_objects",
        "raw_artifact_receipts",
        "raw_artifact_captures",
        "raw_artifact_capture_members",
    })
    _COLUMNS = {
        "raw_artifact_metadata": frozenset({"metadata_key", "schema_version"}),
        "raw_artifact_objects": frozenset({
            "content_sha256", "relative_path", "size_bytes", "created_at",
        }),
        "raw_artifact_receipts": frozenset({
            "receipt_id", "provider", "product", "fetch_id", "upstream_id",
            "content_type", "received_at", "content_sha256", "size_bytes",
            "provenance_json", "provenance_sha256", "receipt_sha256",
            "created_at",
        }),
        "raw_artifact_captures": frozenset({
            "capture_id", "provider", "product", "fetch_id",
            "upstream_capture_id",
            "product_timestamp", "manifest_json", "manifest_sha256",
            "created_at",
        }),
        "raw_artifact_capture_members": frozenset({
            "capture_id", "role", "receipt_id",
        }),
    }
    _RECLAMATION_TABLES = frozenset({
        "raw_artifact_reclamation_metadata",
        "raw_artifact_reclamation_prepares",
        "raw_artifact_reclamation_candidates",
        "raw_artifact_reclamation_capture_members",
        "raw_artifact_reclamation_receipt_references",
        "raw_artifact_reclamation_object_references",
        "raw_artifact_reclamation_catalog_applies",
        "raw_artifact_reclamation_unlink_intents",
        "raw_artifact_reclamation_unlink_failures",
        "raw_artifact_reclamation_unlink_durable",
        "raw_artifact_reclamation_unlinked_batches",
        "raw_artifact_reclamation_applies",
        "raw_artifact_reclamation_aborts",
        "raw_artifact_reclamation_capture_tombstones",
        "raw_artifact_reclamation_receipt_tombstones",
    })
    _RECLAMATION_COLUMNS = {
        "raw_artifact_reclamation_metadata": frozenset({
            "metadata_key", "extension_schema_version", "base_schema_version",
            "extension_schema", "store_instance_nonce",
            "source_store_identity_sha256", "store_profile_sha256", "created_at",
        }),
        "raw_artifact_reclamation_prepares": frozenset({
            "request_sha256", "request_json", "prepare_receipt_sha256",
            "prepare_receipt_json", "source_fence_sha256",
            "child_reference_root_sha256", "candidate_bindings_sha256",
            "candidate_authority_sha256", "coordinator_authority_sha256",
            "source_view_sha256", "candidate_count", "releasable_bytes",
            "releasable_entries", "prepared_at",
        }),
        "raw_artifact_reclamation_candidates": frozenset({
            "request_sha256", "candidate_ordinal", "binding_sha256",
            "candidate_sha256", "acquisition_job_id", "scheduler_job_id",
            "record_sha256", "segment_sha256", "metadata_reference",
            "metadata_reference_sha256", "metadata_sha256",
            "archive_object_count", "archive_object_reference_root_sha256",
            "archive_object_total_bytes", "downstream_lifecycle_view_sha256",
            "execution_receipts_view_sha256", "scheduler_view_sha256",
            "raw_artifacts_view_sha256", "capacity_ledger_view_sha256", "capture_id",
            "capture_receipt_sha256", "provider", "product", "fetch_id",
            "upstream_capture_id", "manifest_sha256", "evidence_stream_id",
            "evidence_stream_sequence", "consumer_closure_receipt_sha256",
            "consumer_closure_root_sha256",
        }),
        "raw_artifact_reclamation_capture_members": frozenset({
            "request_sha256", "capture_id", "role", "receipt_id",
            "receipt_sha256", "content_sha256", "size_bytes",
        }),
        "raw_artifact_reclamation_receipt_references": frozenset({
            "request_sha256", "receipt_id", "provider", "product", "fetch_id",
            "upstream_id", "receipt_sha256", "content_sha256", "size_bytes",
            "total_capture_references", "candidate_capture_references",
            "delete_eligible",
        }),
        "raw_artifact_reclamation_object_references": frozenset({
            "request_sha256", "content_sha256", "relative_path", "size_bytes",
            "total_receipt_references", "delete_receipt_references",
            "delete_eligible",
        }),
        "raw_artifact_reclamation_catalog_applies": frozenset({
            "request_sha256", "prepare_receipt_sha256",
            "committed_decision_sha256", "committed_decision_receipt_sha256",
            "committed_decision_json", "predecessor_apply_receipt_sha256",
            "catalog_apply_sha256", "shared_reference_root_sha256",
            "unlink_item_root_sha256", "unlink_item_count", "unlink_bytes",
            "files_pending_at", "state",
        }),
        "raw_artifact_reclamation_unlink_intents": frozenset({
            "request_sha256", "unlink_ordinal", "intent_sha256",
            "content_sha256", "relative_path", "size_bytes", "st_dev",
            "st_ino", "st_mode", "st_nlink",
        }),
        "raw_artifact_reclamation_unlink_failures": frozenset({
            "request_sha256", "unlink_ordinal", "intent_sha256", "phase",
            "observed_st_dev", "observed_st_ino", "observed_st_mode",
            "observed_st_nlink", "observed_st_size", "object_path_absent",
            "failure_sha256", "observed_at", "state",
        }),
        "raw_artifact_reclamation_unlink_durable": frozenset({
            "request_sha256", "unlink_ordinal", "intent_sha256",
            "predecessor_unlink_sha256", "directory_barrier_sha256",
            "directory_st_dev", "directory_st_ino", "directory_st_mode",
            "object_was_present", "unlink_event_sha256", "unlinked_at",
            "state",
        }),
        "raw_artifact_reclamation_unlinked_batches": frozenset({
            "request_sha256", "unlink_item_root_sha256",
            "directory_barrier_root_sha256", "last_unlink_event_sha256",
            "unlinked_durable_sha256", "unlink_item_count", "unlinked_bytes",
            "completed_at", "state",
        }),
        "raw_artifact_reclamation_applies": frozenset({
            "request_sha256", "apply_receipt_sha256", "apply_receipt_json",
            "journal_receipt_sha256", "journal_receipt_json", "effect_sha256",
            "post_apply_view_sha256", "released_bytes", "released_entries",
            "applied_at", "state",
        }),
        "raw_artifact_reclamation_aborts": frozenset({
            "request_sha256", "abort_receipt_sha256", "abort_receipt_json",
            "coordinator_abort_decision_sha256",
            "coordinator_abort_receipt_sha256", "coordinator_abort_receipt_json",
            "fence_release_root_sha256", "aborted_at", "state",
        }),
        "raw_artifact_reclamation_capture_tombstones": frozenset({
            "capture_id", "provider", "product", "fetch_id",
            "upstream_capture_id", "manifest_sha256", "capture_receipt_sha256",
            "request_sha256", "catalog_apply_sha256", "tombstone_sha256",
            "created_at",
        }),
        "raw_artifact_reclamation_receipt_tombstones": frozenset({
            "receipt_id", "provider", "product", "fetch_id", "upstream_id",
            "receipt_sha256", "request_sha256",
            "catalog_apply_sha256", "tombstone_sha256", "created_at",
        }),
    }
    _RECLAMATION_INDEX_COLUMNS = {
        "idx_raw_artifact_reclamation_candidates_capture": (
            "capture_id", "request_sha256",
        ),
        "idx_raw_artifact_reclamation_members_receipt": (
            "receipt_id", "request_sha256",
        ),
        "idx_raw_artifact_reclamation_receipts_content": (
            "content_sha256", "request_sha256",
        ),
    }
    _RECEIPT_SELECT = """
        SELECT receipt.receipt_id, receipt.provider, receipt.product,
               receipt.fetch_id, receipt.upstream_id, receipt.content_type,
               receipt.received_at, receipt.content_sha256,
               receipt.size_bytes, receipt.provenance_json,
               receipt.provenance_sha256, receipt.receipt_sha256,
               receipt.created_at, object.relative_path,
               object.size_bytes AS object_size_bytes
        FROM raw_artifact_receipts AS receipt
        JOIN raw_artifact_objects AS object
          ON object.content_sha256=receipt.content_sha256
    """

    def __init__(
        self,
        root: str | Path,
        *,
        limits: RawArtifactLimits | None = None,
        fault_injector: Callable[[str], None] | None = None,
        candidate_authority: SourceReclamationCandidateAuthority | None = None,
        committed_decision_authority: HeldCommittedDecisionAuthority | None = None,
        reclamation_directory_fsync: Callable[[Path], None] | None = None,
        reclamation_clock: Callable[[], float] | None = None,
    ) -> None:
        supplied_root = Path(root)
        if not supplied_root.is_absolute():
            raise ValueError("raw-artifactroot moet een absoluut pad zijn")
        self.root = supplied_root
        self.objects_path = self.root / OBJECT_DIRECTORY
        self.staging_path = self.root / STAGING_DIRECTORY
        self.catalog_path = self.root / CATALOG_FILENAME
        self.profile_path = self.root / PROFILE_FILENAME
        self.profile_staging_path = self.root / PROFILE_STAGING_FILENAME
        self.limits = limits or RawArtifactLimits()
        self._fault_injector = fault_injector
        self._lock = threading.RLock()
        self._candidate_authority: SourceReclamationCandidateAuthority | None = None
        self._committed_decision_authority: HeldCommittedDecisionAuthority | None = (
            None
        )
        self._candidate_authority_sha256: str | None = None
        self._committed_decision_authority_sha256: str | None = None
        if (candidate_authority is None) != (
            committed_decision_authority is None
        ):
            raise ValueError(
                "raw reclamation authorities moeten beide tegelijk worden gebonden"
            )
        if candidate_authority is not None:
            assert committed_decision_authority is not None
            self.bind_reclamation_authorities_v1(
                candidate_authority,
                committed_decision_authority,
            )
        if (
            reclamation_directory_fsync is not None
            and not callable(reclamation_directory_fsync)
        ):
            raise TypeError(
                "raw reclamation directory-syncauthority moet callable zijn"
            )
        # A supplied callback is an explicit constructor-held durability
        # authority.  POSIX has a strict built-in implementation; Windows has
        # none because the legacy best-effort directory fsync is not proof.
        self._reclamation_directory_fsync_authority = (
            reclamation_directory_fsync
        )
        if reclamation_clock is not None and not callable(reclamation_clock):
            raise TypeError("raw reclamation clock moet callable zijn")
        self._reclamation_clock = reclamation_clock or time.time
        self._storage_usage_cache: _StorageUsage | None = None
        self._initialized = False

    def bind_reclamation_authorities_v1(
        self,
        candidate_authority: SourceReclamationCandidateAuthority,
        committed_decision_authority: HeldCommittedDecisionAuthority,
    ) -> None:
        """Create-once bind both live reclamation authorities as one pair.

        This in-memory binding closes the construction cycle between a raw
        store, the retention candidate authority and its coordinator without
        replacing the already injected store object or changing persistent
        store identity.  Only an unbound-to-exact-pair transition is allowed;
        rebinding the identical two authority objects is an idempotent proof.
        """

        with self._lock:
            if not isinstance(
                candidate_authority,
                SourceReclamationCandidateAuthority,
            ):
                raise TypeError(
                    "raw reclamation candidate-authority heeft een ongeldig type"
                )
            if not isinstance(
                committed_decision_authority,
                HeldCommittedDecisionAuthority,
            ):
                raise TypeError(
                    "raw reclamation decision-authority heeft een ongeldig type"
                )
            candidate_sha256 = require_reclamation_sha256(
                candidate_authority.authority_sha256,
                "raw reclamation candidate-authority SHA-256",
            )
            decision_sha256 = require_reclamation_sha256(
                committed_decision_authority.authority_sha256,
                "raw reclamation decision-authority SHA-256",
            )
            current_candidate = self._candidate_authority
            current_decision = self._committed_decision_authority
            current_candidate_sha256 = self._candidate_authority_sha256
            current_decision_sha256 = (
                self._committed_decision_authority_sha256
            )
            if (
                current_candidate is None
                and current_decision is None
                and current_candidate_sha256 is None
                and current_decision_sha256 is None
            ):
                self._candidate_authority = candidate_authority
                self._committed_decision_authority = (
                    committed_decision_authority
                )
                self._candidate_authority_sha256 = candidate_sha256
                self._committed_decision_authority_sha256 = decision_sha256
                return
            if (
                current_candidate is None
                or current_decision is None
                or current_candidate_sha256 is None
                or current_decision_sha256 is None
            ):
                raise RawArtifactIntegrityError(
                    "raw reclamation authoritybinding is onvolledig"
                )
            if (
                current_candidate is candidate_authority
                and current_decision is committed_decision_authority
            ):
                if (
                    not hmac.compare_digest(
                        candidate_sha256,
                        current_candidate_sha256,
                    )
                    or not hmac.compare_digest(
                        decision_sha256,
                        current_decision_sha256,
                    )
                ):
                    raise RawArtifactIntegrityError(
                        "raw reclamation authority-identiteit is gewijzigd"
                    )
                return
            raise RawArtifactConflictError(
                "raw reclamation authorities zijn al aan een ander exact paar gebonden"
            )

    def initialize(self) -> dict[str, Any]:
        """Create or validate the dedicated catalog without deleting anything."""
        with self._lock:
            self._prepare_layout()
            catalog_existed = self.catalog_path.exists()
            try:
                with closing(self._connect_unconfigured()) as database:
                    database.execute("PRAGMA foreign_keys=ON")
                    database.execute("PRAGMA synchronous=FULL")
                    database.execute("BEGIN IMMEDIATE")
                    try:
                        tables = self._table_names(database)
                        if not tables:
                            self._create_schema(database)
                        self._validate_schema(database)
                        database.commit()
                    except BaseException:
                        if database.in_transaction:
                            database.rollback()
                        raise
                if not catalog_existed:
                    self._fsync_directory(self.root)
                with closing(self._connect()) as database:
                    self._validate_schema(database)
            except sqlite3.DatabaseError as exc:
                raise RawArtifactIntegrityError(
                    "raw-artifactcatalogus is geen geldige SQLite-database"
                ) from exc
            self._initialized = True
            return self.stats()

    def attach_existing(self) -> None:
        """Attach a capture child to the already-proven parent raw store.

        The worker parent performs :meth:`initialize`, the complete catalog
        integrity proof and the physical retention audit before acquisition
        admission opens.  Every provider attempt then runs in a fresh child
        process.  Repeating ``initialize()`` in every child used to execute a
        ``BEGIN IMMEDIATE``, ``PRAGMA integrity_check`` and a full physical
        stats scan.  With parallel provider I/O those readers and writers
        needlessly serialized on the shared rollback-journal catalog and
        could consume an entire provider deadline before HTTP even started.

        This method is deliberately attach-only: it creates no directory,
        profile or schema and it never repairs anything.  It re-proves the
        exact store profile, regular catalog identity, closed table set and
        immutable base schema using a read-only SQLite connection.  Missing
        or conflicting state fails closed; the parent remains the sole
        startup/integrity authority.
        """

        with self._lock:
            self._assert_safe_layout()
            try:
                info = self.catalog_path.lstat()
            except FileNotFoundError as exc:
                raise RawArtifactIntegrityError(
                    "raw-artifactchild mist de bewezen parentcatalogus"
                ) from exc
            if self.catalog_path.is_symlink() or not stat.S_ISREG(info.st_mode):
                raise RawArtifactIntegrityError(
                    "raw-artifactchildcatalogus is geen regulier bestand"
                )
            if int(info.st_size) <= 0:
                raise RawArtifactIntegrityError(
                    "raw-artifactchildcatalogus is leeg"
                )
            if self._catalog_database_bytes() > self.limits.max_catalog_bytes:
                raise RawArtifactCapacityError(
                    scope="catalog",
                    current_bytes=self._catalog_database_bytes(),
                    attempted_bytes=0,
                    limit_bytes=self.limits.max_catalog_bytes,
                )
            try:
                uri = f"{self.catalog_path.resolve().as_uri()}?mode=ro"
                with closing(sqlite3.connect(
                    uri,
                    uri=True,
                    timeout=5.0,
                )) as database:
                    database.row_factory = sqlite3.Row
                    database.execute("PRAGMA query_only=ON")
                    tables = self._table_names(database)
                    extension_tables = self._TABLES | self._RECLAMATION_TABLES
                    if tables not in (self._TABLES, extension_tables):
                        raise RawArtifactIntegrityError(
                            "raw-artifactchildcatalogus heeft onbekend schema"
                        )
                    self._validate_base_schema(database)
                    if tables == extension_tables:
                        self._validate_reclamation_extension_schema(database)
                    journal_mode = str(database.execute(
                        "PRAGMA journal_mode"
                    ).fetchone()[0]).lower()
                    if journal_mode != "delete":
                        raise RawArtifactIntegrityError(
                            "raw-artifactchildcatalogus wijkt af van rollback-journaling"
                        )
            except RawArtifactError:
                raise
            except sqlite3.DatabaseError as exc:
                raise RawArtifactIntegrityError(
                    "raw-artifactchild kon parentcatalogus niet valideren"
                ) from exc
            self._initialized = True

    def recover_unmanaged_content_objects(self) -> int:
        """Recover crash stages and catalog objects without inventing receipts.

        A child can be hard-killed while writing its private staging file, or
        after atomically publishing its content-addressed object but before the
        surrounding SQLite transaction commits.  Under the worker's exclusive
        startup fence, complete stages are published, incomplete scratch stages
        are removed, and verified objects are registered only as neutral object
        facts.  A later normal retry can attach its exact receipt and capture;
        no provider/provenance claim is invented.
        """

        self._require_initialized()
        recovered = 0
        with self._lock, closing(self._connect()) as database:
            database.execute("BEGIN IMMEDIATE")
            try:
                self._recover_crash_staging_objects()
                storage_usage = self._scan_storage(force=True)
                if storage_usage.staging_entries != 0:
                    raise RawArtifactIntegrityError(
                        "raw-artifactstaging vereist afzonderlijk herstel"
                    )
                catalog_paths = {
                    str(row[0])
                    for row in database.execute(
                        "SELECT relative_path FROM raw_artifact_objects"
                    )
                }
                unmanaged = sorted(storage_usage.entries - catalog_paths)
                for relative_path in unmanaged:
                    supplied = PurePosixPath(relative_path)
                    if (
                        len(supplied.parts) != 2
                        or supplied.parts[0] != OBJECT_DIRECTORY
                        or _OBJECT_NAME_RE.fullmatch(supplied.parts[1]) is None
                    ):
                        raise RawArtifactIntegrityError(
                            "raw-artifactopslag bevat onbekend unmanaged object"
                        )
                    content_sha256 = supplied.parts[1][
                        :-len(OBJECT_FILE_SUFFIX)
                    ]
                    target = self._safe_object_path(
                        relative_path,
                        content_sha256,
                    )
                    info = target.lstat()
                    if (
                        target.is_symlink()
                        or not stat.S_ISREG(info.st_mode)
                        or info.st_size > self.limits.max_object_bytes
                    ):
                        raise RawArtifactIntegrityError(
                            "unmanaged raw-artifactobject is onveilig"
                        )
                    self._read_verified(
                        target,
                        expected_size=int(info.st_size),
                        expected_sha256=content_sha256,
                        return_bytes=False,
                    )
                    database.execute(
                        """
                        INSERT INTO raw_artifact_objects(
                            content_sha256, relative_path, size_bytes, created_at
                        ) VALUES(?,?,?,?)
                        """,
                        (
                            content_sha256,
                            relative_path,
                            int(info.st_size),
                            time.time(),
                        ),
                    )
                    recovered += 1
                database.commit()
            except sqlite3.IntegrityError as exc:
                if database.in_transaction:
                    database.rollback()
                raise RawArtifactIntegrityError(
                    "unmanaged raw-artifactobject recovery conflict"
                ) from exc
            except sqlite3.OperationalError as exc:
                if database.in_transaction:
                    database.rollback()
                if self._is_sqlite_capacity_error(exc):
                    raise self._catalog_capacity_error() from exc
                raise
            except BaseException:
                if database.in_transaction:
                    database.rollback()
                raise
        if recovered:
            verified = self.stats()
            if (
                verified["unmanaged_file_count"] != 0
                or verified["catalog_bytes"] != verified["filesystem_bytes"]
                or verified["object_count"]
                != verified["filesystem_object_entries"]
            ):
                raise RawArtifactIntegrityError(
                    "unmanaged raw-artifactobject recovery verifieert niet"
                )
        return recovered

    @staticmethod
    def _recovery_file_identity(info: os.stat_result) -> tuple[int, ...]:
        return (
            int(info.st_dev),
            int(info.st_ino),
            int(info.st_mode),
            int(info.st_size),
            int(info.st_nlink),
        )

    def _crash_stage_sha256(
        self,
        stage_path: Path,
        expected_info: os.stat_result,
    ) -> str:
        """Hash one proven private stage without following links."""

        flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(
            os,
            "O_NOFOLLOW",
            0,
        )
        try:
            descriptor = os.open(stage_path, flags)
        except OSError as exc:
            raise RawArtifactIntegrityError(
                "raw-artifact crash-stage kon niet veilig worden geopend"
            ) from exc
        try:
            expected_identity = self._recovery_file_identity(expected_info)
            opened = os.fstat(descriptor)
            if self._recovery_file_identity(opened) != expected_identity:
                raise RawArtifactIntegrityError(
                    "raw-artifact crash-stage veranderde voor herstel"
                )
            digest = hashlib.sha256()
            while chunk := os.read(descriptor, FILE_STORE_CHUNK_BYTES):
                digest.update(chunk)
            after_descriptor = os.fstat(descriptor)
            after_path = stage_path.lstat()
            if (
                self._recovery_file_identity(after_descriptor)
                != expected_identity
                or self._recovery_file_identity(after_path)
                != expected_identity
            ):
                raise RawArtifactIntegrityError(
                    "raw-artifact crash-stage veranderde tijdens herstel"
                )
            return digest.hexdigest()
        finally:
            os.close(descriptor)

    def _recover_crash_staging_objects(self) -> int:
        """Publish complete stages and remove only proven private scratch files."""

        directory_identities = self._file_publication_directory_identities()
        recovered = 0
        for stage_path in sorted(self.staging_path.iterdir()):
            if _STAGING_NAME_RE.fullmatch(stage_path.name) is None:
                raise RawArtifactIntegrityError(
                    "raw-artifactopslag bevat onbekende staging-entry"
                )
            info = stage_path.lstat()
            if (
                self._path_is_link_or_reparse(stage_path, info)
                or not stat.S_ISREG(info.st_mode)
                or int(info.st_ino) <= 0
                or int(info.st_nlink) not in {1, 2}
                or int(info.st_size) > self.limits.max_object_bytes
            ):
                raise RawArtifactIntegrityError(
                    "raw-artifact crash-stage heeft een onveilige identity"
                )
            content_sha256 = stage_path.name.split(".", 2)[1]
            actual_sha256 = self._crash_stage_sha256(stage_path, info)
            target = self._safe_object_path(
                self._relative_object_path(content_sha256),
                content_sha256,
            )
            target_exists = target.exists()
            if target_exists:
                self._read_verified(
                    target,
                    expected_size=int(target.lstat().st_size),
                    expected_sha256=content_sha256,
                    return_bytes=False,
                )
                target_info = target.lstat()
                if int(info.st_nlink) == 2 and (
                    int(info.st_dev),
                    int(info.st_ino),
                ) != (
                    int(target_info.st_dev),
                    int(target_info.st_ino),
                ):
                    raise RawArtifactIntegrityError(
                        "raw-artifact linked crash-stage wijkt af van objecttarget"
                    )
            elif actual_sha256 == content_sha256:
                if int(info.st_nlink) != 1:
                    raise RawArtifactIntegrityError(
                        "raw-artifact complete crash-stage heeft onbekende hardlink"
                    )
                os.link(stage_path, target)
                self._fsync_directory(self.objects_path)
                self._read_verified(
                    target,
                    expected_size=int(info.st_size),
                    expected_sha256=content_sha256,
                    return_bytes=False,
                )
                recovered += 1
            elif int(info.st_nlink) != 1:
                raise RawArtifactIntegrityError(
                    "raw-artifact incomplete crash-stage heeft onbekende hardlink"
                )
            self._reprove_file_publication_directories(directory_identities)
            self._unlink_owned_file_stage(
                stage_path,
                expected_identity=self._stage_identity(info),
                allowed_link_counts=(
                    frozenset({1, 2})
                    if target_exists or actual_sha256 == content_sha256
                    else frozenset({1})
                ),
            )
            directory_identities = self._file_publication_directory_identities()
        self._storage_usage_cache = None
        return recovered

    def initialize_reclamation_extension_v1(self) -> str:
        """Atomically opt an exact raw-v3 catalogue into reclamation v1.

        Ordinary :meth:`initialize` never performs this migration.  That keeps
        legacy append-only stores byte/profile compatible and ensures a crash
        can expose only the complete extension table set or none of it.
        """

        self._require_initialized()
        with self._lock, closing(self._connect()) as database:
            database.execute("BEGIN IMMEDIATE")
            try:
                tables = self._table_names(database)
                if tables == self._TABLES:
                    self._validate_base_schema(database)
                    nonce = secrets.token_hex(32)
                    profile_sha256 = hashlib.sha256(
                        self._store_profile_bytes()
                    ).hexdigest()
                    identity_sha256 = self._reclamation_store_identity(
                        nonce=nonce,
                        profile_sha256=profile_sha256,
                    )
                    self._create_reclamation_extension_schema(
                        database,
                        nonce=nonce,
                        profile_sha256=profile_sha256,
                        identity_sha256=identity_sha256,
                        created_at=time.time(),
                    )
                elif tables == self._TABLES | self._RECLAMATION_TABLES:
                    identity_sha256 = self._load_reclamation_store_identity(
                        database
                    )
                else:
                    raise RawArtifactIntegrityError(
                        "raw-artifactcatalogus heeft onbekend of onvolledig schema"
                    )
                self._validate_schema(database)
                database.commit()
            except sqlite3.OperationalError as exc:
                if database.in_transaction:
                    database.rollback()
                if self._is_sqlite_capacity_error(exc):
                    raise self._catalog_capacity_error() from exc
                raise
            except BaseException:
                if database.in_transaction:
                    database.rollback()
                raise
        with closing(self._connect()) as database:
            self._validate_schema(database)
            return self._load_reclamation_store_identity(database)

    @property
    def reclamation_extension_enabled(self) -> bool:
        """Return whether the complete known extension is present."""

        self._require_initialized()
        with self._lock, closing(self._connect()) as database:
            tables = self._table_names(database)
            if tables == self._TABLES:
                return False
            self._validate_schema(database)
            return True

    @property
    def source_store_identity_sha256(self) -> str:
        """Return the persistent protocol identity of this raw-store instance."""

        self._require_initialized()
        with self._lock, closing(self._connect()) as database:
            self._require_reclamation_extension(database)
            return self._load_reclamation_store_identity(database)

    @property
    def role(self) -> str:
        return "raw_artifacts"

    @property
    def coordinator_authority_sha256(self) -> str:
        authority = self._require_committed_decision_authority()
        return authority.authority_sha256

    @property
    def candidate_authority_sha256(self) -> str:
        authority = self._require_candidate_authority()
        return authority.authority_sha256

    @property
    def reclamation_directory_sync_available(self) -> bool:
        """Whether positive raw unlink can hold a strict directory barrier."""

        return (
            self._reclamation_directory_fsync_authority is not None
            or os.name != "nt"
        )

    def load_source_local_prepare_v2(
        self,
        request_sha256: str,
    ) -> SourceLocalPrepareReceiptV2 | None:
        self._require_initialized()
        request_sha256 = require_reclamation_sha256(
            request_sha256,
            "raw prepare request SHA-256",
        )
        authority_pair = self._reclamation_authority_pair_snapshot()
        with self._lock, closing(self._connect()) as database:
            self._require_reclamation_extension(database)
            receipt = self._load_reclamation_prepare(database, request_sha256)
            if receipt is not None:
                self._assert_reclamation_prepare_authority_pair(
                    receipt,
                    authority_pair=authority_pair,
                )
            return receipt

    def load_source_local_apply_v1(
        self,
        request_sha256: str,
    ) -> SourceLocalApplyReceiptV1 | None:
        self._require_initialized()
        request_sha256 = require_reclamation_sha256(
            request_sha256,
            "raw apply request SHA-256",
        )
        authority_pair = self._reclamation_authority_pair_snapshot()
        with self._lock, closing(self._connect()) as database:
            self._require_reclamation_extension(database)
            receipt = self._load_reclamation_apply(database, request_sha256)
            prepared = self._load_reclamation_prepare(database, request_sha256)
            if receipt is not None:
                if prepared is None:
                    raise RawArtifactIntegrityError(
                        "raw apply mist zijn lokale prepare"
                    )
                self._assert_reclamation_prepare_authority_pair(
                    prepared,
                    authority_pair=authority_pair,
                )
            return receipt

    def load_source_local_abort_v1(
        self,
        request_sha256: str,
    ) -> SourceLocalAbortReceiptV1 | None:
        self._require_initialized()
        request_sha256 = require_reclamation_sha256(
            request_sha256,
            "raw abort request SHA-256",
        )
        authority_pair = self._reclamation_authority_pair_snapshot()
        with self._lock, closing(self._connect()) as database:
            self._require_reclamation_extension(database)
            receipt = self._load_reclamation_abort(database, request_sha256)
            prepared = self._load_reclamation_prepare(database, request_sha256)
            if receipt is not None:
                if prepared is None:
                    raise RawArtifactIntegrityError(
                        "raw abort mist zijn lokale prepare"
                    )
                self._assert_reclamation_prepare_authority_pair(
                    prepared,
                    authority_pair=authority_pair,
                )
            return receipt

    def load_raw_unlink_journal_v1(
        self,
        request_sha256: str,
    ) -> RawUnlinkJournalReceiptV1 | None:
        self._require_initialized()
        request_sha256 = require_reclamation_sha256(
            request_sha256,
            "raw unlink journal request SHA-256",
        )
        authority_pair = self._reclamation_authority_pair_snapshot()
        with self._lock, closing(self._connect()) as database:
            self._require_reclamation_extension(database)
            receipt = self._load_reclamation_journal(database, request_sha256)
            prepared = self._load_reclamation_prepare(database, request_sha256)
            if receipt is not None:
                if prepared is None:
                    raise RawArtifactIntegrityError(
                        "raw unlinkjournal mist zijn lokale prepare"
                    )
                self._assert_reclamation_prepare_authority_pair(
                    prepared,
                    authority_pair=authority_pair,
                )
            return receipt

    def prepare_source_local_v2(
        self,
        request: SourceReclamationRequestV2,
    ) -> SourceLocalPrepareReceiptV2:
        """Fence one exact sealed candidate inventory in the local raw store."""

        self._require_initialized()
        if not isinstance(request, SourceReclamationRequestV2):
            raise TypeError("raw reclamation request heeft een ongeldig type")
        authority_pair = self._reclamation_authority_pair_snapshot()
        if request.candidate_authority_sha256 != authority_pair.candidate_sha256:
            raise RawArtifactConflictError(
                "raw reclamation request bindt een andere candidate authority"
            )
        request_json = self._protocol_json(request.as_dict())

        # A completed or already fenced request remains a pure idempotent
        # lookup.  In particular, an apply retry may arrive after its base
        # captures have deliberately disappeared.
        with self._lock, closing(self._connect()) as database:
            self._require_reclamation_extension(database)
            existing = database.execute(
                "SELECT request_json FROM raw_artifact_reclamation_prepares "
                "WHERE request_sha256=?",
                (request.request_sha256,),
            ).fetchone()
            if existing is not None:
                if str(existing["request_json"]) != request_json:
                    raise RawArtifactConflictError(
                        "raw reclamation request-SHA werd voor andere facts gebruikt"
                    )
                receipt = self._load_reclamation_prepare(
                    database,
                    request.request_sha256,
                )
                if receipt is None:  # pragma: no cover - same transaction
                    raise RawArtifactIntegrityError(
                        "raw reclamation preparereceipt ontbreekt"
                    )
                self._assert_reclamation_prepare_authority_pair(
                    receipt,
                    authority_pair=authority_pair,
                )
                return receipt

        with ExitStack() as candidate_holds:
            evidence = self._hold_reclamation_candidate_evidence(
                request,
                candidate_holds,
            )
            with self._lock, closing(self._connect()) as database:
                database.execute("BEGIN IMMEDIATE")
                try:
                    self._require_reclamation_extension(database)
                    existing = database.execute(
                        "SELECT request_json FROM "
                        "raw_artifact_reclamation_prepares "
                        "WHERE request_sha256=?",
                        (request.request_sha256,),
                    ).fetchone()
                    if existing is not None:
                        if str(existing["request_json"]) != request_json:
                            raise RawArtifactConflictError(
                                "raw reclamation request-SHA werd voor andere facts gebruikt"
                            )
                        receipt = self._load_reclamation_prepare(
                            database,
                            request.request_sha256,
                        )
                        if receipt is None:  # pragma: no cover - same transaction
                            raise RawArtifactIntegrityError(
                                "raw reclamation preparereceipt ontbreekt"
                            )
                        self._assert_reclamation_prepare_authority_pair(
                            receipt,
                            authority_pair=authority_pair,
                        )
                        database.commit()
                        return receipt

                    store_identity = self._load_reclamation_store_identity(
                        database
                    )
                    plan = self._build_reclamation_prepare_plan(
                        database,
                        request=request,
                        evidence=evidence,
                        store_identity_sha256=store_identity,
                    )
                    prepared_at = self._reclamation_now()
                    receipt = SourceLocalPrepareReceiptV2.create(
                        request_sha256=request.request_sha256,
                        role=self.role,
                        source_store_identity_sha256=store_identity,
                        coordinator_authority_sha256=(
                            authority_pair.decision_sha256
                        ),
                        candidate_authority_sha256=(
                            authority_pair.candidate_sha256
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
                        source_view_sha256=plan.source_view_sha256,
                        child_reference_root_sha256=(
                            plan.child_reference_root_sha256
                        ),
                        source_fence_sha256=plan.source_fence_sha256,
                        candidate_count=request.candidate_count,
                        releasable_bytes=plan.releasable_bytes,
                        releasable_entries=plan.releasable_entries,
                        prepared_at=prepared_at,
                    )
                    database.execute(
                        """
                        INSERT INTO raw_artifact_reclamation_prepares(
                            request_sha256, request_json,
                            prepare_receipt_sha256, prepare_receipt_json,
                            source_fence_sha256,
                            child_reference_root_sha256,
                            candidate_bindings_sha256,
                            candidate_authority_sha256,
                            coordinator_authority_sha256,
                            source_view_sha256, candidate_count,
                            releasable_bytes, releasable_entries, prepared_at
                        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            request.request_sha256,
                            request_json,
                            receipt.prepare_receipt_sha256,
                            self._protocol_json(receipt.as_dict()),
                            receipt.source_fence_sha256,
                            receipt.child_reference_root_sha256,
                            receipt.candidate_bindings_sha256,
                            receipt.candidate_authority_sha256,
                            receipt.coordinator_authority_sha256,
                            receipt.source_view_sha256,
                            receipt.candidate_count,
                            receipt.releasable_bytes,
                            receipt.releasable_entries,
                            receipt.prepared_at,
                        ),
                    )
                    self._insert_reclamation_mapping_rows(
                        database,
                        table="raw_artifact_reclamation_candidates",
                        rows=plan.candidate_rows,
                    )
                    self._insert_reclamation_mapping_rows(
                        database,
                        table="raw_artifact_reclamation_capture_members",
                        rows=plan.member_rows,
                    )
                    self._insert_reclamation_mapping_rows(
                        database,
                        table="raw_artifact_reclamation_receipt_references",
                        rows=plan.receipt_rows,
                    )
                    self._insert_reclamation_mapping_rows(
                        database,
                        table="raw_artifact_reclamation_object_references",
                        rows=plan.object_rows,
                    )
                    self._checkpoint("reclamation_prepare_before_commit")
                    database.commit()
                except sqlite3.OperationalError as exc:
                    if database.in_transaction:
                        database.rollback()
                    if self._is_sqlite_capacity_error(exc):
                        raise self._catalog_capacity_error() from exc
                    raise
                except sqlite3.IntegrityError as exc:
                    if database.in_transaction:
                        database.rollback()
                    raise RawArtifactConflictError(
                        "raw reclamation prepare kon zijn fence niet atomair vastleggen"
                    ) from exc
                except BaseException:
                    if database.in_transaction:
                        database.rollback()
                    raise
        self._checkpoint("reclamation_prepare_after_commit")
        return receipt

    def abort_source_local_v1(
        self,
        request_sha256: str,
        *,
        expected_abort_decision_sha256: str,
    ) -> SourceLocalAbortReceiptV1:
        """Release a local prepare fence only under held physical ABORTING."""

        self._require_initialized()
        request_sha256 = require_reclamation_sha256(
            request_sha256,
            "raw abort request SHA-256",
        )
        expected_abort_decision_sha256 = require_reclamation_sha256(
            expected_abort_decision_sha256,
            "raw expected abort decision SHA-256",
        )
        authority_pair = self._reclamation_authority_pair_snapshot()
        decision_authority = authority_pair.decision
        with self._lock, closing(self._connect()) as database:
            self._require_reclamation_extension(database)
            prepared = self._load_reclamation_prepare(
                database,
                request_sha256,
            )
            if prepared is None:
                raise RawArtifactConflictError(
                    "raw reclamation abort mist een lokale prepare"
                )
            self._assert_reclamation_prepare_authority_pair(
                prepared,
                authority_pair=authority_pair,
            )

        with decision_authority.hold_abort_decision_v1(
            request_sha256,
            expected_abort_decision_sha256=expected_abort_decision_sha256,
            aborting_role=self.role,
            expected_prepare_receipt_sha256=prepared.prepare_receipt_sha256,
            expected_source_store_identity_sha256=(
                prepared.source_store_identity_sha256
            ),
            expected_source_fence_sha256=prepared.source_fence_sha256,
        ) as decision:
            if not isinstance(decision, CoordinatorAbortDecisionReceiptV1):
                raise RawArtifactIntegrityError(
                    "raw reclamation abort-authority gaf een verkeerd receipt"
                )
            if (
                decision.request_sha256 != request_sha256
                or decision.abort_decision_sha256
                != expected_abort_decision_sha256
            ):
                raise RawArtifactConflictError(
                    "held raw abortbesluit bindt andere requestfacts"
                )
            with self._lock, closing(self._connect()) as database:
                database.execute("BEGIN IMMEDIATE")
                try:
                    self._require_reclamation_extension(database)
                    current = self._load_reclamation_prepare(
                        database,
                        request_sha256,
                    )
                    if current != prepared:
                        raise RawArtifactConflictError(
                            "raw reclamation prepare veranderde vóór abort"
                        )
                    if database.execute(
                        "SELECT 1 FROM raw_artifact_reclamation_catalog_applies "
                        "WHERE request_sha256=?",
                        (request_sha256,),
                    ).fetchone() is not None:
                        raise RawArtifactConflictError(
                            "raw FILES_PENDING kan niet meer aborteren"
                        )
                    existing = self._load_reclamation_abort(
                        database,
                        request_sha256,
                    )
                    if existing is not None:
                        row = database.execute(
                            "SELECT coordinator_abort_decision_sha256,"
                            "coordinator_abort_receipt_sha256 FROM "
                            "raw_artifact_reclamation_aborts "
                            "WHERE request_sha256=?",
                            (request_sha256,),
                        ).fetchone()
                        if (
                            row is None
                            or str(row["coordinator_abort_decision_sha256"])
                            != decision.abort_decision_sha256
                            or str(row["coordinator_abort_receipt_sha256"])
                            != decision.receipt_sha256
                        ):
                            raise RawArtifactConflictError(
                                "raw abortretry bindt een ander fysiek besluit"
                            )
                        database.commit()
                        return existing

                    aborted_at = self._reclamation_now()
                    fence_release_root_sha256 = reclamation_digest_mapping({
                        "coordinator_abort_decision_sha256": (
                            decision.abort_decision_sha256
                        ),
                        "coordinator_abort_receipt_sha256": (
                            decision.receipt_sha256
                        ),
                        "participant_prepare_root_sha256": (
                            decision.participant_prepare_root_sha256
                        ),
                        "prepare_receipt_sha256": (
                            prepared.prepare_receipt_sha256
                        ),
                        "request_sha256": request_sha256,
                        "schema": (
                            "stv3-worker-raw-reclamation-fence-release-v1"
                        ),
                        "source_fence_sha256": prepared.source_fence_sha256,
                        "source_store_identity_sha256": (
                            prepared.source_store_identity_sha256
                        ),
                    })
                    receipt = SourceLocalAbortReceiptV1.create(
                        request_sha256=request_sha256,
                        role=self.role,
                        source_store_identity_sha256=(
                            prepared.source_store_identity_sha256
                        ),
                        prepare_receipt_sha256=(
                            prepared.prepare_receipt_sha256
                        ),
                        coordinator_abort_decision_sha256=(
                            decision.abort_decision_sha256
                        ),
                        fence_release_root_sha256=fence_release_root_sha256,
                        aborted_at=aborted_at,
                    )
                    database.execute(
                        """
                        INSERT INTO raw_artifact_reclamation_aborts(
                            request_sha256, abort_receipt_sha256,
                            abort_receipt_json,
                            coordinator_abort_decision_sha256,
                            coordinator_abort_receipt_sha256,
                            coordinator_abort_receipt_json,
                            fence_release_root_sha256, aborted_at, state
                        ) VALUES(?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            request_sha256,
                            receipt.abort_receipt_sha256,
                            self._protocol_json(receipt.as_dict()),
                            decision.abort_decision_sha256,
                            decision.receipt_sha256,
                            self._protocol_json(decision.as_dict()),
                            receipt.fence_release_root_sha256,
                            receipt.aborted_at,
                            RAW_RECLAMATION_ABORTED,
                        ),
                    )
                    self._checkpoint("reclamation_abort_before_commit")
                    database.commit()
                except sqlite3.OperationalError as exc:
                    if database.in_transaction:
                        database.rollback()
                    if self._is_sqlite_capacity_error(exc):
                        raise self._catalog_capacity_error() from exc
                    raise
                except sqlite3.IntegrityError as exc:
                    if database.in_transaction:
                        database.rollback()
                    raise RawArtifactConflictError(
                        "raw reclamation abort kon zijn fence niet vrijgeven"
                    ) from exc
                except BaseException:
                    if database.in_transaction:
                        database.rollback()
                    raise
        self._checkpoint("reclamation_abort_after_commit")
        return receipt

    def apply_source_local_v1(
        self,
        request_sha256: str,
        *,
        predecessor_apply_receipt_sha256: str | None,
    ) -> SourceLocalApplyReceiptV1:
        """Apply raw reclamation through its durable catalog/unlink journal."""

        self._require_initialized()
        request_sha256 = require_reclamation_sha256(
            request_sha256,
            "raw apply request SHA-256",
        )
        predecessor_apply_receipt_sha256 = require_reclamation_sha256(
            predecessor_apply_receipt_sha256,
            "raw predecessor apply receipt SHA-256",
        )
        authority_pair = self._reclamation_authority_pair_snapshot()
        decision_authority = authority_pair.decision
        with self._lock, closing(self._connect()) as database:
            self._require_reclamation_extension(database)
            prepared = self._load_reclamation_prepare(
                database,
                request_sha256,
            )
            if prepared is None:
                raise RawArtifactConflictError(
                    "raw reclamation apply mist een lokale prepare"
                )
            if self._load_reclamation_abort(database, request_sha256) is not None:
                raise RawArtifactConflictError(
                    "raw reclamation apply kan geen aborted fence hergebruiken"
                )
            request = self._load_reclamation_request(
                database,
                request_sha256,
            )
            self._assert_reclamation_prepare_authority_pair(
                prepared,
                authority_pair=authority_pair,
            )
            if (
                request.candidate_authority_sha256
                != authority_pair.candidate_sha256
            ):
                raise RawArtifactConflictError(
                    "raw reclamation prepare bindt andere instance-authority"
                )

        with decision_authority.hold_committed_decision_v1(
            request_sha256,
            applying_role=self.role,
            expected_predecessor_apply_receipt_sha256=(
                predecessor_apply_receipt_sha256
            ),
        ) as decision:
            self._validate_reclamation_committed_decision(
                request=request,
                prepared=prepared,
                decision=decision,
            )
            with self._lock, closing(self._connect()) as database:
                self._require_reclamation_extension(database)
                applied = self._load_reclamation_apply(
                    database,
                    request_sha256,
                )
                if applied is not None:
                    catalog = self._load_reclamation_catalog_apply(
                        database,
                        request_sha256=request_sha256,
                    )
                    self._validate_reclamation_catalog_decision(
                        catalog=catalog,
                        decision=decision,
                        predecessor_apply_receipt_sha256=(
                            predecessor_apply_receipt_sha256
                        ),
                    )
                    return applied

            # A positive raw apply always requires an explicit, constructor-
            # bound directory durability authority.  On Windows the default
            # is deliberately unavailable, even for a zero-object plan.
            if not self.reclamation_directory_sync_available:
                raise RawArtifactIntegrityError(
                    "raw reclamation mist geverifieerde directory-syncauthority"
                )
            self._ensure_reclamation_catalog_apply(
                request=request,
                prepared=prepared,
                decision=decision,
                predecessor_apply_receipt_sha256=(
                    predecessor_apply_receipt_sha256
                ),
            )
            receipt = self._resume_reclamation_unlink_journal(
                request=request,
                prepared=prepared,
                decision=decision,
                predecessor_apply_receipt_sha256=(
                    predecessor_apply_receipt_sha256
                ),
            )
        return receipt

    def resume_journaled_raw_prune_v1(
        self,
        request_sha256: str,
    ) -> SourceLocalApplyReceiptV1:
        """Resume one immutable FILES_PENDING journal after a process crash."""

        self._require_initialized()
        request_sha256 = require_reclamation_sha256(
            request_sha256,
            "raw resume request SHA-256",
        )
        self._require_reclamation_authority_pair()
        with self._lock, closing(self._connect()) as database:
            self._require_reclamation_extension(database)
            row = database.execute(
                "SELECT predecessor_apply_receipt_sha256 FROM "
                "raw_artifact_reclamation_catalog_applies "
                "WHERE request_sha256=?",
                (request_sha256,),
            ).fetchone()
            if row is None:
                raise RawArtifactConflictError(
                    "raw reclamation resume mist een FILES_PENDING journal"
                )
            predecessor = require_reclamation_sha256(
                str(row["predecessor_apply_receipt_sha256"]),
                "raw journal predecessor apply receipt SHA-256",
            )
        return self.apply_source_local_v1(
            request_sha256,
            predecessor_apply_receipt_sha256=predecessor,
        )

    def store(
        self,
        *,
        provider: str,
        product: str,
        fetch_id: str,
        content_type: str,
        received_at: float,
        upstream_id: str,
        provenance: Mapping[str, Any],
        payload: bytes,
    ) -> RawArtifactReceipt:
        """Store exact bytes and commit their complete provenance atomically."""
        return self._store_input(
            provider=provider,
            product=product,
            fetch_id=fetch_id,
            content_type=content_type,
            received_at=received_at,
            upstream_id=upstream_id,
            provenance=provenance,
            payload=payload,
            source_authority=None,
            expected_size=None,
            expected_sha256=None,
        )

    def store_file(
        self,
        *,
        provider: str,
        product: str,
        fetch_id: str,
        content_type: str,
        received_at: float,
        upstream_id: str,
        provenance: Mapping[str, Any],
        source_path: str | Path,
        expected_size: int,
        expected_sha256: str,
    ) -> RawArtifactReceipt:
        """Publish one exact regular file without collecting its payload bytes.

        The caller supplies the size and SHA-256 proven while downloading.  A
        no-follow descriptor pins the same single-link source identity while
        this store independently streams and hashes it into a deterministic,
        crash-resumable staging file.  The source remains caller-owned.
        """

        self._require_initialized()
        if (
            isinstance(expected_size, bool)
            or not isinstance(expected_size, int)
            or expected_size < 0
        ):
            raise ValueError("raw-artifactbronbestandgrootte is ongeldig")
        if expected_size > self.limits.max_object_bytes:
            raise RawArtifactCapacityError(
                scope="object",
                current_bytes=0,
                attempted_bytes=expected_size,
                limit_bytes=self.limits.max_object_bytes,
            )
        if (
            not isinstance(expected_sha256, str)
            or _SHA256_RE.fullmatch(expected_sha256) is None
        ):
            raise ValueError("raw-artifactbronbestand SHA-256 is ongeldig")
        with self._hold_external_file_authority(
            source_path,
            expected_size=expected_size,
        ) as authority:
            return self._store_input(
                provider=provider,
                product=product,
                fetch_id=fetch_id,
                content_type=content_type,
                received_at=received_at,
                upstream_id=upstream_id,
                provenance=provenance,
                payload=None,
                source_authority=authority,
                expected_size=expected_size,
                expected_sha256=expected_sha256,
            )

    def _store_input(
        self,
        *,
        provider: str,
        product: str,
        fetch_id: str,
        content_type: str,
        received_at: float,
        upstream_id: str,
        provenance: Mapping[str, Any],
        payload: bytes | None,
        source_authority: _ExternalFileAuthority | None,
        expected_size: int | None,
        expected_sha256: str | None,
    ) -> RawArtifactReceipt:
        """Shared transactional path for in-memory and file-backed inputs."""

        self._require_initialized()
        provider = self._identifier(provider, "provider", 128)
        product = self._identifier(product, "product", 256)
        fetch_id = self._identifier(fetch_id, "fetch_id", 512)
        content_type = self._identifier(content_type, "content_type", 256)
        upstream_id = self._identifier(upstream_id, "upstream_id", 1024)
        received_at = self._timestamp(received_at, "received_at")
        if source_authority is None:
            if not isinstance(payload, bytes):
                raise TypeError("raw-artifactpayload moet exact bytes zijn")
            if expected_size is not None or expected_sha256 is not None:
                raise RawArtifactIntegrityError(
                    "raw-artifact geheugeninput bevat file-only verwachtingen"
                )
            size_bytes = len(payload)
            content_sha256 = hashlib.sha256(payload).hexdigest()
        else:
            if payload is not None:
                raise RawArtifactIntegrityError(
                    "raw-artifactinput mag niet tegelijk bytes en een bestand zijn"
                )
            if (
                expected_size is None
                or expected_sha256 is None
                or _SHA256_RE.fullmatch(expected_sha256) is None
            ):
                raise RawArtifactIntegrityError(
                    "raw-artifactbestand mist exacte grootte of SHA-256"
                )
            size_bytes = expected_size
            content_sha256 = expected_sha256
        if size_bytes > self.limits.max_object_bytes:
            raise RawArtifactCapacityError(
                scope="object",
                current_bytes=0,
                attempted_bytes=size_bytes,
                limit_bytes=self.limits.max_object_bytes,
            )
        provenance_json, provenance_sha256 = self._provenance(provenance)
        receipt_sha256 = self._receipt_sha256(
            provider=provider,
            product=product,
            fetch_id=fetch_id,
            content_type=content_type,
            received_at=received_at,
            upstream_id=upstream_id,
            content_sha256=content_sha256,
            size_bytes=size_bytes,
            provenance_sha256=provenance_sha256,
        )
        receipt_id = self._receipt_id(
            provider=provider,
            product=product,
            fetch_id=fetch_id,
        )
        relative_path = self._relative_object_path(content_sha256)
        target = self._safe_object_path(relative_path, content_sha256)
        file_stage_path = (
            self._file_stage_path(content_sha256)
            if source_authority is not None
            else None
        )

        with self._lock, closing(self._connect()) as database:
            database.execute("BEGIN IMMEDIATE")
            try:
                self._checkpoint("store_after_begin")
                # Directory timestamps are only a telemetry-cache hint.  An
                # external writer can restore them after an in-place change,
                # so admission always performs a complete physical scan.  It
                # runs under the catalog writer transaction to serialize all
                # cooperating store processes around accounting/publication.
                storage_usage = self._scan_storage(force=True)
                file_stage_capacity_credit = (
                    self._file_stage_capacity_credit(
                        file_stage_path,
                        target,
                    )
                    if file_stage_path is not None
                    else 0
                )
                admitted_filesystem_bytes = (
                    storage_usage.total_bytes - file_stage_capacity_credit
                )
                if (
                    admitted_filesystem_bytes < 0
                    or admitted_filesystem_bytes > self.limits.max_total_bytes
                ):
                    raise RawArtifactCapacityError(
                        scope="total",
                        current_bytes=max(0, admitted_filesystem_bytes),
                        attempted_bytes=0,
                        limit_bytes=self.limits.max_total_bytes,
                    )
                existing_row = database.execute(
                    self._RECEIPT_SELECT + " WHERE receipt.receipt_id=?",
                    (receipt_id,),
                ).fetchone()
                if existing_row is not None:
                    existing = self._validated_receipt(existing_row)
                    if (
                        existing.provider != provider
                        or existing.product != product
                        or existing.fetch_id != fetch_id
                        or existing.content_type != content_type
                        or existing.upstream_id != upstream_id
                        or existing.content_sha256 != content_sha256
                        or existing.size_bytes != size_bytes
                        or existing.provenance_sha256 != provenance_sha256
                    ):
                        raise RawArtifactConflictError(
                            "fetch-id werd al gebruikt voor andere bytes of provenance"
                        )
                    if source_authority is not None:
                        self._verify_external_file_authority(
                            source_authority,
                            expected_size=size_bytes,
                            expected_sha256=content_sha256,
                        )
                    self._read_verified(
                        Path(existing.artifact_path),
                        expected_size=existing.size_bytes,
                        expected_sha256=existing.content_sha256,
                        return_bytes=False,
                    )
                    database.commit()
                    if file_stage_path is not None:
                        self._cleanup_completed_file_stage(
                            file_stage_path,
                            Path(existing.artifact_path),
                            expected_size=existing.size_bytes,
                            expected_sha256=existing.content_sha256,
                        )
                    return existing

                # Once a prepare has frozen exact receipt/object refcounts, a
                # new receipt for any referenced content would invalidate that
                # immutable plan.  Check before publishing bytes: the schema
                # triggers remain the transaction-race backstop, while this
                # preflight prevents a rejected tombstone/fence retry from
                # leaving an uncatalogued object behind on disk.
                if "raw_artifact_reclamation_metadata" in self._table_names(
                    database
                ):
                    fenced = database.execute(
                        """
                        SELECT 1
                        FROM raw_artifact_reclamation_receipt_tombstones
                        WHERE receipt_id=?
                           OR (
                               provider=? AND product=? AND
                               (fetch_id=? OR upstream_id=?)
                           )
                        UNION ALL
                        SELECT 1
                        FROM raw_artifact_reclamation_object_references AS reference
                        LEFT JOIN raw_artifact_reclamation_aborts AS aborted
                          ON aborted.request_sha256=reference.request_sha256
                        LEFT JOIN raw_artifact_reclamation_applies AS applied
                          ON applied.request_sha256=reference.request_sha256
                        WHERE reference.content_sha256=?
                          AND aborted.request_sha256 IS NULL
                          AND applied.request_sha256 IS NULL
                        LIMIT 1
                        """,
                        (
                            receipt_id,
                            provider,
                            product,
                            fetch_id,
                            upstream_id,
                            content_sha256,
                        ),
                    ).fetchone()
                    if fenced is not None:
                        raise RawArtifactConflictError(
                            "raw-artifactwrite botst met een reclamation "
                            "fence of tombstone"
                        )

                receipt_count = int(database.execute(
                    "SELECT count(*) FROM raw_artifact_receipts"
                ).fetchone()[0])
                if receipt_count >= self.limits.max_receipt_entries:
                    raise RawArtifactEntryCapacityError(
                        scope="receipts",
                        current_entries=receipt_count,
                        attempted_entries=1,
                        limit_entries=self.limits.max_receipt_entries,
                    )

                identity_rows = database.execute(
                    """
                    SELECT DISTINCT content_sha256
                    FROM raw_artifact_receipts
                    WHERE provider=? AND product=? AND upstream_id=?
                    """,
                    (provider, product, upstream_id),
                ).fetchall()
                if any(
                    str(row["content_sha256"]) != content_sha256
                    for row in identity_rows
                ):
                    raise RawArtifactConflictError(
                        "stabiele upstream-id verwijst al naar andere bytes"
                    )

                filesystem_bytes = storage_usage.total_bytes
                object_row = database.execute(
                    """
                    SELECT content_sha256, relative_path, size_bytes
                    FROM raw_artifact_objects WHERE content_sha256=?
                    """,
                    (content_sha256,),
                ).fetchone()
                if object_row is not None:
                    if (
                        object_row["relative_path"] != relative_path
                        or int(object_row["size_bytes"]) != size_bytes
                    ):
                        raise RawArtifactIntegrityError(
                            "raw-artifactobjectcatalogus is inconsistent"
                        )
                    if source_authority is not None:
                        self._verify_external_file_authority(
                            source_authority,
                            expected_size=size_bytes,
                            expected_sha256=content_sha256,
                        )
                    self._read_verified(
                        target,
                        expected_size=size_bytes,
                        expected_sha256=content_sha256,
                        return_bytes=False,
                    )
                else:
                    target_present = target.exists() or target.is_symlink()
                    if not target_present:
                        resumable_stage_bytes = 0
                        resumable_stage_present = False
                        resumable_stage_identity: (
                            tuple[int, int, int, int] | None
                        ) = None
                        if file_stage_path is not None:
                            stage_info = self._resumable_file_stage_info(
                                file_stage_path,
                            )
                            if stage_info is not None:
                                resumable_stage_present = True
                                resumable_stage_bytes = int(stage_info.st_size)
                                resumable_stage_identity = (
                                    int(stage_info.st_dev),
                                    int(stage_info.st_ino),
                                    int(stage_info.st_mode),
                                    int(stage_info.st_size),
                                )
                        if storage_usage.object_entries >= self.limits.max_object_entries:
                            raise RawArtifactEntryCapacityError(
                                scope="objects",
                                current_entries=storage_usage.object_entries,
                                attempted_entries=1,
                                limit_entries=self.limits.max_object_entries,
                            )
                        if (
                            not resumable_stage_present
                            and storage_usage.staging_entries
                            >= self.limits.max_staging_entries
                        ):
                            raise RawArtifactEntryCapacityError(
                                scope="staging",
                                current_entries=storage_usage.staging_entries,
                                attempted_entries=1,
                                limit_entries=self.limits.max_staging_entries,
                            )
                        effective_filesystem_bytes = (
                            filesystem_bytes - resumable_stage_bytes
                        )
                        if (
                            effective_filesystem_bytes < 0
                            or effective_filesystem_bytes + size_bytes
                            > self.limits.max_total_bytes
                        ):
                            raise RawArtifactCapacityError(
                                scope="total",
                                current_bytes=max(0, effective_filesystem_bytes),
                                attempted_bytes=size_bytes,
                                limit_bytes=self.limits.max_total_bytes,
                            )
                        if source_authority is None:
                            assert payload is not None
                            self._publish_exact(
                                payload,
                                target,
                                expected_size=size_bytes,
                                expected_sha256=content_sha256,
                                prior_usage=storage_usage,
                            )
                        else:
                            assert file_stage_path is not None
                            self._publish_exact_file(
                                source_authority,
                                file_stage_path,
                                target,
                                expected_size=size_bytes,
                                expected_sha256=content_sha256,
                                expected_stage_identity=(
                                    resumable_stage_identity
                                ),
                            )
                            post_publish_usage = self._scan_storage(force=True)
                            if (
                                post_publish_usage.total_bytes
                                > self.limits.max_total_bytes
                            ):
                                raise RawArtifactCapacityError(
                                    scope="total",
                                    current_bytes=post_publish_usage.total_bytes,
                                    attempted_bytes=0,
                                    limit_bytes=self.limits.max_total_bytes,
                                )
                            if (
                                post_publish_usage.object_entries
                                > self.limits.max_object_entries
                            ):
                                raise RawArtifactEntryCapacityError(
                                    scope="objects",
                                    current_entries=(
                                        post_publish_usage.object_entries
                                    ),
                                    attempted_entries=0,
                                    limit_entries=self.limits.max_object_entries,
                                )
                            if (
                                post_publish_usage.staging_entries
                                > self.limits.max_staging_entries
                            ):
                                raise RawArtifactEntryCapacityError(
                                    scope="staging",
                                    current_entries=(
                                        post_publish_usage.staging_entries
                                    ),
                                    attempted_entries=0,
                                    limit_entries=self.limits.max_staging_entries,
                                )
                    else:
                        if source_authority is not None:
                            self._verify_external_file_authority(
                                source_authority,
                                expected_size=size_bytes,
                                expected_sha256=content_sha256,
                            )
                        self._read_verified(
                            target,
                            expected_size=size_bytes,
                            expected_sha256=content_sha256,
                            return_bytes=False,
                        )
                    self._checkpoint("store_after_object_publish")
                    database.execute(
                        """
                        INSERT INTO raw_artifact_objects(
                            content_sha256, relative_path, size_bytes, created_at
                        ) VALUES(?,?,?,?)
                        """,
                        (content_sha256, relative_path, size_bytes, time.time()),
                    )

                self._checkpoint("store_before_receipt")
                created_at = time.time()
                database.execute(
                    """
                    INSERT INTO raw_artifact_receipts(
                        receipt_id, provider, product, fetch_id, upstream_id,
                        content_type, received_at, content_sha256, size_bytes,
                        provenance_json, provenance_sha256, receipt_sha256,
                        created_at
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        receipt_id, provider, product, fetch_id, upstream_id,
                        content_type, received_at, content_sha256, size_bytes,
                        provenance_json, provenance_sha256, receipt_sha256,
                        created_at,
                    ),
                )
                self._read_verified(
                    target,
                    expected_size=size_bytes,
                    expected_sha256=content_sha256,
                    return_bytes=False,
                )
                self._checkpoint("store_before_commit")
                database.commit()
            except sqlite3.IntegrityError as exc:
                if database.in_transaction:
                    database.rollback()
                raise RawArtifactConflictError(
                    "raw-artifactwrite botst met een reclamation fence of tombstone"
                ) from exc
            except sqlite3.OperationalError as exc:
                if database.in_transaction:
                    database.rollback()
                if self._is_sqlite_capacity_error(exc):
                    raise self._catalog_capacity_error() from exc
                raise
            except BaseException:
                if database.in_transaction:
                    database.rollback()
                raise
        self._checkpoint("store_after_commit")
        if file_stage_path is not None:
            self._cleanup_completed_file_stage(
                file_stage_path,
                target,
                expected_size=size_bytes,
                expected_sha256=content_sha256,
            )
        # The object and the exact receipt facts were verified inside the
        # committing transaction. Reopening the receipt here used to hash the
        # same potentially large object once more without adding a new crash
        # boundary. A retry after a lost return still uses ``load_receipt()``
        # and therefore performs the full authority verification.
        return RawArtifactReceipt(
            receipt_id=receipt_id,
            artifact_path=str(target),
            content_sha256=content_sha256,
            size_bytes=size_bytes,
            provider=provider,
            product=product,
            fetch_id=fetch_id,
            content_type=content_type,
            received_at=received_at,
            upstream_id=upstream_id,
            provenance_sha256=provenance_sha256,
            receipt_sha256=receipt_sha256,
            created_at=created_at,
            _provenance_json=provenance_json,
        )

    def commit_capture(
        self,
        *,
        provider: str,
        product: str,
        fetch_id: str,
        upstream_capture_id: str,
        product_timestamp: float | None,
        members: Mapping[str, str],
    ) -> RawArtifactCaptureReceipt:
        """Atomically publish a complete role-to-raw-receipt capture manifest."""
        self._require_initialized()
        provider = self._identifier(provider, "provider", 128)
        product = self._identifier(product, "product", 256)
        fetch_id = self._identifier(fetch_id, "fetch_id", 512)
        upstream_capture_id = self._identifier(
            upstream_capture_id, "upstream_capture_id", 1024
        )
        if product_timestamp is not None:
            product_timestamp = self._timestamp(
                product_timestamp, "product_timestamp"
            )
        if not isinstance(members, Mapping) or not members:
            raise ValueError("raw-artifactcapture vereist leden")
        if len(members) > self.limits.max_capture_members:
            raise RawArtifactEntryCapacityError(
                scope="capture_members",
                current_entries=0,
                attempted_entries=len(members),
                limit_entries=self.limits.max_capture_members,
            )
        normalized_members: list[tuple[str, str]] = []
        for role, receipt_id in members.items():
            normalized_members.append((
                self._identifier(role, "capture-role", 128),
                self._identifier(receipt_id, "receipt_id", 96),
            ))
        normalized_members.sort()
        if len({role for role, _ in normalized_members}) != len(normalized_members):
            raise ValueError("raw-artifactcapture bevat dubbele rollen")
        if len({value for _, value in normalized_members}) != len(normalized_members):
            raise ValueError("raw-artifactcapture hergebruikt één receipt voor meerdere rollen")

        with self._lock, closing(self._connect()) as database:
            database.execute("BEGIN IMMEDIATE")
            try:
                self._checkpoint("capture_after_begin")
                # A capture is also a new catalog authority.  Re-establish the
                # whole physical raw layout even when the manifest is an exact
                # retry; an unrelated unknown/over-capacity entry may not be
                # hidden by the telemetry cache.
                self._scan_storage(force=True)
                member_receipts: list[RawArtifactReceipt] = []
                manifest_members: list[dict[str, Any]] = []
                for role, receipt_id in normalized_members:
                    row = database.execute(
                        self._RECEIPT_SELECT + " WHERE receipt.receipt_id=?",
                        (receipt_id,),
                    ).fetchone()
                    if row is None:
                        raise RawArtifactNotFoundError(
                            f"raw-artifactcapture mist lid {role}"
                        )
                    receipt = self._validated_receipt(row)
                    if receipt.provider != provider:
                        raise RawArtifactConflictError(
                            "raw-artifactcapture mengt providers"
                        )
                    self._read_verified(
                        Path(receipt.artifact_path),
                        expected_size=receipt.size_bytes,
                        expected_sha256=receipt.content_sha256,
                        return_bytes=False,
                    )
                    member_receipts.append(receipt)
                    manifest_members.append({
                        "content_sha256": receipt.content_sha256,
                        "received_at": receipt.received_at,
                        "receipt_id": receipt.receipt_id,
                        "role": role,
                    })
                manifest = {
                    "fetch_id": fetch_id,
                    "members": manifest_members,
                    "product": product,
                    "product_timestamp": product_timestamp,
                    "provider": provider,
                    "schema": RAW_ARTIFACT_CAPTURE_SCHEMA,
                    "upstream_capture_id": upstream_capture_id,
                }
                manifest_json = self._canonical_json(manifest)
                manifest_sha256 = hashlib.sha256(
                    manifest_json.encode("utf-8")
                ).hexdigest()
                capture_id = self._capture_id(
                    provider=provider,
                    product=product,
                    fetch_id=fetch_id,
                )
                existing = database.execute(
                    "SELECT * FROM raw_artifact_captures WHERE capture_id=?",
                    (capture_id,),
                ).fetchone()
                if existing is None:
                    capture_count = int(database.execute(
                        "SELECT count(*) FROM raw_artifact_captures"
                    ).fetchone()[0])
                    if capture_count >= self.limits.max_capture_entries:
                        raise RawArtifactEntryCapacityError(
                            scope="captures",
                            current_entries=capture_count,
                            attempted_entries=1,
                            limit_entries=self.limits.max_capture_entries,
                        )
                    created_at = time.time()
                    database.execute(
                        """
                        INSERT INTO raw_artifact_captures(
                            capture_id, provider, product, fetch_id,
                            upstream_capture_id, product_timestamp,
                            manifest_json, manifest_sha256, created_at
                        ) VALUES(?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            capture_id, provider, product, fetch_id,
                            upstream_capture_id, product_timestamp,
                            manifest_json, manifest_sha256, created_at,
                        ),
                    )
                    database.executemany(
                        """
                        INSERT INTO raw_artifact_capture_members(
                            capture_id, role, receipt_id
                        ) VALUES(?,?,?)
                        """,
                        [
                            (capture_id, role, receipt_id)
                            for role, receipt_id in normalized_members
                        ],
                    )
                elif (
                    str(existing["provider"]) != provider
                    or str(existing["product"]) != product
                    or str(existing["fetch_id"]) != fetch_id
                    or str(existing["manifest_json"]) != manifest_json
                    or str(existing["manifest_sha256"]) != manifest_sha256
                ):
                    raise RawArtifactConflictError(
                        "capture-fetch-id werd al gebruikt voor een ander manifest"
                    )
                else:
                    created_at = self._timestamp(
                        existing["created_at"], "capture-created-at"
                    )
                self._checkpoint("capture_before_commit")
                database.commit()
            except sqlite3.IntegrityError as exc:
                if database.in_transaction:
                    database.rollback()
                raise RawArtifactConflictError(
                    "raw capturewrite botst met een reclamation fence of tombstone"
                ) from exc
            except sqlite3.OperationalError as exc:
                if database.in_transaction:
                    database.rollback()
                if self._is_sqlite_capacity_error(exc):
                    raise self._catalog_capacity_error() from exc
                raise
            except BaseException:
                if database.in_transaction:
                    database.rollback()
                raise
        self._checkpoint("capture_after_commit")
        capture_members = tuple(
            RawArtifactCaptureMember(
                role=role,
                receipt_id=receipt.receipt_id,
                content_sha256=receipt.content_sha256,
                receipt_sha256=receipt.receipt_sha256,
                received_at=receipt.received_at,
            )
            for (role, _receipt_id), receipt in zip(
                normalized_members,
                member_receipts,
                strict=True,
            )
        )
        return RawArtifactCaptureReceipt(
            capture_id=capture_id,
            provider=provider,
            product=product,
            fetch_id=fetch_id,
            upstream_capture_id=upstream_capture_id,
            product_timestamp=product_timestamp,
            manifest_sha256=manifest_sha256,
            capture_receipt_sha256=self._capture_receipt_sha256(
                capture_id=capture_id,
                provider=provider,
                product=product,
                fetch_id=fetch_id,
                upstream_capture_id=upstream_capture_id,
                product_timestamp=product_timestamp,
                manifest_sha256=manifest_sha256,
                members=capture_members,
            ),
            created_at=created_at,
            members=capture_members,
        )

    def _load_capture_bundle(
        self, capture_id: str,
        *,
        return_bytes: bool,
    ) -> tuple[
        RawArtifactCaptureReceipt,
        dict[str, RawArtifactReceipt],
        dict[str, bytes],
    ] | None:
        self._require_initialized()
        capture_id = self._identifier(capture_id, "capture_id", 96)
        with self._lock, closing(self._connect()) as database:
            row = database.execute(
                "SELECT * FROM raw_artifact_captures WHERE capture_id=?",
                (capture_id,),
            ).fetchone()
            if row is None:
                return None
            member_rows = database.execute(
                """
                SELECT member.role AS capture_role,
                       receipt.receipt_id, receipt.provider,
                       receipt.product, receipt.fetch_id,
                       receipt.upstream_id, receipt.content_type,
                       receipt.received_at, receipt.content_sha256,
                       receipt.size_bytes, receipt.provenance_json,
                       receipt.provenance_sha256, receipt.receipt_sha256,
                       receipt.created_at, object.relative_path,
                       object.size_bytes AS object_size_bytes
                FROM raw_artifact_capture_members AS member
                JOIN raw_artifact_receipts AS receipt
                  ON receipt.receipt_id=member.receipt_id
                JOIN raw_artifact_objects AS object
                  ON object.content_sha256=receipt.content_sha256
                WHERE member.capture_id=? ORDER BY member.role
                """,
                (capture_id,),
            ).fetchall()
        provider = self._identifier(row["provider"], "provider", 128)
        product = self._identifier(row["product"], "product", 256)
        fetch_id = self._identifier(row["fetch_id"], "fetch_id", 512)
        upstream_capture_id = self._identifier(
            row["upstream_capture_id"], "upstream_capture_id", 1024
        )
        product_timestamp = (
            self._timestamp(row["product_timestamp"], "product_timestamp")
            if row["product_timestamp"] is not None else None
        )
        created_at = self._timestamp(row["created_at"], "created_at")
        if not member_rows:
            raise RawArtifactIntegrityError(
                "raw-artifactcapture heeft geen leden"
            )
        normalized_members: list[RawArtifactCaptureMember] = []
        receipts_by_role: dict[str, RawArtifactReceipt] = {}
        payloads_by_role: dict[str, bytes] = {}
        for member in member_rows:
            role = self._identifier(
                member["capture_role"], "capture-role", 128
            )
            receipt = self._validated_receipt(member)
            payload = self._read_verified(
                Path(receipt.artifact_path),
                expected_size=receipt.size_bytes,
                expected_sha256=receipt.content_sha256,
                return_bytes=return_bytes,
            )
            if return_bytes:
                if payload is None:  # pragma: no cover - return contract
                    raise AssertionError("raw-artifactcapturebytes ontbreken")
                payloads_by_role[role] = payload
            if role in receipts_by_role:
                raise RawArtifactIntegrityError(
                    "raw-artifactcapture bevat een dubbele rol"
                )
            receipts_by_role[role] = receipt
            normalized_members.append(RawArtifactCaptureMember(
                role=role,
                receipt_id=receipt.receipt_id,
                content_sha256=receipt.content_sha256,
                receipt_sha256=receipt.receipt_sha256,
                received_at=receipt.received_at,
            ))
        members = tuple(normalized_members)
        manifest = {
            "fetch_id": fetch_id,
            "members": [
                {
                    "content_sha256": member.content_sha256,
                    "received_at": member.received_at,
                    "receipt_id": member.receipt_id,
                    "role": member.role,
                }
                for member in members
            ],
            "product": product,
            "product_timestamp": product_timestamp,
            "provider": provider,
            "schema": RAW_ARTIFACT_CAPTURE_SCHEMA,
            "upstream_capture_id": upstream_capture_id,
        }
        manifest_json = self._canonical_json(manifest)
        manifest_sha256 = hashlib.sha256(
            manifest_json.encode("utf-8")
        ).hexdigest()
        if (
            row["manifest_json"] != manifest_json
            or row["manifest_sha256"] != manifest_sha256
            or capture_id != self._capture_id(
                provider=provider,
                product=product,
                fetch_id=fetch_id,
            )
        ):
            raise RawArtifactIntegrityError(
                "raw-artifactcapturemanifest klopt niet"
            )
        capture_receipt_sha256 = self._capture_receipt_sha256(
            capture_id=capture_id,
            provider=provider,
            product=product,
            fetch_id=fetch_id,
            upstream_capture_id=upstream_capture_id,
            product_timestamp=product_timestamp,
            manifest_sha256=manifest_sha256,
            members=members,
        )
        capture = RawArtifactCaptureReceipt(
            capture_id=capture_id,
            provider=provider,
            product=product,
            fetch_id=fetch_id,
            upstream_capture_id=upstream_capture_id,
            product_timestamp=product_timestamp,
            manifest_sha256=manifest_sha256,
            capture_receipt_sha256=capture_receipt_sha256,
            created_at=created_at,
            members=members,
        )
        return capture, receipts_by_role, payloads_by_role

    def load_capture_bundle(
        self, capture_id: str,
    ) -> tuple[
        RawArtifactCaptureReceipt,
        dict[str, RawArtifactReceipt],
    ] | None:
        """Load a capture plus receipts with one SHA pass per member."""

        bundle = self._load_capture_bundle(capture_id, return_bytes=False)
        return None if bundle is None else (bundle[0], bundle[1])

    def load_capture_bundle_bytes(
        self, capture_id: str,
    ) -> tuple[
        RawArtifactCaptureReceipt,
        dict[str, RawArtifactReceipt],
        dict[str, bytes],
    ] | None:
        """Load capture, receipts and exact bytes in one SHA pass per member."""

        return self._load_capture_bundle(capture_id, return_bytes=True)

    def load_capture(
        self, capture_id: str,
    ) -> RawArtifactCaptureReceipt | None:
        """Load and verify a complete atomic capture manifest."""

        bundle = self.load_capture_bundle(capture_id)
        return None if bundle is None else bundle[0]

    def load_capture_by_fetch(
        self,
        *,
        provider: str,
        product: str,
        fetch_id: str,
    ) -> RawArtifactCaptureReceipt | None:
        """Load the unique verified capture bound to one acquisition fetch.

        The three-part identity is used by the worker-only restart recovery
        path.  Returning a verified receipt instead of exposing catalogue rows
        keeps raw bytes and unvalidated metadata behind this store boundary.
        """

        self._require_initialized()
        provider = self._identifier(provider, "provider", 128)
        product = self._identifier(product, "product", 256)
        fetch_id = self._identifier(fetch_id, "fetch_id", 512)
        with self._lock, closing(self._connect()) as database:
            rows = database.execute(
                "SELECT capture_id FROM raw_artifact_captures "
                "WHERE provider=? AND product=? AND fetch_id=? "
                "ORDER BY created_at DESC,capture_id DESC LIMIT 2",
                (provider, product, fetch_id),
            ).fetchall()
        if not rows:
            return None
        if len(rows) != 1:
            raise RawArtifactIntegrityError(
                "capture fetch identity is not unique"
            )
        return self.load_capture(str(rows[0]["capture_id"]))

    def load_capture_receipt(
        self,
        capture_id: str,
    ) -> RawArtifactCaptureReceipt | None:
        """Return the verified, payload-free schema-3 capture receipt view."""
        return self.load_capture(capture_id)

    def load_receipt(self, receipt_id: str) -> RawArtifactReceipt | None:
        """Load and fully verify one receipt; absence is not a dry observation."""
        self._require_initialized()
        receipt_id = self._identifier(receipt_id, "receipt_id", 96)
        with self._lock, closing(self._connect()) as database:
            row = database.execute(
                self._RECEIPT_SELECT + " WHERE receipt.receipt_id=?",
                (receipt_id,),
            ).fetchone()
        if row is None:
            return None
        receipt = self._validated_receipt(row)
        self._read_verified(
            Path(receipt.artifact_path),
            expected_size=receipt.size_bytes,
            expected_sha256=receipt.content_sha256,
            return_bytes=False,
        )
        return receipt

    def accounting_receipts(self) -> tuple[RawArtifactReceipt, ...]:
        """Return a bounded, payload-free catalog receipt inventory.

        Receipt checksums, catalog bindings and every unique object are
        validated here. Complete captures are independently verified again by
        ``accounting_captures`` so recovery cannot rely on a stale inventory
        view across its bounded concurrency window.
        """

        self._require_initialized()
        with self._lock, closing(self._connect()) as database:
            rows = database.execute(
                self._RECEIPT_SELECT + " ORDER BY receipt.receipt_id LIMIT ?",
                (self.limits.max_receipt_entries + 1,),
            ).fetchall()
        if len(rows) > self.limits.max_receipt_entries:
            raise RawArtifactIntegrityError(
                "raw-artifactreceiptinventory overschrijdt zijn vaste grens"
            )
        receipts = tuple(self._validated_receipt(row) for row in rows)
        verified_objects: set[str] = set()
        for receipt in receipts:
            if receipt.content_sha256 in verified_objects:
                continue
            self._read_verified(
                Path(receipt.artifact_path),
                expected_size=receipt.size_bytes,
                expected_sha256=receipt.content_sha256,
                return_bytes=False,
            )
            verified_objects.add(receipt.content_sha256)
        return receipts

    def accounting_captures(self) -> tuple[RawArtifactCaptureReceipt, ...]:
        """Return every bounded capture after manifest and object verification."""

        self._require_initialized()
        with self._lock, closing(self._connect()) as database:
            rows = database.execute(
                "SELECT capture_id FROM raw_artifact_captures "
                "ORDER BY capture_id LIMIT ?",
                (self.limits.max_capture_entries + 1,),
            ).fetchall()
        if len(rows) > self.limits.max_capture_entries:
            raise RawArtifactIntegrityError(
                "raw-artifactcaptureinventory overschrijdt zijn vaste grens"
            )
        captures: list[RawArtifactCaptureReceipt] = []
        for row in rows:
            capture = self.load_capture(str(row[0]))
            if capture is None:  # pragma: no cover - append-only catalog contract
                raise RawArtifactIntegrityError(
                    "raw-artifactcapture verdween tijdens inventory"
                )
            captures.append(capture)
        return tuple(captures)

    def load_receipt_bytes(
        self,
        receipt_id: str,
    ) -> tuple[RawArtifactReceipt, bytes] | None:
        """Load verified receipt plus bytes with exactly one object read.

        ``load_receipt()`` deliberately verifies the full immutable object.
        Calling it from ``read_bytes()`` used to read and hash that same file a
        second time immediately afterwards.  Large native products therefore
        paid two complete I/O/SHA passes for one logical read.  This combined
        primitive validates the catalog receipt first and then obtains the
        exact bytes in the single mandatory verified object pass.
        """

        self._require_initialized()
        receipt_id = self._identifier(receipt_id, "receipt_id", 96)
        with self._lock, closing(self._connect()) as database:
            row = database.execute(
                self._RECEIPT_SELECT + " WHERE receipt.receipt_id=?",
                (receipt_id,),
            ).fetchone()
        if row is None:
            return None
        receipt = self._validated_receipt(row)
        payload = self._read_verified(
            Path(receipt.artifact_path),
            expected_size=receipt.size_bytes,
            expected_sha256=receipt.content_sha256,
            return_bytes=True,
        )
        if payload is None:  # pragma: no cover - return contract
            raise AssertionError("raw-artifactbytes ontbreken")
        return receipt, payload

    def read_receipt_bytes(
        self,
        receipt_id: str,
    ) -> tuple[RawArtifactReceipt, bytes]:
        """Read verified receipt plus bytes or raise when it is absent."""

        loaded = self.load_receipt_bytes(receipt_id)
        if loaded is None:
            raise RawArtifactNotFoundError("raw-artifactreceipt bestaat niet")
        return loaded

    def read_bytes(self, receipt_id: str) -> bytes:
        """Return exact stored bytes or raise; never synthesize fallback data."""

        return self.read_receipt_bytes(receipt_id)[1]

    def stats(self) -> dict[str, Any]:
        """Report catalog and physical usage without retention or cleanup."""
        self._require_initialized()
        with self._lock, closing(self._connect()) as database:
            row = database.execute(
                """
                SELECT
                  (SELECT count(*) FROM raw_artifact_objects) AS objects,
                  (SELECT count(*) FROM raw_artifact_receipts) AS receipts,
                  (SELECT count(*) FROM raw_artifact_captures) AS captures,
                  (SELECT COALESCE(sum(size_bytes), 0)
                     FROM raw_artifact_objects) AS catalog_bytes
                """
            ).fetchone()
            catalog_paths = {
                str(value[0])
                for value in database.execute(
                    "SELECT relative_path FROM raw_artifact_objects"
                )
            }
            catalog_page_size = int(
                database.execute("PRAGMA page_size").fetchone()[0]
            )
            catalog_main_database_bytes = (
                int(database.execute("PRAGMA page_count").fetchone()[0])
                * catalog_page_size
            )
            catalog_main_database_max_bytes = (
                raw_catalog_main_database_max_bytes(
                    max_catalog_bytes=self.limits.max_catalog_bytes,
                    page_size_bytes=catalog_page_size,
                )
            )
            if (
                catalog_main_database_bytes
                > catalog_main_database_max_bytes
            ):
                raise RawArtifactCapacityError(
                    scope="catalog",
                    current_bytes=catalog_main_database_bytes,
                    attempted_bytes=0,
                    limit_bytes=catalog_main_database_max_bytes,
                )
            storage_usage = self._scan_storage()
        return {
            "schema_version": RAW_ARTIFACT_SCHEMA_VERSION,
            "object_count": int(row["objects"]),
            "receipt_count": int(row["receipts"]),
            "capture_count": int(row["captures"]),
            "catalog_bytes": int(row["catalog_bytes"]),
            "filesystem_bytes": storage_usage.total_bytes,
            "filesystem_object_entries": storage_usage.object_entries,
            "filesystem_staging_entries": storage_usage.staging_entries,
            "unmanaged_file_count": len(storage_usage.entries - catalog_paths),
            "max_object_bytes": self.limits.max_object_bytes,
            "max_total_bytes": self.limits.max_total_bytes,
            "max_object_entries": self.limits.max_object_entries,
            "max_staging_entries": self.limits.max_staging_entries,
            "max_receipt_entries": self.limits.max_receipt_entries,
            "max_capture_entries": self.limits.max_capture_entries,
            "max_capture_members": self.limits.max_capture_members,
            "max_catalog_bytes": self.limits.max_catalog_bytes,
            "catalog_database_bytes": self._catalog_database_bytes(),
            "catalog_main_database_bytes": catalog_main_database_bytes,
            "catalog_main_database_max_bytes": (
                catalog_main_database_max_bytes
            ),
            "catalog_page_size_bytes": catalog_page_size,
            "store_profile_sha256": hashlib.sha256(
                self._store_profile_bytes()
            ).hexdigest(),
            "retention_policy": "none",
        }

    def integrity_check(self) -> dict[str, Any]:
        """Verify schema, receipts and every catalogued object without writes."""
        self._require_initialized()
        issues: list[str] = []
        checked_objects = 0
        checked_receipts = 0
        try:
            self._assert_safe_layout()
            with self._lock, closing(self._connect()) as database:
                self._validate_schema(database)
                objects = database.execute(
                    "SELECT content_sha256, relative_path, size_bytes "
                    "FROM raw_artifact_objects ORDER BY content_sha256"
                ).fetchall()
                receipts = database.execute(
                    self._RECEIPT_SELECT + " ORDER BY receipt.receipt_id"
                ).fetchall()
                capture_ids = [
                    str(row[0])
                    for row in database.execute(
                        "SELECT capture_id FROM raw_artifact_captures "
                        "ORDER BY capture_id"
                    )
                ]
                reclamation_audit = self._audit_reclamation_extension(
                    database
                )
            for row in objects:
                path = self._safe_object_path(
                    str(row["relative_path"]), str(row["content_sha256"])
                )
                self._read_verified(
                    path,
                    expected_size=int(row["size_bytes"]),
                    expected_sha256=str(row["content_sha256"]),
                    return_bytes=False,
                )
                checked_objects += 1
            for row in receipts:
                self._validated_receipt(row)
                checked_receipts += 1
            for capture_id in capture_ids:
                self.load_capture(capture_id)
            self._scan_storage(force=True)
        except (RawArtifactError, sqlite3.DatabaseError, OSError, ValueError) as exc:
            issues.append(str(exc))
        return {
            "healthy": not issues,
            "checked_objects": checked_objects,
            "checked_receipts": checked_receipts,
            "checked_captures": (
                len(capture_ids) if "capture_ids" in locals() else 0
            ),
            "reclamation": (
                reclamation_audit
                if "reclamation_audit" in locals()
                else {"enabled": False, "checked_prepares": 0}
            ),
            "issues": issues,
        }

    def startup_integrity_check(self) -> dict[str, Any]:
        """Validate the durable catalog without re-hashing the full raw corpus.

        Object bytes are content-addressed and are still verified by every
        read path.  Re-reading the complete historical corpus during every
        process start made control-plane admission grow linearly with the
        database and could delay provider acquisition for nearly an hour.
        Startup instead proves the SQLite catalog, foreign-key graph and
        bounded row inventories.  The retention guard separately compares
        those inventories with a fresh filesystem scan before admission.
        """

        self._require_initialized()
        issues: list[str] = []
        object_count = 0
        receipt_count = 0
        capture_count = 0
        reclamation_audit: dict[str, Any] = {
            "enabled": False,
            "checked_prepares": 0,
        }
        try:
            self._assert_safe_layout()
            with self._lock, closing(self._connect()) as database:
                self._validate_schema(database)
                quick_check = database.execute("PRAGMA quick_check").fetchone()
                if quick_check is None or str(quick_check[0]) != "ok":
                    raise RawArtifactIntegrityError(
                        "raw-artifactcatalogus quick-check faalde"
                    )
                foreign_key_issue = database.execute(
                    "PRAGMA foreign_key_check"
                ).fetchone()
                if foreign_key_issue is not None:
                    raise RawArtifactIntegrityError(
                        "raw-artifactcatalogus foreign-keycheck faalde"
                    )
                counts = database.execute(
                    """
                    SELECT
                      (SELECT count(*) FROM raw_artifact_objects),
                      (SELECT count(*) FROM raw_artifact_receipts),
                      (SELECT count(*) FROM raw_artifact_captures)
                    """
                ).fetchone()
                object_count = int(counts[0])
                receipt_count = int(counts[1])
                capture_count = int(counts[2])
                if object_count > self.limits.max_object_entries:
                    raise RawArtifactIntegrityError(
                        "raw-artifactobjectinventory overschrijdt zijn vaste grens"
                    )
                if receipt_count > self.limits.max_receipt_entries:
                    raise RawArtifactIntegrityError(
                        "raw-artifactreceiptinventory overschrijdt zijn vaste grens"
                    )
                if capture_count > self.limits.max_capture_entries:
                    raise RawArtifactIntegrityError(
                        "raw-artifactcaptureinventory overschrijdt zijn vaste grens"
                    )
                reclamation_audit = self._audit_reclamation_extension(database)
        except (RawArtifactError, sqlite3.DatabaseError, OSError, ValueError) as exc:
            issues.append(str(exc))
        return {
            "healthy": not issues,
            "checked_objects": object_count,
            "checked_receipts": receipt_count,
            "checked_captures": capture_count,
            "reclamation": reclamation_audit,
            "issues": issues,
            "verification_scope": "startup_catalog_metadata",
        }

    def _audit_reclamation_extension(
        self,
        database: sqlite3.Connection,
    ) -> dict[str, Any]:
        tables = self._table_names(database)
        if tables == self._TABLES:
            return {
                "enabled": False,
                "checked_aborts": 0,
                "checked_applies": 0,
                "checked_files_pending": 0,
                "checked_prepares": 0,
            }
        self._require_reclamation_extension(database)
        request_rows = database.execute(
            "SELECT request_sha256 FROM raw_artifact_reclamation_prepares "
            "ORDER BY request_sha256"
        ).fetchall()
        counters = {
            "enabled": True,
            "checked_aborts": 0,
            "checked_applies": 0,
            "checked_files_pending": 0,
            "checked_prepares": len(request_rows),
        }
        for row in request_rows:
            request_sha256 = str(row["request_sha256"])
            prepared = self._load_reclamation_prepare(
                database,
                request_sha256,
            )
            if prepared is None:  # pragma: no cover - selected parent row
                raise RawArtifactIntegrityError(
                    "raw reclamation audit verloor een prepare"
                )
            aborted = self._load_reclamation_abort(
                database,
                request_sha256,
            )
            has_catalog = database.execute(
                "SELECT 1 FROM raw_artifact_reclamation_catalog_applies "
                "WHERE request_sha256=?",
                (request_sha256,),
            ).fetchone() is not None
            if aborted is not None:
                if has_catalog:
                    raise RawArtifactIntegrityError(
                        "raw reclamation is tegelijk aborted en FILES_PENDING"
                    )
                counters["checked_aborts"] += 1
                continue
            request = self._load_reclamation_request(
                database,
                request_sha256,
            )
            if not has_catalog:
                self._reprove_reclamation_prepare_plan(
                    database,
                    request=request,
                    prepared=prepared,
                )
                continue
            catalog = self._load_reclamation_catalog_apply(
                database,
                request_sha256=request_sha256,
            )
            events = self._load_reclamation_unlink_events(
                database,
                catalog=catalog,
            )
            applied = self._load_reclamation_apply(
                database,
                request_sha256,
            )
            if applied is not None:
                counters["checked_applies"] += 1
                continue
            counters["checked_files_pending"] += 1
            self._assert_reclamation_durable_paths_absent(
                catalog=catalog,
                event_count=len(events),
            )
            self._assert_reclamation_pending_catalog_absence(
                database,
                request_sha256=request_sha256,
            )
            for identity in catalog["identities"][len(events):]:
                path = self._safe_object_path(
                    identity.relative_path,
                    identity.content_sha256,
                )
                try:
                    path.lstat()
                except FileNotFoundError:
                    # Catalog commit followed by unlink but preceding the
                    # directory barrier/event insert is a valid crash point.
                    continue
                self._reclamation_file_identity(
                    path,
                    expected_size=identity.size_bytes,
                    expected_sha256=identity.content_sha256,
                    expected_identity=identity,
                )
            batch = self._load_reclamation_unlinked_batch(
                database,
                catalog=catalog,
            )
            if batch is not None and len(events) != len(catalog["intent_rows"]):
                raise RawArtifactIntegrityError(
                    "raw reclamation durable batch heeft onvolledige events"
                )
        return counters

    def _prepare_layout(self) -> None:
        root_existed = self.root.exists()
        if self.root.exists() and (
            self.root.is_symlink() or not self.root.is_dir()
        ):
            raise RawArtifactIntegrityError(
                "raw-artifactroot is geen veilige reguliere map"
            )
        self.root.mkdir(parents=True, exist_ok=True)
        if not root_existed:
            self._fsync_directory(self.root.parent)
            self._fsync_directory(self.root)
        for path, label in (
            (self.objects_path, "objectmap"),
            (self.staging_path, "stagingmap"),
        ):
            path_existed = path.exists()
            if path.exists() and (path.is_symlink() or not path.is_dir()):
                raise RawArtifactIntegrityError(
                    f"raw-artifact{label} is geen veilige reguliere map"
                )
            path.mkdir(exist_ok=True)
            if not path_existed:
                self._fsync_directory(self.root)
        if self.catalog_path.is_symlink():
            raise RawArtifactIntegrityError(
                "raw-artifactcatalogus mag geen symlink zijn"
            )
        self._ensure_store_profile()
        self._assert_safe_layout()

    def _assert_safe_layout(self) -> None:
        root = self.root.resolve()
        if self.root.is_symlink() or not self.root.is_dir():
            raise RawArtifactIntegrityError("raw-artifactroot is onveilig")
        for path in (self.objects_path, self.staging_path):
            info = path.lstat()
            if not stat.S_ISDIR(info.st_mode) or path.is_symlink():
                raise RawArtifactIntegrityError("raw-artifactsubmap is onveilig")
            if path.resolve().parent != root:
                raise RawArtifactIntegrityError("raw-artifactsubmap ontsnapt uit root")
        if self.catalog_path.is_symlink():
            raise RawArtifactIntegrityError("raw-artifactcatalogus is een symlink")
        self._validate_store_profile()
        self._validate_profile_stage_if_present(self._store_profile_bytes())

    def _store_profile_bytes(self) -> bytes:
        limits = {
            name: int(getattr(self.limits, name))
            for name in RawArtifactLimits.__dataclass_fields__
        }
        document = {
            "limits": limits,
            "raw_store_schema_version": RAW_ARTIFACT_SCHEMA_VERSION,
            "schema": STORE_PROFILE_SCHEMA,
        }
        return self._canonical_json(document).encode("utf-8")

    def _ensure_store_profile(self) -> None:
        expected = self._store_profile_bytes()
        if self.profile_path.exists() or self.profile_path.is_symlink():
            self._validate_store_profile(expected=expected)
            self._validate_profile_stage_if_present(expected)
            return

        self._validate_existing_layout_for_profile()
        created_stage = False
        descriptor: int | None = None
        try:
            flags = (
                os.O_CREAT
                | os.O_EXCL
                | os.O_WRONLY
                | getattr(os, "O_BINARY", 0)
                | getattr(os, "O_NOFOLLOW", 0)
            )
            try:
                descriptor = os.open(self.profile_staging_path, flags, 0o600)
                created_stage = True
            except FileExistsError:
                self._validate_profile_stage(expected)
            if descriptor is not None:
                written = 0
                while written < len(expected):
                    count = os.write(descriptor, expected[written:])
                    if count <= 0:  # pragma: no cover - defensive OS contract
                        raise RawArtifactIntegrityError(
                            "raw-artifactlimietprofiel kon niet volledig worden geschreven"
                        )
                    written += count
                os.fsync(descriptor)
                os.close(descriptor)
                descriptor = None
                self._fsync_directory(self.root)
            try:
                os.link(self.profile_staging_path, self.profile_path)
            except FileExistsError:
                pass
            self._fsync_directory(self.root)
            self._validate_store_profile(expected=expected)
        finally:
            if descriptor is not None:
                os.close(descriptor)
            if created_stage:
                self.profile_staging_path.unlink(missing_ok=True)
                self._fsync_directory(self.root)

    def _validate_profile_stage_if_present(self, expected: bytes) -> None:
        if (
            self.profile_staging_path.exists()
            or self.profile_staging_path.is_symlink()
        ):
            self._validate_profile_stage(expected)

    def _validate_profile_stage(self, expected: bytes) -> None:
        try:
            info = self.profile_staging_path.lstat()
        except FileNotFoundError as exc:
            raise RawArtifactIntegrityError(
                "raw-artifactlimietprofielstage verdween tijdens initialisatie"
            ) from exc
        if self.profile_staging_path.is_symlink() or not stat.S_ISREG(info.st_mode):
            raise RawArtifactIntegrityError(
                "raw-artifactlimietprofielstage is geen regulier bestand"
            )
        if info.st_size != len(expected) or info.st_size > 4096:
            raise RawArtifactConflictError(
                "raw-artifactstore heeft een conflicterende limietprofielstage"
            )
        try:
            actual = self.profile_staging_path.read_bytes()
        except OSError as exc:
            raise RawArtifactIntegrityError(
                "raw-artifactlimietprofielstage kon niet worden gelezen"
            ) from exc
        if actual != expected:
            raise RawArtifactConflictError(
                "raw-artifactstore heeft een conflicterende limietprofielstage"
            )

    def _validate_existing_layout_for_profile(self) -> None:
        usage = self._scan_storage_uncached(self._storage_fingerprints())
        if usage.total_bytes > self.limits.max_total_bytes:
            raise RawArtifactCapacityError(
                scope="total",
                current_bytes=usage.total_bytes,
                attempted_bytes=0,
                limit_bytes=self.limits.max_total_bytes,
            )
        catalog_bytes = self._catalog_database_bytes()
        if catalog_bytes > self.limits.max_catalog_bytes:
            raise RawArtifactCapacityError(
                scope="catalog",
                current_bytes=catalog_bytes,
                attempted_bytes=0,
                limit_bytes=self.limits.max_catalog_bytes,
            )
        if not self.catalog_path.exists():
            return
        info = self.catalog_path.lstat()
        if self.catalog_path.is_symlink() or not stat.S_ISREG(info.st_mode):
            raise RawArtifactIntegrityError(
                "raw-artifactcatalogus is geen regulier bestand"
            )
        try:
            with closing(sqlite3.connect(
                f"{self.catalog_path.resolve().as_uri()}?mode=ro",
                uri=True,
            )) as database:
                database.row_factory = sqlite3.Row
                database.execute("PRAGMA query_only=ON")
                tables = self._table_names(database)
                if not tables:
                    return
                if tables not in (
                    self._TABLES,
                    self._TABLES | self._RECLAMATION_TABLES,
                ):
                    raise RawArtifactIntegrityError(
                        "raw-artifactcatalogus heeft onbekend of onvolledig schema"
                    )
                self._validate_schema(database)
                receipt_count = int(database.execute(
                    "SELECT count(*) FROM raw_artifact_receipts"
                ).fetchone()[0])
                capture_count = int(database.execute(
                    "SELECT count(*) FROM raw_artifact_captures"
                ).fetchone()[0])
                oversized_capture = database.execute(
                    "SELECT capture_id FROM raw_artifact_capture_members "
                    "GROUP BY capture_id HAVING count(*)>? LIMIT 1",
                    (self.limits.max_capture_members,),
                ).fetchone()
        except RawArtifactError:
            raise
        except sqlite3.DatabaseError as exc:
            raise RawArtifactIntegrityError(
                "raw-artifactcatalogus is geen geldige SQLite-database"
            ) from exc
        if receipt_count > self.limits.max_receipt_entries:
            raise RawArtifactEntryCapacityError(
                scope="receipts",
                current_entries=receipt_count,
                attempted_entries=0,
                limit_entries=self.limits.max_receipt_entries,
            )
        if capture_count > self.limits.max_capture_entries:
            raise RawArtifactEntryCapacityError(
                scope="captures",
                current_entries=capture_count,
                attempted_entries=0,
                limit_entries=self.limits.max_capture_entries,
            )
        if oversized_capture is not None:
            raise RawArtifactEntryCapacityError(
                scope="capture_members",
                current_entries=self.limits.max_capture_members + 1,
                attempted_entries=0,
                limit_entries=self.limits.max_capture_members,
            )

    def _validate_store_profile(self, *, expected: bytes | None = None) -> None:
        if expected is None:
            expected = self._store_profile_bytes()
        try:
            info = self.profile_path.lstat()
        except FileNotFoundError as exc:
            raise RawArtifactIntegrityError(
                "raw-artifactstore mist zijn limietprofiel"
            ) from exc
        if self.profile_path.is_symlink() or not stat.S_ISREG(info.st_mode):
            raise RawArtifactIntegrityError(
                "raw-artifactlimietprofiel is geen regulier bestand"
            )
        if info.st_size != len(expected) or info.st_size > 4096:
            raise RawArtifactConflictError(
                "raw-artifactstore werd met een ander limietprofiel geopend"
            )
        try:
            actual = self.profile_path.read_bytes()
        except OSError as exc:
            raise RawArtifactIntegrityError(
                "raw-artifactlimietprofiel kon niet worden gelezen"
            ) from exc
        if actual != expected:
            raise RawArtifactConflictError(
                "raw-artifactstore werd met een ander limietprofiel geopend"
            )

    def _connect_unconfigured(self) -> sqlite3.Connection:
        self._assert_safe_layout()
        physical_bytes = self._catalog_database_bytes()
        if physical_bytes > self.limits.max_catalog_bytes:
            raise RawArtifactCapacityError(
                scope="catalog",
                current_bytes=physical_bytes,
                attempted_bytes=0,
                limit_bytes=self.limits.max_catalog_bytes,
            )
        database = sqlite3.connect(
            self.catalog_path,
            timeout=30.0,
            isolation_level=None,
        )
        try:
            database.row_factory = sqlite3.Row
            self._configure_catalog_capacity(database)
            return database
        except BaseException:
            database.close()
            raise

    def _connect(self) -> sqlite3.Connection:
        database = self._connect_unconfigured()
        try:
            database.execute("PRAGMA foreign_keys=ON")
            database.execute("PRAGMA busy_timeout=30000")
            database.execute("PRAGMA synchronous=FULL")
            journal_mode = str(database.execute(
                "PRAGMA journal_mode=DELETE"
            ).fetchone()[0]).lower()
            if journal_mode != "delete":
                raise RawArtifactIntegrityError(
                    "raw-artifactcatalogus kon rollback-journaling niet vastleggen"
                )
            database.execute("PRAGMA journal_size_limit=0")
            database.execute("PRAGMA temp_store=MEMORY")
            return database
        except BaseException:
            database.close()
            raise

    def _configure_catalog_capacity(self, database: sqlite3.Connection) -> None:
        page_size = int(database.execute("PRAGMA page_size").fetchone()[0])
        page_count = int(database.execute("PRAGMA page_count").fetchone()[0])
        # A rollback journal may contain every original database page plus
        # framing.  Only one quarter of the physical envelope is assigned to
        # the main database; the remaining three quarters conservatively
        # cover journal/framing and filesystem variance.  Unlike WAL, a
        # reader blocks the writer instead of permitting an unbounded log.
        maximum_bytes = raw_catalog_main_database_max_bytes(
            max_catalog_bytes=self.limits.max_catalog_bytes,
            page_size_bytes=page_size,
        )
        maximum_pages = maximum_bytes // page_size
        if page_count > maximum_pages:
            raise RawArtifactCapacityError(
                scope="catalog",
                current_bytes=page_count * page_size,
                attempted_bytes=0,
                limit_bytes=self.limits.max_catalog_bytes,
            )
        configured = int(
            database.execute(f"PRAGMA max_page_count={maximum_pages}").fetchone()[0]
        )
        if configured != maximum_pages:
            raise RawArtifactIntegrityError(
                "raw-artifactcatalogus kon zijn pagegrens niet vastleggen"
            )

    def _catalog_database_bytes(self) -> int:
        total = 0
        for path in (
            self.catalog_path,
            Path(f"{self.catalog_path}-journal"),
            Path(f"{self.catalog_path}-wal"),
            Path(f"{self.catalog_path}-shm"),
        ):
            if not path.exists():
                continue
            info = path.lstat()
            if path.is_symlink() or not stat.S_ISREG(info.st_mode):
                raise RawArtifactIntegrityError(
                    "raw-artifactcatalogus bevat een onveilig hulpbestand"
                )
            total += int(info.st_size)
        return total

    def _catalog_capacity_error(self) -> RawArtifactCapacityError:
        return RawArtifactCapacityError(
            scope="catalog",
            current_bytes=self._catalog_database_bytes(),
            attempted_bytes=0,
            limit_bytes=self.limits.max_catalog_bytes,
        )

    @staticmethod
    def _is_sqlite_capacity_error(error: sqlite3.OperationalError) -> bool:
        message = str(error).lower()
        return "database or disk is full" in message or "database is full" in message

    @staticmethod
    def _table_names(database: sqlite3.Connection) -> set[str]:
        return {
            str(row[0])
            for row in database.execute(
                """
                SELECT name FROM sqlite_master
                WHERE type='table' AND name NOT LIKE 'sqlite_%'
                """
            )
        }

    @staticmethod
    def _normalise_schema_sql(value: str) -> str:
        """Canonicalise insignificant SQLite DDL whitespace only."""

        return " ".join(value.split())

    @classmethod
    def _schema_inventory(
        cls,
        database: sqlite3.Connection,
    ) -> tuple[tuple[str, str, str, str], ...]:
        rows = database.execute(
            """
            SELECT type,name,tbl_name,sql FROM sqlite_master
            WHERE name NOT LIKE 'sqlite_%'
              AND type IN ('table','index','trigger','view')
              AND sql IS NOT NULL
            ORDER BY type,name
            """
        ).fetchall()
        return tuple(
            (
                str(row[0]),
                str(row[1]),
                str(row[2]),
                cls._normalise_schema_sql(str(row[3])),
            )
            for row in rows
        )

    @classmethod
    @lru_cache(maxsize=2)
    def _canonical_schema_inventory(
        cls,
        reclamation_enabled: bool,
    ) -> tuple[tuple[str, str, str, str], ...]:
        """Build the one accepted schema from the production create path."""

        with closing(sqlite3.connect(":memory:")) as expected:
            cls._create_schema(expected)
            if reclamation_enabled:
                cls._create_reclamation_extension_schema(
                    expected,
                    nonce="0" * 64,
                    profile_sha256="1" * 64,
                    identity_sha256="2" * 64,
                    created_at=0.0,
                )
            return cls._schema_inventory(expected)

    @classmethod
    def _validate_schema_inventory(
        cls,
        database: sqlite3.Connection,
        *,
        reclamation_enabled: bool,
    ) -> None:
        if cls._schema_inventory(database) != cls._canonical_schema_inventory(
            reclamation_enabled
        ):
            scope = "base+reclamation" if reclamation_enabled else "base"
            raise RawArtifactIntegrityError(
                "raw-artifactcatalogus heeft afwijkende canonieke "
                f"{scope}-schema-DDL"
            )

    @staticmethod
    def _create_schema(database: sqlite3.Connection) -> None:
        database.execute(
            """
            CREATE TABLE raw_artifact_metadata(
                metadata_key TEXT PRIMARY KEY CHECK(metadata_key='schema'),
                schema_version INTEGER NOT NULL
            ) WITHOUT ROWID
            """
        )
        database.execute(
            "INSERT INTO raw_artifact_metadata VALUES('schema', ?)",
            (RAW_ARTIFACT_SCHEMA_VERSION,),
        )
        database.execute(
            """
            CREATE TABLE raw_artifact_objects(
                content_sha256 TEXT PRIMARY KEY
                    CHECK(length(content_sha256)=64),
                relative_path TEXT NOT NULL UNIQUE,
                size_bytes INTEGER NOT NULL CHECK(size_bytes>=0),
                created_at REAL NOT NULL,
                CHECK(relative_path='objects/' || content_sha256 || '.raw')
            ) WITHOUT ROWID
            """
        )
        database.execute(
            """
            CREATE TABLE raw_artifact_receipts(
                receipt_id TEXT PRIMARY KEY,
                provider TEXT NOT NULL,
                product TEXT NOT NULL,
                fetch_id TEXT NOT NULL,
                upstream_id TEXT NOT NULL,
                content_type TEXT NOT NULL,
                received_at REAL NOT NULL,
                content_sha256 TEXT NOT NULL REFERENCES
                    raw_artifact_objects(content_sha256),
                size_bytes INTEGER NOT NULL CHECK(size_bytes>=0),
                provenance_json TEXT NOT NULL,
                provenance_sha256 TEXT NOT NULL
                    CHECK(length(provenance_sha256)=64),
                receipt_sha256 TEXT NOT NULL CHECK(length(receipt_sha256)=64),
                created_at REAL NOT NULL,
                UNIQUE(provider, product, fetch_id)
            ) WITHOUT ROWID
            """
        )
        database.execute(
            "CREATE INDEX idx_raw_artifact_receipts_content "
            "ON raw_artifact_receipts(content_sha256)"
        )
        database.execute(
            "CREATE INDEX idx_raw_artifact_receipts_identity "
            "ON raw_artifact_receipts(provider, product, upstream_id)"
        )
        database.execute(
            """
            CREATE TABLE raw_artifact_captures(
                capture_id TEXT PRIMARY KEY,
                provider TEXT NOT NULL,
                product TEXT NOT NULL,
                fetch_id TEXT NOT NULL,
                upstream_capture_id TEXT NOT NULL,
                product_timestamp REAL,
                manifest_json TEXT NOT NULL,
                manifest_sha256 TEXT NOT NULL
                    CHECK(length(manifest_sha256)=64),
                created_at REAL NOT NULL,
                UNIQUE(provider, product, fetch_id)
            ) WITHOUT ROWID
            """
        )
        database.execute(
            """
            CREATE TABLE raw_artifact_capture_members(
                capture_id TEXT NOT NULL REFERENCES
                    raw_artifact_captures(capture_id),
                role TEXT NOT NULL,
                receipt_id TEXT NOT NULL REFERENCES
                    raw_artifact_receipts(receipt_id),
                PRIMARY KEY(capture_id, role),
                UNIQUE(capture_id, receipt_id)
            ) WITHOUT ROWID
            """
        )

    @classmethod
    def _create_reclamation_extension_schema(
        cls,
        database: sqlite3.Connection,
        *,
        nonce: str,
        profile_sha256: str,
        identity_sha256: str,
        created_at: float,
    ) -> None:
        """Create the complete additive extension inside the caller's txn."""

        statements = (
            """
            CREATE TABLE raw_artifact_reclamation_metadata(
                metadata_key TEXT PRIMARY KEY
                    CHECK(metadata_key='reclamation_extension'),
                extension_schema_version INTEGER NOT NULL CHECK(
                    extension_schema_version=1
                ),
                base_schema_version INTEGER NOT NULL CHECK(base_schema_version=3),
                extension_schema TEXT NOT NULL,
                store_instance_nonce TEXT NOT NULL UNIQUE
                    CHECK(length(store_instance_nonce)=64),
                source_store_identity_sha256 TEXT NOT NULL UNIQUE
                    CHECK(length(source_store_identity_sha256)=64),
                store_profile_sha256 TEXT NOT NULL
                    CHECK(length(store_profile_sha256)=64),
                created_at REAL NOT NULL
            ) WITHOUT ROWID
            """,
            """
            CREATE TABLE raw_artifact_reclamation_prepares(
                request_sha256 TEXT PRIMARY KEY CHECK(length(request_sha256)=64),
                request_json TEXT NOT NULL,
                prepare_receipt_sha256 TEXT NOT NULL UNIQUE
                    CHECK(length(prepare_receipt_sha256)=64),
                prepare_receipt_json TEXT NOT NULL,
                source_fence_sha256 TEXT NOT NULL UNIQUE
                    CHECK(length(source_fence_sha256)=64),
                child_reference_root_sha256 TEXT NOT NULL
                    CHECK(length(child_reference_root_sha256)=64),
                candidate_bindings_sha256 TEXT NOT NULL
                    CHECK(length(candidate_bindings_sha256)=64),
                candidate_authority_sha256 TEXT NOT NULL
                    CHECK(length(candidate_authority_sha256)=64),
                coordinator_authority_sha256 TEXT NOT NULL
                    CHECK(length(coordinator_authority_sha256)=64),
                source_view_sha256 TEXT NOT NULL
                    CHECK(length(source_view_sha256)=64),
                candidate_count INTEGER NOT NULL CHECK(candidate_count>0),
                releasable_bytes INTEGER NOT NULL CHECK(releasable_bytes>=0),
                releasable_entries INTEGER NOT NULL CHECK(releasable_entries>=0),
                prepared_at REAL NOT NULL
            ) WITHOUT ROWID
            """,
            """
            CREATE TABLE raw_artifact_reclamation_candidates(
                request_sha256 TEXT NOT NULL REFERENCES
                    raw_artifact_reclamation_prepares(request_sha256),
                candidate_ordinal INTEGER NOT NULL CHECK(candidate_ordinal>=0),
                binding_sha256 TEXT NOT NULL CHECK(length(binding_sha256)=64),
                candidate_sha256 TEXT NOT NULL CHECK(length(candidate_sha256)=64),
                acquisition_job_id TEXT NOT NULL,
                scheduler_job_id TEXT NOT NULL,
                record_sha256 TEXT NOT NULL CHECK(length(record_sha256)=64),
                segment_sha256 TEXT NOT NULL CHECK(length(segment_sha256)=64),
                metadata_reference BLOB NOT NULL CHECK(
                    length(metadata_reference)>0 AND
                    length(metadata_reference)<=65536
                ),
                metadata_reference_sha256 TEXT NOT NULL
                    CHECK(length(metadata_reference_sha256)=64),
                metadata_sha256 TEXT NOT NULL CHECK(length(metadata_sha256)=64),
                archive_object_count INTEGER NOT NULL CHECK(archive_object_count>=0),
                archive_object_reference_root_sha256 TEXT NOT NULL
                    CHECK(length(archive_object_reference_root_sha256)=64),
                archive_object_total_bytes INTEGER NOT NULL
                    CHECK(archive_object_total_bytes>=0),
                downstream_lifecycle_view_sha256 TEXT NOT NULL
                    CHECK(length(downstream_lifecycle_view_sha256)=64),
                execution_receipts_view_sha256 TEXT NOT NULL
                    CHECK(length(execution_receipts_view_sha256)=64),
                scheduler_view_sha256 TEXT NOT NULL
                    CHECK(length(scheduler_view_sha256)=64),
                raw_artifacts_view_sha256 TEXT NOT NULL
                    CHECK(length(raw_artifacts_view_sha256)=64),
                capacity_ledger_view_sha256 TEXT NOT NULL
                    CHECK(length(capacity_ledger_view_sha256)=64),
                capture_id TEXT NOT NULL,
                capture_receipt_sha256 TEXT NOT NULL
                    CHECK(length(capture_receipt_sha256)=64),
                provider TEXT NOT NULL,
                product TEXT NOT NULL,
                fetch_id TEXT NOT NULL,
                upstream_capture_id TEXT NOT NULL,
                manifest_sha256 TEXT NOT NULL CHECK(length(manifest_sha256)=64),
                evidence_stream_id TEXT NOT NULL,
                evidence_stream_sequence INTEGER NOT NULL
                    CHECK(evidence_stream_sequence>0),
                consumer_closure_receipt_sha256 TEXT NOT NULL
                    CHECK(length(consumer_closure_receipt_sha256)=64),
                consumer_closure_root_sha256 TEXT NOT NULL
                    CHECK(length(consumer_closure_root_sha256)=64),
                PRIMARY KEY(request_sha256, candidate_ordinal),
                UNIQUE(request_sha256, binding_sha256),
                UNIQUE(request_sha256, candidate_sha256),
                UNIQUE(request_sha256, capture_id)
            ) WITHOUT ROWID
            """,
            """
            CREATE TABLE raw_artifact_reclamation_capture_members(
                request_sha256 TEXT NOT NULL,
                capture_id TEXT NOT NULL,
                role TEXT NOT NULL,
                receipt_id TEXT NOT NULL,
                receipt_sha256 TEXT NOT NULL CHECK(length(receipt_sha256)=64),
                content_sha256 TEXT NOT NULL CHECK(length(content_sha256)=64),
                size_bytes INTEGER NOT NULL CHECK(size_bytes>=0),
                PRIMARY KEY(request_sha256, capture_id, role),
                UNIQUE(request_sha256, capture_id, receipt_id),
                FOREIGN KEY(request_sha256, capture_id) REFERENCES
                    raw_artifact_reclamation_candidates(request_sha256, capture_id)
            ) WITHOUT ROWID
            """,
            """
            CREATE TABLE raw_artifact_reclamation_receipt_references(
                request_sha256 TEXT NOT NULL REFERENCES
                    raw_artifact_reclamation_prepares(request_sha256),
                receipt_id TEXT NOT NULL,
                provider TEXT NOT NULL,
                product TEXT NOT NULL,
                fetch_id TEXT NOT NULL,
                upstream_id TEXT NOT NULL,
                receipt_sha256 TEXT NOT NULL CHECK(length(receipt_sha256)=64),
                content_sha256 TEXT NOT NULL CHECK(length(content_sha256)=64),
                size_bytes INTEGER NOT NULL CHECK(size_bytes>=0),
                total_capture_references INTEGER NOT NULL
                    CHECK(total_capture_references>=0),
                candidate_capture_references INTEGER NOT NULL CHECK(
                    candidate_capture_references>=0 AND
                    candidate_capture_references<=total_capture_references
                ),
                delete_eligible INTEGER NOT NULL CHECK(delete_eligible IN (0,1)),
                PRIMARY KEY(request_sha256, receipt_id)
            ) WITHOUT ROWID
            """,
            """
            CREATE TABLE raw_artifact_reclamation_object_references(
                request_sha256 TEXT NOT NULL REFERENCES
                    raw_artifact_reclamation_prepares(request_sha256),
                content_sha256 TEXT NOT NULL CHECK(length(content_sha256)=64),
                relative_path TEXT NOT NULL,
                size_bytes INTEGER NOT NULL CHECK(size_bytes>=0),
                total_receipt_references INTEGER NOT NULL
                    CHECK(total_receipt_references>0),
                delete_receipt_references INTEGER NOT NULL CHECK(
                    delete_receipt_references>=0 AND
                    delete_receipt_references<=total_receipt_references
                ),
                delete_eligible INTEGER NOT NULL CHECK(delete_eligible IN (0,1)),
                PRIMARY KEY(request_sha256, content_sha256),
                CHECK(relative_path='objects/' || content_sha256 || '.raw')
            ) WITHOUT ROWID
            """,
            """
            CREATE TABLE raw_artifact_reclamation_catalog_applies(
                request_sha256 TEXT PRIMARY KEY REFERENCES
                    raw_artifact_reclamation_prepares(request_sha256),
                prepare_receipt_sha256 TEXT NOT NULL
                    CHECK(length(prepare_receipt_sha256)=64),
                committed_decision_sha256 TEXT NOT NULL
                    CHECK(length(committed_decision_sha256)=64),
                committed_decision_receipt_sha256 TEXT NOT NULL UNIQUE
                    CHECK(length(committed_decision_receipt_sha256)=64),
                committed_decision_json TEXT NOT NULL,
                predecessor_apply_receipt_sha256 TEXT NOT NULL
                    CHECK(length(predecessor_apply_receipt_sha256)=64),
                catalog_apply_sha256 TEXT NOT NULL UNIQUE
                    CHECK(length(catalog_apply_sha256)=64),
                shared_reference_root_sha256 TEXT NOT NULL
                    CHECK(length(shared_reference_root_sha256)=64),
                unlink_item_root_sha256 TEXT NOT NULL
                    CHECK(length(unlink_item_root_sha256)=64),
                unlink_item_count INTEGER NOT NULL CHECK(unlink_item_count>=0),
                unlink_bytes INTEGER NOT NULL CHECK(unlink_bytes>=0),
                files_pending_at REAL NOT NULL,
                state TEXT NOT NULL CHECK(state='FILES_PENDING')
            ) WITHOUT ROWID
            """,
            """
            CREATE TABLE raw_artifact_reclamation_unlink_intents(
                request_sha256 TEXT NOT NULL REFERENCES
                    raw_artifact_reclamation_catalog_applies(request_sha256),
                unlink_ordinal INTEGER NOT NULL CHECK(unlink_ordinal>=0),
                intent_sha256 TEXT NOT NULL UNIQUE CHECK(length(intent_sha256)=64),
                content_sha256 TEXT NOT NULL CHECK(length(content_sha256)=64),
                relative_path TEXT NOT NULL,
                size_bytes INTEGER NOT NULL CHECK(size_bytes>=0),
                st_dev INTEGER NOT NULL CHECK(st_dev>=0),
                st_ino INTEGER NOT NULL CHECK(st_ino>0),
                st_mode INTEGER NOT NULL CHECK(st_mode>0),
                st_nlink INTEGER NOT NULL CHECK(st_nlink=1),
                PRIMARY KEY(request_sha256, unlink_ordinal),
                UNIQUE(request_sha256, content_sha256),
                UNIQUE(request_sha256, unlink_ordinal, intent_sha256),
                CHECK(relative_path='objects/' || content_sha256 || '.raw')
            ) WITHOUT ROWID
            """,
            """
            CREATE TABLE raw_artifact_reclamation_unlink_failures(
                request_sha256 TEXT PRIMARY KEY REFERENCES
                    raw_artifact_reclamation_catalog_applies(request_sha256),
                unlink_ordinal INTEGER NOT NULL CHECK(unlink_ordinal>=0),
                intent_sha256 TEXT NOT NULL CHECK(length(intent_sha256)=64),
                phase TEXT NOT NULL CHECK(
                    phase IN ('PRE_UNLINK','POST_UNLINK')
                ),
                observed_st_dev INTEGER NOT NULL CHECK(observed_st_dev>=0),
                observed_st_ino INTEGER NOT NULL CHECK(observed_st_ino>0),
                observed_st_mode INTEGER NOT NULL CHECK(observed_st_mode>0),
                observed_st_nlink INTEGER NOT NULL CHECK(observed_st_nlink>0),
                observed_st_size INTEGER NOT NULL CHECK(observed_st_size>=0),
                object_path_absent INTEGER NOT NULL
                    CHECK(object_path_absent IN (0,1)),
                failure_sha256 TEXT NOT NULL UNIQUE
                    CHECK(length(failure_sha256)=64),
                observed_at REAL NOT NULL,
                state TEXT NOT NULL CHECK(state='FAILED_FENCED'),
                FOREIGN KEY(request_sha256, unlink_ordinal, intent_sha256)
                    REFERENCES raw_artifact_reclamation_unlink_intents(
                        request_sha256, unlink_ordinal, intent_sha256
                    )
            ) WITHOUT ROWID
            """,
            """
            CREATE TABLE raw_artifact_reclamation_unlink_durable(
                request_sha256 TEXT NOT NULL,
                unlink_ordinal INTEGER NOT NULL,
                intent_sha256 TEXT NOT NULL CHECK(length(intent_sha256)=64),
                predecessor_unlink_sha256 TEXT CHECK(
                    predecessor_unlink_sha256 IS NULL OR
                    length(predecessor_unlink_sha256)=64
                ),
                directory_barrier_sha256 TEXT NOT NULL
                    CHECK(length(directory_barrier_sha256)=64),
                directory_st_dev INTEGER NOT NULL CHECK(directory_st_dev>=0),
                directory_st_ino INTEGER NOT NULL CHECK(directory_st_ino>=0),
                directory_st_mode INTEGER NOT NULL CHECK(directory_st_mode>0),
                object_was_present INTEGER NOT NULL
                    CHECK(object_was_present IN (0,1)),
                unlink_event_sha256 TEXT NOT NULL UNIQUE
                    CHECK(length(unlink_event_sha256)=64),
                unlinked_at REAL NOT NULL,
                state TEXT NOT NULL CHECK(state='UNLINKED_DURABLE'),
                PRIMARY KEY(request_sha256, unlink_ordinal),
                FOREIGN KEY(request_sha256, unlink_ordinal, intent_sha256)
                    REFERENCES raw_artifact_reclamation_unlink_intents(
                        request_sha256, unlink_ordinal, intent_sha256
                    ),
                CHECK(
                    (unlink_ordinal=0 AND predecessor_unlink_sha256 IS NULL) OR
                    (unlink_ordinal>0 AND predecessor_unlink_sha256 IS NOT NULL)
                )
            ) WITHOUT ROWID
            """,
            """
            CREATE TABLE raw_artifact_reclamation_unlinked_batches(
                request_sha256 TEXT PRIMARY KEY REFERENCES
                    raw_artifact_reclamation_catalog_applies(request_sha256),
                unlink_item_root_sha256 TEXT NOT NULL
                    CHECK(length(unlink_item_root_sha256)=64),
                directory_barrier_root_sha256 TEXT NOT NULL
                    CHECK(length(directory_barrier_root_sha256)=64),
                last_unlink_event_sha256 TEXT CHECK(
                    last_unlink_event_sha256 IS NULL OR
                    length(last_unlink_event_sha256)=64
                ),
                unlinked_durable_sha256 TEXT NOT NULL UNIQUE
                    CHECK(length(unlinked_durable_sha256)=64),
                unlink_item_count INTEGER NOT NULL CHECK(unlink_item_count>=0),
                unlinked_bytes INTEGER NOT NULL CHECK(unlinked_bytes>=0),
                completed_at REAL NOT NULL,
                state TEXT NOT NULL CHECK(state='UNLINKED_DURABLE'),
                CHECK(
                    (unlink_item_count=0 AND last_unlink_event_sha256 IS NULL) OR
                    (unlink_item_count>0 AND last_unlink_event_sha256 IS NOT NULL)
                )
            ) WITHOUT ROWID
            """,
            """
            CREATE TABLE raw_artifact_reclamation_applies(
                request_sha256 TEXT PRIMARY KEY REFERENCES
                    raw_artifact_reclamation_unlinked_batches(request_sha256),
                apply_receipt_sha256 TEXT NOT NULL UNIQUE
                    CHECK(length(apply_receipt_sha256)=64),
                apply_receipt_json TEXT NOT NULL,
                journal_receipt_sha256 TEXT NOT NULL UNIQUE
                    CHECK(length(journal_receipt_sha256)=64),
                journal_receipt_json TEXT NOT NULL,
                effect_sha256 TEXT NOT NULL CHECK(length(effect_sha256)=64),
                post_apply_view_sha256 TEXT NOT NULL
                    CHECK(length(post_apply_view_sha256)=64),
                released_bytes INTEGER NOT NULL CHECK(released_bytes>=0),
                released_entries INTEGER NOT NULL CHECK(released_entries>=0),
                applied_at REAL NOT NULL,
                state TEXT NOT NULL CHECK(state='APPLIED')
            ) WITHOUT ROWID
            """,
            """
            CREATE TABLE raw_artifact_reclamation_aborts(
                request_sha256 TEXT PRIMARY KEY REFERENCES
                    raw_artifact_reclamation_prepares(request_sha256),
                abort_receipt_sha256 TEXT NOT NULL UNIQUE
                    CHECK(length(abort_receipt_sha256)=64),
                abort_receipt_json TEXT NOT NULL,
                coordinator_abort_decision_sha256 TEXT NOT NULL
                    CHECK(length(coordinator_abort_decision_sha256)=64),
                coordinator_abort_receipt_sha256 TEXT NOT NULL UNIQUE
                    CHECK(length(coordinator_abort_receipt_sha256)=64),
                coordinator_abort_receipt_json TEXT NOT NULL,
                fence_release_root_sha256 TEXT NOT NULL
                    CHECK(length(fence_release_root_sha256)=64),
                aborted_at REAL NOT NULL,
                state TEXT NOT NULL CHECK(state='ABORTED')
            ) WITHOUT ROWID
            """,
            """
            CREATE TABLE raw_artifact_reclamation_capture_tombstones(
                capture_id TEXT PRIMARY KEY,
                provider TEXT NOT NULL,
                product TEXT NOT NULL,
                fetch_id TEXT NOT NULL,
                upstream_capture_id TEXT NOT NULL,
                manifest_sha256 TEXT NOT NULL CHECK(length(manifest_sha256)=64),
                capture_receipt_sha256 TEXT NOT NULL
                    CHECK(length(capture_receipt_sha256)=64),
                request_sha256 TEXT NOT NULL REFERENCES
                    raw_artifact_reclamation_catalog_applies(request_sha256),
                catalog_apply_sha256 TEXT NOT NULL
                    CHECK(length(catalog_apply_sha256)=64),
                tombstone_sha256 TEXT NOT NULL UNIQUE
                    CHECK(length(tombstone_sha256)=64),
                created_at REAL NOT NULL,
                UNIQUE(provider, product, fetch_id),
                UNIQUE(provider, product, upstream_capture_id)
            ) WITHOUT ROWID
            """,
            """
            CREATE TABLE raw_artifact_reclamation_receipt_tombstones(
                receipt_id TEXT PRIMARY KEY,
                provider TEXT NOT NULL,
                product TEXT NOT NULL,
                fetch_id TEXT NOT NULL,
                upstream_id TEXT NOT NULL,
                receipt_sha256 TEXT NOT NULL CHECK(length(receipt_sha256)=64),
                request_sha256 TEXT NOT NULL REFERENCES
                    raw_artifact_reclamation_catalog_applies(request_sha256),
                catalog_apply_sha256 TEXT NOT NULL
                    CHECK(length(catalog_apply_sha256)=64),
                tombstone_sha256 TEXT NOT NULL UNIQUE
                    CHECK(length(tombstone_sha256)=64),
                created_at REAL NOT NULL,
                UNIQUE(provider, product, fetch_id),
                UNIQUE(provider, product, upstream_id)
            ) WITHOUT ROWID
            """,
        )
        for statement in statements:
            database.execute(statement)

        database.execute(
            """
            INSERT INTO raw_artifact_reclamation_metadata(
                metadata_key, extension_schema_version, base_schema_version,
                extension_schema, store_instance_nonce,
                source_store_identity_sha256, store_profile_sha256, created_at
            ) VALUES('reclamation_extension',?,?,?,?,?,?,?)
            """,
            (
                RAW_ARTIFACT_RECLAMATION_EXTENSION_SCHEMA_VERSION,
                RAW_ARTIFACT_SCHEMA_VERSION,
                RAW_ARTIFACT_RECLAMATION_EXTENSION_SCHEMA,
                nonce,
                identity_sha256,
                profile_sha256,
                created_at,
            ),
        )
        database.execute(
            "CREATE INDEX idx_raw_artifact_reclamation_candidates_capture "
            "ON raw_artifact_reclamation_candidates(capture_id,request_sha256)"
        )
        database.execute(
            "CREATE INDEX idx_raw_artifact_reclamation_members_receipt "
            "ON raw_artifact_reclamation_capture_members(receipt_id,request_sha256)"
        )
        database.execute(
            "CREATE INDEX idx_raw_artifact_reclamation_receipts_content "
            "ON raw_artifact_reclamation_receipt_references("
            "content_sha256,request_sha256)"
        )

        for table in sorted(cls._RECLAMATION_TABLES):
            database.execute(
                f"CREATE TRIGGER {table}_no_update BEFORE UPDATE ON {table} "
                f"BEGIN SELECT RAISE(ABORT,'{table} is immutable'); END"
            )
            database.execute(
                f"CREATE TRIGGER {table}_no_delete BEFORE DELETE ON {table} "
                f"BEGIN SELECT RAISE(ABORT,'{table} is immutable'); END"
            )

        database.execute(
            """
            CREATE TRIGGER raw_artifact_reclamation_capture_identity_guard
            BEFORE INSERT ON raw_artifact_captures
            WHEN EXISTS(
                    SELECT 1
                    FROM raw_artifact_reclamation_capture_tombstones AS tombstone
                    WHERE tombstone.capture_id=NEW.capture_id
                       OR (
                           tombstone.provider=NEW.provider AND
                           tombstone.product=NEW.product AND
                           (
                               tombstone.fetch_id=NEW.fetch_id OR
                               tombstone.upstream_capture_id=NEW.upstream_capture_id
                           )
                       )
                 )
                 OR EXISTS(
                    SELECT 1
                    FROM raw_artifact_reclamation_candidates AS candidate
                    LEFT JOIN raw_artifact_reclamation_aborts AS aborted
                      ON aborted.request_sha256=candidate.request_sha256
                    LEFT JOIN raw_artifact_reclamation_applies AS applied
                      ON applied.request_sha256=candidate.request_sha256
                    WHERE aborted.request_sha256 IS NULL
                      AND applied.request_sha256 IS NULL
                      AND (
                          candidate.capture_id=NEW.capture_id OR
                          (
                              candidate.provider=NEW.provider AND
                              candidate.product=NEW.product AND
                              (
                                  candidate.fetch_id=NEW.fetch_id OR
                                  candidate.upstream_capture_id=
                                      NEW.upstream_capture_id
                              )
                          )
                      )
                 )
            BEGIN SELECT RAISE(ABORT,'raw capture identity is tombstoned'); END
            """
        )
        database.execute(
            """
            CREATE TRIGGER raw_artifact_reclamation_receipt_identity_guard
            BEFORE INSERT ON raw_artifact_receipts
            WHEN EXISTS(
                SELECT 1
                FROM raw_artifact_reclamation_receipt_tombstones AS tombstone
                WHERE tombstone.receipt_id=NEW.receipt_id
                   OR (
                       tombstone.provider=NEW.provider AND
                       tombstone.product=NEW.product AND
                       (
                           tombstone.fetch_id=NEW.fetch_id OR
                           tombstone.upstream_id=NEW.upstream_id
                       )
                   )
            )
            BEGIN SELECT RAISE(ABORT,'raw receipt identity is tombstoned'); END
            """
        )
        database.execute(
            """
            CREATE TRIGGER raw_artifact_reclamation_catalog_apply_not_aborted
            BEFORE INSERT ON raw_artifact_reclamation_catalog_applies
            WHEN EXISTS(
                SELECT 1 FROM raw_artifact_reclamation_aborts
                WHERE request_sha256=NEW.request_sha256
            )
            BEGIN SELECT RAISE(ABORT,'aborted raw prepare cannot apply'); END
            """
        )
        database.execute(
            """
            CREATE TRIGGER raw_artifact_reclamation_abort_before_files_pending
            BEFORE INSERT ON raw_artifact_reclamation_aborts
            WHEN EXISTS(
                SELECT 1 FROM raw_artifact_reclamation_catalog_applies
                WHERE request_sha256=NEW.request_sha256
            )
            BEGIN SELECT RAISE(ABORT,'raw FILES_PENDING cannot abort'); END
            """
        )
        database.execute(
            """
            CREATE TRIGGER raw_artifact_reclamation_object_catalog_fence
            BEFORE INSERT ON raw_artifact_objects
            WHEN EXISTS(
                SELECT 1
                FROM raw_artifact_reclamation_object_references AS reference
                LEFT JOIN raw_artifact_reclamation_aborts AS aborted
                  ON aborted.request_sha256=reference.request_sha256
                LEFT JOIN raw_artifact_reclamation_applies AS applied
                  ON applied.request_sha256=reference.request_sha256
                WHERE reference.content_sha256=NEW.content_sha256
                  AND aborted.request_sha256 IS NULL
                  AND applied.request_sha256 IS NULL
            )
            BEGIN SELECT RAISE(ABORT,'raw object is fenced for reclamation'); END
            """
        )
        database.execute(
            """
            CREATE TRIGGER raw_artifact_reclamation_receipt_reference_fence
            BEFORE INSERT ON raw_artifact_receipts
            WHEN EXISTS(
                SELECT 1
                FROM raw_artifact_reclamation_object_references AS reference
                LEFT JOIN raw_artifact_reclamation_aborts AS aborted
                  ON aborted.request_sha256=reference.request_sha256
                LEFT JOIN raw_artifact_reclamation_applies AS applied
                  ON applied.request_sha256=reference.request_sha256
                WHERE reference.content_sha256=NEW.content_sha256
                  AND aborted.request_sha256 IS NULL
                  AND applied.request_sha256 IS NULL
            )
            BEGIN SELECT RAISE(ABORT,'raw object is fenced for reclamation'); END
            """
        )
        database.execute(
            """
            CREATE TRIGGER raw_artifact_reclamation_capture_member_fence
            BEFORE INSERT ON raw_artifact_capture_members
            WHEN EXISTS(
                SELECT 1
                FROM raw_artifact_reclamation_receipt_references AS reference
                LEFT JOIN raw_artifact_reclamation_aborts AS aborted
                  ON aborted.request_sha256=reference.request_sha256
                LEFT JOIN raw_artifact_reclamation_applies AS applied
                  ON applied.request_sha256=reference.request_sha256
                WHERE reference.receipt_id=NEW.receipt_id
                  AND aborted.request_sha256 IS NULL
                  AND applied.request_sha256 IS NULL
            )
            BEGIN SELECT RAISE(ABORT,'raw receipt is fenced for reclamation'); END
            """
        )

    def _validate_schema(self, database: sqlite3.Connection) -> None:
        tables = self._table_names(database)
        extension_tables = self._TABLES | self._RECLAMATION_TABLES
        if tables not in (self._TABLES, extension_tables):
            raise RawArtifactIntegrityError(
                "raw-artifactcatalogus heeft onbekend of onvolledig schema"
            )
        reclamation_enabled = tables == extension_tables
        self._validate_schema_inventory(
            database,
            reclamation_enabled=reclamation_enabled,
        )
        self._validate_base_schema(database)
        if reclamation_enabled:
            self._validate_reclamation_extension_schema(database)
        integrity = [str(row[0]) for row in database.execute("PRAGMA integrity_check")]
        if integrity != ["ok"]:
            raise RawArtifactIntegrityError("raw-artifactcatalogus faalt integrity_check")
        if database.execute("PRAGMA foreign_key_check").fetchone() is not None:
            raise RawArtifactIntegrityError("raw-artifactcatalogus faalt foreign keys")

    def _validate_base_schema(self, database: sqlite3.Connection) -> None:
        for table, expected in self._COLUMNS.items():
            actual = {
                str(row["name"])
                for row in database.execute(f"PRAGMA table_info({table})")
            }
            if actual != expected:
                raise RawArtifactIntegrityError(
                    f"raw-artifacttabel {table} heeft onverwachte kolommen"
                )
        rows = database.execute(
            "SELECT metadata_key, schema_version FROM raw_artifact_metadata"
        ).fetchall()
        if len(rows) != 1 or rows[0]["metadata_key"] != "schema":
            raise RawArtifactIntegrityError("raw-artifactcatalogus mist schema-identiteit")
        if int(rows[0]["schema_version"]) != RAW_ARTIFACT_SCHEMA_VERSION:
            raise RawArtifactIntegrityError("onbekende raw-artifactschemaversie")

    def _validate_reclamation_extension_schema(
        self,
        database: sqlite3.Connection,
    ) -> None:
        for table, expected in self._RECLAMATION_COLUMNS.items():
            actual = {
                str(row["name"])
                for row in database.execute(f"PRAGMA table_info({table})")
            }
            if actual != expected:
                raise RawArtifactIntegrityError(
                    f"raw reclamation-tabel {table} heeft onverwachte kolommen"
                )

        for index, expected in self._RECLAMATION_INDEX_COLUMNS.items():
            row = database.execute(
                "SELECT type,tbl_name,sql FROM sqlite_master WHERE name=?",
                (index,),
            ).fetchone()
            if row is None or str(row["type"]) != "index" or row["sql"] is None:
                raise RawArtifactIntegrityError(
                    f"raw reclamation-index {index} ontbreekt"
                )
            columns = tuple(
                str(item["name"])
                for item in database.execute(f"PRAGMA index_info({index})")
            )
            if columns != expected:
                raise RawArtifactIntegrityError(
                    f"raw reclamation-index {index} heeft onverwachte kolommen"
                )

        expected_triggers = self._reclamation_trigger_names()
        expected_objects = (
            self._RECLAMATION_TABLES
            | frozenset(self._RECLAMATION_INDEX_COLUMNS)
            | expected_triggers
        )
        objects = database.execute(
            """
            SELECT type,name,tbl_name,sql FROM sqlite_master
            WHERE name LIKE 'raw_artifact_reclamation_%'
               OR name LIKE 'idx_raw_artifact_reclamation_%'
            """
        ).fetchall()
        if {str(row["name"]) for row in objects} != expected_objects:
            raise RawArtifactIntegrityError(
                "raw reclamation-extension heeft onbekende of ontbrekende schema-objecten"
            )
        triggers = {
            str(row["name"]): row
            for row in objects
            if str(row["type"]) == "trigger"
        }
        if set(triggers) != expected_triggers:
            raise RawArtifactIntegrityError(
                "raw reclamation-extension heeft onbekende of ontbrekende triggers"
            )
        fixed_targets = {
            "raw_artifact_reclamation_capture_identity_guard": (
                "raw_artifact_captures"
            ),
            "raw_artifact_reclamation_receipt_identity_guard": (
                "raw_artifact_receipts"
            ),
            "raw_artifact_reclamation_catalog_apply_not_aborted": (
                "raw_artifact_reclamation_catalog_applies"
            ),
            "raw_artifact_reclamation_abort_before_files_pending": (
                "raw_artifact_reclamation_aborts"
            ),
            "raw_artifact_reclamation_object_catalog_fence": (
                "raw_artifact_objects"
            ),
            "raw_artifact_reclamation_receipt_reference_fence": (
                "raw_artifact_receipts"
            ),
            "raw_artifact_reclamation_capture_member_fence": (
                "raw_artifact_capture_members"
            ),
        }
        for name, row in triggers.items():
            if name in fixed_targets:
                expected_target = fixed_targets[name]
            elif name.endswith("_no_update"):
                expected_target = name.removesuffix("_no_update")
            elif name.endswith("_no_delete"):
                expected_target = name.removesuffix("_no_delete")
            else:  # pragma: no cover - exact trigger inventory established
                raise RawArtifactIntegrityError(
                    "raw reclamation-extension heeft onbekende trigger"
                )
            if (
                str(row["tbl_name"]) != expected_target
                or row["sql"] is None
                or not str(row["sql"]).strip()
            ):
                raise RawArtifactIntegrityError(
                    f"raw reclamation-trigger {name} heeft onverwachte authority"
                )
        self._load_reclamation_store_identity(database)

    @classmethod
    def _reclamation_trigger_names(cls) -> frozenset[str]:
        immutable = {
            f"{table}_{operation}"
            for table in cls._RECLAMATION_TABLES
            for operation in ("no_update", "no_delete")
        }
        immutable.update({
            "raw_artifact_reclamation_capture_identity_guard",
            "raw_artifact_reclamation_receipt_identity_guard",
            "raw_artifact_reclamation_catalog_apply_not_aborted",
            "raw_artifact_reclamation_abort_before_files_pending",
            "raw_artifact_reclamation_object_catalog_fence",
            "raw_artifact_reclamation_receipt_reference_fence",
            "raw_artifact_reclamation_capture_member_fence",
        })
        return frozenset(immutable)

    def _require_reclamation_extension(
        self,
        database: sqlite3.Connection,
    ) -> None:
        tables = self._table_names(database)
        if tables == self._TABLES:
            raise RuntimeError(
                "raw reclamation-extension v1 vereist expliciete initialisatie"
            )
        if tables != self._TABLES | self._RECLAMATION_TABLES:
            raise RawArtifactIntegrityError(
                "raw-artifactcatalogus heeft onbekend of onvolledig schema"
            )
        # Every reclamation entrypoint re-establishes both the exact legacy-v3
        # authority and the complete additive extension.  Column equality alone
        # cannot prove FK/CHECK/UNIQUE, index order, or immutable trigger bodies.
        self._validate_schema(database)

    @staticmethod
    def _reclamation_store_identity(*, nonce: str, profile_sha256: str) -> str:
        if (
            _SHA256_RE.fullmatch(nonce) is None
            or _SHA256_RE.fullmatch(profile_sha256) is None
        ):
            raise RawArtifactIntegrityError(
                "raw reclamation store-instance facts zijn ongeldig"
            )
        return reclamation_digest_mapping({
            "base_schema_version": RAW_ARTIFACT_SCHEMA_VERSION,
            "extension_schema": RAW_ARTIFACT_RECLAMATION_EXTENSION_SCHEMA,
            "extension_schema_version": (
                RAW_ARTIFACT_RECLAMATION_EXTENSION_SCHEMA_VERSION
            ),
            "schema": RAW_ARTIFACT_RECLAMATION_STORE_IDENTITY_SCHEMA,
            "store_instance_nonce": nonce,
            "store_profile_sha256": profile_sha256,
        })

    def _load_reclamation_store_identity(
        self,
        database: sqlite3.Connection,
    ) -> str:
        rows = database.execute(
            "SELECT * FROM raw_artifact_reclamation_metadata"
        ).fetchall()
        if len(rows) != 1:
            raise RawArtifactIntegrityError(
                "raw reclamation-extension mist zijn singleton-identiteit"
            )
        row = rows[0]
        if (
            str(row["metadata_key"]) != "reclamation_extension"
            or int(row["extension_schema_version"])
            != RAW_ARTIFACT_RECLAMATION_EXTENSION_SCHEMA_VERSION
            or int(row["base_schema_version"]) != RAW_ARTIFACT_SCHEMA_VERSION
            or str(row["extension_schema"])
            != RAW_ARTIFACT_RECLAMATION_EXTENSION_SCHEMA
        ):
            raise RawArtifactIntegrityError(
                "raw reclamation-extension heeft onbekende schema-identiteit"
            )
        nonce = str(row["store_instance_nonce"])
        profile_sha256 = str(row["store_profile_sha256"])
        identity_sha256 = str(row["source_store_identity_sha256"])
        expected_profile = hashlib.sha256(
            self._store_profile_bytes()
        ).hexdigest()
        if profile_sha256 != expected_profile:
            raise RawArtifactConflictError(
                "raw reclamation store-instance bindt een ander storeprofiel"
            )
        if identity_sha256 != self._reclamation_store_identity(
            nonce=nonce,
            profile_sha256=profile_sha256,
        ):
            raise RawArtifactIntegrityError(
                "raw reclamation store-instance checksum klopt niet"
            )
        self._timestamp(row["created_at"], "raw reclamation created_at")
        return identity_sha256

    @staticmethod
    def _candidate_binding_from_row(
        row: sqlite3.Row,
    ) -> SourceReclamationCandidateBindingV1:
        return SourceReclamationCandidateBindingV1(
            binding_sha256=str(row["binding_sha256"]),
            candidate_sha256=str(row["candidate_sha256"]),
            acquisition_job_id=str(row["acquisition_job_id"]),
            scheduler_job_id=str(row["scheduler_job_id"]),
            raw_capture_id=str(row["capture_id"]),
            evidence_stream_id=str(row["evidence_stream_id"]),
            evidence_stream_sequence=int(row["evidence_stream_sequence"]),
            sealed_archive_segment_sha256=str(row["segment_sha256"]),
            sealed_archive_record_sha256=str(row["record_sha256"]),
            consumer_closure_receipt_sha256=str(
                row["consumer_closure_receipt_sha256"]
            ),
            consumer_closure_root_sha256=str(
                row["consumer_closure_root_sha256"]
            ),
        )

    def _load_reclamation_prepare(
        self,
        database: sqlite3.Connection,
        request_sha256: str,
    ) -> SourceLocalPrepareReceiptV2 | None:
        row = database.execute(
            "SELECT * FROM raw_artifact_reclamation_prepares "
            "WHERE request_sha256=?",
            (request_sha256,),
        ).fetchone()
        if row is None:
            return None
        request = self._protocol_receipt_from_json(
            str(row["request_json"]),
            SourceReclamationRequestV2,
        )
        receipt = self._protocol_receipt_from_json(
            str(row["prepare_receipt_json"]),
            SourceLocalPrepareReceiptV2,
        )
        if not isinstance(request, SourceReclamationRequestV2) or not isinstance(
            receipt,
            SourceLocalPrepareReceiptV2,
        ):
            raise RawArtifactIntegrityError(
                "raw reclamation preparereceipt heeft verkeerd type"
            )
        store_identity = self._load_reclamation_store_identity(database)
        if (
            request.request_sha256 != request_sha256
            or receipt.request_sha256 != request_sha256
            or str(row["prepare_receipt_sha256"])
            != receipt.prepare_receipt_sha256
            or str(row["source_fence_sha256"]) != receipt.source_fence_sha256
            or str(row["child_reference_root_sha256"])
            != receipt.child_reference_root_sha256
            or str(row["candidate_bindings_sha256"])
            != receipt.candidate_bindings_sha256
            or str(row["candidate_authority_sha256"])
            != receipt.candidate_authority_sha256
            or str(row["coordinator_authority_sha256"])
            != receipt.coordinator_authority_sha256
            or str(row["source_view_sha256"]) != receipt.source_view_sha256
            or int(row["candidate_count"]) != receipt.candidate_count
            or int(row["releasable_bytes"]) != receipt.releasable_bytes
            or int(row["releasable_entries"]) != receipt.releasable_entries
            or float(row["prepared_at"]) != receipt.prepared_at
            or receipt.role != self.role
            or receipt.source_store_identity_sha256 != store_identity
            or receipt.candidate_count != request.candidate_count
            or receipt.candidate_authority_sha256
            != request.candidate_authority_sha256
            or receipt.sealed_archive_anchor_sha256
            != request.sealed_archive_anchor_sha256
            or receipt.archive_binding_root_sha256
            != request.archive_binding_root_sha256
            or receipt.candidate_bindings_sha256
            != request.candidate_bindings_sha256
            or receipt.consumer_registry_root_sha256
            != request.consumer_registry_root_sha256
            or receipt.consumer_epoch_root_sha256
            != request.consumer_epoch_root_sha256
        ):
            raise RawArtifactIntegrityError(
                "raw reclamation preparebindings verschillen"
            )

        candidates = database.execute(
            "SELECT * FROM raw_artifact_reclamation_candidates "
            "WHERE request_sha256=? ORDER BY candidate_ordinal",
            (request_sha256,),
        ).fetchall()
        if (
            len(candidates) != receipt.candidate_count
            or [int(item["candidate_ordinal"]) for item in candidates]
            != list(range(receipt.candidate_count))
        ):
            raise RawArtifactIntegrityError(
                "raw reclamation candidateinventory verschilt"
            )
        bindings: list[SourceReclamationCandidateBindingV1] = []
        metadata_references: list[bytes] = []
        for candidate in candidates:
            binding = self._candidate_binding_from_row(candidate)
            metadata_reference = bytes(candidate["metadata_reference"])
            try:
                reference = json.loads(metadata_reference.decode("ascii"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise RawArtifactIntegrityError(
                    "raw reclamation metadata-reference is corrupt"
                ) from exc
            if not isinstance(reference, dict):
                raise RawArtifactIntegrityError(
                    "raw reclamation metadata-reference is geen mapping"
                )
            # The public helpers re-establish the exact closed-key contract.
            for role in (
                "downstream_lifecycle",
                "execution_receipts",
                "scheduler",
                "raw_artifacts",
                "capacity_ledger",
            ):
                candidate_participant_view_sha256(reference, role)
            if (
                len(metadata_reference) > RECLAMATION_MAX_REFERENCE_BYTES
                or hashlib.sha256(metadata_reference).hexdigest()
                != str(candidate["metadata_reference_sha256"])
                or str(reference.get("metadata_sha256"))
                != str(candidate["metadata_sha256"])
                or int(reference.get("archive_object_count", -1))
                != int(candidate["archive_object_count"])
                or str(reference.get("archive_object_reference_root_sha256"))
                != str(candidate["archive_object_reference_root_sha256"])
                or int(reference.get("archive_object_total_bytes", -1))
                != int(candidate["archive_object_total_bytes"])
                or str(reference.get("downstream_lifecycle_view_sha256"))
                != str(candidate["downstream_lifecycle_view_sha256"])
                or str(reference.get("execution_receipts_view_sha256"))
                != str(candidate["execution_receipts_view_sha256"])
                or str(reference.get("scheduler_view_sha256"))
                != str(candidate["scheduler_view_sha256"])
                or str(reference.get("raw_artifacts_view_sha256"))
                != str(candidate["raw_artifacts_view_sha256"])
                or str(reference.get("capacity_ledger_view_sha256"))
                != str(candidate["capacity_ledger_view_sha256"])
            ):
                raise RawArtifactIntegrityError(
                    "raw reclamation metadata-referencefacts verschillen"
                )
            bindings.append(binding)
            metadata_references.append(metadata_reference)
        binding_tuple = tuple(bindings)
        metadata_tuple = tuple(metadata_references)
        if (
            reclamation_candidate_binding_root(binding_tuple)
            != request.candidate_bindings_sha256
            or reclamation_archive_binding_root(binding_tuple, metadata_tuple)
            != request.archive_binding_root_sha256
        ):
            raise RawArtifactIntegrityError(
                "raw reclamation candidate roots verschillen"
            )
        self._validate_persisted_reclamation_prepare_plan(
            database,
            request=request,
            receipt=receipt,
            candidates=tuple(candidates),
        )
        return receipt

    def _validate_persisted_reclamation_prepare_plan(
        self,
        database: sqlite3.Connection,
        *,
        request: SourceReclamationRequestV2,
        receipt: SourceLocalPrepareReceiptV2,
        candidates: tuple[sqlite3.Row, ...],
    ) -> None:
        request_sha256 = request.request_sha256
        members = database.execute(
            "SELECT * FROM raw_artifact_reclamation_capture_members "
            "WHERE request_sha256=? ORDER BY capture_id,role",
            (request_sha256,),
        ).fetchall()
        receipt_rows = database.execute(
            "SELECT * FROM raw_artifact_reclamation_receipt_references "
            "WHERE request_sha256=? ORDER BY receipt_id",
            (request_sha256,),
        ).fetchall()
        object_rows = database.execute(
            "SELECT * FROM raw_artifact_reclamation_object_references "
            "WHERE request_sha256=? ORDER BY content_sha256",
            (request_sha256,),
        ).fetchall()
        candidate_by_capture = {
            str(row["capture_id"]): row for row in candidates
        }
        receipt_by_id = {
            str(row["receipt_id"]): row for row in receipt_rows
        }
        object_by_content = {
            str(row["content_sha256"]): row for row in object_rows
        }
        if (
            len(candidate_by_capture) != len(candidates)
            or len(receipt_by_id) != len(receipt_rows)
            or len(object_by_content) != len(object_rows)
            or not receipt_rows
            or not object_rows
        ):
            raise RawArtifactIntegrityError(
                "raw reclamation persisted referenceinventory verschilt"
            )
        member_counts_by_capture: dict[str, int] = {}
        member_counts_by_receipt: dict[str, int] = {}
        for member in members:
            capture_id = str(member["capture_id"])
            receipt_id = str(member["receipt_id"])
            candidate = candidate_by_capture.get(capture_id)
            reference = receipt_by_id.get(receipt_id)
            if (
                candidate is None
                or reference is None
                or str(member["receipt_sha256"])
                != str(reference["receipt_sha256"])
                or str(member["content_sha256"])
                != str(reference["content_sha256"])
                or int(member["size_bytes"]) != int(reference["size_bytes"])
            ):
                raise RawArtifactIntegrityError(
                    "raw reclamation persisted capture-member verschilt"
                )
            member_counts_by_capture[capture_id] = (
                member_counts_by_capture.get(capture_id, 0) + 1
            )
            member_counts_by_receipt[receipt_id] = (
                member_counts_by_receipt.get(receipt_id, 0) + 1
            )
        if any(
            member_counts_by_capture.get(capture_id, 0) < 1
            for capture_id in candidate_by_capture
        ):
            raise RawArtifactIntegrityError(
                "raw reclamation persisted capture heeft geen leden"
            )

        delete_receipts_by_content: dict[str, int] = {}
        referenced_contents: set[str] = set()
        for receipt_id, reference in receipt_by_id.items():
            candidate_references = int(
                reference["candidate_capture_references"]
            )
            total_references = int(reference["total_capture_references"])
            delete_eligible = int(reference["delete_eligible"])
            content_sha256 = str(reference["content_sha256"])
            referenced_contents.add(content_sha256)
            object_reference = object_by_content.get(content_sha256)
            if (
                candidate_references != member_counts_by_receipt.get(
                    receipt_id,
                    0,
                )
                or candidate_references < 1
                or candidate_references > total_references
                or delete_eligible
                != int(candidate_references == total_references)
                or object_reference is None
                or int(object_reference["size_bytes"])
                != int(reference["size_bytes"])
            ):
                raise RawArtifactIntegrityError(
                    "raw reclamation persisted receipt-referencecount verschilt"
                )
            if delete_eligible:
                delete_receipts_by_content[content_sha256] = (
                    delete_receipts_by_content.get(content_sha256, 0) + 1
                )
        for content_sha256, reference in object_by_content.items():
            delete_references = int(reference["delete_receipt_references"])
            total_references = int(reference["total_receipt_references"])
            delete_eligible = int(reference["delete_eligible"])
            if (
                delete_references
                != delete_receipts_by_content.get(content_sha256, 0)
                or delete_references > total_references
                or total_references < 1
                or delete_eligible != int(delete_references == total_references)
                or content_sha256 not in referenced_contents
            ):
                raise RawArtifactIntegrityError(
                    "raw reclamation persisted object-referencecount verschilt"
                )

        source_view_sha256 = reclamation_inventory_view_sha256(
            role=self.role,
            candidate_views=tuple(
                (
                    str(candidate["candidate_sha256"]),
                    str(candidate["raw_artifacts_view_sha256"]),
                )
                for candidate in candidates
            ),
        )
        capture_root = self._ordered_reclamation_root(
            schema="stv3-worker-raw-reclamation-capture-reference-root-v1",
            item_sha256s=tuple(
                reclamation_digest_mapping({
                    "capture_id": str(candidate["capture_id"]),
                    "capture_receipt_sha256": str(
                        candidate["capture_receipt_sha256"]
                    ),
                    "candidate_sha256": str(candidate["candidate_sha256"]),
                })
                for candidate in candidates
            ),
        )
        receipt_root = self._ordered_reclamation_root(
            schema="stv3-worker-raw-reclamation-receipt-reference-root-v1",
            item_sha256s=tuple(
                reclamation_digest_mapping({
                    column: self._normalized_reclamation_row_value(row[column])
                    for column in self._RECLAMATION_COLUMNS[
                        "raw_artifact_reclamation_receipt_references"
                    ]
                })
                for row in receipt_rows
            ),
        )
        object_root = self._ordered_reclamation_root(
            schema="stv3-worker-raw-reclamation-object-reference-root-v1",
            item_sha256s=tuple(
                reclamation_digest_mapping({
                    column: self._normalized_reclamation_row_value(row[column])
                    for column in self._RECLAMATION_COLUMNS[
                        "raw_artifact_reclamation_object_references"
                    ]
                })
                for row in object_rows
            ),
        )
        child_reference_root_sha256 = reclamation_digest_mapping({
            "apply_order": [
                "capture_members",
                "captures",
                "receipts_at_zero_capture_references",
                "objects_at_zero_receipt_references",
            ],
            "capture_reference_root_sha256": capture_root,
            "object_reference_root_sha256": object_root,
            "receipt_reference_root_sha256": receipt_root,
            "schema": "stv3-worker-raw-reference-closure-prepare-v1",
        })
        source_fence_sha256 = reclamation_digest_mapping({
            "candidate_bindings_sha256": request.candidate_bindings_sha256,
            "child_reference_root_sha256": child_reference_root_sha256,
            "request_sha256": request_sha256,
            "schema": "stv3-worker-raw-reclamation-source-fence-v1",
            "source_store_identity_sha256": (
                receipt.source_store_identity_sha256
            ),
            "source_view_sha256": source_view_sha256,
        })
        releasable_bytes = sum(
            int(row["size_bytes"])
            for row in object_rows
            if int(row["delete_eligible"]) == 1
        )
        releasable_entries = sum(
            int(row["delete_eligible"]) for row in object_rows
        )
        if (
            source_view_sha256 != receipt.source_view_sha256
            or child_reference_root_sha256
            != receipt.child_reference_root_sha256
            or source_fence_sha256 != receipt.source_fence_sha256
            or releasable_bytes != receipt.releasable_bytes
            or releasable_entries != receipt.releasable_entries
        ):
            raise RawArtifactIntegrityError(
                "raw reclamation persisted prepare-roots verschillen"
            )

    def _load_reclamation_request(
        self,
        database: sqlite3.Connection,
        request_sha256: str,
    ) -> SourceReclamationRequestV2:
        row = database.execute(
            "SELECT request_json FROM raw_artifact_reclamation_prepares "
            "WHERE request_sha256=?",
            (request_sha256,),
        ).fetchone()
        if row is None:
            raise RawArtifactConflictError(
                "raw reclamation request heeft geen lokale prepare"
            )
        request = self._protocol_receipt_from_json(
            str(row["request_json"]),
            SourceReclamationRequestV2,
        )
        if (
            not isinstance(request, SourceReclamationRequestV2)
            or request.request_sha256 != request_sha256
        ):
            raise RawArtifactIntegrityError(
                "raw reclamation requestindex verschilt"
            )
        return request

    def _stored_reclamation_candidate_evidence(
        self,
        database: sqlite3.Connection,
        request_sha256: str,
    ) -> tuple[_ReclamationCandidateEvidence, ...]:
        rows = database.execute(
            "SELECT * FROM raw_artifact_reclamation_candidates "
            "WHERE request_sha256=? ORDER BY candidate_ordinal",
            (request_sha256,),
        ).fetchall()
        evidence: list[_ReclamationCandidateEvidence] = []
        for ordinal, row in enumerate(rows):
            if int(row["candidate_ordinal"]) != ordinal:
                raise RawArtifactIntegrityError(
                    "raw reclamation candidateordinal is niet aaneengesloten"
                )
            metadata_reference = bytes(row["metadata_reference"])
            try:
                record = json.loads(metadata_reference.decode("ascii"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise RawArtifactIntegrityError(
                    "raw reclamation metadata-reference is corrupt"
                ) from exc
            if not isinstance(record, dict):
                raise RawArtifactIntegrityError(
                    "raw reclamation metadata-reference is geen mapping"
                )
            evidence.append(_ReclamationCandidateEvidence(
                binding=self._candidate_binding_from_row(row),
                metadata_reference=metadata_reference,
                record=record,
            ))
        return tuple(evidence)

    @staticmethod
    def _normalized_reclamation_row_value(value: object) -> object:
        if isinstance(value, memoryview):
            return bytes(value)
        return value

    def _assert_reclamation_plan_rows(
        self,
        database: sqlite3.Connection,
        *,
        table: str,
        request_sha256: str,
        expected_rows: tuple[dict[str, Any], ...],
        key_columns: tuple[str, ...],
    ) -> None:
        expected_columns = self._RECLAMATION_COLUMNS.get(table)
        if expected_columns is None:
            raise AssertionError("onbekende raw reclamation plantabel")
        actual_rows = database.execute(
            f"SELECT * FROM {table} WHERE request_sha256=?",
            (request_sha256,),
        ).fetchall()

        def keyed(rows: object) -> dict[tuple[object, ...], dict[str, object]]:
            result: dict[tuple[object, ...], dict[str, object]] = {}
            for source in rows:  # type: ignore[union-attr]
                mapping = {
                    column: self._normalized_reclamation_row_value(
                        source[column]
                    )
                    for column in expected_columns
                }
                key = tuple(mapping[column] for column in key_columns)
                if key in result:
                    raise RawArtifactIntegrityError(
                        f"raw reclamation {table} herhaalt een plankey"
                    )
                result[key] = mapping
            return result

        if keyed(actual_rows) != keyed(expected_rows):
            raise RawArtifactConflictError(
                f"raw reclamation {table} verschilt van zijn fenced plan"
            )

    def _reprove_reclamation_prepare_plan(
        self,
        database: sqlite3.Connection,
        *,
        request: SourceReclamationRequestV2,
        prepared: SourceLocalPrepareReceiptV2,
    ) -> _ReclamationPreparePlan:
        evidence = self._stored_reclamation_candidate_evidence(
            database,
            request.request_sha256,
        )
        plan = self._build_reclamation_prepare_plan(
            database,
            request=request,
            evidence=evidence,
            store_identity_sha256=prepared.source_store_identity_sha256,
            exclude_active_request_sha256=request.request_sha256,
        )
        if (
            plan.source_view_sha256 != prepared.source_view_sha256
            or plan.child_reference_root_sha256
            != prepared.child_reference_root_sha256
            or plan.source_fence_sha256 != prepared.source_fence_sha256
            or plan.releasable_bytes != prepared.releasable_bytes
            or plan.releasable_entries != prepared.releasable_entries
            or len(plan.candidate_rows) != prepared.candidate_count
        ):
            raise RawArtifactConflictError(
                "raw reclamation sourcefacts veranderden na prepare"
            )
        self._assert_reclamation_plan_rows(
            database,
            table="raw_artifact_reclamation_candidates",
            request_sha256=request.request_sha256,
            expected_rows=plan.candidate_rows,
            key_columns=("request_sha256", "candidate_ordinal"),
        )
        self._assert_reclamation_plan_rows(
            database,
            table="raw_artifact_reclamation_capture_members",
            request_sha256=request.request_sha256,
            expected_rows=plan.member_rows,
            key_columns=("request_sha256", "capture_id", "role"),
        )
        self._assert_reclamation_plan_rows(
            database,
            table="raw_artifact_reclamation_receipt_references",
            request_sha256=request.request_sha256,
            expected_rows=plan.receipt_rows,
            key_columns=("request_sha256", "receipt_id"),
        )
        self._assert_reclamation_plan_rows(
            database,
            table="raw_artifact_reclamation_object_references",
            request_sha256=request.request_sha256,
            expected_rows=plan.object_rows,
            key_columns=("request_sha256", "content_sha256"),
        )
        return plan

    def _validate_reclamation_committed_decision(
        self,
        *,
        request: SourceReclamationRequestV2,
        prepared: SourceLocalPrepareReceiptV2,
        decision: CommittedReclamationDecisionReceiptV1,
    ) -> None:
        if not isinstance(decision, CommittedReclamationDecisionReceiptV1):
            raise RawArtifactIntegrityError(
                "raw reclamation decision-authority gaf een verkeerd receipt"
            )
        bindings = tuple(
            item for item in decision.participant_prepares
            if item.role == self.role
        )
        if len(bindings) != 1:
            raise RawArtifactIntegrityError(
                "committed raw participantbinding ontbreekt of herhaalt"
            )
        binding = bindings[0]
        if (
            decision.request_sha256 != request.request_sha256
            or decision.sealed_archive_anchor_sha256
            != request.sealed_archive_anchor_sha256
            or decision.archive_binding_root_sha256
            != request.archive_binding_root_sha256
            or decision.candidate_bindings_sha256
            != request.candidate_bindings_sha256
            or decision.candidate_authority_sha256
            != request.candidate_authority_sha256
            or decision.consumer_registry_root_sha256
            != request.consumer_registry_root_sha256
            or decision.consumer_epoch_root_sha256
            != request.consumer_epoch_root_sha256
            or decision.candidate_count != request.candidate_count
            or binding.prepare_receipt_sha256
            != prepared.prepare_receipt_sha256
            or binding.source_store_identity_sha256
            != prepared.source_store_identity_sha256
            or binding.source_fence_sha256 != prepared.source_fence_sha256
        ):
            raise RawArtifactConflictError(
                "held committed raw decision verschilt van de lokale prepare"
            )

    @staticmethod
    def _reclamation_unlink_intent_sha256(
        *,
        request_sha256: str,
        unlink_ordinal: int,
        identity: _ReclamationFileIdentity,
    ) -> str:
        return reclamation_digest_mapping({
            **identity.as_dict(),
            "request_sha256": request_sha256,
            "schema": RAW_ARTIFACT_RECLAMATION_UNLINK_INTENT_SCHEMA,
            "unlink_ordinal": unlink_ordinal,
        })

    @staticmethod
    def _reclamation_catalog_apply_sha256(
        *,
        request_sha256: str,
        source_store_identity_sha256: str,
        prepare_receipt_sha256: str,
        committed_decision_sha256: str,
        committed_decision_receipt_sha256: str,
        predecessor_apply_receipt_sha256: str,
        shared_reference_root_sha256: str,
        unlink_item_root_sha256: str,
        unlink_item_count: int,
        unlink_bytes: int,
        files_pending_at: float,
    ) -> str:
        return reclamation_digest_mapping({
            "committed_decision_receipt_sha256": (
                committed_decision_receipt_sha256
            ),
            "committed_decision_sha256": committed_decision_sha256,
            "files_pending_at": float(files_pending_at),
            "predecessor_apply_receipt_sha256": (
                predecessor_apply_receipt_sha256
            ),
            "prepare_receipt_sha256": prepare_receipt_sha256,
            "request_sha256": request_sha256,
            "schema": RAW_ARTIFACT_RECLAMATION_CATALOG_APPLY_SCHEMA,
            "shared_reference_root_sha256": shared_reference_root_sha256,
            "source_store_identity_sha256": source_store_identity_sha256,
            "state": RAW_RECLAMATION_FILES_PENDING,
            "unlink_bytes": unlink_bytes,
            "unlink_item_count": unlink_item_count,
            "unlink_item_root_sha256": unlink_item_root_sha256,
        })

    @staticmethod
    def _assert_reclamation_intents_match_prepare_plan(
        database: sqlite3.Connection,
        *,
        request_sha256: str,
        identities: tuple[_ReclamationFileIdentity, ...],
    ) -> None:
        """Bind FILES_PENDING targets to the durable prepared object plan."""

        expected_rows = database.execute(
            "SELECT content_sha256,relative_path,size_bytes FROM "
            "raw_artifact_reclamation_object_references "
            "WHERE request_sha256=? AND delete_eligible=1 "
            "ORDER BY content_sha256",
            (request_sha256,),
        ).fetchall()
        expected = tuple(
            (
                str(row["content_sha256"]),
                str(row["relative_path"]),
                int(row["size_bytes"]),
            )
            for row in expected_rows
        )
        actual = tuple(
            (
                identity.content_sha256,
                identity.relative_path,
                identity.size_bytes,
            )
            for identity in identities
        )
        if actual != expected:
            raise RawArtifactIntegrityError(
                "raw unlink-intents verschillen van het durable prepareplan"
            )

    def _load_reclamation_catalog_apply(
        self,
        database: sqlite3.Connection,
        *,
        request_sha256: str,
    ) -> dict[str, Any]:
        row = database.execute(
            "SELECT * FROM raw_artifact_reclamation_catalog_applies "
            "WHERE request_sha256=?",
            (request_sha256,),
        ).fetchone()
        if row is None:
            raise RawArtifactIntegrityError(
                "raw reclamation FILES_PENDING receipt ontbreekt"
            )
        prepared = self._load_reclamation_prepare(database, request_sha256)
        if prepared is None:
            raise RawArtifactIntegrityError(
                "raw reclamation catalogapply mist zijn prepare"
            )
        request = self._load_reclamation_request(database, request_sha256)
        decision = self._protocol_receipt_from_json(
            str(row["committed_decision_json"]),
            CommittedReclamationDecisionReceiptV1,
        )
        if not isinstance(decision, CommittedReclamationDecisionReceiptV1):
            raise RawArtifactIntegrityError(
                "raw reclamation catalogbesluit heeft verkeerd type"
            )
        self._validate_reclamation_committed_decision(
            request=request,
            prepared=prepared,
            decision=decision,
        )
        intent_rows = database.execute(
            "SELECT * FROM raw_artifact_reclamation_unlink_intents "
            "WHERE request_sha256=? ORDER BY unlink_ordinal",
            (request_sha256,),
        ).fetchall()
        intent_sha256s: list[str] = []
        identities: list[_ReclamationFileIdentity] = []
        for ordinal, intent in enumerate(intent_rows):
            if int(intent["unlink_ordinal"]) != ordinal:
                raise RawArtifactIntegrityError(
                    "raw unlink-intentordinal is niet aaneengesloten"
                )
            identity = _ReclamationFileIdentity(
                content_sha256=require_reclamation_sha256(
                    str(intent["content_sha256"]),
                    "raw unlink content SHA-256",
                ),
                relative_path=str(intent["relative_path"]),
                size_bytes=int(intent["size_bytes"]),
                st_dev=int(intent["st_dev"]),
                st_ino=int(intent["st_ino"]),
                st_mode=int(intent["st_mode"]),
                st_nlink=int(intent["st_nlink"]),
            )
            if (
                identity.size_bytes < 0
                or identity.size_bytes > self.limits.max_object_bytes
                or identity.st_dev < 0
                or identity.st_ino <= 0
                or identity.st_mode <= 0
                or not stat.S_ISREG(identity.st_mode)
                or identity.st_nlink != 1
            ):
                raise RawArtifactIntegrityError(
                    "raw unlink-intent heeft ongeldige fysieke facts"
                )
            self._safe_object_path(
                identity.relative_path,
                identity.content_sha256,
            )
            intent_sha256 = require_reclamation_sha256(
                str(intent["intent_sha256"]),
                "raw unlink intent SHA-256",
            )
            if intent_sha256 != self._reclamation_unlink_intent_sha256(
                request_sha256=request_sha256,
                unlink_ordinal=ordinal,
                identity=identity,
            ):
                raise RawArtifactIntegrityError(
                    "raw unlink-intentchecksum verschilt"
                )
            intent_sha256s.append(intent_sha256)
            identities.append(identity)
        identity_tuple = tuple(identities)
        self._assert_reclamation_intents_match_prepare_plan(
            database,
            request_sha256=request_sha256,
            identities=identity_tuple,
        )
        unlink_item_root_sha256 = self._ordered_reclamation_root(
            schema="stv3-worker-raw-reclamation-unlink-item-root-v1",
            item_sha256s=tuple(intent_sha256s),
        )
        files_pending_at = self._timestamp(
            row["files_pending_at"],
            "raw FILES_PENDING clock",
        )
        if files_pending_at < prepared.prepared_at:
            raise RawArtifactIntegrityError(
                "raw FILES_PENDING clock ligt vóór prepare"
            )
        predecessor = require_reclamation_sha256(
            str(row["predecessor_apply_receipt_sha256"]),
            "raw catalog predecessor apply receipt SHA-256",
        )
        catalog_apply_sha256 = self._reclamation_catalog_apply_sha256(
            request_sha256=request_sha256,
            source_store_identity_sha256=(
                prepared.source_store_identity_sha256
            ),
            prepare_receipt_sha256=prepared.prepare_receipt_sha256,
            committed_decision_sha256=decision.decision_sha256,
            committed_decision_receipt_sha256=decision.receipt_sha256,
            predecessor_apply_receipt_sha256=predecessor,
            shared_reference_root_sha256=(
                prepared.child_reference_root_sha256
            ),
            unlink_item_root_sha256=unlink_item_root_sha256,
            unlink_item_count=len(intent_rows),
            unlink_bytes=sum(item.size_bytes for item in identities),
            files_pending_at=files_pending_at,
        )
        if (
            str(row["prepare_receipt_sha256"])
            != prepared.prepare_receipt_sha256
            or str(row["committed_decision_sha256"])
            != decision.decision_sha256
            or str(row["committed_decision_receipt_sha256"])
            != decision.receipt_sha256
            or str(row["catalog_apply_sha256"]) != catalog_apply_sha256
            or str(row["shared_reference_root_sha256"])
            != prepared.child_reference_root_sha256
            or str(row["unlink_item_root_sha256"])
            != unlink_item_root_sha256
            or int(row["unlink_item_count"]) != len(intent_rows)
            or int(row["unlink_bytes"])
            != sum(item.size_bytes for item in identities)
            or str(row["state"]) != RAW_RECLAMATION_FILES_PENDING
            or len(intent_rows) != prepared.releasable_entries
            or sum(item.size_bytes for item in identities)
            != prepared.releasable_bytes
        ):
            raise RawArtifactIntegrityError(
                "raw reclamation catalogapplybindings verschillen"
            )
        self._raise_if_reclamation_unlink_fenced(
            database,
            request_sha256=request_sha256,
            intent_rows=tuple(intent_rows),
            files_pending_at=files_pending_at,
        )
        return {
            "catalog_apply_sha256": catalog_apply_sha256,
            "decision": decision,
            "files_pending_at": files_pending_at,
            "identities": identity_tuple,
            "intent_rows": tuple(intent_rows),
            "predecessor_apply_receipt_sha256": predecessor,
            "row": row,
            "unlink_item_root_sha256": unlink_item_root_sha256,
        }

    @staticmethod
    def _validate_reclamation_catalog_decision(
        *,
        catalog: Mapping[str, Any],
        decision: CommittedReclamationDecisionReceiptV1,
        predecessor_apply_receipt_sha256: str,
    ) -> None:
        if (
            catalog["decision"] != decision
            or catalog["predecessor_apply_receipt_sha256"]
            != predecessor_apply_receipt_sha256
        ):
            raise RawArtifactConflictError(
                "raw FILES_PENDING journal bindt een ander committed besluit"
            )

    def _ensure_reclamation_catalog_apply(
        self,
        *,
        request: SourceReclamationRequestV2,
        prepared: SourceLocalPrepareReceiptV2,
        decision: CommittedReclamationDecisionReceiptV1,
        predecessor_apply_receipt_sha256: str,
    ) -> None:
        with self._lock, closing(self._connect()) as database:
            database.execute("BEGIN IMMEDIATE")
            try:
                self._require_reclamation_extension(database)
                existing = database.execute(
                    "SELECT 1 FROM raw_artifact_reclamation_catalog_applies "
                    "WHERE request_sha256=?",
                    (request.request_sha256,),
                ).fetchone()
                if existing is not None:
                    catalog = self._load_reclamation_catalog_apply(
                        database,
                        request_sha256=request.request_sha256,
                    )
                    self._validate_reclamation_catalog_decision(
                        catalog=catalog,
                        decision=decision,
                        predecessor_apply_receipt_sha256=(
                            predecessor_apply_receipt_sha256
                        ),
                    )
                    database.commit()
                    return
                if self._load_reclamation_abort(
                    database,
                    request.request_sha256,
                ) is not None:
                    raise RawArtifactConflictError(
                        "aborted raw prepare kan niet naar FILES_PENDING"
                    )
                current = self._load_reclamation_prepare(
                    database,
                    request.request_sha256,
                )
                if current != prepared or self._load_reclamation_request(
                    database,
                    request.request_sha256,
                ) != request:
                    raise RawArtifactConflictError(
                        "raw prepare veranderde vóór catalogapply"
                    )
                plan = self._reprove_reclamation_prepare_plan(
                    database,
                    request=request,
                    prepared=prepared,
                )
                identities: list[_ReclamationFileIdentity] = []
                for object_row in sorted(
                    (
                        row for row in plan.object_rows
                        if int(row["delete_eligible"]) == 1
                    ),
                    key=lambda row: str(row["content_sha256"]),
                ):
                    identities.append(self._reclamation_file_identity(
                        self._safe_object_path(
                            str(object_row["relative_path"]),
                            str(object_row["content_sha256"]),
                        ),
                        expected_size=int(object_row["size_bytes"]),
                        expected_sha256=str(object_row["content_sha256"]),
                    ))
                intent_rows: list[dict[str, Any]] = []
                intent_sha256s: list[str] = []
                for ordinal, identity in enumerate(identities):
                    intent_sha256 = self._reclamation_unlink_intent_sha256(
                        request_sha256=request.request_sha256,
                        unlink_ordinal=ordinal,
                        identity=identity,
                    )
                    intent_sha256s.append(intent_sha256)
                    intent_rows.append({
                        "request_sha256": request.request_sha256,
                        "unlink_ordinal": ordinal,
                        "intent_sha256": intent_sha256,
                        **identity.as_dict(),
                    })
                unlink_item_root_sha256 = self._ordered_reclamation_root(
                    schema="stv3-worker-raw-reclamation-unlink-item-root-v1",
                    item_sha256s=tuple(intent_sha256s),
                )
                files_pending_at = self._reclamation_now()
                if files_pending_at < prepared.prepared_at:
                    raise RawArtifactConflictError(
                        "raw reclamation clock liep terug vóór FILES_PENDING"
                    )
                catalog_apply_sha256 = self._reclamation_catalog_apply_sha256(
                    request_sha256=request.request_sha256,
                    source_store_identity_sha256=(
                        prepared.source_store_identity_sha256
                    ),
                    prepare_receipt_sha256=prepared.prepare_receipt_sha256,
                    committed_decision_sha256=decision.decision_sha256,
                    committed_decision_receipt_sha256=decision.receipt_sha256,
                    predecessor_apply_receipt_sha256=(
                        predecessor_apply_receipt_sha256
                    ),
                    shared_reference_root_sha256=(
                        prepared.child_reference_root_sha256
                    ),
                    unlink_item_root_sha256=unlink_item_root_sha256,
                    unlink_item_count=len(intent_rows),
                    unlink_bytes=sum(item.size_bytes for item in identities),
                    files_pending_at=files_pending_at,
                )
                database.execute(
                    """
                    INSERT INTO raw_artifact_reclamation_catalog_applies(
                        request_sha256, prepare_receipt_sha256,
                        committed_decision_sha256,
                        committed_decision_receipt_sha256,
                        committed_decision_json,
                        predecessor_apply_receipt_sha256,
                        catalog_apply_sha256, shared_reference_root_sha256,
                        unlink_item_root_sha256, unlink_item_count,
                        unlink_bytes, files_pending_at, state
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        request.request_sha256,
                        prepared.prepare_receipt_sha256,
                        decision.decision_sha256,
                        decision.receipt_sha256,
                        self._protocol_json(decision.as_dict()),
                        predecessor_apply_receipt_sha256,
                        catalog_apply_sha256,
                        prepared.child_reference_root_sha256,
                        unlink_item_root_sha256,
                        len(intent_rows),
                        sum(item.size_bytes for item in identities),
                        files_pending_at,
                        RAW_RECLAMATION_FILES_PENDING,
                    ),
                )
                self._insert_reclamation_mapping_rows(
                    database,
                    table="raw_artifact_reclamation_unlink_intents",
                    rows=tuple(intent_rows),
                )
                for candidate in plan.candidate_rows:
                    tombstone_sha256 = reclamation_digest_mapping({
                        "capture_id": candidate["capture_id"],
                        "capture_receipt_sha256": (
                            candidate["capture_receipt_sha256"]
                        ),
                        "catalog_apply_sha256": catalog_apply_sha256,
                        "fetch_id": candidate["fetch_id"],
                        "manifest_sha256": candidate["manifest_sha256"],
                        "product": candidate["product"],
                        "provider": candidate["provider"],
                        "request_sha256": request.request_sha256,
                        "schema": RAW_ARTIFACT_RECLAMATION_TOMBSTONE_SCHEMA,
                        "tombstone_kind": "capture_identity",
                        "upstream_capture_id": (
                            candidate["upstream_capture_id"]
                        ),
                    })
                    database.execute(
                        """
                        INSERT INTO raw_artifact_reclamation_capture_tombstones(
                            capture_id,provider,product,fetch_id,
                            upstream_capture_id,manifest_sha256,
                            capture_receipt_sha256,request_sha256,
                            catalog_apply_sha256,tombstone_sha256,created_at
                        ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            candidate["capture_id"],
                            candidate["provider"],
                            candidate["product"],
                            candidate["fetch_id"],
                            candidate["upstream_capture_id"],
                            candidate["manifest_sha256"],
                            candidate["capture_receipt_sha256"],
                            request.request_sha256,
                            catalog_apply_sha256,
                            tombstone_sha256,
                            files_pending_at,
                        ),
                    )
                for reference in plan.receipt_rows:
                    if int(reference["delete_eligible"]) != 1:
                        continue
                    tombstone_sha256 = reclamation_digest_mapping({
                        "catalog_apply_sha256": catalog_apply_sha256,
                        "fetch_id": reference["fetch_id"],
                        "product": reference["product"],
                        "provider": reference["provider"],
                        "receipt_id": reference["receipt_id"],
                        "receipt_sha256": reference["receipt_sha256"],
                        "request_sha256": request.request_sha256,
                        "schema": RAW_ARTIFACT_RECLAMATION_TOMBSTONE_SCHEMA,
                        "tombstone_kind": "receipt_identity",
                        "upstream_id": reference["upstream_id"],
                    })
                    database.execute(
                        """
                        INSERT INTO raw_artifact_reclamation_receipt_tombstones(
                            receipt_id,provider,product,fetch_id,upstream_id,
                            receipt_sha256,request_sha256,
                            catalog_apply_sha256,tombstone_sha256,created_at
                        ) VALUES(?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            reference["receipt_id"],
                            reference["provider"],
                            reference["product"],
                            reference["fetch_id"],
                            reference["upstream_id"],
                            reference["receipt_sha256"],
                            request.request_sha256,
                            catalog_apply_sha256,
                            tombstone_sha256,
                            files_pending_at,
                        ),
                    )

                member_counts: dict[str, int] = {}
                for member in plan.member_rows:
                    capture_id = str(member["capture_id"])
                    member_counts[capture_id] = member_counts.get(capture_id, 0) + 1
                for candidate in plan.candidate_rows:
                    capture_id = str(candidate["capture_id"])
                    deleted_members = database.execute(
                        "DELETE FROM raw_artifact_capture_members "
                        "WHERE capture_id=?",
                        (capture_id,),
                    ).rowcount
                    if deleted_members != member_counts.get(capture_id, 0):
                        raise RawArtifactConflictError(
                            "raw captureleden veranderden vóór catalogapply"
                        )
                    if database.execute(
                        "DELETE FROM raw_artifact_captures WHERE capture_id=?",
                        (capture_id,),
                    ).rowcount != 1:
                        raise RawArtifactConflictError(
                            "raw capture verdween vóór catalogapply"
                        )
                for reference in plan.receipt_rows:
                    if int(reference["delete_eligible"]) != 1:
                        continue
                    remaining = int(database.execute(
                        "SELECT count(*) FROM raw_artifact_capture_members "
                        "WHERE receipt_id=?",
                        (reference["receipt_id"],),
                    ).fetchone()[0])
                    if remaining != 0:
                        raise RawArtifactConflictError(
                            "raw receipt heeft nog capture-FK-references"
                        )
                    if database.execute(
                        "DELETE FROM raw_artifact_receipts WHERE receipt_id=?",
                        (reference["receipt_id"],),
                    ).rowcount != 1:
                        raise RawArtifactConflictError(
                            "raw receipt verdween vóór catalogapply"
                        )
                for reference in plan.object_rows:
                    if int(reference["delete_eligible"]) != 1:
                        continue
                    remaining = int(database.execute(
                        "SELECT count(*) FROM raw_artifact_receipts "
                        "WHERE content_sha256=?",
                        (reference["content_sha256"],),
                    ).fetchone()[0])
                    if remaining != 0:
                        raise RawArtifactConflictError(
                            "raw object heeft nog receipt-FK-references"
                        )
                    if database.execute(
                        "DELETE FROM raw_artifact_objects "
                        "WHERE content_sha256=?",
                        (reference["content_sha256"],),
                    ).rowcount != 1:
                        raise RawArtifactConflictError(
                            "raw object verdween vóór catalogapply"
                        )
                self._checkpoint("reclamation_catalog_before_commit")
                database.commit()
            except sqlite3.OperationalError as exc:
                if database.in_transaction:
                    database.rollback()
                if self._is_sqlite_capacity_error(exc):
                    raise self._catalog_capacity_error() from exc
                raise
            except sqlite3.IntegrityError as exc:
                if database.in_transaction:
                    database.rollback()
                raise RawArtifactConflictError(
                    "raw catalogapply kon niet atomair worden vastgelegd"
                ) from exc
            except BaseException:
                if database.in_transaction:
                    database.rollback()
                raise
        self._storage_usage_cache = None
        self._checkpoint("reclamation_catalog_after_commit")

    @staticmethod
    def _reclamation_directory_barrier_sha256(
        *,
        request_sha256: str,
        unlink_ordinal: int,
        intent_sha256: str,
        directory_facts: Mapping[str, int],
        object_was_present: bool,
        unlinked_at: float,
    ) -> str:
        return reclamation_digest_mapping({
            "directory_st_dev": int(directory_facts["st_dev"]),
            "directory_st_ino": int(directory_facts["st_ino"]),
            "directory_st_mode": int(directory_facts["st_mode"]),
            "intent_sha256": intent_sha256,
            "object_was_present": object_was_present,
            "request_sha256": request_sha256,
            "schema": "stv3-worker-raw-directory-fsync-barrier-v1",
            "unlink_ordinal": unlink_ordinal,
            "unlinked_at": float(unlinked_at),
        })

    @staticmethod
    def _reclamation_unlink_event_sha256(
        *,
        request_sha256: str,
        unlink_ordinal: int,
        intent_sha256: str,
        predecessor_unlink_sha256: str | None,
        directory_barrier_sha256: str,
        unlinked_at: float,
    ) -> str:
        return reclamation_digest_mapping({
            "directory_barrier_sha256": directory_barrier_sha256,
            "intent_sha256": intent_sha256,
            "predecessor_unlink_sha256": predecessor_unlink_sha256,
            "request_sha256": request_sha256,
            "schema": RAW_ARTIFACT_RECLAMATION_UNLINK_DURABLE_SCHEMA,
            "state": RAW_RECLAMATION_UNLINKED_DURABLE,
            "unlink_ordinal": unlink_ordinal,
            "unlinked_at": float(unlinked_at),
        })

    @staticmethod
    def _reclamation_unlink_failure_sha256(
        *,
        request_sha256: str,
        unlink_ordinal: int,
        intent_sha256: str,
        phase: str,
        observed: os.stat_result,
        object_path_absent: bool,
        observed_at: float,
    ) -> str:
        return reclamation_digest_mapping({
            "failure_code": "HARDLINK_ALIAS_OBSERVED",
            "intent_sha256": intent_sha256,
            "object_path_absent": object_path_absent,
            "observed_at": float(observed_at),
            "observed_st_dev": int(observed.st_dev),
            "observed_st_ino": int(observed.st_ino),
            "observed_st_mode": int(observed.st_mode),
            "observed_st_nlink": int(observed.st_nlink),
            "observed_st_size": int(observed.st_size),
            "phase": phase,
            "request_sha256": request_sha256,
            "schema": RAW_ARTIFACT_RECLAMATION_UNLINK_FAILURE_SCHEMA,
            "state": RAW_RECLAMATION_FAILED_FENCED,
            "unlink_ordinal": unlink_ordinal,
        })

    def _raise_if_reclamation_unlink_fenced(
        self,
        database: sqlite3.Connection,
        *,
        request_sha256: str,
        intent_rows: tuple[sqlite3.Row, ...],
        files_pending_at: float,
    ) -> None:
        row = database.execute(
            "SELECT * FROM raw_artifact_reclamation_unlink_failures "
            "WHERE request_sha256=?",
            (request_sha256,),
        ).fetchone()
        if row is None:
            return
        ordinal = int(row["unlink_ordinal"])
        if ordinal < 0 or ordinal >= len(intent_rows):
            raise RawArtifactIntegrityError(
                "raw hardlink-failure bindt geen geldige unlink-intent"
            )
        intent = intent_rows[ordinal]
        phase = str(row["phase"])
        observed_at = self._timestamp(
            row["observed_at"],
            "raw hardlink-failure clock",
        )
        observed = os.stat_result((
            int(row["observed_st_mode"]),
            int(row["observed_st_ino"]),
            int(row["observed_st_dev"]),
            int(row["observed_st_nlink"]),
            0,
            0,
            int(row["observed_st_size"]),
            0.0,
            0.0,
            0.0,
        ))
        object_path_absent = int(row["object_path_absent"])
        expected_failure_sha256 = self._reclamation_unlink_failure_sha256(
            request_sha256=request_sha256,
            unlink_ordinal=ordinal,
            intent_sha256=str(intent["intent_sha256"]),
            phase=phase,
            observed=observed,
            object_path_absent=bool(object_path_absent),
            observed_at=observed_at,
        )
        forbidden_claim = database.execute(
            "SELECT 1 FROM raw_artifact_reclamation_unlink_durable "
            "WHERE request_sha256=? AND unlink_ordinal>=? LIMIT 1",
            (request_sha256, ordinal),
        ).fetchone()
        if forbidden_claim is None:
            forbidden_claim = database.execute(
                "SELECT 1 FROM raw_artifact_reclamation_unlinked_batches "
                "WHERE request_sha256=? UNION ALL SELECT 1 FROM "
                "raw_artifact_reclamation_applies WHERE request_sha256=? LIMIT 1",
                (request_sha256, request_sha256),
            ).fetchone()
        if (
            str(row["intent_sha256"]) != str(intent["intent_sha256"])
            or phase not in {"PRE_UNLINK", "POST_UNLINK"}
            or int(row["observed_st_dev"]) != int(intent["st_dev"])
            or int(row["observed_st_ino"]) != int(intent["st_ino"])
            or int(row["observed_st_mode"]) != int(intent["st_mode"])
            or int(row["observed_st_size"]) != int(intent["size_bytes"])
            or int(row["observed_st_nlink"]) <= 0
            or object_path_absent not in (0, 1)
            or str(row["failure_sha256"]) != expected_failure_sha256
            or observed_at < files_pending_at
            or str(row["state"]) != RAW_RECLAMATION_FAILED_FENCED
            or forbidden_claim is not None
        ):
            raise RawArtifactIntegrityError(
                "raw hardlink-failurefence heeft afwijkende bindings"
            )
        raise RawArtifactIntegrityError(
            "raw reclamation is duurzaam gefenced na waargenomen hardlink-alias"
        )

    def _fence_reclamation_hardlink_alias(
        self,
        database: sqlite3.Connection,
        *,
        request_sha256: str,
        unlink_ordinal: int,
        intent_sha256: str,
        phase: str,
        observed: os.stat_result,
        object_path_absent: bool,
        files_pending_at: float,
    ) -> None:
        if int(observed.st_nlink) <= 0:
            raise RawArtifactIntegrityError(
                "raw hardlink-failure vereist een levende alias"
            )
        observed_at = self._reclamation_now()
        if observed_at < files_pending_at:
            raise RawArtifactConflictError(
                "raw reclamation clock liep terug vÃ³Ã³r hardlink-failurefence"
            )
        failure_sha256 = self._reclamation_unlink_failure_sha256(
            request_sha256=request_sha256,
            unlink_ordinal=unlink_ordinal,
            intent_sha256=intent_sha256,
            phase=phase,
            observed=observed,
            object_path_absent=object_path_absent,
            observed_at=observed_at,
        )
        database.execute(
            """
            INSERT INTO raw_artifact_reclamation_unlink_failures(
                request_sha256,unlink_ordinal,intent_sha256,phase,
                observed_st_dev,observed_st_ino,observed_st_mode,
                observed_st_nlink,observed_st_size,object_path_absent,
                failure_sha256,observed_at,state
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                request_sha256,
                unlink_ordinal,
                intent_sha256,
                phase,
                int(observed.st_dev),
                int(observed.st_ino),
                int(observed.st_mode),
                int(observed.st_nlink),
                int(observed.st_size),
                int(object_path_absent),
                failure_sha256,
                observed_at,
                RAW_RECLAMATION_FAILED_FENCED,
            ),
        )
        # Commit the terminal fence before surfacing the failure.  The caller's
        # outer rollback guard observes that the transaction is already closed.
        database.commit()
        raise RawArtifactIntegrityError(
            "raw reclamation is duurzaam gefenced na waargenomen hardlink-alias"
        )

    def _load_reclamation_unlink_events(
        self,
        database: sqlite3.Connection,
        *,
        catalog: Mapping[str, Any],
    ) -> tuple[dict[str, Any], ...]:
        request_sha256 = str(catalog["row"]["request_sha256"])
        rows = database.execute(
            "SELECT * FROM raw_artifact_reclamation_unlink_durable "
            "WHERE request_sha256=? ORDER BY unlink_ordinal",
            (request_sha256,),
        ).fetchall()
        intents = catalog["intent_rows"]
        if len(rows) > len(intents):
            raise RawArtifactIntegrityError(
                "raw unlinkjournal bevat meer events dan intents"
            )
        events: list[dict[str, Any]] = []
        predecessor: str | None = None
        previous_clock = float(catalog["files_pending_at"])
        for ordinal, row in enumerate(rows):
            intent = intents[ordinal]
            if (
                int(row["unlink_ordinal"]) != ordinal
                or str(row["intent_sha256"])
                != str(intent["intent_sha256"])
            ):
                raise RawArtifactIntegrityError(
                    "raw unlinkjournal is niet een exact intentprefix"
                )
            stored_predecessor = (
                None
                if row["predecessor_unlink_sha256"] is None
                else require_reclamation_sha256(
                    str(row["predecessor_unlink_sha256"]),
                    "raw predecessor unlink SHA-256",
                )
            )
            if stored_predecessor != predecessor:
                raise RawArtifactIntegrityError(
                    "raw unlinkjournal predecessorchain verschilt"
                )
            directory_facts = {
                "st_dev": int(row["directory_st_dev"]),
                "st_ino": int(row["directory_st_ino"]),
                "st_mode": int(row["directory_st_mode"]),
            }
            if (
                directory_facts["st_dev"] < 0
                or directory_facts["st_ino"] < 0
                or directory_facts["st_mode"] <= 0
                or not stat.S_ISDIR(directory_facts["st_mode"])
                or int(row["object_was_present"]) not in (0, 1)
            ):
                raise RawArtifactIntegrityError(
                    "raw unlinkjournal directoryfacts zijn ongeldig"
                )
            unlinked_at = self._timestamp(
                row["unlinked_at"],
                "raw unlink clock",
            )
            if unlinked_at < previous_clock:
                raise RawArtifactIntegrityError(
                    "raw unlinkjournal clock is niet monotoon"
                )
            directory_barrier_sha256 = (
                self._reclamation_directory_barrier_sha256(
                    request_sha256=request_sha256,
                    unlink_ordinal=ordinal,
                    intent_sha256=str(intent["intent_sha256"]),
                    directory_facts=directory_facts,
                    object_was_present=bool(row["object_was_present"]),
                    unlinked_at=unlinked_at,
                )
            )
            unlink_event_sha256 = self._reclamation_unlink_event_sha256(
                request_sha256=request_sha256,
                unlink_ordinal=ordinal,
                intent_sha256=str(intent["intent_sha256"]),
                predecessor_unlink_sha256=predecessor,
                directory_barrier_sha256=directory_barrier_sha256,
                unlinked_at=unlinked_at,
            )
            if (
                str(row["directory_barrier_sha256"])
                != directory_barrier_sha256
                or str(row["unlink_event_sha256"])
                != unlink_event_sha256
                or str(row["state"])
                != RAW_RECLAMATION_UNLINKED_DURABLE
            ):
                raise RawArtifactIntegrityError(
                    "raw unlinkjournal eventchecksum verschilt"
                )
            events.append({
                "directory_barrier_sha256": directory_barrier_sha256,
                "directory_facts": directory_facts,
                "intent_sha256": str(intent["intent_sha256"]),
                "object_was_present": bool(row["object_was_present"]),
                "predecessor_unlink_sha256": predecessor,
                "unlink_event_sha256": unlink_event_sha256,
                "unlink_ordinal": ordinal,
                "unlinked_at": unlinked_at,
            })
            predecessor = unlink_event_sha256
            previous_clock = unlinked_at
        return tuple(events)

    @staticmethod
    def _reclamation_unlinked_batch_sha256(
        *,
        request_sha256: str,
        unlink_item_root_sha256: str,
        directory_barrier_root_sha256: str,
        last_unlink_event_sha256: str | None,
        unlink_item_count: int,
        unlinked_bytes: int,
        completed_at: float,
    ) -> str:
        return reclamation_digest_mapping({
            "completed_at": float(completed_at),
            "directory_barrier_root_sha256": (
                directory_barrier_root_sha256
            ),
            "last_unlink_event_sha256": last_unlink_event_sha256,
            "request_sha256": request_sha256,
            "schema": "stv3-worker-raw-unlinked-durable-batch-v1",
            "state": RAW_RECLAMATION_UNLINKED_DURABLE,
            "unlink_item_count": unlink_item_count,
            "unlink_item_root_sha256": unlink_item_root_sha256,
            "unlinked_bytes": unlinked_bytes,
        })

    def _load_reclamation_unlinked_batch(
        self,
        database: sqlite3.Connection,
        *,
        catalog: Mapping[str, Any],
    ) -> dict[str, Any] | None:
        request_sha256 = str(catalog["row"]["request_sha256"])
        row = database.execute(
            "SELECT * FROM raw_artifact_reclamation_unlinked_batches "
            "WHERE request_sha256=?",
            (request_sha256,),
        ).fetchone()
        if row is None:
            return None
        events = self._load_reclamation_unlink_events(
            database,
            catalog=catalog,
        )
        if len(events) != len(catalog["intent_rows"]):
            raise RawArtifactIntegrityError(
                "raw UNLINKED_DURABLE batch heeft geen volledige eventprefix"
            )
        directory_barrier_root_sha256 = self._ordered_reclamation_root(
            schema="stv3-worker-raw-directory-barrier-root-v1",
            item_sha256s=tuple(
                event["directory_barrier_sha256"] for event in events
            ),
        )
        last_event = (
            events[-1]["unlink_event_sha256"] if events else None
        )
        completed_at = self._timestamp(
            row["completed_at"],
            "raw UNLINKED_DURABLE clock",
        )
        minimum_completed_at = (
            float(events[-1]["unlinked_at"])
            if events
            else float(catalog["files_pending_at"])
        )
        if completed_at < minimum_completed_at:
            raise RawArtifactIntegrityError(
                "raw UNLINKED_DURABLE clock ligt vóór zijn eventprefix"
            )
        unlinked_durable_sha256 = self._reclamation_unlinked_batch_sha256(
            request_sha256=request_sha256,
            unlink_item_root_sha256=catalog["unlink_item_root_sha256"],
            directory_barrier_root_sha256=directory_barrier_root_sha256,
            last_unlink_event_sha256=last_event,
            unlink_item_count=len(catalog["intent_rows"]),
            unlinked_bytes=int(catalog["row"]["unlink_bytes"]),
            completed_at=completed_at,
        )
        if (
            str(row["unlink_item_root_sha256"])
            != catalog["unlink_item_root_sha256"]
            or str(row["directory_barrier_root_sha256"])
            != directory_barrier_root_sha256
            or row["last_unlink_event_sha256"] != last_event
            or str(row["unlinked_durable_sha256"])
            != unlinked_durable_sha256
            or int(row["unlink_item_count"])
            != len(catalog["intent_rows"])
            or int(row["unlinked_bytes"])
            != int(catalog["row"]["unlink_bytes"])
            or str(row["state"])
            != RAW_RECLAMATION_UNLINKED_DURABLE
        ):
            raise RawArtifactIntegrityError(
                "raw UNLINKED_DURABLE batchbindings verschillen"
            )
        return {
            "completed_at": completed_at,
            "directory_barrier_root_sha256": (
                directory_barrier_root_sha256
            ),
            "events": events,
            "last_unlink_event_sha256": last_event,
            "row": row,
            "unlinked_durable_sha256": unlinked_durable_sha256,
        }

    @staticmethod
    def _assert_reclamation_path_absent(path: Path) -> None:
        try:
            path.lstat()
        except FileNotFoundError:
            return
        except OSError as exc:
            raise RawArtifactIntegrityError(
                "raw reclamation objectabsence kon niet worden bewezen"
            ) from exc
        raise RawArtifactIntegrityError(
            "raw reclamation duurzaam unlinked object is opnieuw verschenen"
        )

    def _assert_reclamation_durable_paths_absent(
        self,
        *,
        catalog: Mapping[str, Any],
        event_count: int,
    ) -> None:
        identities = catalog["identities"]
        for identity in identities[:event_count]:
            self._assert_reclamation_path_absent(self._safe_object_path(
                identity.relative_path,
                identity.content_sha256,
            ))

    @staticmethod
    def _reclamation_receipt_identity_is_tombstoned(
        database: sqlite3.Connection,
        receipt: RawArtifactReceipt,
    ) -> bool:
        return database.execute(
            "SELECT 1 FROM raw_artifact_reclamation_receipt_tombstones "
            "WHERE receipt_id=? OR (provider=? AND product=? AND "
            "(fetch_id=? OR upstream_id=?)) LIMIT 1",
            (
                receipt.receipt_id,
                receipt.provider,
                receipt.product,
                receipt.fetch_id,
                receipt.upstream_id,
            ),
        ).fetchone() is not None

    @staticmethod
    def _reclamation_capture_identity_is_tombstoned(
        database: sqlite3.Connection,
        capture: RawArtifactCaptureReceipt,
    ) -> bool:
        return database.execute(
            "SELECT 1 FROM raw_artifact_reclamation_capture_tombstones "
            "WHERE capture_id=? OR (provider=? AND product=? AND "
            "(fetch_id=? OR upstream_capture_id=?)) LIMIT 1",
            (
                capture.capture_id,
                capture.provider,
                capture.product,
                capture.fetch_id,
                capture.upstream_capture_id,
            ),
        ).fetchone() is not None

    def _assert_reclamation_applied_replay_authority(
        self,
        database: sqlite3.Connection,
        *,
        catalog: Mapping[str, Any],
    ) -> None:
        """Reprove permanent semantic deletion while permitting byte reuse."""

        request_sha256 = str(catalog["row"]["request_sha256"])
        original_identity_checks = (
            (
                "SELECT 1 FROM raw_artifact_captures AS live JOIN "
                "raw_artifact_reclamation_candidates AS original ON "
                "original.capture_id=live.capture_id OR ("
                "original.provider=live.provider AND "
                "original.product=live.product AND ("
                "original.fetch_id=live.fetch_id OR "
                "original.upstream_capture_id=live.upstream_capture_id)) "
                "WHERE original.request_sha256=? LIMIT 1",
                "raw APPLIED capture-identiteit is opnieuw verschenen",
            ),
            (
                "SELECT 1 FROM raw_artifact_receipts AS live JOIN "
                "raw_artifact_reclamation_receipt_references AS original ON "
                "original.receipt_id=live.receipt_id OR ("
                "original.provider=live.provider AND "
                "original.product=live.product AND ("
                "original.fetch_id=live.fetch_id OR "
                "original.upstream_id=live.upstream_id)) "
                "WHERE original.request_sha256=? "
                "AND original.delete_eligible=1 LIMIT 1",
                "raw APPLIED receipt-identiteit is opnieuw verschenen",
            ),
        )
        for statement, message in original_identity_checks:
            if database.execute(
                statement,
                (request_sha256,),
            ).fetchone() is not None:
                raise RawArtifactIntegrityError(message)

        for identity in catalog["identities"]:
            path = self._safe_object_path(
                identity.relative_path,
                identity.content_sha256,
            )
            object_row = database.execute(
                "SELECT relative_path,size_bytes FROM raw_artifact_objects "
                "WHERE content_sha256=?",
                (identity.content_sha256,),
            ).fetchone()
            try:
                path_present = not self._reclamation_path_is_absent(
                    path,
                    directory_descriptor=None,
                )
            except RawArtifactIntegrityError:
                raise
            if object_row is None:
                if path_present:
                    raise RawArtifactIntegrityError(
                        "raw APPLIED objectpad verscheen zonder base-catalogauthority"
                    )
                continue
            if (
                str(object_row["relative_path"]) != identity.relative_path
                or int(object_row["size_bytes"]) != identity.size_bytes
                or not path_present
            ):
                raise RawArtifactIntegrityError(
                    "raw APPLIED heringevoerd object heeft afwijkende pathfacts"
                )
            # A new publication may have a different inode, but its canonical
            # path, exact bytes, SHA, size and single-link authority must hold.
            self._reclamation_file_identity(
                path,
                expected_size=identity.size_bytes,
                expected_sha256=identity.content_sha256,
            )
            receipt_rows = database.execute(
                self._RECEIPT_SELECT
                + " WHERE receipt.content_sha256=? ORDER BY receipt.receipt_id",
                (identity.content_sha256,),
            ).fetchall()
            if not receipt_rows:
                raise RawArtifactIntegrityError(
                    "raw APPLIED heringevoerd object mist een nieuwe receipt"
                )
            capture_ids: set[str] = set()
            for row in receipt_rows:
                receipt = self._validated_receipt(row)
                if self._reclamation_receipt_identity_is_tombstoned(
                    database,
                    receipt,
                ):
                    raise RawArtifactIntegrityError(
                        "raw APPLIED heringevoerde receipt gebruikt een tombstone-identiteit"
                    )
                member_rows = database.execute(
                    "SELECT capture_id FROM raw_artifact_capture_members "
                    "WHERE receipt_id=? ORDER BY capture_id",
                    (receipt.receipt_id,),
                ).fetchall()
                if not member_rows:
                    raise RawArtifactIntegrityError(
                        "raw APPLIED heringevoerde receipt mist een capturegraph"
                    )
                capture_ids.update(str(item["capture_id"]) for item in member_rows)
            for capture_id in sorted(capture_ids):
                capture, graph_receipts, _payloads = (
                    self._reclamation_capture_bundle(database, capture_id)
                )
                if self._reclamation_capture_identity_is_tombstoned(
                    database,
                    capture,
                ):
                    raise RawArtifactIntegrityError(
                        "raw APPLIED heringevoerde capture gebruikt een tombstone-identiteit"
                    )
                for graph_receipt in graph_receipts.values():
                    if self._reclamation_receipt_identity_is_tombstoned(
                        database,
                        graph_receipt,
                    ):
                        raise RawArtifactIntegrityError(
                            "raw APPLIED capturegraph bevat een tombstone-receipt"
                        )

    @staticmethod
    def _assert_reclamation_pending_catalog_absence(
        database: sqlite3.Connection,
        *,
        request_sha256: str,
    ) -> None:
        """Prove FILES_PENDING has no resurrected base-catalog authority."""

        checks = (
            (
                "SELECT 1 FROM raw_artifact_captures AS live "
                "JOIN raw_artifact_reclamation_candidates AS prepared "
                "ON prepared.capture_id=live.capture_id OR ("
                "prepared.provider=live.provider AND "
                "prepared.product=live.product AND ("
                "prepared.fetch_id=live.fetch_id OR "
                "prepared.upstream_capture_id=live.upstream_capture_id)) "
                "WHERE prepared.request_sha256=? LIMIT 1",
                "raw FILES_PENDING capture is opnieuw in de catalogus verschenen",
            ),
            (
                "SELECT 1 FROM raw_artifact_receipts AS live "
                "JOIN raw_artifact_reclamation_receipt_references AS prepared "
                "ON prepared.receipt_id=live.receipt_id OR ("
                "prepared.provider=live.provider AND "
                "prepared.product=live.product AND ("
                "prepared.fetch_id=live.fetch_id OR "
                "prepared.upstream_id=live.upstream_id)) "
                "WHERE prepared.request_sha256=? "
                "AND prepared.delete_eligible=1 LIMIT 1",
                "raw FILES_PENDING receipt is opnieuw in de catalogus verschenen",
            ),
            (
                "SELECT 1 FROM raw_artifact_objects AS live "
                "JOIN raw_artifact_reclamation_object_references AS prepared "
                "ON prepared.content_sha256=live.content_sha256 "
                "WHERE prepared.request_sha256=? "
                "AND prepared.delete_eligible=1 LIMIT 1",
                "raw FILES_PENDING object is opnieuw in de catalogus verschenen",
            ),
            (
                "SELECT 1 FROM raw_artifact_receipts AS live "
                "JOIN raw_artifact_reclamation_object_references AS prepared "
                "ON prepared.content_sha256=live.content_sha256 "
                "WHERE prepared.request_sha256=? "
                "AND prepared.delete_eligible=1 LIMIT 1",
                "raw FILES_PENDING content heeft een nieuwe catalogusreference",
            ),
        )
        for statement, message in checks:
            if database.execute(statement, (request_sha256,)).fetchone() is not None:
                raise RawArtifactIntegrityError(message)

    @staticmethod
    def _reclamation_path_is_absent(
        path: Path,
        *,
        directory_descriptor: int | None,
    ) -> bool:
        try:
            if directory_descriptor is None:
                path.lstat()
            else:
                os.stat(
                    path.name,
                    dir_fd=directory_descriptor,
                    follow_symlinks=False,
                )
        except FileNotFoundError:
            return True
        except OSError as exc:
            raise RawArtifactIntegrityError(
                "raw reclamation objectabsence kon niet worden bewezen"
            ) from exc
        return False

    @staticmethod
    def _assert_reclamation_descriptor_identity(
        observed: os.stat_result,
        *,
        identity: _ReclamationFileIdentity,
    ) -> None:
        if (
            int(observed.st_dev) != identity.st_dev
            or int(observed.st_ino) != identity.st_ino
            or int(observed.st_mode) != identity.st_mode
            or int(observed.st_size) != identity.size_bytes
            or not stat.S_ISREG(observed.st_mode)
        ):
            raise RawArtifactIntegrityError(
                "raw reclamation held objectauthority veranderde"
            )

    def _unlink_one_reclamation_object(
        self,
        database: sqlite3.Connection,
        *,
        request_sha256: str,
        unlink_ordinal: int,
        intent_sha256: str,
        identity: _ReclamationFileIdentity,
        files_pending_at: float,
    ) -> tuple[bool, dict[str, int]]:
        path = self._safe_object_path(
            identity.relative_path,
            identity.content_sha256,
        )
        try:
            path.lstat()
        except FileNotFoundError:
            object_was_present = False
            directory_facts = self._strict_reclamation_directory_fsync(
                self.objects_path
            )
            self._assert_reclamation_path_absent(path)
            return object_was_present, directory_facts
        except OSError as exc:
            raise RawArtifactIntegrityError(
                "raw reclamation objectstatus kon niet worden bewezen"
            ) from exc

        object_was_present = True
        with self._hold_reclamation_file_authority(
            path,
            expected_size=identity.size_bytes,
            expected_sha256=identity.content_sha256,
            expected_identity=identity,
        ) as (descriptor, _held_identity, directory_descriptor):
            just_before = os.fstat(descriptor)
            self._assert_reclamation_descriptor_identity(
                just_before,
                identity=identity,
            )
            if int(just_before.st_nlink) != 1:
                self._fence_reclamation_hardlink_alias(
                    database,
                    request_sha256=request_sha256,
                    unlink_ordinal=unlink_ordinal,
                    intent_sha256=intent_sha256,
                    phase="PRE_UNLINK",
                    observed=just_before,
                    object_path_absent=self._reclamation_path_is_absent(
                        path,
                        directory_descriptor=directory_descriptor,
                    ),
                    files_pending_at=files_pending_at,
                )
            try:
                if directory_descriptor is None:
                    path_authority = path.lstat()
                else:
                    path_authority = os.stat(
                        path.name,
                        dir_fd=directory_descriptor,
                        follow_symlinks=False,
                    )
            except OSError as exc:
                raise RawArtifactIntegrityError(
                    "raw reclamation object verdween vÃ³Ã³r unlink"
                ) from exc
            self._assert_reclamation_descriptor_identity(
                path_authority,
                identity=identity,
            )
            if int(path_authority.st_nlink) != 1 or path.is_symlink():
                raise RawArtifactIntegrityError(
                    "raw reclamation object veranderde vÃ³Ã³r unlink"
                )

            # A cooperative interleaver can add a hardlink at this exact edge.
            # The still-held descriptor makes the post-unlink nlink observable.
            self._checkpoint("reclamation_unlink_after_preunlink_reproof")
            try:
                if directory_descriptor is None:
                    path.unlink()
                else:
                    os.unlink(path.name, dir_fd=directory_descriptor)
            except OSError as exc:
                after_failure = os.fstat(descriptor)
                self._assert_reclamation_descriptor_identity(
                    after_failure,
                    identity=identity,
                )
                if int(after_failure.st_nlink) > 1:
                    self._fence_reclamation_hardlink_alias(
                        database,
                        request_sha256=request_sha256,
                        unlink_ordinal=unlink_ordinal,
                        intent_sha256=intent_sha256,
                        phase="PRE_UNLINK",
                        observed=after_failure,
                        object_path_absent=self._reclamation_path_is_absent(
                            path,
                            directory_descriptor=directory_descriptor,
                        ),
                        files_pending_at=files_pending_at,
                    )
                raise RawArtifactIntegrityError(
                    "raw reclamation objectunlink faalde"
                ) from exc

            after_unlink = os.fstat(descriptor)
            self._assert_reclamation_descriptor_identity(
                after_unlink,
                identity=identity,
            )
            path_absent = self._reclamation_path_is_absent(
                path,
                directory_descriptor=directory_descriptor,
            )
            if int(after_unlink.st_nlink) > 0:
                self._fence_reclamation_hardlink_alias(
                    database,
                    request_sha256=request_sha256,
                    unlink_ordinal=unlink_ordinal,
                    intent_sha256=intent_sha256,
                    phase="POST_UNLINK",
                    observed=after_unlink,
                    object_path_absent=path_absent,
                    files_pending_at=files_pending_at,
                )
            if int(after_unlink.st_nlink) != 0 or not path_absent:
                raise RawArtifactIntegrityError(
                    "raw reclamation unlink mist permanente pathabsence"
                )
            self._checkpoint("reclamation_unlink_after_unlink")
            directory_facts = self._strict_reclamation_directory_fsync(
                self.objects_path
            )
            if not self._reclamation_path_is_absent(
                path,
                directory_descriptor=directory_descriptor,
            ):
                raise RawArtifactIntegrityError(
                    "raw reclamation object verscheen tijdens directorybarrier"
                )
        return object_was_present, directory_facts

    def _resume_reclamation_unlink_journal(
        self,
        *,
        request: SourceReclamationRequestV2,
        prepared: SourceLocalPrepareReceiptV2,
        decision: CommittedReclamationDecisionReceiptV1,
        predecessor_apply_receipt_sha256: str,
    ) -> SourceLocalApplyReceiptV1:
        while True:
            journal_complete = False
            with self._lock, closing(self._connect()) as database:
                database.execute("BEGIN IMMEDIATE")
                try:
                    self._require_reclamation_extension(database)
                    applied = self._load_reclamation_apply(
                        database,
                        request.request_sha256,
                    )
                    if applied is not None:
                        database.commit()
                        return applied
                    catalog = self._load_reclamation_catalog_apply(
                        database,
                        request_sha256=request.request_sha256,
                    )
                    self._validate_reclamation_catalog_decision(
                        catalog=catalog,
                        decision=decision,
                        predecessor_apply_receipt_sha256=(
                            predecessor_apply_receipt_sha256
                        ),
                    )
                    events = self._load_reclamation_unlink_events(
                        database,
                        catalog=catalog,
                    )
                    self._assert_reclamation_durable_paths_absent(
                        catalog=catalog,
                        event_count=len(events),
                    )
                    self._assert_reclamation_pending_catalog_absence(
                        database,
                        request_sha256=request.request_sha256,
                    )
                    if len(events) == len(catalog["intent_rows"]):
                        database.commit()
                        journal_complete = True
                    else:
                        ordinal = len(events)
                        intent = catalog["intent_rows"][ordinal]
                        identity = catalog["identities"][ordinal]
                        predecessor = (
                            events[-1]["unlink_event_sha256"]
                            if events
                            else None
                        )
                        object_was_present, directory_facts = (
                            self._unlink_one_reclamation_object(
                                database,
                                request_sha256=request.request_sha256,
                                unlink_ordinal=ordinal,
                                intent_sha256=str(intent["intent_sha256"]),
                                identity=identity,
                                files_pending_at=float(
                                    catalog["files_pending_at"]
                                ),
                            )
                        )
                        unlinked_at = self._reclamation_now()
                        minimum_unlinked_at = (
                            float(events[-1]["unlinked_at"])
                            if events
                            else float(catalog["files_pending_at"])
                        )
                        if unlinked_at < minimum_unlinked_at:
                            raise RawArtifactConflictError(
                                "raw reclamation clock liep terug vóór "
                                "UNLINKED_DURABLE"
                            )
                        directory_barrier_sha256 = (
                            self._reclamation_directory_barrier_sha256(
                                request_sha256=request.request_sha256,
                                unlink_ordinal=ordinal,
                                intent_sha256=str(intent["intent_sha256"]),
                                directory_facts=directory_facts,
                                object_was_present=object_was_present,
                                unlinked_at=unlinked_at,
                            )
                        )
                        unlink_event_sha256 = self._reclamation_unlink_event_sha256(
                            request_sha256=request.request_sha256,
                            unlink_ordinal=ordinal,
                            intent_sha256=str(intent["intent_sha256"]),
                            predecessor_unlink_sha256=predecessor,
                            directory_barrier_sha256=directory_barrier_sha256,
                            unlinked_at=unlinked_at,
                        )
                        database.execute(
                            """
                            INSERT INTO raw_artifact_reclamation_unlink_durable(
                                request_sha256,unlink_ordinal,intent_sha256,
                                predecessor_unlink_sha256,
                                directory_barrier_sha256,directory_st_dev,
                                directory_st_ino,directory_st_mode,
                                object_was_present,unlink_event_sha256,
                                unlinked_at,state
                            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                            """,
                            (
                                request.request_sha256,
                                ordinal,
                                intent["intent_sha256"],
                                predecessor,
                                directory_barrier_sha256,
                                directory_facts["st_dev"],
                                directory_facts["st_ino"],
                                directory_facts["st_mode"],
                                int(object_was_present),
                                unlink_event_sha256,
                                unlinked_at,
                                RAW_RECLAMATION_UNLINKED_DURABLE,
                            ),
                        )
                        database.commit()
                except sqlite3.OperationalError as exc:
                    if database.in_transaction:
                        database.rollback()
                    if self._is_sqlite_capacity_error(exc):
                        raise self._catalog_capacity_error() from exc
                    raise
                except BaseException:
                    if database.in_transaction:
                        database.rollback()
                    raise
            if journal_complete:
                break
            self._storage_usage_cache = None
            self._checkpoint("reclamation_unlink_event_after_commit")

        with self._lock, closing(self._connect()) as database:
            database.execute("BEGIN IMMEDIATE")
            try:
                self._require_reclamation_extension(database)
                catalog = self._load_reclamation_catalog_apply(
                    database,
                    request_sha256=request.request_sha256,
                )
                self._validate_reclamation_catalog_decision(
                    catalog=catalog,
                    decision=decision,
                    predecessor_apply_receipt_sha256=(
                        predecessor_apply_receipt_sha256
                    ),
                )
                events = self._load_reclamation_unlink_events(
                    database,
                    catalog=catalog,
                )
                if len(events) != len(catalog["intent_rows"]):
                    raise RawArtifactIntegrityError(
                        "raw unlinkjournal is niet volledig"
                    )
                self._assert_reclamation_durable_paths_absent(
                    catalog=catalog,
                    event_count=len(events),
                )
                self._assert_reclamation_pending_catalog_absence(
                    database,
                    request_sha256=request.request_sha256,
                )
                batch = self._load_reclamation_unlinked_batch(
                    database,
                    catalog=catalog,
                )
                if batch is None:
                    completed_at = self._reclamation_now()
                    minimum_completed_at = (
                        float(events[-1]["unlinked_at"])
                        if events
                        else float(catalog["files_pending_at"])
                    )
                    if completed_at < minimum_completed_at:
                        raise RawArtifactConflictError(
                            "raw reclamation clock liep terug vóór batchdurability"
                        )
                    directory_barrier_root_sha256 = (
                        self._ordered_reclamation_root(
                            schema=(
                                "stv3-worker-raw-directory-barrier-root-v1"
                            ),
                            item_sha256s=tuple(
                                event["directory_barrier_sha256"]
                                for event in events
                            ),
                        )
                    )
                    last_event = (
                        events[-1]["unlink_event_sha256"] if events else None
                    )
                    unlinked_durable_sha256 = (
                        self._reclamation_unlinked_batch_sha256(
                            request_sha256=request.request_sha256,
                            unlink_item_root_sha256=(
                                catalog["unlink_item_root_sha256"]
                            ),
                            directory_barrier_root_sha256=(
                                directory_barrier_root_sha256
                            ),
                            last_unlink_event_sha256=last_event,
                            unlink_item_count=len(events),
                            unlinked_bytes=int(
                                catalog["row"]["unlink_bytes"]
                            ),
                            completed_at=completed_at,
                        )
                    )
                    database.execute(
                        """
                        INSERT INTO raw_artifact_reclamation_unlinked_batches(
                            request_sha256,unlink_item_root_sha256,
                            directory_barrier_root_sha256,
                            last_unlink_event_sha256,
                            unlinked_durable_sha256,unlink_item_count,
                            unlinked_bytes,completed_at,state
                        ) VALUES(?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            request.request_sha256,
                            catalog["unlink_item_root_sha256"],
                            directory_barrier_root_sha256,
                            last_event,
                            unlinked_durable_sha256,
                            len(events),
                            int(catalog["row"]["unlink_bytes"]),
                            completed_at,
                            RAW_RECLAMATION_UNLINKED_DURABLE,
                        ),
                    )
                self._checkpoint("reclamation_unlinked_before_commit")
                database.commit()
            except sqlite3.OperationalError as exc:
                if database.in_transaction:
                    database.rollback()
                if self._is_sqlite_capacity_error(exc):
                    raise self._catalog_capacity_error() from exc
                raise
            except BaseException:
                if database.in_transaction:
                    database.rollback()
                raise
        self._checkpoint("reclamation_unlinked_after_commit")
        return self._finalize_reclamation_apply(
            request=request,
            prepared=prepared,
            decision=decision,
            predecessor_apply_receipt_sha256=(
                predecessor_apply_receipt_sha256
            ),
        )

    def _load_reclamation_tombstone_roots(
        self,
        database: sqlite3.Connection,
        *,
        catalog: Mapping[str, Any],
    ) -> tuple[str, str]:
        request_sha256 = str(catalog["row"]["request_sha256"])
        catalog_apply_sha256 = str(catalog["catalog_apply_sha256"])
        created_at = float(catalog["files_pending_at"])
        candidate_rows = database.execute(
            "SELECT * FROM raw_artifact_reclamation_candidates "
            "WHERE request_sha256=? ORDER BY capture_id",
            (request_sha256,),
        ).fetchall()
        capture_rows = database.execute(
            "SELECT * FROM raw_artifact_reclamation_capture_tombstones "
            "WHERE request_sha256=? ORDER BY capture_id",
            (request_sha256,),
        ).fetchall()
        candidates = {
            str(row["capture_id"]): row for row in candidate_rows
        }
        if len(capture_rows) != len(candidates):
            raise RawArtifactIntegrityError(
                "raw capture-tombstoneinventory verschilt"
            )
        capture_hashes: list[str] = []
        for row in capture_rows:
            capture_id = str(row["capture_id"])
            candidate = candidates.get(capture_id)
            if candidate is None:
                raise RawArtifactIntegrityError(
                    "raw capture-tombstone heeft geen prepared candidate"
                )
            tombstone_sha256 = reclamation_digest_mapping({
                "capture_id": capture_id,
                "capture_receipt_sha256": str(
                    candidate["capture_receipt_sha256"]
                ),
                "catalog_apply_sha256": catalog_apply_sha256,
                "fetch_id": str(candidate["fetch_id"]),
                "manifest_sha256": str(candidate["manifest_sha256"]),
                "product": str(candidate["product"]),
                "provider": str(candidate["provider"]),
                "request_sha256": request_sha256,
                "schema": RAW_ARTIFACT_RECLAMATION_TOMBSTONE_SCHEMA,
                "tombstone_kind": "capture_identity",
                "upstream_capture_id": str(
                    candidate["upstream_capture_id"]
                ),
            })
            if (
                str(row["provider"]) != str(candidate["provider"])
                or str(row["product"]) != str(candidate["product"])
                or str(row["fetch_id"]) != str(candidate["fetch_id"])
                or str(row["upstream_capture_id"])
                != str(candidate["upstream_capture_id"])
                or str(row["manifest_sha256"])
                != str(candidate["manifest_sha256"])
                or str(row["capture_receipt_sha256"])
                != str(candidate["capture_receipt_sha256"])
                or str(row["catalog_apply_sha256"])
                != catalog_apply_sha256
                or str(row["tombstone_sha256"]) != tombstone_sha256
                or float(row["created_at"]) != created_at
            ):
                raise RawArtifactIntegrityError(
                    "raw capture-tombstonefacts verschillen"
                )
            capture_hashes.append(tombstone_sha256)

        reference_rows = database.execute(
            "SELECT * FROM raw_artifact_reclamation_receipt_references "
            "WHERE request_sha256=? AND delete_eligible=1 "
            "ORDER BY receipt_id",
            (request_sha256,),
        ).fetchall()
        receipt_rows = database.execute(
            "SELECT * FROM raw_artifact_reclamation_receipt_tombstones "
            "WHERE request_sha256=? ORDER BY receipt_id",
            (request_sha256,),
        ).fetchall()
        references = {
            str(row["receipt_id"]): row for row in reference_rows
        }
        if len(receipt_rows) != len(references):
            raise RawArtifactIntegrityError(
                "raw receipt-tombstoneinventory verschilt"
            )
        receipt_hashes: list[str] = []
        for row in receipt_rows:
            receipt_id = str(row["receipt_id"])
            reference = references.get(receipt_id)
            if reference is None:
                raise RawArtifactIntegrityError(
                    "raw receipt-tombstone heeft geen prepared reference"
                )
            tombstone_sha256 = reclamation_digest_mapping({
                "catalog_apply_sha256": catalog_apply_sha256,
                "fetch_id": str(reference["fetch_id"]),
                "product": str(reference["product"]),
                "provider": str(reference["provider"]),
                "receipt_id": receipt_id,
                "receipt_sha256": str(reference["receipt_sha256"]),
                "request_sha256": request_sha256,
                "schema": RAW_ARTIFACT_RECLAMATION_TOMBSTONE_SCHEMA,
                "tombstone_kind": "receipt_identity",
                "upstream_id": str(reference["upstream_id"]),
            })
            if (
                str(row["provider"]) != str(reference["provider"])
                or str(row["product"]) != str(reference["product"])
                or str(row["fetch_id"]) != str(reference["fetch_id"])
                or str(row["upstream_id"])
                != str(reference["upstream_id"])
                or str(row["receipt_sha256"])
                != str(reference["receipt_sha256"])
                or str(row["catalog_apply_sha256"])
                != catalog_apply_sha256
                or str(row["tombstone_sha256"]) != tombstone_sha256
                or float(row["created_at"]) != created_at
            ):
                raise RawArtifactIntegrityError(
                    "raw receipt-tombstonefacts verschillen"
                )
            receipt_hashes.append(tombstone_sha256)
        return (
            self._ordered_reclamation_root(
                schema="stv3-worker-raw-capture-tombstone-root-v1",
                item_sha256s=tuple(capture_hashes),
            ),
            self._ordered_reclamation_root(
                schema="stv3-worker-raw-receipt-tombstone-root-v1",
                item_sha256s=tuple(receipt_hashes),
            ),
        )

    def _finalize_reclamation_apply(
        self,
        *,
        request: SourceReclamationRequestV2,
        prepared: SourceLocalPrepareReceiptV2,
        decision: CommittedReclamationDecisionReceiptV1,
        predecessor_apply_receipt_sha256: str,
    ) -> SourceLocalApplyReceiptV1:
        with self._lock, closing(self._connect()) as database:
            database.execute("BEGIN IMMEDIATE")
            try:
                self._require_reclamation_extension(database)
                catalog = self._load_reclamation_catalog_apply(
                    database,
                    request_sha256=request.request_sha256,
                )
                self._validate_reclamation_catalog_decision(
                    catalog=catalog,
                    decision=decision,
                    predecessor_apply_receipt_sha256=(
                        predecessor_apply_receipt_sha256
                    ),
                )
                existing = self._load_reclamation_apply(
                    database,
                    request.request_sha256,
                )
                if existing is not None:
                    database.commit()
                    return existing
                batch = self._load_reclamation_unlinked_batch(
                    database,
                    catalog=catalog,
                )
                if batch is None:
                    raise RawArtifactIntegrityError(
                        "raw apply mist UNLINKED_DURABLE"
                    )
                self._assert_reclamation_durable_paths_absent(
                    catalog=catalog,
                    event_count=len(catalog["intent_rows"]),
                )
                self._assert_reclamation_pending_catalog_absence(
                    database,
                    request_sha256=request.request_sha256,
                )
                capture_tombstone_root, receipt_tombstone_root = (
                    self._load_reclamation_tombstone_roots(
                        database,
                        catalog=catalog,
                    )
                )
                journal = RawUnlinkJournalReceiptV1.create(
                    request_sha256=request.request_sha256,
                    raw_store_identity_sha256=(
                        prepared.source_store_identity_sha256
                    ),
                    prepare_receipt_sha256=(
                        prepared.prepare_receipt_sha256
                    ),
                    committed_decision_receipt_sha256=(
                        decision.receipt_sha256
                    ),
                    catalog_apply_sha256=catalog["catalog_apply_sha256"],
                    shared_reference_root_sha256=(
                        prepared.child_reference_root_sha256
                    ),
                    unlink_item_root_sha256=(
                        catalog["unlink_item_root_sha256"]
                    ),
                    directory_barrier_root_sha256=(
                        batch["directory_barrier_root_sha256"]
                    ),
                    unlink_item_count=len(catalog["intent_rows"]),
                    unlinked_bytes=int(catalog["row"]["unlink_bytes"]),
                    completed_at=float(batch["completed_at"]),
                )
                effect_sha256 = reclamation_digest_mapping({
                    "catalog_apply_sha256": catalog["catalog_apply_sha256"],
                    "capture_tombstone_root_sha256": (
                        capture_tombstone_root
                    ),
                    "journal_receipt_sha256": journal.journal_receipt_sha256,
                    "receipt_tombstone_root_sha256": (
                        receipt_tombstone_root
                    ),
                    "request_sha256": request.request_sha256,
                    "schema": "stv3-worker-raw-reclamation-effect-v1",
                    "unlinked_durable_sha256": (
                        batch["unlinked_durable_sha256"]
                    ),
                })
                post_apply_view_sha256 = reclamation_digest_mapping({
                    "candidate_bindings_sha256": (
                        request.candidate_bindings_sha256
                    ),
                    "capture_tombstone_root_sha256": (
                        capture_tombstone_root
                    ),
                    "receipt_tombstone_root_sha256": (
                        receipt_tombstone_root
                    ),
                    "request_sha256": request.request_sha256,
                    "schema": "stv3-worker-raw-reclamation-post-apply-view-v1",
                    "source_store_identity_sha256": (
                        prepared.source_store_identity_sha256
                    ),
                })
                applied_at = self._reclamation_now()
                if applied_at < float(batch["completed_at"]):
                    raise RawArtifactConflictError(
                        "raw reclamation clock liep terug vóór APPLIED"
                    )
                receipt = SourceLocalApplyReceiptV1.create(
                    request_sha256=request.request_sha256,
                    role=self.role,
                    source_store_identity_sha256=(
                        prepared.source_store_identity_sha256
                    ),
                    prepare_receipt_sha256=(
                        prepared.prepare_receipt_sha256
                    ),
                    committed_decision_sha256=decision.decision_sha256,
                    committed_decision_receipt_sha256=decision.receipt_sha256,
                    predecessor_apply_receipt_sha256=(
                        predecessor_apply_receipt_sha256
                    ),
                    effect_sha256=effect_sha256,
                    post_apply_view_sha256=post_apply_view_sha256,
                    released_bytes=int(catalog["row"]["unlink_bytes"]),
                    released_entries=len(catalog["intent_rows"]),
                    applied_at=applied_at,
                )
                database.execute(
                    """
                    INSERT INTO raw_artifact_reclamation_applies(
                        request_sha256,apply_receipt_sha256,
                        apply_receipt_json,journal_receipt_sha256,
                        journal_receipt_json,effect_sha256,
                        post_apply_view_sha256,released_bytes,
                        released_entries,applied_at,state
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        request.request_sha256,
                        receipt.apply_receipt_sha256,
                        self._protocol_json(receipt.as_dict()),
                        journal.journal_receipt_sha256,
                        self._protocol_json(journal.as_dict()),
                        receipt.effect_sha256,
                        receipt.post_apply_view_sha256,
                        receipt.released_bytes,
                        receipt.released_entries,
                        receipt.applied_at,
                        RAW_RECLAMATION_APPLIED,
                    ),
                )
                self._checkpoint("reclamation_apply_before_commit")
                database.commit()
            except sqlite3.OperationalError as exc:
                if database.in_transaction:
                    database.rollback()
                if self._is_sqlite_capacity_error(exc):
                    raise self._catalog_capacity_error() from exc
                raise
            except sqlite3.IntegrityError as exc:
                if database.in_transaction:
                    database.rollback()
                raise RawArtifactConflictError(
                    "raw APPLIED receipt kon niet atomair worden vastgelegd"
                ) from exc
            except BaseException:
                if database.in_transaction:
                    database.rollback()
                raise
        self._checkpoint("reclamation_apply_after_commit")
        return receipt

    def _load_reclamation_apply(
        self,
        database: sqlite3.Connection,
        request_sha256: str,
    ) -> SourceLocalApplyReceiptV1 | None:
        row = database.execute(
            "SELECT * FROM raw_artifact_reclamation_applies "
            "WHERE request_sha256=?",
            (request_sha256,),
        ).fetchone()
        if row is None:
            return None
        receipt = self._protocol_receipt_from_json(
            str(row["apply_receipt_json"]),
            SourceLocalApplyReceiptV1,
        )
        journal = self._protocol_receipt_from_json(
            str(row["journal_receipt_json"]),
            RawUnlinkJournalReceiptV1,
        )
        if not isinstance(receipt, SourceLocalApplyReceiptV1) or not isinstance(
            journal,
            RawUnlinkJournalReceiptV1,
        ):
            raise RawArtifactIntegrityError(
                "raw reclamation applyreceipt heeft verkeerd type"
            )
        prepare = self._load_reclamation_prepare(database, request_sha256)
        request = self._load_reclamation_request(database, request_sha256)
        catalog = self._load_reclamation_catalog_apply(
            database,
            request_sha256=request_sha256,
        )
        batch = self._load_reclamation_unlinked_batch(
            database,
            catalog=catalog,
        )
        if batch is None:
            raise RawArtifactIntegrityError(
                "raw APPLIED receipt mist UNLINKED_DURABLE"
            )
        capture_tombstone_root, receipt_tombstone_root = (
            self._load_reclamation_tombstone_roots(
                database,
                catalog=catalog,
            )
        )
        self._assert_reclamation_applied_replay_authority(
            database,
            catalog=catalog,
        )
        expected_effect_sha256 = reclamation_digest_mapping({
            "catalog_apply_sha256": catalog["catalog_apply_sha256"],
            "capture_tombstone_root_sha256": capture_tombstone_root,
            "journal_receipt_sha256": journal.journal_receipt_sha256,
            "receipt_tombstone_root_sha256": receipt_tombstone_root,
            "request_sha256": request_sha256,
            "schema": "stv3-worker-raw-reclamation-effect-v1",
            "unlinked_durable_sha256": batch["unlinked_durable_sha256"],
        })
        expected_post_apply_view_sha256 = reclamation_digest_mapping({
            "candidate_bindings_sha256": request.candidate_bindings_sha256,
            "capture_tombstone_root_sha256": capture_tombstone_root,
            "receipt_tombstone_root_sha256": receipt_tombstone_root,
            "request_sha256": request_sha256,
            "schema": "stv3-worker-raw-reclamation-post-apply-view-v1",
            "source_store_identity_sha256": (
                prepare.source_store_identity_sha256
                if prepare is not None
                else ""
            ),
        })
        stored_decision = catalog["decision"]
        if (
            prepare is None
            or receipt.request_sha256 != request_sha256
            or receipt.role != self.role
            or receipt.source_store_identity_sha256
            != self._load_reclamation_store_identity(database)
            or receipt.prepare_receipt_sha256 != prepare.prepare_receipt_sha256
            or receipt.committed_decision_sha256
            != stored_decision.decision_sha256
            or receipt.committed_decision_receipt_sha256
            != stored_decision.receipt_sha256
            or receipt.predecessor_apply_receipt_sha256
            != catalog["predecessor_apply_receipt_sha256"]
            or str(row["apply_receipt_sha256"]) != receipt.apply_receipt_sha256
            or str(row["journal_receipt_sha256"])
            != journal.journal_receipt_sha256
            or str(row["effect_sha256"]) != receipt.effect_sha256
            or str(row["post_apply_view_sha256"])
            != receipt.post_apply_view_sha256
            or int(row["released_bytes"]) != receipt.released_bytes
            or int(row["released_entries"]) != receipt.released_entries
            or float(row["applied_at"]) != receipt.applied_at
            or str(row["state"]) != RAW_RECLAMATION_APPLIED
            or receipt.applied_at < float(batch["completed_at"])
            or receipt.effect_sha256 != expected_effect_sha256
            or receipt.post_apply_view_sha256
            != expected_post_apply_view_sha256
            or journal.request_sha256 != request_sha256
            or journal.raw_store_identity_sha256
            != receipt.source_store_identity_sha256
            or journal.prepare_receipt_sha256 != prepare.prepare_receipt_sha256
            or journal.committed_decision_receipt_sha256
            != stored_decision.receipt_sha256
            or journal.catalog_apply_sha256
            != catalog["catalog_apply_sha256"]
            or journal.shared_reference_root_sha256
            != prepare.child_reference_root_sha256
            or journal.unlink_item_root_sha256
            != catalog["unlink_item_root_sha256"]
            or journal.directory_barrier_root_sha256
            != batch["directory_barrier_root_sha256"]
            or journal.unlink_item_count != len(catalog["intent_rows"])
            or journal.unlinked_bytes != int(catalog["row"]["unlink_bytes"])
            or journal.completed_at != float(batch["completed_at"])
        ):
            raise RawArtifactIntegrityError(
                "raw reclamation applybindings verschillen"
            )
        return receipt

    def _load_reclamation_journal(
        self,
        database: sqlite3.Connection,
        request_sha256: str,
    ) -> RawUnlinkJournalReceiptV1 | None:
        row = database.execute(
            "SELECT journal_receipt_json FROM raw_artifact_reclamation_applies "
            "WHERE request_sha256=?",
            (request_sha256,),
        ).fetchone()
        if row is None:
            return None
        if self._load_reclamation_apply(database, request_sha256) is None:
            raise RawArtifactIntegrityError(
                "raw unlinkjournal mist zijn APPLIED receipt"
            )
        journal = self._protocol_receipt_from_json(
            str(row["journal_receipt_json"]),
            RawUnlinkJournalReceiptV1,
        )
        if not isinstance(journal, RawUnlinkJournalReceiptV1):
            raise RawArtifactIntegrityError(
                "raw reclamation unlinkjournal heeft verkeerd type"
            )
        return journal

    def _load_reclamation_abort(
        self,
        database: sqlite3.Connection,
        request_sha256: str,
    ) -> SourceLocalAbortReceiptV1 | None:
        row = database.execute(
            "SELECT * FROM raw_artifact_reclamation_aborts "
            "WHERE request_sha256=?",
            (request_sha256,),
        ).fetchone()
        if row is None:
            return None
        receipt = self._protocol_receipt_from_json(
            str(row["abort_receipt_json"]),
            SourceLocalAbortReceiptV1,
        )
        decision = self._protocol_receipt_from_json(
            str(row["coordinator_abort_receipt_json"]),
            CoordinatorAbortDecisionReceiptV1,
        )
        if not isinstance(receipt, SourceLocalAbortReceiptV1) or not isinstance(
            decision,
            CoordinatorAbortDecisionReceiptV1,
        ):
            raise RawArtifactIntegrityError(
                "raw reclamation abortreceipt heeft verkeerd type"
            )
        prepare = self._load_reclamation_prepare(database, request_sha256)
        if (
            prepare is None
            or receipt.request_sha256 != request_sha256
            or receipt.role != self.role
            or receipt.source_store_identity_sha256
            != self._load_reclamation_store_identity(database)
            or receipt.prepare_receipt_sha256 != prepare.prepare_receipt_sha256
            or receipt.coordinator_abort_decision_sha256
            != decision.abort_decision_sha256
            or decision.request_sha256 != request_sha256
            or str(row["abort_receipt_sha256"])
            != receipt.abort_receipt_sha256
            or str(row["coordinator_abort_decision_sha256"])
            != decision.abort_decision_sha256
            or str(row["coordinator_abort_receipt_sha256"])
            != decision.receipt_sha256
            or str(row["fence_release_root_sha256"])
            != receipt.fence_release_root_sha256
            or float(row["aborted_at"]) != receipt.aborted_at
            or str(row["state"]) != RAW_RECLAMATION_ABORTED
        ):
            raise RawArtifactIntegrityError(
                "raw reclamation abortbindings verschillen"
            )
        return receipt

    def _hold_reclamation_candidate_evidence(
        self,
        request: SourceReclamationRequestV2,
        stack: ExitStack,
    ) -> tuple[_ReclamationCandidateEvidence, ...]:
        authority = self._require_candidate_authority()
        if request.candidate_authority_sha256 != authority.authority_sha256:
            raise RawArtifactConflictError(
                "raw reclamation request bindt een andere candidate authority"
            )
        after_cursor = 0
        evidence: list[_ReclamationCandidateEvidence] = []
        while after_cursor < request.candidate_count:
            page = stack.enter_context(authority.hold_candidate_page_v1(
                request,
                after_cursor=after_cursor,
                max_candidates=min(
                    RECLAMATION_MAX_PAGE_CANDIDATES,
                    request.candidate_count - after_cursor,
                ),
            ))
            if (
                page.request_sha256 != request.request_sha256
                or page.candidate_authority_sha256
                != request.candidate_authority_sha256
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
                or page.after_cursor != after_cursor
                or page.total_candidate_count != request.candidate_count
            ):
                raise RawArtifactConflictError(
                    "raw reclamation candidate page bindt andere authority"
                )
            records = page.decoded_records()
            for binding, metadata_reference, record in zip(
                page.bindings,
                page.record_payloads,
                records,
                strict=True,
            ):
                evidence.append(_ReclamationCandidateEvidence(
                    binding=binding,
                    metadata_reference=metadata_reference,
                    record=record,
                ))
            after_cursor = page.next_cursor
            if page.exhausted:
                break
        if (
            after_cursor != request.candidate_count
            or len(evidence) != request.candidate_count
        ):
            raise RawArtifactIntegrityError(
                "raw reclamation candidate pagination is onvolledig"
            )
        bindings = tuple(item.binding for item in evidence)
        metadata_references = tuple(
            item.metadata_reference for item in evidence
        )
        if (
            reclamation_candidate_binding_root(bindings)
            != request.candidate_bindings_sha256
            or reclamation_archive_binding_root(bindings, metadata_references)
            != request.archive_binding_root_sha256
        ):
            raise RawArtifactIntegrityError(
                "raw reclamation complete candidate roots verschillen"
            )
        return tuple(evidence)

    def _build_reclamation_prepare_plan(
        self,
        database: sqlite3.Connection,
        *,
        request: SourceReclamationRequestV2,
        evidence: tuple[_ReclamationCandidateEvidence, ...],
        store_identity_sha256: str,
        exclude_active_request_sha256: str | None = None,
    ) -> _ReclamationPreparePlan:
        candidate_rows: list[dict[str, Any]] = []
        member_rows: list[dict[str, Any]] = []
        candidate_views: list[tuple[str, str]] = []
        receipt_facts: dict[str, RawArtifactReceipt] = {}
        candidate_receipt_references: dict[str, int] = {}
        content_sizes: dict[str, int] = {}
        capture_ids: set[str] = set()

        for ordinal, candidate in enumerate(evidence):
            binding = candidate.binding
            if binding.raw_capture_id in capture_ids:
                raise RawArtifactIntegrityError(
                    "raw reclamation candidate herhaalt capture-ID"
                )
            capture_ids.add(binding.raw_capture_id)
            capture, receipts, payloads = self._reclamation_capture_bundle(
                database,
                binding.raw_capture_id,
            )
            duplicate_capture_identity = database.execute(
                """
                SELECT capture_id FROM raw_artifact_captures
                WHERE provider=? AND product=? AND upstream_capture_id=?
                  AND capture_id<>?
                LIMIT 1
                """,
                (
                    capture.provider,
                    capture.product,
                    capture.upstream_capture_id,
                    capture.capture_id,
                ),
            ).fetchone()
            if duplicate_capture_identity is not None:
                raise RawArtifactConflictError(
                    "raw reclamation capture heeft een levende dubbele "
                    "upstream-identiteit"
                )
            projection = {
                "raw_capture": capture.to_metadata(),
                "raw_objects": {
                    role: {
                        "content_sha256": hashlib.sha256(payload).hexdigest(),
                        "size_bytes": len(payload),
                    }
                    for role, payload in sorted(payloads.items())
                },
                "raw_receipts": {
                    role: self._reclamation_receipt_view(receipt)
                    for role, receipt in sorted(receipts.items())
                },
            }
            current_view_sha256 = reclamation_candidate_view_sha256(
                role=self.role,
                candidate_sha256=binding.candidate_sha256,
                projection=projection,
            )
            sealed_view_sha256 = candidate_participant_view_sha256(
                candidate.record,
                self.role,
            )
            if current_view_sha256 != sealed_view_sha256:
                raise RawArtifactConflictError(
                    "sealed raw candidateprojectie verschilt van source"
                )
            candidate_views.append((
                binding.candidate_sha256,
                current_view_sha256,
            ))
            record = candidate.record
            candidate_rows.append({
                "request_sha256": request.request_sha256,
                "candidate_ordinal": ordinal,
                "binding_sha256": binding.binding_sha256,
                "candidate_sha256": binding.candidate_sha256,
                "acquisition_job_id": binding.acquisition_job_id,
                "scheduler_job_id": binding.scheduler_job_id,
                "record_sha256": binding.sealed_archive_record_sha256,
                "segment_sha256": binding.sealed_archive_segment_sha256,
                "metadata_reference": candidate.metadata_reference,
                "metadata_reference_sha256": hashlib.sha256(
                    candidate.metadata_reference
                ).hexdigest(),
                "metadata_sha256": str(record["metadata_sha256"]),
                "archive_object_count": int(record["archive_object_count"]),
                "archive_object_reference_root_sha256": str(
                    record["archive_object_reference_root_sha256"]
                ),
                "archive_object_total_bytes": int(
                    record["archive_object_total_bytes"]
                ),
                "downstream_lifecycle_view_sha256": str(
                    record["downstream_lifecycle_view_sha256"]
                ),
                "execution_receipts_view_sha256": str(
                    record["execution_receipts_view_sha256"]
                ),
                "scheduler_view_sha256": str(
                    record["scheduler_view_sha256"]
                ),
                "raw_artifacts_view_sha256": str(
                    record["raw_artifacts_view_sha256"]
                ),
                "capacity_ledger_view_sha256": str(
                    record["capacity_ledger_view_sha256"]
                ),
                "capture_id": capture.capture_id,
                "capture_receipt_sha256": capture.capture_receipt_sha256,
                "provider": capture.provider,
                "product": capture.product,
                "fetch_id": capture.fetch_id,
                "upstream_capture_id": capture.upstream_capture_id,
                "manifest_sha256": capture.manifest_sha256,
                "evidence_stream_id": binding.evidence_stream_id,
                "evidence_stream_sequence": binding.evidence_stream_sequence,
                "consumer_closure_receipt_sha256": (
                    binding.consumer_closure_receipt_sha256
                ),
                "consumer_closure_root_sha256": (
                    binding.consumer_closure_root_sha256
                ),
            })
            for role, receipt in sorted(receipts.items()):
                known = receipt_facts.get(receipt.receipt_id)
                if known is not None and known != receipt:
                    raise RawArtifactIntegrityError(
                        "raw reclamation receiptfacts verschillen tussen captures"
                    )
                receipt_facts[receipt.receipt_id] = receipt
                candidate_receipt_references[receipt.receipt_id] = (
                    candidate_receipt_references.get(receipt.receipt_id, 0) + 1
                )
                content_sizes.setdefault(
                    receipt.content_sha256,
                    receipt.size_bytes,
                )
                if content_sizes[receipt.content_sha256] != receipt.size_bytes:
                    raise RawArtifactIntegrityError(
                        "raw reclamation contentgrootte verschilt"
                    )
                member_rows.append({
                    "request_sha256": request.request_sha256,
                    "capture_id": capture.capture_id,
                    "role": role,
                    "receipt_id": receipt.receipt_id,
                    "receipt_sha256": receipt.receipt_sha256,
                    "content_sha256": receipt.content_sha256,
                    "size_bytes": receipt.size_bytes,
                })

        source_view_sha256 = reclamation_inventory_view_sha256(
            role=self.role,
            candidate_views=tuple(candidate_views),
        )
        all_capture_counts = {
            str(row["receipt_id"]): int(row["reference_count"])
            for row in database.execute(
                "SELECT receipt_id,count(*) AS reference_count "
                "FROM raw_artifact_capture_members GROUP BY receipt_id"
            )
        }
        receipt_rows: list[dict[str, Any]] = []
        deleted_receipt_ids: set[str] = set()
        for receipt_id in sorted(receipt_facts):
            receipt = receipt_facts[receipt_id]
            total_references = all_capture_counts.get(receipt_id, 0)
            candidate_references = candidate_receipt_references[receipt_id]
            if candidate_references > total_references:
                raise RawArtifactIntegrityError(
                    "raw reclamation capture-referencecount is onmogelijk"
                )
            delete_eligible = candidate_references == total_references
            if delete_eligible:
                duplicate_receipt_identity = database.execute(
                    """
                    SELECT receipt_id FROM raw_artifact_receipts
                    WHERE provider=? AND product=? AND upstream_id=?
                      AND receipt_id<>?
                    LIMIT 1
                    """,
                    (
                        receipt.provider,
                        receipt.product,
                        receipt.upstream_id,
                        receipt.receipt_id,
                    ),
                ).fetchone()
                if duplicate_receipt_identity is not None:
                    raise RawArtifactConflictError(
                        "raw reclamation receipt heeft een levende dubbele "
                        "upstream-identiteit"
                    )
                deleted_receipt_ids.add(receipt_id)
            receipt_rows.append({
                "request_sha256": request.request_sha256,
                "receipt_id": receipt.receipt_id,
                "provider": receipt.provider,
                "product": receipt.product,
                "fetch_id": receipt.fetch_id,
                "upstream_id": receipt.upstream_id,
                "receipt_sha256": receipt.receipt_sha256,
                "content_sha256": receipt.content_sha256,
                "size_bytes": receipt.size_bytes,
                "total_capture_references": total_references,
                "candidate_capture_references": candidate_references,
                "delete_eligible": int(delete_eligible),
            })

        all_receipt_counts = {
            str(row["content_sha256"]): int(row["reference_count"])
            for row in database.execute(
                "SELECT content_sha256,count(*) AS reference_count "
                "FROM raw_artifact_receipts GROUP BY content_sha256"
            )
        }
        deleted_receipts_by_content: dict[str, int] = {}
        for receipt_id in deleted_receipt_ids:
            content_sha256 = receipt_facts[receipt_id].content_sha256
            deleted_receipts_by_content[content_sha256] = (
                deleted_receipts_by_content.get(content_sha256, 0) + 1
            )
        object_rows: list[dict[str, Any]] = []
        for content_sha256 in sorted(content_sizes):
            total_references = all_receipt_counts.get(content_sha256, 0)
            deleted_references = deleted_receipts_by_content.get(
                content_sha256,
                0,
            )
            if deleted_references > total_references or total_references < 1:
                raise RawArtifactIntegrityError(
                    "raw reclamation receipt-referencecount is onmogelijk"
                )
            delete_eligible = deleted_references == total_references
            relative_path = self._relative_object_path(content_sha256)
            size_bytes = content_sizes[content_sha256]
            if delete_eligible:
                self._reclamation_file_identity(
                    self._safe_object_path(relative_path, content_sha256),
                    expected_size=size_bytes,
                    expected_sha256=content_sha256,
                )
            object_rows.append({
                "request_sha256": request.request_sha256,
                "content_sha256": content_sha256,
                "relative_path": relative_path,
                "size_bytes": size_bytes,
                "total_receipt_references": total_references,
                "delete_receipt_references": deleted_references,
                "delete_eligible": int(delete_eligible),
            })

        active_capture_ids = {
            str(row["capture_id"])
            for row in database.execute(
                """
                SELECT candidate.capture_id,candidate.request_sha256
                FROM raw_artifact_reclamation_candidates AS candidate
                LEFT JOIN raw_artifact_reclamation_aborts AS aborted
                  USING(request_sha256)
                LEFT JOIN raw_artifact_reclamation_applies AS applied
                  USING(request_sha256)
                WHERE aborted.request_sha256 IS NULL
                  AND applied.request_sha256 IS NULL
                """
            )
            if (
                exclude_active_request_sha256 is None
                or str(row["request_sha256"])
                != exclude_active_request_sha256
            )
        }
        active_receipt_ids = {
            str(row["receipt_id"])
            for row in database.execute(
                """
                SELECT reference.receipt_id,reference.request_sha256
                FROM raw_artifact_reclamation_receipt_references AS reference
                LEFT JOIN raw_artifact_reclamation_aborts AS aborted
                  USING(request_sha256)
                LEFT JOIN raw_artifact_reclamation_applies AS applied
                  USING(request_sha256)
                WHERE aborted.request_sha256 IS NULL
                  AND applied.request_sha256 IS NULL
                """
            )
            if (
                exclude_active_request_sha256 is None
                or str(row["request_sha256"])
                != exclude_active_request_sha256
            )
        }
        active_object_ids = {
            str(row["content_sha256"])
            for row in database.execute(
                """
                SELECT reference.content_sha256,reference.request_sha256
                FROM raw_artifact_reclamation_object_references AS reference
                LEFT JOIN raw_artifact_reclamation_aborts AS aborted
                  USING(request_sha256)
                LEFT JOIN raw_artifact_reclamation_applies AS applied
                  USING(request_sha256)
                WHERE aborted.request_sha256 IS NULL
                  AND applied.request_sha256 IS NULL
                """
            )
            if (
                exclude_active_request_sha256 is None
                or str(row["request_sha256"])
                != exclude_active_request_sha256
            )
        }
        if (
            active_capture_ids & capture_ids
            or active_receipt_ids & set(receipt_facts)
            or active_object_ids & set(content_sizes)
        ):
            raise RawArtifactConflictError(
                "raw reclamation candidateclosure is al source-lokaal fenced"
            )

        capture_root = self._ordered_reclamation_root(
            schema="stv3-worker-raw-reclamation-capture-reference-root-v1",
            item_sha256s=tuple(
                reclamation_digest_mapping({
                    "capture_id": row["capture_id"],
                    "capture_receipt_sha256": row["capture_receipt_sha256"],
                    "candidate_sha256": row["candidate_sha256"],
                })
                for row in candidate_rows
            ),
        )
        receipt_root = self._ordered_reclamation_root(
            schema="stv3-worker-raw-reclamation-receipt-reference-root-v1",
            item_sha256s=tuple(
                reclamation_digest_mapping(row)
                for row in receipt_rows
            ),
        )
        object_root = self._ordered_reclamation_root(
            schema="stv3-worker-raw-reclamation-object-reference-root-v1",
            item_sha256s=tuple(
                reclamation_digest_mapping(row)
                for row in object_rows
            ),
        )
        child_reference_root_sha256 = reclamation_digest_mapping({
            "apply_order": [
                "capture_members",
                "captures",
                "receipts_at_zero_capture_references",
                "objects_at_zero_receipt_references",
            ],
            "capture_reference_root_sha256": capture_root,
            "object_reference_root_sha256": object_root,
            "receipt_reference_root_sha256": receipt_root,
            "schema": "stv3-worker-raw-reference-closure-prepare-v1",
        })
        releasable_bytes = sum(
            int(row["size_bytes"])
            for row in object_rows
            if int(row["delete_eligible"]) == 1
        )
        releasable_entries = sum(
            int(row["delete_eligible"])
            for row in object_rows
        )
        source_fence_sha256 = reclamation_digest_mapping({
            "candidate_bindings_sha256": request.candidate_bindings_sha256,
            "child_reference_root_sha256": child_reference_root_sha256,
            "request_sha256": request.request_sha256,
            "schema": "stv3-worker-raw-reclamation-source-fence-v1",
            "source_store_identity_sha256": store_identity_sha256,
            "source_view_sha256": source_view_sha256,
        })
        return _ReclamationPreparePlan(
            candidate_rows=tuple(candidate_rows),
            member_rows=tuple(member_rows),
            receipt_rows=tuple(receipt_rows),
            object_rows=tuple(object_rows),
            source_view_sha256=source_view_sha256,
            child_reference_root_sha256=child_reference_root_sha256,
            source_fence_sha256=source_fence_sha256,
            releasable_bytes=releasable_bytes,
            releasable_entries=releasable_entries,
        )

    def _validated_receipt(self, row: sqlite3.Row) -> RawArtifactReceipt:
        provider = self._identifier(row["provider"], "provider", 128)
        product = self._identifier(row["product"], "product", 256)
        fetch_id = self._identifier(row["fetch_id"], "fetch_id", 512)
        content_type = self._identifier(row["content_type"], "content_type", 256)
        upstream_id = self._identifier(row["upstream_id"], "upstream_id", 1024)
        received_at = self._timestamp(row["received_at"], "received_at")
        content_sha256 = str(row["content_sha256"])
        if not _SHA256_RE.fullmatch(content_sha256):
            raise RawArtifactIntegrityError("raw-artifact heeft ongeldige SHA-256")
        size_bytes = int(row["size_bytes"])
        if size_bytes < 0 or size_bytes != int(row["object_size_bytes"]):
            raise RawArtifactIntegrityError("raw-artifactgrootte is inconsistent")
        provenance_json = str(row["provenance_json"])
        try:
            provenance = json.loads(provenance_json)
        except (TypeError, json.JSONDecodeError) as exc:
            raise RawArtifactIntegrityError("raw-artifactprovenance is ongeldig") from exc
        canonical_provenance, provenance_sha256 = self._provenance(provenance)
        if (
            canonical_provenance != provenance_json
            or provenance_sha256 != row["provenance_sha256"]
        ):
            raise RawArtifactIntegrityError("raw-artifactprovenancechecksum klopt niet")
        expected_receipt_sha = self._receipt_sha256(
            provider=provider,
            product=product,
            fetch_id=fetch_id,
            content_type=content_type,
            received_at=received_at,
            upstream_id=upstream_id,
            content_sha256=content_sha256,
            size_bytes=size_bytes,
            provenance_sha256=provenance_sha256,
        )
        if row["receipt_sha256"] != expected_receipt_sha:
            raise RawArtifactIntegrityError("raw-artifactreceiptchecksum klopt niet")
        expected_receipt_id = self._receipt_id(
            provider=provider,
            product=product,
            fetch_id=fetch_id,
        )
        if row["receipt_id"] != expected_receipt_id:
            raise RawArtifactIntegrityError("raw-artifactreceipt-ID klopt niet")
        relative_path = str(row["relative_path"])
        path = self._safe_object_path(relative_path, content_sha256)
        created_at = self._timestamp(row["created_at"], "created_at")
        return RawArtifactReceipt(
            receipt_id=expected_receipt_id,
            artifact_path=str(path),
            content_sha256=content_sha256,
            size_bytes=size_bytes,
            provider=provider,
            product=product,
            fetch_id=fetch_id,
            content_type=content_type,
            received_at=received_at,
            upstream_id=upstream_id,
            provenance_sha256=provenance_sha256,
            receipt_sha256=expected_receipt_sha,
            created_at=created_at,
            _provenance_json=provenance_json,
        )

    @staticmethod
    def _reclamation_receipt_view(
        receipt: RawArtifactReceipt,
    ) -> dict[str, Any]:
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

    def _reclamation_capture_bundle(
        self,
        database: sqlite3.Connection,
        capture_id: str,
    ) -> tuple[
        RawArtifactCaptureReceipt,
        dict[str, RawArtifactReceipt],
        dict[str, bytes],
    ]:
        row = database.execute(
            "SELECT * FROM raw_artifact_captures WHERE capture_id=?",
            (capture_id,),
        ).fetchone()
        if row is None:
            raise RawArtifactConflictError(
                "raw reclamation candidate capture ontbreekt"
            )
        member_rows = database.execute(
            """
            SELECT member.role AS capture_role,
                   receipt.receipt_id, receipt.provider, receipt.product,
                   receipt.fetch_id, receipt.upstream_id, receipt.content_type,
                   receipt.received_at, receipt.content_sha256,
                   receipt.size_bytes, receipt.provenance_json,
                   receipt.provenance_sha256, receipt.receipt_sha256,
                   receipt.created_at, object.relative_path,
                   object.size_bytes AS object_size_bytes
            FROM raw_artifact_capture_members AS member
            JOIN raw_artifact_receipts AS receipt
              ON receipt.receipt_id=member.receipt_id
            JOIN raw_artifact_objects AS object
              ON object.content_sha256=receipt.content_sha256
            WHERE member.capture_id=? ORDER BY member.role
            """,
            (capture_id,),
        ).fetchall()
        if not member_rows:
            raise RawArtifactIntegrityError(
                "raw reclamation candidate capture heeft geen leden"
            )
        provider = self._identifier(row["provider"], "provider", 128)
        product = self._identifier(row["product"], "product", 256)
        fetch_id = self._identifier(row["fetch_id"], "fetch_id", 512)
        upstream_capture_id = self._identifier(
            row["upstream_capture_id"], "upstream_capture_id", 1024
        )
        product_timestamp = (
            self._timestamp(row["product_timestamp"], "product_timestamp")
            if row["product_timestamp"] is not None
            else None
        )
        created_at = self._timestamp(row["created_at"], "created_at")
        receipts: dict[str, RawArtifactReceipt] = {}
        payloads: dict[str, bytes] = {}
        members: list[RawArtifactCaptureMember] = []
        for member_row in member_rows:
            role = self._identifier(
                member_row["capture_role"], "capture-role", 128
            )
            if role in receipts:
                raise RawArtifactIntegrityError(
                    "raw reclamation candidate capture heeft dubbele rol"
                )
            receipt = self._validated_receipt(member_row)
            if receipt.provider != provider:
                raise RawArtifactIntegrityError(
                    "raw reclamation candidate capture mengt providers"
                )
            payload = self._read_verified(
                Path(receipt.artifact_path),
                expected_size=receipt.size_bytes,
                expected_sha256=receipt.content_sha256,
                return_bytes=True,
            )
            if payload is None:  # pragma: no cover - return contract
                raise AssertionError("raw reclamation payload ontbreekt")
            receipts[role] = receipt
            payloads[role] = payload
            members.append(RawArtifactCaptureMember(
                role=role,
                receipt_id=receipt.receipt_id,
                content_sha256=receipt.content_sha256,
                receipt_sha256=receipt.receipt_sha256,
                received_at=receipt.received_at,
            ))
        member_tuple = tuple(members)
        manifest = {
            "fetch_id": fetch_id,
            "members": [
                {
                    "content_sha256": member.content_sha256,
                    "received_at": member.received_at,
                    "receipt_id": member.receipt_id,
                    "role": member.role,
                }
                for member in member_tuple
            ],
            "product": product,
            "product_timestamp": product_timestamp,
            "provider": provider,
            "schema": RAW_ARTIFACT_CAPTURE_SCHEMA,
            "upstream_capture_id": upstream_capture_id,
        }
        manifest_json = self._canonical_json(manifest)
        manifest_sha256 = hashlib.sha256(
            manifest_json.encode("utf-8")
        ).hexdigest()
        if (
            str(row["manifest_json"]) != manifest_json
            or str(row["manifest_sha256"]) != manifest_sha256
            or capture_id != self._capture_id(
                provider=provider,
                product=product,
                fetch_id=fetch_id,
            )
        ):
            raise RawArtifactIntegrityError(
                "raw reclamation candidate capturemanifest verschilt"
            )
        capture = RawArtifactCaptureReceipt(
            capture_id=capture_id,
            provider=provider,
            product=product,
            fetch_id=fetch_id,
            upstream_capture_id=upstream_capture_id,
            product_timestamp=product_timestamp,
            manifest_sha256=manifest_sha256,
            capture_receipt_sha256=self._capture_receipt_sha256(
                capture_id=capture_id,
                provider=provider,
                product=product,
                fetch_id=fetch_id,
                upstream_capture_id=upstream_capture_id,
                product_timestamp=product_timestamp,
                manifest_sha256=manifest_sha256,
                members=member_tuple,
            ),
            created_at=created_at,
            members=member_tuple,
        )
        return capture, receipts, payloads

    @staticmethod
    def _path_is_link_or_reparse(path: Path, info: os.stat_result) -> bool:
        reparse = int(getattr(info, "st_file_attributes", 0)) & int(
            getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
        )
        return path.is_symlink() or bool(reparse)

    @staticmethod
    def _external_file_identity(
        info: os.stat_result,
    ) -> tuple[int, int, int, int, int]:
        return (
            int(info.st_dev),
            int(info.st_ino),
            int(info.st_mode),
            int(info.st_nlink),
            int(info.st_size),
        )

    @staticmethod
    def _external_file_time_identity(
        info: os.stat_result,
    ) -> tuple[int, int]:
        return (
            int(getattr(info, "st_mtime_ns", int(info.st_mtime * 1_000_000_000))),
            int(getattr(info, "st_ctime_ns", int(info.st_ctime * 1_000_000_000))),
        )

    @classmethod
    def _external_ancestor_identities(
        cls,
        parent: Path,
    ) -> tuple[tuple[Path, int, int, int], ...]:
        identities: list[tuple[Path, int, int, int]] = []
        current = parent
        while True:
            try:
                info = current.lstat()
            except OSError as exc:
                raise RawArtifactIntegrityError(
                    "raw-artifactbronpad heeft een onbereikbare bovenliggende map"
                ) from exc
            if (
                cls._path_is_link_or_reparse(current, info)
                or not stat.S_ISDIR(info.st_mode)
                or int(info.st_ino) <= 0
            ):
                raise RawArtifactIntegrityError(
                    "raw-artifactbronpad bevat een symlink, reparse-point of "
                    "niet-reguliere bovenliggende map"
                )
            identities.append(
                (current, int(info.st_dev), int(info.st_ino), int(info.st_mode))
            )
            if current.parent == current:
                break
            current = current.parent
        return tuple(identities)

    @classmethod
    def _reprove_external_ancestors(
        cls,
        identities: tuple[tuple[Path, int, int, int], ...],
    ) -> None:
        for path, expected_dev, expected_ino, expected_mode in identities:
            try:
                info = path.lstat()
            except OSError as exc:
                raise RawArtifactIntegrityError(
                    "raw-artifactbronpad veranderde tijdens file-store"
                ) from exc
            if (
                cls._path_is_link_or_reparse(path, info)
                or not stat.S_ISDIR(info.st_mode)
                or (
                    int(info.st_dev),
                    int(info.st_ino),
                    int(info.st_mode),
                )
                != (expected_dev, expected_ino, expected_mode)
            ):
                raise RawArtifactIntegrityError(
                    "raw-artifactbronpad werd tijdens file-store vervangen"
                )

    @contextmanager
    def _hold_external_file_authority(
        self,
        source_path: str | Path,
        *,
        expected_size: int,
    ) -> Iterator[_ExternalFileAuthority]:
        if not isinstance(source_path, (str, Path)):
            raise TypeError("raw-artifactbronpad moet tekst of Path zijn")
        supplied = Path(source_path)
        if not supplied.is_absolute():
            raise ValueError("raw-artifactbronpad moet absoluut zijn")
        path = Path(os.path.abspath(os.fspath(supplied)))
        self._assert_safe_layout()
        try:
            resolved = path.resolve(strict=True)
        except OSError as exc:
            raise RawArtifactIntegrityError(
                "raw-artifactbronbestand is niet beschikbaar"
            ) from exc
        store_root = self.root.resolve()
        if resolved == store_root or store_root in resolved.parents:
            raise RawArtifactIntegrityError(
                "raw-artifactbronbestand mag niet in de raw-store staan"
            )
        ancestor_identities = self._external_ancestor_identities(path.parent)
        try:
            before = path.lstat()
        except OSError as exc:
            raise RawArtifactIntegrityError(
                "raw-artifactbronbestand is niet beschikbaar"
            ) from exc
        if (
            self._path_is_link_or_reparse(path, before)
            or not stat.S_ISREG(before.st_mode)
            or int(before.st_size) != expected_size
            or int(before.st_ino) <= 0
            or int(before.st_nlink) != 1
        ):
            raise RawArtifactIntegrityError(
                "raw-artifactbron mist exacte reguliere single-link identity"
            )
        try:
            descriptor = self._open_reclamation_file_descriptor(path)
        except OSError as exc:
            raise RawArtifactIntegrityError(
                "raw-artifactbron kon niet veilig no-follow worden geopend"
            ) from exc
        try:
            opened = os.fstat(descriptor)
            identity = self._external_file_identity(before)
            if (
                not stat.S_ISREG(opened.st_mode)
                or self._external_file_identity(opened) != identity
            ):
                raise RawArtifactIntegrityError(
                    "raw-artifactbron veranderde tijdens openen"
                )
            self._reprove_external_ancestors(ancestor_identities)
            yield _ExternalFileAuthority(
                path=path,
                descriptor=descriptor,
                file_identity=identity,
                path_time_identity=self._external_file_time_identity(before),
                descriptor_time_identity=(
                    self._external_file_time_identity(opened)
                ),
                ancestor_identities=ancestor_identities,
            )
        finally:
            os.close(descriptor)

    def _verify_external_file_authority(
        self,
        authority: _ExternalFileAuthority,
        *,
        expected_size: int,
        expected_sha256: str,
        output_descriptor: int | None = None,
    ) -> None:
        if not isinstance(authority, _ExternalFileAuthority):
            raise TypeError("raw-artifact file-authority heeft een ongeldig type")
        if _SHA256_RE.fullmatch(expected_sha256) is None:
            raise RawArtifactIntegrityError("raw-artifactbron SHA-256 is ongeldig")
        self._reprove_external_ancestors(authority.ancestor_identities)
        try:
            before_path = authority.path.lstat()
        except OSError as exc:
            raise RawArtifactIntegrityError(
                "raw-artifactbron verdween voor het streamen"
            ) from exc
        opened = os.fstat(authority.descriptor)
        if (
            self._path_is_link_or_reparse(authority.path, before_path)
            or not stat.S_ISREG(opened.st_mode)
            or self._external_file_identity(before_path)
            != authority.file_identity
            or self._external_file_identity(opened) != authority.file_identity
            or self._external_file_time_identity(before_path)
            != authority.path_time_identity
            or self._external_file_time_identity(opened)
            != authority.descriptor_time_identity
        ):
            raise RawArtifactIntegrityError(
                "raw-artifactbronidentity veranderde voor het streamen"
            )
        if output_descriptor is not None:
            output = os.fstat(output_descriptor)
            if not stat.S_ISREG(output.st_mode) or int(output.st_nlink) != 1:
                raise RawArtifactIntegrityError(
                    "raw-artifactstage mist reguliere single-link identity"
                )
            os.ftruncate(output_descriptor, 0)
            os.lseek(output_descriptor, 0, os.SEEK_SET)
        os.lseek(authority.descriptor, 0, os.SEEK_SET)
        digest = hashlib.sha256()
        total = 0
        while total <= expected_size:
            chunk = os.read(
                authority.descriptor,
                min(FILE_STORE_CHUNK_BYTES, expected_size + 1 - total),
            )
            if not chunk:
                break
            total += len(chunk)
            if total > expected_size:
                raise RawArtifactIntegrityError(
                    "raw-artifactbron bevat bytes voorbij de verwachte grootte"
                )
            digest.update(chunk)
            if output_descriptor is not None:
                written = 0
                while written < len(chunk):
                    count = os.write(output_descriptor, chunk[written:])
                    if count <= 0:  # pragma: no cover - defensive OS contract
                        raise RawArtifactIntegrityError(
                            "raw-artifactbron kon niet volledig worden gestreamd"
                        )
                    written += count
        after_descriptor = os.fstat(authority.descriptor)
        try:
            after_path = authority.path.lstat()
        except OSError as exc:
            raise RawArtifactIntegrityError(
                "raw-artifactbron verdween tijdens het streamen"
            ) from exc
        self._reprove_external_ancestors(authority.ancestor_identities)
        if (
            total != expected_size
            or not hmac.compare_digest(digest.hexdigest(), expected_sha256)
            or self._path_is_link_or_reparse(authority.path, after_path)
            or self._external_file_identity(after_descriptor)
            != authority.file_identity
            or self._external_file_identity(after_path) != authority.file_identity
            or self._external_file_time_identity(after_descriptor)
            != authority.descriptor_time_identity
            or self._external_file_time_identity(after_path)
            != authority.path_time_identity
        ):
            raise RawArtifactIntegrityError(
                "raw-artifactbron SHA-256, grootte of identity veranderde"
            )

    def _file_stage_path(self, expected_sha256: str) -> Path:
        if _SHA256_RE.fullmatch(expected_sha256) is None:
            raise RawArtifactIntegrityError("raw-artifactstage SHA-256 is ongeldig")
        path = self.staging_path / f".{expected_sha256}.{FILE_STORE_STAGE_SUFFIX}"
        if path.parent != self.staging_path:
            raise RawArtifactIntegrityError("raw-artifactstage ontsnapt uit staging")
        return path

    def _file_publication_directory_identities(
        self,
    ) -> tuple[tuple[Path, int, int, int], ...]:
        self._assert_safe_layout()
        identities: list[tuple[Path, int, int, int]] = []
        for path in (self.root, self.staging_path, self.objects_path):
            info = path.lstat()
            if (
                self._path_is_link_or_reparse(path, info)
                or not stat.S_ISDIR(info.st_mode)
                or int(info.st_ino) <= 0
            ):
                raise RawArtifactIntegrityError(
                    "raw-artifact file-publicatiemappen zijn onveilig"
                )
            identities.append(
                (path, int(info.st_dev), int(info.st_ino), int(info.st_mode))
            )
        return tuple(identities)

    @classmethod
    def _reprove_file_publication_directories(
        cls,
        identities: tuple[tuple[Path, int, int, int], ...],
    ) -> None:
        for path, expected_dev, expected_ino, expected_mode in identities:
            try:
                info = path.lstat()
            except OSError as exc:
                raise RawArtifactIntegrityError(
                    "raw-artifact file-publicatiemap verdween"
                ) from exc
            if (
                cls._path_is_link_or_reparse(path, info)
                or not stat.S_ISDIR(info.st_mode)
                or (
                    int(info.st_dev),
                    int(info.st_ino),
                    int(info.st_mode),
                )
                != (expected_dev, expected_ino, expected_mode)
            ):
                raise RawArtifactIntegrityError(
                    "raw-artifact file-publicatiemap werd vervangen"
                )

    def _resumable_file_stage_info(
        self,
        stage_path: Path,
    ) -> os.stat_result | None:
        try:
            info = stage_path.lstat()
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise RawArtifactIntegrityError(
                "raw-artifact file-stage is niet beschikbaar"
            ) from exc
        if (
            stage_path.parent != self.staging_path
            or _STAGING_NAME_RE.fullmatch(stage_path.name) is None
            or self._path_is_link_or_reparse(stage_path, info)
            or not stat.S_ISREG(info.st_mode)
            or int(info.st_ino) <= 0
            or int(info.st_nlink) != 1
            or int(info.st_size) > self.limits.max_object_bytes
        ):
            raise RawArtifactIntegrityError(
                "raw-artifact file-stage heeft geen hervatbare single-link identity"
            )
        return info

    def _file_stage_capacity_credit(
        self,
        stage_path: Path,
        target: Path,
    ) -> int:
        """Exclude only this API's exact removable stage from admission usage."""

        try:
            info = stage_path.lstat()
        except FileNotFoundError:
            return 0
        except OSError as exc:
            raise RawArtifactIntegrityError(
                "raw-artifact file-stage is niet beschikbaar voor capacity"
            ) from exc
        if (
            stage_path.parent != self.staging_path
            or _STAGING_NAME_RE.fullmatch(stage_path.name) is None
            or self._path_is_link_or_reparse(stage_path, info)
            or not stat.S_ISREG(info.st_mode)
            or int(info.st_ino) <= 0
            or int(info.st_size) > self.limits.max_object_bytes
        ):
            raise RawArtifactIntegrityError(
                "raw-artifact file-stage is onveilig voor capacity-credit"
            )
        if int(info.st_nlink) == 1:
            return int(info.st_size)
        if int(info.st_nlink) == 2:
            try:
                target_info = target.lstat()
            except OSError as exc:
                raise RawArtifactIntegrityError(
                    "raw-artifact linked file-stage mist zijn objecttarget"
                ) from exc
            if (
                self._path_is_link_or_reparse(target, target_info)
                or not stat.S_ISREG(target_info.st_mode)
                or (int(info.st_dev), int(info.st_ino))
                != (int(target_info.st_dev), int(target_info.st_ino))
            ):
                raise RawArtifactIntegrityError(
                    "raw-artifact linked file-stage wijkt af van objecttarget"
                )
            # The physical scanner already deduplicates these two hardlinks.
            return 0
        raise RawArtifactIntegrityError(
            "raw-artifact file-stage heeft onverwachte hardlinks"
        )

    @staticmethod
    def _stage_identity(info: os.stat_result) -> tuple[int, int, int]:
        return int(info.st_dev), int(info.st_ino), int(info.st_mode)

    def _unlink_owned_file_stage(
        self,
        stage_path: Path,
        *,
        expected_identity: tuple[int, int, int],
        allowed_link_counts: frozenset[int],
    ) -> None:
        try:
            info = stage_path.lstat()
        except FileNotFoundError:
            return
        except OSError as exc:
            raise RawArtifactIntegrityError(
                "raw-artifact file-stage kon niet worden herbewezen"
            ) from exc
        if (
            stage_path.parent != self.staging_path
            or self._path_is_link_or_reparse(stage_path, info)
            or not stat.S_ISREG(info.st_mode)
            or self._stage_identity(info) != expected_identity
            or int(info.st_nlink) not in allowed_link_counts
        ):
            raise RawArtifactIntegrityError(
                "raw-artifact file-stage werd voor cleanup vervangen"
            )
        stage_path.unlink()
        self._fsync_directory(self.staging_path)

    def _publish_exact_file(
        self,
        authority: _ExternalFileAuthority,
        stage_path: Path,
        target: Path,
        *,
        expected_size: int,
        expected_sha256: str,
        expected_stage_identity: tuple[int, int, int, int] | None,
    ) -> None:
        descriptor: int | None = None
        stage_identity: tuple[int, int, int] | None = None
        linked_new_object = False
        directory_identities = self._file_publication_directory_identities()
        try:
            existing = self._resumable_file_stage_info(stage_path)
            observed_stage_identity = (
                None
                if existing is None
                else (
                    int(existing.st_dev),
                    int(existing.st_ino),
                    int(existing.st_mode),
                    int(existing.st_size),
                )
            )
            if observed_stage_identity != expected_stage_identity:
                raise RawArtifactIntegrityError(
                    "raw-artifact file-stage wijzigde na capacity-admission"
                )
            flags = (
                os.O_WRONLY
                | getattr(os, "O_BINARY", 0)
                | getattr(os, "O_NOFOLLOW", 0)
            )
            if existing is None:
                descriptor = os.open(
                    stage_path,
                    flags | os.O_CREAT | os.O_EXCL,
                    0o600,
                )
                opened = os.fstat(descriptor)
                if not stat.S_ISREG(opened.st_mode) or int(opened.st_nlink) != 1:
                    raise RawArtifactIntegrityError(
                        "raw-artifact file-stage werd niet regulier aangemaakt"
                    )
            else:
                descriptor = os.open(stage_path, flags)
                opened = os.fstat(descriptor)
                if (
                    self._stage_identity(opened) != self._stage_identity(existing)
                    or int(opened.st_nlink) != 1
                    or int(opened.st_size) != int(existing.st_size)
                ):
                    raise RawArtifactIntegrityError(
                        "raw-artifact file-stage veranderde tijdens hervatten"
                    )
            stage_identity = self._stage_identity(opened)
            self._checkpoint("store_file_after_stage_open")
            self._verify_external_file_authority(
                authority,
                expected_size=expected_size,
                expected_sha256=expected_sha256,
                output_descriptor=descriptor,
            )
            os.fsync(descriptor)
            written = os.fstat(descriptor)
            if (
                self._stage_identity(written) != stage_identity
                or int(written.st_nlink) != 1
                or int(written.st_size) != expected_size
            ):
                raise RawArtifactIntegrityError(
                    "raw-artifact file-stage veranderde na het streamen"
                )
            self._checkpoint("store_file_after_stage_fsync")
            os.close(descriptor)
            descriptor = None
            self._fsync_directory(self.staging_path)
            self._read_verified(
                stage_path,
                expected_size=expected_size,
                expected_sha256=expected_sha256,
                return_bytes=False,
                allow_staging=True,
            )
            self._reprove_file_publication_directories(directory_identities)
            try:
                os.link(stage_path, target)
                linked_new_object = True
            except FileExistsError:
                pass
            self._checkpoint("store_file_after_object_link")
            self._read_verified(
                target,
                expected_size=expected_size,
                expected_sha256=expected_sha256,
                return_bytes=False,
            )
            self._fsync_directory(self.objects_path)
            self._reprove_file_publication_directories(directory_identities)
        finally:
            if descriptor is not None:
                os.close(descriptor)
            if stage_identity is not None:
                self._reprove_file_publication_directories(
                    directory_identities,
                )
                self._unlink_owned_file_stage(
                    stage_path,
                    expected_identity=stage_identity,
                    allowed_link_counts=(
                        frozenset({1, 2})
                        if linked_new_object
                        else frozenset({1})
                    ),
                )
            self._storage_usage_cache = None

    def _cleanup_completed_file_stage(
        self,
        stage_path: Path,
        target: Path,
        *,
        expected_size: int,
        expected_sha256: str,
    ) -> None:
        directory_identities = self._file_publication_directory_identities()
        try:
            stage_info = stage_path.lstat()
        except FileNotFoundError:
            return
        except OSError as exc:
            raise RawArtifactIntegrityError(
                "raw-artifact hervatbare stage kon niet worden herbewezen"
            ) from exc
        if (
            stage_path.parent != self.staging_path
            or self._path_is_link_or_reparse(stage_path, stage_info)
            or not stat.S_ISREG(stage_info.st_mode)
            or int(stage_info.st_ino) <= 0
        ):
            raise RawArtifactIntegrityError(
                "raw-artifact hervatbare stage is onveilig"
            )
        self._read_verified(
            target,
            expected_size=expected_size,
            expected_sha256=expected_sha256,
            return_bytes=False,
        )
        self._reprove_file_publication_directories(directory_identities)
        target_info = target.lstat()
        same_object = (
            int(stage_info.st_dev),
            int(stage_info.st_ino),
        ) == (int(target_info.st_dev), int(target_info.st_ino))
        # A single-link stage may be a partial crash remnant; the verified
        # committed target supersedes it.  A two-link stage is removable only
        # when it is the target's exact hardlink left after object publication.
        if int(stage_info.st_nlink) != 1 and not (
            same_object and int(stage_info.st_nlink) == 2
        ):
            raise RawArtifactIntegrityError(
                "raw-artifact hervatbare stage heeft onverwachte hardlinks"
            )
        self._unlink_owned_file_stage(
            stage_path,
            expected_identity=self._stage_identity(stage_info),
            allowed_link_counts=frozenset({int(stage_info.st_nlink)}),
        )
        self._reprove_file_publication_directories(directory_identities)
        self._storage_usage_cache = None

    def _publish_exact(
        self,
        payload: bytes,
        target: Path,
        *,
        expected_size: int,
        expected_sha256: str,
        prior_usage: _StorageUsage,
    ) -> None:
        temporary_path: Path | None = None
        linked_new_object = False
        try:
            with tempfile.NamedTemporaryFile(
                dir=self.staging_path,
                prefix=f".{expected_sha256}.",
                delete=False,
            ) as temporary:
                temporary.write(payload)
                temporary.flush()
                os.fsync(temporary.fileno())
                temporary_path = Path(temporary.name)
            self._fsync_directory(self.staging_path)
            self._read_verified(
                temporary_path,
                expected_size=expected_size,
                expected_sha256=expected_sha256,
                return_bytes=False,
                allow_staging=True,
            )
            try:
                os.link(temporary_path, target)
                linked_new_object = True
            except FileExistsError:
                pass
            self._read_verified(
                target,
                expected_size=expected_size,
                expected_sha256=expected_sha256,
                return_bytes=False,
            )
            self._fsync_directory(self.objects_path)
        finally:
            if temporary_path is not None:
                temporary_path.unlink(missing_ok=True)
                self._fsync_directory(self.staging_path)
            if linked_new_object:
                relative = target.relative_to(self.root).as_posix()
                entries = set(prior_usage.entries)
                added = relative not in entries
                entries.add(relative)
                self._storage_usage_cache = _StorageUsage(
                    total_bytes=(
                        prior_usage.total_bytes + (expected_size if added else 0)
                    ),
                    entries=frozenset(entries),
                    object_entries=prior_usage.object_entries + (1 if added else 0),
                    staging_entries=prior_usage.staging_entries,
                    fingerprints=self._storage_fingerprints(),
                )
            else:
                # An external writer won the content-addressed target race.
                # Its full physical state must be rescanned before reuse.
                self._storage_usage_cache = None

    def _read_verified(
        self,
        path: Path,
        *,
        expected_size: int,
        expected_sha256: str,
        return_bytes: bool,
        allow_staging: bool = False,
    ) -> bytes | None:
        if not _SHA256_RE.fullmatch(expected_sha256):
            raise RawArtifactIntegrityError("raw-artifact SHA-256 is ongeldig")
        if expected_size < 0 or expected_size > self.limits.max_object_bytes:
            raise RawArtifactIntegrityError("raw-artifactgrootte is ongeldig")
        allowed_parent = self.staging_path if allow_staging else self.objects_path
        if path.parent.resolve() != allowed_parent.resolve():
            raise RawArtifactIntegrityError("raw-artifactpad ontsnapt uit opslagmap")
        info = path.lstat()
        if not stat.S_ISREG(info.st_mode) or path.is_symlink():
            raise RawArtifactIntegrityError("raw-artifactpad is geen regulier bestand")
        if info.st_size != expected_size:
            raise RawArtifactIntegrityError("raw-artifactgrootte veranderde")
        flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(path, flags)
        except OSError as exc:
            raise RawArtifactIntegrityError("raw-artifact kon niet veilig worden geopend") from exc
        chunks: list[bytes] | None = [] if return_bytes else None
        digest = hashlib.sha256()
        total = 0
        try:
            opened = os.fstat(descriptor)
            if not stat.S_ISREG(opened.st_mode) or opened.st_size != expected_size:
                raise RawArtifactIntegrityError("raw-artifact veranderde tijdens openen")
            while True:
                chunk = os.read(descriptor, min(1024 * 1024, expected_size + 1 - total))
                if not chunk:
                    break
                total += len(chunk)
                if total > expected_size:
                    raise RawArtifactIntegrityError("raw-artifact bevat extra bytes")
                digest.update(chunk)
                if chunks is not None:
                    chunks.append(chunk)
        finally:
            os.close(descriptor)
        if total != expected_size or digest.hexdigest() != expected_sha256:
            raise RawArtifactIntegrityError("raw-artifact SHA-256 of grootte klopt niet")
        after = path.lstat()
        if (
            after.st_size != opened.st_size
            or (after.st_dev, after.st_ino) != (opened.st_dev, opened.st_ino)
        ):
            raise RawArtifactIntegrityError("raw-artifactpad werd tijdens lezen vervangen")
        return b"".join(chunks) if chunks is not None else None

    def _scan_storage(self, *, force: bool = False) -> _StorageUsage:
        """Return bounded physical usage; rescan only across a changed boundary."""

        with self._lock:
            self._assert_safe_layout()
            fingerprints = self._storage_fingerprints()
            cached = self._storage_usage_cache
            if not force and cached is not None and cached.fingerprints == fingerprints:
                return cached
            usage = self._scan_storage_uncached(fingerprints)
            self._storage_usage_cache = usage
            return usage

    def _scan_storage_uncached(
        self,
        fingerprints: tuple[tuple[int, int, int, int], tuple[int, int, int, int]],
    ) -> _StorageUsage:
        seen: set[tuple[int, int] | tuple[str, str]] = set()
        entries: set[str] = set()
        total = 0
        object_entries = 0
        staging_entries = 0
        layouts = (
            (
                self.objects_path,
                _OBJECT_NAME_RE,
                self.limits.max_object_entries,
                "objects",
            ),
            (
                self.staging_path,
                _STAGING_NAME_RE,
                self.limits.max_staging_entries,
                "staging",
            ),
        )
        for directory, filename_pattern, entry_limit, scope in layouts:
            directory_entries = 0
            for path in directory.iterdir():
                directory_entries += 1
                if directory_entries > entry_limit:
                    raise RawArtifactEntryCapacityError(
                        scope=scope,
                        current_entries=directory_entries,
                        attempted_entries=0,
                        limit_entries=entry_limit,
                    )
                if filename_pattern.fullmatch(path.name) is None:
                    raise RawArtifactIntegrityError(
                        f"raw-artifactopslag bevat onbekende {scope}-entry"
                    )
                info = path.lstat()
                if not stat.S_ISREG(info.st_mode) or path.is_symlink():
                    raise RawArtifactIntegrityError(
                        "raw-artifactopslag bevat symlink of niet-regulier bestand"
                    )
                relative = path.relative_to(self.root).as_posix()
                entries.add(relative)
                inode: tuple[int, int] | tuple[str, str]
                if info.st_ino:
                    inode = (int(info.st_dev), int(info.st_ino))
                else:  # pragma: no cover - filesystem without stable inode
                    inode = ("path", relative)
                if inode not in seen:
                    seen.add(inode)
                    total += int(info.st_size)
                    if info.st_size > self.limits.max_object_bytes:
                        raise RawArtifactCapacityError(
                            scope="object",
                            current_bytes=0,
                            attempted_bytes=int(info.st_size),
                            limit_bytes=self.limits.max_object_bytes,
                        )
                    if total > self.limits.max_total_bytes:
                        raise RawArtifactCapacityError(
                            scope="total",
                            current_bytes=total,
                            attempted_bytes=0,
                            limit_bytes=self.limits.max_total_bytes,
                        )
                if directory == self.objects_path:
                    object_entries += 1
                else:
                    staging_entries += 1
        after = self._storage_fingerprints()
        if after != fingerprints:
            raise RawArtifactIntegrityError(
                "raw-artifactopslag wijzigde tijdens de fysieke usage-scan"
            )
        return _StorageUsage(
            total_bytes=total,
            entries=frozenset(entries),
            object_entries=object_entries,
            staging_entries=staging_entries,
            fingerprints=after,
        )

    def _storage_fingerprints(
        self,
    ) -> tuple[tuple[int, int, int, int], tuple[int, int, int, int]]:
        return (
            self._directory_fingerprint(self.objects_path),
            self._directory_fingerprint(self.staging_path),
        )

    @staticmethod
    def _directory_fingerprint(path: Path) -> tuple[int, int, int, int]:
        info = path.lstat()
        if path.is_symlink() or not stat.S_ISDIR(info.st_mode):
            raise RawArtifactIntegrityError("raw-artifactsubmap is onveilig")
        return (
            int(info.st_dev),
            int(info.st_ino),
            int(info.st_mtime_ns),
            int(info.st_ctime_ns),
        )

    def _safe_object_path(self, relative_path: str, sha256: str) -> Path:
        expected = PurePosixPath(OBJECT_DIRECTORY, f"{sha256}.raw")
        supplied = PurePosixPath(relative_path)
        if (
            not _SHA256_RE.fullmatch(sha256)
            or supplied != expected
            or supplied.is_absolute()
            or ".." in supplied.parts
        ):
            raise RawArtifactIntegrityError("raw-artifactpad is ongeldig")
        path = self.root.joinpath(*supplied.parts)
        if path.parent.resolve() != self.objects_path.resolve():
            raise RawArtifactIntegrityError("raw-artifactpad ontsnapt uit root")
        if path.is_symlink():
            raise RawArtifactIntegrityError("raw-artifactpad mag geen symlink zijn")
        return path

    @staticmethod
    def _relative_object_path(sha256: str) -> str:
        if not _SHA256_RE.fullmatch(sha256):  # pragma: no cover - internal hash
            raise ValueError("ongeldige SHA-256")
        return PurePosixPath(OBJECT_DIRECTORY, f"{sha256}.raw").as_posix()

    @staticmethod
    def _canonical_json(value: Any) -> str:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )

    @staticmethod
    def _protocol_json(value: object) -> str:
        return reclamation_canonical_bytes(value).decode("ascii")

    @classmethod
    def _insert_reclamation_mapping_rows(
        cls,
        database: sqlite3.Connection,
        *,
        table: str,
        rows: tuple[dict[str, Any], ...],
    ) -> None:
        """Insert one exact internal row shape without accepting SQL names."""

        expected = cls._RECLAMATION_COLUMNS.get(table)
        if expected is None:
            raise AssertionError("onbekende raw reclamation-tabel")
        if not rows:
            return
        columns = tuple(rows[0])
        if frozenset(columns) != expected or any(
            frozenset(row) != expected for row in rows
        ):
            raise RawArtifactIntegrityError(
                f"raw reclamation plan voor {table} heeft onverwachte velden"
            )
        placeholders = ",".join("?" for _ in columns)
        database.executemany(
            f"INSERT INTO {table}({','.join(columns)}) VALUES({placeholders})",
            [tuple(row[column] for column in columns) for row in rows],
        )

    @staticmethod
    def _ordered_reclamation_root(
        *,
        schema: str,
        item_sha256s: tuple[str, ...],
    ) -> str:
        if not item_sha256s:
            return reclamation_digest_mapping({
                "count": 0,
                "schema": schema,
            })
        head = reclamation_digest_mapping({
            "count": len(item_sha256s),
            "schema": schema,
            "start": True,
        })
        for ordinal, item_sha256 in enumerate(item_sha256s):
            require_reclamation_sha256(item_sha256, "raw ordered root item")
            head = reclamation_digest_mapping({
                "item_sha256": item_sha256,
                "ordinal": ordinal,
                "previous_sha256": head,
                "schema": "stv3-worker-raw-reclamation-root-link-v1",
            })
        return reclamation_digest_mapping({
            "count": len(item_sha256s),
            "head_sha256": head,
            "schema": schema,
        })

    @classmethod
    def _protocol_receipt_from_json(
        cls,
        payload: object,
        receipt_type: type[
            SourceReclamationRequestV2
            | SourceLocalPrepareReceiptV2
            | CommittedReclamationDecisionReceiptV1
            | CoordinatorAbortDecisionReceiptV1
            | SourceLocalApplyReceiptV1
            | SourceLocalAbortReceiptV1
            | RawUnlinkJournalReceiptV1
        ],
    ) -> (
        SourceReclamationRequestV2
        | SourceLocalPrepareReceiptV2
        | CommittedReclamationDecisionReceiptV1
        | CoordinatorAbortDecisionReceiptV1
        | SourceLocalApplyReceiptV1
        | SourceLocalAbortReceiptV1
        | RawUnlinkJournalReceiptV1
    ):
        if not isinstance(payload, str):
            raise RawArtifactIntegrityError(
                "raw reclamation protocolreceipt is geen tekst"
            )
        try:
            decoded = json.loads(payload)
        except (TypeError, json.JSONDecodeError) as exc:
            raise RawArtifactIntegrityError(
                "raw reclamation protocolreceipt is corrupt"
            ) from exc
        if not isinstance(decoded, dict):
            raise RawArtifactIntegrityError(
                "raw reclamation protocolreceipt is geen mapping"
            )
        values = dict(decoded)
        values.pop("protocol_contract", None)
        values.pop("schema", None)
        try:
            if receipt_type is CommittedReclamationDecisionReceiptV1:
                # Apply order is a closed envelope fact re-derived by
                # ``as_dict`` rather than a constructor field.  Canonical byte
                # equality below still rejects it when missing or changed.
                values.pop("apply_order", None)
                raw_bindings = values.get("participant_prepares")
                if not isinstance(raw_bindings, list):
                    raise TypeError("participant prepares zijn geen lijst")
                bindings: list[ParticipantPrepareBindingV1] = []
                for item in raw_bindings:
                    if not isinstance(item, dict):
                        raise TypeError("participant prepare is geen mapping")
                    binding = dict(item)
                    binding.pop("schema", None)
                    bindings.append(ParticipantPrepareBindingV1(**binding))
                values["participant_prepares"] = tuple(bindings)
            field_names = {
                name
                for name, field_definition in receipt_type.__dataclass_fields__.items()
                if field_definition.init
            }
            if set(values) != field_names:
                raise TypeError("protocolreceiptvelden verschillen")
            receipt = receipt_type(**values)
        except (TypeError, ValueError, RuntimeError) as exc:
            raise RawArtifactIntegrityError(
                "raw reclamation protocolreceiptvelden verschillen"
            ) from exc
        if cls._protocol_json(receipt.as_dict()) != payload:
            raise RawArtifactIntegrityError(
                "raw reclamation protocolreceipt is niet canonical"
            )
        return receipt

    def _require_candidate_authority(
        self,
    ) -> SourceReclamationCandidateAuthority:
        candidate, _decision = self._require_reclamation_authority_pair()
        return candidate

    def _require_committed_decision_authority(
        self,
    ) -> HeldCommittedDecisionAuthority:
        _candidate, decision = self._require_reclamation_authority_pair()
        return decision

    def _assert_reclamation_prepare_authority_pair(
        self,
        prepared: SourceLocalPrepareReceiptV2,
        *,
        authority_pair: _ReclamationAuthorityPairSnapshot,
    ) -> None:
        if (
            prepared.candidate_authority_sha256
            != authority_pair.candidate_sha256
            or prepared.coordinator_authority_sha256
            != authority_pair.decision_sha256
        ):
            raise RawArtifactConflictError(
                "raw reclamation prepare bindt andere instance-authority"
            )

    def _reclamation_authority_pair_snapshot(
        self,
    ) -> _ReclamationAuthorityPairSnapshot:
        with self._lock:
            candidate, decision = self._require_reclamation_authority_pair()
            candidate_sha256 = self._candidate_authority_sha256
            decision_sha256 = self._committed_decision_authority_sha256
            if candidate_sha256 is None or decision_sha256 is None:
                raise RawArtifactIntegrityError(
                    "raw reclamation authoritybinding mist zijn identitysnapshot"
                )
            return _ReclamationAuthorityPairSnapshot(
                candidate=candidate,
                decision=decision,
                candidate_sha256=candidate_sha256,
                decision_sha256=decision_sha256,
            )

    def _require_reclamation_authority_pair(
        self,
    ) -> tuple[
        SourceReclamationCandidateAuthority,
        HeldCommittedDecisionAuthority,
    ]:
        with self._lock:
            candidate = self._candidate_authority
            decision = self._committed_decision_authority
            candidate_sha256 = self._candidate_authority_sha256
            decision_sha256 = self._committed_decision_authority_sha256
            if (
                candidate is None
                and decision is None
                and candidate_sha256 is None
                and decision_sha256 is None
            ):
                raise RuntimeError(
                    "raw reclamation vereist gebonden candidate- en decision-authorities"
                )
            if (
                candidate is None
                or decision is None
                or candidate_sha256 is None
                or decision_sha256 is None
                or not isinstance(
                    candidate,
                    SourceReclamationCandidateAuthority,
                )
                or not isinstance(decision, HeldCommittedDecisionAuthority)
            ):
                raise RawArtifactIntegrityError(
                    "raw reclamation authoritybinding is onvolledig of ongeldig"
                )
            try:
                current_candidate_sha256 = require_reclamation_sha256(
                    candidate.authority_sha256,
                    "raw reclamation candidate-authority SHA-256",
                )
                current_decision_sha256 = require_reclamation_sha256(
                    decision.authority_sha256,
                    "raw reclamation decision-authority SHA-256",
                )
            except (TypeError, ValueError, RuntimeError) as exc:
                raise RawArtifactIntegrityError(
                    "raw reclamation authority-identiteit is ongeldig"
                ) from exc
            if (
                not hmac.compare_digest(
                    current_candidate_sha256,
                    candidate_sha256,
                )
                or not hmac.compare_digest(
                    current_decision_sha256,
                    decision_sha256,
                )
            ):
                raise RawArtifactIntegrityError(
                    "raw reclamation authority-identiteit is gewijzigd"
                )
            return candidate, decision

    def _reclamation_now(self) -> float:
        return self._timestamp(self._reclamation_clock(), "raw reclamation clock")

    def _strict_reclamation_directory_fsync(self, path: Path) -> dict[str, int]:
        """Hold and prove one directory durability barrier or fail closed."""

        try:
            before = path.lstat()
        except OSError as exc:
            raise RawArtifactIntegrityError(
                "raw reclamation directory is niet beschikbaar"
            ) from exc
        if path.is_symlink() or not stat.S_ISDIR(before.st_mode):
            raise RawArtifactIntegrityError(
                "raw reclamation directory is niet veilig"
            )
        authority = self._reclamation_directory_fsync_authority
        if authority is None:
            if os.name == "nt":
                raise RawArtifactIntegrityError(
                    "raw reclamation mist geverifieerde Windows directory-syncauthority"
                )
            flags = (
                os.O_RDONLY
                | getattr(os, "O_DIRECTORY", 0)
                | getattr(os, "O_NOFOLLOW", 0)
            )
            try:
                descriptor = os.open(path, flags)
            except OSError as exc:
                raise RawArtifactIntegrityError(
                    "raw reclamation directory kon niet veilig worden geopend"
                ) from exc
            try:
                opened = os.fstat(descriptor)
                if (
                    not stat.S_ISDIR(opened.st_mode)
                    or (int(opened.st_dev), int(opened.st_ino))
                    != (int(before.st_dev), int(before.st_ino))
                ):
                    raise RawArtifactIntegrityError(
                        "raw reclamation directory veranderde tijdens openen"
                    )
                try:
                    os.fsync(descriptor)
                except OSError as exc:
                    raise RawArtifactIntegrityError(
                        "raw reclamation directory-syncbarrier faalde"
                    ) from exc
            finally:
                os.close(descriptor)
        else:
            try:
                authority(path)
            except BaseException as exc:
                raise RawArtifactIntegrityError(
                    "raw reclamation directory-syncauthority faalde"
                ) from exc
        try:
            after = path.lstat()
        except OSError as exc:
            raise RawArtifactIntegrityError(
                "raw reclamation directory verdween na sync"
            ) from exc
        if (
            path.is_symlink()
            or not stat.S_ISDIR(after.st_mode)
            or (int(after.st_dev), int(after.st_ino))
            != (int(before.st_dev), int(before.st_ino))
        ):
            raise RawArtifactIntegrityError(
                "raw reclamation directory werd tijdens sync vervangen"
            )
        return {
            "st_dev": int(after.st_dev),
            "st_ino": int(after.st_ino),
            "st_mode": int(after.st_mode),
        }

    @staticmethod
    def _open_reclamation_file_descriptor(path: Path) -> int:
        """Open read-only/no-follow while permitting held-handle unlink on NT."""

        flags = (
            os.O_RDONLY
            | getattr(os, "O_BINARY", 0)
            | getattr(os, "O_NOFOLLOW", 0)
        )
        if os.name != "nt":
            return os.open(path, flags)

        # ``os.open`` on Windows denies deletion while the descriptor is held.
        # CreateFileW can preserve the same read authority with explicit delete
        # sharing, allowing the descriptor to remain live through unlink and
        # the post-unlink link-count proof.
        import ctypes
        import msvcrt

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        create_file = kernel32.CreateFileW
        create_file.argtypes = (
            ctypes.c_wchar_p,
            ctypes.c_uint32,
            ctypes.c_uint32,
            ctypes.c_void_p,
            ctypes.c_uint32,
            ctypes.c_uint32,
            ctypes.c_void_p,
        )
        create_file.restype = ctypes.c_void_p
        handle = create_file(
            str(path),
            0x80000000,  # GENERIC_READ
            0x00000001 | 0x00000002 | 0x00000004,  # SHARE READ/WRITE/DELETE
            None,
            3,  # OPEN_EXISTING
            0x00200000 | 0x00000080,  # OPEN_REPARSE_POINT | NORMAL
            None,
        )
        invalid_handle = ctypes.c_void_p(-1).value
        if handle in (None, invalid_handle):
            error = ctypes.get_last_error()
            raise OSError(error, os.strerror(error), str(path))
        try:
            return msvcrt.open_osfhandle(
                int(handle),
                os.O_RDONLY | getattr(os, "O_BINARY", 0),
            )
        except BaseException:
            kernel32.CloseHandle(ctypes.c_void_p(handle))
            raise

    @contextmanager
    def _hold_reclamation_file_authority(
        self,
        path: Path,
        *,
        expected_size: int,
        expected_sha256: str,
        expected_identity: _ReclamationFileIdentity | None = None,
    ) -> Iterator[tuple[int, _ReclamationFileIdentity, int | None]]:
        """Hold exact file bytes and optional POSIX parent-dir authority."""

        relative_path = self._relative_object_path(expected_sha256)
        if path != self._safe_object_path(relative_path, expected_sha256):
            raise RawArtifactIntegrityError("raw reclamation objectpad verschilt")
        if expected_size < 0 or expected_size > self.limits.max_object_bytes:
            raise RawArtifactIntegrityError("raw reclamation objectgrootte is ongeldig")
        try:
            before = path.lstat()
        except OSError as exc:
            raise RawArtifactIntegrityError(
                "raw reclamation object is niet beschikbaar"
            ) from exc
        if (
            path.is_symlink()
            or not stat.S_ISREG(before.st_mode)
            or int(before.st_size) != expected_size
            or int(before.st_ino) <= 0
            or int(before.st_nlink) != 1
        ):
            raise RawArtifactIntegrityError(
                "raw reclamation object mist exacte reguliere single-link identity"
            )
        try:
            descriptor = self._open_reclamation_file_descriptor(path)
        except OSError as exc:
            raise RawArtifactIntegrityError(
                "raw reclamation object kon niet veilig worden geopend"
            ) from exc
        directory_descriptor: int | None = None
        try:
            if os.name != "nt":
                directory_before = self.objects_path.lstat()
                directory_descriptor = os.open(
                    self.objects_path,
                    os.O_RDONLY
                    | getattr(os, "O_DIRECTORY", 0)
                    | getattr(os, "O_NOFOLLOW", 0),
                )
                directory_opened = os.fstat(directory_descriptor)
                if (
                    self.objects_path.is_symlink()
                    or not stat.S_ISDIR(directory_opened.st_mode)
                    or (
                        int(directory_before.st_dev),
                        int(directory_before.st_ino),
                    ) != (
                        int(directory_opened.st_dev),
                        int(directory_opened.st_ino),
                    )
                ):
                    raise RawArtifactIntegrityError(
                        "raw reclamation objectdirectory veranderde tijdens openen"
                    )
            opened = os.fstat(descriptor)
            opened_facts = (
                int(opened.st_dev),
                int(opened.st_ino),
                int(opened.st_mode),
                int(opened.st_nlink),
                int(opened.st_size),
            )
            before_facts = (
                int(before.st_dev),
                int(before.st_ino),
                int(before.st_mode),
                int(before.st_nlink),
                int(before.st_size),
            )
            if opened_facts != before_facts or not stat.S_ISREG(opened.st_mode):
                raise RawArtifactIntegrityError(
                    "raw reclamation object veranderde tijdens openen"
                )
            digest = hashlib.sha256()
            total = 0
            while True:
                chunk = os.read(
                    descriptor,
                    min(1024 * 1024, expected_size + 1 - total),
                )
                if not chunk:
                    break
                total += len(chunk)
                if total > expected_size:
                    raise RawArtifactIntegrityError(
                        "raw reclamation object bevat extra bytes"
                    )
                digest.update(chunk)
            if total != expected_size or digest.hexdigest() != expected_sha256:
                raise RawArtifactIntegrityError(
                    "raw reclamation object SHA-256 of grootte verschilt"
                )
            try:
                after = path.lstat()
            except OSError as exc:
                raise RawArtifactIntegrityError(
                    "raw reclamation object verdween tijdens reproof"
                ) from exc
            after_facts = (
                int(after.st_dev),
                int(after.st_ino),
                int(after.st_mode),
                int(after.st_nlink),
                int(after.st_size),
            )
            if after_facts != opened_facts or path.is_symlink():
                raise RawArtifactIntegrityError(
                    "raw reclamation object werd tijdens reproof vervangen"
                )
            identity = _ReclamationFileIdentity(
                content_sha256=expected_sha256,
                relative_path=relative_path,
                size_bytes=expected_size,
                st_dev=int(after.st_dev),
                st_ino=int(after.st_ino),
                st_mode=int(after.st_mode),
                st_nlink=int(after.st_nlink),
            )
            if expected_identity is not None and identity != expected_identity:
                raise RawArtifactIntegrityError(
                    "raw reclamation objectidentity verschilt van immutable intent"
                )
            yield descriptor, identity, directory_descriptor
        finally:
            if directory_descriptor is not None:
                os.close(directory_descriptor)
            os.close(descriptor)

    def _reclamation_file_identity(
        self,
        path: Path,
        *,
        expected_size: int,
        expected_sha256: str,
        expected_identity: _ReclamationFileIdentity | None = None,
    ) -> _ReclamationFileIdentity:
        """Reprove exact bytes, path and physical identity for one unlink."""

        with self._hold_reclamation_file_authority(
            path,
            expected_size=expected_size,
            expected_sha256=expected_sha256,
            expected_identity=expected_identity,
        ) as (_descriptor, identity, _directory_descriptor):
            return identity

    @classmethod
    def _provenance(cls, value: Mapping[str, Any]) -> tuple[str, str]:
        if not isinstance(value, Mapping):
            raise RawArtifactProvenanceError("raw-artifactprovenance moet een object zijn")
        try:
            encoded = cls._canonical_json(dict(value))
            normalized = json.loads(encoded)
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise RawArtifactProvenanceError(
                "raw-artifactprovenance moet eindige JSON zijn"
            ) from exc
        if not isinstance(normalized, dict) or not _REQUIRED_PROVENANCE <= normalized.keys():
            raise RawArtifactProvenanceError(
                "raw-artifactprovenance mist verplichte velden"
            )
        if normalized["representation"] != NATIVE_REPRESENTATION:
            raise RawArtifactProvenanceError(
                "decoded data wordt niet automatisch als upstream-native aanvaard"
            )
        if normalized["capture_stage"] != NATIVE_CAPTURE_STAGE:
            raise RawArtifactProvenanceError(
                "upstream-native bytes moeten expliciet vóór decode zijn vastgelegd"
            )
        for key in ("collector", "collector_version"):
            cls._identifier(normalized[key], f"provenance.{key}", 256)
        encoded = cls._canonical_json(normalized)
        if len(encoded.encode("utf-8")) > MAX_PROVENANCE_BYTES:
            raise RawArtifactProvenanceError("raw-artifactprovenance is te groot")
        return encoded, hashlib.sha256(encoded.encode("utf-8")).hexdigest()

    @classmethod
    def _receipt_id(cls, *, provider: str, product: str, fetch_id: str) -> str:
        identity = {
            "fetch_id": fetch_id,
            "product": product,
            "provider": provider,
            "schema": "stv3-worker-raw-native-fetch-identity-v1",
        }
        digest = hashlib.sha256(
            cls._canonical_json(identity).encode("utf-8")
        ).hexdigest()
        return f"raw-{digest}"

    @classmethod
    def _capture_id(cls, *, provider: str, product: str, fetch_id: str) -> str:
        identity = {
            "fetch_id": fetch_id,
            "product": product,
            "provider": provider,
            "schema": "stv3-worker-raw-native-capture-identity-v1",
        }
        digest = hashlib.sha256(
            cls._canonical_json(identity).encode("utf-8")
        ).hexdigest()
        return f"capture-{digest}"

    @classmethod
    def _receipt_sha256(
        cls,
        *,
        provider: str,
        product: str,
        fetch_id: str,
        content_type: str,
        received_at: float,
        upstream_id: str,
        content_sha256: str,
        size_bytes: int,
        provenance_sha256: str,
    ) -> str:
        document = {
            "content_sha256": content_sha256,
            "content_type": content_type,
            "fetch_id": fetch_id,
            "product": product,
            "provenance_sha256": provenance_sha256,
            "provider": provider,
            "received_at": received_at,
            "schema": RAW_ARTIFACT_RECEIPT_SCHEMA,
            "size_bytes": size_bytes,
            "upstream_id": upstream_id,
        }
        return hashlib.sha256(cls._canonical_json(document).encode("utf-8")).hexdigest()

    @classmethod
    def _capture_receipt_sha256(
        cls,
        *,
        capture_id: str,
        provider: str,
        product: str,
        fetch_id: str,
        upstream_capture_id: str,
        product_timestamp: float | None,
        manifest_sha256: str,
        members: tuple[RawArtifactCaptureMember, ...],
    ) -> str:
        """Bind the complete payload-free capture view to one stable digest."""
        document = {
            "capture_id": capture_id,
            "capture_schema": RAW_ARTIFACT_CAPTURE_SCHEMA,
            "complete": True,
            "fetch_id": fetch_id,
            "manifest_sha256": manifest_sha256,
            "members": [
                {
                    "content_sha256": member.content_sha256,
                    "member_id": member.member_id,
                    "receipt_id": member.receipt_id,
                    "receipt_sha256": member.receipt_sha256,
                    "received_at": member.received_at,
                    "role": member.role,
                }
                for member in members
            ],
            "product": product,
            "product_timestamp": product_timestamp,
            "provider": provider,
            "raw_store_schema_version": RAW_ARTIFACT_SCHEMA_VERSION,
            "schema": RAW_ARTIFACT_CAPTURE_RECEIPT_SCHEMA,
            "upstream_capture_id": upstream_capture_id,
        }
        encoded = cls._canonical_json(document).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    @staticmethod
    def _identifier(value: object, name: str, maximum: int) -> str:
        if (
            not isinstance(value, str)
            or not value
            or value != value.strip()
            or len(value) > maximum
            or any(ord(character) < 32 for character in value)
        ):
            raise ValueError(f"{name} is ongeldig")
        return value

    @staticmethod
    def _timestamp(value: object, name: str) -> float:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError(f"{name} is ongeldig")
        result = float(value)
        if not math.isfinite(result) or result < 0.0:
            raise ValueError(f"{name} is ongeldig")
        return result

    @staticmethod
    def _fsync_directory(path: Path) -> None:
        flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        try:
            descriptor = os.open(path, flags)
        except OSError as exc:
            if os.name == "nt":
                return
            raise RawArtifactIntegrityError(
                "raw-artifactmap kon niet crash-durable worden gesynchroniseerd"
            ) from exc
        try:
            try:
                os.fsync(descriptor)
            except OSError as exc:
                if os.name != "nt":
                    raise RawArtifactIntegrityError(
                        "raw-artifactmap kon niet crash-durable worden gesynchroniseerd"
                    ) from exc
        finally:
            os.close(descriptor)

    def _checkpoint(self, name: str) -> None:
        if self._fault_injector is not None:
            self._fault_injector(name)

    def _require_initialized(self) -> None:
        if not self._initialized:
            raise RuntimeError("raw-artifactstore vereist initialize()")
