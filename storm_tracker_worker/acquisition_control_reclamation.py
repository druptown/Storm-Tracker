"""Native v2 reclamation participants for bounded acquisition control stores.

This module is deliberately inert: importing it does not migrate a database,
delete a row, start a scheduler, or wire a worker service.  A participant is
created explicitly around one already-initialised execution, scheduler, or
capacity store.  Its additive extension lives in the same SQLite database as
the source rows so a tombstone, source-row removal, and local APPLIED receipt
share one transaction.

The irreversible boundary is constructor-bound twice: candidate pages come
from a physically held archive/registry authority and apply/abort decisions
come from a physically held coordinator authority.  Caller-provided hashes are
therefore comparison values only, never permission to mutate source state.
"""

from __future__ import annotations

from contextlib import ExitStack, closing, contextmanager
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
from typing import Any, Callable, Iterator, Literal, Mapping, Sequence

from .acquisition_source_reclamation_protocol import (
    PARTICIPANT_APPLY_ORDER,
    AcquisitionSourceReclamationProtocolError,
    CommittedReclamationDecisionReceiptV1,
    CoordinatorAbortDecisionReceiptV1,
    HeldCommittedDecisionAuthority,
    SourceLocalAbortReceiptV1,
    SourceLocalApplyReceiptV1,
    SourceLocalPrepareReceiptV2,
    SourceReclamationCandidateAuthority,
    SourceReclamationCandidateBindingV1,
    SourceReclamationRequestV2,
    archive_binding_root,
    candidate_binding_root,
    candidate_participant_view_sha256,
    canonical_bytes,
    digest_mapping,
    require_sha256,
)
from .acquisition_source_reclamation_views import (
    participant_candidate_view_sha256,
    participant_inventory_view_sha256,
)


CONTROL_RECLAMATION_CONTRACT = (
    "stv3-worker-acquisition-control-store-reclamation-v1"
)
CONTROL_RECLAMATION_SCHEMA_VERSION = 1
CONTROL_RECLAMATION_IDENTITY_SCHEMA = (
    "stv3-worker-acquisition-control-store-identity-v1"
)
CONTROL_RECLAMATION_MEMBER_SCHEMA = (
    "stv3-worker-acquisition-control-store-reclamation-member-v1"
)
CONTROL_RECLAMATION_EFFECT_SCHEMA = (
    "stv3-worker-acquisition-control-store-reclamation-effect-v1"
)
CONTROL_RECLAMATION_POST_VIEW_SCHEMA = (
    "stv3-worker-acquisition-control-store-reclamation-post-view-v1"
)
CONTROL_RECLAMATION_FENCE_SCHEMA = (
    "stv3-worker-acquisition-control-store-reclamation-fence-v1"
)
CONTROL_RECLAMATION_RELEASE_SCHEMA = (
    "stv3-worker-acquisition-control-store-reclamation-release-v1"
)

MAX_CONTROL_RECLAMATION_DECISIONS = 4096
MAX_CONTROL_RECLAMATION_MEMBERS = 4096
MAX_CONTROL_RECLAMATION_INVENTORY_BYTES = 128 * 1024 * 1024
CANDIDATE_PAGE_SIZE = 64

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_ROLES = frozenset(PARTICIPANT_APPLY_ORDER)
_CONTROL_ROLES = frozenset({
    "execution_receipts",
    "scheduler",
    "capacity_ledger",
})

CONTROL_RECLAMATION_TABLE_COLUMNS: Mapping[str, frozenset[str]] = {
    "source_reclamation_meta": frozenset({
        "singleton",
        "schema_version",
        "contract",
        "role",
        "store_instance_sha256",
        "base_store_binding_sha256",
        "candidate_authority_sha256",
        "coordinator_authority_sha256",
        "next_fence_epoch",
        "last_observed_at",
        "created_at",
    }),
    "source_reclamation_decisions": frozenset({
        "request_sha256",
        "request_json",
        "state",
        "active_slot",
        "prepare_receipt_json",
        "source_view_sha256",
        "child_reference_root_sha256",
        "source_fence_sha256",
        "candidate_count",
        "releasable_bytes",
        "releasable_entries",
        "prepared_at",
        "committed_decision_sha256",
        "committed_receipt_sha256",
        "predecessor_apply_receipt_sha256",
        "effect_sha256",
        "post_apply_view_sha256",
        "apply_receipt_json",
        "applied_at",
        "abort_decision_sha256",
        "fence_release_root_sha256",
        "abort_receipt_json",
        "aborted_at",
    }),
    "source_reclamation_members": frozenset({
        "request_sha256",
        "candidate_sha256",
        "primary_identity",
        "secondary_identity",
        "binding_json",
        "source_record_json",
        "view_sha256",
        "member_sha256",
    }),
    "source_reclamation_tombstones": frozenset({
        "candidate_sha256",
        "request_sha256",
        "primary_identity",
        "secondary_identity",
        "source_record_sha256",
        "effect_sha256",
        "created_at",
    }),
}
CONTROL_RECLAMATION_TABLES = frozenset(CONTROL_RECLAMATION_TABLE_COLUMNS)


class AcquisitionControlReclamationError(RuntimeError):
    """Base error for an inert control-store participant."""


class AcquisitionControlReclamationIntegrityError(
    AcquisitionControlReclamationError
):
    """A source row, store identity, schema, or authority differs."""


class AcquisitionControlReclamationConflict(AcquisitionControlReclamationError):
    """A stable request or source identity was reused inconsistently."""


class AcquisitionControlReclamationCapacityError(
    AcquisitionControlReclamationError
):
    """A bounded inventory or SQLite envelope is exhausted."""


def _normalise_sql(value: str) -> str:
    collapsed = " ".join(value.replace(";", "").split()).lower()
    collapsed = re.sub(
        r"^create (table|unique index|index|trigger) if not exists ",
        r"create \1 ",
        collapsed,
    )
    return re.sub(r"\s*([(),=!<>])\s*", r"\1", collapsed)


def _base_schema_objects(
    database: sqlite3.Connection,
) -> Mapping[tuple[str, str, str], str]:
    """Return every declared non-reclamation schema object exactly."""

    return {
        (str(row[0]), str(row[1]), str(row[2])): _normalise_sql(str(row[3]))
        for row in database.execute(
            "SELECT type,name,tbl_name,sql FROM sqlite_schema "
            "WHERE type IN ('table','index','trigger') AND sql IS NOT NULL "
            "AND name NOT LIKE 'sqlite_%'"
        )
        if str(row[1]) not in CONTROL_RECLAMATION_SCHEMA_OBJECT_NAMES
    }


def _finite_timestamp(value: object, label: str) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        or not 0.0 <= float(value) <= 32_503_680_000.0
    ):
        raise AcquisitionControlReclamationIntegrityError(f"{label} is invalid")
    return float(value)


def _canonical_mapping(value: Mapping[str, Any]) -> bytes:
    try:
        return canonical_bytes(dict(value))
    except AcquisitionSourceReclamationProtocolError as exc:
        raise AcquisitionControlReclamationIntegrityError(
            "control reclamation value is not canonical"
        ) from exc


def _ordered_chain_root(
    *,
    schema: str,
    rows: Sequence[Mapping[str, Any]],
) -> str:
    if not isinstance(schema, str) or not schema or not rows:
        raise AcquisitionControlReclamationIntegrityError(
            "control reclamation chain input is invalid"
        )
    head = digest_mapping({"count": len(rows), "schema": f"{schema}-head"})
    for ordinal, row in enumerate(rows):
        head = digest_mapping({
            "item_sha256": digest_mapping(dict(row)),
            "ordinal": ordinal,
            "previous_sha256": head,
            "schema": f"{schema}-item",
        })
    return digest_mapping({
        "count": len(rows),
        "head_sha256": head,
        "schema": f"{schema}-final",
    })


def _decode_mapping(blob: object, *, label: str) -> dict[str, Any]:
    try:
        payload = bytes(blob)
        value = json.loads(payload.decode("ascii"))
    except (TypeError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AcquisitionControlReclamationIntegrityError(
            f"{label} is corrupt"
        ) from exc
    if not isinstance(value, dict) or payload != _canonical_mapping(value):
        raise AcquisitionControlReclamationIntegrityError(
            f"{label} is not canonical"
        )
    return value


def _path_witness(path: Path) -> tuple[int, int, int, int]:
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise AcquisitionControlReclamationIntegrityError(
            "control source-store path is unavailable"
        ) from exc
    if (
        stat.S_ISLNK(metadata.st_mode)
        or not stat.S_ISREG(metadata.st_mode)
        or int(metadata.st_nlink) != 1
    ):
        raise AcquisitionControlReclamationIntegrityError(
            "control source-store path is not an exclusive regular file"
        )
    return (
        int(metadata.st_dev),
        int(metadata.st_ino),
        stat.S_IFMT(metadata.st_mode),
        int(metadata.st_nlink),
    )


def _normalised_path_sha256(path: Path) -> str:
    value = os.path.normcase(os.path.normpath(str(path.resolve(strict=True))))
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True, repr=False)
class HeldControlCandidate:
    binding: SourceReclamationCandidateBindingV1
    record: Mapping[str, Any]
    record_payload: bytes


@dataclass(frozen=True, slots=True, repr=False)
class HeldControlCandidateInventory:
    request_sha256: str
    candidates: tuple[HeldControlCandidate, ...]
    total_bytes: int


def _candidate_projection(
    binding: SourceReclamationCandidateBindingV1,
) -> dict[str, Any]:
    return {
        "acquisition_job_id": binding.acquisition_job_id,
        "evidence_stream_id": binding.evidence_stream_id,
        "evidence_stream_sequence": binding.evidence_stream_sequence,
        "raw_capture_id": binding.raw_capture_id,
        "scheduler_job_id": binding.scheduler_job_id,
    }


@contextmanager
def hold_control_candidate_inventory(
    authority: SourceReclamationCandidateAuthority,
    request: SourceReclamationRequestV2,
) -> Iterator[HeldControlCandidateInventory]:
    """Hold every bounded candidate page across one participant commit."""

    if not isinstance(authority, SourceReclamationCandidateAuthority):
        raise TypeError("candidate authority has the wrong type")
    if not isinstance(request, SourceReclamationRequestV2):
        raise TypeError("request has the wrong type")
    if not hmac.compare_digest(
        request.candidate_authority_sha256,
        authority.authority_sha256,
    ):
        raise AcquisitionControlReclamationConflict(
            "candidate authority differs from the request"
        )
    items: list[HeldControlCandidate] = []
    cursor = 0
    total_bytes = 0
    with ExitStack() as stack:
        while cursor < request.candidate_count:
            page = stack.enter_context(authority.hold_candidate_page_v1(
                request,
                after_cursor=cursor,
                max_candidates=min(
                    CANDIDATE_PAGE_SIZE,
                    request.candidate_count - cursor,
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
                or page.after_cursor != cursor
                or page.total_candidate_count != request.candidate_count
                or not page.bindings
            ):
                raise AcquisitionControlReclamationIntegrityError(
                    "held candidate page differs from the request"
                )
            for binding, payload, record in zip(
                page.bindings,
                page.record_payloads,
                page.decoded_records(),
                strict=True,
            ):
                if (
                    record.get("candidate_sha256")
                    != binding.candidate_sha256
                    or record.get("record_sha256")
                    != binding.sealed_archive_record_sha256
                ):
                    raise AcquisitionControlReclamationIntegrityError(
                        "held candidate record binding differs"
                    )
                total_bytes += len(payload)
                if total_bytes > MAX_CONTROL_RECLAMATION_INVENTORY_BYTES:
                    raise AcquisitionControlReclamationCapacityError(
                        "held candidate inventory exceeds its byte bound"
                    )
                items.append(HeldControlCandidate(
                    binding=binding,
                    record=record,
                    record_payload=payload,
                ))
            cursor = page.next_cursor
            if page.exhausted != (cursor == request.candidate_count):
                raise AcquisitionControlReclamationIntegrityError(
                    "held candidate exhaustion differs"
                )
        if (
            len(items) != request.candidate_count
            or tuple(item.binding.candidate_sha256 for item in items)
            != tuple(sorted(item.binding.candidate_sha256 for item in items))
            or len({item.binding.candidate_sha256 for item in items})
            != len(items)
        ):
            raise AcquisitionControlReclamationIntegrityError(
                "held candidate inventory differs"
            )
        bindings = tuple(item.binding for item in items)
        payloads = tuple(item.record_payload for item in items)
        if (
            candidate_binding_root(bindings)
            != request.candidate_bindings_sha256
            or archive_binding_root(bindings, payloads)
            != request.archive_binding_root_sha256
        ):
            raise AcquisitionControlReclamationIntegrityError(
                "held candidate inventory roots differ"
            )
        yield HeldControlCandidateInventory(
            request_sha256=request.request_sha256,
            candidates=tuple(items),
            total_bytes=total_bytes,
        )


_CONTROL_TABLE_DDL: tuple[tuple[str, str], ...] = (
    (
        "source_reclamation_meta",
        """
        CREATE TABLE source_reclamation_meta(
            singleton INTEGER PRIMARY KEY CHECK(singleton=1),
            schema_version INTEGER NOT NULL,
            contract TEXT NOT NULL,
            role TEXT NOT NULL,
            store_instance_sha256 TEXT NOT NULL,
            base_store_binding_sha256 TEXT NOT NULL,
            candidate_authority_sha256 TEXT NOT NULL,
            coordinator_authority_sha256 TEXT NOT NULL,
            next_fence_epoch INTEGER NOT NULL CHECK(next_fence_epoch>=1),
            last_observed_at REAL,
            created_at REAL NOT NULL
        ) WITHOUT ROWID
        """,
    ),
    (
        "source_reclamation_decisions",
        """
        CREATE TABLE source_reclamation_decisions(
            request_sha256 TEXT PRIMARY KEY,
            request_json BLOB NOT NULL,
            state TEXT NOT NULL CHECK(state IN(
                'PREPARED','APPLYING','APPLIED','ABORTED'
            )),
            active_slot INTEGER UNIQUE CHECK(active_slot=1 OR active_slot IS NULL),
            prepare_receipt_json BLOB NOT NULL,
            source_view_sha256 TEXT NOT NULL,
            child_reference_root_sha256 TEXT NOT NULL,
            source_fence_sha256 TEXT NOT NULL,
            candidate_count INTEGER NOT NULL CHECK(candidate_count>=1),
            releasable_bytes INTEGER NOT NULL CHECK(releasable_bytes>=0),
            releasable_entries INTEGER NOT NULL CHECK(releasable_entries>=0),
            prepared_at REAL NOT NULL,
            committed_decision_sha256 TEXT,
            committed_receipt_sha256 TEXT,
            predecessor_apply_receipt_sha256 TEXT,
            effect_sha256 TEXT,
            post_apply_view_sha256 TEXT,
            apply_receipt_json BLOB,
            applied_at REAL,
            abort_decision_sha256 TEXT,
            fence_release_root_sha256 TEXT,
            abort_receipt_json BLOB,
            aborted_at REAL,
            CHECK(
                (state='PREPARED' AND active_slot=1 AND
                 committed_decision_sha256 IS NULL AND
                 committed_receipt_sha256 IS NULL AND
                 predecessor_apply_receipt_sha256 IS NULL AND
                 effect_sha256 IS NULL AND post_apply_view_sha256 IS NULL AND
                 apply_receipt_json IS NULL AND applied_at IS NULL AND
                 abort_decision_sha256 IS NULL AND
                 fence_release_root_sha256 IS NULL AND
                 abort_receipt_json IS NULL AND aborted_at IS NULL)
                OR
                (state='APPLYING' AND active_slot=1 AND
                 committed_decision_sha256 IS NOT NULL AND
                 committed_receipt_sha256 IS NOT NULL AND
                 predecessor_apply_receipt_sha256 IS NOT NULL AND
                 effect_sha256 IS NULL AND post_apply_view_sha256 IS NULL AND
                 apply_receipt_json IS NULL AND applied_at IS NULL AND
                 abort_decision_sha256 IS NULL AND
                 fence_release_root_sha256 IS NULL AND
                 abort_receipt_json IS NULL AND aborted_at IS NULL)
                OR
                (state='APPLIED' AND active_slot IS NULL AND
                 committed_decision_sha256 IS NOT NULL AND
                 committed_receipt_sha256 IS NOT NULL AND
                 predecessor_apply_receipt_sha256 IS NOT NULL AND
                 effect_sha256 IS NOT NULL AND
                 post_apply_view_sha256 IS NOT NULL AND
                 apply_receipt_json IS NOT NULL AND applied_at IS NOT NULL AND
                 abort_decision_sha256 IS NULL AND
                 fence_release_root_sha256 IS NULL AND
                 abort_receipt_json IS NULL AND aborted_at IS NULL)
                OR
                (state='ABORTED' AND active_slot IS NULL AND
                 committed_decision_sha256 IS NULL AND
                 committed_receipt_sha256 IS NULL AND
                 predecessor_apply_receipt_sha256 IS NULL AND
                 effect_sha256 IS NULL AND post_apply_view_sha256 IS NULL AND
                 apply_receipt_json IS NULL AND applied_at IS NULL AND
                 abort_decision_sha256 IS NOT NULL AND
                 fence_release_root_sha256 IS NOT NULL AND
                 abort_receipt_json IS NOT NULL AND aborted_at IS NOT NULL)
            )
        ) WITHOUT ROWID
        """,
    ),
    (
        "source_reclamation_members",
        """
        CREATE TABLE source_reclamation_members(
            request_sha256 TEXT NOT NULL REFERENCES
                source_reclamation_decisions(request_sha256),
            candidate_sha256 TEXT NOT NULL,
            primary_identity TEXT NOT NULL,
            secondary_identity TEXT NOT NULL,
            binding_json BLOB NOT NULL,
            source_record_json BLOB NOT NULL,
            view_sha256 TEXT NOT NULL,
            member_sha256 TEXT NOT NULL UNIQUE,
            PRIMARY KEY(request_sha256,candidate_sha256)
        ) WITHOUT ROWID
        """,
    ),
    (
        "source_reclamation_tombstones",
        """
        CREATE TABLE source_reclamation_tombstones(
            candidate_sha256 TEXT PRIMARY KEY,
            request_sha256 TEXT NOT NULL REFERENCES
                source_reclamation_decisions(request_sha256),
            primary_identity TEXT NOT NULL UNIQUE,
            secondary_identity TEXT NOT NULL UNIQUE,
            source_record_sha256 TEXT NOT NULL,
            effect_sha256 TEXT NOT NULL,
            created_at REAL NOT NULL
        ) WITHOUT ROWID
        """,
    ),
)

_CONTROL_INDEX_DDL: tuple[tuple[str, str], ...] = (
    (
        "source_reclamation_member_request_order",
        "CREATE INDEX source_reclamation_member_request_order ON "
        "source_reclamation_members(request_sha256,candidate_sha256)",
    ),
)


def _control_trigger_definitions() -> Mapping[str, str]:
    return {
        "source_reclamation_meta_no_delete": (
            "CREATE TRIGGER source_reclamation_meta_no_delete BEFORE DELETE ON "
            "source_reclamation_meta BEGIN SELECT RAISE(ABORT,'reclamation "
            "metadata is immutable'); END"
        ),
        "source_reclamation_meta_update_guard": (
            "CREATE TRIGGER source_reclamation_meta_update_guard BEFORE UPDATE ON "
            "source_reclamation_meta WHEN NEW.singleton<>OLD.singleton OR "
            "NEW.schema_version<>OLD.schema_version OR NEW.contract<>OLD.contract "
            "OR NEW.role<>OLD.role OR NEW.store_instance_sha256<>"
            "OLD.store_instance_sha256 OR NEW.base_store_binding_sha256<>"
            "OLD.base_store_binding_sha256 OR NEW.candidate_authority_sha256<>"
            "OLD.candidate_authority_sha256 OR NEW.coordinator_authority_sha256<>"
            "OLD.coordinator_authority_sha256 OR NEW.next_fence_epoch<="
            "OLD.next_fence_epoch OR (OLD.last_observed_at IS NOT NULL AND "
            "(NEW.last_observed_at IS NULL OR NEW.last_observed_at<"
            "OLD.last_observed_at)) OR NEW.created_at<>OLD.created_at BEGIN "
            "SELECT RAISE(ABORT,'reclamation metadata transition differs'); END"
        ),
        "source_reclamation_decision_no_delete": (
            "CREATE TRIGGER source_reclamation_decision_no_delete BEFORE DELETE ON "
            "source_reclamation_decisions BEGIN SELECT RAISE(ABORT,'reclamation "
            "decision is immutable'); END"
        ),
        "source_reclamation_decision_update_guard": (
            "CREATE TRIGGER source_reclamation_decision_update_guard BEFORE UPDATE "
            "ON source_reclamation_decisions WHEN NEW.request_sha256<>"
            "OLD.request_sha256 OR NEW.request_json<>OLD.request_json OR "
            "NEW.prepare_receipt_json<>OLD.prepare_receipt_json OR "
            "NEW.source_view_sha256<>OLD.source_view_sha256 OR "
            "NEW.child_reference_root_sha256<>OLD.child_reference_root_sha256 OR "
            "NEW.source_fence_sha256<>OLD.source_fence_sha256 OR "
            "NEW.candidate_count<>OLD.candidate_count OR NEW.releasable_bytes<>"
            "OLD.releasable_bytes OR NEW.releasable_entries<>"
            "OLD.releasable_entries OR NEW.prepared_at<>OLD.prepared_at OR NOT ("
            "(OLD.state='PREPARED' AND NEW.state IN('APPLYING','ABORTED')) OR "
            "(OLD.state='APPLYING' AND NEW.state='APPLIED')) BEGIN SELECT "
            "RAISE(ABORT,'reclamation decision transition differs'); END"
        ),
        "source_reclamation_members_no_update": (
            "CREATE TRIGGER source_reclamation_members_no_update BEFORE UPDATE ON "
            "source_reclamation_members BEGIN SELECT RAISE(ABORT,'reclamation "
            "member is immutable'); END"
        ),
        "source_reclamation_members_no_delete": (
            "CREATE TRIGGER source_reclamation_members_no_delete BEFORE DELETE ON "
            "source_reclamation_members BEGIN SELECT RAISE(ABORT,'reclamation "
            "member is immutable'); END"
        ),
        "source_reclamation_tombstones_no_update": (
            "CREATE TRIGGER source_reclamation_tombstones_no_update BEFORE UPDATE "
            "ON source_reclamation_tombstones BEGIN SELECT RAISE(ABORT,'source "
            "tombstone is immutable'); END"
        ),
        "source_reclamation_tombstones_no_delete": (
            "CREATE TRIGGER source_reclamation_tombstones_no_delete BEFORE DELETE "
            "ON source_reclamation_tombstones BEGIN SELECT RAISE(ABORT,'source "
            "tombstone is immutable'); END"
        ),
    }


def _source_trigger_definitions(role: str) -> Mapping[str, str]:
    if role == "execution_receipts":
        return {
            "execution_reclamation_receipt_insert_guard": (
                "CREATE TRIGGER execution_reclamation_receipt_insert_guard "
                "BEFORE INSERT ON execution_receipts WHEN EXISTS(SELECT 1 FROM "
                "source_reclamation_tombstones WHERE primary_identity="
                "NEW.acquisition_job_id OR secondary_identity=NEW.scheduler_job_id) "
                "OR EXISTS(SELECT 1 FROM source_reclamation_members AS member "
                "JOIN source_reclamation_decisions AS decision USING(request_sha256) "
                "WHERE decision.state IN('PREPARED','APPLYING') AND ("
                "member.primary_identity=NEW.acquisition_job_id OR "
                "member.secondary_identity=NEW.scheduler_job_id)) BEGIN SELECT "
                "RAISE(ABORT,'execution source identity is fenced'); END"
            ),
            "execution_reclamation_receipt_update_guard": (
                "CREATE TRIGGER execution_reclamation_receipt_update_guard "
                "BEFORE UPDATE ON execution_receipts WHEN EXISTS(SELECT 1 FROM "
                "source_reclamation_tombstones WHERE primary_identity IN("
                "OLD.acquisition_job_id,NEW.acquisition_job_id) OR "
                "secondary_identity IN(OLD.scheduler_job_id,NEW.scheduler_job_id)) "
                "OR EXISTS(SELECT 1 FROM source_reclamation_members AS member JOIN "
                "source_reclamation_decisions AS decision USING(request_sha256) "
                "WHERE decision.state IN('PREPARED','APPLYING') AND ("
                "member.primary_identity IN(OLD.acquisition_job_id,"
                "NEW.acquisition_job_id) OR member.secondary_identity IN("
                "OLD.scheduler_job_id,NEW.scheduler_job_id))) BEGIN SELECT "
                "RAISE(ABORT,'execution source identity is fenced'); END"
            ),
            "execution_reclamation_receipt_delete_guard": (
                "CREATE TRIGGER execution_reclamation_receipt_delete_guard "
                "BEFORE DELETE ON execution_receipts WHEN NOT EXISTS(SELECT 1 FROM "
                "source_reclamation_members AS member JOIN "
                "source_reclamation_decisions AS decision USING(request_sha256) "
                "WHERE decision.state='APPLYING' AND "
                "member.primary_identity=OLD.acquisition_job_id) BEGIN SELECT "
                "RAISE(ABORT,'execution receipt deletion is unauthorized'); END"
            ),
            "execution_reclamation_head_delete_guard": (
                "CREATE TRIGGER execution_reclamation_head_delete_guard BEFORE "
                "DELETE ON terminal_heads WHEN NOT EXISTS(SELECT 1 FROM "
                "source_reclamation_members AS member JOIN "
                "source_reclamation_decisions AS decision USING(request_sha256) "
                "WHERE decision.state='APPLYING' AND "
                "member.primary_identity=OLD.acquisition_job_id) BEGIN SELECT "
                "RAISE(ABORT,'execution head deletion is unauthorized'); END"
            ),
            "execution_reclamation_head_insert_guard": (
                "CREATE TRIGGER execution_reclamation_head_insert_guard BEFORE "
                "INSERT ON terminal_heads WHEN EXISTS(SELECT 1 FROM "
                "source_reclamation_tombstones WHERE primary_identity="
                "NEW.acquisition_job_id) OR EXISTS(SELECT 1 FROM "
                "source_reclamation_members AS member JOIN "
                "source_reclamation_decisions AS decision USING(request_sha256) "
                "WHERE decision.state IN('PREPARED','APPLYING') AND "
                "member.primary_identity=NEW.acquisition_job_id) BEGIN SELECT "
                "RAISE(ABORT,'execution head identity is fenced'); END"
            ),
            "execution_reclamation_head_update_guard": (
                "CREATE TRIGGER execution_reclamation_head_update_guard BEFORE "
                "UPDATE ON terminal_heads BEGIN SELECT RAISE(ABORT,'execution "
                "head is immutable'); END"
            ),
        }
    if role == "scheduler":
        return {
            "scheduler_reclamation_insert_guard": (
                "CREATE TRIGGER scheduler_reclamation_insert_guard BEFORE INSERT "
                "ON scheduler_outbox WHEN EXISTS(SELECT 1 FROM "
                "source_reclamation_tombstones WHERE primary_identity=NEW.job_id "
                "OR secondary_identity=NEW.slot_id) OR EXISTS(SELECT 1 FROM "
                "source_reclamation_members AS member JOIN "
                "source_reclamation_decisions AS decision USING(request_sha256) "
                "WHERE decision.state IN('PREPARED','APPLYING') AND ("
                "member.primary_identity=NEW.job_id OR "
                "member.secondary_identity=NEW.slot_id)) BEGIN SELECT "
                "RAISE(ABORT,'scheduler source identity is fenced'); END"
            ),
            "scheduler_reclamation_update_guard": (
                "CREATE TRIGGER scheduler_reclamation_update_guard BEFORE UPDATE "
                "ON scheduler_outbox WHEN EXISTS(SELECT 1 FROM "
                "source_reclamation_tombstones WHERE primary_identity IN("
                "OLD.job_id,NEW.job_id) OR secondary_identity IN("
                "OLD.slot_id,NEW.slot_id)) OR EXISTS(SELECT 1 FROM "
                "source_reclamation_members AS member JOIN "
                "source_reclamation_decisions AS decision USING(request_sha256) "
                "WHERE decision.state IN('PREPARED','APPLYING') AND ("
                "member.primary_identity IN(OLD.job_id,NEW.job_id) OR "
                "member.secondary_identity IN(OLD.slot_id,NEW.slot_id))) "
                "BEGIN SELECT RAISE(ABORT,"
                "'scheduler source identity is fenced'); END"
            ),
            "scheduler_reclamation_delete_guard": (
                "CREATE TRIGGER scheduler_reclamation_delete_guard BEFORE DELETE "
                "ON scheduler_outbox WHEN NOT EXISTS(SELECT 1 FROM "
                "source_reclamation_members AS member JOIN "
                "source_reclamation_decisions AS decision USING(request_sha256) "
                "WHERE decision.state='APPLYING' AND "
                "member.primary_identity=OLD.job_id) BEGIN SELECT RAISE(ABORT,"
                "'scheduler source deletion is unauthorized'); END"
            ),
        }
    if role == "capacity_ledger":
        return {
            "capacity_reclamation_insert_guard": (
                "CREATE TRIGGER capacity_reclamation_insert_guard BEFORE INSERT "
                "ON reservations WHEN EXISTS(SELECT 1 FROM "
                "source_reclamation_tombstones WHERE primary_identity="
                "NEW.reservation_id OR secondary_identity=NEW.job_identity) OR "
                "EXISTS(SELECT 1 FROM source_reclamation_members AS member JOIN "
                "source_reclamation_decisions AS decision USING(request_sha256) "
                "WHERE decision.state IN('PREPARED','APPLYING') AND ("
                "member.primary_identity=NEW.reservation_id OR "
                "member.secondary_identity=NEW.job_identity)) BEGIN SELECT "
                "RAISE(ABORT,'capacity source identity is fenced'); END"
            ),
            "capacity_reclamation_update_guard": (
                "CREATE TRIGGER capacity_reclamation_update_guard BEFORE UPDATE "
                "ON reservations WHEN EXISTS(SELECT 1 FROM "
                "source_reclamation_tombstones WHERE primary_identity IN("
                "OLD.reservation_id,NEW.reservation_id) OR secondary_identity IN("
                "OLD.job_identity,NEW.job_identity)) OR EXISTS(SELECT 1 FROM "
                "source_reclamation_members AS member JOIN "
                "source_reclamation_decisions AS decision USING(request_sha256) "
                "WHERE decision.state IN('PREPARED','APPLYING') AND ("
                "member.primary_identity IN(OLD.reservation_id,"
                "NEW.reservation_id) OR member.secondary_identity IN("
                "OLD.job_identity,NEW.job_identity))) BEGIN SELECT "
                "RAISE(ABORT,'capacity source identity is fenced'); END"
            ),
            "capacity_reclamation_delete_guard": (
                "CREATE TRIGGER capacity_reclamation_delete_guard BEFORE DELETE "
                "ON reservations WHEN NOT EXISTS(SELECT 1 FROM "
                "source_reclamation_members AS member JOIN "
                "source_reclamation_decisions AS decision USING(request_sha256) "
                "WHERE decision.state='APPLYING' AND "
                "member.primary_identity=OLD.reservation_id) BEGIN SELECT "
                "RAISE(ABORT,'capacity source deletion is unauthorized'); END"
            ),
        }
    raise ValueError("role is outside the control-store catalogue")


CONTROL_RECLAMATION_INDEXES = frozenset(name for name, _ in _CONTROL_INDEX_DDL)
CONTROL_RECLAMATION_TRIGGERS = frozenset(_control_trigger_definitions())
CONTROL_RECLAMATION_SOURCE_TRIGGERS = frozenset(
    name
    for role in sorted(_CONTROL_ROLES)
    for name in _source_trigger_definitions(role)
)
CONTROL_RECLAMATION_SCHEMA_OBJECT_NAMES = (
    CONTROL_RECLAMATION_TABLES
    | CONTROL_RECLAMATION_INDEXES
    | CONTROL_RECLAMATION_TRIGGERS
    | CONTROL_RECLAMATION_SOURCE_TRIGGERS
)


def control_reclamation_extension_present(database: sqlite3.Connection) -> bool:
    names = {
        str(row[0])
        for row in database.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%'"
        )
    }
    overlap = names & CONTROL_RECLAMATION_TABLES
    if overlap and overlap != CONTROL_RECLAMATION_TABLES:
        raise AcquisitionControlReclamationIntegrityError(
            "control reclamation extension is partial"
        )
    return overlap == CONTROL_RECLAMATION_TABLES


def validate_control_reclamation_extension_schema(
    database: sqlite3.Connection,
    *,
    expected_role: str,
    expected_base_store_binding_sha256: str | None = None,
    expected_candidate_authority_sha256: str | None = None,
    expected_coordinator_authority_sha256: str | None = None,
) -> sqlite3.Row:
    """Validate the complete additive schema and return immutable metadata."""

    if expected_role not in _CONTROL_ROLES:
        raise ValueError("expected_role is outside the control-store catalogue")
    if not control_reclamation_extension_present(database):
        raise AcquisitionControlReclamationIntegrityError(
            "control reclamation extension is unavailable"
        )
    actual_tables = {
        str(row[0]): _normalise_sql(str(row[1]))
        for row in database.execute(
            "SELECT name,sql FROM sqlite_master WHERE type='table' "
            "AND name IN ({})".format(
                ",".join("?" for _ in CONTROL_RECLAMATION_TABLES)
            ),
            tuple(sorted(CONTROL_RECLAMATION_TABLES)),
        )
    }
    expected_tables = {
        name: _normalise_sql(statement)
        for name, statement in _CONTROL_TABLE_DDL
    }
    if actual_tables != expected_tables:
        raise AcquisitionControlReclamationIntegrityError(
            "control reclamation extension table definitions differ"
        )
    for table, columns in CONTROL_RECLAMATION_TABLE_COLUMNS.items():
        actual = frozenset(
            str(row[1]) for row in database.execute(f"PRAGMA table_info({table})")
        )
        if actual != columns:
            raise AcquisitionControlReclamationIntegrityError(
                "control reclamation extension columns differ"
            )
    actual_indexes = {
        str(row[0]): _normalise_sql(str(row[1]))
        for row in database.execute(
            "SELECT name,sql FROM sqlite_master WHERE type='index' "
            "AND name IN ({})".format(
                ",".join("?" for _ in CONTROL_RECLAMATION_INDEXES)
            ),
            tuple(sorted(CONTROL_RECLAMATION_INDEXES)),
        )
    }
    expected_indexes = {
        name: _normalise_sql(statement) for name, statement in _CONTROL_INDEX_DDL
    }
    if actual_indexes != expected_indexes:
        raise AcquisitionControlReclamationIntegrityError(
            "control reclamation extension indexes differ"
        )
    actual_triggers = {
        str(row[0]): _normalise_sql(str(row[1]))
        for row in database.execute(
            "SELECT name,sql FROM sqlite_master WHERE type='trigger' "
            "AND name IN ({})".format(
                ",".join("?" for _ in CONTROL_RECLAMATION_TRIGGERS)
            ),
            tuple(sorted(CONTROL_RECLAMATION_TRIGGERS)),
        )
    }
    expected_triggers = {
        name: _normalise_sql(statement)
        for name, statement in _control_trigger_definitions().items()
    }
    if actual_triggers != expected_triggers:
        raise AcquisitionControlReclamationIntegrityError(
            "control reclamation extension triggers differ"
        )
    expected_source_triggers = {
        name: _normalise_sql(statement)
        for name, statement in _source_trigger_definitions(expected_role).items()
    }
    source_trigger_names = tuple(sorted(CONTROL_RECLAMATION_SOURCE_TRIGGERS))
    actual_source_triggers = {
        str(row[0]): _normalise_sql(str(row[1]))
        for row in database.execute(
            "SELECT name,sql FROM sqlite_master WHERE type='trigger' "
            "AND name IN ({})".format(
                ",".join("?" for _ in source_trigger_names)
            ),
            source_trigger_names,
        )
    }
    if actual_source_triggers != expected_source_triggers:
        raise AcquisitionControlReclamationIntegrityError(
            "control reclamation source triggers differ"
        )
    if database.execute("PRAGMA foreign_key_check").fetchone() is not None:
        raise AcquisitionControlReclamationIntegrityError(
            "control reclamation foreign-key closure differs"
        )
    rows = database.execute(
        "SELECT * FROM source_reclamation_meta"
    ).fetchall()
    if len(rows) != 1:
        raise AcquisitionControlReclamationIntegrityError(
            "control reclamation metadata singleton differs"
        )
    row = rows[0]
    for key in (
        "store_instance_sha256",
        "base_store_binding_sha256",
        "candidate_authority_sha256",
        "coordinator_authority_sha256",
    ):
        require_sha256(str(row[key]), f"control reclamation {key}")
    if (
        int(row["singleton"]) != 1
        or int(row["schema_version"]) != CONTROL_RECLAMATION_SCHEMA_VERSION
        or str(row["contract"]) != CONTROL_RECLAMATION_CONTRACT
        or str(row["role"]) != expected_role
        or int(row["next_fence_epoch"]) < 1
    ):
        raise AcquisitionControlReclamationIntegrityError(
            "control reclamation metadata differs"
        )
    if (
        expected_base_store_binding_sha256 is not None
        and not hmac.compare_digest(
            str(row["base_store_binding_sha256"]),
            require_sha256(
                expected_base_store_binding_sha256,
                "expected base store binding SHA-256",
            ),
        )
    ):
        raise AcquisitionControlReclamationIntegrityError(
            "control reclamation base-store binding differs"
        )
    if (
        expected_candidate_authority_sha256 is not None
        and not hmac.compare_digest(
            str(row["candidate_authority_sha256"]),
            require_sha256(
                expected_candidate_authority_sha256,
                "expected candidate authority SHA-256",
            ),
        )
    ):
        raise AcquisitionControlReclamationIntegrityError(
            "control reclamation candidate authority differs"
        )
    if (
        expected_coordinator_authority_sha256 is not None
        and not hmac.compare_digest(
            str(row["coordinator_authority_sha256"]),
            require_sha256(
                expected_coordinator_authority_sha256,
                "expected coordinator authority SHA-256",
            ),
        )
    ):
        raise AcquisitionControlReclamationIntegrityError(
            "control reclamation coordinator authority differs"
        )
    return row


class _ControlReclamationParticipantV2:
    """Shared transaction and authority machinery for three control stores."""

    role: str

    def __init__(
        self,
        store: object,
        *,
        candidate_authority: SourceReclamationCandidateAuthority,
        committed_decision_authority: HeldCommittedDecisionAuthority,
        clock: Callable[[], float] = time.time,
        fault_injector: Callable[[str], None] | None = None,
    ) -> None:
        if self.role not in _CONTROL_ROLES:
            raise TypeError("participant role is outside the control-store catalogue")
        if not isinstance(candidate_authority, SourceReclamationCandidateAuthority):
            raise TypeError("candidate_authority has the wrong type")
        if not isinstance(
            committed_decision_authority,
            HeldCommittedDecisionAuthority,
        ):
            raise TypeError("committed_decision_authority has the wrong type")
        if not callable(clock):
            raise TypeError("clock must be callable")
        if fault_injector is not None and not callable(fault_injector):
            raise TypeError("fault_injector must be callable")
        path = getattr(store, "path", None)
        connect = getattr(store, "_connect", None)
        if not isinstance(path, Path) or not path.is_absolute() or not callable(connect):
            raise TypeError("control source store has no absolute SQLite authority")
        self._store = store
        self.path = path
        self._candidate = candidate_authority
        self._committed = committed_decision_authority
        self._clock = clock
        self._fault_injector = fault_injector
        inherited_lock = getattr(store, "_lock", None)
        self._lock = (
            inherited_lock
            if hasattr(inherited_lock, "__enter__")
            else threading.RLock()
        )
        self._base_store_binding = self._base_store_binding_sha256()
        self._database_witness: tuple[int, int, int, int] | None = None
        self._store_instance_sha256: str | None = None
        self._install_extension()

    def _fault(self, point: str) -> None:
        if self._fault_injector is not None:
            self._fault_injector(f"{self.role}.{point}")

    def _base_store_binding_sha256(self) -> str:
        raise NotImplementedError

    def _source_snapshot(
        self,
        database: sqlite3.Connection,
        inventory: HeldControlCandidateInventory,
    ) -> tuple[tuple[dict[str, Any], ...], int, int]:
        raise NotImplementedError

    def _validate_base_schema(self, database: sqlite3.Connection) -> None:
        """Validate role-specific source DDL before trusting extension rows."""

        raise NotImplementedError

    def _apply_source_rows(
        self,
        database: sqlite3.Connection,
        *,
        request_sha256: str,
        members: tuple[dict[str, Any], ...],
    ) -> tuple[int, int]:
        raise NotImplementedError

    def _reproof_stored_members(
        self,
        database: sqlite3.Connection,
        *,
        members: tuple[dict[str, Any], ...],
    ) -> tuple[int, int]:
        """Reprove immutable local source rows without archive-page material."""

        raise NotImplementedError

    def _post_apply_projection(
        self,
        database: sqlite3.Connection,
        *,
        request_sha256: str,
        members: tuple[dict[str, Any], ...],
    ) -> dict[str, Any]:
        raise NotImplementedError

    def _connect(self) -> sqlite3.Connection:
        self._verify_path()
        database = self._store._connect()
        try:
            database.row_factory = sqlite3.Row
            database.execute("PRAGMA foreign_keys=ON")
            database.execute("PRAGMA synchronous=FULL")
            return database
        except BaseException:
            database.close()
            raise

    def _verify_path(self) -> None:
        witness = _path_witness(self.path)
        if self._database_witness is not None and witness != self._database_witness:
            raise AcquisitionControlReclamationIntegrityError(
                "control source-store path authority changed"
            )

    def _install_extension(self) -> None:
        with self._lock, closing(self._store._connect()) as database:
            database.row_factory = sqlite3.Row
            database.execute("PRAGMA foreign_keys=ON")
            before = _path_witness(self.path)
            try:
                database.execute("BEGIN IMMEDIATE")
                present = control_reclamation_extension_present(database)
                if not present:
                    known_names = {
                        str(row[0])
                        for row in database.execute(
                            "SELECT name FROM sqlite_master WHERE name NOT LIKE "
                            "'sqlite_%'"
                        )
                    }
                    if known_names & CONTROL_RECLAMATION_SCHEMA_OBJECT_NAMES:
                        raise AcquisitionControlReclamationIntegrityError(
                            "control reclamation extension is partial"
                        )
                    for _name, statement in _CONTROL_TABLE_DDL:
                        database.execute(statement)
                    for _name, statement in _CONTROL_INDEX_DDL:
                        database.execute(statement)
                    for statement in _control_trigger_definitions().values():
                        database.execute(statement)
                    for statement in _source_trigger_definitions(self.role).values():
                        database.execute(statement)
                    created_at = _finite_timestamp(
                        self._clock(),
                        "control reclamation creation clock",
                    )
                    instance = hashlib.sha256(secrets.token_bytes(32)).hexdigest()
                    database.execute(
                        "INSERT INTO source_reclamation_meta VALUES(1,?,?,?,?,?,?,?,1,NULL,?)",
                        (
                            CONTROL_RECLAMATION_SCHEMA_VERSION,
                            CONTROL_RECLAMATION_CONTRACT,
                            self.role,
                            instance,
                            self._base_store_binding,
                            self._candidate.authority_sha256,
                            self._committed.authority_sha256,
                            created_at,
                        ),
                    )
                    self._fault("initialize.before_commit")
                    database.commit()
                    self._fault("initialize.after_commit")
                else:
                    row = validate_control_reclamation_extension_schema(
                        database,
                        expected_role=self.role,
                        expected_base_store_binding_sha256=(
                            self._base_store_binding
                        ),
                        expected_candidate_authority_sha256=(
                            self._candidate.authority_sha256
                        ),
                        expected_coordinator_authority_sha256=(
                            self._committed.authority_sha256
                        ),
                    )
                    database.commit()
                    instance = str(row["store_instance_sha256"])
            except BaseException:
                if database.in_transaction:
                    database.rollback()
                raise
        after = _path_witness(self.path)
        if before != after:
            raise AcquisitionControlReclamationIntegrityError(
                "control source-store path changed during extension setup"
            )
        self._database_witness = after
        self._store_instance_sha256 = instance
        self._durability_barrier()

    def _durability_barrier(self) -> None:
        self._verify_path()
        for item in (
            self.path,
            Path(str(self.path) + "-wal"),
            Path(str(self.path) + "-journal"),
        ):
            if not item.exists():
                continue
            metadata = item.lstat()
            if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
                raise AcquisitionControlReclamationIntegrityError(
                    "control source-store durability path is unsafe"
                )
            descriptor = os.open(
                item,
                os.O_RDWR if os.name == "nt" else os.O_RDONLY,
            )
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        self._verify_path()

    def _validated_meta(self, database: sqlite3.Connection) -> sqlite3.Row:
        self._validate_base_schema(database)
        current_binding = self._base_store_binding_sha256()
        if not hmac.compare_digest(current_binding, self._base_store_binding):
            raise AcquisitionControlReclamationIntegrityError(
                "control base-store binding changed"
            )
        row = validate_control_reclamation_extension_schema(
            database,
            expected_role=self.role,
            expected_base_store_binding_sha256=self._base_store_binding,
            expected_candidate_authority_sha256=self.candidate_authority_sha256,
            expected_coordinator_authority_sha256=(
                self.coordinator_authority_sha256
            ),
        )
        if self._store_instance_sha256 is not None and not hmac.compare_digest(
            str(row["store_instance_sha256"]),
            self._store_instance_sha256,
        ):
            raise AcquisitionControlReclamationIntegrityError(
                "control source-store instance changed"
            )
        return row

    @property
    def candidate_authority_sha256(self) -> str:
        current = self._candidate.authority_sha256
        return require_sha256(current, "candidate authority SHA-256")

    @property
    def coordinator_authority_sha256(self) -> str:
        current = self._committed.authority_sha256
        return require_sha256(current, "coordinator authority SHA-256")

    @property
    def store_instance_sha256(self) -> str:
        instance = self._store_instance_sha256
        if instance is None:
            raise AcquisitionControlReclamationIntegrityError(
                "control source-store instance is unavailable"
            )
        with self._lock, closing(self._connect()) as database:
            row = self._validated_meta(database)
        current = str(row["store_instance_sha256"])
        if not hmac.compare_digest(current, instance):
            raise AcquisitionControlReclamationIntegrityError(
                "control source-store instance changed"
            )
        return instance

    @property
    def source_store_identity_sha256(self) -> str:
        return self._identity_sha256(self.store_instance_sha256)

    def _identity_sha256(self, store_instance_sha256: str) -> str:
        return digest_mapping({
            "base_store_binding_sha256": self._base_store_binding,
            "candidate_authority_sha256": self.candidate_authority_sha256,
            "contract": CONTROL_RECLAMATION_CONTRACT,
            "coordinator_authority_sha256": self.coordinator_authority_sha256,
            "path_sha256": _normalised_path_sha256(self.path),
            "role": self.role,
            "schema": CONTROL_RECLAMATION_IDENTITY_SCHEMA,
            "schema_version": CONTROL_RECLAMATION_SCHEMA_VERSION,
            "store_instance_sha256": require_sha256(
                store_instance_sha256,
                "control store instance SHA-256",
            ),
        })

    @staticmethod
    def _receipt_from_blob(
        blob: object,
        receipt_type: type[
            SourceLocalPrepareReceiptV2
            | SourceLocalApplyReceiptV1
            | SourceLocalAbortReceiptV1
        ],
        *,
        label: str,
    ) -> SourceLocalPrepareReceiptV2 | SourceLocalApplyReceiptV1 | SourceLocalAbortReceiptV1:
        value = _decode_mapping(blob, label=label)
        fields = set(receipt_type.__dataclass_fields__)
        if set(value) != fields | {"protocol_contract", "schema"}:
            raise AcquisitionControlReclamationIntegrityError(
                f"{label} shape differs"
            )
        return receipt_type(**{key: value[key] for key in fields})

    @classmethod
    def _prepare_from_blob(cls, blob: object) -> SourceLocalPrepareReceiptV2:
        value = cls._receipt_from_blob(
            blob,
            SourceLocalPrepareReceiptV2,
            label="control prepare receipt",
        )
        assert isinstance(value, SourceLocalPrepareReceiptV2)
        return value

    @classmethod
    def _apply_from_blob(cls, blob: object) -> SourceLocalApplyReceiptV1:
        value = cls._receipt_from_blob(
            blob,
            SourceLocalApplyReceiptV1,
            label="control apply receipt",
        )
        assert isinstance(value, SourceLocalApplyReceiptV1)
        return value

    @classmethod
    def _abort_from_blob(cls, blob: object) -> SourceLocalAbortReceiptV1:
        value = cls._receipt_from_blob(
            blob,
            SourceLocalAbortReceiptV1,
            label="control abort receipt",
        )
        assert isinstance(value, SourceLocalAbortReceiptV1)
        return value

    @staticmethod
    def _request_from_blob(blob: object) -> SourceReclamationRequestV2:
        value = _decode_mapping(blob, label="control reclamation request")
        fields = set(SourceReclamationRequestV2.__dataclass_fields__)
        if set(value) != fields | {"protocol_contract", "schema"}:
            raise AcquisitionControlReclamationIntegrityError(
                "control reclamation request shape differs"
            )
        return SourceReclamationRequestV2(**{key: value[key] for key in fields})

    def _advance_clock(
        self,
        database: sqlite3.Connection,
        meta: sqlite3.Row,
        *,
        label: str,
    ) -> tuple[float, int]:
        observed = _finite_timestamp(self._clock(), label)
        previous = meta["last_observed_at"]
        if previous is not None and observed < float(previous):
            raise AcquisitionControlReclamationConflict(
                "control reclamation clock rollback"
            )
        next_epoch = int(meta["next_fence_epoch"]) + 1
        database.execute(
            "UPDATE source_reclamation_meta SET next_fence_epoch=?,"
            "last_observed_at=? WHERE singleton=1",
            (next_epoch, observed),
        )
        return observed, int(meta["next_fence_epoch"])

    @staticmethod
    def _validate_snapshot_members(
        *,
        role: str,
        inventory: HeldControlCandidateInventory,
        members: tuple[dict[str, Any], ...],
    ) -> tuple[str, str]:
        expected_keys = {
            "binding",
            "candidate_sha256",
            "primary_identity",
            "secondary_identity",
            "source_record",
            "source_record_sha256",
            "view_sha256",
        }
        if (
            not isinstance(members, tuple)
            or len(members) != len(inventory.candidates)
            or any(set(item) != expected_keys for item in members)
            or tuple(item["candidate_sha256"] for item in members)
            != tuple(
                item.binding.candidate_sha256 for item in inventory.candidates
            )
        ):
            raise AcquisitionControlReclamationIntegrityError(
                "control source snapshot shape differs"
            )
        candidate_views: list[tuple[str, str]] = []
        child_rows: list[dict[str, Any]] = []
        for held, member in zip(inventory.candidates, members, strict=True):
            if (
                member["binding"] != held.binding.as_dict()
                or not isinstance(member["primary_identity"], str)
                or not member["primary_identity"]
                or not isinstance(member["secondary_identity"], str)
                or not member["secondary_identity"]
                or not isinstance(member["source_record"], dict)
                or member["source_record_sha256"]
                != digest_mapping(member["source_record"])
                or member["view_sha256"]
                != candidate_participant_view_sha256(held.record, role)
            ):
                raise AcquisitionControlReclamationConflict(
                    "control current source differs from sealed participant view"
                )
            _canonical_mapping(member["source_record"])
            candidate_views.append((
                held.binding.candidate_sha256,
                str(member["view_sha256"]),
            ))
            child_rows.append({
                "candidate_sha256": held.binding.candidate_sha256,
                "primary_identity": member["primary_identity"],
                "secondary_identity": member["secondary_identity"],
                "source_record_sha256": member["source_record_sha256"],
            })
        if (
            len({item["primary_identity"] for item in members}) != len(members)
            or len({item["secondary_identity"] for item in members})
            != len(members)
        ):
            raise AcquisitionControlReclamationConflict(
                "control source candidate identities repeat"
            )
        return (
            participant_inventory_view_sha256(
                role=role,
                candidate_views=tuple(candidate_views),
            ),
            _ordered_chain_root(
                schema="stv3-worker-control-source-child-reference-v1",
                rows=child_rows,
            ),
        )

    def prepare_source_local_v2(
        self,
        request: SourceReclamationRequestV2,
    ) -> SourceLocalPrepareReceiptV2:
        if not isinstance(request, SourceReclamationRequestV2):
            raise TypeError("request must be SourceReclamationRequestV2")
        if request.candidate_authority_sha256 != self.candidate_authority_sha256:
            raise AcquisitionControlReclamationConflict(
                "control request candidate authority differs"
            )
        store_identity = self.source_store_identity_sha256
        with hold_control_candidate_inventory(
            self._candidate,
            request,
        ) as inventory:
            with self._lock, closing(self._connect()) as database:
                try:
                    database.execute("BEGIN IMMEDIATE")
                    meta = self._validated_meta(database)
                    existing = database.execute(
                        "SELECT * FROM source_reclamation_decisions "
                        "WHERE request_sha256=?",
                        (request.request_sha256,),
                    ).fetchone()
                    if existing is not None:
                        receipt = self._prepare_from_blob(
                            existing["prepare_receipt_json"]
                        )
                        persisted = self._load_members(
                            database,
                            request.request_sha256,
                        )
                        source_view, child_root = self._stored_member_roots(
                            persisted
                        )
                        if (
                            len(persisted) != len(inventory.candidates)
                            or tuple(
                                item["candidate_sha256"] for item in persisted
                            ) != tuple(
                                item.binding.candidate_sha256
                                for item in inventory.candidates
                            )
                            or any(
                                stored["binding"] != held.binding.as_dict()
                                or stored["view_sha256"]
                                != candidate_participant_view_sha256(
                                    held.record,
                                    self.role,
                                )
                                for stored, held in zip(
                                    persisted,
                                    inventory.candidates,
                                    strict=True,
                                )
                            )
                        ):
                            raise AcquisitionControlReclamationConflict(
                                "control persisted candidates differ from authority"
                            )
                        state = str(existing["state"])
                        current_identity = self._identity_sha256(
                            str(meta["store_instance_sha256"])
                        )
                        self._validate_request_and_prepare(
                            request=request,
                            prepare=receipt,
                            members=persisted,
                            current_source_store_identity_sha256=(
                                current_identity
                            ),
                        )
                        self._validate_prepare_row(
                            row=existing,
                            request=request,
                            prepare=receipt,
                        )
                        if state == "PREPARED":
                            releasable_bytes, releasable_entries = (
                                self._reproof_stored_members(
                                    database,
                                    members=persisted,
                                )
                            )
                        elif state == "APPLYING":
                            raise AcquisitionControlReclamationIntegrityError(
                                "control source is stranded in APPLYING"
                            )
                        elif state == "APPLIED":
                            self._validate_applied_state(
                                database,
                                row=existing,
                                request=request,
                                prepare=receipt,
                                members=persisted,
                                current_source_store_identity_sha256=(
                                    current_identity
                                ),
                            )
                            releasable_bytes = int(
                                existing["releasable_bytes"]
                            )
                            releasable_entries = int(
                                existing["releasable_entries"]
                            )
                        elif state == "ABORTED":
                            self._validate_aborted_state(
                                database,
                                row=existing,
                                request=request,
                                prepare=receipt,
                                members=persisted,
                                current_source_store_identity_sha256=(
                                    current_identity
                                ),
                            )
                            releasable_bytes = int(
                                existing["releasable_bytes"]
                            )
                            releasable_entries = int(
                                existing["releasable_entries"]
                            )
                        else:  # pragma: no cover - closed SQL CHECK
                            raise AcquisitionControlReclamationIntegrityError(
                                "control prepare state differs"
                            )
                        if (
                            bytes(existing["request_json"])
                            != canonical_bytes(request.as_dict())
                            or receipt.source_store_identity_sha256
                            != store_identity
                            or receipt.source_view_sha256 != source_view
                            or receipt.child_reference_root_sha256 != child_root
                            or receipt.releasable_bytes != releasable_bytes
                            or receipt.releasable_entries != releasable_entries
                        ):
                            raise AcquisitionControlReclamationConflict(
                                "control prepare identity was reused"
                            )
                        database.commit()
                        return receipt
                    members, releasable_bytes, releasable_entries = (
                        self._source_snapshot(database, inventory)
                    )
                    source_view, child_root = self._validate_snapshot_members(
                        role=self.role,
                        inventory=inventory,
                        members=members,
                    )
                    active = database.execute(
                        "SELECT request_sha256 FROM source_reclamation_decisions "
                        "WHERE active_slot=1"
                    ).fetchone()
                    if active is not None:
                        raise AcquisitionControlReclamationConflict(
                            "control source reclamation fence is held"
                        )
                    decision_count = int(database.execute(
                        "SELECT count(*) FROM source_reclamation_decisions"
                    ).fetchone()[0])
                    member_count = int(database.execute(
                        "SELECT count(*) FROM source_reclamation_members"
                    ).fetchone()[0])
                    if (
                        decision_count >= MAX_CONTROL_RECLAMATION_DECISIONS
                        or member_count + len(members)
                        > MAX_CONTROL_RECLAMATION_MEMBERS
                    ):
                        raise AcquisitionControlReclamationCapacityError(
                            "control reclamation retention bound is exhausted"
                        )
                    observed, fence_epoch = self._advance_clock(
                        database,
                        meta,
                        label="control reclamation prepare clock",
                    )
                    source_fence = digest_mapping({
                        "candidate_bindings_sha256": (
                            request.candidate_bindings_sha256
                        ),
                        "child_reference_root_sha256": child_root,
                        "fence_epoch": fence_epoch,
                        "request_sha256": request.request_sha256,
                        "schema": CONTROL_RECLAMATION_FENCE_SCHEMA,
                        "source_store_identity_sha256": store_identity,
                        "source_view_sha256": source_view,
                    })
                    receipt = SourceLocalPrepareReceiptV2.create(
                        request_sha256=request.request_sha256,
                        role=self.role,
                        source_store_identity_sha256=store_identity,
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
                        source_view_sha256=source_view,
                        child_reference_root_sha256=child_root,
                        source_fence_sha256=source_fence,
                        candidate_count=len(members),
                        releasable_bytes=releasable_bytes,
                        releasable_entries=releasable_entries,
                        prepared_at=observed,
                    )
                    database.execute(
                        "INSERT INTO source_reclamation_decisions("
                        "request_sha256,request_json,state,active_slot,"
                        "prepare_receipt_json,source_view_sha256,"
                        "child_reference_root_sha256,source_fence_sha256,"
                        "candidate_count,releasable_bytes,releasable_entries,"
                        "prepared_at) VALUES(?,?,'PREPARED',1,?,?,?,?,?,?,?,?)",
                        (
                            request.request_sha256,
                            sqlite3.Binary(canonical_bytes(request.as_dict())),
                            sqlite3.Binary(canonical_bytes(receipt.as_dict())),
                            source_view,
                            child_root,
                            source_fence,
                            len(members),
                            releasable_bytes,
                            releasable_entries,
                            observed,
                        ),
                    )
                    for member in members:
                        member_core = {
                            "binding": member["binding"],
                            "candidate_sha256": member["candidate_sha256"],
                            "primary_identity": member["primary_identity"],
                            "request_sha256": request.request_sha256,
                            "schema": CONTROL_RECLAMATION_MEMBER_SCHEMA,
                            "secondary_identity": member["secondary_identity"],
                            "source_record_sha256": (
                                member["source_record_sha256"]
                            ),
                            "view_sha256": member["view_sha256"],
                        }
                        database.execute(
                            "INSERT INTO source_reclamation_members VALUES(?,?,?,?,?,?,?,?)",
                            (
                                request.request_sha256,
                                member["candidate_sha256"],
                                member["primary_identity"],
                                member["secondary_identity"],
                                sqlite3.Binary(canonical_bytes(member["binding"])),
                                sqlite3.Binary(
                                    canonical_bytes(member["source_record"])
                                ),
                                member["view_sha256"],
                                digest_mapping(member_core),
                            ),
                        )
                    self._fault("prepare.before_commit")
                    database.commit()
                    self._fault("prepare.after_commit")
                except BaseException:
                    if database.in_transaction:
                        database.rollback()
                    raise
        self._durability_barrier()
        return receipt

    def _load_members(
        self,
        database: sqlite3.Connection,
        request_sha256: str,
    ) -> tuple[dict[str, Any], ...]:
        rows = database.execute(
            "SELECT * FROM source_reclamation_members WHERE request_sha256=? "
            "ORDER BY candidate_sha256",
            (request_sha256,),
        ).fetchall()
        members: list[dict[str, Any]] = []
        for row in rows:
            binding_value = _decode_mapping(
                row["binding_json"],
                label="control candidate binding",
            )
            binding_fields = set(
                SourceReclamationCandidateBindingV1.__dataclass_fields__
            )
            if set(binding_value) != binding_fields | {"schema"}:
                raise AcquisitionControlReclamationIntegrityError(
                    "control candidate binding shape differs"
                )
            binding = SourceReclamationCandidateBindingV1(**{
                key: binding_value[key] for key in binding_fields
            })
            source_record = _decode_mapping(
                row["source_record_json"],
                label="control source record",
            )
            view_sha = require_sha256(
                str(row["view_sha256"]),
                "control member view SHA-256",
            )
            source_sha = digest_mapping(source_record)
            member_core = {
                "binding": binding.as_dict(),
                "candidate_sha256": binding.candidate_sha256,
                "primary_identity": str(row["primary_identity"]),
                "request_sha256": request_sha256,
                "schema": CONTROL_RECLAMATION_MEMBER_SCHEMA,
                "secondary_identity": str(row["secondary_identity"]),
                "source_record_sha256": source_sha,
                "view_sha256": view_sha,
            }
            if (
                str(row["candidate_sha256"]) != binding.candidate_sha256
                or str(row["member_sha256"]) != digest_mapping(member_core)
            ):
                raise AcquisitionControlReclamationIntegrityError(
                    "control source member checksum differs"
                )
            members.append({
                "binding": binding.as_dict(),
                "candidate_sha256": binding.candidate_sha256,
                "primary_identity": str(row["primary_identity"]),
                "secondary_identity": str(row["secondary_identity"]),
                "source_record": source_record,
                "source_record_sha256": source_sha,
                "view_sha256": view_sha,
            })
        return tuple(members)

    def _stored_member_roots(
        self,
        members: tuple[dict[str, Any], ...],
    ) -> tuple[str, str]:
        if not members:
            raise AcquisitionControlReclamationIntegrityError(
                "control source member inventory is empty"
            )
        source_view = participant_inventory_view_sha256(
            role=self.role,
            candidate_views=tuple(
                (item["candidate_sha256"], item["view_sha256"])
                for item in members
            ),
        )
        child_root = _ordered_chain_root(
            schema="stv3-worker-control-source-child-reference-v1",
            rows=[{
                "candidate_sha256": item["candidate_sha256"],
                "primary_identity": item["primary_identity"],
                "secondary_identity": item["secondary_identity"],
                "source_record_sha256": item["source_record_sha256"],
            } for item in members],
        )
        return source_view, child_root

    def _load_decision_row(
        self,
        database: sqlite3.Connection,
        request_sha256: str,
    ) -> sqlite3.Row | None:
        return database.execute(
            "SELECT * FROM source_reclamation_decisions WHERE request_sha256=?",
            (require_sha256(request_sha256, "control request SHA-256"),),
        ).fetchone()

    def _validate_request_and_prepare(
        self,
        *,
        request: SourceReclamationRequestV2,
        prepare: SourceLocalPrepareReceiptV2,
        members: tuple[dict[str, Any], ...],
        current_source_store_identity_sha256: str,
    ) -> None:
        source_view, child_root = self._stored_member_roots(members)
        if (
            prepare.request_sha256 != request.request_sha256
            or prepare.role != self.role
            or prepare.source_store_identity_sha256
            != require_sha256(
                current_source_store_identity_sha256,
                "current control source-store identity SHA-256",
            )
            or prepare.coordinator_authority_sha256
            != self.coordinator_authority_sha256
            or prepare.candidate_authority_sha256
            != self.candidate_authority_sha256
            or prepare.sealed_archive_anchor_sha256
            != request.sealed_archive_anchor_sha256
            or prepare.archive_binding_root_sha256
            != request.archive_binding_root_sha256
            or prepare.candidate_bindings_sha256
            != request.candidate_bindings_sha256
            or prepare.consumer_registry_root_sha256
            != request.consumer_registry_root_sha256
            or prepare.consumer_epoch_root_sha256
            != request.consumer_epoch_root_sha256
            or prepare.candidate_count != request.candidate_count
            or prepare.candidate_count != len(members)
            or prepare.source_view_sha256 != source_view
            or prepare.child_reference_root_sha256 != child_root
        ):
            raise AcquisitionControlReclamationIntegrityError(
                "control request or prepare binding differs"
            )

    def _validate_committed_decision(
        self,
        *,
        decision: CommittedReclamationDecisionReceiptV1,
        request: SourceReclamationRequestV2,
        prepare: SourceLocalPrepareReceiptV2,
        predecessor_apply_receipt_sha256: str | None,
    ) -> None:
        if not isinstance(decision, CommittedReclamationDecisionReceiptV1):
            raise AcquisitionControlReclamationIntegrityError(
                "committed decision authority returned the wrong type"
            )
        binding = decision.participant_prepares[
            PARTICIPANT_APPLY_ORDER.index(self.role)
        ]
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
            or binding.role != self.role
            or binding.prepare_receipt_sha256
            != prepare.prepare_receipt_sha256
            or binding.source_store_identity_sha256
            != prepare.source_store_identity_sha256
            or binding.source_fence_sha256 != prepare.source_fence_sha256
        ):
            raise AcquisitionControlReclamationConflict(
                "committed decision differs from the local prepare"
            )
        role_index = PARTICIPANT_APPLY_ORDER.index(self.role)
        if role_index == 0:
            if predecessor_apply_receipt_sha256 is not None:
                raise AcquisitionControlReclamationConflict(
                    "first participant unexpectedly has a predecessor"
                )
        else:
            require_sha256(
                predecessor_apply_receipt_sha256,
                "control predecessor apply receipt SHA-256",
            )

    def _validate_prepare_row(
        self,
        *,
        row: sqlite3.Row,
        request: SourceReclamationRequestV2,
        prepare: SourceLocalPrepareReceiptV2,
    ) -> None:
        if (
            str(row["request_sha256"]) != request.request_sha256
            or bytes(row["request_json"])
            != canonical_bytes(request.as_dict())
            or bytes(row["prepare_receipt_json"])
            != canonical_bytes(prepare.as_dict())
            or str(row["source_view_sha256"])
            != prepare.source_view_sha256
            or str(row["child_reference_root_sha256"])
            != prepare.child_reference_root_sha256
            or str(row["source_fence_sha256"])
            != prepare.source_fence_sha256
            or int(row["candidate_count"]) != prepare.candidate_count
            or int(row["releasable_bytes"])
            != prepare.releasable_bytes
            or int(row["releasable_entries"])
            != prepare.releasable_entries
            or float(row["prepared_at"]) != prepare.prepared_at
        ):
            raise AcquisitionControlReclamationIntegrityError(
                "control durable prepare row differs"
            )

    @staticmethod
    def _effect_mapping(
        *,
        role: str,
        request_sha256: str,
        source_store_identity_sha256: str,
        committed_receipt_sha256: str,
        predecessor_apply_receipt_sha256: str | None,
        members: tuple[dict[str, Any], ...],
        released_bytes: int,
        released_entries: int,
    ) -> dict[str, Any]:
        return {
            "committed_decision_receipt_sha256": committed_receipt_sha256,
            "members": [{
                "candidate_sha256": item["candidate_sha256"],
                "primary_identity": item["primary_identity"],
                "secondary_identity": item["secondary_identity"],
                "source_record_sha256": item["source_record_sha256"],
            } for item in members],
            "predecessor_apply_receipt_sha256": (
                predecessor_apply_receipt_sha256
            ),
            "released_bytes": released_bytes,
            "released_entries": released_entries,
            "request_sha256": request_sha256,
            "role": role,
            "schema": CONTROL_RECLAMATION_EFFECT_SCHEMA,
            "source_store_identity_sha256": source_store_identity_sha256,
        }

    def _validate_applied_state(
        self,
        database: sqlite3.Connection,
        *,
        row: sqlite3.Row,
        request: SourceReclamationRequestV2,
        prepare: SourceLocalPrepareReceiptV2,
        members: tuple[dict[str, Any], ...],
        current_source_store_identity_sha256: str,
    ) -> SourceLocalApplyReceiptV1:
        self._validate_request_and_prepare(
            request=request,
            prepare=prepare,
            members=members,
            current_source_store_identity_sha256=(
                current_source_store_identity_sha256
            ),
        )
        self._validate_prepare_row(
            row=row,
            request=request,
            prepare=prepare,
        )
        if str(row["state"]) != "APPLIED" or row["apply_receipt_json"] is None:
            raise AcquisitionControlReclamationIntegrityError(
                "control applied state is incomplete"
            )
        receipt = self._apply_from_blob(row["apply_receipt_json"])
        committed_decision = require_sha256(
            str(row["committed_decision_sha256"]),
            "control committed decision SHA-256",
        )
        committed_receipt = require_sha256(
            str(row["committed_receipt_sha256"]),
            "control committed receipt SHA-256",
        )
        predecessor = require_sha256(
            str(row["predecessor_apply_receipt_sha256"]),
            "control predecessor apply receipt SHA-256",
        )
        effect_sha = require_sha256(
            str(row["effect_sha256"]),
            "control effect SHA-256",
        )
        post_apply_sha = require_sha256(
            str(row["post_apply_view_sha256"]),
            "control post-apply view SHA-256",
        )
        expected_effect = digest_mapping(self._effect_mapping(
            role=self.role,
            request_sha256=request.request_sha256,
            source_store_identity_sha256=(
                prepare.source_store_identity_sha256
            ),
            committed_receipt_sha256=committed_receipt,
            predecessor_apply_receipt_sha256=predecessor,
            members=members,
            released_bytes=prepare.releasable_bytes,
            released_entries=prepare.releasable_entries,
        ))
        applied_at = _finite_timestamp(
            row["applied_at"],
            "control applied clock",
        )
        if (
            receipt.request_sha256 != request.request_sha256
            or receipt.role != self.role
            or receipt.source_store_identity_sha256
            != prepare.source_store_identity_sha256
            or receipt.prepare_receipt_sha256
            != prepare.prepare_receipt_sha256
            or receipt.committed_decision_sha256 != committed_decision
            or receipt.committed_decision_receipt_sha256 != committed_receipt
            or receipt.predecessor_apply_receipt_sha256 != predecessor
            or receipt.effect_sha256 != effect_sha
            or effect_sha != expected_effect
            or receipt.post_apply_view_sha256 != post_apply_sha
            or receipt.released_bytes != prepare.releasable_bytes
            or receipt.released_entries != prepare.releasable_entries
            or receipt.applied_at != applied_at
        ):
            raise AcquisitionControlReclamationIntegrityError(
                "control applied receipt or effect differs"
            )
        tombstones = database.execute(
            "SELECT * FROM source_reclamation_tombstones "
            "WHERE request_sha256=? ORDER BY candidate_sha256",
            (request.request_sha256,),
        ).fetchall()
        if len(tombstones) != len(members):
            raise AcquisitionControlReclamationIntegrityError(
                "control applied tombstone inventory differs"
            )
        for tombstone, member in zip(tombstones, members, strict=True):
            if (
                str(tombstone["candidate_sha256"])
                != member["candidate_sha256"]
                or str(tombstone["request_sha256"])
                != request.request_sha256
                or str(tombstone["primary_identity"])
                != member["primary_identity"]
                or str(tombstone["secondary_identity"])
                != member["secondary_identity"]
                or str(tombstone["source_record_sha256"])
                != member["source_record_sha256"]
                or str(tombstone["effect_sha256"]) != effect_sha
                or _finite_timestamp(
                    tombstone["created_at"],
                    "control tombstone clock",
                ) != applied_at
            ):
                raise AcquisitionControlReclamationIntegrityError(
                    "control applied tombstone differs"
                )
        # This re-proves source absence and each store's live invariants.  The
        # returned projection is intentionally not rehashed: capacity counters
        # may legitimately evolve after this request committed.
        self._post_apply_projection(
            database,
            request_sha256=request.request_sha256,
            members=members,
        )
        return receipt

    def _validate_aborted_state(
        self,
        database: sqlite3.Connection,
        *,
        row: sqlite3.Row,
        request: SourceReclamationRequestV2,
        prepare: SourceLocalPrepareReceiptV2,
        members: tuple[dict[str, Any], ...],
        current_source_store_identity_sha256: str,
    ) -> SourceLocalAbortReceiptV1:
        self._validate_request_and_prepare(
            request=request,
            prepare=prepare,
            members=members,
            current_source_store_identity_sha256=(
                current_source_store_identity_sha256
            ),
        )
        self._validate_prepare_row(
            row=row,
            request=request,
            prepare=prepare,
        )
        if str(row["state"]) != "ABORTED" or row["abort_receipt_json"] is None:
            raise AcquisitionControlReclamationIntegrityError(
                "control aborted state is incomplete"
            )
        receipt = self._abort_from_blob(row["abort_receipt_json"])
        aborted_at = _finite_timestamp(
            row["aborted_at"],
            "control aborted clock",
        )
        if (
            receipt.request_sha256 != request.request_sha256
            or receipt.role != self.role
            or receipt.source_store_identity_sha256
            != prepare.source_store_identity_sha256
            or receipt.prepare_receipt_sha256
            != prepare.prepare_receipt_sha256
            or receipt.coordinator_abort_decision_sha256
            != require_sha256(
                str(row["abort_decision_sha256"]),
                "control abort decision SHA-256",
            )
            or receipt.fence_release_root_sha256
            != require_sha256(
                str(row["fence_release_root_sha256"]),
                "control fence release root SHA-256",
            )
            or receipt.aborted_at != aborted_at
        ):
            raise AcquisitionControlReclamationIntegrityError(
                "control aborted receipt differs"
            )
        if database.execute(
            "SELECT 1 FROM source_reclamation_tombstones "
            "WHERE request_sha256=? LIMIT 1",
            (request.request_sha256,),
        ).fetchone() is not None:
            raise AcquisitionControlReclamationIntegrityError(
                "control aborted request unexpectedly has a tombstone"
            )
        # Source rows are proved inside the abort transaction before its fence
        # is released.  They may later be reclaimed by a distinct request, so
        # an old ABORTED receipt must not require those rows to remain forever.
        return receipt

    def load_source_local_prepare_v2(
        self,
        request_sha256: str,
    ) -> SourceLocalPrepareReceiptV2 | None:
        request = require_sha256(
            request_sha256,
            "control prepare request SHA-256",
        )
        with self._lock, closing(self._connect()) as database:
            meta = self._validated_meta(database)
            row = self._load_decision_row(database, request)
            if row is None:
                return None
            receipt = self._prepare_from_blob(row["prepare_receipt_json"])
            members = self._load_members(database, request)
            stored_request = self._request_from_blob(row["request_json"])
            current_identity = self._identity_sha256(
                str(meta["store_instance_sha256"])
            )
            self._validate_request_and_prepare(
                request=stored_request,
                prepare=receipt,
                members=members,
                current_source_store_identity_sha256=current_identity,
            )
            self._validate_prepare_row(
                row=row,
                request=stored_request,
                prepare=receipt,
            )
            state = str(row["state"])
            if state == "PREPARED":
                current_bytes, current_entries = self._reproof_stored_members(
                    database,
                    members=members,
                )
                if (
                    current_bytes != receipt.releasable_bytes
                    or current_entries != receipt.releasable_entries
                ):
                    raise AcquisitionControlReclamationIntegrityError(
                        "control prepared source extent differs"
                    )
            elif state == "APPLIED":
                self._validate_applied_state(
                    database,
                    row=row,
                    request=stored_request,
                    prepare=receipt,
                    members=members,
                    current_source_store_identity_sha256=current_identity,
                )
            elif state == "ABORTED":
                self._validate_aborted_state(
                    database,
                    row=row,
                    request=stored_request,
                    prepare=receipt,
                    members=members,
                    current_source_store_identity_sha256=current_identity,
                )
            else:
                raise AcquisitionControlReclamationIntegrityError(
                    "control source is stranded in APPLYING"
                )
            return receipt

    def load_source_local_apply_v1(
        self,
        request_sha256: str,
    ) -> SourceLocalApplyReceiptV1 | None:
        request = require_sha256(
            request_sha256,
            "control apply request SHA-256",
        )
        with self._lock, closing(self._connect()) as database:
            meta = self._validated_meta(database)
            row = self._load_decision_row(database, request)
            if row is None or row["apply_receipt_json"] is None:
                return None
            stored_request = self._request_from_blob(row["request_json"])
            prepare = self._prepare_from_blob(row["prepare_receipt_json"])
            members = self._load_members(database, request)
            receipt = self._validate_applied_state(
                database,
                row=row,
                request=stored_request,
                prepare=prepare,
                members=members,
                current_source_store_identity_sha256=self._identity_sha256(
                    str(meta["store_instance_sha256"])
                ),
            )
            return receipt

    def load_source_local_abort_v1(
        self,
        request_sha256: str,
    ) -> SourceLocalAbortReceiptV1 | None:
        request = require_sha256(
            request_sha256,
            "control abort request SHA-256",
        )
        with self._lock, closing(self._connect()) as database:
            meta = self._validated_meta(database)
            row = self._load_decision_row(database, request)
            if row is None or row["abort_receipt_json"] is None:
                return None
            stored_request = self._request_from_blob(row["request_json"])
            prepare = self._prepare_from_blob(row["prepare_receipt_json"])
            members = self._load_members(database, request)
            receipt = self._validate_aborted_state(
                database,
                row=row,
                request=stored_request,
                prepare=prepare,
                members=members,
                current_source_store_identity_sha256=self._identity_sha256(
                    str(meta["store_instance_sha256"])
                ),
            )
            return receipt

    def apply_source_local_v1(
        self,
        request_sha256: str,
        *,
        predecessor_apply_receipt_sha256: str | None,
    ) -> SourceLocalApplyReceiptV1:
        request_id = require_sha256(
            request_sha256,
            "control apply request SHA-256",
        )
        role_index = PARTICIPANT_APPLY_ORDER.index(self.role)
        if role_index == 0:
            if predecessor_apply_receipt_sha256 is not None:
                raise AcquisitionControlReclamationConflict(
                    "first control participant unexpectedly has a predecessor"
                )
        else:
            require_sha256(
                predecessor_apply_receipt_sha256,
                "control predecessor apply receipt SHA-256",
            )
        prepare = self.load_source_local_prepare_v2(request_id)
        if prepare is None:
            raise AcquisitionControlReclamationConflict(
                "control source has no durable prepare"
            )
        with self._committed.hold_committed_decision_v1(
            request_id,
            applying_role=self.role,
            expected_predecessor_apply_receipt_sha256=(
                predecessor_apply_receipt_sha256
            ),
        ) as decision:
            result: SourceLocalApplyReceiptV1
            with self._lock, closing(self._connect()) as database:
                try:
                    database.execute("BEGIN IMMEDIATE")
                    meta = self._validated_meta(database)
                    row = self._load_decision_row(database, request_id)
                    if row is None:
                        raise AcquisitionControlReclamationConflict(
                            "control prepare disappeared before apply"
                        )
                    request = self._request_from_blob(row["request_json"])
                    stored_prepare = self._prepare_from_blob(
                        row["prepare_receipt_json"]
                    )
                    members = self._load_members(database, request_id)
                    self._validate_request_and_prepare(
                        request=request,
                        prepare=stored_prepare,
                        members=members,
                        current_source_store_identity_sha256=(
                            self._identity_sha256(
                                str(meta["store_instance_sha256"])
                            )
                        ),
                    )
                    self._validate_prepare_row(
                        row=row,
                        request=request,
                        prepare=stored_prepare,
                    )
                    if stored_prepare != prepare:
                        raise AcquisitionControlReclamationConflict(
                            "control prepare changed before apply"
                        )
                    self._validate_committed_decision(
                        decision=decision,
                        request=request,
                        prepare=stored_prepare,
                        predecessor_apply_receipt_sha256=(
                            predecessor_apply_receipt_sha256
                        ),
                    )
                    state = str(row["state"])
                    if state == "APPLIED":
                        result = self._validate_applied_state(
                            database,
                            row=row,
                            request=request,
                            prepare=stored_prepare,
                            members=members,
                            current_source_store_identity_sha256=(
                                self._identity_sha256(
                                    str(meta["store_instance_sha256"])
                                )
                            ),
                        )
                        if (
                            result.committed_decision_sha256
                            != decision.decision_sha256
                            or result.committed_decision_receipt_sha256
                            != decision.receipt_sha256
                            or result.predecessor_apply_receipt_sha256
                            != predecessor_apply_receipt_sha256
                            or result.effect_sha256 != row["effect_sha256"]
                            or result.post_apply_view_sha256
                            != row["post_apply_view_sha256"]
                        ):
                            raise AcquisitionControlReclamationConflict(
                                "control apply replay differs"
                            )
                        database.commit()
                    elif state == "PREPARED":
                        current_bytes, current_entries = (
                            self._reproof_stored_members(
                                database,
                                members=members,
                            )
                        )
                        if (
                            current_bytes != stored_prepare.releasable_bytes
                            or current_entries
                            != stored_prepare.releasable_entries
                        ):
                            raise AcquisitionControlReclamationConflict(
                                "control source release extent changed"
                            )
                        observed, _fence_epoch = self._advance_clock(
                            database,
                            meta,
                            label="control reclamation apply clock",
                        )
                        database.execute(
                            "UPDATE source_reclamation_decisions SET "
                            "state='APPLYING',committed_decision_sha256=?,"
                            "committed_receipt_sha256=?,"
                            "predecessor_apply_receipt_sha256=? "
                            "WHERE request_sha256=? AND state='PREPARED'",
                            (
                                decision.decision_sha256,
                                decision.receipt_sha256,
                                predecessor_apply_receipt_sha256,
                                request_id,
                            ),
                        )
                        self._fault("apply.after_applying")
                        released_bytes, released_entries = (
                            self._apply_source_rows(
                                database,
                                request_sha256=request_id,
                                members=members,
                            )
                        )
                        if (
                            released_bytes != stored_prepare.releasable_bytes
                            or released_entries
                            != stored_prepare.releasable_entries
                        ):
                            raise AcquisitionControlReclamationConflict(
                                "control source applied extent differs"
                            )
                        effect = self._effect_mapping(
                            role=self.role,
                            request_sha256=request_id,
                            source_store_identity_sha256=(
                                stored_prepare.source_store_identity_sha256
                            ),
                            committed_receipt_sha256=decision.receipt_sha256,
                            predecessor_apply_receipt_sha256=(
                                predecessor_apply_receipt_sha256
                            ),
                            members=members,
                            released_bytes=released_bytes,
                            released_entries=released_entries,
                        )
                        effect_sha = digest_mapping(effect)
                        for member in members:
                            database.execute(
                                "INSERT INTO source_reclamation_tombstones "
                                "VALUES(?,?,?,?,?,?,?)",
                                (
                                    member["candidate_sha256"],
                                    request_id,
                                    member["primary_identity"],
                                    member["secondary_identity"],
                                    member["source_record_sha256"],
                                    effect_sha,
                                    observed,
                                ),
                            )
                        post_projection = self._post_apply_projection(
                            database,
                            request_sha256=request_id,
                            members=members,
                        )
                        post_apply = {
                            "projection": post_projection,
                            "request_sha256": request_id,
                            "role": self.role,
                            "schema": CONTROL_RECLAMATION_POST_VIEW_SCHEMA,
                            "source_store_identity_sha256": (
                                stored_prepare.source_store_identity_sha256
                            ),
                        }
                        post_apply_sha = digest_mapping(post_apply)
                        result = SourceLocalApplyReceiptV1.create(
                            request_sha256=request_id,
                            role=self.role,
                            source_store_identity_sha256=(
                                stored_prepare.source_store_identity_sha256
                            ),
                            prepare_receipt_sha256=(
                                stored_prepare.prepare_receipt_sha256
                            ),
                            committed_decision_sha256=(
                                decision.decision_sha256
                            ),
                            committed_decision_receipt_sha256=(
                                decision.receipt_sha256
                            ),
                            predecessor_apply_receipt_sha256=(
                                predecessor_apply_receipt_sha256
                            ),
                            effect_sha256=effect_sha,
                            post_apply_view_sha256=post_apply_sha,
                            released_bytes=released_bytes,
                            released_entries=released_entries,
                            applied_at=observed,
                        )
                        database.execute(
                            "UPDATE source_reclamation_decisions SET "
                            "state='APPLIED',active_slot=NULL,effect_sha256=?,"
                            "post_apply_view_sha256=?,apply_receipt_json=?,"
                            "applied_at=? WHERE request_sha256=? "
                            "AND state='APPLYING'",
                            (
                                effect_sha,
                                post_apply_sha,
                                sqlite3.Binary(canonical_bytes(result.as_dict())),
                                observed,
                                request_id,
                            ),
                        )
                        self._fault("apply.before_commit")
                        database.commit()
                        self._fault("apply.after_commit")
                    elif state == "ABORTED":
                        raise AcquisitionControlReclamationConflict(
                            "aborted control source cannot be applied"
                        )
                    else:
                        raise AcquisitionControlReclamationIntegrityError(
                            "control source is stranded in APPLYING"
                        )
                except BaseException:
                    if database.in_transaction:
                        database.rollback()
                    raise
        self._durability_barrier()
        return result

    def abort_source_local_v1(
        self,
        request_sha256: str,
        *,
        expected_abort_decision_sha256: str,
    ) -> SourceLocalAbortReceiptV1:
        request_id = require_sha256(
            request_sha256,
            "control abort request SHA-256",
        )
        expected_abort = require_sha256(
            expected_abort_decision_sha256,
            "control abort decision SHA-256",
        )
        prepare = self.load_source_local_prepare_v2(request_id)
        if prepare is None:
            raise AcquisitionControlReclamationConflict(
                "control source has no durable prepare"
            )
        with self._committed.hold_abort_decision_v1(
            request_id,
            expected_abort_decision_sha256=expected_abort,
            aborting_role=self.role,
            expected_prepare_receipt_sha256=prepare.prepare_receipt_sha256,
            expected_source_store_identity_sha256=(
                prepare.source_store_identity_sha256
            ),
            expected_source_fence_sha256=prepare.source_fence_sha256,
        ) as decision:
            if (
                not isinstance(decision, CoordinatorAbortDecisionReceiptV1)
                or decision.request_sha256 != request_id
                or decision.abort_decision_sha256 != expected_abort
            ):
                raise AcquisitionControlReclamationConflict(
                    "held abort decision differs"
                )
            result: SourceLocalAbortReceiptV1
            with self._lock, closing(self._connect()) as database:
                try:
                    database.execute("BEGIN IMMEDIATE")
                    meta = self._validated_meta(database)
                    row = self._load_decision_row(database, request_id)
                    if row is None:
                        raise AcquisitionControlReclamationConflict(
                            "control prepare disappeared before abort"
                        )
                    request = self._request_from_blob(row["request_json"])
                    stored_prepare = self._prepare_from_blob(
                        row["prepare_receipt_json"]
                    )
                    members = self._load_members(database, request_id)
                    self._validate_request_and_prepare(
                        request=request,
                        prepare=stored_prepare,
                        members=members,
                        current_source_store_identity_sha256=(
                            self._identity_sha256(
                                str(meta["store_instance_sha256"])
                            )
                        ),
                    )
                    self._validate_prepare_row(
                        row=row,
                        request=request,
                        prepare=stored_prepare,
                    )
                    if stored_prepare != prepare:
                        raise AcquisitionControlReclamationConflict(
                            "control prepare changed before abort"
                        )
                    state = str(row["state"])
                    if state == "ABORTED":
                        result = self._validate_aborted_state(
                            database,
                            row=row,
                            request=request,
                            prepare=stored_prepare,
                            members=members,
                            current_source_store_identity_sha256=(
                                self._identity_sha256(
                                    str(meta["store_instance_sha256"])
                                )
                            ),
                        )
                        if (
                            result.coordinator_abort_decision_sha256
                            != expected_abort
                            or result.prepare_receipt_sha256
                            != stored_prepare.prepare_receipt_sha256
                            or result.source_store_identity_sha256
                            != stored_prepare.source_store_identity_sha256
                        ):
                            raise AcquisitionControlReclamationConflict(
                                "control abort replay differs"
                            )
                        database.commit()
                    elif state == "PREPARED":
                        current_bytes, current_entries = (
                            self._reproof_stored_members(
                                database,
                                members=members,
                            )
                        )
                        if (
                            current_bytes != stored_prepare.releasable_bytes
                            or current_entries
                            != stored_prepare.releasable_entries
                        ):
                            raise AcquisitionControlReclamationConflict(
                                "control source changed before abort"
                            )
                        observed, fence_epoch = self._advance_clock(
                            database,
                            meta,
                            label="control reclamation abort clock",
                        )
                        release_root = digest_mapping({
                            "abort_decision_receipt_sha256": (
                                decision.receipt_sha256
                            ),
                            "fence_epoch": fence_epoch,
                            "prepare_receipt_sha256": (
                                stored_prepare.prepare_receipt_sha256
                            ),
                            "request_sha256": request_id,
                            "role": self.role,
                            "schema": CONTROL_RECLAMATION_RELEASE_SCHEMA,
                            "source_fence_sha256": (
                                stored_prepare.source_fence_sha256
                            ),
                            "source_store_identity_sha256": (
                                stored_prepare.source_store_identity_sha256
                            ),
                        })
                        result = SourceLocalAbortReceiptV1.create(
                            request_sha256=request_id,
                            role=self.role,
                            source_store_identity_sha256=(
                                stored_prepare.source_store_identity_sha256
                            ),
                            prepare_receipt_sha256=(
                                stored_prepare.prepare_receipt_sha256
                            ),
                            coordinator_abort_decision_sha256=expected_abort,
                            fence_release_root_sha256=release_root,
                            aborted_at=observed,
                        )
                        database.execute(
                            "UPDATE source_reclamation_decisions SET "
                            "state='ABORTED',active_slot=NULL,"
                            "abort_decision_sha256=?,"
                            "fence_release_root_sha256=?,abort_receipt_json=?,"
                            "aborted_at=? WHERE request_sha256=? "
                            "AND state='PREPARED'",
                            (
                                expected_abort,
                                release_root,
                                sqlite3.Binary(canonical_bytes(result.as_dict())),
                                observed,
                                request_id,
                            ),
                        )
                        self._fault("abort.before_commit")
                        database.commit()
                        self._fault("abort.after_commit")
                    else:
                        raise AcquisitionControlReclamationConflict(
                            "applied control source cannot be aborted"
                        )
                except BaseException:
                    if database.in_transaction:
                        database.rollback()
                    raise
        self._durability_barrier()
        return result


class ExecutionReceiptReclamationParticipantV2(
    _ControlReclamationParticipantV2
):
    """Exact terminal-head plus complete execution-history participant."""

    role = "execution_receipts"

    def __init__(self, store: object, **kwargs: Any) -> None:
        from .acquisition_execution_store import ExecutionReceiptStore

        if not isinstance(store, ExecutionReceiptStore):
            raise TypeError("store must be ExecutionReceiptStore")
        super().__init__(store, **kwargs)

    def _base_store_binding_sha256(self) -> str:
        from .acquisition_execution_store import CONTRACT, SCHEMA_VERSION

        return digest_mapping({
            "contract": CONTRACT,
            "max_database_bytes": int(self._store._max_database_bytes),
            "max_receipts": int(self._store._max_receipts),
            "path_sha256": _normalised_path_sha256(self.path),
            "schema": "stv3-worker-execution-source-binding-v1",
            "schema_version": SCHEMA_VERSION,
        })

    def _validate_base_schema(self, database: sqlite3.Connection) -> None:
        expected_objects = {
            ("table", "execution_schema", "execution_schema"):
                _normalise_sql(
                    "CREATE TABLE execution_schema("
                    "singleton INTEGER PRIMARY KEY CHECK(singleton=1),"
                    "schema_version INTEGER NOT NULL)"
                ),
            ("table", "execution_receipts", "execution_receipts"):
                _normalise_sql(
                    "CREATE TABLE execution_receipts("
                    "receipt_sha256 TEXT PRIMARY KEY,"
                    "scheduler_job_id TEXT NOT NULL,"
                    "acquisition_job_id TEXT NOT NULL,"
                    "binding_sha256 TEXT NOT NULL UNIQUE,"
                    "attempt_count INTEGER NOT NULL,"
                    "outcome TEXT NOT NULL,"
                    "reason_code TEXT NOT NULL,"
                    "terminal INTEGER NOT NULL CHECK(terminal IN (0,1)),"
                    "content_sha256 TEXT,raw_receipt_sha256 TEXT,"
                    "capture_sha256 TEXT,capacity_commit_sha256 TEXT,"
                    "created_at REAL NOT NULL)"
                ),
            ("table", "terminal_heads", "terminal_heads"):
                _normalise_sql(
                    "CREATE TABLE terminal_heads("
                    "acquisition_job_id TEXT PRIMARY KEY,"
                    "receipt_sha256 TEXT NOT NULL UNIQUE REFERENCES "
                    "execution_receipts(receipt_sha256))"
                ),
        }
        if _base_schema_objects(database) != expected_objects:
            raise AcquisitionControlReclamationIntegrityError(
                "execution receipt base schema fingerprint differs"
            )
        expected = {
            "execution_schema": {
                "singleton",
                "schema_version",
            },
            "execution_receipts": {
                "receipt_sha256",
                "scheduler_job_id",
                "acquisition_job_id",
                "binding_sha256",
                "attempt_count",
                "outcome",
                "reason_code",
                "terminal",
                "content_sha256",
                "raw_receipt_sha256",
                "capture_sha256",
                "capacity_commit_sha256",
                "created_at",
            },
            "terminal_heads": {
                "acquisition_job_id",
                "receipt_sha256",
            },
        }
        for table, columns in expected.items():
            actual = {
                str(row[1])
                for row in database.execute(
                    f"PRAGMA table_info({table})"
                ).fetchall()
            }
            if actual != columns:
                raise AcquisitionControlReclamationIntegrityError(
                    "execution receipt base schema differs"
                )
        schema = database.execute(
            "SELECT singleton,schema_version FROM execution_schema"
        ).fetchall()
        if (
            len(schema) != 1
            or int(schema[0]["singleton"]) != 1
            or int(schema[0]["schema_version"]) != 1
        ):
            raise AcquisitionControlReclamationIntegrityError(
                "execution receipt schema authority differs"
            )

    def _history(
        self,
        database: sqlite3.Connection,
        acquisition_job_id: str,
    ) -> tuple[Any, ...]:
        rows = database.execute(
            "SELECT * FROM execution_receipts WHERE acquisition_job_id=? "
            "ORDER BY attempt_count,created_at,receipt_sha256 LIMIT ?",
            (acquisition_job_id, int(self._store._max_receipts) + 1),
        ).fetchall()
        if not rows or len(rows) > int(self._store._max_receipts):
            raise AcquisitionControlReclamationConflict(
                "execution history is unavailable or unbounded"
            )
        history = tuple(self._store._from_row(row) for row in rows)
        terminals = tuple(item for item in history if item.terminal)
        head = database.execute(
            "SELECT receipt_sha256 FROM terminal_heads "
            "WHERE acquisition_job_id=?",
            (acquisition_job_id,),
        ).fetchone()
        if (
            len(terminals) != 1
            or head is None
            or str(head["receipt_sha256"]) != terminals[0].receipt_sha256
        ):
            raise AcquisitionControlReclamationConflict(
                "execution terminal head and history differ"
            )
        return history

    def _member_for(
        self,
        database: sqlite3.Connection,
        held: HeldControlCandidate,
    ) -> dict[str, Any]:
        binding = held.binding
        history = self._history(database, binding.acquisition_job_id)
        if any(
            item.acquisition_job_id != binding.acquisition_job_id
            or item.scheduler_job_id != binding.scheduler_job_id
            for item in history
        ):
            raise AcquisitionControlReclamationConflict(
                "execution history identity differs from the candidate"
            )
        projection = {
            "execution_history": [item.as_dict() for item in history],
        }
        view_sha = participant_candidate_view_sha256(
            role=self.role,
            candidate_sha256=binding.candidate_sha256,
            projection=projection,
        )
        if view_sha != candidate_participant_view_sha256(
            held.record,
            self.role,
        ):
            raise AcquisitionControlReclamationConflict(
                "execution history differs from the sealed view"
            )
        return {
            "binding": binding.as_dict(),
            "candidate_sha256": binding.candidate_sha256,
            "primary_identity": binding.acquisition_job_id,
            "secondary_identity": binding.scheduler_job_id,
            "source_record": projection,
            "source_record_sha256": digest_mapping(projection),
            "view_sha256": view_sha,
        }

    def _source_snapshot(
        self,
        database: sqlite3.Connection,
        inventory: HeldControlCandidateInventory,
    ) -> tuple[tuple[dict[str, Any], ...], int, int]:
        self._validate_base_schema(database)
        members = tuple(
            self._member_for(database, held)
            for held in inventory.candidates
        )
        return (
            members,
            0,
            sum(
                len(item["source_record"]["execution_history"]) + 1
                for item in members
            ),
        )

    def _reproof_stored_members(
        self,
        database: sqlite3.Connection,
        *,
        members: tuple[dict[str, Any], ...],
    ) -> tuple[int, int]:
        self._validate_base_schema(database)
        entries = 0
        for member in members:
            history = self._history(
                database,
                member["primary_identity"],
            )
            projection = {
                "execution_history": [item.as_dict() for item in history],
            }
            current_view = participant_candidate_view_sha256(
                role=self.role,
                candidate_sha256=member["candidate_sha256"],
                projection=projection,
            )
            if (
                projection != member["source_record"]
                or current_view != member["view_sha256"]
                or any(
                    item.scheduler_job_id != member["secondary_identity"]
                    for item in history
                )
            ):
                raise AcquisitionControlReclamationConflict(
                    "execution history changed after prepare"
                )
            entries += len(history) + 1
        return 0, entries

    def _apply_source_rows(
        self,
        database: sqlite3.Connection,
        *,
        request_sha256: str,
        members: tuple[dict[str, Any], ...],
    ) -> tuple[int, int]:
        released = self._reproof_stored_members(
            database,
            members=members,
        )
        for member in members:
            history_count = len(
                member["source_record"]["execution_history"]
            )
            head = database.execute(
                "DELETE FROM terminal_heads WHERE acquisition_job_id=?",
                (member["primary_identity"],),
            )
            receipts = database.execute(
                "DELETE FROM execution_receipts WHERE acquisition_job_id=?",
                (member["primary_identity"],),
            )
            if head.rowcount != 1 or receipts.rowcount != history_count:
                raise AcquisitionControlReclamationConflict(
                    "execution source deletion count differs"
                )
        return released

    def _post_apply_projection(
        self,
        database: sqlite3.Connection,
        *,
        request_sha256: str,
        members: tuple[dict[str, Any], ...],
    ) -> dict[str, Any]:
        remaining = 0
        for member in members:
            remaining += int(database.execute(
                "SELECT count(*) FROM execution_receipts "
                "WHERE acquisition_job_id=?",
                (member["primary_identity"],),
            ).fetchone()[0])
            remaining += int(database.execute(
                "SELECT count(*) FROM terminal_heads "
                "WHERE acquisition_job_id=?",
                (member["primary_identity"],),
            ).fetchone()[0])
        tombstones = int(database.execute(
            "SELECT count(*) FROM source_reclamation_tombstones "
            "WHERE request_sha256=?",
            (request_sha256,),
        ).fetchone()[0])
        if remaining != 0 or tombstones != len(members):
            raise AcquisitionControlReclamationIntegrityError(
                "execution post-apply closure differs"
            )
        return {
            "candidate_count": len(members),
            "remaining_source_rows": remaining,
            "tombstone_count": tombstones,
        }


class SchedulerReclamationParticipantV2(_ControlReclamationParticipantV2):
    """Exact immutable terminal scheduler-row participant."""

    role = "scheduler"

    def __init__(self, store: object, **kwargs: Any) -> None:
        from .autonomous_scheduler import AutonomousScheduler

        if not isinstance(store, AutonomousScheduler):
            raise TypeError("store must be AutonomousScheduler")
        super().__init__(store, **kwargs)

    def _base_store_binding_sha256(self) -> str:
        from .autonomous_scheduler import SCHEMA_VERSION

        desired_identity = self._store._desired_state_store.identity_sha256
        return digest_mapping({
            "clock_backward_tolerance_seconds": (
                self._store._clock_backward_tolerance_seconds
            ),
            "desired_store_identity_sha256": desired_identity,
            "lease_seconds": self._store._lease_seconds,
            "max_database_bytes": self._store._max_database_bytes,
            "max_desired_state_age_seconds": (
                self._store._max_desired_state_age_seconds
            ),
            "max_outbox_rows": self._store._max_outbox_rows,
            "path_sha256": _normalised_path_sha256(self.path),
            "retry_base_seconds": self._store._retry_base_seconds,
            "retry_max_seconds": self._store._retry_max_seconds,
            "schema": "stv3-worker-scheduler-source-binding-v1",
            "schema_version": SCHEMA_VERSION,
        })

    def _validate_base_schema(self, database: sqlite3.Connection) -> None:
        expected_objects = {
            ("table", "scheduler_schema", "scheduler_schema"):
                _normalise_sql(
                    "CREATE TABLE scheduler_schema("
                    "singleton INTEGER PRIMARY KEY CHECK(singleton=1),"
                    "schema_version INTEGER NOT NULL)"
                ),
            ("table", "scheduler_runtime", "scheduler_runtime"):
                _normalise_sql(
                    "CREATE TABLE scheduler_runtime("
                    "singleton INTEGER PRIMARY KEY CHECK(singleton=1),"
                    "last_wall_time REAL,active_desired_state_sha256 TEXT,"
                    "active_plan_sha256 TEXT,active_intent_count INTEGER NOT NULL "
                    "DEFAULT 0 CHECK(active_intent_count>=0))"
                ),
            ("table", "scheduler_outbox", "scheduler_outbox"):
                _normalise_sql(
                    """
                    CREATE TABLE scheduler_outbox(
                        job_id TEXT PRIMARY KEY,
                        slot_id TEXT NOT NULL UNIQUE,
                        record_sha256 TEXT NOT NULL,
                        desired_state_sha256 TEXT NOT NULL,
                        intent_id TEXT NOT NULL,
                        job_type TEXT NOT NULL,
                        cadence_seconds INTEGER NOT NULL
                            CHECK(cadence_seconds>=1),
                        slot_index INTEGER NOT NULL CHECK(slot_index>=0),
                        scheduled_for REAL NOT NULL CHECK(scheduled_for>=0),
                        deadline_at REAL NOT NULL
                            CHECK(deadline_at>scheduled_for),
                        priority INTEGER NOT NULL CHECK(priority>=0),
                        payload_json BLOB NOT NULL,
                        payload_sha256 TEXT NOT NULL,
                        state TEXT NOT NULL CHECK(state IN(
                            'pending','leased','retry','completed','expired',
                            'superseded'
                        )),
                        attempt_count INTEGER NOT NULL DEFAULT 0
                            CHECK(attempt_count>=0),
                        next_attempt_at REAL NOT NULL CHECK(next_attempt_at>=0),
                        lease_owner TEXT,
                        lease_token TEXT,
                        lease_expires_at REAL,
                        last_attempt_token TEXT,
                        last_transition TEXT,
                        last_error_code TEXT,
                        created_at REAL NOT NULL CHECK(created_at>=0),
                        updated_at REAL NOT NULL CHECK(updated_at>=0),
                        terminal_at REAL,
                        CHECK(
                            (state='leased' AND lease_owner IS NOT NULL AND
                             lease_token IS NOT NULL AND
                             lease_expires_at IS NOT NULL)
                            OR
                            (state!='leased' AND lease_owner IS NULL AND
                             lease_token IS NULL AND
                             lease_expires_at IS NULL)
                        )
                    )
                    """
                ),
            ("index", "scheduler_one_active_lease", "scheduler_outbox"):
                _normalise_sql(
                    "CREATE UNIQUE INDEX scheduler_one_active_lease ON "
                    "scheduler_outbox((1)) WHERE state='leased'"
                ),
            ("index", "scheduler_ready_order", "scheduler_outbox"):
                _normalise_sql(
                    "CREATE INDEX scheduler_ready_order ON scheduler_outbox("
                    "state,next_attempt_at,priority,scheduled_for,job_id)"
                ),
        }
        if _base_schema_objects(database) != expected_objects:
            raise AcquisitionControlReclamationIntegrityError(
                "scheduler base schema fingerprint differs"
            )
        schema = database.execute(
            "SELECT singleton,schema_version FROM scheduler_schema"
        ).fetchall()
        if (
            len(schema) != 1
            or int(schema[0]["singleton"]) != 1
            or int(schema[0]["schema_version"]) != 1
        ):
            raise AcquisitionControlReclamationIntegrityError(
                "scheduler schema authority differs"
            )

    def _terminal_projection(
        self,
        database: sqlite3.Connection,
        job_id: str,
    ) -> tuple[dict[str, Any], str]:
        from .autonomous_scheduler import SchedulerTerminalProof

        row = database.execute(
            "SELECT * FROM scheduler_outbox WHERE job_id=?",
            (job_id,),
        ).fetchone()
        if row is None:
            raise AcquisitionControlReclamationConflict(
                "scheduler terminal source is unavailable"
            )
        self._store._validate_static_row(row)
        if (
            str(row["state"]) not in {
                "completed",
                "expired",
                "superseded",
            }
            or row["terminal_at"] is None
            or row["lease_owner"] is not None
            or row["lease_token"] is not None
            or row["lease_expires_at"] is not None
            or row["last_transition"] is None
        ):
            raise AcquisitionControlReclamationConflict(
                "scheduler source row is not terminal"
            )
        proof = SchedulerTerminalProof(
            job_id=str(row["job_id"]),
            slot_id=str(row["slot_id"]),
            record_sha256=str(row["record_sha256"]),
            desired_state_sha256=str(row["desired_state_sha256"]),
            intent_id=str(row["intent_id"]),
            job_type=str(row["job_type"]),
            payload_sha256=str(row["payload_sha256"]),
            state=str(row["state"]),
            slot_index=int(row["slot_index"]),
            scheduled_for=float(row["scheduled_for"]),
            deadline_at=float(row["deadline_at"]),
            attempt_count=int(row["attempt_count"]),
            last_transition=str(row["last_transition"]),
            last_error_code=(
                None
                if row["last_error_code"] is None
                else str(row["last_error_code"])
            ),
            terminal_at=float(row["terminal_at"]),
        )
        return (
            {"scheduler_terminal": {
                **proof.as_dict(),
                "proof_sha256": proof.proof_sha256,
            }},
            proof.slot_id,
        )

    def _source_snapshot(
        self,
        database: sqlite3.Connection,
        inventory: HeldControlCandidateInventory,
    ) -> tuple[tuple[dict[str, Any], ...], int, int]:
        members: list[dict[str, Any]] = []
        for held in inventory.candidates:
            projection, slot_id = self._terminal_projection(
                database,
                held.binding.scheduler_job_id,
            )
            view_sha = participant_candidate_view_sha256(
                role=self.role,
                candidate_sha256=held.binding.candidate_sha256,
                projection=projection,
            )
            if view_sha != candidate_participant_view_sha256(
                held.record,
                self.role,
            ):
                raise AcquisitionControlReclamationConflict(
                    "scheduler terminal differs from the sealed view"
                )
            members.append({
                "binding": held.binding.as_dict(),
                "candidate_sha256": held.binding.candidate_sha256,
                "primary_identity": held.binding.scheduler_job_id,
                "secondary_identity": slot_id,
                "source_record": projection,
                "source_record_sha256": digest_mapping(projection),
                "view_sha256": view_sha,
            })
        return tuple(members), 0, len(members)

    def _reproof_stored_members(
        self,
        database: sqlite3.Connection,
        *,
        members: tuple[dict[str, Any], ...],
    ) -> tuple[int, int]:
        for member in members:
            projection, slot_id = self._terminal_projection(
                database,
                member["primary_identity"],
            )
            view_sha = participant_candidate_view_sha256(
                role=self.role,
                candidate_sha256=member["candidate_sha256"],
                projection=projection,
            )
            if (
                projection != member["source_record"]
                or slot_id != member["secondary_identity"]
                or view_sha != member["view_sha256"]
            ):
                raise AcquisitionControlReclamationConflict(
                    "scheduler terminal changed after prepare"
                )
        return 0, len(members)

    def _apply_source_rows(
        self,
        database: sqlite3.Connection,
        *,
        request_sha256: str,
        members: tuple[dict[str, Any], ...],
    ) -> tuple[int, int]:
        released = self._reproof_stored_members(
            database,
            members=members,
        )
        for member in members:
            cursor = database.execute(
                "DELETE FROM scheduler_outbox WHERE job_id=?",
                (member["primary_identity"],),
            )
            if cursor.rowcount != 1:
                raise AcquisitionControlReclamationConflict(
                    "scheduler source deletion count differs"
                )
        return released

    def _post_apply_projection(
        self,
        database: sqlite3.Connection,
        *,
        request_sha256: str,
        members: tuple[dict[str, Any], ...],
    ) -> dict[str, Any]:
        remaining = sum(
            int(database.execute(
                "SELECT count(*) FROM scheduler_outbox WHERE job_id=?",
                (item["primary_identity"],),
            ).fetchone()[0])
            for item in members
        )
        tombstones = int(database.execute(
            "SELECT count(*) FROM source_reclamation_tombstones "
            "WHERE request_sha256=?",
            (request_sha256,),
        ).fetchone()[0])
        if remaining != 0 or tombstones != len(members):
            raise AcquisitionControlReclamationIntegrityError(
                "scheduler post-apply closure differs"
            )
        return {
            "candidate_count": len(members),
            "remaining_source_rows": remaining,
            "tombstone_count": tombstones,
        }


class CapacityLedgerReclamationParticipantV2(
    _ControlReclamationParticipantV2
):
    """Committed capacity release with exact provider/global counter closure."""

    role = "capacity_ledger"

    def __init__(self, store: object, **kwargs: Any) -> None:
        from .raw_capacity_ledger import RawCapacityLedger

        if not isinstance(store, RawCapacityLedger):
            raise TypeError("store must be RawCapacityLedger")
        super().__init__(store, **kwargs)

    def _base_store_binding_sha256(self) -> str:
        from .raw_capacity_ledger import (
            ADMISSION_STATUS,
            ALGORITHM_VERSION,
            LEDGER_CONTRACT,
            SCHEMA_VERSION,
        )

        return digest_mapping({
            "admission_status": ADMISSION_STATUS,
            "algorithm_version": ALGORITHM_VERSION,
            "contract": LEDGER_CONTRACT,
            "path_sha256": _normalised_path_sha256(self.path),
            "profile": self._store.profile.to_mapping(),
            "schema": "stv3-worker-capacity-source-binding-v1",
            "schema_version": SCHEMA_VERSION,
        })

    def _validate_base_schema(self, database: sqlite3.Connection) -> None:
        self._store._validate_schema(database, full_integrity=False)

    def _validate_capacity_fact_counters(
        self,
        database: sqlite3.Connection,
    ) -> None:
        """Reconcile bounded aggregates with every immutable reservation."""

        rows = database.execute(
            "SELECT * FROM reservations ORDER BY reservation_id LIMIT ?",
            (int(self._store.profile.max_reservations) + 1,),
        ).fetchall()
        if len(rows) > int(self._store.profile.max_reservations):
            raise AcquisitionControlReclamationCapacityError(
                "capacity reservation facts exceed their immutable bound"
            )
        names = (
            "reservation_rows",
            "reserved_count",
            "committed_count",
            "reserved_bytes",
            "committed_bytes",
            "reserved_entries",
            "committed_entries",
        )
        totals = {name: 0 for name in names}
        provider_totals = {
            item.provider: {name: 0 for name in names}
            for item in self._store.profile.providers
        }
        for row in rows:
            receipt = self._store._validated_reservation(row)
            current = provider_totals.get(receipt.provider)
            if current is None:
                raise AcquisitionControlReclamationIntegrityError(
                    "capacity reservation provider differs"
                )
            totals["reservation_rows"] += 1
            current["reservation_rows"] += 1
            prefix = "reserved" if receipt.status == "RESERVED" else "committed"
            byte_count = (
                receipt.requested_bytes
                if receipt.status == "RESERVED"
                else int(receipt.actual_bytes or 0)
            )
            entry_count = (
                receipt.requested_entries
                if receipt.status == "RESERVED"
                else int(receipt.actual_entries or 0)
            )
            for values in (totals, current):
                values[f"{prefix}_count"] += 1
                values[f"{prefix}_bytes"] += byte_count
                values[f"{prefix}_entries"] += entry_count
        if totals != self._store._counter_mapping(
            self._store._global_row(database)
        ):
            raise AcquisitionControlReclamationIntegrityError(
                "global capacity counters differ from reservation facts"
            )
        for provider, expected in provider_totals.items():
            if expected != self._store._counter_mapping(
                self._store._provider_row(database, provider)
            ):
                raise AcquisitionControlReclamationIntegrityError(
                    "provider capacity counters differ from reservation facts"
                )

    def _committed_for_job(
        self,
        database: sqlite3.Connection,
        *,
        job_identity: str,
        candidate_sha256: str,
        expected_view_sha256: str,
    ) -> Any:
        rows = database.execute(
            "SELECT * FROM reservations WHERE job_identity=? "
            "ORDER BY provider,reservation_id",
            (job_identity,),
        ).fetchall()
        matches = []
        for row in rows:
            receipt = self._store._validated_reservation(row)
            if receipt.status != "COMMITTED":
                continue
            projection = {"capacity_commit": receipt.to_mapping()}
            if participant_candidate_view_sha256(
                role=self.role,
                candidate_sha256=candidate_sha256,
                projection=projection,
            ) == expected_view_sha256:
                matches.append(receipt)
        if len(matches) != 1:
            raise AcquisitionControlReclamationConflict(
                "capacity commit is unavailable or ambiguous"
            )
        return matches[0]

    def _source_snapshot(
        self,
        database: sqlite3.Connection,
        inventory: HeldControlCandidateInventory,
    ) -> tuple[tuple[dict[str, Any], ...], int, int]:
        self._store._validate_fast_invariants(database)
        self._validate_capacity_fact_counters(database)
        members: list[dict[str, Any]] = []
        released_bytes = 0
        released_entries = 0
        for held in inventory.candidates:
            expected_view = candidate_participant_view_sha256(
                held.record,
                self.role,
            )
            receipt = self._committed_for_job(
                database,
                job_identity=held.binding.acquisition_job_id,
                candidate_sha256=held.binding.candidate_sha256,
                expected_view_sha256=expected_view,
            )
            projection = {"capacity_commit": receipt.to_mapping()}
            members.append({
                "binding": held.binding.as_dict(),
                "candidate_sha256": held.binding.candidate_sha256,
                "primary_identity": receipt.reservation_id,
                "secondary_identity": receipt.job_identity,
                "source_record": projection,
                "source_record_sha256": digest_mapping(projection),
                "view_sha256": expected_view,
            })
            released_bytes += int(receipt.actual_bytes or 0)
            released_entries += int(receipt.actual_entries or 0)
        return tuple(members), released_bytes, released_entries

    def _reproof_stored_members(
        self,
        database: sqlite3.Connection,
        *,
        members: tuple[dict[str, Any], ...],
    ) -> tuple[int, int]:
        self._store._validate_fast_invariants(database)
        self._validate_capacity_fact_counters(database)
        released_bytes = 0
        released_entries = 0
        for member in members:
            row = database.execute(
                "SELECT * FROM reservations WHERE reservation_id=?",
                (member["primary_identity"],),
            ).fetchone()
            if row is None:
                raise AcquisitionControlReclamationConflict(
                    "capacity commit disappeared after prepare"
                )
            receipt = self._store._validated_reservation(row)
            projection = {"capacity_commit": receipt.to_mapping()}
            view_sha = participant_candidate_view_sha256(
                role=self.role,
                candidate_sha256=member["candidate_sha256"],
                projection=projection,
            )
            if (
                receipt.status != "COMMITTED"
                or receipt.job_identity != member["secondary_identity"]
                or projection != member["source_record"]
                or view_sha != member["view_sha256"]
            ):
                raise AcquisitionControlReclamationConflict(
                    "capacity commit changed after prepare"
                )
            released_bytes += int(receipt.actual_bytes or 0)
            released_entries += int(receipt.actual_entries or 0)
        return released_bytes, released_entries

    def _apply_source_rows(
        self,
        database: sqlite3.Connection,
        *,
        request_sha256: str,
        members: tuple[dict[str, Any], ...],
    ) -> tuple[int, int]:
        released = self._reproof_stored_members(
            database,
            members=members,
        )
        trigger = database.execute(
            "SELECT sql FROM sqlite_master WHERE type='trigger' "
            "AND name='reservation_no_delete'"
        ).fetchone()
        expected_trigger = (
            "CREATE TRIGGER reservation_no_delete BEFORE DELETE ON reservations "
            "BEGIN SELECT RAISE(ABORT,'capacity is never released'); END"
        )
        if (
            trigger is None
            or _normalise_sql(str(trigger["sql"]))
            != _normalise_sql(expected_trigger)
        ):
            raise AcquisitionControlReclamationIntegrityError(
                "capacity no-release authority differs"
            )
        database.execute("DROP TRIGGER reservation_no_delete")
        for member in members:
            values = member["source_record"]["capacity_commit"]
            actual_bytes = int(values["actual_bytes"])
            actual_entries = int(values["actual_entries"])
            provider = str(values["provider"])
            global_cursor = database.execute(
                "UPDATE global_capacity SET "
                "reservation_rows=reservation_rows-1,"
                "committed_count=committed_count-1,"
                "committed_bytes=committed_bytes-?,"
                "committed_entries=committed_entries-? "
                "WHERE singleton=1 AND reservation_rows>=1 "
                "AND committed_count>=1 AND committed_bytes>=? "
                "AND committed_entries>=?",
                (
                    actual_bytes,
                    actual_entries,
                    actual_bytes,
                    actual_entries,
                ),
            )
            provider_cursor = database.execute(
                "UPDATE provider_capacity SET "
                "reservation_rows=reservation_rows-1,"
                "committed_count=committed_count-1,"
                "committed_bytes=committed_bytes-?,"
                "committed_entries=committed_entries-? "
                "WHERE provider=? AND reservation_rows>=1 "
                "AND committed_count>=1 AND committed_bytes>=? "
                "AND committed_entries>=?",
                (
                    actual_bytes,
                    actual_entries,
                    provider,
                    actual_bytes,
                    actual_entries,
                ),
            )
            deleted = database.execute(
                "DELETE FROM reservations WHERE reservation_id=?",
                (member["primary_identity"],),
            )
            if (
                global_cursor.rowcount != 1
                or provider_cursor.rowcount != 1
                or deleted.rowcount != 1
            ):
                raise AcquisitionControlReclamationConflict(
                    "capacity source release count differs"
                )
        database.execute(expected_trigger)
        self._store._validate_fast_invariants(database)
        self._validate_capacity_fact_counters(database)
        return released

    def _post_apply_projection(
        self,
        database: sqlite3.Connection,
        *,
        request_sha256: str,
        members: tuple[dict[str, Any], ...],
    ) -> dict[str, Any]:
        self._store._validate_fast_invariants(database)
        self._validate_capacity_fact_counters(database)
        remaining = sum(
            int(database.execute(
                "SELECT count(*) FROM reservations WHERE reservation_id=?",
                (item["primary_identity"],),
            ).fetchone()[0])
            for item in members
        )
        tombstones = int(database.execute(
            "SELECT count(*) FROM source_reclamation_tombstones "
            "WHERE request_sha256=?",
            (request_sha256,),
        ).fetchone()[0])
        global_row = self._store._counter_mapping(
            self._store._global_row(database)
        )
        provider_rows = {
            str(row["provider"]): self._store._counter_mapping(row)
            for row in database.execute(
                "SELECT * FROM provider_capacity ORDER BY provider"
            ).fetchall()
        }
        if remaining != 0 or tombstones != len(members):
            raise AcquisitionControlReclamationIntegrityError(
                "capacity post-apply closure differs"
            )
        return {
            "candidate_count": len(members),
            "global_counters": global_row,
            "provider_counters": provider_rows,
            "remaining_source_rows": remaining,
            "tombstone_count": tombstones,
        }


__all__ = [
    "AcquisitionControlReclamationCapacityError",
    "AcquisitionControlReclamationConflict",
    "AcquisitionControlReclamationError",
    "AcquisitionControlReclamationIntegrityError",
    "CONTROL_RECLAMATION_CONTRACT",
    "CONTROL_RECLAMATION_SCHEMA_OBJECT_NAMES",
    "CONTROL_RECLAMATION_SCHEMA_VERSION",
    "CONTROL_RECLAMATION_TABLES",
    "CapacityLedgerReclamationParticipantV2",
    "ExecutionReceiptReclamationParticipantV2",
    "HeldControlCandidate",
    "HeldControlCandidateInventory",
    "SchedulerReclamationParticipantV2",
    "control_reclamation_extension_present",
    "hold_control_candidate_inventory",
    "validate_control_reclamation_extension_schema",
]
