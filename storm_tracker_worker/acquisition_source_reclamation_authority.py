"""Machine-readable fail-closed authority for acquisition source reclamation.

The existing source-retention archive can prove and durably seal exact source
evidence.  It cannot prove that five independent source authorities can be
pruned as one restart-safe transaction.  This module records that boundary in
a deterministic, content-addressed document.  Its optional offline coordinator
writes only dedicated prepare journals; it has no source-store mutator and can
never authorise or perform source release.

The gap catalogue is deliberately executable contract data rather than prose.
When a participant eventually implements one of the named capabilities, the
catalogue and its tests must be revised before any positive authority can be
designed.  A sealed-export plan or archive anchor is evidence input only; it is
never promoted into a deletion token.
"""
from __future__ import annotations

import base64
from contextlib import closing, contextmanager
from dataclasses import asdict, dataclass, field, replace
import errno
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import threading
import time
from typing import Any, Callable, Iterator, Mapping

from .acquisition_consumer_registry import (
    AcquisitionConsumerClosureReceipt,
    AcquisitionConsumerRegistryBoundEpochVerifier,
    AcquisitionConsumerRegistryError,
    parse_consumer_closure_receipt,
)
from .acquisition_evidence_lifecycle import (
    CONTRACT as EVIDENCE_LIFECYCLE_CONTRACT,
    AcquisitionEvidenceLifecycleStore,
)
from .acquisition_execution_store import (
    CONTRACT as EXECUTION_CONTRACT,
    ExecutionReceiptStore,
)
from .acquisition_source_retention import (
    CONTRACT as SOURCE_RETENTION_CONTRACT,
    AcquisitionSourceRetentionArchive,
    AcquisitionSourceRetentionPlan,
    RetentionDisposition,
)
from .autonomous_scheduler import (
    CONTRACT as SCHEDULER_CONTRACT,
    AutonomousScheduler,
)
from .raw_artifacts import (
    RAW_ARTIFACT_CAPTURE_SCHEMA,
    RAW_ARTIFACT_RECEIPT_SCHEMA,
    RAW_ARTIFACT_SCHEMA_VERSION,
    RawArtifactReceipt,
    RawArtifactStore,
)
from .raw_capacity_ledger import LEDGER_CONTRACT, RawCapacityLedger


COORDINATOR_CONTRACT = (
    "stv3-worker-acquisition-source-reclamation-coordinator-v1"
)
COORDINATOR_SCHEMA_VERSION = 1
PARTICIPANT_PREPARE_CONTRACT = (
    "stv3-worker-acquisition-source-reclamation-participant-prepare-v2"
)
CONSUMER_CLOSURE_ROOT_SCHEMA = (
    "stv3-worker-acquisition-reclamation-consumer-closure-root-v1"
)
PREPARING = "PREPARING"
PREPARED = "PREPARED"
COMMITTED = "COMMITTED"
APPLIED = "APPLIED"
COORDINATOR_STATES = (PREPARING, PREPARED, COMMITTED, APPLIED)
PARTICIPANT_ROLES = (
    "capacity_ledger",
    "downstream_lifecycle",
    "execution_receipts",
    "raw_artifacts",
    "scheduler",
)
APPLY_ENABLED = False
APPLY_BLOCKERS = (
    "complete_consumer_registry_epoch_root_authority_unavailable",
    "participant_source_local_apply_transactions_unavailable",
    "raw_filesystem_unlink_restart_journal_unavailable",
)
REGISTRY_VERIFIED_APPLY_BLOCKERS = (
    "participant_source_local_apply_transactions_unavailable",
    "raw_filesystem_unlink_restart_journal_unavailable",
)
COORDINATOR_LOCK_BYTES = b"STV3_RECLAMATION_COORDINATOR_LOCK_V1\n"
MAX_COORDINATOR_CANDIDATES = 4096
MAX_COORDINATOR_DECISIONS = 2048
MAX_COORDINATOR_EVENTS = 16384
MAX_PARTICIPANT_PREPARES = 2048
MAX_COORDINATOR_DATABASE_BYTES = 256 * 1024 * 1024
MAX_PARTICIPANT_DATABASE_BYTES = 64 * 1024 * 1024


CONTRACT = "stv3-worker-acquisition-source-reclamation-gap-authority-v1"
SCHEMA = "stv3-worker-acquisition-source-reclamation-eligibility-v1"
STATUS = "withheld"
REASON_CODE = "cross_store_atomic_prune_contract_unavailable"
MAX_AUTHORITY_BYTES = 64 * 1024

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_CODE = re.compile(r"^[a-z][a-z0-9_.:-]{0,191}$")


class AcquisitionSourceReclamationAuthorityError(RuntimeError):
    """The fail-closed authority or one of its evidence bindings is invalid."""


def _code(value: object, label: str) -> str:
    if not isinstance(value, str) or _CODE.fullmatch(value) is None:
        raise ValueError(f"invalid {label}")
    return value


@dataclass(frozen=True, slots=True)
class RequiredStoreCapability:
    """One exact participant capability absent from the current store API."""

    role: str
    current_contract: str
    present_evidence_apis: tuple[str, ...]
    required_prepare_api: str
    required_apply_api: str
    required_resolver_api: str
    closure_invariant: str

    def __post_init__(self) -> None:
        _code(self.role, "store role")
        _code(self.current_contract, "store contract")
        if (
            not isinstance(self.present_evidence_apis, tuple)
            or not self.present_evidence_apis
            or self.present_evidence_apis
            != tuple(sorted(set(self.present_evidence_apis)))
        ):
            raise ValueError("present evidence APIs must be sorted and unique")
        for value in self.present_evidence_apis:
            _code(value, "present evidence API")
        _code(self.required_prepare_api, "required prepare API")
        _code(self.required_apply_api, "required apply API")
        _code(self.required_resolver_api, "required resolver API")
        _code(self.closure_invariant, "closure invariant")

    def as_dict(self) -> dict[str, Any]:
        return {
            "closure_invariant": self.closure_invariant,
            "current_contract": self.current_contract,
            "present_evidence_apis": list(self.present_evidence_apis),
            "required_apply_api": self.required_apply_api,
            "required_prepare_api": self.required_prepare_api,
            "required_resolver_api": self.required_resolver_api,
            "role": self.role,
        }


RAW_STORE_CONTRACT = (
    f"raw-artifact-store-schema-{RAW_ARTIFACT_SCHEMA_VERSION}:"
    f"{RAW_ARTIFACT_RECEIPT_SCHEMA}:{RAW_ARTIFACT_CAPTURE_SCHEMA}"
)


REQUIRED_STORE_CAPABILITIES = (
    RequiredStoreCapability(
        role="capacity_ledger",
        current_contract=LEDGER_CONTRACT,
        present_evidence_apis=("audit", "load", "load_for_job", "stats"),
        required_prepare_api="prepare_committed_capacity_release_cas_v1",
        required_apply_api="apply_committed_capacity_release_decision_v1",
        required_resolver_api="resolve_archived_capacity_commit_v1",
        closure_invariant="provider_and_global_counters_decrement_exactly_once",
    ),
    RequiredStoreCapability(
        role="downstream_lifecycle",
        current_contract=EVIDENCE_LIFECYCLE_CONTRACT,
        present_evidence_apis=(
            "audit_archive",
            "load_terminal_consumption",
            "lookup_sequence",
        ),
        required_prepare_api="prepare_complete_source_reference_closure_cas_v1",
        required_apply_api="apply_source_reference_closure_decision_v1",
        required_resolver_api="resolve_archived_source_reference_closure_v1",
        closure_invariant="all_registered_consumers_are_terminal_and_archived",
    ),
    RequiredStoreCapability(
        role="execution_receipts",
        current_contract=EXECUTION_CONTRACT,
        present_evidence_apis=(
            "load_job_receipts",
            "load_terminal",
            "terminal_receipt_page",
        ),
        required_prepare_api="prepare_execution_history_prune_cas_v1",
        required_apply_api="apply_execution_history_prune_decision_v1",
        required_resolver_api="resolve_archived_execution_history_v1",
        closure_invariant="terminal_head_and_complete_attempt_history_move_together",
    ),
    RequiredStoreCapability(
        role="raw_artifacts",
        current_contract=RAW_STORE_CONTRACT,
        present_evidence_apis=(
            "accounting_captures",
            "accounting_receipts",
            "load_capture_bundle_bytes",
            "stats",
        ),
        required_prepare_api="prepare_reference_counted_capture_prune_cas_v1",
        required_apply_api="apply_journaled_raw_prune_decision_v1",
        required_resolver_api="resolve_archived_capture_receipt_object_v1",
        closure_invariant="shared_receipts_and_objects_unlink_only_at_zero_references",
    ),
    RequiredStoreCapability(
        role="scheduler",
        current_contract=SCHEDULER_CONTRACT,
        present_evidence_apis=("diagnostics", "get_record", "terminal_proof"),
        required_prepare_api="prepare_terminal_scheduler_prune_cas_v1",
        required_apply_api="apply_terminal_scheduler_prune_decision_v1",
        required_resolver_api="resolve_archived_scheduler_terminal_v1",
        closure_invariant="reconcile_cannot_rematerialize_an_archived_terminal_slot",
    ),
)

if tuple(item.role for item in REQUIRED_STORE_CAPABILITIES) != tuple(
    sorted(item.role for item in REQUIRED_STORE_CAPABILITIES)
):
    raise RuntimeError("reclamation store capability catalogue is not canonical")


CROSS_STORE_REQUIREMENTS = (
    "archive_anchor_and_record_are_revalidated_before_prepare",
    "committed_decision_is_durable_before_any_participant_apply",
    "complete_downstream_consumer_registry_has_a_bound_epoch_root",
    "each_participant_prepare_is_compare_and_swap_on_an_exact_view_root",
    "filesystem_unlink_has_a_durable_restart_journal",
    "one_bounded_coordinator_journal_orders_prepared_committed_and_applied",
    "participant_apply_is_idempotent_and_resumes_after_process_or_host_crash",
    "prepared_participants_are_revalidated_before_the_commit_decision",
    "rollback_before_commit_preserves_every_source_authority",
)

if CROSS_STORE_REQUIREMENTS != tuple(sorted(set(CROSS_STORE_REQUIREMENTS))):
    raise RuntimeError("cross-store reclamation requirements are not canonical")


def _canonical(value: Any) -> bytes:
    try:
        payload = json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
    except (TypeError, ValueError, RecursionError) as err:
        raise AcquisitionSourceReclamationAuthorityError(
            "reclamation authority is not canonical"
        ) from err
    if len(payload) > MAX_AUTHORITY_BYTES:
        raise AcquisitionSourceReclamationAuthorityError(
            "reclamation authority exceeds its byte bound"
        )
    return payload


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _sha(value: object, label: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise AcquisitionSourceReclamationAuthorityError(f"invalid {label}")
    return value


def _count(value: object, label: str) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not 0 <= value <= 2**53
    ):
        raise AcquisitionSourceReclamationAuthorityError(f"invalid {label}")
    return value


def capability_catalog_mapping() -> dict[str, Any]:
    """Return the fixed current-store gap catalogue in canonical order."""

    return {
        "apply_hard_disabled": True,
        "coordinator_journal_states": list(COORDINATOR_STATES),
        "consumer_closure_receipts_implemented": True,
        "consumer_registry_epoch_root_verifier_implemented": True,
        "cross_store_requirements": list(CROSS_STORE_REQUIREMENTS),
        "exact_archive_resolvers_implemented": True,
        "participant_prepare_sidecars_implemented": True,
        "participant_source_apply_apis_implemented": False,
        "participants": [item.as_dict() for item in REQUIRED_STORE_CAPABILITIES],
        "positive_authority_implemented": False,
        "source_mutation_implemented": False,
    }


def capability_catalog_sha256() -> str:
    return _digest(capability_catalog_mapping())


@dataclass(frozen=True, slots=True, repr=False)
class FailClosedReclamationAuthority:
    authority_sha256: str
    source_binding_sha256: str
    source_view_sha256: str
    retention_plan_sha256: str
    sealed_archive_anchor_sha256: str
    candidate_count: int
    sealed_export_eligible_count: int
    withheld_count: int
    sealed_archive_record_count: int
    capability_catalog_sha256: str
    status: str = STATUS
    reason_code: str = REASON_CODE
    reclaim_authorized: bool = False
    source_release_performed: bool = False

    def as_dict(self) -> dict[str, Any]:
        return {
            **self._core(),
            "authority_sha256": self.authority_sha256,
        }

    def to_json_bytes(self) -> bytes:
        validate_fail_closed_reclamation_authority(self)
        return _canonical(self.as_dict())

    def _core(self) -> dict[str, Any]:
        return {
            "candidate_count": self.candidate_count,
            "capability_catalog": capability_catalog_mapping(),
            "capability_catalog_sha256": self.capability_catalog_sha256,
            "contract": CONTRACT,
            "reason_code": self.reason_code,
            "reclaim_authorized": self.reclaim_authorized,
            "retention_contract": SOURCE_RETENTION_CONTRACT,
            "retention_plan_sha256": self.retention_plan_sha256,
            "schema": SCHEMA,
            "sealed_archive_anchor_sha256": self.sealed_archive_anchor_sha256,
            "sealed_archive_record_count": self.sealed_archive_record_count,
            "sealed_export_eligible_count": self.sealed_export_eligible_count,
            "source_binding_sha256": self.source_binding_sha256,
            "source_release_performed": self.source_release_performed,
            "source_view_sha256": self.source_view_sha256,
            "status": self.status,
            "withheld_count": self.withheld_count,
        }


def _validated_plan_counts(
    plan: AcquisitionSourceRetentionPlan,
) -> tuple[int, int, int]:
    if not isinstance(plan, AcquisitionSourceRetentionPlan):
        raise TypeError("plan must be AcquisitionSourceRetentionPlan")
    if plan.reclaim_authorized is not False:
        raise AcquisitionSourceReclamationAuthorityError(
            "retention plan unexpectedly authorises reclamation"
        )
    if not isinstance(plan.dispositions, tuple) or not plan.dispositions:
        raise AcquisitionSourceReclamationAuthorityError(
            "retention plan dispositions are unavailable"
        )
    eligible = 0
    withheld = 0
    candidate_hashes: list[str] = []
    for disposition in plan.dispositions:
        if not isinstance(disposition, RetentionDisposition):
            raise AcquisitionSourceReclamationAuthorityError(
                "retention disposition type differs"
            )
        candidate_hashes.append(
            _sha(disposition.candidate_sha256, "candidate SHA-256")
        )
        if disposition.status == "sealed_export_eligible":
            if disposition.reason_code != "all_authorities_cross_bound":
                raise AcquisitionSourceReclamationAuthorityError(
                    "eligible retention disposition differs"
                )
            _sha(disposition.record_sha256, "retention record SHA-256")
            eligible += 1
        elif (
            disposition.status == "withheld"
            and disposition.record_sha256 is None
            and isinstance(disposition.reason_code, str)
            and disposition.reason_code
        ):
            withheld += 1
        else:
            raise AcquisitionSourceReclamationAuthorityError(
                "retention disposition is not fail closed"
            )
    if (
        candidate_hashes != sorted(candidate_hashes)
        or len(candidate_hashes) != len(set(candidate_hashes))
    ):
        raise AcquisitionSourceReclamationAuthorityError(
            "retention candidate accounting differs"
        )
    return len(candidate_hashes), eligible, withheld


def build_fail_closed_reclamation_authority(
    plan: AcquisitionSourceRetentionPlan,
    archive: AcquisitionSourceRetentionArchive,
) -> FailClosedReclamationAuthority:
    """Bind a retention plan and durable archive head to an immutable refusal.

    This builder intentionally has no positive branch.  Even a completely
    eligible and sealed source record remains withheld until every capability
    in :data:`REQUIRED_STORE_CAPABILITIES` and :data:`CROSS_STORE_REQUIREMENTS`
    exists and is independently proven.
    """

    if not isinstance(archive, AcquisitionSourceRetentionArchive):
        raise TypeError("archive must be AcquisitionSourceRetentionArchive")
    candidate_count, eligible, withheld = _validated_plan_counts(plan)
    plan_evidence = archive.validate_plan_evidence(plan)
    if (
        plan_evidence.get("candidate_count") != candidate_count
        or plan_evidence.get("eligible_count") != eligible
        or plan_evidence.get("withheld_count") != withheld
        or plan_evidence.get("source_reclaim_authorized") is not False
    ):
        raise AcquisitionSourceReclamationAuthorityError(
            "retention plan evidence accounting differs"
        )
    archive_audit: Mapping[str, Any] = archive.audit()
    source_binding = _sha(plan.source_binding_sha256, "source binding SHA-256")
    source_view = _sha(plan.source_view_sha256, "source view SHA-256")
    plan_sha = _sha(plan.plan_sha256, "retention plan SHA-256")
    if (
        archive_audit.get("contract") != SOURCE_RETENTION_CONTRACT
        or archive_audit.get("healthy") is not True
        or archive_audit.get("reclaim_enabled") is not False
        or archive_audit.get("source_binding_sha256") != source_binding
    ):
        raise AcquisitionSourceReclamationAuthorityError(
            "sealed archive audit is not a healthy matching fail-closed authority"
        )
    archive_anchor = _sha(
        archive_audit.get("archive_anchor_sha256"),
        "sealed archive anchor SHA-256",
    )
    archived_count = _count(
        archive_audit.get("archived_job_count"),
        "sealed archive record count",
    )
    catalog_sha = capability_catalog_sha256()
    provisional = FailClosedReclamationAuthority(
        authority_sha256="0" * 64,
        source_binding_sha256=source_binding,
        source_view_sha256=source_view,
        retention_plan_sha256=plan_sha,
        sealed_archive_anchor_sha256=archive_anchor,
        candidate_count=candidate_count,
        sealed_export_eligible_count=eligible,
        withheld_count=withheld,
        sealed_archive_record_count=archived_count,
        capability_catalog_sha256=catalog_sha,
    )
    authority = FailClosedReclamationAuthority(
        **{
            **asdict(provisional),
            "authority_sha256": _digest(provisional._core()),
        }
    )
    validate_fail_closed_reclamation_authority(authority)
    return authority


def validate_fail_closed_reclamation_authority(
    authority: FailClosedReclamationAuthority,
) -> None:
    """Reject forged, drifted or accidentally positive authority objects."""

    if not isinstance(authority, FailClosedReclamationAuthority):
        raise TypeError("authority must be FailClosedReclamationAuthority")
    _sha(authority.authority_sha256, "authority SHA-256")
    _sha(authority.source_binding_sha256, "source binding SHA-256")
    _sha(authority.source_view_sha256, "source view SHA-256")
    _sha(authority.retention_plan_sha256, "retention plan SHA-256")
    _sha(
        authority.sealed_archive_anchor_sha256,
        "sealed archive anchor SHA-256",
    )
    for value, label in (
        (authority.candidate_count, "candidate count"),
        (authority.sealed_export_eligible_count, "eligible count"),
        (authority.withheld_count, "withheld count"),
        (authority.sealed_archive_record_count, "archive record count"),
    ):
        _count(value, label)
    if (
        authority.status != STATUS
        or authority.reason_code != REASON_CODE
        or authority.reclaim_authorized is not False
        or authority.source_release_performed is not False
        or authority.capability_catalog_sha256 != capability_catalog_sha256()
        or authority.candidate_count
        != authority.sealed_export_eligible_count + authority.withheld_count
        or authority.authority_sha256 != _digest(authority._core())
    ):
        raise AcquisitionSourceReclamationAuthorityError(
            "reclamation authority differs from the fail-closed contract"
        )
    _canonical(authority.as_dict())


def parse_fail_closed_reclamation_authority(
    payload: bytes,
) -> FailClosedReclamationAuthority:
    """Parse only the exact canonical authority shape and revalidate its hash."""

    if not isinstance(payload, bytes) or not 1 <= len(payload) <= MAX_AUTHORITY_BYTES:
        raise AcquisitionSourceReclamationAuthorityError(
            "reclamation authority payload size is invalid"
        )
    try:
        value = json.loads(payload.decode("ascii"))
    except (UnicodeDecodeError, json.JSONDecodeError) as err:
        raise AcquisitionSourceReclamationAuthorityError(
            "reclamation authority JSON is invalid"
        ) from err
    if not isinstance(value, dict) or payload != _canonical(value):
        raise AcquisitionSourceReclamationAuthorityError(
            "reclamation authority JSON is not canonical"
        )
    expected_keys = {
        "authority_sha256",
        "candidate_count",
        "capability_catalog",
        "capability_catalog_sha256",
        "contract",
        "reason_code",
        "reclaim_authorized",
        "retention_contract",
        "retention_plan_sha256",
        "schema",
        "sealed_archive_anchor_sha256",
        "sealed_archive_record_count",
        "sealed_export_eligible_count",
        "source_binding_sha256",
        "source_release_performed",
        "source_view_sha256",
        "status",
        "withheld_count",
    }
    if (
        set(value) != expected_keys
        or value.get("contract") != CONTRACT
        or value.get("schema") != SCHEMA
        or value.get("retention_contract") != SOURCE_RETENTION_CONTRACT
        or value.get("capability_catalog") != capability_catalog_mapping()
    ):
        raise AcquisitionSourceReclamationAuthorityError(
            "reclamation authority shape or catalogue differs"
        )
    authority = FailClosedReclamationAuthority(
        authority_sha256=value["authority_sha256"],
        source_binding_sha256=value["source_binding_sha256"],
        source_view_sha256=value["source_view_sha256"],
        retention_plan_sha256=value["retention_plan_sha256"],
        sealed_archive_anchor_sha256=value["sealed_archive_anchor_sha256"],
        candidate_count=value["candidate_count"],
        sealed_export_eligible_count=value["sealed_export_eligible_count"],
        withheld_count=value["withheld_count"],
        sealed_archive_record_count=value["sealed_archive_record_count"],
        capability_catalog_sha256=value["capability_catalog_sha256"],
        status=value["status"],
        reason_code=value["reason_code"],
        reclaim_authorized=value["reclaim_authorized"],
        source_release_performed=value["source_release_performed"],
    )
    validate_fail_closed_reclamation_authority(authority)
    return authority


class AcquisitionSourceReclamationCoordinatorError(RuntimeError):
    """A coordinator journal, participant view or fence failed closed."""


class AcquisitionSourceReclamationBusy(
    AcquisitionSourceReclamationCoordinatorError
):
    pass


class AcquisitionSourceReclamationConflict(
    AcquisitionSourceReclamationCoordinatorError
):
    pass


class AcquisitionSourceReclamationIntegrityError(
    AcquisitionSourceReclamationCoordinatorError
):
    pass


class AcquisitionSourceReclamationCapacityError(
    AcquisitionSourceReclamationCoordinatorError
):
    pass


class AcquisitionSourceReclamationDurabilityError(
    AcquisitionSourceReclamationCoordinatorError
):
    pass


class AcquisitionSourceReclamationApplyDisabled(
    AcquisitionSourceReclamationCoordinatorError
):
    """The current schemas cannot safely authorise source mutation."""

    def __init__(self, *, request_sha256: str, operation: str) -> None:
        self.request_sha256 = _sha(request_sha256, "request SHA-256")
        self.operation = _code(operation, "disabled operation")
        self.reason_code = "source_reclamation_apply_hard_disabled"
        self.blockers = APPLY_BLOCKERS
        super().__init__(self.reason_code)

    def as_dict(self) -> dict[str, Any]:
        return {
            "apply_enabled": False,
            "blockers": list(self.blockers),
            "contract": COORDINATOR_CONTRACT,
            "operation": self.operation,
            "reason_code": self.reason_code,
            "request_sha256": self.request_sha256,
            "source_release_performed": False,
        }


def _journal_canonical(value: Any) -> bytes:
    try:
        payload = json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
    except (TypeError, ValueError, RecursionError) as err:
        raise AcquisitionSourceReclamationIntegrityError(
            "reclamation journal value is not canonical"
        ) from err
    if len(payload) > 16 * 1024 * 1024:
        raise AcquisitionSourceReclamationCapacityError(
            "reclamation journal value exceeds its byte bound"
        )
    return payload


def _journal_digest(value: Any) -> str:
    return hashlib.sha256(_journal_canonical(value)).hexdigest()


def _consumer_closure_root(
    receipts: tuple[AcquisitionConsumerClosureReceipt, ...],
) -> str:
    return _journal_digest({
        "receipt_sha256s": [item.receipt_sha256 for item in receipts],
        "schema": CONSUMER_CLOSURE_ROOT_SCHEMA,
    })


def _finite_time(value: object, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"invalid {label}")
    result = float(value)
    if not math.isfinite(result) or not 0.0 <= result <= 2**53:
        raise ValueError(f"invalid {label}")
    return result


def _sqlite_is_full(error: BaseException) -> bool:
    return "full" in str(error).lower()


def _safe_raw_receipt_mapping(receipt: RawArtifactReceipt) -> dict[str, Any]:
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


@dataclass(frozen=True, slots=True)
class ReclamationCoordinatorLimits:
    max_candidates: int = 128
    max_decisions: int = 256
    max_events: int = 4096
    max_participant_prepares: int = 256
    max_database_bytes: int = 64 * 1024 * 1024
    max_participant_database_bytes: int = 16 * 1024 * 1024
    lock_timeout_seconds: float = 5.0

    def __post_init__(self) -> None:
        for value, maximum, label in (
            (self.max_candidates, MAX_COORDINATOR_CANDIDATES, "candidate bound"),
            (self.max_decisions, MAX_COORDINATOR_DECISIONS, "decision bound"),
            (self.max_events, MAX_COORDINATOR_EVENTS, "event bound"),
            (
                self.max_participant_prepares,
                MAX_PARTICIPANT_PREPARES,
                "participant prepare bound",
            ),
            (
                self.max_database_bytes,
                MAX_COORDINATOR_DATABASE_BYTES,
                "coordinator database bound",
            ),
            (
                self.max_participant_database_bytes,
                MAX_PARTICIPANT_DATABASE_BYTES,
                "participant database bound",
            ),
        ):
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or not 1 <= value <= maximum
            ):
                raise ValueError(f"invalid {label}")
        _finite_time(self.lock_timeout_seconds, "lock timeout")
        if self.lock_timeout_seconds <= 0.0 or self.lock_timeout_seconds > 60.0:
            raise ValueError("invalid lock timeout")
        if (
            self.max_database_bytes < 64 * 1024
            or self.max_participant_database_bytes < 64 * 1024
        ):
            raise ValueError("reclamation database bounds are too small")

    @property
    def fingerprint(self) -> str:
        return _journal_digest({
            "contract": COORDINATOR_CONTRACT,
            **asdict(self),
        })


@dataclass(frozen=True, slots=True, repr=False)
class ReclamationPrepareRequest:
    request_sha256: str
    source_binding_sha256: str
    source_view_sha256: str
    retention_plan_sha256: str
    sealed_archive_anchor_sha256: str
    candidate_bindings_sha256: str
    candidate_count: int
    claimed_consumer_registry_root_sha256: str | None = None
    claimed_consumer_epoch_root_sha256: str | None = None

    def _core(self) -> dict[str, Any]:
        return {
            "candidate_bindings_sha256": self.candidate_bindings_sha256,
            "candidate_count": self.candidate_count,
            "claimed_consumer_epoch_root_sha256": (
                self.claimed_consumer_epoch_root_sha256
            ),
            "claimed_consumer_registry_root_sha256": (
                self.claimed_consumer_registry_root_sha256
            ),
            "contract": COORDINATOR_CONTRACT,
            "retention_plan_sha256": self.retention_plan_sha256,
            "sealed_archive_anchor_sha256": (
                self.sealed_archive_anchor_sha256
            ),
            "source_binding_sha256": self.source_binding_sha256,
            "source_view_sha256": self.source_view_sha256,
        }

    def as_dict(self) -> dict[str, Any]:
        return {**self._core(), "request_sha256": self.request_sha256}


def validate_reclamation_prepare_request(
    request: ReclamationPrepareRequest,
) -> None:
    if not isinstance(request, ReclamationPrepareRequest):
        raise TypeError("request must be ReclamationPrepareRequest")
    for value, label in (
        (request.request_sha256, "request SHA-256"),
        (request.source_binding_sha256, "source binding SHA-256"),
        (request.source_view_sha256, "source view SHA-256"),
        (request.retention_plan_sha256, "retention plan SHA-256"),
        (request.sealed_archive_anchor_sha256, "archive anchor SHA-256"),
        (request.candidate_bindings_sha256, "candidate binding SHA-256"),
    ):
        _sha(value, label)
    for value, label in (
        (
            request.claimed_consumer_registry_root_sha256,
            "claimed consumer registry root SHA-256",
        ),
        (
            request.claimed_consumer_epoch_root_sha256,
            "claimed consumer epoch root SHA-256",
        ),
    ):
        if value is not None:
            _sha(value, label)
    if (
        request.claimed_consumer_registry_root_sha256 is None
    ) != (request.claimed_consumer_epoch_root_sha256 is None):
        raise AcquisitionSourceReclamationIntegrityError(
            "claimed consumer registry and epoch roots must be paired"
        )
    if (
        isinstance(request.candidate_count, bool)
        or not isinstance(request.candidate_count, int)
        or not 1 <= request.candidate_count <= MAX_COORDINATOR_CANDIDATES
        or request.request_sha256 != _journal_digest(request._core())
    ):
        raise AcquisitionSourceReclamationIntegrityError(
            "reclamation prepare request checksum differs"
        )


def _request_apply_blockers(
    request: ReclamationPrepareRequest,
) -> tuple[str, ...]:
    validate_reclamation_prepare_request(request)
    if request.claimed_consumer_registry_root_sha256 is None:
        return APPLY_BLOCKERS
    return REGISTRY_VERIFIED_APPLY_BLOCKERS


@dataclass(frozen=True, slots=True, repr=False)
class ReclamationParticipantPrepareReceipt:
    prepare_sha256: str
    request_sha256: str
    role: str
    archived_view_sha256: str
    current_view_sha256: str
    child_reference_root_sha256: str
    candidate_count: int
    releasable_bytes: int
    releasable_entries: int
    prepared_at: float
    consumer_closure_receipts: tuple[
        AcquisitionConsumerClosureReceipt, ...
    ] = field(default_factory=tuple, repr=False)
    apply_enabled: bool = False

    def _core(self) -> dict[str, Any]:
        return {
            "apply_enabled": self.apply_enabled,
            "archived_view_sha256": self.archived_view_sha256,
            "candidate_count": self.candidate_count,
            "child_reference_root_sha256": (
                self.child_reference_root_sha256
            ),
            "consumer_closure_receipts": [
                item.as_dict() for item in self.consumer_closure_receipts
            ],
            "contract": PARTICIPANT_PREPARE_CONTRACT,
            "current_view_sha256": self.current_view_sha256,
            "prepared_at": self.prepared_at,
            "releasable_bytes": self.releasable_bytes,
            "releasable_entries": self.releasable_entries,
            "request_sha256": self.request_sha256,
            "role": self.role,
        }

    def as_dict(self) -> dict[str, Any]:
        return {**self._core(), "prepare_sha256": self.prepare_sha256}


def validate_participant_prepare_receipt(
    receipt: ReclamationParticipantPrepareReceipt,
) -> None:
    if not isinstance(receipt, ReclamationParticipantPrepareReceipt):
        raise TypeError("receipt must be ReclamationParticipantPrepareReceipt")
    for value, label in (
        (receipt.prepare_sha256, "prepare SHA-256"),
        (receipt.request_sha256, "request SHA-256"),
        (receipt.archived_view_sha256, "archived view SHA-256"),
        (receipt.current_view_sha256, "current view SHA-256"),
        (receipt.child_reference_root_sha256, "reference root SHA-256"),
    ):
        _sha(value, label)
    if receipt.role not in PARTICIPANT_ROLES:
        raise AcquisitionSourceReclamationIntegrityError(
            "unknown reclamation participant role"
        )
    for value, label in (
        (receipt.candidate_count, "candidate count"),
        (receipt.releasable_bytes, "releasable bytes"),
        (receipt.releasable_entries, "releasable entries"),
    ):
        _count(value, label)
    _finite_time(receipt.prepared_at, "prepared time")
    closures = receipt.consumer_closure_receipts
    if (
        not isinstance(closures, tuple)
        or any(
            not isinstance(item, AcquisitionConsumerClosureReceipt)
            for item in closures
        )
        or tuple(
            (item.workload_id, item.acquisition_job_id, item.candidate_sha256)
            for item in closures
        )
        != tuple(
            sorted(
                (
                    item.workload_id,
                    item.acquisition_job_id,
                    item.candidate_sha256,
                )
                for item in closures
            )
        )
        or len({item.candidate_sha256 for item in closures}) != len(closures)
        or len({item.acquisition_job_id for item in closures}) != len(closures)
    ):
        raise AcquisitionSourceReclamationIntegrityError(
            "participant consumer closure receipt set differs"
        )
    if receipt.role == "downstream_lifecycle":
        if closures and (
            len(closures) != receipt.candidate_count
            or receipt.child_reference_root_sha256
            != _consumer_closure_root(closures)
        ):
            raise AcquisitionSourceReclamationIntegrityError(
                "downstream consumer closure root differs"
            )
    elif closures:
        raise AcquisitionSourceReclamationIntegrityError(
            "non-downstream participant contains consumer closures"
        )
    if (
        receipt.candidate_count < 1
        or receipt.apply_enabled is not False
        or receipt.archived_view_sha256 != receipt.current_view_sha256
        or receipt.prepare_sha256 != _journal_digest(receipt._core())
    ):
        raise AcquisitionSourceReclamationIntegrityError(
            "participant prepare receipt differs"
        )


@dataclass(frozen=True, slots=True)
class ReclamationPrepareOutcome:
    request_sha256: str
    state: str
    decision_sha256: str
    participant_root_sha256: str
    candidate_count: int
    owner_epoch: int
    consumer_closure_root_sha256: str | None = None
    blockers: tuple[str, ...] = APPLY_BLOCKERS
    reclaim_authorized: bool = False
    source_release_performed: bool = False

    def as_dict(self) -> dict[str, Any]:
        return {
            "blockers": list(self.blockers),
            "candidate_count": self.candidate_count,
            "contract": COORDINATOR_CONTRACT,
            "consumer_closure_root_sha256": (
                self.consumer_closure_root_sha256
            ),
            "decision_sha256": self.decision_sha256,
            "owner_epoch": self.owner_epoch,
            "participant_root_sha256": self.participant_root_sha256,
            "reclaim_authorized": self.reclaim_authorized,
            "request_sha256": self.request_sha256,
            "source_release_performed": self.source_release_performed,
            "state": self.state,
        }


@dataclass(frozen=True, slots=True)
class _ParticipantView:
    archived_view_sha256: str
    current_view_sha256: str
    child_reference_root_sha256: str
    releasable_bytes: int = 0
    releasable_entries: int = 0
    consumer_closure_receipts: tuple[
        AcquisitionConsumerClosureReceipt, ...
    ] = field(default_factory=tuple, repr=False)


_RECLAMATION_PROCESS_LOCKS_GUARD = threading.Lock()
_RECLAMATION_PROCESS_LOCKS: dict[str, threading.Lock] = {}


def _reclamation_process_lock(path: Path) -> threading.Lock:
    key = os.path.normcase(str(path))
    with _RECLAMATION_PROCESS_LOCKS_GUARD:
        return _RECLAMATION_PROCESS_LOCKS.setdefault(key, threading.Lock())


def _lock_file(descriptor: int) -> None:
    os.lseek(descriptor, 0, os.SEEK_SET)
    if os.name == "nt":
        import msvcrt

        msvcrt.locking(descriptor, msvcrt.LK_NBLCK, 1)
    else:
        import fcntl

        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)


def _unlock_file(descriptor: int) -> None:
    os.lseek(descriptor, 0, os.SEEK_SET)
    if os.name == "nt":
        import msvcrt

        msvcrt.locking(descriptor, msvcrt.LK_UNLCK, 1)
    else:
        import fcntl

        fcntl.flock(descriptor, fcntl.LOCK_UN)


class _ParticipantPrepareJournal:
    """Bounded sidecar journal; it never changes its participant source."""

    _TABLES = frozenset({"participant_meta", "participant_prepares"})
    _COLUMNS = {
        "participant_meta": frozenset({
            "singleton",
            "schema_version",
            "role",
            "limits_sha256",
            "last_observed_at",
        }),
        "participant_prepares": frozenset({
            "request_sha256",
            "prepare_sha256",
            "payload_json",
            "prepared_at",
        }),
    }

    def __init__(
        self,
        path: Path,
        *,
        role: str,
        limits: ReclamationCoordinatorLimits,
        clock: Callable[[], float],
        directory_fsync: Callable[[Path], None],
        fault_injector: Callable[[str], None] | None,
    ) -> None:
        if not path.is_absolute():
            raise ValueError("participant journal path must be absolute")
        if role not in PARTICIPANT_ROLES:
            raise ValueError("unknown participant role")
        self.path = path
        self.role = role
        self._limits = limits
        self._clock = clock
        self._directory_fsync = directory_fsync
        self._fault_injector = fault_injector
        self._lock = threading.RLock()
        self.initialize()

    def _fault(self, point: str) -> None:
        if self._fault_injector is not None:
            self._fault_injector(f"participant.{self.role}.{point}")

    def _connect(self) -> sqlite3.Connection:
        database = sqlite3.connect(self.path, timeout=2.0)
        try:
            database.row_factory = sqlite3.Row
            database.execute("PRAGMA foreign_keys=ON")
            database.execute("PRAGMA synchronous=FULL")
            mode = str(
                database.execute("PRAGMA journal_mode=DELETE").fetchone()[0]
            )
            if mode.lower() != "delete":
                raise AcquisitionSourceReclamationDurabilityError(
                    "participant journal mode differs"
                )
            page_size = int(database.execute("PRAGMA page_size").fetchone()[0])
            max_pages = self._limits.max_participant_database_bytes // page_size
            database.execute(f"PRAGMA max_page_count={max_pages}")
            if int(database.execute("PRAGMA page_count").fetchone()[0]) > max_pages:
                raise AcquisitionSourceReclamationCapacityError(
                    "participant prepare database bound exceeded"
                )
            database.execute("PRAGMA temp_store=MEMORY")
            return database
        except BaseException:
            database.close()
            raise

    @staticmethod
    def _tables(database: sqlite3.Connection) -> frozenset[str]:
        return frozenset(
            str(row[0])
            for row in database.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name NOT LIKE 'sqlite_%'"
            )
        )

    def _validate_schema(self, database: sqlite3.Connection) -> sqlite3.Row:
        if self._tables(database) != self._TABLES:
            raise AcquisitionSourceReclamationIntegrityError(
                "participant journal schema differs"
            )
        for table, expected in self._COLUMNS.items():
            columns = frozenset(
                str(row[1])
                for row in database.execute(f"PRAGMA table_info({table})")
            )
            if columns != expected:
                raise AcquisitionSourceReclamationIntegrityError(
                    "participant journal columns differ"
                )
        if tuple(database.execute("PRAGMA integrity_check").fetchone()) != (
            "ok",
        ):
            raise AcquisitionSourceReclamationIntegrityError(
                "participant journal integrity check failed"
            )
        row = database.execute(
            "SELECT * FROM participant_meta WHERE singleton=1"
        ).fetchone()
        if (
            row is None
            or int(row["schema_version"]) != COORDINATOR_SCHEMA_VERSION
            or str(row["role"]) != self.role
            or str(row["limits_sha256"]) != self._limits.fingerprint
        ):
            raise AcquisitionSourceReclamationIntegrityError(
                "participant journal metadata differs"
            )
        return row

    def _physical_bytes(self) -> int:
        return sum(
            item.stat().st_size
            for item in self.path.parent.iterdir()
            if item.is_file()
            and not item.is_symlink()
            and item.name.startswith(self.path.name)
        )

    def _barrier(self) -> None:
        try:
            descriptor = os.open(str(self.path), os.O_RDWR)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            self._directory_fsync(self.path.parent)
        except OSError as err:
            if err.errno == errno.ENOSPC:
                raise AcquisitionSourceReclamationCapacityError(
                    "participant prepare filesystem full"
                ) from err
            raise AcquisitionSourceReclamationDurabilityError(
                "participant prepare durability barrier failed"
            ) from err

    def initialize(self) -> None:
        with self._lock:
            if self.path.exists() and self.path.is_symlink():
                raise AcquisitionSourceReclamationIntegrityError(
                    "participant journal is a symlink"
                )
            created_parent = not self.path.parent.exists()
            self.path.parent.mkdir(parents=True, exist_ok=True)
            if created_parent:
                self._directory_fsync(self.path.parent.parent)
            with closing(self._connect()) as database:
                try:
                    database.execute("BEGIN IMMEDIATE")
                    tables = self._tables(database)
                    if not tables:
                        database.executescript(
                            """
                            CREATE TABLE participant_meta(
                                singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                                schema_version INTEGER NOT NULL,
                                role TEXT NOT NULL,
                                limits_sha256 TEXT NOT NULL,
                                last_observed_at REAL
                            ) WITHOUT ROWID;
                            CREATE TABLE participant_prepares(
                                request_sha256 TEXT PRIMARY KEY,
                                prepare_sha256 TEXT NOT NULL UNIQUE,
                                payload_json BLOB NOT NULL,
                                prepared_at REAL NOT NULL
                            ) WITHOUT ROWID;
                            """
                        )
                        database.execute(
                            "INSERT INTO participant_meta VALUES(1,?,?,?,NULL)",
                            (
                                COORDINATOR_SCHEMA_VERSION,
                                self.role,
                                self._limits.fingerprint,
                            ),
                        )
                    self._validate_schema(database)
                    database.commit()
                except sqlite3.OperationalError as err:
                    if database.in_transaction:
                        database.rollback()
                    if _sqlite_is_full(err):
                        raise AcquisitionSourceReclamationCapacityError(
                            "participant prepare database full"
                        ) from err
                    raise
                except BaseException:
                    if database.in_transaction:
                        database.rollback()
                    raise
            self._barrier()

    @staticmethod
    def _decode_receipt(payload: bytes) -> ReclamationParticipantPrepareReceipt:
        try:
            value = json.loads(payload.decode("ascii"))
        except (UnicodeDecodeError, json.JSONDecodeError) as err:
            raise AcquisitionSourceReclamationIntegrityError(
                "participant prepare receipt is corrupt"
            ) from err
        if not isinstance(value, dict) or payload != _journal_canonical(value):
            raise AcquisitionSourceReclamationIntegrityError(
                "participant prepare receipt is not canonical"
            )
        expected = {
            "apply_enabled",
            "archived_view_sha256",
            "candidate_count",
            "child_reference_root_sha256",
            "consumer_closure_receipts",
            "contract",
            "current_view_sha256",
            "prepare_sha256",
            "prepared_at",
            "releasable_bytes",
            "releasable_entries",
            "request_sha256",
            "role",
        }
        if set(value) != expected or value.get("contract") != PARTICIPANT_PREPARE_CONTRACT:
            raise AcquisitionSourceReclamationIntegrityError(
                "participant prepare receipt shape differs"
            )
        closure_values = value["consumer_closure_receipts"]
        if not isinstance(closure_values, list):
            raise AcquisitionSourceReclamationIntegrityError(
                "participant consumer closure receipt set differs"
            )
        try:
            closures = tuple(
                parse_consumer_closure_receipt(item)
                for item in closure_values
            )
        except AcquisitionConsumerRegistryError:
            raise AcquisitionSourceReclamationIntegrityError(
                "participant consumer closure receipt is invalid"
            ) from None
        receipt = ReclamationParticipantPrepareReceipt(
            prepare_sha256=value["prepare_sha256"],
            request_sha256=value["request_sha256"],
            role=value["role"],
            archived_view_sha256=value["archived_view_sha256"],
            current_view_sha256=value["current_view_sha256"],
            child_reference_root_sha256=value[
                "child_reference_root_sha256"
            ],
            candidate_count=value["candidate_count"],
            releasable_bytes=value["releasable_bytes"],
            releasable_entries=value["releasable_entries"],
            prepared_at=value["prepared_at"],
            consumer_closure_receipts=closures,
            apply_enabled=value["apply_enabled"],
        )
        validate_participant_prepare_receipt(receipt)
        return receipt

    def load(
        self, request_sha256: str
    ) -> ReclamationParticipantPrepareReceipt | None:
        request = _sha(request_sha256, "request SHA-256")
        with self._lock, closing(self._connect()) as database:
            self._validate_schema(database)
            row = database.execute(
                "SELECT payload_json FROM participant_prepares "
                "WHERE request_sha256=?",
                (request,),
            ).fetchone()
        return None if row is None else self._decode_receipt(bytes(row[0]))

    def prepare(
        self,
        request: ReclamationPrepareRequest,
        *,
        view_builder: Callable[[], _ParticipantView],
    ) -> ReclamationParticipantPrepareReceipt:
        validate_reclamation_prepare_request(request)
        before = view_builder()
        if before.archived_view_sha256 != before.current_view_sha256:
            raise AcquisitionSourceReclamationConflict(
                f"{self.role}_participant_view_cas_conflict"
            )
        with self._lock, closing(self._connect()) as database:
            try:
                database.execute("BEGIN IMMEDIATE")
                meta = self._validate_schema(database)
                observed_at = _finite_time(self._clock(), "participant clock")
                last = meta["last_observed_at"]
                if last is not None and observed_at < float(last):
                    raise AcquisitionSourceReclamationConflict(
                        "participant_clock_rollback"
                    )
                row = database.execute(
                    "SELECT payload_json FROM participant_prepares "
                    "WHERE request_sha256=?",
                    (request.request_sha256,),
                ).fetchone()
                if row is not None:
                    existing = self._decode_receipt(bytes(row[0]))
                    if (
                        existing.role != self.role
                        or existing.candidate_count != request.candidate_count
                        or existing.archived_view_sha256
                        != before.archived_view_sha256
                        or existing.current_view_sha256
                        != before.current_view_sha256
                        or existing.child_reference_root_sha256
                        != before.child_reference_root_sha256
                        or existing.releasable_bytes != before.releasable_bytes
                        or existing.releasable_entries
                        != before.releasable_entries
                        or existing.consumer_closure_receipts
                        != before.consumer_closure_receipts
                    ):
                        raise AcquisitionSourceReclamationConflict(
                            "participant_prepare_identity_reused"
                        )
                    database.commit()
                    return existing
                count = int(
                    database.execute(
                        "SELECT count(*) FROM participant_prepares"
                    ).fetchone()[0]
                )
                if count >= self._limits.max_participant_prepares:
                    raise AcquisitionSourceReclamationCapacityError(
                        "participant prepare retention bound reached"
                    )
                if (
                    self._physical_bytes()
                    >= self._limits.max_participant_database_bytes
                ):
                    raise AcquisitionSourceReclamationCapacityError(
                        "participant prepare database bound reached"
                    )
                during = view_builder()
                if during != before:
                    raise AcquisitionSourceReclamationConflict(
                        f"{self.role}_participant_view_changed"
                    )
                provisional = ReclamationParticipantPrepareReceipt(
                    prepare_sha256="0" * 64,
                    request_sha256=request.request_sha256,
                    role=self.role,
                    archived_view_sha256=before.archived_view_sha256,
                    current_view_sha256=before.current_view_sha256,
                    child_reference_root_sha256=(
                        before.child_reference_root_sha256
                    ),
                    candidate_count=request.candidate_count,
                    releasable_bytes=before.releasable_bytes,
                    releasable_entries=before.releasable_entries,
                    prepared_at=observed_at,
                    consumer_closure_receipts=(
                        before.consumer_closure_receipts
                    ),
                )
                receipt = replace(
                    provisional,
                    prepare_sha256=_journal_digest(provisional._core()),
                )
                validate_participant_prepare_receipt(receipt)
                payload = _journal_canonical(receipt.as_dict())
                self._fault("before_insert")
                database.execute(
                    "INSERT INTO participant_prepares VALUES(?,?,?,?)",
                    (
                        request.request_sha256,
                        receipt.prepare_sha256,
                        payload,
                        observed_at,
                    ),
                )
                database.execute(
                    "UPDATE participant_meta SET last_observed_at=? "
                    "WHERE singleton=1",
                    (observed_at,),
                )
                self._fault("before_commit")
                database.commit()
                self._fault("after_commit")
            except sqlite3.OperationalError as err:
                if database.in_transaction:
                    database.rollback()
                if "full" in str(err).lower():
                    raise AcquisitionSourceReclamationCapacityError(
                        "participant prepare database full"
                    ) from err
                raise
            except BaseException:
                if database.in_transaction:
                    database.rollback()
                raise
        self._barrier()
        after = view_builder()
        if after != before:
            raise AcquisitionSourceReclamationConflict(
                f"{self.role}_participant_view_changed_after_prepare"
            )
        return receipt

    def audit(self) -> dict[str, Any]:
        with self._lock, closing(self._connect()) as database:
            meta = self._validate_schema(database)
            rows = database.execute(
                "SELECT request_sha256,prepare_sha256,payload_json,prepared_at "
                "FROM participant_prepares ORDER BY request_sha256"
            ).fetchall()
            if len(rows) > self._limits.max_participant_prepares:
                raise AcquisitionSourceReclamationCapacityError(
                    "participant prepare retention bound exceeded"
                )
            for row in rows:
                receipt = self._decode_receipt(bytes(row["payload_json"]))
                if (
                    receipt.request_sha256 != str(row["request_sha256"])
                    or receipt.prepare_sha256 != str(row["prepare_sha256"])
                    or receipt.role != self.role
                    or receipt.prepared_at != float(row["prepared_at"])
                ):
                    raise AcquisitionSourceReclamationIntegrityError(
                        "participant prepare index differs"
                    )
            last = meta["last_observed_at"]
            if rows and (
                last is None
                or float(last) != max(float(row["prepared_at"]) for row in rows)
            ):
                raise AcquisitionSourceReclamationIntegrityError(
                    "participant prepare clock head differs"
                )
            database_bytes = self._physical_bytes()
            if database_bytes > self._limits.max_participant_database_bytes:
                raise AcquisitionSourceReclamationCapacityError(
                    "participant prepare database bound exceeded"
                )
        return {
            "apply_enabled": False,
            "database_bytes": database_bytes,
            "last_observed_at": meta["last_observed_at"],
            "prepare_count": len(rows),
            "role": self.role,
        }


def _record_candidate(record: Mapping[str, Any]) -> Mapping[str, Any]:
    candidate = record.get("candidate")
    if not isinstance(candidate, dict):
        raise AcquisitionSourceReclamationIntegrityError(
            "sealed candidate mapping is unavailable"
        )
    return candidate


def _raw_participant_view(
    raw: RawArtifactStore,
    records: tuple[dict[str, Any], ...],
) -> _ParticipantView:
    archived: list[dict[str, Any]] = []
    current: list[dict[str, Any]] = []
    candidate_capture_ids: set[str] = set()
    candidate_receipt_ids: set[str] = set()
    for record in records:
        candidate = _record_candidate(record)
        capture_id = str(candidate.get("raw_capture_id"))
        if capture_id in candidate_capture_ids:
            raise AcquisitionSourceReclamationIntegrityError(
                "reclamation candidate repeats one raw capture"
            )
        candidate_capture_ids.add(capture_id)
        expected_capture = record.get("raw_capture")
        expected_receipts = record.get("raw_receipts")
        expected_objects = record.get("raw_objects")
        if not all(
            isinstance(item, dict)
            for item in (expected_capture, expected_receipts, expected_objects)
        ):
            raise AcquisitionSourceReclamationIntegrityError(
                "sealed raw participant view is incomplete"
            )
        bundle = raw.load_capture_bundle_bytes(capture_id)
        if bundle is None:
            raise AcquisitionSourceReclamationConflict(
                "raw_capture_missing_during_prepare"
            )
        capture, receipts, payloads = bundle
        normalized_objects: dict[str, Any] = {}
        for role in sorted(payloads):
            payload = payloads[role]
            expected_object = expected_objects.get(role)
            if not isinstance(expected_object, dict):
                raise AcquisitionSourceReclamationIntegrityError(
                    "sealed raw object role differs"
                )
            try:
                sealed_payload = base64.b64decode(
                    expected_object.get("payload_base64"), validate=True
                )
            except (TypeError, ValueError) as err:
                raise AcquisitionSourceReclamationIntegrityError(
                    "sealed raw object encoding differs"
                ) from err
            if sealed_payload != payload:
                raise AcquisitionSourceReclamationConflict(
                    "sealed_raw_bytes_differ_from_source"
                )
            normalized_objects[role] = {
                "content_sha256": hashlib.sha256(payload).hexdigest(),
                "size_bytes": len(payload),
            }
            candidate_receipt_ids.add(receipts[role].receipt_id)
        expected_normalized_objects = {
            role: {
                "content_sha256": value.get("content_sha256"),
                "size_bytes": value.get("size_bytes"),
            }
            for role, value in sorted(expected_objects.items())
            if isinstance(value, dict)
        }
        expected_view = {
            "candidate_sha256": record.get("candidate_sha256"),
            "raw_capture": expected_capture,
            "raw_objects": expected_normalized_objects,
            "raw_receipts": expected_receipts,
        }
        current_view = {
            "candidate_sha256": record.get("candidate_sha256"),
            "raw_capture": capture.to_metadata(),
            "raw_objects": normalized_objects,
            "raw_receipts": {
                role: _safe_raw_receipt_mapping(receipt)
                for role, receipt in sorted(receipts.items())
            },
        }
        archived.append(expected_view)
        current.append(current_view)
    captures = raw.accounting_captures()
    receipts = raw.accounting_receipts()
    capture_references: dict[str, set[str]] = {}
    for capture in captures:
        for member in capture.members:
            capture_references.setdefault(member.receipt_id, set()).add(
                capture.capture_id
            )
    receipt_content = {
        receipt.receipt_id: receipt.content_sha256 for receipt in receipts
    }
    content_receipts: dict[str, set[str]] = {}
    content_sizes: dict[str, int] = {}
    for receipt in receipts:
        content_receipts.setdefault(receipt.content_sha256, set()).add(
            receipt.receipt_id
        )
        content_sizes.setdefault(receipt.content_sha256, receipt.size_bytes)
    prunable_receipts = {
        receipt_id
        for receipt_id in candidate_receipt_ids
        if not (
            capture_references.get(receipt_id, set()) - candidate_capture_ids
        )
    }
    prunable_objects = {
        content
        for content, receipt_ids in content_receipts.items()
        if receipt_ids and receipt_ids <= prunable_receipts
    }
    reference_view = {
        "apply_order": [
            "capture_members",
            "captures",
            "receipts_at_zero_capture_references",
            "objects_at_zero_receipt_references",
        ],
        "candidate_capture_ids_sha256": _journal_digest(
            sorted(candidate_capture_ids)
        ),
        "candidate_receipt_ids_sha256": _journal_digest(
            sorted(candidate_receipt_ids)
        ),
        "prunable_object_content_sha256s": sorted(prunable_objects),
        "prunable_receipt_ids_sha256": _journal_digest(
            sorted(prunable_receipts)
        ),
        "remaining_capture_reference_counts": {
            _journal_digest(receipt_id): len(
                capture_references.get(receipt_id, set())
                - candidate_capture_ids
            )
            for receipt_id in sorted(candidate_receipt_ids)
        },
        "schema": "stv3-worker-raw-reference-closure-prepare-v1",
    }
    return _ParticipantView(
        archived_view_sha256=_journal_digest(archived),
        current_view_sha256=_journal_digest(current),
        child_reference_root_sha256=_journal_digest(reference_view),
        releasable_bytes=sum(content_sizes[item] for item in prunable_objects),
        releasable_entries=len(prunable_receipts),
    )


def _scheduler_participant_view(
    scheduler: AutonomousScheduler,
    records: tuple[dict[str, Any], ...],
) -> _ParticipantView:
    archived = []
    current = []
    for record in records:
        candidate = _record_candidate(record)
        proof = scheduler.terminal_proof(str(candidate.get("scheduler_job_id")))
        if proof is None:
            raise AcquisitionSourceReclamationConflict(
                "scheduler_terminal_missing_during_prepare"
            )
        archived.append({
            "candidate_sha256": record.get("candidate_sha256"),
            "scheduler_terminal": record.get("scheduler_terminal"),
        })
        current.append({
            "candidate_sha256": record.get("candidate_sha256"),
            "scheduler_terminal": {
                **proof.as_dict(),
                "proof_sha256": proof.proof_sha256,
            },
        })
    root = _journal_digest({
        "candidate_sha256s": [item.get("candidate_sha256") for item in records],
        "role": "scheduler",
        "terminal_only": True,
    })
    return _ParticipantView(
        _journal_digest(archived), _journal_digest(current), root
    )


def _execution_participant_view(
    executions: ExecutionReceiptStore,
    records: tuple[dict[str, Any], ...],
) -> _ParticipantView:
    archived = []
    current = []
    for record in records:
        candidate = _record_candidate(record)
        history = executions.load_job_receipts(
            str(candidate.get("acquisition_job_id"))
        )
        archived.append({
            "candidate_sha256": record.get("candidate_sha256"),
            "execution_history": record.get("execution_history"),
        })
        current.append({
            "candidate_sha256": record.get("candidate_sha256"),
            "execution_history": [item.as_dict() for item in history],
        })
    root = _journal_digest({
        "candidate_sha256s": [item.get("candidate_sha256") for item in records],
        "role": "execution_receipts",
        "terminal_head_and_history_together": True,
    })
    return _ParticipantView(
        _journal_digest(archived), _journal_digest(current), root
    )


def _capacity_participant_view(
    capacity: RawCapacityLedger,
    records: tuple[dict[str, Any], ...],
) -> _ParticipantView:
    archived = []
    current = []
    byte_count = 0
    entry_count = 0
    for record in records:
        candidate = _record_candidate(record)
        expected = record.get("capacity_commit")
        raw_capture = record.get("raw_capture")
        if not isinstance(expected, dict) or not isinstance(raw_capture, dict):
            raise AcquisitionSourceReclamationIntegrityError(
                "sealed capacity participant view is incomplete"
            )
        reservation = capacity.load_for_job(
            provider=str(raw_capture.get("provider")),
            job_identity=str(candidate.get("acquisition_job_id")),
        )
        if reservation is None:
            raise AcquisitionSourceReclamationConflict(
                "capacity_commit_missing_during_prepare"
            )
        archived.append({
            "candidate_sha256": record.get("candidate_sha256"),
            "capacity_commit": expected,
        })
        current.append({
            "candidate_sha256": record.get("candidate_sha256"),
            "capacity_commit": reservation.to_mapping(),
        })
        byte_count += int(reservation.actual_bytes or 0)
        entry_count += int(reservation.actual_entries or 0)
    root = _journal_digest({
        "candidate_sha256s": [item.get("candidate_sha256") for item in records],
        "dependency": "raw_artifacts_APPLIED",
        "role": "capacity_ledger",
    })
    return _ParticipantView(
        _journal_digest(archived),
        _journal_digest(current),
        root,
        releasable_bytes=byte_count,
        releasable_entries=entry_count,
    )


def _evidence_participant_view(
    evidence: AcquisitionEvidenceLifecycleStore,
    records: tuple[dict[str, Any], ...],
    *,
    request: ReclamationPrepareRequest,
    archive_bindings: tuple[dict[str, str], ...],
    registry_verifier: AcquisitionConsumerRegistryBoundEpochVerifier | None,
) -> _ParticipantView:
    archived = []
    current = []
    closures: list[AcquisitionConsumerClosureReceipt] = []
    binding_index = {
        item.get("candidate_sha256"): item for item in archive_bindings
    }
    if (
        len(binding_index) != len(archive_bindings)
        or len(archive_bindings) != len(records)
    ):
        raise AcquisitionSourceReclamationIntegrityError(
            "sealed source archive binding inventory differs"
        )
    registry_verification = None
    if registry_verifier is not None:
        try:
            registry_verification = registry_verifier.verify_held()
        except AcquisitionConsumerRegistryError:
            raise AcquisitionSourceReclamationConflict(
                "complete_consumer_registry_epoch_root_authority_unavailable"
            ) from None
    for record in records:
        candidate = _record_candidate(record)
        terminal = evidence.load_terminal_consumption(
            str(candidate.get("evidence_stream_id")),
            int(candidate.get("evidence_stream_sequence")),
        )
        if terminal is None:
            raise AcquisitionSourceReclamationConflict(
                "downstream_terminal_missing_during_prepare"
            )
        terminal_identity = evidence.terminal_event_identity(
            str(candidate.get("evidence_stream_id")),
            int(candidate.get("evidence_stream_sequence")),
        )
        if terminal_identity is None:
            raise AcquisitionSourceReclamationConflict(
                "downstream_terminal_identity_missing_during_prepare"
            )
        if (
            terminal.status != terminal_identity.status
            or terminal.archive_segment_id
            != terminal_identity.archive_segment_id
        ):
            raise AcquisitionSourceReclamationIntegrityError(
                "downstream terminal storage identity differs"
            )
        archived.append({
            "candidate_sha256": record.get("candidate_sha256"),
            "downstream": record.get("downstream"),
        })
        current.append({
            "candidate_sha256": record.get("candidate_sha256"),
            "downstream": {
                "archive_segment_id": terminal.archive_segment_id,
                "consumption_receipt": terminal.consumption_receipt.as_dict(),
                "producer_receipt": terminal.producer_receipt.as_dict(),
                "proof_sha256": _journal_digest(terminal.proof),
                "storage_status": terminal.status,
            },
        })
        if registry_verifier is not None:
            assert registry_verification is not None
            candidate_sha = _sha(
                record.get("candidate_sha256"),
                "sealed candidate SHA-256",
            )
            binding = binding_index.get(candidate_sha)
            if (
                binding is None
                or binding.get("record_sha256")
                != record.get("record_sha256")
            ):
                raise AcquisitionSourceReclamationIntegrityError(
                    "sealed source archive binding differs"
                )
            workload_id = str(candidate.get("evidence_stream_id"))
            member = registry_verifier.registry_root.by_workload.get(workload_id)
            if member is None:
                raise AcquisitionSourceReclamationConflict(
                    "consumer closure workload is not registered"
                )
            try:
                closures.append(
                    AcquisitionConsumerClosureReceipt.create(
                        candidate_sha256=candidate_sha,
                        acquisition_job_id=str(
                            candidate.get("acquisition_job_id")
                        ),
                        member=member,
                        verification=registry_verification,
                        producer_receipt=terminal.producer_receipt,
                        consumption_receipt=terminal.consumption_receipt,
                        terminal_identity=terminal_identity,
                        source_archive_anchor_sha256=(
                            request.sealed_archive_anchor_sha256
                        ),
                        source_archive_segment_sha256=_sha(
                            binding.get("segment_sha256"),
                            "sealed source archive segment SHA-256",
                        ),
                        source_archive_record_sha256=_sha(
                            binding.get("record_sha256"),
                            "sealed source archive record SHA-256",
                        ),
                    )
                )
            except AcquisitionConsumerRegistryError:
                raise AcquisitionSourceReclamationConflict(
                    "complete_consumer_registry_epoch_root_authority_unavailable"
                ) from None
    closure_receipts = tuple(
        sorted(
            closures,
            key=lambda item: (
                item.workload_id,
                item.acquisition_job_id,
                item.candidate_sha256,
            ),
        )
    )
    if registry_verifier is None:
        root = _journal_digest({
            "candidate_sha256s": [
                item.get("candidate_sha256") for item in records
            ],
            "complete_consumer_registry_verified": False,
            "role": "downstream_lifecycle",
        })
    else:
        if len(closure_receipts) != len(records):
            raise AcquisitionSourceReclamationIntegrityError(
                "consumer closure receipt inventory differs"
            )
        root = _consumer_closure_root(closure_receipts)
    return _ParticipantView(
        _journal_digest(archived),
        _journal_digest(current),
        root,
        consumer_closure_receipts=closure_receipts,
    )


class ExactArchivedReclamationResolver:
    """Resolve one fully verified sealed record without consulting hot rows."""

    def __init__(
        self,
        archive: AcquisitionSourceRetentionArchive,
        *,
        expected_anchor_sha256: str,
    ) -> None:
        if not isinstance(archive, AcquisitionSourceRetentionArchive):
            raise TypeError("archive must be AcquisitionSourceRetentionArchive")
        self._archive = archive
        self._expected_anchor = _sha(
            expected_anchor_sha256, "expected archive anchor SHA-256"
        )

    def _record(self, candidate_sha256: str) -> dict[str, Any]:
        candidate = _sha(candidate_sha256, "candidate SHA-256")
        audit = self._archive.audit()
        if (
            audit.get("healthy") is not True
            or audit.get("archive_anchor_sha256") != self._expected_anchor
        ):
            raise AcquisitionSourceReclamationConflict(
                "sealed_archive_anchor_cas_conflict"
            )
        lookup = self._archive.lookup(candidate)
        if (
            lookup.status != "archived"
            or lookup.record is None
            or lookup.record.get("candidate_sha256") != candidate
        ):
            raise AcquisitionSourceReclamationConflict(
                "sealed_archive_candidate_unavailable"
            )
        return lookup.record

    @staticmethod
    def _copy(value: Any) -> Any:
        return json.loads(
            json.dumps(
                value,
                allow_nan=False,
                ensure_ascii=True,
                separators=(",", ":"),
                sort_keys=True,
            )
        )

    def resolve_archived_capacity_commit_v1(
        self, candidate_sha256: str
    ) -> dict[str, Any]:
        value = self._record(candidate_sha256).get("capacity_commit")
        if not isinstance(value, dict):
            raise AcquisitionSourceReclamationIntegrityError(
                "archived capacity commit is unavailable"
            )
        return self._copy(value)

    def resolve_archived_source_reference_closure_v1(
        self, candidate_sha256: str
    ) -> dict[str, Any]:
        value = self._record(candidate_sha256).get("downstream")
        if not isinstance(value, dict):
            raise AcquisitionSourceReclamationIntegrityError(
                "archived downstream closure is unavailable"
            )
        return self._copy(value)

    def resolve_archived_execution_history_v1(
        self, candidate_sha256: str
    ) -> tuple[dict[str, Any], ...]:
        value = self._record(candidate_sha256).get("execution_history")
        if not isinstance(value, list) or not value:
            raise AcquisitionSourceReclamationIntegrityError(
                "archived execution history is unavailable"
            )
        copied = self._copy(value)
        return tuple(copied)

    def resolve_archived_capture_receipt_object_v1(
        self, candidate_sha256: str
    ) -> dict[str, Any]:
        record = self._record(candidate_sha256)
        value = {
            "raw_capture": record.get("raw_capture"),
            "raw_objects": record.get("raw_objects"),
            "raw_receipts": record.get("raw_receipts"),
        }
        if not all(isinstance(item, dict) for item in value.values()):
            raise AcquisitionSourceReclamationIntegrityError(
                "archived raw capture bundle is unavailable"
            )
        return self._copy(value)

    def resolve_archived_scheduler_terminal_v1(
        self, candidate_sha256: str
    ) -> dict[str, Any]:
        value = self._record(candidate_sha256).get("scheduler_terminal")
        if not isinstance(value, dict):
            raise AcquisitionSourceReclamationIntegrityError(
                "archived scheduler terminal is unavailable"
            )
        return self._copy(value)


class ReclamationParticipantPreparer:
    """Five source-specific prepare APIs backed by bounded sidecar journals.

    The sidecars make prepare receipts restart-durable without changing any
    current source schema.  They are evidence-only compare-and-swap receipts,
    not source-local commit or deletion tokens.
    """

    def __init__(
        self,
        root: Path,
        *,
        raw_store: RawArtifactStore,
        scheduler: AutonomousScheduler,
        execution_store: ExecutionReceiptStore,
        capacity_ledger: RawCapacityLedger,
        evidence_store: AcquisitionEvidenceLifecycleStore,
        consumer_registry_verifier: (
            AcquisitionConsumerRegistryBoundEpochVerifier | None
        ),
        limits: ReclamationCoordinatorLimits,
        clock: Callable[[], float],
        directory_fsync: Callable[[Path], None],
        fault_injector: Callable[[str], None] | None,
    ) -> None:
        self._raw = raw_store
        self._scheduler = scheduler
        self._executions = execution_store
        self._capacity = capacity_ledger
        self._evidence = evidence_store
        self._consumer_registry_verifier = consumer_registry_verifier
        self._journals = {
            role: _ParticipantPrepareJournal(
                root / f"{role}.sqlite3",
                role=role,
                limits=limits,
                clock=clock,
                directory_fsync=directory_fsync,
                fault_injector=fault_injector,
            )
            for role in PARTICIPANT_ROLES
        }

    def prepare_committed_capacity_release_cas_v1(
        self,
        request: ReclamationPrepareRequest,
        records: tuple[dict[str, Any], ...],
    ) -> ReclamationParticipantPrepareReceipt:
        return self._journals["capacity_ledger"].prepare(
            request,
            view_builder=lambda: _capacity_participant_view(
                self._capacity, records
            ),
        )

    def prepare_complete_source_reference_closure_cas_v1(
        self,
        request: ReclamationPrepareRequest,
        records: tuple[dict[str, Any], ...],
        *,
        archive_bindings: tuple[dict[str, str], ...],
    ) -> ReclamationParticipantPrepareReceipt:
        return self._journals["downstream_lifecycle"].prepare(
            request,
            view_builder=lambda: _evidence_participant_view(
                self._evidence,
                records,
                request=request,
                archive_bindings=archive_bindings,
                registry_verifier=self._consumer_registry_verifier,
            ),
        )

    def prepare_execution_history_prune_cas_v1(
        self,
        request: ReclamationPrepareRequest,
        records: tuple[dict[str, Any], ...],
    ) -> ReclamationParticipantPrepareReceipt:
        return self._journals["execution_receipts"].prepare(
            request,
            view_builder=lambda: _execution_participant_view(
                self._executions, records
            ),
        )

    def prepare_reference_counted_capture_prune_cas_v1(
        self,
        request: ReclamationPrepareRequest,
        records: tuple[dict[str, Any], ...],
    ) -> ReclamationParticipantPrepareReceipt:
        return self._journals["raw_artifacts"].prepare(
            request,
            view_builder=lambda: _raw_participant_view(self._raw, records),
        )

    def prepare_terminal_scheduler_prune_cas_v1(
        self,
        request: ReclamationPrepareRequest,
        records: tuple[dict[str, Any], ...],
    ) -> ReclamationParticipantPrepareReceipt:
        return self._journals["scheduler"].prepare(
            request,
            view_builder=lambda: _scheduler_participant_view(
                self._scheduler, records
            ),
        )

    def load(
        self, *, role: str, request_sha256: str
    ) -> ReclamationParticipantPrepareReceipt | None:
        if role not in PARTICIPANT_ROLES:
            raise ValueError("unknown participant role")
        return self._journals[role].load(request_sha256)

    def audit(self) -> dict[str, Any]:
        return {
            role: self._journals[role].audit()
            for role in PARTICIPANT_ROLES
        }


class AcquisitionSourceReclamationCoordinator:
    """Crash-resumable PREPARE coordinator with permanently disabled apply.

    Only coordinator and participant sidecar journals are written.  The five
    source authorities remain byte-for-byte outside this class's mutation
    surface.  ``commit()`` and ``apply()`` always raise a structured blocker.
    """

    _TABLES = frozenset({
        "reclamation_meta",
        "reclamation_decisions",
        "reclamation_participants",
        "reclamation_events",
    })
    _COLUMNS = {
        "reclamation_meta": frozenset({
            "singleton",
            "schema_version",
            "limits_sha256",
            "owner_epoch",
            "last_observed_at",
        }),
        "reclamation_decisions": frozenset({
            "request_sha256",
            "request_json",
            "state",
            "decision_sha256",
            "participant_root_sha256",
            "candidate_count",
            "created_at",
            "updated_at",
            "owner_epoch",
            "blockers_json",
        }),
        "reclamation_participants": frozenset({
            "request_sha256",
            "role",
            "prepare_sha256",
            "owner_epoch",
        }),
        "reclamation_events": frozenset({
            "event_sequence",
            "request_sha256",
            "event_type",
            "state",
            "owner_epoch",
            "observed_at",
            "payload_sha256",
            "previous_event_sha256",
            "event_sha256",
        }),
    }

    def __init__(
        self,
        root: str | Path,
        *,
        archive: AcquisitionSourceRetentionArchive,
        consumer_registry_verifier: (
            AcquisitionConsumerRegistryBoundEpochVerifier | None
        ) = None,
        limits: ReclamationCoordinatorLimits | None = None,
        clock: Callable[[], float] = time.time,
        fault_injector: Callable[[str], None] | None = None,
        directory_fsync: Callable[[Path], None] | None = None,
    ) -> None:
        supplied = Path(root)
        if not supplied.is_absolute():
            raise ValueError("reclamation coordinator root must be absolute")
        if not isinstance(archive, AcquisitionSourceRetentionArchive):
            raise TypeError("archive must be AcquisitionSourceRetentionArchive")
        if not callable(clock):
            raise TypeError("clock must be callable")
        if fault_injector is not None and not callable(fault_injector):
            raise TypeError("fault injector must be callable")
        if consumer_registry_verifier is not None and not isinstance(
            consumer_registry_verifier,
            AcquisitionConsumerRegistryBoundEpochVerifier,
        ):
            raise TypeError("consumer registry verifier has wrong type")
        self.root = supplied
        self.path = self.root / "reclamation_coordinator.sqlite3"
        self.lock_path = self.root / "coordinator.lock"
        self.participant_root = self.root / "participants"
        self._archive = archive
        self._consumer_registry_verifier = consumer_registry_verifier
        self._limits = limits or ReclamationCoordinatorLimits()
        self._clock = clock
        self._fault_injector = fault_injector
        self._directory_fsync = directory_fsync or self._default_directory_fsync
        self._lock = threading.RLock()
        if self._consumer_registry_verifier is not None:
            try:
                self._consumer_registry_verifier.assert_disjoint_root(self.root)
            except AcquisitionConsumerRegistryError:
                raise AcquisitionSourceReclamationIntegrityError(
                    "complete_consumer_registry_epoch_root_authority_unavailable"
                ) from None
        self._validate_disjoint_root()
        self._prepare_layout()
        self._initialize_journal()
        self._participants = ReclamationParticipantPreparer(
            self.participant_root,
            raw_store=archive._raw,
            scheduler=archive._scheduler,
            execution_store=archive._executions,
            capacity_ledger=archive._capacity,
            evidence_store=archive._evidence,
            consumer_registry_verifier=self._consumer_registry_verifier,
            limits=self._limits,
            clock=self._clock,
            directory_fsync=self._directory_fsync,
            fault_injector=self._fault_injector,
        )

    def _validate_disjoint_root(self) -> None:
        coordinator = self.root.resolve()
        directory_authorities = (
            self._archive.root.resolve(),
            self._archive._raw.root.resolve(),
            self._archive._evidence.root.resolve(),
        )
        file_authorities = (
            self._archive._scheduler.path.resolve(),
            self._archive._executions.path.resolve(),
            self._archive._capacity.path.resolve(),
        )
        for authority in directory_authorities:
            if (
                coordinator == authority
                or coordinator.is_relative_to(authority)
                or authority.is_relative_to(coordinator)
            ):
                raise AcquisitionSourceReclamationIntegrityError(
                    "coordinator root overlaps a source authority"
                )
        for authority in file_authorities:
            if coordinator == authority or authority.is_relative_to(coordinator):
                raise AcquisitionSourceReclamationIntegrityError(
                    "coordinator root contains a source authority"
                )

    @staticmethod
    def _default_directory_fsync(path: Path) -> None:
        if os.name == "nt":
            raise AcquisitionSourceReclamationDurabilityError(
                "directory_fsync_unavailable"
            )
        descriptor = os.open(
            str(path), os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        )
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def _fault(self, point: str) -> None:
        if self._fault_injector is not None:
            self._fault_injector(f"coordinator.{point}")

    def _prepare_layout(self) -> None:
        if self.root.exists() and self.root.is_symlink():
            raise AcquisitionSourceReclamationIntegrityError(
                "coordinator root is a symlink"
            )
        created = not self.root.exists()
        self.root.mkdir(parents=True, exist_ok=True)
        if created:
            self._directory_fsync(self.root.parent)
        if self.participant_root.exists() and self.participant_root.is_symlink():
            raise AcquisitionSourceReclamationIntegrityError(
                "participant root is a symlink"
            )
        self.participant_root.mkdir(exist_ok=True)
        if not self.lock_path.exists():
            try:
                with self.lock_path.open("xb") as stream:
                    stream.write(COORDINATOR_LOCK_BYTES)
                    stream.flush()
                    os.fsync(stream.fileno())
                self._directory_fsync(self.root)
            except FileExistsError:
                pass
        if (
            self.lock_path.is_symlink()
            or self.lock_path.read_bytes() != COORDINATOR_LOCK_BYTES
        ):
            raise AcquisitionSourceReclamationIntegrityError(
                "coordinator lock anchor differs"
            )

    @contextmanager
    def _exclusive(self) -> Iterator[None]:
        process = _reclamation_process_lock(self.lock_path)
        if not process.acquire(timeout=self._limits.lock_timeout_seconds):
            raise AcquisitionSourceReclamationBusy(
                "reclamation_coordinator_busy"
            )
        descriptor: int | None = None
        locked = False
        try:
            descriptor = os.open(str(self.lock_path), os.O_RDWR)
            deadline = time.monotonic() + self._limits.lock_timeout_seconds
            while True:
                try:
                    _lock_file(descriptor)
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
                        raise AcquisitionSourceReclamationBusy(
                            "reclamation_coordinator_busy"
                        ) from None
                    time.sleep(0.01)
            os.lseek(descriptor, 0, os.SEEK_SET)
            if (
                os.read(descriptor, len(COORDINATOR_LOCK_BYTES) + 1)
                != COORDINATOR_LOCK_BYTES
            ):
                raise AcquisitionSourceReclamationIntegrityError(
                    "coordinator lock anchor differs"
                )
            yield
        finally:
            if descriptor is not None:
                try:
                    if locked:
                        _unlock_file(descriptor)
                finally:
                    os.close(descriptor)
            process.release()

    def _connect(self) -> sqlite3.Connection:
        database = sqlite3.connect(self.path, timeout=2.0)
        try:
            database.row_factory = sqlite3.Row
            database.execute("PRAGMA foreign_keys=ON")
            database.execute("PRAGMA synchronous=FULL")
            mode = str(
                database.execute("PRAGMA journal_mode=DELETE").fetchone()[0]
            )
            if mode.lower() != "delete":
                raise AcquisitionSourceReclamationDurabilityError(
                    "coordinator journal mode differs"
                )
            page_size = int(database.execute("PRAGMA page_size").fetchone()[0])
            max_pages = self._limits.max_database_bytes // page_size
            database.execute(f"PRAGMA max_page_count={max_pages}")
            if int(database.execute("PRAGMA page_count").fetchone()[0]) > max_pages:
                raise AcquisitionSourceReclamationCapacityError(
                    "coordinator database bound exceeded"
                )
            database.execute("PRAGMA temp_store=MEMORY")
            return database
        except BaseException:
            database.close()
            raise

    @staticmethod
    def _tables(database: sqlite3.Connection) -> frozenset[str]:
        return frozenset(
            str(row[0])
            for row in database.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name NOT LIKE 'sqlite_%'"
            )
        )

    def _validate_schema(self, database: sqlite3.Connection) -> sqlite3.Row:
        if self._tables(database) != self._TABLES:
            raise AcquisitionSourceReclamationIntegrityError(
                "coordinator journal schema differs"
            )
        for table, expected in self._COLUMNS.items():
            columns = frozenset(
                str(row[1])
                for row in database.execute(f"PRAGMA table_info({table})")
            )
            if columns != expected:
                raise AcquisitionSourceReclamationIntegrityError(
                    "coordinator journal columns differ"
                )
        if tuple(database.execute("PRAGMA integrity_check").fetchone()) != (
            "ok",
        ):
            raise AcquisitionSourceReclamationIntegrityError(
                "coordinator journal integrity check failed"
            )
        row = database.execute(
            "SELECT * FROM reclamation_meta WHERE singleton=1"
        ).fetchone()
        if (
            row is None
            or int(row["schema_version"]) != COORDINATOR_SCHEMA_VERSION
            or str(row["limits_sha256"]) != self._limits.fingerprint
        ):
            raise AcquisitionSourceReclamationIntegrityError(
                "coordinator journal metadata differs"
            )
        return row

    def _initialize_journal(self) -> None:
        if self.path.exists() and self.path.is_symlink():
            raise AcquisitionSourceReclamationIntegrityError(
                "coordinator journal is a symlink"
            )
        with self._lock, closing(self._connect()) as database:
            try:
                database.execute("BEGIN IMMEDIATE")
                if not self._tables(database):
                    database.executescript(
                        """
                        CREATE TABLE reclamation_meta(
                            singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                            schema_version INTEGER NOT NULL,
                            limits_sha256 TEXT NOT NULL,
                            owner_epoch INTEGER NOT NULL CHECK(owner_epoch>=0),
                            last_observed_at REAL
                        ) WITHOUT ROWID;
                        CREATE TABLE reclamation_decisions(
                            request_sha256 TEXT PRIMARY KEY,
                            request_json BLOB NOT NULL,
                            state TEXT NOT NULL,
                            decision_sha256 TEXT,
                            participant_root_sha256 TEXT,
                            candidate_count INTEGER NOT NULL,
                            created_at REAL NOT NULL,
                            updated_at REAL NOT NULL,
                            owner_epoch INTEGER NOT NULL,
                            blockers_json BLOB NOT NULL
                        ) WITHOUT ROWID;
                        CREATE TABLE reclamation_participants(
                            request_sha256 TEXT NOT NULL REFERENCES
                                reclamation_decisions(request_sha256),
                            role TEXT NOT NULL,
                            prepare_sha256 TEXT NOT NULL,
                            owner_epoch INTEGER NOT NULL,
                            PRIMARY KEY(request_sha256,role)
                        ) WITHOUT ROWID;
                        CREATE TABLE reclamation_events(
                            event_sequence INTEGER PRIMARY KEY,
                            request_sha256 TEXT NOT NULL REFERENCES
                                reclamation_decisions(request_sha256),
                            event_type TEXT NOT NULL,
                            state TEXT NOT NULL,
                            owner_epoch INTEGER NOT NULL,
                            observed_at REAL NOT NULL,
                            payload_sha256 TEXT NOT NULL,
                            previous_event_sha256 TEXT,
                            event_sha256 TEXT NOT NULL UNIQUE
                        );
                        """
                    )
                    database.execute(
                        "INSERT INTO reclamation_meta VALUES(1,?,?,0,NULL)",
                        (
                            COORDINATOR_SCHEMA_VERSION,
                            self._limits.fingerprint,
                        ),
                    )
                self._validate_schema(database)
                database.commit()
            except sqlite3.OperationalError as err:
                if database.in_transaction:
                    database.rollback()
                if _sqlite_is_full(err):
                    raise AcquisitionSourceReclamationCapacityError(
                        "coordinator database full"
                    ) from err
                raise
            except BaseException:
                if database.in_transaction:
                    database.rollback()
                raise
        self._barrier()

    def _physical_bytes(self) -> int:
        return sum(
            item.stat().st_size
            for item in self.root.iterdir()
            if item.is_file()
            and not item.is_symlink()
            and item.name.startswith(self.path.name)
        )

    def _barrier(self) -> None:
        try:
            descriptor = os.open(str(self.path), os.O_RDWR)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            self._directory_fsync(self.root)
        except OSError as err:
            if err.errno == errno.ENOSPC:
                raise AcquisitionSourceReclamationCapacityError(
                    "coordinator filesystem full"
                ) from err
            raise AcquisitionSourceReclamationDurabilityError(
                "coordinator durability barrier failed"
            ) from err

    def _claim_owner(self) -> tuple[int, float]:
        with closing(self._connect()) as database:
            try:
                database.execute("BEGIN IMMEDIATE")
                meta = self._validate_schema(database)
                observed_at = _finite_time(self._clock(), "coordinator clock")
                if (
                    meta["last_observed_at"] is not None
                    and observed_at < float(meta["last_observed_at"])
                ):
                    raise AcquisitionSourceReclamationConflict(
                        "coordinator_clock_rollback"
                    )
                epoch = int(meta["owner_epoch"]) + 1
                if epoch > 2**63 - 1:
                    raise AcquisitionSourceReclamationCapacityError(
                        "coordinator owner epoch exhausted"
                    )
                database.execute(
                    "UPDATE reclamation_meta SET owner_epoch=?,"
                    "last_observed_at=? WHERE singleton=1 AND owner_epoch=?",
                    (epoch, observed_at, int(meta["owner_epoch"])),
                )
                if database.total_changes != 1:
                    raise AcquisitionSourceReclamationConflict(
                        "coordinator_owner_epoch_cas_conflict"
                    )
                database.commit()
            except sqlite3.OperationalError as err:
                if database.in_transaction:
                    database.rollback()
                if _sqlite_is_full(err):
                    raise AcquisitionSourceReclamationCapacityError(
                        "coordinator database full"
                    ) from err
                raise
            except BaseException:
                if database.in_transaction:
                    database.rollback()
                raise
        self._barrier()
        return epoch, observed_at

    @staticmethod
    def _event_core(
        *,
        sequence: int,
        request_sha256: str,
        event_type: str,
        state: str,
        owner_epoch: int,
        observed_at: float,
        payload_sha256: str,
        previous_event_sha256: str | None,
    ) -> dict[str, Any]:
        return {
            "event_sequence": sequence,
            "event_type": event_type,
            "observed_at": observed_at,
            "owner_epoch": owner_epoch,
            "payload_sha256": payload_sha256,
            "previous_event_sha256": previous_event_sha256,
            "request_sha256": request_sha256,
            "state": state,
        }

    def _append_event(
        self,
        database: sqlite3.Connection,
        *,
        request_sha256: str,
        event_type: str,
        state: str,
        owner_epoch: int,
        observed_at: float,
        payload_sha256: str,
    ) -> str:
        count = int(
            database.execute(
                "SELECT count(*) FROM reclamation_events"
            ).fetchone()[0]
        )
        if count >= self._limits.max_events:
            raise AcquisitionSourceReclamationCapacityError(
                "coordinator event retention bound reached"
            )
        previous = database.execute(
            "SELECT event_sequence,event_sha256 FROM reclamation_events "
            "ORDER BY event_sequence DESC LIMIT 1"
        ).fetchone()
        sequence = 1 if previous is None else int(previous[0]) + 1
        previous_sha = None if previous is None else str(previous[1])
        core = self._event_core(
            sequence=sequence,
            request_sha256=request_sha256,
            event_type=event_type,
            state=state,
            owner_epoch=owner_epoch,
            observed_at=observed_at,
            payload_sha256=payload_sha256,
            previous_event_sha256=previous_sha,
        )
        event_sha = _journal_digest({
            "contract": COORDINATOR_CONTRACT,
            "event": core,
        })
        database.execute(
            "INSERT INTO reclamation_events VALUES(?,?,?,?,?,?,?,?,?)",
            (
                sequence,
                request_sha256,
                event_type,
                state,
                owner_epoch,
                observed_at,
                payload_sha256,
                previous_sha,
                event_sha,
            ),
        )
        return event_sha

    def _sealed_context(
        self,
        plan: AcquisitionSourceRetentionPlan,
        *,
        claimed_consumer_registry_root_sha256: str | None,
        claimed_consumer_epoch_root_sha256: str | None,
    ) -> tuple[
        ReclamationPrepareRequest,
        tuple[dict[str, Any], ...],
        tuple[dict[str, str], ...],
    ]:
        registry = self._consumer_registry_verifier
        if claimed_consumer_registry_root_sha256 is not None:
            claimed_consumer_registry_root_sha256 = _sha(
                claimed_consumer_registry_root_sha256,
                "claimed consumer registry root SHA-256",
            )
        if claimed_consumer_epoch_root_sha256 is not None:
            claimed_consumer_epoch_root_sha256 = _sha(
                claimed_consumer_epoch_root_sha256,
                "claimed consumer epoch root SHA-256",
            )
        if registry is None:
            if (
                claimed_consumer_registry_root_sha256 is not None
                or claimed_consumer_epoch_root_sha256 is not None
            ):
                raise AcquisitionSourceReclamationConflict(
                    "complete_consumer_registry_epoch_root_authority_unavailable"
                )
        else:
            try:
                verification = registry.verify_held()
            except AcquisitionConsumerRegistryError:
                raise AcquisitionSourceReclamationConflict(
                    "complete_consumer_registry_epoch_root_authority_unavailable"
                ) from None
            if (
                claimed_consumer_registry_root_sha256 is not None
                and claimed_consumer_registry_root_sha256
                != verification.registry_root_sha256
            ) or (
                claimed_consumer_epoch_root_sha256 is not None
                and claimed_consumer_epoch_root_sha256
                != verification.epoch_root_sha256
            ):
                raise AcquisitionSourceReclamationConflict(
                    "claimed consumer registry epoch roots differ"
                )
            if (
                claimed_consumer_registry_root_sha256 is None
            ) != (claimed_consumer_epoch_root_sha256 is None):
                raise AcquisitionSourceReclamationConflict(
                    "claimed consumer registry epoch roots must be paired"
                )
            claimed_consumer_registry_root_sha256 = (
                verification.registry_root_sha256
            )
            claimed_consumer_epoch_root_sha256 = verification.epoch_root_sha256
        authority = build_fail_closed_reclamation_authority(
            plan, self._archive
        )
        if (
            authority.candidate_count < 1
            or authority.candidate_count > self._limits.max_candidates
            or authority.sealed_export_eligible_count
            != authority.candidate_count
            or authority.withheld_count != 0
        ):
            raise AcquisitionSourceReclamationConflict(
                "complete_sealed_archive_required"
            )
        audit_before = self._archive.audit()
        if (
            audit_before.get("healthy") is not True
            or audit_before.get("archive_anchor_sha256")
            != authority.sealed_archive_anchor_sha256
        ):
            raise AcquisitionSourceReclamationConflict(
                "sealed_archive_anchor_cas_conflict"
            )
        bindings: list[dict[str, Any]] = []
        records: list[dict[str, Any]] = []
        for disposition in plan.dispositions:
            if (
                disposition.status != "sealed_export_eligible"
                or disposition.record_sha256 is None
            ):
                raise AcquisitionSourceReclamationConflict(
                    "complete_sealed_archive_required"
                )
            lookup = self._archive.lookup(disposition.candidate_sha256)
            if (
                lookup.status != "archived"
                or lookup.record is None
                or lookup.segment_sha256 is None
                or lookup.record.get("record_sha256")
                != disposition.record_sha256
            ):
                raise AcquisitionSourceReclamationConflict(
                    "sealed_archive_record_missing_or_partial"
                )
            bindings.append({
                "candidate_sha256": disposition.candidate_sha256,
                "record_sha256": disposition.record_sha256,
                "segment_sha256": lookup.segment_sha256,
            })
            records.append(lookup.record)
        if bindings != sorted(bindings, key=lambda item: item["candidate_sha256"]):
            raise AcquisitionSourceReclamationIntegrityError(
                "sealed candidate binding order differs"
            )
        audit_after = self._archive.audit()
        if (
            audit_after.get("archive_anchor_sha256")
            != audit_before.get("archive_anchor_sha256")
        ):
            raise AcquisitionSourceReclamationConflict(
                "sealed_archive_changed_during_prepare"
            )
        core = {
            "candidate_bindings_sha256": _journal_digest(bindings),
            "candidate_count": len(bindings),
            "claimed_consumer_epoch_root_sha256": (
                claimed_consumer_epoch_root_sha256
            ),
            "claimed_consumer_registry_root_sha256": (
                claimed_consumer_registry_root_sha256
            ),
            "contract": COORDINATOR_CONTRACT,
            "retention_plan_sha256": authority.retention_plan_sha256,
            "sealed_archive_anchor_sha256": (
                authority.sealed_archive_anchor_sha256
            ),
            "source_binding_sha256": authority.source_binding_sha256,
            "source_view_sha256": authority.source_view_sha256,
        }
        request = ReclamationPrepareRequest(
            request_sha256=_journal_digest(core),
            source_binding_sha256=authority.source_binding_sha256,
            source_view_sha256=authority.source_view_sha256,
            retention_plan_sha256=authority.retention_plan_sha256,
            sealed_archive_anchor_sha256=(
                authority.sealed_archive_anchor_sha256
            ),
            candidate_bindings_sha256=core["candidate_bindings_sha256"],
            candidate_count=len(bindings),
            claimed_consumer_registry_root_sha256=(
                claimed_consumer_registry_root_sha256
            ),
            claimed_consumer_epoch_root_sha256=(
                claimed_consumer_epoch_root_sha256
            ),
        )
        validate_reclamation_prepare_request(request)
        if registry is not None:
            try:
                registry.verify_held()
            except AcquisitionConsumerRegistryError:
                raise AcquisitionSourceReclamationConflict(
                    "complete_consumer_registry_epoch_root_authority_unavailable"
                ) from None
        return request, tuple(records), tuple(bindings)

    def _begin_preparing(
        self,
        request: ReclamationPrepareRequest,
        *,
        owner_epoch: int,
        observed_at: float,
    ) -> str:
        payload = _journal_canonical(request.as_dict())
        blockers = _journal_canonical(list(_request_apply_blockers(request)))
        with closing(self._connect()) as database:
            try:
                database.execute("BEGIN IMMEDIATE")
                self._validate_schema(database)
                row = database.execute(
                    "SELECT * FROM reclamation_decisions "
                    "WHERE request_sha256=?",
                    (request.request_sha256,),
                ).fetchone()
                if row is not None:
                    if (
                        bytes(row["request_json"]) != payload
                        or int(row["candidate_count"])
                        != request.candidate_count
                        or bytes(row["blockers_json"]) != blockers
                        or str(row["state"]) not in {PREPARING, PREPARED}
                    ):
                        raise AcquisitionSourceReclamationConflict(
                            "coordinator_request_identity_reused"
                        )
                    database.execute(
                        "UPDATE reclamation_decisions SET owner_epoch=?,"
                        "updated_at=? WHERE request_sha256=?",
                        (owner_epoch, observed_at, request.request_sha256),
                    )
                    database.commit()
                    return str(row["state"])
                decision_count = int(
                    database.execute(
                        "SELECT count(*) FROM reclamation_decisions"
                    ).fetchone()[0]
                )
                if decision_count >= self._limits.max_decisions:
                    raise AcquisitionSourceReclamationCapacityError(
                        "coordinator decision retention bound reached"
                    )
                if self._physical_bytes() >= self._limits.max_database_bytes:
                    raise AcquisitionSourceReclamationCapacityError(
                        "coordinator database bound reached"
                    )
                database.execute(
                    "INSERT INTO reclamation_decisions VALUES(?,?,?,NULL,NULL,"
                    "?,?,?,?,?)",
                    (
                        request.request_sha256,
                        payload,
                        PREPARING,
                        request.candidate_count,
                        observed_at,
                        observed_at,
                        owner_epoch,
                        blockers,
                    ),
                )
                self._append_event(
                    database,
                    request_sha256=request.request_sha256,
                    event_type="BEGIN_PREPARING",
                    state=PREPARING,
                    owner_epoch=owner_epoch,
                    observed_at=observed_at,
                    payload_sha256=request.request_sha256,
                )
                self._fault("before_preparing_commit")
                database.commit()
                self._fault("after_preparing_commit")
            except sqlite3.OperationalError as err:
                if database.in_transaction:
                    database.rollback()
                if _sqlite_is_full(err):
                    raise AcquisitionSourceReclamationCapacityError(
                        "coordinator database full"
                    ) from err
                raise
            except BaseException:
                if database.in_transaction:
                    database.rollback()
                raise
        self._barrier()
        return PREPARING

    def _bind_participant(
        self,
        receipt: ReclamationParticipantPrepareReceipt,
        *,
        owner_epoch: int,
        observed_at: float,
    ) -> None:
        validate_participant_prepare_receipt(receipt)
        with closing(self._connect()) as database:
            try:
                database.execute("BEGIN IMMEDIATE")
                self._validate_schema(database)
                decision = database.execute(
                    "SELECT state FROM reclamation_decisions "
                    "WHERE request_sha256=?",
                    (receipt.request_sha256,),
                ).fetchone()
                if decision is None or str(decision[0]) not in {
                    PREPARING,
                    PREPARED,
                }:
                    raise AcquisitionSourceReclamationConflict(
                        "participant has no preparing decision"
                    )
                row = database.execute(
                    "SELECT prepare_sha256 FROM reclamation_participants "
                    "WHERE request_sha256=? AND role=?",
                    (receipt.request_sha256, receipt.role),
                ).fetchone()
                if row is not None:
                    if str(row[0]) != receipt.prepare_sha256:
                        raise AcquisitionSourceReclamationConflict(
                            "participant prepare binding differs"
                        )
                    database.commit()
                    return
                if str(decision[0]) != PREPARING:
                    raise AcquisitionSourceReclamationIntegrityError(
                        "prepared decision lacks participant binding"
                    )
                database.execute(
                    "INSERT INTO reclamation_participants VALUES(?,?,?,?)",
                    (
                        receipt.request_sha256,
                        receipt.role,
                        receipt.prepare_sha256,
                        owner_epoch,
                    ),
                )
                self._append_event(
                    database,
                    request_sha256=receipt.request_sha256,
                    event_type=f"PARTICIPANT_{receipt.role.upper()}",
                    state=PREPARING,
                    owner_epoch=owner_epoch,
                    observed_at=observed_at,
                    payload_sha256=receipt.prepare_sha256,
                )
                self._fault(f"before_{receipt.role}_journal_commit")
                database.commit()
                self._fault(f"after_{receipt.role}_journal_commit")
            except sqlite3.OperationalError as err:
                if database.in_transaction:
                    database.rollback()
                if _sqlite_is_full(err):
                    raise AcquisitionSourceReclamationCapacityError(
                        "coordinator database full"
                    ) from err
                raise
            except BaseException:
                if database.in_transaction:
                    database.rollback()
                raise
        self._barrier()

    def _reprove_consumer_closures(
        self,
        request: ReclamationPrepareRequest,
        receipt: ReclamationParticipantPrepareReceipt,
    ) -> None:
        validate_participant_prepare_receipt(receipt)
        if receipt.role != "downstream_lifecycle":
            raise AcquisitionSourceReclamationIntegrityError(
                "consumer closure receipt has the wrong participant role"
            )
        closures = receipt.consumer_closure_receipts
        registry = self._consumer_registry_verifier
        if request.claimed_consumer_registry_root_sha256 is None:
            if closures:
                raise AcquisitionSourceReclamationIntegrityError(
                    "unbound reclamation request contains consumer closures"
                )
            return
        if registry is None or len(closures) != request.candidate_count:
            raise AcquisitionSourceReclamationConflict(
                "complete_consumer_registry_epoch_root_authority_unavailable"
            )
        try:
            verification = registry.verify_held()
        except AcquisitionConsumerRegistryError:
            raise AcquisitionSourceReclamationConflict(
                "complete_consumer_registry_epoch_root_authority_unavailable"
            ) from None
        if (
            verification.registry_root_sha256
            != request.claimed_consumer_registry_root_sha256
            or verification.epoch_root_sha256
            != request.claimed_consumer_epoch_root_sha256
        ):
            raise AcquisitionSourceReclamationConflict(
                "consumer registry epoch changed before PREPARED"
            )
        for closure in closures:
            member = registry.registry_root.by_workload.get(closure.workload_id)
            if member is None:
                raise AcquisitionSourceReclamationIntegrityError(
                    "consumer closure workload is not registered"
                )
            lookup = self._archive.lookup(closure.candidate_sha256)
            if (
                lookup.status != "archived"
                or lookup.record is None
                or lookup.segment_sha256
                != closure.source_archive_segment_sha256
                or lookup.record.get("record_sha256")
                != closure.source_archive_record_sha256
                or closure.source_archive_anchor_sha256
                != request.sealed_archive_anchor_sha256
            ):
                raise AcquisitionSourceReclamationConflict(
                    "consumer closure source archive binding changed"
                )
            candidate = _record_candidate(lookup.record)
            if (
                candidate.get("acquisition_job_id")
                != closure.acquisition_job_id
                or candidate.get("evidence_stream_id") != closure.stream_id
                or candidate.get("evidence_stream_sequence")
                != closure.stream_sequence
            ):
                raise AcquisitionSourceReclamationIntegrityError(
                    "consumer closure candidate binding differs"
                )
            terminal = self._archive._evidence.load_terminal_consumption(
                closure.stream_id,
                closure.stream_sequence,
            )
            identity = self._archive._evidence.terminal_event_identity(
                closure.stream_id,
                closure.stream_sequence,
            )
            if terminal is None or identity is None:
                raise AcquisitionSourceReclamationConflict(
                    "consumer closure lifecycle terminal is unavailable"
                )
            try:
                expected = AcquisitionConsumerClosureReceipt.create(
                    candidate_sha256=closure.candidate_sha256,
                    acquisition_job_id=closure.acquisition_job_id,
                    member=member,
                    verification=verification,
                    producer_receipt=terminal.producer_receipt,
                    consumption_receipt=terminal.consumption_receipt,
                    terminal_identity=identity,
                    source_archive_anchor_sha256=(
                        request.sealed_archive_anchor_sha256
                    ),
                    source_archive_segment_sha256=lookup.segment_sha256,
                    source_archive_record_sha256=closure.source_archive_record_sha256,
                )
            except AcquisitionConsumerRegistryError:
                raise AcquisitionSourceReclamationConflict(
                    "complete_consumer_registry_epoch_root_authority_unavailable"
                ) from None
            if expected != closure:
                raise AcquisitionSourceReclamationIntegrityError(
                    "consumer closure receipt cannot be reproved"
                )

    def _finish_prepared(
        self,
        request: ReclamationPrepareRequest,
        *,
        owner_epoch: int,
        observed_at: float,
    ) -> ReclamationPrepareOutcome:
        receipts = []
        for role in PARTICIPANT_ROLES:
            receipt = self._participants.load(
                role=role, request_sha256=request.request_sha256
            )
            if receipt is None:
                raise AcquisitionSourceReclamationConflict(
                    "participant_prepare_set_incomplete"
                )
            receipts.append(receipt)
        downstream = next(
            item for item in receipts if item.role == "downstream_lifecycle"
        )
        self._reprove_consumer_closures(request, downstream)
        blockers = _request_apply_blockers(request)
        participant_mapping = [
            {
                "prepare_sha256": item.prepare_sha256,
                "role": item.role,
            }
            for item in receipts
        ]
        participant_root = _journal_digest(participant_mapping)
        decision_core = {
            "apply_enabled": False,
            "apply_order": [
                "downstream_lifecycle",
                "execution_receipts",
                "scheduler",
                "raw_artifacts_child_first",
                "capacity_ledger_after_raw_APPLIED",
            ],
            "blockers": list(blockers),
            "contract": COORDINATOR_CONTRACT,
            "participant_root_sha256": participant_root,
            "request_sha256": request.request_sha256,
            "source_release_performed": False,
        }
        decision_sha = _journal_digest(decision_core)
        with closing(self._connect()) as database:
            try:
                database.execute("BEGIN IMMEDIATE")
                self._validate_schema(database)
                row = database.execute(
                    "SELECT * FROM reclamation_decisions "
                    "WHERE request_sha256=?",
                    (request.request_sha256,),
                ).fetchone()
                if row is None:
                    raise AcquisitionSourceReclamationIntegrityError(
                        "preparing decision disappeared"
                    )
                bindings = database.execute(
                    "SELECT role,prepare_sha256 FROM reclamation_participants "
                    "WHERE request_sha256=? ORDER BY role",
                    (request.request_sha256,),
                ).fetchall()
                if [
                    {"prepare_sha256": str(item[1]), "role": str(item[0])}
                    for item in bindings
                ] != participant_mapping:
                    raise AcquisitionSourceReclamationIntegrityError(
                        "participant sidecars and coordinator differ"
                    )
                if str(row["state"]) == PREPARED:
                    if (
                        str(row["decision_sha256"]) != decision_sha
                        or str(row["participant_root_sha256"])
                        != participant_root
                    ):
                        raise AcquisitionSourceReclamationIntegrityError(
                            "prepared decision checksum differs"
                        )
                    database.commit()
                elif str(row["state"]) == PREPARING:
                    self._fault("before_prepared_transition")
                    database.execute(
                        "UPDATE reclamation_decisions SET state=?,"
                        "decision_sha256=?,participant_root_sha256=?,"
                        "updated_at=?,owner_epoch=? WHERE request_sha256=? "
                        "AND state=?",
                        (
                            PREPARED,
                            decision_sha,
                            participant_root,
                            observed_at,
                            owner_epoch,
                            request.request_sha256,
                            PREPARING,
                        ),
                    )
                    if database.total_changes < 1:
                        raise AcquisitionSourceReclamationConflict(
                            "prepared transition CAS conflict"
                        )
                    self._append_event(
                        database,
                        request_sha256=request.request_sha256,
                        event_type="FINISH_PREPARED",
                        state=PREPARED,
                        owner_epoch=owner_epoch,
                        observed_at=observed_at,
                        payload_sha256=decision_sha,
                    )
                    self._fault("before_prepared_commit")
                    database.commit()
                    self._fault("after_prepared_commit")
                else:
                    raise AcquisitionSourceReclamationIntegrityError(
                        "disabled coordinator reached a positive state"
                    )
            except sqlite3.OperationalError as err:
                if database.in_transaction:
                    database.rollback()
                if _sqlite_is_full(err):
                    raise AcquisitionSourceReclamationCapacityError(
                        "coordinator database full"
                    ) from err
                raise
            except BaseException:
                if database.in_transaction:
                    database.rollback()
                raise
        self._barrier()
        return ReclamationPrepareOutcome(
            request_sha256=request.request_sha256,
            state=PREPARED,
            decision_sha256=decision_sha,
            participant_root_sha256=participant_root,
            candidate_count=request.candidate_count,
            owner_epoch=owner_epoch,
            consumer_closure_root_sha256=(
                downstream.child_reference_root_sha256
                if downstream.consumer_closure_receipts
                else None
            ),
            blockers=blockers,
        )

    def prepare(
        self,
        plan: AcquisitionSourceRetentionPlan,
        *,
        claimed_consumer_registry_root_sha256: str | None = None,
        claimed_consumer_epoch_root_sha256: str | None = None,
    ) -> ReclamationPrepareOutcome:
        """Prepare all participants and durably stop at fail-closed PREPARED."""

        with self._lock, self._exclusive():
            request, records, archive_bindings = self._sealed_context(
                plan,
                claimed_consumer_registry_root_sha256=(
                    claimed_consumer_registry_root_sha256
                ),
                claimed_consumer_epoch_root_sha256=(
                    claimed_consumer_epoch_root_sha256
                ),
            )
            owner_epoch, observed_at = self._claim_owner()
            self._begin_preparing(
                request,
                owner_epoch=owner_epoch,
                observed_at=observed_at,
            )
            methods = (
                lambda: self._participants.prepare_committed_capacity_release_cas_v1(
                    request, records
                ),
                lambda: self._participants.prepare_complete_source_reference_closure_cas_v1(
                    request,
                    records,
                    archive_bindings=archive_bindings,
                ),
                lambda: self._participants.prepare_execution_history_prune_cas_v1(
                    request, records
                ),
                lambda: self._participants.prepare_reference_counted_capture_prune_cas_v1(
                    request, records
                ),
                lambda: self._participants.prepare_terminal_scheduler_prune_cas_v1(
                    request, records
                ),
            )
            for method in methods:
                receipt = method()
                self._fault(f"after_{receipt.role}_prepare")
                self._bind_participant(
                    receipt,
                    owner_epoch=owner_epoch,
                    observed_at=observed_at,
                )
            audit_after = self._archive.audit()
            if (
                audit_after.get("archive_anchor_sha256")
                != request.sealed_archive_anchor_sha256
            ):
                raise AcquisitionSourceReclamationConflict(
                    "sealed_archive_changed_before_prepared"
                )
            if self._consumer_registry_verifier is not None:
                try:
                    self._consumer_registry_verifier.verify_held()
                except AcquisitionConsumerRegistryError:
                    raise AcquisitionSourceReclamationConflict(
                        "complete_consumer_registry_epoch_root_authority_unavailable"
                    ) from None
            return self._finish_prepared(
                request,
                owner_epoch=owner_epoch,
                observed_at=observed_at,
            )

    def commit(self, request_sha256: str) -> None:
        raise AcquisitionSourceReclamationApplyDisabled(
            request_sha256=request_sha256, operation="commit"
        )

    def apply(self, request_sha256: str) -> None:
        raise AcquisitionSourceReclamationApplyDisabled(
            request_sha256=request_sha256, operation="apply"
        )

    def load_consumer_closure_receipts(
        self,
        request_sha256: str,
    ) -> tuple[AcquisitionConsumerClosureReceipt, ...]:
        """Load and reprove the immutable per-job PREPARED closure receipts."""

        request = self.load_request(request_sha256)
        if request is None:
            raise AcquisitionSourceReclamationConflict(
                "reclamation request is unknown"
            )
        receipt = self._participants.load(
            role="downstream_lifecycle",
            request_sha256=request.request_sha256,
        )
        if receipt is None:
            return ()
        self._reprove_consumer_closures(request, receipt)
        return receipt.consumer_closure_receipts

    def resolver(
        self, request_sha256: str
    ) -> ExactArchivedReclamationResolver:
        request = self.load_request(request_sha256)
        if request is None:
            raise AcquisitionSourceReclamationConflict(
                "reclamation request is unknown"
            )
        return ExactArchivedReclamationResolver(
            self._archive,
            expected_anchor_sha256=request.sealed_archive_anchor_sha256,
        )

    @staticmethod
    def _decode_request(payload: bytes) -> ReclamationPrepareRequest:
        try:
            value = json.loads(payload.decode("ascii"))
        except (UnicodeDecodeError, json.JSONDecodeError) as err:
            raise AcquisitionSourceReclamationIntegrityError(
                "coordinator request is corrupt"
            ) from err
        if not isinstance(value, dict) or payload != _journal_canonical(value):
            raise AcquisitionSourceReclamationIntegrityError(
                "coordinator request is not canonical"
            )
        expected = {
            "candidate_bindings_sha256",
            "candidate_count",
            "claimed_consumer_epoch_root_sha256",
            "claimed_consumer_registry_root_sha256",
            "contract",
            "request_sha256",
            "retention_plan_sha256",
            "sealed_archive_anchor_sha256",
            "source_binding_sha256",
            "source_view_sha256",
        }
        if set(value) != expected or value.get("contract") != COORDINATOR_CONTRACT:
            raise AcquisitionSourceReclamationIntegrityError(
                "coordinator request shape differs"
            )
        request = ReclamationPrepareRequest(
            request_sha256=value["request_sha256"],
            source_binding_sha256=value["source_binding_sha256"],
            source_view_sha256=value["source_view_sha256"],
            retention_plan_sha256=value["retention_plan_sha256"],
            sealed_archive_anchor_sha256=value[
                "sealed_archive_anchor_sha256"
            ],
            candidate_bindings_sha256=value["candidate_bindings_sha256"],
            candidate_count=value["candidate_count"],
            claimed_consumer_registry_root_sha256=value[
                "claimed_consumer_registry_root_sha256"
            ],
            claimed_consumer_epoch_root_sha256=value[
                "claimed_consumer_epoch_root_sha256"
            ],
        )
        validate_reclamation_prepare_request(request)
        return request

    def load_request(
        self, request_sha256: str
    ) -> ReclamationPrepareRequest | None:
        request = _sha(request_sha256, "request SHA-256")
        with closing(self._connect()) as database:
            self._validate_schema(database)
            row = database.execute(
                "SELECT request_json FROM reclamation_decisions "
                "WHERE request_sha256=?",
                (request,),
            ).fetchone()
        return None if row is None else self._decode_request(bytes(row[0]))

    def audit(self) -> dict[str, Any]:
        """Validate journal chain, state machine, sidecars and hard bounds."""

        with self._lock, self._exclusive(), closing(self._connect()) as database:
            meta = self._validate_schema(database)
            decisions = database.execute(
                "SELECT * FROM reclamation_decisions ORDER BY request_sha256"
            ).fetchall()
            bindings = database.execute(
                "SELECT * FROM reclamation_participants "
                "ORDER BY request_sha256,role"
            ).fetchall()
            events = database.execute(
                "SELECT * FROM reclamation_events ORDER BY event_sequence"
            ).fetchall()
            if (
                len(decisions) > self._limits.max_decisions
                or len(events) > self._limits.max_events
            ):
                raise AcquisitionSourceReclamationCapacityError(
                    "coordinator journal retention bound exceeded"
                )
            previous_sha: str | None = None
            last_state: dict[str, str] = {}
            events_by_request: dict[str, list[dict[str, Any]]] = {}
            previous_observed_at: float | None = None
            meta_epoch = int(meta["owner_epoch"])
            for expected_sequence, row in enumerate(events, start=1):
                if int(row["event_sequence"]) != expected_sequence:
                    raise AcquisitionSourceReclamationIntegrityError(
                        "coordinator event sequence contains a gap"
                    )
                core = self._event_core(
                    sequence=expected_sequence,
                    request_sha256=str(row["request_sha256"]),
                    event_type=str(row["event_type"]),
                    state=str(row["state"]),
                    owner_epoch=int(row["owner_epoch"]),
                    observed_at=float(row["observed_at"]),
                    payload_sha256=str(row["payload_sha256"]),
                    previous_event_sha256=(
                        None
                        if row["previous_event_sha256"] is None
                        else str(row["previous_event_sha256"])
                    ),
                )
                expected_sha = _journal_digest({
                    "contract": COORDINATOR_CONTRACT,
                    "event": core,
                })
                _sha(core["request_sha256"], "event request SHA-256")
                _sha(core["payload_sha256"], "event payload SHA-256")
                _sha(str(row["event_sha256"]), "event SHA-256")
                observed = _finite_time(
                    core["observed_at"], "event observed time"
                )
                if (
                    core["previous_event_sha256"] != previous_sha
                    or str(row["event_sha256"]) != expected_sha
                    or core["state"] not in {PREPARING, PREPARED}
                    or not 1 <= core["owner_epoch"] <= meta_epoch
                    or (
                        previous_observed_at is not None
                        and observed < previous_observed_at
                    )
                ):
                    raise AcquisitionSourceReclamationIntegrityError(
                        "coordinator event chain differs"
                    )
                previous_sha = expected_sha
                previous_observed_at = observed
                last_state[core["request_sha256"]] = core["state"]
                events_by_request.setdefault(
                    core["request_sha256"], []
                ).append(core)
            binding_map: dict[str, list[dict[str, str]]] = {}
            for row in bindings:
                role = str(row["role"])
                request_sha = str(row["request_sha256"])
                prepare_sha = str(row["prepare_sha256"])
                binding_epoch = int(row["owner_epoch"])
                if (
                    role not in PARTICIPANT_ROLES
                    or not 1 <= binding_epoch <= meta_epoch
                ):
                    raise AcquisitionSourceReclamationIntegrityError(
                        "coordinator participant role differs"
                    )
                sidecar = self._participants.load(
                    role=role, request_sha256=request_sha
                )
                if sidecar is None or sidecar.prepare_sha256 != prepare_sha:
                    raise AcquisitionSourceReclamationIntegrityError(
                        "coordinator participant sidecar differs"
                    )
                binding_map.setdefault(request_sha, []).append({
                    "prepare_sha256": prepare_sha,
                    "role": role,
                })
            preparing = 0
            prepared = 0
            registry_verified = 0
            for row in decisions:
                request = self._decode_request(bytes(row["request_json"]))
                request_blockers = _request_apply_blockers(request)
                state = str(row["state"])
                created_at = _finite_time(row["created_at"], "decision created time")
                updated_at = _finite_time(row["updated_at"], "decision updated time")
                decision_epoch = int(row["owner_epoch"])
                if (
                    request.request_sha256 != str(row["request_sha256"])
                    or int(row["candidate_count"]) != request.candidate_count
                    or bytes(row["blockers_json"])
                    != _journal_canonical(list(request_blockers))
                    or state not in {PREPARING, PREPARED}
                    or last_state.get(request.request_sha256) != state
                    or updated_at < created_at
                    or not 1 <= decision_epoch <= meta_epoch
                ):
                    raise AcquisitionSourceReclamationIntegrityError(
                        "coordinator decision differs"
                    )
                items = binding_map.get(request.request_sha256, [])
                downstream_receipt = self._participants.load(
                    role="downstream_lifecycle",
                    request_sha256=request.request_sha256,
                )
                if request.claimed_consumer_registry_root_sha256 is not None:
                    registry = self._consumer_registry_verifier
                    if registry is None:
                        raise AcquisitionSourceReclamationIntegrityError(
                            "complete_consumer_registry_epoch_root_authority_unavailable"
                        )
                    try:
                        registry_verification = registry.verify_held()
                    except AcquisitionConsumerRegistryError:
                        raise AcquisitionSourceReclamationIntegrityError(
                            "complete_consumer_registry_epoch_root_authority_unavailable"
                        ) from None
                    if (
                        registry_verification.registry_root_sha256
                        != request.claimed_consumer_registry_root_sha256
                        or registry_verification.epoch_root_sha256
                        != request.claimed_consumer_epoch_root_sha256
                    ):
                        raise AcquisitionSourceReclamationIntegrityError(
                            "prepared consumer registry epoch differs"
                        )
                if downstream_receipt is not None:
                    self._reprove_consumer_closures(
                        request,
                        downstream_receipt,
                    )
                if request.claimed_consumer_registry_root_sha256 is not None:
                    registry_verified += 1
                request_events = events_by_request.get(
                    request.request_sha256, []
                )
                if (
                    not request_events
                    or request_events[0]["event_type"] != "BEGIN_PREPARING"
                    or request_events[0]["state"] != PREPARING
                    or request_events[0]["payload_sha256"]
                    != request.request_sha256
                ):
                    raise AcquisitionSourceReclamationIntegrityError(
                        "coordinator decision begin event differs"
                    )
                expected_participant_events = [
                    {
                        "event_type": f"PARTICIPANT_{item['role'].upper()}",
                        "payload_sha256": item["prepare_sha256"],
                        "state": PREPARING,
                    }
                    for item in items
                ]
                actual_participant_events = [
                    {
                        "event_type": item["event_type"],
                        "payload_sha256": item["payload_sha256"],
                        "state": item["state"],
                    }
                    for item in request_events[1 : 1 + len(items)]
                ]
                if actual_participant_events != expected_participant_events:
                    raise AcquisitionSourceReclamationIntegrityError(
                        "coordinator participant event set differs"
                    )
                if state == PREPARED:
                    if [item["role"] for item in items] != list(
                        PARTICIPANT_ROLES
                    ):
                        raise AcquisitionSourceReclamationIntegrityError(
                            "prepared decision participant set differs"
                        )
                    root = _journal_digest(items)
                    decision_sha = _journal_digest({
                        "apply_enabled": False,
                        "apply_order": [
                            "downstream_lifecycle",
                            "execution_receipts",
                            "scheduler",
                            "raw_artifacts_child_first",
                            "capacity_ledger_after_raw_APPLIED",
                        ],
                        "blockers": list(request_blockers),
                        "contract": COORDINATOR_CONTRACT,
                        "participant_root_sha256": root,
                        "request_sha256": request.request_sha256,
                        "source_release_performed": False,
                    })
                    if (
                        str(row["participant_root_sha256"]) != root
                        or str(row["decision_sha256"]) != decision_sha
                        or len(request_events) != len(items) + 2
                        or request_events[-1]["event_type"]
                        != "FINISH_PREPARED"
                        or request_events[-1]["state"] != PREPARED
                        or request_events[-1]["payload_sha256"]
                        != decision_sha
                    ):
                        raise AcquisitionSourceReclamationIntegrityError(
                            "prepared decision hash differs"
                        )
                    prepared += 1
                else:
                    if (
                        row["participant_root_sha256"] is not None
                        or row["decision_sha256"] is not None
                        or len(request_events) != len(items) + 1
                    ):
                        raise AcquisitionSourceReclamationIntegrityError(
                            "preparing decision exposes a final hash"
                        )
                    preparing += 1
            if set(events_by_request) != {
                str(row["request_sha256"]) for row in decisions
            }:
                raise AcquisitionSourceReclamationIntegrityError(
                    "coordinator event request set differs"
                )
            participant_audit = self._participants.audit()
            database_bytes = self._physical_bytes()
            if database_bytes > self._limits.max_database_bytes:
                raise AcquisitionSourceReclamationCapacityError(
                    "coordinator database bound exceeded"
                )
        return {
            "apply_blockers": list(APPLY_BLOCKERS),
            "apply_enabled": False,
            "contract": COORDINATOR_CONTRACT,
            "database_bytes": database_bytes,
            "decision_count": len(decisions),
            "event_count": len(events),
            "last_event_sha256": previous_sha,
            "owner_epoch": int(meta["owner_epoch"]),
            "participant_count": len(bindings),
            "participant_journals": participant_audit,
            "prepared_count": prepared,
            "preparing_count": preparing,
            "registry_verified_decision_count": registry_verified,
            "source_release_performed": False,
        }


__all__ = [
    "APPLIED",
    "APPLY_BLOCKERS",
    "APPLY_ENABLED",
    "COMMITTED",
    "COORDINATOR_CONTRACT",
    "COORDINATOR_SCHEMA_VERSION",
    "COORDINATOR_STATES",
    "CONTRACT",
    "CROSS_STORE_REQUIREMENTS",
    "AcquisitionSourceReclamationApplyDisabled",
    "FailClosedReclamationAuthority",
    "MAX_AUTHORITY_BYTES",
    "REASON_CODE",
    "REGISTRY_VERIFIED_APPLY_BLOCKERS",
    "REQUIRED_STORE_CAPABILITIES",
    "PARTICIPANT_PREPARE_CONTRACT",
    "PARTICIPANT_ROLES",
    "PREPARED",
    "PREPARING",
    "ReclamationCoordinatorLimits",
    "ReclamationParticipantPrepareReceipt",
    "ReclamationParticipantPreparer",
    "ReclamationPrepareOutcome",
    "ReclamationPrepareRequest",
    "ExactArchivedReclamationResolver",
    "RequiredStoreCapability",
    "SCHEMA",
    "STATUS",
    "AcquisitionSourceReclamationAuthorityError",
    "AcquisitionSourceReclamationBusy",
    "AcquisitionSourceReclamationCapacityError",
    "AcquisitionSourceReclamationConflict",
    "AcquisitionSourceReclamationCoordinator",
    "AcquisitionSourceReclamationCoordinatorError",
    "AcquisitionSourceReclamationDurabilityError",
    "AcquisitionSourceReclamationIntegrityError",
    "build_fail_closed_reclamation_authority",
    "capability_catalog_mapping",
    "capability_catalog_sha256",
    "parse_fail_closed_reclamation_authority",
    "validate_fail_closed_reclamation_authority",
    "validate_participant_prepare_receipt",
    "validate_reclamation_prepare_request",
]
