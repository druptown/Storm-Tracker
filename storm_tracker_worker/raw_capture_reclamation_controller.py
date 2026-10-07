"""Explicit, offline-only operator boundary for RAW source reclamation.

The controller never discovers or selects candidates.  It advances one exact,
content-addressed v2 request only when a phase-specific operator authorization
and a physically held archive/hold/golden-reference proof agree.  The durable
five-role coordinator remains the sole source mutation authority.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator, Mapping
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass
import json
import math
import re
import threading
import time
from typing import Any, Callable, Final

from .acquisition_source_reclamation_apply_authority import (
    AcquisitionSourceReclamationApplyCapacityError,
    AcquisitionSourceReclamationApplyConflict,
    AcquisitionSourceReclamationApplyCoordinator,
    AcquisitionSourceReclamationApplyIntegrityError,
)
from .acquisition_source_reclamation_protocol import (
    ABORTED,
    ABORTING,
    APPLIED,
    COMMITTED,
    PARTICIPANT_APPLY_ORDER,
    PREPARED,
    PREPARING,
    CommittedReclamationDecisionReceiptV1,
    ParticipantPrepareBindingV1,
    SourceLocalPrepareReceiptV2,
    SourceReclamationParticipantV2,
    SourceReclamationRequestV2,
    digest_mapping,
)


CONTRACT: Final = "stv3-worker-raw-capture-reclamation-controller-v1"
COMMAND_SCHEMA: Final = (
    "stv3-worker-raw-capture-reclamation-phase-command-v1"
)
BLOCKER_PROOF_SCHEMA: Final = (
    "stv3-worker-raw-capture-reclamation-blocker-proof-v1"
)
PREPARED_REVIEW_SCHEMA: Final = (
    "stv3-worker-raw-capture-reclamation-prepared-review-v1"
)
PHASE_RECEIPT_SCHEMA: Final = (
    "stv3-worker-raw-capture-reclamation-phase-receipt-v1"
)
DIAGNOSTICS_CONTRACT: Final = (
    "stv3-worker-raw-capture-reclamation-controller-diagnostics-v1"
)
OPERATOR_AUTHORITY_ID: Final = (
    "worker_authority.raw_capture_reclamation_operator_v1"
)
BLOCKER_AUTHORITY_ID: Final = (
    "worker_authority.raw_capture_reclamation_blockers_v1"
)

PREPARE: Final = "PREPARE"
COMMIT: Final = "COMMIT"
APPLY: Final = "APPLY"
ABORT: Final = "ABORT"
PHASES: Final = (PREPARE, COMMIT, APPLY, ABORT)

AUTOMATIC_CANDIDATE_SELECTION: Final = False
AUTOMATIC_RECLAMATION: Final = False
LIVE_SERVICE_WIRING: Final = False
METEOROLOGICAL_OUTPUT: Final = False
DASHBOARD_OUTPUT: Final = False
PRODUCTION_ENABLE_READY: Final = False

MAX_DIAGNOSTIC_BYTES: Final = 12 * 1024
MAX_COUNTER: Final = 1_000_000_000
MAX_CANDIDATES: Final = 4096
MAX_RELEASE_COUNT: Final = 2**53
MAX_TIMESTAMP: Final = 32_503_680_000.0
MAX_AUTHORIZATION_SECONDS: Final = 24 * 60 * 60

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_NONCE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,191}$")


class RawCaptureReclamationControllerError(RuntimeError):
    """Privacy-safe base error for the offline controller boundary."""


class RawCaptureReclamationControllerDisabled(
    RawCaptureReclamationControllerError
):
    """The explicit controller enable bit is false."""


class RawCaptureReclamationControllerIntegrityError(
    RawCaptureReclamationControllerError
):
    """An authority, request, blocker proof, or receipt differs."""


class RawCaptureReclamationControllerConflict(
    RawCaptureReclamationControllerError
):
    """The requested phase is incompatible with durable state."""


class RawCaptureReclamationControllerCapacityError(
    RawCaptureReclamationControllerError
):
    """A bounded controller or upstream capacity limit was reached."""


def _sha256(value: object, label: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise RawCaptureReclamationControllerIntegrityError(
            f"{label}_invalid"
        )
    return value


def _count(value: object, label: str, *, maximum: int) -> int:
    if type(value) is not int or not 0 <= value <= maximum:
        raise RawCaptureReclamationControllerIntegrityError(
            f"{label}_invalid"
        )
    return value


def _timestamp(value: object, label: str) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        or not 0.0 <= float(value) <= MAX_TIMESTAMP
    ):
        raise RawCaptureReclamationControllerIntegrityError(
            f"{label}_invalid"
        )
    return float(value)


def _canonical(value: object, *, maximum: int = MAX_DIAGNOSTIC_BYTES) -> bytes:
    try:
        payload = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
    except (TypeError, ValueError, UnicodeEncodeError):
        raise RawCaptureReclamationControllerIntegrityError(
            "controller_canonical_value_invalid"
        ) from None
    if len(payload) > maximum:
        raise RawCaptureReclamationControllerCapacityError(
            "controller_document_capacity_exceeded"
        )
    return payload


@dataclass(frozen=True, slots=True, repr=False)
class RawCaptureReclamationBlockerProofV1:
    proof_sha256: str
    blocker_authority_sha256: str
    request_sha256: str
    candidate_bindings_sha256: str
    sealed_archive_anchor_sha256: str
    archive_binding_root_sha256: str
    archive_audit_sha256: str
    hold_inventory_sha256: str
    golden_reference_inventory_sha256: str
    candidate_count: int
    active_hold_overlap_count: int
    golden_reference_overlap_count: int
    archive_verified: bool
    hold_inventory_complete: bool
    golden_reference_inventory_complete: bool

    def __post_init__(self) -> None:
        for value, label in (
            (self.proof_sha256, "blocker_proof_sha256"),
            (self.blocker_authority_sha256, "blocker_authority_sha256"),
            (self.request_sha256, "blocker_request_sha256"),
            (self.candidate_bindings_sha256, "blocker_candidate_root_sha256"),
            (self.sealed_archive_anchor_sha256, "blocker_archive_anchor_sha256"),
            (self.archive_binding_root_sha256, "blocker_archive_root_sha256"),
            (self.archive_audit_sha256, "blocker_archive_audit_sha256"),
            (self.hold_inventory_sha256, "blocker_hold_inventory_sha256"),
            (
                self.golden_reference_inventory_sha256,
                "blocker_golden_inventory_sha256",
            ),
        ):
            _sha256(value, label)
        _count(self.candidate_count, "blocker_candidate_count", maximum=MAX_CANDIDATES)
        _count(
            self.active_hold_overlap_count,
            "blocker_active_hold_overlap_count",
            maximum=MAX_CANDIDATES,
        )
        _count(
            self.golden_reference_overlap_count,
            "blocker_golden_reference_overlap_count",
            maximum=MAX_CANDIDATES,
        )
        if (
            self.candidate_count < 1
            or type(self.archive_verified) is not bool
            or type(self.hold_inventory_complete) is not bool
            or type(self.golden_reference_inventory_complete) is not bool
        ):
            raise RawCaptureReclamationControllerIntegrityError(
                "blocker_proof_shape_invalid"
            )
        if self.proof_sha256 != digest_mapping(self._core()):
            raise RawCaptureReclamationControllerIntegrityError(
                "blocker_proof_checksum_mismatch"
            )

    def _core(self) -> dict[str, Any]:
        return {
            "active_hold_overlap_count": self.active_hold_overlap_count,
            "archive_audit_sha256": self.archive_audit_sha256,
            "archive_binding_root_sha256": self.archive_binding_root_sha256,
            "archive_verified": self.archive_verified,
            "blocker_authority_id": BLOCKER_AUTHORITY_ID,
            "blocker_authority_sha256": self.blocker_authority_sha256,
            "candidate_bindings_sha256": self.candidate_bindings_sha256,
            "candidate_count": self.candidate_count,
            "golden_reference_inventory_complete": (
                self.golden_reference_inventory_complete
            ),
            "golden_reference_inventory_sha256": (
                self.golden_reference_inventory_sha256
            ),
            "golden_reference_overlap_count": (
                self.golden_reference_overlap_count
            ),
            "hold_inventory_complete": self.hold_inventory_complete,
            "hold_inventory_sha256": self.hold_inventory_sha256,
            "request_sha256": self.request_sha256,
            "schema": BLOCKER_PROOF_SCHEMA,
            "sealed_archive_anchor_sha256": (
                self.sealed_archive_anchor_sha256
            ),
        }

    @property
    def clear(self) -> bool:
        return (
            self.archive_verified
            and self.hold_inventory_complete
            and self.golden_reference_inventory_complete
            and self.active_hold_overlap_count == 0
            and self.golden_reference_overlap_count == 0
        )

    def as_dict(self) -> dict[str, Any]:
        return {**self._core(), "proof_sha256": self.proof_sha256}

    @classmethod
    def create(
        cls,
        *,
        blocker_authority_sha256: str,
        request_sha256: str,
        candidate_bindings_sha256: str,
        sealed_archive_anchor_sha256: str,
        archive_binding_root_sha256: str,
        archive_audit_sha256: str,
        hold_inventory_sha256: str,
        golden_reference_inventory_sha256: str,
        candidate_count: int,
        active_hold_overlap_count: int,
        golden_reference_overlap_count: int,
        archive_verified: bool,
        hold_inventory_complete: bool,
        golden_reference_inventory_complete: bool,
    ) -> "RawCaptureReclamationBlockerProofV1":
        values = {
            "active_hold_overlap_count": active_hold_overlap_count,
            "archive_audit_sha256": archive_audit_sha256,
            "archive_binding_root_sha256": archive_binding_root_sha256,
            "archive_verified": archive_verified,
            "blocker_authority_sha256": blocker_authority_sha256,
            "candidate_bindings_sha256": candidate_bindings_sha256,
            "candidate_count": candidate_count,
            "golden_reference_inventory_complete": (
                golden_reference_inventory_complete
            ),
            "golden_reference_inventory_sha256": (
                golden_reference_inventory_sha256
            ),
            "golden_reference_overlap_count": golden_reference_overlap_count,
            "hold_inventory_complete": hold_inventory_complete,
            "hold_inventory_sha256": hold_inventory_sha256,
            "request_sha256": request_sha256,
            "sealed_archive_anchor_sha256": sealed_archive_anchor_sha256,
        }
        core = {
            **values,
            "blocker_authority_id": BLOCKER_AUTHORITY_ID,
            "schema": BLOCKER_PROOF_SCHEMA,
        }
        return cls(proof_sha256=digest_mapping(core), **values)


@dataclass(frozen=True, slots=True, repr=False)
class RawCaptureReclamationPhaseCommandV1:
    command_sha256: str
    operator_authority_sha256: str
    phase: str
    request_sha256: str
    candidate_authority_sha256: str
    coordinator_authority_sha256: str
    candidate_bindings_sha256: str
    sealed_archive_anchor_sha256: str
    archive_binding_root_sha256: str
    blocker_proof_sha256: str
    prepared_review_sha256: str | None
    committed_receipt_sha256: str | None
    candidate_count: int
    max_releasable_bytes_per_role: int
    max_releasable_entries_per_role: int
    issued_at: float
    expires_at: float
    nonce: str
    operator_initiated: bool = True
    automatic_candidate_selection: bool = False

    def __post_init__(self) -> None:
        for value, label in (
            (self.command_sha256, "command_sha256"),
            (self.operator_authority_sha256, "operator_authority_sha256"),
            (self.request_sha256, "command_request_sha256"),
            (self.candidate_authority_sha256, "command_candidate_authority_sha256"),
            (self.coordinator_authority_sha256, "command_coordinator_authority_sha256"),
            (self.candidate_bindings_sha256, "command_candidate_root_sha256"),
            (self.sealed_archive_anchor_sha256, "command_archive_anchor_sha256"),
            (self.archive_binding_root_sha256, "command_archive_root_sha256"),
            (self.blocker_proof_sha256, "command_blocker_proof_sha256"),
        ):
            _sha256(value, label)
        if self.phase not in PHASES:
            raise RawCaptureReclamationControllerIntegrityError(
                "command_phase_invalid"
            )
        for value, label in (
            (self.prepared_review_sha256, "command_prepared_review_sha256"),
            (self.committed_receipt_sha256, "command_committed_receipt_sha256"),
        ):
            if value is not None:
                _sha256(value, label)
        _count(self.candidate_count, "command_candidate_count", maximum=MAX_CANDIDATES)
        _count(
            self.max_releasable_bytes_per_role,
            "command_releasable_bytes",
            maximum=MAX_RELEASE_COUNT,
        )
        _count(
            self.max_releasable_entries_per_role,
            "command_releasable_entries",
            maximum=MAX_RELEASE_COUNT,
        )
        issued = _timestamp(self.issued_at, "command_issued_at")
        expires = _timestamp(self.expires_at, "command_expires_at")
        if (
            self.candidate_count < 1
            or not isinstance(self.nonce, str)
            or _NONCE.fullmatch(self.nonce) is None
            or self.operator_initiated is not True
            or self.automatic_candidate_selection is not False
            or expires <= issued
            or expires - issued > MAX_AUTHORIZATION_SECONDS
        ):
            raise RawCaptureReclamationControllerIntegrityError(
                "command_shape_invalid"
            )
        if self.phase == PREPARE:
            if (
                self.prepared_review_sha256 is not None
                or self.committed_receipt_sha256 is not None
            ):
                raise RawCaptureReclamationControllerIntegrityError(
                    "prepare_command_evidence_invalid"
                )
        elif self.phase == COMMIT:
            if (
                self.prepared_review_sha256 is None
                or self.committed_receipt_sha256 is not None
            ):
                raise RawCaptureReclamationControllerIntegrityError(
                    "commit_command_evidence_invalid"
                )
        elif self.phase == APPLY:
            if (
                self.prepared_review_sha256 is None
                or self.committed_receipt_sha256 is None
            ):
                raise RawCaptureReclamationControllerIntegrityError(
                    "apply_command_evidence_invalid"
                )
        elif (
            self.prepared_review_sha256 is not None
            or self.committed_receipt_sha256 is not None
            or self.max_releasable_bytes_per_role != 0
            or self.max_releasable_entries_per_role != 0
        ):
            raise RawCaptureReclamationControllerIntegrityError(
                "abort_command_evidence_invalid"
            )
        if self.command_sha256 != digest_mapping(self._core()):
            raise RawCaptureReclamationControllerIntegrityError(
                "command_checksum_mismatch"
            )

    def _core(self) -> dict[str, Any]:
        return {
            "archive_binding_root_sha256": self.archive_binding_root_sha256,
            "automatic_candidate_selection": self.automatic_candidate_selection,
            "blocker_proof_sha256": self.blocker_proof_sha256,
            "candidate_authority_sha256": self.candidate_authority_sha256,
            "candidate_bindings_sha256": self.candidate_bindings_sha256,
            "candidate_count": self.candidate_count,
            "committed_receipt_sha256": self.committed_receipt_sha256,
            "coordinator_authority_sha256": self.coordinator_authority_sha256,
            "expires_at": float(self.expires_at),
            "issued_at": float(self.issued_at),
            "max_releasable_bytes_per_role": self.max_releasable_bytes_per_role,
            "max_releasable_entries_per_role": self.max_releasable_entries_per_role,
            "nonce": self.nonce,
            "operator_authority_id": OPERATOR_AUTHORITY_ID,
            "operator_authority_sha256": self.operator_authority_sha256,
            "operator_initiated": self.operator_initiated,
            "phase": self.phase,
            "prepared_review_sha256": self.prepared_review_sha256,
            "request_sha256": self.request_sha256,
            "schema": COMMAND_SCHEMA,
            "sealed_archive_anchor_sha256": self.sealed_archive_anchor_sha256,
        }

    def as_dict(self) -> dict[str, Any]:
        return {**self._core(), "command_sha256": self.command_sha256}

    @classmethod
    def create(
        cls,
        *,
        operator_authority_sha256: str,
        phase: str,
        request_sha256: str,
        candidate_authority_sha256: str,
        coordinator_authority_sha256: str,
        candidate_bindings_sha256: str,
        sealed_archive_anchor_sha256: str,
        archive_binding_root_sha256: str,
        blocker_proof_sha256: str,
        candidate_count: int,
        max_releasable_bytes_per_role: int,
        max_releasable_entries_per_role: int,
        issued_at: float,
        expires_at: float,
        nonce: str,
        prepared_review_sha256: str | None = None,
        committed_receipt_sha256: str | None = None,
    ) -> "RawCaptureReclamationPhaseCommandV1":
        issued = _timestamp(issued_at, "command_issued_at")
        expires = _timestamp(expires_at, "command_expires_at")
        core = {
            "archive_binding_root_sha256": archive_binding_root_sha256,
            "automatic_candidate_selection": False,
            "blocker_proof_sha256": blocker_proof_sha256,
            "candidate_authority_sha256": candidate_authority_sha256,
            "candidate_bindings_sha256": candidate_bindings_sha256,
            "candidate_count": candidate_count,
            "committed_receipt_sha256": committed_receipt_sha256,
            "coordinator_authority_sha256": coordinator_authority_sha256,
            "expires_at": expires,
            "issued_at": issued,
            "max_releasable_bytes_per_role": max_releasable_bytes_per_role,
            "max_releasable_entries_per_role": max_releasable_entries_per_role,
            "nonce": nonce,
            "operator_authority_id": OPERATOR_AUTHORITY_ID,
            "operator_authority_sha256": operator_authority_sha256,
            "operator_initiated": True,
            "phase": phase,
            "prepared_review_sha256": prepared_review_sha256,
            "request_sha256": request_sha256,
            "schema": COMMAND_SCHEMA,
            "sealed_archive_anchor_sha256": sealed_archive_anchor_sha256,
        }
        constructor = {
            key: value
            for key, value in core.items()
            if key not in {"operator_authority_id", "schema"}
        }
        return cls(command_sha256=digest_mapping(core), **constructor)


class RawCaptureReclamationOperatorAuthority(ABC):
    """Constructor-bound physical authority for explicit operator commands."""

    __slots__ = ("_authority_sha256",)

    def __init__(self, authority_sha256: str) -> None:
        self._authority_sha256 = _sha256(
            authority_sha256,
            "operator_authority_sha256",
        )

    @property
    def authority_id(self) -> str:
        return OPERATOR_AUTHORITY_ID

    @property
    def authority_sha256(self) -> str:
        return self._authority_sha256

    @abstractmethod
    def hold_phase_authorization_v1(
        self,
        command: RawCaptureReclamationPhaseCommandV1,
    ) -> AbstractContextManager[RawCaptureReclamationPhaseCommandV1]:
        """Hold and reproof the exact non-revoked operator command."""


class RawCaptureReclamationBlockerAuthority(ABC):
    """Physical verifier for archive, hold and golden-reference blockers."""

    __slots__ = ("_authority_sha256",)

    def __init__(self, authority_sha256: str) -> None:
        self._authority_sha256 = _sha256(
            authority_sha256,
            "blocker_authority_sha256",
        )

    @property
    def authority_id(self) -> str:
        return BLOCKER_AUTHORITY_ID

    @property
    def authority_sha256(self) -> str:
        return self._authority_sha256

    @abstractmethod
    def hold_verified_blockers_v1(
        self,
        request: SourceReclamationRequestV2,
        *,
        expected_proof_sha256: str,
    ) -> AbstractContextManager[RawCaptureReclamationBlockerProofV1]:
        """Hold complete archive/hold/golden inventories through one phase."""


@dataclass(frozen=True, slots=True)
class RawCaptureReclamationControllerLimits:
    max_candidates_per_request: int = MAX_CANDIDATES
    max_releasable_bytes_per_role: int = MAX_RELEASE_COUNT
    max_releasable_entries_per_role: int = MAX_RELEASE_COUNT
    max_diagnostic_bytes: int = MAX_DIAGNOSTIC_BYTES

    def __post_init__(self) -> None:
        for value, label, maximum in (
            (
                self.max_candidates_per_request,
                "controller_candidate_limit",
                MAX_CANDIDATES,
            ),
            (
                self.max_releasable_bytes_per_role,
                "controller_release_byte_limit",
                MAX_RELEASE_COUNT,
            ),
            (
                self.max_releasable_entries_per_role,
                "controller_release_entry_limit",
                MAX_RELEASE_COUNT,
            ),
        ):
            _count(value, label, maximum=maximum)
            if value < 1:
                raise ValueError(f"{label}_invalid")
        if not 2048 <= self.max_diagnostic_bytes <= MAX_DIAGNOSTIC_BYTES:
            raise ValueError("controller_diagnostic_limit_invalid")


@dataclass(frozen=True, slots=True, repr=False)
class RawCaptureReclamationPreparedReviewV1:
    """Stable PREPARED evidence that remains replayable after state advances.

    Mutable coordinator snapshot identities deliberately do not participate in
    this review.  The exact durable five-role prepare receipts do, via
    ``participant_prepare_root_sha256``.
    """

    review_sha256: str
    request_sha256: str
    blocker_proof_sha256: str
    participant_prepare_root_sha256: str
    candidate_count: int
    max_releasable_bytes_per_role: int
    max_releasable_entries_per_role: int

    def __post_init__(self) -> None:
        for value, label in (
            (self.review_sha256, "review_sha256"),
            (self.request_sha256, "review_request_sha256"),
            (self.blocker_proof_sha256, "review_blocker_proof_sha256"),
            (self.participant_prepare_root_sha256, "review_prepare_root_sha256"),
        ):
            _sha256(value, label)
        _count(self.candidate_count, "review_candidate_count", maximum=MAX_CANDIDATES)
        _count(
            self.max_releasable_bytes_per_role,
            "review_releasable_bytes",
            maximum=MAX_RELEASE_COUNT,
        )
        _count(
            self.max_releasable_entries_per_role,
            "review_releasable_entries",
            maximum=MAX_RELEASE_COUNT,
        )
        if self.candidate_count < 1 or self.review_sha256 != digest_mapping(
            self._core()
        ):
            raise RawCaptureReclamationControllerIntegrityError(
                "prepared_review_invalid"
            )

    def _core(self) -> dict[str, Any]:
        return {
            "blocker_proof_sha256": self.blocker_proof_sha256,
            "candidate_count": self.candidate_count,
            "max_releasable_bytes_per_role": self.max_releasable_bytes_per_role,
            "max_releasable_entries_per_role": self.max_releasable_entries_per_role,
            "participant_prepare_root_sha256": self.participant_prepare_root_sha256,
            "request_sha256": self.request_sha256,
            "schema": PREPARED_REVIEW_SCHEMA,
        }

    def as_dict(self) -> dict[str, Any]:
        return {**self._core(), "review_sha256": self.review_sha256}

    @classmethod
    def create(cls, **values: Any) -> "RawCaptureReclamationPreparedReviewV1":
        core = {**values, "schema": PREPARED_REVIEW_SCHEMA}
        return cls(review_sha256=digest_mapping(core), **values)


@dataclass(frozen=True, slots=True, repr=False)
class RawCaptureReclamationPhaseReceiptV1:
    receipt_sha256: str
    command_sha256: str
    blocker_proof_sha256: str
    request_sha256: str
    phase: str
    coordinator_snapshot_sha256: str
    state: str
    prepared_review_sha256: str | None
    committed_receipt_sha256: str | None
    prepared_role_count: int
    applied_role_count: int
    source_release_performed: bool

    def __post_init__(self) -> None:
        for value, label in (
            (self.receipt_sha256, "phase_receipt_sha256"),
            (self.command_sha256, "phase_receipt_command_sha256"),
            (self.blocker_proof_sha256, "phase_receipt_blocker_sha256"),
            (self.request_sha256, "phase_receipt_request_sha256"),
            (self.coordinator_snapshot_sha256, "phase_receipt_snapshot_sha256"),
        ):
            _sha256(value, label)
        if self.phase not in PHASES or self.state not in {
            PREPARING,
            PREPARED,
            COMMITTED,
            APPLIED,
            ABORTING,
            ABORTED,
        }:
            raise RawCaptureReclamationControllerIntegrityError(
                "phase_receipt_state_invalid"
            )
        for value, label in (
            (self.prepared_review_sha256, "phase_receipt_review_sha256"),
            (self.committed_receipt_sha256, "phase_receipt_committed_sha256"),
        ):
            if value is not None:
                _sha256(value, label)
        _count(
            self.prepared_role_count,
            "phase_receipt_prepared_count",
            maximum=len(PARTICIPANT_APPLY_ORDER),
        )
        _count(
            self.applied_role_count,
            "phase_receipt_applied_count",
            maximum=len(PARTICIPANT_APPLY_ORDER),
        )
        if (
            type(self.source_release_performed) is not bool
            or self.source_release_performed != (self.state == APPLIED)
            or self.receipt_sha256 != digest_mapping(self._core())
        ):
            raise RawCaptureReclamationControllerIntegrityError(
                "phase_receipt_invalid"
            )

    def _core(self) -> dict[str, Any]:
        return {
            "applied_role_count": self.applied_role_count,
            "blocker_proof_sha256": self.blocker_proof_sha256,
            "command_sha256": self.command_sha256,
            "committed_receipt_sha256": self.committed_receipt_sha256,
            "coordinator_snapshot_sha256": self.coordinator_snapshot_sha256,
            "phase": self.phase,
            "prepared_review_sha256": self.prepared_review_sha256,
            "prepared_role_count": self.prepared_role_count,
            "request_sha256": self.request_sha256,
            "schema": PHASE_RECEIPT_SCHEMA,
            "source_release_performed": self.source_release_performed,
            "state": self.state,
        }

    def as_dict(self) -> dict[str, Any]:
        return {**self._core(), "receipt_sha256": self.receipt_sha256}

    @classmethod
    def create(cls, **values: Any) -> "RawCaptureReclamationPhaseReceiptV1":
        core = {**values, "schema": PHASE_RECEIPT_SCHEMA}
        return cls(receipt_sha256=digest_mapping(core), **values)


class RawCaptureReclamationController:
    """Explicit phase controller over the durable offline five-role APIs."""

    def __init__(
        self,
        *,
        coordinator: AcquisitionSourceReclamationApplyCoordinator,
        participants: tuple[SourceReclamationParticipantV2, ...],
        operator_authority: RawCaptureReclamationOperatorAuthority,
        blocker_authority: RawCaptureReclamationBlockerAuthority,
        enabled: bool = False,
        limits: RawCaptureReclamationControllerLimits | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        if type(enabled) is not bool:
            raise TypeError("enabled must be bool")
        if not isinstance(
            coordinator,
            AcquisitionSourceReclamationApplyCoordinator,
        ):
            raise TypeError("coordinator has wrong type")
        if not isinstance(
            operator_authority,
            RawCaptureReclamationOperatorAuthority,
        ) or operator_authority.authority_id != OPERATOR_AUTHORITY_ID:
            raise TypeError("operator authority has wrong type or ID")
        if not isinstance(
            blocker_authority,
            RawCaptureReclamationBlockerAuthority,
        ) or blocker_authority.authority_id != BLOCKER_AUTHORITY_ID:
            raise TypeError("blocker authority has wrong type or ID")
        if (
            not isinstance(participants, tuple)
            or tuple(getattr(item, "role", None) for item in participants)
            != PARTICIPANT_APPLY_ORDER
            or any(
                not isinstance(item, SourceReclamationParticipantV2)
                for item in participants
            )
        ):
            raise TypeError("participants differ from the five-role contract")
        if not callable(clock):
            raise TypeError("clock must be callable")
        candidate_authority_sha256 = _sha256(
            coordinator.candidate_authority_sha256,
            "controller_candidate_authority_sha256",
        )
        coordinator_authority_sha256 = _sha256(
            coordinator.authority_sha256,
            "controller_coordinator_authority_sha256",
        )
        source_store_identities: set[str] = set()
        for participant in participants:
            source_store_identity_sha256 = _sha256(
                participant.source_store_identity_sha256,
                "controller_participant_store_identity_sha256",
            )
            if (
                participant.candidate_authority_sha256
                != candidate_authority_sha256
                or participant.coordinator_authority_sha256
                != coordinator_authority_sha256
                or source_store_identity_sha256 in source_store_identities
            ):
                raise RawCaptureReclamationControllerIntegrityError(
                    "controller_participant_authority_mismatch"
                )
            source_store_identities.add(source_store_identity_sha256)
        self._coordinator = coordinator
        self._participants = participants
        self._operator_authority = operator_authority
        self._blocker_authority = blocker_authority
        self._enabled = enabled
        self._limits = limits or RawCaptureReclamationControllerLimits()
        self._clock = clock
        self._candidate_authority_sha256 = candidate_authority_sha256
        self._coordinator_authority_sha256 = coordinator_authority_sha256
        self._lock = threading.RLock()
        self._last_error_code: str | None = None
        self._last_receipt: RawCaptureReclamationPhaseReceiptV1 | None = None
        self._counts = {
            "abort_calls": 0,
            "apply_calls": 0,
            "blocked_calls": 0,
            "commit_calls": 0,
            "failures": 0,
            "prepare_calls": 0,
            "review_calls": 0,
        }

    def _increment(self, key: str) -> None:
        self._counts[key] = min(MAX_COUNTER, self._counts[key] + 1)

    def _require_enabled(self) -> None:
        if not self._enabled:
            self._increment("blocked_calls")
            self._last_error_code = "controller_disabled"
            raise RawCaptureReclamationControllerDisabled(
                "controller_disabled"
            )

    def _validate_request(
        self,
        request: SourceReclamationRequestV2,
    ) -> None:
        if not isinstance(request, SourceReclamationRequestV2):
            raise TypeError("request must be SourceReclamationRequestV2")
        if (
            request.candidate_authority_sha256
            != self._candidate_authority_sha256
            or request.candidate_count > self._limits.max_candidates_per_request
        ):
            raise RawCaptureReclamationControllerIntegrityError(
                "request_authority_or_capacity_mismatch"
            )

    def _validate_command(
        self,
        request: SourceReclamationRequestV2,
        command: RawCaptureReclamationPhaseCommandV1,
        *,
        phase: str,
    ) -> None:
        self._validate_request(request)
        if not isinstance(command, RawCaptureReclamationPhaseCommandV1):
            raise TypeError("command must be RawCaptureReclamationPhaseCommandV1")
        now = _timestamp(self._clock(), "controller_clock")
        if (
            command.phase != phase
            or command.operator_authority_sha256
            != self._operator_authority.authority_sha256
            or command.request_sha256 != request.request_sha256
            or command.candidate_authority_sha256
            != request.candidate_authority_sha256
            or command.coordinator_authority_sha256
            != self._coordinator_authority_sha256
            or command.candidate_bindings_sha256
            != request.candidate_bindings_sha256
            or command.sealed_archive_anchor_sha256
            != request.sealed_archive_anchor_sha256
            or command.archive_binding_root_sha256
            != request.archive_binding_root_sha256
            or command.candidate_count != request.candidate_count
            or command.max_releasable_bytes_per_role
            > self._limits.max_releasable_bytes_per_role
            or command.max_releasable_entries_per_role
            > self._limits.max_releasable_entries_per_role
            or now < command.issued_at
            or now > command.expires_at
        ):
            raise RawCaptureReclamationControllerIntegrityError(
                "phase_command_authority_mismatch"
            )

    def _validate_blocker_proof(
        self,
        request: SourceReclamationRequestV2,
        command: RawCaptureReclamationPhaseCommandV1,
        proof: RawCaptureReclamationBlockerProofV1,
        *,
        require_clear: bool,
    ) -> None:
        if (
            not isinstance(proof, RawCaptureReclamationBlockerProofV1)
            or proof.proof_sha256 != command.blocker_proof_sha256
            or proof.blocker_authority_sha256
            != self._blocker_authority.authority_sha256
            or proof.request_sha256 != request.request_sha256
            or proof.candidate_bindings_sha256
            != request.candidate_bindings_sha256
            or proof.sealed_archive_anchor_sha256
            != request.sealed_archive_anchor_sha256
            or proof.archive_binding_root_sha256
            != request.archive_binding_root_sha256
            or proof.candidate_count != request.candidate_count
        ):
            raise RawCaptureReclamationControllerIntegrityError(
                "reclamation_blocker_proof_mismatch"
            )
        if require_clear and not proof.clear:
            raise RawCaptureReclamationControllerConflict(
                "reclamation_blocker_proof_not_clear"
            )

    @contextmanager
    def _held_authorities(
        self,
        request: SourceReclamationRequestV2,
        command: RawCaptureReclamationPhaseCommandV1,
        *,
        phase: str,
    ) -> Iterator[RawCaptureReclamationBlockerProofV1]:
        try:
            self._validate_command(request, command, phase=phase)
            with self._operator_authority.hold_phase_authorization_v1(
                command
            ) as held_command:
                if held_command != command:
                    raise RawCaptureReclamationControllerIntegrityError(
                        "held_operator_command_mismatch"
                    )
                with self._blocker_authority.hold_verified_blockers_v1(
                    request,
                    expected_proof_sha256=command.blocker_proof_sha256,
                ) as proof:
                    self._validate_blocker_proof(
                        request,
                        command,
                        proof,
                        require_clear=(phase != ABORT),
                    )
                    yield proof
        except RawCaptureReclamationControllerCapacityError:
            raise RawCaptureReclamationControllerCapacityError(
                "controller_capacity_rejected"
            ) from None
        except RawCaptureReclamationControllerConflict:
            raise RawCaptureReclamationControllerConflict(
                "controller_conflict"
            ) from None
        except RawCaptureReclamationControllerIntegrityError:
            raise RawCaptureReclamationControllerIntegrityError(
                "controller_integrity_rejected"
            ) from None
        except RawCaptureReclamationControllerError:
            raise RawCaptureReclamationControllerError(
                "controller_operation_rejected"
            ) from None
        except AcquisitionSourceReclamationApplyCapacityError:
            raise RawCaptureReclamationControllerCapacityError(
                "upstream_capacity_rejected"
            ) from None
        except AcquisitionSourceReclamationApplyConflict:
            raise RawCaptureReclamationControllerConflict(
                "upstream_state_conflict"
            ) from None
        except AcquisitionSourceReclamationApplyIntegrityError:
            raise RawCaptureReclamationControllerIntegrityError(
                "upstream_integrity_rejected"
            ) from None
        except Exception:
            raise RawCaptureReclamationControllerIntegrityError(
                "authority_reproof_failed"
            ) from None

    def _prepared_review(
        self,
        request: SourceReclamationRequestV2,
        *,
        blocker_proof_sha256: str,
    ) -> RawCaptureReclamationPreparedReviewV1:
        snapshot = self._coordinator.load_state_v2(request.request_sha256)
        if snapshot is None or snapshot.state not in {
            PREPARED,
            COMMITTED,
            APPLIED,
        }:
            raise RawCaptureReclamationControllerConflict(
                "prepared_review_state_unavailable"
            )
        bindings: list[ParticipantPrepareBindingV1] = []
        maximum_bytes = 0
        maximum_entries = 0
        for role, participant in zip(
            PARTICIPANT_APPLY_ORDER,
            self._participants,
            strict=True,
        ):
            receipt = participant.load_source_local_prepare_v2(
                request.request_sha256
            )
            if (
                not isinstance(receipt, SourceLocalPrepareReceiptV2)
                or receipt.role != role
                or receipt.request_sha256 != request.request_sha256
                or receipt.coordinator_authority_sha256
                != self._coordinator_authority_sha256
                or receipt.candidate_authority_sha256
                != self._candidate_authority_sha256
                or receipt.source_store_identity_sha256
                != participant.source_store_identity_sha256
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
                or receipt.candidate_count != request.candidate_count
            ):
                raise RawCaptureReclamationControllerIntegrityError(
                    "participant_prepare_receipt_mismatch"
                )
            maximum_bytes = max(maximum_bytes, receipt.releasable_bytes)
            maximum_entries = max(maximum_entries, receipt.releasable_entries)
            bindings.append(ParticipantPrepareBindingV1(
                role=role,
                prepare_receipt_sha256=receipt.prepare_receipt_sha256,
                source_store_identity_sha256=(
                    receipt.source_store_identity_sha256
                ),
                source_fence_sha256=receipt.source_fence_sha256,
            ))
        participant_root = digest_mapping(
            [item.as_dict() for item in bindings]
        )
        return RawCaptureReclamationPreparedReviewV1.create(
            request_sha256=request.request_sha256,
            blocker_proof_sha256=blocker_proof_sha256,
            participant_prepare_root_sha256=participant_root,
            candidate_count=request.candidate_count,
            max_releasable_bytes_per_role=maximum_bytes,
            max_releasable_entries_per_role=maximum_entries,
        )

    @staticmethod
    def _validate_review_capacity(
        review: RawCaptureReclamationPreparedReviewV1,
        command: RawCaptureReclamationPhaseCommandV1,
    ) -> None:
        if (
            review.max_releasable_bytes_per_role
            > command.max_releasable_bytes_per_role
            or review.max_releasable_entries_per_role
            > command.max_releasable_entries_per_role
        ):
            raise RawCaptureReclamationControllerCapacityError(
                "operator_release_envelope_exceeded"
            )

    def _validate_committed_receipt(
        self,
        request: SourceReclamationRequestV2,
        review: RawCaptureReclamationPreparedReviewV1,
        committed: CommittedReclamationDecisionReceiptV1,
    ) -> None:
        if (
            not isinstance(
                committed,
                CommittedReclamationDecisionReceiptV1,
            )
            or committed.request_sha256 != request.request_sha256
            or committed.candidate_authority_sha256
            != self._candidate_authority_sha256
            or committed.sealed_archive_anchor_sha256
            != request.sealed_archive_anchor_sha256
            or committed.archive_binding_root_sha256
            != request.archive_binding_root_sha256
            or committed.candidate_bindings_sha256
            != request.candidate_bindings_sha256
            or committed.consumer_registry_root_sha256
            != request.consumer_registry_root_sha256
            or committed.consumer_epoch_root_sha256
            != request.consumer_epoch_root_sha256
            or committed.participant_prepare_root_sha256
            != review.participant_prepare_root_sha256
            or committed.candidate_count != request.candidate_count
        ):
            raise RawCaptureReclamationControllerIntegrityError(
                "committed_receipt_mismatch"
            )

    @staticmethod
    def _receipt(
        *,
        command: RawCaptureReclamationPhaseCommandV1,
        proof: RawCaptureReclamationBlockerProofV1,
        snapshot: Any,
        review: RawCaptureReclamationPreparedReviewV1 | None,
        committed_receipt_sha256: str | None,
    ) -> RawCaptureReclamationPhaseReceiptV1:
        return RawCaptureReclamationPhaseReceiptV1.create(
            command_sha256=command.command_sha256,
            blocker_proof_sha256=proof.proof_sha256,
            request_sha256=command.request_sha256,
            phase=command.phase,
            coordinator_snapshot_sha256=snapshot.snapshot_sha256,
            state=snapshot.state,
            prepared_review_sha256=(
                None if review is None else review.review_sha256
            ),
            committed_receipt_sha256=committed_receipt_sha256,
            prepared_role_count=len(snapshot.prepared_roles),
            applied_role_count=len(snapshot.applied_roles),
            source_release_performed=(snapshot.state == APPLIED),
        )

    def _record_success(
        self,
        receipt: RawCaptureReclamationPhaseReceiptV1,
    ) -> RawCaptureReclamationPhaseReceiptV1:
        self._last_error_code = None
        self._last_receipt = receipt
        return receipt

    def _run_failure(self, error: Exception) -> None:
        self._increment("failures")
        if isinstance(error, RawCaptureReclamationControllerDisabled):
            code = "controller_disabled"
        elif isinstance(error, RawCaptureReclamationControllerCapacityError):
            code = "controller_capacity_rejected"
        elif isinstance(error, RawCaptureReclamationControllerConflict):
            code = "controller_conflict"
        elif isinstance(error, RawCaptureReclamationControllerIntegrityError):
            code = "controller_integrity_rejected"
        else:
            code = "controller_operation_failed"
        self._last_error_code = code

    def review_prepared_v1(
        self,
        request: SourceReclamationRequestV2,
        command: RawCaptureReclamationPhaseCommandV1,
    ) -> RawCaptureReclamationPreparedReviewV1:
        """Read exact durable PREPARED evidence; never advance state."""

        with self._lock:
            self._require_enabled()
            self._increment("review_calls")
            try:
                with self._held_authorities(
                    request,
                    command,
                    phase=PREPARE,
                ) as proof:
                    return self._prepared_review(
                        request,
                        blocker_proof_sha256=proof.proof_sha256,
                    )
            except Exception as error:
                self._run_failure(error)
                raise

    def prepare_v1(
        self,
        request: SourceReclamationRequestV2,
        command: RawCaptureReclamationPhaseCommandV1,
    ) -> RawCaptureReclamationPhaseReceiptV1:
        """Prepare exactly one supplied request; never commit or apply it."""

        with self._lock:
            self._require_enabled()
            self._increment("prepare_calls")
            try:
                with self._held_authorities(
                    request,
                    command,
                    phase=PREPARE,
                ) as proof:
                    snapshot = self._coordinator.prepare_v2(
                        request,
                        participants=self._participants,
                    )
                    if snapshot.state not in {PREPARED, COMMITTED, APPLIED}:
                        raise RawCaptureReclamationControllerConflict(
                            "prepare_did_not_reach_prepared_state"
                        )
                    review = self._prepared_review(
                        request,
                        blocker_proof_sha256=proof.proof_sha256,
                    )
                    self._validate_review_capacity(review, command)
                    return self._record_success(self._receipt(
                        command=command,
                        proof=proof,
                        snapshot=snapshot,
                        review=review,
                        committed_receipt_sha256=(
                            snapshot.committed_receipt_sha256
                        ),
                    ))
            except Exception as error:
                self._run_failure(error)
                raise

    def commit_v1(
        self,
        request: SourceReclamationRequestV2,
        command: RawCaptureReclamationPhaseCommandV1,
    ) -> RawCaptureReclamationPhaseReceiptV1:
        """Commit an operator-reviewed PREPARED request; never apply it."""

        with self._lock:
            self._require_enabled()
            self._increment("commit_calls")
            try:
                with self._held_authorities(
                    request,
                    command,
                    phase=COMMIT,
                ) as proof:
                    review = self._prepared_review(
                        request,
                        blocker_proof_sha256=proof.proof_sha256,
                    )
                    if review.review_sha256 != command.prepared_review_sha256:
                        raise RawCaptureReclamationControllerIntegrityError(
                            "commit_prepared_review_mismatch"
                        )
                    self._validate_review_capacity(review, command)
                    committed = self._coordinator.commit_v1(
                        request.request_sha256,
                        participants=self._participants,
                    )
                    self._validate_committed_receipt(
                        request,
                        review,
                        committed,
                    )
                    snapshot = self._coordinator.load_state_v2(
                        request.request_sha256
                    )
                    if snapshot is None or snapshot.state not in {
                        COMMITTED,
                        APPLIED,
                    } or snapshot.committed_receipt_sha256 != (
                        committed.receipt_sha256
                    ):
                        raise RawCaptureReclamationControllerIntegrityError(
                            "committed_state_mismatch"
                        )
                    return self._record_success(self._receipt(
                        command=command,
                        proof=proof,
                        snapshot=snapshot,
                        review=review,
                        committed_receipt_sha256=committed.receipt_sha256,
                    ))
            except Exception as error:
                self._run_failure(error)
                raise

    def apply_v1(
        self,
        request: SourceReclamationRequestV2,
        command: RawCaptureReclamationPhaseCommandV1,
    ) -> RawCaptureReclamationPhaseReceiptV1:
        """Apply the exact COMMITTED request in the closed five-role order."""

        with self._lock:
            self._require_enabled()
            self._increment("apply_calls")
            try:
                with self._held_authorities(
                    request,
                    command,
                    phase=APPLY,
                ) as proof:
                    review = self._prepared_review(
                        request,
                        blocker_proof_sha256=proof.proof_sha256,
                    )
                    if review.review_sha256 != command.prepared_review_sha256:
                        raise RawCaptureReclamationControllerIntegrityError(
                            "apply_prepared_review_mismatch"
                        )
                    self._validate_review_capacity(review, command)
                    before = self._coordinator.load_state_v2(
                        request.request_sha256
                    )
                    if (
                        before is None
                        or before.state not in {COMMITTED, APPLIED}
                        or before.committed_receipt_sha256
                        != command.committed_receipt_sha256
                    ):
                        raise RawCaptureReclamationControllerIntegrityError(
                            "apply_requires_exact_committed_state"
                        )
                    snapshot = self._coordinator.apply_v1(
                        request.request_sha256,
                        participants=self._participants,
                    )
                    if (
                        snapshot.state != APPLIED
                        or snapshot.applied_roles != PARTICIPANT_APPLY_ORDER
                        or snapshot.committed_receipt_sha256
                        != command.committed_receipt_sha256
                    ):
                        raise RawCaptureReclamationControllerIntegrityError(
                            "applied_state_mismatch"
                        )
                    return self._record_success(self._receipt(
                        command=command,
                        proof=proof,
                        snapshot=snapshot,
                        review=review,
                        committed_receipt_sha256=(
                            command.committed_receipt_sha256
                        ),
                    ))
            except Exception as error:
                self._run_failure(error)
                raise

    def abort_v1(
        self,
        request: SourceReclamationRequestV2,
        command: RawCaptureReclamationPhaseCommandV1,
    ) -> RawCaptureReclamationPhaseReceiptV1:
        """Explicitly abort prepared fences; this phase releases no source."""

        with self._lock:
            self._require_enabled()
            self._increment("abort_calls")
            try:
                with self._held_authorities(
                    request,
                    command,
                    phase=ABORT,
                ) as proof:
                    snapshot = self._coordinator.abort_v1(
                        request.request_sha256,
                        participants=self._participants,
                    )
                    if snapshot.state != ABORTED or snapshot.applied_roles:
                        raise RawCaptureReclamationControllerIntegrityError(
                            "aborted_state_mismatch"
                        )
                    return self._record_success(self._receipt(
                        command=command,
                        proof=proof,
                        snapshot=snapshot,
                        review=None,
                        committed_receipt_sha256=None,
                    ))
            except Exception as error:
                self._run_failure(error)
                raise

    def diagnostics(self) -> dict[str, Any]:
        """Return bounded hashes/counts only; never inspect a source store."""

        with self._lock:
            result = {
                "automatic_candidate_selection": AUTOMATIC_CANDIDATE_SELECTION,
                "automatic_reclamation": AUTOMATIC_RECLAMATION,
                "blocker_authority_id": BLOCKER_AUTHORITY_ID,
                "blocker_authority_sha256": (
                    self._blocker_authority.authority_sha256
                ),
                "candidate_authority_sha256": (
                    self._candidate_authority_sha256
                ),
                "contract": DIAGNOSTICS_CONTRACT,
                "controller_contract": CONTRACT,
                "coordinator_authority_sha256": (
                    self._coordinator_authority_sha256
                ),
                "counts": dict(self._counts),
                "dashboard_output": DASHBOARD_OUTPUT,
                "enabled": self._enabled,
                "last_error_code": self._last_error_code,
                "last_receipt_sha256": (
                    None
                    if self._last_receipt is None
                    else self._last_receipt.receipt_sha256
                ),
                "live_service_wiring": LIVE_SERVICE_WIRING,
                "meteorological_output": METEOROLOGICAL_OUTPUT,
                "operator_authority_id": OPERATOR_AUTHORITY_ID,
                "operator_authority_sha256": (
                    self._operator_authority.authority_sha256
                ),
                "participant_roles": list(PARTICIPANT_APPLY_ORDER),
                "production_enable_ready": PRODUCTION_ENABLE_READY,
            }
        _canonical(result, maximum=self._limits.max_diagnostic_bytes)
        return json.loads(json.dumps(result))


__all__ = [
    "ABORT",
    "APPLY",
    "AUTOMATIC_CANDIDATE_SELECTION",
    "AUTOMATIC_RECLAMATION",
    "BLOCKER_AUTHORITY_ID",
    "BLOCKER_PROOF_SCHEMA",
    "COMMIT",
    "COMMAND_SCHEMA",
    "CONTRACT",
    "DASHBOARD_OUTPUT",
    "DIAGNOSTICS_CONTRACT",
    "LIVE_SERVICE_WIRING",
    "METEOROLOGICAL_OUTPUT",
    "OPERATOR_AUTHORITY_ID",
    "PHASES",
    "PREPARE",
    "PRODUCTION_ENABLE_READY",
    "RawCaptureReclamationBlockerAuthority",
    "RawCaptureReclamationBlockerProofV1",
    "RawCaptureReclamationController",
    "RawCaptureReclamationControllerCapacityError",
    "RawCaptureReclamationControllerConflict",
    "RawCaptureReclamationControllerDisabled",
    "RawCaptureReclamationControllerError",
    "RawCaptureReclamationControllerIntegrityError",
    "RawCaptureReclamationControllerLimits",
    "RawCaptureReclamationOperatorAuthority",
    "RawCaptureReclamationPhaseCommandV1",
    "RawCaptureReclamationPhaseReceiptV1",
    "RawCaptureReclamationPreparedReviewV1",
]
