"""Dependency-light contracts for inert acquisition-source reclamation v2.

This module contains canonical, immutable request and receipt types only.  It
does not open a source store, delete a row, unlink a file, or activate worker
wiring.  The older fail-closed v1 reclamation authority deliberately remains a
separate contract and its PREPARED receipts are never accepted here.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass
from typing import Any, Iterator, Mapping, Protocol, runtime_checkable
import hashlib
import json
import math
import re


PROTOCOL_CONTRACT = "stv3-worker-acquisition-source-reclamation-protocol-v2"
REQUEST_SCHEMA = "stv3-worker-acquisition-source-reclamation-request-v2"
SOURCE_LOCAL_PREPARE_SCHEMA = (
    "stv3-worker-acquisition-source-local-prepare-receipt-v2"
)
COMMITTED_DECISION_SCHEMA = (
    "stv3-worker-acquisition-source-reclamation-committed-decision-v1"
)
COMMITTED_DECISION_RECEIPT_SCHEMA = (
    "stv3-worker-acquisition-source-reclamation-committed-receipt-v1"
)
COORDINATOR_ABORT_DECISION_SCHEMA = (
    "stv3-worker-acquisition-source-reclamation-abort-decision-v1"
)
SOURCE_LOCAL_APPLY_SCHEMA = (
    "stv3-worker-acquisition-source-local-apply-receipt-v1"
)
SOURCE_LOCAL_ABORT_SCHEMA = (
    "stv3-worker-acquisition-source-local-abort-receipt-v1"
)
RAW_UNLINK_JOURNAL_SCHEMA = (
    "stv3-worker-acquisition-source-raw-unlink-journal-receipt-v1"
)
PARTICIPANT_PREPARE_BINDING_SCHEMA = (
    "stv3-worker-acquisition-source-prepare-binding-v1"
)
CANDIDATE_BINDING_SCHEMA = (
    "stv3-worker-acquisition-source-reclamation-candidate-binding-v1"
)
CANDIDATE_PAGE_SCHEMA = (
    "stv3-worker-acquisition-source-reclamation-candidate-page-v1"
)
CANDIDATE_REFERENCE_RECORD_SCHEMA = (
    "stv3-worker-acquisition-source-reclamation-candidate-reference-v1"
)
CANDIDATE_BINDING_ROOT_SCHEMA = (
    "stv3-worker-acquisition-source-reclamation-candidate-root-v1"
)
ARCHIVE_BINDING_ROOT_SCHEMA = (
    "stv3-worker-acquisition-source-reclamation-archive-root-v1"
)
ORDERED_ROOT_LINK_SCHEMA = (
    "stv3-worker-acquisition-source-reclamation-ordered-root-link-v1"
)

PREPARING = "PREPARING"
PREPARED = "PREPARED"
COMMITTED = "COMMITTED"
APPLIED = "APPLIED"
ABORTING = "ABORTING"
ABORTED = "ABORTED"
COORDINATOR_STATES = (
    PREPARING,
    PREPARED,
    COMMITTED,
    APPLIED,
    ABORTING,
    ABORTED,
)

PARTICIPANT_APPLY_ORDER = (
    "downstream_lifecycle",
    "execution_receipts",
    "scheduler",
    "raw_artifacts",
    "capacity_ledger",
)
PARTICIPANT_ROLES = frozenset(PARTICIPANT_APPLY_ORDER)
MAX_CANDIDATES = 4096
MAX_RELEASE_COUNT = 2**53
MAX_TIMESTAMP = 32_503_680_000.0
MAX_CANONICAL_BYTES = 512 * 1024
MAX_CANDIDATE_PAGE_RECORDS = 64
MAX_CANDIDATE_REFERENCE_RECORD_BYTES = 64 * 1024
MAX_CANDIDATE_PAGE_BYTES = (
    MAX_CANDIDATE_PAGE_RECORDS * MAX_CANDIDATE_REFERENCE_RECORD_BYTES
)

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,191}$")


class AcquisitionSourceReclamationProtocolError(RuntimeError):
    """Base error for malformed or conflicting v2 protocol evidence."""


class AcquisitionSourceReclamationProtocolIntegrityError(
    AcquisitionSourceReclamationProtocolError
):
    """A canonical shape, checksum, role, state, or dependency differs."""


class AcquisitionSourceReclamationProtocolConflict(
    AcquisitionSourceReclamationProtocolError
):
    """A stable protocol identity was reused for different evidence."""


def canonical_bytes(value: object) -> bytes:
    """Return bounded canonical ASCII JSON used by every v2 identity."""

    try:
        result = json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
    except (TypeError, ValueError, UnicodeError, RecursionError) as exc:
        raise AcquisitionSourceReclamationProtocolIntegrityError(
            "reclamation protocol value is not canonical JSON"
        ) from exc
    if len(result) > MAX_CANONICAL_BYTES:
        raise AcquisitionSourceReclamationProtocolIntegrityError(
            "reclamation protocol value exceeds its canonical byte bound"
        )
    return result


def digest_mapping(value: object) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def _ordered_sha256_root(*, schema: str, item_sha256s: tuple[str, ...]) -> str:
    """Bounded streaming root; no candidate-count-sized canonical document."""

    if not isinstance(item_sha256s, tuple) or not item_sha256s:
        raise AcquisitionSourceReclamationProtocolIntegrityError(
            "ordered root inventory is empty"
        )
    head = digest_mapping(
        {"count": len(item_sha256s), "schema": schema, "start": True}
    )
    for ordinal, item_sha256 in enumerate(item_sha256s):
        head = digest_mapping(
            {
                "item_sha256": require_sha256(
                    item_sha256, "ordered root item SHA-256"
                ),
                "ordinal": ordinal,
                "previous_sha256": head,
                "schema": ORDERED_ROOT_LINK_SCHEMA,
            }
        )
    return digest_mapping(
        {
            "count": len(item_sha256s),
            "head_sha256": head,
            "schema": schema,
        }
    )


def candidate_binding_root(
    bindings: tuple["SourceReclamationCandidateBindingV1", ...],
) -> str:
    """Hash the complete candidate inventory in global candidate-SHA order."""

    if (
        not isinstance(bindings, tuple)
        or not 1 <= len(bindings) <= MAX_CANDIDATES
        or any(
            not isinstance(item, SourceReclamationCandidateBindingV1)
            for item in bindings
        )
    ):
        raise AcquisitionSourceReclamationProtocolIntegrityError(
            "candidate binding inventory shape differs"
        )
    ordered = tuple(sorted(bindings, key=lambda item: item.candidate_sha256))
    if bindings != ordered or len({item.candidate_sha256 for item in bindings}) != len(
        bindings
    ):
        raise AcquisitionSourceReclamationProtocolIntegrityError(
            "candidate binding inventory is not globally canonical"
        )
    return _ordered_sha256_root(
        schema=CANDIDATE_BINDING_ROOT_SCHEMA,
        item_sha256s=tuple(item.binding_sha256 for item in bindings),
    )


def archive_binding_root(
    bindings: tuple["SourceReclamationCandidateBindingV1", ...],
    record_payloads: tuple[bytes, ...],
) -> str:
    """Hash exact archive identities plus their bounded metadata references."""

    candidate_binding_root(bindings)
    if not isinstance(record_payloads, tuple) or len(record_payloads) != len(bindings):
        raise AcquisitionSourceReclamationProtocolIntegrityError(
            "archive binding inventory shape differs"
        )
    item_sha256s: list[str] = []
    for binding, payload in zip(bindings, record_payloads, strict=True):
        record = _decode_candidate_record(payload)
        if (
            record["candidate_sha256"] != binding.candidate_sha256
            or record["record_sha256"] != binding.sealed_archive_record_sha256
            or record["sealed_archive_segment_sha256"]
            != binding.sealed_archive_segment_sha256
        ):
            raise AcquisitionSourceReclamationProtocolIntegrityError(
                "archive metadata reference differs from candidate binding"
            )
        item_sha256s.append(
            digest_mapping({
                "candidate_sha256": binding.candidate_sha256,
                "metadata_reference_sha256": hashlib.sha256(payload).hexdigest(),
                "sealed_archive_record_sha256": (
                    binding.sealed_archive_record_sha256
                ),
                "sealed_archive_segment_sha256": (
                    binding.sealed_archive_segment_sha256
                ),
            })
        )
    return _ordered_sha256_root(
        schema=ARCHIVE_BINDING_ROOT_SCHEMA,
        item_sha256s=tuple(item_sha256s),
    )


def require_sha256(value: object, label: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise AcquisitionSourceReclamationProtocolIntegrityError(
            f"{label} is not SHA-256"
        )
    return value


def require_identifier(value: object, label: str) -> str:
    if not isinstance(value, str) or _IDENTIFIER.fullmatch(value) is None:
        raise AcquisitionSourceReclamationProtocolIntegrityError(
            f"{label} is invalid"
        )
    return value


def require_role(value: object) -> str:
    if not isinstance(value, str) or value not in PARTICIPANT_ROLES:
        raise AcquisitionSourceReclamationProtocolIntegrityError(
            "reclamation participant role is outside the closed catalogue"
        )
    return value


def require_count(value: object, label: str, *, maximum: int) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not 0 <= value <= maximum
    ):
        raise AcquisitionSourceReclamationProtocolIntegrityError(
            f"{label} is outside its bound"
        )
    return value


def require_timestamp(value: object, label: str) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        or not 0.0 <= float(value) <= MAX_TIMESTAMP
    ):
        raise AcquisitionSourceReclamationProtocolIntegrityError(
            f"{label} is invalid"
        )
    return float(value)


def _require_fixed_sha_tuple(
    values: object,
    *,
    label: str,
    length: int,
) -> tuple[str, ...]:
    if not isinstance(values, tuple) or len(values) != length:
        raise AcquisitionSourceReclamationProtocolIntegrityError(
            f"{label} shape differs"
        )
    result = tuple(require_sha256(item, label) for item in values)
    if len(set(result)) != len(result):
        raise AcquisitionSourceReclamationProtocolIntegrityError(
            f"{label} contains duplicates"
        )
    return result


def _decode_candidate_record(value: bytes) -> dict[str, Any]:
    if not isinstance(value, bytes) or not value:
        raise AcquisitionSourceReclamationProtocolIntegrityError(
            "candidate archive record is empty"
        )
    if len(value) > MAX_CANDIDATE_REFERENCE_RECORD_BYTES:
        raise AcquisitionSourceReclamationProtocolIntegrityError(
            "candidate metadata-reference record exceeds its byte bound"
        )
    try:
        decoded = json.loads(value.decode("ascii"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AcquisitionSourceReclamationProtocolIntegrityError(
            "candidate archive record is corrupt"
        ) from exc
    if not isinstance(decoded, dict):
        raise AcquisitionSourceReclamationProtocolIntegrityError(
            "candidate archive record is not a mapping"
        )
    try:
        canonical = json.dumps(
            decoded,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
    except (TypeError, ValueError, UnicodeError, RecursionError) as exc:
        raise AcquisitionSourceReclamationProtocolIntegrityError(
            "candidate archive record is not canonical JSON"
        ) from exc
    if canonical != value:
        raise AcquisitionSourceReclamationProtocolIntegrityError(
            "candidate archive record is not canonical"
        )
    expected = {
        "archive_object_count",
        "archive_object_reference_root_sha256",
        "archive_object_total_bytes",
        "capacity_ledger_view_sha256",
        "candidate_sha256",
        "downstream_lifecycle_view_sha256",
        "execution_receipts_view_sha256",
        "metadata_sha256",
        "raw_artifacts_view_sha256",
        "record_sha256",
        "schema",
        "scheduler_view_sha256",
        "sealed_archive_segment_sha256",
    }
    if set(decoded) != expected or decoded.get("schema") != (
        CANDIDATE_REFERENCE_RECORD_SCHEMA
    ):
        raise AcquisitionSourceReclamationProtocolIntegrityError(
            "candidate page contains an inline or unknown archive record"
        )
    for key in (
        "archive_object_reference_root_sha256",
        "capacity_ledger_view_sha256",
        "candidate_sha256",
        "downstream_lifecycle_view_sha256",
        "execution_receipts_view_sha256",
        "metadata_sha256",
        "raw_artifacts_view_sha256",
        "record_sha256",
        "scheduler_view_sha256",
        "sealed_archive_segment_sha256",
    ):
        require_sha256(decoded.get(key), f"candidate reference {key}")
    require_count(
        decoded.get("archive_object_count"),
        "candidate archive object count",
        maximum=MAX_RELEASE_COUNT,
    )
    require_count(
        decoded.get("archive_object_total_bytes"),
        "candidate archive object bytes",
        maximum=MAX_RELEASE_COUNT,
    )
    return decoded


def candidate_participant_view_sha256(
    record: Mapping[str, Any],
    role: str,
) -> str:
    """Return the sealed role-local source projection bound by a reference."""

    selected_role = require_role(role)
    canonical = _decode_candidate_record(canonical_bytes(dict(record)))
    return require_sha256(
        canonical[f"{selected_role}_view_sha256"],
        "candidate participant view SHA-256",
    )


@dataclass(frozen=True, slots=True)
class SourceReclamationCandidateBindingV1:
    binding_sha256: str
    candidate_sha256: str
    acquisition_job_id: str
    scheduler_job_id: str
    raw_capture_id: str
    evidence_stream_id: str
    evidence_stream_sequence: int
    sealed_archive_segment_sha256: str
    sealed_archive_record_sha256: str
    consumer_closure_receipt_sha256: str
    consumer_closure_root_sha256: str

    def __post_init__(self) -> None:
        for value, label in (
            (self.binding_sha256, "candidate binding SHA-256"),
            (self.candidate_sha256, "candidate SHA-256"),
            (self.sealed_archive_segment_sha256, "candidate segment SHA-256"),
            (self.sealed_archive_record_sha256, "candidate record SHA-256"),
            (self.consumer_closure_receipt_sha256, "candidate closure receipt SHA-256"),
            (self.consumer_closure_root_sha256, "candidate closure root SHA-256"),
        ):
            require_sha256(value, label)
        for value, label in (
            (self.acquisition_job_id, "candidate acquisition job ID"),
            (self.scheduler_job_id, "candidate scheduler job ID"),
            (self.raw_capture_id, "candidate raw capture ID"),
            (self.evidence_stream_id, "candidate evidence stream ID"),
        ):
            require_identifier(value, label)
        require_count(
            self.evidence_stream_sequence,
            "candidate evidence stream sequence",
            maximum=2**63 - 1,
        )
        if self.evidence_stream_sequence < 1:
            raise AcquisitionSourceReclamationProtocolIntegrityError(
                "candidate evidence stream sequence is invalid"
            )
        if self.binding_sha256 != digest_mapping(self._core()):
            raise AcquisitionSourceReclamationProtocolIntegrityError(
                "candidate binding checksum differs"
            )

    def _core(self) -> dict[str, Any]:
        return {
            "acquisition_job_id": self.acquisition_job_id,
            "candidate_sha256": self.candidate_sha256,
            "consumer_closure_receipt_sha256": self.consumer_closure_receipt_sha256,
            "consumer_closure_root_sha256": self.consumer_closure_root_sha256,
            "evidence_stream_id": self.evidence_stream_id,
            "evidence_stream_sequence": self.evidence_stream_sequence,
            "raw_capture_id": self.raw_capture_id,
            "scheduler_job_id": self.scheduler_job_id,
            "schema": CANDIDATE_BINDING_SCHEMA,
            "sealed_archive_record_sha256": self.sealed_archive_record_sha256,
            "sealed_archive_segment_sha256": self.sealed_archive_segment_sha256,
        }

    def as_dict(self) -> dict[str, Any]:
        return {**self._core(), "binding_sha256": self.binding_sha256}

    @classmethod
    def create(
        cls,
        *,
        candidate_sha256: str,
        acquisition_job_id: str,
        scheduler_job_id: str,
        raw_capture_id: str,
        evidence_stream_id: str,
        evidence_stream_sequence: int,
        sealed_archive_segment_sha256: str,
        sealed_archive_record_sha256: str,
        consumer_closure_receipt_sha256: str,
        consumer_closure_root_sha256: str,
    ) -> "SourceReclamationCandidateBindingV1":
        core = {
            "acquisition_job_id": acquisition_job_id,
            "candidate_sha256": candidate_sha256,
            "consumer_closure_receipt_sha256": consumer_closure_receipt_sha256,
            "consumer_closure_root_sha256": consumer_closure_root_sha256,
            "evidence_stream_id": evidence_stream_id,
            "evidence_stream_sequence": evidence_stream_sequence,
            "raw_capture_id": raw_capture_id,
            "scheduler_job_id": scheduler_job_id,
            "schema": CANDIDATE_BINDING_SCHEMA,
            "sealed_archive_record_sha256": sealed_archive_record_sha256,
            "sealed_archive_segment_sha256": sealed_archive_segment_sha256,
        }
        return cls(binding_sha256=digest_mapping(core), **{
            key: value for key, value in core.items() if key != "schema"
        })


@dataclass(frozen=True, slots=True, repr=False)
class SourceReclamationCandidatePageV1:
    page_sha256: str
    request_sha256: str
    candidate_authority_sha256: str
    sealed_archive_anchor_sha256: str
    archive_binding_root_sha256: str
    candidate_bindings_sha256: str
    consumer_registry_root_sha256: str
    consumer_epoch_root_sha256: str
    after_cursor: int
    next_cursor: int
    total_candidate_count: int
    bindings: tuple[SourceReclamationCandidateBindingV1, ...]
    record_payloads: tuple[bytes, ...]
    exhausted: bool

    def __post_init__(self) -> None:
        for value, label in (
            (self.page_sha256, "candidate page SHA-256"),
            (self.request_sha256, "candidate page request SHA-256"),
            (self.candidate_authority_sha256, "candidate authority SHA-256"),
            (self.sealed_archive_anchor_sha256, "candidate page archive anchor SHA-256"),
            (self.archive_binding_root_sha256, "candidate page archive root SHA-256"),
            (self.candidate_bindings_sha256, "candidate inventory root SHA-256"),
            (self.consumer_registry_root_sha256, "candidate registry root SHA-256"),
            (self.consumer_epoch_root_sha256, "candidate epoch root SHA-256"),
        ):
            require_sha256(value, label)
        for value, label in (
            (self.after_cursor, "candidate page cursor"),
            (self.next_cursor, "candidate next cursor"),
            (self.total_candidate_count, "candidate total count"),
        ):
            require_count(value, label, maximum=MAX_CANDIDATES)
        if (
            not isinstance(self.bindings, tuple)
            or not isinstance(self.record_payloads, tuple)
            or not isinstance(self.exhausted, bool)
            or not 1 <= len(self.bindings) <= MAX_CANDIDATE_PAGE_RECORDS
            or len(self.bindings) != len(self.record_payloads)
            or self.next_cursor != self.after_cursor + len(self.bindings)
            or self.next_cursor > self.total_candidate_count
            or self.exhausted != (self.next_cursor == self.total_candidate_count)
            or tuple(item.candidate_sha256 for item in self.bindings)
            != tuple(sorted(item.candidate_sha256 for item in self.bindings))
            or len({item.candidate_sha256 for item in self.bindings})
            != len(self.bindings)
        ):
            raise AcquisitionSourceReclamationProtocolIntegrityError(
                "candidate page shape or order differs"
            )
        total_bytes = 0
        record_sha256s: list[str] = []
        for binding, payload in zip(
            self.bindings,
            self.record_payloads,
            strict=True,
        ):
            total_bytes += len(payload)
            if total_bytes > MAX_CANDIDATE_PAGE_BYTES:
                raise AcquisitionSourceReclamationProtocolIntegrityError(
                    "candidate page exceeds its byte bound"
                )
            decoded = _decode_candidate_record(payload)
            if (
                decoded.get("candidate_sha256") != binding.candidate_sha256
                or decoded.get("record_sha256")
                != binding.sealed_archive_record_sha256
                or decoded.get("sealed_archive_segment_sha256")
                != binding.sealed_archive_segment_sha256
            ):
                raise AcquisitionSourceReclamationProtocolIntegrityError(
                    "candidate binding differs from its sealed record"
                )
            record_sha256s.append(hashlib.sha256(payload).hexdigest())
        if self.page_sha256 != digest_mapping(
            self._core(record_payload_sha256s=tuple(record_sha256s))
        ):
            raise AcquisitionSourceReclamationProtocolIntegrityError(
                "candidate page checksum differs"
            )

    def _core(
        self,
        *,
        record_payload_sha256s: tuple[str, ...],
    ) -> dict[str, Any]:
        return {
            "after_cursor": self.after_cursor,
            "archive_binding_root_sha256": self.archive_binding_root_sha256,
            "bindings": [item.as_dict() for item in self.bindings],
            "candidate_authority_sha256": self.candidate_authority_sha256,
            "candidate_bindings_sha256": self.candidate_bindings_sha256,
            "consumer_epoch_root_sha256": self.consumer_epoch_root_sha256,
            "consumer_registry_root_sha256": self.consumer_registry_root_sha256,
            "exhausted": self.exhausted,
            "next_cursor": self.next_cursor,
            "record_payload_sha256s": list(record_payload_sha256s),
            "request_sha256": self.request_sha256,
            "schema": CANDIDATE_PAGE_SCHEMA,
            "sealed_archive_anchor_sha256": self.sealed_archive_anchor_sha256,
            "total_candidate_count": self.total_candidate_count,
        }

    def decoded_records(self) -> tuple[dict[str, Any], ...]:
        return tuple(_decode_candidate_record(item) for item in self.record_payloads)

    @classmethod
    def create(
        cls,
        *,
        request_sha256: str,
        candidate_authority_sha256: str,
        sealed_archive_anchor_sha256: str,
        archive_binding_root_sha256: str,
        candidate_bindings_sha256: str,
        consumer_registry_root_sha256: str,
        consumer_epoch_root_sha256: str,
        after_cursor: int,
        total_candidate_count: int,
        bindings: tuple[SourceReclamationCandidateBindingV1, ...],
        record_payloads: tuple[bytes, ...],
    ) -> "SourceReclamationCandidatePageV1":
        next_cursor = after_cursor + len(bindings)
        exhausted = next_cursor == total_candidate_count
        record_sha256s = tuple(
            hashlib.sha256(item).hexdigest() for item in record_payloads
        )
        core = {
            "after_cursor": after_cursor,
            "archive_binding_root_sha256": archive_binding_root_sha256,
            "bindings": [item.as_dict() for item in bindings],
            "candidate_authority_sha256": candidate_authority_sha256,
            "candidate_bindings_sha256": candidate_bindings_sha256,
            "consumer_epoch_root_sha256": consumer_epoch_root_sha256,
            "consumer_registry_root_sha256": consumer_registry_root_sha256,
            "exhausted": exhausted,
            "next_cursor": next_cursor,
            "record_payload_sha256s": list(record_sha256s),
            "request_sha256": request_sha256,
            "schema": CANDIDATE_PAGE_SCHEMA,
            "sealed_archive_anchor_sha256": sealed_archive_anchor_sha256,
            "total_candidate_count": total_candidate_count,
        }
        return cls(
            page_sha256=digest_mapping(core),
            request_sha256=request_sha256,
            candidate_authority_sha256=candidate_authority_sha256,
            sealed_archive_anchor_sha256=sealed_archive_anchor_sha256,
            archive_binding_root_sha256=archive_binding_root_sha256,
            candidate_bindings_sha256=candidate_bindings_sha256,
            consumer_registry_root_sha256=consumer_registry_root_sha256,
            consumer_epoch_root_sha256=consumer_epoch_root_sha256,
            after_cursor=after_cursor,
            next_cursor=next_cursor,
            total_candidate_count=total_candidate_count,
            bindings=bindings,
            record_payloads=record_payloads,
            exhausted=exhausted,
        )


class SourceReclamationCandidateAuthority(ABC):
    """Constructor-bound, physically held sealed candidate resolver."""

    __slots__ = ("_authority_sha256",)

    def __init__(self, authority_sha256: str) -> None:
        self._authority_sha256 = require_sha256(
            authority_sha256,
            "candidate authority SHA-256",
        )

    @property
    def authority_sha256(self) -> str:
        return self._authority_sha256

    @abstractmethod
    def hold_candidate_page_v1(
        self,
        request: "SourceReclamationRequestV2",
        *,
        after_cursor: int,
        max_candidates: int,
    ) -> AbstractContextManager[SourceReclamationCandidatePageV1]:
        """Hold archive/registry/path witnesses until one local DB commit."""

    @contextmanager
    def hold_current_request_authority_v1(
        self,
        request: "SourceReclamationRequestV2",
    ) -> Iterator[SourceReclamationCandidatePageV1]:
        """Hold the authority that makes one sealed request current.

        Historical candidate authorities remain source compatible: their
        default current-authority proof is the exact first one-record page
        held by ``hold_candidate_page_v1``.  Authorities with a stronger
        notion of currentness (for example, a live consumer-registry epoch)
        override this method while retaining the same yielded page contract.
        """

        if not isinstance(request, SourceReclamationRequestV2):
            raise TypeError("request must be SourceReclamationRequestV2")
        with self.hold_candidate_page_v1(
            request,
            after_cursor=0,
            max_candidates=1,
        ) as page:
            if not isinstance(page, SourceReclamationCandidatePageV1):
                raise AcquisitionSourceReclamationProtocolIntegrityError(
                    "current candidate authority yielded an invalid page"
                )
            if (
                page.request_sha256 != request.request_sha256
                or page.candidate_authority_sha256
                != request.candidate_authority_sha256
                or page.candidate_authority_sha256 != self.authority_sha256
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
                or page.after_cursor != 0
                or page.next_cursor != 1
                or page.total_candidate_count != request.candidate_count
                or len(page.bindings) != 1
                or len(page.record_payloads) != 1
                or page.exhausted != (request.candidate_count == 1)
            ):
                raise AcquisitionSourceReclamationProtocolIntegrityError(
                    "current candidate authority page differs from request"
                )
            yield page


@dataclass(frozen=True, slots=True)
class SourceReclamationRequestV2:
    request_sha256: str
    source_binding_sha256: str
    source_view_sha256: str
    sealed_archive_anchor_sha256: str
    archive_binding_root_sha256: str
    candidate_bindings_sha256: str
    candidate_authority_sha256: str
    consumer_registry_root_sha256: str
    consumer_epoch_root_sha256: str
    candidate_count: int
    created_at: float

    def __post_init__(self) -> None:
        for value, label in (
            (self.request_sha256, "request SHA-256"),
            (self.source_binding_sha256, "source binding SHA-256"),
            (self.source_view_sha256, "source view SHA-256"),
            (self.sealed_archive_anchor_sha256, "sealed archive anchor SHA-256"),
            (self.archive_binding_root_sha256, "archive binding root SHA-256"),
            (self.candidate_bindings_sha256, "candidate bindings SHA-256"),
            (self.candidate_authority_sha256, "candidate authority SHA-256"),
            (self.consumer_registry_root_sha256, "consumer registry root SHA-256"),
            (self.consumer_epoch_root_sha256, "consumer epoch root SHA-256"),
        ):
            require_sha256(value, label)
        require_count(
            self.candidate_count,
            "candidate count",
            maximum=MAX_CANDIDATES,
        )
        if self.candidate_count < 1:
            raise AcquisitionSourceReclamationProtocolIntegrityError(
                "positive reclamation request has no candidates"
            )
        require_timestamp(self.created_at, "request clock")
        if self.request_sha256 != digest_mapping(self._core()):
            raise AcquisitionSourceReclamationProtocolIntegrityError(
                "reclamation request checksum differs"
            )

    def _core(self) -> dict[str, Any]:
        return {
            "archive_binding_root_sha256": self.archive_binding_root_sha256,
            "candidate_bindings_sha256": self.candidate_bindings_sha256,
            "candidate_authority_sha256": self.candidate_authority_sha256,
            "candidate_count": self.candidate_count,
            "consumer_epoch_root_sha256": self.consumer_epoch_root_sha256,
            "consumer_registry_root_sha256": self.consumer_registry_root_sha256,
            "created_at": float(self.created_at),
            "protocol_contract": PROTOCOL_CONTRACT,
            "schema": REQUEST_SCHEMA,
            "sealed_archive_anchor_sha256": self.sealed_archive_anchor_sha256,
            "source_binding_sha256": self.source_binding_sha256,
            "source_view_sha256": self.source_view_sha256,
        }

    def as_dict(self) -> dict[str, Any]:
        return {**self._core(), "request_sha256": self.request_sha256}

    @classmethod
    def create(
        cls,
        *,
        source_binding_sha256: str,
        source_view_sha256: str,
        sealed_archive_anchor_sha256: str,
        archive_binding_root_sha256: str,
        candidate_bindings_sha256: str,
        candidate_authority_sha256: str,
        consumer_registry_root_sha256: str,
        consumer_epoch_root_sha256: str,
        candidate_count: int,
        created_at: float,
    ) -> "SourceReclamationRequestV2":
        core = {
            "archive_binding_root_sha256": archive_binding_root_sha256,
            "candidate_bindings_sha256": candidate_bindings_sha256,
            "candidate_authority_sha256": candidate_authority_sha256,
            "candidate_count": candidate_count,
            "consumer_epoch_root_sha256": consumer_epoch_root_sha256,
            "consumer_registry_root_sha256": consumer_registry_root_sha256,
            "created_at": float(created_at),
            "protocol_contract": PROTOCOL_CONTRACT,
            "schema": REQUEST_SCHEMA,
            "sealed_archive_anchor_sha256": sealed_archive_anchor_sha256,
            "source_binding_sha256": source_binding_sha256,
            "source_view_sha256": source_view_sha256,
        }
        return cls(request_sha256=digest_mapping(core), **{
            key: value for key, value in core.items()
            if key not in {"protocol_contract", "schema"}
        })


@dataclass(frozen=True, slots=True)
class SourceLocalPrepareReceiptV2:
    prepare_receipt_sha256: str
    request_sha256: str
    role: str
    source_store_identity_sha256: str
    coordinator_authority_sha256: str
    candidate_authority_sha256: str
    sealed_archive_anchor_sha256: str
    archive_binding_root_sha256: str
    candidate_bindings_sha256: str
    consumer_registry_root_sha256: str
    consumer_epoch_root_sha256: str
    source_view_sha256: str
    child_reference_root_sha256: str
    source_fence_sha256: str
    candidate_count: int
    releasable_bytes: int
    releasable_entries: int
    prepared_at: float

    def __post_init__(self) -> None:
        for value, label in (
            (self.prepare_receipt_sha256, "local prepare receipt SHA-256"),
            (self.request_sha256, "local prepare request SHA-256"),
            (self.source_store_identity_sha256, "source store identity SHA-256"),
            (self.coordinator_authority_sha256, "coordinator authority SHA-256"),
            (self.candidate_authority_sha256, "candidate authority SHA-256"),
            (self.sealed_archive_anchor_sha256, "sealed archive anchor SHA-256"),
            (self.archive_binding_root_sha256, "archive binding root SHA-256"),
            (self.candidate_bindings_sha256, "candidate bindings SHA-256"),
            (self.consumer_registry_root_sha256, "consumer registry root SHA-256"),
            (self.consumer_epoch_root_sha256, "consumer epoch root SHA-256"),
            (self.source_view_sha256, "local source view SHA-256"),
            (self.child_reference_root_sha256, "child reference root SHA-256"),
            (self.source_fence_sha256, "source fence SHA-256"),
        ):
            require_sha256(value, label)
        require_role(self.role)
        require_count(self.candidate_count, "candidate count", maximum=MAX_CANDIDATES)
        if self.candidate_count < 1:
            raise AcquisitionSourceReclamationProtocolIntegrityError(
                "local prepare has no candidates"
            )
        require_count(
            self.releasable_bytes,
            "releasable bytes",
            maximum=MAX_RELEASE_COUNT,
        )
        require_count(
            self.releasable_entries,
            "releasable entries",
            maximum=MAX_RELEASE_COUNT,
        )
        require_timestamp(self.prepared_at, "local prepare clock")
        if self.prepare_receipt_sha256 != digest_mapping(self._core()):
            raise AcquisitionSourceReclamationProtocolIntegrityError(
                "local prepare receipt checksum differs"
            )

    def _core(self) -> dict[str, Any]:
        return {
            "archive_binding_root_sha256": self.archive_binding_root_sha256,
            "candidate_bindings_sha256": self.candidate_bindings_sha256,
            "candidate_authority_sha256": self.candidate_authority_sha256,
            "candidate_count": self.candidate_count,
            "child_reference_root_sha256": self.child_reference_root_sha256,
            "consumer_epoch_root_sha256": self.consumer_epoch_root_sha256,
            "consumer_registry_root_sha256": self.consumer_registry_root_sha256,
            "coordinator_authority_sha256": self.coordinator_authority_sha256,
            "prepared_at": float(self.prepared_at),
            "protocol_contract": PROTOCOL_CONTRACT,
            "releasable_bytes": self.releasable_bytes,
            "releasable_entries": self.releasable_entries,
            "request_sha256": self.request_sha256,
            "role": self.role,
            "schema": SOURCE_LOCAL_PREPARE_SCHEMA,
            "sealed_archive_anchor_sha256": self.sealed_archive_anchor_sha256,
            "source_fence_sha256": self.source_fence_sha256,
            "source_store_identity_sha256": self.source_store_identity_sha256,
            "source_view_sha256": self.source_view_sha256,
        }

    def as_dict(self) -> dict[str, Any]:
        return {**self._core(), "prepare_receipt_sha256": self.prepare_receipt_sha256}

    @classmethod
    def create(
        cls,
        *,
        request_sha256: str,
        role: str,
        source_store_identity_sha256: str,
        coordinator_authority_sha256: str,
        candidate_authority_sha256: str,
        sealed_archive_anchor_sha256: str,
        archive_binding_root_sha256: str,
        candidate_bindings_sha256: str,
        consumer_registry_root_sha256: str,
        consumer_epoch_root_sha256: str,
        source_view_sha256: str,
        child_reference_root_sha256: str,
        source_fence_sha256: str,
        candidate_count: int,
        releasable_bytes: int,
        releasable_entries: int,
        prepared_at: float,
    ) -> "SourceLocalPrepareReceiptV2":
        values = {
            "request_sha256": request_sha256,
            "role": role,
            "source_store_identity_sha256": source_store_identity_sha256,
            "coordinator_authority_sha256": coordinator_authority_sha256,
            "candidate_authority_sha256": candidate_authority_sha256,
            "sealed_archive_anchor_sha256": sealed_archive_anchor_sha256,
            "archive_binding_root_sha256": archive_binding_root_sha256,
            "candidate_bindings_sha256": candidate_bindings_sha256,
            "consumer_registry_root_sha256": consumer_registry_root_sha256,
            "consumer_epoch_root_sha256": consumer_epoch_root_sha256,
            "source_view_sha256": source_view_sha256,
            "child_reference_root_sha256": child_reference_root_sha256,
            "source_fence_sha256": source_fence_sha256,
            "candidate_count": candidate_count,
            "releasable_bytes": releasable_bytes,
            "releasable_entries": releasable_entries,
            "prepared_at": prepared_at,
        }
        core = {
            "archive_binding_root_sha256": archive_binding_root_sha256,
            "candidate_bindings_sha256": candidate_bindings_sha256,
            "candidate_authority_sha256": candidate_authority_sha256,
            "candidate_count": candidate_count,
            "child_reference_root_sha256": child_reference_root_sha256,
            "consumer_epoch_root_sha256": consumer_epoch_root_sha256,
            "consumer_registry_root_sha256": consumer_registry_root_sha256,
            "coordinator_authority_sha256": coordinator_authority_sha256,
            "prepared_at": float(prepared_at),
            "protocol_contract": PROTOCOL_CONTRACT,
            "releasable_bytes": releasable_bytes,
            "releasable_entries": releasable_entries,
            "request_sha256": request_sha256,
            "role": role,
            "schema": SOURCE_LOCAL_PREPARE_SCHEMA,
            "sealed_archive_anchor_sha256": sealed_archive_anchor_sha256,
            "source_fence_sha256": source_fence_sha256,
            "source_store_identity_sha256": source_store_identity_sha256,
            "source_view_sha256": source_view_sha256,
        }
        return cls(prepare_receipt_sha256=digest_mapping(core), **values)


@dataclass(frozen=True, slots=True)
class ParticipantPrepareBindingV1:
    role: str
    prepare_receipt_sha256: str
    source_store_identity_sha256: str
    source_fence_sha256: str

    def __post_init__(self) -> None:
        require_role(self.role)
        require_sha256(self.prepare_receipt_sha256, "prepare binding receipt SHA-256")
        require_sha256(self.source_store_identity_sha256, "prepare binding store SHA-256")
        require_sha256(self.source_fence_sha256, "prepare binding fence SHA-256")

    def as_dict(self) -> dict[str, str]:
        return {
            "prepare_receipt_sha256": self.prepare_receipt_sha256,
            "role": self.role,
            "schema": PARTICIPANT_PREPARE_BINDING_SCHEMA,
            "source_fence_sha256": self.source_fence_sha256,
            "source_store_identity_sha256": self.source_store_identity_sha256,
        }


@dataclass(frozen=True, slots=True)
class CommittedReclamationDecisionReceiptV1:
    receipt_sha256: str
    decision_sha256: str
    request_sha256: str
    coordinator_store_identity_sha256: str
    sealed_archive_anchor_sha256: str
    archive_binding_root_sha256: str
    candidate_bindings_sha256: str
    candidate_authority_sha256: str
    consumer_registry_root_sha256: str
    consumer_epoch_root_sha256: str
    participant_prepare_root_sha256: str
    participant_prepares: tuple[ParticipantPrepareBindingV1, ...]
    candidate_count: int
    owner_epoch: int
    committed_at: float
    commit_event_sequence: int
    previous_event_sha256: str
    commit_event_sha256: str
    state: str = COMMITTED
    source_release_performed: bool = False

    def __post_init__(self) -> None:
        for value, label in (
            (self.receipt_sha256, "committed receipt SHA-256"),
            (self.decision_sha256, "committed decision SHA-256"),
            (self.request_sha256, "committed request SHA-256"),
            (self.coordinator_store_identity_sha256, "coordinator store identity SHA-256"),
            (self.sealed_archive_anchor_sha256, "sealed archive anchor SHA-256"),
            (self.archive_binding_root_sha256, "archive binding root SHA-256"),
            (self.candidate_bindings_sha256, "candidate bindings SHA-256"),
            (self.candidate_authority_sha256, "candidate authority SHA-256"),
            (self.consumer_registry_root_sha256, "consumer registry root SHA-256"),
            (self.consumer_epoch_root_sha256, "consumer epoch root SHA-256"),
            (self.participant_prepare_root_sha256, "participant prepare root SHA-256"),
            (self.previous_event_sha256, "previous coordinator event SHA-256"),
            (self.commit_event_sha256, "commit event SHA-256"),
        ):
            require_sha256(value, label)
        require_count(self.candidate_count, "candidate count", maximum=MAX_CANDIDATES)
        if self.candidate_count < 1:
            raise AcquisitionSourceReclamationProtocolIntegrityError(
                "committed decision has no candidates"
            )
        require_count(self.owner_epoch, "owner epoch", maximum=2**63 - 1)
        require_count(
            self.commit_event_sequence,
            "commit event sequence",
            maximum=2**63 - 1,
        )
        if self.owner_epoch < 1 or self.commit_event_sequence < 1:
            raise AcquisitionSourceReclamationProtocolIntegrityError(
                "committed decision sequence authority is invalid"
            )
        require_timestamp(self.committed_at, "committed decision clock")
        if (
            not isinstance(self.participant_prepares, tuple)
            or tuple(item.role for item in self.participant_prepares)
            != PARTICIPANT_APPLY_ORDER
            or any(
                not isinstance(item, ParticipantPrepareBindingV1)
                for item in self.participant_prepares
            )
        ):
            raise AcquisitionSourceReclamationProtocolIntegrityError(
                "committed participant prepare inventory differs"
            )
        expected_root = digest_mapping(
            [item.as_dict() for item in self.participant_prepares]
        )
        if self.participant_prepare_root_sha256 != expected_root:
            raise AcquisitionSourceReclamationProtocolIntegrityError(
                "participant prepare root checksum differs"
            )
        if self.state != COMMITTED or self.source_release_performed is not False:
            raise AcquisitionSourceReclamationProtocolIntegrityError(
                "committed decision makes a premature release claim"
            )
        if self.decision_sha256 != digest_mapping(self._decision_core()):
            raise AcquisitionSourceReclamationProtocolIntegrityError(
                "committed decision checksum differs"
            )
        if self.receipt_sha256 != digest_mapping(self._receipt_core()):
            raise AcquisitionSourceReclamationProtocolIntegrityError(
                "committed decision receipt checksum differs"
            )

    def _decision_core(self) -> dict[str, Any]:
        return {
            "apply_order": list(PARTICIPANT_APPLY_ORDER),
            "archive_binding_root_sha256": self.archive_binding_root_sha256,
            "candidate_bindings_sha256": self.candidate_bindings_sha256,
            "candidate_authority_sha256": self.candidate_authority_sha256,
            "candidate_count": self.candidate_count,
            "consumer_epoch_root_sha256": self.consumer_epoch_root_sha256,
            "consumer_registry_root_sha256": self.consumer_registry_root_sha256,
            "coordinator_store_identity_sha256": self.coordinator_store_identity_sha256,
            "participant_prepare_root_sha256": self.participant_prepare_root_sha256,
            "participant_prepares": [item.as_dict() for item in self.participant_prepares],
            "protocol_contract": PROTOCOL_CONTRACT,
            "request_sha256": self.request_sha256,
            "schema": COMMITTED_DECISION_SCHEMA,
            "sealed_archive_anchor_sha256": self.sealed_archive_anchor_sha256,
            "source_release_performed": False,
            "state": COMMITTED,
        }

    def _receipt_core(self) -> dict[str, Any]:
        return {
            **self._decision_core(),
            "commit_event_sequence": self.commit_event_sequence,
            "commit_event_sha256": self.commit_event_sha256,
            "committed_at": float(self.committed_at),
            "decision_sha256": self.decision_sha256,
            "owner_epoch": self.owner_epoch,
            "previous_event_sha256": self.previous_event_sha256,
            "schema": COMMITTED_DECISION_RECEIPT_SCHEMA,
        }

    def as_dict(self) -> dict[str, Any]:
        return {**self._receipt_core(), "receipt_sha256": self.receipt_sha256}

    @classmethod
    def create(
        cls,
        *,
        request_sha256: str,
        coordinator_store_identity_sha256: str,
        sealed_archive_anchor_sha256: str,
        archive_binding_root_sha256: str,
        candidate_bindings_sha256: str,
        candidate_authority_sha256: str,
        consumer_registry_root_sha256: str,
        consumer_epoch_root_sha256: str,
        participant_prepares: tuple[ParticipantPrepareBindingV1, ...],
        candidate_count: int,
        owner_epoch: int,
        committed_at: float,
        commit_event_sequence: int,
        previous_event_sha256: str,
        commit_event_sha256: str,
    ) -> "CommittedReclamationDecisionReceiptV1":
        participant_root = digest_mapping(
            [item.as_dict() for item in participant_prepares]
        )
        decision_core = {
            "apply_order": list(PARTICIPANT_APPLY_ORDER),
            "archive_binding_root_sha256": archive_binding_root_sha256,
            "candidate_bindings_sha256": candidate_bindings_sha256,
            "candidate_authority_sha256": candidate_authority_sha256,
            "candidate_count": candidate_count,
            "consumer_epoch_root_sha256": consumer_epoch_root_sha256,
            "consumer_registry_root_sha256": consumer_registry_root_sha256,
            "coordinator_store_identity_sha256": coordinator_store_identity_sha256,
            "participant_prepare_root_sha256": participant_root,
            "participant_prepares": [item.as_dict() for item in participant_prepares],
            "protocol_contract": PROTOCOL_CONTRACT,
            "request_sha256": request_sha256,
            "schema": COMMITTED_DECISION_SCHEMA,
            "sealed_archive_anchor_sha256": sealed_archive_anchor_sha256,
            "source_release_performed": False,
            "state": COMMITTED,
        }
        decision_sha256 = digest_mapping(decision_core)
        receipt_core = {
            **decision_core,
            "commit_event_sequence": commit_event_sequence,
            "commit_event_sha256": commit_event_sha256,
            "committed_at": float(committed_at),
            "decision_sha256": decision_sha256,
            "owner_epoch": owner_epoch,
            "previous_event_sha256": previous_event_sha256,
            "schema": COMMITTED_DECISION_RECEIPT_SCHEMA,
        }
        return cls(
            receipt_sha256=digest_mapping(receipt_core),
            decision_sha256=decision_sha256,
            request_sha256=request_sha256,
            coordinator_store_identity_sha256=coordinator_store_identity_sha256,
            sealed_archive_anchor_sha256=sealed_archive_anchor_sha256,
            archive_binding_root_sha256=archive_binding_root_sha256,
            candidate_bindings_sha256=candidate_bindings_sha256,
            candidate_authority_sha256=candidate_authority_sha256,
            consumer_registry_root_sha256=consumer_registry_root_sha256,
            consumer_epoch_root_sha256=consumer_epoch_root_sha256,
            participant_prepare_root_sha256=participant_root,
            participant_prepares=participant_prepares,
            candidate_count=candidate_count,
            owner_epoch=owner_epoch,
            committed_at=committed_at,
            commit_event_sequence=commit_event_sequence,
            previous_event_sha256=previous_event_sha256,
            commit_event_sha256=commit_event_sha256,
        )


@dataclass(frozen=True, slots=True)
class CoordinatorAbortDecisionReceiptV1:
    receipt_sha256: str
    abort_decision_sha256: str
    request_sha256: str
    coordinator_store_identity_sha256: str
    participant_prepare_root_sha256: str
    owner_epoch: int
    aborting_at: float
    abort_event_sequence: int
    previous_event_sha256: str
    abort_event_sha256: str
    state: str = ABORTING

    def __post_init__(self) -> None:
        for value, label in (
            (self.receipt_sha256, "abort receipt SHA-256"),
            (self.abort_decision_sha256, "abort decision SHA-256"),
            (self.request_sha256, "abort request SHA-256"),
            (self.coordinator_store_identity_sha256, "abort coordinator identity SHA-256"),
            (self.participant_prepare_root_sha256, "abort participant root SHA-256"),
            (self.previous_event_sha256, "abort previous event SHA-256"),
            (self.abort_event_sha256, "abort event SHA-256"),
        ):
            require_sha256(value, label)
        require_count(self.owner_epoch, "abort owner epoch", maximum=2**63 - 1)
        require_count(
            self.abort_event_sequence,
            "abort event sequence",
            maximum=2**63 - 1,
        )
        if self.owner_epoch < 1 or self.abort_event_sequence < 1:
            raise AcquisitionSourceReclamationProtocolIntegrityError(
                "abort sequence authority is invalid"
            )
        require_timestamp(self.aborting_at, "abort decision clock")
        if self.state != ABORTING:
            raise AcquisitionSourceReclamationProtocolIntegrityError(
                "abort decision state differs"
            )
        if self.abort_decision_sha256 != digest_mapping(self._decision_core()):
            raise AcquisitionSourceReclamationProtocolIntegrityError(
                "abort decision checksum differs"
            )
        if self.receipt_sha256 != digest_mapping(self._receipt_core()):
            raise AcquisitionSourceReclamationProtocolIntegrityError(
                "abort decision receipt checksum differs"
            )

    def _decision_core(self) -> dict[str, Any]:
        return {
            "coordinator_store_identity_sha256": self.coordinator_store_identity_sha256,
            "participant_prepare_root_sha256": self.participant_prepare_root_sha256,
            "protocol_contract": PROTOCOL_CONTRACT,
            "request_sha256": self.request_sha256,
            "schema": COORDINATOR_ABORT_DECISION_SCHEMA,
            "state": ABORTING,
        }

    def _receipt_core(self) -> dict[str, Any]:
        return {
            **self._decision_core(),
            "abort_decision_sha256": self.abort_decision_sha256,
            "abort_event_sequence": self.abort_event_sequence,
            "abort_event_sha256": self.abort_event_sha256,
            "aborting_at": float(self.aborting_at),
            "owner_epoch": self.owner_epoch,
            "previous_event_sha256": self.previous_event_sha256,
        }

    def as_dict(self) -> dict[str, Any]:
        return {**self._receipt_core(), "receipt_sha256": self.receipt_sha256}

    @classmethod
    def create(
        cls,
        *,
        request_sha256: str,
        coordinator_store_identity_sha256: str,
        participant_prepare_root_sha256: str,
        owner_epoch: int,
        aborting_at: float,
        abort_event_sequence: int,
        previous_event_sha256: str,
        abort_event_sha256: str,
    ) -> "CoordinatorAbortDecisionReceiptV1":
        decision_core = {
            "coordinator_store_identity_sha256": coordinator_store_identity_sha256,
            "participant_prepare_root_sha256": participant_prepare_root_sha256,
            "protocol_contract": PROTOCOL_CONTRACT,
            "request_sha256": request_sha256,
            "schema": COORDINATOR_ABORT_DECISION_SCHEMA,
            "state": ABORTING,
        }
        decision_sha256 = digest_mapping(decision_core)
        receipt_core = {
            **decision_core,
            "abort_decision_sha256": decision_sha256,
            "abort_event_sequence": abort_event_sequence,
            "abort_event_sha256": abort_event_sha256,
            "aborting_at": float(aborting_at),
            "owner_epoch": owner_epoch,
            "previous_event_sha256": previous_event_sha256,
        }
        return cls(
            receipt_sha256=digest_mapping(receipt_core),
            abort_decision_sha256=decision_sha256,
            request_sha256=request_sha256,
            coordinator_store_identity_sha256=coordinator_store_identity_sha256,
            participant_prepare_root_sha256=participant_prepare_root_sha256,
            owner_epoch=owner_epoch,
            aborting_at=aborting_at,
            abort_event_sequence=abort_event_sequence,
            previous_event_sha256=previous_event_sha256,
            abort_event_sha256=abort_event_sha256,
        )


@dataclass(frozen=True, slots=True)
class SourceLocalApplyReceiptV1:
    apply_receipt_sha256: str
    request_sha256: str
    role: str
    source_store_identity_sha256: str
    prepare_receipt_sha256: str
    committed_decision_sha256: str
    committed_decision_receipt_sha256: str
    predecessor_apply_receipt_sha256: str | None
    effect_sha256: str
    post_apply_view_sha256: str
    released_bytes: int
    released_entries: int
    applied_at: float
    state: str = APPLIED

    def __post_init__(self) -> None:
        for value, label in (
            (self.apply_receipt_sha256, "local apply receipt SHA-256"),
            (self.request_sha256, "local apply request SHA-256"),
            (self.source_store_identity_sha256, "local apply store identity SHA-256"),
            (self.prepare_receipt_sha256, "local apply prepare receipt SHA-256"),
            (self.committed_decision_sha256, "local apply committed decision SHA-256"),
            (self.committed_decision_receipt_sha256, "local apply committed receipt SHA-256"),
            (self.effect_sha256, "local apply effect SHA-256"),
            (self.post_apply_view_sha256, "local post-apply view SHA-256"),
        ):
            require_sha256(value, label)
        role = require_role(self.role)
        role_index = PARTICIPANT_APPLY_ORDER.index(role)
        if role_index == 0:
            if self.predecessor_apply_receipt_sha256 is not None:
                raise AcquisitionSourceReclamationProtocolIntegrityError(
                    "first participant unexpectedly binds a predecessor"
                )
        else:
            require_sha256(
                self.predecessor_apply_receipt_sha256,
                "predecessor apply receipt SHA-256",
            )
        require_count(self.released_bytes, "released bytes", maximum=MAX_RELEASE_COUNT)
        require_count(self.released_entries, "released entries", maximum=MAX_RELEASE_COUNT)
        require_timestamp(self.applied_at, "local apply clock")
        if self.state != APPLIED:
            raise AcquisitionSourceReclamationProtocolIntegrityError(
                "local apply state differs"
            )
        if self.apply_receipt_sha256 != digest_mapping(self._core()):
            raise AcquisitionSourceReclamationProtocolIntegrityError(
                "local apply receipt checksum differs"
            )

    def _core(self) -> dict[str, Any]:
        return {
            "applied_at": float(self.applied_at),
            "committed_decision_receipt_sha256": self.committed_decision_receipt_sha256,
            "committed_decision_sha256": self.committed_decision_sha256,
            "effect_sha256": self.effect_sha256,
            "post_apply_view_sha256": self.post_apply_view_sha256,
            "predecessor_apply_receipt_sha256": self.predecessor_apply_receipt_sha256,
            "prepare_receipt_sha256": self.prepare_receipt_sha256,
            "protocol_contract": PROTOCOL_CONTRACT,
            "released_bytes": self.released_bytes,
            "released_entries": self.released_entries,
            "request_sha256": self.request_sha256,
            "role": self.role,
            "schema": SOURCE_LOCAL_APPLY_SCHEMA,
            "source_store_identity_sha256": self.source_store_identity_sha256,
            "state": APPLIED,
        }

    def as_dict(self) -> dict[str, Any]:
        return {**self._core(), "apply_receipt_sha256": self.apply_receipt_sha256}

    @classmethod
    def create(
        cls,
        *,
        request_sha256: str,
        role: str,
        source_store_identity_sha256: str,
        prepare_receipt_sha256: str,
        committed_decision_sha256: str,
        committed_decision_receipt_sha256: str,
        predecessor_apply_receipt_sha256: str | None,
        effect_sha256: str,
        post_apply_view_sha256: str,
        released_bytes: int,
        released_entries: int,
        applied_at: float,
    ) -> "SourceLocalApplyReceiptV1":
        values = {
            "request_sha256": request_sha256,
            "role": role,
            "source_store_identity_sha256": source_store_identity_sha256,
            "prepare_receipt_sha256": prepare_receipt_sha256,
            "committed_decision_sha256": committed_decision_sha256,
            "committed_decision_receipt_sha256": committed_decision_receipt_sha256,
            "predecessor_apply_receipt_sha256": predecessor_apply_receipt_sha256,
            "effect_sha256": effect_sha256,
            "post_apply_view_sha256": post_apply_view_sha256,
            "released_bytes": released_bytes,
            "released_entries": released_entries,
            "applied_at": applied_at,
        }
        core = {
            "applied_at": float(applied_at),
            "committed_decision_receipt_sha256": committed_decision_receipt_sha256,
            "committed_decision_sha256": committed_decision_sha256,
            "effect_sha256": effect_sha256,
            "post_apply_view_sha256": post_apply_view_sha256,
            "predecessor_apply_receipt_sha256": predecessor_apply_receipt_sha256,
            "prepare_receipt_sha256": prepare_receipt_sha256,
            "protocol_contract": PROTOCOL_CONTRACT,
            "released_bytes": released_bytes,
            "released_entries": released_entries,
            "request_sha256": request_sha256,
            "role": role,
            "schema": SOURCE_LOCAL_APPLY_SCHEMA,
            "source_store_identity_sha256": source_store_identity_sha256,
            "state": APPLIED,
        }
        return cls(apply_receipt_sha256=digest_mapping(core), **values)


@dataclass(frozen=True, slots=True)
class SourceLocalAbortReceiptV1:
    abort_receipt_sha256: str
    request_sha256: str
    role: str
    source_store_identity_sha256: str
    prepare_receipt_sha256: str
    coordinator_abort_decision_sha256: str
    fence_release_root_sha256: str
    aborted_at: float
    state: str = ABORTED

    def __post_init__(self) -> None:
        for value, label in (
            (self.abort_receipt_sha256, "local abort receipt SHA-256"),
            (self.request_sha256, "local abort request SHA-256"),
            (self.source_store_identity_sha256, "local abort store identity SHA-256"),
            (self.prepare_receipt_sha256, "local abort prepare receipt SHA-256"),
            (self.coordinator_abort_decision_sha256, "coordinator abort decision SHA-256"),
            (self.fence_release_root_sha256, "fence release root SHA-256"),
        ):
            require_sha256(value, label)
        require_role(self.role)
        require_timestamp(self.aborted_at, "local abort clock")
        if self.state != ABORTED:
            raise AcquisitionSourceReclamationProtocolIntegrityError(
                "local abort state differs"
            )
        if self.abort_receipt_sha256 != digest_mapping(self._core()):
            raise AcquisitionSourceReclamationProtocolIntegrityError(
                "local abort receipt checksum differs"
            )

    def _core(self) -> dict[str, Any]:
        return {
            "aborted_at": float(self.aborted_at),
            "coordinator_abort_decision_sha256": self.coordinator_abort_decision_sha256,
            "fence_release_root_sha256": self.fence_release_root_sha256,
            "prepare_receipt_sha256": self.prepare_receipt_sha256,
            "protocol_contract": PROTOCOL_CONTRACT,
            "request_sha256": self.request_sha256,
            "role": self.role,
            "schema": SOURCE_LOCAL_ABORT_SCHEMA,
            "source_store_identity_sha256": self.source_store_identity_sha256,
            "state": ABORTED,
        }

    def as_dict(self) -> dict[str, Any]:
        return {**self._core(), "abort_receipt_sha256": self.abort_receipt_sha256}

    @classmethod
    def create(
        cls,
        *,
        request_sha256: str,
        role: str,
        source_store_identity_sha256: str,
        prepare_receipt_sha256: str,
        coordinator_abort_decision_sha256: str,
        fence_release_root_sha256: str,
        aborted_at: float,
    ) -> "SourceLocalAbortReceiptV1":
        values = {
            "request_sha256": request_sha256,
            "role": role,
            "source_store_identity_sha256": source_store_identity_sha256,
            "prepare_receipt_sha256": prepare_receipt_sha256,
            "coordinator_abort_decision_sha256": coordinator_abort_decision_sha256,
            "fence_release_root_sha256": fence_release_root_sha256,
            "aborted_at": aborted_at,
        }
        core = {
            "aborted_at": float(aborted_at),
            "coordinator_abort_decision_sha256": coordinator_abort_decision_sha256,
            "fence_release_root_sha256": fence_release_root_sha256,
            "prepare_receipt_sha256": prepare_receipt_sha256,
            "protocol_contract": PROTOCOL_CONTRACT,
            "request_sha256": request_sha256,
            "role": role,
            "schema": SOURCE_LOCAL_ABORT_SCHEMA,
            "source_store_identity_sha256": source_store_identity_sha256,
            "state": ABORTED,
        }
        return cls(abort_receipt_sha256=digest_mapping(core), **values)


@dataclass(frozen=True, slots=True)
class RawUnlinkJournalReceiptV1:
    journal_receipt_sha256: str
    request_sha256: str
    raw_store_identity_sha256: str
    prepare_receipt_sha256: str
    committed_decision_receipt_sha256: str
    catalog_apply_sha256: str
    shared_reference_root_sha256: str
    unlink_item_root_sha256: str
    directory_barrier_root_sha256: str
    unlink_item_count: int
    unlinked_bytes: int
    completed_at: float
    state: str = APPLIED

    def __post_init__(self) -> None:
        for value, label in (
            (self.journal_receipt_sha256, "raw unlink journal receipt SHA-256"),
            (self.request_sha256, "raw unlink request SHA-256"),
            (self.raw_store_identity_sha256, "raw store identity SHA-256"),
            (self.prepare_receipt_sha256, "raw prepare receipt SHA-256"),
            (self.committed_decision_receipt_sha256, "raw committed receipt SHA-256"),
            (self.catalog_apply_sha256, "raw catalog apply SHA-256"),
            (self.shared_reference_root_sha256, "raw shared reference root SHA-256"),
            (self.unlink_item_root_sha256, "raw unlink item root SHA-256"),
            (self.directory_barrier_root_sha256, "raw directory barrier root SHA-256"),
        ):
            require_sha256(value, label)
        require_count(
            self.unlink_item_count,
            "raw unlink item count",
            maximum=MAX_RELEASE_COUNT,
        )
        require_count(
            self.unlinked_bytes,
            "raw unlinked bytes",
            maximum=MAX_RELEASE_COUNT,
        )
        require_timestamp(self.completed_at, "raw unlink completion clock")
        if self.state != APPLIED:
            raise AcquisitionSourceReclamationProtocolIntegrityError(
                "raw unlink journal is not fully applied"
            )
        if self.journal_receipt_sha256 != digest_mapping(self._core()):
            raise AcquisitionSourceReclamationProtocolIntegrityError(
                "raw unlink journal receipt checksum differs"
            )

    def _core(self) -> dict[str, Any]:
        return {
            "catalog_apply_sha256": self.catalog_apply_sha256,
            "committed_decision_receipt_sha256": self.committed_decision_receipt_sha256,
            "completed_at": float(self.completed_at),
            "directory_barrier_root_sha256": self.directory_barrier_root_sha256,
            "prepare_receipt_sha256": self.prepare_receipt_sha256,
            "protocol_contract": PROTOCOL_CONTRACT,
            "raw_store_identity_sha256": self.raw_store_identity_sha256,
            "request_sha256": self.request_sha256,
            "schema": RAW_UNLINK_JOURNAL_SCHEMA,
            "shared_reference_root_sha256": self.shared_reference_root_sha256,
            "state": APPLIED,
            "unlink_item_count": self.unlink_item_count,
            "unlink_item_root_sha256": self.unlink_item_root_sha256,
            "unlinked_bytes": self.unlinked_bytes,
        }

    def as_dict(self) -> dict[str, Any]:
        return {**self._core(), "journal_receipt_sha256": self.journal_receipt_sha256}

    @classmethod
    def create(
        cls,
        *,
        request_sha256: str,
        raw_store_identity_sha256: str,
        prepare_receipt_sha256: str,
        committed_decision_receipt_sha256: str,
        catalog_apply_sha256: str,
        shared_reference_root_sha256: str,
        unlink_item_root_sha256: str,
        directory_barrier_root_sha256: str,
        unlink_item_count: int,
        unlinked_bytes: int,
        completed_at: float,
    ) -> "RawUnlinkJournalReceiptV1":
        values = {
            "request_sha256": request_sha256,
            "raw_store_identity_sha256": raw_store_identity_sha256,
            "prepare_receipt_sha256": prepare_receipt_sha256,
            "committed_decision_receipt_sha256": committed_decision_receipt_sha256,
            "catalog_apply_sha256": catalog_apply_sha256,
            "shared_reference_root_sha256": shared_reference_root_sha256,
            "unlink_item_root_sha256": unlink_item_root_sha256,
            "directory_barrier_root_sha256": directory_barrier_root_sha256,
            "unlink_item_count": unlink_item_count,
            "unlinked_bytes": unlinked_bytes,
            "completed_at": completed_at,
        }
        core = {
            "catalog_apply_sha256": catalog_apply_sha256,
            "committed_decision_receipt_sha256": committed_decision_receipt_sha256,
            "completed_at": float(completed_at),
            "directory_barrier_root_sha256": directory_barrier_root_sha256,
            "prepare_receipt_sha256": prepare_receipt_sha256,
            "protocol_contract": PROTOCOL_CONTRACT,
            "raw_store_identity_sha256": raw_store_identity_sha256,
            "request_sha256": request_sha256,
            "schema": RAW_UNLINK_JOURNAL_SCHEMA,
            "shared_reference_root_sha256": shared_reference_root_sha256,
            "state": APPLIED,
            "unlink_item_count": unlink_item_count,
            "unlink_item_root_sha256": unlink_item_root_sha256,
            "unlinked_bytes": unlinked_bytes,
        }
        return cls(journal_receipt_sha256=digest_mapping(core), **values)


class HeldCommittedDecisionAuthority(ABC):
    """Constructor-bound physical verifier held across one source apply.

    Source adapters receive one authority instance at construction.  No apply
    API accepts a caller-supplied verifier callback or caller-built decision.
    """

    __slots__ = ("_authority_sha256",)

    def __init__(self, authority_sha256: str) -> None:
        self._authority_sha256 = require_sha256(
            authority_sha256,
            "held committed-decision authority SHA-256",
        )

    @property
    def authority_sha256(self) -> str:
        return self._authority_sha256

    @abstractmethod
    def hold_committed_decision_v1(
        self,
        request_sha256: str,
        *,
        applying_role: str,
        expected_predecessor_apply_receipt_sha256: str | None,
    ) -> AbstractContextManager[CommittedReclamationDecisionReceiptV1]:
        """Hold COMMITTED plus the exact predecessor progress until commit."""

    @abstractmethod
    def hold_abort_decision_v1(
        self,
        request_sha256: str,
        *,
        expected_abort_decision_sha256: str,
        aborting_role: str,
        expected_prepare_receipt_sha256: str,
        expected_source_store_identity_sha256: str,
        expected_source_fence_sha256: str,
    ) -> AbstractContextManager[CoordinatorAbortDecisionReceiptV1]:
        """Hold ABORTING plus the exact local prepare until fence release."""


@runtime_checkable
class SourceReclamationParticipantV2(Protocol):
    """Closed adapter surface used by the inert v2 coordinator."""

    @property
    def role(self) -> str: ...

    @property
    def source_store_identity_sha256(self) -> str: ...

    @property
    def coordinator_authority_sha256(self) -> str: ...

    @property
    def candidate_authority_sha256(self) -> str: ...

    def prepare_source_local_v2(
        self,
        request: SourceReclamationRequestV2,
    ) -> SourceLocalPrepareReceiptV2: ...

    def load_source_local_prepare_v2(
        self,
        request_sha256: str,
    ) -> SourceLocalPrepareReceiptV2 | None: ...

    def apply_source_local_v1(
        self,
        request_sha256: str,
        *,
        predecessor_apply_receipt_sha256: str | None,
    ) -> SourceLocalApplyReceiptV1: ...

    def load_source_local_apply_v1(
        self,
        request_sha256: str,
    ) -> SourceLocalApplyReceiptV1 | None: ...

    def abort_source_local_v1(
        self,
        request_sha256: str,
        *,
        expected_abort_decision_sha256: str,
    ) -> SourceLocalAbortReceiptV1: ...

    def load_source_local_abort_v1(
        self,
        request_sha256: str,
    ) -> SourceLocalAbortReceiptV1 | None: ...


__all__ = [
    "ABORTED",
    "ABORTING",
    "APPLIED",
    "COMMITTED",
    "COORDINATOR_STATES",
    "CommittedReclamationDecisionReceiptV1",
    "CoordinatorAbortDecisionReceiptV1",
    "HeldCommittedDecisionAuthority",
    "PARTICIPANT_APPLY_ORDER",
    "PARTICIPANT_ROLES",
    "PREPARED",
    "PREPARING",
    "ParticipantPrepareBindingV1",
    "PROTOCOL_CONTRACT",
    "RawUnlinkJournalReceiptV1",
    "SourceLocalAbortReceiptV1",
    "SourceLocalApplyReceiptV1",
    "SourceLocalPrepareReceiptV2",
    "SourceReclamationCandidateAuthority",
    "SourceReclamationCandidateBindingV1",
    "SourceReclamationCandidatePageV1",
    "SourceReclamationParticipantV2",
    "SourceReclamationRequestV2",
    "AcquisitionSourceReclamationProtocolConflict",
    "AcquisitionSourceReclamationProtocolError",
    "AcquisitionSourceReclamationProtocolIntegrityError",
    "archive_binding_root",
    "candidate_binding_root",
    "candidate_participant_view_sha256",
    "canonical_bytes",
    "digest_mapping",
    "require_count",
    "require_identifier",
    "require_role",
    "require_sha256",
    "require_timestamp",
]
