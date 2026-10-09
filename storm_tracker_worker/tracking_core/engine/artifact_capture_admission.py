"""Shadow-only admission between raw contracts, retention and exact storage.

The deterministic admission function never blocks weather-data processing.
It only decides whether caller-supplied bytes may be durably written as exact
upstream evidence.  No provider transport imports this module.
"""
from __future__ import annotations

from dataclasses import dataclass, field, fields, is_dataclass
from enum import Enum
import hashlib
import json
import math
import re
from typing import Any, Iterable

from .artifact_retention_planner import (
    ArtifactEvidence,
    ArtifactKey,
    CapacitySnapshot,
    RetentionDecision,
    RetentionPlan,
    RetentionPolicy,
    plan_artifact_retention,
)
from .provider_artifact_contracts import (
    CONTRACTS,
    RawCaptureStatus,
    contract_registry_sha256,
    derive_record_id,
    validate_redacted,
)


CAPTURE_ADMISSION_SCHEMA_VERSION = "artifact-capture-admission-v1"
SHADOW_ONLY = True
_SHA256 = re.compile(r"[0-9a-f]{64}")
_MEDIA_TYPE = re.compile(r"[a-z0-9][a-z0-9.+-]*/[a-z0-9][a-z0-9.+-]*")


class CaptureAvailability(str, Enum):
    """What the caller actually has at the capture boundary."""

    UPSTREAM_EXACT_SINGLE = "UPSTREAM_EXACT_SINGLE"
    UPSTREAM_EXACT_MULTI = "UPSTREAM_EXACT_MULTI"
    DECODED_BODY_ONLY = "DECODED_BODY_ONLY"
    WORKER_DERIVED_ONLY = "WORKER_DERIVED_ONLY"
    UNKNOWN = "UNKNOWN"


class CaptureOrder(str, Enum):
    """Whether the supplied bytes existed before parsing or transformation."""

    BEFORE_PARSE = "BEFORE_PARSE"
    AFTER_PARSE = "AFTER_PARSE"
    UNKNOWN = "UNKNOWN"


class RawWriteDisposition(str, Enum):
    """Durable-raw outcome; independent from observation processing."""

    STORE_AUTHORIZED = "STORE_AUTHORIZED"
    HOLD = "HOLD"
    ARCHIVE_REQUIRED = "ARCHIVE_REQUIRED"
    REJECT_CAPACITY = "REJECT_CAPACITY"
    RAW_CAPTURE_BLOCKED = "RAW_CAPTURE_BLOCKED"
    IDEMPOTENT_ALREADY_PRESENT = "IDEMPOTENT_ALREADY_PRESENT"
    REVISION_CONFLICT = "REVISION_CONFLICT"


class CaptureExecutionError(RuntimeError):
    """An authorized write could not safely be executed."""


class AtomicArtifactSetUnsupported(CaptureExecutionError):
    """The supplied store has no atomic multi-artifact-set capability."""


def _identifier(value: str, name: str, maximum: int = 512) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
    normalized = value.strip()
    if len(normalized) > maximum or "\0" in normalized:
        raise ValueError(f"{name} is invalid")
    validate_redacted(normalized, record_identity=True)
    return normalized


def _time(value: float | None, name: str) -> float | None:
    if value is None:
        return None
    number = float(value)
    if not math.isfinite(number) or number < 0:
        raise ValueError(f"{name} must be a finite non-negative timestamp")
    return number


@dataclass(frozen=True, slots=True)
class CapturedMember:
    """Caller-supplied bytes and their secret-free artifact-set identity."""

    member_id: str
    role: str
    media_type: str
    payload: bytes = field(repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "member_id",
            _identifier(self.member_id, "member_id"),
        )
        object.__setattr__(self, "role", _identifier(self.role, "role", 100))
        if (
            not isinstance(self.media_type, str)
            or not _MEDIA_TYPE.fullmatch(self.media_type)
        ):
            raise ValueError("media_type must be canonical and parameter-free")
        if not isinstance(self.payload, bytes):
            raise TypeError("payload must be exact bytes")
        if not self.payload:
            raise ValueError("payload must not be empty")


@dataclass(frozen=True, slots=True)
class CaptureRequest:
    """One source revision observed at a declared capture boundary."""

    provider: str
    receipt_id: str
    revision_id: str
    received_at: float
    nominal_at: float | None
    availability: CaptureAvailability
    capture_order: CaptureOrder
    expected_member_ids: tuple[str, ...]
    members: tuple[CapturedMember, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.provider, str) or not re.fullmatch(
            r"[a-z0-9_]+",
            self.provider,
        ):
            raise ValueError("provider must be a lowercase registry identifier")
        key = ArtifactKey(self.provider, self.receipt_id, self.revision_id)
        object.__setattr__(self, "receipt_id", key.receipt_id)
        object.__setattr__(self, "revision_id", key.revision_id)
        object.__setattr__(
            self,
            "received_at",
            _time(self.received_at, "received_at"),
        )
        object.__setattr__(
            self,
            "nominal_at",
            _time(self.nominal_at, "nominal_at"),
        )
        if not isinstance(self.availability, CaptureAvailability):
            raise TypeError("availability must be CaptureAvailability")
        if not isinstance(self.capture_order, CaptureOrder):
            raise TypeError("capture_order must be CaptureOrder")
        if not isinstance(self.members, tuple) or not self.members:
            raise ValueError("members must be a non-empty tuple")
        if not all(isinstance(member, CapturedMember) for member in self.members):
            raise TypeError("members must contain CapturedMember values")
        member_ids = [member.member_id for member in self.members]
        if len(set(member_ids)) != len(member_ids):
            raise ValueError("members contain duplicate member_id values")
        object.__setattr__(
            self,
            "members",
            tuple(sorted(self.members, key=lambda item: item.member_id)),
        )
        if (
            not isinstance(self.expected_member_ids, tuple)
            or not self.expected_member_ids
        ):
            raise ValueError("expected_member_ids must be a non-empty tuple")
        expected = tuple(
            sorted(
                _identifier(value, "expected_member_id")
                for value in self.expected_member_ids
            )
        )
        if len(set(expected)) != len(expected):
            raise ValueError("expected_member_ids contains duplicates")
        object.__setattr__(self, "expected_member_ids", expected)


@dataclass(frozen=True, slots=True)
class MemberReceiptMetadata:
    member_id: str
    role: str
    media_type: str
    payload_sha256: str
    byte_length: int


@dataclass(frozen=True, slots=True)
class RawStoreWritePlan:
    """Secret-free plan; exact bytes remain only in the caller request."""

    member_id: str
    upstream_record_id: str
    payload_sha256: str
    byte_length: int
    media_type: str
    parser_version: str


@dataclass(frozen=True, slots=True)
class CaptureDecision:
    """Immutable admission receipt; weather processing is always allowed."""

    schema_version: str
    provider: str
    product: str
    receipt_id: str
    revision_id: str
    received_at: float
    nominal_at: float | None
    availability: CaptureAvailability
    contract_capture_status: str
    capture_order: CaptureOrder
    processing_allowed: bool
    retention_decision: str
    disposition: RawWriteDisposition
    durable_raw_write_authorized: bool
    reasons: tuple[str, ...]
    payload_sha256: str
    payload_hash_scope: str
    byte_length: int
    expected_member_ids: tuple[str, ...]
    missing_member_ids: tuple[str, ...]
    unexpected_member_ids: tuple[str, ...]
    artifact_set_complete: bool
    atomic_set_required: bool
    contract_registry_sha256: str
    provider_contract_sha256: str
    retention_plan_sha256: str
    members: tuple[MemberReceiptMetadata, ...]
    write_plan: tuple[RawStoreWritePlan, ...]
    decision_sha256: str = field(init=False)

    def __post_init__(self) -> None:
        if self.processing_allowed is not True:
            raise ValueError("processing_allowed must remain true")
        if not isinstance(self.members, tuple) or not self.members:
            raise ValueError("members must be a non-empty tuple")
        if not all(
            isinstance(member, MemberReceiptMetadata)
            and _SHA256.fullmatch(member.payload_sha256)
            and member.byte_length > 0
            for member in self.members
        ):
            raise ValueError("members contain invalid receipt metadata")
        computed_hash, computed_scope = _artifact_set_hash(self.members)
        if (
            computed_hash != self.payload_sha256
            or computed_scope != self.payload_hash_scope
            or sum(member.byte_length for member in self.members)
            != self.byte_length
        ):
            raise ValueError("payload summary does not match member receipts")
        if self.artifact_set_complete != (
            not self.missing_member_ids and not self.unexpected_member_ids
        ):
            raise ValueError("artifact_set_complete contradicts member sets")
        exact = self.availability in {
            CaptureAvailability.UPSTREAM_EXACT_SINGLE,
            CaptureAvailability.UPSTREAM_EXACT_MULTI,
        }
        if self.atomic_set_required != (
            self.availability is CaptureAvailability.UPSTREAM_EXACT_MULTI
        ):
            raise ValueError("atomic_set_required contradicts availability")
        if self.durable_raw_write_authorized:
            if (
                not exact
                or self.capture_order is not CaptureOrder.BEFORE_PARSE
                or not self.artifact_set_complete
                or self.retention_decision != RetentionDecision.STORE.value
                or self.disposition is not RawWriteDisposition.STORE_AUTHORIZED
                or not self.write_plan
            ):
                raise ValueError("authorized raw write has inconsistent facts")
        elif self.write_plan:
            raise ValueError("unauthorized decision must not contain a write plan")
        for name in (
            "payload_sha256",
            "contract_registry_sha256",
            "provider_contract_sha256",
            "retention_plan_sha256",
        ):
            if not _SHA256.fullmatch(str(getattr(self, name))):
                raise ValueError(f"{name} must be lowercase SHA-256")
        metadata = _canonical(self, exclude={"decision_sha256"})
        validate_redacted(metadata)
        object.__setattr__(
            self,
            "decision_sha256",
            _hash_json(metadata),
        )

    def to_receipt_metadata(self) -> dict[str, Any]:
        """Return JSON-safe, payload-free, secret-validated metadata."""

        result = _canonical(self)
        validate_redacted(result)
        return result


@dataclass(frozen=True, slots=True)
class CaptureExecutionReceipt:
    decision_sha256: str
    raw_store_receipt_ids: tuple[str, ...]
    payload_sha256_values: tuple[str, ...]
    byte_length: int
    raw_store_artifact_set_id: str | None = None
    raw_store_manifest_sha256: str | None = None


def admit_artifact_capture(
    request: CaptureRequest,
    *,
    inventory: Iterable[ArtifactEvidence],
    policy: RetentionPolicy,
    capacity: CapacitySnapshot,
) -> CaptureDecision:
    """Make a deterministic raw-write decision without performing I/O."""

    artifacts = tuple(inventory)
    keys = [artifact.key for artifact in artifacts]
    if len(keys) != len(set(keys)):
        raise ValueError("inventory contains duplicate revision identities")

    contract = CONTRACTS.get(request.provider)
    contract_hash = _provider_contract_hash(request.provider)
    member_receipts = tuple(_member_receipt(member) for member in request.members)
    set_hash, hash_scope = _artifact_set_hash(member_receipts)
    total_bytes = sum(member.byte_length for member in member_receipts)
    attribution = (
        contract.attribution.value
        if contract is not None and contract.attribution.value
        else "UNKNOWN"
    )
    candidate = ArtifactEvidence(
        key=ArtifactKey(
            request.provider,
            request.receipt_id,
            request.revision_id,
        ),
        size_bytes=total_bytes,
        received_at=request.received_at,
        nominal_at=request.nominal_at,
        content_sha256=set_hash,
        attribution=attribution,
        provenance=tuple(sorted((
            ("availability", request.availability.value),
            ("capture_order", request.capture_order.value),
            ("member_manifest_sha256", set_hash),
            ("provider_contract_sha256", contract_hash),
        ))),
    )

    existing = next(
        (artifact for artifact in artifacts if artifact.key == candidate.key),
        None,
    )
    if existing is None:
        plan = plan_artifact_retention(
            candidate,
            inventory=artifacts,
            policy=policy,
            capacity=capacity,
        )
        retention_decision = plan.decision.value
        plan_hash = _hash_json({
            "plan": _canonical(plan),
            "policy": _canonical(policy),
            "capacity": _canonical(capacity),
            "inventory": _canonical(artifacts),
        })
        plan_reasons = plan.reasons
        identity_state = "NEW_REVISION"
    elif _same_revision(existing, candidate):
        plan = None
        retention_decision = "IDEMPOTENT_ALREADY_PRESENT"
        plan_hash = _hash_json({
            "state": retention_decision,
            "candidate": _canonical(candidate),
            "policy": _canonical(policy),
            "capacity": _canonical(capacity),
            "inventory": _canonical(artifacts),
        })
        plan_reasons = ("IDEMPOTENT_ALREADY_PRESENT",)
        identity_state = retention_decision
    else:
        plan = None
        retention_decision = "REVISION_CONFLICT"
        plan_hash = _hash_json({
            "state": retention_decision,
            "candidate": _canonical(candidate),
            "existing": _canonical(existing),
            "policy": _canonical(policy),
            "capacity": _canonical(capacity),
            "inventory": _canonical(artifacts),
        })
        plan_reasons = ("REVISION_IDENTITY_REUSED_FOR_DIFFERENT_CONTENT",)
        identity_state = retention_decision

    capture_reasons, missing, unexpected = _capture_reasons(
        request,
        contract=contract,
    )
    exact = request.availability in {
        CaptureAvailability.UPSTREAM_EXACT_SINGLE,
        CaptureAvailability.UPSTREAM_EXACT_MULTI,
    }
    retention_store = (
        plan is not None and plan.decision is RetentionDecision.STORE
    )
    authorized = (
        identity_state == "NEW_REVISION"
        and exact
        and not capture_reasons
        and retention_store
    )
    complete = not missing and not unexpected
    write_plan = (
        _write_plan(request, contract=contract, members=member_receipts)
        if authorized and contract is not None
        else ()
    )
    reasons = tuple(dict.fromkeys((*capture_reasons, *plan_reasons)))
    disposition = _disposition(
        identity_state=identity_state,
        capture_reasons=capture_reasons,
        plan=plan,
    )
    return CaptureDecision(
        schema_version=CAPTURE_ADMISSION_SCHEMA_VERSION,
        provider=request.provider,
        product=contract.product if contract is not None else "UNKNOWN",
        receipt_id=request.receipt_id,
        revision_id=request.revision_id,
        received_at=request.received_at,
        nominal_at=request.nominal_at,
        availability=request.availability,
        contract_capture_status=(
            contract.raw_capture_status.value
            if contract is not None
            else "UNKNOWN"
        ),
        capture_order=request.capture_order,
        processing_allowed=True,
        retention_decision=retention_decision,
        disposition=disposition,
        durable_raw_write_authorized=authorized,
        reasons=reasons,
        payload_sha256=set_hash,
        payload_hash_scope=hash_scope,
        byte_length=total_bytes,
        expected_member_ids=request.expected_member_ids,
        missing_member_ids=missing,
        unexpected_member_ids=unexpected,
        artifact_set_complete=complete,
        atomic_set_required=(
            request.availability
            is CaptureAvailability.UPSTREAM_EXACT_MULTI
        ),
        contract_registry_sha256=contract_registry_sha256(),
        provider_contract_sha256=contract_hash,
        retention_plan_sha256=plan_hash,
        members=member_receipts,
        write_plan=write_plan,
    )


def execute_authorized_store(
    request: CaptureRequest,
    decision: CaptureDecision,
    raw_store: Any,
) -> CaptureExecutionReceipt:
    """Execute an authorized exact-byte write without partial artifact sets."""

    if decision.decision_sha256 != _hash_json(
        _canonical(decision, exclude={"decision_sha256"})
    ):
        raise CaptureExecutionError("capture decision integrity check failed")
    if decision.schema_version != CAPTURE_ADMISSION_SCHEMA_VERSION:
        raise CaptureExecutionError("capture decision schema is not current")
    if not decision.durable_raw_write_authorized:
        raise CaptureExecutionError("durable raw write is not authorized")
    if decision.retention_decision != RetentionDecision.STORE.value:
        raise CaptureExecutionError("retention decision is not STORE")
    if decision.processing_allowed is not True:
        raise CaptureExecutionError("weather processing invariant was violated")
    if (
        request.provider != decision.provider
        or request.receipt_id != decision.receipt_id
        or request.revision_id != decision.revision_id
        or request.received_at != decision.received_at
        or request.nominal_at != decision.nominal_at
        or request.availability is not decision.availability
        or request.capture_order is not decision.capture_order
        or request.expected_member_ids != decision.expected_member_ids
    ):
        raise CaptureExecutionError("request does not match capture decision")
    contract = CONTRACTS.get(request.provider)
    if (
        contract is None
        or not contract.operational_eligible
        or decision.product != contract.product
        or decision.contract_capture_status
        != contract.raw_capture_status.value
        or decision.contract_registry_sha256 != contract_registry_sha256()
        or decision.provider_contract_sha256
        != _provider_contract_hash(request.provider)
    ):
        raise CaptureExecutionError(
            "capture decision does not match the current provider contract"
        )
    capture_reasons, missing, unexpected = _capture_reasons(
        request,
        contract=contract,
    )
    if capture_reasons or missing or unexpected:
        raise CaptureExecutionError(
            "request is no longer eligible for exact raw storage"
        )
    receipts = tuple(_member_receipt(member) for member in request.members)
    set_hash, _ = _artifact_set_hash(receipts)
    if (
        set_hash != decision.payload_sha256
        or sum(item.byte_length for item in receipts) != decision.byte_length
        or receipts != decision.members
    ):
        raise CaptureExecutionError("caller payload does not match decision")

    if len(request.members) != len(decision.write_plan):
        raise CaptureExecutionError("write plan member count mismatch")
    if decision.write_plan != _write_plan(
        request,
        contract=contract,
        members=receipts,
    ):
        raise CaptureExecutionError(
            "write plan does not match the current request and contract"
        )
    for member, write in zip(
        request.members,
        decision.write_plan,
        strict=True,
    ):
        if (
            member.member_id != write.member_id
            or hashlib.sha256(member.payload).hexdigest()
            != write.payload_sha256
            or len(member.payload) != write.byte_length
            or member.media_type != write.media_type
        ):
            raise CaptureExecutionError("write plan member mismatch")

    if decision.atomic_set_required:
        store_set = getattr(raw_store, "store_set", None)
        if not callable(store_set):
            raise AtomicArtifactSetUnsupported(
                "RawArtifactStore has no atomic store_set capability"
            )
        upstream_set_id = _raw_store_upstream_set_id(decision)
        set_receipt = store_set(
            provider_id=decision.provider,
            upstream_set_id=upstream_set_id,
            members=tuple(
                {
                    "member_id": member.member_id,
                    "role": member.role,
                    "upstream_record_id": write.upstream_record_id,
                    "payload": member.payload,
                    "media_type": write.media_type,
                    "parser_version": write.parser_version,
                }
                for member, write in zip(
                    request.members,
                    decision.write_plan,
                    strict=True,
                )
            ),
            fetched_at=decision.received_at,
            nominal_at=decision.nominal_at,
            provenance_sha256=decision.decision_sha256,
        )
        stored_members = tuple(getattr(set_receipt, "members", ()))
        if (
            str(getattr(set_receipt, "provider_id", ""))
            != decision.provider
            or str(getattr(set_receipt, "upstream_set_id", ""))
            != upstream_set_id
            or str(getattr(set_receipt, "provenance_sha256", ""))
            != decision.decision_sha256
            or int(getattr(set_receipt, "member_count", -1))
            != len(decision.write_plan)
            or int(getattr(set_receipt, "total_size_bytes", -1))
            != decision.byte_length
            or len(stored_members) != len(decision.write_plan)
        ):
            raise CaptureExecutionError(
                "RawArtifactStore returned another artifact set"
            )
        for stored, write in zip(
            stored_members,
            decision.write_plan,
            strict=True,
        ):
            if (
                str(getattr(stored, "member_id", "")) != write.member_id
                or str(getattr(stored, "upstream_record_id", ""))
                != write.upstream_record_id
                or str(getattr(stored, "content_sha256", ""))
                != write.payload_sha256
                or int(getattr(stored, "size_bytes", -1))
                != write.byte_length
                or str(getattr(stored, "media_type", ""))
                != write.media_type
                or str(getattr(stored, "parser_version", ""))
                != write.parser_version
            ):
                raise CaptureExecutionError(
                    "RawArtifactStore returned another set member"
                )
        artifact_set_id = str(
            getattr(set_receipt, "artifact_set_id", "")
        )
        manifest_sha256 = str(
            getattr(set_receipt, "manifest_sha256", "")
        )
        if (
            not artifact_set_id
            or not _SHA256.fullmatch(manifest_sha256)
        ):
            raise CaptureExecutionError(
                "RawArtifactStore returned invalid set proof"
            )
        return CaptureExecutionReceipt(
            decision_sha256=decision.decision_sha256,
            raw_store_receipt_ids=tuple(
                str(member.receipt_id) for member in stored_members
            ),
            payload_sha256_values=tuple(
                str(member.content_sha256) for member in stored_members
            ),
            byte_length=decision.byte_length,
            raw_store_artifact_set_id=artifact_set_id,
            raw_store_manifest_sha256=manifest_sha256,
        )

    if len(decision.write_plan) != 1:
        raise CaptureExecutionError(
            "single-artifact decision has multiple write-plan members"
        )
    member = request.members[0]
    write = decision.write_plan[0]
    receipt = raw_store.store(
        provider_id=decision.provider,
        upstream_record_id=write.upstream_record_id,
        payload=member.payload,
        fetched_at=decision.received_at,
        nominal_at=decision.nominal_at,
        media_type=write.media_type,
        parser_version=write.parser_version,
    )
    if (
        str(receipt.provider_id) != decision.provider
        or str(receipt.upstream_record_id) != write.upstream_record_id
        or str(receipt.content_sha256) != write.payload_sha256
        or int(receipt.size_bytes) != write.byte_length
    ):
        raise CaptureExecutionError("RawArtifactStore returned another receipt")
    return CaptureExecutionReceipt(
        decision_sha256=decision.decision_sha256,
        raw_store_receipt_ids=(str(receipt.receipt_id),),
        payload_sha256_values=(str(receipt.content_sha256),),
        byte_length=int(receipt.size_bytes),
    )


def _raw_store_upstream_set_id(decision: CaptureDecision) -> str:
    digest = hashlib.sha256(
        (
            f"{decision.provider}\0{decision.receipt_id}\0"
            f"{decision.revision_id}"
        ).encode("utf-8")
    ).hexdigest()
    return f"capture-set-{digest}"


def _capture_reasons(
    request: CaptureRequest,
    *,
    contract: Any,
) -> tuple[tuple[str, ...], tuple[str, ...], tuple[str, ...]]:
    observed = {member.member_id for member in request.members}
    expected = set(request.expected_member_ids)
    missing = tuple(sorted(expected - observed))
    unexpected = tuple(sorted(observed - expected))
    reasons: list[str] = []
    if contract is None:
        reasons.append("UNKNOWN_PROVIDER_CONTRACT")
    elif not contract.operational_eligible:
        reasons.append("PROVIDER_CAPABILITY_NOT_OPERATIONAL")

    expected_status = {
        CaptureAvailability.UPSTREAM_EXACT_SINGLE: (
            RawCaptureStatus.EXACT_BYTES_TRANSIENT
        ),
        CaptureAvailability.UPSTREAM_EXACT_MULTI: (
            RawCaptureStatus.MULTI_ARTIFACT_TRANSIENT
        ),
        CaptureAvailability.DECODED_BODY_ONLY: (
            RawCaptureStatus.DECODED_BODY_ONLY
        ),
        CaptureAvailability.WORKER_DERIVED_ONLY: (
            RawCaptureStatus.WORKER_DERIVED_ARTIFACT_ONLY
        ),
        CaptureAvailability.UNKNOWN: None,
    }[request.availability]
    if request.availability is CaptureAvailability.UNKNOWN:
        reasons.append("UNKNOWN_CAPTURE_AVAILABILITY")
    elif contract is not None and contract.raw_capture_status is not expected_status:
        reasons.append("CAPTURE_AVAILABILITY_CONTRACT_MISMATCH")

    exact = request.availability in {
        CaptureAvailability.UPSTREAM_EXACT_SINGLE,
        CaptureAvailability.UPSTREAM_EXACT_MULTI,
    }
    if request.availability is CaptureAvailability.DECODED_BODY_ONLY:
        reasons.append("DECODED_BODY_IS_NOT_UPSTREAM_RAW")
    if request.availability is CaptureAvailability.WORKER_DERIVED_ONLY:
        reasons.append("WORKER_DERIVED_ARTIFACT_IS_NOT_UPSTREAM_RAW")
    if exact and request.capture_order is not CaptureOrder.BEFORE_PARSE:
        reasons.append("EXACT_BYTES_NOT_CAPTURED_BEFORE_PARSE")
    if (
        request.availability is CaptureAvailability.UPSTREAM_EXACT_SINGLE
        and len(request.members) != 1
    ):
        reasons.append("SINGLE_ARTIFACT_CAPTURE_HAS_MULTIPLE_MEMBERS")
    if (
        request.availability is CaptureAvailability.UPSTREAM_EXACT_MULTI
        and len(request.expected_member_ids) < 2
    ):
        reasons.append("MULTI_ARTIFACT_CONTRACT_REQUIRES_MULTIPLE_MEMBERS")
    if missing:
        reasons.append("ARTIFACT_SET_MEMBERS_MISSING")
    if unexpected:
        reasons.append("ARTIFACT_SET_HAS_UNEXPECTED_MEMBERS")
    if exact and contract is not None:
        invalid_media = sorted({
            member.media_type
            for member in request.members
            if member.media_type not in contract.allowed_media_types
        })
        if invalid_media:
            reasons.append("MEDIA_TYPE_OUTSIDE_PROVIDER_CONTRACT")
    return tuple(dict.fromkeys(reasons)), missing, unexpected


def _write_plan(
    request: CaptureRequest,
    *,
    contract: Any,
    members: tuple[MemberReceiptMetadata, ...],
) -> tuple[RawStoreWritePlan, ...]:
    result = []
    for member in members:
        source_identity = (
            f"{request.receipt_id}/revision/{request.revision_id}/"
            f"member/{member.member_id}"
        )
        upstream_id = derive_record_id(
            request.provider,
            source_record_id=source_identity,
            nominal_timestamp=request.nominal_at,
            payload_digest=member.payload_sha256,
        )
        result.append(RawStoreWritePlan(
            member_id=member.member_id,
            upstream_record_id=upstream_id,
            payload_sha256=member.payload_sha256,
            byte_length=member.byte_length,
            media_type=member.media_type,
            parser_version=contract.parser_version,
        ))
    return tuple(result)


def _member_receipt(member: CapturedMember) -> MemberReceiptMetadata:
    return MemberReceiptMetadata(
        member_id=member.member_id,
        role=member.role,
        media_type=member.media_type,
        payload_sha256=hashlib.sha256(member.payload).hexdigest(),
        byte_length=len(member.payload),
    )


def _artifact_set_hash(
    members: tuple[MemberReceiptMetadata, ...],
) -> tuple[str, str]:
    if len(members) == 1:
        return members[0].payload_sha256, "single_payload_exact_bytes"
    manifest = {
        "schema": CAPTURE_ADMISSION_SCHEMA_VERSION,
        "members": [_canonical(member) for member in members],
    }
    return (
        _hash_json(manifest),
        "canonical_complete_member_manifest_not_concatenated_bytes",
    )


def _same_revision(
    existing: ArtifactEvidence,
    candidate: ArtifactEvidence,
) -> bool:
    return (
        existing.key == candidate.key
        and existing.size_bytes == candidate.size_bytes
        and existing.nominal_at == candidate.nominal_at
        and existing.content_sha256 == candidate.content_sha256
        and existing.attribution == candidate.attribution
        and existing.provenance == candidate.provenance
    )


def _disposition(
    *,
    identity_state: str,
    capture_reasons: tuple[str, ...],
    plan: RetentionPlan | None,
) -> RawWriteDisposition:
    if identity_state == "IDEMPOTENT_ALREADY_PRESENT":
        return RawWriteDisposition.IDEMPOTENT_ALREADY_PRESENT
    if identity_state == "REVISION_CONFLICT":
        return RawWriteDisposition.REVISION_CONFLICT
    if capture_reasons:
        return RawWriteDisposition.RAW_CAPTURE_BLOCKED
    if plan is None:
        return RawWriteDisposition.RAW_CAPTURE_BLOCKED
    return {
        RetentionDecision.STORE: RawWriteDisposition.STORE_AUTHORIZED,
        RetentionDecision.HOLD: RawWriteDisposition.HOLD,
        RetentionDecision.ARCHIVE_REQUIRED: (
            RawWriteDisposition.ARCHIVE_REQUIRED
        ),
        RetentionDecision.REJECT_CAPACITY: (
            RawWriteDisposition.REJECT_CAPACITY
        ),
    }[plan.decision]


def _provider_contract_hash(provider: str) -> str:
    contract = CONTRACTS.get(provider)
    return _hash_json({
        "provider": provider,
        "contract": contract.to_dict() if contract is not None else None,
    })


def _canonical(value: Any, *, exclude: set[str] | None = None) -> Any:
    excluded = exclude or set()
    if isinstance(value, Enum):
        return value.value
    if is_dataclass(value):
        return {
            item.name: _canonical(getattr(value, item.name))
            for item in fields(value)
            if item.name not in excluded
        }
    if isinstance(value, dict):
        return {
            str(key): _canonical(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if isinstance(value, (tuple, list)):
        return [_canonical(item) for item in value]
    return value


def _hash_json(value: Any) -> str:
    payload = json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


__all__ = [
    "AtomicArtifactSetUnsupported",
    "CAPTURE_ADMISSION_SCHEMA_VERSION",
    "CaptureAvailability",
    "CaptureDecision",
    "CaptureExecutionError",
    "CaptureExecutionReceipt",
    "CaptureOrder",
    "CaptureRequest",
    "CapturedMember",
    "MemberReceiptMetadata",
    "RawStoreWritePlan",
    "RawWriteDisposition",
    "SHADOW_ONLY",
    "admit_artifact_capture",
    "execute_authorized_store",
]
