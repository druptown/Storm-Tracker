"""Pure shadow planner for raw-artifact retention and archive pressure.

The planner is deliberately inert.  It never opens a path, writes an artifact,
archives bytes or deletes local evidence.  It only turns an immutable
inventory, a caller-supplied policy and a capacity snapshot into an auditable
decision.

Every source revision is a separate inventory item.  Content-addressed
deduplication in a future storage layer must therefore not make a revision
disappear from quota and provenance accounting.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import math
import re
from typing import Iterable, Mapping

from .provider_artifact_contracts import (
    CONTRACTS,
    FactKind,
    RetentionClass,
    validate_redacted,
)


RETENTION_PLANNER_SCHEMA_VERSION = "artifact-retention-plan-v1"
SHADOW_ONLY = True
MAX_STORAGE_BYTES = (1 << 63) - 1

_SHA256 = re.compile(r"[0-9a-f]{64}")


class RetentionDecision(str, Enum):
    """The only actions a caller may infer from a plan."""

    STORE = "STORE"
    HOLD = "HOLD"
    ARCHIVE_REQUIRED = "ARCHIVE_REQUIRED"
    REJECT_CAPACITY = "REJECT_CAPACITY"


class ArchiveState(str, Enum):
    """Externally asserted state of one local source revision."""

    LOCAL_ONLY = "LOCAL_ONLY"
    ARCHIVE_UNVERIFIED = "ARCHIVE_UNVERIFIED"
    ARCHIVE_VERIFIED = "ARCHIVE_VERIFIED"


def _identifier(value: str, field_name: str, limit: int = 512) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    normalized = value.strip()
    if len(normalized) > limit or "\0" in normalized:
        raise ValueError(f"{field_name} is invalid")
    return normalized


def _positive_int(value: int, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field_name} must be an integer")
    if value <= 0:
        raise ValueError(f"{field_name} must be greater than zero")
    if value > MAX_STORAGE_BYTES:
        raise OverflowError(f"{field_name} exceeds the storage integer budget")
    return value


def _nonnegative_int(value: int, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field_name} must be an integer")
    if value < 0:
        raise ValueError(f"{field_name} must not be negative")
    if value > MAX_STORAGE_BYTES:
        raise OverflowError(f"{field_name} exceeds the storage integer budget")
    return value


def _finite_time(value: float | None, field_name: str) -> float | None:
    if value is None:
        return None
    number = float(value)
    if not math.isfinite(number) or number < 0.0:
        raise ValueError(f"{field_name} must be a finite non-negative timestamp")
    return number


def _checked_add(left: int, right: int, field_name: str) -> int:
    result = left + right
    if result > MAX_STORAGE_BYTES:
        raise OverflowError(f"{field_name} exceeds the storage integer budget")
    return result


def _checked_sum(values: Iterable[int], field_name: str) -> int:
    result = 0
    for value in values:
        result = _checked_add(result, value, field_name)
    return result


def _normalize_pairs(
    values: Mapping[str, int] | Iterable[tuple[str, int]],
    field_name: str,
) -> tuple[tuple[str, int], ...]:
    items = values.items() if isinstance(values, Mapping) else values
    normalized: dict[str, int] = {}
    for provider, value in items:
        provider_id = _identifier(provider, f"{field_name}.provider", 100)
        if provider_id in normalized:
            raise ValueError(f"{field_name} contains duplicate provider {provider_id!r}")
        normalized[provider_id] = _positive_int(
            value,
            f"{field_name}[{provider_id}]",
        )
    return tuple(sorted(normalized.items()))


def _normalize_attribution(
    values: Mapping[str, str] | Iterable[tuple[str, str]],
) -> tuple[tuple[str, str], ...]:
    items = values.items() if isinstance(values, Mapping) else values
    normalized: dict[str, str] = {}
    for provider, attribution in items:
        provider_id = _identifier(provider, "attribution.provider", 100)
        if provider_id in normalized:
            raise ValueError(
                f"attribution contains duplicate provider {provider_id!r}"
            )
        text = _identifier(attribution, f"attribution[{provider_id}]", 1000)
        validate_redacted(text, record_identity=True)
        normalized[provider_id] = text
    return tuple(sorted(normalized.items()))


@dataclass(frozen=True, order=True, slots=True)
class ArtifactKey:
    """Stable quota identity; a revision never collapses into another."""

    provider: str
    receipt_id: str
    revision_id: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "provider", _identifier(self.provider, "provider", 100))
        object.__setattr__(
            self,
            "receipt_id",
            _identifier(self.receipt_id, "receipt_id"),
        )
        object.__setattr__(
            self,
            "revision_id",
            _identifier(self.revision_id, "revision_id"),
        )
        validate_redacted(
            {
                "provider": self.provider,
                "receipt_id": self.receipt_id,
                "revision_id": self.revision_id,
            },
            record_identity=True,
        )


@dataclass(frozen=True, slots=True)
class ArtifactEvidence:
    """One exact received source revision and its archive/provenance facts."""

    key: ArtifactKey
    size_bytes: int
    received_at: float
    nominal_at: float | None
    content_sha256: str
    attribution: str
    provenance: tuple[tuple[str, str], ...]
    replay_dependency_refcount: int = 0
    archive_state: ArchiveState = ArchiveState.LOCAL_ONLY
    archive_checksum_sha256: str | None = None
    archive_checksum_verified: bool = False
    archive_receipt_id: str | None = None
    legal_hold: bool = False
    locally_present: bool = True

    def __post_init__(self) -> None:
        _positive_int(self.size_bytes, "size_bytes")
        object.__setattr__(
            self,
            "received_at",
            _finite_time(self.received_at, "received_at"),
        )
        object.__setattr__(
            self,
            "nominal_at",
            _finite_time(self.nominal_at, "nominal_at"),
        )
        if not isinstance(self.content_sha256, str) or not _SHA256.fullmatch(
            self.content_sha256
        ):
            raise ValueError("content_sha256 must be lowercase SHA-256")
        object.__setattr__(
            self,
            "attribution",
            _identifier(self.attribution, "attribution", 1000),
        )
        if not isinstance(self.provenance, tuple) or not self.provenance:
            raise ValueError("provenance must contain immutable key/value facts")
        normalized_provenance: list[tuple[str, str]] = []
        seen: set[str] = set()
        for raw_key, raw_value in self.provenance:
            key = _identifier(raw_key, "provenance key", 100)
            value = _identifier(raw_value, f"provenance[{key}]", 1000)
            if key in seen:
                raise ValueError(f"provenance contains duplicate key {key!r}")
            seen.add(key)
            normalized_provenance.append((key, value))
        if tuple(normalized_provenance) != tuple(sorted(normalized_provenance)):
            raise ValueError("provenance must be sorted for deterministic records")
        object.__setattr__(self, "provenance", tuple(normalized_provenance))
        _nonnegative_int(
            self.replay_dependency_refcount,
            "replay_dependency_refcount",
        )
        if self.archive_checksum_sha256 is not None and (
            not isinstance(self.archive_checksum_sha256, str)
            or not _SHA256.fullmatch(self.archive_checksum_sha256)
        ):
            raise ValueError("archive_checksum_sha256 must be lowercase SHA-256")
        if self.archive_receipt_id is not None:
            object.__setattr__(
                self,
                "archive_receipt_id",
                _identifier(self.archive_receipt_id, "archive_receipt_id"),
            )
        if not isinstance(self.archive_checksum_verified, bool):
            raise TypeError("archive_checksum_verified must be boolean")
        if not isinstance(self.legal_hold, bool):
            raise TypeError("legal_hold must be boolean")
        if not isinstance(self.locally_present, bool):
            raise TypeError("locally_present must be boolean")
        validate_redacted(
            {
                "attribution": self.attribution,
                "provenance": self.provenance,
                "archive_receipt_id": self.archive_receipt_id,
            },
            record_identity=True,
        )


@dataclass(frozen=True, slots=True)
class RetentionPolicy:
    """Caller-owned capacity and attribution policy."""

    global_quota_bytes: int
    reserve_free_bytes: int
    per_provider_quota_bytes: tuple[tuple[str, int], ...]
    attribution_by_provider: tuple[tuple[str, str], ...]

    def __post_init__(self) -> None:
        _positive_int(self.global_quota_bytes, "global_quota_bytes")
        _positive_int(self.reserve_free_bytes, "reserve_free_bytes")
        quotas = _normalize_pairs(
            self.per_provider_quota_bytes,
            "per_provider_quota_bytes",
        )
        attribution = _normalize_attribution(self.attribution_by_provider)
        object.__setattr__(self, "per_provider_quota_bytes", quotas)
        object.__setattr__(self, "attribution_by_provider", attribution)

    @classmethod
    def from_mappings(
        cls,
        *,
        global_quota_bytes: int,
        reserve_free_bytes: int,
        per_provider_quota_bytes: Mapping[str, int],
        attribution_by_provider: Mapping[str, str],
    ) -> "RetentionPolicy":
        return cls(
            global_quota_bytes=global_quota_bytes,
            reserve_free_bytes=reserve_free_bytes,
            per_provider_quota_bytes=tuple(per_provider_quota_bytes.items()),
            attribution_by_provider=tuple(attribution_by_provider.items()),
        )


@dataclass(frozen=True, slots=True)
class CapacitySnapshot:
    """External free-space and observed-growth facts at planning time."""

    free_bytes: int
    projected_daily_growth_bytes: int
    measured_at: float

    def __post_init__(self) -> None:
        _positive_int(self.free_bytes, "free_bytes")
        _positive_int(
            self.projected_daily_growth_bytes,
            "projected_daily_growth_bytes",
        )
        object.__setattr__(
            self,
            "measured_at",
            _finite_time(self.measured_at, "measured_at"),
        )


@dataclass(frozen=True, slots=True)
class ArchiveBlock:
    key: ArtifactKey
    reasons: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ArchiveCandidate:
    key: ArtifactKey
    size_bytes: int
    received_at: float


@dataclass(frozen=True, slots=True)
class RetentionPlan:
    """Auditable pure result; selected candidates are still not mutated."""

    schema_version: str
    decision: RetentionDecision
    key: ArtifactKey
    reasons: tuple[str, ...]
    review_required: bool
    candidate_size_bytes: int
    current_global_usage_bytes: int
    current_provider_usage_bytes: int
    projected_global_usage_bytes: int
    projected_provider_usage_bytes: int
    free_bytes_before: int
    projected_free_bytes_after_store: int
    reserve_free_bytes: int
    projected_daily_growth_bytes: int
    days_to_reserve_before: float
    days_to_reserve_after_store: float
    global_reclaim_required_bytes: int
    provider_reclaim_required_bytes: int
    reserve_reclaim_required_bytes: int
    eligible_archive_candidates: tuple[ArchiveCandidate, ...]
    selected_archive_candidates: tuple[ArchiveCandidate, ...]
    selected_reclaim_bytes: int
    archive_blocks: tuple[ArchiveBlock, ...]


def _contract_review_reasons(provider: str) -> tuple[str, ...]:
    contract = CONTRACTS.get(provider)
    if contract is None:
        return ("UNKNOWN_PROVIDER", "REVIEW_REQUIRED")
    reasons: list[str] = []
    if contract.retention_class is RetentionClass.CONTRACT_REVIEW_HOLD:
        reasons.append("CONTRACT_REVIEW_HOLD")
    if contract.license_terms.kind is FactKind.UNKNOWN:
        reasons.append("LICENSE_UNKNOWN")
    if reasons:
        reasons.append("REVIEW_REQUIRED")
    return tuple(reasons)


def _archive_reasons(
    artifact: ArtifactEvidence,
    *,
    policy_quotas: Mapping[str, int],
    configured_attribution: Mapping[str, str],
) -> tuple[str, ...]:
    reasons = list(_contract_review_reasons(artifact.key.provider))
    contract = CONTRACTS.get(artifact.key.provider)
    if contract is not None and not reasons:
        if artifact.key.provider not in policy_quotas:
            reasons.extend(("PROVIDER_QUOTA_NOT_CONFIGURED", "REVIEW_REQUIRED"))
        required = (
            contract.attribution.value
            if contract.attribution.kind is FactKind.FIXED
            else None
        )
        configured = configured_attribution.get(artifact.key.provider)
        if required is None:
            reasons.extend(("ATTRIBUTION_CONTRACT_UNKNOWN", "REVIEW_REQUIRED"))
        elif configured is None:
            reasons.extend(("ATTRIBUTION_NOT_CONFIGURED", "REVIEW_REQUIRED"))
        elif configured != required or artifact.attribution != required:
            reasons.extend(("ATTRIBUTION_MISMATCH", "REVIEW_REQUIRED"))
    if not artifact.locally_present:
        reasons.append("NOT_LOCALLY_PRESENT")
    if artifact.legal_hold:
        reasons.append("LEGAL_HOLD")
    if artifact.replay_dependency_refcount:
        reasons.append("ACTIVE_REPLAY_DEPENDENCY")
    if artifact.archive_state is not ArchiveState.ARCHIVE_VERIFIED:
        reasons.append("ARCHIVE_NOT_VERIFIED")
    if not artifact.archive_checksum_verified:
        reasons.append("ARCHIVE_CHECKSUM_NOT_VERIFIED")
    if artifact.archive_receipt_id is None:
        reasons.append("ARCHIVE_RECEIPT_MISSING")
    if artifact.archive_checksum_sha256 is None:
        reasons.append("ARCHIVE_CHECKSUM_MISSING")
    elif artifact.archive_checksum_sha256 != artifact.content_sha256:
        reasons.append("ARCHIVE_CHECKSUM_MISMATCH")
    return tuple(dict.fromkeys(reasons))


def _archive_inventory(
    artifacts: tuple[ArtifactEvidence, ...],
    *,
    policy_quotas: Mapping[str, int],
    configured_attribution: Mapping[str, str],
) -> tuple[tuple[ArchiveCandidate, ...], tuple[ArchiveBlock, ...]]:
    eligible: list[ArchiveCandidate] = []
    blocked: list[ArchiveBlock] = []
    ordered = sorted(
        artifacts,
        key=lambda item: (
            item.received_at,
            item.key.provider,
            item.key.receipt_id,
            item.key.revision_id,
        ),
    )
    for artifact in ordered:
        reasons = _archive_reasons(
            artifact,
            policy_quotas=policy_quotas,
            configured_attribution=configured_attribution,
        )
        if reasons:
            blocked.append(ArchiveBlock(artifact.key, reasons))
        else:
            eligible.append(
                ArchiveCandidate(
                    artifact.key,
                    artifact.size_bytes,
                    artifact.received_at,
                )
            )
    return tuple(eligible), tuple(blocked)


def _days_to_reserve(free_bytes: int, reserve: int, daily_growth: int) -> float:
    return max(free_bytes - reserve, 0) / daily_growth


def _select_archive_candidates(
    eligible: tuple[ArchiveCandidate, ...],
    *,
    provider: str,
    provider_required: int,
    total_required: int,
) -> tuple[ArchiveCandidate, ...] | None:
    selected: list[ArchiveCandidate] = []
    selected_keys: set[ArtifactKey] = set()
    provider_bytes = 0
    total_bytes = 0

    for item in eligible:
        if item.key.provider != provider or provider_bytes >= provider_required:
            continue
        selected.append(item)
        selected_keys.add(item.key)
        provider_bytes = _checked_add(
            provider_bytes,
            item.size_bytes,
            "selected provider archive bytes",
        )
        total_bytes = _checked_add(
            total_bytes,
            item.size_bytes,
            "selected archive bytes",
        )
    if provider_bytes < provider_required:
        return None

    for item in eligible:
        if total_bytes >= total_required:
            break
        if item.key in selected_keys:
            continue
        selected.append(item)
        selected_keys.add(item.key)
        total_bytes = _checked_add(
            total_bytes,
            item.size_bytes,
            "selected archive bytes",
        )
    if total_bytes < total_required:
        return None
    return tuple(selected)


def plan_artifact_retention(
    candidate: ArtifactEvidence,
    *,
    inventory: Iterable[ArtifactEvidence],
    policy: RetentionPolicy,
    capacity: CapacitySnapshot,
) -> RetentionPlan:
    """Plan one revision without performing storage or archive I/O."""

    artifacts = tuple(inventory)
    keys = [artifact.key for artifact in artifacts]
    if len(set(keys)) != len(keys):
        raise ValueError("inventory contains duplicate source revision identities")
    if candidate.key in set(keys):
        raise ValueError("candidate revision already exists in inventory")

    current_global = _checked_sum(
        (
            artifact.size_bytes
            for artifact in artifacts
            if artifact.locally_present
        ),
        "current global usage",
    )
    current_provider = _checked_sum(
        (
            artifact.size_bytes
            for artifact in artifacts
            if artifact.locally_present
            and artifact.key.provider == candidate.key.provider
        ),
        "current provider usage",
    )
    projected_global = _checked_add(
        current_global,
        candidate.size_bytes,
        "projected global usage",
    )
    projected_provider = _checked_add(
        current_provider,
        candidate.size_bytes,
        "projected provider usage",
    )
    projected_free = max(capacity.free_bytes - candidate.size_bytes, 0)
    policy_quotas = dict(policy.per_provider_quota_bytes)
    configured_attribution = dict(policy.attribution_by_provider)
    eligible, blocks = _archive_inventory(
        artifacts,
        policy_quotas=policy_quotas,
        configured_attribution=configured_attribution,
    )
    reasons = list(_contract_review_reasons(candidate.key.provider))
    contract = CONTRACTS.get(candidate.key.provider)
    provider_quota = policy_quotas.get(candidate.key.provider)
    if contract is not None and not reasons:
        if provider_quota is None:
            reasons.extend(("PROVIDER_QUOTA_NOT_CONFIGURED", "REVIEW_REQUIRED"))
        required = (
            contract.attribution.value
            if contract.attribution.kind is FactKind.FIXED
            else None
        )
        configured = configured_attribution.get(candidate.key.provider)
        if required is None:
            reasons.extend(("ATTRIBUTION_CONTRACT_UNKNOWN", "REVIEW_REQUIRED"))
        elif configured is None:
            reasons.extend(("ATTRIBUTION_NOT_CONFIGURED", "REVIEW_REQUIRED"))
        elif configured != required or candidate.attribution != required:
            reasons.extend(("ATTRIBUTION_MISMATCH", "REVIEW_REQUIRED"))

    global_required = max(projected_global - policy.global_quota_bytes, 0)
    provider_required = (
        max(projected_provider - provider_quota, 0)
        if provider_quota is not None
        else 0
    )
    reserve_projection = _checked_add(
        candidate.size_bytes,
        policy.reserve_free_bytes,
        "candidate plus free-space reserve",
    )
    reserve_required = max(
        reserve_projection - capacity.free_bytes,
        0,
    )

    base = {
        "schema_version": RETENTION_PLANNER_SCHEMA_VERSION,
        "key": candidate.key,
        "candidate_size_bytes": candidate.size_bytes,
        "current_global_usage_bytes": current_global,
        "current_provider_usage_bytes": current_provider,
        "projected_global_usage_bytes": projected_global,
        "projected_provider_usage_bytes": projected_provider,
        "free_bytes_before": capacity.free_bytes,
        "projected_free_bytes_after_store": projected_free,
        "reserve_free_bytes": policy.reserve_free_bytes,
        "projected_daily_growth_bytes": capacity.projected_daily_growth_bytes,
        "days_to_reserve_before": _days_to_reserve(
            capacity.free_bytes,
            policy.reserve_free_bytes,
            capacity.projected_daily_growth_bytes,
        ),
        "days_to_reserve_after_store": _days_to_reserve(
            projected_free,
            policy.reserve_free_bytes,
            capacity.projected_daily_growth_bytes,
        ),
        "global_reclaim_required_bytes": global_required,
        "provider_reclaim_required_bytes": provider_required,
        "reserve_reclaim_required_bytes": reserve_required,
        "eligible_archive_candidates": eligible,
        "archive_blocks": blocks,
    }

    if reasons:
        return RetentionPlan(
            decision=RetentionDecision.HOLD,
            reasons=tuple(dict.fromkeys(reasons)),
            review_required=True,
            selected_archive_candidates=(),
            selected_reclaim_bytes=0,
            **base,
        )

    required_total = max(global_required, reserve_required)
    if not required_total and not provider_required:
        return RetentionPlan(
            decision=RetentionDecision.STORE,
            reasons=("WITHIN_CONFIGURED_POLICY",),
            review_required=False,
            selected_archive_candidates=(),
            selected_reclaim_bytes=0,
            **base,
        )

    selected = _select_archive_candidates(
        eligible,
        provider=candidate.key.provider,
        provider_required=provider_required,
        total_required=required_total,
    )
    if selected is None:
        if candidate.legal_hold:
            return RetentionPlan(
                decision=RetentionDecision.HOLD,
                reasons=("LEGAL_HOLD_CAPACITY_REVIEW", "REVIEW_REQUIRED"),
                review_required=True,
                selected_archive_candidates=(),
                selected_reclaim_bytes=0,
                **base,
            )
        return RetentionPlan(
            decision=RetentionDecision.REJECT_CAPACITY,
            reasons=("INSUFFICIENT_ARCHIVABLE_BYTES",),
            review_required=False,
            selected_archive_candidates=(),
            selected_reclaim_bytes=0,
            **base,
        )

    reclaimed = _checked_sum(
        (item.size_bytes for item in selected),
        "selected reclaim bytes",
    )
    return RetentionPlan(
        decision=RetentionDecision.ARCHIVE_REQUIRED,
        reasons=("VERIFIED_ARCHIVE_ACTION_REQUIRED_BEFORE_STORE",),
        review_required=False,
        selected_archive_candidates=selected,
        selected_reclaim_bytes=reclaimed,
        **base,
    )


__all__ = [
    "ArchiveBlock",
    "ArchiveCandidate",
    "ArchiveState",
    "ArtifactEvidence",
    "ArtifactKey",
    "CapacitySnapshot",
    "MAX_STORAGE_BYTES",
    "RETENTION_PLANNER_SCHEMA_VERSION",
    "RetentionDecision",
    "RetentionPlan",
    "RetentionPolicy",
    "SHADOW_ONLY",
    "plan_artifact_retention",
]
