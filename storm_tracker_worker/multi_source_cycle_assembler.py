"""Assemble exact all-provider cycles from verified raw capture receipts."""
from __future__ import annotations

from dataclasses import dataclass
import math
import re
from typing import Iterable, Mapping

from .desired_state import DesiredState
from .multi_source_operational_cycle import (
    CATALOG_BY_ID,
    CATALOG_SOURCES,
    CycleStatus,
    CycleTarget,
    EvidenceReference,
    GroundInput,
    LightningInput,
    ModelInput,
    MultiSourceOperationalCycle,
    RadarInput,
    SatelliteInput,
    SourceContribution,
    SourceKind,
    ValidationInput,
    empty_source_input,
    evaluate_target_applicability,
)
from .raw_artifacts import RawArtifactCaptureReceipt


METHOD = "worker_verified_raw_capture_cycle_assembler_v1"
# DWD RADOLAN RV is normally published several minutes behind its nominal
# product timestamp.  The proven legacy integration accepts those frames for
# 15 minutes; a 10-minute floor made freshly downloaded RV captures stale
# before worker-only quantitative processing could use them.
DEFAULT_FRESHNESS_SECONDS = 15 * 60
MAX_FRESHNESS_SECONDS = 24 * 60 * 60
_REASON = re.compile(r"^[a-z][a-z0-9_]{0,127}$")
_SHA = re.compile(r"^[0-9a-f]{64}$")


class MultiSourceCycleAssemblerError(ValueError):
    """Verified capture evidence cannot form an exact closed cycle."""


@dataclass(frozen=True, slots=True)
class CapturedSourceEvidence:
    source_id: str
    product_at_ms: int
    received_at_ms: int
    content_sha256: str
    receipt_sha256: str
    size_bytes: int
    schema_id: str = "stv3-worker-verified-raw-capture-evidence-v1"

    def __post_init__(self) -> None:
        if self.source_id not in {item.source_id for item in CATALOG_SOURCES}:
            raise MultiSourceCycleAssemblerError("capturebron staat niet in catalogus")
        for value, label in (
            (self.product_at_ms, "product_at_ms"),
            (self.received_at_ms, "received_at_ms"),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise MultiSourceCycleAssemblerError(f"{label} is ongeldig")
        if self.received_at_ms < self.product_at_ms:
            raise MultiSourceCycleAssemblerError("capturetijden zijn omgekeerd")
        for value, label in (
            (self.content_sha256, "content_sha256"),
            (self.receipt_sha256, "receipt_sha256"),
        ):
            if not isinstance(value, str) or _SHA.fullmatch(value) is None:
                raise MultiSourceCycleAssemblerError(f"{label} is ongeldig")
        if (
            isinstance(self.size_bytes, bool)
            or not isinstance(self.size_bytes, int)
            or not 1 <= self.size_bytes <= 512 * 1024 * 1024
        ):
            raise MultiSourceCycleAssemblerError("capture size_bytes is ongeldig")


def captured_evidence_from_raw_receipt(
    source_id: str,
    capture: RawArtifactCaptureReceipt,
) -> CapturedSourceEvidence:
    """Project a schema-3 capture without copying or exposing raw bytes."""
    if not isinstance(capture, RawArtifactCaptureReceipt) or not capture.complete:
        raise MultiSourceCycleAssemblerError("raw capture is niet volledig")
    if not capture.members:
        raise MultiSourceCycleAssemblerError("raw capture heeft geen members")
    product_at = capture.product_timestamp
    if product_at is None:
        product_at = max(member.received_at for member in capture.members)
    if not math.isfinite(float(product_at)) or product_at < 0:
        raise MultiSourceCycleAssemblerError("raw producttijd is ongeldig")
    content_sha = capture.manifest_sha256
    return CapturedSourceEvidence(
        source_id=source_id,
        product_at_ms=int(float(product_at) * 1000),
        received_at_ms=int(
            max(member.received_at for member in capture.members) * 1000
        ),
        content_sha256=content_sha,
        receipt_sha256=capture.capture_receipt_sha256,
        size_bytes=sum(
            # The capture receipt deliberately does not carry member byte
            # sizes.  The manifest itself is the bounded evidence object here.
            len(member.role.encode("utf-8"))
            + len(member.content_sha256)
            + len(member.receipt_sha256)
            for member in capture.members
        ),
    )


def _fresh_until_ms(source, evidence: CapturedSourceEvidence) -> int:
    cadence = source.cadence_seconds or DEFAULT_FRESHNESS_SECONDS
    freshness = min(
        MAX_FRESHNESS_SECONDS,
        max(DEFAULT_FRESHNESS_SECONDS, cadence * 2),
    )
    return evidence.product_at_ms + freshness * 1000


def _payload(source, evidence: CapturedSourceEvidence):
    proof = (EvidenceReference(
        role="canonical_input",
        content_sha256=evidence.content_sha256,
        receipt_sha256=evidence.receipt_sha256,
        size_bytes=evidence.size_bytes,
        schema_id=evidence.schema_id,
    ),)
    coverage = source.coverage_bbox or (-90.0, -180.0, 90.0, 180.0)
    if source.kind is SourceKind.RADAR:
        return RadarInput(
            evidence.product_at_ms, coverage,
            "provider_native_uninterpreted", "provider_native_unit", 1, 1, proof,
        )
    if source.kind is SourceKind.SATELLITE:
        return SatelliteInput(
            evidence.product_at_ms, coverage,
            source.satellite_id or source.provider_id,
            "provider_native_uninterpreted", "provider_native_unit", 1, 1, proof,
        )
    if source.kind is SourceKind.LIGHTNING:
        return LightningInput(
            max(0, evidence.product_at_ms - 60_000),
            evidence.product_at_ms,
            0,
            0,
            False,
            proof,
        )
    if source.kind is SourceKind.GROUND:
        return GroundInput(
            max(0, evidence.product_at_ms - 60_000),
            evidence.product_at_ms,
            1,
            1,
            ("provider_native_uninterpreted",),
            proof,
        )
    if source.kind is SourceKind.MODEL:
        return ModelInput(
            evidence.product_at_ms,
            evidence.product_at_ms,
            evidence.product_at_ms + (source.cadence_seconds or 3600) * 1000,
            1,
            ("provider_native_guidance",),
            proof,
        )
    return ValidationInput(
        evidence.product_at_ms,
        1,
        source.source_role,
        proof,
    )


def _reason(value: object) -> str:
    if not isinstance(value, str) or _REASON.fullmatch(value) is None:
        return "source_capture_failed"
    return value


def build_raw_capture_operational_cycle(
    desired_state: DesiredState,
    captures: Iterable[CapturedSourceEvidence],
    *,
    source_errors: Mapping[str, str] | None = None,
    contribution_overrides: Mapping[str, SourceContribution] | None = None,
    cycle_sequence: int,
    cycle_started_at_ms: int,
    cycle_completed_at_ms: int,
    producer_id: str = "storm_tracker_worker",
) -> MultiSourceOperationalCycle:
    """Build one exact 29-source status/input cycle from latest raw evidence."""
    if not isinstance(desired_state, DesiredState):
        raise MultiSourceCycleAssemblerError("desired state is niet gevalideerd")
    target_rows = tuple(CycleTarget.create(
        target_id=item.target_id,
        latitude=item.latitude,
        longitude=item.longitude,
        country_code=item.country_code,
        available=item.available,
    ) for item in desired_state.targets)
    if not target_rows:
        raise MultiSourceCycleAssemblerError("desired state heeft geen targets")
    capture_rows = tuple(captures)
    if any(not isinstance(item, CapturedSourceEvidence) for item in capture_rows):
        raise MultiSourceCycleAssemblerError("captures bevatten een fout type")
    by_source = {item.source_id: item for item in capture_rows}
    if len(by_source) != len(capture_rows):
        raise MultiSourceCycleAssemblerError("captures bevatten dubbele bronnen")
    errors = dict(source_errors or {})
    if set(errors) - {item.source_id for item in CATALOG_SOURCES}:
        raise MultiSourceCycleAssemblerError("source_errors bevat onbekende bron")
    overrides = dict(contribution_overrides or {})
    catalog_ids = {item.source_id for item in CATALOG_SOURCES}
    sorted_targets = tuple(sorted(target_rows, key=lambda item: item.target_id))
    if set(overrides) - catalog_ids or any(
        not isinstance(item, SourceContribution)
        or source_id != item.source_id
        or item.applicability != tuple(
            evaluate_target_applicability(CATALOG_BY_ID[source_id], target)
            for target in sorted_targets
        )
        for source_id, item in overrides.items()
    ):
        raise MultiSourceCycleAssemblerError(
            "contribution_overrides bevat ongeldige bron- of targetauthority"
        )
    contributions = []
    for source in CATALOG_SOURCES:
        override = overrides.get(source.source_id)
        if override is not None:
            contributions.append(override)
            continue
        applicable = any(
            evaluate_target_applicability(source, target).applicable
            for target in target_rows
        )
        evidence = by_source.get(source.source_id)
        if not applicable:
            status = CycleStatus.NOT_APPLICABLE
            reason = "target_outside_catalog_scope"
            payload = empty_source_input(source.kind)
            fresh_until = None
        elif source.catalog_blocked:
            status = CycleStatus.BLOCKED
            reason = source.catalog_blocked_reason
            payload = empty_source_input(source.kind)
            fresh_until = None
        elif evidence is None:
            status = CycleStatus.ERROR
            reason = _reason(errors.get(source.source_id, "awaiting_first_capture"))
            payload = empty_source_input(source.kind)
            fresh_until = None
        else:
            fresh_until = _fresh_until_ms(source, evidence)
            status = (
                CycleStatus.CURRENT
                if fresh_until >= cycle_completed_at_ms
                else CycleStatus.STALE
            )
            reason = (
                None if status is CycleStatus.CURRENT
                else "freshness_deadline_exceeded"
            )
            payload = _payload(source, evidence)
        contributions.append(SourceContribution.create(
            source_id=source.source_id,
            targets=target_rows,
            status=status,
            payload=payload,
            reason_code=reason,
            fresh_until_ms=fresh_until,
        ))
    return MultiSourceOperationalCycle.create(
        desired_state_sha256=desired_state.state_sha256,
        authority_epoch=desired_state.authority_epoch,
        cycle_sequence=cycle_sequence,
        producer_id=producer_id,
        cycle_started_at_ms=cycle_started_at_ms,
        cycle_completed_at_ms=cycle_completed_at_ms,
        targets=target_rows,
        contributions=contributions,
    )


__all__ = [
    "CapturedSourceEvidence",
    "DEFAULT_FRESHNESS_SECONDS",
    "MAX_FRESHNESS_SECONDS",
    "METHOD",
    "MultiSourceCycleAssemblerError",
    "build_raw_capture_operational_cycle",
    "captured_evidence_from_raw_receipt",
]
