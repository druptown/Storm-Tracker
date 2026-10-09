"""Expose one complete multi-source cycle through the HA snapshot contract.

This is the bounded status/read-model bridge between the worker-owned source
cycle and the thin Home Assistant client.  It deliberately keeps target
weather facts unknown until a later fusion stage has admitted actual values,
while making every catalog source and its exact failure state visible.
"""
from __future__ import annotations

import math
import re
from typing import Any, Mapping

from .desired_state import DesiredState
from .multi_source_operational_cycle import (
    CycleStatus,
    GroundInput,
    LightningInput,
    ModelInput,
    MultiSourceOperationalCycle,
    RadarInput,
    SatelliteInput,
    SourceContribution,
    ValidationInput,
)
from .operational_snapshot_builder import (
    PENDING_REGION_ID,
    build_unknown_operational_snapshot,
)
from .operational_fact_revision import (
    AdmissionDisposition,
    CommittedOperationalFactRevision,
    FactAdmissionReceipt,
    OperationalFactKind,
    validate_committed_operational_revision,
)
from .tracking_core.operational_snapshot import validate_snapshot
from .worker_acquisition_plan import build_worker_acquisition_plan


METHOD = "worker_multi_source_status_snapshot_v1"
MIN_PUBLICATION_LEAD_MS = 30_000
REASON_VALUES_NOT_FUSED = "multi_source_values_not_yet_fused"
REASON_SOURCE_CURRENT = "source_current"
REASON_FRESHNESS_EXPIRED = "freshness_deadline_exceeded"
REASON_FRESHNESS_NOT_PUBLICATION_SAFE = (
    "freshness_deadline_not_publication_safe"
)


class MultiSourceSnapshotError(ValueError):
    """The cycle cannot safely be represented for the desired state."""


def _timestamp(value: object, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise MultiSourceSnapshotError(f"{label} moet een tijdstip zijn")
    result = float(value)
    if not math.isfinite(result) or result < 0:
        raise MultiSourceSnapshotError(f"{label} moet eindig en positief zijn")
    return result


def _product_valid_at(contribution: SourceContribution) -> float | None:
    payload = contribution.payload
    milliseconds: int | None
    if isinstance(payload, (RadarInput, SatelliteInput)):
        milliseconds = payload.observed_at_ms
    elif isinstance(payload, (LightningInput, GroundInput)):
        milliseconds = payload.window_end_ms
    elif isinstance(payload, ModelInput):
        milliseconds = payload.valid_from_ms
    elif isinstance(payload, ValidationInput):
        milliseconds = payload.reference_at_ms
    else:  # pragma: no cover - closed SourceInput union
        raise MultiSourceSnapshotError("source payloadtype is onbekend")
    return None if milliseconds is None else milliseconds / 1000.0


def _effective_status(
    contribution: SourceContribution,
    *,
    evaluated_at_ms: int,
) -> CycleStatus:
    if (
        contribution.status is CycleStatus.CURRENT
        and contribution.fresh_until_ms is not None
        and contribution.fresh_until_ms
        <= evaluated_at_ms + MIN_PUBLICATION_LEAD_MS
    ):
        return CycleStatus.STALE
    return contribution.status


def publication_safe_current_source_ids(
    cycle: MultiSourceOperationalCycle,
    *,
    evaluated_at_ms: int,
) -> frozenset[str]:
    """Return sources that remain current through the publication margin."""

    if not isinstance(cycle, MultiSourceOperationalCycle):
        raise MultiSourceSnapshotError("cycle is niet strikt gevalideerd")
    if isinstance(evaluated_at_ms, bool) or not isinstance(evaluated_at_ms, int):
        raise MultiSourceSnapshotError("evaluated_at_ms moet een geheel tijdstip zijn")
    return frozenset(
        contribution.source_id
        for contribution in cycle.contributions
        if _effective_status(
            contribution,
            evaluated_at_ms=evaluated_at_ms,
        ) is CycleStatus.CURRENT
    )


def _provider_item(
    contribution: SourceContribution,
    *,
    evaluated_at_ms: int,
    received_at: float,
) -> dict[str, Any]:
    effective = _effective_status(
        contribution,
        evaluated_at_ms=evaluated_at_ms,
    )
    product_valid_at = _product_valid_at(contribution)
    if effective is CycleStatus.CURRENT:
        if product_valid_at is None:
            raise MultiSourceSnapshotError(
                f"actuele bron {contribution.source_id} mist producttijd"
            )
        state = "operational"
        freshness = "current"
        reason = contribution.reason_code or REASON_SOURCE_CURRENT
        error = None
    elif effective is CycleStatus.STALE:
        state = "stale"
        freshness = "stale"
        if contribution.reason_code is not None:
            reason = contribution.reason_code
        elif (
            contribution.status is CycleStatus.CURRENT
            and contribution.fresh_until_ms is not None
            and contribution.fresh_until_ms > evaluated_at_ms
        ):
            reason = REASON_FRESHNESS_NOT_PUBLICATION_SAFE
        else:
            reason = REASON_FRESHNESS_EXPIRED
        error = None
    elif effective is CycleStatus.ERROR:
        state = "degraded"
        freshness = "unknown"
        reason = contribution.reason_code or "source_failed"
        error = reason
    elif effective is CycleStatus.BLOCKED:
        freshness = "unknown"
        reason = contribution.reason_code or "source_blocked"
        if reason == "api_required":
            state = "api_required"
            error = None
        else:
            state = "offline"
            error = reason
    else:
        state = "unknown"
        freshness = "unknown"
        reason = contribution.reason_code or "target_outside_catalog_scope"
        error = None
    return {
        # Source identities, rather than shared vendor IDs, remain unique.  A
        # vendor can supply multiple independently fresh products.
        "provider_id": contribution.source_id,
        "display_name": contribution.source_id,
        "state": state,
        "freshness": freshness,
        "product_valid_at": product_valid_at,
        "received_at": received_at if product_valid_at is not None else None,
        "error": error,
        "reason_codes": [reason],
    }


def _verify_cycle_targets(
    desired_state: DesiredState,
    cycle: MultiSourceOperationalCycle,
) -> None:
    desired_targets = tuple(
        (
            item.target_id,
            float(item.latitude),
            float(item.longitude),
            item.country_code,
            item.available,
        )
        for item in desired_state.targets
    )
    cycle_targets = tuple(
        (
            item.target_id,
            float(item.latitude),
            float(item.longitude),
            item.country_code,
            item.available,
        )
        for item in cycle.targets
    )
    if cycle_targets != desired_targets:
        raise MultiSourceSnapshotError(
            "cycle targets verschillen van de actieve desired state"
        )


def _project_acquisition_regions(
    desired_state: DesiredState,
    snapshot: dict[str, Any],
) -> None:
    """Project the worker's exact acquisition topology into the read model."""

    plan = build_worker_acquisition_plan(desired_state)
    target_by_id = {
        item["target_id"]: item for item in snapshot["targets"]["items"]
    }
    assigned: set[str] = set()
    regions: list[dict[str, Any]] = []
    for index, region in enumerate(plan.regions, start=1):
        target_ids = list(region.target_ids)
        regions.append({
            "region_id": region.region_id,
            "display_name": f"Regio {index}",
            "state": "unknown",
            "provider_ids": [],
            "target_ids": target_ids,
        })
        for target_id in target_ids:
            target_by_id[target_id]["region_id"] = region.region_id
            assigned.add(target_id)

    # Unavailable locations deliberately remain pending. They are still part
    # of the target-complete snapshot, but may not invent a schedulable region.
    pending = sorted(set(target_by_id) - assigned)
    if pending:
        regions.append({
            "region_id": PENDING_REGION_ID,
            "display_name": "Nog niet meteorologisch toegewezen",
            "state": "unknown",
            "provider_ids": [],
            "target_ids": pending,
        })
    snapshot["regions"]["items"] = sorted(
        regions,
        key=lambda item: item["region_id"],
    )


def build_multi_source_status_snapshot(
    desired_state: DesiredState,
    cycle: MultiSourceOperationalCycle,
    *,
    revision: int,
    evaluated_at: float,
    worker_state: str = "online",
    queue_depth: int = 0,
    failed_jobs: int = 0,
    provider_status_overrides: Mapping[str, tuple[str, str]] | None = None,
) -> dict[str, Any]:
    """Build a strict snapshot that exposes all sources without fake weather.

    A source marked current contributes health and provenance only.  Target
    rain, storm, ETA, probability and warning facts stay unknown until the
    value-fusion stage supplies independently admissible facts.
    """
    if not isinstance(desired_state, DesiredState):
        raise MultiSourceSnapshotError("desired_state is niet strikt gevalideerd")
    if not isinstance(cycle, MultiSourceOperationalCycle):
        raise MultiSourceSnapshotError("cycle is niet strikt gevalideerd")
    if cycle.desired_state_sha256 != desired_state.state_sha256:
        raise MultiSourceSnapshotError(
            "cycle hoort niet bij de actieve desired state"
        )
    if cycle.authority_epoch != desired_state.authority_epoch:
        raise MultiSourceSnapshotError("cycle authority epoch is verouderd")
    _verify_cycle_targets(desired_state, cycle)
    evaluated = _timestamp(evaluated_at, "evaluated_at")
    evaluated_ms = int(evaluated * 1000)
    if evaluated_ms < cycle.cycle_completed_at_ms:
        raise MultiSourceSnapshotError("evaluated_at ligt voor cycle completion")
    received_at = cycle.cycle_completed_at_ms / 1000.0

    providers = [
        _provider_item(
            contribution,
            evaluated_at_ms=evaluated_ms,
            received_at=received_at,
        )
        for contribution in cycle.contributions
    ]
    overrides = dict(provider_status_overrides or {})
    allowed_override_ids = {"lightning.blitzortung.event_ingress"}
    if set(overrides) - allowed_override_ids:
        raise MultiSourceSnapshotError("providerstatusoverride bevat onbekende bron")
    providers_by_id = {item["provider_id"]: item for item in providers}
    for provider_id, override in overrides.items():
        if (
            not isinstance(override, tuple)
            or len(override) != 2
            or override[0] not in {"operational", "degraded", "offline", "unknown"}
            or not isinstance(override[1], str)
            or re.fullmatch(r"[a-z][a-z0-9_]{0,127}", override[1]) is None
        ):
            raise MultiSourceSnapshotError("providerstatusoverride is ongeldig")
        provider = providers_by_id[provider_id]
        state, reason = override
        provider.update({
            "state": state,
            "freshness": "unknown",
            "product_valid_at": None,
            "received_at": None,
            "error": reason if state in {"degraded", "offline"} else None,
            "reason_codes": [reason],
        })
    current_ids = sorted(
        item["provider_id"]
        for item in providers
        if item["freshness"] == "current"
    )
    current_product_times = [
        float(item["product_valid_at"])
        for item in providers
        if item["freshness"] == "current"
        and item["product_valid_at"] is not None
    ]
    current_deadlines = [
        contribution.fresh_until_ms / 1000.0
        for contribution in cycle.contributions
        if _effective_status(
            contribution,
            evaluated_at_ms=evaluated_ms,
        ) is CycleStatus.CURRENT
        and contribution.fresh_until_ms is not None
    ]
    effective_counts = {
        "current": sum(item["state"] == "operational" for item in providers),
        "stale": sum(item["state"] == "stale" for item in providers),
        "error": sum(item["state"] == "degraded" for item in providers),
        "blocked": sum(item["state"] == "offline" for item in providers),
        "api_required": sum(item["state"] == "api_required" for item in providers),
    }
    status_text = (
        f"Bronnen: {len(current_ids)} actueel, "
        f"{effective_counts['stale']} verouderd, "
        f"{effective_counts['error']} fout, "
        f"{effective_counts['blocked']} geblokkeerd, "
        f"{effective_counts['api_required']} API vereist"
    )
    snapshot = build_unknown_operational_snapshot(
        desired_state,
        revision=revision,
        evaluated_at=evaluated,
        worker_state=worker_state,
        queue_depth=queue_depth,
        failed_jobs=failed_jobs,
        reason_code=REASON_VALUES_NOT_FUSED,
        worker_status_text=status_text,
    )
    _project_acquisition_regions(desired_state, snapshot)
    snapshot["providers"]["items"] = providers
    snapshot["provenance_source_ids"] = current_ids
    snapshot["provenance_reason_codes"] = [REASON_VALUES_NOT_FUSED]
    if current_product_times:
        snapshot["product_valid_at"] = max(current_product_times)
        snapshot["received_at"] = received_at
        snapshot["valid_until"] = min(
            float(snapshot["valid_until"]),
            min(current_deadlines),
        )
    return validate_snapshot(snapshot)


def build_multi_source_status_fact_revision(
    desired_state: DesiredState,
    cycle: MultiSourceOperationalCycle,
    *,
    revision: int,
    evaluated_at: float,
    worker_state: str = "online",
    queue_depth: int = 0,
    failed_jobs: int = 0,
    provider_status_overrides: Mapping[str, tuple[str, str]] | None = None,
) -> CommittedOperationalFactRevision:
    """Commit the all-source status view with exact current-source evidence."""

    snapshot = build_multi_source_status_snapshot(
        desired_state,
        cycle,
        revision=revision,
        evaluated_at=evaluated_at,
        worker_state=worker_state,
        queue_depth=queue_depth,
        failed_jobs=failed_jobs,
        provider_status_overrides=provider_status_overrides,
    )
    contribution_by_id = {
        item.source_id: item for item in cycle.contributions
    }
    receipts = []
    for provider in snapshot["providers"]["items"]:
        if provider["freshness"] != "current":
            continue
        contribution = contribution_by_id[provider["provider_id"]]
        source_receipts = tuple(
            evidence.receipt_sha256
            for evidence in contribution.payload.evidence
        )
        if not source_receipts:
            raise MultiSourceSnapshotError(
                f"actuele bron {contribution.source_id} mist evidencereceipt"
            )
        receipts.append(FactAdmissionReceipt.create(
            fact_kind=OperationalFactKind.PROVIDER_CURRENT,
            scope_id=provider["provider_id"],
            fact_value=provider,
            desired_state_sha256=desired_state.state_sha256,
            authority_epoch=desired_state.authority_epoch,
            source_receipt_sha256s=source_receipts,
            certificate_sha256s=(
                contribution.source_descriptor_sha256,
            ),
            process_receipt_sha256=contribution.contribution_sha256,
            full_scan_complete=False,
            external_truth_validated=False,
            decided_at=float(snapshot["evaluated_at"]),
            valid_until=float(snapshot["valid_until"]),
            disposition=AdmissionDisposition.ADMITTED,
            reason_codes=(REASON_SOURCE_CURRENT,),
        ))
    committed = CommittedOperationalFactRevision.create(
        desired_state_sha256=desired_state.state_sha256,
        authority_epoch=desired_state.authority_epoch,
        snapshot=snapshot,
        admission_receipts=receipts,
    )
    validate_committed_operational_revision(desired_state, committed)
    return committed


__all__ = [
    "METHOD",
    "MIN_PUBLICATION_LEAD_MS",
    "MultiSourceSnapshotError",
    "REASON_FRESHNESS_EXPIRED",
    "REASON_FRESHNESS_NOT_PUBLICATION_SAFE",
    "REASON_SOURCE_CURRENT",
    "REASON_VALUES_NOT_FUSED",
    "build_multi_source_status_fact_revision",
    "build_multi_source_status_snapshot",
    "publication_safe_current_source_ids",
]
