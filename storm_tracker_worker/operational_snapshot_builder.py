"""Pure fail-closed builders for worker-owned operational snapshots.

The first safe cutover snapshot must be publishable before any provider has
produced admissible meteorological evidence.  It must still enumerate the
configured Home Assistant targets, but it may not invent weather, source times,
provider health, positions or probabilities.  This module builds precisely that
initial/transition state.  Rich weather snapshots are added by later worker
pipeline stages using the same strict contract.
"""
from __future__ import annotations

import math
import re
from typing import Any

from .desired_state import DesiredState
from .tracking_core.operational_snapshot import (
    MAX_SNAPSHOT_TTL_SECONDS,
    validate_snapshot,
)
from .worker_acquisition_plan import build_worker_acquisition_regions


METHOD = "worker_unknown_operational_snapshot_v1"
PENDING_REGION_ID = "region:awaiting_evidence"
# Unknown snapshots make no weather claim, but their control-plane state must
# still be renewed.  Keep this fixed and equal to the contract's hard ceiling;
# callers cannot silently extend authority with a custom TTL.
UNKNOWN_SNAPSHOT_TTL_SECONDS = 5 * 60.0
if UNKNOWN_SNAPSHOT_TTL_SECONDS > MAX_SNAPSHOT_TTL_SECONDS:
    raise RuntimeError("unknown snapshot TTL overschrijdt contractgrens")
_REASON_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,159}$")
_WORKER_STATES = frozenset({
    "online", "busy", "degraded", "offline", "unknown",
})


class OperationalSnapshotBuildError(ValueError):
    """The caller supplied invalid bounded control-plane material."""


def _counter(value: object, name: str) -> int:
    if type(value) is not int or not 0 <= value <= 1_000_000_000:
        raise OperationalSnapshotBuildError(f"{name} is geen geldige teller")
    return value


def _timestamp(value: object, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise OperationalSnapshotBuildError(f"{name} moet een tijdstip zijn")
    result = float(value)
    if not math.isfinite(result) or result < 0:
        raise OperationalSnapshotBuildError(f"{name} moet eindig en positief zijn")
    return result


def _reason(value: object) -> str:
    if not isinstance(value, str) or _REASON_RE.fullmatch(value) is None:
        raise OperationalSnapshotBuildError("reason_code is geen veilig begrensd ID")
    return value


def _status_text(value: object) -> str:
    if not isinstance(value, str) or value != value.strip() or len(value) > 256:
        raise OperationalSnapshotBuildError("worker_status_text is ongeldig")
    if any(ord(character) < 32 for character in value):
        raise OperationalSnapshotBuildError("worker_status_text bevat controletekens")
    return value


def _unknown_ground(reason_code: str) -> dict[str, Any]:
    return {
        "status": "unknown",
        "freshness": "unknown",
        "confirmation": None,
        "same_track": None,
        "evidence_valid_at": None,
        "observed_at_min": None,
        "observed_at_max": None,
        "received_at": None,
        "physical_site_count": 0,
        "wet_site_count": 0,
        "source_ids": [],
        "reason_codes": [reason_code],
    }


def _unknown_target(
    target,
    reason_code: str,
    *,
    region_id: str,
) -> dict[str, Any]:
    return {
        "target_id": target.target_id,
        "display_name": target.display_name,
        "region_id": region_id,
        # Both weather facts are unknown, so the strict contract requires an
        # empty user-facing status rather than a derived sentence.
        "status_text": "",
        "current_precipitation": {
            "state": "unknown",
            "freshness": "unknown",
            "rain_rate_mm_h": None,
            "product_valid_at": None,
            "provider_ids": [],
            "reason_codes": [reason_code],
            "ground_evidence": _unknown_ground(reason_code),
        },
        "next_threat": {
            "state": "unknown",
            "freshness": "unknown",
            "track_id": None,
            "calibrated_probability_percent": None,
            "probability_calibration_id": None,
            "eta_minutes": None,
            "distance_km": None,
            "passage_status": "unknown",
            "product_valid_at": None,
            "provider_ids": [],
            "reason_codes": [reason_code],
        },
    }


def build_unknown_operational_snapshot(
    desired_state: DesiredState,
    *,
    revision: int,
    evaluated_at: float,
    worker_state: str,
    queue_depth: int,
    failed_jobs: int,
    reason_code: str = "awaiting_meteorological_evidence",
    worker_status_text: str = "",
) -> dict[str, Any]:
    """Build a target-complete snapshot containing no weather assertions.

    Coordinates, location accuracy, target availability and desired-state
    settings intentionally do not cross this read-model boundary.  They remain
    worker-internal scheduling inputs.  The top-level product and receipt times
    are ``None`` because no meteorological source has supplied either fact.
    """
    if not isinstance(desired_state, DesiredState):
        raise OperationalSnapshotBuildError(
            "desired_state moet eerst strikt gevalideerd zijn"
        )
    revision_value = _counter(revision, "revision")
    if revision_value < 1:
        raise OperationalSnapshotBuildError("revision moet positief zijn")
    evaluated = _timestamp(evaluated_at, "evaluated_at")
    if worker_state not in _WORKER_STATES:
        raise OperationalSnapshotBuildError("worker_state is niet ondersteund")
    queue_value = _counter(queue_depth, "queue_depth")
    failed_value = _counter(failed_jobs, "failed_jobs")
    reason = _reason(reason_code)
    status_text = _status_text(worker_status_text)

    planned_regions = build_worker_acquisition_regions(desired_state)
    target_regions = {
        target_id: region.region_id
        for region in planned_regions
        for target_id in region.target_ids
    }
    ordered_targets = sorted(
        desired_state.targets, key=lambda item: item.target_id
    )
    unassigned_target_ids = [
        target.target_id
        for target in ordered_targets
        if target.target_id not in target_regions
    ]
    targets = [
        _unknown_target(
            target,
            reason,
            region_id=target_regions.get(target.target_id, PENDING_REGION_ID),
        )
        for target in ordered_targets
    ]
    regions = [
        {
            "region_id": region.region_id,
            "display_name": f"Regio {index}",
            "state": "unknown",
            "provider_ids": [],
            "target_ids": list(region.target_ids),
        }
        for index, region in enumerate(planned_regions, start=1)
    ]
    if unassigned_target_ids:
        regions.append({
            "region_id": PENDING_REGION_ID,
            "display_name": "Nog niet meteorologisch toegewezen",
            "state": "unknown",
            "provider_ids": [],
            "target_ids": unassigned_target_ids,
        })
    # The strict snapshot contract requires every collection to be ordered by
    # its public identifier.  Acquisition regions are already sorted, but the
    # synthetic pending region can sort either before or after their hashed
    # identifiers.  Sort the combined collection so the result is valid for
    # every live target/region hash, not merely hashes below ``awaiting_*``.
    regions.sort(key=lambda item: item["region_id"])
    snapshot = {
        "contract": "operational_snapshot_v1",
        "revision": revision_value,
        "desired_state_sha256": desired_state.state_sha256,
        "product_valid_at": None,
        "received_at": None,
        "evaluated_at": evaluated,
        "valid_until": evaluated + UNKNOWN_SNAPSHOT_TTL_SECONDS,
        "provenance_complete": False,
        "provenance_source_ids": [],
        "provenance_reason_codes": [reason],
        "worker": {
            "state": worker_state,
            "queue_depth": queue_value,
            "failed_jobs": failed_value,
            "status_text": status_text,
        },
        "regions": {
            "complete": True,
            "overflow_count": 0,
            "items": regions,
        },
        "targets": {
            "complete": True,
            "overflow_count": 0,
            "items": targets,
        },
        "providers": {
            "complete": True,
            "overflow_count": 0,
            "items": [],
        },
        "ui_storms": {
            "complete": True,
            "overflow_count": 0,
            "items": [],
        },
        "map_artifact": {"artifact_handle": None, "etag": None},
    }
    return validate_snapshot(snapshot)


__all__ = [
    "METHOD",
    "OperationalSnapshotBuildError",
    "PENDING_REGION_ID",
    "UNKNOWN_SNAPSHOT_TTL_SECONDS",
    "build_unknown_operational_snapshot",
]
