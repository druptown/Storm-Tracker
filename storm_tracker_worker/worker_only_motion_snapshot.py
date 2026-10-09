"""Promote certified radar-track intersections to possible target threats.

The motion tracker deliberately emits shadow candidates first.  This module is
the narrow admission boundary: it binds one candidate to the exact current
radar observation and only publishes ``possible`` with no calibrated
probability.  Stronger ``expected`` and negative ``none`` claims remain behind
their existing independent-validation gates.
"""
from __future__ import annotations

import hashlib
import json
from typing import Iterable

from .desired_state import DesiredState
from .multi_source_operational_cycle import MultiSourceOperationalCycle
from .multi_source_radar_snapshot import (
    RadarTargetObservation,
    build_positive_radar_fact_revision,
)
from .operational_fact_revision import (
    AdmissionDisposition,
    CommittedOperationalFactRevision,
    FactAdmissionReceipt,
    OperationalFactKind,
    validate_committed_operational_revision,
)
from .tracking_core.operational_snapshot import validate_snapshot
from .worker_only_cell_tracking import (
    ADMISSION_STATUS,
    SHADOW_ARRIVAL_HORIZON_SECONDS,
    WorkerOnlyShadowArrivalCandidate,
)


MOTION_THREAT_REASON = "certified_radar_track_possible"
MAX_MOTION_SOURCE_AGE_SECONDS = 20.0 * 60.0


class WorkerOnlyMotionSnapshotError(ValueError):
    """A radar motion candidate cannot be bound to current exact evidence."""


def _digest(value: object) -> str:
    return hashlib.sha256(json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")).hexdigest()


def _candidate_process_sha(
    candidate: WorkerOnlyShadowArrivalCandidate,
    observation: RadarTargetObservation,
) -> str:
    return _digest({
        "admission_boundary": "worker_only_radar_motion_possible_v1",
        "arrival": {
            "candidate_id": candidate.candidate_id,
            "detection_id": candidate.detection_id,
            "entry_seconds": candidate.entry_seconds,
            "exit_seconds": candidate.exit_seconds,
            "method": candidate.method,
            "reference_valid_at": candidate.reference_valid_at,
            "source_candidate_content_sha256": (
                candidate.source_candidate_content_sha256
            ),
            "target_id": candidate.target_id,
            "track_id": candidate.track_id,
            "vx_m_s": candidate.vx_m_s,
            "vy_m_s": candidate.vy_m_s,
            "workload_id": candidate.workload_id,
        },
        "radar_observation_process_receipt_sha256": (
            observation.process_receipt_sha256
        ),
    })


def build_radar_motion_nowcast_revision(
    desired_state: DesiredState,
    cycle: MultiSourceOperationalCycle,
    radar_observations: Iterable[RadarTargetObservation],
    arrivals: Iterable[WorkerOnlyShadowArrivalCandidate],
    *,
    revision: int,
    evaluated_at: float,
    worker_state: str = "online",
    queue_depth: int = 0,
    failed_jobs: int = 0,
) -> CommittedOperationalFactRevision:
    """Publish the earliest current certified radar intersection per target."""

    observations = tuple(sorted(
        radar_observations,
        key=lambda item: (item.target_id, item.source_id),
    ))
    base = build_positive_radar_fact_revision(
        desired_state,
        cycle,
        observations,
        revision=revision,
        evaluated_at=evaluated_at,
        worker_state=worker_state,
        queue_depth=queue_depth,
        failed_jobs=failed_jobs,
    )
    snapshot = validate_snapshot(base.snapshot)
    current_providers = {
        item["provider_id"]
        for item in snapshot["providers"]["items"]
        if item["freshness"] == "current"
    }
    available_targets = {
        item.target_id
        for item in desired_state.targets
        if item.available
    }
    observation_by_key = {
        (item.target_id, item.source_id): item
        for item in observations
    }
    selected: dict[
        str,
        tuple[WorkerOnlyShadowArrivalCandidate, RadarTargetObservation],
    ] = {}
    evaluated = float(snapshot["evaluated_at"])
    for candidate in sorted(
        arrivals,
        key=lambda item: (item.target_id, item.entry_seconds, item.track_id),
    ):
        if not isinstance(candidate, WorkerOnlyShadowArrivalCandidate):
            raise WorkerOnlyMotionSnapshotError(
                "motion_arrivals_contain_invalid_type"
            )
        if candidate.admission_status != ADMISSION_STATUS:
            raise WorkerOnlyMotionSnapshotError(
                "motion_arrival_shadow_contract_changed"
            )
        if candidate.low_confidence_fragment:
            continue
        if candidate.target_id not in available_targets:
            raise WorkerOnlyMotionSnapshotError(
                "motion_arrival_target_not_available"
            )
        if candidate.workload_id not in current_providers:
            continue
        observation = observation_by_key.get(
            (candidate.target_id, candidate.workload_id)
        )
        if observation is None:
            continue
        if candidate.reference_valid_at != observation.product_valid_at:
            continue
        source_age = evaluated - candidate.reference_valid_at
        if not 0.0 <= source_age <= MAX_MOTION_SOURCE_AGE_SECONDS:
            continue
        if (
            candidate.horizon_seconds != SHADOW_ARRIVAL_HORIZON_SECONDS
            or candidate.entry_seconds > SHADOW_ARRIVAL_HORIZON_SECONDS
        ):
            raise WorkerOnlyMotionSnapshotError(
                "motion_arrival_horizon_invalid"
            )
        selected.setdefault(candidate.target_id, (candidate, observation))

    targets = {
        item["target_id"]: item
        for item in snapshot["targets"]["items"]
    }
    receipts = list(base.admission_receipts)
    for target_id, (candidate, observation) in sorted(selected.items()):
        target = targets[target_id]
        eta_minutes = round(candidate.entry_seconds / 60.0, 1)
        threat = target["next_threat"]
        threat.update({
            "state": "available",
            "freshness": "current",
            "track_id": candidate.track_id,
            "calibrated_probability_percent": None,
            "probability_calibration_id": None,
            "eta_minutes": eta_minutes,
            "distance_km": None,
            "passage_status": "possible",
            "product_valid_at": candidate.reference_valid_at,
            "provider_ids": [candidate.workload_id],
            "reason_codes": [MOTION_THREAT_REASON],
        })
        if target["current_precipitation"]["state"] != "raining":
            target["status_text"] = (
                f"Neerslag mogelijk over {round(eta_minutes):d} min"
            )
        receipts.append(FactAdmissionReceipt.create(
            fact_kind=OperationalFactKind.NEXT_THREAT_AVAILABLE,
            scope_id=target_id,
            fact_value=threat,
            desired_state_sha256=desired_state.state_sha256,
            authority_epoch=desired_state.authority_epoch,
            source_receipt_sha256s=(observation.source_receipt_sha256,),
            certificate_sha256s=(observation.certificate_sha256,),
            process_receipt_sha256=_candidate_process_sha(
                candidate,
                observation,
            ),
            full_scan_complete=False,
            external_truth_validated=False,
            decided_at=evaluated,
            valid_until=float(snapshot["valid_until"]),
            disposition=AdmissionDisposition.ADMITTED,
            reason_codes=(MOTION_THREAT_REASON,),
        ))

    committed = CommittedOperationalFactRevision.create(
        desired_state_sha256=desired_state.state_sha256,
        authority_epoch=desired_state.authority_epoch,
        snapshot=validate_snapshot(snapshot),
        admission_receipts=receipts,
    )
    validate_committed_operational_revision(desired_state, committed)
    return committed


__all__ = [
    "MAX_MOTION_SOURCE_AGE_SECONDS",
    "MOTION_THREAT_REASON",
    "WorkerOnlyMotionSnapshotError",
    "build_radar_motion_nowcast_revision",
]
