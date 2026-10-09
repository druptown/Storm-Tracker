"""Publish ground-validated deterministic worker nowcasts as target threats."""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import math
import re
from typing import Any, Iterable, Mapping

from .desired_state import DesiredState
from .multi_source_operational_cycle import MultiSourceOperationalCycle
from .multi_source_radar_snapshot import (
    PositiveRadarTargetObservation,
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


METHOD = "worker_ground_validated_sparse_nowcast_v1"
THREAT_REASON = "motion_nowcast_ground_validated"
MAX_THREAT_HORIZON_MINUTES = 90.0
MAX_GROUND_VALIDATION_AGE_SECONDS = 20.0 * 60.0
_SHA = re.compile(r"^[0-9a-f]{64}$")
_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,191}$")


class MultiSourceNowcastSnapshotError(ValueError):
    """A motion threat lacks exact current source or validation authority."""


def _sha(value: object, label: str) -> str:
    if not isinstance(value, str) or _SHA.fullmatch(value) is None:
        raise MultiSourceNowcastSnapshotError(f"{label} is geen SHA-256")
    return value


def _id(value: object, label: str) -> str:
    if not isinstance(value, str) or _ID.fullmatch(value) is None:
        raise MultiSourceNowcastSnapshotError(f"{label} is geen veilig ID")
    return value


def _number(
    value: object,
    label: str,
    *,
    minimum: float = 0.0,
    maximum: float | None = None,
) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise MultiSourceNowcastSnapshotError(f"{label} is geen getal")
    result = float(value)
    if not math.isfinite(result) or result < minimum or (
        maximum is not None and result > maximum
    ):
        raise MultiSourceNowcastSnapshotError(f"{label} ligt buiten de grens")
    return result


def _digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")).hexdigest()


@dataclass(frozen=True, slots=True)
class TargetNowcastThreat:
    target_id: str
    source_id: str
    track_id: str
    eta_minutes: float
    product_valid_at: float
    confidence: str
    peak_intensity: int
    source_receipt_sha256: str
    certificate_sha256: str
    process_receipt_sha256: str

    def __post_init__(self) -> None:
        _id(self.target_id, "target_id")
        _id(self.source_id, "source_id")
        _id(self.track_id, "track_id")
        _number(
            self.eta_minutes,
            "eta_minutes",
            maximum=MAX_THREAT_HORIZON_MINUTES,
        )
        _number(self.product_valid_at, "product_valid_at")
        if self.confidence not in {"medium", "high"}:
            raise MultiSourceNowcastSnapshotError("motion confidence is onvoldoende")
        if (
            isinstance(self.peak_intensity, bool)
            or not isinstance(self.peak_intensity, int)
            or not 1 <= self.peak_intensity <= 255
        ):
            raise MultiSourceNowcastSnapshotError("peak_intensity is ongeldig")
        _sha(self.source_receipt_sha256, "source_receipt_sha256")
        _sha(self.certificate_sha256, "certificate_sha256")
        _sha(self.process_receipt_sha256, "process_receipt_sha256")


@dataclass(frozen=True, slots=True)
class IndependentGroundValidation:
    track_id: str
    source_id: str
    observed_at: float
    wet: bool
    source_receipt_sha256: str
    certificate_sha256: str
    process_receipt_sha256: str

    def __post_init__(self) -> None:
        _id(self.track_id, "track_id")
        _id(self.source_id, "source_id")
        if not self.source_id.startswith("ground."):
            raise MultiSourceNowcastSnapshotError("validatiebron is geen grondbron")
        _number(self.observed_at, "observed_at")
        if type(self.wet) is not bool:
            raise MultiSourceNowcastSnapshotError("wet moet boolean zijn")
        _sha(self.source_receipt_sha256, "source_receipt_sha256")
        _sha(self.certificate_sha256, "certificate_sha256")
        _sha(self.process_receipt_sha256, "process_receipt_sha256")


def threats_from_deterministic_nowcast(
    nowcast: Mapping[str, Any],
    *,
    source_receipt_sha256: str,
    certificate_sha256: str,
    process_receipt_sha256: str,
) -> tuple[TargetNowcastThreat, ...]:
    """Project ready target hits from ``motion.deterministic_nowcast``."""
    if not isinstance(nowcast, Mapping) or nowcast.get("status") != "ready":
        return ()
    source_id = _id(nowcast.get("source"), "nowcast source")
    confidence = nowcast.get("confidence")
    if confidence not in {"medium", "high"}:
        return ()
    base_timestamp = int(_number(nowcast.get("base_timestamp"), "base_timestamp"))
    targets = nowcast.get("targets")
    if not isinstance(targets, Mapping) or len(targets) > 64:
        raise MultiSourceNowcastSnapshotError("nowcast targets zijn ongeldig")
    result = []
    for target_id, raw in sorted(targets.items()):
        if not isinstance(raw, Mapping) or raw.get("eta_minutes") is None:
            continue
        peak = raw.get("peak_intensity")
        first = raw.get("first_step_intensity")
        if (
            isinstance(peak, bool)
            or not isinstance(peak, int)
            or peak <= 0
            or isinstance(first, bool)
            or not isinstance(first, int)
            or first <= 0
        ):
            continue
        track_sha = _digest({
            "base_timestamp": base_timestamp,
            "method": nowcast.get("method"),
            "source_id": source_id,
            "target_id": target_id,
        })
        result.append(TargetNowcastThreat(
            target_id=str(target_id),
            source_id=source_id,
            track_id=f"nowcast:{track_sha}",
            eta_minutes=float(raw["eta_minutes"]),
            product_valid_at=float(base_timestamp),
            confidence=str(confidence),
            peak_intensity=peak,
            source_receipt_sha256=source_receipt_sha256,
            certificate_sha256=certificate_sha256,
            process_receipt_sha256=process_receipt_sha256,
        ))
    return tuple(result)


def _threat_receipt(
    desired: DesiredState,
    snapshot: dict[str, Any],
    threat_value: dict[str, Any],
    threat: TargetNowcastThreat,
    validation: IndependentGroundValidation,
) -> FactAdmissionReceipt:
    process_sha = _digest({
        "ground_process_receipt_sha256": validation.process_receipt_sha256,
        "method": METHOD,
        "nowcast_process_receipt_sha256": threat.process_receipt_sha256,
        "track_id": threat.track_id,
    })
    return FactAdmissionReceipt.create(
        fact_kind=OperationalFactKind.NEXT_THREAT_AVAILABLE,
        scope_id=threat.target_id,
        fact_value=threat_value,
        desired_state_sha256=desired.state_sha256,
        authority_epoch=desired.authority_epoch,
        source_receipt_sha256s=(
            threat.source_receipt_sha256,
            validation.source_receipt_sha256,
        ),
        certificate_sha256s=(
            threat.certificate_sha256,
            validation.certificate_sha256,
        ),
        process_receipt_sha256=process_sha,
        full_scan_complete=False,
        external_truth_validated=True,
        decided_at=float(snapshot["evaluated_at"]),
        valid_until=float(snapshot["valid_until"]),
        disposition=AdmissionDisposition.ADMITTED,
        reason_codes=(THREAT_REASON,),
    )


def build_ground_validated_nowcast_revision(
    desired_state: DesiredState,
    cycle: MultiSourceOperationalCycle,
    radar_observations: Iterable[PositiveRadarTargetObservation],
    threats: Iterable[TargetNowcastThreat],
    validations: Iterable[IndependentGroundValidation],
    *,
    revision: int,
    evaluated_at: float,
    worker_state: str = "online",
    queue_depth: int = 0,
    failed_jobs: int = 0,
) -> CommittedOperationalFactRevision:
    """Add only nowcast hits corroborated by fresh independent ground truth."""
    base = build_positive_radar_fact_revision(
        desired_state,
        cycle,
        radar_observations,
        revision=revision,
        evaluated_at=evaluated_at,
        worker_state=worker_state,
        queue_depth=queue_depth,
        failed_jobs=failed_jobs,
    )
    snapshot = validate_snapshot(base.snapshot)
    threat_rows = tuple(sorted(threats, key=lambda item: (item.target_id, item.track_id)))
    validation_rows = tuple(sorted(validations, key=lambda item: (item.track_id, item.source_id)))
    if any(not isinstance(item, TargetNowcastThreat) for item in threat_rows):
        raise MultiSourceNowcastSnapshotError("threats bevatten een fout type")
    if any(not isinstance(item, IndependentGroundValidation) for item in validation_rows):
        raise MultiSourceNowcastSnapshotError("validations bevatten een fout type")
    if len({item.target_id for item in threat_rows}) != len(threat_rows):
        raise MultiSourceNowcastSnapshotError("meer dan één threat per target")
    validation_by_track: dict[str, IndependentGroundValidation] = {}
    for item in validation_rows:
        if item.track_id in validation_by_track:
            raise MultiSourceNowcastSnapshotError("meer dan één validatie per track")
        validation_by_track[item.track_id] = item

    current_providers = {
        item["provider_id"]
        for item in snapshot["providers"]["items"]
        if item["freshness"] == "current"
    }
    targets = {item["target_id"]: item for item in snapshot["targets"]["items"]}
    admitted = list(base.admission_receipts)
    evaluated = float(snapshot["evaluated_at"])
    for threat in threat_rows:
        target = targets.get(threat.target_id)
        validation = validation_by_track.get(threat.track_id)
        if target is None or threat.source_id not in current_providers:
            raise MultiSourceNowcastSnapshotError("threat authority is niet actueel")
        if validation is None or not validation.wet:
            raise MultiSourceNowcastSnapshotError("threat mist nat grondbewijs")
        if validation.source_id not in current_providers:
            raise MultiSourceNowcastSnapshotError("grondvalidatiebron is niet actueel")
        if not 0.0 <= evaluated - validation.observed_at <= (
            MAX_GROUND_VALIDATION_AGE_SECONDS
        ):
            raise MultiSourceNowcastSnapshotError("grondvalidatie is niet vers")
        threat_value = target["next_threat"]
        threat_value.update({
            "state": "available",
            "freshness": "current",
            "track_id": threat.track_id,
            "calibrated_probability_percent": None,
            "probability_calibration_id": None,
            "eta_minutes": float(threat.eta_minutes),
            "distance_km": None,
            "passage_status": "possible",
            "product_valid_at": float(threat.product_valid_at),
            "provider_ids": [threat.source_id],
            "reason_codes": [THREAT_REASON],
        })
        if target["current_precipitation"]["state"] != "raining":
            target["status_text"] = (
                f"Neerslag mogelijk over {round(threat.eta_minutes):d} min"
            )
        admitted.append(_threat_receipt(
            desired_state,
            snapshot,
            threat_value,
            threat,
            validation,
        ))
    committed = CommittedOperationalFactRevision.create(
        desired_state_sha256=desired_state.state_sha256,
        authority_epoch=desired_state.authority_epoch,
        snapshot=validate_snapshot(snapshot),
        admission_receipts=admitted,
    )
    validate_committed_operational_revision(desired_state, committed)
    return committed


__all__ = [
    "IndependentGroundValidation",
    "MAX_GROUND_VALIDATION_AGE_SECONDS",
    "MAX_THREAT_HORIZON_MINUTES",
    "METHOD",
    "MultiSourceNowcastSnapshotError",
    "THREAT_REASON",
    "TargetNowcastThreat",
    "build_ground_validated_nowcast_revision",
    "threats_from_deterministic_nowcast",
]
