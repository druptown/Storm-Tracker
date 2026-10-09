"""Admit rain and qualified non-positive radar samples into the snapshot.

The bridge remains intentionally asymmetric: a positive quantitative radar
sample may support a current-raining fact.  An explicit certified zero sample
may only support the narrower statement that this radar did not observe
positive rain at the target; it never becomes a dry claim.  An in-domain
below-detection sample has the same narrow presentation but remains distinct
from an exact zero. Missing, nodata and outside-domain samples stay unknown.
Dry and no-threat claims still require wider independent authorities.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import math
import re
import statistics
from typing import Any, Iterable, Sequence

from .desired_state import DesiredState
from .multi_source_operational_cycle import MultiSourceOperationalCycle
from .multi_source_radar_facts import (
    RadarCoverageState,
    RadarTargetContributionBatch,
    RadarObservationState,
)
from .multi_source_snapshot import build_multi_source_status_snapshot
from .operational_fact_revision import (
    AdmissionDisposition,
    CommittedOperationalFactRevision,
    FactAdmissionReceipt,
    OperationalFactKind,
    validate_committed_operational_revision,
)
from .tracking_core.operational_snapshot import validate_snapshot


METHOD = "worker_positive_radar_fusion_v1"
RAIN_REASON = "positive_radar_candidates_fused"
REFLECTIVITY_ECHO_REASON = "certified_positive_reflectivity_echo_rain_rate_withheld"
ZERO_OBSERVATION_REASON = "certified_zero_radar_sample_dry_claim_withheld"
UNDETECT_OBSERVATION_REASON = (
    "certified_undetect_radar_sample_dry_claim_withheld"
)
THREAT_WITHHELD_REASON = "nowcast_motion_not_yet_admitted"
MAX_RAIN_RATE_MM_H = 1000.0
MAX_FUTURE_SKEW_SECONDS = 60.0
ALLOWED_POSITIVE_RADAR_SOURCE_IDS = frozenset({
    "radar.dwd.radolan_rv",
    "radar.knmi.rtcor",
    "radar.metoffice.uk_rate",
    "satellite.noaa.err",
})
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,191}$")


class MultiSourceRadarSnapshotError(ValueError):
    """Positive radar evidence is malformed, stale or authority-mismatched."""


def _sha(value: object, label: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise MultiSourceRadarSnapshotError(f"{label} is geen SHA-256")
    return value


def _identifier(value: object, label: str) -> str:
    if not isinstance(value, str) or _SAFE_ID.fullmatch(value) is None:
        raise MultiSourceRadarSnapshotError(f"{label} is geen veilig ID")
    return value


def _time(value: object, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise MultiSourceRadarSnapshotError(f"{label} is geen tijdstip")
    result = float(value)
    if not math.isfinite(result) or result < 0:
        raise MultiSourceRadarSnapshotError(f"{label} is geen eindig tijdstip")
    return result


def _canonical_sha(value: Any) -> str:
    return hashlib.sha256(json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")).hexdigest()


@dataclass(frozen=True, slots=True)
class PositiveRadarTargetObservation:
    """Minimal content-bound positive radar fact consumed by the fusion rule."""

    source_id: str
    target_id: str
    rain_rate_mm_h: float
    product_valid_at: float
    received_at: float
    source_receipt_sha256: str
    certificate_sha256: str
    process_receipt_sha256: str

    def __post_init__(self) -> None:
        _identifier(self.source_id, "source_id")
        _identifier(self.target_id, "target_id")
        if self.source_id not in ALLOWED_POSITIVE_RADAR_SOURCE_IDS:
            raise MultiSourceRadarSnapshotError(
                "source_id heeft geen gecertificeerde positieve radarsemantiek"
            )
        if (
            isinstance(self.rain_rate_mm_h, bool)
            or not isinstance(self.rain_rate_mm_h, (int, float))
            or not math.isfinite(float(self.rain_rate_mm_h))
            or not 0.0 < float(self.rain_rate_mm_h) <= MAX_RAIN_RATE_MM_H
        ):
            raise MultiSourceRadarSnapshotError("rain_rate_mm_h is ongeldig")
        valid_at = _time(self.product_valid_at, "product_valid_at")
        received_at = _time(self.received_at, "received_at")
        if received_at < valid_at:
            raise MultiSourceRadarSnapshotError("radartijden zijn omgekeerd")
        _sha(self.source_receipt_sha256, "source_receipt_sha256")
        _sha(self.certificate_sha256, "certificate_sha256")
        _sha(self.process_receipt_sha256, "process_receipt_sha256")

    def receipt_mapping(self) -> dict[str, Any]:
        return {
            "certificate_sha256": self.certificate_sha256,
            "process_receipt_sha256": self.process_receipt_sha256,
            "product_valid_at": float(self.product_valid_at),
            "rain_rate_mm_h": float(self.rain_rate_mm_h),
            "received_at": float(self.received_at),
            "source_id": self.source_id,
            "source_receipt_sha256": self.source_receipt_sha256,
            "target_id": self.target_id,
        }


@dataclass(frozen=True, slots=True)
class ReflectivityEchoTargetObservation:
    """Certified OPERA reflectivity echo without a rain-rate claim."""

    source_id: str
    target_id: str
    reflectivity_dbz: float
    quality_index: float
    product_valid_at: float
    received_at: float
    source_receipt_sha256: str
    certificate_sha256: str
    process_receipt_sha256: str

    def __post_init__(self) -> None:
        _identifier(self.source_id, "source_id")
        _identifier(self.target_id, "target_id")
        if self.source_id != "radar.opera.dbzh":
            raise MultiSourceRadarSnapshotError(
                "source_id heeft geen gecertificeerde reflectiviteitsemantiek"
            )
        if (
            isinstance(self.reflectivity_dbz, bool)
            or not isinstance(self.reflectivity_dbz, (int, float))
            or not math.isfinite(float(self.reflectivity_dbz))
            or not 8.0 <= float(self.reflectivity_dbz) <= 100.0
        ):
            raise MultiSourceRadarSnapshotError("reflectivity_dbz is ongeldig")
        if (
            isinstance(self.quality_index, bool)
            or not isinstance(self.quality_index, (int, float))
            or not math.isfinite(float(self.quality_index))
            or not 0.0 <= float(self.quality_index) <= 1.0
        ):
            raise MultiSourceRadarSnapshotError("quality_index is ongeldig")
        valid_at = _time(self.product_valid_at, "product_valid_at")
        received_at = _time(self.received_at, "received_at")
        if received_at < valid_at:
            raise MultiSourceRadarSnapshotError("radartijden zijn omgekeerd")
        _sha(self.source_receipt_sha256, "source_receipt_sha256")
        _sha(self.certificate_sha256, "certificate_sha256")
        _sha(self.process_receipt_sha256, "process_receipt_sha256")

    def receipt_mapping(self) -> dict[str, Any]:
        return {
            "certificate_sha256": self.certificate_sha256,
            "observation_state": "reflectivity_echo",
            "process_receipt_sha256": self.process_receipt_sha256,
            "product_valid_at": float(self.product_valid_at),
            "quality_index": float(self.quality_index),
            "received_at": float(self.received_at),
            "reflectivity_dbz": float(self.reflectivity_dbz),
            "source_id": self.source_id,
            "source_receipt_sha256": self.source_receipt_sha256,
            "target_id": self.target_id,
        }


@dataclass(frozen=True, slots=True)
class WithheldDryRadarTargetObservation:
    """Certified target pixel that is exactly zero but grants no dry claim."""

    source_id: str
    target_id: str
    rain_rate_mm_h: float
    product_valid_at: float
    received_at: float
    source_receipt_sha256: str
    certificate_sha256: str
    process_receipt_sha256: str

    def __post_init__(self) -> None:
        _identifier(self.source_id, "source_id")
        _identifier(self.target_id, "target_id")
        if self.source_id not in ALLOWED_POSITIVE_RADAR_SOURCE_IDS:
            raise MultiSourceRadarSnapshotError(
                "source_id heeft geen gecertificeerde radarsemantiek"
            )
        if (
            isinstance(self.rain_rate_mm_h, bool)
            or not isinstance(self.rain_rate_mm_h, (int, float))
            or not math.isfinite(float(self.rain_rate_mm_h))
            or float(self.rain_rate_mm_h) != 0.0
        ):
            raise MultiSourceRadarSnapshotError(
                "withheld dry rain_rate_mm_h moet exact nul zijn"
            )
        valid_at = _time(self.product_valid_at, "product_valid_at")
        received_at = _time(self.received_at, "received_at")
        if received_at < valid_at:
            raise MultiSourceRadarSnapshotError("radartijden zijn omgekeerd")
        _sha(self.source_receipt_sha256, "source_receipt_sha256")
        _sha(self.certificate_sha256, "certificate_sha256")
        _sha(self.process_receipt_sha256, "process_receipt_sha256")

    def receipt_mapping(self) -> dict[str, Any]:
        return {
            "certificate_sha256": self.certificate_sha256,
            "observation_state": "dry_withheld",
            "process_receipt_sha256": self.process_receipt_sha256,
            "product_valid_at": float(self.product_valid_at),
            "rain_rate_mm_h": 0.0,
            "received_at": float(self.received_at),
            "source_id": self.source_id,
            "source_receipt_sha256": self.source_receipt_sha256,
            "target_id": self.target_id,
        }


@dataclass(frozen=True, slots=True)
class WithheldUndetectRadarTargetObservation:
    """Certified in-domain pixel below detection, never promoted to dry."""

    source_id: str
    target_id: str
    rain_rate_mm_h: None
    product_valid_at: float
    received_at: float
    source_receipt_sha256: str
    certificate_sha256: str
    process_receipt_sha256: str

    def __post_init__(self) -> None:
        _identifier(self.source_id, "source_id")
        _identifier(self.target_id, "target_id")
        if self.source_id not in ALLOWED_POSITIVE_RADAR_SOURCE_IDS:
            raise MultiSourceRadarSnapshotError(
                "source_id heeft geen gecertificeerde radarsemantiek"
            )
        if self.rain_rate_mm_h is not None:
            raise MultiSourceRadarSnapshotError(
                "withheld undetect rain_rate_mm_h moet null zijn"
            )
        valid_at = _time(self.product_valid_at, "product_valid_at")
        received_at = _time(self.received_at, "received_at")
        if received_at < valid_at:
            raise MultiSourceRadarSnapshotError("radartijden zijn omgekeerd")
        _sha(self.source_receipt_sha256, "source_receipt_sha256")
        _sha(self.certificate_sha256, "certificate_sha256")
        _sha(self.process_receipt_sha256, "process_receipt_sha256")

    def receipt_mapping(self) -> dict[str, Any]:
        return {
            "certificate_sha256": self.certificate_sha256,
            "observation_state": "undetect_withheld",
            "process_receipt_sha256": self.process_receipt_sha256,
            "product_valid_at": float(self.product_valid_at),
            "rain_rate_mm_h": None,
            "received_at": float(self.received_at),
            "source_id": self.source_id,
            "source_receipt_sha256": self.source_receipt_sha256,
            "target_id": self.target_id,
        }


RadarTargetObservation = (
    PositiveRadarTargetObservation
    | ReflectivityEchoTargetObservation
    | WithheldDryRadarTargetObservation
    | WithheldUndetectRadarTargetObservation
)


def radar_observations_from_radar_batch(
    batch: RadarTargetContributionBatch,
) -> tuple[RadarTargetObservation, ...]:
    """Project positive and qualified non-positive radar observations."""
    if not isinstance(batch, RadarTargetContributionBatch):
        raise MultiSourceRadarSnapshotError("radar batch is niet gevalideerd")
    result: list[RadarTargetObservation] = []
    for contribution in batch.contributions:
        common = {
            "source_id": contribution.workload_id,
            "target_id": contribution.target_id,
            "product_valid_at": contribution.product_valid_at,
            "received_at": contribution.received_at,
            "source_receipt_sha256": contribution.canonical_receipt_sha256,
            "certificate_sha256": contribution.semantics_certificate_sha256,
            "process_receipt_sha256": contribution.process_evidence_sha256,
        }
        if (
            contribution.observation_state
            is RadarObservationState.POSITIVE_RAIN_CANDIDATE
        ):
            if (
                not contribution.current_rain_candidate
                or contribution.rain_rate_mm_h is None
            ):
                raise MultiSourceRadarSnapshotError(
                    "positieve radarbijdrage is inconsistent"
                )
            result.append(PositiveRadarTargetObservation(
                rain_rate_mm_h=contribution.rain_rate_mm_h,
                **common,
            ))
        elif (
            contribution.observation_state is RadarObservationState.DRY_WITHHELD
            and contribution.rain_rate_mm_h == 0.0
        ):
            result.append(WithheldDryRadarTargetObservation(
                rain_rate_mm_h=0.0,
                **common,
            ))
        elif (
            contribution.observation_state is RadarObservationState.DRY_WITHHELD
            and contribution.coverage_state is RadarCoverageState.IN_DOMAIN_UNDETECT
            and contribution.rain_rate_mm_h is None
        ):
            result.append(WithheldUndetectRadarTargetObservation(
                rain_rate_mm_h=None,
                **common,
            ))
    return tuple(sorted(result, key=lambda item: (item.target_id, item.source_id)))


def positive_observations_from_radar_batch(
    batch: RadarTargetContributionBatch,
) -> tuple[PositiveRadarTargetObservation, ...]:
    """Project only explicitly positive, certified batch contributions."""
    return tuple(
        item
        for item in radar_observations_from_radar_batch(batch)
        if isinstance(item, PositiveRadarTargetObservation)
    )


def _provider_receipts(
    desired: DesiredState,
    cycle: MultiSourceOperationalCycle,
    snapshot: dict[str, Any],
) -> list[FactAdmissionReceipt]:
    contribution_by_id = {item.source_id: item for item in cycle.contributions}
    valid_until = float(snapshot["valid_until"])
    decided_at = float(snapshot["evaluated_at"])
    receipts = []
    for provider in snapshot["providers"]["items"]:
        if provider["freshness"] != "current":
            continue
        contribution = contribution_by_id[provider["provider_id"]]
        source_receipts = tuple(
            evidence.receipt_sha256 for evidence in contribution.payload.evidence
        )
        if not source_receipts:
            raise MultiSourceRadarSnapshotError(
                f"actuele bron {contribution.source_id} mist evidencereceipt"
            )
        receipts.append(FactAdmissionReceipt.create(
            fact_kind=OperationalFactKind.PROVIDER_CURRENT,
            scope_id=provider["provider_id"],
            fact_value=provider,
            desired_state_sha256=desired.state_sha256,
            authority_epoch=desired.authority_epoch,
            source_receipt_sha256s=source_receipts,
            certificate_sha256s=(contribution.source_descriptor_sha256,),
            process_receipt_sha256=contribution.contribution_sha256,
            full_scan_complete=False,
            external_truth_validated=False,
            decided_at=decided_at,
            valid_until=valid_until,
            disposition=AdmissionDisposition.ADMITTED,
            reason_codes=("source_current",),
        ))
    return receipts


def _fusion_receipt_sha(
    observations: Sequence[RadarTargetObservation],
) -> str:
    return _canonical_sha({
        "inputs": [item.receipt_mapping() for item in observations],
        "method": METHOD,
    })


def _fact_receipt(
    desired: DesiredState,
    snapshot: dict[str, Any],
    *,
    fact_kind: OperationalFactKind,
    scope_id: str,
    fact_value: Any,
    observations: Sequence[RadarTargetObservation],
    reason_codes: Sequence[str] = (RAIN_REASON,),
) -> FactAdmissionReceipt:
    return FactAdmissionReceipt.create(
        fact_kind=fact_kind,
        scope_id=scope_id,
        fact_value=fact_value,
        desired_state_sha256=desired.state_sha256,
        authority_epoch=desired.authority_epoch,
        source_receipt_sha256s=(
            item.source_receipt_sha256 for item in observations
        ),
        certificate_sha256s=(
            item.certificate_sha256 for item in observations
        ),
        process_receipt_sha256=_fusion_receipt_sha(observations),
        full_scan_complete=False,
        external_truth_validated=False,
        decided_at=float(snapshot["evaluated_at"]),
        valid_until=float(snapshot["valid_until"]),
        disposition=AdmissionDisposition.ADMITTED,
        reason_codes=reason_codes,
    )


def build_positive_radar_fact_revision(
    desired_state: DesiredState,
    cycle: MultiSourceOperationalCycle,
    observations: Iterable[RadarTargetObservation],
    *,
    revision: int,
    evaluated_at: float,
    worker_state: str = "online",
    queue_depth: int = 0,
    failed_jobs: int = 0,
) -> CommittedOperationalFactRevision:
    """Fuse positive radar samples and return an exactly admitted revision."""
    if not isinstance(desired_state, DesiredState):
        raise MultiSourceRadarSnapshotError("desired state is niet gevalideerd")
    if not isinstance(cycle, MultiSourceOperationalCycle):
        raise MultiSourceRadarSnapshotError("cycle is niet gevalideerd")
    values = tuple(sorted(observations, key=lambda item: (item.target_id, item.source_id)))
    if any(
        not isinstance(
            item,
            (
                PositiveRadarTargetObservation,
                ReflectivityEchoTargetObservation,
                WithheldDryRadarTargetObservation,
                WithheldUndetectRadarTargetObservation,
            ),
        )
        for item in values
    ):
        raise MultiSourceRadarSnapshotError("radarobservaties bevatten een fout type")
    if len({(item.target_id, item.source_id) for item in values}) != len(values):
        raise MultiSourceRadarSnapshotError("radarobservaties zijn dubbel")

    snapshot = build_multi_source_status_snapshot(
        desired_state,
        cycle,
        revision=revision,
        evaluated_at=evaluated_at,
        worker_state=worker_state,
        queue_depth=queue_depth,
        failed_jobs=failed_jobs,
    )
    evaluated = float(snapshot["evaluated_at"])
    current_provider_ids = {
        item["provider_id"]
        for item in snapshot["providers"]["items"]
        if item["freshness"] == "current"
    }
    desired_by_id = {item.target_id: item for item in desired_state.targets}
    for item in values:
        target = desired_by_id.get(item.target_id)
        if target is None or not target.available:
            raise MultiSourceRadarSnapshotError(
                "radarobservatie verwijst naar onbekend/onbeschikbaar target"
            )
        if item.source_id not in current_provider_ids:
            raise MultiSourceRadarSnapshotError(
                "radarobservatie verwijst naar niet-actuele bron"
            )
        if item.product_valid_at > evaluated + MAX_FUTURE_SKEW_SECONDS:
            raise MultiSourceRadarSnapshotError("radarproduct ligt in de toekomst")

    by_target: dict[str, list[RadarTargetObservation]] = {}
    for item in values:
        by_target.setdefault(item.target_id, []).append(item)
    admitted: list[FactAdmissionReceipt] = _provider_receipts(
        desired_state,
        cycle,
        snapshot,
    )
    region_provider_ids: dict[str, set[str]] = {}
    target_region_ids = {
        item["target_id"]: item["region_id"]
        for item in snapshot["targets"]["items"]
    }
    for target in snapshot["targets"]["items"]:
        target_observations = by_target.get(target["target_id"], [])
        if not target_observations:
            target["next_threat"]["reason_codes"] = [THREAT_WITHHELD_REASON]
            continue
        positive_observations = [
            item
            for item in target_observations
            if isinstance(item, PositiveRadarTargetObservation)
        ]
        echo_observations = [
            item
            for item in target_observations
            if isinstance(item, ReflectivityEchoTargetObservation)
        ]
        zero_observations = [
            item
            for item in target_observations
            if isinstance(item, WithheldDryRadarTargetObservation)
        ]
        undetect_observations = [
            item
            for item in target_observations
            if isinstance(item, WithheldUndetectRadarTargetObservation)
        ]
        nonpositive_observations = zero_observations + undetect_observations
        admitted_observations = (
            positive_observations or echo_observations or nonpositive_observations
        )
        source_ids = sorted(item.source_id for item in admitted_observations)
        product_valid_at = max(
            float(item.product_valid_at) for item in admitted_observations
        )
        current = target["current_precipitation"]
        target["next_threat"]["reason_codes"] = [THREAT_WITHHELD_REASON]
        if positive_observations:
            rates = [
                float(item.rain_rate_mm_h) for item in positive_observations
            ]
            fused_rate = round(float(statistics.median(rates)), 3)
            current.update({
                "state": "raining",
                "freshness": "current",
                "rain_rate_mm_h": fused_rate,
                "product_valid_at": product_valid_at,
                "provider_ids": source_ids,
                "reason_codes": [RAIN_REASON],
            })
            target["status_text"] = f"Regen: {fused_rate:.1f} mm/u"
            admitted.append(_fact_receipt(
                desired_state,
                snapshot,
                fact_kind=OperationalFactKind.CURRENT_RAINING,
                scope_id=target["target_id"],
                fact_value=current,
                observations=positive_observations,
            ))
        elif echo_observations:
            strongest_echo = max(
                echo_observations,
                key=lambda item: (item.reflectivity_dbz, item.quality_index),
            )
            current.update({
                "state": "unknown",
                "freshness": "current",
                "rain_rate_mm_h": None,
                "product_valid_at": product_valid_at,
                "provider_ids": source_ids,
                "reason_codes": [REFLECTIVITY_ECHO_REASON],
            })
            target["status_text"] = (
                f"Radarecho: {strongest_echo.reflectivity_dbz:.1f} dBZ "
                "(regenintensiteit nog niet gekwantificeerd)"
            )
        else:
            reason = (
                ZERO_OBSERVATION_REASON
                if zero_observations
                else UNDETECT_OBSERVATION_REASON
            )
            current.update({
                "state": "unknown",
                "freshness": "current",
                "rain_rate_mm_h": None,
                "product_valid_at": product_valid_at,
                "provider_ids": source_ids,
                "reason_codes": [reason],
            })
            target["status_text"] = (
                (
                    "Geen positieve neerslag in actuele radar; "
                    "droogte niet onafhankelijk bevestigd"
                )
                if zero_observations
                else (
                    "Neerslag onder detectiedrempel in actuele radar; "
                    "droogte niet onafhankelijk bevestigd"
                )
            )
        region_provider_ids.setdefault(
            target_region_ids[target["target_id"]], set()
        ).update(source_ids)

    region_by_id = {
        item["region_id"]: item for item in snapshot["regions"]["items"]
    }
    for region_id, provider_ids in sorted(region_provider_ids.items()):
        region = region_by_id[region_id]
        region["state"] = "current"
        region["provider_ids"] = sorted(provider_ids)
        region_observations = tuple(
            item
            for item in values
            if target_region_ids[item.target_id] == region_id
        )
        admitted.append(_fact_receipt(
            desired_state,
            snapshot,
            fact_kind=OperationalFactKind.REGION_CURRENT,
            scope_id=region["region_id"],
            fact_value=region,
            observations=region_observations,
            reason_codes=(
                (RAIN_REASON,)
                if any(
                    isinstance(item, PositiveRadarTargetObservation)
                    for item in region_observations
                )
                else (
                    (REFLECTIVITY_ECHO_REASON,)
                    if any(
                        isinstance(item, ReflectivityEchoTargetObservation)
                        for item in region_observations
                    )
                    else (
                        (ZERO_OBSERVATION_REASON,)
                        if any(
                            isinstance(item, WithheldDryRadarTargetObservation)
                            for item in region_observations
                        )
                        else (UNDETECT_OBSERVATION_REASON,)
                    )
                )
            ),
        ))
    validated = validate_snapshot(snapshot)
    committed = CommittedOperationalFactRevision.create(
        desired_state_sha256=desired_state.state_sha256,
        authority_epoch=desired_state.authority_epoch,
        snapshot=validated,
        admission_receipts=admitted,
    )
    validate_committed_operational_revision(desired_state, committed)
    return committed


__all__ = [
    "ALLOWED_POSITIVE_RADAR_SOURCE_IDS",
    "METHOD",
    "MAX_RAIN_RATE_MM_H",
    "MultiSourceRadarSnapshotError",
    "PositiveRadarTargetObservation",
    "ReflectivityEchoTargetObservation",
    "REFLECTIVITY_ECHO_REASON",
    "RadarTargetObservation",
    "RAIN_REASON",
    "THREAT_WITHHELD_REASON",
    "WithheldDryRadarTargetObservation",
    "WithheldUndetectRadarTargetObservation",
    "UNDETECT_OBSERVATION_REASON",
    "ZERO_OBSERVATION_REASON",
    "build_positive_radar_fact_revision",
    "positive_observations_from_radar_batch",
    "radar_observations_from_radar_batch",
]
