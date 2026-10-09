"""Deterministic quality control and fusion for normalized observations."""

from __future__ import annotations

from dataclasses import dataclass
import math
from types import MappingProxyType
from typing import Mapping

from .thin_observation import (
    MAX_GROUND_SOURCE_AGE_SECONDS,
    NormalizedObservation,
    ObservationKind,
    ObservationQuality,
)
from .thin_provider_runtime import ProviderResult


MAX_FUSION_AGE_SECONDS = 15 * 60
PROVIDER_FUSION_AGE_SECONDS = {
    # The public AEMET composite is officially published in batches and can
    # legitimately trail wall time by more than the generic radar window.
    "aemet_radar": 45 * 60,
    "meteofrance_radar": 25 * 60,
    "dpc_radar": 20 * 60,
    "kmi_appv4": 20 * 60,
    "metoffice_radar": 20 * 60,
    "rainviewer": 20 * 60,
    "geosphere": 90 * 60,
}
MAX_FUTURE_SKEW_SECONDS = 30
_GROUND_KINDS = frozenset({
    ObservationKind.TEMPERATURE,
    ObservationKind.PRESSURE,
    ObservationKind.RELATIVE_HUMIDITY,
    ObservationKind.WIND_SPEED,
    ObservationKind.WIND_DIRECTION,
})

_QUALITY_WEIGHT = {
    ObservationQuality.GOOD: 1.0,
    ObservationQuality.SUSPECT: 0.6,
    ObservationQuality.POOR: 0.25,
    ObservationQuality.UNKNOWN: 0.4,
}


@dataclass(frozen=True, slots=True)
class FusedTargetObservation:
    target_id: str
    status: str
    evaluated_at: float
    precipitation_rate_mm_h: float | None
    reflectivity_dbz: float | None
    radar_level: float | None
    rain_probability_percent: float | None
    pressure_hpa: float | None
    temperature_c: float | None
    lightning_count: int
    confidence: float
    contributors: tuple[str, ...]
    source_observed_at: Mapping[str, float]

    def as_dict(self) -> dict:
        return {
            "target_id": self.target_id,
            "status": self.status,
            "evaluated_at": self.evaluated_at,
            "precipitation_rate_mm_h": self.precipitation_rate_mm_h,
            "reflectivity_dbz": self.reflectivity_dbz,
            "radar_level": self.radar_level,
            "rain_probability_percent": self.rain_probability_percent,
            "pressure_hpa": self.pressure_hpa,
            "temperature_c": self.temperature_c,
            "lightning_count": self.lightning_count,
            "confidence": self.confidence,
            "contributors": list(self.contributors),
            "source_observed_at": dict(self.source_observed_at),
        }


@dataclass(frozen=True, slots=True)
class FusionCycle:
    evaluated_at: float
    targets: Mapping[str, FusedTargetObservation]
    providers: Mapping[str, ProviderResult]
    rejected: Mapping[str, tuple[str, ...]]
    observations: tuple[NormalizedObservation, ...]


def _fused_value(values: list[NormalizedObservation]) -> float | None:
    if not values:
        return None
    total_weight = sum(_QUALITY_WEIGHT[item.quality] for item in values)
    if total_weight <= 0.0:
        return None
    return sum(item.value * _QUALITY_WEIGHT[item.quality] for item in values) / total_weight


def _current_precipitation_values(
    values: list[NormalizedObservation],
) -> list[NormalizedObservation]:
    """Keep forecast guidance and unusable radar quality out of current weather."""
    eligible: list[NormalizedObservation] = []
    for item in values:
        if item.metadata.get("role") == "model_guidance":
            continue
        quality_index = item.metadata.get("quality_index")
        if (
            item.kind in {ObservationKind.REFLECTIVITY, ObservationKind.RADAR_LEVEL}
            and isinstance(quality_index, (int, float))
            and not isinstance(quality_index, bool)
            and (not math.isfinite(float(quality_index)) or float(quality_index) <= 0.0)
        ):
            continue
        eligible.append(item)
    return eligible


def _remove_disagreement_outliers(
    values: list[NormalizedObservation],
) -> tuple[list[NormalizedObservation], list[NormalizedObservation]]:
    if len(values) < 3:
        return values, []
    ordered = sorted(item.value for item in values)
    median = ordered[len(ordered) // 2]
    tolerance = max(5.0, max(1.0, abs(median)) * 3.0)
    accepted = [item for item in values if abs(item.value - median) <= tolerance]
    rejected = [item for item in values if item not in accepted]
    return accepted, rejected


def fuse_observations(
    provider_results: Mapping[str, ProviderResult],
    *,
    evaluated_at: float,
    target_ids: tuple[str, ...] = (),
) -> FusionCycle:
    evaluated = float(evaluated_at)
    providers: dict[str, ProviderResult] = {}
    rejected: dict[str, set[str]] = {}
    accepted: list[NormalizedObservation] = []

    for provider_id, candidate in sorted(provider_results.items()):
        if not isinstance(candidate, ProviderResult):
            candidate = ProviderResult(
                provider_id=str(provider_id),
                ready=False,
                status="error",
                reason_code="invalid_provider_result",
                observations=(),
                attempted_at=evaluated,
                completed_at=evaluated,
            )
        providers[str(provider_id)] = candidate
        for observation in candidate.observations:
            age = evaluated - observation.observed_at
            maximum_age = (
                MAX_GROUND_SOURCE_AGE_SECONDS
                if observation.kind in _GROUND_KINDS
                else PROVIDER_FUSION_AGE_SECONDS.get(
                    observation.provider_id,
                    MAX_FUSION_AGE_SECONDS,
                )
            )
            if age > maximum_age:
                rejected.setdefault(str(provider_id), set()).add("stale")
                continue
            if age < -MAX_FUTURE_SKEW_SECONDS:
                rejected.setdefault(str(provider_id), set()).add("future")
                continue
            if observation.provider_id != candidate.provider_id:
                rejected.setdefault(str(provider_id), set()).add("provider_mismatch")
                continue
            if (
                observation.target_id is not None
                and observation.metadata.get("operational_use")
                == "visualisation_only"
            ):
                rejected.setdefault(str(provider_id), set()).add(
                    "visualisation_only"
                )
                continue
            accepted.append(observation)

    grouped: dict[tuple[str, ObservationKind], list[NormalizedObservation]] = {}
    known_targets = set(target_ids)
    for observation in accepted:
        if observation.target_id is None:
            continue
        known_targets.add(observation.target_id)
        grouped.setdefault((observation.target_id, observation.kind), []).append(observation)

    filtered: dict[tuple[str, ObservationKind], list[NormalizedObservation]] = {}
    for key, values in grouped.items():
        retained, outliers = _remove_disagreement_outliers(values)
        filtered[key] = retained
        for outlier in outliers:
            rejected.setdefault(outlier.provider_id, set()).add("disagreement_outlier")

    accepted_set = {
        id(item)
        for values in filtered.values()
        for item in values
    }
    accepted = [
        item
        for item in accepted
        if item.target_id is None or id(item) in accepted_set
    ]

    targets: dict[str, FusedTargetObservation] = {}
    for target_id in sorted(known_targets):
        by_kind = {
            kind: filtered.get((target_id, kind), [])
            for kind in ObservationKind
        }
        target_observations = [
            item for values in by_kind.values() for item in values
        ]
        contributors = tuple(sorted({item.provider_id for item in target_observations}))
        confidence = (
            sum(_QUALITY_WEIGHT[item.quality] for item in target_observations)
            / len(target_observations)
            if target_observations
            else 0.0
        )
        source_times: dict[str, float] = {}
        for item in target_observations:
            source_times[item.provider_id] = max(
                source_times.get(item.provider_id, 0.0),
                item.observed_at,
            )
        lightning = by_kind[ObservationKind.LIGHTNING_STRIKE]
        targets[target_id] = FusedTargetObservation(
            target_id=target_id,
            status="operational" if target_observations else "onvoldoende_data",
            evaluated_at=evaluated,
            precipitation_rate_mm_h=_fused_value(_current_precipitation_values(
                by_kind[ObservationKind.RAIN_RATE]
            )),
            reflectivity_dbz=_fused_value(_current_precipitation_values(
                by_kind[ObservationKind.REFLECTIVITY]
            )),
            radar_level=_fused_value(_current_precipitation_values(
                by_kind[ObservationKind.RADAR_LEVEL]
            )),
            rain_probability_percent=_fused_value(
                by_kind[ObservationKind.RAIN_PROBABILITY]
            ),
            pressure_hpa=_fused_value(by_kind[ObservationKind.PRESSURE]),
            temperature_c=_fused_value(by_kind[ObservationKind.TEMPERATURE]),
            lightning_count=int(sum(item.value for item in lightning)),
            confidence=confidence,
            contributors=contributors,
            source_observed_at=MappingProxyType(source_times),
        )

    return FusionCycle(
        evaluated_at=evaluated,
        targets=MappingProxyType(targets),
        providers=MappingProxyType(providers),
        rejected=MappingProxyType({
            provider_id: tuple(sorted(reasons))
            for provider_id, reasons in sorted(rejected.items())
        }),
        observations=tuple(accepted),
    )


__all__ = ["FusedTargetObservation", "FusionCycle", "fuse_observations"]
