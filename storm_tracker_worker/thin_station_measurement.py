"""Canonical, target-independent measurements from physical ground stations."""

from __future__ import annotations

from dataclasses import dataclass
import math
from types import MappingProxyType
from typing import Any, Iterable, Mapping

from .thin_observation import (
    NormalizedObservation,
    ObservationKind,
    ObservationQuality,
)
from .thin_station_catalog import normalize_capabilities


_QUALITY_RANK = {
    ObservationQuality.UNKNOWN: 0,
    ObservationQuality.POOR: 1,
    ObservationQuality.SUSPECT: 2,
    ObservationQuality.GOOD: 3,
}

_MEASUREMENT_ALIASES = {
    "humidity": "relative_humidity",
    "rain_60min": "rain_accumulation",
    "rain_total": "rain_accumulation",
    "rain_accumulation_10min": "rain_accumulation",
    "rain_rate_10min_equivalent": "rain_rate",
}


def normalize_station_measurement_type(
    value: object,
    *,
    fallback_kind: ObservationKind | None = None,
) -> str:
    """Map provider labels to the small canonical station vocabulary."""

    raw = str(value or "").strip().lower()
    if raw:
        candidate = _MEASUREMENT_ALIASES.get(raw, raw)
    elif fallback_kind is not None:
        candidate = fallback_kind.value
    else:
        raise ValueError("station_measurement_type_missing")
    return normalize_capabilities((candidate,))[0]


@dataclass(frozen=True, slots=True)
class StationMeasurement:
    """One physical measurement, independent from targets and regions."""

    provider_id: str
    station_id: str
    product_id: str
    measurement_type: str
    value: float
    unit: str
    observed_at: float
    received_at: float
    quality: ObservationQuality
    metadata: Mapping[str, Any]

    @property
    def identity(self) -> tuple[str, str, float, str]:
        return (
            self.provider_id,
            self.station_id,
            self.observed_at,
            self.measurement_type,
        )

    @classmethod
    def from_observation(
        cls,
        observation: NormalizedObservation,
    ) -> "StationMeasurement | None":
        station_id = str(observation.metadata.get("station_id", "")).strip()
        if (
            not station_id
            or len(station_id) > 191
            or any(ord(character) < 32 for character in station_id)
        ):
            return None
        try:
            normalized_type = normalize_station_measurement_type(
                observation.metadata.get("measurement"),
                fallback_kind=observation.kind,
            )
        except (ValueError, IndexError):
            return None
        if not (
            math.isfinite(observation.observed_at)
            and math.isfinite(observation.received_at)
        ):
            return None
        return cls(
            provider_id=observation.provider_id,
            station_id=station_id,
            product_id=observation.product_id,
            measurement_type=normalized_type,
            value=observation.value,
            unit=observation.unit,
            observed_at=observation.observed_at,
            received_at=observation.received_at,
            quality=observation.quality,
            metadata=MappingProxyType(dict(observation.metadata)),
        )


def _preference(measurement: StationMeasurement) -> tuple[int, float, str, float, str]:
    return (
        _QUALITY_RANK[measurement.quality],
        measurement.received_at,
        measurement.product_id,
        measurement.value,
        measurement.unit,
    )


def deduplicate_station_measurements(
    observations: Iterable[NormalizedObservation],
) -> tuple[StationMeasurement, ...]:
    """Return one deterministic best measurement per physical identity."""

    selected: dict[tuple[str, str, float, str], StationMeasurement] = {}
    for observation in observations:
        measurement = StationMeasurement.from_observation(observation)
        if measurement is None:
            continue
        previous = selected.get(measurement.identity)
        if previous is None or _preference(measurement) > _preference(previous):
            selected[measurement.identity] = measurement
    return tuple(selected[identity] for identity in sorted(selected))


__all__ = [
    "StationMeasurement",
    "deduplicate_station_measurements",
    "normalize_station_measurement_type",
]
