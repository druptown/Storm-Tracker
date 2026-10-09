"""Small provider-neutral observation contract for the thin runtime."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import math
from types import MappingProxyType
from typing import Any, Mapping


MAX_SOURCE_AGE_SECONDS = 15 * 60
MAX_GROUND_SOURCE_AGE_SECONDS = 2 * 60 * 60
MAX_FUTURE_SKEW_SECONDS = 30


class ThinObservationError(ValueError):
    """A provider value cannot enter the operational pipeline."""


class ObservationKind(str, Enum):
    RAIN_RATE = "rain_rate"
    RAIN_ACCUMULATION = "rain_accumulation"
    REFLECTIVITY = "reflectivity"
    RADAR_LEVEL = "radar_level"
    TEMPERATURE = "temperature"
    PRESSURE = "pressure"
    RELATIVE_HUMIDITY = "relative_humidity"
    WIND_SPEED = "wind_speed"
    WIND_DIRECTION = "wind_direction"
    LIGHTNING_STRIKE = "lightning_strike"
    RAIN_PROBABILITY = "rain_probability"


class ObservationQuality(str, Enum):
    GOOD = "good"
    SUSPECT = "suspect"
    POOR = "poor"
    UNKNOWN = "unknown"


_CANONICAL_UNITS: dict[ObservationKind, str] = {
    ObservationKind.RAIN_RATE: "mm/h",
    ObservationKind.RAIN_ACCUMULATION: "mm",
    ObservationKind.REFLECTIVITY: "dBZ",
    ObservationKind.RADAR_LEVEL: "level",
    ObservationKind.TEMPERATURE: "°C",
    ObservationKind.PRESSURE: "hPa",
    ObservationKind.RELATIVE_HUMIDITY: "%",
    ObservationKind.WIND_SPEED: "m/s",
    ObservationKind.WIND_DIRECTION: "°",
    ObservationKind.LIGHTNING_STRIKE: "count",
    ObservationKind.RAIN_PROBABILITY: "%",
}

_VALUE_RANGES: dict[ObservationKind, tuple[float, float]] = {
    ObservationKind.RAIN_RATE: (0.0, 1_000.0),
    ObservationKind.RAIN_ACCUMULATION: (0.0, 10_000.0),
    ObservationKind.REFLECTIVITY: (-100.0, 100.0),
    ObservationKind.RADAR_LEVEL: (0.0, 255.0),
    ObservationKind.TEMPERATURE: (-120.0, 80.0),
    ObservationKind.PRESSURE: (100.0, 1_200.0),
    ObservationKind.RELATIVE_HUMIDITY: (0.0, 100.0),
    ObservationKind.WIND_SPEED: (0.0, 200.0),
    ObservationKind.WIND_DIRECTION: (0.0, 360.0),
    ObservationKind.LIGHTNING_STRIKE: (0.0, 1_000_000.0),
    ObservationKind.RAIN_PROBABILITY: (0.0, 100.0),
}

_SENSITIVE_METADATA_PARTS = (
    "authorization",
    "api_key",
    "apikey",
    "client_secret",
    "password",
    "refresh_token",
    "access_token",
)


def _unit_key(unit: str) -> str:
    return unit.strip().lower().replace(" ", "")


def normalize_unit(
    kind: ObservationKind,
    value: float,
    unit: str,
) -> tuple[float, str]:
    """Convert a provider value to the canonical unit for its kind."""

    if not isinstance(kind, ObservationKind):
        raise ThinObservationError("kind_invalid")
    try:
        numeric = float(value)
    except (TypeError, ValueError) as error:
        raise ThinObservationError("value_invalid") from error
    if not math.isfinite(numeric):
        raise ThinObservationError("value_non_finite")
    if not isinstance(unit, str) or not unit.strip():
        raise ThinObservationError("unit_invalid")

    key = _unit_key(unit)
    if kind is ObservationKind.RAIN_RATE:
        if key in {"mm/h", "mm/hr", "mm/hour"}:
            converted = numeric
        elif key in {"in/h", "in/hr", "inch/h", "inches/h"}:
            converted = numeric * 25.4
        else:
            raise ThinObservationError("unit_unsupported")
    elif kind is ObservationKind.RAIN_ACCUMULATION:
        if key in {"mm", "millimeter", "millimetre"}:
            converted = numeric
        elif key in {"in", "inch", "inches"}:
            converted = numeric * 25.4
        else:
            raise ThinObservationError("unit_unsupported")
    elif kind is ObservationKind.TEMPERATURE:
        if key in {"°c", "c", "celsius"}:
            converted = numeric
        elif key in {"°f", "f", "fahrenheit"}:
            converted = (numeric - 32.0) * 5.0 / 9.0
        elif key in {"k", "kelvin"}:
            converted = numeric - 273.15
        else:
            raise ThinObservationError("unit_unsupported")
    elif kind is ObservationKind.PRESSURE:
        if key in {"hpa", "mbar"}:
            converted = numeric
        elif key == "pa":
            converted = numeric / 100.0
        else:
            raise ThinObservationError("unit_unsupported")
    elif kind is ObservationKind.WIND_SPEED:
        if key in {"m/s", "mps"}:
            converted = numeric
        elif key in {"km/h", "kph"}:
            converted = numeric / 3.6
        elif key in {"mph", "mi/h"}:
            converted = numeric * 0.44704
        else:
            raise ThinObservationError("unit_unsupported")
    else:
        aliases = {
            ObservationKind.REFLECTIVITY: {"dbz"},
            ObservationKind.RADAR_LEVEL: {"level"},
            ObservationKind.RELATIVE_HUMIDITY: {"%", "percent", "pct"},
            ObservationKind.WIND_DIRECTION: {"°", "deg", "degree", "degrees"},
            ObservationKind.LIGHTNING_STRIKE: {"count", "event", "events"},
            ObservationKind.RAIN_PROBABILITY: {"%", "percent", "pct"},
        }
        if key not in aliases[kind]:
            raise ThinObservationError("unit_unsupported")
        converted = numeric

    lower, upper = _VALUE_RANGES[kind]
    if converted < lower or converted > upper:
        raise ThinObservationError("value_out_of_range")
    return converted, _CANONICAL_UNITS[kind]


def _freeze_json(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({str(key): _freeze_json(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_json(item) for item in value)
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    raise ThinObservationError("metadata_not_json_compatible")


def _thaw_json(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _thaw_json(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw_json(item) for item in value]
    return value


def _safe_metadata(metadata: Mapping[str, Any] | None) -> Mapping[str, Any]:
    safe: dict[str, Any] = {}
    for key, value in (metadata or {}).items():
        normalized_key = str(key).lower()
        if any(part in normalized_key for part in _SENSITIVE_METADATA_PARTS):
            continue
        safe[str(key)] = _freeze_json(value)
    return MappingProxyType(safe)


@dataclass(frozen=True, slots=True)
class NormalizedObservation:
    provider_id: str
    product_id: str
    target_id: str | None
    kind: ObservationKind
    value: float
    unit: str
    observed_at: float
    received_at: float
    latitude: float
    longitude: float
    quality: ObservationQuality
    geometry: Mapping[str, Any] | None
    metadata: Mapping[str, Any]

    @classmethod
    def create(
        cls,
        *,
        provider_id: str,
        product_id: str,
        target_id: str | None,
        kind: ObservationKind,
        value: float,
        unit: str,
        observed_at: float,
        received_at: float,
        latitude: float,
        longitude: float,
        quality: ObservationQuality,
        geometry: Mapping[str, Any] | None = None,
        metadata: Mapping[str, Any] | None = None,
        maximum_age_seconds: float = MAX_SOURCE_AGE_SECONDS,
    ) -> "NormalizedObservation":
        if not isinstance(provider_id, str) or not provider_id.strip():
            raise ThinObservationError("provider_id_invalid")
        if not isinstance(product_id, str) or not product_id.strip():
            raise ThinObservationError("product_id_invalid")
        if target_id is not None and (not isinstance(target_id, str) or not target_id.strip()):
            raise ThinObservationError("target_id_invalid")
        if not isinstance(kind, ObservationKind):
            raise ThinObservationError("kind_invalid")
        if not isinstance(quality, ObservationQuality):
            raise ThinObservationError("quality_invalid")

        try:
            observed = float(observed_at)
            received = float(received_at)
            lat = float(latitude)
            lon = float(longitude)
            maximum_age = float(maximum_age_seconds)
        except (TypeError, ValueError) as error:
            raise ThinObservationError("coordinate_or_time_invalid") from error
        if not math.isfinite(observed) or not math.isfinite(received):
            raise ThinObservationError("observed_at_invalid")
        if observed > received + MAX_FUTURE_SKEW_SECONDS:
            raise ThinObservationError("observed_at_invalid")
        if not math.isfinite(maximum_age) or maximum_age < 0.0:
            raise ThinObservationError("maximum_age_invalid")
        if received - observed > maximum_age:
            raise ThinObservationError("observation_stale")
        if not math.isfinite(lat) or lat < -90.0 or lat > 90.0:
            raise ThinObservationError("latitude_invalid")
        if not math.isfinite(lon) or lon < -180.0 or lon > 180.0:
            raise ThinObservationError("longitude_invalid")

        normalized_value, normalized_unit = normalize_unit(kind, value, unit)
        frozen_geometry = None if geometry is None else _freeze_json(geometry)
        if frozen_geometry is not None and not isinstance(frozen_geometry, Mapping):
            raise ThinObservationError("geometry_invalid")

        return cls(
            provider_id=provider_id.strip(),
            product_id=product_id.strip(),
            target_id=None if target_id is None else target_id.strip(),
            kind=kind,
            value=normalized_value,
            unit=normalized_unit,
            observed_at=observed,
            received_at=received,
            latitude=lat,
            longitude=lon,
            quality=quality,
            geometry=frozen_geometry,
            metadata=_safe_metadata(metadata),
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "provider_id": self.provider_id,
            "product_id": self.product_id,
            "target_id": self.target_id,
            "kind": self.kind.value,
            "value": self.value,
            "unit": self.unit,
            "observed_at": self.observed_at,
            "received_at": self.received_at,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "quality": self.quality.value,
            "geometry": None if self.geometry is None else _thaw_json(self.geometry),
            "metadata": _thaw_json(self.metadata),
        }


__all__ = [
    "MAX_SOURCE_AGE_SECONDS",
    "MAX_GROUND_SOURCE_AGE_SECONDS",
    "NormalizedObservation",
    "ObservationKind",
    "ObservationQuality",
    "ThinObservationError",
    "normalize_unit",
]
