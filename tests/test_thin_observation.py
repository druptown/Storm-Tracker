from __future__ import annotations

import pytest

from storm_tracker_worker.thin_observation import (
    NormalizedObservation,
    ObservationKind,
    ObservationQuality,
    ThinObservationError,
)


def _rain_observation(**overrides: object) -> NormalizedObservation:
    values: dict[str, object] = {
        "provider_id": "test",
        "product_id": "rain",
        "target_id": "home",
        "kind": ObservationKind.RAIN_RATE,
        "value": 2.0,
        "unit": "mm/h",
        "observed_at": 100.0,
        "received_at": 101.0,
        "latitude": 51.1,
        "longitude": 4.4,
        "quality": ObservationQuality.GOOD,
    }
    values.update(overrides)
    return NormalizedObservation.create(**values)


def test_normalized_rain_rate_converts_inches_to_mm_per_hour():
    observation = _rain_observation(value=1.0, unit="in/h")

    assert observation.value == 25.4
    assert observation.unit == "mm/h"


def test_observation_rejects_future_timestamp_before_inspecting_value():
    with pytest.raises(ThinObservationError, match="observed_at_invalid"):
        _rain_observation(
            value=float("nan"),
            observed_at=200.0,
            received_at=100.0,
        )


def test_observation_rejects_non_finite_meteorological_value():
    with pytest.raises(ThinObservationError, match="value_non_finite"):
        _rain_observation(value=float("nan"))


def test_observation_rejects_source_older_than_fifteen_minutes():
    with pytest.raises(ThinObservationError, match="observation_stale"):
        _rain_observation(observed_at=100.0, received_at=1000.01)


def test_as_dict_keeps_geometry_but_never_emits_sensitive_metadata():
    observation = _rain_observation(
        geometry={"type": "Point", "coordinates": [4.4, 51.1]},
        metadata={
            "source_url": "https://weather.invalid/frame",
            "revision": "2026-09-20T10:00Z",
            "authorization": "Bearer secret",
            "api_key": "secret",
        },
    )

    payload = observation.as_dict()

    assert payload["geometry"] == {
        "type": "Point",
        "coordinates": [4.4, 51.1],
    }
    assert payload["metadata"] == {
        "source_url": "https://weather.invalid/frame",
        "revision": "2026-09-20T10:00Z",
    }
    assert "secret" not in repr(payload)


def test_coordinates_are_validated_independently_of_provider():
    with pytest.raises(ThinObservationError, match="latitude_invalid"):
        _rain_observation(latitude=91.0)

