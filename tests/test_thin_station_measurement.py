from __future__ import annotations

import pytest

from storm_tracker_worker.thin_observation import (
    NormalizedObservation,
    ObservationKind,
    ObservationQuality,
)
from storm_tracker_worker.thin_station_measurement import (
    StationMeasurement,
    deduplicate_station_measurements,
)


NOW = 1_800_000_000.0


def _observation(
    *,
    target_id: str = "home",
    value: float = 2.0,
    kind: ObservationKind = ObservationKind.RAIN_RATE,
    quality: ObservationQuality = ObservationQuality.GOOD,
    received_at: float = NOW + 1.0,
    metadata: dict[str, str] | None = None,
) -> NormalizedObservation:
    return NormalizedObservation.create(
        provider_id="netatmo",
        product_id="getpublicdata",
        target_id=target_id,
        kind=kind,
        value=value,
        unit="m/s" if kind is ObservationKind.WIND_SPEED else "mm/h",
        observed_at=NOW,
        received_at=received_at,
        latitude=51.05,
        longitude=4.42,
        quality=quality,
        metadata=(
            {"station_id": "station-1", "measurement": kind.value}
            if metadata is None
            else metadata
        ),
    )


def test_target_is_not_part_of_station_measurement_identity():
    first = _observation(target_id="home", value=2.0)
    second = _observation(target_id="child", value=2.0)

    rows = deduplicate_station_measurements((first, second))

    assert len(rows) == 1
    assert rows[0].identity == ("netatmo", "station-1", NOW, "rain_rate")


def test_wind_speed_and_gust_are_distinct_measurements():
    rows = deduplicate_station_measurements((
        _observation(
            kind=ObservationKind.WIND_SPEED,
            metadata={"station_id": "station-1", "measurement": "wind_speed"},
        ),
        _observation(
            kind=ObservationKind.WIND_SPEED,
            metadata={"station_id": "station-1", "measurement": "wind_gust"},
        ),
    ))

    assert {row.measurement_type for row in rows} == {"wind_speed", "wind_gust"}


def test_missing_station_id_stays_outside_station_route():
    assert StationMeasurement.from_observation(
        _observation(metadata={"measurement": "rain_rate"}),
    ) is None


def test_measurement_type_can_be_inferred_from_supported_observation_kind():
    row = StationMeasurement.from_observation(
        _observation(metadata={"station_id": "station-1"}),
    )

    assert row is not None
    assert row.measurement_type == "rain_rate"


@pytest.mark.parametrize(
    ("provider_label", "canonical"),
    (
        ("rain_60min", "rain_accumulation"),
        ("rain_total", "rain_accumulation"),
        ("rain_accumulation_10min", "rain_accumulation"),
        ("rain_rate_10min_equivalent", "rain_rate"),
        ("humidity", "relative_humidity"),
    ),
)
def test_provider_measurement_aliases_use_canonical_station_types(
    provider_label,
    canonical,
):
    kind = (
        ObservationKind.RELATIVE_HUMIDITY
        if canonical == "relative_humidity"
        else ObservationKind.RAIN_ACCUMULATION
        if canonical == "rain_accumulation"
        else ObservationKind.RAIN_RATE
    )
    unit = "%" if kind is ObservationKind.RELATIVE_HUMIDITY else (
        "mm" if kind is ObservationKind.RAIN_ACCUMULATION else "mm/h"
    )
    observation = NormalizedObservation.create(
        provider_id="netatmo",
        product_id="getpublicdata",
        target_id="home",
        kind=kind,
        value=2.0,
        unit=unit,
        observed_at=NOW,
        received_at=NOW + 1.0,
        latitude=51.05,
        longitude=4.42,
        quality=ObservationQuality.GOOD,
        metadata={"station_id": "station-1", "measurement": provider_label},
    )

    row = StationMeasurement.from_observation(observation)

    assert row is not None
    assert row.measurement_type == canonical


def test_unknown_measurement_type_stays_outside_station_route():
    observation = _observation(metadata={
        "station_id": "station-1",
        "measurement": "hail_probability",
    })

    assert StationMeasurement.from_observation(observation) is None


def test_best_quality_wins_even_when_received_earlier():
    good = _observation(
        value=2.0,
        quality=ObservationQuality.GOOD,
        received_at=NOW + 1.0,
    )
    suspect = _observation(
        target_id="child",
        value=8.0,
        quality=ObservationQuality.SUSPECT,
        received_at=NOW + 2.0,
    )

    assert deduplicate_station_measurements((good, suspect))[0].value == 2.0


def test_latest_received_wins_when_quality_is_equal():
    first = _observation(value=2.0, received_at=NOW + 1.0)
    latest = _observation(
        target_id="child",
        value=3.0,
        received_at=NOW + 2.0,
    )

    row = deduplicate_station_measurements((first, latest))[0]

    assert row.value == 3.0
    assert row.received_at == NOW + 2.0
