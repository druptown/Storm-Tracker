"""Tests voor robuuste regionale Netatmo-luchtdruktrends."""
from types import SimpleNamespace

import pytest


def _observations(timestamp, pressures):
    return [
        SimpleNamespace(station_id=station_id, pressure=pressure, timestamp=timestamp)
        for station_id, pressure in pressures.items()
    ]


def test_pressure_trend_uses_station_deltas_not_absolute_levels(pressure_trend_module):
    tracker = pressure_trend_module.PressureTrendTracker()
    base = 1_700_000_000.0
    baselines = {"low": 980.0, "mid": 1010.0, "high": 1040.0}

    result = None
    for step in range(13):
        timestamp = base + step * 300
        pressures = {
            station_id: baseline - step * drop_per_step
            for (station_id, baseline), drop_per_step in zip(
                baselines.items(), (0.20, 0.25, 0.30)
            )
        }
        result = tracker.update(_observations(timestamp, pressures), timestamp)

    assert result["median_pressure_hpa"] == pytest.approx(1007.0)
    assert result["delta_15m_hpa"] == pytest.approx(-0.75)
    assert result["delta_30m_hpa"] == pytest.approx(-1.5)
    assert result["delta_60m_hpa"] == pytest.approx(-3.0)
    assert result["stations_60m"] == 3
    assert result["trend"] == "snelle_daling"
    assert result["rapid_fall"] is True


def test_pressure_trend_requires_three_paired_stations(pressure_trend_module):
    tracker = pressure_trend_module.PressureTrendTracker()
    base = 1_700_000_000.0
    result = None
    for step in range(13):
        timestamp = base + step * 300
        result = tracker.update(
            _observations(
                timestamp,
                {"a": 1010.0 - step / 6, "b": 1012.0 - step / 6},
            ),
            timestamp,
        )

    assert result["stations_60m"] == 2
    assert result["delta_60m_hpa"] is None
    assert result["trend"] == "onvoldoende_data"


def test_cold_start_never_invents_pressure_change(pressure_trend_module):
    tracker = pressure_trend_module.PressureTrendTracker()
    result = tracker.update([], timestamp=1_000.0)

    assert result["trend"] == "onvoldoende_data"
    assert result["rapid_fall"] is False
    assert result["median_pressure_hpa"] is None
    assert result["delta_15m_hpa"] is None
    assert result["delta_30m_hpa"] is None
    assert result["delta_60m_hpa"] is None


def test_pressure_history_survives_snapshot_restore(pressure_trend_module):
    base = 1_700_000_000.0
    original = pressure_trend_module.PressureTrendTracker()
    for step in range(7):
        timestamp = base + step * 300
        original.update(
            _observations(
                timestamp,
                {"a": 1010.0, "b": 1011.0, "c": 1012.0},
            ),
            timestamp,
        )

    restored = pressure_trend_module.PressureTrendTracker()
    assert restored.restore(original.to_snapshot(), base + 1800) == 3
    result = None
    for step in range(7, 13):
        timestamp = base + step * 300
        fraction = (step - 6) / 6
        result = restored.update(
            _observations(
                timestamp,
                {
                    "a": 1010.0 - 2.0 * fraction,
                    "b": 1011.0 - 2.0 * fraction,
                    "c": 1012.0 - 2.0 * fraction,
                },
            ),
            timestamp,
        )

    assert result["delta_60m_hpa"] == pytest.approx(-2.0)
    assert result["trend"] == "snelle_daling"


def test_pressure_trend_rejects_implausible_values_and_jumps(pressure_trend_module):
    tracker = pressure_trend_module.PressureTrendTracker()
    base = 1_700_000_000.0
    tracker.update(
        _observations(base, {"a": 1010.0, "b": 1011.0, "c": 1200.0, "d": 1012.0}),
        base,
    )
    result = tracker.update(
        _observations(base + 3600, {"a": 990.0, "b": 991.0, "c": 1000.0, "d": 992.0}),
        base + 3600,
    )

    assert result["pressure_station_count"] == 4
    assert result["stations_60m"] == 0
    assert result["delta_60m_hpa"] is None


@pytest.mark.parametrize(
    ("delta", "expected"),
    [(-2.0, "snelle_daling"), (-1.0, "dalend"), (0.2, "stabiel"),
     (1.0, "stijgend"), (2.0, "snelle_stijging")],
)
def test_pressure_trend_classification(pressure_trend_module, delta, expected):
    assert pressure_trend_module.PressureTrendTracker.classify(delta) == expected
