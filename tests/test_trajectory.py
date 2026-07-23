"""Tests voor de adaptieve, lichte stormtrajectschatter."""
from dataclasses import dataclass
import math

import pytest


@dataclass
class Point:
    lat: float
    lon: float
    ts: float


def _point(origin_lat, origin_lon, timestamp, east_km, north_km):
    return Point(
        lat=origin_lat + north_km / 110.574,
        lon=origin_lon
        + east_km / (111.32 * math.cos(math.radians(origin_lat))),
        ts=timestamp,
    )


def test_straight_track_keeps_linear_model(trajectory_module):
    points = [
        _point(51.0, 4.0, index * 300.0, index * 4.0, 0.0)
        for index in range(7)
    ]

    result = trajectory_module.fit_adaptive_trajectory(points)

    assert result is not None
    assert result.model == "linear"
    assert result.speed_kmh == pytest.approx(48.0, rel=0.03)
    assert result.prediction_error_km < 0.1


def test_predictably_curved_track_selects_constant_acceleration(
    trajectory_module,
):
    points = []
    for index in range(7):
        hours = index * 5.0 / 60.0
        points.append(
            _point(
                51.0,
                4.0,
                index * 300.0,
                east_km=40.0 * hours,
                north_km=0.5 * 80.0 * hours * hours,
            )
        )

    result = trajectory_module.fit_adaptive_trajectory(points)

    assert result is not None
    assert result.model == "constant_acceleration"
    assert result.acceleration_kmh2 == pytest.approx(80.0, rel=0.08)
    assert result.prediction_error_km < result.linear_prediction_error_km
    assert result.fit_quality > 0.95


def test_implausible_acceleration_is_rejected(trajectory_module):
    points = [
        _point(
            51.0,
            4.0,
            index * 300.0,
            east_km=float(index * index * 12),
            north_km=0.0,
        )
        for index in range(7)
    ]

    result = trajectory_module.fit_adaptive_trajectory(points)

    assert result is not None
    assert result.model == "linear"
