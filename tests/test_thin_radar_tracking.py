from __future__ import annotations

import numpy as np
import pytest
from pyproj import Transformer

from storm_tracker_worker.desired_state import Target
from storm_tracker_worker.thin_provider_adapters import ThinRadarField
from storm_tracker_worker.thin_radar_tracking import ThinRadarTracker, build_target_radar_frame


def test_tracker_needs_no_provider_specific_coordinate_scale():
    target = Target(
        "home", "Home", "home", True, 0.0, 0.01,
        100.0, 5.0, 20.0, "BE",
    )
    target_x, target_y = Transformer.from_crs(
        "EPSG:4326", "EPSG:3857", always_xy=True,
    ).transform(target.longitude, target.latitude)
    field = ThinRadarField(
        "knmi_radar", "rtcor", 100.0, "EPSG:3857",
        target_x - 2_000.0, target_y + 2_000.0, 1_000.0, 1_000.0,
        np.ones((4, 4), dtype=float), np.ones((4, 4), dtype=bool),
    )

    frame = build_target_radar_frame(field, target, maximum_pixels=16)

    assert frame.target_x_m == pytest.approx(target_x)
    assert frame.target_y_m == pytest.approx(target_y)
    assert frame.frame.grid.x_resolution_m == pytest.approx(1_000.0)


def test_target_radar_frame_crops_a_metric_field_around_the_target():
    field = ThinRadarField(
        provider_id="dwd_radolan",
        product_id="radolan_rw",
        observed_at=100.0,
        crs="EPSG:3857",
        origin_x_m=-3_000.0,
        origin_y_m=3_000.0,
        x_resolution_m=1_000.0,
        y_resolution_m=1_000.0,
        rain_rate_mm_h=np.arange(36, dtype=float).reshape(6, 6),
        valid_mask=np.ones((6, 6), dtype=bool),
    )
    target = Target(
        target_id="home",
        display_name="Home",
        kind="home",
        available=True,
        latitude=0.0,
        longitude=0.0,
        position_observed_at=100.0,
        location_accuracy_m=5.0,
        observation_radius_km=50.0,
        country_code="BE",
    )

    result = build_target_radar_frame(field, target, maximum_pixels=16)

    assert result.target_id == "home"
    assert result.frame.grid.width * result.frame.grid.height <= 16
    assert result.frame.grid.crs == "EPSG:3857"
    assert result.frame.valid_at == 100.0
    assert result.frame.values == (
        (7.0, 8.0, 9.0, 10.0),
        (13.0, 14.0, 15.0, 16.0),
        (19.0, 20.0, 21.0, 22.0),
        (25.0, 26.0, 27.0, 28.0),
    )
    assert result.target_x_m == 0.0
    assert result.target_y_m == 0.0


def test_radar_tracker_advances_the_existing_celltrack_engine():
    target = Target("home", "Home", "home", True, 0.0, 0.0, 100.0, 5.0, 50.0, "BE")
    tracker = ThinRadarTracker()

    def field(valid_at: float) -> ThinRadarField:
        rates = np.zeros((16, 16), dtype=float)
        rates[6:10, 6:10] = 5.0
        return ThinRadarField("dwd_radolan", "radolan_rw", valid_at, "EPSG:3857", -8_000.0, 8_000.0, 1_000.0, 1_000.0, rates, np.ones((16, 16), dtype=bool))

    first = tracker.advance(field(100.0), target)
    second = tracker.advance(field(400.0), target)

    assert first.next_state.revision == 1
    assert second.next_state.revision == 2
    assert second.next_state.ledger.tracks


def test_radar_tracker_ignores_a_repeated_native_frame_without_erasing_tracks(monkeypatch):
    target = Target("home", "Home", "home", True, 0.0, 0.0, 100.0, 5.0, 50.0, "BE")
    tracker = ThinRadarTracker()

    def field(valid_at: float, first_column: int) -> ThinRadarField:
        rates = np.zeros((16, 16), dtype=float)
        rates[6:10, first_column:first_column + 4] = 5.0
        return ThinRadarField(
            "dwd_radolan", "radolan_rw", valid_at, "EPSG:3857",
            -8_000.0, 8_000.0, 1_000.0, 1_000.0, rates,
            np.ones((16, 16), dtype=bool),
        )

    tracker.advance(field(100.0, 4), target)
    tracker.advance(field(400.0, 6), target)
    before = tracker.tracks(field(400.0, 6), "home")
    def unexpected_crop(*args, **kwargs):
        raise AssertionError("cached frame must not be decoded/cropped again")
    monkeypatch.setattr("storm_tracker_worker.thin_radar_tracking.build_target_radar_frame", unexpected_crop)
    repeated = tracker.advance(field(400.0, 6), target)

    assert before
    assert before[0].velocity_east_kmh > 0.0
    assert before[0].geometry["type"] == "MultiPolygon"
    assert before[0].geometry["coordinates"]
    assert repeated is None
    assert tracker.tracks(field(400.0, 6), "home") == before
