"""Storm Tracker V3 — tests/test_storm.py v0.1.0

Directe tests voor engine/storm.py (de Storm-dataclass zelf), los van
de StormEngine die hem aanstuurt.
"""
from __future__ import annotations

import time

import pytest


def _make_strike(ts, lat, lon):
    class _S:
        pass
    s = _S()
    s.timestamp, s.lat, s.lon = ts, lat, lon
    return s


def test_new_storm_has_default_dormant_false(storm_module):
    storm = storm_module.Storm(centroid_lat=51.0, centroid_lon=4.0)
    assert storm.is_dormant is False
    assert storm.strike_count == 0
    assert storm.storm_id  # niet leeg


def test_two_storms_have_unique_ids(storm_module):
    s1 = storm_module.Storm()
    s2 = storm_module.Storm()
    assert s1.storm_id != s2.storm_id


def test_add_strikes_updates_count_and_clears_dormant(storm_module):
    storm = storm_module.Storm()
    storm.is_dormant = True
    now = time.time()
    strikes = [_make_strike(now, 51.0, 4.0), _make_strike(now, 51.01, 4.01)]

    storm.add_strikes(strikes)

    assert storm.strike_count == 2
    assert storm.is_dormant is False
    assert storm._dirty is True
    assert len(storm._strike_history) == 2


def test_is_expired_true_after_threshold(storm_module):
    storm = storm_module.Storm()
    storm.last_update = time.time() - 10 * 60  # 10 min geleden
    assert storm.is_expired(expire_minutes=5.0) is True
    assert storm.is_expired(expire_minutes=15.0) is False


def test_strikes_in_window_filters_by_age(storm_module):
    storm = storm_module.Storm()
    now = time.time()
    storm._strike_history = [
        (now - 400, 51.0, 4.0),   # ouder dan 5 min -> buiten venster
        (now - 60,  51.1, 4.1),   # binnen 5 min venster
    ]
    recent = storm.strikes_in_window(minutes=5)
    assert len(recent) == 1
    assert recent[0][1] == 51.1


def test_update_counts_reflects_recent_strikes(storm_module):
    storm = storm_module.Storm()
    now = time.time()
    storm._strike_history = [(now, 51.0, 4.0) for _ in range(3)]
    storm.update_counts()
    assert storm.strikes_5min == 3
    assert storm.strikes_60min == 3


def test_prune_history_removes_old_strikes(storm_module):
    storm = storm_module.Storm()
    now = time.time()
    storm._strike_history = [
        (now - 100 * 60, 51.0, 4.0),  # 100 min oud -> weg bij default max_age=90
        (now - 10 * 60,  51.1, 4.1),  # blijft
    ]
    storm.prune_history(max_age_minutes=90)
    assert len(storm._strike_history) == 1
    assert storm._strike_history[0][1] == 51.1


def test_motion_to_target_only_returns_eta_when_approaching(storm_module):
    storm = storm_module.Storm(
        centroid_lat=51.0,
        centroid_lon=4.0,
        heading_deg=90.0,
        speed_kmh=60.0,
        confidence="Matig",
    )

    east = storm.motion_to_target(51.0, 5.0, distance_km=60.0)
    assert east["moving_towards"] is True
    assert east["approach_speed_kmh"] == pytest.approx(60.0, abs=0.1)
    assert east["eta_minutes"] == pytest.approx(60.0, abs=0.1)
    # Een koers van 90 graden en de grootcirkelkoers naar (51, 5) wijken
    # licht af; de correcte cross-trackafstand is ongeveer een halve km.
    assert east["closest_pass_distance_km"] == pytest.approx(0.5, abs=0.1)
    assert east["closest_pass_minutes"] == pytest.approx(70.0, abs=2.0)

    west = storm.motion_to_target(51.0, 3.0, distance_km=60.0)
    assert west["moving_towards"] is False
    assert west["approach_speed_kmh"] == pytest.approx(-60.0, abs=0.1)
    assert west["eta_minutes"] is None
    assert west["closest_pass_distance_km"] is None
    assert west["closest_pass_minutes"] is None


def test_motion_to_target_suppresses_eta_without_reliable_vector(storm_module):
    storm = storm_module.Storm(
        centroid_lat=51.0,
        centroid_lon=4.0,
        heading_deg=90.0,
        speed_kmh=60.0,
        confidence="Onvoldoende data",
    )

    motion = storm.motion_to_target(51.0, 5.0, distance_km=60.0)
    assert motion["moving_towards"] is True
    assert motion["eta_minutes"] is None
    assert motion["closest_pass_distance_km"] is None


def test_motion_to_target_without_vector_returns_nullable_sensor_values(storm_module):
    storm = storm_module.Storm(centroid_lat=51.0, centroid_lon=4.0)

    motion = storm.motion_to_target(51.0, 5.0, distance_km=60.0)
    assert motion["bearing_to_target_deg"] is not None
    assert motion["approach_speed_kmh"] is None
    assert motion["moving_towards"] is None
    assert motion["eta_minutes"] is None
    assert motion["closest_pass_distance_km"] is None


def test_distance_to_polygon_origin_detects_hit_and_miss(storm_module):
    assert storm_module._distance_to_polygon_origin([
        (-2.0, -2.0), (2.0, -2.0), (2.0, 2.0), (-2.0, 2.0)
    ]) == 0.0
    assert storm_module._distance_to_polygon_origin([
        (3.0, -1.0), (5.0, -1.0), (5.0, 1.0), (3.0, 1.0)
    ]) == pytest.approx(3.0)


def test_radar_tracking_status_requires_two_frames(storm_module):
    storm = storm_module.Storm()
    storm.radar_cells["first"] = storm_module.RadarCellSnapshot(
        cell_id="first", timestamp=1_000.0, lat=51.0, lon=4.0,
        intensity=3, area_km2=100.0,
    )
    assert storm.consecutive_radar_frames == 1
    assert storm.tracking_status == "waargenomen"

    storm.radar_cells["second"] = storm_module.RadarCellSnapshot(
        cell_id="second", timestamp=1_300.0, lat=51.1, lon=4.1,
        intensity=3, area_km2=100.0,
    )
    assert storm.consecutive_radar_frames == 2
    assert storm.tracking_status == "bevestigd"
    assert storm.last_radar_timestamp == 1_300.0


def test_radar_tracking_sequence_restarts_after_large_gap(storm_module):
    storm = storm_module.Storm()
    for key, timestamp in (("old", 1_000.0), ("new", 3_000.0)):
        storm.radar_cells[key] = storm_module.RadarCellSnapshot(
            cell_id=key, timestamp=timestamp, lat=51.0, lon=4.0,
            intensity=3, area_km2=100.0,
        )

    assert storm.consecutive_radar_frames == 1
    assert storm.tracking_status == "waargenomen"


def test_unconfirmed_radar_system_suppresses_motion_projection(storm_module):
    storm = storm_module.Storm(
        centroid_lat=51.0, centroid_lon=4.0,
        heading_deg=90.0, speed_kmh=60.0, confidence="Matig",
    )
    storm.radar_cells["only"] = storm_module.RadarCellSnapshot(
        cell_id="only", timestamp=1_000.0, lat=51.0, lon=4.0,
        intensity=3, area_km2=100.0,
    )

    motion = storm.motion_to_target(51.0, 5.0, distance_km=60.0)
    assert motion["approach_speed_kmh"] is None
    assert motion["eta_minutes"] is None
