"""Tests voor de compacte operationele neerslagstatus."""
from datetime import datetime, timezone


def _add_frame(storm_module, storm, key, timestamp, *, lat=51.0, lon=4.0, dbz=35.0):
    storm.radar_cells[key] = storm_module.RadarCellSnapshot(
        cell_id=key,
        timestamp=timestamp,
        lat=lat,
        lon=lon,
        intensity=3,
        area_km2=100.0,
        max_dbz=dbz,
    )


def test_status_is_dry_without_active_systems(nowcast_module):
    result = nowcast_module.build_precipitation_status(
        [], 51.0, 5.0,
        radar_source="opera",
        pressure_trend={"trend": "stabiel", "delta_60m_hpa": -0.2},
    )

    assert result["status"] == "droog"
    assert result["active_system_count"] == 0
    assert result["radar_source"] == "opera"
    assert result["pressure_trend"] == "stabiel"


def test_cold_start_without_pressure_history_is_neutral(nowcast_module):
    result = nowcast_module.build_precipitation_status(
        [], 51.0, 5.0, radar_source=None, pressure_trend=None
    )

    assert result["status"] == "droog"
    assert result["pressure_trend"] == "onvoldoende_data"
    assert result["pressure_delta_60m_hpa"] is None
    assert result["rapid_pressure_fall"] is False
    assert result["forecast_confidence_percent"] is None


def test_single_radar_frame_is_only_observed(nowcast_module, storm_module):
    storm = storm_module.Storm(storm_id="echo", centroid_lat=51.0, centroid_lon=4.0)
    _add_frame(storm_module, storm, "one", 1_000.0, dbz=32.0)

    result = nowcast_module.build_precipitation_status([storm], 51.0, 5.0)

    assert result["status"] == "waargenomen"
    assert result["tracking_status"] == "waargenomen"
    assert result["consecutive_radar_frames"] == 1
    assert result["eta_minutes"] is None


def test_confirmed_approaching_system_gets_eta(nowcast_module, storm_module):
    storm = storm_module.Storm(
        storm_id="approaching",
        centroid_lat=51.0,
        centroid_lon=4.0,
        heading_deg=90.0,
        speed_kmh=60.0,
        confidence="Matig",
    )
    _add_frame(storm_module, storm, "one", 1_000.0, dbz=34.0)
    _add_frame(storm_module, storm, "two", 1_300.0, lat=51.0, lon=4.1, dbz=38.0)

    result = nowcast_module.build_precipitation_status([storm], 51.0, 5.0)

    assert result["status"] == "naderend"
    assert result["tracking_status"] == "bevestigd"
    assert result["moving_towards"] is True
    assert result["eta_minutes"] is not None
    assert result["max_dbz"] == 38.0


def test_target_forecast_projects_passage_intensity_and_confidence(nowcast_module, storm_module):
    storm = storm_module.Storm(
        storm_id="forecast", centroid_lat=51.0, centroid_lon=4.0,
        heading_deg=90.0, speed_kmh=80.0, confidence="Hoog",
    )
    _add_frame(storm_module, storm, "one", 1_000.0, lon=4.0, dbz=30.0)
    _add_frame(storm_module, storm, "two", 1_300.0, lon=4.1, dbz=35.0)

    motion = storm.motion_to_target(51.0, 4.8)
    forecast = nowcast_module._target_forecast(
        storm, motion, now_utc=datetime(2026, 7, 20, 12, 0, tzinfo=timezone.utc)
    )

    assert forecast["forecast_available"] is True
    assert forecast["forecast_horizon_minutes"] <= 90
    assert forecast["expected_passage_at"].startswith("2026-07-20T12:")
    assert forecast["forecast_intensity_dbz"] > 35.0
    assert forecast["forecast_intensity_label"] in {"matig", "zwaar"}
    assert 25 <= forecast["forecast_confidence_percent"] <= 95


def test_target_forecast_refuses_low_confidence_or_long_horizon(nowcast_module, storm_module):
    storm = storm_module.Storm(
        storm_id="uncertain", centroid_lat=51.0, centroid_lon=4.0,
        heading_deg=90.0, speed_kmh=30.0, confidence="Laag",
    )
    _add_frame(storm_module, storm, "one", 1_000.0, dbz=35.0)
    _add_frame(storm_module, storm, "two", 1_300.0, lon=4.1, dbz=36.0)

    forecast = nowcast_module._target_forecast(
        storm, {
            "closest_pass_minutes": 30,
            "passage_classification": "raak",
        }
    )

    assert forecast["forecast_available"] is False
    assert forecast["expected_passage_at"] is None


def test_confirmed_system_preferred_over_closer_single_echo(nowcast_module, storm_module):
    close_echo = storm_module.Storm(storm_id="close", centroid_lat=51.0, centroid_lon=4.9)
    _add_frame(storm_module, close_echo, "close-one", 1_000.0, lat=51.0, lon=4.9)

    confirmed = storm_module.Storm(storm_id="confirmed", centroid_lat=51.0, centroid_lon=4.0)
    _add_frame(storm_module, confirmed, "far-one", 1_000.0)
    _add_frame(storm_module, confirmed, "far-two", 1_300.0, lon=4.1)

    result = nowcast_module.build_precipitation_status(
        [close_echo, confirmed], 51.0, 5.0
    )

    assert result["storm_id"] == "confirmed"
    assert result["status"] == "bevestigd"
    assert result["active_system_count"] == 2


def test_approaching_hit_is_preferred_over_closer_moving_away(nowcast_module, storm_module):
    moving_away = storm_module.Storm(
        storm_id="away", centroid_lat=51.0, centroid_lon=4.4,
        heading_deg=270.0, speed_kmh=50.0, confidence="Hoog",
    )
    _add_frame(storm_module, moving_away, "away-one", 1_000.0, lon=4.4)
    _add_frame(storm_module, moving_away, "away-two", 1_300.0, lon=4.3)

    approaching = storm_module.Storm(
        storm_id="incoming", centroid_lat=51.0, centroid_lon=4.0,
        heading_deg=90.0, speed_kmh=60.0, confidence="Hoog",
    )
    _add_frame(storm_module, approaching, "in-one", 1_000.0, lon=4.0)
    _add_frame(storm_module, approaching, "in-two", 1_300.0, lon=4.1)

    result = nowcast_module.build_precipitation_status(
        [moving_away, approaching], 51.0, 5.0
    )

    assert result["storm_id"] == "incoming"
    assert result["selected_reason"] in {"forecast_hit", "forecast_edge"}
    assert result["status"] == "naderend"


def test_current_precipitation_stays_primary_over_future_threat(nowcast_module, storm_module):
    current = storm_module.Storm(
        storm_id="current", centroid_lat=51.0, centroid_lon=5.0,
        heading_deg=270.0, speed_kmh=40.0, confidence="Hoog",
    )
    _add_frame(storm_module, current, "cur-one", 1_000.0, lon=5.0)
    _add_frame(storm_module, current, "cur-two", 1_300.0, lon=4.99)

    future = storm_module.Storm(
        storm_id="future", centroid_lat=51.0, centroid_lon=4.0,
        heading_deg=90.0, speed_kmh=60.0, confidence="Hoog",
    )
    _add_frame(storm_module, future, "future-one", 1_000.0, lon=4.0)
    _add_frame(storm_module, future, "future-two", 1_300.0, lon=4.1)

    result = nowcast_module.build_precipitation_status([future, current], 51.0, 5.0)

    assert result["storm_id"] == "current"
    assert result["selected_reason"] == "current_precipitation"


def test_confirmed_reliable_system_reports_away_and_lateral_motion(
    nowcast_module, storm_module,
):
    away = storm_module.Storm(
        storm_id="away", centroid_lat=51.0, centroid_lon=4.0,
        heading_deg=270.0, speed_kmh=60.0, confidence="Matig",
    )
    _add_frame(storm_module, away, "away-one", 1_000.0)
    _add_frame(storm_module, away, "away-two", 1_300.0, lon=3.9)
    result = nowcast_module.build_precipitation_status([away], 51.0, 5.0)
    assert result["status"] == "wegtrekkend"
    assert result["approach_speed_kmh"] < -1
    assert result["eta_minutes"] is None

    lateral = storm_module.Storm(
        storm_id="lateral", centroid_lat=51.0, centroid_lon=4.0,
        heading_deg=0.0, speed_kmh=60.0, confidence="Hoog",
    )
    _add_frame(storm_module, lateral, "side-one", 1_000.0)
    _add_frame(storm_module, lateral, "side-two", 1_300.0, lat=51.1)
    result = nowcast_module.build_precipitation_status([lateral], 51.0, 5.0)
    assert result["status"] == "langs_trekkend"
    assert abs(result["approach_speed_kmh"]) <= 1


def test_low_confidence_vector_keeps_confirmed_status(nowcast_module, storm_module):
    storm = storm_module.Storm(
        storm_id="uncertain", centroid_lat=51.0, centroid_lon=4.0,
        heading_deg=90.0, speed_kmh=60.0, confidence="Laag",
    )
    _add_frame(storm_module, storm, "one", 1_000.0)
    _add_frame(storm_module, storm, "two", 1_300.0, lon=4.1)
    result = nowcast_module.build_precipitation_status([storm], 51.0, 5.0)
    assert result["status"] == "bevestigd"
    assert result["moving_towards"] is True
    assert result["motion_confidence"] == "Laag"
