"""Tests voor de compacte operationele neerslagstatus."""
import math
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


def test_same_system_eta_cannot_jump_back_after_imminent_warning(nowcast_module):
    previous = {
        "status": "naderend",
        "storm_id": "same-system",
        "eta_minutes": 1.0,
        "passage_classification": "rand",
        "approach_speed_kmh": 48.0,
    }
    current = {
        "status": "naderend",
        "storm_id": "same-system",
        "eta_minutes": 64.0,
        "passage_classification": "raak",
        "approach_speed_kmh": 47.0,
        "approaching_systems": [{
            "storm_id": "same-system",
            "eta_minutes": 64,
            "selected": True,
        }],
    }

    result = nowcast_module.stabilize_eta_continuity(
        current, previous, elapsed_minutes=2.5
    )

    assert result["eta_minutes"] == 0
    assert result["raw_eta_minutes"] == 64.0
    assert result["eta_continuity_guarded"] is True
    assert result["eta_continuity_reason"] == "same_system_eta_jump"
    assert result["approaching_systems"][0]["eta_minutes"] == 0
    assert result["approaching_systems"][0]["raw_eta_minutes"] == 64


def test_eta_continuity_allows_new_system_and_small_correction(nowcast_module):
    previous = {
        "status": "naderend",
        "storm_id": "old",
        "eta_minutes": 10.0,
        "passage_classification": "raak",
        "approach_speed_kmh": 30.0,
    }
    replacement = {
        "status": "naderend",
        "storm_id": "new",
        "eta_minutes": 70.0,
        "passage_classification": "raak",
        "approach_speed_kmh": 30.0,
    }
    corrected = {
        **previous,
        "eta_minutes": 18.0,
    }

    assert nowcast_module.stabilize_eta_continuity(
        replacement, previous, elapsed_minutes=2.0
    )["eta_minutes"] == 70.0
    assert nowcast_module.stabilize_eta_continuity(
        corrected, previous, elapsed_minutes=2.0
    )["eta_minutes"] == 18.0


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
    assert forecast["forecast_block_reason"] == "bewegingsvector_onvoldoende_betrouwbaar"


def test_target_forecast_refuses_predicted_miss(nowcast_module, storm_module):
    storm = storm_module.Storm(
        storm_id="miss", centroid_lat=51.0, centroid_lon=4.0,
        heading_deg=0.0, speed_kmh=45.0, confidence="Hoog",
    )
    _add_frame(storm_module, storm, "one", 1_000.0, dbz=35.0)
    _add_frame(storm_module, storm, "two", 1_300.0, lat=51.1, dbz=36.0)

    forecast = nowcast_module._target_forecast(
        storm, {
            "closest_pass_minutes": 30,
            "passage_classification": "mist",
        }
    )

    assert forecast["forecast_available"] is False
    assert forecast["forecast_block_reason"] == "geen_passage_binnen_corridor"


def test_approaching_system_that_misses_gets_regional_status_without_warning(
    nowcast_module, storm_module,
):
    storm = storm_module.Storm(
        storm_id="regional-miss",
        centroid_lat=51.0,
        centroid_lon=4.0,
        heading_deg=45.0,
        speed_kmh=50.0,
        confidence="Hoog",
    )
    _add_frame(storm_module, storm, "one", 1_000.0, lat=51.0, lon=4.0)
    _add_frame(storm_module, storm, "two", 1_600.0, lat=51.0, lon=4.1)
    storm.heading_deg = 45.0
    storm.speed_kmh = 50.0
    storm.velocity_east_kmh = 50.0 * math.sin(math.radians(45.0))
    storm.velocity_north_kmh = 50.0 * math.cos(math.radians(45.0))
    storm.confidence = "Hoog"

    result = nowcast_module.build_precipitation_status(
        [storm], 51.0, 5.0
    )

    assert result["approach_speed_kmh"] > 1.0
    assert result["passage_classification"] == "mist"
    assert result["status"] == "nadert_regio"
    assert result["local_passage_expected"] is False
    assert result["eta_reliable"] is False
    assert result["warning_eligible"] is False
    assert result["forecast_block_reason"] == "geen_passage_binnen_corridor"
    assert 0 <= result["reliability_score_percent"] <= 100
    assert result["reliability_label"] in {
        "Onvoldoende", "Laag", "Matig", "Hoog"
    }
    assert set(result["reliability_factors"]) == {
        "tracking", "frames", "motion", "hindcast", "passage", "radar_source"
    }


def test_edge_passage_with_material_hindcast_error_cannot_score_high(
    nowcast_module,
):
    class Storm:
        consecutive_radar_frames = 14
        tracking_status = "bevestigd"
        motion_fit_quality = 0.674
        motion_prediction_error_km = 6.52

    score, label, factors = nowcast_module._reliability_score(
        Storm(),
        {"passage_classification": "rand"},
        {"motion_guidance_score": 81},
        radar_source="rainviewer",
    )

    assert factors["hindcast"] < 50
    assert score <= 69
    assert label == "Matig"


def test_target_forecast_explains_that_system_is_moving_away(
    nowcast_module, storm_module,
):
    storm = storm_module.Storm(
        storm_id="away-uncertain", centroid_lat=51.0, centroid_lon=4.0,
        heading_deg=270.0, speed_kmh=45.0, confidence="Laag",
    )
    _add_frame(storm_module, storm, "one", 1_000.0, lon=4.0)
    _add_frame(storm_module, storm, "two", 1_300.0, lon=3.9)

    motion = storm.motion_to_target(51.0, 5.0)
    forecast = nowcast_module._target_forecast(storm, motion)

    assert motion["approach_speed_kmh"] < -1
    assert forecast["forecast_available"] is False
    assert forecast["expected_passage_at"] is None
    assert forecast["forecast_block_reason"] == "systeem_trekt_weg"


def test_confirmed_system_preferred_over_closer_single_echo(nowcast_module, storm_module):
    close_echo = storm_module.Storm(storm_id="close", centroid_lat=51.0, centroid_lon=4.9)
    _add_frame(storm_module, close_echo, "close-one", 1_300.0, lat=51.0, lon=4.9)

    confirmed = storm_module.Storm(storm_id="confirmed", centroid_lat=51.0, centroid_lon=4.0)
    _add_frame(storm_module, confirmed, "far-one", 1_000.0)
    _add_frame(storm_module, confirmed, "far-two", 1_300.0, lon=4.1)

    result = nowcast_module.build_precipitation_status(
        [close_echo, confirmed], 51.0, 5.0
    )

    assert result["storm_id"] == "confirmed"
    assert result["nearest_precipitation_storm_id"] == "close"
    assert result["tracked_system_storm_id"] == "confirmed"
    assert result["nearest_precipitation_is_tracked_system"] is False
    assert result["distance_system_relation"] == "verschillende_systemen"
    assert result["distance_km"] < 10
    assert result["nearest_precipitation_distance_km"] == result["distance_km"]
    assert result["nearest_precipitation_frames"] == 1
    assert result["tracked_system_frames"] == 2
    assert result["nearest_precipitation_max_dbz"] == 35.0
    assert result["tracked_system_distance_km"] > result["distance_km"]
    assert (
        result["nearest_precipitation_centroid_distance_km"]
        < result["tracked_system_centroid_distance_km"]
    )
    assert result["status"] == "bevestigd"
    assert result["active_system_count"] == 2


def test_distance_uses_same_current_radar_frame_as_map(nowcast_module, storm_module):
    storm = storm_module.Storm(
        storm_id="moving-rain", centroid_lat=51.0, centroid_lon=4.0
    )
    _add_frame(
        storm_module, storm, "historical-near", 1_000.0, lat=51.0, lon=5.0
    )
    _add_frame(
        storm_module, storm, "current-far", 1_600.0, lat=51.0, lon=4.0
    )

    result = nowcast_module.build_precipitation_status(
        [storm], 51.0, 5.0
    )

    assert result["distance_km"] > 60
    assert result["impact_lon"] == 4.0


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


def test_incoming_system_takes_focus_from_departing_current_contour(
    nowcast_module, storm_module,
):
    current = storm_module.Storm(
        storm_id="current",
        centroid_lat=51.0,
        centroid_lon=5.1,
        heading_deg=90.0,
        speed_kmh=40.0,
        confidence="Hoog",
    )
    _add_frame(storm_module, current, "cur-one", 1_000.0, lon=5.01)
    _add_frame(storm_module, current, "cur-two", 1_300.0, lon=5.01)

    future = storm_module.Storm(
        storm_id="future", centroid_lat=51.0, centroid_lon=4.0,
        heading_deg=90.0, speed_kmh=60.0, confidence="Hoog",
    )
    _add_frame(storm_module, future, "future-one", 1_000.0, lon=4.0)
    _add_frame(storm_module, future, "future-two", 1_300.0, lon=4.1)

    result = nowcast_module.build_precipitation_status([future, current], 51.0, 5.0)

    assert result["nearest_precipitation_storm_id"] == "current"
    assert result["distance_km"] <= 5.0
    assert result["storm_id"] == "future"
    assert result["selected_reason"] in {"forecast_hit", "forecast_edge"}
    assert result["status"] == "naderend"
    assert result["approaching_system_count"] == 1
    assert result["approaching_systems"][0]["storm_id"] == "future"
    assert result["approaching_systems"][0]["selected"] is True


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


def test_curved_validated_track_can_approach_while_tangent_is_lateral(
    nowcast_module, storm_module,
):
    """De volledige baan primeert op de momentane raaklijn."""
    storm = storm_module.Storm(
        storm_id="curved",
        centroid_lat=51.0,
        centroid_lon=4.0,
        heading_deg=0.0,
        speed_kmh=20.0,
        confidence="Matig",
        motion_model="constant_acceleration",
        velocity_east_kmh=0.0,
        velocity_north_kmh=20.0,
        acceleration_east_kmh2=80.0,
        acceleration_north_kmh2=-40.0,
        motion_prediction_error_km=1.0,
    )
    _add_frame(storm_module, storm, "one", 1_000.0, lat=51.0, lon=4.0)
    _add_frame(storm_module, storm, "two", 1_300.0, lat=51.0, lon=4.0)
    target_lon = 4.0 + 40.0 / (111.32 * 0.6293)

    result = nowcast_module.build_precipitation_status(
        [storm], 51.0, target_lon
    )

    assert abs(result["approach_speed_kmh"]) <= 1.0
    assert result["eta_minutes"] is not None
    assert result["passage_classification"] in {"raak", "rand"}
    assert result["status"] == "naderend"


def test_low_confidence_vector_reports_direction_but_keeps_eta_unreliable(
    nowcast_module, storm_module,
):
    storm = storm_module.Storm(
        storm_id="uncertain", centroid_lat=51.0, centroid_lon=4.0,
        heading_deg=90.0, speed_kmh=60.0, confidence="Laag",
    )
    _add_frame(storm_module, storm, "one", 1_000.0)
    _add_frame(storm_module, storm, "two", 1_300.0, lon=4.1)
    result = nowcast_module.build_precipitation_status([storm], 51.0, 5.0)
    assert result["status"] == "naderend"
    assert result["moving_towards"] is True
    assert result["motion_confidence"] == "Laag"
    assert result["direction_reliable"] is False
    assert result["eta_reliable"] is False
    assert result["forecast_block_reason"] == "bewegingsvector_onvoldoende_betrouwbaar"


def test_low_confidence_moving_away_is_visible_without_eta(
    nowcast_module, storm_module,
):
    storm = storm_module.Storm(
        storm_id="uncertain-away", centroid_lat=51.0, centroid_lon=4.0,
        heading_deg=270.0, speed_kmh=60.0, confidence="Laag",
    )
    _add_frame(storm_module, storm, "one", 1_000.0)
    _add_frame(storm_module, storm, "two", 1_300.0, lon=3.9)

    result = nowcast_module.build_precipitation_status([storm], 51.0, 5.0)

    assert result["status"] == "wegtrekkend"
    assert result["approach_speed_kmh"] < -1
    assert result["direction_reliable"] is False
    assert result["eta_reliable"] is False
    assert result["eta_minutes"] is None
    assert result["forecast_available"] is False
    assert result["forecast_block_reason"] == "systeem_trekt_weg"


def test_overlapping_contour_moving_away_is_not_reported_as_arriving(
    nowcast_module, storm_module,
):
    storm = storm_module.Storm(
        storm_id="broad-away",
        centroid_lat=51.0,
        centroid_lon=5.1,
        heading_deg=90.0,
        speed_kmh=50.0,
        confidence="Matig",
    )
    _add_frame(storm_module, storm, "one", 1_000.0, lat=51.0, lon=4.40)
    _add_frame(storm_module, storm, "two", 1_600.0, lat=51.0, lon=4.40)

    result = nowcast_module.build_precipitation_status(
        [storm], 51.0, 4.35
    )

    assert result["distance_km"] <= 5.0
    assert result["approach_speed_kmh"] < -1.0
    assert result["status"] == "wegtrekkend"
    assert result["selected_reason"] == "moving_away"
    assert result["eta_minutes"] is None
    assert result["eta_reliable"] is False
    assert result["forecast_available"] is False
    assert result["forecast_block_reason"] == "systeem_trekt_weg"


def test_two_frame_pysteps_vector_publishes_provisional_eta(
    nowcast_module, storm_module,
):
    storm = storm_module.Storm(
        storm_id="early-vector",
        centroid_lat=51.0,
        centroid_lon=4.0,
        confidence="Onvoldoende data",
    )
    _add_frame(storm_module, storm, "one", 1_000.0, lon=4.0)
    _add_frame(storm_module, storm, "two", 1_600.0, lon=4.1)
    result = nowcast_module.build_precipitation_status(
        [storm],
        51.0,
        4.8,
        motion_guidance={
            "status": "warming_up_vectors",
            "frames_used": 2,
            "confidence": "insufficient",
            "translation_km_per_hour": 42.0,
            "heading_deg": 90.0,
            "motion_match_iou": 0.70,
            "wet_cell_ratio": 1.05,
        },
    )

    assert result["eta_minutes"] is not None
    assert result["motion_basis"] == "pysteps_raster"
    assert result["motion_guidance_stage"] == "provisional"
    assert 1 <= result["motion_guidance_score"] < 60
    assert result["eta_reliable"] is False
    assert result["forecast_provisional"] is True


def test_three_frame_verified_pysteps_vector_unlocks_reliable_eta(
    nowcast_module, storm_module,
):
    storm = storm_module.Storm(
        storm_id="verified-vector",
        centroid_lat=51.0,
        centroid_lon=4.0,
        confidence="Onvoldoende data",
    )
    _add_frame(storm_module, storm, "one", 1_000.0, lon=4.0)
    _add_frame(storm_module, storm, "two", 1_600.0, lon=4.1)
    result = nowcast_module.build_precipitation_status(
        [storm],
        51.0,
        4.8,
        motion_guidance={
            "status": "ready",
            "frames_used": 3,
            "confidence": "high",
            "translation_km_per_hour": 42.0,
            "heading_deg": 90.0,
            "verification_iou": 0.72,
            "motion_match_iou": 0.72,
            "vector_consistency_deg": 0.0,
            "wet_cell_ratio": 1.0,
        },
    )

    assert result["eta_minutes"] is not None
    assert result["motion_guidance_stage"] == "verified"
    assert result["motion_guidance_score"] >= 60
    assert result["eta_reliable"] is True
    assert result["forecast_available"] is True
    assert result["forecast_provisional"] is False


def test_extreme_finite_pysteps_vector_is_not_rejected_by_speed(
    nowcast_module, storm_module,
):
    storm = storm_module.Storm(
        storm_id="fast-vector",
        centroid_lat=51.0,
        centroid_lon=4.0,
        confidence="Onvoldoende data",
    )
    _add_frame(storm_module, storm, "one", 1_000.0, lon=4.0)
    _add_frame(storm_module, storm, "two", 1_600.0, lon=4.1)

    result = nowcast_module.build_precipitation_status(
        [storm],
        51.0,
        4.8,
        motion_guidance={
            "status": "warming_up_vectors",
            "frames_used": 2,
            "confidence": "insufficient",
            "translation_km_per_hour": 880.0,
            "heading_deg": 90.0,
            "motion_match_iou": 0.70,
            "wet_cell_ratio": 1.05,
        },
    )

    assert result["motion_basis"] == "pysteps_raster"
    assert result["motion_guidance_stage"] == "provisional"
    assert result["eta_minutes"] is not None
    assert result["eta_minutes"] < 10.0
