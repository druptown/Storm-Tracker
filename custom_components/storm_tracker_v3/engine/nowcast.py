"""Compact operationeel neerslagbeeld voor sensoren en dashboards."""
from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone


def _intensity_label(dbz: float) -> str:
    if dbz < 20:
        return "zeer licht"
    if dbz < 30:
        return "licht"
    if dbz < 40:
        return "matig"
    if dbz < 50:
        return "zwaar"
    return "zeer zwaar"


def _target_forecast(storm, motion: dict, *, now_utc: datetime | None = None) -> dict:
    """Maak een conservatieve korte-termijnprognose voor één target."""
    empty = {
        "forecast_available": False,
        "expected_passage_at": None,
        "forecast_horizon_minutes": None,
        "forecast_confidence_percent": None,
        "forecast_intensity_dbz": None,
        "forecast_intensity_label": None,
        "intensity_trend_dbz_per_hour": None,
        "forecast_block_reason": None,
        "forecast_motion_model": getattr(storm, "motion_model", "none"),
    }
    if storm.tracking_status != "bevestigd":
        return {**empty, "forecast_block_reason": "systeem_nog_niet_bevestigd"}
    if storm.confidence not in {"Matig", "Hoog"}:
        return {**empty, "forecast_block_reason": "bewegingsvector_onvoldoende_betrouwbaar"}
    minutes = motion.get("closest_pass_minutes")
    passage_classification = motion.get("passage_classification")
    if minutes is None or passage_classification is None:
        return {**empty, "forecast_block_reason": "passage_nog_niet_berekenbaar"}
    if passage_classification == "mist":
        return {**empty, "forecast_block_reason": "geen_passage_binnen_corridor"}
    if minutes < 0 or minutes > 90:
        return {**empty, "forecast_block_reason": "buiten_prognosehorizon"}
    if not any(cell.max_dbz is not None for cell in storm.radar_cells.values()):
        return {**empty, "forecast_block_reason": "intensiteit_onbekend"}

    cells = [cell for cell in storm.radar_cells.values() if cell.max_dbz is not None]
    if not cells:
        return empty
    timestamps = sorted({cell.timestamp for cell in cells})
    latest_ts = timestamps[-1]
    current_dbz = max(cell.max_dbz for cell in cells if cell.timestamp == latest_ts)
    trend = 0.0
    if len(timestamps) >= 2:
        earliest_ts = timestamps[0]
        elapsed_hours = (latest_ts - earliest_ts) / 3600.0
        if elapsed_hours > 0:
            earliest_dbz = max(
                cell.max_dbz for cell in cells if cell.timestamp == earliest_ts
            )
            trend = (current_dbz - earliest_dbz) / elapsed_hours

    # Intensiteitstrends zijn grillig: extrapoleer maximaal ±10 dBZ.
    change = max(-10.0, min(10.0, trend * float(minutes) / 60.0))
    forecast_dbz = max(0.0, min(75.0, current_dbz + change))
    base_confidence = 82 if storm.confidence == "Hoog" else 64
    frame_bonus = min(10, max(0, storm.consecutive_radar_frames - 2) * 3)
    horizon_penalty = round(float(minutes) / 90.0 * 24)
    confidence = max(25, min(95, base_confidence + frame_bonus - horizon_penalty))
    reference = now_utc or datetime.now(timezone.utc)
    passage_at = reference + timedelta(minutes=float(minutes))
    return {
        "forecast_available": True,
        "expected_passage_at": passage_at.isoformat(timespec="minutes"),
        "forecast_horizon_minutes": round(float(minutes)),
        "forecast_confidence_percent": confidence,
        "forecast_intensity_dbz": round(forecast_dbz, 1),
        "forecast_intensity_label": _intensity_label(forecast_dbz),
        "intensity_trend_dbz_per_hour": round(trend, 1),
        "forecast_block_reason": None,
        "forecast_motion_model": getattr(storm, "motion_model", "none"),
    }


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    value = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(dlon / 2) ** 2
    )
    return 6371.0088 * 2 * math.atan2(math.sqrt(value), math.sqrt(1 - value))


def _candidate_rank(point: tuple, storm, motion: dict) -> tuple[tuple, str]:
    """Rangschik systemen op relevantie voor één target, niet alleen afstand."""
    distance = float(point[0])
    confirmed = storm.tracking_status == "bevestigd"
    if not confirmed:
        return ((1, 8, distance), "observed")
    if distance <= 5.0:
        return ((0, 0, distance), "current_precipitation")

    passage = motion.get("passage_classification")
    minutes = motion.get("closest_pass_minutes")
    within_horizon = minutes is not None and 0 <= minutes <= 90
    if within_horizon and passage == "raak":
        return ((0, 1, minutes, distance), "forecast_hit")
    if within_horizon and passage == "rand":
        return ((0, 2, minutes, distance), "forecast_edge")

    approach = motion.get("approach_speed_kmh")
    if approach is not None and approach > 1.0:
        if within_horizon and passage != "mist":
            return ((0, 3, minutes, distance), "approaching")
        return ((0, 4, distance), "approaching")
    if approach is not None and abs(approach) <= 1.0:
        return ((0, 5, distance), "lateral")
    if approach is not None and approach < -1.0:
        return ((0, 6, distance), "moving_away")
    return ((0, 7, distance), "confirmed")


def build_precipitation_status(
    storms: list,
    target_lat: float,
    target_lon: float,
    *,
    radar_source: str | None = None,
    pressure_trend: dict | None = None,
) -> dict:
    """Vat actieve systemen samen tot één dashboardvriendelijke toestand."""
    active = [storm for storm in storms if not getattr(storm, "is_dormant", False)]
    base = {
        "status": "droog",
        "radar_source": radar_source,
        "active_system_count": len(active),
        "pressure_trend": (pressure_trend or {}).get("trend", "onvoldoende_data"),
        "pressure_delta_60m_hpa": (pressure_trend or {}).get("delta_60m_hpa"),
        "rapid_pressure_fall": (pressure_trend or {}).get("rapid_fall", False),
        "selected_reason": None,
        "nearest_precipitation_storm_id": None,
        "nearest_precipitation_distance_km": None,
        "nearest_precipitation_centroid_distance_km": None,
        "nearest_precipitation_max_dbz": None,
        "nearest_precipitation_frames": None,
        "tracked_system_distance_km": None,
        "tracked_system_centroid_distance_km": None,
        "eta_reliable": False,
        "eta_basis": None,
        "footprint_pass_minutes": None,
        "forecast_available": False,
        "expected_passage_at": None,
        "forecast_horizon_minutes": None,
        "forecast_confidence_percent": None,
        "forecast_intensity_dbz": None,
        "forecast_intensity_label": None,
        "intensity_trend_dbz_per_hour": None,
        "forecast_block_reason": None,
        "forecast_motion_model": None,
    }
    if not active:
        return base

    candidates = []
    for storm in active:
        point = storm.closest_radar_point(target_lat, target_lon)
        if point is None:
            point = (
                _haversine_km(target_lat, target_lon, storm.centroid_lat, storm.centroid_lon),
                storm.centroid_lat,
                storm.centroid_lon,
            )
        motion = storm.motion_to_target(target_lat, target_lon, distance_km=point[0])
        rank, reason = _candidate_rank(point, storm, motion)
        candidates.append((rank, point, storm, motion, reason))

    _, tracked_point, storm, motion, selected_reason = min(
        candidates, key=lambda item: item[0]
    )
    _, nearest_point, nearest_storm, _, _ = min(
        candidates, key=lambda item: float(item[1][0])
    )
    nearest_distance, impact_lat, impact_lon = nearest_point
    tracked_distance = float(tracked_point[0])
    nearest_centroid_distance = _haversine_km(
        target_lat,
        target_lon,
        nearest_storm.centroid_lat,
        nearest_storm.centroid_lon,
    )
    tracked_centroid_distance = _haversine_km(
        target_lat, target_lon, storm.centroid_lat, storm.centroid_lon
    )
    nearest_radar_dbz = [
        cell.max_dbz
        for cell in nearest_storm.radar_cells.values()
        if cell.max_dbz is not None
    ]
    tracking_status = storm.tracking_status
    status = "bevestigd" if tracking_status == "bevestigd" else "waargenomen"
    if tracking_status == "bevestigd" and storm.confidence in {"Matig", "Hoog"}:
        # Een gebogen, gevalideerd traject kan het target raken terwijl de
        # onmiddellijke snelheidsvector nog zijdelings of zelfs licht
        # weggericht is. In dat geval is de volledige trajectprojectie
        # betrouwbaarder dan de raaklijn van het laatste meetpunt.
        trajectory_passage = (
            motion.get("eta_minutes") is not None
            and motion.get("passage_classification") in {"raak", "rand"}
            and motion.get("closest_pass_minutes") is not None
            and 0 <= float(motion["closest_pass_minutes"]) <= 90
        )
        approach_speed = motion["approach_speed_kmh"]
        if trajectory_passage:
            status = "naderend"
        elif approach_speed is not None and approach_speed > 1.0:
            status = "naderend"
        elif approach_speed is not None and approach_speed < -1.0:
            status = "wegtrekkend"
        elif approach_speed is not None:
            status = "langs_trekkend"

    radar_dbz = [
        cell.max_dbz for cell in storm.radar_cells.values()
        if cell.max_dbz is not None
    ]
    forecast = _target_forecast(storm, motion)
    return {
        **base,
        "status": status,
        "storm_id": storm.storm_id,
        "nearest_precipitation_storm_id": nearest_storm.storm_id,
        "selected_reason": selected_reason,
        "tracking_status": tracking_status,
        # Backwards-compatible safety distance: always the nearest observed
        # precipitation edge, irrespective of which system has the best motion track.
        "distance_km": round(nearest_distance, 1),
        "nearest_precipitation_distance_km": round(nearest_distance, 1),
        "nearest_precipitation_centroid_distance_km": round(
            nearest_centroid_distance, 1
        ),
        "nearest_precipitation_max_dbz": (
            round(max(nearest_radar_dbz), 1) if nearest_radar_dbz else None
        ),
        "nearest_precipitation_frames": nearest_storm.consecutive_radar_frames,
        "tracked_system_distance_km": round(tracked_distance, 1),
        "tracked_system_centroid_distance_km": round(
            tracked_centroid_distance, 1
        ),
        "impact_lat": round(impact_lat, 4),
        "impact_lon": round(impact_lon, 4),
        "system_lat": round(storm.centroid_lat, 4),
        "system_lon": round(storm.centroid_lon, 4),
        "system_type": storm.system_type,
        "consecutive_radar_frames": storm.consecutive_radar_frames,
        "max_dbz": round(max(radar_dbz), 1) if radar_dbz else None,
        "heading_deg": round(storm.heading_deg, 0) if storm.heading_deg is not None else None,
        "speed_kmh": round(storm.speed_kmh, 1) if storm.speed_kmh is not None else None,
        "approach_speed_kmh": motion["approach_speed_kmh"],
        "moving_towards": motion["moving_towards"],
        "eta_minutes": motion["eta_minutes"],
        "eta_basis": motion.get("eta_basis"),
        "eta_reliable": (
            tracking_status == "bevestigd"
            and storm.confidence in {"Matig", "Hoog"}
            and motion["eta_minutes"] is not None
        ),
        "closest_pass_distance_km": motion["closest_pass_distance_km"],
        "closest_pass_minutes": motion["closest_pass_minutes"],
        "footprint_pass_distance_km": motion["footprint_pass_distance_km"],
        "footprint_pass_minutes": motion.get("footprint_pass_minutes"),
        "passage_classification": motion["passage_classification"],
        "passage_uncertainty_km": motion["passage_uncertainty_km"],
        "motion_confidence": storm.confidence,
        "motion_sample_count": getattr(storm, "motion_sample_count", 0),
        "motion_history_minutes": getattr(storm, "motion_history_minutes", 0.0),
        "motion_fit_quality": getattr(storm, "motion_fit_quality", 0.0),
        "motion_model": getattr(storm, "motion_model", "none"),
        "motion_basis": getattr(storm, "motion_basis", "unknown"),
        "motion_prediction_error_km": getattr(
            storm, "motion_prediction_error_km", None
        ),
        "motion_model_gain": getattr(storm, "motion_model_gain", 0.0),
        "motion_acceleration_kmh2": round(
            math.hypot(
                getattr(storm, "acceleration_east_kmh2", 0.0),
                getattr(storm, "acceleration_north_kmh2", 0.0),
            ),
            1,
        ),
        **forecast,
    }
