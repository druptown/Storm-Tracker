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
    }
    minutes = motion.get("closest_pass_minutes")
    if (
        storm.tracking_status != "bevestigd"
        or storm.confidence not in {"Matig", "Hoog"}
        or minutes is None
        or minutes < 0
        or minutes > 90
        or motion.get("passage_classification") is None
    ):
        return empty

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
        candidates.append((point, storm))

    # Een bevestigd systeem is operationeel belangrijker dan een toevallige
    # eenmalige echo. Gebruik een waarneming alleen als er nog niets bevestigd is.
    confirmed = [item for item in candidates if item[1].tracking_status == "bevestigd"]
    point, storm = min(confirmed or candidates, key=lambda item: item[0][0])
    distance, impact_lat, impact_lon = point
    motion = storm.motion_to_target(target_lat, target_lon, distance_km=distance)
    tracking_status = storm.tracking_status
    status = "bevestigd" if tracking_status == "bevestigd" else "waargenomen"
    if tracking_status == "bevestigd" and storm.confidence in {"Matig", "Hoog"}:
        approach_speed = motion["approach_speed_kmh"]
        if approach_speed is not None and approach_speed > 1.0:
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
        "tracking_status": tracking_status,
        "distance_km": round(distance, 1),
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
        "closest_pass_distance_km": motion["closest_pass_distance_km"],
        "closest_pass_minutes": motion["closest_pass_minutes"],
        "footprint_pass_distance_km": motion["footprint_pass_distance_km"],
        "passage_classification": motion["passage_classification"],
        "passage_uncertainty_km": motion["passage_uncertainty_km"],
        "motion_confidence": storm.confidence,
        **forecast,
    }
