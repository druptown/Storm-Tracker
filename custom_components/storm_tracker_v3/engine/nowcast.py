"""Compact operationeel neerslagbeeld voor sensoren en dashboards."""
from __future__ import annotations

import math


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
    if tracking_status == "bevestigd" and motion["moving_towards"] is True:
        status = "naderend"

    radar_dbz = [
        cell.max_dbz for cell in storm.radar_cells.values()
        if cell.max_dbz is not None
    ]
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
        "motion_confidence": storm.confidence,
    }
