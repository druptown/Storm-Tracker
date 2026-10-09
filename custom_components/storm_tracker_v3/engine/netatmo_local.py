"""Locatiegerichte samenvatting van Netatmo-regenmeters."""
from __future__ import annotations

import math
import time

RAIN_THRESHOLD_MM_H = 0.1
DEFAULT_LOCAL_RADIUS_KM = 10.0


def _distance_km(lat1, lon1, lat2, lon2) -> float:
    phi1, phi2 = math.radians(float(lat1)), math.radians(float(lat2))
    dphi = phi2 - phi1
    dlambda = math.radians(float(lon2) - float(lon1))
    value = (
        math.sin(dphi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    )
    return 6371.0088 * 2 * math.atan2(
        math.sqrt(value), math.sqrt(max(0.0, 1 - value))
    )


def build_local_netatmo_summary(
    observations,
    target_lat,
    target_lon,
    *,
    local_radius_km: float = DEFAULT_LOCAL_RADIUS_KM,
    now: float | None = None,
) -> dict:
    """Geef lokale regenaanwijzing en onafhankelijke bevestiging."""
    observations = list(observations or [])
    if not observations:
        return {
            "netatmo_local_status": "onvoldoende_data",
            "netatmo_local_radius_km": local_radius_km,
            "netatmo_local_stations": 0,
            "netatmo_local_raining_stations": 0,
            "netatmo_local_confirmed": False,
        }

    measured = sorted(
        (
            (
                _distance_km(
                    target_lat, target_lon, observation.lat, observation.lon
                ),
                observation,
            )
            for observation in observations
        ),
        key=lambda item: item[0],
    )
    local = [
        item for item in measured if item[0] <= float(local_radius_km)
    ]
    local_raining = [
        item
        for item in local
        if float(item[1].rain_mm or 0.0) >= RAIN_THRESHOLD_MM_H
    ]
    all_raining = [
        item
        for item in measured
        if float(item[1].rain_mm or 0.0) >= RAIN_THRESHOLD_MM_H
    ]
    rain_values = [
        float(item[1].rain_mm or 0.0) for item in local_raining
    ]
    confirmed = len(local_raining) >= 2
    status = (
        "regen_bevestigd"
        if confirmed
        else "regen_aanwijzing"
        if local_raining
        else "geen_lokale_regenmeter_nat"
        if local
        else "geen_lokale_stations"
    )
    nearest_rain = all_raining[0] if all_raining else None
    nearest_station = measured[0]
    newest = max(float(item[1].timestamp) for item in measured)
    current_time = time.time() if now is None else float(now)
    return {
        "netatmo_local_status": status,
        "netatmo_local_radius_km": local_radius_km,
        "netatmo_local_stations": len(local),
        "netatmo_local_raining_stations": len(local_raining),
        "netatmo_local_confirmed": confirmed,
        "netatmo_local_max_rain_mm_h": (
            round(max(rain_values), 2) if rain_values else 0.0
        ),
        "netatmo_local_average_rain_mm_h": (
            round(sum(rain_values) / len(rain_values), 2)
            if rain_values else 0.0
        ),
        "netatmo_nearest_station_id": nearest_station[1].station_id,
        "netatmo_nearest_station_distance_km": round(
            nearest_station[0], 1
        ),
        "netatmo_nearest_rain_station_id": (
            nearest_rain[1].station_id if nearest_rain else None
        ),
        "netatmo_nearest_rain_distance_km": (
            round(nearest_rain[0], 1) if nearest_rain else None
        ),
        "netatmo_nearest_rain_mm_h": (
            round(float(nearest_rain[1].rain_mm or 0.0), 2)
            if nearest_rain else None
        ),
        "netatmo_data_age_seconds": round(
            max(0.0, current_time - newest), 1
        ),
    }
