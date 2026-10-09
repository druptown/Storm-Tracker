"""Worker-owned Open-Meteo request and compact model interpretation."""
from __future__ import annotations

import json
import math
from urllib.parse import urlencode

from .provider_radar import _download_with_headers

URL = "https://api.open-meteo.com/v1/forecast"
CURRENT = ("precipitation",)
MINUTELY = (
    "precipitation", "rain", "showers", "cape", "lightning_potential",
    "wind_gusts_10m", "weather_code", "freezing_level_height",
)
HOURLY = (
    "precipitation_probability", "pressure_msl", "lifted_index",
    "convective_inhibition", "wind_speed_850hPa", "wind_direction_850hPa",
    "wind_speed_700hPa", "wind_direction_700hPa",
    "relative_humidity_700hPa", "cloud_cover",
)
STEPS = 13
OPERATIONAL_STEPS = 7
HOURS = 6


def _float(value, default=None):
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return parsed if math.isfinite(parsed) else default


def _series(section, key, limit):
    values = section.get(key, ()) if isinstance(section, dict) else ()
    return [_float(item) for item in values[:limit]] if isinstance(values, list) else []


def _available(values):
    return [value for value in values if value is not None]


def _rounded(value, digits=2):
    return round(value, digits) if value is not None else None


def _first(values):
    return next((value for value in values if value is not None), None)


def _parse_location(payload, requested_lat, requested_lon):
    current_section = payload.get("current") or {}
    minutely = payload.get("minutely_15") or {}
    hourly = payload.get("hourly") or {}
    current = _float(current_section.get("precipitation"))
    precipitation = _series(minutely, "precipitation", STEPS)
    rain = _series(minutely, "rain", STEPS)
    showers = _series(minutely, "showers", STEPS)
    cape = _series(minutely, "cape", STEPS)
    lightning = _series(minutely, "lightning_potential", STEPS)
    gusts = _series(minutely, "wind_gusts_10m", STEPS)
    weather_code = _series(minutely, "weather_code", STEPS)
    freezing = _series(minutely, "freezing_level_height", STEPS)
    probability = _series(hourly, "precipitation_probability", HOURS)
    pressure = _series(hourly, "pressure_msl", HOURS)
    lifted = _series(hourly, "lifted_index", HOURS)
    inhibition = _series(hourly, "convective_inhibition", HOURS)
    speed850 = _series(hourly, "wind_speed_850hPa", HOURS)
    direction850 = _series(hourly, "wind_direction_850hPa", HOURS)
    speed700 = _series(hourly, "wind_speed_700hPa", HOURS)
    direction700 = _series(hourly, "wind_direction_700hPa", HOURS)
    humidity700 = _series(hourly, "relative_humidity_700hPa", HOURS)
    cloud = _series(hourly, "cloud_cover", HOURS)
    operational = precipitation[:OPERATIONAL_STEPS]
    operational_values = _available(operational)
    wet_indices = [index for index, value in enumerate(operational) if value is not None and value > 0]
    all_values = _available(precipitation)
    variables = {
        "precipitation": precipitation, "rain": rain, "showers": showers,
        "cape": cape, "lightning_potential": lightning,
        "wind_gusts_10m": gusts, "weather_code": weather_code,
        "freezing_level_height": freezing,
        "precipitation_probability": probability, "pressure_msl": pressure,
        "lifted_index": lifted, "convective_inhibition": inhibition,
        "wind_speed_850hPa": speed850, "wind_direction_850hPa": direction850,
        "wind_speed_700hPa": speed700, "wind_direction_700hPa": direction700,
        "relative_humidity_700hPa": humidity700, "cloud_cover": cloud,
    }
    return {
        "role": "model_guidance",
        "current_precipitation_mm": _rounded(current),
        "forecast_90m_max_mm": _rounded(max(operational_values) if operational_values else None),
        "forecast_90m_total_mm": _rounded(sum(operational_values) if operational_values else None),
        "forecast_90m_wet_steps": len(wet_indices) if operational_values else None,
        "forecast_90m_first_wet_minutes": wet_indices[0] * 15 if wet_indices else None,
        "forecast_90m_precipitation_mm": [_rounded(v) for v in operational],
        "forecast_90m_rain_mm": [_rounded(v) for v in rain[:OPERATIONAL_STEPS]],
        "forecast_90m_showers_mm": [_rounded(v) for v in showers[:OPERATIONAL_STEPS]],
        "forecast_3h_max_mm": _rounded(max(all_values) if all_values else None),
        "forecast_3h_total_mm": _rounded(sum(all_values) if all_values else None),
        "forecast_3h_precipitation_mm": [_rounded(v) for v in precipitation],
        "precipitation_probability_max_6h_percent": _rounded(max(_available(probability)) if _available(probability) else None, 1),
        "cape_max_3h_jkg": _rounded(max(_available(cape)) if _available(cape) else None, 1),
        "lightning_potential_max_3h": _rounded(max(_available(lightning)) if _available(lightning) else None, 1),
        "wind_gusts_max_3h_kmh": _rounded(max(_available(gusts)) if _available(gusts) else None, 1),
        "freezing_level_min_3h_m": _rounded(min(_available(freezing)) if _available(freezing) else None, 0),
        "pressure_msl_hpa": _rounded(_first(pressure), 1),
        "lifted_index_min_6h": _rounded(min(_available(lifted)) if _available(lifted) else None, 1),
        "convective_inhibition_min_6h_jkg": _rounded(min(_available(inhibition)) if _available(inhibition) else None, 1),
        "wind_850hpa_speed_kmh": _rounded(_first(speed850), 1),
        "wind_850hpa_direction_deg": _rounded(_first(direction850), 0),
        "wind_700hpa_speed_kmh": _rounded(_first(speed700), 1),
        "wind_700hpa_direction_deg": _rounded(_first(direction700), 0),
        "relative_humidity_700hpa_percent": _rounded(_first(humidity700), 1),
        "cloud_cover_percent": _rounded(_first(cloud), 1),
        "convective_guidance_available": any(_available(v) for v in (cape, lightning, lifted, inhibition)),
        "aloft_wind_guidance_available": _first(speed700) is not None and _first(direction700) is not None,
        "available_variables": [key for key, values in variables.items() if _available(values)],
        "requested_variable_count": len(CURRENT) + len(MINUTELY) + len(HOURLY),
        "forecast_15m_steps": STEPS, "forecast_hours": HOURS,
        "model_latitude": _float(payload.get("latitude"), requested_lat),
        "model_longitude": _float(payload.get("longitude"), requested_lon),
        "elevation": payload.get("elevation"), "timezone": payload.get("timezone", "UTC"),
        "generation_time_ms": _float(payload.get("generationtime_ms")),
        "model_selection": "best_match", "fifteen_minute_data_may_be_interpolated": True,
    }


def open_meteo_fetch(job) -> dict:
    targets = dict(job.payload.get("targets") or {})
    coordinates = []
    indices = {}
    seen = {}
    for target_id, value in sorted(targets.items()):
        lat, lon = float(value["latitude"]), float(value["longitude"])
        if not (-90 <= lat <= 90 and -180 <= lon <= 180):
            continue
        key = (round(lat, 2), round(lon, 2))
        if key not in seen:
            seen[key] = len(coordinates)
            coordinates.append((lat, lon))
        indices[target_id] = (seen[key], lat, lon)
    if not coordinates:
        return {"provider_status": "no_targets", "gear": "NO_TARGETS", "target_results": {}}
    params = {
        "latitude": ",".join(f"{lat:.5f}" for lat, _ in coordinates),
        "longitude": ",".join(f"{lon:.5f}" for _, lon in coordinates),
        "current": ",".join(CURRENT), "minutely_15": ",".join(MINUTELY),
        "forecast_minutely_15": str(STEPS), "hourly": ",".join(HOURLY),
        "forecast_hours": str(HOURS), "timezone": "UTC",
    }
    raw = _download_with_headers(
        f"{URL}?{urlencode(params)}", maximum_bytes=8 * 1024 * 1024
    )
    payload = json.loads(raw.decode("utf-8"))
    items = payload if isinstance(payload, list) else [payload]
    if len(items) != len(coordinates):
        raise ValueError("Open-Meteo locatieaantal komt niet overeen")
    locations = [_parse_location(item, *coordinate) for item, coordinate in zip(items, coordinates)]
    target_results = {
        target_id: {
            **locations[index], "target_id": target_id,
            "requested_latitude": lat, "requested_longitude": lon,
            "shared_location_index": index,
        }
        for target_id, (index, lat, lon) in indices.items()
    }
    current = [value["current_precipitation_mm"] for value in target_results.values()]
    forecast = [value["forecast_90m_max_mm"] for value in target_results.values()]
    known_current, known_forecast = _available(current), _available(forecast)
    known = known_current + known_forecast
    wet_now = sum(value > 0 for value in known_current) if known_current else None
    wet_forecast = sum(value > 0 for value in known_forecast) if known_forecast else None
    return {
        "provider_status": "ok", "role": "model_guidance",
        "is_raining": wet_now > 0 if wet_now is not None else None,
        "max_precipitation": _rounded(max(known) if known else None),
        "wet_points": sum((a is not None and a > 0) or (b is not None and b > 0) for a, b in zip(current, forecast)) if known else None,
        "wet_now": wet_now, "wet_forecast_90m": wet_forecast,
        "total_points": len(coordinates), "targets_requested": len(indices),
        "targets_received": len(target_results),
        "gear": "HIGH" if (wet_now or wet_forecast) else "LOW" if known else "PARTIAL",
        "target_results": target_results,
        "requested_variable_count": len(CURRENT) + len(MINUTELY) + len(HOURLY),
        "forecast_15m_steps": STEPS, "forecast_hours": HOURS,
    }
