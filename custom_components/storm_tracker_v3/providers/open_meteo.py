"""Targetgerichte Open-Meteo-modelbegeleiding voor Storm Tracker V3.

Open-Meteo is een weermodelprovider, geen radar- of grondwaarheidsbron. Deze
provider vraagt uitsluitend de actuele targetlocaties op en levert per target
compacte neerslag-, convectie-, druk- en windbegeleiding. De resultaten mogen
daarom niet als ruimtelijke radarobservaties naar de Observation Fusion Engine
worden gestuurd en mogen lokale radar- of bliksemwaarschuwingen niet
onderdrukken.
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timezone
import logging
import math
import time
from typing import Any, AsyncIterator, Mapping, Optional

import aiohttp

_LOGGER = logging.getLogger(__name__)

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
TIMEOUT_S = 15
CACHE_TTL_S = 30 * 60
INITIAL_BACKOFF_S = 30 * 60
MAX_BACKOFF_S = 6 * 60 * 60
FORECAST_STEPS_15M = 13
OPERATIONAL_STEPS_15M = 7
FORECAST_HOURS = 6

# Negentien velden blijft bij het huidige targetaantal ruim onder de gratis
# Open-Meteo-limieten, ook wanneer bewegende targets de cache om de vijf
# minuten ongeldig maken. De bron blijft modelbegeleiding: ruwe waarden worden
# opgeslagen, maar nog niet rechtstreeks in waarschuwingen gewogen.
CURRENT_VARIABLES = (
    "precipitation",
)
MINUTELY_15_VARIABLES = (
    "precipitation",
    "rain",
    "showers",
    "cape",
    "lightning_potential",
    "wind_gusts_10m",
    "weather_code",
    "freezing_level_height",
)
HOURLY_VARIABLES = (
    "precipitation_probability",
    "pressure_msl",
    "lifted_index",
    "convective_inhibition",
    "wind_speed_850hPa",
    "wind_direction_850hPa",
    "wind_speed_700hPa",
    "wind_direction_700hPa",
    "relative_humidity_700hPa",
    "cloud_cover",
)
REQUESTED_VARIABLE_COUNT = (
    len(CURRENT_VARIABLES)
    + len(MINUTELY_15_VARIABLES)
    + len(HOURLY_VARIABLES)
)


def _utc_iso(timestamp: float | None) -> str | None:
    if timestamp is None:
        return None
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()


def _float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value if value is not None else default)
    except (TypeError, ValueError):
        return default


def _optional_float(value: Any) -> float | None:
    """Behoud ontbrekende modelwaarden als onbekend in plaats van droog/nul."""
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _series(
    section: Mapping[str, Any],
    key: str,
    limit: int,
) -> list[float | None]:
    raw = section.get(key, ())
    if not isinstance(raw, (list, tuple)):
        return []
    return [_optional_float(value) for value in raw[:limit]]


def _available(series: list[float | None]) -> list[float]:
    return [value for value in series if value is not None]


def _first(series: list[float | None]) -> float | None:
    return next((value for value in series if value is not None), None)


def _rounded(value: float | None, digits: int = 2) -> float | None:
    return round(value, digits) if value is not None else None


def _coordinate_key(lat: float, lon: float) -> tuple[float, float]:
    """Deel praktisch identieke targetlocaties op dezelfde modelcel.

    Twee decimalen is ongeveer een kilometer en blijft ruimschoots fijner dan
    de onderliggende weermodellen. Personen die samen thuis zijn veroorzaken
    daardoor maar één externe locatieopvraag.
    """
    return round(float(lat), 2), round(float(lon), 2)


def _normalise_targets(
    targets: Mapping[str, tuple[float, float] | Mapping[str, Any]],
) -> tuple[
    list[tuple[float, float]],
    dict[str, tuple[int, float, float]],
]:
    """Dedupliceer targets en onthoud de opgevraagde locatie per target."""
    coordinates: list[tuple[float, float]] = []
    coordinate_indices: dict[tuple[float, float], int] = {}
    target_indices: dict[str, tuple[int, float, float]] = {}
    for target_id, raw in sorted(targets.items()):
        if isinstance(raw, Mapping):
            lat = _float(raw.get("latitude"), float("nan"))
            lon = _float(raw.get("longitude"), float("nan"))
        else:
            lat, lon = float(raw[0]), float(raw[1])
        if not (-90 <= lat <= 90 and -180 <= lon <= 180):
            continue
        key = _coordinate_key(lat, lon)
        index = coordinate_indices.get(key)
        if index is None:
            index = len(coordinates)
            coordinate_indices[key] = index
            coordinates.append((lat, lon))
        target_indices[str(target_id)] = (index, lat, lon)
    return coordinates, target_indices


class OpenMeteoProvider:
    """Eén gedeelde broker voor alle actuele targets."""

    def __init__(self, session=None) -> None:
        self._session = session
        self._last_result = self._initial_result()
        self._last_fetch_monotonic = 0.0
        self._last_signature: tuple[tuple[float, float], ...] = ()
        self._backoff_until = 0.0
        self._backoff_seconds = INITIAL_BACKOFF_S
        self._fetch_sequence = 0
        self._last_attempt_ts: float | None = None
        self._last_success_ts: float | None = None
        self._last_http_status: int | None = None
        self._consecutive_failures = 0
        self._last_error: str | None = None
        self._last_targets_requested = 0
        self._last_unique_locations = 0

    @staticmethod
    def _initial_result() -> dict:
        return {
            "is_raining": None,
            "max_precipitation": None,
            "wet_points": None,
            "wet_now": None,
            "wet_forecast_90m": None,
            "total_points": 0,
            "targets_requested": 0,
            "targets_received": 0,
            "gear": "INITIALIZING",
            "provider_status": "initializing",
            "target_results": {},
            "fetch_sequence": 0,
            "last_attempt_at": None,
            "last_success_at": None,
            "data_age_seconds": None,
            "last_http_status": None,
            "consecutive_failures": 0,
            "last_error": None,
            "next_retry_at": None,
            "role": "model_guidance",
            "requested_variable_count": REQUESTED_VARIABLE_COUNT,
            "forecast_15m_steps": FORECAST_STEPS_15M,
            "forecast_hours": FORECAST_HOURS,
        }

    def set_callback(self, cb) -> None:
        return None

    def start(self) -> None:
        return None

    def stop(self) -> None:
        return None

    @property
    def last_result(self) -> dict:
        return self._runtime_result()

    @property
    def is_raining(self) -> bool | None:
        return self._last_result.get("is_raining")

    async def fetch(
        self,
        targets: Mapping[str, tuple[float, float] | Mapping[str, Any]],
    ) -> dict:
        """Haal één compacte forecast op voor alle unieke targetlocaties."""
        coordinates, target_indices = _normalise_targets(targets)
        self._last_targets_requested = len(target_indices)
        self._last_unique_locations = len(coordinates)
        signature = tuple(_coordinate_key(lat, lon) for lat, lon in coordinates)
        now_mono = time.monotonic()
        now_ts = time.time()

        if not coordinates:
            self._last_result = {
                **self._initial_result(),
                "gear": "NO_TARGETS",
                "provider_status": "no_targets",
            }
            return self._runtime_result()

        signature_unchanged = signature == self._last_signature
        if (
            signature_unchanged
            and self._last_fetch_monotonic
            and now_mono - self._last_fetch_monotonic < CACHE_TTL_S
        ):
            return self._runtime_result()
        if now_mono < self._backoff_until:
            return self._runtime_result()

        self._last_attempt_ts = now_ts
        params = {
            "latitude": ",".join(f"{lat:.5f}" for lat, _ in coordinates),
            "longitude": ",".join(f"{lon:.5f}" for _, lon in coordinates),
            "current": ",".join(CURRENT_VARIABLES),
            "minutely_15": ",".join(MINUTELY_15_VARIABLES),
            "forecast_minutely_15": str(FORECAST_STEPS_15M),
            "hourly": ",".join(HOURLY_VARIABLES),
            "forecast_hours": str(FORECAST_HOURS),
            "timezone": "UTC",
        }

        try:
            async with self._request(params) as response:
                self._last_http_status = int(response.status)
                if response.status == 429:
                    retry_after = _retry_after_seconds(response)
                    delay = min(
                        max(self._backoff_seconds, retry_after or 0),
                        MAX_BACKOFF_S,
                    )
                    self._backoff_until = now_mono + delay
                    self._backoff_seconds = min(delay * 2, MAX_BACKOFF_S)
                    self._record_failure("rate_limited")
                    _LOGGER.warning(
                        "OpenMeteoProvider: 429; volgende poging over %.0f minuten",
                        delay / 60,
                    )
                    return self._runtime_result()
                if response.status != 200:
                    self._record_failure(f"http_{response.status}")
                    _LOGGER.warning("OpenMeteoProvider: HTTP %d", response.status)
                    return self._runtime_result()
                payload = await response.json(content_type=None)
        except TimeoutError:
            self._record_failure("timeout")
            _LOGGER.warning(
                "OpenMeteoProvider: timeout na %d seconden", TIMEOUT_S
            )
            return self._runtime_result()
        except Exception as err:
            self._record_failure(type(err).__name__)
            _LOGGER.exception("OpenMeteoProvider: fout bij ophalen data")
            return self._runtime_result()

        items = payload if isinstance(payload, list) else [payload]
        if not items or not all(isinstance(item, Mapping) for item in items):
            self._record_failure("invalid_payload")
            return self._runtime_result()
        if len(items) != len(coordinates):
            self._record_failure(
                f"location_count_mismatch:{len(items)}/{len(coordinates)}"
            )
            return self._runtime_result()

        location_results = [
            _parse_location(item, requested_lat=lat, requested_lon=lon)
            for item, (lat, lon) in zip(items, coordinates)
        ]
        target_results = {}
        for target_id, (index, requested_lat, requested_lon) in target_indices.items():
            target_results[target_id] = {
                **location_results[index],
                "target_id": target_id,
                "requested_latitude": requested_lat,
                "requested_longitude": requested_lon,
                "shared_location_index": index,
            }

        current_values = [
            item["current_precipitation_mm"] for item in target_results.values()
        ]
        forecast_values = [
            item["forecast_90m_max_mm"] for item in target_results.values()
        ]
        known_current = [value for value in current_values if value is not None]
        known_forecast = [
            value for value in forecast_values if value is not None
        ]
        known_precipitation = known_current + known_forecast
        max_precipitation = (
            max(known_precipitation) if known_precipitation else None
        )
        wet_now = (
            sum(value > 0 for value in known_current)
            if known_current else None
        )
        wet_forecast = (
            sum(value > 0 for value in known_forecast)
            if known_forecast else None
        )
        any_wet = bool(wet_now or wet_forecast)

        self._fetch_sequence += 1
        self._last_success_ts = time.time()
        self._last_fetch_monotonic = now_mono
        self._last_signature = signature
        self._backoff_until = 0.0
        self._backoff_seconds = INITIAL_BACKOFF_S
        self._consecutive_failures = 0
        self._last_error = None
        self._last_result = {
            "is_raining": (
                wet_now > 0 if wet_now is not None else None
            ),
            "max_precipitation": _rounded(max_precipitation),
            "wet_points": (
                sum(
                    (current is not None and current > 0)
                    or (forecast is not None and forecast > 0)
                    for current, forecast in zip(
                        current_values, forecast_values
                    )
                )
                if known_precipitation else None
            ),
            "wet_now": wet_now,
            "wet_forecast_90m": wet_forecast,
            "total_points": len(coordinates),
            "targets_requested": len(target_indices),
            "targets_received": len(target_results),
            "gear": (
                "HIGH" if any_wet
                else "LOW" if known_precipitation
                else "PARTIAL"
            ),
            "provider_status": "ok",
            "target_results": target_results,
            "fetch_sequence": self._fetch_sequence,
            "role": "model_guidance",
            "requested_variable_count": REQUESTED_VARIABLE_COUNT,
            "forecast_15m_steps": FORECAST_STEPS_15M,
            "forecast_hours": FORECAST_HOURS,
        }
        _LOGGER.info(
            "Open-Meteo: %d targets via %d unieke locaties; nu %d nat; "
            "komende 90 min %d nat; max %.2f mm/15min",
            len(target_indices),
            len(coordinates),
            wet_now or 0,
            wet_forecast or 0,
            max_precipitation or 0.0,
        )
        return self._runtime_result()

    @asynccontextmanager
    async def _request(self, params: Mapping[str, str]) -> AsyncIterator[Any]:
        """Gebruik de HA-sessie, met een zelfstandige fallback voor tests."""
        timeout = aiohttp.ClientTimeout(total=TIMEOUT_S)
        if self._session is not None:
            async with self._session.get(
                OPEN_METEO_URL, params=params, timeout=timeout
            ) as response:
                yield response
            return
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(OPEN_METEO_URL, params=params) as response:
                yield response

    def _record_failure(self, error: str) -> None:
        self._consecutive_failures += 1
        self._last_error = error

    def _runtime_result(self) -> dict:
        result = dict(self._last_result)
        status = result.get("provider_status", "initializing")
        if self._last_error:
            status = self._last_error
        if self._last_success_ts is None and self._last_error:
            result["gear"] = self._last_error.upper()
        data_age = (
            max(0.0, time.time() - self._last_success_ts)
            if self._last_success_ts is not None else None
        )
        if data_age is not None and data_age > CACHE_TTL_S * 2:
            status = "stale"
            result["gear"] = "STALE"
        result.update({
            "provider_status": status,
            "last_attempt_at": _utc_iso(self._last_attempt_ts),
            "last_success_at": _utc_iso(self._last_success_ts),
            "data_age_seconds": (
                round(data_age, 1) if data_age is not None else None
            ),
            "last_http_status": self._last_http_status,
            "consecutive_failures": self._consecutive_failures,
            "last_error": self._last_error,
            "role": "model_guidance",
            "requested_variable_count": REQUESTED_VARIABLE_COUNT,
            "forecast_15m_steps": FORECAST_STEPS_15M,
            "forecast_hours": FORECAST_HOURS,
            "targets_requested": self._last_targets_requested,
            "total_points": self._last_unique_locations,
            "next_retry_at": (
                _utc_iso(
                    time.time() + max(0.0, self._backoff_until - time.monotonic())
                )
                if self._backoff_until > time.monotonic() else None
            ),
        })
        return result


def _parse_location(
    payload: Mapping[str, Any],
    *,
    requested_lat: float,
    requested_lon: float,
) -> dict:
    current_section = payload.get("current", {})
    if not isinstance(current_section, Mapping):
        current_section = {}
    current = _optional_float(current_section.get("precipitation"))
    minutely = payload.get("minutely_15", {})
    if not isinstance(minutely, Mapping):
        minutely = {}
    hourly = payload.get("hourly", {})
    if not isinstance(hourly, Mapping):
        hourly = {}

    precipitation = _series(
        minutely, "precipitation", FORECAST_STEPS_15M
    )
    rain = _series(minutely, "rain", FORECAST_STEPS_15M)
    showers = _series(minutely, "showers", FORECAST_STEPS_15M)
    cape = _series(minutely, "cape", FORECAST_STEPS_15M)
    lightning_potential = _series(
        minutely, "lightning_potential", FORECAST_STEPS_15M
    )
    gusts = _series(minutely, "wind_gusts_10m", FORECAST_STEPS_15M)
    weather_code = _series(
        minutely, "weather_code", FORECAST_STEPS_15M
    )
    freezing_level = _series(
        minutely, "freezing_level_height", FORECAST_STEPS_15M
    )

    precipitation_probability = _series(
        hourly, "precipitation_probability", FORECAST_HOURS
    )
    pressure_msl = _series(hourly, "pressure_msl", FORECAST_HOURS)
    lifted_index = _series(hourly, "lifted_index", FORECAST_HOURS)
    inhibition = _series(
        hourly, "convective_inhibition", FORECAST_HOURS
    )
    wind_speed_850 = _series(hourly, "wind_speed_850hPa", FORECAST_HOURS)
    wind_direction_850 = _series(
        hourly, "wind_direction_850hPa", FORECAST_HOURS
    )
    wind_speed_700 = _series(hourly, "wind_speed_700hPa", FORECAST_HOURS)
    wind_direction_700 = _series(
        hourly, "wind_direction_700hPa", FORECAST_HOURS
    )
    relative_humidity_700 = _series(
        hourly, "relative_humidity_700hPa", FORECAST_HOURS
    )
    cloud_cover = _series(hourly, "cloud_cover", FORECAST_HOURS)

    precipitation_values = _available(precipitation)
    operational_precipitation = precipitation[:OPERATIONAL_STEPS_15M]
    wet_indices = [
        index
        for index, value in enumerate(operational_precipitation)
        if value is not None and value > 0
    ]
    first_wet_minutes = wet_indices[0] * 15 if wet_indices else None
    available_variables = [
        key for key, values in {
            "precipitation": precipitation,
            "rain": rain,
            "showers": showers,
            "cape": cape,
            "lightning_potential": lightning_potential,
            "wind_gusts_10m": gusts,
            "weather_code": weather_code,
            "freezing_level_height": freezing_level,
            "precipitation_probability": precipitation_probability,
            "pressure_msl": pressure_msl,
            "lifted_index": lifted_index,
            "convective_inhibition": inhibition,
            "wind_speed_850hPa": wind_speed_850,
            "wind_direction_850hPa": wind_direction_850,
            "wind_speed_700hPa": wind_speed_700,
            "wind_direction_700hPa": wind_direction_700,
            "relative_humidity_700hPa": relative_humidity_700,
            "cloud_cover": cloud_cover,
        }.items()
        if _available(values)
    ]
    operational_values = _available(operational_precipitation)
    return {
        "role": "model_guidance",
        "current_precipitation_mm": _rounded(current),
        "forecast_90m_max_mm": _rounded(
            max(operational_values) if operational_values else None
        ),
        "forecast_90m_total_mm": _rounded(
            sum(operational_values) if operational_values else None
        ),
        "forecast_90m_wet_steps": (
            len(wet_indices) if operational_values else None
        ),
        "forecast_90m_first_wet_minutes": first_wet_minutes,
        "forecast_90m_precipitation_mm": [
            _rounded(value) for value in operational_precipitation
        ],
        "forecast_90m_rain_mm": [
            _rounded(value) for value in rain[:OPERATIONAL_STEPS_15M]
        ],
        "forecast_90m_showers_mm": [
            _rounded(value) for value in showers[:OPERATIONAL_STEPS_15M]
        ],
        "forecast_3h_max_mm": _rounded(
            max(precipitation_values) if precipitation_values else None
        ),
        "forecast_3h_total_mm": _rounded(
            sum(precipitation_values) if precipitation_values else None
        ),
        "forecast_3h_precipitation_mm": [
            _rounded(value) for value in precipitation
        ],
        "precipitation_probability_max_6h_percent": _rounded(
            max(_available(precipitation_probability))
            if _available(precipitation_probability) else None,
            1,
        ),
        "cape_max_3h_jkg": _rounded(
            max(_available(cape)) if _available(cape) else None,
            1,
        ),
        "lightning_potential_max_3h": _rounded(
            max(_available(lightning_potential))
            if _available(lightning_potential) else None,
            1,
        ),
        "wind_gusts_max_3h_kmh": _rounded(
            max(_available(gusts)) if _available(gusts) else None,
            1,
        ),
        "freezing_level_min_3h_m": _rounded(
            min(_available(freezing_level))
            if _available(freezing_level) else None,
            0,
        ),
        "pressure_msl_hpa": _rounded(_first(pressure_msl), 1),
        "lifted_index_min_6h": _rounded(
            min(_available(lifted_index))
            if _available(lifted_index) else None,
            1,
        ),
        "convective_inhibition_min_6h_jkg": _rounded(
            min(_available(inhibition)) if _available(inhibition) else None,
            1,
        ),
        "wind_850hpa_speed_kmh": _rounded(_first(wind_speed_850), 1),
        "wind_850hpa_direction_deg": _rounded(
            _first(wind_direction_850), 0
        ),
        "wind_700hpa_speed_kmh": _rounded(_first(wind_speed_700), 1),
        "wind_700hpa_direction_deg": _rounded(
            _first(wind_direction_700), 0
        ),
        "relative_humidity_700hpa_percent": _rounded(
            _first(relative_humidity_700), 1
        ),
        "cloud_cover_percent": _rounded(_first(cloud_cover), 1),
        "convective_guidance_available": any(
            _available(values)
            for values in (cape, lightning_potential, lifted_index, inhibition)
        ),
        "aloft_wind_guidance_available": bool(
            _first(wind_speed_700) is not None
            and _first(wind_direction_700) is not None
        ),
        "available_variables": available_variables,
        "requested_variable_count": REQUESTED_VARIABLE_COUNT,
        "forecast_15m_steps": FORECAST_STEPS_15M,
        "forecast_hours": FORECAST_HOURS,
        "model_latitude": _float(payload.get("latitude"), requested_lat),
        "model_longitude": _float(payload.get("longitude"), requested_lon),
        "elevation": payload.get("elevation"),
        "timezone": payload.get("timezone", "UTC"),
        "generation_time_ms": _optional_float(
            payload.get("generationtime_ms")
        ),
        "model_selection": "best_match",
        "fifteen_minute_data_may_be_interpolated": True,
    }


def _retry_after_seconds(response) -> Optional[float]:
    """Lees een numerieke Retry-After-header zonder aiohttp-internals."""
    headers = getattr(response, "headers", {}) or {}
    raw = headers.get("Retry-After") or headers.get("retry-after")
    if raw is None:
        return None
    try:
        return max(0.0, float(raw))
    except (TypeError, ValueError):
        return None
