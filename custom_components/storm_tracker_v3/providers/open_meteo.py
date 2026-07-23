"""Targetgerichte Open-Meteo-modelverwachtingen voor Storm Tracker V3.

Open-Meteo is een weermodelprovider, geen radarbron. Deze provider vraagt
uitsluitend de actuele targetlocaties op en levert per target een onafhankelijke
90-minutencontrole. De resultaten mogen daarom niet als ruimtelijke
radarobservaties naar de Observation Fusion Engine worden gestuurd.
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timezone
import logging
import time
from typing import Any, AsyncIterator, Mapping, Optional

import aiohttp

_LOGGER = logging.getLogger(__name__)

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
TIMEOUT_S = 15
CACHE_TTL_S = 30 * 60
INITIAL_BACKOFF_S = 30 * 60
MAX_BACKOFF_S = 6 * 60 * 60
FORECAST_STEPS_15M = 7


def _utc_iso(timestamp: float | None) -> str | None:
    if timestamp is None:
        return None
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()


def _float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value if value is not None else default)
    except (TypeError, ValueError):
        return default


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
            "current": "precipitation",
            "minutely_15": "precipitation,rain,showers",
            "forecast_minutely_15": str(FORECAST_STEPS_15M),
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
        max_precipitation = max(current_values + forecast_values, default=0.0)
        wet_now = sum(value > 0 for value in current_values)
        wet_forecast = sum(value > 0 for value in forecast_values)

        self._fetch_sequence += 1
        self._last_success_ts = time.time()
        self._last_fetch_monotonic = now_mono
        self._last_signature = signature
        self._backoff_until = 0.0
        self._backoff_seconds = INITIAL_BACKOFF_S
        self._consecutive_failures = 0
        self._last_error = None
        self._last_result = {
            "is_raining": wet_now > 0,
            "max_precipitation": round(max_precipitation, 2),
            "wet_points": sum(
                current > 0 or forecast > 0
                for current, forecast in zip(current_values, forecast_values)
            ),
            "wet_now": wet_now,
            "wet_forecast_90m": wet_forecast,
            "total_points": len(coordinates),
            "targets_requested": len(target_indices),
            "targets_received": len(target_results),
            "gear": "HIGH" if wet_now or wet_forecast else "LOW",
            "provider_status": "ok",
            "target_results": target_results,
            "fetch_sequence": self._fetch_sequence,
        }
        _LOGGER.info(
            "Open-Meteo: %d targets via %d unieke locaties; nu %d nat; "
            "komende 90 min %d nat; max %.2f mm/15min",
            len(target_indices),
            len(coordinates),
            wet_now,
            wet_forecast,
            max_precipitation,
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
    current = _float(payload.get("current", {}).get("precipitation"))
    minutely = payload.get("minutely_15", {})
    precipitation = [
        _float(value) for value in minutely.get("precipitation", [])
    ][:FORECAST_STEPS_15M]
    rain = [_float(value) for value in minutely.get("rain", [])][
        :FORECAST_STEPS_15M
    ]
    showers = [_float(value) for value in minutely.get("showers", [])][
        :FORECAST_STEPS_15M
    ]
    wet_indices = [
        index for index, value in enumerate(precipitation) if value > 0
    ]
    first_wet_minutes = wet_indices[0] * 15 if wet_indices else None
    return {
        "current_precipitation_mm": round(current, 2),
        "forecast_90m_max_mm": round(max(precipitation, default=0.0), 2),
        "forecast_90m_total_mm": round(sum(precipitation), 2),
        "forecast_90m_wet_steps": len(wet_indices),
        "forecast_90m_first_wet_minutes": first_wet_minutes,
        "forecast_90m_precipitation_mm": [
            round(value, 2) for value in precipitation
        ],
        "forecast_90m_rain_mm": [round(value, 2) for value in rain],
        "forecast_90m_showers_mm": [round(value, 2) for value in showers],
        "model_latitude": _float(payload.get("latitude"), requested_lat),
        "model_longitude": _float(payload.get("longitude"), requested_lon),
        "elevation": payload.get("elevation"),
        "timezone": payload.get("timezone", "UTC"),
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
