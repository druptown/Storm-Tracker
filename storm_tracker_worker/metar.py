"""Operational worldwide METAR precipitation observations."""
from __future__ import annotations

import csv
from datetime import datetime, timezone
import io
import math
import re
import threading
import time
from urllib.request import Request, urlopen

from .tracking_core.engine.bounded_payload import (
    decompress_limited,
    read_limited_stream,
)
from .tracking_core.engine.observation_time_quality import (
    ObservationTimePolicy,
    assess_observation_time,
)

METAR_CACHE_URL = "https://aviationweather.gov/data/cache/metars.cache.csv.gz"
MAX_COMPRESSED_BYTES = 16 * 1024 * 1024
MAX_UNCOMPRESSED_BYTES = 64 * 1024 * 1024
MAX_AGE_SECONDS = 120 * 60
MAX_RADIUS_KM = 75.0
REFRESH_SECONDS = 60
METAR_TIME_POLICY = ObservationTimePolicy(
    policy_id="metar-current-report-v1",
    maximum_age_seconds=MAX_AGE_SECONDS,
    maximum_future_skew_seconds=0.0,
    maximum_clock_evidence_age_seconds=0.0,
)
PRECIPITATION_PHENOMENA = (
    "DZ", "RA", "SN", "SG", "IC", "PL", "GR", "GS", "UP",
)
WEATHER_DESCRIPTORS = ("MI", "PR", "BC", "DR", "BL", "SH", "TS", "FZ")
_HOURLY_PRECIPITATION_RE = re.compile(r"(?:^|\s)P(\d{4})(?=\s|$)")


def _distance_km(lat1, lon1, lat2, lon2) -> float:
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = phi2 - phi1
    dlambda = math.radians(lon2 - lon1)
    value = (
        math.sin(dphi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    )
    return 6371.0088 * 2 * math.atan2(
        math.sqrt(value), math.sqrt(max(0.0, 1.0 - value))
    )


def _parse_time(value: str) -> float | None:
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except (AttributeError, ValueError):
        return None


def _parse_weather_group(raw_group: str) -> dict:
    """Parse one METAR present-weather group without inventing a rain volume."""

    raw = str(raw_group or "").strip().upper()
    value = raw
    intensity = None
    if value[:1] in {"+", "-"}:
        intensity, value = value[0], value[1:]
    proximity = value.startswith("VC")
    if proximity:
        value = value[2:]
    descriptor = next(
        (
            candidate
            for candidate in WEATHER_DESCRIPTORS
            if value.startswith(candidate)
        ),
        None,
    )
    if descriptor is not None:
        value = value[len(descriptor):]
    phenomena = tuple(
        value[index:index + 2]
        for index in range(0, len(value), 2)
        if (
            len(value[index:index + 2]) == 2
            and value[index:index + 2] in PRECIPITATION_PHENOMENA
        )
    )
    precipitation = bool(phenomena)
    return {
        "raw": raw,
        "intensity": intensity,
        "proximity": "vicinity" if proximity else "at_station",
        "descriptor": descriptor,
        "phenomena": list(phenomena),
        "precipitation": precipitation,
        "local_precipitation": precipitation and not proximity,
    }


def _parse_weather_groups(weather: str) -> tuple[dict, ...]:
    return tuple(
        _parse_weather_group(token)
        for group in str(weather or "").split()
        for token in group.split("/")
        if token.strip()
    )


def _hourly_precipitation_mm(raw_text: str) -> float | None:
    """Decode ``Prrrr`` as an accumulation, never as current weather."""

    match = _HOURLY_PRECIPITATION_RE.search(str(raw_text or "").upper())
    if match is None:
        return None
    return round(int(match.group(1)) * 0.01 * 25.4, 3)


def _present_weather_capability(raw_text: str, groups: tuple[dict, ...]) -> bool:
    """Whether absence of a local weather group can be used as dry evidence."""

    text = str(raw_text or "").upper()
    if "PWINO" in text:
        return False
    if groups:
        return True
    if " AUTO " not in f" {text} ":
        return True
    return " AO2 " in f" {text} "


def _is_wet(weather: str, _raw_text: str) -> bool:
    """Compatibility helper: only precipitation at the station is wet."""

    return any(
        group["local_precipitation"]
        for group in _parse_weather_groups(weather)
    )


def _decode_with_quarantine(
    payload: bytes,
    now: float,
) -> tuple[list[dict], list[dict]]:
    text = decompress_limited(
        payload,
        codec="gzip",
        maximum_compressed_bytes=MAX_COMPRESSED_BYTES,
        maximum_uncompressed_bytes=MAX_UNCOMPRESSED_BYTES,
    ).decode("utf-8-sig", "replace")
    observations = []
    quarantined = []
    for row in csv.DictReader(io.StringIO(text)):
        observed_at = _parse_time(row.get("observation_time", ""))
        if observed_at is None:
            continue
        time_assessment = assess_observation_time(
            observed_at=observed_at,
            received_at=now,
            evaluated_at=now,
            policy=METAR_TIME_POLICY,
            clock_health=None,
        )
        from_future = time_assessment.future_at_evaluation_seconds > 0.0
        stale = time_assessment.state.value == "stale"
        try:
            latitude = float(row["latitude"])
            longitude = float(row["longitude"])
        except (KeyError, TypeError, ValueError):
            continue
        weather = str(row.get("wx_string") or "")
        raw_text = str(row.get("raw_text") or "")
        weather_groups = _parse_weather_groups(weather)
        wet = any(
            group["local_precipitation"] for group in weather_groups
        )
        vicinity_wet = any(
            group["precipitation"] and not group["local_precipitation"]
            for group in weather_groups
        )
        present_weather_capable = _present_weather_capability(
            raw_text,
            weather_groups,
        )
        hourly_precipitation_mm = _hourly_precipitation_mm(raw_text)
        status_flags = tuple(
            flag
            for flag in ("PWINO", "PNO", "FZRANO", "TSNO")
            if flag in raw_text.upper()
        )
        quality_flags = list(status_flags)
        if "$" in raw_text:
            quality_flags.append("maintenance_required")
        if from_future:
            quality_flags.append("observation_from_future")
        if stale:
            quality_flags.append("observation_stale")
        observation = {
            "lat": latitude,
            "lon": longitude,
            "timestamp": observed_at,
            "station_id": f"metar:{row.get('station_id') or ''}",
            "source": "metar",
            "received_at": now,
            "rain_mm": None,
            "rain_5min": None,
            "rain_60min": None,
            "rain_capable": present_weather_capable,
            "can_observe_dry": present_weather_capable,
            "precipitation_present": wet,
            "precipitation_in_vicinity": vicinity_wet,
            "quantitative_precipitation": False,
            "raw_text": raw_text,
            "raw_payload": {
                "station_id": str(row.get("station_id") or ""),
                "observation_time": str(row.get("observation_time") or ""),
                "wx_string": weather,
                "weather_groups": list(weather_groups),
                "raw_text": raw_text,
                "sensor_status_flags": list(status_flags),
                "maintenance_required": "$" in raw_text,
                "hourly_precipitation_mm": hourly_precipitation_mm,
                "hourly_precipitation_interval":
                    "since_previous_metar_report",
                # The cache contains only the current report.  Its timestamp
                # is the interval end; the previous report time is unknown
                # and must never be invented as exactly one hour earlier.
                "hourly_precipitation_interval_start": None,
                "hourly_precipitation_interval_end": (
                    observed_at
                    if hourly_precipitation_mm is not None else None
                ),
            },
            "measurement_kind": "metar_present_weather",
            "measurement_unit": "categorical",
            "parser_version": "metar_wx_v4",
            "interpretation_reason": (
                f"current_weather:{weather}" if wet
                else f"vicinity_weather:{weather}" if vicinity_wet
                else "present_weather_sensor_unavailable"
                if not present_weather_capable
                else "no_current_precipitation_weather_group"
            ),
            "quality_flags": quality_flags,
            "time_quality": time_assessment.state.value,
            "time_quality_details": {
                "algorithm_version": time_assessment.algorithm_version,
                "policy_id": time_assessment.policy_id,
                "policy_fingerprint": time_assessment.policy_fingerprint,
                "observed_at": time_assessment.observed_at,
                "received_at": time_assessment.received_at,
                "evaluated_at": time_assessment.evaluated_at,
                "observation_age_seconds": (
                    time_assessment.observation_age_seconds
                ),
                "transport_delay_seconds": (
                    time_assessment.transport_delay_seconds
                ),
                "future_at_evaluation_seconds": (
                    time_assessment.future_at_evaluation_seconds
                ),
                "future_at_receipt_seconds": (
                    time_assessment.future_at_receipt_seconds
                ),
                "receipt_future_at_evaluation_seconds": (
                    time_assessment.receipt_future_at_evaluation_seconds
                ),
                "clock_evidence_id": time_assessment.clock_evidence_id,
                "reasons": list(time_assessment.reasons),
            },
            "usable_for_current_decision": (
                time_assessment.usable_for_current_decision
            ),
        }
        if not time_assessment.usable_for_current_decision:
            quarantined.append(observation)
        else:
            observations.append(observation)
    return observations, quarantined


def _decode(payload: bytes, now: float) -> list[dict]:
    """Compatibility view containing only observations valid for now."""

    return _decode_with_quarantine(payload, now)[0]


class MetarWorker:
    def __init__(self, *, opener=urlopen) -> None:
        self._opener = opener
        self._lock = threading.RLock()
        self._downloaded_at = 0.0
        self._observations: list[dict] = []
        self._quarantined_observations: list[dict] = []

    def fetch(self, payload: dict) -> dict:
        with self._lock:
            now = datetime.now(timezone.utc).timestamp()
            cache = "memory"
            if now - self._downloaded_at >= REFRESH_SECONDS:
                request = Request(
                    METAR_CACHE_URL,
                    headers={"User-Agent": "StormTrackerWorker/1.12"},
                )
                with self._opener(request, timeout=30) as response:
                    headers = getattr(response, "headers", None)
                    declared = (
                        headers.get("Content-Length")
                        if headers is not None else None
                    )
                    compressed = read_limited_stream(
                        response,
                        maximum_bytes=MAX_COMPRESSED_BYTES,
                        declared_bytes=declared,
                    )
                    (
                        self._observations,
                        self._quarantined_observations,
                    ) = _decode_with_quarantine(compressed, now)
                self._downloaded_at = now
                cache = "fresh"
            latitude = float(payload["latitude"])
            longitude = float(payload["longitude"])
            radius_km = min(
                MAX_RADIUS_KM, max(1.0, float(payload.get("radius_km", 50.0)))
            )
            nearby = [
                item for item in self._observations
                if _distance_km(
                    latitude, longitude, item["lat"], item["lon"]
                ) <= radius_km
            ]
            nearby_quarantined = [
                item for item in self._quarantined_observations
                if _distance_km(
                    latitude, longitude, item["lat"], item["lon"]
                ) <= radius_km
            ]
            return {
                "method": "metar_worker_v1",
                "query_key": str(payload.get("query_key") or ""),
                "query_kind": str(payload.get("query_kind") or "local"),
                "radius_km": radius_km,
                "queried_at": now,
                "station_count": len(nearby),
                "wet_station_count": sum(
                    bool(item["precipitation_present"]) for item in nearby
                ),
                "observations": nearby,
                "quarantined_observation_count": len(nearby_quarantined),
                "quarantined_observations": nearby_quarantined,
                "cache": cache,
            }


_METAR = MetarWorker()


def metar_fetch(job) -> dict:
    return _METAR.fetch(job.payload)
