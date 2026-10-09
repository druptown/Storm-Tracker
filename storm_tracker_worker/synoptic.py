"""Synoptic ground observations with bounded caching and rate limiting."""
from __future__ import annotations

from datetime import datetime, timezone
import json
import os
import threading
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .tracking_core.engine.bounded_payload import read_limited_stream
from .tracking_core.engine.observation_time_quality import (
    ObservationTimePolicy,
    assess_observation_time,
)

SYNOPTIC_TIMESERIES_URL = "https://api.synopticdata.com/v2/stations/timeseries"
SYNOPTIC_TIMEOUT_SECONDS = 20
SYNOPTIC_MAX_RESPONSE_BYTES = 16 * 1024 * 1024
LOCAL_RADIUS_KM = 20.0
ROAMING_RADIUS_KM = 50.0
MAX_RADIUS_KM = ROAMING_RADIUS_KM
MAX_STATIONS = 50
MIN_QUERY_INTERVAL_SECONDS = 5 * 60
LAST_GOOD_SECONDS = 15 * 60
FRESH_SECONDS = 30 * 60
SYNOPTIC_TIME_POLICY = ObservationTimePolicy(
    policy_id="synoptic-ground-observation-v1",
    maximum_age_seconds=FRESH_SECONDS,
    maximum_future_skew_seconds=120.0,
    maximum_clock_evidence_age_seconds=300.0,
)


def _timestamp(value) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _precip_intervals(observations: dict) -> list[dict]:
    """Decode Synoptic's derived precipitation without losing its interval.

    With ``precip=1`` every value in ``precip_intervals`` is the amount
    between two consecutive ``date_time`` entries.  It is therefore neither
    an instantaneous rain state nor a rate at the interval end.
    """
    dates = observations.get("date_time")
    dates = dates if isinstance(dates, list) else []
    parsed_dates = [_timestamp(value) for value in dates]
    intervals = []
    for name, items in observations.items():
        if not str(name).startswith("precip_intervals") or not isinstance(items, list):
            continue
        for index, value in enumerate(items):
            if isinstance(value, (int, float)):
                interval_end = (
                    parsed_dates[index] if index < len(parsed_dates) else None
                )
                interval_start = (
                    parsed_dates[index - 1]
                    if index > 0 and index - 1 < len(parsed_dates)
                    else None
                )
                interval_complete = bool(
                    interval_start is not None
                    and interval_end is not None
                    and interval_end > interval_start
                )
                intervals.append({
                    "field": str(name),
                    "sensor_set": (
                        str(name).split("_set_", 1)[1]
                        if "_set_" in str(name)
                        else None
                    ),
                    "sample_index": index,
                    "value_mm": max(0.0, float(value)),
                    "interval_start": interval_start,
                    "interval_end": interval_end,
                    "interval_complete": interval_complete,
                    "interval_seconds": (
                        (interval_end - interval_start).total_seconds()
                        if interval_complete
                        else None
                    ),
                })
    return intervals


def _series_values(
    observations: dict,
    variable: str,
) -> list[tuple[float, datetime | None]]:
    """Lees alle sensorsets van één Synoptic-variabele uit."""
    dates = observations.get("date_time")
    dates = dates if isinstance(dates, list) else []
    values = []
    prefix = f"{variable}_set_"
    for name, items in observations.items():
        if not str(name).startswith(prefix) or not isinstance(items, list):
            continue
        for index, value in enumerate(items):
            if isinstance(value, (int, float)):
                measured = _timestamp(dates[index]) if index < len(dates) else None
                values.append((float(value), measured))
    return values


def _latest_fresh(
    observations: dict,
    variables: tuple[str, ...],
    now: datetime,
) -> tuple[float | None, datetime | None]:
    for variable in variables:
        values = [
            (value, measured)
            for value, measured in _series_values(observations, variable)
            if measured is not None
            and 0 <= (now - measured).total_seconds() <= FRESH_SECONDS
        ]
        if values:
            return max(values, key=lambda item: item[1])
    return None, None


def _query(
    latitude: float,
    longitude: float,
    *,
    radius_km: float,
    token: str,
    opener=urlopen,
    limit: int = MAX_STATIONS,
) -> dict:
    query = urlencode({
        "token": token,
        "radius": (
            f"{float(latitude):.5f},{float(longitude):.5f},"
            f"{float(radius_km) / 1.609344:.2f}"
        ),
        "vars": (
            "precip,air_temp,relative_humidity,wind_speed,"
            "wind_direction,wind_gust,sea_level_pressure,pressure"
        ),
        "precip": "1",
        # Keep the full Synoptic QC suite enabled and remove values that fail
        # a requested check.  Supplying ``qc=on`` without explicitly keeping
        # ``qc_remove_data=on`` would change the API default to returning
        # flagged values.
        "qc": "on",
        "qc_remove_data": "on",
        "qc_flags": "on",
        "qc_checks": "synopticlabs",
        "units": "metric,temp|C,speed|kph,pres|mb,precip|mm",
        "recent": "60",
        "limit": str(min(1000, max(1, int(limit)))),
    })
    request = Request(
        f"{SYNOPTIC_TIMESERIES_URL}?{query}",
        headers={
            "Accept": "application/json",
            "Accept-Encoding": "identity",
            "User-Agent": "StormTrackerWorker",
        },
    )
    with opener(request, timeout=SYNOPTIC_TIMEOUT_SECONDS) as response:
        headers = getattr(response, "headers", None)
        declared = (
            headers.get("Content-Length") if headers is not None else None
        )
        raw = read_limited_stream(
            response,
            maximum_bytes=SYNOPTIC_MAX_RESPONSE_BYTES,
            declared_bytes=declared,
            require_declared_match=declared is not None,
        )
        payload = json.loads(raw.decode("utf-8"))
    summary = payload.get("SUMMARY") if isinstance(payload, dict) else None
    response_code = int(summary.get("RESPONSE_CODE", 0)) if isinstance(summary, dict) else 0
    response_message = str(summary.get("RESPONSE_MESSAGE") or "") if isinstance(summary, dict) else ""
    # Synoptic categoriseert een geldige query zonder stations ook als code 2.
    # Dat is een operationeel leeg resultaat, geen workerfout.
    if response_code == 2 and "no station" in response_message.lower():
        payload["STATION"] = []
        return payload
    if not isinstance(summary, dict) or response_code != 1:
        raise RuntimeError(
            f"Synoptic API-fout {response_code}: {response_message[:160]}"
        )
    return payload


def _observations(payload: dict, queried_at: float) -> list[dict]:
    now = datetime.fromtimestamp(queried_at, tz=timezone.utc)
    result = []
    for station in (payload.get("STATION") or [])[:MAX_STATIONS]:
        try:
            observations = station.get("OBSERVATIONS") or {}
            intervals = _precip_intervals(observations)
            complete_intervals = [
                item
                for item in intervals
                if item["interval_complete"]
                and item["interval_end"] is not None
            ]
            recent = [
                item
                for item in complete_intervals
                if 0
                <= (now - item["interval_end"]).total_seconds()
                <= FRESH_SECONDS
            ]
            pressure, pressure_time = _latest_fresh(
                observations, ("sea_level_pressure", "pressure"), now
            )
            temperature, temperature_time = _latest_fresh(
                observations, ("air_temp",), now
            )
            humidity, humidity_time = _latest_fresh(
                observations, ("relative_humidity",), now
            )
            wind_speed, wind_time = _latest_fresh(
                observations, ("wind_speed",), now
            )
            wind_angle, wind_angle_time = _latest_fresh(
                observations, ("wind_direction",), now
            )
            gust_speed, gust_time = _latest_fresh(
                observations, ("wind_gust",), now
            )
            timestamps = [
                measured for measured in (
                    pressure_time, temperature_time, humidity_time, wind_time,
                    wind_angle_time, gust_time,
                ) if measured is not None
            ]
            reported_times = [
                measured
                for measured in (
                    _timestamp(value)
                    for value in (
                        observations.get("date_time")
                        if isinstance(observations.get("date_time"), list)
                        else []
                    )
                )
                if measured is not None
            ]
            if not complete_intervals and not timestamps and not reported_times:
                continue
            newest_precip = max(
                recent or complete_intervals,
                key=lambda item: (
                    item["interval_end"],
                    item["interval_start"],
                    item["field"],
                    item["sample_index"],
                ),
                default=None,
            )
            newest = (
                newest_precip["interval_end"]
                if newest_precip is not None
                else max(timestamps or reported_times)
            )
            precipitation_present = any(
                item["value_mm"] > 0.0
                for item in (recent or complete_intervals)
            )
            can_observe_dry = bool(recent)
            observed_at = newest.timestamp()
            time_assessment = assess_observation_time(
                observed_at=observed_at,
                received_at=queried_at,
                evaluated_at=queried_at,
                policy=SYNOPTIC_TIME_POLICY,
                clock_health=None,
            )
            quality_flags = []
            if intervals and not recent:
                quality_flags.append("no_complete_fresh_precip_interval")
            if any(
                item["interval_end"] is not None
                and not item["interval_complete"]
                for item in intervals
            ):
                quality_flags.append("incomplete_precip_interval")
            if not time_assessment.usable_for_current_decision:
                quality_flags.append(
                    f"time_quality_{time_assessment.state.value}"
                )
            result.append({
                "obs_type": "rain",
                "lat": float(station["LATITUDE"]),
                "lon": float(station["LONGITUDE"]),
                # Regenverificatie gebruikt de klok van het neerslagveld,
                # niet die van een mogelijk recentere temperatuurmeting.
                "timestamp": observed_at,
                "received_at": queried_at,
                "station_id": f"synoptic:{station['STID']}",
                "source": "synoptic",
                "rain_mm": None,
                "rain_5min": None,
                "rain_60min": None,
                "rain_capable": can_observe_dry,
                "can_observe_dry": can_observe_dry,
                "precipitation_present": precipitation_present,
                "quantitative_precipitation": False,
                "measurement_kind": "synoptic_precip_interval",
                "measurement_unit": "mm_interval",
                "measurement_interval_start": (
                    newest_precip["interval_start"].timestamp()
                    if newest_precip else None
                ),
                "measurement_interval_end": (
                    newest_precip["interval_end"].timestamp()
                    if newest_precip else None
                ),
                "parser_version": "synoptic_interval_v4",
                "interpretation_reason": (
                    "positive_recent_precip_interval"
                    if precipitation_present
                    else "no_complete_fresh_precip_interval"
                    if not can_observe_dry
                    else "no_positive_recent_precip_interval"
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
                "raw_payload": {
                    "stid": str(station["STID"]),
                    "precipitation_intervals": [
                        {
                            "field": item["field"],
                            "sensor_set": item["sensor_set"],
                            "sample_index": item["sample_index"],
                            "value_mm": item["value_mm"],
                            "interval_start": (
                                item["interval_start"].timestamp()
                                if item["interval_start"] is not None
                                else None
                            ),
                            "interval_end": (
                                item["interval_end"].timestamp()
                                if item["interval_end"] is not None
                                else None
                            ),
                            "interval_seconds": item["interval_seconds"],
                            "interval_complete": item["interval_complete"],
                        }
                        for item in intervals
                    ][-24:],
                    "selected_field": (
                        newest_precip["field"] if newest_precip else None
                    ),
                    "selected_sample_index": (
                        newest_precip["sample_index"]
                        if newest_precip else None
                    ),
                    "qc_flagged": station.get("QC_FLAGGED"),
                    "qc_summary": station.get("QC_SUMMARY"),
                    "sensor_variables": station.get("SENSOR_VARIABLES"),
                    "api_summary": payload.get("SUMMARY"),
                },
                "wind_speed": wind_speed,
                "wind_angle": wind_angle,
                "gust_speed": gust_speed,
                "pressure": pressure,
                "temperature": temperature,
                "humidity": humidity,
            })
        except (KeyError, TypeError, ValueError):
            continue
    return result


class SynopticWorker:
    """Own the credential, cache and bounded Synoptic query lifecycle."""

    def __init__(self, *, opener=urlopen) -> None:
        self._token = os.getenv("STV3_SYNOPTIC_TOKEN", "")
        self._opener = opener
        self._cache: dict[str, dict] = {}
        self._lock = threading.RLock()

    def fetch(self, payload: dict) -> dict:
        with self._lock:
            if not self._token:
                return {
                    "method": "synoptic_worker_v1",
                    "query_key": str(payload.get("query_key") or ""),
                    "query_kind": str(payload.get("query_kind") or "local"),
                    "radius_km": float(payload.get("radius_km", LOCAL_RADIUS_KM)),
                    "queried_at": datetime.now(timezone.utc).timestamp(),
                    "station_count": 0,
                    "fresh_station_count": 0,
                    "wet_station_count": 0,
                    "observations": [],
                    "quarantined_observation_count": 0,
                    "quarantined_observations": [],
                    "cache": "none",
                    "status": "credentials_missing",
                }
            latitude = float(payload["latitude"])
            longitude = float(payload["longitude"])
            query_kind = str(payload.get("query_kind") or "local")
            requested_radius = float(
                payload.get(
                    "radius_km",
                    ROAMING_RADIUS_KM
                    if query_kind == "roaming"
                    else LOCAL_RADIUS_KM,
                )
            )
            radius_km = min(MAX_RADIUS_KM, max(1.0, requested_radius))
            query_key = str(
                payload.get("query_key")
                or f"{query_kind}:{latitude:.2f},{longitude:.2f}:{radius_km:.0f}"
            )
            # Do not depend on a process-wide monkeypatch of ``time.time``:
            # worker tests and third-party libraries can replace it.  The
            # observation timestamps themselves are UTC datetimes.
            now = datetime.now(timezone.utc).timestamp()
            cached = self._cache.get(query_key)
            if cached and now - cached["queried_at"] < MIN_QUERY_INTERVAL_SECONDS:
                return {
                    **cached["result"],
                    "cache": "rate_limited",
                    "age_seconds": round(now - cached["queried_at"], 1),
                }
            try:
                raw = _query(
                    latitude,
                    longitude,
                    radius_km=radius_km,
                    token=self._token,
                    opener=self._opener,
                )
                parsed = _observations(raw, now)
                observations = [
                    item
                    for item in parsed
                    if item.get("usable_for_current_decision", True)
                ]
                quarantined = [
                    item
                    for item in parsed
                    if not item.get("usable_for_current_decision", True)
                ]
                wet = [
                    item for item in observations
                    if bool(item.get("precipitation_present"))
                ]
                result = {
                    "method": "synoptic_worker_v1",
                    "query_key": query_key,
                    "query_kind": query_kind,
                    "radius_km": radius_km,
                    "queried_at": now,
                    "station_count": len(observations),
                    "fresh_station_count": len(observations),
                    "wet_station_count": len(wet),
                    "observations": observations,
                    "quarantined_observation_count": len(quarantined),
                    "quarantined_observations": quarantined,
                    "cache": "fresh",
                }
                self._cache[query_key] = {"queried_at": now, "result": result}
                return result
            except Exception:
                if cached and now - cached["queried_at"] <= LAST_GOOD_SECONDS:
                    return {
                        **cached["result"],
                        "cache": "last_good",
                        "age_seconds": round(now - cached["queried_at"], 1),
                    }
                raise


_SYNOPTIC = SynopticWorker()


def synoptic_fetch(job) -> dict:
    return _SYNOPTIC.fetch(job.payload)


def probe_radius(
    latitude: float,
    longitude: float,
    *,
    radius_km: float = LOCAL_RADIUS_KM,
    token: str | None = None,
    opener=urlopen,
) -> dict:
    """Capability probe without returning IDs, coordinates or credentials."""
    token = token or os.getenv("STV3_SYNOPTIC_TOKEN", "")
    if not token:
        return {"status": "credentials_missing", "station_count": 0}
    radius_km = min(MAX_RADIUS_KM, max(1.0, float(radius_km)))
    try:
        payload = _query(
            latitude,
            longitude,
            radius_km=radius_km,
            token=token,
            opener=opener,
        )
        observations = _observations(
            payload, datetime.now(timezone.utc).timestamp()
        )
        current_observations = [
            item
            for item in observations
            if item.get("usable_for_current_decision", True)
        ]
    except HTTPError as err:
        return {
            "status": "http_error",
            "http_status": int(err.code),
            "station_count": 0,
        }
    except (
        URLError,
        TimeoutError,
        RuntimeError,
        ValueError,
        json.JSONDecodeError,
        UnicodeDecodeError,
    ):
        return {"status": "transport_error", "station_count": 0}
    return {
        "status": "ok",
        "station_count": len(current_observations),
        "precipitation_station_count": len(current_observations),
        "fresh_precipitation_station_count": len(current_observations),
        "quarantined_observation_count": (
            len(observations) - len(current_observations)
        ),
        "wet_station_count": sum(
            bool(item.get("precipitation_present"))
            for item in current_observations
        ),
        "radius_km": radius_km,
    }
