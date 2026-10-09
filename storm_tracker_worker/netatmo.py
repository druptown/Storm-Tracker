"""Netatmo OAuth, bounded 20 km queries, caching and rate limiting."""
from __future__ import annotations

import json
import hashlib
import math
import os
import stat
import threading
import time
import urllib.parse
import urllib.request
import urllib.error

from .tracking_core.engine.bounded_payload import read_limited_stream
from .tracking_core.engine.observation_time_quality import (
    ObservationTimePolicy,
    assess_observation_time,
)

TOKEN_URL = "https://api.netatmo.com/oauth2/token"
PUBLIC_DATA_URL = "https://api.netatmo.com/api/getpublicdata"
QUERY_RADIUS_KM = 20.0
MIN_QUERY_INTERVAL_SECONDS = 5 * 60
LAST_GOOD_SECONDS = 15 * 60
HTTP_TIMEOUT_SECONDS = 15
BROKER_REFRESH_DEADLINE_BUDGET_SECONDS = HTTP_TIMEOUT_SECONDS * 2 + 2
TOKEN_MAX_RESPONSE_BYTES = 512 * 1024
TOKEN_STATE_MAX_BYTES = 16 * 1024
TOKEN_VALUE_MAX_CHARS = 4096
TOKEN_MAX_LIFETIME_SECONDS = 31 * 24 * 60 * 60
PUBLIC_DATA_MAX_RESPONSE_BYTES = 16 * 1024 * 1024
NETATMO_TIME_POLICY = ObservationTimePolicy(
    policy_id="netatmo-ground-observation-v1",
    maximum_age_seconds=LAST_GOOD_SECONDS,
    maximum_future_skew_seconds=120.0,
    maximum_clock_evidence_age_seconds=300.0,
)


def _read_json_response(response, *, maximum_bytes: int):
    headers = getattr(response, "headers", None)
    declared = headers.get("Content-Length") if headers is not None else None
    raw = read_limited_stream(
        response,
        maximum_bytes=maximum_bytes,
        declared_bytes=declared,
        require_declared_match=declared is not None,
    )
    return json.loads(raw.decode("utf-8"))


def _oauth_token_value(value: object) -> str:
    """Validate one opaque OAuth credential without echoing it on failure."""

    if (
        not isinstance(value, str)
        or not value
        or len(value) > TOKEN_VALUE_MAX_CHARS
        or not value.isascii()
        or any(ord(character) <= 0x20 or ord(character) == 0x7F for character in value)
    ):
        raise RuntimeError("Netatmo OAuth-response is ongeldig")
    return value


def _oauth_lifetime(value: object) -> float:
    if isinstance(value, bool):
        raise RuntimeError("Netatmo OAuth-response is ongeldig")
    try:
        lifetime = float(value)
    except (TypeError, ValueError, OverflowError) as err:
        raise RuntimeError("Netatmo OAuth-response is ongeldig") from err
    if (
        not math.isfinite(lifetime)
        or lifetime <= 0.0
        or lifetime > TOKEN_MAX_LIFETIME_SECONDS
    ):
        raise RuntimeError("Netatmo OAuth-response is ongeldig")
    return lifetime


def _strict_json_object(raw: str) -> dict:
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate key")
            result[key] = value
        return result

    value = json.loads(raw, object_pairs_hook=pairs)
    if not isinstance(value, dict):
        raise ValueError("not an object")
    return value


class NetatmoWorker:
    def __init__(self) -> None:
        self._client_id = os.getenv("STV3_NETATMO_CLIENT_ID", "")
        self._client_secret = os.getenv("STV3_NETATMO_CLIENT_SECRET", "")
        self._refresh_token = os.getenv("STV3_NETATMO_REFRESH_TOKEN", "")
        self._access_token = ""
        self._token_expires = 0.0
        self._cache: dict[str, dict] = {}
        self._lock = threading.RLock()
        self._credential_fingerprint = ""
        self._token_state_path = os.getenv(
            "STV3_NETATMO_TOKEN_STATE", "/data/netatmo_token_state.json"
        )
        self._token_state_durable = True
        self._token_state_load_error: str | None = None
        self._persisted_token_state = self._load_token_state()
        if self.configured():
            self._credential_fingerprint = hashlib.sha256(
                "\0".join((
                    self._client_id,
                    self._client_secret,
                    self._refresh_token,
                )).encode("utf-8")
            ).hexdigest()
            if (
                self._persisted_token_state.get(
                    "credential_fingerprint"
                ) == self._credential_fingerprint
                and self._persisted_token_state.get("refresh_token")
            ):
                self._refresh_token = str(
                    self._persisted_token_state["refresh_token"]
                )

    def configured(self) -> bool:
        return bool(
            self._client_id and self._client_secret and self._refresh_token
        )

    @staticmethod
    def _validate_credential_reference(payload: dict) -> None:
        """Reject secret-bearing MQTT jobs; credentials live in the add-on."""

        forbidden = tuple(
            key
            for key in (
                "client_id",
                "client_secret",
                "refresh_token",
                "access_token",
            )
            if key in payload
        )
        if forbidden:
            raise ValueError(
                "Netatmo-credentials mogen niet in een MQTT-jobpayload staan"
            )
        reference = payload.get("credential_ref")
        if reference not in {None, "worker_environment"}:
            raise ValueError("onbekende Netatmo credential reference")

    def _load_token_state(self) -> dict:
        try:
            metadata = os.lstat(self._token_state_path)
            if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
                raise ValueError("tokenstate is geen regulier bestand")
            if metadata.st_size > TOKEN_STATE_MAX_BYTES:
                raise ValueError("tokenstate overschrijdt de bytegrens")
            flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
            descriptor = os.open(self._token_state_path, flags)
            try:
                opened = os.fstat(descriptor)
                if (
                    not stat.S_ISREG(opened.st_mode)
                    or not os.path.samestat(metadata, opened)
                    or opened.st_size > TOKEN_STATE_MAX_BYTES
                ):
                    raise ValueError("tokenstate veranderde tijdens het openen")
                with os.fdopen(descriptor, "r", encoding="utf-8") as handle:
                    descriptor = -1
                    state = _strict_json_object(
                        handle.read(TOKEN_STATE_MAX_BYTES + 1)
                    )
            finally:
                if descriptor >= 0:
                    os.close(descriptor)
            if set(state) != {"credential_fingerprint", "refresh_token"}:
                raise ValueError("tokenstate heeft een onbekend schema")
            fingerprint = state["credential_fingerprint"]
            if (
                not isinstance(fingerprint, str)
                or len(fingerprint) != 64
                or any(character not in "0123456789abcdef" for character in fingerprint)
            ):
                raise ValueError("tokenstate fingerprint is ongeldig")
            _oauth_token_value(state["refresh_token"])
            return state
        except FileNotFoundError:
            return {}
        except (OSError, RuntimeError, ValueError, TypeError) as err:
            self._token_state_load_error = type(err).__name__
            return {}

    def _fsync_token_directory(self) -> None:
        """Commit the atomic directory entry on the Linux add-on host."""

        if os.name == "nt":
            return
        directory = os.path.dirname(self._token_state_path) or "."
        descriptor = os.open(
            directory,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
        )
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def _save_token_state(self) -> None:
        if not self._credential_fingerprint or not self._refresh_token:
            return
        refresh_token = _oauth_token_value(self._refresh_token)
        state = {
            "credential_fingerprint": self._credential_fingerprint,
            "refresh_token": refresh_token,
        }
        encoded = json.dumps(
            state,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
        if len(encoded) > TOKEN_STATE_MAX_BYTES:
            raise RuntimeError("Netatmo refreshcredentialstate is te groot")
        temporary = (
            f"{self._token_state_path}.{os.getpid()}."
            f"{threading.get_ident()}.tmp"
        )
        try:
            os.makedirs(
                os.path.dirname(self._token_state_path) or ".",
                exist_ok=True,
            )
            flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
            flags |= getattr(os, "O_NOFOLLOW", 0)
            descriptor = os.open(temporary, flags, 0o600)
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(encoded)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self._token_state_path)
            self._fsync_token_directory()
            self._persisted_token_state = state
            self._token_state_durable = True
        except OSError as err:
            self._token_state_durable = False
            try:
                os.unlink(temporary)
            except OSError:
                pass
            raise RuntimeError(
                "Netatmo refreshcredential kon niet duurzaam worden bewaard"
            ) from err

    def fetch(
        self,
        payload: dict,
        *,
        access_token: str | None = None,
    ) -> dict:
        with self._lock:
            self._validate_credential_reference(payload)
            if access_token is None and not self.configured():
                raise RuntimeError("Netatmo-workercredentials ontbreken")
            if access_token is not None:
                try:
                    access_token = _oauth_token_value(access_token)
                except RuntimeError as err:
                    raise ValueError(
                        "brokered Netatmo access token is ongeldig"
                    ) from err
            lat = float(payload["latitude"])
            lon = float(payload["longitude"])
            query_kind = str(payload.get("query_kind", "local"))
            query_key = str(
                payload.get("query_key")
                or f"{query_kind}:{lat:.2f},{lon:.2f}"
            )
            now = time.time()
            cached = self._cache.get(query_key)
            if (
                cached
                and now - cached["queried_at"] < MIN_QUERY_INTERVAL_SECONDS
            ):
                return {
                    **cached["result"],
                    "cache": "rate_limited",
                    "age_seconds": round(now - cached["queried_at"], 1),
                }
            try:
                token = access_token or self._token(now)
                fetched = self._query(token, lat, lon)
                observations = [
                    item
                    for item in fetched
                    if item.get("usable_for_current_decision", True)
                ]
                quarantined = [
                    item
                    for item in fetched
                    if not item.get("usable_for_current_decision", True)
                ]
                result = {
                    "method": "netatmo_worker_20km_v1",
                    "query_key": query_key,
                    "query_kind": query_kind,
                    "radius_km": QUERY_RADIUS_KM,
                    "queried_at": now,
                    "station_count": len(observations),
                    "observations": observations,
                    "quarantined_observation_count": len(quarantined),
                    "quarantined_observations": quarantined,
                    "cache": "fresh",
                }
                self._cache[query_key] = {
                    "queried_at": now,
                    "result": result,
                }
                return result
            except Exception:
                if cached and now - cached["queried_at"] <= LAST_GOOD_SECONDS:
                    return {
                        **cached["result"],
                        "cache": "last_good",
                        "age_seconds": round(now - cached["queried_at"], 1),
                    }
                raise

    def _token(self, now: float) -> str:
        if not self._token_state_durable:
            # A prior rotated credential was accepted by Netatmo but its
            # durable replace failed.  Retry that compensation before using
            # any cached access token or issuing another refresh request.
            self._save_token_state()
        if self._access_token and now < self._token_expires - 60:
            return self._access_token
        body = urllib.parse.urlencode({
            "grant_type": "refresh_token",
            "client_id": self._client_id,
            "client_secret": self._client_secret,
            "refresh_token": self._refresh_token,
        }).encode("ascii")
        request = urllib.request.Request(
            TOKEN_URL,
            data=body,
            headers={"Accept-Encoding": "identity"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(
                request, timeout=HTTP_TIMEOUT_SECONDS
            ) as response:
                result = _read_json_response(
                    response,
                    maximum_bytes=TOKEN_MAX_RESPONSE_BYTES,
                )
        except urllib.error.HTTPError as err:
            raise RuntimeError(f"Netatmo OAuth HTTP {err.code}") from err
        except (OSError, urllib.error.URLError) as err:
            raise RuntimeError("Netatmo OAuth transportfout") from err
        if not isinstance(result, dict):
            raise RuntimeError("Netatmo OAuth-response is ongeldig")
        access_token = _oauth_token_value(result.get("access_token"))
        lifetime = _oauth_lifetime(result.get("expires_in", 10800))
        rotated_refresh_token = result.get("refresh_token")
        if rotated_refresh_token is not None:
            rotated_refresh_token = _oauth_token_value(rotated_refresh_token)
        if rotated_refresh_token and rotated_refresh_token != self._refresh_token:
            self._refresh_token = rotated_refresh_token
            self._token_state_durable = False
            self._save_token_state()
        self._access_token = access_token
        self._token_expires = now + lifetime
        return self._access_token

    def broker_access_token(self, payload: dict, *, deadline: float) -> str:
        """Finish credential rotation durably in the non-killable parent."""

        with self._lock:
            self._validate_credential_reference(payload)
            if self._token_state_load_error is not None:
                raise RuntimeError(
                    "Netatmo refreshcredentialstate is onleesbaar"
                )
            if not self.configured():
                raise RuntimeError("Netatmo-workercredentials ontbreken")
            now = time.time()
            try:
                deadline = float(deadline)
            except (TypeError, ValueError, OverflowError) as err:
                raise TimeoutError("ongeldig Netatmo OAuth-deadlinebudget") from err
            if not math.isfinite(deadline):
                raise TimeoutError("ongeldig Netatmo OAuth-deadlinebudget")
            if not self._token_state_durable:
                self._save_token_state()
            if self._access_token and now < self._token_expires - 60:
                return self._access_token
            if (
                deadline - now
                <= BROKER_REFRESH_DEADLINE_BUDGET_SECONDS
            ):
                raise TimeoutError(
                    "onvoldoende deadlinebudget voor Netatmo OAuth-refresh"
                )
            # _token() persists a rotated refreshcredential, including fsync,
            # before it can return the access token to the killable child.
            return self._token(now)

    @staticmethod
    def _query(token: str, lat: float, lon: float) -> list[dict]:
        return NetatmoWorker.query_radius(
            token,
            lat,
            lon,
            QUERY_RADIUS_KM,
            include_quarantined=True,
        )

    @staticmethod
    def query_radius(
        token: str,
        lat: float,
        lon: float,
        radius_km: float,
        *,
        include_quarantined: bool = False,
    ) -> list[dict]:
        """Query a bounded survey radius; operational calls stay at 20 km."""
        radius_km = min(100.0, max(1.0, float(radius_km)))
        deg = radius_km / 111.0
        query = urllib.parse.urlencode({
            "lat_ne": lat + deg,
            "lon_ne": lon + deg,
            "lat_sw": lat - deg,
            "lon_sw": lon - deg,
            "filter": "true",
        })
        request = urllib.request.Request(
            f"{PUBLIC_DATA_URL}?{query}",
            headers={
                "Accept-Encoding": "identity",
                "Authorization": f"Bearer {token}",
            },
        )
        with urllib.request.urlopen(
            request, timeout=HTTP_TIMEOUT_SECONDS
        ) as response:
            data = _read_json_response(
                response,
                maximum_bytes=PUBLIC_DATA_MAX_RESPONSE_BYTES,
            )
        observations = _parse_observations(data, time.time())
        return [
            item for item in observations
            if _haversine_km(
                lat, lon, float(item["lat"]), float(item["lon"])
            ) <= radius_km
            and (
                include_quarantined
                or item.get("usable_for_current_decision", True)
            )
        ]


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    import math

    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    value = (
        math.sin(dp / 2) ** 2
        + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    )
    return 6371.0 * 2.0 * math.atan2(
        math.sqrt(value), math.sqrt(max(0.0, 1.0 - value))
    )


def _parse_observations(data: dict, observed_at: float) -> list[dict]:
    observations = []
    for item in data.get("body", []):
        try:
            location = item.get("place", {}).get("location", [0, 0])
            station = {
                "rain_mm": 0.0,
                "rain_5min": 0.0,
                "rain_60min": 0.0,
                "rain_capable": False,
                "wind_speed": None,
                "wind_angle": None,
                "gust_speed": None,
                "pressure": None,
                "temperature": None,
                "humidity": None,
            }
            value_timestamps = []
            rain_timestamps = []
            rain_measurements = []
            for raw_module_id, measure in item.get("measures", {}).items():
                module_id = str(raw_module_id)
                sensor_id = str(
                    measure.get("sensor_id")
                    or measure.get("_id")
                    or module_id
                )
                if "rain_live" in measure:
                    station["rain_capable"] = True
                    station["rain_mm"] = max(
                        station["rain_mm"],
                        float(measure.get("rain_live") or 0.0),
                    )
                if "rain_60min" in measure:
                    station["rain_capable"] = True
                    station["rain_60min"] = float(
                        measure.get("rain_60min") or 0.0
                    )
                    station["rain_5min"] = station["rain_60min"] / 12.0
                for source, target in (
                    ("wind_strength", "wind_speed"),
                    ("wind_angle", "wind_angle"),
                    ("gust_strength", "gust_speed"),
                ):
                    if measure.get(source) is not None:
                        station[target] = float(measure[source])
                result_values = measure.get("res", {})
                module_timestamps = []
                for raw_timestamp in result_values:
                    try:
                        parsed_timestamp = float(raw_timestamp)
                        value_timestamps.append(parsed_timestamp)
                        module_timestamps.append(parsed_timestamp)
                    except (TypeError, ValueError):
                        continue
                if (
                    "rain_live" in measure
                    or "rain_60min" in measure
                ):
                    rain_timestamp = None
                    rain_timestamp_field = None
                    for candidate_field in (
                        "rain_timeutc",
                        "rain_utc",
                    ):
                        if measure.get(candidate_field) is not None:
                            try:
                                rain_timestamp = float(
                                    measure[candidate_field]
                                )
                                rain_timestamp_field = candidate_field
                                break
                            except (TypeError, ValueError):
                                continue
                    if rain_timestamp is None and module_timestamps:
                        rain_timestamp = max(module_timestamps)
                        rain_timestamp_field = "res_latest"
                    if rain_timestamp is not None:
                        rain_timestamps.append(rain_timestamp)
                        value_timestamps.append(rain_timestamp)
                    rain_live = (
                        float(measure["rain_live"])
                        if measure.get("rain_live") is not None else None
                    )
                    rain_60min = (
                        float(measure["rain_60min"])
                        if measure.get("rain_60min") is not None else None
                    )
                    rain_measurements.append({
                        "module_id": module_id,
                        "sensor_id": sensor_id,
                        "measurement_timestamp": rain_timestamp,
                        "measurement_timestamp_field":
                            rain_timestamp_field,
                        "rain_live_mm_h": rain_live,
                        "rain_60min_mm": rain_60min,
                        "rain_60min_interval_start": (
                            rain_timestamp - 3600.0
                            if rain_timestamp is not None
                            and rain_60min is not None
                            else None
                        ),
                        "rain_60min_interval_end": (
                            rain_timestamp
                            if rain_timestamp is not None
                            and rain_60min is not None
                            else None
                        ),
                        "result_timestamps": sorted(module_timestamps)[-8:],
                        "measurement_types": list(
                            measure.get("type") or []
                        ),
                    })
                kinds = measure.get("type", [])
                latest = list(result_values.values())[-1] if result_values else []
                for kind in ("pressure", "temperature", "humidity"):
                    if kind in kinds:
                        index = kinds.index(kind)
                        if index < len(latest) and latest[index] is not None:
                            station[kind] = float(latest[index])
            measurement_timestamp = (
                max(rain_timestamps)
                if station["rain_capable"] and rain_timestamps
                else max(value_timestamps)
                if value_timestamps
                else observed_at
            )
            timestamp_estimated = not (
                rain_timestamps
                if station["rain_capable"]
                else value_timestamps
            )
            time_assessment = assess_observation_time(
                observed_at=measurement_timestamp,
                received_at=observed_at,
                evaluated_at=observed_at,
                policy=NETATMO_TIME_POLICY,
                clock_health=None,
            )
            usable_for_current_decision = (
                time_assessment.usable_for_current_decision
                and not timestamp_estimated
            )
            rain_present = float(station["rain_mm"] or 0.0) >= 0.1
            observations.append({
                "obs_type": "rain",
                "lat": float(location[1]),
                "lon": float(location[0]),
                "timestamp": measurement_timestamp,
                "received_at": observed_at,
                "station_id": str(item.get("_id", "")),
                "source": "netatmo",
                "precipitation_present": rain_present,
                "quantitative_precipitation": True,
                "measurement_kind": "personal_weather_station",
                "measurement_unit": "mm_h",
                "parser_version": "netatmo_pws_v4",
                "interpretation_reason": (
                    "rain_live_at_or_above_0.1_mm_h"
                    if rain_present else "rain_live_below_0.1_mm_h"
                ),
                "quality_flags": (
                    (
                        []
                        if not timestamp_estimated
                        else ["timestamp_estimated"]
                    )
                    + (
                        []
                        if time_assessment.usable_for_current_decision
                        else [f"time_quality_{time_assessment.state.value}"]
                    )
                ),
                "time_quality": (
                    "timestamp_estimated"
                    if timestamp_estimated
                    else time_assessment.state.value
                ),
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
                    "reasons": list(time_assessment.reasons)
                    + (
                        ["provider_measurement_timestamp_missing"]
                        if timestamp_estimated else []
                    ),
                },
                "usable_for_current_decision": usable_for_current_decision,
                "raw_payload": {
                    "station_id": str(item.get("_id", "")),
                    "module_count": len(item.get("measures", {})),
                    "measurement_timestamps": sorted(value_timestamps)[-8:],
                    "rain_measurement_timestamps": sorted(
                        rain_timestamps
                    )[-8:],
                    # Each module is a distinct provider fact.  The legacy
                    # aggregate rain_* fields above remain for transport
                    # compatibility and must not be used by the adapter.
                    "rain_measurements": rain_measurements,
                },
                **station,
            })
        except (IndexError, TypeError, ValueError):
            continue
    return observations


_NETATMO = NetatmoWorker()


def netatmo_fetch(job) -> dict:
    context = getattr(job, "execution_context", None)
    access_token = (
        context.get("netatmo_access_token")
        if isinstance(context, dict)
        else None
    )
    return _NETATMO.fetch(job.payload, access_token=access_token)


def broker_netatmo_access_token(payload: dict, *, deadline: float) -> str:
    """Parent-side OAuth broker used before hard-kill-safe query execution."""

    return _NETATMO.broker_access_token(payload, deadline=deadline)
