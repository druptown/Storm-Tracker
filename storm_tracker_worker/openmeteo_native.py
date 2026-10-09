"""Exact Open-Meteo forecast capture and strict response verification.

The worker stores the exact JSON response before inspecting its structure.
Input is the pseudonymous target-query projection produced by
``AcquisitionQueryAuthority.openmeteo_shadow_query_batch``; target IDs never
cross the child boundary.  This module makes no rain, threat, model-skill or
operational-admission claim.  A separate parent-side decoder may consume the
verified bytes; raw capture remains independent from that interpretation.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import math
from pathlib import Path
import re
import threading
import time
from typing import Any, Callable, Mapping
from urllib.parse import urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

from .raw_artifacts import (
    NATIVE_CAPTURE_STAGE,
    NATIVE_REPRESENTATION,
    RAW_ARTIFACT_SCHEMA_VERSION,
    RawArtifactCaptureReceipt,
    RawArtifactReceipt,
    RawArtifactStore,
)


CONTRACT = "stv3-worker-openmeteo-exact-raw-shadow-v1"
QUERY_BATCH_CONTRACT = "stv3-openmeteo-target-query-batch-v1"
OPENMETEO_ORIGIN = "https://api.open-meteo.com"
OPENMETEO_PATH = "/v1/forecast"
OPENMETEO_URL = OPENMETEO_ORIGIN + OPENMETEO_PATH

CURRENT = ("precipitation",)
MINUTELY_15 = (
    "precipitation",
    "rain",
    "showers",
    "cape",
    "lightning_potential",
    "wind_gusts_10m",
    "weather_code",
    "freezing_level_height",
)
HOURLY = (
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
EXPECTED_CURRENT_UNITS = {
    "interval": "seconds",
    "precipitation": "mm",
    "time": "unixtime",
}
EXPECTED_MINUTELY_15_UNITS = {
    "cape": "J/kg",
    "freezing_level_height": "m",
    "lightning_potential": "J/kg",
    "precipitation": "mm",
    "rain": "mm",
    "showers": "mm",
    "time": "unixtime",
    "weather_code": "wmo code",
    "wind_gusts_10m": "km/h",
}
EXPECTED_HOURLY_UNITS = {
    "cloud_cover": "%",
    "convective_inhibition": "J/kg",
    "lifted_index": "undefined",
    "precipitation_probability": "%",
    "pressure_msl": "hPa",
    "relative_humidity_700hPa": "%",
    "time": "unixtime",
    "wind_direction_700hPa": "°",
    "wind_direction_850hPa": "°",
    "wind_speed_700hPa": "km/h",
    "wind_speed_850hPa": "km/h",
}
# Open-Meteo changed the dimensionless ``lifted_index`` unit marker from the
# literal ``undefined`` to an empty string in September 2026. Preserve both
# exact upstream representations so retained captures remain replayable while
# new responses continue to pass a closed unit contract.
SUPPORTED_LIFTED_INDEX_UNITS = frozenset({"", "undefined"})
FORECAST_MINUTELY_15 = 13
FORECAST_HOURS = 6
MAX_QUERY_COUNT = 64
MAX_RESPONSE_BYTES = 8 * 1024 * 1024
MAX_URL_CHARS = 8192
MAX_HEADER_CHARS = 8192
HTTP_READ_CHUNK_BYTES = 64 * 1024
MAX_HTTP_READ_CHUNKS = (MAX_RESPONSE_BYTES // HTTP_READ_CHUNK_BYTES) + 2
HTTP_IO_TIMEOUT_SECONDS = 20.0
CAPTURE_DEADLINE_SECONDS = 45.0

OPENMETEO_RAW_SHARE_ROOT = Path("/share/storm_tracker_worker")
OPENMETEO_RAW_ARTIFACT_ROOT = OPENMETEO_RAW_SHARE_ROOT / "raw" / "openmeteo"
OPENMETEO_RESPONSE_PRODUCT = "openmeteo_forecast_native_json"
OPENMETEO_CAPTURE_PRODUCT = "openmeteo_query_bound_forecast_capture"
OPENMETEO_COLLECTOR = "openmeteo_public_forecast_exact_raw"
OPENMETEO_COLLECTOR_VERSION = "worker-openmeteo-exact-raw-v1"
OPENMETEO_STRUCTURE_INSPECTOR_VERSION = "openmeteo-forecast-shape-v1"
OPENMETEO_ADMISSION_STATUS = "exact_raw_model_semantics_not_admitted"

# Immutable, canonical policy evidence for the free public endpoint used by
# this capture.  The URLs and published limits were reviewed on 2026-08-09;
# activation must revalidate mutable upstream terms.  This is provenance, not
# legal advice and not an assertion of an unlimited retention right.
OPENMETEO_ATTRIBUTION_POLICY_CANONICAL = (
    b'{"api_key_authorized":false,"attribution_display_required":true,'
    b'"contract":"stv3-openmeteo-attribution-authority-v1",'
    b'"data_license_name":"CC BY 4.0",'
    b'"data_license_url":"https://creativecommons.org/licenses/by/4.0/",'
    b'"free_api_commercial_use_authorized":false,"legal_advice":false,'
    b'"modification_notice":"Exact API bytes are preserved; Storm Tracker adds '
    b'hashes and structural metadata.",'
    b'"personal_home_automation_scope":"authorized_noncommercial_example",'
    b'"provider_coordinate_log_notice":"Webserver logs may contain request '
    b'coordinates.",'
    b'"provider_credit":"Open-Meteo",'
    b'"provider_license_notice_url":"https://open-meteo.com/en/license",'
    b'"provider_log_retention_days":90,'
    b'"provider_log_third_party_sharing":false,'
    b'"provider_pricing_url":"https://open-meteo.com/en/pricing",'
    b'"provider_source_url":"https://open-meteo.com/",'
    b'"provider_terms_url":"https://open-meteo.com/en/terms",'
    b'"published_call_limits":{"day":10000,"hour":5000,"minute":600,'
    b'"month":300000,"provider_metering_rules_apply":true},'
    b'"retention_right":"not_established",'
    b'"service_scope":"free_noncommercial_personal_home_automation",'
    b'"terms_reviewed_on":"2026-08-09",'
    b'"upstream_model_credit_resolution_required":true}'
)
OPENMETEO_ATTRIBUTION_POLICY_SHA256 = hashlib.sha256(
    OPENMETEO_ATTRIBUTION_POLICY_CANONICAL
).hexdigest()
OPENMETEO_ATTRIBUTION_POLICY_ID = (
    f"oma1:{OPENMETEO_ATTRIBUTION_POLICY_SHA256}"
)

_JOB_ID_RE = re.compile(
    r"^acq:[a-z2-7]{8}(?::z:[a-z2-7]{8}){5}:z:[a-z2-7]{4}$"
)
_QUERY_KEY_RE = re.compile(
    r"^aqt:[a-z2-7]{8}(?::z:[a-z2-7]{8}){5}:z:[a-z2-7]{4}$"
)
_HEADER_NAME_RE = re.compile(r"^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$")
_SINGLETON_RESPONSE_HEADERS = frozenset({
    "content-encoding",
    "content-length",
    "content-type",
})
_RAW_STORE: RawArtifactStore | None = None
_RAW_STORE_LOCK = threading.Lock()
_CAPTURE_LOCK = threading.Lock()


class OpenMeteoNativeError(RuntimeError):
    """Base class for bounded, payload-free capture errors."""


class OpenMeteoNativeContractError(OpenMeteoNativeError):
    """The request or response is outside the closed contract."""


class OpenMeteoNativeCapacityError(OpenMeteoNativeError):
    """The transport or structural budget was exceeded."""


class OpenMeteoNativeTransportError(OpenMeteoNativeError):
    """A detail-free transport failure that cannot expose target coordinates."""


class OpenMeteoNativeCorruptionError(OpenMeteoNativeError):
    """Durable bytes no longer match their exact receipts."""


@dataclass(frozen=True, slots=True)
class OpenMeteoHttpResponse:
    payload: bytes
    content_type: str
    received_at: float


@dataclass(frozen=True, slots=True)
class OpenMeteoStructureIdentity:
    location_count: int
    minutely_steps: int
    hourly_steps: int
    variable_count: int
    structure_sha256: str


class _RejectRedirects(HTTPRedirectHandler):
    def redirect_request(self, *_args: Any, **_kwargs: Any):
        raise OpenMeteoNativeContractError("Open-Meteo redirect is niet toegestaan")


def _open_without_redirects(request: Request, *, timeout: float):
    return build_opener(ProxyHandler({}), _RejectRedirects()).open(
        request, timeout=timeout
    )


def _remaining(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if not math.isfinite(remaining) or remaining <= 0:
        raise TimeoutError("Open-Meteo capturedeadline is verstreken")
    return remaining


def _finite_coordinate(value: object, *, name: str, low: float, high: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise OpenMeteoNativeContractError(f"Open-Meteo {name} is ongeldig")
    try:
        result = float(value)
    except (OverflowError, TypeError, ValueError):
        raise OpenMeteoNativeContractError(
            f"Open-Meteo {name} is ongeldig"
        ) from None
    if not math.isfinite(result) or not low <= result <= high:
        raise OpenMeteoNativeContractError(f"Open-Meteo {name} is ongeldig")
    return 0.0 if result == 0.0 else result


def _validate_job(job: object) -> tuple[str, tuple[dict[str, object], ...]]:
    job_id = getattr(job, "job_id", None)
    if not isinstance(job_id, str) or _JOB_ID_RE.fullmatch(job_id) is None:
        raise OpenMeteoNativeContractError(
            "Open-Meteo capturejob mist een geldige job_id"
        )
    payload = getattr(job, "payload", None)
    if type(payload) is not dict or set(payload) != {
        "contract",
        "operational_handler_compatible",
        "queries",
        "target_resolution_required",
    }:
        raise OpenMeteoNativeContractError(
            "Open-Meteo handlerpayload wijkt af van authorityprojectie"
        )
    if (
        payload["contract"] != QUERY_BATCH_CONTRACT
        or payload["operational_handler_compatible"] is not False
        or payload["target_resolution_required"] is not True
        or type(payload["queries"]) is not list
        or not 1 <= len(payload["queries"]) <= MAX_QUERY_COUNT
    ):
        raise OpenMeteoNativeContractError(
            "Open-Meteo querybatch is niet fail-closed"
        )
    queries: list[dict[str, object]] = []
    seen_keys: set[str] = set()
    for raw in payload["queries"]:
        if type(raw) is not dict or set(raw) != {
            "latitude", "longitude", "query_key", "query_kind"
        }:
            raise OpenMeteoNativeContractError(
                "Open-Meteo targetquery heeft een onbekend veld"
            )
        key = raw["query_key"]
        kind = raw["query_kind"]
        if (
            not isinstance(key, str)
            or _QUERY_KEY_RE.fullmatch(key) is None
            or key in seen_keys
            or not isinstance(kind, str)
            or kind not in {"local", "roaming"}
        ):
            raise OpenMeteoNativeContractError(
                "Open-Meteo targetquery-identiteit is ongeldig"
            )
        seen_keys.add(key)
        queries.append({
            "latitude": _finite_coordinate(
                raw["latitude"], name="latitude", low=-90.0, high=90.0
            ),
            "longitude": _finite_coordinate(
                raw["longitude"], name="longitude", low=-180.0, high=180.0
            ),
            "query_key": key,
            "query_kind": kind,
        })
    # The authority sorts by pseudonymous target-reference SHA, not by the
    # independently derived query key.  Preserve that authenticated order;
    # the parent/child payload hash binds it and the response list uses it.
    return job_id, tuple(queries)


def _request_parameters(queries: tuple[dict[str, object], ...]) -> dict[str, str]:
    return {
        "latitude": ",".join(f"{float(item['latitude']):.5f}" for item in queries),
        "longitude": ",".join(f"{float(item['longitude']):.5f}" for item in queries),
        "current": ",".join(CURRENT),
        "minutely_15": ",".join(MINUTELY_15),
        "forecast_minutely_15": str(FORECAST_MINUTELY_15),
        "hourly": ",".join(HOURLY),
        "forecast_hours": str(FORECAST_HOURS),
        "timezone": "UTC",
        "timeformat": "unixtime",
        "precipitation_unit": "mm",
        "wind_speed_unit": "kmh",
        # Preserve the selected numerical model grid instead of applying the
        # API's elevation downscaling or land-cell preference.
        # The API requires one elevation selector per coordinate in a batch.
        # A scalar ``nan`` is accepted for one target but rejected once the
        # worker projects multiple Life360/fictitious targets together.
        "elevation": ",".join("nan" for _query in queries),
        "cell_selection": "nearest",
    }


def _request_identity(
    queries: tuple[dict[str, object], ...], params: Mapping[str, str]
) -> tuple[str, str]:
    body = {
        "attribution_policy_id": OPENMETEO_ATTRIBUTION_POLICY_ID,
        "contract": "stv3-openmeteo-request-identity-v1",
        "parameters": dict(sorted(params.items())),
        "query_keys": [str(item["query_key"]) for item in queries],
        "query_kinds": [str(item["query_kind"]) for item in queries],
    }
    encoded = json.dumps(
        body, ensure_ascii=True, allow_nan=False, sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest(), urlencode(params, safe=",")


def _raw_store(injected: RawArtifactStore | None = None) -> RawArtifactStore:
    if injected is not None:
        injected.initialize()
        return injected
    share = OPENMETEO_RAW_SHARE_ROOT.resolve(strict=False)
    root = OPENMETEO_RAW_ARTIFACT_ROOT.resolve(strict=False)
    if root == share or share not in root.parents:
        raise OpenMeteoNativeContractError(
            "Open-Meteo raw-artifactpad ontsnapt uit worker-share"
        )
    global _RAW_STORE
    with _RAW_STORE_LOCK:
        if _RAW_STORE is None:
            candidate = RawArtifactStore(root)
            candidate.initialize()
            _RAW_STORE = candidate
        return _RAW_STORE


def _bounded_header(headers: object, name: str, *, maximum: int = 256) -> str:
    getter = getattr(headers, "get", None)
    if not callable(getter):
        raise OpenMeteoNativeContractError(
            "Open-Meteo responseheaders ontbreken"
        )
    try:
        raw = getter(name)
        value = "" if raw is None else str(raw).strip()
    except Exception:
        raise OpenMeteoNativeContractError(
            f"Open-Meteo {name}-header is ongeldig"
        ) from None
    if len(value) > maximum or any(
        ord(char) < 0x20 or ord(char) == 0x7F for char in value
    ):
        raise OpenMeteoNativeContractError(
            f"Open-Meteo {name}-header is ongeldig"
        )
    return value


def _validate_response_headers(headers: object) -> None:
    items = getattr(headers, "items", None)
    if not callable(items):
        raise OpenMeteoNativeContractError(
            "Open-Meteo responseheaders ontbreken"
        )
    seen_singletons: set[str] = set()
    total_chars = 0
    try:
        entries = items()
        for raw_name, raw_value in entries:
            name = str(raw_name)
            value = str(raw_value)
            if (
                _HEADER_NAME_RE.fullmatch(name) is None
                or any(ord(char) < 0x20 or ord(char) == 0x7F for char in value)
            ):
                raise OpenMeteoNativeContractError(
                    "Open-Meteo responseheader is ongeldig"
                )
            total_chars += len(name) + len(value) + 4
            if total_chars > MAX_HEADER_CHARS:
                raise OpenMeteoNativeCapacityError(
                    "Open-Meteo responseheaders overschrijden de grens"
                )
            lowered = name.lower()
            if lowered in _SINGLETON_RESPONSE_HEADERS:
                if lowered in seen_singletons:
                    raise OpenMeteoNativeContractError(
                        "Open-Meteo response bevat dubbele framingheaders"
                    )
                seen_singletons.add(lowered)
    except (OpenMeteoNativeError, TimeoutError):
        raise
    except Exception:
        raise OpenMeteoNativeContractError(
            "Open-Meteo responseheaders zijn ongeldig"
        ) from None


def _declared_length(headers: object) -> int | None:
    value = _bounded_header(headers, "Content-Length", maximum=32)
    if not value:
        return None
    if not value.isascii() or not value.isdecimal():
        raise OpenMeteoNativeContractError(
            "Open-Meteo Content-Length is ongeldig"
        )
    result = int(value)
    if not 0 < result <= MAX_RESPONSE_BYTES:
        raise OpenMeteoNativeCapacityError(
            "Open-Meteo Content-Length overschrijdt de grens"
        )
    return result


def _tighten_stream_timeout(response: object, *, timeout: float) -> None:
    """Best-effort tightening of urllib's socket to the shrinking deadline."""

    frontier = [response]
    visited: set[int] = set()
    for _depth in range(4):
        following: list[object] = []
        for candidate in frontier:
            identity = id(candidate)
            if identity in visited:
                continue
            visited.add(identity)
            try:
                setter = getattr(candidate, "settimeout", None)
            except Exception:
                setter = None
            if callable(setter):
                try:
                    setter(timeout)
                except Exception:
                    raise OpenMeteoNativeTransportError(
                        "Open-Meteo read-timeout kon niet worden begrensd"
                    ) from None
                return
            for attribute in ("fp", "raw", "_sock", "sock"):
                try:
                    child = getattr(candidate, attribute, None)
                except Exception:
                    continue
                if child is not None:
                    following.append(child)
        frontier = following
        if not frontier:
            return


def _read_body(response: object, *, declared: int | None, deadline: float) -> bytes:
    reader = getattr(response, "read1", None)
    if not callable(reader):
        reader = getattr(response, "read", None)
    if not callable(reader):
        raise OpenMeteoNativeContractError(
            "Open-Meteo respons heeft geen bytestream"
        )
    chunks: list[bytes] = []
    total = 0
    for _chunk_index in range(MAX_HTTP_READ_CHUNKS):
        remaining = _remaining(deadline)
        _tighten_stream_timeout(
            response,
            timeout=min(HTTP_IO_TIMEOUT_SECONDS, remaining),
        )
        requested = min(
            HTTP_READ_CHUNK_BYTES,
            MAX_RESPONSE_BYTES + 1 - total,
        )
        try:
            chunk = reader(requested)
        except TimeoutError:
            raise TimeoutError("Open-Meteo response-read is verlopen") from None
        except Exception:
            raise OpenMeteoNativeTransportError(
                "Open-Meteo response-read is mislukt"
            ) from None
        if not chunk:
            break
        if not isinstance(chunk, (bytes, bytearray, memoryview)):
            raise OpenMeteoNativeContractError(
                "Open-Meteo responsstream leverde geen bytes"
            )
        immutable = bytes(chunk)
        if len(immutable) > requested:
            raise OpenMeteoNativeCapacityError(
                "Open-Meteo responschunk overschrijdt de grens"
            )
        chunks.append(immutable)
        total += len(immutable)
        if total > MAX_RESPONSE_BYTES or (declared is not None and total > declared):
            raise OpenMeteoNativeCapacityError(
                "Open-Meteo respons overschrijdt de bytegrens"
            )
        if declared is not None and total == declared:
            break
    else:
        raise OpenMeteoNativeCapacityError(
            "Open-Meteo respons overschrijdt de chunkgrens"
        )
    _remaining(deadline)
    payload = b"".join(chunks)
    if not payload or (declared is not None and len(payload) != declared):
        raise OpenMeteoNativeContractError(
            "Open-Meteo responslengte wijkt af van HTTP"
        )
    return payload


def _request(
    *, query_string: str, deadline: float, opener: Callable[..., Any] | None
) -> OpenMeteoHttpResponse:
    url = OPENMETEO_URL + "?" + query_string
    if len(url) > MAX_URL_CHARS:
        raise OpenMeteoNativeCapacityError("Open-Meteo request-URL is te groot")
    request = Request(
        url,
        headers={
            "Accept": "application/json",
            "Accept-Encoding": "identity",
            "User-Agent": "Storm-Tracker-Worker/OpenMeteo-ExactRaw-v1",
        },
        method="GET",
    )
    open_call = opener or _open_without_redirects
    try:
        response = open_call(
            request,
            timeout=min(HTTP_IO_TIMEOUT_SECONDS, _remaining(deadline)),
        )
    except (OpenMeteoNativeError, TimeoutError):
        raise
    except Exception:
        raise OpenMeteoNativeTransportError(
            "Open-Meteo transport kon geen response openen"
        ) from None
    completed = False
    try:
        try:
            status = getattr(response, "status", None)
            if status is None:
                getcode = getattr(response, "getcode", None)
                status = getcode() if callable(getcode) else None
            if isinstance(status, bool) or not isinstance(status, int) or status != 200:
                raise OpenMeteoNativeContractError(
                    "Open-Meteo HTTP-status is niet 200"
                )
            final_url = str(getattr(response, "geturl", lambda: url)())
            parsed = urlsplit(final_url)
            if (
                parsed.scheme != "https"
                or parsed.hostname != "api.open-meteo.com"
                or parsed.port not in {None, 443}
                or parsed.path != OPENMETEO_PATH
                or final_url != url
            ):
                raise OpenMeteoNativeContractError(
                    "Open-Meteo response-URL wijkt af van de exacte request"
                )
            headers = getattr(response, "headers", None)
            _validate_response_headers(headers)
            content_type = _bounded_header(
                headers, "Content-Type", maximum=256
            ).split(";", 1)[0].lower()
            if content_type != "application/json":
                raise OpenMeteoNativeContractError(
                    "Open-Meteo respons is geen JSON"
                )
            if _bounded_header(headers, "Content-Encoding", maximum=64):
                raise OpenMeteoNativeContractError(
                    "Open-Meteo content-encoding is niet exact identity"
                )
            payload = _read_body(
                response, declared=_declared_length(headers), deadline=deadline
            )
            received_at = time.time()
            if not math.isfinite(received_at) or received_at < 0:
                raise OpenMeteoNativeContractError(
                    "Open-Meteo ontvangstklok is ongeldig"
                )
            completed = True
            return OpenMeteoHttpResponse(payload, content_type, received_at)
        except (OpenMeteoNativeError, TimeoutError):
            raise
        except Exception:
            raise OpenMeteoNativeContractError(
                "Open-Meteo responsemetadata is ongeldig"
            ) from None
    finally:
        closer = getattr(response, "close", None)
        if callable(closer):
            try:
                closer()
            except Exception:
                if completed:
                    raise OpenMeteoNativeTransportError(
                        "Open-Meteo response kon niet veilig sluiten"
                    ) from None


def _strict_json(payload: bytes) -> Any:
    def reject_constant(value: str) -> None:
        raise ValueError(f"non-finite JSON constant {value}")

    def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate JSON key")
            result[key] = value
        return result

    try:
        return json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=unique_object,
            parse_constant=reject_constant,
        )
    except (RecursionError, UnicodeDecodeError, json.JSONDecodeError, ValueError):
        raise OpenMeteoNativeContractError(
            "Open-Meteo respons is geen strikte JSON"
        ) from None


def _attribution_policy() -> dict[str, Any]:
    """Return an isolated copy of the hashed attribution authority."""

    decoded = _strict_json(OPENMETEO_ATTRIBUTION_POLICY_CANONICAL)
    if type(decoded) is not dict:
        raise OpenMeteoNativeCorruptionError(
            "Open-Meteo attributionauthority is ongeldig"
        )
    return decoded


def _finite_number(value: object, *, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise OpenMeteoNativeContractError(f"Open-Meteo {label} is ongeldig")
    try:
        result = float(value)
    except (OverflowError, TypeError, ValueError):
        raise OpenMeteoNativeContractError(
            f"Open-Meteo {label} is ongeldig"
        ) from None
    if not math.isfinite(result):
        raise OpenMeteoNativeContractError(f"Open-Meteo {label} is ongeldig")
    return result


def _unix_timestamp(value: object, *, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise OpenMeteoNativeContractError(f"Open-Meteo {label} is ongeldig")
    return value


def _time_axis(
    section: Mapping[str, Any], *, count: int, step: int, label: str
) -> tuple[int, ...]:
    times = section.get("time")
    if type(times) is not list or len(times) != count:
        raise OpenMeteoNativeContractError(
            f"Open-Meteo {label}-tijdsas heeft een verkeerde lengte"
        )
    parsed = tuple(
        _unix_timestamp(value, label=f"{label}.time") for value in times
    )
    if parsed[0] % step != 0:
        raise OpenMeteoNativeContractError(
            f"Open-Meteo {label}-tijdsas is niet UTC-uitgelijnd"
        )
    if any(later - earlier != step for earlier, later in zip(parsed, parsed[1:])):
        raise OpenMeteoNativeContractError(
            f"Open-Meteo {label}-tijdsas heeft een verkeerde cadence"
        )
    return parsed


def _variable_axes(
    section: Mapping[str, Any], names: tuple[str, ...], *, count: int, label: str
) -> None:
    if set(section) != {"time", *names}:
        raise OpenMeteoNativeContractError(
            f"Open-Meteo {label}-variabelenset wijkt af"
        )
    for name in names:
        values = section.get(name)
        if type(values) is not list or len(values) != count:
            raise OpenMeteoNativeContractError(
                f"Open-Meteo {label}.{name} heeft een verkeerde lengte"
            )
        for value in values:
            if value is not None:
                _finite_number(value, label=f"{label}.{name}")


def _inspect(payload: bytes, *, query_count: int) -> OpenMeteoStructureIdentity:
    decoded = _strict_json(payload)
    if query_count == 1:
        if type(decoded) is not dict:
            raise OpenMeteoNativeContractError(
                "Open-Meteo enkelpuntsrespons is geen object"
            )
        items = [decoded]
    else:
        if type(decoded) is not list:
            raise OpenMeteoNativeContractError(
                "Open-Meteo meerpuntsrespons is geen lijst"
            )
        items = decoded
    if len(items) != query_count or any(type(item) is not dict for item in items):
        raise OpenMeteoNativeContractError(
            "Open-Meteo locatieaantal wijkt af van request"
        )
    rows: list[dict[str, Any]] = []
    required = {
        "current", "current_units", "elevation", "generationtime_ms",
        "hourly", "hourly_units", "latitude", "longitude", "minutely_15",
        "minutely_15_units", "timezone", "timezone_abbreviation",
        "utc_offset_seconds",
    }
    location_id_mode = (
        "indexed"
        if query_count > 1
        and all(
            "location_id" not in item if index == 0
            else item.get("location_id") == index
            for index, item in enumerate(items)
        )
        else "legacy"
    )
    if location_id_mode == "legacy" and any(
        "location_id" in item for item in items
    ):
        raise OpenMeteoNativeContractError(
            "Open-Meteo locatie-indexen wijken af van requestvolgorde"
        )
    for index, item in enumerate(items):
        expected_fields = (
            required | {"location_id"}
            if location_id_mode == "indexed" and index > 0
            else required
        )
        if set(item) != expected_fields:
            raise OpenMeteoNativeContractError(
                "Open-Meteo responsvelden wijken af van contract"
            )
        _finite_coordinate(item["latitude"], name="model latitude", low=-90, high=90)
        _finite_coordinate(item["longitude"], name="model longitude", low=-180, high=180)
        _finite_number(item["elevation"], label="elevation")
        _finite_number(item["generationtime_ms"], label="generationtime_ms")
        if (
            isinstance(item["utc_offset_seconds"], bool)
            or not isinstance(item["utc_offset_seconds"], int)
            or item["utc_offset_seconds"] != 0
            or not isinstance(item["timezone"], str)
            or item["timezone"] not in {"GMT", "UTC"}
            or not isinstance(item["timezone_abbreviation"], str)
            or item["timezone_abbreviation"] not in {"GMT", "UTC"}
        ):
            raise OpenMeteoNativeContractError(
                "Open-Meteo responstijdzone is niet UTC"
            )
        current = item["current"]
        current_units = item["current_units"]
        minutely = item["minutely_15"]
        minutely_units = item["minutely_15_units"]
        hourly = item["hourly"]
        hourly_units = item["hourly_units"]
        if any(type(value) is not dict for value in (
            current, current_units, minutely, minutely_units, hourly, hourly_units
        )):
            raise OpenMeteoNativeContractError(
                "Open-Meteo responssectie is geen object"
            )
        if set(current) != {"time", "interval", *CURRENT}:
            raise OpenMeteoNativeContractError("Open-Meteo current-set wijkt af")
        current_time = _unix_timestamp(current["time"], label="current.time")
        if (
            isinstance(current["interval"], bool)
            or not isinstance(current["interval"], int)
            or current["interval"] != 900
        ):
            raise OpenMeteoNativeContractError("Open-Meteo current-interval wijkt af")
        for name in CURRENT:
            if current[name] is not None:
                _finite_number(current[name], label=f"current.{name}")
        if current_units != EXPECTED_CURRENT_UNITS:
            raise OpenMeteoNativeContractError("Open-Meteo current-units wijken af")
        minutely_times = _time_axis(
            minutely,
            count=FORECAST_MINUTELY_15,
            step=900,
            label="minutely_15",
        )
        _variable_axes(minutely, MINUTELY_15, count=FORECAST_MINUTELY_15, label="minutely_15")
        hourly_times = _time_axis(
            hourly,
            count=FORECAST_HOURS,
            step=3600,
            label="hourly",
        )
        _variable_axes(hourly, HOURLY, count=FORECAST_HOURS, label="hourly")
        if (
            current_time != minutely_times[0]
            or not hourly_times[0] <= current_time < hourly_times[0] + 3600
        ):
            raise OpenMeteoNativeContractError(
                "Open-Meteo tijdsecties zijn onderling niet uitgelijnd"
            )
        if minutely_units != EXPECTED_MINUTELY_15_UNITS:
            raise OpenMeteoNativeContractError(
                "Open-Meteo minutely_15-units wijken af"
            )
        expected_hourly_units = dict(EXPECTED_HOURLY_UNITS)
        lifted_index_unit = hourly_units.get("lifted_index")
        expected_hourly_units["lifted_index"] = lifted_index_unit
        if (
            lifted_index_unit not in SUPPORTED_LIFTED_INDEX_UNITS
            or hourly_units != expected_hourly_units
        ):
            raise OpenMeteoNativeContractError(
                "Open-Meteo hourly-units wijken af"
            )
        unit_rows = {
            "current": dict(sorted(current_units.items())),
            "hourly": dict(sorted(hourly_units.items())),
            "minutely_15": dict(sorted(minutely_units.items())),
        }
        rows.append({
            "hourly_keys": sorted(hourly),
            "minutely_keys": sorted(minutely),
            "units": unit_rows,
        })
    structure_bytes = json.dumps(
        rows, ensure_ascii=True, allow_nan=False, sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")
    return OpenMeteoStructureIdentity(
        location_count=len(items),
        minutely_steps=FORECAST_MINUTELY_15,
        hourly_steps=FORECAST_HOURS,
        variable_count=len(CURRENT) + len(MINUTELY_15) + len(HOURLY),
        structure_sha256=hashlib.sha256(structure_bytes).hexdigest(),
    )


def decode_verified_openmeteo_response(
    payload: bytes,
    *,
    query_count: int,
) -> tuple[dict[str, Any], ...]:
    """Return response locations only after the complete closed schema passes."""

    _inspect(payload, query_count=query_count)
    decoded = _strict_json(payload)
    items = [decoded] if query_count == 1 else decoded
    # ``_inspect`` proved exact dict/list types, count, axes, units and every
    # numeric value.  Copy the mappings so no caller can mutate decoded state
    # shared with another processing stage.
    return tuple(dict(item) for item in items)


def _provenance(*, request_sha256: str, query_count: int) -> dict[str, object]:
    return {
        "attribution_policy": _attribution_policy(),
        "attribution_policy_id": OPENMETEO_ATTRIBUTION_POLICY_ID,
        "attribution_policy_sha256": OPENMETEO_ATTRIBUTION_POLICY_SHA256,
        "capture_stage": NATIVE_CAPTURE_STAGE,
        "collector": OPENMETEO_COLLECTOR,
        "collector_version": OPENMETEO_COLLECTOR_VERSION,
        "coordinate_values_in_diagnostics": False,
        "endpoint_origin": OPENMETEO_ORIGIN,
        "endpoint_path": OPENMETEO_PATH,
        "elevation_downscaling": "disabled",
        "model_selection": "automatic_best_match",
        "operational_admission": False,
        "parser_version": OPENMETEO_STRUCTURE_INSPECTOR_VERSION,
        "query_count": query_count,
        "request_sha256": request_sha256,
        "representation": NATIVE_REPRESENTATION,
        "target_ids_in_child": False,
    }


def _receipt_id(store: RawArtifactStore, job_id: str) -> str:
    return store._receipt_id(  # noqa: SLF001 - durable identity contract
        provider="openmeteo", product=OPENMETEO_RESPONSE_PRODUCT,
        fetch_id=f"{job_id}:response_json",
    )


def _capture_id(store: RawArtifactStore, job_id: str) -> str:
    return store._capture_id(  # noqa: SLF001 - durable identity contract
        provider="openmeteo", product=OPENMETEO_CAPTURE_PRODUCT,
        fetch_id=f"{job_id}:capture",
    )


def _upstream_id(request_sha256: str, content_sha256: str) -> str:
    return (
        f"api.open-meteo.com{OPENMETEO_PATH}"
        f"|request_sha256={request_sha256}|content_sha256={content_sha256}"
    )


def _capture_upstream_id(request_sha256: str, content_sha256: str) -> str:
    return (
        f"{OPENMETEO_CAPTURE_PRODUCT}|request_sha256={request_sha256}"
        f"|content_sha256={content_sha256}"
        f"|attribution_policy_sha256={OPENMETEO_ATTRIBUTION_POLICY_SHA256}"
    )


def _validate_receipt(
    receipt: RawArtifactReceipt, *, job_id: str, request_sha256: str,
    query_count: int,
) -> None:
    if (
        receipt.provider != "openmeteo"
        or receipt.product != OPENMETEO_RESPONSE_PRODUCT
        or receipt.fetch_id != f"{job_id}:response_json"
        or receipt.upstream_id != _upstream_id(request_sha256, receipt.content_sha256)
        or receipt.content_type != "application/json"
        or not 1 <= receipt.size_bytes <= MAX_RESPONSE_BYTES
        or receipt.provenance != _provenance(
            request_sha256=request_sha256, query_count=query_count
        )
    ):
        raise OpenMeteoNativeCorruptionError(
            "Open-Meteo raw receipt wijkt af van contract"
        )


def _validate_capture(
    capture: RawArtifactCaptureReceipt, *, job_id: str,
    request_sha256: str, receipt: RawArtifactReceipt,
) -> None:
    members = {member.role: member.receipt_id for member in capture.members}
    expected_upstream = _capture_upstream_id(
        request_sha256, receipt.content_sha256
    )
    if (
        capture.raw_store_schema_version != RAW_ARTIFACT_SCHEMA_VERSION
        or capture.provider != "openmeteo"
        or capture.product != OPENMETEO_CAPTURE_PRODUCT
        or capture.fetch_id != f"{job_id}:capture"
        or capture.upstream_capture_id != expected_upstream
        or capture.product_timestamp is not None
        or members != {"response_json": receipt.receipt_id}
    ):
        raise OpenMeteoNativeCorruptionError(
            "Open-Meteo capture wijkt af van contract"
        )


def _result(
    *, capture: RawArtifactCaptureReceipt, receipt: RawArtifactReceipt,
    identity: OpenMeteoStructureIdentity, request_sha256: str,
) -> dict[str, object]:
    return {
        "admission_status": OPENMETEO_ADMISSION_STATUS,
        "attribution_policy_id": OPENMETEO_ATTRIBUTION_POLICY_ID,
        "capture_state": "captured_structure_inspected_not_decoded",
        "contract": CONTRACT,
        "meteorological_claim": False,
        "model_skill_claim": False,
        "operational_admission_eligible": False,
        "request_sha256": request_sha256,
        "structure": {
            "hourly_steps": identity.hourly_steps,
            "location_count": identity.location_count,
            "minutely_steps": identity.minutely_steps,
            "structure_sha256": identity.structure_sha256,
            "variable_count": identity.variable_count,
        },
        "raw_provenance": {
            "raw_capture_id": capture.capture_id,
            "raw_capture_manifest_sha256": capture.manifest_sha256,
            "raw_capture_receipt_sha256": capture.capture_receipt_sha256,
            "raw_member_count": 1,
            "raw_store_schema_version": RAW_ARTIFACT_SCHEMA_VERSION,
            "response": {
                "content_sha256": receipt.content_sha256,
                "receipt_id": receipt.receipt_id,
                "receipt_sha256": receipt.receipt_sha256,
                "received_at": receipt.received_at,
                "size_bytes": receipt.size_bytes,
            },
        },
        "weather_state": "unknown",
    }


def _resume(
    store: RawArtifactStore, *, job_id: str, request_sha256: str,
    query_count: int, deadline: float,
) -> dict[str, object] | None:
    bundle = store.load_capture_bundle_bytes(_capture_id(store, job_id))
    _remaining(deadline)
    if bundle is not None:
        capture, receipts, payloads = bundle
        if set(receipts) != {"response_json"} or set(payloads) != {"response_json"}:
            raise OpenMeteoNativeCorruptionError(
                "Open-Meteo capture mist response_json"
            )
        receipt = receipts["response_json"]
        payload = payloads["response_json"]
    else:
        capture = None
        loaded = store.load_receipt_bytes(_receipt_id(store, job_id))
        if loaded is None:
            return None
        receipt, payload = loaded
    _validate_receipt(
        receipt, job_id=job_id, request_sha256=request_sha256,
        query_count=query_count,
    )
    identity = _inspect(payload, query_count=query_count)
    _remaining(deadline)
    if capture is None:
        capture = store.commit_capture(
            provider="openmeteo",
            product=OPENMETEO_CAPTURE_PRODUCT,
            fetch_id=f"{job_id}:capture",
            upstream_capture_id=(
                _capture_upstream_id(request_sha256, receipt.content_sha256)
            ),
            product_timestamp=None,
            members={"response_json": receipt.receipt_id},
        )
        _remaining(deadline)
    _validate_capture(
        capture, job_id=job_id, request_sha256=request_sha256, receipt=receipt
    )
    return _result(
        capture=capture, receipt=receipt, identity=identity,
        request_sha256=request_sha256,
    )


def capture_openmeteo_forecast(
    job: object,
    *,
    raw_store: RawArtifactStore | None = None,
    deadline_at: float | None = None,
    opener: Callable[..., Any] | None = None,
    _fault_injector: Callable[[str], None] | None = None,
) -> dict[str, object]:
    """Capture exact response bytes; never infer present or future rain."""

    job_id, queries = _validate_job(job)
    params = _request_parameters(queries)
    request_sha256, query_string = _request_identity(queries, params)
    now = time.monotonic()
    if deadline_at is None:
        deadline = now + CAPTURE_DEADLINE_SECONDS
    elif isinstance(deadline_at, bool) or not isinstance(
        deadline_at, (int, float)
    ):
        raise OpenMeteoNativeContractError(
            "Open-Meteo deadline_at is ongeldig"
        )
    else:
        try:
            requested_deadline = float(deadline_at)
        except (OverflowError, TypeError, ValueError):
            raise OpenMeteoNativeContractError(
                "Open-Meteo deadline_at is ongeldig"
            ) from None
        if not math.isfinite(requested_deadline):
            raise OpenMeteoNativeContractError(
                "Open-Meteo deadline_at is ongeldig"
            )
        deadline = min(requested_deadline, now + CAPTURE_DEADLINE_SECONDS)
    _remaining(deadline)
    store = _raw_store(raw_store)
    with _CAPTURE_LOCK:
        _remaining(deadline)
        resumed = _resume(
            store, job_id=job_id, request_sha256=request_sha256,
            query_count=len(queries), deadline=deadline,
        )
        if resumed is not None:
            return resumed
        response = _request(
            query_string=query_string, deadline=deadline, opener=opener
        )
        digest = hashlib.sha256(response.payload).hexdigest()
        receipt = store.store(
            provider="openmeteo",
            product=OPENMETEO_RESPONSE_PRODUCT,
            fetch_id=f"{job_id}:response_json",
            content_type=response.content_type,
            received_at=response.received_at,
            upstream_id=_upstream_id(request_sha256, digest),
            provenance=_provenance(
                request_sha256=request_sha256, query_count=len(queries)
            ),
            payload=response.payload,
        )
        _remaining(deadline)
        if _fault_injector is not None:
            _fault_injector("openmeteo.after_response_store")
        _remaining(deadline)
        identity = _inspect(response.payload, query_count=len(queries))
        _remaining(deadline)
        if _fault_injector is not None:
            _fault_injector("openmeteo.before_capture_commit")
        _remaining(deadline)
        capture = store.commit_capture(
            provider="openmeteo",
            product=OPENMETEO_CAPTURE_PRODUCT,
            fetch_id=f"{job_id}:capture",
            upstream_capture_id=(
                _capture_upstream_id(request_sha256, receipt.content_sha256)
            ),
            product_timestamp=None,
            members={"response_json": receipt.receipt_id},
        )
        if _fault_injector is not None:
            _fault_injector("openmeteo.after_capture_commit")
        _validate_capture(
            capture, job_id=job_id, request_sha256=request_sha256,
            receipt=receipt,
        )
        return _result(
            capture=capture, receipt=receipt, identity=identity,
            request_sha256=request_sha256,
        )


__all__ = [
    "CAPTURE_DEADLINE_SECONDS",
    "CONTRACT",
    "FORECAST_HOURS",
    "FORECAST_MINUTELY_15",
    "MAX_QUERY_COUNT",
    "MAX_RESPONSE_BYTES",
    "OPENMETEO_ADMISSION_STATUS",
    "OPENMETEO_ATTRIBUTION_POLICY_CANONICAL",
    "OPENMETEO_ATTRIBUTION_POLICY_ID",
    "OPENMETEO_ATTRIBUTION_POLICY_SHA256",
    "OPENMETEO_CAPTURE_PRODUCT",
    "OPENMETEO_RESPONSE_PRODUCT",
    "OpenMeteoNativeCapacityError",
    "OpenMeteoNativeContractError",
    "OpenMeteoNativeCorruptionError",
    "OpenMeteoNativeError",
    "OpenMeteoNativeTransportError",
    "OpenMeteoStructureIdentity",
    "capture_openmeteo_forecast",
    "decode_verified_openmeteo_response",
]
