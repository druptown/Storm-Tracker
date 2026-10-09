"""Exact raw-only capture for the public, no-secret source subset.

The module deliberately stops at transport evidence.  A successful HTTPS
response is committed as uninterpreted bytes before any consumer may inspect
JSON or attach meteorological meaning.  It is standalone and is not imported
by the service, Home Assistant, MQTT, the shared activation catalogue, or a
decoder.

Provider terms and retention rights are outside this technical contract.  The
only access statement made here is that every request profile is anonymous:
no credential, cookie, token, or caller-provided header is accepted.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import math
import re
import threading
import time
from types import MappingProxyType
from typing import Any, Callable, Mapping
from urllib.parse import parse_qsl, urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

from .raw_artifacts import (
    NATIVE_CAPTURE_STAGE,
    NATIVE_REPRESENTATION,
    RAW_ARTIFACT_SCHEMA_VERSION,
    RawArtifactCaptureReceipt,
    RawArtifactReceipt,
    RawArtifactStore,
)


CONTRACT = "stv3-worker-public-source-exact-raw-v1"
RESULT_CONTRACT = "stv3-worker-public-source-exact-raw-result-v1"
LOCATION_QUERY_CONTRACT = "stv3-public-source-region-query-v1"
CATALOG_QUERY_CONTRACT = "stv3-public-source-no-query-v1"
OPENMETEO_QUERY_BATCH_CONTRACT = "stv3-openmeteo-target-query-batch-v1"

# Request fields are intentionally repeated as a tiny transport contract
# instead of importing ``openmeteo_native``: that older module contains a
# response-shape inspector, while this product must remain raw-only.  Focused
# tests bind these values back to the existing request contract to prevent
# silent drift.
OPENMETEO_CURRENT = ("precipitation",)
OPENMETEO_MINUTELY_15 = (
    "precipitation",
    "rain",
    "showers",
    "cape",
    "lightning_potential",
    "wind_gusts_10m",
    "weather_code",
    "freezing_level_height",
)
OPENMETEO_HOURLY = (
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
OPENMETEO_FORECAST_MINUTELY_15 = 13
OPENMETEO_FORECAST_HOURS = 6

OPENMETEO_WORKLOAD_ID = "model.openmeteo.region"
OPENMETEO_ICON2I_WORKLOAD_ID = (
    "model.openmeteo.italiameteo_icon2i.region"
)
GEOSPHERE_WORKLOAD_ID = "validation.geosphere.location"
ITALIAMETEO_CATALOG_WORKLOAD_ID = (
    "validation.italiameteo.dpc_sri_historical_catalog"
)
METEOLUX_WORKLOAD_ID = "validation.meteolux.location"

OPENMETEO_HANDLER_ID = "openmeteo_forecast_unverified_exact_raw_v1"
OPENMETEO_ICON2I_HANDLER_ID = (
    "openmeteo_italiameteo_icon2i_unverified_exact_raw_v1"
)
GEOSPHERE_HANDLER_ID = "geosphere_inca_location_unverified_exact_raw_v1"
ITALIAMETEO_CATALOG_HANDLER_ID = (
    "italiameteo_dpc_sri_catalog_unverified_exact_raw_v1"
)
METEOLUX_HANDLER_ID = "meteolux_location_weather_unverified_exact_raw_v1"

OPENMETEO_HOST = "api.open-meteo.com"
OPENMETEO_PATH = "/v1/forecast"
GEOSPHERE_HOST = "dataset.api.hub.geosphere.at"
GEOSPHERE_PATH = (
    "/v1/timeseries/forecast/nowcast-v1-15min-1km"
)
ITALIAMETEO_HOST = "meteohub.agenziaitaliameteo.it"
ITALIAMETEO_PATH = "/api/datasets/radar_sri_dpc/opendata"
METEOLUX_HOST = "metapi.ana.lu"
METEOLUX_PATH = "/api/v1/metapp/weather"

MAX_URL_CHARS = 8192
MAX_HEADER_CHARS = 8192
HTTP_READ_CHUNK_BYTES = 64 * 1024
HTTP_IO_TIMEOUT_SECONDS = 20.0
CAPTURE_DEADLINE_SECONDS = 60.0
MAX_LOCATION_QUERIES = 64
UNINTERPRETED_CONTENT_TYPE = "application/octet-stream"

_JOB_ID_RE = re.compile(
    r"^acq:[a-z2-7]{8}(?::z:[a-z2-7]{8}){5}:z:[a-z2-7]{4}$"
)
_TARGET_QUERY_KEY_RE = re.compile(
    r"^aqt:[a-z2-7]{8}(?::z:[a-z2-7]{8}){5}:z:[a-z2-7]{4}$"
)
_REGION_QUERY_KEY_RE = re.compile(
    r"^aqr:[a-z2-7]{8}(?::z:[a-z2-7]{8}){5}:z:[a-z2-7]{4}$"
)
_HEADER_NAME_RE = re.compile(r"^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$")
_MEDIA_TYPE_RE = re.compile(
    r"^[A-Za-z0-9!#$&^_.+\-]+/[A-Za-z0-9!#$&^_.+\-]+$"
)
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_CAPTURE_LOCK = threading.Lock()


class PublicSourceRawError(RuntimeError):
    """Base class for payload-free exact-raw capture failures."""


class PublicSourceRawContractError(PublicSourceRawError):
    """A request or response escaped the closed transport contract."""


class PublicSourceRawCapacityError(PublicSourceRawError):
    """A URL, header, body, query, or time budget was exhausted."""


class PublicSourceRawTransportError(PublicSourceRawError):
    """The anonymous HTTPS transport failed without exposing its URL."""


class PublicSourceRawCorruptionError(PublicSourceRawError):
    """Previously committed evidence no longer matches this exact job."""


@dataclass(frozen=True, slots=True)
class PublicSourceHandlerSpec:
    handler_id: str
    workload_id: str
    provider: str
    product_id: str
    raw_product: str
    capture_product: str
    host: str
    path: str
    query_profile: str
    maximum_transport_bytes: int
    maximum_artifacts: int
    maximum_wall_seconds: int
    coverage_bounds: tuple[float, float, float, float] | None
    provenance_kind: str

    @property
    def origin(self) -> str:
        return f"https://{self.host}"

    @property
    def role(self) -> str:
        return "response_bytes"

    @property
    def fetch_suffix(self) -> str:
        return ":response_bytes"

    @property
    def capture_fetch_suffix(self) -> str:
        return ":capture"

    def child_handler_mapping(self) -> dict[str, Any]:
        """Return the immutable fields needed by a closed child dispatcher."""

        return {
            "allowed_content_types": [UNINTERPRETED_CONTENT_TYPE],
            "capture_fetch_suffix": self.capture_fetch_suffix,
            "capture_product": self.capture_product,
            "capture_provenance_kind": self.provenance_kind,
            "capture_state": "captured_not_decoded",
            "decode_field": "decode_status",
            "decode_status": "not_decoded",
            "handler_id": self.handler_id,
            "ledger_provider": self.provider,
            "maximum_artifacts": self.maximum_artifacts,
            "maximum_transport_bytes": self.maximum_transport_bytes,
            "maximum_wall_seconds": self.maximum_wall_seconds,
            "native_provider": self.provider,
            "payload_contract": self.query_profile,
            "raw_product": self.raw_product,
            "receipt_fetch_suffix": self.fetch_suffix,
            "result_provider": self.provider,
            "role": self.role,
            "source_field": "sources",
            "source_hash_prefix": "",
            "workload_id": self.workload_id,
        }


def _spec(
    handler_id: str,
    workload_id: str,
    provider: str,
    product_id: str,
    host: str,
    path: str,
    query_profile: str,
    maximum_mebibytes: int,
    coverage_bounds: tuple[float, float, float, float] | None,
    provenance_kind: str,
) -> PublicSourceHandlerSpec:
    stem = product_id.removesuffix("_json")
    return PublicSourceHandlerSpec(
        handler_id=handler_id,
        workload_id=workload_id,
        provider=provider,
        product_id=product_id,
        raw_product=f"{stem}_unverified_response_bytes",
        capture_product=f"{stem}_unverified_exact_capture",
        host=host,
        path=path,
        query_profile=query_profile,
        maximum_transport_bytes=maximum_mebibytes * 1024 * 1024,
        maximum_artifacts=1,
        maximum_wall_seconds=60,
        coverage_bounds=coverage_bounds,
        provenance_kind=provenance_kind,
    )


_SPECS = (
    _spec(
        OPENMETEO_HANDLER_ID,
        OPENMETEO_WORKLOAD_ID,
        "openmeteo",
        "openmeteo_forecast_native_json",
        OPENMETEO_HOST,
        OPENMETEO_PATH,
        "openmeteo_target_query_batch_v1",
        8,
        None,
        "openmeteo_unverified_response_v1",
    ),
    _spec(
        OPENMETEO_ICON2I_HANDLER_ID,
        OPENMETEO_ICON2I_WORKLOAD_ID,
        "openmeteo",
        "openmeteo_italia_meteo_arpae_icon_2i_forecast_json",
        OPENMETEO_HOST,
        OPENMETEO_PATH,
        "public_region_query_v1",
        4,
        (35.0, 5.5, 48.5, 19.0),
        "openmeteo_icon2i_unverified_response_v1",
    ),
    _spec(
        GEOSPHERE_HANDLER_ID,
        GEOSPHERE_WORKLOAD_ID,
        "geosphere",
        "geosphere_inca_point_nowcast_validation_json",
        GEOSPHERE_HOST,
        GEOSPHERE_PATH,
        "public_region_query_v1",
        4,
        (45.5, 8.1, 49.5, 17.75),
        "geosphere_inca_unverified_response_v1",
    ),
    _spec(
        ITALIAMETEO_CATALOG_HANDLER_ID,
        ITALIAMETEO_CATALOG_WORKLOAD_ID,
        "italiameteo",
        "italiameteo_dpc_sri_historical_daily_catalog_json",
        ITALIAMETEO_HOST,
        ITALIAMETEO_PATH,
        "exact_empty_object_v1",
        4,
        None,
        "italiameteo_catalog_unverified_response_v1",
    ),
    _spec(
        METEOLUX_HANDLER_ID,
        METEOLUX_WORKLOAD_ID,
        "meteolux",
        "meteolux_location_weather_validation_json",
        METEOLUX_HOST,
        METEOLUX_PATH,
        "public_region_query_v1",
        4,
        (48.5, 4.5, 51.0, 7.5),
        "meteolux_weather_unverified_response_v1",
    ),
)

HANDLER_SPECS_BY_ID: Mapping[str, PublicSourceHandlerSpec] = MappingProxyType(
    {item.handler_id: item for item in _SPECS}
)
HANDLER_SPECS_BY_WORKLOAD: Mapping[str, PublicSourceHandlerSpec] = (
    MappingProxyType({item.workload_id: item for item in _SPECS})
)


@dataclass(frozen=True, slots=True)
class _RequestBinding:
    url: str
    request_sha256: str
    query_count: int


@dataclass(frozen=True, slots=True)
class _HttpResponse:
    payload: bytes
    status: int
    received_at: float
    declared_length: int | None
    reported_media_type: str
    response_headers_sha256: str
    final_url_sha256: str
    transport_sha256: str


class _RejectRedirects(HTTPRedirectHandler):
    def redirect_request(self, *_args: Any, **_kwargs: Any):
        raise PublicSourceRawContractError("providerredirect is niet toegestaan")


def _open_without_redirects(request: Request, *, timeout: float):
    return build_opener(ProxyHandler({}), _RejectRedirects()).open(
        request, timeout=timeout
    )


def _canonical_bytes(value: Mapping[str, Any]) -> bytes:
    try:
        return json.dumps(
            dict(value), allow_nan=False, ensure_ascii=True,
            separators=(",", ":"), sort_keys=True,
        ).encode("ascii")
    except (TypeError, ValueError, UnicodeError) as err:
        raise PublicSourceRawContractError(
            "public-sourcebewijs is niet canoniek"
        ) from err


def _digest(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _remaining(deadline: float) -> float:
    value = deadline - time.monotonic()
    if not math.isfinite(value) or value <= 0:
        raise TimeoutError("public-source capturedeadline is verstreken")
    return value


def _deadline(deadline_at: float | None) -> float:
    now = time.monotonic()
    if deadline_at is None:
        return now + CAPTURE_DEADLINE_SECONDS
    if isinstance(deadline_at, bool) or not isinstance(deadline_at, (int, float)):
        raise PublicSourceRawContractError("deadline_at is ongeldig")
    try:
        requested = float(deadline_at)
    except (TypeError, ValueError, OverflowError):
        raise PublicSourceRawContractError("deadline_at is ongeldig") from None
    if not math.isfinite(requested):
        raise PublicSourceRawContractError("deadline_at is ongeldig")
    result = min(requested, now + CAPTURE_DEADLINE_SECONDS)
    _remaining(result)
    return result


def _job(job: object) -> tuple[str, dict[str, Any]]:
    job_id = getattr(job, "job_id", None)
    payload = getattr(job, "payload", None)
    if not isinstance(job_id, str) or _JOB_ID_RE.fullmatch(job_id) is None:
        raise PublicSourceRawContractError(
            "public-sourcejob mist een veilige native job_id"
        )
    if type(payload) is not dict:
        raise PublicSourceRawContractError(
            "public-sourcejob mist een exacte handlerpayload"
        )
    return job_id, payload


def _coordinate(value: object, *, label: str, low: float, high: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise PublicSourceRawContractError(f"{label} is ongeldig")
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        raise PublicSourceRawContractError(f"{label} is ongeldig") from None
    if not math.isfinite(result) or not low <= result <= high:
        raise PublicSourceRawContractError(f"{label} is ongeldig")
    return 0.0 if result == 0.0 else result


def _location_payload(
    spec: PublicSourceHandlerSpec, payload: dict[str, Any]
) -> tuple[float, float]:
    if set(payload) != {"contract", "latitude", "longitude", "query_key"}:
        raise PublicSourceRawContractError(
            "regioquery bevat onbekende of ontbrekende velden"
        )
    if payload["contract"] != LOCATION_QUERY_CONTRACT:
        raise PublicSourceRawContractError("regioquerycontract wijkt af")
    key = payload["query_key"]
    if not isinstance(key, str) or _REGION_QUERY_KEY_RE.fullmatch(key) is None:
        raise PublicSourceRawContractError("regioquery-identiteit is ongeldig")
    latitude = _coordinate(
        payload["latitude"], label="latitude", low=-90.0, high=90.0
    )
    longitude = _coordinate(
        payload["longitude"], label="longitude", low=-180.0, high=180.0
    )
    if spec.coverage_bounds is not None:
        south, west, north, east = spec.coverage_bounds
        if not south <= latitude <= north or not west <= longitude <= east:
            raise PublicSourceRawContractError(
                "regioquery valt buiten het bewezen providerbereik"
            )
    return latitude, longitude


def _openmeteo_batch(payload: dict[str, Any]) -> tuple[tuple[float, float], ...]:
    if set(payload) != {
        "contract", "operational_handler_compatible", "queries",
        "target_resolution_required",
    }:
        raise PublicSourceRawContractError(
            "Open-Meteo querybatch bevat onbekende velden"
        )
    queries = payload["queries"]
    if (
        payload["contract"] != OPENMETEO_QUERY_BATCH_CONTRACT
        or payload["operational_handler_compatible"] is not False
        or payload["target_resolution_required"] is not True
        or type(queries) is not list
        or not 1 <= len(queries) <= MAX_LOCATION_QUERIES
    ):
        raise PublicSourceRawContractError("Open-Meteo querybatch wijkt af")
    result: list[tuple[float, float]] = []
    seen: set[str] = set()
    for item in queries:
        if type(item) is not dict or set(item) != {
            "latitude", "longitude", "query_key", "query_kind",
        }:
            raise PublicSourceRawContractError(
                "Open-Meteo targetquery wijkt af"
            )
        key = item["query_key"]
        if (
            not isinstance(key, str)
            or _TARGET_QUERY_KEY_RE.fullmatch(key) is None
            or key in seen
            or item["query_kind"] not in {"local", "roaming"}
        ):
            raise PublicSourceRawContractError(
                "Open-Meteo targetquery-identiteit is ongeldig"
            )
        seen.add(key)
        result.append((
            _coordinate(item["latitude"], label="latitude", low=-90.0, high=90.0),
            _coordinate(
                item["longitude"], label="longitude", low=-180.0, high=180.0
            ),
        ))
    return tuple(result)


def _parameters(
    spec: PublicSourceHandlerSpec, payload: dict[str, Any]
) -> tuple[dict[str, str], int]:
    if spec.workload_id == OPENMETEO_WORKLOAD_ID:
        locations = _openmeteo_batch(payload)
        return {
            "latitude": ",".join(f"{item[0]:.5f}" for item in locations),
            "longitude": ",".join(f"{item[1]:.5f}" for item in locations),
            "current": ",".join(OPENMETEO_CURRENT),
            "minutely_15": ",".join(OPENMETEO_MINUTELY_15),
            "forecast_minutely_15": str(
                OPENMETEO_FORECAST_MINUTELY_15
            ),
            "hourly": ",".join(OPENMETEO_HOURLY),
            "forecast_hours": str(OPENMETEO_FORECAST_HOURS),
            "timezone": "UTC",
            "timeformat": "unixtime",
            "precipitation_unit": "mm",
            "wind_speed_unit": "kmh",
            # Open-Meteo applies scalar parameters per coordinate.  For a
            # multi-location request ``elevation`` must therefore contain the
            # same number of values as latitude/longitude; a single ``nan``
            # is rejected with HTTP 400 and used to make every live batch
            # retry until its scheduler deadline.
            "elevation": ",".join("nan" for _item in locations),
            "cell_selection": "nearest",
        }, len(locations)
    if spec.workload_id == ITALIAMETEO_CATALOG_WORKLOAD_ID:
        if payload:
            raise PublicSourceRawContractError(
                "ItaliaMeteo catalogus accepteert geen querypayload"
            )
        return {}, 0
    latitude, longitude = _location_payload(spec, payload)
    if spec.workload_id == OPENMETEO_ICON2I_WORKLOAD_ID:
        return {
            "latitude": f"{latitude:.5f}",
            "longitude": f"{longitude:.5f}",
            "models": "italia_meteo_arpae_icon_2i",
            "forecast_hours": "6",
            "timezone": "UTC",
            "hourly": "precipitation,lightning_potential",
        }, 1
    if spec.workload_id == GEOSPHERE_WORKLOAD_ID:
        return {
            "parameters": "rr",
            "lat_lon": f"{latitude:.5f},{longitude:.5f}",
            "output_format": "geojson",
        }, 1
    if spec.workload_id == METEOLUX_WORKLOAD_ID:
        return {
            "langcode": "en",
            "lat": f"{latitude:.5f}",
            "long": f"{longitude:.5f}",
        }, 1
    raise PublicSourceRawContractError("workload mist een gesloten queryprofiel")


def _request_binding(
    spec: PublicSourceHandlerSpec, payload: dict[str, Any]
) -> _RequestBinding:
    params, query_count = _parameters(spec, payload)
    query = urlencode(params, safe=",")
    url = spec.origin + spec.path + (("?" + query) if query else "")
    if len(url) > MAX_URL_CHARS:
        raise PublicSourceRawCapacityError("request-URL overschrijdt de grens")
    parsed = urlsplit(url)
    if (
        parsed.scheme != "https"
        or parsed.hostname != spec.host
        or parsed.port not in {None, 443}
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path != spec.path
        or parsed.fragment
    ):
        raise PublicSourceRawContractError("request ontsnapt uit de HTTPS-allowlist")
    try:
        pairs = parse_qsl(
            parsed.query,
            keep_blank_values=True,
            strict_parsing=True,
            max_num_fields=32,
        ) if parsed.query else []
    except ValueError as err:
        raise PublicSourceRawContractError("requestquery is ongeldig") from err
    if len(pairs) != len(params) or dict(pairs) != params:
        raise PublicSourceRawContractError("requestquery wijkt af van allowlist")
    descriptor = {
        "accept": "*/*",
        "accept_encoding": "identity",
        "anonymous": True,
        "contract": "stv3-public-source-request-descriptor-v1",
        "host": spec.host,
        "method": "GET",
        "path": spec.path,
        "query": dict(sorted(params.items())),
        "workload_id": spec.workload_id,
    }
    return _RequestBinding(url, _digest(descriptor), query_count)


def _header_entries(headers: object) -> tuple[tuple[str, str], ...]:
    items = getattr(headers, "items", None)
    if not callable(items):
        raise PublicSourceRawContractError("responseheaders ontbreken")
    result: list[tuple[str, str]] = []
    total = 0
    try:
        raw_entries = items()
        for raw_name, raw_value in raw_entries:
            name = str(raw_name)
            value = str(raw_value)
            if _HEADER_NAME_RE.fullmatch(name) is None or any(
                ord(char) < 0x20 or ord(char) == 0x7f for char in value
            ):
                raise PublicSourceRawContractError("responseheader is ongeldig")
            total += len(name) + len(value) + 4
            if total > MAX_HEADER_CHARS:
                raise PublicSourceRawCapacityError(
                    "responseheaders overschrijden de grens"
                )
            result.append((name.lower(), value.strip()))
    except (PublicSourceRawError, TimeoutError):
        raise
    except Exception:
        raise PublicSourceRawContractError("responseheaders zijn ongeldig") from None
    for singleton in ("content-length", "content-encoding"):
        if sum(name == singleton for name, _ in result) > 1:
            raise PublicSourceRawContractError(
                "response bevat dubbele transportframing"
            )
    return tuple(result)


def _header_values(
    entries: tuple[tuple[str, str], ...], name: str
) -> tuple[str, ...]:
    return tuple(value for key, value in entries if key == name)


def _declared_length(
    entries: tuple[tuple[str, str], ...], maximum: int
) -> int | None:
    values = _header_values(entries, "content-length")
    if not values:
        return None
    value = values[0]
    if not value.isascii() or not value.isdecimal():
        raise PublicSourceRawContractError("Content-Length is ongeldig")
    result = int(value)
    if result > maximum:
        raise PublicSourceRawCapacityError("Content-Length overschrijdt de grens")
    return result


def _reported_media_type(entries: tuple[tuple[str, str], ...]) -> str:
    values = _header_values(entries, "content-type")
    if len(values) != 1:
        return "unverified"
    media_type = values[0].split(";", 1)[0].strip().lower()
    return media_type if _MEDIA_TYPE_RE.fullmatch(media_type) else "unverified"


def _tighten_stream_timeout(response: object, *, timeout: float) -> None:
    frontier = [response]
    visited: set[int] = set()
    for _depth in range(4):
        following: list[object] = []
        for candidate in frontier:
            if id(candidate) in visited:
                continue
            visited.add(id(candidate))
            setter = getattr(candidate, "settimeout", None)
            if callable(setter):
                try:
                    setter(timeout)
                except Exception:
                    raise PublicSourceRawTransportError(
                        "response-timeout kon niet worden begrensd"
                    ) from None
                return
            for attribute in ("fp", "raw", "_sock", "sock"):
                try:
                    child = getattr(candidate, attribute, None)
                except Exception:
                    child = None
                if child is not None:
                    following.append(child)
        frontier = following
        if not frontier:
            return


def _read_body(
    response: object,
    *,
    declared_length: int | None,
    maximum: int,
    deadline: float,
) -> bytes:
    reader = getattr(response, "read1", None)
    if not callable(reader):
        reader = getattr(response, "read", None)
    if not callable(reader):
        raise PublicSourceRawContractError("response heeft geen bytestream")
    chunks: list[bytes] = []
    total = 0
    maximum_chunks = (maximum // HTTP_READ_CHUNK_BYTES) + 2
    for _index in range(maximum_chunks):
        remaining = _remaining(deadline)
        _tighten_stream_timeout(
            response, timeout=min(HTTP_IO_TIMEOUT_SECONDS, remaining)
        )
        requested = min(HTTP_READ_CHUNK_BYTES, maximum + 1 - total)
        try:
            raw = reader(requested)
        except TimeoutError:
            raise TimeoutError("public-source response-read is verlopen") from None
        except Exception:
            raise PublicSourceRawTransportError(
                "public-source response-read is mislukt"
            ) from None
        if not raw:
            break
        if not isinstance(raw, (bytes, bytearray, memoryview)):
            raise PublicSourceRawContractError(
                "responsstream leverde geen bytes"
            )
        chunk = bytes(raw)
        if len(chunk) > requested:
            raise PublicSourceRawCapacityError("responsechunk overschrijdt de grens")
        chunks.append(chunk)
        total += len(chunk)
        if total > maximum or (
            declared_length is not None and total > declared_length
        ):
            raise PublicSourceRawCapacityError("response overschrijdt de bytegrens")
        if declared_length is not None and total == declared_length:
            break
    else:
        raise PublicSourceRawCapacityError("response overschrijdt de chunkgrens")
    payload = b"".join(chunks)
    if declared_length is not None and len(payload) != declared_length:
        raise PublicSourceRawContractError(
            "responslengte wijkt af van Content-Length"
        )
    return payload


def _fetch(
    spec: PublicSourceHandlerSpec,
    binding: _RequestBinding,
    *,
    deadline: float,
    opener: Callable[..., Any] | None,
) -> _HttpResponse:
    request = Request(
        binding.url,
        headers={
            "Accept": "*/*",
            "Accept-Encoding": "identity",
            "User-Agent": "Storm-Tracker-Worker/PublicSource-ExactRaw-v1",
        },
        method="GET",
    )
    open_call = opener or _open_without_redirects
    try:
        response = open_call(
            request,
            timeout=min(HTTP_IO_TIMEOUT_SECONDS, _remaining(deadline)),
        )
    except (PublicSourceRawError, TimeoutError):
        raise
    except Exception:
        raise PublicSourceRawTransportError(
            "publieke providertransport kon geen response openen"
        ) from None
    try:
        status = getattr(response, "status", None)
        if status is None:
            getter = getattr(response, "getcode", None)
            status = getter() if callable(getter) else None
        if (
            isinstance(status, bool)
            or not isinstance(status, int)
            or not 200 <= status < 300
        ):
            raise PublicSourceRawContractError(
                "providerantwoord is geen succesvolle HTTP-response"
            )
        final_url = str(getattr(response, "geturl", lambda: binding.url)())
        if final_url != binding.url:
            raise PublicSourceRawContractError(
                "providerantwoord wijkt af van de exacte request-URL"
            )
        parsed = urlsplit(final_url)
        if (
            parsed.scheme != "https"
            or parsed.hostname != spec.host
            or parsed.port not in {None, 443}
            or parsed.path != spec.path
            or parsed.username is not None
            or parsed.password is not None
            or parsed.fragment
        ):
            raise PublicSourceRawContractError(
                "providerantwoord ontsnapt uit de HTTPS-allowlist"
            )
        entries = _header_entries(getattr(response, "headers", None))
        encodings = _header_values(entries, "content-encoding")
        if encodings and encodings != ("identity",):
            raise PublicSourceRawContractError(
                "response gebruikt geen exacte identity-representatie"
            )
        declared = _declared_length(entries, spec.maximum_transport_bytes)
        payload = _read_body(
            response,
            declared_length=declared,
            maximum=spec.maximum_transport_bytes,
            deadline=deadline,
        )
        received_at = time.time()
        if not math.isfinite(received_at) or received_at < 0:
            raise PublicSourceRawContractError("ontvangstklok is ongeldig")
        headers_sha256 = hashlib.sha256(
            json.dumps(
                sorted(entries), ensure_ascii=True, allow_nan=False,
                separators=(",", ":"),
            ).encode("ascii")
        ).hexdigest()
        final_url_sha256 = hashlib.sha256(final_url.encode("ascii")).hexdigest()
        content_sha256 = hashlib.sha256(payload).hexdigest()
        media_type = _reported_media_type(entries)
        transport = {
            "content_sha256": content_sha256,
            "declared_length": declared,
            "final_url_sha256": final_url_sha256,
            "received_at": received_at,
            "reported_media_type": media_type,
            "request_sha256": binding.request_sha256,
            "response_headers_sha256": headers_sha256,
            "size_bytes": len(payload),
            "status": status,
            "transport_contract": "stv3-public-source-https-transport-v1",
        }
        return _HttpResponse(
            payload=payload,
            status=status,
            received_at=received_at,
            declared_length=declared,
            reported_media_type=media_type,
            response_headers_sha256=headers_sha256,
            final_url_sha256=final_url_sha256,
            transport_sha256=_digest(transport),
        )
    except (PublicSourceRawError, TimeoutError):
        raise
    except Exception:
        raise PublicSourceRawContractError(
            "provider-responsemetadata is ongeldig"
        ) from None
    finally:
        closer = getattr(response, "close", None)
        if callable(closer):
            try:
                closer()
            except Exception:
                # Closing happens after the bounded body has been copied.  A
                # socket-close anomaly must not discard already received raw
                # data or broaden the request authority.
                pass


def _provenance(
    spec: PublicSourceHandlerSpec,
    binding: _RequestBinding,
    response: _HttpResponse,
) -> dict[str, Any]:
    return {
        "access_class": "public_anonymous_no_secret",
        "capture_stage": NATIVE_CAPTURE_STAGE,
        "collector": "storm_tracker_public_source_exact_raw",
        "collector_version": "public-source-exact-raw-v1",
        "content_encoding": "identity",
        "declared_length": response.declared_length,
        "endpoint_host": spec.host,
        "endpoint_path": spec.path,
        "final_url_sha256": response.final_url_sha256,
        "legal_advice": False,
        "operational_admission": False,
        "parse_attempted": False,
        "provider_rights_claim": False,
        "provenance_kind": spec.provenance_kind,
        "query_count": binding.query_count,
        "reported_media_type": response.reported_media_type,
        "representation": NATIVE_REPRESENTATION,
        "request_sha256": binding.request_sha256,
        "response_headers_sha256": response.response_headers_sha256,
        "response_status": response.status,
        "retention_right_claim": False,
        "semantic_status": "unverified",
        "transport_sha256": response.transport_sha256,
        "weather_state": "unknown",
    }


def _upstream_id(
    spec: PublicSourceHandlerSpec,
    request_sha256: str,
    transport_sha256: str,
    content_sha256: str,
) -> str:
    return (
        f"{spec.host}{spec.path}|request={request_sha256}"
        f"|transport={transport_sha256}|content={content_sha256}"
    )


def _capture_upstream_id(
    spec: PublicSourceHandlerSpec,
    request_sha256: str,
    transport_sha256: str,
    content_sha256: str,
) -> str:
    return (
        f"{spec.capture_product}|request={request_sha256}"
        f"|transport={transport_sha256}|content={content_sha256}"
    )


def _validate_provenance(
    spec: PublicSourceHandlerSpec,
    binding: _RequestBinding,
    receipt: RawArtifactReceipt,
) -> str:
    provenance = receipt.provenance
    fixed = {
        "access_class": "public_anonymous_no_secret",
        "capture_stage": NATIVE_CAPTURE_STAGE,
        "collector": "storm_tracker_public_source_exact_raw",
        "collector_version": "public-source-exact-raw-v1",
        "content_encoding": "identity",
        "endpoint_host": spec.host,
        "endpoint_path": spec.path,
        "legal_advice": False,
        "operational_admission": False,
        "parse_attempted": False,
        "provider_rights_claim": False,
        "provenance_kind": spec.provenance_kind,
        "query_count": binding.query_count,
        "representation": NATIVE_REPRESENTATION,
        "request_sha256": binding.request_sha256,
        "retention_right_claim": False,
        "semantic_status": "unverified",
        "weather_state": "unknown",
    }
    if any(provenance.get(key) != value for key, value in fixed.items()):
        raise PublicSourceRawCorruptionError(
            "duurzame public-sourceprovenance wijkt af"
        )
    for key in (
        "final_url_sha256", "request_sha256", "response_headers_sha256",
        "transport_sha256",
    ):
        if not isinstance(provenance.get(key), str) or _SHA256_RE.fullmatch(
            provenance[key]
        ) is None:
            raise PublicSourceRawCorruptionError(
                "duurzame public-sourcehash wijkt af"
            )
    status = provenance.get("response_status")
    declared = provenance.get("declared_length")
    media_type = provenance.get("reported_media_type")
    if (
        isinstance(status, bool)
        or not isinstance(status, int)
        or not 200 <= status < 300
        or (
            declared is not None
            and (
                isinstance(declared, bool)
                or not isinstance(declared, int)
                or not 0 <= declared <= spec.maximum_transport_bytes
            )
        )
        or not isinstance(media_type, str)
        or not 1 <= len(media_type) <= 256
        or (declared is not None and declared != receipt.size_bytes)
    ):
        raise PublicSourceRawCorruptionError(
            "duurzame public-sourcetransportmetadata wijkt af"
        )
    transport = {
        "content_sha256": receipt.content_sha256,
        "declared_length": declared,
        "final_url_sha256": provenance["final_url_sha256"],
        "received_at": receipt.received_at,
        "reported_media_type": media_type,
        "request_sha256": binding.request_sha256,
        "response_headers_sha256": provenance["response_headers_sha256"],
        "size_bytes": receipt.size_bytes,
        "status": status,
        "transport_contract": "stv3-public-source-https-transport-v1",
    }
    expected_transport = _digest(transport)
    if provenance["transport_sha256"] != expected_transport:
        raise PublicSourceRawCorruptionError(
            "duurzame transportchecksum wijkt af"
        )
    return expected_transport


def _validate_receipt(
    spec: PublicSourceHandlerSpec,
    binding: _RequestBinding,
    job_id: str,
    receipt: RawArtifactReceipt,
) -> str:
    transport_sha256 = _validate_provenance(spec, binding, receipt)
    if (
        receipt.provider != spec.provider
        or receipt.product != spec.raw_product
        or receipt.fetch_id != f"{job_id}{spec.fetch_suffix}"
        or receipt.content_type != UNINTERPRETED_CONTENT_TYPE
        or not 0 <= receipt.size_bytes <= spec.maximum_transport_bytes
        or receipt.upstream_id != _upstream_id(
            spec,
            binding.request_sha256,
            transport_sha256,
            receipt.content_sha256,
        )
    ):
        raise PublicSourceRawCorruptionError(
            "duurzame public-sourcereceipt wijkt af"
        )
    return transport_sha256


def _validate_capture(
    spec: PublicSourceHandlerSpec,
    binding: _RequestBinding,
    job_id: str,
    receipt: RawArtifactReceipt,
    capture: RawArtifactCaptureReceipt,
    transport_sha256: str,
) -> None:
    members = {item.role: item.receipt_id for item in capture.members}
    if (
        capture.raw_store_schema_version != RAW_ARTIFACT_SCHEMA_VERSION
        or capture.provider != spec.provider
        or capture.product != spec.capture_product
        or capture.fetch_id != f"{job_id}{spec.capture_fetch_suffix}"
        or capture.upstream_capture_id != _capture_upstream_id(
            spec,
            binding.request_sha256,
            transport_sha256,
            receipt.content_sha256,
        )
        or capture.product_timestamp is not None
        or members != {spec.role: receipt.receipt_id}
    ):
        raise PublicSourceRawCorruptionError(
            "duurzame public-sourcecapture wijkt af"
        )


def _result(
    spec: PublicSourceHandlerSpec,
    binding: _RequestBinding,
    receipt: RawArtifactReceipt,
    capture: RawArtifactCaptureReceipt,
) -> dict[str, Any]:
    return {
        "capture_state": "captured_not_decoded",
        "contract": RESULT_CONTRACT,
        "decode_status": "not_decoded",
        "handler_id": spec.handler_id,
        "meteorological_claim": False,
        "operational_admission_eligible": False,
        "parse_attempted": False,
        "provider": spec.provider,
        "provider_rights_claim": False,
        "request_sha256": binding.request_sha256,
        "semantic_status": "unverified",
        "weather_state": "unknown",
        "workload_id": spec.workload_id,
        "raw_provenance": {
            "raw_capture_id": capture.capture_id,
            "raw_capture_manifest_sha256": capture.manifest_sha256,
            "raw_capture_receipt_sha256": capture.capture_receipt_sha256,
            "raw_member_count": 1,
            "raw_store_schema_version": RAW_ARTIFACT_SCHEMA_VERSION,
            "sources": {
                spec.role: {
                    "content_sha256": receipt.content_sha256,
                    "receipt_id": receipt.receipt_id,
                    "receipt_sha256": receipt.receipt_sha256,
                    "role": spec.role,
                    "size_bytes": receipt.size_bytes,
                }
            },
        },
    }


def _resume(
    spec: PublicSourceHandlerSpec,
    binding: _RequestBinding,
    job_id: str,
    store: RawArtifactStore,
) -> dict[str, Any] | None:
    capture_id = store._capture_id(  # noqa: SLF001 - durable identity contract
        provider=spec.provider,
        product=spec.capture_product,
        fetch_id=f"{job_id}{spec.capture_fetch_suffix}",
    )
    bundle = store.load_capture_bundle_bytes(capture_id)
    if bundle is not None:
        capture, receipts, payloads = bundle
        if set(receipts) != {spec.role} or set(payloads) != {spec.role}:
            raise PublicSourceRawCorruptionError(
                "duurzame public-sourcecapture mist zijn exacte rol"
            )
        receipt = receipts[spec.role]
    else:
        capture = None
        receipt_id = store._receipt_id(  # noqa: SLF001
            provider=spec.provider,
            product=spec.raw_product,
            fetch_id=f"{job_id}{spec.fetch_suffix}",
        )
        loaded = store.load_receipt_bytes(receipt_id)
        if loaded is None:
            return None
        receipt, _payload = loaded
    transport_sha256 = _validate_receipt(
        spec, binding, job_id, receipt
    )
    if capture is None:
        capture = store.commit_capture(
            provider=spec.provider,
            product=spec.capture_product,
            fetch_id=f"{job_id}{spec.capture_fetch_suffix}",
            upstream_capture_id=_capture_upstream_id(
                spec,
                binding.request_sha256,
                transport_sha256,
                receipt.content_sha256,
            ),
            product_timestamp=None,
            members={spec.role: receipt.receipt_id},
        )
    _validate_capture(
        spec,
        binding,
        job_id,
        receipt,
        capture,
        transport_sha256,
    )
    return _result(spec, binding, receipt, capture)


def capture_public_source_handler(
    handler_id: str,
    job: object,
    *,
    raw_store: RawArtifactStore,
    deadline_at: float | None = None,
    opener: Callable[..., Any] | None = None,
    _fault_injector: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    """Capture one fixed handler response without parsing its payload."""

    try:
        spec = HANDLER_SPECS_BY_ID[handler_id]
    except (KeyError, TypeError):
        raise PublicSourceRawContractError(
            "handler valt buiten de gesloten publieke subset"
        ) from None
    job_id, payload = _job(job)
    binding = _request_binding(spec, payload)
    deadline = _deadline(deadline_at)
    if not isinstance(raw_store, RawArtifactStore):
        raise PublicSourceRawContractError("raw_store heeft het verkeerde type")
    raw_store.initialize()
    if raw_store.limits.max_object_bytes < spec.maximum_transport_bytes:
        raise PublicSourceRawCapacityError(
            "raw store objectgrens is kleiner dan het transportbudget"
        )
    with _CAPTURE_LOCK:
        resumed = _resume(spec, binding, job_id, raw_store)
        if resumed is not None:
            return resumed
        response = _fetch(
            spec, binding, deadline=deadline, opener=opener
        )
        content_sha256 = hashlib.sha256(response.payload).hexdigest()
        receipt = raw_store.store(
            provider=spec.provider,
            product=spec.raw_product,
            fetch_id=f"{job_id}{spec.fetch_suffix}",
            content_type=UNINTERPRETED_CONTENT_TYPE,
            received_at=response.received_at,
            upstream_id=_upstream_id(
                spec,
                binding.request_sha256,
                response.transport_sha256,
                content_sha256,
            ),
            provenance=_provenance(spec, binding, response),
            payload=response.payload,
        )
        if _fault_injector is not None:
            _fault_injector("public_source.after_response_store")
        capture = raw_store.commit_capture(
            provider=spec.provider,
            product=spec.capture_product,
            fetch_id=f"{job_id}{spec.capture_fetch_suffix}",
            upstream_capture_id=_capture_upstream_id(
                spec,
                binding.request_sha256,
                response.transport_sha256,
                receipt.content_sha256,
            ),
            product_timestamp=None,
            members={spec.role: receipt.receipt_id},
        )
        if _fault_injector is not None:
            _fault_injector("public_source.after_capture_commit")
        _validate_capture(
            spec,
            binding,
            job_id,
            receipt,
            capture,
            response.transport_sha256,
        )
        return _result(spec, binding, receipt, capture)


def capture_public_source(
    workload_id: str,
    job: object,
    **kwargs: Any,
) -> dict[str, Any]:
    """Workload-keyed drop-in wrapper for parent-side closed dispatch."""

    try:
        spec = HANDLER_SPECS_BY_WORKLOAD[workload_id]
    except (KeyError, TypeError):
        raise PublicSourceRawContractError(
            "workload valt buiten de gesloten publieke subset"
        ) from None
    return capture_public_source_handler(spec.handler_id, job, **kwargs)


def capture_openmeteo_region(job: object, **kwargs: Any) -> dict[str, Any]:
    return capture_public_source(OPENMETEO_WORKLOAD_ID, job, **kwargs)


def capture_openmeteo_icon2i_region(
    job: object, **kwargs: Any
) -> dict[str, Any]:
    return capture_public_source(OPENMETEO_ICON2I_WORKLOAD_ID, job, **kwargs)


def capture_geosphere_location(job: object, **kwargs: Any) -> dict[str, Any]:
    return capture_public_source(GEOSPHERE_WORKLOAD_ID, job, **kwargs)


def capture_italiameteo_catalog(job: object, **kwargs: Any) -> dict[str, Any]:
    return capture_public_source(ITALIAMETEO_CATALOG_WORKLOAD_ID, job, **kwargs)


def capture_meteolux_location(job: object, **kwargs: Any) -> dict[str, Any]:
    return capture_public_source(METEOLUX_WORKLOAD_ID, job, **kwargs)


def child_handler_contracts() -> tuple[dict[str, Any], ...]:
    """Expose sorted, copy-only integration records for a shared dispatcher."""

    return tuple(
        HANDLER_SPECS_BY_ID[key].child_handler_mapping()
        for key in sorted(HANDLER_SPECS_BY_ID)
    )


__all__ = [
    "CAPTURE_DEADLINE_SECONDS",
    "CATALOG_QUERY_CONTRACT",
    "CONTRACT",
    "GEOSPHERE_HANDLER_ID",
    "GEOSPHERE_WORKLOAD_ID",
    "HANDLER_SPECS_BY_ID",
    "HANDLER_SPECS_BY_WORKLOAD",
    "ITALIAMETEO_CATALOG_HANDLER_ID",
    "ITALIAMETEO_CATALOG_WORKLOAD_ID",
    "LOCATION_QUERY_CONTRACT",
    "METEOLUX_HANDLER_ID",
    "METEOLUX_WORKLOAD_ID",
    "OPENMETEO_HANDLER_ID",
    "OPENMETEO_HOURLY",
    "OPENMETEO_ICON2I_HANDLER_ID",
    "OPENMETEO_ICON2I_WORKLOAD_ID",
    "OPENMETEO_MINUTELY_15",
    "OPENMETEO_QUERY_BATCH_CONTRACT",
    "OPENMETEO_WORKLOAD_ID",
    "PublicSourceHandlerSpec",
    "PublicSourceRawCapacityError",
    "PublicSourceRawContractError",
    "PublicSourceRawCorruptionError",
    "PublicSourceRawError",
    "PublicSourceRawTransportError",
    "RESULT_CONTRACT",
    "capture_geosphere_location",
    "capture_italiameteo_catalog",
    "capture_meteolux_location",
    "capture_openmeteo_icon2i_region",
    "capture_openmeteo_region",
    "capture_public_source",
    "capture_public_source_handler",
    "child_handler_contracts",
]
