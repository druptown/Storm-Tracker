"""Exact, isolated capture candidate for Netatmo ``getpublicdata``.

The candidate stores the exact HTTP response bytes in the schema-3
``RawArtifactStore`` before JSON parsing or meteorological interpretation.
Only a credential-free bounding-box request context is durable.  The OAuth
access token exists solely in memory and in the Authorization request header.

This module is deliberately not wired into the worker service.  Its output is
structural capture evidence, never a wet/dry observation, a coverage claim or
ground confirmation.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import base64
import binascii
import hashlib
import html
import json
import math
import os
from pathlib import Path
import re
import stat
import threading
import time
from typing import Any, Iterator
import unicodedata
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import (
    HTTPRedirectHandler,
    ProxyHandler,
    Request,
    build_opener,
)

from .raw_artifacts import (
    NATIVE_CAPTURE_STAGE,
    NATIVE_REPRESENTATION,
    RawArtifactCaptureReceipt,
    RawArtifactNotFoundError,
    RawArtifactReceipt,
    RawArtifactStore,
)


NETATMO_ORIGIN = "https://api.netatmo.com"
NETATMO_PUBLIC_DATA_PATH = "/api/getpublicdata"
NETATMO_PUBLIC_DATA_URL = f"{NETATMO_ORIGIN}{NETATMO_PUBLIC_DATA_PATH}"

NETATMO_RAW_SHARE_ROOT = Path("/share/storm_tracker_worker")
NETATMO_RAW_ARTIFACT_ROOT = (
    NETATMO_RAW_SHARE_ROOT / "raw" / "netatmo_public_data"
)
NETATMO_JSON_PRODUCT = "netatmo_getpublicdata_json_response"
NETATMO_CAPTURE_PRODUCT = "netatmo_getpublicdata_query_capture"
NETATMO_RAW_COLLECTOR = "netatmo_getpublicdata_exact_response"
NETATMO_RAW_COLLECTOR_VERSION = "worker-netatmo-getpublicdata-exact-raw-v2"
NETATMO_STRUCTURE_INSPECTOR_VERSION = "netatmo-getpublicdata-structure-v1"
NETATMO_QUERY_CONTEXT_SCHEMA = "stv3-netatmo-getpublicdata-query-context-v2"
NETATMO_VALIDATION_SCHEMA = "stv3-netatmo-structure-verdict-v1"
NETATMO_ADMISSION_STATUS = "candidate_unwired_parser_not_certified"
NETATMO_BROKER_EFFECT_CONTRACT = (
    "stv3-worker-effect-netatmo-parent-broker-v1"
)

MAX_RESPONSE_BYTES = 8 * 1024 * 1024
MAX_JSON_NODES = 200_000
MAX_JSON_DEPTH = 48
MAX_JSON_OBJECT_MEMBERS = 10_000
MAX_JSON_ARRAY_ITEMS = 20_000
MAX_JSON_STRING_CHARS = 256 * 1024
MAX_JSON_KEY_CHARS = 512
MAX_STATIONS = 4_096
MAX_MODULES_PER_STATION = 64
MAX_TIMESTAMP_TOKENS_PER_MODULE = 2_048
MAX_DURABLE_ID_CHARS = 128
MAX_ACCESS_TOKEN_CHARS = 2_048
MIN_ACCESS_TOKEN_CHARS = 16
MIN_RADIUS_KM = 1.0
MAX_RADIUS_KM = 20.0
EARTH_RADIUS_KM = 6371.0088
MAX_TRANSPORT_ATTEMPTS = 3
HTTP_IO_TIMEOUT_SECONDS = 10.0
JOB_TOTAL_DEADLINE_SECONDS = 35.0
HTTP_READ_CHUNK_BYTES = 64 * 1024
LOCK_POLL_SECONDS = 0.05
MAX_VALIDATION_VERDICTS = 2_048
MAX_VALIDATION_VERDICT_BYTES = 2 * 1024 * 1024
MAX_VALIDATION_TOTAL_BYTES = 128 * 1024 * 1024
MAX_SECRET_NORMALIZATION_ROUNDS = 16
MAX_LOCK_SHARDS = 64

_PAYLOAD_KEYS = frozenset({
    "latitude",
    "longitude",
    "radius_km",
    "query_key",
    "query_kind",
})
_QUERY_KINDS = frozenset({"local", "roaming"})
_ALLOWED_CONTENT_TYPES = frozenset({"application/json"})
_CANONICAL_ID_RE = re.compile(r"^[a-z0-9]+(?:[-_.:@/][a-z0-9]+)*$")
_FORBIDDEN_ID_SEGMENT_RE = re.compile(
    r"(?:^|[-_.:@/])(access[-_]?token|refresh[-_]?token|authorization|"
    r"cookie|client[-_]?secret|api[-_]?key|secret)(?:$|[-_.:@/])",
    re.IGNORECASE,
)
_FORBIDDEN_RESPONSE_KEY_RE = re.compile(
    rb'["\']\s*(?:access[_-]?token|refresh[_-]?token|authorization|'
    rb'set[_-]?cookie|cookie|client[_-]?(?:id|secret)|application[_-]?key|'
    rb'api[_-]?key)\s*["\']\s*:',
    re.IGNORECASE,
)
_PERCENT_ESCAPE_RE = re.compile(br"%([0-9A-Fa-f]{2})")
_ASCII_ESCAPE_RE = re.compile(
    br"\\(?:u([0-9A-Fa-f]{4})|U([0-9A-Fa-f]{8})|x([0-9A-Fa-f]{2}))"
)
_TIMESTAMP_FIELDS = frozenset({
    "time_utc",
    "timeutc",
    "rain_timeutc",
    "rain_utc",
    "date_setup",
    "last_setup",
    "last_status_store",
    "last_upgrade",
    "last_message",
    "time_server",
})

_NETATMO_RAW_STORE: RawArtifactStore | None = None
_NETATMO_RAW_STORE_LOCK = threading.Lock()
_NETATMO_CAPTURE_LOCK = threading.Lock()
_SECRET_SCAN_ATTESTATION = object()


class NetatmoNativeTransportError(RuntimeError):
    """A retryable, detail-free transport failure."""


class NetatmoNativeContractError(ValueError):
    """A fail-closed local response or capture-contract violation."""


class NetatmoNativeDeadlineError(NetatmoNativeContractError):
    """Expiry of the single absolute monotone job deadline."""


class NetatmoNativeProviderResponseError(NetatmoNativeContractError):
    """A safe HTTP or API response class without response text."""

    def __init__(
        self,
        message: str = "Netatmo providerresponse is niet bruikbaar",
        *,
        http_status: int | None = None,
        retryable: bool = False,
    ) -> None:
        super().__init__(message)
        self.http_status = http_status
        self.retryable = bool(retryable)


@dataclass(frozen=True, slots=True)
class NetatmoQuery:
    latitude: float
    longitude: float
    radius_km: float
    query_key: str
    query_kind: str
    lat_ne: float
    lon_ne: float
    lat_sw: float
    lon_sw: float


@dataclass(frozen=True, slots=True)
class NetatmoHttpResponse:
    payload: bytes
    content_type: str
    received_at: float
    transport_attempt: int
    secret_scan_attestation: object | None = None


@dataclass(frozen=True, slots=True)
class NetatmoJsonIdentity:
    """Structural identities/timestamps only; no measurement interpretation."""

    station_structures: tuple[dict[str, object], ...]
    response_timestamp_tokens: tuple[dict[str, object], ...]

    @property
    def station_count(self) -> int:
        return len(self.station_structures)

    def to_document(self) -> dict[str, object]:
        return {
            "station_structures": [
                _json_copy(item) for item in self.station_structures
            ],
            "response_timestamp_tokens": [
                _json_copy(item) for item in self.response_timestamp_tokens
            ],
        }


class _RejectRedirects(HTTPRedirectHandler):
    """Never forward the Authorization header to another origin."""

    def redirect_request(self, *_args, **_kwargs):
        return None


def _json_copy(value: object) -> Any:
    return json.loads(_canonical_json_bytes(value))


def _canonical_json_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _new_deadline() -> float:
    value = float(time.monotonic())
    if not math.isfinite(value):
        raise NetatmoNativeContractError("Netatmo monotone klok is ongeldig")
    return value + JOB_TOTAL_DEADLINE_SECONDS


def _remaining_seconds(deadline: float, phase: str) -> float:
    remaining = float(deadline) - float(time.monotonic())
    if not math.isfinite(remaining) or remaining <= 0.0:
        raise NetatmoNativeDeadlineError(
            f"Netatmo job overschrijdt de absolute deadline ({phase})"
        )
    return remaining


def _finite_number(value: object, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise NetatmoNativeContractError(
            f"Netatmo {label} moet een eindig getal zijn"
        )
    result = float(value)
    if not math.isfinite(result):
        raise NetatmoNativeContractError(
            f"Netatmo {label} moet een eindig getal zijn"
        )
    return result


def _validate_access_token(value: object) -> str:
    if (
        not isinstance(value, str)
        or not MIN_ACCESS_TOKEN_CHARS <= len(value) <= MAX_ACCESS_TOKEN_CHARS
        or any(ord(character) < 33 or ord(character) > 126 for character in value)
    ):
        raise NetatmoNativeContractError(
            "Netatmo brokered access credential ontbreekt of is ongeldig"
        )
    return value


def _access_token(job: object, explicit: str | None) -> str:
    if explicit is not None:
        return _validate_access_token(explicit)
    context = getattr(job, "execution_context", None)
    if type(context) is not dict or frozenset(context) not in {
        frozenset({"netatmo_access_token"}),
        frozenset({"effect_contract_id", "netatmo_access_token"}),
    }:
        raise NetatmoNativeContractError(
            "Netatmo native capture vereist een brokered access credential"
        )
    if (
        "effect_contract_id" in context
        and context["effect_contract_id"] != NETATMO_BROKER_EFFECT_CONTRACT
    ):
        raise NetatmoNativeContractError(
            "Netatmo execution-context heeft een onbekend effectcontract"
        )
    return _validate_access_token(context["netatmo_access_token"])


def _secret_variants(secret: str) -> frozenset[bytes]:
    raw = secret.encode("ascii")
    variants = {
        raw,
        quote(secret, safe="").encode("ascii"),
        json.dumps(secret)[1:-1].encode("ascii"),
        html.escape(secret).encode("ascii"),
        base64.b64encode(raw),
        base64.urlsafe_b64encode(raw),
        base64.b32encode(raw),
        binascii.hexlify(raw),
    }
    return frozenset(item for item in variants if item)


def _decode_ascii_escapes(value: bytes) -> bytes:
    def replace(match: re.Match[bytes]) -> bytes:
        encoded = next(group for group in match.groups() if group is not None)
        codepoint = int(encoded, 16)
        if codepoint > 0x10FFFF or 0xD800 <= codepoint <= 0xDFFF:
            return match.group(0)
        return chr(codepoint).encode("utf-8")

    return _ASCII_ESCAPE_RE.sub(replace, value)


def _normalize_secret_surface(value: bytes) -> bytes:
    result = _PERCENT_ESCAPE_RE.sub(
        lambda match: bytes((int(match.group(1), 16),)),
        value,
    )
    # JSON can preserve a literal escape as ``\\u....``. Collapse exactly one
    # escaping layer per bounded normalization round before decoding Unicode.
    result = result.replace(b"\\\\", b"\\")
    result = _decode_ascii_escapes(result)
    try:
        decoded = result.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        return result
    encoded = unicodedata.normalize("NFKC", html.unescape(decoded)).encode("utf-8")
    if len(encoded) > MAX_RESPONSE_BYTES:
        raise NetatmoNativeContractError(
            "Netatmo credentialnormalisatie overschrijdt de bytegrens"
        )
    return encoded


def _contains_split_secret(value: bytes, secret: str) -> bool:
    # A provider echo may insert visual separators between token characters.
    # Whitespace is included because JSON string escaping can disguise it.
    separators = frozenset(b"-_.:@/ \t\r\n")
    compact_value = bytes(byte for byte in value if byte not in separators)
    for variant in _secret_variants(secret):
        compact_variant = bytes(
            byte for byte in variant if byte not in separators
        )
        if (
            len(compact_variant) >= MIN_ACCESS_TOKEN_CHARS
            and compact_variant in compact_value
        ):
            return True
    return False


def _reject_secret_echo(
    payload: bytes,
    secret: str,
    *,
    deadline: float,
) -> None:
    if not isinstance(payload, bytes) or len(payload) > MAX_RESPONSE_BYTES:
        raise NetatmoNativeContractError(
            "Netatmo-response overschrijdt het securitycontract"
        )
    variants = _secret_variants(secret)
    current = payload
    seen: set[bytes] = set()
    for _round in range(MAX_SECRET_NORMALIZATION_ROUNDS):
        _remaining_seconds(deadline, "credentialscan")
        if current in seen:
            break
        seen.add(current)
        if any(variant in current for variant in variants):
            raise NetatmoNativeContractError(
                "Netatmo-response bevat verboden credentialmateriaal"
            )
        if _contains_split_secret(current, secret):
            raise NetatmoNativeContractError(
                "Netatmo-response bevat verboden credentialmateriaal"
            )
        if _FORBIDDEN_RESPONSE_KEY_RE.search(current):
            raise NetatmoNativeContractError(
                "Netatmo-response bevat verboden authmetadata"
            )
        normalized = _normalize_secret_surface(current)
        if normalized == current:
            break
        current = normalized


def _ensure_identifier_safe(value: str, credential: str, label: str) -> None:
    encoded = value.encode("utf-8")
    if (
        credential.encode("ascii") in encoded
        or any(item in encoded for item in _secret_variants(credential))
        or _FORBIDDEN_ID_SEGMENT_RE.search(value) is not None
    ):
        raise NetatmoNativeContractError(
            f"Netatmo {label} bevat verboden credentialmetadata"
        )


def _validate_job_id(value: object, credential: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value) > MAX_DURABLE_ID_CHARS
        or _CANONICAL_ID_RE.fullmatch(value) is None
    ):
        raise NetatmoNativeContractError(
            "Netatmo native capture mist een canonieke job_id"
        )
    _ensure_identifier_safe(value, credential, "job_id")
    return value


def _validate_query_payload(payload: object, credential: str) -> NetatmoQuery:
    if type(payload) is not dict or frozenset(payload) != _PAYLOAD_KEYS:
        raise NetatmoNativeContractError(
            "Netatmo native capture vereist exact vijf queryvelden"
        )
    latitude = _finite_number(payload["latitude"], "latitude")
    longitude = _finite_number(payload["longitude"], "longitude")
    radius_km = _finite_number(payload["radius_km"], "radius_km")
    if not -90.0 <= latitude <= 90.0:
        raise NetatmoNativeContractError("Netatmo latitude valt buiten bereik")
    if not -180.0 <= longitude <= 180.0:
        raise NetatmoNativeContractError("Netatmo longitude valt buiten bereik")
    if not MIN_RADIUS_KM <= radius_km <= MAX_RADIUS_KM:
        raise NetatmoNativeContractError(
            "Netatmo radius_km valt buiten het capturecontract"
        )
    query_key = payload["query_key"]
    if (
        not isinstance(query_key, str)
        or not query_key
        or len(query_key) > MAX_DURABLE_ID_CHARS
        or _CANONICAL_ID_RE.fullmatch(query_key) is None
    ):
        raise NetatmoNativeContractError(
            "Netatmo query_key is geen canonieke korte identiteit"
        )
    _ensure_identifier_safe(query_key, credential, "query_key")
    query_kind = payload["query_kind"]
    if not isinstance(query_kind, str) or query_kind not in _QUERY_KINDS:
        raise NetatmoNativeContractError(
            "Netatmo query_kind valt buiten het capturecontract"
        )

    # The public API accepts a bounding box, not a geodesic radius.  Preserve
    # that distinction and reject dateline/pole crossings instead of silently
    # changing the requested geometry.
    latitude = 0.0 if latitude == 0.0 else latitude
    longitude = 0.0 if longitude == 0.0 else longitude
    angular_radius = radius_km / EARTH_RADIUS_KM
    cos_latitude = math.cos(math.radians(latitude))
    lat_delta = math.degrees(angular_radius)
    if abs(cos_latitude) < 1e-12:
        raise NetatmoNativeContractError(
            "Netatmo bounding box is niet definieerbaar aan de pool"
        )
    longitude_ratio = math.sin(angular_radius) / abs(cos_latitude)
    if longitude_ratio >= 1.0:
        raise NetatmoNativeContractError(
            "Netatmo bounding box omvat een pool"
        )
    lon_delta = math.degrees(math.asin(longitude_ratio))
    scale = 1_000_000.0
    # Round every wire boundary outwards.  The six-decimal serialization below
    # can therefore never shrink the requested spherical circle.
    lat_ne = math.ceil((latitude + lat_delta) * scale) / scale
    lat_sw = math.floor((latitude - lat_delta) * scale) / scale
    lon_ne = math.ceil((longitude + lon_delta) * scale) / scale
    lon_sw = math.floor((longitude - lon_delta) * scale) / scale
    if lat_ne > 90.0 or lat_sw < -90.0 or lon_ne > 180.0 or lon_sw < -180.0:
        raise NetatmoNativeContractError(
            "Netatmo bounding box kruist pool of datumgrens"
        )
    return NetatmoQuery(
        latitude=latitude,
        longitude=longitude,
        radius_km=radius_km,
        query_key=query_key,
        query_kind=query_kind,
        lat_ne=lat_ne,
        lon_ne=lon_ne,
        lat_sw=lat_sw,
        lon_sw=lon_sw,
    )


def _query_parameters(query: NetatmoQuery) -> tuple[tuple[str, str], ...]:
    return (
        ("lat_ne", f"{query.lat_ne:.6f}"),
        ("lon_ne", f"{query.lon_ne:.6f}"),
        ("lat_sw", f"{query.lat_sw:.6f}"),
        ("lon_sw", f"{query.lon_sw:.6f}"),
        ("filter", "true"),
    )


def _query_context(query: NetatmoQuery) -> dict[str, object]:
    return {
        "schema": NETATMO_QUERY_CONTEXT_SCHEMA,
        "method": "GET",
        "origin": NETATMO_ORIGIN,
        "path": NETATMO_PUBLIC_DATA_PATH,
        "query_identity": query.query_key,
        "query_kind": query.query_kind,
        "requested_center": {
            "latitude": query.latitude,
            "longitude": query.longitude,
        },
        "requested_radius_km": query.radius_km,
        "wire_geometry": "bounding_box_not_geodesic_circle",
        "parameters": [
            {"name": name, "value": value}
            for name, value in _query_parameters(query)
        ],
        "coverage_status": "unknown_not_proven_by_station_count",
    }


def _query_context_sha256(context: dict[str, object]) -> str:
    return hashlib.sha256(_canonical_json_bytes(context)).hexdigest()


def _open_public_request(request: Request, *, timeout: float):
    opener = build_opener(ProxyHandler({}), _RejectRedirects())
    return opener.open(request, timeout=timeout)


def _declared_content_length(headers: object) -> int | None:
    value = headers.get("Content-Length") if hasattr(headers, "get") else None
    if value is None:
        return None
    try:
        result = int(value)
    except (TypeError, ValueError) as error:
        raise NetatmoNativeContractError(
            "Netatmo Content-Length is ongeldig"
        ) from error
    if result < 0 or result > MAX_RESPONSE_BYTES:
        raise NetatmoNativeContractError(
            "Netatmo-response overschrijdt de bytegrens"
        )
    return result


def _content_type(headers: object) -> str:
    value = headers.get("Content-Type") if hasattr(headers, "get") else None
    if not isinstance(value, str):
        raise NetatmoNativeContractError("Netatmo-response mist Content-Type")
    normalized = value.split(";", 1)[0].strip().lower()
    if normalized not in _ALLOWED_CONTENT_TYPES:
        raise NetatmoNativeContractError(
            "Netatmo-response heeft een onverwacht Content-Type"
        )
    return normalized


def _read_response_body(
    response: object,
    *,
    declared_bytes: int | None,
    deadline: float,
) -> bytes:
    chunks: list[bytes] = []
    total = 0
    maximum_chunks = (MAX_RESPONSE_BYTES // HTTP_READ_CHUNK_BYTES) + 2
    for _ in range(maximum_chunks):
        _remaining_seconds(deadline, "response-read")
        chunk = response.read(HTTP_READ_CHUNK_BYTES)
        if not chunk:
            break
        if not isinstance(chunk, bytes):
            raise NetatmoNativeContractError(
                "Netatmo-transport leverde geen exacte bytes"
            )
        total += len(chunk)
        if total > MAX_RESPONSE_BYTES:
            raise NetatmoNativeContractError(
                "Netatmo-response overschrijdt de bytegrens"
            )
        chunks.append(chunk)
    else:
        raise NetatmoNativeContractError(
            "Netatmo-response overschrijdt de chunkgrens"
        )
    payload = b"".join(chunks)
    if not payload:
        raise NetatmoNativeContractError("Netatmo-response is leeg")
    if declared_bytes is not None and len(payload) != declared_bytes:
        raise NetatmoNativeTransportError(
            "Netatmo-response werd niet volledig ontvangen"
        )
    return payload


def _retryable_http_status(status: int) -> bool:
    return status in {408, 425, 429, 500, 502, 503, 504}


def _download_once(
    query: NetatmoQuery,
    *,
    credential: str,
    deadline: float,
    transport_attempt: int,
) -> NetatmoHttpResponse:
    encoded = urlencode(_query_parameters(query))
    request = Request(
        f"{NETATMO_PUBLIC_DATA_URL}?{encoded}",
        headers={
            "Accept": "application/json",
            "Accept-Encoding": "identity",
            "Authorization": f"Bearer {credential}",
            "Cache-Control": "no-cache",
        },
        method="GET",
    )
    timeout = min(
        HTTP_IO_TIMEOUT_SECONDS,
        _remaining_seconds(deadline, "request-open"),
    )
    try:
        with _open_public_request(request, timeout=timeout) as response:
            status = int(getattr(response, "status", response.getcode()))
            if status != 200:
                raise NetatmoNativeProviderResponseError(
                    "Netatmo HTTP-response is niet succesvol",
                    http_status=status,
                    retryable=_retryable_http_status(status),
                )
            final_url = str(response.geturl())
            if final_url != request.full_url:
                raise NetatmoNativeContractError(
                    "Netatmo-response komt niet van de exacte request-URL"
                )
            content_type = _content_type(response.headers)
            declared = _declared_content_length(response.headers)
            payload = _read_response_body(
                response,
                declared_bytes=declared,
                deadline=deadline,
            )
    except HTTPError as error:
        status = int(error.code)
        raise NetatmoNativeProviderResponseError(
            "Netatmo HTTP-response is niet succesvol",
            http_status=status,
            retryable=_retryable_http_status(status),
        ) from None
    except NetatmoNativeContractError:
        raise
    except (URLError, TimeoutError, OSError, ValueError):
        raise NetatmoNativeTransportError(
            "Netatmo transport is tijdelijk niet beschikbaar"
        ) from None
    _reject_secret_echo(payload, credential, deadline=deadline)
    return NetatmoHttpResponse(
        payload=payload,
        content_type=content_type,
        received_at=float(time.time()),
        transport_attempt=transport_attempt,
        secret_scan_attestation=_SECRET_SCAN_ATTESTATION,
    )


def _bounded_backoff(attempt: int, deadline: float) -> None:
    delay = min(0.25 * (2 ** (attempt - 1)), 1.0)
    remaining = _remaining_seconds(deadline, "retry-backoff")
    if remaining <= delay:
        raise NetatmoNativeDeadlineError(
            "Netatmo job heeft geen retrybudget meer"
        )
    time.sleep(delay)


def _download_with_retries(
    query: NetatmoQuery,
    *,
    credential: str,
    deadline: float,
) -> NetatmoHttpResponse:
    last_error: NetatmoNativeContractError | RuntimeError | None = None
    for attempt in range(1, MAX_TRANSPORT_ATTEMPTS + 1):
        try:
            return _download_once(
                query,
                credential=credential,
                deadline=deadline,
                transport_attempt=attempt,
            )
        except NetatmoNativeDeadlineError:
            raise
        except NetatmoNativeTransportError as error:
            last_error = error
        except NetatmoNativeProviderResponseError as error:
            if not error.retryable:
                raise
            last_error = error
        if attempt < MAX_TRANSPORT_ATTEMPTS:
            _bounded_backoff(attempt, deadline)
    if isinstance(last_error, NetatmoNativeProviderResponseError):
        raise NetatmoNativeProviderResponseError(
            "Netatmo retrybare HTTP-response bleef mislukken",
            http_status=last_error.http_status,
            retryable=True,
        ) from None
    raise NetatmoNativeTransportError(
        "Netatmo transport bleef na begrensde retries mislukken"
    ) from None


def _reject_duplicate_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise NetatmoNativeContractError(
                "Netatmo JSON bevat een dubbele objectsleutel"
            )
        result[key] = value
    return result


def _parse_json(payload: bytes) -> object:
    try:
        text = payload.decode("utf-8", errors="strict")
        return json.loads(
            text,
            object_pairs_hook=_reject_duplicate_object,
            parse_constant=lambda _value: (_ for _ in ()).throw(
                NetatmoNativeContractError(
                    "Netatmo JSON bevat een niet-eindig getal"
                )
            ),
        )
    except NetatmoNativeContractError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError, MemoryError):
        raise NetatmoNativeContractError(
            "Netatmo-response is geen geldige begrensde UTF-8 JSON"
        ) from None


def _validate_json_bounds(document: object, *, deadline: float) -> None:
    stack: list[tuple[object, int]] = [(document, 1)]
    nodes = 0
    while stack:
        value, depth = stack.pop()
        nodes += 1
        if nodes % 1024 == 0:
            _remaining_seconds(deadline, "JSON-structuurscan")
        if nodes > MAX_JSON_NODES:
            raise NetatmoNativeContractError(
                "Netatmo JSON overschrijdt de nodegrens"
            )
        if depth > MAX_JSON_DEPTH:
            raise NetatmoNativeContractError(
                "Netatmo JSON overschrijdt de dieptelimiet"
            )
        if isinstance(value, dict):
            if len(value) > MAX_JSON_OBJECT_MEMBERS:
                raise NetatmoNativeContractError(
                    "Netatmo JSON-object overschrijdt de membergrens"
                )
            for key, child in value.items():
                if not isinstance(key, str) or len(key) > MAX_JSON_KEY_CHARS:
                    raise NetatmoNativeContractError(
                        "Netatmo JSON bevat een onbegrensde objectsleutel"
                    )
                stack.append((child, depth + 1))
        elif isinstance(value, list):
            if len(value) > MAX_JSON_ARRAY_ITEMS:
                raise NetatmoNativeContractError(
                    "Netatmo JSON-array overschrijdt de itemgrens"
                )
            stack.extend((item, depth + 1) for item in value)
        elif isinstance(value, str):
            if len(value) > MAX_JSON_STRING_CHARS:
                raise NetatmoNativeContractError(
                    "Netatmo JSON-string overschrijdt de tekenlimiet"
                )
        elif isinstance(value, float) and not math.isfinite(value):
            raise NetatmoNativeContractError(
                "Netatmo JSON bevat een niet-eindig getal"
            )
        elif value is not None and not isinstance(value, (bool, int, float)):
            raise NetatmoNativeContractError(
                "Netatmo JSON bevat een onbekend waardetype"
            )


def _timestamp_token(source: str, value: object) -> dict[str, object]:
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        raise NetatmoNativeContractError(
            "Netatmo upstream timestamp heeft een onbekend type"
        )
    if isinstance(value, float) and not math.isfinite(value):
        raise NetatmoNativeContractError(
            "Netatmo upstream timestamp is niet eindig"
        )
    if isinstance(value, str) and (not value or len(value) > 128):
        raise NetatmoNativeContractError(
            "Netatmo upstream timestampstring is onbegrensd"
        )
    return {
        "source": source,
        "json_type": (
            "string" if isinstance(value, str)
            else "integer" if isinstance(value, int)
            else "number"
        ),
        "value": value,
    }


def _inspect_response(payload: bytes, *, deadline: float) -> NetatmoJsonIdentity:
    _remaining_seconds(deadline, "JSON-parse-start")
    document = _parse_json(payload)
    _remaining_seconds(deadline, "JSON-parse-einde")
    _validate_json_bounds(document, deadline=deadline)
    if not isinstance(document, dict):
        raise NetatmoNativeContractError(
            "Netatmo-response heeft geen JSON-object als root"
        )
    status_value = document.get("status")
    if status_value != "ok":
        raise NetatmoNativeProviderResponseError(
            "Netatmo API-status is niet succesvol",
            retryable=True,
        )
    body = document.get("body")
    if not isinstance(body, list) or len(body) > MAX_STATIONS:
        raise NetatmoNativeContractError(
            "Netatmo-response heeft een onbegrensde stationslijst"
        )

    response_timestamps: list[dict[str, object]] = []
    # ``time_exec`` is a duration, not an upstream timestamp.
    for field in ("time_server",):
        if field in document:
            response_timestamps.append(_timestamp_token(field, document[field]))

    station_structures: list[dict[str, object]] = []
    seen_stations: set[str] = set()
    for station_index, station in enumerate(body):
        _remaining_seconds(deadline, "station-structuurscan")
        if not isinstance(station, dict):
            raise NetatmoNativeContractError(
                "Netatmo stationslijst bevat geen object"
            )
        station_id = station.get("_id")
        if (
            not isinstance(station_id, str)
            or not station_id
            or len(station_id) > 256
            or any(ord(character) < 32 for character in station_id)
        ):
            raise NetatmoNativeContractError(
                "Netatmo station mist een begrensde provideridentiteit"
            )
        if station_id in seen_stations:
            raise NetatmoNativeContractError(
                "Netatmo-response bevat een dubbele stationidentiteit"
            )
        seen_stations.add(station_id)
        place = station.get("place")
        location = place.get("location") if isinstance(place, dict) else None
        if not isinstance(location, list) or len(location) != 2:
            raise NetatmoNativeContractError(
                "Netatmo station mist een begrensde locatie"
            )
        longitude = _finite_number(location[0], "stationlongitude")
        latitude = _finite_number(location[1], "stationlatitude")
        if not -180.0 <= longitude <= 180.0 or not -90.0 <= latitude <= 90.0:
            raise NetatmoNativeContractError(
                "Netatmo stationlocatie valt buiten bereik"
            )
        measures = station.get("measures")
        if not isinstance(measures, dict) or len(measures) > MAX_MODULES_PER_STATION:
            raise NetatmoNativeContractError(
                "Netatmo station mist een begrensde measures-structuur"
            )
        module_structures: list[dict[str, object]] = []
        for module_id, measure in measures.items():
            if (
                not isinstance(module_id, str)
                or not module_id
                or len(module_id) > 256
                or not isinstance(measure, dict)
            ):
                raise NetatmoNativeContractError(
                    "Netatmo measure mist een begrensde module-identiteit"
                )
            timestamps: list[dict[str, object]] = []
            for field in sorted(_TIMESTAMP_FIELDS):
                if field in measure:
                    timestamps.append(_timestamp_token(field, measure[field]))
            results = measure.get("res")
            if results is not None:
                if (
                    not isinstance(results, dict)
                    or len(results) > MAX_TIMESTAMP_TOKENS_PER_MODULE
                ):
                    raise NetatmoNativeContractError(
                        "Netatmo res-structuur overschrijdt de timestampgrens"
                    )
                for raw_timestamp in results:
                    if not raw_timestamp or len(raw_timestamp) > 128:
                        raise NetatmoNativeContractError(
                            "Netatmo res bevat een onbegrensde timestampkey"
                        )
                    timestamps.append(_timestamp_token("res_key", raw_timestamp))
            if len(timestamps) > MAX_TIMESTAMP_TOKENS_PER_MODULE + len(
                _TIMESTAMP_FIELDS
            ):
                raise NetatmoNativeContractError(
                    "Netatmo module overschrijdt de timestampgrens"
                )
            module_structures.append({
                "module_id": module_id,
                "upstream_timestamp_tokens": timestamps,
            })
        station_timestamps: list[dict[str, object]] = []
        for field in sorted(_TIMESTAMP_FIELDS):
            if field in station:
                station_timestamps.append(_timestamp_token(field, station[field]))
        station_structures.append({
            "station_index": station_index,
            "station_id": station_id,
            "station_upstream_timestamp_tokens": station_timestamps,
            "modules": module_structures,
        })
    return NetatmoJsonIdentity(
        station_structures=tuple(station_structures),
        response_timestamp_tokens=tuple(response_timestamps),
    )


def _raw_store(injected: RawArtifactStore | None = None) -> RawArtifactStore:
    if injected is not None:
        injected.initialize()
        return injected
    share_root = NETATMO_RAW_SHARE_ROOT.resolve(strict=False)
    raw_root = NETATMO_RAW_ARTIFACT_ROOT.resolve(strict=False)
    if raw_root == share_root or share_root not in raw_root.parents:
        raise RuntimeError("Netatmo raw-artifactpad ontsnapt uit worker-share")
    global _NETATMO_RAW_STORE
    with _NETATMO_RAW_STORE_LOCK:
        if _NETATMO_RAW_STORE is None:
            candidate = RawArtifactStore(raw_root)
            candidate.initialize()
            _NETATMO_RAW_STORE = candidate
        return _NETATMO_RAW_STORE


def _safe_auxiliary_directory(store: RawArtifactStore, name: str) -> Path:
    root = store.root.resolve(strict=True)
    path = store.root / name
    if path.exists() or path.is_symlink():
        info = path.lstat()
        if path.is_symlink() or not stat.S_ISDIR(info.st_mode):
            raise NetatmoNativeContractError(
                "Netatmo hulpmappad is onveilig"
            )
    else:
        path.mkdir(mode=0o700, exist_ok=True)
        _fsync_directory(store.root)
    info = path.lstat()
    if path.is_symlink() or not stat.S_ISDIR(info.st_mode):
        raise NetatmoNativeContractError("Netatmo hulpmappad is onveilig")
    if path.resolve(strict=True).parent != root:
        raise NetatmoNativeContractError("Netatmo hulpmappad ontsnapt uit raw-root")
    return path


def _fsync_directory(path: Path) -> None:
    if os.name == "nt":
        return
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


@contextmanager
def _deadline_file_lock(path: Path, *, deadline: float) -> Iterator[None]:
    _remaining_seconds(deadline, "proceslock-open")
    path.parent.mkdir(mode=0o700, exist_ok=True)
    flags = (
        os.O_CREAT
        | os.O_RDWR
        | getattr(os, "O_BINARY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    descriptor = os.open(path, flags, 0o600)
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode):
            raise NetatmoNativeContractError(
                "Netatmo proceslock is geen regulier bestand"
            )
        path_info = path.lstat()
        if path.is_symlink() or not stat.S_ISREG(path_info.st_mode):
            raise NetatmoNativeContractError("Netatmo proceslock is onveilig")
        if path_info.st_size > 1:
            raise NetatmoNativeContractError(
                "Netatmo proceslock overschrijdt de bytegrens"
            )
        if os.name != "nt" and (
            path_info.st_dev != info.st_dev or path_info.st_ino != info.st_ino
        ):
            raise NetatmoNativeContractError(
                "Netatmo proceslock veranderde tijdens openen"
            )
        while True:
            try:
                if os.name == "nt":
                    import msvcrt

                    os.lseek(descriptor, 0, os.SEEK_SET)
                    if info.st_size == 0:
                        os.write(descriptor, b"\0")
                        os.fsync(descriptor)
                    os.lseek(descriptor, 0, os.SEEK_SET)
                    msvcrt.locking(descriptor, msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except (BlockingIOError, OSError):
                time.sleep(min(LOCK_POLL_SECONDS, _remaining_seconds(
                    deadline, "proceslock-wacht"
                )))
        try:
            yield
        finally:
            if os.name == "nt":
                import msvcrt

                os.lseek(descriptor, 0, os.SEEK_SET)
                msvcrt.locking(descriptor, msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(descriptor, fcntl.LOCK_UN)
    finally:
        os.close(descriptor)


def _lock_path(store: RawArtifactStore, label: str, identity: str) -> Path:
    digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()
    directory = _safe_auxiliary_directory(store, "netatmo_locks")
    if label == "slot":
        shard = int(digest[:8], 16) % MAX_LOCK_SHARDS
        filename = f"slot-{shard:02x}.lock"
    elif label in {"capture-capacity", "validation-capacity"}:
        filename = f"{label}.lock"
    else:
        raise NetatmoNativeContractError("Netatmo proceslocklabel is onbekend")
    allowed = re.compile(
        r"(?:capture-capacity|validation-capacity|slot-[0-9a-f]{2})\.lock"
    )
    entries = list(directory.iterdir())
    if len(entries) > MAX_LOCK_SHARDS + 2:
        raise NetatmoNativeContractError(
            "Netatmo proceslockmap overschrijdt de entrygrens"
        )
    for entry in entries:
        if (
            entry.is_symlink()
            or not entry.is_file()
            or allowed.fullmatch(entry.name) is None
        ):
            raise NetatmoNativeContractError(
                "Netatmo proceslockmap bevat een onbekend bestand"
            )
    return directory / filename


def _validation_directory(store: RawArtifactStore) -> Path:
    return _safe_auxiliary_directory(store, "netatmo_validation")


def _validation_path(store: RawArtifactStore, content_sha256: str) -> Path:
    if re.fullmatch(r"[0-9a-f]{64}", content_sha256) is None:
        raise NetatmoNativeContractError(
            "Netatmo validationcache ontving een ongeldige SHA-256"
        )
    identity = hashlib.sha256(
        f"{NETATMO_STRUCTURE_INSPECTOR_VERSION}:{content_sha256}".encode("ascii")
    ).hexdigest()
    return _validation_directory(store) / f"{identity}.json"


def _validation_payload(
    content_sha256: str,
    *,
    verdict: str,
    identity: NetatmoJsonIdentity | None,
) -> bytes:
    body: dict[str, object] = {
        "schema": NETATMO_VALIDATION_SCHEMA,
        "structure_inspector_version": NETATMO_STRUCTURE_INSPECTOR_VERSION,
        "content_sha256": content_sha256,
        "verdict": verdict,
        "safe_reason": (
            "accepted_structural_identity"
            if verdict == "accepted"
            else "deterministic_structure_contract_rejected"
        ),
        "identity": identity.to_document() if identity is not None else None,
    }
    body_sha256 = hashlib.sha256(_canonical_json_bytes(body)).hexdigest()
    return _canonical_json_bytes({**body, "body_sha256": body_sha256})


def _identity_from_document(value: object) -> NetatmoJsonIdentity:
    if not isinstance(value, dict) or frozenset(value) != frozenset({
        "station_structures",
        "response_timestamp_tokens",
    }):
        raise NetatmoNativeContractError(
            "Netatmo validationcache heeft een onbekende identiteit"
        )
    stations = value["station_structures"]
    timestamps = value["response_timestamp_tokens"]
    if (
        not isinstance(stations, list)
        or len(stations) > MAX_STATIONS
        or not isinstance(timestamps, list)
        or len(timestamps) > len(_TIMESTAMP_FIELDS)
    ):
        raise NetatmoNativeContractError(
            "Netatmo validationcache overschrijdt de identiteitsgrens"
        )
    seen_station_ids: set[str] = set()
    for expected_index, station in enumerate(stations):
        if not isinstance(station, dict) or frozenset(station) != frozenset({
            "station_index",
            "station_id",
            "station_upstream_timestamp_tokens",
            "modules",
        }):
            raise NetatmoNativeContractError(
                "Netatmo validationcache bevat een ongeldige stationstructuur"
            )
        station_id = station["station_id"]
        if (
            station["station_index"] != expected_index
            or not isinstance(station_id, str)
            or not station_id
            or len(station_id) > 256
            or station_id in seen_station_ids
        ):
            raise NetatmoNativeContractError(
                "Netatmo validationcache bevat een ongeldige stationidentiteit"
            )
        seen_station_ids.add(station_id)
        station_tokens = station["station_upstream_timestamp_tokens"]
        modules = station["modules"]
        if (
            not isinstance(station_tokens, list)
            or len(station_tokens) > len(_TIMESTAMP_FIELDS)
            or not isinstance(modules, list)
            or len(modules) > MAX_MODULES_PER_STATION
        ):
            raise NetatmoNativeContractError(
                "Netatmo validationcache overschrijdt de stationsgrens"
            )
        _validate_cached_timestamp_tokens(station_tokens)
        module_ids: set[str] = set()
        for module in modules:
            if not isinstance(module, dict) or frozenset(module) != frozenset({
                "module_id",
                "upstream_timestamp_tokens",
            }):
                raise NetatmoNativeContractError(
                    "Netatmo validationcache bevat een ongeldige modulestructuur"
                )
            module_id = module["module_id"]
            module_tokens = module["upstream_timestamp_tokens"]
            if (
                not isinstance(module_id, str)
                or not module_id
                or len(module_id) > 256
                or module_id in module_ids
                or not isinstance(module_tokens, list)
                or len(module_tokens)
                > MAX_TIMESTAMP_TOKENS_PER_MODULE + len(_TIMESTAMP_FIELDS)
            ):
                raise NetatmoNativeContractError(
                    "Netatmo validationcache overschrijdt de modulegrens"
                )
            module_ids.add(module_id)
            _validate_cached_timestamp_tokens(module_tokens)
    _validate_cached_timestamp_tokens(timestamps)
    # The verdict is derived data only. Re-serializing additionally proves it
    # remains a finite JSON tree; the raw response remains the sole authority.
    encoded = _canonical_json_bytes(value)
    if len(encoded) > MAX_VALIDATION_VERDICT_BYTES:
        raise NetatmoNativeContractError(
            "Netatmo validationcache overschrijdt de bytegrens"
        )
    return NetatmoJsonIdentity(
        station_structures=tuple(_json_copy(item) for item in stations),
        response_timestamp_tokens=tuple(_json_copy(item) for item in timestamps),
    )


def _validate_cached_timestamp_tokens(tokens: list[object]) -> None:
    for token in tokens:
        if not isinstance(token, dict) or frozenset(token) != frozenset({
            "source",
            "json_type",
            "value",
        }):
            raise NetatmoNativeContractError(
                "Netatmo validationcache bevat een ongeldige timestamptoken"
            )
        source = token["source"]
        value = token["value"]
        expected_type = (
            "string" if isinstance(value, str)
            else "integer" if isinstance(value, int) and not isinstance(value, bool)
            else "number" if isinstance(value, float) and math.isfinite(value)
            else None
        )
        if (
            not isinstance(source, str)
            or not source
            or len(source) > 128
            or token["json_type"] != expected_type
            or (isinstance(value, str) and (not value or len(value) > 128))
        ):
            raise NetatmoNativeContractError(
                "Netatmo validationcache bevat een ongeldige timestamptoken"
            )


def _load_validation_verdict(
    store: RawArtifactStore,
    *,
    content_sha256: str,
) -> NetatmoJsonIdentity | None:
    directory = _validation_directory(store)
    _validation_usage(directory)
    path = _validation_path(store, content_sha256)
    if not path.exists() and not path.is_symlink():
        return None
    info = path.lstat()
    if path.is_symlink() or not stat.S_ISREG(info.st_mode):
        raise NetatmoNativeContractError("Netatmo validationcache is onveilig")
    if info.st_size <= 0 or info.st_size > MAX_VALIDATION_VERDICT_BYTES:
        raise NetatmoNativeContractError(
            "Netatmo validationcache heeft een ongeldige grootte"
        )
    try:
        document = json.loads(
            _read_bounded_regular_file(
                path,
                maximum_bytes=MAX_VALIDATION_VERDICT_BYTES,
            ).decode("utf-8", errors="strict")
        )
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise NetatmoNativeContractError(
            "Netatmo validationcache is beschadigd"
        ) from None
    expected_keys = frozenset({
        "schema",
        "structure_inspector_version",
        "content_sha256",
        "verdict",
        "safe_reason",
        "identity",
        "body_sha256",
    })
    if not isinstance(document, dict) or frozenset(document) != expected_keys:
        raise NetatmoNativeContractError(
            "Netatmo validationcache heeft een onbekend schema"
        )
    checksum = document.pop("body_sha256")
    if (
        document["schema"] != NETATMO_VALIDATION_SCHEMA
        or document["structure_inspector_version"]
        != NETATMO_STRUCTURE_INSPECTOR_VERSION
        or document["content_sha256"] != content_sha256
        or not isinstance(checksum, str)
        or hashlib.sha256(_canonical_json_bytes(document)).hexdigest() != checksum
    ):
        raise NetatmoNativeContractError(
            "Netatmo validationcache faalt zijn identiteitsbewijs"
        )
    if document["verdict"] == "rejected" and document["identity"] is None:
        if document["safe_reason"] != (
            "deterministic_structure_contract_rejected"
        ):
            raise NetatmoNativeContractError(
                "Netatmo validationcache bevat een onbekende afkeurreden"
            )
        raise NetatmoNativeContractError(
            "Netatmo deterministische structuurafkeur is reeds compleet"
        )
    if (
        document["verdict"] != "accepted"
        or document["safe_reason"] != "accepted_structural_identity"
    ):
        raise NetatmoNativeContractError(
            "Netatmo validationcache bevat een onbekend verdict"
        )
    return _identity_from_document(document["identity"])


def _read_bounded_regular_file(path: Path, *, maximum_bytes: int) -> bytes:
    flags = (
        os.O_RDONLY
        | getattr(os, "O_BINARY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise NetatmoNativeContractError(
            "Netatmo validationcache kon niet veilig worden geopend"
        ) from error
    chunks: list[bytes] = []
    try:
        opened = os.fstat(descriptor)
        path_info = path.lstat()
        if (
            not stat.S_ISREG(opened.st_mode)
            or path.is_symlink()
            or not stat.S_ISREG(path_info.st_mode)
            or opened.st_size <= 0
            or opened.st_size > maximum_bytes
            or (
                os.name != "nt"
                and (
                    opened.st_dev != path_info.st_dev
                    or opened.st_ino != path_info.st_ino
                )
            )
        ):
            raise NetatmoNativeContractError(
                "Netatmo validationcache is geen begrensd regulier bestand"
            )
        total = 0
        while True:
            chunk = os.read(descriptor, min(64 * 1024, maximum_bytes + 1 - total))
            if not chunk:
                break
            total += len(chunk)
            if total > maximum_bytes:
                raise NetatmoNativeContractError(
                    "Netatmo validationcache overschrijdt de bytegrens"
                )
            chunks.append(chunk)
        if total != opened.st_size:
            raise NetatmoNativeContractError(
                "Netatmo validationcache veranderde tijdens lezen"
            )
    finally:
        os.close(descriptor)
    return b"".join(chunks)


def _validation_usage(directory: Path) -> tuple[list[Path], int]:
    entries = list(directory.iterdir())
    if len(entries) > MAX_VALIDATION_VERDICTS:
        raise NetatmoNativeContractError(
            "Netatmo validationcache overschrijdt de entrygrens"
        )
    total_bytes = 0
    for entry in entries:
        try:
            info = entry.lstat()
        except OSError as error:
            raise NetatmoNativeContractError(
                "Netatmo validationcache kon niet worden geaudit"
            ) from error
        if (
            entry.is_symlink()
            or not stat.S_ISREG(info.st_mode)
            or re.fullmatch(r"[0-9a-f]{64}\.json", entry.name) is None
            or info.st_size <= 0
            or info.st_size > MAX_VALIDATION_VERDICT_BYTES
        ):
            raise NetatmoNativeContractError(
                "Netatmo validationcache bevat een onbekend bestand"
            )
        total_bytes += info.st_size
    if total_bytes > MAX_VALIDATION_TOTAL_BYTES:
        raise NetatmoNativeContractError(
            "Netatmo validationcache overschrijdt de totale bytegrens"
        )
    return entries, total_bytes


def _publish_validation_verdict(
    store: RawArtifactStore,
    *,
    content_sha256: str,
    verdict: str,
    identity: NetatmoJsonIdentity | None,
    deadline: float,
) -> None:
    directory = _validation_directory(store)
    target = _validation_path(store, content_sha256)
    payload = _validation_payload(
        content_sha256,
        verdict=verdict,
        identity=identity,
    )
    if len(payload) > MAX_VALIDATION_VERDICT_BYTES:
        raise NetatmoNativeContractError(
            "Netatmo validationverdict overschrijdt de bytegrens"
        )
    with _deadline_file_lock(
        _lock_path(store, "validation-capacity", "global"),
        deadline=deadline,
    ):
        if target.exists():
            _load_validation_verdict(store, content_sha256=content_sha256)
            return
        entries, total_bytes = _validation_usage(directory)
        if len(entries) >= MAX_VALIDATION_VERDICTS:
            raise NetatmoNativeContractError(
                "Netatmo validationcache heeft zijn entrygrens bereikt"
            )
        if total_bytes + len(payload) > MAX_VALIDATION_TOTAL_BYTES:
            raise NetatmoNativeContractError(
                "Netatmo validationcache overschrijdt de totale bytegrens"
            )
        temporary = directory / f".{target.stem}.{os.getpid()}.{threading.get_ident()}"
        descriptor: int | None = None
        try:
            descriptor = os.open(
                temporary,
                os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_BINARY", 0),
                0o600,
            )
            offset = 0
            while offset < len(payload):
                written = os.write(descriptor, payload[offset:])
                if written <= 0:
                    raise NetatmoNativeContractError(
                        "Netatmo validationverdict werd niet volledig geschreven"
                    )
                offset += written
            os.fsync(descriptor)
            os.close(descriptor)
            descriptor = None
            try:
                os.link(temporary, target)
            except FileExistsError:
                pass
            _fsync_directory(directory)
        finally:
            if descriptor is not None:
                os.close(descriptor)
            temporary.unlink(missing_ok=True)
            _fsync_directory(directory)
        _load_validation_verdict(store, content_sha256=content_sha256)


def _inspect_response_cached(
    store: RawArtifactStore,
    payload: bytes,
    *,
    content_sha256: str,
    deadline: float,
) -> NetatmoJsonIdentity:
    cached = _load_validation_verdict(store, content_sha256=content_sha256)
    if cached is not None:
        return cached
    try:
        identity = _inspect_response(payload, deadline=deadline)
    except (NetatmoNativeDeadlineError, NetatmoNativeProviderResponseError):
        # Deadlines and provider status are not deterministic structural
        # verdicts and must not poison the durable validation cache.
        raise
    except NetatmoNativeContractError:
        _publish_validation_verdict(
            store,
            content_sha256=content_sha256,
            verdict="rejected",
            identity=None,
            deadline=deadline,
        )
        raise
    _publish_validation_verdict(
        store,
        content_sha256=content_sha256,
        verdict="accepted",
        identity=identity,
        deadline=deadline,
    )
    return identity


def _native_provenance(
    context: dict[str, object],
    *,
    transport_attempt: int,
) -> dict[str, object]:
    return {
        "representation": NATIVE_REPRESENTATION,
        "capture_stage": NATIVE_CAPTURE_STAGE,
        "collector": NETATMO_RAW_COLLECTOR,
        "collector_version": NETATMO_RAW_COLLECTOR_VERSION,
        "structure_inspector_version": NETATMO_STRUCTURE_INSPECTOR_VERSION,
        "query_context": context,
        "query_context_sha256": _query_context_sha256(context),
        "transport_attempt": transport_attempt,
        "response_structure_claim": "none_before_post_store_inspection",
        "meteorological_interpretation": "none",
    }


def _receipt_id(store: RawArtifactStore, job_id: str) -> str:
    return store._receipt_id(  # noqa: SLF001 - worker store identity contract
        provider="netatmo",
        product=NETATMO_JSON_PRODUCT,
        fetch_id=f"{job_id}:response_json",
    )


def _capture_id(store: RawArtifactStore, job_id: str) -> str:
    return store._capture_id(  # noqa: SLF001 - worker store identity contract
        provider="netatmo",
        product=NETATMO_CAPTURE_PRODUCT,
        fetch_id=f"{job_id}:capture",
    )


def _expected_upstream_id(
    context: dict[str, object],
    content_sha256: str,
) -> str:
    return (
        "netatmo/getpublicdata"
        f"|query_sha256={_query_context_sha256(context)}"
        f"|content_sha256={content_sha256}"
    )


def _validate_receipt(
    receipt: RawArtifactReceipt,
    *,
    job_id: str,
    context: dict[str, object],
) -> None:
    attempt = receipt.provenance.get("transport_attempt")
    if (
        isinstance(attempt, bool)
        or not isinstance(attempt, int)
        or not 1 <= attempt <= MAX_TRANSPORT_ATTEMPTS
        or receipt.provider != "netatmo"
        or receipt.product != NETATMO_JSON_PRODUCT
        or receipt.fetch_id != f"{job_id}:response_json"
        or receipt.content_type not in _ALLOWED_CONTENT_TYPES
        or receipt.size_bytes <= 0
        or receipt.size_bytes > MAX_RESPONSE_BYTES
        or receipt.upstream_id != _expected_upstream_id(
            context, receipt.content_sha256
        )
        or receipt.provenance != _native_provenance(
            context, transport_attempt=attempt
        )
    ):
        raise RuntimeError(
            "duurzaam Netatmo-receipt heeft een verkeerde identiteit"
        )


def _commit_capture(
    store: RawArtifactStore,
    *,
    job_id: str,
    receipt: RawArtifactReceipt,
    context: dict[str, object],
) -> RawArtifactCaptureReceipt:
    return store.commit_capture(
        provider="netatmo",
        product=NETATMO_CAPTURE_PRODUCT,
        fetch_id=f"{job_id}:capture",
        upstream_capture_id=(
            f"{NETATMO_CAPTURE_PRODUCT}"
            f"|query_sha256={_query_context_sha256(context)}"
            f"|content_sha256={receipt.content_sha256}"
        ),
        # Independent station/module timestamps cannot be collapsed into one
        # product-valid timestamp without meteorological interpretation.
        product_timestamp=None,
        members={"response_json": receipt.receipt_id},
    )


def _validate_capture(
    capture: RawArtifactCaptureReceipt,
    *,
    job_id: str,
    receipt: RawArtifactReceipt,
    context: dict[str, object],
) -> None:
    members = {member.role: member.receipt_id for member in capture.members}
    expected = (
        f"{NETATMO_CAPTURE_PRODUCT}"
        f"|query_sha256={_query_context_sha256(context)}"
        f"|content_sha256={receipt.content_sha256}"
    )
    if (
        capture.provider != "netatmo"
        or capture.product != NETATMO_CAPTURE_PRODUCT
        or capture.fetch_id != f"{job_id}:capture"
        or capture.upstream_capture_id != expected
        or capture.product_timestamp is not None
        or members != {"response_json": receipt.receipt_id}
    ):
        raise RuntimeError(
            "duurzame Netatmo-capture heeft een verkeerde identiteit"
        )


def _capture_result(
    *,
    capture: RawArtifactCaptureReceipt,
    receipt: RawArtifactReceipt,
    context: dict[str, object],
    identity: NetatmoJsonIdentity,
) -> dict[str, object]:
    raw_source = {
        "role": "response_json",
        "raw_receipt_id": receipt.receipt_id,
        "raw_content_sha256": receipt.content_sha256,
        "raw_receipt_sha256": receipt.receipt_sha256,
        "fetch_id": receipt.fetch_id,
        "upstream_record_id": receipt.upstream_id,
        "received_at": receipt.received_at,
        "content_type": receipt.content_type,
        "collector_version": NETATMO_RAW_COLLECTOR_VERSION,
        "structure_inspector_version": NETATMO_STRUCTURE_INSPECTOR_VERSION,
        "query_context_sha256": _query_context_sha256(context),
    }
    return {
        "provider": "netatmo",
        "product": NETATMO_JSON_PRODUCT,
        "query_scope": "per_tracker_bounding_box",
        "query_identity": context["query_identity"],
        "query_kind": context["query_kind"],
        "coverage_status": "unknown_not_proven_by_station_count",
        "station_count": identity.station_count,
        "station_structures": [
            _json_copy(item) for item in identity.station_structures
        ],
        "response_timestamp_tokens": [
            _json_copy(item) for item in identity.response_timestamp_tokens
        ],
        "timestamp_semantics": "exact_tokens_preserved_not_interpreted",
        "measurement_semantics": "not_decoded",
        "capture_state": "captured_structural_only",
        "weather_state": "unknown",
        "precipitation_state": "unknown",
        "wet_claim": False,
        "dry_claim": False,
        "ground_confirmation_eligible": False,
        "operational_admission_eligible": False,
        "admission_status": NETATMO_ADMISSION_STATUS,
        "raw_provenance": {
            "raw_store_schema_version": capture.raw_store_schema_version,
            "raw_capture_id": capture.capture_id,
            "raw_capture_manifest_sha256": capture.manifest_sha256,
            "raw_capture_receipt_sha256": capture.capture_receipt_sha256,
            "raw_capture_fetch_id": capture.fetch_id,
            "upstream_capture_id": capture.upstream_capture_id,
            "product_timestamp": None,
            "collector_version": NETATMO_RAW_COLLECTOR_VERSION,
            "provenance_grade": "exact_raw",
            "provenance_scope": (
                "exact_getpublicdata_json_with_credential_free_query_context"
            ),
            "query_context": context,
            "query_context_sha256": _query_context_sha256(context),
            "raw_sources": {"response_json": raw_source},
        },
    }


def _resume_existing(
    store: RawArtifactStore,
    *,
    job_id: str,
    context: dict[str, object],
    credential: str,
    deadline: float,
) -> dict[str, object] | None:
    _remaining_seconds(deadline, "resume-start")
    bundle = store.load_capture_bundle_bytes(_capture_id(store, job_id))
    if bundle is not None:
        capture, receipts, payloads = bundle
        if set(receipts) != {"response_json"} or set(payloads) != {
            "response_json"
        }:
            raise RuntimeError(
                "Netatmo-capture heeft geen exact responsemember"
            )
        receipt = receipts["response_json"]
        payload = payloads["response_json"]
        _validate_receipt(receipt, job_id=job_id, context=context)
        _reject_secret_echo(payload, credential, deadline=deadline)
        _validate_capture(
            capture,
            job_id=job_id,
            receipt=receipt,
            context=context,
        )
        identity = _inspect_response_cached(
            store,
            payload,
            content_sha256=receipt.content_sha256,
            deadline=deadline,
        )
        return _capture_result(
            capture=capture,
            receipt=receipt,
            context=context,
            identity=identity,
        )
    try:
        receipt, payload = store.read_receipt_bytes(_receipt_id(store, job_id))
    except RawArtifactNotFoundError:
        return None
    _validate_receipt(receipt, job_id=job_id, context=context)
    _reject_secret_echo(payload, credential, deadline=deadline)
    identity = _inspect_response_cached(
        store,
        payload,
        content_sha256=receipt.content_sha256,
        deadline=deadline,
    )
    capture = _commit_capture(
        store,
        job_id=job_id,
        receipt=receipt,
        context=context,
    )
    return _capture_result(
        capture=capture,
        receipt=receipt,
        context=context,
        identity=identity,
    )


def capture_netatmo_public_data(
    job: object,
    *,
    raw_store: RawArtifactStore | None = None,
    access_token: str | None = None,
) -> dict[str, object]:
    """Capture one exact tracker-scoped response without weather claims."""

    deadline = _new_deadline()
    credential = _access_token(job, access_token)
    job_id = _validate_job_id(getattr(job, "job_id", None), credential)
    query = _validate_query_payload(getattr(job, "payload", None), credential)
    context = _query_context(query)
    store = _raw_store(raw_store)
    acquired = _NETATMO_CAPTURE_LOCK.acquire(
        timeout=_remaining_seconds(deadline, "capturelock")
    )
    if not acquired:
        raise NetatmoNativeDeadlineError(
            "Netatmo job overschrijdt de absolute deadline (capturelock)"
        )
    try:
        with _deadline_file_lock(
            _lock_path(store, "capture-capacity", "global"),
            deadline=deadline,
        ), _deadline_file_lock(
            _lock_path(store, "slot", job_id),
            deadline=deadline,
        ):
            resumed = _resume_existing(
                store,
                job_id=job_id,
                context=context,
                credential=credential,
                deadline=deadline,
            )
            if resumed is not None:
                return resumed
            response = _download_with_retries(
                query,
                credential=credential,
                deadline=deadline,
            )
            # Repeat the security scan at the orchestration boundary.  This
            # makes alternate/test transports unable to bypass the no-secret
            # persistence rule by forging an attestation marker.
            _reject_secret_echo(
                response.payload,
                credential,
                deadline=deadline,
            )
            _remaining_seconds(deadline, "voor-raw-opslag")
            content_sha256 = hashlib.sha256(response.payload).hexdigest()
            receipt = store.store(
                provider="netatmo",
                product=NETATMO_JSON_PRODUCT,
                fetch_id=f"{job_id}:response_json",
                content_type=response.content_type,
                received_at=response.received_at,
                upstream_id=_expected_upstream_id(context, content_sha256),
                provenance=_native_provenance(
                    context,
                    transport_attempt=response.transport_attempt,
                ),
                payload=response.payload,
            )
            # This is the first JSON operation. Exact upstream bytes and the
            # credential-free request context are already durable.
            identity = _inspect_response_cached(
                store,
                response.payload,
                content_sha256=receipt.content_sha256,
                deadline=deadline,
            )
            capture = _commit_capture(
                store,
                job_id=job_id,
                receipt=receipt,
                context=context,
            )
            return _capture_result(
                capture=capture,
                receipt=receipt,
                context=context,
                identity=identity,
            )
    finally:
        _NETATMO_CAPTURE_LOCK.release()


__all__ = [
    "NETATMO_ADMISSION_STATUS",
    "NETATMO_CAPTURE_PRODUCT",
    "NETATMO_JSON_PRODUCT",
    "NETATMO_QUERY_CONTEXT_SCHEMA",
    "NetatmoHttpResponse",
    "NetatmoJsonIdentity",
    "NetatmoNativeContractError",
    "NetatmoNativeDeadlineError",
    "NetatmoNativeProviderResponseError",
    "NetatmoNativeTransportError",
    "NetatmoQuery",
    "capture_netatmo_public_data",
]
