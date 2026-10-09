"""Exact, isolated raw capture for one Synoptic tracker query.

This candidate persists the exact upstream JSON response before the first
JSON or meteorological inspection.  The credential is read exclusively from
the worker environment and is deliberately absent from the durable query
context, receipt provenance, capture result and raised error messages.

The module is intentionally not connected to the worker service.  It emits no
station observations and cannot make wet, dry or ground-confirmation claims.
The request is precipitation-only and retains provider-derived intervals plus
their provider QC metadata without interpreting either as an instantaneous
measurement or as in-situ truth.
"""
from __future__ import annotations

from collections import OrderedDict
import copy
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
import threading
import time
from typing import Any, Iterable
import unicodedata
from urllib.error import HTTPError
from urllib.parse import urlencode
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


SYNOPTIC_ORIGIN = "https://api.synopticdata.com"
SYNOPTIC_TIMESERIES_PATH = "/v2/stations/timeseries"
SYNOPTIC_TIMESERIES_URL = f"{SYNOPTIC_ORIGIN}{SYNOPTIC_TIMESERIES_PATH}"
SYNOPTIC_CREDENTIAL_ENV = "STV3_SYNOPTIC_TOKEN"

SYNOPTIC_RAW_SHARE_ROOT = Path("/share/storm_tracker_worker")
SYNOPTIC_RAW_ARTIFACT_ROOT = (
    SYNOPTIC_RAW_SHARE_ROOT / "raw" / "synoptic_timeseries"
)
SYNOPTIC_JSON_PRODUCT = "synoptic_timeseries_json_response"
SYNOPTIC_CAPTURE_PRODUCT = "synoptic_timeseries_query_capture"
SYNOPTIC_RAW_COLLECTOR = "synoptic_timeseries_exact_response"
SYNOPTIC_RAW_COLLECTOR_VERSION = "worker-synoptic-timeseries-exact-raw-v5"
SYNOPTIC_STRUCTURE_INSPECTOR_VERSION = "synoptic-timeseries-json-structure-v4"
SYNOPTIC_QUERY_CONTEXT_SCHEMA = "stv3-synoptic-timeseries-query-context-v5"
SYNOPTIC_SEMANTIC_CLASS = (
    "provider_derived_precipitation_timeseries_with_qc_metadata"
)
SYNOPTIC_ADMISSION_STATUS = "exact_raw_parser_not_certified"
SYNOPTIC_COVERAGE_STATUS = "coverage_incomplete"

MAX_RESPONSE_BYTES = 16 * 1024 * 1024
MAX_JSON_NODES = 250_000
MAX_JSON_DEPTH = 64
MAX_JSON_OBJECT_MEMBERS = 10_000
MAX_JSON_ARRAY_ITEMS = 100_000
MAX_JSON_STRING_CHARS = 1024 * 1024
MAX_JSON_KEY_CHARS = 512
MAX_STATIONS_PER_QUERY = 50
MAX_DURABLE_ID_CHARS = 96
MAX_CREDENTIAL_NORMALIZATION_ROUNDS = 16
MAX_CREDENTIAL_ENCODED_CANDIDATES = 4096
MAX_REQUEST_ATTEMPTS_PER_JOB = 8
HTTP_READ_CHUNK_BYTES = 64 * 1024
MAX_HTTP_READ_CHUNKS = (MAX_RESPONSE_BYTES // HTTP_READ_CHUNK_BYTES) + 2
MAX_RUNTIME_CACHE_ENTRIES = 128
MIN_RADIUS_KM = 1.0
MAX_RADIUS_KM = 50.0
RECENT_MINUTES = 60
LEGACY_MIN_QUERY_INTERVAL_SECONDS = 5 * 60
HTTP_IO_TIMEOUT_SECONDS = 10.0
HTTP_TOTAL_DEADLINE_SECONDS = 30.0

# Station selection must be driven only by precipitation capability.  Adding
# unrelated variables creates OR-selection and can consume the exact station
# limit with stations that have no precipitation evidence.
_VARIABLES = "precip"
_UNITS = "metric,temp|C,speed|kph,pres|mb,precip|mm"
_PAYLOAD_KEYS = frozenset({
    "latitude",
    "longitude",
    "radius_km",
    "query_key",
    "query_kind",
})
_QUERY_KINDS = frozenset({"local", "roaming"})
_CANONICAL_JOB_ID_RE = re.compile(
    r"^[a-z0-9]+(?:[-_.:@/][a-z0-9]+)*$"
)
_CANONICAL_QUERY_KEY_RE = re.compile(
    r"^[a-z0-9]+(?:[-_.:@/][a-z0-9]+)*$"
)
# Synoptic's official public-token contract is exactly 32 lowercase
# hexadecimal characters. Accepting an application key here would be both a
# security error and a guaranteed API failure.
_CREDENTIAL_RE = re.compile(r"^[0-9a-f]{32}$")
_CREDENTIAL_LIKE_SUBSTRING_RE = re.compile(r"[0-9A-Fa-f]{32}")
_IDENTITY_SEPARATOR_RE = re.compile(r"[-_.:@/]")
_PERCENT_ESCAPE_RE = re.compile(br"%([0-9A-Fa-f]{2})")
_ASCII_ESCAPE_RE = re.compile(
    br"\\(?:u([0-9A-Fa-f]{4})|U([0-9A-Fa-f]{8})|x([0-9A-Fa-f]{2}))"
)
_BASE64_TOKEN_RE = re.compile(
    br"(?<![A-Za-z0-9+/_-])([A-Za-z0-9+/_-]{43}=?)"
    br"(?![A-Za-z0-9+/_=-])"
)
_BASE32_TOKEN_RE = re.compile(
    br"(?<![A-Za-z2-7])([A-Za-z2-7]{52}(?:={4})?)"
    br"(?![A-Za-z2-7=])",
    re.IGNORECASE,
)
_CODE_TWO_AMBIGUITY_RE = re.compile(
    r"^no stations? found for this request"
    r"(?:,? or (?:(?:your account does)|(?:you do)) not have access to "
    r"(?:the )?requested (?:station\(s\)|stations?|data)"
    r"(?:\. please contact support@synopticdata\.com)?)?[.!]?$",
    re.IGNORECASE,
)
_PRECIP_INTERVAL_FIELD_RE = re.compile(r"^precip_intervals_set_[A-Za-z0-9_]+$")
_PRECIP_ACCUMULATED_FIELD_RE = re.compile(
    r"^precip_accumulated_set_[A-Za-z0-9_]+$"
)
_ALLOWED_CONTENT_TYPES = frozenset({"application/json"})

_SYNOPTIC_RAW_STORE: RawArtifactStore | None = None
_SYNOPTIC_RAW_STORE_LOCK = threading.Lock()
_SYNOPTIC_CAPTURE_LOCK = threading.Lock()
_SYNOPTIC_RUNTIME_CACHE_LOCK = threading.Lock()
_CREDENTIAL_SCAN_ATTESTATION = object()


class SynopticNativeTransportError(RuntimeError):
    """A deliberately detail-free network error that cannot expose a secret."""


class SynopticNativeContractError(ValueError):
    """A local fail-closed response or payload contract violation."""


class SynopticNativeDeadlineError(SynopticNativeContractError):
    """A retryable expiry of the one absolute monotone job deadline."""


class SynopticNativeProviderResponseError(SynopticNativeContractError):
    """A safely received provider error that cannot complete a capture."""

    response_class = "provider_error"

    def __init__(
        self,
        message: str = "Synoptic providerresponse is provider_error",
        *,
        http_status: int | None = None,
        retryable: bool = True,
    ) -> None:
        super().__init__(message)
        self.http_status = http_status
        self.retryable = bool(retryable)


@dataclass(frozen=True, slots=True)
class SynopticQuery:
    latitude: float
    longitude: float
    radius_km: float
    query_key: str
    query_kind: str


@dataclass(frozen=True, slots=True)
class SynopticHttpResponse:
    payload: bytes
    content_type: str
    received_at: float
    credential_scan_attestation: object | None = None


@dataclass(frozen=True, slots=True)
class SynopticJsonIdentity:
    """Structural API identity only; deliberately contains no weather facts."""

    response_class: str
    structural_status: str
    precipitation_structure_verified: bool
    sensor_metadata_structure_verified: bool
    qc_structure_verified: bool


@dataclass(frozen=True, slots=True)
class _CachedRejection:
    rejection_kind: str
    http_status: int | None
    retryable: bool

    def raise_safe(self) -> None:
        if self.rejection_kind == "provider_error":
            raise SynopticNativeProviderResponseError(
                "Synoptic providerresponse is reeds veilig geclassificeerd",
                http_status=self.http_status,
                retryable=self.retryable,
            )
        raise SynopticNativeContractError(
            "Synoptic deterministische structuurafkeur is reeds compleet"
        )


_INSPECTION_CACHE: OrderedDict[
    tuple[str, object, str],
    SynopticJsonIdentity | _CachedRejection,
] = OrderedDict()
_COMPLETE_RESULT_CACHE: OrderedDict[
    tuple[str, str, str],
    dict[str, object],
] = OrderedDict()
_JOB_REJECTION_CACHE: OrderedDict[
    tuple[str, str, str],
    _CachedRejection,
] = OrderedDict()
_JOB_NEXT_ATTEMPT_CACHE: OrderedDict[
    tuple[str, str, str],
    int,
] = OrderedDict()


class _RejectRedirects(HTTPRedirectHandler):
    """Never forward the authorization header to a redirect target."""

    def redirect_request(self, *_args, **_kwargs):
        return None


def _open_synoptic_request(request: Request, *, timeout: float):
    """Open without environment proxies and without redirect handling."""

    opener = build_opener(ProxyHandler({}), _RejectRedirects())
    return opener.open(request, timeout=timeout)


def _finite_number(value: object, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"Synoptic {name} moet een eindig getal zijn")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"Synoptic {name} moet een eindig getal zijn")
    return result


def _new_job_deadline() -> float:
    now = float(time.monotonic())
    if not math.isfinite(now):
        raise SynopticNativeContractError(
            "Synoptic monotone klok is ongeldig"
        )
    return now + HTTP_TOTAL_DEADLINE_SECONDS


def _remaining_seconds(deadline: float, phase: str) -> float:
    now = float(time.monotonic())
    remaining = deadline - now
    if not math.isfinite(remaining) or remaining <= 0.0:
        raise SynopticNativeDeadlineError(
            f"Synoptic job overschrijdt de absolute deadline ({phase})"
        )
    return remaining


def _check_deadline(deadline: float | None, phase: str) -> None:
    if deadline is not None:
        _remaining_seconds(deadline, phase)


def _contains_reconstructed_credential_identity(value: str) -> bool:
    """Find every 32-hex identity, also when split by allowed separators.

    UUID-shaped text is intentionally not exempt: it still carries exactly
    32 reconstructable hexadecimal characters and could therefore be a token
    disguised with UUID hyphens.  Durable tracker IDs must use a non-secret
    prefix or another non-hex identifier instead.
    """

    compact = _IDENTITY_SEPARATOR_RE.sub("", value)
    return _CREDENTIAL_LIKE_SUBSTRING_RE.search(compact) is not None


def _validate_job_id(value: object) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value) > MAX_DURABLE_ID_CHARS
    ):
        raise ValueError("Synoptic capturejob mist een geldige job_id")
    if _contains_reconstructed_credential_identity(value):
        raise ValueError(
            "Synoptic job_id bevat een credentialachtige identiteit"
        )
    if _CANONICAL_JOB_ID_RE.fullmatch(value) is None:
        raise ValueError("Synoptic job_id is geen canonieke korte identiteit")
    return value


def _validate_query_payload(payload: object) -> SynopticQuery:
    if type(payload) is not dict or frozenset(payload) != _PAYLOAD_KEYS:
        raise ValueError(
            "Synoptic native capture vereist exact de vijf queryvelden"
        )
    latitude = _finite_number(payload["latitude"], "latitude")
    longitude = _finite_number(payload["longitude"], "longitude")
    radius_km = _finite_number(payload["radius_km"], "radius_km")
    if not -90.0 <= latitude <= 90.0:
        raise ValueError("Synoptic latitude valt buiten het geldige bereik")
    if not -180.0 <= longitude <= 180.0:
        raise ValueError("Synoptic longitude valt buiten het geldige bereik")
    if not MIN_RADIUS_KM <= radius_km <= MAX_RADIUS_KM:
        raise ValueError("Synoptic radius_km valt buiten het capturecontract")
    query_key = payload["query_key"]
    if (
        not isinstance(query_key, str)
        or not query_key
        or len(query_key) > MAX_DURABLE_ID_CHARS
    ):
        raise ValueError("Synoptic query_key heeft geen korte identiteit")
    if _contains_reconstructed_credential_identity(query_key):
        raise ValueError(
            "Synoptic query_key bevat een credentialachtige identiteit"
        )
    if _CANONICAL_QUERY_KEY_RE.fullmatch(query_key) is None:
        raise ValueError("Synoptic query_key is geen canonieke identiteit")
    query_kind = payload["query_kind"]
    if not isinstance(query_kind, str) or query_kind not in _QUERY_KINDS:
        raise ValueError("Synoptic query_kind valt buiten het capturecontract")
    # Avoid signed zero creating two durable identities for one wire query.
    latitude = 0.0 if latitude == 0.0 else latitude
    longitude = 0.0 if longitude == 0.0 else longitude
    return SynopticQuery(
        latitude=latitude,
        longitude=longitude,
        radius_km=radius_km,
        query_key=query_key,
        query_kind=query_kind,
    )


def _query_parameters(query: SynopticQuery) -> tuple[tuple[str, str], ...]:
    # Synoptic accepts only two decimal miles.  Round outward so the wire
    # radius can never become smaller than the authority-approved kilometre
    # radius (e.g. 1.000 km used to serialize as only 0.99779328 km).
    radius_miles = math.ceil((query.radius_km / 1.609344) * 100.0) / 100.0
    return (
        (
            "radius",
            f"{query.latitude:.5f},{query.longitude:.5f},{radius_miles:.2f}",
        ),
        ("vars", _VARIABLES),
        ("precip", "1"),
        ("all_reports", "1"),
        ("sensorvars", "1"),
        ("qc", "on"),
        ("qc_remove_data", "off"),
        ("qc_flags", "on"),
        ("qc_checks", "synopticlabs,madis,mesowest"),
        ("units", _UNITS),
        ("output", "json"),
        ("recent", str(RECENT_MINUTES)),
        ("limit", str(MAX_STATIONS_PER_QUERY)),
    )


def _query_context(query: SynopticQuery) -> dict[str, object]:
    """Return the complete durable request semantics except the credential."""

    return {
        "schema": SYNOPTIC_QUERY_CONTEXT_SCHEMA,
        "method": "GET",
        "origin": SYNOPTIC_ORIGIN,
        "path": SYNOPTIC_TIMESERIES_PATH,
        "query_identity": query.query_key,
        "query_kind": query.query_kind,
        "station_selection": {
            "mode": "precipitation_variables_only",
            "provider_derived_precipitation_intervals_requested": True,
            "exact_station_limit": MAX_STATIONS_PER_QUERY,
            "coverage_status": SYNOPTIC_COVERAGE_STATUS,
        },
        "information_preservation": {
            "all_reports": True,
            "sensor_metadata": True,
            "provider_qc_values_removed": False,
            "provider_qc_flags_requested": True,
            "provider_qc_sources": [
                "synopticlabs",
                "madis",
                "mesowest",
            ],
        },
        "parameters": [
            {"name": name, "value": value}
            for name, value in _query_parameters(query)
        ],
    }


def _canonical_json_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _query_context_sha256(context: dict[str, object]) -> str:
    return hashlib.sha256(_canonical_json_bytes(context)).hexdigest()


def _environment_credential() -> str:
    value = os.getenv(SYNOPTIC_CREDENTIAL_ENV)
    if (
        not isinstance(value, str)
        or value != value.strip()
        or _CREDENTIAL_RE.fullmatch(value) is None
    ):
        raise ValueError("Synoptic workercredential ontbreekt of is ongeldig")
    return value


def _request_url(query: SynopticQuery) -> str:
    """Return the exact credential-free request URL."""

    encoded = urlencode(_query_parameters(query))
    return f"{SYNOPTIC_TIMESERIES_URL}?{encoded}"


def _decode_percent_escapes(value: bytes) -> bytes:
    return _PERCENT_ESCAPE_RE.sub(
        lambda match: bytes((int(match.group(1), 16),)),
        value,
    )


def _decode_ascii_unicode_escapes(value: bytes) -> bytes:
    def replace(match: re.Match[bytes]) -> bytes:
        encoded = next(group for group in match.groups() if group is not None)
        codepoint = int(encoded, 16)
        if codepoint > 0x10FFFF or 0xD800 <= codepoint <= 0xDFFF:
            return match.group(0)
        return chr(codepoint).encode("utf-8")

    return _ASCII_ESCAPE_RE.sub(replace, value)


def _decode_html_and_fullwidth(value: bytes) -> bytes:
    try:
        text = value.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        # The later JSON stage rejects this response.  Credential scanning
        # must nevertheless remain deterministic and must never drop bytes in
        # a way that could join a secret across an invalid sequence.
        return value
    normalized = unicodedata.normalize("NFKC", html.unescape(text))
    encoded = normalized.encode("utf-8")
    if len(encoded) > MAX_RESPONSE_BYTES:
        raise SynopticNativeContractError(
            "Synoptic credentialnormalisatie overschrijdt de bytegrens"
        )
    return encoded


def _credential_normalization_round(value: bytes) -> bytes:
    percent_decoded = (
        _decode_percent_escapes(value) if b"%" in value else value
    )
    escape_decoded = (
        _decode_ascii_unicode_escapes(percent_decoded)
        if b"\\" in percent_decoded
        else percent_decoded
    )
    return _decode_html_and_fullwidth(escape_decoded)


def _credential_literal_pattern(credential: str) -> re.Pattern[bytes]:
    separator = br"[-_.:@/]?"
    parts = [re.escape(character.encode("ascii")) for character in credential]
    return re.compile(separator.join(parts), re.IGNORECASE)


def _decoded_token_is_credential(decoded: bytes, credential: str) -> bool:
    if len(decoded) != len(credential):
        return False
    return decoded.lower() == credential.encode("ascii")


def _contains_encoded_credential(
    candidate: bytes,
    credential: str,
    *,
    deadline: float | None,
    remaining_candidates: list[int],
) -> bool:
    for match in _BASE64_TOKEN_RE.finditer(candidate):
        remaining_candidates[0] -= 1
        if remaining_candidates[0] < 0:
            raise SynopticNativeContractError(
                "Synoptic credentialscan overschrijdt de kandidaatgrens"
            )
        _check_deadline(deadline, "credentialscan-base64")
        token = match.group(1)
        padded = token + (b"=" * ((-len(token)) % 4))
        for altchars in (None, b"-_"):
            try:
                decoded = base64.b64decode(
                    padded,
                    altchars=altchars,
                    validate=True,
                )
            except (binascii.Error, ValueError):
                continue
            if _decoded_token_is_credential(decoded, credential):
                return True

    for match in _BASE32_TOKEN_RE.finditer(candidate):
        remaining_candidates[0] -= 1
        if remaining_candidates[0] < 0:
            raise SynopticNativeContractError(
                "Synoptic credentialscan overschrijdt de kandidaatgrens"
            )
        _check_deadline(deadline, "credentialscan-base32")
        token = match.group(1)
        padded = token + (b"=" * ((-len(token)) % 8))
        try:
            decoded = base64.b32decode(padded, casefold=True)
        except (binascii.Error, ValueError):
            continue
        if _decoded_token_is_credential(decoded, credential):
            return True
    return False


def _contains_credential_echo(
    payload: bytes,
    credential: str,
    *,
    deadline: float | None = None,
) -> bool:
    """Detect literal or encoded credential echoes without parsing JSON.

    Normalisation is deliberately bounded to two reversible encodings used by
    HTTP and JSON.  Iteration catches mixed and double-encoded forms while the
    hard response-size ceiling bounds memory and CPU use.
    """

    if not isinstance(payload, bytes) or len(payload) > MAX_RESPONSE_BYTES:
        raise SynopticNativeContractError(
            "Synoptic credentialscan overschrijdt de bytegrens"
        )
    secret_pattern = _credential_literal_pattern(credential)
    remaining_candidates = [MAX_CREDENTIAL_ENCODED_CANDIDATES]
    candidate = bytes(payload)
    for _ in range(MAX_CREDENTIAL_NORMALIZATION_ROUNDS):
        _check_deadline(deadline, "credentialscan-normalisatie")
        if secret_pattern.search(candidate) is not None:
            return True
        if _contains_encoded_credential(
            candidate,
            credential,
            deadline=deadline,
            remaining_candidates=remaining_candidates,
        ):
            return True
        decoded = _credential_normalization_round(candidate)
        if decoded == candidate:
            return False
        candidate = decoded
    if secret_pattern.search(candidate) is not None:
        return True
    if _contains_encoded_credential(
        candidate,
        credential,
        deadline=deadline,
        remaining_candidates=remaining_candidates,
    ):
        return True
    if _credential_normalization_round(candidate) != candidate:
        raise SynopticNativeContractError(
            "Synoptic-response overschrijdt de credentialnormalisatiegrens"
        )
    return False


def _reject_credential_echo(
    payload: bytes,
    credential: str,
    *,
    deadline: float | None = None,
) -> None:
    if _contains_credential_echo(payload, credential, deadline=deadline):
        raise SynopticNativeContractError(
            "Synoptic-response bevat een vorm van de workercredential"
        )


def _single_header(headers, name: str) -> str | None:
    if headers is None:
        return None
    get_all = getattr(headers, "get_all", None)
    if callable(get_all):
        values = get_all(name)
        if values is not None:
            if len(values) != 1:
                raise SynopticNativeContractError(
                    f"Synoptic-response bevat een ambigu {name}"
                )
            return str(values[0])
    value = headers.get(name)
    return None if value is None else str(value)


def _declared_content_length(headers) -> int | None:
    value = _single_header(headers, "Content-Length")
    if value is None:
        return None
    normalized = value.strip()
    if re.fullmatch(r"\d+", normalized) is None:
        raise SynopticNativeContractError(
            "Synoptic-response heeft een ongeldige Content-Length"
        )
    declared = int(normalized)
    if declared > MAX_RESPONSE_BYTES:
        raise SynopticNativeContractError(
            "Synoptic-response overschrijdt de bytegrens"
        )
    return declared


def _read_response_body(
    response,
    *,
    declared_bytes: int | None,
    deadline: float,
) -> bytes:
    body = bytearray()
    total = 0
    read_count = 0
    while total <= MAX_RESPONSE_BYTES:
        _check_deadline(deadline, "responsstream-voor-read")
        read_count += 1
        if read_count > MAX_HTTP_READ_CHUNKS:
            raise SynopticNativeContractError(
                "Synoptic-responsstream overschrijdt de chunkgrens"
            )
        request_bytes = min(
            HTTP_READ_CHUNK_BYTES,
            MAX_RESPONSE_BYTES + 1 - total,
        )
        chunk = response.read(request_bytes)
        _check_deadline(deadline, "responsstream-na-read")
        if not chunk:
            break
        if not isinstance(chunk, (bytes, bytearray, memoryview)):
            raise SynopticNativeContractError(
                "Synoptic-responsstream leverde geen bytes"
            )
        chunk_size = len(chunk)
        if chunk_size > request_bytes or total + chunk_size > MAX_RESPONSE_BYTES:
            raise SynopticNativeContractError(
                "Synoptic-responsstream overschrijdt de aangevraagde bytegrens"
            )
        if declared_bytes is not None and total + chunk_size > declared_bytes:
            raise SynopticNativeContractError(
                "Synoptic-responslengte wijkt af van Content-Length"
            )
        immutable = bytes(chunk)
        body.extend(immutable)
        total += chunk_size
    payload = bytes(body)
    if declared_bytes is not None and len(payload) != declared_bytes:
        raise SynopticNativeContractError(
            "Synoptic-responslengte wijkt af van Content-Length"
        )
    if not payload:
        raise SynopticNativeContractError("Synoptic-response is leeg")
    return payload


def _download_response(
    query: SynopticQuery,
    *,
    context: dict[str, object],
    credential: str,
    deadline: float | None = None,
) -> SynopticHttpResponse:
    if deadline is None:
        deadline = _new_job_deadline()
    _check_deadline(deadline, "download-start")
    context_bytes = _canonical_json_bytes(context)
    secret_bytes = credential.encode("utf-8")
    if secret_bytes in context_bytes:
        raise SynopticNativeContractError(
            "Synoptic querycontext overlapt de workercredential"
        )
    request_url = _request_url(query)
    request = Request(
        request_url,
        headers={
            "Accept": "application/json",
            "Accept-Encoding": "identity",
            "Authorization": f"Bearer {credential}",
            "User-Agent": "StormTrackerWorker/1.16",
        },
    )
    try:
        timeout = min(
            HTTP_IO_TIMEOUT_SECONDS,
            _remaining_seconds(deadline, "request-open"),
        )
        with _open_synoptic_request(
            request,
            timeout=timeout,
        ) as response:
            _check_deadline(deadline, "response-headers")
            status = getattr(response, "status", None)
            if status in {401, 403, 429} or (
                isinstance(status, int) and status >= 500
            ):
                raise SynopticNativeProviderResponseError(
                    http_status=int(status),
                    retryable=(status == 429 or status >= 500),
                )
            if status != 200:
                raise SynopticNativeContractError(
                    "Synoptic-response heeft geen HTTP 200-status"
                )
            geturl = getattr(response, "geturl", None)
            if not callable(geturl) or geturl() != request_url:
                raise SynopticNativeContractError(
                    "Synoptic-response komt niet van de exacte request-URL"
                )
            headers = getattr(response, "headers", None)
            encoding = (_single_header(headers, "Content-Encoding") or "identity")
            if encoding.strip().lower() != "identity":
                raise SynopticNativeContractError(
                    "Synoptic-response heeft onverwachte transportcompressie"
                )
            content_type_header = _single_header(headers, "Content-Type")
            content_type = (
                str(content_type_header or "").split(";", 1)[0].strip().lower()
            )
            if content_type not in _ALLOWED_CONTENT_TYPES:
                raise SynopticNativeContractError(
                    "Synoptic-response heeft een onverwacht Content-Type"
                )
            declared = _declared_content_length(headers)
            payload = _read_response_body(
                response,
                declared_bytes=declared,
                deadline=deadline,
            )
            _reject_credential_echo(
                payload,
                credential,
                deadline=deadline,
            )
            _check_deadline(deadline, "download-compleet")
            received_at = float(time.time())
            if not math.isfinite(received_at) or received_at < 0.0:
                raise SynopticNativeContractError(
                    "Synoptic ontvangsttijd is ongeldig"
                )
            return SynopticHttpResponse(
                payload=payload,
                content_type=content_type,
                received_at=received_at,
                credential_scan_attestation=_CREDENTIAL_SCAN_ATTESTATION,
            )
    except SynopticNativeContractError:
        raise
    except HTTPError as error:
        # urllib raises this before yielding a response object.  Its body did
        # not pass our origin/type/size/credential gates, so it is never read
        # or persisted.  The numeric status is safe, payload-free metadata.
        status = getattr(error, "code", None)
        if status in {401, 403, 429} or (
            isinstance(status, int) and status >= 500
        ):
            raise SynopticNativeProviderResponseError(
                http_status=int(status),
                retryable=(status == 429 or status >= 500),
            ) from None
        raise SynopticNativeContractError(
            "Synoptic-response heeft geen toegelaten HTTP-status"
        ) from None
    except Exception:
        # urllib exceptions can include request details. Never chain or
        # interpolate them because headers can contain the credential.
        raise SynopticNativeTransportError(
            "Synoptic netwerkcapture is mislukt zonder remote details"
        ) from None


def _bounded_json_int(value: str) -> int:
    if len(value.lstrip("-")) > 19:
        raise SynopticNativeContractError("Synoptic JSON-getal is onbegrensd")
    return int(value)


def _bounded_json_float(value: str) -> float:
    if len(value) > 64:
        raise SynopticNativeContractError("Synoptic JSON-getal is onbegrensd")
    result = float(value)
    if not math.isfinite(result):
        raise SynopticNativeContractError("Synoptic JSON bevat een niet-eindig getal")
    return result


def _reject_json_constant(_value: str):
    raise SynopticNativeContractError("Synoptic JSON bevat een niet-eindig getal")


def _unique_json_object(pairs: Iterable[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise SynopticNativeContractError(
                "Synoptic JSON bevat een dubbele objectsleutel"
            )
        if len(result) >= MAX_JSON_OBJECT_MEMBERS:
            raise SynopticNativeContractError(
                "Synoptic JSON-object overschrijdt de membergrens"
            )
        result[key] = value
    return result


def _validate_json_bounds(
    root: object,
    *,
    deadline: float | None = None,
) -> None:
    stack: list[tuple[object, int]] = [(root, 0)]
    nodes = 0
    while stack:
        value, depth = stack.pop()
        nodes += 1
        if nodes % 1024 == 0:
            _check_deadline(deadline, "JSON-structuurscan")
        if nodes > MAX_JSON_NODES:
            raise SynopticNativeContractError(
                "Synoptic JSON overschrijdt de structuurlimiet"
            )
        if depth > MAX_JSON_DEPTH:
            raise SynopticNativeContractError(
                "Synoptic JSON overschrijdt de dieptelimiet"
            )
        if isinstance(value, dict):
            if len(value) > MAX_JSON_OBJECT_MEMBERS:
                raise SynopticNativeContractError(
                    "Synoptic JSON-object overschrijdt de membergrens"
                )
            for key, child in value.items():
                if not isinstance(key, str) or len(key) > MAX_JSON_KEY_CHARS:
                    raise SynopticNativeContractError(
                        "Synoptic JSON bevat een onbegrensde objectsleutel"
                    )
                stack.append((child, depth + 1))
        elif isinstance(value, list):
            if len(value) > MAX_JSON_ARRAY_ITEMS:
                raise SynopticNativeContractError(
                    "Synoptic JSON-array overschrijdt de itemgrens"
                )
            stack.extend((child, depth + 1) for child in value)
        elif isinstance(value, str):
            if len(value) > MAX_JSON_STRING_CHARS:
                raise SynopticNativeContractError(
                    "Synoptic JSON-string overschrijdt de tekenlimiet"
                )
        elif value is not None and not isinstance(value, (bool, int, float)):
            raise SynopticNativeContractError(
                "Synoptic JSON bevat een onbekend waardetype"
            )
        elif isinstance(value, float) and not math.isfinite(value):
            raise SynopticNativeContractError(
                "Synoptic JSON bevat een niet-eindig getal"
            )


def _is_bounded_string(value: object, *, maximum: int = 256) -> bool:
    return isinstance(value, str) and 0 < len(value) <= maximum


def _is_precip_value_array(value: object, sample_count: int) -> bool:
    if not isinstance(value, list) or len(value) != sample_count:
        return False
    return all(
        item is None
        or (
            not isinstance(item, bool)
            and isinstance(item, (int, float))
            and math.isfinite(float(item))
        )
        for item in value
    )


def _qc_flag_shape(
    value: object,
    *,
    sample_count: int,
    precipitation_fields: frozenset[str],
) -> bool:
    if isinstance(value, bool):
        return True
    if not isinstance(value, dict):
        return False
    if not precipitation_fields.issubset(value):
        return False
    matched_precipitation_field = False
    for key, flags in value.items():
        if key not in precipitation_fields:
            continue
        matched_precipitation_field = True
        if isinstance(flags, bool):
            continue
        if (
            not isinstance(flags, list)
            or len(flags) != sample_count
            or any(item is not None and not isinstance(item, bool) for item in flags)
        ):
            return False
    return matched_precipitation_field


def _qc_summary_shape(value: object) -> bool:
    if not isinstance(value, dict):
        return False
    applied = value.get("QC_TESTS_APPLIED", value.get("QC_CHECKS_APPLIED"))
    flagged = value.get("TOTAL_OBSERVATIONS_FLAGGED")
    return (
        isinstance(applied, list)
        and len(applied) <= 4096
        and all(_is_bounded_string(item) for item in applied)
        and not isinstance(flagged, bool)
        and isinstance(flagged, int)
        and flagged >= 0
    )


def _sensor_variable_shape(
    value: object,
    *,
    interval_fields: frozenset[str],
    accumulated_fields: frozenset[str],
) -> bool:
    if not isinstance(value, dict):
        return False
    interval_metadata = value.get("precip_intervals")
    accumulated_metadata = value.get("precip_accumulated")
    if not isinstance(interval_metadata, dict) or not isinstance(
        accumulated_metadata, dict
    ):
        return False
    if not interval_fields.issubset(interval_metadata):
        return False
    if not accumulated_fields.issubset(accumulated_metadata):
        return False
    return all(
        isinstance(metadata, dict)
        for metadata in (
            *(interval_metadata[field] for field in interval_fields),
            *(accumulated_metadata[field] for field in accumulated_fields),
        )
    )


def _verified_success_structure(
    document: dict[str, object],
    stations: list[object],
    *,
    deadline: float | None,
) -> bool:
    units = document.get("UNITS")
    if (
        not isinstance(units, dict)
        or not all(
            _is_bounded_string(units.get(key))
            for key in ("precip_intervals", "precip_accumulated")
        )
        or not _qc_summary_shape(document.get("QC_SUMMARY"))
    ):
        return False

    station_ids: set[str] = set()
    for station in stations:
        _check_deadline(deadline, "stationsstructuurscan")
        if not isinstance(station, dict):
            return False
        stid = station.get("STID")
        if not _is_bounded_string(stid, maximum=64) or stid in station_ids:
            return False
        station_ids.add(stid)
        for coordinate_name, minimum, maximum in (
            ("LATITUDE", -90.0, 90.0),
            ("LONGITUDE", -180.0, 180.0),
        ):
            coordinate = station.get(coordinate_name)
            if isinstance(coordinate, bool) or not isinstance(
                coordinate, (str, int, float)
            ):
                return False
            try:
                coordinate_value = float(coordinate)
                if not math.isfinite(coordinate_value) or not (
                    minimum <= coordinate_value <= maximum
                ):
                    return False
            except (TypeError, ValueError, OverflowError):
                return False

        observations = station.get("OBSERVATIONS")
        if not isinstance(observations, dict):
            return False
        date_time = observations.get("date_time")
        if (
            not isinstance(date_time, list)
            or not date_time
            or len(date_time) > MAX_JSON_ARRAY_ITEMS
            or any(
                not _is_bounded_string(item, maximum=128)
                for item in date_time
            )
        ):
            return False
        interval_fields = frozenset(
            key
            for key in observations
            if isinstance(key, str) and _PRECIP_INTERVAL_FIELD_RE.fullmatch(key)
        )
        accumulated_fields = frozenset(
            key
            for key in observations
            if isinstance(key, str)
            and _PRECIP_ACCUMULATED_FIELD_RE.fullmatch(key)
        )
        if not interval_fields or not accumulated_fields:
            return False
        precipitation_fields = interval_fields | accumulated_fields
        if any(
            not _is_precip_value_array(observations[field], len(date_time))
            for field in precipitation_fields
        ):
            return False
        if not _sensor_variable_shape(
            station.get("SENSOR_VARIABLES"),
            interval_fields=interval_fields,
            accumulated_fields=accumulated_fields,
        ):
            return False
        if not _qc_flag_shape(
            station.get("QC_FLAGGED"),
            sample_count=len(date_time),
            precipitation_fields=precipitation_fields,
        ):
            return False
    return True


def _provider_error_is_retryable(
    *,
    http_status: int | None,
    response_code: int,
    response_message: str,
) -> bool:
    status = http_status if http_status is not None else response_code
    if status == 429 or status >= 500:
        return True
    if status in {401, 403}:
        return False
    normalized = response_message.casefold()
    return any(
        marker in normalized
        for marker in (
            "too many requests",
            "rate limit",
            "temporarily unavailable",
            "provider unavailable",
            "internal error",
            "please wait",
            "retry",
        )
    )


def _inspect_response(
    payload: bytes,
    *,
    deadline: float | None = None,
) -> SynopticJsonIdentity:
    """Validate JSON/API shape only after the exact response is durable."""

    if not isinstance(payload, bytes) or not payload:
        raise SynopticNativeContractError(
            "Synoptic raw response ontbreekt voor inspectie"
        )
    _check_deadline(deadline, "JSON-parse-start")
    try:
        text = payload.decode("utf-8", errors="strict")
        document = json.loads(
            text,
            object_pairs_hook=_unique_json_object,
            parse_constant=_reject_json_constant,
            parse_float=_bounded_json_float,
            parse_int=_bounded_json_int,
        )
    except SynopticNativeContractError:
        raise
    except (RecursionError, UnicodeDecodeError, json.JSONDecodeError, ValueError):
        raise SynopticNativeContractError(
            "Synoptic-response is geen geldige begrensde UTF-8 JSON"
        ) from None
    _check_deadline(deadline, "JSON-parse-einde")
    _validate_json_bounds(document, deadline=deadline)
    if not isinstance(document, dict):
        raise SynopticNativeContractError(
            "Synoptic-response heeft geen JSON-object als root"
        )
    summary = document.get("SUMMARY")
    if not isinstance(summary, dict):
        raise SynopticNativeContractError(
            "Synoptic-response mist een structurele SUMMARY"
        )
    response_code = summary.get("RESPONSE_CODE")
    response_message = summary.get("RESPONSE_MESSAGE")
    object_count = summary.get("NUMBER_OF_OBJECTS")
    http_status = summary.get("HTTP_STATUS_CODE")
    if (
        isinstance(response_code, bool)
        or not isinstance(response_code, int)
        or not isinstance(response_message, str)
        or len(response_message) > 1024
        or (
            http_status is not None
            and (
                isinstance(http_status, bool)
                or not isinstance(http_status, int)
                or not 100 <= http_status <= 599
            )
        )
    ):
        raise SynopticNativeContractError(
            "Synoptic SUMMARY heeft een onbekend structureel contract"
        )
    stations = document.get("STATION")
    if response_code == 1:
        if (
            isinstance(object_count, bool)
            or not isinstance(object_count, int)
            or not 1 <= object_count <= MAX_STATIONS_PER_QUERY
            or not isinstance(stations, list)
            or len(stations) != object_count
            or len(stations) > MAX_STATIONS_PER_QUERY
            or any(not isinstance(item, dict) for item in stations)
        ):
            raise SynopticNativeContractError(
                "Synoptic succesrespons heeft een onbekende stationsstructuur"
            )
        structure_verified = _verified_success_structure(
            document,
            stations,
            deadline=deadline,
        )
        return SynopticJsonIdentity(
            response_class=(
                "success" if structure_verified
                else "success_structurally_unverified"
            ),
            structural_status=(
                "verified_precip_sensor_qc_structure"
                if structure_verified
                else "structurally_unverified"
            ),
            precipitation_structure_verified=structure_verified,
            sensor_metadata_structure_verified=structure_verified,
            qc_structure_verified=structure_verified,
        )
    elif response_code == 2:
        if (
            object_count not in (None, 0)
            or stations not in (None, [])
        ):
            raise SynopticNativeContractError(
                "Synoptic responsecode 2 heeft geen begrensde ambiguiteitsvorm"
            )
        normalized_message = " ".join(response_message.split())
        if (
            http_status not in (None, 200)
            or _CODE_TWO_AMBIGUITY_RE.fullmatch(normalized_message) is None
        ):
            raise SynopticNativeProviderResponseError(
                "Synoptic providerresponse is provider_error en niet compleet",
                http_status=http_status,
                retryable=_provider_error_is_retryable(
                    http_status=http_status,
                    response_code=response_code,
                    response_message=response_message,
                ),
            )
        # The narrowly accepted official ambiguity is not proof that the
        # requested radius was observed: it can mean no matching stations or
        # inaccessible data. Never turn it into precipitation or coverage.
        return SynopticJsonIdentity(
            response_class="ambiguous_no_data_or_access",
            structural_status="verified_official_code_2_ambiguity",
            precipitation_structure_verified=False,
            sensor_metadata_structure_verified=False,
            qc_structure_verified=False,
        )
    else:
        raise SynopticNativeProviderResponseError(
            "Synoptic providerresponse is geen succesvolle capture",
            http_status=(
                response_code if 100 <= response_code <= 599 else None
            ),
            retryable=_provider_error_is_retryable(
                http_status=http_status,
                response_code=response_code,
                response_message=response_message,
            ),
        )


def _bounded_cache_put(cache: OrderedDict, key: object, value: object) -> None:
    cache[key] = value
    cache.move_to_end(key)
    while len(cache) > MAX_RUNTIME_CACHE_ENTRIES:
        cache.popitem(last=False)


def _inspection_cache_key(content_sha256: str) -> tuple[str, object, str]:
    return (
        SYNOPTIC_STRUCTURE_INSPECTOR_VERSION,
        _inspect_response,
        content_sha256,
    )


def _inspect_response_cached(
    payload: bytes,
    *,
    content_sha256: str,
    deadline: float,
) -> SynopticJsonIdentity:
    key = _inspection_cache_key(content_sha256)
    with _SYNOPTIC_RUNTIME_CACHE_LOCK:
        cached = _INSPECTION_CACHE.get(key)
        if cached is not None:
            _INSPECTION_CACHE.move_to_end(key)
    _check_deadline(deadline, "inspectiecache")
    if isinstance(cached, _CachedRejection):
        cached.raise_safe()
    if isinstance(cached, SynopticJsonIdentity):
        return cached
    try:
        identity = _inspect_response(payload, deadline=deadline)
    except SynopticNativeDeadlineError:
        raise
    except SynopticNativeProviderResponseError as error:
        rejection = _CachedRejection(
            rejection_kind="provider_error",
            http_status=error.http_status,
            retryable=error.retryable,
        )
        with _SYNOPTIC_RUNTIME_CACHE_LOCK:
            _bounded_cache_put(_INSPECTION_CACHE, key, rejection)
        raise
    except SynopticNativeContractError:
        rejection = _CachedRejection(
            rejection_kind="contract_error",
            http_status=None,
            retryable=False,
        )
        with _SYNOPTIC_RUNTIME_CACHE_LOCK:
            _bounded_cache_put(_INSPECTION_CACHE, key, rejection)
        raise
    with _SYNOPTIC_RUNTIME_CACHE_LOCK:
        _bounded_cache_put(_INSPECTION_CACHE, key, identity)
    return identity


def _raw_store(injected: RawArtifactStore | None = None) -> RawArtifactStore:
    if injected is not None:
        injected.initialize()
        return injected
    share_root = SYNOPTIC_RAW_SHARE_ROOT.resolve(strict=False)
    raw_root = SYNOPTIC_RAW_ARTIFACT_ROOT.resolve(strict=False)
    if raw_root == share_root or share_root not in raw_root.parents:
        raise RuntimeError("Synoptic raw-artifactpad ontsnapt uit worker-share")
    global _SYNOPTIC_RAW_STORE
    with _SYNOPTIC_RAW_STORE_LOCK:
        if _SYNOPTIC_RAW_STORE is None:
            candidate = RawArtifactStore(raw_root)
            candidate.initialize()
            _SYNOPTIC_RAW_STORE = candidate
        return _SYNOPTIC_RAW_STORE


def _native_provenance(
    context: dict[str, object],
    *,
    request_attempt: int,
) -> dict[str, object]:
    return {
        "representation": NATIVE_REPRESENTATION,
        "capture_stage": NATIVE_CAPTURE_STAGE,
        "collector": SYNOPTIC_RAW_COLLECTOR,
        "collector_version": SYNOPTIC_RAW_COLLECTOR_VERSION,
        "structure_inspector_version": SYNOPTIC_STRUCTURE_INSPECTOR_VERSION,
        "query_context": context,
        "query_context_sha256": _query_context_sha256(context),
        "request_attempt": request_attempt,
        "requested_semantic_class": SYNOPTIC_SEMANTIC_CLASS,
        "response_structure_claim": "none_before_post_store_inspection",
        "provider_qc_request": "requested_and_exact_bytes_preserved",
        "precipitation_request": "derived_precipitation_requested",
        "coverage_status": SYNOPTIC_COVERAGE_STATUS,
        "meteorological_interpretation_verified_after_decode": False,
    }


def _receipt_fetch_id(job_id: str, request_attempt: int) -> str:
    if (
        isinstance(request_attempt, bool)
        or not isinstance(request_attempt, int)
        or not 1 <= request_attempt <= MAX_REQUEST_ATTEMPTS_PER_JOB
    ):
        raise ValueError("Synoptic request_attempt valt buiten de grens")
    return f"{job_id}:response_json:attempt:{request_attempt}"


def _receipt_id(
    store: RawArtifactStore,
    job_id: str,
    request_attempt: int = 1,
) -> str:
    return store._receipt_id(  # noqa: SLF001 - worker store identity contract
        provider="synoptic",
        product=SYNOPTIC_JSON_PRODUCT,
        fetch_id=_receipt_fetch_id(job_id, request_attempt),
    )


def _capture_id(store: RawArtifactStore, job_id: str) -> str:
    return store._capture_id(  # noqa: SLF001 - worker store identity contract
        provider="synoptic",
        product=SYNOPTIC_CAPTURE_PRODUCT,
        fetch_id=f"{job_id}:capture",
    )


def _expected_upstream_id(
    context: dict[str, object],
    content_sha256: str,
    *,
    request_attempt: int,
) -> str:
    return (
        "synoptic/stations/timeseries"
        f"|query_sha256={_query_context_sha256(context)}"
        f"|request_attempt={request_attempt}"
        f"|content_sha256={content_sha256}"
    )


def _validate_receipt(
    receipt: RawArtifactReceipt,
    *,
    job_id: str,
    context: dict[str, object],
    request_attempt: int,
) -> None:
    expected_provenance = _native_provenance(
        context,
        request_attempt=request_attempt,
    )
    if (
        receipt.provider != "synoptic"
        or receipt.product != SYNOPTIC_JSON_PRODUCT
        or receipt.fetch_id != _receipt_fetch_id(job_id, request_attempt)
        or receipt.content_type not in _ALLOWED_CONTENT_TYPES
        or receipt.size_bytes <= 0
        or receipt.size_bytes > MAX_RESPONSE_BYTES
        or receipt.upstream_id
        != _expected_upstream_id(
            context,
            receipt.content_sha256,
            request_attempt=request_attempt,
        )
        or receipt.provenance != expected_provenance
    ):
        raise RuntimeError(
            "duurzaam Synoptic-receipt heeft een verkeerde identiteit"
        )


def _commit_capture(
    store: RawArtifactStore,
    *,
    job_id: str,
    receipt: RawArtifactReceipt,
    context: dict[str, object],
) -> RawArtifactCaptureReceipt:
    return store.commit_capture(
        provider="synoptic",
        product=SYNOPTIC_CAPTURE_PRODUCT,
        fetch_id=f"{job_id}:capture",
        upstream_capture_id=(
            f"{SYNOPTIC_CAPTURE_PRODUCT}"
            f"|query_sha256={_query_context_sha256(context)}"
            f"|content_sha256={receipt.content_sha256}"
        ),
        # One response contains independent station observation timestamps.
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
    expected_upstream = (
        f"{SYNOPTIC_CAPTURE_PRODUCT}"
        f"|query_sha256={_query_context_sha256(context)}"
        f"|content_sha256={receipt.content_sha256}"
    )
    members = {member.role: member.receipt_id for member in capture.members}
    if (
        capture.provider != "synoptic"
        or capture.product != SYNOPTIC_CAPTURE_PRODUCT
        or capture.fetch_id != f"{job_id}:capture"
        or capture.upstream_capture_id != expected_upstream
        or capture.product_timestamp is not None
        or members != {"response_json": receipt.receipt_id}
    ):
        raise RuntimeError(
            "duurzame Synoptic-capture heeft een verkeerde identiteit"
        )


def _source_reference(
    receipt: RawArtifactReceipt,
    *,
    context: dict[str, object],
) -> dict[str, object]:
    return {
        "role": "response_json",
        "raw_receipt_id": receipt.receipt_id,
        "raw_content_sha256": receipt.content_sha256,
        "raw_receipt_sha256": receipt.receipt_sha256,
        "fetch_id": receipt.fetch_id,
        "upstream_record_id": receipt.upstream_id,
        "received_at": receipt.received_at,
        "content_type": receipt.content_type,
        "collector_version": SYNOPTIC_RAW_COLLECTOR_VERSION,
        "structure_inspector_version": SYNOPTIC_STRUCTURE_INSPECTOR_VERSION,
        "query_context_sha256": _query_context_sha256(context),
        "request_attempt": receipt.provenance["request_attempt"],
        "requested_semantic_class": SYNOPTIC_SEMANTIC_CLASS,
        "response_structure_claim": "post_store_result_only",
    }


def _capture_provenance(
    capture: RawArtifactCaptureReceipt,
    *,
    receipt: RawArtifactReceipt,
    context: dict[str, object],
    identity: SynopticJsonIdentity,
) -> dict[str, object]:
    return {
        "raw_store_schema_version": capture.raw_store_schema_version,
        "raw_capture_id": capture.capture_id,
        "raw_capture_manifest_sha256": capture.manifest_sha256,
        "raw_capture_receipt_sha256": capture.capture_receipt_sha256,
        "raw_capture_fetch_id": capture.fetch_id,
        "upstream_capture_id": capture.upstream_capture_id,
        "product_timestamp": None,
        "collector_version": SYNOPTIC_RAW_COLLECTOR_VERSION,
        "provenance_grade": "exact_raw",
        "provenance_scope": (
            "upstream_json_response_with_credential_free_query_context"
        ),
        "request_attempt": receipt.provenance["request_attempt"],
        "response_structure_status": identity.structural_status,
        "precipitation_structure_verified": (
            identity.precipitation_structure_verified
        ),
        "sensor_metadata_structure_verified": (
            identity.sensor_metadata_structure_verified
        ),
        "qc_structure_verified": identity.qc_structure_verified,
        "coverage_status": SYNOPTIC_COVERAGE_STATUS,
        "query_context": context,
        "query_context_sha256": _query_context_sha256(context),
        "raw_sources": {
            "response_json": _source_reference(
                receipt,
                context=context,
            )
        },
    }


def _capture_result(
    *,
    capture: RawArtifactCaptureReceipt,
    receipt: RawArtifactReceipt,
    context: dict[str, object],
    identity: SynopticJsonIdentity,
) -> dict[str, object]:
    structure_verified = identity.structural_status == (
        "verified_precip_sensor_qc_structure"
    )
    return {
        "provider": "synoptic",
        "product": SYNOPTIC_JSON_PRODUCT,
        "semantic_class": (
            SYNOPTIC_SEMANTIC_CLASS
            if structure_verified
            else "uninterpreted_synoptic_json_response"
        ),
        "query_scope": "per_tracker",
        "query_identity": context["query_identity"],
        "query_kind": context["query_kind"],
        "station_selection_mode": "precipitation_variables_only",
        "coverage_status": SYNOPTIC_COVERAGE_STATUS,
        "area_coverage_evidence": "none",
        "provider_response_class": identity.response_class,
        "response_structure_status": identity.structural_status,
        "precipitation_structure_verified": (
            identity.precipitation_structure_verified
        ),
        "sensor_metadata_structure_verified": (
            identity.sensor_metadata_structure_verified
        ),
        "qc_structure_verified": identity.qc_structure_verified,
        "evidence_origin": (
            "provider_derived" if structure_verified
            else "provider_response_not_structurally_verified"
        ),
        "provider_qc_status": (
            "preserved_structurally_verified_not_interpreted"
            if structure_verified
            else "not_claimed_structure_unverified"
        ),
        "precipitation_interval_semantics": (
            "rolling_or_report_interval_not_instantaneous"
            if structure_verified
            else "not_claimed_structure_unverified"
        ),
        "query_load": {
            "network_requests_per_new_job": 1,
            "network_requests_per_exact_job_retry": 0,
            "legacy_minimum_interval_seconds": (
                LEGACY_MIN_QUERY_INTERVAL_SECONDS
            ),
            "result_cache_scope": "exact_job_receipt_only",
            "maximum_request_attempts_per_job": (
                MAX_REQUEST_ATTEMPTS_PER_JOB
            ),
        },
        "decode_status": "not_meteorologically_decoded",
        "capture_state": "captured_not_decoded",
        "weather_state": "unknown",
        "operational_admission_eligible": False,
        "admission_status": SYNOPTIC_ADMISSION_STATUS,
        "raw_provenance": _capture_provenance(
            capture,
            receipt=receipt,
            context=context,
            identity=identity,
        ),
    }


def _store_scope(store: RawArtifactStore) -> str:
    return os.path.normcase(str(store.root.resolve(strict=False)))


def _job_state_key(
    store: RawArtifactStore,
    *,
    job_id: str,
    context: dict[str, object],
) -> tuple[str, str, str]:
    return (_store_scope(store), job_id, _query_context_sha256(context))


def _complete_result_key(
    store: RawArtifactStore,
    capture: RawArtifactCaptureReceipt,
) -> tuple[str, str, str]:
    return (
        _store_scope(store),
        capture.capture_id,
        capture.capture_receipt_sha256,
    )


def _cached_complete_result(
    store: RawArtifactStore,
    capture: RawArtifactCaptureReceipt,
) -> dict[str, object] | None:
    key = _complete_result_key(store, capture)
    with _SYNOPTIC_RUNTIME_CACHE_LOCK:
        cached = _COMPLETE_RESULT_CACHE.get(key)
        if cached is None:
            return None
        _COMPLETE_RESULT_CACHE.move_to_end(key)
        return copy.deepcopy(cached)


def _cache_complete_result(
    store: RawArtifactStore,
    capture: RawArtifactCaptureReceipt,
    result: dict[str, object],
) -> None:
    with _SYNOPTIC_RUNTIME_CACHE_LOCK:
        _bounded_cache_put(
            _COMPLETE_RESULT_CACHE,
            _complete_result_key(store, capture),
            copy.deepcopy(result),
        )


def _cached_job_state(
    key: tuple[str, str, str],
) -> tuple[_CachedRejection | None, int | None]:
    with _SYNOPTIC_RUNTIME_CACHE_LOCK:
        rejection = _JOB_REJECTION_CACHE.get(key)
        if rejection is not None:
            _JOB_REJECTION_CACHE.move_to_end(key)
        next_attempt = _JOB_NEXT_ATTEMPT_CACHE.get(key)
        if next_attempt is not None:
            _JOB_NEXT_ATTEMPT_CACHE.move_to_end(key)
    return rejection, next_attempt


def _cache_job_rejection(
    key: tuple[str, str, str],
    rejection: _CachedRejection,
) -> None:
    with _SYNOPTIC_RUNTIME_CACHE_LOCK:
        _bounded_cache_put(_JOB_REJECTION_CACHE, key, rejection)
        _JOB_NEXT_ATTEMPT_CACHE.pop(key, None)


def _cache_next_attempt(
    key: tuple[str, str, str],
    next_attempt: int,
) -> None:
    with _SYNOPTIC_RUNTIME_CACHE_LOCK:
        _bounded_cache_put(_JOB_NEXT_ATTEMPT_CACHE, key, next_attempt)
        _JOB_REJECTION_CACHE.pop(key, None)


def _clear_job_state(key: tuple[str, str, str]) -> None:
    with _SYNOPTIC_RUNTIME_CACHE_LOCK:
        _JOB_REJECTION_CACHE.pop(key, None)
        _JOB_NEXT_ATTEMPT_CACHE.pop(key, None)


def _rejection_for_error(
    error: SynopticNativeContractError,
) -> _CachedRejection:
    if isinstance(error, SynopticNativeProviderResponseError):
        return _CachedRejection(
            rejection_kind="provider_error",
            http_status=error.http_status,
            retryable=error.retryable,
        )
    return _CachedRejection(
        rejection_kind="contract_error",
        http_status=None,
        retryable=False,
    )


def _clear_runtime_caches_for_tests() -> None:
    with _SYNOPTIC_RUNTIME_CACHE_LOCK:
        _INSPECTION_CACHE.clear()
        _COMPLETE_RESULT_CACHE.clear()
        _JOB_REJECTION_CACHE.clear()
        _JOB_NEXT_ATTEMPT_CACHE.clear()


def _resume_existing(
    store: RawArtifactStore,
    *,
    job_id: str,
    context: dict[str, object],
    deadline: float,
) -> tuple[dict[str, object] | None, int]:
    _check_deadline(deadline, "resume-start")
    job_state_key = _job_state_key(
        store,
        job_id=job_id,
        context=context,
    )
    bundle = store.load_capture_bundle_bytes(_capture_id(store, job_id))
    if bundle is not None:
        capture, receipts, payloads = bundle
        if (
            frozenset(receipts) != frozenset({"response_json"})
            or frozenset(payloads) != frozenset({"response_json"})
        ):
            raise RuntimeError(
                "Synoptic-capture heeft geen exact responsemember"
            )
        receipt = receipts["response_json"]
        expected_prefix = f"{job_id}:response_json:attempt:"
        if not receipt.fetch_id.startswith(expected_prefix):
            raise RuntimeError(
                "duurzaam Synoptic-receipt heeft geen request-attempt"
            )
        attempt_text = receipt.fetch_id[len(expected_prefix):]
        if re.fullmatch(r"[1-9][0-9]*", attempt_text) is None:
            raise RuntimeError(
                "duurzaam Synoptic-receipt heeft een ongeldige request-attempt"
            )
        request_attempt = int(attempt_text)
        _validate_receipt(
            receipt,
            job_id=job_id,
            context=context,
            request_attempt=request_attempt,
        )
        _validate_capture(
            capture,
            job_id=job_id,
            receipt=receipt,
            context=context,
        )
        cached_result = _cached_complete_result(store, capture)
        if cached_result is not None:
            _clear_job_state(job_state_key)
            return cached_result, request_attempt
        identity = _inspect_response_cached(
            payloads["response_json"],
            content_sha256=receipt.content_sha256,
            deadline=deadline,
        )
        _check_deadline(deadline, "resume-complete-capture")
        result = _capture_result(
            capture=capture,
            receipt=receipt,
            context=context,
            identity=identity,
        )
        _cache_complete_result(store, capture, result)
        _clear_job_state(job_state_key)
        return (
            result,
            request_attempt,
        )

    cached_rejection, cached_next_attempt = _cached_job_state(job_state_key)
    if cached_rejection is not None:
        cached_rejection.raise_safe()
    scan_start_attempt = cached_next_attempt or 1
    if not 1 <= scan_start_attempt <= MAX_REQUEST_ATTEMPTS_PER_JOB:
        raise SynopticNativeProviderResponseError(
            "Synoptic request-attemptgrens is bereikt",
            retryable=False,
        )

    first_missing_attempt: int | None = None
    for request_attempt in range(
        scan_start_attempt,
        MAX_REQUEST_ATTEMPTS_PER_JOB + 1,
    ):
        _check_deadline(deadline, "resume-attemptscan")
        try:
            receipt, exact_payload = store.read_receipt_bytes(
                _receipt_id(store, job_id, request_attempt)
            )
        except RawArtifactNotFoundError:
            if first_missing_attempt is None:
                first_missing_attempt = request_attempt
            continue
        if first_missing_attempt is not None:
            raise RuntimeError(
                "Synoptic request-attemptreeks bevat een duurzaam gat"
            )
        _validate_receipt(
            receipt,
            job_id=job_id,
            context=context,
            request_attempt=request_attempt,
        )
        try:
            identity = _inspect_response_cached(
                exact_payload,
                content_sha256=receipt.content_sha256,
                deadline=deadline,
            )
        except SynopticNativeDeadlineError:
            raise
        except SynopticNativeProviderResponseError as error:
            rejection = _rejection_for_error(error)
            if (
                rejection.retryable
                and request_attempt < MAX_REQUEST_ATTEMPTS_PER_JOB
            ):
                _cache_next_attempt(job_state_key, request_attempt + 1)
                continue
            if rejection.retryable:
                rejection = _CachedRejection(
                    rejection_kind="provider_error",
                    http_status=rejection.http_status,
                    retryable=False,
                )
            _cache_job_rejection(job_state_key, rejection)
            raise
        except SynopticNativeContractError as error:
            _cache_job_rejection(
                job_state_key,
                _rejection_for_error(error),
            )
            raise
        capture = _commit_capture(
            store,
            job_id=job_id,
            receipt=receipt,
            context=context,
        )
        _check_deadline(deadline, "resume-capture-commit")
        result = _capture_result(
            capture=capture,
            receipt=receipt,
            context=context,
            identity=identity,
        )
        _cache_complete_result(store, capture, result)
        _clear_job_state(job_state_key)
        return (
            result,
            request_attempt,
        )

    if first_missing_attempt is None:
        rejection = _CachedRejection(
            rejection_kind="provider_error",
            http_status=None,
            retryable=False,
        )
        _cache_job_rejection(job_state_key, rejection)
        raise SynopticNativeProviderResponseError(
            "Synoptic request-attemptgrens is bereikt",
            retryable=False,
        )
    return None, first_missing_attempt


def capture_synoptic_timeseries(
    job,
    *,
    raw_store: RawArtifactStore | None = None,
) -> dict[str, object]:
    """Capture one tracker-scoped response without decoding weather facts."""

    deadline = _new_job_deadline()
    job_id = _validate_job_id(getattr(job, "job_id", None))
    query = _validate_query_payload(getattr(job, "payload", None))
    context = _query_context(query)
    _check_deadline(deadline, "lokale-validatie")
    store = _raw_store(raw_store)
    _check_deadline(deadline, "raw-store-initialisatie")
    acquired = _SYNOPTIC_CAPTURE_LOCK.acquire(
        timeout=_remaining_seconds(deadline, "capturelock")
    )
    if not acquired:
        raise SynopticNativeDeadlineError(
            "Synoptic job overschrijdt de absolute deadline (capturelock)"
        )
    try:
        resumed, request_attempt = _resume_existing(
            store,
            job_id=job_id,
            context=context,
            deadline=deadline,
        )
        if resumed is not None:
            return resumed

        job_state_key = _job_state_key(
            store,
            job_id=job_id,
            context=context,
        )
        credential = _environment_credential()
        try:
            response = _download_response(
                query,
                context=context,
                credential=credential,
                deadline=deadline,
            )
            # Keep this security boundary in the orchestration path as well as
            # in the concrete downloader so an alternate/test transport can
            # never persist a credential echo.
            if (
                response.credential_scan_attestation
                is not _CREDENTIAL_SCAN_ATTESTATION
            ):
                _reject_credential_echo(
                    response.payload,
                    credential,
                    deadline=deadline,
                )
        except (SynopticNativeDeadlineError, SynopticNativeTransportError):
            # Timing and I/O failures have no accepted response evidence and
            # remain retryable with the same request-attempt number.
            raise
        except SynopticNativeProviderResponseError as error:
            rejection = _rejection_for_error(error)
            if not rejection.retryable:
                _cache_job_rejection(job_state_key, rejection)
            raise
        except SynopticNativeContractError as error:
            _cache_job_rejection(
                job_state_key,
                _rejection_for_error(error),
            )
            raise
        _check_deadline(deadline, "voor-raw-opslag")
        content_sha256 = hashlib.sha256(response.payload).hexdigest()
        receipt = store.store(
            provider="synoptic",
            product=SYNOPTIC_JSON_PRODUCT,
            fetch_id=_receipt_fetch_id(job_id, request_attempt),
            content_type=response.content_type,
            received_at=response.received_at,
            upstream_id=_expected_upstream_id(
                context,
                content_sha256,
                request_attempt=request_attempt,
            ),
            provenance=_native_provenance(
                context,
                request_attempt=request_attempt,
            ),
            payload=response.payload,
        )

        # This is the first JSON operation. The exact response and the exact
        # credential-free query semantics are already durable in schema 3.
        _check_deadline(deadline, "na-raw-opslag")
        try:
            identity = _inspect_response_cached(
                response.payload,
                content_sha256=receipt.content_sha256,
                deadline=deadline,
            )
        except SynopticNativeDeadlineError:
            raise
        except SynopticNativeProviderResponseError as error:
            rejection = _rejection_for_error(error)
            if (
                rejection.retryable
                and request_attempt < MAX_REQUEST_ATTEMPTS_PER_JOB
            ):
                _cache_next_attempt(job_state_key, request_attempt + 1)
            else:
                if rejection.retryable:
                    rejection = _CachedRejection(
                        rejection_kind="provider_error",
                        http_status=rejection.http_status,
                        retryable=False,
                    )
                _cache_job_rejection(job_state_key, rejection)
            raise
        except SynopticNativeContractError as error:
            _cache_job_rejection(
                job_state_key,
                _rejection_for_error(error),
            )
            raise
        capture = _commit_capture(
            store,
            job_id=job_id,
            receipt=receipt,
            context=context,
        )
        _check_deadline(deadline, "capture-commit")
        result = _capture_result(
            capture=capture,
            receipt=receipt,
            context=context,
            identity=identity,
        )
        _cache_complete_result(store, capture, result)
        _clear_job_state(job_state_key)
        return result
    finally:
        _SYNOPTIC_CAPTURE_LOCK.release()


__all__ = [
    "SYNOPTIC_ADMISSION_STATUS",
    "SYNOPTIC_CAPTURE_PRODUCT",
    "SYNOPTIC_COVERAGE_STATUS",
    "SYNOPTIC_JSON_PRODUCT",
    "SYNOPTIC_QUERY_CONTEXT_SCHEMA",
    "SYNOPTIC_SEMANTIC_CLASS",
    "SynopticNativeContractError",
    "SynopticNativeDeadlineError",
    "SynopticNativeProviderResponseError",
    "SynopticNativeTransportError",
    "SynopticQuery",
    "capture_synoptic_timeseries",
]
