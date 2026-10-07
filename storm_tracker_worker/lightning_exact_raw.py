"""Standalone exact-byte RAW capture for satellite-lightning products.

This module is deliberately not wired into the worker runtime.  It provides
closed handler and recovery metadata for a later integration step, while the
capture implementation itself stops before every meteorological decode or
admission decision.

Two provider contracts are implemented:

* EUMETSAT MTG LI: the exact public search response is durable before the
  bounded JSON link selection.  The selected original NetCDF response and the
  search response then form one schema-3 capture manifest.  OAuth client
  credentials are accepted only by the explicit parent-side broker helper;
  neither credentials nor bearer tokens can enter a job, result, receipt,
  provenance document, or capture manifest.
* NOAA GOES-18/19 GLM: the exact previous- and current-hour S3 listing bodies
  are both durable before bounded XML selection.  At most twelve deterministically
  selected original NetCDF responses are retained under closed dynamic roles
  and committed with both listings in one schema-3 capture manifest.

Every bounded successful 2xx entity body is stored byte-for-byte, including an
empty or malformed body.  Malformed selection authority prevents a final
manifest but never rolls back a raw response that was already received.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import base64
import hashlib
import json
import math
import re
import threading
import time
from types import MappingProxyType
from typing import Any, Callable, ContextManager, Iterator, Mapping
from urllib.parse import parse_qsl, quote, urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener
from xml.etree import ElementTree as ET

from .raw_artifacts import (
    NATIVE_CAPTURE_STAGE,
    NATIVE_REPRESENTATION,
    RAW_ARTIFACT_SCHEMA_VERSION,
    RawArtifactCaptureReceipt,
    RawArtifactReceipt,
    RawArtifactStore,
)


CONTRACT = "stv3-worker-lightning-exact-raw-v1"
RESULT_CONTRACT = "stv3-worker-lightning-exact-raw-result-v1"
TRANSPORT_CONTRACT = "stv3-worker-lightning-https-transport-v1"
EMPTY_PAYLOAD_CONTRACT = "exact_empty_object_v1"

EUMETSAT_WORKLOAD_ID = "lightning.eumetsat.li"
EUMETSAT_HANDLER_ID = "eumetsat_mtg_li_search_netcdf_exact_raw_v1"
EUMETSAT_PROVIDER = "eumetsat"
EUMETSAT_SEARCH_PRODUCT = "eumetsat_mtg_li_search_response_bytes"
EUMETSAT_NETCDF_PRODUCT = "eumetsat_mtg_li_original_netcdf_response_bytes"
EUMETSAT_CAPTURE_PRODUCT = "eumetsat_mtg_li_exact_raw_capture"
EUMETSAT_SEARCH_HOST = "api.eumetsat.int"
EUMETSAT_SEARCH_PATH = "/data/search-products/1.0.0/os"
EUMETSAT_DOWNLOAD_PATH_PREFIX = "/data/download/"
EUMETSAT_TOKEN_PATH = "/token"
EUMETSAT_COLLECTION_ID = "EO:EUM:DAT:0691"
EUMETSAT_SEARCH_MAX_BYTES = 4 * 1024 * 1024
EUMETSAT_NETCDF_MAX_BYTES = 10 * 1024 * 1024
EUMETSAT_TOKEN_MAX_BYTES = 512 * 1024
EUMETSAT_MAX_FEATURES = 64
EUMETSAT_MAX_ENTRIES_PER_FEATURE = 256
EUMETSAT_EPHEMERAL_TOKEN_ENV = "STV3_EUMETSAT_EPHEMERAL_ACCESS_TOKEN"

GOES18_WORKLOAD_ID = "lightning.goes18.glm"
GOES19_WORKLOAD_ID = "lightning.goes19.glm"
GOES18_HANDLER_ID = "noaa_goes18_glm_listings_netcdf_exact_raw_v1"
GOES19_HANDLER_ID = "noaa_goes19_glm_listings_netcdf_exact_raw_v1"
GOES_PROVIDER = "noaa_goes"
GOES_LISTING_PRODUCT = "noaa_goes_glm_s3_listobjects_v2_response_bytes"
GOES_NETCDF_PRODUCT = "noaa_goes_glm_original_netcdf_response_bytes"
GOES_CAPTURE_PRODUCT = "noaa_goes_glm_exact_raw_capture"
GOES_PREFIX = "GLM-L2-LCFA"
GOES_MAX_LISTING_BYTES = 4 * 1024 * 1024
GOES_MAX_NETCDF_BYTES = 5 * 1024 * 1024
GOES_MAX_SELECTED_OBJECTS = 12
GOES_LIST_MAX_KEYS = 1000
GOES_S3_NAMESPACE = "http://s3.amazonaws.com/doc/2006-03-01/"

UNINTERPRETED_CONTENT_TYPE = "application/octet-stream"
MAX_URL_CHARS = 8192
MAX_HEADER_CHARS = 8192
MAX_HTTP_FIELDS = 128
HTTP_READ_CHUNK_BYTES = 64 * 1024
HTTP_IO_TIMEOUT_SECONDS = 20.0
CAPTURE_DEADLINE_SECONDS = 105.0

_SAFE_JOB_ID_RE = re.compile(
    r"^acq:[a-z2-7]{8}(?::z:[a-z2-7]{8}){5}:z:[a-z2-7]{4}$"
)
_HEADER_NAME_RE = re.compile(r"^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$")
_MEDIA_TYPE_RE = re.compile(
    r"^[A-Za-z0-9!#$&^_.+\-]+/[A-Za-z0-9!#$&^_.+\-]+$"
)
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SAFE_PRODUCT_ID_RE = re.compile(r"^[\x21-\x7e]{1,512}$")
_SAFE_TITLE_RE = re.compile(r"^[\x20-\x7e]{1,1024}$")
_SAFE_QUERY_NAME_RE = re.compile(r"^[A-Za-z0-9_.~-]{1,64}$")
_INVALID_PERCENT_RE = re.compile(r"%(?![0-9A-Fa-f]{2})")
_GOES_PREFIX_RE = re.compile(r"^GLM-L2-LCFA/\d{4}/\d{3}/\d{2}/$")
_GOES_KEY_RE = re.compile(
    r"^GLM-L2-LCFA/(?P<year>\d{4})/(?P<day>\d{3})/(?P<hour>\d{2})/"
    r"(?P<filename>OR_GLM-L2-LCFA_G(?P<satellite>18|19)_"
    r"s(?P<start>\d{14})_e(?P<end>\d{14})_c(?P<created>\d{14})\.nc)$"
)
_GOES_OBJECT_ROLE_RE = re.compile(r"^netcdf_(?P<index>0[0-9]|1[01])$")
_FORBIDDEN_XML = re.compile(br"<!\s*(?:DOCTYPE|ENTITY)", re.IGNORECASE)
_SECRET_QUERY_MARKERS = (
    "access_token",
    "authorization",
    "credential",
    "signature",
    "secret",
    "token",
)
_SENSITIVE_RESPONSE_HEADERS = frozenset({
    "authentication-info",
    "proxy-authenticate",
    "set-cookie",
    "www-authenticate",
})
_CAPTURE_LOCK = threading.Lock()


class LightningRawError(RuntimeError):
    """Base class for payload-free exact-raw lightning failures."""


class LightningRawContractError(LightningRawError):
    """A request, response, or selection escaped its closed contract."""


class LightningRawCapacityError(LightningRawError):
    """A URL, header, body, selection, or time boundary was exhausted."""


class LightningRawTransportError(LightningRawError):
    """HTTPS failed without exposing a credential or provider URL."""


class LightningRawCorruptionError(LightningRawError):
    """Durable evidence drifted from its immutable role binding."""


class EumetsatOAuthError(LightningRawError):
    """The parent-side bounded OAuth broker could not mint a token."""


@dataclass(frozen=True, slots=True)
class LightningHandlerSpec:
    handler_id: str
    workload_id: str
    provider: str
    capture_product: str
    maximum_transport_bytes: int
    maximum_artifacts: int
    maximum_wall_seconds: int
    static_roles: tuple[str, ...]
    dynamic_role_pattern: str | None
    dynamic_minimum: int
    dynamic_maximum: int
    credential_contract: str

    def child_handler_mapping(self) -> dict[str, Any]:
        """Return payload-free fields for a later closed child dispatcher."""

        return {
            "allowed_content_types": [UNINTERPRETED_CONTENT_TYPE],
            "capture_fetch_suffix": ":capture",
            "capture_product": self.capture_product,
            "capture_provenance_kind": (
                "eumetsat_mtg_li_exact_raw_v1"
                if self.provider == EUMETSAT_PROVIDER
                else "noaa_goes_glm_exact_raw_v1"
            ),
            "capture_state": "captured_not_decoded",
            "credential_contract": self.credential_contract,
            "decode_field": "decode_status",
            "decode_status": "not_decoded",
            "dynamic_role_maximum": self.dynamic_maximum,
            "dynamic_role_minimum": self.dynamic_minimum,
            "dynamic_role_pattern": self.dynamic_role_pattern,
            "handler_id": self.handler_id,
            "ledger_provider": self.provider,
            "maximum_artifacts": self.maximum_artifacts,
            "maximum_transport_bytes": self.maximum_transport_bytes,
            "maximum_wall_seconds": self.maximum_wall_seconds,
            "meteorological_parse": False,
            "native_provider": self.provider,
            "payload_contract": EMPTY_PAYLOAD_CONTRACT,
            "required_static_roles": list(self.static_roles),
            "result_provider": self.provider,
            "source_field": "sources",
            "workload_id": self.workload_id,
        }


@dataclass(frozen=True, slots=True)
class RecoveryMemberMetadata:
    role: str | None
    role_pattern: str | None
    raw_product: str
    receipt_suffix: str
    maximum_transport_bytes: int
    minimum_count: int
    maximum_count: int
    provenance_kind: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "allowed_content_types": [UNINTERPRETED_CONTENT_TYPE],
            "maximum_count": self.maximum_count,
            "maximum_transport_bytes": self.maximum_transport_bytes,
            "minimum_count": self.minimum_count,
            "provenance_kind": self.provenance_kind,
            "raw_product": self.raw_product,
            "receipt_suffix": self.receipt_suffix,
            "role": self.role,
            "role_pattern": self.role_pattern,
        }


@dataclass(frozen=True, slots=True)
class RecoveryBindingMetadata:
    handler_id: str
    workload_id: str
    provider: str
    capture_product: str
    maximum_transport_bytes: int
    minimum_artifacts: int
    maximum_artifacts: int
    members: tuple[RecoveryMemberMetadata, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "capture_fetch_suffix": ":capture",
            "capture_product": self.capture_product,
            "handler_id": self.handler_id,
            "maximum_artifacts": self.maximum_artifacts,
            "maximum_transport_bytes": self.maximum_transport_bytes,
            "members": [item.as_dict() for item in self.members],
            "minimum_artifacts": self.minimum_artifacts,
            "provider": self.provider,
            "schema": "stv3-worker-lightning-dynamic-recovery-binding-v1",
            "workload_id": self.workload_id,
        }


class EphemeralBearerToken:
    """Short-lived bearer material with a redacted representation.

    CPython cannot promise elimination of every temporary immutable copy.  The
    owned buffer is nevertheless overwritten on close and no serialization,
    mapping, provenance, or result API is exposed.
    """

    __slots__ = ("_buffer", "_closed")

    def __init__(self, value: str) -> None:
        if (
            not isinstance(value, str)
            or not value.isascii()
            or not 16 <= len(value) <= 8192
            or any(ord(char) <= 0x20 or ord(char) == 0x7f for char in value)
        ):
            raise EumetsatOAuthError("EUMETSAT access-token is ongeldig")
        self._buffer = bytearray(value.encode("ascii"))
        self._closed = False

    @property
    def closed(self) -> bool:
        return self._closed

    def authorization_value(self) -> str:
        if self._closed:
            raise EumetsatOAuthError("EUMETSAT access-token is gesloten")
        return "Bearer " + self._buffer.decode("ascii")

    def close(self) -> None:
        if not self._closed:
            for index in range(len(self._buffer)):
                self._buffer[index] = 0
            self._closed = True

    def __enter__(self) -> "EphemeralBearerToken":
        if self._closed:
            raise EumetsatOAuthError("EUMETSAT access-token is gesloten")
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def __repr__(self) -> str:
        return "EphemeralBearerToken(<redacted>)"


@dataclass(frozen=True, slots=True)
class _RequestBinding:
    url: str
    request_sha256: str
    endpoint_host: str
    endpoint_path_class: str
    access_class: str


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


@dataclass(frozen=True, slots=True)
class _EumetsatSelection:
    product_id: str
    title: str
    url: str
    url_sha256: str


@dataclass(frozen=True, slots=True)
class _GoesObject:
    satellite: int
    key: str
    key_sha256: str
    start: str
    end: str
    created: str


class _RejectRedirects(HTTPRedirectHandler):
    def redirect_request(self, *_args: Any, **_kwargs: Any):
        raise LightningRawContractError("providerredirect is niet toegestaan")


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
        raise LightningRawContractError("lightningbewijs is niet canoniek") from err


def _digest(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _remaining(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if not math.isfinite(remaining) or remaining <= 0:
        raise TimeoutError("lightning RAW-capturedeadline is verstreken")
    return remaining


def _deadline(deadline_at: float | None) -> float:
    now = time.monotonic()
    if deadline_at is None:
        return now + CAPTURE_DEADLINE_SECONDS
    if isinstance(deadline_at, bool) or not isinstance(deadline_at, (int, float)):
        raise LightningRawContractError("deadline_at is ongeldig")
    requested = float(deadline_at)
    if not math.isfinite(requested):
        raise LightningRawContractError("deadline_at is ongeldig")
    result = min(requested, now + CAPTURE_DEADLINE_SECONDS)
    _remaining(result)
    return result


def _job(job: object) -> str:
    job_id = getattr(job, "job_id", None)
    payload = getattr(job, "payload", None)
    if not isinstance(job_id, str) or _SAFE_JOB_ID_RE.fullmatch(job_id) is None:
        raise LightningRawContractError("lightningjob mist een veilige native job_id")
    if type(payload) is not dict or payload:
        raise LightningRawContractError(
            "lightning RAW-handler vereist een exact lege jobpayload"
        )
    return job_id


def _safe_ascii_url(url: object) -> str:
    if not isinstance(url, str) or not 1 <= len(url) <= MAX_URL_CHARS:
        raise LightningRawCapacityError("provider-URL valt buiten de grens")
    if not url.isascii() or any(ord(char) < 0x21 or ord(char) == 0x7f for char in url):
        raise LightningRawContractError("provider-URL is niet canoniek ASCII")
    return url


def _request_binding(
    url: str,
    *,
    endpoint_host: str,
    endpoint_path_class: str,
    access_class: str,
    method: str = "GET",
    body_sha256: str | None = None,
) -> _RequestBinding:
    url = _safe_ascii_url(url)
    descriptor = {
        "accept": "*/*",
        "accept_encoding": "identity",
        "access_class": access_class,
        "body_sha256": body_sha256,
        "contract": "stv3-lightning-request-descriptor-v1",
        "endpoint_host": endpoint_host,
        "endpoint_path_class": endpoint_path_class,
        "method": method,
        "url_sha256": hashlib.sha256(url.encode("ascii")).hexdigest(),
    }
    return _RequestBinding(
        url=url,
        request_sha256=_digest(descriptor),
        endpoint_host=endpoint_host,
        endpoint_path_class=endpoint_path_class,
        access_class=access_class,
    )


def _eumetsat_search_binding() -> _RequestBinding:
    params = {
        "c": "4",
        "format": "json",
        "pi": EUMETSAT_COLLECTION_ID,
        "si": "0",
    }
    url = (
        f"https://{EUMETSAT_SEARCH_HOST}{EUMETSAT_SEARCH_PATH}?"
        + urlencode(params)
    )
    parsed = urlsplit(url)
    if parsed.path != EUMETSAT_SEARCH_PATH or dict(parse_qsl(parsed.query)) != params:
        raise AssertionError("vaste EUMETSAT searchbinding is ongeldig")
    return _request_binding(
        url,
        endpoint_host=EUMETSAT_SEARCH_HOST,
        endpoint_path_class=EUMETSAT_SEARCH_PATH,
        access_class="public_anonymous_no_secret",
    )


def _validate_eumetsat_download_url(url: object) -> str:
    value = _safe_ascii_url(url)
    if _INVALID_PERCENT_RE.search(value):
        raise LightningRawContractError("EUMETSAT downloadlink bevat ongeldige escaping")
    parsed = urlsplit(value)
    try:
        port = parsed.port
    except ValueError as err:
        raise LightningRawContractError("EUMETSAT downloadpoort is ongeldig") from err
    if (
        parsed.scheme != "https"
        or parsed.hostname != EUMETSAT_SEARCH_HOST
        or port not in {None, 443}
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
        or not parsed.path.startswith(EUMETSAT_DOWNLOAD_PATH_PREFIX)
        or "//" in parsed.path
    ):
        raise LightningRawContractError(
            "EUMETSAT downloadlink ontsnapt uit de HTTPS-allowlist"
        )
    try:
        pairs = parse_qsl(
            parsed.query,
            keep_blank_values=True,
            strict_parsing=True,
            max_num_fields=16,
        ) if parsed.query else []
    except ValueError as err:
        raise LightningRawContractError("EUMETSAT downloadquery is ongeldig") from err
    for name, item in pairs:
        lowered = name.casefold()
        if (
            _SAFE_QUERY_NAME_RE.fullmatch(name) is None
            or len(item) > 2048
            or any(marker in lowered for marker in _SECRET_QUERY_MARKERS)
        ):
            raise LightningRawContractError(
                "EUMETSAT downloadquery valt buiten de allowlist"
            )
    named_entries = tuple(item for name, item in pairs if name == "name")
    if pairs:
        if len(pairs) != 1 or len(named_entries) != 1 or not named_entries[0].endswith(
            ".nc"
        ):
            raise LightningRawContractError(
                "EUMETSAT downloadquery bindt geen exact NetCDF-entry"
            )
    elif not parsed.path.endswith(".nc"):
        raise LightningRawContractError(
            "EUMETSAT downloadpad bindt geen exact NetCDF-entry"
        )
    return value


def _eumetsat_product_binding(url: str) -> _RequestBinding:
    url = _validate_eumetsat_download_url(url)
    return _request_binding(
        url,
        endpoint_host=EUMETSAT_SEARCH_HOST,
        endpoint_path_class=EUMETSAT_DOWNLOAD_PATH_PREFIX + "authorized_netcdf_entry",
        access_class="oauth_bearer_ephemeral_child_only",
    )


def _goes_host(satellite: int) -> str:
    if satellite not in {18, 19}:
        raise LightningRawContractError("GOES-satelliet moet 18 of 19 zijn")
    return f"noaa-goes{satellite}.s3.amazonaws.com"


def _goes_prefix(moment: datetime) -> str:
    utc = moment.astimezone(timezone.utc)
    return (
        f"{GOES_PREFIX}/{utc.year}/{utc.timetuple().tm_yday:03d}/"
        f"{utc.hour:02d}/"
    )


def _goes_listing_binding(satellite: int, prefix: str) -> _RequestBinding:
    if _GOES_PREFIX_RE.fullmatch(prefix) is None:
        raise LightningRawContractError("GOES listingprefix is ongeldig")
    host = _goes_host(satellite)
    params = {
        "list-type": "2",
        "max-keys": str(GOES_LIST_MAX_KEYS),
        "prefix": prefix,
    }
    url = f"https://{host}/?" + urlencode(params)
    parsed = urlsplit(url)
    if parsed.path != "/" or dict(parse_qsl(parsed.query)) != params:
        raise AssertionError("vaste GOES listingbinding is ongeldig")
    return _request_binding(
        url,
        endpoint_host=host,
        endpoint_path_class="s3_listobjects_v2",
        access_class="public_anonymous_no_secret",
    )


def _goes_object_binding(satellite: int, key: str) -> _RequestBinding:
    parsed_key = _parse_goes_key(key, satellite=satellite)
    host = _goes_host(satellite)
    encoded = quote(parsed_key.key, safe="/ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._-")
    url = f"https://{host}/{encoded}"
    parsed = urlsplit(url)
    if (
        parsed.scheme != "https"
        or parsed.hostname != host
        or parsed.port not in {None, 443}
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or parsed.path != "/" + parsed_key.key
    ):
        raise LightningRawContractError("GOES object-URL ontsnapt uit de allowlist")
    return _request_binding(
        url,
        endpoint_host=host,
        endpoint_path_class=f"/{GOES_PREFIX}/native_netcdf",
        access_class="public_anonymous_no_secret",
    )


def _header_entries(headers: object) -> tuple[tuple[str, str], ...]:
    items = getattr(headers, "items", None)
    if not callable(items):
        raise LightningRawContractError("responseheaders ontbreken")
    result: list[tuple[str, str]] = []
    total = 0
    try:
        raw_entries = tuple(items())
    except Exception:
        raise LightningRawContractError("responseheaders zijn ongeldig") from None
    if len(raw_entries) > MAX_HTTP_FIELDS:
        raise LightningRawCapacityError("response bevat te veel headers")
    for raw_name, raw_value in raw_entries:
        name = str(raw_name)
        value = str(raw_value)
        if _HEADER_NAME_RE.fullmatch(name) is None or any(
            ord(char) < 0x20 or ord(char) == 0x7f for char in value
        ):
            raise LightningRawContractError("responseheader is ongeldig")
        total += len(name) + len(value) + 4
        if total > MAX_HEADER_CHARS:
            raise LightningRawCapacityError("responseheaders overschrijden de grens")
        result.append((name.casefold(), value.strip()))
    for singleton in ("content-length", "content-encoding"):
        if sum(name == singleton for name, _ in result) > 1:
            raise LightningRawContractError("response bevat dubbele transportframing")
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
        raise LightningRawContractError("Content-Length is ongeldig")
    result = int(value)
    if result > maximum:
        raise LightningRawCapacityError("Content-Length overschrijdt de grens")
    return result


def _reported_media_type(entries: tuple[tuple[str, str], ...]) -> str:
    values = _header_values(entries, "content-type")
    if len(values) != 1:
        return "unverified"
    media_type = values[0].split(";", 1)[0].strip().casefold()
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
                    raise LightningRawTransportError(
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
    # ``HTTPResponse.read1()`` may return a single small transport fragment even
    # when more bytes are immediately available.  The chunk-count bound below
    # assumes reads are filled up to the requested size (or reach EOF), which is
    # the contract provided by ``HTTPResponse.read(size)``.  Prefer that method
    # and retain ``read1`` only as a compatibility fallback for custom streams.
    reader = getattr(response, "read", None)
    if not callable(reader):
        reader = getattr(response, "read1", None)
    if not callable(reader):
        raise LightningRawContractError("response heeft geen bytestream")
    chunks: list[bytes] = []
    total = 0
    maximum_chunks = (maximum // HTTP_READ_CHUNK_BYTES) + 2
    for _index in range(maximum_chunks):
        _tighten_stream_timeout(
            response, timeout=min(HTTP_IO_TIMEOUT_SECONDS, _remaining(deadline))
        )
        requested = min(HTTP_READ_CHUNK_BYTES, maximum + 1 - total)
        try:
            raw = reader(requested)
        except TimeoutError:
            raise TimeoutError("lightning response-read is verlopen") from None
        except Exception:
            raise LightningRawTransportError(
                "lightning response-read is mislukt"
            ) from None
        if not raw:
            break
        if not isinstance(raw, (bytes, bytearray, memoryview)):
            raise LightningRawContractError("responsstream leverde geen bytes")
        chunk = bytes(raw)
        if len(chunk) > requested:
            raise LightningRawCapacityError("responsechunk overschrijdt de grens")
        chunks.append(chunk)
        total += len(chunk)
        if total > maximum or (
            declared_length is not None and total > declared_length
        ):
            raise LightningRawCapacityError("response overschrijdt de bytegrens")
        if declared_length is not None and total == declared_length:
            break
    else:
        raise LightningRawCapacityError("response overschrijdt de chunkgrens")
    payload = b"".join(chunks)
    if declared_length is not None and len(payload) != declared_length:
        raise LightningRawContractError(
            "responslengte wijkt af van Content-Length"
        )
    return payload


def _fetch_exact(
    binding: _RequestBinding,
    *,
    maximum_bytes: int,
    deadline: float,
    opener: Callable[..., Any] | None,
    authorization: str | None = None,
    method: str = "GET",
    body: bytes | None = None,
    request_content_type: str | None = None,
) -> _HttpResponse:
    headers = {
        "Accept": "*/*",
        "Accept-Encoding": "identity",
        "User-Agent": "Storm-Tracker-Worker/Lightning-ExactRaw-v1",
    }
    if authorization is not None:
        if not authorization.startswith(("Bearer ", "Basic ")):
            raise LightningRawContractError("authorizationtype is ongeldig")
        headers["Authorization"] = authorization
    if request_content_type is not None:
        headers["Content-Type"] = request_content_type
    request = Request(
        binding.url,
        data=body,
        headers=headers,
        method=method,
    )
    open_call = opener or _open_without_redirects
    try:
        response = open_call(
            request,
            timeout=min(HTTP_IO_TIMEOUT_SECONDS, _remaining(deadline)),
        )
    except (LightningRawError, TimeoutError):
        raise
    except Exception:
        raise LightningRawTransportError(
            "lightning providertransport kon geen response openen"
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
            raise LightningRawContractError(
                "providerantwoord is geen succesvolle HTTP-response"
            )
        final_url = str(getattr(response, "geturl", lambda: binding.url)())
        if final_url != binding.url:
            raise LightningRawContractError(
                "providerantwoord wijkt af van de exacte request-URL"
            )
        parsed = urlsplit(final_url)
        if (
            parsed.scheme != "https"
            or parsed.hostname != binding.endpoint_host
            or parsed.port not in {None, 443}
            or parsed.username is not None
            or parsed.password is not None
            or parsed.fragment
        ):
            raise LightningRawContractError(
                "providerantwoord ontsnapt uit de HTTPS-allowlist"
            )
        entries = _header_entries(getattr(response, "headers", None))
        encodings = _header_values(entries, "content-encoding")
        if encodings and tuple(item.casefold() for item in encodings) != ("identity",):
            raise LightningRawContractError(
                "response gebruikt geen exacte identity-representatie"
            )
        declared = _declared_length(entries, maximum_bytes)
        payload = _read_body(
            response,
            declared_length=declared,
            maximum=maximum_bytes,
            deadline=deadline,
        )
        received_at = time.time()
        if not math.isfinite(received_at) or received_at < 0:
            raise LightningRawContractError("ontvangstklok is ongeldig")
        safe_entries = sorted(
            item for item in entries if item[0] not in _SENSITIVE_RESPONSE_HEADERS
        )
        headers_sha256 = hashlib.sha256(
            json.dumps(
                safe_entries, ensure_ascii=True, allow_nan=False,
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
            "transport_contract": TRANSPORT_CONTRACT,
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
    except (LightningRawError, TimeoutError):
        raise
    except Exception:
        raise LightningRawContractError(
            "lightning provider-responsemetadata is ongeldig"
        ) from None
    finally:
        closer = getattr(response, "close", None)
        if callable(closer):
            try:
                closer()
            except Exception:
                pass


def mint_eumetsat_access_token_parent(
    consumer_key: str,
    consumer_secret: str,
    *,
    opener: Callable[..., Any] | None = None,
    deadline_at: float | None = None,
) -> EphemeralBearerToken:
    """Mint one non-serializable access token from parent-owned credentials.

    The OAuth response is authentication material, not meteorological source
    data.  It is bounded and parsed in memory but is intentionally never sent
    to ``RawArtifactStore``.
    """

    for value, label in (
        (consumer_key, "consumer key"),
        (consumer_secret, "consumer secret"),
    ):
        if (
            not isinstance(value, str)
            or not value
            or len(value) > 4096
            or "\x00" in value
        ):
            raise EumetsatOAuthError(f"EUMETSAT {label} ontbreekt of is ongeldig")
    try:
        basic = base64.b64encode(
            f"{consumer_key}:{consumer_secret}".encode("utf-8")
        ).decode("ascii")
    except UnicodeError as err:
        raise EumetsatOAuthError("EUMETSAT clientreferentie is ongeldig") from err
    body = b"grant_type=client_credentials&validity_period=3600"
    binding = _request_binding(
        f"https://{EUMETSAT_SEARCH_HOST}{EUMETSAT_TOKEN_PATH}",
        endpoint_host=EUMETSAT_SEARCH_HOST,
        endpoint_path_class=EUMETSAT_TOKEN_PATH,
        access_class="oauth_client_credentials_parent_only",
        method="POST",
        body_sha256=hashlib.sha256(body).hexdigest(),
    )
    response = _fetch_exact(
        binding,
        maximum_bytes=EUMETSAT_TOKEN_MAX_BYTES,
        deadline=_deadline(deadline_at),
        opener=opener,
        authorization="Basic " + basic,
        method="POST",
        body=body,
        request_content_type="application/x-www-form-urlencoded",
    )
    try:
        document = json.loads(response.payload.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as err:
        raise EumetsatOAuthError("EUMETSAT OAuth-response is ongeldig") from err
    if type(document) is not dict or len(document) > 32:
        raise EumetsatOAuthError("EUMETSAT OAuth-response is onbegrensd")
    token_value = document.get("access_token")
    try:
        return EphemeralBearerToken(token_value)
    except EumetsatOAuthError:
        raise
    finally:
        document = None
        token_value = None
        response = None
        basic = ""


@contextmanager
def eumetsat_parent_token_context(
    consumer_key: str,
    consumer_secret: str,
    *,
    opener: Callable[..., Any] | None = None,
    deadline_at: float | None = None,
) -> Iterator[EphemeralBearerToken]:
    token = mint_eumetsat_access_token_parent(
        consumer_key,
        consumer_secret,
        opener=opener,
        deadline_at=deadline_at,
    )
    try:
        yield token
    finally:
        token.close()


def _provenance(
    *,
    provider: str,
    role: str,
    provenance_kind: str,
    binding: _RequestBinding,
    response: _HttpResponse,
    locator_sha256: str,
) -> dict[str, Any]:
    return {
        "access_class": binding.access_class,
        "authentication_material_persisted": False,
        "capture_stage": NATIVE_CAPTURE_STAGE,
        "collector": "storm_tracker_lightning_exact_raw",
        "collector_version": "lightning-exact-raw-v1",
        "content_encoding": "identity",
        "declared_length": response.declared_length,
        "endpoint_host": binding.endpoint_host,
        "endpoint_path_class": binding.endpoint_path_class,
        "final_url_sha256": response.final_url_sha256,
        "legal_advice": False,
        "locator_sha256": locator_sha256,
        "meteorological_parse_attempted_before_store": False,
        "operational_admission": False,
        "provider": provider,
        "provider_rights_claim": False,
        "provenance_kind": provenance_kind,
        "reported_media_type": response.reported_media_type,
        "representation": NATIVE_REPRESENTATION,
        "request_sha256": binding.request_sha256,
        "response_headers_sha256": response.response_headers_sha256,
        "response_status": response.status,
        "retention_right_claim": False,
        "role": role,
        "semantic_status": "unverified",
        "transport_sha256": response.transport_sha256,
        "weather_state": "unknown",
    }


def _upstream_id(
    *,
    product: str,
    role: str,
    locator_sha256: str,
    request_sha256: str,
    transport_sha256: str,
    content_sha256: str,
) -> str:
    return (
        f"{product}|role={role}|locator={locator_sha256}"
        f"|request={request_sha256}|transport={transport_sha256}"
        f"|content={content_sha256}"
    )


def _store_response(
    store: RawArtifactStore,
    *,
    job_id: str,
    provider: str,
    product: str,
    role: str,
    provenance_kind: str,
    binding: _RequestBinding,
    response: _HttpResponse,
    locator_sha256: str,
) -> RawArtifactReceipt:
    content_sha256 = hashlib.sha256(response.payload).hexdigest()
    return store.store(
        provider=provider,
        product=product,
        fetch_id=f"{job_id}:{role}",
        content_type=UNINTERPRETED_CONTENT_TYPE,
        received_at=response.received_at,
        upstream_id=_upstream_id(
            product=product,
            role=role,
            locator_sha256=locator_sha256,
            request_sha256=binding.request_sha256,
            transport_sha256=response.transport_sha256,
            content_sha256=content_sha256,
        ),
        provenance=_provenance(
            provider=provider,
            role=role,
            provenance_kind=provenance_kind,
            binding=binding,
            response=response,
            locator_sha256=locator_sha256,
        ),
        payload=response.payload,
    )


def _receipt_id(
    store: RawArtifactStore, provider: str, product: str, job_id: str, role: str
) -> str:
    return store._receipt_id(  # noqa: SLF001 - schema-3 identity contract
        provider=provider,
        product=product,
        fetch_id=f"{job_id}:{role}",
    )


def _capture_id(
    store: RawArtifactStore, provider: str, product: str, job_id: str
) -> str:
    return store._capture_id(  # noqa: SLF001 - schema-3 identity contract
        provider=provider,
        product=product,
        fetch_id=f"{job_id}:capture",
    )


def _validate_receipt(
    receipt: RawArtifactReceipt,
    *,
    job_id: str,
    provider: str,
    product: str,
    role: str,
    provenance_kind: str,
    binding: _RequestBinding,
    locator_sha256: str,
    maximum_bytes: int,
) -> None:
    provenance = receipt.provenance
    fixed = {
        "access_class": binding.access_class,
        "authentication_material_persisted": False,
        "capture_stage": NATIVE_CAPTURE_STAGE,
        "collector": "storm_tracker_lightning_exact_raw",
        "collector_version": "lightning-exact-raw-v1",
        "content_encoding": "identity",
        "endpoint_host": binding.endpoint_host,
        "endpoint_path_class": binding.endpoint_path_class,
        "legal_advice": False,
        "locator_sha256": locator_sha256,
        "meteorological_parse_attempted_before_store": False,
        "operational_admission": False,
        "provider": provider,
        "provider_rights_claim": False,
        "provenance_kind": provenance_kind,
        "representation": NATIVE_REPRESENTATION,
        "request_sha256": binding.request_sha256,
        "retention_right_claim": False,
        "role": role,
        "semantic_status": "unverified",
        "weather_state": "unknown",
    }
    if any(provenance.get(key) != value for key, value in fixed.items()):
        raise LightningRawCorruptionError("duurzame lightningprovenance wijkt af")
    for key in (
        "final_url_sha256", "response_headers_sha256", "transport_sha256",
    ):
        if not isinstance(provenance.get(key), str) or _SHA256_RE.fullmatch(
            provenance[key]
        ) is None:
            raise LightningRawCorruptionError("duurzame lightninghash wijkt af")
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
                or not 0 <= declared <= maximum_bytes
            )
        )
        or not isinstance(media_type, str)
        or not 1 <= len(media_type) <= 256
        or receipt.size_bytes > maximum_bytes
        or (declared is not None and declared != receipt.size_bytes)
    ):
        raise LightningRawCorruptionError("duurzame transportmetadata wijkt af")
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
        "transport_contract": TRANSPORT_CONTRACT,
    }
    transport_sha256 = _digest(transport)
    if (
        provenance["transport_sha256"] != transport_sha256
        or receipt.provider != provider
        or receipt.product != product
        or receipt.fetch_id != f"{job_id}:{role}"
        or receipt.content_type != UNINTERPRETED_CONTENT_TYPE
        or receipt.upstream_id != _upstream_id(
            product=product,
            role=role,
            locator_sha256=locator_sha256,
            request_sha256=binding.request_sha256,
            transport_sha256=transport_sha256,
            content_sha256=receipt.content_sha256,
        )
    ):
        raise LightningRawCorruptionError("duurzame lightningreceipt wijkt af")


def _load_receipt(
    store: RawArtifactStore,
    *,
    job_id: str,
    provider: str,
    product: str,
    role: str,
) -> tuple[RawArtifactReceipt, bytes] | None:
    return store.load_receipt_bytes(
        _receipt_id(store, provider, product, job_id, role)
    )


def _members_digest(receipts: Mapping[str, RawArtifactReceipt]) -> str:
    return _digest({
        "contract": "stv3-lightning-capture-member-set-v1",
        "members": [
            {
                "content_sha256": receipts[role].content_sha256,
                "receipt_sha256": receipts[role].receipt_sha256,
                "role": role,
            }
            for role in sorted(receipts)
        ],
    })


def _capture_upstream_id(
    capture_product: str, receipts: Mapping[str, RawArtifactReceipt]
) -> str:
    return f"{capture_product}|members_sha256={_members_digest(receipts)}"


def _validate_capture(
    capture: RawArtifactCaptureReceipt,
    *,
    job_id: str,
    provider: str,
    capture_product: str,
    receipts: Mapping[str, RawArtifactReceipt],
) -> None:
    members = {item.role: item.receipt_id for item in capture.members}
    expected = {role: receipt.receipt_id for role, receipt in receipts.items()}
    if (
        capture.raw_store_schema_version != RAW_ARTIFACT_SCHEMA_VERSION
        or capture.provider != provider
        or capture.product != capture_product
        or capture.fetch_id != f"{job_id}:capture"
        or capture.upstream_capture_id
        != _capture_upstream_id(capture_product, receipts)
        or capture.product_timestamp is not None
        or members != expected
    ):
        raise LightningRawCorruptionError("duurzame lightningcapture wijkt af")


def _commit_capture(
    store: RawArtifactStore,
    *,
    job_id: str,
    provider: str,
    capture_product: str,
    receipts: Mapping[str, RawArtifactReceipt],
) -> RawArtifactCaptureReceipt:
    capture = store.commit_capture(
        provider=provider,
        product=capture_product,
        fetch_id=f"{job_id}:capture",
        upstream_capture_id=_capture_upstream_id(capture_product, receipts),
        product_timestamp=None,
        members={role: receipt.receipt_id for role, receipt in receipts.items()},
    )
    _validate_capture(
        capture,
        job_id=job_id,
        provider=provider,
        capture_product=capture_product,
        receipts=receipts,
    )
    return capture


def _result(
    spec: LightningHandlerSpec,
    capture: RawArtifactCaptureReceipt,
    receipts: Mapping[str, RawArtifactReceipt],
) -> dict[str, Any]:
    return {
        "capture_state": "captured_not_decoded",
        "contract": RESULT_CONTRACT,
        "decode_status": "not_decoded",
        "handler_id": spec.handler_id,
        "meteorological_claim": False,
        "meteorological_parse_attempted": False,
        "operational_admission_eligible": False,
        "provider": spec.provider,
        "provider_rights_claim": False,
        "selection_authority_parse_attempted": True,
        "semantic_status": "unverified",
        "weather_state": "unknown",
        "workload_id": spec.workload_id,
        "raw_provenance": {
            "raw_capture_id": capture.capture_id,
            "raw_capture_manifest_sha256": capture.manifest_sha256,
            "raw_capture_receipt_sha256": capture.capture_receipt_sha256,
            "raw_member_count": len(receipts),
            "raw_store_schema_version": RAW_ARTIFACT_SCHEMA_VERSION,
            "sources": {
                role: {
                    "content_sha256": receipts[role].content_sha256,
                    "receipt_id": receipts[role].receipt_id,
                    "receipt_sha256": receipts[role].receipt_sha256,
                    "role": role,
                    "size_bytes": receipts[role].size_bytes,
                }
                for role in sorted(receipts)
            },
        },
    }


def _parse_eumetsat_selection(payload: bytes) -> _EumetsatSelection:
    try:
        document = json.loads(payload.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as err:
        raise LightningRawContractError(
            "duurzame EUMETSAT search-response is geen begrensde JSON"
        ) from err
    if type(document) is not dict or len(document) > 128:
        raise LightningRawCapacityError("EUMETSAT searchdocument is onbegrensd")
    features = document.get("features")
    if type(features) is not list or len(features) > EUMETSAT_MAX_FEATURES:
        raise LightningRawCapacityError("EUMETSAT featurelijst is onbegrensd")
    candidates: list[tuple[int, int, _EumetsatSelection]] = []
    for feature_index, feature in enumerate(features):
        if type(feature) is not dict:
            continue
        product_id = feature.get("id")
        properties = feature.get("properties")
        if (
            not isinstance(product_id, str)
            or _SAFE_PRODUCT_ID_RE.fullmatch(product_id) is None
            or type(properties) is not dict
        ):
            continue
        links = properties.get("links")
        entries = links.get("sip-entries") if type(links) is dict else None
        if type(entries) is not list:
            continue
        if len(entries) > EUMETSAT_MAX_ENTRIES_PER_FEATURE:
            raise LightningRawCapacityError("EUMETSAT entrylijst is onbegrensd")
        for entry_index, entry in enumerate(entries):
            if type(entry) is not dict:
                continue
            title = entry.get("title")
            href = entry.get("href")
            if (
                not isinstance(title, str)
                or _SAFE_TITLE_RE.fullmatch(title) is None
                or "CHK-BODY" not in title
                or not title.endswith(".nc")
                or not isinstance(href, str)
            ):
                continue
            try:
                url = _validate_eumetsat_download_url(href)
            except LightningRawError:
                continue
            parsed = urlsplit(url)
            names = tuple(
                value for name, value in parse_qsl(parsed.query) if name == "name"
            )
            if (
                names and names != (title,)
            ) or (
                not names and parsed.path.rsplit("/", 1)[-1] != title
            ):
                continue
            candidates.append((
                feature_index,
                entry_index,
                _EumetsatSelection(
                    product_id=product_id,
                    title=title,
                    url=url,
                    url_sha256=hashlib.sha256(url.encode("ascii")).hexdigest(),
                ),
            ))
    if not candidates:
        raise LightningRawContractError(
            "EUMETSAT search-response bevat geen toegestane NetCDF BODY-link"
        )
    candidates.sort(key=lambda item: (item[0], item[1]))
    return candidates[0][2]


def _eumetsat_locator(role: str, selection: _EumetsatSelection | None) -> str:
    if role == "search_response":
        return _digest({
            "collection_id": EUMETSAT_COLLECTION_ID,
            "role": role,
            "search_path": EUMETSAT_SEARCH_PATH,
        })
    if role == "netcdf4" and selection is not None:
        return _digest({
            "product_id": selection.product_id,
            "role": role,
            "title": selection.title,
            "url_sha256": selection.url_sha256,
        })
    raise LightningRawContractError("EUMETSAT locatorrol is ongeldig")


def _validate_eumetsat_bundle(
    job_id: str,
    capture: RawArtifactCaptureReceipt,
    receipts: Mapping[str, RawArtifactReceipt],
    payloads: Mapping[str, bytes],
) -> _EumetsatSelection:
    if set(receipts) != {"search_response", "netcdf4"} or set(payloads) != set(receipts):
        raise LightningRawCorruptionError("EUMETSAT capture mist verplichte rollen")
    search_binding = _eumetsat_search_binding()
    search_locator = _eumetsat_locator("search_response", None)
    _validate_receipt(
        receipts["search_response"],
        job_id=job_id,
        provider=EUMETSAT_PROVIDER,
        product=EUMETSAT_SEARCH_PRODUCT,
        role="search_response",
        provenance_kind="eumetsat_mtg_li_search_v1",
        binding=search_binding,
        locator_sha256=search_locator,
        maximum_bytes=EUMETSAT_SEARCH_MAX_BYTES,
    )
    selection = _parse_eumetsat_selection(payloads["search_response"])
    body_binding = _eumetsat_product_binding(selection.url)
    _validate_receipt(
        receipts["netcdf4"],
        job_id=job_id,
        provider=EUMETSAT_PROVIDER,
        product=EUMETSAT_NETCDF_PRODUCT,
        role="netcdf4",
        provenance_kind="eumetsat_mtg_li_netcdf_v1",
        binding=body_binding,
        locator_sha256=_eumetsat_locator("netcdf4", selection),
        maximum_bytes=EUMETSAT_NETCDF_MAX_BYTES,
    )
    _validate_capture(
        capture,
        job_id=job_id,
        provider=EUMETSAT_PROVIDER,
        capture_product=EUMETSAT_CAPTURE_PRODUCT,
        receipts=receipts,
    )
    return selection


def capture_eumetsat_mtg_li_raw(
    job: object,
    *,
    raw_store: RawArtifactStore,
    access_token_supplier: Callable[[], EphemeralBearerToken] | None = None,
    opener: Callable[..., Any] | None = None,
    deadline_at: float | None = None,
    _fault_injector: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    """Capture EUMETSAT search plus selected NetCDF without decoding it."""

    job_id = _job(job)
    if not isinstance(raw_store, RawArtifactStore):
        raise LightningRawContractError("raw_store heeft het verkeerde type")
    raw_store.initialize()
    if raw_store.limits.max_object_bytes < EUMETSAT_NETCDF_MAX_BYTES:
        raise LightningRawCapacityError(
            "raw store objectgrens is kleiner dan EUMETSAT transportbudget"
        )
    deadline = _deadline(deadline_at)
    spec = HANDLER_SPECS_BY_ID[EUMETSAT_HANDLER_ID]
    with _CAPTURE_LOCK:
        complete = raw_store.load_capture_bundle_bytes(
            _capture_id(
                raw_store, EUMETSAT_PROVIDER, EUMETSAT_CAPTURE_PRODUCT, job_id
            )
        )
        if complete is not None:
            capture, receipts, payloads = complete
            _validate_eumetsat_bundle(job_id, capture, receipts, payloads)
            return _result(spec, capture, receipts)

        receipts: dict[str, RawArtifactReceipt] = {}
        search_binding = _eumetsat_search_binding()
        search_locator = _eumetsat_locator("search_response", None)
        loaded_search = _load_receipt(
            raw_store,
            job_id=job_id,
            provider=EUMETSAT_PROVIDER,
            product=EUMETSAT_SEARCH_PRODUCT,
            role="search_response",
        )
        if loaded_search is None:
            response = _fetch_exact(
                search_binding,
                maximum_bytes=EUMETSAT_SEARCH_MAX_BYTES,
                deadline=deadline,
                opener=opener,
            )
            search_receipt = _store_response(
                raw_store,
                job_id=job_id,
                provider=EUMETSAT_PROVIDER,
                product=EUMETSAT_SEARCH_PRODUCT,
                role="search_response",
                provenance_kind="eumetsat_mtg_li_search_v1",
                binding=search_binding,
                response=response,
                locator_sha256=search_locator,
            )
            search_payload = response.payload
            if _fault_injector is not None:
                _fault_injector("eumetsat.after_search_store")
        else:
            search_receipt, search_payload = loaded_search
        _validate_receipt(
            search_receipt,
            job_id=job_id,
            provider=EUMETSAT_PROVIDER,
            product=EUMETSAT_SEARCH_PRODUCT,
            role="search_response",
            provenance_kind="eumetsat_mtg_li_search_v1",
            binding=search_binding,
            locator_sha256=search_locator,
            maximum_bytes=EUMETSAT_SEARCH_MAX_BYTES,
        )
        receipts["search_response"] = search_receipt

        # Selection happens only after the exact search entity is durable.
        selection = _parse_eumetsat_selection(search_payload)
        body_binding = _eumetsat_product_binding(selection.url)
        body_locator = _eumetsat_locator("netcdf4", selection)
        loaded_body = _load_receipt(
            raw_store,
            job_id=job_id,
            provider=EUMETSAT_PROVIDER,
            product=EUMETSAT_NETCDF_PRODUCT,
            role="netcdf4",
        )
        if loaded_body is None:
            if not callable(access_token_supplier):
                raise EumetsatOAuthError(
                    "EUMETSAT vereist een parent-gebrokerd ephemeral access-token"
                )
            token = access_token_supplier()
            if not isinstance(token, EphemeralBearerToken):
                raise EumetsatOAuthError(
                    "EUMETSAT tokenleverancier gaf geen ephemeral token"
                )
            try:
                response = _fetch_exact(
                    body_binding,
                    maximum_bytes=EUMETSAT_NETCDF_MAX_BYTES,
                    deadline=deadline,
                    opener=opener,
                    authorization=token.authorization_value(),
                )
            finally:
                token.close()
            body_receipt = _store_response(
                raw_store,
                job_id=job_id,
                provider=EUMETSAT_PROVIDER,
                product=EUMETSAT_NETCDF_PRODUCT,
                role="netcdf4",
                provenance_kind="eumetsat_mtg_li_netcdf_v1",
                binding=body_binding,
                response=response,
                locator_sha256=body_locator,
            )
            body_payload = response.payload
            if _fault_injector is not None:
                _fault_injector("eumetsat.after_netcdf_store")
        else:
            body_receipt, body_payload = loaded_body
        _validate_receipt(
            body_receipt,
            job_id=job_id,
            provider=EUMETSAT_PROVIDER,
            product=EUMETSAT_NETCDF_PRODUCT,
            role="netcdf4",
            provenance_kind="eumetsat_mtg_li_netcdf_v1",
            binding=body_binding,
            locator_sha256=body_locator,
            maximum_bytes=EUMETSAT_NETCDF_MAX_BYTES,
        )
        receipts["netcdf4"] = body_receipt
        if body_payload is None:  # pragma: no cover - store return contract
            raise LightningRawCorruptionError("EUMETSAT NetCDF-bytes ontbreken")
        if _fault_injector is not None:
            _fault_injector("eumetsat.before_capture_commit")
        capture = _commit_capture(
            raw_store,
            job_id=job_id,
            provider=EUMETSAT_PROVIDER,
            capture_product=EUMETSAT_CAPTURE_PRODUCT,
            receipts=receipts,
        )
        if _fault_injector is not None:
            _fault_injector("eumetsat.after_capture_commit")
        return _result(spec, capture, receipts)


def _parse_goes_key(key: object, *, satellite: int) -> _GoesObject:
    if not isinstance(key, str) or not key.isascii() or len(key) > 512:
        raise LightningRawContractError("GOES objectkey is ongeldig")
    match = _GOES_KEY_RE.fullmatch(key)
    if match is None or int(match.group("satellite")) != satellite:
        raise LightningRawContractError("GOES objectkey valt buiten het GLM-profiel")
    prefix = key.rsplit("/", 1)[0] + "/"
    if _GOES_PREFIX_RE.fullmatch(prefix) is None:
        raise LightningRawContractError("GOES objectprefix is ongeldig")
    return _GoesObject(
        satellite=satellite,
        key=key,
        key_sha256=hashlib.sha256(key.encode("ascii")).hexdigest(),
        start=match.group("start"),
        end=match.group("end"),
        created=match.group("created"),
    )


def _xml_local(tag: object) -> tuple[str, str]:
    if not isinstance(tag, str) or not tag.startswith("{") or "}" not in tag:
        raise LightningRawContractError("GOES listing bevat een vreemde namespace")
    namespace, local = tag[1:].split("}", 1)
    return namespace, local


def _parse_goes_listing(
    payload: bytes, *, satellite: int, expected_prefix: str
) -> tuple[_GoesObject, ...]:
    if _FORBIDDEN_XML.search(payload):
        raise LightningRawContractError("GOES listing bevat verboden XML-declaraties")
    try:
        root = ET.fromstring(payload)
    except ET.ParseError as err:
        raise LightningRawContractError("duurzame GOES listing-XML is ongeldig") from err
    namespace, local = _xml_local(root.tag)
    if namespace != GOES_S3_NAMESPACE or local != "ListBucketResult":
        raise LightningRawContractError("GOES listing heeft verkeerde root")
    nodes = tuple(root.iter())
    if len(nodes) > 8_192:
        raise LightningRawCapacityError("GOES listingstructuur is onbegrensd")
    children: dict[str, list[ET.Element]] = {}
    for child in root:
        child_namespace, child_local = _xml_local(child.tag)
        if child_namespace != GOES_S3_NAMESPACE:
            raise LightningRawContractError("GOES listingnamespace wijkt af")
        children.setdefault(child_local, []).append(child)
    prefixes = children.get("Prefix", [])
    buckets = children.get("Name", [])
    truncation = children.get("IsTruncated", [])
    if (
        len(prefixes) != 1
        or (prefixes[0].text or "") != expected_prefix
        or len(buckets) != 1
        or (buckets[0].text or "") != f"noaa-goes{satellite}"
        or len(truncation) != 1
        or (truncation[0].text or "").casefold() != "false"
    ):
        raise LightningRawContractError("GOES listingcontext wijkt af")
    contents = children.get("Contents", [])
    if len(contents) > GOES_LIST_MAX_KEYS:
        raise LightningRawCapacityError("GOES listing bevat te veel objecten")
    objects: list[_GoesObject] = []
    for content in contents:
        keys: list[str] = []
        for item in content:
            item_namespace, item_local = _xml_local(item.tag)
            if item_namespace != GOES_S3_NAMESPACE:
                raise LightningRawContractError("GOES objectnamespace wijkt af")
            if item_local == "Key" and item.text is not None:
                keys.append(item.text)
        if len(keys) != 1:
            continue
        try:
            candidate = _parse_goes_key(keys[0], satellite=satellite)
        except LightningRawContractError:
            continue
        if not candidate.key.startswith(expected_prefix):
            continue
        objects.append(candidate)
    return tuple(objects)


def _select_goes_objects(
    groups: tuple[tuple[_GoesObject, ...], ...]
) -> tuple[_GoesObject, ...]:
    by_key: dict[str, _GoesObject] = {}
    for group in groups:
        for item in group:
            existing = by_key.setdefault(item.key, item)
            if existing != item:
                raise LightningRawContractError("GOES objectidentiteit botst")
    ordered = sorted(
        by_key.values(),
        key=lambda item: (item.start, item.end, item.created, item.key),
    )
    selected = tuple(ordered[-GOES_MAX_SELECTED_OBJECTS:])
    if not selected:
        raise LightningRawContractError(
            "GOES listings bevatten geen selecteerbaar GLM NetCDF-object"
        )
    return selected


def _goes_listing_locator(satellite: int, role: str, prefix: str) -> str:
    return _digest({"prefix": prefix, "role": role, "satellite": satellite})


def _goes_object_locator(satellite: int, role: str, item: _GoesObject) -> str:
    return _digest({
        "key_sha256": item.key_sha256,
        "role": role,
        "satellite": satellite,
    })


def _goes_role(index: int) -> str:
    if not 0 <= index < GOES_MAX_SELECTED_OBJECTS:
        raise LightningRawContractError("GOES dynamische rolindex is ongeldig")
    return f"netcdf_{index:02d}"


def _prefix_from_listing_receipt(
    receipt: RawArtifactReceipt, *, satellite: int, role: str
) -> str:
    provenance = receipt.provenance
    prefix = provenance.get("selection_prefix")
    if not isinstance(prefix, str) or _GOES_PREFIX_RE.fullmatch(prefix) is None:
        raise LightningRawCorruptionError("GOES listingreceipt mist zijn prefix")
    if provenance.get("selection_satellite") != satellite or provenance.get(
        "role"
    ) != role:
        raise LightningRawCorruptionError("GOES listingbinding wijkt af")
    return prefix


def _listing_provenance(
    *,
    satellite: int,
    role: str,
    prefix: str,
    binding: _RequestBinding,
    response: _HttpResponse,
) -> dict[str, Any]:
    value = _provenance(
        provider=GOES_PROVIDER,
        role=role,
        provenance_kind="noaa_goes_glm_listing_v1",
        binding=binding,
        response=response,
        locator_sha256=_goes_listing_locator(satellite, role, prefix),
    )
    value["selection_prefix"] = prefix
    value["selection_satellite"] = satellite
    return value


def _validate_goes_listing_receipt(
    receipt: RawArtifactReceipt,
    *,
    job_id: str,
    satellite: int,
    role: str,
    prefix: str,
    binding: _RequestBinding,
) -> None:
    if (
        receipt.provenance.get("selection_prefix") != prefix
        or receipt.provenance.get("selection_satellite") != satellite
    ):
        raise LightningRawCorruptionError("GOES listingprovenance wijkt af")
    _validate_receipt(
        receipt,
        job_id=job_id,
        provider=GOES_PROVIDER,
        product=GOES_LISTING_PRODUCT,
        role=role,
        provenance_kind="noaa_goes_glm_listing_v1",
        binding=binding,
        locator_sha256=_goes_listing_locator(satellite, role, prefix),
        maximum_bytes=GOES_MAX_LISTING_BYTES,
    )


def _goes_object_provenance(
    *,
    satellite: int,
    role: str,
    item: _GoesObject,
    binding: _RequestBinding,
    response: _HttpResponse,
) -> dict[str, Any]:
    value = _provenance(
        provider=GOES_PROVIDER,
        role=role,
        provenance_kind="noaa_goes_glm_netcdf_v1",
        binding=binding,
        response=response,
        locator_sha256=_goes_object_locator(satellite, role, item),
    )
    value["object_key_sha256"] = item.key_sha256
    value["selection_satellite"] = satellite
    return value


def _validate_goes_object_receipt(
    receipt: RawArtifactReceipt,
    *,
    job_id: str,
    satellite: int,
    role: str,
    item: _GoesObject,
    binding: _RequestBinding,
) -> None:
    if (
        receipt.provenance.get("object_key_sha256") != item.key_sha256
        or receipt.provenance.get("selection_satellite") != satellite
    ):
        raise LightningRawCorruptionError("GOES objectprovenance wijkt af")
    _validate_receipt(
        receipt,
        job_id=job_id,
        provider=GOES_PROVIDER,
        product=GOES_NETCDF_PRODUCT,
        role=role,
        provenance_kind="noaa_goes_glm_netcdf_v1",
        binding=binding,
        locator_sha256=_goes_object_locator(satellite, role, item),
        maximum_bytes=GOES_MAX_NETCDF_BYTES,
    )


def _goes_spec(satellite: int) -> LightningHandlerSpec:
    return HANDLER_SPECS_BY_ID[
        GOES18_HANDLER_ID if satellite == 18 else GOES19_HANDLER_ID
    ]


def _validate_goes_complete_bundle(
    *,
    job_id: str,
    satellite: int,
    capture: RawArtifactCaptureReceipt,
    receipts: Mapping[str, RawArtifactReceipt],
    payloads: Mapping[str, bytes],
) -> None:
    if set(receipts) != set(payloads):
        raise LightningRawCorruptionError("GOES capturepayloadset wijkt af")
    listing_roles = ("listing_previous", "listing_current")
    if not set(listing_roles).issubset(receipts):
        raise LightningRawCorruptionError("GOES capture mist beide listings")
    previous_prefix = _prefix_from_listing_receipt(
        receipts["listing_previous"], satellite=satellite, role="listing_previous"
    )
    current_prefix = _prefix_from_listing_receipt(
        receipts["listing_current"], satellite=satellite, role="listing_current"
    )
    try:
        previous_time = datetime.strptime(
            previous_prefix.removeprefix(GOES_PREFIX + "/").rstrip("/"),
            "%Y/%j/%H",
        ).replace(tzinfo=timezone.utc)
    except ValueError as err:
        raise LightningRawCorruptionError(
            "GOES duurzame listingtijd is ongeldig"
        ) from err
    if current_prefix != _goes_prefix(previous_time + timedelta(hours=1)):
        raise LightningRawCorruptionError("GOES listinguren zijn niet aangrenzend")
    previous_binding = _goes_listing_binding(satellite, previous_prefix)
    current_binding = _goes_listing_binding(satellite, current_prefix)
    _validate_goes_listing_receipt(
        receipts["listing_previous"],
        job_id=job_id,
        satellite=satellite,
        role="listing_previous",
        prefix=previous_prefix,
        binding=previous_binding,
    )
    _validate_goes_listing_receipt(
        receipts["listing_current"],
        job_id=job_id,
        satellite=satellite,
        role="listing_current",
        prefix=current_prefix,
        binding=current_binding,
    )
    groups = (
        _parse_goes_listing(
            payloads["listing_previous"],
            satellite=satellite,
            expected_prefix=previous_prefix,
        ),
        _parse_goes_listing(
            payloads["listing_current"],
            satellite=satellite,
            expected_prefix=current_prefix,
        ),
    )
    selected = _select_goes_objects(groups)
    expected_roles = {*listing_roles, *(_goes_role(i) for i in range(len(selected)))}
    if set(receipts) != expected_roles:
        raise LightningRawCorruptionError("GOES dynamische capture-rollen wijken af")
    for index, item in enumerate(selected):
        role = _goes_role(index)
        binding = _goes_object_binding(satellite, item.key)
        _validate_goes_object_receipt(
            receipts[role],
            job_id=job_id,
            satellite=satellite,
            role=role,
            item=item,
            binding=binding,
        )
    _validate_capture(
        capture,
        job_id=job_id,
        provider=GOES_PROVIDER,
        capture_product=GOES_CAPTURE_PRODUCT,
        receipts=receipts,
    )


def capture_noaa_goes_glm_raw(
    job: object,
    *,
    satellite: int,
    raw_store: RawArtifactStore,
    now: datetime | None = None,
    opener: Callable[..., Any] | None = None,
    deadline_at: float | None = None,
    _fault_injector: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    """Capture two exact listings and up to twelve exact GLM objects."""

    job_id = _job(job)
    if isinstance(satellite, bool) or not isinstance(satellite, int):
        raise LightningRawContractError("GOES-satelliet moet exact integer zijn")
    _goes_host(satellite)
    if not isinstance(raw_store, RawArtifactStore):
        raise LightningRawContractError("raw_store heeft het verkeerde type")
    raw_store.initialize()
    if (
        raw_store.limits.max_object_bytes < GOES_MAX_NETCDF_BYTES
        or raw_store.limits.max_capture_members < 2 + GOES_MAX_SELECTED_OBJECTS
    ):
        raise LightningRawCapacityError("raw store is kleiner dan het GOES contract")
    deadline = _deadline(deadline_at)
    spec = _goes_spec(satellite)
    with _CAPTURE_LOCK:
        complete = raw_store.load_capture_bundle_bytes(
            _capture_id(raw_store, GOES_PROVIDER, GOES_CAPTURE_PRODUCT, job_id)
        )
        if complete is not None:
            capture, receipts, payloads = complete
            _validate_goes_complete_bundle(
                job_id=job_id,
                satellite=satellite,
                capture=capture,
                receipts=receipts,
                payloads=payloads,
            )
            return _result(spec, capture, receipts)

        listing_roles = ("listing_previous", "listing_current")
        loaded_listings = {
            role: _load_receipt(
                raw_store,
                job_id=job_id,
                provider=GOES_PROVIDER,
                product=GOES_LISTING_PRODUCT,
                role=role,
            )
            for role in listing_roles
        }
        if loaded_listings["listing_current"] is not None and loaded_listings[
            "listing_previous"
        ] is None:
            raise LightningRawCorruptionError(
                "GOES current-listing bestaat zonder previous-listing"
            )
        if loaded_listings["listing_previous"] is not None:
            previous_prefix = _prefix_from_listing_receipt(
                loaded_listings["listing_previous"][0],
                satellite=satellite,
                role="listing_previous",
            )
            try:
                prefix_time = datetime.strptime(
                    previous_prefix.removeprefix(GOES_PREFIX + "/").rstrip("/"),
                    "%Y/%j/%H",
                ).replace(tzinfo=timezone.utc)
            except ValueError as err:
                raise LightningRawCorruptionError(
                    "GOES duurzame listingtijd is ongeldig"
                ) from err
            current_prefix = _goes_prefix(prefix_time + timedelta(hours=1))
        else:
            if now is None:
                reference = datetime.now(timezone.utc)
            elif not isinstance(now, datetime) or now.tzinfo is None:
                raise LightningRawContractError("GOES referentietijd is ongeldig")
            else:
                reference = now.astimezone(timezone.utc)
            current_hour = reference.replace(minute=0, second=0, microsecond=0)
            previous_prefix = _goes_prefix(current_hour - timedelta(hours=1))
            current_prefix = _goes_prefix(current_hour)
        prefixes = (previous_prefix, current_prefix)

        receipts: dict[str, RawArtifactReceipt] = {}
        listing_payloads: dict[str, bytes] = {}
        for role, prefix in zip(listing_roles, prefixes, strict=True):
            binding = _goes_listing_binding(satellite, prefix)
            loaded = loaded_listings[role]
            if loaded is None:
                response = _fetch_exact(
                    binding,
                    maximum_bytes=GOES_MAX_LISTING_BYTES,
                    deadline=deadline,
                    opener=opener,
                )
                # GOES listing provenance adds only a non-secret prefix and
                # satellite to the generic exact transport evidence.
                content_sha256 = hashlib.sha256(response.payload).hexdigest()
                receipt = raw_store.store(
                    provider=GOES_PROVIDER,
                    product=GOES_LISTING_PRODUCT,
                    fetch_id=f"{job_id}:{role}",
                    content_type=UNINTERPRETED_CONTENT_TYPE,
                    received_at=response.received_at,
                    upstream_id=_upstream_id(
                        product=GOES_LISTING_PRODUCT,
                        role=role,
                        locator_sha256=_goes_listing_locator(satellite, role, prefix),
                        request_sha256=binding.request_sha256,
                        transport_sha256=response.transport_sha256,
                        content_sha256=content_sha256,
                    ),
                    provenance=_listing_provenance(
                        satellite=satellite,
                        role=role,
                        prefix=prefix,
                        binding=binding,
                        response=response,
                    ),
                    payload=response.payload,
                )
                payload = response.payload
                if _fault_injector is not None:
                    _fault_injector(f"goes.after_{role}_store")
            else:
                receipt, payload = loaded
            _validate_goes_listing_receipt(
                receipt,
                job_id=job_id,
                satellite=satellite,
                role=role,
                prefix=prefix,
                binding=binding,
            )
            receipts[role] = receipt
            listing_payloads[role] = payload
            _remaining(deadline)

        # Both exact XML responses are durable before this first parse.
        groups = tuple(
            _parse_goes_listing(
                listing_payloads[role],
                satellite=satellite,
                expected_prefix=prefix,
            )
            for role, prefix in zip(listing_roles, prefixes, strict=True)
        )
        selected = _select_goes_objects(groups)

        for index, item in enumerate(selected):
            role = _goes_role(index)
            binding = _goes_object_binding(satellite, item.key)
            loaded = _load_receipt(
                raw_store,
                job_id=job_id,
                provider=GOES_PROVIDER,
                product=GOES_NETCDF_PRODUCT,
                role=role,
            )
            if loaded is None:
                response = _fetch_exact(
                    binding,
                    maximum_bytes=GOES_MAX_NETCDF_BYTES,
                    deadline=deadline,
                    opener=opener,
                )
                content_sha256 = hashlib.sha256(response.payload).hexdigest()
                receipt = raw_store.store(
                    provider=GOES_PROVIDER,
                    product=GOES_NETCDF_PRODUCT,
                    fetch_id=f"{job_id}:{role}",
                    content_type=UNINTERPRETED_CONTENT_TYPE,
                    received_at=response.received_at,
                    upstream_id=_upstream_id(
                        product=GOES_NETCDF_PRODUCT,
                        role=role,
                        locator_sha256=_goes_object_locator(satellite, role, item),
                        request_sha256=binding.request_sha256,
                        transport_sha256=response.transport_sha256,
                        content_sha256=content_sha256,
                    ),
                    provenance=_goes_object_provenance(
                        satellite=satellite,
                        role=role,
                        item=item,
                        binding=binding,
                        response=response,
                    ),
                    payload=response.payload,
                )
                payload = response.payload
                if _fault_injector is not None:
                    _fault_injector(f"goes.after_{role}_store")
            else:
                receipt, payload = loaded
            _validate_goes_object_receipt(
                receipt,
                job_id=job_id,
                satellite=satellite,
                role=role,
                item=item,
                binding=binding,
            )
            if payload is None:  # pragma: no cover - store return contract
                raise LightningRawCorruptionError("GOES NetCDF-bytes ontbreken")
            receipts[role] = receipt
            _remaining(deadline)

        # A stale partial member outside the newly selected closed role set is
        # corruption, never an implicit revision or silent discard.
        for index in range(len(selected), GOES_MAX_SELECTED_OBJECTS):
            role = _goes_role(index)
            if _load_receipt(
                raw_store,
                job_id=job_id,
                provider=GOES_PROVIDER,
                product=GOES_NETCDF_PRODUCT,
                role=role,
            ) is not None:
                raise LightningRawCorruptionError(
                    "GOES partial capture bevat een onverwachte dynamische rol"
                )
        if _fault_injector is not None:
            _fault_injector("goes.before_capture_commit")
        capture = _commit_capture(
            raw_store,
            job_id=job_id,
            provider=GOES_PROVIDER,
            capture_product=GOES_CAPTURE_PRODUCT,
            receipts=receipts,
        )
        if _fault_injector is not None:
            _fault_injector("goes.after_capture_commit")
        return _result(spec, capture, receipts)


def capture_noaa_goes18_glm_raw(job: object, **kwargs: Any) -> dict[str, Any]:
    return capture_noaa_goes_glm_raw(job, satellite=18, **kwargs)


def capture_noaa_goes19_glm_raw(job: object, **kwargs: Any) -> dict[str, Any]:
    return capture_noaa_goes_glm_raw(job, satellite=19, **kwargs)


_EUMETSAT_SPEC = LightningHandlerSpec(
    handler_id=EUMETSAT_HANDLER_ID,
    workload_id=EUMETSAT_WORKLOAD_ID,
    provider=EUMETSAT_PROVIDER,
    capture_product=EUMETSAT_CAPTURE_PRODUCT,
    maximum_transport_bytes=(
        EUMETSAT_SEARCH_MAX_BYTES + EUMETSAT_NETCDF_MAX_BYTES
    ),
    maximum_artifacts=2,
    maximum_wall_seconds=50,
    static_roles=("search_response", "netcdf4"),
    dynamic_role_pattern=None,
    dynamic_minimum=0,
    dynamic_maximum=0,
    credential_contract=(
        "parent_oauth_client_credentials_to_ephemeral_child_bearer_v1"
    ),
)
_GOES18_SPEC = LightningHandlerSpec(
    handler_id=GOES18_HANDLER_ID,
    workload_id=GOES18_WORKLOAD_ID,
    provider=GOES_PROVIDER,
    capture_product=GOES_CAPTURE_PRODUCT,
    maximum_transport_bytes=(
        2 * GOES_MAX_LISTING_BYTES
        + GOES_MAX_SELECTED_OBJECTS * GOES_MAX_NETCDF_BYTES
    ),
    maximum_artifacts=2 + GOES_MAX_SELECTED_OBJECTS,
    maximum_wall_seconds=105,
    static_roles=("listing_previous", "listing_current"),
    dynamic_role_pattern=_GOES_OBJECT_ROLE_RE.pattern,
    dynamic_minimum=1,
    dynamic_maximum=GOES_MAX_SELECTED_OBJECTS,
    credential_contract="public_anonymous_no_secret",
)
_GOES19_SPEC = LightningHandlerSpec(
    handler_id=GOES19_HANDLER_ID,
    workload_id=GOES19_WORKLOAD_ID,
    provider=GOES_PROVIDER,
    capture_product=GOES_CAPTURE_PRODUCT,
    maximum_transport_bytes=_GOES18_SPEC.maximum_transport_bytes,
    maximum_artifacts=_GOES18_SPEC.maximum_artifacts,
    maximum_wall_seconds=_GOES18_SPEC.maximum_wall_seconds,
    static_roles=_GOES18_SPEC.static_roles,
    dynamic_role_pattern=_GOES18_SPEC.dynamic_role_pattern,
    dynamic_minimum=_GOES18_SPEC.dynamic_minimum,
    dynamic_maximum=_GOES18_SPEC.dynamic_maximum,
    credential_contract="public_anonymous_no_secret",
)

HANDLER_SPECS_BY_ID: Mapping[str, LightningHandlerSpec] = MappingProxyType({
    item.handler_id: item for item in (_EUMETSAT_SPEC, _GOES18_SPEC, _GOES19_SPEC)
})
HANDLER_SPECS_BY_WORKLOAD: Mapping[str, LightningHandlerSpec] = MappingProxyType({
    item.workload_id: item for item in HANDLER_SPECS_BY_ID.values()
})

_EUMETSAT_RECOVERY = RecoveryBindingMetadata(
    handler_id=EUMETSAT_HANDLER_ID,
    workload_id=EUMETSAT_WORKLOAD_ID,
    provider=EUMETSAT_PROVIDER,
    capture_product=EUMETSAT_CAPTURE_PRODUCT,
    maximum_transport_bytes=_EUMETSAT_SPEC.maximum_transport_bytes,
    minimum_artifacts=2,
    maximum_artifacts=2,
    members=(
        RecoveryMemberMetadata(
            role="search_response",
            role_pattern=None,
            raw_product=EUMETSAT_SEARCH_PRODUCT,
            receipt_suffix=":search_response",
            maximum_transport_bytes=EUMETSAT_SEARCH_MAX_BYTES,
            minimum_count=1,
            maximum_count=1,
            provenance_kind="eumetsat_mtg_li_search_v1",
        ),
        RecoveryMemberMetadata(
            role="netcdf4",
            role_pattern=None,
            raw_product=EUMETSAT_NETCDF_PRODUCT,
            receipt_suffix=":netcdf4",
            maximum_transport_bytes=EUMETSAT_NETCDF_MAX_BYTES,
            minimum_count=1,
            maximum_count=1,
            provenance_kind="eumetsat_mtg_li_netcdf_v1",
        ),
    ),
)


def _goes_recovery(spec: LightningHandlerSpec) -> RecoveryBindingMetadata:
    return RecoveryBindingMetadata(
        handler_id=spec.handler_id,
        workload_id=spec.workload_id,
        provider=GOES_PROVIDER,
        capture_product=GOES_CAPTURE_PRODUCT,
        maximum_transport_bytes=spec.maximum_transport_bytes,
        minimum_artifacts=3,
        maximum_artifacts=2 + GOES_MAX_SELECTED_OBJECTS,
        members=(
            RecoveryMemberMetadata(
                role="listing_previous",
                role_pattern=None,
                raw_product=GOES_LISTING_PRODUCT,
                receipt_suffix=":listing_previous",
                maximum_transport_bytes=GOES_MAX_LISTING_BYTES,
                minimum_count=1,
                maximum_count=1,
                provenance_kind="noaa_goes_glm_listing_v1",
            ),
            RecoveryMemberMetadata(
                role="listing_current",
                role_pattern=None,
                raw_product=GOES_LISTING_PRODUCT,
                receipt_suffix=":listing_current",
                maximum_transport_bytes=GOES_MAX_LISTING_BYTES,
                minimum_count=1,
                maximum_count=1,
                provenance_kind="noaa_goes_glm_listing_v1",
            ),
            RecoveryMemberMetadata(
                role=None,
                role_pattern=_GOES_OBJECT_ROLE_RE.pattern,
                raw_product=GOES_NETCDF_PRODUCT,
                receipt_suffix=":{role}",
                maximum_transport_bytes=GOES_MAX_NETCDF_BYTES,
                minimum_count=1,
                maximum_count=GOES_MAX_SELECTED_OBJECTS,
                provenance_kind="noaa_goes_glm_netcdf_v1",
            ),
        ),
    )


RECOVERY_BINDING_METADATA_BY_HANDLER: Mapping[
    str, RecoveryBindingMetadata
] = MappingProxyType({
    EUMETSAT_HANDLER_ID: _EUMETSAT_RECOVERY,
    GOES18_HANDLER_ID: _goes_recovery(_GOES18_SPEC),
    GOES19_HANDLER_ID: _goes_recovery(_GOES19_SPEC),
})


def lightning_child_handler_contracts() -> dict[str, dict[str, Any]]:
    """Return independent drop-in contracts keyed by workload identity."""

    return {
        spec.workload_id: spec.child_handler_mapping()
        for spec in HANDLER_SPECS_BY_ID.values()
    }


def lightning_recovery_binding_metadata() -> dict[str, dict[str, Any]]:
    """Return bounded static/dynamic metadata for later recovery wiring."""

    return {
        handler_id: binding.as_dict()
        for handler_id, binding in RECOVERY_BINDING_METADATA_BY_HANDLER.items()
    }


def eumetsat_oauth_broker_contract() -> dict[str, Any]:
    """Describe the only authorized credential path without secret values."""

    return {
        "child_ephemeral_env": EUMETSAT_EPHEMERAL_TOKEN_ENV,
        "child_job_payload_secret_fields": [],
        "client_credentials_owner": "worker_parent_only",
        "consumer_key_secret_ref": "worker_secret.eumetsat_consumer_key",
        "consumer_secret_secret_ref": "worker_secret.eumetsat_consumer_secret",
        "contract": "stv3-eumetsat-parent-oauth-broker-v1",
        "ephemeral_env_must_be_removed_after_execution": True,
        "oauth_response_persisted": False,
        "result_contains_authentication_material": False,
    }


__all__ = [
    "CONTRACT",
    "EUMETSAT_CAPTURE_PRODUCT",
    "EUMETSAT_EPHEMERAL_TOKEN_ENV",
    "EUMETSAT_HANDLER_ID",
    "EUMETSAT_NETCDF_PRODUCT",
    "EUMETSAT_SEARCH_PRODUCT",
    "EUMETSAT_WORKLOAD_ID",
    "EumetsatOAuthError",
    "EphemeralBearerToken",
    "GOES18_HANDLER_ID",
    "GOES18_WORKLOAD_ID",
    "GOES19_HANDLER_ID",
    "GOES19_WORKLOAD_ID",
    "GOES_CAPTURE_PRODUCT",
    "GOES_LISTING_PRODUCT",
    "GOES_MAX_SELECTED_OBJECTS",
    "GOES_NETCDF_PRODUCT",
    "HANDLER_SPECS_BY_ID",
    "HANDLER_SPECS_BY_WORKLOAD",
    "LightningRawCapacityError",
    "LightningRawContractError",
    "LightningRawCorruptionError",
    "LightningRawError",
    "LightningRawTransportError",
    "RECOVERY_BINDING_METADATA_BY_HANDLER",
    "RESULT_CONTRACT",
    "capture_eumetsat_mtg_li_raw",
    "capture_noaa_goes18_glm_raw",
    "capture_noaa_goes19_glm_raw",
    "capture_noaa_goes_glm_raw",
    "eumetsat_oauth_broker_contract",
    "eumetsat_parent_token_context",
    "lightning_child_handler_contracts",
    "lightning_recovery_binding_metadata",
    "mint_eumetsat_access_token_parent",
]
