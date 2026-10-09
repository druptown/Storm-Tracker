"""Lightweight exact-raw capture for the KMI mobile-app v4 feed.

This module is deliberately a transport boundary, not a weather adapter.  It
stores the exact ``getForecasts`` response before performing the minimum JSON
inspection needed to select one animation resource.  It then stores the exact
selected response and atomically commits both receipts as one raw capture.

The app-v4 request contains a credential-shaped daily query value and the
selected resource URI may itself contain short-lived query material.  Full
URLs, query strings, coordinates, response headers, and secret values are
therefore never copied into result, receipt, recovery, or diagnostic metadata.
Only bounded SHA-256 bindings and allowlisted host/path facts are retained.

No image decoder, meteorological decoder, NumPy, Pillow, pyproj, Home
Assistant, or MQTT code belongs here.  Capturing bytes makes no claim about
meteorological meaning, operational admission, provider rights, or retention
rights.  The unresolved contract review remains explicit in the returned
technical evidence and in the drop-in handler metadata.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import hashlib
import json
import math
import re
import threading
import time
from types import MappingProxyType
from typing import Any, Callable, Mapping
from urllib.parse import urlencode, urlsplit
from urllib.request import (
    HTTPRedirectHandler,
    ProxyHandler,
    Request,
    build_opener,
)
from zoneinfo import ZoneInfo

from .raw_artifacts import (
    NATIVE_CAPTURE_STAGE,
    NATIVE_REPRESENTATION,
    RAW_ARTIFACT_SCHEMA_VERSION,
    RawArtifactCaptureReceipt,
    RawArtifactReceipt,
    RawArtifactStore,
)


CONTRACT = "stv3-worker-kmi-appv4-exact-raw-v1"
RESULT_CONTRACT = "stv3-worker-kmi-appv4-exact-raw-result-v1"
HANDLER_METADATA_SCHEMA = "stv3-worker-kmi-appv4-handler-metadata-v1"
RECOVERY_METADATA_SCHEMA = "stv3-worker-kmi-appv4-recovery-binding-v1"
TRANSPORT_CONTRACT = "stv3-kmi-appv4-https-transport-v1"
PUBLIC_REGION_QUERY_CONTRACT = "stv3-public-source-region-query-v1"

HANDLER_ID = "kmi_appv4_visual_exact_raw_only_v1"
WORKLOAD_ID = "radar.kmi.appv4_visual"
PROVIDER = "kmi"

FORECAST_PRODUCT = "kmi_appv4_forecast_manifest"
IMAGE_PRODUCT = "kmi_appv4_visualisation_png"
CAPTURE_PRODUCT = "kmi_appv4_visualisation_capture"
FORECAST_ROLE = "forecast_json"
IMAGE_ROLE = "radar_png"

FORECAST_PROVENANCE_KIND = "kmi_appv4_forecast_response_v2"
IMAGE_PROVENANCE_KIND = "kmi_appv4_image_response_v2"
CAPTURE_PROVENANCE_KIND = "kmi_appv4_visual_capture_v2"
COLLECTOR = "storm_tracker_kmi_appv4_exact_raw"
COLLECTOR_VERSION = "worker-kmi-appv4-exact-raw-v2"

FORECAST_HOST = "app.meteo.be"
FORECAST_PATH = "/services/appv4/"
FORECAST_ENDPOINT = f"https://{FORECAST_HOST}{FORECAST_PATH}"
# KMI moved the app-v4 animation frames to its official CDN in September
# 2026 while retaining the forecast manifest on app.meteo.be.  Keep this an
# exact hostname allowlist: redirects and arbitrary subdomains remain denied.
ALLOWED_IMAGE_HOSTS = frozenset({"app.meteo.be", "cdn.meteo.be"})
APP_USER_AGENT = "be.meteo.app"

# This value is part of the already deployed mobile-app request contract.  It
# remains private to this transport module and is never exposed through an
# API, exception, receipt, result, or metadata mapping.
_APPV4_REQUEST_SECRET = "r9EnW374jkJ9acc"
_APPV4_SERVICE = "getForecasts"
_BRUSSELS = ZoneInfo("Europe/Brussels")

UNINTERPRETED_CONTENT_TYPE = "application/octet-stream"
FORECAST_MAX_BYTES = 4 * 1024 * 1024
IMAGE_MAX_BYTES = 8 * 1024 * 1024
MAXIMUM_TRANSPORT_BYTES = FORECAST_MAX_BYTES + IMAGE_MAX_BYTES
MAXIMUM_ARTIFACTS = 2
MAXIMUM_WALL_SECONDS = 45
HTTP_IO_TIMEOUT_SECONDS = 15.0
CAPTURE_DEADLINE_SECONDS = 45.0
READ_CHUNK_BYTES = 64 * 1024
MAX_URL_CHARS = 8_192
MAX_HEADER_COUNT = 64
MAX_HEADER_CHARS = 8_192
MAX_TOTAL_HEADER_CHARS = 32_768
MAX_SEQUENCE_ITEMS = 512
MAX_JSON_NESTING = 64
MAX_TIMESTAMP_CHARS = 128

# south, west, north, east -- the same declared extent as the legacy visual
# route.  This is request coverage only; it is not a georeferencing claim.
COVERAGE_BOUNDS = (46.5, -2.5, 53.0, 10.5)
RETENTION_REQUIREMENT_ID = "worker_authority.kmi_appv4_raw_retention_v1"

_JOB_ID_RE = re.compile(
    r"^acq:[a-z2-7]{8}(?::z:[a-z2-7]{8}){5}:z:[a-z2-7]{4}$"
)
_QUERY_KEY_RE = re.compile(
    r"^aqr:[a-z2-7]{8}(?::z:[a-z2-7]{8}){5}:z:[a-z2-7]{4}$"
)
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_MEDIA_TYPE_RE = re.compile(
    r"^[A-Za-z0-9!#$&^_.+\-]+/[A-Za-z0-9!#$&^_.+\-]+$"
)
_HEADER_NAME_RE = re.compile(r"^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$")
_LOCAL_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_CAPTURE_LOCK = threading.Lock()


class KmiAppv4RawError(RuntimeError):
    """Base error for payload-free KMI app-v4 capture failures."""


class KmiAppv4ContractError(KmiAppv4RawError):
    """A request, response, or job escaped the closed technical contract."""


class KmiAppv4CapacityError(KmiAppv4RawError):
    """A body, URL, header, role, or deadline boundary was exhausted."""


class KmiAppv4TransportError(KmiAppv4RawError):
    """HTTPS failed without reflecting an endpoint or credential value."""


class KmiAppv4SelectionError(KmiAppv4RawError):
    """Durable forecast bytes cannot safely select an allowlisted resource."""


class KmiAppv4CorruptionError(KmiAppv4RawError):
    """Existing durable evidence differs from this exact capture authority."""


@dataclass(frozen=True, slots=True)
class KmiAppv4RoleSpec:
    role: str
    raw_product: str
    fetch_suffix: str
    maximum_transport_bytes: int
    provenance_kind: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "allowed_content_types": [UNINTERPRETED_CONTENT_TYPE],
            "fetch_suffix": self.fetch_suffix,
            "maximum_transport_bytes": self.maximum_transport_bytes,
            "provenance_kind": self.provenance_kind,
            "raw_product": self.raw_product,
            "role": self.role,
        }


@dataclass(frozen=True, slots=True)
class KmiAppv4HandlerSpec:
    handler_id: str
    workload_id: str
    provider: str
    capture_product: str
    roles: tuple[KmiAppv4RoleSpec, ...]
    maximum_transport_bytes: int
    maximum_artifacts: int
    maximum_wall_seconds: int

    def child_handler_mapping(self) -> dict[str, Any]:
        """Return a secret-free, copy-only contract for later dispatcher wiring."""

        return {
            "allowed_content_types": [UNINTERPRETED_CONTENT_TYPE],
            "capture_fetch_suffix": ":capture",
            "capture_product": self.capture_product,
            "capture_provenance_kind": CAPTURE_PROVENANCE_KIND,
            "capture_state": "captured_not_decoded",
            "credential_contract": "code_owned_daily_app_request_key_v1",
            "decode_field": "decode_status",
            "decode_status": "not_attempted",
            "handler_id": self.handler_id,
            "ledger_provider": self.provider,
            "maximum_artifacts": self.maximum_artifacts,
            "maximum_transport_bytes": self.maximum_transport_bytes,
            "maximum_wall_seconds": self.maximum_wall_seconds,
            "meteorological_parse": False,
            "native_provider": self.provider,
            "payload_contract": "public_region_query_v1",
            "required_requirement_ids": [RETENTION_REQUIREMENT_ID],
            "required_roles": [item.role for item in self.roles],
            "result_provider": self.provider,
            "rights_review_status": "contract_review_hold",
            "role_contracts": [item.as_dict() for item in self.roles],
            "safe_native_identity": True,
            "schema": HANDLER_METADATA_SCHEMA,
            "source_field": "sources",
            "source_hash_prefix": "",
            "workload_id": self.workload_id,
        }

    def recovery_mapping(self) -> dict[str, Any]:
        """Return bounded static metadata for shared crash recovery wiring."""

        return {
            "capture_fetch_suffix": ":capture",
            "capture_product": self.capture_product,
            "capture_provenance_kind": CAPTURE_PROVENANCE_KIND,
            "handler_id": self.handler_id,
            "job_identity_kind": "safe_native_alias_v1",
            "ledger_provider": self.provider,
            "maximum_artifacts": self.maximum_artifacts,
            "maximum_transport_bytes": self.maximum_transport_bytes,
            "members": [item.as_dict() for item in self.roles],
            "raw_provider": self.provider,
            "schema": RECOVERY_METADATA_SCHEMA,
            "workload_id": self.workload_id,
        }


HANDLER_SPEC = KmiAppv4HandlerSpec(
    handler_id=HANDLER_ID,
    workload_id=WORKLOAD_ID,
    provider=PROVIDER,
    capture_product=CAPTURE_PRODUCT,
    roles=(
        KmiAppv4RoleSpec(
            role=FORECAST_ROLE,
            raw_product=FORECAST_PRODUCT,
            fetch_suffix=":forecast_json",
            maximum_transport_bytes=FORECAST_MAX_BYTES,
            provenance_kind=FORECAST_PROVENANCE_KIND,
        ),
        KmiAppv4RoleSpec(
            role=IMAGE_ROLE,
            raw_product=IMAGE_PRODUCT,
            fetch_suffix=":radar_png",
            maximum_transport_bytes=IMAGE_MAX_BYTES,
            provenance_kind=IMAGE_PROVENANCE_KIND,
        ),
    ),
    maximum_transport_bytes=MAXIMUM_TRANSPORT_BYTES,
    maximum_artifacts=MAXIMUM_ARTIFACTS,
    maximum_wall_seconds=MAXIMUM_WALL_SECONDS,
)


@dataclass(frozen=True, slots=True)
class _LocationAuthority:
    latitude: float
    longitude: float
    query_key: str
    authority_sha256: str


@dataclass(frozen=True, slots=True)
class _RequestBinding:
    role: str
    url: str
    host: str
    path: str
    request_sha256: str
    request_local_date: str | None
    query_authority_sha256: str
    selected_uri_path_sha256: str | None = None
    product_timestamp: float | None = None


@dataclass(frozen=True, slots=True)
class _HttpResponse:
    payload: bytes
    status_code: int
    received_at: float
    declared_length: int | None
    reported_media_type: str
    response_headers_sha256: str
    final_target_sha256: str
    transport_sha256: str


@dataclass(frozen=True, slots=True)
class _FrameSelection:
    binding: _RequestBinding
    product_timestamp: float


class _RejectRedirects(HTTPRedirectHandler):
    def redirect_request(self, *_args: Any, **_kwargs: Any) -> None:
        raise KmiAppv4ContractError("KMI app-v4 redirect is verboden")


def _canonical_bytes(value: Mapping[str, Any]) -> bytes:
    try:
        return json.dumps(
            dict(value),
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
    except (TypeError, ValueError, OverflowError) as exc:
        raise KmiAppv4ContractError(
            "KMI app-v4 authority is niet canoniek"
        ) from exc


def _digest(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _finite_number(
    value: object,
    *,
    name: str,
    minimum: float,
    maximum: float,
) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise KmiAppv4ContractError(f"KMI app-v4 {name} is ongeldig")
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        raise KmiAppv4ContractError(
            f"KMI app-v4 {name} is ongeldig"
        ) from None
    if not math.isfinite(result) or not minimum <= result <= maximum:
        raise KmiAppv4ContractError(f"KMI app-v4 {name} is ongeldig")
    return 0.0 if result == 0.0 else result


def _wall_time(clock: Callable[[], float]) -> float:
    try:
        value = clock()
    except Exception:
        raise KmiAppv4ContractError("KMI app-v4 wall clock faalde") from None
    return _finite_number(
        value, name="wall clock", minimum=0.0, maximum=float(2**53)
    )


def _job(job: object) -> tuple[str, dict[str, Any]]:
    job_id = getattr(job, "job_id", None)
    payload = getattr(job, "payload", None)
    if not isinstance(job_id, str) or _JOB_ID_RE.fullmatch(job_id) is None:
        raise KmiAppv4ContractError(
            "KMI app-v4 job mist een veilige native jobidentity"
        )
    if type(payload) is not dict:
        raise KmiAppv4ContractError(
            "KMI app-v4 job mist een exacte handlerpayload"
        )
    return job_id, payload


def _location_authority(payload: dict[str, Any]) -> _LocationAuthority:
    if set(payload) != {"contract", "latitude", "longitude", "query_key"}:
        raise KmiAppv4ContractError(
            "KMI app-v4 regiopayload bevat onbekende of ontbrekende velden"
        )
    if payload["contract"] != PUBLIC_REGION_QUERY_CONTRACT:
        raise KmiAppv4ContractError(
            "KMI app-v4 regiopayloadcontract wijkt af"
        )
    query_key = payload["query_key"]
    if not isinstance(query_key, str) or _QUERY_KEY_RE.fullmatch(query_key) is None:
        raise KmiAppv4ContractError(
            "KMI app-v4 regioquery-identiteit is ongeldig"
        )
    latitude = _finite_number(
        payload["latitude"], name="latitude", minimum=-90.0, maximum=90.0
    )
    longitude = _finite_number(
        payload["longitude"], name="longitude", minimum=-180.0, maximum=180.0
    )
    south, west, north, east = COVERAGE_BOUNDS
    if not south <= latitude <= north or not west <= longitude <= east:
        raise KmiAppv4ContractError(
            "KMI app-v4 regioquery valt buiten de gesloten providerdekking"
        )
    authority = {
        "contract": PUBLIC_REGION_QUERY_CONTRACT,
        "latitude": float(f"{latitude:.5f}"),
        "longitude": float(f"{longitude:.5f}"),
        "query_key": query_key,
        "workload_id": WORKLOAD_ID,
    }
    return _LocationAuthority(
        latitude=authority["latitude"],
        longitude=authority["longitude"],
        query_key=query_key,
        authority_sha256=_digest(authority),
    )


def _validated_local_date(value: object) -> str:
    if not isinstance(value, str) or _LOCAL_DATE_RE.fullmatch(value) is None:
        raise KmiAppv4CorruptionError(
            "duurzame KMI app-v4 requestdatum is ongeldig"
        )
    try:
        parsed = datetime.strptime(value, "%Y-%m-%d")
    except ValueError:
        raise KmiAppv4CorruptionError(
            "duurzame KMI app-v4 requestdatum is ongeldig"
        ) from None
    if parsed.strftime("%Y-%m-%d") != value:
        raise KmiAppv4CorruptionError(
            "duurzame KMI app-v4 requestdatum is niet canoniek"
        )
    return value


def _app_request_key(local_date: str) -> str:
    day = datetime.strptime(local_date, "%Y-%m-%d").strftime("%d/%m/%Y")
    return hashlib.md5(  # noqa: S324 - provider's fixed compatibility scheme
        f"{_APPV4_REQUEST_SECRET};{_APPV4_SERVICE};{day}".encode("ascii")
    ).hexdigest()


def _forecast_binding(
    authority: _LocationAuthority,
    *,
    local_date: str,
) -> _RequestBinding:
    local_date = _validated_local_date(local_date)
    query = urlencode((
        ("s", _APPV4_SERVICE),
        ("k", _app_request_key(local_date)),
        ("lat", f"{authority.latitude:.5f}"),
        ("long", f"{authority.longitude:.5f}"),
    ))
    url = f"{FORECAST_ENDPOINT}?{query}"
    if len(url) > MAX_URL_CHARS or not url.isascii():
        raise KmiAppv4CapacityError(
            "KMI app-v4 forecastrequest overschrijdt de URL-grens"
        )
    return _RequestBinding(
        role=FORECAST_ROLE,
        url=url,
        host=FORECAST_HOST,
        path=FORECAST_PATH,
        request_sha256=_sha256_text(url),
        request_local_date=local_date,
        query_authority_sha256=authority.authority_sha256,
    )


def _safe_image_binding(
    uri: object,
    *,
    query_authority_sha256: str,
    product_timestamp: float,
) -> _RequestBinding:
    if (
        not isinstance(uri, str)
        or not uri
        or uri != uri.strip()
        or len(uri) > MAX_URL_CHARS
        or not uri.isascii()
        or any(ord(character) < 0x20 or ord(character) == 0x7F for character in uri)
    ):
        raise KmiAppv4SelectionError(
            "KMI app-v4 animatie bevat geen veilige resource-identiteit"
        )
    try:
        parts = urlsplit(uri)
        port = parts.port
    except ValueError:
        raise KmiAppv4SelectionError(
            "KMI app-v4 animatie bevat geen veilige HTTPS-resource"
        ) from None
    if (
        parts.scheme != "https"
        or parts.hostname not in ALLOWED_IMAGE_HOSTS
        or port not in {None, 443}
        or parts.username is not None
        or parts.password is not None
        or not parts.path.startswith("/")
        or len(parts.path) > 4096
        or bool(parts.fragment)
    ):
        raise KmiAppv4SelectionError(
            "KMI app-v4 animatieresource valt buiten de HTTPS-allowlist"
        )
    return _RequestBinding(
        role=IMAGE_ROLE,
        url=uri,
        host=str(parts.hostname),
        path=parts.path,
        request_sha256=_sha256_text(uri),
        request_local_date=None,
        query_authority_sha256=query_authority_sha256,
        selected_uri_path_sha256=_sha256_text(parts.path),
        product_timestamp=product_timestamp,
    )


def _remaining(deadline: float) -> float:
    value = float(deadline) - time.monotonic()
    if not math.isfinite(value) or value <= 0:
        raise KmiAppv4CapacityError(
            "KMI app-v4 capturedeadline is verstreken"
        )
    return value


def _deadline(value: float | None) -> float:
    now = time.monotonic()
    if value is None:
        return now + CAPTURE_DEADLINE_SECONDS
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise KmiAppv4ContractError("KMI app-v4 deadline is ongeldig")
    try:
        requested = float(value)
    except (TypeError, ValueError, OverflowError):
        raise KmiAppv4ContractError("KMI app-v4 deadline is ongeldig") from None
    if not math.isfinite(requested):
        raise KmiAppv4ContractError("KMI app-v4 deadline is ongeldig")
    result = min(requested, now + CAPTURE_DEADLINE_SECONDS)
    _remaining(result)
    return result


def _open_without_redirects(request: Request, *, timeout: float) -> Any:
    return build_opener(ProxyHandler({}), _RejectRedirects()).open(
        request, timeout=timeout
    )


def _header_entries(headers: object) -> tuple[tuple[str, str], ...]:
    if headers is None:
        return ()
    items = getattr(headers, "items", None)
    if not callable(items):
        raise KmiAppv4ContractError(
            "KMI app-v4 responseheaders zijn ongeldig"
        )
    try:
        raw_items = list(items())
    except Exception:
        raise KmiAppv4ContractError(
            "KMI app-v4 responseheaders zijn ongeldig"
        ) from None
    if len(raw_items) > MAX_HEADER_COUNT:
        raise KmiAppv4CapacityError(
            "KMI app-v4 response bevat te veel headers"
        )
    result: list[tuple[str, str]] = []
    total = 0
    for pair in raw_items:
        if not isinstance(pair, tuple) or len(pair) != 2:
            raise KmiAppv4ContractError(
                "KMI app-v4 responseheader is ongeldig"
            )
        name, value = str(pair[0]), str(pair[1])
        if (
            not name
            or len(name) > 256
            or _HEADER_NAME_RE.fullmatch(name) is None
            or len(value) > MAX_HEADER_CHARS
            or any(
                ord(character) < 0x20 or ord(character) == 0x7F
                for character in value
            )
        ):
            raise KmiAppv4ContractError(
                "KMI app-v4 responseheader is ongeldig"
            )
        total += len(name) + len(value)
        if total > MAX_TOTAL_HEADER_CHARS:
            raise KmiAppv4CapacityError(
                "KMI app-v4 responseheaders overschrijden de grens"
            )
        result.append((name.lower(), value.strip()))
    return tuple(sorted(result))


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
    if len(values) != 1:
        raise KmiAppv4ContractError(
            "KMI app-v4 Content-Length is dubbelzinnig"
        )
    value = values[0]
    if not value.isascii() or not value.isdecimal():
        raise KmiAppv4ContractError(
            "KMI app-v4 Content-Length is ongeldig"
        )
    length = int(value)
    if length > maximum:
        raise KmiAppv4CapacityError(
            "KMI app-v4 Content-Length overschrijdt de rolgrens"
        )
    return length


def _reported_media_type(entries: tuple[tuple[str, str], ...]) -> str:
    values = _header_values(entries, "content-type")
    if len(values) != 1:
        return "unknown"
    media_type = values[0].split(";", 1)[0].strip().lower()
    if len(media_type) > 256 or _MEDIA_TYPE_RE.fullmatch(media_type) is None:
        return "unknown"
    return media_type


def _read_body(
    response: object,
    *,
    maximum: int,
    declared_length: int | None,
    deadline: float,
) -> bytes:
    reader = getattr(response, "read", None)
    if not callable(reader):
        raise KmiAppv4ContractError(
            "KMI app-v4 response mist een bytestream"
        )
    chunks: list[bytes] = []
    total = 0
    maximum_chunks = maximum // READ_CHUNK_BYTES + 2
    for _index in range(maximum_chunks):
        _remaining(deadline)
        requested = min(READ_CHUNK_BYTES, maximum + 1 - total)
        try:
            raw = reader(requested)
        except (TimeoutError, KmiAppv4RawError):
            raise
        except Exception:
            raise KmiAppv4TransportError(
                "KMI app-v4 responsstream faalde"
            ) from None
        _remaining(deadline)
        if not raw:
            break
        if not isinstance(raw, (bytes, bytearray, memoryview)):
            raise KmiAppv4ContractError(
                "KMI app-v4 responsstream leverde geen bytes"
            )
        block = bytes(raw)
        if len(block) > requested:
            raise KmiAppv4CapacityError(
                "KMI app-v4 responsechunk overschrijdt de grens"
            )
        chunks.append(block)
        total += len(block)
        if total > maximum or (
            declared_length is not None and total > declared_length
        ):
            raise KmiAppv4CapacityError(
                "KMI app-v4 response overschrijdt de rolgrens"
            )
        if declared_length is not None and total == declared_length:
            break
    else:
        raise KmiAppv4CapacityError(
            "KMI app-v4 responsstream overschrijdt de chunkgrens"
        )
    payload = b"".join(chunks)
    if declared_length is not None and len(payload) != declared_length:
        raise KmiAppv4ContractError(
            "KMI app-v4 responslengte wijkt af van Content-Length"
        )
    return payload


def _fetch(
    binding: _RequestBinding,
    *,
    maximum: int,
    deadline: float,
    opener: Callable[..., Any] | None,
    wall_clock: Callable[[], float],
) -> _HttpResponse:
    request = Request(
        binding.url,
        headers={
            "Accept": "*/*",
            "Accept-Encoding": "identity",
            "User-Agent": APP_USER_AGENT,
        },
        method="GET",
    )
    open_call = opener or _open_without_redirects
    try:
        response = open_call(
            request,
            timeout=min(HTTP_IO_TIMEOUT_SECONDS, _remaining(deadline)),
        )
    except (KmiAppv4RawError, TimeoutError):
        raise
    except Exception:
        raise KmiAppv4TransportError(
            "KMI app-v4 HTTPS-response kon niet worden geopend"
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
            raise KmiAppv4ContractError(
                "KMI app-v4 antwoord is geen succesvolle HTTP-response"
            )
        try:
            final_url = str(getattr(response, "geturl", lambda: binding.url)())
        except Exception:
            raise KmiAppv4ContractError(
                "KMI app-v4 response mist zijn requestbinding"
            ) from None
        if final_url != binding.url:
            raise KmiAppv4ContractError(
                "KMI app-v4 response week af van de exacte request"
            )
        entries = _header_entries(getattr(response, "headers", None))
        encodings = tuple(
            item.strip().lower()
            for value in _header_values(entries, "content-encoding")
            for item in value.split(",")
            if item.strip()
        )
        if encodings and any(item != "identity" for item in encodings):
            raise KmiAppv4ContractError(
                "KMI app-v4 response gebruikt geen identity-representatie"
            )
        declared = _declared_length(entries, maximum)
        payload = _read_body(
            response,
            maximum=maximum,
            declared_length=declared,
            deadline=deadline,
        )
        received_at = _wall_time(wall_clock)
        headers_sha256 = _digest({
            "entries": [[key, value] for key, value in entries]
        })
        final_target_sha256 = _sha256_text(final_url)
        media_type = _reported_media_type(entries)
        transport = {
            "body_sha256": hashlib.sha256(payload).hexdigest(),
            "body_size_bytes": len(payload),
            "declared_length": declared,
            "final_target_sha256": final_target_sha256,
            "received_at": received_at,
            "reported_media_type": media_type,
            "request_sha256": binding.request_sha256,
            "response_headers_sha256": headers_sha256,
            "role": binding.role,
            "status_code": status,
            "transport_contract": TRANSPORT_CONTRACT,
        }
        return _HttpResponse(
            payload=payload,
            status_code=status,
            received_at=received_at,
            declared_length=declared,
            reported_media_type=media_type,
            response_headers_sha256=headers_sha256,
            final_target_sha256=final_target_sha256,
            transport_sha256=_digest(transport),
        )
    except (KmiAppv4RawError, TimeoutError):
        raise
    except Exception:
        raise KmiAppv4ContractError(
            "KMI app-v4 responsemetadata is ongeldig"
        ) from None
    finally:
        closer = getattr(response, "close", None)
        if callable(closer):
            try:
                closer()
            except Exception:
                pass


def _transport_document(
    binding: _RequestBinding, response: _HttpResponse
) -> dict[str, Any]:
    return {
        "body_sha256": hashlib.sha256(response.payload).hexdigest(),
        "body_size_bytes": len(response.payload),
        "declared_length": response.declared_length,
        "final_target_sha256": response.final_target_sha256,
        "received_at": response.received_at,
        "reported_media_type": response.reported_media_type,
        "request_sha256": binding.request_sha256,
        "response_headers_sha256": response.response_headers_sha256,
        "role": binding.role,
        "status_code": response.status_code,
        "transport_contract": TRANSPORT_CONTRACT,
    }


def _provenance(
    binding: _RequestBinding,
    response: _HttpResponse,
    *,
    provenance_kind: str,
) -> dict[str, Any]:
    transport = _transport_document(binding, response)
    result: dict[str, Any] = {
        "access_class": "embedded_mobile_app_request_contract",
        "capture_stage": NATIVE_CAPTURE_STAGE,
        "collector": COLLECTOR,
        "collector_version": COLLECTOR_VERSION,
        "content_encoding": "identity",
        "endpoint_host": binding.host,
        "endpoint_path_sha256": _sha256_text(binding.path),
        "legal_advice": False,
        "meteorological_parse_attempted": False,
        "operational_admission": False,
        "payload_parse_scope": (
            "minimal_artifact_link_selection"
            if binding.role == FORECAST_ROLE
            else "none"
        ),
        "provider_rights_claim": False,
        "provenance_kind": provenance_kind,
        "query_authority_sha256": binding.query_authority_sha256,
        "representation": NATIVE_REPRESENTATION,
        "request_sha256": binding.request_sha256,
        "retention_activation_requirement_id": RETENTION_REQUIREMENT_ID,
        "retention_right_claim": False,
        "rights_review_status": "contract_review_hold",
        "semantic_status": "unverified_visualisation_bytes",
        "transport": transport,
        "transport_sha256": response.transport_sha256,
        "weather_state": "unknown",
    }
    if binding.request_local_date is not None:
        result["request_local_date"] = binding.request_local_date
    if binding.selected_uri_path_sha256 is not None:
        result["selected_uri_path_sha256"] = binding.selected_uri_path_sha256
    if binding.product_timestamp is not None:
        result["product_timestamp"] = binding.product_timestamp
    return result


def _forecast_upstream_id(
    binding: _RequestBinding, content_sha256: str
) -> str:
    return (
        f"{FORECAST_HOST}{FORECAST_PATH}|request={binding.request_sha256}"
        f"|content={content_sha256}"
    )


def _image_upstream_id(
    binding: _RequestBinding, content_sha256: str
) -> str:
    return (
        f"{binding.host}|target={binding.request_sha256}"
        f"|path={binding.selected_uri_path_sha256}|content={content_sha256}"
    )


def _capture_upstream_id(
    forecast: RawArtifactReceipt,
    image: RawArtifactReceipt,
    selection: _FrameSelection,
) -> str:
    return (
        f"{CAPTURE_PRODUCT}|forecast={forecast.content_sha256}"
        f"|image={image.content_sha256}"
        f"|target={selection.binding.request_sha256}"
        f"|time={selection.product_timestamp:.6f}"
    )


def _preflight_json_text(text: str) -> None:
    depth = 0
    in_string = False
    escaped = False
    for character in text:
        if in_string:
            if escaped:
                escaped = False
            elif character == "\\":
                escaped = True
            elif character == '"':
                in_string = False
            continue
        if character == '"':
            in_string = True
        elif character in "[{":
            depth += 1
            if depth > MAX_JSON_NESTING:
                raise KmiAppv4SelectionError(
                    "KMI app-v4 forecast-JSON is te diep genest"
                )
        elif character in "]}":
            depth -= 1
            if depth < 0:
                raise KmiAppv4SelectionError(
                    "KMI app-v4 forecast-JSON is structureel ongeldig"
                )
    if in_string or depth != 0:
        raise KmiAppv4SelectionError(
            "KMI app-v4 forecast-JSON is structureel ongeldig"
        )


def _reject_json_constant(_value: str) -> None:
    raise KmiAppv4SelectionError(
        "KMI app-v4 forecast-JSON bevat geen eindige JSON-waarde"
    )


def _frame_timestamp(value: object) -> float:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value) > MAX_TIMESTAMP_CHARS
    ):
        raise KmiAppv4SelectionError(
            "KMI app-v4 frame mist een begrensde tijd"
        )
    try:
        instant = datetime.fromisoformat(value)
    except (TypeError, ValueError, OverflowError):
        raise KmiAppv4SelectionError(
            "KMI app-v4 frametijd is ongeldig"
        ) from None
    if instant.tzinfo is None or instant.utcoffset() is None:
        raise KmiAppv4SelectionError(
            "KMI app-v4 frametijd mist een timezone"
        )
    try:
        timestamp = instant.timestamp()
    except (OSError, OverflowError, ValueError):
        raise KmiAppv4SelectionError(
            "KMI app-v4 frametijd valt buiten het tijdcontract"
        ) from None
    if not math.isfinite(timestamp) or not 0.0 <= timestamp <= float(2**53):
        raise KmiAppv4SelectionError(
            "KMI app-v4 frametijd valt buiten het tijdcontract"
        )
    return timestamp


def _select_frame(
    payload: bytes,
    *,
    query_authority_sha256: str,
    selection_at: float,
) -> _FrameSelection:
    if not payload:
        raise KmiAppv4SelectionError(
            "KMI app-v4 forecastresponse is leeg na duurzame opslag"
        )
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError:
        raise KmiAppv4SelectionError(
            "KMI app-v4 forecastresponse is geen UTF-8 JSON"
        ) from None
    _preflight_json_text(text)
    try:
        document = json.loads(text, parse_constant=_reject_json_constant)
    except KmiAppv4SelectionError:
        raise
    except (json.JSONDecodeError, RecursionError, MemoryError, TypeError, ValueError):
        raise KmiAppv4SelectionError(
            "KMI app-v4 forecastresponse is geen begrensde JSON"
        ) from None
    if type(document) is not dict:
        raise KmiAppv4SelectionError(
            "KMI app-v4 forecastresponse mist een JSON-object"
        )
    animation = document.get("animation")
    sequence = animation.get("sequence") if type(animation) is dict else None
    if type(sequence) is not list or not 1 <= len(sequence) <= MAX_SEQUENCE_ITEMS:
        raise KmiAppv4SelectionError(
            "KMI app-v4 forecastresponse mist een begrensde animatiesequentie"
        )
    selection_at = _finite_number(
        selection_at,
        name="selectietijd",
        minimum=0.0,
        maximum=float(2**53),
    )
    candidates: list[tuple[float, int, _RequestBinding]] = []
    for index, item in enumerate(sequence):
        if type(item) is not dict:
            continue
        try:
            timestamp = _frame_timestamp(item.get("time"))
            binding = _safe_image_binding(
                item.get("uri"),
                query_authority_sha256=query_authority_sha256,
                product_timestamp=timestamp,
            )
        except KmiAppv4SelectionError:
            continue
        candidates.append((timestamp, index, binding))
    if not candidates:
        raise KmiAppv4SelectionError(
            "KMI app-v4 forecastresponse bevat geen allowlisted frame"
        )
    historical = [item for item in candidates if item[0] <= selection_at]
    if historical:
        timestamp, _index, binding = max(historical, key=lambda item: (item[0], item[1]))
    else:
        timestamp, _index, binding = min(candidates, key=lambda item: (item[0], item[1]))
    return _FrameSelection(binding=binding, product_timestamp=timestamp)


def _transport_from_receipt(
    receipt: RawArtifactReceipt,
    *,
    binding: _RequestBinding,
    maximum: int,
) -> str:
    provenance = receipt.provenance
    transport = provenance.get("transport")
    if type(transport) is not dict:
        raise KmiAppv4CorruptionError(
            "duurzame KMI app-v4 transportmetadata ontbreekt"
        )
    status = transport.get("status_code")
    declared = transport.get("declared_length")
    media_type = transport.get("reported_media_type")
    if (
        transport.get("transport_contract") != TRANSPORT_CONTRACT
        or transport.get("role") != binding.role
        or transport.get("request_sha256") != binding.request_sha256
        or transport.get("final_target_sha256") != binding.request_sha256
        or transport.get("body_sha256") != receipt.content_sha256
        or transport.get("body_size_bytes") != receipt.size_bytes
        or transport.get("received_at") != receipt.received_at
        or isinstance(status, bool)
        or not isinstance(status, int)
        or not 200 <= status < 300
        or (
            declared is not None
            and (
                isinstance(declared, bool)
                or not isinstance(declared, int)
                or not 0 <= declared <= maximum
                or declared != receipt.size_bytes
            )
        )
        or not isinstance(media_type, str)
        or not 1 <= len(media_type) <= 256
        or not isinstance(transport.get("response_headers_sha256"), str)
        or _SHA256_RE.fullmatch(transport["response_headers_sha256"]) is None
    ):
        raise KmiAppv4CorruptionError(
            "duurzame KMI app-v4 transportmetadata wijkt af"
        )
    transport_sha256 = _digest(transport)
    if provenance.get("transport_sha256") != transport_sha256:
        raise KmiAppv4CorruptionError(
            "duurzame KMI app-v4 transportchecksum wijkt af"
        )
    return transport_sha256


def _validate_fixed_provenance(
    receipt: RawArtifactReceipt,
    *,
    binding: _RequestBinding,
    provenance_kind: str,
    endpoint_path_sha256: str | None = None,
) -> None:
    provenance = receipt.provenance
    expected_path_sha256 = (
        _sha256_text(binding.path)
        if endpoint_path_sha256 is None
        else endpoint_path_sha256
    )
    fixed = {
        "access_class": "embedded_mobile_app_request_contract",
        "capture_stage": NATIVE_CAPTURE_STAGE,
        "collector": COLLECTOR,
        "collector_version": COLLECTOR_VERSION,
        "content_encoding": "identity",
        "endpoint_host": binding.host,
        "endpoint_path_sha256": expected_path_sha256,
        "legal_advice": False,
        "meteorological_parse_attempted": False,
        "operational_admission": False,
        "payload_parse_scope": (
            "minimal_artifact_link_selection"
            if binding.role == FORECAST_ROLE
            else "none"
        ),
        "provider_rights_claim": False,
        "provenance_kind": provenance_kind,
        "query_authority_sha256": binding.query_authority_sha256,
        "representation": NATIVE_REPRESENTATION,
        "request_sha256": binding.request_sha256,
        "retention_activation_requirement_id": RETENTION_REQUIREMENT_ID,
        "retention_right_claim": False,
        "rights_review_status": "contract_review_hold",
        "semantic_status": "unverified_visualisation_bytes",
        "weather_state": "unknown",
    }
    if any(provenance.get(key) != value for key, value in fixed.items()):
        raise KmiAppv4CorruptionError(
            "duurzame KMI app-v4 provenance wijkt af"
        )


def _validate_forecast_receipt(
    receipt: RawArtifactReceipt,
    payload: bytes,
    *,
    job_id: str,
    authority: _LocationAuthority,
) -> _FrameSelection:
    provenance = receipt.provenance
    local_date = _validated_local_date(provenance.get("request_local_date"))
    binding = _forecast_binding(authority, local_date=local_date)
    _validate_fixed_provenance(
        receipt,
        binding=binding,
        provenance_kind=FORECAST_PROVENANCE_KIND,
    )
    if (
        receipt.provider != PROVIDER
        or receipt.product != FORECAST_PRODUCT
        or receipt.fetch_id != f"{job_id}:forecast_json"
        or receipt.content_type != UNINTERPRETED_CONTENT_TYPE
        or not 0 <= receipt.size_bytes <= FORECAST_MAX_BYTES
        or receipt.size_bytes != len(payload)
        or receipt.content_sha256 != hashlib.sha256(payload).hexdigest()
        or receipt.upstream_id
        != _forecast_upstream_id(binding, receipt.content_sha256)
    ):
        raise KmiAppv4CorruptionError(
            "duurzame KMI app-v4 forecastreceipt wijkt af"
        )
    _transport_from_receipt(
        receipt, binding=binding, maximum=FORECAST_MAX_BYTES
    )
    return _select_frame(
        payload,
        query_authority_sha256=authority.authority_sha256,
        selection_at=receipt.received_at,
    )


def _validate_image_receipt(
    receipt: RawArtifactReceipt,
    payload: bytes,
    *,
    job_id: str,
    selection: _FrameSelection,
) -> None:
    binding = selection.binding
    _validate_fixed_provenance(
        receipt,
        binding=binding,
        provenance_kind=IMAGE_PROVENANCE_KIND,
    )
    provenance = receipt.provenance
    if (
        provenance.get("selected_uri_path_sha256")
        != binding.selected_uri_path_sha256
        or provenance.get("product_timestamp") != selection.product_timestamp
        or receipt.provider != PROVIDER
        or receipt.product != IMAGE_PRODUCT
        or receipt.fetch_id != f"{job_id}:radar_png"
        or receipt.content_type != UNINTERPRETED_CONTENT_TYPE
        or not 0 <= receipt.size_bytes <= IMAGE_MAX_BYTES
        or receipt.size_bytes != len(payload)
        or receipt.content_sha256 != hashlib.sha256(payload).hexdigest()
        or receipt.upstream_id != _image_upstream_id(
            binding, receipt.content_sha256
        )
    ):
        raise KmiAppv4CorruptionError(
            "duurzame KMI app-v4 beeldreceipt wijkt af"
        )
    _transport_from_receipt(receipt, binding=binding, maximum=IMAGE_MAX_BYTES)


def _validate_capture(
    capture: RawArtifactCaptureReceipt,
    *,
    job_id: str,
    forecast: RawArtifactReceipt,
    image: RawArtifactReceipt,
    selection: _FrameSelection,
) -> None:
    members = {item.role: item.receipt_id for item in capture.members}
    if (
        capture.raw_store_schema_version != RAW_ARTIFACT_SCHEMA_VERSION
        or capture.provider != PROVIDER
        or capture.product != CAPTURE_PRODUCT
        or capture.fetch_id != f"{job_id}:capture"
        or capture.upstream_capture_id
        != _capture_upstream_id(forecast, image, selection)
        or capture.product_timestamp != selection.product_timestamp
        or members
        != {
            FORECAST_ROLE: forecast.receipt_id,
            IMAGE_ROLE: image.receipt_id,
        }
    ):
        raise KmiAppv4CorruptionError(
            "duurzame KMI app-v4 capture wijkt af"
        )


def _source(role: str, receipt: RawArtifactReceipt) -> dict[str, Any]:
    return {
        "content_sha256": receipt.content_sha256,
        "receipt_id": receipt.receipt_id,
        "receipt_sha256": receipt.receipt_sha256,
        "role": role,
        "size_bytes": receipt.size_bytes,
    }


def _result(
    capture: RawArtifactCaptureReceipt,
    forecast: RawArtifactReceipt,
    image: RawArtifactReceipt,
    selection: _FrameSelection,
) -> dict[str, Any]:
    sources = {
        FORECAST_ROLE: _source(FORECAST_ROLE, forecast),
        IMAGE_ROLE: _source(IMAGE_ROLE, image),
    }
    return {
        "capture_state": "captured_not_decoded",
        "contract": RESULT_CONTRACT,
        "decode_status": "not_attempted",
        "handler_id": HANDLER_ID,
        "meteorological_claim": False,
        "meteorological_parse_attempted": False,
        "operational_admission_eligible": False,
        "product_timestamp": selection.product_timestamp,
        "provider": PROVIDER,
        "provider_rights_claim": False,
        "retention_activation_requirement_id": RETENTION_REQUIREMENT_ID,
        "retention_right_claim": False,
        "rights_review_status": "contract_review_hold",
        "semantic_status": "unverified_visualisation_bytes",
        "weather_state": "unknown",
        "workload_id": WORKLOAD_ID,
        "raw_provenance": {
            "raw_capture_id": capture.capture_id,
            "raw_capture_manifest_sha256": capture.manifest_sha256,
            "raw_capture_receipt_sha256": capture.capture_receipt_sha256,
            "raw_member_count": 2,
            "raw_store_schema_version": RAW_ARTIFACT_SCHEMA_VERSION,
            "sources": sources,
        },
    }


def _receipt_id(
    store: RawArtifactStore, *, product: str, fetch_id: str
) -> str:
    return store._receipt_id(  # noqa: SLF001 - immutable identity contract
        provider=PROVIDER,
        product=product,
        fetch_id=fetch_id,
    )


def _capture_id(store: RawArtifactStore, job_id: str) -> str:
    return store._capture_id(  # noqa: SLF001 - immutable identity contract
        provider=PROVIDER,
        product=CAPTURE_PRODUCT,
        fetch_id=f"{job_id}:capture",
    )


def _load_receipt_bytes(
    store: RawArtifactStore, *, product: str, fetch_id: str
) -> tuple[RawArtifactReceipt, bytes] | None:
    return store.load_receipt_bytes(
        _receipt_id(store, product=product, fetch_id=fetch_id)
    )


def _capture_locked(
    job_id: str,
    authority: _LocationAuthority,
    *,
    raw_store: RawArtifactStore,
    deadline: float,
    opener: Callable[..., Any] | None,
    wall_clock: Callable[[], float],
    fault_injector: Callable[[str], None] | None,
) -> dict[str, Any]:
    complete = raw_store.load_capture_bundle_bytes(_capture_id(raw_store, job_id))
    if complete is not None:
        capture, receipts, payloads = complete
        if set(receipts) != {FORECAST_ROLE, IMAGE_ROLE} or set(payloads) != {
            FORECAST_ROLE,
            IMAGE_ROLE,
        }:
            raise KmiAppv4CorruptionError(
                "duurzame KMI app-v4 capture mist zijn exacte rollen"
            )
        forecast = receipts[FORECAST_ROLE]
        image = receipts[IMAGE_ROLE]
        selection = _validate_forecast_receipt(
            forecast,
            payloads[FORECAST_ROLE],
            job_id=job_id,
            authority=authority,
        )
        _validate_image_receipt(
            image,
            payloads[IMAGE_ROLE],
            job_id=job_id,
            selection=selection,
        )
        _validate_capture(
            capture,
            job_id=job_id,
            forecast=forecast,
            image=image,
            selection=selection,
        )
        return _result(capture, forecast, image, selection)

    forecast_loaded = _load_receipt_bytes(
        raw_store,
        product=FORECAST_PRODUCT,
        fetch_id=f"{job_id}:forecast_json",
    )
    if forecast_loaded is None:
        request_at = _wall_time(wall_clock)
        local_date = datetime.fromtimestamp(
            request_at, tz=_BRUSSELS
        ).date().isoformat()
        forecast_binding = _forecast_binding(authority, local_date=local_date)
        response = _fetch(
            forecast_binding,
            maximum=FORECAST_MAX_BYTES,
            deadline=deadline,
            opener=opener,
            wall_clock=wall_clock,
        )
        content_sha256 = hashlib.sha256(response.payload).hexdigest()
        forecast = raw_store.store(
            provider=PROVIDER,
            product=FORECAST_PRODUCT,
            fetch_id=f"{job_id}:forecast_json",
            content_type=UNINTERPRETED_CONTENT_TYPE,
            received_at=response.received_at,
            upstream_id=_forecast_upstream_id(
                forecast_binding, content_sha256
            ),
            provenance=_provenance(
                forecast_binding,
                response,
                provenance_kind=FORECAST_PROVENANCE_KIND,
            ),
            payload=response.payload,
        )
        forecast_payload = response.payload
        if fault_injector is not None:
            fault_injector("kmi_appv4.after_forecast_store")
    else:
        forecast, forecast_payload = forecast_loaded

    # This is the first payload inspection.  The durable forecast receipt and
    # exact object already exist, including for empty or malformed HTTP 2xx.
    selection = _validate_forecast_receipt(
        forecast,
        forecast_payload,
        job_id=job_id,
        authority=authority,
    )
    if fault_injector is not None:
        fault_injector("kmi_appv4.after_link_selection")

    image_loaded = _load_receipt_bytes(
        raw_store,
        product=IMAGE_PRODUCT,
        fetch_id=f"{job_id}:radar_png",
    )
    if image_loaded is None:
        image_response = _fetch(
            selection.binding,
            maximum=IMAGE_MAX_BYTES,
            deadline=deadline,
            opener=opener,
            wall_clock=wall_clock,
        )
        image_sha256 = hashlib.sha256(image_response.payload).hexdigest()
        image = raw_store.store(
            provider=PROVIDER,
            product=IMAGE_PRODUCT,
            fetch_id=f"{job_id}:radar_png",
            content_type=UNINTERPRETED_CONTENT_TYPE,
            received_at=image_response.received_at,
            upstream_id=_image_upstream_id(
                selection.binding, image_sha256
            ),
            provenance=_provenance(
                selection.binding,
                image_response,
                provenance_kind=IMAGE_PROVENANCE_KIND,
            ),
            payload=image_response.payload,
        )
        image_payload = image_response.payload
        if fault_injector is not None:
            fault_injector("kmi_appv4.after_image_store")
    else:
        image, image_payload = image_loaded
    _validate_image_receipt(
        image,
        image_payload,
        job_id=job_id,
        selection=selection,
    )

    capture = raw_store.commit_capture(
        provider=PROVIDER,
        product=CAPTURE_PRODUCT,
        fetch_id=f"{job_id}:capture",
        upstream_capture_id=_capture_upstream_id(
            forecast, image, selection
        ),
        product_timestamp=selection.product_timestamp,
        members={
            FORECAST_ROLE: forecast.receipt_id,
            IMAGE_ROLE: image.receipt_id,
        },
    )
    if fault_injector is not None:
        fault_injector("kmi_appv4.after_capture_commit")
    _validate_capture(
        capture,
        job_id=job_id,
        forecast=forecast,
        image=image,
        selection=selection,
    )
    return _result(capture, forecast, image, selection)


def capture_kmi_appv4_visual_raw(
    job: object,
    *,
    raw_store: RawArtifactStore,
    deadline_at: float | None = None,
    opener: Callable[..., Any] | None = None,
    _wall_clock: Callable[[], float] = time.time,
    _fault_injector: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    """Capture forecast + selected image exactly, without weather decoding."""

    job_id, payload = _job(job)
    authority = _location_authority(payload)
    deadline = _deadline(deadline_at)
    if not isinstance(raw_store, RawArtifactStore):
        raise KmiAppv4ContractError("KMI app-v4 raw_store heeft verkeerd type")
    raw_store.initialize()
    if raw_store.limits.max_object_bytes < IMAGE_MAX_BYTES:
        raise KmiAppv4CapacityError(
            "KMI app-v4 raw store objectgrens is kleiner dan het beeldbudget"
        )
    if raw_store.limits.max_total_bytes < MAXIMUM_TRANSPORT_BYTES:
        raise KmiAppv4CapacityError(
            "KMI app-v4 raw store totaalgrens is kleiner dan het capturebudget"
        )
    if raw_store.limits.max_capture_members < MAXIMUM_ARTIFACTS:
        raise KmiAppv4CapacityError(
            "KMI app-v4 raw store kan de vereiste rolset niet bevatten"
        )
    with _CAPTURE_LOCK:
        return _capture_locked(
            job_id,
            authority,
            raw_store=raw_store,
            deadline=deadline,
            opener=opener,
            wall_clock=_wall_clock,
            fault_injector=_fault_injector,
        )


def _recovery_sha256(value: object, label: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise KmiAppv4CorruptionError(
            f"duurzame KMI app-v4 {label} is ongeldig"
        )
    return value


def _recovery_number(value: object, label: str) -> float:
    try:
        return _finite_number(
            value,
            name=label,
            minimum=0.0,
            maximum=float(2**53),
        )
    except KmiAppv4ContractError as err:
        raise KmiAppv4CorruptionError(
            f"duurzame KMI app-v4 {label} is ongeldig"
        ) from err


def _validate_recovery_job_id(job_id: object) -> str:
    if not isinstance(job_id, str) or _JOB_ID_RE.fullmatch(job_id) is None:
        raise KmiAppv4CorruptionError(
            "KMI app-v4 recoveryjob heeft een onveilige identity"
        )
    return job_id


def _raw_recovery_binding(
    receipt: RawArtifactReceipt,
    *,
    job_id: str,
    role: str,
) -> tuple[_RequestBinding, str, int, str]:
    """Reconstruct one secret-free receipt authority from durable hashes."""

    if not isinstance(receipt, RawArtifactReceipt):
        raise KmiAppv4CorruptionError(
            "KMI app-v4 recoveryreceipt heeft het verkeerde type"
        )
    job_id = _validate_recovery_job_id(job_id)
    provenance = receipt.provenance
    base_keys = {
        "access_class",
        "capture_stage",
        "collector",
        "collector_version",
        "content_encoding",
        "endpoint_host",
        "endpoint_path_sha256",
        "legal_advice",
        "meteorological_parse_attempted",
        "operational_admission",
        "payload_parse_scope",
        "provider_rights_claim",
        "provenance_kind",
        "query_authority_sha256",
        "representation",
        "request_sha256",
        "retention_activation_requirement_id",
        "retention_right_claim",
        "rights_review_status",
        "semantic_status",
        "transport",
        "transport_sha256",
        "weather_state",
    }
    extras = (
        {"request_local_date"}
        if role == FORECAST_ROLE
        else {"product_timestamp", "selected_uri_path_sha256"}
        if role == IMAGE_ROLE
        else set()
    )
    if role not in {FORECAST_ROLE, IMAGE_ROLE} or set(provenance) != base_keys | extras:
        raise KmiAppv4CorruptionError(
            "duurzame KMI app-v4 recoveryrol of provenance wijkt af"
        )
    transport = provenance.get("transport")
    if type(transport) is not dict or set(transport) != {
        "body_sha256",
        "body_size_bytes",
        "declared_length",
        "final_target_sha256",
        "received_at",
        "reported_media_type",
        "request_sha256",
        "response_headers_sha256",
        "role",
        "status_code",
        "transport_contract",
    }:
        raise KmiAppv4CorruptionError(
            "duurzame KMI app-v4 recoverytransport wijkt af"
        )
    query_sha256 = _recovery_sha256(
        provenance.get("query_authority_sha256"), "query-authoritychecksum"
    )
    request_sha256 = _recovery_sha256(
        provenance.get("request_sha256"), "requestchecksum"
    )
    endpoint_path_sha256 = _recovery_sha256(
        provenance.get("endpoint_path_sha256"), "endpointpadchecksum"
    )
    if role == FORECAST_ROLE:
        local_date = _validated_local_date(provenance.get("request_local_date"))
        if (
            provenance.get("endpoint_host") != FORECAST_HOST
            or endpoint_path_sha256 != _sha256_text(FORECAST_PATH)
        ):
            raise KmiAppv4CorruptionError(
                "duurzame KMI app-v4 forecastauthority wijkt af"
            )
        binding = _RequestBinding(
            role=FORECAST_ROLE,
            url="",
            host=FORECAST_HOST,
            path=FORECAST_PATH,
            request_sha256=request_sha256,
            request_local_date=local_date,
            query_authority_sha256=query_sha256,
        )
        product = FORECAST_PRODUCT
        maximum = FORECAST_MAX_BYTES
        provenance_kind = FORECAST_PROVENANCE_KIND
    else:
        host = provenance.get("endpoint_host")
        selected_path_sha256 = _recovery_sha256(
            provenance.get("selected_uri_path_sha256"),
            "geselecteerde-padchecksum",
        )
        product_timestamp = _recovery_number(
            provenance.get("product_timestamp"), "producttijd"
        )
        if host not in ALLOWED_IMAGE_HOSTS or endpoint_path_sha256 != selected_path_sha256:
            raise KmiAppv4CorruptionError(
                "duurzame KMI app-v4 beeldauthority wijkt af"
            )
        binding = _RequestBinding(
            role=IMAGE_ROLE,
            url="",
            host=str(host),
            path="",
            request_sha256=request_sha256,
            request_local_date=None,
            query_authority_sha256=query_sha256,
            selected_uri_path_sha256=selected_path_sha256,
            product_timestamp=product_timestamp,
        )
        product = IMAGE_PRODUCT
        maximum = IMAGE_MAX_BYTES
        provenance_kind = IMAGE_PROVENANCE_KIND
    _validate_fixed_provenance(
        receipt,
        binding=binding,
        provenance_kind=provenance_kind,
        endpoint_path_sha256=endpoint_path_sha256,
    )
    if (
        receipt.provider != PROVIDER
        or receipt.product != product
        or receipt.fetch_id != f"{job_id}:{role}"
        or receipt.content_type != UNINTERPRETED_CONTENT_TYPE
        or isinstance(receipt.size_bytes, bool)
        or not isinstance(receipt.size_bytes, int)
        or not 0 <= receipt.size_bytes <= maximum
        or not isinstance(receipt.content_sha256, str)
        or _SHA256_RE.fullmatch(receipt.content_sha256) is None
        or not isinstance(receipt.receipt_sha256, str)
        or _SHA256_RE.fullmatch(receipt.receipt_sha256) is None
    ):
        raise KmiAppv4CorruptionError(
            "duurzame KMI app-v4 recoveryreceipt wijkt af"
        )
    expected_upstream = (
        _forecast_upstream_id(binding, receipt.content_sha256)
        if role == FORECAST_ROLE
        else _image_upstream_id(binding, receipt.content_sha256)
    )
    expected_receipt_id = RawArtifactStore._receipt_id(  # noqa: SLF001
        provider=PROVIDER,
        product=product,
        fetch_id=receipt.fetch_id,
    )
    if receipt.upstream_id != expected_upstream or receipt.receipt_id != expected_receipt_id:
        raise KmiAppv4CorruptionError(
            "duurzame KMI app-v4 recoveryidentity wijkt af"
        )
    transport_sha256 = _transport_from_receipt(
        receipt,
        binding=binding,
        maximum=maximum,
    )
    return binding, product, maximum, transport_sha256


def validate_kmi_appv4_receipt_for_raw_recovery(
    receipt: RawArtifactReceipt,
    *,
    job_id: str,
    role: str,
) -> str:
    """Reprove one receipt using only its secret-free durable authority."""

    return _raw_recovery_binding(
        receipt,
        job_id=job_id,
        role=role,
    )[3]


def validate_kmi_appv4_capture_for_raw_recovery(
    capture: RawArtifactCaptureReceipt,
    receipts: Mapping[str, RawArtifactReceipt],
    payloads: Mapping[str, bytes],
    *,
    job_id: str,
) -> dict[str, Any]:
    """Reprove a complete exact bundle without coordinates or query secrets."""

    job_id = _validate_recovery_job_id(job_id)
    if (
        not isinstance(capture, RawArtifactCaptureReceipt)
        or not isinstance(receipts, Mapping)
        or not isinstance(payloads, Mapping)
        or set(receipts) != {FORECAST_ROLE, IMAGE_ROLE}
        or set(payloads) != {FORECAST_ROLE, IMAGE_ROLE}
    ):
        raise KmiAppv4CorruptionError(
            "KMI app-v4 raw recovery mist een complete rolset"
        )
    forecast = receipts[FORECAST_ROLE]
    image = receipts[IMAGE_ROLE]
    forecast_payload = payloads[FORECAST_ROLE]
    image_payload = payloads[IMAGE_ROLE]
    if not isinstance(forecast_payload, bytes) or not isinstance(image_payload, bytes):
        raise KmiAppv4CorruptionError(
            "KMI app-v4 raw recovery mist exacte payloadbytes"
        )
    forecast_binding = _raw_recovery_binding(
        forecast,
        job_id=job_id,
        role=FORECAST_ROLE,
    )[0]
    _raw_recovery_binding(
        image,
        job_id=job_id,
        role=IMAGE_ROLE,
    )
    if (
        forecast.size_bytes != len(forecast_payload)
        or forecast.content_sha256 != hashlib.sha256(forecast_payload).hexdigest()
        or image.size_bytes != len(image_payload)
        or image.content_sha256 != hashlib.sha256(image_payload).hexdigest()
    ):
        raise KmiAppv4CorruptionError(
            "KMI app-v4 raw recoverypayload wijkt af"
        )
    selection = _select_frame(
        forecast_payload,
        query_authority_sha256=forecast_binding.query_authority_sha256,
        selection_at=forecast.received_at,
    )
    _validate_image_receipt(
        image,
        image_payload,
        job_id=job_id,
        selection=selection,
    )
    _validate_capture(
        capture,
        job_id=job_id,
        forecast=forecast,
        image=image,
        selection=selection,
    )
    return _result(capture, forecast, image, selection)


def validate_kmi_appv4_capture_for_recovery(
    capture: RawArtifactCaptureReceipt,
    receipts: Mapping[str, RawArtifactReceipt],
    payloads: Mapping[str, bytes],
    *,
    job_id: str,
    handler_payload: Mapping[str, Any],
) -> dict[str, Any]:
    """Reprove one complete bundle without trusting serialized result data."""

    if (
        not isinstance(capture, RawArtifactCaptureReceipt)
        or not isinstance(receipts, Mapping)
        or not isinstance(payloads, Mapping)
        or set(receipts) != {FORECAST_ROLE, IMAGE_ROLE}
        or set(payloads) != {FORECAST_ROLE, IMAGE_ROLE}
        or type(handler_payload) is not dict
    ):
        raise KmiAppv4CorruptionError(
            "KMI app-v4 recovery mist een complete rolset"
        )
    if not isinstance(job_id, str) or _JOB_ID_RE.fullmatch(job_id) is None:
        raise KmiAppv4CorruptionError(
            "KMI app-v4 recoveryjob heeft een onveilige identity"
        )
    authority = _location_authority(dict(handler_payload))
    forecast = receipts[FORECAST_ROLE]
    image = receipts[IMAGE_ROLE]
    forecast_payload = payloads[FORECAST_ROLE]
    image_payload = payloads[IMAGE_ROLE]
    if not isinstance(forecast_payload, bytes) or not isinstance(image_payload, bytes):
        raise KmiAppv4CorruptionError(
            "KMI app-v4 recovery mist exacte payloadbytes"
        )
    selection = _validate_forecast_receipt(
        forecast,
        forecast_payload,
        job_id=job_id,
        authority=authority,
    )
    _validate_image_receipt(
        image,
        image_payload,
        job_id=job_id,
        selection=selection,
    )
    _validate_capture(
        capture,
        job_id=job_id,
        forecast=forecast,
        image=image,
        selection=selection,
    )
    return _result(capture, forecast, image, selection)


def kmi_appv4_child_handler_contracts() -> dict[str, dict[str, Any]]:
    """Return a fresh workload-keyed drop-in handler metadata mapping."""

    return {WORKLOAD_ID: HANDLER_SPEC.child_handler_mapping()}


def kmi_appv4_recovery_binding_metadata() -> dict[str, dict[str, Any]]:
    """Return a fresh handler-keyed drop-in recovery metadata mapping."""

    return {HANDLER_ID: HANDLER_SPEC.recovery_mapping()}


# Read-only catalogue for consumers that prefer discovery without helper calls.
HANDLER_SPECS_BY_ID: Mapping[str, KmiAppv4HandlerSpec] = MappingProxyType({
    HANDLER_ID: HANDLER_SPEC,
})
HANDLER_SPECS_BY_WORKLOAD: Mapping[str, KmiAppv4HandlerSpec] = MappingProxyType({
    WORKLOAD_ID: HANDLER_SPEC,
})


__all__ = [
    "CAPTURE_DEADLINE_SECONDS",
    "CAPTURE_PRODUCT",
    "CAPTURE_PROVENANCE_KIND",
    "CONTRACT",
    "FORECAST_MAX_BYTES",
    "FORECAST_PRODUCT",
    "FORECAST_PROVENANCE_KIND",
    "FORECAST_ROLE",
    "HANDLER_ID",
    "HANDLER_SPEC",
    "HANDLER_SPECS_BY_ID",
    "HANDLER_SPECS_BY_WORKLOAD",
    "IMAGE_MAX_BYTES",
    "IMAGE_PRODUCT",
    "IMAGE_PROVENANCE_KIND",
    "IMAGE_ROLE",
    "KmiAppv4CapacityError",
    "KmiAppv4ContractError",
    "KmiAppv4CorruptionError",
    "KmiAppv4HandlerSpec",
    "KmiAppv4RawError",
    "KmiAppv4RoleSpec",
    "KmiAppv4SelectionError",
    "KmiAppv4TransportError",
    "MAXIMUM_ARTIFACTS",
    "MAXIMUM_TRANSPORT_BYTES",
    "PUBLIC_REGION_QUERY_CONTRACT",
    "RESULT_CONTRACT",
    "RETENTION_REQUIREMENT_ID",
    "UNINTERPRETED_CONTENT_TYPE",
    "WORKLOAD_ID",
    "capture_kmi_appv4_visual_raw",
    "kmi_appv4_child_handler_contracts",
    "kmi_appv4_recovery_binding_metadata",
    "validate_kmi_appv4_capture_for_raw_recovery",
    "validate_kmi_appv4_capture_for_recovery",
    "validate_kmi_appv4_receipt_for_raw_recovery",
]
