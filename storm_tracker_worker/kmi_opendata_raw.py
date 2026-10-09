"""Exact-raw KMI OpenData GeoServer capture.

The capture boundary in this module is deliberately narrower than a weather
decoder.  It accepts only allowlisted KMI WFS/WCS requests, streams one bounded
HTTP response, and makes the exact response body durable before any caller can
interpret it.  Content type or meteorological semantics are recorded as
transport facts only; an unexpected but otherwise valid HTTP 200 response is
still retained for later analysis.

No Home Assistant, MQTT, credential, decoder, or admission code belongs here.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import re
import threading
import time
from typing import Any, Callable, Mapping, Sequence
from urllib.parse import quote, urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

from .raw_artifacts import (
    NATIVE_CAPTURE_STAGE,
    NATIVE_REPRESENTATION,
    RAW_ARTIFACT_SCHEMA_VERSION,
    RawArtifactCaptureReceipt,
    RawArtifactReceipt,
    RawArtifactStore,
)


KMI_OPENDATA_ENDPOINT = "https://opendata.meteo.be/geoserver/ows"
KMI_OPENDATA_HOST = "opendata.meteo.be"
KMI_OPENDATA_PATH = "/geoserver/ows"

PROVIDER = "kmi"
METHOD = "kmi_opendata_geoserver_exact_raw_capture_v1"
COLLECTOR = "kmi_opendata_geoserver_exact_raw"
COLLECTOR_VERSION = "worker-kmi-opendata-exact-raw-v1"
CANONICAL_REQUEST_SCHEMA = "stv3-kmi-opendata-canonical-request-v1"
TRANSPORT_RECEIPT_SCHEMA = "stv3-kmi-opendata-transport-receipt-v1"
DESCRIPTOR_SCHEMA = "stv3-kmi-opendata-layer-descriptor-v1"

AWS_10MIN_LAYER = "aws:aws_10min"
PUBLIC_RADAR_COVERAGE = "radar__belgian_rainfall_composite"

AWS_10MIN_RESPONSE_PRODUCT = "kmi_aws_10min_wfs_exact_response"
AWS_10MIN_CAPTURE_PRODUCT = "kmi_aws_10min_wfs_exact_capture"
PUBLIC_RADAR_RESPONSE_PRODUCT = "kmi_public_radar_wcs_exact_response"
PUBLIC_RADAR_CAPTURE_PRODUCT = "kmi_public_radar_wcs_exact_capture"

MAX_HTTP_HEADER_CHARS = 8_192
MAX_REQUEST_URL_CHARS = 8_192
MAX_QUERY_VALUE_CHARS = 4_096
MAX_RESPONSE_BYTES = 96 * 1024 * 1024
AWS_10MIN_MAX_RESPONSE_BYTES = 32 * 1024 * 1024
PUBLIC_RADAR_MAX_RESPONSE_BYTES = 64 * 1024 * 1024
STREAM_CHUNK_BYTES = 128 * 1024
HTTP_IO_TIMEOUT_SECONDS = 20.0
CAPTURE_TOTAL_DEADLINE_SECONDS = 50.0
MAX_AWS_WINDOW_SECONDS = 24 * 60 * 60

RAW_SHARE_ROOT = Path("/share/storm_tracker_worker")
RAW_ARTIFACT_ROOT = RAW_SHARE_ROOT / "raw" / "kmi_opendata"

_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,255}$")
_JOB_ID_RE = re.compile(r"^[^\x00-\x1f\x7f]{1,440}$")
_CONTENT_TYPE_RE = re.compile(
    r"^[A-Za-z0-9!#$&^_.+-]+/[A-Za-z0-9!#$&^_.+-]+$"
)
_SERVICE_PARAMETERS = {
    "WFS": frozenset({
        "BBOX",
        "COUNT",
        "CQL_FILTER",
        "OUTPUTFORMAT",
        "PROPERTYNAME",
        "RESULTTYPE",
        "SORTBY",
        "SRSNAME",
        "STARTINDEX",
    }),
    "WCS": frozenset({
        "FORMAT",
        "MEDIATYPE",
        "OUTPUTCRS",
        "SCALEAXES",
        "SUBSET",
        "SUBSETTINGCRS",
    }),
}
_BASE_PARAMETERS = frozenset({
    "SERVICE", "VERSION", "REQUEST", "TYPENAMES", "COVERAGEID"
})
_LAYER_POLICIES = {
    ("WFS", AWS_10MIN_LAYER): {
        "version": "2.0.0",
        "operation": "GetFeature",
        "layer_parameter": "TYPENAMES",
        "response_product": AWS_10MIN_RESPONSE_PRODUCT,
        "capture_product": AWS_10MIN_CAPTURE_PRODUCT,
    },
    ("WCS", PUBLIC_RADAR_COVERAGE): {
        "version": "2.0.1",
        "operation": "GetCoverage",
        "layer_parameter": "COVERAGEID",
        "response_product": PUBLIC_RADAR_RESPONSE_PRODUCT,
        "capture_product": PUBLIC_RADAR_CAPTURE_PRODUCT,
    },
}

_RAW_STORE: RawArtifactStore | None = None
_RAW_STORE_LOCK = threading.Lock()
_CAPTURE_LOCK = threading.Lock()


class KmiOpenDataError(RuntimeError):
    """Base capture failure; it never represents a weather observation."""


class KmiOpenDataContractError(KmiOpenDataError):
    """A request or transport fact left the explicit KMI allowlist."""


class KmiOpenDataCapacityError(KmiOpenDataError):
    """A declared or streamed response exceeded its exact byte boundary."""


class KmiOpenDataDeadlineError(KmiOpenDataError):
    """The bounded capture deadline elapsed."""


class KmiOpenDataCorruptionError(KmiOpenDataError):
    """Durable raw evidence conflicts with its request/transport receipt."""


@dataclass(frozen=True, slots=True)
class KmiLayerDescriptor:
    """One code-owned, allowlisted KMI GeoServer response description.

    ``parameters`` contains only operation-specific KVP entries.  Endpoint,
    service, version, operation, and layer identity are added canonically by
    :func:`canonical_request`; callers cannot override them through query data.
    Repeated keys are supported for WCS ``SUBSET`` parameters.
    """

    descriptor_id: str
    service: str
    layer: str
    parameters: tuple[tuple[str, str], ...]
    maximum_bytes: int
    expected_content_types: tuple[str, ...]
    endpoint: str = KMI_OPENDATA_ENDPOINT


@dataclass(frozen=True, slots=True)
class KmiCanonicalRequest:
    method: str
    url: str
    service: str
    layer: str
    headers: tuple[tuple[str, str], ...]

    def to_mapping(self) -> dict[str, object]:
        return {
            "schema": CANONICAL_REQUEST_SCHEMA,
            "method": self.method,
            "url": self.url,
            "service": self.service,
            "layer": self.layer,
            "headers": {key: value for key, value in self.headers},
        }

    @property
    def request_sha256(self) -> str:
        return _sha256_json(self.to_mapping())


@dataclass(frozen=True, slots=True)
class KmiHttpResponse:
    """One exact HTTP response body and its bounded transport facts."""

    payload: bytes
    status_code: int
    final_url: str
    content_type: str
    content_type_header: str | None
    content_encoding: str
    declared_content_length: int | None
    etag: str | None
    last_modified: str | None
    received_at: float


class _RejectRedirects(HTTPRedirectHandler):
    def redirect_request(self, *_args: Any, **_kwargs: Any) -> None:
        raise KmiOpenDataContractError("KMI GeoServer HTTP-redirect is verboden")


def _canonical_json(value: Mapping[str, object]) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _sha256_json(value: Mapping[str, object]) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _safe_identifier(value: object, name: str) -> str:
    if not isinstance(value, str) or _IDENTIFIER_RE.fullmatch(value) is None:
        raise KmiOpenDataContractError(f"KMI descriptorveld {name} is ongeldig")
    return value


def _validate_endpoint(endpoint: object) -> str:
    if not isinstance(endpoint, str) or endpoint != endpoint.strip():
        raise KmiOpenDataContractError("KMI GeoServer-endpoint is ongeldig")
    try:
        parts = urlsplit(endpoint)
        port = parts.port
    except ValueError as exc:
        raise KmiOpenDataContractError(
            "KMI GeoServer-endpoint is ongeldig"
        ) from exc
    if (
        parts.scheme != "https"
        or parts.netloc != KMI_OPENDATA_HOST
        or parts.hostname != KMI_OPENDATA_HOST
        or port is not None
        or parts.username is not None
        or parts.password is not None
        or parts.path != KMI_OPENDATA_PATH
        or parts.query
        or parts.fragment
    ):
        raise KmiOpenDataContractError(
            "KMI GeoServer-endpoint valt buiten HTTPS-host/path-allowlist"
        )
    return endpoint


def _normalize_content_type(value: object) -> str:
    if not isinstance(value, str):
        raise KmiOpenDataContractError("KMI expected Content-Type is ongeldig")
    result = value.strip().lower()
    if len(result) > 256 or _CONTENT_TYPE_RE.fullmatch(result) is None:
        raise KmiOpenDataContractError("KMI expected Content-Type is ongeldig")
    return result


def _layer_policy(descriptor: KmiLayerDescriptor) -> Mapping[str, str]:
    if not isinstance(descriptor, KmiLayerDescriptor):
        raise KmiOpenDataContractError("KMI capture mist een layerdescriptor")
    _safe_identifier(descriptor.descriptor_id, "descriptor_id")
    if (
        not isinstance(descriptor.service, str)
        or descriptor.service not in _SERVICE_PARAMETERS
    ):
        raise KmiOpenDataContractError("KMI service valt buiten WFS/WCS-allowlist")
    if not isinstance(descriptor.layer, str):
        raise KmiOpenDataContractError("KMI layer is ongeldig")
    policy = _LAYER_POLICIES.get((descriptor.service, descriptor.layer))
    if policy is None:
        raise KmiOpenDataContractError("KMI service/layer valt buiten allowlist")
    _validate_endpoint(descriptor.endpoint)
    if (
        isinstance(descriptor.maximum_bytes, bool)
        or not isinstance(descriptor.maximum_bytes, int)
        or not 0 < descriptor.maximum_bytes <= MAX_RESPONSE_BYTES
    ):
        raise KmiOpenDataContractError("KMI responsebytegrens is ongeldig")
    if (
        not isinstance(descriptor.expected_content_types, tuple)
        or not descriptor.expected_content_types
        or len(descriptor.expected_content_types) > 16
        or len(set(descriptor.expected_content_types))
        != len(descriptor.expected_content_types)
    ):
        raise KmiOpenDataContractError("KMI expected Content-Type-set is ongeldig")
    for content_type in descriptor.expected_content_types:
        if _normalize_content_type(content_type) != content_type:
            raise KmiOpenDataContractError(
                "KMI expected Content-Type-set is niet canoniek"
            )
    return policy


def _validated_parameters(
    descriptor: KmiLayerDescriptor,
) -> tuple[tuple[str, str], ...]:
    _layer_policy(descriptor)
    if not isinstance(descriptor.parameters, tuple) or len(descriptor.parameters) > 32:
        raise KmiOpenDataContractError("KMI queryparameters zijn onbegrensd")
    permitted = _SERVICE_PARAMETERS[descriptor.service]
    normalized: list[tuple[str, str]] = []
    for pair in descriptor.parameters:
        if (
            not isinstance(pair, tuple)
            or len(pair) != 2
            or not all(isinstance(item, str) for item in pair)
        ):
            raise KmiOpenDataContractError("KMI queryparameter is ongeldig")
        key, value = pair
        if key not in permitted or key in _BASE_PARAMETERS:
            raise KmiOpenDataContractError(
                "KMI queryparameter valt buiten service-allowlist"
            )
        if (
            not value
            or value != value.strip()
            or len(value) > MAX_QUERY_VALUE_CHARS
            or any(ord(character) < 0x20 or ord(character) == 0x7F for character in value)
        ):
            raise KmiOpenDataContractError("KMI querywaarde is ongeldig")
        normalized.append((key, value))
    key_counts = {
        key: sum(1 for candidate, _value in normalized if candidate == key)
        for key, _value in normalized
    }
    if descriptor.service == "WFS" and any(
        count != 1 for count in key_counts.values()
    ):
        raise KmiOpenDataContractError("KMI WFS query bevat dubbele parameters")
    if descriptor.service == "WCS" and any(
        count != 1 and key != "SUBSET" for key, count in key_counts.items()
    ):
        raise KmiOpenDataContractError("KMI WCS query bevat dubbele parameters")
    if key_counts.get("SUBSET", 0) > 8:
        raise KmiOpenDataContractError("KMI WCS bevat te veel SUBSET-parameters")
    if descriptor.service == "WFS":
        count_values = [value for key, value in normalized if key == "COUNT"]
        if len(count_values) > 1 or any(
            not value.isascii()
            or not value.isdecimal()
            or not 1 <= int(value) <= 50_000
            for value in count_values
        ):
            raise KmiOpenDataContractError("KMI WFS COUNT is ongeldig")
    return tuple(sorted(normalized))


def descriptor_document(descriptor: KmiLayerDescriptor) -> dict[str, object]:
    """Return the canonical, secret-free descriptor authority."""
    policy = _layer_policy(descriptor)
    parameters = _validated_parameters(descriptor)
    return {
        "schema": DESCRIPTOR_SCHEMA,
        "descriptor_id": descriptor.descriptor_id,
        "endpoint": descriptor.endpoint,
        "service": descriptor.service,
        "version": policy["version"],
        "operation": policy["operation"],
        "layer_parameter": policy["layer_parameter"],
        "layer": descriptor.layer,
        "parameters": [[key, value] for key, value in parameters],
        "maximum_bytes": descriptor.maximum_bytes,
        "expected_content_types": list(descriptor.expected_content_types),
        "response_product": policy["response_product"],
        "capture_product": policy["capture_product"],
    }


def _descriptor_from_document(document: object) -> KmiLayerDescriptor:
    expected = {
        "schema",
        "descriptor_id",
        "endpoint",
        "service",
        "version",
        "operation",
        "layer_parameter",
        "layer",
        "parameters",
        "maximum_bytes",
        "expected_content_types",
        "response_product",
        "capture_product",
    }
    if type(document) is not dict or set(document) != expected:
        raise KmiOpenDataCorruptionError("KMI recoverydescriptor is onvolledig")
    parameters = document["parameters"]
    content_types = document["expected_content_types"]
    if (
        type(parameters) is not list
        or len(parameters) > 32
        or any(
            type(pair) is not list
            or len(pair) != 2
            or any(not isinstance(item, str) for item in pair)
            for pair in parameters
        )
        or type(content_types) is not list
        or not content_types
        or len(content_types) > 16
        or any(not isinstance(item, str) for item in content_types)
    ):
        raise KmiOpenDataCorruptionError("KMI recoverydescriptor is ongeldig")
    descriptor = KmiLayerDescriptor(
        descriptor_id=document["descriptor_id"],
        service=document["service"],
        layer=document["layer"],
        parameters=tuple((pair[0], pair[1]) for pair in parameters),
        maximum_bytes=document["maximum_bytes"],
        expected_content_types=tuple(content_types),
        endpoint=document["endpoint"],
    )
    try:
        observed = descriptor_document(descriptor)
    except KmiOpenDataError as exc:
        raise KmiOpenDataCorruptionError(
            "KMI recoverydescriptor valt buiten de allowlist"
        ) from exc
    if observed != document:
        raise KmiOpenDataCorruptionError("KMI recoverydescriptor is niet canoniek")
    return descriptor


def canonical_request(descriptor: KmiLayerDescriptor) -> KmiCanonicalRequest:
    """Build the only URL that may be sent for ``descriptor``."""
    policy = _layer_policy(descriptor)
    query: list[tuple[str, str]] = [
        ("SERVICE", descriptor.service),
        ("VERSION", policy["version"]),
        ("REQUEST", policy["operation"]),
        (policy["layer_parameter"], descriptor.layer),
        *_validated_parameters(descriptor),
    ]
    query.sort()
    url = descriptor.endpoint + "?" + urlencode(
        query,
        doseq=True,
        quote_via=quote,
        safe="",
    )
    if len(url) > MAX_REQUEST_URL_CHARS:
        raise KmiOpenDataContractError("KMI canonical request-URL is te lang")
    return KmiCanonicalRequest(
        method="GET",
        url=url,
        service=descriptor.service,
        layer=descriptor.layer,
        headers=(
            ("Accept", "*/*"),
            ("Accept-Encoding", "identity"),
            ("User-Agent", "StormTrackerWorker/1.16 KMIExactRaw/1"),
        ),
    )


KMI_AWS_10MIN_LATEST = KmiLayerDescriptor(
    descriptor_id="kmi_aws_10min_latest_bounded_v1",
    service="WFS",
    layer=AWS_10MIN_LAYER,
    parameters=(
        ("COUNT", "512"),
        ("OUTPUTFORMAT", "application/json"),
        ("SORTBY", "timestamp D"),
    ),
    maximum_bytes=AWS_10MIN_MAX_RESPONSE_BYTES,
    expected_content_types=("application/geo+json", "application/json"),
)


def _utc_second(value: datetime, name: str) -> str:
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
        or value.microsecond != 0
    ):
        raise KmiOpenDataContractError(
            f"KMI {name} moet een timezone-aware seconde zijn"
        )
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def aws_10min_window_descriptor(
    start_at: datetime,
    end_at: datetime,
) -> KmiLayerDescriptor:
    """Describe a complete half-open AWS interval, without a feature limit."""
    start_text = _utc_second(start_at, "window_start")
    end_text = _utc_second(end_at, "window_end")
    seconds = (
        end_at.astimezone(timezone.utc) - start_at.astimezone(timezone.utc)
    ).total_seconds()
    if not 0 < seconds <= MAX_AWS_WINDOW_SECONDS:
        raise KmiOpenDataContractError("KMI AWS-window valt buiten 24 uur")
    cql_filter = (
        f"timestamp >= '{start_text}' AND timestamp < '{end_text}'"
    )
    return replace(
        KMI_AWS_10MIN_LATEST,
        descriptor_id="kmi_aws_10min_half_open_window_v1",
        parameters=(
            ("CQL_FILTER", cql_filter),
            ("OUTPUTFORMAT", "application/json"),
            ("SORTBY", "timestamp A,code A"),
        ),
    )


def public_radar_time_descriptor(nominal_at: datetime) -> KmiLayerDescriptor:
    """Describe one raw public KMI WCS coverage response at an exact time."""
    nominal = _utc_second(nominal_at, "radar_nominal_at")
    return KmiLayerDescriptor(
        descriptor_id="kmi_public_radar_wcs_time_v1",
        service="WCS",
        layer=PUBLIC_RADAR_COVERAGE,
        parameters=(("FORMAT", "image/tiff"), ("SUBSET", f'time("{nominal}")')),
        maximum_bytes=PUBLIC_RADAR_MAX_RESPONSE_BYTES,
        expected_content_types=("image/tiff",),
    )


def _remaining(deadline: float) -> float:
    result = float(deadline) - time.monotonic()
    if not math.isfinite(result) or result <= 0:
        raise KmiOpenDataDeadlineError("KMI raw-capturedeadline is verstreken")
    return result


def _bounded_header(
    headers: Any,
    name: str,
    *,
    maximum: int = MAX_HTTP_HEADER_CHARS,
) -> str | None:
    value = headers.get(name) if headers is not None else None
    if value is None:
        return None
    result = str(value).strip()
    if (
        not result
        or len(result) > maximum
        or any(ord(character) < 0x20 or ord(character) == 0x7F for character in result)
    ):
        raise KmiOpenDataContractError("KMI HTTP-header is ongeldig")
    return result


def _declared_length(headers: Any, *, maximum: int) -> int | None:
    value = _bounded_header(headers, "Content-Length", maximum=32)
    if value is None:
        return None
    if not value.isascii() or not value.isdecimal():
        raise KmiOpenDataContractError("KMI Content-Length is ongeldig")
    result = int(value)
    if result > maximum:
        raise KmiOpenDataCapacityError("KMI Content-Length overschrijdt de bytegrens")
    return result


def _read_bounded(
    response: Any,
    *,
    maximum: int,
    declared: int | None,
    deadline: float,
) -> bytes:
    chunks: list[bytes] = []
    total = 0
    while total <= maximum:
        _remaining(deadline)
        requested = min(STREAM_CHUNK_BYTES, maximum + 1 - total)
        block = response.read(requested)
        _remaining(deadline)
        if not block:
            break
        if not isinstance(block, (bytes, bytearray, memoryview)):
            raise KmiOpenDataContractError("KMI responsstream leverde geen bytes")
        immutable = bytes(block)
        if len(immutable) > requested:
            raise KmiOpenDataCapacityError("KMI responschunk overschrijdt de grens")
        chunks.append(immutable)
        total += len(immutable)
        if total > maximum or (declared is not None and total > declared):
            raise KmiOpenDataCapacityError("KMI response overschrijdt de bytegrens")
    payload = b"".join(chunks)
    if declared is not None and len(payload) != declared:
        raise KmiOpenDataContractError("KMI response is onvolledig")
    return payload


def _default_open(request: Request, *, timeout: float):
    return build_opener(ProxyHandler({}), _RejectRedirects()).open(
        request, timeout=timeout
    )


def _http_get(
    request_authority: KmiCanonicalRequest,
    *,
    maximum_bytes: int,
    deadline: float,
    opener: Callable[..., Any] | None = None,
) -> KmiHttpResponse:
    request = Request(
        request_authority.url,
        method=request_authority.method,
        headers={key: value for key, value in request_authority.headers},
    )
    active = _default_open if opener is None else opener
    with active(
        request,
        timeout=min(HTTP_IO_TIMEOUT_SECONDS, _remaining(deadline)),
    ) as response:
        status = getattr(response, "status", None)
        if isinstance(status, bool) or not isinstance(status, int) or status != 200:
            raise KmiOpenDataContractError("KMI GeoServer gaf geen HTTP 200")
        geturl = getattr(response, "geturl", None)
        if not callable(geturl) or str(geturl()) != request_authority.url:
            raise KmiOpenDataContractError(
                "KMI response kwam niet van de canonical request-URL"
            )
        headers = getattr(response, "headers", None)
        encoding = (
            _bounded_header(headers, "Content-Encoding", maximum=64) or "identity"
        ).lower()
        if encoding != "identity":
            raise KmiOpenDataContractError(
                "KMI transportcompressie wijkt af van exact identity"
            )
        declared = _declared_length(headers, maximum=maximum_bytes)
        raw_content_type = _bounded_header(headers, "Content-Type", maximum=256)
        if raw_content_type is None:
            content_type = "application/octet-stream"
        else:
            candidate = raw_content_type.split(";", 1)[0].strip().lower()
            content_type = (
                candidate
                if _CONTENT_TYPE_RE.fullmatch(candidate) is not None
                else "application/octet-stream"
            )
        payload = _read_bounded(
            response,
            maximum=maximum_bytes,
            declared=declared,
            deadline=deadline,
        )
        received_at = time.time()
        etag = _bounded_header(headers, "ETag")
        last_modified = _bounded_header(headers, "Last-Modified")
    return KmiHttpResponse(
        payload=payload,
        status_code=200,
        final_url=request_authority.url,
        content_type=content_type,
        content_type_header=raw_content_type,
        content_encoding=encoding,
        declared_content_length=declared,
        etag=etag,
        last_modified=last_modified,
        received_at=received_at,
    )


def _job_id(job: Any) -> str:
    value = getattr(job, "job_id", None)
    payload = getattr(job, "payload", None)
    if not isinstance(value, str) or _JOB_ID_RE.fullmatch(value) is None:
        raise KmiOpenDataContractError("KMI capturejob mist een begrensde job_id")
    if type(payload) is not dict or payload:
        raise KmiOpenDataContractError(
            "KMI capture accepteert geen jobpayload of credential"
        )
    return value


def _raw_store(injected: RawArtifactStore | None) -> RawArtifactStore:
    if injected is not None:
        injected.initialize()
        return injected
    share = RAW_SHARE_ROOT.resolve(strict=False)
    root = RAW_ARTIFACT_ROOT.resolve(strict=False)
    if root == share or share not in root.parents:
        raise KmiOpenDataCorruptionError("KMI raw store ontsnapt uit worker-share")
    global _RAW_STORE
    with _RAW_STORE_LOCK:
        if _RAW_STORE is None:
            _RAW_STORE = RawArtifactStore(root)
            _RAW_STORE.initialize()
        return _RAW_STORE


def _response_product(descriptor: KmiLayerDescriptor) -> str:
    return _layer_policy(descriptor)["response_product"]


def _capture_product(descriptor: KmiLayerDescriptor) -> str:
    return _layer_policy(descriptor)["capture_product"]


def _receipt_id(
    store: RawArtifactStore,
    job_id: str,
    descriptor: KmiLayerDescriptor,
) -> str:
    return store._receipt_id(  # noqa: SLF001 - deterministic schema-3 identity
        provider=PROVIDER,
        product=_response_product(descriptor),
        fetch_id=f"{job_id}:response",
    )


def _capture_id(
    store: RawArtifactStore,
    job_id: str,
    descriptor: KmiLayerDescriptor,
) -> str:
    return store._capture_id(  # noqa: SLF001 - deterministic schema-3 identity
        provider=PROVIDER,
        product=_capture_product(descriptor),
        fetch_id=f"{job_id}:capture",
    )


def _transport_document(
    response: KmiHttpResponse,
    *,
    request_sha256: str,
    expected_content_types: Sequence[str],
) -> dict[str, object]:
    body_sha256 = hashlib.sha256(response.payload).hexdigest()
    return {
        "schema": TRANSPORT_RECEIPT_SCHEMA,
        "request_sha256": request_sha256,
        "status_code": response.status_code,
        "final_url_sha256": hashlib.sha256(
            response.final_url.encode("utf-8")
        ).hexdigest(),
        "content_type": response.content_type,
        "content_type_header": response.content_type_header,
        "content_encoding": response.content_encoding,
        "declared_content_length": response.declared_content_length,
        "body_size_bytes": len(response.payload),
        "body_sha256": body_sha256,
        "etag": response.etag,
        "last_modified": response.last_modified,
        "received_at": response.received_at,
        "expected_content_type_match": response.content_type
        in expected_content_types,
    }


def _provenance(
    descriptor: KmiLayerDescriptor,
    request_authority: KmiCanonicalRequest,
    transport: Mapping[str, object],
) -> dict[str, object]:
    descriptor_authority = descriptor_document(descriptor)
    return {
        "representation": NATIVE_REPRESENTATION,
        "capture_stage": NATIVE_CAPTURE_STAGE,
        "collector": COLLECTOR,
        "collector_version": COLLECTOR_VERSION,
        "role": "geoserver_response",
        "descriptor": descriptor_authority,
        "descriptor_sha256": _sha256_json(descriptor_authority),
        "canonical_request": request_authority.to_mapping(),
        "canonical_request_sha256": request_authority.request_sha256,
        "transport_receipt": dict(transport),
        "transport_receipt_sha256": _sha256_json(transport),
        "response_body_parsed_before_store": False,
        "meteorological_semantics": "unknown_unverified",
        "operational_admission": "not_evaluated",
        "semantic_or_admission_status_blocks_storage": False,
        "credential_transport": "none",
    }


def _upstream_id(request_sha256: str, content_sha256: str) -> str:
    return (
        "kmi-opendata:request=" + request_sha256
        + ":response=" + content_sha256
    )


def _validate_receipt(
    receipt: RawArtifactReceipt,
    *,
    job_id: str,
    descriptor: KmiLayerDescriptor,
    request_authority: KmiCanonicalRequest,
) -> None:
    provenance = receipt.provenance
    transport = provenance.get("transport_receipt")
    descriptor_authority = descriptor_document(descriptor)
    transport_receipt_sha256 = (
        _sha256_json(transport) if isinstance(transport, Mapping) else None
    )
    transport_content_type = (
        transport.get("content_type") if isinstance(transport, Mapping) else None
    )
    expected_content_type_match = (
        transport_content_type in descriptor.expected_content_types
    )
    stored_content_type = (
        transport_content_type
        if expected_content_type_match
        else "application/octet-stream"
    )
    expected_upstream_id = _upstream_id(
        request_authority.request_sha256, receipt.content_sha256
    )
    if (
        receipt.provider != PROVIDER
        or receipt.product != _response_product(descriptor)
        or receipt.fetch_id != f"{job_id}:response"
        or not 0 <= receipt.size_bytes <= descriptor.maximum_bytes
        or receipt.upstream_id != expected_upstream_id
        or provenance.get("representation") != NATIVE_REPRESENTATION
        or provenance.get("capture_stage") != NATIVE_CAPTURE_STAGE
        or provenance.get("collector") != COLLECTOR
        or provenance.get("collector_version") != COLLECTOR_VERSION
        or provenance.get("descriptor") != descriptor_authority
        or provenance.get("descriptor_sha256") != _sha256_json(descriptor_authority)
        or provenance.get("canonical_request") != request_authority.to_mapping()
        or provenance.get("canonical_request_sha256")
        != request_authority.request_sha256
        or provenance.get("transport_receipt_sha256")
        != transport_receipt_sha256
        or provenance.get("response_body_parsed_before_store") is not False
        or provenance.get("meteorological_semantics") != "unknown_unverified"
        or provenance.get("operational_admission") != "not_evaluated"
        or provenance.get("semantic_or_admission_status_blocks_storage") is not False
        or provenance.get("credential_transport") != "none"
        or not isinstance(transport, Mapping)
        or transport.get("schema") != TRANSPORT_RECEIPT_SCHEMA
        or transport.get("request_sha256") != request_authority.request_sha256
        or transport.get("status_code") != 200
        or transport.get("final_url_sha256")
        != hashlib.sha256(request_authority.url.encode("utf-8")).hexdigest()
        or receipt.content_type != stored_content_type
        or transport.get("content_encoding") != "identity"
        or transport.get("body_size_bytes") != receipt.size_bytes
        or transport.get("body_sha256") != receipt.content_sha256
        or transport.get("received_at") != receipt.received_at
        or transport.get("expected_content_type_match")
        is not expected_content_type_match
    ):
        raise KmiOpenDataCorruptionError("KMI raw receipt wijkt af van authority")


def _upstream_capture_id(
    descriptor: KmiLayerDescriptor,
    request_authority: KmiCanonicalRequest,
    receipt: RawArtifactReceipt,
) -> str:
    return (
        f"kmi-opendata:{descriptor.descriptor_id}"
        f":request={request_authority.request_sha256}"
        f":response={receipt.content_sha256}"
    )


def _validate_capture(
    capture: RawArtifactCaptureReceipt,
    *,
    job_id: str,
    descriptor: KmiLayerDescriptor,
    request_authority: KmiCanonicalRequest,
    receipt: RawArtifactReceipt,
) -> None:
    members = {member.role: member.receipt_id for member in capture.members}
    if (
        capture.raw_store_schema_version != RAW_ARTIFACT_SCHEMA_VERSION
        or capture.provider != PROVIDER
        or capture.product != _capture_product(descriptor)
        or capture.fetch_id != f"{job_id}:capture"
        or capture.product_timestamp is not None
        or capture.upstream_capture_id
        != _upstream_capture_id(descriptor, request_authority, receipt)
        or members != {"geoserver_response": receipt.receipt_id}
    ):
        raise KmiOpenDataCorruptionError("KMI raw capture wijkt af van authority")


def _result(
    capture: RawArtifactCaptureReceipt,
    receipt: RawArtifactReceipt,
    descriptor: KmiLayerDescriptor,
    request_authority: KmiCanonicalRequest,
) -> dict[str, object]:
    transport = receipt.provenance["transport_receipt"]
    source = {
        "role": "geoserver_response",
        "receipt_id": receipt.receipt_id,
        "content_sha256": receipt.content_sha256,
        "receipt_sha256": receipt.receipt_sha256,
        "size_bytes": receipt.size_bytes,
        "content_type": receipt.content_type,
        "received_at": receipt.received_at,
    }
    return {
        "method": METHOD,
        "provider": PROVIDER,
        "service": descriptor.service,
        "layer": descriptor.layer,
        "descriptor_id": descriptor.descriptor_id,
        "capture_state": "captured_exact_raw",
        "decode_status": "not_attempted",
        "meteorological_semantics": "unknown_unverified",
        "operational_admission": "not_evaluated",
        "operational_admission_eligible": False,
        "storage_complete": True,
        "semantic_or_admission_status_blocked_storage": False,
        "weather_state": "unknown",
        "raw_provenance": {
            "raw_store_schema_version": RAW_ARTIFACT_SCHEMA_VERSION,
            "raw_capture_id": capture.capture_id,
            "raw_capture_manifest_sha256": capture.manifest_sha256,
            "raw_capture_receipt_sha256": capture.capture_receipt_sha256,
            "upstream_capture_id": capture.upstream_capture_id,
            "canonical_request_sha256": request_authority.request_sha256,
            "transport_receipt": transport,
            "transport_receipt_sha256": receipt.provenance[
                "transport_receipt_sha256"
            ],
            "source": source,
            "sources": {"geoserver_response": source},
        },
    }


def validate_kmi_opendata_receipt_for_recovery(
    receipt: RawArtifactReceipt,
    *,
    job_id: str,
) -> KmiLayerDescriptor:
    """Reprove one durable KMI response without trusting caller metadata."""

    if not isinstance(receipt, RawArtifactReceipt):
        raise KmiOpenDataCorruptionError("KMI recovery mist een raw receipt")
    descriptor = _descriptor_from_document(receipt.provenance.get("descriptor"))
    request_authority = canonical_request(descriptor)
    _validate_receipt(
        receipt,
        job_id=job_id,
        descriptor=descriptor,
        request_authority=request_authority,
    )
    return descriptor


def validate_kmi_opendata_capture_for_recovery(
    capture: RawArtifactCaptureReceipt,
    receipt: RawArtifactReceipt,
    *,
    job_id: str,
) -> None:
    """Reprove a complete KMI capture against its canonical request."""

    if not isinstance(capture, RawArtifactCaptureReceipt):
        raise KmiOpenDataCorruptionError("KMI recovery mist een raw capture")
    descriptor = validate_kmi_opendata_receipt_for_recovery(
        receipt,
        job_id=job_id,
    )
    _validate_capture(
        capture,
        job_id=job_id,
        descriptor=descriptor,
        request_authority=canonical_request(descriptor),
        receipt=receipt,
    )


def _capture_locked(
    job_id: str,
    *,
    descriptor: KmiLayerDescriptor,
    request_authority: KmiCanonicalRequest,
    store: RawArtifactStore,
    deadline: float,
    opener: Callable[..., Any] | None,
    fault: Callable[[str], None] | None,
) -> dict[str, object]:
    capture_id = _capture_id(store, job_id, descriptor)
    complete = store.load_capture_bundle(capture_id)
    if complete is not None:
        capture, receipts = complete
        if set(receipts) != {"geoserver_response"}:
            raise KmiOpenDataCorruptionError("KMI capture mist zijn response")
        receipt = receipts["geoserver_response"]
        _validate_receipt(
            receipt,
            job_id=job_id,
            descriptor=descriptor,
            request_authority=request_authority,
        )
        _validate_capture(
            capture,
            job_id=job_id,
            descriptor=descriptor,
            request_authority=request_authority,
            receipt=receipt,
        )
        return _result(capture, receipt, descriptor, request_authority)

    receipt = store.load_receipt(_receipt_id(store, job_id, descriptor))
    if receipt is None:
        response = _http_get(
            request_authority,
            maximum_bytes=descriptor.maximum_bytes,
            deadline=deadline,
            opener=opener,
        )
        transport = _transport_document(
            response,
            request_sha256=request_authority.request_sha256,
            expected_content_types=descriptor.expected_content_types,
        )
        content_sha256 = str(transport["body_sha256"])
        stored_content_type = (
            response.content_type
            if transport["expected_content_type_match"] is True
            else "application/octet-stream"
        )
        receipt = store.store(
            provider=PROVIDER,
            product=_response_product(descriptor),
            fetch_id=f"{job_id}:response",
            content_type=stored_content_type,
            received_at=response.received_at,
            upstream_id=_upstream_id(
                request_authority.request_sha256, content_sha256
            ),
            provenance=_provenance(
                descriptor, request_authority, transport
            ),
            payload=response.payload,
        )
        if fault is not None:
            fault("kmi_opendata.after_response_store")
    _validate_receipt(
        receipt,
        job_id=job_id,
        descriptor=descriptor,
        request_authority=request_authority,
    )
    _remaining(deadline)
    if fault is not None:
        fault("kmi_opendata.before_capture_commit")
    capture = store.commit_capture(
        provider=PROVIDER,
        product=_capture_product(descriptor),
        fetch_id=f"{job_id}:capture",
        upstream_capture_id=_upstream_capture_id(
            descriptor, request_authority, receipt
        ),
        product_timestamp=None,
        members={"geoserver_response": receipt.receipt_id},
    )
    if fault is not None:
        fault("kmi_opendata.after_capture_commit")
    _validate_capture(
        capture,
        job_id=job_id,
        descriptor=descriptor,
        request_authority=request_authority,
        receipt=receipt,
    )
    return _result(capture, receipt, descriptor, request_authority)


def capture_kmi_opendata(
    job: Any,
    *,
    descriptor: KmiLayerDescriptor = KMI_AWS_10MIN_LATEST,
    raw_store: RawArtifactStore | None = None,
    deadline_at: float | None = None,
    _opener: Callable[..., Any] | None = None,
    _fault_injector: Callable[[str], None] | None = None,
) -> dict[str, object]:
    """Capture one allowlisted KMI response without parsing its body."""
    job_id = _job_id(job)
    request_authority = canonical_request(descriptor)
    now = time.monotonic()
    if deadline_at is None:
        deadline = now + CAPTURE_TOTAL_DEADLINE_SECONDS
    elif (
        isinstance(deadline_at, bool)
        or not isinstance(deadline_at, (int, float))
        or not math.isfinite(float(deadline_at))
    ):
        raise KmiOpenDataContractError("KMI deadline_at is ongeldig")
    else:
        deadline = min(float(deadline_at), now + CAPTURE_TOTAL_DEADLINE_SECONDS)
    store = _raw_store(raw_store)
    acquired = _CAPTURE_LOCK.acquire(timeout=_remaining(deadline))
    if not acquired:
        raise KmiOpenDataDeadlineError("KMI capturelock overschreed de deadline")
    try:
        _remaining(deadline)
        return _capture_locked(
            job_id,
            descriptor=descriptor,
            request_authority=request_authority,
            store=store,
            deadline=deadline,
            opener=_opener,
            fault=_fault_injector,
        )
    finally:
        _CAPTURE_LOCK.release()


def capture_kmi_aws_10min(
    job: Any,
    **kwargs: Any,
) -> dict[str, object]:
    """Dispatch-compatible convenience handler for the bounded latest AWS set."""
    return capture_kmi_opendata(
        job, descriptor=KMI_AWS_10MIN_LATEST, **kwargs
    )


def capture_kmi_aws_10min_window(
    job: Any,
    *,
    start_at: datetime,
    end_at: datetime,
    **kwargs: Any,
) -> dict[str, object]:
    """Capture every raw AWS feature returned for one half-open UTC window."""
    return capture_kmi_opendata(
        job,
        descriptor=aws_10min_window_descriptor(start_at, end_at),
        **kwargs,
    )


__all__ = [
    "AWS_10MIN_LAYER",
    "AWS_10MIN_MAX_RESPONSE_BYTES",
    "AWS_10MIN_CAPTURE_PRODUCT",
    "AWS_10MIN_RESPONSE_PRODUCT",
    "CANONICAL_REQUEST_SCHEMA",
    "CAPTURE_TOTAL_DEADLINE_SECONDS",
    "KMI_AWS_10MIN_LATEST",
    "KMI_OPENDATA_ENDPOINT",
    "KMI_OPENDATA_HOST",
    "KMI_OPENDATA_PATH",
    "KmiCanonicalRequest",
    "KmiHttpResponse",
    "KmiLayerDescriptor",
    "KmiOpenDataCapacityError",
    "KmiOpenDataContractError",
    "KmiOpenDataCorruptionError",
    "KmiOpenDataDeadlineError",
    "KmiOpenDataError",
    "PUBLIC_RADAR_COVERAGE",
    "TRANSPORT_RECEIPT_SCHEMA",
    "aws_10min_window_descriptor",
    "canonical_request",
    "capture_kmi_aws_10min",
    "capture_kmi_aws_10min_window",
    "capture_kmi_opendata",
    "descriptor_document",
    "public_radar_time_descriptor",
    "validate_kmi_opendata_capture_for_recovery",
    "validate_kmi_opendata_receipt_for_recovery",
]
