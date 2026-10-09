"""Exact, non-operational native capture for Italy's official DPC SRI.

The candidate retains the exact ``findLastProductByType`` JSON response and
the exact selected source GeoTIFF.  The ``downloadProduct`` response contains
a short-lived S3 presigned URL (including an AWS security token), so that
credential-bearing envelope is deliberately never persisted.  Its canonical,
secret-free object identity (bucket/key/product time/origin/path) is bound to
the GeoTIFF receipt provenance instead.  It is not falsely labelled as a raw
third member.  This makes selection and retries reproducible without retaining
a transport credential.

Every original member is durable before its structural inspection.  The TIFF
inspector reads only the bounded container directory and never a raster pixel.
This module emits no rain, dry, cell, forecast, quality or admission fact and
is intentionally absent from the production worker and Home Assistant.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import errno
import hashlib
import json
import math
import os
from pathlib import Path
import re
import stat
import struct
import threading
import time
from typing import Any, Callable, Iterator, Mapping
from urllib.parse import parse_qsl, quote, urlsplit
from urllib.request import (
    HTTPRedirectHandler,
    ProxyHandler,
    Request,
    build_opener,
)

from .raw_artifacts import (
    NATIVE_CAPTURE_STAGE,
    NATIVE_REPRESENTATION,
    RAW_ARTIFACT_SCHEMA_VERSION,
    RawArtifactCaptureReceipt,
    RawArtifactReceipt,
    RawArtifactStore,
)


DPC_API_ORIGIN = "https://radar-api.protezionecivile.it"
DPC_WEB_ORIGIN = "https://radar.protezionecivile.it"
DPC_CATALOG_PATH = "/findLastProductByType"
DPC_DOWNLOAD_PATH = "/downloadProduct"
DPC_PRODUCT_TYPE = "SRI"
DPC_PERIOD = "PT5M"
DPC_BUCKET = "s3-prod-dpc-radar"
DPC_S3_HOST = "s3-prod-dpc-radar.s3.eu-south-1.amazonaws.com"
DPC_S3_ORIGIN = f"https://{DPC_S3_HOST}"

MAX_CATALOG_BYTES = 2 * 1024 * 1024
MAX_DOWNLOAD_RESPONSE_BYTES = 64 * 1024
MAX_DOWNLOAD_IDENTITY_BYTES = 4 * 1024
MAX_GEOTIFF_BYTES = 25 * 1024 * 1024
MAX_SIGNED_URL_CHARS = 8192
MAX_HTTP_HEADER_CHARS = 8192
MAX_JSON_NODES = 4096
MAX_JSON_DEPTH = 16
MAX_IFD_ENTRIES = 256
HTTP_IO_TIMEOUT_SECONDS = 15.0
CAPTURE_TOTAL_DEADLINE_SECONDS = 60.0
LOCK_POLL_SECONDS = 0.05

DPC_RAW_SHARE_ROOT = Path("/share/storm_tracker_worker")
DPC_RAW_ARTIFACT_ROOT = DPC_RAW_SHARE_ROOT / "raw" / "dpc_sri_native"
DPC_CATALOG_PRODUCT = "dpc_sri_last_product_catalog_json"
DPC_GEOTIFF_PRODUCT = "dpc_sri_original_geotiff"
DPC_CAPTURE_PRODUCT = "dpc_sri_catalog_geotiff_capture"
DPC_COLLECTOR = "dpc_radar_v2_exact_native"
DPC_COLLECTOR_VERSION = "worker-dpc-radar-v2-exact-native-v1"
DPC_CATALOG_INSPECTOR_VERSION = "dpc-radar-v2-catalog-v1"
DPC_TIFF_INSPECTOR_VERSION = "dpc-sri-geotiff-container-v1"
DPC_DOWNLOAD_IDENTITY_SCHEMA = "stv3-dpc-download-identity-v1"
DPC_SEMANTIC_CLASS = "surface_rainfall_intensity_estimate"
DPC_ADMISSION_STATUS = "exact_native_shadow_not_decoded_not_admitted"

_JOB_ID_RE = re.compile(r"^[^\x00-\x1f\x7f]{1,480}$")
_KEY_RE = re.compile(
    r"^SRI/(?P<day>\d{2})-(?P<month>\d{2})-(?P<year>\d{4})-"
    r"(?P<hour>\d{2})-(?P<minute>\d{2})\.tif$"
)
_SIGNED_QUERY_KEYS = frozenset({
    "X-Amz-Algorithm",
    "X-Amz-Credential",
    "X-Amz-Date",
    "X-Amz-Expires",
    "X-Amz-Security-Token",
    "X-Amz-Signature",
    "X-Amz-SignedHeaders",
    "response-content-disposition",
})
_REQUIRED_TIFF_TAGS = frozenset({
    256, 257, 258, 259, 273, 277, 278, 279, 339, 33550, 33922, 34735
})

_RAW_STORE: RawArtifactStore | None = None
_RAW_STORE_LOCK = threading.Lock()
_CAPTURE_LOCK = threading.Lock()


class DpcNativeError(RuntimeError):
    """Base candidate failure; never a weather state."""


class DpcNativeContractError(DpcNativeError):
    """Official transport/identity shape drifted from the pinned contract."""


class DpcNativeCapacityError(DpcNativeError):
    """A hard byte/member/container bound rejected the attempt."""


class DpcNativeDeadlineError(DpcNativeError):
    """The one cooperative capture deadline elapsed."""


class DpcNativeCorruptionError(DpcNativeError):
    """Durable bytes or receipts no longer match their exact authority."""


@dataclass(frozen=True, slots=True)
class DpcHttpResponse:
    payload: bytes
    content_type: str
    received_at: float
    content_length: int
    etag: str | None = None
    last_modified: str | None = None


@dataclass(frozen=True, slots=True)
class DpcCatalogIdentity:
    product_type: str
    product_time_ms: int
    period: str

    @property
    def product_timestamp(self) -> float:
        return self.product_time_ms / 1000.0


@dataclass(frozen=True, slots=True)
class DpcDownloadIdentity:
    product_type: str
    product_time_ms: int
    bucket: str
    key: str
    expires_seconds: int
    object_origin: str
    object_path: str

    def to_mapping(self) -> dict[str, object]:
        return {
            "schema": DPC_DOWNLOAD_IDENTITY_SCHEMA,
            "product_type": self.product_type,
            "product_time_ms": self.product_time_ms,
            "bucket": self.bucket,
            "key": self.key,
            "expires_seconds": self.expires_seconds,
            "object_origin": self.object_origin,
            "object_path": self.object_path,
            "presigned_query_persisted": False,
        }


@dataclass(frozen=True, slots=True)
class DpcTiffIdentity:
    byte_order: str
    width: int
    height: int
    bits_per_sample: int
    compression: int
    samples_per_pixel: int
    sample_format: int
    ifd_entries: int
    georeference_tags: tuple[int, ...]
    pixel_values_read: bool = False


class _RejectRedirects(HTTPRedirectHandler):
    def redirect_request(self, *_args, **_kwargs):
        raise DpcNativeContractError("DPC HTTP-redirect is niet toegestaan")


def _open_without_redirects(request: Request, *, timeout: float):
    return build_opener(ProxyHandler({}), _RejectRedirects()).open(
        request, timeout=timeout
    )


def _remaining(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if not math.isfinite(remaining) or remaining <= 0.0:
        raise DpcNativeDeadlineError("DPC capturedeadline is verstreken")
    return remaining


def _bounded_header(
    headers: Any,
    name: str,
    *,
    required: bool = False,
    maximum: int = MAX_HTTP_HEADER_CHARS,
) -> str | None:
    value = headers.get(name) if headers is not None else None
    if value is None:
        if required:
            raise DpcNativeContractError(f"DPC HTTP-header {name} ontbreekt")
        return None
    result = str(value).strip()
    if (
        not result
        or len(result) > maximum
        or any(ord(char) < 0x20 or ord(char) == 0x7F for char in result)
    ):
        raise DpcNativeContractError(f"DPC HTTP-header {name} is ongeldig")
    return result


def _content_type(headers: Any) -> str:
    value = _bounded_header(headers, "Content-Type", required=True, maximum=256)
    assert value is not None
    return value.split(";", 1)[0].strip().lower()


def _declared_length(headers: Any, *, maximum_bytes: int) -> int:
    value = _bounded_header(headers, "Content-Length", required=True, maximum=32)
    assert value is not None
    if not value.isascii() or not value.isdecimal():
        raise DpcNativeContractError("DPC Content-Length is ongeldig")
    result = int(value)
    if not 0 < result <= maximum_bytes:
        raise DpcNativeCapacityError("DPC Content-Length valt buiten de bytegrens")
    return result


def _read_bounded_body(
    response: Any,
    *,
    maximum_bytes: int,
    declared_bytes: int,
    deadline: float,
) -> bytes:
    chunks: list[bytes] = []
    total = 0
    while total <= maximum_bytes:
        _remaining(deadline)
        requested = min(64 * 1024, maximum_bytes + 1 - total)
        chunk = response.read(requested)
        _remaining(deadline)
        if not chunk:
            break
        if not isinstance(chunk, (bytes, bytearray, memoryview)):
            raise DpcNativeContractError("DPC responsstream leverde geen bytes")
        immutable = bytes(chunk)
        if len(immutable) > requested:
            raise DpcNativeCapacityError("DPC responschunk overschrijdt de grens")
        chunks.append(immutable)
        total += len(immutable)
        if total > declared_bytes or total > maximum_bytes:
            raise DpcNativeCapacityError("DPC response overschrijdt de bytegrens")
    payload = b"".join(chunks)
    if len(payload) != declared_bytes:
        raise DpcNativeContractError("DPC responslengte wijkt af van HTTP")
    return payload


def _request_exact(
    url: str,
    *,
    method: str,
    request_body: bytes | None,
    headers: Mapping[str, str],
    maximum_bytes: int,
    allowed_content_types: frozenset[str],
    deadline: float,
    opener: Callable[..., Any] | None = None,
) -> DpcHttpResponse:
    request = Request(
        url,
        data=request_body,
        method=method,
        headers=dict(headers),
    )
    active = _open_without_redirects if opener is None else opener
    with active(
        request,
        timeout=min(HTTP_IO_TIMEOUT_SECONDS, _remaining(deadline)),
    ) as response:
        if getattr(response, "status", None) != 200:
            raise DpcNativeContractError("DPC endpoint gaf geen HTTP 200")
        geturl = getattr(response, "geturl", None)
        if not callable(geturl) or str(geturl()) != url:
            raise DpcNativeContractError("DPC response kwam van een ander endpoint")
        response_headers = getattr(response, "headers", None)
        encoding = (
            _bounded_header(
                response_headers,
                "Content-Encoding",
                required=False,
                maximum=64,
            )
            or "identity"
        ).lower()
        if encoding != "identity":
            raise DpcNativeContractError("DPC transportcompressie is verboden")
        content_type = _content_type(response_headers)
        if content_type not in allowed_content_types:
            raise DpcNativeContractError("DPC Content-Type valt buiten contract")
        declared = _declared_length(
            response_headers, maximum_bytes=maximum_bytes
        )
        payload = _read_bounded_body(
            response,
            maximum_bytes=maximum_bytes,
            declared_bytes=declared,
            deadline=deadline,
        )
        return DpcHttpResponse(
            payload=payload,
            content_type=content_type,
            received_at=time.time(),
            content_length=len(payload),
            etag=_bounded_header(response_headers, "ETag", maximum=256),
            last_modified=_bounded_header(
                response_headers, "Last-Modified", maximum=128
            ),
        )


def _fetch_catalog(*, deadline: float) -> DpcHttpResponse:
    url = f"{DPC_API_ORIGIN}{DPC_CATALOG_PATH}?type={DPC_PRODUCT_TYPE}"
    return _request_exact(
        url,
        method="GET",
        request_body=None,
        headers={
            "Accept": "application/json",
            "Accept-Encoding": "identity",
            "Origin": DPC_WEB_ORIGIN,
            "User-Agent": "StormTrackerWorker/dpc-native-candidate-v1",
        },
        maximum_bytes=MAX_CATALOG_BYTES,
        allowed_content_types=frozenset({"application/json", "text/json"}),
        deadline=deadline,
    )


def _download_request_body(product_time_ms: int) -> bytes:
    return json.dumps(
        {"productType": DPC_PRODUCT_TYPE, "productDate": product_time_ms},
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _fetch_download_envelope(
    product_time_ms: int,
    *,
    deadline: float,
) -> DpcHttpResponse:
    return _request_exact(
        f"{DPC_API_ORIGIN}{DPC_DOWNLOAD_PATH}",
        method="POST",
        request_body=_download_request_body(product_time_ms),
        headers={
            "Accept": "application/json",
            "Accept-Encoding": "identity",
            "Content-Type": "application/json",
            "Origin": DPC_WEB_ORIGIN,
            "User-Agent": "StormTrackerWorker/dpc-native-candidate-v1",
        },
        maximum_bytes=MAX_DOWNLOAD_RESPONSE_BYTES,
        allowed_content_types=frozenset({"application/json", "text/json"}),
        deadline=deadline,
    )


def _fetch_geotiff(
    signed_url: str,
    identity: DpcDownloadIdentity,
    *,
    deadline: float,
) -> DpcHttpResponse:
    _validate_signed_url(signed_url, identity=identity)
    return _request_exact(
        signed_url,
        method="GET",
        request_body=None,
        headers={
            "Accept": "application/octet-stream,image/tiff,image/geotiff",
            "Accept-Encoding": "identity",
            "User-Agent": "StormTrackerWorker/dpc-native-candidate-v1",
        },
        maximum_bytes=MAX_GEOTIFF_BYTES,
        allowed_content_types=frozenset({
            "application/octet-stream",
            "binary/octet-stream",
            "image/tiff",
            "image/geotiff",
        }),
        deadline=deadline,
    )


def _strict_json(payload: bytes, *, maximum_bytes: int, context: str) -> Any:
    if not isinstance(payload, bytes) or not 1 <= len(payload) <= maximum_bytes:
        raise DpcNativeCapacityError(f"DPC {context} valt buiten de bytegrens")
    try:
        text = payload.decode("utf-8", errors="strict")
    except UnicodeDecodeError as exc:
        raise DpcNativeContractError(f"DPC {context} is geen UTF-8") from exc

    def object_pairs(pairs):
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise DpcNativeContractError(
                    f"DPC {context} bevat dubbele JSON-sleutels"
                )
            result[key] = value
        return result

    try:
        document = json.loads(
            text,
            object_pairs_hook=object_pairs,
            parse_constant=lambda _value: (_ for _ in ()).throw(
                DpcNativeContractError(
                    f"DPC {context} bevat een niet-eindig getal"
                )
            ),
        )
    except json.JSONDecodeError as exc:
        raise DpcNativeContractError(f"DPC {context} is ongeldige JSON") from exc
    pending = [(document, 0)]
    nodes = 0
    while pending:
        value, depth = pending.pop()
        nodes += 1
        if nodes > MAX_JSON_NODES or depth > MAX_JSON_DEPTH:
            raise DpcNativeCapacityError(f"DPC {context} JSON is onbegrensd")
        if isinstance(value, dict):
            pending.extend((item, depth + 1) for item in value.values())
        elif isinstance(value, list):
            pending.extend((item, depth + 1) for item in value)
    return document


def _product_time(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise DpcNativeContractError("DPC producttime is niet exact integer ms")
    if not 946_684_800_000 <= value <= 4_102_444_800_000:
        raise DpcNativeContractError("DPC producttime valt buiten het tijdcontract")
    if value % 300_000 != 0:
        raise DpcNativeContractError("DPC SRI producttime ligt niet op 5 minuten")
    return value


def _inspect_catalog(payload: bytes) -> DpcCatalogIdentity:
    document = _strict_json(
        payload,
        maximum_bytes=MAX_CATALOG_BYTES,
        context="catalogus",
    )
    if not isinstance(document, dict) or set(document) != {"total", "lastProducts"}:
        raise DpcNativeContractError("DPC catalogusroot wijkt af")
    products = document["lastProducts"]
    if document["total"] != 1 or not isinstance(products, list) or len(products) != 1:
        raise DpcNativeContractError("DPC catalogus bevat niet exact één SRI-head")
    product = products[0]
    if (
        not isinstance(product, dict)
        or set(product) != {"productType", "time", "period"}
        or product["productType"] != DPC_PRODUCT_TYPE
        or product["period"] != DPC_PERIOD
    ):
        raise DpcNativeContractError("DPC SRI catalogusidentiteit wijkt af")
    return DpcCatalogIdentity(
        product_type=DPC_PRODUCT_TYPE,
        product_time_ms=_product_time(product["time"]),
        period=DPC_PERIOD,
    )


def _expected_key(product_time_ms: int) -> str:
    timestamp = datetime.fromtimestamp(product_time_ms / 1000, tz=timezone.utc)
    return f"SRI/{timestamp:%d-%m-%Y-%H-%M}.tif"


def _validate_key(key: object, *, product_time_ms: int) -> str:
    if not isinstance(key, str) or len(key) > 128 or _KEY_RE.fullmatch(key) is None:
        raise DpcNativeContractError("DPC downloadkey valt buiten het SRI-contract")
    if key != _expected_key(product_time_ms):
        raise DpcNativeContractError("DPC downloadkey en catalogustijd verschillen")
    return key


def _validate_signed_url(
    url: object,
    *,
    identity: DpcDownloadIdentity,
) -> str:
    if (
        not isinstance(url, str)
        or not 1 <= len(url) <= MAX_SIGNED_URL_CHARS
        or any(ord(char) < 0x20 or ord(char) == 0x7F for char in url)
    ):
        raise DpcNativeContractError("DPC signed URL is ongeldig")
    parsed = urlsplit(url)
    if (
        parsed.scheme != "https"
        or parsed.hostname != DPC_S3_HOST
        or parsed.port not in (None, 443)
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
        or parsed.path != identity.object_path
    ):
        raise DpcNativeContractError("DPC signed URL ontsnapt uit het vaste object")
    if not parsed.query or len(parsed.query) > 6144:
        raise DpcNativeContractError("DPC signed URL mist begrensde authenticatie")
    try:
        pairs = parse_qsl(
            parsed.query,
            keep_blank_values=True,
            strict_parsing=True,
            max_num_fields=16,
        )
    except ValueError as exc:
        raise DpcNativeContractError("DPC signed URL-query is ongeldig") from exc
    query = dict(pairs)
    if len(query) != len(pairs) or set(query) != _SIGNED_QUERY_KEYS:
        raise DpcNativeContractError("DPC signed URL-queryvelden wijken af")
    filename = identity.key.rsplit("/", 1)[-1]
    if (
        query["X-Amz-Algorithm"] != "AWS4-HMAC-SHA256"
        or query["X-Amz-Expires"] != str(identity.expires_seconds)
        or query["X-Amz-SignedHeaders"] != "host"
        or re.fullmatch(r"[0-9a-fA-F]{64}", query["X-Amz-Signature"]) is None
        or re.fullmatch(r"\d{8}T\d{6}Z", query["X-Amz-Date"]) is None
        or not 1 <= len(query["X-Amz-Credential"]) <= 1024
        or not 1 <= len(query["X-Amz-Security-Token"]) <= 4096
        or query["response-content-disposition"]
        != f'attachment; filename="{filename}"'
    ):
        raise DpcNativeContractError("DPC signed URL-profiel wijkt af")
    return url


def _inspect_download_envelope(
    payload: bytes,
    *,
    catalog: DpcCatalogIdentity,
) -> tuple[DpcDownloadIdentity, str]:
    document = _strict_json(
        payload,
        maximum_bytes=MAX_DOWNLOAD_RESPONSE_BYTES,
        context="downloadantwoord",
    )
    if (
        not isinstance(document, dict)
        or set(document) != {"bucket", "key", "url", "expiresSeconds"}
        or document["bucket"] != DPC_BUCKET
    ):
        raise DpcNativeContractError("DPC downloadantwoord wijkt af")
    expires = document["expiresSeconds"]
    if isinstance(expires, bool) or not isinstance(expires, int) or not 1 <= expires <= 900:
        raise DpcNativeContractError("DPC URL-vervaltijd valt buiten contract")
    key = _validate_key(document["key"], product_time_ms=catalog.product_time_ms)
    path = "/" + quote(key, safe="/-_.")
    identity = DpcDownloadIdentity(
        product_type=DPC_PRODUCT_TYPE,
        product_time_ms=catalog.product_time_ms,
        bucket=DPC_BUCKET,
        key=key,
        expires_seconds=expires,
        object_origin=DPC_S3_ORIGIN,
        object_path=path,
    )
    return identity, _validate_signed_url(document["url"], identity=identity)


def _identity_bytes(identity: DpcDownloadIdentity) -> bytes:
    payload = json.dumps(
        identity.to_mapping(),
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    if len(payload) > MAX_DOWNLOAD_IDENTITY_BYTES:
        raise DpcNativeCapacityError("DPC downloadidentiteit overschrijdt grens")
    return payload


def _inspect_identity_bytes(payload: bytes) -> DpcDownloadIdentity:
    document = _strict_json(
        payload,
        maximum_bytes=MAX_DOWNLOAD_IDENTITY_BYTES,
        context="downloadidentiteit",
    )
    expected = {
        "schema",
        "product_type",
        "product_time_ms",
        "bucket",
        "key",
        "expires_seconds",
        "object_origin",
        "object_path",
        "presigned_query_persisted",
    }
    if not isinstance(document, dict) or set(document) != expected:
        raise DpcNativeCorruptionError("DPC duurzame downloadidentiteit wijkt af")
    if (
        not isinstance(document["product_type"], str)
        or not isinstance(document["bucket"], str)
        or not isinstance(document["key"], str)
        or not isinstance(document["object_origin"], str)
        or not isinstance(document["object_path"], str)
        or isinstance(document["expires_seconds"], bool)
        or not isinstance(document["expires_seconds"], int)
    ):
        raise DpcNativeCorruptionError("DPC downloadidentiteit heeft onjuiste types")
    identity = DpcDownloadIdentity(
        product_type=document["product_type"],
        product_time_ms=_product_time(document["product_time_ms"]),
        bucket=document["bucket"],
        key=document["key"],
        expires_seconds=document["expires_seconds"],
        object_origin=document["object_origin"],
        object_path=document["object_path"],
    )
    if (
        document["schema"] != DPC_DOWNLOAD_IDENTITY_SCHEMA
        or document["presigned_query_persisted"] is not False
        or identity.product_type != DPC_PRODUCT_TYPE
        or identity.bucket != DPC_BUCKET
        or identity.object_origin != DPC_S3_ORIGIN
        or identity.object_path != "/" + quote(identity.key, safe="/-_.")
        or not 1 <= identity.expires_seconds <= 900
    ):
        raise DpcNativeCorruptionError("DPC downloadidentiteit is inconsistent")
    _validate_key(identity.key, product_time_ms=identity.product_time_ms)
    if _identity_bytes(identity) != payload:
        raise DpcNativeCorruptionError("DPC downloadidentiteit is niet canonical")
    return identity


def _ifd_scalar(
    entry: tuple[int, int, int, bytes],
    *,
    endian: str,
    tag: int,
) -> int:
    _entry_tag, field_type, count, raw = entry
    if count != 1 or field_type not in {3, 4}:
        raise DpcNativeContractError(f"DPC TIFF-tag {tag} is niet scalair")
    if field_type == 3:
        return struct.unpack(f"{endian}H", raw[:2])[0]
    return struct.unpack(f"{endian}I", raw)[0]


def _inspect_geotiff(payload: bytes) -> DpcTiffIdentity:
    if not isinstance(payload, bytes) or not 14 <= len(payload) <= MAX_GEOTIFF_BYTES:
        raise DpcNativeCapacityError("DPC GeoTIFF valt buiten de bytegrens")
    if payload[:2] == b"II":
        endian, byte_order = "<", "little_endian"
    elif payload[:2] == b"MM":
        endian, byte_order = ">", "big_endian"
    else:
        raise DpcNativeContractError("DPC object is geen TIFF-container")
    if struct.unpack(f"{endian}H", payload[2:4])[0] != 42:
        raise DpcNativeContractError("DPC TIFF magic wijkt af")
    offset = struct.unpack(f"{endian}I", payload[4:8])[0]
    if offset < 8 or offset + 2 > len(payload):
        raise DpcNativeContractError("DPC TIFF IFD-offset is ongeldig")
    count = struct.unpack(f"{endian}H", payload[offset : offset + 2])[0]
    if not 1 <= count <= MAX_IFD_ENTRIES:
        raise DpcNativeCapacityError("DPC TIFF IFD-entrygrens is overschreden")
    end = offset + 2 + count * 12 + 4
    if end > len(payload):
        raise DpcNativeContractError("DPC TIFF IFD is afgekapt")
    entries: dict[int, tuple[int, int, int, bytes]] = {}
    for index in range(count):
        start = offset + 2 + index * 12
        tag, field_type, item_count = struct.unpack(
            f"{endian}HHI", payload[start : start + 8]
        )
        if tag in entries:
            raise DpcNativeContractError("DPC TIFF bevat dubbele tags")
        if item_count <= 0 or item_count > 10_000_000:
            raise DpcNativeCapacityError("DPC TIFF tagcount is onbegrensd")
        entries[tag] = (tag, field_type, item_count, payload[start + 8 : start + 12])
    next_ifd = struct.unpack(f"{endian}I", payload[end - 4 : end])[0]
    if next_ifd != 0:
        raise DpcNativeContractError("DPC TIFF bevat meerdere directories")
    if not _REQUIRED_TIFF_TAGS.issubset(entries):
        raise DpcNativeContractError("DPC GeoTIFF mist verplichte rastertags")
    exact_profiles = {
        258: (3, 1),
        259: (3, 1),
        273: (4, 1),
        277: (3, 1),
        278: (4, 1),
        279: (4, 1),
        339: (3, 1),
        33550: (12, 3),
        33922: (12, 6),
    }
    for tag, profile in exact_profiles.items():
        if (entries[tag][1], entries[tag][2]) != profile:
            raise DpcNativeContractError(
                f"DPC TIFF-tag {tag} heeft een onverwacht type/count"
            )
    if entries[256][1] not in {3, 4} or entries[256][2] != 1:
        raise DpcNativeContractError("DPC TIFF-widthprofiel wijkt af")
    if entries[257][1] not in {3, 4} or entries[257][2] != 1:
        raise DpcNativeContractError("DPC TIFF-heightprofiel wijkt af")
    if (
        entries[34735][1] != 3
        or not 4 <= entries[34735][2] <= 256
        or entries[34735][2] % 4 != 0
    ):
        raise DpcNativeContractError("DPC GeoKeyDirectory-profiel wijkt af")
    width = _ifd_scalar(entries[256], endian=endian, tag=256)
    height = _ifd_scalar(entries[257], endian=endian, tag=257)
    bits = _ifd_scalar(entries[258], endian=endian, tag=258)
    compression = _ifd_scalar(entries[259], endian=endian, tag=259)
    samples = _ifd_scalar(entries[277], endian=endian, tag=277)
    sample_format = _ifd_scalar(entries[339], endian=endian, tag=339)
    if (width, height, bits, compression, samples, sample_format) != (
        1200,
        1400,
        32,
        5,
        1,
        3,
    ):
        raise DpcNativeContractError("DPC SRI GeoTIFF-profiel wijkt af")
    rows_per_strip = _ifd_scalar(entries[278], endian=endian, tag=278)
    strip_offset = _ifd_scalar(entries[273], endian=endian, tag=273)
    strip_bytes = _ifd_scalar(entries[279], endian=endian, tag=279)
    if (
        rows_per_strip != height
        or strip_offset < 8
        or strip_bytes <= 0
        or strip_offset + strip_bytes > offset
    ):
        raise DpcNativeContractError("DPC TIFF-stripprofiel wijkt af")
    return DpcTiffIdentity(
        byte_order=byte_order,
        width=width,
        height=height,
        bits_per_sample=bits,
        compression=compression,
        samples_per_pixel=samples,
        sample_format=sample_format,
        ifd_entries=count,
        georeference_tags=tuple(sorted(_REQUIRED_TIFF_TAGS & {33550, 33922, 34735})),
    )


def _validate_job(job: Any) -> str:
    job_id = getattr(job, "job_id", None)
    if (
        not isinstance(job_id, str)
        or job_id != job_id.strip()
        or _JOB_ID_RE.fullmatch(job_id) is None
    ):
        raise ValueError("DPC capturejob mist een begrensde job_id")
    payload = getattr(job, "payload", None)
    if type(payload) is not dict or payload:
        raise ValueError("DPC native capture vereist een exact lege jobpayload")
    return job_id


def _raw_store(injected: RawArtifactStore | None) -> RawArtifactStore:
    if injected is not None:
        injected.initialize()
        return injected
    share = DPC_RAW_SHARE_ROOT.resolve(strict=False)
    root = DPC_RAW_ARTIFACT_ROOT.resolve(strict=False)
    if root == share or share not in root.parents:
        raise DpcNativeCorruptionError("DPC raw store ontsnapt uit worker-share")
    global _RAW_STORE
    with _RAW_STORE_LOCK:
        if _RAW_STORE is None:
            _RAW_STORE = RawArtifactStore(root)
            _RAW_STORE.initialize()
        return _RAW_STORE


def _safe_auxiliary_directory(store: RawArtifactStore) -> Path:
    root = Path(store.root)
    if root.is_symlink() or not root.is_dir():
        raise DpcNativeCorruptionError("DPC raw root is onveilig")
    directory = root / "dpc_native_locks"
    directory.mkdir(mode=0o700, exist_ok=True)
    if directory.is_symlink() or directory.resolve().parent != root.resolve():
        raise DpcNativeCorruptionError("DPC lockmap ontsnapt uit raw root")
    return directory


@contextmanager
def _deadline_thread_lock(*, deadline: float) -> Iterator[None]:
    acquired = _CAPTURE_LOCK.acquire(timeout=_remaining(deadline))
    if not acquired:
        raise DpcNativeDeadlineError("DPC capturethreadlock overschreed de deadline")
    try:
        _remaining(deadline)
        yield
    finally:
        _CAPTURE_LOCK.release()


@contextmanager
def _deadline_file_lock(
    store: RawArtifactStore,
    *,
    deadline: float,
) -> Iterator[None]:
    path = _safe_auxiliary_directory(store) / "capture-capacity.lock"
    if path.exists() and (path.is_symlink() or not path.is_file()):
        raise DpcNativeCorruptionError("DPC lockpad is onveilig")
    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_BINARY", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags, 0o600)
    acquired = False
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode):
            raise DpcNativeCorruptionError("DPC lock is geen regulier bestand")
        if info.st_size == 0:
            os.write(descriptor, b"\x00")
            os.fsync(descriptor)
        while not acquired:
            try:
                os.lseek(descriptor, 0, os.SEEK_SET)
                if os.name == "nt":  # pragma: no cover - platform selected
                    import msvcrt

                    msvcrt.locking(descriptor, msvcrt.LK_NBLCK, 1)
                else:  # pragma: no branch - platform selected
                    import fcntl

                    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                acquired = True
            except OSError as exc:
                if exc.errno not in {
                    errno.EACCES,
                    errno.EAGAIN,
                    errno.EDEADLK,
                    errno.EWOULDBLOCK,
                }:
                    raise
                time.sleep(min(LOCK_POLL_SECONDS, _remaining(deadline)))
        _remaining(deadline)
        yield
    finally:
        if acquired:
            os.lseek(descriptor, 0, os.SEEK_SET)
            if os.name == "nt":  # pragma: no cover - platform selected
                import msvcrt

                msvcrt.locking(descriptor, msvcrt.LK_UNLCK, 1)
            else:  # pragma: no branch - platform selected
                import fcntl

                fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def _receipt_id(store: RawArtifactStore, job_id: str, role: str) -> str:
    product = {
        "catalog_json": DPC_CATALOG_PRODUCT,
        "geotiff": DPC_GEOTIFF_PRODUCT,
    }[role]
    return store._receipt_id(  # noqa: SLF001 - deterministic raw-store contract
        provider="dpc_radar",
        product=product,
        fetch_id=f"{job_id}:{role}",
    )


def _capture_id(store: RawArtifactStore, job_id: str) -> str:
    return store._capture_id(  # noqa: SLF001 - deterministic raw-store contract
        provider="dpc_radar",
        product=DPC_CAPTURE_PRODUCT,
        fetch_id=f"{job_id}:capture",
    )


def _catalog_provenance() -> dict[str, object]:
    return {
        "representation": NATIVE_REPRESENTATION,
        "capture_stage": NATIVE_CAPTURE_STAGE,
        "collector": DPC_COLLECTOR,
        "collector_version": DPC_COLLECTOR_VERSION,
        "structure_inspector_version": DPC_CATALOG_INSPECTOR_VERSION,
        "operation": "findLastProductByType",
        "request_method": "GET",
        "request_product_type": DPC_PRODUCT_TYPE,
        "origin_required": DPC_WEB_ORIGIN,
    }


def _geotiff_provenance(
    identity: DpcDownloadIdentity,
    response: DpcHttpResponse,
) -> dict[str, object]:
    return {
        "representation": NATIVE_REPRESENTATION,
        "capture_stage": NATIVE_CAPTURE_STAGE,
        "collector": DPC_COLLECTOR,
        "collector_version": DPC_COLLECTOR_VERSION,
        "structure_inspector_version": DPC_TIFF_INSPECTOR_VERSION,
        "operation": "GetObject",
        "bucket": identity.bucket,
        "upstream_key": identity.key,
        "object_origin": identity.object_origin,
        "object_path": identity.object_path,
        "signed_url_expires_seconds": identity.expires_seconds,
        "presigned_url_persisted": False,
        "presigned_query_persisted": False,
        "product_type": identity.product_type,
        "product_time_ms": identity.product_time_ms,
        "content_type": response.content_type,
        "content_length": response.content_length,
        "etag": response.etag,
        "last_modified": response.last_modified,
        "semantic_class": DPC_SEMANTIC_CLASS,
        "declared_quantity": "surface_rainfall_intensity",
        "declared_unit": "mm/h",
        "pixel_values_read": False,
        "quantitative_semantics_admitted": False,
    }


def _validate_catalog_receipt(receipt: RawArtifactReceipt, *, job_id: str) -> None:
    if (
        receipt.provider != "dpc_radar"
        or receipt.product != DPC_CATALOG_PRODUCT
        or receipt.fetch_id != f"{job_id}:catalog_json"
        or receipt.content_type not in {"application/json", "text/json"}
        or receipt.size_bytes <= 0
        or receipt.size_bytes > MAX_CATALOG_BYTES
        or receipt.upstream_id
        != (
            f"radar-api.protezionecivile.it{DPC_CATALOG_PATH}?type=SRI"
            f"|content_sha256={receipt.content_sha256}"
        )
        or receipt.provenance != _catalog_provenance()
    ):
        raise DpcNativeCorruptionError("DPC catalogusreceipt wijkt af")


def _validate_geotiff_receipt(
    receipt: RawArtifactReceipt,
    *,
    job_id: str,
    identity: DpcDownloadIdentity,
) -> None:
    provenance = receipt.provenance
    etag = provenance.get("etag")
    last_modified = provenance.get("last_modified")
    if (
        receipt.provider != "dpc_radar"
        or receipt.product != DPC_GEOTIFF_PRODUCT
        or receipt.fetch_id != f"{job_id}:geotiff"
        or receipt.content_type
        not in {
            "application/octet-stream",
            "binary/octet-stream",
            "image/tiff",
            "image/geotiff",
        }
        or receipt.size_bytes <= 0
        or receipt.size_bytes > MAX_GEOTIFF_BYTES
        or receipt.upstream_id
        != (
            f"s3:{identity.bucket}:{identity.key}"
            f"|content_sha256={receipt.content_sha256}"
        )
        or provenance.get("representation") != NATIVE_REPRESENTATION
        or provenance.get("capture_stage") != NATIVE_CAPTURE_STAGE
        or provenance.get("collector") != DPC_COLLECTOR
        or provenance.get("collector_version") != DPC_COLLECTOR_VERSION
        or provenance.get("structure_inspector_version")
        != DPC_TIFF_INSPECTOR_VERSION
        or provenance.get("operation") != "GetObject"
        or provenance.get("bucket") != identity.bucket
        or provenance.get("upstream_key") != identity.key
        or provenance.get("object_origin") != identity.object_origin
        or provenance.get("object_path") != identity.object_path
        or provenance.get("signed_url_expires_seconds")
        != identity.expires_seconds
        or provenance.get("presigned_url_persisted") is not False
        or provenance.get("presigned_query_persisted") is not False
        or provenance.get("product_time_ms") != identity.product_time_ms
        or provenance.get("product_type") != DPC_PRODUCT_TYPE
        or provenance.get("content_type") != receipt.content_type
        or provenance.get("content_length") != receipt.size_bytes
        or provenance.get("semantic_class") != DPC_SEMANTIC_CLASS
        or provenance.get("declared_quantity")
        != "surface_rainfall_intensity"
        or provenance.get("declared_unit") != "mm/h"
        or (
            etag is not None
            and (not isinstance(etag, str) or not 1 <= len(etag) <= 256)
        )
        or (
            last_modified is not None
            and (
                not isinstance(last_modified, str)
                or not 1 <= len(last_modified) <= 128
            )
        )
        or provenance.get("pixel_values_read") is not False
        or provenance.get("quantitative_semantics_admitted") is not False
    ):
        raise DpcNativeCorruptionError("DPC GeoTIFF-receipt wijkt af")


def _identity_from_geotiff_receipt(
    receipt: RawArtifactReceipt,
    *,
    catalog: DpcCatalogIdentity,
) -> DpcDownloadIdentity:
    provenance = receipt.provenance
    identity = DpcDownloadIdentity(
        product_type=provenance.get("product_type"),
        product_time_ms=provenance.get("product_time_ms"),
        bucket=provenance.get("bucket"),
        key=provenance.get("upstream_key"),
        expires_seconds=provenance.get("signed_url_expires_seconds"),
        object_origin=provenance.get("object_origin"),
        object_path=provenance.get("object_path"),
    )
    try:
        canonical = _inspect_identity_bytes(_identity_bytes(identity))
    except (DpcNativeError, TypeError, ValueError) as exc:
        raise DpcNativeCorruptionError(
            "DPC GeoTIFF-receipt bevat geen geldige selectie-identiteit"
        ) from exc
    if canonical.product_time_ms != catalog.product_time_ms:
        raise DpcNativeCorruptionError("DPC receipt/catalogusselectie wijkt af")
    return canonical


def _upstream_capture_id(
    receipts: Mapping[str, RawArtifactReceipt],
    identity: DpcDownloadIdentity,
) -> str:
    return (
        f"{DPC_CAPTURE_PRODUCT}|product_time_ms={identity.product_time_ms}"
        f"|key={identity.key}"
        f"|catalog={receipts['catalog_json'].content_sha256}"
        f"|geotiff={receipts['geotiff'].content_sha256}"
    )


def _validate_capture(
    capture: RawArtifactCaptureReceipt,
    *,
    job_id: str,
    receipts: Mapping[str, RawArtifactReceipt],
    identity: DpcDownloadIdentity,
) -> None:
    members = {item.role: item.receipt_id for item in capture.members}
    if (
        capture.raw_store_schema_version != RAW_ARTIFACT_SCHEMA_VERSION
        or capture.provider != "dpc_radar"
        or capture.product != DPC_CAPTURE_PRODUCT
        or capture.fetch_id != f"{job_id}:capture"
        or capture.product_timestamp != identity.product_time_ms / 1000.0
        or capture.upstream_capture_id != _upstream_capture_id(receipts, identity)
        or members
        != {role: receipt.receipt_id for role, receipt in receipts.items()}
    ):
        raise DpcNativeCorruptionError("DPC complete capture wijkt af")


def _source_reference(receipt: RawArtifactReceipt, *, role: str) -> dict[str, object]:
    return {
        "role": role,
        "receipt_id": receipt.receipt_id,
        "content_sha256": receipt.content_sha256,
        "receipt_sha256": receipt.receipt_sha256,
        "size_bytes": receipt.size_bytes,
        "received_at": receipt.received_at,
    }


def _capture_result(
    capture: RawArtifactCaptureReceipt,
    *,
    receipts: Mapping[str, RawArtifactReceipt],
    catalog: DpcCatalogIdentity,
    identity: DpcDownloadIdentity,
    tiff: DpcTiffIdentity,
) -> dict[str, object]:
    return {
        "method": "dpc_sri_native_capture_v1",
        "capture_state": "captured_not_decoded",
        "weather_state": "unknown",
        "decode_status": "not_decoded",
        "pixel_values_read": False,
        "forecast_eligible": False,
        "operational_admission_eligible": False,
        "admission_status": DPC_ADMISSION_STATUS,
        "raw_provenance": {
            "raw_store_schema_version": RAW_ARTIFACT_SCHEMA_VERSION,
            "raw_capture_id": capture.capture_id,
            "raw_capture_manifest_sha256": capture.manifest_sha256,
            "raw_capture_receipt_sha256": capture.capture_receipt_sha256,
            "upstream_capture_id": capture.upstream_capture_id,
            "provenance_grade": "exact_native_members_with_secret_free_selection_identity",
            "provenance_scope": (
                "exact_catalog_json_exact_original_geotiff_selection_bound_in_receipt"
            ),
            "product_type": catalog.product_type,
            "product_period": catalog.period,
            "product_time_ms": catalog.product_time_ms,
            "bucket": identity.bucket,
            "selected_key": identity.key,
            "selection_identity_schema": DPC_DOWNLOAD_IDENTITY_SCHEMA,
            "selection_identity": identity.to_mapping(),
            "presigned_url_persisted": False,
            "raw_member_count": len(receipts),
            "sources": {
                role: _source_reference(receipt, role=role)
                for role, receipt in sorted(receipts.items())
            },
            "container": {
                "inspector_version": DPC_TIFF_INSPECTOR_VERSION,
                "byte_order": tiff.byte_order,
                "width": tiff.width,
                "height": tiff.height,
                "bits_per_sample": tiff.bits_per_sample,
                "compression": tiff.compression,
                "samples_per_pixel": tiff.samples_per_pixel,
                "sample_format": tiff.sample_format,
                "ifd_entries": tiff.ifd_entries,
                "georeference_tags": list(tiff.georeference_tags),
                "pixel_values_read": False,
            },
        },
    }


def _load_optional(
    store: RawArtifactStore,
    *,
    job_id: str,
    role: str,
) -> tuple[RawArtifactReceipt | None, bytes | None]:
    loaded = store.load_receipt_bytes(_receipt_id(store, job_id, role))
    if loaded is None:
        return None, None
    return loaded


def capture_latest_dpc_sri(
    job: Any,
    *,
    raw_store: RawArtifactStore | None = None,
    deadline_at: float | None = None,
    _fault_injector: Callable[[str], None] | None = None,
) -> dict[str, object]:
    """Capture one complete official SRI authority without decoding pixels."""
    job_id = _validate_job(job)
    now = time.monotonic()
    if deadline_at is None:
        deadline = now + CAPTURE_TOTAL_DEADLINE_SECONDS
    else:
        if (
            isinstance(deadline_at, bool)
            or not isinstance(deadline_at, (int, float))
            or not math.isfinite(float(deadline_at))
        ):
            raise ValueError("DPC deadline_at is ongeldig")
        deadline = min(float(deadline_at), now + CAPTURE_TOTAL_DEADLINE_SECONDS)

    _remaining(deadline)
    store = _raw_store(raw_store)
    with _deadline_thread_lock(deadline=deadline), _deadline_file_lock(
        store, deadline=deadline
    ):
        _remaining(deadline)
        complete = store.load_capture_bundle_bytes(_capture_id(store, job_id))
        if complete is not None:
            capture, receipts, payloads = complete
            if set(receipts) != {
                "catalog_json",
                "geotiff",
            } or set(payloads) != set(receipts):
                raise DpcNativeCorruptionError("DPC capture mist verplichte rollen")
            _validate_catalog_receipt(receipts["catalog_json"], job_id=job_id)
            catalog = _inspect_catalog(payloads["catalog_json"])
            identity = _identity_from_geotiff_receipt(
                receipts["geotiff"], catalog=catalog
            )
            _validate_geotiff_receipt(
                receipts["geotiff"], job_id=job_id, identity=identity
            )
            tiff = _inspect_geotiff(payloads["geotiff"])
            _validate_capture(
                capture,
                job_id=job_id,
                receipts=receipts,
                identity=identity,
            )
            _remaining(deadline)
            return _capture_result(
                capture,
                receipts=receipts,
                catalog=catalog,
                identity=identity,
                tiff=tiff,
            )

        receipts: dict[str, RawArtifactReceipt] = {}
        catalog_receipt, catalog_payload = _load_optional(
            store, job_id=job_id, role="catalog_json"
        )
        if catalog_receipt is None:
            response = _fetch_catalog(deadline=deadline)
            digest = hashlib.sha256(response.payload).hexdigest()
            catalog_receipt = store.store(
                provider="dpc_radar",
                product=DPC_CATALOG_PRODUCT,
                fetch_id=f"{job_id}:catalog_json",
                content_type=response.content_type,
                received_at=response.received_at,
                upstream_id=(
                    f"radar-api.protezionecivile.it{DPC_CATALOG_PATH}?type=SRI"
                    f"|content_sha256={digest}"
                ),
                provenance=_catalog_provenance(),
                payload=response.payload,
            )
            catalog_payload = response.payload
            if _fault_injector is not None:
                _fault_injector("dpc.after_catalog_store")
        else:
            _validate_catalog_receipt(catalog_receipt, job_id=job_id)
        if catalog_payload is None:
            raise DpcNativeCorruptionError("DPC catalogusbytes ontbreken")
        receipts["catalog_json"] = catalog_receipt

        # Exact catalog bytes are durable before selection.
        catalog = _inspect_catalog(catalog_payload)
        _remaining(deadline)

        geotiff_receipt, geotiff_payload = _load_optional(
            store, job_id=job_id, role="geotiff"
        )
        if geotiff_receipt is None:
            envelope = _fetch_download_envelope(
                catalog.product_time_ms, deadline=deadline
            )
            identity, signed_url = _inspect_download_envelope(
                envelope.payload, catalog=catalog
            )
            # Canonicalize the secret-free identity in memory. It is receipt
            # provenance, never a falsely labelled raw member.
            identity = _inspect_identity_bytes(_identity_bytes(identity))
            if _fault_injector is not None:
                _fault_injector("dpc.after_download_identity_validation")
            response = _fetch_geotiff(signed_url, identity, deadline=deadline)
            digest = hashlib.sha256(response.payload).hexdigest()
            geotiff_receipt = store.store(
                provider="dpc_radar",
                product=DPC_GEOTIFF_PRODUCT,
                fetch_id=f"{job_id}:geotiff",
                content_type=response.content_type,
                received_at=response.received_at,
                upstream_id=(
                    f"s3:{identity.bucket}:{identity.key}"
                    f"|content_sha256={digest}"
                ),
                provenance=_geotiff_provenance(identity, response),
                payload=response.payload,
            )
            geotiff_payload = response.payload
            if _fault_injector is not None:
                _fault_injector("dpc.after_geotiff_store")
        else:
            identity = _identity_from_geotiff_receipt(
                geotiff_receipt, catalog=catalog
            )
            _validate_geotiff_receipt(
                geotiff_receipt, job_id=job_id, identity=identity
            )
        if geotiff_payload is None:
            raise DpcNativeCorruptionError("DPC GeoTIFF-bytes ontbreken")
        receipts["geotiff"] = geotiff_receipt

        # Exact original GeoTIFF is durable before container inspection.
        tiff = _inspect_geotiff(geotiff_payload)
        _remaining(deadline)
        if _fault_injector is not None:
            _fault_injector("dpc.before_capture_commit")
        capture = store.commit_capture(
            provider="dpc_radar",
            product=DPC_CAPTURE_PRODUCT,
            fetch_id=f"{job_id}:capture",
            upstream_capture_id=_upstream_capture_id(receipts, identity),
            product_timestamp=catalog.product_timestamp,
            members={role: receipt.receipt_id for role, receipt in receipts.items()},
        )
        if _fault_injector is not None:
            _fault_injector("dpc.after_capture_commit")
        _validate_capture(
            capture,
            job_id=job_id,
            receipts=receipts,
            identity=identity,
        )
        _remaining(deadline)
        return _capture_result(
            capture,
            receipts=receipts,
            catalog=catalog,
            identity=identity,
            tiff=tiff,
        )


__all__ = [
    "DPC_ADMISSION_STATUS",
    "DPC_CAPTURE_PRODUCT",
    "DPC_CATALOG_PRODUCT",
    "DPC_GEOTIFF_PRODUCT",
    "DpcCatalogIdentity",
    "DpcDownloadIdentity",
    "DpcHttpResponse",
    "DpcNativeCapacityError",
    "DpcNativeContractError",
    "DpcNativeCorruptionError",
    "DpcNativeDeadlineError",
    "DpcNativeError",
    "DpcTiffIdentity",
    "capture_latest_dpc_sri",
]
