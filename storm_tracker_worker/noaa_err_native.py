"""Exact, non-operational NOAA Enterprise Rain Rate native capture.

The candidate retains the exact Amazon S3 ``ListObjectsV2`` XML document and
the exact selected NetCDF-4 object before either document is inspected.  It
performs only bounded transport and structural identity checks.  It never
reads a precipitation pixel and never emits a wet/dry, cell, observation,
forecast or operational-admission fact.

This module is intentionally not registered as a production job handler.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
import hashlib
import io
import math
from pathlib import Path
import re
import struct
import threading
import time
from typing import Any, Mapping
from urllib.parse import quote, urlencode, unquote_to_bytes, urlsplit
from urllib.request import (
    HTTPRedirectHandler,
    ProxyHandler,
    Request,
    build_opener,
)
import xml.etree.ElementTree as ET

import h5py
import numpy as np

from .raw_artifacts import (
    NATIVE_CAPTURE_STAGE,
    NATIVE_REPRESENTATION,
    RawArtifactCaptureReceipt,
    RawArtifactNotFoundError,
    RawArtifactReceipt,
    RawArtifactStore,
)


NOAA_ERR_BUCKET_NAME = "noaa-enterprise-rainrate-pds"
NOAA_ERR_S3_HOST = f"{NOAA_ERR_BUCKET_NAME}.s3.amazonaws.com"
NOAA_ERR_S3_ORIGIN = f"https://{NOAA_ERR_S3_HOST}"
NOAA_ERR_PRODUCT_PREFIX = "BLEND/RainRate-Blend-INST"
NOAA_ERR_LIST_MAX_KEYS = 1000
NOAA_ERR_XML_NAMESPACE = "http://s3.amazonaws.com/doc/2006-03-01/"

MAX_LISTING_XML_BYTES = 1024 * 1024
MAX_NETCDF4_BYTES = 32 * 1024 * 1024
MAX_KEY_BYTES = 1024
MAX_HTTP_HEADER_CHARS = 8192
HTTP_IO_TIMEOUT_SECONDS = 20.0
HTTP_TOTAL_DEADLINE_SECONDS = 45.0

NOAA_ERR_ALLOWED_LIST_CONTENT_TYPES = frozenset({
    "application/xml",
    "text/xml",
})
NOAA_ERR_ALLOWED_OBJECT_CONTENT_TYPES = frozenset({
    "application/netcdf",
    "application/octet-stream",
    "application/x-netcdf",
    "binary/octet-stream",
})

NOAA_ERR_GRID_ROWS = 6501
NOAA_ERR_GRID_COLUMNS = 18000
NOAA_ERR_GRID_RESOLUTION_DEGREES = 0.02
NOAA_ERR_FINAL_BLEND_INPUTS = frozenset({
    "GOES-19",
    "GOES-18",
    "Himawari-9",
    "MSG2",
    "MSG3",
})

NOAA_ERR_RAW_SHARE_ROOT = Path("/share/storm_tracker_worker")
NOAA_ERR_RAW_ARTIFACT_ROOT = (
    NOAA_ERR_RAW_SHARE_ROOT / "raw" / "noaa_enterprise_rainrate"
)
NOAA_ERR_LISTING_PRODUCT = "noaa_err_s3_listobjects_v2_xml"
NOAA_ERR_NETCDF4_PRODUCT = "noaa_err_instantaneous_glb5_netcdf4"
NOAA_ERR_CAPTURE_PRODUCT = "noaa_err_listing_and_netcdf4_capture"
NOAA_ERR_RAW_COLLECTOR = "noaa_err_public_s3_native"
NOAA_ERR_RAW_COLLECTOR_VERSION = "worker-noaa-err-exact-raw-v1"
NOAA_ERR_XML_INSPECTOR_VERSION = "s3-listobjectsv2-noaa-err-v1"
NOAA_ERR_STRUCTURE_INSPECTOR_VERSION = "noaa-err-netcdf4-golden-v2"
NOAA_ERR_SEMANTIC_CLASS = "satellite_precipitation_rate_product"
NOAA_ERR_ADMISSION_STATUS = "exact_raw_decoder_not_certified"


class NoaaErrNativeProfileError(ValueError):
    """Base class for typed, payload-free native-profile failures."""


class NoaaErrNativeUnsupportedError(NoaaErrNativeProfileError):
    """The bytes are readable but do not match the certified product profile."""

    deterministic = True
    retryable = False


class NoaaErrNativeCorruptionError(NoaaErrNativeProfileError):
    """The bytes cannot be safely interpreted as the declared HDF5 product."""

    # Corruption and lower-level HDF5 I/O failures share this public type.  Do
    # not make them immutable: a retry must be allowed to distinguish a
    # transient reader/storage failure from repeatable bad source bytes.
    deterministic = False
    retryable = True

_KEY_RE = re.compile(
    r"^BLEND/RainRate-Blend-INST/"
    r"(?P<year>\d{4})/(?P<month>\d{2})/(?P<day>\d{2})/(?P<hour>\d{2})/"
    r"RRQPE-INST-GLB-(?P<inputs>[1-9])_"
    r"v(?P<major>\d+)r(?P<revision>\d+)_blend_"
    r"s(?P<start>\d{15})_e(?P<end>\d{15})_c(?P<created>\d{15})\.nc$"
)
_ETAG_RE = re.compile(r'^"[0-9a-f]{32}(?:-[1-9]\d*)?"$')
_JOB_ID_RE = re.compile(r"^[^\x00-\x1f\x7f]{1,480}$")
_RAW_STORE: RawArtifactStore | None = None
_RAW_STORE_LOCK = threading.Lock()
_CAPTURE_LOCK = threading.Lock()
_INSPECTION_REJECTION_CACHE_LOCK = threading.Lock()
_INSPECTION_REJECTION_CACHE_MAX = 1024
_INSPECTION_REJECTION_CACHE: dict[
    tuple[str, str, str, str], tuple[type[NoaaErrNativeProfileError], str]
] = {}


@dataclass(frozen=True, slots=True)
class S3HttpResponse:
    """Exact response bytes plus bounded non-secret HTTP transport facts."""

    payload: bytes
    content_type: str
    content_length: int
    declared_content_length: int | None
    received_at: float
    etag: str | None = None
    last_modified: str | None = None
    last_modified_timestamp: float | None = None
    version_id: str | None = None
    request_id: str | None = None
    request_id_2: str | None = None
    server_side_encryption: str | None = None


@dataclass(frozen=True, slots=True)
class NoaaErrObject:
    """One strict S3 listing entry for the complete five-input blend."""

    key: str
    etag: str
    last_modified: str
    last_modified_timestamp: float
    size_bytes: int
    storage_class: str
    checksum_algorithms: tuple[str, ...]
    checksum_type: str | None
    product_start: float
    product_end: float
    created: float
    version: str


@dataclass(frozen=True, slots=True)
class NoaaErrStructureIdentity:
    """Bounded container identity; no dataset element has been read."""

    product_start: float
    product_end: float
    created: float
    rows: int
    columns: int
    rrqpe_dtype: str
    dqf_dtype: str
    platform_names: tuple[str, ...]
    conventions: str
    title: str


class _RejectRedirects(HTTPRedirectHandler):
    """Never turn the fixed public S3 request into another network target."""

    def redirect_request(self, *_args, **_kwargs):
        raise ValueError("NOAA ERR S3-redirect is niet toegestaan")


def _open_without_redirects(request: Request, *, timeout: float):
    opener = build_opener(ProxyHandler({}), _RejectRedirects())
    return opener.open(request, timeout=timeout)


def _validate_job_id(value: object) -> str:
    if not isinstance(value, str) or value != value.strip() or not _JOB_ID_RE.fullmatch(value):
        raise ValueError("NOAA ERR capturejob mist een geldige job_id")
    return value


def _validate_empty_payload(value: object) -> None:
    if type(value) is not dict or value:
        raise ValueError("NOAA ERR native capture vereist een exact lege jobpayload")


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
            raise ValueError(f"NOAA ERR HTTP-header {name} ontbreekt")
        return None
    text = str(value).strip()
    if (
        not text
        or len(text) > maximum
        or any(ord(character) < 0x20 or ord(character) == 0x7F for character in text)
    ):
        raise ValueError(f"NOAA ERR HTTP-header {name} is ongeldig")
    return text


def _content_type(headers: Any) -> str:
    value = _bounded_header(headers, "Content-Type", required=True, maximum=256)
    assert value is not None
    return value.split(";", 1)[0].strip().lower()


def _declared_content_length(
    headers: Any,
    *,
    maximum_bytes: int,
    required: bool,
) -> int | None:
    value = _bounded_header(
        headers, "Content-Length", required=required, maximum=32
    )
    if value is None:
        return None
    if not value.isascii() or not value.isdecimal():
        raise ValueError("NOAA ERR Content-Length is ongeldig")
    result = int(value)
    if result <= 0 or result > maximum_bytes:
        raise ValueError("NOAA ERR Content-Length valt buiten de vaste grens")
    return result


def _rfc3339_timestamp(value: str, *, context: str) -> float:
    if not isinstance(value, str) or not value or len(value) > 64:
        raise ValueError(f"{context} bevat geen begrensde RFC3339-tijd")
    candidate = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        moment = datetime.fromisoformat(candidate)
    except ValueError as err:
        raise ValueError(f"{context} bevat geen geldige RFC3339-tijd") from err
    if moment.tzinfo is None or moment.utcoffset() is None:
        raise ValueError(f"{context} mist een tijdzone")
    timestamp = moment.timestamp()
    if not math.isfinite(timestamp):
        raise ValueError(f"{context} bevat geen eindige tijd")
    return timestamp


def _http_timestamp(value: str, *, context: str) -> float:
    try:
        moment = parsedate_to_datetime(value)
    except (TypeError, ValueError) as err:
        raise ValueError(f"{context} bevat geen geldige HTTP-datum") from err
    if moment.tzinfo is None or moment.utcoffset() is None:
        raise ValueError(f"{context} mist een tijdzone")
    timestamp = moment.timestamp()
    if not math.isfinite(timestamp):
        raise ValueError(f"{context} bevat geen eindige tijd")
    return timestamp


def _validate_fixed_s3_url(
    url: str,
    *,
    expected_path: str,
    expected_query: str,
) -> str:
    if not isinstance(url, str) or len(url) > 8192:
        raise ValueError("NOAA ERR S3-URL is ongeldig")
    parsed = urlsplit(url)
    try:
        port = parsed.port
    except ValueError as err:
        raise ValueError("NOAA ERR S3-URL is ongeldig") from err
    if (
        parsed.scheme != "https"
        or parsed.hostname != NOAA_ERR_S3_HOST
        or parsed.username is not None
        or parsed.password is not None
        or port not in {None, 443}
        or parsed.path != expected_path
        or parsed.query != expected_query
        or bool(parsed.fragment)
        or parsed.netloc not in {NOAA_ERR_S3_HOST, f"{NOAA_ERR_S3_HOST}:443"}
    ):
        raise ValueError("NOAA ERR S3-URL wijkt af van vaste publieke origin/pad")
    return url


def _read_bounded_body(
    response: Any,
    *,
    declared_bytes: int | None,
    maximum_bytes: int,
) -> bytes:
    deadline = time.monotonic() + HTTP_TOTAL_DEADLINE_SECONDS
    chunks: list[bytes] = []
    total = 0
    while total <= maximum_bytes:
        if time.monotonic() > deadline:
            raise TimeoutError("NOAA ERR S3-download overschrijdt totale deadline")
        requested = min(64 * 1024, maximum_bytes + 1 - total)
        chunk = response.read(requested)
        if time.monotonic() > deadline:
            raise TimeoutError("NOAA ERR S3-download overschrijdt totale deadline")
        if not chunk:
            break
        if not isinstance(chunk, (bytes, bytearray, memoryview)):
            raise TypeError("NOAA ERR S3-responsstream moet bytes opleveren")
        immutable = bytes(chunk)
        chunks.append(immutable)
        total += len(immutable)
        if declared_bytes is not None and total > declared_bytes:
            raise ValueError("NOAA ERR S3-body is groter dan Content-Length")
        if total > maximum_bytes:
            raise ValueError("NOAA ERR S3-body overschrijdt de vaste grens")
    payload = b"".join(chunks)
    if declared_bytes is not None and len(payload) != declared_bytes:
        raise ValueError("NOAA ERR S3-body wijkt af van Content-Length")
    return payload


def _fetch_s3(
    url: str,
    *,
    expected_path: str,
    expected_query: str,
    maximum_bytes: int,
    allowed_content_types: frozenset[str],
    object_metadata_required: bool,
) -> S3HttpResponse:
    validated_url = _validate_fixed_s3_url(
        url,
        expected_path=expected_path,
        expected_query=expected_query,
    )
    request = Request(
        validated_url,
        headers={
            "Accept": ",".join(sorted(allowed_content_types)),
            "Accept-Encoding": "identity",
            "User-Agent": "StormTrackerWorker/noaa-err-native-candidate-v1",
        },
    )
    with _open_without_redirects(request, timeout=HTTP_IO_TIMEOUT_SECONDS) as response:
        if getattr(response, "status", None) != 200:
            raise ValueError("NOAA ERR S3-response heeft geen HTTP 200-status")
        geturl = getattr(response, "geturl", None)
        if not callable(geturl) or str(geturl()) != validated_url:
            raise ValueError("NOAA ERR S3-response kwam van een onverwacht endpoint")
        headers = getattr(response, "headers", None)
        encoding = _bounded_header(
            headers, "Content-Encoding", required=False, maximum=64
        )
        if encoding is not None and encoding.lower() != "identity":
            raise ValueError("onverwachte NOAA ERR S3-transportcompressie")
        content_type = _content_type(headers)
        if content_type not in allowed_content_types:
            raise ValueError("NOAA ERR S3-response heeft onverwacht Content-Type")
        declared = _declared_content_length(
            headers,
            maximum_bytes=maximum_bytes,
            required=object_metadata_required,
        )
        etag = _bounded_header(headers, "ETag", required=object_metadata_required, maximum=256)
        if etag is not None and _ETAG_RE.fullmatch(etag.lower()) is None:
            raise ValueError("NOAA ERR S3-response heeft een ongeldige ETag")
        last_modified = _bounded_header(
            headers, "Last-Modified", required=object_metadata_required, maximum=128
        )
        last_modified_timestamp = (
            _http_timestamp(last_modified, context="NOAA ERR Last-Modified")
            if last_modified is not None
            else None
        )
        payload = _read_bounded_body(
            response,
            declared_bytes=declared,
            maximum_bytes=maximum_bytes,
        )
        return S3HttpResponse(
            payload=payload,
            content_type=content_type,
            content_length=len(payload),
            declared_content_length=declared,
            received_at=time.time(),
            etag=etag.lower() if etag is not None else None,
            last_modified=last_modified,
            last_modified_timestamp=last_modified_timestamp,
            version_id=_bounded_header(headers, "x-amz-version-id", maximum=1024),
            request_id=_bounded_header(headers, "x-amz-request-id", maximum=256),
            request_id_2=_bounded_header(headers, "x-amz-id-2", maximum=2048),
            server_side_encryption=_bounded_header(
                headers, "x-amz-server-side-encryption", maximum=128
            ),
        )


def _listing_prefix(now: datetime | None = None) -> str:
    moment = datetime.now(timezone.utc) if now is None else now
    if not isinstance(moment, datetime) or moment.tzinfo is None or moment.utcoffset() is None:
        raise ValueError("NOAA ERR listingtijd moet tijdzonebewust zijn")
    stable_day = moment.astimezone(timezone.utc) - timedelta(minutes=30)
    return f"{NOAA_ERR_PRODUCT_PREFIX}/{stable_day:%Y/%m/%d}/"


def _listing_request(prefix: str) -> tuple[str, str, str]:
    if not isinstance(prefix, str) or not re.fullmatch(
        r"BLEND/RainRate-Blend-INST/\d{4}/\d{2}/\d{2}/", prefix
    ):
        raise ValueError("NOAA ERR listingprefix is ongeldig")
    query = urlencode((
        ("list-type", "2"),
        ("prefix", prefix),
        ("max-keys", str(NOAA_ERR_LIST_MAX_KEYS)),
        ("encoding-type", "url"),
    ))
    url = f"{NOAA_ERR_S3_ORIGIN}/?{query}"
    return _validate_fixed_s3_url(url, expected_path="/", expected_query=query), "/", query


def _fetch_listing(prefix: str) -> S3HttpResponse:
    url, path, query = _listing_request(prefix)
    return _fetch_s3(
        url,
        expected_path=path,
        expected_query=query,
        maximum_bytes=MAX_LISTING_XML_BYTES,
        allowed_content_types=NOAA_ERR_ALLOWED_LIST_CONTENT_TYPES,
        object_metadata_required=False,
    )


def _safe_object_key(value: str, *, prefix: str) -> tuple[str, re.Match[str]]:
    if not isinstance(value, str):
        raise ValueError("NOAA ERR S3-object mist een tekstuele sleutel")
    encoded = value.encode("utf-8", errors="strict")
    if (
        not value
        or len(encoded) > MAX_KEY_BYTES
        or not value.isascii()
        or "\\" in value
        or "//" in value
        or any(part in {"", ".", ".."} for part in value.split("/"))
        or any(ord(character) < 0x20 or ord(character) == 0x7F for character in value)
        or not value.startswith(prefix)
    ):
        raise ValueError("NOAA ERR S3-listing bevat een onveilige objectsleutel")
    match = _KEY_RE.fullmatch(value)
    if match is None:
        raise ValueError("NOAA ERR S3-listing bevat een onbekende productsleutel")
    return value, match


def _filename_timestamp(value: str, *, context: str) -> float:
    if not isinstance(value, str) or not re.fullmatch(r"\d{15}", value):
        raise ValueError(f"{context} bevat geen NOAA-producttijd")
    try:
        base = datetime.strptime(value[:14], "%Y%m%d%H%M%S").replace(
            tzinfo=timezone.utc
        )
    except ValueError as err:
        raise ValueError(f"{context} bevat een ongeldige NOAA-producttijd") from err
    return base.timestamp() + int(value[14]) / 10.0


def _decode_url_text(value: str, *, context: str) -> str:
    try:
        decoded = unquote_to_bytes(value).decode("utf-8", errors="strict")
    except (UnicodeDecodeError, ValueError) as err:
        raise ValueError(f"{context} bevat ongeldige URL-encoding") from err
    if quote(decoded, safe="/-_.") != value:
        raise ValueError(f"{context} is niet canoniek URL-gecodeerd")
    return decoded


def _xml_name(element: ET.Element) -> str:
    prefix = f"{{{NOAA_ERR_XML_NAMESPACE}}}"
    if not isinstance(element.tag, str) or not element.tag.startswith(prefix):
        raise ValueError("NOAA ERR listing gebruikt een onverwachte XML-namespace")
    return element.tag[len(prefix):]


def _xml_scalar(element: ET.Element, *, field: str, maximum: int = 4096) -> str:
    if element.attrib or len(element):
        raise ValueError(f"NOAA ERR XML-veld {field} heeft onverwachte structuur")
    value = element.text or ""
    if (
        len(value) > maximum
        or any(
            (ord(character) < 0x20 and character not in "\t\r\n")
            or ord(character) == 0x7F
            for character in value
        )
    ):
        raise ValueError(f"NOAA ERR XML-veld {field} is onbegrensd of ongeldig")
    return value


def _one_xml_field(
    fields: Mapping[str, list[ET.Element]],
    name: str,
    *,
    required: bool = True,
    maximum: int = 4096,
) -> str | None:
    values = fields.get(name, [])
    if len(values) != (1 if required else min(1, len(values))):
        if required or values:
            raise ValueError(f"NOAA ERR XML-veld {name} ontbreekt of is dubbel")
        return None
    if not values:
        return None
    return _xml_scalar(values[0], field=name, maximum=maximum)


def _group_xml_children(
    element: ET.Element,
    *,
    allowed: frozenset[str],
) -> dict[str, list[ET.Element]]:
    if element.attrib:
        raise ValueError("NOAA ERR XML-element bevat onverwachte attributen")
    result: dict[str, list[ET.Element]] = {}
    for child in element:
        name = _xml_name(child)
        if name not in allowed:
            raise ValueError(f"NOAA ERR XML bevat onbekend veld {name}")
        result.setdefault(name, []).append(child)
    return result


def _parse_listing_xml(payload: bytes, *, expected_prefix: str) -> NoaaErrObject:
    """Parse a previously persisted exact S3 listing and select final GLB-5."""

    if not isinstance(payload, bytes) or not payload or len(payload) > MAX_LISTING_XML_BYTES:
        raise ValueError("NOAA ERR listing-XML is leeg, niet exact bytes of te groot")
    lowered = payload.lower()
    if b"<!doctype" in lowered or b"<!entity" in lowered:
        raise ValueError("NOAA ERR listing-XML bevat verboden DTD/entityconstructie")
    try:
        payload.decode("utf-8", errors="strict")
        root = ET.fromstring(payload)
    except (UnicodeDecodeError, ET.ParseError) as err:
        raise ValueError("NOAA ERR listing is geen veilige geldige UTF-8 XML") from err
    if _xml_name(root) != "ListBucketResult":
        raise ValueError("NOAA ERR listing heeft niet de S3 ListBucketResult-root")
    fields = _group_xml_children(
        root,
        allowed=frozenset({
            "Name",
            "Prefix",
            "KeyCount",
            "MaxKeys",
            "EncodingType",
            "IsTruncated",
            "Contents",
        }),
    )
    name = _one_xml_field(fields, "Name", maximum=256)
    encoding = _one_xml_field(fields, "EncodingType", maximum=32)
    prefix_value = _one_xml_field(fields, "Prefix", maximum=2048)
    if name != NOAA_ERR_BUCKET_NAME or encoding != "url":
        raise ValueError("NOAA ERR listing hoort niet bij vast bucket/encodingcontract")
    assert prefix_value is not None
    if _decode_url_text(prefix_value, context="NOAA ERR Prefix") != expected_prefix:
        raise ValueError("NOAA ERR listingprefix wijkt af van de aanvraag")
    if _one_xml_field(fields, "IsTruncated", maximum=8) != "false":
        raise ValueError("NOAA ERR listing is getrunceerd; selectie is niet compleet")

    def integer_field(field: str, *, maximum: int) -> int:
        value = _one_xml_field(fields, field, maximum=32)
        assert value is not None
        if not value.isascii() or not value.isdecimal():
            raise ValueError(f"NOAA ERR XML-veld {field} is geen geheel getal")
        number = int(value)
        if number < 0 or number > maximum:
            raise ValueError(f"NOAA ERR XML-veld {field} valt buiten de grens")
        return number

    max_keys = integer_field("MaxKeys", maximum=NOAA_ERR_LIST_MAX_KEYS)
    if max_keys != NOAA_ERR_LIST_MAX_KEYS:
        raise ValueError("NOAA ERR listing bevestigt niet de vaste max-keys")
    key_count = integer_field("KeyCount", maximum=NOAA_ERR_LIST_MAX_KEYS)
    contents = fields.get("Contents", [])
    if key_count != len(contents):
        raise ValueError("NOAA ERR listing KeyCount stemt niet overeen met Contents")

    candidates: list[NoaaErrObject] = []
    seen_keys: set[str] = set()
    allowed_contents = frozenset({
        "Key",
        "LastModified",
        "ETag",
        "ChecksumAlgorithm",
        "ChecksumType",
        "Size",
        "StorageClass",
    })
    for entry in contents:
        entry_fields = _group_xml_children(entry, allowed=allowed_contents)
        for singular in (
            "Key", "LastModified", "ETag", "ChecksumType", "Size", "StorageClass"
        ):
            if len(entry_fields.get(singular, [])) > 1:
                raise ValueError(f"NOAA ERR Contents bevat dubbel veld {singular}")
        encoded_key = _one_xml_field(entry_fields, "Key", maximum=MAX_KEY_BYTES * 3)
        assert encoded_key is not None
        decoded_key = _decode_url_text(encoded_key, context="NOAA ERR Key")
        key, match = _safe_object_key(decoded_key, prefix=expected_prefix)
        if key in seen_keys:
            raise ValueError("NOAA ERR listing bevat een dubbele objectsleutel")
        seen_keys.add(key)
        if match.group("inputs") != "5":
            continue
        last_modified = _one_xml_field(entry_fields, "LastModified", maximum=64)
        etag = _one_xml_field(entry_fields, "ETag", maximum=256)
        size_text = _one_xml_field(entry_fields, "Size", maximum=32)
        storage_class = _one_xml_field(entry_fields, "StorageClass", maximum=64)
        assert last_modified is not None and etag is not None
        assert size_text is not None and storage_class is not None
        normalized_etag = etag.lower()
        if _ETAG_RE.fullmatch(normalized_etag) is None:
            raise ValueError("NOAA ERR listing bevat een ongeldige ETag")
        if not size_text.isascii() or not size_text.isdecimal():
            raise ValueError("NOAA ERR listing bevat een ongeldige objectgrootte")
        size_bytes = int(size_text)
        if size_bytes <= 0 or size_bytes > MAX_NETCDF4_BYTES:
            raise ValueError("NOAA ERR objectgrootte valt buiten NetCDF-4-grens")
        if storage_class != "STANDARD":
            raise ValueError("NOAA ERR object gebruikt een onverwachte StorageClass")
        algorithms = tuple(
            _xml_scalar(item, field="ChecksumAlgorithm", maximum=32)
            for item in entry_fields.get("ChecksumAlgorithm", [])
        )
        if len(algorithms) != len(set(algorithms)) or any(
            algorithm not in {"CRC32", "CRC32C", "SHA1", "SHA256"}
            for algorithm in algorithms
        ):
            raise ValueError("NOAA ERR listing bevat ongeldige checksummetadata")
        checksum_type = _one_xml_field(
            entry_fields, "ChecksumType", required=False, maximum=32
        )
        if checksum_type not in {None, "FULL_OBJECT", "COMPOSITE"}:
            raise ValueError("NOAA ERR listing bevat onbekend checksumtype")
        if checksum_type is not None and not algorithms:
            raise ValueError("NOAA ERR checksumtype ontbreekt bij algoritme")

        product_start = _filename_timestamp(
            match.group("start"), context="NOAA ERR bestandsnaam-start"
        )
        product_end = _filename_timestamp(
            match.group("end"), context="NOAA ERR bestandsnaam-einde"
        )
        created = _filename_timestamp(
            match.group("created"), context="NOAA ERR bestandsnaam-creatie"
        )
        duration = product_end - product_start
        if not 599.0 <= duration <= 600.0:
            raise ValueError("NOAA ERR sleutel is geen tienminutenproduct")
        if created < product_end or created - product_end > 60 * 60:
            raise ValueError("NOAA ERR sleutel heeft ongeldige creatietijd")
        key_prefix = datetime.fromtimestamp(product_start, timezone.utc).strftime(
            f"{NOAA_ERR_PRODUCT_PREFIX}/%Y/%m/%d/%H/"
        )
        if not key.startswith(key_prefix):
            raise ValueError("NOAA ERR sleutelpad en producttijd verschillen")
        candidates.append(NoaaErrObject(
            key=key,
            etag=normalized_etag,
            last_modified=last_modified,
            last_modified_timestamp=_rfc3339_timestamp(
                last_modified, context="NOAA ERR LastModified"
            ),
            size_bytes=size_bytes,
            storage_class=storage_class,
            checksum_algorithms=algorithms,
            checksum_type=checksum_type,
            product_start=product_start,
            product_end=product_end,
            created=created,
            version=f"v{match.group('major')}r{match.group('revision')}",
        ))
    if not candidates:
        raise ValueError("NOAA ERR listing bevat geen complete GLB-5 NetCDF-4")
    return max(
        candidates,
        key=lambda item: (
            item.product_start,
            item.created,
            item.last_modified_timestamp,
            item.key,
        ),
    )


def _object_request(selected: NoaaErrObject) -> tuple[str, str, str]:
    if not isinstance(selected, NoaaErrObject):
        raise TypeError("NOAA ERR objectdownload vereist gevalideerde listingmetadata")
    key, match = _safe_object_key(
        selected.key,
        prefix=f"{NOAA_ERR_PRODUCT_PREFIX}/",
    )
    if match.group("inputs") != "5":
        raise ValueError("NOAA ERR objectdownload vereist de complete GLB-5-blend")
    parsed_start = _filename_timestamp(
        match.group("start"), context="NOAA ERR objectstart"
    )
    parsed_end = _filename_timestamp(
        match.group("end"), context="NOAA ERR objecteinde"
    )
    parsed_created = _filename_timestamp(
        match.group("created"), context="NOAA ERR objectcreatie"
    )
    parsed_version = f"v{match.group('major')}r{match.group('revision')}"
    if (
        selected.product_start != parsed_start
        or selected.product_end != parsed_end
        or selected.created != parsed_created
        or selected.version != parsed_version
        or _ETAG_RE.fullmatch(selected.etag) is None
        or selected.size_bytes <= 0
        or selected.size_bytes > MAX_NETCDF4_BYTES
        or selected.storage_class != "STANDARD"
        or selected.last_modified_timestamp != _rfc3339_timestamp(
            selected.last_modified,
            context="NOAA ERR geselecteerde LastModified",
        )
    ):
        raise ValueError("NOAA ERR geselecteerde objectmetadata is intern inconsistent")
    path = "/" + quote(key, safe="/-_.")
    url = f"{NOAA_ERR_S3_ORIGIN}{path}"
    return _validate_fixed_s3_url(url, expected_path=path, expected_query=""), path, ""


def _fetch_object(selected: NoaaErrObject) -> S3HttpResponse:
    url, path, query = _object_request(selected)
    response = _fetch_s3(
        url,
        expected_path=path,
        expected_query=query,
        maximum_bytes=MAX_NETCDF4_BYTES,
        allowed_content_types=NOAA_ERR_ALLOWED_OBJECT_CONTENT_TYPES,
        object_metadata_required=True,
    )
    if response.content_length != selected.size_bytes:
        raise ValueError("NOAA ERR HTTP-grootte wijkt af van S3-listing")
    if response.etag != selected.etag:
        raise ValueError("NOAA ERR HTTP-ETag wijkt af van S3-listing")
    if response.last_modified_timestamp != selected.last_modified_timestamp:
        raise ValueError("NOAA ERR HTTP Last-Modified wijkt af van S3-listing")
    return response


def _raw_store(injected: RawArtifactStore | None = None) -> RawArtifactStore:
    if injected is not None:
        injected.initialize()
        return injected
    share_root = NOAA_ERR_RAW_SHARE_ROOT.resolve(strict=False)
    raw_root = NOAA_ERR_RAW_ARTIFACT_ROOT.resolve(strict=False)
    if raw_root == share_root or share_root not in raw_root.parents:
        raise RuntimeError("NOAA ERR raw-artifactpad ontsnapt uit worker-share")
    global _RAW_STORE
    with _RAW_STORE_LOCK:
        if _RAW_STORE is None:
            candidate = RawArtifactStore(raw_root)
            candidate.initialize()
            _RAW_STORE = candidate
        return _RAW_STORE


def _http_provenance(response: S3HttpResponse) -> dict[str, object]:
    return {
        "content_type": response.content_type,
        "content_length": response.content_length,
        "declared_content_length": response.declared_content_length,
        "etag": response.etag,
        "last_modified": response.last_modified,
        "version_id": response.version_id,
        "request_id": response.request_id,
        "request_id_2": response.request_id_2,
        "server_side_encryption": response.server_side_encryption,
    }


def _listing_provenance(prefix: str, response: S3HttpResponse) -> dict[str, object]:
    return {
        "representation": NATIVE_REPRESENTATION,
        "capture_stage": NATIVE_CAPTURE_STAGE,
        "collector": NOAA_ERR_RAW_COLLECTOR,
        "collector_version": NOAA_ERR_RAW_COLLECTOR_VERSION,
        "structure_inspector_version": NOAA_ERR_XML_INSPECTOR_VERSION,
        "bucket": NOAA_ERR_BUCKET_NAME,
        "operation": "ListObjectsV2",
        "listing_prefix": prefix,
        "list_type": 2,
        "max_keys": NOAA_ERR_LIST_MAX_KEYS,
        "encoding_type": "url",
        "http": _http_provenance(response),
    }


def _object_provenance(
    selected: NoaaErrObject,
    response: S3HttpResponse,
) -> dict[str, object]:
    return {
        "representation": NATIVE_REPRESENTATION,
        "capture_stage": NATIVE_CAPTURE_STAGE,
        "collector": NOAA_ERR_RAW_COLLECTOR,
        "collector_version": NOAA_ERR_RAW_COLLECTOR_VERSION,
        "structure_inspector_version": NOAA_ERR_STRUCTURE_INSPECTOR_VERSION,
        "bucket": NOAA_ERR_BUCKET_NAME,
        "upstream_key": selected.key,
        "listing_etag": selected.etag,
        "listing_last_modified": selected.last_modified,
        "listing_size_bytes": selected.size_bytes,
        "listing_storage_class": selected.storage_class,
        "listing_checksum_algorithms": list(selected.checksum_algorithms),
        "listing_checksum_type": selected.checksum_type,
        "filename_product_start": selected.product_start,
        "filename_product_end": selected.product_end,
        "filename_created": selected.created,
        "filename_version": selected.version,
        "declared_semantic_class": NOAA_ERR_SEMANTIC_CLASS,
        "declared_quantity": "instantaneous_precipitation_rate",
        "declared_unit": "mm/h",
        "quantitative_semantics_verified_after_decode": False,
        "http": _http_provenance(response),
    }


def _scalar_attribute(attributes: Any, name: str) -> Any:
    if name not in attributes:
        raise ValueError(f"NOAA ERR NetCDF-4 mist attribuut {name}")
    value = attributes[name]
    if hasattr(value, "shape") and getattr(value, "shape", ()) not in {(), (1,)}:
        raise ValueError(f"NOAA ERR attribuut {name} is niet scalair")
    if hasattr(value, "item"):
        try:
            value = value.item()
        except ValueError as err:
            raise ValueError(f"NOAA ERR attribuut {name} is niet scalair") from err
    return value


def _text_attribute(attributes: Any, name: str, *, maximum: int = 4096) -> str:
    value = _scalar_attribute(attributes, name)
    if isinstance(value, bytes):
        try:
            value = value.decode("utf-8", errors="strict")
        except UnicodeDecodeError as err:
            raise ValueError(f"NOAA ERR attribuut {name} is geen UTF-8") from err
    if (
        not isinstance(value, str)
        or not value
        or len(value) > maximum
        or "\x00" in value
    ):
        raise ValueError(f"NOAA ERR attribuut {name} is geen begrensde tekst")
    return value.strip()


def _number_attribute(attributes: Any, name: str) -> float:
    value = _scalar_attribute(attributes, name)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"NOAA ERR attribuut {name} is niet numeriek")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"NOAA ERR attribuut {name} is niet eindig")
    return result


def _exact_float32_attribute(attributes: Any, name: str, expected: float) -> None:
    """Require the exact IEEE-754 float32 value used by the live product."""

    if name not in attributes:
        raise ValueError(f"NOAA ERR NetCDF-4 mist attribuut {name}")
    value = np.asarray(attributes[name])
    if value.shape != (1,) or value.dtype.kind != "f" or value.dtype.itemsize != 4:
        raise ValueError(f"NOAA ERR attribuut {name} is niet exact float32")
    expected_bits = struct.pack(">f", float(expected))
    candidate_bits = struct.pack(">f", float(value[0]))
    if candidate_bits != expected_bits:
        raise ValueError(f"NOAA ERR NetCDF-4 heeft afwijkende {name}")


_INPUT_FILE_PATTERNS = {
    "GOES-19": re.compile(r"^RRQPE-ABI-INST_v1r1_g19_s\d{15}_e\d{15}_c\d{15}\.nc$"),
    "GOES-18": re.compile(r"^RRQPE-ABI-INST_v1r1_g18_s\d{15}_e\d{15}_c\d{15}\.nc$"),
    "Himawari-9": re.compile(r"^RRQPE-AHI-INST_v1r1_h09_s\d{15}_e\d{15}_c\d{15}\.nc$"),
    "MSG2": re.compile(r"^RRQPE-SEVIRI-INST_v1r1_met9_s\d{15}_e\d{15}_c\d{15}\.nc$"),
    "MSG3": re.compile(r"^RRQPE-SEVIRI-INST_v1r1_met10_s\d{15}_e\d{15}_c\d{15}\.nc$"),
}


def _validate_complete_input_files(attributes: Any) -> tuple[str, ...]:
    if _number_attribute(attributes, "Number_of_Input_Files") != 5.0:
        raise ValueError("NOAA ERR monitoring bevestigt geen vijf inputs")
    if _number_attribute(attributes, "Number_of_Missing_Inputs") != 0.0:
        raise ValueError("NOAA ERR monitoring meldt ontbrekende inputs")
    filenames = tuple(
        part for part in _text_attribute(
            attributes, "Input_Files", maximum=4096
        ).split() if part
    )
    if len(filenames) != 5 or len(set(filenames)) != 5:
        raise ValueError("NOAA ERR monitoring bevat geen vijf unieke inputbestanden")
    matched: dict[str, str] = {}
    for filename in filenames:
        platforms = [
            platform for platform, pattern in _INPUT_FILE_PATTERNS.items()
            if pattern.fullmatch(filename)
        ]
        if len(platforms) != 1 or platforms[0] in matched:
            raise ValueError("NOAA ERR inputbestanden koppelen niet exact aan platforms")
        matched[platforms[0]] = filename
    if frozenset(matched) != NOAA_ERR_FINAL_BLEND_INPUTS:
        raise ValueError("NOAA ERR inputbestanden missen een gecertificeerd platform")
    return tuple(matched[platform] for platform in (
        "GOES-19", "GOES-18", "Himawari-9", "MSG2", "MSG3",
    ))


def _hard_dataset(root: h5py.File, name: str) -> h5py.Dataset:
    link = root.get(name, getlink=True)
    if not isinstance(link, h5py.HardLink):
        raise ValueError(f"NOAA ERR NetCDF-4-pad {name} is geen intern hard link")
    value = root.get(name)
    if not isinstance(value, h5py.Dataset):
        raise ValueError(f"NOAA ERR NetCDF-4 mist dataset {name}")
    return value


def _validate_hdf_self_contained(root: h5py.File) -> None:
    seen_groups: set[int] = set()
    object_count = 0

    def walk(group: h5py.Group, *, depth: int) -> None:
        nonlocal object_count
        if depth > 16:
            raise ValueError("NOAA ERR NetCDF-4-groepsdiepte overschrijdt de grens")
        identity = hash(group.id)
        if identity in seen_groups:
            return
        seen_groups.add(identity)
        if len(group) > 1024:
            raise ValueError("NOAA ERR NetCDF-4-groep bevat te veel objecten")
        for name in group.keys():
            object_count += 1
            if object_count > 4096:
                raise ValueError("NOAA ERR NetCDF-4 bevat te veel objecten")
            link = group.get(name, getlink=True)
            if not isinstance(link, h5py.HardLink):
                raise ValueError("NOAA ERR NetCDF-4 bevat externe of symbolische link")
            value = group.get(name)
            if isinstance(value, h5py.Group):
                walk(value, depth=depth + 1)
            elif isinstance(value, h5py.Dataset):
                if bool(value.is_virtual) or bool(value.external):
                    raise ValueError("NOAA ERR NetCDF-4 gebruikt externe datasetopslag")
            else:
                raise ValueError("NOAA ERR NetCDF-4 bevat onbekend objecttype")

    walk(root, depth=0)


def _validate_chunked_grid(dataset: h5py.Dataset, *, dtype_kind: str, itemsize: int) -> None:
    if dataset.ndim != 2 or tuple(dataset.shape) != (
        NOAA_ERR_GRID_ROWS,
        NOAA_ERR_GRID_COLUMNS,
    ):
        raise ValueError("NOAA ERR NetCDF-4 heeft een onverwachte rastervorm")
    if dataset.dtype.kind != dtype_kind or dataset.dtype.itemsize != itemsize:
        raise ValueError("NOAA ERR NetCDF-4 heeft een onverwachte rasterdtype")
    chunks = dataset.chunks
    if (
        chunks is None
        or len(chunks) != 2
        or any(
            isinstance(value, bool)
            or not isinstance(value, int)
            or value <= 0
            or value > dimension
            for value, dimension in zip(chunks, dataset.shape)
        )
        or math.prod(chunks) * dataset.dtype.itemsize > 8 * 1024 * 1024
        or dataset.compression != "gzip"
        or bool(dataset.is_virtual)
        or bool(dataset.external)
    ):
        raise ValueError("NOAA ERR NetCDF-4 rasteropslag valt buiten het contract")


def _inspect_netcdf4_identity(
    payload: bytes,
    *,
    selected: NoaaErrObject,
) -> NoaaErrStructureIdentity:
    """Inspect only HDF5 metadata/shape; never index or materialize a grid."""

    if not isinstance(payload, bytes) or not payload or len(payload) > MAX_NETCDF4_BYTES:
        raise NoaaErrNativeCorruptionError(
            "NOAA ERR NetCDF-4 is leeg, niet exact bytes of te groot"
        )
    if not payload.startswith(b"\x89HDF\r\n\x1a\n"):
        raise NoaaErrNativeCorruptionError(
            "NOAA ERR object heeft geen HDF5/NetCDF-4-signatuur"
        )
    try:
        with h5py.File(io.BytesIO(payload), "r") as root:
            _validate_hdf_self_contained(root)
            conventions = _text_attribute(root.attrs, "Conventions", maximum=64)
            title = _text_attribute(root.attrs, "title", maximum=128)
            if conventions != "CF-1.6" or title != "RRQPE_INST_GLB":
                raise ValueError("NOAA ERR NetCDF-4 heeft een onbekende productidentiteit")
            if _text_attribute(root.attrs, "production_environment", maximum=64) != "prod":
                raise ValueError("NOAA ERR NetCDF-4 is geen productieproduct")
            if _text_attribute(root.attrs, "processing_level", maximum=128) != "NOAA Level 3":
                raise ValueError("NOAA ERR NetCDF-4 is geen NOAA Level 3-product")
            platform_text = _text_attribute(root.attrs, "platform", maximum=512)
            platform_names = tuple(part.strip() for part in platform_text.split(","))
            if frozenset(platform_names) != NOAA_ERR_FINAL_BLEND_INPUTS or len(platform_names) != 5:
                raise ValueError("NOAA ERR NetCDF-4 is niet de complete vijf-inputblend")
            product_start = _rfc3339_timestamp(
                _text_attribute(root.attrs, "time_coverage_start", maximum=64),
                context="NOAA ERR time_coverage_start",
            )
            product_end = _rfc3339_timestamp(
                _text_attribute(root.attrs, "time_coverage_end", maximum=64),
                context="NOAA ERR time_coverage_end",
            )
            created = _rfc3339_timestamp(
                _text_attribute(root.attrs, "date_created", maximum=64),
                context="NOAA ERR date_created",
            )
            if product_start != selected.product_start:
                raise ValueError("NOAA ERR sleutel en NetCDF-4-starttijd verschillen")
            if not 530.0 <= product_end - product_start <= 600.0:
                raise ValueError("NOAA ERR NetCDF-4 is geen tienminutenvenster")
            if not 0.0 <= selected.product_end - product_end <= 61.0:
                raise ValueError("NOAA ERR sleutel en NetCDF-4-eindtijd verschillen")
            if abs(created - selected.created) > 1.0:
                raise ValueError("NOAA ERR sleutel en NetCDF-4-creatietijd verschillen")
            expected_numbers = {
                "geospatial_lat_min": -60.0,
                "geospatial_lat_max": 70.0,
                "geospatial_lon_min": -180.0,
                "geospatial_lon_max": 179.98,
                "geospatial_lat_resolution": NOAA_ERR_GRID_RESOLUTION_DEGREES,
                "geospatial_lon_resolution": NOAA_ERR_GRID_RESOLUTION_DEGREES,
            }
            for name, expected in expected_numbers.items():
                _exact_float32_attribute(root.attrs, name, expected)

            rows = _hard_dataset(root, "Rows")
            columns = _hard_dataset(root, "Columns")
            rrqpe = _hard_dataset(root, "RRQPE")
            dqf = _hard_dataset(root, "DQF")
            monitoring = _hard_dataset(root, "MonitoringMetaData")
            _hard_dataset(root, "quality_information")
            if (
                tuple(rows.shape) != (NOAA_ERR_GRID_ROWS,)
                or rows.dtype.kind != "f"
                or rows.dtype.itemsize != 4
                or tuple(columns.shape) != (NOAA_ERR_GRID_COLUMNS,)
                or columns.dtype.kind != "f"
                or columns.dtype.itemsize != 4
            ):
                raise ValueError("NOAA ERR NetCDF-4 dimensieschalen wijken af")
            _validate_chunked_grid(rrqpe, dtype_kind="i", itemsize=2)
            _validate_chunked_grid(dqf, dtype_kind="i", itemsize=1)
            if not math.isclose(
                _number_attribute(rrqpe.attrs, "scale_factor"), 0.1,
                rel_tol=0.0, abs_tol=1e-6,
            ) or _number_attribute(rrqpe.attrs, "add_offset") != 0.0:
                raise ValueError("NOAA ERR RRQPE schaalmetadata wijkt af")
            if _number_attribute(rrqpe.attrs, "_FillValue") != -9990.0:
                raise ValueError("NOAA ERR RRQPE fillwaarde wijkt af")
            if _text_attribute(rrqpe.attrs, "units", maximum=32) != "mm/h":
                raise ValueError("NOAA ERR RRQPE eenheid wijkt af")
            if _number_attribute(dqf.attrs, "_FillValue") != -1.0:
                raise ValueError("NOAA ERR DQF fillwaarde wijkt af")
            _validate_complete_input_files(monitoring.attrs)
            rrqpe_dtype = str(rrqpe.dtype)
            dqf_dtype = str(dqf.dtype)
    except NoaaErrNativeProfileError:
        raise
    except OSError as err:
        raise NoaaErrNativeCorruptionError(
            "NOAA ERR object is geen geldige NetCDF-4/HDF5-container"
        ) from err
    except (ValueError, TypeError, KeyError, UnicodeError) as err:
        raise NoaaErrNativeUnsupportedError(str(err)) from err

    return NoaaErrStructureIdentity(
        product_start=product_start,
        product_end=product_end,
        created=created,
        rows=NOAA_ERR_GRID_ROWS,
        columns=NOAA_ERR_GRID_COLUMNS,
        rrqpe_dtype=rrqpe_dtype,
        dqf_dtype=dqf_dtype,
        platform_names=platform_names,
        conventions=conventions,
        title=title,
    )


def _inspect_netcdf4_identity_cached(
    payload: bytes,
    *,
    selected: NoaaErrObject,
    content_sha256: str,
) -> NoaaErrStructureIdentity:
    """Cache only immutable deterministic rejection for exact source bytes."""

    cache_key = (
        content_sha256,
        selected.key,
        selected.etag,
        NOAA_ERR_STRUCTURE_INSPECTOR_VERSION,
    )
    with _INSPECTION_REJECTION_CACHE_LOCK:
        cached = _INSPECTION_REJECTION_CACHE.get(cache_key)
    if cached is not None:
        error_type, message = cached
        raise error_type(message)
    try:
        return _inspect_netcdf4_identity(payload, selected=selected)
    except NoaaErrNativeUnsupportedError as error:
        with _INSPECTION_REJECTION_CACHE_LOCK:
            if len(_INSPECTION_REJECTION_CACHE) < _INSPECTION_REJECTION_CACHE_MAX:
                _INSPECTION_REJECTION_CACHE.setdefault(
                    cache_key, (type(error), str(error))
                )
        raise


def _validate_complete_capture_contract(
    capture: RawArtifactCaptureReceipt,
    *,
    receipts: Mapping[str, RawArtifactReceipt],
    selected: NoaaErrObject,
    identity: NoaaErrStructureIdentity,
) -> None:
    expected_upstream_id = (
        f"{NOAA_ERR_CAPTURE_PRODUCT}|key={selected.key}|etag={selected.etag}"
        f"|listing={receipts['listing_xml'].content_sha256}"
        f"|netcdf4={receipts['netcdf4'].content_sha256}"
    )
    if (
        capture.provider != "noaa_enterprise_rainrate"
        or capture.product != NOAA_ERR_CAPTURE_PRODUCT
        or not capture.fetch_id.endswith(":capture")
        or capture.product_timestamp != identity.product_start
        or capture.upstream_capture_id != expected_upstream_id
        or frozenset(receipts) != frozenset({"listing_xml", "netcdf4"})
    ):
        raise RuntimeError("duurzaam NOAA ERR capturecontract wijkt af")


def _optional_receipt_bytes(
    store: RawArtifactStore,
    receipt_id: str,
) -> tuple[RawArtifactReceipt | None, bytes | None]:
    try:
        return store.read_receipt_bytes(receipt_id)
    except RawArtifactNotFoundError:
        return None, None


def _listing_receipt_id(store: RawArtifactStore, job_id: str) -> str:
    return store._receipt_id(  # noqa: SLF001 - deterministic worker store contract
        provider="noaa_enterprise_rainrate",
        product=NOAA_ERR_LISTING_PRODUCT,
        fetch_id=f"{job_id}:listing_xml",
    )


def _netcdf_receipt_id(store: RawArtifactStore, job_id: str) -> str:
    return store._receipt_id(  # noqa: SLF001 - deterministic worker store contract
        provider="noaa_enterprise_rainrate",
        product=NOAA_ERR_NETCDF4_PRODUCT,
        fetch_id=f"{job_id}:netcdf4",
    )


def _validate_listing_receipt(
    receipt: RawArtifactReceipt,
    *,
    job_id: str,
) -> str:
    provenance = receipt.provenance
    prefix = provenance.get("listing_prefix")
    http = provenance.get("http")
    if not isinstance(prefix, str):
        raise RuntimeError("duurzaam NOAA ERR listingreceipt mist prefix")
    expected_upstream = (
        f"s3-list-v2://{NOAA_ERR_BUCKET_NAME}/{prefix}"
        f"|max_keys={NOAA_ERR_LIST_MAX_KEYS}"
        f"|content_sha256={receipt.content_sha256}"
    )
    if (
        receipt.provider != "noaa_enterprise_rainrate"
        or receipt.product != NOAA_ERR_LISTING_PRODUCT
        or receipt.fetch_id != f"{job_id}:listing_xml"
        or receipt.upstream_id != expected_upstream
        or receipt.content_type not in NOAA_ERR_ALLOWED_LIST_CONTENT_TYPES
        or receipt.size_bytes <= 0
        or receipt.size_bytes > MAX_LISTING_XML_BYTES
        or provenance.get("bucket") != NOAA_ERR_BUCKET_NAME
        or provenance.get("operation") != "ListObjectsV2"
        or provenance.get("list_type") != 2
        or provenance.get("max_keys") != NOAA_ERR_LIST_MAX_KEYS
        or provenance.get("encoding_type") != "url"
        or not isinstance(http, dict)
        or http.get("content_type") != receipt.content_type
        or http.get("content_length") != receipt.size_bytes
        or http.get("declared_content_length") not in {
            None,
            receipt.size_bytes,
        }
    ):
        raise RuntimeError("duurzaam NOAA ERR listingreceipt heeft verkeerde identiteit")
    _listing_request(prefix)
    return prefix


def _validate_netcdf_receipt(
    receipt: RawArtifactReceipt,
    *,
    job_id: str,
    selected: NoaaErrObject,
) -> None:
    provenance = receipt.provenance
    http = provenance.get("http")
    if (
        receipt.provider != "noaa_enterprise_rainrate"
        or receipt.product != NOAA_ERR_NETCDF4_PRODUCT
        or receipt.fetch_id != f"{job_id}:netcdf4"
        or receipt.upstream_id != f"s3://{NOAA_ERR_BUCKET_NAME}/{selected.key}"
        or receipt.content_type not in NOAA_ERR_ALLOWED_OBJECT_CONTENT_TYPES
        or receipt.size_bytes != selected.size_bytes
        or provenance.get("upstream_key") != selected.key
        or provenance.get("listing_etag") != selected.etag
        or provenance.get("listing_last_modified") != selected.last_modified
        or provenance.get("listing_size_bytes") != selected.size_bytes
        or provenance.get("listing_storage_class") != selected.storage_class
        or provenance.get("filename_product_start") != selected.product_start
        or provenance.get("filename_product_end") != selected.product_end
        or provenance.get("filename_created") != selected.created
        or not isinstance(http, dict)
        or http.get("content_type") != receipt.content_type
        or http.get("content_length") != selected.size_bytes
        or http.get("declared_content_length") != selected.size_bytes
        or http.get("etag") != selected.etag
        or not isinstance(http.get("last_modified"), str)
        or _http_timestamp(
            http["last_modified"], context="duurzame NOAA ERR Last-Modified"
        ) != selected.last_modified_timestamp
    ):
        raise RuntimeError("duurzaam NOAA ERR NetCDF-receipt heeft verkeerde identiteit")


def _source_reference(receipt: RawArtifactReceipt, *, role: str) -> dict[str, object]:
    return {
        "role": role,
        "raw_receipt_id": receipt.receipt_id,
        "raw_content_sha256": receipt.content_sha256,
        "raw_receipt_sha256": receipt.receipt_sha256,
        "fetch_id": receipt.fetch_id,
        "upstream_record_id": receipt.upstream_id,
        "received_at": receipt.received_at,
        "content_type": receipt.content_type,
        "size_bytes": receipt.size_bytes,
        "collector_version": NOAA_ERR_RAW_COLLECTOR_VERSION,
    }


def _capture_result(
    capture: RawArtifactCaptureReceipt,
    *,
    listing_receipt: RawArtifactReceipt,
    netcdf_receipt: RawArtifactReceipt,
    selected: NoaaErrObject,
    identity: NoaaErrStructureIdentity,
) -> dict[str, object]:
    return {
        "provider": "noaa_enterprise_rainrate",
        "product": NOAA_ERR_NETCDF4_PRODUCT,
        "declared_semantic_class": NOAA_ERR_SEMANTIC_CLASS,
        "declared_quantity": "instantaneous_precipitation_rate",
        "declared_unit": "mm/h",
        "declared_product_start_timestamp": identity.product_start,
        "declared_product_end_timestamp": identity.product_end,
        "decode_status": "not_decoded",
        "capture_state": "captured_not_decoded",
        "weather_state": "unknown",
        "quantitative_semantics_eligible": False,
        "operational_admission_eligible": False,
        "admission_status": NOAA_ERR_ADMISSION_STATUS,
        "raw_provenance": {
            "raw_store_schema_version": capture.raw_store_schema_version,
            "raw_capture_id": capture.capture_id,
            "raw_capture_manifest_sha256": capture.manifest_sha256,
            "raw_capture_receipt_sha256": capture.capture_receipt_sha256,
            "raw_capture_fetch_id": capture.fetch_id,
            "upstream_capture_id": capture.upstream_capture_id,
            "product_timestamp": identity.product_start,
            "collector_version": NOAA_ERR_RAW_COLLECTOR_VERSION,
            "structure_inspector_versions": {
                "listing_xml": NOAA_ERR_XML_INSPECTOR_VERSION,
                "netcdf4": NOAA_ERR_STRUCTURE_INSPECTOR_VERSION,
            },
            "provenance_grade": "exact_raw",
            "provenance_scope": "s3_listing_xml_and_selected_netcdf4_bytes",
            "selection": {
                "bucket": NOAA_ERR_BUCKET_NAME,
                "key": selected.key,
                "etag": selected.etag,
                "last_modified": selected.last_modified,
                "size_bytes": selected.size_bytes,
                "storage_class": selected.storage_class,
                "checksum_algorithms": list(selected.checksum_algorithms),
                "checksum_type": selected.checksum_type,
                "filename_version": selected.version,
                "complete_blend_inputs": 5,
            },
            "structure": {
                "rows": identity.rows,
                "columns": identity.columns,
                "rrqpe_dtype": identity.rrqpe_dtype,
                "dqf_dtype": identity.dqf_dtype,
                "platform_names": list(identity.platform_names),
                "conventions": identity.conventions,
                "title": identity.title,
                "pixel_values_read": False,
            },
            "raw_sources": {
                "listing_xml": _source_reference(
                    listing_receipt, role="listing_xml"
                ),
                "netcdf4": _source_reference(netcdf_receipt, role="netcdf4"),
            },
        },
    }


def capture_latest_noaa_err_netcdf4(
    job,
    *,
    raw_store: RawArtifactStore | None = None,
    now: datetime | None = None,
) -> dict[str, object]:
    """Capture one complete ERR blend as raw evidence, never weather output."""

    job_id = _validate_job_id(getattr(job, "job_id", None))
    _validate_empty_payload(getattr(job, "payload", None))
    store = _raw_store(raw_store)

    with _CAPTURE_LOCK:
        capture_id = store._capture_id(  # noqa: SLF001 - deterministic store contract
            provider="noaa_enterprise_rainrate",
            product=NOAA_ERR_CAPTURE_PRODUCT,
            fetch_id=f"{job_id}:capture",
        )
        complete_bundle = store.load_capture_bundle_bytes(capture_id)
        if complete_bundle is not None:
            capture, receipts, payloads = complete_bundle
            if frozenset(receipts) != frozenset({"listing_xml", "netcdf4"}):
                raise RuntimeError("duurzaam NOAA ERR capture mist rollen")
            listing_receipt = receipts["listing_xml"]
            netcdf_receipt = receipts["netcdf4"]
            prefix = _validate_listing_receipt(listing_receipt, job_id=job_id)
            selected = _parse_listing_xml(
                payloads["listing_xml"], expected_prefix=prefix
            )
            _validate_netcdf_receipt(
                netcdf_receipt, job_id=job_id, selected=selected
            )
            identity = _inspect_netcdf4_identity_cached(
                payloads["netcdf4"],
                selected=selected,
                content_sha256=netcdf_receipt.content_sha256,
            )
            _validate_complete_capture_contract(
                capture,
                receipts=receipts,
                selected=selected,
                identity=identity,
            )
            return _capture_result(
                capture,
                listing_receipt=listing_receipt,
                netcdf_receipt=netcdf_receipt,
                selected=selected,
                identity=identity,
            )

        listing_receipt, listing_payload = _optional_receipt_bytes(
            store, _listing_receipt_id(store, job_id)
        )
        netcdf_receipt, netcdf_payload = _optional_receipt_bytes(
            store, _netcdf_receipt_id(store, job_id)
        )
        if netcdf_receipt is not None and listing_receipt is None:
            raise RuntimeError("NOAA ERR NetCDF-receipt bestaat zonder listingbewijs")

        if listing_receipt is None:
            prefix = _listing_prefix(now)
            listing_response = _fetch_listing(prefix)
            listing_sha256 = hashlib.sha256(listing_response.payload).hexdigest()
            listing_receipt = store.store(
                provider="noaa_enterprise_rainrate",
                product=NOAA_ERR_LISTING_PRODUCT,
                fetch_id=f"{job_id}:listing_xml",
                content_type=listing_response.content_type,
                received_at=listing_response.received_at,
                upstream_id=(
                    f"s3-list-v2://{NOAA_ERR_BUCKET_NAME}/{prefix}"
                    f"|max_keys={NOAA_ERR_LIST_MAX_KEYS}"
                    f"|content_sha256={listing_sha256}"
                ),
                provenance=_listing_provenance(prefix, listing_response),
                payload=listing_response.payload,
            )
            listing_payload = listing_response.payload
        else:
            prefix = _validate_listing_receipt(listing_receipt, job_id=job_id)
            if listing_payload is None:  # pragma: no cover - tuple invariant
                raise RuntimeError("NOAA ERR listingbytes ontbreken")
        # The exact XML is durable before the first XML parse.
        selected = _parse_listing_xml(listing_payload, expected_prefix=prefix)

        if netcdf_receipt is None:
            object_response = _fetch_object(selected)
            netcdf_receipt = store.store(
                provider="noaa_enterprise_rainrate",
                product=NOAA_ERR_NETCDF4_PRODUCT,
                fetch_id=f"{job_id}:netcdf4",
                content_type=object_response.content_type,
                received_at=object_response.received_at,
                upstream_id=f"s3://{NOAA_ERR_BUCKET_NAME}/{selected.key}",
                provenance=_object_provenance(selected, object_response),
                payload=object_response.payload,
            )
            netcdf_payload = object_response.payload
        else:
            _validate_netcdf_receipt(
                netcdf_receipt,
                job_id=job_id,
                selected=selected,
            )
            if netcdf_payload is None:  # pragma: no cover - tuple invariant
                raise RuntimeError("NOAA ERR NetCDF-bytes ontbreken")
        # The exact NetCDF-4 bytes are durable before HDF5 is opened.  Invalid
        # structure deliberately leaves receipts but no complete capture.
        identity = _inspect_netcdf4_identity_cached(
            netcdf_payload,
            selected=selected,
            content_sha256=netcdf_receipt.content_sha256,
        )
        capture = store.commit_capture(
            provider="noaa_enterprise_rainrate",
            product=NOAA_ERR_CAPTURE_PRODUCT,
            fetch_id=f"{job_id}:capture",
            upstream_capture_id=(
                f"{NOAA_ERR_CAPTURE_PRODUCT}|key={selected.key}"
                f"|etag={selected.etag}|listing={listing_receipt.content_sha256}"
                f"|netcdf4={netcdf_receipt.content_sha256}"
            ),
            product_timestamp=identity.product_start,
            members={
                "listing_xml": listing_receipt.receipt_id,
                "netcdf4": netcdf_receipt.receipt_id,
            },
        )
        _validate_complete_capture_contract(
            capture,
            receipts={
                "listing_xml": listing_receipt,
                "netcdf4": netcdf_receipt,
            },
            selected=selected,
            identity=identity,
        )
        return _capture_result(
            capture,
            listing_receipt=listing_receipt,
            netcdf_receipt=netcdf_receipt,
            selected=selected,
            identity=identity,
        )


__all__ = [
    "NOAA_ERR_ADMISSION_STATUS",
    "NOAA_ERR_CAPTURE_PRODUCT",
    "NOAA_ERR_LISTING_PRODUCT",
    "NOAA_ERR_NETCDF4_PRODUCT",
    "NoaaErrNativeCorruptionError",
    "NoaaErrNativeProfileError",
    "NoaaErrNativeUnsupportedError",
    "NoaaErrObject",
    "NoaaErrStructureIdentity",
    "S3HttpResponse",
    "capture_latest_noaa_err_netcdf4",
]
