"""Unwired exact-native capture candidate for NOAA GOES ABI RRQPEF.

The module deliberately stops after durable schema-3 capture and bounded
container metadata inspection.  It never reads RRQPE or DQF pixel values and
is not registered as a worker job handler.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
import errno
import hashlib
import io
import json
import math
import os
from pathlib import Path
import re
import stat
import threading
import time
from typing import Any, Callable, Iterable, Iterator, Mapping
from urllib.parse import quote, unquote_to_bytes, urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener
from xml.etree import ElementTree as ET

import h5py
import numpy as np

from .raw_artifacts import (
    NATIVE_CAPTURE_STAGE,
    NATIVE_REPRESENTATION,
    RAW_ARTIFACT_SCHEMA_VERSION,
    RawArtifactCaptureReceipt,
    RawArtifactReceipt,
    RawArtifactStore,
)


SATELLITES = (18, 19)
PRODUCT_PREFIX = "ABI-L2-RRQPEF"
S3_NAMESPACE = "http://s3.amazonaws.com/doc/2006-03-01/"
LIST_MAX_KEYS = 1_000
MAX_LISTING_BYTES = 2 * 1024 * 1024
MAX_NETCDF4_BYTES = 16 * 1024 * 1024
MAX_HTTP_HEADER_CHARS = 8_192
MAX_ROOT_MEMBERS = 128
MAX_DATASET_ATTRIBUTES = 96
MAX_DATASET_RANK = 2
MAX_DIMENSION = 10_000
HTTP_IO_TIMEOUT_SECONDS = 30.0
CAPTURE_TOTAL_DEADLINE_SECONDS = 90.0
STREAM_CHUNK_BYTES = 256 * 1024
LOCK_POLL_SECONDS = 0.02

GRID_ROWS = 5_424
GRID_COLUMNS = 5_424
FIXED_GRID_RESOLUTION = "2.0km at nadir"
PROJECTION_HEIGHT_METRES = 35_786_023.0
SEMI_MAJOR_AXIS_METRES = 6_378_137.0
SEMI_MINOR_AXIS_METRES = 6_356_752.31414
INVERSE_FLATTENING = 298.2572221
PROJECTION_LONGITUDE = {18: -137.0, 19: -75.0}
ORBITAL_SLOT = {18: "GOES-West", 19: "GOES-East"}

PROVIDER = "noaa_goes_rrqpe"
LISTING_PRODUCT = "noaa_goes_rrqpe_s3_listobjects_v2_xml"
NETCDF4_PRODUCT = "noaa_goes_rrqpe_abi_l2_original_netcdf4"
CAPTURE_PRODUCT = "noaa_goes_rrqpe_exact_native_capture"
RAW_COLLECTOR = "noaa_goes_public_s3_native"
RAW_COLLECTOR_VERSION = "worker-goes-rrqpe-exact-raw-v1"
STRUCTURE_INSPECTOR_VERSION = "goes-abi-rrqpef-netcdf4-identity-v1"
ADMISSION_STATUS = "decoder_dqf_codes_and_held_out_validation_not_certified"

OFFICIAL_MATURITY = {
    18: {
        "level": "full",
        "effective_date": "2025-01-04",
        "source": (
            "https://www.ospo.noaa.gov/operations/goes/"
            "product-quality-overview/index.html"
        ),
    },
    19: {
        "level": "provisional",
        "effective_date": "2025-02-05",
        "source": (
            "https://www.ospo.noaa.gov/operations/goes/"
            "product-quality-overview/index.html"
        ),
    },
}

RAW_SHARE_ROOT = Path("/share/storm_tracker_worker")
RAW_ARTIFACT_ROOT = RAW_SHARE_ROOT / "raw" / "goes_rrqpe_native"

_PREFIX_RE = re.compile(r"^ABI-L2-RRQPEF/\d{4}/\d{3}/\d{2}/$")
_KEY_RE = re.compile(
    r"^ABI-L2-RRQPEF/(?P<year>\d{4})/(?P<day>\d{3})/(?P<hour>\d{2})/"
    r"(?P<filename>OR_ABI-L2-RRQPEF-M(?P<mode>[346])_G(?P<satellite>18|19)_"
    r"s(?P<start>\d{14})_e(?P<end>\d{14})_c(?P<created>\d{14})\.nc)$"
)
_JOB_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,191}$")
_ETAG_RE = re.compile(r'^"[0-9A-Fa-f]{32}"$')
_INVALID_PERCENT_RE = re.compile(r"%(?![0-9A-Fa-f]{2})")
_ALLOWED_LISTING_CONTENT_TYPES = frozenset({"application/xml", "text/xml"})
_ALLOWED_OBJECT_CONTENT_TYPES = frozenset({
    "application/netcdf",
    "application/octet-stream",
    "application/x-netcdf",
    "binary/octet-stream",
})

_RAW_STORE: RawArtifactStore | None = None
_RAW_STORE_LOCK = threading.Lock()
_CAPTURE_LOCK = threading.Lock()


class GoesRrqpeNativeError(RuntimeError):
    """Base error for the bounded exact-native candidate."""


class GoesRrqpeNativeContractError(GoesRrqpeNativeError):
    """An upstream response is outside the pinned native profile."""


class GoesRrqpeNativeDeadlineError(GoesRrqpeNativeError):
    """The absolute capture deadline expired."""


class GoesRrqpeNativeCorruptionError(GoesRrqpeNativeError):
    """Durable schema-3 evidence conflicts with its immutable identity."""


@dataclass(frozen=True, slots=True)
class S3Response:
    payload: bytes
    content_type: str
    received_at: float
    content_length: int | None
    etag: str | None
    last_modified: float | None
    version_id: str | None
    request_id: str | None
    request_id_2: str | None
    server_side_encryption: str | None


@dataclass(frozen=True, slots=True)
class GoesRrqpeObject:
    satellite: int
    key: str
    filename: str
    mode: int
    observation_start: float
    observation_end: float
    creation_time: float
    last_modified: float
    etag: str
    size_bytes: int
    storage_class: str
    checksum_algorithms: tuple[str, ...]
    checksum_type: str | None


@dataclass(frozen=True, slots=True)
class GoesRrqpeNetcdfIdentity:
    satellite: int
    mode: int
    filename: str
    observation_start: float
    observation_end: float
    creation_time: float
    platform_id: str
    orbital_slot: str
    timeline_id: str
    production_site: str
    rows: int
    columns: int
    rrqpe_dtype: str
    rrqpe_chunks: tuple[int, int]
    rrqpe_compression: str
    rrqpe_compression_level: int | None
    dqf_present: bool
    dqf_dtype: str
    dqf_chunks: tuple[int, int]
    dqf_compression: str
    dqf_compression_level: int | None
    dqf_attribute_names_sha256: str
    dqf_flag_metadata_sha256: str
    x_dtype: str
    y_dtype: str
    x_scale_factor: float
    x_add_offset: float
    y_scale_factor: float
    y_add_offset: float
    perspective_point_height: float
    semi_major_axis: float
    semi_minor_axis: float
    inverse_flattening: float
    latitude_of_projection_origin: float
    longitude_of_projection_origin: float
    sweep_angle_axis: str
    root_member_count: int
    root_member_names_sha256: str
    root_structure_sha256: str
    global_attribute_names_sha256: str
    pixel_values_read: bool = False


class _RejectRedirects(HTTPRedirectHandler):
    def redirect_request(self, *_args, **_kwargs):
        raise GoesRrqpeNativeContractError("GOES S3-redirect is niet toegestaan")


def _open_without_redirects(request: Request, *, timeout: float):
    return build_opener(ProxyHandler({}), _RejectRedirects()).open(
        request, timeout=timeout
    )


def _remaining(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if not math.isfinite(remaining) or remaining <= 0.0:
        raise GoesRrqpeNativeDeadlineError("GOES RRQPE-capturedeadline is verstreken")
    return remaining


def _satellite(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value not in SATELLITES:
        raise ValueError("GOES RRQPE-satelliet moet exact 18 of 19 zijn")
    return value


def _validate_job(job: Any) -> str:
    job_id = getattr(job, "job_id", None)
    if (
        not isinstance(job_id, str)
        or job_id != job_id.strip()
        or _JOB_ID_RE.fullmatch(job_id) is None
    ):
        raise ValueError("GOES RRQPE-capturejob mist een begrensde job_id")
    payload = getattr(job, "payload", None)
    if type(payload) is not dict or payload:
        raise ValueError("GOES RRQPE native capture vereist een exact lege jobpayload")
    return job_id


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
            raise GoesRrqpeNativeContractError(f"GOES HTTP-header {name} ontbreekt")
        return None
    result = str(value).strip()
    if (
        not result
        or len(result) > maximum
        or any(ord(character) < 0x20 or ord(character) == 0x7F for character in result)
    ):
        raise GoesRrqpeNativeContractError(f"GOES HTTP-header {name} is ongeldig")
    return result


def _content_type(headers: Any) -> str:
    value = _bounded_header(headers, "Content-Type", required=True, maximum=256)
    assert value is not None
    return value.split(";", 1)[0].strip().lower()


def _content_length(
    headers: Any,
    *,
    maximum_bytes: int,
    required: bool,
) -> int | None:
    value = _bounded_header(headers, "Content-Length", required=required, maximum=32)
    if value is None:
        return None
    if not value.isascii() or not value.isdecimal():
        raise GoesRrqpeNativeContractError("GOES Content-Length is ongeldig")
    result = int(value)
    if not 1 <= result <= maximum_bytes:
        raise GoesRrqpeNativeContractError("GOES Content-Length valt buiten de bytegrens")
    return result


def _http_date(value: str, *, context: str) -> float:
    try:
        moment = parsedate_to_datetime(value)
    except (TypeError, ValueError) as error:
        raise GoesRrqpeNativeContractError(f"{context} bevat geen geldige HTTP-datum") from error
    if moment.tzinfo is None or moment.utcoffset() is None:
        raise GoesRrqpeNativeContractError(f"{context} mist een tijdzone")
    result = moment.timestamp()
    if not math.isfinite(result):
        raise GoesRrqpeNativeContractError(f"{context} bevat geen eindige tijd")
    return result


def _rfc3339(value: str, *, context: str) -> float:
    if not isinstance(value, str) or not 1 <= len(value) <= 64:
        raise GoesRrqpeNativeContractError(f"{context} bevat geen begrensde tijd")
    candidate = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        moment = datetime.fromisoformat(candidate)
    except ValueError as error:
        raise GoesRrqpeNativeContractError(f"{context} bevat geen geldige tijd") from error
    if moment.tzinfo is None or moment.utcoffset() is None:
        raise GoesRrqpeNativeContractError(f"{context} mist een tijdzone")
    result = moment.timestamp()
    if not math.isfinite(result):
        raise GoesRrqpeNativeContractError(f"{context} bevat geen eindige tijd")
    return result


def _fixed_url(
    url: str,
    *,
    satellite: int,
    expected_path: str,
    expected_query: str,
) -> str:
    expected_host = f"noaa-goes{satellite}.s3.amazonaws.com"
    parts = urlsplit(url)
    if (
        parts.scheme != "https"
        or parts.hostname != expected_host
        or parts.port is not None
        or parts.username is not None
        or parts.password is not None
        or parts.path != expected_path
        or parts.query != expected_query
        or parts.fragment
    ):
        raise GoesRrqpeNativeContractError("GOES URL valt buiten het vaste S3-endpoint")
    return url


def _read_bounded_body(
    response: Any,
    *,
    maximum_bytes: int,
    declared_bytes: int | None,
    deadline: float,
) -> bytes:
    chunks: list[bytes] = []
    total = 0
    while total <= maximum_bytes:
        _remaining(deadline)
        requested = min(STREAM_CHUNK_BYTES, maximum_bytes + 1 - total)
        block = response.read(requested)
        _remaining(deadline)
        if not block:
            break
        if not isinstance(block, (bytes, bytearray, memoryview)):
            raise GoesRrqpeNativeContractError("GOES responsstream leverde geen bytes")
        immutable = bytes(block)
        if len(immutable) > requested:
            raise GoesRrqpeNativeContractError("GOES responschunk overschrijdt de grens")
        chunks.append(immutable)
        total += len(immutable)
        if total > maximum_bytes or (
            declared_bytes is not None and total > declared_bytes
        ):
            raise GoesRrqpeNativeContractError("GOES response overschrijdt de bytegrens")
    payload = b"".join(chunks)
    if not payload:
        raise GoesRrqpeNativeContractError("GOES response is leeg")
    if declared_bytes is not None and len(payload) != declared_bytes:
        raise GoesRrqpeNativeContractError("GOES responslengte wijkt af van HTTP")
    return payload


def _fetch_s3(
    url: str,
    *,
    satellite: int,
    expected_path: str,
    expected_query: str,
    maximum_bytes: int,
    allowed_content_types: frozenset[str],
    object_metadata_required: bool,
    deadline: float,
    if_match: str | None = None,
    opener: Callable[..., Any] | None = None,
) -> S3Response:
    _fixed_url(
        url,
        satellite=satellite,
        expected_path=expected_path,
        expected_query=expected_query,
    )
    headers = {
        "Accept-Encoding": "identity",
        "User-Agent": "StormTrackerWorker/goes-rrqpe-native-candidate-v1",
    }
    if if_match is not None:
        if _ETAG_RE.fullmatch(if_match) is None:
            raise GoesRrqpeNativeContractError("GOES If-Match ETag is ongeldig")
        headers["If-Match"] = if_match
    request = Request(url, headers=headers, method="GET")
    active_opener = _open_without_redirects if opener is None else opener
    with active_opener(
        request,
        timeout=min(HTTP_IO_TIMEOUT_SECONDS, _remaining(deadline)),
    ) as response:
        if int(getattr(response, "status", 0)) != 200:
            raise GoesRrqpeNativeContractError("GOES S3 gaf geen HTTP 200")
        geturl = getattr(response, "geturl", None)
        if not callable(geturl):
            raise GoesRrqpeNativeContractError("GOES response mist endpointidentiteit")
        _fixed_url(
            str(geturl()),
            satellite=satellite,
            expected_path=expected_path,
            expected_query=expected_query,
        )
        response_headers = getattr(response, "headers", None)
        encoding = (
            _bounded_header(response_headers, "Content-Encoding", maximum=64)
            or "identity"
        ).lower()
        if encoding != "identity":
            raise GoesRrqpeNativeContractError("GOES transportcompressie is verboden")
        response_content_type = _content_type(response_headers)
        if response_content_type not in allowed_content_types:
            raise GoesRrqpeNativeContractError("GOES Content-Type valt buiten contract")
        declared = _content_length(
            response_headers,
            maximum_bytes=maximum_bytes,
            required=object_metadata_required,
        )
        etag = _bounded_header(
            response_headers, "ETag", required=object_metadata_required, maximum=128
        )
        if etag is not None and _ETAG_RE.fullmatch(etag) is None:
            raise GoesRrqpeNativeContractError("GOES HTTP-ETag is ongeldig")
        modified_text = _bounded_header(
            response_headers,
            "Last-Modified",
            required=object_metadata_required,
            maximum=128,
        )
        modified = (
            None
            if modified_text is None
            else _http_date(modified_text, context="GOES Last-Modified")
        )
        payload = _read_bounded_body(
            response,
            maximum_bytes=maximum_bytes,
            declared_bytes=declared,
            deadline=deadline,
        )
        received_at = time.time()
        if not math.isfinite(received_at) or received_at < 0:
            raise GoesRrqpeNativeContractError("GOES ontvangsttijd is ongeldig")
        return S3Response(
            payload=payload,
            content_type=response_content_type,
            received_at=received_at,
            content_length=declared,
            etag=etag,
            last_modified=modified,
            version_id=_bounded_header(response_headers, "x-amz-version-id", maximum=1024),
            request_id=_bounded_header(response_headers, "x-amz-request-id", maximum=256),
            request_id_2=_bounded_header(response_headers, "x-amz-id-2", maximum=2048),
            server_side_encryption=_bounded_header(
                response_headers, "x-amz-server-side-encryption", maximum=128
            ),
        )


def _listing_prefixes(now: datetime | None = None) -> tuple[str, str]:
    moment = datetime.now(timezone.utc) if now is None else now
    if not isinstance(moment, datetime) or moment.tzinfo is None or moment.utcoffset() is None:
        raise ValueError("GOES referentietijd moet tijdzonebewust zijn")
    current = moment.astimezone(timezone.utc)
    previous = current - timedelta(hours=1)
    return tuple(
        f"{PRODUCT_PREFIX}/{value.year}/{value.timetuple().tm_yday:03d}/{value.hour:02d}/"
        for value in (previous, current)
    )


def _adjacent_prefix(previous_prefix: str) -> str:
    if _PREFIX_RE.fullmatch(previous_prefix) is None:
        raise GoesRrqpeNativeCorruptionError("duurzame GOES listingprefix is ongeldig")
    parts = previous_prefix.rstrip("/").split("/")
    try:
        if len(parts) != 4 or parts[0] != PRODUCT_PREFIX:
            raise ValueError
        moment = datetime.strptime("/".join(parts[1:]), "%Y/%j/%H").replace(
            tzinfo=timezone.utc
        )
    except ValueError as error:
        raise GoesRrqpeNativeCorruptionError("duurzame GOES listingprefix heeft ongeldige tijd") from error
    next_hour = moment + timedelta(hours=1)
    return (
        f"{PRODUCT_PREFIX}/{next_hour.year}/"
        f"{next_hour.timetuple().tm_yday:03d}/{next_hour.hour:02d}/"
    )


def _listing_request(satellite: int, prefix: str) -> tuple[str, str, str]:
    satellite = _satellite(satellite)
    if _PREFIX_RE.fullmatch(prefix) is None:
        raise GoesRrqpeNativeContractError("GOES listingprefix is ongeldig")
    query = urlencode((
        ("list-type", "2"),
        ("prefix", prefix),
        ("max-keys", str(LIST_MAX_KEYS)),
        ("encoding-type", "url"),
    ))
    url = f"https://noaa-goes{satellite}.s3.amazonaws.com/?{query}"
    return (
        _fixed_url(url, satellite=satellite, expected_path="/", expected_query=query),
        "/",
        query,
    )


def _fetch_listing(satellite: int, prefix: str, *, deadline: float) -> S3Response:
    url, path, query = _listing_request(satellite, prefix)
    return _fetch_s3(
        url,
        satellite=satellite,
        expected_path=path,
        expected_query=query,
        maximum_bytes=MAX_LISTING_BYTES,
        allowed_content_types=_ALLOWED_LISTING_CONTENT_TYPES,
        object_metadata_required=False,
        deadline=deadline,
    )


def _object_url(satellite: int, key: str) -> tuple[str, str]:
    satellite = _satellite(satellite)
    match = _KEY_RE.fullmatch(key)
    if match is None or int(match.group("satellite")) != satellite:
        raise GoesRrqpeNativeContractError("GOES objectkey is ongeldig")
    path = "/" + quote(key, safe="/")
    url = f"https://noaa-goes{satellite}.s3.amazonaws.com{path}"
    return (
        _fixed_url(url, satellite=satellite, expected_path=path, expected_query=""),
        path,
    )


def _fetch_object(selected: GoesRrqpeObject, *, deadline: float) -> S3Response:
    url, path = _object_url(selected.satellite, selected.key)
    response = _fetch_s3(
        url,
        satellite=selected.satellite,
        expected_path=path,
        expected_query="",
        maximum_bytes=MAX_NETCDF4_BYTES,
        allowed_content_types=_ALLOWED_OBJECT_CONTENT_TYPES,
        object_metadata_required=True,
        deadline=deadline,
        if_match=selected.etag,
    )
    if (
        response.etag != selected.etag
        or len(response.payload) != selected.size_bytes
        or response.last_modified != selected.last_modified
    ):
        raise GoesRrqpeNativeContractError(
            "GOES objectrevision wijkt af van het duurzame listingbewijs"
        )
    return response


def _children(element: ET.Element) -> dict[str, list[ET.Element]]:
    grouped: dict[str, list[ET.Element]] = {}
    namespace_prefix = "{" + S3_NAMESPACE + "}"
    for child in element:
        if not child.tag.startswith(namespace_prefix):
            raise GoesRrqpeNativeContractError("GOES listing bevat vreemde namespace")
        grouped.setdefault(child.tag.rsplit("}", 1)[-1], []).append(child)
    return grouped


def _xml_scalar(
    grouped: dict[str, list[ET.Element]],
    name: str,
    *,
    maximum: int = 4_096,
) -> str:
    values = grouped.get(name, [])
    if len(values) != 1 or values[0].text is None:
        raise GoesRrqpeNativeContractError(
            f"GOES listingveld {name} is niet exact één"
        )
    value = values[0].text
    if not 1 <= len(value) <= maximum:
        raise GoesRrqpeNativeContractError(f"GOES listingveld {name} is onbegrensd")
    return value


def _strict_unquote(value: str, *, context: str, maximum: int) -> str:
    if (
        not isinstance(value, str)
        or not 1 <= len(value) <= maximum * 3
        or _INVALID_PERCENT_RE.search(value) is not None
    ):
        raise GoesRrqpeNativeContractError(f"{context} is geen begrensde URL-tekst")
    try:
        decoded = unquote_to_bytes(value).decode("utf-8", errors="strict")
    except UnicodeDecodeError as error:
        raise GoesRrqpeNativeContractError(f"{context} is geen UTF-8") from error
    if (
        not 1 <= len(decoded) <= maximum
        or any(ord(character) < 0x20 or ord(character) == 0x7F for character in decoded)
    ):
        raise GoesRrqpeNativeContractError(f"{context} bevat ongeldige tekst")
    return decoded


def _goes_filename_time(value: str, *, context: str) -> float:
    if not isinstance(value, str) or re.fullmatch(r"\d{14}", value) is None:
        raise GoesRrqpeNativeContractError(f"GOES {context} is geen filenaamstijd")
    try:
        moment = datetime.strptime(value[:13], "%Y%j%H%M%S").replace(
            tzinfo=timezone.utc,
            microsecond=int(value[13]) * 100_000,
        )
    except ValueError as error:
        raise GoesRrqpeNativeContractError(f"GOES {context} is ongeldig") from error
    if moment.strftime("%Y%j%H%M%S") + str(moment.microsecond // 100_000) != value:
        raise GoesRrqpeNativeContractError(f"GOES {context} normaliseert anders")
    return moment.timestamp()


def _parse_product_key(
    key: str,
    *,
    satellite: int,
    expected_prefix: str,
) -> tuple[str, int, float, float, float]:
    match = _KEY_RE.fullmatch(key)
    if (
        match is None
        or not key.startswith(expected_prefix)
        or int(match.group("satellite")) != satellite
    ):
        raise GoesRrqpeNativeContractError("GOES listing bevat een vreemde objectkey")
    start = _goes_filename_time(match.group("start"), context="observatiestart")
    end = _goes_filename_time(match.group("end"), context="observatie-einde")
    created = _goes_filename_time(match.group("created"), context="creatietijd")
    start_moment = datetime.fromtimestamp(start, timezone.utc)
    if (
        match.group("year") != f"{start_moment.year:04d}"
        or match.group("day") != f"{start_moment.timetuple().tm_yday:03d}"
        or match.group("hour") != f"{start_moment.hour:02d}"
        or not start < end <= created
        or not 4 * 60 <= end - start <= 16 * 60
        or not 0 <= created - end <= 10 * 60
    ):
        raise GoesRrqpeNativeContractError("GOES keypad en filenaamtijden verschillen")
    return match.group("filename"), int(match.group("mode")), start, end, created


def _parse_listing(
    payload: bytes,
    *,
    satellite: int,
    expected_prefix: str,
) -> tuple[GoesRrqpeObject, ...]:
    satellite = _satellite(satellite)
    if not isinstance(payload, bytes) or not 1 <= len(payload) <= MAX_LISTING_BYTES:
        raise GoesRrqpeNativeContractError("GOES listing valt buiten de bytegrens")
    upper_prefix = payload[: min(len(payload), 65_536)].upper()
    if b"<!DOCTYPE" in upper_prefix or b"<!ENTITY" in upper_prefix:
        raise GoesRrqpeNativeContractError("GOES listing bevat verboden XML-declaraties")
    try:
        root = ET.fromstring(payload)
    except ET.ParseError as error:
        raise GoesRrqpeNativeContractError("GOES listing-XML is ongeldig") from error
    if root.tag != f"{{{S3_NAMESPACE}}}ListBucketResult":
        raise GoesRrqpeNativeContractError("GOES listing heeft verkeerde root")
    grouped = _children(root)
    allowed = {
        "Name",
        "Prefix",
        "KeyCount",
        "MaxKeys",
        "IsTruncated",
        "EncodingType",
        "Contents",
    }
    if set(grouped) - allowed:
        raise GoesRrqpeNativeContractError("GOES listing bevat onbekende velden")
    if _xml_scalar(grouped, "Name") != f"noaa-goes{satellite}":
        raise GoesRrqpeNativeContractError("GOES listing noemt verkeerde bucket")
    prefix = _strict_unquote(
        _xml_scalar(grouped, "Prefix"), context="GOES Prefix", maximum=256
    )
    if prefix != expected_prefix:
        raise GoesRrqpeNativeContractError("GOES listingprefix wijkt af")
    if _xml_scalar(grouped, "EncodingType") != "url":
        raise GoesRrqpeNativeContractError("GOES listing mist URL-encoding")
    if _xml_scalar(grouped, "MaxKeys") != str(LIST_MAX_KEYS):
        raise GoesRrqpeNativeContractError("GOES listing MaxKeys wijkt af")
    if _xml_scalar(grouped, "IsTruncated") != "false":
        raise GoesRrqpeNativeContractError("GOES listing is onvolledig/truncated")
    key_count_text = _xml_scalar(grouped, "KeyCount")
    if not key_count_text.isascii() or not key_count_text.isdecimal():
        raise GoesRrqpeNativeContractError("GOES KeyCount is ongeldig")
    contents = grouped.get("Contents", [])
    if int(key_count_text) != len(contents) or len(contents) > LIST_MAX_KEYS:
        raise GoesRrqpeNativeContractError("GOES KeyCount en Contents verschillen")

    result: list[GoesRrqpeObject] = []
    seen: set[str] = set()
    for entry in contents:
        fields = _children(entry)
        base_fields = {"Key", "LastModified", "ETag", "Size", "StorageClass"}
        optional_fields = {"ChecksumAlgorithm", "ChecksumType"}
        if set(fields) - (base_fields | optional_fields) or not base_fields <= set(fields):
            raise GoesRrqpeNativeContractError("GOES Contents-profiel wijkt af")
        if any(len(fields[name]) != 1 for name in base_fields):
            raise GoesRrqpeNativeContractError("GOES Contents bevat dubbel basisveld")
        key = _strict_unquote(
            _xml_scalar(fields, "Key", maximum=2_048),
            context="GOES Key",
            maximum=1_024,
        )
        if key in seen:
            raise GoesRrqpeNativeContractError("GOES listing bevat dubbele objectkey")
        seen.add(key)
        filename, mode, start, end, created = _parse_product_key(
            key, satellite=satellite, expected_prefix=expected_prefix
        )
        etag = _xml_scalar(fields, "ETag", maximum=128)
        if _ETAG_RE.fullmatch(etag) is None:
            raise GoesRrqpeNativeContractError("GOES listing-ETag is ongeldig")
        size_text = _xml_scalar(fields, "Size", maximum=32)
        if not size_text.isascii() or not size_text.isdecimal():
            raise GoesRrqpeNativeContractError("GOES listingsize is ongeldig")
        size_bytes = int(size_text)
        if not 1 <= size_bytes <= MAX_NETCDF4_BYTES:
            raise GoesRrqpeNativeContractError("GOES listingsize valt buiten grens")
        storage_class = _xml_scalar(fields, "StorageClass", maximum=64)
        if storage_class != "STANDARD":
            raise GoesRrqpeNativeContractError("GOES storage class wijkt af van recent profiel")
        algorithms = tuple(
            _xml_scalar({"value": [element]}, "value", maximum=32)
            for element in fields.get("ChecksumAlgorithm", [])
        )
        if len(algorithms) > 4 or len(set(algorithms)) != len(algorithms):
            raise GoesRrqpeNativeContractError("GOES checksumalgoritmen zijn ongeldig")
        checksum_type_values = fields.get("ChecksumType", [])
        if len(checksum_type_values) > 1:
            raise GoesRrqpeNativeContractError("GOES ChecksumType is dubbel")
        checksum_type = (
            None
            if not checksum_type_values
            else _xml_scalar({"value": checksum_type_values}, "value", maximum=32)
        )
        last_modified = _rfc3339(
            _xml_scalar(fields, "LastModified"), context="GOES LastModified"
        )
        if last_modified + 1.0 < created:
            raise GoesRrqpeNativeContractError("GOES LastModified ligt vóór creatietijd")
        result.append(GoesRrqpeObject(
            satellite=satellite,
            key=key,
            filename=filename,
            mode=mode,
            observation_start=start,
            observation_end=end,
            creation_time=created,
            last_modified=last_modified,
            etag=etag,
            size_bytes=size_bytes,
            storage_class=storage_class,
            checksum_algorithms=algorithms,
            checksum_type=checksum_type,
        ))
    return tuple(sorted(
        result,
        key=lambda item: (item.observation_end, item.creation_time, item.key),
    ))


def _select_latest(
    groups: Iterable[tuple[GoesRrqpeObject, ...]],
) -> GoesRrqpeObject:
    objects = [item for group in groups for item in group]
    if not objects:
        raise GoesRrqpeNativeContractError("GOES listings bevatten geen recent RRQPEF-object")
    by_key: dict[str, GoesRrqpeObject] = {}
    for item in objects:
        previous = by_key.setdefault(item.key, item)
        if previous != item:
            raise GoesRrqpeNativeContractError("GOES objectkey heeft conflicterende revisie")
    return max(
        by_key.values(),
        key=lambda item: (item.observation_end, item.creation_time, item.key),
    )


def _attribute_text(attributes: Any, name: str) -> str:
    if name not in attributes:
        raise GoesRrqpeNativeContractError(f"GOES NetCDF-attribuut {name} ontbreekt")
    value = attributes[name]
    if isinstance(value, np.ndarray):
        if value.size != 1:
            raise GoesRrqpeNativeContractError(f"GOES NetCDF-attribuut {name} is niet scalar")
        value = value.reshape(-1)[0]
    if isinstance(value, (bytes, np.bytes_)):
        try:
            value = bytes(value).decode("utf-8", errors="strict")
        except UnicodeDecodeError as error:
            raise GoesRrqpeNativeContractError(
                f"GOES NetCDF-attribuut {name} is geen UTF-8"
            ) from error
    if not isinstance(value, (str, np.str_)):
        raise GoesRrqpeNativeContractError(f"GOES NetCDF-attribuut {name} is geen tekst")
    result = str(value)
    if (
        not 1 <= len(result) <= 8_192
        or any(ord(character) < 0x20 and character not in "\t\n\r" for character in result)
    ):
        raise GoesRrqpeNativeContractError(f"GOES NetCDF-attribuut {name} is ongeldig")
    return result


def _attribute_float(attributes: Any, name: str) -> float:
    if name not in attributes:
        raise GoesRrqpeNativeContractError(f"GOES NetCDF-attribuut {name} ontbreekt")
    value = attributes[name]
    array = np.asarray(value)
    if array.size != 1:
        raise GoesRrqpeNativeContractError(f"GOES NetCDF-attribuut {name} is niet scalar")
    scalar = array.reshape(-1)[0]
    if isinstance(scalar, (bool, np.bool_)) or not isinstance(scalar, np.number):
        raise GoesRrqpeNativeContractError(f"GOES NetCDF-attribuut {name} is geen getal")
    result = float(scalar)
    if not math.isfinite(result):
        raise GoesRrqpeNativeContractError(f"GOES NetCDF-attribuut {name} is niet eindig")
    return result


def _attribute_integer_vector(
    attributes: Any,
    name: str,
    *,
    length: int,
) -> tuple[int, ...]:
    if name not in attributes:
        raise GoesRrqpeNativeContractError(f"GOES NetCDF-attribuut {name} ontbreekt")
    array = np.asarray(attributes[name]).reshape(-1)
    if array.size != length or array.dtype.kind not in "iu":
        raise GoesRrqpeNativeContractError(f"GOES NetCDF-attribuut {name} is ongeldig")
    return tuple(int(value) for value in array)


def _hard_dataset(root: h5py.File, name: str) -> h5py.Dataset:
    link = root.get(name, getlink=True)
    if not isinstance(link, h5py.HardLink):
        raise GoesRrqpeNativeContractError(
            f"GOES NetCDF-object {name} ontbreekt of bevat externe/symbolische link"
        )
    value = root.get(name)
    if not isinstance(value, h5py.Dataset):
        raise GoesRrqpeNativeContractError(f"GOES NetCDF-object {name} is geen dataset")
    return value


def _dataset_storage_profile(dataset: h5py.Dataset, *, context: str) -> None:
    creation = dataset.id.get_create_plist()
    if creation.get_external_count() != 0 or creation.get_layout() == h5py.h5d.VIRTUAL:
        raise GoesRrqpeNativeContractError(f"GOES {context} gebruikt externe/virtuele opslag")


def _name_digest(values: Iterable[str]) -> str:
    return hashlib.sha256("\n".join(sorted(values)).encode("utf-8")).hexdigest()


def _json_digest(value: object) -> str:
    payload = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("ascii")
    return hashlib.sha256(payload).hexdigest()


def _attribute_metadata_value(value: object) -> object:
    array = np.asarray(value)
    if array.size > 64:
        raise GoesRrqpeNativeContractError("GOES NetCDF-attribuut is onbegrensd")
    result = array.tolist()

    def normalize(item: object) -> object:
        if isinstance(item, list):
            return [normalize(child) for child in item]
        if isinstance(item, (bytes, np.bytes_)):
            return bytes(item).decode("utf-8", errors="strict")
        if isinstance(item, (np.integer, np.floating)):
            item = item.item()
        if isinstance(item, float) and not math.isfinite(item):
            raise GoesRrqpeNativeContractError("GOES NetCDF-attribuut is niet eindig")
        if isinstance(item, (str, int, float, bool)) or item is None:
            return item
        raise GoesRrqpeNativeContractError("GOES NetCDF-attribuut heeft onbekend type")

    return normalize(result)


def _root_structure(root: h5py.File) -> tuple[int, str, str]:
    names = tuple(root.keys())
    if not 1 <= len(names) <= MAX_ROOT_MEMBERS:
        raise GoesRrqpeNativeContractError("GOES NetCDF-rootgrootte valt buiten grens")
    profile: list[dict[str, object]] = []
    for name in names:
        if not isinstance(name, str) or not 1 <= len(name) <= 256:
            raise GoesRrqpeNativeContractError("GOES NetCDF-rootnaam is ongeldig")
        value = _hard_dataset(root, name)
        if (
            value.ndim > MAX_DATASET_RANK
            or any(not 0 <= int(size) <= MAX_DIMENSION for size in value.shape)
            or len(value.attrs) > MAX_DATASET_ATTRIBUTES
        ):
            raise GoesRrqpeNativeContractError("GOES NetCDF-dataset valt buiten structuurgrens")
        _dataset_storage_profile(value, context=name)
        attribute_names = tuple(str(attr) for attr in value.attrs.keys())
        if any(not 1 <= len(attr) <= 256 for attr in attribute_names):
            raise GoesRrqpeNativeContractError("GOES NetCDF-attribuutnaam is ongeldig")
        profile.append({
            "name": name,
            "shape": tuple(int(size) for size in value.shape),
            "dtype": str(value.dtype),
            "chunks": None if value.chunks is None else tuple(int(size) for size in value.chunks),
            "compression": value.compression,
            "compression_opts": value.compression_opts,
            "attribute_names_sha256": _name_digest(attribute_names),
        })
    return len(names), _name_digest(names), _json_digest(profile)


def _inspect_netcdf4(
    payload: bytes,
    *,
    selected: GoesRrqpeObject,
) -> GoesRrqpeNetcdfIdentity:
    if not isinstance(payload, bytes) or len(payload) != selected.size_bytes:
        raise GoesRrqpeNativeContractError("GOES NetCDF-bytes en listing verschillen")
    if payload[:8] != b"\x89HDF\r\n\x1a\n":
        raise GoesRrqpeNativeContractError("GOES object mist HDF5-signature")
    try:
        product = h5py.File(io.BytesIO(payload), "r")
    except (OSError, ValueError) as error:
        raise GoesRrqpeNativeContractError("GOES object is geen leesbare NetCDF-4/HDF5") from error
    with product:
        root_count, root_names_digest, root_structure_digest = _root_structure(product)
        required_global_attributes = {
            "dataset_name",
            "platform_ID",
            "scene_id",
            "timeline_id",
            "time_coverage_start",
            "time_coverage_end",
            "date_created",
            "spatial_resolution",
            "orbital_slot",
            "production_site",
        }
        if not required_global_attributes <= set(product.attrs.keys()):
            raise GoesRrqpeNativeContractError("GOES NetCDF mist verplichte globale attributen")
        if len(product.attrs) > 128:
            raise GoesRrqpeNativeContractError("GOES NetCDF heeft te veel globale attributen")
        dataset_name = _attribute_text(product.attrs, "dataset_name")
        platform_id = _attribute_text(product.attrs, "platform_ID")
        orbital_slot = _attribute_text(product.attrs, "orbital_slot")
        timeline_id = _attribute_text(product.attrs, "timeline_id")
        production_site = _attribute_text(product.attrs, "production_site")
        if (
            dataset_name != selected.filename
            or platform_id != f"G{selected.satellite}"
            or orbital_slot != ORBITAL_SLOT[selected.satellite]
            or timeline_id != f"ABI Mode {selected.mode}"
            or _attribute_text(product.attrs, "scene_id") != "Full Disk"
            or _attribute_text(product.attrs, "spatial_resolution") != FIXED_GRID_RESOLUTION
        ):
            raise GoesRrqpeNativeContractError("GOES filename/platform/scene-profiel wijkt af")
        product_start = _rfc3339(
            _attribute_text(product.attrs, "time_coverage_start"),
            context="GOES time_coverage_start",
        )
        product_end = _rfc3339(
            _attribute_text(product.attrs, "time_coverage_end"),
            context="GOES time_coverage_end",
        )
        product_created = _rfc3339(
            _attribute_text(product.attrs, "date_created"),
            context="GOES date_created",
        )
        if any(
            not math.isclose(actual, expected, rel_tol=0.0, abs_tol=0.001)
            for actual, expected in (
                (product_start, selected.observation_start),
                (product_end, selected.observation_end),
                (product_created, selected.creation_time),
            )
        ):
            raise GoesRrqpeNativeContractError("GOES filename- en NetCDF-tijden verschillen")

        rrqpe = _hard_dataset(product, "RRQPE")
        dqf = _hard_dataset(product, "DQF")
        x_axis = _hard_dataset(product, "x")
        y_axis = _hard_dataset(product, "y")
        projection = _hard_dataset(product, "goes_imager_projection")
        for context, dataset in (
            ("RRQPE", rrqpe),
            ("DQF", dqf),
            ("x", x_axis),
            ("y", y_axis),
            ("goes_imager_projection", projection),
        ):
            _dataset_storage_profile(dataset, context=context)

        if (
            rrqpe.shape != (GRID_ROWS, GRID_COLUMNS)
            or rrqpe.dtype.kind != "u"
            or rrqpe.dtype.itemsize != 2
            or rrqpe.chunks is None
            or tuple(int(value) for value in rrqpe.chunks) != (24, GRID_COLUMNS)
            or rrqpe.compression != "gzip"
            or _attribute_text(rrqpe.attrs, "standard_name") != "rainfall_rate"
            or _attribute_text(rrqpe.attrs, "units") != "mm h-1"
            or _attribute_text(rrqpe.attrs, "grid_mapping") != "goes_imager_projection"
            or _attribute_text(rrqpe.attrs, "ancillary_variables") != "DQF"
            or _attribute_integer_vector(rrqpe.attrs, "valid_range", length=2) != (0, 65_530)
            or int(_attribute_float(rrqpe.attrs, "_FillValue")) != 65_535
            or not math.isclose(
                _attribute_float(rrqpe.attrs, "scale_factor"),
                0.00152602,
                rel_tol=0.0,
                abs_tol=1e-10,
            )
            or _attribute_float(rrqpe.attrs, "add_offset") != 0.0
        ):
            raise GoesRrqpeNativeContractError("GOES RRQPE-structuurprofiel wijkt af")

        if (
            dqf.shape != rrqpe.shape
            or dqf.dtype.kind != "u"
            or dqf.dtype.itemsize != 1
            or dqf.chunks is None
            or tuple(int(value) for value in dqf.chunks) != (48, GRID_COLUMNS)
            or dqf.compression != "gzip"
            or _attribute_text(dqf.attrs, "standard_name") != "status_flag"
            or _attribute_text(dqf.attrs, "units") != "1"
            or _attribute_text(dqf.attrs, "grid_mapping") != "goes_imager_projection"
            or _attribute_integer_vector(dqf.attrs, "valid_range", length=2) != (0, 127)
            or int(_attribute_float(dqf.attrs, "_FillValue")) != 255
            or int(_attribute_float(dqf.attrs, "number_of_qf_values")) != 8
        ):
            raise GoesRrqpeNativeContractError("GOES DQF-structuurprofiel wijkt af")
        dqf_required_metadata = {"flag_masks", "flag_values", "flag_meanings"}
        if not dqf_required_metadata <= set(dqf.attrs.keys()):
            raise GoesRrqpeNativeContractError("GOES DQF-vlagmetadata ontbreekt")
        dqf_flag_metadata = {
            name: _attribute_metadata_value(dqf.attrs[name])
            for name in sorted(dqf_required_metadata)
        }

        expected_axes = (
            (x_axis, "X", "projection_x_coordinate", 1.0, -1.0),
            (y_axis, "Y", "projection_y_coordinate", -1.0, 1.0),
        )
        axis_values: list[tuple[float, float]] = []
        for axis, axis_name, standard_name, scale_sign, offset_sign in expected_axes:
            scale = _attribute_float(axis.attrs, "scale_factor")
            offset = _attribute_float(axis.attrs, "add_offset")
            if (
                axis.shape != (GRID_COLUMNS,)
                or axis.dtype.kind != "i"
                or axis.dtype.itemsize != 2
                or axis.chunks is not None
                or axis.compression is not None
                or _attribute_text(axis.attrs, "axis") != axis_name
                or _attribute_text(axis.attrs, "standard_name") != standard_name
                or _attribute_text(axis.attrs, "units") != "rad"
                or math.copysign(1.0, scale) != scale_sign
                or math.copysign(1.0, offset) != offset_sign
                or not math.isclose(abs(scale), 5.6e-5, rel_tol=0.0, abs_tol=1e-10)
                or not math.isclose(abs(offset), 0.151844, rel_tol=0.0, abs_tol=1e-7)
            ):
                raise GoesRrqpeNativeContractError("GOES x/y fixed-gridprofiel wijkt af")
            axis_values.append((scale, offset))
        if (
            not math.isclose(axis_values[0][0], -axis_values[1][0], rel_tol=0.0, abs_tol=1e-12)
            or not math.isclose(axis_values[0][1], -axis_values[1][1], rel_tol=0.0, abs_tol=1e-12)
        ):
            raise GoesRrqpeNativeContractError("GOES x/y assen zijn niet symmetrisch")

        projection_values = {
            "perspective_point_height": _attribute_float(
                projection.attrs, "perspective_point_height"
            ),
            "semi_major_axis": _attribute_float(projection.attrs, "semi_major_axis"),
            "semi_minor_axis": _attribute_float(projection.attrs, "semi_minor_axis"),
            "inverse_flattening": _attribute_float(
                projection.attrs, "inverse_flattening"
            ),
            "latitude_of_projection_origin": _attribute_float(
                projection.attrs, "latitude_of_projection_origin"
            ),
            "longitude_of_projection_origin": _attribute_float(
                projection.attrs, "longitude_of_projection_origin"
            ),
        }
        expected_projection = {
            "perspective_point_height": PROJECTION_HEIGHT_METRES,
            "semi_major_axis": SEMI_MAJOR_AXIS_METRES,
            "semi_minor_axis": SEMI_MINOR_AXIS_METRES,
            "inverse_flattening": INVERSE_FLATTENING,
            "latitude_of_projection_origin": 0.0,
            "longitude_of_projection_origin": PROJECTION_LONGITUDE[selected.satellite],
        }
        if (
            projection.shape != ()
            or projection.dtype.kind != "i"
            or projection.dtype.itemsize != 4
            or _attribute_text(projection.attrs, "grid_mapping_name") != "geostationary"
            or _attribute_text(projection.attrs, "sweep_angle_axis") != "x"
            or any(
                not math.isclose(
                    projection_values[name], expected,
                    rel_tol=0.0,
                    abs_tol=1e-8,
                )
                for name, expected in expected_projection.items()
            )
        ):
            raise GoesRrqpeNativeContractError("GOES geostationaire projectie wijkt af")

        return GoesRrqpeNetcdfIdentity(
            satellite=selected.satellite,
            mode=selected.mode,
            filename=selected.filename,
            observation_start=product_start,
            observation_end=product_end,
            creation_time=product_created,
            platform_id=platform_id,
            orbital_slot=orbital_slot,
            timeline_id=timeline_id,
            production_site=production_site,
            rows=GRID_ROWS,
            columns=GRID_COLUMNS,
            rrqpe_dtype=str(rrqpe.dtype),
            rrqpe_chunks=tuple(int(value) for value in rrqpe.chunks),
            rrqpe_compression=str(rrqpe.compression),
            rrqpe_compression_level=(
                None if rrqpe.compression_opts is None else int(rrqpe.compression_opts)
            ),
            dqf_present=True,
            dqf_dtype=str(dqf.dtype),
            dqf_chunks=tuple(int(value) for value in dqf.chunks),
            dqf_compression=str(dqf.compression),
            dqf_compression_level=(
                None if dqf.compression_opts is None else int(dqf.compression_opts)
            ),
            dqf_attribute_names_sha256=_name_digest(str(name) for name in dqf.attrs.keys()),
            dqf_flag_metadata_sha256=_json_digest(dqf_flag_metadata),
            x_dtype=str(x_axis.dtype),
            y_dtype=str(y_axis.dtype),
            x_scale_factor=axis_values[0][0],
            x_add_offset=axis_values[0][1],
            y_scale_factor=axis_values[1][0],
            y_add_offset=axis_values[1][1],
            perspective_point_height=projection_values["perspective_point_height"],
            semi_major_axis=projection_values["semi_major_axis"],
            semi_minor_axis=projection_values["semi_minor_axis"],
            inverse_flattening=projection_values["inverse_flattening"],
            latitude_of_projection_origin=projection_values[
                "latitude_of_projection_origin"
            ],
            longitude_of_projection_origin=projection_values[
                "longitude_of_projection_origin"
            ],
            sweep_angle_axis="x",
            root_member_count=root_count,
            root_member_names_sha256=root_names_digest,
            root_structure_sha256=root_structure_digest,
            global_attribute_names_sha256=_name_digest(
                str(name) for name in product.attrs.keys()
            ),
        )


def _raw_store(injected: RawArtifactStore | None) -> RawArtifactStore:
    if injected is not None:
        injected.initialize()
        return injected
    share = RAW_SHARE_ROOT.resolve(strict=False)
    root = RAW_ARTIFACT_ROOT.resolve(strict=False)
    if root == share or share not in root.parents:
        raise GoesRrqpeNativeCorruptionError("GOES raw store ontsnapt uit worker-share")
    global _RAW_STORE
    with _RAW_STORE_LOCK:
        if _RAW_STORE is None:
            _RAW_STORE = RawArtifactStore(root)
            _RAW_STORE.initialize()
        return _RAW_STORE


def _safe_auxiliary_directory(store: RawArtifactStore) -> Path:
    root = Path(store.root)
    if root.is_symlink() or not root.is_dir():
        raise GoesRrqpeNativeCorruptionError("GOES raw root is onveilig")
    directory = root / "goes_rrqpe_native_locks"
    directory.mkdir(mode=0o700, exist_ok=True)
    if directory.is_symlink() or directory.resolve().parent != root.resolve():
        raise GoesRrqpeNativeCorruptionError("GOES lockmap ontsnapt uit raw root")
    return directory


@contextmanager
def _deadline_thread_lock(*, deadline: float) -> Iterator[None]:
    acquired = _CAPTURE_LOCK.acquire(timeout=_remaining(deadline))
    if not acquired:
        raise GoesRrqpeNativeDeadlineError("GOES capturethreadlock overschreed de deadline")
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
        raise GoesRrqpeNativeCorruptionError("GOES lockpad is onveilig")
    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_BINARY", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags, 0o600)
    acquired = False
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode):
            raise GoesRrqpeNativeCorruptionError("GOES lock is geen regulier bestand")
        if info.st_size == 0:
            os.write(descriptor, b"\x00")
            os.fsync(descriptor)
        while not acquired:
            try:
                os.lseek(descriptor, 0, os.SEEK_SET)
                if os.name == "nt":  # pragma: no cover - selected by platform
                    import msvcrt

                    msvcrt.locking(descriptor, msvcrt.LK_NBLCK, 1)
                else:  # pragma: no branch - selected by platform
                    import fcntl

                    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                acquired = True
            except OSError as error:
                if error.errno not in {
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
            if os.name == "nt":  # pragma: no cover - selected by platform
                import msvcrt

                msvcrt.locking(descriptor, msvcrt.LK_UNLCK, 1)
            else:  # pragma: no branch - selected by platform
                import fcntl

                fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def _role_fetch_id(job_id: str, satellite: int, role: str) -> str:
    return f"{job_id}:G{satellite}:{role}"


def _receipt_id(
    store: RawArtifactStore,
    job_id: str,
    satellite: int,
    role: str,
) -> str:
    product = LISTING_PRODUCT if role.startswith("listing_") else NETCDF4_PRODUCT
    return store._receipt_id(  # noqa: SLF001 - deterministic schema-3 identity
        provider=PROVIDER,
        product=product,
        fetch_id=_role_fetch_id(job_id, satellite, role),
    )


def _capture_id(store: RawArtifactStore, job_id: str, satellite: int) -> str:
    return store._capture_id(  # noqa: SLF001 - deterministic schema-3 identity
        provider=PROVIDER,
        product=CAPTURE_PRODUCT,
        fetch_id=_role_fetch_id(job_id, satellite, "capture"),
    )


def _http_provenance(response: S3Response) -> dict[str, object]:
    return {
        "etag": response.etag,
        "last_modified": response.last_modified,
        "request_id": response.request_id,
        "request_id_2": response.request_id_2,
        "server_side_encryption": response.server_side_encryption,
        "version_id": response.version_id,
    }


def _listing_provenance(
    satellite: int,
    role: str,
    prefix: str,
    response: S3Response,
) -> dict[str, object]:
    return {
        "representation": NATIVE_REPRESENTATION,
        "capture_stage": NATIVE_CAPTURE_STAGE,
        "collector": RAW_COLLECTOR,
        "collector_version": RAW_COLLECTOR_VERSION,
        "satellite": satellite,
        "bucket": f"noaa-goes{satellite}",
        "operation": "ListObjectsV2",
        "role": role,
        "prefix": prefix,
        "list_type": 2,
        "max_keys": LIST_MAX_KEYS,
        "encoding_type": "url",
        "anonymous_public_access": True,
        "credentials_used": False,
        "http": _http_provenance(response),
        "decoded_or_admitted": False,
    }


def _object_provenance(
    selected: GoesRrqpeObject,
    response: S3Response,
) -> dict[str, object]:
    return {
        "representation": NATIVE_REPRESENTATION,
        "capture_stage": NATIVE_CAPTURE_STAGE,
        "collector": RAW_COLLECTOR,
        "collector_version": RAW_COLLECTOR_VERSION,
        "satellite": selected.satellite,
        "bucket": f"noaa-goes{selected.satellite}",
        "operation": "GetObject",
        "anonymous_public_access": True,
        "credentials_used": False,
        "upstream_key": selected.key,
        "filename": selected.filename,
        "product": PRODUCT_PREFIX,
        "abi_mode": selected.mode,
        "filename_observation_start": selected.observation_start,
        "filename_observation_end": selected.observation_end,
        "filename_creation_time": selected.creation_time,
        "listing_etag": selected.etag,
        "listing_last_modified": selected.last_modified,
        "listing_size_bytes": selected.size_bytes,
        "listing_storage_class": selected.storage_class,
        "listing_checksum_algorithms": list(selected.checksum_algorithms),
        "listing_checksum_type": selected.checksum_type,
        "http": _http_provenance(response),
        "schema3_full_object_buffer_required": True,
        "operational_memory_certified": False,
        "decoded_or_admitted": False,
        "official_product_maturity": {
            **OFFICIAL_MATURITY[selected.satellite],
            "provenance_only_not_a_quality_score": True,
        },
    }


def _optional_receipt(
    store: RawArtifactStore,
    *,
    job_id: str,
    satellite: int,
    role: str,
) -> tuple[RawArtifactReceipt | None, bytes | None]:
    loaded = store.load_receipt_bytes(_receipt_id(store, job_id, satellite, role))
    return (None, None) if loaded is None else loaded


def _validate_listing_receipt(
    receipt: RawArtifactReceipt,
    *,
    job_id: str,
    satellite: int,
    role: str,
) -> str:
    provenance = receipt.provenance
    prefix = provenance.get("prefix")
    if (
        receipt.provider != PROVIDER
        or receipt.product != LISTING_PRODUCT
        or receipt.fetch_id != _role_fetch_id(job_id, satellite, role)
        or receipt.content_type not in _ALLOWED_LISTING_CONTENT_TYPES
        or provenance.get("satellite") != satellite
        or provenance.get("bucket") != f"noaa-goes{satellite}"
        or provenance.get("operation") != "ListObjectsV2"
        or provenance.get("role") != role
        or provenance.get("list_type") != 2
        or provenance.get("max_keys") != LIST_MAX_KEYS
        or provenance.get("encoding_type") != "url"
        or provenance.get("anonymous_public_access") is not True
        or provenance.get("credentials_used") is not False
        or not isinstance(prefix, str)
        or _PREFIX_RE.fullmatch(prefix) is None
    ):
        raise GoesRrqpeNativeCorruptionError("duurzame GOES listingreceipt wijkt af")
    return prefix


def _validate_object_receipt(
    receipt: RawArtifactReceipt,
    *,
    job_id: str,
    selected: GoesRrqpeObject,
) -> None:
    provenance = receipt.provenance
    if (
        receipt.provider != PROVIDER
        or receipt.product != NETCDF4_PRODUCT
        or receipt.fetch_id != _role_fetch_id(job_id, selected.satellite, "netcdf4")
        or receipt.size_bytes != selected.size_bytes
        or receipt.content_type not in _ALLOWED_OBJECT_CONTENT_TYPES
        or provenance.get("satellite") != selected.satellite
        or provenance.get("bucket") != f"noaa-goes{selected.satellite}"
        or provenance.get("operation") != "GetObject"
        or provenance.get("anonymous_public_access") is not True
        or provenance.get("credentials_used") is not False
        or provenance.get("upstream_key") != selected.key
        or provenance.get("filename") != selected.filename
        or provenance.get("product") != PRODUCT_PREFIX
        or provenance.get("abi_mode") != selected.mode
        or provenance.get("filename_observation_start") != selected.observation_start
        or provenance.get("filename_observation_end") != selected.observation_end
        or provenance.get("filename_creation_time") != selected.creation_time
        or provenance.get("listing_etag") != selected.etag
        or provenance.get("listing_last_modified") != selected.last_modified
        or provenance.get("listing_size_bytes") != selected.size_bytes
        or provenance.get("listing_storage_class") != selected.storage_class
        or provenance.get("listing_checksum_algorithms") != list(selected.checksum_algorithms)
        or provenance.get("listing_checksum_type") != selected.checksum_type
    ):
        raise GoesRrqpeNativeCorruptionError("duurzame GOES NetCDF-receipt wijkt af")


def _source_reference(receipt: RawArtifactReceipt, *, role: str) -> dict[str, object]:
    return {
        "role": role,
        "receipt_id": receipt.receipt_id,
        "content_sha256": receipt.content_sha256,
        "receipt_sha256": receipt.receipt_sha256,
        "size_bytes": receipt.size_bytes,
        "received_at": receipt.received_at,
    }


def _validate_capture(
    capture: RawArtifactCaptureReceipt,
    *,
    job_id: str,
    satellite: int,
    receipts: Mapping[str, RawArtifactReceipt],
    selected: GoesRrqpeObject,
) -> None:
    members = {member.role: member for member in capture.members}
    if (
        capture.raw_store_schema_version != RAW_ARTIFACT_SCHEMA_VERSION
        or capture.provider != PROVIDER
        or capture.product != CAPTURE_PRODUCT
        or capture.fetch_id != _role_fetch_id(job_id, satellite, "capture")
        or capture.product_timestamp != selected.observation_start
        or set(members) != set(receipts)
        or any(
            members[role].receipt_id != receipt.receipt_id
            for role, receipt in receipts.items()
        )
    ):
        raise GoesRrqpeNativeCorruptionError("duurzame GOES complete capture wijkt af")


def _capture_result(
    capture: RawArtifactCaptureReceipt,
    *,
    receipts: Mapping[str, RawArtifactReceipt],
    selected: GoesRrqpeObject,
    identity: GoesRrqpeNetcdfIdentity,
) -> dict[str, object]:
    maturity = OFFICIAL_MATURITY[selected.satellite]
    return {
        "method": "goes_rrqpe_native_capture_v1",
        "capture_state": "captured_not_decoded",
        "weather_state": "unknown",
        "decode_status": "not_decoded",
        "pixel_values_read": False,
        "forecast_eligible": False,
        "quantitative_semantics_eligible": False,
        "operational_admission_eligible": False,
        "admission_status": ADMISSION_STATUS,
        "raw_provenance": {
            "raw_store_schema_version": RAW_ARTIFACT_SCHEMA_VERSION,
            "raw_capture_id": capture.capture_id,
            "raw_capture_manifest_sha256": capture.manifest_sha256,
            "raw_capture_receipt_sha256": capture.capture_receipt_sha256,
            "upstream_capture_id": capture.upstream_capture_id,
            "raw_member_count": len(receipts),
            "provenance_grade": "exact_raw",
            "provenance_scope": (
                "two_recent_s3_listobjects_v2_xml_responses_and_selected_original_netcdf4"
            ),
            "satellite": selected.satellite,
            "platform_id": identity.platform_id,
            "orbital_slot": identity.orbital_slot,
            "product": PRODUCT_PREFIX,
            "abi_mode": selected.mode,
            "selected_key": selected.key,
            "selected_filename": selected.filename,
            "selected_etag": selected.etag,
            "filename_observation_start": selected.observation_start,
            "filename_observation_end": selected.observation_end,
            "filename_creation_time": selected.creation_time,
            "official_product_maturity": {
                **maturity,
                "provenance_only_not_a_quality_score": True,
            },
            "sources": {
                role: _source_reference(receipt, role=role)
                for role, receipt in sorted(receipts.items())
            },
            "structure": {
                "inspector_version": STRUCTURE_INSPECTOR_VERSION,
                "container": "NetCDF-4/HDF5",
                "scene": "Full Disk",
                "spatial_resolution": FIXED_GRID_RESOLUTION,
                "rows": identity.rows,
                "columns": identity.columns,
                "rrqpe_dtype": identity.rrqpe_dtype,
                "rrqpe_chunks": list(identity.rrqpe_chunks),
                "rrqpe_compression": identity.rrqpe_compression,
                "rrqpe_compression_level": identity.rrqpe_compression_level,
                "dqf_present": identity.dqf_present,
                "dqf_dtype": identity.dqf_dtype,
                "dqf_chunks": list(identity.dqf_chunks),
                "dqf_compression": identity.dqf_compression,
                "dqf_compression_level": identity.dqf_compression_level,
                "dqf_attribute_names_sha256": identity.dqf_attribute_names_sha256,
                "dqf_flag_metadata_sha256": identity.dqf_flag_metadata_sha256,
                "dqf_codes_certified": False,
                "x_dtype": identity.x_dtype,
                "y_dtype": identity.y_dtype,
                "x_scale_factor": identity.x_scale_factor,
                "x_add_offset": identity.x_add_offset,
                "y_scale_factor": identity.y_scale_factor,
                "y_add_offset": identity.y_add_offset,
                "grid_mapping_name": "geostationary",
                "perspective_point_height": identity.perspective_point_height,
                "semi_major_axis": identity.semi_major_axis,
                "semi_minor_axis": identity.semi_minor_axis,
                "inverse_flattening": identity.inverse_flattening,
                "latitude_of_projection_origin": (
                    identity.latitude_of_projection_origin
                ),
                "longitude_of_projection_origin": (
                    identity.longitude_of_projection_origin
                ),
                "sweep_angle_axis": identity.sweep_angle_axis,
                "root_member_count": identity.root_member_count,
                "root_member_names_sha256": identity.root_member_names_sha256,
                "root_structure_sha256": identity.root_structure_sha256,
                "global_attribute_names_sha256": (
                    identity.global_attribute_names_sha256
                ),
                "pixel_values_read": identity.pixel_values_read,
            },
        },
    }


def _upstream_capture_id(
    receipts: Mapping[str, RawArtifactReceipt],
    selected: GoesRrqpeObject,
) -> str:
    return (
        f"{CAPTURE_PRODUCT}|G{selected.satellite}|key={selected.key}"
        f"|etag={selected.etag}"
        f"|previous={receipts['listing_previous'].content_sha256}"
        f"|current={receipts['listing_current'].content_sha256}"
        f"|netcdf4={receipts['netcdf4'].content_sha256}"
    )


def capture_latest_goes_rrqpe(
    job: Any,
    *,
    satellite: int,
    raw_store: RawArtifactStore | None = None,
    now: datetime | None = None,
    deadline_at: float | None = None,
    _fault_injector: Callable[[str], None] | None = None,
) -> dict[str, object]:
    """Capture one satellite's exact recent selection authority and NetCDF-4."""
    job_id = _validate_job(job)
    satellite = _satellite(satellite)
    monotonic_now = time.monotonic()
    if deadline_at is None:
        deadline = monotonic_now + CAPTURE_TOTAL_DEADLINE_SECONDS
    else:
        if (
            isinstance(deadline_at, bool)
            or not isinstance(deadline_at, (int, float))
            or not math.isfinite(float(deadline_at))
        ):
            raise ValueError("GOES deadline_at is ongeldig")
        deadline = min(
            float(deadline_at),
            monotonic_now + CAPTURE_TOTAL_DEADLINE_SECONDS,
        )
    _remaining(deadline)
    store = _raw_store(raw_store)
    roles = ("listing_previous", "listing_current")

    with _deadline_thread_lock(deadline=deadline), _deadline_file_lock(
        store, deadline=deadline
    ):
        _remaining(deadline)
        complete = store.load_capture_bundle_bytes(
            _capture_id(store, job_id, satellite)
        )
        if complete is not None:
            capture, receipts, payloads = complete
            if set(receipts) != {*roles, "netcdf4"} or set(payloads) != set(receipts):
                raise GoesRrqpeNativeCorruptionError("GOES capture mist verplichte rollen")
            prefixes = {
                role: _validate_listing_receipt(
                    receipts[role],
                    job_id=job_id,
                    satellite=satellite,
                    role=role,
                )
                for role in roles
            }
            if prefixes["listing_current"] != _adjacent_prefix(
                prefixes["listing_previous"]
            ):
                raise GoesRrqpeNativeCorruptionError("GOES listinguren zijn niet aangrenzend")
            groups = tuple(
                _parse_listing(
                    payloads[role],
                    satellite=satellite,
                    expected_prefix=prefixes[role],
                )
                for role in roles
            )
            selected = _select_latest(groups)
            _validate_object_receipt(
                receipts["netcdf4"], job_id=job_id, selected=selected
            )
            identity = _inspect_netcdf4(payloads["netcdf4"], selected=selected)
            _validate_capture(
                capture,
                job_id=job_id,
                satellite=satellite,
                receipts=receipts,
                selected=selected,
            )
            _remaining(deadline)
            return _capture_result(
                capture,
                receipts=receipts,
                selected=selected,
                identity=identity,
            )

        loaded = {
            role: _optional_receipt(
                store,
                job_id=job_id,
                satellite=satellite,
                role=role,
            )
            for role in (*roles, "netcdf4")
        }
        previous_receipt = loaded["listing_previous"][0]
        current_receipt = loaded["listing_current"][0]
        object_receipt = loaded["netcdf4"][0]
        if current_receipt is not None and previous_receipt is None:
            raise GoesRrqpeNativeCorruptionError(
                "GOES current-listing bestaat zonder previous-listing"
            )
        if object_receipt is not None and (
            previous_receipt is None or current_receipt is None
        ):
            raise GoesRrqpeNativeCorruptionError(
                "GOES NetCDF-receipt bestaat zonder beide listings"
            )
        if previous_receipt is None:
            prefixes = _listing_prefixes(now)
        else:
            previous_prefix = _validate_listing_receipt(
                previous_receipt,
                job_id=job_id,
                satellite=satellite,
                role="listing_previous",
            )
            prefixes = (previous_prefix, _adjacent_prefix(previous_prefix))

        receipts: dict[str, RawArtifactReceipt] = {}
        payloads: dict[str, bytes] = {}
        for role, prefix in zip(roles, prefixes, strict=True):
            receipt, payload = loaded[role]
            if receipt is None:
                response = _fetch_listing(satellite, prefix, deadline=deadline)
                digest = hashlib.sha256(response.payload).hexdigest()
                receipt = store.store(
                    provider=PROVIDER,
                    product=LISTING_PRODUCT,
                    fetch_id=_role_fetch_id(job_id, satellite, role),
                    content_type=response.content_type,
                    received_at=response.received_at,
                    upstream_id=(
                        f"s3-list-v2:noaa-goes{satellite}:{prefix}:"
                        f"max-{LIST_MAX_KEYS}:sha256:{digest}"
                    ),
                    provenance=_listing_provenance(
                        satellite, role, prefix, response
                    ),
                    payload=response.payload,
                )
                payload = response.payload
                if _fault_injector is not None:
                    _fault_injector(f"goes_rrqpe_native.after_{role}_store")
            else:
                stored_prefix = _validate_listing_receipt(
                    receipt,
                    job_id=job_id,
                    satellite=satellite,
                    role=role,
                )
                if stored_prefix != prefix or payload is None:
                    raise GoesRrqpeNativeCorruptionError(
                        "GOES duurzame listingcontext verschilt"
                    )
            receipts[role] = receipt
            payloads[role] = payload
            _remaining(deadline)

        # Both exact ListObjectsV2 response bodies are durable before XML parsing.
        groups = tuple(
            _parse_listing(
                payloads[role],
                satellite=satellite,
                expected_prefix=prefix,
            )
            for role, prefix in zip(roles, prefixes, strict=True)
        )
        selected = _select_latest(groups)
        _remaining(deadline)

        netcdf_receipt, netcdf_payload = loaded["netcdf4"]
        if netcdf_receipt is None:
            response = _fetch_object(selected, deadline=deadline)
            netcdf_receipt = store.store(
                provider=PROVIDER,
                product=NETCDF4_PRODUCT,
                fetch_id=_role_fetch_id(job_id, satellite, "netcdf4"),
                content_type=response.content_type,
                received_at=response.received_at,
                upstream_id=(
                    f"s3:noaa-goes{satellite}:{selected.key}:etag:{selected.etag}"
                ),
                provenance=_object_provenance(selected, response),
                payload=response.payload,
            )
            netcdf_payload = response.payload
            if _fault_injector is not None:
                _fault_injector("goes_rrqpe_native.after_netcdf4_store")
        else:
            _validate_object_receipt(
                netcdf_receipt, job_id=job_id, selected=selected
            )
        if netcdf_payload is None:
            raise GoesRrqpeNativeCorruptionError("GOES NetCDF-bytes ontbreken")
        receipts["netcdf4"] = netcdf_receipt
        payloads["netcdf4"] = netcdf_payload

        # The complete original object is durable before the first HDF5 access.
        identity = _inspect_netcdf4(netcdf_payload, selected=selected)
        _remaining(deadline)
        if _fault_injector is not None:
            _fault_injector("goes_rrqpe_native.before_capture_commit")
        capture = store.commit_capture(
            provider=PROVIDER,
            product=CAPTURE_PRODUCT,
            fetch_id=_role_fetch_id(job_id, satellite, "capture"),
            upstream_capture_id=_upstream_capture_id(receipts, selected),
            product_timestamp=identity.observation_start,
            members={role: receipt.receipt_id for role, receipt in receipts.items()},
        )
        if _fault_injector is not None:
            _fault_injector("goes_rrqpe_native.after_capture_commit")
        _validate_capture(
            capture,
            job_id=job_id,
            satellite=satellite,
            receipts=receipts,
            selected=selected,
        )
        _remaining(deadline)
        return _capture_result(
            capture,
            receipts=receipts,
            selected=selected,
            identity=identity,
        )


def capture_latest_goes18_rrqpe(job: Any, **kwargs: Any) -> dict[str, object]:
    """Explicit unwired GOES-18 wrapper."""
    return capture_latest_goes_rrqpe(job, satellite=18, **kwargs)


def capture_latest_goes19_rrqpe(job: Any, **kwargs: Any) -> dict[str, object]:
    """Explicit unwired GOES-19 wrapper."""
    return capture_latest_goes_rrqpe(job, satellite=19, **kwargs)


__all__ = [
    "ADMISSION_STATUS",
    "CAPTURE_PRODUCT",
    "GoesRrqpeNativeContractError",
    "GoesRrqpeNativeCorruptionError",
    "GoesRrqpeNativeDeadlineError",
    "GoesRrqpeNativeError",
    "GoesRrqpeNetcdfIdentity",
    "GoesRrqpeObject",
    "LISTING_PRODUCT",
    "NETCDF4_PRODUCT",
    "OFFICIAL_MATURITY",
    "S3Response",
    "capture_latest_goes18_rrqpe",
    "capture_latest_goes19_rrqpe",
    "capture_latest_goes_rrqpe",
]
