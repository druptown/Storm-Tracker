"""Isolated exact-byte capture for the public Met Office UK radar composite.

This candidate owns both public-S3 listings used at the UTC day boundary and
the selected ODIM HDF5 object.  Every upstream response is made durable in the
schema-3 raw store before XML or HDF5 inspection.  The module deliberately is
not a worker handler and never emits rain, dry, cell or admission decisions.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
import hashlib
import io
import math
from pathlib import Path
import re
import threading
import time
from typing import Any, Iterable
from urllib.parse import quote, unquote, urlencode, urlsplit
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


BUCKET_NAME = "met-office-radar-obs-data"
S3_HOST = "met-office-radar-obs-data.s3.eu-west-2.amazonaws.com"
S3_ORIGIN = f"https://{S3_HOST}"
S3_NAMESPACE = "http://s3.amazonaws.com/doc/2006-03-01/"
LIST_MAX_KEYS = 1_000
MAX_LISTING_BYTES = 2 * 1024 * 1024
MAX_HDF5_BYTES = 20 * 1024 * 1024
MAX_HEADER_CHARS = 8_192
HTTP_TIMEOUT_SECONDS = 30.0
CAPTURE_TOTAL_DEADLINE_SECONDS = 75.0
# A blocking urllib socket operation cannot observe the monotonic checkpoint
# until it returns.  Keep that final cooperative overshoot below the 15-second
# parent margin even if a peer stops sending midway through a response.
SOCKET_OPERATION_TIMEOUT_SECONDS = 5.0
STREAM_CHUNK_BYTES = 256 * 1024

PROVIDER = "met_office"
LISTING_PRODUCT = "met_office_uk_radar_s3_listobjects_v2_xml"
HDF5_PRODUCT = "met_office_uk_radar_rainrate_odim_hdf5"
CAPTURE_PRODUCT = "met_office_uk_radar_selection_capture"
RAW_COLLECTOR = "met_office_public_s3_native"
RAW_COLLECTOR_VERSION = "worker-met-office-exact-raw-v1"
STRUCTURE_INSPECTOR_VERSION = "met-office-uk-odim-identity-v1"
ADMISSION_STATUS = "exact_raw_decoder_and_held_out_validation_not_certified"

RAW_SHARE_ROOT = Path("/share/storm_tracker_worker")
RAW_ARTIFACT_ROOT = RAW_SHARE_ROOT / "raw" / "met_office_uk_radar"

ODIM_CONVENTIONS = "ODIM_H5/V2_2"
ODIM_VERSION = "H5rad 2.2"
ODIM_OBJECT = "COMP"
ODIM_PRODUCT = "COMP"
ODIM_QUANTITY = "RATE"
GRID_ROWS = 2_175
GRID_COLUMNS = 1_725
GRID_SCALE_METRES = 1_000.0
GRID_PROJECTION = (
    "+proj=tmerc +lat_0=49 +lon_0=-2 +k=0.999601 +x_0=400000 "
    "+y_0=-100000 +ellps=airy +units=m"
)
GRID_CORNERS = {
    "LL_lat": 43.83405184575088,
    "LL_lon": -12.011794919778003,
    "LR_lat": 43.70087658067965,
    "LR_lon": 9.415780820163617,
    "UL_lat": 62.92071695962483,
    "UL_lon": -17.973932221553792,
    "UR_lat": 62.651484803966376,
    "UR_lon": 16.128870790695817,
}

_KEY_RE = re.compile(
    r"^radar/(?P<day>\d{4}/\d{2}/\d{2})/"
    r"(?P<stamp>\d{12})_ODIM_ng_radar_rainrate_composite_1km_UK\.h5$"
)
_PREFIX_RE = re.compile(r"^radar/\d{4}/\d{2}/\d{2}/$")
_JOB_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,191}$")
_ETAG_RE = re.compile(r'^"[0-9A-Fa-f]{32}"$')
_WMO_RE = re.compile(r"^WMO:\d{5}$")
_NODE_RE = re.compile(r"^[a-z0-9]{3,16}$")
_ALLOWED_LIST_CONTENT_TYPES = frozenset({"application/xml", "text/xml"})
_ALLOWED_OBJECT_CONTENT_TYPES = frozenset({
    "application/octet-stream",
    "application/x-hdf5",
    "binary/octet-stream",
})
_RAW_STORE: RawArtifactStore | None = None
_RAW_STORE_LOCK = threading.Lock()
_CAPTURE_LOCK = threading.Lock()
_DEADLINE_CONTEXT = threading.local()


class MetOfficeNativeError(ValueError):
    """The upstream response is outside the bounded native profile."""


class MetOfficeNativeCorruptionError(MetOfficeNativeError):
    """Durable evidence conflicts with its immutable identity."""


class MetOfficeNativeDeadlineError(MetOfficeNativeError):
    """The one cooperative capture deadline elapsed."""


@dataclass(frozen=True, slots=True)
class S3Response:
    payload: bytes
    content_type: str
    received_at: float
    etag: str | None
    last_modified: float | None
    version_id: str | None
    request_id: str | None
    request_id_2: str | None
    server_side_encryption: str | None


@dataclass(frozen=True, slots=True)
class MetOfficeObject:
    key: str
    product_timestamp: float
    last_modified: float
    etag: str
    size_bytes: int
    storage_class: str
    checksum_algorithm: str | None
    checksum_type: str | None


@dataclass(frozen=True, slots=True)
class MetOfficeOdimIdentity:
    product_timestamp: float
    input_window_start: float
    input_window_end: float
    rows: int
    columns: int
    xscale_metres: float
    yscale_metres: float
    projection_sha256: str
    quantity: str
    dtype: str
    chunk_rows: int
    chunk_columns: int
    compression: str
    compression_level: int
    chunk_count: int
    stored_raster_bytes: int
    source_wmo_ids: tuple[str, ...]
    source_nodes: tuple[str, ...]
    source_wmo_sha256: str
    source_nodes_sha256: str
    software: str
    software_version: str
    task: str
    zr_a: float
    zr_b: float


class _RejectRedirects(HTTPRedirectHandler):
    def redirect_request(self, *_args, **_kwargs):
        raise MetOfficeNativeError("Met Office S3-redirect is niet toegestaan")


def _open_without_redirects(request: Request, *, timeout: float):
    return build_opener(ProxyHandler({}), _RejectRedirects()).open(
        request, timeout=timeout
    )


def _job_id(value: object) -> str:
    if not isinstance(value, str) or _JOB_ID_RE.fullmatch(value) is None:
        raise MetOfficeNativeError("Met Office capturejob mist een geldige job_id")
    return value


def _empty_payload(value: object) -> None:
    if type(value) is not dict or value:
        raise MetOfficeNativeError(
            "Met Office native capture vereist een exact lege jobpayload"
        )


def _header(
    headers: Any,
    name: str,
    *,
    required: bool = False,
    maximum: int = MAX_HEADER_CHARS,
) -> str | None:
    value = headers.get(name) if headers is not None else None
    if value is None:
        if required:
            raise MetOfficeNativeError(f"Met Office HTTP-header {name} ontbreekt")
        return None
    text = str(value).strip()
    if (
        not text
        or len(text) > maximum
        or any(ord(character) < 0x20 or ord(character) == 0x7F for character in text)
    ):
        raise MetOfficeNativeError(f"Met Office HTTP-header {name} is ongeldig")
    return text


def _content_type(headers: Any) -> str:
    value = _header(headers, "Content-Type", required=True, maximum=256)
    assert value is not None
    return value.split(";", 1)[0].strip().lower()


def _content_length(
    headers: Any,
    *,
    maximum_bytes: int,
    required: bool,
) -> int | None:
    value = _header(headers, "Content-Length", required=required, maximum=32)
    if value is None:
        return None
    if not value.isascii() or not value.isdecimal():
        raise MetOfficeNativeError("Met Office Content-Length is ongeldig")
    result = int(value)
    if not 1 <= result <= maximum_bytes:
        raise MetOfficeNativeError("Met Office Content-Length valt buiten de grens")
    return result


def _rfc3339(value: str, *, context: str) -> float:
    if not isinstance(value, str) or not 1 <= len(value) <= 64:
        raise MetOfficeNativeError(f"{context} bevat geen begrensde tijd")
    candidate = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        moment = datetime.fromisoformat(candidate)
    except ValueError as err:
        raise MetOfficeNativeError(f"{context} bevat geen geldige tijd") from err
    if moment.tzinfo is None or moment.utcoffset() is None:
        raise MetOfficeNativeError(f"{context} mist een tijdzone")
    result = moment.timestamp()
    if not math.isfinite(result):
        raise MetOfficeNativeError(f"{context} bevat geen eindige tijd")
    return result


def _http_date(value: str, *, context: str) -> float:
    try:
        moment = parsedate_to_datetime(value)
    except (TypeError, ValueError) as err:
        raise MetOfficeNativeError(f"{context} bevat geen geldige HTTP-datum") from err
    if moment.tzinfo is None or moment.utcoffset() is None:
        raise MetOfficeNativeError(f"{context} mist een tijdzone")
    result = moment.timestamp()
    if not math.isfinite(result):
        raise MetOfficeNativeError(f"{context} bevat geen eindige tijd")
    return result


def _fixed_url(url: str, *, expected_path: str, expected_query: str) -> str:
    parts = urlsplit(url)
    if (
        parts.scheme != "https"
        or parts.hostname != S3_HOST
        or parts.port is not None
        or parts.username is not None
        or parts.password is not None
        or parts.path != expected_path
        or parts.query != expected_query
        or parts.fragment
    ):
        raise MetOfficeNativeError("Met Office URL valt buiten het vaste endpoint")
    return url


def _remaining() -> float:
    """Return the active capture remainder or the legacy per-call timeout."""

    deadline = getattr(_DEADLINE_CONTEXT, "deadline", None)
    if deadline is None:
        return HTTP_TIMEOUT_SECONDS
    remaining = float(deadline) - time.monotonic()
    if not math.isfinite(remaining) or remaining <= 0.0:
        raise MetOfficeNativeDeadlineError(
            "Met Office capturedeadline is verstreken"
        )
    return remaining


@contextmanager
def _capture_deadline(deadline: float):
    previous = getattr(_DEADLINE_CONTEXT, "deadline", None)
    _DEADLINE_CONTEXT.deadline = deadline
    try:
        _remaining()
        yield
    finally:
        if previous is None:
            try:
                del _DEADLINE_CONTEXT.deadline
            except AttributeError:  # pragma: no cover - defensive cleanup
                pass
        else:
            _DEADLINE_CONTEXT.deadline = previous


def _read_bounded(response: Any, *, maximum_bytes: int, declared: int | None) -> bytes:
    chunks: list[bytes] = []
    total = 0
    while True:
        _remaining()
        block = response.read(min(STREAM_CHUNK_BYTES, maximum_bytes + 1 - total))
        _remaining()
        if not block:
            break
        if not isinstance(block, bytes):
            raise MetOfficeNativeError("Met Office transport leverde geen bytes")
        chunks.append(block)
        total += len(block)
        if total > maximum_bytes:
            raise MetOfficeNativeError("Met Office response overschrijdt bytegrens")
    payload = b"".join(chunks)
    if not payload:
        raise MetOfficeNativeError("Met Office response is leeg")
    if declared is not None and len(payload) != declared:
        raise MetOfficeNativeError("Met Office payloadlengte wijkt af van header")
    return payload


def _fetch_s3(
    url: str,
    *,
    expected_path: str,
    expected_query: str,
    maximum_bytes: int,
    allowed_content_types: frozenset[str],
    object_metadata_required: bool,
    if_match: str | None = None,
) -> S3Response:
    _remaining()
    _fixed_url(url, expected_path=expected_path, expected_query=expected_query)
    headers = {"Accept-Encoding": "identity", "User-Agent": "StormTrackerWorker/1"}
    if if_match is not None:
        if _ETAG_RE.fullmatch(if_match) is None:
            raise MetOfficeNativeError("Met Office If-Match ETag is ongeldig")
        headers["If-Match"] = if_match
    request = Request(url, headers=headers)
    with _open_without_redirects(
        request,
        timeout=min(
            HTTP_TIMEOUT_SECONDS,
            SOCKET_OPERATION_TIMEOUT_SECONDS,
            _remaining(),
        ),
    ) as response:
        _remaining()
        status = int(getattr(response, "status", 0))
        if status != 200:
            raise MetOfficeNativeError(f"Met Office HTTP-status is {status}, geen 200")
        _fixed_url(
            str(response.geturl()),
            expected_path=expected_path,
            expected_query=expected_query,
        )
        response_headers = response.headers
        encoding = _header(response_headers, "Content-Encoding", maximum=128)
        if encoding is not None and encoding.lower() != "identity":
            raise MetOfficeNativeError(
                "Met Office response gebruikt transportcompressie"
            )
        content_type = _content_type(response_headers)
        if content_type not in allowed_content_types:
            raise MetOfficeNativeError("Met Office Content-Type is niet toegestaan")
        declared = _content_length(
            response_headers,
            maximum_bytes=maximum_bytes,
            required=object_metadata_required,
        )
        etag = _header(response_headers, "ETag", required=object_metadata_required)
        if etag is not None and _ETAG_RE.fullmatch(etag) is None:
            raise MetOfficeNativeError("Met Office ETag is ongeldig")
        modified_text = _header(
            response_headers, "Last-Modified", required=object_metadata_required
        )
        modified = (
            None
            if modified_text is None
            else _http_date(modified_text, context="Met Office Last-Modified")
        )
        received_at = time.time()
        if not math.isfinite(received_at) or received_at < 0:
            raise MetOfficeNativeError("Met Office ontvangsttijd is ongeldig")
        payload = _read_bounded(
            response, maximum_bytes=maximum_bytes, declared=declared
        )
        _remaining()
        return S3Response(
            payload=payload,
            content_type=content_type,
            received_at=received_at,
            etag=etag,
            last_modified=modified,
            version_id=_header(response_headers, "x-amz-version-id", maximum=1024),
            request_id=_header(response_headers, "x-amz-request-id", maximum=256),
            request_id_2=_header(response_headers, "x-amz-id-2", maximum=2048),
            server_side_encryption=_header(
                response_headers, "x-amz-server-side-encryption", maximum=128
            ),
        )


def _listing_prefixes(now: datetime | None = None) -> tuple[str, str]:
    moment = datetime.now(timezone.utc) if now is None else now
    if not isinstance(moment, datetime) or moment.tzinfo is None or moment.utcoffset() is None:
        raise MetOfficeNativeError("Met Office referentietijd moet tijdzonebewust zijn")
    current = moment.astimezone(timezone.utc)
    previous = current - timedelta(days=1)
    return (
        f"radar/{previous:%Y/%m/%d}/",
        f"radar/{current:%Y/%m/%d}/",
    )


def _adjacent_prefix(previous_prefix: str) -> str:
    if _PREFIX_RE.fullmatch(previous_prefix) is None:
        raise MetOfficeNativeCorruptionError("duurzame listingprefix is ongeldig")
    moment = datetime.strptime(previous_prefix[6:16], "%Y/%m/%d").replace(
        tzinfo=timezone.utc
    )
    return f"radar/{moment + timedelta(days=1):%Y/%m/%d}/"


def _listing_request(prefix: str) -> tuple[str, str, str]:
    if _PREFIX_RE.fullmatch(prefix) is None:
        raise MetOfficeNativeError("Met Office listingprefix is ongeldig")
    query = urlencode((
        ("list-type", "2"),
        ("prefix", prefix),
        ("max-keys", str(LIST_MAX_KEYS)),
        ("encoding-type", "url"),
    ))
    url = f"{S3_ORIGIN}/?{query}"
    return _fixed_url(url, expected_path="/", expected_query=query), "/", query


def _fetch_listing(prefix: str) -> S3Response:
    url, path, query = _listing_request(prefix)
    return _fetch_s3(
        url,
        expected_path=path,
        expected_query=query,
        maximum_bytes=MAX_LISTING_BYTES,
        allowed_content_types=_ALLOWED_LIST_CONTENT_TYPES,
        object_metadata_required=False,
    )


def _object_url(key: str) -> tuple[str, str]:
    if _KEY_RE.fullmatch(key) is None:
        raise MetOfficeNativeError("Met Office objectkey is ongeldig")
    path = "/" + quote(key, safe="/")
    url = f"{S3_ORIGIN}{path}"
    return _fixed_url(url, expected_path=path, expected_query=""), path


def _fetch_object(selected: MetOfficeObject) -> S3Response:
    url, path = _object_url(selected.key)
    response = _fetch_s3(
        url,
        expected_path=path,
        expected_query="",
        maximum_bytes=MAX_HDF5_BYTES,
        allowed_content_types=_ALLOWED_OBJECT_CONTENT_TYPES,
        object_metadata_required=True,
        if_match=selected.etag,
    )
    if (
        response.etag != selected.etag
        or len(response.payload) != selected.size_bytes
        or response.last_modified != selected.last_modified
    ):
        raise MetOfficeNativeError(
            "Met Office objectrevision wijkt af van het listingbewijs"
        )
    return response


def _children(element: ET.Element) -> dict[str, list[ET.Element]]:
    grouped: dict[str, list[ET.Element]] = {}
    for child in element:
        if not child.tag.startswith("{" + S3_NAMESPACE + "}"):
            raise MetOfficeNativeError("Met Office listing bevat vreemde namespace")
        grouped.setdefault(child.tag.rsplit("}", 1)[-1], []).append(child)
    return grouped


def _scalar(
    grouped: dict[str, list[ET.Element]],
    name: str,
    *,
    maximum: int = 4_096,
) -> str:
    values = grouped.get(name, [])
    if len(values) != 1 or values[0].text is None:
        raise MetOfficeNativeError(f"Met Office listingveld {name} is niet exact één")
    value = values[0].text
    if not 1 <= len(value) <= maximum:
        raise MetOfficeNativeError(f"Met Office listingveld {name} is onbegrensd")
    return value


def _parse_product_key(key: str, *, expected_prefix: str) -> float:
    match = _KEY_RE.fullmatch(key)
    if match is None or not key.startswith(expected_prefix):
        raise MetOfficeNativeError("Met Office listing bevat een vreemde objectkey")
    try:
        moment = datetime.strptime(match.group("stamp"), "%Y%m%d%H%M").replace(
            tzinfo=timezone.utc
        )
    except ValueError as err:
        raise MetOfficeNativeError("Met Office objectkey bevat ongeldige tijd") from err
    if match.group("day") != moment.strftime("%Y/%m/%d") or moment.minute % 15:
        raise MetOfficeNativeError("Met Office objectkey en productslot verschillen")
    return moment.timestamp()


def _parse_listing(payload: bytes, *, expected_prefix: str) -> tuple[MetOfficeObject, ...]:
    try:
        root = ET.fromstring(payload)
    except ET.ParseError as err:
        raise MetOfficeNativeError("Met Office listing-XML is ongeldig") from err
    if root.tag != f"{{{S3_NAMESPACE}}}ListBucketResult":
        raise MetOfficeNativeError("Met Office listing heeft verkeerde root")
    grouped = _children(root)
    allowed = {
        "Name", "Prefix", "KeyCount", "MaxKeys", "IsTruncated",
        "EncodingType", "Contents",
    }
    if set(grouped) - allowed:
        raise MetOfficeNativeError("Met Office listing bevat onbekende velden")
    if _scalar(grouped, "Name") != BUCKET_NAME:
        raise MetOfficeNativeError("Met Office listing noemt verkeerde bucket")
    prefix = unquote(_scalar(grouped, "Prefix"))
    if prefix != expected_prefix:
        raise MetOfficeNativeError("Met Office listingprefix wijkt af")
    if _scalar(grouped, "EncodingType") != "url":
        raise MetOfficeNativeError("Met Office listing mist URL-encoding")
    if _scalar(grouped, "MaxKeys") != str(LIST_MAX_KEYS):
        raise MetOfficeNativeError("Met Office listing MaxKeys wijkt af")
    if _scalar(grouped, "IsTruncated") != "false":
        raise MetOfficeNativeError("Met Office listing is onvolledig/truncated")
    key_count_text = _scalar(grouped, "KeyCount")
    if not key_count_text.isascii() or not key_count_text.isdecimal():
        raise MetOfficeNativeError("Met Office KeyCount is ongeldig")
    contents = grouped.get("Contents", [])
    if int(key_count_text) != len(contents) or len(contents) > LIST_MAX_KEYS:
        raise MetOfficeNativeError("Met Office KeyCount en Contents verschillen")

    result: list[MetOfficeObject] = []
    seen: set[str] = set()
    for entry in contents:
        fields = _children(entry)
        base_fields = {"Key", "LastModified", "ETag", "Size", "StorageClass"}
        checksum_fields = {"ChecksumAlgorithm", "ChecksumType"}
        actual_fields = frozenset(fields)
        if actual_fields not in {
            frozenset(base_fields),
            frozenset(base_fields | checksum_fields),
        }:
            raise MetOfficeNativeError("Met Office Contents-profiel wijkt af")
        key = unquote(_scalar(fields, "Key", maximum=1_024))
        product_timestamp = _parse_product_key(key, expected_prefix=expected_prefix)
        if key in seen:
            raise MetOfficeNativeError("Met Office listing bevat dubbele objectkey")
        seen.add(key)
        etag = _scalar(fields, "ETag", maximum=128)
        if _ETAG_RE.fullmatch(etag) is None:
            raise MetOfficeNativeError("Met Office listing-ETag is ongeldig")
        size_text = _scalar(fields, "Size", maximum=32)
        if not size_text.isascii() or not size_text.isdecimal():
            raise MetOfficeNativeError("Met Office listingsize is ongeldig")
        size = int(size_text)
        if not 1 <= size <= MAX_HDF5_BYTES:
            raise MetOfficeNativeError("Met Office listingsize valt buiten grens")
        storage_class = _scalar(fields, "StorageClass", maximum=64)
        if storage_class not in {"STANDARD", "INTELLIGENT_TIERING"}:
            raise MetOfficeNativeError("Met Office storage class is onbekend")
        checksum_algorithm = None
        checksum_type = None
        if checksum_fields <= set(fields):
            checksum_algorithm = _scalar(fields, "ChecksumAlgorithm", maximum=32)
            checksum_type = _scalar(fields, "ChecksumType", maximum=32)
            if checksum_algorithm != "SHA256" or checksum_type != "FULL_OBJECT":
                raise MetOfficeNativeError("Met Office checksumprofiel is onbekend")
        result.append(MetOfficeObject(
            key=key,
            product_timestamp=product_timestamp,
            last_modified=_rfc3339(
                _scalar(fields, "LastModified"), context="Met Office LastModified"
            ),
            etag=etag,
            size_bytes=size,
            storage_class=storage_class,
            checksum_algorithm=checksum_algorithm,
            checksum_type=checksum_type,
        ))
    return tuple(sorted(result, key=lambda item: (item.product_timestamp, item.key)))


def _select_latest(groups: Iterable[tuple[MetOfficeObject, ...]]) -> MetOfficeObject:
    objects = [item for group in groups for item in group]
    if not objects:
        raise MetOfficeNativeError("Met Office listings bevatten geen HDF5-product")
    by_key: dict[str, MetOfficeObject] = {}
    for item in objects:
        previous = by_key.setdefault(item.key, item)
        if previous != item:
            raise MetOfficeNativeError("Met Office objectkey heeft conflicterende revisie")
    return max(by_key.values(), key=lambda item: (item.product_timestamp, item.key))


def _attribute_text(attributes: Any, name: str) -> str:
    if name not in attributes:
        raise MetOfficeNativeError(f"Met Office ODIM-attribuut {name} ontbreekt")
    value = attributes[name]
    if isinstance(value, bytes):
        try:
            value = value.decode("utf-8")
        except UnicodeDecodeError as err:
            raise MetOfficeNativeError(
                f"Met Office ODIM-attribuut {name} is geen UTF-8"
            ) from err
    if not isinstance(value, str) or not 1 <= len(value) <= 8_192:
        raise MetOfficeNativeError(f"Met Office ODIM-attribuut {name} is ongeldig")
    return value


def _attribute_float(attributes: Any, name: str) -> float:
    if name not in attributes:
        raise MetOfficeNativeError(f"Met Office ODIM-attribuut {name} ontbreekt")
    value = attributes[name]
    if isinstance(value, np.ndarray):
        if value.shape != ():
            raise MetOfficeNativeError(f"Met Office ODIM-attribuut {name} is niet scalar")
        value = value.item()
    if isinstance(value, (bool, np.bool_)) or not isinstance(value, (int, float, np.number)):
        raise MetOfficeNativeError(f"Met Office ODIM-attribuut {name} is geen getal")
    result = float(value)
    if not math.isfinite(result):
        raise MetOfficeNativeError(f"Met Office ODIM-attribuut {name} is niet eindig")
    return result


def _exact_attributes(value: Any, names: set[str], context: str) -> None:
    if set(value.attrs.keys()) != names:
        raise MetOfficeNativeError(f"Met Office {context}-attributen wijken af")


def _hard_child(group: h5py.Group, name: str, expected_type: type):
    link = group.get(name, getlink=True)
    if not isinstance(link, h5py.HardLink):
        raise MetOfficeNativeError("Met Office ODIM bevat externe/symbolische link")
    child = group.get(name)
    if not isinstance(child, expected_type):
        raise MetOfficeNativeError(f"Met Office ODIM-object {name} heeft verkeerd type")
    return child


def _odim_time(date_value: str, time_value: str, *, context: str) -> float:
    if not re.fullmatch(r"\d{8}", date_value) or not re.fullmatch(r"\d{6}", time_value):
        raise MetOfficeNativeError(f"Met Office {context} is geen ODIM-tijd")
    try:
        return datetime.strptime(date_value + time_value, "%Y%m%d%H%M%S").replace(
            tzinfo=timezone.utc
        ).timestamp()
    except ValueError as err:
        raise MetOfficeNativeError(f"Met Office {context} is ongeldig") from err


def _sha256_ids(values: tuple[str, ...]) -> str:
    return hashlib.sha256("\n".join(values).encode("ascii")).hexdigest()


def _inspect_odim(payload: bytes, *, selected: MetOfficeObject) -> MetOfficeOdimIdentity:
    if not isinstance(payload, bytes) or len(payload) != selected.size_bytes:
        raise MetOfficeNativeError("Met Office HDF5-bytes en listing verschillen")
    if payload[:8] != b"\x89HDF\r\n\x1a\n":
        raise MetOfficeNativeError("Met Office object mist HDF5-signature")
    try:
        root = h5py.File(io.BytesIO(payload), "r")
    except (OSError, ValueError) as err:
        raise MetOfficeNativeError("Met Office object is geen leesbare HDF5") from err
    with root:
        if set(root.keys()) != {"dataset1", "how", "what", "where"}:
            raise MetOfficeNativeError("Met Office ODIM-rootprofiel wijkt af")
        _exact_attributes(root, {"Conventions"}, "root")
        if _attribute_text(root.attrs, "Conventions") != ODIM_CONVENTIONS:
            raise MetOfficeNativeError("Met Office ODIM-Conventions wijkt af")

        what = _hard_child(root, "what", h5py.Group)
        where = _hard_child(root, "where", h5py.Group)
        how = _hard_child(root, "how", h5py.Group)
        dataset = _hard_child(root, "dataset1", h5py.Group)
        if set(dataset.keys()) != {"data1", "how", "what"}:
            raise MetOfficeNativeError("Met Office dataset1-profiel wijkt af")
        data_group = _hard_child(dataset, "data1", h5py.Group)
        dataset_how = _hard_child(dataset, "how", h5py.Group)
        dataset_what = _hard_child(dataset, "what", h5py.Group)
        if set(data_group.keys()) != {"data", "what"}:
            raise MetOfficeNativeError("Met Office data1-profiel wijkt af")
        raster = _hard_child(data_group, "data", h5py.Dataset)
        data_what = _hard_child(data_group, "what", h5py.Group)

        _exact_attributes(what, {"date", "object", "source", "time", "version"}, "what")
        _exact_attributes(where, {
            "LL_lat", "LL_lon", "LR_lat", "LR_lon", "UL_lat", "UL_lon",
            "UR_lat", "UR_lon", "projdef", "xscale", "xsize", "yscale", "ysize",
        }, "where")
        _exact_attributes(how, {
            "VPRCorr", "camethod", "endepochs", "nodes", "simulated", "software",
            "startepochs", "sw_version", "task", "zr_a", "zr_b",
        }, "how")
        _exact_attributes(dataset, set(), "dataset1")
        _exact_attributes(data_group, set(), "data1")
        _exact_attributes(dataset_how, {"origin"}, "dataset1/how")
        _exact_attributes(dataset_what, {
            "enddate", "endtime", "product", "startdate", "starttime",
        }, "dataset1/what")
        _exact_attributes(data_what, {"gain", "nodata", "offset", "quantity", "undetect"}, "data1/what")
        _exact_attributes(raster, set(), "raster")

        if (
            _attribute_text(what.attrs, "object") != ODIM_OBJECT
            or _attribute_text(what.attrs, "version") != ODIM_VERSION
            or _attribute_text(dataset_what.attrs, "product") != ODIM_PRODUCT
            or _attribute_text(data_what.attrs, "quantity") != ODIM_QUANTITY
            or _attribute_text(dataset_how.attrs, "origin") != "UPPER LEFT"
        ):
            raise MetOfficeNativeError("Met Office ODIM-productsemantiek wijkt af")
        product_timestamp = _odim_time(
            _attribute_text(what.attrs, "date"),
            _attribute_text(what.attrs, "time"),
            context="nominale producttijd",
        )
        start = _odim_time(
            _attribute_text(dataset_what.attrs, "startdate"),
            _attribute_text(dataset_what.attrs, "starttime"),
            context="inputstart",
        )
        end = _odim_time(
            _attribute_text(dataset_what.attrs, "enddate"),
            _attribute_text(dataset_what.attrs, "endtime"),
            context="inputeinde",
        )
        if (
            product_timestamp != selected.product_timestamp
            or start > end
            or end - start > 15 * 60
            or abs(start - product_timestamp) > 15 * 60
            or abs(end - product_timestamp) > 15 * 60
        ):
            raise MetOfficeNativeError("Met Office filename- en ODIM-tijden verschillen")
        if (
            _attribute_float(how.attrs, "startepochs") != start
            or _attribute_float(how.attrs, "endepochs") != end
        ):
            raise MetOfficeNativeError("Met Office ODIM epoch- en teksttijden verschillen")

        if (
            raster.shape != (GRID_ROWS, GRID_COLUMNS)
            or raster.dtype.kind != "f"
            or raster.dtype.itemsize != 4
            or int(_attribute_float(where.attrs, "ysize")) != GRID_ROWS
            or int(_attribute_float(where.attrs, "xsize")) != GRID_COLUMNS
            or _attribute_float(where.attrs, "xscale") != GRID_SCALE_METRES
            or _attribute_float(where.attrs, "yscale") != GRID_SCALE_METRES
            or _attribute_text(where.attrs, "projdef") != GRID_PROJECTION
        ):
            raise MetOfficeNativeError("Met Office ODIM-gridprofiel wijkt af")
        for name, expected in GRID_CORNERS.items():
            if not math.isclose(
                _attribute_float(where.attrs, name), expected, rel_tol=0.0, abs_tol=1e-12
            ):
                raise MetOfficeNativeError("Met Office ODIM-gridhoek wijkt af")
        for name, expected in {
            "gain": 1.0, "offset": 0.0, "nodata": -1.0, "undetect": 0.0,
        }.items():
            if _attribute_float(data_what.attrs, name) != expected:
                raise MetOfficeNativeError("Met Office RATE-codering wijkt af")

        creation = raster.id.get_create_plist()
        filter_count = creation.get_nfilters()
        filter_definition = creation.get_filter(0) if filter_count == 1 else None
        chunk_count = int(raster.id.get_num_chunks())
        expected_chunk_count = math.ceil(GRID_ROWS / int(raster.chunks[0])) if raster.chunks else 0
        if (
            creation.get_external_count() != 0
            or creation.get_layout() == h5py.h5d.VIRTUAL
            or raster.chunks is None
            or raster.compression != "gzip"
            or raster.compression_opts != 6
            or raster.chunks[1] != GRID_COLUMNS
            or filter_count != 1
            or filter_definition is None
            or int(filter_definition[0]) != int(h5py.h5z.FILTER_DEFLATE)
            or tuple(int(value) for value in filter_definition[2]) != (6,)
            or chunk_count != expected_chunk_count
            or raster.id.get_storage_size() <= 0
        ):
            raise MetOfficeNativeError("Met Office rasteropslagprofiel wijkt af")

        sources = tuple(_attribute_text(what.attrs, "source").split(","))
        nodes = tuple(_attribute_text(how.attrs, "nodes").split(","))
        if (
            not 1 <= len(sources) <= 64
            or not 1 <= len(nodes) <= 64
            or len(set(sources)) != len(sources)
            or len(set(nodes)) != len(nodes)
            or any(_WMO_RE.fullmatch(value) is None for value in sources)
            or any(_NODE_RE.fullmatch(value) is None for value in nodes)
        ):
            raise MetOfficeNativeError("Met Office bronidentiteiten zijn ongeldig")
        if (
            _attribute_text(how.attrs, "VPRCorr") != "True"
            or _attribute_text(how.attrs, "simulated") != "False"
            or _attribute_text(how.attrs, "camethod") != "DOMAIN"
        ):
            raise MetOfficeNativeError("Met Office processingflags wijken af")

        return MetOfficeOdimIdentity(
            product_timestamp=product_timestamp,
            input_window_start=start,
            input_window_end=end,
            rows=GRID_ROWS,
            columns=GRID_COLUMNS,
            xscale_metres=GRID_SCALE_METRES,
            yscale_metres=GRID_SCALE_METRES,
            projection_sha256=hashlib.sha256(GRID_PROJECTION.encode("ascii")).hexdigest(),
            quantity=ODIM_QUANTITY,
            dtype=str(raster.dtype),
            chunk_rows=int(raster.chunks[0]),
            chunk_columns=int(raster.chunks[1]),
            compression=str(raster.compression),
            compression_level=int(raster.compression_opts),
            chunk_count=chunk_count,
            stored_raster_bytes=int(raster.id.get_storage_size()),
            source_wmo_ids=sources,
            source_nodes=nodes,
            source_wmo_sha256=_sha256_ids(sources),
            source_nodes_sha256=_sha256_ids(nodes),
            software=_attribute_text(how.attrs, "software"),
            software_version=_attribute_text(how.attrs, "sw_version"),
            task=_attribute_text(how.attrs, "task"),
            zr_a=_attribute_float(how.attrs, "zr_a"),
            zr_b=_attribute_float(how.attrs, "zr_b"),
        )


def _raw_store(injected: RawArtifactStore | None = None) -> RawArtifactStore:
    if injected is not None:
        injected.initialize()
        return injected
    global _RAW_STORE
    with _RAW_STORE_LOCK:
        if _RAW_STORE is None:
            candidate = RawArtifactStore(RAW_ARTIFACT_ROOT)
            candidate.initialize()
            _RAW_STORE = candidate
        return _RAW_STORE


def _http_provenance(response: S3Response) -> dict[str, object]:
    return {
        "etag": response.etag,
        "last_modified": response.last_modified,
        "request_id": response.request_id,
        "request_id_2": response.request_id_2,
        "server_side_encryption": response.server_side_encryption,
        "version_id": response.version_id,
    }


def _listing_provenance(role: str, prefix: str, response: S3Response) -> dict[str, object]:
    return {
        "representation": NATIVE_REPRESENTATION,
        "capture_stage": NATIVE_CAPTURE_STAGE,
        "collector": RAW_COLLECTOR,
        "collector_version": RAW_COLLECTOR_VERSION,
        "bucket": BUCKET_NAME,
        "operation": "ListObjectsV2",
        "role": role,
        "prefix": prefix,
        "list_type": 2,
        "max_keys": LIST_MAX_KEYS,
        "encoding_type": "url",
        "http": _http_provenance(response),
        "decoded_or_admitted": False,
    }


def _object_provenance(selected: MetOfficeObject, response: S3Response) -> dict[str, object]:
    return {
        "representation": NATIVE_REPRESENTATION,
        "capture_stage": NATIVE_CAPTURE_STAGE,
        "collector": RAW_COLLECTOR,
        "collector_version": RAW_COLLECTOR_VERSION,
        "bucket": BUCKET_NAME,
        "operation": "GetObject",
        "upstream_key": selected.key,
        "listing_etag": selected.etag,
        "listing_last_modified": selected.last_modified,
        "listing_size_bytes": selected.size_bytes,
        "listing_storage_class": selected.storage_class,
        "listing_checksum_algorithm": selected.checksum_algorithm,
        "listing_checksum_type": selected.checksum_type,
        "filename_product_timestamp": selected.product_timestamp,
        "http": _http_provenance(response),
        "schema3_full_object_buffer_required": True,
        "operational_memory_certified": False,
        "decoded_or_admitted": False,
    }


def _receipt_id(store: RawArtifactStore, job_id: str, role: str) -> str:
    product = LISTING_PRODUCT if role.startswith("listing_") else HDF5_PRODUCT
    return store._receipt_id(  # noqa: SLF001 - deterministic schema-3 identity
        provider=PROVIDER,
        product=product,
        fetch_id=f"{job_id}:{role}",
    )


def _optional_receipt(
    store: RawArtifactStore, job_id: str, role: str
) -> tuple[RawArtifactReceipt | None, bytes | None]:
    loaded = store.load_receipt_bytes(_receipt_id(store, job_id, role))
    return (None, None) if loaded is None else loaded


def _validate_listing_receipt(
    receipt: RawArtifactReceipt, *, job_id: str, role: str
) -> str:
    provenance = receipt.provenance
    prefix = provenance.get("prefix")
    if (
        receipt.provider != PROVIDER
        or receipt.product != LISTING_PRODUCT
        or receipt.fetch_id != f"{job_id}:{role}"
        or receipt.content_type not in _ALLOWED_LIST_CONTENT_TYPES
        or provenance.get("bucket") != BUCKET_NAME
        or provenance.get("operation") != "ListObjectsV2"
        or provenance.get("role") != role
        or provenance.get("list_type") != 2
        or provenance.get("max_keys") != LIST_MAX_KEYS
        or provenance.get("encoding_type") != "url"
        or not isinstance(prefix, str)
        or _PREFIX_RE.fullmatch(prefix) is None
    ):
        raise MetOfficeNativeCorruptionError("duurzame Met Office listingreceipt wijkt af")
    return prefix


def _validate_object_receipt(
    receipt: RawArtifactReceipt, *, job_id: str, selected: MetOfficeObject
) -> None:
    provenance = receipt.provenance
    if (
        receipt.provider != PROVIDER
        or receipt.product != HDF5_PRODUCT
        or receipt.fetch_id != f"{job_id}:odim_hdf5"
        or receipt.size_bytes != selected.size_bytes
        or receipt.content_type not in _ALLOWED_OBJECT_CONTENT_TYPES
        or provenance.get("bucket") != BUCKET_NAME
        or provenance.get("operation") != "GetObject"
        or provenance.get("upstream_key") != selected.key
        or provenance.get("listing_etag") != selected.etag
        or provenance.get("listing_last_modified") != selected.last_modified
        or provenance.get("listing_size_bytes") != selected.size_bytes
        or provenance.get("listing_storage_class") != selected.storage_class
        or provenance.get("listing_checksum_algorithm") != selected.checksum_algorithm
        or provenance.get("listing_checksum_type") != selected.checksum_type
        or provenance.get("filename_product_timestamp") != selected.product_timestamp
    ):
        raise MetOfficeNativeCorruptionError("duurzame Met Office HDF5-receipt wijkt af")


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
    receipts: dict[str, RawArtifactReceipt],
    selected: MetOfficeObject,
) -> None:
    members = {member.role: member for member in capture.members}
    if (
        capture.raw_store_schema_version != RAW_ARTIFACT_SCHEMA_VERSION
        or capture.provider != PROVIDER
        or capture.product != CAPTURE_PRODUCT
        or capture.product_timestamp != selected.product_timestamp
        or set(members) != set(receipts)
        or any(members[role].receipt_id != receipt.receipt_id for role, receipt in receipts.items())
    ):
        raise MetOfficeNativeCorruptionError("Met Office complete capture wijkt af")


def _capture_result(
    capture: RawArtifactCaptureReceipt,
    *,
    receipts: dict[str, RawArtifactReceipt],
    selected: MetOfficeObject,
    identity: MetOfficeOdimIdentity,
) -> dict[str, object]:
    return {
        "method": "met_office_native_capture_v1",
        "capture_state": "captured_not_decoded",
        "weather_state": "unknown",
        "decode_status": "not_decoded",
        "quantitative_semantics_eligible": False,
        "operational_admission_eligible": False,
        "admission_status": ADMISSION_STATUS,
        "raw_provenance": {
            "raw_store_schema_version": RAW_ARTIFACT_SCHEMA_VERSION,
            "raw_capture_id": capture.capture_id,
            "raw_capture_manifest_sha256": capture.manifest_sha256,
            "raw_capture_receipt_sha256": capture.capture_receipt_sha256,
            "upstream_capture_id": capture.upstream_capture_id,
            "provenance_grade": "exact_raw",
            "provenance_scope": "two_s3_listing_xml_and_selected_odim_hdf5_bytes",
            "selected_key": selected.key,
            "selected_etag": selected.etag,
            "selected_product_timestamp": selected.product_timestamp,
            "sources": {
                role: _source_reference(receipt, role=role)
                for role, receipt in sorted(receipts.items())
            },
            "structure": {
                "inspector_version": STRUCTURE_INSPECTOR_VERSION,
                "conventions": ODIM_CONVENTIONS,
                "h5rad_version": ODIM_VERSION,
                "quantity": identity.quantity,
                "rows": identity.rows,
                "columns": identity.columns,
                "xscale_metres": identity.xscale_metres,
                "yscale_metres": identity.yscale_metres,
                "projection_sha256": identity.projection_sha256,
                "dtype": identity.dtype,
                "chunk_rows": identity.chunk_rows,
                "chunk_columns": identity.chunk_columns,
                "compression": identity.compression,
                "compression_level": identity.compression_level,
                "chunk_count": identity.chunk_count,
                "stored_raster_bytes": identity.stored_raster_bytes,
                "input_window_start": identity.input_window_start,
                "input_window_end": identity.input_window_end,
                "source_wmo_count": len(identity.source_wmo_ids),
                "source_node_count": len(identity.source_nodes),
                "source_wmo_sha256": identity.source_wmo_sha256,
                "source_nodes_sha256": identity.source_nodes_sha256,
                "software": identity.software,
                "software_version": identity.software_version,
                "task": identity.task,
                "zr_a": identity.zr_a,
                "zr_b": identity.zr_b,
                "pixel_values_read": False,
                "input_window_is_uniform_observation_time": False,
            },
        },
    }


def capture_latest_met_office_odim(
    job,
    *,
    raw_store: RawArtifactStore | None = None,
    now: datetime | None = None,
    deadline_at: float | None = None,
) -> dict[str, object]:
    """Capture two exact listings plus selected ODIM bytes, without decoding."""
    job_id = _job_id(getattr(job, "job_id", None))
    _empty_payload(getattr(job, "payload", None))
    monotonic_now = time.monotonic()
    if deadline_at is None:
        deadline = monotonic_now + CAPTURE_TOTAL_DEADLINE_SECONDS
    else:
        if (
            isinstance(deadline_at, bool)
            or not isinstance(deadline_at, (int, float))
            or not math.isfinite(float(deadline_at))
        ):
            raise ValueError("Met Office deadline_at is ongeldig")
        deadline = min(
            float(deadline_at),
            monotonic_now + CAPTURE_TOTAL_DEADLINE_SECONDS,
        )
    store = _raw_store(raw_store)
    roles = ("listing_previous", "listing_current")

    with _capture_deadline(deadline), _CAPTURE_LOCK:
        _remaining()
        capture_id = store._capture_id(  # noqa: SLF001 - deterministic store contract
            provider=PROVIDER,
            product=CAPTURE_PRODUCT,
            fetch_id=f"{job_id}:capture",
        )
        complete = store.load_capture_bundle_bytes(capture_id)
        if complete is not None:
            capture, receipts, payloads = complete
            if set(receipts) != {*roles, "odim_hdf5"}:
                raise MetOfficeNativeCorruptionError("Met Office capture mist rollen")
            prefixes = {
                role: _validate_listing_receipt(receipts[role], job_id=job_id, role=role)
                for role in roles
            }
            groups = tuple(
                _parse_listing(payloads[role], expected_prefix=prefixes[role])
                for role in roles
            )
            selected = _select_latest(groups)
            _validate_object_receipt(receipts["odim_hdf5"], job_id=job_id, selected=selected)
            identity = _inspect_odim(payloads["odim_hdf5"], selected=selected)
            _validate_capture(capture, receipts=receipts, selected=selected)
            _remaining()
            return _capture_result(
                capture, receipts=receipts, selected=selected, identity=identity
            )

        loaded = {
            role: _optional_receipt(store, job_id, role)
            for role in (*roles, "odim_hdf5")
        }
        if loaded["listing_current"][0] is not None and loaded["listing_previous"][0] is None:
            raise MetOfficeNativeCorruptionError(
                "Met Office current-listing bestaat zonder previous-listing"
            )
        if loaded["listing_previous"][0] is not None:
            previous_prefix = _validate_listing_receipt(
                loaded["listing_previous"][0], job_id=job_id, role="listing_previous"
            )
            prefixes = (previous_prefix, _adjacent_prefix(previous_prefix))
        else:
            prefixes = _listing_prefixes(now)

        receipts: dict[str, RawArtifactReceipt] = {}
        payloads: dict[str, bytes] = {}
        for role, prefix in zip(roles, prefixes, strict=True):
            _remaining()
            receipt, payload = loaded[role]
            if receipt is None:
                response = _fetch_listing(prefix)
                _remaining()
                receipt = store.store(
                    provider=PROVIDER,
                    product=LISTING_PRODUCT,
                    fetch_id=f"{job_id}:{role}",
                    content_type=response.content_type,
                    received_at=response.received_at,
                    upstream_id=f"s3-list-v2:{BUCKET_NAME}:{prefix}:max-{LIST_MAX_KEYS}",
                    provenance=_listing_provenance(role, prefix, response),
                    payload=response.payload,
                )
                payload = response.payload
            else:
                stored_prefix = _validate_listing_receipt(
                    receipt, job_id=job_id, role=role
                )
                if stored_prefix != prefix or payload is None:
                    raise MetOfficeNativeCorruptionError(
                        "Met Office durable listingcontext verschilt"
                    )
            receipts[role] = receipt
            payloads[role] = payload
            _remaining()

        # Both exact XML documents are durable before selection/parsing.
        groups = tuple(
            _parse_listing(payloads[role], expected_prefix=prefix)
            for role, prefix in zip(roles, prefixes, strict=True)
        )
        selected = _select_latest(groups)
        _remaining()

        object_receipt, object_payload = loaded["odim_hdf5"]
        if object_receipt is None:
            response = _fetch_object(selected)
            _remaining()
            object_receipt = store.store(
                provider=PROVIDER,
                product=HDF5_PRODUCT,
                fetch_id=f"{job_id}:odim_hdf5",
                content_type=response.content_type,
                received_at=response.received_at,
                upstream_id=f"s3:{BUCKET_NAME}:{selected.key}:etag:{selected.etag}",
                provenance=_object_provenance(selected, response),
                payload=response.payload,
            )
            object_payload = response.payload
        else:
            _validate_object_receipt(
                object_receipt, job_id=job_id, selected=selected
            )
            if object_payload is None:
                raise MetOfficeNativeCorruptionError("Met Office HDF5-bytes ontbreken")
        receipts["odim_hdf5"] = object_receipt
        payloads["odim_hdf5"] = object_payload

        # Exact bytes are durable before the first HDF5 metadata inspection.
        identity = _inspect_odim(object_payload, selected=selected)
        _remaining()
        capture = store.commit_capture(
            provider=PROVIDER,
            product=CAPTURE_PRODUCT,
            fetch_id=f"{job_id}:capture",
            upstream_capture_id=(
                f"{CAPTURE_PRODUCT}|key={selected.key}|etag={selected.etag}"
                f"|previous={receipts['listing_previous'].content_sha256}"
                f"|current={receipts['listing_current'].content_sha256}"
                f"|hdf5={object_receipt.content_sha256}"
            ),
            product_timestamp=identity.product_timestamp,
            members={role: receipt.receipt_id for role, receipt in receipts.items()},
        )
        _validate_capture(capture, receipts=receipts, selected=selected)
        _remaining()
        return _capture_result(
            capture, receipts=receipts, selected=selected, identity=identity
        )


__all__ = [
    "ADMISSION_STATUS",
    "CAPTURE_TOTAL_DEADLINE_SECONDS",
    "CAPTURE_PRODUCT",
    "HDF5_PRODUCT",
    "LISTING_PRODUCT",
    "MetOfficeNativeCorruptionError",
    "MetOfficeNativeDeadlineError",
    "MetOfficeNativeError",
    "MetOfficeObject",
    "MetOfficeOdimIdentity",
    "S3Response",
    "SOCKET_OPERATION_TIMEOUT_SECONDS",
    "capture_latest_met_office_odim",
]
