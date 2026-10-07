"""Exact native capture for KNMI's real-time five-minute QPE product.

The Open Data API control-plane responses select a file and issue a temporary
download URL.  They are deliberately not weather artifacts: the URL contains
temporary credentials and must never be persisted.  Only the exact upstream
HDF5 payload is retained, before any HDF5 inspection takes place.

This module performs structural product identification only.  It emits no
cells, rain/no-rain fact, quality score or operational radar evidence.  A
separate, certified decoder and admission gate remain mandatory.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import errno
import hashlib
import ipaddress
import io
import json
import math
import os
from pathlib import Path
import re
import stat
import tempfile
import threading
import time
from typing import Any, Iterator, Mapping
from urllib.parse import quote, urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener, urlopen

import h5py

from .raw_artifacts import (
    NATIVE_CAPTURE_STAGE,
    NATIVE_REPRESENTATION,
    RawArtifactCaptureReceipt,
    RawArtifactReceipt,
    RawArtifactStore,
)
KNMI_OPEN_DATA_BASE = "https://api.dataplatform.knmi.nl/open-data/v1"
KNMI_DATASET_NAME = "nl_rdr_data_rtcor_5m"
KNMI_DATASET_VERSION = "1.0"
KNMI_DATASET_LICENSE = "CC-BY-4.0"
KNMI_LIST_QUERY = {
    "maxKeys": 1,
    "orderBy": "created",
    "sorting": "desc",
}

MAX_CONTROL_JSON_BYTES = 256 * 1024
MAX_HDF5_BYTES = 40 * 1024 * 1024
MAX_TEMPORARY_URL_CHARS = 8192
MAX_FILENAME_CHARS = 255
HTTP_IO_TIMEOUT_SECONDS = 20.0
JOB_TOTAL_DEADLINE_SECONDS = 75.0
STREAM_CHUNK_BYTES = 64 * 1024
LOCK_POLL_SECONDS = 0.05
MAX_CONTROL_JSON_DEPTH = 16
MAX_CONTROL_JSON_OBJECTS = 2_048
MAX_VALIDATION_VERDICT_BYTES = 8 * 1024
MAX_VALIDATION_VERDICTS = 1_024
KNMI_GRID_COLUMNS = 700
KNMI_GRID_ROWS = 765
KNMI_SUPPORTED_HDF_TAG_VERSIONS = frozenset({"3.6"})
KNMI_HTTP_USER_AGENT = "StormTrackerWorker/knmi-native-candidate-v1"

_ALLOWED_DOWNLOAD_HOST_SUFFIXES = (
    ".knmi.nl",
    ".amazonaws.com",
    ".googleapis.com",
    ".googleusercontent.com",
    ".cloudfront.net",
    ".blob.core.windows.net",
)

KNMI_RAW_SHARE_ROOT = Path("/share/storm_tracker_worker")
KNMI_RAW_ARTIFACT_ROOT = KNMI_RAW_SHARE_ROOT / "raw" / "knmi_rtcor_5m"
KNMI_HDF5_PRODUCT = "knmi_nl_rdr_data_rtcor_5m_hdf5"
KNMI_CAPTURE_PRODUCT = "knmi_nl_rdr_data_rtcor_5m_capture"
KNMI_RAW_COLLECTOR = "knmi_open_data_native_file"
KNMI_RAW_COLLECTOR_VERSION = "worker-knmi-rtcor-5m-exact-raw-v1"
KNMI_STRUCTURE_INSPECTOR_VERSION = "knmi-rtcor-live-golden-identity-v2"
KNMI_SEMANTIC_CLASS = "quantitative_precipitation_accumulation"
KNMI_QUANTITY = "precipitation_accumulation"
KNMI_UNIT = "mm"
KNMI_ACCUMULATION_MINUTES = 5.0
KNMI_ADMISSION_STATUS = "exact_raw_decoder_not_certified"

_FILENAME_RE = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9._-]{0,250}\.(?:h5|hdf|hdf5)$",
)
_RTCOR_FILENAME_RE = re.compile(
    r"^RAD_NL25_(?P<kind>RAC)_RT_(?P<timestamp>\d{12})\.h5$",
)
_HDF_TIMESTAMP_RE = re.compile(
    r"^(?P<day>\d{2})-(?P<month>[A-Za-z]{3})-(?P<year>\d{4});"
    r"(?P<hour>\d{2}):(?P<minute>\d{2}):(?P<second>\d{2})"
    r"(?:\.(?P<fraction>\d{1,6}))?\.?$"
)
_MONTHS = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}
KNMI_INTERNAL_PRODUCT_NAME = "RAD_NL25_COR_NA"
KNMI_PRIMARY_IMAGE_PARAMETER = "PRECIP_[MM]"
_KNMI_RAW_STORE: RawArtifactStore | None = None
_KNMI_RAW_STORE_LOCK = threading.Lock()
_KNMI_CAPTURE_LOCK = threading.Lock()
_ACTIVE_DEADLINE = threading.local()
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class KnmiNativeTransientError(ValueError):
    """Retryable local/transport failure that must never become a verdict."""


@dataclass(frozen=True, slots=True)
class _HttpBytes:
    """One exact HTTP response body and non-secret transport facts."""

    payload: bytes
    content_type: str
    received_at: float


@dataclass(frozen=True, slots=True)
class KnmiHdfIdentity:
    """Small structural identity extracted without reading raster values."""

    product_group_name: str
    image_product_name: str
    image_geo_parameter: str
    hdf_tag_version: str
    product_start: float
    product_end: float
    image_rows: int
    image_columns: int


@dataclass(frozen=True, slots=True)
class KnmiFileSummary:
    """Strict, non-secret FileSummary from the official KNMI OpenAPI schema."""

    filename: str
    last_modified: str
    last_modified_timestamp: float
    size_bytes: int
    created: str
    created_timestamp: float


@dataclass(frozen=True, slots=True)
class KnmiDownloadDescriptor:
    """Strict, non-secret fields accompanying a temporary object URL."""

    content_type: str
    size_bytes: int
    last_modified: str
    last_modified_timestamp: float
    temporary_download_url: str


class _RejectRedirects(HTTPRedirectHandler):
    """Never follow redirects carrying API keys or presigned credentials."""

    def redirect_request(self, *_args, **_kwargs):
        raise ValueError("KNMI HTTP-redirect is niet toegestaan")


def _open_without_redirects(request: Request, *, timeout: float):
    return build_opener(_RejectRedirects()).open(request, timeout=timeout)


def _remaining_seconds(deadline: float) -> float:
    remaining = float(deadline) - time.monotonic()
    if not math.isfinite(remaining) or remaining <= 0:
        raise TimeoutError("KNMI totale jobdeadline is verstreken")
    return remaining


def _active_deadline(default_seconds: float = JOB_TOTAL_DEADLINE_SECONDS) -> float:
    value = getattr(_ACTIVE_DEADLINE, "value", None)
    if value is None:
        return time.monotonic() + default_seconds
    _remaining_seconds(value)
    return float(value)


@contextmanager
def _deadline_scope(deadline: float) -> Iterator[None]:
    previous = getattr(_ACTIVE_DEADLINE, "value", None)
    _ACTIVE_DEADLINE.value = deadline
    try:
        yield
    finally:
        if previous is None:
            try:
                del _ACTIVE_DEADLINE.value
            except AttributeError:  # pragma: no cover - defensive cleanup
                pass
        else:
            _ACTIVE_DEADLINE.value = previous


@contextmanager
def _deadline_thread_lock(lock: threading.Lock, *, deadline: float) -> Iterator[None]:
    acquired = lock.acquire(timeout=_remaining_seconds(deadline))
    if not acquired:
        raise TimeoutError("KNMI capturelock overschrijdt de jobdeadline")
    try:
        _remaining_seconds(deadline)
        yield
    finally:
        lock.release()


def _safe_auxiliary_directory(store: RawArtifactStore, name: str) -> Path:
    root = Path(store.root)
    if root.is_symlink() or not root.is_dir():
        raise ValueError("KNMI auxiliary root is onveilig")
    directory = root / name
    existed = directory.exists()
    if existed and (directory.is_symlink() or not directory.is_dir()):
        raise ValueError("KNMI auxiliary map is onveilig")
    directory.mkdir(mode=0o700, exist_ok=True)
    if directory.is_symlink() or directory.resolve().parent != root.resolve():
        raise ValueError("KNMI auxiliary map ontsnapt uit raw root")
    if not existed and os.name != "nt":
        descriptor = os.open(root, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    return directory


def _slot_lock_path(store: RawArtifactStore, job_id: str) -> Path:
    token = hashlib.sha256(job_id.encode("utf-8", errors="strict")).hexdigest()
    return _safe_auxiliary_directory(store, "knmi_locks") / f"slot-{token}.lock"


def _capture_capacity_lock_path(store: RawArtifactStore) -> Path:
    return _safe_auxiliary_directory(store, "knmi_locks") / "capture-capacity.lock"


@contextmanager
def _deadline_file_lock(path: Path, *, deadline: float) -> Iterator[None]:
    _remaining_seconds(deadline)
    if path.exists() and (path.is_symlink() or not path.is_file()):
        raise ValueError("KNMI lockpad is onveilig")
    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_BINARY", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags, 0o600)
    acquired = False
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode):
            raise ValueError("KNMI lockpad is geen regulier bestand")
        if info.st_size == 0:
            os.write(descriptor, b"\x00")
            os.fsync(descriptor)
        os.lseek(descriptor, 0, os.SEEK_SET)
        while not acquired:
            try:
                if os.name == "nt":  # pragma: no cover - platform selected
                    import msvcrt

                    msvcrt.locking(descriptor, msvcrt.LK_NBLCK, 1)
                else:  # pragma: no branch - platform selected
                    import fcntl

                    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                acquired = True
            except OSError as err:
                if err.errno not in {
                    errno.EACCES,
                    errno.EAGAIN,
                    errno.EDEADLK,
                    errno.EWOULDBLOCK,
                }:
                    raise
                time.sleep(min(LOCK_POLL_SECONDS, _remaining_seconds(deadline)))
        _remaining_seconds(deadline)
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


def _validate_json_resources(value: Any) -> None:
    pending: list[tuple[Any, int]] = [(value, 0)]
    objects = 0
    while pending:
        current, depth = pending.pop()
        objects += 1
        if objects > MAX_CONTROL_JSON_OBJECTS:
            raise ValueError("KNMI-controleresponse bevat te veel JSON-objecten")
        if depth > MAX_CONTROL_JSON_DEPTH:
            raise ValueError("KNMI-controleresponse is te diep genest")
        if isinstance(current, dict):
            pending.extend((item, depth + 1) for item in current.values())
        elif isinstance(current, list):
            pending.extend((item, depth + 1) for item in current)


def _json_without_duplicates(payload: bytes) -> Any:
    """Decode bounded UTF-8 JSON while rejecting ambiguous object keys."""

    if not isinstance(payload, bytes) or not 1 <= len(payload) <= MAX_CONTROL_JSON_BYTES:
        raise ValueError("KNMI-controleresponse overschrijdt de vaste JSON-grens")
    try:
        text = payload.decode("utf-8", errors="strict")
    except UnicodeDecodeError as err:
        raise ValueError("KNMI-controleresponse is geen geldige UTF-8") from err

    def object_pairs(pairs):
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("KNMI-controleresponse bevat dubbele JSON-sleutels")
            result[key] = value
        return result

    def invalid_constant(_value: str):
        raise ValueError("KNMI-controleresponse bevat niet-eindige JSON-getallen")

    try:
        decoded = json.loads(
            text,
            object_pairs_hook=object_pairs,
            parse_constant=invalid_constant,
        )
    except json.JSONDecodeError as err:
        raise ValueError("KNMI-controleresponse bevat ongeldige JSON") from err
    _validate_json_resources(decoded)
    return decoded


def _content_type(headers: Any) -> str:
    value = str(headers.get("Content-Type") or "") if headers is not None else ""
    if len(value) > 256 or any(ord(character) < 0x20 for character in value):
        raise ValueError("KNMI Content-Type-header is ongeldig")
    return value.split(";", 1)[0].strip().lower()


def _read_response(
    request: Request,
    *,
    maximum_bytes: int,
    timeout: float,
    opener=None,
    expected_url: str | None = None,
) -> _HttpBytes:
    """Read one identity-encoded response under a hard byte contract."""

    deadline = _active_deadline(max(float(timeout), 0.001))
    active_opener = urlopen if opener is None else opener
    with active_opener(
        request,
        timeout=min(float(timeout), _remaining_seconds(deadline)),
    ) as response:
        status = getattr(response, "status", 200)
        if isinstance(status, bool) or status != 200:
            raise ValueError("KNMI-response heeft geen HTTP 200-status")
        geturl = getattr(response, "geturl", None)
        if expected_url is not None and callable(geturl):
            if str(geturl()) != expected_url:
                raise ValueError("KNMI-response kwam van een onverwacht endpoint")
        headers = getattr(response, "headers", None)
        encoding = (
            str(headers.get("Content-Encoding") or "identity")
            if headers is not None
            else "identity"
        )
        if len(encoding) > 64 or any(ord(character) < 0x20 for character in encoding):
            raise ValueError("KNMI Content-Encoding-header is ongeldig")
        if encoding.lower() != "identity":
            raise ValueError("onverwachte KNMI-transportcompressie")
        declared_raw = headers.get("Content-Length") if headers is not None else None
        declared: int | None = None
        if declared_raw is not None:
            declared_text = str(declared_raw)
            if not declared_text.isascii() or not declared_text.isdecimal():
                raise ValueError("KNMI Content-Length is ongeldig")
            declared = int(declared_text)
            if declared < 0 or declared > maximum_bytes:
                raise ValueError("payload overschrijdt contractlimiet")
        chunks: list[bytes] = []
        total = 0
        while total <= maximum_bytes:
            _remaining_seconds(deadline)
            requested = min(STREAM_CHUNK_BYTES, maximum_bytes + 1 - total)
            chunk = response.read(requested)
            _remaining_seconds(deadline)
            if not chunk:
                break
            if not isinstance(chunk, (bytes, bytearray, memoryview)):
                raise TypeError("KNMI-responsstream moet bytes opleveren")
            immutable = bytes(chunk)
            if len(immutable) > requested:
                raise ValueError("KNMI-responsstream overschrijdt de chunkgrens")
            total += len(immutable)
            if total > maximum_bytes:
                raise ValueError("payload overschrijdt contractlimiet")
            if declared is not None and total > declared:
                raise ValueError("KNMI-response is groter dan Content-Length")
            chunks.append(immutable)
        payload = b"".join(chunks)
        if declared is not None and len(payload) != declared:
            raise ValueError("KNMI-response wijkt af van Content-Length")
        received_at = time.time()
        content_type = _content_type(headers)
        return _HttpBytes(
            payload=payload,
            content_type=content_type,
            received_at=received_at,
        )


def _api_json(url: str, *, api_key: str) -> Mapping[str, Any]:
    """Fetch one authenticated KNMI control-plane document."""

    if not url.startswith(f"{KNMI_OPEN_DATA_BASE}/"):
        raise ValueError("KNMI API-pad valt buiten het vaste Open Data-endpoint")
    request = Request(
        url,
        headers={
            "Accept": "application/json",
            "Accept-Encoding": "identity",
            "User-Agent": KNMI_HTTP_USER_AGENT,
        },
    )
    # urllib copies regular headers when following redirects.  Keep the key in
    # the explicitly unredirected header set *and* use a no-redirect opener.
    request.add_unredirected_header("Authorization", api_key)
    response = _read_response(
        request,
        maximum_bytes=MAX_CONTROL_JSON_BYTES,
        timeout=20,
        opener=_open_without_redirects,
        expected_url=url,
    )
    if not (
        response.content_type == "application/json"
        or response.content_type.endswith("+json")
    ):
        raise ValueError("KNMI-controleresponse heeft geen JSON-Content-Type")
    decoded = _json_without_duplicates(response.payload)
    if not isinstance(decoded, dict):
        raise ValueError("KNMI-controleresponse moet een JSON-object zijn")
    if "error" in decoded:
        raise ValueError("KNMI Open Data API retourneerde een fout")
    return decoded


def _safe_filename(value: Any) -> str:
    if not isinstance(value, str):
        raise ValueError("KNMI-bestandslijst mist een tekstuele bestandsnaam")
    if (
        len(value) > MAX_FILENAME_CHARS
        or ".." in value
        or not _FILENAME_RE.fullmatch(value)
    ):
        raise ValueError("KNMI-bestandslijst bevat een onveilige HDF5-bestandsnaam")
    return value


def _exact_object_keys(
    value: Any,
    *,
    required: frozenset[str],
    context: str,
) -> Mapping[str, Any]:
    if not isinstance(value, dict) or frozenset(value) != required:
        raise ValueError(f"{context} wijkt af van het vaste KNMI OpenAPI-schema")
    return value


def _bounded_text(value: Any, *, field: str, maximum: int = 512) -> str:
    if not isinstance(value, str) or not value or len(value) > maximum:
        raise ValueError(f"KNMI OpenAPI-veld {field} is geen begrensde tekst")
    if any(ord(character) < 0x20 for character in value):
        raise ValueError(f"KNMI OpenAPI-veld {field} bevat controletekens")
    return value


def _rfc3339_timestamp(value: Any, *, field: str) -> tuple[str, float]:
    text = _bounded_text(value, field=field, maximum=64)
    candidate = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        moment = datetime.fromisoformat(candidate)
    except ValueError as err:
        raise ValueError(f"KNMI OpenAPI-veld {field} is geen ISO8601-tijd") from err
    if moment.tzinfo is None or moment.utcoffset() is None:
        raise ValueError(f"KNMI OpenAPI-veld {field} mist een tijdzone")
    timestamp = moment.timestamp()
    if not math.isfinite(timestamp):
        raise ValueError(f"KNMI OpenAPI-veld {field} bevat geen eindige tijd")
    return text, timestamp


def _positive_size(
    value: Any,
    *,
    field: str,
    encoded_as_string: bool,
) -> int:
    if encoded_as_string:
        if (
            not isinstance(value, str)
            or not value.isascii()
            or not value.isdecimal()
        ):
            raise ValueError(f"KNMI OpenAPI-veld {field} is geen bestandsgrootte")
        result = int(value)
    elif isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"KNMI OpenAPI-veld {field} is geen bestandsgrootte")
    else:
        result = value
    if result <= 0 or result > MAX_HDF5_BYTES:
        raise ValueError(f"KNMI OpenAPI-veld {field} valt buiten de HDF5-grens")
    return result


def _declared_content_type(value: Any) -> str:
    text = _bounded_text(value, field="contentType", maximum=256).lower()
    if ";" in text or not re.fullmatch(r"[a-z0-9!#$&^_.+-]+/[a-z0-9!#$&^_.+-]+", text):
        raise ValueError("KNMI OpenAPI-veld contentType is ongeldig")
    return text


def _latest_filename(*, api_key: str) -> KnmiFileSummary:
    query = urlencode(KNMI_LIST_QUERY)
    root = (
        f"{KNMI_OPEN_DATA_BASE}/datasets/{KNMI_DATASET_NAME}"
        f"/versions/{KNMI_DATASET_VERSION}/files?{query}"
    )
    response = _api_json(root, api_key=api_key)
    listing = _exact_object_keys(
        response,
        required=frozenset({
            "isTruncated",
            "files",
            "resultCount",
            "maxResults",
            "startAfterFilename",
            "nextPageToken",
        }),
        context="KNMI-bestandslijst",
    )
    if not isinstance(listing["isTruncated"], bool):
        raise ValueError("KNMI-bestandslijst heeft een ongeldige truncatievlag")
    for key in ("resultCount", "maxResults"):
        value = listing[key]
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError(f"KNMI-bestandslijst heeft een ongeldig veld {key}")
    if listing["resultCount"] != 1 or listing["maxResults"] != 1:
        raise ValueError("KNMI-bestandslijst bevestigt niet exact één resultaat")
    for key in ("startAfterFilename", "nextPageToken"):
        if not isinstance(listing[key], str) or len(listing[key]) > 8192:
            raise ValueError(f"KNMI-bestandslijst heeft een ongeldig veld {key}")
    files = listing["files"]
    if not isinstance(files, list) or len(files) != 1:
        raise ValueError("KNMI-bestandslijst moet precies één nieuwste bestand bevatten")
    record = _exact_object_keys(
        files[0],
        required=frozenset({"filename", "lastModified", "size", "created"}),
        context="KNMI-bestandsrecord",
    )
    last_modified, last_modified_timestamp = _rfc3339_timestamp(
        record["lastModified"], field="lastModified"
    )
    created, created_timestamp = _rfc3339_timestamp(
        record["created"], field="created"
    )
    return KnmiFileSummary(
        filename=_safe_filename(record["filename"]),
        last_modified=last_modified,
        last_modified_timestamp=last_modified_timestamp,
        size_bytes=_positive_size(
            record["size"], field="size", encoded_as_string=False
        ),
        created=created,
        created_timestamp=created_timestamp,
    )


def _validated_download_url(value: str) -> str:
    """Accept only public HTTPS object-store hosts used by KNMI delivery."""

    if not isinstance(value, str) or not value:
        raise ValueError("KNMI URL-response mist een tijdelijke downloadlink")
    if len(value) > MAX_TEMPORARY_URL_CHARS:
        raise ValueError("KNMI tijdelijke downloadlink overschrijdt de lengtegrens")
    if not value.isascii() or any(
        ord(character) <= 0x20 or ord(character) == 0x7F
        for character in value
    ):
        raise ValueError("KNMI tijdelijke downloadlink is ongeldig")
    parsed = urlsplit(value)
    try:
        port = parsed.port
    except ValueError as err:
        raise ValueError("KNMI tijdelijke downloadlink is ongeldig") from err
    hostname = (parsed.hostname or "").lower()
    try:
        ipaddress.ip_address(hostname)
    except ValueError:
        is_ip_literal = False
    else:
        is_ip_literal = True
    allowed_host = any(
        hostname == suffix[1:] or hostname.endswith(suffix)
        for suffix in _ALLOWED_DOWNLOAD_HOST_SUFFIXES
    )
    if (
        parsed.scheme.lower() != "https"
        or not hostname
        or hostname.endswith(".")
        or is_ip_literal
        or not allowed_host
        or port not in {None, 443}
        or parsed.username is not None
        or parsed.password is not None
        or not parsed.path.startswith("/")
        or bool(parsed.fragment)
    ):
        raise ValueError("KNMI tijdelijke downloadlink is ongeldig")
    return value


def _temporary_download_url(
    summary: KnmiFileSummary,
    *,
    api_key: str,
) -> KnmiDownloadDescriptor:
    if not isinstance(summary, KnmiFileSummary):
        raise TypeError("KNMI-download vereist een gevalideerd bestandsrecord")
    safe_filename = _safe_filename(summary.filename)
    endpoint = (
        f"{KNMI_OPEN_DATA_BASE}/datasets/{KNMI_DATASET_NAME}"
        f"/versions/{KNMI_DATASET_VERSION}/files/"
        f"{quote(safe_filename, safe='')}/url"
    )
    response = _api_json(endpoint, api_key=api_key)
    record = _exact_object_keys(
        response,
        required=frozenset({
            "contentType",
            "size",
            "lastModified",
            "temporaryDownloadUrl",
        }),
        context="KNMI-downloadbeschrijving",
    )
    size_bytes = _positive_size(
        record["size"], field="size", encoded_as_string=True
    )
    last_modified, last_modified_timestamp = _rfc3339_timestamp(
        record["lastModified"], field="lastModified"
    )
    if size_bytes != summary.size_bytes:
        raise ValueError("KNMI bestandsgrootte veranderde tussen lijst en download")
    if last_modified_timestamp != summary.last_modified_timestamp:
        raise ValueError("KNMI wijzigingstijd veranderde tussen lijst en download")
    return KnmiDownloadDescriptor(
        content_type=_declared_content_type(record["contentType"]),
        size_bytes=size_bytes,
        last_modified=last_modified,
        last_modified_timestamp=last_modified_timestamp,
        temporary_download_url=_validated_download_url(record["temporaryDownloadUrl"]),
    )


def _download_hdf5(descriptor: KnmiDownloadDescriptor) -> _HttpBytes:
    """Download a presigned object without forwarding KNMI Authorization."""

    if not isinstance(descriptor, KnmiDownloadDescriptor):
        raise TypeError("KNMI-download vereist een gevalideerde downloadbeschrijving")
    validated_url = _validated_download_url(descriptor.temporary_download_url)
    try:
        response = _read_response(
            Request(
                validated_url,
                headers={
                    "Accept": "application/octet-stream,application/x-hdf5",
                    "Accept-Encoding": "identity",
                    "User-Agent": KNMI_HTTP_USER_AGENT,
                },
            ),
            maximum_bytes=MAX_HDF5_BYTES,
            timeout=30,
            opener=_open_without_redirects,
            expected_url=validated_url,
        )
    except (TimeoutError, OSError):
        # Transport exceptions can embed the presigned URL. Preserve only a
        # typed retryable fact, never the URL or credential-bearing message.
        raise KnmiNativeTransientError("KNMI objectdownload mislukt (tijdelijk)") from None
    except Exception:
        # urllib/HTTP exceptions can embed the complete presigned URL.  Never
        # let temporary query credentials reach a persisted job error.
        raise ValueError("KNMI objectdownload mislukt") from None
    if len(response.payload) != descriptor.size_bytes:
        raise ValueError("KNMI-downloadgrootte wijkt af van de OpenAPI-beschrijving")
    if response.content_type != descriptor.content_type:
        raise ValueError("KNMI-download Content-Type wijkt af van de OpenAPI-beschrijving")
    return response


def _raw_store(injected: RawArtifactStore | None = None) -> RawArtifactStore:
    if injected is not None:
        injected.initialize()
        return injected
    share_root = KNMI_RAW_SHARE_ROOT.resolve(strict=False)
    raw_root = KNMI_RAW_ARTIFACT_ROOT.resolve(strict=False)
    if raw_root == share_root or share_root not in raw_root.parents:
        raise RuntimeError("KNMI raw-artifactpad ontsnapt uit worker-share")
    global _KNMI_RAW_STORE
    with _KNMI_RAW_STORE_LOCK:
        if _KNMI_RAW_STORE is None:
            candidate = RawArtifactStore(raw_root)
            candidate.initialize()
            _KNMI_RAW_STORE = candidate
        return _KNMI_RAW_STORE


def _native_provenance(
    summary: KnmiFileSummary,
    descriptor: KnmiDownloadDescriptor,
) -> dict[str, object]:
    """Describe exact source bytes without retaining URLs or credentials."""

    return {
        "representation": NATIVE_REPRESENTATION,
        "capture_stage": NATIVE_CAPTURE_STAGE,
        "collector": KNMI_RAW_COLLECTOR,
        "collector_version": KNMI_RAW_COLLECTOR_VERSION,
        "structure_inspector_version": KNMI_STRUCTURE_INSPECTOR_VERSION,
        "dataset_name": KNMI_DATASET_NAME,
        "dataset_version": KNMI_DATASET_VERSION,
        "dataset_license": KNMI_DATASET_LICENSE,
        "selection_order": "created_descending",
        "selection_limit": 1,
        "upstream_filename": _safe_filename(summary.filename),
        "listing_created": summary.created,
        "listing_last_modified": summary.last_modified,
        "listing_size_bytes": summary.size_bytes,
        "download_last_modified": descriptor.last_modified,
        "download_size_bytes": descriptor.size_bytes,
        "download_content_type": descriptor.content_type,
        "declared_semantic_class": KNMI_SEMANTIC_CLASS,
        "declared_quantity": KNMI_QUANTITY,
        "declared_unit": KNMI_UNIT,
        "declared_accumulation_minutes": KNMI_ACCUMULATION_MINUTES,
        "quantitative_semantics_verified_after_decode": False,
    }


def _scalar_text(attributes: Any, name: str) -> str:
    if name not in attributes:
        raise ValueError(f"KNMI HDF5 mist metadata-attribuut {name}")
    value = attributes[name]
    if hasattr(value, "item"):
        try:
            value = value.item()
        except ValueError as err:
            raise ValueError(f"KNMI HDF5-attribuut {name} is niet scalair") from err
    if isinstance(value, bytes):
        try:
            value = value.decode("ascii", errors="strict")
        except UnicodeDecodeError as err:
            raise ValueError(f"KNMI HDF5-attribuut {name} is geen ASCII") from err
    if not isinstance(value, str):
        raise ValueError(f"KNMI HDF5-attribuut {name} is geen tekst")
    value = value.strip()
    if not value or len(value) > 512 or "\x00" in value:
        raise ValueError(f"KNMI HDF5-attribuut {name} is ongeldig")
    return value


def _positive_int(attributes: Any, name: str, *, maximum: int) -> int:
    if name not in attributes:
        raise ValueError(f"KNMI HDF5 mist metadata-attribuut {name}")
    value = attributes[name]
    if hasattr(value, "item"):
        value = value.item()
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"KNMI HDF5-attribuut {name} is niet numeriek")
    number = float(value)
    if not math.isfinite(number) or not number.is_integer():
        raise ValueError(f"KNMI HDF5-attribuut {name} is geen geheel getal")
    result = int(number)
    if result <= 0 or result > maximum:
        raise ValueError(f"KNMI HDF5-attribuut {name} valt buiten het rastercontract")
    return result


def _parse_hdf_timestamp(value: str) -> float:
    match = _HDF_TIMESTAMP_RE.fullmatch(value.strip())
    if match is None:
        raise ValueError("KNMI HDF5 bevat een onbekend tijdformaat")
    month = _MONTHS.get(match.group("month").upper())
    if month is None:
        raise ValueError("KNMI HDF5 bevat een onbekende maand")
    fraction = (match.group("fraction") or "").ljust(6, "0")
    try:
        moment = datetime(
            int(match.group("year")),
            month,
            int(match.group("day")),
            int(match.group("hour")),
            int(match.group("minute")),
            int(match.group("second")),
            int(fraction or "0"),
            tzinfo=timezone.utc,
        )
    except ValueError as err:
        raise ValueError("KNMI HDF5 bevat een ongeldige producttijd") from err
    return moment.timestamp()


def _validate_filename_identity(filename: str, identity: KnmiHdfIdentity) -> None:
    match = _RTCOR_FILENAME_RE.fullmatch(_safe_filename(filename))
    if match is None:
        raise ValueError("KNMI-bestandsnaam is niet het vaste NL25 RTCOR-product")
    expected_internal = {
        "RAC": KNMI_INTERNAL_PRODUCT_NAME,
    }[match.group("kind").upper()]
    if identity.product_group_name != expected_internal:
        raise ValueError("KNMI-bestandsnaam en HDF5-productidentiteit verschillen")
    try:
        filename_timestamp = datetime.strptime(
            match.group("timestamp"), "%Y%m%d%H%M"
        ).replace(tzinfo=timezone.utc).timestamp()
    except ValueError as err:
        raise ValueError("KNMI-bestandsnaam bevat een ongeldige producttijd") from err
    if filename_timestamp != identity.product_end:
        raise ValueError("KNMI-bestandsnaam en HDF5-producttijd verschillen")


def _hard_group(root: h5py.File, path: str) -> h5py.Group:
    link = root.get(path, getlink=True)
    if not isinstance(link, h5py.HardLink):
        raise ValueError(f"KNMI HDF5-pad {path} is geen intern hard link")
    value = root.get(path)
    if not isinstance(value, h5py.Group):
        raise ValueError(f"KNMI HDF5 mist de {path}-groep")
    return value


def _hard_dataset(root: h5py.File, path: str) -> h5py.Dataset:
    link = root.get(path, getlink=True)
    if not isinstance(link, h5py.HardLink):
        raise ValueError(f"KNMI HDF5-pad {path} is geen intern hard link")
    value = root.get(path)
    if not isinstance(value, h5py.Dataset):
        raise ValueError(f"KNMI HDF5 mist {path}")
    return value


def _validate_hdf_self_contained(
    root: h5py.File,
    *,
    deadline: float | None = None,
) -> None:
    """Reject link/resource tricks before any raster dataset can be admitted."""

    seen_groups: set[int] = set()
    object_count = 0

    def walk(group: h5py.Group, *, depth: int) -> None:
        nonlocal object_count
        if deadline is not None:
            _remaining_seconds(deadline)
        if depth > 16:
            raise ValueError("KNMI HDF5-groepsdiepte overschrijdt de grens")
        group_identity = hash(group.id)
        if group_identity in seen_groups:
            return
        seen_groups.add(group_identity)
        if len(group) > 1024:
            raise ValueError("KNMI HDF5-groep bevat te veel objecten")
        for name in group.keys():
            if deadline is not None:
                _remaining_seconds(deadline)
            object_count += 1
            if object_count > 4096:
                raise ValueError("KNMI HDF5 bevat te veel objecten")
            link = group.get(name, getlink=True)
            if not isinstance(link, h5py.HardLink):
                raise ValueError("KNMI HDF5 bevat een externe of symbolische link")
            value = group.get(name)
            if isinstance(value, h5py.Group):
                walk(value, depth=depth + 1)
            elif isinstance(value, h5py.Dataset):
                if bool(value.is_virtual) or bool(value.external):
                    raise ValueError("KNMI HDF5 bevat externe of virtuele datasetopslag")
            else:
                raise ValueError("KNMI HDF5 bevat een niet-ondersteund objecttype")

    walk(root, depth=0)


def _validate_auxiliary_image(
    root: h5py.File,
    *,
    group_path: str,
    parameter: str,
    deadline: float | None = None,
) -> None:
    if deadline is not None:
        _remaining_seconds(deadline)
    group = _hard_group(root, group_path)
    if _scalar_text(group.attrs, "image_product_name") != KNMI_INTERNAL_PRODUCT_NAME:
        raise ValueError(f"KNMI HDF5-{group_path} heeft een andere productidentiteit")
    if _scalar_text(group.attrs, "image_geo_parameter") != parameter:
        raise ValueError(f"KNMI HDF5-{group_path} heeft een andere quantity")
    dataset = _hard_dataset(root, f"{group_path}/image_data")
    if tuple(dataset.shape) != (KNMI_GRID_ROWS, KNMI_GRID_COLUMNS):
        raise ValueError(f"KNMI HDF5-{group_path} heeft een andere rastervorm")
    if dataset.dtype.kind != "u" or dataset.dtype.itemsize != 1:
        raise ValueError(f"KNMI HDF5-{group_path} heeft een andere dtype")
    if dataset.chunks is not None and (
        len(dataset.chunks) != 2
        or any(
            isinstance(value, bool)
            or not isinstance(value, int)
            or value <= 0
            or value > dimension
            for value, dimension in zip(dataset.chunks, dataset.shape)
        )
        or math.prod(dataset.chunks) * dataset.dtype.itemsize > 8 * 1024 * 1024
    ):
        raise ValueError(f"KNMI HDF5-{group_path} overschrijdt de chunkgrens")
    if dataset.compression not in {None, "gzip", "lzf", "szip"}:
        raise ValueError(f"KNMI HDF5-{group_path} gebruikt een onbekend filter")


def _inspect_hdf_identity(
    payload: bytes,
    *,
    deadline: float | None = None,
) -> KnmiHdfIdentity:
    """Validate the KNMI HDF container and identity, never raster values."""

    if deadline is None:
        deadline = getattr(_ACTIVE_DEADLINE, "value", None)
    if not isinstance(payload, bytes) or not payload:
        raise ValueError("KNMI HDF5-payload is leeg of niet exact bytes")
    if len(payload) > MAX_HDF5_BYTES:
        raise ValueError("KNMI HDF5-payload overschrijdt de vaste objectgrens")
    if deadline is not None:
        _remaining_seconds(deadline)
    try:
        with h5py.File(io.BytesIO(payload), "r") as root:
            _validate_hdf_self_contained(root, deadline=deadline)
            overview = _hard_group(root, "overview").attrs
            group_name = _scalar_text(overview, "product_group_name")
            if group_name != KNMI_INTERNAL_PRODUCT_NAME:
                raise ValueError("KNMI HDF5 is niet het realtime radaraccumulatieproduct")
            tag_version = _scalar_text(overview, "hdftag_version_number")
            if tag_version not in KNMI_SUPPORTED_HDF_TAG_VERSIONS:
                raise ValueError("KNMI HDF5 gebruikt een niet-ondersteunde HDF-tagversie")
            product_start = _parse_hdf_timestamp(
                _scalar_text(overview, "product_datetime_start")
            )
            product_end = _parse_hdf_timestamp(
                _scalar_text(overview, "product_datetime_end")
            )
            duration = product_end - product_start
            if not math.isfinite(duration) or not 299.0 <= duration <= 301.0:
                raise ValueError("KNMI HDF5 is geen exacte vijfminutenperiode")

            geographic = _hard_group(root, "geographic").attrs
            columns = _positive_int(
                geographic, "geo_number_columns", maximum=10_000
            )
            rows = _positive_int(geographic, "geo_number_rows", maximum=10_000)
            if columns != KNMI_GRID_COLUMNS or rows != KNMI_GRID_ROWS:
                raise ValueError("KNMI HDF5 heeft niet het NL25 700x765-raster")

            image_group = _hard_group(root, "image1")
            image_product_name = _scalar_text(
                image_group.attrs, "image_product_name"
            )
            if image_product_name != KNMI_INTERNAL_PRODUCT_NAME:
                raise ValueError("KNMI HDF5-image is niet het NL25 realtime product")
            if image_product_name != group_name:
                raise ValueError("KNMI HDF5-productgroep en image-identiteit verschillen")
            image_geo_parameter = _scalar_text(
                image_group.attrs, "image_geo_parameter"
            )
            if image_geo_parameter != KNMI_PRIMARY_IMAGE_PARAMETER:
                raise ValueError("KNMI HDF5-image declareert niet PRECIP in millimeter")
            image = _hard_dataset(root, "image1/image_data")
            if image.ndim != 2 or tuple(image.shape) != (rows, columns):
                raise ValueError("KNMI HDF5-rastervorm stemt niet overeen met metadata")
            if image.dtype.kind != "u" or image.dtype.itemsize != 2:
                raise ValueError("KNMI HDF5-rasterdtype valt buiten het capturecontract")
            if image.chunks is not None:
                if (
                    len(image.chunks) != 2
                    or any(
                        isinstance(value, bool)
                        or not isinstance(value, int)
                        or value <= 0
                        or value > dimension
                        for value, dimension in zip(image.chunks, image.shape)
                    )
                    or math.prod(image.chunks) * image.dtype.itemsize > 8 * 1024 * 1024
                ):
                    raise ValueError("KNMI HDF5-chunkindeling valt buiten de grens")
            bytes_per_pixel = _positive_int(
                image_group.attrs,
                "image_bytes_per_pixel",
                maximum=32,
            )
            image_size = _positive_int(
                image_group.attrs,
                "image_size",
                maximum=MAX_HDF5_BYTES * 8,
            )
            if (
                bytes_per_pixel != int(image.dtype.itemsize)
                or image_size != rows * columns * bytes_per_pixel
            ):
                raise ValueError("KNMI HDF5-imagegrootte stemt niet overeen")
            _validate_auxiliary_image(
                root,
                group_path="image2",
                parameter="QUALITY_[-]",
                deadline=deadline,
            )
            _validate_auxiliary_image(
                root,
                group_path="image3",
                parameter="ADJUSTMENT_FACTOR_[DB]",
                deadline=deadline,
            )
    except OSError as err:
        raise KnmiNativeTransientError(
            "KNMI HDF5-container kon tijdelijk niet worden gelezen"
        ) from err

    if deadline is not None:
        _remaining_seconds(deadline)

    return KnmiHdfIdentity(
        product_group_name=group_name,
        image_product_name=image_product_name,
        image_geo_parameter=image_geo_parameter,
        hdf_tag_version=tag_version,
        product_start=product_start,
        product_end=product_end,
        image_rows=rows,
        image_columns=columns,
    )


def _canonical_json_bytes(value: Mapping[str, object]) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")


def _identity_from_document(value: Any) -> KnmiHdfIdentity:
    fields = {
        "product_group_name",
        "image_product_name",
        "image_geo_parameter",
        "hdf_tag_version",
        "product_start",
        "product_end",
        "image_rows",
        "image_columns",
    }
    if not isinstance(value, dict) or set(value) != fields:
        raise ValueError("KNMI validation verdict bevat identity-schema drift")
    for field in (
        "product_group_name",
        "image_product_name",
        "image_geo_parameter",
        "hdf_tag_version",
    ):
        if not isinstance(value[field], str) or not 1 <= len(value[field]) <= 512:
            raise ValueError("KNMI validation verdict bevat ongeldige identiteit")
    for field in ("product_start", "product_end"):
        if isinstance(value[field], bool) or not isinstance(value[field], (int, float)):
            raise ValueError("KNMI validation verdict bevat ongeldige producttijd")
        if not math.isfinite(float(value[field])):
            raise ValueError("KNMI validation verdict bevat niet-eindige producttijd")
    for field in ("image_rows", "image_columns"):
        if isinstance(value[field], bool) or not isinstance(value[field], int):
            raise ValueError("KNMI validation verdict bevat ongeldige rastervorm")
    identity = KnmiHdfIdentity(
        product_group_name=value["product_group_name"],
        image_product_name=value["image_product_name"],
        image_geo_parameter=value["image_geo_parameter"],
        hdf_tag_version=value["hdf_tag_version"],
        product_start=float(value["product_start"]),
        product_end=float(value["product_end"]),
        image_rows=value["image_rows"],
        image_columns=value["image_columns"],
    )
    if (
        identity.product_group_name != KNMI_INTERNAL_PRODUCT_NAME
        or identity.image_product_name != KNMI_INTERNAL_PRODUCT_NAME
        or identity.image_geo_parameter != KNMI_PRIMARY_IMAGE_PARAMETER
        or identity.hdf_tag_version not in KNMI_SUPPORTED_HDF_TAG_VERSIONS
        or identity.image_rows != KNMI_GRID_ROWS
        or identity.image_columns != KNMI_GRID_COLUMNS
        or not 299.0 <= identity.product_end - identity.product_start <= 301.0
    ):
        raise ValueError("KNMI validation verdict past niet bij het capturecontract")
    return identity


def _validation_verdict_id(content_sha256: str) -> str:
    if _SHA256_RE.fullmatch(content_sha256) is None:
        raise ValueError("KNMI verdict content-SHA-256 is ongeldig")
    key = {
        "content_sha256": content_sha256,
        "inspector_version": KNMI_STRUCTURE_INSPECTOR_VERSION,
        "schema": "stv3-knmi-validation-key-v1",
    }
    return hashlib.sha256(_canonical_json_bytes(key)).hexdigest()


def _validation_verdict_path(store: RawArtifactStore, content_sha256: str) -> Path:
    verdict_id = _validation_verdict_id(content_sha256)
    return _safe_auxiliary_directory(store, "knmi_validation_verdicts") / (
        f"{verdict_id}.json"
    )


def _load_validation_verdict(
    store: RawArtifactStore,
    *,
    content_sha256: str,
) -> tuple[str, KnmiHdfIdentity | str] | None:
    path = _validation_verdict_path(store, content_sha256)
    if not path.exists():
        return None
    info = path.lstat()
    if path.is_symlink() or not stat.S_ISREG(info.st_mode):
        raise ValueError("KNMI validation verdictpad is onveilig")
    if not 1 <= info.st_size <= MAX_VALIDATION_VERDICT_BYTES:
        raise ValueError("KNMI validation verdictgrootte is ongeldig")
    payload = path.read_bytes()
    if len(payload) != info.st_size:
        raise ValueError("KNMI validation verdict veranderde tijdens lezen")
    try:
        document = json.loads(payload.decode("ascii", errors="strict"))
    except (UnicodeDecodeError, json.JSONDecodeError) as err:
        raise ValueError("KNMI validation verdict is geen canonieke JSON") from err
    common = {
        "content_sha256",
        "inspector_version",
        "schema",
        "status",
        "verdict_id",
    }
    status = document.get("status") if isinstance(document, dict) else None
    expected = common | ({"identity"} if status == "validated" else {"error"})
    expected_id = _validation_verdict_id(content_sha256)
    if (
        not isinstance(document, dict)
        or set(document) != expected
        or document.get("schema") != "stv3-knmi-validation-verdict-v1"
        or document.get("inspector_version") != KNMI_STRUCTURE_INSPECTOR_VERSION
        or document.get("content_sha256") != content_sha256
        or document.get("verdict_id") != expected_id
        or payload != _canonical_json_bytes(document)
    ):
        raise ValueError("KNMI validation verdict past niet bij de bronbytes")
    if status == "validated":
        return status, _identity_from_document(document["identity"])
    error = document.get("error")
    if (
        status != "deterministic_rejection"
        or not isinstance(error, str)
        or not 1 <= len(error) <= 512
        or any(ord(character) < 0x20 for character in error)
    ):
        raise ValueError("KNMI rejection verdict is ongeldig")
    return status, error


def _fsync_directory(directory: Path) -> None:
    if os.name == "nt":
        return
    descriptor = os.open(directory, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _publish_validation_verdict(
    store: RawArtifactStore,
    *,
    content_sha256: str,
    identity: KnmiHdfIdentity | None = None,
    error: str | None = None,
    deadline: float,
) -> tuple[str, KnmiHdfIdentity | str]:
    if (identity is None) == (error is None):
        raise ValueError("KNMI validation verdict mist een eenduidige status")
    verdict_id = _validation_verdict_id(content_sha256)
    document: dict[str, object] = {
        "content_sha256": content_sha256,
        "inspector_version": KNMI_STRUCTURE_INSPECTOR_VERSION,
        "schema": "stv3-knmi-validation-verdict-v1",
        "status": "validated" if identity is not None else "deterministic_rejection",
        "verdict_id": verdict_id,
    }
    if identity is not None:
        document["identity"] = asdict(identity)
    else:
        safe_error = str(error)
        if not 1 <= len(safe_error) <= 512 or any(
            ord(character) < 0x20 for character in safe_error
        ):
            safe_error = "KNMI HDF5 wijkt af van het vaste capturecontract"
        document["error"] = safe_error
    payload = _canonical_json_bytes(document)
    if len(payload) > MAX_VALIDATION_VERDICT_BYTES:
        raise ValueError("KNMI validation verdict overschrijdt de bytegrens")
    path = _validation_verdict_path(store, content_sha256)
    directory = path.parent
    capacity_lock = directory / "capacity.lock"
    with _deadline_file_lock(capacity_lock, deadline=deadline):
        existing = _load_validation_verdict(store, content_sha256=content_sha256)
        if existing is None:
            temporary_path: Path | None = None
            try:
                with tempfile.NamedTemporaryFile(
                    mode="w+b",
                    prefix=".knmi-verdict-",
                    suffix=".tmp",
                    dir=directory,
                    delete=False,
                ) as temporary:
                    temporary.write(payload)
                    temporary.flush()
                    os.fsync(temporary.fileno())
                    temporary_path = Path(temporary.name)
                try:
                    os.link(temporary_path, path)
                except FileExistsError:
                    pass
                _fsync_directory(directory)
            finally:
                if temporary_path is not None:
                    temporary_path.unlink(missing_ok=True)
            existing = _load_validation_verdict(store, content_sha256=content_sha256)
        if existing is None:  # pragma: no cover - durability invariant
            raise RuntimeError("KNMI validation verdict ontbreekt na publicatie")
        if (
            existing[0] != document["status"]
            or (
                existing[0] == "validated"
                and asdict(existing[1]) != document.get("identity")
            )
            or (
                existing[0] == "deterministic_rejection"
                and existing[1] != document.get("error")
            )
        ):
            raise ValueError("KNMI validation verdict-ID heeft een conflict")
        candidates = sorted(
            (
                item.lstat().st_mtime_ns,
                item.name,
                item,
            )
            for item in directory.iterdir()
            if re.fullmatch(r"[0-9a-f]{64}\.json", item.name)
            and item.is_file()
            and not item.is_symlink()
        )
        for _mtime, _name, candidate in candidates[:-MAX_VALIDATION_VERDICTS]:
            if candidate != path:
                candidate.unlink(missing_ok=True)
        _fsync_directory(directory)
    _remaining_seconds(deadline)
    return existing


def _inspect_hdf_identity_cached(
    store: RawArtifactStore,
    payload: bytes,
    *,
    content_sha256: str,
    deadline: float,
) -> KnmiHdfIdentity:
    _remaining_seconds(deadline)
    verdict = _load_validation_verdict(store, content_sha256=content_sha256)
    if verdict is not None:
        if verdict[0] == "validated":
            assert isinstance(verdict[1], KnmiHdfIdentity)
            return verdict[1]
        raise ValueError(str(verdict[1]))
    try:
        # The active deadline scope keeps legacy one-argument inspectors and
        # test attestations compatible while still enforcing one absolute job
        # deadline in the concrete implementation.
        identity = _inspect_hdf_identity(payload)
    except (TimeoutError, KnmiNativeTransientError):
        raise
    except ValueError as err:
        _publish_validation_verdict(
            store,
            content_sha256=content_sha256,
            error=str(err),
            deadline=deadline,
        )
        raise
    _publish_validation_verdict(
        store,
        content_sha256=content_sha256,
        identity=identity,
        deadline=deadline,
    )
    return identity


def _source_reference(
    receipt: RawArtifactReceipt,
    *,
    filename: str,
    identity: KnmiHdfIdentity,
) -> dict[str, object]:
    return {
        "role": "hdf5",
        "raw_receipt_id": receipt.receipt_id,
        "raw_content_sha256": receipt.content_sha256,
        "raw_receipt_sha256": receipt.receipt_sha256,
        "fetch_id": receipt.fetch_id,
        "upstream_record_id": receipt.upstream_id,
        "upstream_filename": filename,
        "received_at": receipt.received_at,
        "content_type": receipt.content_type,
        "product_timestamp": identity.product_end,
        "product_start_timestamp": identity.product_start,
        "collector_version": KNMI_RAW_COLLECTOR_VERSION,
        "structure_inspector_version": KNMI_STRUCTURE_INSPECTOR_VERSION,
        "hdf_tag_version": identity.hdf_tag_version,
        "product_group_name": identity.product_group_name,
        "grid_rows": identity.image_rows,
        "grid_columns": identity.image_columns,
        "declared_semantic_class": KNMI_SEMANTIC_CLASS,
        "declared_quantity": KNMI_QUANTITY,
        "declared_unit": KNMI_UNIT,
        "declared_accumulation_minutes": KNMI_ACCUMULATION_MINUTES,
        "quantitative_semantics_eligible": False,
    }


def _capture_provenance(
    capture: RawArtifactCaptureReceipt,
    *,
    receipt: RawArtifactReceipt,
    filename: str,
    identity: KnmiHdfIdentity,
) -> dict[str, object]:
    return {
        "raw_store_schema_version": capture.raw_store_schema_version,
        "raw_capture_id": capture.capture_id,
        "raw_capture_manifest_sha256": capture.manifest_sha256,
        "raw_capture_receipt_sha256": capture.capture_receipt_sha256,
        "raw_capture_fetch_id": capture.fetch_id,
        "upstream_capture_id": capture.upstream_capture_id,
        "product_timestamp": identity.product_end,
        "collector_version": KNMI_RAW_COLLECTOR_VERSION,
        "structure_inspector_versions": {
            "hdf5": KNMI_STRUCTURE_INSPECTOR_VERSION,
        },
        "provenance_grade": "exact_raw",
        "provenance_scope": "upstream_hdf5_bytes_only",
        "raw_sources": {
            "hdf5": _source_reference(
                receipt,
                filename=filename,
                identity=identity,
            )
        },
    }


def _reject_secret_payload(payload: Any) -> None:
    if not isinstance(payload, dict):
        raise ValueError("KNMI workerjobpayload moet een object zijn")
    if payload:
        raise ValueError("KNMI workerjobpayload moet exact leeg zijn")


def _worker_api_key() -> str:
    value = os.getenv("STV3_KNMI_API_KEY", "")
    if (
        not value
        or len(value) > 4096
        or not value.isascii()
        or any(ord(character) <= 0x20 or ord(character) == 0x7F for character in value)
    ):
        raise ValueError("KNMI API-key ontbreekt of is ongeldig in de workeromgeving")
    return value


def _validate_job_id(value: Any) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value) > 480
        or any(ord(character) < 0x20 or ord(character) == 0x7F for character in value)
    ):
        raise ValueError("KNMI workerjob mist een geldige job_id")
    return value


def _receipt_id(store: RawArtifactStore, job_id: str) -> str:
    return store._receipt_id(  # noqa: SLF001 - deterministic raw-store contract
        provider="knmi",
        product=KNMI_HDF5_PRODUCT,
        fetch_id=f"{job_id}:hdf5",
    )


def _capture_id(store: RawArtifactStore, job_id: str) -> str:
    return store._capture_id(  # noqa: SLF001 - deterministic raw-store contract
        provider="knmi",
        product=KNMI_CAPTURE_PRODUCT,
        fetch_id=f"{job_id}:capture",
    )


def _validate_durable_receipt(
    receipt: RawArtifactReceipt,
    *,
    job_id: str,
) -> str:
    provenance = receipt.provenance
    filename = _safe_filename(provenance.get("upstream_filename"))
    expected_upstream = (
        f"{KNMI_DATASET_NAME}/{KNMI_DATASET_VERSION}|filename={filename}"
    )
    encoded = json.dumps(provenance, sort_keys=True, ensure_ascii=True)
    if (
        receipt.provider != "knmi"
        or receipt.product != KNMI_HDF5_PRODUCT
        or receipt.fetch_id != f"{job_id}:hdf5"
        or receipt.upstream_id != expected_upstream
        or receipt.content_type not in {"application/octet-stream", "application/x-hdf5"}
        or not 1 <= receipt.size_bytes <= MAX_HDF5_BYTES
        or provenance.get("representation") != NATIVE_REPRESENTATION
        or provenance.get("capture_stage") != NATIVE_CAPTURE_STAGE
        or provenance.get("collector") != KNMI_RAW_COLLECTOR
        or provenance.get("collector_version") != KNMI_RAW_COLLECTOR_VERSION
        or provenance.get("structure_inspector_version")
        != KNMI_STRUCTURE_INSPECTOR_VERSION
        or provenance.get("dataset_name") != KNMI_DATASET_NAME
        or provenance.get("dataset_version") != KNMI_DATASET_VERSION
        or provenance.get("listing_size_bytes") != receipt.size_bytes
        or provenance.get("download_size_bytes") != receipt.size_bytes
        or "https://" in encoded
        or "authorization" in encoded.lower()
        or "credential" in encoded.lower()
        or "signature" in encoded.lower()
    ):
        raise RuntimeError("duurzaam KNMI-receipt heeft een verkeerde identiteit")
    return filename


def _validate_durable_capture(
    capture: RawArtifactCaptureReceipt,
    *,
    job_id: str,
    receipt: RawArtifactReceipt,
    filename: str,
    identity: KnmiHdfIdentity,
) -> None:
    expected_upstream = (
        f"{KNMI_CAPTURE_PRODUCT}|filename={filename}"
        f"|product_timestamp={identity.product_end:.6f}"
        f"|content_sha256={receipt.content_sha256}"
    )
    members = {member.role: member.receipt_id for member in capture.members}
    if (
        capture.provider != "knmi"
        or capture.product != KNMI_CAPTURE_PRODUCT
        or capture.fetch_id != f"{job_id}:capture"
        or capture.upstream_capture_id != expected_upstream
        or capture.product_timestamp != identity.product_end
        or members != {"hdf5": receipt.receipt_id}
    ):
        raise RuntimeError("duurzame KNMI-capture heeft een verkeerde identiteit")


def _capture_result(
    *,
    capture: RawArtifactCaptureReceipt,
    receipt: RawArtifactReceipt,
    filename: str,
    identity: KnmiHdfIdentity,
) -> dict[str, object]:
    return {
        "provider": "knmi",
        "product": KNMI_HDF5_PRODUCT,
        "dataset_name": KNMI_DATASET_NAME,
        "dataset_version": KNMI_DATASET_VERSION,
        "declared_semantic_class": KNMI_SEMANTIC_CLASS,
        "declared_quantity": KNMI_QUANTITY,
        "declared_unit": KNMI_UNIT,
        "declared_accumulation_minutes": KNMI_ACCUMULATION_MINUTES,
        "frame_timestamp": identity.product_end,
        "quantitative_semantics_eligible": False,
        "operational_admission_eligible": False,
        "admission_status": KNMI_ADMISSION_STATUS,
        "decode_status": "not_decoded",
        "capture_state": "captured_not_decoded",
        "weather_state": "unknown",
        "raw_provenance": _capture_provenance(
            capture,
            receipt=receipt,
            filename=filename,
            identity=identity,
        ),
    }


def capture_latest_knmi_hdf5(
    job,
    *,
    raw_store: RawArtifactStore | None = None,
) -> dict[str, object]:
    """Capture the latest native file and return payload-free evidence facts."""

    job_id = _validate_job_id(getattr(job, "job_id", None))
    payload = getattr(job, "payload", None)
    _reject_secret_payload(payload)
    deadline = time.monotonic() + JOB_TOTAL_DEADLINE_SECONDS
    with _deadline_scope(deadline):
        store = _raw_store(raw_store)
        _remaining_seconds(deadline)
        with _deadline_thread_lock(_KNMI_CAPTURE_LOCK, deadline=deadline):
            with _deadline_file_lock(
                _capture_capacity_lock_path(store),
                deadline=deadline,
            ), _deadline_file_lock(
                _slot_lock_path(store, job_id),
                deadline=deadline,
            ):
                bundle = store.load_capture_bundle_bytes(_capture_id(store, job_id))
                _remaining_seconds(deadline)
                if bundle is not None:
                    capture, receipts, payloads = bundle
                    if set(receipts) != {"hdf5"} or set(payloads) != {"hdf5"}:
                        raise RuntimeError("KNMI-capture mist zijn exacte HDF5-member")
                    receipt = receipts["hdf5"]
                    exact_payload = payloads["hdf5"]
                    filename = _validate_durable_receipt(receipt, job_id=job_id)
                    identity = _inspect_hdf_identity_cached(
                        store,
                        exact_payload,
                        content_sha256=receipt.content_sha256,
                        deadline=deadline,
                    )
                    _validate_filename_identity(filename, identity)
                    _validate_durable_capture(
                        capture,
                        job_id=job_id,
                        receipt=receipt,
                        filename=filename,
                        identity=identity,
                    )
                    _remaining_seconds(deadline)
                    return _capture_result(
                        capture=capture,
                        receipt=receipt,
                        filename=filename,
                        identity=identity,
                    )

                loaded = store.load_receipt_bytes(_receipt_id(store, job_id))
                _remaining_seconds(deadline)
                if loaded is None:
                    api_key = _worker_api_key()
                    summary = _latest_filename(api_key=api_key)
                    _remaining_seconds(deadline)
                    descriptor = _temporary_download_url(summary, api_key=api_key)
                    _remaining_seconds(deadline)
                    response = _download_hdf5(descriptor)
                    _remaining_seconds(deadline)
                    receipt = store.store(
                        provider="knmi",
                        product=KNMI_HDF5_PRODUCT,
                        fetch_id=f"{job_id}:hdf5",
                        content_type=response.content_type,
                        received_at=response.received_at,
                        upstream_id=(
                            f"{KNMI_DATASET_NAME}/{KNMI_DATASET_VERSION}"
                            f"|filename={summary.filename}"
                        ),
                        provenance=_native_provenance(summary, descriptor),
                        payload=response.payload,
                    )
                    _remaining_seconds(deadline)
                    exact_payload = response.payload
                    filename = summary.filename
                    del descriptor
                    filename = _validate_durable_receipt(receipt, job_id=job_id)
                else:
                    receipt, exact_payload = loaded
                    filename = _validate_durable_receipt(receipt, job_id=job_id)

                # Exact bytes are durable before this first HDF5 operation.
                identity = _inspect_hdf_identity_cached(
                    store,
                    exact_payload,
                    content_sha256=receipt.content_sha256,
                    deadline=deadline,
                )
                _validate_filename_identity(filename, identity)
                _remaining_seconds(deadline)
                upstream_capture_id = (
                    f"{KNMI_CAPTURE_PRODUCT}|filename={filename}"
                    f"|product_timestamp={identity.product_end:.6f}"
                    f"|content_sha256={receipt.content_sha256}"
                )
                capture = store.commit_capture(
                    provider="knmi",
                    product=KNMI_CAPTURE_PRODUCT,
                    fetch_id=f"{job_id}:capture",
                    upstream_capture_id=upstream_capture_id,
                    product_timestamp=identity.product_end,
                    members={"hdf5": receipt.receipt_id},
                )
                _remaining_seconds(deadline)
                return _capture_result(
                    capture=capture,
                    receipt=receipt,
                    filename=filename,
                    identity=identity,
                )


__all__ = [
    "KNMI_ACCUMULATION_MINUTES",
    "KNMI_ADMISSION_STATUS",
    "KNMI_CAPTURE_PRODUCT",
    "KNMI_DATASET_NAME",
    "KNMI_DATASET_VERSION",
    "KNMI_HDF5_PRODUCT",
    "KNMI_QUANTITY",
    "KNMI_SEMANTIC_CLASS",
    "KNMI_UNIT",
    "KnmiDownloadDescriptor",
    "KnmiFileSummary",
    "KnmiHdfIdentity",
    "capture_latest_knmi_hdf5",
]
