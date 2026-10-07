"""Exact native capture for AviationWeather's worldwide METAR CSV cache.

This is a global source capture, not a tracker query.  It retains the exact
``metars.cache.csv.gz`` response before decompression and performs only a
bounded CSV/schema inspection.  It intentionally emits no station weather
facts and no local wet/dry decision.
"""
from __future__ import annotations

import csv
import codecs
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import errno
import hashlib
import json
import math
import os
from pathlib import Path
import re
import stat
import tempfile
import threading
import time
from typing import Any, Iterator
from urllib.request import (
    HTTPRedirectHandler,
    ProxyHandler,
    Request,
    build_opener,
)
import zlib

from .raw_artifacts import (
    NATIVE_CAPTURE_STAGE,
    NATIVE_REPRESENTATION,
    RawArtifactCaptureReceipt,
    RawArtifactReceipt,
    RawArtifactStore,
)
METAR_CACHE_URL = "https://aviationweather.gov/data/cache/metars.cache.csv.gz"
MAX_COMPRESSED_BYTES = 16 * 1024 * 1024
MAX_UNCOMPRESSED_BYTES = 64 * 1024 * 1024
MAX_CSV_ROWS = 20_000
MAX_CSV_FIELD_CHARS = 32 * 1024
MAX_CSV_RECORD_CHARS = 128 * 1024
MAX_INSPECTION_SECONDS = 10.0
HTTP_IO_TIMEOUT_SECONDS = 10.0
HTTP_TOTAL_DEADLINE_SECONDS = 30.0
JOB_TOTAL_DEADLINE_SECONDS = 45.0
LOCK_POLL_SECONDS = 0.05
MAX_VALIDATION_VERDICT_BYTES = 16 * 1024
MAX_VALIDATION_VERDICTS = 1_024
MAX_CURRENT_REPORT_AGE_SECONDS = 2 * 60 * 60
MAX_FUTURE_CLOCK_SKEW_SECONDS = 5 * 60

METAR_ALLOWED_CONTENT_TYPES = frozenset({
    "application/gzip",
    "application/octet-stream",
    "application/x-gzip",
})
METAR_EXPECTED_HEADER = (
    "raw_text",
    "station_id",
    "observation_time",
    "latitude",
    "longitude",
    "temp_c",
    "dewpoint_c",
    "wind_dir_degrees",
    "wind_speed_kt",
    "wind_gust_kt",
    "visibility_statute_mi",
    "altim_in_hg",
    "sea_level_pressure_mb",
    "corrected",
    "auto",
    "auto_station",
    "maintenance_indicator_on",
    "no_signal",
    "lightning_sensor_off",
    "freezing_rain_sensor_off",
    "present_weather_sensor_off",
    "wx_string",
    "sky_cover",
    "cloud_base_ft_agl",
    "sky_cover",
    "cloud_base_ft_agl",
    "sky_cover",
    "cloud_base_ft_agl",
    "sky_cover",
    "cloud_base_ft_agl",
    "flight_category",
    "three_hr_pressure_tendency_mb",
    "maxT_c",
    "minT_c",
    "maxT24hr_c",
    "minT24hr_c",
    "precip_in",
    "pcp3hr_in",
    "pcp6hr_in",
    "pcp24hr_in",
    "snow_in",
    "vert_vis_ft",
    "metar_type",
    "elevation_m",
)

METAR_RAW_SHARE_ROOT = Path("/share/storm_tracker_worker")
METAR_RAW_ARTIFACT_ROOT = METAR_RAW_SHARE_ROOT / "raw" / "metar_cache"
METAR_GZIP_PRODUCT = "aviationweather_current_metars_csv_gzip"
METAR_CAPTURE_PRODUCT = "aviationweather_current_metars_capture"
METAR_RAW_COLLECTOR = "aviationweather_metars_cache"
METAR_RAW_COLLECTOR_VERSION = "worker-metar-cache-exact-raw-v2"
METAR_STRUCTURE_INSPECTOR_VERSION = "aviationweather-metars-csv-structure-v2"
METAR_SEMANTIC_CLASS = "raw_terminal_observation_reports"
METAR_ADMISSION_STATUS = "exact_raw_parser_not_certified"
METAR_TIMESTAMP_QUALITY_STATE = "unverified_non_operational"

_RAW_TEXT_INDEX = 0
_STATION_INDEX = 1
_OBSERVATION_TIME_INDEX = 2
_LATITUDE_INDEX = 3
_LONGITUDE_INDEX = 4
_METAR_TYPE_INDEX = 42
_STATION_RE = re.compile(r"^[A-Z0-9]{4}$")
_OBSERVATION_TIME_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?Z$"
)
_METAR_RAW_STORE: RawArtifactStore | None = None
_METAR_RAW_STORE_LOCK = threading.Lock()
_METAR_CAPTURE_LOCK = threading.Lock()
_ACTIVE_DEADLINE = threading.local()
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


@dataclass(frozen=True, slots=True)
class _MetarHttpResponse:
    payload: bytes
    content_type: str
    received_at: float


@dataclass(frozen=True, slots=True)
class MetarCacheIdentity:
    row_count: int
    structurally_valid_row_count: int
    invalid_timestamp_row_count: int
    current_timestamp_row_count: int
    stale_timestamp_row_count: int
    future_timestamp_row_count: int
    declared_oldest_observation_timestamp: float
    declared_newest_observation_timestamp: float
    header: tuple[str, ...]


class _RejectRedirects(HTTPRedirectHandler):
    """Reject every redirect so a cache fetch cannot become an SSRF hop."""

    def redirect_request(self, *_args, **_kwargs):
        return None


def _open_cache_request(request: Request, *, timeout: float):
    opener = build_opener(ProxyHandler({}), _RejectRedirects())
    return opener.open(request, timeout=timeout)


def _remaining_seconds(deadline: float) -> float:
    remaining = float(deadline) - time.monotonic()
    if not math.isfinite(remaining) or remaining <= 0:
        raise TimeoutError("METAR totale jobdeadline is verstreken")
    return remaining


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
            except AttributeError:  # pragma: no cover
                pass
        else:
            _ACTIVE_DEADLINE.value = previous


@contextmanager
def _deadline_thread_lock(lock: threading.Lock, *, deadline: float) -> Iterator[None]:
    if not lock.acquire(timeout=_remaining_seconds(deadline)):
        raise TimeoutError("METAR capturelock overschrijdt de jobdeadline")
    try:
        _remaining_seconds(deadline)
        yield
    finally:
        lock.release()


def _safe_auxiliary_directory(store: RawArtifactStore, name: str) -> Path:
    root = Path(store.root)
    if root.is_symlink() or not root.is_dir():
        raise ValueError("METAR auxiliary root is onveilig")
    directory = root / name
    existed = directory.exists()
    if existed and (directory.is_symlink() or not directory.is_dir()):
        raise ValueError("METAR auxiliary map is onveilig")
    directory.mkdir(mode=0o700, exist_ok=True)
    if directory.is_symlink() or directory.resolve().parent != root.resolve():
        raise ValueError("METAR auxiliary map ontsnapt uit raw root")
    if not existed and os.name != "nt":
        descriptor = os.open(root, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    return directory


def _slot_lock_path(store: RawArtifactStore, job_id: str) -> Path:
    token = hashlib.sha256(job_id.encode("utf-8", errors="strict")).hexdigest()
    return _safe_auxiliary_directory(store, "metar_locks") / f"slot-{token}.lock"


def _capture_capacity_lock_path(store: RawArtifactStore) -> Path:
    return _safe_auxiliary_directory(store, "metar_locks") / "capture-capacity.lock"


@contextmanager
def _deadline_file_lock(path: Path, *, deadline: float) -> Iterator[None]:
    _remaining_seconds(deadline)
    if path.exists() and (path.is_symlink() or not path.is_file()):
        raise ValueError("METAR lockpad is onveilig")
    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_BINARY", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags, 0o600)
    acquired = False
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode):
            raise ValueError("METAR lockpad is geen regulier bestand")
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


def _declared_content_length(headers) -> int | None:
    value = headers.get("Content-Length") if headers is not None else None
    if value is None:
        return None
    normalized = str(value).strip()
    if re.fullmatch(r"\d+", normalized) is None:
        raise ValueError("ongeldige METAR Content-Length")
    declared = int(normalized)
    if declared > MAX_COMPRESSED_BYTES:
        raise ValueError("opgegeven payloadlengte overschrijdt contractlimiet")
    return declared


def _read_cache_body(response, *, declared_bytes: int | None) -> bytes:
    own_deadline = time.monotonic() + HTTP_TOTAL_DEADLINE_SECONDS
    active = getattr(_ACTIVE_DEADLINE, "value", None)
    deadline = min(own_deadline, active) if active is not None else own_deadline
    chunks: list[bytes] = []
    total = 0
    while total <= MAX_COMPRESSED_BYTES:
        if time.monotonic() > deadline:
            raise TimeoutError("METAR-download overschrijdt totale deadline")
        request_bytes = min(64 * 1024, MAX_COMPRESSED_BYTES + 1 - total)
        chunk = response.read(request_bytes)
        if time.monotonic() > deadline:
            raise TimeoutError("METAR-download overschrijdt totale deadline")
        if not chunk:
            break
        if not isinstance(chunk, (bytes, bytearray, memoryview)):
            raise TypeError("METAR-responsstream moet bytes opleveren")
        immutable = bytes(chunk)
        if len(immutable) > request_bytes:
            raise ValueError("METAR-responsstream overschrijdt de chunkgrens")
        chunks.append(immutable)
        total += len(immutable)
        if declared_bytes is not None and total > declared_bytes:
            raise ValueError("payloadlengte wijkt af van de opgegeven lengte")
        if total > MAX_COMPRESSED_BYTES:
            raise ValueError("payload overschrijdt contractlimiet")
    payload = b"".join(chunks)
    if declared_bytes is not None and len(payload) != declared_bytes:
        raise ValueError("payloadlengte wijkt af van de opgegeven lengte")
    return payload


def _download_cache() -> _MetarHttpResponse:
    request = Request(
        METAR_CACHE_URL,
        headers={
            "Accept": "application/gzip,application/octet-stream",
            "Accept-Encoding": "identity",
            "User-Agent": "StormTrackerWorker/1.16",
        },
    )
    active = getattr(_ACTIVE_DEADLINE, "value", None)
    timeout = HTTP_IO_TIMEOUT_SECONDS
    if active is not None:
        timeout = min(timeout, _remaining_seconds(active))
    with _open_cache_request(
        request,
        timeout=timeout,
    ) as response:
        if getattr(response, "status", None) != 200:
            raise ValueError("METAR-cacheantwoord heeft geen HTTP 200-status")
        geturl = getattr(response, "geturl", None)
        if not callable(geturl) or geturl() != METAR_CACHE_URL:
            raise ValueError("METAR-cacheantwoord komt niet van de exacte bron-URL")
        headers = getattr(response, "headers", None)
        encoding = (
            str(headers.get("Content-Encoding") or "identity")
            if headers is not None
            else "identity"
        )
        if len(encoding) > 64 or any(ord(character) < 0x20 for character in encoding):
            raise ValueError("METAR Content-Encoding-header is ongeldig")
        if encoding.lower() != "identity":
            raise ValueError("onverwachte METAR-transportcompressie")
        declared = _declared_content_length(headers)
        raw_content_type = (
            str(headers.get("Content-Type") or "")
            if headers is not None
            else ""
        )
        if len(raw_content_type) > 256 or any(
            ord(character) < 0x20 for character in raw_content_type
        ):
            raise ValueError("METAR Content-Type-header is ongeldig")
        content_type = raw_content_type.split(";", 1)[0].strip().lower()
        if content_type not in METAR_ALLOWED_CONTENT_TYPES:
            raise ValueError("METAR-cache heeft een onverwacht Content-Type")
        payload = _read_cache_body(response, declared_bytes=declared)
        return _MetarHttpResponse(
            payload=payload,
            content_type=content_type,
            received_at=time.time(),
        )


def _raw_store(injected: RawArtifactStore | None = None) -> RawArtifactStore:
    if injected is not None:
        injected.initialize()
        return injected
    share_root = METAR_RAW_SHARE_ROOT.resolve(strict=False)
    raw_root = METAR_RAW_ARTIFACT_ROOT.resolve(strict=False)
    if raw_root == share_root or share_root not in raw_root.parents:
        raise RuntimeError("METAR raw-artifactpad ontsnapt uit worker-share")
    global _METAR_RAW_STORE
    with _METAR_RAW_STORE_LOCK:
        if _METAR_RAW_STORE is None:
            candidate = RawArtifactStore(raw_root)
            candidate.initialize()
            _METAR_RAW_STORE = candidate
        return _METAR_RAW_STORE


def _observation_timestamp(value: str) -> float | None:
    if not isinstance(value, str) or _OBSERVATION_TIME_RE.fullmatch(value) is None:
        return None
    try:
        moment = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (AttributeError, OverflowError, OSError, ValueError):
        return None
    if moment.tzinfo is None or moment.utcoffset() != timezone.utc.utcoffset(moment):
        return None
    try:
        timestamp = moment.astimezone(timezone.utc).timestamp()
    except (OverflowError, OSError, ValueError):
        return None
    return timestamp if math.isfinite(timestamp) and timestamp >= 0 else None


def _iter_gzip_chunks(payload: bytes, *, deadline: float) -> Iterator[bytes]:
    """Yield one bounded gzip member without materialising its full output."""

    if not isinstance(payload, bytes) or not payload:
        raise ValueError("METAR-cachepayload is leeg of niet exact bytes")
    if len(payload) > MAX_COMPRESSED_BYTES:
        raise ValueError("gecomprimeerde payload overschrijdt contractlimiet")
    effective_limit = min(
        MAX_UNCOMPRESSED_BYTES,
        max(1, math.floor(len(payload) * 200.0)),
    )
    decoder = zlib.decompressobj(zlib.MAX_WBITS | 16)
    pending = payload
    total = 0
    try:
        while pending:
            if time.monotonic() > deadline:
                raise TimeoutError("METAR-inspectie overschrijdt totale deadline")
            before = len(pending)
            remaining = effective_limit - total
            decoded = decoder.decompress(
                pending,
                min(64 * 1024, remaining + 1),
            )
            pending = decoder.unconsumed_tail
            if decoded:
                total += len(decoded)
                if total > effective_limit:
                    raise ValueError(
                        "uitgepakte payload overschrijdt contractlimiet"
                    )
                yield decoded
            if decoder.eof:
                break
            if not pending:
                break
            if not decoded and len(pending) >= before:
                raise ValueError("gecomprimeerde payload maakt geen voortgang")
    except zlib.error as err:
        raise ValueError("gecomprimeerde payload is ongeldig") from err
    if not decoder.eof:
        raise ValueError("gecomprimeerde payload is onvolledig")
    if decoder.unused_data or decoder.unconsumed_tail or pending:
        raise ValueError(
            "gecomprimeerde payload bevat extra leden of trailing data"
        )


def _iter_csv_lines(payload: bytes, *, deadline: float) -> Iterator[str]:
    decoder = codecs.getincrementaldecoder("utf-8-sig")("strict")
    buffer = ""
    for chunk in _iter_gzip_chunks(payload, deadline=deadline):
        try:
            text = decoder.decode(chunk, final=False)
        except UnicodeDecodeError as err:
            raise ValueError("METAR-cache is geen geldige UTF-8 CSV") from err
        if "\x00" in text:
            raise ValueError("METAR-cache bevat NUL-bytes")
        buffer += text
        while True:
            newline = buffer.find("\n")
            if newline < 0:
                break
            line = buffer[:newline + 1]
            buffer = buffer[newline + 1:]
            if len(line) > MAX_CSV_RECORD_CHARS:
                raise ValueError("METAR-cache bevat een te lange fysieke CSV-regel")
            yield line
        if len(buffer) > MAX_CSV_RECORD_CHARS:
            raise ValueError("METAR-cache bevat een te lange fysieke CSV-regel")
    try:
        buffer += decoder.decode(b"", final=True)
    except UnicodeDecodeError as err:
        raise ValueError("METAR-cache is geen geldige UTF-8 CSV") from err
    if "\x00" in buffer:
        raise ValueError("METAR-cache bevat NUL-bytes")
    if buffer:
        if len(buffer) > MAX_CSV_RECORD_CHARS:
            raise ValueError("METAR-cache bevat een te lange fysieke CSV-regel")
        yield buffer


class _BoundedCsvLines:
    """Bound physical and multi-line logical CSV records before parsing."""

    def __init__(self, lines: Iterator[str]) -> None:
        self._lines = iter(lines)
        self._logical_record_chars = 0

    def __iter__(self):
        return self

    def __next__(self) -> str:
        line = next(self._lines)
        self._logical_record_chars += len(line)
        if self._logical_record_chars > MAX_CSV_RECORD_CHARS:
            raise ValueError("METAR-cache bevat een te lang logisch CSV-record")
        return line

    def record_complete(self) -> None:
        self._logical_record_chars = 0


def _inspect_cache(
    payload: bytes,
    *,
    received_at: float,
) -> MetarCacheIdentity:
    """Validate exact AWC gzip/CSV shape without interpreting weather."""

    if (
        isinstance(received_at, bool)
        or not isinstance(received_at, (int, float))
        or not math.isfinite(float(received_at))
        or float(received_at) < 0.0
    ):
        raise ValueError("METAR received_at is ongeldig")
    reference_time = float(received_at)
    own_deadline = time.monotonic() + MAX_INSPECTION_SECONDS
    active = getattr(_ACTIVE_DEADLINE, "value", None)
    deadline = min(own_deadline, active) if active is not None else own_deadline
    lines = _BoundedCsvLines(_iter_csv_lines(payload, deadline=deadline))
    reader = csv.reader(lines, strict=True)
    try:
        header_row = next(reader)
    except StopIteration as err:
        raise ValueError("METAR-cache bevat geen CSV-header") from err
    except csv.Error as err:
        raise ValueError("METAR-cache bevat ongeldige CSV") from err
    lines.record_complete()
    header = tuple(header_row)
    if header != METAR_EXPECTED_HEADER:
        raise ValueError("METAR-cache heeft een onbekend of ambigu CSV-schema")

    row_count = 0
    structurally_valid = 0
    invalid_timestamps = 0
    current_timestamps = 0
    stale_timestamps = 0
    future_timestamps = 0
    oldest: float | None = None
    newest: float | None = None
    while True:
        try:
            row = next(reader)
        except StopIteration:
            break
        except csv.Error as err:
            raise ValueError("METAR-cache bevat ongeldige CSV") from err
        lines.record_complete()
        row_count += 1
        if row_count > MAX_CSV_ROWS:
            raise ValueError("METAR-cache overschrijdt de rijenlimiet")
        if len(row) != len(METAR_EXPECTED_HEADER) or any(
            len(value) > MAX_CSV_FIELD_CHARS for value in row
        ):
            raise ValueError(
                "METAR-cache bevat een onbegrensde of verschoven CSV-rij"
            )

        station = row[_STATION_INDEX]
        observed_at = _observation_timestamp(row[_OBSERVATION_TIME_INDEX])
        if observed_at is None:
            invalid_timestamps += 1
        else:
            oldest = observed_at if oldest is None else min(oldest, observed_at)
            newest = observed_at if newest is None else max(newest, observed_at)
            if observed_at > reference_time + MAX_FUTURE_CLOCK_SKEW_SECONDS:
                future_timestamps += 1
            elif observed_at < reference_time - MAX_CURRENT_REPORT_AGE_SECONDS:
                stale_timestamps += 1
            else:
                current_timestamps += 1
        try:
            latitude = float(row[_LATITUDE_INDEX] or "nan")
            longitude = float(row[_LONGITUDE_INDEX] or "nan")
        except (TypeError, ValueError):
            continue
        if (
            _STATION_RE.fullmatch(station) is None
            or observed_at is None
            or not row[_RAW_TEXT_INDEX].strip()
            or row[_METAR_TYPE_INDEX] not in {"METAR", "SPECI"}
            or not math.isfinite(latitude)
            or not math.isfinite(longitude)
            or not -90.0 <= latitude <= 90.0
            or not -180.0 <= longitude <= 180.0
        ):
            continue
        structurally_valid += 1
    if (
        row_count == 0
        or structurally_valid == 0
        or oldest is None
        or newest is None
    ):
        raise ValueError("METAR-cache bevat geen structureel geldige observaties")
    return MetarCacheIdentity(
        row_count=row_count,
        structurally_valid_row_count=structurally_valid,
        invalid_timestamp_row_count=invalid_timestamps,
        current_timestamp_row_count=current_timestamps,
        stale_timestamp_row_count=stale_timestamps,
        future_timestamp_row_count=future_timestamps,
        declared_oldest_observation_timestamp=oldest,
        declared_newest_observation_timestamp=newest,
        header=header,
    )


def _canonical_json_bytes(value: dict[str, object]) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")


def _identity_document(identity: MetarCacheIdentity) -> dict[str, object]:
    value = asdict(identity)
    value["header"] = list(identity.header)
    return value


def _identity_from_document(value: Any) -> MetarCacheIdentity:
    fields = set(_identity_document(MetarCacheIdentity(
        row_count=1,
        structurally_valid_row_count=1,
        invalid_timestamp_row_count=0,
        current_timestamp_row_count=1,
        stale_timestamp_row_count=0,
        future_timestamp_row_count=0,
        declared_oldest_observation_timestamp=0.0,
        declared_newest_observation_timestamp=0.0,
        header=METAR_EXPECTED_HEADER,
    )))
    if not isinstance(value, dict) or set(value) != fields:
        raise ValueError("METAR validation verdict bevat identity-schema drift")
    integer_fields = fields - {
        "declared_oldest_observation_timestamp",
        "declared_newest_observation_timestamp",
        "header",
    }
    if any(
        isinstance(value[field], bool)
        or not isinstance(value[field], int)
        or value[field] < 0
        for field in integer_fields
    ):
        raise ValueError("METAR validation verdict bevat ongeldige tellers")
    if value["row_count"] < 1 or value["structurally_valid_row_count"] < 1:
        raise ValueError("METAR validation verdict bevat geen geldige rijen")
    if value["structurally_valid_row_count"] > value["row_count"]:
        raise ValueError("METAR validation verdict bevat inconsistente tellers")
    for field in (
        "declared_oldest_observation_timestamp",
        "declared_newest_observation_timestamp",
    ):
        if isinstance(value[field], bool) or not isinstance(value[field], (int, float)):
            raise ValueError("METAR validation verdict bevat een ongeldige tijd")
        if not math.isfinite(float(value[field])) or float(value[field]) < 0:
            raise ValueError("METAR validation verdict bevat een ongeldige tijd")
    if tuple(value["header"]) != METAR_EXPECTED_HEADER:
        raise ValueError("METAR validation verdict bevat een ander CSV-schema")
    return MetarCacheIdentity(
        **{
            **{field: value[field] for field in integer_fields},
            "declared_oldest_observation_timestamp": float(
                value["declared_oldest_observation_timestamp"]
            ),
            "declared_newest_observation_timestamp": float(
                value["declared_newest_observation_timestamp"]
            ),
            "header": METAR_EXPECTED_HEADER,
        }
    )


def _validation_verdict_id(content_sha256: str, received_at: float) -> str:
    if _SHA256_RE.fullmatch(content_sha256) is None or not math.isfinite(received_at):
        raise ValueError("METAR validation verdictsleutel is ongeldig")
    key = {
        "content_sha256": content_sha256,
        "inspector_version": METAR_STRUCTURE_INSPECTOR_VERSION,
        "received_at_hex": float(received_at).hex(),
        "schema": "stv3-metar-validation-key-v1",
    }
    return hashlib.sha256(_canonical_json_bytes(key)).hexdigest()


def _validation_verdict_path(
    store: RawArtifactStore,
    *,
    content_sha256: str,
    received_at: float,
) -> Path:
    verdict_id = _validation_verdict_id(content_sha256, received_at)
    return _safe_auxiliary_directory(store, "metar_validation_verdicts") / (
        f"{verdict_id}.json"
    )


def _load_validation_verdict(
    store: RawArtifactStore,
    *,
    content_sha256: str,
    received_at: float,
) -> tuple[str, MetarCacheIdentity | str] | None:
    path = _validation_verdict_path(
        store,
        content_sha256=content_sha256,
        received_at=received_at,
    )
    if not path.exists():
        return None
    info = path.lstat()
    if path.is_symlink() or not stat.S_ISREG(info.st_mode):
        raise ValueError("METAR validation verdictpad is onveilig")
    if not 1 <= info.st_size <= MAX_VALIDATION_VERDICT_BYTES:
        raise ValueError("METAR validation verdictgrootte is ongeldig")
    payload = path.read_bytes()
    if len(payload) != info.st_size:
        raise ValueError("METAR validation verdict veranderde tijdens lezen")
    try:
        document = json.loads(payload.decode("ascii", errors="strict"))
    except (UnicodeDecodeError, json.JSONDecodeError) as err:
        raise ValueError("METAR validation verdict is geen canonieke JSON") from err
    status = document.get("status") if isinstance(document, dict) else None
    common = {
        "content_sha256",
        "inspector_version",
        "received_at_hex",
        "schema",
        "status",
        "verdict_id",
    }
    expected = common | ({"identity"} if status == "validated" else {"error"})
    if (
        not isinstance(document, dict)
        or set(document) != expected
        or document.get("schema") != "stv3-metar-validation-verdict-v1"
        or document.get("inspector_version") != METAR_STRUCTURE_INSPECTOR_VERSION
        or document.get("content_sha256") != content_sha256
        or document.get("received_at_hex") != float(received_at).hex()
        or document.get("verdict_id")
        != _validation_verdict_id(content_sha256, received_at)
        or payload != _canonical_json_bytes(document)
    ):
        raise ValueError("METAR validation verdict past niet bij de bronbytes")
    if status == "validated":
        return status, _identity_from_document(document["identity"])
    error = document.get("error")
    if (
        status != "deterministic_rejection"
        or not isinstance(error, str)
        or not 1 <= len(error) <= 512
        or any(ord(character) < 0x20 for character in error)
    ):
        raise ValueError("METAR rejection verdict is ongeldig")
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
    received_at: float,
    identity: MetarCacheIdentity | None = None,
    error: str | None = None,
    deadline: float,
) -> tuple[str, MetarCacheIdentity | str]:
    if (identity is None) == (error is None):
        raise ValueError("METAR validation verdict mist een eenduidige status")
    verdict_id = _validation_verdict_id(content_sha256, received_at)
    document: dict[str, object] = {
        "content_sha256": content_sha256,
        "inspector_version": METAR_STRUCTURE_INSPECTOR_VERSION,
        "received_at_hex": float(received_at).hex(),
        "schema": "stv3-metar-validation-verdict-v1",
        "status": "validated" if identity is not None else "deterministic_rejection",
        "verdict_id": verdict_id,
    }
    if identity is not None:
        document["identity"] = _identity_document(identity)
    else:
        safe_error = str(error)
        if not 1 <= len(safe_error) <= 512 or any(
            ord(character) < 0x20 for character in safe_error
        ):
            safe_error = "METAR-cache wijkt af van het vaste capturecontract"
        document["error"] = safe_error
    payload = _canonical_json_bytes(document)
    if len(payload) > MAX_VALIDATION_VERDICT_BYTES:
        raise ValueError("METAR validation verdict overschrijdt de bytegrens")
    path = _validation_verdict_path(
        store,
        content_sha256=content_sha256,
        received_at=received_at,
    )
    directory = path.parent
    with _deadline_file_lock(directory / "capacity.lock", deadline=deadline):
        existing = _load_validation_verdict(
            store,
            content_sha256=content_sha256,
            received_at=received_at,
        )
        if existing is None:
            temporary_path: Path | None = None
            try:
                with tempfile.NamedTemporaryFile(
                    mode="w+b",
                    prefix=".metar-verdict-",
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
            existing = _load_validation_verdict(
                store,
                content_sha256=content_sha256,
                received_at=received_at,
            )
        if existing is None:  # pragma: no cover
            raise RuntimeError("METAR validation verdict ontbreekt na publicatie")
        if (
            existing[0] != document["status"]
            or (
                existing[0] == "validated"
                and _identity_document(existing[1]) != document.get("identity")
            )
            or (
                existing[0] == "deterministic_rejection"
                and existing[1] != document.get("error")
            )
        ):
            raise ValueError("METAR validation verdict-ID heeft een conflict")
        candidates = sorted(
            (item.lstat().st_mtime_ns, item.name, item)
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


def _inspect_cache_cached(
    store: RawArtifactStore,
    payload: bytes,
    *,
    content_sha256: str,
    received_at: float,
    deadline: float,
) -> MetarCacheIdentity:
    verdict = _load_validation_verdict(
        store,
        content_sha256=content_sha256,
        received_at=received_at,
    )
    if verdict is not None:
        if verdict[0] == "validated":
            assert isinstance(verdict[1], MetarCacheIdentity)
            return verdict[1]
        raise ValueError(str(verdict[1]))
    try:
        identity = _inspect_cache(payload, received_at=received_at)
    except TimeoutError:
        raise
    except ValueError as err:
        _publish_validation_verdict(
            store,
            content_sha256=content_sha256,
            received_at=received_at,
            error=str(err),
            deadline=deadline,
        )
        raise
    _publish_validation_verdict(
        store,
        content_sha256=content_sha256,
        received_at=received_at,
        identity=identity,
        deadline=deadline,
    )
    return identity


def _native_provenance() -> dict[str, object]:
    return {
        "representation": NATIVE_REPRESENTATION,
        "capture_stage": NATIVE_CAPTURE_STAGE,
        "collector": METAR_RAW_COLLECTOR,
        "collector_version": METAR_RAW_COLLECTOR_VERSION,
        "structure_inspector_version": METAR_STRUCTURE_INSPECTOR_VERSION,
        "upstream_filename": "metars.cache.csv.gz",
        "declared_semantic_class": METAR_SEMANTIC_CLASS,
        "coverage": "worldwide_current_reports",
        "provider_update_cadence_seconds": 60,
        "meteorological_interpretation_verified_after_decode": False,
    }


def _source_reference(
    receipt: RawArtifactReceipt,
    identity: MetarCacheIdentity,
) -> dict[str, object]:
    return {
        "role": "csv_gzip",
        "raw_receipt_id": receipt.receipt_id,
        "raw_content_sha256": receipt.content_sha256,
        "raw_receipt_sha256": receipt.receipt_sha256,
        "fetch_id": receipt.fetch_id,
        "upstream_record_id": receipt.upstream_id,
        "received_at": receipt.received_at,
        "content_type": receipt.content_type,
        "upstream_filename": "metars.cache.csv.gz",
        "collector_version": str(receipt.provenance["collector_version"]),
        "structure_inspector_version": METAR_STRUCTURE_INSPECTOR_VERSION,
        "csv_row_count": identity.row_count,
        "structurally_valid_row_count": identity.structurally_valid_row_count,
        "invalid_timestamp_row_count": identity.invalid_timestamp_row_count,
        "current_timestamp_row_count": identity.current_timestamp_row_count,
        "stale_timestamp_row_count": identity.stale_timestamp_row_count,
        "future_timestamp_row_count": identity.future_timestamp_row_count,
        "declared_oldest_observation_timestamp": (
            identity.declared_oldest_observation_timestamp
        ),
        "declared_newest_observation_timestamp": (
            identity.declared_newest_observation_timestamp
        ),
        "timestamp_quality_state": METAR_TIMESTAMP_QUALITY_STATE,
        "semantic_class": METAR_SEMANTIC_CLASS,
    }


def _capture_provenance(
    capture: RawArtifactCaptureReceipt,
    *,
    receipt: RawArtifactReceipt,
    identity: MetarCacheIdentity,
) -> dict[str, object]:
    return {
        "raw_store_schema_version": capture.raw_store_schema_version,
        "raw_capture_id": capture.capture_id,
        "raw_capture_manifest_sha256": capture.manifest_sha256,
        "raw_capture_receipt_sha256": capture.capture_receipt_sha256,
        "raw_capture_fetch_id": capture.fetch_id,
        "upstream_capture_id": capture.upstream_capture_id,
        "product_timestamp": None,
        "collector_version": str(receipt.provenance["collector_version"]),
        "provenance_grade": "exact_raw",
        "provenance_scope": "upstream_gzip_bytes_only",
        "raw_sources": {
            "csv_gzip": _source_reference(receipt, identity),
        },
    }


def _capture_result(
    *,
    capture: RawArtifactCaptureReceipt,
    receipt: RawArtifactReceipt,
    identity: MetarCacheIdentity,
) -> dict[str, object]:
    return {
        "provider": "metar",
        "product": METAR_GZIP_PRODUCT,
        "semantic_class": METAR_SEMANTIC_CLASS,
        "coverage": "worldwide_current_reports",
        "decode_status": "not_meteorologically_decoded",
        "capture_state": "captured_not_decoded",
        "weather_state": "unknown",
        "timestamp_quality_state": METAR_TIMESTAMP_QUALITY_STATE,
        "operational_admission_eligible": False,
        "admission_status": METAR_ADMISSION_STATUS,
        "csv_row_count": identity.row_count,
        "structurally_valid_row_count": identity.structurally_valid_row_count,
        "invalid_timestamp_row_count": identity.invalid_timestamp_row_count,
        "current_timestamp_row_count": identity.current_timestamp_row_count,
        "stale_timestamp_row_count": identity.stale_timestamp_row_count,
        "future_timestamp_row_count": identity.future_timestamp_row_count,
        "declared_oldest_observation_timestamp": (
            identity.declared_oldest_observation_timestamp
        ),
        "declared_newest_observation_timestamp": (
            identity.declared_newest_observation_timestamp
        ),
        "raw_provenance": _capture_provenance(
            capture,
            receipt=receipt,
            identity=identity,
        ),
    }


def _validate_job_id(value: object) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value) > 480
        or any(ord(character) < 32 for character in value)
    ):
        raise ValueError("METAR capturejob mist geldige job_id")
    return value


def _receipt_id(store: RawArtifactStore, job_id: str) -> str:
    return store._receipt_id(  # noqa: SLF001 - internal worker store contract
        provider="metar",
        product=METAR_GZIP_PRODUCT,
        fetch_id=f"{job_id}:csv_gzip",
    )


def _capture_id(store: RawArtifactStore, job_id: str) -> str:
    return store._capture_id(  # noqa: SLF001 - internal worker store contract
        provider="metar",
        product=METAR_CAPTURE_PRODUCT,
        fetch_id=f"{job_id}:capture",
    )


def _validate_receipt(
    receipt: RawArtifactReceipt,
    *,
    job_id: str,
) -> None:
    expected_upstream = (
        "aviationweather/metars.cache.csv.gz"
        f"|content_sha256={receipt.content_sha256}"
    )
    if (
        receipt.provider != "metar"
        or receipt.product != METAR_GZIP_PRODUCT
        or receipt.fetch_id != f"{job_id}:csv_gzip"
        or receipt.upstream_id != expected_upstream
        or receipt.content_type not in METAR_ALLOWED_CONTENT_TYPES
        or receipt.size_bytes <= 0
        or receipt.size_bytes > MAX_COMPRESSED_BYTES
        or receipt.provenance != _native_provenance()
    ):
        raise RuntimeError("duurzaam METAR-receipt heeft een verkeerde identiteit")


def _commit_capture(
    store: RawArtifactStore,
    *,
    job_id: str,
    receipt: RawArtifactReceipt,
) -> RawArtifactCaptureReceipt:
    return store.commit_capture(
        provider="metar",
        product=METAR_CAPTURE_PRODUCT,
        fetch_id=f"{job_id}:capture",
        upstream_capture_id=(
            f"{METAR_CAPTURE_PRODUCT}|content_sha256={receipt.content_sha256}"
        ),
        # Independent station times cannot become one global product-valid time.
        product_timestamp=None,
        members={"csv_gzip": receipt.receipt_id},
    )


def _resume_existing(
    store: RawArtifactStore,
    *,
    job_id: str,
    deadline: float,
) -> dict[str, object] | None:
    _remaining_seconds(deadline)
    bundle = store.load_capture_bundle_bytes(_capture_id(store, job_id))
    if bundle is not None:
        capture, receipts, payloads = bundle
        if set(receipts) != {"csv_gzip"} or set(payloads) != {"csv_gzip"}:
            raise RuntimeError("METAR-capture bestaat zonder deterministisch receipt")
        receipt = receipts["csv_gzip"]
        exact_payload = payloads["csv_gzip"]
    else:
        capture = None
        loaded = store.load_receipt_bytes(_receipt_id(store, job_id))
        if loaded is None:
            return None
        receipt, exact_payload = loaded
    _validate_receipt(receipt, job_id=job_id)
    if capture is not None:
        expected_upstream = (
            f"{METAR_CAPTURE_PRODUCT}|content_sha256={receipt.content_sha256}"
        )
        members = {member.role: member.receipt_id for member in capture.members}
        if (
            capture.provider != "metar"
            or capture.product != METAR_CAPTURE_PRODUCT
            or capture.fetch_id != f"{job_id}:capture"
            or capture.upstream_capture_id != expected_upstream
            or capture.product_timestamp is not None
            or members != {"csv_gzip": receipt.receipt_id}
        ):
            raise RuntimeError("duurzame METAR-capture heeft een verkeerde identiteit")
    _remaining_seconds(deadline)
    identity = _inspect_cache_cached(
        store,
        exact_payload,
        content_sha256=receipt.content_sha256,
        received_at=receipt.received_at,
        deadline=deadline,
    )
    if capture is None:
        _remaining_seconds(deadline)
        capture = _commit_capture(store, job_id=job_id, receipt=receipt)
    _remaining_seconds(deadline)
    return _capture_result(
        capture=capture,
        receipt=receipt,
        identity=identity,
    )


def capture_latest_metar_cache(
    job,
    *,
    raw_store: RawArtifactStore | None = None,
) -> dict[str, object]:
    """Capture one global cache version, without making a local weather fact."""

    job_id = _validate_job_id(getattr(job, "job_id", None))
    payload: Any = getattr(job, "payload", None)
    if type(payload) is not dict or payload:
        raise ValueError("METAR native capture vereist een exact lege jobpayload")

    deadline = time.monotonic() + JOB_TOTAL_DEADLINE_SECONDS
    with _deadline_scope(deadline):
        store = _raw_store(raw_store)
        _remaining_seconds(deadline)
        with _deadline_thread_lock(_METAR_CAPTURE_LOCK, deadline=deadline):
            with _deadline_file_lock(
                _capture_capacity_lock_path(store),
                deadline=deadline,
            ), _deadline_file_lock(
                _slot_lock_path(store, job_id),
                deadline=deadline,
            ):
                resumed = _resume_existing(
                    store,
                    job_id=job_id,
                    deadline=deadline,
                )
                if resumed is not None:
                    return resumed

                response = _download_cache()
                _remaining_seconds(deadline)
                content_sha256 = hashlib.sha256(response.payload).hexdigest()
                receipt = store.store(
                    provider="metar",
                    product=METAR_GZIP_PRODUCT,
                    fetch_id=f"{job_id}:csv_gzip",
                    content_type=response.content_type,
                    received_at=response.received_at,
                    upstream_id=(
                        "aviationweather/metars.cache.csv.gz"
                        f"|content_sha256={content_sha256}"
                    ),
                    provenance=_native_provenance(),
                    payload=response.payload,
                )
                _remaining_seconds(deadline)

                # The exact gzip response is durable before decompression/CSV.
                identity = _inspect_cache_cached(
                    store,
                    response.payload,
                    content_sha256=receipt.content_sha256,
                    received_at=receipt.received_at,
                    deadline=deadline,
                )
                _remaining_seconds(deadline)
                capture = _commit_capture(store, job_id=job_id, receipt=receipt)
                _remaining_seconds(deadline)
                return _capture_result(
                    capture=capture,
                    receipt=receipt,
                    identity=identity,
                )


__all__ = [
    "METAR_ADMISSION_STATUS",
    "METAR_CAPTURE_PRODUCT",
    "METAR_GZIP_PRODUCT",
    "METAR_EXPECTED_HEADER",
    "METAR_SEMANTIC_CLASS",
    "MetarCacheIdentity",
    "capture_latest_metar_cache",
]
