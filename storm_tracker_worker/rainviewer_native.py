"""Fail-closed exact-byte capture for RainViewer public weather-map tiles.

RainViewer exposes a global composite-reflectivity *visualisation*.  It is not
an official quantitative rain-rate product and the published frame time is a
map generation time, not one common observation time for all contributing
radars.  This isolated candidate therefore retains the exact upstream bytes
without decoding them into rain, dry, cells, intensity or ground evidence.

One capture is deliberately scoped to one tracker region.  It contains the
exact weather-map manifest plus a 3x3 set of unsmoothed radar tiles and the
matching coverage tiles.  The latter are mandatory: a transparent radar tile
alone cannot distinguish no echo from no radar coverage.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict, dataclass
import errno
import hashlib
import json
import math
import os
from pathlib import Path
import re
import stat
import struct
import tempfile
import threading
import time
from typing import Any, Iterator, Mapping
from urllib.parse import urlsplit
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


RAINVIEWER_MANIFEST_URL = "https://api.rainviewer.com/public/weather-maps.json"
RAINVIEWER_TILE_HOST = "https://tilecache.rainviewer.com"
RAINVIEWER_ZOOM = 5
RAINVIEWER_TILE_SIZE = 256
RAINVIEWER_COLOR_SCHEME = 2
RAINVIEWER_SMOOTH = 0
RAINVIEWER_SNOW = 0

MAX_MANIFEST_BYTES = 2 * 1024 * 1024
MAX_TILE_BYTES = 2 * 1024 * 1024
MAX_MANIFEST_FRAMES = 64
HTTP_IO_TIMEOUT_SECONDS = 10.0
HTTP_TOTAL_DEADLINE_SECONDS = 30.0
CAPTURE_TOTAL_DEADLINE_SECONDS = 60.0
LOCK_POLL_SECONDS = 0.05
MAX_MANIFEST_JSON_DEPTH = 16
MAX_MANIFEST_JSON_OBJECTS = 4_096
MAX_VALIDATION_VERDICT_BYTES = 8 * 1024
MAX_VALIDATION_VERDICTS = 2_048

RAINVIEWER_RAW_SHARE_ROOT = Path("/share/storm_tracker_worker")
RAINVIEWER_RAW_ARTIFACT_ROOT = (
    RAINVIEWER_RAW_SHARE_ROOT / "raw" / "rainviewer_weather_maps"
)
RAINVIEWER_MANIFEST_PRODUCT = "rainviewer_weather_maps_manifest_json"
RAINVIEWER_RADAR_TILE_PRODUCT = "rainviewer_composite_reflectivity_png"
RAINVIEWER_COVERAGE_TILE_PRODUCT = "rainviewer_radar_coverage_png"
RAINVIEWER_CAPTURE_PRODUCT = "rainviewer_region_visual_capture"
RAINVIEWER_RAW_COLLECTOR = "rainviewer_weather_maps_exact_bytes"
RAINVIEWER_RAW_COLLECTOR_VERSION = "worker-rainviewer-exact-raw-v1"
RAINVIEWER_STRUCTURE_INSPECTOR_VERSION = "rainviewer-public-v2-structure-v1"
RAINVIEWER_SEMANTIC_CLASS = "categorical_composite_reflectivity_visualisation"
RAINVIEWER_ADMISSION_STATUS = "visualisation_only_not_quantitative_radar"
RAINVIEWER_TIMESTAMP_QUALITY_STATE = (
    "composite_frame_generation_time_not_uniform_observation_time"
)

_FRAME_PATH_RE = re.compile(
    r"^/v2/radar/(?:(?P<timestamp>[0-9]{10})|(?P<opaque>[0-9a-f]{12}))$"
)
_JOB_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,255}$")
_PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"
_OFFSETS = tuple(
    (dx, dy)
    for dx in (-1, 0, 1)
    for dy in (-1, 0, 1)
)
_RAW_STORE: RawArtifactStore | None = None
_RAW_STORE_LOCK = threading.Lock()
_CAPTURE_LOCK = threading.Lock()
_ACTIVE_DEADLINE = threading.local()
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


@dataclass(frozen=True, slots=True)
class RainviewerHttpResponse:
    """Exact HTTP response bytes plus bounded non-secret transport facts."""

    payload: bytes
    content_type: str
    received_at: float


@dataclass(frozen=True, slots=True)
class RainviewerManifestIdentity:
    """Strict identity of the selected public composite frame."""

    api_version: str
    manifest_generated_at: float
    tile_host: str
    frame_path: str
    frame_generation_timestamp: float


@dataclass(frozen=True, slots=True)
class RainviewerPngIdentity:
    width: int
    height: int
    bit_depth: int
    color_type: int


class _RejectRedirects(HTTPRedirectHandler):
    def redirect_request(self, *_args, **_kwargs):
        return None


def _open_without_redirects(request: Request, *, timeout: float):
    return build_opener(ProxyHandler({}), _RejectRedirects()).open(
        request, timeout=timeout
    )


def _safe_public_url(url: str, *, exact_host: str, exact_path: str) -> str:
    if not isinstance(url, str) or len(url) > 1024 or any(ord(c) < 32 for c in url):
        raise ValueError("RainViewer-URL is ongeldig")
    parsed = urlsplit(url)
    if (
        parsed.scheme != "https"
        or parsed.hostname != exact_host
        or parsed.port not in (None, 443)
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or parsed.path != exact_path
    ):
        raise ValueError("RainViewer-URL valt buiten het vaste publieke endpoint")
    return url


def _declared_content_length(headers: Any, *, maximum_bytes: int) -> int | None:
    value = headers.get("Content-Length") if headers is not None else None
    if value is None:
        return None
    normalized = str(value).strip()
    if re.fullmatch(r"[0-9]+", normalized) is None:
        raise ValueError("RainViewer Content-Length is ongeldig")
    declared = int(normalized)
    if declared > maximum_bytes:
        raise ValueError("RainViewer-payload overschrijdt de contractlimiet")
    return declared


def _read_bounded_body(
    response: Any,
    *,
    maximum_bytes: int,
    declared_bytes: int | None,
    deadline: float | None = None,
) -> bytes:
    body_deadline = min(
        time.monotonic() + HTTP_TOTAL_DEADLINE_SECONDS,
        deadline if deadline is not None else math.inf,
    )
    chunks: list[bytes] = []
    total = 0
    while total <= maximum_bytes:
        if time.monotonic() > body_deadline:
            raise TimeoutError("RainViewer-download overschrijdt de totale deadline")
        request_bytes = min(64 * 1024, maximum_bytes + 1 - total)
        chunk = response.read(request_bytes)
        if time.monotonic() > body_deadline:
            raise TimeoutError("RainViewer-download overschrijdt de totale deadline")
        if not chunk:
            break
        if not isinstance(chunk, (bytes, bytearray, memoryview)):
            raise TypeError("RainViewer-responsstream moet bytes opleveren")
        immutable = bytes(chunk)
        if len(immutable) > request_bytes:
            raise ValueError("RainViewer-responsstream overschrijdt de chunkgrens")
        chunks.append(immutable)
        total += len(immutable)
        if declared_bytes is not None and total > declared_bytes:
            raise ValueError("RainViewer-payloadlengte wijkt af")
        if total > maximum_bytes:
            raise ValueError("RainViewer-payload overschrijdt de contractlimiet")
    payload = b"".join(chunks)
    if declared_bytes is not None and len(payload) != declared_bytes:
        raise ValueError("RainViewer-payloadlengte wijkt af")
    return payload


def _download_exact(
    url: str,
    *,
    expected_host: str,
    expected_path: str,
    maximum_bytes: int,
    allowed_content_types: frozenset[str],
    opener=None,
    deadline: float | None = None,
) -> RainviewerHttpResponse:
    _safe_public_url(url, exact_host=expected_host, exact_path=expected_path)
    request = Request(
        url,
        headers={
            "Accept": ",".join(sorted(allowed_content_types)),
            "Accept-Encoding": "identity",
            "User-Agent": "StormTrackerWorker/rainviewer-native-candidate-v1",
        },
    )
    active_opener = _open_without_redirects if opener is None else opener
    remaining = _remaining_seconds(deadline)
    with active_opener(
        request,
        timeout=min(HTTP_IO_TIMEOUT_SECONDS, remaining),
    ) as response:
        if getattr(response, "status", None) != 200:
            raise ValueError("RainViewer-response heeft geen HTTP 200-status")
        geturl = getattr(response, "geturl", None)
        if not callable(geturl) or str(geturl()) != url:
            raise ValueError("RainViewer-response kwam van een onverwacht endpoint")
        headers = getattr(response, "headers", None)
        encoding = str(
            headers.get("Content-Encoding") or "identity"
            if headers is not None
            else "identity"
        ).lower()
        if len(encoding) > 64 or any(ord(character) < 0x20 for character in encoding):
            raise ValueError("RainViewer Content-Encoding-header is ongeldig")
        if encoding != "identity":
            raise ValueError("RainViewer-transportcompressie is niet toegestaan")
        raw_content_type = str(
            headers.get("Content-Type") or ""
            if headers is not None
            else ""
        )
        if len(raw_content_type) > 256 or any(
            ord(character) < 0x20 for character in raw_content_type
        ):
            raise ValueError("RainViewer Content-Type-header is ongeldig")
        content_type = raw_content_type.split(";", 1)[0].strip().lower()
        if content_type not in allowed_content_types:
            raise ValueError("RainViewer Content-Type valt buiten het contract")
        declared = _declared_content_length(
            headers, maximum_bytes=maximum_bytes
        )
        payload = _read_bounded_body(
            response,
            maximum_bytes=maximum_bytes,
            declared_bytes=declared,
            deadline=deadline,
        )
        return RainviewerHttpResponse(
            payload=payload,
            content_type=content_type,
            received_at=time.time(),
        )


def _download_manifest(*, deadline: float | None = None) -> RainviewerHttpResponse:
    parsed = urlsplit(RAINVIEWER_MANIFEST_URL)
    return _download_exact(
        RAINVIEWER_MANIFEST_URL,
        expected_host="api.rainviewer.com",
        expected_path=parsed.path,
        maximum_bytes=MAX_MANIFEST_BYTES,
        allowed_content_types=frozenset({"application/json", "text/json"}),
        deadline=deadline,
    )


def _download_tile(
    url: str, *, deadline: float | None = None,
) -> RainviewerHttpResponse:
    parsed = urlsplit(url)
    return _download_exact(
        url,
        expected_host="tilecache.rainviewer.com",
        expected_path=parsed.path,
        maximum_bytes=MAX_TILE_BYTES,
        allowed_content_types=frozenset({"image/png"}),
        deadline=deadline,
    )


def _remaining_seconds(deadline: float | None) -> float:
    if deadline is None:
        return HTTP_IO_TIMEOUT_SECONDS
    remaining = deadline - time.monotonic()
    if not math.isfinite(remaining) or remaining <= 0:
        raise TimeoutError("RainViewer-capture overschrijdt de jobbrede deadline")
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
        raise TimeoutError("RainViewer capturelock overschrijdt de jobdeadline")
    try:
        _remaining_seconds(deadline)
        yield
    finally:
        lock.release()


def _safe_auxiliary_directory(store: RawArtifactStore, name: str) -> Path:
    root = Path(store.root)
    if root.is_symlink() or not root.is_dir():
        raise ValueError("RainViewer auxiliary root is onveilig")
    directory = root / name
    existed = directory.exists()
    if existed and (directory.is_symlink() or not directory.is_dir()):
        raise ValueError("RainViewer auxiliary map is onveilig")
    directory.mkdir(mode=0o700, exist_ok=True)
    if directory.is_symlink() or directory.resolve().parent != root.resolve():
        raise ValueError("RainViewer auxiliary map ontsnapt uit raw root")
    if not existed and os.name != "nt":
        descriptor = os.open(root, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    return directory


def _slot_lock_path(store: RawArtifactStore, job_id: str) -> Path:
    token = hashlib.sha256(job_id.encode("utf-8", errors="strict")).hexdigest()
    return _safe_auxiliary_directory(store, "rainviewer_locks") / f"slot-{token}.lock"


def _capture_capacity_lock_path(store: RawArtifactStore) -> Path:
    return _safe_auxiliary_directory(store, "rainviewer_locks") / "capture-capacity.lock"


@contextmanager
def _deadline_file_lock(path: Path, *, deadline: float) -> Iterator[None]:
    _remaining_seconds(deadline)
    if path.exists() and (path.is_symlink() or not path.is_file()):
        raise ValueError("RainViewer lockpad is onveilig")
    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_BINARY", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags, 0o600)
    acquired = False
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode):
            raise ValueError("RainViewer lockpad is geen regulier bestand")
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
    count = 0
    while pending:
        current, depth = pending.pop()
        count += 1
        if count > MAX_MANIFEST_JSON_OBJECTS:
            raise ValueError("RainViewer-manifest bevat te veel JSON-objecten")
        if depth > MAX_MANIFEST_JSON_DEPTH:
            raise ValueError("RainViewer-manifest is te diep genest")
        if isinstance(current, dict):
            pending.extend((item, depth + 1) for item in current.values())
        elif isinstance(current, list):
            pending.extend((item, depth + 1) for item in current)


def _json_without_duplicates(payload: bytes) -> Any:
    try:
        text = payload.decode("utf-8", errors="strict")
    except UnicodeDecodeError as err:
        raise ValueError("RainViewer-manifest is geen geldige UTF-8") from err

    def object_pairs(pairs):
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("RainViewer-manifest bevat dubbele JSON-sleutels")
            result[key] = value
        return result

    def invalid_constant(_value: str):
        raise ValueError("RainViewer-manifest bevat niet-eindige JSON-getallen")

    try:
        decoded = json.loads(
            text,
            object_pairs_hook=object_pairs,
            parse_constant=invalid_constant,
        )
    except json.JSONDecodeError as err:
        raise ValueError("RainViewer-manifest bevat ongeldige JSON") from err
    _validate_json_resources(decoded)
    return decoded


def _strict_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"RainViewer {name} is geen positief geheel getal")
    return value


def _unix_timestamp(value: Any, name: str) -> int:
    result = _strict_int(value, name)
    # A public v2 weather-map timestamp outside this deliberately broad
    # operational epoch is structural drift, not a value to coerce to float.
    if not 946_684_800 <= result <= 4_102_444_800:
        raise ValueError(f"RainViewer {name} valt buiten het tijdcontract")
    return result


def _inspect_manifest(payload: bytes) -> RainviewerManifestIdentity:
    if not isinstance(payload, bytes) or not 1 <= len(payload) <= MAX_MANIFEST_BYTES:
        raise ValueError("RainViewer-manifest overschrijdt de vaste objectgrens")
    active = getattr(_ACTIVE_DEADLINE, "value", None)
    if active is not None:
        _remaining_seconds(active)
    document = _json_without_duplicates(payload)
    if not isinstance(document, dict):
        raise ValueError("RainViewer-manifestroot is geen object")
    version = document.get("version")
    if not isinstance(version, str) or not re.fullmatch(r"2(?:\.[0-9]+){0,2}", version):
        raise ValueError("RainViewer-manifest gebruikt geen ondersteunde v2-API")
    generated = _unix_timestamp(document.get("generated"), "generated")
    host = document.get("host")
    if host != RAINVIEWER_TILE_HOST:
        raise ValueError("RainViewer-manifest wijst niet naar de vaste tilehost")
    radar = document.get("radar")
    if not isinstance(radar, dict):
        raise ValueError("RainViewer-manifest mist het radarobject")
    frames = radar.get("past")
    if not isinstance(frames, list) or not 1 <= len(frames) <= MAX_MANIFEST_FRAMES:
        raise ValueError("RainViewer-manifest heeft geen begrensde past-reeks")

    normalized: list[tuple[int, str]] = []
    seen_times: set[int] = set()
    seen_paths: set[str] = set()
    for frame in frames:
        if active is not None:
            _remaining_seconds(active)
        if not isinstance(frame, dict) or set(frame) != {"time", "path"}:
            raise ValueError("RainViewer-frame heeft een onverwachte structuur")
        timestamp = _unix_timestamp(frame["time"], "frame.time")
        path = frame["path"]
        if not isinstance(path, str) or len(path) > 255:
            raise ValueError("RainViewer-framepad is ongeldig")
        match = _FRAME_PATH_RE.fullmatch(path)
        if match is None:
            raise ValueError("RainViewer-framepad valt buiten het publieke v2-contract")
        path_timestamp = match.group("timestamp")
        if path_timestamp is not None and int(path_timestamp) != timestamp:
            raise ValueError("RainViewer-framepad en generatietijd spreken elkaar tegen")
        if timestamp in seen_times:
            raise ValueError("RainViewer-manifest bevat een dubbele frametijd")
        if path in seen_paths:
            raise ValueError("RainViewer-manifest bevat een dubbel framepad")
        seen_times.add(timestamp)
        seen_paths.add(path)
        normalized.append((timestamp, path))
    timestamp, path = max(normalized)
    if timestamp > generated + 300 or generated - timestamp > 3_600:
        raise ValueError("RainViewer nieuwste frame en manifesttijd zijn inconsistent")
    return RainviewerManifestIdentity(
        api_version=version,
        manifest_generated_at=float(generated),
        tile_host=host,
        frame_path=path,
        frame_generation_timestamp=float(timestamp),
    )


def _inspect_png(payload: bytes) -> RainviewerPngIdentity:
    if (
        not isinstance(payload, bytes)
        or not 45 <= len(payload) <= MAX_TILE_BYTES
        or payload[:8] != _PNG_SIGNATURE
    ):
        raise ValueError("RainViewer-tegel is geen geldige PNG-container")
    active = getattr(_ACTIVE_DEADLINE, "value", None)
    offset = 8
    chunks = 0
    saw_idat = False
    ihdr: bytes | None = None
    while offset < len(payload):
        if active is not None:
            _remaining_seconds(active)
        chunks += 1
        if chunks > 256 or offset + 12 > len(payload):
            raise ValueError("RainViewer-PNG heeft een onbegrensde chunkstructuur")
        length = struct.unpack(">I", payload[offset:offset + 4])[0]
        chunk_type = payload[offset + 4:offset + 8]
        end = offset + 12 + length
        if length > MAX_TILE_BYTES or end > len(payload):
            raise ValueError("RainViewer-PNG-chunk valt buiten de payload")
        data = payload[offset + 8:offset + 8 + length]
        declared_crc = struct.unpack(">I", payload[offset + 8 + length:end])[0]
        actual_crc = zlib.crc32(chunk_type + data) & 0xFFFFFFFF
        if declared_crc != actual_crc:
            raise ValueError("RainViewer-PNG heeft een ongeldige chunkchecksum")
        if chunks == 1:
            if chunk_type != b"IHDR" or length != 13:
                raise ValueError("RainViewer-PNG mist een canonieke IHDR")
            ihdr = data
        elif chunk_type == b"IHDR":
            raise ValueError("RainViewer-PNG bevat meerdere IHDR-chunks")
        if chunk_type == b"IDAT":
            saw_idat = True
        if chunk_type == b"IEND":
            if length != 0 or end != len(payload):
                raise ValueError("RainViewer-PNG heeft een ongeldige IEND")
            break
        if chunk_type and 65 <= chunk_type[0] <= 90 and chunk_type not in {
            b"IHDR", b"PLTE", b"IDAT", b"IEND"
        }:
            raise ValueError("RainViewer-PNG bevat een onbekende kritieke chunk")
        offset = end
    else:
        raise ValueError("RainViewer-PNG mist IEND")
    if ihdr is None or not saw_idat:
        raise ValueError("RainViewer-PNG mist verplichte beelddata")
    width, height, bit_depth, color_type, compression, filtering, interlace = (
        struct.unpack(">IIBBBBB", ihdr)
    )
    if width != RAINVIEWER_TILE_SIZE or height != RAINVIEWER_TILE_SIZE:
        raise ValueError("RainViewer-PNG heeft niet de gevraagde tegelafmetingen")
    if bit_depth != 8 or color_type not in {4, 6}:
        raise ValueError("RainViewer-PNG heeft een onverwacht alfakanaalcontract")
    if compression != 0 or filtering != 0 or interlace not in {0, 1}:
        raise ValueError("RainViewer-PNG gebruikt een onverwachte IHDR-codering")
    return RainviewerPngIdentity(width, height, bit_depth, color_type)


def _canonical_json_bytes(value: dict[str, object]) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")


def _identity_document(
    identity: RainviewerManifestIdentity | RainviewerPngIdentity,
) -> dict[str, object]:
    return asdict(identity)


def _identity_from_document(
    kind: str,
    value: Any,
) -> RainviewerManifestIdentity | RainviewerPngIdentity:
    if kind == "manifest":
        fields = {
            "api_version",
            "manifest_generated_at",
            "tile_host",
            "frame_path",
            "frame_generation_timestamp",
        }
        if not isinstance(value, dict) or set(value) != fields:
            raise ValueError("RainViewer manifestverdict bevat identity-schema drift")
        if (
            not isinstance(value["api_version"], str)
            or re.fullmatch(r"2(?:\.[0-9]+){0,2}", value["api_version"]) is None
            or value["tile_host"] != RAINVIEWER_TILE_HOST
            or not isinstance(value["frame_path"], str)
            or _FRAME_PATH_RE.fullmatch(value["frame_path"]) is None
        ):
            raise ValueError("RainViewer manifestverdict bevat ongeldige identiteit")
        for field in ("manifest_generated_at", "frame_generation_timestamp"):
            if isinstance(value[field], bool) or not isinstance(value[field], (int, float)):
                raise ValueError("RainViewer manifestverdict bevat een ongeldige tijd")
            if not math.isfinite(float(value[field])):
                raise ValueError("RainViewer manifestverdict bevat een ongeldige tijd")
        identity = RainviewerManifestIdentity(
            api_version=value["api_version"],
            manifest_generated_at=float(value["manifest_generated_at"]),
            tile_host=value["tile_host"],
            frame_path=value["frame_path"],
            frame_generation_timestamp=float(value["frame_generation_timestamp"]),
        )
        if (
            identity.frame_generation_timestamp > identity.manifest_generated_at + 300
            or identity.manifest_generated_at - identity.frame_generation_timestamp > 3_600
        ):
            raise ValueError("RainViewer manifestverdict bevat inconsistente tijden")
        return identity
    if kind == "png":
        fields = {"width", "height", "bit_depth", "color_type"}
        if (
            not isinstance(value, dict)
            or set(value) != fields
            or any(
                isinstance(value[field], bool)
                or not isinstance(value[field], int)
                for field in fields
            )
        ):
            raise ValueError("RainViewer PNG-verdict bevat identity-schema drift")
        identity = RainviewerPngIdentity(**value)
        if (
            identity.width != RAINVIEWER_TILE_SIZE
            or identity.height != RAINVIEWER_TILE_SIZE
            or identity.bit_depth != 8
            or identity.color_type not in {4, 6}
        ):
            raise ValueError("RainViewer PNG-verdict past niet bij het tegelcontract")
        return identity
    raise ValueError("RainViewer validation verdict heeft een onbekende soort")


def _validation_verdict_id(kind: str, content_sha256: str) -> str:
    if kind not in {"manifest", "png"} or _SHA256_RE.fullmatch(content_sha256) is None:
        raise ValueError("RainViewer validation verdictsleutel is ongeldig")
    key = {
        "content_sha256": content_sha256,
        "inspector_version": RAINVIEWER_STRUCTURE_INSPECTOR_VERSION,
        "kind": kind,
        "schema": "stv3-rainviewer-validation-key-v1",
    }
    return hashlib.sha256(_canonical_json_bytes(key)).hexdigest()


def _validation_verdict_path(
    store: RawArtifactStore,
    *,
    kind: str,
    content_sha256: str,
) -> Path:
    verdict_id = _validation_verdict_id(kind, content_sha256)
    return _safe_auxiliary_directory(store, "rainviewer_validation_verdicts") / (
        f"{verdict_id}.json"
    )


def _load_validation_verdict(
    store: RawArtifactStore,
    *,
    kind: str,
    content_sha256: str,
) -> tuple[str, RainviewerManifestIdentity | RainviewerPngIdentity | str] | None:
    path = _validation_verdict_path(
        store,
        kind=kind,
        content_sha256=content_sha256,
    )
    if not path.exists():
        return None
    info = path.lstat()
    if path.is_symlink() or not stat.S_ISREG(info.st_mode):
        raise ValueError("RainViewer validation verdictpad is onveilig")
    if not 1 <= info.st_size <= MAX_VALIDATION_VERDICT_BYTES:
        raise ValueError("RainViewer validation verdictgrootte is ongeldig")
    payload = path.read_bytes()
    if len(payload) != info.st_size:
        raise ValueError("RainViewer validation verdict veranderde tijdens lezen")
    try:
        document = json.loads(payload.decode("ascii", errors="strict"))
    except (UnicodeDecodeError, json.JSONDecodeError) as err:
        raise ValueError("RainViewer validation verdict is geen canonieke JSON") from err
    status = document.get("status") if isinstance(document, dict) else None
    common = {
        "content_sha256",
        "inspector_version",
        "kind",
        "schema",
        "status",
        "verdict_id",
    }
    expected = common | ({"identity"} if status == "validated" else {"error"})
    if (
        not isinstance(document, dict)
        or set(document) != expected
        or document.get("schema") != "stv3-rainviewer-validation-verdict-v1"
        or document.get("inspector_version") != RAINVIEWER_STRUCTURE_INSPECTOR_VERSION
        or document.get("kind") != kind
        or document.get("content_sha256") != content_sha256
        or document.get("verdict_id") != _validation_verdict_id(kind, content_sha256)
        or payload != _canonical_json_bytes(document)
    ):
        raise ValueError("RainViewer validation verdict past niet bij de bronbytes")
    if status == "validated":
        return status, _identity_from_document(kind, document["identity"])
    error = document.get("error")
    if (
        status != "deterministic_rejection"
        or not isinstance(error, str)
        or not 1 <= len(error) <= 512
        or any(ord(character) < 0x20 for character in error)
    ):
        raise ValueError("RainViewer rejection verdict is ongeldig")
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
    kind: str,
    content_sha256: str,
    identity: RainviewerManifestIdentity | RainviewerPngIdentity | None = None,
    error: str | None = None,
    deadline: float,
) -> tuple[str, RainviewerManifestIdentity | RainviewerPngIdentity | str]:
    if (identity is None) == (error is None):
        raise ValueError("RainViewer validation verdict mist een eenduidige status")
    verdict_id = _validation_verdict_id(kind, content_sha256)
    document: dict[str, object] = {
        "content_sha256": content_sha256,
        "inspector_version": RAINVIEWER_STRUCTURE_INSPECTOR_VERSION,
        "kind": kind,
        "schema": "stv3-rainviewer-validation-verdict-v1",
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
            safe_error = "RainViewer-bronbytes wijken af van het capturecontract"
        document["error"] = safe_error
    payload = _canonical_json_bytes(document)
    if len(payload) > MAX_VALIDATION_VERDICT_BYTES:
        raise ValueError("RainViewer validation verdict overschrijdt de bytegrens")
    path = _validation_verdict_path(store, kind=kind, content_sha256=content_sha256)
    directory = path.parent
    with _deadline_file_lock(directory / "capacity.lock", deadline=deadline):
        existing = _load_validation_verdict(
            store,
            kind=kind,
            content_sha256=content_sha256,
        )
        if existing is None:
            temporary_path: Path | None = None
            try:
                with tempfile.NamedTemporaryFile(
                    mode="w+b",
                    prefix=".rainviewer-verdict-",
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
                kind=kind,
                content_sha256=content_sha256,
            )
        if existing is None:  # pragma: no cover
            raise RuntimeError("RainViewer validation verdict ontbreekt na publicatie")
        if existing[0] != document["status"]:
            raise ValueError("RainViewer validation verdict-ID heeft een conflict")
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


def _inspect_manifest_cached(
    store: RawArtifactStore,
    payload: bytes,
    *,
    content_sha256: str,
    deadline: float,
) -> RainviewerManifestIdentity:
    verdict = _load_validation_verdict(
        store,
        kind="manifest",
        content_sha256=content_sha256,
    )
    if verdict is not None:
        if verdict[0] == "validated":
            assert isinstance(verdict[1], RainviewerManifestIdentity)
            return verdict[1]
        raise ValueError(str(verdict[1]))
    try:
        identity = _inspect_manifest(payload)
    except TimeoutError:
        raise
    except ValueError as err:
        _publish_validation_verdict(
            store,
            kind="manifest",
            content_sha256=content_sha256,
            error=str(err),
            deadline=deadline,
        )
        raise
    _publish_validation_verdict(
        store,
        kind="manifest",
        content_sha256=content_sha256,
        identity=identity,
        deadline=deadline,
    )
    return identity


def _inspect_png_cached(
    store: RawArtifactStore,
    payload: bytes,
    *,
    content_sha256: str,
    deadline: float,
) -> RainviewerPngIdentity:
    verdict = _load_validation_verdict(
        store,
        kind="png",
        content_sha256=content_sha256,
    )
    if verdict is not None:
        if verdict[0] == "validated":
            assert isinstance(verdict[1], RainviewerPngIdentity)
            return verdict[1]
        raise ValueError(str(verdict[1]))
    try:
        identity = _inspect_png(payload)
    except TimeoutError:
        raise
    except ValueError as err:
        _publish_validation_verdict(
            store,
            kind="png",
            content_sha256=content_sha256,
            error=str(err),
            deadline=deadline,
        )
        raise
    _publish_validation_verdict(
        store,
        kind="png",
        content_sha256=content_sha256,
        identity=identity,
        deadline=deadline,
    )
    return identity


def _raw_store(injected: RawArtifactStore | None = None) -> RawArtifactStore:
    if injected is not None:
        injected.initialize()
        return injected
    share_root = RAINVIEWER_RAW_SHARE_ROOT.resolve(strict=False)
    raw_root = RAINVIEWER_RAW_ARTIFACT_ROOT.resolve(strict=False)
    if raw_root == share_root or share_root not in raw_root.parents:
        raise RuntimeError("RainViewer raw-artifactpad ontsnapt uit worker-share")
    global _RAW_STORE
    with _RAW_STORE_LOCK:
        if _RAW_STORE is None:
            candidate = RawArtifactStore(raw_root)
            candidate.initialize()
            _RAW_STORE = candidate
        return _RAW_STORE


def _job_identity(job: Any) -> tuple[str, float, float, float]:
    job_id = str(getattr(job, "job_id", "") or "")
    if _JOB_ID_RE.fullmatch(job_id) is None:
        raise ValueError("RainViewer workerjob mist een veilig job_id")
    payload = getattr(job, "payload", None)
    if not isinstance(payload, dict) or set(payload) != {"regions"}:
        raise ValueError("RainViewer workerjobpayload moet exact regions bevatten")
    regions = payload["regions"]
    if not isinstance(regions, (list, tuple)) or len(regions) != 1:
        raise ValueError("RainViewer raw-candidate vereist exact één regio")
    region = regions[0]
    if not isinstance(region, Mapping):
        raise ValueError("RainViewer-regio is geen object")
    if set(region) != {"center_lat", "center_lon", "horizon_km"}:
        raise ValueError("RainViewer-regio heeft onverwachte velden")
    lat = region["center_lat"]
    lon = region["center_lon"]
    if (
        isinstance(lat, bool)
        or isinstance(lon, bool)
        or not isinstance(lat, (int, float))
        or not isinstance(lon, (int, float))
        or not math.isfinite(float(lat))
        or not math.isfinite(float(lon))
        or not -85.05112878 < float(lat) < 85.05112878
        or not -180.0 <= float(lon) < 180.0
    ):
        raise ValueError("RainViewer-regiocoördinaten vallen buiten Web Mercator")
    horizon = region["horizon_km"]
    if (
        isinstance(horizon, bool)
        or not isinstance(horizon, (int, float))
        or not math.isfinite(float(horizon))
        or not 0.0 < float(horizon) <= 1000.0
    ):
        raise ValueError("RainViewer-horizon is ongeldig")
    return job_id, float(lat), float(lon), float(horizon)


def _center_tile(lat: float, lon: float) -> tuple[int, int]:
    count = 2**RAINVIEWER_ZOOM
    x = int(math.floor((lon + 180.0) / 360.0 * count)) % count
    latitude = math.radians(lat)
    y = int(math.floor(
        (1.0 - math.asinh(math.tan(latitude)) / math.pi) / 2.0 * count
    ))
    if not 1 <= y < count - 1:
        raise ValueError("RainViewer 3x3-capture ondersteunt deze poolrand nog niet")
    return x, y


def _offset_token(value: int) -> str:
    return "m1" if value == -1 else "p1" if value == 1 else "0"


def _tile_role(kind: str, dx: int, dy: int) -> str:
    return f"{kind}_dx_{_offset_token(dx)}_dy_{_offset_token(dy)}"


def _tile_coordinates(center_x: int, center_y: int, dx: int, dy: int) -> tuple[int, int]:
    count = 2**RAINVIEWER_ZOOM
    return (center_x + dx) % count, center_y + dy


def _tile_latitude(y: int) -> float:
    count = 2**RAINVIEWER_ZOOM
    return math.degrees(math.atan(math.sinh(math.pi * (1.0 - 2.0 * y / count))))


def _wrapped_longitude_delta(first: float, second: float) -> float:
    return (second - first + 180.0) % 360.0 - 180.0


def _guaranteed_coverage_radius_km(
    *,
    lat: float,
    lon: float,
    center_x: int,
    center_y: int,
) -> float:
    """Conservative distance from the tracker to the 3x3 union boundary."""

    count = 2**RAINVIEWER_ZOOM
    left_lon = ((center_x - 1) / count * 360.0) - 180.0
    right_lon = ((center_x + 2) / count * 360.0) - 180.0
    top_lat = _tile_latitude(center_y - 1)
    bottom_lat = _tile_latitude(center_y + 2)
    radius = 6371.0088
    north = radius * math.radians(top_lat - lat)
    south = radius * math.radians(lat - bottom_lat)

    def meridian_distance(boundary_lon: float) -> float:
        delta = abs(math.radians(_wrapped_longitude_delta(lon, boundary_lon)))
        value = min(1.0, abs(math.cos(math.radians(lat)) * math.sin(delta)))
        return radius * math.asin(value)

    west = meridian_distance(left_lon)
    east = meridian_distance(right_lon)
    # Five percent is retained as a projection/rounding safety margin.  This
    # number describes capture coverage only, never radar availability.
    return max(0.0, min(north, south, west, east) * 0.95)


def _tile_url(
    identity: RainviewerManifestIdentity,
    *,
    kind: str,
    x: int,
    y: int,
) -> str:
    if kind == "radar":
        suffix = (
            f"/{RAINVIEWER_TILE_SIZE}/{RAINVIEWER_ZOOM}/{x}/{y}/"
            f"{RAINVIEWER_COLOR_SCHEME}/{RAINVIEWER_SMOOTH}_{RAINVIEWER_SNOW}.png"
        )
        path = f"{identity.frame_path}{suffix}"
    elif kind == "coverage":
        path = (
            f"/v2/coverage/0/{RAINVIEWER_TILE_SIZE}/{RAINVIEWER_ZOOM}/"
            f"{x}/{y}/0/0_0.png"
        )
    else:  # pragma: no cover - internal invariant
        raise ValueError("onbekende RainViewer-tegelsoort")
    url = f"{identity.tile_host}{path}"
    _safe_public_url(
        url,
        exact_host="tilecache.rainviewer.com",
        exact_path=path,
    )
    return url


def _manifest_provenance() -> dict[str, object]:
    return {
        "representation": NATIVE_REPRESENTATION,
        "capture_stage": NATIVE_CAPTURE_STAGE,
        "collector": RAINVIEWER_RAW_COLLECTOR,
        "collector_version": RAINVIEWER_RAW_COLLECTOR_VERSION,
        "response_role": "weather_maps_manifest",
        "source_host": "api.rainviewer.com",
        "source_path": "/public/weather-maps.json",
    }


def _manifest_upstream_id(payload: bytes) -> str:
    return (
        "rainviewer-public-weather-maps-v2-manifest-sha256-"
        + hashlib.sha256(payload).hexdigest()
    )


def _tile_provenance(
    identity: RainviewerManifestIdentity,
    *,
    kind: str,
    x: int,
    y: int,
    center_lat: float,
    center_lon: float,
    horizon_km: float,
) -> dict[str, object]:
    return {
        "representation": NATIVE_REPRESENTATION,
        "capture_stage": NATIVE_CAPTURE_STAGE,
        "collector": RAINVIEWER_RAW_COLLECTOR,
        "collector_version": RAINVIEWER_RAW_COLLECTOR_VERSION,
        "response_role": f"{kind}_tile",
        "source_host": "tilecache.rainviewer.com",
        "frame_path": identity.frame_path if kind == "radar" else None,
        "frame_generation_timestamp": identity.frame_generation_timestamp,
        "tile_size": RAINVIEWER_TILE_SIZE,
        "zoom": RAINVIEWER_ZOOM,
        "x": x,
        "y": y,
        "color_scheme": RAINVIEWER_COLOR_SCHEME if kind == "radar" else 0,
        "smooth": RAINVIEWER_SMOOTH if kind == "radar" else 0,
        "snow": RAINVIEWER_SNOW if kind == "radar" else 0,
        "request_center_lat": center_lat,
        "request_center_lon": center_lon,
        "request_horizon_km": horizon_km,
    }


def _tile_upstream_id(
    identity: RainviewerManifestIdentity,
    *,
    kind: str,
    x: int,
    y: int,
) -> str:
    frame_identity = identity.frame_path.rsplit("/", 1)[-1]
    return (
        f"rainviewer-{kind}-frame-{int(identity.frame_generation_timestamp)}"
        f"-id-{frame_identity if kind == 'radar' else 'coverage-v2'}"
        f"-z{RAINVIEWER_ZOOM}-x{x}-y{y}"
    )


def _assert_receipt_contract(
    receipt: RawArtifactReceipt,
    *,
    product: str,
    fetch_id: str,
    upstream_id: str,
    content_types: frozenset[str],
    provenance: Mapping[str, object],
) -> None:
    maximum = MAX_MANIFEST_BYTES if product == RAINVIEWER_MANIFEST_PRODUCT else MAX_TILE_BYTES
    if (
        receipt.provider != "rainviewer"
        or receipt.product != product
        or receipt.fetch_id != fetch_id
        or receipt.upstream_id != upstream_id
        or receipt.content_type not in content_types
        or not 1 <= receipt.size_bytes <= maximum
        or receipt.provenance != provenance
    ):
        raise ValueError("RainViewer hervatte receipt past niet bij het broncontract")


def _load_receipt_bytes(
    store: RawArtifactStore,
    *,
    product: str,
    fetch_id: str,
) -> tuple[RawArtifactReceipt, bytes] | None:
    receipt_id = RawArtifactStore._receipt_id(
        provider="rainviewer", product=product, fetch_id=fetch_id
    )
    return store.load_receipt_bytes(receipt_id)


def _store_or_load_manifest(
    store: RawArtifactStore,
    *,
    job_id: str,
    deadline: float,
) -> tuple[RawArtifactReceipt, bytes]:
    fetch_id = f"{job_id}:manifest"
    loaded = _load_receipt_bytes(
        store, product=RAINVIEWER_MANIFEST_PRODUCT, fetch_id=fetch_id
    )
    if loaded is None:
        _remaining_seconds(deadline)
        response = _download_manifest(deadline=deadline)
        receipt = store.store(
            provider="rainviewer",
            product=RAINVIEWER_MANIFEST_PRODUCT,
            fetch_id=fetch_id,
            content_type=response.content_type,
            received_at=response.received_at,
            upstream_id=_manifest_upstream_id(response.payload),
            provenance=_manifest_provenance(),
            payload=response.payload,
        )
        exact = response.payload
    else:
        receipt, exact = loaded
    _assert_receipt_contract(
        receipt,
        product=RAINVIEWER_MANIFEST_PRODUCT,
        fetch_id=fetch_id,
        upstream_id=_manifest_upstream_id(exact),
        content_types=frozenset({"application/json", "text/json"}),
        provenance=_manifest_provenance(),
    )
    return receipt, exact


def _store_or_load_tile(
    store: RawArtifactStore,
    *,
    job_id: str,
    role: str,
    kind: str,
    identity: RainviewerManifestIdentity,
    x: int,
    y: int,
    center_lat: float,
    center_lon: float,
    horizon_km: float,
    deadline: float,
) -> tuple[RawArtifactReceipt, bytes]:
    product = (
        RAINVIEWER_RADAR_TILE_PRODUCT
        if kind == "radar"
        else RAINVIEWER_COVERAGE_TILE_PRODUCT
    )
    fetch_id = f"{job_id}:{role}"
    loaded = _load_receipt_bytes(store, product=product, fetch_id=fetch_id)
    expected_provenance = _tile_provenance(
        identity,
        kind=kind,
        x=x,
        y=y,
        center_lat=center_lat,
        center_lon=center_lon,
        horizon_km=horizon_km,
    )
    expected_upstream_id = _tile_upstream_id(
        identity, kind=kind, x=x, y=y
    )
    if loaded is None:
        url = _tile_url(identity, kind=kind, x=x, y=y)
        _remaining_seconds(deadline)
        response = _download_tile(url, deadline=deadline)
        receipt = store.store(
            provider="rainviewer",
            product=product,
            fetch_id=fetch_id,
            content_type=response.content_type,
            received_at=response.received_at,
            upstream_id=expected_upstream_id,
            provenance=expected_provenance,
            payload=response.payload,
        )
        exact = response.payload
    else:
        receipt, exact = loaded
    _assert_receipt_contract(
        receipt,
        product=product,
        fetch_id=fetch_id,
        upstream_id=expected_upstream_id,
        content_types=frozenset({"image/png"}),
        provenance=expected_provenance,
    )
    return receipt, exact


def _source_reference(
    receipt: RawArtifactReceipt,
    *,
    role: str,
    kind: str,
    x: int | None = None,
    y: int | None = None,
) -> dict[str, object]:
    value: dict[str, object] = {
        "role": role,
        "kind": kind,
        "raw_receipt_id": receipt.receipt_id,
        "raw_content_sha256": receipt.content_sha256,
        "raw_receipt_sha256": receipt.receipt_sha256,
        "fetch_id": receipt.fetch_id,
        "upstream_record_id": receipt.upstream_id,
        "received_at": receipt.received_at,
        "content_type": receipt.content_type,
        "collector_version": RAINVIEWER_RAW_COLLECTOR_VERSION,
    }
    if x is not None and y is not None:
        value.update({"zoom": RAINVIEWER_ZOOM, "x": x, "y": y})
    return value


def _capture_provenance(
    capture: RawArtifactCaptureReceipt,
    *,
    identity: RainviewerManifestIdentity,
    sources: dict[str, dict[str, object]],
    center_lat: float,
    center_lon: float,
    horizon_km: float,
    guaranteed_coverage_radius_km: float,
) -> dict[str, object]:
    return {
        "raw_store_schema_version": capture.raw_store_schema_version,
        "raw_capture_id": capture.capture_id,
        "raw_capture_manifest_sha256": capture.manifest_sha256,
        "raw_capture_receipt_sha256": capture.capture_receipt_sha256,
        "raw_capture_fetch_id": capture.fetch_id,
        "upstream_capture_id": capture.upstream_capture_id,
        "frame_generation_timestamp": identity.frame_generation_timestamp,
        "manifest_generated_at": identity.manifest_generated_at,
        "collector_version": RAINVIEWER_RAW_COLLECTOR_VERSION,
        "structure_inspector_version": RAINVIEWER_STRUCTURE_INSPECTOR_VERSION,
        "provenance_grade": "exact_upstream_visualisation_bytes",
        "provenance_scope": "manifest_plus_unsmoothed_radar_and_coverage_tiles",
        "request_region": {
            "center_lat": center_lat,
            "center_lon": center_lon,
            "horizon_km": horizon_km,
            "guaranteed_capture_coverage_radius_km": guaranteed_coverage_radius_km,
        },
        "raw_sources": sources,
    }


def _capture_id(store: RawArtifactStore, job_id: str) -> str:
    return store._capture_id(  # noqa: SLF001 - deterministic raw-store contract
        provider="rainviewer",
        product=RAINVIEWER_CAPTURE_PRODUCT,
        fetch_id=f"{job_id}:capture",
    )


def _upstream_capture_id(
    identity: RainviewerManifestIdentity,
    *,
    center_x: int,
    center_y: int,
    lat: float,
    lon: float,
    horizon_km: float,
) -> str:
    return (
        f"rainviewer-region-frame-{int(identity.frame_generation_timestamp)}"
        f"-id-{identity.frame_path.rsplit('/', 1)[-1]}"
        f"-z{RAINVIEWER_ZOOM}-center-x{center_x}-y{center_y}"
        f"-lat{lat:.8f}-lon{lon:.8f}-horizon{horizon_km:.3f}km"
    )


def _capture_result(
    *,
    capture: RawArtifactCaptureReceipt,
    identity: RainviewerManifestIdentity,
    sources: dict[str, dict[str, object]],
    lat: float,
    lon: float,
    horizon_km: float,
    guaranteed_radius: float,
) -> dict[str, object]:
    return {
        "provider": "rainviewer",
        "product": RAINVIEWER_CAPTURE_PRODUCT,
        "declared_semantic_class": RAINVIEWER_SEMANTIC_CLASS,
        "frame_generation_timestamp": identity.frame_generation_timestamp,
        "timestamp_quality_state": RAINVIEWER_TIMESTAMP_QUALITY_STATE,
        "coverage_mask_captured": True,
        "radar_tile_count": 9,
        "coverage_tile_count": 9,
        "raw_member_count": 19,
        "request_horizon_km": horizon_km,
        "guaranteed_capture_coverage_radius_km": guaranteed_radius,
        "captured_frame_count": 1,
        "motion_history_eligible": False,
        "operational_admission_eligible": False,
        "quantitative_semantics_eligible": False,
        "decode_status": "not_decoded",
        "capture_state": "captured_not_decoded",
        "weather_state": "unknown",
        "admission_status": RAINVIEWER_ADMISSION_STATUS,
        "raw_provenance": _capture_provenance(
            capture,
            identity=identity,
            sources=sources,
            center_lat=lat,
            center_lon=lon,
            horizon_km=horizon_km,
            guaranteed_coverage_radius_km=guaranteed_radius,
        ),
    }


def _resume_complete_capture(
    store: RawArtifactStore,
    *,
    job_id: str,
    lat: float,
    lon: float,
    horizon_km: float,
    center_x: int,
    center_y: int,
    guaranteed_radius: float,
    deadline: float,
) -> dict[str, object] | None:
    _remaining_seconds(deadline)
    bundle = store.load_capture_bundle_bytes(_capture_id(store, job_id))
    if bundle is None:
        return None
    capture, receipts, payloads = bundle
    expected_roles = {"manifest"} | {
        _tile_role(kind, dx, dy)
        for kind in ("radar", "coverage")
        for dx, dy in _OFFSETS
    }
    if set(receipts) != expected_roles or set(payloads) != expected_roles:
        raise RuntimeError("RainViewer-capture heeft niet exact negentien members")
    manifest_receipt = receipts["manifest"]
    manifest_bytes = payloads["manifest"]
    _assert_receipt_contract(
        manifest_receipt,
        product=RAINVIEWER_MANIFEST_PRODUCT,
        fetch_id=f"{job_id}:manifest",
        upstream_id=_manifest_upstream_id(manifest_bytes),
        content_types=frozenset({"application/json", "text/json"}),
        provenance=_manifest_provenance(),
    )
    identity = _inspect_manifest_cached(
        store,
        manifest_bytes,
        content_sha256=manifest_receipt.content_sha256,
        deadline=deadline,
    )
    sources: dict[str, dict[str, object]] = {
        "manifest": _source_reference(
            manifest_receipt,
            role="manifest",
            kind="manifest",
        )
    }
    for kind in ("radar", "coverage"):
        for dx, dy in _OFFSETS:
            _remaining_seconds(deadline)
            role = _tile_role(kind, dx, dy)
            x, y = _tile_coordinates(center_x, center_y, dx, dy)
            receipt = receipts[role]
            exact = payloads[role]
            product = (
                RAINVIEWER_RADAR_TILE_PRODUCT
                if kind == "radar"
                else RAINVIEWER_COVERAGE_TILE_PRODUCT
            )
            _assert_receipt_contract(
                receipt,
                product=product,
                fetch_id=f"{job_id}:{role}",
                upstream_id=_tile_upstream_id(identity, kind=kind, x=x, y=y),
                content_types=frozenset({"image/png"}),
                provenance=_tile_provenance(
                    identity,
                    kind=kind,
                    x=x,
                    y=y,
                    center_lat=lat,
                    center_lon=lon,
                    horizon_km=horizon_km,
                ),
            )
            _inspect_png_cached(
                store,
                exact,
                content_sha256=receipt.content_sha256,
                deadline=deadline,
            )
            sources[role] = _source_reference(
                receipt,
                role=role,
                kind=kind,
                x=x,
                y=y,
            )
    expected_upstream = _upstream_capture_id(
        identity,
        center_x=center_x,
        center_y=center_y,
        lat=lat,
        lon=lon,
        horizon_km=horizon_km,
    )
    members = {member.role: member.receipt_id for member in capture.members}
    if (
        capture.provider != "rainviewer"
        or capture.product != RAINVIEWER_CAPTURE_PRODUCT
        or capture.fetch_id != f"{job_id}:capture"
        or capture.upstream_capture_id != expected_upstream
        or capture.product_timestamp != identity.frame_generation_timestamp
        or members != {role: receipts[role].receipt_id for role in expected_roles}
    ):
        raise RuntimeError("duurzame RainViewer-capture heeft een verkeerde identiteit")
    _remaining_seconds(deadline)
    return _capture_result(
        capture=capture,
        identity=identity,
        sources=sources,
        lat=lat,
        lon=lon,
        horizon_km=horizon_km,
        guaranteed_radius=guaranteed_radius,
    )


def capture_latest_rainviewer_region(
    job: Any,
    *,
    raw_store: RawArtifactStore | None = None,
) -> dict[str, object]:
    """Capture one region without emitting any meteorological observation."""

    job_id, lat, lon, horizon_km = _job_identity(job)
    center_x, center_y = _center_tile(lat, lon)
    guaranteed_radius = _guaranteed_coverage_radius_km(
        lat=lat,
        lon=lon,
        center_x=center_x,
        center_y=center_y,
    )
    if horizon_km > guaranteed_radius:
        raise ValueError(
            "RainViewer 3x3-tegelset dekt de gevraagde horizon niet gegarandeerd"
        )
    deadline = time.monotonic() + CAPTURE_TOTAL_DEADLINE_SECONDS
    with _deadline_scope(deadline):
        store = _raw_store(raw_store)
        _remaining_seconds(deadline)
        with _deadline_thread_lock(_CAPTURE_LOCK, deadline=deadline):
            with _deadline_file_lock(
                _capture_capacity_lock_path(store),
                deadline=deadline,
            ), _deadline_file_lock(
                _slot_lock_path(store, job_id),
                deadline=deadline,
            ):
                resumed = _resume_complete_capture(
                    store,
                    job_id=job_id,
                    lat=lat,
                    lon=lon,
                    horizon_km=horizon_km,
                    center_x=center_x,
                    center_y=center_y,
                    guaranteed_radius=guaranteed_radius,
                    deadline=deadline,
                )
                if resumed is not None:
                    return resumed

                manifest_receipt, manifest_bytes = _store_or_load_manifest(
                    store, job_id=job_id, deadline=deadline
                )
                _remaining_seconds(deadline)

                # Exact manifest is durable before this first JSON parse.
                identity = _inspect_manifest_cached(
                    store,
                    manifest_bytes,
                    content_sha256=manifest_receipt.content_sha256,
                    deadline=deadline,
                )
                roles: dict[str, str] = {"manifest": manifest_receipt.receipt_id}
                sources: dict[str, dict[str, object]] = {
                    "manifest": _source_reference(
                        manifest_receipt, role="manifest", kind="manifest"
                    )
                }
                for kind in ("radar", "coverage"):
                    for dx, dy in _OFFSETS:
                        _remaining_seconds(deadline)
                        role = _tile_role(kind, dx, dy)
                        x, y = _tile_coordinates(center_x, center_y, dx, dy)
                        receipt, exact = _store_or_load_tile(
                            store,
                            job_id=job_id,
                            role=role,
                            kind=kind,
                            identity=identity,
                            x=x,
                            y=y,
                            center_lat=lat,
                            center_lon=lon,
                            horizon_km=horizon_km,
                            deadline=deadline,
                        )
                        # Exact tile is durable before PNG inspection.
                        _inspect_png_cached(
                            store,
                            exact,
                            content_sha256=receipt.content_sha256,
                            deadline=deadline,
                        )
                        _remaining_seconds(deadline)
                        roles[role] = receipt.receipt_id
                        sources[role] = _source_reference(
                            receipt, role=role, kind=kind, x=x, y=y
                        )

                upstream_capture_id = _upstream_capture_id(
                    identity,
                    center_x=center_x,
                    center_y=center_y,
                    lat=lat,
                    lon=lon,
                    horizon_km=horizon_km,
                )
                _remaining_seconds(deadline)
                capture = store.commit_capture(
                    provider="rainviewer",
                    product=RAINVIEWER_CAPTURE_PRODUCT,
                    fetch_id=f"{job_id}:capture",
                    upstream_capture_id=upstream_capture_id,
                    product_timestamp=identity.frame_generation_timestamp,
                    members=roles,
                )
                _remaining_seconds(deadline)
                return _capture_result(
                    capture=capture,
                    identity=identity,
                    sources=sources,
                    lat=lat,
                    lon=lon,
                    horizon_km=horizon_km,
                    guaranteed_radius=guaranteed_radius,
                )


RAINVIEWER_CANDIDATE_REQUIRED_ROLES = tuple(sorted(
    ("manifest",)
    + tuple(_tile_role("radar", dx, dy) for dx, dy in _OFFSETS)
    + tuple(_tile_role("coverage", dx, dy) for dx, dy in _OFFSETS)
))


__all__ = [
    "RAINVIEWER_ADMISSION_STATUS",
    "RAINVIEWER_CANDIDATE_REQUIRED_ROLES",
    "RAINVIEWER_CAPTURE_PRODUCT",
    "RAINVIEWER_SEMANTIC_CLASS",
    "RAINVIEWER_TIMESTAMP_QUALITY_STATE",
    "RainviewerHttpResponse",
    "RainviewerManifestIdentity",
    "RainviewerPngIdentity",
    "capture_latest_rainviewer_region",
]
