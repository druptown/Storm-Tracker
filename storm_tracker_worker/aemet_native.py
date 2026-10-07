"""Exact-byte, shadow-only AEMET national-radar OpenData capture.

The official OpenData envelope and both responses selected by that envelope
are retained as three schema-3 raw members.  The ``api_key`` is an HTTP-header
transport secret: it is never added to a URL, identifier, receipt, raw body,
result or error message.  Exact response bytes are durable before JSON parsing
or any product inspection.

This candidate deliberately performs no GeoTIFF decode.  AEMET documents the
national product as lowest-PPI reflectivity in dBZ, reprojected to EPSG:4326,
with a per-file ``ESCALA`` colour mapping.  Those are provenance claims, not a
rain-rate interpretation or operational admission.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import errno
import hashlib
import json
import math
import os
from pathlib import Path
import re
import stat
import threading
import time
from typing import Any, Callable, Iterator, Mapping
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

from .raw_artifacts import (
    NATIVE_CAPTURE_STAGE,
    NATIVE_REPRESENTATION,
    RAW_ARTIFACT_SCHEMA_VERSION,
    RawArtifactCaptureReceipt,
    RawArtifactReceipt,
    RawArtifactStore,
)


API_ORIGIN = "https://opendata.aemet.es"
API_HOST = "opendata.aemet.es"
API_PATH = "/opendata/api/red/radar/nacional"
API_URL = f"{API_ORIGIN}{API_PATH}"
DOWNLOAD_PATH_PREFIX = "/opendata/sh/"

PROVIDER = "aemet_radar"
ENVELOPE_PRODUCT = "aemet_national_radar_opendata_envelope_json"
DATA_PRODUCT = "aemet_national_radar_opendata_data_response"
METADATA_PRODUCT = "aemet_national_radar_opendata_metadata_response"
CAPTURE_PRODUCT = "aemet_national_radar_opendata_exact_capture"
COLLECTOR = "aemet_opendata_exact_native"
COLLECTOR_VERSION = "worker-aemet-opendata-exact-native-v1"
METHOD = "aemet_opendata_exact_native_capture_v1"

REQUIRED_ROLES = (
    "opendata_envelope_json",
    "opendata_data_response",
    "opendata_metadata_response",
)
ROLE_PRODUCTS = {
    REQUIRED_ROLES[0]: ENVELOPE_PRODUCT,
    REQUIRED_ROLES[1]: DATA_PRODUCT,
    REQUIRED_ROLES[2]: METADATA_PRODUCT,
}

OFFICIAL_SPECIFICATION = (
    "https://opendata.aemet.es/AEMET_OpenData_specification.json"
)
OFFICIAL_RADAR_HELP = (
    "https://www.aemet.es/en/eltiempo/observacion/radar/ayuda"
)

MAX_ENVELOPE_BYTES = 64 * 1024
MAX_DATA_BYTES = 96 * 1024 * 1024
MAX_METADATA_BYTES = 4 * 1024 * 1024
MAX_HTTP_HEADER_CHARS = 8_192
MAX_JSON_NODES = 64
MAX_JSON_DEPTH = 4
STREAM_CHUNK_BYTES = 128 * 1024
HTTP_IO_TIMEOUT_SECONDS = 20.0
CAPTURE_TOTAL_DEADLINE_SECONDS = 75.0
LOCK_POLL_SECONDS = 0.025

RAW_SHARE_ROOT = Path("/share/storm_tracker_worker")
RAW_ARTIFACT_ROOT = RAW_SHARE_ROOT / "raw" / "aemet_opendata"

_JOB_ID_RE = re.compile(r"^[^\x00-\x1f\x7f]{1,440}$")
_DOWNLOAD_PATH_RE = re.compile(
    r"^/opendata/sh/[A-Za-z0-9][A-Za-z0-9._~-]{7,511}$"
)
_ALLOWED_ENVELOPE_CONTENT_TYPES = frozenset({"application/json"})
_ALLOWED_DATA_CONTENT_TYPES = frozenset({
    "application/gzip",
    "application/octet-stream",
    "application/x-gzip",
    "application/x-tar",
    "application/zip",
    "binary/octet-stream",
    "image/tiff",
})
_ALLOWED_METADATA_CONTENT_TYPES = frozenset({
    "application/json",
    "application/octet-stream",
    "text/plain",
    "text/xml",
})

_RAW_STORE: RawArtifactStore | None = None
_RAW_STORE_LOCK = threading.Lock()
_CAPTURE_LOCK = threading.Lock()


class AemetNativeError(RuntimeError):
    """Base capture failure; it never implies a weather observation."""


class AemetNativeContractError(AemetNativeError):
    """The remote response or caller left the pinned capture contract."""


class AemetNativeDeadlineError(AemetNativeError):
    """The absolute bounded capture deadline elapsed."""


class AemetNativeCorruptionError(AemetNativeError):
    """Durable schema-3 evidence conflicts with its immutable identity."""


@dataclass(frozen=True, slots=True)
class AemetHttpResponse:
    """One exact, bounded HTTP response body plus safe transport metadata."""

    payload: bytes
    content_type: str
    received_at: float
    content_length: int
    etag: str | None = None
    last_modified: str | None = None


@dataclass(frozen=True, slots=True)
class AemetEnvelopeIdentity:
    """Strict in-memory selection extracted from one durable envelope."""

    description: str
    data_url: str
    metadata_url: str
    data_selection_sha256: str
    metadata_selection_sha256: str


class _RejectRedirects(HTTPRedirectHandler):
    def redirect_request(self, *_args: Any, **_kwargs: Any) -> None:
        raise AemetNativeContractError("AEMET HTTP-redirect is niet toegestaan")


def _remaining(deadline: float) -> float:
    remaining = float(deadline) - time.monotonic()
    if not math.isfinite(remaining) or remaining <= 0:
        raise AemetNativeDeadlineError("AEMET capturedeadline is verstreken")
    return remaining


def _job_id(job: Any) -> str:
    value = getattr(job, "job_id", None)
    payload = getattr(job, "payload", None)
    if not isinstance(value, str) or _JOB_ID_RE.fullmatch(value) is None:
        raise AemetNativeContractError("AEMET capturejob mist een begrensde job_id")
    if type(payload) is not dict or payload:
        raise AemetNativeContractError(
            "AEMET native capture vereist een exact lege jobpayload"
        )
    return value


def _api_key_from_environment() -> str:
    value = os.getenv("STV3_AEMET_API_KEY", "")
    if (
        not isinstance(value, str)
        or value != value.strip()
        or not 16 <= len(value) <= 4_096
        or not value.isascii()
        or any(ord(character) < 0x21 or ord(character) == 0x7F for character in value)
    ):
        raise AemetNativeContractError("AEMET transportcredential ontbreekt of is ongeldig")
    return value


def _assert_secret_absent(value: bytes | str | None, api_key: str) -> None:
    if value is None:
        return
    if isinstance(value, bytes):
        try:
            encoded = api_key.encode("utf-8")
        except UnicodeEncodeError as exc:  # pragma: no cover - key validation catches it
            raise AemetNativeContractError("AEMET transportcredential is ongeldig") from exc
        leaked = encoded in value
    else:
        leaked = api_key in value
    if leaked:
        raise AemetNativeContractError(
            "AEMET upstreamrespons bevat transportcredentialmateriaal"
        )


def _header(headers: Any, name: str, *, maximum: int = MAX_HTTP_HEADER_CHARS) -> str | None:
    value = headers.get(name) if headers is not None else None
    if value is None:
        return None
    result = str(value).strip()
    if (
        not result
        or len(result) > maximum
        or any(ord(character) < 0x20 or ord(character) == 0x7F for character in result)
    ):
        raise AemetNativeContractError("AEMET HTTP-header is ongeldig")
    return result


def _content_type(headers: Any) -> str:
    value = _header(headers, "Content-Type", maximum=256)
    if value is None:
        raise AemetNativeContractError("AEMET Content-Type ontbreekt")
    return value.split(";", 1)[0].strip().lower()


def _declared_length(headers: Any, *, maximum: int) -> int | None:
    value = _header(headers, "Content-Length", maximum=32)
    if value is None:
        return None
    if not value.isascii() or not value.isdecimal():
        raise AemetNativeContractError("AEMET Content-Length is ongeldig")
    result = int(value)
    if not 1 <= result <= maximum:
        raise AemetNativeContractError("AEMET Content-Length overschrijdt de grens")
    return result


def _read_bounded(response: Any, *, maximum: int, declared: int | None) -> bytes:
    chunks: list[bytes] = []
    total = 0
    while True:
        block = response.read(min(STREAM_CHUNK_BYTES, maximum + 1 - total))
        if not block:
            break
        if not isinstance(block, bytes):
            raise AemetNativeContractError("AEMET transport leverde geen bytes")
        chunks.append(block)
        total += len(block)
        if total > maximum:
            raise AemetNativeContractError("AEMET response overschrijdt de bytegrens")
    payload = b"".join(chunks)
    if not payload or (declared is not None and len(payload) != declared):
        raise AemetNativeContractError("AEMET response is leeg of onvolledig")
    return payload


def _validate_api_url(url: str) -> None:
    parts = urlsplit(url)
    if (
        parts.scheme != "https"
        or parts.netloc != API_HOST
        or parts.path != API_PATH
        or parts.query
        or parts.fragment
        or parts.username is not None
        or parts.password is not None
        or parts.port is not None
    ):
        raise AemetNativeContractError("AEMET API-URL wijkt af van het vaste endpoint")


def _validate_download_url(url: object) -> str:
    if not isinstance(url, str) or not 1 <= len(url) <= 2_048 or url != url.strip():
        raise AemetNativeContractError("AEMET selectielink is ongeldig")
    parts = urlsplit(url)
    if (
        parts.scheme != "https"
        or parts.netloc != API_HOST
        or _DOWNLOAD_PATH_RE.fullmatch(parts.path) is None
        or not parts.path.startswith(DOWNLOAD_PATH_PREFIX)
        or parts.query
        or parts.fragment
        or parts.username is not None
        or parts.password is not None
        or parts.port is not None
    ):
        raise AemetNativeContractError("AEMET selectielink valt buiten de HTTPS-allowlist")
    return url


def _http_get(
    url: str,
    *,
    api_key: str | None,
    maximum: int,
    allowed_content_types: frozenset[str],
    deadline: float,
) -> AemetHttpResponse:
    if api_key is None:
        _validate_download_url(url)
    else:
        _validate_api_url(url)
    headers = {
        "Accept": "*/*",
        "Accept-Encoding": "identity",
        "User-Agent": "StormTrackerWorker/aemet-native-candidate-v1",
    }
    if api_key is not None:
        headers["api_key"] = api_key
    request = Request(url, headers=headers, method="GET")
    try:
        opener = build_opener(ProxyHandler({}), _RejectRedirects())
        with opener.open(
            request, timeout=min(HTTP_IO_TIMEOUT_SECONDS, _remaining(deadline))
        ) as response:
            status = int(getattr(response, "status", 0))
            if status != 200:
                raise AemetNativeContractError("AEMET endpoint gaf geen HTTP 200")
            response_url = str(getattr(response, "url", url))
            if api_key is None:
                _validate_download_url(response_url)
            else:
                _validate_api_url(response_url)
            response_headers = getattr(response, "headers", None)
            if _header(response_headers, "Content-Encoding") is not None:
                raise AemetNativeContractError(
                    "AEMET transportcompressie is niet toegestaan"
                )
            media_type = _content_type(response_headers)
            if media_type not in allowed_content_types:
                raise AemetNativeContractError("AEMET response heeft onverwacht mediatype")
            declared = _declared_length(response_headers, maximum=maximum)
            payload = _read_bounded(response, maximum=maximum, declared=declared)
            etag = _header(response_headers, "ETag", maximum=256)
            last_modified = _header(response_headers, "Last-Modified", maximum=128)
    except AemetNativeError:
        raise
    except Exception:
        # Never chain transport exceptions: Request headers can contain api_key.
        raise AemetNativeContractError("AEMET HTTPS-transport is mislukt") from None
    if api_key is not None:
        _assert_secret_absent(payload, api_key)
        _assert_secret_absent(etag, api_key)
        _assert_secret_absent(last_modified, api_key)
    return AemetHttpResponse(
        payload=payload,
        content_type=media_type,
        received_at=time.time(),
        content_length=len(payload),
        etag=etag,
        last_modified=last_modified,
    )


def _fetch_envelope(api_key: str, *, deadline: float) -> AemetHttpResponse:
    return _http_get(
        API_URL,
        api_key=api_key,
        maximum=MAX_ENVELOPE_BYTES,
        allowed_content_types=_ALLOWED_ENVELOPE_CONTENT_TYPES,
        deadline=deadline,
    )


def _fetch_download(url: str, role: str, *, deadline: float) -> AemetHttpResponse:
    if role == REQUIRED_ROLES[1]:
        maximum = MAX_DATA_BYTES
        media_types = _ALLOWED_DATA_CONTENT_TYPES
    elif role == REQUIRED_ROLES[2]:
        maximum = MAX_METADATA_BYTES
        media_types = _ALLOWED_METADATA_CONTENT_TYPES
    else:  # pragma: no cover - private call boundary
        raise AemetNativeContractError("AEMET downloadrol is ongeldig")
    return _http_get(
        url,
        api_key=None,
        maximum=maximum,
        allowed_content_types=media_types,
        deadline=deadline,
    )


def _json_without_duplicates(payload: bytes) -> Any:
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise AemetNativeContractError("AEMET envelope is geen UTF-8") from exc

    def pairs(values: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in values:
            if key in result:
                raise AemetNativeContractError("AEMET envelope bevat dubbele JSON-sleutels")
            result[key] = value
        return result

    try:
        return json.loads(text, object_pairs_hook=pairs)
    except (json.JSONDecodeError, RecursionError) as exc:
        raise AemetNativeContractError("AEMET envelope is geen geldige JSON") from exc


def _bounded_json(value: Any, *, depth: int = 0, counter: list[int] | None = None) -> None:
    if counter is None:
        counter = [0]
    counter[0] += 1
    if counter[0] > MAX_JSON_NODES or depth > MAX_JSON_DEPTH:
        raise AemetNativeContractError("AEMET envelope overschrijdt de JSON-grens")
    if isinstance(value, dict):
        for key, nested in value.items():
            if not isinstance(key, str) or len(key) > 128:
                raise AemetNativeContractError("AEMET envelope bevat ongeldige sleutel")
            _bounded_json(nested, depth=depth + 1, counter=counter)
    elif isinstance(value, list):
        for nested in value:
            _bounded_json(nested, depth=depth + 1, counter=counter)
    elif isinstance(value, str):
        if len(value) > 8_192:
            raise AemetNativeContractError("AEMET envelope bevat te lange tekst")
    elif value is not None and not isinstance(value, (bool, int, float)):
        raise AemetNativeContractError("AEMET envelope bevat onbekend JSON-type")


def _inspect_envelope(payload: bytes) -> AemetEnvelopeIdentity:
    root = _json_without_duplicates(payload)
    _bounded_json(root)
    if not isinstance(root, dict) or set(root) != {
        "descripcion", "estado", "datos", "metadatos"
    }:
        raise AemetNativeContractError("AEMET envelope wijkt af van schema 200")
    if type(root["estado"]) is not int or root["estado"] != 200:
        raise AemetNativeContractError("AEMET envelope bevat geen succesvolle status")
    description = root["descripcion"]
    if (
        not isinstance(description, str)
        or description != description.strip()
        or not 1 <= len(description) <= 512
        or any(ord(character) < 0x20 for character in description)
    ):
        raise AemetNativeContractError("AEMET envelopebeschrijving is ongeldig")
    data_url = _validate_download_url(root["datos"])
    metadata_url = _validate_download_url(root["metadatos"])
    if data_url == metadata_url:
        raise AemetNativeContractError("AEMET data- en metadatarespons zijn niet onderscheiden")
    return AemetEnvelopeIdentity(
        description=description,
        data_url=data_url,
        metadata_url=metadata_url,
        data_selection_sha256=hashlib.sha256(data_url.encode("utf-8")).hexdigest(),
        metadata_selection_sha256=hashlib.sha256(metadata_url.encode("utf-8")).hexdigest(),
    )


def _validate_response(
    response: AemetHttpResponse,
    *,
    role: str,
    api_key: str | None = None,
) -> None:
    if not isinstance(response, AemetHttpResponse):
        raise AemetNativeContractError("AEMET transport leverde geen begrensde respons")
    maximum, allowed = {
        REQUIRED_ROLES[0]: (MAX_ENVELOPE_BYTES, _ALLOWED_ENVELOPE_CONTENT_TYPES),
        REQUIRED_ROLES[1]: (MAX_DATA_BYTES, _ALLOWED_DATA_CONTENT_TYPES),
        REQUIRED_ROLES[2]: (MAX_METADATA_BYTES, _ALLOWED_METADATA_CONTENT_TYPES),
    }[role]
    if (
        not isinstance(response.payload, bytes)
        or not 1 <= len(response.payload) <= maximum
        or response.content_length != len(response.payload)
        or response.content_type not in allowed
        or isinstance(response.received_at, bool)
        or not isinstance(response.received_at, (int, float))
        or not math.isfinite(float(response.received_at))
        or response.received_at < 0
    ):
        raise AemetNativeContractError("AEMET response wijkt af van de bytegrens")
    for value in (response.etag, response.last_modified):
        if value is not None and (
            not isinstance(value, str)
            or not 1 <= len(value) <= 256
            or any(ord(character) < 0x20 for character in value)
        ):
            raise AemetNativeContractError("AEMET responseheader is ongeldig")
    if api_key is not None:
        _assert_secret_absent(response.payload, api_key)
        _assert_secret_absent(response.etag, api_key)
        _assert_secret_absent(response.last_modified, api_key)


def _raw_store(injected: RawArtifactStore | None) -> RawArtifactStore:
    if injected is not None:
        if not isinstance(injected, RawArtifactStore):
            raise TypeError("raw_store moet een RawArtifactStore zijn")
        injected.initialize()
        return injected
    share = RAW_SHARE_ROOT.resolve(strict=False)
    root = RAW_ARTIFACT_ROOT.resolve(strict=False)
    if root == share or share not in root.parents:
        raise AemetNativeCorruptionError("AEMET raw store ontsnapt uit worker-share")
    global _RAW_STORE
    with _RAW_STORE_LOCK:
        if _RAW_STORE is None:
            _RAW_STORE = RawArtifactStore(root)
            _RAW_STORE.initialize()
        return _RAW_STORE


def _safe_auxiliary_directory(store: RawArtifactStore) -> Path:
    root = Path(store.root)
    if root.is_symlink() or not root.is_dir():
        raise AemetNativeCorruptionError("AEMET raw root is onveilig")
    directory = root / "aemet_native_locks"
    directory.mkdir(mode=0o700, exist_ok=True)
    if directory.is_symlink() or directory.resolve().parent != root.resolve():
        raise AemetNativeCorruptionError("AEMET lockmap ontsnapt uit raw root")
    return directory


@contextmanager
def _deadline_thread_lock(*, deadline: float) -> Iterator[None]:
    acquired = _CAPTURE_LOCK.acquire(timeout=_remaining(deadline))
    if not acquired:
        raise AemetNativeDeadlineError("AEMET capturethreadlock overschreed de deadline")
    try:
        _remaining(deadline)
        yield
    finally:
        _CAPTURE_LOCK.release()


@contextmanager
def _deadline_file_lock(store: RawArtifactStore, *, deadline: float) -> Iterator[None]:
    path = _safe_auxiliary_directory(store) / "capture-capacity.lock"
    if path.exists() and (path.is_symlink() or not path.is_file()):
        raise AemetNativeCorruptionError("AEMET lockpad is onveilig")
    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_BINARY", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags, 0o600)
    acquired = False
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode):
            raise AemetNativeCorruptionError("AEMET lock is geen regulier bestand")
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


def _receipt_id(store: RawArtifactStore, job_id: str, role: str) -> str:
    return store._receipt_id(  # noqa: SLF001 - deterministic schema-3 identity
        provider=PROVIDER,
        product=ROLE_PRODUCTS[role],
        fetch_id=f"{job_id}:{role}",
    )


def _capture_id(store: RawArtifactStore, job_id: str) -> str:
    return store._capture_id(  # noqa: SLF001 - deterministic schema-3 identity
        provider=PROVIDER,
        product=CAPTURE_PRODUCT,
        fetch_id=f"{job_id}:capture",
    )


def _selection_digest(identity: AemetEnvelopeIdentity, role: str) -> str:
    if role == REQUIRED_ROLES[1]:
        return identity.data_selection_sha256
    if role == REQUIRED_ROLES[2]:
        return identity.metadata_selection_sha256
    raise AemetNativeContractError("AEMET selectie heeft ongeldige rol")


def _provenance(
    *,
    role: str,
    response: AemetHttpResponse | None = None,
    selection_sha256: str | None = None,
) -> dict[str, object]:
    base: dict[str, object] = {
        "representation": NATIVE_REPRESENTATION,
        "capture_stage": NATIVE_CAPTURE_STAGE,
        "collector": COLLECTOR,
        "collector_version": COLLECTOR_VERSION,
        "role": role,
        "request_method": "GET",
        "api_key_transport": "http_header",
        "api_key_persisted": False,
        "request_url_persisted": False,
        "decoded_or_admitted": False,
    }
    if role == REQUIRED_ROLES[0]:
        base["request_host"] = API_HOST
        base["request_path"] = API_PATH
        base["official_operation"] = "GET /api/red/radar/nacional"
    else:
        assert response is not None and selection_sha256 is not None
        base.update({
            "request_host": API_HOST,
            "request_path_prefix": DOWNLOAD_PATH_PREFIX,
            "selection_url_sha256": selection_sha256,
            "content_type": response.content_type,
            "content_length": response.content_length,
            "etag": response.etag,
            "last_modified": response.last_modified,
        })
    return base


def _load_optional(
    store: RawArtifactStore, *, job_id: str, role: str
) -> tuple[RawArtifactReceipt | None, bytes | None]:
    loaded = store.load_receipt_bytes(_receipt_id(store, job_id, role))
    return (None, None) if loaded is None else loaded


def _validate_receipt(
    receipt: RawArtifactReceipt,
    *,
    job_id: str,
    role: str,
    identity: AemetEnvelopeIdentity | None = None,
) -> None:
    maximum, allowed = {
        REQUIRED_ROLES[0]: (MAX_ENVELOPE_BYTES, _ALLOWED_ENVELOPE_CONTENT_TYPES),
        REQUIRED_ROLES[1]: (MAX_DATA_BYTES, _ALLOWED_DATA_CONTENT_TYPES),
        REQUIRED_ROLES[2]: (MAX_METADATA_BYTES, _ALLOWED_METADATA_CONTENT_TYPES),
    }[role]
    expected_upstream: str
    if role == REQUIRED_ROLES[0]:
        expected_provenance = _provenance(role=role)
        expected_upstream = (
            f"{API_HOST}{API_PATH}|content_sha256={receipt.content_sha256}"
        )
    else:
        if identity is None:
            raise AemetNativeCorruptionError("AEMET receipt mist envelopeselectie")
        selection = _selection_digest(identity, role)
        provenance = receipt.provenance
        synthetic = AemetHttpResponse(
            # Only immutable receipt metadata is reconstructed here.  Never
            # allocate a second object-sized buffer during offline resume.
            payload=b"",
            content_type=receipt.content_type,
            received_at=receipt.received_at,
            content_length=receipt.size_bytes,
            etag=provenance.get("etag"),
            last_modified=provenance.get("last_modified"),
        )
        expected_provenance = _provenance(
            role=role, response=synthetic, selection_sha256=selection
        )
        expected_upstream = (
            f"{API_HOST}|selection_sha256={selection}"
            f"|content_sha256={receipt.content_sha256}"
        )
    if (
        receipt.provider != PROVIDER
        or receipt.product != ROLE_PRODUCTS[role]
        or receipt.fetch_id != f"{job_id}:{role}"
        or receipt.content_type not in allowed
        or not 1 <= receipt.size_bytes <= maximum
        or receipt.upstream_id != expected_upstream
        or receipt.provenance != expected_provenance
    ):
        raise AemetNativeCorruptionError("AEMET schema-3 receipt wijkt af")


def _upstream_capture_id(
    receipts: Mapping[str, RawArtifactReceipt], identity: AemetEnvelopeIdentity
) -> str:
    return (
        "aemet-national-radar-opendata"
        f"|envelope={receipts[REQUIRED_ROLES[0]].content_sha256}"
        f"|data_selection={identity.data_selection_sha256}"
        f"|data={receipts[REQUIRED_ROLES[1]].content_sha256}"
        f"|metadata_selection={identity.metadata_selection_sha256}"
        f"|metadata={receipts[REQUIRED_ROLES[2]].content_sha256}"
    )


def _validate_capture(
    capture: RawArtifactCaptureReceipt,
    *,
    job_id: str,
    receipts: Mapping[str, RawArtifactReceipt],
    identity: AemetEnvelopeIdentity,
) -> None:
    members = {member.role: member.receipt_id for member in capture.members}
    if (
        capture.raw_store_schema_version != RAW_ARTIFACT_SCHEMA_VERSION
        or capture.provider != PROVIDER
        or capture.product != CAPTURE_PRODUCT
        or capture.fetch_id != f"{job_id}:capture"
        or capture.product_timestamp is not None
        or capture.upstream_capture_id != _upstream_capture_id(receipts, identity)
        or members
        != {role: receipts[role].receipt_id for role in REQUIRED_ROLES}
    ):
        raise AemetNativeCorruptionError("AEMET complete capture wijkt af")


def _source(receipt: RawArtifactReceipt, role: str) -> dict[str, object]:
    return {
        "role": role,
        "receipt_id": receipt.receipt_id,
        "content_sha256": receipt.content_sha256,
        "receipt_sha256": receipt.receipt_sha256,
        "size_bytes": receipt.size_bytes,
        "received_at": receipt.received_at,
        "content_type": receipt.content_type,
    }


def _result(
    capture: RawArtifactCaptureReceipt,
    receipts: Mapping[str, RawArtifactReceipt],
    identity: AemetEnvelopeIdentity,
) -> dict[str, object]:
    return {
        "method": METHOD,
        "capture_state": "captured_not_decoded",
        "weather_state": "unknown",
        "decode_status": "blocked_no_certified_geotiff_decoder",
        "quantitative_rain_rate_eligible": False,
        "forecast_eligible": False,
        "operational_admission_eligible": False,
        "pixel_values_read": False,
        "raw_provenance": {
            "raw_store_schema_version": RAW_ARTIFACT_SCHEMA_VERSION,
            "raw_capture_id": capture.capture_id,
            "raw_capture_manifest_sha256": capture.manifest_sha256,
            "raw_capture_receipt_sha256": capture.capture_receipt_sha256,
            "upstream_capture_id": capture.upstream_capture_id,
            "raw_member_count": len(receipts),
            "required_roles": list(REQUIRED_ROLES),
            "api_key_transport": "http_header",
            "api_key_persisted": False,
            "download_urls_persisted_outside_exact_envelope": False,
            "envelope_description": identity.description,
            "official_product_semantics": {
                "quantity": "lowest_PPI_radar_reflectivity",
                "unit": "dBZ",
                "container": "GeoTIFF",
                "crs": "EPSG:4326",
                "per_file_ESCALA_required": True,
                "provenance_only": True,
            },
            "unresolved_semantics": [
                "geotiff_grid_geometry_and_pixel_support",
                "per_file_ESCALA_value_mapping",
                "timestamp_timezone_and_observation_support",
                "nodata_noecho_coverage_and_pixel_qc",
                "reflectivity_to_rain_rate_relation",
                "official_current_golden_capture",
            ],
            "official_sources": [OFFICIAL_SPECIFICATION, OFFICIAL_RADAR_HELP],
            "sources": {
                role: _source(receipts[role], role) for role in REQUIRED_ROLES
            },
        },
    }


def _capture_locked(
    job_id: str,
    *,
    store: RawArtifactStore,
    deadline: float,
    fault: Callable[[str], None] | None,
) -> dict[str, object]:
    complete = store.load_capture_bundle_bytes(_capture_id(store, job_id))
    if complete is not None:
        capture, receipts, payloads = complete
        if set(receipts) != set(REQUIRED_ROLES) or set(payloads) != set(REQUIRED_ROLES):
            raise AemetNativeCorruptionError("AEMET complete capture mist rollen")
        _validate_receipt(receipts[REQUIRED_ROLES[0]], job_id=job_id, role=REQUIRED_ROLES[0])
        identity = _inspect_envelope(payloads[REQUIRED_ROLES[0]])
        for role in REQUIRED_ROLES[1:]:
            _validate_receipt(receipts[role], job_id=job_id, role=role, identity=identity)
        _validate_capture(capture, job_id=job_id, receipts=receipts, identity=identity)
        return _result(capture, receipts, identity)

    receipts: dict[str, RawArtifactReceipt] = {}
    transport_secret: str | None = None
    envelope_receipt, envelope_payload = _load_optional(
        store, job_id=job_id, role=REQUIRED_ROLES[0]
    )
    if envelope_receipt is None:
        transport_secret = _api_key_from_environment()
        _assert_secret_absent(job_id, transport_secret)
        response = _fetch_envelope(transport_secret, deadline=deadline)
        _validate_response(
            response, role=REQUIRED_ROLES[0], api_key=transport_secret
        )
        digest = hashlib.sha256(response.payload).hexdigest()
        envelope_receipt = store.store(
            provider=PROVIDER,
            product=ENVELOPE_PRODUCT,
            fetch_id=f"{job_id}:{REQUIRED_ROLES[0]}",
            content_type=response.content_type,
            received_at=response.received_at,
            upstream_id=f"{API_HOST}{API_PATH}|content_sha256={digest}",
            provenance=_provenance(role=REQUIRED_ROLES[0]),
            payload=response.payload,
        )
        envelope_payload = response.payload
        if fault is not None:
            fault("aemet.after_envelope_store")
    else:
        _validate_receipt(envelope_receipt, job_id=job_id, role=REQUIRED_ROLES[0])
    assert envelope_payload is not None
    receipts[REQUIRED_ROLES[0]] = envelope_receipt

    # The exact envelope is durable before it is parsed into selection URLs.
    identity = _inspect_envelope(envelope_payload)
    _remaining(deadline)

    for role, url in (
        (REQUIRED_ROLES[1], identity.data_url),
        (REQUIRED_ROLES[2], identity.metadata_url),
    ):
        receipt, payload = _load_optional(store, job_id=job_id, role=role)
        if receipt is None:
            response = _fetch_download(url, role, deadline=deadline)
            # On a fresh three-member attempt, prove that the credential was
            # not reflected by either selected response.  A partial offline
            # retry need not resolve a credential that was never sent to the
            # download endpoints.
            _validate_response(response, role=role, api_key=transport_secret)
            selection = _selection_digest(identity, role)
            digest = hashlib.sha256(response.payload).hexdigest()
            receipt = store.store(
                provider=PROVIDER,
                product=ROLE_PRODUCTS[role],
                fetch_id=f"{job_id}:{role}",
                content_type=response.content_type,
                received_at=response.received_at,
                upstream_id=(
                    f"{API_HOST}|selection_sha256={selection}"
                    f"|content_sha256={digest}"
                ),
                provenance=_provenance(
                    role=role, response=response, selection_sha256=selection
                ),
                payload=response.payload,
            )
            payload = response.payload
            if fault is not None:
                fault(f"aemet.after_{'data' if role == REQUIRED_ROLES[1] else 'metadata'}_store")
        else:
            _validate_receipt(receipt, job_id=job_id, role=role, identity=identity)
        assert payload is not None
        receipts[role] = receipt
        _remaining(deadline)

    if fault is not None:
        fault("aemet.before_capture_commit")
    capture = store.commit_capture(
        provider=PROVIDER,
        product=CAPTURE_PRODUCT,
        fetch_id=f"{job_id}:capture",
        upstream_capture_id=_upstream_capture_id(receipts, identity),
        product_timestamp=None,
        members={role: receipts[role].receipt_id for role in REQUIRED_ROLES},
    )
    if fault is not None:
        fault("aemet.after_capture_commit")
    _validate_capture(capture, job_id=job_id, receipts=receipts, identity=identity)
    return _result(capture, receipts, identity)


def capture_latest_aemet_national_radar(
    job: Any,
    *,
    raw_store: RawArtifactStore | None = None,
    deadline_at: float | None = None,
    _fault_injector: Callable[[str], None] | None = None,
) -> dict[str, object]:
    """Capture the official three-member OpenData set without decoding it."""
    job_id = _job_id(job)
    now = time.monotonic()
    if deadline_at is None:
        deadline = now + CAPTURE_TOTAL_DEADLINE_SECONDS
    elif (
        isinstance(deadline_at, bool)
        or not isinstance(deadline_at, (int, float))
        or not math.isfinite(float(deadline_at))
    ):
        raise AemetNativeContractError("AEMET deadline_at is ongeldig")
    else:
        deadline = min(float(deadline_at), now + CAPTURE_TOTAL_DEADLINE_SECONDS)
    store = _raw_store(raw_store)
    with _deadline_thread_lock(deadline=deadline):
        with _deadline_file_lock(store, deadline=deadline):
            return _capture_locked(
                job_id,
                store=store,
                deadline=deadline,
                fault=_fault_injector,
            )


__all__ = [
    "API_HOST",
    "API_PATH",
    "API_URL",
    "AemetEnvelopeIdentity",
    "AemetHttpResponse",
    "AemetNativeContractError",
    "AemetNativeCorruptionError",
    "AemetNativeDeadlineError",
    "AemetNativeError",
    "CAPTURE_PRODUCT",
    "DATA_PRODUCT",
    "ENVELOPE_PRODUCT",
    "METADATA_PRODUCT",
    "REQUIRED_ROLES",
    "capture_latest_aemet_national_radar",
]
