"""Isolated exact-byte capture for EUMETNET OPERA CIRRUS DBZH.

The legacy certification candidate validates a durable HDF5 file without
reading meteorological raster values.  Its science contract remains isolated.

The separate raw-only hook is file-backed end to end: it publishes via the
schema-3 store's bounded file API, performs no HDF inspection, creates no
full-object Python buffer, and makes no weather claim.  OPERA retention rights
remain an explicit, separate activation prerequisite.
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
import shutil
import stat
import tempfile
import threading
import time
from typing import TYPE_CHECKING, Any, Iterator, Mapping
from urllib.parse import quote, urlsplit
from urllib.request import (
    HTTPRedirectHandler,
    ProxyHandler,
    Request,
    build_opener,
)

if TYPE_CHECKING:
    import h5py

from .raw_artifacts import (
    NATIVE_CAPTURE_STAGE,
    NATIVE_REPRESENTATION,
    RAW_ARTIFACT_SCHEMA_VERSION,
    RawArtifactCaptureReceipt,
    RawArtifactLimits,
    RawArtifactReceipt,
    RawArtifactStore,
)


OPERA_S3_ENDPOINT = "https://s3.waw3-1.cloudferro.com"
OPERA_S3_HOST = "s3.waw3-1.cloudferro.com"
OPERA_S3_BUCKET = "openradar-24h"
OPERA_PLATFORM_ID = "0-20010-0-OPERA"
OPERA_PRODUCT_CODE = "DBZH"

OPERA_GRID_COLUMNS = 3_800
OPERA_GRID_ROWS = 4_400
OPERA_GRID_SCALE_METRES = 1_000.0
OPERA_CHUNK_ROWS = 760
OPERA_CHUNK_COLUMNS = 880
OPERA_DATASET_DTYPE = "<f8"
OPERA_DEFLATE_LEVEL = 9
OPERA_ODIM_CONVENTIONS = "ODIM_H5/V2_4"
OPERA_H5RAD_VERSION = "H5rad 2.4"
OPERA_SOURCE = "ORG:247"
OPERA_PRODUCT_NAME = "OPERA CIRRUS maximum reflectivity composite"
OPERA_PROJDEF = (
    "+proj=laea +lat_0=55.0 +lon_0=10.0 +x_0=1950000.0 "
    "+y_0=-2100000.0 +units=m +ellps=WGS84"
)
OPERA_CORNERS = {
    "LL_lat": 31.7462153182675,
    "LL_lon": -10.4345768386404,
    "LR_lat": 31.987650276733,
    "LR_lon": 29.421038635578,
    "UL_lat": 67.0228327624372,
    "UL_lon": -39.5357864125034,
    "UR_lat": 67.6210371071631,
    "UR_lon": 57.8119647501499,
}
OPERA_DBZH_GAIN = 1.0
OPERA_DBZH_OFFSET = 0.0
OPERA_DBZH_NODATA = -9_999_000.0
OPERA_DBZH_UNDETECT = -8_888_000.0
OPERA_QUALITY_GAIN = 1.0
OPERA_QUALITY_OFFSET = 0.0
OPERA_QUALITY_NODATA = -9_999_000.0
OPERA_QUALITY_TASK = "pl.imgw.quality.qi_total"
OPERA_IMAGE_CLASS = "IMAGE"
OPERA_IMAGE_VERSION = "1.2"
OPERA_HOW_COMMENT = (
    "Certain DBZH volume data are obtained by postprocessing at Meteo France, "
    "using the Lukas-Canade advection method "
    "(DBZH.meteo-france.advection.pysteps-1.5.0)."
)
OPERA_HOW_LICENSE = "https://creativecommons.org/licenses/by/4.0/ (CC BY 4.0)"
OPERA_HOW_FIXED_TEXT = {
    "comment": OPERA_HOW_COMMENT,
    "creator_email": "support.opera@eumetnet.eu",
    "creator_name": "Meteo France",
    "creator_type": "institution",
    "creator_url": "https://meteofrance.fr",
    "institution": "EUMETNET",
    "license": OPERA_HOW_LICENSE,
    "naming_authority": "eu.eumetnet",
    "publisher_email": "support.opera@eumetnet.eu",
    "publisher_name": "EUMETNET",
    "publisher_type": "institution",
    "publisher_url": "https://eumetnet.eu",
}

MAX_OPERA_HDF5_BYTES = 384 * 1024 * 1024
MAX_HDF_OBJECTS = 4_096
MAX_HDF_GROUP_MEMBERS = 1_024
MAX_HDF_DEPTH = 16
MAX_HDF_DATASETS = 1_024
MAX_HDF_LOGICAL_BYTES = 2 * 1024 * 1024 * 1024
MAX_HDF_CHUNK_BYTES = 16 * 1024 * 1024
HTTP_IO_TIMEOUT_SECONDS = 30.0
JOB_TOTAL_DEADLINE_SECONDS = 300.0
STREAM_CHUNK_BYTES = 1024 * 1024
LOCK_POLL_SECONDS = 0.05
MAX_VALIDATION_VERDICT_BYTES = 16 * 1024
OPERA_HTTP_USER_AGENT = "StormTrackerWorker/opera-native-candidate-v3"

OPERA_RAW_SHARE_ROOT = Path("/share/storm_tracker_worker")
OPERA_RAW_ARTIFACT_ROOT = OPERA_RAW_SHARE_ROOT / "raw" / "opera_cirrus_dbzh"
OPERA_HDF5_PRODUCT = "eumetnet_opera_cirrus_dbzh_odim_hdf5"
OPERA_CAPTURE_PRODUCT = "eumetnet_opera_cirrus_dbzh_capture"
OPERA_RAW_COLLECTOR = "eumetnet_ord_cloudferro_native_object"
OPERA_RAW_COLLECTOR_VERSION = "worker-opera-cirrus-dbzh-exact-raw-v3"
OPERA_STREAM_RAW_COLLECTOR_VERSION = (
    "worker-opera-cirrus-dbzh-exact-file-raw-v4"
)
OPERA_RAW_ONLY_CAPTURE_CONTRACT_VERSION = "opera-cirrus-raw-only-capture-v1"
OPERA_RAW_ONLY_INTERPRETATION = "unverified"
OPERA_RAW_RETENTION_AUTHORITY_ID = (
    "worker_authority.opera_raw_retention_v1"
)
OPERA_RAW_ONLY_HANDLER_PATH = (
    "storm_tracker_worker.opera_native:capture_opera_dbzh_hdf5_raw_only"
)
OPERA_RAW_ONLY_PROVENANCE_KIND = "opera_stream_raw_v1"
OPERA_TRANSPORT_SPOOL_SCHEMA = "stv3-opera-transport-spool-v1"
OPERA_TRANSPORT_SPOOL_PREFIX = ".opera-transport-spool-"
OPERA_TRANSPORT_SPOOL_MAX_ENTRIES = 2
OPERA_TRANSPORT_FREE_MARGIN_BYTES = 8 * 1024 * 1024
OPERA_STRUCTURE_INSPECTOR_VERSION = "opera-cirrus-live-golden-contract-v3"
OPERA_CERTIFICATION_CONTRACT_VERSION = "opera-cirrus-certification-v3"
OPERA_SEMANTIC_CLASS = "pan_european_maximum_reflectivity_composite"
OPERA_QUANTITY = "maximum_horizontal_reflectivity_factor"
OPERA_UNIT = "dBZ"
OPERA_TIMESTAMP_QUALITY_STATE = (
    "nominal_composite_time_input_window_not_uniform_observation_time"
)
OPERA_ADMISSION_STATUS = "exact_raw_only_decoder_rights_and_memory_not_certified"
OPERA_RIGHTS_STATUS = "official_sources_conflict_requires_explicit_clearance"
OPERA_MEMORY_ACTIVATION_BLOCKER = (
    "schema3_store_requires_one_complete_bytes_object"
)

_CIRRUS_EARLIEST_TIMESTAMP = datetime(
    2024, 1, 1, tzinfo=timezone.utc
).timestamp()
_LATEST_CONTRACT_TIMESTAMP = datetime(
    2100, 1, 1, tzinfo=timezone.utc
).timestamp()
_KEY_RE = re.compile(
    r"^(?P<date>\d{4}/\d{2}/\d{2})/OPERA/COMP/"
    r"OPERA@(?P<stamp>\d{8}T\d{4})@0@DBZH\.h5$"
)
_ETAG_RE = re.compile(r'^"(?P<digest>[0-9a-fA-F]{32})(?P<parts>-\d{1,6})?"$')
_VERSION_ID_RE = re.compile(r"^[A-Za-z0-9._~+/=-]{1,256}$")
_NODE_ID_RE = re.compile(r"[a-z0-9]{3,16}")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_ACQUISITION_ID_RE = re.compile(r"^acquisition:[0-9a-f]{64}$")
_ALLOWED_CONTENT_TYPES = frozenset({
    "application/octet-stream",
    "application/x-hdf5",
    "binary/octet-stream",
})
_OPERA_RAW_STORE: RawArtifactStore | None = None
_OPERA_RAW_STORE_LOCK = threading.Lock()
_OPERA_CAPTURE_LOCK = threading.Lock()


@dataclass(frozen=True, slots=True)
class OperaRevisionIdentity:
    """Non-secret immutable object metadata obtained from a strict HEAD."""

    etag: str
    version_id: str | None
    content_length: int
    content_type: str


@dataclass(frozen=True, slots=True)
class OperaDownloadedFile:
    """One bounded temporary file; it deliberately contains no payload bytes."""

    path: str
    size_bytes: int
    content_sha256: str
    content_type: str
    received_at: float
    etag: str
    version_id: str | None


@dataclass(frozen=True, slots=True)
class OperaHdfIdentity:
    """Exact live CIRRUS container identity, without raster interpretation."""

    conventions: str
    h5rad_version: str
    source: str
    object_name: str
    product_name: str
    product_description: str
    quantity: str
    quality_task: str
    nominal_timestamp: float
    input_window_start_timestamp: float
    input_window_end_timestamp: float
    rows: int
    columns: int
    xscale_metres: float
    yscale_metres: float
    projection_sha256: str
    reflectivity_dtype: str
    quality_dtype: str
    chunk_count_per_raster: int
    node_count: int
    nodes_sha256: str
    license: str
    creator_name: str
    publisher_name: str
    upstream_comment: str
    upstream_advected_contributions: bool
    motion_truth_eligible: bool


@dataclass(frozen=True, slots=True)
class _RasterStorageIdentity:
    object_token: int
    chunk_count: int
    byte_intervals: tuple[tuple[int, int], ...]


class _RejectRedirects(HTTPRedirectHandler):
    def redirect_request(self, *_args, **_kwargs):
        raise ValueError("OPERA HTTP-redirect is niet toegestaan")


def _open_without_redirects(request: Request, *, timeout: float):
    return build_opener(ProxyHandler({}), _RejectRedirects()).open(
        request, timeout=timeout
    )


def _remaining_seconds(deadline: float) -> float:
    if isinstance(deadline, bool) or not isinstance(deadline, (int, float)):
        raise ValueError("OPERA jobdeadline is ongeldig")
    remaining = float(deadline) - time.monotonic()
    if not math.isfinite(remaining) or remaining <= 0:
        raise TimeoutError("OPERA totale jobdeadline is verstreken")
    return remaining


def _io_timeout(deadline: float) -> float:
    return min(HTTP_IO_TIMEOUT_SECONDS, _remaining_seconds(deadline))


def _safe_auxiliary_directory(store: RawArtifactStore, name: str) -> Path:
    root = Path(store.root)
    if root.is_symlink() or not root.is_dir():
        raise ValueError("OPERA auxiliary root is onveilig")
    directory = root / name
    if directory.exists() and (directory.is_symlink() or not directory.is_dir()):
        raise ValueError("OPERA auxiliary map is onveilig")
    directory.mkdir(mode=0o700, exist_ok=True)
    if directory.is_symlink() or directory.resolve().parent != root.resolve():
        raise ValueError("OPERA auxiliary map ontsnapt uit raw root")
    return directory


def _slot_lock_path(store: RawArtifactStore, job_id: str) -> Path:
    token = hashlib.sha256(job_id.encode("ascii")).hexdigest()
    return _safe_auxiliary_directory(store, "opera_locks") / f"slot-{token}.lock"


def _large_buffer_lock_path(store: RawArtifactStore) -> Path:
    return _safe_auxiliary_directory(store, "opera_locks") / "global-buffer.lock"


def _transport_spool_lock_path(store: RawArtifactStore) -> Path:
    return (
        _safe_auxiliary_directory(store, "opera_locks")
        / "global-transport-spool.lock"
    )


def _transport_spool_directory(store: RawArtifactStore) -> Path:
    """Return one raw-root-bound spool outside the content-addressed store.

    ``RawArtifactStore.store_file`` intentionally rejects source files below
    its own root.  The spool is therefore a direct sibling whose name binds
    the canonical raw-root identity.  It remains inside the same pinned
    worker/raw authority and, on the normal deployment layout, the same
    filesystem as the CAS.
    """

    root = Path(store.root)
    if not root.is_absolute() or root.is_symlink() or not root.is_dir():
        raise ValueError("OPERA raw-rootauthority is onveilig")
    resolved_root = root.resolve(strict=True)
    parent = resolved_root.parent
    if parent.is_symlink() or not parent.is_dir():
        raise ValueError("OPERA spool-parentauthority is onveilig")
    token = hashlib.sha256(
        (
            OPERA_TRANSPORT_SPOOL_SCHEMA
            + "\x00"
            + str(resolved_root)
        ).encode("utf-8", errors="strict")
    ).hexdigest()[:24]
    directory = parent / f"{OPERA_TRANSPORT_SPOOL_PREFIX}{token}"
    if directory.exists() and (
        directory.is_symlink() or not directory.is_dir()
    ):
        raise ValueError("OPERA transportspool is onveilig")
    directory.mkdir(mode=0o700, exist_ok=True)
    details = directory.lstat()
    if (
        directory.is_symlink()
        or not stat.S_ISDIR(details.st_mode)
        or directory.resolve(strict=True).parent != parent
        or directory.resolve(strict=True) == resolved_root
        or resolved_root in directory.resolve(strict=True).parents
    ):
        raise ValueError("OPERA transportspool ontsnapt uit zijn authority")
    if os.name != "nt" and stat.S_IMODE(details.st_mode) != 0o700:
        os.chmod(directory, 0o700)
        if stat.S_IMODE(directory.lstat().st_mode) != 0o700:
            raise ValueError("OPERA transportspool is niet exclusief")
    return directory


def _slot_spool_path(
    store: RawArtifactStore,
    raw_identity: str,
) -> Path:
    token = hashlib.sha256(raw_identity.encode("ascii")).hexdigest()
    return _transport_spool_directory(store) / f"slot-{token}.h5.part"


def _fsync_directory(path: Path) -> None:
    if os.name == "nt":  # directory fsync is not available on Windows
        return
    descriptor = os.open(
        path,
        os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
    )
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _reconcile_transport_spool(directory: Path) -> None:
    """Delete the sole bounded crash residue while holding the global lock."""

    directory = Path(directory)
    if directory.is_symlink() or not directory.is_dir():
        raise ValueError("OPERA transportspool is onveilig")
    entries = tuple(directory.iterdir())
    if len(entries) > OPERA_TRANSPORT_SPOOL_MAX_ENTRIES:
        raise ValueError("OPERA transportspool overschrijdt zijn entrygrens")
    pattern = re.compile(r"^slot-[0-9a-f]{64}\.h5\.part$")
    for entry in entries:
        details = entry.lstat()
        if (
            pattern.fullmatch(entry.name) is None
            or entry.is_symlink()
            or not stat.S_ISREG(details.st_mode)
            or int(getattr(details, "st_nlink", 1)) != 1
            or entry.resolve(strict=True).parent != directory.resolve(strict=True)
            or not 0 <= details.st_size <= MAX_OPERA_HDF5_BYTES
        ):
            raise ValueError("OPERA transportspool bevat een onbekende entry")
        entry.unlink()
    if entries:
        _fsync_directory(directory)


def _preflight_transport_space(
    store: RawArtifactStore,
    spool_directory: Path,
    *,
    content_length: int,
) -> None:
    """Prove free space for the spool plus the independent CAS copy."""

    if (
        isinstance(content_length, bool)
        or not isinstance(content_length, int)
        or not 1 <= content_length <= MAX_OPERA_HDF5_BYTES
    ):
        raise ValueError("OPERA transportpreflight kreeg een ongeldige grootte")
    raw_root = Path(store.root)
    spool_details = spool_directory.stat()
    raw_details = raw_root.stat()
    spool_free = int(shutil.disk_usage(spool_directory).free)
    raw_free = int(shutil.disk_usage(raw_root).free)
    margin = OPERA_TRANSPORT_FREE_MARGIN_BYTES
    if int(spool_details.st_dev) == int(raw_details.st_dev):
        if min(spool_free, raw_free) < (2 * content_length) + margin:
            raise OSError("OPERA transportspool en CAS hebben onvoldoende vrije ruimte")
        return
    if (
        spool_free < content_length + margin
        or raw_free < content_length + margin
    ):
        raise OSError("OPERA transportspool of CAS heeft onvoldoende vrije ruimte")


def _raw_only_checkpoint(_point: str) -> None:
    """Private no-op seam for native hard-exit recovery proofs."""

    return None


@contextmanager
def _deadline_file_lock(path: Path, *, deadline: float) -> Iterator[None]:
    """Acquire one portable inter-process byte lock under the shared deadline."""

    _remaining_seconds(deadline)
    path = Path(path)
    if path.exists() and (path.is_symlink() or not path.is_file()):
        raise ValueError("OPERA lockpad is onveilig")
    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_BINARY", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags, 0o600)
    acquired = False
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode):
            raise ValueError("OPERA lockpad is geen regulier bestand")
        if info.st_size == 0:
            os.write(descriptor, b"\x00")
            os.fsync(descriptor)
        os.lseek(descriptor, 0, os.SEEK_SET)
        while not acquired:
            _remaining_seconds(deadline)
            try:
                if os.name == "nt":  # pragma: no cover - selected by platform
                    import msvcrt

                    msvcrt.locking(descriptor, msvcrt.LK_NBLCK, 1)
                else:  # pragma: no branch - selected by platform
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
        yield
    finally:
        if acquired:
            try:
                os.lseek(descriptor, 0, os.SEEK_SET)
                if os.name == "nt":  # pragma: no cover - selected by platform
                    import msvcrt

                    msvcrt.locking(descriptor, msvcrt.LK_UNLCK, 1)
                else:  # pragma: no branch - selected by platform
                    import fcntl

                    fcntl.flock(descriptor, fcntl.LOCK_UN)
            finally:
                os.close(descriptor)
        else:
            os.close(descriptor)


def _content_type(headers: Any) -> str:
    value = str(headers.get("Content-Type") or "") if headers is not None else ""
    return value.split(";", 1)[0].strip().lower()


def _content_length(headers: Any) -> int:
    raw = str(headers.get("Content-Length") or "") if headers is not None else ""
    if not raw.isascii() or not raw.isdecimal():
        raise ValueError("OPERA-response mist een geldige Content-Length")
    value = int(raw)
    if not 1 <= value <= MAX_OPERA_HDF5_BYTES:
        raise ValueError("OPERA Content-Length valt buiten de objectgrens")
    return value


def _etag(headers: Any) -> str:
    raw = str(headers.get("ETag") or "") if headers is not None else ""
    match = _ETAG_RE.fullmatch(raw)
    if match is None:
        raise ValueError("OPERA-response mist een sterke begrensde ETag")
    # An HTTP entity-tag is opaque and case-sensitive.  The hexadecimal shape
    # is validated, but the exact upstream octets must reach If-Match and the
    # durable revision identity unchanged.
    return raw


def _version_id(headers: Any) -> str | None:
    raw = headers.get("x-amz-version-id") if headers is not None else None
    if raw is None or str(raw).strip() in {"", "null"}:
        return None
    text = str(raw)
    if _VERSION_ID_RE.fullmatch(text) is None:
        raise ValueError("OPERA-response bevat een onveilig version-id")
    return text


def _validate_object_url(url: str, *, expected_key: str) -> None:
    if not isinstance(url, str) or _KEY_RE.fullmatch(expected_key or "") is None:
        raise ValueError("OPERA object-URL of key is ongeldig")
    parsed = urlsplit(url)
    expected_path = f"/{OPERA_S3_BUCKET}/{quote(expected_key, safe='/@.')}"
    try:
        port = parsed.port
    except ValueError as err:
        raise ValueError("OPERA object-URL is ongeldig") from err
    if (
        parsed.scheme != "https"
        or (parsed.hostname or "").lower() != OPERA_S3_HOST
        or port not in {None, 443}
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path != expected_path
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("OPERA object-URL wijkt af van het vaste ORD-endpoint")


def _object_url(object_key: str) -> str:
    if not isinstance(object_key, str) or _KEY_RE.fullmatch(object_key) is None:
        raise ValueError("OPERA object-key wijkt af van het vaste ORD-contract")
    url = f"{OPERA_S3_ENDPOINT}/{OPERA_S3_BUCKET}/{quote(object_key, safe='/@.')}"
    _validate_object_url(url, expected_key=object_key)
    return url


def _validate_response_origin(response: Any, *, expected_url: str) -> Any:
    status = getattr(response, "status", None)
    if isinstance(status, bool) or status != 200:
        raise ValueError("OPERA objectrequest vereist HTTP 200")
    geturl = getattr(response, "geturl", None)
    if not callable(geturl) or str(geturl()) != expected_url:
        raise ValueError("OPERA-response kwam van een onverwacht endpoint")
    headers = getattr(response, "headers", None)
    encoding = (
        str(headers.get("Content-Encoding") or "identity")
        if headers is not None
        else "identity"
    ).lower()
    if encoding != "identity":
        raise ValueError("OPERA-response gebruikt transportcompressie")
    return headers


def _revision_from_headers(headers: Any) -> OperaRevisionIdentity:
    content_type = _content_type(headers)
    if content_type not in _ALLOWED_CONTENT_TYPES:
        raise ValueError("OPERA-response heeft een onverwacht Content-Type")
    return OperaRevisionIdentity(
        etag=_etag(headers),
        version_id=_version_id(headers),
        content_length=_content_length(headers),
        content_type=content_type,
    )


def _head_hdf5(
    object_key: str,
    *,
    deadline: float,
    opener=None,
) -> OperaRevisionIdentity:
    """Resolve the current immutable revision without downloading its body."""

    url = _object_url(object_key)
    request = Request(
        url,
        method="HEAD",
        headers={
            "Accept": "application/x-hdf5, application/octet-stream",
            "Accept-Encoding": "identity",
            "User-Agent": OPERA_HTTP_USER_AGENT,
        },
    )
    active_opener = _open_without_redirects if opener is None else opener
    with active_opener(request, timeout=_io_timeout(deadline)) as response:
        headers = _validate_response_origin(response, expected_url=url)
        revision = _revision_from_headers(headers)
    _remaining_seconds(deadline)
    return revision


def _revision_token(revision: OperaRevisionIdentity) -> str:
    if not isinstance(revision, OperaRevisionIdentity):
        raise TypeError("OPERA revision-token vereist gevalideerde HEAD-metadata")
    document = {
        "content_length": revision.content_length,
        "content_type": revision.content_type,
        "etag": revision.etag,
        "schema": "stv3-opera-upstream-revision-v1",
        "version_id": revision.version_id,
    }
    encoded = json.dumps(document, separators=(",", ":"), sort_keys=True).encode()
    return hashlib.sha256(encoded).hexdigest()


def _canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _identity_document(identity: OperaHdfIdentity) -> dict[str, object]:
    return {
        "chunk_count_per_raster": identity.chunk_count_per_raster,
        "columns": identity.columns,
        "conventions": identity.conventions,
        "creator_name": identity.creator_name,
        "h5rad_version": identity.h5rad_version,
        "input_window_end_timestamp": identity.input_window_end_timestamp,
        "input_window_start_timestamp": identity.input_window_start_timestamp,
        "license": identity.license,
        "motion_truth_eligible": identity.motion_truth_eligible,
        "node_count": identity.node_count,
        "nodes_sha256": identity.nodes_sha256,
        "nominal_timestamp": identity.nominal_timestamp,
        "object_name": identity.object_name,
        "product_description": identity.product_description,
        "product_name": identity.product_name,
        "projection_sha256": identity.projection_sha256,
        "publisher_name": identity.publisher_name,
        "quality_dtype": identity.quality_dtype,
        "quality_task": identity.quality_task,
        "quantity": identity.quantity,
        "reflectivity_dtype": identity.reflectivity_dtype,
        "rows": identity.rows,
        "source": identity.source,
        "upstream_advected_contributions": identity.upstream_advected_contributions,
        "upstream_comment": identity.upstream_comment,
        "xscale_metres": identity.xscale_metres,
        "yscale_metres": identity.yscale_metres,
    }


def _identity_from_document(document: Any) -> OperaHdfIdentity:
    expected_fields = set(_identity_document(OperaHdfIdentity(
        conventions=OPERA_ODIM_CONVENTIONS,
        h5rad_version=OPERA_H5RAD_VERSION,
        source=OPERA_SOURCE,
        object_name="COMP",
        product_name="MAX",
        product_description=OPERA_PRODUCT_NAME,
        quantity=OPERA_PRODUCT_CODE,
        quality_task=OPERA_QUALITY_TASK,
        nominal_timestamp=0.0,
        input_window_start_timestamp=-599.0,
        input_window_end_timestamp=0.0,
        rows=OPERA_GRID_ROWS,
        columns=OPERA_GRID_COLUMNS,
        xscale_metres=OPERA_GRID_SCALE_METRES,
        yscale_metres=OPERA_GRID_SCALE_METRES,
        projection_sha256=hashlib.sha256(OPERA_PROJDEF.encode("ascii")).hexdigest(),
        reflectivity_dtype=OPERA_DATASET_DTYPE,
        quality_dtype=OPERA_DATASET_DTYPE,
        chunk_count_per_raster=1,
        node_count=1,
        nodes_sha256="0" * 64,
        license=OPERA_HOW_LICENSE,
        creator_name=OPERA_HOW_FIXED_TEXT["creator_name"],
        publisher_name=OPERA_HOW_FIXED_TEXT["publisher_name"],
        upstream_comment=OPERA_HOW_COMMENT,
        upstream_advected_contributions=True,
        motion_truth_eligible=False,
    )))
    if not isinstance(document, dict) or set(document) != expected_fields:
        raise ValueError("OPERA validation verdict bevat identity-schema drift")
    integer_fields = {"rows", "columns", "chunk_count_per_raster", "node_count"}
    float_fields = {
        "nominal_timestamp",
        "input_window_start_timestamp",
        "input_window_end_timestamp",
        "xscale_metres",
        "yscale_metres",
    }
    boolean_fields = {"upstream_advected_contributions", "motion_truth_eligible"}
    for field in integer_fields:
        if isinstance(document[field], bool) or not isinstance(document[field], int):
            raise ValueError("OPERA validation verdict bevat ongeldige integers")
    for field in float_fields:
        if isinstance(document[field], bool) or not isinstance(document[field], (int, float)):
            raise ValueError("OPERA validation verdict bevat ongeldige tijden of schalen")
        if not math.isfinite(float(document[field])):
            raise ValueError("OPERA validation verdict bevat niet-eindige waarden")
    for field in boolean_fields:
        if not isinstance(document[field], bool):
            raise ValueError("OPERA validation verdict bevat ongeldige booleans")
    for field in expected_fields - integer_fields - float_fields - boolean_fields:
        if not isinstance(document[field], str) or not document[field]:
            raise ValueError("OPERA validation verdict bevat ongeldige tekst")
    identity = OperaHdfIdentity(
        **{
            **document,
            **{field: float(document[field]) for field in float_fields},
        }
    )
    if (
        identity.conventions != OPERA_ODIM_CONVENTIONS
        or identity.h5rad_version != OPERA_H5RAD_VERSION
        or identity.source != OPERA_SOURCE
        or identity.object_name != "COMP"
        or identity.product_name != "MAX"
        or identity.product_description != OPERA_PRODUCT_NAME
        or identity.quantity != OPERA_PRODUCT_CODE
        or identity.quality_task != OPERA_QUALITY_TASK
        or identity.rows != OPERA_GRID_ROWS
        or identity.columns != OPERA_GRID_COLUMNS
        or identity.xscale_metres != OPERA_GRID_SCALE_METRES
        or identity.yscale_metres != OPERA_GRID_SCALE_METRES
        or identity.projection_sha256
        != hashlib.sha256(OPERA_PROJDEF.encode("ascii")).hexdigest()
        or identity.reflectivity_dtype != OPERA_DATASET_DTYPE
        or identity.quality_dtype != OPERA_DATASET_DTYPE
        or identity.license != OPERA_HOW_LICENSE
        or identity.creator_name != OPERA_HOW_FIXED_TEXT["creator_name"]
        or identity.publisher_name != OPERA_HOW_FIXED_TEXT["publisher_name"]
        or identity.upstream_comment != OPERA_HOW_COMMENT
        or identity.upstream_advected_contributions is not True
        or identity.motion_truth_eligible is not False
        or not 1 <= identity.node_count <= 512
        or _SHA256_RE.fullmatch(identity.nodes_sha256) is None
        or identity.chunk_count_per_raster
        != math.ceil(OPERA_GRID_ROWS / OPERA_CHUNK_ROWS)
        * math.ceil(OPERA_GRID_COLUMNS / OPERA_CHUNK_COLUMNS)
        or identity.input_window_end_timestamp != identity.nominal_timestamp
        or identity.input_window_end_timestamp
        - identity.input_window_start_timestamp
        != 599.0
    ):
        raise ValueError("OPERA validation verdict past niet bij v3-contract")
    return identity


def _validation_verdict_id(
    revision: OperaRevisionIdentity,
    *,
    content_sha256: str,
) -> str:
    if _SHA256_RE.fullmatch(content_sha256) is None:
        raise ValueError("OPERA verdict content-SHA-256 is ongeldig")
    key = {
        "certification_contract": OPERA_CERTIFICATION_CONTRACT_VERSION,
        "content_sha256": content_sha256,
        "inspector_version": OPERA_STRUCTURE_INSPECTOR_VERSION,
        "revision_token": _revision_token(revision),
        "schema": "stv3-opera-validation-key-v1",
    }
    return hashlib.sha256(_canonical_json_bytes(key)).hexdigest()


def _validation_verdict_path(store: RawArtifactStore, verdict_id: str) -> Path:
    if _SHA256_RE.fullmatch(verdict_id) is None:
        raise ValueError("OPERA validation verdict-ID is ongeldig")
    return _safe_auxiliary_directory(store, "opera_validation_verdicts") / (
        f"{verdict_id}.json"
    )


def _validation_verdict_document(
    revision: OperaRevisionIdentity,
    *,
    content_sha256: str,
    status: str,
    identity: OperaHdfIdentity | None = None,
    error_message: str | None = None,
) -> dict[str, object]:
    verdict_id = _validation_verdict_id(
        revision, content_sha256=content_sha256
    )
    base: dict[str, object] = {
        "certification_contract": OPERA_CERTIFICATION_CONTRACT_VERSION,
        "content_sha256": content_sha256,
        "inspector_version": OPERA_STRUCTURE_INSPECTOR_VERSION,
        "revision_token": _revision_token(revision),
        "schema": "stv3-opera-validation-verdict-v1",
        "status": status,
        "verdict_id": verdict_id,
    }
    if status == "validated_complete" and identity is not None and error_message is None:
        base["identity"] = _identity_document(identity)
    elif status == "deterministic_rejection" and identity is None:
        if (
            not isinstance(error_message, str)
            or not error_message
            or len(error_message) > 512
            or any(ord(character) < 0x20 for character in error_message)
        ):
            raise ValueError("OPERA validation error is niet veilig begrensd")
        base["error_code"] = "unsupported_hdf5_contract"
        base["error_message"] = error_message
    else:
        raise ValueError("OPERA validation verdictstatus is ongeldig")
    return base


def _validate_verdict_document(
    document: Any,
    revision: OperaRevisionIdentity,
    *,
    content_sha256: str,
) -> dict[str, object]:
    if not isinstance(document, dict):
        raise ValueError("OPERA validation verdict is geen object")
    common = {
        "certification_contract",
        "content_sha256",
        "inspector_version",
        "revision_token",
        "schema",
        "status",
        "verdict_id",
    }
    status = document.get("status")
    expected_fields = (
        common | {"identity"}
        if status == "validated_complete"
        else common | {"error_code", "error_message"}
    )
    if set(document) != expected_fields:
        raise ValueError("OPERA validation verdict-schema wijkt af")
    expected_id = _validation_verdict_id(revision, content_sha256=content_sha256)
    if (
        document["schema"] != "stv3-opera-validation-verdict-v1"
        or document["certification_contract"] != OPERA_CERTIFICATION_CONTRACT_VERSION
        or document["inspector_version"] != OPERA_STRUCTURE_INSPECTOR_VERSION
        or document["revision_token"] != _revision_token(revision)
        or document["content_sha256"] != content_sha256
        or document["verdict_id"] != expected_id
    ):
        raise ValueError("OPERA validation verdict past niet bij bronrevision")
    if status == "validated_complete":
        _identity_from_document(document["identity"])
    elif status == "deterministic_rejection":
        if (
            document["error_code"] != "unsupported_hdf5_contract"
            or not isinstance(document["error_message"], str)
            or not 1 <= len(document["error_message"]) <= 512
            or any(ord(character) < 0x20 for character in document["error_message"])
        ):
            raise ValueError("OPERA rejection verdict is ongeldig")
    else:
        raise ValueError("OPERA validation verdictstatus is onbekend")
    return document


def _load_validation_verdict(
    store: RawArtifactStore,
    revision: OperaRevisionIdentity,
    *,
    content_sha256: str,
) -> tuple[dict[str, object], str] | None:
    verdict_id = _validation_verdict_id(revision, content_sha256=content_sha256)
    path = _validation_verdict_path(store, verdict_id)
    if not path.exists():
        return None
    info = path.lstat()
    if path.is_symlink() or not stat.S_ISREG(info.st_mode):
        raise ValueError("OPERA validation verdictpad is onveilig")
    if not 1 <= info.st_size <= MAX_VALIDATION_VERDICT_BYTES:
        raise ValueError("OPERA validation verdictgrootte is ongeldig")
    payload = path.read_bytes()
    if len(payload) != info.st_size:
        raise ValueError("OPERA validation verdict veranderde tijdens lezen")
    try:
        document = json.loads(payload.decode("utf-8", errors="strict"))
    except (UnicodeDecodeError, json.JSONDecodeError) as err:
        raise ValueError("OPERA validation verdict is geen canonieke JSON") from err
    document = _validate_verdict_document(
        document, revision, content_sha256=content_sha256
    )
    canonical = _canonical_json_bytes(document)
    if payload != canonical:
        raise ValueError("OPERA validation verdict is niet canoniek")
    return document, hashlib.sha256(payload).hexdigest()


def _publish_validation_verdict(
    store: RawArtifactStore,
    revision: OperaRevisionIdentity,
    *,
    content_sha256: str,
    status: str,
    identity: OperaHdfIdentity | None = None,
    error_message: str | None = None,
) -> tuple[dict[str, object], str]:
    document = _validation_verdict_document(
        revision,
        content_sha256=content_sha256,
        status=status,
        identity=identity,
        error_message=error_message,
    )
    payload = _canonical_json_bytes(document)
    if len(payload) > MAX_VALIDATION_VERDICT_BYTES:
        raise ValueError("OPERA validation verdict overschrijdt bytegrens")
    path = _validation_verdict_path(store, str(document["verdict_id"]))
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w+b",
            prefix=".opera-verdict-",
            suffix=".tmp",
            dir=path.parent,
            delete=False,
        ) as temporary:
            temporary.write(payload)
            temporary.flush()
            os.fsync(temporary.fileno())
            temporary_path = Path(temporary.name)
        try:
            os.link(temporary_path, path)
        except FileExistsError:
            existing = _load_validation_verdict(
                store, revision, content_sha256=content_sha256
            )
            if existing is None or _canonical_json_bytes(existing[0]) != payload:
                raise ValueError("OPERA validation verdict-ID heeft een conflict")
            return existing
        if os.name != "nt":  # pragma: no branch - directory fsync is POSIX
            descriptor = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
    loaded = _load_validation_verdict(store, revision, content_sha256=content_sha256)
    if loaded is None:  # pragma: no cover - durable publish invariant
        raise RuntimeError("OPERA validation verdict ontbreekt na publicatie")
    return loaded


@contextmanager
def _open_download_file(
    *,
    temporary_directory: str | Path | None,
    destination_path: str | Path | None,
) -> Iterator[tuple[Any, Path, tuple[int, int]]]:
    if destination_path is None:
        directory = (
            None
            if temporary_directory is None
            else str(Path(temporary_directory))
        )
        with tempfile.NamedTemporaryFile(
            mode="w+b",
            prefix="stv3-opera-",
            suffix=".h5.part",
            dir=directory,
            delete=False,
        ) as temporary:
            details = os.fstat(temporary.fileno())
            yield temporary, Path(temporary.name), (
                int(details.st_dev),
                int(details.st_ino),
            )
        return
    if temporary_directory is not None:
        raise ValueError("OPERA download kreeg twee tijdelijke authorities")
    path = Path(destination_path)
    parent = path.parent
    if (
        not path.is_absolute()
        or parent.is_symlink()
        or not parent.is_dir()
        or path.exists()
    ):
        raise ValueError("OPERA downloaddoel is onveilig")
    flags = os.O_RDWR | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags, 0o600)
    try:
        details = os.fstat(descriptor)
        if (
            not stat.S_ISREG(details.st_mode)
            or int(getattr(details, "st_nlink", 1)) != 1
            or details.st_size != 0
        ):
            raise ValueError("OPERA downloaddoel is niet exclusief")
        with os.fdopen(descriptor, "w+b", closefd=True) as temporary:
            descriptor = -1
            yield temporary, path, (int(details.st_dev), int(details.st_ino))
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _download_hdf5(
    object_key: str,
    revision: OperaRevisionIdentity,
    *,
    deadline: float,
    opener=None,
    temporary_directory: str | Path | None = None,
    destination_path: str | Path | None = None,
) -> OperaDownloadedFile:
    """Stream exactly Content-Length+1 at most into one temporary file."""

    if not isinstance(revision, OperaRevisionIdentity):
        raise TypeError("OPERA download vereist gevalideerde revision metadata")
    url = _object_url(object_key)
    request = Request(
        url,
        headers={
            "Accept": "application/x-hdf5, application/octet-stream",
            "Accept-Encoding": "identity",
            "If-Match": revision.etag,
            "User-Agent": OPERA_HTTP_USER_AGENT,
        },
    )
    active_opener = _open_without_redirects if opener is None else opener
    temporary_path: Path | None = None
    temporary_witness: tuple[int, int] | None = None
    try:
        with _open_download_file(
            temporary_directory=temporary_directory,
            destination_path=destination_path,
        ) as (temporary, path, witness):
            temporary_path = path
            temporary_witness = witness
            with active_opener(request, timeout=_io_timeout(deadline)) as response:
                headers = _validate_response_origin(response, expected_url=url)
                observed = _revision_from_headers(headers)
                if observed != revision:
                    raise ValueError("OPERA GET-revision wijkt af van de HEAD-revision")
                digest = hashlib.sha256()
                total = 0
                # Probe one byte beyond the declaration.  This makes the
                # declared length, rather than the 384-MiB global ceiling, the
                # effective body read boundary.
                while total <= revision.content_length:
                    _remaining_seconds(deadline)
                    request_bytes = min(
                        STREAM_CHUNK_BYTES,
                        revision.content_length + 1 - total,
                    )
                    chunk = response.read(request_bytes)
                    _remaining_seconds(deadline)
                    if not chunk:
                        break
                    if not isinstance(chunk, (bytes, bytearray, memoryview)):
                        raise TypeError("OPERA HTTP-stream leverde geen bytes")
                    immutable = bytes(chunk)
                    total += len(immutable)
                    if total > revision.content_length:
                        raise ValueError(
                            "OPERA payload bevat bytes voorbij Content-Length"
                        )
                    temporary.write(immutable)
                    digest.update(immutable)
                if total != revision.content_length:
                    raise ValueError("OPERA payloadlengte wijkt af van Content-Length")
            temporary.flush()
            os.fsync(temporary.fileno())
        details = temporary_path.lstat()
        if (
            temporary_path.is_symlink()
            or not stat.S_ISREG(details.st_mode)
            or int(getattr(details, "st_nlink", 1)) != 1
            or (int(details.st_dev), int(details.st_ino)) != temporary_witness
            or details.st_size != revision.content_length
        ):
            raise ValueError("OPERA downloadbestand verloor zijn exclusieve identiteit")
        _remaining_seconds(deadline)
        return OperaDownloadedFile(
            path=str(temporary_path),
            size_bytes=revision.content_length,
            content_sha256=digest.hexdigest(),
            content_type=revision.content_type,
            received_at=time.time(),
            etag=revision.etag,
            version_id=revision.version_id,
        )
    except BaseException:
        if temporary_path is not None:
            try:
                details = temporary_path.lstat()
            except FileNotFoundError:
                details = None
            if details is not None and (
                temporary_path.is_symlink()
                or not stat.S_ISREG(details.st_mode)
                or temporary_witness is None
                or (int(details.st_dev), int(details.st_ino))
                != temporary_witness
            ):
                raise ValueError(
                    "OPERA downloadresidu heeft een andere identiteit"
                )
            if details is not None:
                temporary_path.unlink()
                if destination_path is not None:
                    _fsync_directory(temporary_path.parent)
        raise


def _canonical_job_id(timestamp: float) -> str:
    moment = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    return f"opera-dbzh-{moment:%Y%m%dT%H%M}"


def _job_identity(job: Any) -> tuple[str, float, str]:
    payload = getattr(job, "payload", None)
    if not isinstance(payload, dict) or frozenset(payload) != {"product_timestamp"}:
        raise ValueError("OPERA workerjob vereist exact product_timestamp")
    value = payload["product_timestamp"]
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("OPERA product_timestamp is ongeldig")
    timestamp = float(value)
    if (
        not math.isfinite(timestamp)
        or not timestamp.is_integer()
        or timestamp < _CIRRUS_EARLIEST_TIMESTAMP
        or timestamp >= _LATEST_CONTRACT_TIMESTAMP
        or int(timestamp) % 300 != 0
    ):
        raise ValueError("OPERA product_timestamp is geen vijfminuten-slot")
    job_id = getattr(job, "job_id", None)
    canonical = _canonical_job_id(timestamp)
    if job_id != canonical:
        raise ValueError("OPERA job_id moet exact de canonieke slot-ID zijn")
    moment = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    key = (
        f"{moment:%Y/%m/%d}/OPERA/COMP/"
        f"OPERA@{moment:%Y%m%dT%H%M}@0@DBZH.h5"
    )
    if _KEY_RE.fullmatch(key) is None:  # pragma: no cover
        raise RuntimeError("OPERA object-keygenerator schond zijn contract")
    return canonical, timestamp, key


def _raw_store(injected: RawArtifactStore | None = None) -> RawArtifactStore:
    if injected is not None:
        injected.initialize()
        return injected
    share_root = OPERA_RAW_SHARE_ROOT.resolve(strict=False)
    raw_root = OPERA_RAW_ARTIFACT_ROOT.resolve(strict=False)
    if raw_root == share_root or share_root not in raw_root.parents:
        raise RuntimeError("OPERA raw-artifactpad ontsnapt uit worker-share")
    global _OPERA_RAW_STORE
    with _OPERA_RAW_STORE_LOCK:
        if _OPERA_RAW_STORE is None:
            candidate = RawArtifactStore(
                raw_root,
                limits=RawArtifactLimits(max_object_bytes=MAX_OPERA_HDF5_BYTES),
            )
            candidate.initialize()
            _OPERA_RAW_STORE = candidate
        return _OPERA_RAW_STORE


def _revision_upstream_id(
    object_key: str,
    revision: OperaRevisionIdentity,
    content_sha256: str,
) -> str:
    version = revision.version_id or "none"
    etag = revision.etag.strip('"')
    return (
        f"s3://{OPERA_S3_BUCKET}/{object_key}|etag={etag}"
        f"|version_id={version}|sha256={content_sha256}"
    )


def _native_provenance(
    *,
    object_key: str,
    nominal_timestamp: float,
    revision: OperaRevisionIdentity,
    content_sha256: str,
) -> dict[str, object]:
    return {
        "representation": NATIVE_REPRESENTATION,
        "capture_stage": NATIVE_CAPTURE_STAGE,
        "collector": OPERA_RAW_COLLECTOR,
        "collector_version": OPERA_RAW_COLLECTOR_VERSION,
        "structure_inspector_version": OPERA_STRUCTURE_INSPECTOR_VERSION,
        "ord_endpoint_host": OPERA_S3_HOST,
        "ord_bucket": OPERA_S3_BUCKET,
        "upstream_object_key": object_key,
        "upstream_platform_id": OPERA_PLATFORM_ID,
        "upstream_product_code": OPERA_PRODUCT_CODE,
        "upstream_etag": revision.etag,
        "upstream_version_id": revision.version_id,
        "upstream_content_length": revision.content_length,
        "upstream_content_sha256": content_sha256,
        "upstream_revision_token": _revision_token(revision),
        "nominal_product_timestamp": nominal_timestamp,
        "access_mode": "anonymous_no_sign_request",
        "rights_status": OPERA_RIGHTS_STATUS,
        "declared_semantic_class": OPERA_SEMANTIC_CLASS,
        "declared_quantity": OPERA_QUANTITY,
        "declared_unit": OPERA_UNIT,
        "transport_streaming_to_file": True,
        "schema3_full_object_buffer_required": True,
        "operational_memory_certified": False,
        "memory_activation_blocker": OPERA_MEMORY_ACTIVATION_BLOCKER,
        "decoded_or_admitted": False,
    }


def _raw_only_native_provenance(
    *,
    object_key: str,
    requested_timestamp: float,
    revision: OperaRevisionIdentity,
    content_sha256: str,
) -> dict[str, object]:
    """Describe transport facts only; make no HDF or weather assertion."""

    return {
        "representation": NATIVE_REPRESENTATION,
        "capture_stage": NATIVE_CAPTURE_STAGE,
        "provenance_kind": OPERA_RAW_ONLY_PROVENANCE_KIND,
        "collector": OPERA_RAW_COLLECTOR,
        "collector_version": OPERA_STREAM_RAW_COLLECTOR_VERSION,
        "ord_endpoint_host": OPERA_S3_HOST,
        "ord_bucket": OPERA_S3_BUCKET,
        "upstream_object_key": object_key,
        "upstream_platform_id": OPERA_PLATFORM_ID,
        "upstream_product_code_from_object_key": OPERA_PRODUCT_CODE,
        "upstream_etag": revision.etag,
        "upstream_version_id": revision.version_id,
        "upstream_content_length": revision.content_length,
        "upstream_content_sha256": content_sha256,
        "upstream_revision_token": _revision_token(revision),
        "requested_object_slot_timestamp": requested_timestamp,
        "access_mode": "anonymous_no_sign_request",
        "rights_status": OPERA_RIGHTS_STATUS,
        "rights_activation_prerequisite": OPERA_RAW_RETENTION_AUTHORITY_ID,
        "interpretation": OPERA_RAW_ONLY_INTERPRETATION,
        "content_validation": "exact_size_sha256_and_file_identity_only",
        "transport_streaming_to_file": True,
        "raw_store_file_streaming": True,
        "full_object_buffer_created": False,
        "hdf5_inspection_attempted": False,
        "decoded_or_admitted": False,
        "weather_claim_generated": False,
    }


def _regular_file(path: Path, *, expected_size: int) -> os.stat_result:
    info = path.lstat()
    if path.is_symlink() or not stat.S_ISREG(info.st_mode):
        raise ValueError("OPERA capturepad is geen regulier bestand")
    if info.st_size != expected_size:
        raise ValueError("OPERA capturebestand heeft een andere grootte")
    return info


def _hash_file(path: Path, *, expected_size: int, deadline: float) -> str:
    before = _regular_file(path, expected_size=expected_size)
    digest = hashlib.sha256()
    total = 0
    with path.open("rb") as stream:
        while total <= expected_size:
            _remaining_seconds(deadline)
            chunk = stream.read(min(STREAM_CHUNK_BYTES, expected_size + 1 - total))
            if not chunk:
                break
            total += len(chunk)
            if total > expected_size:
                raise ValueError("OPERA capturebestand bevat extra bytes")
            digest.update(chunk)
    after = _regular_file(path, expected_size=expected_size)
    if total != expected_size or (before.st_dev, before.st_ino) != (
        after.st_dev,
        after.st_ino,
    ):
        raise ValueError("OPERA capturebestand veranderde tijdens hashing")
    _remaining_seconds(deadline)
    return digest.hexdigest()


def _read_once_for_schema3(
    path: Path,
    *,
    expected_size: int,
    expected_sha256: str,
    deadline: float,
) -> bytes:
    """Create the one unavoidable full buffer required by RawArtifactStore."""

    _regular_file(path, expected_size=expected_size)
    _remaining_seconds(deadline)
    with path.open("rb") as stream:
        payload = stream.read(expected_size + 1)
    _remaining_seconds(deadline)
    if len(payload) != expected_size:
        raise ValueError("OPERA schema-3-buffer heeft een andere grootte")
    if hashlib.sha256(payload).hexdigest() != expected_sha256:
        raise ValueError("OPERA schema-3-buffer heeft een andere SHA-256")
    return payload


def _scalar(value: Any, *, field: str) -> Any:
    if hasattr(value, "shape") and getattr(value, "shape", ()) not in {(), None}:
        raise ValueError(f"OPERA HDF5-attribuut {field} is niet scalair")
    if hasattr(value, "item"):
        try:
            value = value.item()
        except (TypeError, ValueError) as err:
            raise ValueError(f"OPERA HDF5-attribuut {field} is niet scalair") from err
    return value


def _required_attr(attributes: Any, name: str, *, field: str | None = None) -> Any:
    if name not in attributes:
        raise ValueError(f"OPERA HDF5 mist attribuut {field or name}")
    return _scalar(attributes[name], field=field or name)


def _ascii(value: Any, *, field: str, maximum: int = 2_048) -> str:
    value = _scalar(value, field=field)
    if isinstance(value, bytes):
        try:
            value = value.decode("ascii", errors="strict")
        except UnicodeDecodeError as err:
            raise ValueError(f"OPERA HDF5-attribuut {field} is geen ASCII") from err
    if (
        not isinstance(value, str)
        or not value
        or len(value) > maximum
        or "\x00" in value
        or any(not 0x20 <= ord(character) <= 0x7E for character in value)
    ):
        raise ValueError(f"OPERA HDF5-attribuut {field} is ongeldige tekst")
    return value


def _finite_number(value: Any, *, field: str) -> float:
    value = _scalar(value, field=field)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"OPERA HDF5-attribuut {field} is niet numeriek")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"OPERA HDF5-attribuut {field} is niet eindig")
    return result


def _exact_number(attributes: Any, name: str, expected: float, *, field: str) -> float:
    actual = _finite_number(_required_attr(attributes, name, field=field), field=field)
    if actual != expected:
        raise ValueError(f"OPERA HDF5-attribuut {field} wijkt af")
    return actual


def _positive_integer(value: Any, *, field: str, maximum: int) -> int:
    number = _finite_number(value, field=field)
    if not number.is_integer() or not 1 <= number <= maximum:
        raise ValueError(f"OPERA HDF5-attribuut {field} is buiten de grens")
    return int(number)


def _exact_attr_names(attributes: Any, expected: set[str], *, field: str) -> None:
    if set(attributes.keys()) != expected:
        raise ValueError(f"OPERA HDF5-attributenschema {field} wijkt af")


def _exact_members(group: h5py.Group, expected: set[str], *, field: str) -> None:
    if set(group.keys()) != expected:
        raise ValueError(f"OPERA HDF5-objectboom {field} wijkt af")


def _object_token(value: h5py.Group | h5py.Dataset | h5py.File) -> int:
    try:
        token = int(h5py.h5o.get_info(value.id).addr)
    except (AttributeError, TypeError, ValueError, RuntimeError) as err:
        raise ValueError("OPERA HDF5-objecttoken kon niet exact worden bepaald") from err
    if token < 0:
        raise ValueError("OPERA HDF5-objecttoken is ongeldig")
    return token


def _parse_nodes(value: Any) -> tuple[str, ...]:
    text = _ascii(value, field="how.nodes", maximum=8_192)
    nodes = tuple(_NODE_ID_RE.findall(text))
    canonical = ", ".join(f"'{node}'" for node in nodes)
    if (
        not 1 <= len(nodes) <= 512
        or text != canonical
        or len(set(nodes)) != len(nodes)
        or nodes != tuple(sorted(nodes))
    ):
        raise ValueError("OPERA HDF5 how.nodes is niet canoniek")
    return nodes


def _filter_name(value: Any) -> bytes:
    """Normalize h5py's version-dependent DCPL filter-name representation."""

    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        try:
            return value.encode("ascii", errors="strict")
        except UnicodeEncodeError as err:
            raise ValueError("OPERA HDF5-filternaam is geen ASCII") from err
    raise ValueError("OPERA HDF5-filternaam heeft een onbekend type")


def _hard_group(root: h5py.File, path: str) -> h5py.Group:
    link = root.get(path, getlink=True)
    if not isinstance(link, h5py.HardLink):
        raise ValueError(f"OPERA HDF5-pad {path} is geen intern hard link")
    value = root.get(path)
    if not isinstance(value, h5py.Group):
        raise ValueError(f"OPERA HDF5 mist groep {path}")
    return value


def _hard_dataset(root: h5py.File, path: str) -> h5py.Dataset:
    link = root.get(path, getlink=True)
    if not isinstance(link, h5py.HardLink):
        raise ValueError(f"OPERA HDF5-pad {path} is geen intern hard link")
    value = root.get(path)
    if not isinstance(value, h5py.Dataset):
        raise ValueError(f"OPERA HDF5 mist dataset {path}")
    return value


def _generic_dataset_contract(dataset: h5py.Dataset) -> int:
    if bool(dataset.is_virtual) or bool(dataset.external):
        raise ValueError("OPERA HDF5 bevat externe of virtuele datasetopslag")
    if dataset.ndim > 4 or any(
        not isinstance(dimension, int) or dimension < 0 or dimension > 100_000
        for dimension in dataset.shape
    ):
        raise ValueError("OPERA HDF5 bevat een onbegrensde datasetvorm")
    if dataset.dtype.hasobject or not 1 <= int(dataset.dtype.itemsize) <= 16:
        raise ValueError("OPERA HDF5 bevat een onbegrensd datatype")
    logical_bytes = math.prod(dataset.shape) * int(dataset.dtype.itemsize)
    if logical_bytes > MAX_HDF_LOGICAL_BYTES:
        raise ValueError("OPERA HDF5-dataset overschrijdt de logische bytegrens")
    if dataset.chunks is not None and (
        len(dataset.chunks) != dataset.ndim
        or any(
            chunk <= 0 or chunk > dimension
            for chunk, dimension in zip(dataset.chunks, dataset.shape)
        )
        or math.prod(dataset.chunks) * int(dataset.dtype.itemsize)
        > MAX_HDF_CHUNK_BYTES
    ):
        raise ValueError("OPERA HDF5-dataset overschrijdt de chunkgrens")
    if dataset.shuffle or dataset.fletcher32 or dataset.scaleoffset is not None:
        raise ValueError("OPERA HDF5 bevat een niet-gecertificeerde DCPL-transformatie")
    dcpl = dataset.id.get_create_plist()
    for index in range(dcpl.get_nfilters()):
        filter_id, flags, values, name = dcpl.get_filter(index)
        if (
            int(filter_id) != int(h5py.h5z.FILTER_DEFLATE)
            or int(flags) != 1
            or tuple(int(value) for value in values) != (OPERA_DEFLATE_LEVEL,)
            or _filter_name(name) != b"deflate"
        ):
            raise ValueError("OPERA HDF5 bevat een onbekende DCPL-filterpipeline")
    return logical_bytes


def _validate_hdf_self_contained(root: h5py.File, *, deadline: float) -> None:
    seen_objects: set[int] = {_object_token(root)}
    object_count = 0
    dataset_count = 0
    logical_bytes = 0

    def walk(group: h5py.Group, *, depth: int) -> None:
        nonlocal object_count, dataset_count, logical_bytes
        _remaining_seconds(deadline)
        if depth > MAX_HDF_DEPTH:
            raise ValueError("OPERA HDF5-groepsdiepte overschrijdt de grens")
        if len(group) > MAX_HDF_GROUP_MEMBERS:
            raise ValueError("OPERA HDF5-groep bevat te veel objecten")
        for name in group.keys():
            object_count += 1
            if object_count > MAX_HDF_OBJECTS:
                raise ValueError("OPERA HDF5 bevat te veel objecten")
            link = group.get(name, getlink=True)
            if not isinstance(link, h5py.HardLink):
                raise ValueError("OPERA HDF5 bevat een externe of symbolische link")
            value = group.get(name)
            if isinstance(value, h5py.Group):
                identity = _object_token(value)
                if identity in seen_objects:
                    raise ValueError("OPERA HDF5 bevat een hardlink-alias")
                seen_objects.add(identity)
                walk(value, depth=depth + 1)
            elif isinstance(value, h5py.Dataset):
                identity = _object_token(value)
                if identity in seen_objects:
                    raise ValueError("OPERA HDF5 bevat een hardlink-alias")
                seen_objects.add(identity)
                dataset_count += 1
                if dataset_count > MAX_HDF_DATASETS:
                    raise ValueError("OPERA HDF5 bevat te veel datasets")
                logical_bytes += _generic_dataset_contract(value)
                if logical_bytes > MAX_HDF_LOGICAL_BYTES:
                    raise ValueError("OPERA HDF5 overschrijdt totale logische bytes")
            else:
                raise ValueError("OPERA HDF5 bevat een onbekend objecttype")

    walk(root, depth=0)


def _timestamp_from_odim(date_value: str, time_value: str) -> float:
    if not re.fullmatch(r"\d{8}", date_value) or not re.fullmatch(
        r"\d{6}", time_value
    ):
        raise ValueError("OPERA HDF5 bevat geen strikte ODIM-producttijd")
    try:
        return datetime.strptime(
            date_value + time_value, "%Y%m%d%H%M%S"
        ).replace(tzinfo=timezone.utc).timestamp()
    except ValueError as err:
        raise ValueError("OPERA HDF5 bevat een ongeldige ODIM-producttijd") from err


def _validate_raster_dcpl(
    dataset: h5py.Dataset,
    *,
    rows: int,
    columns: int,
    file_size: int,
    field: str,
    deadline: float,
) -> _RasterStorageIdentity:
    if tuple(dataset.shape) != (rows, columns) or dataset.dtype.str != OPERA_DATASET_DTYPE:
        raise ValueError(f"OPERA HDF5-{field} heeft shape of dtype drift")
    if dataset.chunks != (OPERA_CHUNK_ROWS, OPERA_CHUNK_COLUMNS):
        raise ValueError(f"OPERA HDF5-{field} heeft een andere chunkvorm")
    if (
        dataset.compression != "gzip"
        or dataset.compression_opts != OPERA_DEFLATE_LEVEL
        or dataset.shuffle
        or dataset.fletcher32
        or dataset.scaleoffset is not None
    ):
        raise ValueError(f"OPERA HDF5-{field} heeft DCPL-drift")
    dcpl = dataset.id.get_create_plist()
    if dcpl.get_layout() != h5py.h5d.CHUNKED or dcpl.get_nfilters() != 1:
        raise ValueError(f"OPERA HDF5-{field} heeft een andere DCPL-layout")
    filter_id, flags, values, name = dcpl.get_filter(0)
    if (
        int(filter_id) != int(h5py.h5z.FILTER_DEFLATE)
        or int(flags) != 1
        or tuple(int(value) for value in values) != (OPERA_DEFLATE_LEVEL,)
        or _filter_name(name) != b"deflate"
    ):
        raise ValueError(f"OPERA HDF5-{field} heeft een andere filterpipeline")
    expected_offsets = {
        (row, column)
        for row in range(0, rows, OPERA_CHUNK_ROWS)
        for column in range(0, columns, OPERA_CHUNK_COLUMNS)
    }
    chunk_count = int(dataset.id.get_num_chunks())
    if chunk_count != len(expected_offsets):
        raise ValueError(f"OPERA HDF5-{field} heeft ontbrekende chunks")
    observed_offsets: set[tuple[int, int]] = set()
    byte_intervals: list[tuple[int, int]] = []
    for index in range(chunk_count):
        _remaining_seconds(deadline)
        info = dataset.id.get_chunk_info(index)
        offset = tuple(int(value) for value in info.chunk_offset)
        if (
            offset not in expected_offsets
            or offset in observed_offsets
            or int(info.filter_mask) != 0
            or not 1 <= int(info.size) <= MAX_HDF_CHUNK_BYTES
            or not 0 <= int(info.byte_offset) < file_size
            or int(info.byte_offset) + int(info.size) > file_size
        ):
            raise ValueError(f"OPERA HDF5-{field} heeft ongeldige chunk/filtermask metadata")
        observed_offsets.add(offset)
        byte_intervals.append(
            (int(info.byte_offset), int(info.byte_offset) + int(info.size))
        )
    if observed_offsets != expected_offsets:
        raise ValueError(f"OPERA HDF5-{field} mist chunkposities")
    byte_intervals.sort()
    if any(
        current_start < previous_end
        for (_previous_start, previous_end), (current_start, _current_end)
        in zip(byte_intervals, byte_intervals[1:])
    ):
        raise ValueError(f"OPERA HDF5-{field} heeft overlappende chunkopslag")
    return _RasterStorageIdentity(
        object_token=_object_token(dataset),
        chunk_count=chunk_count,
        byte_intervals=tuple(byte_intervals),
    )


def _inspect_opera_hdf5(
    artifact_path: str | Path,
    *,
    expected_size: int,
    deadline: float,
) -> OperaHdfIdentity:
    """Inspect the durable file-backed object; never create a BytesIO copy."""

    # The raw-only capture hook must remain importable and runnable without the
    # scientific HDF stack.  Only the legacy certification path loads h5py.
    global h5py
    import h5py as h5py_module

    h5py = h5py_module

    path = Path(artifact_path)
    _regular_file(path, expected_size=expected_size)
    _remaining_seconds(deadline)
    try:
        with h5py.File(path, "r") as root:
            _validate_hdf_self_contained(root, deadline=deadline)
            top_what_group = _hard_group(root, "what")
            where_group = _hard_group(root, "where")
            root_how_group = _hard_group(root, "how")
            dataset1_group = _hard_group(root, "dataset1")
            dataset_what_group = _hard_group(root, "dataset1/what")
            data1_group = _hard_group(root, "dataset1/data1")
            data_what_group = _hard_group(root, "dataset1/data1/what")
            quality1_group = _hard_group(root, "dataset1/data1/quality1")
            quality_what_group = _hard_group(
                root, "dataset1/data1/quality1/what"
            )
            quality_how_group = _hard_group(
                root, "dataset1/data1/quality1/how"
            )
            reflectivity = _hard_dataset(root, "dataset1/data1/data")
            quality = _hard_dataset(root, "dataset1/data1/quality1/data")

            _exact_members(root, {"what", "where", "how", "dataset1"}, field="root")
            _exact_members(top_what_group, set(), field="what")
            _exact_members(where_group, set(), field="where")
            _exact_members(root_how_group, set(), field="how")
            _exact_members(dataset1_group, {"what", "data1"}, field="dataset1")
            _exact_members(dataset_what_group, set(), field="dataset1/what")
            _exact_members(
                data1_group,
                {"what", "data", "quality1"},
                field="dataset1/data1",
            )
            _exact_members(data_what_group, set(), field="dataset1/data1/what")
            _exact_members(
                quality1_group,
                {"what", "how", "data"},
                field="dataset1/data1/quality1",
            )
            _exact_members(
                quality_what_group, set(), field="dataset1/data1/quality1/what"
            )
            _exact_members(
                quality_how_group, set(), field="dataset1/data1/quality1/how"
            )
            for group, field in (
                (dataset1_group, "dataset1"),
                (data1_group, "dataset1/data1"),
                (quality1_group, "dataset1/data1/quality1"),
            ):
                _exact_attr_names(group.attrs, set(), field=field)
            for dataset, field in (
                (reflectivity, "dataset1/data1/data"),
                (quality, "dataset1/data1/quality1/data"),
            ):
                _exact_attr_names(
                    dataset.attrs, {"CLASS", "IMAGE_VERSION"}, field=field
                )
                if (
                    _ascii(dataset.attrs["CLASS"], field=f"{field}.CLASS")
                    != OPERA_IMAGE_CLASS
                    or _ascii(
                        dataset.attrs["IMAGE_VERSION"],
                        field=f"{field}.IMAGE_VERSION",
                    )
                    != OPERA_IMAGE_VERSION
                ):
                    raise ValueError(f"OPERA HDF5-datasetidentiteit {field} wijkt af")

            conventions = _ascii(
                _required_attr(root.attrs, "Conventions"), field="Conventions"
            )
            _exact_attr_names(root.attrs, {"Conventions"}, field="root")
            if conventions != OPERA_ODIM_CONVENTIONS:
                raise ValueError("OPERA HDF5 Conventions wijkt af van ODIM 2.4")

            top_what = top_what_group.attrs
            _exact_attr_names(
                top_what,
                {"date", "object", "source", "time", "version"},
                field="what",
            )
            h5rad_version = _ascii(
                _required_attr(top_what, "version", field="what.version"),
                field="what.version",
            )
            source = _ascii(
                _required_attr(top_what, "source", field="what.source"),
                field="what.source",
            )
            object_name = _ascii(
                _required_attr(top_what, "object", field="what.object"),
                field="what.object",
            )
            if (
                h5rad_version != OPERA_H5RAD_VERSION
                or source != OPERA_SOURCE
                or object_name != "COMP"
            ):
                raise ValueError("OPERA HDF5 top-level productidentiteit wijkt af")
            nominal_timestamp = _timestamp_from_odim(
                _ascii(
                    _required_attr(top_what, "date", field="what.date"),
                    field="what.date",
                    maximum=8,
                ),
                _ascii(
                    _required_attr(top_what, "time", field="what.time"),
                    field="what.time",
                    maximum=6,
                ),
            )

            root_how = root_how_group.attrs
            _exact_attr_names(
                root_how,
                {*OPERA_HOW_FIXED_TEXT, "nodes"},
                field="how",
            )
            how_text: dict[str, str] = {}
            for name, expected in OPERA_HOW_FIXED_TEXT.items():
                actual = _ascii(
                    _required_attr(root_how, name, field=f"how.{name}"),
                    field=f"how.{name}",
                    maximum=2_048,
                )
                if actual != expected:
                    raise ValueError(f"OPERA HDF5 how.{name} wijkt af")
                how_text[name] = actual
            nodes = _parse_nodes(
                _required_attr(root_how, "nodes", field="how.nodes")
            )
            nodes_sha256 = hashlib.sha256(
                _canonical_json_bytes(list(nodes))
            ).hexdigest()

            where = where_group.attrs
            _exact_attr_names(
                where,
                {"xsize", "ysize", "xscale", "yscale", "projdef", *OPERA_CORNERS},
                field="where",
            )
            columns = _positive_integer(
                _required_attr(where, "xsize", field="where.xsize"),
                field="where.xsize",
                maximum=20_000,
            )
            rows = _positive_integer(
                _required_attr(where, "ysize", field="where.ysize"),
                field="where.ysize",
                maximum=20_000,
            )
            if columns != OPERA_GRID_COLUMNS or rows != OPERA_GRID_ROWS:
                raise ValueError("OPERA HDF5 heeft niet het 3800x4400-raster")
            xscale = _exact_number(
                where, "xscale", OPERA_GRID_SCALE_METRES, field="where.xscale"
            )
            yscale = _exact_number(
                where, "yscale", OPERA_GRID_SCALE_METRES, field="where.yscale"
            )
            projdef = _ascii(
                _required_attr(where, "projdef", field="where.projdef"),
                field="where.projdef",
            )
            if projdef != OPERA_PROJDEF:
                raise ValueError("OPERA HDF5 LAEA-projectie wijkt af")
            for name, expected in OPERA_CORNERS.items():
                _exact_number(where, name, expected, field=f"where.{name}")

            dataset_what = dataset_what_group.attrs
            _exact_attr_names(
                dataset_what,
                {"product", "prodname", "startdate", "starttime", "enddate", "endtime"},
                field="dataset1/what",
            )
            product_name = _ascii(
                _required_attr(dataset_what, "product", field="dataset1.what.product"),
                field="dataset1.what.product",
            )
            if product_name != "MAX":
                raise ValueError("OPERA HDF5 dataset is geen MAX-product")
            product_description = _ascii(
                _required_attr(
                    dataset_what, "prodname", field="dataset1.what.prodname"
                ),
                field="dataset1.what.prodname",
            )
            if product_description != OPERA_PRODUCT_NAME:
                raise ValueError("OPERA HDF5 productnaam wijkt af")
            input_window_start = _timestamp_from_odim(
                _ascii(
                    _required_attr(dataset_what, "startdate"),
                    field="dataset1.what.startdate",
                    maximum=8,
                ),
                _ascii(
                    _required_attr(dataset_what, "starttime"),
                    field="dataset1.what.starttime",
                    maximum=6,
                ),
            )
            input_window_end = _timestamp_from_odim(
                _ascii(
                    _required_attr(dataset_what, "enddate"),
                    field="dataset1.what.enddate",
                    maximum=8,
                ),
                _ascii(
                    _required_attr(dataset_what, "endtime"),
                    field="dataset1.what.endtime",
                    maximum=6,
                ),
            )
            if (
                input_window_end != nominal_timestamp
                or input_window_end - input_window_start != 599.0
            ):
                raise ValueError("OPERA HDF5 tienminuten-invoervenster wijkt af")

            data_what = data_what_group.attrs
            _exact_attr_names(
                data_what,
                {"quantity", "gain", "offset", "nodata", "undetect"},
                field="dataset1/data1/what",
            )
            quantity = _ascii(
                _required_attr(data_what, "quantity"), field="data1.what.quantity"
            )
            if quantity != OPERA_PRODUCT_CODE:
                raise ValueError("OPERA HDF5 dataset is geen DBZH-product")
            _exact_number(data_what, "gain", OPERA_DBZH_GAIN, field="data1.what.gain")
            _exact_number(
                data_what, "offset", OPERA_DBZH_OFFSET, field="data1.what.offset"
            )
            _exact_number(
                data_what, "nodata", OPERA_DBZH_NODATA, field="data1.what.nodata"
            )
            _exact_number(
                data_what,
                "undetect",
                OPERA_DBZH_UNDETECT,
                field="data1.what.undetect",
            )

            quality_what = quality_what_group.attrs
            if set(quality_what.keys()) != {"gain", "offset", "nodata"}:
                raise ValueError("OPERA HDF5 quality1/what-schema wijkt af")
            _exact_number(
                quality_what,
                "gain",
                OPERA_QUALITY_GAIN,
                field="quality1.what.gain",
            )
            _exact_number(
                quality_what,
                "offset",
                OPERA_QUALITY_OFFSET,
                field="quality1.what.offset",
            )
            _exact_number(
                quality_what,
                "nodata",
                OPERA_QUALITY_NODATA,
                field="quality1.what.nodata",
            )
            quality_how = quality_how_group.attrs
            _exact_attr_names(quality_how, {"task"}, field="quality1/how")
            quality_task = _ascii(
                _required_attr(quality_how, "task"), field="quality1.how.task"
            )
            if quality_task != OPERA_QUALITY_TASK:
                raise ValueError("OPERA HDF5 quality-task wijkt af")

            reflectivity_chunks = _validate_raster_dcpl(
                reflectivity,
                rows=rows,
                columns=columns,
                file_size=expected_size,
                field="DBZH-raster",
                deadline=deadline,
            )
            quality_chunks = _validate_raster_dcpl(
                quality,
                rows=rows,
                columns=columns,
                file_size=expected_size,
                field="kwaliteitsraster",
                deadline=deadline,
            )
            if reflectivity_chunks.chunk_count != quality_chunks.chunk_count:
                raise ValueError("OPERA HDF5 raster-chunkaantallen verschillen")
            if reflectivity_chunks.object_token == quality_chunks.object_token:
                raise ValueError("OPERA HDF5 DBZH en QI delen een datasetobject")
            combined_intervals = sorted(
                (*reflectivity_chunks.byte_intervals, *quality_chunks.byte_intervals)
            )
            if any(
                current_start < previous_end
                for (_previous_start, previous_end), (current_start, _current_end)
                in zip(combined_intervals, combined_intervals[1:])
            ):
                raise ValueError("OPERA HDF5 DBZH en QI delen fysieke chunkopslag")
            reflectivity_dtype = reflectivity.dtype.str
            quality_dtype = quality.dtype.str
    except OSError as err:
        if getattr(err, "errno", None) is not None:
            # Filesystem/device failures are transient I/O and must never be
            # persisted as a deterministic source verdict.
            raise
        raise ValueError("OPERA-download is geen geldige HDF5-container") from err
    _remaining_seconds(deadline)
    return OperaHdfIdentity(
        conventions=conventions,
        h5rad_version=h5rad_version,
        source=source,
        object_name=object_name,
        product_name=product_name,
        product_description=product_description,
        quantity=quantity,
        quality_task=quality_task,
        nominal_timestamp=nominal_timestamp,
        input_window_start_timestamp=input_window_start,
        input_window_end_timestamp=input_window_end,
        rows=rows,
        columns=columns,
        xscale_metres=xscale,
        yscale_metres=yscale,
        projection_sha256=hashlib.sha256(projdef.encode("ascii")).hexdigest(),
        reflectivity_dtype=reflectivity_dtype,
        quality_dtype=quality_dtype,
        chunk_count_per_raster=reflectivity_chunks.chunk_count,
        node_count=len(nodes),
        nodes_sha256=nodes_sha256,
        license=how_text["license"],
        creator_name=how_text["creator_name"],
        publisher_name=how_text["publisher_name"],
        upstream_comment=how_text["comment"],
        upstream_advected_contributions=True,
        motion_truth_eligible=False,
    )


def _assert_identity_matches_request(
    *, object_key: str, requested_timestamp: float, identity: OperaHdfIdentity
) -> None:
    match = _KEY_RE.fullmatch(object_key)
    if match is None:
        raise ValueError("OPERA object-key wijkt af van ORD")
    key_timestamp = datetime.strptime(
        match.group("stamp"), "%Y%m%dT%H%M"
    ).replace(tzinfo=timezone.utc).timestamp()
    if key_timestamp != requested_timestamp or identity.nominal_timestamp != requested_timestamp:
        raise ValueError("OPERA object-key, jobslot en HDF5-producttijd verschillen")


def _revision_fetch_ids(job_id: str, revision: OperaRevisionIdentity) -> tuple[str, str]:
    token = _revision_token(revision)
    return (
        f"{job_id}:hdf5:{OPERA_RAW_COLLECTOR_VERSION}:revision-{token}",
        f"{job_id}:capture:{OPERA_CERTIFICATION_CONTRACT_VERSION}:revision-{token}",
    )


def _assert_receipt_contract(
    receipt: RawArtifactReceipt,
    *,
    fetch_id: str,
    object_key: str,
    nominal_timestamp: float,
    revision: OperaRevisionIdentity,
) -> None:
    expected_provenance = _native_provenance(
        object_key=object_key,
        nominal_timestamp=nominal_timestamp,
        revision=revision,
        content_sha256=receipt.content_sha256,
    )
    if (
        receipt.provider != "opera"
        or receipt.product != OPERA_HDF5_PRODUCT
        or receipt.fetch_id != fetch_id
        or receipt.upstream_id
        != _revision_upstream_id(object_key, revision, receipt.content_sha256)
        or receipt.content_type != revision.content_type
        or receipt.size_bytes != revision.content_length
        or receipt.provenance != expected_provenance
    ):
        raise ValueError("OPERA hervatte receipt past niet bij de bronrevision")


def _expected_upstream_capture_id(
    *, object_key: str, timestamp: float, revision: OperaRevisionIdentity, sha256: str
) -> str:
    return (
        f"{OPERA_CAPTURE_PRODUCT}|object_key={object_key}"
        f"|nominal_timestamp={timestamp:.0f}|revision={_revision_token(revision)}"
        f"|content_sha256={sha256}|collector={OPERA_RAW_COLLECTOR_VERSION}"
        f"|inspector={OPERA_STRUCTURE_INSPECTOR_VERSION}"
        f"|certification={OPERA_CERTIFICATION_CONTRACT_VERSION}"
    )


def _load_complete_capture(
    store: RawArtifactStore,
    *,
    capture_fetch_id: str,
    raw_fetch_id: str,
    object_key: str,
    revision: OperaRevisionIdentity,
    timestamp: float,
) -> tuple[
    RawArtifactCaptureReceipt,
    RawArtifactReceipt,
    OperaHdfIdentity,
    dict[str, object],
    str,
] | None:
    capture_id = RawArtifactStore._capture_id(
        provider="opera", product=OPERA_CAPTURE_PRODUCT, fetch_id=capture_fetch_id
    )
    bundle = store.load_capture_bundle(capture_id)
    if bundle is None:
        return None
    capture, receipts = bundle
    if len(capture.members) != 1:
        raise ValueError("OPERA bestaande capture heeft geen exact HDF5-lid")
    member = capture.members[0]
    if set(receipts) != {"hdf5"}:
        raise ValueError("OPERA bestaande capture heeft receipt-role drift")
    receipt = receipts["hdf5"]
    receipt_id = RawArtifactStore._receipt_id(
        provider="opera", product=OPERA_HDF5_PRODUCT, fetch_id=raw_fetch_id
    )
    upstream_capture_id = _expected_upstream_capture_id(
        object_key=object_key,
        timestamp=timestamp,
        revision=revision,
        sha256=member.content_sha256,
    )
    if (
        capture.provider != "opera"
        or capture.product != OPERA_CAPTURE_PRODUCT
        or capture.fetch_id != capture_fetch_id
        or capture.upstream_capture_id != upstream_capture_id
        or capture.product_timestamp != timestamp
        or member.role != "hdf5"
        or member.receipt_id != receipt_id
        or receipt.receipt_id != member.receipt_id
        or receipt.content_sha256 != member.content_sha256
        or receipt.receipt_sha256 != member.receipt_sha256
    ):
        raise ValueError("OPERA bestaande capture past niet bij de bronrevision")
    _assert_receipt_contract(
        receipt,
        fetch_id=raw_fetch_id,
        object_key=object_key,
        nominal_timestamp=timestamp,
        revision=revision,
    )
    verdict = _load_validation_verdict(
        store, revision, content_sha256=member.content_sha256
    )
    if verdict is None or verdict[0]["status"] != "validated_complete":
        raise ValueError("OPERA complete capture mist zijn immutable validatieverdict")
    identity = _identity_from_document(verdict[0]["identity"])
    _assert_identity_matches_request(
        object_key=object_key,
        requested_timestamp=timestamp,
        identity=identity,
    )
    return capture, receipt, identity, verdict[0], verdict[1]


def _source_reference(
    receipt: RawArtifactReceipt,
    *,
    object_key: str,
    revision: OperaRevisionIdentity,
    identity: OperaHdfIdentity,
) -> dict[str, object]:
    return {
        "role": "hdf5",
        "raw_receipt_id": receipt.receipt_id,
        "raw_content_sha256": receipt.content_sha256,
        "raw_receipt_sha256": receipt.receipt_sha256,
        "raw_size_bytes": receipt.size_bytes,
        "fetch_id": receipt.fetch_id,
        "upstream_record_id": receipt.upstream_id,
        "upstream_object_key": object_key,
        "upstream_etag": revision.etag,
        "upstream_version_id": revision.version_id,
        "upstream_revision_token": _revision_token(revision),
        "received_at": receipt.received_at,
        "content_type": receipt.content_type,
        "collector_version": OPERA_RAW_COLLECTOR_VERSION,
        "structure_inspector_version": OPERA_STRUCTURE_INSPECTOR_VERSION,
        "certification_contract": OPERA_CERTIFICATION_CONTRACT_VERSION,
        "odim_conventions": identity.conventions,
        "h5rad_version": identity.h5rad_version,
        "source": identity.source,
        "nominal_product_timestamp": identity.nominal_timestamp,
        "input_window_start_timestamp": identity.input_window_start_timestamp,
        "input_window_end_timestamp": identity.input_window_end_timestamp,
        "grid_rows": identity.rows,
        "grid_columns": identity.columns,
        "xscale_metres": identity.xscale_metres,
        "yscale_metres": identity.yscale_metres,
        "projection_sha256": identity.projection_sha256,
        "quality_task": identity.quality_task,
        "product_description": identity.product_description,
        "reflectivity_dtype": identity.reflectivity_dtype,
        "quality_dtype": identity.quality_dtype,
        "chunk_shape": [OPERA_CHUNK_ROWS, OPERA_CHUNK_COLUMNS],
        "compression": "gzip",
        "compression_level": OPERA_DEFLATE_LEVEL,
        "shuffle": False,
        "fletcher32": False,
        "scaleoffset": None,
        "chunk_filter_mask": 0,
        "chunk_count_per_raster": identity.chunk_count_per_raster,
        "node_count": identity.node_count,
        "nodes_sha256": identity.nodes_sha256,
        "upstream_license": identity.license,
        "upstream_creator_name": identity.creator_name,
        "upstream_publisher_name": identity.publisher_name,
        "upstream_comment": identity.upstream_comment,
        "upstream_advected_contributions": identity.upstream_advected_contributions,
        "motion_truth_eligible": identity.motion_truth_eligible,
        "declared_semantic_class": OPERA_SEMANTIC_CLASS,
        "declared_quantity": OPERA_QUANTITY,
        "declared_unit": OPERA_UNIT,
        "quantitative_semantics_eligible": False,
    }


def _capture_provenance(
    capture: RawArtifactCaptureReceipt,
    *,
    receipt: RawArtifactReceipt,
    object_key: str,
    revision: OperaRevisionIdentity,
    identity: OperaHdfIdentity,
    validation_verdict: dict[str, object],
    validation_verdict_sha256: str,
) -> dict[str, object]:
    return {
        "raw_store_schema_version": capture.raw_store_schema_version,
        "raw_capture_id": capture.capture_id,
        "raw_capture_manifest_sha256": capture.manifest_sha256,
        "raw_capture_receipt_sha256": capture.capture_receipt_sha256,
        "raw_capture_fetch_id": capture.fetch_id,
        "upstream_capture_id": capture.upstream_capture_id,
        "nominal_product_timestamp": identity.nominal_timestamp,
        "collector_version": OPERA_RAW_COLLECTOR_VERSION,
        "certification_contract": OPERA_CERTIFICATION_CONTRACT_VERSION,
        "structure_inspector_versions": {"hdf5": OPERA_STRUCTURE_INSPECTOR_VERSION},
        "validation_verdict_id": validation_verdict["verdict_id"],
        "validation_verdict_sha256": validation_verdict_sha256,
        "validation_status": validation_verdict["status"],
        "provenance_grade": "exact_raw_revision_aware",
        "provenance_scope": "single_official_ord_hdf5_revision",
        "rights_status": OPERA_RIGHTS_STATUS,
        "memory_contract": {
            "transport_streaming_to_file": True,
            "hdf5_inspection_file_backed": True,
            "schema3_full_object_buffer_required": True,
            "operational_memory_certified": False,
            "activation_blocker": OPERA_MEMORY_ACTIVATION_BLOCKER,
        },
        "raw_sources": {
            "hdf5": _source_reference(
                receipt,
                object_key=object_key,
                revision=revision,
                identity=identity,
            )
        },
    }


def _result(
    capture: RawArtifactCaptureReceipt,
    *,
    receipt: RawArtifactReceipt,
    object_key: str,
    revision: OperaRevisionIdentity,
    identity: OperaHdfIdentity,
    validation_verdict: dict[str, object],
    validation_verdict_sha256: str,
) -> dict[str, object]:
    return {
        "provider": "opera",
        "product": OPERA_HDF5_PRODUCT,
        "upstream_platform_id": OPERA_PLATFORM_ID,
        "declared_semantic_class": OPERA_SEMANTIC_CLASS,
        "declared_quantity": OPERA_QUANTITY,
        "declared_unit": OPERA_UNIT,
        "nominal_product_timestamp": identity.nominal_timestamp,
        "timestamp_quality_state": OPERA_TIMESTAMP_QUALITY_STATE,
        "raw_member_count": 1,
        "revision_aware_identity": True,
        "certification_contract": OPERA_CERTIFICATION_CONTRACT_VERSION,
        "validation_status": "validated_complete",
        "retryable": False,
        "upstream_advected_contributions": identity.upstream_advected_contributions,
        "motion_truth_eligible": identity.motion_truth_eligible,
        "quantitative_semantics_eligible": False,
        "operational_admission_eligible": False,
        "operational_memory_eligible": False,
        "admission_status": OPERA_ADMISSION_STATUS,
        "rights_status": OPERA_RIGHTS_STATUS,
        "memory_activation_blocker": OPERA_MEMORY_ACTIVATION_BLOCKER,
        "decode_status": "not_decoded",
        "capture_state": "captured_not_decoded",
        "weather_state": "unknown",
        "raw_provenance": _capture_provenance(
            capture,
            receipt=receipt,
            object_key=object_key,
            revision=revision,
            identity=identity,
            validation_verdict=validation_verdict,
            validation_verdict_sha256=validation_verdict_sha256,
        ),
    }


def _rejected_result(
    *,
    receipt: RawArtifactReceipt,
    object_key: str,
    requested_timestamp: float,
    revision: OperaRevisionIdentity,
    validation_verdict: dict[str, object],
    validation_verdict_sha256: str,
) -> dict[str, object]:
    return {
        "provider": "opera",
        "product": OPERA_HDF5_PRODUCT,
        "upstream_platform_id": OPERA_PLATFORM_ID,
        "declared_semantic_class": OPERA_SEMANTIC_CLASS,
        "declared_quantity": OPERA_QUANTITY,
        "declared_unit": OPERA_UNIT,
        "nominal_product_timestamp": requested_timestamp,
        "timestamp_quality_state": OPERA_TIMESTAMP_QUALITY_STATE,
        "raw_member_count": 1,
        "revision_aware_identity": True,
        "certification_contract": OPERA_CERTIFICATION_CONTRACT_VERSION,
        "quantitative_semantics_eligible": False,
        "operational_admission_eligible": False,
        "operational_memory_eligible": False,
        "admission_status": OPERA_ADMISSION_STATUS,
        "rights_status": OPERA_RIGHTS_STATUS,
        "memory_activation_blocker": OPERA_MEMORY_ACTIVATION_BLOCKER,
        "decode_status": "not_decoded",
        "capture_state": "validation_rejected_non_retryable",
        "validation_status": "deterministic_rejection",
        "retryable": False,
        "weather_state": "unknown",
        "upstream_advected_contributions": None,
        "motion_truth_eligible": False,
        "raw_provenance": {
            "raw_store_schema_version": RAW_ARTIFACT_SCHEMA_VERSION,
            "raw_receipt_id": receipt.receipt_id,
            "raw_content_sha256": receipt.content_sha256,
            "raw_receipt_sha256": receipt.receipt_sha256,
            "raw_fetch_id": receipt.fetch_id,
            "upstream_record_id": receipt.upstream_id,
            "upstream_object_key": object_key,
            "upstream_revision_token": _revision_token(revision),
            "collector_version": OPERA_RAW_COLLECTOR_VERSION,
            "structure_inspector_version": OPERA_STRUCTURE_INSPECTOR_VERSION,
            "certification_contract": OPERA_CERTIFICATION_CONTRACT_VERSION,
            "validation_verdict_id": validation_verdict["verdict_id"],
            "validation_verdict_sha256": validation_verdict_sha256,
            "validation_status": validation_verdict["status"],
            "validation_error_code": validation_verdict["error_code"],
            "validation_error": validation_verdict["error_message"],
            "rights_status": OPERA_RIGHTS_STATUS,
            "memory_contract": {
                "transport_streaming_to_file": True,
                "hdf5_inspection_file_backed": True,
                "schema3_full_object_buffer_required": True,
                "operational_memory_certified": False,
                "activation_blocker": OPERA_MEMORY_ACTIVATION_BLOCKER,
            },
        },
    }


def _raw_only_fetch_ids(
    raw_identity: str,
) -> tuple[str, str]:
    return (
        f"{raw_identity}:hdf5",
        f"{raw_identity}:capture",
    )


def _raw_only_upstream_capture_id(
    *,
    object_key: str,
    requested_timestamp: float,
    revision: OperaRevisionIdentity,
    content_sha256: str,
) -> str:
    return (
        f"{OPERA_CAPTURE_PRODUCT}|object_key={object_key}"
        f"|requested_slot={requested_timestamp:.0f}"
        f"|revision={_revision_token(revision)}"
        f"|content_sha256={content_sha256}"
        f"|collector={OPERA_STREAM_RAW_COLLECTOR_VERSION}"
        f"|contract={OPERA_RAW_ONLY_CAPTURE_CONTRACT_VERSION}"
        f"|interpretation={OPERA_RAW_ONLY_INTERPRETATION}"
    )


def _assert_raw_only_receipt_contract(
    receipt: RawArtifactReceipt,
    *,
    fetch_id: str,
    object_key: str,
    requested_timestamp: float,
    revision: OperaRevisionIdentity,
) -> None:
    expected_provenance = _raw_only_native_provenance(
        object_key=object_key,
        requested_timestamp=requested_timestamp,
        revision=revision,
        content_sha256=receipt.content_sha256,
    )
    if (
        receipt.provider != "opera"
        or receipt.product != OPERA_HDF5_PRODUCT
        or receipt.fetch_id != fetch_id
        or receipt.upstream_id
        != _revision_upstream_id(object_key, revision, receipt.content_sha256)
        or receipt.content_type != revision.content_type
        or receipt.size_bytes != revision.content_length
        or receipt.provenance != expected_provenance
    ):
        raise ValueError("OPERA raw-only receipt past niet bij de bronrevision")


def _revision_from_raw_only_receipt(
    receipt: RawArtifactReceipt,
    *,
    fetch_id: str,
    object_key: str,
    requested_timestamp: float,
) -> OperaRevisionIdentity:
    """Reconstruct the exact upstream revision from durable provenance only."""

    provenance = receipt.provenance
    if not isinstance(provenance, Mapping):
        raise ValueError("OPERA raw-only receipt mist provenance")
    etag = provenance.get("upstream_etag")
    version_id = provenance.get("upstream_version_id")
    content_length = provenance.get("upstream_content_length")
    if not isinstance(etag, str) or _ETAG_RE.fullmatch(etag) is None:
        raise ValueError("OPERA raw-only receipt heeft geen sterke ETag")
    if version_id is not None and (
        not isinstance(version_id, str)
        or _VERSION_ID_RE.fullmatch(version_id) is None
    ):
        raise ValueError("OPERA raw-only receipt heeft een onveilig version-id")
    if (
        isinstance(content_length, bool)
        or not isinstance(content_length, int)
        or not 1 <= content_length <= MAX_OPERA_HDF5_BYTES
        or content_length != receipt.size_bytes
        or receipt.content_type not in _ALLOWED_CONTENT_TYPES
    ):
        raise ValueError("OPERA raw-only receipt heeft een ongeldige transportgrens")
    revision = OperaRevisionIdentity(
        etag=etag,
        version_id=version_id,
        content_length=content_length,
        content_type=receipt.content_type,
    )
    _assert_raw_only_receipt_contract(
        receipt,
        fetch_id=fetch_id,
        object_key=object_key,
        requested_timestamp=requested_timestamp,
        revision=revision,
    )
    return revision


def _load_raw_only_capture(
    store: RawArtifactStore,
    *,
    raw_fetch_id: str,
    capture_fetch_id: str,
    object_key: str,
    requested_timestamp: float,
) -> tuple[
    RawArtifactCaptureReceipt,
    RawArtifactReceipt,
    OperaRevisionIdentity,
] | None:
    capture_id = RawArtifactStore._capture_id(
        provider="opera",
        product=OPERA_CAPTURE_PRODUCT,
        fetch_id=capture_fetch_id,
    )
    bundle = store.load_capture_bundle(capture_id)
    if bundle is None:
        return None
    capture, receipts = bundle
    if len(capture.members) != 1 or set(receipts) != {"hdf5"}:
        raise ValueError("OPERA raw-only capture heeft geen exact HDF5-lid")
    member = capture.members[0]
    receipt = receipts["hdf5"]
    expected_receipt_id = RawArtifactStore._receipt_id(
        provider="opera",
        product=OPERA_HDF5_PRODUCT,
        fetch_id=raw_fetch_id,
    )
    revision = _revision_from_raw_only_receipt(
        receipt,
        fetch_id=raw_fetch_id,
        object_key=object_key,
        requested_timestamp=requested_timestamp,
    )
    expected_upstream_capture_id = _raw_only_upstream_capture_id(
        object_key=object_key,
        requested_timestamp=requested_timestamp,
        revision=revision,
        content_sha256=member.content_sha256,
    )
    if (
        capture.provider != "opera"
        or capture.product != OPERA_CAPTURE_PRODUCT
        or capture.fetch_id != capture_fetch_id
        or capture.upstream_capture_id != expected_upstream_capture_id
        or capture.product_timestamp is not None
        or member.role != "hdf5"
        or member.receipt_id != expected_receipt_id
        or receipt.receipt_id != expected_receipt_id
        or receipt.content_sha256 != member.content_sha256
        or receipt.receipt_sha256 != member.receipt_sha256
    ):
        raise ValueError("OPERA raw-only capture past niet bij de bronrevision")
    return capture, receipt, revision


def _raw_only_context_from_receipt(
    receipt: RawArtifactReceipt,
    *,
    raw_identity: str,
) -> tuple[str, float, OperaRevisionIdentity]:
    provenance = receipt.provenance
    object_key = provenance.get("upstream_object_key")
    requested_timestamp = provenance.get("requested_object_slot_timestamp")
    if (
        not isinstance(object_key, str)
        or _KEY_RE.fullmatch(object_key) is None
        or isinstance(requested_timestamp, bool)
        or not isinstance(requested_timestamp, (int, float))
        or not math.isfinite(float(requested_timestamp))
        or not float(requested_timestamp).is_integer()
    ):
        raise ValueError("OPERA durable receipt mist zijn slotcontext")
    timestamp = float(requested_timestamp)
    if (
        timestamp < _CIRRUS_EARLIEST_TIMESTAMP
        or timestamp >= _LATEST_CONTRACT_TIMESTAMP
        or int(timestamp) % 300 != 0
    ):
        raise ValueError("OPERA durable receipt heeft een ongeldig slot")
    moment = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    expected_key = (
        f"{moment:%Y/%m/%d}/OPERA/COMP/"
        f"OPERA@{moment:%Y%m%dT%H%M}@0@DBZH.h5"
    )
    if object_key != expected_key:
        raise ValueError("OPERA durable receipt veranderde zijn object-key")
    revision = _revision_from_raw_only_receipt(
        receipt,
        fetch_id=f"{raw_identity}:hdf5",
        object_key=object_key,
        requested_timestamp=timestamp,
    )
    return object_key, timestamp, revision


def validate_opera_raw_only_receipt_for_recovery(
    receipt: RawArtifactReceipt,
    *,
    job_id: str,
) -> dict[str, object]:
    """Validate one OPERA receipt without reading or returning its payload."""

    if (
        not isinstance(job_id, str)
        or _ACQUISITION_ID_RE.fullmatch(job_id) is None
    ):
        raise ValueError("OPERA recovery job identity is ongeldig")
    object_key, requested_timestamp, revision = _raw_only_context_from_receipt(
        receipt,
        raw_identity=job_id,
    )
    return {
        "content_sha256": receipt.content_sha256,
        "object_key": object_key,
        "requested_timestamp": requested_timestamp,
        "revision_token": _revision_token(revision),
        "upstream_id": _revision_upstream_id(
            object_key,
            revision,
            receipt.content_sha256,
        ),
    }


def validate_opera_raw_only_capture_for_recovery(
    capture: RawArtifactCaptureReceipt,
    receipts_by_role: Mapping[str, RawArtifactReceipt],
    *,
    job_id: str,
) -> dict[str, object]:
    """Validate the fixed OPERA manifest by reference/hash only."""

    if set(receipts_by_role) != {"hdf5"} or len(capture.members) != 1:
        raise ValueError("OPERA recovery capture heeft geen exact HDF5-lid")
    receipt = receipts_by_role["hdf5"]
    context = validate_opera_raw_only_receipt_for_recovery(
        receipt,
        job_id=job_id,
    )
    object_key, requested_timestamp, revision = _raw_only_context_from_receipt(
        receipt,
        raw_identity=job_id,
    )
    raw_fetch_id, capture_fetch_id = _raw_only_fetch_ids(job_id)
    expected_capture_id = RawArtifactStore._capture_id(
        provider="opera",
        product=OPERA_CAPTURE_PRODUCT,
        fetch_id=capture_fetch_id,
    )
    member = capture.members[0]
    if (
        receipt.fetch_id != raw_fetch_id
        or capture.capture_id != expected_capture_id
        or capture.provider != "opera"
        or capture.product != OPERA_CAPTURE_PRODUCT
        or capture.fetch_id != capture_fetch_id
        or capture.product_timestamp is not None
        or member.role != "hdf5"
        or member.receipt_id != receipt.receipt_id
        or member.content_sha256 != receipt.content_sha256
        or member.receipt_sha256 != receipt.receipt_sha256
        or capture.upstream_capture_id
        != _raw_only_upstream_capture_id(
            object_key=object_key,
            requested_timestamp=requested_timestamp,
            revision=revision,
            content_sha256=receipt.content_sha256,
        )
    ):
        raise ValueError("OPERA recovery capture drifted")
    return {
        **context,
        "upstream_capture_id": capture.upstream_capture_id,
    }


def _raw_only_result(
    capture: RawArtifactCaptureReceipt,
    *,
    receipt: RawArtifactReceipt,
    object_key: str,
    requested_timestamp: float,
    revision: OperaRevisionIdentity,
) -> dict[str, object]:
    return {
        "provider": "opera",
        "product": OPERA_HDF5_PRODUCT,
        "upstream_platform_id": OPERA_PLATFORM_ID,
        "requested_object_slot_timestamp": requested_timestamp,
        "raw_member_count": 1,
        "revision_aware_identity": True,
        "raw_capture_contract": OPERA_RAW_ONLY_CAPTURE_CONTRACT_VERSION,
        "interpretation": OPERA_RAW_ONLY_INTERPRETATION,
        "validation_status": "not_interpreted",
        "decode_status": "not_attempted",
        "capture_state": "exact_raw_committed",
        "retryable": False,
        "weather_state": "unknown",
        "weather_claim_generated": False,
        "quantitative_semantics_eligible": False,
        "operational_admission_eligible": False,
        "rights_status": OPERA_RIGHTS_STATUS,
        "rights_activation_prerequisite": OPERA_RAW_RETENTION_AUTHORITY_ID,
        "raw_provenance": {
            "raw_store_schema_version": capture.raw_store_schema_version,
            "raw_capture_id": capture.capture_id,
            "raw_capture_manifest_sha256": capture.manifest_sha256,
            "raw_capture_receipt_sha256": capture.capture_receipt_sha256,
            "raw_capture_fetch_id": capture.fetch_id,
            "upstream_capture_id": capture.upstream_capture_id,
            "collector_version": OPERA_STREAM_RAW_COLLECTOR_VERSION,
            "raw_capture_contract": OPERA_RAW_ONLY_CAPTURE_CONTRACT_VERSION,
            "interpretation": OPERA_RAW_ONLY_INTERPRETATION,
            "rights_status": OPERA_RIGHTS_STATUS,
            "rights_activation_prerequisite": OPERA_RAW_RETENTION_AUTHORITY_ID,
            "memory_contract": {
                "transport_streaming_to_file": True,
                "raw_store_file_streaming": True,
                "full_object_buffer_created": False,
                "hdf5_inspection_attempted": False,
            },
            "raw_sources": {
                "hdf5": {
                    "role": "hdf5",
                    "raw_receipt_id": receipt.receipt_id,
                    "raw_content_sha256": receipt.content_sha256,
                    "raw_receipt_sha256": receipt.receipt_sha256,
                    "raw_size_bytes": receipt.size_bytes,
                    "size_bytes": receipt.size_bytes,
                    "fetch_id": receipt.fetch_id,
                    "upstream_record_id": receipt.upstream_id,
                    "upstream_object_key": object_key,
                    "upstream_etag": revision.etag,
                    "upstream_version_id": revision.version_id,
                    "upstream_revision_token": _revision_token(revision),
                    "received_at": receipt.received_at,
                    "content_type": receipt.content_type,
                    "collector_version": OPERA_STREAM_RAW_COLLECTOR_VERSION,
                    "interpretation": OPERA_RAW_ONLY_INTERPRETATION,
                    "hdf5_inspection_attempted": False,
                    "weather_claim_generated": False,
                }
            },
        },
    }


def _capture_opera_dbzh_hdf5_raw_only_slot_locked(
    *,
    raw_identity: str,
    requested_timestamp: float,
    object_key: str,
    store: RawArtifactStore,
    deadline: float,
) -> dict[str, object]:
    raw_fetch_id, capture_fetch_id = _raw_only_fetch_ids(raw_identity)
    spool_directory = _transport_spool_directory(store)
    # A prior process may have committed the receipt and died before its
    # ``finally`` cleanup. Reconcile before every durable early return/resume.
    with _deadline_file_lock(
        _transport_spool_lock_path(store),
        deadline=deadline,
    ):
        _reconcile_transport_spool(spool_directory)
    complete = _load_raw_only_capture(
        store,
        raw_fetch_id=raw_fetch_id,
        capture_fetch_id=capture_fetch_id,
        object_key=object_key,
        requested_timestamp=requested_timestamp,
    )
    _remaining_seconds(deadline)
    if complete is not None:
        return _raw_only_result(
            complete[0],
            receipt=complete[1],
            object_key=object_key,
            requested_timestamp=requested_timestamp,
            revision=complete[2],
        )

    receipt_id = RawArtifactStore._receipt_id(
        provider="opera",
        product=OPERA_HDF5_PRODUCT,
        fetch_id=raw_fetch_id,
    )
    receipt = store.load_receipt(receipt_id)
    _remaining_seconds(deadline)
    if receipt is not None:
        revision = _revision_from_raw_only_receipt(
            receipt,
            fetch_id=raw_fetch_id,
            object_key=object_key,
            requested_timestamp=requested_timestamp,
        )
    else:
        spool_path = _slot_spool_path(store, raw_identity)
        with _deadline_file_lock(
            _transport_spool_lock_path(store),
            deadline=deadline,
        ):
            _reconcile_transport_spool(spool_directory)
            revision = _head_hdf5(object_key, deadline=deadline)
            _remaining_seconds(deadline)
            _preflight_transport_space(
                store,
                spool_directory,
                content_length=revision.content_length,
            )
            try:
                downloaded = _download_hdf5(
                    object_key,
                    revision,
                    deadline=deadline,
                    destination_path=spool_path,
                )
                download_path = Path(downloaded.path)
                if (
                    download_path != spool_path
                    or downloaded.size_bytes != revision.content_length
                    or downloaded.content_type != revision.content_type
                    or downloaded.etag != revision.etag
                    or downloaded.version_id != revision.version_id
                    or _SHA256_RE.fullmatch(downloaded.content_sha256) is None
                ):
                    raise ValueError("OPERA tijdelijke downloadmetadata veranderde")
                _remaining_seconds(deadline)
                receipt = store.store_file(
                    provider="opera",
                    product=OPERA_HDF5_PRODUCT,
                    fetch_id=raw_fetch_id,
                    content_type=downloaded.content_type,
                    received_at=downloaded.received_at,
                    upstream_id=_revision_upstream_id(
                        object_key,
                        revision,
                        downloaded.content_sha256,
                    ),
                    provenance=_raw_only_native_provenance(
                        object_key=object_key,
                        requested_timestamp=requested_timestamp,
                        revision=revision,
                        content_sha256=downloaded.content_sha256,
                    ),
                    source_path=download_path,
                    expected_size=downloaded.size_bytes,
                    expected_sha256=downloaded.content_sha256,
                )
                _raw_only_checkpoint(
                    "raw_only.after_receipt_commit_before_spool_cleanup"
                )
                _remaining_seconds(deadline)
            finally:
                _reconcile_transport_spool(spool_directory)
    _assert_raw_only_receipt_contract(
        receipt,
        fetch_id=raw_fetch_id,
        object_key=object_key,
        requested_timestamp=requested_timestamp,
        revision=revision,
    )
    capture = store.commit_capture(
        provider="opera",
        product=OPERA_CAPTURE_PRODUCT,
        fetch_id=capture_fetch_id,
        upstream_capture_id=_raw_only_upstream_capture_id(
            object_key=object_key,
            requested_timestamp=requested_timestamp,
            revision=revision,
            content_sha256=receipt.content_sha256,
        ),
        product_timestamp=None,
        members={"hdf5": receipt.receipt_id},
    )
    _remaining_seconds(deadline)
    return _raw_only_result(
        capture,
        receipt=receipt,
        object_key=object_key,
        requested_timestamp=requested_timestamp,
        revision=revision,
    )


def _capture_opera_dbzh_hdf5_raw_only_locked(
    job: Any,
    *,
    raw_store: RawArtifactStore | None,
    raw_identity: str | None,
    deadline: float,
) -> dict[str, object]:
    job_id, requested_timestamp, object_key = _job_identity(job)
    if raw_identity is None:
        raw_identity = job_id
    elif (
        not isinstance(raw_identity, str)
        or _ACQUISITION_ID_RE.fullmatch(raw_identity) is None
    ):
        raise ValueError("OPERA raw identity is geen acquisition-origin")
    _remaining_seconds(deadline)
    store = _raw_store(raw_store)
    _remaining_seconds(deadline)
    with _deadline_file_lock(
        _slot_lock_path(store, raw_identity),
        deadline=deadline,
    ):
        return _capture_opera_dbzh_hdf5_raw_only_slot_locked(
            raw_identity=raw_identity,
            requested_timestamp=requested_timestamp,
            object_key=object_key,
            store=store,
            deadline=deadline,
        )


def capture_opera_dbzh_hdf5_raw_only(
    job: Any,
    *,
    raw_store: RawArtifactStore | None = None,
    raw_identity: str | None = None,
) -> dict[str, object]:
    """Capture exact OPERA bytes without HDF inspection or weather claims."""

    deadline = time.monotonic() + JOB_TOTAL_DEADLINE_SECONDS
    if not _OPERA_CAPTURE_LOCK.acquire(timeout=_remaining_seconds(deadline)):
        raise TimeoutError("OPERA totale jobdeadline verstreek op de capturelock")
    try:
        return _capture_opera_dbzh_hdf5_raw_only_locked(
            job,
            raw_store=raw_store,
            raw_identity=raw_identity,
            deadline=deadline,
        )
    finally:
        _OPERA_CAPTURE_LOCK.release()


def _capture_opera_dbzh_hdf5_locked(
    job: Any,
    *,
    raw_store: RawArtifactStore | None,
    deadline: float,
) -> dict[str, object]:
    job_id, requested_timestamp, object_key = _job_identity(job)
    _remaining_seconds(deadline)
    store = _raw_store(raw_store)
    _remaining_seconds(deadline)
    with _deadline_file_lock(_slot_lock_path(store, job_id), deadline=deadline):
        return _capture_opera_dbzh_hdf5_slot_locked(
            job_id=job_id,
            requested_timestamp=requested_timestamp,
            object_key=object_key,
            store=store,
            deadline=deadline,
        )


def _capture_opera_dbzh_hdf5_slot_locked(
    *,
    job_id: str,
    requested_timestamp: float,
    object_key: str,
    store: RawArtifactStore,
    deadline: float,
) -> dict[str, object]:
    revision = _head_hdf5(object_key, deadline=deadline)
    _remaining_seconds(deadline)
    raw_fetch_id, capture_fetch_id = _revision_fetch_ids(job_id, revision)
    complete = _load_complete_capture(
        store,
        capture_fetch_id=capture_fetch_id,
        raw_fetch_id=raw_fetch_id,
        object_key=object_key,
        revision=revision,
        timestamp=requested_timestamp,
    )
    _remaining_seconds(deadline)
    if complete is not None:
        capture, receipt, identity, verdict_document, verdict_sha256 = complete
        return _result(
            capture,
            receipt=receipt,
            object_key=object_key,
            revision=revision,
            identity=identity,
            validation_verdict=verdict_document,
            validation_verdict_sha256=verdict_sha256,
        )

    receipt_id = RawArtifactStore._receipt_id(
        provider="opera", product=OPERA_HDF5_PRODUCT, fetch_id=raw_fetch_id
    )
    receipt = store.load_receipt(receipt_id)
    _remaining_seconds(deadline)
    if receipt is None:
        downloaded = _download_hdf5(
            object_key,
            revision,
            deadline=deadline,
        )
        download_path = Path(downloaded.path)
        try:
            if (
                downloaded.size_bytes != revision.content_length
                or downloaded.content_type != revision.content_type
                or downloaded.etag != revision.etag
                or downloaded.version_id != revision.version_id
                or _hash_file(
                    download_path,
                    expected_size=downloaded.size_bytes,
                    deadline=deadline,
                )
                != downloaded.content_sha256
            ):
                raise ValueError("OPERA tijdelijke download veranderde")
            with _deadline_file_lock(
                _large_buffer_lock_path(store), deadline=deadline
            ):
                payload = _read_once_for_schema3(
                    download_path,
                    expected_size=downloaded.size_bytes,
                    expected_sha256=downloaded.content_sha256,
                    deadline=deadline,
                )
                try:
                    _remaining_seconds(deadline)
                    receipt = store.store(
                        provider="opera",
                        product=OPERA_HDF5_PRODUCT,
                        fetch_id=raw_fetch_id,
                        content_type=downloaded.content_type,
                        received_at=downloaded.received_at,
                        upstream_id=_revision_upstream_id(
                            object_key, revision, downloaded.content_sha256
                        ),
                        provenance=_native_provenance(
                            object_key=object_key,
                            nominal_timestamp=requested_timestamp,
                            revision=revision,
                            content_sha256=downloaded.content_sha256,
                        ),
                        payload=payload,
                    )
                    _remaining_seconds(deadline)
                finally:
                    del payload
        finally:
            download_path.unlink(missing_ok=True)
    _assert_receipt_contract(
        receipt,
        fetch_id=raw_fetch_id,
        object_key=object_key,
        nominal_timestamp=requested_timestamp,
        revision=revision,
    )
    artifact_path = Path(receipt.artifact_path)
    verdict = _load_validation_verdict(
        store, revision, content_sha256=receipt.content_sha256
    )
    if verdict is not None and verdict[0]["status"] == "deterministic_rejection":
        return _rejected_result(
            receipt=receipt,
            object_key=object_key,
            requested_timestamp=requested_timestamp,
            revision=revision,
            validation_verdict=verdict[0],
            validation_verdict_sha256=verdict[1],
        )
    if verdict is not None:
        identity = _identity_from_document(verdict[0]["identity"])
    else:
        try:
            identity = _inspect_opera_hdf5(
                artifact_path,
                expected_size=receipt.size_bytes,
                deadline=deadline,
            )
            _assert_identity_matches_request(
                object_key=object_key,
                requested_timestamp=requested_timestamp,
                identity=identity,
            )
        except (TimeoutError, OSError):
            raise
        except ValueError as err:
            message = str(err)
            if not message:
                message = "OPERA HDF5 faalt het exacte v3-contract"
            verdict = _publish_validation_verdict(
                store,
                revision,
                content_sha256=receipt.content_sha256,
                status="deterministic_rejection",
                error_message=message[:512],
            )
            return _rejected_result(
                receipt=receipt,
                object_key=object_key,
                requested_timestamp=requested_timestamp,
                revision=revision,
                validation_verdict=verdict[0],
                validation_verdict_sha256=verdict[1],
            )
        verdict = _publish_validation_verdict(
            store,
            revision,
            content_sha256=receipt.content_sha256,
            status="validated_complete",
            identity=identity,
        )
    _assert_identity_matches_request(
        object_key=object_key,
        requested_timestamp=requested_timestamp,
        identity=identity,
    )
    upstream_capture_id = _expected_upstream_capture_id(
        object_key=object_key,
        timestamp=requested_timestamp,
        revision=revision,
        sha256=receipt.content_sha256,
    )
    _remaining_seconds(deadline)
    capture = store.commit_capture(
        provider="opera",
        product=OPERA_CAPTURE_PRODUCT,
        fetch_id=capture_fetch_id,
        upstream_capture_id=upstream_capture_id,
        product_timestamp=identity.nominal_timestamp,
        members={"hdf5": receipt.receipt_id},
    )
    _remaining_seconds(deadline)
    return _result(
        capture,
        receipt=receipt,
        object_key=object_key,
        revision=revision,
        identity=identity,
        validation_verdict=verdict[0],
        validation_verdict_sha256=verdict[1],
    )


def capture_opera_dbzh_hdf5(
    job: Any,
    *,
    raw_store: RawArtifactStore | None = None,
) -> dict[str, object]:
    """Capture one canonical slot under one shared monotonic job deadline."""

    deadline = time.monotonic() + JOB_TOTAL_DEADLINE_SECONDS
    if not _OPERA_CAPTURE_LOCK.acquire(timeout=_remaining_seconds(deadline)):
        raise TimeoutError("OPERA totale jobdeadline verstreek op de capturelock")
    try:
        return _capture_opera_dbzh_hdf5_locked(
            job,
            raw_store=raw_store,
            deadline=deadline,
        )
    finally:
        _OPERA_CAPTURE_LOCK.release()


OPERA_CANDIDATE_REQUIRED_ROLES = ("hdf5",)
OPERA_RAW_ONLY_REQUIRED_ROLES = ("hdf5",)


__all__ = [
    "OPERA_ADMISSION_STATUS",
    "OPERA_CANDIDATE_REQUIRED_ROLES",
    "OPERA_CAPTURE_PRODUCT",
    "OPERA_HDF5_PRODUCT",
    "OPERA_MEMORY_ACTIVATION_BLOCKER",
    "OPERA_QUANTITY",
    "OPERA_RAW_ONLY_CAPTURE_CONTRACT_VERSION",
    "OPERA_RAW_ONLY_HANDLER_PATH",
    "OPERA_RAW_ONLY_INTERPRETATION",
    "OPERA_RAW_ONLY_PROVENANCE_KIND",
    "OPERA_RAW_ONLY_REQUIRED_ROLES",
    "OPERA_RAW_RETENTION_AUTHORITY_ID",
    "OPERA_RIGHTS_STATUS",
    "OPERA_SEMANTIC_CLASS",
    "OPERA_STREAM_RAW_COLLECTOR_VERSION",
    "OPERA_TRANSPORT_SPOOL_PREFIX",
    "OPERA_TIMESTAMP_QUALITY_STATE",
    "OPERA_UNIT",
    "OperaDownloadedFile",
    "OperaHdfIdentity",
    "OperaRevisionIdentity",
    "capture_opera_dbzh_hdf5",
    "capture_opera_dbzh_hdf5_raw_only",
    "validate_opera_raw_only_capture_for_recovery",
    "validate_opera_raw_only_receipt_for_recovery",
]
